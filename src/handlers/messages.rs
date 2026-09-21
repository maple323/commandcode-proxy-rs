//! POST /v1/messages —— Anthropic Messages API 兼容。

use crate::cc::{build_cc_request, forward_to_cc};
use crate::errors::{
    incomplete_upstream_detail, incomplete_upstream_error, map_cc_error, map_cc_event_error,
    map_finish_reason,
};
use crate::http_util::{
    json_response, read_body_limited, read_ndjson_all, take_lines, timeout_message, BodyError,
    FinalError, StreamCtx, UpstreamErr,
};
use crate::state::AppState;
use crate::translate::{build_anthropic_response, convert_anthropic_to_openai, AnthropicSseTranslator};
use crate::util::{get_api_key, json_string, random_uuid, summarize_upstream_error};
use crate::{log_error, log_warn};
use axum::extract::Request;
use axum::response::Response;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn anthropic_error(status: u16, ty: &str, message: &str, retry_after: Option<u64>) -> Response {
    let mut body = Map::new();
    body.insert("type".into(), json!("error"));
    body.insert("error".into(), json!({ "type": ty, "message": message }));
    if let Some(ra) = retry_after {
        body.insert("retry_after".into(), json!(ra));
    }
    json_response(status, &Value::Object(body))
}

pub async fn handle(state: Arc<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();

    let anthropic_req = match read_body_limited(body, state.limits.max_body_size).await {
        Ok(v) => v,
        Err(BodyError::TooLarge(limit)) => {
            let mb = limit / 1024 / 1024;
            return anthropic_error(
                413,
                "invalid_request_error",
                &format!("Request body exceeds {mb}MB limit"),
                None,
            );
        }
        Err(BodyError::InvalidJson) => {
            return anthropic_error(400, "invalid_request_error", "Invalid JSON body", None);
        }
    };

    let Some(api_key) = get_api_key(&parts.headers) else {
        return json_response(
            401,
            &json!({ "type": "error", "error": { "type": "authentication_error", "message": "Missing API key. Send in Authorization: Bearer <key> or x-api-key header" } }),
        );
    };

    let stream = anthropic_req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let model = anthropic_req
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("claude-sonnet-4-6")
        .to_string();

    let openai_req = convert_anthropic_to_openai(&anthropic_req);
    let cc_body = build_cc_request(&state.cfg, &state.profile, &openai_req);

    let ctx = StreamCtx::new();
    let _handler_guard = if stream {
        None
    } else {
        Some(ctx.guard("/v1/messages", "messageId", "(pending)", &model, false))
    };

    state.ensure_initialized(&api_key).await;

    let upstream = match forward_to_cc(&state, cc_body, &api_key, &parts.headers, None).await {
        Ok(r) => r,
        Err(e) => {
            ctx.mark_completed();
            log_error!("Upstream error", { "message": e.to_string() });
            return anthropic_error(502, "proxy_error", &format!("Upstream error: {e}"), Some(10));
        }
    };

    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        let text = upstream.text().await.unwrap_or_default();
        let mapped = map_cc_error(status, Some(&text));
        log_error!("CC API error (Anthropic)", {
            "status": status,
            "code": mapped.code,
            "body": summarize_upstream_error(&text, 500)
        });
        ctx.mark_completed();
        // 与 JS 一致：这里不透传 retry_after（仅 chat 端点会带）
        return anthropic_error(
            mapped.status,
            &mapped.error_type(),
            &mapped.error_message(),
            None,
        );
    }

    if stream {
        return crate::http_util::sse_from_stream(anthropic_stream(
            Arc::clone(&state),
            upstream,
            model,
            ctx,
        ))
        .await;
    }

    // ── 非流式 Anthropic JSON ──
    let mut full_text = String::new();
    let mut thinking_text = String::new();
    let mut finish_reason = "stop".to_string();
    let mut saw_finish = false;
    let mut usage: Option<Value> = None;
    let mut tool_calls: Option<Vec<Value>> = None;
    let mut upstream_error: Option<crate::errors::MappedError> = None;

    let read_result = {
        let ctx_ref = &ctx;
        read_ndjson_all(upstream, state.limits.nonstream_idle_timeout_ms, |line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed == "[DONE]" {
                return;
            }
            let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
                return;
            };
            let Some(ty) = event.get("type").and_then(|t| t.as_str()) else {
                return;
            };
            ctx_ref.set_event(ty);
            match ty {
                "text-delta" => {
                    full_text.push_str(event.get("text").and_then(|t| t.as_str()).unwrap_or(""))
                }
                "reasoning-delta" => {
                    thinking_text.push_str(event.get("text").and_then(|t| t.as_str()).unwrap_or(""))
                }
                "tool-call" => {
                    let args = match event.get("input") {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Null) | None => "{}".to_string(),
                        Some(other) => json_string(other),
                    };
                    tool_calls.get_or_insert_with(Vec::new).push(json!({
                        "id": event.get("toolCallId").and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("call_{}", &random_uuid().replace('-', "")[..8])),
                        "type": "function",
                        "function": {
                            "name": event.get("toolName").and_then(|v| v.as_str()).unwrap_or(""),
                            "arguments": args,
                        },
                    }));
                }
                "finish-step" | "finish" => {
                    saw_finish = true;
                    finish_reason = map_finish_reason(
                        event.get("finishReason").and_then(|f| f.as_str()).unwrap_or("stop"),
                    );
                    let u = event
                        .get("totalUsage")
                        .filter(|v| v.is_object())
                        .or_else(|| event.get("usage").filter(|v| v.is_object()));
                    if let Some(u) = u {
                        usage = Some(u.clone());
                    }
                }
                "error" => {
                    let mapped = map_cc_event_error(&event);
                    log_warn!("CC error (Anthropic non-stream)", {
                        "message": event.get("error").and_then(|e| e.get("message"))
                            .or_else(|| event.get("message")),
                        "upstreamStatus": mapped.reported_status,
                        "upstreamRetryable": event.get("error").and_then(|e| e.get("isRetryable")),
                        "code": mapped.code,
                        "mappedTo": mapped.status
                    });
                    upstream_error = Some(mapped);
                }
                "text-start" | "text-end" | "start" | "start-step" | "reasoning-start"
                | "reasoning-end" | "provider-metadata" | "tool-input-start"
                | "tool-input-delta" | "tool-input-end" | "tool-error" => {}
                other => {
                    log_warn!("Unknown CC event type", { "type": other });
                }
            }
        })
        .await
    };

    match read_result {
        Err(UpstreamErr::Idle) => {
            log_warn!("Stream idle timeout", {
                "path": "/v1/messages",
                "model": model,
                "streaming": false,
                "timeoutMs": state.limits.nonstream_idle_timeout_ms,
                "elapsedMs": ctx.elapsed_ms(),
                "bytesReceived": ctx.bytes(),
                "lastCcEvent": last_event_of(&ctx),
                "partialLen": full_text.chars().count()
            });
            ctx.mark_completed();
            let n = state.bump_consecutive_timeouts();
            return anthropic_error(429, "rate_limit_error", &timeout_message(n), Some(5));
        }
        Err(UpstreamErr::Io(e)) => {
            ctx.mark_completed();
            log_error!("Upstream error", { "message": e });
            return anthropic_error(502, "proxy_error", &format!("Upstream error: {e}"), Some(10));
        }
        Ok(_) => {}
    }

    if let Some(err) = upstream_error {
        ctx.mark_completed();
        return anthropic_error(
            err.status,
            &err.error_type(),
            &err.error_message(),
            err.retry_after(),
        );
    }

    // 上游没有正常走完 finish —— 对齐 CLI 按可重试 502 处理，不谎报成功
    if let Some(detail) = incomplete_upstream_detail(saw_finish, Some(finish_reason.as_str())) {
        log_warn!("Upstream stream incomplete", { "path": "/v1/messages", "reason": detail });
        let err = incomplete_upstream_error(detail);
        ctx.mark_completed();
        return anthropic_error(
            err.status,
            &err.error_type(),
            &err.error_message(),
            err.retry_after(),
        );
    }

    // 零输出判定按实际内容：上游偶发不回 totalUsage 时，旧口径会把有完整文本的响应误杀成 429
    if full_text.is_empty() && thinking_text.is_empty() && tool_calls.is_none() {
        ctx.mark_completed();
        return anthropic_error(
            429,
            "rate_limit_error",
            "Empty response from upstream (zero output tokens)",
            Some(10),
        );
    }

    state.reset_consecutive_timeouts();
    ctx.mark_completed();
    json_response(
        200,
        &build_anthropic_response(
            &model,
            &full_text,
            tool_calls.as_ref(),
            &finish_reason,
            usage.as_ref(),
            &thinking_text,
        ),
    )
}

fn last_event_of(ctx: &StreamCtx) -> String {
    let s = ctx.last_event.lock().map(|v| v.clone()).unwrap_or_default();
    if s.is_empty() {
        "(none)".to_string()
    } else {
        s
    }
}

/// 流式翻译：CC NDJSON → Anthropic SSE。
///
/// message_start 不单独 yield —— 与 JS 的 `buf` 缓冲等价：只有当「真的来了一个
/// 非 message_start 事件」时，才把它和 message_start 一起拼成首个 item 送出去。
/// 这样 `sse_from_stream` 预取首项时，无输出场景仍能回 JSON 429/502 让 SDK 自动重试。
fn anthropic_stream(
    state: Arc<AppState>,
    upstream: reqwest::Response,
    model: String,
    ctx: StreamCtx,
) -> impl futures_util::Stream<Item = Result<Bytes, FinalError>> + Send + 'static {
    async_stream::stream! {
        let message_id = format!("msg_{}", &random_uuid().replace('-', "")[..12]);
        let _guard = ctx.guard("/v1/messages", "messageId", &message_id, &model, true);

        let mut translator = AnthropicSseTranslator::new(&model, &message_id);
        let mut pending_start: Option<String> = Some(translator.message_start());
        let mut reader = upstream.bytes_stream();
        let idle = Duration::from_millis(state.limits.stream_idle_timeout_ms);
        let mut buffer: Vec<u8> = Vec::new();
        let mut last_sent = Instant::now();

        // 心跳：等价于 chat 端点的 ': keepalive'。Anthropic 翻译器会吞掉 signal 事件，
        // 这里改用空闲计时发 ping（Anthropic 标准事件，官方 SDK 会忽略），
        // 覆盖上游排队 / 长 thinking 的静默窗口，避免下游 60s 首字节超时。
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        heartbeat.tick().await; // 丢掉立即触发的第一拍

        // 用 pinned Sleep 而不是每次重建 timeout：否则心跳会不断重置空闲窗口，
        // 导致 idle timeout 永不触发。
        let sleep = tokio::time::sleep(idle);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => {
                    log_warn!("Stream idle timeout", {
                        "path": "/v1/messages",
                        "model": model,
                        "streaming": true,
                        "timeoutMs": state.limits.stream_idle_timeout_ms,
                        "elapsedMs": ctx.elapsed_ms(),
                        "id": message_id,
                        "bytesReceived": ctx.bytes(),
                        "lastCcEvent": last_event_of(&ctx),
                        "inputTokens": translator.input_tokens(),
                        "outputTokens": translator.output_tokens(),
                        "cachedInputTokens": translator.cached_input_tokens()
                    });
                    ctx.mark_completed();
                    let n = state.bump_consecutive_timeouts();
                    let msg = timeout_message(n);
                    if let Some(ms) = pending_start.take() {
                        // 只出过 message_start：整条流没写过字节，回 JSON 让 SDK 重试
                        let _ = ms;
                        yield Err(FinalError {
                            status: 429,
                            body: json!({ "type": "error", "error": { "type": "rate_limit_error", "message": msg }, "retry_after": 5 }),
                        });
                    } else {
                        yield Ok(Bytes::from(format!(
                            "event: error\ndata: {}\n\n",
                            json_string(&json!({
                                "type": "error",
                                "error": { "type": "rate_limit_error", "message": msg },
                                "retry_after": 5,
                            }))
                        )));
                    }
                    return;
                }
                _ = heartbeat.tick() => {
                    if pending_start.is_none() && last_sent.elapsed() >= Duration::from_secs(15) {
                        yield Ok(Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n"));
                        last_sent = Instant::now();
                    }
                }
                next = reader.next() => {
                    let chunk = match next {
                        None => break,
                        Some(Err(e)) => {
                            log_error!("Anthropic stream error", { "message": e.to_string() });
                            ctx.mark_completed();
                            if let Some(ms) = pending_start.take() {
                                let _ = ms;
                                yield Err(FinalError {
                                    status: 502,
                                    body: json!({ "type": "error", "error": { "type": "proxy_error", "message": format!("Upstream error: {e}") }, "retry_after": 10 }),
                                });
                            } else {
                                yield Ok(Bytes::from(format!(
                                    "event: error\ndata: {}\n\n",
                                    json_string(&json!({ "type": "error", "error": { "type": "internal_error", "message": e.to_string() } }))
                                )));
                            }
                            return;
                        }
                        Some(Ok(c)) => c,
                    };
                    // 收到 chunk，重置空闲窗口
                    sleep.as_mut().reset(tokio::time::Instant::now() + idle);

                    ctx.add_bytes(chunk.len());
                    buffer.extend_from_slice(&chunk);

                    let mut lines: Vec<String> = Vec::new();
                    take_lines(&mut buffer, &mut lines);

                    for line in &lines {
                        let events = translator.parse_line(line);
                        for evt in events {
                            let payload = match pending_start.take() {
                                Some(ms) => Bytes::from(format!("{ms}{evt}")),
                                None => Bytes::from(evt),
                            };
                            last_sent = Instant::now();
                            yield Ok(payload);
                        }
                        if !translator.last_cc_event.is_empty() {
                            ctx.set_event(&translator.last_cc_event);
                        }
                    }
                }
            }
        }

        // 处理剩余 buffer
        if !buffer.is_empty() {
            let rest = String::from_utf8_lossy(&buffer).into_owned();
            if !rest.trim().is_empty() {
                for evt in translator.parse_line(&rest) {
                    let payload = match pending_start.take() {
                        Some(ms) => Bytes::from(format!("{ms}{evt}")),
                        None => Bytes::from(evt),
                    };
                    last_sent = Instant::now();
                    yield Ok(payload);
                }
            }
        }

        // 收尾：finalize 负责关闭挂起的块并发出 message_delta + message_stop
        // （或 incomplete / 零输出对应的 error 事件）。
        for evt in translator.finalize() {
            let payload = match pending_start.take() {
                Some(ms) => Bytes::from(format!("{ms}{evt}")),
                None => Bytes::from(evt),
            };
            last_sent = Instant::now();
            yield Ok(payload);
        }

        // 上游错误在流内已经发过 error 事件；只有「一个字节都没写出去」时才回 JSON
        if let Some(err) = translator.upstream_error.clone() {
            if pending_start.is_some() {
                ctx.mark_completed();
                yield Err(FinalError { status: err.status, body: err.body.clone() });
                return;
            }
        }
        let _ = last_sent;
        state.reset_consecutive_timeouts();
        ctx.mark_completed();
    }
}
