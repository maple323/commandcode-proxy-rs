//! POST /v1/chat/completions —— OpenAI Chat Completions 兼容。

use crate::cc::{build_cc_request, forward_to_cc};
use crate::errors::{
    incomplete_upstream_detail, incomplete_upstream_error, map_cc_error, map_cc_event_error,
    map_finish_reason, normalize_usage, to_openai_finish_reason,
};
use crate::http_util::{
    empty_response_body, json_response, read_body_limited, read_ndjson_all, sse_data,
    sse_from_stream, take_lines, timeout_message, BodyError, FinalError, StreamCtx, UpstreamErr,
};
use crate::state::AppState;
use crate::translate::ChatSseTranslator;
use crate::util::{
    get_api_key, json_string, now_unix, nullish_i64, random_uuid, summarize_upstream_error,
};
use crate::{log_error, log_warn};
use axum::extract::Request;
use axum::response::Response;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(state: Arc<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();

    let openai_req = match read_body_limited(body, state.limits.max_body_size).await {
        Ok(v) => v,
        Err(BodyError::TooLarge(limit)) => {
            let mb = limit / 1024 / 1024;
            return json_response(
                413,
                &json!({ "error": { "message": format!("Request body exceeds {mb}MB limit"), "type": "invalid_request_error" } }),
            );
        }
        Err(BodyError::InvalidJson) => {
            return json_response(
                400,
                &json!({ "error": { "message": "Invalid JSON body", "type": "invalid_request_error" } }),
            );
        }
    };

    let Some(api_key) = get_api_key(&parts.headers) else {
        return json_response(
            401,
            &json!({ "error": { "message": "Missing API key. Send in Authorization: Bearer <key> or x-api-key header", "type": "auth_error" } }),
        );
    };

    let stream = openai_req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let model = openai_req
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("deepseek/deepseek-v4-flash")
        .to_string();
    let completion_id = format!("chatcmpl-{}", &random_uuid().replace('-', "")[..12]);
    let created = now_unix();

    let cc_body = build_cc_request(&state.cfg, &state.profile, &openai_req);
    let prompt_cache_key = openai_req
        .get("prompt_cache_key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let ctx = StreamCtx::new();
    // 非流式路径：守卫活在 handler 里，客户端断连 → future 被 drop → 上游请求一并中止
    let _handler_guard = if stream {
        None
    } else {
        Some(ctx.guard("/v1/chat/completions", "completionId", &completion_id, &model, false))
    };

    state.ensure_initialized(&api_key).await;

    let upstream = match forward_to_cc(
        &state,
        cc_body,
        &api_key,
        &parts.headers,
        prompt_cache_key.as_deref(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            ctx.mark_completed();
            log_error!("Upstream error", { "message": e.to_string() });
            return json_response(
                502,
                &json!({ "error": { "message": format!("Upstream error: {e}"), "type": "proxy_error", "input_tokens": 0 }, "retry_after": 10 }),
            );
        }
    };

    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        let text = upstream.text().await.unwrap_or_default();
        let mapped = map_cc_error(status, Some(&text));
        log_error!("CC API error", {
            "status": status,
            "code": mapped.code,
            "body": summarize_upstream_error(&text, 500)
        });
        ctx.mark_completed();
        return crate::http_util::upstream_error_response(&mapped);
    }

    if stream {
        let body = sse_from_stream(chat_stream(
            Arc::clone(&state),
            upstream,
            model,
            completion_id,
            created,
            ctx,
        ))
        .await;
        return body;
    }

    // ── 非流式：缓冲完整 NDJSON ──
    let mut full_text = String::new();
    let mut reasoning_content = String::new();
    let mut finish_reason = "stop".to_string();
    let mut saw_finish = false;
    let mut usage: Option<Value> = None;
    let mut tool_calls: Option<Vec<Value>> = None;
    let mut upstream_error: Option<crate::errors::MappedError> = None;

    let read_result = {
        let ctx_ref = &ctx;
        read_ndjson_all(
            upstream,
            state.limits.nonstream_idle_timeout_ms,
            |line| {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed == "[DONE]" || trimmed.starts_with(':') {
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
                    "text-delta" => full_text
                        .push_str(event.get("text").and_then(|t| t.as_str()).unwrap_or("")),
                    "reasoning-delta" => reasoning_content
                        .push_str(event.get("text").and_then(|t| t.as_str()).unwrap_or("")),
                    "tool-call" => {
                        let id = event
                            .get("toolCallId")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| {
                                format!("call_{}", &random_uuid().replace('-', "")[..8])
                            });
                        let args = match event.get("input") {
                            Some(Value::String(s)) => s.clone(),
                            Some(Value::Null) | None => "{}".to_string(),
                            Some(other) => json_string(other),
                        };
                        tool_calls.get_or_insert_with(Vec::new).push(json!({
                            "id": id,
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
                        if let Some(u) = event.get("totalUsage").filter(|v| v.is_object()) {
                            usage = Some(u.clone());
                        }
                    }
                    "error" => {
                        let mapped = map_cc_event_error(&event);
                        log_warn!("CC stream error (non-stream)", {
                            "message": event.get("error").and_then(|e| e.get("message"))
                                .or_else(|| event.get("message")),
                            "upstreamStatus": mapped.reported_status,
                            "upstreamRetryable": event.get("error").and_then(|e| e.get("isRetryable")),
                            "code": mapped.code,
                            "mappedTo": mapped.status
                        });
                        upstream_error = Some(mapped);
                    }
                    // 无内容的事件：上游每个响应都会发，它们不携带内容（内容在 text-delta），
                    // 掉进 default 会打成 'Unknown CC event type' 刷屏
                    "text-start" | "text-end" | "start" | "start-step" | "reasoning-start"
                    | "reasoning-end" | "provider-metadata" | "tool-input-start"
                    | "tool-input-delta" | "tool-input-end" | "tool-error" => {}
                    other => {
                        log_warn!("Unknown CC event type", { "type": other });
                    }
                }
            },
        )
        .await
    };

    match read_result {
        Err(UpstreamErr::Idle) => {
            log_warn!("Stream idle timeout", {
                "path": "/v1/chat/completions",
                "model": model,
                "streaming": false,
                "timeoutMs": state.limits.nonstream_idle_timeout_ms,
                "elapsedMs": ctx.elapsed_ms(),
                "id": completion_id,
                "bytesReceived": ctx.bytes(),
                "lastCcEvent": last_event_of(&ctx),
                "partialLen": full_text.chars().count()
            });
            ctx.mark_completed();
            let n = state.bump_consecutive_timeouts();
            return json_response(
                429,
                &json!({ "error": { "message": timeout_message(n), "type": "rate_limit_error", "input_tokens": 0 }, "retry_after": 5 }),
            );
        }
        Err(UpstreamErr::Io(e)) => {
            ctx.mark_completed();
            log_error!("Stream error", { "message": e });
            return json_response(
                502,
                &json!({ "error": { "message": format!("Upstream error: {e}"), "type": "proxy_error", "input_tokens": 0 }, "retry_after": 10 }),
            );
        }
        Ok(_) => {}
    }

    if let Some(err) = upstream_error {
        ctx.mark_completed();
        return crate::http_util::upstream_error_response(&err);
    }

    // 上游没有正常走完 finish —— 对齐 CLI 按可重试 502 处理，不谎报成功
    if let Some(detail) = incomplete_upstream_detail(saw_finish, Some(finish_reason.as_str())) {
        log_warn!("Upstream stream incomplete", { "path": "/v1/chat/completions", "reason": detail });
        ctx.mark_completed();
        return crate::http_util::upstream_error_response(&incomplete_upstream_error(detail));
    }

    // 输出 token 为 0 时记为错误，避免下游异常计费
    let reported_output = usage
        .as_ref()
        .map(|u| nullish_i64(u.get("outputTokens"), 0))
        .unwrap_or(0);
    if reported_output == 0 {
        ctx.mark_completed();
        return json_response(429, &empty_response_body());
    }

    state.reset_consecutive_timeouts();

    let mut message = Map::new();
    message.insert("role".into(), json!("assistant"));
    message.insert(
        "content".into(),
        if full_text.is_empty() {
            Value::Null
        } else {
            json!(full_text)
        },
    );
    if let Some(tc) = tool_calls {
        message.insert("tool_calls".into(), Value::Array(tc));
    }
    if !reasoning_content.is_empty() {
        message.insert("reasoning_content".into(), json!(reasoning_content));
    }

    let mut u = usage.unwrap_or_else(|| json!({}));
    normalize_usage(&mut u);
    let in_tok = nullish_i64(u.get("inputTokens"), 0);
    let out_tok = nullish_i64(u.get("outputTokens"), 0);

    ctx.mark_completed();
    json_response(
        200,
        &json!({
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": Value::Object(message),
                "finish_reason": to_openai_finish_reason(&finish_reason),
            }],
            "usage": {
                "prompt_tokens": in_tok,
                "completion_tokens": out_tok,
                "total_tokens": in_tok + out_tok,
                "prompt_tokens_details": {
                    "cached_tokens": nullish_i64(u.get("cachedInputTokens"), 0),
                },
            },
        }),
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

/// 流式翻译：CC NDJSON → OpenAI SSE。
fn chat_stream(
    state: Arc<AppState>,
    upstream: reqwest::Response,
    model: String,
    completion_id: String,
    created: i64,
    ctx: StreamCtx,
) -> impl futures_util::Stream<Item = Result<Bytes, FinalError>> + Send + 'static {
    async_stream::stream! {
        let _guard = ctx.guard(
            "/v1/chat/completions",
            "completionId",
            &completion_id,
            &model,
            true,
        );

        let mut translator = ChatSseTranslator::new(&model, &completion_id, created);
        let mut reader = upstream.bytes_stream();
        let idle = Duration::from_millis(state.limits.stream_idle_timeout_ms);
        let mut buffer: Vec<u8> = Vec::new();
        let mut started = false;

        loop {
            let chunk = match tokio::time::timeout(idle, reader.next()).await {
                // 上游读空闲超时：只计 reader 的等待，每收到一个 chunk 重置
                Err(_) => {
                    log_warn!("Stream idle timeout", {
                        "path": "/v1/chat/completions",
                        "model": model,
                        "streaming": true,
                        "timeoutMs": state.limits.stream_idle_timeout_ms,
                        "elapsedMs": ctx.elapsed_ms(),
                        "id": completion_id,
                        "bytesReceived": ctx.bytes(),
                        "lastCcEvent": last_event_of(&ctx),
                        "inputTokens": translator.input_tokens,
                        "outputTokens": translator.output_tokens,
                        "cachedInputTokens": translator.cached_input_tokens
                    });
                    ctx.mark_completed();
                    let n = state.bump_consecutive_timeouts();
                    let msg = timeout_message(n);
                    if !started {
                        yield Err(FinalError {
                            status: 429,
                            body: json!({ "error": { "message": msg, "type": "rate_limit_error", "input_tokens": 0 }, "retry_after": 5 }),
                        });
                    } else {
                        yield Ok(sse_data(&json!({ "error": { "message": msg, "type": "rate_limit_error" }, "retry_after": 5 })));
                    }
                    return;
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    log_error!("Stream error", { "message": e.to_string() });
                    ctx.mark_completed();
                    if !started {
                        yield Err(FinalError {
                            status: 502,
                            body: json!({ "error": { "message": format!("Upstream error: {e}"), "type": "proxy_error", "input_tokens": 0 }, "retry_after": 10 }),
                        });
                    } else {
                        yield Ok(sse_data(&json!({ "error": { "message": e.to_string(), "type": "proxy_error" } })));
                    }
                    return;
                }
                Ok(Some(Ok(c))) => c,
            };

            ctx.add_bytes(chunk.len());
            buffer.extend_from_slice(&chunk);
            let mut lines: Vec<String> = Vec::new();
            take_lines(&mut buffer, &mut lines);

            let mut had_output = false;
            for line in &lines {
                let events = translator.parse_line(line);
                if !events.is_empty() {
                    started = true;
                    for evt in events {
                        yield Ok(Bytes::from(evt));
                    }
                    had_output = true;
                }
                if !translator.last_cc_event.is_empty() {
                    ctx.set_event(&translator.last_cc_event);
                }
            }
            // silent events 期间发 keepalive，防止客户端超时断开
            if started && !had_output {
                yield Ok(Bytes::from_static(b": keepalive\n\n"));
            }
        }

        // 处理剩余 buffer
        if !buffer.is_empty() {
            let rest = String::from_utf8_lossy(&buffer).into_owned();
            if !rest.trim().is_empty() {
                let events = translator.parse_line(&rest);
                if !events.is_empty() {
                    started = true;
                    for evt in events {
                        yield Ok(Bytes::from(evt));
                    }
                }
            }
        }

        if let Some(err) = translator.upstream_error.clone() {
            ctx.mark_completed();
            if !started {
                yield Err(FinalError { status: err.status, body: err.body.clone() });
            } else {
                yield Ok(sse_data(&err.body));
            }
            return;
        }

        // 上游没有正常走完 finish —— 不能补一个 finish_reason 就 [DONE]，
        // 那等于把截断谎报成完整回答。必须排在零输出判定之前。
        if let Some(detail) = translator.incomplete_detail() {
            log_warn!("Upstream stream incomplete", { "path": "/v1/chat/completions", "reason": detail });
            let err = incomplete_upstream_error(detail);
            ctx.mark_completed();
            if !started {
                yield Err(FinalError { status: err.status, body: err.body.clone() });
            } else {
                yield Ok(sse_data(&err.body));
            }
            return;
        }

        if translator.output_tokens == 0 {
            let body = empty_response_body();
            ctx.mark_completed();
            if !started {
                yield Err(FinalError { status: 429, body });
            } else {
                yield Ok(sse_data(&body));
            }
            return;
        }

        state.reset_consecutive_timeouts();
        ctx.mark_completed();
        yield Ok(Bytes::from(ChatSseTranslator::done_event()));
    }
}
