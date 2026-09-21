//! POST /v1/responses —— OpenAI Responses API（Codex / 新版 SDK）。
//!
//! 代理仍是无状态转换层：把 input 翻译成内部 Chat 格式，复用同一套 CC 转发管线。
//! 不支持 previous_response_id / store（需要服务端保存会话，与无状态定位冲突），
//! 收到直接 400，避免静默降级成错误答案。

use crate::cc::{build_cc_request, forward_to_cc};
use crate::errors::{
    incomplete_upstream_detail, incomplete_upstream_error, map_cc_error, map_cc_event_error,
    map_finish_reason, MappedError,
};
use crate::http_util::{
    json_response, read_body_limited, read_ndjson_all, take_lines, timeout_message, BodyError,
    FinalError, StreamCtx, UpstreamErr,
};
use crate::state::AppState;
use crate::translate::{
    build_responses_object, convert_responses_to_chat, new_responses_id, ResponsesSseTranslator,
};
use crate::util::{get_api_key, json_string, now_unix, random_uuid, summarize_upstream_error};
use crate::{log_error, log_warn};
use axum::extract::Request;
use axum::response::Response;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;

fn responses_error(status: u16, ty: &str, message: &str, retry_after: Option<u64>) -> Response {
    let mut body = Map::new();
    body.insert(
        "error".into(),
        json!({ "message": message, "type": ty, "code": null, "param": null }),
    );
    if let Some(ra) = retry_after {
        body.insert("retry_after".into(), json!(ra));
    }
    json_response(status, &Value::Object(body))
}

fn error_from_mapped(err: &MappedError) -> Response {
    responses_error(
        err.status,
        &err.error_type(),
        &err.error_message(),
        err.retry_after(),
    )
}

pub async fn handle(state: Arc<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();

    let resp_req = match read_body_limited(body, state.limits.max_body_size).await {
        Ok(v) => v,
        Err(BodyError::TooLarge(limit)) => {
            let mb = limit / 1024 / 1024;
            return responses_error(
                413,
                "invalid_request_error",
                &format!("Request body exceeds {mb}MB limit"),
                None,
            );
        }
        Err(BodyError::InvalidJson) => {
            return responses_error(400, "invalid_request_error", "Invalid JSON body", None);
        }
    };

    if resp_req
        .get("previous_response_id")
        .map(crate::util::truthy)
        .unwrap_or(false)
    {
        return responses_error(
            400,
            "invalid_request_error",
            "previous_response_id is not supported (this proxy is stateless); send the full input each turn",
            None,
        );
    }

    let Some(api_key) = get_api_key(&parts.headers) else {
        return responses_error(
            401,
            "authentication_error",
            "Missing API key. Send in Authorization: Bearer <key> or x-api-key header",
            None,
        );
    };

    let chat_req = convert_responses_to_chat(&resp_req);
    let messages_empty = chat_req
        .get("messages")
        .and_then(|m| m.as_array())
        .map(|a| a.is_empty())
        .unwrap_or(true);
    if messages_empty {
        return responses_error(400, "invalid_request_error", "input is required", None);
    }

    let stream = chat_req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let model = chat_req
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("deepseek/deepseek-v4-flash")
        .to_string();
    let response_id = new_responses_id("resp_");
    let created = now_unix();

    // 回显字段：与请求里的原值保持一致（缺省时用规范默认）
    let mut echo_opts = Map::new();
    echo_opts.insert(
        "instructions".into(),
        resp_req.get("instructions").cloned().unwrap_or(Value::Null),
    );
    echo_opts.insert(
        "max_output_tokens".into(),
        resp_req
            .get("max_output_tokens")
            .cloned()
            .unwrap_or(Value::Null),
    );
    if let Some(t) = resp_req.get("temperature") {
        echo_opts.insert("temperature".into(), t.clone());
    }
    if let Some(t) = resp_req.get("top_p") {
        echo_opts.insert("top_p".into(), t.clone());
    }
    echo_opts.insert(
        "reasoning".into(),
        resp_req
            .get("reasoning")
            .filter(|v| crate::util::truthy(v))
            .cloned()
            .unwrap_or(Value::Null),
    );
    echo_opts.insert(
        "tool_choice".into(),
        match resp_req.get("tool_choice") {
            Some(Value::String(s)) => json!(s),
            _ => json!("auto"),
        },
    );
    echo_opts.insert(
        "tools".into(),
        resp_req
            .get("tools")
            .filter(|v| crate::util::truthy(v))
            .cloned()
            .unwrap_or_else(|| json!([])),
    );

    let cc_body = build_cc_request(&state.cfg, &state.profile, &chat_req);
    let prompt_cache_key = chat_req
        .get("prompt_cache_key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let ctx = StreamCtx::new();
    let _handler_guard = if stream {
        None
    } else {
        Some(ctx.guard("/v1/responses", "responseId", &response_id, &model, false))
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
            log_error!("Responses handler error", { "message": e.to_string() });
            return responses_error(502, "proxy_error", &format!("Upstream error: {e}"), Some(10));
        }
    };

    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        let text = upstream.text().await.unwrap_or_default();
        let mapped = map_cc_error(status, Some(&text));
        log_error!("CC API error", {
            "status": status,
            "path": "/v1/responses",
            "code": mapped.code,
            "body": summarize_upstream_error(&text, 500)
        });
        ctx.mark_completed();
        return error_from_mapped(&mapped);
    }

    if stream {
        return crate::http_util::sse_from_stream(responses_stream(
            Arc::clone(&state),
            upstream,
            model,
            response_id,
            created,
            ctx,
        ))
        .await;
    }

    // ── 非流式：缓冲完整 NDJSON 后一次性构造 Responses 对象 ──
    let mut full_text = String::new();
    let mut thinking_text = String::new();
    let mut usage: Option<Value> = None;
    let mut finish_reason = "stop".to_string();
    let mut saw_finish = false;
    let mut upstream_error: Option<MappedError> = None;
    let mut tool_calls: Vec<Value> = Vec::new();

    let read_result = {
        let ctx_ref = &ctx;
        read_ndjson_all(upstream, state.limits.nonstream_idle_timeout_ms, |line| {
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
                    tool_calls.push(json!({
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
                "path": "/v1/responses",
                "model": model,
                "streaming": false,
                "timeoutMs": state.limits.nonstream_idle_timeout_ms,
                "elapsedMs": ctx.elapsed_ms(),
                "bytesReceived": ctx.bytes(),
                "lastCcEvent": last_event_of(&ctx)
            });
            ctx.mark_completed();
            let n = state.bump_consecutive_timeouts();
            return responses_error(429, "rate_limit_error", &timeout_message(n), Some(5));
        }
        Err(UpstreamErr::Io(e)) => {
            ctx.mark_completed();
            log_error!("Responses handler error", { "message": e });
            return responses_error(502, "proxy_error", &format!("Upstream error: {e}"), Some(10));
        }
        Ok(_) => {}
    }

    if let Some(err) = upstream_error {
        ctx.mark_completed();
        return error_from_mapped(&err);
    }

    if let Some(detail) = incomplete_upstream_detail(saw_finish, Some(finish_reason.as_str())) {
        log_warn!("Upstream stream incomplete", { "path": "/v1/responses", "reason": detail });
        ctx.mark_completed();
        return error_from_mapped(&incomplete_upstream_error(detail));
    }

    if full_text.is_empty() && thinking_text.is_empty() && tool_calls.is_empty() {
        ctx.mark_completed();
        return responses_error(
            429,
            "rate_limit_error",
            "Empty response from upstream (zero output tokens)",
            Some(10),
        );
    }

    state.reset_consecutive_timeouts();
    echo_opts.insert("finishReason".into(), json!(finish_reason));
    ctx.mark_completed();
    json_response(
        200,
        &build_responses_object(
            &response_id,
            &model,
            created,
            &full_text,
            &thinking_text,
            if tool_calls.is_empty() { None } else { Some(&tool_calls) },
            usage.as_ref(),
            &Value::Object(echo_opts),
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

/// 流式翻译：CC NDJSON → OpenAI Responses 具名 SSE。
fn responses_stream(
    state: Arc<AppState>,
    upstream: reqwest::Response,
    model: String,
    response_id: String,
    created: i64,
    ctx: StreamCtx,
) -> impl futures_util::Stream<Item = Result<Bytes, FinalError>> + Send + 'static {
    async_stream::stream! {
        let _guard = ctx.guard("/v1/responses", "responseId", &response_id, &model, true);

        let mut translator = ResponsesSseTranslator::new(&model, &response_id, created);
        let mut reader = upstream.bytes_stream();
        let idle = Duration::from_millis(state.limits.stream_idle_timeout_ms);
        let mut buffer: Vec<u8> = Vec::new();

        loop {
            let chunk = match tokio::time::timeout(idle, reader.next()).await {
                Err(_) => {
                    log_warn!("Stream idle timeout", {
                        "path": "/v1/responses",
                        "model": model,
                        "streaming": true,
                        "timeoutMs": state.limits.stream_idle_timeout_ms,
                        "elapsedMs": ctx.elapsed_ms(),
                        "bytesReceived": ctx.bytes(),
                        "lastCcEvent": last_event_of(&ctx)
                    });
                    ctx.mark_completed();
                    let n = state.bump_consecutive_timeouts();
                    let msg = timeout_message(n);
                    if !translator.started() {
                        yield Err(FinalError {
                            status: 429,
                            body: json!({ "error": { "message": msg, "type": "rate_limit_error", "code": null, "param": null }, "retry_after": 5 }),
                        });
                    } else {
                        yield Ok(Bytes::from(translator.error_event(&msg)));
                    }
                    return;
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    log_error!("Stream error", { "message": e.to_string(), "path": "/v1/responses" });
                    ctx.mark_completed();
                    if !translator.started() {
                        yield Err(FinalError {
                            status: 502,
                            body: json!({ "error": { "message": format!("Upstream error: {e}"), "type": "proxy_error", "code": null, "param": null }, "retry_after": 10 }),
                        });
                    } else {
                        yield Ok(Bytes::from(translator.error_event(&e.to_string())));
                    }
                    return;
                }
                Ok(Some(Ok(c))) => c,
            };

            ctx.add_bytes(chunk.len());
            buffer.extend_from_slice(&chunk);

            let mut lines: Vec<String> = Vec::new();
            take_lines(&mut buffer, &mut lines);

            for line in &lines {
                for evt in translator.parse_line(line) {
                    yield Ok(Bytes::from(evt));
                }
                if !translator.last_cc_event.is_empty() {
                    ctx.set_event(&translator.last_cc_event);
                }
            }
        }

        // 处理剩余 buffer
        if !buffer.is_empty() {
            let rest = String::from_utf8_lossy(&buffer).into_owned();
            if !rest.trim().is_empty() {
                for evt in translator.parse_line(&rest) {
                    yield Ok(Bytes::from(evt));
                }
            }
        }

        if let Some(err) = translator.upstream_error.clone() {
            ctx.mark_completed();
            if !translator.started() {
                yield Err(FinalError { status: err.status, body: err.body.clone() });
            } else {
                for evt in translator.fail(&err.error_message()) {
                    yield Ok(Bytes::from(evt));
                }
            }
            return;
        }

        // 上游没有正常走完 finish / 零输出：此时一个字节都还没写出去，回 JSON 让 SDK 重试
        if translator.output_tokens == 0 && !translator.started() {
            ctx.mark_completed();
            yield Err(FinalError {
                status: 429,
                body: json!({ "error": { "message": "Empty response from upstream (zero output tokens)", "type": "rate_limit_error", "code": null, "param": null }, "retry_after": 10 }),
            });
            return;
        }

        for evt in translator.finish() {
            yield Ok(Bytes::from(evt));
        }
        state.reset_consecutive_timeouts();
        ctx.mark_completed();
    }
}
