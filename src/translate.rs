//! CC NDJSON 事件 → 下游三种协议的翻译层。
//!
//! 上游事件集（真实 CLI 抓包）：start / start-step / text-start / text-delta / text-end /
//! reasoning-start / reasoning-delta / reasoning-end / tool-input-start / tool-input-delta /
//! tool-input-end / tool-call / finish-step / finish / error / provider-metadata / tool-error。
//!
//! 注意 `finish-step` 不在官方 CLI 的事件集里，但本代理认它 —— 既然认它，就不能让它
//! 变成「没完成」，否则会把原本正常的响应误判成 502。

use crate::errors::{
    anthropic_input_tokens, fake_thinking_signature, incomplete_upstream_detail,
    incomplete_upstream_error, map_anthropic_stop_reason, map_cc_event_error, map_finish_reason,
    normalize_usage, to_openai_finish_reason, MappedError,
};
use crate::util::{json_string, nullish_i64, nullish_num, random_uuid, try_parse_json};
use serde_json::{json, Map, Value};

fn event_text(event: &Value) -> String {
    match event.get("text") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn event_finish_reason(event: &Value) -> Option<String> {
    event.get("finishReason").and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// `typeof input === 'string' ? input : JSON.stringify(input || {})`
fn tool_arguments(event: &Value) -> String {
    match event.get("input") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "{}".to_string(),
        Some(other) => json_string(other),
    }
}

// ══════════════════════════════════════════════════════════════
// 1) CC NDJSON → OpenAI Chat Completions SSE
// ══════════════════════════════════════════════════════════════

pub struct ChatSseTranslator {
    model: String,
    completion_id: String,
    created: i64,
    saw_finish: bool,
    chunk_index: usize,
    finish_reason: Option<String>,
    usage: Option<Value>,
    tool_call_index: usize,
    pub last_cc_event: String,
    pub upstream_error: Option<MappedError>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_input_tokens: i64,
}

impl ChatSseTranslator {
    pub fn new(model: &str, completion_id: &str, created: i64) -> Self {
        Self {
            model: model.to_string(),
            completion_id: completion_id.to_string(),
            created,
            saw_finish: false,
            chunk_index: 0,
            finish_reason: None,
            usage: None,
            tool_call_index: 0,
            last_cc_event: String::new(),
            upstream_error: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
        }
    }

    pub fn done_event() -> &'static str {
        "data: [DONE]\n\n"
    }

    pub fn incomplete_detail(&self) -> Option<&'static str> {
        incomplete_upstream_detail(self.saw_finish, self.finish_reason.as_deref())
    }

    fn make_chunk(
        &self,
        delta: Value,
        finish_reason: Option<&str>,
        usage: Option<Value>,
    ) -> String {
        let mut chunk = Map::new();
        chunk.insert("id".into(), json!(self.completion_id));
        chunk.insert("object".into(), json!("chat.completion.chunk"));
        chunk.insert("created".into(), json!(self.created));
        chunk.insert("model".into(), json!(self.model));
        chunk.insert(
            "choices".into(),
            json!([{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }]),
        );
        if let Some(u) = usage {
            chunk.insert("usage".into(), u);
        }
        format!("data: {}\n\n", json_string(&Value::Object(chunk)))
    }

    /// 解析一行 NDJSON，返回要写出的 SSE 片段（无输出则返回空）。
    pub fn parse_line(&mut self, line: &str) -> Vec<String> {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "[DONE]" || trimmed.starts_with(':') {
            return Vec::new();
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            return Vec::new();
        };
        let Some(ty) = event.get("type").and_then(|t| t.as_str()) else {
            return Vec::new();
        };
        self.last_cc_event = ty.to_string();
        let mut out: Vec<String> = Vec::new();

        match ty {
            "text-start" | "reasoning-start" | "start" | "start-step" => {}

            "text-delta" => {
                let text = {
                    let t = event_text(&event);
                    if t.is_empty() {
                        event.get("delta").and_then(|d| d.as_str()).unwrap_or("").to_string()
                    } else {
                        t
                    }
                };
                if text.is_empty() {
                    return out;
                }
                let delta = if self.chunk_index == 0 {
                    json!({ "role": "assistant", "content": text })
                } else {
                    json!({ "content": text })
                };
                self.chunk_index += 1;
                out.push(self.make_chunk(delta, None, None));
            }

            "reasoning-delta" => {
                let text = event_text(&event);
                if text.is_empty() {
                    return out;
                }
                let delta = if self.chunk_index == 0 {
                    json!({ "role": "assistant", "reasoning_content": text })
                } else {
                    json!({ "reasoning_content": text })
                };
                self.chunk_index += 1;
                out.push(self.make_chunk(delta, None, None));
            }

            "tool-call" => {
                let id = event
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        format!("call_{}_{}", crate::util::now_unix() * 1000, self.tool_call_index)
                    });
                let name = event.get("toolName").and_then(|v| v.as_str()).unwrap_or("");
                let args = tool_arguments(&event);
                let tc_entry = json!({
                    "index": self.tool_call_index,
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": args },
                });
                let delta = if self.chunk_index == 0 {
                    json!({ "role": "assistant", "content": null, "tool_calls": [tc_entry] })
                } else {
                    json!({ "tool_calls": [tc_entry] })
                };
                self.chunk_index += 1;
                self.tool_call_index += 1;
                out.push(self.make_chunk(delta, None, None));
            }

            "finish-step" => {
                self.saw_finish = true;
                if let Some(fr) = event_finish_reason(&event) {
                    self.finish_reason = Some(map_finish_reason(&fr));
                }
                if let Some(u) = event.get("usage").filter(|v| v.is_object()) {
                    self.usage = Some(u.clone());
                    self.input_tokens = nullish_i64(u.get("inputTokens"), 0);
                    self.output_tokens = nullish_i64(u.get("outputTokens"), 0);
                    self.cached_input_tokens = nullish_i64(u.get("cachedInputTokens"), 0);
                }
            }

            "finish" => {
                self.saw_finish = true;
                let base = self
                    .finish_reason
                    .clone()
                    .unwrap_or_else(|| map_finish_reason(&event_finish_reason(&event).unwrap_or_else(|| "stop".into())));
                let fr = to_openai_finish_reason(&base).to_string();
                let mut u = event
                    .get("totalUsage")
                    .filter(|v| v.is_object())
                    .cloned()
                    .or_else(|| self.usage.clone())
                    .unwrap_or_else(|| json!({}));
                normalize_usage(&mut u);
                self.input_tokens = nullish_i64(u.get("inputTokens"), 0);
                self.output_tokens = nullish_i64(u.get("outputTokens"), 0);
                self.cached_input_tokens = nullish_i64(u.get("cachedInputTokens"), 0);
                let openai_usage = json!({
                    "prompt_tokens": self.input_tokens,
                    "completion_tokens": self.output_tokens,
                    "total_tokens": self.input_tokens + self.output_tokens,
                    "prompt_tokens_details": { "cached_tokens": self.cached_input_tokens },
                });
                out.push(self.make_chunk(json!({}), Some(&fr), Some(openai_usage)));
            }

            "error" => {
                let msg = event
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .or_else(|| event.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error")
                    .to_string();
                let mapped = map_cc_event_error(&event);
                // 先映射再记日志，并把上游自带的状态/可重试性一并打出 ——
                // 排查容量/限流类问题时，真正需要的就是这两个字段
                crate::log_warn!("CC stream error", {
                    "message": msg,
                    "upstreamStatus": mapped.reported_status,
                    "upstreamRetryable": event.get("error").and_then(|e| e.get("isRetryable")),
                    "code": mapped.code,
                    "mappedTo": mapped.status
                });
                self.upstream_error = Some(mapped);
                // 不发 finish_reason chunk —— 让流的自然结束处理它。否则后续的
                // finish(tool_calls) 会被「见到第一个 finish_reason 就停」的下游 agent 循环忽略。
            }

            "reasoning-end" | "provider-metadata" | "tool-input-start" | "tool-input-delta"
            | "tool-input-end" | "tool-error" | "text-end" => {}

            other => {
                crate::log_warn!("Unknown CC event type", { "type": other });
            }
        }

        out
    }
}

// ══════════════════════════════════════════════════════════════
// 2) CC NDJSON → Anthropic Messages SSE
// ══════════════════════════════════════════════════════════════

pub struct AnthropicSseTranslator {
    message_id: String,
    model: String,
    next_block_index: usize,
    current_block_index: i64,
    current_block_type: Option<&'static str>,
    block_started: bool,
    input_tokens: i64,
    output_tokens: i64,
    cached_input_tokens: i64,
    cache_write_tokens: i64,
    /// -1 = 上游未提供该字段，改用减法兜底
    no_cache_tokens: i64,
    stop_reason: Option<String>,
    /// 归一化后的 finishReason（mapAnthropicStopReason 之前的值），用于判定「是否正常结束」
    finish_norm: Option<String>,
    saw_finish: bool,
    has_error: bool,
    current_thinking_text: String,
    pub last_cc_event: String,
    pub upstream_error: Option<MappedError>,
}

impl AnthropicSseTranslator {
    pub fn new(model: &str, message_id: &str) -> Self {
        Self {
            message_id: message_id.to_string(),
            model: model.to_string(),
            next_block_index: 0,
            current_block_index: -1,
            current_block_type: None,
            block_started: false,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            no_cache_tokens: -1,
            stop_reason: None,
            finish_norm: None,
            saw_finish: false,
            has_error: false,
            current_thinking_text: String::new(),
            last_cc_event: String::new(),
            upstream_error: None,
        }
    }

    pub fn input_tokens(&self) -> i64 {
        self.input_tokens
    }

    pub fn output_tokens(&self) -> i64 {
        self.output_tokens
    }

    pub fn cached_input_tokens(&self) -> i64 {
        self.cached_input_tokens
    }

    /// message_start 永远是第一个事件。
    pub fn message_start(&self) -> String {
        let payload = json!({
            "type": "message_start",
            "message": {
                "id": self.message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": self.model,
                "usage": { "input_tokens": 0, "output_tokens": 0 },
            }
        });
        format!("event: message_start\ndata: {}\n\n", json_string(&payload))
    }

    fn sse(ty: &str, payload: Value) -> String {
        format!("event: {ty}\ndata: {}\n\n", json_string(&payload))
    }

    /// 关闭当前块（text 或 thinking）。thinking 块在 stop 前发 signature_delta。
    fn close_block(&mut self) -> String {
        if !self.block_started {
            return String::new();
        }
        let idx = self.current_block_index;
        let ty = self.current_block_type;
        let mut out = String::new();
        if ty == Some("thinking") {
            out.push_str(&Self::sse(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": idx,
                    "delta": {
                        "type": "signature_delta",
                        "signature": fake_thinking_signature(&self.current_thinking_text),
                    }
                }),
            ));
            self.current_thinking_text.clear();
        }
        self.block_started = false;
        self.current_block_type = None;
        out.push_str(&Self::sse(
            "content_block_stop",
            json!({ "type": "content_block_stop", "index": idx }),
        ));
        out
    }

    fn start_block(&mut self, ty: &'static str, content_block: Value) -> String {
        if self.block_started && self.current_block_type == Some(ty) {
            return String::new();
        }
        let mut out = self.close_block();
        self.current_block_index = self.next_block_index as i64;
        self.next_block_index += 1;
        self.current_block_type = Some(ty);
        self.block_started = true;
        out.push_str(&Self::sse(
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": self.current_block_index,
                "content_block": content_block,
            }),
        ));
        out
    }

    pub fn parse_line(&mut self, line: &str) -> Vec<String> {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "[DONE]" {
            return Vec::new();
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            return Vec::new();
        };
        let Some(ty) = event.get("type").and_then(|t| t.as_str()) else {
            return Vec::new();
        };
        self.last_cc_event = ty.to_string();
        let mut out: Vec<String> = Vec::new();

        match ty {
            "start" | "start-step" | "text-start" | "reasoning-start" => {}

            "reasoning-delta" => {
                // CC reasoning → Anthropic thinking block（Claude Code 会显示成思考）
                let text = event_text(&event);
                if text.is_empty() {
                    return out;
                }
                let start = self.start_block("thinking", json!({ "type": "thinking", "thinking": "" }));
                self.current_thinking_text.push_str(&text);
                out.push(start);
                out.push(Self::sse(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": self.current_block_index,
                        "delta": { "type": "thinking_delta", "thinking": text },
                    }),
                ));
            }

            "text-delta" => {
                let text = event_text(&event);
                let start = self.start_block("text", json!({ "type": "text", "text": "" }));
                out.push(start);
                out.push(Self::sse(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": self.current_block_index,
                        "delta": { "type": "text_delta", "text": text },
                    }),
                ));
                self.output_tokens += 1;
            }

            "tool-call" => {
                let close = self.close_block();
                if !close.is_empty() {
                    out.push(close);
                }
                let id = event
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("toolu_{}", &random_uuid().replace('-', "")[..12]));
                let name = event.get("toolName").and_then(|v| v.as_str()).unwrap_or("");
                let input = tool_arguments(&event);
                let tc_index = self.next_block_index as i64;
                self.next_block_index += 1;
                out.push(Self::sse(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": tc_index,
                        "content_block": { "type": "tool_use", "id": id, "name": name, "input": {} },
                    }),
                ));
                out.push(Self::sse(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": tc_index,
                        "delta": { "type": "input_json_delta", "partial_json": input },
                    }),
                ));
                out.push(Self::sse(
                    "content_block_stop",
                    json!({ "type": "content_block_stop", "index": tc_index }),
                ));
                self.output_tokens += 20;
            }

            "finish-step" | "finish" => {
                // 上游的 finishReason 是 'tool-calls'（连字符），必须先过 mapFinishReason 规范化成
                // 'tool_calls'，否则会掉进 mapAnthropicStopReason 的 default 变成 end_turn。
                self.saw_finish = true;
                if let Some(fr) = event_finish_reason(&event) {
                    let norm = map_finish_reason(&fr);
                    self.stop_reason = Some(map_anthropic_stop_reason(&norm).to_string());
                    self.finish_norm = Some(norm);
                }
                let u = event
                    .get("totalUsage")
                    .filter(|v| v.is_object())
                    .or_else(|| event.get("usage").filter(|v| v.is_object()))
                    .cloned();
                if let Some(mut u) = u {
                    normalize_usage(&mut u);
                    self.input_tokens = nullish_i64(u.get("inputTokens"), self.input_tokens);
                    self.output_tokens = nullish_i64(u.get("outputTokens"), self.output_tokens);
                    self.cached_input_tokens =
                        nullish_i64(u.get("cachedInputTokens"), self.cached_input_tokens);
                    self.cache_write_tokens = nullish_i64(
                        u.get("inputTokenDetails").and_then(|d| d.get("cacheWriteTokens")),
                        self.cache_write_tokens,
                    );
                    if let Some(n) = nullish_num(
                        u.get("inputTokenDetails").and_then(|d| d.get("noCacheTokens")),
                    ) {
                        self.no_cache_tokens = n;
                    }
                }
                // 上游未回报 usage 时保留本地按 delta 计数的估算值 —— 清零会把有内容的
                // 响应误判成零输出（触发 429）。
            }

            "error" => {
                self.has_error = true;
                let mapped = map_cc_event_error(&event);
                let err = mapped.body.get("error").cloned().unwrap_or_else(|| json!({}));
                out.push(Self::sse("error", json!({ "type": "error", "error": err })));
                self.upstream_error = Some(mapped);
            }

            "reasoning-end" | "provider-metadata" | "tool-input-start" | "tool-input-delta"
            | "tool-input-end" | "tool-error" | "text-end" => {}

            other => {
                crate::log_warn!("Unknown CC event type", { "type": other });
            }
        }

        out
    }

    /// 流结束后的收尾：关闭挂起的块，发 message_delta + message_stop（或 error 事件）。
    pub fn finalize(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        if self.has_error {
            return out;
        }
        let close = self.close_block();
        if !close.is_empty() {
            out.push(close);
        }

        // 上游没有正常走完 finish（无 finish 事件 / provider 报连接失败）：
        // 绝不能补一个 end_turn 就 message_stop —— 那等于把截断谎报成完整回答。
        if let Some(detail) = incomplete_upstream_detail(self.saw_finish, self.finish_norm.as_deref()) {
            crate::log_warn!("Upstream stream incomplete", { "path": "/v1/messages", "reason": detail });
            let err = incomplete_upstream_error(detail);
            out.push(Self::sse(
                "error",
                json!({ "type": "error", "error": err.body.get("error").cloned().unwrap_or(json!({})) }),
            ));
            return out;
        }

        if self.output_tokens == 0 {
            out.push(Self::sse(
                "error",
                json!({
                    "type": "error",
                    "error": {
                        "type": "rate_limit_error",
                        "message": "Empty response from upstream (zero output tokens)",
                    },
                    "retry_after": 10,
                }),
            ));
            return out;
        }

        let input_tokens = if self.no_cache_tokens >= 0 {
            self.no_cache_tokens
        } else {
            (self.input_tokens - self.cached_input_tokens - self.cache_write_tokens).max(0)
        };
        out.push(Self::sse(
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": self.stop_reason.clone().unwrap_or_else(|| "end_turn".into()) },
                "usage": {
                    "output_tokens": self.output_tokens,
                    "cache_read_input_tokens": self.cached_input_tokens,
                    "cache_creation_input_tokens": self.cache_write_tokens,
                    "input_tokens": input_tokens,
                },
            }),
        ));
        out.push(Self::sse("message_stop", json!({ "type": "message_stop" })));
        out
    }
}

// ══════════════════════════════════════════════════════════════
// 3) Anthropic 请求 → OpenAI（再交给 buildCcRequest）
// ══════════════════════════════════════════════════════════════

pub fn convert_anthropic_to_openai(anthropic_req: &Value) -> Value {
    let mut system_prompt = String::new();
    let mut system_blocks: Option<Vec<Value>> = None;
    if let Some(sys) = anthropic_req.get("system") {
        match sys {
            Value::String(s) => system_prompt = s.clone(),
            Value::Array(arr) => {
                // 保留 cache_control：buildCcRequest 需要块数组才能把断点下发
                let blocks: Vec<Value> = arr
                    .iter()
                    .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .map(|b| {
                        let mut blk = Map::new();
                        blk.insert("type".into(), json!("text"));
                        blk.insert("text".into(), json!(b.get("text").and_then(|t| t.as_str()).unwrap_or("")));
                        if let Some(cc) = b.get("cache_control").filter(|c| !c.is_null()) {
                            blk.insert("cache_control".into(), cc.clone());
                        }
                        Value::Object(blk)
                    })
                    .collect();
                system_prompt = blocks
                    .iter()
                    .map(|b| b.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                system_blocks = Some(blocks);
            }
            _ => {}
        }
    }

    let mut tool_name_from_id: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut openai_messages: Vec<Value> = Vec::new();

    if !system_prompt.is_empty() {
        match &system_blocks {
            Some(blocks) if !blocks.is_empty() => {
                openai_messages.push(json!({ "role": "system", "content": blocks }));
            }
            _ => {
                openai_messages.push(json!({ "role": "system", "content": system_prompt }));
            }
        }
    }

    let empty: Vec<Value> = Vec::new();
    let messages = anthropic_req
        .get("messages")
        .and_then(|m| m.as_array())
        .unwrap_or(&empty);

    for msg in messages {
        match msg.get("role").and_then(|r| r.as_str()).unwrap_or("") {
            "assistant" => {
                let mut text_content = String::new();
                let mut thinking_content = String::new();
                let mut text_parts: Vec<Value> = Vec::new();
                let mut text_has_cache = false;
                let mut tool_calls: Vec<Value> = Vec::new();
                let blocks: Vec<Value> = match msg.get("content") {
                    Some(Value::Array(a)) => a.clone(),
                    other => vec![json!({
                        "type": "text",
                        "text": other.and_then(|v| v.as_str()).unwrap_or(""),
                    })],
                };
                for block in &blocks {
                    match block.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            let t = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                            text_content.push_str(t);
                            let mut part = Map::new();
                            part.insert("type".into(), json!("text"));
                            part.insert("text".into(), json!(t));
                            if let Some(cc) = block.get("cache_control").filter(|c| !c.is_null()) {
                                part.insert("cache_control".into(), cc.clone());
                                text_has_cache = true;
                            }
                            text_parts.push(Value::Object(part));
                        }
                        Some("thinking") => {
                            thinking_content.push_str(
                                block.get("thinking").and_then(|v| v.as_str()).unwrap_or(""),
                            );
                        }
                        Some("tool_use") => {
                            let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            tool_name_from_id.insert(id.clone(), name.clone());
                            tool_calls.push(json!({
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": json_string(block.get("input").unwrap_or(&json!({}))),
                                },
                            }));
                        }
                        _ => {}
                    }
                }
                let content = if text_parts.len() > 1 || text_has_cache {
                    Value::Array(text_parts)
                } else if text_content.is_empty() {
                    Value::Null
                } else {
                    Value::String(text_content)
                };
                let mut assistant_msg = Map::new();
                assistant_msg.insert("role".into(), json!("assistant"));
                assistant_msg.insert("content".into(), content);
                if !thinking_content.is_empty() {
                    assistant_msg.insert("reasoning_content".into(), json!(thinking_content));
                }
                if !tool_calls.is_empty() {
                    assistant_msg.insert("tool_calls".into(), Value::Array(tool_calls));
                }
                openai_messages.push(Value::Object(assistant_msg));
            }

            "user" => {
                let mut text_content = String::new();
                let mut parts: Vec<Value> = Vec::new();
                let mut text_has_cache = false;
                let mut tool_results: Vec<Value> = Vec::new();
                match msg.get("content") {
                    Some(Value::String(s)) => text_content = s.clone(),
                    Some(Value::Array(arr)) => {
                        for block in arr {
                            match block.get("type").and_then(|t| t.as_str()) {
                                Some("text") => {
                                    let t = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                                    text_content.push_str(t);
                                    let mut part = Map::new();
                                    part.insert("type".into(), json!("text"));
                                    part.insert("text".into(), json!(t));
                                    if let Some(cc) = block.get("cache_control").filter(|c| !c.is_null()) {
                                        part.insert("cache_control".into(), cc.clone());
                                        text_has_cache = true;
                                    }
                                    parts.push(Value::Object(part));
                                }
                                Some("image") => {
                                    // Anthropic 图片块：source.type=base64 + media_type + data，或 source.url
                                    let s = block.get("source").cloned().unwrap_or_else(|| json!({}));
                                    let url = if s.get("type").and_then(|t| t.as_str()) == Some("base64")
                                        && s.get("data").and_then(|d| d.as_str()).is_some_and(|d| !d.is_empty())
                                    {
                                        format!(
                                            "data:{};base64,{}",
                                            s.get("media_type").and_then(|m| m.as_str()).unwrap_or("image/png"),
                                            s.get("data").and_then(|d| d.as_str()).unwrap_or("")
                                        )
                                    } else {
                                        s.get("url").and_then(|u| u.as_str()).unwrap_or("").to_string()
                                    };
                                    if !url.is_empty() {
                                        parts.push(json!({ "type": "image_url", "image_url": { "url": url } }));
                                    }
                                }
                                Some("tool_result") => tool_results.push(block.clone()),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }

                // tool_result 优先入队：OpenAI 语义要求 tool 消息紧跟 assistant 的 tool_calls
                for tr in &tool_results {
                    let tool_content = match tr.get("content") {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Array(a)) => a
                            .iter()
                            .map(|c| c.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string())
                            .collect::<Vec<_>>()
                            .join("\n"),
                        Some(Value::Null) | None => String::new(),
                        Some(other) => other.to_string(),
                    };
                    // 会话恢复等场景下 tool_use_id 可能找不到对应 assistant tool_use（历史被客户端裁剪），
                    // 此时不硬塞空 name，避免 CC 上游报 "Tool result is missing"
                    let mut tool_msg = Map::new();
                    tool_msg.insert("role".into(), json!("tool"));
                    tool_msg.insert(
                        "tool_call_id".into(),
                        tr.get("tool_use_id").cloned().unwrap_or(Value::Null),
                    );
                    tool_msg.insert("content".into(), json!(tool_content));
                    if let Some(id) = tr.get("tool_use_id").and_then(|v| v.as_str()) {
                        if let Some(name) = tool_name_from_id.get(id) {
                            tool_msg.insert("name".into(), json!(name));
                        }
                    }
                    openai_messages.push(Value::Object(tool_msg));
                }

                if !parts.is_empty() || !text_content.is_empty() {
                    // 单块纯文本仍用字符串（线格不变）；多块 / 带断点 / 含图片时用块数组
                    let single_text = parts.len() <= 1
                        && (parts.is_empty()
                            || parts[0].get("type").and_then(|t| t.as_str()) == Some("text"))
                        && !text_has_cache;
                    let content = if single_text {
                        Value::String(text_content)
                    } else {
                        Value::Array(parts)
                    };
                    openai_messages.push(json!({ "role": "user", "content": content }));
                }
            }

            _ => {}
        }
    }

    let mut openai_req = Map::new();
    openai_req.insert(
        "model".into(),
        json!(anthropic_req
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("deepseek/deepseek-v4-flash")),
    );
    openai_req.insert("messages".into(), Value::Array(openai_messages));
    openai_req.insert(
        "max_tokens".into(),
        json!(nullish_i64(anthropic_req.get("max_tokens"), 64_000)),
    );
    openai_req.insert(
        "stream".into(),
        json!(anthropic_req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false)),
    );

    if let Some(tools) = anthropic_req.get("tools").and_then(|t| t.as_array()) {
        if !tools.is_empty() {
            let mapped: Vec<Value> = tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": t.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                            "description": t.get("description").and_then(|d| d.as_str()).unwrap_or(""),
                            "parameters": t.get("input_schema").cloned().unwrap_or_else(|| json!({ "type": "object", "properties": {} })),
                        },
                    })
                })
                .collect();
            openai_req.insert("tools".into(), Value::Array(mapped));
        }
    }

    if let Some(tc) = anthropic_req.get("tool_choice") {
        let mapped = match tc.get("type").and_then(|t| t.as_str()) {
            None | Some("auto") => Some(json!("auto")),
            Some("any") => Some(json!("required")),
            Some("none") => Some(json!("none")),
            Some("tool") => Some(json!({
                "type": "function",
                "function": { "name": tc.get("name").cloned().unwrap_or(Value::Null) },
            })),
            _ => None,
        };
        if let Some(m) = mapped {
            openai_req.insert("tool_choice".into(), m);
        }
    }

    if let Some(t) = anthropic_req.get("temperature").filter(|v| !v.is_null()) {
        openai_req.insert("temperature".into(), t.clone());
    }
    if let Some(t) = anthropic_req.get("top_p").filter(|v| !v.is_null()) {
        openai_req.insert("top_p".into(), t.clone());
    }
    if let Some(s) = anthropic_req.get("stop_sequences").filter(|v| !v.is_null()) {
        openai_req.insert("stop".into(), s.clone());
    }
    if let Some(u) = anthropic_req
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .filter(|v| !v.is_null())
    {
        openai_req.insert("user".into(), u.clone());
    }

    // Anthropic thinking → reasoning_effort（LiteLLM 标准映射）
    if let Some(t) = anthropic_req.get("thinking") {
        match t.get("type").and_then(|x| x.as_str()) {
            Some("disabled") | Some("none") => {}
            Some("adaptive") => {
                let effort = t
                    .get("effort")
                    .and_then(|e| e.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("medium");
                openai_req.insert("reasoning_effort".into(), json!(effort));
            }
            _ => {
                if let Some(budget) = nullish_num(t.get("budget_tokens")) {
                    let effort = if budget >= 10_000 {
                        "high"
                    } else if budget >= 5_000 {
                        "medium"
                    } else {
                        "low"
                    };
                    openai_req.insert("reasoning_effort".into(), json!(effort));
                }
            }
        }
    }

    Value::Object(openai_req)
}

/// 非流式 Anthropic 响应构造。
pub fn build_anthropic_response(
    model: &str,
    full_text: &str,
    tool_calls: Option<&Vec<Value>>,
    finish_reason: &str,
    usage: Option<&Value>,
    thinking_text: &str,
) -> Value {
    let mut content: Vec<Value> = Vec::new();
    if !thinking_text.is_empty() {
        content.push(json!({
            "type": "thinking",
            "thinking": thinking_text,
            "signature": fake_thinking_signature(thinking_text),
        }));
    }
    if !full_text.is_empty() {
        content.push(json!({ "type": "text", "text": full_text }));
    }
    if let Some(tcs) = tool_calls {
        for tc in tcs {
            let args = tc
                .get("function")
                .and_then(|f| f.get("arguments"))
                .and_then(|a| a.as_str())
                .unwrap_or("{}");
            let input = try_parse_json(args);
            content.push(json!({
                "type": "tool_use",
                "id": tc.get("id").cloned().unwrap_or(Value::Null),
                "name": tc.get("function").and_then(|f| f.get("name")).cloned().unwrap_or(Value::Null),
                "input": input,
            }));
        }
    }

    let mut u = usage.cloned().unwrap_or_else(|| json!({}));
    if !u.is_object() {
        u = json!({});
    }
    normalize_usage(&mut u);

    // CC 未回报 usage 时按内容长度估算输出 token，避免客户端展示/记账为 0
    let est_out = (((full_text.chars().count() + thinking_text.chars().count()) as f64 / 4.0).ceil()
        as i64
        + tool_calls.map(|t| t.len() as i64 * 20).unwrap_or(0))
    .max(1);
    let output_tokens = nullish_i64(u.get("outputTokens"), 0);
    let output_tokens = if output_tokens == 0 { est_out } else { output_tokens };

    json!({
        "id": format!("msg_{}", &random_uuid().replace('-', "")[..12]),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": map_anthropic_stop_reason(finish_reason),
        "stop_sequence": null,
        "usage": {
            "input_tokens": anthropic_input_tokens(Some(&u), None),
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": nullish_i64(
                u.get("inputTokenDetails").and_then(|d| d.get("cacheWriteTokens")), 0),
            "cache_read_input_tokens": nullish_i64(u.get("cachedInputTokens"), 0),
        },
    })
}

// ══════════════════════════════════════════════════════════════
// 4) OpenAI Responses API（/v1/responses）
// ══════════════════════════════════════════════════════════════

pub fn new_responses_id(prefix: &str) -> String {
    let compact = random_uuid().replace('-', "");
    format!("{prefix}{}", &compact[..24])
}

fn responses_text_of(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .map(|p| p.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string())
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn responses_reasoning_of(item: &Value) -> String {
    let join = |key: &str| -> String {
        item.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|p| p.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default()
    };
    if item.get("summary").and_then(|s| s.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
        return join("summary");
    }
    if item.get("content").and_then(|s| s.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
        return join("content");
    }
    item.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string()
}

/// Responses 请求 → 内部 Chat 请求。
pub fn convert_responses_to_chat(resp_req: &Value) -> Value {
    let mut messages: Vec<Value> = Vec::new();

    if let Some(instr) = resp_req.get("instructions").filter(|v| !v.is_null()) {
        let sys = responses_text_of(Some(instr));
        if !sys.is_empty() {
            messages.push(json!({ "role": "system", "content": sys }));
        }
    }

    // Responses 把 reasoning / message / function_call 拆成并列 item，
    // Chat 要求它们挂在同一条 assistant 消息上，故先累积再冲刷。
    let mut pending: Option<Map<String, Value>> = None;

    fn flush(pending: &mut Option<Map<String, Value>>, messages: &mut Vec<Value>) {
        let Some(mut p) = pending.take() else { return };
        let has_tool_calls = p
            .get("tool_calls")
            .and_then(|t| t.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if !has_tool_calls {
            p.remove("tool_calls");
        }
        if p.get("reasoning_content").and_then(|r| r.as_str()).map(|s| s.is_empty()).unwrap_or(true) {
            p.remove("reasoning_content");
        }
        if p.get("content").map(|c| c.is_null()).unwrap_or(true) && !has_tool_calls {
            return;
        }
        messages.push(Value::Object(p));
    }

    match resp_req.get("input") {
        Some(Value::String(s)) => {
            messages.push(json!({ "role": "user", "content": s }));
        }
        Some(Value::Array(items)) => {
            for item in items {
                if !item.is_object() {
                    continue;
                }
                // EasyInputMessage 的 required 只有 role 与 content —— type 是可选的。
                // item.type 缺失但有 role 时按 message 处理，否则这类 item 会被静默丢弃。
                let ty = item
                    .get("type")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        if item.get("role").is_some() {
                            Some("message".to_string())
                        } else {
                            None
                        }
                    });
                match ty.as_deref() {
                    Some("reasoning") => {
                        let t = responses_reasoning_of(item);
                        if !t.is_empty() {
                            pending
                                .get_or_insert_with(|| {
                                    let mut m = Map::new();
                                    m.insert("role".into(), json!("assistant"));
                                    m.insert("content".into(), Value::Null);
                                    m
                                })
                                .insert("reasoning_content".into(), json!(t));
                        }
                    }
                    Some("message") => {
                        let text = responses_text_of(item.get("content"));
                        match item.get("role").and_then(|r| r.as_str()) {
                            Some("assistant") => {
                                if !text.is_empty() {
                                    pending
                                        .get_or_insert_with(|| {
                                            let mut m = Map::new();
                                            m.insert("role".into(), json!("assistant"));
                                            m.insert("content".into(), Value::Null);
                                            m
                                        })
                                        .insert("content".into(), json!(text));
                                }
                            }
                            Some("system") | Some("developer") => {
                                flush(&mut pending, &mut messages);
                                messages.push(json!({ "role": "system", "content": text }));
                            }
                            _ => {
                                flush(&mut pending, &mut messages);
                                messages.push(json!({ "role": "user", "content": text }));
                            }
                        }
                    }
                    Some("function_call") => {
                        let id = item
                            .get("call_id")
                            .or_else(|| item.get("id"))
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("call_{}", &random_uuid().replace('-', "")[..8]));
                        let entry = json!({
                            "id": id,
                            "type": "function",
                            "function": {
                                "name": item.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                                "arguments": item.get("arguments").and_then(|a| a.as_str()).unwrap_or("{}"),
                            },
                        });
                        let p = pending.get_or_insert_with(|| {
                            let mut m = Map::new();
                            m.insert("role".into(), json!("assistant"));
                            m.insert("content".into(), Value::Null);
                            m
                        });
                        match p.get_mut("tool_calls").and_then(|t| t.as_array_mut()) {
                            Some(arr) => arr.push(entry),
                            None => {
                                p.insert("tool_calls".into(), json!([entry]));
                            }
                        }
                    }
                    Some("function_call_output") => {
                        flush(&mut pending, &mut messages);
                        let output = item.get("output");
                        let content = match output {
                            Some(Value::String(s)) => s.clone(),
                            Some(Value::Null) | None => String::new(),
                            Some(other) => json_string(other),
                        };
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": item.get("call_id").and_then(|c| c.as_str()).unwrap_or(""),
                            "content": content,
                        }));
                    }
                    other => {
                        crate::log_warn!("Unknown Responses input item type", { "type": other });
                    }
                }
            }
        }
        _ => {}
    }
    flush(&mut pending, &mut messages);

    let mut tools: Option<Vec<Value>> = None;
    if let Some(arr) = resp_req.get("tools").and_then(|t| t.as_array()) {
        if !arr.is_empty() {
            let filtered: Vec<Value> = arr
                .iter()
                .filter(|t| {
                    t.get("type").and_then(|x| x.as_str()) == Some("function") || t.get("name").is_some()
                })
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": t.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                            "description": t.get("description").and_then(|d| d.as_str()).unwrap_or(""),
                            "parameters": t.get("parameters").cloned().unwrap_or_else(|| json!({ "type": "object", "properties": {} })),
                        },
                    })
                })
                .collect();
            if !filtered.is_empty() {
                tools = Some(filtered);
            }
        }
    }

    let mut out = Map::new();
    out.insert(
        "model".into(),
        resp_req.get("model").cloned().unwrap_or(Value::Null),
    );
    out.insert("messages".into(), Value::Array(messages));
    out.insert(
        "stream".into(),
        json!(resp_req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false)),
    );
    if let Some(t) = tools {
        out.insert("tools".into(), Value::Array(t));
    }
    if let Some(tc) = resp_req.get("tool_choice") {
        match tc {
            Value::String(_) => {
                out.insert("tool_choice".into(), tc.clone());
            }
            Value::Object(_) => {
                if let Some(name) = tc.get("name").and_then(|n| n.as_str()) {
                    out.insert(
                        "tool_choice".into(),
                        json!({ "type": "function", "function": { "name": name } }),
                    );
                }
            }
            _ => {}
        }
    }
    if let Some(v) = resp_req.get("max_output_tokens").filter(|v| !v.is_null()) {
        out.insert("max_tokens".into(), v.clone());
    }
    if let Some(v) = resp_req.get("temperature").filter(|v| !v.is_null()) {
        out.insert("temperature".into(), v.clone());
    }
    if let Some(v) = resp_req.get("top_p").filter(|v| !v.is_null()) {
        out.insert("top_p".into(), v.clone());
    }
    if let Some(v) = resp_req.get("parallel_tool_calls").filter(|v| !v.is_null()) {
        out.insert("parallel_tool_calls".into(), v.clone());
    }
    if let Some(eff) = resp_req
        .get("reasoning")
        .filter(|r| r.is_object())
        .and_then(|r| r.get("effort"))
        .filter(|v| v.as_str().map(|s| !s.is_empty()).unwrap_or(false))
    {
        out.insert("reasoning_effort".into(), eff.clone());
    }
    Value::Object(out)
}

/// Responses 的 input_tokens 是总数，cached / cache_write 均为其子集 ——
/// 与 Anthropic 相反（那里 cache_read 是独立增量，必须做减法）。此处直接沿用、不做减法。
pub fn build_responses_usage(usage: Option<&Value>, fallback_output_tokens: i64) -> Value {
    let mut u = usage.cloned().unwrap_or_else(|| json!({}));
    if !u.is_object() {
        u = json!({});
    }
    normalize_usage(&mut u);
    let in_tok = nullish_i64(u.get("inputTokens"), 0);
    let out_tok = {
        let v = nullish_i64(u.get("outputTokens"), 0);
        if v == 0 {
            fallback_output_tokens
        } else {
            v
        }
    };
    json!({
        "input_tokens": in_tok,
        "input_tokens_details": {
            "cached_tokens": nullish_i64(u.get("cachedInputTokens"), 0),
            "cache_write_tokens": nullish_i64(
                u.get("inputTokenDetails").and_then(|d| d.get("cacheWriteTokens")), 0),
        },
        "output_tokens": out_tok,
        "output_tokens_details": { "reasoning_tokens": 0 },
        "total_tokens": in_tok + out_tok,
    })
}

pub fn build_responses_output(
    full_text: &str,
    thinking_text: &str,
    tool_calls: Option<&Vec<Value>>,
) -> Vec<Value> {
    let mut output: Vec<Value> = Vec::new();
    if !thinking_text.is_empty() {
        output.push(json!({
            "type": "reasoning",
            "id": new_responses_id("rs_"),
            "summary": [{ "type": "summary_text", "text": thinking_text }],
        }));
    }
    if !full_text.is_empty() {
        output.push(json!({
            "type": "message",
            "id": new_responses_id("msg_"),
            "status": "completed",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": full_text, "annotations": [] }],
        }));
    }
    if let Some(tcs) = tool_calls {
        for tc in tcs {
            let raw_args = tc
                .get("function")
                .and_then(|f| f.get("arguments"))
                .cloned()
                .unwrap_or_else(|| json!("{}"));
            let arguments = match raw_args {
                Value::String(s) => s,
                other => json_string(&other),
            };
            output.push(json!({
                "type": "function_call",
                "id": new_responses_id("fc_"),
                "call_id": tc.get("id").cloned().unwrap_or(Value::Null),
                "name": tc.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()).unwrap_or(""),
                "arguments": arguments,
                "status": "completed",
            }));
        }
    }
    output
}

#[allow(clippy::too_many_arguments)]
pub fn build_responses_object(
    response_id: &str,
    model: &str,
    created: i64,
    full_text: &str,
    thinking_text: &str,
    tool_calls: Option<&Vec<Value>>,
    usage: Option<&Value>,
    opts: &Value,
) -> Value {
    let finish_reason = opts.get("finishReason").and_then(|v| v.as_str()).unwrap_or("");
    let truncated = finish_reason == "length";
    let paused = finish_reason == "pause_turn";
    let get = |k: &str| opts.get(k).cloned().unwrap_or(Value::Null);
    let max_output_tokens = match opts.get("max_output_tokens") {
        Some(Value::Null) | None => Value::Null,
        Some(v) => v.clone(),
    };
    let temperature = match opts.get("temperature") {
        Some(Value::Null) | None => json!(1),
        Some(v) => v.clone(),
    };
    let top_p = match opts.get("top_p") {
        Some(Value::Null) | None => json!(1),
        Some(v) => v.clone(),
    };
    json!({
        "id": response_id,
        "object": "response",
        "created_at": created,
        "status": if truncated || paused { "incomplete" } else { "completed" },
        "completed_at": crate::util::now_unix(),
        "error": null,
        "incomplete_details": if truncated {
            json!({ "reason": "max_output_tokens" })
        } else if paused {
            json!({ "reason": "pause_turn" })
        } else {
            Value::Null
        },
        "input": opts.get("input").cloned().unwrap_or_else(|| json!([])),
        "instructions": opts.get("instructions").cloned().unwrap_or(Value::Null),
        "max_output_tokens": max_output_tokens,
        "model": model,
        "output": build_responses_output(full_text, thinking_text, tool_calls),
        "output_text": full_text,
        "parallel_tool_calls": true,
        "previous_response_id": null,
        "reasoning": get("reasoning"),
        "store": false,
        "temperature": temperature,
        "text": { "format": { "type": "text" } },
        "tool_choice": opts.get("tool_choice").cloned().unwrap_or_else(|| json!("auto")),
        "tools": opts.get("tools").cloned().unwrap_or_else(|| json!([])),
        "top_p": top_p,
        "truncation": "disabled",
        "usage": build_responses_usage(usage, 0),
        "user": null,
        "metadata": {},
    })
}

// ══════════════════════════════════════════════════════════════
// 5) CC NDJSON → OpenAI Responses 具名 SSE 事件
// ══════════════════════════════════════════════════════════════

#[derive(PartialEq, Clone, Copy)]
enum ItemKind {
    Message,
    FunctionCall,
    Reasoning,
}

struct CurrentItem {
    kind: ItemKind,
    index: usize,
    item: Value,
    text_buf: String,
}

pub struct ResponsesSseTranslator {
    model: String,
    response_id: String,
    created: i64,
    seq: u64,
    created_sent: bool,
    current: Option<CurrentItem>,
    output_index: usize,
    done_items: Vec<Value>,
    usage: Option<Value>,
    text_acc: String,
    finish_reason: Option<String>,
    saw_finish: bool,
    pub last_cc_event: String,
    pub upstream_error: Option<MappedError>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_input_tokens: i64,
}

impl ResponsesSseTranslator {
    pub fn new(model: &str, response_id: &str, created: i64) -> Self {
        Self {
            model: model.to_string(),
            response_id: response_id.to_string(),
            created,
            seq: 0,
            created_sent: false,
            current: None,
            output_index: 0,
            done_items: Vec::new(),
            usage: None,
            text_acc: String::new(),
            finish_reason: None,
            saw_finish: false,
            last_cc_event: String::new(),
            upstream_error: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
        }
    }

    pub fn started(&self) -> bool {
        self.created_sent
    }

    fn sse(&mut self, ty: &str, data: Value) -> String {
        let mut m = Map::new();
        m.insert("type".into(), json!(ty));
        m.insert("sequence_number".into(), json!(self.seq));
        self.seq += 1;
        if let Value::Object(d) = data {
            for (k, v) in d {
                m.insert(k, v);
            }
        }
        format!("event: {ty}\ndata: {}\n\n", json_string(&Value::Object(m)))
    }

    fn base_response(&self, status: &str, output: Option<Vec<Value>>) -> Value {
        json!({
            "id": self.response_id,
            "object": "response",
            "created_at": self.created,
            "status": status,
            "output": output.unwrap_or_default(),
            "output_text": "",
            "model": self.model,
            "error": null,
            "incomplete_details": null,
            "parallel_tool_calls": true,
            "previous_response_id": null,
            "store": false,
            "tools": [],
            "metadata": {},
        })
    }

    fn start_response(&mut self) -> Vec<String> {
        self.created_sent = true;
        let a = self.sse("response.created", json!({ "response": self.base_response("in_progress", None) }));
        let b = self.sse("response.in_progress", json!({ "response": self.base_response("in_progress", None) }));
        vec![a, b]
    }

    fn close_item(&mut self) -> Vec<String> {
        let Some(cur) = self.current.take() else {
            return Vec::new();
        };
        let mut out: Vec<String> = Vec::new();
        let idx = cur.index;
        let mut item = cur.item;
        match cur.kind {
            ItemKind::Message => {
                out.push(self.sse(
                    "response.output_text.done",
                    json!({
                        "item_id": item.get("id"), "output_index": idx, "content_index": 0,
                        "text": cur.text_buf, "logprobs": [],
                    }),
                ));
                out.push(self.sse(
                    "response.content_part.done",
                    json!({
                        "item_id": item.get("id"), "output_index": idx, "content_index": 0,
                        "part": { "type": "output_text", "text": cur.text_buf, "annotations": [] },
                    }),
                ));
                item["content"] = json!([{ "type": "output_text", "text": cur.text_buf, "annotations": [] }]);
                item["status"] = json!("completed");
            }
            ItemKind::FunctionCall => {
                out.push(self.sse(
                    "response.function_call_arguments.done",
                    json!({
                        "item_id": item.get("id"), "output_index": idx,
                        "arguments": item.get("arguments"),
                    }),
                ));
                item["status"] = json!("completed");
            }
            ItemKind::Reasoning => {
                out.push(self.sse(
                    "response.reasoning_summary_text.done",
                    json!({
                        "item_id": item.get("id"), "output_index": idx, "summary_index": 0,
                        "text": cur.text_buf,
                    }),
                ));
                out.push(self.sse(
                    "response.reasoning_summary_part.done",
                    json!({
                        "item_id": item.get("id"), "output_index": idx, "summary_index": 0,
                        "part": { "type": "summary_text", "text": cur.text_buf },
                    }),
                ));
                item["summary"] = json!([{ "type": "summary_text", "text": cur.text_buf }]);
                item["status"] = json!("completed");
            }
        }
        out.push(self.sse("response.output_item.done", json!({ "output_index": idx, "item": item })));
        self.done_items.push(item);
        out
    }

    fn open_item(&mut self, kind: ItemKind, item: Value) -> Vec<String> {
        let mut out = self.close_item();
        let index = self.output_index;
        self.output_index += 1;
        let item_id = item.get("id").cloned().unwrap_or(Value::Null);
        self.current = Some(CurrentItem {
            kind,
            index,
            item,
            text_buf: String::new(),
        });
        let added_item = self.current.as_ref().unwrap().item.clone();
        out.push(self.sse(
            "response.output_item.added",
            json!({ "output_index": index, "item": added_item }),
        ));
        match kind {
            ItemKind::Message => out.push(self.sse(
                "response.content_part.added",
                json!({
                    "item_id": item_id, "output_index": index, "content_index": 0,
                    "part": { "type": "output_text", "text": "", "annotations": [] },
                }),
            )),
            ItemKind::Reasoning => out.push(self.sse(
                "response.reasoning_summary_part.added",
                json!({
                    "item_id": item_id, "output_index": index, "summary_index": 0,
                    "part": { "type": "summary_text", "text": "" },
                }),
            )),
            ItemKind::FunctionCall => {}
        }
        out
    }

    pub fn parse_line(&mut self, line: &str) -> Vec<String> {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed == "[DONE]" || trimmed.starts_with(':') {
            return Vec::new();
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            return Vec::new();
        };
        let Some(ty) = event.get("type").and_then(|t| t.as_str()) else {
            return Vec::new();
        };
        self.last_cc_event = ty.to_string();
        let mut out: Vec<String> = Vec::new();

        match ty {
            "text-start" | "reasoning-start" | "start" | "start-step" => {}

            "text-delta" => {
                let text = {
                    let t = event_text(&event);
                    if t.is_empty() {
                        event.get("delta").and_then(|d| d.as_str()).unwrap_or("").to_string()
                    } else {
                        t
                    }
                };
                if text.is_empty() {
                    return out;
                }
                if !self.created_sent {
                    out.extend(self.start_response());
                }
                if self.current.as_ref().map(|c| c.kind) != Some(ItemKind::Message) {
                    out.extend(self.open_item(
                        ItemKind::Message,
                        json!({
                            "type": "message", "id": new_responses_id("msg_"),
                            "status": "in_progress", "role": "assistant", "content": [],
                        }),
                    ));
                }
                let (item_id, index) = {
                    let cur = self.current.as_mut().unwrap();
                    cur.text_buf.push_str(&text);
                    (cur.item.get("id").cloned().unwrap_or(Value::Null), cur.index)
                };
                self.text_acc.push_str(&text);
                out.push(self.sse(
                    "response.output_text.delta",
                    json!({
                        "item_id": item_id, "output_index": index, "content_index": 0,
                        "delta": text, "logprobs": [],
                    }),
                ));
            }

            "reasoning-delta" => {
                let text = event_text(&event);
                if text.is_empty() {
                    return out;
                }
                if !self.created_sent {
                    out.extend(self.start_response());
                }
                if self.current.as_ref().map(|c| c.kind) != Some(ItemKind::Reasoning) {
                    out.extend(self.open_item(
                        ItemKind::Reasoning,
                        json!({
                            "type": "reasoning", "id": new_responses_id("rs_"),
                            "summary": [], "status": "in_progress",
                        }),
                    ));
                }
                let (item_id, index) = {
                    let cur = self.current.as_mut().unwrap();
                    cur.text_buf.push_str(&text);
                    (cur.item.get("id").cloned().unwrap_or(Value::Null), cur.index)
                };
                out.push(self.sse(
                    "response.reasoning_summary_text.delta",
                    json!({
                        "item_id": item_id, "output_index": index, "summary_index": 0, "delta": text,
                    }),
                ));
            }

            "tool-call" => {
                if !self.created_sent {
                    out.extend(self.start_response());
                }
                let call_id = event
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| new_responses_id("call_"));
                let args = tool_arguments(&event);
                out.extend(self.open_item(
                    ItemKind::FunctionCall,
                    json!({
                        "type": "function_call", "id": new_responses_id("fc_"),
                        "call_id": call_id,
                        "name": event.get("toolName").and_then(|v| v.as_str()).unwrap_or(""),
                        "arguments": "", "status": "in_progress",
                    }),
                ));
                let (item_id, index) = {
                    let cur = self.current.as_mut().unwrap();
                    cur.item["arguments"] = json!(args);
                    (cur.item.get("id").cloned().unwrap_or(Value::Null), cur.index)
                };
                out.push(self.sse(
                    "response.function_call_arguments.delta",
                    json!({ "item_id": item_id, "output_index": index, "delta": args }),
                ));
            }

            "finish" => {
                self.saw_finish = true;
                // 必须归一化：截断类不止 'length'（还有 max_output_tokens /
                // model_context_window_exceeded），直接比对原始值会漏判成 completed。
                self.finish_reason = event_finish_reason(&event).map(|fr| map_finish_reason(&fr));
                let u = event
                    .get("totalUsage")
                    .filter(|v| v.is_object())
                    .or_else(|| event.get("usage").filter(|v| v.is_object()))
                    .cloned();
                if let Some(mut u) = u {
                    normalize_usage(&mut u);
                    self.input_tokens = nullish_i64(u.get("inputTokens"), 0);
                    self.output_tokens = nullish_i64(u.get("outputTokens"), 0);
                    self.cached_input_tokens = nullish_i64(u.get("cachedInputTokens"), 0);
                    self.usage = Some(u);
                }
            }

            "error" => {
                self.upstream_error = Some(map_cc_event_error(&event));
            }

            _ => {}
        }

        out
    }

    /// 流正常结束后发 response.completed / incomplete / failed。
    pub fn finish(&mut self) -> Vec<String> {
        if !self.created_sent {
            return Vec::new();
        }
        let mut out = self.close_item();

        // 上游没有正常走完 finish —— 不能报 response.completed（那是把截断谎报成完整）。
        if let Some(detail) = incomplete_upstream_detail(self.saw_finish, self.finish_reason.as_deref()) {
            crate::log_warn!("Upstream stream incomplete", { "path": "/v1/responses", "reason": detail });
            let err = incomplete_upstream_error(detail);
            let mut resp = self.base_response("failed", None);
            resp["error"] = json!({
                "code": "upstream_error",
                "message": err.body.get("error").and_then(|e| e.get("message")).cloned().unwrap_or(Value::Null),
            });
            out.push(self.sse("response.failed", json!({ "response": resp })));
            return out;
        }

        let truncated = self.finish_reason.as_deref() == Some("length");
        let paused = self.finish_reason.as_deref() == Some("pause_turn");
        let status = if truncated || paused { "incomplete" } else { "completed" };
        let mut resp = self.base_response(status, Some(self.done_items.clone()));
        resp["output_text"] = json!(self.text_acc);
        resp["incomplete_details"] = if truncated {
            json!({ "reason": "max_output_tokens" })
        } else if paused {
            json!({ "reason": "pause_turn" })
        } else {
            Value::Null
        };
        let fallback = self.output_tokens;
        resp["usage"] = build_responses_usage(self.usage.as_ref(), fallback);
        let event_name = if truncated || paused {
            "response.incomplete"
        } else {
            "response.completed"
        };
        out.push(self.sse(event_name, json!({ "response": resp })));
        out
    }

    pub fn fail(&mut self, message: &str) -> Vec<String> {
        if !self.created_sent {
            return Vec::new();
        }
        let mut resp = self.base_response("failed", None);
        resp["error"] = json!({
            "code": "upstream_error",
            "message": if message.is_empty() { "Upstream error" } else { message },
        });
        vec![self.sse("response.failed", json!({ "response": resp }))]
    }

    pub fn error_event(&mut self, message: &str) -> String {
        self.sse(
            "error",
            json!({
                "code": null,
                "message": if message.is_empty() { "Upstream error" } else { message },
                "param": null,
            }),
        )
    }
}
