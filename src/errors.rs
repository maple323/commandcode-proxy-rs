//! 错误映射与协议取值规范化。

use crate::util::sha256_bytes;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
pub struct MappedError {
    pub status: u16,
    pub code: Option<Value>,
    pub reported_status: Option<u16>,
    pub body: Value,
}

impl MappedError {
    pub fn error_type(&self) -> String {
        self.body
            .get("error")
            .and_then(|e| e.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("upstream_error")
            .to_string()
    }

    pub fn error_message(&self) -> String {
        self.body
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("Upstream error")
            .to_string()
    }

    pub fn retry_after(&self) -> Option<u64> {
        self.body.get("retry_after").and_then(|v| v.as_u64())
    }
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// 上游状态码 → 本代理状态码 / 错误类型
fn cc_status_map(cc_status: u16) -> (u16, &'static str) {
    match cc_status {
        400 => (400, "invalid_request_error"),
        401 => (401, "authentication_error"),
        402 => (429, "rate_limit_error"), // payment required → rate limit
        403 => (401, "authentication_error"),
        404 => (404, "not_found"),
        422 => (400, "invalid_request_error"),
        429 => (429, "rate_limit_error"),
        500 => (502, "upstream_error"),
        502 => (502, "upstream_error"),
        503 => (503, "temporarily_unavailable"),
        _ => (502, "upstream_error"),
    }
}

fn error_body(message: &str, ty: &str, code: &Option<Value>, retry_after: Option<u64>) -> Value {
    let mut err = Map::new();
    err.insert("message".into(), json!(message));
    err.insert("type".into(), json!(ty));
    if let Some(c) = code {
        if !c.is_null() {
            err.insert("code".into(), c.clone());
        }
    }
    let mut body = Map::new();
    body.insert("error".into(), Value::Object(err));
    if let Some(ra) = retry_after {
        body.insert("retry_after".into(), json!(ra));
    }
    Value::Object(body)
}

/// 上游 HTTP 错误（非 2xx）→ 下游错误体
pub fn map_cc_error(cc_status: u16, cc_body: Option<&str>) -> MappedError {
    let (mapped_status, mapped_type) = cc_status_map(cc_status);
    let mut message = format!("CC API error ({cc_status})");
    let mut code: Option<Value> = None;

    if let Some(text) = cc_body {
        if !text.is_empty() {
            match serde_json::from_str::<Value>(text) {
                Ok(parsed) => {
                    let m = parsed
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .or_else(|| parsed.get("message"));
                    if let Some(m) = m {
                        message = match m {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                    }
                    // 上游错误体：{"success":false,"error":{"code":"BAD_REQUEST"|"USAGE_EXCEEDED",...}}
                    code = parsed
                        .get("error")
                        .and_then(|e| e.get("code"))
                        .or_else(|| parsed.get("code"))
                        .filter(|c| !c.is_null())
                        .cloned();
                }
                Err(_) => {
                    let truncated: String = text.chars().take(200).collect();
                    if !truncated.is_empty() {
                        message = truncated;
                    }
                }
            }
        }
    }

    if cc_status == 429 {
        return MappedError {
            status: 429,
            code: code.clone(),
            reported_status: None,
            body: error_body(&message, "rate_limit_error", &code, Some(30)),
        };
    }

    MappedError {
        status: mapped_status,
        code: code.clone(),
        reported_status: None,
        body: error_body(&message, mapped_type, &code, None),
    }
}

/// 流内 error 事件 → 下游错误体
pub fn map_cc_event_error(event: &Value) -> MappedError {
    let error = event.get("error");
    let message = error
        .and_then(|e| e.get("message"))
        .or_else(|| event.get("message"))
        .map(|m| match m {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| "Unknown CC error".to_string());
    let code = error
        .and_then(|e| e.get("code"))
        .or_else(|| event.get("code"))
        .filter(|c| !c.is_null())
        .cloned();

    // 取值链：parseEmbeddedErrorJSON(message)?.status ?? error.statusCode ?? null
    let reported_status: Option<u16> = embedded_status(&message).or_else(|| {
        let sc = num(error.and_then(|e| e.get("statusCode")))?;
        if sc.fract() == 0.0 && (0.0..=65535.0).contains(&sc) {
            Some(sc as u16)
        } else {
            None
        }
    });
    let cc_status = reported_status.unwrap_or(502);
    let (mapped_status, mapped_type) = cc_status_map(cc_status);

    if mapped_status == 429 {
        return MappedError {
            status: 429,
            code: code.clone(),
            reported_status,
            body: error_body(&message, "rate_limit_error", &code, Some(30)),
        };
    }

    MappedError {
        status: mapped_status,
        code: code.clone(),
        reported_status,
        body: error_body(&message, mapped_type, &code, None),
    }
}

/// 匹配 message 前缀 `<NNN>`
fn embedded_status(message: &str) -> Option<u16> {
    let b = message.as_bytes();
    if b.len() < 5 || b[0] != b'<' || b[4] != b'>' {
        return None;
    }
    if !b[1].is_ascii_digit() || !b[2].is_ascii_digit() || !b[3].is_ascii_digit() {
        return None;
    }
    let n = (b[1] - b'0') as u16 * 100 + (b[2] - b'0') as u16 * 10 + (b[3] - b'0') as u16;
    Some(n)
}

/// 上游「没有正常走完」的两种情形，CLI 都当成可重试的 502。
/// saw_finish 的口径是「上游给过任何完成信号」：终态 finish，以及本代理一直在处理的 finish-step。
pub fn incomplete_upstream_detail(saw_finish: bool, finish_reason: Option<&str>) -> Option<&'static str> {
    if !saw_finish {
        return Some("no finish event");
    }
    if finish_reason == Some("upstream_error") {
        return Some("provider reported an upstream connection failure");
    }
    None
}

pub fn incomplete_upstream_error(detail: &str) -> MappedError {
    MappedError {
        status: 502,
        code: None,
        reported_status: None,
        body: json!({
            "error": {
                "message": format!("Upstream stream ended without a completion finish ({detail}) — response was truncated"),
                "type": "upstream_error",
            },
            "retry_after": 10,
        }),
    }
}

/// 上游 finishReason → 内部规范化取值。
/// 关键点：'length' 家族不止 'length' 一个值。max_output_tokens 与
/// model_context_window_exceeded 都是「输出被截断」，折成 stop/end_turn 等于把半截回答谎报成完整回答。
pub fn map_finish_reason(reason: &str) -> String {
    let r = reason.trim().to_lowercase();
    if r.is_empty() {
        return "stop".to_string();
    }
    if r == "tool-calls" || r == "tool_calls" || r == "tool_use" {
        return "tool_calls".to_string();
    }
    if r == "length" || r == "max_tokens" || r == "max_output_tokens" || r == "model_context_window_exceeded" {
        return "length".to_string();
    }
    if is_network_failure_finish(&r) {
        return "upstream_error".to_string();
    }
    r
}

/// `^(?:network|connection|upstream)[-_\s]?error$`
fn is_network_failure_finish(r: &str) -> bool {
    for prefix in ["network", "connection", "upstream"] {
        if let Some(rest) = r.strip_prefix(prefix) {
            let tail = rest
                .strip_prefix("error")
                .or_else(|| rest.strip_prefix("-error"))
                .or_else(|| rest.strip_prefix("_error"))
                .or_else(|| rest.strip_prefix(" error"));
            if tail == Some("") {
                return true;
            }
        }
    }
    false
}

pub fn map_anthropic_stop_reason(finish_reason: &str) -> &'static str {
    match finish_reason {
        "tool_calls" => "tool_use",
        "length" => "max_tokens",
        "stop" => "end_turn",
        // Anthropic 的原生枚举，必须原样透出：它表示「这一轮被暂停，后面还有内容」。
        "pause_turn" => "pause_turn",
        "refusal" => "refusal",
        _ => "end_turn",
    }
}

/// OpenAI 的 finish_reason 没有 pause_turn：折成 'length' 至少如实表达「输出不完整」，
/// 下游的截断处理会做对的事。
pub fn to_openai_finish_reason(finish_reason: &str) -> &str {
    if finish_reason == "pause_turn" {
        "length"
    } else {
        finish_reason
    }
}

/// CC usage 规范化：outputTokens 为 0 → 全部清零（防误计费）。
pub fn normalize_usage(u: &mut Value) {
    if !u.is_object() {
        return;
    }
    let ot = num(u.get("outputTokens"));
    if ot.map(|v| v == 0.0).unwrap_or(true) {
        u["inputTokens"] = json!(0);
        u["cachedInputTokens"] = json!(0);
    }
}

/// CC 的 inputTokens 是「总数」（含缓存命中），Anthropic 的 input_tokens 只计非缓存部分。
/// 优先采用 inputTokenDetails.noCacheTokens，缺失时回退到减法。
pub fn anthropic_input_tokens(usage: Option<&Value>, no_cache_override: Option<i64>) -> i64 {
    if let Some(v) = no_cache_override {
        if v >= 0 {
            return v;
        }
    }
    let u = match usage {
        Some(u) => u,
        None => return 0,
    };
    let details = u.get("inputTokenDetails");
    if let Some(n) = num(details.and_then(|d| d.get("noCacheTokens"))) {
        if n >= 0.0 {
            return n as i64;
        }
    }
    let cache_read = num(u.get("cachedInputTokens"))
        .or_else(|| num(details.and_then(|d| d.get("cacheReadTokens"))))
        .unwrap_or(0.0);
    let cache_write = num(details.and_then(|d| d.get("cacheWriteTokens"))).unwrap_or(0.0);
    let input = num(u.get("inputTokens")).unwrap_or(0.0);
    (input - cache_read - cache_write).max(0.0) as i64
}

/// 为 thinking 块生成 Claude 形态的假签名。
/// Anthropic 会用密码学校验 thinking 签名，第三方代理无法铸造合法签名；
/// Claude Code 的浅层校验只要求 base64 以 'E' 开头、载荷首字节 0x12 —— 这里满足它。
pub fn fake_thinking_signature(thinking_text: &str) -> String {
    use base64::Engine;
    let seed_src = if thinking_text.is_empty() {
        "dsh-proxy-thinking"
    } else {
        thinking_text
    };
    let seed = sha256_bytes(seed_src.as_bytes());
    let mut raw = Vec::with_capacity(34);
    raw.push(0x12);
    raw.push(seed.len() as u8);
    raw.extend_from_slice(&seed);
    base64::engine::general_purpose::STANDARD.encode(raw)
}
