//! 通用工具：时间、slug、traceparent、API key 提取、JSON 辅助。

use bytes::Bytes;
use rand::Rng;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// `new Date().toISOString().slice(0, 10)` —— UTC 日期。
pub fn date_str_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

pub fn random_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill(&mut buf[..]);
    hex::encode(buf)
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// CLI 的 slug 规则：对完整工作目录做 slugify，空则 "root"，无随机后缀。
pub fn slugify_project_path(p: &str) -> String {
    let lower = p.to_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut last_dash = true; // 前导分隔符会被 trim 掉
    for ch in lower.chars() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "root".to_string()
    } else {
        out
    }
}

pub fn generate_traceparent() -> String {
    format!("00-{}-{}-01", random_hex(16), random_hex(8))
}

/// 从 `user_xxxxx` 形态里提取 key（等价 `/user_[a-zA-Z0-9_-]+/`）。
fn extract_user_key(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut search = 0usize;
    while let Some(rel) = s[search..].find("user_") {
        let start = search + rel;
        let rest_start = start + 5;
        let rest = &s[rest_start..];
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
            .unwrap_or(rest.len());
        if end > 0 {
            return Some(format!("user_{}", &rest[..end]));
        }
        // 该处 `user_` 后面没有合法字符，继续往后找
        search = start + 5;
        if search >= bytes.len() {
            break;
        }
    }
    None
}

/// 优先 `Authorization: Bearer user_...`，回退 `x-api-key`。
pub fn get_api_key(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(rest) = auth.strip_prefix("Bearer ") {
            if let Some(k) = extract_user_key(rest) {
                return Some(k);
            }
        }
    }
    if let Some(x) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        if let Some(k) = extract_user_key(x) {
            return Some(k);
        }
    }
    None
}

pub fn try_parse_json(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| Value::Object(Map::new()))
}

/// JS 真值语义（用于复刻 `a || b || c` 链）。
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// 依次取第一个「真值」参数，全都不满足则返回最后一个（默认值）。
pub fn first_truthy(candidates: &[Value], default: Value) -> Value {
    for c in candidates {
        if truthy(c) {
            return c.clone();
        }
    }
    default
}

/// `String(value ?? '')`
pub fn string_or_empty(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// `value ?? default`（仅 null / 缺失时取默认；数字/数字串会被转换）
pub fn nullish_i64(v: Option<&Value>, default: i64) -> i64 {
    nullish_num(v).unwrap_or(default)
}

/// `typeof value === 'number' ? value : undefined` 的宽松版本
pub fn nullish_num(v: Option<&Value>) -> Option<i64> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => n.as_f64().map(|f| f as i64),
        Some(Value::String(s)) => s.trim().parse::<f64>().ok().map(|f| f as i64),
        Some(_) => None,
    }
}

/// `c?.text ?? c?.content ?? ''`
pub fn nullish_text(c: &Value) -> String {
    let t = c.get("text");
    let chosen = match t {
        Some(Value::Null) | None => c.get("content"),
        Some(v) => Some(v),
    };
    string_or_empty(chosen)
}

/// 把上游错误体摘要成单行，截断到 limit 个字符。
pub fn summarize_upstream_error(text: &str, limit: usize) -> String {
    if text.is_empty() {
        return String::new();
    }
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let count = flat.chars().count();
    if count > limit {
        let head: String = flat.chars().take(limit).collect();
        format!("{head}…({} more)", count - limit)
    } else {
        flat
    }
}

/// `data:image/png;base64,....` → `image/png`
pub fn data_url_media_type(url: &str) -> Option<String> {
    let rest = url.strip_prefix("data:")?;
    let end = rest.find([';', ','])?;
    let mt = &rest[..end];
    if mt.is_empty() {
        None
    } else {
        Some(mt.to_string())
    }
}

pub fn json_bytes(v: &Value) -> Bytes {
    Bytes::from(serde_json::to_vec(v).unwrap_or_else(|_| b"{}".to_vec()))
}

/// 把 `Value` 序列化成字符串（紧凑）。
pub fn json_string(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string())
}
