//! HTTP 层公共工具：受限读体、JSON/SSE 响应构造、下游断连守卫、上游 NDJSON 读取。

use crate::util::{json_bytes, json_string};
use crate::state::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 请求体读取错误
#[derive(Debug)]
pub enum BodyError {
    TooLarge(usize),
    InvalidJson,
}

/// 413 拒绝后转入排空模式：继续读取并丢弃剩余请求体，保持 keep-alive 连接可复用，
/// 让客户端明确收到 413 而不是 Connection reset；但客户端无视 413 持续上传超过
/// DRAIN_LIMIT 则强制停止读取。
const DRAIN_LIMIT: usize = 32 * 1024 * 1024;

pub async fn read_body_limited(body: Body, limit: usize) -> Result<Value, BodyError> {
    let mut stream = body.into_data_stream();
    let mut buf = BytesMut::new();
    let mut too_large = false;
    let mut drained = 0usize;

    while let Some(frame) = stream.next().await {
        let Ok(data) = frame else {
            return Err(BodyError::InvalidJson);
        };
        if too_large {
            drained += data.len();
            if drained > DRAIN_LIMIT {
                break;
            }
        } else if buf.len() + data.len() > limit {
            too_large = true;
            buf.clear();
        } else {
            buf.extend_from_slice(&data);
        }
    }

    if too_large {
        return Err(BodyError::TooLarge(limit));
    }
    serde_json::from_slice::<Value>(&buf).map_err(|_| BodyError::InvalidJson)
}

/// 流式路径的终态错误：只有在「尚未写出任何 SSE 字节」时才走这条路，
/// 由 handler 转成普通 JSON 错误响应，让 SDK 能按可重试错误退避。
#[derive(Debug, Clone)]
pub struct FinalError {
    pub status: u16,
    pub body: Value,
}

impl std::fmt::Display for FinalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream final error {}", self.status)
    }
}

impl std::error::Error for FinalError {}

pub fn json_response(status: u16, body: &Value) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json");
    if let Some(ra) = body.get("retry_after").and_then(|v| v.as_u64()) {
        builder = builder.header("retry-after", ra.to_string());
    }
    builder
        .body(Body::from(json_bytes(body)))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

pub fn text_response(status: u16, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

pub const SSE_HEADERS: [(&str, &str); 4] = [
    ("content-type", "text/event-stream"),
    ("cache-control", "no-cache"),
    ("connection", "keep-alive"),
    ("x-accel-buffering", "no"),
];

/// 把翻译器产出的 SSE 流包成响应。
///
/// 关键行为：**预取第一个 item** 才决定发 SSE 头还是回 JSON 错误 ——
/// 与 JS 版「延迟写 200 header」等价：完全无输出时还能回 JSON 429/502 让 SDK 自动重试。
pub async fn sse_from_stream<S>(stream: S) -> Response
where
    S: Stream<Item = Result<Bytes, FinalError>> + Send + 'static,
{
    let mut pinned = Box::pin(stream);
    match pinned.next().await {
        Some(Ok(first)) => {
            let head = futures_util::stream::once(async move { Ok::<Bytes, FinalError>(first) });
            let mut builder = Response::builder().status(200);
            for (k, v) in SSE_HEADERS {
                builder = builder.header(k, v);
            }
            builder
                .body(Body::from_stream(head.chain(pinned)))
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        Some(Err(e)) => json_response(e.status, &e.body),
        None => json_response(
            502,
            &json!({ "error": { "message": "Upstream error: empty response stream", "type": "proxy_error" } }),
        ),
    }
}

/// 超时提示语：连续超时达到阈值时提醒压缩上下文。
pub fn timeout_message(consecutive: usize) -> String {
    if consecutive >= crate::state::TIMEOUT_REDUCE_CONTEXT_THRESHOLD {
        "Response timeout - try reducing context length (summarize earlier messages)".to_string()
    } else {
        "Response timeout - request timed out".to_string()
    }
}

/// 下游断连守卫。
///
/// hyper 在连接关闭时会 drop 请求处理 future —— 因此「流被 drop 且未走到终态」
/// 就等价于 JS 版 `res.on('close')` 里的 `!res.writableEnded`。
/// Drop 同时会连带 drop 上游 reqwest 响应，从而真正打断 CC 上游、不浪费 token。
pub struct DisconnectGuard {
    pub path: &'static str,
    pub id_key: &'static str,
    pub id: String,
    pub model: String,
    pub start: Instant,
    pub bytes: Arc<AtomicUsize>,
    pub last_event: Arc<Mutex<String>>,
    pub completed: Arc<AtomicBool>,
    pub streaming: bool,
    pub extra: Option<Value>,
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        if self.completed.load(Ordering::Relaxed) {
            return;
        }
        let last = self
            .last_event
            .lock()
            .map(|s| s.clone())
            .unwrap_or_default();
        let last = if last.is_empty() { "(none)".to_string() } else { last };
        let reason = if last.starts_with("tool-input") {
            "tool-input-silent-timeout"
        } else if last.contains("delta") {
            "streaming-active-disconnect"
        } else {
            "client-hangup"
        };
        let mut data = serde_json::Map::new();
        data.insert("path".into(), json!(self.path));
        data.insert(self.id_key.into(), json!(self.id));
        data.insert("model".into(), json!(self.model));
        data.insert("reason".into(), json!(reason));
        data.insert("streaming".into(), json!(self.streaming));
        data.insert("elapsedMs".into(), json!(self.start.elapsed().as_millis() as u64));
        data.insert("bytesReceived".into(), json!(self.bytes.load(Ordering::Relaxed)));
        data.insert("lastCcEvent".into(), json!(last));
        if let Some(Value::Object(extra)) = &self.extra {
            for (k, v) in extra {
                data.insert(k.clone(), v.clone());
            }
        }
        crate::logging::warn("Client disconnected", Some(Value::Object(data)));
    }
}

/// 从缓冲区里切出所有**完整行**（以 `\n` 结尾），追加到 `out`，并把已消费的前缀
/// 从缓冲区移除；末尾不完整的那一段留在缓冲区里等下一个 chunk。
///
/// ⚠️ 必须写 `&buf[from..nl]` 而不是 `&buf[..nl]`。
/// 用 `&buf[..nl]` 时第 1 行恰好正确（from==0），从第 2 行起会把前面所有行
/// 一起拼进来（`line1\nline2`），JSON 解析必然失败 —— 症状是「整条流只认出
/// 第一个事件」，进而误报 "no finish event"。这个 bug 曾在 4 处重复出现，
/// 因此收敛成唯一实现。
pub fn take_lines(buf: &mut Vec<u8>, out: &mut Vec<String>) {
    if !buf.contains(&b'\n') {
        return;
    }
    let mut from = 0usize;
    while let Some(pos) = buf[from..].iter().position(|&b| b == b'\n') {
        let nl = from + pos;
        out.push(String::from_utf8_lossy(&buf[from..nl]).into_owned());
        from = nl + 1;
    }
    buf.drain(..from);
}

/// 上游读取失败
pub enum UpstreamErr {
    /// 读空闲超时（只计 reader 的等待，每收到一个 chunk 重置）
    Idle,
    Io(String),
}

/// 一次性读完整条上游 NDJSON，逐行回调。
///
/// 只在「新到数据含换行」时切分：避免对增长中的超长单行（大 tool-call / tool_result）
/// 反复做全量 split —— O(n²) → O(n)。
pub async fn read_ndjson_all<F>(
    resp: reqwest::Response,
    idle_ms: u64,
    mut on_line: F,
) -> Result<usize, UpstreamErr>
where
    F: FnMut(&str),
{
    let mut stream = resp.bytes_stream();
    let idle = Duration::from_millis(idle_ms);
    let mut buf: Vec<u8> = Vec::new();
    let mut bytes = 0usize;
    let mut lines: Vec<String> = Vec::new();

    loop {
        match tokio::time::timeout(idle, stream.next()).await {
            Err(_) => return Err(UpstreamErr::Idle),
            Ok(None) => break,
            Ok(Some(Err(e))) => return Err(UpstreamErr::Io(e.to_string())),
            Ok(Some(Ok(chunk))) => {
                bytes += chunk.len();
                buf.extend_from_slice(&chunk);
                // 只在「新到数据含换行」时才切分：避免对增长中的超长单行
                // （大 tool-call / tool_result）反复做全量扫描 —— O(n²) → O(n)
                if chunk.contains(&b'\n') {
                    take_lines(&mut buf, &mut lines);
                    for line in lines.drain(..) {
                        on_line(&line);
                    }
                }
            }
        }
    }

    if !buf.is_empty() {
        on_line(&String::from_utf8_lossy(&buf));
    }
    Ok(bytes)
}

/// 把一段字节转成 SSE `data: ...` 行。
pub fn sse_data(body: &Value) -> Bytes {
    Bytes::from(format!("data: {}\n\n", json_string(body)))
}

/// 供 handler 复用：创建断连守卫所需的共享计数。
pub struct StreamCtx {
    pub bytes: Arc<AtomicUsize>,
    pub last_event: Arc<Mutex<String>>,
    pub completed: Arc<AtomicBool>,
    pub start: Instant,
}

impl Default for StreamCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamCtx {
    pub fn new() -> Self {
        Self {
            bytes: Arc::new(AtomicUsize::new(0)),
            last_event: Arc::new(Mutex::new(String::new())),
            completed: Arc::new(AtomicBool::new(false)),
            start: Instant::now(),
        }
    }

    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::Relaxed);
    }

    pub fn set_event(&self, ev: &str) {
        if let Ok(mut s) = self.last_event.lock() {
            *s = ev.to_string();
        }
    }

    pub fn add_bytes(&self, n: usize) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    pub fn guard(
        &self,
        path: &'static str,
        id_key: &'static str,
        id: &str,
        model: &str,
        streaming: bool,
    ) -> DisconnectGuard {
        DisconnectGuard {
            path,
            id_key,
            id: id.to_string(),
            model: model.to_string(),
            start: self.start,
            bytes: Arc::clone(&self.bytes),
            last_event: Arc::clone(&self.last_event),
            completed: Arc::clone(&self.completed),
            streaming,
            extra: None,
        }
    }
}

/// 便捷：把上游 HTTP 错误（非 2xx）映射成下游响应。
pub fn upstream_error_response(err: &crate::errors::MappedError) -> Response {
    json_response(err.status, &err.body)
}

/// 空响应（零输出）错误体，两个协议共用同一份文案。
pub fn empty_response_body() -> Value {
    json!({
        "error": {
            "message": "Empty response from upstream (zero output tokens)",
            "type": "rate_limit_error",
        },
        "retry_after": 10,
    })
}

#[allow(dead_code)]
pub fn state_type_hint(_: &Arc<AppState>) {}
