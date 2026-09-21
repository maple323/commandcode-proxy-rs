//! 日志：格式与 JS 版一致 `[ISO] [level] msg {json}`，同时可追加到文件。

use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

pub struct Logger {
    file: Option<Mutex<std::fs::File>>,
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

pub fn init(log_file: &str) {
    let file = if log_file.is_empty() {
        None
    } else {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)
            .ok()
            .map(Mutex::new)
    };
    let _ = LOGGER.set(Logger { file });
}

fn render(level: &str, msg: &str, data: Option<&Value>) -> String {
    let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    match data {
        Some(v) => format!("[{ts}] [{level}] {msg} {}", serde_json::to_string(v).unwrap_or_default()),
        None => format!("[{ts}] [{level}] {msg}"),
    }
}

fn emit(level: &str, msg: &str, data: Option<Value>) {
    let line = render(level, msg, data.as_ref());
    println!("{line}");
    if let Some(logger) = LOGGER.get() {
        if let Some(file) = &logger.file {
            if let Ok(mut f) = file.lock() {
                let _ = writeln!(f, "{line}");
            }
        }
    }
}

pub fn info(msg: &str, data: Option<Value>) {
    emit("info", msg, data);
}

pub fn warn(msg: &str, data: Option<Value>) {
    emit("warn", msg, data);
}

pub fn error(msg: &str, data: Option<Value>) {
    emit("error", msg, data);
}

// 便捷宏：`log_info!("msg", { "k": v })`
// 注意第二个参数必须用 `tt` 片段收集 —— `{ "k": v }` 本身不是合法的 Rust 表达式，
// 只有交给 `json!` 才能解析。

#[macro_export]
macro_rules! log_info {
    ($msg:expr) => { $crate::logging::info($msg, None) };
    ($msg:expr, $($data:tt)*) => {
        $crate::logging::info($msg, Some(serde_json::json!($($data)*)))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($msg:expr) => { $crate::logging::warn($msg, None) };
    ($msg:expr, $($data:tt)*) => {
        $crate::logging::warn($msg, Some(serde_json::json!($($data)*)))
    };
}

#[macro_export]
macro_rules! log_error {
    ($msg:expr) => { $crate::logging::error($msg, None) };
    ($msg:expr, $($data:tt)*) => {
        $crate::logging::error($msg, Some(serde_json::json!($($data)*)))
    };
}
