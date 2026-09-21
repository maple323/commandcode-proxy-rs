//! 配置加载：默认值 → config.json → 环境变量（与 proxy.mjs 的 loadConfig 逐项对齐）。

use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub host: String,
    pub api_key: String,
    pub api_base: String,
    pub project_slug: String,
    pub log_file: String,
    pub log_level: String,
    pub use_provider_models: bool,
    pub model_refresh_interval_ms: u64,
    pub zdr: bool,
    /// 信封 mode。服务端枚举：agent|learning|custom-agent|custom-agent-create|title-gen|tool-desc|compact|vision
    pub cli_mode: String,
    /// lifecycle metadata 的 mode —— 另一个枚举：interactive | non-interactive
    pub cli_session_mode: String,
    pub fingerprint_salt: String,
    /// 伪造的项目目录（留空则用内置的 C:\Users\dev\projects\app）
    pub device_project_dir: String,
    pub empty_system_placeholder: bool,
    /// 上游 HTTP 代理，如 http://127.0.0.1:7890
    pub upstream_proxy: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 3000,
            host: "0.0.0.0".to_string(),
            api_key: String::new(),
            api_base: "https://api.commandcode.ai".to_string(),
            project_slug: "cc-proxy".to_string(),
            log_file: String::new(),
            log_level: "info".to_string(),
            use_provider_models: true,
            model_refresh_interval_ms: 5 * 60 * 1000,
            zdr: false,
            cli_mode: "agent".to_string(),
            cli_session_mode: "interactive".to_string(),
            fingerprint_salt: String::new(),
            device_project_dir: String::new(),
            empty_system_placeholder: true,
            upstream_proxy: String::new(),
        }
    }
}

/// 只由环境变量决定的运行期限制（config.json 无法覆盖，与 JS 版一致）。
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_body_size: usize,
    pub stream_idle_timeout_ms: u64,
    pub nonstream_idle_timeout_ms: u64,
    pub max_inflight: usize,
    pub client_drain_timeout_ms: u64,
    pub keepalive_timeout_ms: u64,
}

fn env_u64(key: &str) -> Option<u64> {
    let raw = std::env::var(key).ok()?;
    let parsed = raw.trim().parse::<i64>().ok()?;
    if parsed > 0 {
        Some(parsed as u64)
    } else {
        None
    }
}

/// 与 JS 的 `Number.parseInt(process.env.X ?? '', 10)` 行为对齐：只接受正整数。
fn env_positive(key: &str, default: u64) -> u64 {
    env_u64(key).unwrap_or(default)
}

impl Limits {
    pub fn from_env() -> Self {
        Self {
            max_body_size: (env_positive("CC_MAX_BODY_MB", 100) as usize) * 1024 * 1024,
            stream_idle_timeout_ms: env_positive("CC_STREAM_IDLE_MS", 30_000),
            nonstream_idle_timeout_ms: env_positive("CC_NONSTREAM_IDLE_MS", 90_000),
            // 默认 0 = 不限并发
            max_inflight: env_u64("CC_MAX_INFLIGHT").unwrap_or(0) as usize,
            // 默认 0 = 禁用僵死客户端看门狗
            client_drain_timeout_ms: env_u64("CC_CLIENT_DRAIN_TIMEOUT_MS").unwrap_or(0),
            keepalive_timeout_ms: env_positive("CC_KEEPALIVE_TIMEOUT_MS", 65_000),
        }
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    v.as_bool()
}

fn as_str(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.to_string())
}

fn as_u64(v: &Value) -> Option<u64> {
    v.as_u64()
}

impl Config {
    /// 查找 config.json：优先可执行文件所在目录，其次当前工作目录。
    fn locate_config() -> Option<PathBuf> {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                candidates.push(dir.join("config.json"));
            }
        }
        candidates.push(PathBuf::from("config.json"));
        candidates.into_iter().find(|p| p.exists())
    }

    pub fn load() -> (Self, Option<PathBuf>) {
        let mut cfg = Config::default();
        let mut loaded_from = None;

        if let Some(path) = Self::locate_config() {
            match std::fs::read_to_string(&path) {
                Ok(text) => match serde_json::from_str::<Value>(&text) {
                    Ok(Value::Object(map)) => {
                        // 等价于 Object.assign(defaults, user)：逐字段覆盖，未知键忽略。
                        if let Some(v) = map.get("port").and_then(as_u64) {
                            cfg.port = v as u16;
                        }
                        if let Some(v) = map.get("host").and_then(as_str) {
                            cfg.host = v;
                        }
                        if let Some(v) = map.get("apiKey").and_then(as_str) {
                            cfg.api_key = v;
                        }
                        if let Some(v) = map.get("apiBase").and_then(as_str) {
                            cfg.api_base = v;
                        }
                        if let Some(v) = map.get("projectSlug").and_then(as_str) {
                            cfg.project_slug = v;
                        }
                        if let Some(v) = map.get("logFile").and_then(as_str) {
                            cfg.log_file = v;
                        }
                        if let Some(v) = map.get("logLevel").and_then(as_str) {
                            cfg.log_level = v;
                        }
                        if let Some(v) = map.get("useProviderModels").and_then(as_bool) {
                            cfg.use_provider_models = v;
                        }
                        if let Some(v) = map.get("modelRefreshIntervalMs").and_then(as_u64) {
                            cfg.model_refresh_interval_ms = v;
                        }
                        if let Some(v) = map.get("zdr").and_then(as_bool) {
                            cfg.zdr = v;
                        }
                        if let Some(v) = map.get("cliMode").and_then(as_str) {
                            cfg.cli_mode = v;
                        }
                        if let Some(v) = map.get("cliSessionMode").and_then(as_str) {
                            cfg.cli_session_mode = v;
                        }
                        if let Some(v) = map.get("fingerprintSalt").and_then(as_str) {
                            cfg.fingerprint_salt = v;
                        }
                        if let Some(v) = map.get("deviceProjectDir").and_then(as_str) {
                            cfg.device_project_dir = v;
                        }
                        if let Some(v) = map.get("emptySystemPlaceholder").and_then(as_bool) {
                            cfg.empty_system_placeholder = v;
                        }
                        if let Some(v) = map.get("upstreamProxy").and_then(as_str) {
                            cfg.upstream_proxy = v;
                        }
                        loaded_from = Some(path);
                    }
                    _ => {
                        eprintln!("[config] Failed to parse config.json: not a JSON object");
                    }
                },
                Err(e) => {
                    eprintln!("[config] Failed to read config.json: {e}");
                }
            }
        }

        // 环境变量覆写
        if let Ok(v) = std::env::var("PORT") {
            if let Ok(n) = v.trim().parse::<i64>() {
                if (0..=65535).contains(&n) {
                    cfg.port = n as u16;
                }
            }
        }
        if let Ok(v) = std::env::var("HOST") {
            cfg.host = v;
        }
        if let Ok(v) = std::env::var("CC_API_BASE") {
            cfg.api_base = v;
        }
        if let Ok(v) = std::env::var("PROJECT_SLUG") {
            cfg.project_slug = v;
        }
        if let Ok(v) = std::env::var("LOG_FILE") {
            cfg.log_file = v;
        }
        if let Ok(v) = std::env::var("CC_USE_PROVIDER_MODELS") {
            cfg.use_provider_models = v != "false";
        }
        if let Ok(v) = std::env::var("CMD_ZDR") {
            cfg.zdr = v == "1";
        }
        if let Ok(v) = std::env::var("CC_FINGERPRINT_SALT") {
            cfg.fingerprint_salt = v;
        }
        if let Ok(v) = std::env::var("CC_DEVICE_PROJECT_DIR") {
            cfg.device_project_dir = v;
        }
        if let Ok(v) = std::env::var("CC_CLI_MODE") {
            cfg.cli_mode = v;
        }
        if let Ok(v) = std::env::var("CC_CLI_SESSION_MODE") {
            cfg.cli_session_mode = v;
        }
        if let Ok(v) = std::env::var("CC_EMPTY_SYSTEM_PLACEHOLDER") {
            cfg.empty_system_placeholder = v != "false";
        }
        if let Ok(v) = std::env::var("CC_UPSTREAM_PROXY") {
            cfg.upstream_proxy = v;
        }

        (cfg, loaded_from)
    }

    pub fn api_base_trimmed(&self) -> &str {
        self.api_base.trim_end_matches('/')
    }
}

/// 校验上游代理地址：只支持 http://（CONNECT 隧道）。
/// 返回规范化后的 (host, port, Option<Basic auth>)。
pub struct ProxyTarget {
    pub host: String,
    pub port: u16,
    /// 规范化后的 `Basic xxx`；实际请求由 reqwest 从代理 URL 里自行取用，
    /// 这里保留是为了把「带口令的 URL 只出现在这一处」这件事显式化。
    #[allow(dead_code)]
    pub auth: Option<String>,
}

/// 代理 URL 可能带 user:pass —— 任何日志/错误消息都只允许出现 host:port。
pub fn redact_proxy_url(raw: &str) -> String {
    if raw.is_empty() {
        return "(direct)".to_string();
    }
    match parse_proxy_url(raw) {
        Ok(t) => format!("http://{}:{}", t.host, t.port),
        Err(_) => "(invalid upstreamProxy)".to_string(),
    }
}

pub fn parse_proxy_url(raw: &str) -> Result<ProxyTarget, String> {
    let err = || "upstreamProxy is not a valid URL (expected http://host:port)".to_string();
    let after_scheme = raw.split_once("://").ok_or_else(err)?;
    let (scheme, rest) = after_scheme;
    if !scheme.eq_ignore_ascii_case("http") {
        return Err(format!(
            "upstreamProxy only supports http:// (CONNECT) proxies, got {scheme}://"
        ));
    }
    // 去掉 path / query
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err(err());
    }
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p.parse().map_err(|_| err())?;
            (h.to_string(), port)
        }
        None => (hostport.to_string(), 80),
    };
    if host.is_empty() {
        return Err(err());
    }
    let auth = userinfo.map(|u| {
        let (user, pass) = u.split_once(':').unwrap_or((u, ""));
        let decoded_user = percent_decode(user);
        let decoded_pass = percent_decode(pass);
        use base64::Engine;
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD
                .encode(format!("{decoded_user}:{decoded_pass}"))
        )
    });
    Ok(ProxyTarget { host, port, auth })
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 便捷函数：把配置文件路径转成字符串用于启动日志。
pub fn path_display(p: &Option<PathBuf>) -> String {
    match p {
        Some(p) => p.display().to_string(),
        None => "(built-in defaults)".to_string(),
    }
}

#[allow(dead_code)]
pub fn is_same_file(a: &Path, b: &Path) -> bool {
    a == b
}
