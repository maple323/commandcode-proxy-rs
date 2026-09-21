//! 全局状态：会话、每 Key 指纹状态、模型缓存、在途计数、连续超时计数。

use crate::config::{Config, Limits};
use crate::fingerprint::{generate_fingerprint, DeviceProfile};
use crate::util::{now_unix, random_uuid};
use crate::{log_info, log_warn};
use rand::Rng;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// 本代理**实际实现**的 wire 协议版本（对齐 command-code@1.53.1 源码）。
/// 真机发的永远是「形状 + 版本号」自洽的组合；如果版本号跟着 npm 走而形状没变，
/// 就变成「自称最新版、却说旧方言」—— 这比版本号过期更容易被行为分析挑出来。
/// 因此这里报的是协议版本，npm 上更新了只告警、不自动改。
pub const CC_PROTOCOL_VERSION: &str = "1.53.1";

/// 每个 API Key 独立一个 session，12h 过期 + 1h 随机抖动
const SESSION_DURATION_MS: i64 = 12 * 60 * 60 * 1000;
const SESSION_JITTER_MS: i64 = 60 * 60 * 1000;

/// 初始化预请求：首次 + 每 8h + 2h 抖动
const INIT_REFRESH_MS: i64 = 8 * 60 * 60 * 1000;
const INIT_JITTER_MS: i64 = 2 * 60 * 60 * 1000;

/// 连续超时阈值：连续 3 次超时才提醒压缩上下文，任意成功请求后重置
pub const TIMEOUT_REDUCE_CONTEXT_THRESHOLD: usize = 3;

pub struct SessionEntry {
    pub session_id: String,
    pub expires_at: i64,
}

pub struct KeyState {
    pub fingerprint: Value,
    pub next_init_at: AtomicI64,
}

pub struct ModelInfo {
    pub id: String,
    pub name: String,
}

struct ModelsCache {
    models: Vec<ModelInfo>,
    fetched_at: Instant,
}

pub struct AppState {
    pub cfg: Config,
    pub limits: Limits,
    pub profile: DeviceProfile,
    /// 直连客户端（npm registry 版本检查，不走上游代理）
    pub client: reqwest::Client,
    /// 上游客户端（发往 CC 的请求，可能经 HTTP 代理）
    pub upstream: reqwest::Client,
    pub sessions: Mutex<HashMap<String, SessionEntry>>,
    pub key_states: Mutex<HashMap<String, Arc<KeyState>>>,
    pub inflight: AtomicUsize,
    pub consecutive_timeouts: AtomicUsize,
    models_cache: Mutex<Option<ModelsCache>>,
}

/// 静态兜底模型列表（Provider API 不可用时使用）
pub const MODELS: &[(&str, &str)] = &[
    // Anthropic
    ("claude-sonnet-4-6", "Claude Sonnet 4.6"),
    ("claude-opus-4-8", "Claude Opus 4.8"),
    ("claude-opus-4-7", "Claude Opus 4.7"),
    ("claude-haiku-4-5-20251001", "Claude Haiku 4.5"),
    // OpenAI
    ("gpt-5.5", "GPT-5.5"),
    ("gpt-5.4", "GPT-5.4"),
    ("gpt-5.4-mini", "GPT-5.4 Mini"),
    ("gpt-5.3-codex", "GPT-5.3 Codex"),
    // DeepSeek
    ("deepseek/deepseek-v4-pro", "DeepSeek V4 Pro"),
    ("deepseek/deepseek-v4-flash", "DeepSeek V4 Flash"),
    // Kimi
    ("moonshotai/Kimi-K2.6", "Kimi K2.6"),
    ("moonshotai/Kimi-K2.5", "Kimi K2.5"),
    // GLM
    ("zai-org/GLM-5.1", "GLM 5.1"),
    ("zai-org/GLM-5", "GLM 5"),
    // MiniMax
    ("MiniMaxAI/MiniMax-M3", "MiniMax M3"),
    ("MiniMaxAI/MiniMax-M2.7", "MiniMax M2.7"),
    ("MiniMaxAI/MiniMax-M2.5", "MiniMax M2.5"),
    // Qwen
    ("Qwen/Qwen3.6-Max-Preview", "Qwen 3.6 Max Preview"),
    ("Qwen/Qwen3.6-Plus", "Qwen 3.6 Plus"),
    ("Qwen/Qwen3.7-Max", "Qwen 3.7 Max"),
    // Step
    ("stepfun/Step-3.7-Flash", "Step 3.7 Flash"),
    ("stepfun/Step-3.5-Flash", "Step 3.5 Flash"),
    // Xiaomi
    ("xiaomi/mimo-v2.5-pro", "MiMo V2.5 Pro"),
    ("xiaomi/mimo-v2.5", "MiMo V2.5"),
    // Gemini
    ("google/gemini-3.5-flash", "Gemini 3.5 Flash"),
    ("google/gemini-3.1-flash-lite", "Gemini 3.1 Flash Lite"),
];

fn build_clients(cfg: &Config) -> (reqwest::Client, reqwest::Client) {
    // 关键：必须显式 no_proxy()。
    // JS 版用 undici 的 fetch，默认**不读** HTTP_PROXY/HTTPS_PROXY 环境变量；
    // 而 reqwest 默认会读，于是企业环境（或沙箱）里设了 HTTP_PROXY 时行为会与
    // JS 版分叉：本该直连的请求被塞进环境代理，且走代理时请求行变成绝对形式
    // （POST http://host/path），对端按源服务器解析就会 404。
    // 只有显式配置的 upstreamProxy 才应该走代理。
    let direct = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("failed to build http client");

    let mut builder = reqwest::Client::builder().no_proxy();
    if !cfg.upstream_proxy.is_empty() {
        builder = builder.connect_timeout(std::time::Duration::from_millis(15_000));
        match reqwest::Proxy::all(&cfg.upstream_proxy) {
            Ok(p) => builder = builder.proxy(p),
            Err(e) => {
                crate::log_error!("Failed to configure upstreamProxy", { "error": e.to_string() });
                std::process::exit(1);
            }
        }
    }
    let upstream = builder.build().expect("failed to build upstream client");
    (direct, upstream)
}

impl AppState {
    pub fn new(cfg: Config, limits: Limits) -> Arc<Self> {
        let profile = DeviceProfile::new(&cfg.device_project_dir);
        let (client, upstream) = build_clients(&cfg);
        Arc::new(Self {
            cfg,
            limits,
            profile,
            client,
            upstream,
            sessions: Mutex::new(HashMap::new()),
            key_states: Mutex::new(HashMap::new()),
            inflight: AtomicUsize::new(0),
            consecutive_timeouts: AtomicUsize::new(0),
            models_cache: Mutex::new(None),
        })
    }

    /// 与 CLI 的 buildCommandAuthHeaders 对齐：没有 x-co-flag；User-Agent 固定 "cli"
    pub fn api_base(&self) -> &str {
        self.cfg.api_base_trimmed()
    }

    // ── 会话管理 ──────────────────────────────────────

    pub fn ensure_session(&self, api_key: &str) -> String {
        let now = chrono::Utc::now().timestamp_millis();
        {
            let sessions = self.sessions.lock().unwrap();
            if let Some(entry) = sessions.get(api_key) {
                if now < entry.expires_at {
                    return entry.session_id.clone();
                }
            }
        }
        let jitter = rand::thread_rng().gen_range(0..SESSION_JITTER_MS);
        let session_id = random_uuid();
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(
            api_key.to_string(),
            SessionEntry {
                session_id: session_id.clone(),
                expires_at: now + SESSION_DURATION_MS + jitter,
            },
        );
        log_info!("Session created", {
            "sessionId": &session_id[..8],
            "storeSize": sessions.len()
        });
        session_id
    }

    pub fn get_session_id(
        &self,
        headers: &axum::http::HeaderMap,
        api_key: &str,
        prompt_cache_key: Option<&str>,
    ) -> String {
        let header = |name: &str| -> Option<String> {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };
        let candidates = [
            header("x-session-id"),
            header("x-claude-code-session-id"),
            header("session_id"),
            prompt_cache_key.map(|s| s.to_string()),
        ];
        for id in candidates.into_iter().flatten() {
            if id.chars().count() >= 8 {
                return id;
            }
        }
        self.ensure_session(api_key)
    }

    /// 每小时清理过期 session 与对应 key 状态，防止 Map 无限增长。
    pub fn cleanup_sessions(&self) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut cleaned = 0usize;
        let mut sessions = self.sessions.lock().unwrap();
        let mut key_states = self.key_states.lock().unwrap();
        sessions.retain(|key, entry| {
            if now >= entry.expires_at {
                key_states.remove(key);
                cleaned += 1;
                false
            } else {
                true
            }
        });
        if cleaned > 0 {
            log_info!("Session cleanup", { "cleaned": cleaned, "remaining": sessions.len() });
        }
    }

    // ── 每 Key 指纹状态 ───────────────────────────────

    pub fn get_or_create_key_state(&self, api_key: &str) -> Arc<KeyState> {
        {
            let states = self.key_states.lock().unwrap();
            if let Some(s) = states.get(api_key) {
                return Arc::clone(s);
            }
        }
        let fingerprint = generate_fingerprint(&self.cfg.fingerprint_salt, api_key, &self.profile);
        let state = Arc::new(KeyState {
            fingerprint,
            next_init_at: AtomicI64::new(0),
        });
        let mut states = self.key_states.lock().unwrap();
        let entry = states
            .entry(api_key.to_string())
            .or_insert_with(|| Arc::clone(&state));
        let is_new = Arc::ptr_eq(entry, &state);
        let result = Arc::clone(entry);
        drop(states);
        if is_new {
            log_info!("Fingerprint generated for key", { "keyPrefix": &api_key[..api_key.len().min(8)] });
        }
        result
    }

    // ── 初始化预请求（fingerprint + lifecycle） ────────

    pub async fn ensure_initialized(&self, api_key: &str) {
        let state = self.get_or_create_key_state(api_key);
        let now = chrono::Utc::now().timestamp_millis();
        if now < state.next_init_at.load(Ordering::Relaxed) {
            return;
        }

        let mut headers = axum::http::HeaderMap::new();
        let mut put = |k: &'static str, v: String| {
            if let Ok(val) = axum::http::HeaderValue::from_str(&v) {
                headers.insert(k, val);
            }
        };
        put("content-type", "application/json".to_string());
        put("x-cli-environment", "production".to_string());
        put("authorization", format!("Bearer {api_key}"));
        put("x-command-code-version", CC_PROTOCOL_VERSION.to_string());
        if self.cfg.zdr {
            put("x-cmd-zdr", "1".to_string());
        }

        let fingerprint = state.fingerprint.clone();
        let components = fingerprint
            .get("components")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let platform = components
            .get("platform")
            .and_then(|v| v.as_str())
            .unwrap_or("win32")
            .to_string();
        let arch = components
            .get("arch")
            .and_then(|v| v.as_str())
            .unwrap_or("x64")
            .to_string();

        let fp_url = format!("{}/alpha/fingerprint/record", self.api_base());
        let lifecycle_url = format!("{}/alpha/lifecycle-events", self.api_base());
        let lifecycle_body = json!({
            "eventType": "cli_session_exists",
            "metadata": {
                "sessionId": format!("sess_{}", crate::util::random_hex(8)),
                "cliVersion": CC_PROTOCOL_VERSION,
                "mode": if self.cfg.cli_session_mode.is_empty() { "interactive" } else { &self.cfg.cli_session_mode },
                "os": format!("{platform}-{arch}"),
            }
        });

        let fingerprint_task = async {
            let res = self
                .upstream
                .post(&fp_url)
                .headers(headers.clone())
                .body(crate::util::json_string(&fingerprint))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => log_info!("Fingerprint recorded"),
                Ok(r) => log_warn!("Fingerprint record failed", { "status": r.status().as_u16() }),
                Err(e) => log_warn!("Fingerprint record error", { "error": e.to_string() }),
            }
        };

        let lifecycle_task = async {
            let res = self
                .upstream
                .post(&lifecycle_url)
                .headers(headers.clone())
                .body(crate::util::json_string(&lifecycle_body))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => log_info!("Lifecycle event sent"),
                Ok(r) => log_warn!("Lifecycle event failed", { "status": r.status().as_u16() }),
                Err(e) => log_warn!("Lifecycle event error", { "error": e.to_string() }),
            }
        };

        // 并行发两个预请求（与 JS 的 Promise.all 一致：单个失败不影响另一个）
        tokio::join!(fingerprint_task, lifecycle_task);

        let jitter = rand::thread_rng().gen_range(0..INIT_JITTER_MS);
        state
            .next_init_at
            .store(chrono::Utc::now().timestamp_millis() + INIT_REFRESH_MS + jitter, Ordering::Relaxed);
        log_info!("Fingerprint/lifecycle next refresh", {
            "nextIn": format!("{}h", (INIT_REFRESH_MS + jitter) as f64 / 3_600_000.0)
        });
    }

    // ── 模型列表 ──────────────────────────────────────

    pub async fn fetch_models(&self, api_key: Option<&str>) -> Vec<ModelInfo> {
        {
            let cache = self.models_cache.lock().unwrap();
            if let Some(c) = cache.as_ref() {
                if c.fetched_at.elapsed().as_millis() < self.cfg.model_refresh_interval_ms as u128 {
                    return c
                        .models
                        .iter()
                        .map(|m| ModelInfo { id: m.id.clone(), name: m.name.clone() })
                        .collect();
                }
            }
        }

        let fallback = || -> Vec<ModelInfo> {
            MODELS
                .iter()
                .map(|(id, name)| ModelInfo { id: (*id).to_string(), name: (*name).to_string() })
                .collect()
        };

        let Some(api_key) = api_key else {
            return fallback();
        };
        if !self.cfg.use_provider_models {
            return fallback();
        }

        let url = format!("{}/provider/v1/models", self.api_base());
        let res = self
            .upstream
            .get(&url)
            .header("authorization", format!("Bearer {api_key}"))
            .header("x-cli-environment", "production")
            .header("x-command-code-version", CC_PROTOCOL_VERSION)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await;

        match res {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    let parsed: Value = r.json().await.unwrap_or(Value::Null);
                    if let Some(arr) = parsed.get("data").and_then(|d| d.as_array()) {
                        let models: Vec<ModelInfo> = arr
                            .iter()
                            .filter_map(|m| m.get("id").and_then(|i| i.as_str()))
                            .map(|id| ModelInfo { id: id.to_string(), name: id.to_string() })
                            .collect();
                        log_info!("Fetched models from Provider API", { "count": models.len() });
                        let mut cache = self.models_cache.lock().unwrap();
                        *cache = Some(ModelsCache {
                            models: models
                                .iter()
                                .map(|m| ModelInfo { id: m.id.clone(), name: m.name.clone() })
                                .collect(),
                            fetched_at: Instant::now(),
                        });
                        return models;
                    }
                }
                log_warn!("Provider models fetch failed, using hardcoded list", { "status": status.as_u16() });
            }
            Err(e) => {
                log_warn!("Provider models fetch error, using hardcoded list", { "error": e.to_string() });
            }
        }
        fallback()
    }

    // ── 在途计数 ──────────────────────────────────────

    pub fn inflight_acquire(self: &Arc<Self>) -> InflightGuard {
        if self.limits.max_inflight == 0 {
            return InflightGuard {
                state: Arc::clone(self),
                acquired: false,
                limited: false,
            };
        }
        // 先乐观自增，再用 CAS 收敛，避免「检查-自增」竞态
        loop {
            let cur = self.inflight.load(Ordering::Acquire);
            if cur >= self.limits.max_inflight {
                return InflightGuard {
                    state: Arc::clone(self),
                    acquired: false,
                    limited: true,
                };
            }
            if self
                .inflight
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return InflightGuard {
                    state: Arc::clone(self),
                    acquired: true,
                    limited: true,
                };
            }
        }
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }

    pub fn bump_consecutive_timeouts(&self) -> usize {
        self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn reset_consecutive_timeouts(&self) {
        self.consecutive_timeouts.store(0, Ordering::Relaxed);
    }
}

/// 在途槽位守卫：无论成功/出错/断连/超时都会归还（Drop 语义天然幂等）。
pub struct InflightGuard {
    state: Arc<AppState>,
    acquired: bool,
    limited: bool,
}

impl InflightGuard {
    /// 是否启用了在途上限且被拒（用于 503 判定）
    pub fn rejected(&self) -> bool {
        self.limited && !self.acquired
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.acquired {
            let _ = self
                .state
                .inflight
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    Some(v.saturating_sub(1))
                });
        }
    }
}

/// 协议漂移检测（只告警，不改版本号）：上游 CLI 更新可能带来协议变化，
/// 这里只负责提醒「该重新读包对齐了」。
pub async fn check_protocol_drift(client: &reqwest::Client) {
    let res = client
        .get("https://registry.npmjs.org/command-code/latest")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;
    match res {
        Ok(r) if r.status().is_success() => {
            let pkg: Value = r.json().await.unwrap_or(Value::Null);
            let latest = pkg.get("version").and_then(|v| v.as_str()).unwrap_or("");
            if !latest.is_empty() && latest != CC_PROTOCOL_VERSION {
                log_warn!("CC CLI version drift: protocol may have changed, re-align from the npm package", {
                    "implemented": CC_PROTOCOL_VERSION,
                    "latest": latest
                });
            } else if !latest.is_empty() {
                log_info!("CC CLI version in sync", { "version": latest });
            }
        }
        Ok(r) => log_warn!("CC version check failed", { "status": r.status().as_u16() }),
        Err(e) => log_warn!("CC version check failed", { "error": e.to_string() }),
    }
}

#[allow(dead_code)]
pub fn unix_now() -> i64 {
    now_unix()
}
