//! Command Code → OpenAI / Anthropic 兼容反向代理（Rust 重写版）。
//!
//! 行为对齐参考实现 proxy.mjs（commandcode-proxy, MIT）：设备指纹、会话管理、
//! 三套协议翻译、上游代理、背压与空闲超时、在途上限等均逐项移植。

mod cc;
mod config;
mod errors;
mod fingerprint;
mod handlers;
mod http_util;
mod logging;
mod state;
mod translate;
mod util;

use crate::config::{parse_proxy_url, redact_proxy_url};
use crate::http_util::json_response;
use crate::state::{AppState, InflightGuard, CC_PROTOCOL_VERSION};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use bytes::Bytes;
use futures_util::Stream;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use serde_json::json;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::ServiceExt;

#[tokio::main]
async fn main() {
    let (cfg, cfg_path) = config::Config::load();
    logging::init(&cfg.log_file);
    let limits = config::Limits::from_env();

    // 启动即校验上游代理：写错的地址应当立刻拒绝启动，而不是每个请求各 502 一次
    if !cfg.upstream_proxy.is_empty() {
        if let Err(e) = parse_proxy_url(&cfg.upstream_proxy) {
            log_error!("Invalid upstreamProxy, refusing to start", {
                "error": e,
                "value": redact_proxy_url(&cfg.upstream_proxy)
            });
            std::process::exit(1);
        }
        log_info!("Upstream requests will go through the configured proxy", {
            "proxy": redact_proxy_url(&cfg.upstream_proxy)
        });
    }

    let state = AppState::new(cfg, limits);

    // ── 后台任务：会话清理 + 协议漂移检测 ──
    {
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3600));
            ticker.tick().await; // 跳过立即触发的第一拍
            loop {
                ticker.tick().await;
                st.cleanup_sessions();
            }
        });
    }
    {
        let st = Arc::clone(&state);
        let client = st.client.clone();
        tokio::spawn(async move {
            // 启动时立即检查一次，之后每 24h
            state::check_protocol_drift(&client).await;
            let mut ticker = tokio::time::interval(Duration::from_secs(24 * 3600));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                state::check_protocol_drift(&client).await;
            }
        });
    }

    print_banner(&state, &cfg_path);

    let app = Router::new()
        .fallback(handlers::dispatch)
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            app_middleware,
        ))
        .with_state(Arc::clone(&state));

    let addr = format!("{}:{}", state.cfg.host, state.cfg.port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            log_error!("Failed to bind", { "addr": addr, "error": e.to_string() });
            std::process::exit(1);
        }
    };

    // header_read_timeout 同时充当 keep-alive 空闲上界（hyper 1.x 没有独立的
    // keepAliveTimeout）。反代侧 keepalive_timeout 必须小于它，否则反代会复用一条
    // 后端已经关掉的连接，POST 请求直接吃 EPIPE/502。
    let header_timeout = Duration::from_millis(state.limits.keepalive_timeout_ms + 1000);

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let app = app.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let mut http1 = hyper::server::conn::http1::Builder::new();
            http1
                .keep_alive(true)
                .header_read_timeout(header_timeout)
                .timer(TokioTimer::new());
            let service = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let app = app.clone();
                async move {
                    let req = req.map(Body::new);
                    let res = app.oneshot(req).await.unwrap_or_else(|_| {
                        json_response(
                            500,
                            &json!({ "error": { "message": "internal error", "type": "internal_error" } }),
                        )
                    });
                    Ok::<_, std::convert::Infallible>(res)
                }
            });
            let _ = http1.serve_connection(io, service).await;
        });
    }
}

/// CORS + 在途上限准入。
///
/// 在途槽位通过包裹响应体来维持生命周期：流式响应写完（或客户端断连）才归还，
/// 与 JS 版 `res.once('finish'|'close', release)` 的「取先到者、幂等」语义一致。
async fn app_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    // /health 与 / 例外：探活与编排器不该因业务繁忙而收 503
    let is_liveness = path == "/health" || path == "/";

    if req.method() == Method::OPTIONS {
        return with_cors(Response::builder().status(204).body(Body::empty()).unwrap());
    }

    let guard = if is_liveness {
        None
    } else {
        Some(state.inflight_acquire())
    };

    if let Some(g) = &guard {
        if g.rejected() {
            log_warn!("In-flight limit reached, rejecting request", {
                "maxInflight": state.limits.max_inflight,
                "inflight": state.inflight_count(),
                "path": path
            });
            return with_cors(json_response(
                503,
                &json!({
                    "error": {
                        "message": format!("Too many concurrent requests (limit {}), retry shortly", state.limits.max_inflight),
                        "type": "server_busy",
                    },
                    "retry_after": 5,
                }),
            ));
        }
    }

    let res = with_cors(next.run(req).await);
    match guard {
        Some(g) => attach_guard(res, g),
        None => res,
    }
}

fn with_cors(mut res: Response) -> Response {
    let headers = res.headers_mut();
    headers.insert(
        "access-control-allow-origin",
        HeaderValue::from_static("*"),
    );
    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("*"),
    );
    res
}

/// 让在途槽位随响应体一起存活/释放。
struct GuardedStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, axum::Error>> + Send>>,
    _guard: InflightGuard,
}

impl Stream for GuardedStream {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.as_mut().poll_next(cx)
    }
}

fn attach_guard(res: Response, guard: InflightGuard) -> Response {
    let (parts, body) = res.into_parts();
    let stream = GuardedStream {
        inner: Box::pin(body.into_data_stream()),
        _guard: guard,
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

fn print_banner(state: &Arc<AppState>, cfg_path: &Option<std::path::PathBuf>) {
    let cfg = &state.cfg;
    let limits = &state.limits;
    log_info!("CC Proxy started", {
        "url": format!("http://{}:{}", cfg.host, cfg.port),
        "api": cfg.api_base,
        "models": state::MODELS.len(),
        "protocolVersion": CC_PROTOCOL_VERSION,
        "configFile": config::path_display(cfg_path),
        "session": "12h + 1h jitter, per API key",
        "zdr": if cfg.zdr { "enabled (x-cmd-zdr: 1 on generation/init requests)" } else { "off (CMD_ZDR=1 or per-request x-cmd-zdr: 1 to enable)" },
        "emptySystemPlaceholder": if cfg.empty_system_placeholder { "on (space placeholder for requests without system prompt)" } else { "off" },
        "logFile": if cfg.log_file.is_empty() { "(console only)" } else { &cfg.log_file },
        "keepAliveTimeout": format!("{}ms (反代侧 keepalive_timeout 必须小于它)", limits.keepalive_timeout_ms),
        "idleTimeouts": format!("stream {}ms / nonstream {}ms", limits.stream_idle_timeout_ms, limits.nonstream_idle_timeout_ms),
        "maxInflight": if limits.max_inflight > 0 { format!("{} (global, /health exempt)", limits.max_inflight) } else { "unlimited (CC_MAX_INFLIGHT=0)".to_string() },
        "maxBodyMB": limits.max_body_size / 1048576,
        "upstreamProxy": redact_proxy_url(&cfg.upstream_proxy),
    });

    if limits.client_drain_timeout_ms > 0 {
        log_info!("Client drain timeout enabled", { "timeoutMs": limits.client_drain_timeout_ms });
    }

    let body_cap_mb = limits.max_body_size / 1048576;
    let worst_case_mb = (body_cap_mb as f64 * 5.5).round() as u64;
    if worst_case_mb >= 500 {
        log_warn!("Request body limit implies high per-request worst-case memory", {
            "maxBodyMB": body_cap_mb,
            "worstCaseRSSPerRequestMB": worst_case_mb,
            "hint": "lower CC_MAX_BODY_MB, set CC_MAX_INFLIGHT, and/or cap in-flight requests at the reverse proxy"
        });
    }
    if cfg.api_key.is_empty() {
        log_info!("No API key in config. API key must be sent in Authorization: Bearer <key> header per request.");
    }
    let _ = TokioExecutor::new();
}
