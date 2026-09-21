//! 路由分发与各端点 handler。

pub mod chat;
pub mod messages;
pub mod responses;

use crate::http_util::{json_response, text_response};
use crate::state::AppState;
use axum::extract::{Request, State};
use axum::http::Method;
use axum::response::Response;
use serde_json::json;
use std::sync::Arc;

/// 与 JS 版的路由判定逐条对齐：方法不匹配时同样落 404（而不是 405）。
pub async fn dispatch(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    match (path.as_str(), method) {
        ("/v1/chat/completions", Method::POST) => chat::handle(state, req).await,
        ("/v1/messages", Method::POST) => messages::handle(state, req).await,
        ("/v1/responses", Method::POST) => responses::handle(state, req).await,
        ("/v1/models", Method::GET) => handle_models(state, req).await,
        ("/health", _) | ("/", _) => text_response(200, "OK"),
        _ => json_response(
            404,
            &json!({ "error": { "message": "Not found", "type": "not_found" } }),
        ),
    }
}

async fn handle_models(state: Arc<AppState>, req: Request) -> Response {
    // 带 key 时优先走 Provider API 动态列表，失败/无 key 时回退到静态列表
    let api_key = crate::util::get_api_key(req.headers());
    let models = state.fetch_models(api_key.as_deref()).await;
    let now = crate::util::now_unix();
    let data: Vec<serde_json::Value> = models
        .iter()
        .map(|m| {
            json!({
                "id": m.id,
                "object": "model",
                "created": now,
                "owned_by": "command-code",
            })
        })
        .collect();
    json_response(200, &json!({ "object": "list", "data": data }))
}
