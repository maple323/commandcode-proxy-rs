//! CC 请求体构建 + 上游转发（含 HTTP 代理隧道）。

use crate::config::Config;
use crate::fingerprint::DeviceProfile;
use crate::state::AppState;
use crate::util::{
    data_url_media_type, date_str_utc, first_truthy, generate_traceparent, json_string,
    nullish_text, slugify_project_path, string_or_empty, try_parse_json, truthy,
};
use serde_json::{json, Map, Value};

/// CLI 发送前会重写部分工具名（resolveToolNameAlias / ow 表）
fn to_wire_tool_name(name: &str) -> &str {
    match name {
        "bash_output" => "shell_output",
        "task_output" => "shell_output",
        "tool_search" => "search_tools",
        "read_multiple_files" => "read_file",
        other => other,
    }
}

/// CLI 的 toWireToolOutput：只取文本块，用 '\n' 拼接
fn to_wire_tool_output_value(content: Option<&Value>) -> String {
    match content {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()) == Some("text"))
            .map(|c| string_or_empty(c.get("text")))
            .collect::<Vec<_>>()
            .join("\n"),
        Some(other) => other.to_string(),
    }
}

fn is_system_role(role: &str) -> bool {
    role == "system" || role == "developer"
}

fn get_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|x| x.as_str())
}

fn cache_control_of(v: &Value) -> Option<Value> {
    match v.get("cache_control") {
        Some(c) if !c.is_null() => Some(c.clone()),
        _ => None,
    }
}

/// 构建发往 CC `/alpha/generate` 的请求体。
pub fn build_cc_request(cfg: &Config, profile: &DeviceProfile, openai_req: &Value) -> Value {
    let empty_arr: Vec<Value> = Vec::new();
    let messages: &Vec<Value> = openai_req
        .get("messages")
        .and_then(|m| m.as_array())
        .unwrap_or(&empty_arr);

    // ── 系统提示：块数组，非最后一块补 \n，cache_control 逐块保留 ──
    let mut system_blocks: Vec<Value> = Vec::new();
    for m in messages {
        let role = get_str(m, "role").unwrap_or("");
        if !is_system_role(role) {
            continue;
        }
        match m.get("content") {
            Some(Value::String(s)) => {
                if !s.is_empty() {
                    system_blocks.push(json!({ "type": "text", "text": s }));
                }
            }
            Some(Value::Array(arr)) => {
                for c in arr {
                    let text = nullish_text(c);
                    let cc = cache_control_of(c);
                    if text.is_empty() && cc.is_none() {
                        continue;
                    }
                    let mut block = Map::new();
                    block.insert("type".into(), json!("text"));
                    block.insert("text".into(), Value::String(text));
                    if let Some(cc) = cc {
                        block.insert("cache_control".into(), cc);
                    }
                    system_blocks.push(Value::Object(block));
                }
            }
            Some(Value::Null) | None => {}
            Some(other) => {
                system_blocks.push(json!({ "type": "text", "text": other.to_string() }));
            }
        }
    }
    let last = system_blocks.len().saturating_sub(1);
    for (i, block) in system_blocks.iter_mut().enumerate() {
        if i < last {
            if let Some(t) = block.get_mut("text") {
                if let Some(s) = t.as_str() {
                    *t = Value::String(format!("{s}\n"));
                }
            }
        }
    }

    let chat_messages: Vec<&Value> = messages
        .iter()
        .filter(|m| !is_system_role(get_str(m, "role").unwrap_or("")))
        .collect();

    // tool_call_id → tool_name 反查表
    let mut tool_name_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for msg in &chat_messages {
        if get_str(msg, "role") != Some("assistant") {
            continue;
        }
        if let Some(tcs) = msg.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in tcs {
                if let Some(id) = tc.get("id").and_then(|i| i.as_str()) {
                    let name = tc
                        .get("function")
                        .and_then(|f| f.get("name"))
                        .and_then(|n| n.as_str())
                        .unwrap_or("");
                    tool_name_map.insert(id.to_string(), name.to_string());
                }
            }
        }
    }

    // ── messages 转换 ──
    let mut cc_messages: Vec<Value> = Vec::with_capacity(chat_messages.len());
    for msg in &chat_messages {
        let role = get_str(msg, "role").unwrap_or("");
        match role {
            "user" => {
                let content = msg.get("content");
                match content {
                    Some(Value::String(s)) => {
                        cc_messages.push(json!({
                            "role": "user",
                            "content": [{ "type": "text", "text": s }]
                        }));
                    }
                    Some(Value::Array(arr)) => {
                        // 多模态：text + image_url → CC image 格式，顺序原样保留
                        let parts: Vec<Value> = arr
                            .iter()
                            .map(|part| {
                                if part.get("type").and_then(|t| t.as_str()) == Some("image_url") {
                                    let url = part
                                        .get("image_url")
                                        .and_then(|i| i.get("url"))
                                        .and_then(|u| u.as_str())
                                        .unwrap_or("");
                                    let mut image_part = Map::new();
                                    image_part.insert("type".into(), json!("image"));
                                    image_part.insert("image".into(), json!(url));
                                    if let Some(mt) = data_url_media_type(url) {
                                        image_part.insert("mimeType".into(), json!(mt));
                                    }
                                    Value::Object(image_part)
                                } else {
                                    part.clone()
                                }
                            })
                            .collect();
                        cc_messages.push(json!({ "role": "user", "content": parts }));
                    }
                    other => {
                        cc_messages.push(json!({
                            "role": "user",
                            "content": [{ "type": "text", "text": string_or_empty(other) }]
                        }));
                    }
                }
            }
            "assistant" => {
                let mut parts: Vec<Value> = Vec::new();
                // 思考内容必须回传：CC 在 thinking 模式下校验 reasoning 是否随历史带回，
                // 丢弃会让上游直接拒绝。次序也必须与 CLI 抓包格式一致：[reasoning, text, tool-call]。
                let reasoning_content = msg.get("reasoning_content").filter(|v| truthy(v));
                if let Some(rc) = reasoning_content {
                    parts.push(json!({ "type": "reasoning", "text": rc }));
                }
                match msg.get("content") {
                    Some(Value::String(s)) => {
                        if !s.is_empty() {
                            parts.push(json!({ "type": "text", "text": s }));
                        }
                    }
                    Some(Value::Array(arr)) => {
                        for part in arr {
                            match part.get("type").and_then(|t| t.as_str()) {
                                Some("text") => parts.push(part.clone()),
                                Some("reasoning") if reasoning_content.is_none() => {
                                    parts.push(part.clone())
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                if let Some(tcs) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tcs {
                        let func = tc.get("function");
                        let name = func
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        let args = func.and_then(|f| f.get("arguments"));
                        let input = match args {
                            Some(Value::String(s)) => try_parse_json(s),
                            Some(Value::Null) | None => json!({}),
                            Some(other) => other.clone(),
                        };
                        let mut entry = Map::new();
                        entry.insert("type".into(), json!("tool-call"));
                        if let Some(id) = tc.get("id").filter(|v| truthy(v)) {
                            entry.insert("toolCallId".into(), id.clone());
                        }
                        entry.insert("toolName".into(), json!(name));
                        entry.insert("input".into(), input);
                        parts.push(Value::Object(entry));
                    }
                }
                cc_messages.push(json!({ "role": "assistant", "content": parts }));
            }
            "tool" => {
                let tool_call_id = msg.get("tool_call_id").cloned();
                let lookup = tool_call_id
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .and_then(|id| tool_name_map.get(id))
                    .cloned();
                let tool_name = first_truthy(
                    &[
                        lookup.map(Value::String).unwrap_or(Value::Null),
                        msg.get("name").cloned().unwrap_or(Value::Null),
                    ],
                    Value::String(String::new()),
                );
                let mut entry = Map::new();
                entry.insert("type".into(), json!("tool-result"));
                if let Some(id) = tool_call_id.filter(|v| truthy(v)) {
                    entry.insert("toolCallId".into(), id);
                }
                entry.insert("toolName".into(), tool_name);
                entry.insert(
                    "output".into(),
                    json!({
                        "type": "text",
                        "value": to_wire_tool_output_value(msg.get("content"))
                    }),
                );
                cc_messages.push(json!({
                    "role": "tool",
                    "content": [Value::Object(entry)]
                }));
            }
            _ => {
                // 未知 role 兜底：归一化为 user 并保证 content 为数组，避免 CC 校验拒绝
                let text = match msg.get("content") {
                    Some(Value::Null) | None => String::new(),
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                };
                cc_messages.push(json!({
                    "role": "user",
                    "content": [{ "type": "text", "text": text }]
                }));
            }
        }
    }

    // ── 缓存断点 ──
    // system 是块数组，断点可以原样留在 system 上；否则若给了 OpenAI 系的 prompt_cache_key，
    // 把断点落在 system 最后一块 —— 缓存按前缀计算，system 正是最前的那段前缀。
    let prompt_cache_key = openai_req.get("prompt_cache_key").filter(|v| truthy(v));
    let has_cache_marker = system_blocks.iter().any(|b| cache_control_of(b).is_some())
        || cc_messages.iter().any(|msg| {
            msg.get("content")
                .and_then(|c| c.as_array())
                .map(|arr| arr.iter().any(|p| cache_control_of(p).is_some()))
                .unwrap_or(false)
        });
    if prompt_cache_key.is_some() && !has_cache_marker && !system_blocks.is_empty() {
        if let Some(last_block) = system_blocks.last_mut() {
            last_block["cache_control"] = json!({ "type": "ephemeral" });
        }
    }

    // ── config ──
    let mut config = Map::new();
    config.insert("workingDir".into(), json!(profile.project_dir));
    config.insert("date".into(), json!(date_str_utc()));
    config.insert("environment".into(), json!(profile.platform));
    config.insert("structure".into(), json!([]));
    config.insert("isGitRepo".into(), json!(false));
    config.insert("currentBranch".into(), json!(""));
    config.insert("mainBranch".into(), json!(""));
    config.insert("gitStatus".into(), json!(""));
    config.insert("recentCommits".into(), json!([]));

    // ── params ──
    let model = openai_req
        .get("model")
        .filter(|v| truthy(v))
        .and_then(|v| v.as_str())
        .unwrap_or("deepseek/deepseek-v4-flash")
        .to_string();

    let max_tokens = openai_req
        .get("max_tokens")
        .filter(|v| truthy(v))
        .and_then(|v| v.as_f64())
        .unwrap_or(64_000.0)
        .min(200_000.0);

    let mut params = Map::new();
    params.insert("model".into(), json!(model));
    params.insert("messages".into(), Value::Array(cc_messages));
    params.insert("max_tokens".into(), json!(max_tokens as i64));
    params.insert("stream".into(), json!(true));

    if !system_blocks.is_empty() {
        params.insert("system".into(), Value::Array(system_blocks));
    } else if cfg.empty_system_placeholder {
        // CC 上游在 params.system 缺省时会注入自身约 7.5K token 的默认提示词，
        // 既产生大量 cached tokens 又污染对话。发一个空格占位即可绕过。
        params.insert("system".into(), json!([{ "type": "text", "text": " " }]));
    }

    if let Some(t) = openai_req.get("temperature") {
        if !t.is_null() {
            params.insert("temperature".into(), t.clone());
        }
    }
    if let Some(re) = openai_req.get("reasoning_effort") {
        if !re.is_null() {
            params.insert("reasoning_effort".into(), re.clone());
        }
    }

    // CLI 总是下发 tools（没有工具时是空数组）—— 空数组与缺键在 wire 上可观测
    let empty_tools: Vec<Value> = Vec::new();
    let tools = openai_req
        .get("tools")
        .and_then(|t| t.as_array())
        .unwrap_or(&empty_tools);
    let wire_tools: Vec<Value> = tools
        .iter()
        .map(|t| {
            let func = t.get("function");
            let name = first_truthy(
                &[
                    func.and_then(|f| f.get("name")).cloned().unwrap_or(Value::Null),
                    t.get("name").cloned().unwrap_or(Value::Null),
                ],
                Value::String(String::new()),
            );
            let name = string_or_empty(Some(&name));
            let description = first_truthy(
                &[
                    func.and_then(|f| f.get("description")).cloned().unwrap_or(Value::Null),
                    t.get("description").cloned().unwrap_or(Value::Null),
                ],
                Value::String(String::new()),
            );
            let schema = first_truthy(
                &[
                    func.and_then(|f| f.get("parameters")).cloned().unwrap_or(Value::Null),
                    t.get("input_schema").cloned().unwrap_or(Value::Null),
                ],
                json!({ "type": "object", "properties": {} }),
            );
            json!({
                "name": to_wire_tool_name(&name),
                "description": description,
                "input_schema": schema,
            })
        })
        .collect();
    params.insert("tools".into(), Value::Array(wire_tools));

    if let Some(tc) = openai_req.get("tool_choice") {
        if !tc.is_null() {
            let mapped = match tc {
                Value::String(s) => {
                    let t = match s.as_str() {
                        "auto" => "auto",
                        "none" => "none",
                        "required" => "any",
                        _ => "auto",
                    };
                    json!({ "type": t })
                }
                Value::Object(_) if tc.get("type").and_then(|t| t.as_str()) == Some("function") => {
                    let name = tc
                        .get("function")
                        .and_then(|f| f.get("name"))
                        .cloned()
                        .unwrap_or(Value::Null);
                    json!({ "type": "tool", "name": name })
                }
                other => other.clone(),
            };
            params.insert("tool_choice".into(), mapped);
        } else {
            params.insert("tool_choice".into(), tc.clone());
        }
    }
    if let Some(p) = openai_req.get("parallel_tool_calls") {
        if !p.is_null() {
            params.insert("parallel_tool_calls".into(), p.clone());
        }
    }

    // ── 顶层信封（键序与 CLI 一致）──
    let mut body = Map::new();
    body.insert("config".into(), Value::Object(config));
    body.insert("memory".into(), Value::Null);
    body.insert("taste".into(), Value::Null);
    body.insert("skills".into(), Value::Null); // CLI 发 null，不是空串
    body.insert("permissionMode".into(), json!("standard"));
    body.insert(
        "mode".into(),
        json!(if cfg.cli_mode.is_empty() { "agent" } else { &cfg.cli_mode }),
    );
    body.insert("params".into(), Value::Object(params));
    Value::Object(body)
}

fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, c) in b.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// CLI 的 toWireThreadId：只有合法 UUID 才放进信封，否则整个键省略。
/// 同时按 CLI 的键顺序重排：config, memory, taste, skills, permissionMode, threadId, mode, promptCache, params
fn reorder_envelope(body: &Value, session_id: &str) -> Value {
    let mut ordered = Map::new();
    for k in ["config", "memory", "taste", "skills", "permissionMode"] {
        if let Some(v) = body.get(k) {
            ordered.insert(k.to_string(), v.clone());
        }
    }
    ordered.insert("threadId".into(), json!(session_id));
    for k in ["mode", "promptCache", "params"] {
        if let Some(v) = body.get(k) {
            ordered.insert(k.to_string(), v.clone());
        }
    }
    Value::Object(ordered)
}

/// 转发到 CC `/alpha/generate`。
pub async fn forward_to_cc(
    state: &AppState,
    body: Value,
    api_key: &str,
    incoming_headers: &axum::http::HeaderMap,
    prompt_cache_key: Option<&str>,
) -> Result<reqwest::Response, reqwest::Error> {
    let url = format!("{}/alpha/generate", state.api_base());
    let traceparent = generate_traceparent();
    let session_id = state.get_session_id(incoming_headers, api_key, prompt_cache_key);

    let body = if is_uuid(&session_id) {
        reorder_envelope(&body, &session_id)
    } else {
        body
    };

    let mut req = state
        .upstream
        .post(&url)
        .header("content-type", "application/json")
        .header("user-agent", "cli")
        .header("x-command-code-version", crate::state::CC_PROTOCOL_VERSION)
        .header("x-cli-environment", "production")
        .header("x-project-slug", slugify_project_path(&state.profile.project_dir))
        .header("x-taste-learning", "false")
        .header("x-session-id", &session_id)
        .header("authorization", format!("Bearer {api_key}"))
        .header("traceparent", traceparent);

    if state.cfg.zdr || incoming_headers.get("x-cmd-zdr").and_then(|v| v.to_str().ok()) == Some("1") {
        req = req.header("x-cmd-zdr", "1");
    }

    req.body(json_string(&body)).send().await
}
