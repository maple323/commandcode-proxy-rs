# commandcode-proxy (Rust)

[`MAXeaglet/commandcode-proxy`](https://github.com/MAXeaglet/commandcode-proxy) 的 **Rust 重写版**。

把 **Command Code (CC) API** 转换成三种下游都能直接用的兼容端点：

| 端点 | 协议 |
|---|---|
| `POST /v1/chat/completions` | OpenAI Chat Completions（SSE + 非流式） |
| `POST /v1/responses` | OpenAI Responses（SSE + 非流式） |
| `POST /v1/messages` | Anthropic Messages（SSE + 非流式） |
| `GET /v1/models` | 动态模型列表 |
| `GET /health` | 健康检查 |

原版是一个 3490 行的单文件 `proxy.mjs`（零依赖 Node ESM）。本版把它拆成 12 个 Rust 模块，
**wire 协议、请求信封、错误映射、指纹派生逐项对齐**，并保留原版的全部运维特性。

---

## 快速开始

### 本地构建

```bash
cargo build --release
./target/release/commandcode-proxy        # Windows: .\target\release\commandcode-proxy.exe
```

默认监听 `0.0.0.0:3000`（带 `config.json` 时为 `3050`）。API Key **不必**写进配置，
按请求从 `Authorization: Bearer <key>` 或 `x-api-key` 头里取。

```bash
curl http://127.0.0.1:3050/v1/chat/completions \
  -H 'Authorization: Bearer user_xxxxxxxx' \
  -H 'content-type: application/json' \
  -d '{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}'
```

### Docker

```bash
docker compose up -d --build          # 端口由 PROXY_PORT 控制，默认 3050
docker build -t commandcode-proxy-rs:latest .
```

---

## 配置

### `config.json`

查找顺序：**可执行文件所在目录** → **当前工作目录**。未知键忽略，缺省键用内置默认值。

```json
{
  "port": 3050,
  "host": "0.0.0.0",
  "apiKey": "",
  "apiBase": "https://api.commandcode.ai",
  "projectSlug": "cc-proxy",
  "logFile": "",
  "logLevel": "info",
  "useProviderModels": true,
  "zdr": false,
  "cliMode": "agent",
  "cliSessionMode": "interactive",
  "fingerprintSalt": "",
  "deviceProjectDir": "",
  "emptySystemPlaceholder": true,
  "upstreamProxy": ""
}
```

> `logLevel` 与原版一样：字段被读取但当前未参与过滤，保留是为了配置兼容。

### 环境变量（覆盖 config.json）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `3000` / `3050` | 监听端口 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `CC_API_BASE` | `https://api.commandcode.ai` | 上游地址 |
| `CC_UPSTREAM_PROXY` | 空 | 让**发往上游**的请求走 HTTP 代理（仅 `http://` CONNECT） |
| `PROJECT_SLUG` | `cc-proxy` | `x-project-slug` 请求头 |
| `LOG_FILE` | 空 | 日志落盘路径 |
| `CC_USE_PROVIDER_MODELS` | `true` | 动态拉取模型列表 |
| `CMD_ZDR` | 关 | `1` 开启 ZDR-only 路由 |
| `CC_CLI_MODE` | `agent` | 信封 `mode` |
| `CC_CLI_SESSION_MODE` | `interactive` | lifecycle 元数据的 `mode` |
| `CC_FINGERPRINT_SALT` | 空 | 设备指纹盐 |
| `CC_DEVICE_PROJECT_DIR` | 空 | 伪装的项目目录 |
| `CC_EMPTY_SYSTEM_PLACEHOLDER` | `true` | 无 system prompt 时发空格占位 |
| `CC_MAX_BODY_MB` | `100` | 请求体上限，超限 `413` |
| `CC_STREAM_IDLE_MS` | `30000` | 流式上游读空闲超时 |
| `CC_NONSTREAM_IDLE_MS` | `90000` | 非流式上游读空闲超时 |
| `CC_MAX_INFLIGHT` | `0`（不限） | 在途请求上限，超限 `503` |
| `CC_CLIENT_DRAIN_TIMEOUT_MS` | 空（禁用） | 下游背压阻塞超过该毫秒数即断开该客户端 |
| `CC_KEEPALIVE_TIMEOUT_MS` | `65000` | 后端 keep-alive 时长，**必须大于反代侧的 keepalive_timeout** |

---

## 错误码

上游错误按固定表映射成下游能自动重试的状态码：

| 上游 | 下游 |
|---|---|
| 402 | 429 |
| 403 | 401 |
| 422 | 400 |
| 500 / 502 | 502 |
| 503 | 503 |
| 其它 | 502 `upstream_error` |

另外两条关键语义：

- **流被截断**（上游没发过 `finish` / `finish-step`）→ `502 upstream_error` + `retry_after`，
  绝不补一个 `finish_reason` 把半截回答谎报成完整回答。
- **输出 token 为 0** → `429 rate_limit_error`，避免下游异常计费。

流式请求在**写出第一个 SSE 字节之前**仍然会返回 JSON 错误（等价原版的「延迟写 200 头」），
这样 SDK 才能按可重试错误正常退避。

---

## 与原版的性能对比

两边指向**同一个假上游**，对比启动、内存与吞吐（本机 16 逻辑核心 / Node v25.2.1）：

| 指标 | 原版 (Node) | Rust | 倍数 |
|---|---|---|---|
| 启动到 `/health` 200 | 514.4 ms | 1.5 ms | **343×** |
| 空闲 RSS | 61.1 MB | 12.5 MB | **4.9×** |
| 负载后 RSS | 91.3 MB | 15.3 MB | **6.0×** |
| 顺序 QPS（小 body） | 273.1 | 390.7 | 1.43× |
| 并发 QPS（50 并发） | 1052.9 | 2359.3 | **2.24×** |
| 20KB 请求体 QPS | 124.1 | 155.1 | 1.25× |

> 假上游是 Python `ThreadingHTTPServer`。由于它向 Rust 版供出了 2359 req/s（远高于
> Node 的 1053），可以确认瓶颈在代理侧、对比成立，而不是卡在测试脚手架。

**并发差距的来源**：Node 的 JS 主线程只有一条，Rust 侧 tokio 默认按 CPU 核数起 worker
（本机 16）。这项在本机是 2.24× 而非 16×，因为负载是 I/O 密集且上游是共享瓶颈 ——
差距在**上游变慢或请求体变大**时会更明显（大 body 那行就是 CPU 侧开销的体现）。

**反过来，原版在这些方面更好**：零依赖单文件（本版 `Cargo.lock` 有 187 个 crate，
供应链面更大）、免构建（`node proxy.mjs` 即跑，本版首次构建约 1 分钟且 `target/` 约 595 MB）、
改一行即生效。本版换来的是启动/内存/并发与编译期检查，**不是全面碾压**。

---

## 目录结构

```
src/
  main.rs        HTTP 服务器、accept 循环、中间件（CORS + 在途准入）
  config.rs      配置加载（默认值 → config.json → 环境变量）
  state.rs       AppState、会话管理、上游客户端、初始化预请求、在途计数
  cc.rs          CC 请求体构造 + 信封排序 + 上游转发
  fingerprint.rs 设备指纹确定性派生
  errors.rs      错误映射、finish_reason 归一化、usage 换算
  translate.rs   三套协议翻译器（Chat / Anthropic / Responses）
  http_util.rs   受限读体、SSE 响应、NDJSON 行切分、断连守卫
  logging.rs     结构化日志
  util.rs        UUID / 哈希 / traceparent 等
  handlers/      三个端点 handler
```

---

## 与 JS 原版的差异

行为对齐是目标，下面是**有意为之**或**必须注意**的差异：

1. **上游代理只认显式配置。** 原版用 undici 的 `fetch`，默认**不读** `HTTP_PROXY` /
   `HTTPS_PROXY` 环境变量；而 `reqwest` 默认会读。若不处理，在企业环境或 CI 里设了
   `HTTP_PROXY` 时行为会分叉：本该直连的请求被塞进环境代理，且走代理时请求行会变成
   绝对形式（`POST http://host/path`），对端按源服务器解析就会 404。
   本版对两个客户端都显式调用 `.no_proxy()`，**只有 `upstreamProxy` 配置了才走代理**。
2. **`logLevel` 未生效** —— 与原版一致（原版也只是声明了字段）。
3. **`/health` 与 `/` 响应任意方法**，未知路径/方法统一 `404 {"error":{"type":"not_found"}}`。
4. 原版是单文件；本版按职责拆模块，便于单测与后续维护。

---

## 免责声明

仅供个人学习与协议研究使用。请遵守上游服务条款，自行承担使用风险。

原项目版权归 [MAXeaglet](https://github.com/MAXeaglet) 所有，MIT License。
