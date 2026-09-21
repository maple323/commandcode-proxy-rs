# ── 构建阶段 ────────────────────────────────────────────────
# 用 Debian(glibc) 而非 alpine(musl)：rustls 的 ring 后端在 musl 下
# 需要额外的构建依赖，glibc 镜像开箱即用。
FROM rust:1-slim-bookworm AS builder
WORKDIR /build

# 先只拷依赖清单 + 一个占位 main.rs，把「编译所有依赖」这一步固化进 layer cache。
# 之后改业务代码时这一层不会失效，重复构建能从 ~4min 降到 ~40s。
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
 && printf 'fn main() {}\n' > src/main.rs \
 && cargo build --release --locked \
 && rm -rf src

# 再拷真实源码，只重编本 crate
COPY src ./src
# ⚠️ 这里必须 touch，否则镜像里装的是上面那个 `fn main() {}` 空壳。
# 原因：docker COPY 保留构建上下文里文件的 mtime（≈ checkout 时刻），
# 而占位构建写下的 fingerprint 时间戳更晚。Cargo 用 mtime 判单元新鲜度，
# 真实源码「比 fingerprint 更旧」→ 判定为未改动 → 直接跳过重编 → 静默出坏镜像。
RUN find src -type f -exec touch {} + \
 && cargo build --release --locked

# 防呆：占位 main.rs 编出来的二进制约 300 KB，真实产物约 4.5 MB。
# 不做这道校验的话，上面那个坑会静默出厂（v1.0.0 就是这么坏的）。
RUN size=$(stat -c %s target/release/commandcode-proxy) \
 && echo "构建产物大小: ${size} 字节" \
 && if [ "$size" -lt 1000000 ]; then \
      echo "错误：产物仅 ${size} 字节，疑似仍是占位 main.rs 编出的二进制" >&2; \
      exit 1; \
    fi

# ── 运行阶段 ────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/commandcode-proxy /app/commandcode-proxy
COPY config.json /app/config.json

EXPOSE 3050
ENV PORT=3050 \
    HOST=0.0.0.0

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:3050/health || exit 1

CMD ["/app/commandcode-proxy"]
