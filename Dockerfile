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
RUN cargo build --release --locked

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
