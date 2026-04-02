# ─── Stage 1: Build ────────────────────────────────────────────────
FROM rust:1.93-slim AS builder

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cargo build --release -p fluree-db-server -p fluree-db-cli

# ─── Stage 2: Runtime ─────────────────────────────────────────────
FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/fluree-server /usr/local/bin/
COPY --from=builder /src/target/release/fluree /usr/local/bin/

EXPOSE 8090

ENTRYPOINT ["fluree-server"]
CMD ["--listen-addr", "0.0.0.0:8090"]
