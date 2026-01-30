#!/usr/bin/env bash
#
# OTEL Firehose Demo
#
# Starts Jaeger, an OTEL-enabled Fluree server, and a client that streams a
# large Turtle file as batched HTTP transactions. After the import finishes,
# open the Jaeger UI to investigate the transaction waterfall.
#
# Usage:
#   ./examples/otel-firehose-demo/run.sh <path-to-ttl-file>
#
# Environment overrides (all optional):
#   BATCH_SIZE              Statements per batch           (default: 10000)
#   INFLIGHT                Concurrent HTTP requests       (default: 8)
#   LEDGER                  Ledger alias                   (default: firehose-demo)
#   SERVER_PORT             Fluree server port             (default: 8090)
#   STORAGE_DIR             Server storage path            (default: tmpdir)
#   REINDEX_MIN_BYTES       Soft indexing threshold         (default: 10000000)
#   REINDEX_MAX_BYTES       Hard novelty limit              (default: 200000000)
#   OTEL_TRACES_SAMPLER     Trace sampling strategy        (default: always_on)
#   OTEL_TRACES_SAMPLER_ARG Sampler argument               (default: 1.0)
#   RUST_LOG                Server log filter              (default: see below)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Arguments & defaults
# ---------------------------------------------------------------------------

TTL_FILE="${1:-}"
if [ -z "$TTL_FILE" ]; then
    echo "Usage: $0 <path-to-ttl-file>"
    echo ""
    echo "Example:"
    echo "  $0 ~/Downloads/open-street-map/alb.osm.ttl"
    exit 1
fi

if [ ! -f "$TTL_FILE" ]; then
    echo "Error: file not found: $TTL_FILE"
    exit 1
fi

BATCH_SIZE="${BATCH_SIZE:-10000}"
INFLIGHT="${INFLIGHT:-8}"
LEDGER="${LEDGER:-firehose-demo}"
SERVER_PORT="${SERVER_PORT:-8090}"
REINDEX_MIN="${REINDEX_MIN_BYTES:-10000000}"     # 10 MB
REINDEX_MAX="${REINDEX_MAX_BYTES:-200000000}"     # 200 MB
JAEGER_CONTAINER="fluree-jaeger-demo"

# Use a temp dir for storage unless the user provides one.
if [ -n "${STORAGE_DIR:-}" ]; then
    STORAGE_PATH="$STORAGE_DIR"
    OWN_STORAGE=false
else
    STORAGE_PATH="$(mktemp -d)"
    OWN_STORAGE=true
fi

# OTEL / logging defaults
SAMPLER="${OTEL_TRACES_SAMPLER:-always_on}"
SAMPLER_ARG="${OTEL_TRACES_SAMPLER_ARG:-1.0}"
SERVER_LOG="${RUST_LOG:-info,fluree_db_query=debug,fluree_db_transact=debug}"

SERVER_PID=""

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

cleanup() {
    echo ""
    echo "Shutting down..."

    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        echo "  Fluree server stopped."
    fi

    if [ "$OWN_STORAGE" = true ] && [ -d "$STORAGE_PATH" ]; then
        rm -rf "$STORAGE_PATH"
        echo "  Temp storage cleaned up."
    fi

    echo "  Jaeger still running at http://localhost:16686"
    echo "  Stop it with: docker rm -f $JAEGER_CONTAINER"
    echo "Done."
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v docker &>/dev/null; then
    echo "Error: docker is required but not found in PATH."
    echo "Install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo "Error: Docker daemon is not running."
    exit 1
fi

if ! command -v cargo &>/dev/null; then
    echo "Error: cargo is required but not found in PATH."
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Start Jaeger
# ---------------------------------------------------------------------------

echo "==> Starting Jaeger..."

# Remove stale container if present.
docker rm -f "$JAEGER_CONTAINER" >/dev/null 2>&1 || true

docker run -d --name "$JAEGER_CONTAINER" \
    -p 4317:4317 \
    -p 16686:16686 \
    jaegertracing/jaeger:latest >/dev/null

echo "    OTLP collector on :4317, UI on http://localhost:16686"

# ---------------------------------------------------------------------------
# 2. Build server + client
# ---------------------------------------------------------------------------

echo "==> Building fluree-db-server (release, otel)..."
cargo build --release -p fluree-db-server --features otel \
    --manifest-path "$REPO_ROOT/Cargo.toml" 2>&1 | tail -1

echo "==> Building otel-firehose-client (release)..."
cargo build --release -p otel-firehose-client \
    --manifest-path "$REPO_ROOT/Cargo.toml" 2>&1 | tail -1

SERVER_BIN="$REPO_ROOT/target/release/fluree-server"
CLIENT_BIN="$REPO_ROOT/target/release/otel-firehose-client"

# ---------------------------------------------------------------------------
# 3. Start Fluree server with OTEL
# ---------------------------------------------------------------------------

echo "==> Starting Fluree server (port $SERVER_PORT, storage: $STORAGE_PATH)..."
echo "    indexing=true, reindex-min=${REINDEX_MIN}, reindex-max=${REINDEX_MAX}"

OTEL_SERVICE_NAME=fluree \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_TRACES_SAMPLER="$SAMPLER" \
OTEL_TRACES_SAMPLER_ARG="$SAMPLER_ARG" \
RUST_LOG="$SERVER_LOG" \
    "$SERVER_BIN" \
        --listen-addr "0.0.0.0:$SERVER_PORT" \
        --storage-path "$STORAGE_PATH" \
        --indexing-enabled \
        --reindex-min-bytes "$REINDEX_MIN" \
        --reindex-max-bytes "$REINDEX_MAX" &

SERVER_PID=$!
echo "    PID $SERVER_PID"

# ---------------------------------------------------------------------------
# 4. Wait for server health
# ---------------------------------------------------------------------------

echo "==> Waiting for server to be ready..."

HEALTH_URL="http://localhost:$SERVER_PORT/health"
MAX_WAIT=60
WAITED=0

while ! curl -sf "$HEALTH_URL" >/dev/null 2>&1; do
    sleep 1
    WAITED=$((WAITED + 1))
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "Error: server did not become healthy within ${MAX_WAIT}s."
        exit 1
    fi
done
echo "    Server ready (${WAITED}s)."

# ---------------------------------------------------------------------------
# 5. Run the firehose client
# ---------------------------------------------------------------------------

echo "==> Running firehose client..."
echo "    File:       $TTL_FILE"
echo "    Batch size: $BATCH_SIZE"
echo "    In-flight:  $INFLIGHT"
echo "    Ledger:     $LEDGER"
echo ""

RUST_LOG=warn,otel_firehose_client=info \
    "$CLIENT_BIN" "$TTL_FILE" \
        -b "$BATCH_SIZE" \
        --inflight "$INFLIGHT" \
        -l "$LEDGER" \
        --server-url "http://localhost:$SERVER_PORT"

# ---------------------------------------------------------------------------
# 6. Done — hold open for Jaeger investigation
# ---------------------------------------------------------------------------

echo ""
echo "============================================================"
echo "  Import complete!"
echo ""
echo "  Investigate traces at:  http://localhost:16686"
echo "  Select service \"fluree\" and click \"Find Traces\"."
echo ""
echo "  Press Enter to shut down, or Ctrl-C to exit."
echo "============================================================"

read -r
