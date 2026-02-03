#!/usr/bin/env bash
#
# OTEL Benchmark Runner
#
# Starts Jaeger, builds fluree-bench with OTEL support, runs the benchmark
# with trace export, and prints the Jaeger UI URL for investigation.
#
# Usage:
#   ./benchmarks/fluree-bench/scripts/bench-otel.sh ingest --data-size-mb 10 --concurrency 4
#   ./benchmarks/fluree-bench/scripts/bench-otel.sh full --data-size-mb 50
#
# All arguments after the script name are passed directly to fluree-bench.
# The --otel flag is added automatically.
#
# Environment overrides (all optional):
#   OTEL_EXPORTER_OTLP_ENDPOINT   OTLP endpoint    (default: http://localhost:4317)
#   OTEL_SERVICE_NAME             Service name      (default: fluree-bench)
#   OTEL_TRACES_SAMPLER           Sampling strategy (default: always_on)
#   OTEL_TRACES_SAMPLER_ARG       Sampler argument  (default: 1.0)
#   RUST_LOG                      Log filter        (default: see below)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

JAEGER_CONTAINER="fluree-bench-jaeger"

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

BENCH_ARGS=("$@")
if [ ${#BENCH_ARGS[@]} -eq 0 ]; then
    echo "Usage: $0 <subcommand> [flags...]"
    echo ""
    echo "Examples:"
    echo "  $0 ingest --data-size-mb 10 --concurrency 4"
    echo "  $0 full --data-size-mb 50"
    echo ""
    echo "All flags are passed to fluree-bench. --otel is added automatically."
    exit 1
fi

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

cleanup() {
    echo ""
    echo "Benchmark complete."
    echo "  Investigate traces at: http://localhost:16686"
    echo "  Select service \"fluree-bench\" and click \"Find Traces\"."
    echo ""
    echo "  Stop Jaeger with: docker rm -f $JAEGER_CONTAINER"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

if ! command -v docker &>/dev/null; then
    echo "Error: docker is required but not found in PATH."
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
# 1. Start Jaeger (if not already running)
# ---------------------------------------------------------------------------

if docker ps --format '{{.Names}}' | grep -q "^${JAEGER_CONTAINER}$"; then
    echo "==> Jaeger already running ($JAEGER_CONTAINER)"
else
    echo "==> Starting Jaeger..."
    docker rm -f "$JAEGER_CONTAINER" >/dev/null 2>&1 || true
    docker run -d --name "$JAEGER_CONTAINER" \
        -p 4317:4317 \
        -p 16686:16686 \
        jaegertracing/jaeger:latest >/dev/null
    echo "    OTLP on :4317, UI at http://localhost:16686"
    # Give Jaeger a moment to start accepting connections.
    sleep 2
fi

# ---------------------------------------------------------------------------
# 2. Build fluree-bench with OTEL support
# ---------------------------------------------------------------------------

echo "==> Building fluree-bench (release, bench-otel)..."
cargo build --release -p fluree-bench --features bench-otel \
    --manifest-path "$REPO_ROOT/Cargo.toml" 2>&1 | tail -1

BENCH_BIN="$REPO_ROOT/target/release/fluree-bench"

# ---------------------------------------------------------------------------
# 3. Run benchmark with OTEL export
# ---------------------------------------------------------------------------

echo "==> Running benchmark..."
echo "    Args: ${BENCH_ARGS[*]} --otel"
echo ""

OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-fluree-bench}" \
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}" \
OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-grpc}" \
OTEL_TRACES_SAMPLER="${OTEL_TRACES_SAMPLER:-always_on}" \
OTEL_TRACES_SAMPLER_ARG="${OTEL_TRACES_SAMPLER_ARG:-1.0}" \
RUST_LOG="${RUST_LOG:-warn,fluree_bench=info,fluree_db_api=debug,fluree_db_transact=debug,fluree_db_indexer=debug}" \
    "$BENCH_BIN" "${BENCH_ARGS[@]}" --otel
