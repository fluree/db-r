#!/usr/bin/env bash
# Bulk import via fluree-ingest with OTEL export (standalone, no server needed).
# Usage: ingest-smoke.sh INGEST_BIN GEN_DIR STORAGE_DIR ENTITIES PARSE_THREADS LEDGER

set -euo pipefail

INGEST_BIN="${1:?Usage: ingest-smoke.sh INGEST_BIN GEN_DIR STORAGE_DIR ENTITIES PARSE_THREADS LEDGER}"
GEN_DIR="${2:?}"
STORAGE_DIR="${3:?}"
ENTITIES="${4:-10000}"
PARSE_THREADS="${5:-4}"
LEDGER="${6:-ingest-test:main}"

CHUNKS_DIR="${GEN_DIR}/chunks"

echo "=== Ingest OTEL Smoke Test ==="
echo "Binary:        ${INGEST_BIN}"
echo "Chunks dir:    ${CHUNKS_DIR}"
echo "Storage dir:   ${STORAGE_DIR}"
echo "Entities:      ${ENTITIES}"
echo "Parse threads: ${PARSE_THREADS}"
echo "Ledger:        ${LEDGER}"
echo ""

# Verify chunks exist
if [ ! -d "${CHUNKS_DIR}" ] || [ -z "$(ls -A "${CHUNKS_DIR}" 2>/dev/null)" ]; then
    echo "No chunks found in ${CHUNKS_DIR}. Run 'make generate' first."
    exit 1
fi

CHUNK_COUNT=$(ls "${CHUNKS_DIR}"/chunk_*.ttl 2>/dev/null | wc -l | tr -d ' ')
echo "Found ${CHUNK_COUNT} chunk files"
echo ""

# Clean storage dir for a fresh ledger
if [ -d "${STORAGE_DIR}" ]; then
    echo "Cleaning previous storage at ${STORAGE_DIR}..."
    rm -rf "${STORAGE_DIR}"
fi
mkdir -p "${STORAGE_DIR}"

echo "Starting bulk import with OTEL tracing..."
OTEL_SERVICE_NAME=fluree-ingest \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG="${RUST_LOG:-info,fluree_db_api=debug,fluree_db_transact=debug,fluree_db_indexer=debug}" \
"${INGEST_BIN}" \
    --chunks-dir "${CHUNKS_DIR}" \
    --db-dir "${STORAGE_DIR}" \
    --ledger "${LEDGER}" \
    --import \
    --parse-threads "${PARSE_THREADS}"

echo ""
echo "Ingest smoke test complete."
echo "Check Jaeger for service: fluree-ingest"
echo "  http://localhost:16686/search?service=fluree-ingest"
