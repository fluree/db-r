#!/usr/bin/env bash
# High-volume ingest using fluree-ingest with OTEL export.
# Usage: firehose.sh INGEST_BIN GEN_DIR STORAGE_DIR [ENTITIES] [PARSE_THREADS]

set -euo pipefail

INGEST_BIN="${1:?Usage: firehose.sh INGEST_BIN GEN_DIR STORAGE_DIR [ENTITIES] [PARSE_THREADS]}"
GEN_DIR="${2:?Usage: firehose.sh INGEST_BIN GEN_DIR STORAGE_DIR [ENTITIES] [PARSE_THREADS]}"
STORAGE_DIR="${3:?Usage: firehose.sh INGEST_BIN GEN_DIR STORAGE_DIR [ENTITIES] [PARSE_THREADS]}"
ENTITIES="${4:-100000}"
PARSE_THREADS="${5:-4}"

CHUNKS_DIR="${GEN_DIR}/chunks"
LEDGER="firehose:main"

echo "=== Firehose Ingest ==="
echo "Binary:        ${INGEST_BIN}"
echo "Chunks dir:    ${CHUNKS_DIR}"
echo "Storage dir:   ${STORAGE_DIR}"
echo "Entities:      ${ENTITIES}"
echo "Parse threads: ${PARSE_THREADS}"
echo ""

# Verify chunks exist
if [ ! -d "${CHUNKS_DIR}" ] || [ -z "$(ls -A "${CHUNKS_DIR}" 2>/dev/null)" ]; then
    echo "No chunks found. Run 'make generate' first."
    exit 1
fi

CHUNK_COUNT=$(ls "${CHUNKS_DIR}"/chunk_*.ttl 2>/dev/null | wc -l | tr -d ' ')
echo "Found ${CHUNK_COUNT} chunk files"
echo ""

mkdir -p "${STORAGE_DIR}"

echo "Starting bulk import..."
OTEL_SERVICE_NAME=fluree-ingest \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG=info,fluree_db_transact=debug,fluree_db_indexer=debug \
"${INGEST_BIN}" \
    --chunks-dir "${CHUNKS_DIR}" \
    --db-dir "${STORAGE_DIR}" \
    --ledger "${LEDGER}" \
    --import \
    --parse-threads "${PARSE_THREADS}" \
    --build-index

echo ""
echo "Firehose ingest complete."
echo "Check Jaeger for service: fluree-ingest"
