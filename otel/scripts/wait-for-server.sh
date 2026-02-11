#!/usr/bin/env bash
# Wait for the Fluree server to become healthy.
# Usage: wait-for-server.sh [port]
set -euo pipefail

PORT="${1:-${PORT:-8090}}"
URL="http://localhost:${PORT}/health"
MAX_WAIT=30
INTERVAL=1

echo "Waiting for server at ${URL} (max ${MAX_WAIT}s)..."

elapsed=0
while [ "$elapsed" -lt "$MAX_WAIT" ]; do
    if curl -sf "$URL" > /dev/null 2>&1; then
        echo "Server is ready (took ${elapsed}s)"
        exit 0
    fi
    sleep "$INTERVAL"
    elapsed=$((elapsed + INTERVAL))
done

echo "ERROR: Server did not become healthy within ${MAX_WAIT}s"
exit 1
