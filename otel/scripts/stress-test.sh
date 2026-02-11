#!/usr/bin/env bash
# Sustained load test: batch inserts + expensive queries.
# Generates enough trace volume to validate OTEL batch processor behavior.
set -euo pipefail

PORT="${PORT:-8090}"
LEDGER="${LEDGER:-otel-test:main}"
BASE="http://localhost:${PORT}/v1/fluree"
ENTITIES="${ENTITIES:-100000}"
BATCH_SIZE=500
BATCHES=$((ENTITIES / BATCH_SIZE))

echo "=== Stress test: ${ENTITIES} entities in ${BATCHES} batches of ${BATCH_SIZE} ==="
echo ""

# ── Phase 1: Batch inserts ────────────────────────────────────
echo "--- Phase 1: Batch inserts"
start_time=$(date +%s)

for batch in $(seq 1 "$BATCHES"); do
    offset=$(( (batch - 1) * BATCH_SIZE ))

    # Build JSON array of entities
    graph="["
    for i in $(seq 1 "$BATCH_SIZE"); do
        idx=$((offset + i))
        [ "$i" -gt 1 ] && graph+=","
        graph+="{\"@id\":\"ex:stress-${idx}\",\"@type\":\"ex:Product\",\"ex:name\":\"Stress Product ${idx}\",\"ex:price\":$((RANDOM % 1000)).99,\"ex:category\":\"stress\",\"ex:inStock\":true}"
    done
    graph+="]"

    HTTP_CODE=$(curl -sf -X POST "${BASE}/${LEDGER}/insert" \
        -H "Content-Type: application/json" \
        -d "{\"@context\":{\"ex\":\"http://example.org/ns/\"},\"@graph\":${graph}}" \
        -o /dev/null -w "%{http_code}")

    if [ "$((batch % 10))" -eq 0 ] || [ "$batch" -eq "$BATCHES" ]; then
        echo "  batch ${batch}/${BATCHES} (HTTP ${HTTP_CODE})"
    fi
done

end_time=$(date +%s)
echo "  Insert phase: $((end_time - start_time))s for ${ENTITIES} entities"
echo ""

# ── Phase 2: Expensive queries ────────────────────────────────
echo "--- Phase 2: Expensive queries"

echo "  SPARQL: Count all products"
time curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -H "Accept: application/sparql-results+json" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT (COUNT(?p) AS ?total)
WHERE { ?p a ex:Product . }' | python3 -m json.tool 2>/dev/null
echo ""

echo "  FQL: Select all products (large result set)"
time curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d "{
        \"@context\": {\"ex\": \"http://example.org/ns/\"},
        \"select\": [\"?name\"],
        \"where\": [{\"@id\": \"?p\", \"@type\": \"ex:Product\", \"ex:name\": \"?name\"}]
    }" -o /dev/null -w "  HTTP %{http_code} (result suppressed)\n"
echo ""

echo "  SPARQL: Aggregation query"
time curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -H "Accept: application/sparql-results+json" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?category (COUNT(?p) AS ?count) (AVG(?price) AS ?avgPrice)
WHERE {
    ?p a ex:Product .
    ?p ex:category ?category .
    ?p ex:price ?price .
}
GROUP BY ?category' | python3 -m json.tool 2>/dev/null
echo ""

echo "=== Stress test complete. Check Jaeger for trace volume and batch behavior ==="
