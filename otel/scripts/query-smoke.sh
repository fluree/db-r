#!/usr/bin/env bash
# Run representative queries (FQL + SPARQL) to generate trace data.
set -euo pipefail

PORT="${PORT:-8090}"
LEDGER="${LEDGER:-otel-test:main}"
BASE="http://localhost:${PORT}/v1/fluree"

echo "--- FQL: Select all people"
curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {"ex": "http://example.org/ns/"},
        "select": {"?person": ["*"]},
        "where": [{"@id": "?person", "@type": "ex:Person"}]
    }' | python3 -m json.tool 2>/dev/null | head -5
echo "  ..."

echo ""
echo "--- FQL: Select products with filter (price > 20)"
curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name", "?price"],
        "where": [
            {"@id": "?p", "@type": "ex:Product", "ex:name": "?name", "ex:price": "?price"}
        ]
    }' | python3 -m json.tool 2>/dev/null | head -5
echo "  ..."

echo ""
echo "--- SPARQL: Select all people"
curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -H "Accept: application/sparql-results+json" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?name ?age
WHERE {
    ?person a ex:Person .
    ?person ex:name ?name .
    ?person ex:age ?age .
}
ORDER BY ?name' | python3 -m json.tool 2>/dev/null | head -10
echo "  ..."

echo ""
echo "--- SPARQL: Aggregate query (count people by age range)"
curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -H "Accept: application/sparql-results+json" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT (COUNT(?person) AS ?count)
WHERE {
    ?person a ex:Person .
    ?person ex:age ?age .
}' | python3 -m json.tool 2>/dev/null
echo ""

echo "--- Query smoke tests complete"
