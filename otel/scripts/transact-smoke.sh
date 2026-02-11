#!/usr/bin/env bash
# Run representative transactions (insert, update, upsert) to generate trace data.
set -euo pipefail

PORT="${PORT:-8090}"
LEDGER="${LEDGER:-otel-test:main}"
BASE="http://localhost:${PORT}/v1/fluree"

echo "--- Insert: Add new person"
curl -sf -X POST "${BASE}/${LEDGER}/insert" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:frank", "@type": "ex:Person", "ex:name": "Frank", "ex:age": 55, "ex:email": "frank@example.org"}
        ]
    }' -o /dev/null -w "  insert: HTTP %{http_code}\n"

echo "--- Transact: Update person age (WHERE + DELETE + INSERT)"
curl -sf -X POST "${BASE}/${LEDGER}/transact" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {"ex": "http://example.org/ns/"},
        "where": [{"@id": "ex:alice", "ex:age": "?oldAge"}],
        "delete": [{"@id": "ex:alice", "ex:age": "?oldAge"}],
        "insert": [{"@id": "ex:alice", "ex:age": 31}]
    }' -o /dev/null -w "  update: HTTP %{http_code}\n"

echo "--- Upsert: Upsert product"
curl -sf -X POST "${BASE}/${LEDGER}/upsert" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:prod1", "@type": "ex:Product", "ex:name": "Widget A v2", "ex:price": 12.99, "ex:category": "widgets", "ex:inStock": true}
        ]
    }' -o /dev/null -w "  upsert: HTTP %{http_code}\n"

echo "--- Insert via Turtle"
curl -sf -X POST "${BASE}/${LEDGER}/insert" \
    -H "Content-Type: text/turtle" \
    -d '@prefix ex: <http://example.org/ns/> .
ex:grace a ex:Person ;
    ex:name "Grace" ;
    ex:age 33 ;
    ex:email "grace@example.org" .
' -o /dev/null -w "  turtle insert: HTTP %{http_code}\n"

echo "--- Transact smoke tests complete"
