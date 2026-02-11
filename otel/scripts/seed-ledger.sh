#!/usr/bin/env bash
# Create and seed a test ledger with sample data.
set -euo pipefail

PORT="${PORT:-8090}"
LEDGER="${LEDGER:-otel-test:main}"
BASE="http://localhost:${PORT}/v1/fluree"

echo "--- Creating ledger: ${LEDGER}"
curl -sf -X POST "${BASE}/create" \
    -H "Content-Type: application/json" \
    -d "{\"ledger\": \"${LEDGER}\"}" \
    -o /dev/null -w "  create: HTTP %{http_code}\n" || true

echo "--- Inserting schema + sample data"
curl -sf -X POST "${BASE}/${LEDGER}/transact" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {
            "ex":    "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {"@id": "ex:Person",     "@type": "rdfs:Class"},
            {"@id": "ex:Product",    "@type": "rdfs:Class"},
            {"@id": "ex:name",       "@type": "rdf:Property"},
            {"@id": "ex:age",        "@type": "rdf:Property"},
            {"@id": "ex:email",      "@type": "rdf:Property"},
            {"@id": "ex:price",      "@type": "rdf:Property"},
            {"@id": "ex:category",   "@type": "rdf:Property"},
            {"@id": "ex:inStock",    "@type": "rdf:Property"}
        ]
    }' \
    -o /dev/null -w "  schema: HTTP %{http_code}\n"

curl -sf -X POST "${BASE}/${LEDGER}/transact" \
    -H "Content-Type: application/json" \
    -d '{
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id": "ex:alice",   "@type": "ex:Person",  "ex:name": "Alice",   "ex:age": 30, "ex:email": "alice@example.org"},
            {"@id": "ex:bob",     "@type": "ex:Person",  "ex:name": "Bob",     "ex:age": 25, "ex:email": "bob@example.org"},
            {"@id": "ex:charlie", "@type": "ex:Person",  "ex:name": "Charlie", "ex:age": 35, "ex:email": "charlie@example.org"},
            {"@id": "ex:diana",   "@type": "ex:Person",  "ex:name": "Diana",   "ex:age": 28, "ex:email": "diana@example.org"},
            {"@id": "ex:eve",     "@type": "ex:Person",  "ex:name": "Eve",     "ex:age": 42, "ex:email": "eve@example.org"},
            {"@id": "ex:prod1",   "@type": "ex:Product", "ex:name": "Widget A", "ex:price": 9.99,  "ex:category": "widgets",  "ex:inStock": true},
            {"@id": "ex:prod2",   "@type": "ex:Product", "ex:name": "Widget B", "ex:price": 19.99, "ex:category": "widgets",  "ex:inStock": true},
            {"@id": "ex:prod3",   "@type": "ex:Product", "ex:name": "Gadget X", "ex:price": 49.99, "ex:category": "gadgets",  "ex:inStock": false},
            {"@id": "ex:prod4",   "@type": "ex:Product", "ex:name": "Gadget Y", "ex:price": 79.99, "ex:category": "gadgets",  "ex:inStock": true},
            {"@id": "ex:prod5",   "@type": "ex:Product", "ex:name": "Gizmo Z",  "ex:price": 149.99,"ex:category": "gizmos",   "ex:inStock": true}
        ]
    }' \
    -o /dev/null -w "  data: HTTP %{http_code}\n"

echo "--- Seed complete"
