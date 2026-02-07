#!/usr/bin/env bash
# Exercise all query API paths. Assumes seed-ledger.sh has run.
# Usage: query-smoke.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

echo "=== Query Smoke Test ==="
echo "Server: ${BASE_URL}"
echo "Ledger: ${LEDGER}"
echo ""

# Helper: run a curl query and report status + row count
run_query() {
    local label="$1"; shift
    local resp
    resp=$(curl -s -w "\n%{http_code}" "$@")
    local code
    code=$(echo "$resp" | tail -n1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [ "$code" = "200" ]; then
        # Estimate row count from JSON array
        local rows
        rows=$(echo "$body" | grep -o '"@id"' | wc -l | tr -d ' ')
        echo "  [PASS] ${label} (HTTP ${code}, ~${rows} results)"
    else
        echo "  [FAIL] ${label} (HTTP ${code})"
        echo "         ${body}" | head -3
    fi
    sleep 0.5
}

# 1. FQL select — simple pattern
echo "1. FQL select (simple)..."
run_query "FQL simple select" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": {"?product": ["*"]},
  "where": [{"@id": "?product", "@type": "ex:Product", "ex:name": "?name"}],
  "limit": 10
}'

# 2. FQL with filter — price range
echo "2. FQL with filter (price range)..."
run_query "FQL filter" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": ["?name", "?price"],
  "where": [
    {"@id": "?product", "@type": "ex:Product", "ex:name": "?name", "ex:price": "?price"},
    ["filter", "(> ?price 50)"]
  ],
  "limit": 20
}'

# 3. FQL with sort — ORDER BY price DESC
echo "3. FQL with sort (ORDER BY price DESC)..."
run_query "FQL sort" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": ["?name", "?price"],
  "where": [{"@id": "?product", "@type": "ex:Product", "ex:name": "?name", "ex:price": "?price"}],
  "orderBy": [{"desc": "?price"}],
  "limit": 10
}'

# 4. SPARQL select — basic triple pattern
echo "4. SPARQL select (basic)..."
run_query "SPARQL basic" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?name ?sku
WHERE {
    ?product a ex:Product ;
             ex:name ?name ;
             ex:sku ?sku .
}
LIMIT 10'

# 5. SPARQL with OPTIONAL + FILTER
echo "5. SPARQL OPTIONAL + FILTER..."
run_query "SPARQL OPTIONAL+FILTER" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?name ?price ?relatedName
WHERE {
    ?product a ex:Product ;
             ex:name ?name ;
             ex:price ?price ;
             ex:inStock true .
    OPTIONAL {
        ?product ex:relatedTo ?related .
        ?related ex:name ?relatedName .
    }
}
LIMIT 20'

# 6. SPARQL with GROUP BY
echo "6. SPARQL GROUP BY..."
run_query "SPARQL GROUP BY" \
    -X POST "${BASE_URL}/${LEDGER}/query" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?catLabel (COUNT(?product) AS ?productCount)
WHERE {
    ?product a ex:Product ;
             ex:category ?cat .
    ?cat ex:label ?catLabel .
}
GROUP BY ?catLabel
ORDER BY DESC(?productCount)'

# 7. Global query (connection-scoped, uses "from")
echo "7. Global FQL query (connection-scoped)..."
run_query "Global FQL query" \
    -X POST "${BASE_URL}/fluree/query" \
    -H "Content-Type: application/json" \
    -d "{
  \"@context\": {\"ex\": \"http://example.org/ns/\"},
  \"from\": \"${LEDGER}\",
  \"select\": [\"?name\"],
  \"where\": [{\"@id\": \"?s\", \"@type\": \"ex:Category\", \"ex:label\": \"?name\"}]
}"

echo ""
echo "Query smoke test complete."
echo "Check Jaeger for query_execute > query_prepare > query_run > operator spans."
