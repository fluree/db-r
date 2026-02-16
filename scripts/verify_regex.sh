#!/usr/bin/env bash
set -euo pipefail

# REGEX Verification Script
# Tests whether the 128 vs 513 discrepancy is due to data differences or a REGEX bug.

DB_DIR="${DB_DIR:-/Volumes/External-4TB-OWCEnvoy/dblp/fluree-data}"
LEDGER="${LEDGER:-dblp}"
RUST_LOG="${RUST_LOG:-fluree_ingest=info,fluree_db_api::view::query=info,fluree_db_query=info}"

echo "Building fluree-db-ingest (release)..."
cargo build --release -p fluree-db-ingest 2>&1 | tail -5

BINARY="$(cargo metadata --format-version=1 --no-deps 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['target_directory'])")/release/fluree-ingest"

if [ ! -x "$BINARY" ]; then
  echo "ERROR: binary not found at $BINARY"
  exit 1
fi
echo "Binary: $BINARY"
echo ""

run_query() {
  local name="$1"
  local sparql="$2"
  echo "════════════════════════════════════════════════════════════════════════════════"
  echo "Query: $name"
  echo "════════════════════════════════════════════════════════════════════════════════"
  echo "SPARQL: $sparql"
  echo ""

  TMPOUT=$(mktemp)
  set +e
  RUST_LOG="$RUST_LOG" "$BINARY" \
    --db-dir "$DB_DIR" \
    --ledger "$LEDGER" \
    --sparql "$sparql" \
    >"$TMPOUT" 2>&1
  EXIT_CODE=$?
  set -e

  if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR (exit $EXIT_CODE):"
    tail -10 "$TMPOUT"
  else
    # Count result rows (lines starting with a value, not ? header)
    HEADER=$(grep '^?' "$TMPOUT" | head -1 || true)
    if [ -n "$HEADER" ]; then
      # Count lines that don't start with ? or INFO/DEBUG/etc
      ROWS=$(grep -v '^?' "$TMPOUT" | grep -v '^\s*$' | grep -v 'INFO\|DEBUG\|WARN\|ERROR\|fluree' | wc -l | tr -d ' ')
      COLS=$(echo "$HEADER" | awk -F'\t' '{print NF}')
      echo "Result: $ROWS rows x $COLS columns"
      echo ""
      echo "First 10 results:"
      grep -v '^?' "$TMPOUT" | grep -v '^\s*$' | grep -v 'INFO\|DEBUG\|WARN\|ERROR\|fluree' | head -10
    else
      echo "No header found in output"
      tail -20 "$TMPOUT"
    fi
  fi
  rm -f "$TMPOUT"
  echo ""
}

echo ""
echo "=================================================================================="
echo "  REGEX Verification Test Suite"
echo "  DB: $DB_DIR   Ledger: $LEDGER"
echo "=================================================================================="
echo ""

# Query 1: Total unique SIGIR authors (no filter)
# This tells us the total population before REGEX filtering
run_query "Total SIGIR authors (no filter)" \
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?author ?author_label (COUNT(?paper) as ?count) WHERE { ?paper dblp:authoredBy ?author . ?paper dblp:publishedIn "SIGIR" . ?author rdfs:label ?author_label . } GROUP BY ?author ?author_label ORDER BY DESC(?count)'

# Query 2: REGEX filter (the problematic one)
run_query "SIGIR authors matching REGEX M.*D.* (original query)" \
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?author ?author_label ?count WHERE { { SELECT ?author ?author_label (COUNT(?paper) as ?count) WHERE { ?paper dblp:authoredBy ?author . ?paper dblp:publishedIn "SIGIR" . ?author rdfs:label ?author_label . } GROUP BY ?author ?author_label } FILTER REGEX(STR(?author_label), "M.*D.*", "i") } ORDER BY DESC(?count)'

# Query 3: CONTAINS-based approximation
# If REGEX is broken, CONTAINS should still work (it doesn't need complex pattern matching)
run_query "SIGIR authors containing 'M' (simple CONTAINS)" \
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?author ?author_label ?count WHERE { { SELECT ?author ?author_label (COUNT(?paper) as ?count) WHERE { ?paper dblp:authoredBy ?author . ?paper dblp:publishedIn "SIGIR" . ?author rdfs:label ?author_label . } GROUP BY ?author ?author_label } FILTER CONTAINS(LCASE(STR(?author_label)), "m") } ORDER BY DESC(?count)'

# Query 4: Test that STR() is working on encoded literals
run_query "SIGIR authors with non-empty label (testing STR function)" \
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?author ?author_label ?count WHERE { { SELECT ?author ?author_label (COUNT(?paper) as ?count) WHERE { ?paper dblp:authoredBy ?author . ?paper dblp:publishedIn "SIGIR" . ?author rdfs:label ?author_label . } GROUP BY ?author ?author_label } FILTER (STRLEN(STR(?author_label)) > 0) } ORDER BY DESC(?count)'

echo ""
echo "=================================================================================="
echo "  Analysis"
echo "=================================================================================="
echo ""
echo "Compare the results above:"
echo "  - Query 1 shows total SIGIR authors available in our data"
echo "  - Query 2 shows REGEX filter results (currently 128)"
echo "  - Query 3 shows CONTAINS results (simpler filter, should work)"
echo "  - Query 4 tests STR() function on encoded values"
echo ""
echo "If Query 1 shows ~7000+ authors and Query 4 matches, but Query 2/3 show fewer,"
echo "then there may still be a binary_store propagation issue for certain filter paths."
echo ""
