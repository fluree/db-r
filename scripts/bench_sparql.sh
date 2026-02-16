#!/usr/bin/env bash
set -euo pipefail

# DBLP SPARQL Benchmark Script
# Runs 6 queries against Fluree binary indexes and compares with published benchmarks.

DB_DIR="${DB_DIR:-/Volumes/External-4TB-OWCEnvoy/dblp/fluree-data}"
LEDGER="${LEDGER:-dblp}"
# Optional cargo features for fluree-db-ingest build (e.g. FEATURES=otel)
FEATURES="${FEATURES:-}"
# Force a consistent log filter so timing lines are always present
RUST_LOG="${RUST_LOG:-fluree_ingest=info,fluree_db_api::view::query=info,fluree_db_query=info,fluree_db_sparql=info,fluree_db_indexer=info}"

# ── Build once ───────────────────────────────────────────────────────────────

echo "Building fluree-db-ingest (release)..."
BUILD_FEATURES=()
if [ -n "$FEATURES" ]; then
  BUILD_FEATURES=(--features "$FEATURES")
fi
# NOTE: on some bash versions, expanding an empty array with `set -u` can error.
if [ "${#BUILD_FEATURES[@]}" -gt 0 ]; then
  cargo build --release -p fluree-db-ingest "${BUILD_FEATURES[@]}" 2>&1
else
  cargo build --release -p fluree-db-ingest 2>&1
fi
BINARY="$(cargo metadata --format-version=1 --no-deps 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['target_directory'])")/release/fluree-ingest"

if [ ! -x "$BINARY" ]; then
  echo "ERROR: binary not found at $BINARY"
  exit 1
fi
echo "Binary: $BINARY"
echo ""

# ── Query definitions ────────────────────────────────────────────────────────

QUERY_NAMES=(
  "All papers published in SIGIR"
  "Number of papers by venue"
  "Author names matching REGEX"
  "All papers in DBLP until 1940"
  "All papers with their title"
  "All predicates ordered by size"
)

QUERIES=(
  # 1. All papers published in SIGIR
  'PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?paper ?title ?year WHERE { ?paper dblp:title ?title . ?paper dblp:publishedIn "SIGIR" . ?paper dblp:yearOfPublication ?year } ORDER BY DESC(?year)'

  # 2. Number of papers by venue
  'PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?venue (COUNT(?paper) as ?count) WHERE { ?paper dblp:publishedIn ?venue . } GROUP BY ?venue ORDER BY DESC(?count)'

  # 3. Author names matching REGEX
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?author ?author_label ?count WHERE { { SELECT ?author ?author_label (COUNT(?paper) as ?count) WHERE { ?paper dblp:authoredBy ?author . ?paper dblp:publishedIn "SIGIR" . ?author rdfs:label ?author_label . } GROUP BY ?author ?author_label } FILTER REGEX(STR(?author_label), "M.*D.*", "i") } ORDER BY DESC(?count)'

  # 4. All papers in DBLP until 1940
  'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dblp: <https://dblp.org/rdf/schema#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?title ?author ?author_label ?year WHERE { ?paper dblp:title ?title . ?paper dblp:authoredBy ?author . ?paper dblp:yearOfPublication ?year . ?author rdfs:label ?author_label . FILTER (?year <= "1940"^^xsd:gYear) } ORDER BY ASC(?year) ASC(?title)'

  # 5. All papers with their title
  'PREFIX dblp: <https://dblp.org/rdf/schema#> SELECT ?paper ?title WHERE { ?paper dblp:title ?title . }'

  # 6. All predicates ordered by size
  'SELECT ?predicate (COUNT(?subject) as ?count) WHERE { ?subject ?predicate ?object } GROUP BY ?predicate ORDER BY DESC(?count)'
)

# Reference result shapes (rows x cols) from original benchmark
REF_SHAPES=("6264 x 3" "19954 x 2" "513 x 3" "70 x 4" "7167122 x 2" "68 x 3")

# Benchmark times: Oxigraph, Jena, Stardog, GraphDB, Blazegraph, Virtuoso, QLever
BENCH_TIMES=(
  "1.6 0.3 0.52 0.17 0.47 0.54 0.02"
  "2.6 28 2.0 3.1 1.2 1.0 0.02"
  "5.6 4.8 0.61 0.29 0.27 0.98 0.05"
  "313 50 16 0.04 5.9 0.08 0.11"
  "132 54 44 20 18 9.1 4.2"
  "106 279 37 72 0.05 1.48 0.01"
)

# ── Helpers ──────────────────────────────────────────────────────────────────

avg() {
  local nums=($@)
  local sum=0 n=${#nums[@]}
  for v in "${nums[@]}"; do
    sum=$(echo "$sum + $v" | bc -l)
  done
  echo "scale=2; $sum / $n" | bc -l
}

fmt_time() {
  local t="$1"
  local secs
  secs=$(echo "$t" | bc -l 2>/dev/null || echo "0")
  local int_part
  int_part=$(echo "$secs / 1" | bc)
  if [ "$int_part" -ge 60 ] 2>/dev/null; then
    local mins=$((int_part / 60))
    local remainder
    remainder=$(echo "scale=1; $secs - ($mins * 60)" | bc -l)
    echo "${mins}m ${remainder}s"
  elif (( $(echo "$secs < 1" | bc -l) )); then
    printf "%.3fs" "$secs"
  else
    printf "%.2fs" "$secs"
  fi
}

# ── Temp file cleanup ────────────────────────────────────────────────────────

TMPDIR_BENCH=$(mktemp -d)
echo "Temp output dir: $TMPDIR_BENCH"

# Set KEEP_TMP=1 to preserve outputs for debugging.
KEEP_TMP="${KEEP_TMP:-0}"
cleanup() {
  if [ "$KEEP_TMP" = "1" ]; then
    echo "KEEP_TMP=1 set; leaving temp output dir: $TMPDIR_BENCH"
    return 0
  fi
  rm -rf "$TMPDIR_BENCH"
}
trap cleanup EXIT

# ── Run benchmarks ──────────────────────────────────────────────────────────

echo "================================================================================"
echo "  Fluree DBLP SPARQL Benchmark"
echo "  DB: $DB_DIR   Ledger: $LEDGER"
echo "================================================================================"
echo ""

declare -a FLUREE_TIMES
declare -a FLUREE_SHAPES
declare -a FLUREE_STATUSES

for i in "${!QUERIES[@]}"; do
  idx=$((i + 1))
  name="${QUERY_NAMES[$i]}"
  sparql="${QUERIES[$i]}"

  echo "────────────────────────────────────────────────────────────────────────────────"
  echo "[$idx/${#QUERIES[@]}] $name"
  echo "────────────────────────────────────────────────────────────────────────────────"

  OUTFILE="$TMPDIR_BENCH/q${idx}_output.txt"
  CLEANFILE="$TMPDIR_BENCH/q${idx}_clean.txt"

  set +e
  RUST_LOG="$RUST_LOG" "$BINARY" \
    --db-dir "$DB_DIR" \
    --ledger "$LEDGER" \
    --sparql "$sparql" \
    >"$OUTFILE" 2>&1
  EXIT_CODE=$?
  set -e

  # Normalize output for parsing:
  # - decode as UTF-8 with replacement (some output may contain non-UTF8 bytes)
  # - strip ANSI color codes from tracing output
  python3 - <<PY
import re
from pathlib import Path
src = Path(r"$OUTFILE")
dst = Path(r"$CLEANFILE")
s = src.read_bytes().decode("utf-8", errors="replace")
s = re.sub(r"\\x1b\\[[0-9;]*m", "", s)
dst.write_text(s, encoding="utf-8")
PY

  if [ $EXIT_CODE -ne 0 ]; then
    echo "  ERROR (exit code $EXIT_CODE)"
    tail -5 "$CLEANFILE"
    FLUREE_TIMES+=("ERR")
    FLUREE_SHAPES+=("ERROR")
    FLUREE_STATUSES+=("FAIL")
    echo ""
    continue
  fi

  # Extract timing from current tracing output:
  #   fluree_ingest: "Query: N rows in X.XXXs"   (end-to-end wall clock for query_view)
  #   fluree_db_api::view::query: "query_view phases parse_ms=.. plan_ms=.. exec_ms=.."
  #   fluree_ingest: "View loaded in X.XXXs"    (BinaryIndexStore + view setup)
  VIEW_LINE=$(grep -E "fluree_ingest: View loaded in [0-9.]+s" "$CLEANFILE" | tail -1 || true)
  if [ -n "$VIEW_LINE" ]; then
    VIEW_LOAD_S=$(echo "$VIEW_LINE" | sed -E 's/.*View loaded in ([0-9.]+)s.*/\1/')
  else
    VIEW_LOAD_S="?"
  fi

  # Example:
  #   2026-02-05T00:59:19.725094Z  INFO fluree_ingest: Query: 7240 rows in 0.245s
  QUERY_LINE=$(grep -E "fluree_ingest: Query: [0-9]+ rows in [0-9.]+s" "$CLEANFILE" | tail -1 || true)
  if [ -n "$QUERY_LINE" ]; then
    ROWS=$(echo "$QUERY_LINE" | sed -E 's/.*Query: ([0-9]+) rows.*/\1/')
    QUERY_TIME_S=$(echo "$QUERY_LINE" | sed -E 's/.*rows in ([0-9.]+)s.*/\1/')
  else
    ROWS="?"
    QUERY_TIME_S="?"
  fi

  # Fallback: some runs may suppress the fluree_ingest target; the runner emits:
  #   ... fluree_db_query::execute::runner: query execution completed ... total_rows=7240 ... total_ms=242 ...
  if [ "$ROWS" = "?" ]; then
    DONE_LINE=$(grep -E "fluree_db_query::execute::runner: query execution completed.*total_rows=" "$CLEANFILE" | tail -1 || true)
    if [ -n "$DONE_LINE" ]; then
      ROWS=$(echo "$DONE_LINE" | sed -E 's/.*total_rows=([0-9]+).*/\1/')
      DONE_MS=$(echo "$DONE_LINE" | sed -E 's/.*total_ms=([0-9]+).*/\1/')
      if [ -n "${DONE_MS:-}" ]; then
        QUERY_TIME_S=$(echo "scale=6; $DONE_MS / 1000.0" | bc -l)
      fi
    fi
  fi

  # Example:
  #   ... INFO fluree_db_api::view::query: query_view phases parse_ms="1.44" plan_ms="0.08" exec_ms="243.74"
  PHASE_LINE=$(grep -E "fluree_db_api::view::query: query_view phases" "$CLEANFILE" | tail -1 || true)
  if [ -n "$PHASE_LINE" ]; then
    PARSE_MS=$(echo "$PHASE_LINE" | sed -E 's/.*parse_ms="([^"]+)".*/\1/')
    PLAN_MS=$(echo "$PHASE_LINE" | sed -E 's/.*plan_ms="([^"]+)".*/\1/')
    EXEC_MS=$(echo "$PHASE_LINE" | sed -E 's/.*exec_ms="([^"]+)".*/\1/')
    PP_TIME_S=$(echo "scale=6; ($PARSE_MS + $PLAN_MS) / 1000.0" | bc -l)
    EXEC_TIME_S=$(echo "scale=6; $EXEC_MS / 1000.0" | bc -l)
    TOTAL_TIME_S=$(echo "scale=6; $PP_TIME_S + $EXEC_TIME_S" | bc -l)
  else
    PP_TIME_S="?"
    EXEC_TIME_S="$QUERY_TIME_S"
    TOTAL_TIME_S="$QUERY_TIME_S"
  fi

  # Count columns from the header line (starts with ?)
  HEADER=$(grep '^?' "$CLEANFILE" | head -1 || true)
  if [ -n "$HEADER" ]; then
    COLS=$(echo "$HEADER" | awk -F'\t' '{print NF}')
  else
    COLS="?"
  fi

  SHAPE="${ROWS} x ${COLS}"
  echo "  Result shape: $SHAPE  (reference: ${REF_SHAPES[$i]})"
  if [ "$EXEC_TIME_S" != "?" ]; then
    echo "  Execute time (engine): $(fmt_time "$EXEC_TIME_S")  (total with parse+plan: $(fmt_time "$TOTAL_TIME_S"))"
    if [ "$VIEW_LOAD_S" != "?" ] && [ "$QUERY_TIME_S" != "?" ]; then
      echo "  Wall time (view+query): $(fmt_time "$(echo "$VIEW_LOAD_S + $QUERY_TIME_S" | bc -l)")  (view: $(fmt_time "$VIEW_LOAD_S"), query_view: $(fmt_time "$QUERY_TIME_S"))"
    fi
  else
    echo "  Execute time: ?"
  fi

  FLUREE_TIMES+=("$EXEC_TIME_S")
  FLUREE_SHAPES+=("$SHAPE")
  FLUREE_STATUSES+=("OK")

  echo ""
done

# ── Summary table ────────────────────────────────────────────────────────────

echo ""
echo "================================================================================"
echo "  RESULTS SUMMARY"
echo "================================================================================"
echo ""

# Column widths
NAME_W=36
SHAPE_W=14
REF_W=14
FLUREE_W=10
AVG_W=10

printf "%-${NAME_W}s %-${SHAPE_W}s %-${REF_W}s %-${FLUREE_W}s %-${AVG_W}s\n" \
  "Query" "Our Shape" "Ref Shape" "Fluree" "Avg (7)"
printf "%s\n" "$(printf '─%.0s' $(seq 1 $((NAME_W + SHAPE_W + REF_W + FLUREE_W + AVG_W + 4))))"

for i in "${!QUERY_NAMES[@]}"; do
  name="${QUERY_NAMES[$i]}"
  shape="${FLUREE_SHAPES[$i]}"
  ref="${REF_SHAPES[$i]}"

  if [ "${FLUREE_STATUSES[$i]}" = "OK" ]; then
    ftime=$(fmt_time "${FLUREE_TIMES[$i]}")
  else
    ftime="ERROR"
  fi

  bench_avg=$(avg ${BENCH_TIMES[$i]})
  bavg_fmt=$(fmt_time "$bench_avg")

  printf "%-${NAME_W}s %-${SHAPE_W}s %-${REF_W}s %-${FLUREE_W}s %-${AVG_W}s\n" \
    "$name" "$shape" "$ref" "$ftime" "$bavg_fmt"
done

echo ""
echo "Note: 'Avg (7)' is the average across Oxigraph, Jena, Stardog, GraphDB, Blazegraph, Virtuoso, QLever."
echo "      Fluree times are execute-only (exec_ms from query_view phases when available)."
echo ""
