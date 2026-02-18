---
name: trace-overview
description: "This skill should be used when the user asks to 'overview all traces', 'summarize trace data', 'what patterns are in the traces', 'aggregate trace stats', 'compare trace types', 'find anomalies across traces', or provides a Jaeger trace export and wants high-level understanding of all traces rather than drilling into one specific trace."
---

# Trace Overview — Multi-Trace Aggregate Analysis

Analyze all traces in a Jaeger JSON export: classify by operation type, compute duration statistics, detect structural anomaly patterns across the corpus.

## Arguments

$ARGUMENTS — Required: path to Jaeger JSON export file. Optional: question or focus area.

Parse the arguments: extract the file path (first token or quoted path) and any remaining text as the user's question or focus.

## Principles

- **Never read raw JSON into the conversation.** Always use the bundled Python scripts via Bash.
- **Progressive disclosure.** Start with aggregate stats (Level 1). Show per-trace detail and anomalies only when warranted.
- **Synthesize, don't dump.** Call out the interesting patterns — operation mix, duration distribution, structural consistency. Don't just paste the table.
- **Scale-aware.** For files with > 100 traces, aggregate only by default. For > 1000 traces, warn about load time and suggest `--op-filter`.

## Execution Protocol

### Level 1: Aggregate Summary (always run first)

Run the summary script:

```bash
python3 <skill_dir>/scripts/trace_summary.py <FILE>
```

This produces:
- File metadata (size, format, trace count, total spans, services)
- Operation type breakdown: count, average spans, min/p50/p95/max duration
- Per-trace table (auto for < 50 traces): index, root op, span count, duration, error, ledger
- Span name frequency (top 15)
- Tag coverage summary

Present findings: note the operation mix, whether duration distributions are tight or scattered, and any obvious anomalies (error traces, multi-root traces).

Use `--op-filter PATTERN` to focus on one operation type:

```bash
python3 <skill_dir>/scripts/trace_summary.py <FILE> --op-filter "query"
```

### Level 2: Focused Per-Trace Detail (on request)

For large files where per-trace was suppressed, request it explicitly:

```bash
python3 <skill_dir>/scripts/trace_summary.py <FILE> --per-trace --top 30
```

Or filter to a specific operation type:

```bash
python3 <skill_dir>/scripts/trace_summary.py <FILE> --per-trace --op-filter "insert"
```

### Level 3: Cross-Trace Anomaly Detection (on request or if patterns look wrong)

Run the corpus anomaly script:

```bash
python3 <skill_dir>/scripts/trace_corpus_anomalies.py <FILE>
```

This checks across all traces for:
- Duration outliers (> 2 stddev from operation-type mean)
- Structural outliers (different span counts for same operation type)
- Orphaned spans or multiple roots in any trace
- Error traces (spans with error_code tag)
- Missing expected children inconsistencies

Use `--verbose` for per-anomaly detail.

## Interpretation Guide

### Normal Patterns

- **`insert:fql` traces** should have ~17 spans with tight duration variance (I/O bound: `commit_write_commit_blob` dominates)
- **`query:fql`/`query:sparql` traces** have variable span counts (7 for simple select, 100+ for sorted/aggregated). Duration variance is expected.
- **`index_build` traces** should have ~9 spans. Duration depends on novelty size.
- **`ledger:exists`/`ledger:create` traces** are 1-2 spans (no children). Duration under 1ms for exists.
- **`transact:*` and `upsert:*`** share the `transact_execute > txn_stage + txn_commit` structure with `insert:*`.

### Red Flags

- **Traces with multiple roots** — indicates async context loss (`.enter()` over `.await`). Use `trace-inspect` to drill in.
- **Orphaned spans** — parent context was lost, possibly missing span propagation in `spawn_blocking`.
- **Duration outliers** — may indicate contention (index lock wait), I/O spikes, or GC pressure.
- **Structural outliers** — different span counts for same op type may indicate code path divergence (e.g., error short-circuits).
- **Error traces** — check `error_code` tag values. Common: `error:LedgerNotFound`, `error:InvalidQuery`.

### Known-Benign Patterns

- **`index_gc` as separate root traces** — intentionally spawned via `tokio::spawn` (fire-and-forget)
- **Variable query span counts** — driven by query complexity, not a structural problem
- **Zero-duration spans on commit metadata ops** — sub-microsecond, normal

## Large File Handling

| File size | Traces | Default behavior |
|-----------|--------|-----------------|
| < 1 MB | < 50 | Full output: aggregate + per-trace + names + tags |
| 1-50 MB | 50-500 | Aggregate only. Per-trace on request (`--per-trace`). |
| > 50 MB | > 500 | Aggregate only. Suggest `--op-filter` to focus. Warn about load time. |

For stress test files (50K+ traces), always use `--op-filter` and `--top N`.

## Drilling Into Specific Traces

When the overview reveals an interesting trace (outlier, error, anomaly), switch to the `trace-inspect` skill to examine its span tree in detail:

```bash
python3 <trace-inspect_skill_dir>/scripts/trace_tree.py <FILE> --trace N --detail
```

## Resources

- **`scripts/trace_summary.py`** — primary aggregate analysis (--per-trace, --op-filter, --top)
- **`scripts/trace_corpus_anomalies.py`** — cross-trace anomaly detection (--verbose)
- **`scripts/trace_common.py`** — shared Jaeger JSON parsing utilities
- **`references/anomaly-catalog.md`** — anomaly types, root causes, code cross-references

## Jaeger Export Format Quick Reference

- Top-level: `{ "data": [ { "traceID", "spans": [...], "processes": {...} } ] }`
- Duration: **microseconds**. Tags `busy_ns`/`idle_ns`: nanoseconds.
- Parent-child: `references[{refType: "CHILD_OF", spanID: <parent>}]`
- Two formats: flat (individual exports) and nested (full exports). Scripts handle both.
