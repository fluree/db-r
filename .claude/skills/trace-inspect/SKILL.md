---
name: trace-inspect
description: "This skill should be used when the user asks to 'inspect a trace', 'show the span tree', 'analyze trace hierarchy', 'drill into a specific trace', 'what does this trace look like', or provides a Jaeger trace JSON file and wants to understand a single trace's span hierarchy, timing breakdown, or structural health."
---

# Trace Inspect — Single Trace Deep-Dive

Examine one top-level trace from a Jaeger JSON export to understand its span hierarchy, timing breakdown, and structural health.

## Arguments

$ARGUMENTS — Required: path to Jaeger JSON export file. Optional: trace selector (index number, traceID prefix, or operation name to filter by). Optional: problem description or question.

Parse the arguments: extract the file path (first token or quoted path), an optional trace selector (number, hex prefix, or operation pattern), and any remaining text as the user's question.

## Principles

- **Never read raw JSON into the conversation.** Always use the bundled Python scripts via Bash.
- **Progressive disclosure.** Start with the summary tree (Level 1). Escalate to detail or anomaly checks only when needed or requested.
- **Synthesize, don't dump.** Present script output conversationally. Highlight what matters — don't just paste 200 lines of tree output.
- **Cross-reference anomalies** with `references/span-hierarchy.md` before alarming the user. Many patterns that look wrong are expected (cross-thread index builds, zero-duration commit metadata spans).

## Execution Protocol

### Level 1: Summary Tree (always run first)

Run the tree script in summary mode:

```bash
python3 <skill_dir>/scripts/trace_tree.py <FILE> [--trace N] [--op PATTERN]
```

This produces:
- File metadata (size, format, trace count, total spans, service)
- If multiple traces and no selector: a trace list table for the user to pick from
- For selected trace(s): indented span tree with operation name, duration, and percentage of root

Present findings: note the root operation, total duration, and the shape of the tree. Call out anything surprising (unusually many/few spans, unexpected root operation).

### Level 2: Detail Tree (on request or if investigating timing)

Run with `--detail` to add per-span tags, timing gap analysis, and thread crossing detection:

```bash
python3 <skill_dir>/scripts/trace_tree.py <FILE> --trace N --detail
```

This adds:
- Key tags on each span (thread.id, ledger_id, error_code, insert_count, etc.)
- Timing gaps: unaccounted time between children (indicates unlabeled work)
- Thread crossings: parent/child on different threads (expected for indexer, sort_blocking)

Use `--min-duration-ms 1` to filter out sub-millisecond noise on large traces.

### Level 3: Anomaly Detection (on request or if hierarchy looks wrong)

Run the anomaly detection script:

```bash
python3 <skill_dir>/scripts/trace_anomalies.py <FILE> --trace N
```

This checks for: orphaned spans, multiple roots, child > parent duration, unexpected thread crossings, missing expected children, and zero-duration spans.

Before reporting anomalies, read `references/span-hierarchy.md` to determine whether findings are expected or problematic.

## Known-Benign Patterns

Do not alarm the user about these — they are expected behavior:

- **Zero-duration spans** on `commit_namespace_delta`, `commit_build_record`, `commit_verify_sequencing` — these are sub-microsecond metadata operations
- **Cross-thread spans** for `build_index`, `sort_blocking`, `sort_child_next_batch`, `sort_build_rows_batch`, `BinaryIndexStore::load` — these use `spawn_blocking` or `thread::scope`
- **`index_gc` as separate root trace** — intentionally fire-and-forget via `tokio::spawn`
- **Variable span counts in query traces** — query complexity drives operator count (7 spans for simple select, 100+ for sorted/aggregated)
- **`sparql_execute` instead of `query_execute`** — SPARQL queries use a different execution span name

## When to Escalate

- **Multiple root spans in a single trace** → likely `.enter()` guard held across `.await`. Check the code at the root span's `code.file.path` tag.
- **Orphaned spans** → parent context lost, possibly in `spawn_blocking` without `Span::current()` propagation.
- **Large timing gaps** (> 30% of parent) → unlabeled work. Suggest adding a span.
- **Unexpected cross-thread spans** → may indicate missing span propagation in a new `spawn_blocking` or `thread::scope` call.

## Resources

- **`scripts/trace_tree.py`** — primary tree renderer (--summary / --detail modes)
- **`scripts/trace_anomalies.py`** — structural anomaly detection
- **`scripts/trace_common.py`** — shared Jaeger JSON parsing utilities
- **`references/span-hierarchy.md`** — expected hierarchies, tag catalog, thread boundary rules

## Maintenance Note

`trace_common.py` and `references/span-hierarchy.md` are intentionally duplicated between
`trace-inspect/` and `trace-overview/` because Claude skills must be self-contained directories.
When updating these files, **update both copies**.

## Jaeger Export Format Quick Reference

- Top-level: `{ "data": [ { "traceID", "spans": [...], "processes": {...} } ] }`
- Duration: **microseconds**. Tags `busy_ns`/`idle_ns`: nanoseconds.
- Parent-child: `references[{refType: "CHILD_OF", spanID: <parent>}]`
- Two formats: flat (individual exports) and nested (full exports with embedded parent objects in references). Scripts handle both transparently.
