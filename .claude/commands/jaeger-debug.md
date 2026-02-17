# Jaeger Debug

Diagnose tracing issues by analyzing a Jaeger trace export alongside the code that produced those traces. The file shows *what happened*; the code shows *why* and *what's possible*.

## Arguments

$ARGUMENTS — Required: path to the Jaeger JSON export file, followed by a description of the problem or question.

Parse the arguments: extract the file path (first token, or a quoted path) and the problem description (everything after).

## Principles

- **Two-track analysis.** Always investigate both (A) the trace data and (B) the code that produced it. Neither alone is sufficient.
- **Never read the raw JSON into conversation.** Use Python scripts via Bash to extract targeted signal from trace files.
- **Code is the source of truth for what's *possible*.** The trace file shows current behavior; the code + OTEL SDK capabilities show what *could* be done differently.
- **Protect the context window.** Use Explore agents for code investigation (grepping across 37 crates, reading multiple files, checking SDK capabilities). Keep the main session for file analysis results and diagnosis synthesis.
- **Progressive disclosure.** Start with high-level summaries, then drill into specifics guided by the user's problem.
- **End with an actionable diagnosis** that identifies root cause in the code and proposes a concrete fix with awareness of SDK capabilities.

## Execution Strategy: What Runs Where

### Main session (results needed inline for reasoning):
- **All Python scripts** analyzing the trace file — output is compact and directly informs diagnosis
- **Final diagnosis synthesis** — combines findings from both tracks

### Explore agents (bulky code search, summarized on return):
- **Span creation site inventory** — grep all span names found in the trace file across the codebase, return a table of `file:line | span_name | macro_used | fields_set`
- **Runtime context catalog** — read the code around each span creation site, catalog what variables are in scope (HTTP headers, path params, operation enums, content types). Return as a structured table.
- **OTEL SDK capability check** — read Cargo.toml for dependency versions, check what features the `tracing-opentelemetry` version supports (otel.name, otel.kind, otel.status_code, span processors). Return version + feature summary.
- **Doc cross-reference** — check `docs/operations/telemetry.md` span hierarchy, `docs/contributing/tracing-guide.md` patterns, and `dev-docs/deep-tracing-span-catalog.md` against observed traces. Return discrepancy notes.

**Key rule for Explore agents**: Give each agent a *specific extraction task* with a *defined output format*. Not "go explore the tracing code" but "grep for these 5 span names in `*.rs`, read 20 lines around each match, and return a table with columns: file:line, span macro, fields declared, runtime variables in scope within 30 lines." This ensures agent results are concise and slot directly into the diagnosis.

---

## Steps

### Step 1: File Analysis + Code Exploration (parallel)

Launch these simultaneously:

#### 1A. Trace file structure probe + overview (main session, Python via Bash)

Run a single comprehensive Python script that produces:
- File stats (size, trace count, total spans)
- All unique tag keys across the file
- Trace overview table: for each trace, show root span operationName, key tags (operation, ledger_alias, error_code), duration, immediate child span names

This is the "table of contents" for the file. When traces > 50, aggregate by operation type with count and duration stats instead of listing each trace.

```python
import json, os
from collections import Counter

path = "<FILE_PATH>"
print(f"File: {os.path.getsize(path)/1024/1024:.1f} MB")

with open(path) as f:
    data = json.load(f)

traces = data['data']
total_spans = sum(len(t['spans']) for t in traces)
print(f"Traces: {len(traces)} | Spans: {total_spans}")

# All unique tag keys
tag_keys = sorted({t['key'] for tr in traces for sp in tr['spans'] for t in sp.get('tags', [])})
print(f"Tag keys: {tag_keys}\n")

# Trace overview
for i, trace in enumerate(traces):
    spans = trace['spans']
    parent_map = {}
    for s in spans:
        for ref in s.get('references', []):
            if ref.get('refType') == 'CHILD_OF':
                parent_map[s['spanID']] = ref['spanID']
    roots = [s for s in spans if s['spanID'] not in parent_map]
    for root in roots:
        tags = {t['key']: t['value'] for t in root.get('tags', [])}
        children = [s for s in spans if parent_map.get(s['spanID']) == root['spanID']]
        child_ops = [c['operationName'] for c in sorted(children, key=lambda x: x['startTime'])]
        dur_ms = root['duration'] / 1000.0
        print(f"Trace {i+1:3d} | {root['operationName']:<20s} | {dur_ms:8.1f}ms | "
              f"op={tags.get('operation',''):<20s} | children={child_ops}")
```

#### 1B. Code exploration (Explore agent, parallel)

Launch an Explore agent with this specific task:

> **Task**: Investigate the tracing instrumentation code that produces the spans observed in a Jaeger export. I need three things:
>
> 1. **Span creation sites**: Grep for these span names in `*.rs` files: [LIST THE UNIQUE operationNames FROM THE TRACE FILE]. For each match, note the file:line, which macro is used (`info_span!`, `debug_span!`, `trace_span!`, `#[instrument]`), what fields are declared, and what the span name string is.
>
> 2. **Runtime context at span sites**: For the top-level request span creation (likely `create_request_span` or similar in `fluree-db-server/src/routes/`), read the surrounding function signatures and note what runtime values are available (path params, headers, content-type, operation enum, body format) that could enrich the span name or attributes.
>
> 3. **OTEL SDK capabilities**: Check `fluree-db-server/Cargo.toml` for `tracing-opentelemetry` version. That version determines whether `otel.name` (dynamic OTEL span name override), `otel.kind`, and `otel.status_code` fields are supported.
>
> Return a structured summary with these three sections.

Tailor the span name list based on what the trace overview (1A) actually shows. Only grep for names that are relevant to the user's problem.

### Step 2: Problem-Specific Deep Dive (main session)

Based on the user's problem description and the trace overview from 1A, run targeted Python analysis. Choose from these patterns:

**Waterfall dump** — full indented span tree for specific trace(s):
```python
# Walk parent_map recursively, print with indentation, include duration + key tags
```

**Cross-trace comparison** — compare structure of two trace types:
```python
# "Why does trace 5 have transact_execute but trace 27 doesn't?"
# Extract and diff the span name sets or tree structures
```

**Duration outliers** — find and show the slowest traces:
```python
# Sort traces by root span duration, show top N waterfalls
```

**Missing span detection** — check which traces lack expected spans:
```python
expected = ['query_prepare', 'query_run']
for i, trace in enumerate(data['data']):
    names = {s['operationName'] for s in trace['spans']}
    missing = [e for e in expected if e not in names]
    if missing: print(f"Trace {i+1}: missing {missing}")
```

**Tag value extraction** — pull specific attributes from matching spans:
```python
# For spans named X, extract tags Y and Z across all traces
```

**Parent-child validation** — verify nesting matches expected hierarchy from docs:
```python
# "transact_execute should always be child of request"
# "txn_stage should always be child of transact_execute"
```

### Step 3: Synthesis — Combine Both Tracks

At this point you have:
- From 1A: what the traces actually look like (span names, tags, structure, durations)
- From 1B: how the code creates those spans, what runtime context is available, what the SDK supports
- From Step 2: targeted analysis of the specific problem

Combine into a structured diagnosis.

### Step 4: Diagnosis Report

Present findings as:

```
## Diagnosis: <short title>

**Problem**: <what the user observed in Jaeger>

**Root cause**: <what's happening in the code>
- File: <path:line>
- Current behavior: <what the code does>
- Why it produces the observed result: <explanation>

**Evidence from traces**:
- <specific data points from file analysis>
- <counts, patterns, anomalies>

**What the SDK makes possible**:
- <relevant OTEL/tracing features not currently used>
- <runtime context available at span sites but not captured>

**Recommended fix**:
- <concrete approach, referencing specific SDK features>
- <which files and functions to modify>
- <before/after: what Jaeger would show after the fix>

**Secondary findings** (if any):
- <other trace anomalies noticed>
- <discrepancies between docs and observed behavior>
- <issues worth investigating separately>
```

Always end by asking the user whether they'd like to proceed with implementing the fix, investigate a secondary finding, or analyze a different aspect of the traces.

---

## Jaeger Export File Format Reference

- Top-level: `{ "data": [ <trace>, ... ] }`
- Each trace: `{ "traceID", "spans": [...], "processes": {...} }`
- Each span: `{ "operationName", "spanID", "traceID", "startTime" (epoch μs), "duration" (μs), "tags": [{key, value, type}], "references": [{refType, traceID, spanID}], "logs": [...] }`
- Parent-child: `references` with `refType: "CHILD_OF"`, `spanID` = parent
- `processes` maps `processID` → `{ "serviceName", "tags": [...] }`
- Duration is **microseconds** — divide by 1000 for ms, by 1_000_000 for seconds

## Tips

- The trace overview table (1A) is almost always the right starting point — it's cheap and immediately reveals patterns.
- When the file has > 50 traces, aggregate first (group by root span operation tag, show count + min/p50/max duration) before listing individuals.
- When investigating "missing spans", check whether the span is gated behind a log level — `debug_span!` won't appear if RUST_LOG or the OTEL Targets filter doesn't enable that crate at debug. The Explore agent checking B2 should note what Targets filter is configured in telemetry.rs.
- Secondary findings are valuable — trace files are expensive to produce. Extract maximum value from each one.
- If the user's problem is about span *content* (wrong field values, missing attributes), the code exploration is more important than the file analysis. If it's about span *structure* (wrong nesting, missing spans, naming), both tracks are equally important.
