"""Shared utilities for Jaeger trace JSON analysis.

Handles both export formats:
- Flat: individual trace exports (references have spanID only)
- Nested/detail: full exports (references embed parent span objects,
  spans have depth/childSpanIds/hasChildren/relativeStartTime/process)

Duration unit: microseconds (Jaeger standard).
Tags busy_ns/idle_ns are nanoseconds.
"""

import json
import os
import sys
from collections import defaultdict


def load_traces(path):
    """Load Jaeger JSON export, return (traces, metadata).

    metadata keys: file_size, format, trace_count, total_spans, services
    """
    file_size = os.path.getsize(path)
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: {path} is not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    traces = data.get("data", [])
    fmt = detect_format(traces)
    total_spans = sum(len(t.get("spans", [])) for t in traces)

    # Extract service names
    services = set()
    for t in traces:
        procs = t.get("processes", {})
        for p in procs.values():
            if isinstance(p, dict):
                services.add(p.get("serviceName", "unknown"))
        # Nested format: process is inlined on spans
        if fmt == "nested":
            for s in t.get("spans", []):
                proc = s.get("process")
                if isinstance(proc, dict):
                    services.add(proc.get("serviceName", "unknown"))

    meta = {
        "file_size": file_size,
        "format": fmt,
        "trace_count": len(traces),
        "total_spans": total_spans,
        "services": sorted(services) if services else ["unknown"],
    }
    return traces, meta


def detect_format(traces):
    """Detect flat vs nested format by checking spans for nested-only fields."""
    for t in traces:
        for s in t.get("spans", []):
            if "depth" in s or "childSpanIds" in s:
                return "nested"
    return "flat"


def build_span_index(spans):
    """Build lookup structures from a trace's span list.

    Returns dict:
      span_by_id:    {spanID: span}
      parent_map:    {child_spanID: parent_spanID}
      children_map:  {parent_spanID: [child spans sorted by startTime]}
      roots:         [spans with no parent in this trace]
      orphans:       [spans whose parent spanID not found in span_by_id]
    """
    span_by_id = {s["spanID"]: s for s in spans}
    parent_map = {}
    for s in spans:
        for ref in s.get("references", []):
            if ref.get("refType") == "CHILD_OF":
                parent_map[s["spanID"]] = ref["spanID"]

    children_map = defaultdict(list)
    roots = []
    orphans = []

    for s in spans:
        sid = s["spanID"]
        if sid in parent_map:
            pid = parent_map[sid]
            if pid in span_by_id:
                children_map[pid].append(s)
            else:
                orphans.append(s)
        else:
            roots.append(s)

    # Sort children by startTime
    for pid in children_map:
        children_map[pid].sort(key=lambda s: s.get("startTime", 0))

    # Sort roots by startTime
    roots.sort(key=lambda s: s.get("startTime", 0))

    return {
        "span_by_id": span_by_id,
        "parent_map": parent_map,
        "children_map": dict(children_map),
        "roots": roots,
        "orphans": orphans,
    }


def get_tag(span, key, default=None):
    """Extract a single tag value from span's tags array."""
    for t in span.get("tags", []):
        if t["key"] == key:
            return t["value"]
    return default


def get_tags_dict(span):
    """Convert span.tags array to {key: value} dict."""
    return {t["key"]: t["value"] for t in span.get("tags", [])}


def format_duration(us):
    """Smart duration formatting from microseconds."""
    if us is None or us < 0:
        return "???"
    if us == 0:
        return "0us"
    if us < 1000:
        return f"{round(us)}us"
    if us < 1_000_000:
        return f"{us/1000:.1f}ms"
    return f"{us/1_000_000:.2f}s"


def format_size(nbytes):
    """Smart file size formatting."""
    if nbytes < 1024:
        return f"{nbytes} B"
    if nbytes < 1024 * 1024:
        return f"{nbytes/1024:.1f} KB"
    return f"{nbytes/(1024*1024):.1f} MB"


def walk_tree(span_id, index, visitor_fn, depth=0):
    """DFS walk of span tree, calling visitor_fn(span, depth, index) at each node."""
    span = index["span_by_id"].get(span_id)
    if not span:
        return
    visitor_fn(span, depth, index)
    for child in index["children_map"].get(span_id, []):
        walk_tree(child["spanID"], index, visitor_fn, depth + 1)


def get_root_op_name(trace, index=None):
    """Get the operation name of the root span for a trace."""
    if index is None:
        index = build_span_index(trace.get("spans", []))
    if index["roots"]:
        return index["roots"][0]["operationName"]
    return "<no root>"


def select_trace(traces, trace_idx=None, trace_id_prefix=None, op_pattern=None):
    """Select trace(s) from the list by index, traceID prefix, or operation pattern.

    Returns list of (index, trace) tuples.
    """
    if trace_idx is not None:
        if 0 <= trace_idx < len(traces):
            return [(trace_idx, traces[trace_idx])]
        print(f"Error: trace index {trace_idx} out of range (0-{len(traces)-1})", file=sys.stderr)
        sys.exit(1)

    if trace_id_prefix:
        prefix = trace_id_prefix.lower()
        matches = [(i, t) for i, t in enumerate(traces) if t["traceID"].lower().startswith(prefix)]
        if not matches:
            print(f"Error: no trace with ID prefix '{trace_id_prefix}'", file=sys.stderr)
            sys.exit(1)
        return matches

    if op_pattern:
        pattern = op_pattern.lower()
        results = []
        for i, t in enumerate(traces):
            idx = build_span_index(t.get("spans", []))
            root_op = get_root_op_name(t, idx).lower()
            if pattern in root_op:
                results.append((i, t))
        if not results:
            print(f"Error: no trace with root operation matching '{op_pattern}'", file=sys.stderr)
            sys.exit(1)
        return results

    # No selector: return all
    return list(enumerate(traces))


# ---- Shared span hierarchy constants ----
# These are the single source of truth for expected span relationships,
# cross-thread spans, and known zero-duration spans. Both trace-inspect
# and trace-overview skills import from here.
#
# Maintenance: update these when span names are added, renamed, or removed.
# See CLAUDE.md "Tracing & OTEL Spans" section for the full update checklist.

# Expected parent-child relationships (from docs/operations/telemetry.md).
# Values are REQUIRED children — if parent exists, at least one of these should too.
EXPECTED_CHILDREN = {
    # transact_execute has txn_commit always; staging varies by format:
    #   txn_stage (FQL/SPARQL) or stage_turtle_insert (Turtle)
    "transact_execute": {"txn_commit"},  # txn_stage/stage_turtle_insert are format-dependent
    "query_execute": {"query_prepare", "query_run"},
    "sparql_execute": {"query_prepare", "query_run"},
    "query_prepare": {"reasoning_prep"},  # pattern_rewrite + plan are common but optional
    "index_build": {"commit_chain_walk", "dict_merge_and_remap", "build_all_indexes"},
    "build_all_indexes": {"build_index"},
    "bulk_import": {"import_chunks"},
}

# Spans that are expected to cross thread boundaries.
# Includes commit sub-spans (txn_commit runs in spawn_blocking)
# and txn_stage/txn_commit themselves (tokio task migration).
EXPECTED_CROSS_THREAD = {
    "build_index", "build_spot_from_commits", "BinaryIndexStore::load",
    "sort_blocking", "sort_child_next_batch", "sort_build_rows_batch",
    "groupby_blocking",
    # transact phases may cross threads due to tokio task migration
    "txn_stage", "txn_commit", "stage_turtle_insert",
    # commit pipeline runs in spawn_blocking — all children cross threads
    "commit_nameservice_lookup", "commit_verify_sequencing",
    "commit_namespace_delta", "commit_write_raw_txn", "commit_build_record",
    "commit_write_commit_blob", "commit_publish_nameservice",
    "commit_generate_metadata_flakes", "commit_clone_novelty",
    "commit_populate_dict_novelty", "commit_apply_to_novelty",
}

# Spans known to have zero/near-zero duration (sub-microsecond metadata ops)
KNOWN_ZERO_DURATION = {
    "commit_namespace_delta", "commit_build_record", "commit_verify_sequencing",
    "commit_publish_nameservice", "commit_generate_metadata_flakes",
    "commit_clone_novelty", "commit_populate_dict_novelty",
    "where_exec", "cancellation", "delete_gen",
    "sort_build_rows_batch",  # zero-duration in lightweight sort batches
}
