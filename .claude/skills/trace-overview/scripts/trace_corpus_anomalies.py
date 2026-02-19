#!/usr/bin/env python3
"""Cross-trace anomaly detection for Jaeger trace exports.

Analyzes patterns across all traces in a file: duration outliers,
structural outliers, orphaned spans, multiple roots, error traces.

Usage:
    python3 trace_corpus_anomalies.py <file> [options]

Options:
    --verbose          Show per-trace detail for each anomaly
    --op-filter PAT    Only analyze traces matching this operation pattern
    --max-lines N      Cap output lines (default: 200, 0=unlimited)
"""

import argparse
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trace_common import (
    load_traces, build_span_index, get_tag, format_duration, format_size,
)

# Expected parent-child relationships
EXPECTED_CHILDREN = {
    # transact_execute has txn_commit always; staging varies by format:
    #   txn_stage (FQL/SPARQL) or stage_turtle_insert (Turtle)
    "transact_execute": {"txn_commit"},  # txn_stage/stage_turtle_insert are format-dependent
    "query_execute": {"query_prepare", "query_run"},
    "sparql_execute": {"query_prepare", "query_run"},
    "query_prepare": {"reasoning_prep"},
    "index_build": {"commit_chain_walk", "dict_merge_and_remap", "build_all_indexes"},
    "build_all_indexes": {"build_index"},
    "bulk_import": {"import_chunks"},
}

# Includes commit sub-spans (txn_commit runs in spawn_blocking)
# and txn_stage/txn_commit themselves (tokio task migration).
EXPECTED_CROSS_THREAD = {
    "build_index", "build_spot_from_commits", "BinaryIndexStore::load",
    "sort_blocking", "sort_child_next_batch", "sort_build_rows_batch",
    "groupby_blocking",
    # transact phases may cross threads due to tokio task migration
    "txn_stage", "txn_commit", "stage_turtle_insert",
    "commit_nameservice_lookup", "commit_verify_sequencing",
    "commit_namespace_delta", "commit_write_raw_txn", "commit_build_record",
    "commit_write_commit_blob", "commit_publish_nameservice",
    "commit_generate_metadata_flakes", "commit_clone_novelty",
    "commit_populate_dict_novelty", "commit_apply_to_novelty",
}

KNOWN_ZERO_DURATION = {
    "commit_namespace_delta", "commit_build_record", "commit_verify_sequencing",
    "commit_publish_nameservice", "commit_generate_metadata_flakes",
    "commit_clone_novelty", "commit_populate_dict_novelty",
    "where_exec", "cancellation", "delete_gen",
    "sort_build_rows_batch",
}


def analyze_corpus(traces, op_filter=None):
    """Analyze all traces for cross-trace anomaly patterns.

    Returns dict of anomaly categories, each with a list of findings.
    """
    # Build per-trace metadata
    trace_data = []
    for i, t in enumerate(traces):
        spans = t.get("spans", [])
        idx = build_span_index(spans)
        root = idx["roots"][0] if idx["roots"] else None
        root_op = root["operationName"] if root else "<no root>"

        if op_filter and op_filter.lower() not in root_op.lower():
            continue

        trace_data.append({
            "index": i,
            "trace_id": t["traceID"],
            "root_op": root_op,
            "duration": root.get("duration", 0) if root else 0,
            "span_count": len(spans),
            "root_count": len(idx["roots"]),
            "orphan_count": len(idx["orphans"]),
            "orphan_ops": [o["operationName"] for o in idx["orphans"]],
            "root_ops": [r["operationName"] for r in idx["roots"]],
            "error_code": get_tag(root, "error_code", "") if root else "",
            "idx": idx,
            "spans": spans,
        })

    anomalies = {
        "orphaned_spans": [],
        "multiple_roots": [],
        "duration_outliers": [],
        "structural_outliers": [],
        "error_traces": [],
        "missing_children": [],
        "unexpected_thread_crossings": [],
        "zero_duration_unexpected": [],
    }

    # 1. Orphaned spans
    for td in trace_data:
        if td["orphan_count"] > 0:
            anomalies["orphaned_spans"].append(
                f"Trace {td['index']} [{td['root_op']}]: {td['orphan_count']} orphaned — "
                f"{', '.join(td['orphan_ops'][:5])}")

    # 2. Multiple roots
    for td in trace_data:
        if td["root_count"] > 1:
            anomalies["multiple_roots"].append(
                f"Trace {td['index']} [{td['root_op']}]: {td['root_count']} roots — "
                f"{', '.join(td['root_ops'][:5])}")

    # 3. Duration outliers (>2 stddev from op-type mean)
    op_groups = defaultdict(list)
    for td in trace_data:
        op_groups[td["root_op"]].append(td)

    for op, tds in op_groups.items():
        if len(tds) < 3:  # need at least 3 for meaningful stats
            continue
        durations = [td["duration"] for td in tds]
        mean = sum(durations) / len(durations)
        variance = sum((d - mean) ** 2 for d in durations) / len(durations)
        stddev = math.sqrt(variance) if variance > 0 else 0

        if stddev == 0:
            continue

        for td in tds:
            zscore = (td["duration"] - mean) / stddev if stddev > 0 else 0
            if abs(zscore) > 2:
                anomalies["duration_outliers"].append(
                    f"Trace {td['index']} [{op}]: {format_duration(td['duration'])} "
                    f"(mean={format_duration(mean)}, stddev={format_duration(stddev)}, z={zscore:.1f})")

    # 4. Structural outliers (span count variance within op type)
    for op, tds in op_groups.items():
        if len(tds) < 2:
            continue
        counts = [td["span_count"] for td in tds]
        mean_count = sum(counts) / len(counts)
        for td in tds:
            ratio = td["span_count"] / mean_count if mean_count > 0 else 0
            if ratio > 3 or ratio < 0.33:  # 3x or 1/3 of average
                anomalies["structural_outliers"].append(
                    f"Trace {td['index']} [{op}]: {td['span_count']} spans "
                    f"(avg for {op}: {mean_count:.0f}, ratio: {ratio:.1f}x)")

    # 5. Error traces
    for td in trace_data:
        if td["error_code"]:
            anomalies["error_traces"].append(
                f"Trace {td['index']} [{td['root_op']}]: error_code={td['error_code']}")

    # 6. Missing expected children (check consistency within op type)
    missing_patterns = defaultdict(list)
    for td in trace_data:
        for s in td["spans"]:
            op = s["operationName"]
            if op in EXPECTED_CHILDREN:
                expected = EXPECTED_CHILDREN[op]
                children = td["idx"]["children_map"].get(s["spanID"], [])
                child_ops = {c["operationName"] for c in children}
                missing = expected - child_ops
                if missing:
                    missing_patterns[(op, frozenset(missing))].append(td["index"])

    for (parent_op, missing_set), trace_indices in missing_patterns.items():
        anomalies["missing_children"].append(
            f"'{parent_op}' missing {{{', '.join(sorted(missing_set))}}} "
            f"in {len(trace_indices)} trace(s): [{', '.join(str(i) for i in trace_indices[:10])}]")

    # 7. Unexpected thread crossings
    unexpected_per_trace = defaultdict(list)
    for td in trace_data:
        for s in td["spans"]:
            sid = s["spanID"]
            if sid in td["idx"]["parent_map"]:
                pid = td["idx"]["parent_map"][sid]
                parent = td["idx"]["span_by_id"].get(pid)
                if parent:
                    ct = get_tag(s, "thread.id")
                    pt = get_tag(parent, "thread.id")
                    if ct and pt and ct != pt and s["operationName"] not in EXPECTED_CROSS_THREAD:
                        unexpected_per_trace[td["index"]].append(
                            f"{s['operationName']} ({pt} -> {ct})")

    for tidx, crossings in unexpected_per_trace.items():
        td = next(t for t in trace_data if t["index"] == tidx)
        anomalies["unexpected_thread_crossings"].append(
            f"Trace {tidx} [{td['root_op']}]: {len(crossings)} crossing(s) — "
            f"{', '.join(crossings[:5])}")

    # 8. Unexpected zero-duration spans
    zero_unknown = defaultdict(int)
    zero_known = defaultdict(int)
    for td in trace_data:
        for s in td["spans"]:
            if s.get("duration", 0) == 0:
                op = s["operationName"]
                if op in KNOWN_ZERO_DURATION:
                    zero_known[op] += 1
                else:
                    zero_unknown[op] += 1

    for op, count in sorted(zero_unknown.items(), key=lambda x: -x[1]):
        anomalies["zero_duration_unexpected"].append(
            f"'{op}': {count} zero-duration occurrence(s) (NOT in known-benign set)")

    return anomalies, trace_data, op_groups


def print_report(anomalies, trace_data, op_groups, verbose=False, max_lines=200):
    """Print the cross-trace anomaly report."""
    lines = 0

    print(f"=== Cross-Trace Anomaly Scan ===")
    print(f"Traces analyzed: {len(trace_data)} | Operation types: {len(op_groups)}")
    print()
    lines += 3

    CATEGORIES = [
        ("orphaned_spans", "Orphaned Spans", "ERROR"),
        ("multiple_roots", "Multiple Root Spans", "WARNING"),
        ("error_traces", "Error Traces", "WARNING"),
        ("unexpected_thread_crossings", "Unexpected Thread Crossings", "WARNING"),
        ("missing_children", "Missing Expected Children", "WARNING"),
        ("duration_outliers", "Duration Outliers (>2 stddev)", "INFO"),
        ("structural_outliers", "Structural Outliers (span count)", "INFO"),
        ("zero_duration_unexpected", "Unexpected Zero-Duration Spans", "INFO"),
    ]

    any_issues = False
    for key, title, severity in CATEGORIES:
        findings = anomalies[key]
        if max_lines > 0 and lines >= max_lines:
            print(f"... output truncated at {max_lines} lines")
            return

        if not findings:
            print(f"CLEAN: {title}")
            lines += 1
        else:
            any_issues = True
            print(f"\n{severity}: {title} ({len(findings)})")
            lines += 2
            limit = len(findings) if verbose else min(10, len(findings))
            for f in findings[:limit]:
                print(f"  {f}")
                lines += 1
            if not verbose and len(findings) > 10:
                print(f"  ... {len(findings) - 10} more (use --verbose for all)")
                lines += 1

    # Zero-duration known-benign summary
    print()
    lines += 1

    # Overall assessment
    print()
    error_count = len(anomalies["orphaned_spans"])
    warning_count = (len(anomalies["multiple_roots"]) + len(anomalies["error_traces"]) +
                     len(anomalies["unexpected_thread_crossings"]) + len(anomalies["missing_children"]))
    info_count = (len(anomalies["duration_outliers"]) + len(anomalies["structural_outliers"]) +
                  len(anomalies["zero_duration_unexpected"]))

    if not any_issues:
        print("Overall: ALL CLEAN — no anomalies detected")
    else:
        parts = []
        if error_count:
            parts.append(f"{error_count} error(s)")
        if warning_count:
            parts.append(f"{warning_count} warning(s)")
        if info_count:
            parts.append(f"{info_count} informational")
        print(f"Overall: {', '.join(parts)}")
    lines += 2


def main():
    parser = argparse.ArgumentParser(description="Cross-trace anomaly detection")
    parser.add_argument("file", help="Path to Jaeger JSON export file")
    parser.add_argument("--verbose", action="store_true", help="Show all findings per category")
    parser.add_argument("--op-filter", default=None, help="Only analyze matching operations")
    parser.add_argument("--max-lines", type=int, default=200, help="Cap output lines (0=unlimited)")
    args = parser.parse_args()

    traces, meta = load_traces(args.file)

    print(f"File: {os.path.basename(args.file)} ({format_size(meta['file_size'])}, "
          f"{meta['format']} format)")
    print(f"Traces: {meta['trace_count']} | Total spans: {meta['total_spans']}")
    print()

    anomalies, trace_data, op_groups = analyze_corpus(traces, args.op_filter)
    print_report(anomalies, trace_data, op_groups, args.verbose, args.max_lines)


if __name__ == "__main__":
    main()
