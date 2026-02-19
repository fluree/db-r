#!/usr/bin/env python3
"""Anomaly detection for a single Jaeger trace.

Usage:
    python3 trace_anomalies.py <file> [options]

Options:
    --trace N          Select trace by index (0-based)
    --trace-id PREFIX  Select trace by traceID prefix
    --all-traces       Check all traces in the file
    --max-lines N      Cap output lines (default: 200, 0=unlimited)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trace_common import (
    load_traces, build_span_index, get_tag, format_duration, format_size,
    select_trace,
    EXPECTED_CHILDREN, EXPECTED_CROSS_THREAD, KNOWN_ZERO_DURATION,
)


def check_trace(trace_idx, trace):
    """Run all anomaly checks on a single trace. Returns (findings, stats)."""
    spans = trace.get("spans", [])
    idx = build_span_index(spans)
    findings = []
    stats = {"errors": 0, "warnings": 0, "info": 0}

    def add(severity, message, details=None):
        findings.append({"severity": severity, "message": message, "details": details})
        stats[severity.lower()] = stats.get(severity.lower(), 0) + 1

    # 1. Orphaned spans
    if idx["orphans"]:
        add("ERROR", f"{len(idx['orphans'])} orphaned span(s) — parent spanID not in trace",
            [f"  {o['operationName']} (spanID={o['spanID'][:8]}..., "
             f"parent={idx['parent_map'].get(o['spanID'], '?')[:8]}...)"
             for o in idx["orphans"]])
    else:
        add("CLEAN", "No orphaned spans")

    # 2. Multiple root spans
    if len(idx["roots"]) > 1:
        add("WARNING", f"{len(idx['roots'])} root spans (expected 1) — possible async context loss",
            [f"  {r['operationName']} ({format_duration(r.get('duration', 0))})"
             for r in idx["roots"]])
    elif len(idx["roots"]) == 1:
        add("CLEAN", "Single root span")
    else:
        add("ERROR", "No root spans found")

    # 3. Child > parent duration
    violations = []
    for s in spans:
        sid = s["spanID"]
        if sid in idx["parent_map"]:
            pid = idx["parent_map"][sid]
            parent = idx["span_by_id"].get(pid)
            if parent:
                child_dur = s.get("duration", 0)
                parent_dur = parent.get("duration", 0)
                if child_dur > parent_dur > 0 and child_dur - parent_dur > 100:  # >100us tolerance
                    violations.append(
                        f"  {s['operationName']} ({format_duration(child_dur)}) > "
                        f"parent {parent['operationName']} ({format_duration(parent_dur)})")
    if violations:
        add("WARNING", f"{len(violations)} child > parent duration violation(s)", violations[:10])
    else:
        add("CLEAN", "No child > parent duration violations")

    # 4. Zero-duration spans
    zero_dur = [s for s in spans if s.get("duration", 0) == 0]
    known_zero = [s for s in zero_dur if s["operationName"] in KNOWN_ZERO_DURATION]
    unknown_zero = [s for s in zero_dur if s["operationName"] not in KNOWN_ZERO_DURATION]

    if unknown_zero:
        add("WARNING", f"{len(unknown_zero)} unexpected zero-duration span(s)",
            [f"  {s['operationName']}" for s in unknown_zero[:10]])
    if known_zero:
        add("INFO", f"{len(known_zero)} zero-duration span(s) (all known-benign: sub-microsecond metadata ops)",
            [f"  {s['operationName']}" for s in known_zero[:5]]
            + ([f"  ... and {len(known_zero)-5} more"] if len(known_zero) > 5 else []))
    if not zero_dur:
        add("CLEAN", "No zero-duration spans")

    # 5. Thread crossings
    unexpected_crossings = []
    expected_crossings = []
    for s in spans:
        sid = s["spanID"]
        if sid in idx["parent_map"]:
            pid = idx["parent_map"][sid]
            parent = idx["span_by_id"].get(pid)
            if parent:
                child_thread = get_tag(s, "thread.id")
                parent_thread = get_tag(parent, "thread.id")
                if child_thread and parent_thread and child_thread != parent_thread:
                    if s["operationName"] in EXPECTED_CROSS_THREAD:
                        expected_crossings.append(s)
                    else:
                        unexpected_crossings.append(
                            f"  {s['operationName']} (thread {parent_thread} -> {child_thread}, "
                            f"parent: {parent['operationName']})")

    if unexpected_crossings:
        add("WARNING", f"{len(unexpected_crossings)} unexpected thread crossing(s)", unexpected_crossings[:10])
    else:
        total_crossings = len(unexpected_crossings) + len(expected_crossings)
        add("CLEAN", f"No unexpected thread crossings "
            f"({total_crossings} total cross-thread, {len(expected_crossings)} in expected set)")

    # 6. Missing expected children
    for s in spans:
        op = s["operationName"]
        if op in EXPECTED_CHILDREN:
            expected = EXPECTED_CHILDREN[op]
            children = idx["children_map"].get(s["spanID"], [])
            child_ops = {c["operationName"] for c in children}
            missing = expected - child_ops
            if missing:
                add("WARNING", f"'{op}' missing expected children: {', '.join(sorted(missing))}",
                    [f"  Has: {', '.join(sorted(child_ops)) or '(none)'}",
                     f"  Expected: {', '.join(sorted(expected))}"])

    # 7. Deep nesting check
    max_depth = [0]
    def measure_depth(span, depth, index):
        if depth > max_depth[0]:
            max_depth[0] = depth
    for root in idx["roots"]:
        from trace_common import walk_tree
        walk_tree(root["spanID"], idx, measure_depth, 0)
    if max_depth[0] > 6:
        add("INFO", f"Maximum span depth: {max_depth[0]} (> 6, unusually deep)")

    return findings, stats


def print_findings(trace_idx, root_op, findings, stats):
    """Print anomaly report for one trace."""
    lines = 0
    print(f"=== Anomaly Report: Trace {trace_idx} [{root_op}] ===")
    lines += 1

    for f in findings:
        sev = f["severity"]
        if sev == "CLEAN":
            print(f"  CLEAN: {f['message']}")
        else:
            print(f"  {sev}: {f['message']}")
        lines += 1

        if f.get("details"):
            for d in f["details"]:
                print(f"    {d}")
                lines += 1

    summary_parts = []
    if stats["errors"]:
        summary_parts.append(f"{stats['errors']} error(s)")
    if stats["warnings"]:
        summary_parts.append(f"{stats['warnings']} warning(s)")
    if stats["info"]:
        summary_parts.append(f"{stats['info']} info")

    if not stats["errors"] and not stats["warnings"]:
        print(f"\n  Result: HEALTHY")
    else:
        print(f"\n  Result: {', '.join(summary_parts)}")
    lines += 2

    print()
    return lines + 1


def main():
    parser = argparse.ArgumentParser(description="Anomaly detection for Jaeger traces")
    parser.add_argument("file", help="Path to Jaeger JSON export file")
    parser.add_argument("--trace", type=int, default=None, help="Select trace by index")
    parser.add_argument("--trace-id", default=None, help="Select trace by traceID prefix")
    parser.add_argument("--all-traces", action="store_true", help="Check all traces")
    parser.add_argument("--max-lines", type=int, default=200, help="Cap output lines (0=unlimited)")
    args = parser.parse_args()

    traces, meta = load_traces(args.file)

    print(f"File: {os.path.basename(args.file)} ({format_size(meta['file_size'])}, "
          f"{meta['format']} format)")
    print(f"Traces: {meta['trace_count']} | Total spans: {meta['total_spans']}")
    print()
    lines_used = 3

    if args.all_traces:
        selected = list(enumerate(traces))
    elif args.trace is not None or args.trace_id:
        selected = select_trace(traces, args.trace, args.trace_id)
    elif len(traces) == 1:
        selected = [(0, traces[0])]
    else:
        print(f"Multiple traces found. Use --trace N, --trace-id PREFIX, or --all-traces.")
        print(f"Showing summary of first trace (index 0):\n")
        selected = [(0, traces[0])]

    total_stats = {"errors": 0, "warnings": 0, "info": 0}
    for i, trace in selected:
        if args.max_lines > 0 and lines_used >= args.max_lines:
            remaining = len(selected) - selected.index((i, trace))
            print(f"... {remaining} more trace(s) not checked (--max-lines {args.max_lines})")
            break

        idx = build_span_index(trace.get("spans", []))
        root_op = idx["roots"][0]["operationName"] if idx["roots"] else "<no root>"
        findings, stats = check_trace(i, trace)
        lines_used += print_findings(i, root_op, findings, stats)

        for k in total_stats:
            total_stats[k] += stats.get(k, 0)

    if len(selected) > 1:
        print("=== Summary ===")
        print(f"Traces checked: {min(len(selected), len(traces))}")
        if total_stats["errors"] == 0 and total_stats["warnings"] == 0:
            print("Result: ALL HEALTHY")
        else:
            parts = []
            if total_stats["errors"]:
                parts.append(f"{total_stats['errors']} error(s)")
            if total_stats["warnings"]:
                parts.append(f"{total_stats['warnings']} warning(s)")
            print(f"Result: {', '.join(parts)} across all traces")


if __name__ == "__main__":
    main()
