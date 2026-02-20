#!/usr/bin/env python3
"""Aggregate analysis across all traces in a Jaeger JSON export.

Usage:
    python3 trace_summary.py <file> [options]

Options:
    --per-trace          Show per-trace detail table (auto for <50 traces)
    --no-per-trace       Suppress per-trace table even for small files
    --top N              Limit per-trace table to N rows
    --op-filter PATTERN  Only show traces whose root op contains PATTERN
    --max-lines N        Cap output lines (default: 200, 0=unlimited)
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


def percentile(sorted_vals, p):
    """Compute p-th percentile from a sorted list."""
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def analyze_traces(traces, op_filter=None):
    """Analyze all traces, return structured data.

    Returns list of trace_info dicts and op_stats dict.
    """
    trace_infos = []
    for i, t in enumerate(traces):
        spans = t.get("spans", [])
        idx = build_span_index(spans)
        root = idx["roots"][0] if idx["roots"] else None

        info = {
            "index": i,
            "trace_id": t["traceID"],
            "span_count": len(spans),
            "root_op": root["operationName"] if root else "<no root>",
            "duration": root.get("duration", 0) if root else 0,
            "root_span": root,
            "root_count": len(idx["roots"]),
            "orphan_count": len(idx["orphans"]),
            "ledger_id": get_tag(root, "ledger_id", "") if root else "",
            "error_code": get_tag(root, "error_code", "") if root else "",
            "operation": get_tag(root, "operation", "") if root else "",
        }
        trace_infos.append(info)

    if op_filter:
        pattern = op_filter.lower()
        trace_infos = [t for t in trace_infos if pattern in t["root_op"].lower()]

    # Group by operation type
    op_groups = defaultdict(list)
    for info in trace_infos:
        op_groups[info["root_op"]].append(info)

    op_stats = {}
    for op, infos in sorted(op_groups.items(), key=lambda x: -len(x[1])):
        durations = sorted([inf["duration"] for inf in infos])
        span_counts = [inf["span_count"] for inf in infos]
        op_stats[op] = {
            "count": len(infos),
            "avg_spans": sum(span_counts) / len(span_counts),
            "min_dur": durations[0],
            "p50_dur": percentile(durations, 50),
            "p95_dur": percentile(durations, 95),
            "max_dur": durations[-1],
            "span_count_range": (min(span_counts), max(span_counts)),
        }

    return trace_infos, op_stats


def count_span_names(traces, op_filter_infos=None):
    """Count span name frequencies across traces.

    Returns list of (name, total_count, trace_count) sorted by total_count desc.
    """
    if op_filter_infos is not None:
        valid_indices = {inf["index"] for inf in op_filter_infos}
    else:
        valid_indices = None

    name_counts = defaultdict(int)
    name_traces = defaultdict(set)
    for i, t in enumerate(traces):
        if valid_indices is not None and i not in valid_indices:
            continue
        for s in t.get("spans", []):
            name = s["operationName"]
            name_counts[name] += 1
            name_traces[name].add(i)

    results = [(name, name_counts[name], len(name_traces[name]))
               for name in name_counts]
    results.sort(key=lambda x: -x[1])
    return results


def count_tag_coverage(traces, op_filter_infos=None):
    """Count tag key presence across all spans.

    Returns list of (key, span_count, total_spans) sorted by span_count desc.
    """
    if op_filter_infos is not None:
        valid_indices = {inf["index"] for inf in op_filter_infos}
    else:
        valid_indices = None

    tag_counts = defaultdict(int)
    total_spans = 0
    for i, t in enumerate(traces):
        if valid_indices is not None and i not in valid_indices:
            continue
        for s in t.get("spans", []):
            total_spans += 1
            seen = set()
            for tag in s.get("tags", []):
                k = tag["key"]
                if k not in seen:
                    tag_counts[k] += 1
                    seen.add(k)

    results = [(k, tag_counts[k], total_spans) for k in tag_counts]
    results.sort(key=lambda x: -x[1])
    return results


def print_op_breakdown(op_stats, lines_budget):
    """Print operation type breakdown table. Returns lines used."""
    lines = 0
    header = (f"{'Operation':<30s}  {'Count':>5s}  {'Spans(avg)':>10s}  "
              f"{'Min':>8s}  {'P50':>8s}  {'P95':>8s}  {'Max':>8s}")
    print("=== Operation Type Breakdown ===")
    print(header)
    print("-" * len(header))
    lines += 3

    for op, stats in op_stats.items():
        if lines_budget > 0 and lines >= lines_budget:
            print(f"... ({len(op_stats) - lines + 3} more operation types)")
            lines += 1
            break
        span_range = stats["span_count_range"]
        span_str = (f"{stats['avg_spans']:.0f}" if span_range[0] == span_range[1]
                    else f"{stats['avg_spans']:.0f} ({span_range[0]}-{span_range[1]})")
        print(f"{op:<30s}  {stats['count']:5d}  {span_str:>10s}  "
              f"{format_duration(stats['min_dur']):>8s}  "
              f"{format_duration(stats['p50_dur']):>8s}  "
              f"{format_duration(stats['p95_dur']):>8s}  "
              f"{format_duration(stats['max_dur']):>8s}")
        lines += 1

    print()
    return lines + 1


def print_per_trace(trace_infos, top_n=None, lines_budget=0):
    """Print per-trace detail table. Returns lines used."""
    lines = 0
    display = trace_infos[:top_n] if top_n else trace_infos

    header = f"{'#':>4s}  {'Operation':<30s}  {'Spans':>5s}  {'Duration':>10s}  {'Error':<12s}  {'Ledger'}"
    print("=== Per-Trace Detail ===")
    print(header)
    print("-" * len(header))
    lines += 3

    for info in display:
        if lines_budget > 0 and lines >= lines_budget:
            remaining = len(display) - (lines - 3)
            print(f"... {remaining} more trace(s)")
            lines += 1
            break

        flags = ""
        if info["root_count"] > 1:
            flags += f" MULTI-ROOT({info['root_count']})"
        if info["orphan_count"] > 0:
            flags += f" ORPHANS({info['orphan_count']})"

        print(f"{info['index']:4d}  {info['root_op']:<30s}  {info['span_count']:5d}  "
              f"{format_duration(info['duration']):>10s}  "
              f"{info['error_code']:<12s}  {info['ledger_id']}{flags}")
        lines += 1

    if top_n and len(trace_infos) > top_n:
        print(f"... showing {top_n} of {len(trace_infos)} traces")
        lines += 1

    print()
    return lines + 1


def print_span_names(name_data, top_n=20):
    """Print span name frequency table. Returns lines used."""
    lines = 0
    print(f"=== Span Name Frequency (top {min(top_n, len(name_data))}) ===")
    lines += 1
    for name, count, trace_count in name_data[:top_n]:
        print(f"  {name:<45s}  {count:5d}  (in {trace_count} trace{'s' if trace_count != 1 else ''})")
        lines += 1
    print()
    return lines + 1


def print_tag_coverage(tag_data, total_spans):
    """Print tag coverage summary. Returns lines used."""
    lines = 0
    print("=== Tag Coverage ===")
    lines += 1

    universal = [k for k, c, t in tag_data if c == total_spans]
    partial = [(k, c, t) for k, c, t in tag_data if c < total_spans]

    if universal:
        print(f"  Universal ({len(universal)}): {', '.join(universal)}")
        lines += 1

    # Show non-universal tags (skip otel-standard ones that are always present)
    SKIP_TAGS = {"busy_ns", "idle_ns", "code.file.path", "code.line.number",
                 "code.module.name", "otel.scope.name", "span.kind", "target",
                 "thread.id", "thread.name"}
    interesting = [(k, c, t) for k, c, t in partial if k not in SKIP_TAGS]
    if interesting:
        print(f"  Partial coverage:")
        lines += 1
        for k, c, t in interesting[:15]:
            print(f"    {k:<35s}  {c:5d}/{t} spans ({c/t*100:.0f}%)")
            lines += 1

    print()
    return lines + 1


def main():
    parser = argparse.ArgumentParser(description="Aggregate analysis of Jaeger trace export")
    parser.add_argument("file", help="Path to Jaeger JSON export file")
    parser.add_argument("--per-trace", action="store_true", default=None,
                        help="Show per-trace table (auto for <50 traces)")
    parser.add_argument("--no-per-trace", action="store_true",
                        help="Suppress per-trace table")
    parser.add_argument("--top", type=int, default=None, help="Limit per-trace rows")
    parser.add_argument("--op-filter", default=None,
                        help="Only show traces whose root op contains PATTERN")
    parser.add_argument("--max-lines", type=int, default=200,
                        help="Cap output lines (0=unlimited)")
    args = parser.parse_args()

    traces, meta = load_traces(args.file)

    # File header
    print(f"File: {os.path.basename(args.file)} ({format_size(meta['file_size'])}, "
          f"{meta['format']} format)")
    print(f"Traces: {meta['trace_count']} | Total spans: {meta['total_spans']} | "
          f"Service: {', '.join(meta['services'])}")
    print()
    lines_used = 3

    # Analyze
    trace_infos, op_stats = analyze_traces(traces, args.op_filter)

    if not trace_infos:
        print("No traces found" + (f" matching '{args.op_filter}'" if args.op_filter else ""))
        return

    # Operation breakdown (always)
    budget = max(0, args.max_lines - lines_used) if args.max_lines > 0 else 0
    lines_used += print_op_breakdown(op_stats, budget)

    # Per-trace table (auto for <50 traces unless suppressed)
    show_per_trace = args.per_trace
    if show_per_trace is None and not args.no_per_trace:
        show_per_trace = len(trace_infos) < 50

    if show_per_trace:
        budget = max(0, args.max_lines - lines_used) if args.max_lines > 0 else 0
        lines_used += print_per_trace(trace_infos, args.top, budget)

    # Span name frequency
    if args.max_lines == 0 or lines_used < args.max_lines - 25:
        name_data = count_span_names(traces, trace_infos if args.op_filter else None)
        lines_used += print_span_names(name_data, top_n=15)

    # Tag coverage
    if args.max_lines == 0 or lines_used < args.max_lines - 20:
        tag_data = count_tag_coverage(traces, trace_infos if args.op_filter else None)
        print_tag_coverage(tag_data, meta['total_spans'])


if __name__ == "__main__":
    main()
