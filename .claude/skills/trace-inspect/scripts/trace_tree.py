#!/usr/bin/env python3
"""Render the span tree for a single Jaeger trace.

Usage:
    python3 trace_tree.py <file> [options]

Options:
    --trace N              Select trace by index (0-based)
    --trace-id PREFIX      Select trace by traceID prefix
    --op PATTERN           Select traces whose root op contains PATTERN
    --detail               Show tags, timing gaps, thread crossings
    --min-duration-ms N    Hide spans shorter than N ms (default: show all)
    --max-lines N          Cap output lines (default: 200, 0=unlimited)
"""

import argparse
import os
import sys

# Import from same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trace_common import (
    load_traces, build_span_index, get_tag, get_tags_dict,
    format_duration, format_size, select_trace,
)


def print_file_header(path, meta):
    print(f"File: {os.path.basename(path)} ({format_size(meta['file_size'])}, {meta['format']} format)")
    print(f"Traces: {meta['trace_count']} | Total spans: {meta['total_spans']} | "
          f"Service: {', '.join(meta['services'])}")
    print()


def print_trace_list(traces):
    """Print a table of all traces for selection."""
    print(f"{'#':>4s}  {'Operation':<30s}  {'Spans':>5s}  {'Duration':>10s}  {'Ledger'}")
    print("-" * 75)
    for i, t in enumerate(traces):
        spans = t.get("spans", [])
        idx = build_span_index(spans)
        if idx["roots"]:
            root = idx["roots"][0]
            op = root["operationName"]
            dur = format_duration(root.get("duration", 0))
            ledger = get_tag(root, "ledger_id", "")
        else:
            op = "<no root>"
            dur = "???"
            ledger = ""
        print(f"{i:4d}  {op:<30s}  {len(spans):5d}  {dur:>10s}  {ledger}")
    print()


def render_summary_tree(trace_idx, trace, max_lines=200, lines_used=0):
    """Render indented span tree with operation name, duration, % of root."""
    spans = trace.get("spans", [])
    idx = build_span_index(spans)

    if not idx["roots"]:
        print(f"  (no root spans found)")
        return lines_used + 1

    root = idx["roots"][0]
    root_dur = max(root.get("duration", 1), 1)  # avoid div by zero
    print(f"=== Trace {trace_idx} [{root['operationName']}] "
          f"{format_duration(root.get('duration', 0))}, {len(spans)} spans ===")
    lines_used += 1

    if len(idx["roots"]) > 1:
        print(f"  WARNING: {len(idx['roots'])} root spans (expected 1) -- possible async context loss")
        lines_used += 1

    if idx["orphans"]:
        print(f"  WARNING: {len(idx['orphans'])} orphaned spans (parent not in trace)")
        lines_used += 1

    def print_span(span, depth, index):
        nonlocal lines_used
        if max_lines > 0 and lines_used >= max_lines:
            return

        indent = "  " * depth
        op = span["operationName"]
        dur = span.get("duration", 0)
        pct = (dur / root_dur * 100) if root_dur > 0 else 0

        print(f"{indent}{op:<{40-2*depth}s}  {format_duration(dur):>10s}  {pct:5.1f}%")
        lines_used += 1

    for root_span in idx["roots"]:
        walk_tree_limited(root_span["spanID"], idx, print_span, 0, max_lines, lines_used)

    if idx["orphans"] and lines_used < max_lines:
        print(f"\n  Orphaned spans:")
        lines_used += 2
        for orph in idx["orphans"]:
            if max_lines > 0 and lines_used >= max_lines:
                break
            print(f"    {orph['operationName']}  {format_duration(orph.get('duration', 0))}")
            lines_used += 1

    print()
    return lines_used


def render_detail_tree(trace_idx, trace, min_duration_ms=0, max_lines=200, lines_used=0):
    """Render span tree with tags, timing gaps, thread crossings."""
    spans = trace.get("spans", [])
    idx = build_span_index(spans)

    if not idx["roots"]:
        print(f"  (no root spans found)")
        return lines_used + 1

    root = idx["roots"][0]
    root_dur = max(root.get("duration", 1), 1)
    min_dur_us = min_duration_ms * 1000

    print(f"=== Trace {trace_idx} [{root['operationName']}] "
          f"{format_duration(root.get('duration', 0))}, {len(spans)} spans (detail) ===")
    lines_used += 1

    if len(idx["roots"]) > 1:
        print(f"  WARNING: {len(idx['roots'])} root spans -- possible async context loss")
        lines_used += 1

    INTERESTING_TAGS = [
        "ledger_id", "operation", "error_code", "tracker_time", "tracker_fuel",
        "insert_count", "delete_count", "flake_count", "delta_bytes",
        "retraction_count", "assertion_count", "pattern_count",
        "patterns_before", "patterns_after", "result_count",
        "commit_t", "commit_ops", "order", "rows",
    ]

    def format_tags(span):
        tags = get_tags_dict(span)
        parts = []
        thread_id = tags.get("thread.id")
        if thread_id is not None:
            parts.append(f"thread={thread_id}")
        for key in INTERESTING_TAGS:
            val = tags.get(key)
            if val is not None and val != "":
                parts.append(f"{key}={val}")
        return "  ".join(parts)

    def print_detail_span(span, depth, index):
        nonlocal lines_used
        if max_lines > 0 and lines_used >= max_lines:
            return

        dur = span.get("duration", 0)
        if dur < min_dur_us and depth > 0:
            return

        indent = "  " * depth
        op = span["operationName"]
        pct = (dur / root_dur * 100) if root_dur > 0 else 0
        tag_str = format_tags(span)

        print(f"{indent}{op:<{40-2*depth}s}  {format_duration(dur):>10s}  {pct:5.1f}%  {tag_str}")
        lines_used += 1

        # Detect timing gaps between children.
        # NOTE: gap can be negative when children execute concurrently
        # (e.g. thread::scope, parallel builds). Negative gap means
        # children's wall-clock sum exceeds parent's — expected for parallelism.
        children = index["children_map"].get(span["spanID"], [])
        if children and dur > 0:
            parent_start = span.get("startTime", 0)
            parent_end = parent_start + dur
            child_sum = sum(c.get("duration", 0) for c in children)
            gap = dur - child_sum

            # Check for thread crossings
            parent_thread = get_tag(span, "thread.id")
            for child in children:
                child_thread = get_tag(child, "thread.id")
                if parent_thread and child_thread and parent_thread != child_thread:
                    if max_lines > 0 and lines_used >= max_lines:
                        return
                    child_indent = "  " * (depth + 1)
                    print(f"{child_indent}[thread crossing: {parent_thread} -> {child_thread} "
                          f"for {child['operationName']}]")
                    lines_used += 1

            # Show significant gaps (> 5% of parent or > 1ms)
            if gap > dur * 0.05 and gap > 1000:
                if max_lines > 0 and lines_used >= max_lines:
                    return
                child_indent = "  " * (depth + 1)
                print(f"{child_indent}[gap: {format_duration(gap)} unaccounted "
                      f"({gap/dur*100:.0f}% of parent)]")
                lines_used += 1

    for root_span in idx["roots"]:
        walk_tree_limited(root_span["spanID"], idx, print_detail_span, 0, max_lines, lines_used)

    print()
    return lines_used


def walk_tree_limited(span_id, index, visitor_fn, depth, max_lines, lines_used):
    """DFS walk with line limit awareness.

    Note: lines_used is tracked via nonlocal in the visitor_fn closure
    (see render_summary_tree / render_detail_tree). The max_lines check
    here provides an additional guard against runaway recursion.
    """
    if max_lines > 0 and lines_used >= max_lines:
        return
    span = index["span_by_id"].get(span_id)
    if not span:
        return
    visitor_fn(span, depth, index)
    for child in index["children_map"].get(span_id, []):
        if max_lines > 0 and lines_used >= max_lines:
            break
        walk_tree_limited(child["spanID"], index, visitor_fn, depth + 1, max_lines, lines_used)


def main():
    parser = argparse.ArgumentParser(description="Render span tree for a Jaeger trace export")
    parser.add_argument("file", help="Path to Jaeger JSON export file")
    parser.add_argument("--trace", type=int, default=None, help="Select trace by index (0-based)")
    parser.add_argument("--trace-id", default=None, help="Select trace by traceID prefix")
    parser.add_argument("--op", default=None, help="Select traces whose root op contains this pattern")
    parser.add_argument("--detail", action="store_true", help="Show tags, timing gaps, thread crossings")
    parser.add_argument("--min-duration-ms", type=float, default=0, help="Hide spans shorter than N ms")
    parser.add_argument("--max-lines", type=int, default=200, help="Cap output lines (0=unlimited)")
    args = parser.parse_args()

    traces, meta = load_traces(args.file)
    print_file_header(args.file, meta)

    # If no selector and multiple traces, show list
    has_selector = args.trace is not None or args.trace_id or args.op
    if not has_selector and len(traces) > 1:
        print_trace_list(traces)
        if len(traces) <= 5:
            # Small file: show all trees
            selected = list(enumerate(traces))
        else:
            print("Specify --trace N, --trace-id PREFIX, or --op PATTERN to inspect a specific trace.")
            return
    else:
        selected = select_trace(traces, args.trace, args.trace_id, args.op)

    lines_used = 4  # header lines
    for i, trace in selected:
        if args.max_lines > 0 and lines_used >= args.max_lines:
            remaining = len(selected) - selected.index((i, trace))
            print(f"... {remaining} more trace(s) not shown (--max-lines {args.max_lines})")
            break

        if args.detail:
            lines_used = render_detail_tree(i, trace, args.min_duration_ms, args.max_lines, lines_used)
        else:
            lines_used = render_summary_tree(i, trace, args.max_lines, lines_used)


if __name__ == "__main__":
    main()
