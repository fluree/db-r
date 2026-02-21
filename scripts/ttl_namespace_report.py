#!/usr/bin/env python3
"""
Report namespaces *used* in a Turtle file (not just declared prefixes).

Why this exists:
  If your import reports a surprisingly high count of "NS codes", that often
  means the loader is deriving namespaces from *actual IRIs used* in triples,
  not from @prefix declarations. This script approximates that count by scanning
  the Turtle source and collecting distinct namespace IRIs.

What it counts:
  - IRI references: <http://example.com/foo/bar>  -> namespace heuristic split
  - Prefixed names: ex:Thing / ex: / :local       -> resolved via @prefix map

It uses a lightweight state machine to ignore content inside strings, IRIs,
and comments, similar to the splitter in fluree-graph-turtle.

Examples:
  python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl
  python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --top 100
  python3 scripts/ttl_namespace_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --json-out /tmp/dblp_ns.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from ttl_prefix_report import (
    RE_SPARQL_BASE,
    RE_SPARQL_PREFIX,
    RE_TTL_BASE,
    RE_TTL_PREFIX,
)


class ScanState(Enum):
    NORMAL = auto()
    IN_IRI = auto()
    IN_COMMENT = auto()
    IN_SHORT_DQ = auto()  # "..."
    IN_LONG_DQ = auto()  # """..."""
    IN_SHORT_SQ = auto()  # '...'
    IN_LONG_SQ = auto()  # '''...'''
    IN_STRING_ESCAPE = auto()


@dataclass
class Scanner:
    state: ScanState = ScanState.NORMAL
    return_to: Optional[ScanState] = None  # for escapes
    # For long strings we need to detect triple quote endings across chars.
    dq_run: int = 0
    sq_run: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Report namespaces used in a TTL file")
    p.add_argument("input", help="Path to the input Turtle file (.ttl)")
    p.add_argument("--top", type=int, default=50, help="Show top N namespaces by usage (default: 50)")
    p.add_argument("--progress-mb", type=int, default=512, help="Print progress every N MB (default: 512)")
    p.add_argument(
        "--max-bytes",
        type=int,
        default=0,
        help="Stop after reading this many bytes (0 = no limit). Useful for quick comparisons.",
    )
    p.add_argument("--json-out", help="Write a JSON report to this path")
    p.add_argument(
        "--namespace-split",
        choices=["hash-slash-colon", "hash-slash"],
        default="hash-slash",
        help="How to split full IRIs into namespaces (default: hash-slash; matches Fluree sid_for_iri heuristic)",
    )
    return p.parse_args()


def split_namespace(iri: str, mode: str) -> str:
    """
    Heuristic namespace split.

    - Prefer '#', then '/', then (optionally) ':'.
    - Namespace is returned *inclusive* of the delimiter.
    """
    if not iri:
        return iri
    if "#" in iri:
        i = iri.rfind("#")
        return iri[: i + 1]
    if "/" in iri:
        i = iri.rfind("/")
        return iri[: i + 1]
    if mode == "hash-slash-colon" and ":" in iri:
        i = iri.rfind(":")
        return iri[: i + 1]
    # Match Fluree's sid_for_iri fallback: if there's no '/' or '#', use the EMPTY prefix.
    # (The entire IRI becomes the "local" name under ns_code=EMPTY.)
    return ""


def iri_family(ns_iri: str, path_segments: int) -> str:
    """
    Group namespace IRIs into broader "families" for explanation.

    Example:
      ns_iri = "https://dblp.org/pid/163/"
      path_segments=0 -> "https://dblp.org/"
      path_segments=1 -> "https://dblp.org/pid/"
      path_segments=2 -> "https://dblp.org/pid/163/"
    """
    try:
        u = urlparse(ns_iri)
    except Exception:
        return ns_iri

    if not u.scheme or not u.netloc:
        return ns_iri

    base = f"{u.scheme}://{u.netloc}/"
    if path_segments <= 0:
        return base

    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return base

    segs = parts[:path_segments]
    return base + "/".join(segs) + "/"


def _is_pn_prefix_start(ch: str) -> bool:
    return ch.isalpha() or ch == "_"


def _is_pn_prefix_char(ch: str) -> bool:
    return ch.isalnum() or ch in ["_", ".", "-"]


def _is_local_char(ch: str) -> bool:
    # Approximation; good enough for namespace counting.
    return ch.isalnum() or ch in ["_", ".", "-", "~", "%"]


def parse_directive_line(line: str) -> Tuple[Optional[Tuple[str, str]], Optional[str]]:
    """
    Parse @prefix/PREFIX and @base/BASE lines.
    Returns (prefix_decl, base_decl) where:
      - prefix_decl = (prefix, namespace)
      - base_decl = base_iri
    """
    m = RE_TTL_PREFIX.match(line)
    if m:
        return (m.group("pfx"), m.group("ns")), None
    m = RE_SPARQL_PREFIX.match(line)
    if m:
        return (m.group("pfx"), m.group("ns")), None
    m = RE_TTL_BASE.match(line)
    if m:
        return None, m.group("base")
    m = RE_SPARQL_BASE.match(line)
    if m:
        return None, m.group("base")
    return None, None


def scan_line(
    line: str,
    scanner: Scanner,
    prefix_map: Dict[str, str],
    namespace_counts: Counter,
    unresolved_prefix_counts: Counter,
    qname_counts: Counter,
    iriref_counts: Counter,
    namespace_split_mode: str,
) -> None:
    """
    Scan a line of Turtle source, updating counters.
    """
    # First, cheaply capture directive lines even if they appear later.
    # These directives must be in NORMAL/comment-free context; we keep it simple:
    # If the line begins with a directive token after whitespace, parse it.
    stripped = line.lstrip()
    if stripped.startswith(("@prefix", "@base", "PREFIX", "BASE")):
        pfx, base = parse_directive_line(stripped)
        if pfx:
            prefix, ns = pfx
            prefix_map[prefix] = ns
        # base currently not used for namespace counting (rare in large dumps)

    i = 0
    n = len(line)

    while i < n:
        ch = line[i]

        # Handle comment state
        if scanner.state == ScanState.IN_COMMENT:
            if ch == "\n":
                scanner.state = ScanState.NORMAL
            i += 1
            continue

        # NORMAL transitions into comment
        if scanner.state == ScanState.NORMAL and ch == "#":
            scanner.state = ScanState.IN_COMMENT
            i += 1
            continue

        # String escape state
        if scanner.state == ScanState.IN_STRING_ESCAPE:
            # Consume this char, return to prior string state
            scanner.state = scanner.return_to or ScanState.NORMAL
            scanner.return_to = None
            i += 1
            continue

        # IRI reference scanning: <...>
        if scanner.state == ScanState.IN_IRI:
            # We do not attempt to fully parse escapes, but we skip \> properly.
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == ">":
                scanner.state = ScanState.NORMAL
            i += 1
            continue

        # String scanning
        if scanner.state in (ScanState.IN_SHORT_DQ, ScanState.IN_LONG_DQ):
            if ch == "\\":
                scanner.return_to = scanner.state
                scanner.state = ScanState.IN_STRING_ESCAPE
                i += 1
                continue
            if scanner.state == ScanState.IN_SHORT_DQ:
                if ch == '"':
                    scanner.state = ScanState.NORMAL
                i += 1
                continue
            # long """
            if ch == '"':
                scanner.dq_run += 1
                if scanner.dq_run >= 3:
                    scanner.state = ScanState.NORMAL
                    scanner.dq_run = 0
                i += 1
                continue
            scanner.dq_run = 0
            i += 1
            continue

        if scanner.state in (ScanState.IN_SHORT_SQ, ScanState.IN_LONG_SQ):
            if ch == "\\":
                scanner.return_to = scanner.state
                scanner.state = ScanState.IN_STRING_ESCAPE
                i += 1
                continue
            if scanner.state == ScanState.IN_SHORT_SQ:
                if ch == "'":
                    scanner.state = ScanState.NORMAL
                i += 1
                continue
            # long '''
            if ch == "'":
                scanner.sq_run += 1
                if scanner.sq_run >= 3:
                    scanner.state = ScanState.NORMAL
                    scanner.sq_run = 0
                i += 1
                continue
            scanner.sq_run = 0
            i += 1
            continue

        # We are in NORMAL here
        if ch == "<":
            # Extract the IRI payload (best-effort) for counting.
            # We'll scan forward for the next unescaped '>'.
            j = i + 1
            iri_chars: List[str] = []
            while j < n:
                cj = line[j]
                if cj == "\\" and j + 1 < n:
                    # keep the escaped char literal (roughly)
                    iri_chars.append(line[j + 1])
                    j += 2
                    continue
                if cj == ">":
                    break
                iri_chars.append(cj)
                j += 1

            if j < n and line[j] == ">":
                iri = "".join(iri_chars)
                ns = split_namespace(iri, namespace_split_mode)
                namespace_counts[ns] += 1
                iriref_counts["<...>"] += 1
                i = j + 1
                continue

            # If no closing '>' on this line, treat as entering IN_IRI state.
            scanner.state = ScanState.IN_IRI
            i += 1
            continue

        # Enter strings
        if ch == '"':
            # Detect """ long string
            if i + 2 < n and line[i : i + 3] == '"""':
                scanner.state = ScanState.IN_LONG_DQ
                scanner.dq_run = 0
                i += 3
                continue
            scanner.state = ScanState.IN_SHORT_DQ
            i += 1
            continue

        if ch == "'":
            # Detect ''' long string
            if i + 2 < n and line[i : i + 3] == "'''":
                scanner.state = ScanState.IN_LONG_SQ
                scanner.sq_run = 0
                i += 3
                continue
            scanner.state = ScanState.IN_SHORT_SQ
            i += 1
            continue

        # Prefixed name scanning (approx):
        # - prefix:local or prefix: or :local
        # - only if we're at a token boundary-ish (previous char not local/prefix char)
        if ch == ":" or _is_pn_prefix_start(ch):
            prev = line[i - 1] if i > 0 else " "
            if _is_local_char(prev) or _is_pn_prefix_char(prev):
                i += 1
                continue

            # Parse prefix
            pfx = ""
            j = i
            if ch == ":":
                pfx = ""
                j = i + 1
            else:
                # read pn_prefix
                k = i
                while k < n and _is_pn_prefix_char(line[k]):
                    k += 1
                if k < n and line[k] == ":":
                    pfx = line[i:k]
                    j = k + 1
                else:
                    i += 1
                    continue

            # Parse local (optional); pname_ns like "rdf:" is valid usage too.
            k = j
            while k < n and _is_local_char(line[k]):
                k += 1

            qname_counts["qname"] += 1
            qname = f"{pfx}:{line[j:k]}"
            qname_counts[qname] += 1

            ns = prefix_map.get(pfx)
            if ns is None:
                unresolved_prefix_counts[pfx or "(default)"] += 1
            else:
                namespace_counts[ns] += 1

            i = k
            continue

        i += 1


def main() -> None:
    args = parse_args()
    path = Path(args.input)
    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(2)

    file_size = path.stat().st_size
    t0 = time.time()

    prefix_map: Dict[str, str] = {}
    scanner = Scanner()
    namespace_counts: Counter = Counter()
    unresolved_prefix_counts: Counter = Counter()
    qname_counts: Counter = Counter()
    iriref_counts: Counter = Counter()

    bytes_read = 0
    next_progress = args.progress_mb * 1024 * 1024

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            bytes_read += len(line.encode("utf-8"))
            if args.max_bytes and bytes_read > args.max_bytes:
                break
            scan_line(
                line=line,
                scanner=scanner,
                prefix_map=prefix_map,
                namespace_counts=namespace_counts,
                unresolved_prefix_counts=unresolved_prefix_counts,
                qname_counts=qname_counts,
                iriref_counts=iriref_counts,
                namespace_split_mode=args.namespace_split,
            )

            if bytes_read >= next_progress:
                elapsed = time.time() - t0
                pct = (bytes_read / file_size * 100) if file_size else 0.0
                rate = (bytes_read / (1024**2) / elapsed) if elapsed > 0 else 0.0
                print(
                    f"[{pct:5.1f}%] scanned {bytes_read / (1024**3):.2f} GB "
                    f"({rate:.0f} MB/s) namespaces={len(namespace_counts):,} prefixes={len(prefix_map):,}"
                )
                next_progress += args.progress_mb * 1024 * 1024

    elapsed = time.time() - t0

    family_distinct_0: Counter = Counter()
    family_usage_0: Counter = Counter()
    family_distinct_1: Counter = Counter()
    family_usage_1: Counter = Counter()
    family_distinct_2: Counter = Counter()
    family_usage_2: Counter = Counter()

    for ns, c in namespace_counts.items():
        f0 = iri_family(ns, 0)
        f1 = iri_family(ns, 1)
        f2 = iri_family(ns, 2)
        family_distinct_0[f0] += 1
        family_usage_0[f0] += c
        family_distinct_1[f1] += 1
        family_usage_1[f1] += c
        family_distinct_2[f2] += 1
        family_usage_2[f2] += c

    print()
    print(f"Input: {path}")
    print(f"Elapsed: {elapsed:.1f}s")
    print()
    print("Counts:")
    print(f"  Declared prefixes seen:         {len(prefix_map):,}")
    print(f"  Distinct namespaces used:       {len(namespace_counts):,}")
    print(f"  IRI references seen (<...>):    {iriref_counts['<...>']:,}")
    print(f"  Prefixed names seen (qname):    {qname_counts['qname']:,}")
    if unresolved_prefix_counts:
        print(f"  Unresolved prefixes seen:       {len(unresolved_prefix_counts):,}")
    print()

    print(f"Top {args.top} namespaces by usage:")
    for ns, c in namespace_counts.most_common(args.top):
        print(f"  {c:>12,}  {ns}")

    print()
    print("Where the distinct namespaces come from (by URL family):")
    print("  Top 15 by distinct namespaces (scheme+host):")
    for fam, cnt in family_distinct_0.most_common(15):
        print(f"  {cnt:>12,}  {fam}")
    print("  Top 15 by distinct namespaces (scheme+host + 1 path segment):")
    for fam, cnt in family_distinct_1.most_common(15):
        print(f"  {cnt:>12,}  {fam}")

    if unresolved_prefix_counts:
        print()
        print("Top unresolved prefixes (suggests missing/late @prefix declarations):")
        for pfx, c in unresolved_prefix_counts.most_common(min(25, len(unresolved_prefix_counts))):
            print(f"  {c:>12,}  {pfx}")

    report = {
        "input": str(path),
        "elapsed_seconds": elapsed,
        "file_size_bytes": file_size,
        "declared_prefixes_seen": len(prefix_map),
        "distinct_namespaces_used": len(namespace_counts),
        "namespace_split_mode": args.namespace_split,
        "counts": {
            "iriref": iriref_counts["<...>"],
            "qname": qname_counts["qname"],
        },
        "families": {
            "host": family_distinct_0.most_common(50),
            "host_plus_1": family_distinct_1.most_common(50),
            "host_plus_2": family_distinct_2.most_common(50),
        },
        "unresolved_prefixes": unresolved_prefix_counts.most_common(),
        "top_namespaces": namespace_counts.most_common(args.top),
        # Full maps can be very large; keep them only if JSON output requested.
    }

    if args.json_out:
        report["prefix_map"] = prefix_map
        report["all_namespaces"] = namespace_counts.most_common()
        out_path = Path(args.json_out)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print()
        print(f"Wrote JSON report: {out_path}")


if __name__ == "__main__":
    main()

