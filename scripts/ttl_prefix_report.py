#!/usr/bin/env python3
"""
Report Turtle prefix/base declarations.

This script answers: "How many namespace prefixes are declared in the source TTL?"
It focuses on the TTL header by default (where directives are expected), but can
also scan the whole file.

Examples:
  python3 scripts/ttl_prefix_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl
  python3 scripts/ttl_prefix_report.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --scan-all
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


RE_TTL_PREFIX = re.compile(
    r"^\s*@prefix\s+(?P<pfx>[A-Za-z_][A-Za-z0-9_.-]*|)\s*:\s*<(?P<ns>[^>]+)>\s*\.\s*$"
)
RE_TTL_BASE = re.compile(r"^\s*@base\s+<(?P<base>[^>]+)>\s*\.\s*$")
RE_SPARQL_PREFIX = re.compile(
    r"^\s*PREFIX\s+(?P<pfx>[A-Za-z_][A-Za-z0-9_.-]*|)\s*:\s*<(?P<ns>[^>]+)>\s*\.?\s*$",
    re.IGNORECASE,
)
RE_SPARQL_BASE = re.compile(r"^\s*BASE\s+<(?P<base>[^>]+)>\s*\.?\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class PrefixDecl:
    kind: str  # "@prefix" | "PREFIX"
    prefix: str  # may be empty for default prefix
    namespace: str
    line_no: int
    line: str


@dataclass(frozen=True)
class BaseDecl:
    kind: str  # "@base" | "BASE"
    base: str
    line_no: int
    line: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Report TTL @prefix/@base directives")
    p.add_argument("input", help="Path to the input Turtle file (.ttl)")
    p.add_argument(
        "--scan-all",
        action="store_true",
        help="Scan the entire file for directives (default: scan header only)",
    )
    p.add_argument(
        "--json-out",
        help="Write a JSON report to this path",
    )
    return p.parse_args()


def iter_header_lines(path: Path) -> Iterable[Tuple[int, str]]:
    """
    Yield (line_no, line) for the TTL header block.

    Mirrors the heuristic used by scripts/split_ttl.py:
    - directive lines (@prefix/@base/PREFIX/BASE) are part of the header
    - blank lines are allowed inside the header
    - first non-header line ends the header
    """
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            stripped = line.strip()
            if stripped.startswith(("@prefix", "@base", "PREFIX", "BASE")) or stripped == "":
                yield i, line
                continue
            break


def iter_all_lines(path: Path) -> Iterable[Tuple[int, str]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            yield i, line


def parse_directives(lines: Iterable[Tuple[int, str]]) -> Tuple[List[PrefixDecl], List[BaseDecl]]:
    prefixes: List[PrefixDecl] = []
    bases: List[BaseDecl] = []

    for line_no, line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue

        m = RE_TTL_PREFIX.match(line)
        if m:
            prefixes.append(
                PrefixDecl(
                    kind="@prefix",
                    prefix=m.group("pfx"),
                    namespace=m.group("ns"),
                    line_no=line_no,
                    line=line.rstrip("\n"),
                )
            )
            continue

        m = RE_SPARQL_PREFIX.match(line)
        if m:
            prefixes.append(
                PrefixDecl(
                    kind="PREFIX",
                    prefix=m.group("pfx"),
                    namespace=m.group("ns"),
                    line_no=line_no,
                    line=line.rstrip("\n"),
                )
            )
            continue

        m = RE_TTL_BASE.match(line)
        if m:
            bases.append(
                BaseDecl(kind="@base", base=m.group("base"), line_no=line_no, line=line.rstrip("\n"))
            )
            continue

        m = RE_SPARQL_BASE.match(line)
        if m:
            bases.append(
                BaseDecl(kind="BASE", base=m.group("base"), line_no=line_no, line=line.rstrip("\n"))
            )
            continue

    return prefixes, bases


def build_prefix_map(prefixes: List[PrefixDecl]) -> Tuple[Dict[str, str], Dict[str, List[PrefixDecl]]]:
    """
    Returns:
      - final mapping of prefix -> namespace (last definition wins)
      - conflicts mapping for prefixes redefined to a different namespace
    """
    prefix_to_ns: Dict[str, str] = {}
    conflicts: Dict[str, List[PrefixDecl]] = {}

    for d in prefixes:
        prev = prefix_to_ns.get(d.prefix)
        if prev is not None and prev != d.namespace:
            conflicts.setdefault(d.prefix, []).append(d)
        prefix_to_ns[d.prefix] = d.namespace

    return prefix_to_ns, conflicts


def main() -> None:
    args = parse_args()
    path = Path(args.input)
    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(2)

    lines = iter_all_lines(path) if args.scan_all else iter_header_lines(path)
    prefixes, bases = parse_directives(lines)

    prefix_map, conflicts = build_prefix_map(prefixes)
    unique_prefixes = sorted(prefix_map.keys())
    unique_namespaces = sorted({ns for ns in prefix_map.values()})

    print(f"Input: {path}")
    print(f"Mode:  {'scan-all' if args.scan_all else 'header-only'}")
    print()
    print("Declared directives:")
    print(f"  @prefix/PREFIX lines parsed: {len(prefixes):,}")
    print(f"  @base/BASE lines parsed:     {len(bases):,}")
    print(f"  Unique prefixes:             {len(unique_prefixes):,}")
    print(f"  Unique namespace IRIs:       {len(unique_namespaces):,}")
    if bases:
        unique_bases = sorted({b.base for b in bases})
        print(f"  Unique base IRIs:            {len(unique_bases):,}")
    print()

    if conflicts:
        print("Prefix redefinitions to different namespace IRIs:")
        for pfx in sorted(conflicts.keys()):
            # Show first 3 conflicting lines for the prefix (usually enough)
            shown = conflicts[pfx][:3]
            print(f"  - {pfx or '(default)'}: redefined {len(conflicts[pfx])} time(s)")
            for d in shown:
                print(f"      L{d.line_no}: {d.line}")
        print()

    # Print a small sample so it’s easy to eyeball if something looks off.
    print("Sample prefix map (up to 25 entries):")
    for pfx in unique_prefixes[:25]:
        label = f"{pfx}:" if pfx else ":"
        print(f"  {label:<12} {prefix_map[pfx]}")
    if len(unique_prefixes) > 25:
        print(f"  ... ({len(unique_prefixes) - 25} more)")

    report = {
        "input": str(path),
        "mode": "scan-all" if args.scan_all else "header-only",
        "prefix_lines_parsed": len(prefixes),
        "base_lines_parsed": len(bases),
        "unique_prefixes": len(unique_prefixes),
        "unique_namespaces": len(unique_namespaces),
        "prefix_map": prefix_map,
        "bases": [b.base for b in bases],
        "conflicts": {
            (k if k != "" else "(default)"): [{"line_no": d.line_no, "namespace": d.namespace} for d in v]
            for k, v in conflicts.items()
        },
    }

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print()
        print(f"Wrote JSON report: {out_path}")


if __name__ == "__main__":
    main()

