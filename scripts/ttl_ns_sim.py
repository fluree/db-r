#!/usr/bin/env python3
"""
Simulate namespace-prefix extraction on a Turtle source.

Goal:
  Quickly estimate how many distinct "namespace prefixes" a Turtle import will
  allocate, without running the full import pipeline.

This script scans only <...> IRI references (the hot path for subjects/objects
in large TTL dumps). It ignores anything inside strings and comments.

It computes distinct prefix counts under multiple split strategies:
  - last-slash-or-hash: split at last '/' or '#'
  - coarse-heuristic: match the current Rust coarse fallback heuristic

It also parses header @prefix/PREFIX directives to report how many declared
namespaces exist (these are always allocated during parse).

Examples:
  python3 scripts/ttl_ns_sim.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl
  python3 scripts/ttl_ns_sim.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --max-bytes $((1024*1024*1024))
  python3 scripts/ttl_ns_sim.py /Volumes/External-4TB-OWCEnvoy/dblp.ttl --max-gb 1.0 --progress-mb 128
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple


RE_TTL_PREFIX = re.compile(
    r"^\s*@prefix\s+(?P<pfx>[A-Za-z_][A-Za-z0-9_.-]*|)\s*:\s*<(?P<ns>[^>]+)>\s*\.\s*$"
)
RE_SPARQL_PREFIX = re.compile(
    r"^\s*PREFIX\s+(?P<pfx>[A-Za-z_][A-Za-z0-9_.-]*|)\s*:\s*<(?P<ns>[^>]+)>\s*\.?\s*$",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate namespace prefix extraction for TTL imports")
    p.add_argument("input", help="Path to .ttl file")
    p.add_argument(
        "--start-bytes",
        type=int,
        default=0,
        help="Start scanning at this byte offset (default 0). Useful to probe later file regions.",
    )
    p.add_argument(
        "--start-gb",
        type=float,
        default=0.0,
        help="Start scanning at this offset in GB (overrides --start-bytes if set).",
    )
    p.add_argument("--max-bytes", type=int, default=0, help="Stop after reading N bytes (0 = no limit)")
    p.add_argument("--max-gb", type=float, default=1.0, help="Stop after reading N GB (default 1.0). Ignored if --max-bytes is set.")
    p.add_argument(
        "--windows",
        type=int,
        default=1,
        help="Number of evenly spaced windows to scan (default 1). "
        "If >1, scans that many windows of size --max-bytes/--max-gb and unions results.",
    )
    p.add_argument("--progress-mb", type=int, default=256, help="Progress print interval MB (default 256)")
    p.add_argument("--top", type=int, default=25, help="Show top N prefixes by frequency (default 25)")
    return p.parse_args()


def iter_header_prefixes(path: Path) -> Dict[str, str]:
    """
    Parse TTL header directives, returning prefix -> namespace IRI.
    Mirrors scripts/split_ttl.py header heuristic.
    """
    prefix_map: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not (stripped.startswith(("@prefix", "@base", "PREFIX", "BASE")) or stripped == ""):
                break
            if not stripped or stripped.startswith("#"):
                continue
            m = RE_TTL_PREFIX.match(line)
            if m:
                prefix_map[m.group("pfx")] = m.group("ns")
                continue
            m = RE_SPARQL_PREFIX.match(line)
            if m:
                prefix_map[m.group("pfx")] = m.group("ns")
                continue
    return prefix_map


def split_last_slash_or_hash(iri: str) -> str:
    i = iri.rfind("/")
    j = iri.rfind("#")
    pos = i if i > j else j
    if pos == -1:
        return ""  # matches Fluree: empty prefix
    return iri[: pos + 1]


def split_host_only(iri: str) -> str:
    """
    Mirrors fluree-db-transact/src/namespace.rs HostOnly fallback mode.

    - http(s): scheme://host/
    - non-http(s): if no '/' or '#', split at 1st ':' when present
    - else: last-slash-or-hash
    """
    if iri.startswith("http://") or iri.startswith("https://"):
        scheme_end = iri.find("://")
        if scheme_end == -1:
            return split_last_slash_or_hash(iri)
        host_start = scheme_end + 3
        host_end = iri.find("/", host_start)
        if host_end == -1:
            return ""  # matches Rust: ("", iri)
        return iri[: host_end + 1]  # scheme://host/

    if ("/" not in iri) and ("#" not in iri) and (":" in iri):
        first = iri.find(":")
        return iri[: first + 1]

    return split_last_slash_or_hash(iri)

def split_host_plus_first_segment_only(iri: str) -> str:
    """
    Strict coarse split:
      - http(s): scheme://host/<seg1>/ (or scheme://host/ if no seg1 slash)
      - else: if no '/' or '#', split at 2nd ':' when present, else 1st ':'
      - else: last-slash-or-hash
    """
    if iri.startswith("http://") or iri.startswith("https://"):
        scheme_end = iri.find("://")
        if scheme_end == -1:
            return split_last_slash_or_hash(iri)
        host_start = scheme_end + 3
        host_end = iri.find("/", host_start)
        if host_end == -1:
            return ""
        seg1_start = host_end + 1
        seg1_end = iri.find("/", seg1_start)
        if seg1_end == -1:
            return iri[: host_end + 1]  # scheme://host/
        return iri[: seg1_end + 1]      # scheme://host/seg1/

    if ("/" not in iri) and ("#" not in iri) and (":" in iri):
        first = iri.find(":")
        second = iri.find(":", first + 1)
        if second != -1:
            return iri[: second + 1]
        return iri[: first + 1]

    return split_last_slash_or_hash(iri)


def _is_short_digit_bucket(seg: str) -> bool:
    return 0 < len(seg) <= 4 and seg.isdigit()


def split_coarse_heuristic(iri: str) -> str:
    """
    Mirrors fluree-db-transact/src/namespace.rs coarse heuristic (roughly).

    - http(s): generally host + 1 path segment (scheme://host/seg1/)
      * special-case: dblp pid numeric buckets → scheme://host/pid/<digits>/
    - non-http(s): if no '/' or '#', split at 2nd ':' when present (urn:isbn:)
    - fallback: last-slash-or-hash
    """
    if iri.startswith("http://") or iri.startswith("https://"):
        # Extract scheme://host/
        scheme_end = iri.find("://")
        if scheme_end == -1:
            return split_last_slash_or_hash(iri)
        host_start = scheme_end + 3
        host_end = iri.find("/", host_start)
        if host_end == -1:
            return ""  # no path, treat as empty prefix in Fluree fallback
        # seg1
        seg1_start = host_end + 1
        seg1_end = iri.find("/", seg1_start)
        if seg1_end == -1:
            return iri[: host_end + 1]  # scheme://host/
        seg1 = iri[seg1_start:seg1_end]

        # seg2 (optional)
        seg2_start = seg1_end + 1
        seg2_end = iri.find("/", seg2_start)
        if seg2_end != -1:
            seg2 = iri[seg2_start:seg2_end]
            if seg1 == "pid" and _is_short_digit_bucket(seg2):
                return iri[: seg2_end + 1]

        # Default: host/seg1/
        return iri[: seg1_end + 1]

    # Non-http(s): colon compression only when no '/' or '#'
    if ("/" not in iri) and ("#" not in iri) and (":" in iri):
        first = iri.find(":")
        second = iri.find(":", first + 1)
        if second != -1:
            return iri[: second + 1]
        return iri[: first + 1]

    return split_last_slash_or_hash(iri)


def _declared_ns_match(iri: str, declared_ns_by_len_desc: Tuple[str, ...]) -> Optional[str]:
    # Longest-prefix match among declared namespaces.
    for ns in declared_ns_by_len_desc:
        if iri.startswith(ns):
            return ns
    return None


@dataclass
class ScannerState:
    in_comment: bool = False
    in_iri: bool = False
    iri_buf: bytearray = bytearray()
    in_string: Optional[Tuple[int, bool]] = None  # (quote_byte, triple)
    quote_run: int = 0
    escape: bool = False


def scan_iris(
    path: Path,
    start_bytes: int,
    max_bytes: int,
    progress_interval_bytes: int,
) -> Iterable[str]:
    """
    Yield IRI strings from <...> references, skipping strings/comments.
    Reads as bytes for speed and robustness.
    """
    st = ScannerState()
    bytes_read = 0
    next_progress = progress_interval_bytes
    t0 = time.time()

    with path.open("rb") as f:
        if start_bytes:
            f.seek(start_bytes)
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)
            if max_bytes and bytes_read > max_bytes:
                # truncate processing to max_bytes
                chunk = chunk[: max(0, len(chunk) - (bytes_read - max_bytes))]
                bytes_read = max_bytes

            i = 0
            while i < len(chunk):
                b = chunk[i]

                if st.in_comment:
                    if b == 0x0A:  # \n
                        st.in_comment = False
                    i += 1
                    continue

                if st.in_iri:
                    if st.escape:
                        st.escape = False
                        st.iri_buf.append(b)
                        i += 1
                        continue
                    if b == 0x5C:  # backslash
                        st.escape = True
                        i += 1
                        continue
                    if b == 0x3E:  # >
                        st.in_iri = False
                        try:
                            yield st.iri_buf.decode("utf-8", errors="ignore")
                        finally:
                            st.iri_buf.clear()
                        i += 1
                        continue
                    st.iri_buf.append(b)
                    i += 1
                    continue

                if st.in_string is not None:
                    quote, triple = st.in_string
                    if st.escape:
                        st.escape = False
                        i += 1
                        continue
                    if b == 0x5C:  # backslash
                        st.escape = True
                        i += 1
                        continue
                    if not triple:
                        if b == quote or b in (0x0A, 0x0D):  # end quote or newline
                            st.in_string = None
                        i += 1
                        continue
                    # triple
                    if b == quote:
                        st.quote_run += 1
                        if st.quote_run >= 3:
                            st.in_string = None
                            st.quote_run = 0
                        i += 1
                        continue
                    st.quote_run = 0
                    i += 1
                    continue

                # Normal state
                if b == 0x23:  # #
                    st.in_comment = True
                    i += 1
                    continue
                if b == 0x3C:  # <
                    st.in_iri = True
                    st.escape = False
                    st.iri_buf.clear()
                    i += 1
                    continue
                if b == 0x22 or b == 0x27:  # " or '
                    # triple?
                    if i + 2 < len(chunk) and chunk[i + 1] == b and chunk[i + 2] == b:
                        st.in_string = (b, True)
                        st.quote_run = 0
                        i += 3
                    else:
                        st.in_string = (b, False)
                        i += 1
                    continue

                i += 1

            if bytes_read >= next_progress:
                elapsed = time.time() - t0
                rate = (bytes_read / (1024**2) / elapsed) if elapsed > 0 else 0.0
                print(
                    f"[scan] start={start_bytes/(1024**3):.2f}GB read={bytes_read/(1024**3):.2f}GB ({rate:.0f} MB/s)",
                    file=sys.stderr,
                )
                next_progress += progress_interval_bytes

            if max_bytes and bytes_read >= max_bytes:
                break


def main() -> None:
    args = parse_args()
    path = Path(args.input)
    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(2)

    start_bytes = int(args.start_gb * 1024**3) if args.start_gb > 0 else args.start_bytes
    if start_bytes < 0:
        start_bytes = 0

    max_bytes = args.max_bytes if args.max_bytes > 0 else int(args.max_gb * 1024**3)
    progress_interval_bytes = args.progress_mb * 1024 * 1024
    windows = max(1, args.windows)

    declared = iter_header_prefixes(path)
    declared_namespaces = set(declared.values())
    declared_ns_by_len_desc: Tuple[str, ...] = tuple(
        sorted(declared_namespaces, key=len, reverse=True)
    )

    # Unique namespace prefixes by strategy
    uniq_last: Set[str] = set(declared_namespaces)
    uniq_coarse: Set[str] = set(declared_namespaces)
    uniq_host1: Set[str] = set(declared_namespaces)
    uniq_host_only: Set[str] = set(declared_namespaces)

    # Frequency counters (prefix -> count)
    from collections import Counter

    freq_last = Counter()
    freq_coarse = Counter()
    freq_host1 = Counter()
    freq_host_only = Counter()

    iri_count = 0
    t0 = time.time()

    file_size = path.stat().st_size
    if windows == 1:
        starts = [start_bytes]
    else:
        # Evenly spaced windows across file, but always include the header region.
        # Clamp so we never seek beyond EOF.
        max_start = max(0, file_size - max_bytes)
        if max_start == 0:
            starts = [0]
        else:
            step = max_start // (windows - 1) if windows > 1 else 0
            starts = [min(max_start, i * step) for i in range(windows)]

    for w_idx, s in enumerate(starts):
        if windows > 1:
            print(f"[window {w_idx+1}/{windows}] start={s/(1024**3):.2f}GB", file=sys.stderr)
        for iri in scan_iris(
            path,
            start_bytes=s,
            max_bytes=max_bytes,
            progress_interval_bytes=progress_interval_bytes,
        ):
            if not iri:
                continue
            iri_count += 1
            declared_ns = _declared_ns_match(iri, declared_ns_by_len_desc)
            if declared_ns is not None:
                p_last = declared_ns
                p_coarse = declared_ns
                p_host1 = declared_ns
                p_host_only = declared_ns
            else:
                p_last = split_last_slash_or_hash(iri)
                p_coarse = split_coarse_heuristic(iri)
                p_host1 = split_host_plus_first_segment_only(iri)
                p_host_only = split_host_only(iri)
            uniq_last.add(p_last)
            uniq_coarse.add(p_coarse)
            uniq_host1.add(p_host1)
            uniq_host_only.add(p_host_only)
            freq_last[p_last] += 1
            freq_coarse[p_coarse] += 1
            freq_host1[p_host1] += 1
            freq_host_only[p_host_only] += 1

    elapsed = time.time() - t0
    print(f"Input: {path}")
    print(f"Start offset: {start_bytes:,} bytes")
    print(f"Window bytes: {max_bytes:,}")
    print(f"Windows: {windows}")
    print(f"IRI refs scanned (<...>): {iri_count:,}")
    print(f"Elapsed: {elapsed:.1f}s")
    print()
    print("Declared prefixes (header):")
    print(f"  unique @prefix namespaces: {len(declared_namespaces):,}")
    print()
    print("Distinct namespace prefixes (declared + observed IRIs):")
    print(f"  last-slash-or-hash: {len(uniq_last):,}")
    print(f"  coarse-heuristic:   {len(uniq_coarse):,}")
    print(f"  host+seg1-only:     {len(uniq_host1):,}")
    print(f"  host-only:          {len(uniq_host_only):,}")
    print()
    print(f"Top {args.top} namespaces by frequency (last-slash-or-hash):")
    for ns, c in freq_last.most_common(args.top):
        print(f"  {c:>12,}  {ns}")
    print()
    print(f"Top {args.top} namespaces by frequency (coarse-heuristic):")
    for ns, c in freq_coarse.most_common(args.top):
        print(f"  {c:>12,}  {ns}")
    print()
    print(f"Top {args.top} namespaces by frequency (host+seg1-only):")
    for ns, c in freq_host1.most_common(args.top):
        print(f"  {c:>12,}  {ns}")
    print()
    print(f"Top {args.top} namespaces by frequency (host-only):")
    for ns, c in freq_host_only.most_common(args.top):
        print(f"  {c:>12,}  {ns}")


if __name__ == "__main__":
    main()

