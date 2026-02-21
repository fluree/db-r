# Forward dictionary packing strategy (subject/string forward)

This document describes a forward-dictionary layout intended to make **ID → bytes** resolution fast for:

- **Local disk** (fast warm reads via `mmap` + OS page cache)
- **Object storage** (S3/GCS/Azure) using **range GETs** against a small number of large immutable objects

The primary goal is to reduce the “many small objects” problem seen with dictionary trees where a single query that materializes many strings/IRIs ends up touching hundreds of small leaf objects.

This strategy is designed to be implemented **before** (and independently of) incremental fact-index updates.

## Scope

- Covers **forward** dictionaries only:
  - **string forward**: `str_id → string bytes`
  - **subject forward**: `s_id → IRI` (stored as suffix bytes + `ns_code` prefix table)
- Does **not** change fact indexes (leaf/leaflet) or reverse dictionaries in the initial phase.

## Terminology

- **Pack**: a large immutable blob/object (e.g. 256 MiB) that contains many smaller pages and a directory.
- **Page**: the smallest “atomic” unit we seek/fetch/cache (e.g. ~2 MiB). Pages are stored inside packs.
- **Branch / routing table**: metadata mapping ID ranges to pack locations. Prefer inlining this in the index root to avoid an extra fetch.
- **Stream**: an append-only sequence of packs for a single ID space (e.g. `string_fwd`, or `subject_fwd` for one `ns_code`).

## Design principles

- **Few objects, many ranges**: pack many pages into a small number of large pack objects to reduce object count by ~100× while keeping page-level locality.
- **Range-addressable**: pages must be addressable as `(pack_cid, offset, len)` for object storage range GET.
- **mmap-friendly**: packs should be simple to `mmap` locally so page reads become memory reads.
- **Append-only**: forward ID spaces are monotonic; we never rewrite old packs on updates.
- **Contiguous IDs within a page**: avoid storing per-entry IDs and per-entry lengths; use offsets-of-next layout.

## Layout overview

### A. String forward (global monotonic IDs)

The string forward dictionary assigns `str_id` monotonically across the whole ledger. This is naturally append-only.

**Physical layout**

```
string_fwd/
  pack_0000.pak   # covers str_id range [0 .. N0]
  pack_0001.pak   # covers [N0+1 .. N1]
  ...
```

Each `pack_XXXX.pak` contains pages. Each page covers a contiguous `str_id` sub-range.

### B. Subject forward (monotonic per namespace)

Subject IDs embed a namespace code (`ns_code`) and a namespace-local counter. IDs are monotonic *within* a namespace, not globally.

To keep append-only semantics without giving up namespace-code compression, we model subject forward as **one stream per `ns_code`**:

```
subject_fwd/
  ns_0000/
    pack_0000.pak
    pack_0001.pak
    ...
  ns_0001/
    pack_0000.pak
    ...
```

**Stored value bytes**: suffix bytes only (no prefix). At materialization time, prefix is derived from the `ns_code → prefix` table already stored in the index root.

This yields:

- append-only writes for subjects
- small number of objects per namespace (even for large datasets)
- retains compression and locality benefits of namespace codes

## Pack format (proposed)

Packs are immutable blobs stored in CAS/object storage and cacheable by CID.

### Pack header

At minimum:

- magic/version (e.g. `FDK1`)
- pack kind: `string_fwd` or `subject_fwd`
- namespace code (for `subject_fwd` packs)
- `first_id` (inclusive) and `last_id` (inclusive) for the ID space covered by this pack
- `page_count`
- page directory offset/length

### Page directory

Pack contains a directory for pages. Each page entry includes:

- `page_first_id` (inclusive)
- `page_entry_count`
- `page_offset` (byte offset in pack)
- `page_len` (bytes)

This directory enables:

- local `mmap` + binary search to locate a page
- remote range GET to fetch only the page bytes

### Page format: offsets-of-next (QLever-like)

Each page stores variable-length string bytes without per-entry IDs:

```
page_first_id: u64             (optional; often redundant if present in directory)
entry_count:  u32
offsets:      u32[entry_count + 1]   // offsets into `data`, last is end offset
data:         u8[ offsets[entry_count] ]
```

Lookup for an ID inside a page:

```
local = id - page_first_id
start = offsets[local]
end   = offsets[local + 1]
value = data[start..end]
```

Notes:

- Offsets are `u32` to keep overhead low; `data_len` must be ≤ 4 GiB per page (safe for a 2 MiB target).
- Page-level contiguity is required; the branch guarantees each page covers a contiguous ID range.

## Branch/routing metadata (prefer inline in index root)

The branch is intentionally small and should typically be stored inline in the index root (IRB1) to avoid a fetch just to route dictionary lookups.

### String forward branch (inline)

An array of pack entries sorted by `first_id`:

- `first_id`
- `last_id`
- `pack_cid`
- `page_directory_offset` / `page_directory_len` (if the pack directory isn’t at a fixed offset)

Routing:

- binary search pack array to find the pack containing `str_id`
- within pack, binary search page directory to find page containing `str_id`

### Subject forward branch (inline)

For each `ns_code`, store a pack list:

- `ns_code`
- packs: `[(first_local, last_local, pack_cid, ...)]`

Routing:

- decode `s_id` into `(ns_code, local_id)`
- select `ns_code` table
- binary search to find pack containing `local_id`
- locate page, then entry as in string forward

### Why inline

- For common queries, the overhead of “fetch branch object” is pure latency on S3.
- Branch data is small (tens of packs per stream in most cases), so inlining is cheap relative to the root size.

## Local storage behavior (mmap path)

On local disk:

- open pack files and `mmap` them
- locating a page becomes pointer arithmetic + binary search over a small in-memory directory
- reading a value becomes reading `offsets` and slicing `data` directly from the mapped bytes

This is analogous to QLever’s “mmapped offsets + big words blob”, except we use **packed pages inside packs** so we can share one format with object storage.

## Object storage behavior (range GET path)

On S3-like storage:

- Packs are large immutable objects (e.g. 256 MiB) → **very low object count**.
- A lookup fetches only the needed **page ranges** via HTTP range GET (e.g. ~2 MiB).
- Multiple lookups can be batched:
  - pre-collect unique IDs
  - sort by ID
  - group by `(pack_cid, page)` and merge adjacent ranges into fewer GETs

This turns “hundreds of small object GETs” into “a few objects + tens of range requests”, typically a large win.

## Update strategy (many small updates)

Forward dict updates are append-only. We want updates to propagate quickly without rewriting large objects.

### LSM-like mini-pages → pack compaction (recommended)

- Writers emit **small immutable pages** (~2 MiB) as new IDs are allocated.
- Once we have 2 pages, merge into 1 (or more generally, do leveled compaction) until reaching a sealed pack size (e.g. 256 MiB).
- Sealed packs are immutable CAS objects and are referenced from the root’s branch.

This allows:

- fast publish/replication for high-frequency updates (small objects initially)
- eventual consolidation into large packs for query performance (few objects)

### Alternative: single “active pack” per stream

Maintain one actively growing pack until it reaches the target size, then seal.

This is simpler but can be less replication-friendly if the active pack is rewritten or re-uploaded frequently. The mini-page approach avoids that.

## Compression (deferred decision)

For the initial experiment, store **raw UTF-8 bytes** in pages to isolate “layout wins” from compression wins.

Later options:

- add FSST-like per-block codebooks (QLever style) for `string_fwd`
- keep subject forward as suffix bytes + prefix table (already strong structural compression)

Compression should be evaluated separately for:

- local warm reads (CPU vs memory bandwidth)
- object storage (bytes transferred vs CPU)

## Migration / compatibility

We can support multiple dictionary backends simultaneously:

- legacy `DTB1 + DLF1/DLR1` trees
- packed forward dict packs (this doc)

Readers select based on root metadata (magic/version/format flags). This lets us:

- ship the packed format experimentally for TSV/materialization
- keep reverse dicts and other formats unchanged initially

## Why this matters for TSV/materialization

The observed TSV bottleneck was dominated by dictionary leaf access patterns:

- many distinct string IDs → many dict leaf objects touched
- object store and cold caches amplify fixed per-object overhead

Packing reduces object count by ~100× while preserving page-level locality, which is the primary driver for improving “materialize many values” workloads.

