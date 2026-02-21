# Incremental Indexing Strategy (DB-R)

This document captures a **performance-oriented strategy** for adding **incremental binary index updates** to Fluree DB-R without full rebuilds, while preserving existing CAS/content-addressed semantics.

It is intentionally written as a strategy/architecture note (not a step-by-step implementation plan).

## Goals and invariants

- **Fast catch-up**: complete background indexing quickly by rewriting only what changed.
- **Content-addressed reuse**: unchanged artifacts keep their CIDs; the new root references a mixture of old + new artifacts.
- **Correct novelty trimming**: peers and local caches can drop novelty transactions \(t\) covered by the new index while keeping later novelty.
- **Single indexer per ledger**: never more than one indexing job runs concurrently for a given ledger.
- **Parallelizable work**:
  - ledgers are isolated (per-ledger single-flight),
  - within a ledger, **named graphs can be indexed in parallel**,
  - within a graph, **each order (SPOT/PSOT/POST/OPST) can be updated in parallel**.
- **Future-proof publishing**: publish enough **per-graph** metadata so we can later decouple “which graphs updated” from the single root publish.

## Current building blocks we build on

- **Fact indexes**: per-graph, per-order `FBR2` branch manifests pointing to `FLI2` leaf blobs.
- **Root descriptor**: `IndexRootV5` (`IRB1`) published to nameservice, containing:
  - default graph inline routing (leaves embedded),
  - named graph routing (branch CIDs),
  - forward dictionary pack refs (FPK1) + reverse dictionary tree refs (DTB1/DLR1),
  - optional numbig + vector arena refs,
  - optional GC chain refs (`prev_index`, `garbage`).
- **Query-time overlay merge** already exists (overlay ops merged into decoded leaflets). The same merge semantics can be reused to *materialize* novelty into new leaflets/leaves during incremental indexing.

## Index job boundaries: `from_t` and `to_t`

Incremental indexing runs as background jobs that must have a clear transaction interval:

- **`from_t`**: the ledger’s last published `index_t` (the root we are incrementing from).
- **`to_t`**: the latest known ledger `commit_t` at the moment we *actually start work*.
  - If a request is queued behind an in-progress job and commits continue to arrive, the queued job’s `to_t` should advance to the then-latest `commit_t` when it begins, rather than the older `commit_t` at request time.

### Why `to_t` matters (novelty trimming)

When we publish a new index root at `index_t = to_t`, any node/peer that already has the DB cached must be able to:

- **drop novelty entries with \(t \le index\_t\)**, and
- **retain novelty entries with \(t > index\_t\)** that arrived during the job.

This requires that the published root’s `index_t` precisely reflects the coverage of the materialized index state.

## Incremental fact index update strategy (per graph, per order)

### High-level idea: “rewrite only the touched leaves”

For each (graph, order):

1. Translate novelty between `from_t..=to_t` into **sorted overlay ops** in that order’s key space.
2. Use the existing branch manifest routing (leaf key ranges) to identify which leaf blobs overlap the overlay keys.
3. For each overlapped leaf:
   - decode its leaflets,
   - **merge overlay ops into the leaflet row stream** (same semantics as query overlay),
   - re-encode updated leaflets back into one or more `FLI2` leaf blobs,
   - **reuse untouched leaves by CID**; within rewritten leaves, carry through unchanged leaflet bytes without recompression where possible.
4. Produce a new `FBR2` for the graph+order (or update default graph inline routing).

This approach optimizes for:

- minimal decompression and recompression,
- maximal CAS reuse,
- predictable performance when novelty is localized.

### Region 3 / time-travel correctness

`FLI2` leaflets may contain Region 3 (history journal). Incremental materialization must preserve time-travel semantics:

- When a fact is updated/retracted/reasserted in `from_t..=to_t`, the corresponding history events must be retained so replay remains correct from `base_t` onward.
- If we materialize novelty into Region 1/2 without updating Region 3 appropriately, queries at `to_t' < to_t` can become incorrect.

### Parallelism model

- **Across graphs**: process each `g_id` concurrently.
- **Across orders**: within a graph, process SPOT/PSOT/POST/OPST concurrently.
- **Within an order**: process touched leaves concurrently with bounded parallelism (CPU + IO bound).

Constraint: per-ledger orchestration ensures only **one job per ledger** runs; internal parallelism happens *inside* the job.

## Leaflet/leaf splitting strategy (incremental writes)

We treat leaf updates as “insertions into existing leaflets/leaves” followed by deterministic split/rewrap rules to keep artifacts bounded.

### Definitions (current targets)

- **Leaflet target rows**: 25,000.
- **Leaf target leaflets-per-leaf**: 10.
- **Leaf target rows** (approx): 10 × 25,000 = 250,000.

### Split triggers (leaflet-level first)

When applying novelty produces growth, we split at the leaflet level using the following thresholds:

#### 1) Leaflet growth split threshold

- If a leaflet’s post-merge row count exceeds **\(25,000 × 1.5 = 37,500\)**, we split.
- Novelty may be highly concentrated, so **more than one split** may be needed:
  - e.g., if a leaflet grows to ~75k rows, it needs two splits to restore near-target sizes.

Split unit: leaflet row stream (post-merge) is split into multiple leaflets whose row counts are close to the 25k target.

#### 2) Leaf “too many leaflets” split threshold

If a leaf container accumulates too many leaflets after leaflet-level splitting:

- Once we reach **2× leaflets-per-leaf** (today: 20 leaflets),
  - split the leaf container into multiple leaf blobs.

Canonical split:

- Prefer splitting into leaves of **10 leaflets + remainder**.
- Support >2 splits:
  - e.g., 30 leaflets becomes 3 leaves, etc.

Notes:

- Splitting is expected to be rarer at the leaf-container level than the leaflet level.
- This split policy keeps blob sizes and query locality stable without requiring full rebalancing.

### Branch/routing updates

After rewrite/split:

- update `FBR2` leaf entry ranges (`first_key`/`last_key`) to match the new leaf boundaries.
- preserve sort order and key-range non-overlap invariants (required for binary search routing).

## Publishing and peer propagation

### Publishing to nameservice

On completion of a job:

- write/upload new or changed CAS artifacts,
- write a new `IndexRootV5` referencing:
  - reused old leaf/branch/dict artifacts by CID,
  - new artifacts where rewritten,
  - updated `index_t` (= job `to_t`),
  - `prev_index` + `garbage` info as applicable,
- publish via nameservice (`publish_index` semantics).

### Per-graph publication metadata (future-facing)

We should carry “which graphs changed” as explicit metadata as part of the publish path so that:

- today: we can log/observe which graphs were actually touched,
- future: we can publish per-graph index advancement or partial roots if we decide to decouple.

Practical note: the txn metadata graph is expected to update frequently alongside:

- the default graph, and
- any user named graphs touched by novelty.

### Peer consumption: trimming novelty

Peers that receive nameservice notifications and already have ledger state cached must:

- detect index-head advancement,
- load the new root on demand,
- trim novelty to only \(t > index\_t\).

This requires the consumer path to treat “index advanced but commit_t unchanged” as a lightweight update (avoid full reload).

## Dictionary strategy

### Forward dictionaries: subjects + strings (append-friendly)

Forward dictionaries in `IndexRootV5` are represented as **FPK1 packs** (routing table of `(first_id,last_id) -> pack CID`).

Strategy:

- Treat forward dict updates as **append-only** ID ranges.
- In the first phase, build/replace only the “tail pack”:
  - accumulate new entries into the current last pack until it reaches the target size (currently ~256 MiB),
  - when it exceeds target, seal it and append a new pack entry.

This is an LSM-like growth pattern without compaction initially.

### Reverse dictionaries: subjects + strings (frequently updated)

Reverse dictionaries are CoW single-level trees (`DTB1` branch + `DLR1` leaves).

Strategy:

- Use the same split logic style as fact indexes:
  - route changed keys to a leaf,
  - rewrite only affected leaf blobs,
  - split leaf blobs when they exceed the target size,
  - update the `DTB1` branch manifest accordingly.

This keeps update cost proportional to changed keys, not total dictionary size.

## Specialty indexes / arenas (must update carefully)

Some predicates maintain auxiliary index artifacts referenced from the root:

- **Vector arenas** (per-predicate manifests + shards)
- **NumBig arenas** (per-predicate)
- **Geo / spatial** artifacts (if enabled/used)

Strategy considerations:

- **Update only touched predicates**: novelty should drive which predicate arenas must be rebuilt/extended.
- **Preserve immutability + CAS reachability**: any new shard/arena must be referenced from the new root so peers can fetch on demand and GC can reason about reachability.
- **Concurrency**: these are good candidates for parallel updates (per predicate) as long as we maintain deterministic final root assembly.
- **Failure isolation**: a failure updating an auxiliary arena should be handled explicitly (either fail the incremental job and retry, or publish without that auxiliary update only if correctness is preserved — default is to treat as correctness-critical when the predicate is query-visible).

## Concurrency and safety notes

- **Per-ledger single-flight**: enforce at orchestrator level (one job per ledger).
- **Deterministic assembly**: even with parallel work, root encoding must be deterministic (stable ordering of graphs, orders, dictionary refs).
- **Bounded resource usage**:
  - limit concurrent leaf rewrites and CAS uploads (semaphores),
  - prefer streaming encode/write patterns to avoid large intermediate buffers.

## Non-goals (initial phase)

- No multi-segment query-time merging of multiple index snapshots (we keep a single root snapshot view).
- No forward-dict compaction beyond “replace tail pack until sealed”.
- No sophisticated global rebalancing across all leaves (we split only where growth dictates).

