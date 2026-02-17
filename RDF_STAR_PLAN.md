# RDF-star / SPARQL-star support plan (statement annotations)

## Summary (what this plan enables)

This plan adds RDF-star/SPARQL-star quoted-triple *statement annotations* to Fluree **without polluting customer data** (no decomposition triples stored as flakes) and with **minimal impact on normal usage**.

At a high level:

- **Annotations are stored as ordinary flakes** using a synthetic statement subject `stmt_id`:
  - `(stmt_id, ann_predicate, ann_object, dt, t, op, m)`
- A **separate “statement dictionary” sidecar** provides the mapping between `stmt_id` and the quoted triple components `(s, p, o, dt)`.
- Query lowering/planning uses the sidecar **only when mapping is required** (e.g., `<< ?s ?p ?o >>` patterns, projecting `?s/?p/?o`, or filtering by components of a quoted triple).
- The sidecar is implemented as an **LSM-style structure** (novelty/memtable + immutable segments + manifest + compaction), so updates do **not** require rewriting one giant file.

This plan is designed so that if a ledger never uses RDF-star, it pays ~0 overhead: no extra flakes, no sidecar entries, and no query-path work.

## Current state in this repository (baseline)

- The SPARQL lexer recognizes `<<` / `>>` tokens.
- The SPARQL lowering currently supports a **very narrow RDF-star feature** for history/time bindings:
  - `<< s p ?o >> f:t ?t ; f:op ?op .`
  - This is expanded into the inner triple pattern plus `BIND(t(?o) AS ?t)` and `BIND(op(?o) AS ?op)`.
- Other quoted-triple contexts currently error with “not implemented; would require reifying quoted triples”.

This plan generalizes RDF-star support beyond `f:t`/`f:op` by introducing a statement identifier and sidecar dictionary.

## Goals / non-goals

### Goals

- **Spec-aligned semantics**: quoted triples are terms; annotating `<<s p o>>` does *not* assert `(s p o)`.
- **No customer-data pollution**: do **not** store decomposition triples as flakes alongside customer data.
- **Operationally simple**: a **single sidecar “dictionary”** (implemented as immutable segments) instead of maintaining multiple secondary indexes.
- **Reasonable performance**:
  - Fast for the expected common case “retrieve annotations for a known triple”.
  - Correct (but possibly slower) for less common shapes (predicate/object-bound quoted triple patterns).

### Non-goals (initially)

- Full SPARQL-star feature parity (e.g., nested quoted triples in arbitrary contexts, quoted triples in property paths, etc.). These may be added later.
- A general-purpose external columnar format dependency (Parquet) for the sidecar on day 1 (see “Parquet” notes below).

## Semantics decision: annotations do not depend on base-triple assertion

RDF-star quoted triples are *terms* and can be referenced/annotated even if the base triple is not asserted in the dataset.

**Decision:** Retracting the base flake `(s p o)` does **not** implicitly retract annotation flakes about `<<s p o>>`. This avoids cascade maintenance on normal operations and matches the “quoted triple not automatically asserted” semantic model.

If a future product requirement wants “cascade delete” behavior, it should be an **explicit operation/policy** (and likely implemented as a background cleanup), not an implicit rule.

## Data model

### 1) Statement identifier (`stmt_id`)

We represent a quoted triple `<<s p o>>` with a deterministic synthetic identifier `stmt_id` (an internal IRI/SID).

**Decision:** Use a deterministic hash-based ID so that:

- `stmt_id` can be computed directly for **ground** quoted triples (no sidecar read).
- Annotating the same `<<s p o>>` always targets the same subject.

#### Bucket prefix for bounded scans

To support the hard but required pattern `<< ex:bob ?p ?o >> annPred ?v` without scanning the entire sidecar, `stmt_id` includes a bucket prefix derived from the inner subject.

**Decision:** `stmt_id = concat( bucket(hash(s)), hash(s,p,o,dt) )`

- `bucket(hash(s))`: a small integer prefix (e.g., 10–14 bits) derived from `s` only.
- `hash(s,p,o,dt)`: stable hash of a canonical encoding of the full quoted triple.

This makes it cheap to restrict work to “statements about subject `s`” by bucket.

#### Representation

Implementation options (open; choose one):

- **SID-only internal namespace**: store `stmt_id` as a `Sid` in a reserved namespace (preferred for space).
- **Internal IRI form** (for debugging/interoperability): `fluree:stmt:<bucket>:<hash>` encoded to a `Sid`.

### 2) Storing annotations as flakes

RDF-star annotations become normal triples whose subject is the statement ID:

- `<<s p o>> annPred annObj .`
  - stored as flake: `(stmt_id, annPred, annObj, dt, t, op, m)`

Notes:

- `annPred` and `annObj` are normal RDF terms and use existing encoding rules.
- No “decomposition triples” are stored in the main database.

### 3) Statement dictionary sidecar (required for invertibility)

Because `stmt_id` is a hash, it is not invertible. We need a mapping so queries can bind `?s ?p ?o` from `stmt_id`.

**Decision:** Maintain a per-ledger sidecar mapping:

- `stmt_id -> (s, p, o, dt)`

This sidecar:

- is **not** part of the customer graph,
- is used only to support query lowering, joins, and result projection for RDF-star.

## Sidecar design (LSM-style, immutable artifacts)

### Why LSM-style here

We want:

- **incremental updates** without rewriting a monolithic file,
- **immutable storage artifacts** compatible with Fluree’s commit/snapshot model,
- background maintenance to compact/clean.

An LSM-style design matches existing patterns in this repo: novelty overlays + immutable persisted structures + background merge/rewrite.

### Components

#### 1) Memtable (novelty)

In-memory map (or small in-process structure) that records recent entries:

- `stmt_id -> (s,p,o,dt)`

This provides read-your-writes and amortizes small writes.

#### 2) Immutable segments (“SSTables”)

When memtable reaches a threshold, flush it into an immutable, sorted segment file.

Each segment stores entries sorted by `stmt_id` (or `(bucket, stmt_id)` if bucket is explicit).

Segment metadata should include:

- which buckets it contains (single bucket per segment is ideal, but not required),
- entry count,
- optional bloom filter on `stmt_id`,
- optional sparse index / block index,
- optional per-block min/max (primarily on `stmt_id`; optionally on `p` if stored as a column too).

#### 3) Manifest (versioned “current view”)

A small manifest object lists the segment set that constitutes the current statement dictionary:

- ordered by recency (for overlapping runs), and/or
- grouped by “levels” if compaction uses leveling.

Readers pin a manifest snapshot (stable view) similar to pinning a ledger view.

#### 4) Compaction / rewrite

Background maintenance merges segments to:

- remove overwritten duplicates,
- drop tombstones (if ever used),
- control the number of segments,
- optionally split/merge segments for size management.

### Sharding strategy (“worst case” scalability)

We assume worst-case “metadata is huge” must not fall over.

Primary scale mechanism is **segmenting**:

- many immutable files instead of one giant file,
- compaction controls read amplification.

Second mechanism is **bucket partitioning** based on `hash(s)`:

- allows bounding work for `<< s ?p ?o >>` patterns to a fraction of the sidecar,
- makes compaction parallelizable by bucket.

### Do we need bloom filters?

Bloom filters are helpful when:

- there are many segments,
- and queries do many `get(stmt_id)` lookups (e.g., scanning many annotation triples and projecting `?s ?p ?o`).

They are not required for correctness and can be introduced incrementally.

## Query lowering and planning

### Key principle

The main index stores annotations as normal flakes. The sidecar is used only for:

- computing `stmt_id` from quoted triples when possible,
- mapping `stmt_id -> (s,p,o,dt)` for binding/projection/filtering,
- generating candidate statement IDs when quoted triple is partially bound (especially subject-bound).

### Rewrite rules (SPARQL-star)

#### Case A: Ground quoted triple (fast path; no sidecar read)

Input:

```sparql
<< ex:s ex:p ex:o >> ex:confidence ?c .
```

Lowering:

1) compute `stmt_id = stmt_id(ex:s, ex:p, ex:o, dt)`
2) rewrite to:

```sparql
stmt_id ex:confidence ?c .
```

No sidecar access is needed.

#### Case B: Quoted triple subject-bound (candidate generation via bucketed sidecar scan)

Input:

```sparql
<< ex:bob ?p ?o >> ex:confidence ?c .
```

Lowering/planning:

1) compute `bucket = bucket(ex:bob)`
2) consult sidecar for candidate `stmt_id`s in that bucket where decoded `s == ex:bob`
   - optionally also apply any bound `p`/`o` constraints if present
3) join candidates to main index pattern `(stmt_id ex:confidence ?c)`

This avoids scanning all confidence annotations.

#### Case C: Quoted triple predicate/object-bound (fallback; correct but potentially slow)

Example:

```sparql
<< ?s ex:age ?o >> ex:confidence ?c .
```

If `s` is unbound, bucket routing cannot be used. The fallback plan is:

1) drive from annotation triples (e.g., `p = ex:confidence`), yielding `stmt_id`
2) decode `stmt_id` via sidecar mapping to obtain `(s,p,o,dt)`
3) apply constraints (`p == ex:age`, etc.)

This may require many sidecar lookups and can be slower, but maintains spec compliance.

### Result projection: returning quoted triples or their components

If a query projects `?s ?p ?o` that originate from quoted triple terms, the engine must:

- carry `stmt_id` as the join key,
- decode to components using the sidecar at projection time (ideally batched).

### Relationship to existing `t()` / `op()` history functions

This repo already has a Fluree-specific history mechanism:

- `t(?var)` and `op(?var)` extract metadata from bindings produced by scans.
- SPARQL lowering currently uses quoted triples only for `f:t` and `f:op`.

**Plan direction:** keep existing history functionality; add statement IDs for general RDF-star annotations.

Open question: should `<<s p o>>` refer to a timeless triple identity, while history-specific statement identities include `t/op`? See “Open decisions”.

## Transaction/ingest support

### SPARQL UPDATE

Implement parsing/lowering for RDF-star triple patterns in SPARQL UPDATE such that:

- annotation triples are inserted as flakes with subject `stmt_id`
- statement dictionary entries are created/updated for any referenced `stmt_id`

### JSON-LD

If JSON-LD ingestion is extended to support quoted triple annotation patterns, follow the same rule:

- customer-visible graph gets only the annotation triple with subject `stmt_id`
- sidecar stores the mapping for `stmt_id`

(Exact JSON-LD surface syntax is an open decision; SPARQL-star is the first driver.)

## Policy / security considerations

The statement dictionary sidecar is effectively a derived index that can reveal `(s,p,o,dt)` corresponding to internal statement IDs.

Policy enforcement must ensure:

- if a user is not allowed to see a base triple component, they should not be able to infer it via statement annotations.
- statement-id decoding during query execution should be subject to the same policy constraints as normal pattern matching.

Implementation idea:

- decode only to the extent needed for allowed variables,
- apply policy filters after decode but before emitting bindings.

## Operational behavior

### Storage lifecycle

- Sidecar artifacts are immutable and content-addressed like other ledger artifacts.
- Each commit may advance:
  - main index heads
  - sidecar manifest head (if there were RDF-star writes)

### Background maintenance

- compaction merges segments to control read amplification and reclaim space
- compaction can run per bucket to parallelize and bound work

### Export / “dump my triples”

Because decomposition is not stored as customer flakes, exports remain clean:

- dumping customer triples does **not** include dictionary entries.
- exporting RDF-star annotations should present the original quoted-triple syntax (if desired) by using sidecar decode as a presentation layer.

## Implementation sketch (where to change code)

### `fluree-db-sparql`

- Extend lowering beyond `f:t`/`f:op` special-case:
  - parse quoted triple terms wherever SPARQL-star requires,
  - lower them into IR patterns that can compute `stmt_id` or request sidecar candidate generation.

Touchpoints:

- `fluree-db-sparql/src/lower/rdf_star.rs`
- `fluree-db-sparql/src/lower/term.rs`

### `fluree-db-query`

- Add IR support for:
  - “compute stmt_id from ground triple”
  - “decode stmt_id to components” (used for projection and filters)
  - “generate candidate stmt_ids for subject-bound patterns” (sidecar scan)

This likely requires:

- a new execution operator or a new kind of scan source backed by the sidecar,
- batching APIs for decoding many `stmt_id`s efficiently.

### `fluree-db-api` / server layer

- Ensure the ledger handle/view has access to:
  - statement dictionary manifest/head for the chosen timepoint,
  - novelty overlay for read-your-writes.
- Ensure query endpoints can return quoted triple syntax when requested.

### `fluree-db-core` / storage primitives

- Add sidecar artifact addressing rules (similar to other content-addressed artifacts).
- Define stable hashing/canonicalization helpers for `(s,p,o,dt)` if they belong in core.

## Testing plan (minimum)

### Correctness (semantics)

- **Quoted triple is not asserted**:
  - Insert `<<s p o>> ex:confidence 0.9` without asserting `(s p o)`.
  - Query `(s p o)` should not match (unless separately asserted).
- **Annotations survive base triple retraction**:
  - Assert `(s p o)` and annotate it.
  - Retract `(s p o)`.
  - Annotations about `<<s p o>>` still queryable.

### Query pattern coverage

- Ground quoted triple annotation lookup (no sidecar read expected).
- Annotation scan returning `?s ?p ?o` (sidecar decode required).
- Subject-bound quoted triple pattern `<<ex:bob ?p ?o>>` (sidecar candidate generation).
- Predicate-bound quoted triple fallback `<< ?s ex:age ?o >>` (correctness).

### Performance sanity checks (non-bench)

- Ensure no sidecar access in the “easy path” if quoted triple is ground.
- Ensure sidecar read amplification stays bounded as segments grow (manifest + bloom as needed).

## Open decisions (must be resolved during implementation)

### Statement identity details

- **What exactly is hashed?**
  - include `dt` (currently planned: yes)
  - include language tag / list index metadata (probably no; those are not part of RDF triple identity)
  - include graph name / dataset context (if supporting named graphs)
- **Blank nodes / canonicalization**
  - how to hash blank nodes in quoted triples (Skolemize? scope to transaction? disallow in RDF-star initially?)
- **Hash algorithm and stability**
  - choose stable algorithm (e.g., SHA-256 or BLAKE3) and stable byte encoding for components.

### History interaction

- Should statement identity be “timeless triple” only, or also support “fact instance at t/op”?
  - Option: keep timeless `stmt_id(s,p,o,dt)` for RDF-star.
  - Keep existing history metadata (`t/op`) as separate Fluree features.
  - If users want to annotate specific historical assertions/retractions, define a second ID scheme (e.g., `fact_id(s,p,o,dt,t,op)`).

### Storage format choice

- Implement a simple custom segment format first (SSTable-like), or use Parquet as the segment encoding.
  - If using Parquet, you still need manifest + incremental update strategy; Parquet mainly changes the on-disk encoding and pruning metadata.

### Policy interaction

- Decide how to apply policy constraints when decoding `stmt_id` to components.
- Decide whether statement dictionary is considered sensitive and must be filtered per request.

### SPARQL-star surface area

- Which quoted triple contexts to support initially:
  - only quoted triple as subject of an annotation triple?
  - quoted triple in object position?
  - nested quoted triples?
  - quoted triples in property paths?

## Rationale recap (why these decisions)

- **Annotations as flakes**: reuse existing index machinery and keep annotation querying fast.
- **No decomposition flakes**: avoids polluting counts/exports/customer graph; avoids lifecycle complexity.
- **Sidecar dictionary**: required for invertibility and spec compliance (`<< ?s ?p ?o >> ...`).
- **Bucket-prefixed statement IDs**: enables bounded scans for subject-bound quoted triple patterns with minimal extra machinery.
- **LSM-style segments**: supports incremental updates without rewriting a monolithic artifact; aligns with Fluree’s immutable storage model.

