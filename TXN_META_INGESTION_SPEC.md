# Transaction Metadata + Named Graph Ingestion (Mini-Spec)

## Summary

This spec defines how Fluree ingests transactional metadata alongside normal data writes, and how that metadata is stored/queryable in the `txn-meta` named graph.

- **Data writes** (JSON-LD `@graph`, Turtle triples) continue to ingest into the **default graph** (`g_id = 0`).
- **Transaction metadata** (extra fields in JSON-LD, or a TriG `GRAPH` block) is ingested into the **txn-meta named graph** (`g_id = 1`) as **properties on the commit subject**:
  - Subject: `fluree:commit:sha256:<hex>`
  - Graph: `#txn-meta` (fragment selector; pre-reserved; maps to `g_id=1`)
- Users query metadata via the existing ledger alias syntax:
  - `<ledger-alias>#txn-meta`

This spec intentionally **does not** add general-purpose user-defined named graph ingestion yet; it only defines ingestion and querying for the built-in `txn-meta` graph.

## Terminology

- **Default graph**: user data graph (`g_id=0`).
- **txn-meta graph**: internal named graph storing commit/transaction metadata (`g_id=1`).
- **Commit subject**: `fluree:commit:sha256:<hex>` (content-addressed commit identifier).
- **Txn metadata**: user-provided key/value fields that describe the transaction (machine address, internal user id, job id, etc.).

## Goals

- Allow users to attach arbitrary RDF-like metadata to inserts/upserts.
- Store metadata in `txn-meta` so it can be queried independently of user data.
- Ensure metadata is **replay-safe**: it must be reproducible during ledger load/replay and during binary index construction (not only at commit time).
- Avoid introducing or relying on `fluree:db:*` identifiers (commit ids only).

## Non-goals (for this phase)

- Arbitrary named graph ingestion (beyond `txn-meta`).
- Full TriG (we implement a minimal subset sufficient for txn-meta).
- RDF-star support.

---

## Storage model

### Graph

- txn-meta graph selector: `#txn-meta`
- Internal graph id mapping:
  - `g_id=0`: default graph
  - `g_id=1`: txn-meta graph (pre-reserved)

### Subject

All txn-meta metadata statements use the **commit subject** as subject:

- `fluree:commit:sha256:<hex>`

### Built-in commit metadata

The system emits standard commit metadata into txn-meta (examples; exact set may evolve):

- `f:alias` (ledger alias)
- `f:time` (epoch-ms)
- `f:t` (commit t)
- `f:size`, `f:flakes` (cumulative stats)
- `f:previous` (ref)
- `f:author`, `f:txn` (optional)

### User-provided txn metadata

User-provided txn metadata is emitted as *additional properties* on the commit subject in txn-meta:

- Example triples (stored in txn-meta):
  - `<fluree:commit:sha256:...> ex:machine "10.2.3.4" .`
  - `<fluree:commit:sha256:...> ex:internalUserId "u-123" .`
  - `<fluree:commit:sha256:...> ex:jobId "job-987" .`

---

## JSON-LD ingestion

### Accepted input shape

JSON-LD transactions support:

1. **User data** via `@graph` (existing behavior)
2. **Txn metadata** via any **top-level keys** other than JSON-LD keywords.

Example:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    { "@id": "ex:alice", "schema:name": "Alice" }
  ],
  "ex:machine": "10.2.3.4",
  "ex:internalUserId": "u-123",
  "ex:jobId": "job-987",
  "ex:tags": ["import", "nightly"]
}
```

### Metadata key selection rules

- A key is considered **txn metadata** if:
  - It is a top-level key, and
  - It does **not** start with `@`, and
  - It is not `@context` or `@graph`.
- All metadata keys MUST expand to an IRI via context processing (same expansion rules as other JSON-LD properties).

### Metadata value mapping rules

Each top-level metadata entry maps to one or more RDF objects:

- **Scalar** (`string`, `number`, `boolean`) → literal (with datatype inference identical to existing JSON-LD ingestion).
- **Array** → multiple statements with the same predicate.
- **Object**:
  - If it is a JSON-LD **value object** (`{"@value": ..., "@type": ...}` or `{"@value": ..., "@language": ...}`) → typed literal / langString.
  - If it is an **id object** (`{"@id": ...}`) → IRI object (reference).
  - Other objects are **rejected** in phase 1 (to keep metadata semantics predictable). (Future extension: allow nested objects as blank nodes.)

### Where the metadata is stored

- The txn metadata derived from top-level fields is stored in **txn-meta** (`g_id=1`) with **subject = commit subject**.
- It is **not** inserted into the default graph.

---

## Turtle / TTL ingestion

### Phase 1 (recommended): add a minimal TriG subset

We add support for parsing a minimal TriG subset in addition to Turtle:

- **Default graph triples**: parsed as Turtle, ingested to default graph (`g_id=0`).
- **Txn-meta graph block**: a TriG `GRAPH <...> { ... }` block is recognized for the txn-meta selector:
  - `GRAPH <#txn-meta> { ... }`
- Any other `GRAPH <...>` IRI is **rejected** in phase 1.

### “this commit” subject placeholder

Because the commit id is not known at parse time, TriG metadata must use a reserved placeholder subject:

- Placeholder IRI: `fluree:commit:this`

Rules:
- Inside the txn-meta graph block, any statement with subject `fluree:commit:this` is rewritten at emission time to the actual commit subject `fluree:commit:sha256:<hex>`.
- If the txn-meta graph block contains subjects other than `fluree:commit:this`, those statements are rejected in phase 1.

Example:

```trig
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

# default graph
ex:alice schema:name "Alice" .

# txn metadata
GRAPH <#txn-meta> {
  <fluree:commit:this> ex:machine "10.2.3.4" ;
                       ex:internalUserId "u-123" .
}
```

---

## Commit format + replay semantics (required)

### Requirement: replay-safe metadata

Txn metadata must be persisted in the commit blob so it can be re-emitted during:
- ledger load/replay (novelty rebuild)
- binary indexing (txn-meta emission)

### Proposed representation (v2 commit envelope)

Add an **optional** `txn_meta` field to the commit v2 envelope:

- `txn_meta`: a list of predicate/object pairs (multi-valued predicates are represented by repeated entries)

Encoding guidance:
- Keep it independent of the commit subject to avoid self-reference.
- Do **not** store `fluree:commit:this` in the envelope; the envelope is implicitly “about this commit”.
- The indexer/resolver will emit each pair as a txn-meta statement with:
  - subject = commit subject
  - predicate = expanded predicate IRI
  - object = encoded value (literal or ref)
  - graph = txn-meta (`g_id=1`)

### How built-in + user metadata combine

At emission time into txn-meta:
- System emits built-in commit metadata first.
- Then user-provided `txn_meta` entries are emitted.

Collision policy:
- Phase 1: **allow** user metadata to reuse any predicate IRIs (including `f:*`), but system semantics take precedence for query correctness.
- If collisions become problematic, we can add a configurable rule:
  - reject user predicates in `https://ns.flur.ee/ledger#*`, or
  - keep both (multi-valued), or
  - “system-wins” override.

---

## Querying

### Ledger alias reference syntax

Txn-meta is accessed by selecting the named graph fragment on the ledger alias:

- `<ledger-alias>#txn-meta`

This composes with existing:
- branches (`:<branch>`)
- time travel (`@t:`, `@iso:`, `@sha:`)

### SPARQL GRAPH patterns

Dataset-level querying should support:
- selecting txn-meta explicitly via dataset `FROM NAMED` / `GRAPH` patterns, and/or
- querying the view whose primary graph is `txn-meta` via `<alias>#txn-meta`.

---

## Validation + limits

### JSON-LD constraints (phase 1)

- Reject top-level metadata values that are objects other than value objects / id objects.
- Reject keys that cannot be expanded to an IRI.

### TTL/TriG constraints (phase 1)

- Accept at most one `GRAPH <...transactions...> { ... }` block.
- Inside that block:
  - only allow subject `fluree:commit:this`
  - reject other subjects
- Reject other named graph IRIs.

### Size limits (recommended)

To protect indexing/storage:
- Cap total txn-meta entries per transaction (e.g., 256 statements).
- Cap total bytes of txn-meta payload (e.g., 64KB).

---

## Test plan (minimum)

### JSON-LD

- Insert with top-level metadata keys; verify:
  - default graph contains user data
  - txn-meta graph contains metadata on commit subject
- Verify `t(?var)` works with metadata queries (covers encoded bindings path).

### TTL/TriG

- TriG with default + txn-meta block; verify metadata appears in txn-meta.
- Reject unknown graph IRI.
- Reject non-`fluree:commit:this` subject inside txn-meta.

### Replay

- Commit, reload ledger from commit history, confirm txn-meta metadata persists (not only in-memory).

---

## Rollout notes

- Phase 1 is intentionally scoped to txn metadata on commit subjects in txn-meta.
- General named graph ingestion can be layered later by:
  - allowing non-default graphs in op encoding/decoding
  - extending parsers to accept arbitrary graph IRIs
  - updating indexing and policy enforcement paths for multi-graph writes

