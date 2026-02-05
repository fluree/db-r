# Late Materialization (On-Demand Materializer) Design

This document proposes a **general** late-materialization strategy for Fluree’s query engine to avoid eager decoding (dictionary lookups, IRI/string decoding, datatype/lang/meta construction) when downstream operators do not require it.

The goal is to make the hot path operate primarily on **encoded integer IDs**, and to provide a **single shared materializer** that decodes values **only when required** (FILTER/ORDER BY/formatting/cross-ledger join semantics), while preserving correctness for hashing/grouping and multi-ledger joins.

---

## Problem Statement (Observed in Q2 and similar)

Certain queries perform massive scans where:

- subjects are only needed to be counted (`COUNT`), never displayed
- objects are grouped/compared, but only a small set of unique values reach output

Yet the current binary scan path eagerly:

- resolves `s_id -> IRI -> Sid` per row
- decodes `(o_kind,o_key) -> FlakeValue` per row (often hitting string dictionaries)
- constructs `Binding`s for every row and pushes them into GROUP BY / DISTINCT hash structures

This causes:

- **millions of dictionary lookups** that are semantically unnecessary
- high memory pressure by storing full `Binding` rows in `GROUP BY` state

We want:

1. Scan and join in **encoded space**
2. Group, count, and sort using **integer comparisons / hashing**
3. Decode only the **final survivors** (projection/formatting), or decode only the specific columns required for ORDER BY / FILTER

---

## Core Design: Encoded-First Bindings + On-Demand Materializer

### Encoded-first output

In the binary index path, treat dictionary decoding as optional:

- subjects/predicates/objects can remain encoded (integer IDs)
- literals can remain as the existing `Binding::EncodedLit { ... }`
- references can remain encoded (e.g., `s_id` for refs) until required

This requires adding symmetric “encoded” binding variants for the rest of the RDF term space (proposed):

- `Binding::EncodedSid { s_id: u64 }` (subject or ref-object)
- `Binding::EncodedPid { p_id: u32 }` (predicate when `?p` is variable)
- keep `Binding::EncodedLit { o_kind,o_key,p_id,dt_id,lang_id,i_val,t }` (already exists)

All encoded variants must implement `Eq`/`Hash` with **stable semantics** (and should ignore metadata like `t` where appropriate, consistent with current `Binding` equality rules).

### Materializer: one synchronous decoding service

Introduce a **per-query** materializer object that provides conversions:

- **Join key materialization**
  - `JoinKey::Sid` (single-ledger)
  - `JoinKey::Iri` (dataset / multi-ledger)
- **Value materialization**
  - `Value::Comparable` (for FILTER comparisons, ORDER BY)
  - `Value::String` (REGEX, CONTAINS, STR functions, etc.)
  - `Value::Term` (final output formatting, SPARQL/JSON-LD term construction)

Key requirements:

- keep it **sync** for the binary index path (mmap + local files)
- avoid dynamic dispatch; use a small `enum Target` and `#[inline]` wrappers with cold slow paths
- cache inside the materializer (see below)

---

## Correctness Hazard: Do Not Mutate Hash Keys In-Place

> If you rewrite a `Binding` that is already being used as a hash key (e.g., in GROUP BY / DISTINCT / hash joins), you can break hash/equality invariants and corrupt results.

This is especially relevant when upgrading:

- `Binding::Sid` ↔ `Binding::IriMatch`
- `Binding::EncodedLit` ↔ `Binding::Lit`
- `Binding::EncodedSid` ↔ `Binding::Sid`

### Two safe patterns

#### Option A (safe default): materialize into separate *key objects*

Do not rewrite `Binding` values. Instead, hash/group/join uses a separate key type:

- `JoinKey` for joins/distinct
- `GroupKey` for GROUP BY (usually a small tuple of term keys)

The underlying `Binding` remains encoded; the key object is derived on demand:

```text
Binding (encoded) ──materializer──> KeyObject (borrowed/cheap)
```

This avoids all mutation hazards and is easiest to reason about.

#### Option B (fast but careful): allow rewrites only at defined boundaries

Allow rewriting into a cached variant (e.g., turning `Sid` into `IriMatch`) only when:

- the value is not currently used as a key in any hash table, and
- the rewrite happens at a well-defined boundary before any hashing/grouping that depends on it

Examples of safe boundaries:

- immediately after scan output in dataset mode (before hash joins)
- at join boundary when constructing the join key, before insertion into a hash join table
- just before final projection/formatting

Option B can be a later optimization once Option A is in place.

---

## Materializer Targets and Return Shapes (Zero overhead when unused)

### Target enum

Keep the “request API” small:

- `Target::JoinKey(JoinKeyMode)` where `JoinKeyMode ∈ { Sid, Iri }`
- `Target::Comparable`
- `Target::String`
- `Target::Term`

### Key objects should be cheap and mostly borrowed

For join/group keys, prefer borrowed views:

- if the binding already carries `Arc<str>` (e.g., `Binding::IriMatch` / `Binding::Iri`), the join key can borrow `&Arc<str>` or `&str`
- only `Sid` (dataset mode) and encoded IDs require decoding/alloc

### Caching belongs in the materializer (first)

Caching inside `Binding` is tempting but increases the chance of accidental hash-key mutation.

Instead, use per-query caches:

- `Sid -> Arc<str>` canonical IRI
- `(ledger_alias, Arc<str>) -> Sid` (re-encode for the current ledger if needed)
- `(o_kind,o_key,p_id,dt_id,lang_id,i_val) -> decoded FlakeValue / String / Comparable`
- `(s_id) -> Sid` (single-ledger), `(s_id) -> Arc<str>` (dataset)
- `(p_id) -> Sid` or `(p_id) -> Arc<str>` if predicates are variable

This yields the performance benefits of caching without mutating `Binding` values used in hash keys.

---

## Hook Points in This Codebase

### 1) Binary scan output boundary

Current hot spot (example described by Q2):

- `BinaryScanOperator::batch_to_bindings` resolves `s_id` and decodes objects eagerly.

Hook:

- make scan output **encoded-first**:
  - subjects as `EncodedSid { s_id }` unless requested by plan/consumer
  - objects as `EncodedLit` or `EncodedSid` for refs
  - predicates as `EncodedPid` when `?p` is variable

This is the foundation that allows downstream operators to avoid dictionary lookups entirely.

### 2) Join / hash join / distinct

Hash joins and DISTINCT should use `JoinKey` derived via materializer:

- single-ledger: `Sid` key
- dataset: canonical IRI string key (existing `IriMatch` logic becomes the materializer’s implementation)

This consolidates multi-ledger correctness into a single place.

### 3) GROUP BY + aggregates

The current GROUP BY implementation stores full rows:

`HashMap<Vec<Binding>, Vec<Vec<Binding>>>`

This is expensive even if `Binding` is encoded, and it forces large allocations.

General improvement (orthogonal but synergistic):

- introduce a streaming group+aggregate path where GROUP BY stores:
  - `HashMap<GroupKey, AggState>`
  - for COUNT: `AggState = u64`
  - for SUM/AVG/MIN/MAX: small fixed states
  - only “needs all values” aggregates (e.g., GROUP_CONCAT) store larger data

This avoids per-row retention while still being general.

### 4) FILTER / HAVING evaluation

FILTER evaluation should not special-case `Encoded*` bindings in arbitrary places.
Instead, when it needs a string/comparable, it calls materializer with the appropriate target.

This makes FILTER semantics correct and avoids decode work when FILTER is absent.

### 5) ORDER BY

Sort should materialize only the sort keys, and only to the representation needed for comparison.
Existing selective materialization for `EncodedLit` is a good starting point; expand it to:

- `EncodedSid` (if sorting on subject)
- dataset join keys if sorting on `?p` / IRIs

### 6) Formatting / projection

Final output is the natural terminal stage:

- only projected variables get fully materialized
- grouped keys that survive aggregation are materialized once per group

---

## Expected Performance Characteristics

### Where it helps most

- COUNT/GROUP BY queries over high-cardinality predicates (Q2)
- DISTINCT-heavy queries
- joins where only a subset of variables reach output
- multi-ledger joins where canonical IRI decoding can be cached per query instead of repeated

### Overheads introduced

- some additional branching at “use-site” (when code requests a representation)
- materializer cache map lookups (cheap, and only on demand)

In the “nothing needs decoding” case, the added overhead should be minimal if:

- encoded-first is default
- materializer calls are avoided unless required (no filter/order/format)

---

## Migration Plan (Incremental)

1. **Introduce encoded subject/predicate bindings** (`EncodedSid`, `EncodedPid`) and keep equality/hash stable.
2. Add a **per-query materializer** with small target enum + caches (sync).
3. Convert:
   - binary scan output to emit encoded bindings
   - FILTER/HAVING and ORDER BY to request materialization through the materializer
   - formatting to materialize only projected variables
4. Replace GROUP BY’s “store all rows” approach with a general **GroupAggregateOperator** (HashMap<GroupKey, AggState>) for common aggregates.
5. Fold existing dataset/multi-ledger IRI matching code into the materializer join-key path.

---

## Notes on Multi-Ledger (“hacky IriMatch”) Integration

The existing `Binding::IriMatch` concept is sound: canonical IRI is the correct cross-ledger join key.

The key change is to treat it as **materializer output**, not something sprinkled through operators:

- joins/distinct/group keys request `JoinKey::Iri`
- the materializer returns either borrowed canonical IRI (already present) or decodes/caches it

This centralizes correctness and reduces ad hoc conversions.

