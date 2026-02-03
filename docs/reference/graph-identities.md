# Graph Identities and Naming
This document defines **names → things** and recommended naming conventions for Fluree.
It is split into:
- **User-facing naming**: what we say in docs, examples, and APIs.
- **Internal naming**: how we name types/components in Rust so the implementation stays clear.
\
The goal is to make these simultaneously true:
- Users can think in the familiar model: **“database as a value”** (immutable, time-travelable).
- SPARQL semantics remain correct: `GRAPH <…>` identifies a **graph by IRI**.
- Fluree can seamlessly query across:
  - graphs inside a ledger (default + named graphs),
  - across ledgers (federation),
  - and non-ledger sources (BM25, vector, Iceberg/R2RML, etc.).

## Summary: the model in one paragraph
In Fluree, you query **graphs** and often load a **graph snapshot** (an immutable point-in-time view you can query repeatedly). In SPARQL, graph scoping uses `GRAPH <iri> { … }`, where `<iri>` is a **graph identifier** (a graph IRI). Fluree supports multiple kinds of **graph sources** (ledger graphs and non-ledger sources like BM25/vector indexes and tabular mappings). Users may refer to graphs with short, friendly **aliases** that Fluree resolves against a configured base into canonical **graph IRIs**.

Under the hood, this “graph snapshot” corresponds to the same semantic idea many temporal systems describe as **“database as a value”**: immutable, time-travelable, and safe to pass around.

---

## User-facing naming (recommended)

### Core terms
- **Ledger**: A durable data product with a commit chain, identified by a *ledger address* like `mydb:main`.
  - A ledger is what users create/manage.
  - A ledger can contain multiple graphs (default graph + named graphs).
- **Graph**: A query scope (SPARQL term).
  - In a query, a “graph” is identified by an IRI and used to scope patterns (`GRAPH <iri> { … }`).
- **Graph Snapshot**: An immutable point-in-time view of a graph that can be queried repeatedly.
- **Db (database value)**: The underlying semantic model: an immutable value at a point in time.
  - In product/docs we usually say “graph snapshot” because it aligns with SPARQL and the Rust API.
  - Internally you may still see `Db` used for the indexed snapshot type.
- **Graph IRI**: The canonical identity of a graph. This is what SPARQL uses.
- **Graph reference (GraphRef)**: What a user types (often an alias-like string), which Fluree resolves to a Graph IRI.
- **Graph Source**: Anything addressable by a Graph IRI that can participate in query execution.
- **Federation** (preferred) / **Dataset** (SPARQL term): A query executed over a set of graphs.
  - We prefer “federation” when describing the product feature to non-SPARQL users.

### “Graph snapshot” vs “Graph IRI” (how to talk about it)
- **Graph Snapshot (value)** answers: “What immutable point-in-time graph am I querying?”
- **Graph IRI (identifier)** answers: “Which graph does this part of the query run against?”

In practice, you query a graph snapshot by naming its graph:
- When you write `FROM <…>` or `GRAPH <…>`, you are naming a **graph IRI**.
- That graph IRI resolves to a **graph snapshot** (an immutable value) at execution time.

### Time pinning syntax (“the part after `@` pins the Db”)
Fluree supports time pinning in graph references.

**Current syntax (implemented today):**
- `<ledger>:<branch>@t:<t>` — pin to transaction time
- `<ledger>:<branch>@iso:<rfc3339>` — pin to ISO datetime
- `<ledger>:<branch>@sha:<commit-sha>` — pin to commit SHA (prefix allowed)

**Recommended syntax (preferred for new examples/docs):**
- `<ledger>:<branch>@t=<t>`
- `<ledger>:<branch>@iso=<rfc3339>`
- `<ledger>:<branch>@sha=<commit-sha>`

The `=` form avoids ambiguity and escaping headaches around `:` and is more URL/IRI-friendly.

From a user perspective:
- The `@…` portion selects **which Db value** you mean for that ledger graph.

### Named graphs within a ledger
We support multiple named graphs inside a single ledger (shared commit chain, distinct graph identities/indexes).

Recommended user-facing convention (alias-friendly, URL-friendly, avoids `/` as a delimiter inside the ledger namespace):

```
<ledger>:<branch>[ @time-spec ] [ #<named-graph-alias> ]
```

Examples:
- Default graph, latest: `<acme/people:main>`
- Default graph at \(t=1000\): `<acme/people:main@t=1000>`
- Txn metadata graph at latest: `<acme/people:main#txn-meta>`
- Txn metadata graph at \(t=1000\): `<acme/people:main@t=1000#txn-meta>`

#### Important note about `#` fragments
Using `#<named-graph-alias>` is idiomatic RDF identity, but **HTTP clients do not send fragments to servers**.
That’s fine for graph identity and query semantics, but if you want a dereferenceable HTTP endpoint for a named graph,
plan to expose a server-visible selector (e.g., `?graph=txn-meta`) in addition to the canonical identity.

### Full IRIs are always allowed
Semantic web users may prefer full IRIs:
- `https://data.flur.ee/acme/people:main@t=1000#txn-meta`

These should be used as-is (no resolution needed).

### Base resolution (“make aliases globally identifiable”)
Many users prefer short names like `people:main` or `acme/people:main`. To make them globally identifiable:
- Allow a configured base (SPARQL `BASE <…>` or a connection/query base configuration).
- Treat alias-style graph references as **relative IRI references** resolved against that base.

Example:
- Base: `https://data.flur.ee/`
- Ref: `<acme/people:main@t=1000#txn-meta>`
- Graph IRI: `https://data.flur.ee/acme/people:main@t=1000#txn-meta`

### Character and encoding rules (user-facing)
To avoid ambiguity and URL pitfalls:
- **Reserved delimiters**:
  - `@` separates the time specifier
  - `#` separates a named-graph alias (fragment)
  - `:` is used inside the ledger address as `ledger:branch`
- **Do not use raw `@` or `#` inside ledger names, branch names, or named-graph aliases**.
  - If needed, percent-encode them.
- RFC3339 / ISO timestamps must be URL-safe:
  - Prefer UTC with `Z` (e.g., `2026-02-03T17:02:11Z`).
  - If offsets are used (`+05:00`), they should be percent-encoded in IRI contexts.

---

## Graph Sources (user-facing)

We use **Graph Source** as the umbrella term for anything you can name in `FROM`, `FROM NAMED`, or `GRAPH <…>` and query as part of a single execution.

Graph sources differ in capabilities:
- Some behave like RDF triple stores (ledger graphs, some mappings).
- Some provide specialized operators/patterns (BM25 and vector search).

### Categories
- **Ledger Graph Sources**: RDF graphs stored in a ledger (default graph or named graph).
- **Index Graph Sources**: persisted indexes queried through graph-integrated patterns (BM25, Vector/HNSW).
- **Mapped Graph Sources**: non-ledger data mapped into an RDF-shaped graph (R2RML, Iceberg).

### Legacy term: “Virtual Graph”
Older docs and some internal APIs use **Virtual Graph (VG)** for “non-ledger graph sources”.
As we align naming, prefer “graph source” in user-facing docs and examples.

---

## Internal naming (Rust implementation guidance)

### Canonical identities: prefer explicit types, not raw `String`
Internally we should distinguish:
- **`LedgerAddress`**: the durable ledger identifier (e.g., `acme/people:main`)
- **`GraphIri`**: canonical graph identity used by SPARQL (`Arc<str>` or validated URL/IRI type)
- **`GraphRef`**: user input token, resolved to a `GraphIri` using base rules

Even if these are initially just `struct LedgerAddress(Arc<str>)` newtypes, they prevent accidental mixing and make APIs self-documenting.

### Naming rules (make each word mean one thing)
This repo currently mixes `alias`, `address`, and `ledger_alias` in ways that cause confusion. The recommended rule set:

- **`address`**: canonical identifier used as a cache key / stable identity.
  - For ledgers this is the full `name:branch` form (e.g., `people:main`).
  - For graph identities this is the resolved graph IRI (e.g., `https://data.flur.ee/acme/people:main@t=42#txn-meta`).
- **`name`**: a base name without branch (e.g., `people`).
- **`branch`**: the branch name (e.g., `main`).
- **`alias`**: a human-friendly label, and only that.
  - If you need “base name without branch”, prefer `name` (or `ledger_name`) rather than `alias`.

Practical guidelines:
- If a string is used to load/cache/lookup a ledger, call it **`ledger_address`** (not `ledger_alias`).
- If a string is used to identify a graph in SPARQL (`FROM`, `GRAPH`), call it **`graph_iri`** (canonical) or **`graph_ref`** (user input).
- Avoid having two different meanings for the same field name across crates. Prefer longer, explicit names.

### What `Db<S, C>` actually “holds” (today)
In this repo, `Db<S, C>` is a **time-anchored indexed snapshot** plus the handles needed to dereference it.
It does **not** “implement the cache”; it **holds** a cache handle (`Arc<C> where C: NodeCache`) and a storage handle (`S: Storage`).

As of the current design, `Db` may be backed by either:
- **B-tree index roots** (traditional range traversal), or
- an optional **range provider** (e.g., a binary columnar index provider) that can serve range queries without b-tree roots.

Concretely it contains:
- **Identity & time**
  - `alias` (ledger identifier; recommended internal rename: `ledger_address`)
  - `t` (index transaction time; i.e., `index_t` / “how far the indexed snapshot covers”)
  - `version`
- **Index roots** (e.g., SPOT/PSOT/POST/OPST/TSPO), optional
- **Optional `range_provider`**
  - when set, `range_with_overlay()` can delegate range resolution to the provider instead of walking b-tree nodes
- **Namespace table** (namespace code → IRI prefix) used for SID encoding/decoding
- **Optional metadata**
  - stats/config/schema (used for planning, reasoning, formatting, diagnostics)
  - cached schema hierarchy
- **Execution support**
  - storage handle (`S: Storage`)
  - cache handle (`Arc<C>: NodeCache`)
  - SID interner (memory/perf optimization)

#### Meta-only mode (when b-tree roots are absent)
Some construction paths create a “metadata-only” `Db` (e.g., `new_meta(...)`) where b-tree roots are `None`.
In that case:
- callers can still use the Db for namespaces/stats/schema/time identity, and
- actual scan/range execution must go through the configured range provider or other non-btree query operators.

#### Cache budget and ownership (global cache)
Fluree uses a **global cache budget** (e.g., “8GB”) that is intended to be shared across the system, not per ledger/Db.

Design rule:
- The cache service is owned/configured in **`fluree-db-core`** (so all consumers—query engine, indexer, policy, SHACL, etc.—inherit the same cache).
- Individual `Db` instances hold an `Arc<…>` clone of that shared cache handle so all clones/derived views (including prefetch tasks) warm the same cache.

What the `Db` holds vs does not hold:
- **Db holds**: a cache *handle* (shared) and uses it for node/leaf caching keyed by ledger/graph identity + node identifiers.
- **Db does not hold**: the cache *budget/configuration* as part of the snapshot value. That’s global process configuration.

Important: `Db<S,C>` is not “the whole mutable ledger.” It’s the indexed state at a point in time.
The mutable head is represented by ledger state types that combine:
- `Db` (indexed snapshot) + `Novelty` (overlay of committed-but-unindexed transactions)

### Ledger vs Db vs Graph (internal meaning)
- **Ledger** (internal): the durable identity + commit chain + publication state (nameservice record).
- **Db** (internal): the indexed snapshot value used for range/scan (hot path).
- **Graph** (internal): query scoping mechanism (active graph, dataset graph selection, `GRAPH` operator).

Avoid using “graph” as a synonym for “db” in code comments. Prefer:
- “ledger graph” (RDF graph inside a ledger)
- “graph IRI” (identifier)
- “graph source” (registry/resolution concept)

### “Dataset” naming internally
SPARQL calls the set-of-graphs a “dataset”, and the code already models a `DataSet`.
For product-facing APIs and docs, prefer “federation”, but internally:
- It is reasonable to keep `DataSet` as the SPARQL-aligned term.
- Add docstrings that explicitly say **“federated / multi-graph execution”**.

### Suggested internal renames (incremental, low risk)
These can be done gradually (aliases first, then struct renames if desired):
- `Db.alias` → `ledger_address` (or document it as such everywhere).
- `ledger_alias` fields used as graph identifiers → `graph_name` or `graph_ref_name` (depending on stage).
- Reserve **`alias`** for “human-friendly base name” only (like `NsRecord.alias`), and prefer **`address`** for canonical `name:branch` identifiers (like `NsRecord.address`).
- `LedgerSnapshot` (read-only view) → `LedgerReadView` (to reduce “snapshot” ambiguity vs other snapshots).
- Keep `GraphSnapshot` as the user-facing “loaded graph snapshot” type; disambiguate other “snapshot” types instead (Ledger read view, Iceberg snapshot, etc.).

### Ideal end state (recommended shape)
To keep responsibilities clear as storage/index backends evolve, the clean long-term direction is:

- **Snapshot metadata** (pure value):
  - identity (ledger address / graph identity)
  - time (`t`)
  - namespaces
  - stats/config/schema
  - index root pointers *or* provider configuration needed to resolve data

- **Execution services** (handles):
  - storage handle(s)
  - cache handle(s)
  - range provider / graph source resolver hooks

This can be represented either as:
- a split type (`DbMeta` + `DbServices`, held by a `Db` wrapper), or
- a single `Db` struct with a clearly-named nested field (e.g., `services: DbServices<S,C>`) to group runtime handles.

### Future-proof abstraction: Graph Source Registry
To avoid threading many optional providers through execution context:
- Introduce a `GraphSourceRegistry` (internal) that resolves a `GraphIri` to a concrete graph source:
  - ledger graph (default/named graph within a ledger)
  - BM25/vector index source
  - R2RML/Iceberg mapped source

This centralizes:
- “does this graph exist?”
- “what capabilities does it support?” (triple patterns vs search patterns vs mapped scans)
- time pinning rules (`@t`, `@iso`, `@sha`) per source type

---

## Conventions and examples

### SPARQL: base + pinned graphs
```sparql
BASE <https://data.flur.ee/>

SELECT ?s ?p ?o
FROM <acme/people:main@t=1000>
WHERE {
  ?s ?p ?o .
}
```

### SPARQL: txn metadata named graph
```sparql
BASE <https://data.flur.ee/>

SELECT ?commit ?t
FROM NAMED <acme/people:main@t=1000#txn-meta>
WHERE {
  GRAPH <acme/people:main@t=1000#txn-meta> {
    ?commit <https://ns.flur.ee/ledger#t> ?t .
  }
}
```

### JSON-LD Query: pinned graph reference
```json
{
  "from": "acme/people:main@t=1000",
  "select": ["?name"],
  "where": [{ "@id": "?p", "http://schema.org/name": "?name" }]
}
```

---

## Related docs
- `docs/concepts/time-travel.md` (time pinning syntax)
- `docs/concepts/datasets-and-named-graphs.md` (SPARQL dataset semantics)
- `docs/virtual-graphs/overview.md` and `docs/concepts/virtual-graphs.md` (virtual graph overview)
- `docs/reference/glossary.md` (terms glossary)

