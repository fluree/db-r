# Query-Connection + Named Graph IRIs (Handoff Spec)

This document is a concrete handoff for implementing **named graph querying by real graph IRIs** across Fluree’s:
- **Server HTTP API** (`query-connection`)
- **Rust “query-connection” API** (mirrors HTTP semantics)
- **Rust dataset API** (power-user / explicit datasets)

It also defines how to support **cross-ledger queries** without requiring users to “select a ledger first”, while keeping SPARQL syntax intuitive.

---

## Goals

- **Support named graph IRIs inside a ledger** (not just `#txn-meta`), while keeping the existing ledger-alias experience.
- Preserve current **query-connection** behavior: users can reference ledgers by alias and query across any ledgers present in the connection.
- Avoid ambiguous parsing (e.g., do **not** rely on `ledger#<full-IRI>` where the full IRI may itself contain `#`).
- Provide a **single, consistent model** across JSON-LD and SPARQL:
  - JSON-LD query: dataset selection can be arbitrarily expressive (maps/objects).
  - SPARQL query: use a local-only `SERVICE` form for ledger selection, plus standard `GRAPH <iri>` for graph selection.
- Allow same graph IRIs to exist in multiple ledgers by introducing **dataset-local aliases** (unique within the request).

---

## Terminology

- **Ledger alias**: A Fluree ledger identifier (e.g. `sales:main`).
- **Graph IRI**: A real RDF IRI used in SPARQL `GRAPH <...>`.
- **Graph selector**: How a request specifies which graph inside a ledger to query:
  - default graph
  - `txn-meta` (built-in)
  - arbitrary graph IRI (user graphs)
- **Dataset-local alias**: A short name (unique within the request) used to reference a selected graph source.

---

## Current behavior (baseline)

### JSON-LD query dataset selection (today)

In `fluree-db-api/src/dataset.rs`, dataset parsing already supports:
- `"from"`: string | array of strings | object(s) with `"@id"` + time keys
- `"from-named"`: string | array of strings | object(s) with `"@id"` + time keys
- `@t:` / `@iso:` / `@sha:` suffixes inside ledger references, and `#txn-meta` fragment syntax.

### Limitation (today)

The dataset spec treats `FROM` / `FROM NAMED` values as “graph sources” but they are effectively **ledger-scoped views** (plus the special `#txn-meta` fragment). There is no way to specify *“graph IRI X inside ledger Y”* without overloading the identifier string.

---

## Proposed: Dataset Source Object (JSON-LD query-connection)

Extend the `"from"` and `"from-named"` formats to accept objects with a unique `alias`, plus optional per-source options such as graph selection and policy.

### Source object schema

A **Graph Source Object** is a JSON object:

- **`@id`** *(required)*: the ledger reference string (e.g. `sales:main`, `inventory:main@t:5`)
  - The existing time-travel suffixes remain valid (`@t:` / `@iso:` / `@sha:`).
  - Existing `#txn-meta` shorthand remains valid for backwards compatibility, but is not required when using `graph`.
- **`alias`** *(optional but recommended)*: dataset-local alias used for referencing this source in the query
  - **Uniqueness rule**: if an alias appears more than once in the request (across both `"from"` and `"from-named"`), return an error.
- **`graph`** *(optional)*: graph selector inside the ledger
  - If omitted: selects the ledger’s **default graph**.
  - Allowed values:
    - `"default"` (explicit default graph)
    - `"txn-meta"` (built-in txn metadata graph)
    - `"<graph-iri>"` (a full graph IRI string, e.g. `"http://example.org/vocab#products"`)
- **`policy`** *(optional)*: policy options that apply only to this source
  - This enables “policy on the graph” (per-source) without inventing new global request shapes.
  - Exact content should reuse the existing policy JSON formats already accepted by `QueryConnectionOptions`.

### Accepted `"from"` / `"from-named"` forms (new + old)

- **Old** (still supported):
  - `"from": "ledger:main"`
  - `"from": ["ledger1:main", "ledger2:main"]`
  - `"from": {"@id": "ledger:main", "t": 42}`
  - `"from-named": ["ledger:main", "other:main"]`
  - `"from": "ledger:main@t:100#txn-meta"`

- **New**:
  - `"from": {"@id": "ledger:main", "alias": "a"}`
  - `"from": [{"@id": "ledger:main", "alias": "default"}, {"@id": "ledger:main", "alias": "meta", "graph": "txn-meta"}]`
  - `"from-named": [{"@id": "inventory:main", "alias": "products", "graph": "http://example.org/vocab#products"}]`

### Validation rules (JSON-LD dataset parsing)

- **Alias uniqueness**: duplicate `alias` values across the dataset spec is an error.
- **Graph selector coherence**:
  - If `@id` contains `#txn-meta` *and* `graph` is provided, reject as ambiguous (force a single spelling).
  - If `graph` is provided and is not `"default"` / `"txn-meta"`:
    - treat it as a **real graph IRI string**; do not attempt delimiter-based parsing.
- **Binary index requirement**:
  - Selecting `"txn-meta"` or a real graph IRI requires a binary index store (same requirement as current `#txn-meta` support).
- **Unknown graph**:
  - If `graph` is a real IRI and it is not present in the ledger’s graph registry/dictionary, return an error.

### Resolution rules (IRI → internal g_id)

Resolution occurs when building views from a dataset spec:

- `graph = "default"` → `g_id = 0`
- `graph = "txn-meta"` → `g_id = 1` (reserved)
- `graph = "<iri>"` → resolve `iri → g_id` **within that ledger’s graph registry/dictionary**

Implementation note:
- Do not use `ledger#<iri>` parsing for arbitrary IRIs (IRIs often contain `#`).
- Prefer a ledger-local dictionary mapping (e.g., graph dictionary) as the source of truth.

---

## Query semantics by scenario (JSON-LD)

### 1) Single-ledger, default graph (existing)

```json
{
  "from": "mydb:main",
  "select": ["?s"],
  "where": {"@id": "?s"}
}
```

### 2) Single-ledger, txn-meta (existing shorthand)

```json
{
  "from": "mydb:main#txn-meta",
  "select": ["?commit", "?t"],
  "where": [{"@id": "?commit", "https://ns.flur.ee/ledger#t": "?t"}]
}
```

### 3) Single-ledger, real graph IRI (new)

```json
{
  "from": {
    "@id": "mydb:main",
    "alias": "products",
    "graph": "http://example.org/vocab#products"
  },
  "select": ["?p", "?o"],
  "where": {"@id": "?s", "?p": "?o"}
}
```

### 4) Cross-ledger union default graphs (existing)

```json
{
  "from": ["sales:main", "inventory:main"],
  "select": ["?s"],
  "where": {"@id": "?s"}
}
```

### 5) Cross-ledger named graphs with collisions (new, alias disambiguation)

Both ledgers may contain a graph with the same graph IRI. Use distinct dataset-local aliases:

```json
{
  "from-named": [
    {
      "@id": "sales:main",
      "alias": "salesProducts",
      "graph": "http://example.org/vocab#products"
    },
    {
      "@id": "inventory:main",
      "alias": "inventoryProducts",
      "graph": "http://example.org/vocab#products"
    }
  ],
  "select": ["?g", "?sku"],
  "where": [
    ["graph", "?g", {"@id": "?sku", "http://example.org/vocab#type": "http://example.org/vocab#Product"}]
  ]
}
```

Notes:
- The dataset-local alias is how users can *choose* which named graph they mean (implementation detail depends on how `["graph", ...]` is represented internally; see “Implementation hooks” below).

---

## SPARQL: Local `SERVICE` + standard `GRAPH`

### Motivation

SPARQL `FROM` / `FROM NAMED` identify graphs in the dataset, but do not provide a standard concept of “ledger alias in this connection”.

To preserve cross-ledger query-connection semantics *in SPARQL text*, add support for a **local-only SERVICE endpoint** that selects a ledger.

### Ledger-bound SPARQL endpoints (important)

Fluree should support two SPARQL execution modes:

- **Ledger-bound SPARQL** (server endpoint is already scoped to a specific ledger, or Rust executes against a single `FlureeView` / ledger handle):
  - `FROM <graphIRI>` / `FROM NAMED <graphIRI>` / `GRAPH <graphIRI>` refer to **named graphs inside that one ledger**.
  - Multiple `FROM` clauses mean the **default graph is the union** of those named graphs (inside the ledger), matching standard SPARQL dataset semantics.
  - `SERVICE` is unnecessary in this mode and may be rejected to avoid confusion.

- **Connection-bound SPARQL** (`query-connection` endpoint; may span multiple ledgers):
  - Use **local `SERVICE <fluree:ledger:...>`** to select which ledger a block runs against.
  - Inside that block, use standard `GRAPH <graphIRI>` to select named graphs within the selected ledger.

### Supported SPARQL pattern (new)

Support:

```sparql
SERVICE <fluree:ledger:<ledger-ref>> { ... }
```

Where `<ledger-ref>` is the same ledger reference string users already use (including time-travel suffixes).

Examples:

#### 1) Single-ledger default graph (SERVICE-scoped)

```sparql
SELECT ?s ?p ?o
WHERE {
  SERVICE <fluree:ledger:mydb:main> {
    ?s ?p ?o .
  }
}
```

#### 2) Single-ledger named graph IRI

```sparql
SELECT ?sku ?price
WHERE {
  SERVICE <fluree:ledger:mydb:main> {
    GRAPH <http://example.org/vocab#products> {
      ?sku <http://example.org/vocab#price> ?price .
    }
  }
}
```

#### 3) txn-meta as a graph IRI

Txn-meta graph is selected per-ledger via the `#txn-meta` fragment on the ledger reference:
- `mydb:main#txn-meta`

```sparql
PREFIX f: <https://ns.flur.ee/ledger#>
SELECT ?commit ?t
WHERE {
  SERVICE <fluree:ledger:mydb:main> {
    GRAPH <mydb:main#txn-meta> {
      ?commit f:t ?t .
    }
  }
}
```

#### 4) Cross-ledger join

```sparql
PREFIX ex: <http://example.org/vocab#>
SELECT ?sku ?price ?stock
WHERE {
  SERVICE <fluree:ledger:sales:main> {
    ?sku ex:price ?price .
  }
  SERVICE <fluree:ledger:inventory:main> {
    ?sku ex:stock ?stock .
  }
}
```

### SPARQL validation & safety

- Only allow local `SERVICE` endpoints by default:
  - Accept `fluree:ledger:<...>` (or `urn:fluree:ledger:<...>`).
  - Reject `SERVICE <https://...>` unless explicitly enabled via server config (SSRF/perf).
- Within a SERVICE block:
  - `GRAPH <iri>` is resolved within that ledger (IRI → `g_id`).
- If a SERVICE ledger reference cannot be resolved from the connection/nameservice → error.

---

## Rust API story (handoff)

### 1) `query_connection` (Rust) mirrors server HTTP

Treat Rust `query_connection` as a client for the same semantics:
- JSON-LD query supports map/object `"from"` and `"from-named"` entries with `alias`, `graph`, `policy`, etc.
- SPARQL query supports local `SERVICE <fluree:ledger:...>` and graph IRIs via `GRAPH <...>`.

### 1b) Ledger-bound SPARQL (Rust and server ledger endpoints)

When a caller has already selected a single ledger/view (e.g. `fluree.view("mydb:main")` or an HTTP endpoint scoped to `mydb:main`):

- Support standard SPARQL dataset clauses over **graphs within that ledger**:
  - `FROM <graphIRI>` / `FROM NAMED <graphIRI>`
  - `GRAPH <graphIRI> { ... }`
- This is the recommended way to use `FROM` / `FROM NAMED` to select **named graphs within one ledger**.

### 2) Dataset API remains the explicit “power-user” option

Rust users who want full control can build a `FlureeDataSetView` directly:
- Multiple default graphs (union semantics)
- Named graphs bound to explicit views
- Custom collision rules and advanced composition

This API is also the place to add ergonomic helpers such as:
- `view_in_graph(ledger_ref, GraphSelector::TxnMeta | GraphSelector::Iri("..."))`
- `view_in_graph_at(ledger_ref, graph_selector, time_spec)`

---

## Implementation hooks (what to change)

### A) Dataset parsing (`fluree-db-api/src/dataset.rs`)

Extend graph source parsing to accept:
- `alias` (string)
- `graph` selector
- `policy` subobject (optional)

This likely means:
- Extend `GraphSource` to include:
  - `source_alias: Option<String>` (dataset-local alias)
  - `graph_selector: Option<GraphSelector>` (default/txn-meta/iri)
  - `policy_override: Option<PolicySpec>` (or reuse `QueryConnectionOptions`-like subset)
- Keep the existing `identifier + time_spec` parsing for backward compatibility.

### B) Dataset view building (where graph IDs get selected)

When resolving each `GraphSource` to a `FlureeView`:
- Load the base ledger view at the requested time spec (existing).
- If `graph_selector` is present:
  - resolve to `g_id` and apply `select_graph_id(view, g_id)` (existing mechanism).
- If `policy_override` is present:
  - wrap only that view with the specified policy (needs an API-layer decision on how per-view policy composes with global opts).

### C) Graph IRI registry / dictionary (ledger-local)

To resolve `graph IRI → g_id`, use a ledger-local registry/dictionary that is persisted with the index.

Requirements:
- Must be accessible during query execution and view building.
- Must be time-safe (do not silently resolve graph IRIs from an unrelated/“latest” snapshot when time travel is requested).

### D) SPARQL `SERVICE` support

Add a Fluree-local SERVICE resolver:
- Parse `SERVICE <fluree:ledger:...>` and resolve the ledger reference to a view.
- Execute the SERVICE group pattern against that view in-process (no HTTP).
- Join bindings with outer query per SPARQL algebra semantics.

---

## Error behavior (required)

- Duplicate dataset-local alias: **error**
- Unknown ledger reference: **error**
- Named graph selection requested but binary index store is absent: **error**
- Unknown graph IRI within a ledger: **error**
- `SERVICE` endpoint not recognized / not allowed: **error** (or empty solutions for `SERVICE SILENT`)

---

## Backward compatibility

All existing forms remain valid:
- `"from": "ledger:main"`
- `"from-named": ["ledger1:main", "ledger2:main"]`
- time travel suffixes in strings
- `#txn-meta` shorthand in strings

New object forms are additive.

