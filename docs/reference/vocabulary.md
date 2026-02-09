# Fluree System Vocabulary Reference

All Fluree system vocabulary lives under a single canonical namespace:

```
https://ns.flur.ee/db#
```

Users declare a prefix in their JSON-LD `@context` to use compact forms:

```json
{ "@context": { "db": "https://ns.flur.ee/db#" } }
```

Any prefix works (`db:`, `f:`, `fluree:`, etc.) as long as it expands to the canonical IRI. Internally, Fluree always compares on fully expanded IRIs.

The `@vector` shorthand is the one exception: it is a JSON-LD convenience alias that resolves to `db:embeddingVector` without requiring a prefix declaration.

> **Source of truth**: All constants are defined in the [`fluree-vocab`](../../fluree-vocab/src/lib.rs) crate. This document is the user-facing reference.

---

## Commit metadata predicates

These predicates appear on commit subjects in the txn-meta graph. Each commit produces 7-10 flakes describing the commit.

| Predicate | Full IRI | Datatype | Description |
|-----------|----------|----------|-------------|
| `db:address` | `https://ns.flur.ee/db#address` | `xsd:string` | CAS storage address of the commit |
| `db:alias` | `https://ns.flur.ee/db#alias` | `xsd:string` | Ledger alias (e.g. `mydb:main`) |
| `db:v` | `https://ns.flur.ee/db#v` | `xsd:int` | Commit format version |
| `db:time` | `https://ns.flur.ee/db#time` | `xsd:long` | Commit timestamp (epoch milliseconds) |
| `db:t` | `https://ns.flur.ee/db#t` | `xsd:int` | Transaction number (watermark) |
| `db:size` | `https://ns.flur.ee/db#size` | `xsd:long` | Cumulative data size in bytes |
| `db:flakes` | `https://ns.flur.ee/db#flakes` | `xsd:long` | Cumulative flake count |
| `db:previous` | `https://ns.flur.ee/db#previous` | `@id` (ref) | Reference to previous commit (optional) |
| `db:author` | `https://ns.flur.ee/db#author` | `xsd:string` | Transaction signer DID (optional) |
| `db:txn` | `https://ns.flur.ee/db#txn` | `xsd:string` | Transaction storage address (optional) |
| `db:message` | `https://ns.flur.ee/db#message` | `xsd:string` | Commit message (optional) |
| `db:asserts` | `https://ns.flur.ee/db#asserts` | `xsd:long` | Assertion count in this commit |
| `db:retracts` | `https://ns.flur.ee/db#retracts` | `xsd:long` | Retraction count in this commit |

### Querying commit metadata

Commit metadata lives in the `#txn-meta` named graph within each ledger. To query it:

```json
{
  "@context": { "db": "https://ns.flur.ee/db#" },
  "select": ["?t", "?time", "?author"],
  "where": {
    "@graph": "mydb:main#txn-meta",
    "db:t": "?t",
    "db:time": "?time",
    "db:author": "?author"
  }
}
```

### Commit subject identifiers

Commit subjects use the scheme `fluree:commit:sha256:<hex>`. This is a subject identifier scheme, not part of the `db#` predicate vocabulary.

---

## Datalog rules

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:rule` | `https://ns.flur.ee/db#rule` | Datalog rule definition predicate |

---

## Vector datatype

| Term | IRI | Description |
|------|-----|-------------|
| `db:embeddingVector` | `https://ns.flur.ee/db#embeddingVector` | f32-precision embedding vector datatype |
| `@vector` | (shorthand) | JSON-LD alias that resolves to `db:embeddingVector` |

Example usage in a transaction:

```json
{
  "@context": { "db": "https://ns.flur.ee/db#", "ex": "http://example.org/" },
  "insert": {
    "@id": "ex:doc1",
    "ex:embedding": { "@value": [0.1, 0.2, 0.3], "@type": "db:embeddingVector" }
  }
}
```

Or with the `@vector` shorthand:

```json
{
  "insert": {
    "@id": "ex:doc1",
    "ex:embedding": { "@value": [0.1, 0.2, 0.3], "@type": "@vector" }
  }
}
```

---

## Search query vocabulary

These predicates are used in WHERE clause patterns for BM25 and vector search. Users write compact forms like `"db:searchText"` (with `"db"` in their `@context`) or full IRIs.

### BM25 search

| Predicate | Full IRI | Required | Description |
|-----------|----------|----------|-------------|
| `db:graphSource` | `https://ns.flur.ee/db#graphSource` | Yes | Graph source alias (e.g. `"my-search:main"`) |
| `db:searchText` | `https://ns.flur.ee/db#searchText` | Yes | Search query text (string or variable) |
| `db:searchResult` | `https://ns.flur.ee/db#searchResult` | Yes | Result binding (variable or nested object) |
| `db:searchLimit` | `https://ns.flur.ee/db#searchLimit` | No | Maximum results |
| `db:syncBeforeQuery` | `https://ns.flur.ee/db#syncBeforeQuery` | No | Wait for index sync before querying (boolean) |
| `db:timeoutMs` | `https://ns.flur.ee/db#timeoutMs` | No | Query timeout in milliseconds |

### Vector search

| Predicate | Full IRI | Required | Description |
|-----------|----------|----------|-------------|
| `db:graphSource` | `https://ns.flur.ee/db#graphSource` | Yes | Graph source alias |
| `db:queryVector` | `https://ns.flur.ee/db#queryVector` | Yes | Query vector (array of numbers or variable) |
| `db:searchResult` | `https://ns.flur.ee/db#searchResult` | Yes | Result binding |
| `db:distanceMetric` | `https://ns.flur.ee/db#distanceMetric` | No | Distance metric: `"cosine"`, `"dot"`, `"euclidean"` (default: `"cosine"`) |
| `db:searchLimit` | `https://ns.flur.ee/db#searchLimit` | No | Maximum results |
| `db:syncBeforeQuery` | `https://ns.flur.ee/db#syncBeforeQuery` | No | Wait for index sync (boolean) |
| `db:timeoutMs` | `https://ns.flur.ee/db#timeoutMs` | No | Query timeout in milliseconds |

### Nested result objects

Both BM25 and vector search support nested result bindings:

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:resultId` | `https://ns.flur.ee/db#resultId` | Document/subject ID binding |
| `db:resultScore` | `https://ns.flur.ee/db#resultScore` | Search score binding |
| `db:resultLedger` | `https://ns.flur.ee/db#resultLedger` | Source ledger alias (multi-ledger disambiguation) |

Example BM25 search with nested result:

```json
{
  "@context": { "db": "https://ns.flur.ee/db#" },
  "select": ["?doc", "?score"],
  "where": {
    "db:graphSource": "my-search:main",
    "db:searchText": "software engineer",
    "db:searchLimit": 10,
    "db:searchResult": {
      "db:resultId": "?doc",
      "db:resultScore": "?score"
    }
  }
}
```

---

## Nameservice record vocabulary

### Ledger record fields

These predicates appear on ledger nameservice records (the metadata Fluree stores about each ledger).

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:ledger` | `https://ns.flur.ee/db#ledger` | Ledger name/identifier |
| `db:branch` | `https://ns.flur.ee/db#branch` | Branch name (e.g. `main`) |
| `db:t` | `https://ns.flur.ee/db#t` | Current transaction watermark |
| `db:ledgerCommit` | `https://ns.flur.ee/db#ledgerCommit` | Pointer to latest commit address |
| `db:ledgerIndex` | `https://ns.flur.ee/db#ledgerIndex` | Pointer to latest index root |
| `db:status` | `https://ns.flur.ee/db#status` | Record status (`ready`, etc.) |
| `db:defaultContext` | `https://ns.flur.ee/db#defaultContext` | Default JSON-LD context CAS address |

### Graph source record fields

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:name` | `https://ns.flur.ee/db#name` | Graph source base name |
| `db:branch` | `https://ns.flur.ee/db#branch` | Branch |
| `db:status` | `https://ns.flur.ee/db#status` | Status |
| `db:graphSourceConfig` | `https://ns.flur.ee/db#graphSourceConfig` | Configuration JSON |
| `db:graphSourceDependencies` | `https://ns.flur.ee/db#graphSourceDependencies` | Dependent ledger aliases |
| `db:graphSourceIndex` | `https://ns.flur.ee/db#graphSourceIndex` | Index address reference |
| `db:graphSourceIndexT` | `https://ns.flur.ee/db#graphSourceIndexT` | Index watermark (commit t) |
| `db:graphSourceIndexAddress` | `https://ns.flur.ee/db#graphSourceIndexAddress` | Index address (string) |

### Record type taxonomy

Nameservice records use `@type` to classify what kind of graph source a record represents.

**Required kind types** (exactly one per record):

| Type | Full IRI | Description |
|------|----------|-------------|
| `db:LedgerSource` | `https://ns.flur.ee/db#LedgerSource` | Ledger-backed knowledge graph |
| `db:IndexSource` | `https://ns.flur.ee/db#IndexSource` | Index-backed graph source (BM25/HNSW/GEO) |
| `db:MappedSource` | `https://ns.flur.ee/db#MappedSource` | Mapped database (Iceberg, R2RML) |

**Optional subtype `@type` values** (further classify the record):

| Type | Full IRI | Description |
|------|----------|-------------|
| `db:Bm25Index` | `https://ns.flur.ee/db#Bm25Index` | BM25 full-text search index |
| `db:HnswIndex` | `https://ns.flur.ee/db#HnswIndex` | HNSW vector similarity search index |
| `db:GeoIndex` | `https://ns.flur.ee/db#GeoIndex` | Geospatial index |
| `db:IcebergMapping` | `https://ns.flur.ee/db#IcebergMapping` | Iceberg-mapped database |
| `db:R2rmlMapping` | `https://ns.flur.ee/db#R2rmlMapping` | R2RML relational mapping |

---

## Policy vocabulary

These predicates are used to define access control policies.

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:policyClass` | `https://ns.flur.ee/db#policyClass` | Marks a class as policy-governed |
| `db:allow` | `https://ns.flur.ee/db#allow` | Allow/deny flag on a policy rule |
| `db:action` | `https://ns.flur.ee/db#action` | Action this rule governs (view or modify) |
| `db:view` | `https://ns.flur.ee/db#view` | View action IRI |
| `db:modify` | `https://ns.flur.ee/db#modify` | Modify action IRI |
| `db:onProperty` | `https://ns.flur.ee/db#onProperty` | Property-level policy targeting |
| `db:onSubject` | `https://ns.flur.ee/db#onSubject` | Subject-level policy targeting |
| `db:onClass` | `https://ns.flur.ee/db#onClass` | Class-level policy targeting |
| `db:query` | `https://ns.flur.ee/db#query` | Policy query (determines applicability) |
| `db:required` | `https://ns.flur.ee/db#required` | Whether the policy is required (boolean) |
| `db:exMessage` | `https://ns.flur.ee/db#exMessage` | Error message when policy denies access |

See [Policy model and inputs](../security/policy-model.md) for usage details.

---

## RDF-Star annotation predicates

Fluree supports RDF-Star annotations for transaction metadata. These predicates can appear in annotation triples:

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `db:t` | `https://ns.flur.ee/db#t` | Transaction number on an annotated triple |
| `db:op` | `https://ns.flur.ee/db#op` | Operation type (assert/retract) |

---

## Namespace codes (internal)

Fluree encodes namespace IRIs as integer codes for compact storage. These are internal implementation details but useful for contributors working on the core.

| Code | Namespace | IRI |
|------|-----------|-----|
| 0 | (empty) | `""` |
| 1 | JSON-LD | `@` |
| 2 | XSD | `http://www.w3.org/2001/XMLSchema#` |
| 3 | RDF | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` |
| 4 | RDFS | `http://www.w3.org/2000/01/rdf-schema#` |
| 5 | SHACL | `http://www.w3.org/ns/shacl#` |
| 6 | OWL | `http://www.w3.org/2002/07/owl#` |
| 7 | Fluree DB | `https://ns.flur.ee/db#` |
| 8 | DID Key | `did:key:` |
| 9 | Fluree Commit | `fluree:commit:sha256:` |
| 10 | Blank Node | `_:` |
| 11 | OGC GeoSPARQL | `http://www.opengis.net/ont/geosparql#` |
| 100+ | User-defined | (allocated at first use) |

---

## Standard W3C namespaces

Fluree also recognizes these standard W3C namespaces:

| Prefix | IRI | Common predicates |
|--------|-----|-------------------|
| `rdf:` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` | `rdf:type`, `rdf:first`, `rdf:rest` |
| `rdfs:` | `http://www.w3.org/2000/01/rdf-schema#` | `rdfs:label`, `rdfs:subClassOf`, `rdfs:range` |
| `xsd:` | `http://www.w3.org/2001/XMLSchema#` | `xsd:string`, `xsd:int`, `xsd:dateTime` |
| `owl:` | `http://www.w3.org/2002/07/owl#` | `owl:sameAs`, `owl:inverseOf` |
| `sh:` | `http://www.w3.org/ns/shacl#` | `sh:path`, `sh:datatype`, `sh:minCount` |

See [IRIs, namespaces, and JSON-LD @context](../concepts/iri-and-context.md) for details on prefix declarations and IRI resolution.
