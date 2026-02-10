# Fluree System Vocabulary Reference

All Fluree system vocabulary lives under a single canonical namespace:

```
https://ns.flur.ee/db#
```

Users declare a prefix in their JSON-LD `@context` to use compact forms:

```json
{ "@context": { "f": "https://ns.flur.ee/db#" } }
```

Any prefix works (`f:`, `db:`, `fluree:`, etc.) as long as it expands to the canonical IRI. Internally, Fluree always compares on fully expanded IRIs.

The `@vector` shorthand is the one exception: it is a JSON-LD convenience alias that resolves to `f:embeddingVector` without requiring a prefix declaration.

> **Source of truth**: All constants are defined in the `fluree-vocab` crate. This document is the user-facing reference.

---

## Commit metadata predicates

These predicates appear on commit subjects in the txn-meta graph. Each commit produces 7-10 flakes describing the commit.

| Predicate | Full IRI | Datatype | Description |
|-----------|----------|----------|-------------|
| `f:address` | `https://ns.flur.ee/db#address` | `xsd:string` | Commit ContentId (CID string) |
| `f:alias` | `https://ns.flur.ee/db#alias` | `xsd:string` | Ledger alias (e.g. `mydb:main`) |
| `f:v` | `https://ns.flur.ee/db#v` | `xsd:int` | Commit format version |
| `f:time` | `https://ns.flur.ee/db#time` | `xsd:long` | Commit timestamp (epoch milliseconds) |
| `f:t` | `https://ns.flur.ee/db#t` | `xsd:int` | Transaction number (watermark) |
| `f:size` | `https://ns.flur.ee/db#size` | `xsd:long` | Cumulative data size in bytes |
| `f:flakes` | `https://ns.flur.ee/db#flakes` | `xsd:long` | Cumulative flake count |
| `f:previous` | `https://ns.flur.ee/db#previous` | `@id` (ref) | Reference to previous commit (optional) |
| `f:author` | `https://ns.flur.ee/db#author` | `xsd:string` | Transaction signer DID (optional) |
| `f:txn` | `https://ns.flur.ee/db#txn` | `xsd:string` | Transaction ContentId (CID string, optional) |
| `f:message` | `https://ns.flur.ee/db#message` | `xsd:string` | Commit message (optional) |
| `f:asserts` | `https://ns.flur.ee/db#asserts` | `xsd:long` | Assertion count in this commit |
| `f:retracts` | `https://ns.flur.ee/db#retracts` | `xsd:long` | Retraction count in this commit |

### Querying commit metadata

Commit metadata lives in the `#txn-meta` named graph within each ledger. To query it:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "select": ["?t", "?time", "?author"],
  "where": {
    "@graph": "mydb:main#txn-meta",
    "f:t": "?t",
    "f:time": "?time",
    "f:author": "?author"
  }
}
```

### Commit subject identifiers

Commit subjects use the scheme `fluree:commit:<content-id>` (e.g. `fluree:commit:bafybeig...`). This is a subject identifier scheme, not part of the `db#` predicate vocabulary.

---

## Datalog rules

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `f:rule` | `https://ns.flur.ee/db#rule` | Datalog rule definition predicate |

---

## Vector datatype

| Term | IRI | Description |
|------|-----|-------------|
| `f:embeddingVector` | `https://ns.flur.ee/db#embeddingVector` | f32-precision embedding vector datatype |
| `@vector` | (shorthand) | JSON-LD alias that resolves to `f:embeddingVector` |

Example usage in a transaction:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#", "ex": "http://example.org/" },
  "insert": {
    "@id": "ex:doc1",
    "ex:embedding": { "@value": [0.1, 0.2, 0.3], "@type": "f:embeddingVector" }
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

These predicates are used in WHERE clause patterns for BM25 and vector search. Users write compact forms like `"f:searchText"` (with `"f"` in their `@context`) or full IRIs.

### BM25 search

| Predicate | Full IRI | Required | Description |
|-----------|----------|----------|-------------|
| `f:graphSource` | `https://ns.flur.ee/db#graphSource` | Yes | Graph source ID (`name:branch`, e.g. `"my-search:main"`) |
| `f:searchText` | `https://ns.flur.ee/db#searchText` | Yes | Search query text (string or variable) |
| `f:searchResult` | `https://ns.flur.ee/db#searchResult` | Yes | Result binding (variable or nested object) |
| `f:searchLimit` | `https://ns.flur.ee/db#searchLimit` | No | Maximum results |
| `f:syncBeforeQuery` | `https://ns.flur.ee/db#syncBeforeQuery` | No | Wait for index sync before querying (boolean) |
| `f:timeoutMs` | `https://ns.flur.ee/db#timeoutMs` | No | Query timeout in milliseconds |

### Vector search

| Predicate | Full IRI | Required | Description |
|-----------|----------|----------|-------------|
| `f:graphSource` | `https://ns.flur.ee/db#graphSource` | Yes | Graph source ID (`name:branch`) |
| `f:queryVector` | `https://ns.flur.ee/db#queryVector` | Yes | Query vector (array of numbers or variable) |
| `f:searchResult` | `https://ns.flur.ee/db#searchResult` | Yes | Result binding |
| `f:distanceMetric` | `https://ns.flur.ee/db#distanceMetric` | No | Distance metric: `"cosine"`, `"dot"`, `"euclidean"` (default: `"cosine"`) |
| `f:searchLimit` | `https://ns.flur.ee/db#searchLimit` | No | Maximum results |
| `f:syncBeforeQuery` | `https://ns.flur.ee/db#syncBeforeQuery` | No | Wait for index sync (boolean) |
| `f:timeoutMs` | `https://ns.flur.ee/db#timeoutMs` | No | Query timeout in milliseconds |

### Nested result objects

Both BM25 and vector search support nested result bindings:

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `f:resultId` | `https://ns.flur.ee/db#resultId` | Document/subject ID binding |
| `f:resultScore` | `https://ns.flur.ee/db#resultScore` | Search score binding |
| `f:resultLedger` | `https://ns.flur.ee/db#resultLedger` | Source ledger ID (multi-ledger disambiguation) |

Example BM25 search with nested result:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "select": ["?doc", "?score"],
  "where": {
    "f:graphSource": "my-search:main",
    "f:searchText": "software engineer",
    "f:searchLimit": 10,
    "f:searchResult": {
      "f:resultId": "?doc",
      "f:resultScore": "?score"
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
| `f:ledger` | `https://ns.flur.ee/db#ledger` | Ledger name/identifier |
| `f:branch` | `https://ns.flur.ee/db#branch` | Branch name (e.g. `main`) |
| `f:t` | `https://ns.flur.ee/db#t` | Current transaction watermark |
| `f:ledgerCommit` | `https://ns.flur.ee/db#ledgerCommit` | Pointer to latest commit ContentId |
| `f:ledgerIndex` | `https://ns.flur.ee/db#ledgerIndex` | Pointer to latest index root |
| `f:status` | `https://ns.flur.ee/db#status` | Record status (`ready`, etc.) |
| `f:defaultContext` | `https://ns.flur.ee/db#defaultContext` | Default JSON-LD context ContentId |

### Graph source record fields

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `f:name` | `https://ns.flur.ee/db#name` | Graph source base name |
| `f:branch` | `https://ns.flur.ee/db#branch` | Branch |
| `f:status` | `https://ns.flur.ee/db#status` | Status |
| `f:graphSourceConfig` | `https://ns.flur.ee/db#graphSourceConfig` | Configuration JSON |
| `f:graphSourceDependencies` | `https://ns.flur.ee/db#graphSourceDependencies` | Dependent ledger IDs |
| `f:graphSourceIndex` | `https://ns.flur.ee/db#graphSourceIndex` | Index ContentId reference |
| `f:graphSourceIndexT` | `https://ns.flur.ee/db#graphSourceIndexT` | Index watermark (commit t) |
| `f:graphSourceIndexAddress` | `https://ns.flur.ee/db#graphSourceIndexAddress` | Index ContentId (string form) |

### Record type taxonomy

Nameservice records use `@type` to classify what kind of graph source a record represents.

**Required kind types** (exactly one per record):

| Type | Full IRI | Description |
|------|----------|-------------|
| `f:LedgerSource` | `https://ns.flur.ee/db#LedgerSource` | Ledger-backed knowledge graph |
| `f:IndexSource` | `https://ns.flur.ee/db#IndexSource` | Index-backed graph source (BM25/HNSW/GEO) |
| `f:MappedSource` | `https://ns.flur.ee/db#MappedSource` | Mapped database (Iceberg, R2RML) |

**Optional subtype `@type` values** (further classify the record):

| Type | Full IRI | Description |
|------|----------|-------------|
| `f:Bm25Index` | `https://ns.flur.ee/db#Bm25Index` | BM25 full-text search index |
| `f:HnswIndex` | `https://ns.flur.ee/db#HnswIndex` | HNSW vector similarity search index |
| `f:GeoIndex` | `https://ns.flur.ee/db#GeoIndex` | Geospatial index |
| `f:IcebergMapping` | `https://ns.flur.ee/db#IcebergMapping` | Iceberg-mapped database |
| `f:R2rmlMapping` | `https://ns.flur.ee/db#R2rmlMapping` | R2RML relational mapping |

---

## Policy vocabulary

These predicates are used to define access control policies.

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `f:policyClass` | `https://ns.flur.ee/db#policyClass` | Marks a class as policy-governed |
| `f:allow` | `https://ns.flur.ee/db#allow` | Allow/deny flag on a policy rule |
| `f:action` | `https://ns.flur.ee/db#action` | Action this rule governs (view or modify) |
| `f:view` | `https://ns.flur.ee/db#view` | View action IRI |
| `f:modify` | `https://ns.flur.ee/db#modify` | Modify action IRI |
| `f:onProperty` | `https://ns.flur.ee/db#onProperty` | Property-level policy targeting |
| `f:onSubject` | `https://ns.flur.ee/db#onSubject` | Subject-level policy targeting |
| `f:onClass` | `https://ns.flur.ee/db#onClass` | Class-level policy targeting |
| `f:query` | `https://ns.flur.ee/db#query` | Policy query (determines applicability) |
| `f:required` | `https://ns.flur.ee/db#required` | Whether the policy is required (boolean) |
| `f:exMessage` | `https://ns.flur.ee/db#exMessage` | Error message when policy denies access |

See [Policy model and inputs](../security/policy-model.md) for usage details.

---

## RDF-Star annotation predicates

Fluree supports RDF-Star annotations for transaction metadata. These predicates can appear in annotation triples:

| Predicate | Full IRI | Description |
|-----------|----------|-------------|
| `f:t` | `https://ns.flur.ee/db#t` | Transaction number on an annotated triple |
| `f:op` | `https://ns.flur.ee/db#op` | Operation type (assert/retract) |

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
| 9 | Fluree Commit | `fluree:commit:` |
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
