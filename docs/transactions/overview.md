# Transaction Overview

This document provides a comprehensive overview of how transactions work in Fluree, from submission to final indexing.

## What is a Transaction?

A **transaction** in Fluree is a set of changes to the database, represented as RDF triple assertions and retractions. Each transaction is:

- **Atomic**: All changes succeed or all fail
- **Immutable**: Once committed, never modified
- **Timestamped**: Assigned a unique transaction time (t)
- **Auditable**: Complete metadata preserved

## Transaction Lifecycle

### 1. Submission

Client submits transaction to Fluree using either JSON-LD or SPARQL UPDATE:

**JSON-LD Transaction:**
```bash
POST /transact?ledger=mydb:main
Content-Type: application/json

{
  "@context": { "ex": "http://example.org/ns/" },
  "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
}
```

**SPARQL UPDATE:**
```bash
POST /ledger/mydb:main/transact
Content-Type: application/sparql-update

PREFIX ex: <http://example.org/ns/>
INSERT DATA { ex:alice ex:name "Alice" }
```

### 2. Parsing

Fluree parses the transaction:
- Parse JSON/JSON-LD structure
- Expand compact IRIs using @context
- Convert to internal representation

### 3. Validation

Transaction is validated:
- **Syntax validation**: Well-formed IRIs, valid datatypes
- **Semantic validation**: Type compatibility, constraints
- **Policy validation**: Authorization checks

If validation fails, transaction is rejected with error details.

### 4. Conversion to Flakes

Transaction is converted to **flakes** (Fluree's internal triple format):

```text
Subject    Predicate           Object                    Operation
------------------------------------------------------------------------
ex:alice   rdf:type           schema:Person             assert
ex:alice   schema:name        "Alice"^^xsd:string       assert
```

Each flake is a tuple: (subject, predicate, object, transaction-time, operation, metadata)

### 5. Assignment of Transaction Time

Fluree assigns a unique transaction time (t):
- Monotonically increasing integer
- Unique across all transactions
- Used for temporal queries

Example: `t=42`

### 6. Commit

Transaction is committed to storage:
- Flakes written to transaction log
- Commit metadata created (SHA, timestamp, etc.)
- Commit address published to nameservice

**Commit Data:**
```json
{
  "t": 42,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456...",
  "address": "fluree:memory:commit:abc123...",
  "flakes_added": 2,
  "flakes_retracted": 0
}
```

### 7. Nameservice Update

Nameservice is updated with new commit:
- `commit_t` updated to 42
- `commit_address` updated
- Other processes can see new commit

### 8. Indexing (Asynchronous)

Background process indexes the transaction:
- Flakes added to index structures (SPOT, POST, OPST, PSOT)
- Query-optimized data structures built
- Virtual graphs updated (if applicable)

### 9. Index Publication

When indexing completes:
- `index_t` updated to 42
- `index_address` published
- Novelty layer reduced

## Transaction Components

### @context

Defines namespace mappings:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  }
}
```

The @context can be:
- Inline (as above)
- External URL: `"@context": "http://example.org/context.jsonld"`
- Array of contexts: `"@context": [url1, {...}]`

### @graph

Contains the entities being asserted:

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice"
    },
    {
      "@id": "ex:bob",
      "@type": "schema:Person",
      "schema:name": "Bob"
    }
  ]
}
```

### WHERE/DELETE/INSERT

For updates, specify what to match, delete, and insert:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

### SPARQL UPDATE

Alternatively, use SPARQL UPDATE syntax with `Content-Type: application/sparql-update`:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:age ?oldAge .
}
INSERT {
  ?person ex:age 31 .
}
WHERE {
  ?person ex:name "Alice" .
  ?person ex:age ?oldAge .
}
```

SPARQL UPDATE supports:
- `INSERT DATA` - Insert ground triples
- `DELETE DATA` - Delete specific triples
- `DELETE WHERE` - Delete matching patterns
- `DELETE/INSERT WHERE` - Full update with patterns

See [SPARQL UPDATE](../query/sparql.md#sparql-update) for complete documentation.

## Transaction Modes

### Default Mode

```bash
POST /transact?ledger=mydb:main
```

- Additive by default
- Explicit deletes required for updates
- Flexible for partial updates

### Replace Mode

```bash
POST /transact?ledger=mydb:main&mode=replace
```

- Replaces all properties of entities
- Idempotent
- Good for synchronization

See [Upsert](upsert.md) for details.

## Transaction Semantics

### Assertions

**Assertions** add new triples to the database:

```json
{
  "@id": "ex:alice",
  "schema:name": "Alice"
}
```

Creates triple:
```
ex:alice schema:name "Alice"
```

### Retractions

**Retractions** remove existing triples:

```json
{
  "delete": [
    { "@id": "ex:alice", "schema:age": "?age" }
  ],
  "where": [
    { "@id": "ex:alice", "schema:age": "?age" }
  ]
}
```

Removes matching triples.

### Updates

Updates are retraction + assertion:

```text
t=10: ex:alice schema:age 30 (assert)
t=20: ex:alice schema:age 30 (retract), ex:alice schema:age 31 (assert)
```

Historical queries can see both states.

## Commit Metadata

Each commit includes rich metadata:

**Core Fields:**
- `t`: Transaction time
- `timestamp`: ISO 8601 timestamp
- `commit_sha`: Cryptographic hash
- `address`: Storage address

**Counts:**
- `flakes_added`: Number of assertions
- `flakes_retracted`: Number of retractions

**Provenance:**
- `author`: DID of transaction author (if signed)
- `message`: Optional commit message
- `previous_commit`: SHA of previous commit

See [Commit Receipts](commit-receipts.md) for details.

## Indexing Pipeline

### Commit vs Index

**Commit (immediate):**
- Transaction written to log
- Available for time travel queries
- Small, append-only files

**Index (asynchronous):**
- Query-optimized data structures
- Background process
- May lag behind commits

### Novelty Layer

The **novelty layer** is uncommitted data between index and commit:

```text
index_t = 40
commit_t = 45
novelty layer = transactions 41, 42, 43, 44, 45
```

Queries combine:
- Indexed data (up to t=40)
- Novelty layer (t=41 to t=45)

### Index Structures

Fluree maintains four index permutations (SPOT, POST, OPST, PSOT):

**SPOT** (Subject-Predicate-Object-Time):
```
ex:alice → schema:name → "Alice" → t=10
```

**POST** (Predicate-Object-Subject-Time):
```
schema:name → "Alice" → ex:alice → t=10
```

**OPST** (Object-Predicate-Subject-Time):
```
"Alice" → schema:name → ex:alice → t=10
```

**PSOT** (Predicate-Subject-Object-Time):
```
schema:name → ex:alice → "Alice" → t=10
```

Different query patterns use different indexes for optimal performance.

## Transaction Properties

### Atomicity

All-or-nothing execution:
- Validation failure rejects entire transaction
- Parse error rejects entire transaction
- No partial commits

### Consistency

Database remains consistent:
- Constraints enforced
- Types validated
- References checked (optionally)

### Isolation

Transactions are isolated:
- Each sees consistent snapshot
- No dirty reads
- Serializable execution

### Durability

Committed data is durable:
- Written to persistent storage
- Replicated (if configured)
- Immutable

## Error Handling

### Validation Errors

```json
{
  "error": "ValidationError",
  "message": "Invalid IRI format",
  "code": "INVALID_IRI",
  "details": {
    "iri": "not a uri",
    "line": 3
  }
}
```

### Conflict Errors

```json
{
  "error": "ConflictError",
  "message": "Concurrent modification detected",
  "code": "CONCURRENT_MODIFICATION"
}
```

### Policy Errors

```json
{
  "error": "Forbidden",
  "message": "Policy denies transact on mydb:main",
  "code": "POLICY_DENIED"
}
```

## Performance Considerations

### Transaction Size

- Recommended: < 1,000 triples per transaction
- Maximum: Configurable (default 10,000)
- Large transactions increase commit time

### Indexing Lag

- Background indexing may lag behind commits
- Monitor `commit_t - index_t` gap
- Tune indexing frequency if needed

### Batch Operations

For bulk imports:
- Batch into reasonably-sized transactions
- Monitor memory usage
- Allow time for indexing between batches

See [Indexing Side-Effects](indexing-side-effects.md) for details.

## Best Practices

### 1. Meaningful Transaction Units

Group related changes in single transaction:

Good:
```json
{
  "@graph": [
    { "@id": "ex:order-123", "ex:customer": { "@id": "ex:alice" } },
    { "@id": "ex:order-123", "ex:items": [...] },
    { "@id": "ex:order-123", "ex:total": 99.99 }
  ]
}
```

### 2. Include Metadata

Add provenance information:

```json
{
  "@graph": [
    {
      "@id": "ex:alice",
      "schema:name": "Alice",
      "ex:created": "2024-01-22T10:00:00Z",
      "ex:createdBy": "user-123"
    }
  ]
}
```

### 3. Use Descriptive IRIs

Good: `ex:user-alice-123`
Bad: `ex:1`

### 4. Test Transactions

Test transactions before production:
- Validate JSON-LD syntax
- Check IRI formats
- Verify types and constraints

### 5. Monitor Performance

Track metrics:
- Average commit time
- Indexing lag
- Transaction size
- Error rate

### 6. Handle Errors Gracefully

Implement retry logic for transient errors:
- Network errors
- Timeout errors
- Conflict errors (with updated data)

### 7. Design for Time Travel

Remember data is immutable:
- Changes create new versions
- Historical queries see all versions
- Design with temporal access in mind

## Related Documentation

- [Insert](insert.md) - Adding new data
- [Upsert](upsert.md) - Replace mode
- [Update](update-where-delete-insert.md) - Targeted updates
- [Commit Receipts](commit-receipts.md) - Receipt details
- [Indexing Side-Effects](indexing-side-effects.md) - Indexing behavior
