# Quickstart: Write Data

This guide shows you how to write data to Fluree using three main patterns: insert, upsert, and update.

## Prerequisites

- Fluree server running (see [Run the Server](quickstart-server.md))
- A ledger created (see [Create a Ledger](quickstart-ledger.md))

## Understanding Fluree Transactions

Fluree stores data as RDF triples (subject-predicate-object). Transactions are submitted as JSON-LD documents that get converted to triples internally.

### Basic Transaction Structure

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice"
    }
  ]
}
```

This creates triples like:
```text
ex:alice  rdf:type        schema:Person
ex:alice  schema:name     "Alice"
```

## Insert: Adding New Data

The simplest operation is inserting new entities.

### Insert a Single Entity

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice",
        "schema:email": "alice@example.org",
        "schema:age": 30
      }
    ]
  }'
```

Response:

```json
{
  "t": 1,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456...",
  "address": "fluree:memory:commit:abc123...",
  "flakes_added": 4,
  "flakes_retracted": 0
}
```

### Insert Multiple Entities

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:bob",
        "@type": "schema:Person",
        "schema:name": "Bob",
        "schema:email": "bob@example.org"
      },
      {
        "@id": "ex:carol",
        "@type": "schema:Person",
        "schema:name": "Carol",
        "schema:email": "carol@example.org"
      }
    ]
  }'
```

### Insert with Relationships

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:company-a",
        "@type": "schema:Organization",
        "schema:name": "Acme Corp"
      },
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice",
        "schema:worksFor": {"@id": "ex:company-a"}
      }
    ]
  }'
```

## Upsert: Idempotent Writes (Replace Mode)

Upsert (update/insert) replaces all properties of an entity. If the entity exists, it's completely replaced. If it doesn't exist, it's created.

### Basic Upsert

To enable upsert mode, use the `mode=replace` query parameter:

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main&mode=replace" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice Smith",
        "schema:email": "alice.smith@example.org",
        "schema:age": 31
      }
    ]
  }'
```

This **replaces all triples** for `ex:alice` with the new data. The old values are retracted and new values are asserted.

### Upsert Behavior

**First transaction** (entity doesn't exist):
- Creates the entity with all specified properties

**Subsequent transactions** (entity exists):
- Retracts ALL existing properties
- Asserts new properties
- Result: entity has only the newly specified properties

### Use Cases for Upsert

Good for:
- Idempotent writes (can retry safely)
- Syncing from external systems
- Complete entity replacement
- Avoiding duplicate checks

Not good for:
- Partial updates (use UPDATE instead)
- Preserving existing properties
- Additive operations

## Update: Targeted Changes (WHERE/DELETE/INSERT)

For targeted changes to existing data, use the UPDATE pattern with WHERE/DELETE/INSERT.

### Basic Update

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:age": "?oldAge" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:age": "?oldAge" }
    ],
    "insert": [
      { "@id": "ex:alice", "schema:age": 32 }
    ]
  }'
```

This pattern:
1. **WHERE**: Finds matching data
2. **DELETE**: Retracts specific triples
3. **INSERT**: Asserts new triples

### Update Multiple Properties

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:name": "?name", "schema:email": "?email" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:name": "?name", "schema:email": "?email" }
    ],
    "insert": [
      { "@id": "ex:alice", "schema:name": "Alice Johnson", "schema:email": "alice.j@example.org" }
    ]
  }'
```

### Conditional Update

Only update if condition is met:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:age": "?age" },
      { "@id": "?age", "@type": "xsd:integer" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:age": "?age" }
    ],
    "insert": [
      { "@id": "ex:alice", "schema:age": { "@value": "32", "@type": "xsd:integer" } }
    ]
  }'
```

### Adding Properties (Not Replacing)

To add a property without removing existing ones, use INSERT only:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "insert": [
      { "@id": "ex:alice", "schema:telephone": "+1-555-0100" }
    ]
  }'
```

This adds the telephone property without affecting other properties.

## Data Types

Fluree supports various data types through JSON-LD typing:

### Strings (Default)

```json
{
  "@id": "ex:alice",
  "schema:name": "Alice"
}
```

### Numbers

```json
{
  "@id": "ex:alice",
  "schema:age": 30,
  "schema:height": 1.68
}
```

### Booleans

```json
{
  "@id": "ex:alice",
  "schema:active": true
}
```

### Dates

```json
{
  "@id": "ex:alice",
  "schema:birthDate": {
    "@value": "1994-05-15",
    "@type": "xsd:date"
  }
}
```

### Timestamps

```json
{
  "@id": "ex:alice",
  "schema:lastLogin": {
    "@value": "2024-01-22T10:30:00Z",
    "@type": "xsd:dateTime"
  }
}
```

### References (Links to Other Entities)

```json
{
  "@id": "ex:alice",
  "schema:worksFor": { "@id": "ex:company-a" }
}
```

## Transaction Receipts

Every successful transaction returns a receipt with metadata:

```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456789...",
  "address": "fluree:memory:commit:abc123...",
  "flakes_added": 3,
  "flakes_retracted": 2,
  "previous_commit": "def456abc789..."
}
```

Key fields:
- **t**: Transaction time (monotonically increasing)
- **timestamp**: ISO 8601 timestamp
- **commit_sha**: Cryptographic hash of the commit
- **flakes_added**: Number of triples added
- **flakes_retracted**: Number of triples removed

See [Commit Receipts](../transactions/commit-receipts.md) for details.

## Error Handling

### Transaction Errors

If a transaction fails, you'll receive an error response:

```json
{
  "error": "TransactionError",
  "message": "Invalid IRI: not a valid URI",
  "code": "INVALID_IRI"
}
```

Common errors:
- **INVALID_IRI**: Malformed IRIs
- **PARSE_ERROR**: Invalid JSON-LD syntax
- **TYPE_ERROR**: Type mismatch
- **CONSTRAINT_VIOLATION**: Data constraint violated

### Validation

Transactions are validated before being applied:
- JSON-LD syntax must be valid
- IRIs must be well-formed
- Types must be compatible
- References must resolve (optional)

## Best Practices

### 1. Use Appropriate Write Pattern

- **Insert**: New entities, no duplication concerns
- **Upsert**: Idempotent writes, complete replacement
- **Update**: Targeted changes, preserve other properties

### 2. Choose Meaningful IRIs

Good:
```json
{"@id": "ex:user-12345"}
{"@id": "ex:product-widget-2024"}
```

Bad:
```json
{"@id": "ex:1"}
{"@id": "ex:thing"}
```

### 3. Use Consistent Namespaces

Define a clear namespace strategy:

```json
{
  "@context": {
    "app": "https://myapp.com/ns/",
    "schema": "http://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  }
}
```

### 4. Batch Related Changes

Include related entities in a single transaction:

```json
{
  "@graph": [
    {"@id": "ex:order-123", "ex:customer": {"@id": "ex:alice"}},
    {"@id": "ex:order-123", "ex:product": {"@id": "ex:widget"}},
    {"@id": "ex:order-123", "ex:quantity": 5}
  ]
}
```

### 5. Use Typed Literals

Be explicit about types for dates, numbers, etc.:

```json
{
  "@id": "ex:alice",
  "ex:birthDate": {
    "@value": "1994-05-15",
    "@type": "xsd:date"
  }
}
```

## Transaction Size Limits

Be aware of transaction size constraints:

- **Recommended**: < 1000 triples per transaction
- **Maximum**: Configurable (default: 10,000 triples)
- **Large imports**: Use batch processing

See [Indexing Side-Effects](../transactions/indexing-side-effects.md) for performance considerations.

## Next Steps

Now that you can write data:

1. [Query Data](quickstart-query.md) - Learn how to retrieve your data
2. [Transactions Overview](../transactions/overview.md) - Detailed transaction documentation
3. [JSON-LD Context](../concepts/iri-and-context.md) - Understanding @context

## Related Documentation

- [Insert](../transactions/insert.md) - Detailed insert documentation
- [Upsert](../transactions/upsert.md) - Detailed upsert documentation
- [Update](../transactions/update-where-delete-insert.md) - Detailed update documentation
- [Data Types](../concepts/datatypes.md) - Comprehensive type system guide
