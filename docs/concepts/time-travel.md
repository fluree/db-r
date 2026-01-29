# Time Travel

**Differentiator**: Fluree is a **temporal database** that preserves the complete history of all changes. Every transaction is timestamped, enabling queries against any previous state of the data. This "time travel" capability is fundamental to Fluree's architecture and provides capabilities that most databases cannot match.

## Query Formats

Time travel is supported in both JSON-LD and SPARQL query formats. Examples in this document primarily use JSON-LD syntax with SPARQL equivalents shown where relevant.

## Transaction Time

Every transaction in Fluree receives a unique **transaction time** (`t`) - a monotonically increasing integer that represents the logical time of the transaction.

### Transaction Ordering

```text
Transaction 1: t=1
Transaction 2: t=2
Transaction 3: t=3
...
```

- **Monotonic**: Each new transaction gets a higher `t` than all previous transactions
- **Unique**: No two transactions share the same `t`
- **Global**: Transaction times are unique across the entire Fluree instance

### Current Time

The **current time** is the highest transaction time that has been committed. Queries without a time specifier automatically query the current state:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

You can also explicitly specify `@t:latest` to query the latest state:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@t:latest",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Historical Queries

Fluree supports querying data as it existed at any point in time using the `@` syntax in ledger references.

### Point-in-Time Queries

Query data as it existed at a specific transaction using the `from` field with `@t:`:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@t:100",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

### Query at ISO Timestamp

Query using ISO 8601 datetime with `@iso:`:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@iso:2024-01-15T10:30:00Z",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

### Query at Commit SHA

Query at a specific commit using `@sha:` with a commit hash prefix (minimum 6 characters):

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@sha:abc123def456",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Temporal Data Model

### Immutable Facts

Once committed, data is **immutable**. Changes are represented as new facts that supersede previous ones:

```text
t=1: Alice age 25  (assertion)
t=5: Alice age 26  (retraction of age 25, assertion of age 26)
```

History queries capture both the retraction and assertion with `@op`:

```json
[
  [25, 1, "assert"],
  [25, 5, "retract"],
  [26, 5, "assert"]
]
```

Each row shows `[value, transaction_time, operation]`.

### Valid Time vs Transaction Time

Fluree primarily uses **transaction time** (when the fact was recorded in the database). For applications needing **valid time** (when the fact was true in the real world), this can be modeled explicitly as properties:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "@graph": [
    {
      "@id": "ex:alice-employment-1",
      "ex:person": "ex:alice",
      "ex:company": "ex:company-a",
      "ex:validFrom": "2020-01-01T00:00:00Z",
      "ex:validTo": "2023-12-31T23:59:59Z"
    }
  ]
}
```

This allows you to query by both:
- **Transaction time**: When was this recorded? (using `@t:`, `@iso:`, `@sha:`)
- **Valid time**: When was this true? (using standard WHERE clause filters on `ex:validFrom`/`ex:validTo`)

## Snapshot and Indexing

### Database Snapshots

Fluree maintains **indexed snapshots** at regular intervals for efficient historical access:

- **Index**: A complete, optimized snapshot of the database at a specific `t`
- **Novelty**: Uncommitted transactions since the last index
- **Background Indexing**: Continuous process that creates new indexes

### Query Execution Model

Queries combine indexed data with novelty:

```text
Query Result = Indexed Database (up to t=index) + Novelty (t=index+1 to current)
```

This provides:
- **Fast historical queries**: Use appropriate index
- **Real-time current queries**: Include latest transactions
- **Consistent snapshots**: Each query sees a consistent state

## History Queries for Change Tracking

History queries let you see all changes (assertions and retractions) within a time range. Specify the range using two time-specced endpoints in the `from` clause.

### Entity History (JSON-LD)

Track all changes to a specific entity over time by specifying a time range:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?name", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

The `@t` and `@op` annotations bind the transaction time and operation type:
- **@t** - Transaction time when the fact was asserted or retracted
- **@op** - Either `"assert"` or `"retract"`

Returns results showing all changes:

```json
[
  ["Alice", 1, "assert"],
  ["Alice", 5, "retract"],
  ["Alicia", 5, "assert"]
]
```

### Entity History (SPARQL)

The same query in SPARQL uses RDF-star syntax with `FROM...TO`:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?name ?t ?op
FROM <ledger:main@t:1>
TO <ledger:main@t:latest>
WHERE {
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}
ORDER BY ?t
```

### Property-Specific History

Query changes for specific properties:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:100"],
  "select": ["?age", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:age": { "@value": "?age", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

### All Properties History

Query all property changes for an entity:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?p", "?v", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "?p": { "@value": "?v", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

### Time Range with Datetime

Query history using ISO 8601 datetime strings:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "ledger:main@iso:2024-01-01T00:00:00Z",
    "ledger:main@iso:2024-12-31T23:59:59Z"
  ],
  "select": ["?name", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } }
  ]
}
```

### Filter by Operation Type

Filter to show only assertions or only retractions:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?name", "?t"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } },
    ["filter", "(= ?op \"retract\")"]
  ]
}
```

### Pattern History Across Subjects

Query changes for a specific property across all subjects:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?person", "?status", "?t", "?op"],
  "where": [
    { "@id": "?person", "ex:status": { "@value": "?status", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

## Performance Characteristics

### Time Resolution Performance

Different time specifiers have different performance characteristics:

- **@t:NNN** (fastest): Direct transaction number, no resolution needed
- **@iso:DATETIME**: O(log n) binary search through commit timestamps using POST index
- **@sha:PREFIX**: Bounded SPOT scan, O(k) where k is commits matching prefix (use longer prefixes for better performance)

### Index Selection

Fluree automatically selects the most appropriate index for historical queries:

- **Recent history**: Uses current index + novelty (uncommitted transactions)
- **Historical snapshots**: Uses closest index snapshot to target time
- **Point queries** (`@t:`): Direct index lookup for specific transaction

### History Query Performance

History queries scan flakes within the specified time range:

- **Entity history** (specific `@id`): SPOT index scan on subject
- **Property history** (specific predicate): Narrower SPOT scan with predicate filter
- **All properties** (variable predicate `?p`): Full SPOT scan for subject
- **Cross-entity** (variable subject `?s`): POST/PSOT index scan (can be slower for common predicates)

### Optimization Strategies

1. **Use Transaction Numbers**: When possible, use `@t:NNN` instead of `@iso:DATETIME`
2. **Narrow History Patterns**: Use `[subject, predicate]` instead of `[subject]` when you only need specific properties
3. **Limit Time Ranges**: Specify realistic `from`/`to` bounds rather than querying all history
4. **SHA Prefix Length**: Use 7+ character SHA prefixes to avoid ambiguity checks
5. **Index Density**: More frequent indexing improves historical query performance for distant past

### Storage Implications

- **Full History**: All transaction history is preserved (immutable append-only)
- **Index Snapshots**: Periodic snapshots enable efficient historical queries without replaying all transactions
- **Commit Metadata**: Stored as queryable flakes (~8-9 flakes per commit)
- **Transaction JSON**: Optionally stored for audit trails (enable with `txn: true`)

## Practical Applications

### Version Control

Treat data like code with version control:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "app:production@t:1000",
  "select": ["?config"],
  "where": [
    { "@id": "?setting", "ex:value": "?config" }
  ]
}
```

### Regulatory Compliance

Maintain complete audit trails - query data as it existed at time of consent:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "users:main@iso:2024-05-25T14:30:00Z",
  "select": ["?predicate", "?data"],
  "where": [
    { "@id": "ex:alice", "?predicate": "?data" }
  ]
}
```

### Change History Analysis

Track how data evolved over time:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "sales:main@iso:2024-01-01T00:00:00Z",
    "sales:main@iso:2024-12-31T23:59:59Z"
  ],
  "select": ["?order", "?amount", "?t", "?op"],
  "where": [
    { "@id": "?order", "ex:amount": { "@value": "?amount", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

### Debugging and Troubleshooting

Investigate system state at time of incident:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "system:config@iso:2024-01-15T09:15:00Z",
  "select": ["?setting", "?config"],
  "where": [
    { "@id": "?setting", "ex:value": "?config" }
  ]
}
```

## Time Travel in Multi-Ledger Scenarios

### Cross-Ledger Temporal Queries

Query across ledgers at consistent time points:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "customers:main@t:1000",
    "orders:main@t:1000"
  ],
  "select": ["?customer", "?order"],
  "where": [
    { "@id": "?customer", "ex:name": "Alice" },
    { "@id": "?order", "ex:customer": "?customer" }
  ]
}
```

### Ledger Branching

Time travel enables sophisticated branching workflows by querying historical states:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@t:500",
  "select": ["?entity", "?property", "?value"],
  "where": [
    { "@id": "?entity", "?property": "?value" }
  ]
}
```

You can then use this historical state as a basis for creating a new branch or comparing against current state.

## Common Patterns

### Compare Current vs Historical State

Query the same entity at two different points in time:

```json
// Query current state
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main",
  "select": ["?price"],
  "where": [
    { "@id": "ex:product-123", "ex:price": "?price" }
  ]
}

// Query historical state
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "ledger:main@t:100",
  "select": ["?price"],
  "where": [
    { "@id": "ex:product-123", "ex:price": "?price" }
  ]
}
```

### Find When a Change Occurred

Use history queries to identify when a specific change happened:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?status", "?t", "?op"],
  "where": [
    { "@id": "ex:product-123", "ex:status": { "@value": "?status", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

The results show when `ex:status` changed, with `"retract"` for the old value and `"assert"` for the new value at the same transaction time.

### Audit Trail for Compliance

Generate a complete audit trail for a sensitive entity:

```json
{
  "@context": { "schema": "http://schema.org/" },
  "from": [
    "users:main@iso:2024-01-01T00:00:00Z",
    "users:main@t:latest"
  ],
  "select": ["?property", "?value", "?t", "?op"],
  "where": [
    { "@id": "schema:Person/12345", "?property": { "@value": "?value", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

This returns all changes with transaction times for audit purposes. Each result row shows the property, value, when it was changed, and whether it was an assertion or retraction.

### Rollback Detection

Find what changed after a specific commit:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["config:main@t:50", "config:main@t:latest"],
  "select": ["?setting", "?value", "?t", "?op"],
  "where": [
    { "@id": "?setting", "ex:config": { "@value": "?value", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

This shows all configuration changes since transaction 50, useful for identifying what to rollback. You can first query `"from": "config:main@sha:abc123"` to find the transaction number (using point-in-time queries), then use that in the history query.

### Reproduce a Bug at Specific Time

Query the exact state of the system when a bug was reported:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "products:main@iso:2024-06-15T14:30:00Z",
    "inventory:main@iso:2024-06-15T14:30:00Z"
  ],
  "select": ["?product", "?stock", "?reserved"],
  "where": [
    { "@id": "?product", "ex:stockLevel": "?stock" },
    { "@id": "?product", "ex:reserved": "?reserved" }
  ]
}
```

This recreates the exact state across multiple ledgers at the time the bug occurred, making debugging much easier.

## Best Practices

### Time Travel Guidelines

1. **Explicit Time References**: Always specify clear time references (`@t:`, `@iso:`, or `@sha:`) for reproducible queries
2. **Time Zone Awareness**: Use UTC for ISO timestamps to avoid ambiguity
3. **SHA Prefix Length**: Use at least 7-character SHA prefixes to avoid collisions (minimum 6 required)
4. **Performance Testing**: Test query performance across different time ranges and ledger sizes

### History Query Patterns

1. **Narrow Your Scope**: Use specific property patterns rather than wildcard `?p` when you only need certain properties
2. **Limit Time Ranges**: Specify realistic time ranges in the `from` array rather than `@t:1` to `@t:latest`
3. **Use Filters**: Filter by `@op` to show only assertions or retractions when you don't need both
4. **Order Results**: Use `orderBy: "?t"` to see changes in chronological order

### Data Modeling for Time

1. **Temporal Validity**: Model valid time explicitly when needed (separate from transaction time)
2. **Change Tracking**: Use history queries rather than storing change logs manually
3. **Immutable Design**: Design for immutability from the start - never update in place
4. **Audit Patterns**: Leverage history queries for audit trails instead of separate audit tables

### Operational Considerations

1. **Index Maintenance**: Monitor and tune background indexing for optimal historical query performance
2. **Storage Planning**: Plan storage growth for historical data (all history is preserved)
3. **Query Optimization**: Use time-specific queries (`@t:`) rather than datetime resolution (`@iso:`) when transaction numbers are known
4. **Backup Strategy**: Include temporal aspects in backup/recovery plans - commits and indexes are both critical

## Implementation Architecture

### Transaction Pipeline

1. **Transaction Reception**: Assign new transaction time (`t`)
2. **Validation**: Check against current state
3. **Commitment**: Persist transaction with ISO timestamp
4. **Commit Metadata**: Store commit SHA, timestamp, and optional transaction JSON
5. **Indexing**: Background process creates new indexes
6. **Publication**: Update nameservice with new transaction time

### Time Travel Resolution

When you query with `@t:`, `@iso:`, or `@sha:`:

1. **@t:NNN** - Direct transaction number (fastest)
2. **@iso:DATETIME** - Binary search through commit timestamps using POST index
3. **@sha:PREFIX** - Bounded SPOT scan to find matching commit (prefix >= 6 chars)

### Query Execution

1. **Time Resolution**: Resolve time specifiers to specific `t` values
2. **Index Selection**: Choose appropriate index for target time
3. **Novelty Application**: Apply intervening transactions if needed
4. **Result Generation**: Return consistent snapshot

### History Query Execution

1. **Time Range Detection**: The `from` array with two time-specced endpoints activates history mode
2. **Pattern Resolution**: WHERE patterns are executed with history mode enabled
3. **Metadata Capture**: Transaction time (`@t`) and operation (`@op`) are captured for each binding
4. **Result Generation**: Results include both assertions and retractions within the time range

This temporal foundation makes Fluree uniquely powerful for applications requiring complete historical visibility, audit capabilities, and temporal analytics.