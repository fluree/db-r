# Update (WHERE/DELETE/INSERT)

The WHERE/DELETE/INSERT pattern enables targeted updates to existing data in Fluree. This is the most flexible update mechanism, allowing conditional modifications, partial updates, and complex transformations.

## Basic Pattern

The WHERE/DELETE/INSERT pattern has three clauses:

1. **WHERE**: Pattern to match existing data
2. **DELETE**: Triples to retract (using variables from WHERE)
3. **INSERT**: Triples to assert (using variables from WHERE)

```json
{
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
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

This:
1. Finds the current age of ex:alice
2. Deletes that age value
3. Inserts the new age value

## Simple Property Update

Update a single property value:

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "where": [
      { "@id": "ex:alice", "schema:email": "?oldEmail" }
    ],
    "delete": [
      { "@id": "ex:alice", "schema:email": "?oldEmail" }
    ],
    "insert": [
      { "@id": "ex:alice", "schema:email": "alice.new@example.org" }
    ]
  }'
```

## Multiple Property Updates

Update several properties at once:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:name": "?oldName" },
    { "@id": "ex:alice", "schema:email": "?oldEmail" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:name": "?oldName" },
    { "@id": "ex:alice", "schema:email": "?oldEmail" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:name": "Alice Johnson" },
    { "@id": "ex:alice", "schema:email": "alice.j@example.org" }
  ]
}
```

## Conditional Updates

Only update if condition is met:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?age" },
    { "@id": "ex:alice", "ex:status": "?status" }
  ],
  "filter": "?age >= 18 && ?status == 'pending'",
  "delete": [
    { "@id": "ex:alice", "ex:status": "?status" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:status": "approved" }
  ]
}
```

The update only happens if Alice is 18+ and status is "pending".

## Pattern Matching

### Find and Update

Find entities matching a pattern and update them:

```json
{
  "where": [
    { "@id": "?person", "@type": "schema:Person" },
    { "@id": "?person", "ex:status": "pending" }
  ],
  "delete": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "insert": [
    { "@id": "?person", "ex:status": "active" }
  ]
}
```

This updates ALL people with status="pending" to status="active".

### Relationship-Based Updates

Update based on relationships:

```json
{
  "where": [
    { "@id": "?employee", "schema:worksFor": "ex:company-a" },
    { "@id": "?employee", "ex:salary": "?oldSalary" }
  ],
  "delete": [
    { "@id": "?employee", "ex:salary": "?oldSalary" }
  ],
  "insert": [
    { "@id": "?employee", "ex:salary": "?newSalary" }
  ],
  "bind": {
    "?newSalary": "?oldSalary * 1.1"
  }
}
```

Gives all company-a employees a 10% raise.

## Variable Transformation

Use variables from WHERE in INSERT with transformations:

```json
{
  "where": [
    { "@id": "ex:product-123", "ex:price": "?currentPrice" }
  ],
  "delete": [
    { "@id": "ex:product-123", "ex:price": "?currentPrice" }
  ],
  "insert": [
    { "@id": "ex:product-123", "ex:price": "?newPrice" },
    { "@id": "ex:product-123", "ex:previousPrice": "?currentPrice" }
  ],
  "bind": {
    "?newPrice": "?currentPrice * 0.9"
  }
}
```

Applies 10% discount and saves previous price.

## Partial Updates

Update only specific properties, leaving others unchanged:

**Current State:**
```text
ex:alice schema:name "Alice"
ex:alice schema:email "alice@example.org"
ex:alice schema:age 30
ex:alice schema:telephone "+1-555-0100"
```

**Update Only Age:**
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

**Result:**
```text
ex:alice schema:name "Alice"              (unchanged)
ex:alice schema:email "alice@example.org" (unchanged)
ex:alice schema:age 31                     (updated)
ex:alice schema:telephone "+1-555-0100"   (unchanged)
```

## Adding Properties

Add a property without WHERE (when it might not exist):

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:telephone": "+1-555-0100" }
  ]
}
```

Or conditionally add if missing:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:name": "?name" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:telephone": "+1-555-0100" }
  ],
  "optional": [
    { "@id": "ex:alice", "schema:telephone": "?existingPhone" }
  ],
  "filter": "!bound(?existingPhone)"
}
```

## Removing Properties

Remove a property entirely:

```json
{
  "where": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:telephone": "?phone" }
  ]
}
```

No INSERT clause—just deletes.

## Multi-Value Properties

### Replace One Value

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:email": "alice.new@example.org" }
  ]
}
```

### Add Value

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:email": "alice.work@example.org" }
  ]
}
```

### Remove One Value

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "alice.old@example.org" }
  ]
}
```

### Remove All Values

```json
{
  "where": [
    { "@id": "ex:alice", "schema:email": "?email" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:email": "?email" }
  ]
}
```

## Relationship Updates

### Change Relationship

```json
{
  "where": [
    { "@id": "ex:alice", "schema:worksFor": "?oldCompany" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:worksFor": "?oldCompany" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:worksFor": "ex:company-b" }
  ]
}
```

### Add Relationship

```json
{
  "insert": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ]
}
```

### Remove Relationship

```json
{
  "where": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:knows": "ex:bob" }
  ]
}
```

## Complex Updates

### Cascading Updates

Update related entities:

```json
{
  "where": [
    { "@id": "ex:order-123", "ex:status": "?oldStatus" },
    { "@id": "ex:order-123", "ex:items": "?item" },
    { "@id": "?item", "ex:status": "?itemStatus" }
  ],
  "delete": [
    { "@id": "ex:order-123", "ex:status": "?oldStatus" },
    { "@id": "?item", "ex:status": "?itemStatus" }
  ],
  "insert": [
    { "@id": "ex:order-123", "ex:status": "shipped" },
    { "@id": "?item", "ex:status": "shipped" }
  ]
}
```

### Computed Values

Calculate new values based on old:

```json
{
  "where": [
    { "@id": "ex:product-123", "ex:inventory": "?current" },
    { "@id": "ex:product-123", "ex:sold": "?sold" }
  ],
  "delete": [
    { "@id": "ex:product-123", "ex:inventory": "?current" }
  ],
  "insert": [
    { "@id": "ex:product-123", "ex:inventory": "?newInventory" }
  ],
  "bind": {
    "?newInventory": "?current - ?sold"
  }
}
```

## Error Handling

### No Match

If WHERE doesn't match, nothing happens (not an error):

```json
{
  "where": [
    { "@id": "ex:nonexistent", "schema:name": "?name" }
  ],
  "delete": [...],
  "insert": [...]
}
```

Result: No changes, no error.

### Multiple Matches

If WHERE matches multiple entities, all are updated:

```json
{
  "where": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "delete": [
    { "@id": "?person", "ex:status": "pending" }
  ],
  "insert": [
    { "@id": "?person", "ex:status": "approved" }
  ]
}
```

Updates ALL entities with status="pending".

## Comparison: WHERE/DELETE/INSERT vs Replace Mode

| Feature | WHERE/DELETE/INSERT | Replace Mode |
|---------|---------------------|--------------|
| **Granularity** | Property-level | Entity-level |
| **Other properties** | Preserved | Removed |
| **Conditional** | Yes (with filters) | No |
| **Pattern matching** | Yes | No |
| **Idempotent** | Depends on logic | Yes |
| **Use case** | Partial updates | Complete replacement |

## Best Practices

### 1. Be Specific in WHERE

Good (specific):
```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ]
}
```

Risky (might match many):
```json
{
  "where": [
    { "@id": "?person", "schema:age": "?age" }
  ]
}
```

### 2. Always Use Variables

Use variables from WHERE in DELETE:

Good:
```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?oldAge" }]
}
```

Bad (deletes all ages):
```json
{
  "where": [{ "@id": "ex:alice", "schema:age": "?oldAge" }],
  "delete": [{ "@id": "ex:alice", "schema:age": "?age" }]
}
```

### 3. Test Updates

Test on development data first:

```javascript
// Test update logic
const result = await transact(updateQuery);
console.log(`Updated ${result.flakes_retracted} values`);
```

### 4. Use Filters for Safety

Add filters to prevent unintended updates:

```json
{
  "where": [...],
  "filter": "?age >= 0 && ?age <= 150",
  "delete": [...],
  "insert": [...]
}
```

### 5. Handle No Matches

Decide if no matches should be an error in your application:

```javascript
const result = await transact(updateQuery);
if (result.flakes_retracted === 0) {
  console.warn('Update matched no entities');
}
```

### 6. Document Complex Updates

Comment complex update logic:

```javascript
// Update inventory after sale completion
// - Decrement stock by sold quantity
// - Update last-sold timestamp
// - Mark as low-stock if below threshold
const updateInventory = { ... };
```

## Performance Considerations

### Index Usage

WHERE clauses use indexes:
- Subject-based: Fast
- Predicate-based: Fast
- Pattern-based: May be slower

### Batch Updates

For many updates, consider batching:

```javascript
const updates = entities.map(e => createUpdateQuery(e));
for (const update of updates) {
  await transact(update);
}
```

## Related Documentation

- [Insert](insert.md) - Adding new data
- [Upsert](upsert.md) - Replace mode
- [Retractions](retractions.md) - Removing data
- [Overview](overview.md) - Transaction overview
- [Query WHERE Clauses](../query/jsonld-query.md) - WHERE pattern syntax
