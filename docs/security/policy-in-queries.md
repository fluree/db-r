# Policy in Queries

Policies are enforced at query time, filtering results to show only data the user is authorized to see. This document explains how policies affect query execution.

## Query-Time Filtering

When a query executes, Fluree:
1. Parses the query
2. Identifies the subject (from signed request or auth)
3. Collects applicable policies
4. Augments query with policy filters
5. Executes augmented query
6. Returns filtered results

**User never sees unauthorized data.**

## Basic Example

### Without Policy

Query:
```sparql
SELECT ?name
WHERE {
  ?person schema:name ?name .
}
```

Result (all people):
```json
[
  { "name": "Alice" },
  { "name": "Bob" },
  { "name": "Carol" },
  { "name": "David" }
]
```

### With Policy

Policy:
```json
{
  "f:subject": "did:key:z6Mkhabc...",
  "f:action": "query",
  "f:resource": {
    "ex:department": "engineering"
  },
  "f:allow": true
}
```

Same query, filtered result (only engineering):
```json
[
  { "name": "Alice" },
  { "name": "Bob" }
]
```

Carol and David are in other departments, so filtered out.

## Policy Application

### Type-Based Filtering

Policy limiting to specific type:

```json
{
  "f:subject": "*",
  "f:action": "query",
  "f:resource": {
    "@type": "ex:PublicDocument"
  },
  "f:allow": true
}
```

Query:
```sparql
SELECT ?title
WHERE {
  ?doc schema:title ?title .
}
```

Augmented query (automatically):
```sparql
SELECT ?title
WHERE {
  ?doc a ex:PublicDocument .
  ?doc schema:title ?title .
}
```

Only public documents returned.

### Property-Based Filtering

Policy on specific property:

```json
{
  "f:subject": { "ex:role": "hr" },
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:salary"
  },
  "f:allow": true
}
```

For non-HR users, queries requesting salary:
```sparql
SELECT ?name ?salary
WHERE {
  ?person schema:name ?name .
  ?person ex:salary ?salary .
}
```

Result (salary filtered out):
```json
[
  { "name": "Alice", "salary": null },
  { "name": "Bob", "salary": null }
]
```

For HR users, full data returned.

### Entity-Level Filtering

Policy for entity ownership:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

Query:
```sparql
SELECT ?title
WHERE {
  ?doc schema:title ?title .
}
```

Augmented:
```sparql
SELECT ?title
WHERE {
  ?doc ex:owner <did:key:z6Mkhabc...> .
  ?doc schema:title ?title .
}
```

Only documents owned by user returned.

## Pattern Matching

Policies can match complex patterns:

```json
{
  "f:subject": "?user",
  "f:resource": {
    "@type": "ex:Document",
    "ex:department": "?dept"
  },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

This allows users to see documents from their department.

Query:
```json
{
  "select": ["?title"],
  "where": [
    { "@id": "?doc", "schema:title": "?title" }
  ]
}
```

Augmented with policy:
```json
{
  "select": ["?title"],
  "where": [
    { "@id": "?doc", "schema:title": "?title" },
    { "@id": "?doc", "@type": "ex:Document" },
    { "@id": "?doc", "ex:department": "?dept" },
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "bind": {
    "?user": "did:key:z6Mkhabc..."
  }
}
```

## Multi-Policy Scenarios

### Combining Policies

Multiple policies can apply:

**Policy 1 (allow public):**
```json
{
  "f:resource": { "ex:visibility": "public" },
  "f:allow": true
}
```

**Policy 2 (allow owned):**
```json
{
  "f:subject": "?user",
  "f:resource": { "ex:owner": "?user" },
  "f:allow": true
}
```

Query returns:
- Public documents (from policy 1)
- User's private documents (from policy 2)

### Deny Overrides

Deny policies override allow:

**Policy 1 (allow department):**
```json
{
  "f:resource": { "ex:department": "engineering" },
  "f:allow": true
}
```

**Policy 2 (deny sensitive):**
```json
{
  "f:resource": { "ex:classification": "confidential" },
  "f:allow": false
}
```

Query returns:
- Engineering documents (from policy 1)
- EXCEPT confidential ones (blocked by policy 2)

## SPARQL Queries

SPARQL queries are filtered the same way:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

SELECT ?name ?email
WHERE {
  ?person a schema:Person .
  ?person schema:name ?name .
  ?person schema:email ?email .
}
```

With policy limiting to public profiles:
```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

SELECT ?name ?email
WHERE {
  ?person a schema:Person .
  ?person ex:profileVisibility "public" .  # Added by policy
  ?person schema:name ?name .
  ?person schema:email ?email .
}
```

## JSON-LD Queries

JSON-LD queries work similarly:

Original query:
```json
{
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "schema:age": "?age" }
  ]
}
```

With policy (department-based):
```json
{
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "schema:age": "?age" },
    { "@id": "?person", "ex:department": "?dept" },
    { "@id": "<did:key:z6Mkh...>", "ex:department": "?dept" }
  ]
}
```

## Performance Considerations

### Efficient Policies

Type-based policies are efficient:

Good:
```json
{
  "f:resource": { "@type": "ex:PublicData" }
}
```

Less efficient:
```json
{
  "f:resource": { "?pred": "?val" },
  "f:condition": [
    { "f:filter": "complex condition" }
  ]
}
```

### Index Usage

Policy filters use indexes:
- Type filters: Use OPST index
- Property filters: Use POST/PSOT indexes
- Entity filters: Use SPOT index

### Query Planning

Fluree optimizes policy-augmented queries:
- Pushes filters down
- Reorders joins
- Selects optimal indexes

Check query plan:
```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Explain: true" \
  -d '{...}'
```

## Property Redaction

Hide specific properties:

Policy:
```json
{
  "f:subject": "*",
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:ssn"
  },
  "f:allow": false
}
```

Query:
```json
{
  "select": ["?name", "?ssn"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "ex:ssn": "?ssn" }
  ]
}
```

Result (SSN redacted):
```json
[
  { "name": "Alice", "ssn": null },
  { "name": "Bob", "ssn": null }
]
```

## Aggregate Queries

Policies apply to aggregates:

Query:
```sparql
SELECT (COUNT(?person) AS ?count)
WHERE {
  ?person a schema:Person .
}
```

With policy (department-based), count includes only authorized people.

## Graph Crawl Queries

Policies apply to graph crawls:

```json
{
  "select": "?person",
  "where": [
    { "@id": "ex:alice", "schema:knows": "?person" }
  ],
  "depth": 3
}
```

Crawl follows links only to authorized entities.

## Multi-Graph Queries

Policies per graph:

```sparql
SELECT ?data
FROM <public:main>
FROM <private:main>
WHERE {
  ?entity ex:data ?data .
}
```

Policy for public graph allows all, policy for private graph restricts.

## Time Travel Queries

Policies apply to historical queries:

```json
{
  "from": "mydb:main@t:100",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "schema:name": "?name" }
  ]
}
```

Same policies apply, showing historical data user was authorized to see.

## Debugging Policy Filtering

### Enable Policy Trace

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response:
```json
{
  "results": [...],
  "policy_trace": {
    "policies_applied": [
      {
        "id": "ex:policy-1",
        "effect": "filter_added",
        "filter": "?person ex:department 'engineering'"
      }
    ],
    "original_pattern_count": 2,
    "augmented_pattern_count": 4,
    "execution_time_ms": 45
  }
}
```

### Check Effective Query

See augmented query:

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Show-Augmented-Query: true" \
  -d '{...}'
```

Response includes augmented query showing policy filters.

## Common Patterns

### Public Read, Private Write

```json
[
  {
    "@id": "ex:public-read",
    "f:subject": "*",
    "f:action": "query",
    "f:resource": { "ex:visibility": "public" },
    "f:allow": true
  },
  {
    "@id": "ex:owner-full-access",
    "f:subject": "?user",
    "f:action": "*",
    "f:resource": { "ex:owner": "?user" },
    "f:allow": true
  }
]
```

### Department Isolation

```json
{
  "f:subject": "?user",
  "f:resource": { "ex:department": "?dept" },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

### Role-Based Views

```json
[
  {
    "@id": "ex:manager-view",
    "f:subject": { "ex:role": "manager" },
    "f:action": "query",
    "f:resource": { "ex:visibility": ["public", "internal"] },
    "f:allow": true
  },
  {
    "@id": "ex:employee-view",
    "f:subject": { "ex:role": "employee" },
    "f:action": "query",
    "f:resource": { "ex:visibility": "public" },
    "f:allow": true
  }
]
```

## Best Practices

### 1. Test Policy Effects

Test queries with different subjects:

```javascript
async function testQueryWithPolicies() {
  const query = { select: ["?name"], where: [...] };
  
  const adminResult = await queryAs("admin", query);
  const userResult = await queryAs("user", query);
  
  console.log(`Admin sees ${adminResult.length} results`);
  console.log(`User sees ${userResult.length} results`);
}
```

### 2. Monitor Query Performance

Track policy overhead:

```javascript
const start = Date.now();
const result = await query({...});
const duration = Date.now() - start;

if (duration > 1000) {
  logger.warn(`Slow query with policies: ${duration}ms`);
}
```

### 3. Use Specific Filters

Make policy filters as specific as possible for better performance.

### 4. Document Policy Intent

```json
{
  "@id": "ex:policy-1",
  "rdfs:label": "Engineering department access",
  "rdfs:comment": "Allows engineering team to view all technical documents",
  ...
}
```

### 5. Test Edge Cases

- Empty result sets
- Large result sets
- Complex graph patterns
- Multi-graph queries

## Related Documentation

- [Policy Model](policy-model.md) - Policy structure and evaluation
- [Policy in Transactions](policy-in-transactions.md) - Write-time enforcement
- [Query Documentation](../query/README.md) - Query syntax
- [Explain Plans](../query/explain.md) - Query optimization
