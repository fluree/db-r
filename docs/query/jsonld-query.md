# JSON-LD Query

JSON-LD Query is Fluree's native query language, providing a JSON-based interface for querying graph data. It combines the expressiveness of SPARQL with the convenience of JSON, making it easy to integrate with modern applications.

## Overview

JSON-LD Query uses JSON-LD syntax to express queries, leveraging `@context` for IRI expansion and compaction. Queries are structured as JSON objects with familiar clauses like `select`, `where`, `from`, etc.

### Basic Query Structure

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "ex:name": "?name", "ex:age": "?age" }
  ]
}
```

## Query Clauses

### @context

The `@context` defines namespace mappings for IRI expansion/compaction:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "schema": "http://schema.org/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  }
}
```

### select

Specifies which variables to return in results:

```json
{
  "select": ["?name", "?age"]
}
```

**Wildcard Selection:**

```json
{
  "select": "*"
}
```

Returns all variables bound in the query.

### from

Specifies which ledger(s) to query:

**Single Ledger:**

```json
{
  "from": "mydb:main"
}
```

**Multiple Ledgers:**

```json
{
  "from": ["mydb:main", "otherdb:main"]
}
```

**Time Travel:**

```json
{
  "from": "mydb:main@t:100"
}
```

```json
{
  "from": "mydb:main@iso:2024-01-15T10:30:00Z"
}
```

```json
{
  "from": "mydb:main@sha:abc123def456"
}
```

### where

The `where` clause contains query patterns:

**Basic Pattern:**

```json
{
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

**Multiple Patterns:**

```json
{
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    { "@id": "?person", "ex:age": "?age" }
  ]
}
```

**Type Pattern:**

```json
{
  "where": [
    { "@id": "?person", "@type": "ex:User", "ex:name": "?name" }
  ]
}
```

## Pattern Types

### Object Patterns

Match triples where subject, predicate, and object are specified:

```json
{
  "@id": "ex:alice",
  "ex:name": "Alice"
}
```

### Variable Patterns

Use variables (starting with `?`) to match unknown values:

```json
{
  "@id": "?person",
  "ex:name": "?name"
}
```

### Type Patterns

Match entities by type:

```json
{
  "@id": "?person",
  "@type": "ex:User",
  "ex:name": "?name"
}
```

### Property Join Patterns

Match multiple properties of the same subject:

```json
{
  "@id": "?person",
  "ex:name": "?name",
  "ex:age": "?age",
  "ex:email": "?email"
}
```

## Advanced Patterns

### Optional Patterns

Match optional data that may not exist:

```json
{
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    ["optional", { "@id": "?person", "ex:email": "?email" }]
  ]
}
```

**Multiple Optionals:**

```json
{
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    ["optional", { "@id": "?person", "ex:email": "?email" }],
    ["optional", { "@id": "?person", "ex:phone": "?phone" }]
  ]
}
```

**Grouped Optionals:**

```json
{
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    ["optional",
     { "@id": "?person", "ex:email": "?email" },
     { "@id": "?person", "ex:phone": "?phone" }
    ]
  ]
}
```

### Union Patterns

Match data from multiple alternative patterns:

```json
{
  "where": [
    ["union",
     { "@id": "?person", "ex:name": "?name" },
     { "@id": "?person", "ex:alias": "?name" }
    ]
  ]
}
```

### Filter Patterns

Apply conditions to filter results:

**Single Filter:**

```json
{
  "where": [
    { "@id": "?person", "ex:age": "?age" },
    ["filter", "(> ?age 18)"]
  ]
}
```

**Multiple Filters:**

```json
{
  "where": [
    { "@id": "?person", "ex:age": "?age", "ex:name": "?name" },
    ["filter", "(> ?age 18)", "(strStarts ?name \"A\")"]
  ]
}
```

**Complex Filters:**

```json
{
  "where": [
    { "@id": "?person", "ex:age": "?age", "ex:last": "?last" },
    ["filter", "(and (> ?age 45) (strEnds ?last \"ith\"))"]
  ]
}
```

### Bind Patterns

Compute values and bind to variables:

```json
{
  "where": [
    { "@id": "?person", "ex:age": "?age" },
    ["bind", "(+ ?age 1)", "?nextAge"]
  ]
}
```

### Values Patterns

Provide initial bindings:

```json
{
  "where": [
    ["values", "?name", ["Alice", "Bob", "Carol"]],
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Filter Functions

### Comparison Functions

- `(= ?x ?y)` - Equality
- `(!= ?x ?y)` - Inequality
- `(> ?x ?y)` - Greater than
- `(>= ?x ?y)` - Greater than or equal
- `(< ?x ?y)` - Less than
- `(<= ?x ?y)` - Less than or equal

### Logical Functions

- `(and ...)` - Logical AND
- `(or ...)` - Logical OR
- `(not ...)` - Logical NOT

### String Functions

- `(strStarts ?str ?prefix)` - String starts with
- `(strEnds ?str ?suffix)` - String ends with
- `(contains ?str ?substr)` - String contains
- `(regex ?str ?pattern)` - Regular expression match

### Numeric Functions

- `(+ ?x ?y)` - Addition
- `(- ?x ?y)` - Subtraction
- `(* ?x ?y)` - Multiplication
- `(/ ?x ?y)` - Division
- `(abs ?x)` - Absolute value

### Type Functions

- `(bound ?var)` - Variable is bound
- `(isIRI ?x)` - Is an IRI
- `(isBlank ?x)` - Is a blank node
- `(isLiteral ?x)` - Is a literal

## Query Modifiers

### orderBy

Sort results:

```json
{
  "orderBy": ["?name"]
}
```

**Descending Order:**

```json
{
  "orderBy": [["desc", "?age"]]
}
```

**Multiple Sort Keys:**

```json
{
  "orderBy": ["?last", ["desc", "?age"]]
}
```

### limit

Limit number of results:

```json
{
  "limit": 10
}
```

### offset

Skip results:

```json
{
  "offset": 20,
  "limit": 10
}
```

### groupBy

Group results:

```json
{
  "select": ["?category", ["count", "?product"]],
  "groupBy": ["?category"],
  "where": [
    { "@id": "?product", "ex:category": "?category" }
  ]
}
```

### having

Filter grouped results:

```json
{
  "select": ["?category", ["count", "?product"]],
  "groupBy": ["?category"],
  "having": [["filter", "(> (count ?product) 10)"]],
  "where": [
    { "@id": "?product", "ex:category": "?category" }
  ]
}
```

## Aggregation Functions

- `(count ?var)` - Count non-null values
- `(sum ?var)` - Sum numeric values
- `(avg ?var)` - Average numeric values
- `(min ?var)` - Minimum value
- `(max ?var)` - Maximum value
- `(sample ?var)` - Sample value

## Time Travel Queries

Query historical data using time specifiers in `from`:

**Transaction Number:**

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

**ISO Timestamp:**

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

**Commit SHA:**

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

**Multiple Ledgers at Different Times:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger1:main@t:100", "ledger2:main@t:200"],
  "select": ["?data"],
  "where": [
    { "@id": "?entity", "ex:data": "?data" }
  ]
}
```

## History Queries

History queries let you see all changes (assertions and retractions) within a time range. Specify the range using two time-specced endpoints in the `from` clause:

### Time Range Syntax

```json
{
  "from": ["ledger:main@t:1", "ledger:main@t:latest"]
}
```

### Binding Transaction Metadata

Use `@t` and `@op` annotations on value objects to capture metadata:

- **@t** - Binds the transaction time when the fact was asserted/retracted
- **@op** - Binds the operation type: `"assert"` or `"retract"`

**Entity History:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?name", "?age", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } },
    { "@id": "ex:alice", "ex:age": "?age" }
  ],
  "orderBy": "?t"
}
```

**Property-Specific History:**

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

**Time Range with ISO:**

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

**Filter by Operation:**

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

**All Properties History:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["ledger:main@t:1", "ledger:main@t:latest"],
  "select": ["?property", "?value", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "?property": { "@value": "?value", "@t": "?t", "@op": "?op" } }
  ],
  "orderBy": "?t"
}
```

## Virtual Graph Queries

Query virtual graphs using the same syntax:

**BM25 Search:**

```json
{
  "@context": {
    "bm25": "http://ns.flur.ee/bm25#"
  },
  "from": "products-search:main@t:1000",
  "select": ["?product", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" }
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

**Vector Similarity:**

```json
{
  "@context": {
    "vector": "http://ns.flur.ee/vector#"
  },
  "from": "documents-vector:main",
  "select": ["?document", "?similarity"],
  "where": [
    { "@id": "?document", "vector:similar": "machine learning" },
    { "@id": "?document", "vector:score": "?similarity" }
  ],
  "orderBy": [["desc", "?similarity"]],
  "limit": 5
}
```

## Complete Examples

### Simple Select Query

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?name", "?age"],
  "where": [
    {
      "@id": "?person",
      "@type": "ex:User",
      "ex:name": "?name",
      "ex:age": "?age"
    },
    ["filter", "(> ?age 18)"]
  ],
  "orderBy": ["?name"],
  "limit": 10
}
```

### Complex Query with Joins

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?person", "?friend", "?friendName"],
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    { "@id": "?person", "ex:friend": "?friend" },
    { "@id": "?friend", "ex:name": "?friendName" },
    ["filter", "(= ?name \"Alice\")"]
  ]
}
```

### Aggregation Query

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "select": ["?category", ["count", "?product"], ["avg", "?price"]],
  "groupBy": ["?category"],
  "having": [["filter", "(> (count ?product) 5)"]],
  "where": [
    { "@id": "?product", "ex:category": "?category", "ex:price": "?price" }
  ],
  "orderBy": [["desc", ["count", "?product"]]]
}
```

## Best Practices

1. **Always Provide @context**: Makes queries readable and maintainable
2. **Use Specific Patterns**: More specific patterns are more efficient
3. **Limit Result Sets**: Use `limit` for large result sets
4. **Optimize Filters**: Place filters early in `where` clauses
5. **Use Time Specifiers**: Use `@t:` when transaction numbers are known (fastest)
6. **Virtual Graph Selection**: Choose appropriate virtual graphs for query patterns

## Related Documentation

- [SPARQL](sparql.md): SPARQL query language
- [Time Travel](../concepts/time-travel.md): Historical queries
- [Virtual Graphs](../concepts/virtual-graphs.md): Virtual graph queries
- [Output Formats](output-formats.md): Query result formats
