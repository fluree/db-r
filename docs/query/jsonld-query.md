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

### Property Paths

Property paths enable transitive traversal of predicates, following chains of relationships across multiple hops. Define a path alias in `@context` using `@path`, then use the alias as a key in WHERE node-maps.

**Defining a Path Alias:**

Add a term definition with `@path` to your `@context`. The value of `@path` can be a string (SPARQL property path syntax) or an array (S-expression form).

**String Form (SPARQL syntax):**

```json
{
  "@context": {
    "ex": "http://example.org/",
    "knowsPlus": { "@path": "ex:knows+" }
  },
  "select": ["?who"],
  "where": [
    { "@id": "ex:alice", "knowsPlus": "?who" }
  ]
}
```

This returns all entities reachable from `ex:alice` by following one or more `ex:knows` edges transitively.

**Array Form (S-expression):**

```json
{
  "@context": {
    "ex": "http://example.org/",
    "knowsPlus": { "@path": ["+", "ex:knows"] }
  },
  "select": ["?who"],
  "where": [
    { "@id": "ex:alice", "knowsPlus": "?who" }
  ]
}
```

The array form uses the operator as the first element followed by its operands.

**Supported Operators:**

| Operator | String syntax | Array syntax | Description |
|----------|--------------|--------------|-------------|
| One or more | `ex:p+` | `["+", "ex:p"]` | Transitive closure (1+ hops) |
| Zero or more | `ex:p*` | `["*", "ex:p"]` | Reflexive transitive closure (0+ hops) |
| Inverse | `^ex:p` | `["^", "ex:p"]` | Traverse predicate in reverse direction |
| Alternative | <code>ex:a&#124;ex:b</code> | <code>["&#124;", "ex:a", "ex:b"]</code> | Match any of several predicates |
| Sequence | `ex:a/ex:b` | `["/", "ex:a", "ex:b"]` | Follow a chain of predicates (property chain) |

Zero-or-more (`*`) includes the starting node itself in the results (zero hops).

Sequence (`/`) compiles into a chain of triple patterns joined by internal
intermediate variables. Each step must be a simple predicate or an inverse simple
predicate (`^ex:p`). For example, `"ex:friend/ex:name"` matches paths where
subject has a `ex:friend` whose `ex:name` is the result.

**Parsed but Not Yet Supported:**

The following operators are recognized by the parser but currently rejected (not yet supported for execution):

| Operator | String syntax | Array syntax |
|----------|--------------|--------------|
| Zero or one | `ex:p?` | `["?", "ex:p"]` |

**Subject and Object Variables:**

Path aliases work with variables on either side:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "knowsPlus": { "@path": "ex:knows+" }
  },
  "select": ["?x", "?y"],
  "where": [
    { "@id": "?x", "knowsPlus": "?y" }
  ]
}
```

This returns all pairs `(?x, ?y)` where `?y` is transitively reachable from `?x` via `ex:knows`.

**Fixed Subject or Object:**

You can also fix one end to an IRI:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "knowsPlus": { "@path": "ex:knows+" }
  },
  "select": ["?who"],
  "where": [
    { "@id": "?who", "knowsPlus": { "@id": "ex:bob" } }
  ]
}
```

This finds all entities that can reach `ex:bob` through one or more `ex:knows` hops.

**Inverse Example:**

Find entities that know `ex:bob` (traverse `ex:knows` in reverse):

```json
{
  "@context": {
    "ex": "http://example.org/",
    "knownBy": { "@path": "^ex:knows" }
  },
  "select": ["?who"],
  "where": [
    { "@id": "ex:bob", "knownBy": "?who" }
  ]
}
```

**Alternative Example:**

Match entities connected by either `ex:knows` or `ex:likes`:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "connected": { "@path": "ex:knows|ex:likes" }
  },
  "select": ["?who"],
  "where": [
    { "@id": "ex:alice", "connected": "?who" }
  ]
}
```

Inverse can also be applied to complex paths (sequences and alternatives):

- `^(ex:friend/ex:name)` — inverse of a sequence: reverses the step order and inverts each step, producing `(^ex:name)/(^ex:friend)`
- `^(ex:name|ex:nick)` — inverse of an alternative: distributes the inverse into each branch, producing `(^ex:name)|(^ex:nick)`
- Double inverse cancels: `^(^ex:p)` simplifies to `ex:p`

Array form examples:

```json
{ "@path": ["^", ["/", "ex:friend", "ex:name"]] }
{ "@path": ["^", ["|", "ex:name", "ex:nick"]] }
```

Inverse is supported inside alternative branches (e.g. `ex:knows|^ex:knows` matches both directions of the `ex:knows` predicate).

Alternative branches can also be sequence chains. For example, `ex:friend/ex:name|ex:colleague/ex:name` returns the name of a friend OR the name of a colleague:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "contactName": { "@path": "ex:friend/ex:name|ex:colleague/ex:name" }
  },
  "select": ["?name"],
  "where": [
    { "@id": "ex:alice", "contactName": "?name" }
  ]
}
```

Branches can freely mix simple predicates, inverse predicates, and sequence chains (e.g. `ex:name|ex:friend/ex:name|^ex:colleague`).

Alternative uses UNION semantics (bag, not set): when multiple branches match the same `(subject, object)` pair, duplicate solutions are produced. Use `selectDistinct` if set semantics are needed.

**Sequence (Property Chain) Example:**

Follow a chain of predicates. The string form uses `/` to separate steps:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "friendName": { "@path": "ex:friend/ex:name" }
  },
  "select": ["?person", "?name"],
  "where": [
    { "@id": "?person", "friendName": "?name" }
  ]
}
```

The array form uses `"/"` as the operator:

```json
{ "@path": ["/", "ex:friend", "ex:name"] }
```

Sequence steps can include inverse predicates. For example, `"^ex:parent/ex:name"` traverses the `ex:parent` link backwards, then follows `ex:name`:

```json
{ "@path": "^ex:parent/ex:name" }
```

Longer chains are supported: `"ex:friend/ex:address/ex:city"` follows three hops.

Sequence steps can also be alternatives. For example, `"ex:friend/(ex:name|ex:nick)"` distributes the alternative into a union of chains (`ex:friend/ex:name` and `ex:friend/ex:nick`):

```json
{ "@path": "ex:friend/(ex:name|ex:nick)" }
```

Array form:

```json
{ "@path": ["/", "ex:friend", ["|", "ex:name", "ex:nick"]] }
```

Multiple alternative steps are supported: `"(ex:a|ex:b)/(ex:c|ex:d)"` expands to 4 chains. A safety limit of 64 expanded chains is enforced to prevent combinatorial explosion.

Each step must be a simple predicate (`ex:p`), inverse simple predicate (`^ex:p`), or an alternative of simple predicates (`(ex:a|ex:b)`). Transitive (`+`/`*`) and nested sequence modifiers are not allowed inside sequence steps.

**Rules:**

- `@path` and `@reverse` are mutually exclusive on the same term definition (produces an error).
- `@path` and `@id` may coexist on the same term definition; when the alias key appears in a WHERE node-map, the `@path` definition is used.
- Cycle detection is built in: transitive traversal terminates when it encounters a node already visited.
- Variable names starting with `?__` are reserved for internal use (e.g., intermediate join variables generated by sequence paths). These variables will not appear in wildcard (`select: "*"`) output.

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

History queries let you see all changes (assertions and retractions) within a time range. Specify the range using `from` and `to` keys with time-specced endpoints:

### Time Range Syntax

```json
{
  "from": "ledger:main@t:1",
  "to": "ledger:main@t:latest"
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
  "from": "ledger:main@t:1",
  "to": "ledger:main@t:latest",
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
  "from": "ledger:main@t:1",
  "to": "ledger:main@t:100",
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
  "from": "ledger:main@iso:2024-01-01T00:00:00Z",
  "to": "ledger:main@iso:2024-12-31T23:59:59Z",
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
  "from": "ledger:main@t:1",
  "to": "ledger:main@t:latest",
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
  "from": "ledger:main@t:1",
  "to": "ledger:main@t:latest",
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
