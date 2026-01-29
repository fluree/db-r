# Datasets and Multi-Graph Execution

Fluree supports SPARQL datasets, enabling queries across multiple graphs and ledgers simultaneously. This provides powerful data integration capabilities for complex applications.

## SPARQL Datasets

A **dataset** in SPARQL is a collection of graphs used for query execution:

- **Default Graph**: The primary graph for triple patterns without GRAPH clauses
- **Named Graphs**: Additional graphs identified by IRIs, accessible via GRAPH clauses

## FROM Clauses

### Single Default Graph

Specify a single default graph:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "mydb:main",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
}
```

### Multiple Default Graphs

Specify multiple default graphs (union semantics):

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["mydb:main", "otherdb:main"],
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
FROM <otherdb:main>
WHERE {
  ?person ex:name ?name .
}
```

## FROM NAMED Clauses

### Named Graphs

Query specific named graphs:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "fromNamed": ["mydb:main", "otherdb:main"],
  "select": ["?graph", "?name"],
  "where": [
    ["graph", "?graph", { "@id": "?person", "ex:name": "?name" }]
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?graph ?name
FROM NAMED <mydb:main>
FROM NAMED <otherdb:main>
WHERE {
  GRAPH ?graph {
    ?person ex:name ?name .
  }
}
```

### Specific Named Graph

Query a specific named graph:

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM NAMED <mydb:main>
WHERE {
  GRAPH <mydb:main> {
    ?person ex:name ?name .
  }
}
```

## Multi-Ledger Queries

Query across different ledgers:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["customers:main", "orders:main"],
  "select": ["?customer", "?order"],
  "where": [
    { "@id": "?customer", "ex:name": "Alice" },
    { "@id": "?order", "ex:customer": "?customer" }
  ]
}
```

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?order
FROM <customers:main>
FROM <orders:main>
WHERE {
  ?customer ex:name "Alice" .
  ?order ex:customer ?customer .
}
```

## Time-Aware Datasets

Query graphs at different time points:

**JSON-LD Query:**

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

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?data
FROM <ledger1:main@t:100>
FROM <ledger2:main@t:200>
WHERE {
  ?entity ex:data ?data .
}
```

## Graph Patterns

### Default Graph Only

Query only the default graph:

**SPARQL:**

```sparql
SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
  # Matches triples in default graph only
}
```

### Named Graph Only

Query only named graphs:

**SPARQL:**

```sparql
SELECT ?name
FROM NAMED <mydb:main>
WHERE {
  GRAPH <mydb:main> {
    ?person ex:name ?name .
  }
}
```

### Mixed Patterns

Combine default and named graph patterns:

**SPARQL:**

```sparql
SELECT ?name ?metadata
FROM <mydb:main>
FROM NAMED <mydb:metadata>
WHERE {
  ?person ex:name ?name .
  GRAPH <mydb:metadata> {
    ?person ex:created ?metadata .
  }
}
```

## Use Cases

### Data Integration

Combine data from multiple sources:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": ["customers:main", "products:main", "orders:main"],
  "select": ["?customer", "?product", "?order"],
  "where": [
    { "@id": "?customer", "ex:name": "Alice" },
    { "@id": "?order", "ex:customer": "?customer" },
    { "@id": "?order", "ex:product": "?product" }
  ]
}
```

### Cross-Ledger Joins

Join data across different ledgers:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customer ?order ?product
FROM <customers:main>
FROM <orders:main>
FROM <products:main>
WHERE {
  ?customer ex:name "Alice" .
  ?order ex:customer ?customer .
  ?order ex:product ?product .
}
```

### Time-Consistent Queries

Query multiple ledgers at the same point in time:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": [
    "products:main@t:1000",
    "inventory:main@t:1000",
    "pricing:main@t:1000"
  ],
  "select": ["?product", "?stock", "?price"],
  "where": [
    { "@id": "?product", "ex:stockLevel": "?stock" },
    { "@id": "?product", "ex:price": "?price" }
  ]
}
```

## Best Practices

1. **Consistent Time Points**: Use the same time specifier for all graphs in a query
2. **Graph Selection**: Use FROM NAMED when you need to identify the source graph
3. **Performance**: Queries across multiple ledgers may be slower
4. **Data Locality**: Consider data locality when designing multi-ledger queries

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Time Travel](../concepts/time-travel.md): Historical queries
- [Datasets and Named Graphs](../concepts/datasets-and-named-graphs.md): Concept documentation
