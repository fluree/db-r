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

### Named graph sources (datasets)

In SPARQL, `FROM NAMED` identifies **named graphs in the dataset**. In Fluree, these are often *graph sources* such as:
- another ledger (federation / multi-ledger queries), or
- a virtual graph (search, tabular mapping, etc.).

Query across multiple named graph sources:

**JSON-LD Query:**

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from-named": ["mydb:main", "otherdb:main"],
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

### Ledger named graph: `txn-meta`

Fluree provides a built-in named graph inside each ledger for transactional / commit metadata: **`txn-meta`**.

Use the `#txn-meta` fragment on a ledger reference:
- `mydb:main#txn-meta`
- `mydb:main@t:100#txn-meta` (time pinned)

**JSON-LD Query (txn-meta as the default graph):**

```json
{
  "@context": {
    "f": "https://ns.flur.ee/ledger#",
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main#txn-meta",
  "select": ["?commit", "?t", "?machine"],
  "where": [
    { "@id": "?commit", "f:t": "?t" },
    { "@id": "?commit", "ex:machine": "?machine" }
  ]
}
```

**SPARQL Query:**

```sparql
PREFIX f: <https://ns.flur.ee/ledger#>
PREFIX ex: <http://example.org/ns/>

SELECT ?commit ?t ?machine
FROM <mydb:main#txn-meta>
WHERE {
  ?commit f:t ?t .
  OPTIONAL { ?commit ex:machine ?machine }
}
```

### User-Defined Named Graphs

Fluree supports user-defined named graphs ingested via TriG format. These graphs are queryable using the structured `from` object syntax with a `graph` field.

**Ingesting data with named graphs (TriG):**

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  -d '@prefix ex: <http://example.org/ns/> .

      GRAPH <http://example.org/graphs/products> {
          ex:widget ex:name "Widget" ;
                    ex:price "29.99"^^xsd:decimal .
      }'
```

**Querying the named graph (JSON-LD):**

Use the structured `from` object with a `graph` field specifying the graph IRI:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": {
    "@id": "mydb:main",
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [
    { "@id": "?product", "ex:name": "?name" },
    { "@id": "?product", "ex:price": "?price" }
  ]
}
```

**With time-travel:**

```json
{
  "from": {
    "@id": "mydb:main",
    "t": 100,
    "graph": "http://example.org/graphs/products"
  },
  "select": ["?name", "?price"],
  "where": [...]
}
```

**Combining multiple graphs (JSON-LD):**

Query across the default graph and user-defined named graphs:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "from": "mydb:main",
  "from-named": [
    {
      "@id": "mydb:main",
      "alias": "products",
      "graph": "http://example.org/graphs/products"
    }
  ],
  "select": ["?company", "?product", "?price"],
  "where": [
    { "@id": "?company", "@type": "ex:Company" },
    ["graph", "products", { "@id": "?product", "ex:name": "?productName", "ex:price": "?price" }]
  ]
}
```

**Notes:**
- Named graphs are queryable after indexing completes
- The `graph` field accepts the full graph IRI (no URL-encoding required)
- Time-travel is specified via the `t`, `iso`, or `sha` field in the object form
- Use `alias` to create a short reference name for use in GRAPH patterns

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
PREFIX f: <https://ns.flur.ee/ledger#>
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?commit ?t
FROM <mydb:main>
FROM NAMED <mydb:main#txn-meta>
WHERE {
  ?person ex:name ?name .
  GRAPH <mydb:main#txn-meta> {
    ?commit f:t ?t .
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

### SERVICE for Cross-Ledger Queries

Use SPARQL SERVICE to explicitly target specific ledgers within a query:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?customerName ?productName ?quantity
FROM <customers:main>
FROM NAMED <orders:main>
FROM NAMED <products:main>
WHERE {
  # Get customer from default graph
  ?customer ex:name ?customerName .

  # Get orders from orders ledger
  SERVICE <fluree:ledger:orders:main> {
    ?order ex:customer ?customer ;
           ex:product ?product ;
           ex:quantity ?quantity .
  }

  # Get product details from products ledger
  SERVICE <fluree:ledger:products:main> {
    ?product ex:name ?productName .
  }
}
```

SERVICE provides explicit control over which ledger each pattern executes against, enabling complex cross-ledger joins with clear data provenance.

See [SPARQL Service Queries](sparql.md#service-queries) for full documentation.

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
