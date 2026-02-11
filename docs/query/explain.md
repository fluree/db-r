# Explain Plans

Explain plans provide insight into how queries are executed, helping you understand and optimize query performance.

## Overview

Explain plans show:
- Query execution strategy
- Index usage
- Join order
- Filter application
- Estimated costs

## Requesting Explain Plans

### JSON-LD Query

Add `"explain": true` to your query:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "explain": true
}
```

### SPARQL

Use `EXPLAIN` keyword:

```sparql
PREFIX ex: <http://example.org/ns/>

EXPLAIN
SELECT ?name
WHERE {
  ?person ex:name ?name .
}
```

## Explain Plan Structure

### Execution Plan

Shows the query execution plan:

```json
{
  "plan": {
    "type": "select",
    "children": [
      {
        "type": "scan",
        "index": "spot",
        "pattern": ["?person", "ex:name", "?name"]
      }
    ]
  }
}
```

### Index Usage

Shows which indexes are used:

```json
{
  "indexes": [
    {
      "type": "spot",
      "pattern": ["?person", "ex:name", "?name"],
      "estimatedRows": 1000
    }
  ]
}
```

### Join Order

Shows join execution order:

```json
{
  "joins": [
    {
      "left": "scan1",
      "right": "scan2",
      "joinKey": "?person",
      "type": "hash"
    }
  ]
}
```

## Understanding Explain Plans

### Scan Operations

Identify scan operations and their indexes:

- **SPOT**: Subject-Predicate-Object-Time index
- **PSOT**: Predicate-Subject-Object-Time index
- **POST**: Predicate-Object-Subject-Time index
- **OPST**: Object-Predicate-Subject-Time index
- **TSPO**: Time-Subject-Predicate-Object index

### Join Strategies

Understand join strategies:

- **Hash Join**: Build hash table from one side
- **Nested Loop**: Nested iteration
- **Merge Join**: Merge sorted inputs

### Filter Application

Filters are automatically optimized by the query engine:

- **Dependency-Based Placement**: Filters are applied as soon as all their required variables are bound, regardless of where they appear in the query
- **Index Pushdown**: Range-safe filters (comparisons on indexed properties) are pushed down to the index scan
- **Inline Evaluation**: Filters are evaluated during scan/join operations when possible, reducing operator overhead

## Optimization Tips

### Index Selection

Ensure queries use appropriate indexes:

```json
{
  "plan": {
    "scans": [
      {
        "index": "spot",
        "pattern": ["?person", "ex:name", "?name"],
        "efficient": true
      }
    ]
  }
}
```

### Join Order

Optimize join order for better performance:

```json
{
  "plan": {
    "joins": [
      {
        "order": ["scan1", "scan2"],
        "estimatedCost": 1000
      }
    ]
  }
}
```

### Filter Pushdown

Ensure filters are pushed down:

```json
{
  "plan": {
    "filters": [
      {
        "location": "index",
        "filter": "(> ?age 18)",
        "efficient": true
      }
    ]
  }
}
```

## Best Practices

1. **Review Plans**: Regularly review explain plans for optimization opportunities
2. **Index Usage**: Ensure queries use appropriate indexes
3. **Join Order**: Optimize join order for better performance
4. **Filter Placement**: Ensure filters are applied early

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Indexing and Search](../indexing-and-search/README.md): Index details
