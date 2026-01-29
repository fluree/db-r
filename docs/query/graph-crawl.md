# Graph Crawl

Graph crawl enables recursive traversal of graph relationships, following links between entities to discover connected data.

## Overview

Graph crawl queries traverse relationships in the graph, following links from one entity to another. This is useful for:
- Discovering connected entities
- Finding paths between entities
- Exploring graph neighborhoods
- Recursive data extraction

## Basic Graph Crawl

### Follow Single Relationship

Follow a single relationship type:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?person", "?friend"],
  "where": [
    { "@id": "ex:alice", "ex:friend": "?friend" },
    { "@id": "?friend", "ex:name": "?name" }
  ]
}
```

### Recursive Traversal

Follow relationships recursively:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?person", "?connected"],
  "where": [
    { "@id": "ex:alice", "ex:friend+": "?connected" }
  ]
}
```

## Depth-Limited Crawl

Limit traversal depth:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?person", "?friend"],
  "where": [
    { "@id": "ex:alice", "ex:friend*": "?friend" }
  ],
  "maxDepth": 3
}
```

## Multi-Relationship Crawl

Follow multiple relationship types:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?person", "?connected"],
  "where": [
    { "@id": "ex:alice", "ex:friend|ex:colleague": "?connected" }
  ]
}
```

## Use Cases

### Friend Network

Discover friend networks:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?person", "?friend"],
  "where": [
    { "@id": "ex:alice", "ex:friend+": "?friend" }
  ]
}
```

### Organizational Hierarchy

Traverse organizational hierarchies:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?employee", "?manager"],
  "where": [
    { "@id": "ex:employee123", "ex:reportsTo+": "?manager" }
  ]
}
```

### Path Finding

Find paths between entities:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?path"],
  "where": [
    { "@id": "ex:alice", "ex:friend*": "?intermediate" },
    { "@id": "?intermediate", "ex:friend*": "ex:bob" }
  ]
}
```

## Best Practices

1. **Limit Depth**: Use depth limits to avoid infinite loops
2. **Specific Relationships**: Follow specific relationship types
3. **Cycle Detection**: Be aware of cycles in graphs
4. **Performance**: Graph crawl can be expensive for large graphs

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL property paths
- [Datasets](datasets.md): Multi-graph queries
