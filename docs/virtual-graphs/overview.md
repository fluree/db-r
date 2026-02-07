# Graph Sources Overview (Virtual Graphs)

Graph sources enable querying specialized indexes and external data sources using the same query interface as regular Fluree ledgers. This document provides a comprehensive overview of graph source architecture and capabilities.

## Concept

A **graph source** is anything you can address by a graph name/IRI and query as part of a single execution. Some graph sources are ledger-backed RDF graphs; others are backed by different systems optimized for specific query patterns.

**Regular Ledger:**
- Stored as RDF triples
- Indexed with SPOT, POST, OPST, PSOT
- Optimized for graph traversal

**Non-ledger Graph Source (“virtual graph” in some code/docs):**
- Stored in specialized format
- Custom indexing for specific queries
- Optimized for particular use cases

Both are queried using the same SPARQL or JSON-LD Query syntax.

## Architecture

### Components

```text
┌─────────────────────────────────────────┐
│         Fluree Query Engine             │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┴──────────┐
      │                      │
┌─────▼──────┐      ┌───────▼────────┐
│  Regular   │      │    Virtual     │
│  Ledgers   │      │    Graphs      │
└─────┬──────┘      └───────┬────────┘
      │                     │
      │             ┌───────┴────────┐
      │             │                │
┌─────▼──────┐ ┌───▼───┐     ┌─────▼──────┐
│ RDF Triple │ │ BM25  │     │  usearch   │
│   Store    │ │ Index │     │  Vector    │
└────────────┘ └───────┘     └────────────┘
```

### Graph Source Registry (Nameservice)

Non-ledger graph sources (sometimes called “virtual graphs”) are registered in nameservice:

```json
{
  "alias": "products-search:main",
  "type": "virtual-graph",
  "backend": "bm25",
  "source": "products:main",
  "config": {
    "fields": [...]
  },
  "status": "ready",
  "last_sync": "2024-01-22T10:30:00Z"
}
```

## Virtual Graph Types

### 1. BM25 Full-Text Search

**Backend:** Inverted text index

**Purpose:** Keyword search with relevance ranking

**Configuration:**
```json
{
  "type": "bm25",
  "source": "products:main",
  "fields": [
    { "predicate": "schema:name", "weight": 2.0 },
    { "predicate": "schema:description", "weight": 1.0 }
  ]
}
```

**Query:**
```json
{
  "from": "products-search:main",
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" }
  ]
}
```

### 2. Vector Similarity

**Backend:** HNSW index (embedded or remote)

**Purpose:** Semantic search using embeddings

**Configuration:**
```json
{
  "type": "vector",
  "source": "products:main",
  "embedding_property": "ex:embedding",
  "dimensions": 384,
  "metric": "cosine"
}
```

**Query:**
```json
{
  "from": "mydb:main",
  "where": [
    {
      "idx:graph": "products-vector:main",
      "idx:vector": [0.1, 0.2, ...],
      "idx:metric": "cosine",
      "idx:limit": 10,
      "idx:result": {
        "idx:id": "?product",
        "idx:score": "?score"
      }
    }
  ],
  "select": ["?product", "?score"]
}
```

### 3. Apache Iceberg

**Backend:** Iceberg tables / Parquet files

**Purpose:** Analytics on data lake

**Configuration:**
```json
{
  "type": "iceberg",
  "catalog": "glue",
  "warehouse": "s3://data-warehouse/",
  "table": "sales.orders",
  "mapping": {
    "order_id": "ex:orderId",
    "customer_id": "ex:customerId",
    "total": "ex:total"
  }
}
```

**Query:**
```json
{
  "from": "warehouse-orders:main",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" }
  ]
}
```

### 4. R2RML Relational Mapping

**Backend:** SQL databases

**Purpose:** Query relational data as RDF

**Configuration:**
```json
{
  "type": "r2rml",
  "database": "postgresql://localhost/mydb",
  "mapping": "r2rml-mapping.ttl"
}
```

**Query:**
```json
{
  "from": "sql-customers:main",
  "select": ["?name", "?email"],
  "where": [
    { "@id": "?customer", "schema:name": "?name" },
    { "@id": "?customer", "schema:email": "?email" }
  ]
}
```

## Creating Virtual Graphs

### Via HTTP API

```bash
curl -X POST http://localhost:8090/virtual-graph \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products-search",
    "type": "bm25",
    "source": "products:main",
    "config": {
      "fields": [...]
    }
  }'
```

### Via Transaction

Define virtual graph as RDF:

```json
{
  "@graph": [
    {
      "@id": "vg:products-search",
      "@type": "f:VirtualGraph",
      "f:type": "bm25",
      "f:source": "products:main",
      "f:config": {
        "f:fields": [...]
      }
    }
  ]
}
```

## Querying Virtual Graphs

### Single Virtual Graph

Query one virtual graph:

```json
{
  "from": "products-search:main",
  "select": ["?product"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" }
  ]
}
```

### Multiple Virtual Graphs

Combine multiple virtual graphs:

```json
{
  "from": ["products-search:main", "products-vector:main"],
  "select": ["?product", "?textScore", "?vecScore"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?textScore" },
    { "@id": "?product", "vector:similar": {...} },
    { "@id": "?product", "vector:similarity": "?vecScore" }
  ]
}
```

### Virtual + Regular Graphs

Combine virtual and regular ledgers:

```json
{
  "from": ["products:main", "products-search:main"],
  "select": ["?product", "?name", "?price", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" },
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" }
  ]
}
```

## Synchronization

### Source Tracking

Virtual graphs track their source ledger:

```text
Source: products:main @ t=150
Virtual Graph: products-search:main @ source_t=150
```

### Update Modes

**Real-Time:**
- Updates immediately as source changes
- Low latency
- Higher overhead

**Batch:**
- Updates periodically
- Higher latency
- Lower overhead

**Manual:**
- Updates on demand
- Full control
- Requires manual triggering

### Checking Sync Status

```bash
curl http://localhost:8090/virtual-graph/products-search:main/status
```

Response:
```json
{
  "name": "products-search:main",
  "source": "products:main",
  "source_t": 150,
  "virtual_t": 148,
  "lag": 2,
  "last_sync": "2024-01-22T10:30:00Z",
  "status": "syncing"
}
```

## Query Execution

### Query Planning

Query planner handles virtual graphs:

1. **Parse Query:** Extract graph patterns
2. **Route Subqueries:** Identify which graphs handle which patterns
3. **Execute Subqueries:** Run against appropriate backends
4. **Join Results:** Combine results from multiple graphs
5. **Apply Filters:** Final filtering and sorting

### Example Execution

Query:
```json
{
  "from": ["products:main", "products-search:main"],
  "where": [
    { "@id": "?p", "bm25:matches": "laptop" },
    { "@id": "?p", "schema:price": "?price" }
  ],
  "filter": "?price < 1000"
}
```

Execution Plan:
```text
1. Execute on products-search:main:
   SELECT ?p WHERE { ?p bm25:matches "laptop" }
   → Result: [ex:p1, ex:p2, ex:p3, ...]

2. Execute on products:main:
   SELECT ?p ?price WHERE {
     VALUES ?p { ex:p1 ex:p2 ex:p3 ... }
     ?p schema:price ?price
   }
   → Result: [(ex:p1, 899), (ex:p2, 1200), ...]

3. Join and filter:
   ?price < 1000
   → Result: [(ex:p1, 899)]
```

## Performance Characteristics

### BM25 Virtual Graphs

- **Index Build:** O(n × avg_doc_length)
- **Query:** O(log n) with inverted index
- **Space:** 2-3× source data
- **Update:** Incremental, O(doc_size)

### Vector Virtual Graphs

- **Index Build:** O(n log n) for HNSW
- **Query:** O(log n) approximate
- **Space:** 1.5× embedding size
- **Update:** Incremental, O(1)

### Iceberg Virtual Graphs

- **Index Build:** No index (direct file access)
- **Query:** O(partitions scanned)
- **Space:** Zero overhead (uses Parquet files)
- **Update:** Batch-oriented

### R2RML Virtual Graphs

- **Index Build:** No index (SQL queries)
- **Query:** O(SQL complexity)
- **Space:** Zero overhead (uses SQL database)
- **Update:** Real-time (direct DB access)

## Best Practices

### 1. Choose Appropriate Type

Match virtual graph type to use case:
- **Keyword search** → BM25
- **Semantic search** → Vector
- **Analytics** → Iceberg
- **Existing system integration** → R2RML

### 2. Monitor Synchronization

Check sync lag regularly:

```javascript
setInterval(async () => {
  const status = await getVirtualGraphStatus('products-search:main');
  if (status.lag > 10) {
    console.warn(`Virtual graph lag: ${status.lag} transactions`);
  }
}, 60000);
```

### 3. Filter in Virtual Graphs

Push filters to virtual graphs when possible:

Good:
```json
{
  "from": ["products:main", "products-search:main"],
  "where": [
    { "@id": "?p", "bm25:matches": "laptop" },  // Filters early
    { "@id": "?p", "schema:name": "?name" }
  ]
}
```

Bad:
```json
{
  "from": ["products:main", "products-search:main"],
  "where": [
    { "@id": "?p", "schema:name": "?name" },
    { "@id": "?p", "bm25:matches": "laptop" }  // Too late
  ]
}
```

### 4. Use Explain Plans

Understand query execution:

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Explain: true" \
  -d '{...}'
```

### 5. Limit Results

Always use LIMIT with virtual graphs:

```json
{
  "where": [...],
  "limit": 100
}
```

## Troubleshooting

### High Sync Lag

**Symptom:** `lag` increasing

**Causes:**
- Source ledger write rate too high
- Virtual graph indexing too slow
- Resource constraints

**Solutions:**
- Increase indexing resources
- Batch updates
- Use manual sync mode

### Query Performance Issues

**Symptom:** Slow queries combining virtual graphs

**Solutions:**
1. Check explain plan
2. Add filters to reduce intermediate results
3. Ensure virtual graph is synced
4. Consider query rewrite

### Missing Results

**Symptom:** Expected results not returned

**Causes:**
- Virtual graph not synced
- Mapping misconfiguration
- Filter too restrictive

**Solutions:**
- Check sync status
- Verify mapping configuration
- Test subqueries independently

## Related Documentation

- [BM25 Virtual Graph](bm25-virtual-graph.md) - Full-text search
- [Iceberg](iceberg.md) - Data lake integration
- [R2RML](r2rml.md) - Relational mapping
- [BM25 Indexing](../indexing-and-search/bm25.md) - BM25 details
- [Vector Search](../indexing-and-search/vector-search.md) - Vector details
- [Query Datasets](../query/datasets.md) - Multi-graph queries
