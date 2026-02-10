# Indexing and Search

Fluree provides powerful indexing and search capabilities beyond standard graph queries. This section covers background indexing, full-text search, and vector similarity search.

## Index Types

### [Background Indexing](background-indexing.md)

Core database indexing for query performance:
- SPOT, POST, OPST, PSOT indexes
- Automatic index maintenance
- Indexing configuration
- Performance tuning
- Monitoring and metrics

### [Reindex API](reindex.md)

Manual index rebuilding for recovery and maintenance:
- Memory-bounded batched processing
- Checkpointing for resumable operations
- Progress monitoring with callbacks
- Resume after interruption
- Index configuration options

### [BM25 Full-Text Search](bm25.md)

Integrated full-text search using BM25 ranking:
- Creating BM25 indexes
- Configuring fields and weights
- Executing text searches
- Ranking and relevance
- Real-time index updates

### [Vector Search](vector-search.md)

Approximate nearest neighbor (ANN) search for embeddings:
- Vector index configuration
- Embedded HNSW indexes (in-process) or remote via dedicated search service
- Embedding storage with `@vector` datatype (resolves to `https://ns.flur.ee/db#embeddingVector`)
- Similarity queries via `f:*` syntax
- Deployment modes (embedded / remote)
- Use cases (semantic search, recommendations)

### [Geospatial](geospatial.md)

Geographic point data with native binary encoding:
- `geo:wktLiteral` datatype support (OGC GeoSPARQL)
- Automatic POINT geometry detection and optimization
- Packed 60-bit lat/lng encoding (~0.3mm precision)
- Foundation for proximity queries (latitude-band index scans)

## Design docs

- [Search Service Protocol (BM25 + Vector)](../design/SEARCH_SERVICE_PROTOCOL.md) - Unified embedded/remote contract and implementation plan.

## Indexing Architecture

Fluree maintains multiple index types for different query patterns:

**Core Indexes (automatic):**
- SPOT: Subject-Predicate-Object-Time
- POST: Predicate-Object-Subject-Time
- OPST: Object-Predicate-Subject-Time
- PSOT: Predicate-Subject-Object-Time

**Graph Source Indexes (explicit):**
- BM25: Full-text search indexes
- Vector: Embedding similarity indexes
- R2RML: Relational database views
- Iceberg: Data lake integrations

## Background Indexing

Core database indexing happens automatically:

```text
Transaction → Commit → Background Indexer → Index Published
```

**Process:**
1. Transaction committed (t assigned)
2. Commit published to nameservice
3. Background indexer detects new commit
4. Indexes updated (SPOT, POST, OPST, PSOT)
5. Index snapshot published

**Novelty Layer:**
- Gap between latest commit and latest index
- Queries combine indexed data + novelty
- Monitored via `commit_t - index_t`

See [Background Indexing](background-indexing.md) for details.

## Full-Text Search (BM25)

BM25 provides ranked full-text search:

**Creating Index:**
```json
{
  "name": "products-search",
  "source": "mydb:main",
  "fields": [
    { "predicate": "schema:name", "weight": 2.0 },
    { "predicate": "schema:description", "weight": 1.0 }
  ]
}
```

**Searching:**
```json
{
  "from": "products-search:main",
  "select": ["?product", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop computer" },
    { "@id": "?product", "bm25:score": "?score" }
  ],
  "orderBy": ["-?score"]
}
```

See [BM25](bm25.md) for details.

## Vector Search

Similarity search using vector embeddings via HNSW indexes (embedded or remote).

**Important**: Embeddings must be stored with the vector datatype (`@type: "@vector"`, `@type: "f:embeddingVector"`, or full IRI `https://ns.flur.ee/db#embeddingVector`) to preserve array structure.

**Creating Index (Rust API):**
```rust
let config = VectorCreateConfig::new(
    "products-vector", "mydb:main", query, "ex:embedding", 384
);
fluree.create_vector_index(config).await?;
```

**Searching:**
```json
{
  "from": "mydb:main",
  "select": ["?product", "?score"],
  "where": [
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": [0.1, 0.2, ..., 0.9],
      "f:searchLimit": 10,
      "f:searchResult": {
        "f:resultId": "?product",
        "f:resultScore": "?score"
      }
    }
  ]
}
```

See [Vector Search](vector-search.md) for details.

## Index as Graph Sources

Search indexes are exposed as graph sources:

**Graph Source Names:**
- `products-search:main` - BM25 index
- `products-vector:main` - Vector index

**Query Like Regular Ledgers:**
```json
{
  "from": ["mydb:main", "products-search:main"],
  "select": ["?product", "?name", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" },
    { "@id": "?product", "schema:name": "?name" }
  ]
}
```

Combines structured data with search results.

## Index Management

### Creating Indexes

Create via API:

```bash
curl -X POST "http://localhost:8090/index/bm25?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products-search",
    "fields": [...]
  }'
```

### Updating Indexes

Indexes update automatically as data changes:
- New transactions trigger index updates
- Updates are asynchronous
- Index lag monitored separately from main index

### Deleting Indexes

```bash
curl -X DELETE "http://localhost:8090/index/bm25/products-search:main"
```

## Performance Characteristics

### BM25 Search

- **Index Build Time**: O(n) for n documents
- **Query Time**: O(log n) with inverted index
- **Space**: ~2-3x document size
- **Updates**: Incremental, O(doc size)

### Vector Search

- **Flat scan (inline functions)**: O(n) brute-force, viable up to ~100K vectors with binary indexing; binary index provides ~6x speedup over novelty-only scans and ~25x for filtered queries
- **HNSW index**: O(log n) approximate nearest neighbor, recommended for 100K+ vectors or strict latency requirements
- **Space**: ~1.5x embedding size
- **Updates**: Incremental, O(1) per vector
- See [Vector Search -- Performance and Scaling](vector-search.md#performance-and-scaling) for benchmark data and guidance on when to adopt HNSW

### Combined Queries

Combine search with graph queries:

```json
{
  "from": ["mydb:main", "products-search:main"],
  "select": ["?product", "?category"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "schema:category": "?category" }
  ]
}
```

Query optimizer handles joins efficiently.

## Use Cases

### Full-Text Search

**E-commerce Product Search:**
```json
{
  "from": "products-search:main",
  "where": [
    { "@id": "?product", "bm25:matches": "wireless headphones" }
  ],
  "orderBy": ["-?score"],
  "limit": 20
}
```

**Document Management:**
```json
{
  "from": "documents-search:main",
  "where": [
    { "@id": "?doc", "bm25:matches": "quarterly report 2024" },
    { "@id": "?doc", "ex:department": "finance" }
  ]
}
```

### Vector Similarity

**Semantic Search:**
```json
{
  "from": "articles:main",
  "values": [
    ["?queryVec"],
    [{"@value": [0.1, 0.2, 0.3], "@type": "https://ns.flur.ee/db#embeddingVector"}]
  ],
  "where": [
    {
      "f:graphSource": "articles-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 10,
      "f:searchResult": {
        "f:resultId": "?article",
        "f:resultScore": "?vecScore"
      }
    }
  ],
  "select": ["?article", "?vecScore"],
  "orderBy": [["desc", "?vecScore"]]
}
```

**Recommendation Engine:**
```json
{
  "from": "products:main",
  "where": [
    {
      "@id": "ex:product-123",
      "ex:embedding": "?queryVec"
    },
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 5,
      "f:searchResult": { "f:resultId": "?similar", "f:resultScore": "?vecScore" }
    }
  ],
  "select": ["?similar", "?vecScore"],
  "orderBy": [["desc", "?vecScore"]]
}
```

### Hybrid Search

Combine text and vector search:

```json
{
  "from": ["products-search:main", "products-vector:main"],
  "values": [
    ["?queryVec"],
    [{"@value": [0.1, 0.2, 0.3], "@type": "https://ns.flur.ee/db#embeddingVector"}]
  ],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?textScore" },
    {
      "f:graphSource": "products-vector:main",
      "f:queryVector": "?queryVec",
      "f:searchLimit": 100,
      "f:searchResult": { "f:resultId": "?product", "f:resultScore": "?vecScore" }
    }
  ],
  "bind": {
    "?finalScore": "(?textScore * 0.6) + (?vecScore * 0.4)"
  },
  "orderBy": ["-?finalScore"]
}
```

## Monitoring

### Check Index Status

```bash
curl http://localhost:8090/index/status
```

Response:
```json
{
  "indexes": [
    {
      "name": "products-search:main",
      "type": "bm25",
      "status": "ready",
      "documents": 10523,
      "last_updated": "2024-01-22T10:30:00Z",
      "lag_ms": 1500
    },
    {
      "name": "products-vector:main",
      "type": "vector",
      "status": "indexing",
      "vectors": 8934,
      "pending": 1589
    }
  ]
}
```

### Index Metrics

Track index performance:

```javascript
const metrics = await getIndexMetrics();
console.log(`BM25 index lag: ${metrics.bm25_lag_ms}ms`);
console.log(`Vector index size: ${metrics.vector_count} vectors`);
console.log(`Query latency: ${metrics.avg_query_ms}ms`);
```

## Best Practices

### 1. Choose Appropriate Index Type

- **Structured queries**: Use core graph indexes
- **Keyword search**: Use BM25
- **Semantic similarity**: Use vector search
- **Hybrid**: Combine multiple indexes

### 2. Configure Field Weights

Weight important fields higher in BM25:

```json
{
  "fields": [
    { "predicate": "schema:name", "weight": 3.0 },
    { "predicate": "schema:description", "weight": 1.0 },
    { "predicate": "schema:tags", "weight": 2.0 }
  ]
}
```

### 3. Monitor Index Lag

```javascript
setInterval(async () => {
  const status = await checkIndexStatus();
  if (status.lag_ms > 5000) {
    alert('Index lag exceeds 5 seconds');
  }
}, 30000);
```

### 4. Batch Index Updates

For bulk operations, allow time for indexing:

```javascript
for (const batch of batches) {
  await transact(batch);
  await waitForIndexing();
}
```

### 5. Use Appropriate Limits

Limit results for performance:

```json
{
  "where": [
    { "@id": "?doc", "bm25:matches": "search query" }
  ],
  "limit": 100
}
```

## Related Documentation

- [Background Indexing](background-indexing.md) - Core index details
- [BM25](bm25.md) - Full-text search
- [Vector Search](vector-search.md) - Similarity search
- [Graph Sources](../graph-sources/README.md) - Graph source concepts
- [Query](../query/README.md) - Query syntax
