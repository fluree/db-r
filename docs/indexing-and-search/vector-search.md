# Vector Search

Vector search enables similarity search using embedding vectors, supporting use cases like:

- **Semantic search**: Find similar meanings, not just keywords
- **Recommendations**: Find similar products, content, users
- **Image search**: Find similar images by visual features
- **Anomaly detection**: Find unusual patterns

## Status

Vector search is implemented using embedded [usearch](https://github.com/unum-cloud/usearch) indexes following the same architecture as BM25:

- Embedded in-process HNSW indexes (no external service required)
- Remote mode via dedicated search service (`fluree-search-httpd`)
- Snapshot-based persistence with watermarks
- Incremental sync for efficient updates
- Feature-gated via `vector` feature flag

**v1 limitation**: Vector search is **head-only**. Time-travel queries (e.g. requests with `as_of_t` / `@t:`) are not supported and should return an explicit error.

## Important: Embedding Storage Format

**Embeddings MUST be stored with the `f:vector` datatype** to preserve array structure and duplicate values. Without this annotation, RDF normalization can deduplicate array elements (e.g., `[0.5, 0.5, 0.0]` becomes `[0.0, 0.5]`), breaking vector search.

### Correct Format

```json
{
  "@context": {
    "ex": "http://example.org/",
    "f": "https://ns.flur.ee/ledger#"
  },
  "@graph": [
    {
      "@id": "ex:doc1",
      "@type": "ex:Document",
      "ex:embedding": {
        "@value": [0.1, 0.2, 0.3, 0.4],
        "@type": "f:vector"
      }
    }
  ]
}
```

### Incorrect Format (will fail)

```json
{
  "@id": "ex:doc1",
  "ex:embedding": [0.1, 0.2, 0.3, 0.4]
}
```

Plain arrays are decomposed into RDF values where duplicates may be removed.

## Creating Vector Indexes

### Rust API

```rust
use fluree_db_api::{FlureeBuilder, VectorCreateConfig};
use fluree_db_query::vector::DistanceMetric;

let fluree = FlureeBuilder::memory().build_memory();

// Create indexing query to select documents with embeddings
let indexing_query = json!({
    "@context": { "ex": "http://example.org/" },
    "where": [{ "@id": "?x", "@type": "ex:Document" }],
    "select": { "?x": ["@id", "ex:embedding"] }
});

// Create vector index
let config = VectorCreateConfig::new(
    "doc-embeddings",           // index name
    "mydb:main",                // source ledger
    indexing_query,             // what to index
    "ex:embedding",             // embedding property
    768                         // dimensions
)
.with_metric(DistanceMetric::Cosine);

let result = fluree.create_vector_index(config).await?;
println!("Indexed {} vectors", result.vector_count);
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `name` | Index name (creates alias `name:branch`) | Required |
| `ledger` | Source ledger alias | Required |
| `query` | JSON-LD query selecting documents | Required |
| `embedding_property` | Property containing embeddings | Required |
| `dimensions` | Vector dimensions | Required |
| `metric` | Distance metric (Cosine, Dot, Euclidean) | Cosine |
| `connectivity` | HNSW M parameter | 16 |
| `expansion_add` | efConstruction parameter | 128 |
| `expansion_search` | efSearch parameter | 64 |

## Query Syntax

Vector search uses the `idx:*` pattern syntax in WHERE clauses:

```json
{
  "@context": { "ex": "http://example.org/" },
  "from": "mydb:main",
  "where": [
    {
      "idx:graph": "doc-embeddings:main",
      "idx:vector": [0.1, 0.2, 0.3, ...],
      "idx:metric": "cosine",
      "idx:limit": 10,
      "idx:result": {
        "idx:id": "?doc",
        "idx:score": "?score"
      }
    }
  ],
  "select": ["?doc", "?score"]
}
```

### Query Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `idx:graph` | Vector index alias | Yes |
| `idx:vector` | Query vector (array or variable) | Yes |
| `idx:metric` | Distance metric ("cosine", "dot", "euclidean") | No (uses index default) |
| `idx:limit` | Maximum results | No |
| `idx:result` | Result binding (variable or object) | Yes |
| `idx:sync` | Wait for index sync before query | No (default: false) |
| `idx:timeout` | Query timeout in ms | No |

### Result Binding

Simple variable binding:
```json
"idx:result": "?doc"
```

Structured binding with score and ledger:
```json
"idx:result": {
  "idx:id": "?doc",
  "idx:score": "?similarity",
  "idx:ledger": "?source"
}
```

### Variable Query Vectors

Query vector can be a variable bound earlier:
```json
{
  "where": [
    { "@id": "ex:reference-doc", "ex:embedding": "?queryVec" },
    {
      "idx:graph": "embeddings:main",
      "idx:vector": "?queryVec",
      "idx:limit": 5,
      "idx:result": "?similar"
    }
  ]
}
```

## Distance Metrics

### Cosine (Default)

Measures angle between vectors. Best for:
- Text embeddings (e.g., sentence transformers)
- Normalized vectors
- When magnitude doesn't matter

Score range: [-1, 1] (1 = identical, 0 = orthogonal, -1 = opposite)

### Dot Product

Measures alignment and magnitude. Best for:
- Maximum inner product search (MIPS)
- When vector magnitude encodes importance

Score range: (-inf, +inf)

### Euclidean (L2)

Measures straight-line distance. Best for:
- Geometric similarity
- Image feature vectors
- When absolute position matters

Normalized score range: (0, 1] (1 = identical), computed as `1 / (1 + distance)`

**Note**: All metrics are normalized to "higher is better" in query results.

## Index Maintenance

### Sync Updates

After committing new data, sync the vector index:

```rust
let sync_result = fluree.sync_vector_index("doc-embeddings:main").await?;
println!("Upserted: {}, Removed: {}", sync_result.upserted, sync_result.removed);
```

### Full Resync

Rebuild the entire index from scratch:

```rust
let resync_result = fluree.resync_vector_index("doc-embeddings:main").await?;
```

### Check Staleness

```rust
let check = fluree.check_vector_staleness("doc-embeddings:main").await?;
if check.is_stale {
    println!("Index is {} commits behind", check.commits_behind);
}
```

### Drop Index

```rust
fluree.drop_vector_index("doc-embeddings:main").await?;
```

## Time Travel (not supported in v1)

Vector search is head-only in v1 and does not maintain a snapshot history manifest.

```rust
// v1: time-travel is not supported for vector indexes
// Any "as of t" request should return an explicit error.

// Use idx:sync in queries to ensure index is up-to-date
{
  "where": [{
    "idx:graph": "embeddings:main",
    "idx:vector": [...],
    "idx:sync": true,
    "idx:result": "?doc"
  }]
}
```

## Feature Flag

Vector search requires the `vector` feature:

```toml
[dependencies]
fluree-db-api = { version = "0.1", features = ["vector"] }
```

To enable remote vector search (delegating to a dedicated search service), also enable `search-remote-client`:

```toml
[dependencies]
fluree-db-api = { version = "0.1", features = ["vector", "search-remote-client"] }
```

## Deployment Modes

Vector indexes support two deployment modes: **embedded** (default) and **remote**. This mirrors the BM25 deployment architecture, allowing you to run the vector index locally for simplicity, or offload it to a dedicated search service for scalability.

### Embedded Mode (Default)

In embedded mode, the vector index is loaded and searched within the same process as Fluree:

```json
{
  "deployment": {
    "mode": "embedded"
  }
}
```

This is the default behavior when no deployment configuration is specified.

**Advantages:**
- No network latency
- Simpler deployment
- No additional services to manage

**Use when:**
- Index size is manageable
- Single instance deployments
- Development and testing

### Remote Mode

In remote mode, vector search queries are delegated to a dedicated search service:

```json
{
  "deployment": {
    "mode": "remote",
    "endpoint": "http://search.example.com:9090/v1/search",
    "auth_token": "your-secret-token",
    "request_timeout_ms": 10000
  }
}
```

**Configuration options:**
- `mode`: `"remote"` to enable remote search
- `endpoint`: URL of the search service (required)
- `auth_token`: Bearer token for authentication (optional)
- `connect_timeout_ms`: Connection timeout in milliseconds (default: 5000)
- `request_timeout_ms`: Request timeout in milliseconds (default: 30000)

**Advantages:**
- Scales independently from Fluree instances
- Can handle larger indexes with dedicated memory
- Shared search service across multiple Fluree instances

**Use when:**
- Large embedding indexes that need dedicated resources
- Multiple Fluree instances sharing the same vector index
- Production deployments requiring horizontal scaling

### Running the Search Service

The `fluree-search-httpd` binary provides a standalone HTTP server for remote search. When built with the `vector` feature (enabled by default), it supports both BM25 and vector queries:

```bash
fluree-search-httpd \
  --storage-root file:///var/fluree/data \
  --nameservice-path file:///var/fluree/ns \
  --listen 0.0.0.0:9090
```

**Server options:**
- `--storage-root`: Path to Fluree storage (where indexes are persisted)
- `--nameservice-path`: Path to nameservice data
- `--listen`: Address and port to listen on (default: `0.0.0.0:9090`)
- `--cache-max-entries`: Maximum cached indexes (default: 100)
- `--cache-ttl-secs`: Cache TTL in seconds (default: 300)
- `--max-limit`: Maximum results per query (default: 1000)

**Endpoints:**
- `POST /v1/search` - Execute a search query (BM25 or vector)
- `GET /v1/capabilities` - Get server capabilities and limits
- `GET /v1/health` - Health check

### Search Service Protocol

The remote search service uses a JSON-based protocol. Vector queries use the `"vector"` query kind:

**Request:**
```json
{
  "protocol_version": "1.0",
  "graph_source_address": "doc-embeddings:main",
  "query": {
    "kind": "vector",
    "vector": [0.1, 0.2, 0.3, ...],
    "metric": "cosine"
  },
  "limit": 10,
  "sync": false,
  "timeout_ms": 5000
}
```

**Response:**
```json
{
  "protocol_version": "1.0",
  "index_t": 150,
  "hits": [
    { "iri": "ex:doc-456", "ledger_address": "mydb:main", "score": 0.95 }
  ],
  "took_ms": 8
}
```

### Parity Guarantee

Both embedded and remote modes use identical:
- Distance metric computation
- Score normalization (higher = better)
- Snapshot serialization format

This ensures queries return identical results regardless of deployment mode. You can switch between modes without rebuilding the data.

## Performance Characteristics

- **Index Build Time**: O(n log n) for HNSW index construction
- **Query Time**: O(log n) approximate nearest neighbor search
- **Space**: ~1.5x embedding size + IRI mapping overhead
- **Updates**: Incremental via affected-subject tracking

### Tuning Parameters

| Parameter | Effect | Trade-off |
|-----------|--------|-----------|
| `connectivity` (M) | Graph connectivity | Higher = better recall, more memory |
| `expansion_add` (efConstruction) | Build-time search width | Higher = better index quality, slower build |
| `expansion_search` (efSearch) | Query-time search width | Higher = better recall, slower queries |

## Example: Semantic Search

```rust
// 1. Create ledger with documents and embeddings
let tx = json!({
    "@context": {
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/ledger#"
    },
    "@graph": [
        {
            "@id": "ex:doc1",
            "@type": "ex:Article",
            "ex:title": "Introduction to Machine Learning",
            "ex:embedding": { "@value": [0.9, 0.1, 0.05, ...], "@type": "f:vector" }
        },
        {
            "@id": "ex:doc2",
            "@type": "ex:Article",
            "ex:title": "Database Design Patterns",
            "ex:embedding": { "@value": [0.1, 0.8, 0.1, ...], "@type": "f:vector" }
        }
    ]
});

// 2. Create vector index
let config = VectorCreateConfig::new(
    "articles-search", "articles:main", indexing_query, "ex:embedding", 768
);
fluree.create_vector_index(config).await?;

// 3. Query for similar documents
let query = json!({
    "@context": { "ex": "http://example.org/" },
    "from": "articles:main",
    "where": [{
        "idx:graph": "articles-search:main",
        "idx:vector": user_query_embedding,
        "idx:metric": "cosine",
        "idx:limit": 10,
        "idx:result": {
            "idx:id": "?article",
            "idx:score": "?score"
        }
    }],
    "select": ["?article", "?score"]
});

let results = fluree.query_from()
    .jsonld(&query)
    .execute()
    .await?;
```

## Related Documentation

- [BM25](bm25.md) - Full-text search
- [Background Indexing](background-indexing.md) - Core indexing
- [Graph Sources](../graph-sources/README.md) - Graph source concepts
- [Search Service Protocol](../design/SEARCH_SERVICE_PROTOCOL.md) - Protocol specification
