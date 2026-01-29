# Virtual Graphs and Integrations

Virtual graphs extend Fluree's query capabilities by integrating specialized indexes and external data sources. Virtual graphs appear as queryable ledgers but are backed by different storage and indexing systems.

## Virtual Graph Types

### [Overview](overview.md)

Introduction to virtual graphs:
- What are virtual graphs
- Architecture and design
- Use cases
- Performance characteristics
- Creating and managing virtual graphs

### [Iceberg / Parquet](iceberg.md)

Apache Iceberg data lake integration:
- Querying Iceberg tables
- Parquet file support
- Schema mapping
- Partition pruning
- Performance optimization

### [R2RML](r2rml.md)

Relational database mapping:
- R2RML standard
- Mapping relational data to RDF
- SQL query generation
- Join optimization
- Supported databases (PostgreSQL, MySQL, etc.)

### [BM25 Virtual Graph](bm25-virtual-graph.md)

Full-text search as virtual graph:
- BM25 index as queryable ledger
- Search predicates
- Combining with structured queries
- Real-time index updates

## What are Virtual Graphs?

Virtual graphs are queryable data sources that appear as Fluree ledgers but are backed by specialized storage:

**Standard Ledger:**
```text
mydb:main → RDF triple store → SPOT/POST/OPST/PSOT indexes
```

**Virtual Graph:**
```text
products-search:main → BM25 index → Inverted text index
products-vector:main → HNSW → Vector similarity index
warehouse-data:main → Iceberg → Parquet files
legacy-db:main → R2RML → PostgreSQL tables
```

## Query Transparency

Virtual graphs are queried like regular ledgers:

```json
{
  "from": "products-search:main",
  "select": ["?product", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" }
  ]
}
```

Or in SPARQL:

```sparql
SELECT ?product ?score
FROM <products-search:main>
WHERE {
  ?product bm25:matches "laptop" .
  ?product bm25:score ?score .
}
```

## Multi-Graph Queries

Combine regular ledgers with virtual graphs:

```json
{
  "from": ["products:main", "products-search:main"],
  "select": ["?product", "?name", "?price", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" },
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" }
  ],
  "orderBy": ["-?score"]
}
```

Joins structured data from products:main with search results from products-search:main.

## Virtual Graph Lifecycle

### 1. Create Virtual Graph

Define mapping/configuration:

```bash
curl -X POST http://localhost:8090/index/bm25?ledger=mydb:main \
  -d '{"name": "products-search", "fields": [...]}'
```

### 2. Initial Indexing

Build index from source data:
- Load data from source ledger
- Transform to target format
- Build specialized index
- Publish to nameservice

### 3. Incremental Updates

Keep synchronized with source:
- Monitor source ledger for changes
- Update virtual graph incrementally
- Maintain consistency

### 4. Query Execution

Execute queries against virtual graph:
- Parse query
- Route to appropriate backend
- Execute specialized query
- Return results

## Supported Virtual Graphs

### BM25 Full-Text Search

**Purpose:** Keyword search with relevance ranking

**Backend:** Inverted index

**Use Cases:**
- E-commerce product search
- Document search
- Knowledge base search

**Example:**
```json
{
  "from": "docs-search:main",
  "where": [
    { "@id": "?doc", "bm25:matches": "quarterly report" }
  ]
}
```

See [BM25 Virtual Graph](bm25-virtual-graph.md) and [BM25 Indexing](../indexing-and-search/bm25.md).

### Vector Similarity Search

**Purpose:** Semantic search using embeddings

**Backend:** HNSW index (embedded or remote)

**Use Cases:**
- Semantic search
- Recommendations
- Image similarity
- Clustering

See [Vector Search](../indexing-and-search/vector-search.md) for details.

### Apache Iceberg

**Purpose:** Query data lake tables

**Backend:** Apache Iceberg / Parquet files

**Use Cases:**
- Analytics on historical data
- Data warehouse integration
- Large-scale batch data

**Example:**
```json
{
  "from": "warehouse-sales:main",
  "select": ["?date", "?revenue"],
  "where": [
    { "@id": "?sale", "warehouse:date": "?date" },
    { "@id": "?sale", "warehouse:revenue": "?revenue" }
  ],
  "filter": "?date >= '2024-01-01'"
}
```

See [Iceberg / Parquet](iceberg.md).

### R2RML (Relational Databases)

**Purpose:** Query relational databases as RDF

**Backend:** SQL databases (PostgreSQL, MySQL, etc.)

**Use Cases:**
- Legacy database integration
- Gradual migration to graph
- Unified queries across systems

**Example:**
```json
{
  "from": "legacy-customers:main",
  "select": ["?name", "?email"],
  "where": [
    { "@id": "?customer", "schema:name": "?name" },
    { "@id": "?customer", "schema:email": "?email" }
  ]
}
```

See [R2RML](r2rml.md).

## Architecture

### Virtual Graph Registry

Virtual graphs registered in nameservice:

```json
{
  "alias": "products-search:main",
  "type": "bm25",
  "source": "products:main",
  "backend": "inverted_index",
  "status": "ready"
}
```

### Query Routing

Query engine routes to appropriate backend:

```text
Query: FROM <products-search:main>
  ↓
Nameservice lookup: type=bm25
  ↓
Route to BM25 query engine
  ↓
Execute against inverted index
  ↓
Return results
```

### Result Integration

Results from virtual graphs join with regular graphs:

```text
FROM <products:main>, <products-search:main>
  ↓
Execute subquery on products:main → Results A
Execute subquery on products-search:main → Results B
  ↓
Join Results A + B on ?product
  ↓
Return combined results
```

## Performance Considerations

### Query Planning

Virtual graphs affect query optimization:
- Specialized indexes enable efficient filtering
- Push filters down to virtual graph when possible
- Minimize data transfer between graphs

### Data Transfer

Minimize data movement:
- Filter in virtual graph before joining
- Use selective projections
- Leverage virtual graph's native capabilities

### Caching

Some virtual graph backends support caching:
- BM25: Results cacheable
- Vector: Similar queries share computation
- Iceberg: Parquet file caching
- R2RML: SQL query plan caching

## Best Practices

### 1. Choose Appropriate Virtual Graph Type

Match virtual graph to use case:
- Keyword search → BM25
- Semantic search → Vector
- Analytics → Iceberg
- Legacy integration → R2RML

### 2. Filter Early

Push filters to virtual graphs:

Good:
```json
{
  "from": ["products:main", "products-search:main"],
  "where": [
    { "@id": "?p", "bm25:matches": "laptop" },  // Filter in search
    { "@id": "?p", "schema:price": "?price" }
  ],
  "filter": "?price < 1000"
}
```

### 3. Monitor Virtual Graph Lag

Check synchronization status:

```bash
curl http://localhost:8090/index/status/products-search:main
```

### 4. Use Appropriate Limits

Limit results from virtual graphs:

```json
{
  "where": [
    { "@id": "?p", "bm25:matches": "query" }
  ],
  "limit": 100
}
```

### 5. Test Performance

Profile queries combining virtual graphs:

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Explain: true" \
  -d '{...}'
```

## Troubleshooting

### Virtual Graph Not Found

```json
{
  "error": "VirtualGraphNotFound",
  "message": "Virtual graph not found: products-search:main"
}
```

**Solution:** Create virtual graph or check name spelling.

### Synchronization Lag

Virtual graph out of sync with source:

```bash
# Check status
curl http://localhost:8090/index/status/products-search:main

# Trigger rebuild
curl -X POST http://localhost:8090/index/rebuild/products-search:main
```

### Poor Performance

Query combining virtual graphs is slow:

1. Check explain plan
2. Add filters to reduce result set
3. Ensure indexes are up-to-date
4. Consider query rewrite

## Related Documentation

- [Overview](overview.md) - Virtual graph concepts
- [BM25](bm25-virtual-graph.md) - Full-text search
- [Vector Search](../indexing-and-search/vector-search.md) - Similarity search
- [Iceberg](iceberg.md) - Data lake integration
- [R2RML](r2rml.md) - Relational mapping
- [Query Datasets](../query/datasets.md) - Multi-graph queries
