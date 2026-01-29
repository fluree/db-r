# Virtual Graphs

**Differentiator**: Virtual graphs are one of Fluree's most powerful features, enabling seamless integration of specialized indexes and external data sources directly into graph queries. Unlike traditional databases that require separate systems for full-text search, vector similarity, or data lake access, Fluree's virtual graphs make these capabilities first-class citizens in the query language.

## What Are Virtual Graphs?

A **virtual graph** is a specialized index or data source that appears as a queryable graph in Fluree, but may be backed by different storage or indexing technologies. Virtual graphs extend Fluree's query capabilities beyond traditional RDF triple queries.

### Key Characteristics

- **Query Integration**: Virtual graphs can be queried using the same SPARQL and JSON-LD Query syntax as regular ledgers
- **Transparent Access**: Applications don't need to know whether data comes from a ledger or virtual graph
- **Specialized Indexes**: Each virtual graph type is optimized for specific query patterns
- **Time-Travel Support**: Many virtual graphs support historical queries with snapshot history

## Virtual Graph Types

### BM25 Full-Text Search

**Differentiator**: Fluree includes built-in BM25 full-text search indexing, eliminating the need for separate search systems like Elasticsearch.

**Use Cases:**
- Product search with relevance ranking
- Document search with keyword matching
- Content discovery with fuzzy matching

**Example:**

```sparql
# Query BM25 virtual graph for full-text search
SELECT ?product ?score
FROM <products-search:main>
WHERE {
  ?product bm25:matches "laptop" .
  ?product bm25:score ?score .
}
ORDER BY DESC(?score)
LIMIT 10
```

**Key Features:**
- Relevance scoring (BM25 algorithm)
- Configurable parameters (k1, b)
- Language-aware search
- Time-travel support with snapshot history

See the [BM25 documentation](../indexing-and-search/bm25.md) for details.

### Vector Similarity Search (ANN)

**Differentiator**: Native support for approximate nearest neighbor (ANN) queries via embedded HNSW indexes, enabling semantic search and similarity queries. Can run embedded (in-process) or via a dedicated remote search service.

**Use Cases:**
- Semantic search (find similar documents)
- Recommendation systems
- Image similarity search
- Embedding-based queries

**Key Features:**
- Approximate nearest neighbor search (HNSW algorithm)
- Configurable distance metrics (cosine, euclidean, dot product)
- Embedded indexes (no external service required) or remote mode via `fluree-search-httpd`
- Support for high-dimensional vectors
- Snapshot-based persistence with time-travel support

See the [Vector Search documentation](../indexing-and-search/vector-search.md) for details.

### Apache Iceberg Integration

**Differentiator**: Query Apache Iceberg tables and Parquet files directly as virtual graphs, enabling seamless integration with data lake architectures.

**Use Cases:**
- Query data lake formats without ETL
- Combine graph data with tabular data
- Analytics queries over large datasets
- Integration with existing data pipelines

**Example:**

```sparql
# Query Iceberg table as virtual graph
SELECT ?customer ?order ?amount
FROM <iceberg:sales:main>
WHERE {
  ?order ex:customer ?customer .
  ?order ex:amount ?amount .
  FILTER(?amount > 1000)
}
```

**Key Features:**
- Direct querying of Iceberg tables
- Parquet file support
- R2RML mapping for relational data
- Time-travel via Iceberg snapshots

See the [Iceberg documentation](../virtual-graphs/iceberg.md) for details.

### R2RML Relational Mapping

**Differentiator**: Map relational databases to RDF using R2RML (R2RML Mapping Language), enabling graph queries over SQL databases.

**Use Cases:**
- Migrate from SQL to graph gradually
- Query SQL databases using SPARQL
- Integrate legacy systems
- Unified query interface across data sources

**Example:**

```sparql
# Query relational database via R2RML mapping
SELECT ?customer ?order
FROM <r2rml:orders:main>
WHERE {
  ?customer ex:hasOrder ?order .
  ?order ex:status "pending" .
}
```

**Key Features:**
- R2RML standard compliance
- Automatic RDF mapping from SQL schemas
- Read-only access to source databases
- Support for complex joins and transformations

See the [R2RML documentation](../virtual-graphs/r2rml.md) for details.

## Virtual Graph Lifecycle

### Creation

Virtual graphs are created through administrative operations, specifying:
- **Type**: BM25, Vector, Iceberg, or R2RML
- **Configuration**: Type-specific settings
- **Dependencies**: Source ledgers or data sources
- **Branch**: Virtual graphs support branching like ledgers

**Example BM25 Virtual Graph Creation:**

```json
{
  "@type": "fidx:BM25",
  "fidx:name": "products-search",
  "fidx:branch": "main",
  "fidx:sourceLedger": "products:main",
  "fidx:config": {
    "k1": 1.2,
    "b": 0.75,
    "fields": ["name", "description"]
  }
}
```

### Indexing

Virtual graphs maintain their own indexes:
- **BM25**: Full-text indexes are built from source ledger data
- **Vector**: Embeddings stored in HNSW indexes (embedded or remote)
- **Iceberg**: Metadata is cached for efficient querying
- **R2RML**: Mapping rules are applied to generate RDF

### Querying

Virtual graphs are queried like regular ledgers:

```sparql
# Query any virtual graph
SELECT ?result
FROM <virtual-graph-name:branch>
WHERE {
  # Query patterns specific to virtual graph type
}
```

### Time Travel

Many virtual graphs support historical queries using the `@t:` syntax in the ledger reference:

```json
{
  "@context": { "bm25": "http://ns.flur.ee/bm25#" },
  "from": "products-search:main@t:1000",
  "select": ["?product"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" }
  ]
}
```

BM25 virtual graphs maintain snapshot history, enabling time-travel queries over search indexes. Use the same `@t:`, `@iso:`, or `@sha:` syntax as with regular ledgers.

## Virtual Graph Architecture

### Nameservice Integration

Virtual graphs are tracked in the nameservice alongside ledgers:

- **Discovery**: List all virtual graphs via nameservice
- **Metadata**: Configuration and status stored in nameservice
- **Coordination**: Index state tracked separately from source ledgers

### Query Execution

When querying a virtual graph:

1. **Resolution**: Query engine resolves virtual graph from nameservice
2. **Type Detection**: Determines virtual graph type (BM25, Vector, etc.)
3. **Specialized Execution**: Routes to type-specific query handler
4. **Result Integration**: Results integrated with regular graph queries

### Performance Characteristics

Each virtual graph type has different performance characteristics:

- **BM25**: Fast keyword search, relevance scoring
- **Vector**: Approximate similarity search, configurable accuracy/speed tradeoff
- **Iceberg**: Columnar storage, efficient for analytical queries
- **R2RML**: Depends on source database performance

## Use Cases

### Multi-Modal Search

Combine full-text search, vector similarity, and graph queries:

```sparql
SELECT ?product ?textScore ?vectorScore
FROM <products:main>
FROM <products-search:main>  # BM25 virtual graph
FROM <products-vector:main>  # Vector virtual graph
WHERE {
  # Graph query
  ?product ex:category "electronics" .
  
  # BM25 search
  GRAPH <products-search:main> {
    ?product bm25:matches "wireless" .
    ?product bm25:score ?textScore .
  }
  
  # Vector similarity
  GRAPH <products-vector:main> {
    ?product vector:similar "bluetooth speaker" .
    ?product vector:score ?vectorScore .
  }
}
ORDER BY DESC(?textScore + ?vectorScore)
```

### Data Lake Integration

Query both graph and tabular data:

```sparql
SELECT ?customer ?graphData ?lakeData
FROM <customers:main>           # Graph ledger
FROM <iceberg:sales:main>        # Iceberg virtual graph
WHERE {
  # Graph data
  ?customer ex:preferences ?graphData .
  
  # Data lake data
  GRAPH <iceberg:sales:main> {
    ?sale ex:customer ?customer .
    ?sale ex:total ?lakeData .
  }
}
```

### Hybrid Search

Combine semantic and keyword search:

```sparql
SELECT ?document
FROM <documents-search:main>     # BM25 virtual graph
FROM <documents-vector:main>     # Vector virtual graph
WHERE {
  {
    # Keyword match
    GRAPH <documents-search:main> {
      ?document bm25:matches "machine learning" .
    }
  }
  UNION
  {
    # Semantic similarity
    GRAPH <documents-vector:main> {
      ?document vector:similar "artificial intelligence" .
    }
  }
}
```

## Best Practices

### Virtual Graph Design

1. **Choose Appropriate Type**: Match virtual graph type to query patterns
   - Keyword search → BM25
   - Semantic search → Vector
   - Analytics → Iceberg
   - SQL integration → R2RML

2. **Configuration Tuning**: Optimize virtual graph parameters
   - BM25: Tune k1 and b for relevance
   - Vector: Choose appropriate distance metric
   - Iceberg: Optimize partition strategy

3. **Dependency Management**: Understand source data dependencies
   - BM25/Vector: Keep in sync with source ledger
   - Iceberg: Handle schema evolution
   - R2RML: Map schema changes

### Performance Optimization

1. **Index Maintenance**: Keep virtual graph indexes up-to-date
   - Monitor indexing lag
   - Tune indexing frequency
   - Handle large data volumes

2. **Query Planning**: Optimize queries using virtual graphs
   - Use virtual graphs for appropriate query patterns
   - Combine with graph queries efficiently
   - Consider cost of virtual graph queries

3. **Caching**: Cache frequently accessed virtual graph results
   - Cache query results when appropriate
   - Consider virtual graph snapshot caching
   - Balance freshness vs performance

### Operational Considerations

1. **Monitoring**: Track virtual graph health
   - Index build status
   - Query performance
   - Storage usage

2. **Backup**: Include virtual graphs in backup strategy
   - BM25 indexes can be rebuilt
   - Vector indexes stored as snapshots
   - Iceberg metadata in nameservice

3. **Scaling**: Plan for virtual graph scaling
   - BM25: Scale with source ledger size
   - Vector: Scale with embedding count
   - Iceberg: Leverage Iceberg partitioning

## Comparison with Traditional Approaches

### Traditional Architecture

```
Application
    ├── Graph Database (Neo4j, etc.)
    ├── Search Engine (Elasticsearch)
    ├── Vector DB (Pinecone, etc.)
    └── Data Lake (Spark, Presto)
```

**Challenges:**
- Multiple systems to manage
- Data synchronization complexity
- Different query languages
- Separate authentication/authorization

### Fluree Virtual Graph Architecture

```
Application
    └── Fluree
        ├── Graph Ledgers
        ├── BM25 Virtual Graphs (built-in)
        ├── Vector Virtual Graphs
        └── Iceberg Virtual Graphs
```

**Benefits:**
- Single query interface (SPARQL/JSON-LD Query)
- Unified access control (policy enforcement)
- Consistent time-travel across all data
- Simplified operations and deployment

Virtual graphs make Fluree a unified platform for graph, search, vector, and data lake queries, eliminating the complexity of managing multiple specialized systems.
