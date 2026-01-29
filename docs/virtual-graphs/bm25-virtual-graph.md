# BM25 Virtual Graph

BM25 indexes in Fluree are exposed as virtual graphs, allowing full-text search to be seamlessly integrated with graph queries using the same query interface.

## Overview

A BM25 virtual graph provides:
- Full-text search with relevance ranking
- Integration with structured graph queries
- Real-time index updates
- Standard query interface (SPARQL / JSON-LD)

## Creating BM25 Virtual Graphs

See [BM25 Indexing](../indexing-and-search/bm25.md) for detailed index creation.

Basic creation:

```bash
curl -X POST "http://localhost:8090/index/bm25?ledger=mydb:main" \
  -d '{
    "name": "products-search",
    "fields": [
      { "predicate": "schema:name", "weight": 2.0 },
      { "predicate": "schema:description", "weight": 1.0 }
    ]
  }'
```

This creates virtual graph: `products-search:main`

## Virtual Graph Properties

BM25 virtual graphs expose special predicates:

### bm25:matches

Search predicate:

```json
{
  "@id": "?product",
  "bm25:matches": "laptop computer"
}
```

### bm25:score

Relevance score:

```json
{
  "@id": "?product",
  "bm25:score": "?score"
}
```

### bm25:snippet

Highlighted text excerpt:

```json
{
  "@id": "?product",
  "bm25:snippet": "?snippet"
}
```

## Querying BM25 Virtual Graphs

### Basic Search

```json
{
  "@context": {
    "schema": "http://schema.org/",
    "bm25": "https://ns.flur.ee/bm25#"
  },
  "from": "products-search:main",
  "select": ["?product", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "laptop" },
    { "@id": "?product", "bm25:score": "?score" }
  ],
  "orderBy": ["-?score"],
  "limit": 20
}
```

### SPARQL Search

```sparql
PREFIX bm25: <https://ns.flur.ee/bm25#>

SELECT ?product ?score
FROM <products-search:main>
WHERE {
  ?product bm25:matches "laptop" .
  ?product bm25:score ?score .
}
ORDER BY DESC(?score)
LIMIT 20
```

## Combining with Structured Data

### Join with Source Ledger

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
  "filter": "?price < 1000",
  "orderBy": ["-?score"]
}
```

Combines:
- Text search from `products-search:main`
- Structured data from `products:main`
- Filters on price
- Orders by relevance

### Multi-Criteria Search

```json
{
  "from": ["products:main", "products-search:main"],
  "select": ["?product", "?name", "?category", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "wireless headphones" },
    { "@id": "?product", "bm25:score": "?score" },
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:category": "?category" },
    { "@id": "?product", "schema:inStock": true }
  ],
  "filter": "?score > 5.0 && ?category == 'electronics'",
  "orderBy": ["-?score"],
  "limit": 50
}
```

Filters by:
- Text relevance (score > 5.0)
- Category
- Stock status

## Search Features

### Phrase Search

```json
{
  "@id": "?product",
  "bm25:matches": "\"noise cancelling headphones\""
}
```

### Boolean Operators

```json
{
  "@id": "?product",
  "bm25:matches": "laptop AND (gaming OR professional) NOT budget"
}
```

### Wildcards

```json
{
  "@id": "?product",
  "bm25:matches": "comput*"
}
```

### Field-Specific Search

```json
{
  "@id": "?product",
  "bm25:matches": "name:laptop description:professional"
}
```

## Real-Time Updates

BM25 virtual graphs update automatically:

### Insert

```json
{
  "@graph": [
    {
      "@id": "ex:product-new",
      "schema:name": "New Laptop",
      "schema:description": "Latest model"
    }
  ]
}
```

Product immediately searchable in `products-search:main`.

### Update

```json
{
  "where": [
    { "@id": "ex:product-123", "schema:description": "?old" }
  ],
  "delete": [
    { "@id": "ex:product-123", "schema:description": "?old" }
  ],
  "insert": [
    { "@id": "ex:product-123", "schema:description": "Updated description" }
  ]
}
```

Search index updated automatically.

### Delete

```json
{
  "where": [
    { "@id": "ex:product-123", "?pred": "?val" }
  ],
  "delete": [
    { "@id": "ex:product-123", "?pred": "?val" }
  ]
}
```

Product removed from search index.

## Use Cases

### E-commerce Search

```json
{
  "from": ["products:main", "products-search:main"],
  "select": ["?product", "?name", "?price", "?image", "?score"],
  "where": [
    { "@id": "?product", "bm25:matches": "?userQuery" },
    { "@id": "?product", "bm25:score": "?score" },
    { "@id": "?product", "schema:name": "?name" },
    { "@id": "?product", "schema:price": "?price" },
    { "@id": "?product", "schema:image": "?image" },
    { "@id": "?product", "schema:availability": "in_stock" }
  ],
  "filter": "?score > 3.0",
  "orderBy": ["-?score"],
  "limit": 24
}
```

### Document Search

```json
{
  "from": ["documents:main", "docs-search:main"],
  "select": ["?doc", "?title", "?author", "?snippet"],
  "where": [
    { "@id": "?doc", "bm25:matches": "quarterly financial report" },
    { "@id": "?doc", "bm25:score": "?score" },
    { "@id": "?doc", "bm25:snippet": "?snippet" },
    { "@id": "?doc", "schema:headline": "?title" },
    { "@id": "?doc", "schema:author": "?author" },
    { "@id": "?doc", "ex:department": "finance" }
  ],
  "filter": "?score > 10.0",
  "orderBy": ["-?score"]
}
```

### Knowledge Base

```json
{
  "from": ["articles:main", "articles-search:main"],
  "select": ["?article", "?title", "?tags", "?snippet"],
  "where": [
    { "@id": "?article", "bm25:matches": "machine learning" },
    { "@id": "?article", "bm25:snippet": "?snippet" },
    { "@id": "?article", "schema:headline": "?title" },
    { "@id": "?article", "schema:keywords": "?tags" }
  ],
  "limit": 10
}
```

## Performance

### Query Performance

BM25 virtual graph queries:
- Small index (< 10K docs): < 10ms
- Medium index (< 100K docs): < 50ms
- Large index (< 1M docs): < 200ms

### Combined Queries

Queries combining BM25 + structured data:
- Filter in BM25 first (reduces join size)
- Push filters to source ledger
- Use LIMIT to reduce result set

## Best Practices

### 1. Filter in Virtual Graph

Good (filters early):
```json
{
  "from": ["products:main", "products-search:main"],
  "where": [
    { "@id": "?p", "bm25:matches": "laptop" },  // Filters first
    { "@id": "?p", "bm25:score": "?score" },
    { "@id": "?p", "schema:name": "?name" }
  ],
  "filter": "?score > 5.0"
}
```

### 2. Use Score Thresholds

```json
{
  "where": [...],
  "filter": "?score > 3.0"  // Exclude low-relevance results
}
```

### 3. Limit Results

```json
{
  "where": [...],
  "limit": 50  // Reasonable limit
}
```

### 4. Use Snippets for UI

```json
{
  "select": ["?title", "?snippet"],  // Show highlighted excerpts
  "where": [
    { "@id": "?doc", "bm25:matches": "query" },
    { "@id": "?doc", "bm25:snippet": "?snippet" }
  ]
}
```

### 5. Monitor Sync Status

```javascript
const status = await getIndexStatus('products-search:main');
if (status.lag_ms > 5000) {
  console.warn('Search index lag > 5 seconds');
}
```

## Troubleshooting

### No Results

**Causes:**
- Index not synced
- Search terms too specific
- Stopwords filtered out

**Solutions:**
- Check index status
- Try broader search terms
- Use wildcards

### Low Relevance Scores

**Causes:**
- Field weights not optimized
- Documents don't match well
- Common terms (high IDF)

**Solutions:**
- Adjust field weights
- Refine search query
- Use boolean operators

### Slow Queries

**Causes:**
- Large result set
- Complex filters
- Multiple virtual graphs

**Solutions:**
- Add LIMIT clause
- Filter by score threshold
- Optimize query structure

## Related Documentation

- [BM25 Indexing](../indexing-and-search/bm25.md) - Index creation and configuration
- [Virtual Graphs Overview](overview.md) - Virtual graph concepts
- [Query Datasets](../query/datasets.md) - Multi-graph queries
