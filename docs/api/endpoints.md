# API Endpoints

Complete reference for all Fluree HTTP API endpoints.

## Transaction Endpoints

### POST /transact

Submit a transaction to write data to a ledger.

**URL:**
```
POST /transact?ledger={ledger-alias}&mode={mode}
```

**Query Parameters:**
- `ledger` (required): Target ledger (format: `name:branch`)
- `mode` (optional): Transaction mode
  - `default` - Normal insert/update (default)
  - `replace` - Upsert mode (replaces all entity properties)
- `context` (optional): URL to default JSON-LD context

**Request Headers:**
```http
Content-Type: application/json
Accept: application/json
```

**Request Body:**

JSON-LD transaction document:
```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    { "@id": "ex:alice", "ex:name": "Alice" }
  ]
}
```

Or WHERE/DELETE/INSERT update:
```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "where": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:age": 31 }
  ]
}
```

**Response:**

```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456789...",
  "address": "fluree:memory:commit:abc123...",
  "flakes_added": 3,
  "flakes_retracted": 1,
  "previous_commit": "def456abc789..."
}
```

**Status Codes:**
- `200 OK` - Transaction successful
- `400 Bad Request` - Invalid transaction syntax
- `401 Unauthorized` - Authentication required
- `403 Forbidden` - Not authorized for this ledger
- `404 Not Found` - Ledger not found
- `413 Payload Too Large` - Transaction exceeds size limit
- `500 Internal Server Error` - Server error

**Example:**

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
  }'
```

## Query Endpoints

### POST /query

Execute a query against one or more ledgers.

**URL:**
```
POST /query
```

**Request Headers:**
```http
Content-Type: application/json
Accept: application/json
```

Or for SPARQL:
```http
Content-Type: application/sparql-query
Accept: application/sparql-results+json
```

**Request Body (JSON-LD Query):**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main",
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "ex:name": "?name" },
    { "@id": "?person", "ex:age": "?age" }
  ],
  "orderBy": ["?name"],
  "limit": 100
}
```

**Request Body (SPARQL):**

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
ORDER BY ?name
LIMIT 100
```

**Response (JSON-LD Query):**

```json
[
  { "name": "Alice", "age": 30 },
  { "name": "Bob", "age": 25 }
]
```

**Response (SPARQL):**

```json
{
  "head": {
    "vars": ["name", "age"]
  },
  "results": {
    "bindings": [
      {
        "name": { "type": "literal", "value": "Alice" },
        "age": { "type": "literal", "value": "30", "datatype": "http://www.w3.org/2001/XMLSchema#integer" }
      }
    ]
  }
}
```

**Status Codes:**
- `200 OK` - Query successful
- `400 Bad Request` - Invalid query syntax
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Ledger not found
- `413 Payload Too Large` - Query exceeds size limit
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Query timeout or resource limit

**Example:**

```bash
curl -X POST http://localhost:8090/query \
  -H "Content-Type: application/json" \
  -d '{
    "from": "mydb:main",
    "select": ["?name"],
    "where": [{ "@id": "?person", "ex:name": "?name" }]
  }'
```

### History Queries via POST /query

Query the history of entities using the standard `/query` endpoint with a time range in the `from` clause.

**Request Body:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": ["mydb:main@t:1", "mydb:main@t:latest"],
  "select": ["?name", "?age", "?t", "?op"],
  "where": [
    { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } },
    { "@id": "ex:alice", "ex:age": "?age" }
  ],
  "orderBy": "?t"
}
```

The `@t` and `@op` annotations capture transaction metadata:
- **@t** - Transaction time when the value was asserted or retracted
- **@op** - Operation type: `"assert"` or `"retract"`

**Response:**

```json
[
  ["Alice", 30, 1, "assert"],
  ["Alice", 30, 5, "retract"],
  ["Alicia", 31, 5, "assert"]
]
```

**Example:**

```bash
curl -X POST http://localhost:8090/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "from": ["mydb:main@t:1", "mydb:main@t:latest"],
    "select": ["?name", "?t", "?op"],
    "where": [
      { "@id": "ex:alice", "ex:name": { "@value": "?name", "@t": "?t", "@op": "?op" } }
    ],
    "orderBy": "?t"
  }'
```

**SPARQL History Query:**

```bash
curl -X POST http://localhost:8090/query \
  -H "Content-Type: application/sparql-query" \
  -d 'PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?name ?t ?op
FROM <mydb:main@t:1>
TO <mydb:main@t:latest>
WHERE {
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}
ORDER BY ?t'
```

## Nameservice Query Endpoint

### POST /nameservice/query

Query metadata about all ledgers and virtual graphs in the nameservice.

**URL:**
```
POST /nameservice/query
```

**Request Headers:**
```http
Content-Type: application/json
Accept: application/json
```

Or for SPARQL:
```http
Content-Type: application/sparql-query
Accept: application/sparql-results+json
```

**Request Body (JSON-LD Query):**

```json
{
  "@context": {
    "f": "https://ns.flur.ee/ledger#"
  },
  "select": ["?ledger", "?branch", "?t"],
  "where": [
    { "@id": "?ns", "@type": "f:PhysicalDatabase", "f:ledger": "?ledger", "f:branch": "?branch", "f:t": "?t" }
  ],
  "orderBy": [{"var": "?t", "desc": true}]
}
```

**Request Body (SPARQL):**

```sparql
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?ledger ?branch ?t
WHERE {
  ?ns a f:PhysicalDatabase ;
      f:ledger ?ledger ;
      f:branch ?branch ;
      f:t ?t .
}
ORDER BY DESC(?t)
```

**Response (JSON-LD Query):**

```json
[
  ["customers", "main", 150],
  ["products", "main", 100],
  ["inventory", "dev", 50]
]
```

**Response (SPARQL):**

```json
{
  "head": {
    "vars": ["ledger", "branch", "t"]
  },
  "results": {
    "bindings": [
      {
        "ledger": { "type": "literal", "value": "customers" },
        "branch": { "type": "literal", "value": "main" },
        "t": { "type": "literal", "value": "150", "datatype": "http://www.w3.org/2001/XMLSchema#long" }
      }
    ]
  }
}
```

**Available Properties:**

Ledger records (`@type: "f:PhysicalDatabase"`):
- `f:ledger` - Ledger name
- `f:branch` - Branch name
- `f:t` - Transaction number
- `f:status` - "ready" or "retracted"
- `f:commit` - Commit address reference
- `f:index` - Index info with address and t

Virtual graph records (`@type: "f:VirtualGraphDatabase"`):
- `f:name` - Virtual graph name
- `f:branch` - Branch name
- `fidx:config` - Configuration JSON
- `fidx:dependencies` - Source ledger dependencies
- `fidx:indexAddress` - Index address
- `fidx:indexT` - Index t value

**Status Codes:**
- `200 OK` - Query successful
- `400 Bad Request` - Invalid query syntax
- `500 Internal Server Error` - Server error

**Example:**

```bash
# Find all ledgers on main branch
curl -X POST http://localhost:8090/nameservice/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"f": "https://ns.flur.ee/ledger#"},
    "select": ["?ledger"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
  }'

# Find all virtual graphs
curl -X POST http://localhost:8090/nameservice/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"f": "https://ns.flur.ee/ledger#", "fidx": "https://ns.flur.ee/index#"},
    "select": ["?name", "?type"],
    "where": [{"@id": "?vg", "@type": "f:VirtualGraphDatabase", "f:name": "?name"}]
  }'
```

## Ledger Management Endpoints

### GET /ledgers

List all ledgers.

**URL:**
```
GET /ledgers
```

**Response:**

```json
{
  "ledgers": [
    {
      "alias": "mydb:main",
      "branch": "main",
      "commit_t": 5,
      "index_t": 5,
      "created": "2024-01-22T10:00:00.000Z",
      "last_updated": "2024-01-22T10:30:00.000Z"
    },
    {
      "alias": "mydb:dev",
      "branch": "dev",
      "commit_t": 3,
      "index_t": 2,
      "created": "2024-01-22T11:00:00.000Z",
      "last_updated": "2024-01-22T11:15:00.000Z"
    }
  ]
}
```

**Example:**

```bash
curl http://localhost:8090/ledgers
```

### GET /ledgers/:alias

Get metadata for a specific ledger.

**URL:**
```
GET /ledgers/{ledger-alias}
```

**Path Parameters:**
- `ledger-alias`: Ledger identifier (format: `name:branch`)

**Response:**

```json
{
  "alias": "mydb:main",
  "branch": "main",
  "commit_t": 5,
  "index_t": 5,
  "commit_address": "fluree:memory:commit:abc123...",
  "index_address": "fluree:memory:index:def456...",
  "created": "2024-01-22T10:00:00.000Z",
  "last_updated": "2024-01-22T10:30:00.000Z",
  "retracted": false
}
```

**Example:**

```bash
curl http://localhost:8090/ledgers/mydb:main
```

### POST /ledgers

Create a new ledger explicitly (usually ledgers are created implicitly on first transaction).

**URL:**
```
POST /ledgers
```

**Request Body:**

```json
{
  "alias": "mydb:main",
  "config": {
    "default_context": "http://example.org/context.jsonld"
  }
}
```

**Response:**

```json
{
  "alias": "mydb:main",
  "branch": "main",
  "commit_t": 0,
  "index_t": 0,
  "created": "2024-01-22T10:00:00.000Z"
}
```

**Example:**

```bash
curl -X POST http://localhost:8090/ledgers \
  -H "Content-Type: application/json" \
  -d '{"alias": "mydb:main"}'
```

## System Endpoints

### GET /health

Health check endpoint for monitoring.

**URL:**
```
GET /health
```

**Response:**

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "memory",
  "uptime_ms": 123456
}
```

**Status Codes:**
- `200 OK` - System healthy
- `503 Service Unavailable` - System unhealthy

**Example:**

```bash
curl http://localhost:8090/health
```

### GET /status

Detailed server status and statistics.

**URL:**
```
GET /status
```

**Response:**

```json
{
  "version": "0.1.0",
  "uptime_ms": 123456789,
  "storage": {
    "mode": "memory",
    "total_bytes": 12345678,
    "ledgers": 5
  },
  "queries": {
    "total": 1234,
    "active": 3,
    "average_duration_ms": 45
  },
  "transactions": {
    "total": 567,
    "average_duration_ms": 89
  },
  "indexing": {
    "active": true,
    "pending_ledgers": 2
  }
}
```

**Example:**

```bash
curl http://localhost:8090/status
```

### GET /version

Server version information.

**URL:**
```
GET /version
```

**Response:**

```json
{
  "version": "0.1.0",
  "git_commit": "abc123def456",
  "build_date": "2024-01-15",
  "rust_version": "1.75.0"
}
```

**Example:**

```bash
curl http://localhost:8090/version
```

## Virtual Graph Endpoints

### POST /index/bm25

Create or update a BM25 full-text search index.

**URL:**
```
POST /index/bm25?ledger={ledger-alias}
```

**Request Body:**

```json
{
  "name": "products-search",
  "source": "mydb:main",
  "fields": [
    { "predicate": "ex:title", "weight": 2.0 },
    { "predicate": "ex:description", "weight": 1.0 }
  ]
}
```

**Response:**

```json
{
  "name": "products-search:main",
  "status": "indexing",
  "documents": 0,
  "started": "2024-01-22T10:30:00.000Z"
}
```

### POST /index/vector

Create or configure a vector search index. Requires the `vector` feature flag.

**URL:**
```
POST /index/vector?ledger={ledger-alias}
```

**Request Body:**

```json
{
  "name": "products-vector",
  "source": "mydb:main",
  "embedding_property": "ex:embedding",
  "dimensions": 768,
  "metric": "cosine"
}
```

## Admin Endpoints

### POST /admin/index

Trigger manual indexing for a ledger.

**URL:**
```
POST /admin/index?ledger={ledger-alias}
```

**Response:**

```json
{
  "ledger": "mydb:main",
  "status": "indexing",
  "target_t": 10
}
```

### POST /admin/compact

Trigger compaction for a ledger (cleanup old indexes).

**URL:**
```
POST /admin/compact?ledger={ledger-alias}
```

### GET /admin/stats

Get detailed server statistics.

**URL:**
```
GET /admin/stats
```

## Error Responses

All endpoints may return error responses in this format:

```json
{
  "error": "ErrorType",
  "message": "Human-readable error message",
  "code": "ERROR_CODE",
  "details": {
    "field": "Additional context"
  }
}
```

See [Errors and Status Codes](errors.md) for complete error reference.

## Related Documentation

- [Overview](overview.md) - API overview and principles
- [Headers](headers.md) - HTTP headers and content types
- [Signed Requests](signed-requests.md) - Authentication
- [Errors](errors.md) - Error codes and troubleshooting
