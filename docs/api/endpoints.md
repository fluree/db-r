# API Endpoints

Complete reference for all Fluree HTTP API endpoints.

## Base URL / versioning

All endpoints listed below are relative to the server’s **API base URL** (`api_base_url` from `GET /.well-known/fluree.json`).

- Standalone `fluree-server` default: `api_base_url = "/v1/fluree"`
- Example full URL: `http://localhost:8090/v1/fluree/query/<ledger...>`

## Discovery and diagnostics

### GET /.well-known/fluree.json

CLI auth discovery endpoint. Used by `fluree remote add` and `fluree auth login` to auto-configure authentication for a remote.

See [Auth contract (CLI ↔ Server)](../design/auth-contract.md) for the full schema.

Standalone `fluree-server` returns:

- `{"version":1,"api_base_url":"/v1/fluree"}` when no server auth is enabled
- `{"version":1,"api_base_url":"/v1/fluree","auth":{"type":"token"}}` when any server auth mode is enabled (data/events/admin)

OIDC-capable implementations should return `auth.type="oidc_device"` plus `issuer`, `client_id`, and `exchange_url`.

Implementations MAY also return `api_base_url` to tell the CLI where the Fluree API is mounted (for example,
when the API is hosted under `/v1/fluree` or on a separate `data` subdomain).

### GET {api_base_url}/whoami

Diagnostic endpoint for Bearer tokens. Returns a summary of the principal:

- `token_present`: whether a Bearer token was present
- `verified`: whether cryptographic verification succeeded
- `auth_method`: `"embedded_jwk"` (Ed25519) or `"oidc"` (JWKS/RS256)
- identity + scope summary (when verified)

This endpoint is intended for debugging and operator support. See also [Admin, health, and stats](../operations/admin-and-health.md).

## Transaction Endpoints

### POST /transact

Submit a transaction to write data to a ledger. Supports both JSON-LD and SPARQL UPDATE formats.

**URL:**
```
POST /transact?ledger={ledger-alias}&mode={mode}
POST /:ledger/transact
```

**Query Parameters:**
- `ledger` (required for /transact): Target ledger (format: `name:branch`)
- `mode` (optional): Transaction mode (JSON-LD only)
  - `default` - Normal insert/update (default)
  - `replace` - Upsert mode (replaces all entity properties)
- `context` (optional): URL to default JSON-LD context

**Request Headers:**

For JSON-LD transactions:
```http
Content-Type: application/json
Accept: application/json
```

For SPARQL UPDATE:
```http
Content-Type: application/sparql-update
Accept: application/json
```

For Turtle (RDF triples):
```http
Content-Type: text/turtle
Accept: application/json
```

For TriG (named graphs):
```http
Content-Type: application/trig
Accept: application/json
```

**Request Body (JSON-LD):**

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

**Request Body (SPARQL UPDATE):**

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice" .
  ex:alice ex:age 30 .
}
```

Or with DELETE/INSERT:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:age ?oldAge .
}
INSERT {
  ?person ex:age 31 .
}
WHERE {
  ?person ex:name "Alice" .
  ?person ex:age ?oldAge .
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

**Examples:**

JSON-LD transaction:
```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
  }'
```

SPARQL UPDATE (ledger-scoped endpoint):
```bash
curl -X POST http://localhost:8090/ledger/mydb:main/transact \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

SPARQL UPDATE (connection-scoped with header):
```bash
curl -X POST http://localhost:8090/fluree/transact \
  -H "Content-Type: application/sparql-update" \
  -H "Fluree-Ledger: mydb:main" \
  -d 'PREFIX ex: <http://example.org/ns/>
      DELETE { ?s ex:age ?old } INSERT { ?s ex:age 31 }
      WHERE { ?s ex:name "Alice" . ?s ex:age ?old }'
```

Turtle transaction:
```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  -d '@prefix ex: <http://example.org/ns/> .
      ex:alice ex:name "Alice" ; ex:age 30 .'
```

TriG transaction with named graphs:
```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  -d '@prefix ex: <http://example.org/ns/> .

      # Default graph
      ex:company ex:name "Acme Corp" .

      # Named graph for products
      GRAPH <http://example.org/graphs/products> {
          ex:widget ex:name "Widget" ;
                    ex:price "29.99"^^xsd:decimal .
      }'
```

### POST /insert

Insert new data into a ledger. Data must not conflict with existing data.

**URL:**
```
POST /insert?ledger={ledger-alias}
POST /:ledger/insert
POST /fluree/insert
```

**Supported Content Types:**
- `application/json` - JSON-LD
- `text/turtle` - Turtle (fast direct flake path)

**Note:** TriG (`application/trig`) is **not supported** on the insert endpoint. Named graph ingestion via GRAPH blocks requires the upsert path. Use `/upsert` for TriG data.

**Example (JSON-LD):**
```bash
curl -X POST "http://localhost:8090/insert?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }]
  }'
```

**Example (Turtle):**
```bash
curl -X POST "http://localhost:8090/insert?ledger=mydb:main" \
  -H "Content-Type: text/turtle" \
  -d '@prefix ex: <http://example.org/ns/> .
      ex:alice ex:name "Alice" ; ex:age 30 .'
```

### POST /upsert

Upsert data into a ledger. For each (subject, predicate) pair, existing values are retracted before new values are asserted.

**URL:**
```
POST /upsert?ledger={ledger-alias}
POST /:ledger/upsert
POST /fluree/upsert
```

**Supported Content Types:**
- `application/json` - JSON-LD
- `text/turtle` - Turtle
- `application/trig` - TriG with named graphs

**Example (JSON-LD):**
```bash
curl -X POST "http://localhost:8090/upsert?ledger=mydb:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@context": { "ex": "http://example.org/ns/" },
    "@id": "ex:alice",
    "ex:age": 31
  }'
```

**Example (TriG with named graphs):**
```bash
curl -X POST "http://localhost:8090/upsert?ledger=mydb:main" \
  -H "Content-Type: application/trig" \
  -d '@prefix ex: <http://example.org/ns/> .

      # Default graph
      ex:company ex:name "Acme Corp" .

      # Named graph for products
      GRAPH <http://example.org/graphs/products> {
          ex:widget ex:name "Widget" ;
                    ex:price "29.99"^^xsd:decimal .
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

Query the history of entities using the standard `/query` endpoint with `from` and `to` keys specifying the time range.

**Request Body:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main@t:1",
  "to": "mydb:main@t:latest",
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
    "from": "mydb:main@t:1",
    "to": "mydb:main@t:latest",
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

Query metadata about all ledgers and graph sources in the nameservice.

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

Graph source records (`@type: "f:GraphSourceDatabase"`):
- `f:name` - Graph source name
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

# Find all graph sources
curl -X POST http://localhost:8090/nameservice/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"f": "https://ns.flur.ee/ledger#", "fidx": "https://ns.flur.ee/index#"},
    "select": ["?name", "?type"],
    "where": [{"@id": "?gs", "@type": "f:GraphSourceDatabase", "f:name": "?name"}]
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
      "ledger_address": "mydb:main",
      "branch": "main",
      "commit_t": 5,
      "index_t": 5,
      "created": "2024-01-22T10:00:00.000Z",
      "last_updated": "2024-01-22T10:30:00.000Z"
    },
    {
      "ledger_address": "mydb:dev",
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

### GET /ledgers/:address

Get metadata for a specific ledger.

**URL:**
```
GET /ledgers/{ledger-address}
```

**Path Parameters:**
- `ledger-address`: Ledger identifier (format: `name:branch`)

**Response:**

```json
{
  "ledger_address": "mydb:main",
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
  "ledger_address": "mydb:main",
  "config": {
    "default_context": "http://example.org/context.jsonld"
  }
}
```

**Response:**

```json
{
  "ledger_address": "mydb:main",
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
  -d '{"ledger_address": "mydb:main"}'
```

### POST /fluree/create

Create a new ledger.

**URL:**
```
POST /fluree/create
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb:main"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger address (e.g., "mydb" or "mydb:main") |

**Response:**

```json
{
  "ledger": "mydb:main",
  "t": 0,
  "tx-id": "fluree:tx:sha256:abc123...",
  "commit": {
    "address": "fluree:memory://mydb/main/head",
    "hash": ""
  }
}
```

| Field | Description |
|-------|-------------|
| `ledger` | Normalized ledger address |
| `t` | Transaction time (0 for new ledger) |
| `tx-id` | Transaction ID (SHA-256 hash of request) |
| `commit` | Commit information |

**Status Codes:**
- `201 Created` - Ledger created successfully
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `409 Conflict` - Ledger already exists
- `500 Internal Server Error` - Server error

**Examples:**

```bash
# Create ledger (no auth required in default mode)
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Create ledger with auth token (when admin auth enabled)
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ..." \
  -d '{"ledger": "mydb:main"}'

# Create with short address (auto-resolves to :main)
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'
```

### POST /fluree/drop

Drop (delete) a ledger.

**URL:**
```
POST /fluree/drop
```

**Authentication:** When admin auth is enabled (`--admin-auth-mode=required`), requires Bearer token from a trusted issuer. See [Admin Authentication](#admin-authentication).

**Request Body:**

```json
{
  "ledger": "mydb:main",
  "hard": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger address (e.g., "mydb" or "mydb:main") |
| `hard` | boolean | No | If `true`, permanently delete all storage files. Default: `false` (soft drop) |

**Drop Modes:**

- **Soft drop** (`hard: false`, default): Retracts the ledger from the nameservice but preserves all data files. The ledger can potentially be recovered.
- **Hard drop** (`hard: true`): Permanently deletes all commit and index files. **This is irreversible.**

**Response:**

```json
{
  "ledger": "mydb:main",
  "status": "dropped",
  "files_deleted": {
    "commit": 15,
    "index": 8
  }
}
```

| Field | Description |
|-------|-------------|
| `ledger` | Normalized ledger address |
| `status` | One of: `"dropped"`, `"already_retracted"`, `"not_found"` |
| `files_deleted` | File counts (only populated for hard drop) |

**Status Codes:**
- `200 OK` - Drop successful (or already dropped/not found)
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `500 Internal Server Error` - Server error

**Drop Sequence:**

1. Normalizes the address (ensures branch suffix like `:main`)
2. Cancels any pending background indexing
3. Waits for in-progress indexing to complete
4. In hard mode: deletes all storage artifacts (commits + indexes)
5. Retracts from nameservice
6. Disconnects from ledger cache

**Idempotency:**

Safe to call multiple times:
- Returns `"already_retracted"` if the ledger was previously dropped
- Hard mode still attempts file deletion even for already-retracted ledgers (useful for cleanup)

**Examples:**

```bash
# Soft drop (retract only, preserve files)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Hard drop (delete all files - IRREVERSIBLE)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main", "hard": true}'

# Drop with auth token (when admin auth enabled)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ..." \
  -d '{"ledger": "mydb:main", "hard": true}'

# Drop with short address (auto-resolves to :main)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'
```

### GET /fluree/exists

Check if a ledger exists in the nameservice.

**URL:**
```
GET /fluree/exists?ledger={ledger-alias}
```

**Query Parameters:**
- `ledger`: Ledger address (e.g., "mydb" or "mydb:main")

**Alternative:** Use the `fluree-ledger` header instead of query parameter.

**Response:**

```json
{
  "ledger": "mydb:main",
  "exists": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ledger` | string | Ledger address (echoed back) |
| `exists` | boolean | Whether the ledger is registered in the nameservice |

**Status Codes:**
- `200 OK` - Check completed successfully (regardless of whether ledger exists)
- `400 Bad Request` - Missing ledger parameter
- `500 Internal Server Error` - Server error

**Usage Notes:**

This is a lightweight check that only queries the nameservice without loading the ledger data. Use this to:

- Check if a ledger exists before attempting to load it
- Implement conditional create-or-load logic
- Validate ledger addresses in application code

**Examples:**

```bash
# Check via query parameter
curl "http://localhost:8090/fluree/exists?ledger=mydb:main"

# Check via header
curl http://localhost:8090/fluree/exists \
  -H "fluree-ledger: mydb:main"

# Conditional create-or-load in shell
if curl -s "http://localhost:8090/fluree/exists?ledger=mydb" | jq -e '.exists == false' > /dev/null; then
  curl -X POST http://localhost:8090/fluree/create \
    -H "Content-Type: application/json" \
    -d '{"ledger": "mydb"}'
fi
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

## Graph Source Endpoints

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

## Admin Authentication

Administrative endpoints (`/fluree/create`, `/fluree/drop`) can be protected with Bearer token authentication.

### Configuration

Enable admin authentication with CLI flags:

```bash
# Production: require trusted tokens
fluree-server \
  --admin-auth-mode=required \
  --admin-auth-trusted-issuer=did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK

# Development: no authentication (default)
fluree-server --admin-auth-mode=none
```

**Environment Variables:**
- `FLUREE_ADMIN_AUTH_MODE`: `none` (default) or `required`
- `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS`: Comma-separated list of trusted did:key identifiers

### Token Format

Admin tokens use the same JWS format as other Fluree tokens. Required claims:

```json
{
  "iss": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
  "exp": 1705932000,
  "sub": "admin@example.com"
}
```

| Claim | Required | Description |
|-------|----------|-------------|
| `iss` | Yes | Issuer did:key (must be in trusted issuers list) |
| `exp` | Yes | Expiration timestamp (Unix seconds) |
| `sub` | No | Subject identifier |
| `fluree.identity` | No | Identity for audit logging |

### Making Authenticated Requests

Include the token in the Authorization header:

```bash
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOiJFZERTQSIsImp3ayI6ey..." \
  -d '{"ledger": "mydb:main"}'
```

### Issuer Trust

Tokens must be signed by a trusted issuer. Configure trusted issuers:

```bash
# Single issuer
--admin-auth-trusted-issuer=did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK

# Multiple issuers
--admin-auth-trusted-issuer=did:key:z6Mk... \
--admin-auth-trusted-issuer=did:key:z6Mn...

# Fallback to events auth issuers
--events-auth-trusted-issuer=did:key:z6Mk...
```

If no admin-specific issuers are configured, admin auth falls back to `--events-auth-trusted-issuer`.

### Response Codes

- `401 Unauthorized`: Missing or invalid Bearer token
- `401 Unauthorized`: Token expired
- `401 Unauthorized`: Untrusted issuer

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
