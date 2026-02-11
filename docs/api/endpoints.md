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
The CLI treats `oidc_device` as "OIDC interactive login": it uses device-code when the IdP supports it, otherwise authorization-code + PKCE.

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
POST /transact?ledger={ledger-id}&mode={mode}
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
  "commit_id": "bafybeig...commitT5",
  "flakes_added": 3,
  "flakes_retracted": 1,
  "previous_commit_id": "bafybeig...commitT4"
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
POST /insert?ledger={ledger-id}
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
POST /upsert?ledger={ledger-id}
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

### POST /push/*ledger

Push precomputed commit v2 blobs to the server.

This endpoint is intended for Git-like workflows (`fluree push`) where a client has written commits locally and wants the server to validate and commit them.

**URL:**

```
POST /push/<ledger...>
```

**Request Headers:**

```http
Content-Type: application/json
Accept: application/json
Authorization: Bearer <token>
Idempotency-Key: <string>   (optional; recommended)
```

If `Idempotency-Key` is provided, servers MAY treat `POST /push/*ledger` as idempotent for that key (same request body + key should yield the same response), returning the prior success response instead of `409` on client retry after timeouts.

**Request Body:**

JSON object:

- `commits`: array of base64-encoded commit v2 blobs (oldest → newest)
- `blobs` (optional): map of `{ cid: base64Bytes }` for referenced blobs (currently: `commit.txn` when present)

**Response Body (200 OK):**

```json
{
  "ledger": "mydb:main",
  "accepted": 3,
  "head": {
    "t": 42,
    "commit_id": "bafy...headCommit"
  },
  "indexing": {
    "enabled": false,
    "needed": true,
    "novelty_size": 524288,
    "index_t": 30,
    "commit_t": 42
  }
}
```

| Field | Description |
|-------|-------------|
| `indexing.enabled` | Whether background indexing is active on this server. |
| `indexing.needed` | Whether novelty has exceeded `reindex_min_bytes` and indexing should be triggered. |
| `indexing.novelty_size` | Current novelty size in bytes after the push. |
| `indexing.index_t` | Transaction time of the last indexed state. |
| `indexing.commit_t` | Transaction time of the latest committed data (after push). |

When `enabled` is `false` (external indexer mode), the caller should use `needed` and related fields to decide whether to trigger indexing through its own mechanism.

**Error Responses:**

- `409 Conflict`: head changed / diverged / first commit `t` did not match next-t
- `422 Unprocessable Entity`: invalid commit bytes, missing referenced blob, or retraction invariant violation

### GET /commits/*ledger

Export commit blobs from a ledger using stable cursors. Pages walk backward via `previous_ref` — O(limit) per page regardless of ledger size. Used by `fluree pull` and `fluree clone`.

**Requires replication-grade permissions** (`fluree.storage.*`). The storage proxy must be enabled on the server.

**URL:**

```
GET /commits/<ledger...>?limit=100&cursor_id=<cid>
```

**Query Parameters:**

- `limit` (optional): Max commits per page (default 100, server clamps to max 500)
- `cursor_id` (optional): Commit CID cursor for pagination. Omit for first page (starts from head). Use `next_cursor_id` from the previous response for subsequent pages.

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Response Body (200 OK):**

```json
{
  "ledger": "mydb:main",
  "head_commit_id": "bafy...headCommit",
  "head_t": 42,
  "commits": ["<base64>", "<base64>"],
  "blobs": { "bafy...txnBlob": "<base64>" },
  "newest_t": 42,
  "oldest_t": 41,
  "next_cursor_id": "bafy...prevCommit",
  "count": 2,
  "effective_limit": 100
}
```

- `commits`: Raw commit v2 blobs, newest → oldest within each page.
- `blobs`: Referenced txn blobs keyed by CID string.
- `next_cursor_id`: CID cursor for the next page; `null` when genesis is reached.
- `effective_limit`: Actual limit used (after server clamping).

**Responses:**

- `200 OK`: Page of commits returned
- `401 Unauthorized`: Missing or invalid storage token
- `404 Not Found`: Storage proxy not enabled, ledger not found, or not authorized for this ledger

**Pagination:**

Commit CIDs in the immutable chain are stable cursors. New commits appended to the head do not affect backward pointers, so cursors remain valid across pages even when new commits arrive between requests.

### POST /pack/*ledger

Stream all missing CAS objects for a ledger in a single binary response. This is the primary transport for `fluree clone` and `fluree pull`, replacing multiple paginated `GET /commits` requests or per-object `GET /storage/objects` fetches with a single streaming request.

**Requires replication-grade permissions** (`fluree.storage.*`). The storage proxy must be enabled on the server.

**URL:**

```
POST /pack/<ledger...>
```

**Request Headers:**

```http
Content-Type: application/json
Accept: application/x-fluree-pack
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Request Body:**

```json
{
  "protocol": "fluree-pack-v1",
  "want": ["bafy...remoteHead"],
  "have": ["bafy...localHead"],
  "include_indexes": true,
  "want_index_root_id": "bafy...indexRoot",
  "have_index_root_id": "bafy...localIndexRoot"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `protocol` | string | Yes | Must be `"fluree-pack-v1"` |
| `want` | string[] | Yes | ContentId CIDs the client wants (typically the remote commit head) |
| `have` | string[] | No | ContentId CIDs the client already has (typically the local commit head). Server stops walking the commit chain when it reaches a `have` CID. Empty for full clone. |
| `want_index_root_id` | string | No | Index root CID the client wants (typically remote nameservice `index_head_id`). Required when `include_indexes=true`. |
| `have_index_root_id` | string | No | Index root CID the client already has (typically local nameservice `index_head_id`). Used for index artifact diff. |
| `include_indexes` | bool | Yes | Include index artifacts in the stream. When true, the stream contains commit + txn objects plus index root/branch/leaf/dict artifacts. |

**Response:**

Binary stream using the `fluree-pack-v1` wire format (`Content-Type: application/x-fluree-pack`):

```
[Preamble: FPK1 + version(1)] [Header frame] [Data frames...] [End frame]
```

| Frame | Type byte | Content |
|-------|-----------|---------|
| Header | `0x00` | JSON metadata: protocol version, capabilities, `commit_count`, `index_artifact_count`, `estimated_total_bytes` |
| Data | `0x01` | CID binary + raw object bytes (commit, txn blob, or index artifact) |
| Error | `0x02` | UTF-8 error message (terminates stream) |
| Manifest | `0x03` | JSON metadata for phase transitions (e.g. start of index phase) |
| End | `0xFF` | End of stream (no payload) |

Data frames are streamed in **oldest-first topological order** (parents before children), so the client can write objects to CAS as they arrive without buffering the entire stream.

The Header frame includes an `estimated_total_bytes` field that the CLI uses to warn users before large transfers (~1 GiB or more). The estimate is ratio-based (derived from commit count) and may differ from actual transfer size. Set to `0` for commits-only requests.

**Status Codes:**

- `200 OK`: Binary pack stream
- `401 Unauthorized`: Missing or invalid storage token
- `404 Not Found`: Storage proxy not enabled, ledger not found, or not authorized for this ledger

**Example:**

```bash
# Download all commits for a ledger (full clone)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...head"],"have":[]}' \
  --output pack.bin

# Download only missing commits (incremental pull)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...remoteHead"],"have":["bafy...localHead"]}' \
  --output pack.bin

# Download commits + index artifacts (default for CLI pull/clone)
curl -X POST "http://localhost:8090/v1/fluree/pack/mydb:main" \
  -H "Content-Type: application/json" \
  -H "Accept: application/x-fluree-pack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"protocol":"fluree-pack-v1","want":["bafy...head"],"have":[],"include_indexes":true,"want_index_root_id":"bafy...indexRoot"}' \
  --output pack.bin
```

## Storage Proxy Endpoints

These endpoints are intended for peer mode and `fluree clone`/`pull` workflows. They require the storage proxy to be enabled on the server and use replication-grade Bearer tokens (`fluree.storage.*` claims).

### GET /storage/ns/:ledger-id

Fetch the nameservice record for a ledger.

**URL:**

```
GET /storage/ns/{ledger-id}
```

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Response (200 OK):**

```json
{
  "ledger_id": "mydb:main",
  "name": "mydb",
  "branch": "main",
  "commit_head_id": "bafy...commitCid",
  "commit_t": 42,
  "index_head_id": "bafy...indexCid",
  "index_t": 40,
  "default_context": null,
  "retracted": false,
  "config_id": "bafy...configCid"
}
```

| Field | Description |
|-------|-------------|
| `ledger_id` | Canonical ledger ID (e.g., "mydb:main") |
| `name` | Ledger name without branch (e.g., "mydb") |
| `config_id` | CID of the `LedgerConfig` object (origin discovery), if set |

**Status Codes:**

- `200 OK`: Record found
- `404 Not Found`: Storage proxy disabled, ledger not found, or not authorized

### POST /storage/block

Fetch a storage block (index branch or leaf) by CID. The server derives the storage address internally. Leaf blocks are always policy-filtered before return.

Only replication-relevant content kinds are allowed (commits, txns, config, index roots/branches/leaves, dict blobs). Internal metadata kinds (GC records, stats sketches, graph source snapshots) are rejected with 404.

**URL:**

```
POST /storage/block
```

**Request Headers:**

```http
Content-Type: application/json
Authorization: Bearer <token>
Accept: application/octet-stream | application/x-fluree-flakes | application/x-fluree-flakes+json
```

**Request Body:**

Both fields are required:

```json
{
  "cid": "bafy...branchOrLeafCid",
  "ledger": "mydb:main"
}
```

**Responses:**

- `200 OK`: Block bytes (branches) or encoded flakes (leaves)
- `400 Bad Request`: Invalid CID string
- `404 Not Found`: Block not found, disallowed kind, or not authorized

### GET /storage/objects/:cid

Fetch a CAS (content-addressed storage) object by its content identifier. Returns the raw bytes of the stored object after verifying integrity.

This is a **replication-grade** endpoint for `fluree clone`/`pull` workflows. The client knows the CID (from the nameservice record or the commit chain) and wants the raw bytes.

**URL:**

```
GET /storage/objects/{cid}?ledger={ledger-id}
```

**Path Parameters:**

- `cid`: CIDv1 string (base32-lower multibase, e.g., `"bafybeig..."`)

**Query Parameters:**

- `ledger` (required): Ledger ID (e.g., `"mydb:main"`). Required because storage paths are ledger-scoped.

**Request Headers:**

```http
Authorization: Bearer <token>   (requires fluree.storage.* claims)
```

**Kind Allowlist:**

All replication-relevant content kinds are served:

| Kind | Description |
|------|-------------|
| `commit` | Commit chain blobs |
| `txn` | Transaction data blobs |
| `config` | LedgerConfig origin discovery objects |
| `index-root` | Binary index root JSON |
| `index-branch` | Index branch manifests |
| `index-leaf` | Index leaf files |
| `dict` | Dictionary artifacts (predicates, subjects, strings, etc.) |

Only `GarbageRecord` (internal GC metadata) returns 404.

**Response Headers:**

- `Content-Type: application/octet-stream`
- `X-Fluree-Content-Kind`: Content kind label (`commit`, `txn`, `config`, `index-root`, `index-branch`, `index-leaf`, `dict`)

**Response Body:**

Raw bytes of the stored object.

**Integrity Verification:**

The server verifies the hash of the stored bytes against the CID before returning. Commit blobs are format-sniffed:

- **Commit-v2 blobs** (`FCV2` magic): Uses the canonical sub-range hash (SHA-256 over the payload excluding the trailing hash + signature block).
- **All other blobs** (txn, config, future commit formats): Full-bytes SHA-256.

If verification fails, the server returns `500 Internal Server Error` — this indicates storage corruption.

**Status Codes:**

- `200 OK`: Object found and integrity verified
- `400 Bad Request`: Invalid CID string
- `404 Not Found`: Object not found, disallowed kind, not authorized, or storage proxy disabled
- `500 Internal Server Error`: Hash verification failed (storage corruption)

**Example:**

```bash
# Fetch a commit blob by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...commitCid?ledger=mydb:main"

# Fetch a config blob by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...configCid?ledger=mydb:main"

# Fetch an index leaf by CID
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/v1/fluree/storage/objects/bafybeig...leafCid?ledger=mydb:main"
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
PREFIX f: <https://ns.flur.ee/db#>

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
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?ledger", "?branch", "?t"],
  "where": [
    { "@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger", "f:branch": "?branch", "f:t": "?t" }
  ],
  "orderBy": [{"var": "?t", "desc": true}]
}
```

**Request Body (SPARQL):**

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?ledger ?branch ?t
WHERE {
  ?ns a f:LedgerSource ;
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

Ledger records (`@type: "f:LedgerSource"`):
- `f:ledger` - Ledger name
- `f:branch` - Branch name
- `f:t` - Transaction number
- `f:status` - "ready" or "retracted"
- `f:ledgerCommit` - Commit ContentId reference
- `f:ledgerIndex` - Index info with ContentId and t

Graph source records (`@type: "f:GraphSourceDatabase"`):
- `f:name` - Graph source name
- `f:branch` - Branch name
- `f:config` - Configuration JSON
- `f:dependencies` - Source ledger dependencies
- `f:indexId` - Index ContentId
- `f:indexT` - Index t value

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
    "@context": {"f": "https://ns.flur.ee/db#"},
    "select": ["?ledger"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
  }'

# Find all graph sources
curl -X POST http://localhost:8090/nameservice/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"f": "https://ns.flur.ee/db#"},
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
      "ledger_id": "mydb:main",
      "branch": "main",
      "commit_t": 5,
      "index_t": 5,
      "created": "2024-01-22T10:00:00.000Z",
      "last_updated": "2024-01-22T10:30:00.000Z"
    },
    {
      "ledger_id": "mydb:dev",
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

### GET /ledgers/:id

Get metadata for a specific ledger.

**URL:**
```
GET /ledgers/{ledger-id}
```

**Path Parameters:**
- `ledger-id`: Ledger identifier (format: `name:branch`)

**Response:**

```json
{
  "ledger_id": "mydb:main",
  "branch": "main",
  "commit_t": 5,
  "index_t": 5,
  "commit_id": "bafybeig...commitT5",
  "index_id": "bafybeig...indexRootT5",
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
  "ledger_id": "mydb:main",
  "config": {
    "default_context": "http://example.org/context.jsonld"
  }
}
```

**Response:**

```json
{
  "ledger_id": "mydb:main",
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
  -d '{"ledger_id": "mydb:main"}'
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
| `ledger` | string | Yes | Ledger ID (e.g., "mydb" or "mydb:main") |

**Response:**

```json
{
  "ledger": "mydb:main",
  "t": 0,
  "commit_id": "bafybeig...commitT0"
}
```

| Field | Description |
|-------|-------------|
| `ledger` | Normalized ledger ID |
| `t` | Transaction time (0 for new ledger) |
| `commit_id` | ContentId of the initial commit |

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

# Create with short ledger ID (auto-resolves to :main)
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
| `ledger` | string | Yes | Ledger ID (e.g., "mydb" or "mydb:main") |
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
| `ledger` | Normalized ledger ID |
| `status` | One of: `"dropped"`, `"already_retracted"`, `"not_found"` |
| `files_deleted` | File counts (only populated for hard drop) |

**Status Codes:**
- `200 OK` - Drop successful (or already dropped/not found)
- `400 Bad Request` - Invalid request body
- `401 Unauthorized` - Bearer token required (when admin auth enabled)
- `500 Internal Server Error` - Server error

**Drop Sequence:**

1. Normalizes the ledger ID (ensures branch suffix like `:main`)
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

# Drop with short ledger ID (auto-resolves to :main)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb"}'
```

### GET /fluree/info

Get ledger metadata. Used by the CLI for `info`, `push`, `pull`, and `clone`.

**URL:**
```
GET /fluree/info?ledger={ledger-id}
```

**Query Parameters:**
- `ledger` (required): Ledger ID (e.g., "mydb" or "mydb:main")

**Alternative:** Use the `fluree-ledger` header instead of query parameter.

**Response (non-proxy mode):**

Returns comprehensive ledger metadata including namespace codes, property stats, and class counts. Always includes:

```json
{
  "ledger_id": "mydb:main",
  "t": 42,
  "commitId": "bafybeig...headCommitCid",
  "indexId": "bafybeig...indexRootCid",
  "namespaces": { ... },
  "properties": { ... },
  "classes": [ ... ]
}
```

**Response (proxy storage mode):**

Returns simplified nameservice-only metadata:

```json
{
  "ledger_id": "mydb:main",
  "t": 42,
  "commit_head_id": "bafybeig...commitCid",
  "index_head_id": "bafybeig...indexCid"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger_id` | string | Yes | Canonical ledger ID |
| `t` | integer | **Yes** | Current transaction time. Used by push/pull for head comparison. |
| `commitId` | string | No | Head commit CID (non-proxy mode) |
| `commit_head_id` | string | No | Head commit CID (proxy mode) |

> **Important:** The `t` field is required by the CLI for push/pull/clone operations. See [CLI-Server API Contract](../design/cli-server-contract.md) for details.

**Optional query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `realtime_property_details` | boolean | false | Compute live property stats (slower) |
| `include_property_datatypes` | boolean | false | Include datatype info for properties |

**Status Codes:**
- `200 OK` - Ledger found
- `400 Bad Request` - Missing ledger parameter
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Ledger not found

**Examples:**

```bash
# Get ledger info
curl "http://localhost:8090/fluree/info?ledger=mydb:main"

# With auth token
curl "http://localhost:8090/fluree/info?ledger=mydb:main" \
  -H "Authorization: Bearer eyJ..."
```

### GET /fluree/exists

Check if a ledger exists in the nameservice.

**URL:**
```
GET /fluree/exists?ledger={ledger-id}
```

**Query Parameters:**
- `ledger`: Ledger ID (e.g., "mydb" or "mydb:main")

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
| `ledger` | string | Ledger ID (echoed back) |
| `exists` | boolean | Whether the ledger is registered in the nameservice |

**Status Codes:**
- `200 OK` - Check completed successfully (regardless of whether ledger exists)
- `400 Bad Request` - Missing ledger parameter
- `500 Internal Server Error` - Server error

**Usage Notes:**

This is a lightweight check that only queries the nameservice without loading the ledger data. Use this to:

- Check if a ledger exists before attempting to load it
- Implement conditional create-or-load logic
- Validate ledger IDs in application code

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
POST /index/bm25?ledger={ledger-id}
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
POST /index/vector?ledger={ledger-id}
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
POST /admin/index?ledger={ledger-id}
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
POST /admin/compact?ledger={ledger-id}
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

All endpoints may return error responses in this format (and should return `Content-Type: application/json`):

```json
{
  "error": "Human-readable error message",
  "status": 409,
  "@type": "err:db/Conflict",
  "cause": {
    "error": "Optional nested error detail",
    "status": 409,
    "@type": "err:db/SomeInnerError"
  }
}
```

See [Errors and Status Codes](errors.md) for complete error reference.

## CLI Compatibility Requirements

This section summarizes the contract that third-party server implementations (e.g., Solo) must follow to be compatible with the Fluree CLI (`fluree-db-cli`). The CLI discovers the API base URL via `fluree remote add` and constructs endpoint URLs as `{base_url}/{operation}/{ledger}`.

### Required endpoints

| Endpoint | CLI commands |
|----------|-------------|
| `GET /info/{ledger}` | `info`, `push`, `pull`, `clone` |
| `POST /query/{ledger}` | `query` (JSON-LD and SPARQL) |
| `POST /insert/{ledger}` | `insert` |
| `POST /upsert/{ledger}` | `upsert` |
| `GET /exists/{ledger}` | `clone` (pre-create check) |
| `GET /ledgers` | `list --remote` |

For sync workflows (`clone`/`push`/`pull`), these additional endpoints are needed:

| Endpoint | CLI commands | Notes |
|----------|-------------|-------|
| `POST /push/{ledger}` | `push` | Required for push |
| `GET /commits/{ledger}` | `clone`, `pull` | Paginated export fallback |
| `POST /pack/{ledger}` | `clone`, `pull` | Preferred bulk transport; CLI falls back to `/commits` on 404/405/501 |
| `GET /storage/ns/{ledger}` | `clone`, `pull` | Pack preflight (head CID discovery) |

### Critical response field: `t`

The `GET /info/{ledger}` response **must** include a `t` field (integer) representing the current transaction time. This field is used by the CLI for:

- **push**: Comparing `local_t` vs `remote_t` to determine what commits to send and detect divergence
- **pull**: Comparing `remote_t` vs `local_t` to determine if new commits are available
- **clone**: Guarding against cloning empty ledgers (`t == 0`) and displaying progress

Omitting `t` from the info response will cause `push` and `pull` to fail with `"remote ledger-info response missing 't'"`.

### Transaction response format

The `/insert` and `/upsert` endpoints should return a JSON object. The CLI displays the full response as pretty-printed JSON. Common fields include `t`, `tx-id`, and `commit.hash`, but the exact shape is not prescribed — the CLI does not parse individual fields from transaction responses.

### Authentication

All endpoints accept `Authorization: Bearer <token>`. On `401`, the CLI attempts a single token refresh (if OIDC is configured) and retries. See [Auth contract](../design/auth-contract.md) for the full authentication lifecycle.

### Error responses

Error bodies should be JSON with an `error` or `message` field. The CLI extracts the first available string from `message` or `error` for display. Plain-text error bodies are also accepted.

## Related Documentation

- [Overview](overview.md) - API overview and principles
- [Headers](headers.md) - HTTP headers and content types
- [Signed Requests](signed-requests.md) - Authentication
- [Errors](errors.md) - Error codes and troubleshooting
