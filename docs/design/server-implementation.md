# Server implementation guide (CLI compatibility)

This document defines the HTTP endpoints and behaviors a Fluree-compatible server must implement to support the Fluree CLI (`fluree-db-cli`). It covers the full lifecycle: discovery, ledger management, data operations, and replication. A server that implements these endpoints will be interoperable with `fluree clone`, `fluree push`, `fluree pull`, `fluree publish`, and all other CLI sync operations.

For the auth discovery contract, see [Auth contract (CLI ↔ Server)](auth-contract.md).
For the full endpoint reference (including query and transaction), see [Endpoints](../api/endpoints.md).

## Endpoint summary

The CLI uses the following endpoints. All paths are relative to `api_base_url` (discovered via `GET /.well-known/fluree.json`).

### Tier 1: Core (minimum viable CLI support)

These endpoints enable `fluree create`, `fluree push`, `fluree publish`, `fluree info`, and basic ledger management.

| Method | Path | Auth scope | CLI commands | Purpose |
|--------|------|-----------|--------------|---------|
| `GET` | `/.well-known/fluree.json` | None | `remote add`, `auth login` | Auth discovery |
| `POST` | `/create` | Admin (optional) | `create --remote`, `publish` | Create empty ledger |
| `GET` | `/exists?ledger={id}` | Data read (optional) | `publish`, `clone` | Check ledger existence |
| `GET` | `/info/{ledger}` | Data read | `info`, `push`, `pull`, `publish` | Ledger metadata (head t, commit CID) |
| `POST` | `/push/{ledger}` | Data write | `push`, `publish` | Push precomputed commits |

### Tier 2: Replication (clone/pull with full history)

These endpoints enable `fluree clone` and `fluree pull` with index transfer. They require replication-grade permissions (`fluree.storage.*`).

| Method | Path | Auth scope | CLI commands | Purpose |
|--------|------|-----------|--------------|---------|
| `POST` | `/pack/{ledger}` | Storage | `clone`, `pull` | Bulk download (pack stream) |
| `GET` | `/commits/{ledger}` | Storage | `clone`, `pull` (fallback) | Paginated commit export |
| `GET` | `/storage/ns/{ledger}` | Storage | `clone`, `pull` | Nameservice record (head CIDs) |
| `GET` | `/storage/objects/{cid}` | Storage | `clone` (origin-based) | Fetch individual CAS object |

### Tier 3: Query and transaction (full server)

These are the standard data-plane endpoints. See [Endpoints](../api/endpoints.md) for complete documentation.

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/query/{ledger}` | JSON-LD or SPARQL query |
| `POST` | `/insert/{ledger}` | Insert transaction |
| `POST` | `/upsert/{ledger}` | Upsert transaction |
| `POST` | `/update/{ledger}` | WHERE/DELETE/INSERT transaction |
| `POST` | `/drop` | Drop a ledger |

---

## Endpoint specifications

### POST /create

Create a new empty ledger (genesis state, t=0).

**Request:**

```http
POST {api_base_url}/create
Content-Type: application/json
Authorization: Bearer <token>  (when admin auth is enabled)

{"ledger": "mydb:main"}
```

The `ledger` field is the canonical ledger ID. If no branch suffix is provided (e.g., `"mydb"`), the server MUST normalize to `"mydb:main"`.

**Response (201 Created):**

```json
{
  "ledger": "mydb:main",
  "t": 0,
  "commit_id": "bafybeig..."
}
```

**Error responses:**
- `409 Conflict` — Ledger already exists.
- `400 Bad Request` — Missing or invalid `ledger` field.

**Implementation notes:**
- The server MUST create the ledger in its nameservice with `t=0` and an empty commit state.
- The CLI calls this endpoint during `fluree publish` (if the remote ledger doesn't exist) and `fluree create --remote`.

### GET /exists?ledger={id}

Lightweight existence check. Does NOT load ledger data.

**Request:**

```http
GET {api_base_url}/exists?ledger=mydb:main
```

Alternative: pass ledger via `fluree-ledger` header.

**Response (200 OK, always):**

```json
{"ledger": "mydb:main", "exists": true}
```

**Implementation notes:**
- MUST return 200 regardless of whether the ledger exists (the `exists` field carries the result).
- Should query the nameservice only — no ledger data loading.

### GET /info/{ledger}

Return ledger metadata including the current head.

**Request:**

```http
GET {api_base_url}/info/mydb:main
Authorization: Bearer <token>
```

Alternative: `GET /info?ledger=mydb:main` or `fluree-ledger` header.

**Response (200 OK):**

```json
{
  "ledger_id": "mydb:main",
  "t": 42,
  "commitId": "bafybeig...headCommitCid"
}
```

**Required fields for CLI compatibility:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `t` | integer | **Yes** | Current transaction time. The CLI uses this for push/pull head comparison. |
| `commitId` | string | Recommended | Head commit CID. Used by push to verify chain continuity. |

**Error responses:**
- `404 Not Found` — Ledger not found (or out of scope for the token).

### POST /push/{ledger}

Accept precomputed commit v2 blobs from a client.

**Request:**

```http
POST {api_base_url}/push/mydb:main
Content-Type: application/json
Authorization: Bearer <token>
Idempotency-Key: <string>  (optional)

{
  "commits": ["<base64-commit-1>", "<base64-commit-2>"],
  "blobs": {
    "bafybeig...txnCid": "<base64-txn-bytes>"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `commits` | string[] | Yes | Base64-encoded commit v2 blobs, ordered oldest → newest. |
| `blobs` | map | No | `{cid: base64Bytes}` for referenced txn blobs. |

**Response (200 OK):**

```json
{
  "ledger": "mydb:main",
  "accepted": 3,
  "head": {
    "t": 42,
    "commit_id": "bafybeig...newHead"
  }
}
```

**Error responses:**
- `409 Conflict` — Head changed between info check and push, or histories diverged.
- `422 Unprocessable Entity` — Invalid commit bytes, missing txn blob, or validation failure.

**Validation requirements:**

The server MUST validate pushed commits:
1. **Sequence continuity** — Each commit's `previous_ref` must reference the prior commit's CID. The first commit's `previous_ref` must match the server's current head.
2. **Monotonic t** — Transaction times must be strictly increasing.
3. **Integrity** — Each commit v2 blob must pass `verify_commit_v2_blob` (SHA-256 over the canonical sub-range).
4. **Txn blob references** — Every commit that references a txn CID must have the corresponding blob provided in `blobs`.

**Implementation notes:**
- The CLI sends ALL commits in a single request. For the `fluree publish` command (first push of a new ledger), this includes the entire commit chain.
- If `Idempotency-Key` is provided, the server MAY implement idempotent replay (return the previous success response for the same key).

### POST /pack/{ledger}

Stream all missing CAS objects (commits, txn blobs, index artifacts) as a binary pack.

**Request:**

```http
POST {api_base_url}/pack/mydb:main
Content-Type: application/json
Authorization: Bearer <storage-token>

{
  "protocol": "fluree-pack-v1",
  "want": ["bafybeig...remoteHead"],
  "have": [],
  "include_indexes": true,
  "want_index_root_id": "bafybeig...indexRoot",
  "have_index_root_id": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `protocol` | string | Yes | Must be `"fluree-pack-v1"`. |
| `want` | string[] | Yes | Commit CIDs the client wants (typically remote head). |
| `have` | string[] | No | Commit CIDs the client already has (empty for full clone). |
| `include_indexes` | boolean | No | Whether to include index artifacts. Default: false. |
| `want_index_root_id` | string | No | Index root CID to transfer (required when `include_indexes` is true). |
| `have_index_root_id` | string | No | Client's existing index root CID (for incremental index transfer). |

**Response (200 OK):**

```http
Content-Type: application/x-fluree-pack
Transfer-Encoding: chunked
```

Binary stream using the `fluree-pack-v1` wire format:

```
[Preamble: "FPK1" + version(1)]
[Header frame]        — JSON: protocol, capabilities, commit_count, index_artifact_count, estimated_total_bytes
[Data frame]*         — Commit blobs (oldest-first), each followed by its txn blob
[Manifest frame]?     — {"phase": "indexes", "root_id": "...", "artifact_count": N}
[Data frame]*         — Index artifacts (dict blobs, branch manifests, leaf files, etc.)
[End frame]
```

**Frame encoding:**

Each frame: `[type: u8] [length: u32 BE] [payload: length bytes]`

| Type byte | Name | Payload |
|-----------|------|---------|
| `0x00` | Header | JSON metadata |
| `0x01` | Data | CID binary (varint-length-prefixed) + raw object bytes |
| `0x02` | Error | UTF-8 error message |
| `0x03` | Manifest | JSON metadata |
| `0xFF` | End | Empty |

**CID encoding in Data frames:**

The CID is encoded as: `[cid_length: varint] [cid_bytes: cid_length bytes]` followed by the raw object bytes (remaining payload after CID).

**Object ordering:**
- Commits MUST be ordered oldest-first (parents before children).
- Each commit SHOULD be immediately followed by its txn blob (if any), deduplicated.
- Index artifacts follow the Manifest frame and may be in any order.

**Integrity:**
- Commit v2 blobs (`FCV2` magic): SHA-256 over `bytes[0..hash_offset]` (excluding trailing hash+signature block).
- All other blobs: SHA-256 over full bytes. The CID encodes the expected digest.

**Fallback:**

If the server does not support the pack endpoint, it SHOULD return `404`, `405`, or `501`. The CLI will automatically fall back to paginated `GET /commits/{ledger}`.

### GET /commits/{ledger}

Paginated commit export. Fallback when pack is unavailable.

**Request:**

```http
GET {api_base_url}/commits/mydb:main?limit=500
Authorization: Bearer <storage-token>
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 500 | Maximum commits per page. |
| `cursor` | string | None | Cursor from previous page's `next_cursor_id`. |

**Response (200 OK):**

```json
{
  "head_commit_id": "bafybeig...head",
  "head_t": 42,
  "count": 500,
  "commits": [
    {"cid": "bafybeig...c1", "bytes": "<base64>"},
    {"cid": "bafybeig...c2", "bytes": "<base64>"}
  ],
  "blobs": {
    "bafybeig...txn1": "<base64>"
  },
  "next_cursor_id": "bafybeig...lastOnPage"
}
```

Pages walk backward from head. When `next_cursor_id` is null, the client has received all commits.

### GET /storage/ns/{ledger}

Return the nameservice record for a ledger. Used by clone/pull for head CID discovery before requesting a pack.

**Request:**

```http
GET {api_base_url}/storage/ns/mydb:main
Authorization: Bearer <storage-token>
```

**Response (200 OK):**

```json
{
  "commit_head_id": "bafybeig...commitHead",
  "commit_t": 42,
  "index_head_id": "bafybeig...indexRoot",
  "index_t": 40
}
```

### GET /storage/objects/{cid}

Fetch a single CAS object by its ContentId. Used by origin-based clone (CID chain walking).

**Request:**

```http
GET {api_base_url}/storage/objects/bafybeig...cid?ledger=mydb:main
Authorization: Bearer <storage-token>
```

**Response (200 OK):**

```http
Content-Type: application/octet-stream
X-Fluree-Content-Kind: commit | txn | index-root | index-branch | index-leaf | dict | ...

<raw bytes>
```

---

## Auth scopes

The CLI uses two separate token scopes:

### Data API scope (`fluree.ledger.*`)

Used for ledger management and data operations: query, transact, push, info, exists, create.

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.ledger.read.all` | boolean | Read access to all ledgers. |
| `fluree.ledger.read.ledgers` | string[] | Read access to specific ledgers. |
| `fluree.ledger.write.all` | boolean | Write access to all ledgers. |
| `fluree.ledger.write.ledgers` | string[] | Write access to specific ledgers. |

### Storage/replication scope (`fluree.storage.*`)

Used for replication endpoints: pack, commits, storage/ns, storage/objects. These provide raw CAS access and should be restricted to operator/service principals.

| Claim | Type | Description |
|-------|------|-------------|
| `fluree.storage.all` | boolean | Storage access to all ledgers. |
| `fluree.storage.ledgers` | string[] | Storage access to specific ledgers. |

### Scope requirements by endpoint

| Endpoint | Required scope |
|----------|---------------|
| `POST /create` | Admin token (when enabled) or data write |
| `GET /exists` | Data read (optional — may work without auth) |
| `GET /info/{ledger}` | Data read |
| `POST /push/{ledger}` | Data write |
| `POST /pack/{ledger}` | Storage |
| `GET /commits/{ledger}` | Storage |
| `GET /storage/ns/{ledger}` | Storage |
| `GET /storage/objects/{cid}` | Storage |

---

## CLI workflow reference

### `fluree publish <remote> [ledger]`

Publishes a local ledger to a remote server (create + push + configure upstream).

```
CLI                                    Server
 |
 +-- ledger_exists() ──────────────> GET /exists?ledger=mydb:main
 |   ← {"exists": false}
 |
 +-- create_ledger() ──────────────> POST /create
 |   ← 201 {"ledger": "mydb:main", "t": 0}
 |
 +-- [walk local commit chain, collect all commits oldest→newest]
 |
 +-- push_commits() ───────────────> POST /push/mydb:main
 |   body: {commits: [...], blobs: {...}}
 |   ← 200 {"accepted": N, "head": {"t": 42, "commit_id": "..."}}
 |
 +-- [configure upstream tracking locally]
```

If the remote ledger already exists but is empty (t=0), the create step is skipped and commits are pushed directly.

### `fluree clone <remote> <ledger>`

Downloads a ledger from a remote server.

```
CLI                                    Server
 |
 +-- ledger_info() ────────────────> GET /info/mydb:main
 |   ← {"t": 42, "commitId": "..."}
 |
 +-- [create local empty ledger]
 |
 +-- fetch_ns_record() ────────────> GET /storage/ns/mydb:main
 |   ← {"commit_head_id": "...", "index_head_id": "..."}
 |
 +-- fetch_pack() ─────────────────> POST /pack/mydb:main
 |   body: {want: [head_cid], have: [], include_indexes: true, ...}
 |   ← binary pack stream (commits + index artifacts)
 |
 +-- [ingest all objects into local CAS]
 +-- [set commit head + index head]
 +-- [configure upstream tracking]
```

If the pack endpoint returns 404/405/501, the CLI falls back to paginated `GET /commits/{ledger}`.

### `fluree push [ledger]`

Pushes new local commits to a configured upstream.

```
CLI                                    Server
 |
 +-- ledger_info() ────────────────> GET /info/mydb:main
 |   ← {"t": 40, "commitId": "...remoteHead"}
 |
 +-- [compare local head (t=42) with remote head (t=40)]
 +-- [walk local chain from head to remote_t, collect commits where t > 40]
 +-- [verify chain continuity: oldest commit's previous_ref == remote commitId]
 |
 +-- push_commits() ───────────────> POST /push/mydb:main
 |   body: {commits: [t=41, t=42], blobs: {...}}
 |   ← 200 {"accepted": 2, "head": {"t": 42}}
```

### `fluree pull [ledger]`

Fetches and fast-forwards from upstream (inverse of push).

```
CLI                                    Server
 |
 +-- fetch_ns_record() ────────────> GET /storage/ns/mydb:main
 |   ← {"commit_head_id": "...", "commit_t": 42}
 |
 +-- [if remote_t > local_t:]
 |
 +-- fetch_pack() ─────────────────> POST /pack/mydb:main
 |   body: {want: [remote_head], have: [local_head], include_indexes: true}
 |   ← binary pack stream (only missing objects)
 |
 +-- [ingest + fast-forward local head]
```

---

## Ledger portability (.flpack files)

The CLI supports exporting and importing full native ledgers as `.flpack` files using the same `fluree-pack-v1` wire format:

```bash
# Export a ledger (all commits + indexes + dictionaries)
fluree export mydb --format ledger -o mydb.flpack

# Import into a new instance (can use a different ledger name)
fluree create imported-db --from mydb.flpack
```

The `.flpack` file format is identical to the pack stream served by `POST /pack/{ledger}`, with the addition of a **nameservice manifest frame** (`{"phase": "nameservice", ...}`) that carries the commit and index head CIDs needed to reconstruct the nameservice record on import.

This enables ledger portability across instances without requiring a running server — the file contains everything needed to reconstruct the ledger in a new `.fluree/` directory.

---

## Implementer checklist

### Minimum (push/publish support)

- [ ] `GET /.well-known/fluree.json` — Return `{"version": 1}` (optionally with auth config)
- [ ] `POST /create` — Create empty ledger, return 201/409
- [ ] `GET /exists?ledger={id}` — Return `{"exists": bool}`
- [ ] `GET /info/{ledger}` — Return at minimum `{"t": N}` (and `commitId` for push chain verification)
- [ ] `POST /push/{ledger}` — Accept and validate commit blobs, return accepted count + new head

### Full replication (clone/pull support)

- [ ] `GET /storage/ns/{ledger}` — Return nameservice record with head CIDs
- [ ] `POST /pack/{ledger}` — Stream pack binary response
- [ ] `GET /commits/{ledger}` — Paginated commit export (fallback)
- [ ] `GET /storage/objects/{cid}` — Individual CAS object fetch

### Anti-leak semantics

For data-scoped endpoints, return `404 Not Found` (not `401` or `403`) when a token lacks access to a ledger. This prevents leaking ledger existence to unauthorized principals.
