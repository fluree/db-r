# Nameservice Schema v2 Design

**Schema Version: 2**

## Overview

This document describes the design for a unified nameservice schema that supports:

1. **Ledgers** with named graphs and independent indexing
2. **Non-ledger graph sources** (historically “Virtual Graphs”: BM25, Iceberg, Vector, JDBC, etc.) with varying versioning semantics
3. **Four independent atomic concerns** that can be updated without contention
4. **Watermarked updates** for client subscription and push notifications
5. **Pluggable backends** (DynamoDB, S3, filesystem) with consistent semantics

## Design Goals

- **Stable schema**: Minimize attribute changes as features evolve
- **Flexible payloads**: Use JSON Maps for evolving/variable content
- **Reduced conflict probability**: Logically independent concerns minimize contention
- **Client subscriptions**: Watermarks enable efficient change detection
- **Coordination via status**: Soft locks/leases for distributed process coordination

---

## The Four Concerns Model

Each nameservice record has four independent concerns, each with its own watermark and payload:

| # | Concern | Watermark | Payload | Updated By |
|---|---------|-----------|---------|------------|
| 1 | **Head** | `head_v` | `head` | Transactor (on commit) |
| 2 | **Index** | `index_v` | `index` | Indexer (on index publish) |
| 3 | **Status** | `status_v` | `status` | Various (state changes, metrics, locks) |
| 4 | **Config** | `config_v` | `config` | Admin (settings changes) |

Each concern can be pushed independently without affecting or contending with the others.

---

## DynamoDB Schema

### Table Name

`fluree-nameservice` (configurable)

### Attributes

| Attribute | DynamoDB Type | Key | Description |
|-----------|---------------|-----|-------------|
| `pk` | String | Partition Key | Alias identifier (e.g., `"mydb:main"`, `"vg:search:main"`) |
| `v` | Number | | Schema version (always `2` for this schema) |
| `type` | String | | Entity type discriminator |
| `name` | String | | Base name without branch (e.g., `"mydb"`) |
| `branch` | String | | Branch name (e.g., `"main"`) |
| `deps` | List\<String\> | | Dependencies (VGs only, nullable) |
| `created_at` | Number | | Unix epoch seconds, immutable after creation |
| `retracted` | Boolean | | `true` if soft-deleted, `false` otherwise |
| `head_v` | Number | | Watermark for head concern |
| `head` | Map | | Head state payload (JSON) |
| `index_v` | Number | | Watermark for index concern |
| `index` | Map | | Index state payload (JSON) |
| `status_v` | Number | | Watermark for status concern |
| `status` | Map | | Status payload (JSON) |
| `config_v` | Number | | Watermark for config concern |
| `config` | Map | | Config payload (JSON) |

**Total: 16 attributes**

### Design Note: Per-Concern Independence

Each concern is logically independent:
- **No shared `updated_at`**: Each concern's watermark (`head_v`, `index_v`, etc.) serves as its timestamp
- **Disjoint attribute sets**: Updating one concern does not touch any attributes of another concern
- **Reduced conflict probability**: Independent concerns minimize logical contention

**Important**: DynamoDB still serializes writes per item. Concurrent writers to the same `pk` can still race and require retries even when updating different concerns. The independence reduces *logical* conflicts (CAS failures due to watermark checks) but does not eliminate *physical* contention at the item level.

### Entity Types

The `type` attribute discriminates between entity kinds:

| Type Value | Description |
|------------|-------------|
| `ledger` | Fluree ledger with commits, indexes, named graphs |
| `vg:bm25` | BM25 full-text search graph source (legacy: virtual graph) |
| `vg:vector` | Vector similarity search graph source (legacy: virtual graph) |
| `vg:iceberg` | Apache Iceberg table graph source (legacy: virtual graph) |
| `vg:jdbc` | JDBC connection graph source (legacy: virtual graph; live, no versioning) |
| `vg:r2rml` | R2RML relational mapping graph source (legacy: virtual graph) |
| `vg:*` | Future non-ledger graph source types (legacy prefix: `vg:`) |

---

## Watermark Semantics

All watermarks are **strict monotonic counters** that increment on every publish. This ensures:
1. Clients can detect changes by comparing watermarks
2. No change is ever "invisible" to subscribers
3. Simple comparison logic: `if remote_v > local_v then changed`

### head_v (Commit Watermark)

- **Value**: Equals the commit `t` (transaction time)
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Commits are already strictly ordered by `t`, so `t` IS the version

### index_v (Index Watermark)

- **Value**: Atomic incrementing integer (1, 2, 3, ...)
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Index state is multi-dimensional (named graphs at different `t` values). The watermark always increments, even for reindex operations at the same `t`. The actual `t` and `rev` values are in the payload.
- **Reindex handling**: A reindex at `t=42` increments `index_v` and updates `index.{graph}.rev` in the payload

### status_v (Status Watermark)

- **Value**: Atomic incrementing integer
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Status has no `t` relation; version is just a change counter

### config_v (Config Watermark)

- **Value**: Atomic incrementing integer
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Config has no `t` relation; version is just a change counter

### Unborn State Semantics

When a record is initialized but has no data yet for a concern:

| Concern | Unborn Watermark | Unborn Payload | Meaning |
|---------|------------------|----------------|---------|
| `head` | `head_v = 0` | `head = null` | Ledger initialized, no commits yet |
| `index` | `index_v = 0` | `index = null` | No index published yet |
| `status` | `status_v = 1` | `status = {state: "ready"}` | Always has initial status |
| `config` | `config_v = 0` | `config = null` | No config set yet |

**Key distinction**:
- `*_v = 0` with `payload = null`: Initialized but unborn (record exists)
- Record not found (GetItem returns nothing): Unknown/never created

---

## Payload Schemas

### head (Ledger)

```json
{
  "address": "fluree:s3://bucket/commits/abc123.json",
  "t": 42
}
```

| Field | Type | Description |
|-------|------|-------------|
| `address` | String | Storage address of the commit |
| `t` | Number | Transaction time (redundant with `head_v` but explicit) |

### head (Virtual Graph)

For VGs that sync from a source ledger:

```json
{
  "sync_t": 42,
  "sync_address": "fluree:s3://..."
}
```

For VGs with no sync concept (e.g., JDBC): `null` or `{}`

### index (Ledger with Named Graphs)

```json
{
  "default": {
    "address": "fluree:s3://bucket/index/default-t42-r0.avro",
    "t": 42,
    "rev": 0
  },
  "txn-metadata": {
    "address": "fluree:s3://bucket/index/txn-meta-t42-r1.avro",
    "t": 42,
    "rev": 1
  },
  "audit-log": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `{named-graph}` | Object \| null | Index state per named graph |
| `.address` | String | Storage address of the index root |
| `.t` | Number | Transaction time the index covers |
| `.rev` | Number | Revision at that `t` (0, 1, 2... for reindex operations) |

**Named graph = `null`** means that graph exists but hasn't been indexed yet.

### index (Virtual Graph)

For VGs with snapshots (BM25, Vector, Iceberg):

```json
{
  "address": "fluree:s3://bucket/bm25/snapshot-42.bin",
  "sync_t": 42,
  "snapshot_id": "abc123"
}
```

For VGs with no index concept (JDBC): `null`

### status

```json
{
  "state": "ready",
  "queue_depth": 3,
  "last_commit_ms": 45
}
```

| Field | Type | Description |
|-------|------|-------------|
| `state` | String | Current state (see State Values below) |
| `*` | Any | Additional metadata varies by state and entity type |

#### State Values

| State | Description | Typical Metadata |
|-------|-------------|------------------|
| `ready` | Normal operating state (default initial state) | `queue_depth`, `last_commit_ms` |
| `indexing` | Background indexing in progress | `index_lock` |
| `reindexing` | Full reindex in progress | `reindex_lock`, `progress` |
| `syncing` | VG syncing from source | `progress`, `source_t`, `synced_t` |
| `migrating` | Schema/data migration | `migration_lock` |
| `retracted` | Soft-deleted | `retracted_at`, `reason` |
| `error` | Error state | `error`, `error_at` |

### status with Locks (Coordination)

```json
{
  "state": "indexing",
  "index_lock": {
    "holder": "indexer-7f3a",
    "target_t": 45,
    "acquired_at": 1705312200,
    "expires_at": 1705316100
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `index_lock` | Object \| null | Soft lock for indexing coordination |
| `.holder` | String | Identifier of the process holding the lock |
| `.target_t` | Number | The `t` being indexed |
| `.acquired_at` | Number | Unix epoch when lock was acquired |
| `.expires_at` | Number | Unix epoch when lock expires (lease timeout) |

### config

```json
{
  "default_context": "fluree:s3://contexts/v1.json",
  "index_threshold": 1000,
  "replication": {
    "factor": 3,
    "regions": ["us-east-1", "us-west-2"]
  }
}
```

Config is fully flexible JSON. Common fields:

| Field | Type | Description |
|-------|------|-------------|
| `default_context` | String | Default JSON-LD context address |
| `index_threshold` | Number | Commits before auto-index |
| `replication` | Object | Replication settings |

For Virtual Graphs, config contains type-specific settings:

**BM25:**
```json
{
  "k1": 1.2,
  "b": 0.75,
  "fields": ["title", "body", "description"]
}
```

**JDBC:**
```json
{
  "connection_string": "jdbc:postgresql://host:5432/db",
  "schema": "public",
  "pool_size": 10
}
```

---

## DynamoDB Operations

### CAS Semantics (Git-like Push)

All push operations support **compare-and-set (CAS)** semantics with expected old values. This enables Git-like divergence detection:

- Caller provides `expected` (the last-known state) and `new` (the desired state)
- Backend rejects if current state doesn't match `expected`
- On rejection, backend returns `actual` current state for caller to reconcile

This is stronger than simple watermark monotonicity: it detects divergence, not just staleness.

### Create (Initialize)

```
Operation: PutItem
ConditionExpression: attribute_not_exists(#pk)
Item: {
  pk: "mydb:main",
  v: 2,
  type: "ledger",
  name: "mydb",
  branch: "main",
  deps: null,
  created_at: <now>,
  retracted: false,
  head_v: 0,
  head: null,
  index_v: 0,
  index: null,
  status_v: 1,
  status: { state: "ready" },
  config_v: 0,
  config: null
}
```

### push_head (Publish Commit)

**Option A: Monotonic only** (simpler, allows fast-forward by any newer commit)
```
Operation: UpdateItem
Key: { pk: "mydb:main" }
ConditionExpression: attribute_not_exists(#hv) OR #hv < :new_t
UpdateExpression: SET #hv = :new_t, #h = :head
ExpressionAttributeNames: {
  "#hv": "head_v",
  "#h": "head"
}
ExpressionAttributeValues: {
  ":new_t": 42,
  ":head": { "address": "fluree:s3://...", "t": 42 }
}
```

**Option B: CAS with expected value** (Git-like, detects divergence)

CAS checks both watermark equality AND payload equality. The condition is a single OR'd expression handling both existing and unborn cases:

```
Operation: UpdateItem
Key: { pk: "mydb:main" }

// Single condition: existing case OR unborn case
ConditionExpression:
  (#hv = :expected_v AND #h = :expected_head AND :new_t > :expected_v)
  OR
  (#hv = :zero AND attribute_type(#h, :null_type) AND :new_t > :zero)

UpdateExpression: SET #hv = :new_t, #h = :head
ExpressionAttributeNames: {
  "#hv": "head_v",
  "#h": "head"
}
ExpressionAttributeValues: {
  ":expected_v": 41,                                              // caller's last-known v
  ":expected_head": { "address": "fluree:s3://.../t41.json", "t": 41 },  // caller's last-known payload
  ":new_t": 42,
  ":head": { "address": "fluree:s3://.../t42.json", "t": 42 },
  ":zero": 0,
  ":null_type": "NULL"
}
```

**Caller logic**: Set `:expected_v` and `:expected_head` based on last-known state:
- If unborn: `:expected_v = 0`, `:expected_head` can be any value (the unborn clause matches on `#hv = :zero`)
- If existing: `:expected_v = last_v`, `:expected_head = last_payload`

**Note**: DynamoDB *does* support nested paths like `#h.#addr` (with `#h=head`, `#addr=address`). However, comparing the entire map (`#h = :expected_head`) is simpler and avoids partial-match edge cases.

**Recommendation**: Use Option B (CAS) for transactors to detect divergence. Use Option A for distributed sync where fast-forward is acceptable.

### push_index (Publish Index)

**CAS with expected watermark + monotonic enforcement:**
```
Operation: UpdateItem
Key: { pk: "mydb:main" }
ConditionExpression: (#iv = :expected_v AND :new_v > :expected_v)
                     OR
                     (#iv = :zero AND attribute_type(#i, :null_type) AND :expected_v = :zero AND :new_v > :zero)
UpdateExpression: SET #iv = :new_v, #i = :index
ExpressionAttributeNames: {
  "#iv": "index_v",
  "#i": "index"
}
ExpressionAttributeValues: {
  ":expected_v": 16,
  ":zero": 0,
  ":new_v": 17,
  ":index": {
    "default": { "address": "...", "t": 42, "rev": 0 },
    "txn-metadata": { "address": "...", "t": 42, "rev": 1 }
  },
  ":null_type": "NULL"
}
```

**Note**: The condition enforces both CAS (`#iv = :expected_v`) AND monotonicity (`:new_v > :expected_v`). The unborn clause also enforces monotonicity (`:new_v > :zero`). The `rev` field in the payload tracks rebuilds at the same `t`.

### push_status (Update Status)

```
Operation: UpdateItem
Key: { pk: "mydb:main" }
ConditionExpression: (#sv = :expected_v AND :new_v > :expected_v)
                     OR
                     (attribute_not_exists(#sv) AND :expected_v = :zero)
UpdateExpression: SET #sv = :new_v, #s = :status
ExpressionAttributeNames: {
  "#sv": "status_v",
  "#s": "status"
}
ExpressionAttributeValues: {
  ":expected_v": 89,
  ":zero": 0,
  ":new_v": 90,
  ":status": { "state": "ready", "queue_depth": 0 }
}
```

**Note**: `status_v` starts at 1 (not 0) on creation, so `attribute_not_exists(#sv)` handles legacy/migration cases where the attribute is missing. Normal updates use the first clause.

### push_config (Update Config)

```
Operation: UpdateItem
Key: { pk: "mydb:main" }
ConditionExpression: (#cv = :expected_v AND :new_v > :expected_v)
                     OR
                     (#cv = :zero AND attribute_type(#c, :null_type) AND :expected_v = :zero)
UpdateExpression: SET #cv = :new_v, #c = :config
ExpressionAttributeNames: {
  "#cv": "config_v",
  "#c": "config"
}
ExpressionAttributeValues: {
  ":expected_v": 2,
  ":zero": 0,
  ":new_v": 3,
  ":config": { "default_context": "...", "index_threshold": 500 },
  ":null_type": "NULL"
}
```

**Note**: Unborn clause checks both `#cv = :zero` AND `attribute_type(#c, NULL)` to prevent accepting writes against inconsistent states.

### Retract

```
Operation: UpdateItem
Key: { pk: "mydb:main" }
UpdateExpression: SET #r = :true, #sv = :new_sv, #s = :status
ExpressionAttributeNames: {
  "#r": "retracted",
  "#sv": "status_v",
  "#s": "status"
}
ExpressionAttributeValues: {
  ":true": true,
  ":new_sv": 91,
  ":status": { "state": "retracted", "retracted_at": 1705315800 }
}
```

### Lookup (Read)

```
Operation: GetItem
Key: { pk: "mydb:main" }
ConsistentRead: true
```

Returns full record with all concerns.

### List by Type

```
Operation: Query (requires GSI on type)
KeyConditionExpression: #type = :type
ExpressionAttributeNames: { "#type": "type" }
ExpressionAttributeValues: { ":type": "ledger" }
```

Or use Scan with FilterExpression if GSI not available.

---

## Push Result Handling

Each push operation returns one of:

| Result | Meaning | Action |
|--------|---------|--------|
| `Updated` | Update accepted | Proceed |
| `Conflict` | Expected didn't match current | Reconcile using `actual` |

### Rust Types (aligned with existing RefKind/CasResult vocabulary)

```rust
/// Which concern is being read or updated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConcernKind {
    /// The commit head pointer (head_v + head payload)
    Head,
    /// The index state (index_v + index payload)
    Index,
    /// The status state (status_v + status payload)
    Status,
    /// The config state (config_v + config payload)
    Config,
}

/// Value of a concern: watermark + optional payload.
///
/// - `Some(ConcernValue { v: 0, payload: None })` — unborn (initialized, no data)
/// - `Some(ConcernValue { v: N, payload: Some(...) })` — has data
/// - `None` (at Option level) — record doesn't exist
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConcernValue<T> {
    pub v: i64,
    pub payload: Option<T>,
}

/// Outcome of a compare-and-set push operation.
///
/// Conflicts are NOT errors — they are expected outcomes of concurrent
/// writes and must be handled by the caller (retry, report, etc.).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult<T> {
    /// CAS succeeded — the concern was updated to the new value.
    Updated,
    /// CAS failed — `expected` did not match the current value.
    /// `actual` carries the current concern value so the caller can decide
    /// what to do next (retry, diverge, etc.).
    Conflict { actual: Option<ConcernValue<T>> },
}
```

### Conflict Handling

On `Conflict`, the caller receives the actual current state and can:

1. **Fast-forward**: If `actual.v < new.v`, retry with `expected = actual`
2. **Divergence**: If `actual.v >= new.v` or addresses differ unexpectedly, handle merge/error
3. **Retry loop**: For distributed systems, implement bounded retry with backoff

```rust
async fn push_with_retry<T>(
    ns: &impl ConcernPublisher<T>,
    alias: &str,
    kind: ConcernKind,
    new: ConcernValue<T>,
    max_retries: usize,
) -> Result<CasResult<T>> {
    let mut expected = ns.get_concern(alias, kind).await?;

    for _ in 0..max_retries {
        match ns.push_concern(alias, kind, expected.as_ref(), &new).await? {
            CasResult::Updated => return Ok(CasResult::Updated),
            CasResult::Conflict { actual } => {
                // Check if fast-forward is still possible
                if let Some(ref act) = actual {
                    if new.v <= act.v {
                        // Diverged - can't fast-forward
                        return Ok(CasResult::Conflict { actual });
                    }
                }
                // Retry with new expected
                expected = actual;
            }
        }
    }

    // Exhausted retries
    let actual = ns.get_concern(alias, kind).await?;
    Ok(CasResult::Conflict { actual })
}
```

---

## Example Records

### Ledger (Ready State)

```json
{
  "pk": "mydb:main",
  "v": 2,
  "type": "ledger",
  "name": "mydb",
  "branch": "main",
  "deps": null,
  "created_at": 1705312200,
  "retracted": false,

  "head_v": 42,
  "head": {
    "address": "fluree:s3://bucket/commits/mydb/main/t42.json",
    "t": 42
  },

  "index_v": 17,
  "index": {
    "default": {
      "address": "fluree:s3://bucket/index/mydb/main/default-t42-r0.avro",
      "t": 42,
      "rev": 0
    },
    "txn-metadata": {
      "address": "fluree:s3://bucket/index/mydb/main/txn-meta-t40-r0.avro",
      "t": 40,
      "rev": 0
    }
  },

  "status_v": 89,
  "status": {
    "state": "ready",
    "queue_depth": 3,
    "last_commit_ms": 45
  },

  "config_v": 2,
  "config": {
    "default_context": "fluree:s3://bucket/contexts/v1.json",
    "index_threshold": 1000
  }
}
```

### Ledger (Unborn - Just Created)

```json
{
  "pk": "newdb:main",
  "v": 2,
  "type": "ledger",
  "name": "newdb",
  "branch": "main",
  "deps": null,
  "created_at": 1705312200,
  "retracted": false,

  "head_v": 0,
  "head": null,

  "index_v": 0,
  "index": null,

  "status_v": 1,
  "status": {
    "state": "ready"
  },

  "config_v": 0,
  "config": null
}
```

### Ledger (Indexing State with Lock)

```json
{
  "pk": "mydb:main",
  "v": 2,
  "type": "ledger",
  "...": "...",

  "status_v": 90,
  "status": {
    "state": "indexing",
    "index_lock": {
      "holder": "indexer-7f3a",
      "target_t": 45,
      "acquired_at": 1705312200,
      "expires_at": 1705316100
    },
    "progress": 0.45
  }
}
```

### Ledger (Retracted)

```json
{
  "pk": "olddb:main",
  "v": 2,
  "type": "ledger",
  "name": "olddb",
  "branch": "main",
  "deps": null,
  "created_at": 1705300000,
  "retracted": true,

  "head_v": 100,
  "head": {
    "address": "fluree:s3://bucket/commits/olddb/main/t100.json",
    "t": 100
  },

  "index_v": 50,
  "index": { "...": "..." },

  "status_v": 200,
  "status": {
    "state": "retracted",
    "retracted_at": 1705312200,
    "reason": "Migrated to newdb"
  },

  "config_v": 5,
  "config": { "...": "..." }
}
```

### Virtual Graph (BM25)

```json
{
  "pk": "vg:search:main",
  "v": 2,
  "type": "vg:bm25",
  "name": "search",
  "branch": "main",
  "deps": ["mydb:main"],
  "created_at": 1705312200,
  "retracted": false,

  "head_v": 0,
  "head": null,

  "index_v": 5,
  "index": {
    "address": "fluree:s3://bucket/bm25/search/snapshot-t42.bin",
    "sync_t": 42
  },

  "status_v": 12,
  "status": {
    "state": "ready",
    "sync_lag": 0
  },

  "config_v": 1,
  "config": {
    "k1": 1.2,
    "b": 0.75,
    "fields": ["title", "body"]
  }
}
```

### Virtual Graph (Iceberg)

```json
{
  "pk": "vg:analytics:main",
  "v": 2,
  "type": "vg:iceberg",
  "name": "analytics",
  "branch": "main",
  "deps": ["mydb:main"],
  "created_at": 1705312200,
  "retracted": false,

  "head_v": 0,
  "head": null,

  "index_v": 3,
  "index": {
    "address": "s3://iceberg-warehouse/analytics/metadata/v3.metadata.json",
    "snapshot_id": "1234567890123456789",
    "sync_t": 42
  },

  "status_v": 5,
  "status": {
    "state": "ready"
  },

  "config_v": 1,
  "config": {
    "warehouse": "s3://iceberg-warehouse",
    "catalog": "glue",
    "database": "fluree_exports"
  }
}
```

### Virtual Graph (JDBC - No Versioning)

```json
{
  "pk": "vg:erp:main",
  "v": 2,
  "type": "vg:jdbc",
  "name": "erp",
  "branch": "main",
  "deps": null,
  "created_at": 1705312200,
  "retracted": false,

  "head_v": 0,
  "head": null,

  "index_v": 0,
  "index": null,

  "status_v": 1,
  "status": {
    "state": "ready"
  },

  "config_v": 1,
  "config": {
    "connection_string": "jdbc:postgresql://erp.internal:5432/production",
    "schema": "public",
    "pool_size": 10,
    "read_only": true
  }
}
```

---

## Git-like Push Model

The nameservice follows a git-like model where:

1. **Local nameservice**: Each node has a local NS for reads and local writes
2. **Upstream nameservice**: The "source of truth" that accepts or rejects pushes
3. **Push operations**: Local changes are pushed upstream
4. **Forward operations**: Requests can be forwarded upstream without local write

```
┌─────────────────┐         push_head         ┌─────────────────────┐
│  Transactor     │ ────────────────────────▶ │                     │
│  (local NS)     │                           │   Upstream NS       │
└─────────────────┘                           │                     │
                                              │  - DynamoDB, or     │
┌─────────────────┐         push_index        │  - S3 + ETags, or   │
│  Indexer        │ ────────────────────────▶ │  - FS + locks, or   │
│  (local NS)     │                           │  - Service          │
└─────────────────┘                           │                     │
        ▲                                     │  Enforces:          │
        │              pull/sync              │  - Watermark rules  │
        └─────────────────────────────────────│  - Serialization    │
                                              └─────────────────────┘
```

### Upstream NS Backend Options

| Backend | How It Enforces Rules |
|---------|----------------------|
| **DynamoDB** | Conditional expressions on watermarks |
| **S3** | ETags for CAS + application logic |
| **Filesystem** | File locks or single-writer process |
| **Service** | Queue + application logic |

The push interface is the same regardless of backend.

---

## Status-based Coordination (Soft Locks)

Status can carry soft locks for coordinating distributed processes:

### Lock Acquisition Flow

```
1. Indexer starts up
2. Read current status
3. If index_lock exists and not expired:
     → Another indexer is working, wait or skip
4. If no lock or lock expired:
     → Push status with our lock claim (status_v + 1)
     → If accepted: we own the lock, proceed
     → If rejected: someone else claimed it, back off
5. Do indexing work (periodically refresh lock by pushing status)
6. Push index update
7. Push status: clear lock, set state to ready
```

### Lock Expiry (Crash Recovery)

If a process crashes while holding a lock:
- The `expires_at` timestamp allows other processes to take over
- No manual intervention needed
- Typical lease duration: 5-15 minutes depending on operation

### Lock Refresh

Long-running operations should periodically refresh their lock:

```json
{
  "state": "indexing",
  "index_lock": {
    "holder": "indexer-7f3a",
    "target_t": 45,
    "acquired_at": 1705312200,
    "expires_at": 1705316100,
    "refreshed_at": 1705314000
  },
  "progress": 0.67
}
```

---

## Client Subscription Model

Clients track watermarks to detect changes:

```json
{
  "subscriptions": {
    "mydb:main": {
      "type": "ledger",
      "head_v": 42,
      "index_v": 17,
      "status_v": 89,
      "config_v": 2
    },
    "vg:search:main": {
      "type": "vg:bm25",
      "head_v": 0,
      "index_v": 5,
      "status_v": 12,
      "config_v": 1
    }
  }
}
```

### Change Detection

1. Client polls or receives notification
2. Compare watermarks: `if remote.head_v > local.head_v`
3. Fetch only the changed concern(s)
4. Update local cache

### Subscription Granularity

Clients can subscribe to:
- **All concerns** for an alias
- **Specific concerns** (e.g., only `head_v` for a query client)
- **All aliases** of a type (e.g., all ledgers)

---

## File-backed Nameservice Considerations

The logical schema (16 attributes, 4 concerns) can be stored in different physical layouts depending on the backend and deployment needs.

### Layout Options

**Option A: Single File (Unified)**
```
ns@v2/{name}/{branch}.json
```
- Contains all four concerns in one file
- Simplest for reads (one fetch)
- Requires single-writer discipline or file-level CAS

**Option B: Separate Head and Index Files (Current Implementation)**
```
ns@v2/{name}/{branch}.json        # head + status + config
ns@v2/{name}/{branch}.index.json  # index only
```
- Matches current implementation
- Allows transactor and indexer to write independently
- 2 files to read per entity for full state
- **Trade-off**: Status and config updates contend with head updates at file-lock level. Acceptable if status updates are low-frequency (state changes only, not high-frequency metrics).

**Option C: Fully Separate Files (Maximum Independence)**
```
ns@v2/{name}/{branch}.head.json
ns@v2/{name}/{branch}.index.json
ns@v2/{name}/{branch}.status.json
ns@v2/{name}/{branch}.config.json
```
- Each concern in its own file
- Maximum write independence
- 4 files to read per entity

### Recommended Approach

Use **Option B** (separate head/index) as the default:
- Proven in current implementation
- Solves the main contention issue (transactor vs indexer)
- Reasonable read overhead (2 files)
- **Constraint**: Status updates should be coarse-grained (state transitions, not per-transaction metrics). If high-frequency status updates are needed, consider Option C.

Use **Option C** (fully separate files) when:
- Status updates are frequent (e.g., real-time queue depth reporting)
- Multiple independent processes update different concerns
- Write independence is more important than read efficiency

For **queryable nameservice** with many entities:
- Read files in parallel
- Consider in-memory caching with file-change notification
- The 2-file layout is acceptable; 4-file layout may add too much I/O

### Atomicity Mechanisms

| Backend | Mechanism | Notes |
|---------|-----------|-------|
| Filesystem | Atomic rename (write to temp, rename) | POSIX guarantees |
| S3 | ETags for CAS | `If-Match` header |
| GCS | Generation numbers | Similar to ETags |

### File Content Format

Each file contains JSON matching the concern's payload plus metadata:

**head file** (`{name}/{branch}.json`):
```json
{
  "v": 2,
  "pk": "mydb:main",
  "type": "ledger",
  "name": "mydb",
  "branch": "main",
  "created_at": 1705312200,
  "retracted": false,
  "deps": null,
  "head_v": 42,
  "head": { "address": "...", "t": 42 },
  "status_v": 89,
  "status": { "state": "ready", ... },
  "config_v": 2,
  "config": { ... }
}
```

**index file** (`{name}/{branch}.index.json`):
```json
{
  "v": 2,
  "pk": "mydb:main",
  "index_v": 17,
  "index": {
    "default": { "address": "...", "t": 42, "rev": 0 },
    "txn-metadata": { "address": "...", "t": 40, "rev": 0 }
  }
}
```

---

## Migration from v1 Schema

### Current v1 Attributes

| v1 Attribute | v2 Mapping |
|--------------|------------|
| `ledger_alias` | `pk` |
| `ledger_name` | `name` |
| `branch` | `branch` |
| `commit_address` | `head.address` |
| `commit_t` | `head_v` and `head.t` |
| `index_address` | `index.default.address` |
| `index_t` | `index.default.t` (set `index_v = 1` on migration) |
| `default_context_address` | `config.default_context` (set `config_v = 1`) |
| `status` | `status.state` (set `status_v = 1`) |
| `updated_at` | Removed (each concern has its own watermark) |
| (new) | `v = 2` (schema version) |
| (new) | `retracted = (status == "retracted")` |

### Migration Strategy

1. **Read compatibility**: Support reading both v1 and v2 formats
   - Check for `v` attribute: if missing or `v < 2`, treat as v1
   - Map v1 fields to v2 structure on read
2. **Write in v2**: All new writes use v2 format
3. **Lazy migration**: Convert v1 records to v2 on first write
   - Set initial watermarks: `index_v = 1`, `status_v = 1`, `config_v = 1`
4. **Explicit migration**: Batch job to convert all records

---

## Future Considerations

### Global Secondary Indexes (GSIs)

| GSI Name | Partition Key | Sort Key | Use Case |
|----------|---------------|----------|----------|
| `type-index` | `type` | `pk` | List all entities of a type |
| `state-index` | `status_state` | `pk` | Find entities in specific state |

**Note on `state-index`**: DynamoDB GSIs cannot use nested map attributes as keys. To enable this GSI:

1. Add an **optional denormalized attribute** `status_state` (String) at the top level
2. Update `status_state` whenever `status.state` changes
3. This attribute is **not part of the core 16-attribute schema** — only add it if you need GSI-based queries by state

**Alternative**: Use Scan with FilterExpression on `status.state` (less efficient but no schema extension needed)

### Streams and Events

DynamoDB Streams can be enabled to:
- Trigger Lambda on changes
- Build event sourcing
- Replicate to other regions

### Multi-region

For global deployments:
- Use DynamoDB Global Tables
- Or regional nameservices with cross-region sync

---

## Appendix: Attribute Reference

| Attribute | Type | Nullable | Mutable | Description |
|-----------|------|----------|---------|-------------|
| `pk` | String | No | No | Primary key, alias identifier |
| `v` | Number | No | No | Schema version (always `2`) |
| `type` | String | No | No | Entity type discriminator |
| `name` | String | No | No | Base name |
| `branch` | String | No | No | Branch name |
| `deps` | List | Yes | Yes | Dependencies (VGs only) |
| `created_at` | Number | No | No | Creation timestamp (epoch seconds) |
| `retracted` | Boolean | No | Yes | Soft-delete flag |
| `head_v` | Number | No | Yes | Head watermark (= `t` for ledgers) |
| `head` | Map | Yes | Yes | Head payload |
| `index_v` | Number | No | Yes | Index watermark (atomic counter) |
| `index` | Map | Yes | Yes | Index payload |
| `status_v` | Number | No | Yes | Status watermark (atomic counter) |
| `status` | Map | No | Yes | Status payload (always present) |
| `config_v` | Number | No | Yes | Config watermark (atomic counter) |
| `config` | Map | Yes | Yes | Config payload |

**Total: 16 attributes**

### Watermark Semantics Summary

| Watermark | Semantics | Initial Value | Update Rule |
|-----------|-----------|---------------|-------------|
| `head_v` | = commit `t` | 0 (unborn) | Strict: `new > current` |
| `index_v` | Counter | 0 (unborn) | Strict: `new > current` |
| `status_v` | Counter | 1 (ready) | Strict: `new > current` |
| `config_v` | Counter | 0 (unborn) | Strict: `new > current` |
