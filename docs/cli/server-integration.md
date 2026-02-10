# Implementing Server Support For Fluree CLI

This document is for implementers building a custom server (for example in `../solo3/`) that wants to support the Fluree CLI end-to-end.

The CLI supports two broad categories of remote operations:

- **Data API**: query/transact/insert/upsert/info/exists (normal ledger operations).
- **Replication / sync**: clone/pull/fetch (content-addressed replication by CID, via pack + storage proxy).

## Base URL And Discovery

The CLI prefers to be configured with a server origin URL (scheme/host/port) and then uses discovery:

- `GET /.well-known/fluree.json` returns `api_base_url` (usually `/v1/fluree`)

The CLI stores the discovered base as the remote's `base_url` and constructs all other endpoints relative to it.

If you do not implement discovery, users must configure the CLI remote URL to already include the API base (for example `http://localhost:8090/v1`), and the CLI will append `/fluree` as needed.

## Minimum Endpoints By CLI Feature

### `fluree remote add`, `fluree auth login`

- `GET /.well-known/fluree.json`

### `fluree fetch` (nameservice refs only)

- `GET {api_base_url}/nameservice/snapshot`
- `POST {api_base_url}/nameservice/refs/:alias/commit`
- `POST {api_base_url}/nameservice/refs/:alias/index`
- `POST {api_base_url}/nameservice/refs/:alias/init`

### `fluree clone`, `fluree pull` (pack-first replication)

Required:

- `GET {api_base_url}/info/*ledger` (existence + remote `t` preflight; see `/info` minimum fields below)
- `GET {api_base_url}/storage/ns/:alias` (remote NsRecord, includes `commit_head_id` and optional `config_id`)
- `POST {api_base_url}/pack/*ledger` (binary `fluree-pack-v1` stream)

Fallbacks (strongly recommended):

- `GET {api_base_url}/commits/*ledger` (paginated export of commit + txn blobs)
- `GET {api_base_url}/storage/objects/:cid?ledger=:alias` (per-object fetch by CID)

### `fluree push` (commit ingestion)

- `POST {api_base_url}/push/*ledger`

This is not storage-proxy replication; it is a write operation and should be authorized like normal writes.

The CLI sends an `Idempotency-Key` header derived from the pushed commit bytes so servers can safely replay a successful push result if the client retries after a timeout.

### `fluree query`, `fluree insert`, `fluree upsert`, `fluree track`

- `POST {api_base_url}/query/*ledger`
- `POST {api_base_url}/insert/*ledger`
- `POST {api_base_url}/upsert/*ledger`

## Replication Auth Contract

Replication endpoints are intentionally protected more strictly than data reads:

- Pack + commit export + storage proxy endpoints require a Bearer token with `fluree.storage.*` permissions.
- Unauthorized requests should return `404 Not Found` (no existence leak) for these endpoints.

Data API endpoints use normal read/write auth (`fluree.ledger.read.*`, `fluree.ledger.write.*`) and should return `401/403/404` as appropriate for your product.

## Pack Protocol Contract

- Endpoint: `POST {api_base_url}/pack/*ledger`
- Request: JSON `PackRequest` with `"protocol":"fluree-pack-v1"`
- Response: `Content-Type: application/x-fluree-pack`, streaming frames:
  - Preamble `FPK1` + version byte
  - Header frame (mandatory, first)
  - Data frames: CID binary + raw object bytes
  - Optional Manifest frames (phase transitions)
  - End frame (mandatory termination)

Clients verify integrity:

- Commit-v2 blobs (`FCV2` magic): sub-range hash verification.
- All other objects: full-bytes hash verification by CID.

**Graceful fallback:** If you do not implement pack yet, return `404 Not Found`, `405 Method Not Allowed`, `406 Not Acceptable`, or `501 Not Implemented`. The CLI treats those as "pack not supported" and falls back to `GET /commits` plus `GET /storage/objects/:cid`.

## Storage Proxy Contract

These endpoints exist so a client can fetch bytes by CID without knowing storage layout:

- `GET {api_base_url}/storage/ns/:alias` returns `NsRecord` JSON with CID identity fields:
  - `commit_head_id`, `commit_t`, `index_head_id`, `index_t`, optional `config_id`
- `GET {api_base_url}/storage/objects/:cid?ledger=:alias` returns raw bytes for the CID after verifying integrity.

`/storage/block` is only required for query peers that need server-mediated index-leaf access.

## `/info` Response Contract (CLI Minimum)

The CLI currently treats `GET {api_base_url}/info/*ledger` as an opaque JSON object, but it requires these fields:

- `t` (integer): required for `fluree clone` and `fluree pull` preflight and for `fluree push` conflict checks.
- `commitId` (string CID): required for `fluree push` when `t > 0` so it can detect divergence.

Other fields are optional and may be used only for display.

## Origin-Based Replication (LedgerConfig)

The CLI can do origin-based `clone --origin` and `pull` fallback without a named remote by fetching objects via:

- `GET {api_base_url}/storage/objects/:cid?ledger=:alias`

If your nameservice advertises `config_id` on the NsRecord, the CLI will attempt to fetch that `LedgerConfig` blob (by CID) and then use it to try additional origins.

## Quick Validation Script

From a clean project directory:

```bash
fluree init
fluree remote add origin http://localhost:8090
fluree auth login --remote origin --token @token.txt

fluree fetch origin
fluree clone origin mydb:main
fluree pull mydb:main
fluree push mydb:main
```
