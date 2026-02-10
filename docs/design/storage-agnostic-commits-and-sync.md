# Storage-agnostic commits and sync

Fluree uses **ContentId** (CIDv1) values as the primary identifiers for commits, index roots, and other immutable artifacts. This decouples the commit chain and nameservice references from any specific storage backend, enabling replication across different storage systems (filesystem, S3, IPFS, etc.) without rewriting commit data.

## Pack protocol (`fluree-pack-v1`)

The pack protocol enables efficient bulk transfer of CAS objects between Fluree instances. Instead of fetching each commit individually (one HTTP round-trip per commit), the pack protocol streams all missing objects in a single binary response.

### How it works

1. **Client** sends a `POST /pack/{ledger}` request with `want` (CIDs the client needs, typically the remote head) and `have` (CIDs the client already has, typically the local head)
2. **Server** walks the commit chain from each `want` backward until it reaches a `have`, collecting all missing commits and their referenced txn blobs
3. **Server** streams the objects as binary frames (oldest-first topological order)
4. **Client** decodes frames incrementally via a `BytesMut` buffer, verifies integrity of each object, and writes to local CAS

### Wire format

```text
[Preamble: FPK1 + version(1)] [Header frame] [Data|Error]* [End frame]
```

| Frame    | Type byte | Content |
|----------|-----------|---------|
| Header   | `0x00`    | JSON metadata (protocol, capabilities, commit count) |
| Data     | `0x01`    | CID binary + raw object bytes |
| Error    | `0x02`    | UTF-8 error message (terminates stream) |
| End      | `0xFF`    | End of stream |

### Client-side verification

Each data frame is verified before writing to CAS:
- **Commit-v2 blobs** (`FCV2` magic): Sub-range SHA-256 via `verify_commit_v2_blob()` (excludes trailing hash+sig block)
- **All other blobs** (txn, config): Full-bytes SHA-256 via `ContentId::verify()`

Integrity failure is terminal -- the entire ingest is aborted.

### Fallback

When the server does not support the pack endpoint (returns 404, 405, 406, or 501), CLI commands automatically fall back to:
- **Named-remote**: Paginated JSON export via `GET /commits/{ledger}`
- **Origin-based**: CID chain walk via `GET /storage/objects/{cid}`

### Implementation

| Component | Location |
|-----------|----------|
| Wire format (encode/decode) | `fluree-db-core/src/pack.rs` |
| Server-side pack generation | `fluree-db-api/src/pack.rs` |
| Server HTTP endpoint | `fluree-db-server/src/routes/pack.rs` |
| Client-side streaming ingest | `fluree-db-nameservice-sync/src/pack_client.rs` |
| Origin fetcher pack methods | `fluree-db-nameservice-sync/src/origin.rs` |

For the full design document including index pack, graph source packing, and protocol evolution, see `STORAGE_AGNOSTIC_COMMITS_AND_SYNC.md` (repo root).
