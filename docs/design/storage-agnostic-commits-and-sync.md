# Storage-agnostic commits and sync

Fluree uses **ContentId** (CIDv1) values as the primary identifiers for commits, index roots, and other immutable artifacts. This decouples the commit chain and nameservice references from any specific storage backend, enabling replication across different storage systems (filesystem, S3, IPFS, etc.) without rewriting commit data.

For the full design document, see:

- `STORAGE_AGNOSTIC_COMMITS_AND_SYNC.md` (repo root)

