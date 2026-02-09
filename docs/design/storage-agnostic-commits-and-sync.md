# Storage-agnostic commits and sync (design note)

This repo’s current implementation historically used storage addresses (e.g., `fluree:s3://...`, `fluree:file://...`) inside commit chains and nameservice refs, which couples replication to a specific backend.

For the “architecturally right” design that makes commits **purely content-addressed** (IDs only), supports shared backends like **IPFS** without rewriting, and enables pluggable nameservices including **Ethereum**, see the full design document in the repo root:

- `STORAGE_AGNOSTIC_COMMITS_AND_SYNC.md`

