# Design

Architecture and design documents for Fluree's internal systems. These documents describe the rationale behind key design decisions, wire formats, and trait architectures.

## Documents

### [Auth Contract (CLI ↔ Server)](auth-contract.md)

Wire-level contract between the Fluree CLI and any Fluree-compatible server, covering OIDC device auth, token refresh, and storage proxy authentication.

### [Nameservice Schema v2](nameservice-schema-v2.md)

Design of the nameservice schema: ledger records, graph source records, configuration payloads, and the ref/config/tracking store abstractions.

### [Storage-agnostic Commits and Sync](storage-agnostic-commits-and-sync.md)

How ContentId (CIDv1) values decouple the commit chain from storage backends, enabling replication across filesystem, S3, and IPFS. Includes the pack protocol wire format for efficient bulk transfer.

### [ContentId and ContentStore](content-id-and-contentstore.md)

The content-addressed identity layer: `ContentId` type, `ContentStore` trait, multicodec content kinds, and the bridge between CID-based identity and storage-backend addressing.

### [Index Format](index-format.md)

Binary columnar index format: branch/leaf/leaflet hierarchy, dictionary artifacts, SPOT/PSOT/POST/OPST/TSPO layout, and encoding details.

### [Storage Traits](storage-traits.md)

Storage trait architecture: `StorageRead`, `StorageWrite`, `ContentAddressedWrite`, `Storage`, and `NameService` trait design with guidance for implementing new backends.

## Related Documentation

- [Crate Map](../reference/crate-map.md) - Workspace architecture
- [Contributing](../contributing/README.md) - Development guidelines
- [Design Notes](../contributing/design-notes.md) - Internal naming conventions and architectural principles
