# Storage Traits Design

This document describes the storage trait architecture in Fluree DB, explaining the design rationale and providing guidance for implementing new storage backends.

## Overview

Fluree uses a layered storage abstraction that separates:
- **Core traits** (`fluree-db-core`): Runtime-agnostic storage operations with standard `Result<T>` error handling
- **Extension traits** (`fluree-db-nameservice`): Nameservice-specific operations with `StorageExtResult<T>` for richer error semantics

This separation allows the core library to remain simple while nameservice implementations can leverage advanced storage features (CAS operations, pagination, etc.).

## Quick Start: The Prelude

For convenient imports, use the storage prelude:

```rust
use fluree_db_core::prelude::*;

// Now you have access to:
// - Storage, StorageRead, StorageWrite, ContentAddressedWrite (traits)
// - MemoryStorage, FileStorage (implementations)
// - ContentKind, ContentWriteResult, ReadHint (types)

async fn example<S: Storage>(storage: &S) -> Result<()> {
    let bytes = storage.read_bytes("some/address").await?;
    storage.write_bytes("other/address", &bytes).await?;
    Ok(())
}
```

For API consumers, `fluree-db-api` re-exports all storage traits:

```rust
use fluree_db_api::{Storage, StorageRead, MemoryStorage};
```

## Trait Hierarchy

```text
                    ┌─────────────────┐
                    │   StorageRead   │  read_bytes, exists, list_prefix
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │  StorageWrite   │  write_bytes, delete
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │   ContentAddressedWrite     │  content_write_bytes[_with_hash]
              └──────────────┬──────────────┘
                             │
                    ┌────────┴────────┐
                    │     Storage     │  (marker trait - blanket impl)
                    └─────────────────┘
```

## Core Traits (fluree-db-core)

### StorageRead

Read-only storage operations. Implement this for any storage that can retrieve data.

```rust
#[async_trait]
pub trait StorageRead: Debug + Send + Sync {
    /// Read raw bytes from an address
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>>;

    /// Read with a hint for content type optimization
    /// Default implementation ignores the hint
    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        self.read_bytes(address).await
    }

    /// Check if an address exists
    async fn exists(&self, address: &str) -> Result<bool>;

    /// List all addresses with a given prefix
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
```

**Design notes:**
- `read_bytes_hint` enables optimizations like returning pre-encoded flakes for leaf nodes
- `list_prefix` is essential for garbage collection and administrative operations
- All methods return `fluree_db_core::Result<T>` (alias for `std::result::Result<T, Error>`)

### StorageWrite

Mutating storage operations. Implement alongside `StorageRead` for read-write storage.

```rust
#[async_trait]
pub trait StorageWrite: Debug + Send + Sync {
    /// Write raw bytes to an address
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()>;

    /// Delete data at an address
    async fn delete(&self, address: &str) -> Result<()>;
}
```

**Design notes:**
- `delete` is part of the core write trait (not separate) because any writable storage should support deletion
- Implementations should be idempotent: deleting a non-existent address succeeds silently

### ContentAddressedWrite

Extension trait for content-addressed (hash-based) writes. Extends `StorageWrite`.

```rust
#[async_trait]
pub trait ContentAddressedWrite: StorageWrite {
    /// Write bytes with a pre-computed content hash
    /// Returns the canonical address and metadata
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_address: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult>;

    /// Write bytes, computing the hash internally
    /// Default implementation computes SHA-256 and delegates
    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_address: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let hash = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_address, &hash, bytes).await
    }
}
```

**Design notes:**
- `ContentKind` indicates whether data is a commit or index, enabling routing to different storage tiers
- The default `content_write_bytes` implementation handles hash computation, so most backends only need to implement `content_write_bytes_with_hash`
- Content-addressed storage enables deduplication and integrity verification

### Storage (Marker Trait)

A convenience marker trait indicating full storage capability.

```rust
/// Full storage capability: read + content-addressed write
pub trait Storage: StorageRead + ContentAddressedWrite {}

/// Blanket implementation for any type implementing both traits
impl<T: StorageRead + ContentAddressedWrite> Storage for T {}
```

**Usage:**
```rust
// Instead of this verbose bound:
fn process<S: StorageRead + StorageWrite + ContentAddressedWrite>(storage: &S)

// Use this:
fn process<S: Storage>(storage: &S)
```

## Extension Traits (fluree-db-nameservice)

The nameservice crate defines additional traits with `StorageExtResult<T>` for richer error handling (e.g., `PreconditionFailed` for CAS operations).

### StorageList

Paginated listing for large-scale storage backends.

```rust
#[async_trait]
pub trait StorageList {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>>;

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<ListResult>;
}
```

### StorageCas

Compare-and-swap operations for consistent distributed updates.

```rust
#[async_trait]
pub trait StorageCas {
    /// Write only if the address doesn't exist
    async fn write_if_absent(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool>;

    /// Write only if the current version matches expected_etag
    async fn write_if_match(
        &self,
        address: &str,
        bytes: &[u8],
        expected_etag: &str,
    ) -> StorageExtResult<String>;

    /// Read with version/etag for subsequent CAS operations
    async fn read_with_etag(&self, address: &str) -> StorageExtResult<(Vec<u8>, String)>;
}
```

### StorageDelete (nameservice)

Delete with nameservice error semantics.

```rust
#[async_trait]
pub trait StorageDelete {
    async fn delete(&self, address: &str) -> StorageExtResult<()>;
}
```

**Why separate from core `StorageWrite::delete`?**
- Nameservice operations need `StorageExtResult` for errors like `PreconditionFailed`
- Core operations use standard `Result` for simplicity
- Storage backends typically implement both, with the nameservice version delegating to core

## Implementing a Storage Backend

### Minimal Read-Only Backend

For a read-only backend (e.g., `ProxyStorage` that fetches via HTTP):

```rust
#[async_trait]
impl StorageRead for MyReadOnlyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        // Fetch from remote
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        // Check existence (can implement as try-read)
        match self.read_bytes(address).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(Error::storage("list_prefix not supported"))
    }
}

// Must also implement StorageWrite (with error stubs) and ContentAddressedWrite
// if you want to satisfy the Storage marker trait
#[async_trait]
impl StorageWrite for MyReadOnlyStorage {
    async fn write_bytes(&self, _: &str, _: &[u8]) -> Result<()> {
        Err(Error::storage("read-only storage"))
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Err(Error::storage("read-only storage"))
    }
}

#[async_trait]
impl ContentAddressedWrite for MyReadOnlyStorage {
    async fn content_write_bytes_with_hash(&self, ...) -> Result<ContentWriteResult> {
        Err(Error::storage("read-only storage"))
    }
}
```

### Full Read-Write Backend

For a complete backend (e.g., S3, filesystem):

```rust
// 1. Implement core traits
#[async_trait]
impl StorageRead for MyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> { ... }
    async fn exists(&self, address: &str) -> Result<bool> { ... }
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> { ... }
}

#[async_trait]
impl StorageWrite for MyStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> { ... }
    async fn delete(&self, address: &str) -> Result<()> { ... }
}

#[async_trait]
impl ContentAddressedWrite for MyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_address: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        // Build address from kind + alias + hash
        let address = build_content_address(kind, ledger_address, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

// Storage marker trait is automatically satisfied via blanket impl

// 2. Optionally implement nameservice traits for advanced features
#[async_trait]
impl StorageList for MyStorage {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        // Delegate to core trait, convert error
        StorageRead::list_prefix(self, prefix)
            .await
            .map_err(|e| StorageExtError::Other(e.to_string()))
    }
    // ... paginated version
}
```

## Type Erasure with AnyStorage

For dynamic dispatch (e.g., runtime-selected storage backends), use `AnyStorage`:

```rust
/// Type-erased storage wrapper
pub struct AnyStorage {
    inner: Arc<dyn Storage>,
}

impl AnyStorage {
    pub fn new<S: Storage + 'static>(storage: S) -> Self {
        Self { inner: Arc::new(storage) }
    }
}
```

**When to use:**
- `FlureeClient` uses `AnyStorage` to support any backend at runtime
- Generic code should prefer concrete types (`S: Storage`) for better optimization
- Use `AnyStorage` when storage type is determined at runtime (e.g., from config)

## Wrapper Storages

Several wrapper types add functionality to underlying storage:

### TieredStorage

Routes commits and indexes to different backends:

```rust
pub struct TieredStorage<S> {
    commit_storage: S,
    index_storage: S,
}
```

### EncryptedStorage

Adds transparent encryption:

```rust
pub struct EncryptedStorage<S, K> {
    inner: S,
    key_provider: K,
}
```

### AddressIdentifierResolverStorage

Routes reads based on address format (e.g., different storage for legacy addresses):

```rust
pub struct AddressIdentifierResolverStorage {
    default_storage: Arc<dyn Storage>,
    identifier_storages: HashMap<String, Arc<dyn Storage>>,
}
```

## Error Handling

### Core Errors (`fluree_db_core::Error`)

Standard errors for storage operations:
- `NotFound` - Address doesn't exist
- `Storage` - Generic storage failure
- `Io` - Underlying I/O error

### Nameservice Errors (`StorageExtError`)

Extended errors for nameservice operations:
- `NotFound` - Address doesn't exist
- `PreconditionFailed` - CAS condition not met
- `Other` - Generic error with message

## Summary

| Trait | Crate | Purpose | Error Type |
|-------|-------|---------|------------|
| `StorageRead` | core | Read operations | `Result<T>` |
| `StorageWrite` | core | Write + delete | `Result<T>` |
| `ContentAddressedWrite` | core | Hash-based writes | `Result<T>` |
| `Storage` | core | Marker (full capability) | - |
| `StorageList` | nameservice | Paginated listing | `StorageExtResult<T>` |
| `StorageCas` | nameservice | Compare-and-swap | `StorageExtResult<T>` |
| `StorageDelete` | nameservice | Delete with ext errors | `StorageExtResult<T>` |

For most use cases, implement the core traits (`StorageRead`, `StorageWrite`, `ContentAddressedWrite`) and the `Storage` marker trait will be automatically satisfied.
