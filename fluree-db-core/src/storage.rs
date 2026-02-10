//! Storage traits for reading and writing index data
//!
//! This module defines storage traits that apps must implement to provide
//! access to index files. The traits are runtime-agnostic and use
//! `async_trait` for async support.
//!
//! ## Traits
//!
//! - `StorageRead`: Read-only access to stored data (read, exists, list)
//! - `StorageWrite`: Mutating operations (write, delete)
//! - `ContentAddressedWrite`: Content-addressed writes (extends StorageWrite)
//! - `Storage`: Marker trait combining all capabilities
//!
//! ## Implementations
//!
//! Apps provide their own implementations:
//! - `fluree-db-native`: FileStorage (tokio::fs)
//! - `fluree-db-wasm`: Custom implementations (browser fetch API)
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_core::{StorageRead, StorageWrite, ContentAddressedWrite};
//!
//! struct MyStorage { /* ... */ }
//!
//! #[async_trait]
//! impl StorageRead for MyStorage {
//!     async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
//!         // Your implementation
//!     }
//!     async fn exists(&self, address: &str) -> Result<bool> {
//!         // Your implementation
//!     }
//!     async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
//!         // Your implementation
//!     }
//! }
//!
//! #[async_trait]
//! impl StorageWrite for MyStorage {
//!     async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
//!         // Your implementation
//!     }
//!     async fn delete(&self, address: &str) -> Result<()> {
//!         // Your implementation
//!     }
//! }
//! ```

use crate::address_path::ledger_id_to_path_prefix;
use crate::error::Result;
use async_trait::async_trait;
use sha2::Digest;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

// ============================================================================
// Read Hints
// ============================================================================

/// Hint to storage implementation about expected content type
///
/// This enum allows callers to signal format preferences to storage implementations.
/// The default implementation ignores the hint and returns raw bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadHint {
    /// Any bytes format (default, no special negotiation)
    ///
    /// Storage returns raw bytes as stored. This is the default behavior
    /// and matches the semantics of `read_bytes()`.
    #[default]
    AnyBytes,

    /// Prefer pre-parsed leaf flakes (FLKB format) if the address points to a leaf
    ///
    /// Storage implementations that support content negotiation (e.g., ProxyStorage)
    /// can use this hint to request policy-filtered flakes instead of raw bytes.
    /// If the address is not a leaf or the server doesn't support FLKB, falls back
    /// to raw bytes.
    PreferLeafFlakes,
}

// ============================================================================
// Core Traits
// ============================================================================

/// Read-only storage operations
///
/// This trait provides all non-mutating storage operations: reading bytes,
/// checking existence, and listing by prefix.
#[async_trait]
pub trait StorageRead: Debug + Send + Sync {
    /// Read raw bytes from the given address
    ///
    /// The address format is typically:
    /// `fluree:{identifier}:{method}://{path}`
    ///
    /// Returns `Error::NotFound` if the resource doesn't exist.
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>>;

    /// Read bytes with a format hint
    ///
    /// This method allows callers to signal a format preference to storage
    /// implementations that support content negotiation (e.g., ProxyStorage).
    ///
    /// The default implementation ignores the hint and delegates to `read_bytes()`.
    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        let _ = hint; // Default implementation ignores hint
        self.read_bytes(address).await
    }

    /// Check if a resource exists at the given address
    async fn exists(&self, address: &str) -> Result<bool>;

    /// List all objects under a prefix
    ///
    /// Returns all matching keys. May be expensive for large prefixes.
    ///
    /// # Warning
    ///
    /// This can be expensive for large prefixes. Use only for:
    /// - Development/debugging
    /// - Admin operations
    /// - Small, bounded prefixes
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Resolve a CAS address to a local filesystem path, if available.
    ///
    /// Returns `Some(path)` for storage backends where data is already on
    /// the local filesystem (e.g., `FileStorage`). Returns `None` for
    /// remote or in-memory backends.
    ///
    /// Callers use this to avoid redundant copy-to-cache when the data
    /// is already locally accessible.
    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        let _ = address;
        None
    }
}

/// Mutating storage operations
///
/// This trait provides basic write and delete operations.
#[async_trait]
pub trait StorageWrite: Debug + Send + Sync {
    /// Write bytes to the given address
    ///
    /// For content-addressed storage, this should be idempotent:
    /// if content already exists at address, this is a no-op.
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()>;

    /// Delete an object by address
    ///
    /// Returns `Ok(())` if the object was deleted or did not exist.
    /// This is idempotent: deleting a non-existent object succeeds.
    /// Only returns an error for actual failures (network, permissions, etc).
    async fn delete(&self, address: &str) -> Result<()>;
}

// ============================================================================
// Content-Addressed Write (Extension)
// ============================================================================

use crate::content_kind::{dict_kind_extension, ContentKind};

/// Result of a storage-owned content write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentWriteResult {
    /// Canonical address that should be persisted/referenced.
    pub address: String,
    /// Content hash (hex sha256 for built-in storages).
    pub content_hash: String,
    /// Number of bytes written (input length).
    pub size_bytes: usize,
}

/// Content-addressed write operations
///
/// This trait extends `StorageWrite` with the ability to write bytes while
/// letting storage determine the address based on content hash.
#[async_trait]
pub trait ContentAddressedWrite: StorageWrite {
    /// Write bytes using a caller-provided content hash (hex).
    ///
    /// This allows higher layers to control the hashing algorithm for certain
    /// content kinds (e.g. commit IDs that intentionally exclude non-deterministic
    /// fields like wall-clock timestamps), while keeping **storage in charge of
    /// layout** and returning the canonical address.
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult>;

    /// Write bytes, computing the content hash automatically (SHA-256).
    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash_hex, bytes)
            .await
    }
}

// ============================================================================
// Marker Trait
// ============================================================================

/// Identifies the storage method/scheme for CID-to-address mapping.
///
/// Every storage backend must declare its method name (e.g., `"file"`, `"memory"`,
/// `"s3"`). This is used by [`StorageContentStore`] to map `ContentId` values to
/// physical storage addresses via [`content_address`].
///
/// This trait is a supertrait of [`Storage`], ensuring that any type-erased
/// `dyn Storage` (e.g., [`AnyStorage`]) automatically includes `storage_method()`.
pub trait StorageMethod {
    /// Return the storage method identifier (e.g., `"file"`, `"memory"`, `"s3"`).
    fn storage_method(&self) -> &str;
}

/// Full storage capability marker
///
/// This trait combines `StorageRead`, `ContentAddressedWrite`, and `StorageMethod`,
/// providing a single bound for storage backends that support all operations.
///
/// Used for type erasure in `AnyStorage`.
pub trait Storage: StorageRead + ContentAddressedWrite + StorageMethod {}
impl<T: StorageRead + ContentAddressedWrite + StorageMethod> Storage for T {}

// ============================================================================
// ContentStore Trait (CID-first storage abstraction)
// ============================================================================

use crate::content_id::ContentId;

/// Content-addressed store operating on `ContentId` (CIDv1).
///
/// This trait is the CID-first replacement for the address-string-based
/// `Storage` traits above. The API is purely `id → bytes`: physical
/// layout (ledger namespacing, codec subdirectories, etc.) is the
/// implementation's concern, configured at construction time.
///
/// During the migration period, [`StorageContentStore`] provides a bridge
/// from existing `S: Storage` implementations to this trait.
#[async_trait]
pub trait ContentStore: Debug + Send + Sync {
    /// Check if an object exists by CID.
    async fn has(&self, id: &ContentId) -> Result<bool>;

    /// Retrieve object bytes by CID.
    async fn get(&self, id: &ContentId) -> Result<Vec<u8>>;

    /// Store bytes, computing CID from kind + bytes. Returns the CID.
    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId>;

    /// Store bytes with a caller-provided CID (for imports/replication).
    ///
    /// Implementations MUST call `id.verify(bytes)` and reject mismatches.
    ///
    /// **Not for commit-v2 blobs.** Commit CIDs are derived from a
    /// canonical sub-range of the blob, not the full bytes, so
    /// `id.verify(bytes)` will fail. Commit blobs should be written via
    /// `Storage::content_write_bytes_with_hash()` with the pre-verified
    /// canonical hash instead.
    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()>;

    /// Resolve a CID to a local filesystem path, if available.
    ///
    /// Returns `Some(path)` for storage backends where data is already on
    /// the local filesystem (e.g., `FileContentStore`). Returns `None` for
    /// remote or in-memory backends.
    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        let _ = id;
        None
    }
}

// ============================================================================
// MemoryContentStore (CID-first in-memory store)
// ============================================================================

/// In-memory content store for testing, keyed by `ContentId`.
///
/// This is the CID-first counterpart to [`MemoryStorage`].
#[derive(Debug, Clone)]
pub struct MemoryContentStore {
    data: Arc<RwLock<std::collections::HashMap<ContentId, Vec<u8>>>>,
}

impl Default for MemoryContentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryContentStore {
    /// Create a new empty in-memory content store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl ContentStore for MemoryContentStore {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        Ok(self.data.read().expect("RwLock poisoned").contains_key(id))
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        self.data
            .read()
            .expect("RwLock poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(id.to_string()))
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        self.data
            .write()
            .expect("RwLock poisoned")
            .insert(id.clone(), bytes.to_vec());
        Ok(id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        if !id.verify(bytes) {
            return Err(crate::error::Error::storage(format!(
                "CID verification failed: provided CID {} does not match bytes",
                id
            )));
        }
        self.data
            .write()
            .expect("RwLock poisoned")
            .insert(id.clone(), bytes.to_vec());
        Ok(())
    }
}

// ============================================================================
// StorageContentStore (bridge adapter: existing Storage → ContentStore)
// ============================================================================

/// Bridge adapter that wraps an existing `S: Storage` to provide `ContentStore`.
///
/// This is the critical piece for incremental migration: code that already has
/// `S: Storage` can obtain a `ContentStore` without rewriting storage
/// implementations.
///
/// The adapter is constructed with a ledger scope (`ledger_id`) and a
/// storage method name (e.g., `"file"`, `"memory"`, `"s3"`), which together
/// determine the layout rule for mapping CIDs to legacy address strings.
#[derive(Debug, Clone)]
pub struct StorageContentStore<S: Storage> {
    storage: S,
    ledger_id: String,
    method: String,
}

impl<S: Storage> StorageContentStore<S> {
    /// Create a new bridge adapter.
    ///
    /// # Arguments
    ///
    /// * `storage` - The underlying legacy storage implementation
    /// * `ledger_id` - Ledger identifier (e.g., `"mydb:main"`)
    /// * `method` - Storage method name for address generation (e.g., `"file"`, `"memory"`)
    pub fn new(storage: S, ledger_id: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            storage,
            ledger_id: ledger_id.into(),
            method: method.into(),
        }
    }

    /// Map a CID to a legacy address string using the existing layout.
    fn cid_to_address(&self, id: &ContentId) -> Result<String> {
        let kind = id.content_kind().ok_or_else(|| {
            crate::error::Error::storage(format!("unknown codec {} in CID {}", id.codec(), id))
        })?;
        let hex_digest = id.digest_hex();
        let addr = content_address(&self.method, kind, &self.ledger_id, &hex_digest);
        Ok(addr)
    }
}

#[async_trait]
impl<S: Storage + Send + Sync> ContentStore for StorageContentStore<S> {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        let address = self.cid_to_address(id)?;
        self.storage.exists(&address).await
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        let address = self.cid_to_address(id)?;
        self.storage.read_bytes(&address).await
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        let hex_digest = id.digest_hex();
        self.storage
            .content_write_bytes_with_hash(kind, &self.ledger_id, &hex_digest, bytes)
            .await?;
        Ok(id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        if !id.verify(bytes) {
            return Err(crate::error::Error::storage(format!(
                "CID verification failed: provided CID {} does not match bytes",
                id
            )));
        }
        let address = self.cid_to_address(id)?;
        self.storage.write_bytes(&address, bytes).await
    }

    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        let address = self.cid_to_address(id).ok()?;
        self.storage.resolve_local_path(&address)
    }
}

/// Convenience constructor for the `StorageContentStore` bridge adapter.
///
/// Wraps an existing `S: Storage` to provide `ContentStore` semantics.
/// The `method` string (e.g., `"file"`, `"memory"`, `"s3"`) must come from
/// the calling layer — typically from connection config or by parsing an
/// existing address via `parse_fluree_address().method`.
pub fn bridge_content_store<S: Storage>(
    storage: S,
    ledger_id: &str,
    method: &str,
) -> StorageContentStore<S> {
    StorageContentStore::new(storage, ledger_id, method)
}

/// Construct a `ContentStore` from a `Storage` backend using its declared method.
///
/// This is the preferred way to obtain a `ContentStore` — the method is derived
/// from the storage's [`StorageMethod::storage_method()`] implementation, so
/// callers never need to supply a method string manually.
///
/// The `namespace_id` is typically a ledger ID (e.g., `"mydb:main"`) or a
/// graph source ID (e.g., `"my-search:main"`) — it determines the CAS
/// namespace prefix for physical key layout.
pub fn content_store_for<S: Storage>(storage: S, namespace_id: &str) -> StorageContentStore<S> {
    let method = storage.storage_method().to_string();
    StorageContentStore::new(storage, namespace_id, method)
}

// ============================================================================
// Helper Functions (Public for use by other storage implementations)
// ============================================================================

/// Compute SHA-256 hash of bytes and return as hex string.
///
/// This is the standard hash function used for content-addressed storage.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

/// Convert a ledger ID to a path prefix.
///
/// Handles the standard ledger ID format (e.g., "mydb:main" -> "mydb/main").
pub fn ledger_id_prefix_for_path(ledger_id: &str) -> String {
    ledger_id_to_path_prefix(ledger_id).unwrap_or_else(|_| ledger_id.replace(':', "/"))
}

/// Build a storage path for content-addressed data.
///
/// This determines the directory structure for different content types:
/// - Commits: `{ledger_id}/commit/{hash}.fcv2`
/// - Index nodes: `{ledger_id}/index/{ordering}/{hash}.json`
/// - Graph sources: `graph-sources/{ledger_id}/snapshots/{hash}.gssnap`
/// - etc.
pub fn content_path(kind: ContentKind, ledger_id: &str, hash_hex: &str) -> String {
    let prefix = ledger_id_prefix_for_path(ledger_id);
    match kind {
        ContentKind::Commit => format!("{}/commit/{}.fcv2", prefix, hash_hex),
        ContentKind::Txn => format!("{}/txn/{}.json", prefix, hash_hex),
        ContentKind::IndexRoot => format!("{}/index/roots/{}.json", prefix, hash_hex),
        ContentKind::GarbageRecord => format!("{}/index/garbage/{}.json", prefix, hash_hex),
        ContentKind::DictBlob { dict } => {
            let ext = dict_kind_extension(dict);
            format!("{}/index/objects/dicts/{}.{}", prefix, hash_hex, ext)
        }
        ContentKind::IndexBranch => format!("{}/index/objects/branches/{}.fbr", prefix, hash_hex),
        ContentKind::IndexLeaf => format!("{}/index/objects/leaves/{}.fli", prefix, hash_hex),
        ContentKind::LedgerConfig => format!("{}/config/{}.json", prefix, hash_hex),
        ContentKind::StatsSketch => format!("{}/index/stats/{}.hll", prefix, hash_hex),
        ContentKind::GraphSourceSnapshot => {
            format!("graph-sources/{}/snapshots/{}.gssnap", prefix, hash_hex)
        }
        // Forward-compatibility: unknown kinds go to a generic blob directory
        #[allow(unreachable_patterns)]
        _ => format!("{}/blob/{}.bin", prefix, hash_hex),
    }
}

/// Build a Fluree address for content-addressed data.
///
/// # Arguments
///
/// * `method` - Storage method identifier (e.g., "file", "s3", "memory")
/// * `kind` - The type of content being stored
/// * `ledger_id` - Ledger ID (e.g., "mydb:main")
/// * `hash_hex` - Content hash as hex string
///
/// # Returns
///
/// A Fluree address like `fluree:file://mydb/main/commit/{hash}.fcv2`
pub fn content_address(method: &str, kind: ContentKind, ledger_id: &str, hash_hex: &str) -> String {
    let path = content_path(kind, ledger_id, hash_hex);
    format!("fluree:{}://{}", method, path)
}

/// Extract the content hash (filename stem) from a CAS address.
///
/// Decode JSON from bytes
///
/// Helper function for storage implementations.
pub fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

// ============================================================================
// MemoryStorage Implementation
// ============================================================================

/// A simple in-memory storage for testing
///
/// This implementation stores data in a HashMap with interior mutability
/// (via `Arc<RwLock<...>>`) to support both reading and writing.
/// Useful for unit tests and in-memory ledger operations.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new empty memory storage
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Insert data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert(&self, address: impl Into<String>, data: Vec<u8>) {
        self.data
            .write()
            .expect("RwLock poisoned")
            .insert(address.into(), data);
    }

    /// Insert JSON data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert_json<T: serde::Serialize>(
        &self,
        address: impl Into<String>,
        value: &T,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.insert(address, bytes);
        Ok(())
    }
}

#[async_trait]
impl StorageRead for MemoryStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        self.data
            .read()
            .expect("RwLock poisoned")
            .get(address)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(address))
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        Ok(self
            .data
            .read()
            .expect("RwLock poisoned")
            .contains_key(address))
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read().expect("RwLock poisoned");
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[async_trait]
impl StorageWrite for MemoryStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        self.data
            .write()
            .expect("RwLock poisoned")
            .insert(address.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, address: &str) -> Result<()> {
        // Idempotent: ok even if not found
        self.data.write().expect("RwLock poisoned").remove(address);
        Ok(())
    }
}

impl StorageMethod for MemoryStorage {
    fn storage_method(&self) -> &str {
        "memory"
    }
}

#[async_trait]
impl ContentAddressedWrite for MemoryStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address("memory", kind, ledger_id, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

// ============================================================================
// FileStorage Implementation (native only)
// ============================================================================

/// File-based storage for reading index files from disk (native targets only).
///
/// This implementation is intentionally behind the `native` feature because it
/// depends on filesystem access and `tokio::fs`. WASM callers are expected to
/// provide their own `StorageRead` implementation (e.g., fetch-based).
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[derive(Debug, Clone)]
pub struct FileStorage {
    /// Base directory for index files
    base_path: std::path::PathBuf,
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
impl FileStorage {
    /// Create a new file storage with the given base path
    ///
    /// The base path should be the ledger's data directory containing the ledger
    /// subdirectories (e.g. `mydb/main/index/...`).
    pub fn new(base_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Get the base path for this storage
    pub fn base_path(&self) -> &std::path::Path {
        &self.base_path
    }

    /// Extract the path portion from a Fluree address.
    ///
    /// Handles formats like:
    /// - `fluree:file://path/to/file.json` -> `Some("path/to/file.json")`
    /// - `fluree:memory://path/to/file.json` -> `Some("path/to/file.json")`
    /// - `raw/path` -> `None` (not a fluree address)
    fn extract_path_from_address(address: &str) -> Option<&str> {
        if let Some(path) = address.strip_prefix("fluree:file://") {
            return Some(path);
        }
        if address.starts_with("fluree:") {
            if let Some(path_start) = address.find("://") {
                return Some(&address[path_start + 3..]);
            }
        }
        None
    }

    /// Resolve an address to a file path
    ///
    /// Handles both raw file paths and Fluree address format.
    /// Address format: `fluree:file://path/to/file.json`
    fn resolve_path(&self, address: &str) -> Result<std::path::PathBuf> {
        if let Some(path) = Self::extract_path_from_address(address) {
            return self.resolve_relative_path(path);
        }
        // Simple case: just a node ID, look for it as a .json file
        self.resolve_relative_path(&format!("{}.json", address))
    }

    fn resolve_relative_path(&self, path: &str) -> Result<std::path::PathBuf> {
        use std::path::Component;
        let p = std::path::Path::new(path);

        // Disallow absolute paths and path traversal.
        if p.is_absolute()
            || p.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(crate::error::Error::storage(format!(
                "Invalid storage path '{}': must be a relative path without '..'",
                path
            )));
        }

        Ok(self.base_path.join(p))
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[async_trait]
impl StorageRead for FileStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        let path = self.resolve_path(address)?;
        tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                crate::error::Error::not_found(format!("{}: {}", address, path.display()))
            } else {
                crate::error::Error::io(format!("Failed to read {}: {}", path.display(), e))
            }
        })
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        let path = self.resolve_path(address).ok()?;
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        let path = self.resolve_path(address)?;
        match tokio::fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(crate::error::Error::io(format!(
                "Failed to stat {}: {}",
                path.display(),
                e
            ))),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        // Extract the path from the prefix (handle fluree:file:// format)
        let path_prefix = Self::extract_path_from_address(prefix).unwrap_or(prefix);

        // Get the directory to list from and the file prefix to match
        let full_path = self.base_path.join(path_prefix);
        let (list_dir, file_prefix) = if full_path.is_dir() {
            (full_path, String::new())
        } else {
            // The prefix might be a partial filename, so list the parent
            let parent = full_path.parent().unwrap_or(&self.base_path);
            let file_part = full_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            (parent.to_path_buf(), file_part)
        };

        // Check if directory exists
        if !list_dir.exists() {
            return Ok(Vec::new());
        }

        // Walk directory recursively
        let mut results = Vec::new();
        let mut dirs_to_visit = vec![list_dir.clone()];

        while let Some(dir) = dirs_to_visit.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(crate::error::Error::io(format!(
                        "Failed to list {}: {}",
                        dir.display(),
                        e
                    )));
                }
            };

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                crate::error::Error::io(format!("Failed to read entry in {}: {}", dir.display(), e))
            })? {
                let path = entry.path();
                let file_type = entry.file_type().await.map_err(|e| {
                    crate::error::Error::io(format!(
                        "Failed to get file type for {}: {}",
                        path.display(),
                        e
                    ))
                })?;

                if file_type.is_dir() {
                    dirs_to_visit.push(path);
                } else if file_type.is_file() {
                    // Convert back to relative path from base
                    if let Ok(relative) = path.strip_prefix(&self.base_path) {
                        let relative_str = relative.to_string_lossy().to_string();
                        // Check if it matches the file prefix (if any)
                        if file_prefix.is_empty() || relative_str.starts_with(path_prefix) {
                            // Return as fluree:file:// address
                            results.push(format!("fluree:file://{}", relative_str));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[async_trait]
impl StorageWrite for FileStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        let path = self.resolve_path(address)?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                crate::error::Error::io(format!(
                    "Failed to create directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        // Write file (overwrites if exists - idempotent for content-addressed)
        tokio::fs::write(&path, bytes).await.map_err(|e| {
            crate::error::Error::io(format!("Failed to write {}: {}", path.display(), e))
        })
    }

    async fn delete(&self, address: &str) -> Result<()> {
        let path = self.resolve_path(address)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Idempotent: not found is OK
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::error::Error::io(format!(
                "Failed to delete {}: {}",
                path.display(),
                e
            ))),
        }
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
impl StorageMethod for FileStorage {
    fn storage_method(&self) -> &str {
        "file"
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[async_trait]
impl ContentAddressedWrite for FileStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address("file", kind, ledger_id, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_storage() {
        let storage = MemoryStorage::new();
        storage.insert("test/path", b"hello world".to_vec());

        let bytes = storage.read_bytes("test/path").await.unwrap();
        assert_eq!(bytes, b"hello world");

        assert!(storage.exists("test/path").await.unwrap());
        assert!(!storage.exists("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_not_found() {
        let storage = MemoryStorage::new();
        let result = storage.read_bytes("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_storage_json() {
        let storage = MemoryStorage::new();

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        storage.insert_json("test.json", &data).unwrap();

        let bytes = storage.read_bytes("test.json").await.unwrap();
        let parsed: TestData = decode_json(&bytes).unwrap();

        assert_eq!(parsed, data);
    }

    #[tokio::test]
    async fn test_memory_storage_write() {
        let storage = MemoryStorage::new();

        // Test StorageWrite trait
        storage
            .write_bytes("write/test", b"written data")
            .await
            .unwrap();

        let bytes = storage.read_bytes("write/test").await.unwrap();
        assert_eq!(bytes, b"written data");

        // Test idempotency - writing same content again should succeed
        storage
            .write_bytes("write/test", b"overwritten")
            .await
            .unwrap();
        let bytes = storage.read_bytes("write/test").await.unwrap();
        assert_eq!(bytes, b"overwritten");
    }

    #[tokio::test]
    async fn test_memory_storage_delete() {
        let storage = MemoryStorage::new();
        storage.insert("delete/test", b"data".to_vec());

        assert!(storage.exists("delete/test").await.unwrap());
        storage.delete("delete/test").await.unwrap();
        assert!(!storage.exists("delete/test").await.unwrap());

        // Idempotent: deleting non-existent is OK
        storage.delete("delete/test").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_storage_list_prefix() {
        let storage = MemoryStorage::new();
        storage.insert("prefix/a", b"a".to_vec());
        storage.insert("prefix/b", b"b".to_vec());
        storage.insert("other/c", b"c".to_vec());

        let mut results = storage.list_prefix("prefix/").await.unwrap();
        results.sort();
        assert_eq!(results, vec!["prefix/a", "prefix/b"]);
    }

    #[tokio::test]
    async fn test_memory_storage_content_write_layout() {
        let storage = MemoryStorage::new();
        let bytes = br#"{"hello":"world"}"#;
        let res = storage
            .content_write_bytes(ContentKind::Commit, "mydb:main", bytes)
            .await
            .unwrap();

        assert!(res.address.starts_with("fluree:memory://mydb/main/commit/"));
        assert!(res.address.ends_with(".fcv2"));
        assert_eq!(res.size_bytes, bytes.len());

        let roundtrip = storage.read_bytes(&res.address).await.unwrap();
        assert_eq!(roundtrip, bytes);
    }

    // ========================================================================
    // ContentStore tests (CID-first)
    // ========================================================================

    #[tokio::test]
    async fn test_memory_content_store_put_get() {
        let store = MemoryContentStore::new();
        let data = b"content store test";

        let id = store.put(ContentKind::Commit, data).await.unwrap();
        assert!(store.has(&id).await.unwrap());

        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_memory_content_store_not_found() {
        let store = MemoryContentStore::new();
        let fake_id = ContentId::new(ContentKind::Commit, b"nonexistent");
        assert!(!store.has(&fake_id).await.unwrap());
        assert!(store.get(&fake_id).await.is_err());
    }

    #[tokio::test]
    async fn test_memory_content_store_put_with_id() {
        let store = MemoryContentStore::new();
        let data = b"verified content";
        let id = ContentId::new(ContentKind::Txn, data);

        // Correct bytes should succeed
        store.put_with_id(&id, data).await.unwrap();
        assert_eq!(store.get(&id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_memory_content_store_put_with_id_rejects_mismatch() {
        let store = MemoryContentStore::new();
        let id = ContentId::new(ContentKind::Txn, b"original");

        // Wrong bytes should be rejected
        let result = store.put_with_id(&id, b"tampered").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("CID verification failed"));
    }

    #[tokio::test]
    async fn test_bridge_adapter_roundtrip() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage.clone(), "mydb:main", "memory");

        let data = b"bridge test data";
        let id = store.put(ContentKind::Commit, data).await.unwrap();

        // Should be retrievable via ContentStore
        assert!(store.has(&id).await.unwrap());
        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_bridge_adapter_put_with_id() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "mydb:main", "memory");

        let data = b"bridge put_with_id test";
        let id = ContentId::new(ContentKind::IndexRoot, data);

        store.put_with_id(&id, data).await.unwrap();
        assert_eq!(store.get(&id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_bridge_adapter_put_with_id_rejects_mismatch() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "mydb:main", "memory");

        let id = ContentId::new(ContentKind::IndexRoot, b"real data");
        let result = store.put_with_id(&id, b"fake data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bridge_adapter_cid_matches_content_id_new() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "test:main", "memory");

        let data = b"cid consistency check";
        let id_from_store = store.put(ContentKind::Commit, data).await.unwrap();
        let id_from_new = ContentId::new(ContentKind::Commit, data);

        assert_eq!(id_from_store, id_from_new);
    }
}
