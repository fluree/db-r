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

use crate::address_path::alias_to_path_prefix;
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

/// What kind of dictionary blob is being stored.
///
/// Used by [`ContentKind::DictBlob`] to route dictionary artifacts to
/// typed CAS paths (e.g. `objects/dicts/{hash}.dict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DictKind {
    /// Named-graph IRI dictionary (FRD1 format).
    Graphs,
    /// Datatype IRI dictionary (FRD1 format).
    Datatypes,
    /// Language tag dictionary (FRD1 format).
    Languages,
    /// Subject forward file — raw concatenated UTF-8 IRIs (mmap'd).
    SubjectForward,
    /// Subject index — offsets/lengths into subject forward file (FSI1 format).
    SubjectIndex,
    /// Subject reverse hash index — hash→s_id binary search table (SRV1 format, mmap'd).
    SubjectReverse,
    /// String value forward file — raw concatenated UTF-8 (mmap'd).
    StringForward,
    /// String value index — offsets/lengths into string forward file (FSI1 format).
    StringIndex,
    /// String value reverse hash index (SRV1 format, mmap'd).
    StringReverse,
    /// Per-predicate overflow BigInt/BigDecimal arena (NBA1 format).
    NumBig { p_id: u32 },
}

/// File extension for a given [`DictKind`] (used in CAS paths).
fn dict_kind_extension(dict: DictKind) -> &'static str {
    match dict {
        DictKind::Graphs | DictKind::Datatypes | DictKind::Languages => "dict",
        DictKind::SubjectForward | DictKind::StringForward => "fwd",
        DictKind::SubjectIndex | DictKind::StringIndex => "idx",
        DictKind::SubjectReverse | DictKind::StringReverse => "rev",
        DictKind::NumBig { .. } => "nba",
    }
}

/// What a blob "is", so storage can choose its layout.
///
/// Filesystem-like storages typically map this to directory prefixes such as
/// `index/spot/` vs `commit/`. Some storages may ignore it (e.g. IPFS-like
/// content stores).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentKind {
    /// Commit JSON blob
    Commit,
    /// Txn JSON blob
    Txn,
    /// DB root index node (the "root pointer" written each refresh)
    IndexRoot,
    /// Garbage record (GC metadata)
    GarbageRecord,
    /// Reindex checkpoint (mutable / overwritten)
    ReindexCheckpoint,
    /// Dictionary artifact (predicates, subjects, strings, etc.)
    DictBlob { dict: DictKind },
    /// Index branch manifest (FBR1 format)
    IndexBranch,
    /// Index leaf file (FLI1 format)
    IndexLeaf,
}

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
        ledger_alias: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult>;

    /// Write bytes, computing the content hash automatically (SHA-256).
    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_alias: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_alias, &hash_hex, bytes)
            .await
    }
}

// ============================================================================
// Marker Trait
// ============================================================================

/// Full storage capability marker
///
/// This trait combines `StorageRead` and `ContentAddressedWrite` (which itself
/// requires `StorageWrite`), providing a single bound for storage backends
/// that support all operations.
///
/// Used for type erasure in `AnyStorage`.
pub trait Storage: StorageRead + ContentAddressedWrite {}
impl<T: StorageRead + ContentAddressedWrite> Storage for T {}

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

/// Convert a ledger alias to a path prefix.
///
/// Handles the standard alias format (e.g., "mydb:main" -> "mydb/main").
pub fn alias_prefix_for_path(alias: &str) -> String {
    alias_to_path_prefix(alias).unwrap_or_else(|_| alias.replace(':', "/"))
}

/// Build a storage path for content-addressed data.
///
/// This determines the directory structure for different content types:
/// - Commits: `{alias}/commit/{hash}.json`
/// - Index nodes: `{alias}/index/{ordering}/{hash}.json`
/// - etc.
pub fn content_path(kind: ContentKind, alias: &str, hash_hex: &str) -> String {
    let prefix = alias_prefix_for_path(alias);
    match kind {
        ContentKind::Commit => format!("{}/commit/{}.json", prefix, hash_hex),
        ContentKind::Txn => format!("{}/txn/{}.json", prefix, hash_hex),
        ContentKind::IndexRoot => format!("{}/index/roots/{}.json", prefix, hash_hex),
        ContentKind::GarbageRecord => format!("{}/index/garbage/{}.json", prefix, hash_hex),
        ContentKind::ReindexCheckpoint => format!("{}/index/reindex-checkpoint.json", prefix),
        ContentKind::DictBlob { dict } => {
            let ext = dict_kind_extension(dict);
            format!("{}/index/objects/dicts/{}.{}", prefix, hash_hex, ext)
        }
        ContentKind::IndexBranch => format!("{}/index/objects/branches/{}.fbr", prefix, hash_hex),
        ContentKind::IndexLeaf => format!("{}/index/objects/leaves/{}.fli", prefix, hash_hex),
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
/// * `alias` - Ledger alias (e.g., "mydb:main")
/// * `hash_hex` - Content hash as hex string
///
/// # Returns
///
/// A Fluree address like `fluree:file://mydb/main/commit/{hash}.json`
pub fn content_address(method: &str, kind: ContentKind, alias: &str, hash_hex: &str) -> String {
    let path = content_path(kind, alias, hash_hex);
    format!("fluree:{}://{}", method, path)
}

/// Extract the content hash (filename stem) from a CAS address.
///
/// Given an address like `"fluree:file://mydb/main/index/objects/leaves/abc123.fli"`,
/// returns `Some("abc123".to_string())`. Works by extracting the last path segment
/// and stripping the file extension.
///
/// Returns `None` if the address has no path segment or no file extension.
pub fn extract_hash_from_address(address: &str) -> Option<String> {
    // Find the last path segment (after the last '/')
    let filename = address.rsplit('/').next()?;
    // Strip the file extension (after the last '.')
    let dot_pos = filename.rfind('.')?;
    if dot_pos == 0 {
        return None; // hidden file with no stem
    }
    Some(filename[..dot_pos].to_string())
}


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
        self.data
            .write()
            .expect("RwLock poisoned")
            .remove(address);
        Ok(())
    }
}

#[async_trait]
impl ContentAddressedWrite for MemoryStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_alias: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address("memory", kind, ledger_alias, content_hash_hex);
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
            || p.components().any(|c| matches!(c, Component::ParentDir | Component::RootDir | Component::Prefix(_)))
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
            let file_part = full_path.file_name()
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
#[async_trait]
impl ContentAddressedWrite for FileStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_alias: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address("file", kind, ledger_alias, content_hash_hex);
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
        storage.write_bytes("write/test", b"written data").await.unwrap();

        let bytes = storage.read_bytes("write/test").await.unwrap();
        assert_eq!(bytes, b"written data");

        // Test idempotency - writing same content again should succeed
        storage.write_bytes("write/test", b"overwritten").await.unwrap();
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
        assert!(res.address.ends_with(".json"));
        assert_eq!(res.size_bytes, bytes.len());

        let roundtrip = storage.read_bytes(&res.address).await.unwrap();
        assert_eq!(roundtrip, bytes);
    }
}
