//! Connection and database handle types

use crate::config::ConnectionConfig;
use crate::error::Result;
use fluree_db_core::ContentId;
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
use fluree_db_core::FileStorage;
use fluree_db_core::{Db, MemoryStorage, Storage};

/// A Fluree database connection
///
/// Holds the configuration and storage backend for loading databases.
/// Generic over the storage type.
#[derive(Debug)]
pub struct Connection<S> {
    config: ConnectionConfig,
    storage: S,
}

impl<S: Storage> Connection<S> {
    /// Create a new connection with provided storage
    pub fn new(config: ConnectionConfig, storage: S) -> Self {
        Self { config, storage }
    }

    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Get a reference to the storage backend
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

impl<S: Storage + Clone> Connection<S> {
    /// Load a database from a root content ID (cloning storage)
    ///
    /// The `root_id` identifies the index root by its content hash.
    /// The `ledger_id` is needed to derive the storage address.
    pub async fn load_db(&self, root_id: &ContentId, ledger_id: &str) -> Result<Db> {
        Ok(fluree_db_core::load_db(&self.storage, root_id, ledger_id).await?)
    }
}

/// Type alias for a file-backed connection
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub type FileConnection = Connection<FileStorage>;

/// Type alias for a memory-backed connection
pub type MemoryConnection = Connection<MemoryStorage>;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
impl FileConnection {
    /// Load a database from a root content ID with a fresh storage instance
    ///
    /// Creates a new storage instance for the returned Db. For shared storage,
    /// use `load_db` instead.
    pub async fn load_db_fresh_cache(&self, root_id: &ContentId, ledger_id: &str) -> Result<Db> {
        let storage = FileStorage::new(self.storage.base_path());
        Ok(fluree_db_core::load_db(&storage, root_id, ledger_id).await?)
    }
}

// Note: MemoryConnection does not have load_db_fresh_cache because MemoryStorage
// cannot be meaningfully cloned (it contains data). Users of MemoryConnection should
// either:
// 1. Use Arc<MemoryStorage> with a custom Connection type
// 2. Call `fluree_db_core::load_db(&storage, root_id, ledger_id)` directly
// 3. Use a shared storage abstraction (future work)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_new_memory() {
        let config = ConnectionConfig::memory();
        let storage = MemoryStorage::new();
        let conn = Connection::new(config, storage);

        assert!(format!("{:?}", conn.storage()).contains("MemoryStorage"));
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_connection_new_file() {
        let config = ConnectionConfig::file("/tmp/test-db");
        let storage = FileStorage::new("/tmp/test-db");
        let conn = Connection::new(config, storage);

        assert!(format!("{:?}", conn.storage()).contains("FileStorage"));
    }

    #[test]
    fn test_connection_config_access() {
        let config = ConnectionConfig {
            parallelism: 8,
            ..ConnectionConfig::default()
        };
        let storage = MemoryStorage::new();
        let conn = Connection::new(config, storage);

        assert_eq!(conn.config().parallelism, 8);
    }
}
