//! Connection and database handle types

use crate::config::ConnectionConfig;
use crate::error::Result;
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
use fluree_db_core::FileStorage;
use fluree_db_core::{Db, MemoryStorage, NodeCache, SimpleCache, Storage};
use std::sync::Arc;

/// A Fluree database connection
///
/// Holds the configuration, storage backend, and cache for loading databases.
/// Generic over storage and cache types, following the same pattern as `Db`.
#[derive(Debug)]
pub struct Connection<S, C> {
    config: ConnectionConfig,
    storage: S,
    cache: Arc<C>,
}

impl<S: Storage, C: NodeCache> Connection<S, C> {
    /// Create a new connection with provided storage and cache
    pub fn new(config: ConnectionConfig, storage: S, cache: impl Into<Arc<C>>) -> Self {
        Self {
            config,
            storage,
            cache: cache.into(),
        }
    }

    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Get a reference to the storage backend
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get a reference to the node cache
    pub fn cache(&self) -> &Arc<C> {
        &self.cache
    }
}

impl<S: Storage + Clone, C: NodeCache> Connection<S, C> {
    /// Load a database from a root address (cloning storage, sharing cache)
    ///
    /// The root address should point to the database's index root file.
    /// For file storage, this is typically:
    /// `fluree:file://{ledger}/{branch}/index/root/{file}.json`
    ///
    /// Returns a `Db` that shares the connection-wide cache.
    pub async fn load_db(&self, root_address: &str) -> Result<Db<S, C>> {
        Ok(Db::load(self.storage.clone(), Arc::clone(&self.cache), root_address).await?)
    }
}

/// Type alias for a file-backed connection
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub type FileConnection = Connection<FileStorage, SimpleCache>;

/// Type alias for a memory-backed connection
pub type MemoryConnection = Connection<MemoryStorage, SimpleCache>;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
impl FileConnection {
    /// Load a database from a root address
    ///
    /// Creates a new cache for the returned Db. For shared caching,
    /// use a custom Connection with your own cache type.
    pub async fn load_db_fresh_cache(
        &self,
        root_address: &str,
    ) -> Result<Db<FileStorage, SimpleCache>> {
        let storage = FileStorage::new(self.storage.base_path());
        let cache = SimpleCache::new(self.config.cache.max_entries);
        Ok(Db::load(storage, cache, root_address).await?)
    }
}

// Note: MemoryConnection does not have load_db_fresh_cache because MemoryStorage
// cannot be meaningfully cloned (it contains data). Users of MemoryConnection should
// either:
// 1. Use Arc<MemoryStorage> with a custom Connection type
// 2. Pass the same storage instance to Db::load directly
// 3. Use a shared storage abstraction (future work)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_new_memory() {
        let config = ConnectionConfig::memory();
        let storage = MemoryStorage::new();
        let cache = SimpleCache::new(1000);
        let conn = Connection::new(config, storage, cache);

        assert!(format!("{:?}", conn.storage()).contains("MemoryStorage"));
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_connection_new_file() {
        let config = ConnectionConfig::file("/tmp/test-db");
        let storage = FileStorage::new("/tmp/test-db");
        let cache = SimpleCache::new(1000);
        let conn = Connection::new(config, storage, cache);

        assert!(format!("{:?}", conn.storage()).contains("FileStorage"));
    }

    #[test]
    fn test_connection_config_access() {
        let config = ConnectionConfig {
            parallelism: 8,
            ..ConnectionConfig::default()
        };
        let storage = MemoryStorage::new();
        let cache = SimpleCache::new(1000);
        let conn = Connection::new(config, storage, cache);

        assert_eq!(conn.config().parallelism, 8);
    }
}
