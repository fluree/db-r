//! # Fluree DB Connection
//!
//! Configuration parsing and connection initialization for Fluree databases.
//!
//! This crate provides:
//! - Configuration types for storage, cache, and connections
//! - Factory functions for creating storage and cache instances
//! - The `Connection` struct for loading databases
//! - JSON-LD configuration parsing compatible with Clojure Fluree
//!
//! ## Quick Start
//!
//! ```ignore
//! use fluree_db_connection::connect_file;
//!
//! // Connect to a file-based storage
//! let conn = connect_file("/path/to/ledger/data");
//!
//! // Load a specific database snapshot
//! let db = conn.load_db_fresh_cache("fluree:file://ledger/main/index/root/abc123.json").await?;
//! ```
//!
//! ## AWS/S3 Connections
//!
//! For S3-backed storage with DynamoDB nameservice:
//!
//! ```ignore
//! use fluree_db_connection::connect_async;
//! use serde_json::json;
//!
//! let config = json!({
//!     "@context": {
//!         "@base": "https://ns.flur.ee/config/connection/",
//!         "@vocab": "https://ns.flur.ee/system#"
//!     },
//!     "@graph": [
//!         {"@id": "s3Storage", "@type": "Storage", "s3Bucket": "my-bucket", "s3Prefix": "ledgers"},
//!         {"@id": "conn", "@type": "Connection",
//!          "indexStorage": {"@id": "s3Storage"},
//!          "commitStorage": {"@id": "s3Storage"},
//!          "primaryPublisher": {"@type": "Publisher", "dynamodbTable": "fluree-ns"}}
//!     ]
//! });
//! let conn = connect_async(&config).await?;
//! let db = conn.load_db("mydb:main").await?;
//! ```
//!
//! ## Configuration
//!
//! For more control, create a `Connection` directly with custom storage and cache:
//!
//! ```ignore
//! use fluree_db_connection::{Connection, ConnectionConfig};
//! use fluree_db_core::{FileStorage, SimpleCache};
//!
//! let config = ConnectionConfig::file("/data/fluree");
//! let storage = FileStorage::new("/data/fluree");
//! let cache = SimpleCache::new(10000);
//! let conn = Connection::new(config, storage, cache);
//! ```

pub mod cache;
pub mod config;
pub mod connection;
pub mod error;
pub mod graph;
pub mod registry;
pub mod storage;
pub mod vocab;

#[cfg(feature = "aws")]
pub mod aws;

// Re-export main types
pub use cache::{default_cache_entries, default_cache_max_mb, memory_to_cache_size};
pub use config::{CacheConfig, ConnectionConfig, StorageConfig, StorageType};
pub use connection::{Connection, MemoryConnection};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use connection::FileConnection;
pub use error::{ConnectionError, Result};
pub use graph::ConfigGraph;

// AWS re-exports
#[cfg(feature = "aws")]
pub use aws::{
    connect_aws, AwsConfig, AwsConnectionHandle, AwsNameService, AwsNameServiceConfig,
    DynamoDbConfig, S3Config,
};

// Re-export core types commonly used with connections
pub use fluree_db_core::{Db, MemoryStorage};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use fluree_db_core::FileStorage;

/// Create a memory-backed connection
///
/// This is useful for testing or when data doesn't need to persist.
///
/// # Example
///
/// ```
/// use fluree_db_connection::connect_memory;
///
/// let conn = connect_memory();
/// // conn.load_db_fresh_cache("fluree:memory://...").await?;
/// ```
pub fn connect_memory() -> MemoryConnection {
    let config = ConnectionConfig::memory();
    let storage = MemoryStorage::new();
    Connection::new(config, storage)
}

/// Create a file-backed connection
///
/// The `base_path` should be the root directory containing ledger data.
/// Storage addresses like `fluree:file://path/to/file.json` will resolve
/// relative to this base path.
///
/// # Example
///
/// ```ignore
/// use fluree_db_connection::connect_file;
///
/// let conn = connect_file("/data/fluree");
/// let db = conn.load_db_fresh_cache("fluree:file://mydb/main/index/root/abc.json").await?;
/// ```
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub fn connect_file(base_path: &str) -> FileConnection {
    let config = ConnectionConfig::file(base_path);
    let storage = FileStorage::new(base_path);
    Connection::new(config, storage)
}

/// Connection that can be file, memory, or AWS backed
///
/// This enum is returned by `connect()` and `connect_async()` when parsing
/// JSON-LD configs, where the storage type is determined at runtime from the config.
#[derive(Debug)]
pub enum ConnectionHandle {
    /// File-backed connection
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    File(FileConnection),
    /// Memory-backed connection
    Memory(MemoryConnection),
    /// AWS-backed connection (S3 storage, DynamoDB nameservice)
    #[cfg(feature = "aws")]
    Aws(AwsConnectionHandle),
}

impl ConnectionHandle {
    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        match self {
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            ConnectionHandle::File(c) => c.config(),
            ConnectionHandle::Memory(c) => c.config(),
            #[cfg(feature = "aws")]
            ConnectionHandle::Aws(c) => c.config(),
        }
    }

    /// Load a database by alias (for AWS connections) or root address (for local)
    ///
    /// For AWS connections, this uses the nameservice to look up the ledger's
    /// index root address. For local connections, pass the full root address.
    pub async fn load_db(&self, address_or_alias: &str) -> Result<DynDb> {
        match self {
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            ConnectionHandle::File(c) => {
                let db = c.load_db(address_or_alias).await?;
                Ok(DynDb::File(db))
            }
            ConnectionHandle::Memory(c) => {
                let db = c.load_db(address_or_alias).await?;
                Ok(DynDb::Memory(db))
            }
            #[cfg(feature = "aws")]
            ConnectionHandle::Aws(c) => {
                let db = c.load_db(address_or_alias).await?;
                Ok(DynDb::Aws(db))
            }
        }
    }
}

/// Dynamic database handle that can be backed by any storage type
#[derive(Debug)]
pub enum DynDb {
    /// File-backed database
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    File(Db<FileStorage>),
    /// Memory-backed database
    Memory(Db<MemoryStorage>),
    /// AWS-backed database (S3 storage)
    #[cfg(feature = "aws")]
    Aws(Db<fluree_db_storage_aws::S3Storage>),
}

/// Create connection from JSON config (auto-detects format)
///
/// Accepts either:
/// - Clojure-style JSON-LD with @graph and @context
/// - Simple flat JSON format
///
/// # Example
///
/// ```ignore
/// use fluree_db_connection::connect;
/// use serde_json::json;
///
/// // JSON-LD format (Clojure-compatible)
/// let config = json!({
///     "@context": {
///         "@base": "https://ns.flur.ee/config/connection/",
///         "@vocab": "https://ns.flur.ee/system#"
///     },
///     "@graph": [
///         {"@id": "fileStorage", "@type": "Storage", "filePath": "/data"},
///         {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "fileStorage"}}
///     ]
/// });
/// let conn = connect(&config)?;
/// ```
pub fn connect(config_json: &serde_json::Value) -> Result<ConnectionHandle> {
    // Check if this is JSON-LD format
    if fluree_graph_json_ld::is_json_ld(config_json) {
        let config = ConnectionConfig::from_json_ld(config_json)?;
        create_sync_connection(config)
    } else {
        // Fall back to simple flat format
        let config = ConnectionConfig::from_json(config_json)?;
        create_sync_connection(config)
    }
}

/// Create connection from JSON config asynchronously
///
/// This is the async version of `connect()` that supports AWS backends (S3, DynamoDB).
/// Use this when your config includes S3 storage or DynamoDB nameservice.
///
/// # Example
///
/// ```ignore
/// use fluree_db_connection::connect_async;
/// use serde_json::json;
///
/// let config = json!({
///     "@context": {
///         "@base": "https://ns.flur.ee/config/connection/",
///         "@vocab": "https://ns.flur.ee/system#"
///     },
///     "@graph": [
///         {"@id": "s3Storage", "@type": "Storage", "s3Bucket": "my-bucket"},
///         {"@id": "conn", "@type": "Connection",
///          "indexStorage": {"@id": "s3Storage"},
///          "primaryPublisher": {"@type": "Publisher", "dynamodbTable": "fluree-ns"}}
///     ]
/// });
/// let conn = connect_async(&config).await?;
/// let db = conn.load_db("mydb:main").await?;
/// ```
pub async fn connect_async(config_json: &serde_json::Value) -> Result<ConnectionHandle> {
    // Check if this is JSON-LD format
    let config = if fluree_graph_json_ld::is_json_ld(config_json) {
        ConnectionConfig::from_json_ld(config_json)?
    } else {
        ConnectionConfig::from_json(config_json)?
    };

    create_async_connection(config).await
}

/// Sync connection creation for local backends (file, memory)
fn create_sync_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::File => {
            #[cfg(not(all(feature = "native", not(target_arch = "wasm32"))))]
            {
                return Err(ConnectionError::unsupported_component(
                    "https://ns.flur.ee/system#filePath (native feature disabled)",
                ));
            }
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            {
                let path = config
                    .index_storage
                    .path
                    .as_ref()
                    .ok_or_else(|| ConnectionError::invalid_config("File storage requires path"))?;
                let storage = FileStorage::new(path.as_ref());
                Ok(ConnectionHandle::File(Connection::new(
                    config, storage,
                )))
            }
        }
        StorageType::Memory => {
            let storage = MemoryStorage::new();
            Ok(ConnectionHandle::Memory(Connection::new(
                config, storage,
            )))
        }
        StorageType::S3(_) => Err(ConnectionError::invalid_config(
            "S3 storage requires async initialization. Use connect_async() instead of connect().",
        )),
        StorageType::Unsupported { type_iri, .. } => {
            Err(ConnectionError::unsupported_component(type_iri))
        }
    }
}

/// Async connection creation that supports all backends
#[cfg(feature = "aws")]
async fn create_async_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            create_aws_connection(config.clone(), s3_config).await
        }
        StorageType::File => {
            #[cfg(not(all(feature = "native", not(target_arch = "wasm32"))))]
            {
                return Err(ConnectionError::unsupported_component(
                    "https://ns.flur.ee/system#filePath (native feature disabled)",
                ));
            }
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            {
                let path = config
                    .index_storage
                    .path
                    .as_ref()
                    .ok_or_else(|| ConnectionError::invalid_config("File storage requires path"))?;
                let storage = FileStorage::new(path.as_ref());
                Ok(ConnectionHandle::File(Connection::new(
                    config, storage,
                )))
            }
        }
        StorageType::Memory => {
            let storage = MemoryStorage::new();
            Ok(ConnectionHandle::Memory(Connection::new(
                config, storage,
            )))
        }
        StorageType::Unsupported { type_iri, .. } => {
            Err(ConnectionError::unsupported_component(type_iri))
        }
    }
}

/// Create async connection without AWS feature - fallback to sync
#[cfg(not(feature = "aws"))]
async fn create_async_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::S3(_) => Err(ConnectionError::unsupported_component(
            "S3 storage requires the 'aws' feature to be enabled",
        )),
        _ => create_sync_connection(config),
    }
}

/// Create AWS connection from parsed JSON-LD config
///
/// Uses StorageRegistry for storage sharing when the same @id is referenced
/// multiple times (e.g., index and commit storage pointing to same storage node).
#[cfg(feature = "aws")]
async fn create_aws_connection(
    config: ConnectionConfig,
    _s3_config: &config::S3StorageConfig,
) -> Result<ConnectionHandle> {
    use crate::config::PublisherType;
    use crate::registry::StorageRegistry;
    use fluree_db_nameservice::StorageNameService;
    use fluree_db_storage_aws::{DynamoDbConfig as RawDynamoDbConfig, DynamoDbNameService};
    use std::sync::Arc;

    // Get or initialize SDK config
    let sdk_config = aws::get_or_init_sdk_config().await?;

    // Create storage registry for @id-based sharing
    let mut registry = StorageRegistry::new();

    // Create index storage via registry (enables sharing)
    let index_storage = registry
        .get_or_create_s3(sdk_config, &config.index_storage)
        .await?;

    // Create commit storage if configured and different from index
    // Registry handles sharing if same @id
    let commit_storage = if let Some(commit_config) = &config.commit_storage {
        match &commit_config.storage_type {
            StorageType::S3(_) => {
                let storage = registry.get_or_create_s3(sdk_config, commit_config).await?;
                // Only keep separate if actually different instance
                if Arc::ptr_eq(&storage, &index_storage) {
                    None
                } else {
                    Some(storage)
                }
            }
            _ => None, // Non-S3 commit storage not supported with S3 index
        }
    } else {
        None
    };

    // Create nameservice based on publisher config
    let nameservice = match &config.primary_publisher {
        Some(pub_config) => match &pub_config.publisher_type {
            PublisherType::DynamoDb {
                table,
                region,
                endpoint,
                timeout_ms,
                ..
            } => {
                let dynamo_config = RawDynamoDbConfig {
                    table_name: table.to_string(),
                    region: region.as_ref().map(|s| s.to_string()),
                    endpoint: endpoint.as_ref().map(|s| s.to_string()),
                    timeout_ms: *timeout_ms,
                };
                let ns = DynamoDbNameService::new(sdk_config, dynamo_config)
                    .await
                    .map_err(|e| {
                        ConnectionError::storage(format!(
                            "Failed to create DynamoDB nameservice: {}",
                            e
                        ))
                    })?;
                aws::AwsNameService::DynamoDb(Arc::new(ns))
            }
            PublisherType::Storage { storage } => {
                // Storage-backed nameservice - use registry for storage sharing
                let ns_storage = match &storage.storage_type {
                    StorageType::S3(_) => registry.get_or_create_s3(sdk_config, storage).await?,
                    _ => {
                        return Err(ConnectionError::invalid_config(
                            "Storage-backed publisher requires S3 storage",
                        ))
                    }
                };
                // StorageNameService prefix is empty - S3Storage has the bucket prefix
                let ns = StorageNameService::new((*ns_storage).clone(), "");
                aws::AwsNameService::Storage(Arc::new(ns))
            }
            PublisherType::Unsupported { type_iri, .. } => {
                return Err(ConnectionError::unsupported_component(type_iri));
            }
        },
        None => {
            // Default DynamoDB nameservice
            let dynamo_config = RawDynamoDbConfig {
                table_name: "fluree-nameservice".to_string(),
                region: None,
                endpoint: None,
                timeout_ms: None,
            };
            let ns = DynamoDbNameService::new(sdk_config, dynamo_config)
                .await
                .map_err(|e| {
                    ConnectionError::storage(format!(
                        "Failed to create DynamoDB nameservice: {}",
                        e
                    ))
                })?;
            aws::AwsNameService::DynamoDb(Arc::new(ns))
        }
    };

    // Build connection handle directly (bypasses connect_aws to use registry-shared storage)
    let handle = aws::AwsConnectionHandle::new(
        config,
        (*index_storage).clone(),
        commit_storage.map(|s| (*s).clone()),
        nameservice,
    );

    Ok(ConnectionHandle::Aws(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_memory() {
        let conn = connect_memory();
        assert!(format!("{:?}", conn.storage()).contains("MemoryStorage"));
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_connect_file() {
        let conn = connect_file("/tmp/test");
        assert!(format!("{:?}", conn.storage()).contains("FileStorage"));
    }

    #[test]
    fn test_custom_connection() {
        let config = ConnectionConfig {
            parallelism: 8,
            ..ConnectionConfig::default()
        };
        let storage = MemoryStorage::new();
        let conn = Connection::new(config, storage);

        assert_eq!(conn.config().parallelism, 8);
    }
}
