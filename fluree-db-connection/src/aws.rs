//! AWS connection handle for Lambda and cloud deployments
//!
//! Provides `AwsConnectionHandle` which owns AWS-specific storage components
//! (S3 for index/commit storage, DynamoDB or S3 for nameservice) and provides
//! methods for loading databases.
//!
//! ## Nameservice Options
//!
//! - **DynamoDB** (recommended for production): Fast, scalable, conditional updates
//! - **S3 Storage-backed**: Uses same S3 bucket as data, no separate DynamoDB table needed
//!
//! ## S3 Express One Zone Support
//!
//! S3 Express One Zone (directory buckets) is expected to be handled natively by
//! the AWS SDK - use an Express bucket name and the SDK should handle session
//! authentication automatically. **Validate with real AWS integration tests
//! before production use.**
//!
//! # Example with DynamoDB nameservice
//!
//! ```ignore
//! use fluree_db_connection::aws::{connect_aws, AwsConfig, S3Config, AwsNameServiceConfig};
//!
//! let config = AwsConfig {
//!     index_storage: S3Config {
//!         bucket: "my-index-bucket".to_string(),
//!         prefix: Some("ledgers".to_string()),
//!         timeout_ms: Some(30000),
//!     },
//!     commit_storage: None,
//!     nameservice: AwsNameServiceConfig::DynamoDb(DynamoDbConfig {
//!         table_name: "fluree-nameservice".to_string(),
//!         ..Default::default()
//!     }),
//!     cache_max_entries: 10000,
//! };
//!
//! let conn = connect_aws(config).await?;
//! let db = conn.load_db("mydb:main").await?;
//! ```
//!
//! # Example with S3 storage-backed nameservice
//!
//! ```ignore
//! use fluree_db_connection::aws::{connect_aws, AwsConfig, S3Config, AwsNameServiceConfig};
//!
//! let config = AwsConfig {
//!     index_storage: S3Config {
//!         bucket: "my-bucket".to_string(),
//!         prefix: Some("ledgers".to_string()),
//!         timeout_ms: Some(30000),
//!     },
//!     commit_storage: None,
//!     nameservice: AwsNameServiceConfig::Storage {
//!         // Uses same storage config as index_storage by default
//!         prefix: Some("ns".to_string()),
//!     },
//!     cache_max_entries: 10000,
//! };
//!
//! let conn = connect_aws(config).await?;
//! let db = conn.load_db("mydb:main").await?;
//! ```

use crate::config::ConnectionConfig;
use crate::error::{ConnectionError, Result};
use async_trait::async_trait;
use fluree_db_core::Db;
use fluree_db_nameservice::{
    NameService, NameServiceError, NsRecord, Publisher, StorageNameService,
};
use fluree_db_storage_aws::{
    DynamoDbConfig as RawDynamoDbConfig, DynamoDbNameService, S3Config as RawS3Config, S3Storage,
};
use once_cell::sync::OnceCell;
use std::sync::Arc;

/// Global AWS SDK config cache
///
/// Caches the SDK config to avoid repeated credential resolution
/// in Lambda environments where cold start latency matters.
static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::new();

/// S3 storage configuration
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name (supports both standard S3 and S3 Express directory buckets)
    pub bucket: String,
    /// Optional key prefix
    pub prefix: Option<String>,
    /// Operation timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            timeout_ms: None,
        }
    }
}

impl From<S3Config> for RawS3Config {
    fn from(config: S3Config) -> Self {
        RawS3Config {
            bucket: config.bucket,
            prefix: config.prefix,
            endpoint: None,
            timeout_ms: config.timeout_ms,
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
        }
    }
}

/// DynamoDB configuration for nameservice
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// DynamoDB table name
    pub table_name: String,
    /// AWS region (optional, uses SDK default)
    pub region: Option<String>,
    /// Optional endpoint override (e.g. LocalStack)
    pub endpoint: Option<String>,
    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            table_name: "fluree-nameservice".to_string(),
            region: None,
            endpoint: None,
            timeout_ms: None,
        }
    }
}

impl From<DynamoDbConfig> for RawDynamoDbConfig {
    fn from(config: DynamoDbConfig) -> Self {
        RawDynamoDbConfig {
            table_name: config.table_name,
            region: config.region,
            endpoint: config.endpoint,
            timeout_ms: config.timeout_ms,
        }
    }
}

/// Nameservice configuration options for AWS connections
#[derive(Debug, Clone)]
pub enum AwsNameServiceConfig {
    /// Use DynamoDB for nameservice (recommended for production)
    DynamoDb(DynamoDbConfig),
    /// Use S3 storage for nameservice (simpler setup, uses same bucket as data)
    Storage {
        /// Optional prefix for nameservice files within the storage bucket
        /// If None, uses the same prefix as index_storage
        prefix: Option<String>,
    },
}

impl Default for AwsNameServiceConfig {
    fn default() -> Self {
        AwsNameServiceConfig::DynamoDb(DynamoDbConfig::default())
    }
}

/// AWS connection configuration
#[derive(Debug, Clone)]
pub struct AwsConfig {
    /// S3 storage for indexes (primary data structure for queries)
    pub index_storage: S3Config,
    /// Optional separate S3 storage for commits
    /// If None, index_storage is used for both
    pub commit_storage: Option<S3Config>,
    /// Nameservice configuration (DynamoDB or S3 storage-backed)
    pub nameservice: AwsNameServiceConfig,
    /// Maximum cache entries
    pub cache_max_entries: usize,
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            index_storage: S3Config::default(),
            commit_storage: None,
            nameservice: AwsNameServiceConfig::default(),
            cache_max_entries: 10000,
        }
    }
}

/// Runtime nameservice wrapper that can hold either DynamoDB or storage-backed nameservice
///
/// This enum provides a unified interface for both nameservice types.
#[derive(Clone)]
pub enum AwsNameService {
    /// DynamoDB-backed nameservice
    DynamoDb(Arc<DynamoDbNameService>),
    /// S3 storage-backed nameservice
    Storage(Arc<StorageNameService<S3Storage>>),
}

impl std::fmt::Debug for AwsNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DynamoDb(ns) => f.debug_tuple("DynamoDb").field(ns).finish(),
            Self::Storage(ns) => f.debug_tuple("Storage").field(ns).finish(),
        }
    }
}

#[async_trait]
impl NameService for AwsNameService {
    async fn lookup(
        &self,
        ledger_address: &str,
    ) -> std::result::Result<Option<NsRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.lookup(ledger_address).await,
            Self::Storage(ns) => ns.lookup(ledger_address).await,
        }
    }

    async fn alias(
        &self,
        ledger_address: &str,
    ) -> std::result::Result<Option<String>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.alias(ledger_address).await,
            Self::Storage(ns) => ns.alias(ledger_address).await,
        }
    }

    async fn all_records(&self) -> std::result::Result<Vec<NsRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.all_records().await,
            Self::Storage(ns) => ns.all_records().await,
        }
    }
}

#[async_trait]
impl Publisher for AwsNameService {
    async fn publish_ledger_init(&self, alias: &str) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_ledger_init(alias).await,
            Self::Storage(ns) => ns.publish_ledger_init(alias).await,
        }
    }

    async fn publish_commit(
        &self,
        alias: &str,
        commit_addr: &str,
        commit_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_commit(alias, commit_addr, commit_t).await,
            Self::Storage(ns) => ns.publish_commit(alias, commit_addr, commit_t).await,
        }
    }

    async fn publish_index(
        &self,
        alias: &str,
        index_addr: &str,
        index_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_index(alias, index_addr, index_t).await,
            Self::Storage(ns) => ns.publish_index(alias, index_addr, index_t).await,
        }
    }

    async fn retract(&self, alias: &str) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.retract(alias).await,
            Self::Storage(ns) => ns.retract(alias).await,
        }
    }

    fn publishing_address(&self, alias: &str) -> Option<String> {
        match self {
            Self::DynamoDb(ns) => ns.publishing_address(alias),
            Self::Storage(ns) => ns.publishing_address(alias),
        }
    }
}

/// AWS-specific connection handle for Lambda deployments
///
/// S3Storage and nameservice (DynamoDB or S3-backed) are Clone,
/// so this handle can be cheaply cloned for concurrent operations.
#[derive(Clone)]
pub struct AwsConnectionHandle {
    /// Connection configuration
    config: ConnectionConfig,
    /// S3 storage for indexes
    index_storage: S3Storage,
    /// Optional separate S3 storage for commits
    commit_storage: Option<S3Storage>,
    /// Nameservice (DynamoDB or S3 storage-backed)
    nameservice: AwsNameService,
}

impl std::fmt::Debug for AwsConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsConnectionHandle")
            .field("index_storage", &self.index_storage)
            .field(
                "has_separate_commit_storage",
                &self.commit_storage.is_some(),
            )
            .field("nameservice", &self.nameservice)
            .finish()
    }
}

impl AwsConnectionHandle {
    /// Create a new AWS connection handle
    ///
    /// This is used internally by `create_aws_connection` for JSON-LD configs
    /// where storage sharing via registry is desired.
    pub fn new(
        config: ConnectionConfig,
        index_storage: S3Storage,
        commit_storage: Option<S3Storage>,
        nameservice: AwsNameService,
    ) -> Self {
        Self {
            config,
            index_storage,
            commit_storage,
            nameservice,
        }
    }

    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Get a reference to the index storage
    pub fn index_storage(&self) -> &S3Storage {
        &self.index_storage
    }

    /// Get a reference to the commit storage
    ///
    /// Returns the index storage if no separate commit storage was configured.
    pub fn commit_storage(&self) -> &S3Storage {
        self.commit_storage.as_ref().unwrap_or(&self.index_storage)
    }

    /// Get a reference to the nameservice
    pub fn nameservice(&self) -> &AwsNameService {
        &self.nameservice
    }

    /// Load a database by alias
    ///
    /// Uses the nameservice to look up the ledger's index root address,
    /// then loads the database from that address.
    ///
    /// # Arguments
    ///
    /// * `alias` - Ledger alias (e.g., "mydb" or "mydb:main")
    ///
    /// # Returns
    ///
    /// A `Db` instance backed by S3 storage, or an error if the ledger
    /// is not found or has no index.
    pub async fn load_db(&self, alias: &str) -> Result<Db<S3Storage>> {
        let record = self
            .nameservice
            .lookup(alias)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {}", e)))?
            .ok_or_else(|| ConnectionError::not_found(format!("Ledger not found: {}", alias)))?;

        if record.retracted {
            return Err(ConnectionError::not_found(format!(
                "Ledger has been retracted: {}",
                alias
            )));
        }

        let index_address = record.index_address.ok_or_else(|| {
            ConnectionError::not_found(format!("Ledger has no index yet: {}", alias))
        })?;

        self.load_db_by_address(&index_address).await
    }

    /// Load a database by index root address
    ///
    /// Loads the database directly from the given index root address,
    /// bypassing the nameservice lookup.
    ///
    /// # Arguments
    ///
    /// * `root_address` - The index root address (e.g., "fluree:s3://bucket/path/root.json")
    pub async fn load_db_by_address(&self, root_address: &str) -> Result<Db<S3Storage>> {
        let storage = self.index_storage.clone();

        Ok(Db::load(storage, root_address).await?)
    }

    /// Look up a ledger record by alias
    ///
    /// Returns the full `NsRecord` including commit and index addresses.
    pub async fn lookup(&self, alias: &str) -> Result<Option<NsRecord>> {
        self.nameservice
            .lookup(alias)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {}", e)))
    }

    /// Publish a new commit to the nameservice
    ///
    /// This is typically called by the transactor after a successful commit.
    pub async fn publish_commit(
        &self,
        alias: &str,
        commit_addr: &str,
        commit_t: i64,
    ) -> Result<()> {
        self.nameservice
            .publish_commit(alias, commit_addr, commit_t)
            .await
            .map_err(|e| ConnectionError::storage(format!("Publish commit failed: {}", e)))
    }

    /// Publish a new index to the nameservice
    ///
    /// This is typically called by the indexer after successfully writing new index roots.
    pub async fn publish_index(&self, alias: &str, index_addr: &str, index_t: i64) -> Result<()> {
        self.nameservice
            .publish_index(alias, index_addr, index_t)
            .await
            .map_err(|e| ConnectionError::storage(format!("Publish index failed: {}", e)))
    }
}

/// Create an AWS connection handle
///
/// Loads or reuses the cached AWS SDK config and creates S3 storage
/// and nameservice instances.
///
/// # Arguments
///
/// * `config` - AWS configuration specifying buckets and nameservice type
///
/// # Example with DynamoDB
///
/// ```ignore
/// let config = AwsConfig {
///     index_storage: S3Config {
///         bucket: "my-bucket".to_string(),
///         ..Default::default()
///     },
///     nameservice: AwsNameServiceConfig::DynamoDb(DynamoDbConfig {
///         table_name: "fluree-ns".to_string(),
///         ..Default::default()
///     }),
///     ..Default::default()
/// };
///
/// let conn = connect_aws(config).await?;
/// let db = conn.load_db("mydb").await?;
/// ```
///
/// # Example with S3 storage-backed nameservice
///
/// ```ignore
/// let config = AwsConfig {
///     index_storage: S3Config {
///         bucket: "my-bucket".to_string(),
///         prefix: Some("ledgers".to_string()),
///         ..Default::default()
///     },
///     nameservice: AwsNameServiceConfig::Storage { prefix: Some("ns".to_string()) },
///     ..Default::default()
/// };
///
/// let conn = connect_aws(config).await?;
/// ```
pub async fn connect_aws(config: AwsConfig) -> Result<AwsConnectionHandle> {
    // Get or initialize SDK config
    let sdk_config = get_or_init_sdk_config().await?;

    // Create index storage
    let index_storage = S3Storage::new(sdk_config, config.index_storage.clone().into())
        .await
        .map_err(|e| ConnectionError::storage(format!("Failed to create index storage: {}", e)))?;

    // Create commit storage if separate bucket configured
    let commit_storage = if let Some(commit_config) = config.commit_storage {
        Some(
            S3Storage::new(sdk_config, commit_config.into())
                .await
                .map_err(|e| {
                    ConnectionError::storage(format!("Failed to create commit storage: {}", e))
                })?,
        )
    } else {
        None
    };

    // Create nameservice based on config type
    let nameservice = match config.nameservice {
        AwsNameServiceConfig::DynamoDb(dynamo_config) => {
            let ns = DynamoDbNameService::new(sdk_config, dynamo_config.into())
                .await
                .map_err(|e| {
                    ConnectionError::storage(format!(
                        "Failed to create DynamoDB nameservice: {}",
                        e
                    ))
                })?;
            AwsNameService::DynamoDb(Arc::new(ns))
        }
        AwsNameServiceConfig::Storage { prefix } => {
            // Create a separate S3 storage instance for nameservice
            // Use the same bucket as index storage but potentially different prefix
            //
            // The prefix hierarchy is:
            // - S3Storage handles bucket-level prefix (e.g., "ledgers/")
            // - StorageNameService adds "ns@v2/{ledger}/{branch}.json"
            //
            // So if user specifies prefix="ns", final key is: ns/ns@v2/mydb/main.json
            let ns_s3_prefix = prefix.or_else(|| config.index_storage.prefix.clone());

            let ns_s3_config = RawS3Config {
                bucket: config.index_storage.bucket.clone(),
                prefix: ns_s3_prefix,
                endpoint: None,
                timeout_ms: config.index_storage.timeout_ms,
                max_retries: None,
                retry_base_delay_ms: None,
                retry_max_delay_ms: None,
            };

            let ns_storage = S3Storage::new(sdk_config, ns_s3_config)
                .await
                .map_err(|e| {
                    ConnectionError::storage(format!(
                        "Failed to create S3 storage for nameservice: {}",
                        e
                    ))
                })?;

            // StorageNameService prefix is empty - S3Storage already has the bucket prefix
            // StorageNameService will add "ns@v2/..." to keys
            let ns = StorageNameService::new(ns_storage, "");
            AwsNameService::Storage(Arc::new(ns))
        }
    };

    // Build connection config (for compatibility)
    let conn_config = ConnectionConfig {
        cache: crate::config::CacheConfig {
            max_entries: config.cache_max_entries,
            max_mb: 100, // Default value
        },
        ..ConnectionConfig::default()
    };

    Ok(AwsConnectionHandle {
        config: conn_config,
        index_storage,
        commit_storage,
        nameservice,
    })
}

/// Get or initialize the AWS SDK config
///
/// Uses `OnceCell` to cache the config and avoid repeated credential
/// resolution in Lambda environments.
///
/// This is public for use by `create_aws_connection` which builds connections
/// from JSON-LD configs using the storage registry.
pub async fn get_or_init_sdk_config() -> Result<&'static aws_config::SdkConfig> {
    // Try to get existing config
    if let Some(config) = SDK_CONFIG.get() {
        return Ok(config);
    }

    // Load new config
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Try to set it (another thread may have beaten us)
    let _ = SDK_CONFIG.set(config);

    // Return the config (either ours or the one another thread set)
    SDK_CONFIG
        .get()
        .ok_or_else(|| ConnectionError::storage("Failed to initialize AWS SDK config"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert!(config.bucket.is_empty());
        assert!(config.prefix.is_none());
    }

    #[test]
    fn test_dynamodb_config_default() {
        let config = DynamoDbConfig::default();
        assert_eq!(config.table_name, "fluree-nameservice");
    }

    #[test]
    fn test_aws_config_default() {
        let config = AwsConfig::default();
        assert_eq!(config.cache_max_entries, 10000);
        assert!(config.commit_storage.is_none());
    }
}
