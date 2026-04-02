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
//! This module is used by the JSON-LD connection path (`connect_async`) when a
//! `ConnectionConfig` contains S3 storage. It intentionally does not expose a
//! separate AWS-specific configuration surface.

use crate::config::ConnectionConfig;
use crate::error::{ConnectionError, Result};
use async_trait::async_trait;
use fluree_db_core::LedgerSnapshot;
use fluree_db_nameservice::{
    AdminPublisher, CasResult, ConfigCasResult, ConfigPublisher, ConfigValue, GraphSourceLookup,
    GraphSourcePublisher, GraphSourceRecord, GraphSourceType, NameService, NameServiceError,
    NsLookupResult, NsRecord, Publisher, RefKind, RefPublisher, RefValue, StatusCasResult,
    StatusPublisher, StatusValue, StorageNameService,
};
use fluree_db_storage_aws::{DynamoDbNameService, S3Storage};
use once_cell::sync::OnceCell;
use std::sync::Arc;

/// Global AWS SDK config cache
///
/// Caches the SDK config to avoid repeated credential resolution
/// in Lambda environments where cold start latency matters.
static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::new();

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
        ledger_id: &str,
    ) -> std::result::Result<Option<NsRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.lookup(ledger_id).await,
            Self::Storage(ns) => ns.lookup(ledger_id).await,
        }
    }

    async fn all_records(&self) -> std::result::Result<Vec<NsRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.all_records().await,
            Self::Storage(ns) => ns.all_records().await,
        }
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => {
                ns.create_branch(ledger_name, new_branch, source_branch)
                    .await
            }
            Self::Storage(ns) => {
                ns.create_branch(ledger_name, new_branch, source_branch)
                    .await
            }
        }
    }

    async fn drop_branch(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<u32>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.drop_branch(ledger_id).await,
            Self::Storage(ns) => ns.drop_branch(ledger_id).await,
        }
    }

    async fn reset_head(
        &self,
        ledger_id: &str,
        snapshot: fluree_db_nameservice::NsRecordSnapshot,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.reset_head(ledger_id, snapshot).await,
            Self::Storage(ns) => ns.reset_head(ledger_id, snapshot).await,
        }
    }
}

#[async_trait]
impl Publisher for AwsNameService {
    async fn publish_ledger_init(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_ledger_init(ledger_id).await,
            Self::Storage(ns) => ns.publish_ledger_init(ledger_id).await,
        }
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_commit(ledger_id, commit_t, commit_id).await,
            Self::Storage(ns) => ns.publish_commit(ledger_id, commit_t, commit_id).await,
        }
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.publish_index(ledger_id, index_t, index_id).await,
            Self::Storage(ns) => ns.publish_index(ledger_id, index_t, index_id).await,
        }
    }

    async fn retract(&self, ledger_id: &str) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.retract(ledger_id).await,
            Self::Storage(ns) => ns.retract(ledger_id).await,
        }
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        match self {
            Self::DynamoDb(ns) => ns.publishing_ledger_id(ledger_id),
            Self::Storage(ns) => ns.publishing_ledger_id(ledger_id),
        }
    }
}

#[async_trait]
impl AdminPublisher for AwsNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => {
                ns.publish_index_allow_equal(ledger_id, index_t, index_id)
                    .await
            }
            Self::Storage(ns) => {
                ns.publish_index_allow_equal(ledger_id, index_t, index_id)
                    .await
            }
        }
    }
}

#[async_trait]
impl RefPublisher for AwsNameService {
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
    ) -> std::result::Result<Option<RefValue>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.get_ref(ledger_id, kind).await,
            Self::Storage(ns) => ns.get_ref(ledger_id, kind).await,
        }
    }

    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> std::result::Result<CasResult, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.compare_and_set_ref(ledger_id, kind, expected, new).await,
            Self::Storage(ns) => ns.compare_and_set_ref(ledger_id, kind, expected, new).await,
        }
    }
}

#[async_trait]
impl GraphSourcePublisher for AwsNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => {
                ns.publish_graph_source(name, branch, source_type, config, dependencies)
                    .await
            }
            Self::Storage(ns) => {
                ns.publish_graph_source(name, branch, source_type, config, dependencies)
                    .await
            }
        }
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &fluree_db_core::ContentId,
        index_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => {
                ns.publish_graph_source_index(name, branch, index_id, index_t)
                    .await
            }
            Self::Storage(ns) => {
                ns.publish_graph_source_index(name, branch, index_id, index_t)
                    .await
            }
        }
    }

    async fn retract_graph_source(
        &self,
        name: &str,
        branch: &str,
    ) -> std::result::Result<(), NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.retract_graph_source(name, branch).await,
            Self::Storage(ns) => ns.retract_graph_source(name, branch).await,
        }
    }
}

#[async_trait]
impl GraphSourceLookup for AwsNameService {
    async fn lookup_graph_source(
        &self,
        address: &str,
    ) -> std::result::Result<Option<GraphSourceRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.lookup_graph_source(address).await,
            Self::Storage(ns) => ns.lookup_graph_source(address).await,
        }
    }

    async fn lookup_any(
        &self,
        address: &str,
    ) -> std::result::Result<NsLookupResult, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.lookup_any(address).await,
            Self::Storage(ns) => ns.lookup_any(address).await,
        }
    }

    async fn all_graph_source_records(
        &self,
    ) -> std::result::Result<Vec<GraphSourceRecord>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.all_graph_source_records().await,
            Self::Storage(ns) => ns.all_graph_source_records().await,
        }
    }
}

#[async_trait]
impl StatusPublisher for AwsNameService {
    async fn get_status(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<StatusValue>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.get_status(ledger_id).await,
            Self::Storage(ns) => ns.get_status(ledger_id).await,
        }
    }

    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> std::result::Result<StatusCasResult, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.push_status(ledger_id, expected, new).await,
            Self::Storage(ns) => ns.push_status(ledger_id, expected, new).await,
        }
    }
}

#[async_trait]
impl ConfigPublisher for AwsNameService {
    async fn get_config(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<ConfigValue>, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.get_config(ledger_id).await,
            Self::Storage(ns) => ns.get_config(ledger_id).await,
        }
    }

    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> std::result::Result<ConfigCasResult, NameServiceError> {
        match self {
            Self::DynamoDb(ns) => ns.push_config(ledger_id, expected, new).await,
            Self::Storage(ns) => ns.push_config(ledger_id, expected, new).await,
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

    /// Load a database by ledger ID
    ///
    /// Uses the nameservice to look up the ledger's index root address,
    /// then loads the database from that address.
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - Ledger ID (e.g., "mydb" or "mydb:main")
    ///
    /// # Returns
    ///
    /// A `LedgerSnapshot` instance backed by S3 storage, or an error if the ledger
    /// is not found or has no index.
    pub async fn load_ledger_snapshot(&self, ledger_id: &str) -> Result<LedgerSnapshot> {
        let record = self
            .nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {}", e)))?
            .ok_or_else(|| {
                ConnectionError::not_found(format!("Ledger not found: {}", ledger_id))
            })?;

        if record.retracted {
            return Err(ConnectionError::not_found(format!(
                "Ledger has been retracted: {}",
                ledger_id
            )));
        }

        let index_id = record.index_head_id.ok_or_else(|| {
            ConnectionError::not_found(format!("Ledger has no index yet: {}", ledger_id))
        })?;

        let storage = self.index_storage.clone();
        Ok(fluree_db_core::load_ledger_snapshot(&storage, &index_id, ledger_id).await?)
    }

    /// Look up a ledger record by ledger ID
    ///
    /// Returns the full `NsRecord` including commit and index IDs.
    pub async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        self.nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {}", e)))
    }

    /// Publish a new commit to the nameservice
    ///
    /// This is typically called by the transactor after a successful commit.
    pub async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &fluree_db_core::ContentId,
    ) -> Result<()> {
        self.nameservice
            .publish_commit(ledger_id, commit_t, commit_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Publish commit failed: {}", e)))
    }

    /// Publish a new index to the nameservice
    ///
    /// This is typically called by the indexer after successfully writing new index roots.
    pub async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> Result<()> {
        self.nameservice
            .publish_index(ledger_id, index_t, index_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Publish index failed: {}", e)))
    }
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
