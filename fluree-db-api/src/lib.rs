//! # Fluree DB API
//!
//! High-level API for Fluree DB operations, providing unified access to
//! connections, ledger state management, and query execution.
//!
//! This crate composes the lower-level crates:
//! - `fluree-db-connection` - Configuration and storage
//! - `fluree-db-ledger` - Ledger state (Db + Novelty)
//! - `fluree-db-query` - Query execution
//! - `fluree-db-nameservice` - Ledger discovery
//!
//! ## Quick Start
//!
//! ```ignore
//! use fluree_db_api::FlureeBuilder;
//!
//! // Create a file-backed Fluree instance
//! let fluree = FlureeBuilder::file("/data/fluree").build()?;
//!
//! // Create a new ledger
//! let ledger = fluree.create_ledger("mydb").await?;
//!
//! // Insert data
//! let result = fluree.insert(ledger, &data).await?;
//! let ledger = result.ledger;
//!
//! // Query
//! let results = fluree.query(&ledger, &query).await?;
//!
//! // Load an existing ledger
//! let ledger = fluree.ledger("mydb:main").await?;
//! ```

pub mod admin;
pub mod block_fetch;
pub mod bm25_worker;
pub mod commit_transfer;
#[cfg(feature = "credential")]
pub mod credential;
pub mod dataset;
mod error;
pub mod explain;
pub mod format;
pub mod graph;
pub mod graph_query_builder;
pub mod graph_snapshot;
pub mod graph_source;
pub mod graph_transact_builder;
pub mod import;
mod ledger;
pub mod ledger_info;
pub mod nameservice_query;
mod overlay;
pub mod pack;
pub mod policy_builder;
pub mod policy_view;
mod query;
pub mod server_defaults;
mod time_resolve;
pub mod tx;
pub mod tx_builder;
#[cfg(feature = "vector")]
pub mod vector_worker;
pub mod view;

// Ledger caching and management
pub mod ledger_manager;

// Search service integration (embedded adapter, remote client)
pub mod search;

pub use admin::{
    DropMode,
    DropReport,
    DropStatus,
    GraphSourceDropReport,
    // Index maintenance
    IndexStatusResult,
    ReindexOptions,
    ReindexResult,
    TriggerIndexOptions,
    TriggerIndexResult,
};
pub use block_fetch::{
    BlockAccessScope, BlockContent, BlockFetchError, EnforcementMode, FetchedBlock,
    LedgerBlockContext,
};
pub use commit_transfer::{
    Base64Bytes, BulkImportResult, CommitImportResult, ExportCommitsRequest, ExportCommitsResponse,
    PushCommitsRequest, PushCommitsResponse,
};
pub use dataset::{
    sparql_dataset_ledger_ids, DatasetParseError, DatasetSpec, GraphSource, QueryConnectionOptions,
    TimeSpec,
};
pub use error::{ApiError, BuilderError, BuilderErrors, Result};
pub use format::{FormatError, FormatterConfig, JsonLdRowShape, OutputFormat, SelectMode};
pub use graph::Graph;
pub use graph_query_builder::{GraphQueryBuilder, GraphSnapshotQueryBuilder};
pub use graph_snapshot::GraphSnapshot;
pub use graph_source::{
    Bm25CreateConfig, Bm25CreateResult, Bm25DropResult, Bm25StalenessCheck, Bm25SyncResult,
    FlureeIndexProvider, SnapshotSelection,
};
pub use graph_transact_builder::{GraphTransactBuilder, StagedGraph};
pub use import::{
    CreateBuilder, EffectiveImportSettings, ImportBuilder, ImportConfig, ImportError, ImportPhase,
    ImportResult,
};
pub use ledger_info::LedgerInfoBuilder;
pub use ledger_manager::{
    FreshnessCheck, FreshnessSource, LedgerHandle, LedgerManager, LedgerManagerConfig,
    LedgerSnapshot, LedgerWriteGuard, NotifyResult, NsNotify, RemoteWatermark, UpdatePlan,
};
pub use pack::{
    compute_missing_index_artifacts, validate_pack_request, PackChunk, PackStreamError,
    PackStreamResult,
};
pub use policy_view::{
    build_policy_context, wrap_identity_policy_view, wrap_policy_view, wrap_policy_view_historical,
    PolicyWrappedView,
};
pub use query::builder::{
    DatasetQueryBuilder, FromQueryBuilder, GraphSourceMode, ViewQueryBuilder,
};
pub use query::nameservice_builder::NameserviceQueryBuilder;
pub use query::{QueryResult, TrackedErrorResponse, TrackedQueryResponse};
pub use tx::{
    IndexingMode, IndexingStatus, StageResult, TrackedTransactionInput, TransactResult,
    TransactResultRef,
};
pub use tx_builder::{OwnedTransactBuilder, RefTransactBuilder, Staged};
pub use view::{FlureeDataSetView, FlureeView, QueryInput, ReasoningModePrecedence};

#[cfg(feature = "iceberg")]
pub use graph_source::{
    FlureeR2rmlProvider, IcebergCreateConfig, IcebergCreateResult, R2rmlCreateConfig,
    R2rmlCreateResult,
};

pub use bm25_worker::{
    Bm25MaintenanceWorker, Bm25WorkerConfig, Bm25WorkerHandle, Bm25WorkerState, Bm25WorkerStats,
};

#[cfg(feature = "vector")]
pub use vector_worker::{
    VectorMaintenanceWorker, VectorWorkerConfig, VectorWorkerHandle, VectorWorkerState,
    VectorWorkerStats,
};

#[cfg(feature = "vector")]
pub use graph_source::{
    VectorCreateConfig, VectorCreateResult, VectorDropResult, VectorStalenessCheck,
    VectorSyncResult,
};

// Re-export search provider adapter
pub use search::EmbeddedBm25SearchProvider;

// Re-export indexer types for background indexing setup
pub use fluree_db_indexer::{
    BackgroundIndexerWorker, IndexCompletion, IndexOutcome, IndexPhase, IndexStatusSnapshot,
    IndexerConfig, IndexerHandle,
};

// Re-export commonly used types from child crates
pub use fluree_db_connection::{ConnectionConfig, StorageType};
#[cfg(feature = "native")]
pub use fluree_db_core::FileStorage;
pub use fluree_db_core::{
    ContentAddressedWrite, ContentKind, ContentWriteResult, MemoryStorage, OverlayProvider,
    Storage, StorageMethod, StorageRead, StorageWrite,
};
pub use fluree_db_ledger::{
    HistoricalLedgerView, IndexConfig, LedgerState, LedgerView, TypeErasedStore,
};
pub use fluree_db_nameservice::{GraphSourcePublisher, NameService, NsRecord, Publisher};
pub use fluree_db_novelty::{verify_commit_v2_blob, Novelty};
pub use fluree_db_query::{
    execute_pattern, execute_pattern_with_overlay, execute_pattern_with_overlay_at,
    execute_with_dataset_and_bm25, execute_with_dataset_and_policy_and_bm25,
    execute_with_dataset_and_policy_and_providers, execute_with_dataset_and_providers,
    execute_with_overlay, execute_with_overlay_tracked, execute_with_r2rml, Batch, DataSource,
    ExecutableQuery, NoOpR2rmlProvider, Pattern, QueryContextParams, QueryOptions, VarRegistry,
};
// Re-export for lower-level pattern-based queries (internal/advanced use)
pub use fluree_db_query::{Term, TriplePattern};
// Re-export parse types for query results
pub use fluree_db_query::parse::{ParseError, ParsedQuery};
pub use fluree_db_transact::{
    lower_sparql_update, lower_sparql_update_ast, CommitOpts, CommitReceipt,
    LowerError as SparqlUpdateLowerError, NamespaceRegistry, TxnOpts, TxnType,
};

// Re-export SPARQL types (product feature; always enabled)
pub use fluree_db_sparql::{
    lower_sparql, parse_sparql, validate as validate_sparql, Capabilities as SparqlCapabilities,
    Diagnostic as SparqlDiagnostic, LowerError as SparqlLowerError,
    ParseOutput as SparqlParseOutput, Prologue as SparqlPrologue, QueryBody as SparqlQueryBody,
    Severity as SparqlSeverity, SourceSpan as SparqlSourceSpan, SparqlAst,
    UpdateOperation as SparqlUpdateOperation,
};

// Re-export policy types for access control
pub use fluree_db_policy::{
    build_policy_set, build_policy_values_clause, filter_by_required, is_schema_flake,
    NoOpQueryExecutor, PolicyAction, PolicyContext, PolicyError, PolicyQuery, PolicyQueryExecutor,
    PolicyRestriction, PolicySet, PolicyValue, PolicyWrapper, TargetMode,
};

// Re-export tracking types for query/transaction metrics
pub use fluree_db_core::{FuelExceededError, PolicyStats, Tracker, TrackingOptions, TrackingTally};

use async_trait::async_trait;
use fluree_db_connection::Connection;
#[cfg(feature = "native")]
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::memory::MemoryNameService;
#[cfg(feature = "aws")]
use fluree_db_nameservice::StorageNameService;
use std::sync::Arc;

// Re-export encryption types for convenient access
pub use fluree_db_crypto::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
pub use fluree_graph_json_ld::ParsedContext;

// ============================================================================
// Dynamic runtime wrappers (single JSON-LD "source of truth")
// ============================================================================

/// A dynamically-dispatched storage backend.
///
/// This allows `connect_json_ld` to return a single concrete Fluree type regardless of
/// whether the config selects memory, filesystem, or S3 storage.
///
/// Wraps `Arc<dyn Storage>` where `Storage = StorageRead + ContentAddressedWrite`.
#[derive(Clone)]
pub struct AnyStorage(Arc<dyn Storage>);

impl std::fmt::Debug for AnyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyStorage").field(&self.0).finish()
    }
}

impl AnyStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl StorageRead for AnyStorage {
    async fn read_bytes(
        &self,
        address: &str,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.0.read_bytes(address).await
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: fluree_db_core::ReadHint,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.0.read_bytes_hint(address, hint).await
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, fluree_db_core::Error> {
        self.0.exists(address).await
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<String>, fluree_db_core::Error> {
        self.0.list_prefix(prefix).await
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        self.0.resolve_local_path(address)
    }
}

#[async_trait]
impl StorageWrite for AnyStorage {
    async fn write_bytes(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> std::result::Result<(), fluree_db_core::Error> {
        self.0.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> std::result::Result<(), fluree_db_core::Error> {
        self.0.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for AnyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.0
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.0.content_write_bytes(kind, ledger_id, bytes).await
    }
}

impl StorageMethod for AnyStorage {
    fn storage_method(&self) -> &str {
        self.0.storage_method()
    }
}

/// A dynamically-dispatched nameservice + publisher.
pub trait NameServicePublisher:
    fluree_db_nameservice::NameService
    + fluree_db_nameservice::Publisher
    + fluree_db_nameservice::RefPublisher
{
}
impl<T> NameServicePublisher for T where
    T: fluree_db_nameservice::NameService
        + fluree_db_nameservice::Publisher
        + fluree_db_nameservice::RefPublisher
{
}

#[derive(Clone)]
pub struct AnyNameService(Arc<dyn NameServicePublisher>);

impl std::fmt::Debug for AnyNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyNameService").field(&self.0).finish()
    }
}

impl AnyNameService {
    pub fn new(inner: Arc<dyn NameServicePublisher>) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl fluree_db_nameservice::NameService for AnyNameService {
    async fn lookup(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.0.lookup(ledger_id).await
    }

    async fn all_records(
        &self,
    ) -> std::result::Result<
        Vec<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.0.all_records().await
    }
}

#[async_trait]
impl fluree_db_nameservice::Publisher for AnyNameService {
    async fn publish_ledger_init(
        &self,
        alias: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.0.publish_ledger_init(alias).await
    }

    async fn publish_commit(
        &self,
        alias: &str,
        commit_t: i64,
        commit_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.0.publish_commit(alias, commit_t, commit_id).await
    }

    async fn publish_index(
        &self,
        alias: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.0.publish_index(alias, index_t, index_id).await
    }

    async fn retract(
        &self,
        alias: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.0.retract(alias).await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        self.0.publishing_ledger_id(ledger_id)
    }
}

#[async_trait]
impl fluree_db_nameservice::RefPublisher for AnyNameService {
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::RefValue>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.0.get_ref(ledger_id, kind).await
    }

    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
        expected: Option<&fluree_db_nameservice::RefValue>,
        new: &fluree_db_nameservice::RefValue,
    ) -> std::result::Result<
        fluree_db_nameservice::CasResult,
        fluree_db_nameservice::NameServiceError,
    > {
        self.0
            .compare_and_set_ref(ledger_id, kind, expected, new)
            .await
    }
}

/// Transparent delegating nameservice wrapper.
///
/// This wrapper is retained so that builder call-sites do not need
/// restructuring — it simply delegates every call to `inner`.
#[derive(Clone, Debug)]
struct DelegatingNameService<N> {
    inner: N,
}

impl<N> DelegatingNameService<N> {
    fn new(inner: N) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<N> fluree_db_nameservice::NameService for DelegatingNameService<N>
where
    N: fluree_db_nameservice::NameService
        + fluree_db_nameservice::Publisher
        + std::fmt::Debug
        + Send
        + Sync,
{
    async fn lookup(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.inner.lookup(ledger_id).await
    }

    async fn all_records(
        &self,
    ) -> std::result::Result<
        Vec<fluree_db_nameservice::NsRecord>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.inner.all_records().await
    }
}

#[async_trait::async_trait]
impl<N> fluree_db_nameservice::Publisher for DelegatingNameService<N>
where
    N: fluree_db_nameservice::NameService
        + fluree_db_nameservice::Publisher
        + std::fmt::Debug
        + Send
        + Sync,
{
    async fn publish_ledger_init(
        &self,
        alias: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.inner.publish_ledger_init(alias).await
    }

    async fn publish_commit(
        &self,
        alias: &str,
        commit_t: i64,
        commit_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.inner.publish_commit(alias, commit_t, commit_id).await
    }

    async fn publish_index(
        &self,
        alias: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.inner.publish_index(alias, index_t, index_id).await
    }

    async fn retract(
        &self,
        alias: &str,
    ) -> std::result::Result<(), fluree_db_nameservice::NameServiceError> {
        self.inner.retract(alias).await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        self.inner.publishing_ledger_id(ledger_id)
    }
}

#[async_trait::async_trait]
impl<N> fluree_db_nameservice::RefPublisher for DelegatingNameService<N>
where
    N: fluree_db_nameservice::NameService
        + fluree_db_nameservice::Publisher
        + fluree_db_nameservice::RefPublisher
        + std::fmt::Debug
        + Send
        + Sync,
{
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
    ) -> std::result::Result<
        Option<fluree_db_nameservice::RefValue>,
        fluree_db_nameservice::NameServiceError,
    > {
        self.inner.get_ref(ledger_id, kind).await
    }

    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: fluree_db_nameservice::RefKind,
        expected: Option<&fluree_db_nameservice::RefValue>,
        new: &fluree_db_nameservice::RefValue,
    ) -> std::result::Result<
        fluree_db_nameservice::CasResult,
        fluree_db_nameservice::NameServiceError,
    > {
        self.inner
            .compare_and_set_ref(ledger_id, kind, expected, new)
            .await
    }
}

/// Tiered storage router used for Clojure-style `commitStorage` vs `indexStorage`.
///
/// This routes writes/reads based on the address path:
/// - `.../commit/...` and `.../txn/...` -> commit storage
/// - everything else -> index storage
#[derive(Clone, Debug)]
pub struct TieredStorage<S> {
    commit: S,
    index: S,
}

impl<S> TieredStorage<S> {
    pub fn new(commit: S, index: S) -> Self {
        Self { commit, index }
    }

    fn route_to_commit(address: &str) -> bool {
        // Extract the path portion after :// if present (fluree:*://path)
        let path = address.split("://").nth(1).unwrap_or(address);

        // Clojure parity: commit blobs + txn blobs go to commit storage.
        path.contains("/commit/") || path.contains("/txn/")
    }
}

#[async_trait]
impl<S> StorageRead for TieredStorage<S>
where
    S: Storage + Clone + Send + Sync,
{
    async fn read_bytes(
        &self,
        address: &str,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.read_bytes(address).await
        } else {
            self.index.read_bytes(address).await
        }
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: fluree_db_core::ReadHint,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.read_bytes_hint(address, hint).await
        } else {
            self.index.read_bytes_hint(address, hint).await
        }
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.exists(address).await
        } else {
            self.index.exists(address).await
        }
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<String>, fluree_db_core::Error> {
        // Route based on prefix - commit/txn prefixes go to commit storage
        if Self::route_to_commit(prefix) {
            self.commit.list_prefix(prefix).await
        } else {
            self.index.list_prefix(prefix).await
        }
    }
}

#[async_trait]
impl<S> StorageWrite for TieredStorage<S>
where
    S: Storage + Clone + Send + Sync,
{
    async fn write_bytes(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> std::result::Result<(), fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.write_bytes(address, bytes).await
        } else {
            self.index.write_bytes(address, bytes).await
        }
    }

    async fn delete(&self, address: &str) -> std::result::Result<(), fluree_db_core::Error> {
        if Self::route_to_commit(address) {
            self.commit.delete(address).await
        } else {
            self.index.delete(address).await
        }
    }
}

#[async_trait]
impl<S> ContentAddressedWrite for TieredStorage<S>
where
    S: Storage + Clone + Send + Sync,
{
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        // Clojure parity: commit blobs + txn blobs go to commit storage.
        match kind {
            ContentKind::Commit | ContentKind::Txn => {
                self.commit
                    .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
                    .await
            }
            _ => {
                self.index
                    .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
                    .await
            }
        }
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        // Clojure parity: commit blobs + txn blobs go to commit storage.
        match kind {
            ContentKind::Commit | ContentKind::Txn => {
                self.commit
                    .content_write_bytes(kind, ledger_id, bytes)
                    .await
            }
            _ => self.index.content_write_bytes(kind, ledger_id, bytes).await,
        }
    }
}

impl<S: StorageMethod> StorageMethod for TieredStorage<S> {
    fn storage_method(&self) -> &str {
        // Use the index storage method as the canonical method — both tiers
        // use the same method in practice (both S3, both file, etc.)
        self.index.storage_method()
    }
}

// ============================================================================
// Address Identifier Resolver Storage
// ============================================================================

/// Storage wrapper that routes reads based on address identifiers.
///
/// This enables identifier-based routing where addresses like
/// `fluree:<identifier>:<method>://path` are routed to a specific storage backend.
///
/// # Routing Rules
/// - If address contains an identifier that exists in the map -> route to that storage
/// - If address contains an unknown identifier -> fallback to default storage
/// - If address has no identifier -> route to default storage
/// - **Writes always go to default storage** (no identifier-based routing for writes)
///
/// # Example JSON-LD Config
/// ```json
/// {
///   "addressIdentifiers": {
///     "commit-storage": {"@id": "commitS3"},
///     "index-storage": {"@id": "indexS3"}
///   }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct AddressIdentifierResolverStorage {
    /// Default storage for unmatched identifiers and all writes
    default: AnyStorage,
    /// Map of identifier -> storage for routing reads
    identifier_map: std::sync::Arc<std::collections::HashMap<String, AnyStorage>>,
}

impl AddressIdentifierResolverStorage {
    /// Create a new resolver storage.
    ///
    /// # Arguments
    /// - `default`: Storage to use for unmatched identifiers and all writes
    /// - `identifier_map`: Map of identifier string -> storage
    pub fn new(
        default: AnyStorage,
        identifier_map: std::collections::HashMap<String, AnyStorage>,
    ) -> Self {
        Self {
            default,
            identifier_map: std::sync::Arc::new(identifier_map),
        }
    }

    /// Route an address to the appropriate storage for reads.
    fn route(&self, address: &str) -> &AnyStorage {
        if let Some(identifier) = fluree_db_core::extract_identifier(address) {
            if let Some(storage) = self.identifier_map.get(identifier) {
                return storage;
            }
        }
        // Fallback to default for unknown identifiers or no identifier
        &self.default
    }
}

#[async_trait]
impl StorageRead for AddressIdentifierResolverStorage {
    async fn read_bytes(
        &self,
        address: &str,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.route(address).read_bytes(address).await
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: fluree_db_core::ReadHint,
    ) -> std::result::Result<Vec<u8>, fluree_db_core::Error> {
        self.route(address).read_bytes_hint(address, hint).await
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, fluree_db_core::Error> {
        self.route(address).exists(address).await
    }

    /// List always uses the default storage
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<String>, fluree_db_core::Error> {
        self.default.list_prefix(prefix).await
    }

    fn resolve_local_path(&self, address: &str) -> Option<std::path::PathBuf> {
        self.route(address).resolve_local_path(address)
    }
}

#[async_trait]
impl StorageWrite for AddressIdentifierResolverStorage {
    /// Writes always go to the default storage (MVP: no identifier-based write routing)
    async fn write_bytes(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> std::result::Result<(), fluree_db_core::Error> {
        self.default.write_bytes(address, bytes).await
    }

    /// Deletes always go to the default storage
    async fn delete(&self, address: &str) -> std::result::Result<(), fluree_db_core::Error> {
        self.default.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for AddressIdentifierResolverStorage {
    /// Content writes always go to the default storage
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.default
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, fluree_db_core::Error> {
        self.default
            .content_write_bytes(kind, ledger_id, bytes)
            .await
    }
}

impl StorageMethod for AddressIdentifierResolverStorage {
    fn storage_method(&self) -> &str {
        self.default.storage_method()
    }
}

/// Fluree runtime type returned by `connect_json_ld`.
pub type FlureeClient = Fluree<AnyStorage, AnyNameService>;

fn decode_encryption_key_base64(key_str: &str) -> Result<[u8; 32]> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(key_str)
        .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {}", e)))?;

    if decoded.len() != 32 {
        return Err(ApiError::config(format!(
            "Encryption key must be 32 bytes, got {} bytes",
            decoded.len()
        )));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&decoded);
    Ok(key)
}

/// Build an S3 storage instance from a StorageConfig.
///
/// Returns an `Arc<dyn Storage>` which may be either:
/// - `S3Storage` directly (if no encryption key is configured)
/// - `EncryptedStorage<S3Storage, ...>` (if `aes256_key` is set)
#[cfg(feature = "aws")]
async fn build_s3_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;
    use fluree_db_storage_aws::{S3Config as RawS3Config, S3Storage};

    let StorageType::S3(s3_config) = &storage_config.storage_type else {
        return Err(ApiError::config("Expected S3 storage config"));
    };

    let sdk_config = fluree_db_connection::aws::get_or_init_sdk_config()
        .await
        .map_err(|e| ApiError::config(format!("Failed to get AWS SDK config: {}", e)))?;

    let raw_config = RawS3Config {
        bucket: s3_config.bucket.to_string(),
        prefix: s3_config.prefix.as_ref().map(|s| s.to_string()),
        endpoint: s3_config.endpoint.as_ref().map(|s| s.to_string()),
        // Consolidate per-op timeouts to a single SDK operation timeout.
        // Use the maximum to avoid unexpectedly shortening slower operations.
        timeout_ms: {
            let mut max_ms: Option<u64> = None;
            for ms in [
                s3_config.read_timeout_ms,
                s3_config.write_timeout_ms,
                s3_config.list_timeout_ms,
            ]
            .into_iter()
            .flatten()
            {
                max_ms = Some(max_ms.map(|cur| cur.max(ms)).unwrap_or(ms));
            }
            max_ms
        },
        max_retries: s3_config.max_retries.map(|n| n as u32),
        retry_base_delay_ms: s3_config.retry_base_delay_ms,
        retry_max_delay_ms: s3_config.retry_max_delay_ms,
    };

    let storage = S3Storage::new(sdk_config, raw_config)
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {}", e)))?;

    // Wrap with encryption if key is configured
    if let Some(key_str) = storage_config.aes256_key.as_ref() {
        let key = decode_encryption_key_base64(key_str.as_ref())?;
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        Ok(Arc::new(EncryptedStorage::new(storage, key_provider)))
    } else {
        Ok(Arc::new(storage))
    }
}

/// Build a local (memory/file) storage instance from a StorageConfig.
#[cfg(feature = "native")]
fn build_local_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;

    match &storage_config.storage_type {
        StorageType::Memory => Ok(Arc::new(MemoryStorage::new())),
        StorageType::File => {
            let path = storage_config
                .path
                .as_ref()
                .ok_or_else(|| ApiError::config("File storage requires filePath"))?;
            let storage = FileStorage::new(path.as_ref());
            if let Some(key_str) = storage_config.aes256_key.as_ref() {
                let key = decode_encryption_key_base64(key_str.as_ref())?;
                let encryption_key = EncryptionKey::new(key, 0);
                let key_provider = StaticKeyProvider::new(encryption_key);
                Ok(Arc::new(EncryptedStorage::new(storage, key_provider)))
            } else {
                Ok(Arc::new(storage))
            }
        }
        StorageType::S3(_) => Err(ApiError::config(
            "S3 storage in addressIdentifiers is only supported with 'aws' feature",
        )),
        StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
            "Unsupported storage type in addressIdentifiers: {}",
            type_iri
        ))),
    }
}

/// Build a memory storage instance from a StorageConfig (non-native fallback).
#[cfg(not(feature = "native"))]
fn build_local_storage_from_config(
    storage_config: &fluree_db_connection::config::StorageConfig,
) -> Result<Arc<dyn Storage>> {
    use fluree_db_connection::config::StorageType;

    match &storage_config.storage_type {
        StorageType::Memory => Ok(Arc::new(MemoryStorage::new())),
        StorageType::File => Err(ApiError::config(
            "File storage in addressIdentifiers requires 'native' feature",
        )),
        StorageType::S3(_) => Err(ApiError::config(
            "S3 storage in addressIdentifiers requires 'aws' feature",
        )),
        StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
            "Unsupported storage type in addressIdentifiers: {}",
            type_iri
        ))),
    }
}

/// Check if background indexing is enabled in the parsed connection config.
fn is_indexing_enabled(config: &ConnectionConfig) -> bool {
    config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.indexing_enabled)
        .unwrap_or(false)
}

/// Build IndexerConfig from connection defaults, falling back to defaults.
fn build_indexer_config(config: &ConnectionConfig) -> fluree_db_indexer::IndexerConfig {
    let mut indexer_config = fluree_db_indexer::IndexerConfig::default();

    // Apply gc_max_old_indexes from config if present
    if let Some(max_old) = config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.max_old_indexes)
    {
        indexer_config.gc_max_old_indexes = max_old.min(u32::MAX as u64) as u32;
    }

    // Apply gc_min_time_mins from config if present
    if let Some(min_time) = config
        .defaults
        .as_ref()
        .and_then(|d| d.indexing.as_ref())
        .and_then(|i| i.gc_min_time_mins)
    {
        indexer_config.gc_min_time_mins = min_time.min(u32::MAX as u64) as u32;
    }

    indexer_config
}

/// Derive `IndexConfig` from `ConnectionConfig` defaults (or fall back to compiled defaults).
///
/// Used by `connect_json_ld` paths where indexing thresholds come from the JSON-LD
/// connection config rather than the `FlureeBuilder`.
fn derive_index_config(config: &ConnectionConfig) -> IndexConfig {
    let indexing = config.defaults.as_ref().and_then(|d| d.indexing.as_ref());

    IndexConfig {
        reindex_min_bytes: indexing
            .and_then(|i| i.reindex_min_bytes)
            .map(|v| v as usize)
            .unwrap_or(IndexConfig::default().reindex_min_bytes),
        reindex_max_bytes: indexing
            .and_then(|i| i.reindex_max_bytes)
            .map(|v| v as usize)
            .unwrap_or(IndexConfig::default().reindex_max_bytes),
    }
}

/// Start background indexing if enabled in the config.
///
/// Creates a BackgroundIndexerWorker and spawns it on the tokio runtime.
/// Returns the appropriate IndexingMode (Background with handle, or Disabled).
fn start_background_indexing_if_enabled(
    config: &ConnectionConfig,
    storage: AnyStorage,
    nameservice: AnyNameService,
) -> tx::IndexingMode {
    if !is_indexing_enabled(config) {
        return tx::IndexingMode::Disabled;
    }

    let indexer_config = build_indexer_config(config);
    let (worker, handle) = fluree_db_indexer::BackgroundIndexerWorker::new(
        storage,
        Arc::new(nameservice),
        indexer_config,
    );

    // Spawn the worker on the tokio runtime
    tokio::spawn(worker.run());

    tx::IndexingMode::Background(handle)
}

/// Connect using the Clojure-style JSON-LD connection config.
///
/// This is the **single source of truth** entrypoint: all convenience helpers
/// should generate JSON-LD and call this.
pub async fn connect_json_ld(config: &serde_json::Value) -> Result<FlureeClient> {
    let parsed = ConnectionConfig::from_json_ld(config)
        .map_err(|e| ApiError::config(format!("Invalid JSON-LD config: {}", e)))?;

    // --- S3 / AWS path (async-only) ---
    #[cfg(feature = "aws")]
    if matches!(parsed.index_storage.storage_type, StorageType::S3(_)) {
        let handle = fluree_db_connection::connect_async(config).await?;
        let fluree_db_connection::ConnectionHandle::Aws(aws_handle) = handle else {
            return Err(ApiError::config(
                "Expected AWS connection handle for S3 config",
            ));
        };

        // Decide whether to use tiered commit/index routing.
        let index = aws_handle.index_storage().clone();
        let commit = aws_handle.commit_storage().clone();
        let base_storage: Arc<dyn Storage> =
            if index.bucket() != commit.bucket() || index.prefix() != commit.prefix() {
                Arc::new(TieredStorage::new(commit, index))
            } else {
                Arc::new(index)
            };

        // Build address identifier resolver if configured
        let storage: AnyStorage = if let Some(addr_ids) = &aws_handle.config().address_identifiers {
            let mut identifier_map = std::collections::HashMap::new();
            for (identifier, storage_config) in addr_ids.iter() {
                let id_storage: Arc<dyn Storage> = match &storage_config.storage_type {
                    StorageType::S3(_) => build_s3_storage_from_config(storage_config).await?,
                    _ => build_local_storage_from_config(storage_config)?,
                };
                identifier_map.insert(identifier.to_string(), AnyStorage::new(id_storage));
            }
            AnyStorage::new(Arc::new(AddressIdentifierResolverStorage::new(
                AnyStorage::new(base_storage),
                identifier_map,
            )))
        } else {
            AnyStorage::new(base_storage)
        };

        let connection = Connection::new(aws_handle.config().clone(), storage);

        let nameservice_inner = aws_handle.nameservice().clone();
        let nameservice_wrapped = DelegatingNameService::new(nameservice_inner);
        let nameservice = AnyNameService::new(Arc::new(nameservice_wrapped));

        // Start background indexing if enabled in config
        let indexing_mode = start_background_indexing_if_enabled(
            aws_handle.config(),
            connection.storage().clone(),
            nameservice.clone(),
        );

        let index_config = derive_index_config(aws_handle.config());

        return Ok(Fluree {
            connection,
            nameservice,
            indexing_mode,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        });
    }

    // --- Local (memory/filesystem) ---
    match &parsed.index_storage.storage_type {
        StorageType::Memory => {
            let base_storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());

            // Build address identifier resolver if configured
            let storage: AnyStorage = if let Some(addr_ids) = &parsed.address_identifiers {
                let mut identifier_map = std::collections::HashMap::new();
                for (identifier, storage_config) in addr_ids.iter() {
                    let id_storage = build_local_storage_from_config(storage_config)?;
                    identifier_map.insert(identifier.to_string(), AnyStorage::new(id_storage));
                }
                AnyStorage::new(Arc::new(AddressIdentifierResolverStorage::new(
                    AnyStorage::new(base_storage),
                    identifier_map,
                )))
            } else {
                AnyStorage::new(base_storage)
            };

            let connection = Connection::new(parsed, storage);
            let nameservice_inner = MemoryNameService::new();
            let nameservice =
                AnyNameService::new(Arc::new(DelegatingNameService::new(nameservice_inner)));

            // Start background indexing if enabled in config
            let index_config = derive_index_config(connection.config());
            let indexing_mode = start_background_indexing_if_enabled(
                connection.config(),
                connection.storage().clone(),
                nameservice.clone(),
            );

            Ok(Fluree {
                connection,
                nameservice,
                indexing_mode,
                index_config,
                r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
                ledger_manager: None,
            })
        }
        StorageType::File => {
            #[cfg(not(feature = "native"))]
            {
                Err(ApiError::config(
                    "Filesystem storage requires the 'native' feature",
                ))
            }
            #[cfg(feature = "native")]
            {
                let path = parsed
                    .index_storage
                    .path
                    .clone()
                    .ok_or_else(|| ApiError::config("File storage requires filePath"))?;
                let file_storage = FileStorage::new(path.as_ref());
                let base_storage: Arc<dyn Storage> =
                    if let Some(key_str) = parsed.index_storage.aes256_key.as_ref() {
                        // Note: This encrypts storage reads/writes (index/commit blobs). The file-based
                        // nameservice remains plaintext, matching the existing builder behavior.
                        let key = decode_encryption_key_base64(key_str.as_ref())?;
                        let encryption_key = EncryptionKey::new(key, 0);
                        let key_provider = StaticKeyProvider::new(encryption_key);
                        Arc::new(EncryptedStorage::new(file_storage, key_provider))
                    } else {
                        Arc::new(file_storage)
                    };

                // Build address identifier resolver if configured
                let storage: AnyStorage = if let Some(addr_ids) = &parsed.address_identifiers {
                    let mut identifier_map = std::collections::HashMap::new();
                    for (identifier, storage_config) in addr_ids.iter() {
                        let id_storage = build_local_storage_from_config(storage_config)?;
                        identifier_map.insert(identifier.to_string(), AnyStorage::new(id_storage));
                    }
                    AnyStorage::new(Arc::new(AddressIdentifierResolverStorage::new(
                        AnyStorage::new(base_storage),
                        identifier_map,
                    )))
                } else {
                    AnyStorage::new(base_storage)
                };

                let connection = Connection::new(parsed, storage);
                let nameservice_inner = FileNameService::new(path.as_ref());
                let nameservice =
                    AnyNameService::new(Arc::new(DelegatingNameService::new(nameservice_inner)));

                // Start background indexing if enabled in config
                let index_config = derive_index_config(connection.config());
                let indexing_mode = start_background_indexing_if_enabled(
                    connection.config(),
                    connection.storage().clone(),
                    nameservice.clone(),
                );

                Ok(Fluree {
                    connection,
                    nameservice,
                    indexing_mode,
                    index_config,
                    r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
                    ledger_manager: None,
                })
            }
        }
        StorageType::S3(_) => Err(ApiError::config(
            "S3 storage requires the 'aws' feature on fluree-db-api",
        )),
        StorageType::Unsupported { type_iri, .. } => Err(ApiError::config(format!(
            "Unsupported storage type in JSON-LD config: {}",
            type_iri
        ))),
    }
}

/// Convenience helper: connect with in-memory storage.
///
/// This just generates JSON-LD and delegates to `connect_json_ld`.
pub async fn connect_memory() -> Result<FlureeClient> {
    let config = serde_json::json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "memoryStorage", "@type": "Storage"},
            {"@id": "connection",
             "@type": "Connection",
             "commitStorage": {"@id": "memoryStorage"},
             "indexStorage": {"@id": "memoryStorage"},
             "primaryPublisher": {"@type": "Publisher", "storage": {"@id": "memoryStorage"}}}
        ]
    });
    connect_json_ld(&config).await
}

/// Convenience helper: connect with filesystem storage.
///
/// This just generates JSON-LD and delegates to `connect_json_ld`.
#[cfg(feature = "native")]
pub async fn connect_filesystem(path: impl AsRef<str>) -> Result<FlureeClient> {
    let config = serde_json::json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fileStorage", "@type": "Storage", "filePath": path.as_ref()},
            {"@id": "connection",
             "@type": "Connection",
             "commitStorage": {"@id": "fileStorage"},
             "indexStorage": {"@id": "fileStorage"},
             "primaryPublisher": {"@type": "Publisher", "storage": {"@id": "fileStorage"}}}
        ]
    });
    connect_json_ld(&config).await
}

/// Convenience helper: connect with S3 storage (Clojure `connect-s3` parity).
///
/// This just generates JSON-LD and delegates to `connect_json_ld`.
///
/// Notes:
/// - Requires the `aws` feature on `fluree-db-api`.
/// - For LocalStack/MinIO/custom endpoints, `endpoint` should be provided.
/// - For S3 Express directory buckets (`--x-s3` suffix), the helper **ignores** the
///   provided endpoint and omits `s3Endpoint` in the generated JSON-LD to avoid
///   signature/endpoint issues.
#[cfg(feature = "aws")]
pub async fn connect_s3(
    bucket: impl AsRef<str>,
    endpoint: impl AsRef<str>,
) -> Result<FlureeClient> {
    let bucket = bucket.as_ref();
    // S3 Express directory buckets should not be configured with a manual endpoint.
    // The AWS SDK handles directory bucket endpoints and session auth automatically.
    let is_express = fluree_db_storage_aws::S3Storage::is_express_bucket(bucket);
    let storage_node = if is_express {
        serde_json::json!({"@id": "s3Storage", "@type": "Storage", "s3Bucket": bucket})
    } else {
        serde_json::json!({"@id": "s3Storage", "@type": "Storage", "s3Bucket": bucket, "s3Endpoint": endpoint.as_ref()})
    };

    let config = serde_json::json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            storage_node,
            {"@id": "connection",
             "@type": "Connection",
             "commitStorage": {"@id": "s3Storage"},
             "indexStorage": {"@id": "s3Storage"},
             "primaryPublisher": {"@type": "Publisher", "storage": {"@id": "s3Storage"}}}
        ]
    });
    connect_json_ld(&config).await
}

/// Builder for creating Fluree instances
///
/// Provides a fluent API for configuring storage, cache, and nameservice options.
#[derive(Debug, Clone, Default)]
pub struct FlureeBuilder {
    config: ConnectionConfig,
    #[cfg(feature = "native")]
    storage_path: Option<String>,
    /// Optional encryption key (base64-encoded or raw 32 bytes)
    encryption_key: Option<[u8; 32]>,
    /// Optional ledger cache configuration (enables LedgerManager)
    ledger_cache_config: Option<LedgerManagerConfig>,
    /// Optional background indexing configuration.
    /// When set, `build()` will spawn a `BackgroundIndexerWorker`.
    indexing_config: Option<IndexingBuilderConfig>,
}

/// Configuration for background indexing in `FlureeBuilder`.
#[derive(Debug, Clone)]
pub struct IndexingBuilderConfig {
    /// Controls index building parameters (leaf sizes, GC, memory budget).
    pub indexer_config: IndexerConfig,
    /// Controls novelty backpressure thresholds.
    pub index_config: IndexConfig,
}

impl FlureeBuilder {
    /// Create a new builder with default settings (memory storage)
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure for file-based storage
    ///
    /// The path should be the root directory containing ledger data.
    #[cfg(feature = "native")]
    pub fn file(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            config: ConnectionConfig::file(path.clone()),
            storage_path: Some(path),
            encryption_key: None,
            ledger_cache_config: None,
            indexing_config: None,
        }
    }

    /// Configure for memory-based storage
    pub fn memory() -> Self {
        Self {
            config: ConnectionConfig::memory(),
            #[cfg(feature = "native")]
            storage_path: None,
            encryption_key: None,
            ledger_cache_config: None,
            indexing_config: None,
        }
    }

    /// Configure for S3-backed storage (Clojure `connect-s3` parity).
    ///
    /// This sets **index storage** to S3 and configures a **storage-backed nameservice**
    /// using the same S3 storage.
    ///
    /// Notes:
    /// - Requires the `aws` feature on `fluree-db-api`.
    /// - Region/credentials are resolved via the standard AWS SDK chain.
    /// - `endpoint` is required for parity (LocalStack/MinIO/AWS endpoints).
    #[cfg(feature = "aws")]
    pub fn s3(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        use fluree_db_connection::config::{PublisherConfig, PublisherType, S3StorageConfig};
        use fluree_db_connection::StorageConfig;

        let bucket = bucket.into();
        let endpoint = endpoint.into();

        let storage_id: Arc<str> = Arc::from("s3Storage");
        let s3 = S3StorageConfig {
            bucket: Arc::from(bucket),
            prefix: None,
            endpoint: Some(Arc::from(endpoint)),
            read_timeout_ms: None,
            write_timeout_ms: None,
            list_timeout_ms: None,
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            address_identifier: None,
        };

        let storage = StorageConfig {
            id: Some(storage_id.clone()),
            storage_type: StorageType::S3(s3),
            path: None,
            aes256_key: None,
            address_identifier: None,
        };

        let publisher = PublisherConfig {
            id: Some(Arc::from("primaryPublisher")),
            publisher_type: PublisherType::Storage {
                storage: storage.clone(),
            },
        };

        let config = ConnectionConfig {
            index_storage: storage,
            commit_storage: None,
            primary_publisher: Some(publisher),
            ..Default::default()
        };

        Self {
            config,
            #[cfg(feature = "native")]
            storage_path: None,
            encryption_key: None,
            ledger_cache_config: None,
            indexing_config: None,
        }
    }

    /// Set S3 key prefix (e.g. `"ledgers/prod"`).
    #[cfg(feature = "aws")]
    pub fn s3_prefix(mut self, prefix: impl Into<String>) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.prefix = Some(Arc::from(prefix.into()));
        }
        self
    }

    /// Set S3 read timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_read_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.read_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 write timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_write_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.write_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 list timeout in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_list_timeout_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.list_timeout_ms = Some(ms);
        }
        self
    }

    /// Set S3 max retries (retries after the initial attempt).
    #[cfg(feature = "aws")]
    pub fn s3_max_retries(mut self, n: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.max_retries = Some(n);
        }
        self
    }

    /// Set S3 retry base delay in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_retry_base_delay_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.retry_base_delay_ms = Some(ms);
        }
        self
    }

    /// Set S3 retry max delay in milliseconds.
    #[cfg(feature = "aws")]
    pub fn s3_retry_max_delay_ms(mut self, ms: u64) -> Self {
        if let StorageType::S3(s3) = &mut self.config.index_storage.storage_type {
            s3.retry_max_delay_ms = Some(ms);
        }
        self
    }

    /// Set the encryption key for storage encryption.
    ///
    /// When set, all data will be encrypted using AES-256-GCM before being
    /// written to storage. The key must be exactly 32 bytes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = [0u8; 32]; // Use a secure key in production
    /// let fluree = FlureeBuilder::file("/data")
    ///     .with_encryption_key(key)
    ///     .build_encrypted()?;
    /// ```
    pub fn with_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(key);
        self
    }

    /// Set the encryption key from a base64-encoded string.
    ///
    /// This is useful when loading keys from environment variables or config files.
    /// The decoded key must be exactly 32 bytes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let fluree = FlureeBuilder::file("/data")
    ///     .with_encryption_key_base64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")?
    ///     .build_encrypted()?;
    /// ```
    pub fn with_encryption_key_base64(mut self, base64_key: &str) -> Result<Self> {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(base64_key)
            .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {}", e)))?;

        if decoded.len() != 32 {
            return Err(ApiError::config(format!(
                "Encryption key must be 32 bytes, got {} bytes",
                decoded.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        self.encryption_key = Some(key);
        Ok(self)
    }

    /// Create a builder from JSON-LD configuration.
    ///
    /// Parses a JSON-LD configuration document and extracts all settings including
    /// storage path, cache settings, and encryption key.
    ///
    /// # Encryption Key from Environment
    ///
    /// The encryption key can be specified directly or via environment variable:
    ///
    /// ```json
    /// {
    ///   "@context": {"@vocab": "https://ns.flur.ee/system#"},
    ///   "@graph": [{
    ///     "@type": "Connection",
    ///     "indexStorage": {
    ///       "@type": "Storage",
    ///       "filePath": "/data/fluree",
    ///       "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    ///     }
    ///   }]
    /// }
    /// ```
    ///
    /// The key should be base64-encoded, 32 bytes when decoded.
    #[cfg(feature = "native")]
    pub fn from_json_ld(json: &serde_json::Value) -> Result<Self> {
        let config = ConnectionConfig::from_json_ld(json)
            .map_err(|e| ApiError::config(format!("Invalid JSON-LD config: {}", e)))?;

        // Extract path and encryption key from storage config
        let storage_path = config.index_storage.path.as_ref().map(|p| p.to_string());

        let encryption_key = if let Some(key_str) = &config.index_storage.aes256_key {
            Some(Self::decode_encryption_key(key_str)?)
        } else {
            None
        };

        Ok(Self {
            config,
            storage_path,
            encryption_key,
            ledger_cache_config: None,
            indexing_config: None,
        })
    }

    /// Decode a base64-encoded encryption key to 32 bytes.
    fn decode_encryption_key(key_str: &str) -> Result<[u8; 32]> {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(key_str)
            .map_err(|e| ApiError::config(format!("Invalid base64 encryption key: {}", e)))?;

        if decoded.len() != 32 {
            return Err(ApiError::config(format!(
                "Encryption key must be 32 bytes, got {} bytes",
                decoded.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        Ok(key)
    }

    /// Set the maximum cache entries directly.
    ///
    /// By default, cache size is calculated automatically based on 50% of
    /// available system memory. Use this method to override with a specific count.
    pub fn cache_max_entries(mut self, max_entries: usize) -> Self {
        self.config.cache = fluree_db_connection::CacheConfig::with_max_entries(max_entries);
        self
    }

    /// Set the maximum cache size in MB.
    ///
    /// By default, cache size is calculated automatically based on 50% of
    /// available system memory. Use this method to override with a specific MB limit.
    pub fn cache_max_mb(mut self, max_mb: usize) -> Self {
        self.config.cache = fluree_db_connection::CacheConfig::with_max_mb(max_mb);
        self
    }

    /// Set the parallelism level
    pub fn parallelism(mut self, parallelism: usize) -> Self {
        self.config.parallelism = parallelism;
        self
    }

    /// Enable ledger caching with default configuration
    ///
    /// When enabled, loaded ledgers are cached at the connection level,
    /// avoiding per-request ledger reloading. This significantly improves
    /// performance for both queries and transactions.
    ///
    /// Default configuration:
    /// - Idle TTL: 30 minutes
    /// - Sweep interval: 1 minute
    pub fn with_ledger_caching(mut self) -> Self {
        self.ledger_cache_config = Some(LedgerManagerConfig::default());
        self
    }

    /// Enable ledger caching with custom configuration
    ///
    /// Allows fine-tuning the caching behavior:
    /// - `idle_ttl`: How long a ledger stays cached after last access
    /// - `sweep_interval`: How often the cache is checked for idle entries
    pub fn with_ledger_cache_config(mut self, config: LedgerManagerConfig) -> Self {
        self.ledger_cache_config = Some(config);
        self
    }

    /// Enable background indexing with default settings.
    ///
    /// When enabled, `build()` will spawn a `BackgroundIndexerWorker` that
    /// automatically indexes ledgers when novelty exceeds the soft threshold.
    /// Must be called within a tokio runtime context.
    pub fn with_indexing(mut self) -> Self {
        self.indexing_config = Some(IndexingBuilderConfig {
            indexer_config: IndexerConfig::default(),
            index_config: IndexConfig::default(),
        });
        self
    }

    /// Enable background indexing with custom novelty thresholds.
    ///
    /// - `min_bytes`: soft threshold — triggers background indexing
    /// - `max_bytes`: hard threshold — blocks commits until indexed
    pub fn with_indexing_thresholds(mut self, min_bytes: usize, max_bytes: usize) -> Self {
        let index_config = IndexConfig {
            reindex_min_bytes: min_bytes,
            reindex_max_bytes: max_bytes,
        };
        self.indexing_config = Some(IndexingBuilderConfig {
            indexer_config: self
                .indexing_config
                .map(|c| c.indexer_config)
                .unwrap_or_default(),
            index_config,
        });
        self
    }

    /// Build a file-backed Fluree instance
    ///
    /// Returns an error if storage_path is not set.
    /// Indexing is disabled by default; call `with_indexing()` before `build()`
    /// to enable background indexing, or use `set_indexing_mode` after building.
    ///
    /// When indexing is enabled via `with_indexing()`, a `BackgroundIndexerWorker`
    /// is spawned on the tokio runtime. This must be called within a tokio context.
    #[cfg(feature = "native")]
    pub fn build(self) -> Result<Fluree<FileStorage, FileNameService>> {
        let path = self
            .storage_path
            .ok_or_else(|| ApiError::config("File storage requires a path"))?;

        let storage = FileStorage::new(&path);
        let connection = Connection::new(self.config, storage.clone());

        let nameservice = FileNameService::new(&path);

        // Create LedgerManager if caching is enabled
        let ledger_manager = self.ledger_cache_config.map(|config| {
            Arc::new(LedgerManager::new(
                storage.clone(),
                nameservice.clone(),
                config,
            ))
        });

        // Start background indexing if configured
        let (indexing_mode, index_config) = if let Some(idx_config) = self.indexing_config {
            let (worker, handle) = BackgroundIndexerWorker::new(
                storage,
                Arc::new(nameservice.clone()),
                idx_config.indexer_config,
            );
            tokio::spawn(worker.run());
            (
                tx::IndexingMode::Background(handle),
                idx_config.index_config,
            )
        } else {
            (tx::IndexingMode::Disabled, IndexConfig::default())
        };

        Ok(Fluree {
            connection,
            nameservice,
            indexing_mode,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager,
        })
    }

    /// Build a file-backed Fluree instance with AES-256-GCM encryption.
    ///
    /// Uses the provided `key` argument for encryption. Any key previously set on the
    /// builder via `with_encryption_key()` or JSON-LD config is ignored.
    ///
    /// To use a key configured on the builder, use [`build_encrypted_from_config()`] instead.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = [0u8; 32]; // Use a secure key in production
    /// let fluree = FlureeBuilder::file("/path/to/data")
    ///     .build_encrypted(key)?;
    /// ```
    ///
    /// # Security
    ///
    /// - Key material stored in `EncryptionKey` is zeroized on drop
    /// - Uses AES-256-GCM (authenticated encryption with integrity protection)
    /// - Each write uses a fresh random nonce
    /// - Encrypted data is portable between storage backends
    ///
    /// Note: The input `[u8; 32]` passed to this method is not automatically zeroized;
    /// callers should zeroize their own key copies if needed.
    #[cfg(feature = "native")]
    pub fn build_encrypted(
        self,
        key: [u8; 32],
    ) -> Result<Fluree<EncryptedStorage<FileStorage, StaticKeyProvider>, FileNameService>> {
        // Always use the explicitly provided key
        self.build_encrypted_internal(key)
    }

    /// Build a file-backed Fluree instance with encryption using the configured key.
    ///
    /// The encryption key must have been set via `with_encryption_key()`,
    /// `with_encryption_key_base64()`, or parsed from JSON-LD config.
    ///
    /// # Errors
    ///
    /// Returns an error if no encryption key has been configured.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // From JSON-LD config with environment variable
    /// let config = json!({
    ///     "@context": {"@vocab": "https://ns.flur.ee/system#"},
    ///     "@graph": [{
    ///         "@type": "Connection",
    ///         "indexStorage": {
    ///             "@type": "Storage",
    ///             "filePath": "/data/fluree",
    ///             "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    ///         }
    ///     }]
    /// });
    /// let fluree = FlureeBuilder::from_json_ld(&config)?
    ///     .build_encrypted_from_config()?;
    /// ```
    #[cfg(feature = "native")]
    pub fn build_encrypted_from_config(
        self,
    ) -> Result<Fluree<EncryptedStorage<FileStorage, StaticKeyProvider>, FileNameService>> {
        let key = self.encryption_key.ok_or_else(|| {
            ApiError::config("No encryption key configured. Set via with_encryption_key(), with_encryption_key_base64(), or AES256Key in JSON-LD config")
        })?;
        self.build_encrypted_internal(key)
    }

    /// Internal helper to build encrypted storage
    #[cfg(feature = "native")]
    fn build_encrypted_internal(
        self,
        key: [u8; 32],
    ) -> Result<Fluree<EncryptedStorage<FileStorage, StaticKeyProvider>, FileNameService>> {
        let path = self
            .storage_path
            .ok_or_else(|| ApiError::config("File storage requires a path"))?;

        let file_storage = FileStorage::new(&path);
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(file_storage, key_provider);

        let connection = Connection::new(self.config, storage);

        let nameservice = FileNameService::new(&path);

        let index_config = self
            .indexing_config
            .map(|c| c.index_config)
            .unwrap_or_default();

        // Note: Ledger caching not yet supported for encrypted storage
        Ok(Fluree {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        })
    }

    /// Check if this builder has an encryption key configured.
    pub fn has_encryption_key(&self) -> bool {
        self.encryption_key.is_some()
    }

    /// Build a memory-backed Fluree instance
    ///
    /// Indexing is disabled by default; use `set_indexing_mode` after building
    /// to enable background indexing.
    pub fn build_memory(self) -> Fluree<MemoryStorage, MemoryNameService> {
        let storage = MemoryStorage::new();
        let connection = Connection::new(self.config, storage.clone());
        let nameservice = MemoryNameService::new();

        // Create LedgerManager if caching is enabled
        let ledger_manager = self
            .ledger_cache_config
            .map(|config| Arc::new(LedgerManager::new(storage, nameservice.clone(), config)));

        let index_config = self
            .indexing_config
            .map(|c| c.index_config)
            .unwrap_or_default();

        Fluree {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager,
        }
    }

    /// Build a memory-backed Fluree instance with AES-256-GCM encryption
    ///
    /// Useful for testing encryption without touching the filesystem.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    pub fn build_memory_encrypted(
        self,
        key: [u8; 32],
    ) -> Fluree<EncryptedStorage<MemoryStorage, StaticKeyProvider>, MemoryNameService> {
        let mem_storage = MemoryStorage::new();
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(mem_storage, key_provider);

        let connection = Connection::new(self.config, storage);
        let nameservice = MemoryNameService::new();

        let index_config = self
            .indexing_config
            .map(|c| c.index_config)
            .unwrap_or_default();

        // Note: Ledger caching not yet supported for encrypted storage
        Fluree {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        }
    }

    /// Build an S3-backed Fluree instance (storage-backed nameservice).
    ///
    /// This mirrors Clojure's `connect-s3` convenience behavior.
    ///
    /// Notes:
    /// - Requires the `aws` feature.
    /// - Uses the AWS default credential/region chain.
    /// - Ledger caching is currently not enabled for this builder path.
    #[cfg(feature = "aws")]
    pub async fn build_s3(
        self,
    ) -> Result<
        Fluree<
            fluree_db_storage_aws::S3Storage,
            StorageNameService<fluree_db_storage_aws::S3Storage>,
        >,
    > {
        use fluree_db_connection::aws;
        use fluree_db_connection::config::S3StorageConfig;
        use fluree_db_storage_aws::{S3Config, S3Storage};

        let s3_cfg: &S3StorageConfig = match &self.config.index_storage.storage_type {
            StorageType::S3(s3) => s3,
            _ => {
                return Err(ApiError::config(
                    "build_s3 requires FlureeBuilder::s3(...) or an S3 indexStorage config",
                ))
            }
        };

        let timeout_ms = s3_cfg
            .read_timeout_ms
            .into_iter()
            .chain(s3_cfg.write_timeout_ms.into_iter())
            .chain(s3_cfg.list_timeout_ms.into_iter())
            .max();

        let sdk_config = aws::get_or_init_sdk_config().await?;

        let storage = S3Storage::new(
            sdk_config,
            S3Config {
                bucket: s3_cfg.bucket.to_string(),
                prefix: s3_cfg.prefix.as_ref().map(|s| s.to_string()),
                endpoint: s3_cfg.endpoint.as_ref().map(|s| s.to_string()),
                timeout_ms,
                max_retries: s3_cfg.max_retries.map(|n| n as u32),
                retry_base_delay_ms: s3_cfg.retry_base_delay_ms,
                retry_max_delay_ms: s3_cfg.retry_max_delay_ms,
            },
        )
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {}", e)))?;

        let connection = Connection::new(self.config, storage.clone());

        // Empty prefix: S3Storage already applies its own key prefix.
        let nameservice = StorageNameService::new(storage, "");

        let index_config = self
            .indexing_config
            .map(|c| c.index_config)
            .unwrap_or_default();

        Ok(Fluree {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        })
    }

    /// Build an S3-backed Fluree instance with AES-256-GCM encryption.
    ///
    /// All data written to S3 is transparently encrypted before upload,
    /// and decrypted on read.
    ///
    /// Notes:
    /// - Requires the `aws` feature.
    /// - Uses the AWS default credential/region chain.
    /// - Ledger caching is currently not enabled for this builder path.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte AES-256 encryption key
    #[cfg(feature = "aws")]
    pub async fn build_s3_encrypted(
        self,
        key: [u8; 32],
    ) -> Result<
        Fluree<
            EncryptedStorage<fluree_db_storage_aws::S3Storage, StaticKeyProvider>,
            StorageNameService<
                EncryptedStorage<fluree_db_storage_aws::S3Storage, StaticKeyProvider>,
            >,
        >,
    > {
        use fluree_db_connection::aws;
        use fluree_db_connection::config::S3StorageConfig;
        use fluree_db_storage_aws::{S3Config, S3Storage};

        let s3_cfg: &S3StorageConfig = match &self.config.index_storage.storage_type {
            StorageType::S3(s3) => s3,
            _ => return Err(ApiError::config(
                "build_s3_encrypted requires FlureeBuilder::s3(...) or an S3 indexStorage config",
            )),
        };

        let timeout_ms = s3_cfg
            .read_timeout_ms
            .into_iter()
            .chain(s3_cfg.write_timeout_ms.into_iter())
            .chain(s3_cfg.list_timeout_ms.into_iter())
            .max();

        let sdk_config = aws::get_or_init_sdk_config().await?;

        let s3_storage = S3Storage::new(
            sdk_config,
            S3Config {
                bucket: s3_cfg.bucket.to_string(),
                prefix: s3_cfg.prefix.as_ref().map(|s| s.to_string()),
                endpoint: s3_cfg.endpoint.as_ref().map(|s| s.to_string()),
                timeout_ms,
                max_retries: s3_cfg.max_retries.map(|n| n as u32),
                retry_base_delay_ms: s3_cfg.retry_base_delay_ms,
                retry_max_delay_ms: s3_cfg.retry_max_delay_ms,
            },
        )
        .await
        .map_err(|e| ApiError::config(format!("Failed to create S3 storage: {}", e)))?;

        // Wrap with encryption
        let encryption_key = EncryptionKey::new(key, 0);
        let key_provider = StaticKeyProvider::new(encryption_key);
        let storage = EncryptedStorage::new(s3_storage, key_provider);

        let connection = Connection::new(self.config, storage.clone());

        // Empty prefix: S3Storage already applies its own key prefix.
        let nameservice = StorageNameService::new(storage, "");

        let index_config = self
            .indexing_config
            .map(|c| c.index_config)
            .unwrap_or_default();

        Ok(Fluree {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config,
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        })
    }
}

/// Main Fluree API entry point
///
/// Combines connection management, nameservice, and query execution
/// into a unified interface.
///
/// Type parameters:
/// - `S`: Storage implementation (FileStorage or MemoryStorage)
/// - `N`: NameService implementation
pub struct Fluree<S: Storage + 'static, N> {
    /// Connection for database operations (includes storage)
    connection: Connection<S>,
    /// Nameservice for ledger discovery
    nameservice: N,
    /// Indexing mode (disabled or background with handle)
    pub indexing_mode: tx::IndexingMode,
    /// Novelty backpressure thresholds used by commits and soft-trigger logic.
    ///
    /// Set from `FlureeBuilder::with_indexing_thresholds()` for builder paths,
    /// or derived from `ConnectionConfig::defaults.indexing` for JSON-LD paths.
    index_config: IndexConfig,
    /// R2RML cache for compiled mappings and table metadata
    r2rml_cache: std::sync::Arc<graph_source::R2rmlCache>,
    /// Optional ledger manager for connection-level caching
    ///
    /// When enabled via `FlureeBuilder::with_ledger_caching()`, loaded ledgers
    /// are cached for reuse across queries and transactions.
    ledger_manager: Option<Arc<LedgerManager<S, N>>>,
}

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// Create a new Fluree instance with custom components
    ///
    /// Most users should use `FlureeBuilder` instead.
    pub fn new(connection: Connection<S>, nameservice: N) -> Self {
        Self {
            connection,
            nameservice,
            indexing_mode: tx::IndexingMode::Disabled,
            index_config: IndexConfig::default(),
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        }
    }

    /// Create a new Fluree instance with a specific indexing mode
    pub fn with_indexing_mode(
        connection: Connection<S>,
        nameservice: N,
        indexing_mode: tx::IndexingMode,
    ) -> Self {
        Self {
            connection,
            nameservice,
            indexing_mode,
            index_config: IndexConfig::default(),
            r2rml_cache: std::sync::Arc::new(graph_source::R2rmlCache::with_defaults()),
            ledger_manager: None,
        }
    }

    /// Set the indexing mode
    pub fn set_indexing_mode(&mut self, mode: tx::IndexingMode) {
        self.indexing_mode = mode;
    }

    /// Returns the novelty backpressure thresholds for this instance.
    ///
    /// Set from `FlureeBuilder::with_indexing_thresholds()` for builder paths,
    /// or derived from `ConnectionConfig::defaults.indexing` for JSON-LD paths.
    pub(crate) fn default_index_config(&self) -> IndexConfig {
        self.index_config.clone()
    }

    /// Check whether indexing is enabled in connection defaults.
    ///
    /// Defaults to `true` if not explicitly configured.
    pub(crate) fn defaults_indexing_enabled(&self) -> bool {
        self.connection
            .config()
            .defaults
            .as_ref()
            .and_then(|d| d.indexing.as_ref())
            .and_then(|i| i.indexing_enabled)
            .unwrap_or(true)
    }

    /// Get a reference to the nameservice
    pub fn nameservice(&self) -> &N {
        &self.nameservice
    }

    /// Get a reference to the connection
    pub fn connection(&self) -> &Connection<S> {
        &self.connection
    }

    /// Get a reference to the storage (via the connection)
    pub fn storage(&self) -> &S {
        self.connection.storage()
    }

    /// Get a reference to the R2RML cache
    pub fn r2rml_cache(&self) -> &std::sync::Arc<graph_source::R2rmlCache> {
        &self.r2rml_cache
    }

    /// Check if ledger caching is enabled
    ///
    /// Returns true if `with_ledger_caching()` was called on the builder.
    pub fn is_caching_enabled(&self) -> bool {
        self.ledger_manager.is_some()
    }

    /// Get the ledger manager (if caching is enabled)
    pub fn ledger_manager(&self) -> Option<&Arc<LedgerManager<S, N>>> {
        self.ledger_manager.as_ref()
    }
}

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a builder for a new ledger.
    ///
    /// Returns a [`CreateBuilder`] that supports `.import(path)` for bulk import
    /// or can be extended for other creation patterns.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Bulk import from TTL chunks
    /// let result = fluree.create("mydb")
    ///     .import("/data/chunks/")
    ///     .threads(8)
    ///     .run_budget_mb(4096)
    ///     .execute()
    ///     .await?;
    ///
    /// // Query normally after import
    /// let view = fluree.view("mydb").await?;
    /// let qr = fluree.query_view(&view, "SELECT * WHERE { ?s ?p ?o } LIMIT 10").await?;
    /// ```
    pub fn create(&self, ledger_id: &str) -> import::CreateBuilder<'_, S, N> {
        import::CreateBuilder::new(self, ledger_id.to_string())
    }

    /// Create a lazy graph handle for a ledger at the latest head.
    ///
    /// No I/O occurs until a terminal method is called (`.load()`,
    /// `.query().execute()`, `.transact().commit()`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Lazy query
    /// let result = fluree
    ///     .graph("mydb:main")
    ///     .query()
    ///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
    ///     .execute()
    ///     .await?;
    ///
    /// // Lazy transact + commit
    /// let out = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .commit()
    ///     .await?;
    ///
    /// // Materialize for reuse
    /// let db = fluree.graph("mydb:main").load().await?;
    /// ```
    pub fn graph(&self, ledger_id: &str) -> Graph<'_, S, N> {
        Graph::new(self, ledger_id.to_string(), TimeSpec::Latest)
    }

    /// Create a lazy graph handle at a specific time.
    ///
    /// Supports `TimeSpec::AtT`, `TimeSpec::AtTime` (ISO-8601),
    /// `TimeSpec::AtCommit` (SHA prefix), and `TimeSpec::Latest`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree
    ///     .graph_at("mydb:main", TimeSpec::AtT(42))
    ///     .query()
    ///     .jsonld(&q)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn graph_at(&self, ledger_id: &str, spec: TimeSpec) -> Graph<'_, S, N> {
        Graph::new(self, ledger_id.to_string(), spec)
    }
}

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a transaction builder using a cached [`LedgerHandle`].
    ///
    /// This is the recommended way to transact in server/application contexts.
    /// The handle is borrowed and its internal state is updated in-place
    /// on successful commit, ensuring concurrent readers see the update.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = fluree.ledger_cached("mydb:main").await?;
    /// let result = fluree.stage(&handle)
    ///     .insert(&data)
    ///     .execute().await?;
    /// ```
    pub fn stage<'a>(&'a self, handle: &'a LedgerHandle) -> RefTransactBuilder<'a, S, N>
    where
        S: ContentAddressedWrite,
        N: Publisher,
    {
        RefTransactBuilder::new(self, handle)
    }

    /// Create a transaction builder that consumes a `LedgerState`.
    ///
    /// Use this for CLI tools, scripts, or tests where you manage your own
    /// ledger state. The ledger state is consumed and returned in the result
    /// as an updated `LedgerState` after commit.
    ///
    /// For server/application contexts, prefer [`stage()`](Self::stage) which
    /// uses cached handles with proper concurrency support.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.stage_owned(ledger)
    ///     .insert(&data)
    ///     .execute().await?;
    /// let ledger = result.ledger;
    /// ```
    pub fn stage_owned(&self, ledger: LedgerState) -> OwnedTransactBuilder<'_, S, N>
    where
        S: ContentAddressedWrite,
        N: Publisher,
    {
        OwnedTransactBuilder::new(self, ledger)
    }

    /// Create a FROM-driven query builder.
    ///
    /// Use this when the query body itself specifies which ledgers to target
    /// (via `"from"` in JSON-LD or `FROM`/`FROM NAMED` in SPARQL).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.query_from()
    ///     .jsonld(&query_with_from)
    ///     .policy(ctx)
    ///     .execute().await?;
    /// ```
    pub fn query_from(&self) -> FromQueryBuilder<'_, S, N> {
        FromQueryBuilder::new(self)
    }

    /// Create a ledger info builder for retrieving comprehensive ledger metadata.
    ///
    /// Returns metadata including commit info, nameservice record, namespace
    /// codes, stats with decoded IRIs, and index information.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let info = fluree.ledger_info("mydb:main")
    ///     .with_context(&context)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn ledger_info(&self, ledger_id: &str) -> ledger_info::LedgerInfoBuilder<'_, S, N> {
        ledger_info::LedgerInfoBuilder::new(self, ledger_id.to_string())
    }

    /// Check if a ledger exists by address.
    ///
    /// Returns `true` if the ledger is registered in the nameservice,
    /// `false` otherwise. This is a lightweight check that only queries
    /// the nameservice without loading the ledger data.
    ///
    /// # Arguments
    /// * `ledger_id` - Ledger ID (e.g., "my/ledger") or full address
    ///
    /// # Example
    ///
    /// ```ignore
    /// if fluree.ledger_exists("my/ledger").await? {
    ///     let ledger = fluree.ledger("my/ledger").await?;
    /// } else {
    ///     let ledger = fluree.create_ledger(&config).await?;
    /// }
    /// ```
    pub async fn ledger_exists(&self, ledger_id: &str) -> Result<bool> {
        Ok(self.nameservice.lookup(ledger_id).await?.is_some())
    }

    /// Enable connection-level ledger caching on an already-constructed Fluree instance.
    ///
    /// This is primarily intended for servers/peers that construct Fluree with custom
    /// storage or nameservice implementations (e.g. proxy mode) where `FlureeBuilder`
    /// is not used.
    ///
    /// If caching is already enabled, this is a no-op.
    pub fn enable_ledger_caching(self) -> Self {
        self.enable_ledger_cache_config(LedgerManagerConfig::default())
    }

    /// Enable connection-level ledger caching with a custom configuration.
    ///
    /// If caching is already enabled, this is a no-op.
    pub fn enable_ledger_cache_config(mut self, config: LedgerManagerConfig) -> Self {
        if self.ledger_manager.is_some() {
            return self;
        }

        let storage = self.connection.storage().clone();
        let nameservice = self.nameservice.clone();

        self.ledger_manager = Some(Arc::new(LedgerManager::new(storage, nameservice, config)));

        self
    }

    /// Get a cached ledger handle (loads if not cached)
    ///
    /// If caching is disabled (no `with_ledger_caching()` on builder),
    /// returns an ephemeral handle that wraps a fresh load.
    /// Server code should assert caching is enabled if it expects reuse.
    pub async fn ledger_cached(&self, ledger_id: &str) -> Result<LedgerHandle> {
        match &self.ledger_manager {
            Some(mgr) => mgr.get_or_load(ledger_id).await,
            None => {
                // Caching disabled: load fresh, wrap in ephemeral handle
                // Note: This handle is NOT cached; each call loads fresh.
                let state = self.ledger(ledger_id).await?;
                Ok(LedgerHandle::ephemeral(ledger_id.to_string(), state))
            }
        }
    }

    /// Disconnect a ledger from the connection cache
    ///
    /// Releases the cached ledger state, forcing a fresh load on the next access.
    /// This is the Rust equivalent of Clojure's `release-ledger`.
    ///
    /// Use this when:
    /// - You want to force a fresh load of a ledger (e.g., after external changes)
    /// - You want to free memory for a ledger you no longer need
    /// - You're shutting down and want to release resources cleanly
    ///
    /// If caching is disabled, this is a no-op.
    /// If the ledger is currently being loaded or reloaded, waiters will receive
    /// cancellation errors.
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - The ledger ID to disconnect
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Force fresh load on next access
    /// fluree.disconnect_ledger("my/ledger").await;
    ///
    /// // Next query will load fresh state
    /// let handle = fluree.ledger_cached("my/ledger").await?;
    /// ```
    pub async fn disconnect_ledger(&self, ledger_id: &str) {
        if let Some(mgr) = &self.ledger_manager {
            mgr.disconnect(ledger_id).await;
        }
        // If caching is disabled, this is a no-op
    }

    /// Disconnect the Fluree system, performing best-effort cleanup
    ///
    /// Callers should stop issuing new queries and transactions before calling
    /// this method. New operations issued concurrently with `disconnect` may
    /// receive cancellation errors, and `wait_all_idle` could block if new
    /// indexing work keeps arriving.
    ///
    /// This drains and releases cached state:
    /// 1. Cancels any pending background indexing and waits for in-progress work to idle
    /// 2. Evicts all cached ledgers from the ledger manager (with a shutdown flag
    ///    that prevents in-flight loaders from re-inserting)
    /// 3. Clears the R2RML mapping cache
    ///
    /// For full termination, the caller should also:
    /// - Abort the maintenance task JoinHandle (if spawned via `spawn_maintenance`)
    /// - Drop the Fluree instance (stops the indexer worker)
    pub async fn disconnect(&self) {
        // 1. Cancel background indexing and wait for idle
        if let tx::IndexingMode::Background(handle) = &self.indexing_mode {
            handle.cancel_all().await;
            handle.wait_all_idle().await;
        }

        // 2. Evict all cached ledgers
        if let Some(mgr) = &self.ledger_manager {
            mgr.disconnect_all().await;
        }

        // 3. Clear R2RML cache
        self.r2rml_cache.clear().await;
    }

    /// Refresh a cached ledger by polling the nameservice
    ///
    /// This is the Rust equivalent of Clojure's `fluree.db.api/refresh`.
    ///
    /// # Behavior
    ///
    /// - Looks up the latest nameservice record for the ledger
    /// - If the ledger is cached, compares local state vs remote and updates if stale
    /// - Does NOT cold-load: if the ledger isn't cached, returns `NotLoaded`
    ///
    /// # Returns
    ///
    /// - `Ok(None)` - Nameservice lookup returned no record (ledger doesn't exist)
    /// - `Ok(Some(NotifyResult::NotLoaded))` - Record exists but ledger not cached
    /// - `Ok(Some(NotifyResult::Current))` - Ledger is already up to date
    /// - `Ok(Some(NotifyResult::IndexUpdated))` - Index was refreshed (v1: via reload)
    /// - `Ok(Some(NotifyResult::CommitApplied))` - Single commit applied (v1: via reload)
    /// - `Ok(Some(NotifyResult::Reloaded))` - Full reload was performed
    ///
    /// # Use Cases
    ///
    /// - **Serverless/Lambda**: Poll for updates on warm invocations before querying
    /// - **Long-running processes**: Periodic freshness check without SSE subscriptions
    /// - **External updates**: Check if another process has committed new data
    ///
    /// # Important
    ///
    /// `refresh` is only meaningful when ledger caching is enabled (the server always
    /// enables caching). If caching is disabled, returns `Some(NotLoaded)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Cold start: load the ledger
    /// let _ledger = fluree.ledger_cached("mydb:main").await?;
    ///
    /// // Later (warm invocation): poll for updates
    /// match fluree.refresh("mydb:main").await? {
    ///     Some(NotifyResult::Current) => println!("Already up to date"),
    ///     Some(NotifyResult::Reloaded) => println!("Refreshed from remote"),
    ///     Some(NotifyResult::NotLoaded) => println!("Not cached, load first"),
    ///     None => println!("Ledger not found in nameservice"),
    ///     _ => {}
    /// }
    ///
    /// // Query with fresh data
    /// let handle = fluree.ledger_cached("mydb:main").await?;
    /// let snapshot = handle.snapshot().await;
    /// ```
    pub async fn refresh(&self, ledger_id: &str) -> Result<Option<NotifyResult>> {
        // Step A: Check if caching is enabled
        let mgr = match &self.ledger_manager {
            Some(mgr) => mgr,
            None => {
                // Caching disabled - refresh is a no-op (Clojure parity: no cold-load)
                return Ok(Some(NotifyResult::NotLoaded));
            }
        };

        // Step B: Lookup nameservice record
        // The nameservice handles address resolution (mydb -> mydb:main, etc.)
        let ns_record = match self.nameservice.lookup(ledger_id).await? {
            Some(record) => record,
            None => return Ok(None), // Ledger doesn't exist in nameservice
        };

        // Step C: Use NsRecord.ledger_id as the cache key
        // The ledger_id field contains the canonical form (e.g., "testdb:main")
        // Note: NsRecord.name field only contains the name without branch, despite docs
        let canonical_alias = &ns_record.ledger_id;

        // Step D: Delegate to notify with the fresh record
        let result = mgr
            .notify(NsNotify {
                ledger_id: canonical_alias.clone(),
                record: Some(ns_record),
            })
            .await?;

        Ok(Some(result))
    }

    /// Spawn the ledger manager maintenance task (idle eviction)
    ///
    /// Returns JoinHandle for graceful shutdown. Call `.abort()` on shutdown.
    /// Should be called once after building Fluree.
    /// Returns None if caching is not enabled.
    pub fn spawn_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.ledger_manager
            .as_ref()
            .map(|mgr| mgr.spawn_maintenance())
    }
}

/// Convenience functions for common configurations
///
/// Create a file-backed Fluree instance
///
/// This is the most common configuration for production use.
#[cfg(feature = "native")]
pub fn fluree_file(path: impl Into<String>) -> Result<Fluree<FileStorage, FileNameService>> {
    FlureeBuilder::file(path).build()
}

/// Create a memory-backed Fluree instance
///
/// Useful for testing or when persistence is not needed.
pub fn fluree_memory() -> Fluree<MemoryStorage, MemoryNameService> {
    FlureeBuilder::memory().build_memory()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fluree_builder_memory() {
        let fluree = FlureeBuilder::memory()
            .cache_max_entries(500)
            .build_memory();

        assert_eq!(fluree.connection.config().cache.max_entries, 500);
    }

    #[test]
    #[cfg(feature = "native")]
    fn test_fluree_builder_file() {
        let result = FlureeBuilder::file("/tmp/test")
            .parallelism(8)
            .cache_max_entries(1000)
            .build();

        assert!(result.is_ok());
        let fluree = result.unwrap();
        assert_eq!(fluree.connection.config().parallelism, 8);
    }

    #[test]
    #[cfg(feature = "native")]
    fn test_fluree_builder_no_path_error() {
        let result = FlureeBuilder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_fluree_memory_convenience() {
        let _fluree = fluree_memory();
    }

    // ========================================================================
    // IndexConfig propagation tests (commit e6d0044)
    // ========================================================================

    #[test]
    fn test_default_index_config_returns_defaults_without_thresholds() {
        let fluree = FlureeBuilder::memory().build_memory();
        let cfg = fluree.default_index_config();
        assert_eq!(
            cfg.reindex_min_bytes,
            IndexConfig::default().reindex_min_bytes
        );
        assert_eq!(
            cfg.reindex_max_bytes,
            IndexConfig::default().reindex_max_bytes
        );
    }

    #[test]
    fn test_with_indexing_thresholds_propagates_to_default_index_config() {
        // This is the exact scenario that was broken before e6d0044:
        // custom thresholds set via the builder were silently dropped.
        let fluree = FlureeBuilder::memory()
            .with_indexing_thresholds(500_000, 5_000_000)
            .build_memory();

        let cfg = fluree.default_index_config();
        assert_eq!(cfg.reindex_min_bytes, 500_000);
        assert_eq!(cfg.reindex_max_bytes, 5_000_000);
    }

    #[test]
    fn test_derive_index_config_extracts_from_connection_config() {
        use fluree_db_connection::config::{DefaultsConfig, IndexingDefaults};

        let config = ConnectionConfig {
            defaults: Some(DefaultsConfig {
                identity: None,
                indexing: Some(IndexingDefaults {
                    reindex_min_bytes: Some(250_000),
                    reindex_max_bytes: Some(2_500_000),
                    max_old_indexes: None,
                    indexing_enabled: None,
                    track_class_stats: None,
                    gc_min_time_mins: None,
                }),
            }),
            ..Default::default()
        };

        let idx = derive_index_config(&config);
        assert_eq!(idx.reindex_min_bytes, 250_000);
        assert_eq!(idx.reindex_max_bytes, 2_500_000);
    }

    #[test]
    fn test_derive_index_config_falls_back_to_defaults() {
        let config = ConnectionConfig::default();
        let idx = derive_index_config(&config);
        assert_eq!(
            idx.reindex_min_bytes,
            IndexConfig::default().reindex_min_bytes
        );
        assert_eq!(
            idx.reindex_max_bytes,
            IndexConfig::default().reindex_max_bytes
        );
    }

    // ========================================================================
    // Refresh API tests
    // ========================================================================

    #[tokio::test]
    async fn test_refresh_returns_none_when_no_ns_record() {
        // Build with caching enabled
        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();

        // Refresh unknown ledger should return None (not in nameservice)
        let result = fluree.refresh("nonexistent:main").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_refresh_noop_when_not_cached() {
        use fluree_db_core::{ContentId, ContentKind};
        use fluree_db_nameservice::Publisher;

        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();
        let cid = ContentId::new(ContentKind::Commit, b"commit-1");

        // Publish a record to nameservice directly (without caching the ledger)
        fluree
            .nameservice()
            .publish_commit("mydb:main", 5, &cid)
            .await
            .unwrap();

        // Refresh should return NotLoaded (record exists but not cached)
        let result = fluree.refresh("mydb:main").await.unwrap();
        assert_eq!(result, Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_noop_when_caching_disabled() {
        // Build WITHOUT caching
        let fluree = FlureeBuilder::memory().build_memory();

        // Refresh should return NotLoaded (caching disabled = no-op)
        let result = fluree.refresh("mydb:main").await.unwrap();
        assert_eq!(result, Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_with_alias_resolution() {
        use fluree_db_core::{ContentId, ContentKind};
        use fluree_db_nameservice::Publisher;

        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();
        let cid = ContentId::new(ContentKind::Commit, b"commit-1");

        // Publish with canonical alias
        fluree
            .nameservice()
            .publish_commit("mydb:main", 5, &cid)
            .await
            .unwrap();

        // Refresh with short alias should resolve to canonical
        let result = fluree.refresh("mydb").await.unwrap();
        // Should return NotLoaded since we haven't cached it
        assert_eq!(result, Some(NotifyResult::NotLoaded));

        // Refresh with full alias should also work
        let result = fluree.refresh("mydb:main").await.unwrap();
        assert_eq!(result, Some(NotifyResult::NotLoaded));
    }

    #[tokio::test]
    async fn test_refresh_current_when_up_to_date() {
        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();

        // Create a ledger (this publishes to NS and returns state)
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let initial_t = ledger.t();

        // Cache the ledger by loading it
        let handle = fluree.ledger_cached("testdb:main").await.unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, initial_t);

        // Refresh should return Current (cache matches NS)
        let result = fluree.refresh("testdb:main").await.unwrap();
        assert_eq!(result, Some(NotifyResult::Current));

        // Verify cached state is unchanged
        let snapshot_after = handle.snapshot().await;
        assert_eq!(snapshot_after.t, initial_t);
    }

    #[tokio::test]
    async fn test_refresh_after_stage_update() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();

        // Create a ledger (genesis at t=0)
        let _ledger = fluree.create_ledger("txdb").await.unwrap();

        // Cache the ledger (loads from NS at t=0)
        let handle = fluree.ledger_cached("txdb:main").await.unwrap();
        let snapshot_before = handle.snapshot().await;
        assert_eq!(snapshot_before.t, 0);

        // Do a transaction using stage builder (updates cache in place)
        let txn = json!({
            "insert": [{"@id": "ex:test", "ex:name": "test"}]
        });
        let _result = fluree.stage(&handle).update(&txn).execute().await.unwrap();

        // The handle should now reflect the new t
        let snapshot_after = handle.snapshot().await;
        assert_eq!(
            snapshot_after.t, 1,
            "Ledger should be at t=1 after transaction"
        );

        // Refresh should return Current (cache is up to date with NS)
        let result = fluree.refresh("txdb:main").await.unwrap();
        assert_eq!(result, Some(NotifyResult::Current));
    }

    #[tokio::test]
    async fn test_refresh_with_short_alias_caching() {
        // CRITICAL: Test cache key consistency when using short aliases
        // This verifies that caching via "mydb" and refreshing via "mydb" works
        // (the cache key must be canonical, not the raw input alias)

        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();

        // Create ledger
        let _ledger = fluree.create_ledger("shortdb").await.unwrap();

        // Cache via SHORT alias (without :main suffix)
        let handle = fluree.ledger_cached("shortdb").await.unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, 0);

        // Refresh via SHORT alias - should find the cached entry
        // If this returns NotLoaded, there's a cache key mismatch bug
        let result = fluree.refresh("shortdb").await.unwrap();
        assert_eq!(
            result,
            Some(NotifyResult::Current),
            "Short alias refresh should find cached entry (not NotLoaded)"
        );

        // Also verify full alias refresh works
        let result_full = fluree.refresh("shortdb:main").await.unwrap();
        assert_eq!(
            result_full,
            Some(NotifyResult::Current),
            "Full alias refresh should also find cached entry"
        );
    }

    // ========================================================================
    // AddressIdentifierResolverStorage tests
    // ========================================================================

    #[tokio::test]
    async fn test_resolver_routes_to_identifier_storage() {
        // Create two separate memory storages
        // Note: MemoryStorage uses the address directly as the key
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://default-key.json", b"default-data")
            .await
            .unwrap();

        let commit_storage = MemoryStorage::new();
        // When routing via identifier, the full address is passed to the storage
        commit_storage
            .write_bytes(
                "fluree:commit-store:memory://mydb/main/commit/abc.fcv2",
                b"commit-data",
            )
            .await
            .unwrap();

        // Build resolver with identifier mapping
        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "commit-store".to_string(),
            AnyStorage::new(Arc::new(commit_storage)),
        );

        let resolver = AddressIdentifierResolverStorage::new(
            AnyStorage::new(Arc::new(default_storage)),
            identifier_map,
        );

        // Address with identifier should route to mapped storage
        let bytes = resolver
            .read_bytes("fluree:commit-store:memory://mydb/main/commit/abc.fcv2")
            .await
            .unwrap();
        assert_eq!(bytes, b"commit-data");

        // Address without identifier should route to default
        let bytes = resolver
            .read_bytes("fluree:memory://default-key.json")
            .await
            .unwrap();
        assert_eq!(bytes, b"default-data");
    }

    #[tokio::test]
    async fn test_resolver_unknown_identifier_routes_to_default() {
        let default_storage = MemoryStorage::new();
        // Write with the full address that will be used for lookup
        default_storage
            .write_bytes("fluree:unknown-id:memory://fallback.json", b"fallback-data")
            .await
            .unwrap();

        // Empty identifier map - all reads go to default
        let resolver = AddressIdentifierResolverStorage::new(
            AnyStorage::new(Arc::new(default_storage)),
            std::collections::HashMap::new(),
        );

        // Unknown identifier falls through to default
        let bytes = resolver
            .read_bytes("fluree:unknown-id:memory://fallback.json")
            .await
            .unwrap();
        assert_eq!(bytes, b"fallback-data");
    }

    #[tokio::test]
    async fn test_resolver_writes_go_to_default() {
        let default_storage = MemoryStorage::new();
        let other_storage = MemoryStorage::new();

        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "other".to_string(),
            AnyStorage::new(Arc::new(other_storage.clone())),
        );

        let resolver = AddressIdentifierResolverStorage::new(
            AnyStorage::new(Arc::new(default_storage.clone())),
            identifier_map,
        );

        // Write with identifier - should go to default (not to "other" storage)
        resolver
            .write_bytes("fluree:other:memory://test.json", b"written-data")
            .await
            .unwrap();

        // Verify it went to default
        let default_exists = default_storage
            .exists("fluree:other:memory://test.json")
            .await
            .unwrap();
        assert!(default_exists, "Write should go to default storage");

        // Verify it did NOT go to other
        let other_exists = other_storage
            .exists("fluree:other:memory://test.json")
            .await
            .unwrap();
        assert!(!other_exists, "Write should NOT go to mapped storage");
    }

    #[tokio::test]
    async fn test_resolver_read_bytes_hint() {
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://data.json", b"hint-test")
            .await
            .unwrap();

        let resolver = AddressIdentifierResolverStorage::new(
            AnyStorage::new(Arc::new(default_storage)),
            std::collections::HashMap::new(),
        );

        // read_bytes_hint should work same as read_bytes
        let bytes = resolver
            .read_bytes_hint(
                "fluree:memory://data.json",
                fluree_db_core::ReadHint::AnyBytes,
            )
            .await
            .unwrap();
        assert_eq!(bytes, b"hint-test");
    }

    #[tokio::test]
    async fn test_resolver_exists() {
        let default_storage = MemoryStorage::new();
        default_storage
            .write_bytes("fluree:memory://exists.json", b"data")
            .await
            .unwrap();

        let mapped_storage = MemoryStorage::new();
        mapped_storage
            .write_bytes("fluree:mapped:memory://mapped.json", b"mapped-data")
            .await
            .unwrap();

        let mut identifier_map = std::collections::HashMap::new();
        identifier_map.insert(
            "mapped".to_string(),
            AnyStorage::new(Arc::new(mapped_storage)),
        );

        let resolver = AddressIdentifierResolverStorage::new(
            AnyStorage::new(Arc::new(default_storage)),
            identifier_map,
        );

        // Check exists on default storage
        assert!(resolver
            .exists("fluree:memory://exists.json")
            .await
            .unwrap());

        // Check exists on mapped storage
        assert!(resolver
            .exists("fluree:mapped:memory://mapped.json")
            .await
            .unwrap());

        // Check non-existent file
        assert!(!resolver
            .exists("fluree:memory://nonexistent.json")
            .await
            .unwrap());
    }
}

#[cfg(all(test, feature = "shacl"))]
mod shacl_tests;
