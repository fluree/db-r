//! Application state management
//!
//! # Thread Safety Note
//!
//! The HTTP server requires `Send + Sync` for state shared across handlers.
//!
//! # Storage Access Modes
//!
//! The server operates in one of two storage access modes:
//! - **Direct**: Local storage backend (file, S3, DynamoDB, etc.) built via `FlureeBuilder::build_client()`
//! - **Proxy**: All storage reads proxied through transaction server (no storage credentials)

use crate::config::{ServerConfig, ServerRole};
use crate::peer::{ForwardingClient, PeerState, ProxyNameService, ProxyStorage};
use crate::registry::LedgerRegistry;
use crate::telemetry::TelemetryConfig;
use fluree_db_api::{Fluree, FlureeBuilder, FlureeClient, IndexConfig, QueryConnectionOptions};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Proxy-backed Fluree instance type (peer with proxy storage access)
pub type ProxyFluree = Fluree<ProxyStorage, ProxyNameService>;

/// Unified Fluree instance wrapper
///
/// Allows AppState to work with either direct-storage or proxy-backed
/// Fluree instances, selected at runtime based on configuration.
#[derive(Clone)]
pub enum FlureeInstance {
    /// Direct storage backend (file, S3, etc.) built via `FlureeBuilder::build_client()`.
    Direct(Arc<FlureeClient>),
    /// Proxy-backed storage (peer proxying through transaction server).
    Proxy(Arc<ProxyFluree>),
}

impl std::fmt::Debug for FlureeInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlureeInstance::Direct(_) => write!(f, "FlureeInstance::Direct(...)"),
            FlureeInstance::Proxy(_) => write!(f, "FlureeInstance::Proxy(...)"),
        }
    }
}

impl FlureeInstance {
    /// Check if this is a direct storage instance
    pub fn is_direct(&self) -> bool {
        matches!(self, FlureeInstance::Direct(_))
    }

    /// Check if this is a proxy-backed instance
    pub fn is_proxy(&self) -> bool {
        matches!(self, FlureeInstance::Proxy(_))
    }

    /// Get the direct-storage instance (panics if not direct)
    pub fn as_direct(&self) -> &Arc<FlureeClient> {
        match self {
            FlureeInstance::Direct(d) => d,
            _ => panic!("Expected direct-storage Fluree instance"),
        }
    }

    /// Get the proxy-backed instance (panics if not proxy)
    pub fn as_proxy(&self) -> &Arc<ProxyFluree> {
        match self {
            FlureeInstance::Proxy(p) => p,
            _ => panic!("Expected proxy-backed Fluree instance"),
        }
    }

    /// Lookup a ledger in the nameservice
    ///
    /// Works with all instance types.
    pub async fn nameservice_lookup(
        &self,
        ledger_id: &str,
    ) -> fluree_db_nameservice::Result<Option<fluree_db_nameservice::NsRecord>> {
        use fluree_db_nameservice::NameService as _;
        match self {
            FlureeInstance::Direct(d) => d.nameservice().lookup(ledger_id).await,
            FlureeInstance::Proxy(p) => p.nameservice().lookup(ledger_id).await,
        }
    }

    /// Check if a ledger exists by ledger ID
    ///
    /// Returns `true` if the ledger is registered in the nameservice,
    /// `false` otherwise. This is a lightweight check that only queries
    /// the nameservice without loading the ledger data.
    pub async fn ledger_exists(&self, ledger_id: &str) -> fluree_db_api::Result<bool> {
        match self {
            FlureeInstance::Direct(d) => d.ledger_exists(ledger_id).await,
            FlureeInstance::Proxy(p) => p.ledger_exists(ledger_id).await,
        }
    }

    /// List all non-retracted branches for a ledger.
    pub async fn list_branches(
        &self,
        ledger_name: &str,
    ) -> fluree_db_api::Result<Vec<fluree_db_nameservice::NsRecord>> {
        match self {
            FlureeInstance::Direct(d) => d.list_branches(ledger_name).await,
            FlureeInstance::Proxy(p) => p.list_branches(ledger_name).await,
        }
    }

    /// List all nameservice records (ledgers)
    pub async fn all_ns_records(
        &self,
    ) -> fluree_db_nameservice::Result<Vec<fluree_db_nameservice::NsRecord>> {
        use fluree_db_nameservice::NameService as _;
        match self {
            FlureeInstance::Direct(d) => d.nameservice().all_records().await,
            FlureeInstance::Proxy(p) => p.nameservice().all_records().await,
        }
    }

    /// List all graph source records
    pub async fn all_graph_source_records(
        &self,
    ) -> fluree_db_nameservice::Result<Vec<fluree_db_nameservice::GraphSourceRecord>> {
        use fluree_db_nameservice::GraphSourceLookup as _;
        match self {
            FlureeInstance::Direct(d) => d.nameservice().all_graph_source_records().await,
            // Proxy mode: graph source records not available locally
            FlureeInstance::Proxy(_) => Ok(Vec::new()),
        }
    }

    /// Lookup a graph source by ID (e.g., "my-gs:main")
    pub async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> fluree_db_nameservice::Result<Option<fluree_db_nameservice::GraphSourceRecord>> {
        use fluree_db_nameservice::GraphSourceLookup as _;
        match self {
            FlureeInstance::Direct(d) => d.nameservice().lookup_graph_source(graph_source_id).await,
            // Proxy mode: graph source records not available locally
            FlureeInstance::Proxy(_) => Ok(None),
        }
    }

    // === Connection-level query methods (no ledger loading required) ===

    /// Execute a SPARQL query against a connection (dataset specified via FROM clause)
    pub async fn query_connection_sparql_jsonld(
        &self,
        sparql: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => d.query_from().sparql(sparql).execute_formatted().await,
            FlureeInstance::Proxy(p) => p.query_from().sparql(sparql).execute_formatted().await,
        }
    }

    /// Execute a JSON-LD query against a connection (dataset specified via from/to keys)
    ///
    /// This is the connection-scoped query path that properly handles:
    /// - History queries (from + to keys)
    /// - Multi-ledger union queries (from: [ledger1, ledger2])
    /// - Time-travel queries (from: "ledger@t:123")
    pub async fn query_connection_jsonld(
        &self,
        query_json: &serde_json::Value,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                d.query_from().jsonld(query_json).execute_formatted().await
            }
            FlureeInstance::Proxy(p) => p.query_from().jsonld(query_json).execute_formatted().await,
        }
    }

    /// Execute a tracked JSON-LD query against a connection.
    ///
    /// Returns a tracked response body (status/result/time/fuel/policy) that callers can
    /// forward directly, and optionally translate to tracking headers.
    pub async fn query_connection_jsonld_tracked(
        &self,
        query_json: &serde_json::Value,
    ) -> std::result::Result<fluree_db_api::TrackedQueryResponse, fluree_db_api::TrackedErrorResponse>
    {
        match self {
            FlureeInstance::Direct(d) => d.query_from().jsonld(query_json).execute_tracked().await,
            FlureeInstance::Proxy(p) => p.query_from().jsonld(query_json).execute_tracked().await,
        }
    }

    // === Ledger-level query methods (load graph, run query, return JSON) ===

    /// Load a graph and execute a JSON-LD query.
    ///
    /// With iceberg support, resolves graph sources transparently and
    /// enables R2RML providers for GRAPH pattern execution.
    pub async fn query_ledger_jsonld(
        &self,
        ledger_id: &str,
        query_json: &serde_json::Value,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                let view = d.load_graph_db_or_graph_source(ledger_id).await?;
                fluree_db_api::GraphSnapshotQueryBuilder::new_from_parts(d, &view)
                    .jsonld(query_json)
                    .execute_formatted()
                    .await
            }
            FlureeInstance::Proxy(p) => {
                p.graph(ledger_id)
                    .query()
                    .jsonld(query_json)
                    .execute_formatted()
                    .await
            }
        }
    }

    /// Load a graph and execute a tracked query (with fuel metering)
    pub async fn query_ledger_tracked(
        &self,
        ledger_id: &str,
        query_json: &serde_json::Value,
    ) -> std::result::Result<fluree_db_api::TrackedQueryResponse, fluree_db_api::TrackedErrorResponse>
    {
        match self {
            FlureeInstance::Direct(d) => {
                let view = d
                    .load_graph_db_or_graph_source(ledger_id)
                    .await
                    .map_err(|e| {
                        fluree_db_api::TrackedErrorResponse::new(404, e.to_string(), None)
                    })?;
                fluree_db_api::GraphSnapshotQueryBuilder::new_from_parts(d, &view)
                    .jsonld(query_json)
                    .execute_tracked()
                    .await
            }
            FlureeInstance::Proxy(p) => {
                p.graph(ledger_id)
                    .query()
                    .jsonld(query_json)
                    .execute_tracked()
                    .await
            }
        }
    }

    /// Load a graph and execute a SPARQL query
    pub async fn query_ledger_sparql_jsonld(
        &self,
        ledger_id: &str,
        sparql: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                let view = d.load_graph_db_or_graph_source(ledger_id).await?;
                fluree_db_api::GraphSnapshotQueryBuilder::new_from_parts(d, &view)
                    .sparql(sparql)
                    .execute_formatted()
                    .await
            }
            FlureeInstance::Proxy(p) => {
                p.graph(ledger_id)
                    .query()
                    .sparql(sparql)
                    .execute_formatted()
                    .await
            }
        }
    }

    /// Load a graph and execute a SPARQL query with optional identity-based policy
    ///
    /// If `identity` is provided, the query will be executed with policy enforcement
    /// based on the identity's `f:policyClass` property. If no policies are found
    /// for the identity, the query is denied by default.
    pub async fn query_ledger_sparql_with_identity(
        &self,
        ledger_id: &str,
        sparql: &str,
        identity: Option<&str>,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match identity {
            Some(identity_iri) => {
                // Build policy options from identity
                let opts = QueryConnectionOptions {
                    identity: Some(identity_iri.to_string()),
                    ..Default::default()
                };

                match self {
                    FlureeInstance::Direct(d) => {
                        let view = d.db_with_policy(ledger_id, &opts).await?;
                        view.query(d.as_ref())
                            .sparql(sparql)
                            .execute_formatted()
                            .await
                    }
                    FlureeInstance::Proxy(p) => {
                        let view = p.db_with_policy(ledger_id, &opts).await?;
                        view.query(p.as_ref())
                            .sparql(sparql)
                            .execute_formatted()
                            .await
                    }
                }
            }
            None => {
                // No identity - execute without policy
                self.query_ledger_sparql_jsonld(ledger_id, sparql).await
            }
        }
    }

    /// Load a ledger and explain a query plan
    pub async fn explain_ledger(
        &self,
        ledger_id: &str,
        query_json: &serde_json::Value,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                let ledger = d.ledger(ledger_id).await?;
                let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
                d.explain(&db, query_json).await
            }
            FlureeInstance::Proxy(p) => {
                let ledger = p.ledger(ledger_id).await?;
                let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
                p.explain(&db, query_json).await
            }
        }
    }

    /// Load a ledger and explain a SPARQL query plan.
    pub async fn explain_ledger_sparql(
        &self,
        ledger_id: &str,
        sparql: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                let ledger = d.ledger(ledger_id).await?;
                let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
                d.explain_sparql(&db, sparql).await
            }
            FlureeInstance::Proxy(p) => {
                let ledger = p.ledger(ledger_id).await?;
                let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
                p.explain_sparql(&db, sparql).await
            }
        }
    }

    /// Load a ledger directly (for peer mode freshness checking in shared storage mode)
    ///
    /// Returns the ledger state with its index_t for freshness comparison.
    /// In proxy mode, this always fetches the latest from the transaction server.
    pub async fn ledger_index_t(&self, ledger_id: &str) -> fluree_db_api::Result<i64> {
        match self {
            FlureeInstance::Direct(d) => {
                let ledger = d.ledger(ledger_id).await?;
                Ok(ledger.index_t())
            }
            FlureeInstance::Proxy(p) => {
                let ledger = p.ledger(ledger_id).await?;
                Ok(ledger.index_t())
            }
        }
    }

    /// Spawn the ledger manager maintenance task (idle eviction)
    ///
    /// Returns JoinHandle for graceful shutdown. Call `.abort()` on shutdown.
    /// Returns None if caching is not enabled.
    pub fn spawn_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        match self {
            FlureeInstance::Direct(d) => d.spawn_maintenance(),
            FlureeInstance::Proxy(p) => p.spawn_maintenance(),
        }
    }

    /// Get count of cached ledgers (for metrics/stats)
    ///
    /// Returns 0 if caching is not enabled.
    pub async fn cached_ledger_count(&self) -> usize {
        match self {
            FlureeInstance::Direct(d) => {
                if let Some(mgr) = d.ledger_manager() {
                    mgr.cached_count().await
                } else {
                    0
                }
            }
            FlureeInstance::Proxy(p) => {
                if let Some(mgr) = p.ledger_manager() {
                    mgr.cached_count().await
                } else {
                    0
                }
            }
        }
    }

    /// Disconnect a ledger from the connection cache
    ///
    /// Releases the cached ledger state, forcing a fresh load on the next access.
    /// If caching is disabled, this is a no-op.
    pub async fn disconnect_ledger(&self, ledger_id: &str) {
        match self {
            FlureeInstance::Direct(d) => d.disconnect_ledger(ledger_id).await,
            FlureeInstance::Proxy(p) => p.disconnect_ledger(ledger_id).await,
        }
    }

    /// Disconnect the entire Fluree system
    ///
    /// Performs best-effort cleanup: cancels indexing, evicts all cached
    /// ledgers, and clears caches. See `Fluree::disconnect` for details.
    pub async fn disconnect(&self) {
        match self {
            FlureeInstance::Direct(d) => d.disconnect().await,
            FlureeInstance::Proxy(p) => p.disconnect().await,
        }
    }

    /// Build comprehensive ledger info (for MCP get_data_model and /fluree/ledger-info)
    ///
    /// Works with all instance types. Loads the ledger into cache if not already
    /// cached, then builds metadata including commit info, namespace codes, and statistics.
    pub async fn build_ledger_info(
        &self,
        ledger_id: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::Direct(d) => {
                let handle = d.ledger_cached(ledger_id).await?;
                let ledger_state = handle.snapshot().await.to_ledger_state();
                fluree_db_api::ledger_info::build_ledger_info(&ledger_state, d.storage(), None)
                    .await
                    .map_err(|e| fluree_db_api::ApiError::internal(e.to_string()))
            }
            FlureeInstance::Proxy(p) => {
                let handle = p.ledger_cached(ledger_id).await?;
                let ledger_state = handle.snapshot().await.to_ledger_state();
                fluree_db_api::ledger_info::build_ledger_info(&ledger_state, p.storage(), None)
                    .await
                    .map_err(|e| fluree_db_api::ApiError::internal(e.to_string()))
            }
        }
    }
}

/// Application state shared across all request handlers
///
/// Uses `Arc<AppState>` for sharing across handlers via axum's State extractor.
pub struct AppState {
    /// Fluree instance (file-backed or proxy-backed depending on config)
    pub fluree: FlureeInstance,

    /// Server configuration
    pub config: ServerConfig,

    /// Telemetry configuration
    pub telemetry_config: TelemetryConfig,

    /// Server start time for uptime tracking
    pub start_time: Instant,

    /// Optional index configuration
    pub index_config: Option<IndexConfig>,

    /// Ledger registry for tracking loaded ledgers and their watermarks
    pub registry: Arc<LedgerRegistry>,

    // === OIDC / JWKS state ===
    /// JWKS cache for OIDC token verification (None if no JWKS issuers configured)
    #[cfg(feature = "oidc")]
    pub jwks_cache: Option<Arc<crate::jwks::JwksCache>>,

    // === Peer mode state ===
    /// Peer state tracking remote watermarks (peer mode only)
    pub peer_state: Option<Arc<PeerState>>,

    /// HTTP client for transaction forwarding (peer mode only)
    pub forwarding_client: Option<Arc<ForwardingClient>>,

    /// Counter for ledger refreshes (for testing/metrics)
    /// Incremented when a ledger is actually reloaded (not for coalesced requests)
    pub refresh_counter: AtomicU64,

    /// Handle for the background leaflet cache stats logger task.
    /// Aborted on drop so the `Arc<LeafletCache>` doesn't outlive the server.
    cache_stats_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AppState {
    fn spawn_leaflet_cache_stats_logger<S, N>(
        fluree: Arc<Fluree<S, N>>,
    ) -> tokio::task::JoinHandle<()>
    where
        S: fluree_db_core::Storage + Clone + Send + Sync + 'static,
        N: fluree_db_nameservice::NameService + Clone + Send + Sync + 'static,
    {
        // Keep logging lightweight and periodic: one line per minute.
        let cache = Arc::clone(fluree.leaflet_cache());
        let budget_mb = fluree.cache_budget_mb();
        let budget_bytes = (budget_mb as u64).saturating_mul(1024 * 1024);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let used = cache.weighted_size_bytes();
                let entries = cache.entry_count();

                let used_gib = (used as f64) / (1024.0 * 1024.0 * 1024.0);
                let budget_gib = (budget_bytes as f64) / (1024.0 * 1024.0 * 1024.0);
                let pct = if budget_bytes == 0 {
                    0.0
                } else {
                    (used as f64) * 100.0 / (budget_bytes as f64)
                };

                tracing::info!(
                    cache_entries = entries,
                    cache_weighted_bytes = used,
                    cache_weighted_gib = used_gib,
                    cache_budget_mb = budget_mb,
                    cache_budget_gib = budget_gib,
                    cache_budget_pct = pct,
                    "LeafletCache stats"
                );
            }
        })
    }

    /// Create new application state from config
    ///
    /// Initializes the Fluree instance based on config:
    /// - Proxy storage mode: Proxy-backed (peer proxying through tx server)
    /// - Otherwise: Direct storage (file, S3, DynamoDB, etc.) via `FlureeBuilder::build_client()`
    pub async fn new(
        config: ServerConfig,
        telemetry_config: TelemetryConfig,
    ) -> Result<Self, fluree_db_api::ApiError> {
        // Validate configuration at startup
        config.validate().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Invalid configuration: {}", e))
        })?;

        // Create Fluree instance based on config
        let (fluree, cache_stats_handle) = if config.is_proxy_storage_mode() {
            // Proxy mode: peer proxies all storage reads through tx server
            Self::create_proxy_fluree(&config)?
        } else {
            // Direct mode: file, S3, DynamoDB, etc. via build_client()
            Self::create_direct_fluree(&config).await?
        };

        // Default idle TTL of 5 minutes for ledger registry
        let registry = Arc::new(LedgerRegistry::new(Duration::from_secs(300)));

        // Initialize peer mode state if in peer role
        let (peer_state, forwarding_client) = if config.server_role == ServerRole::Peer {
            let peer_state = Arc::new(PeerState::new());
            let forwarding_client = Arc::new(ForwardingClient::new(
                config
                    .tx_server_url
                    .clone()
                    .expect("tx_server_url validated in peer mode"),
            ));
            (Some(peer_state), Some(forwarding_client))
        } else {
            (None, None)
        };

        // Create JWKS cache (sync — no fetching yet)
        #[cfg(feature = "oidc")]
        let jwks_cache = {
            match config.jwks_issuer_configs() {
                Ok(configs) if !configs.is_empty() => {
                    let ttl = Some(Duration::from_secs(config.jwks_cache_ttl));
                    Some(Arc::new(crate::jwks::JwksCache::new(configs, ttl)))
                }
                Ok(_) => None,
                Err(e) => {
                    return Err(fluree_db_api::ApiError::internal(format!(
                        "Invalid JWKS configuration: {}",
                        e
                    )));
                }
            }
        };

        // Build IndexConfig from server config (always set, even if indexing is disabled,
        // so that novelty backpressure thresholds are respected for external indexers).
        let index_config = Some(IndexConfig {
            reindex_min_bytes: config.reindex_min_bytes,
            reindex_max_bytes: config.reindex_max_bytes,
        });

        Ok(Self {
            fluree,
            config,
            telemetry_config,
            start_time: Instant::now(),
            index_config,
            registry,
            #[cfg(feature = "oidc")]
            jwks_cache,
            peer_state,
            forwarding_client,
            refresh_counter: AtomicU64::new(0),
            cache_stats_handle: Some(cache_stats_handle),
        })
    }

    /// Create a direct-storage Fluree instance (file, S3, DynamoDB, etc.)
    async fn create_direct_fluree(
        config: &ServerConfig,
    ) -> Result<(FlureeInstance, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        let mut builder = if let Some(ref path) = config.connection_config {
            // Connection config: build from JSON-LD
            let json_str = std::fs::read_to_string(path).map_err(|e| {
                fluree_db_api::ApiError::internal(format!(
                    "Failed to read connection config file '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                fluree_db_api::ApiError::internal(format!(
                    "Failed to parse connection config file '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            FlureeBuilder::from_json_ld(&json)?
        } else {
            // File-backed: use storage_path or default
            let path = config
                .storage_path
                .clone()
                .unwrap_or_else(|| PathBuf::from(".fluree/storage"));
            let path_str = path.to_string_lossy().to_string();
            FlureeBuilder::file(&path_str)
        };

        // Server-level overrides take precedence over connection config defaults.
        // When --connection-config is used, from_json_ld() may set its own indexing
        // and cache config; the server's settings always override.
        if let Some(max_mb) = config.cache_max_mb {
            builder = builder.cache_max_mb(max_mb);
        }
        if config.indexing_enabled {
            builder = builder
                .with_indexing_thresholds(config.reindex_min_bytes, config.reindex_max_bytes);
        } else if config.connection_config.is_some() {
            // Explicitly disable indexing: connection config may have enabled it,
            // but the server's --indexing-enabled=false (the default) takes precedence.
            builder = builder.without_indexing();
        }

        let fluree = Arc::new(builder.build_client().await?);

        tracing::info!(
            storage_type = config.storage_type_str(),
            "Initialized direct backend"
        );

        let handle = Self::spawn_leaflet_cache_stats_logger(Arc::clone(&fluree));
        Ok((FlureeInstance::Direct(fluree), handle))
    }

    /// Create a proxy-backed Fluree instance for peer proxy mode
    fn create_proxy_fluree(
        config: &ServerConfig,
    ) -> Result<(FlureeInstance, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        let tx_url = config
            .tx_server_url
            .clone()
            .expect("tx_server_url validated in proxy mode");

        let token = config.load_storage_proxy_token().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Failed to load storage proxy token: {}", e))
        })?;

        let storage = ProxyStorage::new(tx_url.clone(), token.clone());
        let nameservice = ProxyNameService::new(tx_url, token);

        let fluree = FlureeBuilder::memory().build_with(storage, nameservice);

        tracing::info!("Initialized peer with proxy storage mode");
        let fluree = Arc::new(fluree);
        let handle = Self::spawn_leaflet_cache_stats_logger(Arc::clone(&fluree));
        Ok((FlureeInstance::Proxy(fluree), handle))
    }

    /// Get server uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Subscribe to nameservice events with a given scope
    ///
    /// Convenience method for accessing the nameservice subscription functionality.
    /// Note: In proxy mode, this returns a dummy subscription since peers don't
    /// serve the /fluree/events endpoint.
    pub async fn subscribe_events(
        &self,
        scope: fluree_db_nameservice::SubscriptionScope,
    ) -> fluree_db_nameservice::Result<fluree_db_nameservice::Subscription> {
        use fluree_db_nameservice::Publication;
        match &self.fluree {
            FlureeInstance::Direct(d) => {
                let ns = d.nameservice();
                match ns.publication() {
                    Some(pub_ns) => pub_ns.subscribe(scope).await,
                    None => Err(fluree_db_nameservice::NameServiceError::storage(
                        "Event subscriptions are not supported by this nameservice backend",
                    )),
                }
            }
            FlureeInstance::Proxy(p) => p.nameservice().subscribe(scope).await,
        }
    }
}

impl Drop for AppState {
    fn drop(&mut self) {
        if let Some(handle) = self.cache_stats_handle.take() {
            handle.abort();
        }
    }
}
