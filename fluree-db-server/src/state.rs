//! Application state management
//!
//! # Thread Safety Note
//!
//! The HTTP server requires `Send + Sync` for state shared across handlers.
//! This server currently only supports **file-based storage** for production use.
//!
//! Memory storage support would require either:
//! - A single-threaded runtime with `LocalSet`
//! - Or refactoring `MemoryNameService` to use `Arc<RwLock<...>>`
//!
//! # Storage Access Modes
//!
//! Peers can operate in two storage access modes:
//! - **Shared**: Direct access to storage (default, requires storage credentials)
//! - **Proxy**: All storage reads proxied through transaction server (no storage credentials)

use crate::config::{ServerConfig, ServerRole};
use crate::peer::{ForwardingClient, PeerState, ProxyNameService, ProxyStorage};
use crate::registry::LedgerRegistry;
use crate::telemetry::TelemetryConfig;
use fluree_db_api::{Fluree, FlureeBuilder, IndexConfig, QueryConnectionOptions};
use fluree_db_connection::{Connection, ConnectionConfig};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::FileStorage;
use fluree_db_nameservice::file::FileNameService;

/// File-backed Fluree instance type (transaction server or peer with shared storage)
pub type FileFluree = Fluree<FileStorage, FileNameService>;

/// Proxy-backed Fluree instance type (peer with proxy storage access)
pub type ProxyFluree = Fluree<ProxyStorage, ProxyNameService>;

/// Unified Fluree instance wrapper
///
/// Allows AppState to work with either file-backed or proxy-backed Fluree
/// instances, selected at runtime based on configuration.
#[derive(Clone)]
pub enum FlureeInstance {
    /// File-backed storage (transaction server or peer with shared storage)
    File(Arc<FileFluree>),
    /// Proxy-backed storage (peer proxying through transaction server)
    Proxy(Arc<ProxyFluree>),
}

impl std::fmt::Debug for FlureeInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlureeInstance::File(_) => write!(f, "FlureeInstance::File(...)"),
            FlureeInstance::Proxy(_) => write!(f, "FlureeInstance::Proxy(...)"),
        }
    }
}

impl FlureeInstance {
    /// Check if this is a file-backed instance
    pub fn is_file(&self) -> bool {
        matches!(self, FlureeInstance::File(_))
    }

    /// Check if this is a proxy-backed instance
    pub fn is_proxy(&self) -> bool {
        matches!(self, FlureeInstance::Proxy(_))
    }

    /// Get the file-backed instance (panics if proxy)
    ///
    /// Use this in code paths that are guaranteed to be transaction-mode only
    /// (e.g., write handlers which forward to tx server in peer mode).
    pub fn as_file(&self) -> &Arc<FileFluree> {
        match self {
            FlureeInstance::File(f) => f,
            FlureeInstance::Proxy(_) => panic!("Expected file-backed Fluree instance"),
        }
    }

    /// Get the proxy-backed instance (panics if file)
    pub fn as_proxy(&self) -> &Arc<ProxyFluree> {
        match self {
            FlureeInstance::Proxy(p) => p,
            FlureeInstance::File(_) => panic!("Expected proxy-backed Fluree instance"),
        }
    }

    /// Lookup a ledger in the nameservice
    ///
    /// Works with both file-backed and proxy-backed instances.
    pub async fn nameservice_lookup(
        &self,
        ledger_id: &str,
    ) -> fluree_db_nameservice::Result<Option<fluree_db_nameservice::NsRecord>> {
        use fluree_db_api::NameService;
        match self {
            FlureeInstance::File(f) => f.nameservice().lookup(ledger_id).await,
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
            FlureeInstance::File(f) => f.ledger_exists(ledger_id).await,
            FlureeInstance::Proxy(p) => p.ledger_exists(ledger_id).await,
        }
    }

    // === Connection-level query methods (no ledger loading required) ===

    /// Execute a SPARQL query against a connection (dataset specified via FROM clause)
    pub async fn query_connection_sparql_jsonld(
        &self,
        sparql: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::File(f) => f.query_from().sparql(sparql).execute_formatted().await,
            FlureeInstance::Proxy(p) => p.query_from().sparql(sparql).execute_formatted().await,
        }
    }

    /// Execute a JSON-LD/FQL query against a connection (dataset specified via from/to keys)
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
            FlureeInstance::File(f) => f.query_from().jsonld(query_json).execute_formatted().await,
            FlureeInstance::Proxy(p) => p.query_from().jsonld(query_json).execute_formatted().await,
        }
    }

    /// Execute a tracked JSON-LD/FQL query against a connection.
    ///
    /// Returns a tracked response body (status/result/time/fuel/policy) that callers can
    /// forward directly, and optionally translate to tracking headers for Clojure parity.
    pub async fn query_connection_jsonld_tracked(
        &self,
        query_json: &serde_json::Value,
    ) -> std::result::Result<fluree_db_api::TrackedQueryResponse, fluree_db_api::TrackedErrorResponse>
    {
        match self {
            FlureeInstance::File(f) => f.query_from().jsonld(query_json).execute_tracked().await,
            FlureeInstance::Proxy(p) => p.query_from().jsonld(query_json).execute_tracked().await,
        }
    }

    // === Ledger-level query methods (load graph, run query, return JSON) ===

    /// Load a graph and execute a JSON-LD query
    pub async fn query_ledger_jsonld(
        &self,
        ledger_id: &str,
        query_json: &serde_json::Value,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::File(f) => {
                f.graph(ledger_id)
                    .query()
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
            FlureeInstance::File(f) => {
                f.graph(ledger_id)
                    .query()
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
            FlureeInstance::File(f) => {
                f.graph(ledger_id)
                    .query()
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
                    FlureeInstance::File(f) => {
                        let view = f.view_with_policy(ledger_id, &opts).await?;
                        view.query(f.as_ref())
                            .sparql(sparql)
                            .execute_formatted()
                            .await
                    }
                    FlureeInstance::Proxy(p) => {
                        let view = p.view_with_policy(ledger_id, &opts).await?;
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
            FlureeInstance::File(f) => {
                let ledger = f.ledger(ledger_id).await?;
                f.explain(&ledger, query_json).await
            }
            FlureeInstance::Proxy(p) => {
                let ledger = p.ledger(ledger_id).await?;
                p.explain(&ledger, query_json).await
            }
        }
    }

    /// Load a ledger directly (for peer mode freshness checking in shared storage mode)
    ///
    /// Returns the ledger state with its index_t for freshness comparison.
    /// In proxy mode, this always fetches the latest from the transaction server.
    pub async fn ledger_index_t(&self, ledger_id: &str) -> fluree_db_api::Result<i64> {
        match self {
            FlureeInstance::File(f) => {
                let ledger = f.ledger(ledger_id).await?;
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
            FlureeInstance::File(f) => f.spawn_maintenance(),
            FlureeInstance::Proxy(p) => p.spawn_maintenance(),
        }
    }

    /// Get count of cached ledgers (for metrics/stats)
    ///
    /// Returns 0 if caching is not enabled.
    pub async fn cached_ledger_count(&self) -> usize {
        match self {
            FlureeInstance::File(f) => {
                if let Some(mgr) = f.ledger_manager() {
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
            FlureeInstance::File(f) => f.disconnect_ledger(ledger_id).await,
            FlureeInstance::Proxy(p) => p.disconnect_ledger(ledger_id).await,
        }
    }

    /// Disconnect the entire Fluree system
    ///
    /// Performs best-effort cleanup: cancels indexing, evicts all cached
    /// ledgers, and clears caches. See `Fluree::disconnect` for details.
    pub async fn disconnect(&self) {
        match self {
            FlureeInstance::File(f) => f.disconnect().await,
            FlureeInstance::Proxy(p) => p.disconnect().await,
        }
    }

    /// Build comprehensive ledger info (for MCP get_data_model and /fluree/ledger-info)
    ///
    /// Works with both file-backed and proxy-backed instances. Loads the ledger
    /// into cache if not already cached, then builds metadata including commit info,
    /// namespace codes, and statistics.
    pub async fn build_ledger_info(
        &self,
        ledger_id: &str,
    ) -> fluree_db_api::Result<serde_json::Value> {
        match self {
            FlureeInstance::File(f) => {
                let handle = f.ledger_cached(ledger_id).await?;
                let ledger_state = handle.snapshot().await.to_ledger_state();
                fluree_db_api::ledger_info::build_ledger_info(&ledger_state, None)
                    .await
                    .map_err(|e| fluree_db_api::ApiError::internal(e.to_string()))
            }
            FlureeInstance::Proxy(p) => {
                let handle = p.ledger_cached(ledger_id).await?;
                let ledger_state = handle.snapshot().await.to_ledger_state();
                fluree_db_api::ledger_info::build_ledger_info(&ledger_state, None)
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

    /// HTTP client for write forwarding (peer mode only)
    pub forwarding_client: Option<Arc<ForwardingClient>>,

    /// Counter for ledger refreshes (for testing/metrics)
    /// Incremented when a ledger is actually reloaded (not for coalesced requests)
    pub refresh_counter: AtomicU64,
}

impl AppState {
    /// Create new application state from config
    ///
    /// Initializes either file-backed or proxy-backed Fluree based on config:
    /// - Transaction server: Always file-backed
    /// - Peer with shared storage: File-backed
    /// - Peer with proxy storage: Proxy-backed (no local storage needed)
    pub fn new(
        config: ServerConfig,
        telemetry_config: TelemetryConfig,
    ) -> Result<Self, fluree_db_api::ApiError> {
        // Validate configuration at startup
        config.validate().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Invalid configuration: {}", e))
        })?;

        // Create Fluree instance based on storage access mode
        let fluree = if config.is_proxy_storage_mode() {
            // Proxy mode: peer proxies all storage reads through tx server
            Self::create_proxy_fluree(&config)?
        } else {
            // Shared mode (or transaction server): use file storage
            Self::create_file_fluree(&config)?
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

        Ok(Self {
            fluree,
            config,
            telemetry_config,
            start_time: Instant::now(),
            index_config: None,
            registry,
            #[cfg(feature = "oidc")]
            jwks_cache,
            peer_state,
            forwarding_client,
            refresh_counter: AtomicU64::new(0),
        })
    }

    /// Create a file-backed Fluree instance
    fn create_file_fluree(
        config: &ServerConfig,
    ) -> Result<FlureeInstance, fluree_db_api::ApiError> {
        let path = config
            .storage_path
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("fluree-server-data"));

        // Convert PathBuf to String for FlureeBuilder
        let path_str = path.to_string_lossy().to_string();

        let builder = FlureeBuilder::file(&path_str)
            .cache_max_entries(config.cache_max_entries)
            .with_ledger_caching(); // Enable connection-level ledger caching

        let fluree = builder.build()?;
        Ok(FlureeInstance::File(Arc::new(fluree)))
    }

    /// Create a proxy-backed Fluree instance for peer proxy mode
    fn create_proxy_fluree(
        config: &ServerConfig,
    ) -> Result<FlureeInstance, fluree_db_api::ApiError> {
        let tx_url = config
            .tx_server_url
            .clone()
            .expect("tx_server_url validated in proxy mode");

        let token = config.load_storage_proxy_token().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Failed to load storage proxy token: {}", e))
        })?;

        // Create proxy storage and nameservice
        let storage = ProxyStorage::new(tx_url.clone(), token.clone());
        let nameservice = ProxyNameService::new(tx_url, token);

        // Create connection with proxy storage
        let connection = Connection::new(ConnectionConfig::default(), storage);

        // Create Fluree instance with proxy components
        //
        // Important: enable connection-level ledger caching so peers can keep
        // cached ledger state hot (refresh-on-events) even in proxy mode.
        let fluree = Fluree::new(connection, nameservice).enable_ledger_caching();

        tracing::info!("Initialized peer with proxy storage mode");
        Ok(FlureeInstance::Proxy(Arc::new(fluree)))
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
            FlureeInstance::File(f) => f.nameservice().subscribe(scope).await,
            FlureeInstance::Proxy(f) => f.nameservice().subscribe(scope).await,
        }
    }
}
