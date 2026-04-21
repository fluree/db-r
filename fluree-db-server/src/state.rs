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
use fluree_db_api::{
    server_defaults, Fluree, FlureeBuilder, IndexConfig, LedgerManagerConfig, NameServiceMode,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

/// Application state shared across all request handlers
///
/// Uses `Arc<AppState>` for sharing across handlers via axum's State extractor.
pub struct AppState {
    /// Fluree instance (storage and nameservice mode selected at startup)
    pub fluree: Arc<Fluree>,

    /// Server configuration
    pub config: ServerConfig,

    /// Resolved path to the config file (.fluree/config.toml or .jsonld).
    /// `None` if no config file was found or used.
    pub config_file_path: Option<PathBuf>,

    /// Mutex serializing config file writes to prevent TOCTOU races.
    pub config_write_lock: tokio::sync::Mutex<()>,

    /// IRI prefix mappings from `.fluree/prefixes.json` (managed by CLI).
    /// Injected as default `@context` entries for JSON-LD queries.
    pub prefixes: Option<std::collections::HashMap<String, String>>,

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

    /// When true, write endpoints (insert, upsert, update, create, drop, etc.)
    /// are rejected with HTTP 503. Read-only endpoints continue to work.
    pub maintenance_mode: AtomicBool,

    /// Counter for ledger refreshes (for testing/metrics)
    /// Incremented when a ledger is actually reloaded (not for coalesced requests)
    pub refresh_counter: AtomicU64,

    /// Set of ledgers whose peer-mode background refresh is in flight.
    ///
    /// Peer queries that detect a stale SSE watermark spawn a background
    /// `mgr.notify()` to catch the cache up. This set dedupes a burst of
    /// concurrent stale-triggered queries so only one background refresh
    /// runs per ledger at a time; the rest simply serve the prior snapshot
    /// and let the in-flight refresh settle for the next query.
    pub refresh_in_flight: StdMutex<HashSet<String>>,

    /// Handle for the background leaflet cache stats logger task.
    /// Aborted on drop so the `Arc<LeafletCache>` doesn't outlive the server.
    cache_stats_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AppState {
    fn spawn_leaflet_cache_stats_logger(fluree: &Arc<Fluree>) -> tokio::task::JoinHandle<()> {
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

        // Resolve config file path (for config write API)
        let config_file_path =
            crate::config_file::resolve_config_path(config.config_file.as_deref());

        // Load IRI prefixes from .fluree/prefixes.json (if present)
        let prefixes = config_file_path.as_ref().and_then(|cfg_path| {
            let fluree_dir = cfg_path.parent()?;
            let prefix_path = fluree_dir.join("prefixes.json");
            match std::fs::read_to_string(&prefix_path) {
                Ok(content) => {
                    match serde_json::from_str::<std::collections::HashMap<String, String>>(
                        &content,
                    ) {
                        Ok(map) if !map.is_empty() => {
                            tracing::info!(
                                count = map.len(),
                                path = %prefix_path.display(),
                                "Loaded IRI prefixes"
                            );
                            Some(map)
                        }
                        Ok(_) => None,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                path = %prefix_path.display(),
                                "Failed to parse prefixes.json"
                            );
                            None
                        }
                    }
                }
                Err(_) => None, // File not found — normal, not an error
            }
        });

        // Create Fluree instance based on storage access mode
        let (fluree, cache_stats_handle) = if config.is_proxy_storage_mode() {
            // Proxy mode: peer proxies all storage reads through tx server
            Self::create_proxy_fluree(&config)?
        } else {
            // Shared mode (or transaction server): use file storage (or S3)
            Self::create_file_fluree(&config)?
        };

        // Use configured TTL for ledger registry, defaulting to 30 minutes
        // (matching LedgerManagerConfig::default() in fluree-db-api).
        let registry_ttl = config
            .ledger_cache_idle_ttl_secs
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(1800));
        let registry = Arc::new(LedgerRegistry::new(registry_ttl));

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

        let start_in_maintenance = config.maintenance_mode;

        Ok(Self {
            fluree,
            config,
            config_file_path,
            config_write_lock: tokio::sync::Mutex::new(()),
            prefixes,
            telemetry_config,
            start_time: Instant::now(),
            index_config,
            registry,
            #[cfg(feature = "oidc")]
            jwks_cache,
            peer_state,
            forwarding_client,
            maintenance_mode: AtomicBool::new(start_in_maintenance),
            refresh_counter: AtomicU64::new(0),
            refresh_in_flight: StdMutex::new(HashSet::new()),
            cache_stats_handle: Some(cache_stats_handle),
        })
    }

    /// Create a file-backed (or S3-backed) Fluree instance.
    ///
    /// Selects the storage backend based on config: if `s3_bucket` is set the
    /// server uses the S3 builder (requires the `aws` feature); otherwise it
    /// falls back to local file storage.
    fn create_file_fluree(
        config: &ServerConfig,
    ) -> Result<(Arc<Fluree>, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        // Determine storage backend: S3 takes precedence if bucket is set
        #[cfg(feature = "aws")]
        let mut builder = if let Some(ref bucket) = config.s3_bucket {
            let endpoint = config.s3_endpoint.as_deref().unwrap_or_default();
            let mut b = FlureeBuilder::s3(bucket, endpoint);
            if let Some(ref prefix) = config.s3_prefix {
                b = b.s3_prefix(prefix);
            }
            b
        } else {
            let path = config
                .storage_path
                .clone()
                .unwrap_or_else(|| PathBuf::from(".fluree/storage"));
            FlureeBuilder::file(path.to_string_lossy())
        };

        #[cfg(not(feature = "aws"))]
        let mut builder = {
            if config.s3_bucket.is_some() {
                return Err(fluree_db_api::ApiError::config(
                    "S3 storage requires the 'aws' feature. Build with: cargo build -p fluree-db-server --features aws",
                ));
            }
            let path = config
                .storage_path
                .clone()
                .unwrap_or_else(|| PathBuf::from(".fluree/storage"));
            FlureeBuilder::file(path.to_string_lossy())
        };

        // Encryption: resolve key from file or direct value
        let encryption_key = match (&config.encryption_key, &config.encryption_key_file) {
            (Some(key), _) => Some(key.clone()),
            (_, Some(path)) => {
                let content = std::fs::read_to_string(path).map_err(|e| {
                    fluree_db_api::ApiError::config(format!(
                        "Failed to read encryption key file {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                Some(content.trim().to_string())
            }
            _ => None,
        };
        if let Some(ref key) = encryption_key {
            builder = builder.with_encryption_key_base64(key)?;
        }

        if let Some(max_mb) = config.cache_max_mb {
            builder = builder.cache_max_mb(max_mb);
        }

        // Wire parallelism
        if let Some(p) = config.parallelism {
            builder = builder.parallelism(p);
        }

        // Wire novelty thresholds (separate from indexing thresholds)
        if config.novelty_min_bytes.is_some() || config.novelty_max_bytes.is_some() {
            builder = builder.with_novelty_thresholds(
                config
                    .novelty_min_bytes
                    .unwrap_or(server_defaults::DEFAULT_REINDEX_MIN_BYTES),
                config
                    .novelty_max_bytes
                    .unwrap_or(server_defaults::DEFAULT_REINDEX_MAX_BYTES),
            );
        }

        // Disk artifact cache budget: propagate to env var so DiskArtifactCache picks it up.
        // The env var may already be set directly; config file / CLI flag values override it.
        // SAFETY: called during single-threaded server init before spawning request handlers.
        if let Some(budget) = config.disk_cache_budget_bytes {
            unsafe {
                std::env::set_var("FLUREE_DISK_CACHE_BUDGET_BYTES", budget.to_string());
            }
        }

        // Ledger manager config: data_dir + cache TTL/sweep settings
        if config.no_ledger_cache {
            builder = builder.without_ledger_caching();
        } else {
            let needs_custom_config = config.data_dir.is_some()
                || config.ledger_cache_idle_ttl_secs.is_some()
                || config.ledger_cache_sweep_secs.is_some();
            if needs_custom_config {
                let mut mgr_config = LedgerManagerConfig::default();
                if let Some(ref dir) = config.data_dir {
                    mgr_config.cache_dir = dir.join("binary_cache");
                }
                if let Some(ttl) = config.ledger_cache_idle_ttl_secs {
                    mgr_config.idle_ttl = Duration::from_secs(ttl);
                }
                if let Some(sweep) = config.ledger_cache_sweep_secs {
                    mgr_config.sweep_interval = Duration::from_secs(sweep);
                }
                builder = builder.with_ledger_cache_config(mgr_config);
            }
        }

        // Wire background indexing if enabled
        if config.indexing_enabled {
            builder = builder
                .with_indexing_thresholds(config.reindex_min_bytes, config.reindex_max_bytes);
        }

        let fluree = Arc::new(builder.build()?);
        let handle = Self::spawn_leaflet_cache_stats_logger(&fluree);
        Ok((fluree, handle))
    }

    /// Create a proxy-backed Fluree instance for peer proxy mode
    fn create_proxy_fluree(
        config: &ServerConfig,
    ) -> Result<(Arc<Fluree>, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        let tx_url = config
            .tx_server_url
            .clone()
            .expect("tx_server_url validated in proxy mode");

        let token = config.load_storage_proxy_token().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Failed to load storage proxy token: {}", e))
        })?;

        let storage = ProxyStorage::new(tx_url.clone(), token.clone());
        let nameservice = ProxyNameService::new(tx_url, token);

        let ns_mode = NameServiceMode::ReadOnly(Arc::new(nameservice));
        let fluree = FlureeBuilder::memory().build_with(storage, ns_mode);

        tracing::info!("Initialized peer with proxy storage mode");
        let fluree = Arc::new(fluree);
        let handle = Self::spawn_leaflet_cache_stats_logger(&fluree);
        Ok((fluree, handle))
    }

    /// Get server uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Returns the configured query timeout, or `None` if disabled (0).
    pub fn query_timeout(&self) -> Option<Duration> {
        if self.config.query_timeout_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(self.config.query_timeout_secs))
        }
    }

    /// Subscribe to ledger/graph-source change events via the event bus.
    pub fn subscribe_events(
        &self,
        scope: fluree_db_nameservice::SubscriptionScope,
    ) -> fluree_db_nameservice::Subscription {
        self.fluree.event_bus().subscribe(scope)
    }
}

impl Drop for AppState {
    fn drop(&mut self) {
        if let Some(handle) = self.cache_stats_handle.take() {
            handle.abort();
        }
    }
}
