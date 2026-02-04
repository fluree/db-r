//! Fluree DB HTTP Server
//!
//! A thin HTTP REST API wrapper around `fluree-db-api`, providing endpoints
//! equivalent to the Clojure Fluree server.
//!
//! # Features
//!
//! - FQL and SPARQL query support
//! - Transaction endpoints (transact, insert, upsert)
//! - History queries
//! - Ledger management (create, drop, info)
//! - Header-based policy injection
//! - CORS support
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_server::{FlureeServer, ServerConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ServerConfig::default();
//!     let server = FlureeServer::new(config).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

pub mod config;
pub mod error;
pub mod extract;
pub mod mcp;
pub mod peer;
pub mod registry;
pub mod routes;
pub mod serde;
pub mod state;
pub mod telemetry;

pub use config::{ServerConfig, ServerRole};
pub use error::{Result, ServerError};
pub use peer::{ForwardingClient, PeerState, PeerSubscriptionTask};
pub use state::AppState;
pub use telemetry::{init_logging, shutdown_tracer, TelemetryConfig};

use axum::Router;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Fluree HTTP Server
pub struct FlureeServer {
    /// Application state
    state: Arc<AppState>,
    /// Configured router
    router: Router,
}

impl FlureeServer {
    /// Create a new server with the given configuration
    pub async fn new(config: ServerConfig) -> std::result::Result<Self, fluree_db_api::ApiError> {
        let telemetry_config = TelemetryConfig::with_server_config(&config);
        let state = Arc::new(AppState::new(config, telemetry_config)?);
        let router = routes::build_router(state.clone());

        Ok(Self { state, router })
    }

    /// Get a reference to the application state
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// Get the router for testing
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Run the server
    pub async fn run(self) -> std::result::Result<(), std::io::Error> {
        let addr = self.state.config.listen_addr;
        let listener = TcpListener::bind(addr).await?;

        // Start peer subscription/sync task if in peer mode
        let subscription_task = if self.state.config.is_peer_mode() {
            let peer_state = self
                .state
                .peer_state
                .clone()
                .expect("peer_state should exist in peer mode");

            if self.state.fluree.is_file() {
                // Shared storage: PeerSyncTask persists refs into local FileNameService
                let events_url = peer::build_peer_events_url(&self.state.config);
                let auth_token = self.state.config.load_peer_events_token().ok().flatten();
                let watch = fluree_db_nameservice_sync::SseRemoteWatch::new(events_url, auth_token);
                let task = peer::PeerSyncTask::new(
                    self.state.fluree.as_file().clone(),
                    peer_state,
                    watch,
                    self.state.config.clone(),
                );
                Some(task.spawn())
            } else {
                // Proxy storage: existing PeerSubscriptionTask (in-memory watermarks only)
                let task = peer::PeerSubscriptionTask::new(
                    self.state.config.clone(),
                    peer_state,
                    self.state.fluree.clone(),
                );
                Some(task.spawn())
            }
        } else {
            None
        };

        // Start ledger manager maintenance task for idle eviction
        let ledger_maintenance_task = self.state.fluree.spawn_maintenance();

        info!(
            addr = %addr,
            storage = %self.state.config.storage_type_str(),
            server_role = ?self.state.config.server_role,
            ledger_caching = ledger_maintenance_task.is_some(),
            mcp_enabled = self.state.config.mcp_enabled,
            "Fluree server starting"
        );

        // Run server
        let result = axum::serve(listener, self.router).await;

        // Cancel maintenance tasks on shutdown
        if let Some(task) = subscription_task {
            task.abort();
        }
        if let Some(task) = ledger_maintenance_task {
            task.abort();
        }

        result
    }

    /// Start the registry maintenance task for tracking ledger watermarks.
    ///
    /// This spawns a background task that:
    /// - Listens to nameservice events and updates registry watermarks
    /// - Periodically sweeps idle entries based on the registry's TTL
    ///
    /// Returns a JoinHandle that can be used to await or abort the task.
    /// The task will automatically stop when the nameservice broadcast channel closes.
    pub async fn start_registry_maintenance(
        &self,
        sweep_interval: std::time::Duration,
    ) -> std::result::Result<tokio::task::JoinHandle<()>, fluree_db_api::ApiError> {
        use fluree_db_nameservice::{Publication, SubscriptionScope};

        let subscription = match &self.state.fluree {
            state::FlureeInstance::File(f) => {
                f.nameservice().subscribe(SubscriptionScope::All).await
            }
            state::FlureeInstance::Proxy(p) => {
                p.nameservice().subscribe(SubscriptionScope::All).await
            }
        }
        .map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Failed to subscribe for registry: {}", e))
        })?;

        let handle = registry::LedgerRegistry::spawn_maintenance_task(
            self.state.registry.clone(),
            subscription.receiver,
            sweep_interval,
        );

        info!("Registry maintenance task started");
        Ok(handle)
    }
}

/// Builder for FlureeServer with fluent API
pub struct FlureeServerBuilder {
    config: ServerConfig,
}

impl FlureeServerBuilder {
    /// Create a new builder with default config (memory storage)
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
        }
    }

    /// Create a builder configured for memory storage
    pub fn memory() -> Self {
        Self::new()
    }

    /// Create a builder configured for file storage
    #[cfg(feature = "native")]
    pub fn file(path: impl Into<std::path::PathBuf>) -> Self {
        let mut builder = Self::new();
        builder.config.storage_path = Some(path.into());
        builder
    }

    /// Set the listen address
    pub fn listen_addr(mut self, addr: impl Into<std::net::SocketAddr>) -> Self {
        self.config.listen_addr = addr.into();
        self
    }

    /// Enable or disable CORS
    pub fn cors_enabled(mut self, enabled: bool) -> Self {
        self.config.cors_enabled = enabled;
        self
    }

    /// Enable or disable background indexing
    pub fn indexing_enabled(mut self, enabled: bool) -> Self {
        self.config.indexing_enabled = enabled;
        self
    }

    /// Set cache max entries
    pub fn cache_max_entries(mut self, entries: usize) -> Self {
        self.config.cache_max_entries = entries;
        self
    }

    /// Build the server
    pub async fn build(self) -> std::result::Result<FlureeServer, fluree_db_api::ApiError> {
        FlureeServer::new(self.config).await
    }
}

impl Default for FlureeServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
