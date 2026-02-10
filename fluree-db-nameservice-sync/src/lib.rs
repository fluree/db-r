//! Git-like remote sync for Fluree DB nameservice
//!
//! This crate provides the sync infrastructure for replicating nameservice
//! state between Fluree instances, modeled after git's remote/fetch/pull/push
//! workflow.
//!
//! # Architecture
//!
//! - [`config`]: Remote and upstream configuration
//! - [`client`]: HTTP client for communicating with remote nameservices
//! - [`origin`]: CAS object fetcher with multi-origin fallback and integrity verification
//! - [`watch`]: Remote watch trait with SSE and polling implementations
//! - [`backoff`]: Exponential backoff utility
//! - [`error`]: Error types for sync operations
//!
//! # Dependencies
//!
//! This crate depends on `fluree-db-nameservice` for core types (`RefPublisher`,
//! `RemoteTrackingStore`, etc.) and `fluree-sse` for SSE parsing. It brings in
//! `reqwest` for HTTP — consumers that don't need sync don't pay this cost.

pub mod backoff;
pub mod client;
pub mod config;
pub mod driver;
pub mod error;
pub mod ledger_config;
pub mod origin;
mod server_sse;
pub mod watch;
pub mod watch_poll;
pub mod watch_sse;

pub use client::{HttpRemoteClient, RemoteNameserviceClient, RemoteSnapshot};
pub use config::{
    MemorySyncConfigStore, RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint,
    SyncConfigStore, UpstreamConfig,
};
pub use driver::{FetchResult, PullResult, PushResult, SyncDriver};
pub use error::{Result, SyncError};
pub use ledger_config::{AuthRequirement, LedgerConfig, Origin, ReplicationDefaults};
pub use origin::{HttpOriginFetcher, MultiOriginFetcher};
pub use watch::{RemoteEvent, RemoteWatch};
pub use watch_poll::PollRemoteWatch;
pub use watch_sse::SseRemoteWatch;
