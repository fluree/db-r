//! Nameservice sync task for peer mode (shared storage)
//!
//! Unlike [`PeerSubscriptionTask`](super::subscription::PeerSubscriptionTask) which only
//! updates in-memory [`PeerState`] watermarks, `PeerSyncTask` **persists remote refs into
//! the local [`FileNameService`]** via `RefPublisher` CAS operations. This means a restarted
//! peer can serve queries immediately without waiting for SSE replay.
//!
//! Used for shared-storage peers where `FlureeInstance::File` is available.
//! Proxy-storage peers continue to use `PeerSubscriptionTask`.

use std::sync::Arc;

use fluree_db_api::{NotifyResult, NsNotify};
use fluree_db_nameservice::{
    CasResult, NameServiceError, NsRecord, Publisher, RefKind, RefPublisher, RefValue,
};
use fluree_db_nameservice_sync::watch::{RemoteEvent, RemoteWatch};
use fluree_db_nameservice_sync::SseRemoteWatch;
use futures::StreamExt;

use crate::config::ServerConfig;
use crate::peer::state::PeerState;
use crate::state::FileFluree;

/// Background task that syncs nameservice state from a remote transaction server
/// into the local `FileNameService` via SSE events and `RefPublisher` CAS operations.
pub struct PeerSyncTask {
    fluree: Arc<FileFluree>,
    peer_state: Arc<PeerState>,
    watch: SseRemoteWatch,
    config: ServerConfig,
}

impl PeerSyncTask {
    pub fn new(
        fluree: Arc<FileFluree>,
        peer_state: Arc<PeerState>,
        watch: SseRemoteWatch,
        config: ServerConfig,
    ) -> Self {
        Self {
            fluree,
            peer_state,
            watch,
            config,
        }
    }

    /// Spawn the sync task as a background tokio task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let mut stream = self.watch.watch();

        while let Some(event) = stream.next().await {
            match event {
                RemoteEvent::Connected => {
                    // Mirror PeerSubscriptionTask: clear stale state, mark connected, preload
                    self.peer_state.clear().await;
                    self.peer_state.set_connected(true).await;

                    tracing::info!("Connected to transaction server, receiving snapshot");
                    self.preload_configured_ledgers().await;
                }
                RemoteEvent::Disconnected { reason } => {
                    self.peer_state.set_connected(false).await;
                    tracing::warn!(reason = %reason, "Disconnected from transaction server");
                }
                RemoteEvent::Fatal { reason } => {
                    self.peer_state.set_connected(false).await;
                    tracing::error!(
                        reason = %reason,
                        "Fatal peer sync error, will not retry"
                    );
                    break;
                }
                RemoteEvent::LedgerUpdated(record) => {
                    self.handle_ledger_updated(&record).await;
                }
                RemoteEvent::LedgerRetracted { alias } => {
                    self.handle_ledger_retracted(&alias).await;
                }
                RemoteEvent::GraphSourceUpdated(record) => {
                    let alias = record.address.clone();
                    let config_hash = graph_source_config_hash(&record.config);
                    let changed = self
                        .peer_state
                        .update_graph_source(
                            &alias,
                            record.index_t,
                            config_hash,
                            record.index_address.clone(),
                        )
                        .await;

                    if changed {
                        tracing::info!(
                            alias = %alias,
                            index_t = record.index_t,
                            "Remote graph source watermark updated"
                        );
                    }
                }
                RemoteEvent::GraphSourceRetracted { alias } => {
                    self.peer_state.remove_graph_source(&alias).await;
                    tracing::info!(alias = %alias, "Graph source retracted from remote");
                }
            }
        }
    }

    /// Persist remote ledger state into local FileNameService, then update
    /// in-memory watermarks and notify LedgerManager.
    async fn handle_ledger_updated(&self, record: &NsRecord) {
        let ns = self.fluree.nameservice();

        // 1. Ensure ledger exists locally (idempotent)
        match ns.publish_ledger_init(&record.address).await {
            Ok(()) => {}
            Err(NameServiceError::LedgerAlreadyExists(_)) => {}
            Err(e) => {
                tracing::warn!(
                    alias = %record.address,
                    error = %e,
                    "Failed to init ledger locally"
                );
                return;
            }
        }

        // 2. Fast-forward commit head (if record has a commit)
        if record.commit_address.is_some() {
            let commit_ref = RefValue {
                address: record.commit_address.clone(),
                t: record.commit_t,
            };
            match ns
                .fast_forward_commit(&record.address, &commit_ref, 3)
                .await
            {
                Ok(CasResult::Updated) => {}
                Ok(CasResult::Conflict { actual }) => {
                    // Local diverged — server is authoritative in Mode B, force-follow.
                    tracing::warn!(
                        alias = %record.address,
                        ?actual,
                        remote_t = record.commit_t,
                        "Local commit diverged from server, force-following"
                    );
                    self.fluree.disconnect_ledger(&record.address).await;
                    if let Some(ref cur) = actual {
                        if cur.t > record.commit_t {
                            // This should never happen in peer Mode B (writes are forwarded),
                            // but if it does, we cannot "force-follow" due to the strict
                            // monotonic guard for commit refs (new.t must be > current.t).
                            tracing::error!(
                                alias = %record.address,
                                local_t = cur.t,
                                remote_t = record.commit_t,
                                "Local commit_t is ahead of remote; refusing to force-follow"
                            );
                            return;
                        }
                    }
                    let force_result = ns
                        .compare_and_set_ref(
                            &record.address,
                            RefKind::CommitHead,
                            actual.as_ref(),
                            &commit_ref,
                        )
                        .await;

                    match force_result {
                        Ok(CasResult::Updated) => {
                            tracing::info!(
                                alias = %record.address,
                                commit_t = record.commit_t,
                                "Force-follow CAS updated local commit head"
                            );
                        }
                        Ok(CasResult::Conflict { actual: new_actual }) => {
                            tracing::warn!(
                                alias = %record.address,
                                ?new_actual,
                                remote_t = record.commit_t,
                                "Force-follow CAS still conflicted (local may remain divergent)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                alias = %record.address,
                                error = %e,
                                "Force-follow CAS failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        alias = %record.address,
                        error = %e,
                        "Failed to fast-forward commit"
                    );
                    return;
                }
            }
        }

        // 3. Update index head — read current first, NOT expected=None
        if record.index_address.is_some() {
            let index_ref = RefValue {
                address: record.index_address.clone(),
                t: record.index_t,
            };
            let current = ns
                .get_ref(&record.address, RefKind::IndexHead)
                .await
                .ok()
                .flatten();
            let index_result = ns
                .compare_and_set_ref(
                    &record.address,
                    RefKind::IndexHead,
                    current.as_ref(),
                    &index_ref,
                )
                .await;
            match index_result {
                Ok(CasResult::Updated) => {}
                Ok(CasResult::Conflict { actual }) => {
                    if let Some(ref cur) = actual {
                        if cur.t > record.index_t {
                            tracing::warn!(
                                alias = %record.address,
                                local_index_t = cur.t,
                                remote_index_t = record.index_t,
                                "Local index_t is ahead of remote; index ref not updated"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        alias = %record.address,
                        error = %e,
                        "Failed to update index ref"
                    );
                }
            }
        }

        // 4. Update in-memory watermarks (AFTER persisting to NS)
        let changed = self
            .peer_state
            .update_ledger(
                &record.address,
                record.commit_t,
                record.index_t,
                record.commit_address.clone(),
                record.index_address.clone(),
            )
            .await;

        if changed {
            tracing::info!(
                alias = %record.address,
                commit_t = record.commit_t,
                index_t = record.index_t,
                "Remote ledger watermark updated (persisted to local NS)"
            );
        }

        // 5. Notify LedgerManager (AFTER NS is updated, so reload sees new refs)
        self.refresh_cached_ledger(record).await;
    }

    /// Retract ledger locally and evict from cache.
    async fn handle_ledger_retracted(&self, alias: &str) {
        // 1. Retract via Publisher::retract()
        let ns = self.fluree.nameservice();
        if let Err(e) = ns.retract(alias).await {
            tracing::warn!(
                alias = %alias,
                error = %e,
                "Failed to retract ledger locally"
            );
        }

        // 2. Clear in-memory watermarks
        self.peer_state.remove_ledger(alias).await;

        // 3. Evict from cache
        self.fluree.disconnect_ledger(alias).await;

        tracing::info!(alias = %alias, "Ledger retracted from remote");
    }

    /// Notify LedgerManager to refresh a cached ledger from the NS update.
    async fn refresh_cached_ledger(&self, record: &NsRecord) {
        let Some(mgr) = self.fluree.ledger_manager() else {
            return;
        };

        match mgr
            .notify(NsNotify {
                alias: record.address.clone(),
                record: Some(record.clone()),
            })
            .await
        {
            Ok(NotifyResult::NotLoaded) => {
                // Not cached — do not cold-load on events
            }
            Ok(NotifyResult::Current) => {
                // Already up to date
            }
            Ok(
                result @ (NotifyResult::Reloaded
                | NotifyResult::IndexUpdated
                | NotifyResult::CommitApplied),
            ) => {
                tracing::info!(
                    alias = %record.address,
                    ?result,
                    "Refreshed cached ledger from SSE update"
                );
            }
            Err(e) => {
                tracing::warn!(
                    alias = %record.address,
                    error = %e,
                    "Failed to refresh cached ledger from SSE update"
                );
            }
        }
    }

    /// Preload explicitly configured ledgers into the cache.
    async fn preload_configured_ledgers(&self) {
        let sub = self.config.peer_subscription();
        if sub.all || sub.ledgers.is_empty() {
            return;
        }

        for alias in &sub.ledgers {
            match self.fluree.ledger_cached(alias).await {
                Ok(_) => {
                    tracing::info!(alias = %alias, "Preloaded ledger into peer cache");
                }
                Err(e) => {
                    tracing::warn!(
                        alias = %alias,
                        error = %e,
                        "Failed to preload ledger"
                    );
                }
            }
        }
    }
}

/// Compute a config hash for graph source change detection.
///
/// Uses SHA-256 truncated to 8 hex chars (4 bytes) to match the server's graph source SSE
/// event ID format. Same algorithm as `GraphSourceRecord::config_hash()` in `fluree-db-peer`.
fn graph_source_config_hash(config: &str) -> String {
    use sha2::{Digest, Sha256};

    let hash = Sha256::digest(config.as_bytes());
    hex::encode(&hash[..4])
}
