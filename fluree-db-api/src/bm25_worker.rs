//! Background BM25 maintenance worker
//!
//! This module provides a background worker that automatically syncs BM25 indexes
//! when their source ledgers are updated. It subscribes to nameservice events and
//! triggers sync operations for dependent virtual graphs.
//!
//! # Architecture
//!
//! The worker maintains a reverse dependency map (ledger -> VGs) and subscribes
//! to nameservice events. When a `LedgerCommitPublished` event is received, it
//! enqueues sync tasks for all dependent VGs.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::{FlureeBuilder, Bm25MaintenanceWorker};
//!
//! let fluree = FlureeBuilder::memory().build_memory();
//!
//! // Start the maintenance worker
//! let worker = Bm25MaintenanceWorker::new(&fluree);
//! let handle = worker.start().await?;
//!
//! // Register a VG for automatic sync
//! handle.register_vg("my-search:main").await?;
//!
//! // Stop the worker when done
//! handle.stop().await;
//! ```

use crate::{ApiError, Result};
use fluree_db_core::{Storage, StorageWrite};
use fluree_db_nameservice::{
    NameService, NameServiceEvent, Publication, Publisher, VirtualGraphPublisher,
};
use futures::StreamExt;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, warn};

/// Configuration for the BM25 maintenance worker.
#[derive(Debug, Clone)]
pub struct Bm25WorkerConfig {
    /// Maximum number of concurrent sync operations.
    pub max_concurrent_syncs: usize,
    /// Whether to auto-register VGs on creation.
    pub auto_register: bool,
    /// Debounce interval in milliseconds (delay sync to batch rapid commits).
    pub debounce_ms: u64,
}

impl Default for Bm25WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_syncs: 4,
            auto_register: true,
            debounce_ms: 100,
        }
    }
}

/// Statistics for the maintenance worker.
#[derive(Debug, Clone, Default)]
pub struct Bm25WorkerStats {
    /// Total number of sync operations performed.
    pub syncs_performed: u64,
    /// Number of sync operations that failed.
    pub syncs_failed: u64,
    /// Number of events received.
    pub events_received: u64,
    /// Number of registered VGs.
    pub registered_vgs: usize,
}

/// State for the BM25 maintenance worker (single-threaded).
///
/// Uses `RefCell` for interior mutability to work in single-threaded contexts.
pub struct Bm25WorkerState {
    /// Reverse dependency map: ledger_alias -> set of vg_aliases.
    ledger_to_vgs: HashMap<String, HashSet<String>>,
    /// Forward map: vg_alias -> set of ledger_aliases (for unregistration).
    vg_to_ledgers: HashMap<String, HashSet<String>>,
    /// Statistics.
    stats: Bm25WorkerStats,
}

impl Bm25WorkerState {
    /// Create a new empty worker state.
    pub fn new() -> Self {
        Self {
            ledger_to_vgs: HashMap::new(),
            vg_to_ledgers: HashMap::new(),
            stats: Bm25WorkerStats::default(),
        }
    }

    /// Register a VG with its dependencies.
    pub fn register_vg(&mut self, vg_alias: &str, dependencies: &[String]) {
        let deps_set: HashSet<String> = dependencies.iter().cloned().collect();

        // Update forward map
        self.vg_to_ledgers
            .insert(vg_alias.to_string(), deps_set.clone());

        // Update reverse map
        for ledger in &deps_set {
            self.ledger_to_vgs
                .entry(ledger.clone())
                .or_default()
                .insert(vg_alias.to_string());
        }

        self.stats.registered_vgs = self.vg_to_ledgers.len();
        debug!(vg_alias, ?dependencies, "Registered VG for maintenance");
    }

    /// Unregister a VG.
    pub fn unregister_vg(&mut self, vg_alias: &str) {
        if let Some(ledgers) = self.vg_to_ledgers.remove(vg_alias) {
            // Remove from reverse map
            for ledger in ledgers {
                if let Some(vgs) = self.ledger_to_vgs.get_mut(&ledger) {
                    vgs.remove(vg_alias);
                    if vgs.is_empty() {
                        self.ledger_to_vgs.remove(&ledger);
                    }
                }
            }
        }
        self.stats.registered_vgs = self.vg_to_ledgers.len();
        debug!(vg_alias, "Unregistered VG from maintenance");
    }

    /// Get VGs that depend on a ledger.
    pub fn vgs_for_ledger(&self, ledger_alias: &str) -> Vec<String> {
        self.ledger_to_vgs
            .get(ledger_alias)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all registered VGs.
    pub fn registered_vgs(&self) -> Vec<String> {
        self.vg_to_ledgers.keys().cloned().collect()
    }

    /// Get all watched ledgers.
    pub fn watched_ledgers(&self) -> Vec<String> {
        self.ledger_to_vgs.keys().cloned().collect()
    }

    /// Record a sync operation.
    pub fn record_sync(&mut self, success: bool) {
        self.stats.syncs_performed += 1;
        if !success {
            self.stats.syncs_failed += 1;
        }
    }

    /// Record an event.
    pub fn record_event(&mut self) {
        self.stats.events_received += 1;
    }

    /// Get current stats.
    pub fn stats(&self) -> &Bm25WorkerStats {
        &self.stats
    }
}

impl Default for Bm25WorkerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to interact with a running BM25 maintenance worker.
///
/// This handle allows registering/unregistering VGs and stopping the worker.
pub struct Bm25WorkerHandle {
    state: Rc<RefCell<Bm25WorkerState>>,
    /// Signal to stop the worker (set to true to request stop).
    stop_requested: Rc<RefCell<bool>>,
}

impl Bm25WorkerHandle {
    /// Register a VG for automatic maintenance.
    ///
    /// The worker will sync this VG whenever any of its source ledgers are updated.
    pub async fn register_vg<N: NameService + VirtualGraphPublisher>(
        &self,
        ns: &N,
        vg_alias: &str,
    ) -> Result<()> {
        // Look up VG to get its dependencies
        let record = ns
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        self.state
            .borrow_mut()
            .register_vg(vg_alias, &record.dependencies);
        Ok(())
    }

    /// Register a VG with explicit dependencies (no nameservice lookup).
    pub fn register_vg_with_deps(&self, vg_alias: &str, dependencies: &[String]) {
        self.state.borrow_mut().register_vg(vg_alias, dependencies);
    }

    /// Unregister a VG from automatic maintenance.
    pub fn unregister_vg(&self, vg_alias: &str) {
        self.state.borrow_mut().unregister_vg(vg_alias);
    }

    /// Get current worker statistics.
    pub fn stats(&self) -> Bm25WorkerStats {
        self.state.borrow().stats().clone()
    }

    /// Get all registered VGs.
    pub fn registered_vgs(&self) -> Vec<String> {
        self.state.borrow().registered_vgs()
    }

    /// Request the worker to stop.
    pub fn stop(&self) {
        *self.stop_requested.borrow_mut() = true;
        info!("BM25 maintenance worker stop requested");
    }

    /// Check if stop has been requested.
    pub fn is_stop_requested(&self) -> bool {
        *self.stop_requested.borrow()
    }
}

/// BM25 maintenance worker.
///
/// Monitors nameservice events and automatically syncs BM25 indexes when their
/// source ledgers are updated.
pub struct Bm25MaintenanceWorker<'a, S: Storage + 'static, N> {
    fluree: &'a crate::Fluree<S, crate::SimpleCache, N>,
    config: Bm25WorkerConfig,
    state: Rc<RefCell<Bm25WorkerState>>,
    stop_requested: Rc<RefCell<bool>>,
}

impl<'a, S, N> Bm25MaintenanceWorker<'a, S, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + VirtualGraphPublisher + Publication,
{
    /// Create a new maintenance worker.
    pub fn new(fluree: &'a crate::Fluree<S, crate::SimpleCache, N>) -> Self {
        Self {
            fluree,
            config: Bm25WorkerConfig::default(),
            state: Rc::new(RefCell::new(Bm25WorkerState::new())),
            stop_requested: Rc::new(RefCell::new(false)),
        }
    }

    /// Create a new maintenance worker with custom config.
    pub fn with_config(
        fluree: &'a crate::Fluree<S, crate::SimpleCache, N>,
        config: Bm25WorkerConfig,
    ) -> Self {
        Self {
            fluree,
            config,
            state: Rc::new(RefCell::new(Bm25WorkerState::new())),
            stop_requested: Rc::new(RefCell::new(false)),
        }
    }

    /// Get a handle to interact with the worker.
    pub fn handle(&self) -> Bm25WorkerHandle {
        Bm25WorkerHandle {
            state: self.state.clone(),
            stop_requested: self.stop_requested.clone(),
        }
    }

    /// Process a single nameservice event.
    ///
    /// Returns the list of VG aliases that need syncing.
    pub fn process_event(&self, event: &NameServiceEvent) -> Vec<String> {
        self.state.borrow_mut().record_event();

        match event {
            NameServiceEvent::LedgerCommitPublished {
                alias, commit_t, ..
            } => {
                let vgs = self.state.borrow().vgs_for_ledger(alias);
                if !vgs.is_empty() {
                    info!(
                        ledger = %alias,
                        commit_t,
                        vg_count = vgs.len(),
                        "Ledger commit triggers VG sync"
                    );
                }
                vgs
            }
            NameServiceEvent::LedgerIndexPublished { alias, index_t, .. } => {
                // Index updates don't require VG sync (commit already triggered it)
                debug!(ledger = %alias, index_t, "Ledger index published (no VG sync needed)");
                vec![]
            }
            NameServiceEvent::VgConfigPublished {
                alias,
                dependencies,
                ..
            } => {
                // Auto-register VG if configured
                if self.config.auto_register {
                    self.state.borrow_mut().register_vg(alias, dependencies);
                    info!(vg = %alias, "Auto-registered VG for maintenance");
                }
                vec![]
            }
            NameServiceEvent::VgRetracted { alias } => {
                // Unregister retracted VG
                self.state.borrow_mut().unregister_vg(alias);
                info!(vg = %alias, "Unregistered retracted VG");
                vec![]
            }
            _ => vec![], // Other events don't trigger sync
        }
    }

    /// Sync a single VG (called by the event loop).
    pub async fn sync_vg(&self, vg_alias: &str) -> Result<()> {
        debug!(vg = %vg_alias, "Syncing VG");

        match self.fluree.sync_bm25_index(vg_alias).await {
            Ok(result) => {
                self.state.borrow_mut().record_sync(true);
                info!(
                    vg = %vg_alias,
                    upserted = result.upserted,
                    removed = result.removed,
                    new_watermark = result.new_watermark,
                    "VG sync completed"
                );
                Ok(())
            }
            Err(e) => {
                self.state.borrow_mut().record_sync(false);
                error!(vg = %vg_alias, error = %e, "VG sync failed");
                Err(e)
            }
        }
    }

    /// Run the maintenance loop.
    ///
    /// This subscribes to nameservice events and processes them until stopped.
    /// The worker uses `Rc<RefCell<...>>` internally, so it must be run on a
    /// single-threaded runtime or via `spawn_local`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tokio::task::LocalSet;
    ///
    /// let worker = Bm25MaintenanceWorker::new(&fluree);
    /// let handle = worker.handle();
    ///
    /// // Option 1: Run in a LocalSet (multi-threaded runtime)
    /// let local = LocalSet::new();
    /// local.spawn_local(async move {
    ///     worker.run().await.ok();
    /// });
    ///
    /// // Option 2: Run on current-thread runtime
    /// // tokio::spawn_local(async move { worker.run().await.ok(); });
    ///
    /// // Later, stop the worker
    /// handle.stop();
    /// ```
    pub async fn run(&self) -> Result<()> {
        info!("Starting BM25 maintenance worker");

        // Subscribe to all nameservice events (ledger and VG changes).
        let mut subscription = self
            .fluree
            .nameservice()
            .subscribe(fluree_db_nameservice::SubscriptionScope::All)
            .await?;

        // Debounced batching: we accumulate VGs to sync and flush them after `debounce_ms`.
        let mut pending: HashSet<String> = HashSet::new();
        let mut next_flush: Option<Instant> = None;

        // In-flight syncs (bounded by config.max_concurrent_syncs).
        let mut in_flight: futures::stream::FuturesUnordered<
            std::pin::Pin<Box<dyn std::future::Future<Output = (String, Result<()>)>>>,
        > = futures::stream::FuturesUnordered::new();

        loop {
            // Check for stop request
            if *self.stop_requested.borrow() {
                info!("BM25 maintenance worker stopping");
                break;
            }

            // Flush pending syncs if debounce timer elapsed and we have capacity.
            let now = Instant::now();
            let can_flush = next_flush.map(|t| now >= t).unwrap_or(false);
            if can_flush {
                while in_flight.len() < self.config.max_concurrent_syncs {
                    let Some(vg_alias) = pending.iter().next().cloned() else {
                        break;
                    };
                    pending.remove(&vg_alias);

                    // Spawn a non-Send future into our in-flight set (polled on this task).
                    let fut = async move {
                        let res = self.sync_vg(&vg_alias).await;
                        (vg_alias, res)
                    };
                    in_flight.push(Box::pin(fut));
                }

                // If we've drained pending, clear flush deadline; otherwise keep flushing.
                if pending.is_empty() {
                    next_flush = None;
                } else {
                    next_flush =
                        Some(Instant::now() + Duration::from_millis(self.config.debounce_ms));
                }
            }

            // Compute a sleep duration: either until next flush or a small tick for stop checks.
            let sleep_until =
                next_flush.unwrap_or_else(|| Instant::now() + Duration::from_millis(100));
            let sleep_fut = time::sleep_until(sleep_until);
            tokio::pin!(sleep_fut);

            tokio::select! {
                biased;

                // Prefer stop checks + flushing, but still service events promptly.
                res = subscription.receiver.recv() => {
                    match res {
                        Ok(event) => {
                            let vgs_to_sync = self.process_event(&event);
                            if !vgs_to_sync.is_empty() {
                                for vg in vgs_to_sync {
                                    pending.insert(vg);
                                }
                                next_flush = Some(Instant::now() + Duration::from_millis(self.config.debounce_ms));
                            }
                        }
                        Err(e) => {
                            // Broadcast channel lagged or closed
                            warn!(error = %e, "Event channel error, resubscribing");
                            subscription = self
                                .fluree
                                .nameservice()
                                .subscribe(fluree_db_nameservice::SubscriptionScope::All)
                                .await?;
                        }
                    }
                }

                // Complete one in-flight sync.
                Some((vg_alias, res)) = in_flight.next() => {
                    if let Err(e) = res {
                        warn!(vg = %vg_alias, error = %e, "Failed to sync VG");
                    }
                }

                // Debounce tick / stop-check tick
                _ = &mut sleep_fut => {}
            }
        }

        info!("BM25 maintenance worker stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_state_register_vg() {
        let mut state = Bm25WorkerState::new();

        state.register_vg(
            "search:main",
            &["ledger1:main".to_string(), "ledger2:main".to_string()],
        );

        assert_eq!(state.registered_vgs(), vec!["search:main"]);
        assert!(state
            .watched_ledgers()
            .contains(&"ledger1:main".to_string()));
        assert!(state
            .watched_ledgers()
            .contains(&"ledger2:main".to_string()));

        let vgs = state.vgs_for_ledger("ledger1:main");
        assert_eq!(vgs, vec!["search:main"]);
    }

    #[test]
    fn test_worker_state_unregister_vg() {
        let mut state = Bm25WorkerState::new();

        state.register_vg("search:main", &["ledger1:main".to_string()]);
        state.register_vg("other:main", &["ledger1:main".to_string()]);

        // Both VGs depend on ledger1
        let vgs = state.vgs_for_ledger("ledger1:main");
        assert_eq!(vgs.len(), 2);

        // Unregister one
        state.unregister_vg("search:main");

        let vgs = state.vgs_for_ledger("ledger1:main");
        assert_eq!(vgs, vec!["other:main"]);

        // Unregister the other
        state.unregister_vg("other:main");

        let vgs = state.vgs_for_ledger("ledger1:main");
        assert!(vgs.is_empty());
        assert!(state.watched_ledgers().is_empty());
    }

    #[test]
    fn test_worker_state_multiple_dependencies() {
        let mut state = Bm25WorkerState::new();

        // VG1 depends on ledger1 and ledger2
        state.register_vg(
            "vg1:main",
            &["ledger1:main".to_string(), "ledger2:main".to_string()],
        );
        // VG2 depends on ledger2 and ledger3
        state.register_vg(
            "vg2:main",
            &["ledger2:main".to_string(), "ledger3:main".to_string()],
        );

        // ledger1 triggers only vg1
        let vgs = state.vgs_for_ledger("ledger1:main");
        assert_eq!(vgs, vec!["vg1:main"]);

        // ledger2 triggers both
        let mut vgs = state.vgs_for_ledger("ledger2:main");
        vgs.sort();
        assert_eq!(vgs, vec!["vg1:main", "vg2:main"]);

        // ledger3 triggers only vg2
        let vgs = state.vgs_for_ledger("ledger3:main");
        assert_eq!(vgs, vec!["vg2:main"]);
    }

    #[test]
    fn test_worker_stats() {
        let mut state = Bm25WorkerState::new();

        state.register_vg("vg:main", &["ledger:main".to_string()]);
        assert_eq!(state.stats().registered_vgs, 1);

        state.record_event();
        state.record_event();
        assert_eq!(state.stats().events_received, 2);

        state.record_sync(true);
        state.record_sync(false);
        assert_eq!(state.stats().syncs_performed, 2);
        assert_eq!(state.stats().syncs_failed, 1);
    }
}
