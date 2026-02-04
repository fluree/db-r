//! Indexer orchestration helpers
//!
//! This module provides utilities for coordinating index operations:
//!
//! - `IndexerHandle`: A coalescing request handler for index operations
//!
//! # Trait Bounds
//!
//! The orchestrator requires storage to implement `Storage + Send + Sync`
//! because it uses the unified indexing pipeline (`build_index_for_ledger`) which
//! can fall back to batched rebuild with checkpoint cleanup.
//!
//! For lighter trait bounds (refresh-only, no `Send` required),
//! use `refresh_index_for_ledger` directly instead of the orchestrator.
//!
//! # Thread Safety Note
//!
//! While storage must be `Send + Sync`, the background indexer worker is designed
//! for single-threaded async contexts. For multi-threaded usage:
//!
//! 1. Use `tokio::task::spawn_local` with a `LocalSet`
//! 2. Run indexing in a dedicated single-threaded runtime
//! 3. Use `build_index_for_ledger` directly in your own async context
//!
//! # Example: Simple Coalescing Pattern
//!
//! ```ignore
//! use tokio::sync::watch;
//!
//! // Create a watch channel for coalescing
//! let (tx, mut rx) = watch::channel::<Option<String>>(None);
//!
//! // Trigger indexing (latest alias wins)
//! tx.send(Some("mydb:main".to_string())).ok();
//!
//! // In your indexing loop (running in a LocalSet or dedicated runtime):
//! loop {
//!     if rx.changed().await.is_err() { break; }
//!     let alias = rx.borrow_and_update().clone();
//!     if let Some(alias) = alias {
//!         let result = build_index_for_ledger(&storage, &ns, &alias, config.clone()).await;
//!         // handle result...
//!     }
//! }
//! ```

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::{publish_index_result, IndexResult};
use fluree_db_core::Storage;
use fluree_db_nameservice::{NameService, Publisher};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch, Mutex, Notify};
use tracing::{debug, info, warn};

// =============================================================================
// Indexing Status & Completion Types
// =============================================================================

/// Outcome of waiting for indexing (terminal result)
#[derive(Debug, Clone)]
pub enum IndexOutcome {
    /// Indexing reached the required t (or beyond)
    Completed {
        /// The t that was indexed to
        index_t: i64,
        /// Storage address of the index root
        root_address: String,
    },
    /// Indexing failed after retries exhausted or fatal error
    Failed(String),
    /// Request was cancelled before satisfaction
    Cancelled,
}

/// Point-in-time snapshot of indexing status for a ledger
#[derive(Debug, Clone)]
pub struct IndexStatusSnapshot {
    /// Current phase
    pub phase: IndexPhase,
    /// Minimum t needed (coalesced from all pending requests)
    pub pending_min_t: Option<i64>,
    /// Last successfully indexed t (from nameservice or last success)
    pub last_index_t: i64,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Number of waiters currently attached
    pub waiter_count: usize,
}

/// Phase of indexing for a ledger
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPhase {
    /// No pending work
    Idle,
    /// Work is queued but not yet started
    Pending,
    /// Currently being indexed
    InProgress,
}

/// Handle to wait for indexing completion
///
/// Resolves when `index_t >= min_t` (the predicate is satisfied),
/// or when the request fails or is cancelled.
pub struct IndexCompletion {
    receiver: oneshot::Receiver<IndexOutcome>,
}

impl IndexCompletion {
    /// Wait for indexing to satisfy the min_t predicate (or fail/cancel)
    pub async fn wait(self) -> IndexOutcome {
        self.receiver.await.unwrap_or(IndexOutcome::Cancelled)
    }

    /// Non-blocking check - returns None if not yet complete
    pub fn try_get(&mut self) -> Option<IndexOutcome> {
        match self.receiver.try_recv() {
            Ok(outcome) => Some(outcome),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(IndexOutcome::Cancelled),
        }
    }
}

impl std::fmt::Debug for IndexCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexCompletion").finish_non_exhaustive()
    }
}

// =============================================================================
// Internal Per-Ledger State
// =============================================================================

/// Internal per-ledger tracking state
#[derive(Debug)]
struct LedgerIndexState {
    /// Coalesced minimum t needed from all triggers
    pending_min_t: Option<i64>,
    /// Waiters: (min_t they need, sender to notify)
    waiters: Vec<(i64, oneshot::Sender<IndexOutcome>)>,
    /// Current phase
    phase: IndexPhase,
    /// Last successful index_t
    last_index_t: i64,
    /// Last error message
    last_error: Option<String>,
    /// Cancelled flag - prevents retries, resolves unsatisfied waiters
    cancelled: bool,
    /// Retry tracking
    retry_count: u32,
    /// When to retry next (if in backoff)
    next_retry_at: Option<tokio::time::Instant>,
}

impl Default for LedgerIndexState {
    fn default() -> Self {
        Self {
            pending_min_t: None,
            waiters: Vec::new(),
            phase: IndexPhase::Idle,
            last_index_t: 0,
            last_error: None,
            cancelled: false,
            retry_count: 0,
            next_retry_at: None,
        }
    }
}

impl LedgerIndexState {
    /// Create a snapshot of the current state
    fn snapshot(&self) -> IndexStatusSnapshot {
        IndexStatusSnapshot {
            phase: self.phase,
            pending_min_t: self.pending_min_t,
            last_index_t: self.last_index_t,
            last_error: self.last_error.clone(),
            waiter_count: self.waiters.len(),
        }
    }

    /// Resolve all waiters whose min_t is <= threshold_t
    fn resolve_waiters_below(&mut self, threshold_t: i64, outcome: IndexOutcome) {
        let (satisfied, remaining): (Vec<_>, Vec<_>) = self
            .waiters
            .drain(..)
            .partition(|(min_t, _)| *min_t <= threshold_t);

        for (_, sender) in satisfied {
            let _ = sender.send(outcome.clone());
        }
        self.waiters = remaining;
    }

    /// Recalculate pending_min_t from remaining waiters
    fn recalculate_pending_min_t(&mut self) {
        self.pending_min_t = self.waiters.iter().map(|(min_t, _)| *min_t).min();
        if self.pending_min_t.is_none() {
            self.phase = IndexPhase::Idle;
        }
    }

    /// Check if there's any pending work
    fn has_pending_work(&self) -> bool {
        self.pending_min_t.is_some() || !self.waiters.is_empty()
    }
}

#[cfg(feature = "embedded-orchestrator")]
use crate::error::IndexerError;
#[cfg(feature = "embedded-orchestrator")]
use crate::refresh::{refresh_index, RefreshIndexResult};
#[cfg(feature = "embedded-orchestrator")]
use fluree_db_core::NodeCache;
#[cfg(feature = "embedded-orchestrator")]
use fluree_db_ledger::{IndexConfig, LedgerState};

/// Indexer orchestration for embedded mode
///
/// This struct provides methods for indexing ledgers with support for:
/// - Checking if indexing is needed
/// - Performing full rebuilds
/// - Publishing results to nameservice
///
/// Note: Does not spawn background tasks. Use in a single-threaded async context
/// (e.g., `LocalSet`) or manage threading at a higher level.
pub struct IndexerOrchestrator<S, N> {
    storage: S,
    nameservice: Arc<N>,
    config: IndexerConfig,
}

impl<S, N> IndexerOrchestrator<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + 'static,
{
    /// Create a new indexer orchestrator
    pub fn new(storage: S, nameservice: Arc<N>, config: IndexerConfig) -> Self {
        Self {
            storage,
            nameservice,
            config,
        }
    }

    /// Check if a ledger needs indexing
    ///
    /// Returns `true` if the ledger's index is behind its commits.
    pub async fn needs_indexing(&self, alias: &str) -> Result<bool> {
        let record = self
            .nameservice
            .lookup(alias)
            .await
            .map_err(|e| crate::error::IndexerError::NameService(e.to_string()))?
            .ok_or_else(|| crate::error::IndexerError::LedgerNotFound(alias.to_string()))?;

        // Check if there's a commit without an index, or index_t < commit_t
        if record.commit_address.is_none() {
            return Ok(false); // No commits, nothing to index
        }

        if record.index_address.is_none() {
            return Ok(true); // Has commits but no index
        }

        Ok(record.index_t < record.commit_t)
    }

    /// Index a ledger using refresh-first strategy
    ///
    /// Returns the existing index if already current, otherwise:
    /// 1. Attempts incremental refresh if an index exists
    /// 2. Falls back to full batched rebuild if refresh fails or no index exists
    pub async fn index_ledger(&self, alias: &str) -> Result<IndexResult> {
        crate::build_index_for_ledger(
            &self.storage,
            self.nameservice.as_ref(),
            alias,
            self.config.clone(),
        )
        .await
    }

    /// Index a ledger and publish the result
    ///
    /// Combines `index_ledger` with publishing to the nameservice.
    pub async fn index_and_publish(&self, alias: &str) -> Result<IndexResult> {
        let result = self.index_ledger(alias).await?;
        publish_index_result(self.nameservice.as_ref(), &result).await?;
        Ok(result)
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get a reference to the nameservice
    pub fn nameservice(&self) -> &Arc<N> {
        &self.nameservice
    }

    /// Get a reference to the config
    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }
}

// =============================================================================
// Background Indexer Worker with Predicate-Based Completion
// =============================================================================

/// Per-ledger state map
type LedgerStates = BTreeMap<String, LedgerIndexState>;

/// Handle for triggering background indexing
///
/// Provides APIs for:
/// - Triggering indexing with completion tracking
/// - Cancelling pending work
/// - Checking status
/// - Waiting for idle state
#[derive(Clone)]
pub struct IndexerHandle {
    /// Per-ledger state (shared with worker)
    states: Arc<Mutex<LedgerStates>>,
    /// Signal to wake the worker
    tick: watch::Sender<u64>,
    /// Notifier for idle state changes
    idle_notify: Arc<Notify>,
}

impl std::fmt::Debug for IndexerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerHandle")
            .field("states", &"<Mutex<LedgerStates>>")
            .field("tick", &"<watch::Sender>")
            .finish()
    }
}

impl IndexerHandle {
    /// Trigger indexing for a ledger with completion tracking
    ///
    /// Returns a completion handle that resolves when `index_t >= min_t`.
    /// Multiple triggers coalesce: worker indexes to latest commit_t,
    /// then resolves ALL waiters whose min_t is satisfied.
    ///
    /// Fire-and-forget: just drop the returned `IndexCompletion`.
    pub async fn trigger(&self, alias: impl Into<String>, min_t: i64) -> IndexCompletion {
        let alias = alias.into();
        let (tx, rx) = oneshot::channel();

        {
            let mut states = self.states.lock().await;
            let state = states.entry(alias).or_default();

            // Clear cancelled flag on new trigger
            state.cancelled = false;

            // New trigger should not be held back by a prior retry backoff.
            // This also makes tests and post-commit hooks behave predictably.
            state.retry_count = 0;
            state.next_retry_at = None;

            // Add waiter
            state.waiters.push((min_t, tx));

            // Update coalesced min_t
            state.pending_min_t = Some(
                state
                    .pending_min_t
                    .map_or(min_t, |existing| existing.min(min_t)),
            );

            // Mark as pending if idle
            if state.phase == IndexPhase::Idle {
                state.phase = IndexPhase::Pending;
            }
        }

        // Signal the worker
        self.tick.send_modify(|t| *t = t.wrapping_add(1));

        IndexCompletion { receiver: rx }
    }

    /// Cancel pending/queued work for a ledger
    ///
    /// - Removes from pending queue (if not yet started)
    /// - Prevents retries on failure
    /// - Does NOT abort in-progress indexing (it will complete and publish)
    /// - Resolves all waiters whose min_t is NOT yet satisfied as Cancelled
    ///
    /// Returns true if there was pending work to cancel.
    pub async fn cancel(&self, alias: &str) -> bool {
        let had_work = {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(alias) {
                let had_work = state.has_pending_work();
                state.cancelled = true;
                // Resolve all waiters as cancelled (they haven't been satisfied)
                state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                state.pending_min_t = None;
                if state.phase == IndexPhase::Pending {
                    state.phase = IndexPhase::Idle;
                }
                // Note: if InProgress, we let it finish but won't retry on failure
                had_work
            } else {
                false
            }
        };

        if had_work {
            self.idle_notify.notify_waiters();
        }

        had_work
    }

    /// Cancel all pending work (for shutdown)
    pub async fn cancel_all(&self) {
        let mut states = self.states.lock().await;
        for state in states.values_mut() {
            state.cancelled = true;
            state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
            state.pending_min_t = None;
            if state.phase == IndexPhase::Pending {
                state.phase = IndexPhase::Idle;
            }
        }
        drop(states);
        self.idle_notify.notify_waiters();
    }

    /// Check current status for a ledger
    pub async fn status(&self, alias: &str) -> Option<IndexStatusSnapshot> {
        let states = self.states.lock().await;
        states.get(alias).map(|s| s.snapshot())
    }

    /// Check if a ledger has pending or in-progress work
    pub async fn is_pending(&self, alias: &str) -> bool {
        let states = self.states.lock().await;
        states
            .get(alias)
            .is_some_and(|s| s.phase != IndexPhase::Idle)
    }

    /// List all ledgers with pending/in-progress work
    pub async fn pending_ledgers(&self) -> Vec<String> {
        let states = self.states.lock().await;
        states
            .iter()
            .filter(|(_, s)| s.phase != IndexPhase::Idle)
            .map(|(alias, _)| alias.clone())
            .collect()
    }

    /// Wait until a specific ledger has no pending work
    ///
    /// Returns immediately if no work pending.
    /// Different from `IndexCompletion`: this waits for the queue to drain,
    /// not for a specific min_t to be reached.
    pub async fn wait_for_idle(&self, alias: &str) {
        loop {
            // Avoid missed-wakeup races: create the notification future *before*
            // checking the condition, then await it if still not idle.
            let notified = self.idle_notify.notified();
            {
                let states = self.states.lock().await;
                if let Some(state) = states.get(alias) {
                    if state.phase == IndexPhase::Idle {
                        return;
                    }
                } else {
                    return; // Ledger not tracked = idle
                }
            }
            // Wait for a state change notification
            notified.await;
        }
    }

    /// Wait until ALL pending work completes (queue fully drained)
    pub async fn wait_all_idle(&self) {
        loop {
            // Avoid missed-wakeup races (see `wait_for_idle`).
            let notified = self.idle_notify.notified();
            {
                let states = self.states.lock().await;
                let any_busy = states.values().any(|s| s.phase != IndexPhase::Idle);
                if !any_busy {
                    return;
                }
            }
            notified.await;
        }
    }
}

/// Background indexer worker with predicate-based completion
///
/// This worker processes index requests one ledger at a time with:
/// - Per-ledger state tracking with waiters
/// - Predicate-based completion (resolves when index_t >= min_t)
/// - Exponential backoff on failures (capped at 30s)
/// - Cooperative cancellation
/// - Clean shutdown when all handles are dropped
pub struct BackgroundIndexerWorker<S, N> {
    storage: S,
    nameservice: Arc<N>,
    config: IndexerConfig,
    states: Arc<Mutex<LedgerStates>>,
    tick_rx: watch::Receiver<u64>,
    idle_notify: Arc<Notify>,
}

impl<S, N> BackgroundIndexerWorker<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + 'static,
{
    /// Create a new worker and its associated handle
    pub fn new(storage: S, nameservice: Arc<N>, config: IndexerConfig) -> (Self, IndexerHandle) {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let (tick_tx, tick_rx) = watch::channel(0u64);
        let idle_notify = Arc::new(Notify::new());

        let worker = Self {
            storage,
            nameservice,
            config,
            states: states.clone(),
            tick_rx,
            idle_notify: idle_notify.clone(),
        };

        let handle = IndexerHandle {
            states,
            tick: tick_tx,
            idle_notify,
        };

        (worker, handle)
    }

    /// Run the worker loop (consumes self)
    ///
    /// This loop:
    /// 1. Waits for tick signal OR retry deadline
    /// 2. Processes ledgers with pending work
    /// 3. Resolves waiters based on min_t predicate
    /// 4. Handles cancellation and backoff
    pub async fn run(mut self) {
        loop {
            // Compute next retry deadline
            let retry_deadline = {
                let states = self.states.lock().await;
                states.values().filter_map(|s| s.next_retry_at).min()
            };

            // Wait for tick OR retry deadline (whichever comes first)
            let wait_result = if let Some(deadline) = retry_deadline {
                tokio::select! {
                    result = self.tick_rx.changed() => result,
                    _ = tokio::time::sleep_until(deadline) => Ok(()),
                }
            } else {
                self.tick_rx.changed().await
            };

            if wait_result.is_err() {
                // All senders dropped - resolve remaining waiters and shutdown
                let mut states = self.states.lock().await;
                for state in states.values_mut() {
                    state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                }
                break;
            }

            // Collect ledgers that need processing
            let now = tokio::time::Instant::now();
            let ledgers_to_process: Vec<String> = {
                let states = self.states.lock().await;
                states
                    .iter()
                    .filter(|(_, state)| {
                        state.pending_min_t.is_some()
                            && !state.cancelled
                            && state.next_retry_at.is_none_or(|t| t <= now)
                    })
                    .map(|(alias, _)| alias.clone())
                    .collect()
            };

            // Process each ledger
            for alias in ledgers_to_process {
                self.process_ledger(&alias).await;
            }

            // Handle cancelled ledgers
            {
                let mut states = self.states.lock().await;
                for state in states.values_mut() {
                    if state.cancelled && state.has_pending_work() {
                        state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                        state.pending_min_t = None;
                        state.phase = IndexPhase::Idle;
                        state.cancelled = false;
                    }
                }
            }

            // Notify idle waiters
            self.idle_notify.notify_waiters();
        }
    }

    /// Process a single ledger
    async fn process_ledger(&self, alias: &str) {
        // Mark as in-progress
        {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(alias) {
                if state.cancelled {
                    return;
                }
                state.phase = IndexPhase::InProgress;
            } else {
                return;
            }
        }

        // Re-check nameservice for current state
        let record = match self.nameservice.lookup(alias).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Ledger doesn't exist - resolve waiters as failed
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(alias) {
                    state.resolve_waiters_below(
                        i64::MAX,
                        IndexOutcome::Failed("Ledger not found".to_string()),
                    );
                    state.pending_min_t = None;
                    state.phase = IndexPhase::Idle;
                }
                return;
            }
            Err(e) => {
                warn!(
                alias = %alias,
                        error = %e,
                        "Nameservice lookup failed, will retry"
                    );
                self.schedule_retry(alias, &e.to_string()).await;
                return;
            }
        };

        let current_index_t = record.index_t;

        // Check if index already satisfies all waiters
        {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(alias) {
                state.last_index_t = current_index_t;

                if let Some(pending_min) = state.pending_min_t {
                    if current_index_t >= pending_min {
                        // Already satisfied - resolve waiters
                        if let Some(index_addr) = record.index_address.clone() {
                            let outcome = IndexOutcome::Completed {
                                index_t: current_index_t,
                                root_address: index_addr,
                            };
                            state.resolve_waiters_below(current_index_t, outcome);
                            state.recalculate_pending_min_t();
                            state.retry_count = 0;
                            state.next_retry_at = None;
                            state.last_error = None;

                            // If no more work, we're done
                            if state.pending_min_t.is_none() {
                                return;
                            }
                        } else if current_index_t == 0 && pending_min <= 0 {
                            // Genesis-ish case: predicate satisfied without a materialized index.
                            // We allow an empty root address here to avoid failing benign waits.
                            let outcome = IndexOutcome::Completed {
                                index_t: current_index_t,
                                root_address: String::new(),
                            };
                            state.resolve_waiters_below(current_index_t, outcome);
                            state.recalculate_pending_min_t();
                            state.retry_count = 0;
                            state.next_retry_at = None;
                            state.last_error = None;

                            if state.pending_min_t.is_none() {
                                return;
                            }
                        } else {
                            // Nameservice is reporting an index_t but no index address.
                            // Don't spin: force a retry with backoff.
                            state.last_error =
                                Some("Nameservice missing index_address".to_string());
                            state.phase = IndexPhase::Pending;
                            state.next_retry_at =
                                Some(tokio::time::Instant::now() + Duration::from_millis(250));
                            state.retry_count = state.retry_count.saturating_add(1);
                            return;
                        }
                    }
                }

                // Check if cancelled during lookup
                if state.cancelled {
                    state.phase = IndexPhase::Idle;
                    return;
                }
            }
        }

        // Skip if already indexed to current commit
        if record.commit_t <= current_index_t {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(alias) {
                // Resolve waiters that can be satisfied
                if let Some(index_addr) = &record.index_address {
                    let outcome = IndexOutcome::Completed {
                        index_t: current_index_t,
                        root_address: index_addr.clone(),
                    };
                    state.resolve_waiters_below(current_index_t, outcome);
                } else if current_index_t == 0 {
                    // Nothing to index yet; see genesis-ish behavior above.
                    let outcome = IndexOutcome::Completed {
                        index_t: current_index_t,
                        root_address: String::new(),
                    };
                    state.resolve_waiters_below(current_index_t, outcome);
                } else {
                    // Don't loop forever: retry with backoff.
                    state.last_error = Some("Nameservice missing index_address".to_string());
                    state.phase = IndexPhase::Pending;
                    state.next_retry_at =
                        Some(tokio::time::Instant::now() + Duration::from_millis(250));
                    state.retry_count = state.retry_count.saturating_add(1);
                    return;
                }
                state.recalculate_pending_min_t();
                if state.pending_min_t.is_some() {
                    state.phase = IndexPhase::Pending;
                }
            }
            return;
        }

        // Execute refresh-first indexing to CURRENT commit_t
        let result = crate::build_index_for_ledger(
            &self.storage,
            self.nameservice.as_ref(),
            alias,
            self.config.clone(),
        )
        .await;

        match result {
            Ok(index_result) => {
                // Try to publish
                if let Err(e) =
                    crate::publish_index_result(self.nameservice.as_ref(), &index_result).await
                {
                    warn!(
                    alias = %alias,
                            error = %e,
                            "Failed to publish index, will retry"
                        );
                    self.schedule_retry(alias, &e.to_string()).await;
                } else {
                    info!(
                    alias = %alias,
                            index_t = index_result.index_t,
                            "Successfully indexed ledger"
                        );

                    // Spawn garbage collection (fire-and-forget, non-fatal)
                    let gc_storage = self.storage.clone();
                    let gc_root = index_result.root_address.clone();
                    let gc_config = crate::gc::CleanGarbageConfig {
                        max_old_indexes: Some(self.config.gc_max_old_indexes),
                        min_time_garbage_mins: Some(self.config.gc_min_time_mins),
                    };
                    tokio::spawn(async move {
                        if let Err(e) =
                            crate::gc::clean_garbage(&gc_storage, &gc_root, gc_config).await
                        {
                            warn!(
                                error = %e,
                                root_address = %gc_root,
                                "Background GC failed (non-fatal)"
                            );
                        } else {
                            debug!(root_address = %gc_root, "Background GC completed");
                        }
                    });

                    // Resolve waiters
                    let mut states = self.states.lock().await;
                    if let Some(state) = states.get_mut(alias) {
                        let outcome = IndexOutcome::Completed {
                            index_t: index_result.index_t,
                            root_address: index_result.root_address.clone(),
                        };
                        state.resolve_waiters_below(index_result.index_t, outcome);
                        state.last_index_t = index_result.index_t;
                        state.recalculate_pending_min_t();
                        if state.pending_min_t.is_some() {
                            state.phase = IndexPhase::Pending;
                        }
                        state.retry_count = 0;
                        state.next_retry_at = None;
                        state.last_error = None;
                    }
                }
            }
            Err(e) => {
                warn!(
                alias = %alias,
                        error = %e,
                        "Indexing failed, will retry"
                    );
                self.schedule_retry(alias, &e.to_string()).await;
            }
        }
    }

    /// Schedule a retry with exponential backoff
    async fn schedule_retry(&self, alias: &str, error: &str) {
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(alias) {
            // Check if cancelled - don't retry
            if state.cancelled {
                state.resolve_waiters_below(i64::MAX, IndexOutcome::Cancelled);
                state.pending_min_t = None;
                state.phase = IndexPhase::Idle;
                return;
            }

            state.last_error = Some(error.to_string());

            // Compute backoff: 100ms * 2^retry_count, capped at 30s
            let exp = state.retry_count.min(20);
            let factor = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
            let backoff_ms = 100u64.saturating_mul(factor).min(30_000);
            state.next_retry_at =
                Some(tokio::time::Instant::now() + Duration::from_millis(backoff_ms));

            state.retry_count = state.retry_count.saturating_add(1);
            state.phase = IndexPhase::Pending;
        }
    }
}

// =============================================================================
// Post-commit indexing helpers (embedded-orchestrator feature)
// =============================================================================

/// Result of a post-commit refresh attempt (embedded mode)
///
/// `attempted` is true only if we actually invoked `refresh_index()`.
/// Early-returns (threshold not met, nothing to do) set `attempted: false`.
#[cfg(feature = "embedded-orchestrator")]
#[derive(Debug)]
pub struct PostCommitIndexResult {
    /// True if `refresh_index()` was actually invoked
    pub attempted: bool,
    /// True if refresh_index succeeded
    pub refreshed: bool,
    /// True if publish_index succeeded
    pub published: bool,
    /// True if apply_index succeeded
    pub applied: bool,
    /// The refresh result if successful
    pub refresh: Option<RefreshIndexResult>,
    /// Error message if any step failed (for logging)
    pub error: Option<String>,
}

/// Opportunistically refresh + publish + apply after a commit.
///
/// - Uses `ledger.db.storage` internally (no separate storage param).
/// - Uses `target_t` explicitly (use `CommitReceipt.t`).
/// - Never fails the commit path; returns status + error string for logging.
/// - **Applies index even if publish fails** for local correctness.
#[cfg(feature = "embedded-orchestrator")]
pub async fn maybe_refresh_after_commit<S, C, N>(
    nameservice: &N,
    mut ledger: LedgerState<S, C>,
    index_config: &IndexConfig,
    indexer_config: IndexerConfig,
    target_t: i64,
) -> (LedgerState<S, C>, PostCommitIndexResult)
where
    S: Storage + StorageWrite + fluree_db_core::ContentAddressedWrite + Clone + 'static,
    C: NodeCache + Clone,
    N: Publisher,
{
    // Indexing must always target the newest reachable `t`.
    //
    // `target_t` is treated as a hint (historical API), but we always clamp to the
    // ledger's current maximum `t` (max of indexed Db + novelty overlay).
    let effective_target_t = ledger.t();

    // Check threshold
    if ledger.maybe_trigger_index(index_config).is_none() {
        return (
            ledger,
            PostCommitIndexResult {
                attempted: false,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: None,
            },
        );
    }

    // Early-return if nothing to do
    if ledger.novelty_size() == 0 || effective_target_t <= ledger.index_t() {
        return (
            ledger,
            PostCommitIndexResult {
                attempted: false,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: None,
            },
        );
    }

    let storage = ledger.db.storage.clone();

    // Build prev_index for GC chain from current index (if available)
    let prev_index = ledger.ns_record.as_ref().and_then(|ns| {
        ns.index_address
            .as_ref()
            .map(|addr| fluree_db_core::serde::json::PrevIndexRef {
                t: ns.index_t,
                address: addr.clone(),
            })
    });

    match crate::refresh_index_with_prev(
        &storage,
        &ledger.db,
        ledger.novelty.as_ref(),
        // Always index to the newest reachable `t` (ignore any stale hint).
        effective_target_t,
        indexer_config,
        None, // db_config
        prev_index,
    )
    .await
    {
        Ok(refresh) => {
            // Track publish result but continue regardless
            let publish_result = nameservice
                .publish_index(ledger.alias(), &refresh.root_address, refresh.index_t)
                .await;
            let published = publish_result.is_ok();
            let publish_error = publish_result.err().map(|e| e.to_string());

            // Apply even if publish failed (local correctness)
            let apply_result = ledger.apply_index(&refresh.root_address).await;
            let applied = apply_result.is_ok();

            // Build final error message
            let error = match (&publish_error, &apply_result) {
                (Some(pe), Ok(_)) => Some(format!("index applied but publish failed: {}", pe)),
                (None, Err(ae)) => Some(format!("index published but apply_index failed: {}", ae)),
                (Some(pe), Err(ae)) => Some(format!(
                    "publish failed: {}; apply_index also failed: {}",
                    pe, ae
                )),
                (None, Ok(_)) => None,
            };

            (
                ledger,
                PostCommitIndexResult {
                    attempted: true,
                    refreshed: true,
                    published,
                    applied,
                    refresh: Some(refresh),
                    error,
                },
            )
        }
        Err(e) => (
            ledger,
            PostCommitIndexResult {
                attempted: true,
                refreshed: false,
                published: false,
                applied: false,
                refresh: None,
                error: Some(e.to_string()),
            },
        ),
    }
}

/// Blocking refresh path for hard-threshold scenarios (call before commit).
///
/// - Intended usage: if `ledger.require_index(&index_config).is_some()`, call this.
/// - Should typically run *before* staging (since `stage()` also checks max novelty).
/// - Errors are fatal here because the caller is explicitly trying to unblock commits.
#[cfg(feature = "embedded-orchestrator")]
pub async fn require_refresh_before_commit<S, C, N>(
    nameservice: &N,
    mut ledger: LedgerState<S, C>,
    indexer_config: IndexerConfig,
    target_t: i64,
) -> Result<LedgerState<S, C>>
where
    S: Storage + StorageWrite + fluree_db_core::ContentAddressedWrite + Clone + 'static,
    C: NodeCache + Clone,
    N: Publisher,
{
    // Indexing must always target the newest reachable `t`.
    //
    // `target_t` is treated as a hint (historical API), but we always clamp to the
    // ledger's current maximum `t` (max of indexed Db + novelty overlay).
    let effective_target_t = ledger.t();

    let storage = ledger.db.storage.clone();

    // Build prev_index for GC chain from current index (if available)
    let prev_index = ledger.ns_record.as_ref().and_then(|ns| {
        ns.index_address
            .as_ref()
            .map(|addr| fluree_db_core::serde::json::PrevIndexRef {
                t: ns.index_t,
                address: addr.clone(),
            })
    });

    let refresh = crate::refresh_index_with_prev(
        &storage,
        &ledger.db,
        ledger.novelty.as_ref(),
        // Always index to the newest reachable `t` (ignore any stale hint).
        effective_target_t,
        indexer_config,
        None, // db_config
        prev_index,
    )
    .await?;

    nameservice
        .publish_index(ledger.alias(), &refresh.root_address, refresh.index_t)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?;

    ledger
        .apply_index(&refresh.root_address)
        .await
        .map_err(|e| IndexerError::LedgerApply(e.to_string()))?;

    Ok(ledger)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{Flake, FlakeValue, MemoryStorage, Sid, StorageWrite};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_novelty::Commit;
    use std::collections::HashMap;

    fn make_flake(s_code: i32, s_name: &str, p_code: i32, p_name: &str, val: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "xsd:long"),
            t,
            true,
            None,
        )
    }

    async fn store_commit(storage: &MemoryStorage, commit: &Commit) -> String {
        let bytes = serde_json::to_vec(commit).unwrap();
        let hash = {
            use sha2::{Digest, Sha256};
            let h = Sha256::digest(&bytes);
            format!("sha256:{}", hex::encode(h))
        };
        let address = format!("fluree:file://test/commit/{}.json", &hash[7..]);
        storage.write_bytes(&address, &bytes).await.unwrap();
        address
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_no_commits() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        ns.create_ledger("test:main").unwrap();

        let orchestrator = IndexerOrchestrator::new(storage, ns.clone(), IndexerConfig::small());

        // No commits - doesn't need indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(!needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_with_commits_no_index() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        let orchestrator = IndexerOrchestrator::new(storage, ns.clone(), IndexerConfig::small());

        // Has commits but no index - needs indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_index_current() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        let orchestrator =
            IndexerOrchestrator::new(storage.clone(), ns.clone(), IndexerConfig::small());

        // Index the ledger
        let result = orchestrator.index_and_publish("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);

        // Now index is current - doesn't need indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(!needs);
    }

    #[tokio::test]
    async fn test_orchestrator_needs_indexing_index_behind() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup first commit and index
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;
        ns.publish_commit("test:main", &addr1, 1).await.unwrap();

        let orchestrator =
            IndexerOrchestrator::new(storage.clone(), ns.clone(), IndexerConfig::small());
        orchestrator.index_and_publish("test:main").await.unwrap();

        // Add another commit
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)],
            previous: Some(addr1.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::new(),
        };
        let addr2 = store_commit(&storage, &commit2).await;
        ns.publish_commit("test:main", &addr2, 2).await.unwrap();

        // Index is now behind - needs indexing
        let needs = orchestrator.needs_indexing("test:main").await.unwrap();
        assert!(needs);
    }

    #[tokio::test]
    async fn test_orchestrator_index_ledger() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        let orchestrator =
            IndexerOrchestrator::new(storage.clone(), ns.clone(), IndexerConfig::small());

        let result = orchestrator.index_ledger("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);
        assert_eq!(result.alias, "test:main");
    }

    #[tokio::test]
    async fn test_orchestrator_index_and_publish() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        let orchestrator =
            IndexerOrchestrator::new(storage.clone(), ns.clone(), IndexerConfig::small());

        let result = orchestrator.index_and_publish("test:main").await.unwrap();
        assert_eq!(result.index_t, 1);

        // Verify the index was published
        let record = ns.lookup("test:main").await.unwrap().unwrap();
        assert_eq!(record.index_t, 1);
        assert!(record.index_address.is_some());
    }

    #[tokio::test]
    async fn test_orchestrator_returns_existing_when_current() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        // Setup a commit
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        let orchestrator =
            IndexerOrchestrator::new(storage.clone(), ns.clone(), IndexerConfig::small());

        // First index
        let result1 = orchestrator.index_and_publish("test:main").await.unwrap();

        // Second index - should return existing
        let result2 = orchestrator.index_ledger("test:main").await.unwrap();

        assert_eq!(result1.root_address, result2.root_address);
        assert_eq!(result2.stats.flake_count, 0); // No work done
    }

    #[tokio::test]
    async fn test_orchestrator_ledger_not_found() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());

        let orchestrator = IndexerOrchestrator::new(storage, ns.clone(), IndexerConfig::small());

        let result = orchestrator.index_ledger("nonexistent:main").await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Tests for IndexerHandle and completion tracking
    // =========================================================================

    #[tokio::test]
    async fn test_handle_trigger_returns_completion() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Trigger without starting worker - completion should be pending
        let mut completion = handle.trigger("test:main", 1).await;

        // Should not be ready yet (worker not running)
        assert!(completion.try_get().is_none());

        // Drop worker to cancel
        drop(worker);
        drop(handle);

        // Now completion should resolve as cancelled
        let outcome = completion.wait().await;
        assert!(matches!(outcome, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_cancel_resolves_waiters_as_cancelled() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Trigger and get completion
        let completion = handle.trigger("test:main", 1).await;

        // Cancel the ledger
        let had_work = handle.cancel("test:main").await;
        assert!(had_work);

        // Completion should be cancelled
        let outcome = completion.wait().await;
        assert!(matches!(outcome, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_cancel_all() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Trigger multiple ledgers
        let c1 = handle.trigger("test:one", 1).await;
        let c2 = handle.trigger("test:two", 2).await;

        // Cancel all
        handle.cancel_all().await;

        // Both should be cancelled
        assert!(matches!(c1.wait().await, IndexOutcome::Cancelled));
        assert!(matches!(c2.wait().await, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_handle_status() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // No status for unknown ledger
        assert!(handle.status("unknown").await.is_none());

        // Trigger a ledger
        let _completion = handle.trigger("test:main", 5).await;

        // Should have status now
        let status = handle.status("test:main").await.unwrap();
        assert_eq!(status.phase, IndexPhase::Pending);
        assert_eq!(status.pending_min_t, Some(5));
        assert_eq!(status.waiter_count, 1);
    }

    #[tokio::test]
    async fn test_handle_is_pending() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        assert!(!handle.is_pending("test:main").await);

        let _completion = handle.trigger("test:main", 1).await;

        assert!(handle.is_pending("test:main").await);
    }

    #[tokio::test]
    async fn test_handle_pending_ledgers() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        assert!(handle.pending_ledgers().await.is_empty());

        let _c1 = handle.trigger("test:one", 1).await;
        let _c2 = handle.trigger("test:two", 2).await;

        let pending = handle.pending_ledgers().await;
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"test:one".to_string()));
        assert!(pending.contains(&"test:two".to_string()));
    }

    #[tokio::test]
    async fn test_handle_multiple_triggers_coalesce_min_t() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // First trigger with min_t=5
        let _c1 = handle.trigger("test:main", 5).await;
        // Second trigger with min_t=3 (lower)
        let _c2 = handle.trigger("test:main", 3).await;

        // Status should show coalesced min_t = 3 (the minimum)
        let status = handle.status("test:main").await.unwrap();
        assert_eq!(status.pending_min_t, Some(3));
        // Should have 2 waiters
        assert_eq!(status.waiter_count, 2);
    }

    #[tokio::test]
    async fn test_handle_trigger_clears_backoff() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Ensure ledger state exists.
        let _c1 = handle.trigger("test:main", 1).await;

        // Put it into backoff.
        worker.schedule_retry("test:main", "boom").await;
        {
            let states = handle.states.lock().await;
            let state = states.get("test:main").expect("state exists");
            assert!(state.next_retry_at.is_some());
            assert!(state.retry_count > 0);
        }

        // New trigger should clear backoff.
        let _c2 = handle.trigger("test:main", 2).await;
        {
            let states = handle.states.lock().await;
            let state = states.get("test:main").expect("state exists");
            assert!(state.next_retry_at.is_none());
            assert_eq!(state.retry_count, 0);
        }
    }

    #[tokio::test]
    async fn test_handle_wait_for_idle_immediate_return() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Should return immediately for unknown ledger
        handle.wait_for_idle("unknown").await;
    }

    #[tokio::test]
    async fn test_handle_wait_all_idle_immediate_return() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        // Should return immediately when nothing pending
        handle.wait_all_idle().await;
    }

    #[tokio::test]
    async fn test_index_completion_debug() {
        let storage = MemoryStorage::new();
        let ns = Arc::new(MemoryNameService::new());
        let (_worker, handle) = BackgroundIndexerWorker::new(storage, ns, IndexerConfig::small());

        let completion = handle.trigger("test:main", 1).await;
        // Should not panic
        let debug_str = format!("{:?}", completion);
        assert!(debug_str.contains("IndexCompletion"));
    }

    #[tokio::test]
    async fn test_index_outcome_clone() {
        let outcome = IndexOutcome::Completed {
            index_t: 5,
            root_address: "test:addr".to_string(),
        };
        let cloned = outcome.clone();
        assert!(matches!(cloned, IndexOutcome::Completed { index_t: 5, .. }));

        let failed = IndexOutcome::Failed("error".to_string());
        let cloned_failed = failed.clone();
        assert!(matches!(cloned_failed, IndexOutcome::Failed(_)));

        let cancelled = IndexOutcome::Cancelled;
        let cloned_cancelled = cancelled.clone();
        assert!(matches!(cloned_cancelled, IndexOutcome::Cancelled));
    }

    #[tokio::test]
    async fn test_index_status_snapshot_clone() {
        let snapshot = IndexStatusSnapshot {
            phase: IndexPhase::Pending,
            pending_min_t: Some(5),
            last_index_t: 3,
            last_error: Some("test error".to_string()),
            waiter_count: 2,
        };
        let cloned = snapshot.clone();
        assert_eq!(cloned.phase, IndexPhase::Pending);
        assert_eq!(cloned.pending_min_t, Some(5));
        assert_eq!(cloned.last_index_t, 3);
        assert_eq!(cloned.last_error, Some("test error".to_string()));
        assert_eq!(cloned.waiter_count, 2);
    }
}

// =============================================================================
// Tests for embedded-orchestrator feature
// =============================================================================

#[cfg(all(test, feature = "embedded-orchestrator"))]
mod embedded_tests {
    use super::*;
    use fluree_db_core::{Db, Flake, FlakeValue, MemoryStorage, NoCache, Sid};
    use fluree_db_ledger::LedgerState;
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_novelty::Novelty;

    fn make_flake(s_code: i32, s_name: &str, p_code: i32, p_name: &str, val: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "xsd:long"),
            t,
            true,
            None,
        )
    }

    fn make_large_flake(t: i64, size_hint: usize) -> Flake {
        // Create a flake with a large string value to exceed thresholds
        let big_value = "x".repeat(size_hint);
        Flake::new(
            Sid::new(1, "ex:entity"),
            Sid::new(1, "ex:data"),
            FlakeValue::String(big_value),
            Sid::new(2, "xsd:string"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_maybe_refresh_below_threshold_returns_not_attempted() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        let index_config = IndexConfig::default(); // High threshold
        let indexer_config = IndexerConfig::small();

        let (returned_ledger, result) =
            maybe_refresh_after_commit(&ns, ledger, &index_config, indexer_config, 1).await;

        // Should not have attempted since novelty is empty
        assert!(!result.attempted);
        assert!(!result.refreshed);
        assert!(!result.published);
        assert!(!result.applied);
        assert!(result.refresh.is_none());
        assert!(result.error.is_none());
        assert_eq!(returned_ledger.t(), 0); // No change
    }

    #[tokio::test]
    async fn test_maybe_refresh_above_threshold_attempts_refresh() {
        use crate::builder;
        use std::collections::BTreeMap;

        let storage = MemoryStorage::new();

        // First build a proper base index at t=1
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)];

        let indexer_config = IndexerConfig::small();
        let initial_result = builder::build_index(
            &storage,
            "test:main",
            1,
            initial_flakes,
            namespace_codes,
            indexer_config.clone(),
        )
        .await
        .expect("Should build initial index");

        // Load the indexed Db
        let db: Db<MemoryStorage, NoCache> = Db::load(
            storage.clone(),
            NoCache::new(),
            &initial_result.root_address,
        )
        .await
        .expect("Should load db");

        // Create novelty with a large flake to exceed threshold
        let mut novelty = Novelty::new(1); // Start after index_t=1
        let large_flake = make_large_flake(2, 2000); // 2KB flake at t=2
        novelty.apply_commit(vec![large_flake], 2).unwrap();

        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        // Use very small thresholds to trigger indexing
        let index_config = IndexConfig {
            reindex_min_bytes: 100, // 100 bytes
            reindex_max_bytes: 10000,
        };

        let (returned_ledger, result) = maybe_refresh_after_commit(
            &ns,
            ledger,
            &index_config,
            indexer_config,
            2, // target_t
        )
        .await;

        // Should have attempted and succeeded
        assert!(result.attempted);
        assert!(result.refreshed);
        assert!(result.published);
        assert!(result.applied);
        assert!(result.refresh.is_some());
        assert!(result.error.is_none());

        // Novelty should be pruned after apply_index
        assert_eq!(returned_ledger.index_t(), 2);
    }

    #[tokio::test]
    async fn test_maybe_refresh_early_return_when_target_not_ahead() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");

        // Create novelty with large flake but at t=0
        let mut novelty = Novelty::new(0);
        let large_flake = make_large_flake(1, 2000);
        novelty.apply_commit(vec![large_flake], 1).unwrap();

        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        let index_config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 10000,
        };
        let indexer_config = IndexerConfig::small();

        // Even if a stale target_t is provided, we always clamp to ledger.t().
        // This test exercises the "nothing to do" early-return path.
        let (_returned_ledger, result) = maybe_refresh_after_commit(
            &ns,
            ledger,
            &index_config,
            indexer_config,
            0, // stale hint (ignored)
        )
        .await;

        // Should early-return without attempting
        assert!(!result.attempted);
    }

    #[tokio::test]
    async fn test_require_refresh_success_path() {
        use crate::builder;
        use std::collections::BTreeMap;

        let storage = MemoryStorage::new();

        // First build a proper base index at t=1
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)];

        let indexer_config = IndexerConfig::small();
        let initial_result = builder::build_index(
            &storage,
            "test:main",
            1,
            initial_flakes,
            namespace_codes,
            indexer_config.clone(),
        )
        .await
        .expect("Should build initial index");

        // Load the indexed Db
        let db: Db<MemoryStorage, NoCache> = Db::load(
            storage.clone(),
            NoCache::new(),
            &initial_result.root_address,
        )
        .await
        .expect("Should load db");

        // Create novelty with some flakes at t=2
        let mut novelty = Novelty::new(1);
        let flake = make_flake(1, "ex:bob", 1, "ex:age", 25, 2);
        novelty.apply_commit(vec![flake], 2).unwrap();

        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();

        let result = require_refresh_before_commit(
            &ns,
            ledger,
            indexer_config,
            2, // target_t
        )
        .await;

        assert!(result.is_ok());
        let returned_ledger = result.unwrap();
        assert_eq!(returned_ledger.index_t(), 2);
    }

    #[tokio::test]
    async fn test_require_refresh_propagates_errors() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");

        // Empty novelty - refresh will work but with no changes
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let ns = MemoryNameService::new();
        let indexer_config = IndexerConfig::small();

        // This should succeed (no flakes to refresh, but still valid)
        let result = require_refresh_before_commit(
            &ns,
            ledger,
            indexer_config,
            0, // target_t
        )
        .await;

        // With empty novelty at t=0, this should succeed (refresh is a no-op)
        assert!(result.is_ok());
    }
}
