//! Background prefetch service for leaf warming (native only)
//!
//! This module provides a bounded background prefetch service that warms the cache
//! by resolving upcoming index nodes before the main query needs them.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     PrefetchService                              │
//! │  ┌──────────────┐    ┌──────────────────────────────────────┐   │
//! │  │ Bounded MPSC │───▶│  Dispatcher + Semaphore Workers      │   │
//! │  │ (queue_depth)│    │  Each: recv → acquire → spawn → drop │   │
//! │  └──────────────┘    └──────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! The service uses a single receiver with a semaphore to bound concurrent work:
//! - One dispatcher task owns the channel receiver
//! - For each request: acquire semaphore permit, spawn worker, drop permit when done
//! - Workers call `resolve_node_materialized_with_overlay` to warm both raw and
//!   materialized cache entries
//!
//! # Coordination with Cache
//!
//! The cache provides single-flight deduplication:
//! - First arrival (prefetch or mainline) inserts `InFlight` and does the work
//! - Others wait on `InFlight` and receive the same result
//! - Orphaned `InFlight` entries are automatically retried (see `SimpleCache`)
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_core::prefetch::{PrefetchService, PrefetchConfig};
//!
//! // At application startup
//! let config = PrefetchConfig::default();
//! let service = PrefetchService::start(config);
//!
//! // During query execution
//! service.try_enqueue(PrefetchRequest {
//!     db: db.clone(),
//!     overlay: overlay.clone(),
//!     node: upcoming_leaf,
//!     to_t,
//!     from_t,
//!     history_mode: false,
//! });
//! ```

#[cfg(feature = "native")]
use crate::index::IndexNode;
#[cfg(feature = "native")]
use crate::overlay::OverlayProvider;
#[cfg(feature = "native")]
use crate::range::resolve_node_materialized_with_overlay;
#[cfg(feature = "native")]
use crate::storage::Storage;
#[cfg(feature = "native")]
use crate::Db;
#[cfg(feature = "native")]
use std::sync::Arc;
#[cfg(feature = "native")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "native")]
use tokio::sync::{mpsc, Semaphore};

// Global counters for prefetch diagnostics
#[cfg(feature = "native")]
static PREFETCH_ENQUEUED: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "native")]
static PREFETCH_COMPLETED: AtomicU64 = AtomicU64::new(0);

/// Get and reset prefetch statistics (enqueued, completed)
#[cfg(feature = "native")]
pub fn prefetch_stats_reset() -> (u64, u64) {
    let enqueued = PREFETCH_ENQUEUED.swap(0, Ordering::Relaxed);
    let completed = PREFETCH_COMPLETED.swap(0, Ordering::Relaxed);
    (enqueued, completed)
}

/// Default number of prefetch workers (bounds concurrent I/O + memory).
#[cfg(feature = "native")]
pub const DEFAULT_PREFETCH_WORKERS: usize = 16;

/// Default prefetch queue depth (requests buffer before dropping).
#[cfg(feature = "native")]
pub const DEFAULT_PREFETCH_QUEUE_DEPTH: usize = 32;

/// Configuration for the prefetch service.
#[cfg(feature = "native")]
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Maximum number of concurrent prefetch operations.
    ///
    /// This bounds:
    /// - Number of spawned tasks
    /// - Number of in-flight storage reads
    /// - Memory held by raw bytes before parsing
    /// - Cache lock pressure
    ///
    /// Default: `min((available_parallelism - 1).max(1), 4)`
    pub num_workers: usize,

    /// Maximum number of queued prefetch requests.
    ///
    /// When the queue is full, new requests are dropped (best-effort prefetch).
    /// This prevents unbounded memory growth under load.
    ///
    /// Default: `num_workers * 8`
    pub queue_depth: usize,
}

#[cfg(feature = "native")]
impl Default for PrefetchConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        let num_workers = (parallelism.saturating_sub(1)).max(1).min(DEFAULT_PREFETCH_WORKERS);
        let queue_depth = num_workers * 8;

        Self {
            num_workers,
            queue_depth,
        }
    }
}

#[cfg(feature = "native")]
impl PrefetchConfig {
    /// Disable prefetch entirely.
    ///
    /// This is useful for tests or in-memory deployments where prefetch provides
    /// little value and can introduce background-task overhead.
    pub fn disabled() -> Self {
        Self {
            num_workers: 0,
            queue_depth: 1, // must be >= 1 for mpsc::channel
        }
    }

    /// Create a new configuration with custom worker count.
    pub fn with_workers(num_workers: usize) -> Self {
        let workers = num_workers.max(1);
        Self {
            num_workers: workers,
            queue_depth: workers * 8,
        }
    }

    /// Set the queue depth.
    pub fn with_queue_depth(mut self, depth: usize) -> Self {
        self.queue_depth = depth.max(1);
        self
    }
}

/// A request to prefetch an index node.
///
/// Contains all context needed to call `resolve_node_materialized_with_overlay`.
#[cfg(feature = "native")]
pub struct PrefetchRequest<S: Storage> {
    /// Database reference (Arc for 'static lifetime in spawned tasks).
    pub db: Arc<Db<S>>,

    /// Overlay provider for uncommitted data.
    pub overlay: Arc<dyn OverlayProvider>,

    /// The index node to prefetch.
    pub node: IndexNode,

    /// Target transaction time for materialization.
    pub to_t: i64,

    /// Optional start time for history queries.
    pub from_t: Option<i64>,

    /// History mode flag.
    pub history_mode: bool,
}

/// Background prefetch service for warming the cache.
///
/// This service runs a dispatcher task that receives prefetch requests
/// and spawns bounded workers to resolve them. The cache provides
/// single-flight deduplication, so prefetch and mainline never duplicate work.
#[cfg(feature = "native")]
pub struct PrefetchService<S: Storage + 'static> {
    sender: mpsc::Sender<PrefetchRequest<S>>,
}

#[cfg(feature = "native")]
impl<S> PrefetchService<S>
where
    S: Storage + 'static,
{
    /// Start the prefetch service with the given configuration.
    ///
    /// This spawns a dispatcher task that will run until the service is dropped
    /// (when all senders are dropped, the receiver closes and the dispatcher exits).
    pub fn start(config: PrefetchConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        // Spawn the dispatcher task only if we're inside a Tokio runtime.
        //
        // Some library/unit tests construct Fluree in a plain `#[test]` without
        // a runtime. In that case, prefetch should be effectively disabled
        // (best-effort optimization), not a hard panic.
        if config.num_workers > 0 && tokio::runtime::Handle::try_current().is_ok() {
            Self::spawn_dispatcher(rx, config.num_workers);
        }
        // else: drop rx, disabling prefetch (try_enqueue will fail)

        Arc::new(Self { sender: tx })
    }

    /// Start with default configuration.
    pub fn start_default() -> Arc<Self> {
        Self::start(PrefetchConfig::default())
    }

    /// Spawn the dispatcher task that processes prefetch requests.
    fn spawn_dispatcher(mut rx: mpsc::Receiver<PrefetchRequest<S>>, num_workers: usize) {
        let semaphore = Arc::new(Semaphore::new(num_workers));

        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                // Acquire permit before spawning (bounds concurrent work)
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        // Semaphore closed - shutdown
                        break;
                    }
                };

                // Spawn worker task
                tokio::spawn(async move {
                    // Permit is held until this task completes
                    let _permit = permit;

                    // Get overlay epoch for cache key
                    let overlay_epoch = req.overlay.epoch();

                    // Warm the cache by resolving the node.
                    // Result is ignored - we're just warming.
                    // Errors are fine (transient failures will be retried by mainline).
                    let _ = resolve_node_materialized_with_overlay(
                        &req.db,
                        req.overlay.as_ref(),
                        overlay_epoch,
                        &req.node,
                        req.from_t,
                        req.to_t,
                        req.history_mode,
                    )
                    .await;
                    
                    PREFETCH_COMPLETED.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }

    /// Try to enqueue a prefetch request.
    ///
    /// This is non-blocking: if the queue is full, the request is dropped.
    /// This is acceptable because prefetch is best-effort optimization.
    ///
    /// Returns `true` if the request was enqueued, `false` if dropped.
    pub fn try_enqueue(&self, request: PrefetchRequest<S>) -> bool {
        let ok = self.sender.try_send(request).is_ok();
        if ok {
            PREFETCH_ENQUEUED.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    /// Check if the prefetch queue has capacity.
    ///
    /// Useful for deciding whether to bother constructing prefetch requests.
    pub fn has_capacity(&self) -> bool {
        self.sender.capacity() > 0
    }
}

#[cfg(feature = "native")]
impl<S: Storage + 'static> Clone for PrefetchService<S> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

// ============================================================================
// WASM stub (no-op)
// ============================================================================

/// WASM stub for PrefetchConfig.
#[cfg(not(feature = "native"))]
#[derive(Debug, Clone, Default)]
pub struct PrefetchConfig;

/// WASM stub for PrefetchService.
///
/// On WASM, prefetch is disabled (no background tasks).
#[cfg(not(feature = "native"))]
pub struct PrefetchService<S> {
    _phantom: std::marker::PhantomData<S>,
}

#[cfg(not(feature = "native"))]
impl<S> PrefetchService<S> {
    /// No-op on WASM.
    pub fn start(_config: PrefetchConfig) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            _phantom: std::marker::PhantomData,
        })
    }

    /// No-op on WASM.
    pub fn start_default() -> std::sync::Arc<Self> {
        Self::start(PrefetchConfig)
    }

    /// Always returns false on WASM (prefetch disabled).
    pub fn try_enqueue<R>(&self, _request: R) -> bool {
        false
    }

    /// Always returns false on WASM.
    pub fn has_capacity(&self) -> bool {
        false
    }
}

#[cfg(not(feature = "native"))]
impl<S> Clone for PrefetchService<S> {
    fn clone(&self) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
#[cfg(feature = "native")]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrefetchConfig::default();
        assert!(config.num_workers >= 1);
        assert!(config.num_workers <= DEFAULT_PREFETCH_WORKERS);
        assert_eq!(config.queue_depth, config.num_workers * 8);
    }

    #[test]
    fn test_config_with_workers() {
        let config = PrefetchConfig::with_workers(2);
        assert_eq!(config.num_workers, 2);
        assert_eq!(config.queue_depth, 16);

        // Minimum of 1 worker
        let config = PrefetchConfig::with_workers(0);
        assert_eq!(config.num_workers, 1);
    }
}
