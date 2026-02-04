//! Cache trait for index node resolution
//!
//! This module defines the `NodeCache` trait that apps can implement to
//! provide caching for resolved index nodes. The trait is runtime-agnostic
//! and uses `async_trait(?Send)` for WASM compatibility.
//!
//! ## Implementations
//!
//! Apps provide their own implementations:
//! - Native: `TokioLruCache` (using `tokio::sync::RwLock`)
//! - WASM: `RefCellCache` (using `RefCell`)
//! - Simple: `NoCache` (no caching, for testing)
//!
//! ## Cache Key
//!
//! Nodes are cached by their ID and the "as-of" transaction time (`to_t`).
//! This allows the same node to have different cached versions for different
//! time-travel queries.

use crate::error::Result;
use crate::index::ResolvedNode;
use async_trait::async_trait;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;

/// Cache key for resolved nodes
///
/// Includes both the node ID and the "as-of" time, allowing
/// time-travel queries to be cached separately.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CacheKind {
    /// Raw node as read from storage (branch children or leaf flakes), independent of time-travel.
    Raw,
    /// Leaf materialized for a specific `to_t` (t-range).
    LeafTRange,
    /// Leaf materialized for a specific `from_t..=to_t` (history-range).
    LeafHistoryRange,
}

/// Cache key for resolved nodes.
///
/// This is intentionally explicit about what is cached:
/// - `Raw` caches the decoded node independent of `to_t`
/// - `LeafTRange` caches a leaf after applying replay/materialization to `to_t`
/// - `LeafHistoryRange` caches a leaf after applying replay/materialization to `from_t..=to_t`
///
/// When using an overlay (novelty), the `epoch` field differentiates cache entries
/// that were created with different overlay states.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    /// What kind of cached value this represents.
    pub kind: CacheKind,
    /// Node storage ID
    pub node_id: String,
    /// "As-of" transaction time
    pub to_t: i64,
    /// Optional "from" time for history queries
    pub from_t: Option<i64>,
    /// Whether the cached leaf is materialized in "history mode".
    ///
    /// This affects whether stale flakes are removed during materialization.
    /// It must be part of the cache key to avoid returning non-history results
    /// for history queries (or vice versa).
    ///
    /// For `CacheKind::Raw`, this is always `false`.
    pub history_mode: bool,
    /// Overlay epoch for cache invalidation when novelty changes
    pub epoch: u64,
}

impl CacheKey {
    /// Create a raw-node cache key (independent of `to_t`).
    ///
    /// Raw nodes are epoch-independent since they represent the underlying
    /// stored data, not materialized views.
    pub fn raw(node_id: impl Into<String>) -> Self {
        Self {
            kind: CacheKind::Raw,
            node_id: node_id.into(),
            to_t: 0,
            from_t: None,
            history_mode: false,
            epoch: 0,
        }
    }

    /// Create a leaf t-range cache key (without overlay).
    ///
    /// Use `leaf_t_range_with_epoch` when querying with a novelty overlay.
    pub fn leaf_t_range(node_id: impl Into<String>, to_t: i64) -> Self {
        Self {
            kind: CacheKind::LeafTRange,
            node_id: node_id.into(),
            to_t,
            from_t: None,
            history_mode: false,
            epoch: 0,
        }
    }

    /// Create a leaf t-range cache key with overlay epoch.
    ///
    /// The epoch should come from `OverlayProvider::epoch()`. When the
    /// overlay changes (new commits applied), the epoch changes and
    /// cached leaf materializations are automatically invalidated.
    pub fn leaf_t_range_with_epoch(node_id: impl Into<String>, to_t: i64, epoch: u64) -> Self {
        Self {
            kind: CacheKind::LeafTRange,
            node_id: node_id.into(),
            to_t,
            from_t: None,
            history_mode: false,
            epoch,
        }
    }

    /// Create a leaf t-range cache key with overlay epoch and history-mode flag.
    pub fn leaf_t_range_with_epoch_and_mode(
        node_id: impl Into<String>,
        to_t: i64,
        epoch: u64,
        history_mode: bool,
    ) -> Self {
        Self {
            kind: CacheKind::LeafTRange,
            node_id: node_id.into(),
            to_t,
            from_t: None,
            history_mode,
            epoch,
        }
    }

    /// Create a leaf history-range cache key.
    pub fn leaf_history_range(node_id: impl Into<String>, from_t: i64, to_t: i64) -> Self {
        Self {
            kind: CacheKind::LeafHistoryRange,
            node_id: node_id.into(),
            to_t,
            from_t: Some(from_t),
            history_mode: false,
            epoch: 0,
        }
    }

    /// Create a leaf history-range cache key with overlay epoch.
    pub fn leaf_history_range_with_epoch(
        node_id: impl Into<String>,
        from_t: i64,
        to_t: i64,
        epoch: u64,
    ) -> Self {
        Self {
            kind: CacheKind::LeafHistoryRange,
            node_id: node_id.into(),
            to_t,
            from_t: Some(from_t),
            history_mode: false,
            epoch,
        }
    }

    /// Create a leaf history-range cache key with overlay epoch and history-mode flag.
    pub fn leaf_history_range_with_epoch_and_mode(
        node_id: impl Into<String>,
        from_t: i64,
        to_t: i64,
        epoch: u64,
        history_mode: bool,
    ) -> Self {
        Self {
            kind: CacheKind::LeafHistoryRange,
            node_id: node_id.into(),
            to_t,
            from_t: Some(from_t),
            history_mode,
            epoch,
        }
    }
}

/// Cache trait for resolved index nodes
///
/// This trait provides get-or-fetch semantics: if a node is in the cache,
/// return it immediately; otherwise, call the fetch function and cache
/// the result.
#[async_trait]
pub trait NodeCache: Debug + Send + Sync {
    /// Get a node from cache, or fetch and cache it
    ///
    /// The `fetch` function is only called if the node is not in the cache.
    /// Implementations should handle concurrent requests for the same key
    /// by deduplicating the fetch (only one fetch per key).
    async fn get_or_fetch<F, Fut>(&self, key: &CacheKey, fetch: F) -> Result<ResolvedNode>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<ResolvedNode>> + Send;

    /// Explicitly evict a node from the cache
    fn evict(&self, key: &CacheKey);

    /// Clear all entries from the cache
    fn clear(&self);

    /// Get the current number of entries in the cache
    fn len(&self) -> usize;

    /// Check if the cache is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A no-op cache that always calls the fetch function
///
/// Useful for testing or when caching is not desired.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCache;

impl NoCache {
    /// Create a new no-op cache
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NodeCache for NoCache {
    async fn get_or_fetch<F, Fut>(&self, _key: &CacheKey, fetch: F) -> Result<ResolvedNode>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<ResolvedNode>> + Send,
    {
        // Always fetch, never cache
        fetch().await
    }

    fn evict(&self, _key: &CacheKey) {
        // No-op
    }

    fn clear(&self) {
        // No-op
    }

    fn len(&self) -> usize {
        0
    }
}

/// Cache statistics for benchmarking
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of evictions
    pub evictions: u64,
    /// Number of Raw (branch) hits
    pub raw_hits: u64,
    /// Number of Raw (branch) misses
    pub raw_misses: u64,
    /// Number of Leaf hits
    pub leaf_hits: u64,
    /// Number of Leaf misses
    pub leaf_misses: u64,
    /// Number of Ready hits (instant return)
    pub ready_hits: u64,
    /// Number of InFlight hits (had to wait)
    pub inflight_hits: u64,
    /// Total time spent waiting on InFlight entries (microseconds)
    pub inflight_wait_us: u64,
    /// Total time spent fetching on miss (microseconds)  
    pub fetch_time_us: u64,
}

impl CacheStats {
    /// Get hit rate as a percentage (0.0 - 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
        self.raw_hits = 0;
        self.raw_misses = 0;
        self.leaf_hits = 0;
        self.leaf_misses = 0;
        self.ready_hits = 0;
        self.inflight_hits = 0;
        self.inflight_wait_us = 0;
        self.fetch_time_us = 0;
    }
}

/// Entry state in the cache - either completed or in-flight
///
/// Uses `futures::lock::Mutex` for WASM compatibility (runtime-agnostic).
enum CacheEntry {
    /// Value is ready
    Ready(ResolvedNode),
    /// Value is being fetched - waiters share this mutex
    /// The Option is None while fetching, Some after completion
    InFlight(std::sync::Arc<futures::lock::Mutex<Option<Result<ResolvedNode>>>>),
}

impl Clone for CacheEntry {
    fn clone(&self) -> Self {
        match self {
            CacheEntry::Ready(node) => CacheEntry::Ready(node.clone()),
            CacheEntry::InFlight(mutex) => CacheEntry::InFlight(mutex.clone()),
        }
    }
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheEntry::Ready(node) => f.debug_tuple("Ready").field(node).finish(),
            CacheEntry::InFlight(_) => f.debug_tuple("InFlight").finish(),
        }
    }
}

/// A simple in-memory cache using RwLock for thread-safe parallel access
///
/// This implementation uses a HashMap with LRU-like eviction.
/// It's suitable for both single-threaded and multi-threaded environments.
/// Includes cache statistics for benchmarking.
///
/// **Deduplication**: If multiple requests come in for the same key while
/// a fetch is in-flight, they all share the same fetch - no duplicate I/O.
/// This is critical for prefetch optimization to work correctly.
#[derive(Debug)]
pub struct SimpleCache {
    entries: std::sync::RwLock<std::collections::HashMap<CacheKey, CacheEntry>>,
    max_entries: usize,
    stats: std::sync::RwLock<CacheStats>,
}

impl Clone for SimpleCache {
    fn clone(&self) -> Self {
        // Only clone Ready entries, skip InFlight
        let entries = self
            .entries
            .read()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| match v {
                CacheEntry::Ready(node) => Some((k.clone(), CacheEntry::Ready(node.clone()))),
                CacheEntry::InFlight(_) => None,
            })
            .collect();
        Self {
            entries: std::sync::RwLock::new(entries),
            max_entries: self.max_entries,
            stats: std::sync::RwLock::new(*self.stats.read().unwrap()),
        }
    }
}

impl SimpleCache {
    /// Create a new simple cache with the given maximum number of entries
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: std::sync::RwLock::new(std::collections::HashMap::new()),
            max_entries,
            stats: std::sync::RwLock::new(CacheStats::default()),
        }
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        *self.stats.read().unwrap()
    }

    /// Reset cache statistics
    pub fn reset_stats(&self) {
        self.stats.write().unwrap().reset();
    }

    /// Get estimated memory usage in bytes
    pub fn estimated_size_bytes(&self) -> usize {
        self.entries
            .read()
            .unwrap()
            .values()
            .filter_map(|entry| match entry {
                CacheEntry::Ready(node) => Some(node.size_bytes()),
                CacheEntry::InFlight(_) => None,
            })
            .sum()
    }
}

#[async_trait]
impl NodeCache for SimpleCache {
    async fn get_or_fetch<F, Fut>(&self, key: &CacheKey, fetch: F) -> Result<ResolvedNode>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<ResolvedNode>> + Send,
    {
        // `fetch` is `FnOnce`, but we may need to retry cache lookup if we detect an
        // orphaned InFlight (i.e., the fetching task was dropped before completing).
        // Keep it in an Option so we can still call it exactly once when we are the fetcher.
        let mut fetch = Some(fetch);

        // Phase 1: Check cache state and potentially insert InFlight marker
        enum Action {
            Hit(ResolvedNode),
            WaitOnInFlight(std::sync::Arc<futures::lock::Mutex<Option<Result<ResolvedNode>>>>),
            DoFetch(std::sync::Arc<futures::lock::Mutex<Option<Result<ResolvedNode>>>>),
        }

        loop {
            let action = {
                let mut entries = self.entries.write().unwrap();

                match entries.get(key) {
                    Some(CacheEntry::Ready(node)) => {
                        // Cache hit - return immediately
                        let mut stats = self.stats.write().unwrap();
                        stats.hits += 1;
                        stats.ready_hits += 1;
                        match key.kind {
                            CacheKind::Raw => stats.raw_hits += 1,
                            CacheKind::LeafTRange | CacheKind::LeafHistoryRange => {
                                stats.leaf_hits += 1
                            }
                        }
                        Action::Hit(node.clone())
                    }
                    Some(CacheEntry::InFlight(mutex)) => {
                        // Another request is fetching this - we'll wait on the same mutex
                        let mut stats = self.stats.write().unwrap();
                        stats.hits += 1; // Count as hit (deduped)
                        stats.inflight_hits += 1;
                        match key.kind {
                            CacheKind::Raw => stats.raw_hits += 1,
                            CacheKind::LeafTRange | CacheKind::LeafHistoryRange => {
                                stats.leaf_hits += 1
                            }
                        }
                        Action::WaitOnInFlight(mutex.clone())
                    }
                    None => {
                        // Cache miss - we'll do the fetch
                        let mut stats = self.stats.write().unwrap();
                        stats.misses += 1;
                        match key.kind {
                            CacheKind::Raw => stats.raw_misses += 1,
                            CacheKind::LeafTRange | CacheKind::LeafHistoryRange => {
                                stats.leaf_misses += 1
                            }
                        }

                        // Evict if needed (only evict Ready entries, not InFlight)
                        if entries.len() >= self.max_entries {
                            let ready_key = entries
                                .iter()
                                .find(|(_, v)| matches!(v, CacheEntry::Ready(_)))
                                .map(|(k, _)| k.clone());
                            if let Some(old_key) = ready_key {
                                entries.remove(&old_key);
                                stats.evictions += 1;
                            }
                        }

                        // Insert InFlight marker with empty mutex
                        let mutex = std::sync::Arc::new(futures::lock::Mutex::new(None));
                        entries.insert(key.clone(), CacheEntry::InFlight(mutex.clone()));
                        Action::DoFetch(mutex)
                    }
                }
            };

            // Phase 2: Handle the action
            match action {
                Action::Hit(node) => return Ok(node),
                Action::WaitOnInFlight(mutex) => {
                    // Wait for the other request to complete by acquiring the lock.
                    // The fetching task holds the lock until it's done.
                    let wait_start = std::time::Instant::now();
                    let guard = mutex.lock().await;
                    let wait_us = wait_start.elapsed().as_micros() as u64;
                    self.stats.write().unwrap().inflight_wait_us += wait_us;

                    match guard.as_ref() {
                        Some(Ok(node)) => return Ok(node.clone()),
                        Some(Err(e)) => return Err(crate::error::Error::Storage(e.to_string())),
                        None => {
                            // Orphaned in-flight: the fetching task was dropped before
                            // completing and never populated the result.
                            //
                            // Treat this as "no work done": remove the stale in-flight entry
                            // (if it still matches this mutex), then retry the lookup/fetch.
                            drop(guard);

                            let mut entries = self.entries.write().unwrap();
                            let remove = matches!(entries.get(key), Some(CacheEntry::InFlight(m)) if std::sync::Arc::ptr_eq(m, &mutex));
                            if remove {
                                entries.remove(key);
                            }
                            continue;
                        }
                    }
                }
                Action::DoFetch(mutex) => {
                    // We're responsible for the fetch - acquire the lock first.
                    let mut guard = mutex.lock().await;

                    // Do the actual fetch (exactly once).
                    let do_fetch = fetch
                        .take()
                        .expect("SimpleCache::get_or_fetch fetch called more than once");
                    let fetch_start = std::time::Instant::now();
                    let result = do_fetch().await;
                    let fetch_us = fetch_start.elapsed().as_micros() as u64;
                    self.stats.write().unwrap().fetch_time_us += fetch_us;

                    // Store the result in the mutex for waiters.
                    match &result {
                        Ok(node) => {
                            *guard = Some(Ok(node.clone()));
                        }
                        Err(e) => {
                            *guard = Some(Err(crate::error::Error::Storage(e.to_string())));
                        }
                    }

                    // Drop the guard before updating the main cache
                    drop(guard);

                    // Phase 3: If successful, upgrade InFlight to Ready
                    match result {
                        Ok(node) => {
                            let mut entries = self.entries.write().unwrap();
                            entries.insert(key.clone(), CacheEntry::Ready(node.clone()));
                            return Ok(node);
                        }
                        Err(e) => {
                            // On error, remove the InFlight entry so future requests retry
                            let mut entries = self.entries.write().unwrap();
                            entries.remove(key);
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    fn evict(&self, key: &CacheKey) {
        if self.entries.write().unwrap().remove(key).is_some() {
            self.stats.write().unwrap().evictions += 1;
        }
    }

    fn clear(&self) {
        self.entries.write().unwrap().clear();
    }

    fn len(&self) -> usize {
        // Only count Ready entries
        self.entries
            .read()
            .unwrap()
            .values()
            .filter(|v| matches!(v, CacheEntry::Ready(_)))
            .count()
    }
}

// =============================================================================
// MokaNodeCache - High-performance concurrent cache with TinyLFU eviction
// =============================================================================

/// Atomic cache statistics for lock-free hot path tracking
#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
#[derive(Debug)]
pub struct AtomicCacheStats {
    pub hits: std::sync::atomic::AtomicU64,
    pub misses: std::sync::atomic::AtomicU64,
    pub evictions: std::sync::atomic::AtomicU64,
    pub raw_hits: std::sync::atomic::AtomicU64,
    pub raw_misses: std::sync::atomic::AtomicU64,
    pub leaf_hits: std::sync::atomic::AtomicU64,
    pub leaf_misses: std::sync::atomic::AtomicU64,
    pub ready_hits: std::sync::atomic::AtomicU64,
    pub inflight_hits: std::sync::atomic::AtomicU64,
    pub inflight_wait_us: std::sync::atomic::AtomicU64,
    pub fetch_time_us: std::sync::atomic::AtomicU64,
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl Default for AtomicCacheStats {
    fn default() -> Self {
        Self {
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            raw_hits: std::sync::atomic::AtomicU64::new(0),
            raw_misses: std::sync::atomic::AtomicU64::new(0),
            leaf_hits: std::sync::atomic::AtomicU64::new(0),
            leaf_misses: std::sync::atomic::AtomicU64::new(0),
            ready_hits: std::sync::atomic::AtomicU64::new(0),
            inflight_hits: std::sync::atomic::AtomicU64::new(0),
            inflight_wait_us: std::sync::atomic::AtomicU64::new(0),
            fetch_time_us: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl AtomicCacheStats {
    /// Get a snapshot of stats as CacheStats
    pub fn snapshot(&self) -> CacheStats {
        use std::sync::atomic::Ordering::Relaxed;
        CacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            raw_hits: self.raw_hits.load(Relaxed),
            raw_misses: self.raw_misses.load(Relaxed),
            leaf_hits: self.leaf_hits.load(Relaxed),
            leaf_misses: self.leaf_misses.load(Relaxed),
            ready_hits: self.ready_hits.load(Relaxed),
            inflight_hits: self.inflight_hits.load(Relaxed),
            inflight_wait_us: self.inflight_wait_us.load(Relaxed),
            fetch_time_us: self.fetch_time_us.load(Relaxed),
        }
    }

    /// Reset all stats to zero
    pub fn reset(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.hits.store(0, Relaxed);
        self.misses.store(0, Relaxed);
        self.evictions.store(0, Relaxed);
        self.raw_hits.store(0, Relaxed);
        self.raw_misses.store(0, Relaxed);
        self.leaf_hits.store(0, Relaxed);
        self.leaf_misses.store(0, Relaxed);
        self.ready_hits.store(0, Relaxed);
        self.inflight_hits.store(0, Relaxed);
        self.inflight_wait_us.store(0, Relaxed);
        self.fetch_time_us.store(0, Relaxed);
    }
}

/// In-flight fetch state for single-flight deduplication
#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
type InFlightResult = std::result::Result<ResolvedNode, std::sync::Arc<str>>;

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
type InFlightSender = std::sync::Arc<tokio::sync::watch::Sender<Option<InFlightResult>>>;

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
type InFlightMap = dashmap::DashMap<CacheKey, InFlightSender>;

/// RAII guard to prevent orphaned in-flight entries.
///
/// If the fetcher task is dropped/cancelled mid-fetch, the guard will remove the
/// in-flight entry, dropping the last sender. This causes waiters to observe
/// channel closure and retry, rather than hanging forever.
#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
struct InFlightFetchGuard<'a> {
    key: CacheKey,
    map: &'a InFlightMap,
    tx: InFlightSender,
    finished: bool,
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl<'a> InFlightFetchGuard<'a> {
    fn new(key: CacheKey, map: &'a InFlightMap, tx: InFlightSender) -> Self {
        Self {
            key,
            map,
            tx,
            finished: false,
        }
    }

    fn finish(mut self, msg: InFlightResult) {
        // Send before removing so late subscribers still observe the result.
        let _ = self.tx.send(Some(msg));
        self.map.remove(&self.key);
        self.finished = true;
    }
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl Drop for InFlightFetchGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Fetch was abandoned; remove in-flight entry to avoid permanent hangs.
            self.map.remove(&self.key);
        }
    }
}

/// High-performance concurrent cache using moka
///
/// This implementation provides:
/// - **No global locks**: Uses sharded internal structure for high concurrency
/// - **Weighted TinyLFU eviction**: Caffeine-style admission/eviction policy
///   (frequency + recency aware, better than pure LRU for most workloads)
/// - **Single-flight deduplication**: Concurrent fetches for same key share result
///   via DashMap-based in-flight tracking
/// - **Accurate memory tracking**: Uses persisted `bytes` field from index nodes
/// - **Lock-free stats**: Atomic counters for hit/miss tracking
///
/// This is the recommended cache for production native builds.
///
/// **Note**: This type is intentionally not `Clone`. Caches should be shared
/// via `Arc<MokaNodeCache>` rather than cloned (which would be expensive and
/// semantically confusing).
#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
pub struct MokaNodeCache {
    cache: moka::sync::Cache<CacheKey, ResolvedNode>,
    in_flight: InFlightMap,
    stats: std::sync::Arc<AtomicCacheStats>,
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl std::fmt::Debug for MokaNodeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaNodeCache")
            .field("entry_count", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .field("in_flight", &self.in_flight.len())
            .finish()
    }
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
impl MokaNodeCache {
    /// Create a new moka cache with the given maximum capacity in bytes
    ///
    /// The cache will evict entries using TinyLFU policy when the total weight
    /// (measured in bytes via `ResolvedNode::size_bytes()`) exceeds this limit.
    ///
    /// **Note**: The byte limit is approximate. Each entry's weight is clamped
    /// to `u32::MAX` (4GiB), and key/overhead sizes are not included.
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        let stats = std::sync::Arc::new(AtomicCacheStats::default());
        let eviction_stats = stats.clone();

        let cache = moka::sync::Cache::builder()
            .weigher(|_key: &CacheKey, value: &ResolvedNode| -> u32 {
                // Clamp to u32::MAX (moka uses u32 for weights)
                // Most nodes are well under 4GB so this is fine
                value.size_bytes().min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .eviction_listener(move |_key, _value, _cause| {
                eviction_stats
                    .evictions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .build();

        Self {
            cache,
            in_flight: dashmap::DashMap::new(),
            stats,
        }
    }

    /// Create a new moka cache with the given maximum number of entries
    ///
    /// This is a simpler alternative to `with_max_bytes` when you don't need
    /// precise memory control. Each entry counts as weight 1.
    pub fn with_max_entries(max_entries: u64) -> Self {
        let stats = std::sync::Arc::new(AtomicCacheStats::default());
        let eviction_stats = stats.clone();

        let cache = moka::sync::Cache::builder()
            .max_capacity(max_entries)
            .eviction_listener(move |_key, _value, _cause| {
                eviction_stats
                    .evictions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .build();

        Self {
            cache,
            in_flight: dashmap::DashMap::new(),
            stats,
        }
    }

    /// Get current cache statistics (lock-free snapshot)
    pub fn stats(&self) -> CacheStats {
        self.stats.snapshot()
    }

    /// Reset cache statistics
    pub fn reset_stats(&self) {
        self.stats.reset();
    }

    /// Get the current weighted size of the cache in bytes
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Get the entry count in the cache
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Run pending maintenance tasks (eviction, etc.)
    ///
    /// Moka performs maintenance lazily, but you can call this to force
    /// immediate cleanup if needed (e.g., before memory measurements).
    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }

    fn record_hit(&self, kind: CacheKind, ready: bool, wait_us: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.stats.hits.fetch_add(1, Relaxed);
        if ready {
            self.stats.ready_hits.fetch_add(1, Relaxed);
        } else {
            self.stats.inflight_hits.fetch_add(1, Relaxed);
            self.stats.inflight_wait_us.fetch_add(wait_us, Relaxed);
        }
        match kind {
            CacheKind::Raw => self.stats.raw_hits.fetch_add(1, Relaxed),
            CacheKind::LeafTRange | CacheKind::LeafHistoryRange => {
                self.stats.leaf_hits.fetch_add(1, Relaxed)
            }
        };
    }

    fn record_miss(&self, kind: CacheKind, fetch_us: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.stats.misses.fetch_add(1, Relaxed);
        self.stats.fetch_time_us.fetch_add(fetch_us, Relaxed);
        match kind {
            CacheKind::Raw => self.stats.raw_misses.fetch_add(1, Relaxed),
            CacheKind::LeafTRange | CacheKind::LeafHistoryRange => {
                self.stats.leaf_misses.fetch_add(1, Relaxed)
            }
        };
    }
}

#[cfg(all(
    feature = "native",
    feature = "moka",
    feature = "dashmap",
    not(target_arch = "wasm32")
))]
#[async_trait]
impl NodeCache for MokaNodeCache {
    async fn get_or_fetch<F, Fut>(&self, key: &CacheKey, fetch: F) -> Result<ResolvedNode>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<ResolvedNode>> + Send,
    {
        let key_kind = key.kind;

        // Fast path: check if already cached (true ready hit)
        if let Some(node) = self.cache.get(key) {
            self.record_hit(key_kind, true, 0);
            return Ok(node);
        }

        // Check if there's an in-flight fetch we can wait on
        // Use entry API to atomically check-and-insert
        let wait_start = std::time::Instant::now();

        // Try to get or create an in-flight entry.
        let receiver = {
            use dashmap::mapref::entry::Entry;
            match self.in_flight.entry(key.clone()) {
                Entry::Occupied(entry) => {
                    // Someone else is fetching - get a receiver to wait on
                    Some(entry.get().subscribe())
                }
                Entry::Vacant(entry) => {
                    // We're the fetcher - create the watch channel
                    let (tx, _rx) = tokio::sync::watch::channel(None::<InFlightResult>);
                    entry.insert(std::sync::Arc::new(tx));
                    None
                }
            }
        };

        if let Some(mut rx) = receiver {
            // Wait for the fetcher to complete
            loop {
                // Check if result is available
                if let Some(msg) = rx.borrow().as_ref() {
                    let wait_us = wait_start.elapsed().as_micros() as u64;
                    self.record_hit(key_kind, false, wait_us);
                    match msg {
                        Ok(node) => return Ok(node.clone()),
                        Err(msg) => return Err(crate::error::Error::cache(msg.to_string())),
                    }
                }
                // Wait for change
                if rx.changed().await.is_err() {
                    // Sender dropped without sending - fall through to fetch ourselves
                    break;
                }
            }
        }

        // We're the fetcher - do the actual fetch
        // IMPORTANT: guard prevents orphaned in-flight entry if this task is dropped.
        let tx = self
            .in_flight
            .get(key)
            .expect("in-flight entry must exist for fetcher")
            .value()
            .clone();
        let fetch_guard = InFlightFetchGuard::new(key.clone(), &self.in_flight, tx);

        let fetch_start = std::time::Instant::now();
        let result = fetch().await;
        let fetch_us = fetch_start.elapsed().as_micros() as u64;

        // Store in cache on success
        if let Ok(ref node) = result {
            self.cache.insert(key.clone(), node.clone());
        }

        // Notify waiters (success OR error) and clean up in-flight entry.
        match &result {
            Ok(node) => fetch_guard.finish(Ok(node.clone())),
            Err(e) => fetch_guard.finish(Err(std::sync::Arc::<str>::from(e.to_string()))),
        }

        self.record_miss(key_kind, fetch_us);
        result
    }

    fn evict(&self, key: &CacheKey) {
        self.cache.invalidate(key);
    }

    fn clear(&self) {
        self.cache.invalidate_all();
        // Drop all senders so waiters observe closure and retry.
        self.in_flight.clear();
    }

    fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::IndexType;
    use crate::index::IndexNode;
    use std::sync::Arc;

    fn make_resolved_leaf(id: &str) -> ResolvedNode {
        ResolvedNode::leaf(
            IndexNode::leaf(id.to_string(), IndexType::Spot, "test".to_string()),
            Arc::from([]),
        )
    }

    #[tokio::test]
    async fn test_no_cache() {
        let cache = NoCache::new();
        let key = CacheKey::raw("test");

        let mut call_count = 0;
        let node = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert!(node.is_leaf());
        assert_eq!(call_count, 1);

        // Second call should also fetch
        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert_eq!(call_count, 2);
    }

    #[tokio::test]
    async fn test_simple_cache() {
        let cache = SimpleCache::new(10);
        let key = CacheKey::raw("test");

        let mut call_count = 0;

        // First call - should fetch
        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert_eq!(call_count, 1);
        assert_eq!(cache.len(), 1);

        // Second call - should use cache
        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert_eq!(call_count, 1); // Still 1, didn't fetch
        assert_eq!(cache.len(), 1);

        // Evict and try again
        cache.evict(&key);
        assert_eq!(cache.len(), 0);

        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert_eq!(call_count, 2); // Now it fetched again
    }

    #[tokio::test]
    async fn test_simple_cache_eviction() {
        let cache = SimpleCache::new(2);

        for i in 0..5 {
            let key = CacheKey::raw(format!("node{}", i));
            let _ = cache
                .get_or_fetch(&key, || async move {
                    Ok(make_resolved_leaf(&format!("node{}", i)))
                })
                .await
                .unwrap();
        }

        // Should have at most 2 entries
        assert!(cache.len() <= 2);
    }

    #[cfg(all(
        feature = "native",
        feature = "moka",
        feature = "dashmap",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn test_moka_cache() {
        let cache = MokaNodeCache::with_max_entries(10);
        let key = CacheKey::raw("test");

        let mut call_count = 0;

        // First call - should fetch
        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        cache.run_pending_tasks(); // Sync cache state
        assert_eq!(call_count, 1);
        assert_eq!(cache.len(), 1);

        // Second call - should use cache
        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        assert_eq!(call_count, 1); // Still 1, didn't fetch
        assert_eq!(cache.len(), 1);

        // Evict and try again
        cache.evict(&key);
        cache.run_pending_tasks(); // Force maintenance
        assert_eq!(cache.len(), 0);

        let _ = cache
            .get_or_fetch(&key, || {
                call_count += 1;
                async { Ok(make_resolved_leaf("test")) }
            })
            .await
            .unwrap();

        cache.run_pending_tasks(); // Sync cache state
        assert_eq!(call_count, 2); // Now it fetched again
        assert_eq!(cache.len(), 1);
    }

    #[cfg(all(
        feature = "native",
        feature = "moka",
        feature = "dashmap",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn test_moka_cache_stats() {
        let cache = MokaNodeCache::with_max_entries(10);

        // First fetch - miss
        let key1 = CacheKey::raw("node1");
        let _ = cache
            .get_or_fetch(&key1, || async { Ok(make_resolved_leaf("node1")) })
            .await
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.raw_misses, 1);

        // Second fetch of same key - hit
        let _ = cache
            .get_or_fetch(&key1, || async { Ok(make_resolved_leaf("node1")) })
            .await
            .unwrap();

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.raw_hits, 1);
    }

    #[cfg(all(
        feature = "native",
        feature = "moka",
        feature = "dashmap",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn test_moka_cache_weighted_eviction() {
        // Create cache with small max bytes (force eviction)
        // Empty leaves have minimal weight, so use a very small capacity
        let cache = MokaNodeCache::with_max_bytes(100);

        for i in 0..10 {
            let key = CacheKey::raw(format!("node{}", i));
            let _ = cache
                .get_or_fetch(&key, || async move {
                    Ok(make_resolved_leaf(&format!("node{}", i)))
                })
                .await
                .unwrap();
        }

        // Force maintenance to run eviction
        cache.run_pending_tasks();

        // Should have evicted some entries due to weight limit
        // The exact number depends on the weight of each entry
        assert!(cache.weighted_size() <= 100 || cache.entry_count() < 10);
    }
}
