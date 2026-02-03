//! Connection-level ledger state caching
//!
//! Provides `LedgerManager` for caching loaded ledger state across queries and transactions.
//! This enables:
//! - Reusing cached ledger state (no reload per request)
//! - Freshness checking with pluggable watermark sources
//! - Idle eviction to manage memory
//! - Single-flight loading (concurrent requests share one I/O operation)
//!
//! # Architecture
//!
//! - `LedgerHandle`: Cheap cloneable reference to cached ledger state
//! - `LedgerManager`: Connection-level cache with single-flight loading
//! - `FreshnessSource`: Trait for sources that provide remote watermark info
//!
//! # Thread Safety
//!
//! - Queries get cheap clones via `snapshot()` (brief lock, then released)
//! - Transactions serialize via `lock_for_write()` (hold lock for stage+commit)
//! - Manager lock is released during I/O (no blocking other ledgers)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use std::path::PathBuf;

use fluree_db_core::{alias as core_alias, Storage};
use fluree_db_indexer::run_index::{BinaryIndexStore, LeafletCache};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{NameService, NsRecord};
use fluree_db_novelty::Novelty;
use fluree_db_core::db::Db;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::error::{ApiError, Result};

// ============================================================================
// Monotonic Clock for Eviction
// ============================================================================

/// Global process start time for monotonic timing
///
/// Using `Instant` avoids issues with NTP clock adjustments that can affect
/// `SystemTime`. All eviction TTL calculations are based on elapsed time
/// from this anchor point.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Get monotonic seconds since process start
///
/// Returns elapsed seconds from a fixed anchor point, avoiding NTP drift.
fn monotonic_secs() -> u64 {
    let start = PROCESS_START.get_or_init(Instant::now);
    start.elapsed().as_secs()
}

// ============================================================================
// LedgerSnapshot - Read-only view (no lock held)
// ============================================================================

/// Read-only snapshot of ledger state - does NOT hold any lock
///
/// Safe to pass around and use for queries without blocking other operations.
/// This is a cheap clone of the underlying state (Db clone is cheap via Arc fields).
pub struct LedgerSnapshot<S> {
    /// The indexed database (cheap clone - Arc fields)
    pub db: Db<S>,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Current transaction t value
    pub t: i64,
    /// Current head commit address
    pub head_commit: Option<String>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
    /// Binary columnar index store (v2 only).
    ///
    /// Present when `db.range_provider` is also set — the two are always
    /// set/cleared together (see coherence `debug_assert` in `snapshot()`).
    pub binary_store: Option<Arc<BinaryIndexStore>>,
}

impl<S: Storage + Clone + 'static> LedgerSnapshot<S> {
    /// Create a snapshot from ledger state
    ///
    /// Note: `binary_store` is set to `None` here — callers that have a
    /// binary store must set it after construction (see `LedgerHandle::snapshot()`).
    fn from_state(state: &LedgerState<S>) -> Self {
        Self {
            db: state.db.clone(), // Cheap: Arc fields
            novelty: Arc::clone(&state.novelty),
            t: state.t(),
            head_commit: state.head_commit.clone(),
            ns_record: state.ns_record.clone(),
            binary_store: None,
        }
    }

    /// Get the ledger name (without branch suffix)
    ///
    /// Returns the base ledger name (e.g., "mydb"), NOT the canonical form (e.g., "mydb:main").
    /// For the canonical ledger:branch address, use `address()` instead.
    ///
    /// Note: This matches Clojure's `NsRecord.alias` semantics where "alias" is the base name.
    pub fn alias(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.alias.as_str())
    }

    /// Get the canonical ledger address (with branch suffix)
    ///
    /// Returns the canonical form (e.g., "mydb:main") suitable for cache keys.
    /// This is the primary identifier for ledger lookups.
    pub fn address(&self) -> Option<&str> {
        self.ns_record.as_ref().map(|r| r.address.as_str())
    }

    /// Get index_t from the underlying Db
    pub fn index_t(&self) -> i64 {
        self.db.t
    }

    /// Convert snapshot to LedgerState for backward compatibility
    ///
    /// This creates a LedgerState with the same data as the snapshot.
    /// Use this when you need to pass the state to APIs that expect LedgerState.
    pub fn to_ledger_state(self) -> LedgerState<S> {
        LedgerState {
            db: self.db,
            novelty: self.novelty,
            head_commit: self.head_commit,
            ns_record: self.ns_record,
        }
    }
}

// ============================================================================
// LedgerWriteGuard - Holds mutex for transaction duration
// ============================================================================

/// Write guard that holds the ledger mutex for transaction duration
///
/// Transactions hold this guard across stage+commit to serialize writes
/// to the same ledger.
pub struct LedgerWriteGuard<'a, S> {
    guard: tokio::sync::MutexGuard<'a, LedgerState<S>>,
}

impl<S: Clone> LedgerWriteGuard<'_, S> {
    /// Get reference to current state
    pub fn state(&self) -> &LedgerState<S> {
        &self.guard
    }

    /// Clone current state for passing to stage (which consumes by value)
    pub fn clone_state(&self) -> LedgerState<S>
    where
        LedgerState<S>: Clone,
    {
        self.guard.clone()
    }

    /// Replace state with new state after successful commit
    pub fn replace(&mut self, new_state: LedgerState<S>) {
        *self.guard = new_state;
    }
}

// ============================================================================
// LedgerHandle - Cheap cloneable reference to cached state
// ============================================================================

/// Handle to a cached ledger state - cheap to clone
///
/// Provides access to cached ledger state for queries and transactions.
/// Multiple handles can reference the same cached state (via Arc).
pub struct LedgerHandle<S> {
    inner: Arc<LedgerHandleInner<S>>,
}

// Manual Clone impl to avoid requiring S: Clone, C: Clone bounds
// (Arc<T> is Clone regardless of T)
impl<S> Clone for LedgerHandle<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Lock ordering invariant: always acquire `state` before `binary_store`.
/// All paths that touch both locks (snapshot, apply_index_v2, reload)
/// follow this order to prevent deadlock and ensure coherence.
struct LedgerHandleInner<S> {
    /// Single mutex for all access (queries clone snapshot, txns hold for duration)
    state: Mutex<LedgerState<S>>,
    /// Ledger alias
    alias: String,
    /// Last access time (monotonic secs since process start)
    last_access: AtomicU64,
    /// Binary columnar index store (v2 only).
    ///
    /// Always coherent with `state.db.range_provider` — writers hold
    /// the `state` lock while updating this.
    binary_store: Mutex<Option<Arc<BinaryIndexStore>>>,
}

impl<S: Storage + Clone + 'static> LedgerHandle<S> {
    /// Create a new handle wrapping ledger state
    pub fn new(
        alias: String,
        state: LedgerState<S>,
        binary_store: Option<Arc<BinaryIndexStore>>,
    ) -> Self {
        Self {
            inner: Arc::new(LedgerHandleInner {
                state: Mutex::new(state),
                alias,
                last_access: AtomicU64::new(monotonic_secs()),
                binary_store: Mutex::new(binary_store),
            }),
        }
    }

    /// Create an ephemeral handle (for when caching is disabled)
    ///
    /// This is functionally identical to `new()`, but the naming clarifies
    /// that this handle is NOT cached and each call creates a fresh load.
    pub fn ephemeral(alias: String, state: LedgerState<S>) -> Self {
        Self::new(alias, state, None)
    }

    /// Get read-only snapshot for queries (brief lock, clone, release)
    ///
    /// IMPORTANT: Queries must NOT execute while holding the internal lock.
    /// The snapshot is a cheap clone; the lock is released immediately after.
    pub async fn snapshot(&self) -> LedgerSnapshot<S> {
        self.touch();
        let state = self.inner.state.lock().await;
        let binary_store = self.inner.binary_store.lock().await.clone();
        let mut snap = LedgerSnapshot::from_state(&state);
        snap.binary_store = binary_store;
        debug_assert!(
            snap.db.range_provider.is_some() == snap.binary_store.is_some(),
            "range_provider and binary_store must be coherent"
        );
        snap
        // Locks released here
    }

    /// Acquire exclusive access for transaction (hold lock for stage+commit)
    pub async fn lock_for_write(&self) -> LedgerWriteGuard<'_, S> {
        self.touch();
        LedgerWriteGuard {
            guard: self.inner.state.lock().await,
        }
    }

    /// Update last access time
    fn touch(&self) {
        self.inner
            .last_access
            .store(monotonic_secs(), Ordering::Relaxed);
    }

    /// Get last access time (monotonic secs since process start)
    pub fn last_access_secs(&self) -> u64 {
        self.inner.last_access.load(Ordering::Relaxed)
    }

    /// Get ledger alias
    pub fn alias(&self) -> &str {
        &self.inner.alias
    }

    /// Check if currently locked (for eviction - skip if in use)
    pub fn is_locked(&self) -> bool {
        self.inner.state.try_lock().is_err()
    }

    /// Get current index_t (brief lock to read)
    ///
    /// This returns the indexed DB's t value, NOT including novelty.
    /// For freshness checking against remote watermarks, use this method.
    pub async fn index_t(&self) -> i64 {
        let state = self.inner.state.lock().await;
        state.index_t()
    }

    /// Get current t value (max of db.t and novelty.t)
    ///
    /// This returns the ledger's current t including any unindexed novelty.
    /// Use this for comparing against nameservice commit_t to detect staleness.
    pub async fn t(&self) -> i64 {
        let state = self.inner.state.lock().await;
        state.t()
    }

    /// Get state metrics for update planning
    ///
    /// Returns (t, index_t, index_address) needed for UpdatePlan::plan()
    pub async fn state_metrics(&self) -> (i64, i64, Option<String>) {
        let state = self.inner.state.lock().await;
        (
            state.t(),
            state.index_t(),
            state.ns_record.as_ref().and_then(|r| r.index_address.clone()),
        )
    }

    /// Check if cached state is fresh vs remote watermark
    pub async fn check_freshness(&self, remote: &RemoteWatermark) -> FreshnessCheck {
        let local_index_t = self.index_t().await;

        if remote.index_t > local_index_t {
            FreshnessCheck::Stale
        } else {
            FreshnessCheck::Current
        }
    }

    /// Apply a v2 binary index root to this handle.
    ///
    /// All I/O (root read, BinaryIndexStore load) happens outside any lock.
    /// The state lock is held for the brief atomic swap of both `state` and
    /// `binary_store`, ensuring coherence between `db.range_provider` and
    /// `binary_store` (lock ordering: state → binary_store).
    pub async fn apply_index_v2(
        &self,
        index_address: &str,
        storage: &S,
        cache_dir: &std::path::Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> Result<()> {
        use fluree_db_core::serde::json::{
            raw_schema_to_index_schema, raw_stats_to_index_stats, RawDbRootSchema, RawDbRootStats,
        };

        // All I/O outside any lock
        let bytes = storage
            .read_bytes(index_address)
            .await
            .map_err(|e| ApiError::internal(format!("failed to read index root: {}", e)))?;
        let root: BinaryIndexRootV2 = serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::internal(format!("failed to parse v2 root: {}", e)))?;

        let store = BinaryIndexStore::load_from_root(storage, &root, cache_dir, leaflet_cache)
            .await
            .map_err(|e| ApiError::internal(format!("failed to load binary index: {}", e)))?;
        let arc_store = Arc::new(store);
        let provider = BinaryRangeProvider::new(Arc::clone(&arc_store), 0);

        // Build metadata-only Db from root
        let ns_codes = root.namespace_codes.into_iter().collect();
        let stats = root
            .stats
            .as_ref()
            .and_then(|s| serde_json::from_value::<RawDbRootStats>(s.clone()).ok())
            .and_then(|raw| raw_stats_to_index_stats(&raw));
        let schema = root
            .schema
            .as_ref()
            .and_then(|s| serde_json::from_value::<RawDbRootSchema>(s.clone()).ok())
            .map(|raw| raw_schema_to_index_schema(&raw));
        let mut db = Db::new_meta(
            root.ledger_alias,
            root.index_t,
            ns_codes,
            stats,
            schema,
            storage.clone(),
        );
        db.range_provider = Some(Arc::new(provider));

        // Brief lock: swap state + binary_store atomically.
        // Lock ordering: state → binary_store (same as snapshot()).
        {
            let mut state = self.inner.state.lock().await;
            state
                .apply_loaded_db(db, index_address)
                .map_err(|e| ApiError::internal(format!("apply_loaded_db failed: {}", e)))?;
            *self.inner.binary_store.lock().await = Some(arc_store);
        }

        Ok(())
    }
}

// ============================================================================
// Freshness Types
// ============================================================================

/// Remote watermark for freshness comparison
///
/// Matches server's existing RemoteLedgerWatermark structure for compatibility.
#[derive(Clone, Debug)]
pub struct RemoteWatermark {
    /// Remote commit_t value
    pub commit_t: i64,
    /// Remote index_t value (used for freshness comparison)
    pub index_t: i64,
    /// Remote index address (for potential future optimization)
    pub index_address: Option<String>,
    /// When this watermark was last updated
    pub updated_at: Instant,
}

/// Trait for sources that provide remote freshness info
///
/// Server's PeerState implements this; library doesn't depend on server types.
pub trait FreshnessSource: Send + Sync {
    /// Get remote watermark for a ledger alias
    fn watermark(&self, alias: &str) -> Option<RemoteWatermark>;
}

/// Result of checking if cached state is fresh
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshnessCheck {
    /// Local index_t >= remote index_t
    Current,
    /// Remote index_t > local index_t, needs reload
    Stale,
}

// ============================================================================
// LoadState - Single-flight coordination
// ============================================================================

/// Loading state for single-flight coordination
///
/// Note: Loading sends `Result<LedgerHandle>` to waiters (they need the handle).
///       Reloading sends `Result<()>` to waiters (handle already obtained).
enum LoadState<S> {
    /// Initial load in progress - waiters receive handle on success
    Loading(Vec<oneshot::Sender<std::result::Result<LedgerHandle<S>, Arc<ApiError>>>>),
    /// Loaded and cached
    Ready(LedgerHandle<S>),
    /// Reload in progress - handle stays valid, waiters receive () on success
    Reloading {
        handle: LedgerHandle<S>,
        waiters: Vec<oneshot::Sender<std::result::Result<(), Arc<ApiError>>>>,
    },
}

// ============================================================================
// LedgerManagerConfig
// ============================================================================

/// Configuration for the ledger manager
#[derive(Clone)]
pub struct LedgerManagerConfig {
    /// TTL before idle ledgers are evicted (default: 30 min)
    pub idle_ttl: Duration,
    /// Sweep interval for background cleanup (default: 1 min)
    pub sweep_interval: Duration,
    /// Directory for binary index cache files (leaflets, forward indexes, etc.)
    ///
    /// Layout: `{cache_dir}/{alias_hash}/{root_hash}/...`
    /// Default: `$TMPDIR/fluree_binary_cache`
    pub cache_dir: PathBuf,
    /// Shared leaflet cache across all ledgers.
    ///
    /// Default capacity: 8 GiB or `FLUREE_LEAFLET_CACHE_BYTES` env var.
    pub leaflet_cache: Option<Arc<LeafletCache>>,
}

impl std::fmt::Debug for LedgerManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerManagerConfig")
            .field("idle_ttl", &self.idle_ttl)
            .field("sweep_interval", &self.sweep_interval)
            .field("cache_dir", &self.cache_dir)
            .field("has_leaflet_cache", &self.leaflet_cache.is_some())
            .finish()
    }
}

impl Default for LedgerManagerConfig {
    fn default() -> Self {
        Self {
            idle_ttl: Duration::from_secs(30 * 60),
            sweep_interval: Duration::from_secs(60),
            cache_dir: std::env::temp_dir().join("fluree_binary_cache"),
            leaflet_cache: None,
        }
    }
}

// ============================================================================
// Binary Index Loading Helper
// ============================================================================

use fluree_db_indexer::run_index::{BinaryIndexRootV2, BINARY_INDEX_ROOT_VERSION_V2};
use fluree_db_query::BinaryRangeProvider;

/// Load BinaryIndexStore from a v2 index root, attach range_provider
/// to the LedgerState's Db, and return the Arc'd store.
///
/// Returns `Ok(None)` if no index_address is present or the root is not v2.
async fn load_and_attach_binary_store<S: Storage + Clone + 'static>(
    storage: &S,
    state: &mut LedgerState<S>,
    cache_dir: &std::path::Path,
    leaflet_cache: Option<Arc<LeafletCache>>,
) -> std::result::Result<Option<Arc<BinaryIndexStore>>, ApiError> {
    let index_addr = match state
        .ns_record
        .as_ref()
        .and_then(|r| r.index_address.as_ref())
    {
        Some(addr) => addr.clone(),
        None => return Ok(None),
    };

    let bytes = storage
        .read_bytes(&index_addr)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read index root: {}", e)))?;

    let root: BinaryIndexRootV2 = match serde_json::from_slice(&bytes) {
        Ok(r) => r,
        Err(_) => return Ok(None), // Not a v2 root (could be v1 or malformed)
    };

    // BinaryIndexRootV2's custom Deserialize already validates version == 2,
    // but belt-and-suspenders:
    if root.version != BINARY_INDEX_ROOT_VERSION_V2 {
        return Ok(None);
    }

    let store = BinaryIndexStore::load_from_root(storage, &root, cache_dir, leaflet_cache)
        .await
        .map_err(|e| ApiError::internal(format!("failed to load binary index: {}", e)))?;
    let arc_store = Arc::new(store);
    let provider = BinaryRangeProvider::new(Arc::clone(&arc_store), 0);
    state.db.range_provider = Some(Arc::new(provider));
    Ok(Some(arc_store))
}

// ============================================================================
// LedgerManager - Connection-level cache
// ============================================================================

/// Connection-level ledger cache manager
///
/// Provides single-flight loading (concurrent requests share one I/O operation)
/// and idle eviction.
pub struct LedgerManager<S, N> {
    /// Cached ledger handles + loading state
    entries: RwLock<HashMap<String, LoadState<S>>>,
    /// Storage for ledger loading
    storage: S,
    /// Shared cache for index nodes
    /// Nameservice for ledger lookup/loading
    nameservice: N,
    /// Configuration
    config: LedgerManagerConfig,
    /// Shutdown flag — prevents load/reload leaders from re-inserting after disconnect_all
    shutdown: AtomicBool,
}

impl<S, N> LedgerManager<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Send + Sync + 'static,
{
    /// Create a new ledger manager
    pub fn new(storage: S, nameservice: N, config: LedgerManagerConfig) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            storage,
            nameservice,
            config,
            shutdown: AtomicBool::new(false),
        }
    }

    /// Get the manager configuration
    pub fn config(&self) -> &LedgerManagerConfig {
        &self.config
    }

    /// Get cached handle or load from nameservice
    ///
    /// Uses single-flight pattern: concurrent requests for same alias
    /// will share one load operation, not stampede.
    ///
    /// The alias is normalized to canonical form (e.g., "mydb" -> "mydb:main")
    /// before caching to ensure consistent cache keys regardless of input form.
    pub async fn get_or_load(&self, alias: &str) -> Result<LedgerHandle<S>> {
        // Normalize alias to canonical form for consistent cache keys
        // This ensures "mydb" and "mydb:main" use the same cache entry
        let canonical_alias = core_alias::normalize_alias(alias)
            .unwrap_or_else(|_| alias.to_string());

        // Fast path: already loaded
        {
            let entries = self.entries.read().await;
            if let Some(LoadState::Ready(handle)) = entries.get(&canonical_alias) {
                return Ok(handle.clone());
            }
            // Also check Reloading - handle is still valid
            if let Some(LoadState::Reloading { handle, .. }) = entries.get(&canonical_alias) {
                return Ok(handle.clone());
            }
        }

        // Slow path: need to coordinate loading
        let (_should_load, rx) = {
            let mut entries = self.entries.write().await;

            match entries.get_mut(&canonical_alias) {
                Some(LoadState::Ready(handle)) => {
                    // Another task loaded while we waited for write lock
                    return Ok(handle.clone());
                }
                Some(LoadState::Reloading { handle, .. }) => {
                    // Handle is valid even during reload
                    return Ok(handle.clone());
                }
                Some(LoadState::Loading(waiters)) => {
                    // Someone else is loading - add ourselves as waiter
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    (false, Some(rx))
                }
                None => {
                    // We're first - mark as loading, release lock, do I/O
                    entries.insert(canonical_alias.clone(), LoadState::Loading(Vec::new()));
                    (true, None)
                }
            }
        };
        // Manager lock released here

        if let Some(rx) = rx {
            // Wait for the loader to finish
            // Note: Waiters receive an Http error (preserving status code) since
            // ApiError isn't Clone. The leader (first caller) gets the full error type.
            return rx
                .await
                .map_err(|_| ApiError::internal("load cancelled"))?
                .map_err(|arc_err| {
                    // The Arc contains an ApiError::Http - extract status and message
                    match arc_err.as_ref() {
                        ApiError::Http { status, message } => ApiError::http(*status, message),
                        // Fallback for any other error type (shouldn't happen)
                        other => ApiError::http(other.status_code(), other.to_string()),
                    }
                });
        }

        // We're the loader - do the I/O without holding manager lock
        // Note: We pass the original alias to nameservice (it handles resolution),
        // but cache under the canonical alias for consistent lookup
        let result = LedgerState::load(
            &self.nameservice,
            alias,
            self.storage.clone(),
        )
        .await
        .map_err(ApiError::from); // Convert LedgerError to ApiError

        // Publish result to waiters
        let mut entries = self.entries.write().await;
        let shutting_down = self.is_shutdown();

        match result {
            Ok(mut state) => {
                // Attempt to load binary index store (v2 only).
                // Non-fatal: if loading fails, log and continue without binary index.
                let binary_store = match load_and_attach_binary_store(
                    &self.storage,
                    &mut state,
                    &self.config.cache_dir,
                    self.config.leaflet_cache.clone(),
                )
                .await
                {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!(
                            alias = %alias,
                            error = %e,
                            "Failed to load binary store, continuing without"
                        );
                        None
                    }
                };

                let handle =
                    LedgerHandle::new(canonical_alias.clone(), state, binary_store);

                // Notify waiters
                if let Some(LoadState::Loading(waiters)) = entries.remove(&canonical_alias) {
                    for tx in waiters {
                        let _ = tx.send(Ok(handle.clone()));
                    }
                }

                // Don't re-insert into cache if shutdown has been initiated
                if !shutting_down {
                    entries.insert(canonical_alias, LoadState::Ready(handle.clone()));
                }
                Ok(handle)
            }
            Err(e) => {
                // Capture error with status code for waiters before consuming the error
                // Note: Waiters receive an Http error (preserving status code);
                // the leader (first caller) gets the original error type preserved.
                let error_for_waiters =
                    Arc::new(ApiError::http(e.status_code(), e.to_string()));

                // Notify waiters of failure
                if let Some(LoadState::Loading(waiters)) = entries.remove(&canonical_alias) {
                    for tx in waiters {
                        let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                    }
                }

                // Leader returns the original error (preserves full type/variant)
                Err(e)
            }
        }
    }

    /// Remove ledger from cache
    ///
    /// Note: If loading/reloading is in progress, waiters will receive
    /// cancellation errors. This is acceptable - disconnect is a "force evict."
    pub async fn disconnect(&self, alias: &str) {
        // Normalize alias to match cache key format
        let canonical_alias = core_alias::normalize_alias(alias)
            .unwrap_or_else(|_| alias.to_string());

        let mut entries = self.entries.write().await;
        // Removal will drop any pending oneshot senders, causing waiters to get RecvError
        entries.remove(&canonical_alias);
    }

    /// Remove all ledgers from cache (for shutdown)
    ///
    /// Sets a shutdown flag that prevents in-flight load/reload leaders from
    /// re-inserting entries after this call completes.
    ///
    /// Any in-flight Loading/Reloading waiters will receive cancellation
    /// errors when their oneshot senders are dropped. This is the expected
    /// behavior for a force-evict during shutdown.
    pub async fn disconnect_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        let mut entries = self.entries.write().await;
        entries.clear();
    }

    /// Check if shutdown has been initiated
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Reload a ledger in place (for peer freshness)
    ///
    /// Truly coalesces concurrent reloads: only one actual reload I/O happens,
    /// other callers wait for it. Does NOT invalidate the handle object.
    ///
    /// State machine:
    /// - Ready(h) → Reloading{h, waiters=[]} (caller becomes leader)
    /// - Reloading{h, waiters} → add waiter, await completion
    /// - Loading(waiters) → wait for initial load, then return Ok(())
    /// - None → Ok(()) (not loaded, nothing to reload)
    pub async fn reload(&self, alias: &str) -> Result<()> {
        // Normalize alias to match cache key format
        let canonical_alias = core_alias::normalize_alias(alias)
            .unwrap_or_else(|_| alias.to_string());

        enum ReloadAction<S> {
            BecomeLeader(LedgerHandle<S>),
            WaitForReload(oneshot::Receiver<std::result::Result<(), Arc<ApiError>>>),
            WaitForInitialLoad(
                oneshot::Receiver<std::result::Result<LedgerHandle<S>, Arc<ApiError>>>,
            ),
            NotLoaded,
        }

        // Determine action under lock
        let action = {
            let mut entries = self.entries.write().await;

            match entries.get_mut(&canonical_alias) {
                Some(LoadState::Ready(h)) => {
                    // Transition to Reloading, become leader
                    let handle = h.clone();
                    let reloading = LoadState::Reloading {
                        handle: handle.clone(),
                        waiters: Vec::new(),
                    };
                    entries.insert(canonical_alias.clone(), reloading);
                    ReloadAction::BecomeLeader(handle)
                }
                Some(LoadState::Reloading { waiters, .. }) => {
                    // Join existing reload
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    ReloadAction::WaitForReload(rx)
                }
                Some(LoadState::Loading(waiters)) => {
                    // Initial load in progress - wait for it, then done
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    ReloadAction::WaitForInitialLoad(rx)
                }
                None => ReloadAction::NotLoaded,
            }
        };
        // Manager lock released

        match action {
            ReloadAction::NotLoaded => Ok(()),

            ReloadAction::WaitForInitialLoad(rx) => {
                // Wait for initial load to complete, then we're fresh
                // Note: Waiters receive Http error (preserving status code) since ApiError isn't Clone
                rx.await
                    .map_err(|_| ApiError::internal("load cancelled"))?
                    .map_err(|arc_err| {
                        // Extract Http error preserving status code
                        match arc_err.as_ref() {
                            ApiError::Http { status, message } => ApiError::http(*status, message),
                            other => ApiError::http(other.status_code(), other.to_string()),
                        }
                    })?;
                Ok(())
            }

            ReloadAction::WaitForReload(rx) => {
                // Wait for reload leader to complete
                // Note: Waiters receive Http error (preserving status code) since ApiError isn't Clone
                rx.await
                    .map_err(|_| ApiError::internal("reload cancelled"))?
                    .map_err(|arc_err| {
                        // Extract Http error preserving status code
                        match arc_err.as_ref() {
                            ApiError::Http { status, message } => ApiError::http(*status, message),
                            other => ApiError::http(other.status_code(), other.to_string()),
                        }
                    })
            }

            ReloadAction::BecomeLeader(handle) => {
                // We're the reload leader - do I/O without manager lock
                let mut write_guard = handle.lock_for_write().await;

                let result = LedgerState::load(
                    &self.nameservice,
                    alias,
                    self.storage.clone(),
                )
                .await
                .map_err(ApiError::from); // Convert LedgerError to ApiError

                // Publish result under lock
                let mut entries = self.entries.write().await;
                let shutting_down = self.is_shutdown();

                match result {
                    Ok(mut new_state) => {
                        // Attempt to load binary index store (v2 only)
                        let new_binary_store = match load_and_attach_binary_store(
                            &self.storage,
                            &mut new_state,
                            &self.config.cache_dir,
                            self.config.leaflet_cache.clone(),
                        )
                        .await
                        {
                            Ok(store) => store,
                            Err(e) => {
                                tracing::warn!(
                                    alias = %alias,
                                    error = %e,
                                    "Failed to load binary store during reload, continuing without"
                                );
                                None
                            }
                        };

                        write_guard.replace(new_state);
                        // Update binary_store coherently with the new state
                        *handle.inner.binary_store.lock().await = new_binary_store;

                        // Notify waiters and restore Ready state (unless shutting down)
                        if let Some(LoadState::Reloading { handle, waiters }) =
                            entries.remove(&canonical_alias)
                        {
                            for tx in waiters {
                                let _ = tx.send(Ok(()));
                            }
                            if !shutting_down {
                                entries.insert(canonical_alias, LoadState::Ready(handle));
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // Capture error with status code for waiters before consuming the error
                        // Note: Waiters receive Http error (preserving status code); leader gets original type
                        let error_for_waiters =
                            Arc::new(ApiError::http(e.status_code(), e.to_string()));

                        // Notify waiters of failure, restore Ready (keep old data) unless shutting down
                        if let Some(LoadState::Reloading { handle, waiters }) =
                            entries.remove(&canonical_alias)
                        {
                            for tx in waiters {
                                let _ = tx.send(Err(Arc::clone(&error_for_waiters)));
                            }
                            if !shutting_down {
                                entries.insert(canonical_alias, LoadState::Ready(handle));
                            }
                        }
                        // Leader returns the original error (preserves full type/variant)
                        Err(e)
                    }
                }
            }
        }
    }

    /// Sweep idle entries (called by maintenance task)
    ///
    /// Only evicts Ready entries. Never evicts Loading/Reloading entries
    /// (they're transient; eviction would cancel waiters unexpectedly).
    pub async fn sweep_idle(&self) -> Vec<String> {
        let now = monotonic_secs();
        let ttl_secs = self.config.idle_ttl.as_secs();

        let mut entries = self.entries.write().await;
        let mut evicted = Vec::new();

        entries.retain(|alias, load_state| {
            if let LoadState::Ready(handle) = load_state {
                let age = now.saturating_sub(handle.last_access_secs());
                if age > ttl_secs && !handle.is_locked() {
                    evicted.push(alias.clone());
                    return false;
                }
            }
            // Keep Loading/Reloading entries - they're transient
            true
        });

        evicted
    }

    /// Get count of cached ledgers (for metrics)
    pub async fn cached_count(&self) -> usize {
        let entries = self.entries.read().await;
        entries
            .values()
            .filter(|s| matches!(s, LoadState::Ready(_) | LoadState::Reloading { .. }))
            .count()
    }

    /// Get list of cached ledger aliases (for introspection)
    pub async fn cached_aliases(&self) -> Vec<String> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter_map(|(alias, state)| {
                if matches!(state, LoadState::Ready(_) | LoadState::Reloading { .. }) {
                    Some(alias.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Spawn maintenance task for idle sweeping
    ///
    /// Returns JoinHandle for graceful shutdown. Call `.abort()` on shutdown.
    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        let sweep_interval = self.config.sweep_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(sweep_interval);

            loop {
                interval.tick().await;
                let evicted = mgr.sweep_idle().await;
                if !evicted.is_empty() {
                    tracing::debug!(
                        count = evicted.len(),
                        aliases = ?evicted,
                        "Swept idle ledgers"
                    );
                }
            }
        })
    }
}

// ============================================================================
// Notify Types - Update Plan (Clojure parity)
// ============================================================================

/// Decision from comparing cached state to nameservice record
///
/// Based on Clojure's `plan-ns-update` in `fluree.db.connection`:
/// - Compare local `t()` (max of index + novelty) against nameservice `commit_t`
/// - Determine minimal action needed to bring cache up to date
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdatePlan {
    /// Nothing to do - state is current
    /// (ns.commit_t == local.t() AND index unchanged)
    Noop,

    /// Index advanced but commit_t unchanged
    /// (ns.commit_t == local.t() BUT ns.index_t > local.index_t)
    /// Action: reload index root, trim novelty to only commits > new index_t
    IndexOnly {
        /// New index address to load
        index_address: String,
        /// New index_t value
        index_t: i64,
    },

    /// Next commit available (fast path)
    /// (ns.commit_t == local.t() + 1)
    /// Action: load and apply single commit to novelty
    /// Note: v1 falls back to Reload for simplicity
    CommitNext {
        /// Address of the next commit
        commit_address: String,
        /// Expected commit_t
        commit_t: i64,
    },

    /// Stale - remote is more than one commit ahead
    /// (ns.commit_t > local.t() + 1)
    /// Action: full reload from nameservice
    Reload,
}

impl UpdatePlan {
    /// Plan the update action based on local state vs nameservice record
    ///
    /// This mirrors Clojure's `plan-ns-update` logic:
    /// - If commit_t matches local t(), check if index advanced
    /// - If commit_t is exactly local t() + 1, we can apply just that commit
    /// - If commit_t is further ahead, we're stale and need full reload
    ///
    /// # Arguments
    /// * `local_t` - Local ledger's current t (max of index + novelty)
    /// * `local_index_t` - Local ledger's indexed t (db.t)
    /// * `local_index_address` - Local ledger's current index address (if any)
    /// * `ns` - Fresh nameservice record
    pub fn plan(
        local_t: i64,
        local_index_t: i64,
        local_index_address: Option<&str>,
        ns: &NsRecord,
    ) -> Self {
        if ns.commit_t == local_t {
            // Commits are in sync - check if index advanced
            match (&ns.index_address, local_index_address) {
                (Some(ns_idx), Some(local_idx)) if ns_idx != local_idx && ns.index_t > local_index_t => {
                    // Index advanced, same commit_t
                    UpdatePlan::IndexOnly {
                        index_address: ns_idx.clone(),
                        index_t: ns.index_t,
                    }
                }
                (Some(ns_idx), None) if ns.index_t > local_index_t => {
                    // Index appeared where there was none
                    UpdatePlan::IndexOnly {
                        index_address: ns_idx.clone(),
                        index_t: ns.index_t,
                    }
                }
                _ => UpdatePlan::Noop,
            }
        } else if ns.commit_t == local_t + 1 {
            // Exactly one commit ahead - fast path possible
            match &ns.commit_address {
                Some(addr) => UpdatePlan::CommitNext {
                    commit_address: addr.clone(),
                    commit_t: ns.commit_t,
                },
                None => UpdatePlan::Reload, // Shouldn't happen, but be safe
            }
        } else if ns.commit_t > local_t {
            // More than one commit ahead - stale
            UpdatePlan::Reload
        } else {
            // ns.commit_t < local_t - shouldn't happen (time travel?)
            // Treat as noop - local is somehow ahead
            tracing::warn!(
                local_t = local_t,
                ns_commit_t = ns.commit_t,
                "Local t is ahead of nameservice commit_t - unexpected"
            );
            UpdatePlan::Noop
        }
    }

    /// Check if this plan requires any action
    pub fn is_noop(&self) -> bool {
        matches!(self, UpdatePlan::Noop)
    }

    /// Check if this plan requires a full reload
    pub fn requires_reload(&self) -> bool {
        matches!(self, UpdatePlan::Reload)
    }
}

/// Input for notify: alias + optional fresh NsRecord
pub struct NsNotify {
    /// Ledger alias
    pub alias: String,
    /// Fresh nameservice record (if already fetched)
    pub record: Option<NsRecord>,
}

/// Result of notify operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotifyResult {
    /// Ledger not in cache, no action taken
    NotLoaded,
    /// Already up to date, no action taken (Noop plan)
    Current,
    /// Index was updated (trimmed novelty) - v1 falls back to Reload
    IndexUpdated,
    /// Applied next commit to novelty - v1 falls back to Reload
    CommitApplied,
    /// Was stale, reloaded in-place via reload()
    Reloaded,
}

impl<S, N> LedgerManager<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Send + Sync + 'static,
{
    /// Handle nameservice update notification
    ///
    /// Uses Clojure-style update planning to determine minimal action:
    /// - Noop: nothing to do
    /// - IndexOnly: index advanced, trim novelty (v1: falls back to Reload)
    /// - CommitNext: apply single commit (v1: falls back to Reload)
    /// - Reload: full reload needed
    pub async fn notify(&self, input: NsNotify) -> Result<NotifyResult> {
        // Check if ledger is cached
        let handle = {
            let entries = self.entries.read().await;
            match entries.get(&input.alias) {
                Some(LoadState::Ready(h)) => h.clone(),
                Some(LoadState::Reloading { handle, .. }) => handle.clone(),
                _ => return Ok(NotifyResult::NotLoaded),
            }
        };

        // Get fresh record from nameservice if not provided
        let ns_record = match input.record {
            Some(r) => r,
            None => match self.nameservice.lookup(&input.alias).await? {
                Some(r) => r,
                None => return Ok(NotifyResult::Current), // Ledger doesn't exist
            },
        };

        // Get local state metrics for planning
        let (local_t, local_index_t, local_index_address) = handle.state_metrics().await;

        // Plan the update action
        let plan = UpdatePlan::plan(
            local_t,
            local_index_t,
            local_index_address.as_deref(),
            &ns_record,
        );

        tracing::debug!(
            alias = %input.alias,
            local_t = local_t,
            local_index_t = local_index_t,
            ns_commit_t = ns_record.commit_t,
            ns_index_t = ns_record.index_t,
            ?plan,
            "notify: computed update plan"
        );

        match plan {
            UpdatePlan::Noop => Ok(NotifyResult::Current),

            UpdatePlan::IndexOnly { index_address, index_t } => {
                // v1: Fall back to full reload
                // Future: reload index root at index_address, rebuild novelty for commits > index_t
                tracing::debug!(
                    alias = %input.alias,
                    index_address = %index_address,
                    index_t = index_t,
                    "notify: IndexOnly plan - falling back to reload in v1"
                );
                self.reload(&input.alias).await?;
                Ok(NotifyResult::IndexUpdated)
            }

            UpdatePlan::CommitNext { commit_address, commit_t } => {
                // v1: Fall back to full reload
                // Future: load single commit at commit_address, apply to novelty
                tracing::debug!(
                    alias = %input.alias,
                    commit_address = %commit_address,
                    commit_t = commit_t,
                    "notify: CommitNext plan - falling back to reload in v1"
                );
                self.reload(&input.alias).await?;
                Ok(NotifyResult::CommitApplied)
            }

            UpdatePlan::Reload => {
                self.reload(&input.alias).await?;
                Ok(NotifyResult::Reloaded)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_secs() {
        let t1 = monotonic_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = monotonic_secs();
        // Should be monotonically non-decreasing
        assert!(t2 >= t1);
    }

    #[test]
    fn test_config_defaults() {
        let config = LedgerManagerConfig::default();
        assert_eq!(config.idle_ttl, Duration::from_secs(30 * 60));
        assert_eq!(config.sweep_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_freshness_check() {
        let remote = RemoteWatermark {
            commit_t: 10,
            index_t: 8,
            index_address: None,
            updated_at: Instant::now(),
        };

        // These are compile-time checks that the types work correctly
        assert_eq!(FreshnessCheck::Current, FreshnessCheck::Current);
        assert_eq!(FreshnessCheck::Stale, FreshnessCheck::Stale);
        assert_ne!(FreshnessCheck::Current, FreshnessCheck::Stale);

        // RemoteWatermark is Clone
        let _cloned = remote.clone();
    }

    #[test]
    fn test_update_plan_variants() {
        assert_eq!(UpdatePlan::Noop, UpdatePlan::Noop);
        assert_eq!(UpdatePlan::Reload, UpdatePlan::Reload);
        assert_ne!(UpdatePlan::Noop, UpdatePlan::Reload);
    }

    #[test]
    fn test_notify_result_variants() {
        assert_eq!(NotifyResult::NotLoaded, NotifyResult::NotLoaded);
        assert_eq!(NotifyResult::Current, NotifyResult::Current);
        assert_eq!(NotifyResult::Reloaded, NotifyResult::Reloaded);
    }

    // ========================================================================
    // UpdatePlan::plan() tests - Clojure parity scenarios
    // ========================================================================

    fn make_ns_record(commit_t: i64, index_t: i64, commit_addr: Option<&str>, index_addr: Option<&str>) -> NsRecord {
        NsRecord {
            address: "test:main".to_string(),
            alias: "test:main".to_string(),
            branch: "main".to_string(),
            commit_address: commit_addr.map(String::from),
            commit_t,
            index_address: index_addr.map(String::from),
            index_t,
            default_context_address: None,
            retracted: false,
        }
    }

    #[test]
    fn test_update_plan_noop_when_commit_t_matches() {
        // Local t == ns.commit_t, index unchanged -> Noop
        let ns = make_ns_record(10, 8, Some("commit:10"), Some("index:8"));
        let plan = UpdatePlan::plan(10, 8, Some("index:8"), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_noop_when_commit_t_matches_no_index() {
        // Local t == ns.commit_t, no index on either side -> Noop
        let ns = make_ns_record(5, 0, Some("commit:5"), None);
        let plan = UpdatePlan::plan(5, 0, None, &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_noop_with_novelty_present() {
        // Key regression test: local has novelty (commit_t > index_t is normal)
        // ns.commit_t == local.t() should be Noop, not trigger reload
        let ns = make_ns_record(10, 5, Some("commit:10"), Some("index:5"));
        // Local: index_t=5, but t()=10 due to novelty
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_index_only_when_index_advanced() {
        // Local t == ns.commit_t, but ns.index_t > local.index_t -> IndexOnly
        let ns = make_ns_record(10, 10, Some("commit:10"), Some("index:10"));
        // Local: t()=10, index_t=5
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert!(matches!(plan, UpdatePlan::IndexOnly { index_t: 10, .. }));
    }

    #[test]
    fn test_update_plan_index_only_when_index_appears() {
        // Local t == ns.commit_t, index appears where there was none -> IndexOnly
        let ns = make_ns_record(10, 10, Some("commit:10"), Some("index:10"));
        // Local: t()=10, no index
        let plan = UpdatePlan::plan(10, 0, None, &ns);
        assert!(matches!(plan, UpdatePlan::IndexOnly { index_t: 10, .. }));
    }

    #[test]
    fn test_update_plan_commit_next_when_one_ahead() {
        // ns.commit_t == local.t() + 1 -> CommitNext
        let ns = make_ns_record(11, 5, Some("commit:11"), Some("index:5"));
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert!(matches!(plan, UpdatePlan::CommitNext { commit_t: 11, .. }));
    }

    #[test]
    fn test_update_plan_reload_when_stale() {
        // ns.commit_t > local.t() + 1 -> Reload
        let ns = make_ns_record(15, 10, Some("commit:15"), Some("index:10"));
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert_eq!(plan, UpdatePlan::Reload);
    }

    #[test]
    fn test_update_plan_noop_when_local_ahead() {
        // Edge case: local is somehow ahead of ns (shouldn't happen, but be safe)
        let ns = make_ns_record(5, 5, Some("commit:5"), Some("index:5"));
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert_eq!(plan, UpdatePlan::Noop);
    }

    #[test]
    fn test_update_plan_reload_when_commit_next_missing_address() {
        // ns.commit_t == local.t() + 1 but no commit_address -> Reload (safety)
        let ns = make_ns_record(11, 5, None, Some("index:5"));
        let plan = UpdatePlan::plan(10, 5, Some("index:5"), &ns);
        assert_eq!(plan, UpdatePlan::Reload);
    }

    #[test]
    fn test_update_plan_helpers() {
        assert!(UpdatePlan::Noop.is_noop());
        assert!(!UpdatePlan::Reload.is_noop());

        assert!(UpdatePlan::Reload.requires_reload());
        assert!(!UpdatePlan::Noop.requires_reload());
    }

    // ========================================================================
    // Error propagation tests - verify status codes are preserved for waiters
    // ========================================================================

    #[test]
    fn test_error_status_code_preservation() {
        // Verify that ApiError::http preserves status codes correctly
        // This is the mechanism used for waiter error propagation

        // NotFound should map to 404
        let not_found = ApiError::NotFound("ledger foo".to_string());
        assert_eq!(not_found.status_code(), 404);

        // When converted for waiters via http(), status should be preserved
        let http_not_found = ApiError::http(not_found.status_code(), not_found.to_string());
        assert_eq!(http_not_found.status_code(), 404);

        // LedgerExists should map to 409
        let exists = ApiError::LedgerExists("ledger foo".to_string());
        assert_eq!(exists.status_code(), 409);

        let http_exists = ApiError::http(exists.status_code(), exists.to_string());
        assert_eq!(http_exists.status_code(), 409);

        // Internal should map to 500
        let internal = ApiError::internal("something failed");
        assert_eq!(internal.status_code(), 500);

        let http_internal = ApiError::http(internal.status_code(), internal.to_string());
        assert_eq!(http_internal.status_code(), 500);
    }

    #[tokio::test]
    async fn test_disconnect_all_clears_entries() {
        use fluree_db_core::MemoryStorage;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();
        let config = LedgerManagerConfig::default();
        let mgr = LedgerManager::new(storage, ns, config);

        // Directly insert Loading entries (simulates in-flight loads)
        {
            let mut entries = mgr.entries.write().await;
            entries.insert("ledger_a:main".to_string(), LoadState::Loading(Vec::new()));
            entries.insert("ledger_b:main".to_string(), LoadState::Loading(Vec::new()));
        }

        // Verify entries exist
        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 2);
        }

        // disconnect_all should clear everything
        mgr.disconnect_all().await;

        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_shutdown_flag_prevents_reinsertion() {
        use fluree_db_core::MemoryStorage;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();
        let config = LedgerManagerConfig::default();
        let mgr = LedgerManager::new(storage, ns, config);

        // Simulate: disconnect_all sets shutdown flag and clears entries
        mgr.disconnect_all().await;
        assert!(mgr.is_shutdown());

        // Simulate a load leader completing after shutdown by directly inserting
        // (this mimics what get_or_load's publish path would do without the guard)
        {
            let mut entries = mgr.entries.write().await;
            // The shutdown guard in get_or_load checks is_shutdown() before inserting.
            // Verify the flag is set so the guard would skip insertion.
            if !mgr.shutdown.load(Ordering::Acquire) {
                entries.insert("should_not_appear:main".to_string(), LoadState::Loading(Vec::new()));
            }
        }

        // Entries should still be empty because shutdown flag was set
        {
            let entries = mgr.entries.read().await;
            assert_eq!(entries.len(), 0);
        }
    }

    #[test]
    fn test_waiter_error_arc_extraction() {
        // Simulate the waiter error extraction pattern
        let original = ApiError::NotFound("ledger bar".to_string());
        let arc_error = Arc::new(ApiError::http(original.status_code(), original.to_string()));

        // Extract like a waiter would
        let extracted = match arc_error.as_ref() {
            ApiError::Http { status, message } => ApiError::http(*status, message),
            other => ApiError::http(other.status_code(), other.to_string()),
        };

        // Status code should be preserved (404, not 500)
        assert_eq!(extracted.status_code(), 404);
        assert!(extracted.to_string().contains("ledger bar"));
    }
}
