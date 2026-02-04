//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Binary columnar index building (`run_index` module)
//! - Background indexing orchestration
//! - Namespace delta replay
//! - Garbage collection support
//! - Checkpoint/resume infrastructure for long-running rebuilds
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! The binary index pipeline (`run_index::index_build`) is the sole indexing path.
//! Legacy b-tree index code has been removed.

pub mod config;
pub mod dict_tree;
pub mod error;
pub mod gc;
pub mod hll;
pub mod orchestrator;
pub mod run_index;
pub mod stats;

// Re-export main types
pub use config::IndexerConfig;
pub use error::{IndexerError, Result};
pub use gc::{
    clean_garbage, load_garbage_record, write_garbage_record, CleanGarbageConfig,
    CleanGarbageResult, GarbageRecord, GarbageRef, DEFAULT_MAX_OLD_INDEXES,
    DEFAULT_MIN_TIME_GARBAGE_MINS,
};
#[cfg(feature = "embedded-orchestrator")]
pub use orchestrator::{
    maybe_refresh_after_commit, require_refresh_before_commit, PostCommitIndexResult,
};
pub use orchestrator::{
    BackgroundIndexerWorker, IndexCompletion, IndexOutcome, IndexPhase, IndexStatusSnapshot,
    IndexerHandle, IndexerOrchestrator,
};
pub use stats::{IndexStatsHook, NoOpStatsHook, StatsArtifacts, StatsSummary};

// Note: The following types/functions are defined in this module and are automatically public:
// - build_index_for_ledger (heavy bounds: Storage + Send)
// - BatchedRebuildConfig, BatchedRebuildResult, batched_rebuild_from_commits, batched_rebuild_resume
// - ReindexCheckpoint, IndexerConfigSnapshot, checkpoint_address, load_checkpoint, write_checkpoint, delete_checkpoint
// - ReindexProgress, ProgressCallback (for observability)
// - CURRENT_INDEX_VERSION, DEFAULT_BATCH_BYTES, DEFAULT_MAX_BATCH_COMMITS, DEFAULT_CHECKPOINT_INTERVAL

use fluree_db_core::{Storage, StorageWrite};
use fluree_db_nameservice::{NameService, Publisher};
use serde::{Deserialize, Serialize};

/// Normalize an alias for comparison purposes
///
/// Handles both canonical `name:branch` format and storage-path style `name/branch`.
/// This is necessary because Db.alias may be stored in either format depending
/// on the code path that created it.
///
/// # Algorithm
///
/// 1. If the alias contains `:`, use canonical parsing via `core_alias::normalize_alias`
///    - If parsing fails, return the original string unchanged (don't manufacture aliases)
/// 2. If the alias has exactly one `/` (storage-path style), convert to `name:branch`
/// 3. Falls back to treating the whole string as the name with default branch
///
/// This ONLY treats a single `/` as a branch separator when there's no `:`.
/// For ledger names that legitimately contain `/` (e.g., "org/project:main"),
/// the canonical format with `:` must be used.
#[cfg(test)]
fn normalize_alias_for_comparison(alias: &str) -> String {
    use fluree_db_core::alias as core_alias;

    // If it has a colon, use canonical parsing
    if alias.contains(':') {
        // If canonical parse succeeds, use it. If it fails (malformed alias),
        // return the original string unchanged rather than manufacturing a new alias.
        return core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
    }

    // Check for storage-path style "name/branch" (exactly one slash, no colon)
    let slash_count = alias.chars().filter(|c| *c == '/').count();
    if slash_count == 1 {
        if let Some(slash_idx) = alias.rfind('/') {
            let name = &alias[..slash_idx];
            let branch = &alias[slash_idx + 1..];
            if !name.is_empty() && !branch.is_empty() {
                return core_alias::format_alias(name, branch);
            }
        }
    }

    // Last resort: treat entire string as name with default branch
    core_alias::format_alias(alias, core_alias::DEFAULT_BRANCH)
}

/// Result of building an index
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Storage address of the index root
    pub root_address: String,
    /// Transaction time the index is current through
    pub index_t: i64,
    /// Ledger alias
    pub alias: String,
    /// Index build statistics
    pub stats: IndexStats,
}

/// Statistics from index building
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (after dedup)
    pub flake_count: usize,
    /// Number of leaf nodes created
    pub leaf_count: usize,
    /// Number of branch nodes created
    pub branch_count: usize,
    /// Total bytes written
    pub total_bytes: usize,
}

/// Default batch size in bytes (100MB)
pub const DEFAULT_BATCH_BYTES: usize = 100_000_000;

/// Default maximum commits per batch
pub const DEFAULT_MAX_BATCH_COMMITS: usize = 100_000;

/// Current index version for compatibility checking
pub const CURRENT_INDEX_VERSION: i32 = 2;

/// Default checkpoint interval (every batch)
pub const DEFAULT_CHECKPOINT_INTERVAL: u32 = 1;

// =============================================================================
// Progress Tracking for Observability
// =============================================================================

/// Progress information for long-running reindex operations
///
/// Reported after each batch flush to provide visibility into progress.
/// Use with `BatchedRebuildConfig.progress_callback` to receive updates.
#[derive(Debug, Clone)]
pub struct ReindexProgress {
    /// Ledger alias being reindexed
    pub alias: String,
    /// Total commits to process (known after initial scan)
    pub total_commits: usize,
    /// Commits processed so far
    pub commits_processed: usize,
    /// Batches flushed so far
    pub batches_flushed: usize,
    /// Total bytes of novelty flushed so far
    pub bytes_flushed: u64,
    /// Current t being processed
    pub current_t: i64,
    /// Target t (final commit)
    pub target_t: i64,
    /// Commits processed per second (rolling average based on elapsed time)
    pub commits_per_sec: f64,
    /// Estimated seconds remaining (based on throughput)
    /// None if not enough data to estimate
    pub estimated_remaining_secs: Option<f64>,
}

impl ReindexProgress {
    /// Calculate percentage complete (0.0 to 100.0)
    pub fn percent_complete(&self) -> f64 {
        if self.total_commits == 0 {
            return 100.0;
        }
        (self.commits_processed as f64 / self.total_commits as f64) * 100.0
    }
}

/// Callback type for progress updates during batched rebuild
///
/// Invoked synchronously after each batch flush. If you need async processing,
/// spawn your own task inside the callback. Avoid blocking operations as they
/// will slow down the reindex.
pub type ProgressCallback = std::sync::Arc<dyn Fn(ReindexProgress) + Send + Sync>;

// =============================================================================
// Checkpointing for Resumable Reindex
// =============================================================================

/// Persisted checkpoint for resumable reindex operations
///
/// Checkpoints are stored in the same storage backend as commits/indexes at:
/// `fluree:file://{alias_with_slashes}/reindex-checkpoint.json`
///
/// Resume semantics: `last_processed_t` is the authoritative boundary. Commits
/// with `t > last_processed_t` will be processed on resume. The `last_processed_commit`
/// field is informational (for debugging/logging) - filtering uses `last_processed_t`.
///
/// On resume, the intermediate index is loaded and its actual `t` is validated
/// against `last_processed_t` to ensure consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexCheckpoint {
    /// Ledger alias being reindexed
    pub alias: String,
    /// Address of the head commit we're rebuilding to
    pub target_head: String,
    /// Target t we're rebuilding to
    pub target_t: i64,
    /// Current progress: address of last processed commit (informational, for logging)
    pub last_processed_commit: String,
    /// Current progress: t of last processed commit (authoritative resume boundary)
    pub last_processed_t: i64,
    /// Address of intermediate index (after last flush)
    pub intermediate_index: Option<String>,
    /// Timestamp when checkpoint was written (ms since epoch)
    pub checkpoint_time_ms: i64,
    /// Batch size configuration (preserved for resume)
    pub batch_bytes: usize,
    /// Max commits per batch (preserved for resume)
    pub max_batch_commits: usize,
    /// Checkpoint interval - how often to write checkpoints (preserved for resume)
    pub checkpoint_interval: u32,
    /// Number of batches flushed so far
    pub batches_flushed: usize,
    /// IndexerConfig serialized for resume (leaf/branch thresholds)
    #[serde(default)]
    pub indexer_config: Option<IndexerConfigSnapshot>,
}

/// Snapshot of IndexerConfig for checkpoint persistence
///
/// This captures the essential configuration needed to resume with the
/// same indexer settings. Uses Option fields to be forward-compatible.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexerConfigSnapshot {
    /// Target leaf node size in bytes
    pub leaf_target_bytes: Option<u64>,
    /// Maximum leaf node size in bytes
    pub leaf_max_bytes: Option<u64>,
    /// Target children per branch node
    pub branch_target_children: Option<usize>,
    /// Maximum children per branch node
    pub branch_max_children: Option<usize>,
    /// GC: Maximum old index versions to retain
    #[serde(default)]
    pub gc_max_old_indexes: Option<u32>,
    /// GC: Minimum age in minutes before GC eligible
    #[serde(default)]
    pub gc_min_time_mins: Option<u32>,
}

impl From<&IndexerConfig> for IndexerConfigSnapshot {
    fn from(config: &IndexerConfig) -> Self {
        Self {
            leaf_target_bytes: Some(config.leaf_target_bytes),
            leaf_max_bytes: Some(config.leaf_max_bytes),
            branch_target_children: Some(config.branch_target_children),
            branch_max_children: Some(config.branch_max_children),
            gc_max_old_indexes: Some(config.gc_max_old_indexes),
            gc_min_time_mins: Some(config.gc_min_time_mins),
        }
    }
}

impl IndexerConfigSnapshot {
    /// Convert back to IndexerConfig, using defaults for missing fields
    pub fn to_indexer_config(&self) -> IndexerConfig {
        let default = IndexerConfig::default();
        IndexerConfig {
            leaf_target_bytes: self.leaf_target_bytes.unwrap_or(default.leaf_target_bytes),
            leaf_max_bytes: self.leaf_max_bytes.unwrap_or(default.leaf_max_bytes),
            branch_target_children: self
                .branch_target_children
                .unwrap_or(default.branch_target_children),
            branch_max_children: self
                .branch_max_children
                .unwrap_or(default.branch_max_children),
            gc_max_old_indexes: self
                .gc_max_old_indexes
                .unwrap_or(default.gc_max_old_indexes),
            gc_min_time_mins: self.gc_min_time_mins.unwrap_or(default.gc_min_time_mins),
            data_dir: None,
        }
    }
}

/// Get the storage address for a reindex checkpoint
///
/// Format: `fluree:file://{ledger}/{branch}/index/reindex-checkpoint.json`
/// (canonical layout; works for S3 because it ignores the scheme and extracts the path).
pub fn checkpoint_address(alias: &str) -> String {
    let prefix = fluree_db_core::address_path::alias_to_path_prefix(alias)
        .unwrap_or_else(|_| alias.replace(':', "/"));
    format!("fluree:file://{}/index/reindex-checkpoint.json", prefix)
}

/// Load a reindex checkpoint from storage
///
/// Returns `None` if no checkpoint exists. Returns an error if the checkpoint
/// exists but is corrupt or unreadable.
///
/// # Not-Found Detection
///
/// Storage backends don't have a standard "not found" error type, so detection
/// relies on string matching against common patterns ("not found", "does not exist",
/// "no such file"). If a storage backend uses unusual error messages, this may
/// incorrectly treat not-found as a real error. In practice, all major storage
/// implementations use these patterns.
pub async fn load_checkpoint<S: Storage>(
    storage: &S,
    alias: &str,
) -> Result<Option<ReindexCheckpoint>> {
    let address = checkpoint_address(alias);
    match storage.read_bytes(&address).await {
        Ok(bytes) => {
            let checkpoint: ReindexCheckpoint = serde_json::from_slice(&bytes).map_err(|e| {
                IndexerError::Checkpoint(format!(
                    "Failed to parse checkpoint at {}: {}",
                    address, e
                ))
            })?;
            Ok(Some(checkpoint))
        }
        Err(e) => {
            // Best-effort detection of "not found" errors via string matching.
            // See doc comment above for caveats.
            let msg = e.to_string().to_lowercase();
            if msg.contains("not found")
                || msg.contains("does not exist")
                || msg.contains("no such file")
            {
                Ok(None)
            } else {
                Err(IndexerError::Checkpoint(format!(
                    "Failed to read checkpoint at {}: {}",
                    address, e
                )))
            }
        }
    }
}

/// Write a reindex checkpoint to storage
pub async fn write_checkpoint<S: fluree_db_core::StorageWrite>(
    storage: &S,
    checkpoint: &ReindexCheckpoint,
) -> Result<()> {
    let address = checkpoint_address(&checkpoint.alias);
    let bytes = serde_json::to_vec_pretty(checkpoint)
        .map_err(|e| IndexerError::Checkpoint(format!("Failed to serialize checkpoint: {}", e)))?;
    storage.write_bytes(&address, &bytes).await.map_err(|e| {
        IndexerError::Checkpoint(format!("Failed to write checkpoint to {}: {}", address, e))
    })?;
    tracing::debug!(
        address = %address,
        last_processed_t = checkpoint.last_processed_t,
        "Checkpoint written"
    );
    Ok(())
}

/// Delete a reindex checkpoint from storage
pub async fn delete_checkpoint<S: StorageWrite>(storage: &S, alias: &str) -> Result<()> {
    let address = checkpoint_address(alias);
    // Ignore errors - checkpoint may not exist
    let _ = storage.delete(&address).await;
    tracing::debug!(address = %address, "Checkpoint deleted");
    Ok(())
}

/// Configuration for batched rebuild
#[derive(Clone)]
pub struct BatchedRebuildConfig {
    /// Indexer configuration (leaf/branch sizes)
    pub indexer_config: IndexerConfig,

    /// Memory budget for batched rebuild (default: 100MB)
    /// When novelty exceeds this, intermediate index is flushed
    pub batch_bytes: usize,

    /// Maximum commits per batch (default: 100,000)
    /// Secondary ceiling for ledgers with many tiny commits
    pub max_batch_commits: usize,

    /// Enable checkpointing for resumable reindex (default: false)
    pub checkpoint: bool,

    /// Checkpoint interval - flush checkpoint every N batches (default: 1)
    pub checkpoint_interval: u32,

    /// Optional callback invoked after each batch flush with progress info
    ///
    /// Invoked synchronously - avoid blocking operations.
    /// Set to None to disable progress reporting.
    pub progress_callback: Option<ProgressCallback>,
}

impl std::fmt::Debug for BatchedRebuildConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchedRebuildConfig")
            .field("indexer_config", &self.indexer_config)
            .field("batch_bytes", &self.batch_bytes)
            .field("max_batch_commits", &self.max_batch_commits)
            .field("checkpoint", &self.checkpoint)
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field(
                "progress_callback",
                &self.progress_callback.as_ref().map(|_| "<callback>"),
            )
            .finish()
    }
}

impl Default for BatchedRebuildConfig {
    fn default() -> Self {
        Self {
            indexer_config: IndexerConfig::default(),
            batch_bytes: DEFAULT_BATCH_BYTES,
            max_batch_commits: DEFAULT_MAX_BATCH_COMMITS,
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        }
    }
}

impl BatchedRebuildConfig {
    /// Create with custom indexer config
    pub fn with_indexer_config(mut self, config: IndexerConfig) -> Self {
        self.indexer_config = config;
        self
    }

    /// Set the batch size in bytes
    pub fn with_batch_bytes(mut self, bytes: usize) -> Self {
        self.batch_bytes = bytes;
        self
    }

    /// Set the maximum commits per batch
    pub fn with_max_batch_commits(mut self, max: usize) -> Self {
        self.max_batch_commits = max;
        self
    }

    /// Enable checkpointing for resumable reindex
    pub fn with_checkpoint(mut self, enabled: bool) -> Self {
        self.checkpoint = enabled;
        self
    }

    /// Set checkpoint interval (flush checkpoint every N batches)
    pub fn with_checkpoint_interval(mut self, interval: u32) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Set progress callback for receiving updates after each batch
    ///
    /// The callback is invoked synchronously after each batch flush.
    /// It receives a `ReindexProgress` struct with current progress info.
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }
}

/// Result of batched rebuild
#[derive(Debug, Clone)]
pub struct BatchedRebuildResult {
    /// Final index result
    pub index_result: IndexResult,
    /// Number of batches flushed during rebuild
    pub batches_flushed: usize,
}

/// Memory-bounded batched rebuild from commit history
///
/// **Note**: The b-tree index pipeline has been removed. This function currently
/// returns an error. It will be rewired to the binary index pipeline.
pub async fn batched_rebuild_from_commits<S>(
    _storage: &S,
    _head_commit_address: &str,
    _alias: &str,
    _config: BatchedRebuildConfig,
) -> Result<BatchedRebuildResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // B-tree index pipeline removed. Will be rewired to binary index pipeline.
    Err(IndexerError::BTreePipelineRemoved)
}

/// Resume a batched rebuild from a checkpoint
///
/// Loads a previously saved checkpoint and continues the rebuild from where it left off.
///
/// # Checkpoint Validation
///
/// - Verifies `checkpoint.target_head` matches `current_head_address` (no new commits)
/// - Validates loaded intermediate index's `t` matches `checkpoint.last_processed_t`
/// - If mismatch detected, returns appropriate error
///
/// # Resume Semantics
///
/// The checkpoint's `last_processed_t` is the authoritative resume boundary. Commits
/// with `t > last_processed_t` will be processed. The intermediate index is loaded
/// and its actual `t` is validated against the checkpoint for consistency.
///
/// # Configuration Preservation
///
/// Batch configuration (batch_bytes, max_batch_commits, checkpoint_interval) is
/// restored from the checkpoint. IndexerConfig is restored from checkpoint if present
/// (checkpoint wins); otherwise falls back to the provided `indexer_config_override`.
/// This ensures resume uses the same settings as the original reindex.
///
/// # Arguments
///
/// * `storage` - Storage backend
/// * `current_head_address` - Current head commit address (for validation)
/// * `checkpoint` - The checkpoint to resume from
/// * `indexer_config_override` - Fallback indexer config if checkpoint doesn't have one
///
/// # Errors
///
/// - `CheckpointHeadMismatch` if ledger has new commits since checkpoint
/// - `Checkpoint` if intermediate index cannot be loaded or t mismatch
pub async fn batched_rebuild_resume<S>(
    _storage: &S,
    _current_head_address: &str,
    _checkpoint: ReindexCheckpoint,
    _indexer_config_override: IndexerConfig,
) -> Result<BatchedRebuildResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // B-tree index pipeline removed. Will be rewired to binary index pipeline.
    Err(IndexerError::BTreePipelineRemoved)
}

/// External indexer entry point
///
/// Builds a binary columnar index from the commit chain. The pipeline:
/// 1. Walks the commit chain and generates sorted run files
/// 2. Builds per-graph leaf/branch indexes for all sort orders
/// 3. Creates a `BinaryIndexRootV2` descriptor and writes it to storage
///
/// Returns early if the index is already current (no work needed).
///
/// Requires the `commit-v2` feature for the binary pipeline. Without it,
/// returns `BTreePipelineRemoved`.
pub async fn build_index_for_ledger<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    let span = tracing::info_span!("index_build", ledger_alias = alias);
    let _guard = span.enter();

    // Look up the ledger record
    let record = nameservice
        .lookup(alias)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(alias.to_string()))?;

    // If index is already current, return it
    if let Some(ref index_addr) = record.index_address {
        if record.index_t >= record.commit_t {
            return Ok(IndexResult {
                root_address: index_addr.clone(),
                index_t: record.index_t,
                alias: alias.to_string(),
                stats: IndexStats::default(),
            });
        }
    }

    build_binary_index(storage, alias, &record, config).await
}

/// Binary index build implementation (commit-v2 feature required).
///
/// Runs the entire pipeline on a blocking thread via `spawn_blocking` +
/// `handle.block_on()` because internal dictionaries contain non-Send types
/// held across await points.
///
/// Pipeline:
/// 1. Walk commit chain backward to collect addresses
/// 2. Resolve commits in forward order using `MultiOrderRunWriter` to produce
///    per-order sorted run files (SPOT, PSOT, POST, OPST)
/// 3. Build per-graph leaf/branch indexes from run files
/// 4. Write `BinaryIndexRootV2` descriptor to storage
async fn build_binary_index<S>(
    storage: &S,
    alias: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_novelty::commit_v2::read_commit_envelope;

    let head_commit_addr = record
        .commit_address
        .clone()
        .ok_or(IndexerError::NoCommits)?;

    // Determine output directory for binary index artifacts
    let data_dir = config
        .data_dir
        .unwrap_or_else(|| std::env::temp_dir().join("fluree-index"));
    let alias_path = fluree_db_core::address_path::alias_to_path_prefix(alias)
        .unwrap_or_else(|_| alias.replace(':', "/"));
    let session_id = uuid::Uuid::new_v4().to_string();
    let run_dir = data_dir
        .join(&alias_path)
        .join("tmp_import")
        .join(&session_id);
    let index_dir = data_dir.join(&alias_path).join("index");

    tracing::info!(
        %head_commit_addr,
        ?run_dir,
        ?index_dir,
        "starting binary index build"
    );

    // Capture values for the blocking task
    let storage = storage.clone();
    let alias = alias.to_string();
    let prev_root_address = record.index_address.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        handle.block_on(async {
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // ---- Phase A: Walk commit chain backward to collect addresses ----
            let addresses = {
                let mut addrs = Vec::new();
                let mut current = Some(head_commit_addr.clone());

                while let Some(addr) = current {
                    let bytes = storage
                        .read_bytes(&addr)
                        .await
                        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", addr, e)))?;
                    let envelope = read_commit_envelope(&bytes)
                        .map_err(|e| IndexerError::StorageRead(e.to_string()))?;
                    current = envelope.previous_address().map(String::from);
                    addrs.push(addr);
                }

                addrs.reverse(); // chronological order (genesis first)
                tracing::info!(commit_count = addrs.len(), "commit chain traversed");
                addrs
            };

            // ---- Phase B: Resolve commits with multi-order run writer ----
            let mut dicts = run_index::GlobalDicts::new(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            let mut resolver = run_index::CommitResolver::new();
            resolver.set_stats_hook(crate::stats::IdStatsHook::new());

            let multi_config = run_index::MultiOrderConfig {
                total_budget_bytes: 256 * 1024 * 1024,
                orders: run_index::RunSortOrder::all_build_orders().to_vec(),
                base_run_dir: run_dir.clone(),
            };
            let mut writer = run_index::MultiOrderRunWriter::new(multi_config)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            for (i, addr) in addresses.iter().enumerate() {
                let bytes = storage
                    .read_bytes(addr)
                    .await
                    .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", addr, e)))?;

                let (op_count, t) = resolver
                    .resolve_blob(&bytes, addr, &alias, &mut dicts, &mut writer)
                    .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

                tracing::debug!(
                    commit = i + 1,
                    t = t,
                    ops = op_count,
                    subjects = dicts.subjects.len(),
                    predicates = dicts.predicates.len(),
                    "commit resolved"
                );
            }

            let id_stats_hook = resolver.take_stats_hook();

            let total_records = writer.total_records();
            let _writer_results = writer
                .finish(&mut dicts.languages)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist dictionaries for index build
            dicts
                .persist(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist namespace map for query-time IRI encoding
            run_index::persist_namespaces(resolver.ns_prefixes(), &run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist reverse hash indexes
            dicts
                .subjects
                .write_reverse_index(&run_dir.join("subjects.rev"))
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            dicts
                .strings
                .write_reverse_index(&run_dir.join("strings.rev"))
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            tracing::info!(
                total_records,
                commits = addresses.len(),
                subjects = dicts.subjects.len(),
                predicates = dicts.predicates.len(),
                "run generation complete, building indexes"
            );

            // ---- Phase C: Build per-graph indexes for all sort orders ----
            let build_results = run_index::build_all_indexes(
                &run_dir,
                &index_dir,
                run_index::RunSortOrder::all_build_orders(),
                25_000, // leaflet_rows
                10,     // leaflets_per_leaf
                1,      // zstd_level
            )
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // ---- Phase D: Upload artifacts to CAS and write v2 root ----

            // D.1: Load store for max_t / base_t / namespace_codes
            let store = run_index::BinaryIndexStore::load(&run_dir, &index_dir)
                .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

            // Build predicate p_id -> (ns_code, suffix) mapping for the root (compact).
            let predicate_sids: Vec<(u16, String)> = (0..dicts.predicates.len())
                .map(|p_id| {
                    let iri = dicts.predicates.resolve(p_id).unwrap_or("");
                    let sid = store.encode_iri(iri);
                    (sid.namespace_code, sid.name.as_ref().to_string())
                })
                .collect();

            // D.2: Upload dictionary artifacts to CAS
            let numbig_p_ids: Vec<u32> = dicts.numbigs.keys().copied().collect();
            let dict_addresses =
                upload_dicts_to_cas(&storage, &alias, &run_dir, &dicts, &numbig_p_ids).await?;

            // D.3: Upload index artifacts (branches + leaves) to CAS
            let graph_addresses = upload_indexes_to_cas(&storage, &alias, &build_results).await?;

            // D.4: Build stats JSON for the planner (RawDbRootStats shape).
            //
            // Preferred: ID-based stats collected during commit resolution (per-graph property
            // stats with datatype counts + HLL NDV). Fallback: SPOT build result for per-graph
            // flake counts only.
            let stats_json = {
                if let Some(hook) = id_stats_hook {
                    let id_result = hook.finalize();

                    let graphs_json: Vec<serde_json::Value> = id_result
                        .graphs
                        .iter()
                        .map(|g| {
                            let props_json: Vec<serde_json::Value> = g
                                .properties
                                .iter()
                                .map(|p| {
                                    serde_json::json!({
                                        "p_id": p.p_id,
                                        "count": p.count,
                                        "ndv_values": p.ndv_values,
                                        "ndv_subjects": p.ndv_subjects,
                                        "last_modified_t": p.last_modified_t,
                                        "datatypes": p.datatypes,
                                    })
                                })
                                .collect();

                            serde_json::json!({
                                "g_id": g.g_id,
                                "flakes": g.flakes,
                                "size": g.size,
                                "properties": props_json,
                            })
                        })
                        .collect();

                    serde_json::json!({
                        "flakes": id_result.total_flakes,
                        "size": 0,
                        "graphs": graphs_json,
                    })
                } else {
                    // Fallback: flake counts only (no per-property / datatype breakdown).
                    let (_, spot_result) = build_results
                        .iter()
                        .find(|(order, _)| *order == run_index::RunSortOrder::Spot)
                        .expect("SPOT index must always be present in build results");

                    let graph_stats: Vec<serde_json::Value> = spot_result
                        .graphs
                        .iter()
                        .map(|g| {
                            serde_json::json!({
                                "g_id": g.g_id,
                                "flakes": g.total_rows,
                                "size": 0
                            })
                        })
                        .collect();

                    let total_flakes: u64 = spot_result.graphs.iter().map(|g| g.total_rows).sum();

                    serde_json::json!({
                        "flakes": total_flakes,
                        "size": 0,
                        "graphs": graph_stats
                    })
                }
            };

            // D.5: Build v2 root with CAS addresses and stats (initially without GC fields)
            let sid_encoding = if dicts.subjects.needs_wide() {
                fluree_db_core::SubjectIdEncoding::Wide
            } else {
                fluree_db_core::SubjectIdEncoding::Narrow
            };
            let subject_watermarks = dicts.subjects.subject_watermarks();
            let string_watermark = dicts.strings.len().saturating_sub(1);

            let mut root = run_index::BinaryIndexRootV2::from_cas_artifacts(
                &alias,
                store.max_t(),
                store.base_t(),
                predicate_sids,
                store.namespace_codes(),
                sid_encoding,
                dict_addresses,
                graph_addresses,
                Some(stats_json),
                None, // schema: requires predicate definitions (future)
                None, // prev_index: set below after garbage computation
                None, // garbage: set below after garbage computation
                subject_watermarks,
                string_watermark,
            );

            // D.5.1: Compute garbage and link prev_index for GC chain.
            //
            // Strategy: use all_cas_addresses() on both old and new roots to
            // compute the set difference. This guarantees both sides use the
            // same enumeration method, eliminating divergence risk.
            if let Some(prev_addr) = prev_root_address.as_deref() {
                // Try to load the previous root as a v2 binary root.
                // If it's a v1 or legacy DbRoot, skip GC linking — mixed-format
                // chains will start a fresh GC chain from this root.
                let prev_bytes = storage.read_bytes(prev_addr).await.ok();
                let prev_root = prev_bytes
                    .as_deref()
                    .and_then(|b| run_index::BinaryIndexRootV2::from_json_bytes(b).ok());

                if let Some(prev) = prev_root {
                    let old_addrs: std::collections::HashSet<String> =
                        prev.all_cas_addresses().into_iter().collect();
                    let new_addrs: std::collections::HashSet<String> =
                        root.all_cas_addresses().into_iter().collect();
                    let garbage_addrs: Vec<String> =
                        old_addrs.difference(&new_addrs).cloned().collect();

                    let garbage_count = garbage_addrs.len();

                    root.garbage =
                        gc::write_garbage_record(&storage, &alias, store.max_t(), garbage_addrs)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                            .map(|r| run_index::BinaryGarbageRef { address: r.address });

                    root.prev_index = Some(run_index::BinaryPrevIndexRef {
                        t: prev.index_t,
                        address: prev_addr.to_string(),
                    });

                    tracing::info!(
                        prev_t = prev.index_t,
                        garbage_count,
                        "GC chain linked to previous binary index root"
                    );
                }
            }

            tracing::info!(
                index_t = root.index_t,
                base_t = root.base_t,
                graphs = root.graphs.len(),
                "binary index built (v2), writing CAS root"
            );

            // D.6: Write root to CAS (auto-hash of canonical compact JSON)
            let root_bytes = root
                .to_json_bytes()
                .map_err(|e| IndexerError::Serialization(e.to_string()))?;
            let write_result = storage
                .content_write_bytes(fluree_db_core::ContentKind::IndexRoot, &alias, &root_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Clean up ephemeral tmp_import session directory
            if let Err(e) = std::fs::remove_dir_all(&run_dir) {
                tracing::warn!(?run_dir, %e, "failed to clean up tmp_import session dir");
            }

            // Compute stats from build results
            let total_leaves: usize = build_results
                .iter()
                .flat_map(|(_, r)| r.graphs.iter())
                .map(|g| g.leaf_count as usize)
                .sum();

            Ok(IndexResult {
                root_address: write_result.address,
                index_t: root.index_t,
                alias: alias.to_string(),
                stats: IndexStats {
                    flake_count: total_records as usize,
                    leaf_count: total_leaves,
                    branch_count: build_results.len(),
                    total_bytes: root_bytes.len(),
                },
            })
        })
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("index build task panicked: {}", e)))?
}

/// Build dictionary CoW trees and upload all dictionary artifacts to CAS.
///
/// Small-cardinality dictionaries (graphs, datatypes, languages) are uploaded
/// as flat blobs. Subject and string dictionaries are built into CoW trees
/// (branch + leaves) for O(log n) lookup at query time.
async fn upload_dicts_to_cas<S: Storage>(
    storage: &S,
    alias: &str,
    run_dir: &std::path::Path,
    dicts: &run_index::GlobalDicts,
    numbig_p_ids: &[u32],
) -> Result<run_index::DictAddresses> {
    use dict_tree::builder::{self, TreeBuildResult};
    use dict_tree::forward_leaf::ForwardEntry;
    use dict_tree::reverse_leaf::ReverseEntry;
    use fluree_db_core::{ContentKind, DictKind};
    use std::collections::BTreeMap;

    /// Read a file and upload to CAS, returning the address.
    async fn upload_flat<S: Storage>(
        storage: &S,
        alias: &str,
        path: &std::path::Path,
        dict: DictKind,
    ) -> Result<String> {
        let bytes = std::fs::read(path)
            .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
        let result = storage
            .content_write_bytes(ContentKind::DictBlob { dict }, alias, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        tracing::debug!(
            path = %path.display(),
            address = %result.address,
            bytes = result.size_bytes,
            "dict artifact uploaded to CAS"
        );
        Ok(result.address)
    }

    /// Upload a tree build result (branch + leaves) to CAS.
    async fn upload_tree<S: Storage>(
        storage: &S,
        alias: &str,
        result: TreeBuildResult,
        dict: DictKind,
    ) -> Result<run_index::DictTreeAddresses> {
        let mut leaf_addresses = Vec::with_capacity(result.leaves.len());
        let mut hash_to_address = std::collections::HashMap::new();

        for leaf in &result.leaves {
            let cas_result = storage
                .content_write_bytes(ContentKind::DictBlob { dict }, alias, &leaf.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            leaf_addresses.push(cas_result.address.clone());
            hash_to_address.insert(leaf.hash.clone(), cas_result.address);
        }

        // Finalize branch with actual CAS addresses, then upload
        let (_, branch_bytes, _) = builder::finalize_branch(result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let branch_result = storage
            .content_write_bytes(ContentKind::DictBlob { dict }, alias, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        Ok(run_index::DictTreeAddresses {
            branch: branch_result.address,
            leaves: leaf_addresses,
        })
    }

    // Small flat dicts
    let graphs = upload_flat(
        storage,
        alias,
        &run_dir.join("graphs.dict"),
        DictKind::Graphs,
    )
    .await?;
    let datatypes = upload_flat(
        storage,
        alias,
        &run_dir.join("datatypes.dict"),
        DictKind::Datatypes,
    )
    .await?;
    let languages = upload_flat(
        storage,
        alias,
        &run_dir.join("languages.dict"),
        DictKind::Languages,
    )
    .await?;

    // Subject trees
    let subject_pairs = dicts
        .subjects
        .read_all_entries()
        .map_err(|e| IndexerError::StorageRead(format!("read subject entries: {}", e)))?;

    let mut subj_fwd: Vec<ForwardEntry> = subject_pairs
        .iter()
        .map(|(sid, iri)| ForwardEntry {
            id: *sid,
            value: iri.clone(),
        })
        .collect();
    subj_fwd.sort_unstable_by_key(|e| e.id);

    let mut subj_rev: Vec<ReverseEntry> = subject_pairs
        .into_iter()
        .map(|(sid, iri)| ReverseEntry { key: iri, id: sid })
        .collect();
    subj_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let sf_tree = builder::build_forward_tree(subj_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject fwd tree: {}", e)))?;
    let sr_tree = builder::build_reverse_tree(subj_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject rev tree: {}", e)))?;

    let subject_forward = upload_tree(storage, alias, sf_tree, DictKind::SubjectForward).await?;
    let subject_reverse = upload_tree(storage, alias, sr_tree, DictKind::SubjectReverse).await?;

    // String trees (read from file-backed forward file)
    let string_pairs = dicts.strings.all_entries()
        .map_err(|e| IndexerError::StorageWrite(format!("read string entries: {}", e)))?;

    let mut str_fwd: Vec<ForwardEntry> = string_pairs
        .iter()
        .map(|(id, val)| ForwardEntry {
            id: *id,
            value: val.clone(),
        })
        .collect();
    str_fwd.sort_unstable_by_key(|e| e.id);

    let mut str_rev: Vec<ReverseEntry> = string_pairs
        .into_iter()
        .map(|(id, val)| ReverseEntry { key: val, id })
        .collect();
    str_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let stf_tree = builder::build_forward_tree(str_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string fwd tree: {}", e)))?;
    let str_tree = builder::build_reverse_tree(str_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string rev tree: {}", e)))?;

    let string_forward = upload_tree(storage, alias, stf_tree, DictKind::StringForward).await?;
    let string_reverse = upload_tree(storage, alias, str_tree, DictKind::StringReverse).await?;

    // Per-predicate numbig arenas
    let mut numbig = BTreeMap::new();
    let nb_dir = run_dir.join("numbig");
    for &p_id in numbig_p_ids {
        let path = nb_dir.join(format!("p_{}.nba", p_id));
        if path.exists() {
            let addr = upload_flat(storage, alias, &path, DictKind::NumBig { p_id }).await?;
            numbig.insert(p_id.to_string(), addr);
        }
    }

    tracing::info!(
        subject_count = dicts.subjects.len(),
        string_count = dicts.strings.len(),
        numbig_count = numbig.len(),
        "dictionary trees built and uploaded to CAS"
    );

    Ok(run_index::DictAddresses {
        graphs,
        datatypes,
        languages,
        subject_forward,
        subject_reverse,
        string_forward,
        string_reverse,
        numbig,
    })
}

/// Upload all index artifacts (branches + leaves) to CAS.
///
/// Reads each `.fbr` and `.fli` file from the index directory, uploads via
/// `content_write_bytes_with_hash` (reusing the existing SHA-256 hashes from
/// the build), and returns per-graph CAS addresses.
pub async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    alias: &str,
    build_results: &[(run_index::RunSortOrder, run_index::IndexBuildResult)],
) -> Result<Vec<run_index::GraphAddresses>> {
    use fluree_db_core::ContentKind;
    use std::collections::BTreeMap;

    let mut graph_map: BTreeMap<u32, BTreeMap<String, run_index::GraphOrderAddresses>> =
        BTreeMap::new();

    for (order, result) in build_results {
        let order_name = order.dir_name().to_string();

        for graph_result in &result.graphs {
            let g_id = graph_result.g_id;
            let graph_dir = &graph_result.graph_dir;

            // Upload branch manifest
            let branch_path = graph_dir.join(format!("{}.fbr", graph_result.branch_hash));
            let branch_bytes = std::fs::read(&branch_path).map_err(|e| {
                IndexerError::StorageRead(format!("read branch {}: {}", branch_path.display(), e))
            })?;
            let branch_write = storage
                .content_write_bytes_with_hash(
                    ContentKind::IndexBranch,
                    alias,
                    &graph_result.branch_hash,
                    &branch_bytes,
                )
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Parse branch manifest to discover leaf files
            let branch_manifest =
                run_index::branch::read_branch_manifest(&branch_path).map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read branch manifest {}: {}",
                        branch_path.display(),
                        e
                    ))
                })?;

            // Upload each leaf file
            let mut leaf_addresses = Vec::with_capacity(branch_manifest.leaves.len());
            for leaf_entry in &branch_manifest.leaves {
                let leaf_bytes = std::fs::read(&leaf_entry.path).map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read leaf {}: {}",
                        leaf_entry.path.display(),
                        e
                    ))
                })?;
                let leaf_write = storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexLeaf,
                        alias,
                        &leaf_entry.content_hash,
                        &leaf_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                leaf_addresses.push(leaf_write.address);
            }

            tracing::debug!(
                g_id,
                order = %order_name,
                leaves = leaf_addresses.len(),
                branch = %branch_write.address,
                "graph/order index artifacts uploaded to CAS"
            );

            graph_map.entry(g_id).or_default().insert(
                order_name.clone(),
                run_index::GraphOrderAddresses {
                    branch: branch_write.address,
                    leaves: leaf_addresses,
                },
            );
        }
    }

    let graph_addresses: Vec<run_index::GraphAddresses> = graph_map
        .into_iter()
        .map(|(g_id, orders)| run_index::GraphAddresses { g_id, orders })
        .collect();

    let total_artifacts: usize = graph_addresses
        .iter()
        .flat_map(|ga| ga.orders.values())
        .map(|oa| 1 + oa.leaves.len())
        .sum();
    tracing::info!(
        graphs = graph_addresses.len(),
        total_artifacts,
        "index artifacts uploaded to CAS"
    );

    Ok(graph_addresses)
}

/// Result of uploading persisted dict flat files to CAS.
///
/// Contains the CAS addresses for all dictionary artifacts plus derived metadata
/// needed for `BinaryIndexRootV2::from_cas_artifacts`.
#[derive(Debug)]
pub struct UploadedDicts {
    pub dict_addresses: run_index::DictAddresses,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,
}

/// Upload dictionary artifacts from persisted flat files to CAS.
///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
///
/// Required files in `run_dir`:
///   - `subjects.fwd`, `subjects.idx`, `subjects.sids`
///   - `strings.fwd`, `strings.idx`
///   - `graphs.dict`, `datatypes.dict`, `languages.dict`
///   - `numbig/p_*.nba` (zero or more)
///
/// Watermark derivation from `subjects.sids`:
///   - Decode each sid64 via `SubjectId::from_u64` → `(ns_code, local_id)`
///   - `subject_watermarks[ns_code]` = max local_id for that ns_code
///   - Overflow ns_code (0xFFFF): always wide, watermark = 0
///   - `needs_wide` = any local_id exceeds `u16::MAX`
///   - `string_watermark` = string entry count − 1 (IDs are 0..=N contiguous)
pub async fn upload_dicts_from_disk<S: Storage>(
    storage: &S,
    alias: &str,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<UploadedDicts> {
    use dict_tree::builder::{self, TreeBuildResult};
    use dict_tree::forward_leaf::ForwardEntry;
    use dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind, SubjectIdEncoding};
    use std::collections::BTreeMap;

    // ---- Inner helpers (same pattern as upload_dicts_to_cas) ----

    async fn upload_flat<S: Storage>(
        storage: &S,
        alias: &str,
        path: &std::path::Path,
        dict: DictKind,
    ) -> Result<String> {
        let bytes = std::fs::read(path)
            .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
        let result = storage
            .content_write_bytes(ContentKind::DictBlob { dict }, alias, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        tracing::debug!(
            path = %path.display(),
            address = %result.address,
            bytes = result.size_bytes,
            "dict artifact uploaded to CAS (from disk)"
        );
        Ok(result.address)
    }

    async fn upload_tree<S: Storage>(
        storage: &S,
        alias: &str,
        result: TreeBuildResult,
        dict: DictKind,
    ) -> Result<run_index::DictTreeAddresses> {
        let mut leaf_addresses = Vec::with_capacity(result.leaves.len());
        let mut hash_to_address = std::collections::HashMap::new();

        for leaf in &result.leaves {
            let cas_result = storage
                .content_write_bytes(ContentKind::DictBlob { dict }, alias, &leaf.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            leaf_addresses.push(cas_result.address.clone());
            hash_to_address.insert(leaf.hash.clone(), cas_result.address);
        }

        let (_, branch_bytes, _) = builder::finalize_branch(result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let branch_result = storage
            .content_write_bytes(ContentKind::DictBlob { dict }, alias, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        Ok(run_index::DictTreeAddresses {
            branch: branch_result.address,
            leaves: leaf_addresses,
        })
    }

    // ---- 1. Upload flat dicts ----
    let graphs = upload_flat(
        storage,
        alias,
        &run_dir.join("graphs.dict"),
        DictKind::Graphs,
    )
    .await?;
    let datatypes = upload_flat(
        storage,
        alias,
        &run_dir.join("datatypes.dict"),
        DictKind::Datatypes,
    )
    .await?;
    let languages = upload_flat(
        storage,
        alias,
        &run_dir.join("languages.dict"),
        DictKind::Languages,
    )
    .await?;

    // ---- 2. Read subjects.fwd + subjects.idx + subjects.sids → build trees ----
    let sids_path = run_dir.join("subjects.sids");
    let sids: Vec<u64> = run_index::dict_io::read_subject_sid_map(&sids_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", sids_path.display(), e)))?;

    let subj_idx_path = run_dir.join("subjects.idx");
    let (subj_offsets, subj_lens) = run_index::dict_io::read_forward_index(&subj_idx_path)
        .map_err(|e| {
            IndexerError::StorageRead(format!("read {}: {}", subj_idx_path.display(), e))
        })?;

    let subj_fwd_data = std::fs::read(run_dir.join("subjects.fwd"))
        .map_err(|e| IndexerError::StorageRead(format!("read subjects.fwd: {}", e)))?;

    // Build forward entries (suffix-only, ns-compressed) and reverse entries
    let mut subj_fwd_entries: Vec<ForwardEntry> = Vec::with_capacity(sids.len());
    let mut subj_rev_entries: Vec<ReverseEntry> = Vec::with_capacity(sids.len());
    for (&sid, (&off, &len)) in sids.iter().zip(subj_offsets.iter().zip(subj_lens.iter())) {
        let iri = &subj_fwd_data[off as usize..(off as usize + len as usize)];
        let iri_str = std::str::from_utf8(iri).unwrap_or("");
        let ns_code = SubjectId::from_u64(sid).ns_code();
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        let suffix = if iri_str.starts_with(prefix) && !prefix.is_empty() {
            &iri[prefix.len()..]
        } else {
            iri
        };
        subj_fwd_entries.push(ForwardEntry {
            id: sid,
            value: suffix.to_vec(),
        });
        subj_rev_entries.push(ReverseEntry {
            key: subject_reverse_key(ns_code, suffix),
            id: sid,
        });
    }
    subj_fwd_entries.sort_by_key(|e| e.id);
    subj_rev_entries.sort_by(|a, b| a.key.cmp(&b.key));

    let sf_tree = builder::build_forward_tree(subj_fwd_entries, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject fwd tree: {}", e)))?;
    let sr_tree = builder::build_reverse_tree(subj_rev_entries, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject rev tree: {}", e)))?;

    let subject_forward = upload_tree(storage, alias, sf_tree, DictKind::SubjectForward).await?;
    let subject_reverse = upload_tree(storage, alias, sr_tree, DictKind::SubjectReverse).await?;

    // ---- 3. Read strings.fwd + strings.idx → build trees ----
    let str_idx_path = run_dir.join("strings.idx");
    let str_fwd_path = run_dir.join("strings.fwd");
    let (string_count, string_forward, string_reverse) = if str_idx_path.exists()
        && str_fwd_path.exists()
    {
        let (str_offsets, str_lens) = run_index::dict_io::read_forward_index(&str_idx_path)
            .map_err(|e| {
                IndexerError::StorageRead(format!("read {}: {}", str_idx_path.display(), e))
            })?;
        let str_fwd_data = std::fs::read(&str_fwd_path)
            .map_err(|e| IndexerError::StorageRead(format!("read strings.fwd: {}", e)))?;

        let str_fwd_entries: Vec<ForwardEntry> = str_offsets
            .iter()
            .zip(str_lens.iter())
            .enumerate()
            .map(|(i, (&off, &len))| ForwardEntry {
                id: i as u64,
                value: str_fwd_data[off as usize..(off as usize + len as usize)].to_vec(),
            })
            .collect();
        let count = str_fwd_entries.len();
        let mut str_rev_entries: Vec<ReverseEntry> = str_fwd_entries
            .iter()
            .map(|e| ReverseEntry {
                key: e.value.clone(),
                id: e.id,
            })
            .collect();
        str_rev_entries.sort_by(|a, b| a.key.cmp(&b.key));

        let stf_tree =
            builder::build_forward_tree(str_fwd_entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build string fwd tree: {}", e)))?;
        let str_tree =
            builder::build_reverse_tree(str_rev_entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build string rev tree: {}", e)))?;

        let sf = upload_tree(storage, alias, stf_tree, DictKind::StringForward).await?;
        let sr = upload_tree(storage, alias, str_tree, DictKind::StringReverse).await?;
        (count, sf, sr)
    } else {
        // No strings persisted — empty trees
        let empty_fwd = builder::build_forward_tree(vec![], builder::DEFAULT_TARGET_LEAF_BYTES)
            .map_err(|e| {
                IndexerError::StorageWrite(format!("build empty string fwd tree: {}", e))
            })?;
        let empty_rev = builder::build_reverse_tree(vec![], builder::DEFAULT_TARGET_LEAF_BYTES)
            .map_err(|e| {
                IndexerError::StorageWrite(format!("build empty string rev tree: {}", e))
            })?;
        let sf = upload_tree(storage, alias, empty_fwd, DictKind::StringForward).await?;
        let sr = upload_tree(storage, alias, empty_rev, DictKind::StringReverse).await?;
        (0, sf, sr)
    };

    // ---- 4. Upload per-predicate numbig arenas ----
    let mut numbig = BTreeMap::new();
    let nb_dir = run_dir.join("numbig");
    if nb_dir.exists() {
        for entry in std::fs::read_dir(&nb_dir)
            .map_err(|e| IndexerError::StorageRead(format!("read numbig dir: {}", e)))?
        {
            let entry = entry
                .map_err(|e| IndexerError::StorageRead(format!("read numbig entry: {}", e)))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str.strip_prefix("p_") {
                if let Some(id_str) = rest.strip_suffix(".nba") {
                    if let Ok(p_id) = id_str.parse::<u32>() {
                        let addr =
                            upload_flat(storage, alias, &entry.path(), DictKind::NumBig { p_id })
                                .await?;
                        numbig.insert(p_id.to_string(), addr);
                    }
                }
            }
        }
    }

    // ---- 5. Compute subject_id_encoding + watermarks from sids ----
    let overflow_ns: u16 = 0xFFFF;
    let mut needs_wide = false;
    let mut max_ns_code: u16 = 0;
    let mut watermark_map: BTreeMap<u16, u64> = BTreeMap::new();

    for &sid in &sids {
        let subject_id = SubjectId::from_u64(sid);
        let ns_code = subject_id.ns_code();
        let local_id = subject_id.local_id();

        if ns_code == overflow_ns {
            needs_wide = true;
            // Overflow namespace: watermark stays at 0
            continue;
        }

        if local_id > u16::MAX as u64 {
            needs_wide = true;
        }

        if ns_code > max_ns_code {
            max_ns_code = ns_code;
        }

        let entry = watermark_map.entry(ns_code).or_insert(0);
        if local_id > *entry {
            *entry = local_id;
        }
    }

    let subject_id_encoding = if needs_wide {
        SubjectIdEncoding::Wide
    } else {
        SubjectIdEncoding::Narrow
    };

    // Build watermarks vec: watermarks[i] = max local_id for ns_code i
    let watermark_len = if watermark_map.is_empty() {
        0
    } else {
        max_ns_code as usize + 1
    };
    let mut subject_watermarks: Vec<u64> = vec![0; watermark_len];
    for (&ns_code, &max_local) in &watermark_map {
        subject_watermarks[ns_code as usize] = max_local;
    }

    let string_watermark = if string_count > 0 {
        (string_count - 1) as u32
    } else {
        0
    };

    tracing::info!(
        subjects = sids.len(),
        strings = string_count,
        numbig_count = numbig.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        "dictionary trees built and uploaded to CAS (from disk)"
    );

    Ok(UploadedDicts {
        dict_addresses: run_index::DictAddresses {
            graphs,
            datatypes,
            languages,
            subject_forward,
            subject_reverse,
            string_forward,
            string_reverse,
            numbig,
        },
        subject_id_encoding,
        subject_watermarks,
        string_watermark,
    })
}

/// Publish index result to nameservice
pub async fn publish_index_result<P: Publisher>(publisher: &P, result: &IndexResult) -> Result<()> {
    publisher
        .publish_index(&result.alias, &result.root_address, result.index_t)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_default() {
        let stats = IndexStats::default();
        assert_eq!(stats.flake_count, 0);
        assert_eq!(stats.leaf_count, 0);
        assert_eq!(stats.branch_count, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_normalize_alias_for_comparison() {
        // Canonical format passes through
        assert_eq!(normalize_alias_for_comparison("test:main"), "test:main");
        assert_eq!(
            normalize_alias_for_comparison("my-ledger:dev"),
            "my-ledger:dev"
        );

        // Storage-path format with single slash converts to canonical
        assert_eq!(normalize_alias_for_comparison("test/main"), "test:main");
        assert_eq!(
            normalize_alias_for_comparison("my-ledger/dev"),
            "my-ledger:dev"
        );

        // Both formats normalize to the same canonical form
        assert_eq!(
            normalize_alias_for_comparison("test/main"),
            normalize_alias_for_comparison("test:main")
        );

        // Name without branch gets default branch
        assert_eq!(normalize_alias_for_comparison("test"), "test:main");

        // Canonical format with explicit branch takes precedence over slashes in name
        // "org/project:main" - the colon is the branch separator, not the slash
        assert_eq!(
            normalize_alias_for_comparison("org/project:main"),
            "org/project:main"
        );

        // Multiple slashes without colon - treated as name with default branch
        // (we don't know which slash is the "branch separator")
        assert_eq!(normalize_alias_for_comparison("a/b/c"), "a/b/c:main");
    }
}
