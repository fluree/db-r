//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Memory-bounded batched rebuilds from commit history
//! - Incremental refresh from novelty
//! - Leaf-local same-op deduplication during refresh
//! - Namespace delta replay
//! - Deterministic serialization
//! - Garbage collection chain support
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! Both modes use the unified indexing pipeline via `build_index_for_ledger`:
//! - Attempts incremental refresh first if an index exists
//! - Falls back to memory-bounded batched rebuild if needed
//!
//! ## Unified Indexing Pipeline
//!
//! The `build_index_for_ledger` function implements a refresh-first strategy:
//! 1. If an existing index is available, attempt incremental refresh
//! 2. If refresh fails (or no index exists), fall back to batched rebuild
//!
//! The batched rebuild (`batched_rebuild_from_commits`) provides:
//! - Memory-bounded processing via configurable batch sizes
//! - Checkpoint/resume support for long-running rebuilds
//! - GC chain support through incremental index refreshes
//!
//! ## Incremental Refresh
//!
//! Incremental refresh rewrites only nodes affected by novelty using `refresh_index`.
//! This is much faster for large indexes with small updates.
//!
//! ## Deduplication Semantics
//!
//! The indexer applies **leaf-local deduplication** during index refresh. This removes
//! redundant consecutive same-op flakes (e.g., assert→assert or retract→retract for
//! the same fact) within each leaf node. State transitions (assert→retract→assert)
//! are preserved.
//!
//! Leaf-local dedup is semantically equivalent to global dedup for query correctness,
//! while being more memory-efficient since it operates incrementally during tree
//! traversal rather than requiring all flakes in memory.
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_indexer::{IndexerConfig, BatchedRebuildConfig, batched_rebuild_from_commits, refresh_index};
//!
//! // Unified indexing (recommended): refresh-first with batched rebuild fallback
//! let result = build_index_for_ledger(
//!     &storage,
//!     &nameservice,
//!     "mydb/main",
//!     IndexerConfig::default(),
//! ).await?;
//!
//! // Direct batched rebuild (for manual control)
//! let config = BatchedRebuildConfig::default();
//! let result = batched_rebuild_from_commits(
//!     &storage,
//!     head_commit_address,
//!     "mydb/main",
//!     config,
//! ).await?;
//!
//! // Incremental refresh (after applying more commits)
//! let result = refresh_index(
//!     &storage,
//!     &db,
//!     &novelty,
//!     target_t,
//!     IndexerConfig::default(),
//! ).await?;
//! ```

pub mod config;
pub mod error;
pub mod gc;
#[cfg(feature = "hll-stats")]
pub mod hll;
#[cfg(feature = "commit-v2")]
pub mod run_index;
pub mod namespace;
pub mod orchestrator;
pub mod refresh;
pub mod stats;
mod writer;

// Test-only modules for building initial indexes in refresh tests
#[cfg(test)]
mod builder;
#[cfg(test)]
mod node;

// Re-export main types
pub use config::IndexerConfig;
pub use error::{IndexerError, Result};
pub use orchestrator::{
    BackgroundIndexerWorker, IndexCompletion, IndexerHandle, IndexerOrchestrator, IndexOutcome,
    IndexPhase, IndexStatusSnapshot,
};
#[cfg(feature = "embedded-orchestrator")]
pub use orchestrator::{maybe_refresh_after_commit, require_refresh_before_commit, PostCommitIndexResult};
pub use refresh::{refresh_index, refresh_index_with_prev, RefreshIndexResult, RefreshResult, RefreshStats};
pub use stats::{IndexStatsHook, NoOpStatsHook, StatsArtifacts, StatsSummary};
pub use gc::{
    CleanGarbageConfig, CleanGarbageResult, GarbageRecord, GarbageRef,
    clean_garbage, collect_garbage_addresses, load_garbage_record, write_garbage_record,
    DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS,
};

// Note: The following types/functions are defined in this module and are automatically public:
// - build_index_for_ledger (heavy bounds: Storage + Send), refresh_index_for_ledger (light bounds)
// - BatchedRebuildConfig, BatchedRebuildResult, batched_rebuild_from_commits, batched_rebuild_resume
// - ReindexCheckpoint, IndexerConfigSnapshot, checkpoint_address, load_checkpoint, write_checkpoint, delete_checkpoint
// - ReindexProgress, ProgressCallback (for observability)
// - CURRENT_INDEX_VERSION, DEFAULT_BATCH_BYTES, DEFAULT_MAX_BATCH_COMMITS, DEFAULT_CHECKPOINT_INTERVAL

use fluree_db_core::cache::NoCache;
use fluree_db_core::serde::json::PrevIndexRef;
use fluree_db_core::{ContentAddressedWrite, Db, Storage, StorageWrite};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::{
    load_commit, trace_commit_envelopes, trace_commits,
    Commit, CommitEnvelope, Novelty,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
            branch_target_children: self.branch_target_children.unwrap_or(default.branch_target_children),
            branch_max_children: self.branch_max_children.unwrap_or(default.branch_max_children),
            gc_max_old_indexes: self.gc_max_old_indexes.unwrap_or(default.gc_max_old_indexes),
            gc_min_time_mins: self.gc_min_time_mins.unwrap_or(default.gc_min_time_mins),
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
            let checkpoint: ReindexCheckpoint = serde_json::from_slice(&bytes)
                .map_err(|e| IndexerError::Checkpoint(format!(
                    "Failed to parse checkpoint at {}: {}", address, e
                )))?;
            Ok(Some(checkpoint))
        }
        Err(e) => {
            // Best-effort detection of "not found" errors via string matching.
            // See doc comment above for caveats.
            let msg = e.to_string().to_lowercase();
            if msg.contains("not found") || msg.contains("does not exist") || msg.contains("no such file") {
                Ok(None)
            } else {
                Err(IndexerError::Checkpoint(format!(
                    "Failed to read checkpoint at {}: {}", address, e
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
        .map_err(|e| IndexerError::Checkpoint(format!(
            "Failed to serialize checkpoint: {}", e
        )))?;
    storage.write_bytes(&address, &bytes).await
        .map_err(|e| IndexerError::Checkpoint(format!(
            "Failed to write checkpoint to {}: {}", address, e
        )))?;
    tracing::debug!(
        address = %address,
        last_processed_t = checkpoint.last_processed_t,
        "Checkpoint written"
    );
    Ok(())
}

/// Delete a reindex checkpoint from storage
pub async fn delete_checkpoint<S: StorageWrite>(
    storage: &S,
    alias: &str,
) -> Result<()> {
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
            .field("progress_callback", &self.progress_callback.as_ref().map(|_| "<callback>"))
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

// =============================================================================
// Helper functions for batched rebuild operations
// =============================================================================

/// Build a PrevIndexRef from optional address and t values.
///
/// This pattern appears multiple times when building the GC chain reference
/// for refresh operations.
#[inline]
fn build_prev_index_ref(
    prev_index_address: Option<&String>,
    prev_index_t: Option<i64>,
) -> Option<PrevIndexRef> {
    prev_index_address.zip(prev_index_t).map(|(addr, t)| PrevIndexRef {
        t,
        address: addr.clone(),
    })
}

/// Create an IndexResult from a RefreshIndexResult.
///
/// Converts the refresh statistics into the public IndexStats format.
#[inline]
fn create_index_result_from_refresh(
    refresh_result: &RefreshIndexResult,
    alias: &str,
) -> IndexResult {
    IndexResult {
        root_address: refresh_result.root_address.clone(),
        index_t: refresh_result.index_t,
        alias: alias.to_string(),
        stats: IndexStats {
            flake_count: refresh_result.stats.nodes_written,
            leaf_count: refresh_result.stats.leaves_modified,
            branch_count: refresh_result.stats.branches_modified,
            total_bytes: 0, // Not tracked in refresh stats
        },
    }
}

/// Create a ReindexCheckpoint struct with the given parameters.
///
/// Consolidates the checkpoint struct construction that appears in both
/// `batched_rebuild_from_commits` and `batched_rebuild_resume`.
#[inline]
fn create_checkpoint_struct(
    alias: &str,
    target_head: &str,
    target_t: i64,
    last_processed_commit: &str,
    last_processed_t: i64,
    intermediate_index: Option<String>,
    batch_bytes: usize,
    max_batch_commits: usize,
    checkpoint_interval: u32,
    batches_flushed: usize,
    indexer_config: Option<IndexerConfigSnapshot>,
) -> ReindexCheckpoint {
    ReindexCheckpoint {
        alias: alias.to_string(),
        target_head: target_head.to_string(),
        target_t,
        last_processed_commit: last_processed_commit.to_string(),
        last_processed_t,
        intermediate_index,
        checkpoint_time_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
        batch_bytes,
        max_batch_commits,
        checkpoint_interval,
        batches_flushed,
        indexer_config,
    }
}

/// Memory-bounded batched rebuild from commit history
///
/// Two-phase algorithm:
/// 1. **Metadata scan (backwards)**: Walk commits collecting lightweight metadata
/// 2. **Forward processing**: Process commits with memory-bounded batching
///
/// # Memory Bounds
///
/// The function flushes the current batch when either:
/// - `novelty.size >= config.batch_bytes` (default: 100MB)
/// - `commits_in_batch >= config.max_batch_commits` (default: 100,000)
///
/// This ensures memory usage stays bounded regardless of ledger size.
///
/// # Checkpointing
///
/// If `config.checkpoint` is true, checkpoints are written after every
/// `config.checkpoint_interval` batch flushes. Checkpoints allow resuming
/// a failed reindex from the last flushed batch using `batched_rebuild_resume`.
/// On successful completion, the checkpoint is deleted.
///
/// # Arguments
///
/// * `storage` - Storage backend for reading commits and writing index
/// * `head_commit_address` - Address of the most recent commit
/// * `alias` - Ledger alias (e.g., "mydb/main")
/// * `config` - Batched rebuild configuration
///
/// # Returns
///
/// `BatchedRebuildResult` containing the final index and statistics about the rebuild.
pub async fn batched_rebuild_from_commits<S>(
    storage: &S,
    head_commit_address: &str,
    alias: &str,
    config: BatchedRebuildConfig,
) -> Result<BatchedRebuildResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let span = tracing::info_span!("batched_rebuild", ledger_alias = alias);
    let _guard = span.enter();

    tracing::info!(
        batch_bytes = config.batch_bytes,
        max_batch_commits = config.max_batch_commits,
        checkpoint = config.checkpoint,
        "Starting batched index rebuild"
    );

    // =========================================================================
    // PHASE 1: Metadata scan (backwards)
    // =========================================================================
    // Walk commits backwards collecting lightweight envelopes.

    tracing::debug!("Phase 1: Scanning commit metadata backwards");

    let mut envelopes: Vec<(String, CommitEnvelope)> = Vec::new();
    let mut target_t: i64 = 0; // Will be set from head envelope

    let mut stream = std::pin::pin!(trace_commit_envelopes(
        storage.clone(),
        head_commit_address.to_string(),
        0, // Scan all the way back
    ));

    while let Some(result) = stream.next().await {
        let (addr, envelope) = result?;

        // Capture target_t from first (head) envelope
        if envelopes.is_empty() {
            target_t = envelope.t;
        }

        envelopes.push((addr, envelope));
    }

    tracing::debug!(
        envelope_count = envelopes.len(),
        "Phase 1 complete: metadata scan finished"
    );

    // Reverse to get forward order (oldest first)
    envelopes.reverse();

    if envelopes.is_empty() {
        tracing::warn!("No commits found");
        return Err(IndexerError::NoCommits);
    }

    // =========================================================================
    // PHASE 2: Initialize starting state
    // =========================================================================
    // Always start from genesis for full rebuild

    tracing::info!("Starting full rebuild from genesis");
    let mut db = Db::genesis(storage.clone(), NoCache, alias);
    let mut novelty = Novelty::new(0);

    // =========================================================================
    // PHASE 3: Forward processing with batching
    // =========================================================================

    tracing::debug!("Phase 2: Processing commits forward with batching");

    let mut batches_flushed = 0;
    let mut commits_in_batch = 0;
    let mut prev_index_address: Option<String> = None;
    let mut prev_index_t: Option<i64> = None;

    // For checkpointing: track last processed commit
    #[allow(unused_assignments)]
    let mut last_processed_commit: String = String::new();
    let mut last_processed_t: i64 = 0;
    let mut batches_since_checkpoint: u32 = 0;

    // All commits need to be processed (starting from genesis)
    let commits_to_process: Vec<_> = envelopes.into_iter().collect();

    let total_commits = commits_to_process.len();
    tracing::info!(
        total_commits = total_commits,
        "Processing {} commits from genesis",
        total_commits
    );

    // Progress tracking for observability
    let start_time = std::time::Instant::now();
    let mut commits_processed: usize = 0;
    let mut bytes_flushed: u64 = 0;

    for (commit_addr, _envelope) in commits_to_process {
        // Track commit address for checkpointing
        last_processed_commit = commit_addr.clone();
        // Load the full commit (with flakes)
        let commit: Commit = load_commit(storage, &commit_addr).await?;

        // Apply namespace delta to Db BEFORE processing flakes
        // This ensures namespace codes are available for flake interpretation
        if !commit.namespace_delta.is_empty() {
            for (code, prefix) in &commit.namespace_delta {
                db.namespace_codes.insert(*code, prefix.clone());
            }
        }

        // Apply flakes to novelty
        novelty.apply_commit(commit.flakes, commit.t)?;
        commits_in_batch += 1;
        commits_processed += 1;

        // Check if we need to flush this batch
        let should_flush = novelty.size >= config.batch_bytes
            || commits_in_batch >= config.max_batch_commits;

        if should_flush {
            let batch_novelty_bytes = novelty.size;

            // Enhanced tracing span for this batch
            let batch_span = tracing::info_span!(
                "reindex_batch",
                batch_num = batches_flushed + 1,
                commits_in_batch = commits_in_batch,
                novelty_bytes = batch_novelty_bytes,
                from_t = last_processed_t,
                to_t = commit.t,
            );
            let _batch_guard = batch_span.enter();

            tracing::info!(
                batch = batches_flushed + 1,
                commits_in_batch = commits_in_batch,
                novelty_size = batch_novelty_bytes,
                target_t = commit.t,
                "Flushing batch"
            );

            // Build prev_index reference for GC chain
            let prev_index = build_prev_index_ref(prev_index_address.as_ref(), prev_index_t);

            // Refresh the index
            let refresh_result = refresh_index_with_prev(
                storage,
                &db,
                &novelty,
                commit.t,
                config.indexer_config.clone(),
                None,
                prev_index,
            )
            .await?;

            // Update state for next batch
            prev_index_address = Some(refresh_result.root_address.clone());
            prev_index_t = Some(refresh_result.index_t);

            // Load the new Db from the refresh result
            db = Db::load(storage.clone(), NoCache, &refresh_result.root_address).await?;

            // Track bytes flushed for progress reporting
            bytes_flushed += batch_novelty_bytes as u64;

            // Reset novelty for next batch
            novelty = Novelty::new(commit.t);
            commits_in_batch = 0;
            batches_flushed += 1;
            last_processed_t = commit.t;
            batches_since_checkpoint += 1;

            // Invoke progress callback if configured
            if let Some(ref callback) = config.progress_callback {
                let elapsed = start_time.elapsed().as_secs_f64();
                let commits_per_sec = if elapsed > 0.0 {
                    commits_processed as f64 / elapsed
                } else {
                    0.0
                };
                let remaining_commits = total_commits.saturating_sub(commits_processed);
                let estimated_remaining = if commits_per_sec > 0.0 {
                    Some(remaining_commits as f64 / commits_per_sec)
                } else {
                    None
                };

                let progress = ReindexProgress {
                    alias: alias.to_string(),
                    total_commits,
                    commits_processed,
                    batches_flushed,
                    bytes_flushed,
                    current_t: commit.t,
                    target_t,
                    commits_per_sec,
                    estimated_remaining_secs: estimated_remaining,
                };
                callback(progress);
            }

            // Write checkpoint if enabled and at interval
            if config.checkpoint && batches_since_checkpoint >= config.checkpoint_interval {
                let checkpoint = create_checkpoint_struct(
                    alias,
                    head_commit_address,
                    target_t,
                    &last_processed_commit,
                    last_processed_t,
                    prev_index_address.clone(),
                    config.batch_bytes,
                    config.max_batch_commits,
                    config.checkpoint_interval,
                    batches_flushed,
                    Some(IndexerConfigSnapshot::from(&config.indexer_config)),
                );
                write_checkpoint(storage, &checkpoint).await?;
                batches_since_checkpoint = 0;
            }
        }
    }

    // =========================================================================
    // PHASE 4: Final flush
    // =========================================================================
    // Process any remaining novelty

    if novelty.len() > 0 || commits_in_batch > 0 {
        let final_t = novelty.t;
        let batch_novelty_bytes = novelty.size;

        // Enhanced tracing span for final batch
        let batch_span = tracing::info_span!(
            "reindex_batch",
            batch_num = batches_flushed + 1,
            commits_in_batch = commits_in_batch,
            novelty_bytes = batch_novelty_bytes,
            final = true,
        );
        let _batch_guard = batch_span.enter();

        tracing::info!(
            commits_in_batch = commits_in_batch,
            novelty_size = batch_novelty_bytes,
            target_t = final_t,
            "Final batch flush"
        );

        let prev_index = build_prev_index_ref(prev_index_address.as_ref(), prev_index_t);

        let refresh_result = refresh_index_with_prev(
            storage,
            &db,
            &novelty,
            final_t,
            config.indexer_config.clone(),
            None,
            prev_index,
        )
        .await?;

        // Update progress tracking
        bytes_flushed += batch_novelty_bytes as u64;
        batches_flushed += 1;

        // Invoke final progress callback (100% complete)
        if let Some(ref callback) = config.progress_callback {
            let elapsed = start_time.elapsed().as_secs_f64();
            let commits_per_sec = if elapsed > 0.0 {
                total_commits as f64 / elapsed
            } else {
                0.0
            };

            let progress = ReindexProgress {
                alias: alias.to_string(),
                total_commits,
                commits_processed: total_commits, // All done
                batches_flushed,
                bytes_flushed,
                current_t: final_t,
                target_t,
                commits_per_sec,
                estimated_remaining_secs: Some(0.0), // Complete
            };
            callback(progress);
        }

        // Delete checkpoint on successful completion
        if config.checkpoint {
            delete_checkpoint(storage, alias).await?;
        }

        tracing::info!(
            batches_flushed = batches_flushed,
            index_t = refresh_result.index_t,
            root_address = %refresh_result.root_address,
            total_elapsed_secs = start_time.elapsed().as_secs_f64(),
            "Batched rebuild complete"
        );

        return Ok(BatchedRebuildResult {
            index_result: create_index_result_from_refresh(&refresh_result, alias),
            batches_flushed,
        });
    }

    // Edge case: no commits to process (index was already current)
    // This shouldn't happen in normal operation but handle gracefully
    // Delete checkpoint on successful completion
    if config.checkpoint {
        delete_checkpoint(storage, alias).await?;
    }

    tracing::info!(
        index_t = db.t,
        "No commits to process, index already current"
    );

    Ok(BatchedRebuildResult {
        index_result: IndexResult {
            root_address: prev_index_address.unwrap_or_default(),
            index_t: db.t,
            alias: alias.to_string(),
            stats: IndexStats::default(),
        },
        batches_flushed,
    })
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
    storage: &S,
    current_head_address: &str,
    checkpoint: ReindexCheckpoint,
    indexer_config_override: IndexerConfig,
) -> Result<BatchedRebuildResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let span = tracing::info_span!("batched_rebuild_resume", ledger_alias = %checkpoint.alias);
    let _guard = span.enter();

    // Restore indexer config from checkpoint, or use override
    let config_from_checkpoint = checkpoint.indexer_config.is_some();
    let indexer_config = checkpoint
        .indexer_config
        .as_ref()
        .map(|snap| snap.to_indexer_config())
        .unwrap_or(indexer_config_override);

    tracing::info!(
        last_processed_t = checkpoint.last_processed_t,
        target_t = checkpoint.target_t,
        batches_flushed = checkpoint.batches_flushed,
        checkpoint_interval = checkpoint.checkpoint_interval,
        config_from_checkpoint = config_from_checkpoint,
        "Resuming batched rebuild from checkpoint"
    );

    // Validate head hasn't changed
    if current_head_address != checkpoint.target_head {
        return Err(IndexerError::CheckpointHeadMismatch {
            checkpoint_head: checkpoint.target_head,
            current_head: current_head_address.to_string(),
        });
    }

    // Load intermediate index if available
    let (mut db, mut novelty, mut prev_index_address, mut prev_index_t): (
        Db<S, NoCache>,
        Novelty,
        Option<String>,
        Option<i64>,
    ) = if let Some(ref index_addr) = checkpoint.intermediate_index {
        match Db::load(storage.clone(), NoCache, index_addr).await {
            Ok(loaded_db) => {
                // Validate loaded index t matches checkpoint expectation
                // The loaded_db.t is ground truth - if it doesn't match, checkpoint is stale
                if loaded_db.t != checkpoint.last_processed_t {
                    return Err(IndexerError::Checkpoint(format!(
                        "Intermediate index t mismatch: loaded t={} but checkpoint expects t={}. \
                         The checkpoint may be stale or the index was modified.",
                        loaded_db.t, checkpoint.last_processed_t
                    )));
                }

                tracing::info!(
                    index_address = %index_addr,
                    index_t = loaded_db.t,
                    "Loaded intermediate index from checkpoint"
                );

                // Use loaded_db.t as the authoritative value for novelty initialization
                let actual_t = loaded_db.t;
                (
                    loaded_db,
                    Novelty::new(actual_t),
                    Some(index_addr.clone()),
                    Some(actual_t),
                )
            }
            Err(e) => {
                return Err(IndexerError::Checkpoint(format!(
                    "Failed to load intermediate index at {}: {}", index_addr, e
                )));
            }
        }
    } else {
        // No intermediate index - start from genesis (unlikely but handle it)
        tracing::warn!("No intermediate index in checkpoint, starting from genesis");
        let genesis_db = Db::genesis(storage.clone(), NoCache, &checkpoint.alias);
        (genesis_db, Novelty::new(0), None, None)
    };

    // Scan commits from checkpoint's last processed commit to head
    // We need to collect addresses of commits AFTER last_processed_commit
    tracing::debug!("Scanning remaining commits from checkpoint");

    let mut remaining_commits: Vec<(String, CommitEnvelope)> = Vec::new();
    let mut stream = std::pin::pin!(trace_commit_envelopes(
        storage.clone(),
        current_head_address.to_string(),
        checkpoint.last_processed_t, // Stop at checkpoint's last processed t
    ));

    while let Some(result) = stream.next().await {
        let (addr, envelope) = result?;
        // Only include commits AFTER checkpoint.last_processed_t
        if envelope.t > checkpoint.last_processed_t {
            remaining_commits.push((addr, envelope));
        }
    }

    // Reverse to get forward order
    remaining_commits.reverse();

    let total_remaining = remaining_commits.len();
    tracing::info!(
        remaining_commits = total_remaining,
        from_t = checkpoint.last_processed_t,
        to_t = checkpoint.target_t,
        "Processing remaining {} commits",
        total_remaining
    );

    // Process remaining commits with checkpointing enabled
    // Use checkpoint's stored interval, not a hardcoded default
    let mut batches_flushed = checkpoint.batches_flushed;
    let mut commits_in_batch = 0;
    #[allow(unused_assignments)]
    let mut last_processed_commit = checkpoint.last_processed_commit.clone();
    #[allow(unused_assignments)]
    let mut last_processed_t = checkpoint.last_processed_t;
    let mut batches_since_checkpoint: u32 = 0;
    let checkpoint_interval = checkpoint.checkpoint_interval;

    for (commit_addr, _envelope) in remaining_commits {
        last_processed_commit = commit_addr.clone();

        // Load the full commit
        let commit: Commit = load_commit(storage, &commit_addr).await?;
        last_processed_t = commit.t;

        // Apply namespace delta
        if !commit.namespace_delta.is_empty() {
            for (code, prefix) in &commit.namespace_delta {
                db.namespace_codes.insert(*code, prefix.clone());
            }
        }

        // Apply flakes to novelty
        novelty.apply_commit(commit.flakes, commit.t)?;
        commits_in_batch += 1;

        // Check if we need to flush
        let should_flush = novelty.size >= checkpoint.batch_bytes
            || commits_in_batch >= checkpoint.max_batch_commits;

        if should_flush {
            tracing::info!(
                batch = batches_flushed + 1,
                commits_in_batch = commits_in_batch,
                novelty_size = novelty.size,
                target_t = commit.t,
                "Flushing batch (resumed)"
            );

            let prev_index = build_prev_index_ref(prev_index_address.as_ref(), prev_index_t);

            let refresh_result = refresh_index_with_prev(
                storage,
                &db,
                &novelty,
                commit.t,
                indexer_config.clone(),
                None,
                prev_index,
            )
            .await?;

            prev_index_address = Some(refresh_result.root_address.clone());
            prev_index_t = Some(refresh_result.index_t);
            db = Db::load(storage.clone(), NoCache, &refresh_result.root_address).await?;
            novelty = Novelty::new(commit.t);
            commits_in_batch = 0;
            batches_flushed += 1;
            batches_since_checkpoint += 1;

            // Write checkpoint at interval
            if batches_since_checkpoint >= checkpoint_interval {
                let new_checkpoint = create_checkpoint_struct(
                    &checkpoint.alias,
                    &checkpoint.target_head,
                    checkpoint.target_t,
                    &last_processed_commit,
                    last_processed_t,
                    prev_index_address.clone(),
                    checkpoint.batch_bytes,
                    checkpoint.max_batch_commits,
                    checkpoint.checkpoint_interval,
                    batches_flushed,
                    checkpoint.indexer_config.clone(),
                );
                write_checkpoint(storage, &new_checkpoint).await?;
                batches_since_checkpoint = 0;
            }
        }
    }

    // Final flush
    if novelty.len() > 0 || commits_in_batch > 0 {
        let final_t = novelty.t;
        tracing::info!(
            commits_in_batch = commits_in_batch,
            novelty_size = novelty.size,
            target_t = final_t,
            "Final batch flush (resumed)"
        );

        let prev_index = build_prev_index_ref(prev_index_address.as_ref(), prev_index_t);

        let refresh_result = refresh_index_with_prev(
            storage,
            &db,
            &novelty,
            final_t,
            indexer_config.clone(),
            None,
            prev_index,
        )
        .await?;

        batches_flushed += 1;

        // Delete checkpoint on success
        delete_checkpoint(storage, &checkpoint.alias).await?;

        tracing::info!(
            batches_flushed = batches_flushed,
            index_t = refresh_result.index_t,
            root_address = %refresh_result.root_address,
            "Resumed batched rebuild complete"
        );

        return Ok(BatchedRebuildResult {
            index_result: create_index_result_from_refresh(&refresh_result, &checkpoint.alias),
            batches_flushed,
        });
    }

    // No remaining commits - checkpoint was at completion
    delete_checkpoint(storage, &checkpoint.alias).await?;

    tracing::info!(
        index_t = db.t,
        "Resumed rebuild: no remaining commits"
    );

    Ok(BatchedRebuildResult {
        index_result: IndexResult {
            root_address: prev_index_address.unwrap_or_default(),
            index_t: db.t,
            alias: checkpoint.alias,
            stats: IndexStats::default(),
        },
        batches_flushed,
    })
}

/// Refresh-only entry point with lighter trait bounds
///
/// Attempts incremental refresh from an existing index. This function has
/// lighter trait bounds than [`build_index_for_ledger`] because it doesn't
/// require `Storage` or `Send`.
///
/// Use this when:
/// - You know an existing index exists
/// - You want to avoid the heavier trait bounds
/// - You want refresh semantics without fallback to full rebuild
///
/// Returns `Ok(existing_index)` if the index is already current (no work needed).
///
/// Returns an error if:
/// - No existing index found
/// - Refresh fails (e.g., corrupted index, missing commits)
///
/// For automatic fallback to full rebuild, use [`build_index_for_ledger`] instead.
pub async fn refresh_index_for_ledger<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + StorageWrite + ContentAddressedWrite + Sync + Clone + 'static,
    N: NameService,
{
    let span = tracing::info_span!("index_refresh", ledger_alias = alias);
    let _guard = span.enter();

    tracing::info!("refreshing index for ledger (refresh-only mode)");

    // Look up the ledger record
    let record = nameservice
        .lookup(alias)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(alias.to_string()))?;

    // Require an existing index
    let index_addr = record
        .index_address
        .as_ref()
        .ok_or_else(|| IndexerError::NoIndex)?;

    // If index is already current, return it directly (no work needed)
    if record.index_t >= record.commit_t {
        tracing::info!(
            index_t = record.index_t,
            commit_t = record.commit_t,
            "index already current, returning existing"
        );
        return Ok(IndexResult {
            root_address: index_addr.clone(),
            index_t: record.index_t,
            alias: alias.to_string(),
            stats: IndexStats::default(),
        });
    }

    // Get head commit address (only needed for refresh path)
    let head_address = record
        .commit_address
        .as_ref()
        .ok_or_else(|| IndexerError::NoCommits)?;

    // Perform incremental refresh
    tracing::info!(
        index_t = record.index_t,
        commit_t = record.commit_t,
        delta = record.commit_t - record.index_t,
        "performing incremental refresh"
    );

    try_refresh(
        storage,
        index_addr,
        head_address,
        record.index_t,
        record.commit_t,
        alias,
        &config,
    )
    .await
}

/// External indexer entry point - refresh-first with rebuild fallback
///
/// Implements a refresh-first strategy:
/// 1. Look up ledger in nameservice
/// 2. If index exists and is behind commit_t: try incremental refresh
/// 3. If no index exists (or refresh fails): fall back to full rebuild
///
/// This provides efficient incremental updates when possible, with reliable
/// full rebuilds as a fallback.
///
/// **Note**: This function requires heavier trait bounds (`Storage + Send`)
/// because the fallback path uses batched rebuild with checkpoint cleanup.
/// For lighter bounds (refresh-only), use [`refresh_index_for_ledger`] instead.
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

    tracing::info!("building index for ledger");

    // Look up the ledger record
    tracing::debug!("looking up ledger record");
    let record = nameservice
        .lookup(alias)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(alias.to_string()))?;

    tracing::debug!(
        commit_t = record.commit_t,
        index_t = record.index_t,
        has_index = record.index_address.is_some(),
        "ledger record found"
    );

    // Get the head commit address
    let head_address = record
        .commit_address
        .as_ref()
        .ok_or_else(|| IndexerError::NoCommits)?;

    // Try refresh-first if there's an existing index
    if let Some(ref index_addr) = record.index_address {
        if record.index_t < record.commit_t {
            // Index exists but is behind - try incremental refresh
            tracing::info!(
                index_t = record.index_t,
                commit_t = record.commit_t,
                delta = record.commit_t - record.index_t,
                "attempting incremental refresh"
            );
            match try_refresh(
                storage,
                index_addr,
                head_address,
                record.index_t,
                record.commit_t,
                alias,
                &config,
            )
            .await
            {
                Ok(result) => {
                    tracing::info!("incremental refresh succeeded");
                    return Ok(result);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "incremental refresh failed, falling back to full rebuild");
                }
            }
        } else {
            // Index is already current (index_t >= commit_t)
            tracing::info!("index already current, skipping build");
            return Ok(IndexResult {
                root_address: index_addr.clone(),
                index_t: record.index_t,
                alias: alias.to_string(),
                stats: IndexStats::default(),
            });
        }
    } else {
        tracing::info!("no existing index found, performing full rebuild");
    }

    // Fallback: full rebuild from commits (using batched rebuild for memory efficiency)
    let batched_config = BatchedRebuildConfig::default().with_indexer_config(config);
    batched_rebuild_from_commits(storage, head_address, alias, batched_config)
        .await
        .map(|r| r.index_result)
}

/// Attempt incremental refresh from an existing index
///
/// This loads the existing index, builds novelty from commits since index_t,
/// and refreshes the index to the target commit_t.
async fn try_refresh<S>(
    storage: &S,
    index_address: &str,
    head_commit_address: &str,
    index_t: i64,
    commit_t: i64,
    alias: &str,
    config: &IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + StorageWrite + ContentAddressedWrite + Sync + Clone + 'static,
{
    let span = tracing::debug_span!("index_refresh", ledger_alias = alias, from_t = index_t, to_t = commit_t);
    let _guard = span.enter();

    // 1. Load existing Db from the index
    tracing::debug!("loading existing index");
    let mut db: Db<S, NoCache> = Db::load(storage.clone(), NoCache, index_address).await?;

    // 2. Build novelty from commits since index_t
    // trace_commits streams HEAD→oldest, so we collect and process
    tracing::debug!("building novelty from commits");
    let mut novelty = Novelty::new(index_t);
    let mut namespace_delta: HashMap<i32, String> = HashMap::new();

    let stream = trace_commits(storage.clone(), head_commit_address.to_string(), index_t);
    futures::pin_mut!(stream);

    let mut commit_count = 0;
    while let Some(result) = stream.next().await {
        let commit = result?;
        commit_count += 1;
        novelty.apply_commit(commit.flakes, commit.t)?;
        // trace_commits streams HEAD→oldest, so newer commits come first
        // Use or_insert to keep the first (newest) value for each key
        for (code, prefix) in commit.namespace_delta {
            namespace_delta.entry(code).or_insert(prefix);
        }
    }

    tracing::debug!(
        commit_count = commit_count,
        novelty_flake_count = novelty.len(),
        "novelty built"
    );

    // 3. Apply namespace deltas to Db BEFORE refresh
    // This ensures the refreshed DbRoot has the correct namespace_codes
    if !namespace_delta.is_empty() {
        tracing::debug!(namespace_update_count = namespace_delta.len(), "applying namespace deltas");
        for (code, prefix) in &namespace_delta {
            db.namespace_codes.insert(*code, prefix.clone());
        }
    }

    // 4. Refresh the index with prev_index for GC chain
    tracing::debug!("refreshing index");
    let prev_index = Some(fluree_db_core::serde::json::PrevIndexRef {
        t: index_t,
        address: index_address.to_string(),
    });
    let refresh_result = refresh_index_with_prev(
        storage, &db, &novelty, commit_t, config.clone(), None, prev_index
    ).await?;

    tracing::debug!(
        nodes_written = refresh_result.stats.nodes_written,
        leaves_modified = refresh_result.stats.leaves_modified,
        branches_modified = refresh_result.stats.branches_modified,
        "index refresh completed"
    );

    Ok(create_index_result_from_refresh(&refresh_result, alias))
}

/// Publish index result to nameservice
pub async fn publish_index_result<P: Publisher>(
    publisher: &P,
    result: &IndexResult,
) -> Result<()> {
    publisher
        .publish_index(&result.alias, &result.root_address, result.index_t)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::cache::NoCache;
    use fluree_db_core::{Flake, FlakeValue, MemoryStorage, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::Publisher;
    use fluree_db_novelty::Commit;
    use std::collections::{BTreeMap, HashMap as TestHashMap};

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
        assert_eq!(normalize_alias_for_comparison("my-ledger:dev"), "my-ledger:dev");

        // Storage-path format with single slash converts to canonical
        assert_eq!(normalize_alias_for_comparison("test/main"), "test:main");
        assert_eq!(normalize_alias_for_comparison("my-ledger/dev"), "my-ledger:dev");

        // Both formats normalize to the same canonical form
        assert_eq!(
            normalize_alias_for_comparison("test/main"),
            normalize_alias_for_comparison("test:main")
        );

        // Name without branch gets default branch
        assert_eq!(normalize_alias_for_comparison("test"), "test:main");

        // Canonical format with explicit branch takes precedence over slashes in name
        // "org/project:main" - the colon is the branch separator, not the slash
        assert_eq!(normalize_alias_for_comparison("org/project:main"), "org/project:main");

        // Multiple slashes without colon - treated as name with default branch
        // (we don't know which slash is the "branch separator")
        assert_eq!(normalize_alias_for_comparison("a/b/c"), "a/b/c:main");
    }

    fn make_flake(
        s_code: i32,
        s_name: &str,
        p_code: i32,
        p_name: &str,
        val: i64,
        t: i64,
    ) -> Flake {
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

    fn make_ref_flake(
        s_code: i32,
        s_name: &str,
        p_code: i32,
        p_name: &str,
        o_code: i32,
        o_name: &str,
        t: i64,
    ) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Ref(Sid::new(o_code, o_name)),
            Sid::new(0, "$id"),
            t,
            true,
            None,
        )
    }

    /// Store a commit in storage and return its address
    async fn store_commit(storage: &MemoryStorage, commit: &Commit) -> String {
        #[cfg(feature = "commit-v2")]
        let bytes = {
            use fluree_db_novelty::commit_v2::envelope::{encode_envelope_fields, CommitV2Envelope};
            use fluree_db_novelty::commit_v2::format::{
                self, CommitV2Footer, CommitV2Header, FOOTER_LEN, HASH_LEN, HEADER_LEN,
            };
            use fluree_db_novelty::commit_v2::op_codec::{encode_op, CommitDicts};
            use std::collections::HashMap;

            let mut dicts = CommitDicts::new();
            let mut ops_buf = Vec::new();
            for f in &commit.flakes {
                encode_op(f, &mut dicts, &mut ops_buf).unwrap();
            }

            let envelope = CommitV2Envelope {
                t: commit.t,
                v: commit.v,
                previous: commit.previous.clone(),
                previous_ref: commit.previous_ref.clone(),
                namespace_delta: commit
                    .namespace_delta
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect::<HashMap<_, _>>(),
                txn: commit.txn.clone(),
                time: commit.time.clone(),
                data: commit.data.clone(),
                index: commit.index.clone(),
                indexed_at: commit.indexed_at.clone(),
            };
            let mut envelope_bytes = Vec::new();
            encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

            let dict_bytes: Vec<Vec<u8>> = vec![
                dicts.graph.serialize(),
                dicts.subject.serialize(),
                dicts.predicate.serialize(),
                dicts.datatype.serialize(),
                dicts.object_ref.serialize(),
            ];

            let ops_section_len = ops_buf.len() as u32;
            let envelope_len = envelope_bytes.len() as u32;
            let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
            let mut dict_locations = [format::DictLocation::default(); 5];
            let mut offset = dict_start as u64;
            for (i, d) in dict_bytes.iter().enumerate() {
                dict_locations[i] = format::DictLocation {
                    offset,
                    len: d.len() as u32,
                };
                offset += d.len() as u64;
            }

            let footer = CommitV2Footer {
                dicts: dict_locations,
                ops_section_len,
            };
            let header = CommitV2Header {
                version: format::VERSION,
                flags: 0,
                t: commit.t,
                op_count: commit.flakes.len() as u32,
                envelope_len,
            };

            let total_len = HEADER_LEN
                + envelope_bytes.len()
                + ops_buf.len()
                + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
                + FOOTER_LEN
                + HASH_LEN;
            let mut blob = vec![0u8; total_len];

            let mut pos = 0;
            header.write_to(&mut blob[pos..]);
            pos += HEADER_LEN;
            blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
            pos += envelope_bytes.len();
            blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
            pos += ops_buf.len();
            for d in &dict_bytes {
                blob[pos..pos + d.len()].copy_from_slice(d);
                pos += d.len();
            }
            footer.write_to(&mut blob[pos..]);
            pos += FOOTER_LEN;

            use sha2::{Digest, Sha256};
            let hash: [u8; 32] = Sha256::digest(&blob[..pos]).into();
            blob[pos..pos + HASH_LEN].copy_from_slice(&hash);
            blob
        };

        #[cfg(not(feature = "commit-v2"))]
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
    async fn test_build_index_for_ledger_no_index_falls_back_to_rebuild() {
        // Setup storage and nameservice
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Create a chain of commits (c1 <- c2)
        let flakes_t1 = vec![
            make_flake(1, "ex:alice", 1, "ex:age", 30, 1),
            make_ref_flake(1, "ex:alice", 1, "ex:knows", 1, "ex:bob", 1),
        ];
        let commit1 = Commit {
            address: String::new(), // Will be set after storing
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: flakes_t1,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        let flakes_t2 = vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)];
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: flakes_t2,
            previous: Some(addr1.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::new(),
        };
        let addr2 = store_commit(&storage, &commit2).await;

        // Publish commits (but no index)
        ns.publish_commit("test:main", &addr1, 1).await.unwrap();
        ns.publish_commit("test:main", &addr2, 2).await.unwrap();

        // Build index - should fall back to rebuild since no index exists
        let config = IndexerConfig::small();
        let result = build_index_for_ledger(&storage, &ns, "test:main", config)
            .await
            .expect("Should build index successfully");

        assert_eq!(result.index_t, 2);
        assert_eq!(result.alias, "test:main");
        assert!(!result.root_address.is_empty());

        // Verify we can load the resulting index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &result.root_address)
            .await
            .expect("Should load db from index");
        assert_eq!(db.t, 2);
        assert_eq!(db.alias, "test:main");
    }

    #[tokio::test]
    async fn test_build_index_for_ledger_index_already_current_returns_existing() {
        // Setup
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Create commit
        let flakes = vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)];
        let commit = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr = store_commit(&storage, &commit).await;
        ns.publish_commit("test:main", &addr, 1).await.unwrap();

        // Build initial index
        let config = IndexerConfig::small();
        let first_result = build_index_for_ledger(&storage, &ns, "test:main", config.clone())
            .await
            .unwrap();

        // Publish the index
        ns.publish_index("test:main", &first_result.root_address, first_result.index_t)
            .await
            .unwrap();

        // Try to build again - should return existing index (no-op)
        let second_result = build_index_for_ledger(&storage, &ns, "test:main", config)
            .await
            .expect("Should return existing index");

        assert_eq!(second_result.root_address, first_result.root_address);
        assert_eq!(second_result.index_t, first_result.index_t);
        // Stats should be empty since no work was done
        assert_eq!(second_result.stats.flake_count, 0);
    }

    #[tokio::test]
    async fn test_build_index_for_ledger_refresh_path_when_index_behind() {
        // Setup
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Create initial commit and index at t=1
        let flakes_t1 = vec![
            make_flake(1, "ex:alice", 1, "ex:age", 30, 1),
            make_ref_flake(1, "ex:alice", 1, "ex:knows", 1, "ex:bob", 1),
        ];
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: flakes_t1,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;
        ns.publish_commit("test:main", &addr1, 1).await.unwrap();

        // Build initial index at t=1
        let config = IndexerConfig::small();
        let initial_result = build_index_for_ledger(&storage, &ns, "test:main", config.clone())
            .await
            .unwrap();
        ns.publish_index("test:main", &initial_result.root_address, initial_result.index_t)
            .await
            .unwrap();

        // Add more commits at t=2 and t=3
        let flakes_t2 = vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)];
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: flakes_t2,
            previous: Some(addr1.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::new(),
        };
        let addr2 = store_commit(&storage, &commit2).await;
        ns.publish_commit("test:main", &addr2, 2).await.unwrap();

        let flakes_t3 = vec![make_flake(1, "ex:charlie", 1, "ex:age", 35, 3)];
        let commit3 = Commit {
            address: String::new(),
            id: None,
            t: 3,
            v: 2,
            time: None,
            flakes: flakes_t3,
            previous: Some(addr2.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::new(),
        };
        let addr3 = store_commit(&storage, &commit3).await;
        ns.publish_commit("test:main", &addr3, 3).await.unwrap();

        // Now build index again - should use refresh path since index (t=1) is behind commit (t=3)
        let refresh_result = build_index_for_ledger(&storage, &ns, "test:main", config)
            .await
            .expect("Should refresh index successfully");

        assert_eq!(refresh_result.index_t, 3);
        assert_ne!(refresh_result.root_address, initial_result.root_address);

        // Verify we can load the resulting index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &refresh_result.root_address)
            .await
            .expect("Should load db from refreshed index");
        assert_eq!(db.t, 3);
    }

    #[tokio::test]
    async fn test_build_index_for_ledger_ledger_not_found() {
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        let config = IndexerConfig::small();
        let result = build_index_for_ledger(&storage, &ns, "nonexistent:main", config).await;

        assert!(matches!(result, Err(IndexerError::LedgerNotFound(_))));
    }

    #[tokio::test]
    async fn test_build_index_for_ledger_no_commits() {
        let storage = MemoryStorage::new();
        let ns = MemoryNameService::new();

        // Create ledger but don't publish any commits
        ns.create_ledger("test:main").unwrap();

        let config = IndexerConfig::small();
        let result = build_index_for_ledger(&storage, &ns, "test:main", config).await;

        assert!(matches!(result, Err(IndexerError::NoCommits)));
    }

    #[tokio::test]
    async fn test_try_refresh_builds_novelty_from_commits() {
        // Setup
        let storage = MemoryStorage::new();

        // Create initial flakes and build index at t=1
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)];

        let config = IndexerConfig::small();
        let initial_result = builder::build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Should build initial index");

        // Create commits at t=2 and t=3, linked to each other
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)],
            previous: None, // Doesn't matter for this test
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: Some(initial_result.root_address.clone()),
            txn: None,
            namespace_delta: TestHashMap::new(),
        };
        let addr2 = store_commit(&storage, &commit2).await;

        let commit3 = Commit {
            address: String::new(),
            id: None,
            t: 3,
            v: 2,
            time: None,
            flakes: vec![make_flake(1, "ex:charlie", 1, "ex:age", 35, 3)],
            previous: Some(addr2.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::new(),
        };
        let addr3 = store_commit(&storage, &commit3).await;

        // Try refresh from the initial index
        let refresh_result = try_refresh(
            &storage,
            &initial_result.root_address,
            &addr3,
            1, // index_t
            3, // commit_t
            "test/ledger",
            &config,
        )
        .await
        .expect("Should refresh successfully");

        assert_eq!(refresh_result.index_t, 3);

        // Load and verify the new index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &refresh_result.root_address)
            .await
            .expect("Should load refreshed db");
        assert_eq!(db.t, 3);
    }

    #[tokio::test]
    async fn test_batched_rebuild_small_batches() {
        // Test batched rebuild with small batch size to force multiple batches
        let storage = MemoryStorage::new();

        // First, create an initial index at t=1 so the genesis Db has roots
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:s0", 1, "ex:age", 0, 1)];
        let initial_result = builder::build_index(
            &storage,
            "test/main",
            1,
            initial_flakes.clone(),
            namespace_codes,
            IndexerConfig::small(),
        )
        .await
        .expect("Initial build should succeed");

        // Now create a chain of commits starting from this index
        // Commit 1 references the initial index
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: initial_flakes,
            previous: None,
            previous_ref: None,
            data: None,
            index: Some(fluree_db_novelty::IndexRef {
                id: None,
                address: initial_result.root_address.clone(),
                v: 2,
                t: Some(1),
            }),
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        // Create commits 2-5
        let mut addresses = vec![addr1.clone()];
        for t in 2i64..=5 {
            let flakes = vec![make_flake(t as i32, &format!("ex:s{}", t), 1, "ex:age", t * 10, t)];
            let commit = Commit {
                address: String::new(),
                id: None,
                t,
                v: 2,
                time: None,
                flakes,
                previous: addresses.last().cloned(),
                previous_ref: None,
                data: None,
                index: None,
                indexed_at: None,
                txn: None,
                namespace_delta: TestHashMap::new(),
            };
            let addr = store_commit(&storage, &commit).await;
            addresses.push(addr);
        }

        let head_address = addresses.last().unwrap();

        // Use small batch size to force multiple batches
        // Also limit commits per batch
        let config = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 150,
            max_batch_commits: 2, // Limit commits per batch
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        };

        let result = batched_rebuild_from_commits(&storage, head_address, "test:main", config)
            .await
            .expect("Batched rebuild should succeed");

        // Should have flushed multiple batches (5 commits / 2 per batch = 3 batches)
        assert!(result.batches_flushed >= 2, "Should flush at least 2 batches, got {}", result.batches_flushed);
        assert_eq!(result.index_result.index_t, 5);

        // Verify we can load the resulting index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &result.index_result.root_address)
            .await
            .expect("Should load db from batched rebuild");
        assert_eq!(db.t, 5);
    }

    #[tokio::test]
    async fn test_progress_callback_invoked() {
        // Test that progress callback is invoked with correct values
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let storage = MemoryStorage::new();

        // Create initial index
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:s0", 1, "ex:age", 0, 1)];
        let _initial_result = builder::build_index(
            &storage,
            "test/main",
            1,
            initial_flakes.clone(),
            namespace_codes,
            IndexerConfig::small(),
        )
        .await
        .expect("Initial build should succeed");

        // Create commits
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: initial_flakes,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        // Create commits 2-5
        let mut addresses = vec![addr1];
        for t in 2i64..=5 {
            let flakes = vec![make_flake(t as i32, &format!("ex:s{}", t), 1, "ex:age", t * 10, t)];
            let commit = Commit {
                address: String::new(),
                id: None,
                t,
                v: 2,
                time: None,
                flakes,
                previous: addresses.last().cloned(),
                previous_ref: None,
                data: None,
                index: None,
                indexed_at: None,
                txn: None,
                namespace_delta: TestHashMap::new(),
            };
            addresses.push(store_commit(&storage, &commit).await);
        }

        // Track callback invocations
        let callback_count = Arc::new(AtomicUsize::new(0));
        let last_progress = Arc::new(std::sync::Mutex::new(None::<ReindexProgress>));

        let count_clone = callback_count.clone();
        let progress_clone = last_progress.clone();
        let callback: ProgressCallback = Arc::new(move |progress| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            *progress_clone.lock().unwrap() = Some(progress);
        });

        let config = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 150, // Small to force multiple batches
            max_batch_commits: 2,
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: Some(callback),
        };

        let result = batched_rebuild_from_commits(
            &storage,
            addresses.last().unwrap(),
            "test:main",
            config,
        )
        .await
        .expect("Should succeed");

        // Verify callback was invoked at least once
        let count = callback_count.load(Ordering::SeqCst);
        assert!(count >= 1, "Progress callback should be invoked at least once, was invoked {} times", count);

        // Verify last progress values make sense
        let last = last_progress.lock().unwrap().clone().expect("Should have progress");
        assert_eq!(last.alias, "test:main");
        assert_eq!(last.total_commits, 5);
        assert_eq!(last.commits_processed, 5); // Final callback should show all processed
        assert_eq!(last.batches_flushed, result.batches_flushed);
        assert!(last.commits_per_sec > 0.0, "Should have positive throughput");
    }

    #[tokio::test]
    async fn test_try_refresh_applies_namespace_deltas() {
        // Setup
        let storage = MemoryStorage::new();

        // Create initial index with limited namespaces
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:alice", 1, "ex:age", 30, 1)];

        let config = IndexerConfig::small();
        let initial_result = builder::build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Should build initial index");

        // Create commit that introduces a new namespace
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: vec![make_flake(100, "schema:Person", 100, "schema:name", 42, 2)],
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: Some(initial_result.root_address.clone()),
            txn: None,
            namespace_delta: TestHashMap::from([(100, "schema:".to_string())]),
        };
        let addr2 = store_commit(&storage, &commit2).await;

        // Try refresh
        let refresh_result = try_refresh(
            &storage,
            &initial_result.root_address,
            &addr2,
            1,
            2,
            "test/ledger",
            &config,
        )
        .await
        .expect("Should refresh successfully");

        // Load and verify namespace was added
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &refresh_result.root_address)
            .await
            .expect("Should load refreshed db");

        assert!(db.namespace_codes.contains_key(&100));
        assert_eq!(db.namespace_codes.get(&100), Some(&"schema:".to_string()));
    }

    // =========================================================================
    // Integration Tests for Batched Reindex
    // =========================================================================

    #[tokio::test]
    async fn test_checkpoint_deleted_on_success() {
        // Test that checkpoint is deleted after successful build completion
        let storage = MemoryStorage::new();

        // Create initial index
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:s0", 1, "ex:age", 0, 1)];
        let _initial_result = builder::build_index(
            &storage,
            "test/main",
            1,
            initial_flakes.clone(),
            namespace_codes,
            IndexerConfig::small(),
        )
        .await
        .expect("Initial build should succeed");

        // Create commits
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: initial_flakes,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        // Create commits 2-10
        let mut addresses = vec![addr1];
        for t in 2i64..=10 {
            let flakes = vec![make_flake(t as i32, &format!("ex:s{}", t), 1, "ex:age", t * 10, t)];
            let commit = Commit {
                address: String::new(),
                id: None,
                t,
                v: 2,
                time: None,
                flakes,
                previous: addresses.last().cloned(),
                previous_ref: None,
                data: None,
                index: None,
                indexed_at: None,
                txn: None,
                namespace_delta: TestHashMap::new(),
            };
            addresses.push(store_commit(&storage, &commit).await);
        }

        let head_address = addresses.last().unwrap();

        // First do a full build WITHOUT checkpointing to get expected result
        let config_no_checkpoint = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 100, // Tiny to force multiple batches
            max_batch_commits: 3,
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        };

        let expected_result = batched_rebuild_from_commits(
            &storage,
            head_address,
            "test:main",
            config_no_checkpoint,
        )
        .await
        .expect("Full build should succeed");

        // Now do a build WITH checkpointing
        let config_with_checkpoint = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 100,
            max_batch_commits: 3,
            checkpoint: true,
            checkpoint_interval: 1, // Checkpoint every batch
            progress_callback: None,
        };

        let result_with_checkpoint = batched_rebuild_from_commits(
            &storage,
            head_address,
            "test:main",
            config_with_checkpoint,
        )
        .await
        .expect("Build with checkpoint should succeed");

        // Verify results match
        assert_eq!(result_with_checkpoint.index_result.index_t, expected_result.index_result.index_t);

        // Checkpoint should be deleted on successful completion
        let checkpoint = load_checkpoint(&storage, "test:main").await.expect("Load should not error");
        assert!(checkpoint.is_none(), "Checkpoint should be deleted after successful completion");
    }

    #[tokio::test]
    async fn test_checkpoint_resume_produces_correct_result() {
        // Test that batched_rebuild_resume() produces the same result as a full build
        let storage = MemoryStorage::new();

        // Namespace codes (stored in commit1's namespace_delta)
        let _namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);

        // Create commit 1 (genesis)
        let flakes1 = vec![make_flake(1, "ex:s1", 1, "ex:age", 10, 1)];
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: flakes1.clone(),
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([
                (0, "fluree:".to_string()),
                (1, "ex:".to_string()),
                (2, "xsd:".to_string()),
            ]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        // Create commits 2-5 (first half)
        let mut addresses = vec![addr1.clone()];
        for t in 2i64..=5 {
            let flakes = vec![make_flake(t as i32, &format!("ex:s{}", t), 1, "ex:age", t * 10, t)];
            let commit = Commit {
                address: String::new(),
                id: None,
                t,
                v: 2,
                time: None,
                flakes,
                previous: addresses.last().cloned(),
                previous_ref: None,
                data: None,
                index: None,
                indexed_at: None,
                txn: None,
                namespace_delta: TestHashMap::new(),
            };
            addresses.push(store_commit(&storage, &commit).await);
        }
        let addr5 = addresses.last().unwrap().clone();

        // Create commits 6-10 (second half)
        for t in 6i64..=10 {
            let flakes = vec![make_flake(t as i32, &format!("ex:s{}", t), 1, "ex:age", t * 10, t)];
            let commit = Commit {
                address: String::new(),
                id: None,
                t,
                v: 2,
                time: None,
                flakes,
                previous: addresses.last().cloned(),
                previous_ref: None,
                data: None,
                index: None,
                indexed_at: None,
                txn: None,
                namespace_delta: TestHashMap::new(),
            };
            addresses.push(store_commit(&storage, &commit).await);
        }
        let head_address = addresses.last().unwrap().clone();

        // Step 1: Build a partial index up to commit 5 (simulate interrupted build)
        let partial_config = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 1_000_000, // Large enough to process all in one batch
            max_batch_commits: 100,
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        };

        let partial_result = batched_rebuild_from_commits(
            &storage,
            &addr5,
            "test:main",
            partial_config,
        )
        .await
        .expect("Partial build should succeed");

        // The partial index is at t=5
        assert_eq!(partial_result.index_result.index_t, 5);

        // Step 2: Create a checkpoint representing the interrupted state
        let checkpoint = ReindexCheckpoint {
            alias: "test:main".to_string(),
            target_head: head_address.clone(),
            target_t: 10,
            last_processed_commit: addr5.clone(),
            last_processed_t: 5,
            intermediate_index: Some(partial_result.index_result.root_address.clone()),
            checkpoint_time_ms: 0,
            batch_bytes: 1_000_000,
            max_batch_commits: 100,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            batches_flushed: 1,
            indexer_config: Some(IndexerConfigSnapshot::from(&IndexerConfig::small())),
        };

        write_checkpoint(&storage, &checkpoint).await.expect("Should write checkpoint");

        // Step 3: Resume from checkpoint
        let resume_result = batched_rebuild_resume(
            &storage,
            &head_address,
            checkpoint,
            IndexerConfig::small(),
        )
        .await
        .expect("Resume should succeed");

        // Step 4: Do a full build from scratch for comparison
        let full_config = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 1_000_000,
            max_batch_commits: 100,
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        };

        let full_result = batched_rebuild_from_commits(
            &storage,
            &head_address,
            "test:main",
            full_config,
        )
        .await
        .expect("Full build should succeed");

        // Step 5: Verify resume produces the same result as full build
        assert_eq!(
            resume_result.index_result.index_t,
            full_result.index_result.index_t,
            "Resume should reach same t as full build"
        );
        assert_eq!(resume_result.index_result.index_t, 10);

        // Verify checkpoint is deleted after successful resume
        let checkpoint_after = load_checkpoint(&storage, "test:main").await.expect("Load should not error");
        assert!(checkpoint_after.is_none(), "Checkpoint should be deleted after successful resume");
    }

    #[tokio::test]
    async fn test_checkpoint_head_mismatch_detection() {
        // Test that resume detects when head has changed since checkpoint
        let storage = MemoryStorage::new();

        // Create a manual checkpoint pointing to a different head
        let checkpoint = ReindexCheckpoint {
            alias: "test:main".to_string(),
            target_head: "old_head_address".to_string(),
            target_t: 5,
            last_processed_commit: "some_commit".to_string(),
            last_processed_t: 3,
            intermediate_index: None,
            checkpoint_time_ms: 0,
            batch_bytes: DEFAULT_BATCH_BYTES,
            max_batch_commits: DEFAULT_MAX_BATCH_COMMITS,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            batches_flushed: 1,
            indexer_config: None,
        };

        write_checkpoint(&storage, &checkpoint).await.expect("Should write checkpoint");

        // Try to resume with a different head address
        let result = batched_rebuild_resume(
            &storage,
            "new_head_address",
            checkpoint,
            IndexerConfig::default(),
        )
        .await;

        // Should fail with CheckpointHeadMismatch
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, IndexerError::CheckpointHeadMismatch { .. }),
            "Expected CheckpointHeadMismatch error, got: {:?}", err
        );
    }

    #[tokio::test]
    async fn test_namespace_delta_across_batch_boundaries() {
        // Test that namespace deltas are correctly accumulated across batch boundaries
        let storage = MemoryStorage::new();

        // Create initial index with minimal namespaces
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
        ]);
        let initial_flakes = vec![make_flake(1, "ex:s0", 1, "ex:name", 100, 1)];
        let _initial_result = builder::build_index(
            &storage,
            "test/main",
            1,
            initial_flakes.clone(),
            namespace_codes,
            IndexerConfig::small(),
        )
        .await
        .expect("Initial build should succeed");

        // Create commits that introduce new namespaces across batches
        let commit1 = Commit {
            address: String::new(),
            id: None,
            t: 1,
            v: 2,
            time: None,
            flakes: initial_flakes,
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(1, "ex:".to_string())]),
        };
        let addr1 = store_commit(&storage, &commit1).await;

        // Commit 2: introduces "schema:" namespace
        let commit2 = Commit {
            address: String::new(),
            id: None,
            t: 2,
            v: 2,
            time: None,
            flakes: vec![make_flake(100, "schema:Person", 1, "ex:name", 200, 2)],
            previous: Some(addr1.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(100, "schema:".to_string())]),
        };
        let addr2 = store_commit(&storage, &commit2).await;

        // Commit 3: introduces "foaf:" namespace
        let commit3 = Commit {
            address: String::new(),
            id: None,
            t: 3,
            v: 2,
            time: None,
            flakes: vec![make_flake(200, "foaf:Agent", 1, "ex:name", 300, 3)],
            previous: Some(addr2.clone()),
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: TestHashMap::from([(200, "foaf:".to_string())]),
        };
        let addr3 = store_commit(&storage, &commit3).await;

        // Use tiny batch size to force namespace deltas to span batches
        let config = BatchedRebuildConfig {
            indexer_config: IndexerConfig::small(),
            batch_bytes: 50, // Very small
            max_batch_commits: 1, // Force batch after each commit
            checkpoint: false,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            progress_callback: None,
        };

        let result = batched_rebuild_from_commits(&storage, &addr3, "test:main", config)
            .await
            .expect("Batched rebuild should succeed");

        // Verify all namespaces are present in final index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &result.index_result.root_address)
            .await
            .expect("Should load final db");

        assert!(db.namespace_codes.contains_key(&1), "Should have ex: namespace");
        assert!(db.namespace_codes.contains_key(&100), "Should have schema: namespace");
        assert!(db.namespace_codes.contains_key(&200), "Should have foaf: namespace");
        assert_eq!(db.namespace_codes.get(&100), Some(&"schema:".to_string()));
        assert_eq!(db.namespace_codes.get(&200), Some(&"foaf:".to_string()));
    }
}
