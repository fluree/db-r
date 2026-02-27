//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Binary columnar index building (`run_index` module)
//! - Background indexing orchestration
//! - Namespace delta replay
//! - Garbage collection support
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! The binary index pipeline (`run_index::index_build`) is the sole indexing path.
//!
//! ## Build Pipeline Modules
//!
//! The actual build pipelines live in [`build`] sub-modules:
//! - [`build::rebuild`]: Full index rebuild from genesis (Phase A..F)
//! - [`build::incremental`]: Incremental index update (Phase 1..5)
//! - [`build::upload`]: CAS upload primitives + index artifact upload
//! - [`build::upload_dicts`]: Dictionary flat-file upload
//! - [`build::spatial`]: Spatial index building
//! - [`build::root_assembly`]: Common root finalization
//! - [`build::commit_chain`]: Commit chain walking

mod build;
pub mod config;
pub mod drop;
pub mod error;
pub mod fulltext_hook;
pub mod gc;
#[path = "stats/hll256.rs"]
pub mod hll;
pub mod orchestrator;
pub mod run_index;
pub mod spatial_hook;
pub mod stats;

// Re-export main types
pub use config::IndexerConfig;
pub use drop::collect_ledger_cids;
pub use error::{IndexerError, Result};
pub use gc::{
    clean_garbage, load_garbage_record, write_garbage_record, CleanGarbageConfig,
    CleanGarbageResult, GarbageRecord, DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS,
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

// Re-export build pipeline types
pub use build::types::{UploadedDicts, UploadedIndexes};

use fluree_db_core::Storage;
use fluree_db_nameservice::{NameService, Publisher};
use tracing::Instrument;

/// Result of building an index
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Content identifier of the index root (derived from SHA-256 of root bytes).
    ///
    /// Always present — derived from the content hash of the index root during build,
    /// or from the persisted CID / address hash during early-return.
    pub root_id: fluree_db_core::ContentId,
    /// Transaction time the index is current through
    pub index_t: i64,
    /// Ledger ID (name:branch format)
    pub ledger_id: String,
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

/// Current index version for compatibility checking
pub const CURRENT_INDEX_VERSION: i32 = 2;

/// External indexer entry point
///
/// Builds a binary columnar index from the commit chain. The pipeline:
/// 1. Walks the commit chain and generates sorted run files
/// 2. Builds per-graph leaf/branch indexes for all sort orders
/// 3. Creates an `IndexRootV5` (IRB1) descriptor and writes it to storage
///
/// Returns early if the index is already current (no work needed).
/// Use `rebuild_index_from_commits` directly to force a rebuild regardless.
pub async fn build_index_for_ledger<S, N>(
    storage: &S,
    nameservice: &N,
    ledger_id: &str,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    let span = tracing::debug_span!("index_build", ledger_id = ledger_id);
    async move {
        // Look up the ledger record
        let record = nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| IndexerError::NameService(e.to_string()))?
            .ok_or_else(|| IndexerError::LedgerNotFound(ledger_id.to_string()))?;

        // If index is already current, return it
        if let Some(ref root_id) = record.index_head_id {
            if record.index_t >= record.commit_t {
                return Ok(IndexResult {
                    root_id: root_id.clone(),
                    index_t: record.index_t,
                    ledger_id: ledger_id.to_string(),
                    stats: IndexStats::default(),
                });
            }
        }

        // Try incremental indexing if conditions are met
        let commit_gap = record.commit_t - record.index_t;
        let can_incremental = config.incremental_enabled
            && record.index_head_id.is_some()
            && record.index_t > 0
            && commit_gap <= config.incremental_max_commits as i64;

        if can_incremental {
            tracing::info!(
                from_t = record.index_t,
                to_t = record.commit_t,
                commit_gap = commit_gap,
                "attempting incremental index"
            );
            match incremental_index_from_root(storage, ledger_id, &record, config.clone()).await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "incremental indexing failed, falling back to full rebuild"
                    );
                }
            }
        } else if config.incremental_enabled && record.index_head_id.is_some() && record.index_t > 0
        {
            tracing::info!(
                commit_gap = commit_gap,
                max = config.incremental_max_commits,
                "commit gap exceeds incremental limit, using full rebuild"
            );
        }

        rebuild_index_from_commits(storage, ledger_id, &record, config).await
    }
    .instrument(span)
    .await
}

/// Build a binary index from an existing nameservice record.
///
/// Unlike `build_index_for_ledger`, this skips the nameservice lookup and
/// the "already current" early-return check. Use this when you already have
/// the `NsRecord` and want to force a rebuild (e.g., `reindex`).
///
/// See [`build::rebuild::rebuild_index_from_commits`] for the full pipeline.
pub async fn rebuild_index_from_commits<S>(
    storage: &S,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    build::rebuild::rebuild_index_from_commits(storage, ledger_id, record, config).await
}

/// Build an incremental index from an existing root, resolving only new commits.
///
/// This is the fast path: instead of rebuilding from genesis, it loads the
/// existing `IndexRootV5`, resolves only commits `(root.index_t .. record.commit_t]`,
/// merges novelty into affected leaves, updates dictionaries incrementally,
/// and publishes a new root that references mostly-unchanged CAS artifacts.
///
/// Falls back to full rebuild on any error — correctness is never at risk.
pub async fn incremental_index_from_root<S>(
    storage: &S,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    build::incremental::incremental_index_from_root(storage, ledger_id, record, config).await
}

/// Upload all index artifacts (branches + leaves) to CAS.
///
/// For the default graph (g_id=0), leaves are uploaded but the branch is NOT —
/// leaf routing is embedded inline in the root. For named graphs, both branches
/// and leaves are uploaded.
pub async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    build_results: &[(
        fluree_db_binary_index::RunSortOrder,
        run_index::IndexBuildResult,
    )],
) -> Result<UploadedIndexes> {
    build::upload::upload_indexes_to_cas(storage, ledger_id, build_results).await
}

/// Upload dictionary artifacts from persisted flat files to CAS.
///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
pub async fn upload_dicts_from_disk<S: Storage>(
    storage: &S,
    ledger_id: &str,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<UploadedDicts> {
    build::upload_dicts::upload_dicts_from_disk(storage, ledger_id, run_dir, namespace_codes).await
}

/// Publish index result to nameservice
pub async fn publish_index_result<P: Publisher>(publisher: &P, result: &IndexResult) -> Result<()> {
    publisher
        .publish_index(&result.ledger_id, result.index_t, &result.root_id)
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
}
