//! Administrative operations for Fluree DB
//!
//! This module provides admin-level operations like `drop_ledger` and
//! `drop_graph_source` that are separate from normal CRUD operations.
//!
//! # Note
//!
//! These operations require `S: Storage`, which provides full read/write/delete
//! capabilities. They work with memory/file/S3 admin backends but are not
//! available on read-only storage.

use crate::{error::ApiError, tx::IndexingMode, Result};
use fluree_db_core::{address_path::alias_to_path_prefix, alias as core_alias, Storage};
use fluree_db_indexer::{build_binary_index, clean_garbage, CleanGarbageConfig};
use fluree_db_nameservice::{AdminPublisher, GraphSourcePublisher, NameService, Publisher};
use std::time::Duration;
use tracing::{info, warn};

// =============================================================================
// Drop Mode and Status Types
// =============================================================================

/// Mode for drop operation
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DropMode {
    /// Retract from nameservice only (data files remain)
    ///
    /// This is the default and safest option. The ledger is marked as retracted
    /// in the nameservice, but all data files remain on disk for potential
    /// recovery.
    #[default]
    Soft,

    /// Retract + delete all storage artifacts (irreversible)
    ///
    /// **WARNING**: This is irreversible. All commit and index files will be
    /// permanently deleted after the nameservice retraction.
    Hard,
}

/// Result status of drop operation
///
/// NOTE: This reflects the **nameservice state at lookup time**, not deletion success.
/// Deletion success is reported via `index_files_deleted`, `commit_files_deleted`,
/// and `warnings` fields in `DropReport`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropStatus {
    /// Record existed and was not retracted at lookup time
    Dropped,
    /// Record was already marked as retracted
    AlreadyRetracted,
    /// No record found for this alias
    #[default]
    NotFound,
}

// =============================================================================
// Drop Report Types
// =============================================================================

/// Report of what was deleted/retracted for a ledger
#[derive(Debug, Clone, Default)]
pub struct DropReport {
    /// The normalized alias that was dropped
    pub alias: String,
    /// Status based on nameservice state at lookup time
    pub status: DropStatus,
    /// Number of index files deleted (Hard mode only)
    pub index_files_deleted: usize,
    /// Number of commit files deleted (Hard mode only)
    pub commit_files_deleted: usize,
    /// Any non-fatal errors or warnings encountered during the operation
    pub warnings: Vec<String>,
}

/// Report of what was deleted/retracted for a graph source
#[derive(Debug, Clone, Default)]
pub struct GraphSourceDropReport {
    /// Name of the graph source
    pub name: String,
    /// Branch of the graph source
    pub branch: String,
    /// Status based on nameservice state at lookup time
    pub status: DropStatus,
    /// Number of files deleted (Hard mode only)
    pub files_deleted: usize,
    /// Any non-fatal errors or warnings encountered during the operation
    pub warnings: Vec<String>,
}

// =============================================================================
// Index Maintenance Types
// =============================================================================

/// Options for trigger_index operation
#[derive(Debug, Clone, Default)]
pub struct TriggerIndexOptions {
    /// Timeout in milliseconds (default: 300,000 = 5 minutes)
    pub timeout_ms: Option<u64>,
}

impl TriggerIndexOptions {
    /// Default timeout: 5 minutes
    pub const DEFAULT_TIMEOUT_MS: u64 = 300_000;

    /// Set the timeout in milliseconds
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// Options for reindex operation
#[derive(Debug, Clone, Default)]
pub struct ReindexOptions {
    /// Indexer configuration (leaf/branch sizes, GC settings)
    /// If not specified, uses IndexerConfig::default()
    pub indexer_config: Option<fluree_db_indexer::IndexerConfig>,
}

impl ReindexOptions {
    /// Set the indexer configuration for controlling output index structure
    ///
    /// Controls leaf/branch node sizes in the resulting index.
    pub fn with_indexer_config(mut self, config: fluree_db_indexer::IndexerConfig) -> Self {
        self.indexer_config = Some(config);
        self
    }
}

/// Result of trigger_index operation
#[derive(Debug, Clone)]
pub struct TriggerIndexResult {
    /// Ledger alias
    pub alias: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// Storage address of the index root
    pub root_address: String,
}

/// Result of reindex operation
#[derive(Debug, Clone)]
pub struct ReindexResult {
    /// Ledger alias
    pub alias: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// Storage address of the new index root
    pub root_address: String,
    /// Build statistics
    pub stats: fluree_db_indexer::IndexStats,
}

/// Result of index_status query
#[derive(Debug, Clone)]
pub struct IndexStatusResult {
    /// Ledger alias
    pub alias: String,
    /// Current index transaction time (from nameservice)
    pub index_t: i64,
    /// Current commit transaction time (from nameservice)
    pub commit_t: i64,
    /// Whether background indexing is enabled
    pub indexing_enabled: bool,
    /// Current indexing phase (Idle/Pending/InProgress)
    pub phase: fluree_db_indexer::IndexPhase,
    /// Pending minimum t (if work is queued)
    pub pending_min_t: Option<i64>,
    /// Last error message (if any)
    pub last_error: Option<String>,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Normalize alias to canonical form with branch
///
/// If the alias already contains a colon (indicating a branch), it's returned as-is.
/// Otherwise, `:main` is appended as the default branch.
fn normalize_alias(alias: &str) -> String {
    core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string())
}

// =============================================================================
// Fluree Drop Implementation
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    // NOTE: Storage trait provides full read/write/delete capabilities.
    S: Storage + Clone + 'static,
    N: NameService + Publisher + Send + Sync + 'static,
{
    /// Drop a ledger
    ///
    /// This operation:
    /// 1. Normalizes the alias (ensures branch suffix like `:main`)
    /// 2. Cancels any pending background indexing
    /// 3. Waits for in-progress indexing to complete
    /// 4. In Hard mode: deletes all storage artifacts (commits + indexes)
    /// 5. Retracts from nameservice
    /// 6. Disconnects from ledger cache (if caching enabled)
    ///
    /// # Arguments
    ///
    /// * `alias` - Ledger alias (e.g., "mydb" or "mydb:main")
    /// * `mode` - `Soft` (retract only) or `Hard` (retract + delete files)
    ///
    /// # Safety
    ///
    /// - `Soft` mode is reversible (data remains, only nameservice retracted)
    /// - `Hard` mode is **IRREVERSIBLE** - all data will be permanently deleted
    ///
    /// # Idempotency
    ///
    /// Safe to call multiple times:
    /// - Returns `AlreadyRetracted` if ledger was previously dropped
    /// - Hard mode still attempts deletion even for `NotFound`/`AlreadyRetracted`
    ///   to enable admin cleanup scenarios
    ///
    /// # External Indexers
    ///
    /// This only stops the in-process background worker. External indexers
    /// (Lambda, etc.) **MUST** check `NsRecord.retracted` before indexing
    /// and before publishing to prevent recreating files after drop.
    pub async fn drop_ledger(&self, alias: &str, mode: DropMode) -> Result<DropReport> {
        // 1. Normalize alias (ensure branch suffix)
        let alias = normalize_alias(alias);
        info!(alias = %alias, mode = ?mode, "Dropping ledger");

        let mut report = DropReport {
            alias: alias.clone(),
            ..Default::default()
        };

        // 2. Lookup current state (for status reporting)
        let record = self.nameservice.lookup(&alias).await?;
        let status = match &record {
            None => DropStatus::NotFound,
            Some(r) if r.retracted => DropStatus::AlreadyRetracted,
            Some(_) => DropStatus::Dropped,
        };
        report.status = status;

        // 3. Stop background indexing (THE FLAKE FIX)
        // NOTE: This only stops the in-process worker. External indexers must
        // check NsRecord.retracted and refuse to index/publish if true.
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            info!(alias = %alias, "Cancelling pending indexing");
            handle.cancel(&alias).await;
            handle.wait_for_idle(&alias).await;
            info!(alias = %alias, "Indexing cancelled and idle");
        }

        // 4. Delete artifacts (Hard mode)
        // Run deletion even for NotFound/AlreadyRetracted - enables admin cleanup
        if matches!(mode, DropMode::Hard) {
            // Canonical storage layout: `ledger/branch/...` (no ':') for portability.
            // Note: this applies to commits, txns, and indexes.
            let prefix = alias_to_path_prefix(&alias)
                .map_err(|e| ApiError::config(format!("Invalid alias '{}': {}", alias, e)))?;

            let commit_prefix = format!("fluree:file://{}/commit/", prefix);
            let index_prefix = format!("fluree:file://{}/index/", prefix);

            info!(commit_prefix = %commit_prefix, index_prefix = %index_prefix, "Deleting artifacts");

            let (commit_count, commit_warnings) =
                self.delete_by_prefix(self.storage(), &commit_prefix).await;
            let (index_count, index_warnings) =
                self.delete_by_prefix(self.storage(), &index_prefix).await;

            report.commit_files_deleted = commit_count;
            report.index_files_deleted = index_count;
            report.warnings.extend(commit_warnings);
            report.warnings.extend(index_warnings);

            info!(
                commit_deleted = commit_count,
                index_deleted = index_count,
                "Artifact deletion complete"
            );
        }

        // 5. Retract from nameservice
        // Always attempt retract on normalized alias - safe to call even if already
        // retracted (idempotent) or NotFound (no-op). This handles cases where
        // lookup used non-canonical alias but retract needs the normalized form.
        if let Err(e) = self.nameservice.retract(&alias).await {
            // Log but don't fail - retract may fail if truly not found
            warn!(alias = %alias, error = %e, "Nameservice retract warning");
            report.warnings.push(format!("Nameservice retract: {}", e));
        }

        // 6. Disconnect from ledger cache (if caching enabled)
        // This evicts the ledger from the LedgerManager so stale state isn't served.
        // Equivalent to Clojure's `release-ledger` at the end of drop-ledger.
        if let Some(mgr) = &self.ledger_manager {
            info!(alias = %alias, "Disconnecting ledger from cache");
            mgr.disconnect(&alias).await;
        }

        info!(alias = %alias, status = ?report.status, "Ledger dropped");
        Ok(report)
    }

    /// Delete all files matching a prefix
    ///
    /// Returns (count_deleted, warnings). Files are sorted before deletion
    /// for deterministic ordering across backends.
    async fn delete_by_prefix(&self, storage: &S, prefix: &str) -> (usize, Vec<String>) {
        let mut warnings = Vec::new();

        let mut files = match storage.list_prefix(prefix).await {
            Ok(f) => f,
            Err(e) => {
                warn!(prefix = %prefix, error = %e, "Failed to list prefix for deletion");
                warnings.push(format!("Failed to list {}: {}", prefix, e));
                return (0, warnings);
            }
        };

        // Sort for deterministic deletion order across backends (helps with debugging/logs)
        files.sort();

        let mut count = 0;
        for file in files {
            if let Err(e) = storage.delete(&file).await {
                warn!(file = %file, error = %e, "Failed to delete file");
                warnings.push(format!("Failed to delete {}: {}", file, e));
            } else {
                count += 1;
            }
        }
        (count, warnings)
    }
}

// =============================================================================
// Graph Source Drop Implementation
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + Publisher + GraphSourcePublisher,
{
    /// Drop a graph source
    ///
    /// This operation:
    /// 1. Looks up the graph source record in the nameservice
    /// 2. In Hard mode: deletes graph source index files (if prefix is defined)
    /// 3. Retracts from nameservice
    ///
    /// # Arguments
    ///
    /// * `name` - Graph source name (e.g., "my-search")
    /// * `branch` - Branch name (defaults to "main" if None)
    /// * `mode` - `Soft` (retract only) or `Hard` (retract + delete files)
    ///
    /// # Note
    ///
    /// Graph source artifact deletion requires a canonical storage prefix defined in the
    /// indexer crate. Until that exists, Hard mode may skip artifact deletion
    /// and report a warning.
    pub async fn drop_graph_source(
        &self,
        name: &str,
        branch: Option<&str>,
        mode: DropMode,
    ) -> Result<GraphSourceDropReport> {
        let branch = branch.unwrap_or("main");
        let alias = format!("{}:{}", name, branch);
        info!(name = %name, branch = %branch, mode = ?mode, "Dropping graph source");

        let mut report = GraphSourceDropReport {
            name: name.to_string(),
            branch: branch.to_string(),
            ..Default::default()
        };

        // 1. Lookup graph source record (for status)
        let record = self.nameservice.lookup_graph_source(&alias).await?;
        let status = match &record {
            None => DropStatus::NotFound,
            Some(r) if r.retracted => DropStatus::AlreadyRetracted,
            Some(_) => DropStatus::Dropped,
        };
        report.status = status;

        // 2. Delete graph source index files (Hard mode)
        if matches!(mode, DropMode::Hard) {
            // TODO: Call graph_source_artifact_prefix() from indexer crate once it exists
            // For now, skip deletion and report a warning
            if record.is_some() {
                report.warnings.push(
                    "Graph source artifact deletion not yet implemented - prefix not standardized"
                        .to_string(),
                );
            }
        }

        // 3. Retract from nameservice (always attempt, idempotent)
        if let Err(e) = self.nameservice.retract_graph_source(name, branch).await {
            warn!(name = %name, branch = %branch, error = %e, "Nameservice graph source retract warning");
            report.warnings.push(format!("Nameservice retract: {}", e));
        }

        info!(name = %name, branch = %branch, status = ?report.status, "Graph source dropped");
        Ok(report)
    }
}

// =============================================================================
// Index Status and Trigger (minimal bounds - not native-only)
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// Get current indexing status for a ledger
    ///
    /// Returns status from both nameservice (index_t, commit_t) and
    /// the background indexer (phase, pending work).
    pub async fn index_status(&self, alias: &str) -> Result<IndexStatusResult> {
        use fluree_db_indexer::IndexPhase;

        let alias = normalize_alias(alias);

        // Get nameservice record
        let record = self
            .nameservice
            .lookup(&alias)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {}", alias)))?;

        // Get indexer status if available
        let (indexing_enabled, phase, pending_min_t, last_error) = match &self.indexing_mode {
            IndexingMode::Background(handle) => {
                if let Some(status) = handle.status(&alias).await {
                    (true, status.phase, status.pending_min_t, status.last_error)
                } else {
                    (true, IndexPhase::Idle, None, None)
                }
            }
            IndexingMode::Disabled => (false, IndexPhase::Idle, None, None),
        };

        Ok(IndexStatusResult {
            alias,
            index_t: record.index_t,
            commit_t: record.commit_t,
            indexing_enabled,
            phase,
            pending_min_t,
            last_error,
        })
    }

    /// Trigger background indexing and wait for completion
    ///
    /// Enqueues an index request for the ledger and waits up to `timeout_ms`
    /// for the index to reach the current commit_t.
    ///
    /// # No-commit ledgers
    /// If the ledger has no commits yet, returns successfully with index_t=0.
    ///
    /// # Concurrent commits
    /// This targets `commit_t` at call time. Commits after the call aren't waited for.
    ///
    /// # Errors
    /// - `IndexingDisabled` if no background indexer configured
    /// - `IndexTimeout` if timeout expires before completion
    /// - `NotFound` if ledger doesn't exist
    pub async fn trigger_index(
        &self,
        alias: &str,
        opts: TriggerIndexOptions,
    ) -> Result<TriggerIndexResult> {
        use fluree_db_indexer::IndexOutcome;

        let alias = normalize_alias(alias);
        info!(alias = %alias, "Triggering index");

        // Check indexing mode
        let handle = match &self.indexing_mode {
            IndexingMode::Background(h) => h,
            IndexingMode::Disabled => return Err(ApiError::IndexingDisabled),
        };

        // Look up current state
        let record = self
            .nameservice
            .lookup(&alias)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {}", alias)))?;

        if record.retracted {
            return Err(ApiError::NotFound(format!(
                "Ledger is retracted: {}",
                alias
            )));
        }

        // Handle no-commit ledgers (nothing to index)
        if record.commit_address.is_none() {
            info!(alias = %alias, "No commits to index");
            return Ok(TriggerIndexResult {
                alias,
                index_t: 0,
                root_address: String::new(),
            });
        }

        // Trigger with min_t = commit_t
        let min_t = record.commit_t;
        let completion = handle.trigger(alias.clone(), min_t).await;

        // Wait with timeout
        let timeout_ms = opts
            .timeout_ms
            .unwrap_or(TriggerIndexOptions::DEFAULT_TIMEOUT_MS);
        let result =
            tokio::time::timeout(Duration::from_millis(timeout_ms), completion.wait()).await;

        match result {
            Ok(IndexOutcome::Completed {
                index_t,
                root_address,
            }) => {
                info!(alias = %alias, index_t = index_t, "Indexing completed");
                Ok(TriggerIndexResult {
                    alias,
                    index_t,
                    root_address,
                })
            }
            Ok(IndexOutcome::Failed(msg)) => {
                Err(ApiError::internal(format!("Indexing failed: {}", msg)))
            }
            Ok(IndexOutcome::Cancelled) => Err(ApiError::internal("Indexing was cancelled")),
            Err(_) => {
                warn!(alias = %alias, timeout_ms = timeout_ms, "Index trigger timed out");
                Err(ApiError::IndexTimeout(timeout_ms))
            }
        }
    }
}

// =============================================================================
// Reindex (requires AdminPublisher for allow-equal publish)
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + AdminPublisher,
{
    /// Full offline reindex from commit history
    ///
    /// Rebuilds the binary index by replaying all commits. This operation:
    /// 1. Cancels any background indexing
    /// 2. Builds a fresh binary columnar index from the commit chain
    /// 3. Validates ledger hasn't advanced (conflict detection)
    /// 4. Publishes new index (allows same t via AdminPublisher)
    ///
    /// # Errors
    /// - `NotFound` if ledger doesn't exist or has no commits
    /// - `ReindexConflict` (409) if ledger advanced during rebuild
    pub async fn reindex(&self, alias: &str, opts: ReindexOptions) -> Result<ReindexResult> {
        let alias = normalize_alias(alias);
        info!(alias = %alias, "Starting reindex");

        // 1. Look up current state and capture commit_t for conflict detection
        let record = self
            .nameservice
            .lookup(&alias)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Ledger not found: {}", alias)))?;

        if record.retracted {
            return Err(ApiError::NotFound(format!(
                "Ledger is retracted: {}",
                alias
            )));
        }

        let initial_commit_t = record.commit_t;
        if record.commit_address.is_none() {
            return Err(ApiError::NotFound("No commits to reindex".to_string()));
        }

        // 2. Cancel background indexing if active
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            info!(alias = %alias, "Cancelling background indexing for reindex");
            handle.cancel(&alias).await;
            handle.wait_for_idle(&alias).await;
        }

        // 3. Build binary index from commit chain
        let indexer_config = opts.indexer_config.clone().unwrap_or_default();
        let gc_max_old_indexes = indexer_config.gc_max_old_indexes;
        let gc_min_time_mins = indexer_config.gc_min_time_mins;

        let index_result =
            build_binary_index(self.storage(), &alias, &record, indexer_config).await?;

        info!(
            alias = %alias,
            index_t = index_result.index_t,
            "Binary index build complete"
        );

        // 4. Conflict detection: check if ledger advanced during rebuild
        let final_record = self.nameservice.lookup(&alias).await?.ok_or_else(|| {
            ApiError::NotFound(format!("Ledger disappeared during reindex: {}", alias))
        })?;

        if final_record.commit_t != initial_commit_t {
            return Err(ApiError::ReindexConflict {
                expected: initial_commit_t,
                found: final_record.commit_t,
            });
        }

        // 5. Publish new index (allows same t for reindex via AdminPublisher)
        self.nameservice
            .publish_index_allow_equal(&alias, &index_result.root_address, index_result.index_t)
            .await?;

        info!(
            alias = %alias,
            index_t = index_result.index_t,
            root_address = %index_result.root_address,
            "Reindex completed"
        );

        // 6. Spawn async garbage collection (non-blocking)
        let storage_clone = self.storage().clone();
        let root_address_clone = index_result.root_address.clone();
        let gc_config = CleanGarbageConfig {
            max_old_indexes: Some(gc_max_old_indexes),
            min_time_garbage_mins: Some(gc_min_time_mins),
        };
        tokio::spawn(async move {
            if let Err(e) = clean_garbage(&storage_clone, &root_address_clone, gc_config).await {
                tracing::warn!(
                    error = %e,
                    root_address = %root_address_clone,
                    "Background garbage collection failed (non-fatal)"
                );
            } else {
                tracing::debug!(root_address = %root_address_clone, "Background garbage collection completed");
            }
        });

        Ok(ReindexResult {
            alias,
            index_t: index_result.index_t,
            root_address: index_result.root_address,
            stats: index_result.stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_alias_with_branch() {
        assert_eq!(normalize_alias("test:main"), "test:main");
        assert_eq!(normalize_alias("mydb:feature"), "mydb:feature");
    }

    #[test]
    fn test_normalize_alias_without_branch() {
        assert_eq!(normalize_alias("test"), "test:main");
        assert_eq!(normalize_alias("mydb"), "mydb:main");
    }

    #[test]
    fn test_drop_mode_default() {
        assert_eq!(DropMode::default(), DropMode::Soft);
    }

    #[test]
    fn test_drop_status_default() {
        assert_eq!(DropStatus::default(), DropStatus::NotFound);
    }
}
