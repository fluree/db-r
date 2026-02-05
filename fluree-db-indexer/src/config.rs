//! Indexer configuration

use crate::gc::{DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS};
use std::path::PathBuf;

/// Configuration for index building
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Target estimated bytes per leaf node
    ///
    /// Leaves will be sized to approximately this many bytes during splits.
    /// Default: 187,500 (half of Clojure default overflow-bytes)
    pub leaf_target_bytes: u64,

    /// Maximum estimated bytes per leaf node
    ///
    /// Leaves split when they exceed this threshold.
    /// Default: 375,000 (Clojure default overflow-bytes)
    pub leaf_max_bytes: u64,

    /// Target number of children per branch node
    ///
    /// Branches will split when they exceed this threshold.
    /// Default: 100
    pub branch_target_children: usize,

    /// Maximum number of children per branch node
    ///
    /// Hard limit to prevent oversized branches.
    /// Default: 200
    pub branch_max_children: usize,

    /// Maximum number of old index versions to retain before garbage collection.
    ///
    /// After each index refresh, if there are more than this many old index
    /// versions in the prev-index chain, the oldest ones become eligible for GC.
    /// Default: 5
    pub gc_max_old_indexes: u32,

    /// Minimum age in minutes before an index version can be garbage collected.
    ///
    /// Even if an index exceeds `gc_max_old_indexes`, it won't be deleted until
    /// it's at least this old. This prevents deleting indexes that concurrent
    /// queries might still be using.
    /// Default: 30 minutes
    pub gc_min_time_mins: u32,

    /// Memory budget (bytes) for the run-sort buffer during index building.
    ///
    /// This total is split evenly across all sort orders (SPOT, PSOT, POST, OPST).
    /// Larger budgets produce fewer spill files and speed up the merge phase at
    /// the cost of higher peak memory. For bulk imports of 1 GB+, 1–2 GB is
    /// recommended.
    ///
    /// Default: 256 MB.
    pub run_budget_bytes: usize,

    /// Base directory for binary index artifacts.
    ///
    /// Ephemeral build artifacts (run files, dicts) are stored under:
    /// `{data_dir}/{alias_path}/tmp_import/{session_id}/`
    ///
    /// Durable index files are stored under:
    /// `{data_dir}/{alias_path}/index/`
    ///
    /// If `None`, defaults to `{system_temp_dir}/fluree-index`. For production
    /// deployments, this should always be set to a persistent directory.
    pub data_dir: Option<PathBuf>,
}

/// Default run-sort budget: 256 MB.
pub const DEFAULT_RUN_BUDGET_BYTES: usize = 256 * 1024 * 1024;

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            leaf_target_bytes: 187_500,
            leaf_max_bytes: 375_000,
            branch_target_children: 100,
            branch_max_children: 200,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
        }
    }
}

impl IndexerConfig {
    /// Create a new configuration with custom values
    pub fn new(
        leaf_target_bytes: u64,
        leaf_max_bytes: u64,
        branch_target_children: usize,
        branch_max_children: usize,
    ) -> Self {
        Self {
            leaf_target_bytes,
            leaf_max_bytes,
            branch_target_children,
            branch_max_children,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
        }
    }

    /// Create a configuration optimized for small datasets
    pub fn small() -> Self {
        Self {
            leaf_target_bytes: 50_000,
            leaf_max_bytes: 100_000,
            branch_target_children: 20,
            branch_max_children: 40,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
        }
    }

    /// Create a configuration optimized for large datasets
    pub fn large() -> Self {
        Self {
            leaf_target_bytes: 750_000,
            leaf_max_bytes: 1_500_000,
            branch_target_children: 200,
            branch_max_children: 400,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
        }
    }

    /// Builder method to set GC max old indexes
    pub fn with_gc_max_old_indexes(mut self, max_old: u32) -> Self {
        self.gc_max_old_indexes = max_old;
        self
    }

    /// Builder method to set GC min time in minutes
    pub fn with_gc_min_time_mins(mut self, min_time: u32) -> Self {
        self.gc_min_time_mins = min_time;
        self
    }

    /// Builder method to set the run-sort memory budget.
    ///
    /// For bulk imports of 1 GB+, use 1–2 GB (e.g., `1024 * 1024 * 1024`).
    pub fn with_run_budget_bytes(mut self, bytes: usize) -> Self {
        self.run_budget_bytes = bytes;
        self
    }

    /// Builder method to set the data directory for binary index artifacts
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IndexerConfig::default();
        assert_eq!(config.leaf_target_bytes, 187_500);
        assert_eq!(config.leaf_max_bytes, 375_000);
        assert_eq!(config.branch_target_children, 100);
        assert_eq!(config.branch_max_children, 200);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
        assert_eq!(config.gc_min_time_mins, DEFAULT_MIN_TIME_GARBAGE_MINS);
    }

    #[test]
    fn test_small_config() {
        let config = IndexerConfig::small();
        assert_eq!(config.leaf_target_bytes, 50_000);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
    }

    #[test]
    fn test_large_config() {
        let config = IndexerConfig::large();
        assert_eq!(config.leaf_target_bytes, 750_000);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
    }

    #[test]
    fn test_gc_config_builders() {
        let config = IndexerConfig::default()
            .with_gc_max_old_indexes(10)
            .with_gc_min_time_mins(60);
        assert_eq!(config.gc_max_old_indexes, 10);
        assert_eq!(config.gc_min_time_mins, 60);
    }
}
