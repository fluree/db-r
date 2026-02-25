//! Index statistics hooks.
//!
//! Provides a hook interface for collecting statistics during index building.
//!
//! ## Submodules
//!
//! - [`hashing`] — Domain-separated hashing for HLL registers
//! - [`sketch_cas`] — CAS-persisted HLL sketch blob serialization
//! - [`id_hook`] — ID-based per-(graph, property) HLL tracking
//! - [`schema_extractor`] — Schema hierarchy extraction from flakes
//! - [`class_property`] — Class-property statistics from novelty + PSOT
//! - [`class_stats`] — JSON/struct output for class stats from SPOT merge

pub mod class_property;
pub mod class_stats;
pub mod hashing;
pub mod id_hook;
pub mod schema_extractor;
pub mod sketch_cas;

// Re-export everything at stats:: level for backward compat
pub use class_property::{
    batch_lookup_subject_classes, compute_class_property_stats_parallel, ClassPropertyExtractor,
    ClassPropertyStatsResult,
};
pub use class_stats::{build_class_stat_entries, build_class_stats_json};
pub use hashing::{subject_hash, value_hash};
pub use id_hook::{GraphPropertyKey, IdPropertyHll, IdStatsHook, IdStatsResult, StatsRecord};
pub use schema_extractor::{SchemaEntry, SchemaExtractor};
pub use sketch_cas::{load_sketch_blob, HllPropertyEntry, HllSketchBlob};

use fluree_db_core::Flake;

/// Hook for collecting index statistics during build
///
/// Implementors receive callbacks during index building and produce
/// artifacts to persist alongside the index.
pub trait IndexStatsHook {
    /// Called for each flake during tree building
    fn on_flake(&mut self, flake: &Flake);

    /// Called after build completes, returns artifacts to persist
    fn finalize(self: Box<Self>) -> StatsArtifacts;
}

/// Artifacts produced by stats collection
#[derive(Debug, Clone, Default)]
pub struct StatsArtifacts {
    /// Summary fields for DbRoot (counts, NDV estimates)
    pub summary: StatsSummary,
}

/// Summary statistics for the index
#[derive(Debug, Clone, Default)]
pub struct StatsSummary {
    /// Total number of flakes in the index
    pub flake_count: usize,
    /// Per-property statistics (sorted by SID for determinism)
    ///
    /// Note: the current binary index pipeline uses `IdStatsHook` and produces
    /// ID-keyed sketches persisted via `HllSketchBlob`. This field is retained
    /// for legacy hook implementations and may be `None`.
    pub properties: Option<Vec<fluree_db_core::PropertyStatEntry>>,
}

/// No-op implementation for Phase A
///
/// Does nothing but count flakes. Placeholder for future HLL/sketch implementations.
#[derive(Debug, Default)]
pub struct NoOpStatsHook {
    flake_count: usize,
}

impl NoOpStatsHook {
    /// Create a new no-op stats hook
    pub fn new() -> Self {
        Self::default()
    }
}

impl IndexStatsHook for NoOpStatsHook {
    fn on_flake(&mut self, _flake: &Flake) {
        self.flake_count += 1;
    }

    fn finalize(self: Box<Self>) -> StatsArtifacts {
        StatsArtifacts {
            summary: StatsSummary {
                flake_count: self.flake_count,
                properties: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_test_flake(t: i64) -> Flake {
        Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_no_op_stats_hook() {
        let mut hook = NoOpStatsHook::new();

        hook.on_flake(&make_test_flake(1));
        hook.on_flake(&make_test_flake(2));
        hook.on_flake(&make_test_flake(3));

        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 3);
    }

    #[test]
    fn test_no_op_stats_hook_empty() {
        let hook = NoOpStatsHook::new();
        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 0);
    }
}
