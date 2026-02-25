//! Bundled database reference for range queries.
//!
//! `GraphDbRef<'a>` combines the four values that always travel together
//! through leaf-crate function signatures:
//!
//! - `snapshot` — the indexed ledger snapshot
//! - `g_id` — which named graph to query
//! - `overlay` — novelty / staged flakes
//! - `t` — upper bound for visible flakes (as-of time)
//!
//! # Time semantics
//!
//! `GraphDbRef.t` is **the db value's as-of time**: the upper bound for
//! visible flakes, including overlay.  It is the responsibility of the
//! bridge/constructor to set this correctly:
//!
//! - `GraphDb.as_graph_db_ref()` → `self.t`
//! - `LedgerState.as_graph_db_ref(g_id)` → `max(novelty.t, snapshot.t)`
//! - `LedgerView.as_graph_db_ref(g_id)` → `base.t() + 1` when staged
//!
//! `from_t` is NOT part of the db value identity — history range queries
//! pass it via `RangeOptions` or as a separate parameter.

use crate::comparator::IndexType;
use crate::db::LedgerSnapshot;
use crate::error::Result;
use crate::flake::Flake;
use crate::ids::GraphId;
use crate::overlay::OverlayProvider;
use crate::query_bounds::{RangeMatch, RangeOptions, RangeTest};
use crate::range::{range_bounded_with_overlay, range_with_overlay};

/// Bundled database reference for range queries.
///
/// Combines the snapshot, graph id, overlay, and as-of time that are
/// always passed together through 40+ function signatures.
///
/// `Copy` — all fields are references or primitives (~34 bytes).
#[derive(Clone, Copy)]
pub struct GraphDbRef<'a> {
    pub snapshot: &'a LedgerSnapshot,
    pub g_id: GraphId,
    pub overlay: &'a dyn OverlayProvider,
    pub t: i64,
}

impl std::fmt::Debug for GraphDbRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphDbRef")
            .field("g_id", &self.g_id)
            .field("t", &self.t)
            .finish_non_exhaustive()
    }
}

impl<'a> GraphDbRef<'a> {
    /// Create a new `GraphDbRef`.
    pub fn new(
        snapshot: &'a LedgerSnapshot,
        g_id: GraphId,
        overlay: &'a dyn OverlayProvider,
        t: i64,
    ) -> Self {
        Self {
            snapshot,
            g_id,
            overlay,
            t,
        }
    }

    /// Execute a range query, auto-filling `to_t` from `self.t`.
    ///
    /// This is the primary convenience method — eliminates the need for
    /// callers to manually set `RangeOptions::default().with_to_t(...)`.
    pub async fn range(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
    ) -> Result<Vec<Flake>> {
        let opts = RangeOptions::default().with_to_t(self.t);
        range_with_overlay(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            test,
            match_val,
            opts,
        )
        .await
    }

    /// Execute a range query with explicit options, auto-filling `to_t`
    /// from `self.t` if the caller hasn't set it.
    pub async fn range_with_opts(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
        opts: RangeOptions,
    ) -> Result<Vec<Flake>> {
        let opts = if opts.to_t.is_none() {
            opts.with_to_t(self.t)
        } else {
            opts
        };
        range_with_overlay(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            test,
            match_val,
            opts,
        )
        .await
    }

    /// Execute a bounded range query with explicit start/end flakes,
    /// auto-filling `to_t` from `self.t` if the caller hasn't set it.
    pub async fn range_bounded(
        &self,
        index: IndexType,
        start_bound: Flake,
        end_bound: Flake,
        opts: RangeOptions,
    ) -> Result<Vec<Flake>> {
        let opts = if opts.to_t.is_none() {
            opts.with_to_t(self.t)
        } else {
            opts
        };
        range_bounded_with_overlay(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            start_bound,
            end_bound,
            opts,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::NoOverlay;

    #[test]
    fn test_graph_db_ref_is_copy() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 1);
        // Copy semantics — both bindings valid after copy
        let db2 = db;
        assert_eq!(db.t, db2.t);
        assert_eq!(db.g_id, db2.g_id);
    }

    #[tokio::test]
    async fn test_range_auto_fills_to_t() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 0);
        // Genesis + NoOverlay → empty result, but should not error
        let result = db
            .range(IndexType::Spot, RangeTest::Eq, RangeMatch::new())
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
