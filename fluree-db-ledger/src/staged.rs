//! Staged transaction support
//!
//! This module provides `LedgerView` for staging transactions before commit.
//! A LedgerView combines:
//! - Base LedgerState (indexed LedgerSnapshot + committed novelty)
//! - Staged flakes (not yet committed)
//!
//! This enables query against staged changes without committing them.

use crate::LedgerState;
use fluree_db_core::{Flake, GraphDbRef, GraphId, IndexType, OverlayProvider};
use fluree_db_novelty::FlakeId;
use std::cmp::Ordering;

/// Arena-style storage for staged flakes
struct StagedStore {
    flakes: Vec<Flake>,
}

impl StagedStore {
    fn new(flakes: Vec<Flake>) -> Self {
        Self { flakes }
    }

    fn get(&self, id: FlakeId) -> &Flake {
        &self.flakes[id as usize]
    }

    fn len(&self) -> usize {
        self.flakes.len()
    }

    fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }
}

/// Staged overlay - maintains sorted vectors like Novelty
struct StagedOverlay {
    store: StagedStore,
    spot: Vec<FlakeId>,
    psot: Vec<FlakeId>,
    post: Vec<FlakeId>,
    opst: Vec<FlakeId>,
}

impl StagedOverlay {
    fn from_flakes(flakes: Vec<Flake>) -> Self {
        if flakes.is_empty() {
            return Self {
                store: StagedStore::new(vec![]),
                spot: vec![],
                psot: vec![],
                post: vec![],
                opst: vec![],
            };
        }

        let store = StagedStore::new(flakes);
        let ids: Vec<FlakeId> = (0..store.len() as FlakeId).collect();

        // Build sorted indexes
        let mut spot = ids.clone();
        spot.sort_by(|&a, &b| IndexType::Spot.compare(store.get(a), store.get(b)));

        let mut psot = ids.clone();
        psot.sort_by(|&a, &b| IndexType::Psot.compare(store.get(a), store.get(b)));

        let mut post = ids.clone();
        post.sort_by(|&a, &b| IndexType::Post.compare(store.get(a), store.get(b)));

        // OPST only for refs
        let mut opst: Vec<FlakeId> = ids
            .iter()
            .copied()
            .filter(|&id| store.get(id).is_ref())
            .collect();
        opst.sort_by(|&a, &b| IndexType::Opst.compare(store.get(a), store.get(b)));

        Self {
            store,
            spot,
            psot,
            post,
            opst,
        }
    }

    fn get_index(&self, index: IndexType) -> &[FlakeId] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    fn slice_for_range(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> &[FlakeId] {
        let ids = self.get_index(index);

        if ids.is_empty() {
            return &[];
        }

        let start = if leftmost {
            0
        } else if let Some(f) = first {
            ids.partition_point(|&id| index.compare(self.store.get(id), f) != Ordering::Greater)
        } else {
            0
        };

        let end = if let Some(r) = rhs {
            ids.partition_point(|&id| index.compare(self.store.get(id), r) != Ordering::Greater)
        } else {
            ids.len()
        };

        if start >= end {
            return &[];
        }

        &ids[start..end]
    }
}

/// A view of a ledger with staged (uncommitted) changes
///
/// This combines:
/// - Base LedgerState (indexed LedgerSnapshot + committed novelty)
/// - Staged flakes (not yet committed)
///
/// Queries against a LedgerView will see the staged changes.
pub struct LedgerView {
    /// Base ledger state
    base: LedgerState,
    /// Staged changes
    staged: StagedOverlay,
    /// Unique epoch for cache keys (different from base novelty)
    staged_epoch: u64,
}

impl LedgerView {
    /// Create a new ledger view with staged flakes
    pub fn stage(base: LedgerState, flakes: Vec<Flake>) -> Self {
        let staged_epoch = base.novelty.epoch + 1;
        Self {
            staged: StagedOverlay::from_flakes(flakes),
            staged_epoch,
            base,
        }
    }

    /// Get the base ledger state
    pub fn base(&self) -> &LedgerState {
        &self.base
    }

    /// Consume the view and return the owned base ledger state
    ///
    /// Use this when the staged changes should be discarded (e.g., no-op updates).
    pub fn into_base(self) -> LedgerState {
        self.base
    }

    /// Get the staged epoch
    pub fn epoch(&self) -> u64 {
        self.staged_epoch
    }

    /// Get the number of staged flakes
    pub fn staged_len(&self) -> usize {
        self.staged.store.len()
    }

    /// Check if there are staged flakes
    pub fn has_staged(&self) -> bool {
        !self.staged.store.is_empty()
    }

    /// Get a reference to the staged flakes
    pub fn staged_flakes(&self) -> &[Flake] {
        &self.staged.store.flakes
    }

    /// Get a reference to the underlying database
    pub fn db(&self) -> &fluree_db_core::LedgerSnapshot {
        &self.base.snapshot
    }

    /// Consume the view and return the base state and staged flakes
    pub fn into_parts(self) -> (LedgerState, Vec<Flake>) {
        (self.base, self.staged.store.flakes)
    }

    /// The effective as-of time for this staged view.
    ///
    /// When staged flakes exist, returns `base.t() + 1` (matching the `t`
    /// assigned to staged flakes in `stage.rs`). Otherwise returns `base.t()`.
    pub fn staged_t(&self) -> i64 {
        if self.has_staged() {
            self.base.t() + 1
        } else {
            self.base.t()
        }
    }

    /// Create a `GraphDbRef` bundling snapshot, graph id, overlay, and time.
    ///
    /// Uses `self` as the overlay (merges base novelty + staged flakes)
    /// and `staged_t()` as the time bound — ensuring staged flakes are
    /// visible through the overlay's `to_t` filtering.
    pub fn as_graph_db_ref(&self, g_id: GraphId) -> GraphDbRef<'_> {
        GraphDbRef::new(self.db(), g_id, self, self.staged_t())
    }
}

impl OverlayProvider for LedgerView {
    fn epoch(&self) -> u64 {
        self.staged_epoch
    }

    fn for_each_overlay_flake(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        // Two-way merge of base novelty slice + staged slice
        // Both are sorted, yield in merged order

        let base_slice = self
            .base
            .novelty
            .slice_for_range(index, first, rhs, leftmost);
        let staged_slice = self.staged.slice_for_range(index, first, rhs, leftmost);

        let mut base_iter = base_slice.iter().map(|&id| self.base.novelty.get_flake(id));
        let mut staged_iter = staged_slice.iter().map(|&id| self.staged.store.get(id));

        let mut base_next = base_iter.next();
        let mut staged_next = staged_iter.next();

        loop {
            match (base_next, staged_next) {
                (Some(base_flake), Some(staged_flake)) => {
                    let cmp = index.compare(base_flake, staged_flake);
                    if cmp != Ordering::Greater {
                        if base_flake.t <= to_t {
                            callback(base_flake);
                        }
                        base_next = base_iter.next();
                    } else {
                        if staged_flake.t <= to_t {
                            callback(staged_flake);
                        }
                        staged_next = staged_iter.next();
                    }
                }
                (Some(base_flake), None) => {
                    if base_flake.t <= to_t {
                        callback(base_flake);
                    }
                    base_next = base_iter.next();
                }
                (None, Some(staged_flake)) => {
                    if staged_flake.t <= to_t {
                        callback(staged_flake);
                    }
                    staged_next = staged_iter.next();
                }
                (None, None) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};
    use fluree_db_novelty::Novelty;

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(p, format!("p{}", p)),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_staged_overlay_empty() {
        let staged = StagedOverlay::from_flakes(vec![]);
        assert!(staged.store.is_empty());
    }

    #[test]
    fn test_staged_overlay_sorting() {
        let flakes = vec![
            make_flake(3, 1, 100, 1),
            make_flake(1, 1, 100, 1),
            make_flake(2, 1, 100, 1),
        ];

        let staged = StagedOverlay::from_flakes(flakes);

        // SPOT should be sorted by subject
        let spot_subjects: Vec<u16> = staged
            .spot
            .iter()
            .map(|&id| staged.store.get(id).s.namespace_code)
            .collect();
        assert_eq!(spot_subjects, vec![1, 2, 3]);
    }

    #[test]
    fn test_ledger_view_overlay_provider() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        // Create base novelty with some flakes
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1), make_flake(3, 1, 300, 1)], 1)
            .unwrap();

        let state = LedgerState::new(snapshot, novelty);

        // Create view with interleaved staged flakes
        let staged_flakes = vec![make_flake(2, 1, 200, 2), make_flake(4, 1, 400, 2)];
        let view = LedgerView::stage(state, staged_flakes);

        // Collect all flakes via overlay provider
        let mut collected = Vec::new();
        view.for_each_overlay_flake(IndexType::Spot, None, None, true, 100, &mut |f| {
            collected.push(f.s.namespace_code)
        });

        // Should be merged in sorted order
        assert_eq!(collected, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_ledger_view_epoch() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1)
            .unwrap();

        let base_epoch = novelty.epoch;
        let state = LedgerState::new(snapshot, novelty);

        let view = LedgerView::stage(state, vec![make_flake(2, 1, 200, 2)]);

        // Staged epoch should be different from base epoch
        assert_eq!(view.epoch(), base_epoch + 1);
    }

    #[test]
    fn test_ledger_view_into_parts() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);

        let staged_flakes = vec![make_flake(1, 1, 100, 1)];
        let view = LedgerView::stage(state, staged_flakes);

        let (base, flakes) = view.into_parts();
        assert_eq!(base.ledger_id(), "test:main");
        assert_eq!(flakes.len(), 1);
    }
}
