//! Novelty overlay for Fluree DB
//!
//! This crate provides in-memory storage for uncommitted transactions (novelty)
//! that overlays the persisted index. It uses sorted vectors per index for
//! cache locality and efficient merge operations.
//!
//! # Design
//!
//! - **Arena storage**: Flakes stored once in a central arena, referenced by FlakeId
//! - **Per-index sorted vectors**: Each index (SPOT, PSOT, POST, OPST) maintains
//!   a sorted vector of FlakeIds ordered by that index's comparator
//! - **Batch commit**: Epoch bumps once per commit, not per flake
//! - **LSM-style merge**: Sort batch by index comparator, then linear merge with existing
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_novelty::Novelty;
//!
//! let mut novelty = Novelty::new(0);
//! novelty.apply_commit(flakes, 1)?;
//!
//! // Get slice for a leaf's range
//! let slice = novelty.slice_for_range(IndexType::Spot, Some(&first), Some(&rhs), false);
//! ```

mod commit;
mod commit_flakes;
pub mod commit_v2;
mod error;
mod stats;

pub use commit::{
    load_commit, load_commit_envelope, trace_commit_envelopes, trace_commits,
    Commit, CommitData, CommitEnvelope, CommitRef, IndexRef, TxnSignature,
};
pub use commit_v2::format::{CommitSignature, ALGO_ED25519};
pub use fluree_db_credential::SigningKey;
pub use commit_flakes::generate_commit_flakes;
pub use error::{NoveltyError, Result};
pub use stats::current_stats;

use fluree_db_core::{Flake, IndexType};
use rayon::Scope;
use std::cmp::Ordering;

/// Index into FlakeStore - u32 limits to ~4B flakes
pub type FlakeId = u32;

/// Maximum FlakeId before overflow
pub const MAX_FLAKE_ID: u32 = u32::MAX - 1;

/// Arena-style storage for flakes
///
/// Flakes are stored once and referenced by FlakeId across all 4 indexes.
#[derive(Default, Clone)]
pub struct FlakeStore {
    /// The actual flakes
    flakes: Vec<Flake>,
    /// Per-flake size in bytes (for accurate size tracking)
    sizes: Vec<usize>,
}

impl FlakeStore {
    /// Create a new empty flake store
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a flake by ID
    pub fn get(&self, id: FlakeId) -> &Flake {
        &self.flakes[id as usize]
    }

    /// Get the number of flakes stored
    pub fn len(&self) -> usize {
        self.flakes.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }

    /// Push a flake with a precomputed size (avoids double size_bytes)
    fn push_with_size(&mut self, flake: Flake, size: usize) -> FlakeId {
        let id = self.flakes.len() as FlakeId;
        self.sizes.push(size);
        self.flakes.push(flake);
        id
    }

    /// Test helper: push a flake (computes size).
    #[cfg(test)]
    fn push(&mut self, flake: Flake) -> FlakeId {
        let size = flake.size_bytes();
        self.push_with_size(flake, size)
    }

    /// Get the size of a flake by ID
    fn size(&self, id: FlakeId) -> usize {
        self.sizes[id as usize]
    }
}

/// Novelty overlay - in-memory storage for uncommitted transactions
///
/// Stores flakes in an arena with per-index sorted vectors for efficient
/// range queries and merge operations.
#[derive(Clone)]
pub struct Novelty {
    /// Canonical flake storage (arena)
    store: FlakeStore,

    /// Per-index sorted vectors of FlakeIds
    /// Each vector sorted by that index's comparator
    spot: Vec<FlakeId>,
    psot: Vec<FlakeId>,
    post: Vec<FlakeId>,
    opst: Vec<FlakeId>,

    /// Total size in bytes (for backpressure)
    pub size: usize,

    /// Latest transaction time in novelty
    pub t: i64,

    /// Epoch for cache invalidation - bumped once per commit
    pub epoch: u64,
}

impl Default for Novelty {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Novelty {
    /// Create a new empty novelty overlay
    pub fn new(t: i64) -> Self {
        Self {
            store: FlakeStore::new(),
            spot: Vec::new(),
            psot: Vec::new(),
            post: Vec::new(),
            opst: Vec::new(),
            size: 0,
            t,
            epoch: 0,
        }
    }

    /// Apply a batch of flakes from a commit
    ///
    /// Epoch bumps ONCE per call, not per flake.
    /// This performs 4 sorts (one per index) on the batch, then 4 linear merges.
    pub fn apply_commit(&mut self, flakes: Vec<Flake>, commit_t: i64) -> Result<()> {
        if flakes.is_empty() {
            return Ok(());
        }

        let span = tracing::info_span!(
            "novelty_apply_commit",
            commit_t = commit_t,
            flake_count = flakes.len(),
            rayon_threads = rayon::current_num_threads()
        );
        let _guard = span.enter();

        // Check FlakeId overflow
        let new_count = self.store.len() + flakes.len();
        if new_count > MAX_FLAKE_ID as usize {
            return Err(NoveltyError::overflow(
                "FlakeId overflow: too many flakes in novelty, trigger reindex",
            ));
        }

        // Update metadata
        self.t = self.t.max(commit_t);
        self.epoch += 1; // Bump epoch once per commit

        // Store flakes in arena and build batch IDs
        let base_id = self.store.len() as FlakeId;
        for flake in flakes {
            let size = flake.size_bytes();
            self.size += size;
            self.store.push_with_size(flake, size);
        }

        // Build batch IDs
        let batch_ids_vec: Vec<FlakeId> = (base_id..self.store.len() as FlakeId).collect();
        let batch_ids: &[FlakeId] = batch_ids_vec.as_slice();

        // Merge batch into each index (LSM-style merge).
        //
        // These merges are independent (read-only access to store + disjoint index vectors),
        // so we can run them in parallel to utilize multiple CPU cores.
        let store = &self.store;
        let (spot, psot, post, opst) = (
            &mut self.spot,
            &mut self.psot,
            &mut self.post,
            &mut self.opst,
        );

        // Propagate the parent span context into Rayon worker threads so that
        // per-index merge spans appear nested under `novelty_apply_commit` in traces.
        let parent = tracing::Span::current();

        rayon::scope(|scope: &Scope<'_>| {
            let parent_spot = parent.clone();
            scope.spawn(move |_| {
                let _p = parent_spot.enter();
                let span = tracing::info_span!("novelty_merge_spot", batch_len = batch_ids.len());
                let _g = span.enter();
                merge_batch_into_index(store, spot, batch_ids, IndexType::Spot)
            });
            let parent_psot = parent.clone();
            scope.spawn(move |_| {
                let _p = parent_psot.enter();
                let span = tracing::info_span!("novelty_merge_psot", batch_len = batch_ids.len());
                let _g = span.enter();
                merge_batch_into_index(store, psot, batch_ids, IndexType::Psot)
            });
            let parent_post = parent.clone();
            scope.spawn(move |_| {
                let _p = parent_post.enter();
                let span = tracing::info_span!("novelty_merge_post", batch_len = batch_ids.len());
                let _g = span.enter();
                merge_batch_into_index(store, post, batch_ids, IndexType::Post)
            });
            let parent_opst = parent.clone();
            scope.spawn(move |_| {
                let _p = parent_opst.enter();
                let span = tracing::info_span!("novelty_merge_opst", batch_len = batch_ids.len());
                let _g = span.enter();
                merge_batch_into_opst_refs_only(store, opst, batch_ids)
            });
        });

        Ok(())
    }

    // merge helpers are free functions below

    /// Clear flakes with t <= cutoff_t (after index merge)
    ///
    /// Uses bitmap instead of HashSet for cache-friendly O(n) clear.
    ///
    /// Note: In the standard Fluree indexing flow, Novelty is replaced entirely
    /// after each index rebuild rather than mutated in-place. This method exists
    /// for completeness but is rarely needed.
    pub fn clear_up_to(&mut self, cutoff_t: i64) {
        let n = self.store.len();
        if n == 0 {
            return;
        }

        // Build alive bitmap and compute new size
        let mut alive = vec![false; n];
        let mut new_size = 0usize;

        for i in 0..n {
            let flake = self.store.get(i as FlakeId);
            if flake.t > cutoff_t {
                alive[i] = true;
                new_size += self.store.size(i as FlakeId);
            }
        }

        // Retain only alive flakes in each index
        self.spot.retain(|&id| alive[id as usize]);
        self.psot.retain(|&id| alive[id as usize]);
        self.post.retain(|&id| alive[id as usize]);
        self.opst.retain(|&id| alive[id as usize]);

        // Update size
        self.size = new_size;

        self.epoch += 1;
    }

    /// Get slice of flake IDs for a leaf's range
    ///
    /// Uses binary search for O(log n + k) slicing.
    ///
    /// Clojure semantics:
    /// - If leftmost=false: left boundary is EXCLUSIVE (> first)
    /// - If leftmost=true: no left boundary
    /// - rhs is INCLUSIVE when present
    pub fn slice_for_range(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> &[FlakeId] {
        let ids = match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        };

        if ids.is_empty() {
            return &[];
        }

        // Find start index
        let start = if leftmost {
            0
        } else if let Some(f) = first {
            // Exclusive: find first element > first
            ids.partition_point(|&id| {
                index.compare(self.store.get(id), f) != Ordering::Greater
            })
        } else {
            0
        };

        // Find end index
        let end = if let Some(r) = rhs {
            // Inclusive: find first element > rhs
            ids.partition_point(|&id| {
                index.compare(self.store.get(id), r) != Ordering::Greater
            })
        } else {
            ids.len()
        };

        if start >= end {
            return &[];
        }

        &ids[start..end]
    }

    /// Get flake reference by ID
    pub fn get_flake(&self, id: FlakeId) -> &Flake {
        self.store.get(id)
    }

    /// Get the number of flakes in novelty
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Check if novelty is empty
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Iterate over all flake IDs for a given index
    pub fn iter_index(&self, index: IndexType) -> impl Iterator<Item = FlakeId> + '_ {
        let ids = match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        };
        ids.iter().copied()
    }
}

impl std::fmt::Debug for Novelty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Novelty")
            .field("flake_count", &self.store.len())
            .field("size", &self.size)
            .field("t", &self.t)
            .field("epoch", &self.epoch)
            .finish()
    }
}

// === OverlayProvider implementation ===

use fluree_db_core::OverlayProvider;

impl OverlayProvider for Novelty {
    fn epoch(&self) -> u64 {
        self.epoch
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
        let slice = self.slice_for_range(index, first, rhs, leftmost);

        for &id in slice {
            let flake = self.get_flake(id);
            if flake.t <= to_t {
                callback(flake);
            }
        }
    }
}

// =============================================================================
// Parallel merge helpers (read-only store + disjoint mutable index vectors)
// =============================================================================

/// LSM-style merge: sort batch by index comparator, then merge with existing target.
fn merge_batch_into_index(
    store: &FlakeStore,
    target: &mut Vec<FlakeId>,
    batch_ids: &[FlakeId],
    index: IndexType,
) {
    use rayon::prelude::*;

    // Sort batch by this index's comparator
    let mut sorted_batch = batch_ids.to_vec();
    sorted_batch.par_sort_unstable_by(|&a, &b| index.compare(store.get(a), store.get(b)));

    // Two-way merge existing + batch
    let mut merged = Vec::with_capacity(target.len() + sorted_batch.len());
    let mut i = 0;
    let mut j = 0;

    while i < target.len() && j < sorted_batch.len() {
        let cmp = index.compare(store.get(target[i]), store.get(sorted_batch[j]));
        if cmp != Ordering::Greater {
            merged.push(target[i]);
            i += 1;
        } else {
            merged.push(sorted_batch[j]);
            j += 1;
        }
    }
    merged.extend_from_slice(&target[i..]);
    merged.extend_from_slice(&sorted_batch[j..]);

    *target = merged;
}

/// Merge only reference flakes into OPST index.
fn merge_batch_into_opst_refs_only(store: &FlakeStore, opst: &mut Vec<FlakeId>, batch_ids: &[FlakeId]) {
    use rayon::prelude::*;

    // Filter to refs only
    let ref_ids: Vec<FlakeId> = batch_ids
        .iter()
        .copied()
        .filter(|&id| store.get(id).is_ref())
        .collect();

    if ref_ids.is_empty() {
        return;
    }

    // Sort by OPST comparator
    let mut sorted_batch = ref_ids;
    sorted_batch.par_sort_unstable_by(|&a, &b| IndexType::Opst.compare(store.get(a), store.get(b)));

    // Two-way merge
    let mut merged = Vec::with_capacity(opst.len() + sorted_batch.len());
    let mut i = 0;
    let mut j = 0;

    while i < opst.len() && j < sorted_batch.len() {
        let cmp = IndexType::Opst.compare(store.get(opst[i]), store.get(sorted_batch[j]));
        if cmp != Ordering::Greater {
            merged.push(opst[i]);
            i += 1;
        } else {
            merged.push(sorted_batch[j]);
            j += 1;
        }
    }
    merged.extend_from_slice(&opst[i..]);
    merged.extend_from_slice(&sorted_batch[j..]);

    *opst = merged;
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(p, format!("p{}", p)),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    fn make_ref_flake(s: u16, p: u16, o_sid: u16, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(p, format!("p{}", p)),
            FlakeValue::Ref(Sid::new(o_sid, format!("s{}", o_sid))),
            Sid::new(1, "id"), // $id datatype marks it as a ref
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_novelty_new() {
        let novelty = Novelty::new(5);
        assert_eq!(novelty.t, 5);
        assert_eq!(novelty.epoch, 0);
        assert_eq!(novelty.size, 0);
        assert!(novelty.is_empty());
    }

    #[test]
    fn test_apply_commit_single() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 200, 1, true),
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        assert_eq!(novelty.len(), 2);
        assert_eq!(novelty.t, 1);
        assert_eq!(novelty.epoch, 1); // Epoch bumped once
        assert!(novelty.size > 0);
    }

    #[test]
    fn test_apply_commit_multiple() {
        let mut novelty = Novelty::new(0);

        // First commit
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1)
            .unwrap();
        assert_eq!(novelty.epoch, 1);

        // Second commit
        novelty
            .apply_commit(vec![make_flake(2, 1, 200, 2, true)], 2)
            .unwrap();
        assert_eq!(novelty.epoch, 2); // Epoch bumped once per commit

        assert_eq!(novelty.len(), 2);
        assert_eq!(novelty.t, 2);
    }

    #[test]
    fn test_apply_commit_empty() {
        let mut novelty = Novelty::new(0);
        novelty.apply_commit(vec![], 1).unwrap();

        // Empty commit should not bump epoch
        assert_eq!(novelty.epoch, 0);
    }

    #[test]
    fn test_spot_ordering() {
        let mut novelty = Novelty::new(0);

        // Add flakes with different subjects
        let flakes = vec![
            make_flake(3, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        // SPOT should order by subject
        let spot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(spot_ids.len(), 3);

        let s1 = novelty.get_flake(spot_ids[0]).s.namespace_code;
        let s2 = novelty.get_flake(spot_ids[1]).s.namespace_code;
        let s3 = novelty.get_flake(spot_ids[2]).s.namespace_code;

        assert!(s1 <= s2 && s2 <= s3);
    }

    #[test]
    fn test_psot_ordering() {
        let mut novelty = Novelty::new(0);

        // Add flakes with different predicates
        let flakes = vec![
            make_flake(1, 3, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(1, 2, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        // PSOT should order by predicate first
        let psot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Psot).collect();
        assert_eq!(psot_ids.len(), 3);

        let p1 = novelty.get_flake(psot_ids[0]).p.namespace_code;
        let p2 = novelty.get_flake(psot_ids[1]).p.namespace_code;
        let p3 = novelty.get_flake(psot_ids[2]).p.namespace_code;

        assert!(p1 <= p2 && p2 <= p3);
    }

    #[test]
    fn test_opst_refs_only() {
        let mut novelty = Novelty::new(0);

        // Add mixed flakes - refs and non-refs
        let flakes = vec![
            make_flake(1, 1, 100, 1, true),     // not a ref
            make_ref_flake(2, 1, 10, 1),         // ref
            make_flake(3, 1, 200, 1, true),     // not a ref
            make_ref_flake(4, 1, 5, 1),          // ref
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        // OPST should only contain refs
        let opst_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Opst).collect();
        assert_eq!(opst_ids.len(), 2);

        for id in opst_ids {
            assert!(novelty.get_flake(id).is_ref());
        }
    }

    #[test]
    fn test_slice_for_range_basic() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
            make_flake(3, 1, 100, 1, true),
            make_flake(4, 1, 100, 1, true),
            make_flake(5, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        // Full range (leftmost, no rhs)
        let slice = novelty.slice_for_range(IndexType::Spot, None, None, true);
        assert_eq!(slice.len(), 5);

        // From subject 2 (exclusive) to end
        let first = make_flake(2, 1, 100, 1, true);
        let slice = novelty.slice_for_range(IndexType::Spot, Some(&first), None, false);
        // Should get subjects 3, 4, 5 (> 2)
        assert_eq!(slice.len(), 3);
    }

    #[test]
    fn test_slice_for_range_with_rhs() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
            make_flake(3, 1, 100, 1, true),
            make_flake(4, 1, 100, 1, true),
            make_flake(5, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1).unwrap();

        // From leftmost to subject 3 (inclusive)
        let rhs = make_flake(3, 1, 100, 1, true);
        let slice = novelty.slice_for_range(IndexType::Spot, None, Some(&rhs), true);
        // Should get subjects 1, 2, 3 (<= 3)
        assert_eq!(slice.len(), 3);
    }

    #[test]
    fn test_clear_up_to() {
        let mut novelty = Novelty::new(0);

        // Add flakes at different times
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(2, 1, 100, 2, true)], 2)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(3, 1, 100, 3, true)], 3)
            .unwrap();

        let initial_size = novelty.size;
        let initial_epoch = novelty.epoch;

        // Clear up to t=1 (should remove flake at t=1)
        novelty.clear_up_to(1);

        // Should have 2 flakes in spot index (t=2 and t=3)
        let remaining: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(remaining.len(), 2);

        // Size should be reduced
        assert!(novelty.size < initial_size);

        // Epoch should be bumped
        assert_eq!(novelty.epoch, initial_epoch + 1);
    }

    #[test]
    fn test_merge_preserves_order() {
        let mut novelty = Novelty::new(0);

        // First batch
        novelty
            .apply_commit(
                vec![
                    make_flake(1, 1, 100, 1, true),
                    make_flake(3, 1, 100, 1, true),
                    make_flake(5, 1, 100, 1, true),
                ],
                1,
            )
            .unwrap();

        // Second batch - interleaved subjects
        novelty
            .apply_commit(
                vec![
                    make_flake(2, 1, 100, 2, true),
                    make_flake(4, 1, 100, 2, true),
                ],
                2,
            )
            .unwrap();

        // Check SPOT ordering
        let spot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(spot_ids.len(), 5);

        // Verify sorted order
        for i in 0..spot_ids.len() - 1 {
            let cmp = IndexType::Spot.compare(
                novelty.get_flake(spot_ids[i]),
                novelty.get_flake(spot_ids[i + 1]),
            );
            assert_ne!(cmp, Ordering::Greater, "SPOT index not sorted at position {}", i);
        }
    }

    #[test]
    fn test_flake_store() {
        let mut store = FlakeStore::new();
        assert!(store.is_empty());

        let f1 = make_flake(1, 1, 100, 1, true);
        let id1 = store.push(f1);
        assert_eq!(id1, 0);
        assert_eq!(store.len(), 1);

        let f2 = make_flake(2, 1, 200, 1, true);
        let id2 = store.push(f2);
        assert_eq!(id2, 1);
        assert_eq!(store.len(), 2);

        assert_eq!(store.get(0).s.namespace_code, 1);
        assert_eq!(store.get(1).s.namespace_code, 2);
    }
}
