//! SpotCursor: leaf-at-a-time Flake iterator for the Phase C SPOT index.
//!
//! Iterates through leaves from a BranchManifest, decoding leaflets and
//! filtering rows by subject (and optionally predicate). Yields batches
//! of Flakes — one batch per leaf file.
//!
//! **Snapshot-only semantics:** The Phase C merge deduplicates to latest-t,
//! so the index is inherently a latest-state snapshot. SpotCursor does not
//! support time-travel (`to_t` filtering).

use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::run_record::RunSortOrder;
use super::spot_store::BinaryIndexStore;
use fluree_db_core::Flake;
use std::io;
use std::ops::Range;
use std::sync::Arc;

/// A cursor that yields batches of Flakes from the Phase C SPOT index.
///
/// Created by `SpotCursor::for_subject(...)`. Each call to `next_leaf()`
/// reads one leaf file, decodes its leaflets, filters matching rows,
/// converts them to Flakes, and returns the batch. Returns `None` when
/// all relevant leaves have been exhausted.
pub struct SpotCursor {
    store: Arc<BinaryIndexStore>,
    g_id: u32,
    /// Indices of leaves to visit (from BranchManifest::find_leaves_for_subject).
    leaf_range: Range<usize>,
    /// Current position within leaf_range.
    current_idx: usize,
    /// Filter: subject s_id.
    filter_s_id: u32,
    /// Filter: predicate p_id (None = all predicates).
    filter_p_id: Option<u32>,
    exhausted: bool,
}

impl SpotCursor {
    /// Create a cursor for a subject-bound query (snapshot semantics).
    ///
    /// The cursor will iterate through all leaves that may contain records
    /// for the given subject in the specified graph.
    pub fn for_subject(
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        s_id: u32,
        p_id: Option<u32>,
    ) -> Self {
        let leaf_range = store
            .branch(g_id)
            .map(|b| b.find_leaves_for_subject(g_id, s_id))
            .unwrap_or(0..0);

        let current_idx = leaf_range.start;
        let exhausted = leaf_range.is_empty();

        Self {
            store,
            g_id,
            leaf_range,
            current_idx,
            filter_s_id: s_id,
            filter_p_id: p_id,
            exhausted,
        }
    }

    /// Yield the next batch of Flakes (one leaf's worth).
    ///
    /// Returns `Ok(None)` when exhausted. Each batch contains all matching
    /// rows from one leaf file, already in SPOT order.
    pub fn next_leaf(&mut self) -> io::Result<Option<Vec<Flake>>> {
        if self.exhausted {
            return Ok(None);
        }

        while self.current_idx < self.leaf_range.end {
            let leaf_idx = self.current_idx;
            self.current_idx += 1;

            let branch = match self.store.branch(self.g_id) {
                Some(b) => b,
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            };

            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_dir = match self.store.leaf_dir(self.g_id) {
                Some(d) => d,
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            };

            let leaf_path = leaf_dir.join(&leaf_entry.path);
            let leaf_data = std::fs::read(&leaf_path)?;
            let header = read_leaf_header(&leaf_data)?;

            let mut flakes = Vec::new();

            for dir_entry in &header.leaflet_dir {
                let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                if end > leaf_data.len() {
                    break;
                }
                let leaflet_bytes = &leaf_data[dir_entry.offset as usize..end];
                let decoded = decode_leaflet(leaflet_bytes, header.p_width, header.dt_width, RunSortOrder::Spot)?;

                for row in 0..decoded.row_count {
                    let row_s_id = decoded.s_ids[row];
                    if row_s_id != self.filter_s_id {
                        continue;
                    }
                    let row_p_id = decoded.p_ids[row];
                    if let Some(filter_p) = self.filter_p_id {
                        if row_p_id != filter_p {
                            continue;
                        }
                    }

                    let flake = self.store.row_to_flake(
                        row_s_id,
                        row_p_id,
                        decoded.o_kinds[row],
                        decoded.o_keys[row],
                        decoded.dt_values[row],
                        decoded.t_values[row],
                        decoded.lang_ids[row],
                        decoded.i_values[row],
                    )?;
                    flakes.push(flake);
                }
            }

            if !flakes.is_empty() {
                return Ok(Some(flakes));
            }
            // Empty leaf (subject not in this leaf after all) — continue to next
        }

        self.exhausted = true;
        Ok(None)
    }

    /// Check if the cursor is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Collect all remaining Flakes from the cursor.
    pub fn collect_all(&mut self) -> io::Result<Vec<Flake>> {
        let mut all = Vec::new();
        while let Some(batch) = self.next_leaf()? {
            all.extend(batch);
        }
        Ok(all)
    }
}
