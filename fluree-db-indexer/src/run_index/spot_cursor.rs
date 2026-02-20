//! SpotCursor: leaf-at-a-time Flake iterator for the Phase C SPOT index.
//!
//! Iterates through leaves from a BranchManifest, decoding leaflets and
//! filtering rows by subject (and optionally predicate). Yields batches
//! of Flakes — one batch per leaf file.
//!
//! **Snapshot-only semantics:** The Phase C merge deduplicates to latest-t,
//! so the index is inherently a latest-state snapshot. SpotCursor does not
//! support time-travel (`to_t` filtering).

use super::binary_index_store::GraphView;
use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::run_record::RunSortOrder;
use super::types::DecodedRow;
use fluree_db_core::Flake;
use fluree_db_core::ListIndex;
use std::io;
use std::ops::Range;

/// A cursor that yields batches of Flakes from the Phase C SPOT index.
///
/// Created by `SpotCursor::for_subject(...)`. Each call to `next_leaf()`
/// reads one leaf file, decodes its leaflets, filters matching rows,
/// converts them to Flakes, and returns the batch. Returns `None` when
/// all relevant leaves have been exhausted.
pub struct SpotCursor {
    graph_view: GraphView,
    /// Indices of leaves to visit (from BranchManifest::find_leaves_for_subject).
    leaf_range: Range<usize>,
    /// Current position within leaf_range.
    current_idx: usize,
    /// Filter: subject s_id.
    filter_s_id: u64,
    /// Filter: predicate p_id (None = all predicates).
    filter_p_id: Option<u32>,
    exhausted: bool,
}

impl SpotCursor {
    /// Create a cursor for a subject-bound query (snapshot semantics).
    ///
    /// The cursor will iterate through all leaves that may contain records
    /// for the given subject in the specified graph.
    pub fn for_subject(graph_view: GraphView, s_id: u64, p_id: Option<u32>) -> Self {
        let g_id = graph_view.g_id();
        let leaf_range = graph_view
            .store()
            .branch(g_id)
            .map(|b| b.find_leaves_for_subject(s_id))
            .unwrap_or(0..0);

        let current_idx = leaf_range.start;
        let exhausted = leaf_range.is_empty();

        Self {
            graph_view,
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

            let g_id = self.graph_view.g_id();
            let branch = match self.graph_view.store().branch(g_id) {
                Some(b) => b,
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            };

            let leaf_entry = &branch.leaves[leaf_idx];

            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("leaf {} has no resolved path", leaf_entry.leaf_cid),
                )
            })?;
            let leaf_data = std::fs::read(leaf_path)?;
            let header = read_leaf_header(&leaf_data)?;

            let mut flakes = Vec::new();

            for dir_entry in &header.leaflet_dir {
                let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                if end > leaf_data.len() {
                    break;
                }
                let leaflet_bytes = &leaf_data[dir_entry.offset as usize..end];
                let decoded = decode_leaflet(
                    leaflet_bytes,
                    header.p_width,
                    header.dt_width,
                    RunSortOrder::Spot,
                )?;

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

                    let decoded_row = DecodedRow {
                        s_id: row_s_id,
                        p_id: row_p_id,
                        o_kind: decoded.o_kinds[row],
                        o_key: decoded.o_keys[row],
                        dt: decoded.dt_values[row],
                        t: decoded.t_values[row] as i64,
                        lang_id: decoded.lang.as_ref().map_or(0, |c| c.get(row as u16)),
                        i: decoded
                            .i_col
                            .as_ref()
                            .map_or(ListIndex::none().as_i32(), |c| c.get(row as u16)),
                    };
                    let flake = self.graph_view.row_to_flake(&decoded_row)?;
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
