//! V3 binary cursor: iterates FLI3 leaves and produces `ColumnBatch` output.
//!
//! Yields **one `ColumnBatch` per leaflet** (not per leaf). This avoids the
//! broken batch-concat problem entirely and matches V5's leaflet-at-a-time
//! iteration model.
//!
//! Overlay merge and time-travel replay are deferred to Phase 5/6.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use crate::format::branch_v3::BranchManifestV3;
use crate::format::leaf_v3::{decode_leaf_dir_v3_with_base, decode_leaf_header_v3, DecodedLeafDirV3};
use crate::format::run_record::RunSortOrder;
use crate::format::run_record_v2::cmp_v2_for_order;

use super::column_loader::load_leaflet_columns;
use super::column_types::{BinaryFilterV3, ColumnBatch, ColumnData, ColumnProjection};
use super::store_v6::BinaryIndexStoreV6;

// ============================================================================
// BinaryCursorV3
// ============================================================================

/// V3 columnar cursor: iterates leaflets across leaves in a branch manifest.
///
/// Yields one `ColumnBatch` per leaflet per `next_batch()` call.
/// Leaf bytes are fetched and decoded on demand when advancing to a new leaf.
pub struct BinaryCursorV3 {
    store: Arc<BinaryIndexStoreV6>,
    order: RunSortOrder,
    branch: Arc<BranchManifestV3>,
    leaf_range: Range<usize>,
    current_leaf_idx: usize,
    filter: BinaryFilterV3,
    projection: ColumnProjection,
    /// Decoded state for the currently-open leaf.
    current_leaf: Option<OpenLeaf>,
    /// Index of the next leaflet within the current leaf.
    current_leaflet_idx: usize,
    exhausted: bool,
}

/// Cached state for a leaf that's been fetched and decoded.
struct OpenLeaf {
    bytes: Vec<u8>,
    dir: DecodedLeafDirV3,
}

impl BinaryCursorV3 {
    /// Create a new cursor over a range of leaves in a branch manifest.
    pub fn new(
        store: Arc<BinaryIndexStoreV6>,
        order: RunSortOrder,
        branch: Arc<BranchManifestV3>,
        min_key: &crate::format::run_record_v2::RunRecordV2,
        max_key: &crate::format::run_record_v2::RunRecordV2,
        filter: BinaryFilterV3,
        projection: ColumnProjection,
    ) -> Self {
        let cmp = cmp_v2_for_order(order);
        let leaf_range = branch.find_leaves_in_range(min_key, max_key, cmp);
        Self {
            store,
            order,
            branch,
            leaf_range: leaf_range.clone(),
            current_leaf_idx: leaf_range.start,
            filter,
            projection,
            current_leaf: None,
            current_leaflet_idx: 0,
            exhausted: leaf_range.is_empty(),
        }
    }

    /// Create a cursor that scans ALL leaves in the branch.
    pub fn scan_all(
        store: Arc<BinaryIndexStoreV6>,
        order: RunSortOrder,
        branch: Arc<BranchManifestV3>,
        filter: BinaryFilterV3,
        projection: ColumnProjection,
    ) -> Self {
        let leaf_count = branch.leaves.len();
        Self {
            store,
            order,
            branch,
            leaf_range: 0..leaf_count,
            current_leaf_idx: 0,
            filter,
            projection,
            current_leaf: None,
            current_leaflet_idx: 0,
            exhausted: leaf_count == 0,
        }
    }

    /// Advance to the next non-empty leaflet and return its `ColumnBatch`.
    ///
    /// Returns `None` when all leaflets in all leaves are exhausted.
    pub fn next_batch(&mut self) -> io::Result<Option<ColumnBatch>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }

            // If we have an open leaf, try the next leaflet in it.
            if let Some(ref leaf) = self.current_leaf {
                while self.current_leaflet_idx < leaf.dir.entries.len() {
                    let entry = &leaf.dir.entries[self.current_leaflet_idx];
                    self.current_leaflet_idx += 1;

                    // Pre-skip by directory metadata.
                    if self.filter.skip_leaflet(entry.p_const, entry.o_type_const) {
                        continue;
                    }
                    if entry.row_count == 0 {
                        continue;
                    }

                    // Load columns.
                    let batch = load_leaflet_columns(
                        &leaf.bytes,
                        entry,
                        leaf.dir.payload_base,
                        &self.projection,
                        self.order,
                    )?;

                    // Apply row-level filter.
                    let batch = if self.filter.is_empty() {
                        batch
                    } else {
                        filter_batch(&self.filter, &batch)
                    };

                    if batch.is_empty() {
                        continue;
                    }

                    return Ok(Some(batch));
                }
                // Exhausted all leaflets in this leaf — drop it and advance.
                self.current_leaf = None;
            }

            // Open the next leaf.
            if self.current_leaf_idx >= self.leaf_range.end {
                self.exhausted = true;
                return Ok(None);
            }

            let leaf_entry = &self.branch.leaves[self.current_leaf_idx];
            self.current_leaf_idx += 1;

            let bytes = self.store.get_leaf_bytes_sync(&leaf_entry.leaf_cid)?;
            let header = decode_leaf_header_v3(&bytes)?;
            let dir = decode_leaf_dir_v3_with_base(&bytes, &header)?;

            self.current_leaf = Some(OpenLeaf { bytes, dir });
            self.current_leaflet_idx = 0;
        }
    }
}

// ============================================================================
// Filtering
// ============================================================================

/// Apply the filter to a batch, returning only matching rows.
/// Returns the batch unchanged if all rows match (avoids copy).
fn filter_batch(filter: &BinaryFilterV3, batch: &ColumnBatch) -> ColumnBatch {
    let mut matching: Vec<usize> = Vec::new();
    for i in 0..batch.row_count {
        let s_id = batch.s_id.get_or(i, 0);
        let p_id = batch.p_id.get_or(i, 0);
        let o_type = batch.o_type.get_or(i, 0);
        let o_key = batch.o_key.get_or(i, 0);
        let o_i = batch.o_i.get_or(i, u32::MAX);
        if filter.matches(s_id, p_id, o_type, o_key, o_i) {
            matching.push(i);
        }
    }

    if matching.len() == batch.row_count {
        return batch.clone();
    }

    gather_batch(batch, &matching)
}

/// Gather rows at the given indices from a batch into a new batch.
fn gather_batch(src: &ColumnBatch, indices: &[usize]) -> ColumnBatch {
    ColumnBatch {
        row_count: indices.len(),
        s_id: gather_column(&src.s_id, indices),
        o_key: gather_column(&src.o_key, indices),
        p_id: gather_column(&src.p_id, indices),
        o_type: gather_column(&src.o_type, indices),
        o_i: gather_column(&src.o_i, indices),
        t: gather_column(&src.t, indices),
    }
}

fn gather_column<T: Copy>(col: &ColumnData<T>, indices: &[usize]) -> ColumnData<T> {
    match col {
        ColumnData::Block(arr) => {
            let gathered: Vec<T> = indices.iter().map(|&i| arr[i]).collect();
            ColumnData::Block(gathered.into())
        }
        ColumnData::Const(v) => ColumnData::Const(*v),
        ColumnData::AbsentDefault => ColumnData::AbsentDefault,
    }
}
