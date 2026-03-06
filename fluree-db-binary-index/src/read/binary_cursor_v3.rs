//! V3 binary cursor: iterates FLI3 leaves and produces `ColumnBatch` output.
//!
//! Yields **one `ColumnBatch` per leaflet** (not per leaf). This avoids the
//! broken batch-concat problem entirely and matches V5's leaflet-at-a-time
//! iteration model.
//!
//! Overlay merge (Phase 5): two-pointer merge of indexed leaflet rows with
//! sorted `OverlayOpV3` values. The four merge cases match V5 semantics:
//! row-only, overlay-only, same-identity (replace/retract), different-identity.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use crate::format::branch_v3::BranchManifestV3;
use crate::format::leaf_v3::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, DecodedLeafDirV3,
};
use crate::format::run_record::RunSortOrder;
use crate::format::run_record_v2::cmp_v2_for_order;
use crate::read::types_v3::{cmp_row_vs_overlay_v3, OverlayOpV3};

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
    /// Overlay ops sorted by this cursor's sort order.
    overlay_ops: Vec<OverlayOpV3>,
    /// Start position in overlay_ops for the current leaf (set per-leaf via slicing).
    overlay_pos: usize,
    /// Exclusive end position in overlay_ops for the current leaf.
    /// Ops beyond this belong to a later leaf and must not be consumed.
    leaf_overlay_end: usize,
    /// Overlay epoch for cache key differentiation.
    epoch: u64,
    /// Time bound for overlay ops (only emit ops with t <= to_t).
    to_t: i64,
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
            // Don't mark exhausted when leaf_range is empty — overlay-only path
            // may still have ops to emit.
            exhausted: false,
            overlay_ops: Vec::new(),
            overlay_pos: 0,
            leaf_overlay_end: 0,
            epoch: 0,
            to_t: i64::MAX,
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
            exhausted: false,
            overlay_ops: Vec::new(),
            overlay_pos: 0,
            leaf_overlay_end: 0,
            epoch: 0,
            to_t: i64::MAX,
        }
    }

    /// Set overlay ops (must be pre-sorted by this cursor's sort order).
    pub fn set_overlay_ops(&mut self, ops: Vec<OverlayOpV3>) {
        let len = ops.len();
        self.overlay_ops = ops;
        self.overlay_pos = 0;
        self.leaf_overlay_end = len; // default: all ops visible (refined per-leaf)
    }

    /// Set the overlay epoch for cache key differentiation.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Set the time bound for overlay ops.
    pub fn set_to_t(&mut self, to_t: i64) {
        self.to_t = to_t;
    }

    /// Whether overlay ops remain for the current leaf.
    fn has_overlay(&self) -> bool {
        self.overlay_pos < self.leaf_overlay_end
    }

    /// Whether any overlay ops remain globally (for overlay-only path).
    fn has_any_overlay(&self) -> bool {
        self.overlay_pos < self.overlay_ops.len()
    }

    /// Slice overlay ops for the leaf at `leaf_idx` using branch manifest keys.
    /// Sets `overlay_pos` and `leaf_overlay_end` for this leaf.
    fn slice_overlay_for_leaf(&mut self, leaf_idx: usize) {
        use super::types_v3::cmp_overlay_vs_record_v3;
        use std::cmp::Ordering;

        let ops = &self.overlay_ops[self.overlay_pos..];
        if ops.is_empty() {
            self.leaf_overlay_end = self.overlay_pos;
            return;
        }

        let leaf_entry = &self.branch.leaves[leaf_idx];

        // Start: skip ops that sort before this leaf's first_key.
        let start_offset = ops.partition_point(|ov| {
            cmp_overlay_vs_record_v3(ov, &leaf_entry.first_key, self.order) == Ordering::Less
        });

        // End: find first op >= next leaf's first_key.
        let next_first = if leaf_idx + 1 < self.branch.leaves.len() {
            Some(&self.branch.leaves[leaf_idx + 1].first_key)
        } else {
            None
        };
        let end_offset = match next_first {
            Some(next_key) => ops.partition_point(|ov| {
                cmp_overlay_vs_record_v3(ov, next_key, self.order) == Ordering::Less
            }),
            None => ops.len(),
        };

        self.overlay_pos += start_offset;
        self.leaf_overlay_end = self.overlay_pos + (end_offset - start_offset);
    }

    /// Advance to the next non-empty leaflet and return its `ColumnBatch`.
    ///
    /// Returns `None` when all leaflets in all leaves are exhausted
    /// (and overlay-only ops have been emitted).
    pub fn next_batch(&mut self) -> io::Result<Option<ColumnBatch>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }

            // If we have an open leaf, try the next leaflet in it.
            // We take the leaf temporarily to avoid borrow conflicts with &mut self.
            if let Some(leaf) = self.current_leaf.take() {
                while self.current_leaflet_idx < leaf.dir.entries.len() {
                    let entry = &leaf.dir.entries[self.current_leaflet_idx];
                    self.current_leaflet_idx += 1;

                    // Pre-skip by directory metadata (only when no overlay —
                    // overlay merge may add rows to otherwise-skippable leaflets).
                    let has_ov = self.has_overlay();
                    if !has_ov
                        && self.filter.skip_leaflet(entry.p_const, entry.o_type_const)
                    {
                        continue;
                    }
                    if entry.row_count == 0 && !has_ov {
                        continue;
                    }

                    // Load columns.
                    let batch = if entry.row_count > 0 {
                        load_leaflet_columns(
                            &leaf.bytes,
                            entry,
                            leaf.dir.payload_base,
                            &self.projection,
                            self.order,
                        )?
                    } else {
                        ColumnBatch::empty()
                    };

                    // Apply row-level filter.
                    let batch = if self.filter.is_empty() || batch.is_empty() {
                        batch
                    } else {
                        filter_batch(&self.filter, &batch)
                    };

                    // Apply overlay merge if we have overlay ops.
                    let batch = if has_ov {
                        self.merge_overlay_into_batch(batch)
                    } else {
                        batch
                    };

                    if batch.is_empty() {
                        continue;
                    }

                    // Put the leaf back before returning.
                    self.current_leaf = Some(leaf);
                    return Ok(Some(batch));
                }
                // Exhausted all leaflets in this leaf — drop it (already taken).
            }

            // Open the next leaf.
            if self.current_leaf_idx >= self.leaf_range.end {
                // All indexed leaves exhausted. Try overlay-only path.
                // Reset leaf_overlay_end to cover all remaining ops.
                self.leaf_overlay_end = self.overlay_ops.len();
                if self.has_any_overlay() {
                    let batch = self.emit_overlay_only();
                    self.exhausted = true;
                    if !batch.is_empty() {
                        return Ok(Some(batch));
                    }
                }
                self.exhausted = true;
                return Ok(None);
            }

            let leaf_idx = self.current_leaf_idx;
            let leaf_cid = self.branch.leaves[leaf_idx].leaf_cid.clone();
            self.current_leaf_idx += 1;

            // Slice overlay ops for this leaf (binary search on branch keys).
            if !self.overlay_ops.is_empty() {
                self.slice_overlay_for_leaf(leaf_idx);
            }

            let bytes = self.store.get_leaf_bytes_sync(&leaf_cid)?;
            let header = decode_leaf_header_v3(&bytes)?;
            let dir = decode_leaf_dir_v3_with_base(&bytes, &header)?;

            self.current_leaf = Some(OpenLeaf { bytes, dir });
            self.current_leaflet_idx = 0;
        }
    }

    // ========================================================================
    // Overlay merge
    // ========================================================================

    /// Two-pointer merge of a base batch with overlay ops at the current position.
    ///
    /// Consumes overlay ops that fall within this batch's sort-order range.
    /// Returns a new batch with merged rows.
    fn merge_overlay_into_batch(&mut self, base: ColumnBatch) -> ColumnBatch {
        let order = self.order;
        let to_t = self.to_t;

        let mut out_s_id: Vec<u64> = Vec::new();
        let mut out_o_key: Vec<u64> = Vec::new();
        let mut out_p_id: Vec<u32> = Vec::new();
        let mut out_o_type: Vec<u16> = Vec::new();
        let mut out_o_i: Vec<u32> = Vec::new();
        let mut out_t: Vec<u32> = Vec::new();

        let mut ri = 0usize;
        let row_count = base.row_count;

        let ov_end = self.leaf_overlay_end;

        while ri < row_count || self.overlay_pos < ov_end {
            // Determine which side to advance.
            if ri >= row_count {
                // Rows exhausted — drain remaining overlay asserts for this leaf.
                if self.overlay_pos >= ov_end {
                    break;
                }
                let ov = &self.overlay_ops[self.overlay_pos];

                if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                    push_overlay_row(
                        ov,
                        &mut out_s_id,
                        &mut out_o_key,
                        &mut out_p_id,
                        &mut out_o_type,
                        &mut out_o_i,
                        &mut out_t,
                    );
                }
                self.overlay_pos += 1;
                continue;
            }

            if self.overlay_pos >= ov_end {
                // Overlay exhausted for this leaf — drain remaining rows.
                push_batch_row(
                    &base,
                    ri,
                    &mut out_s_id,
                    &mut out_o_key,
                    &mut out_p_id,
                    &mut out_o_type,
                    &mut out_o_i,
                    &mut out_t,
                );
                ri += 1;
                continue;
            }

            // Both sides have elements — compare.
            let ov = &self.overlay_ops[self.overlay_pos];
            let r_s = base.s_id.get(ri);
            let r_p = base.p_id.get_or(ri, 0);
            let r_ot = base.o_type.get_or(ri, 0);
            let r_ok = base.o_key.get(ri);
            let r_oi = base.o_i.get_or(ri, u32::MAX);

            match cmp_row_vs_overlay_v3(r_s, r_p, r_ot, r_ok, r_oi, ov, order) {
                std::cmp::Ordering::Less => {
                    // Row sorts before overlay → emit row.
                    push_batch_row(
                        &base,
                        ri,
                        &mut out_s_id,
                        &mut out_o_key,
                        &mut out_p_id,
                        &mut out_o_type,
                        &mut out_o_i,
                        &mut out_t,
                    );
                    ri += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Overlay sorts before row → emit assert, skip retract.
                    if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                        push_overlay_row(
                            ov,
                            &mut out_s_id,
                            &mut out_o_key,
                            &mut out_p_id,
                            &mut out_o_type,
                            &mut out_o_i,
                            &mut out_t,
                        );
                    }
                    self.overlay_pos += 1;
                }
                std::cmp::Ordering::Equal => {
                    // Same sort position — check full identity (all 5 fields).
                    let same_identity = r_s == ov.s_id
                        && r_p == ov.p_id
                        && r_ot == ov.o_type
                        && r_ok == ov.o_key
                        && r_oi == ov.o_i;

                    if same_identity {
                        // Same fact: assert → overlay replaces row; retract → omit row.
                        if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                            push_overlay_row(
                                ov,
                                &mut out_s_id,
                                &mut out_o_key,
                                &mut out_p_id,
                                &mut out_o_type,
                                &mut out_o_i,
                                &mut out_t,
                            );
                        }
                        // Both consumed.
                        ri += 1;
                        self.overlay_pos += 1;
                    } else {
                        // Sort-position tie but different identity — emit row, retry overlay.
                        push_batch_row(
                            &base,
                            ri,
                            &mut out_s_id,
                            &mut out_o_key,
                            &mut out_p_id,
                            &mut out_o_type,
                            &mut out_o_i,
                            &mut out_t,
                        );
                        ri += 1;
                    }
                }
            }
        }

        ColumnBatch {
            row_count: out_s_id.len(),
            s_id: ColumnData::Block(out_s_id.into()),
            o_key: ColumnData::Block(out_o_key.into()),
            p_id: ColumnData::Block(out_p_id.into()),
            o_type: ColumnData::Block(out_o_type.into()),
            o_i: ColumnData::Block(out_o_i.into()),
            t: ColumnData::Block(out_t.into()),
        }
    }

    /// Emit remaining overlay ops as a batch (overlay-only path).
    ///
    /// Called when all indexed leaves are exhausted but overlay ops remain.
    /// These represent facts that exist only in novelty (e.g., new subjects).
    fn emit_overlay_only(&mut self) -> ColumnBatch {
        let mut out_s_id: Vec<u64> = Vec::new();
        let mut out_o_key: Vec<u64> = Vec::new();
        let mut out_p_id: Vec<u32> = Vec::new();
        let mut out_o_type: Vec<u16> = Vec::new();
        let mut out_o_i: Vec<u32> = Vec::new();
        let mut out_t: Vec<u32> = Vec::new();

        while self.overlay_pos < self.overlay_ops.len() {
            let ov = &self.overlay_ops[self.overlay_pos];
            self.overlay_pos += 1;

            if !ov.op || ov.t > self.to_t {
                continue;
            }
            if !self.filter_overlay(ov) {
                continue;
            }

            push_overlay_row(
                ov,
                &mut out_s_id,
                &mut out_o_key,
                &mut out_p_id,
                &mut out_o_type,
                &mut out_o_i,
                &mut out_t,
            );
        }

        ColumnBatch {
            row_count: out_s_id.len(),
            s_id: ColumnData::Block(out_s_id.into()),
            o_key: ColumnData::Block(out_o_key.into()),
            p_id: ColumnData::Block(out_p_id.into()),
            o_type: ColumnData::Block(out_o_type.into()),
            o_i: ColumnData::Block(out_o_i.into()),
            t: ColumnData::Block(out_t.into()),
        }
    }

    /// Check if an overlay op passes the current filter.
    #[inline]
    fn filter_overlay(&self, ov: &OverlayOpV3) -> bool {
        self.filter
            .matches(ov.s_id, ov.p_id, ov.o_type, ov.o_key, ov.o_i)
    }
}

// ============================================================================
// Helpers
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn push_batch_row(
    batch: &ColumnBatch,
    i: usize,
    s_id: &mut Vec<u64>,
    o_key: &mut Vec<u64>,
    p_id: &mut Vec<u32>,
    o_type: &mut Vec<u16>,
    o_i: &mut Vec<u32>,
    t: &mut Vec<u32>,
) {
    s_id.push(batch.s_id.get(i));
    o_key.push(batch.o_key.get(i));
    p_id.push(batch.p_id.get_or(i, 0));
    o_type.push(batch.o_type.get_or(i, 0));
    o_i.push(batch.o_i.get_or(i, u32::MAX));
    t.push(batch.t.get_or(i, 0));
}

fn push_overlay_row(
    ov: &OverlayOpV3,
    s_id: &mut Vec<u64>,
    o_key: &mut Vec<u64>,
    p_id: &mut Vec<u32>,
    o_type: &mut Vec<u16>,
    o_i: &mut Vec<u32>,
    t: &mut Vec<u32>,
) {
    s_id.push(ov.s_id);
    o_key.push(ov.o_key);
    p_id.push(ov.p_id);
    o_type.push(ov.o_type);
    o_i.push(ov.o_i);
    t.push(ov.t as u32);
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
