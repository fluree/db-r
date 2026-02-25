//! BinaryCursor: sort-order-generic leaf iterator for binary columnar indexes.
//!
//! Iterates through leaves from a BranchManifest using any sort order,
//! decoding leaflets and applying columnar filters. Yields batches of
//! decoded rows — one batch per leaf file.
//!
//! **Key design principles:**
//! - Stays in integer-ID space (s_id, p_id, o_kind/o_key) — no Flake conversion
//! - Lazy Region 2: decode dt/t/lang/i only when needed and only for matches
//! - Uses memory-mapped leaf files for OS-managed caching
//! - Query-time overlay merge: when overlay ops are set, merges in-memory
//!   novelty into decoded leaflet rows at read time

use super::binary_index_store::BinaryIndexStore;
use super::replay::replay_leaflet;
use crate::format::branch::LeafEntry;
use crate::format::leaf::read_leaf_header;
use crate::format::leaflet::{
    decode_leaflet_region1, decode_leaflet_region2, decode_leaflet_region3, LeafletHeader,
};
use crate::format::run_record::{cmp_for_order, FactKey, RunRecord, RunSortOrder};
use crate::read::leaflet_cache::{
    CachedRegion1, CachedRegion2, LeafletCacheKey, SparseIColumn, SparseU16Column,
};
use crate::types::{OverlayOp, RowColumnSlice};
use fluree_db_core::subject_id::{SubjectId, SubjectIdColumn};
use fluree_db_core::GraphId;
use fluree_db_core::ListIndex;
use memmap2::Mmap;
use std::cmp::Ordering;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// Sort-order comparison helpers for overlay merge
// ============================================================================

/// Compare a decoded row (from R1+R2) against an OverlayOp using the given
/// sort order. Only compares identity columns (s_id, p_id, o_kind, o_key, dt) — not t/op.
///
/// `dt_row` is the R2 dt value (u32), truncated to u16 for comparison.
/// This matches the truncation pattern in `novelty_merge::cmp_row_vs_record`.
#[inline]
fn cmp_row_vs_overlay(
    s_id: u64,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt_row: u16,
    ov: &OverlayOp,
    order: RunSortOrder,
) -> Ordering {
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&ov.s_id)
            .then(p_id.cmp(&ov.p_id))
            .then(o_kind.cmp(&ov.o_kind))
            .then(o_key.cmp(&ov.o_key))
            .then(dt_row.cmp(&ov.dt)),
        RunSortOrder::Psot => p_id
            .cmp(&ov.p_id)
            .then(s_id.cmp(&ov.s_id))
            .then(o_kind.cmp(&ov.o_kind))
            .then(o_key.cmp(&ov.o_key))
            .then(dt_row.cmp(&ov.dt)),
        RunSortOrder::Post => p_id
            .cmp(&ov.p_id)
            .then(o_kind.cmp(&ov.o_kind))
            .then(o_key.cmp(&ov.o_key))
            .then(dt_row.cmp(&ov.dt))
            .then(s_id.cmp(&ov.s_id)),
        RunSortOrder::Opst => o_kind
            .cmp(&ov.o_kind)
            .then(o_key.cmp(&ov.o_key))
            .then(dt_row.cmp(&ov.dt))
            .then(p_id.cmp(&ov.p_id))
            .then(s_id.cmp(&ov.s_id)),
    }
}

/// Compare an OverlayOp against a RunRecord (branch manifest first/last key)
/// using the given sort order. Only compares identity columns.
///
/// Used for binary-searching overlay ops within a leaf's key range.
#[inline]
fn cmp_overlay_vs_record(ov: &OverlayOp, rec: &RunRecord, order: RunSortOrder) -> Ordering {
    let ov_sid = SubjectId::from_u64(ov.s_id);
    match order {
        RunSortOrder::Spot => ov_sid
            .cmp(&rec.s_id)
            .then(ov.p_id.cmp(&rec.p_id))
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt)),
        RunSortOrder::Psot => ov
            .p_id
            .cmp(&rec.p_id)
            .then(ov_sid.cmp(&rec.s_id))
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt)),
        RunSortOrder::Post => ov
            .p_id
            .cmp(&rec.p_id)
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt))
            .then(ov_sid.cmp(&rec.s_id)),
        RunSortOrder::Opst => ov
            .o_kind
            .cmp(&rec.o_kind)
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt))
            .then(ov.p_id.cmp(&rec.p_id))
            .then(ov_sid.cmp(&rec.s_id)),
    }
}

/// Build a `FactKey` from an `OverlayOp`.
#[inline]
fn fact_key_from_overlay(ov: &OverlayOp) -> FactKey {
    FactKey::from_decoded_row(
        ov.s_id,
        ov.p_id,
        ov.o_kind,
        ov.o_key,
        ov.dt as u32,
        ov.lang_id,
        ov.i_val,
    )
}

// ============================================================================
// merge_overlay: ephemeral overlay merge into decoded columns
// ============================================================================

struct OverlayMergeOut {
    s: Vec<u64>,
    p: Vec<u32>,
    o_kinds: Vec<u8>,
    o_keys: Vec<u64>,
    dt: Vec<u32>,
    t: Vec<u32>,
    lang: Vec<u16>,
    i: Vec<i32>,
}

impl OverlayMergeOut {
    fn with_capacity(cap: usize) -> Self {
        Self {
            s: Vec::with_capacity(cap),
            p: Vec::with_capacity(cap),
            o_kinds: Vec::with_capacity(cap),
            o_keys: Vec::with_capacity(cap),
            dt: Vec::with_capacity(cap),
            t: Vec::with_capacity(cap),
            lang: Vec::with_capacity(cap),
            i: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn push_row(&mut self, input: &RowColumnSlice<'_>, ri: usize) {
        self.s.push(input.s[ri]);
        self.p.push(input.p[ri]);
        self.o_kinds.push(input.o_kinds[ri]);
        self.o_keys.push(input.o_keys[ri]);
        self.dt.push(input.dt[ri]);
        self.t.push(input.t[ri]);
        self.lang.push(input.lang[ri]);
        self.i.push(input.i[ri]);
    }

    #[inline]
    fn push_overlay(&mut self, ov: &OverlayOp) {
        debug_assert!(
            ov.t >= 0 && ov.t <= u32::MAX as i64,
            "merge_overlay: overlay t={} out of u32 range",
            ov.t
        );
        self.s.push(ov.s_id);
        self.p.push(ov.p_id);
        self.o_kinds.push(ov.o_kind);
        self.o_keys.push(ov.o_key);
        self.dt.push(ov.dt as u32);
        self.t.push(ov.t as u32);
        self.lang.push(ov.lang_id);
        self.i.push(ov.i_val);
    }

    fn row_count(&self) -> usize {
        self.s.len()
    }
}

/// Merge overlay ops into decoded leaflet columns (ephemeral, no R3 output).
///
/// Walks the decoded R1+R2 cursor and overlay cursor together in sort order:
///
/// - **Row < overlay**: Emit row unchanged.
/// - **Overlay < row**: Assert → emit overlay; Retract → skip (non-existent).
/// - **Same identity**: Assert → emit overlay (update); Retract → omit row.
/// - **Same sort position, different identity**: Emit row, retry overlay.
///
/// Returns merged (CachedRegion1, CachedRegion2).
fn merge_overlay(
    input: &RowColumnSlice<'_>,
    overlay: &[OverlayOp],
    order: RunSortOrder,
) -> (CachedRegion1, CachedRegion2) {
    let row_count = input.len();
    let cap = row_count + overlay.len();

    let mut out = OverlayMergeOut::with_capacity(cap);

    let mut ri = 0usize; // row cursor
    let mut oi = 0usize; // overlay cursor

    while ri < row_count && oi < overlay.len() {
        let ov = &overlay[oi];
        let s_id = input.s[ri];
        let p_id = input.p[ri];
        let o_kind = input.o_kinds[ri];
        let o_key = input.o_keys[ri];
        let dt = input.dt[ri];
        let lang_id = input.lang[ri];
        let i = input.i[ri];
        let cmp = cmp_row_vs_overlay(s_id, p_id, o_kind, o_key, dt as u16, ov, order);

        match cmp {
            Ordering::Less => {
                out.push_row(input, ri);
                ri += 1;
            }
            Ordering::Greater => {
                if ov.op {
                    out.push_overlay(ov);
                }
                oi += 1;
            }
            Ordering::Equal => {
                let row_key = FactKey::from_decoded_row(s_id, p_id, o_kind, o_key, dt, lang_id, i);
                if row_key == fact_key_from_overlay(ov) {
                    if ov.op {
                        out.push_overlay(ov);
                    }
                    ri += 1;
                    oi += 1;
                } else {
                    out.push_row(input, ri);
                    ri += 1;
                }
            }
        }
    }

    // Drain remaining rows
    while ri < row_count {
        out.push_row(input, ri);
        ri += 1;
    }

    // Drain remaining overlay asserts
    while oi < overlay.len() {
        if overlay[oi].op {
            out.push_overlay(&overlay[oi]);
        }
        oi += 1;
    }

    let merged_count = out.row_count();
    let r1 = CachedRegion1 {
        s_ids: SubjectIdColumn::from_wide(out.s.into_iter().map(SubjectId::from_u64).collect()),
        p_ids: out.p.into(),
        o_kinds: out.o_kinds.into(),
        o_keys: out.o_keys.into(),
        row_count: merged_count,
    };
    let r2 = CachedRegion2 {
        dt_values: out.dt.into(),
        t_values: out.t.into(),
        lang: sparse_lang_from_dense(&out.lang),
        i_col: sparse_i_from_dense(&out.i),
    };
    (r1, r2)
}

/// Build a sparse lang column from a dense u16 vec. Returns `None` if all values are 0.
fn sparse_lang_from_dense(dense: &[u16]) -> Option<SparseU16Column> {
    debug_assert!(
        dense.len() <= u16::MAX as usize,
        "sparse lang positions overflow: len={}",
        dense.len()
    );
    let mut positions = Vec::new();
    let mut values = Vec::new();
    for (i, &v) in dense.iter().enumerate() {
        if v != 0 {
            positions.push(i as u16);
            values.push(v);
        }
    }
    if positions.is_empty() {
        None
    } else {
        Some(SparseU16Column {
            positions: positions.into(),
            values: values.into(),
        })
    }
}

/// Build a sparse i column from a dense i32 vec. Returns `None` if all values are `ListIndex::none()`.
fn sparse_i_from_dense(dense: &[i32]) -> Option<SparseIColumn> {
    debug_assert!(
        dense.len() <= u16::MAX as usize,
        "sparse i positions overflow: len={}",
        dense.len()
    );
    let sentinel = ListIndex::none().as_i32();
    let mut positions = Vec::new();
    let mut values = Vec::new();
    for (i, &v) in dense.iter().enumerate() {
        if v != sentinel {
            positions.push(i as u16);
            values.push(v as u32);
        }
    }
    if positions.is_empty() {
        None
    } else {
        Some(SparseIColumn::U32 {
            positions: positions.into(),
            values: values.into(),
        })
    }
}

// ============================================================================
// find_overlay_for_leaf: binary search overlay ops for a leaf's key range
// ============================================================================

/// Find the slice of overlay ops that fall within a leaf's key range.
///
/// Uses the branch manifest's `first_key`/`last_key` to determine bounds.
/// For the last leaf in the range, `next_first_key` is `None` (extends to +∞).
fn find_overlay_for_leaf<'a>(
    overlay_ops: &'a [OverlayOp],
    leaf_entry: &LeafEntry,
    next_first_key: Option<&RunRecord>,
    order: RunSortOrder,
) -> &'a [OverlayOp] {
    if overlay_ops.is_empty() {
        return &[];
    }

    // Start: first overlay op >= leaf's first_key
    let start = overlay_ops.partition_point(|ov| {
        cmp_overlay_vs_record(ov, &leaf_entry.first_key, order) == Ordering::Less
    });

    // End: first overlay op >= next leaf's first_key (or all remaining)
    let end = match next_first_key {
        Some(next_key) => overlay_ops
            .partition_point(|ov| cmp_overlay_vs_record(ov, next_key, order) == Ordering::Less),
        None => overlay_ops.len(),
    };

    &overlay_ops[start..end]
}

// ============================================================================
// BinaryFilter: columnar filter on integer IDs
// ============================================================================

/// Filter criteria for binary index scans, using integer IDs.
///
/// Applied directly to decoded columnar arrays for fast filtering
/// without any string/IRI resolution.
#[derive(Debug, Clone)]
pub struct BinaryFilter {
    /// Subject filter (exact match).
    pub s_id: Option<u64>,
    /// Predicate filter (exact match).
    pub p_id: Option<u32>,
    /// Object kind filter (exact match on ObjKind discriminant).
    pub o_kind: Option<u8>,
    /// Object key filter (exact match on ObjKey payload).
    pub o_key: Option<u64>,
}

impl BinaryFilter {
    /// Create an empty filter (matches everything).
    pub fn new() -> Self {
        Self {
            s_id: None,
            p_id: None,
            o_kind: None,
            o_key: None,
        }
    }
}

impl Default for BinaryFilter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// DecodedBatch: columnar output from cursor
// ============================================================================

/// A batch of decoded rows from one leaf, in columnar layout.
///
/// Region 1 columns (s_id, p_id, o) are always populated.
/// Region 2 columns (dt, t, lang, i) are populated for matching rows.
#[derive(Debug)]
pub struct DecodedBatch {
    pub s_ids: Vec<u64>,
    pub p_ids: Vec<u32>,
    pub o_kinds: Vec<u8>,
    pub o_keys: Vec<u64>,
    pub dt_values: Vec<u32>,
    pub t_values: Vec<i64>,
    pub lang_ids: Vec<u16>,
    pub i_values: Vec<i32>,
    pub row_count: usize,
}

// ============================================================================
// BinaryCursor
// ============================================================================

/// Sort-order-generic cursor that iterates through leaves from a BranchManifest.
///
/// Created via `BinaryCursor::new()`. Each call to `next_leaf()` reads one leaf
/// file, decodes its leaflets, applies columnar filters, and returns matching
/// rows as a `DecodedBatch`. Returns `None` when all relevant leaves are exhausted.
///
/// **Overlay merge:** When overlay ops are set via `set_overlay_ops()`, the
/// cursor merges in-memory novelty operations into decoded leaflet rows at
/// read time. Overlay ops that cannot be translated to integer IDs should
/// cause the cursor to return an error. There is no legacy index fallback.
pub struct BinaryCursor {
    store: Arc<BinaryIndexStore>,
    order: RunSortOrder,
    g_id: GraphId,
    /// Indices of leaves to visit.
    leaf_indices: Range<usize>,
    /// Current position within leaf_indices.
    current_idx: usize,
    /// Columnar filter for fast row filtering.
    filter: BinaryFilter,
    /// Whether the caller needs Region 2 metadata columns (dt/t/lang/i).
    ///
    /// When false, the cursor returns only Region 1 columns (s_id/p_id/o) and
    /// leaves Region 2 vectors empty in the returned `DecodedBatch`.
    need_region2: bool,
    /// Effective "state-at" t for cache keying. Defaults to `store.max_t()`.
    to_t: i64,
    /// Overlay epoch for cache keying. 0 when no overlay is active.
    epoch: u64,
    /// Overlay ops translated to integer-ID space, sorted by this cursor's
    /// sort order. Empty when no overlay is active.
    overlay_ops: Vec<OverlayOp>,
    exhausted: bool,
}

impl BinaryCursor {
    /// Create a new cursor for a given sort order, graph, and key range.
    ///
    /// Uses `translate_range` output (min_key, max_key) to find relevant leaves
    /// via `find_leaves_in_range`. The filter is applied during leaflet decoding.
    ///
    /// If the graph has no index (e.g., empty default graph), returns an
    /// immediately-exhausted cursor.
    pub fn new(
        store: Arc<BinaryIndexStore>,
        order: RunSortOrder,
        g_id: GraphId,
        min_key: &RunRecord,
        max_key: &RunRecord,
        filter: BinaryFilter,
        need_region2: bool,
    ) -> Self {
        let to_t = store.max_t();

        // Handle missing graph index gracefully (e.g., empty default graph)
        let Some(branch) = store.branch_for_order(g_id, order) else {
            tracing::debug!(
                g_id = g_id,
                order = order.dir_name(),
                "BinaryCursor: no index branch for graph, returning exhausted cursor"
            );
            return Self {
                store,
                order,
                g_id,
                leaf_indices: 0..0,
                current_idx: 0,
                filter,
                need_region2,
                to_t,
                epoch: 0,
                overlay_ops: Vec::new(),
                exhausted: true,
            };
        };

        let cmp = cmp_for_order(order);
        let leaf_indices = branch.find_leaves_in_range(min_key, max_key, cmp);

        let current_idx = leaf_indices.start;
        // Don't set exhausted = true even when leaf_indices is empty.
        // The overlay-only path in next_leaf() needs a chance to run for
        // subjects that exist only in novelty (not in any indexed leaf).
        let exhausted = false;

        Self {
            store,
            order,
            g_id,
            leaf_indices,
            current_idx,
            filter,
            need_region2,
            to_t,
            epoch: 0,
            overlay_ops: Vec::new(),
            exhausted,
        }
    }

    /// Create a cursor for a SPOT subject lookup (convenience).
    ///
    /// Uses the existing `find_leaves_for_subject` optimization on the SPOT branch.
    ///
    /// If the graph has no index (e.g., empty default graph), returns an
    /// immediately-exhausted cursor.
    pub fn for_subject(
        store: Arc<BinaryIndexStore>,
        g_id: GraphId,
        s_id: u64,
        p_id: Option<u32>,
        need_region2: bool,
    ) -> Self {
        let to_t = store.max_t();

        // Handle missing graph index gracefully (e.g., empty default graph)
        let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Spot) else {
            tracing::debug!(
                g_id = g_id,
                s_id = s_id,
                "BinaryCursor::for_subject: no SPOT index for graph, returning exhausted cursor"
            );
            return Self {
                store,
                order: RunSortOrder::Spot,
                g_id,
                leaf_indices: 0..0,
                current_idx: 0,
                filter: BinaryFilter {
                    s_id: Some(s_id),
                    p_id,
                    o_kind: None,
                    o_key: None,
                },
                need_region2,
                to_t,
                epoch: 0,
                overlay_ops: Vec::new(),
                exhausted: true,
            };
        };

        let leaf_indices = branch.find_leaves_for_subject(s_id);

        let current_idx = leaf_indices.start;
        // Don't set exhausted = true even when leaf_indices is empty.
        // The overlay-only path in next_leaf() needs a chance to run for
        // subjects that exist only in novelty (not in any indexed leaf).
        let exhausted = false;

        Self {
            store,
            order: RunSortOrder::Spot,
            g_id,
            leaf_indices,
            current_idx,
            filter: BinaryFilter {
                s_id: Some(s_id),
                p_id,
                o_kind: None,
                o_key: None,
            },
            need_region2,
            to_t,
            epoch: 0,
            overlay_ops: Vec::new(),
            exhausted,
        }
    }

    /// Set the effective "state-at" t for time-travel queries.
    ///
    /// When `to_t < store.max_t()`, the cursor activates time-travel replay:
    /// Region 3 (history journal) is decoded and `replay_leaflet()` is used to
    /// reconstruct the state at `to_t`. Region 2 is always decoded during
    /// time-travel (needed for replay even if the caller doesn't bind metadata).
    ///
    /// If a leaflet has no Region 3 history and `to_t < max_t`, the cursor
    /// returns an error — the caller must handle the uncovered time range.
    pub fn set_to_t(&mut self, to_t: i64) {
        self.to_t = to_t;
    }

    /// Set the overlay epoch for cache keying.
    ///
    /// Distinguishes staged vs committed views at the same `t`.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Set overlay operations for query-time merge.
    ///
    /// The ops must already be sorted by this cursor's sort order.
    /// When set, the cursor merges these ops into decoded leaflet rows.
    pub fn set_overlay_ops(&mut self, ops: Vec<OverlayOp>) {
        self.overlay_ops = ops;
    }

    /// Whether this cursor is performing time-travel replay.
    fn is_time_traveling(&self) -> bool {
        self.to_t < self.store.max_t()
    }

    /// Whether this cursor has overlay ops to merge.
    fn has_overlay(&self) -> bool {
        !self.overlay_ops.is_empty()
    }

    fn overlay_matches_filter(&self, ov: &OverlayOp) -> bool {
        if let Some(f_s_id) = self.filter.s_id {
            if ov.s_id != f_s_id {
                return false;
            }
        }
        if let Some(f_p_id) = self.filter.p_id {
            if ov.p_id != f_p_id {
                return false;
            }
        }
        if let Some(f_o_kind) = self.filter.o_kind {
            if ov.o_kind != f_o_kind {
                return false;
            }
        }
        if let Some(f_o_key) = self.filter.o_key {
            if ov.o_key != f_o_key {
                return false;
            }
        }
        true
    }

    /// Yield the next batch of decoded rows (one leaf's worth).
    ///
    /// Returns `Ok(None)` when exhausted. Each batch contains all matching
    /// rows from one leaf file, already in the cursor's sort order.
    ///
    /// **Lazy Region 2:** Region 1 (s_id, p_id, o) is decoded and filtered first.
    /// Region 2 (dt, t, lang, i) is only decoded for the full leaflet when at least
    /// one row matches. This avoids decompressing metadata for entirely non-matching
    /// leaflets.
    ///
    /// **Time-travel replay:** When `to_t < store.max_t()`, Region 3 is decoded
    /// and `replay_leaflet()` reconstructs the state at the target time. Filters
    /// are applied to the replayed state. If Region 3 is empty (no history
    /// available), returns an error so the caller can fall back.
    ///
    /// **Overlay merge:** When overlay ops are set, the cursor merges them into
    /// decoded leaflet rows per-leaf. Leaflets with no overlapping overlay ops
    /// use the normal (lazy R2) path. Leaflets with overlay ops force R2 decode
    /// and use a streaming merge.
    pub fn next_leaf(&mut self) -> io::Result<Option<DecodedBatch>> {
        tracing::trace!(
            order = ?self.order,
            leaf_indices = ?self.leaf_indices,
            overlay_ops = self.overlay_ops.len(),
            to_t = self.to_t,
            "binary_cursor::next_leaf"
        );
        if self.exhausted {
            return Ok(None);
        }

        let span = tracing::trace_span!(
            "binary_cursor_next_leaf",
            order = self.order.dir_name(),
            g_id = self.g_id,
            to_t = self.to_t,
            overlay_epoch = self.epoch,
            overlay_ops = self.overlay_ops.len(),
            ms = tracing::field::Empty,
            leaflets = tracing::field::Empty,
            r1_hits = tracing::field::Empty,
            r1_misses = tracing::field::Empty,
            r2_hits = tracing::field::Empty,
            r2_misses = tracing::field::Empty
        );
        let _g = span.enter();
        let start = Instant::now();

        let branch = self
            .store
            .branch_for_order(self.g_id, self.order)
            .expect("all four index orders must exist for every graph");

        let time_traveling = self.is_time_traveling();
        let has_overlay = self.has_overlay();

        while self.current_idx < self.leaf_indices.end {
            let leaf_idx = self.current_idx;
            self.current_idx += 1;

            let leaf_entry = &branch.leaves[leaf_idx];

            // Determine overlay ops for this leaf (if any).
            let leaf_overlay = if has_overlay {
                let next_key = if leaf_idx + 1 < branch.leaves.len() {
                    Some(&branch.leaves[leaf_idx + 1].first_key)
                } else {
                    None
                };
                let overlay_slice =
                    find_overlay_for_leaf(&self.overlay_ops, leaf_entry, next_key, self.order);
                tracing::trace!(
                    leaf_idx,
                    leaf_first_key = ?leaf_entry.first_key.s_id,
                    overlay_slice_len = overlay_slice.len(),
                    "binary_cursor: overlay slice for leaf"
                );
                overlay_slice
            } else {
                &[]
            };

            // Memory-map leaf file.
            //
            // For remote CAS stores (e.g., S3), leaf files may be downloaded lazily.
            // If the file is missing, attempt to fetch it into the cache dir and retry.
            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("leaf {} has no resolved path", leaf_entry.leaf_cid),
                )
            })?;
            let file = match std::fs::File::open(leaf_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.store
                        .ensure_index_leaf_cached(&leaf_entry.leaf_cid, leaf_path)?;
                    std::fs::File::open(leaf_path)?
                }
                Err(e) => return Err(e),
            };
            let leaf_mmap = unsafe { Mmap::map(&file)? };
            let header = read_leaf_header(&leaf_mmap)?;

            let mut batch = DecodedBatch {
                s_ids: Vec::new(),
                p_ids: Vec::new(),
                o_kinds: Vec::new(),
                o_keys: Vec::new(),
                dt_values: Vec::new(),
                t_values: Vec::new(),
                lang_ids: Vec::new(),
                i_values: Vec::new(),
                row_count: 0,
            };

            // Compute leaf_id for cache key (xxh3_128 of CID binary).
            let leaf_id = xxhash_rust::xxh3::xxh3_128(&leaf_entry.leaf_cid.to_bytes());
            let cache = self.store.leaflet_cache();

            let mut leaflets: u64 = 0;
            let mut r1_hits: u64 = 0;
            let mut r1_misses: u64 = 0;
            let mut r2_hits: u64 = 0;
            let mut r2_misses: u64 = 0;

            if !leaf_overlay.is_empty() {
                tracing::trace!(
                    leaf_idx,
                    overlay_slice_len = leaf_overlay.len(),
                    "binary_cursor: processing leaf with overlay merge"
                );
                // Overlay merge path: merge overlay ops into leaflet rows.
                // Each leaflet gets its assigned slice of overlay ops;
                // merge_overlay handles insertions, updates, and retractions.
                self.process_leaf_with_overlay(
                    &leaf_mmap,
                    &header,
                    leaf_id,
                    cache,
                    leaf_overlay,
                    &mut batch,
                )?;

                // Clear overlay ops after processing the last leaf to prevent
                // the overlay-only path from re-emitting them on subsequent calls.
                if self.current_idx >= self.leaf_indices.end {
                    self.overlay_ops.clear();
                }
            } else {
                // No overlay for this leaf — use per-leaflet normal/time-travel paths.
                for (leaflet_idx, dir_entry) in header.leaflet_dir.iter().enumerate() {
                    leaflets += 1;
                    let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                    if end > leaf_mmap.len() {
                        break;
                    }
                    let leaflet_bytes = &leaf_mmap[dir_entry.offset as usize..end];

                    // Build cache key for this leaflet.
                    let cache_key = LeafletCacheKey {
                        leaf_id,
                        leaflet_index: leaflet_idx as u8,
                        to_t: self.to_t,
                        epoch: self.epoch,
                    };

                    if let Some(c) = cache {
                        if c.contains_r1(&cache_key) {
                            r1_hits += 1;
                        } else {
                            r1_misses += 1;
                        }
                        // Region 2 is only relevant when the cursor needs it (Region 2 requested or time-travel).
                        if self.need_region2 || time_traveling {
                            if c.contains_r2(&cache_key) {
                                r2_hits += 1;
                            } else {
                                r2_misses += 1;
                            }
                        }
                    }

                    if time_traveling {
                        self.process_leaflet_time_travel(
                            leaflet_bytes,
                            &header,
                            &cache_key,
                            cache,
                            &mut batch,
                        )?;
                    } else {
                        self.process_leaflet_normal(
                            leaflet_bytes,
                            &header,
                            &cache_key,
                            cache,
                            &mut batch,
                        )?;
                    }
                }
            }

            batch.row_count = batch.s_ids.len();
            if batch.row_count > 0 {
                span.record("leaflets", leaflets);
                span.record("r1_hits", r1_hits);
                span.record("r1_misses", r1_misses);
                span.record("r2_hits", r2_hits);
                span.record("r2_misses", r2_misses);
                span.record("ms", (start.elapsed().as_secs_f64() * 1000.0) as u64);
                return Ok(Some(batch));
            }
            // Empty after filtering — continue to next leaf
        }

        // Clear overlay ops after processing all indexed leaves.
        // This prevents the overlay-only path below from re-emitting ops that
        // were already merged into indexed data. The overlay-only path is for
        // subjects that exist ONLY in novelty (not in any indexed leaf).
        if !self.leaf_indices.is_empty() {
            self.overlay_ops.clear();
        }

        // Overlay-only path: when no leaves matched but we have overlay ops,
        // emit assertion ops directly. This handles novelty-only subjects that
        // don't exist in any indexed leaf.
        tracing::trace!(
            overlay_ops = self.overlay_ops.len(),
            filter_s_id = ?self.filter.s_id,
            filter_p_id = ?self.filter.p_id,
            "binary_cursor: overlay-only path check"
        );
        if !self.overlay_ops.is_empty() {
            let mut batch = DecodedBatch {
                s_ids: Vec::new(),
                p_ids: Vec::new(),
                o_kinds: Vec::new(),
                o_keys: Vec::new(),
                dt_values: Vec::new(),
                t_values: Vec::new(),
                lang_ids: Vec::new(),
                i_values: Vec::new(),
                row_count: 0,
            };

            // Emit assertion ops that match the cursor's filter and to_t bound.
            // Note: overlay ops are already filtered by graph during translation,
            // so we don't need to check g_id here.
            for ov in &self.overlay_ops {
                // Must be an assertion and within time bounds
                if !ov.op || ov.t > self.to_t {
                    continue;
                }

                if !self.overlay_matches_filter(ov) {
                    continue;
                }

                batch.s_ids.push(ov.s_id);
                batch.p_ids.push(ov.p_id);
                batch.o_kinds.push(ov.o_kind);
                batch.o_keys.push(ov.o_key);
                batch.dt_values.push(ov.dt as u32);
                batch.t_values.push(ov.t);
                batch.lang_ids.push(ov.lang_id);
                batch.i_values.push(ov.i_val);
            }
            batch.row_count = batch.s_ids.len();

            // Clear overlay ops to prevent re-emission on next call
            self.overlay_ops.clear();

            if batch.row_count > 0 {
                tracing::debug!(
                    rows = batch.row_count,
                    "binary_cursor: emitting overlay-only batch (no matching leaves)"
                );
                span.record("leaflets", 0u64);
                span.record("ms", (start.elapsed().as_secs_f64() * 1000.0) as u64);
                return Ok(Some(batch));
            }
        }

        self.exhausted = true;
        span.record("leaflets", 0u64);
        span.record("r1_hits", 0u64);
        span.record("r1_misses", 0u64);
        span.record("r2_hits", 0u64);
        span.record("r2_misses", 0u64);
        span.record("ms", (start.elapsed().as_secs_f64() * 1000.0) as u64);
        Ok(None)
    }

    /// Process a leaf with overlay merge.
    ///
    /// For each leaflet: decodes R1+R2, computes the slice of overlay ops
    /// assigned to it, merges them via `merge_overlay`, and emits filtered
    /// results.
    ///
    /// ## Overlay assignment
    ///
    /// Overlay ops are assigned to leaflets using exclusive upper bounds:
    ///
    /// - **Non-last leaflet**: gets ops where `sort_key <= last_row`. Ops
    ///   beyond this leaflet's last row are left for a subsequent leaflet
    ///   (typically the next) — `merge_overlay` inserts them before that
    ///   leaflet's first R1 row.
    ///
    /// - **Last leaflet**: gets ALL remaining ops. `merge_overlay` appends
    ///   any that sort after the last row.
    ///
    /// This eliminates separate orphan handling — `merge_overlay` naturally
    /// handles ops outside a leaflet's R1 row range as insertions.
    fn process_leaf_with_overlay(
        &self,
        leaf_mmap: &Mmap,
        header: &crate::format::leaf::LeafFileHeader,
        leaf_id: u128,
        cache: Option<&crate::read::leaflet_cache::LeafletCache>,
        leaf_overlay: &[OverlayOp],
        batch: &mut DecodedBatch,
    ) -> io::Result<()> {
        // `ov_start` tracks the first unconsumed overlay op. Advances
        // monotonically as leaflets are processed in sort order.
        let mut ov_start = 0usize;
        let leaflet_count = header.leaflet_dir.len();

        for (leaflet_idx, dir_entry) in header.leaflet_dir.iter().enumerate() {
            let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
            if end > leaf_mmap.len() {
                break;
            }
            let leaflet_bytes = &leaf_mmap[dir_entry.offset as usize..end];

            // Cache key for raw decode (epoch=0, to_t=max_t → base data).
            let raw_cache_key = LeafletCacheKey {
                leaf_id,
                leaflet_index: leaflet_idx as u8,
                to_t: self.store.max_t(),
                epoch: 0,
            };

            // Decode R1 (from cache or fresh).
            let (leaflet_header, r1) =
                self.decode_r1(leaflet_bytes, header, &raw_cache_key, cache)?;
            if r1.row_count == 0 {
                continue;
            }

            // Decode R2 (needed for sort-key comparison and merge output).
            let r2 = self.decode_r2(
                leaflet_bytes,
                leaflet_header.as_ref(),
                header,
                &raw_cache_key,
                cache,
            )?;

            // Compute exclusive upper bound for this leaflet's overlay slice.
            let ov_end = if leaflet_idx + 1 < leaflet_count {
                // Non-last leaflet: advance past ops whose sort key <= last_row.
                let last = r1.row_count - 1;
                let last_dt = r2.dt_values[last] as u16;
                let mut pos = ov_start;
                while pos < leaf_overlay.len() {
                    if cmp_row_vs_overlay(
                        r1.s_ids.get(last).as_u64(),
                        r1.p_ids[last],
                        r1.o_kinds[last],
                        r1.o_keys[last],
                        last_dt,
                        &leaf_overlay[pos],
                        self.order,
                    ) != Ordering::Less
                    {
                        pos += 1;
                    } else {
                        break;
                    }
                }
                pos
            } else {
                // Last leaflet: all remaining ops.
                leaf_overlay.len()
            };

            let local_overlay = &leaf_overlay[ov_start..ov_end];
            ov_start = ov_end;

            if local_overlay.is_empty() {
                // No overlay ops for this leaflet — filter and emit normally.
                let matches = self.filter_r1(&r1);
                if !matches.is_empty() {
                    self.emit_matched_rows(&r1, &r2, &matches, batch);
                }
            } else {
                // Merge overlay into this leaflet. merge_overlay handles all
                // positions: ops before first row (inserted), ops matching
                // existing rows (updated/retracted), ops after last row
                // (appended for last leaflet).
                let r1_s_u64: Vec<u64> = (0..r1.row_count)
                    .map(|i| r1.s_ids.get(i).as_u64())
                    .collect();
                // Expand CachedRegion2 sparse lang/i to dense vecs for RowColumnSlice.
                let lang_dense: Vec<u16> = (0..r1.row_count).map(|i| r2.lang_id(i)).collect();
                let i_dense: Vec<i32> = (0..r1.row_count).map(|i| r2.list_index(i)).collect();
                let input_slice = RowColumnSlice {
                    s: &r1_s_u64,
                    p: &r1.p_ids,
                    o_kinds: &r1.o_kinds,
                    o_keys: &r1.o_keys,
                    dt: &r2.dt_values,
                    t: &r2.t_values,
                    lang: &lang_dense,
                    i: &i_dense,
                };
                let (merged_r1, merged_r2) = merge_overlay(&input_slice, local_overlay, self.order);

                // Cache merged result at the overlay epoch key.
                let merged_key = LeafletCacheKey {
                    leaf_id,
                    leaflet_index: leaflet_idx as u8,
                    to_t: self.to_t,
                    epoch: self.epoch,
                };
                if let Some(c) = cache {
                    c.get_or_decode_r1(merged_key.clone(), || merged_r1.clone());
                    c.get_or_decode_r2(merged_key.clone(), || merged_r2.clone());
                }

                // Filter and emit.
                tracing::trace!(
                    row_count = merged_r1.row_count,
                    "binary_cursor: merge completed"
                );
                let matches = self.filter_r1(&merged_r1);
                tracing::trace!(
                    matches_len = matches.len(),
                    filter_s_id = ?self.filter.s_id,
                    filter_p_id = ?self.filter.p_id,
                    "binary_cursor: filter applied"
                );
                if !matches.is_empty() {
                    self.emit_matched_rows(&merged_r1, &merged_r2, &matches, batch);
                }
            }
        }

        // The last leaflet was assigned all remaining ops (ov_end = len()),
        // so nothing should remain unconsumed.
        debug_assert_eq!(
            ov_start,
            leaf_overlay.len(),
            "overlay ops remain after processing all leaflets"
        );

        Ok(())
    }

    /// Decode Region 1 from cache or fresh.
    fn decode_r1(
        &self,
        leaflet_bytes: &[u8],
        header: &crate::format::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&crate::read::leaflet_cache::LeafletCache>,
    ) -> io::Result<(Option<LeafletHeader>, CachedRegion1)> {
        if let Some(c) = cache {
            if let Some(cached) = c.get_r1(cache_key) {
                return Ok((None, cached));
            }
        }
        let (lh, s_ids, p_ids, o_kinds, o_keys) =
            decode_leaflet_region1(leaflet_bytes, header.p_width, self.order)?;
        let row_count = lh.row_count as usize;
        let r1 = CachedRegion1 {
            s_ids: SubjectIdColumn::from_wide(s_ids.into_iter().map(SubjectId::from_u64).collect()),
            p_ids: p_ids.into(),
            o_kinds: o_kinds.into(),
            o_keys: o_keys.into(),
            row_count,
        };
        if let Some(c) = cache {
            c.get_or_decode_r1(cache_key.clone(), || r1.clone());
        }
        Ok((Some(lh), r1))
    }

    /// Decode Region 2 from cache or fresh.
    fn decode_r2(
        &self,
        leaflet_bytes: &[u8],
        leaflet_header: Option<&LeafletHeader>,
        header: &crate::format::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&crate::read::leaflet_cache::LeafletCache>,
    ) -> io::Result<CachedRegion2> {
        if let Some(c) = cache {
            if let Some(cached) = c.get_r2(cache_key) {
                return Ok(cached);
            }
        }
        let lh = match leaflet_header {
            Some(h) => h.clone(),
            None => LeafletHeader::read_from(leaflet_bytes)?,
        };
        let decoded = decode_leaflet_region2(leaflet_bytes, &lh, header.dt_width)?;
        let r2 = CachedRegion2 {
            dt_values: decoded.dt_values.into(),
            t_values: decoded.t_values.into(),
            lang: decoded.lang,
            i_col: decoded.i_col,
        };
        if let Some(c) = cache {
            c.get_or_decode_r2(cache_key.clone(), || r2.clone());
        }
        Ok(r2)
    }

    /// Emit matched rows from R1+R2 into a batch (respects need_region2).
    fn emit_matched_rows(
        &self,
        r1: &CachedRegion1,
        r2: &CachedRegion2,
        matches: &[usize],
        batch: &mut DecodedBatch,
    ) {
        if self.need_region2 {
            for &row in matches {
                batch.s_ids.push(r1.s_ids.get(row).as_u64());
                batch.p_ids.push(r1.p_ids[row]);
                batch.o_kinds.push(r1.o_kinds[row]);
                batch.o_keys.push(r1.o_keys[row]);
                batch.dt_values.push(r2.dt_values[row]);
                batch.t_values.push(r2.t_i64(row));
                batch.lang_ids.push(r2.lang_id(row));
                batch.i_values.push(r2.list_index(row));
            }
        } else {
            for &row in matches {
                batch.s_ids.push(r1.s_ids.get(row).as_u64());
                batch.p_ids.push(r1.p_ids[row]);
                batch.o_kinds.push(r1.o_kinds[row]);
                batch.o_keys.push(r1.o_keys[row]);
            }
        }
    }

    /// Process a single leaflet in normal (non-time-travel) mode.
    ///
    /// Decodes Region 1 (cached), applies columnar filters, and lazily decodes
    /// Region 2 only when matches exist and the caller needs metadata.
    fn process_leaflet_normal(
        &self,
        leaflet_bytes: &[u8],
        header: &crate::format::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&crate::read::leaflet_cache::LeafletCache>,
        batch: &mut DecodedBatch,
    ) -> io::Result<()> {
        // Decode (or retrieve from cache) Region 1.
        let (leaflet_header, r1) = self.decode_r1(leaflet_bytes, header, cache_key, cache)?;

        // Collect matching row indices using only Region 1 arrays.
        let matches = self.filter_r1(&r1);
        if matches.is_empty() {
            return Ok(());
        }

        // Decode (or retrieve from cache) Region 2 if needed.
        if self.need_region2 {
            let r2 = self.decode_r2(
                leaflet_bytes,
                leaflet_header.as_ref(),
                header,
                cache_key,
                cache,
            )?;
            self.emit_matched_rows(&r1, &r2, &matches, batch);
        } else {
            for &row in &matches {
                batch.s_ids.push(r1.s_ids.get(row).as_u64());
                batch.p_ids.push(r1.p_ids[row]);
                batch.o_kinds.push(r1.o_kinds[row]);
                batch.o_keys.push(r1.o_keys[row]);
            }
        }

        Ok(())
    }

    /// Process a single leaflet in time-travel mode.
    ///
    /// Decodes R1+R2+R3, replays to reconstruct state at `self.to_t`, caches
    /// the replayed result, and applies filters on the replayed columns.
    ///
    /// If Region 3 is empty (no history available), validates that no row
    /// has t > to_t (defense-in-depth), then uses the current state as-is.
    /// This is safe when the ScanOperator guard ensures
    /// `to_t >= store.base_t()` — an unmodified leaflet's rows all have
    /// t <= base_t <= to_t.
    fn process_leaflet_time_travel(
        &self,
        leaflet_bytes: &[u8],
        header: &crate::format::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&crate::read::leaflet_cache::LeafletCache>,
        batch: &mut DecodedBatch,
    ) -> io::Result<()> {
        // Step 1: Check if replayed R1 is already cached at this to_t.
        if let Some(c) = cache {
            if let Some(r1) = c.get_r1(cache_key) {
                // Cache hit — replayed R1 at this to_t is available.
                let matches = self.filter_r1(&r1);
                if matches.is_empty() {
                    return Ok(());
                }
                if self.need_region2 {
                    if let Some(r2) = c.get_r2(cache_key) {
                        for &row in &matches {
                            batch.s_ids.push(r1.s_ids.get(row).as_u64());
                            batch.p_ids.push(r1.p_ids[row]);
                            batch.o_kinds.push(r1.o_kinds[row]);
                            batch.o_keys.push(r1.o_keys[row]);
                            batch.dt_values.push(r2.dt_values[row]);
                            batch.t_values.push(r2.t_i64(row));
                            batch.lang_ids.push(r2.lang_id(row));
                            batch.i_values.push(r2.list_index(row));
                        }
                        return Ok(());
                    }
                    // R1 cached but R2 not — fall through to decode
                } else {
                    for &row in &matches {
                        batch.s_ids.push(r1.s_ids.get(row).as_u64());
                        batch.p_ids.push(r1.p_ids[row]);
                        batch.o_kinds.push(r1.o_kinds[row]);
                        batch.o_keys.push(r1.o_keys[row]);
                    }
                    return Ok(());
                }
            }
        }

        // Step 2: Cache miss — decode raw R1, R2, R3 from bytes.
        let (lh, s_ids, p_ids, o_kinds, o_keys) =
            decode_leaflet_region1(leaflet_bytes, header.p_width, self.order)?;
        let decoded_r2 = decode_leaflet_region2(leaflet_bytes, &lh, header.dt_width)?;
        let r3_entries = decode_leaflet_region3(leaflet_bytes, &lh)?;

        // Expand sparse lang/i to dense temporaries for RowColumnSlice.
        let row_count_raw = lh.row_count as usize;
        let lang_dense: Vec<u16> = (0..row_count_raw)
            .map(|i| decoded_r2.lang.as_ref().map_or(0, |c| c.get(i as u16)))
            .collect();
        let i_dense: Vec<i32> = (0..row_count_raw)
            .map(|i| {
                decoded_r2
                    .i_col
                    .as_ref()
                    .map_or(ListIndex::none().as_i32(), |c| c.get(i as u16))
            })
            .collect();

        // Step 3: Replay (or pass through if R3 is empty).
        let replay_input = RowColumnSlice {
            s: &s_ids,
            p: &p_ids,
            o_kinds: &o_kinds,
            o_keys: &o_keys,
            dt: &decoded_r2.dt_values,
            t: &decoded_r2.t_values,
            lang: &lang_dense,
            i: &i_dense,
        };
        let (eff_r1, eff_r2) =
            match replay_leaflet(&replay_input, &r3_entries, self.to_t, self.order) {
                Some(replayed) => {
                    // Replay produced new state
                    let r1 = CachedRegion1 {
                        s_ids: SubjectIdColumn::from_wide(
                            replayed
                                .s_ids
                                .into_iter()
                                .map(SubjectId::from_u64)
                                .collect(),
                        ),
                        p_ids: replayed.p_ids.into(),
                        o_kinds: replayed.o_kinds.into(),
                        o_keys: replayed.o_keys.into(),
                        row_count: replayed.row_count,
                    };
                    let r2 = CachedRegion2 {
                        dt_values: replayed.dt_values.into(),
                        t_values: replayed.t_values.into(),
                        lang: sparse_lang_from_dense(&replayed.lang_ids),
                        i_col: sparse_i_from_dense(&replayed.i_values),
                    };
                    (r1, r2)
                }
                None => {
                    // No R3 entries after t_target — current state is valid.
                    //
                    // Defense-in-depth: verify that no row has t > to_t.
                    // An empty R3 legitimately means the leaflet wasn't modified
                    // since base_t. But if any row's t exceeds to_t, the leaflet
                    // WAS modified without a corresponding R3 entry — a data
                    // integrity issue that would produce incorrect time-travel results.
                    if decoded_r2.t_values.iter().any(|&t| (t as i64) > self.to_t) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "time-travel to t={} but leaflet contains rows with t > to_t \
                             and empty Region 3 history",
                                self.to_t,
                            ),
                        ));
                    }
                    let row_count = lh.row_count as usize;
                    let r1 = CachedRegion1 {
                        s_ids: SubjectIdColumn::from_wide(
                            s_ids.into_iter().map(SubjectId::from_u64).collect(),
                        ),
                        p_ids: p_ids.into(),
                        o_kinds: o_kinds.into(),
                        o_keys: o_keys.into(),
                        row_count,
                    };
                    let r2 = CachedRegion2 {
                        dt_values: decoded_r2.dt_values.into(),
                        t_values: decoded_r2.t_values.into(),
                        lang: decoded_r2.lang,
                        i_col: decoded_r2.i_col,
                    };
                    (r1, r2)
                }
            };

        // Step 4: Cache the (replayed or original) result.
        if let Some(c) = cache {
            c.get_or_decode_r1(cache_key.clone(), || eff_r1.clone());
            c.get_or_decode_r2(cache_key.clone(), || eff_r2.clone());
        }

        // Step 5: Filter and emit.
        let matches = self.filter_r1(&eff_r1);
        if matches.is_empty() {
            return Ok(());
        }

        if self.need_region2 {
            for &row in &matches {
                batch.s_ids.push(eff_r1.s_ids.get(row).as_u64());
                batch.p_ids.push(eff_r1.p_ids[row]);
                batch.o_kinds.push(eff_r1.o_kinds[row]);
                batch.o_keys.push(eff_r1.o_keys[row]);
                batch.dt_values.push(eff_r2.dt_values[row]);
                batch.t_values.push(eff_r2.t_i64(row));
                batch.lang_ids.push(eff_r2.lang_id(row));
                batch.i_values.push(eff_r2.list_index(row));
            }
        } else {
            for &row in &matches {
                batch.s_ids.push(eff_r1.s_ids.get(row).as_u64());
                batch.p_ids.push(eff_r1.p_ids[row]);
                batch.o_kinds.push(eff_r1.o_kinds[row]);
                batch.o_keys.push(eff_r1.o_keys[row]);
            }
        }

        Ok(())
    }

    /// Apply columnar filter on Region 1 arrays, returning matching row indices.
    fn filter_r1(&self, r1: &CachedRegion1) -> Vec<usize> {
        let mut matches = Vec::with_capacity(64);
        let filter_sid = self.filter.s_id.map(SubjectId::from_u64);
        for row in 0..r1.row_count {
            if let Some(filter_s) = filter_sid {
                if r1.s_ids.get(row) != filter_s {
                    continue;
                }
            }
            if let Some(filter_p) = self.filter.p_id {
                if r1.p_ids[row] != filter_p {
                    continue;
                }
            }
            if let Some(filter_o_kind) = self.filter.o_kind {
                if r1.o_kinds[row] != filter_o_kind {
                    continue;
                }
            }
            if let Some(filter_o_key) = self.filter.o_key {
                if r1.o_keys[row] != filter_o_key {
                    continue;
                }
            }
            matches.push(row);
        }
        matches
    }

    /// Check if the cursor is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Collect all remaining batches from the cursor into a single batch.
    pub fn collect_all(&mut self) -> io::Result<DecodedBatch> {
        let mut all = DecodedBatch {
            s_ids: Vec::new(),
            p_ids: Vec::new(),
            o_kinds: Vec::new(),
            o_keys: Vec::new(),
            dt_values: Vec::new(),
            t_values: Vec::new(),
            lang_ids: Vec::new(),
            i_values: Vec::new(),
            row_count: 0,
        };

        while let Some(batch) = self.next_leaf()? {
            all.s_ids.extend_from_slice(&batch.s_ids);
            all.p_ids.extend_from_slice(&batch.p_ids);
            all.o_kinds.extend_from_slice(&batch.o_kinds);
            all.o_keys.extend_from_slice(&batch.o_keys);
            all.dt_values.extend_from_slice(&batch.dt_values);
            all.t_values.extend_from_slice(&batch.t_values);
            all.lang_ids.extend_from_slice(&batch.lang_ids);
            all.i_values.extend_from_slice(&batch.i_values);
            all.row_count += batch.row_count;
        }

        Ok(all)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::{DatatypeDictId, ListIndex};

    /// Collect SubjectIdColumn values as u64 for test assertions.
    fn sid_col_to_u64(col: &SubjectIdColumn) -> Vec<u64> {
        (0..col.len()).map(|i| col.get(i).as_u64()).collect()
    }

    fn make_overlay_op(s_id: u64, p_id: u32, val: i64, t: i64, assert: bool) -> OverlayOp {
        OverlayOp {
            s_id,
            p_id,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(val).as_u64(),
            t,
            op: assert,
            dt: DatatypeDictId::INTEGER.as_u16(),
            lang_id: 0,
            i_val: ListIndex::none().as_i32(),
        }
    }

    /// Test helper: owns decoded R1+R2 columns and provides slice access.
    struct TestColumns {
        s: Vec<u64>,
        p: Vec<u32>,
        o_kinds: Vec<u8>,
        o_keys: Vec<u64>,
        dt: Vec<u32>,
        t: Vec<u32>,
        lang: Vec<u16>,
        i: Vec<i32>,
    }

    impl TestColumns {
        /// Create columns from simple (s, p, val, t) tuples.
        fn from_rows(rows: &[(u64, u32, i64, i64)]) -> Self {
            Self {
                s: rows.iter().map(|r| r.0).collect(),
                p: rows.iter().map(|r| r.1).collect(),
                o_kinds: vec![ObjKind::NUM_INT.as_u8(); rows.len()],
                o_keys: rows
                    .iter()
                    .map(|r| ObjKey::encode_i64(r.2).as_u64())
                    .collect(),
                dt: vec![DatatypeDictId::INTEGER.as_u16() as u32; rows.len()],
                t: rows.iter().map(|r| r.3 as u32).collect(),
                lang: vec![0; rows.len()],
                i: vec![ListIndex::none().as_i32(); rows.len()],
            }
        }

        /// Get a RowColumnSlice view of this data.
        fn as_slice(&self) -> RowColumnSlice<'_> {
            RowColumnSlice {
                s: &self.s,
                p: &self.p,
                o_kinds: &self.o_kinds,
                o_keys: &self.o_keys,
                dt: &self.dt,
                t: &self.t,
                lang: &self.lang,
                i: &self.i,
            }
        }
    }

    #[test]
    fn test_merge_overlay_empty() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1), (2, 1, 20, 1)]);
        let overlay: Vec<OverlayOp> = vec![];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 2);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 2]);
    }

    #[test]
    fn test_merge_overlay_assert_new_fact() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1), (3, 1, 30, 1)]);
        let overlay = vec![make_overlay_op(2, 1, 20, 5, true)];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 3);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 2, 3]);
    }

    #[test]
    fn test_merge_overlay_retract_existing() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1), (2, 1, 20, 1), (3, 1, 30, 1)]);
        let overlay = vec![make_overlay_op(2, 1, 20, 5, false)];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 2);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 3]);
    }

    #[test]
    fn test_merge_overlay_update_existing() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1)]);
        let overlay = vec![make_overlay_op(1, 1, 10, 5, true)];
        let (r1, r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 1);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1]);
        assert_eq!(r2.t_i64(0), 5); // updated t
    }

    #[test]
    fn test_merge_overlay_retract_nonexistent() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1)]);
        let overlay = vec![make_overlay_op(0, 1, 5, 5, false)]; // retract before s=1
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 1);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1]);
    }

    #[test]
    fn test_merge_overlay_append_after() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1)]);
        let overlay = vec![
            make_overlay_op(5, 1, 50, 5, true),
            make_overlay_op(6, 1, 60, 5, true),
        ];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 3);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 5, 6]);
    }

    #[test]
    fn test_merge_overlay_prepend_before() {
        let cols = TestColumns::from_rows(&[(5, 1, 50, 1)]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 5, true),
            make_overlay_op(2, 1, 20, 5, true),
        ];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 3);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 2, 5]);
    }

    #[test]
    fn test_merge_overlay_mixed_operations() {
        let cols =
            TestColumns::from_rows(&[(1, 1, 10, 1), (2, 1, 20, 1), (3, 1, 30, 1), (5, 1, 50, 1)]);
        let overlay = vec![
            make_overlay_op(2, 1, 20, 5, false), // retract s=2
            make_overlay_op(3, 1, 30, 5, true),  // update s=3
            make_overlay_op(4, 1, 40, 5, true),  // insert s=4
        ];
        let (r1, r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 4);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 3, 4, 5]);
        assert_eq!(r2.t_i64(1), 5); // s=3 updated
        assert_eq!(r2.t_i64(2), 5); // s=4 inserted
    }

    #[test]
    fn test_merge_overlay_into_empty() {
        let cols = TestColumns::from_rows(&[]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 1, true),
            make_overlay_op(2, 1, 20, 1, true),
        ];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 2);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 2]);
    }

    #[test]
    fn test_merge_overlay_retract_all() {
        let cols = TestColumns::from_rows(&[(1, 1, 10, 1), (2, 1, 20, 1)]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 5, false),
            make_overlay_op(2, 1, 20, 5, false),
        ];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 0);
    }

    #[test]
    fn test_merge_overlay_psot_order() {
        // PSOT: (p, s, o, dt) — p sorts first
        let cols = TestColumns::from_rows(&[
            (1, 1, 10, 1), // p=1, s=1
            (2, 1, 20, 1), // p=1, s=2
            (1, 2, 30, 1), // p=2, s=1
        ]);
        // Insert p=1,s=3 (goes after p=1,s=2 in PSOT order)
        let overlay = vec![OverlayOp {
            s_id: 3,
            p_id: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(25).as_u64(),
            t: 5,
            op: true,
            dt: DatatypeDictId::INTEGER.as_u16(),
            lang_id: 0,
            i_val: ListIndex::none().as_i32(),
        }];
        let (r1, _r2) = merge_overlay(&cols.as_slice(), &overlay, RunSortOrder::Psot);
        assert_eq!(r1.row_count, 4);
        // PSOT order: (p=1,s=1), (p=1,s=2), (p=1,s=3), (p=2,s=1)
        assert_eq!(&*r1.p_ids, &[1, 1, 1, 2]);
        assert_eq!(sid_col_to_u64(&r1.s_ids), &[1, 2, 3, 1]);
    }

    #[test]
    fn test_merge_overlay_same_sort_different_identity() {
        // Two facts with same (s, p, o_kind, o_key, dt) in sort position but different lang_id
        use fluree_db_core::DatatypeDictId;
        let o_kind_val = ObjKind::LEX_ID.as_u8();
        let o_key_val = ObjKey::encode_u32_id(5).as_u64();
        let cols = RowColumnSlice {
            s: &[1u64, 1],
            p: &[1u32, 1],
            o_kinds: &[o_kind_val, o_kind_val],
            o_keys: &[o_key_val, o_key_val],
            dt: &[
                DatatypeDictId::LANG_STRING.as_u16() as u32,
                DatatypeDictId::LANG_STRING.as_u16() as u32,
            ],
            t: &[1u32, 1],
            lang: &[1u16, 2], // different lang_ids
            i: &[ListIndex::none().as_i32(), ListIndex::none().as_i32()],
        };

        // Retract lang_id=1 version only
        let overlay = vec![OverlayOp {
            s_id: 1,
            p_id: 1,
            o_kind: o_kind_val,
            o_key: o_key_val,
            t: 5,
            op: false,
            dt: DatatypeDictId::LANG_STRING.as_u16(),
            lang_id: 1,
            i_val: ListIndex::none().as_i32(),
        }];
        let (r1, r2) = merge_overlay(&cols, &overlay, RunSortOrder::Spot);
        assert_eq!(r1.row_count, 1);
        assert_eq!(r2.lang_id(0), 2); // lang_id=2 survives
    }

    #[test]
    fn test_cmp_overlay_vs_record_spot() {
        let ov = make_overlay_op(5, 3, 10, 1, true);
        let rec = RunRecord::new(
            0,
            SubjectId::from_u64(5),
            3,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        assert_eq!(
            cmp_overlay_vs_record(&ov, &rec, RunSortOrder::Spot),
            Ordering::Equal
        );

        let rec2 = RunRecord::new(
            0,
            SubjectId::from_u64(6),
            3,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        assert_eq!(
            cmp_overlay_vs_record(&ov, &rec2, RunSortOrder::Spot),
            Ordering::Less
        );
    }
}
