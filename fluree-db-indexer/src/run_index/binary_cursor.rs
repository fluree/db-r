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

use super::branch::LeafEntry;
use super::leaf::read_leaf_header;
use super::leaflet::{
    decode_leaflet_region1, decode_leaflet_region2, decode_leaflet_region3, LeafletHeader,
};
use super::leaflet_cache::{CachedRegion1, CachedRegion2, LeafletCacheKey};
use super::replay::replay_leaflet;
use super::run_record::{cmp_for_order, FactKey, RunRecord, RunSortOrder};
use super::spot_store::BinaryIndexStore;
use fluree_db_core::value_id::ObjKind;
use memmap2::Mmap;
use std::cmp::Ordering;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// OverlayOp: overlay operation in integer-ID space
// ============================================================================

/// An overlay operation translated to integer-ID space.
///
/// Produced by translating `Flake` overlay ops via `BinaryIndexStore` reverse
/// lookups (`sid_to_s_id`, `sid_to_p_id`, `value_to_value_id`). Sorted by the
/// cursor's sort order for streaming merge with decoded leaflet rows.
///
/// Unlike `RunRecord`, this type is for ephemeral query-time merge only —
/// overlay ops are never persisted to disk.
#[derive(Debug, Clone, Copy)]
pub struct OverlayOp {
    pub s_id: u32,
    pub p_id: u32,
    /// Object kind discriminant (see `ObjKind`).
    pub o_kind: u8,
    /// Object key payload (interpretation depends on `o_kind`).
    pub o_key: u64,
    pub t: i64,
    /// true = assert, false = retract.
    pub op: bool,
    pub dt: u16,
    pub lang_id: u16,
    pub i_val: i32,
}

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
    s_id: u32,
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
    match order {
        RunSortOrder::Spot => ov
            .s_id
            .cmp(&rec.s_id)
            .then(ov.p_id.cmp(&rec.p_id))
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt)),
        RunSortOrder::Psot => ov
            .p_id
            .cmp(&rec.p_id)
            .then(ov.s_id.cmp(&rec.s_id))
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt)),
        RunSortOrder::Post => ov
            .p_id
            .cmp(&rec.p_id)
            .then(ov.o_kind.cmp(&rec.o_kind))
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt))
            .then(ov.s_id.cmp(&rec.s_id)),
        RunSortOrder::Opst => ov
            .o_kind
            .cmp(&rec.o_kind)
            .then(ov.o_key.cmp(&rec.o_key))
            .then(ov.dt.cmp(&rec.dt))
            .then(ov.p_id.cmp(&rec.p_id))
            .then(ov.s_id.cmp(&rec.s_id)),
    }
}

/// Check if a decoded row and an OverlayOp share the same fact identity.
///
/// Uses FactKey semantics: (s_id, p_id, o_kind, o_key, dt, effective_lang_id, i).
#[inline]
fn same_identity_row_vs_overlay(
    s_id: u32,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt: u32,
    lang_id: u16,
    i: i32,
    ov: &OverlayOp,
) -> bool {
    FactKey::from_decoded_row(s_id, p_id, o_kind, o_key, dt, lang_id, i)
        == FactKey::from_decoded_row(
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
    r1_s: &[u32],
    r1_p: &[u32],
    r1_o_kinds: &[u8],
    r1_o_keys: &[u64],
    r2_dt: &[u32],
    r2_t: &[i64],
    r2_lang: &[u16],
    r2_i: &[i32],
    overlay: &[OverlayOp],
    order: RunSortOrder,
) -> (CachedRegion1, CachedRegion2) {
    let row_count = r1_s.len();
    let cap = row_count + overlay.len();

    let mut out_s = Vec::with_capacity(cap);
    let mut out_p = Vec::with_capacity(cap);
    let mut out_o_kinds: Vec<u8> = Vec::with_capacity(cap);
    let mut out_o_keys: Vec<u64> = Vec::with_capacity(cap);
    let mut out_dt = Vec::with_capacity(cap);
    let mut out_t = Vec::with_capacity(cap);
    let mut out_lang = Vec::with_capacity(cap);
    let mut out_i = Vec::with_capacity(cap);

    let mut ri = 0usize; // row cursor
    let mut oi = 0usize; // overlay cursor

    while ri < row_count && oi < overlay.len() {
        let ov = &overlay[oi];
        let cmp = cmp_row_vs_overlay(
            r1_s[ri],
            r1_p[ri],
            r1_o_kinds[ri],
            r1_o_keys[ri],
            r2_dt[ri] as u16,
            ov,
            order,
        );

        match cmp {
            Ordering::Less => {
                // Row comes first — emit unchanged
                out_s.push(r1_s[ri]);
                out_p.push(r1_p[ri]);
                out_o_kinds.push(r1_o_kinds[ri]);
                out_o_keys.push(r1_o_keys[ri]);
                out_dt.push(r2_dt[ri]);
                out_t.push(r2_t[ri]);
                out_lang.push(r2_lang[ri]);
                out_i.push(r2_i[ri]);
                ri += 1;
            }
            Ordering::Greater => {
                // Overlay comes first (not in existing data)
                if ov.op {
                    emit_overlay(
                        ov,
                        &mut out_s,
                        &mut out_p,
                        &mut out_o_kinds,
                        &mut out_o_keys,
                        &mut out_dt,
                        &mut out_t,
                        &mut out_lang,
                        &mut out_i,
                    );
                }
                // Retract of non-existent → skip
                oi += 1;
            }
            Ordering::Equal => {
                // Sort-order position match — check full identity
                if same_identity_row_vs_overlay(
                    r1_s[ri],
                    r1_p[ri],
                    r1_o_kinds[ri],
                    r1_o_keys[ri],
                    r2_dt[ri],
                    r2_lang[ri],
                    r2_i[ri],
                    ov,
                ) {
                    // Same fact identity
                    if ov.op {
                        // Assert (update) — emit overlay with new t
                        emit_overlay(
                            ov,
                            &mut out_s,
                            &mut out_p,
                            &mut out_o_kinds,
                            &mut out_o_keys,
                            &mut out_dt,
                            &mut out_t,
                            &mut out_lang,
                            &mut out_i,
                        );
                    }
                    // else: Retract — omit from output
                    ri += 1;
                    oi += 1;
                } else {
                    // Same sort position but different identity (e.g., different lang_id/i).
                    // Emit the existing row and retry the overlay on the next iteration.
                    out_s.push(r1_s[ri]);
                    out_p.push(r1_p[ri]);
                    out_o_kinds.push(r1_o_kinds[ri]);
                    out_o_keys.push(r1_o_keys[ri]);
                    out_dt.push(r2_dt[ri]);
                    out_t.push(r2_t[ri]);
                    out_lang.push(r2_lang[ri]);
                    out_i.push(r2_i[ri]);
                    ri += 1;
                }
            }
        }
    }

    // Drain remaining rows
    while ri < row_count {
        out_s.push(r1_s[ri]);
        out_p.push(r1_p[ri]);
        out_o_kinds.push(r1_o_kinds[ri]);
        out_o_keys.push(r1_o_keys[ri]);
        out_dt.push(r2_dt[ri]);
        out_t.push(r2_t[ri]);
        out_lang.push(r2_lang[ri]);
        out_i.push(r2_i[ri]);
        ri += 1;
    }

    // Drain remaining overlay asserts
    while oi < overlay.len() {
        if overlay[oi].op {
            emit_overlay(
                &overlay[oi],
                &mut out_s,
                &mut out_p,
                &mut out_o_kinds,
                &mut out_o_keys,
                &mut out_dt,
                &mut out_t,
                &mut out_lang,
                &mut out_i,
            );
        }
        oi += 1;
    }

    let merged_count = out_s.len();
    let r1 = CachedRegion1 {
        s_ids: out_s.into(),
        p_ids: out_p.into(),
        o_kinds: out_o_kinds.into(),
        o_keys: out_o_keys.into(),
        row_count: merged_count,
    };
    let r2 = CachedRegion2 {
        dt_values: out_dt.into(),
        t_values: out_t.into(),
        lang_ids: out_lang.into(),
        i_values: out_i.into(),
    };
    (r1, r2)
}

/// Emit an OverlayOp to the output column vectors.
#[inline]
fn emit_overlay(
    ov: &OverlayOp,
    s: &mut Vec<u32>,
    p: &mut Vec<u32>,
    o_kinds: &mut Vec<u8>,
    o_keys: &mut Vec<u64>,
    dt: &mut Vec<u32>,
    t: &mut Vec<i64>,
    lang: &mut Vec<u16>,
    i: &mut Vec<i32>,
) {
    s.push(ov.s_id);
    p.push(ov.p_id);
    o_kinds.push(ov.o_kind);
    o_keys.push(ov.o_key);
    dt.push(ov.dt as u32);
    t.push(ov.t);
    lang.push(ov.lang_id);
    i.push(ov.i_val);
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
    pub s_id: Option<u32>,
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

// ============================================================================
// DecodedBatch: columnar output from cursor
// ============================================================================

/// A batch of decoded rows from one leaf, in columnar layout.
///
/// Region 1 columns (s_id, p_id, o) are always populated.
/// Region 2 columns (dt, t, lang, i) are populated for matching rows.
#[derive(Debug)]
pub struct DecodedBatch {
    pub s_ids: Vec<u32>,
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
/// cause the caller to fall back to the B-tree path.
pub struct BinaryCursor {
    store: Arc<BinaryIndexStore>,
    order: RunSortOrder,
    g_id: u32,
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
    pub fn new(
        store: Arc<BinaryIndexStore>,
        order: RunSortOrder,
        g_id: u32,
        min_key: &RunRecord,
        max_key: &RunRecord,
        filter: BinaryFilter,
        need_region2: bool,
    ) -> Self {
        let cmp = cmp_for_order(order);
        let branch = store
            .branch_for_order(g_id, order)
            .expect("all four index orders must exist for every graph");
        let leaf_indices = branch.find_leaves_in_range(min_key, max_key, cmp);

        let current_idx = leaf_indices.start;
        let exhausted = leaf_indices.is_empty();
        let to_t = store.max_t();

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
    pub fn for_subject(
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        s_id: u32,
        p_id: Option<u32>,
        need_region2: bool,
    ) -> Self {
        let branch = store
            .branch_for_order(g_id, RunSortOrder::Spot)
            .expect("SPOT index must exist for every graph");
        let leaf_indices = branch.find_leaves_for_subject(g_id, s_id);

        let current_idx = leaf_indices.start;
        let exhausted = leaf_indices.is_empty();
        let to_t = store.max_t();

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
    /// returns an error — the caller should fall back to the B-tree path.
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
        if self.exhausted {
            return Ok(None);
        }

        let span = tracing::info_span!(
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
                find_overlay_for_leaf(&self.overlay_ops, leaf_entry, next_key, self.order)
            } else {
                &[]
            };

            // Memory-map leaf file (leaf_entry.path is fully resolved by branch reader)
            let file = std::fs::File::open(&leaf_entry.path)?;
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

            // Compute leaf_id for cache key (xxh3_128 of content hash).
            let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.content_hash.as_bytes());
            let cache = self.store.leaflet_cache();

            let mut leaflets: u64 = 0;
            let mut r1_hits: u64 = 0;
            let mut r1_misses: u64 = 0;
            let mut r2_hits: u64 = 0;
            let mut r2_misses: u64 = 0;

            if !leaf_overlay.is_empty() {
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
                span.record("leaflets", &leaflets);
                span.record("r1_hits", &r1_hits);
                span.record("r1_misses", &r1_misses);
                span.record("r2_hits", &r2_hits);
                span.record("r2_misses", &r2_misses);
                span.record("ms", &((start.elapsed().as_secs_f64() * 1000.0) as u64));
                return Ok(Some(batch));
            }
            // Empty after filtering — continue to next leaf
        }

        self.exhausted = true;
        span.record("leaflets", &0u64);
        span.record("r1_hits", &0u64);
        span.record("r1_misses", &0u64);
        span.record("r2_hits", &0u64);
        span.record("r2_misses", &0u64);
        span.record("ms", &((start.elapsed().as_secs_f64() * 1000.0) as u64));
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
        header: &super::leaf::LeafFileHeader,
        leaf_id: u128,
        cache: Option<&super::leaflet_cache::LeafletCache>,
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
                        r1.s_ids[last],
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
                let (merged_r1, merged_r2) = merge_overlay(
                    &r1.s_ids,
                    &r1.p_ids,
                    &r1.o_kinds,
                    &r1.o_keys,
                    &r2.dt_values,
                    &r2.t_values,
                    &r2.lang_ids,
                    &r2.i_values,
                    local_overlay,
                    self.order,
                );

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
                let matches = self.filter_r1(&merged_r1);
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
        header: &super::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&super::leaflet_cache::LeafletCache>,
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
            s_ids: s_ids.into(),
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
        header: &super::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&super::leaflet_cache::LeafletCache>,
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
        let (dt, t, lang, i) = decode_leaflet_region2(leaflet_bytes, &lh, header.dt_width)?;
        let r2 = CachedRegion2 {
            dt_values: dt.into(),
            t_values: t.into(),
            lang_ids: lang.into(),
            i_values: i.into(),
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
                batch.s_ids.push(r1.s_ids[row]);
                batch.p_ids.push(r1.p_ids[row]);
                batch.o_kinds.push(r1.o_kinds[row]);
                batch.o_keys.push(r1.o_keys[row]);
                batch.dt_values.push(r2.dt_values[row]);
                batch.t_values.push(r2.t_values[row]);
                batch.lang_ids.push(r2.lang_ids[row]);
                batch.i_values.push(r2.i_values[row]);
            }
        } else {
            for &row in matches {
                batch.s_ids.push(r1.s_ids[row]);
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
        header: &super::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&super::leaflet_cache::LeafletCache>,
        batch: &mut DecodedBatch,
    ) -> io::Result<()> {
        // Decode (or retrieve from cache) Region 1.
        let (leaflet_header, r1) = if let Some(c) = cache {
            if let Some(cached) = c.get_r1(cache_key) {
                (None, cached)
            } else {
                let (lh, s_ids, p_ids, o_kinds, o_keys) =
                    decode_leaflet_region1(leaflet_bytes, header.p_width, self.order)?;
                let row_count = lh.row_count as usize;
                let cached_r1 = CachedRegion1 {
                    s_ids: s_ids.into(),
                    p_ids: p_ids.into(),
                    o_kinds: o_kinds.into(),
                    o_keys: o_keys.into(),
                    row_count,
                };
                c.get_or_decode_r1(cache_key.clone(), || cached_r1.clone());
                (Some(lh), cached_r1)
            }
        } else {
            let (lh, s_ids, p_ids, o_kinds, o_keys) =
                decode_leaflet_region1(leaflet_bytes, header.p_width, self.order)?;
            let row_count = lh.row_count as usize;
            let r1 = CachedRegion1 {
                s_ids: s_ids.into(),
                p_ids: p_ids.into(),
                o_kinds: o_kinds.into(),
                o_keys: o_keys.into(),
                row_count,
            };
            (Some(lh), r1)
        };

        // Collect matching row indices using only Region 1 arrays.
        let matches = self.filter_r1(&r1);
        if matches.is_empty() {
            return Ok(());
        }

        // Decode (or retrieve from cache) Region 2 if needed.
        if self.need_region2 {
            let r2 = if let Some(c) = cache {
                if let Some(cached) = c.get_r2(cache_key) {
                    cached
                } else {
                    let lh = match leaflet_header.as_ref() {
                        Some(h) => h.clone(),
                        None => LeafletHeader::read_from(leaflet_bytes)?,
                    };
                    let (dt, t, lang, i) =
                        decode_leaflet_region2(leaflet_bytes, &lh, header.dt_width)?;
                    let cached_r2 = CachedRegion2 {
                        dt_values: dt.into(),
                        t_values: t.into(),
                        lang_ids: lang.into(),
                        i_values: i.into(),
                    };
                    c.get_or_decode_r2(cache_key.clone(), || cached_r2.clone());
                    cached_r2
                }
            } else {
                let lh = leaflet_header.as_ref().unwrap();
                let (dt, t, lang, i) = decode_leaflet_region2(leaflet_bytes, lh, header.dt_width)?;
                CachedRegion2 {
                    dt_values: dt.into(),
                    t_values: t.into(),
                    lang_ids: lang.into(),
                    i_values: i.into(),
                }
            };

            for &row in &matches {
                batch.s_ids.push(r1.s_ids[row]);
                batch.p_ids.push(r1.p_ids[row]);
                batch.o_kinds.push(r1.o_kinds[row]);
                batch.o_keys.push(r1.o_keys[row]);
                batch.dt_values.push(r2.dt_values[row]);
                batch.t_values.push(r2.t_values[row]);
                batch.lang_ids.push(r2.lang_ids[row]);
                batch.i_values.push(r2.i_values[row]);
            }
        } else {
            for &row in &matches {
                batch.s_ids.push(r1.s_ids[row]);
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
        header: &super::leaf::LeafFileHeader,
        cache_key: &LeafletCacheKey,
        cache: Option<&super::leaflet_cache::LeafletCache>,
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
                            batch.s_ids.push(r1.s_ids[row]);
                            batch.p_ids.push(r1.p_ids[row]);
                            batch.o_kinds.push(r1.o_kinds[row]);
                            batch.o_keys.push(r1.o_keys[row]);
                            batch.dt_values.push(r2.dt_values[row]);
                            batch.t_values.push(r2.t_values[row]);
                            batch.lang_ids.push(r2.lang_ids[row]);
                            batch.i_values.push(r2.i_values[row]);
                        }
                        return Ok(());
                    }
                    // R1 cached but R2 not — fall through to decode
                } else {
                    for &row in &matches {
                        batch.s_ids.push(r1.s_ids[row]);
                        batch.p_ids.push(r1.p_ids[row]);
                        batch.o_kinds.push(r1.o_kinds[row]);
                        batch.o_keys.push(r1.o_keys[row]);
                    }
                    return Ok(());
                }
            }
        }

        // Step 2: Cache miss — decode raw R1, R2, R3 from bytes.
        let (lh, raw_s_ids, raw_p_ids, raw_o_kinds, raw_o_keys) =
            decode_leaflet_region1(leaflet_bytes, header.p_width, self.order)?;
        let (raw_dt, raw_t, raw_lang, raw_i) =
            decode_leaflet_region2(leaflet_bytes, &lh, header.dt_width)?;
        let r3_entries = decode_leaflet_region3(leaflet_bytes, &lh)?;

        // Step 3: Replay (or pass through if R3 is empty).
        let (eff_r1, eff_r2) = match replay_leaflet(
            &raw_s_ids,
            &raw_p_ids,
            &raw_o_kinds,
            &raw_o_keys,
            &raw_dt,
            &raw_t,
            &raw_lang,
            &raw_i,
            &r3_entries,
            self.to_t,
            self.order,
        ) {
            Some(replayed) => {
                // Replay produced new state
                let r1 = CachedRegion1 {
                    s_ids: replayed.s_ids.into(),
                    p_ids: replayed.p_ids.into(),
                    o_kinds: replayed.o_kinds.into(),
                    o_keys: replayed.o_keys.into(),
                    row_count: replayed.row_count,
                };
                let r2 = CachedRegion2 {
                    dt_values: replayed.dt_values.into(),
                    t_values: replayed.t_values.into(),
                    lang_ids: replayed.lang_ids.into(),
                    i_values: replayed.i_values.into(),
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
                if raw_t.iter().any(|&t| t > self.to_t) {
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
                    s_ids: raw_s_ids.into(),
                    p_ids: raw_p_ids.into(),
                    o_kinds: raw_o_kinds.into(),
                    o_keys: raw_o_keys.into(),
                    row_count,
                };
                let r2 = CachedRegion2 {
                    dt_values: raw_dt.into(),
                    t_values: raw_t.into(),
                    lang_ids: raw_lang.into(),
                    i_values: raw_i.into(),
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
                batch.s_ids.push(eff_r1.s_ids[row]);
                batch.p_ids.push(eff_r1.p_ids[row]);
                batch.o_kinds.push(eff_r1.o_kinds[row]);
                batch.o_keys.push(eff_r1.o_keys[row]);
                batch.dt_values.push(eff_r2.dt_values[row]);
                batch.t_values.push(eff_r2.t_values[row]);
                batch.lang_ids.push(eff_r2.lang_ids[row]);
                batch.i_values.push(eff_r2.i_values[row]);
            }
        } else {
            for &row in &matches {
                batch.s_ids.push(eff_r1.s_ids[row]);
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
        for row in 0..r1.row_count {
            if let Some(filter_s) = self.filter.s_id {
                if r1.s_ids[row] != filter_s {
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
    use super::super::global_dict::dt_ids;
    use super::super::run_record::NO_LIST_INDEX;
    use super::*;
    use fluree_db_core::value_id::ObjKey;

    fn make_overlay_op(s_id: u32, p_id: u32, val: i64, t: i64, assert: bool) -> OverlayOp {
        OverlayOp {
            s_id,
            p_id,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(val).as_u64(),
            t,
            op: assert,
            dt: dt_ids::INTEGER,
            lang_id: 0,
            i_val: NO_LIST_INDEX,
        }
    }

    /// Helper: make decoded R1+R2 columns from simple (s, p, val, t) tuples.
    fn make_columns(
        rows: &[(u32, u32, i64, i64)],
    ) -> (
        Vec<u32>,
        Vec<u32>,
        Vec<u8>,
        Vec<u64>,
        Vec<u32>,
        Vec<i64>,
        Vec<u16>,
        Vec<i32>,
    ) {
        let s: Vec<u32> = rows.iter().map(|r| r.0).collect();
        let p: Vec<u32> = rows.iter().map(|r| r.1).collect();
        let o_kinds: Vec<u8> = vec![ObjKind::NUM_INT.as_u8(); rows.len()];
        let o_keys: Vec<u64> = rows
            .iter()
            .map(|r| ObjKey::encode_i64(r.2).as_u64())
            .collect();
        let dt: Vec<u32> = vec![dt_ids::INTEGER as u32; rows.len()];
        let t: Vec<i64> = rows.iter().map(|r| r.3).collect();
        let lang: Vec<u16> = vec![0; rows.len()];
        let i: Vec<i32> = vec![NO_LIST_INDEX; rows.len()];
        (s, p, o_kinds, o_keys, dt, t, lang, i)
    }

    #[test]
    fn test_merge_overlay_empty() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1), (2, 1, 20, 1)]);
        let overlay: Vec<OverlayOp> = vec![];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 2);
        assert_eq!(&*r1.s_ids, &[1, 2]);
    }

    #[test]
    fn test_merge_overlay_assert_new_fact() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1), (3, 1, 30, 1)]);
        let overlay = vec![make_overlay_op(2, 1, 20, 5, true)];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 3);
        assert_eq!(&*r1.s_ids, &[1, 2, 3]);
    }

    #[test]
    fn test_merge_overlay_retract_existing() {
        let (s, p, ok, okey, dt, t, lang, i) =
            make_columns(&[(1, 1, 10, 1), (2, 1, 20, 1), (3, 1, 30, 1)]);
        let overlay = vec![make_overlay_op(2, 1, 20, 5, false)];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 2);
        assert_eq!(&*r1.s_ids, &[1, 3]);
    }

    #[test]
    fn test_merge_overlay_update_existing() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1)]);
        let overlay = vec![make_overlay_op(1, 1, 10, 5, true)];
        let (r1, r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 1);
        assert_eq!(&*r1.s_ids, &[1]);
        assert_eq!(r2.t_values[0], 5); // updated t
    }

    #[test]
    fn test_merge_overlay_retract_nonexistent() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1)]);
        let overlay = vec![make_overlay_op(0, 1, 5, 5, false)]; // retract before s=1
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 1);
        assert_eq!(&*r1.s_ids, &[1]);
    }

    #[test]
    fn test_merge_overlay_append_after() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1)]);
        let overlay = vec![
            make_overlay_op(5, 1, 50, 5, true),
            make_overlay_op(6, 1, 60, 5, true),
        ];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 3);
        assert_eq!(&*r1.s_ids, &[1, 5, 6]);
    }

    #[test]
    fn test_merge_overlay_prepend_before() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(5, 1, 50, 1)]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 5, true),
            make_overlay_op(2, 1, 20, 5, true),
        ];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 3);
        assert_eq!(&*r1.s_ids, &[1, 2, 5]);
    }

    #[test]
    fn test_merge_overlay_mixed_operations() {
        let (s, p, ok, okey, dt, t, lang, i) =
            make_columns(&[(1, 1, 10, 1), (2, 1, 20, 1), (3, 1, 30, 1), (5, 1, 50, 1)]);
        let overlay = vec![
            make_overlay_op(2, 1, 20, 5, false), // retract s=2
            make_overlay_op(3, 1, 30, 5, true),  // update s=3
            make_overlay_op(4, 1, 40, 5, true),  // insert s=4
        ];
        let (r1, r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 4);
        assert_eq!(&*r1.s_ids, &[1, 3, 4, 5]);
        assert_eq!(r2.t_values[1], 5); // s=3 updated
        assert_eq!(r2.t_values[2], 5); // s=4 inserted
    }

    #[test]
    fn test_merge_overlay_into_empty() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 1, true),
            make_overlay_op(2, 1, 20, 1, true),
        ];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 2);
        assert_eq!(&*r1.s_ids, &[1, 2]);
    }

    #[test]
    fn test_merge_overlay_retract_all() {
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[(1, 1, 10, 1), (2, 1, 20, 1)]);
        let overlay = vec![
            make_overlay_op(1, 1, 10, 5, false),
            make_overlay_op(2, 1, 20, 5, false),
        ];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 0);
    }

    #[test]
    fn test_merge_overlay_psot_order() {
        // PSOT: (p, s, o, dt) — p sorts first
        let (s, p, ok, okey, dt, t, lang, i) = make_columns(&[
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
            dt: dt_ids::INTEGER,
            lang_id: 0,
            i_val: NO_LIST_INDEX,
        }];
        let (r1, _r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Psot,
        );
        assert_eq!(r1.row_count, 4);
        // PSOT order: (p=1,s=1), (p=1,s=2), (p=1,s=3), (p=2,s=1)
        assert_eq!(&*r1.p_ids, &[1, 1, 1, 2]);
        assert_eq!(&*r1.s_ids, &[1, 2, 3, 1]);
    }

    #[test]
    fn test_merge_overlay_same_sort_different_identity() {
        // Two facts with same (s, p, o_kind, o_key, dt) in sort position but different lang_id
        use super::super::global_dict::dt_ids;
        let s = vec![1u32, 1];
        let p = vec![1u32, 1];
        let o_kind_val = ObjKind::LEX_ID.as_u8();
        let o_key_val = ObjKey::encode_u32_id(5).as_u64();
        let ok = vec![o_kind_val, o_kind_val];
        let okey = vec![o_key_val, o_key_val];
        let dt = vec![dt_ids::LANG_STRING as u32, dt_ids::LANG_STRING as u32];
        let t = vec![1i64, 1];
        let lang = vec![1u16, 2]; // different lang_ids
        let i = vec![NO_LIST_INDEX, NO_LIST_INDEX];

        // Retract lang_id=1 version only
        let overlay = vec![OverlayOp {
            s_id: 1,
            p_id: 1,
            o_kind: o_kind_val,
            o_key: o_key_val,
            t: 5,
            op: false,
            dt: dt_ids::LANG_STRING,
            lang_id: 1,
            i_val: NO_LIST_INDEX,
        }];
        let (r1, r2) = merge_overlay(
            &s,
            &p,
            &ok,
            &okey,
            &dt,
            &t,
            &lang,
            &i,
            &overlay,
            RunSortOrder::Spot,
        );
        assert_eq!(r1.row_count, 1);
        assert_eq!(r2.lang_ids[0], 2); // lang_id=2 survives
    }

    #[test]
    fn test_cmp_overlay_vs_record_spot() {
        let ov = make_overlay_op(5, 3, 10, 1, true);
        let rec = RunRecord::new(
            0,
            5,
            3,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            dt_ids::INTEGER,
            0,
            None,
        );
        assert_eq!(
            cmp_overlay_vs_record(&ov, &rec, RunSortOrder::Spot),
            Ordering::Equal
        );

        let rec2 = RunRecord::new(
            0,
            6,
            3,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            1,
            true,
            dt_ids::INTEGER,
            0,
            None,
        );
        assert_eq!(
            cmp_overlay_vs_record(&ov, &rec2, RunSortOrder::Spot),
            Ordering::Less
        );
    }
}
