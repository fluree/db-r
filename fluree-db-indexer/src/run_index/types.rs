//! Query-facing run_index types shared across crate boundaries.

use super::run_record::RunSortOrder;

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
    pub s_id: u64,
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

/// Sort overlay ops by the given sort order's column priority.
///
/// Column priorities must match the on-disk comparator order used by
/// `cmp_for_order`, `cmp_row_vs_overlay`, and `cmp_row_vs_record` so
/// that the merge cursors see a consistent sequence.
pub fn sort_overlay_ops(ops: &mut [OverlayOp], order: RunSortOrder) {
    ops.sort_unstable_by(|a, b| match order {
        RunSortOrder::Spot => a.s_id.cmp(&b.s_id)
            .then(a.p_id.cmp(&b.p_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Psot => a.p_id.cmp(&b.p_id)
            .then(a.s_id.cmp(&b.s_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Post => a.p_id.cmp(&b.p_id)
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.s_id.cmp(&b.s_id)),
        RunSortOrder::Opst => a.o_kind.cmp(&b.o_kind)
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.p_id.cmp(&b.p_id))
            .then(a.s_id.cmp(&b.s_id)),
    });
}
