//! V3 query-facing types: fact identity, decoded rows, overlay operations.
//!
//! These parallel the V2 types in `types.rs` (`DecodedRow`, `OverlayOp`,
//! `sort_overlay_ops`) but use the V3 identity model:
//! `(s_id, p_id, o_type, o_key, o_i)` instead of `(s_id, p_id, o_kind, o_key, dt)`.

use crate::format::run_record::RunSortOrder;
use std::cmp::Ordering;

// ============================================================================
// FactKeyV3 — fact identity
// ============================================================================

/// Fact identity key for the V3 format.
///
/// Two facts with the same `FactKeyV3` are the same fact (differ only in `t`).
/// Used for dedup, overlay merge, and replay membership sets.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FactKeyV3 {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
}

// ============================================================================
// DecodedRowV3
// ============================================================================

/// A single decoded row from V3 columnar storage.
#[derive(Debug, Clone, Copy)]
pub struct DecodedRowV3 {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
    pub t: i64,
}

impl DecodedRowV3 {
    /// Extract the fact identity key (without `t`).
    #[inline]
    pub fn fact_key(&self) -> FactKeyV3 {
        FactKeyV3 {
            s_id: self.s_id,
            p_id: self.p_id,
            o_type: self.o_type,
            o_key: self.o_key,
            o_i: self.o_i,
        }
    }
}

// ============================================================================
// OverlayOpV3
// ============================================================================

/// An overlay operation translated to V3 integer-ID space.
///
/// Produced by translating `Flake` overlay ops via reverse dictionary lookups.
/// Sorted by the cursor's sort order for streaming merge with decoded
/// leaflet columns.
#[derive(Debug, Clone, Copy)]
pub struct OverlayOpV3 {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
    pub t: i64,
    /// true = assert, false = retract.
    pub op: bool,
}

impl OverlayOpV3 {
    /// Extract the fact identity key (without `t` and `op`).
    #[inline]
    pub fn fact_key(&self) -> FactKeyV3 {
        FactKeyV3 {
            s_id: self.s_id,
            p_id: self.p_id,
            o_type: self.o_type,
            o_key: self.o_key,
            o_i: self.o_i,
        }
    }
}

// ============================================================================
// Sort / comparison helpers
// ============================================================================

/// Compare two overlay ops by the V3 sort order (no `t` in sort order).
fn cmp_overlay_v3(a: &OverlayOpV3, b: &OverlayOpV3, order: RunSortOrder) -> Ordering {
    match order {
        RunSortOrder::Spot => a
            .s_id
            .cmp(&b.s_id)
            .then(a.p_id.cmp(&b.p_id))
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i)),
        RunSortOrder::Psot => a
            .p_id
            .cmp(&b.p_id)
            .then(a.s_id.cmp(&b.s_id))
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i)),
        RunSortOrder::Post => a
            .p_id
            .cmp(&b.p_id)
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i))
            .then(a.s_id.cmp(&b.s_id)),
        RunSortOrder::Opst => a
            .o_type
            .cmp(&b.o_type)
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i))
            .then(a.p_id.cmp(&b.p_id))
            .then(a.s_id.cmp(&b.s_id)),
    }
}

/// Sort overlay ops by the given V3 sort order.
pub fn sort_overlay_ops_v3(ops: &mut [OverlayOpV3], order: RunSortOrder) {
    ops.sort_unstable_by(|a, b| cmp_overlay_v3(a, b, order));
}

/// Compare a decoded row (from a `ColumnBatch`) against an overlay op
/// using the V3 sort order. Used by the two-pointer merge in `BinaryCursorV3`.
#[inline]
pub fn cmp_row_vs_overlay_v3(
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
    ov: &OverlayOpV3,
    order: RunSortOrder,
) -> Ordering {
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&ov.s_id)
            .then(p_id.cmp(&ov.p_id))
            .then(o_type.cmp(&ov.o_type))
            .then(o_key.cmp(&ov.o_key))
            .then(o_i.cmp(&ov.o_i)),
        RunSortOrder::Psot => p_id
            .cmp(&ov.p_id)
            .then(s_id.cmp(&ov.s_id))
            .then(o_type.cmp(&ov.o_type))
            .then(o_key.cmp(&ov.o_key))
            .then(o_i.cmp(&ov.o_i)),
        RunSortOrder::Post => p_id
            .cmp(&ov.p_id)
            .then(o_type.cmp(&ov.o_type))
            .then(o_key.cmp(&ov.o_key))
            .then(o_i.cmp(&ov.o_i))
            .then(s_id.cmp(&ov.s_id)),
        RunSortOrder::Opst => o_type
            .cmp(&ov.o_type)
            .then(o_key.cmp(&ov.o_key))
            .then(o_i.cmp(&ov.o_i))
            .then(p_id.cmp(&ov.p_id))
            .then(s_id.cmp(&ov.s_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fact_key_equality() {
        let k1 = FactKeyV3 {
            s_id: 1,
            p_id: 2,
            o_type: 3,
            o_key: 4,
            o_i: u32::MAX,
        };
        let k2 = k1;
        assert_eq!(k1, k2);
    }

    #[test]
    fn overlay_sort_spot() {
        let mut ops = vec![
            OverlayOpV3 {
                s_id: 2,
                p_id: 1,
                o_type: 0,
                o_key: 0,
                o_i: u32::MAX,
                t: 1,
                op: true,
            },
            OverlayOpV3 {
                s_id: 1,
                p_id: 1,
                o_type: 0,
                o_key: 0,
                o_i: u32::MAX,
                t: 1,
                op: true,
            },
        ];
        sort_overlay_ops_v3(&mut ops, RunSortOrder::Spot);
        assert_eq!(ops[0].s_id, 1);
        assert_eq!(ops[1].s_id, 2);
    }

    #[test]
    fn overlay_sort_post() {
        let mut ops = vec![
            OverlayOpV3 {
                s_id: 1,
                p_id: 2,
                o_type: 0,
                o_key: 10,
                o_i: u32::MAX,
                t: 1,
                op: true,
            },
            OverlayOpV3 {
                s_id: 1,
                p_id: 2,
                o_type: 0,
                o_key: 5,
                o_i: u32::MAX,
                t: 1,
                op: true,
            },
        ];
        sort_overlay_ops_v3(&mut ops, RunSortOrder::Post);
        assert_eq!(ops[0].o_key, 5);
        assert_eq!(ops[1].o_key, 10);
    }
}
