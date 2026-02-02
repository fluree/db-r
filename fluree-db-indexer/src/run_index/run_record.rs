//! 40-byte fixed-width run record for external sort.
//!
//! Each record represents a single resolved op with global IDs.
//! The format is `#[repr(C)]` for direct binary I/O.

use fluree_db_core::value_id::{ObjKind, ObjKey};
use std::cmp::Ordering;

use super::global_dict::dt_ids;

/// 40-byte fixed-width record for external sort.
///
/// Sort key: `(g_id, …, o_kind, o_key, dt, t, op)` where the prefix depends on
/// the index order (SPOT, PSOT, POST, OPST).
///
/// `dt` is included in the sort key so that values with the same `(ObjKind,
/// ObjKey)` but different XSD types (e.g., `xsd:integer 3` vs `xsd:long 3`)
/// remain distinguishable.
///
/// ## Wire layout (40 bytes, little-endian)
///
/// ```text
/// g_id:    u32   [0..4]
/// s_id:    u32   [4..8]
/// p_id:    u32   [8..12]
/// dt:      u16   [12..14]   datatype dict index (tie-breaker)
/// o_kind:  u8    [14..15]   object kind discriminant
/// op:      u8    [15..16]   assert (1) / retract (0)
/// o_key:   u64   [16..24]   object key payload (8-byte aligned)
/// t:       i64   [24..32]
/// lang_id: u16   [32..34]   language tag id (0 = none)
/// _pad:    [u8;2][34..36]
/// i:       i32   [36..40]
/// ```
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct RunRecord {
    /// Graph ID (0 = default graph).
    pub g_id: u32,
    /// Subject ID (global dictionary).
    pub s_id: u32,
    /// Predicate ID (global dictionary).
    pub p_id: u32,
    /// Datatype dict index (for sort-key tie-breaking).
    pub dt: u16,
    /// Object kind discriminant (see `ObjKind`).
    pub o_kind: u8,
    /// Assert (1) or retract (0).
    pub op: u8,
    /// Object key payload (interpretation depends on `o_kind`).
    pub o_key: u64,
    /// Transaction number.
    pub t: i64,
    /// Language tag id (per-run assignment, 0 = none).
    pub lang_id: u16,
    /// Padding for alignment.
    pub _pad: [u8; 2],
    /// List index (i32::MIN = none).
    pub i: i32,
}

/// Sentinel value for "no list index".
pub const NO_LIST_INDEX: i32 = i32::MIN;

const _: () = assert!(std::mem::size_of::<RunRecord>() == 40);

impl RunRecord {
    /// Create a new RunRecord with all fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        g_id: u32,
        s_id: u32,
        p_id: u32,
        o_kind: ObjKind,
        o_key: ObjKey,
        t: i64,
        op: bool,
        dt: u16,
        lang_id: u16,
        i: Option<i32>,
    ) -> Self {
        Self {
            g_id,
            s_id,
            p_id,
            dt,
            o_kind: o_kind.as_u8(),
            op: op as u8,
            o_key: o_key.as_u64(),
            t,
            lang_id,
            _pad: [0; 2],
            i: i.unwrap_or(NO_LIST_INDEX),
        }
    }

    /// Serialize to 40 bytes, little-endian.
    pub fn write_le(&self, buf: &mut [u8; 40]) {
        buf[0..4].copy_from_slice(&self.g_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.s_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.p_id.to_le_bytes());
        buf[12..14].copy_from_slice(&self.dt.to_le_bytes());
        buf[14] = self.o_kind;
        buf[15] = self.op;
        buf[16..24].copy_from_slice(&self.o_key.to_le_bytes());
        buf[24..32].copy_from_slice(&self.t.to_le_bytes());
        buf[32..34].copy_from_slice(&self.lang_id.to_le_bytes());
        buf[34..36].copy_from_slice(&self._pad);
        buf[36..40].copy_from_slice(&self.i.to_le_bytes());
    }

    /// Deserialize from 40 bytes, little-endian.
    pub fn read_le(buf: &[u8; 40]) -> Self {
        Self {
            g_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            s_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            dt: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            o_kind: buf[14],
            op: buf[15],
            o_key: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            t: i64::from_le_bytes(buf[24..32].try_into().unwrap()),
            lang_id: u16::from_le_bytes(buf[32..34].try_into().unwrap()),
            _pad: [buf[34], buf[35]],
            i: i32::from_le_bytes(buf[36..40].try_into().unwrap()),
        }
    }

    /// Returns true if the object value is an IRI reference (`ObjKind::REF_ID`).
    /// Used to filter records for the OPST index, which only contains IRI refs.
    #[inline]
    pub fn is_iri_ref(&self) -> bool {
        self.o_kind == ObjKind::REF_ID.as_u8()
    }

    /// Get the object kind as an `ObjKind`.
    #[inline]
    pub fn obj_kind(&self) -> ObjKind {
        ObjKind::from_u8(self.o_kind)
    }

    /// Get the object key as an `ObjKey`.
    #[inline]
    pub fn obj_key(&self) -> ObjKey {
        ObjKey::from_u64(self.o_key)
    }
}

impl std::fmt::Debug for RunRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let i_display = if self.i == NO_LIST_INDEX {
            "None".to_string()
        } else {
            self.i.to_string()
        };
        f.debug_struct("RunRecord")
            .field("g_id", &self.g_id)
            .field("s_id", &self.s_id)
            .field("p_id", &self.p_id)
            .field("o_kind", &ObjKind::from_u8(self.o_kind))
            .field("o_key", &ObjKey::from_u64(self.o_key))
            .field("t", &self.t)
            .field("op", &(self.op != 0))
            .field("dt", &self.dt)
            .field("lang_id", &self.lang_id)
            .field("i", &i_display)
            .finish()
    }
}

// ============================================================================
// Comparators
// ============================================================================

/// SPOT comparator: `(g_id, s_id, p_id, o_kind, o_key, dt, t, op)`.
#[inline]
pub fn cmp_spot(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.g_id
        .cmp(&b.g_id)
        .then(a.s_id.cmp(&b.s_id))
        .then(a.p_id.cmp(&b.p_id))
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// PSOT comparator: `(g_id, p_id, s_id, o_kind, o_key, dt, t, op)`.
#[inline]
pub fn cmp_psot(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.g_id
        .cmp(&b.g_id)
        .then(a.p_id.cmp(&b.p_id))
        .then(a.s_id.cmp(&b.s_id))
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// POST comparator: `(g_id, p_id, o_kind, o_key, dt, s_id, t, op)`.
#[inline]
pub fn cmp_post(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.g_id
        .cmp(&b.g_id)
        .then(a.p_id.cmp(&b.p_id))
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.s_id.cmp(&b.s_id))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// OPST comparator: `(g_id, o_kind, o_key, dt, p_id, s_id, t, op)`.
#[inline]
pub fn cmp_opst(a: &RunRecord, b: &RunRecord) -> Ordering {
    a.g_id
        .cmp(&b.g_id)
        .then(a.o_kind.cmp(&b.o_kind))
        .then(a.o_key.cmp(&b.o_key))
        .then(a.dt.cmp(&b.dt))
        .then(a.p_id.cmp(&b.p_id))
        .then(a.s_id.cmp(&b.s_id))
        .then(a.t.cmp(&b.t))
        .then(a.op.cmp(&b.op))
}

/// Return the comparator function for a given sort order.
pub fn cmp_for_order(order: RunSortOrder) -> fn(&RunRecord, &RunRecord) -> Ordering {
    match order {
        RunSortOrder::Spot => cmp_spot,
        RunSortOrder::Psot => cmp_psot,
        RunSortOrder::Post => cmp_post,
        RunSortOrder::Opst => cmp_opst,
    }
}

/// Sort order identifier for run files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RunSortOrder {
    Spot = 0,
    Psot = 1,
    Post = 2,
    Opst = 3,
}

impl RunSortOrder {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Spot),
            1 => Some(Self::Psot),
            2 => Some(Self::Post),
            3 => Some(Self::Opst),
            _ => None,
        }
    }

    /// Directory name for this sort order (e.g., `"spot"`, `"psot"`).
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Psot => "psot",
            Self::Post => "post",
            Self::Opst => "opst",
        }
    }

    /// All orders that should be built during index generation.
    pub fn all_build_orders() -> &'static [RunSortOrder] {
        &[Self::Spot, Self::Psot, Self::Post, Self::Opst]
    }
}

// ============================================================================
// FactKey — identity key for dedup during replay and merge
// ============================================================================

/// Fact identity key used for deduplication during replay and novelty merge.
///
/// Mirrors the identity semantics of [`same_identity()`](super::merge::same_identity):
/// `(s_id, p_id, o_kind, o_key, dt, effective_lang_id, i)`.
///
/// `lang_id` is forced to 0 unless `dt == LANG_STRING`. `i` participates as-is
/// (with `NO_LIST_INDEX` sentinel for non-list facts).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FactKey {
    pub s_id: u32,
    pub p_id: u32,
    pub o_kind: u8,
    pub o_key: u64,
    pub dt: u16,
    /// Effective lang_id: 0 unless `dt == LANG_STRING`.
    pub lang_id: u16,
    /// List index (`NO_LIST_INDEX` for non-list facts).
    pub i: i32,
}

impl FactKey {
    /// Build a FactKey from a Region 3 entry.
    pub fn from_region3(e: &super::leaflet::Region3Entry) -> Self {
        let effective_lang_id = if e.dt == dt_ids::LANG_STRING { e.lang_id } else { 0 };
        Self {
            s_id: e.s_id,
            p_id: e.p_id,
            o_kind: e.o_kind,
            o_key: e.o_key,
            dt: e.dt,
            lang_id: effective_lang_id,
            i: e.i,
        }
    }

    /// Build a FactKey from decoded Region 1+2 row data.
    ///
    /// `dt_raw` is `u32` (Region 2 decode output); truncated to `u16` here.
    pub fn from_decoded_row(s_id: u32, p_id: u32, o_kind: u8, o_key: u64, dt_raw: u32, lang_id: u16, i: i32) -> Self {
        let dt = dt_raw as u16;
        let effective_lang_id = if dt == dt_ids::LANG_STRING { lang_id } else { 0 };
        Self {
            s_id,
            p_id,
            o_kind,
            o_key,
            dt,
            lang_id: effective_lang_id,
            i,
        }
    }

    /// Build a FactKey from a RunRecord.
    pub fn from_run_record(r: &RunRecord) -> Self {
        let effective_lang_id = if r.dt == dt_ids::LANG_STRING { r.lang_id } else { 0 };
        Self {
            s_id: r.s_id,
            p_id: r.p_id,
            o_kind: r.o_kind,
            o_key: r.o_key,
            dt: r.dt,
            lang_id: effective_lang_id,
            i: r.i,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(s_id: u32, p_id: u32, o_int: i64, dt: u16, t: i64) -> RunRecord {
        RunRecord::new(
            0,
            s_id,
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(o_int),
            t,
            true,
            dt,
            0,
            None,
        )
    }

    #[test]
    fn test_record_size() {
        assert_eq!(std::mem::size_of::<RunRecord>(), 40);
    }

    #[test]
    fn test_serialization_round_trip() {
        let rec = RunRecord::new(
            1,
            42,
            7,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(-100),
            5,
            true,
            dt_ids::LONG,
            3,
            Some(2),
        );

        let mut buf = [0u8; 40];
        rec.write_le(&mut buf);
        let restored = RunRecord::read_le(&buf);

        assert_eq!(rec.g_id, restored.g_id);
        assert_eq!(rec.s_id, restored.s_id);
        assert_eq!(rec.p_id, restored.p_id);
        assert_eq!(rec.o_kind, restored.o_kind);
        assert_eq!(rec.o_key, restored.o_key);
        assert_eq!(rec.t, restored.t);
        assert_eq!(rec.op, restored.op);
        assert_eq!(rec.dt, restored.dt);
        assert_eq!(rec.lang_id, restored.lang_id);
        assert_eq!(rec.i, restored.i);
    }

    #[test]
    fn test_spot_ordering_by_subject() {
        let a = make_record(1, 1, 0, dt_ids::INTEGER, 1);
        let b = make_record(2, 1, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_predicate() {
        let a = make_record(1, 1, 0, dt_ids::INTEGER, 1);
        let b = make_record(1, 2, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_object() {
        let a = make_record(1, 1, 10, dt_ids::INTEGER, 1);
        let b = make_record(1, 1, 20, dt_ids::INTEGER, 1);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_dt_tiebreak() {
        let a = RunRecord::new(
            0, 1, 1,
            ObjKind::NUM_INT, ObjKey::encode_i64(3),
            1, true,
            dt_ids::INTEGER, 0, None,
        );
        let b = RunRecord::new(
            0, 1, 1,
            ObjKind::NUM_INT, ObjKey::encode_i64(3),
            1, true,
            dt_ids::LONG, 0, None,
        );
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
        assert_ne!(a, b);
    }

    #[test]
    fn test_spot_ordering_by_t() {
        let a = make_record(1, 1, 0, dt_ids::INTEGER, 1);
        let b = make_record(1, 1, 0, dt_ids::INTEGER, 2);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_op() {
        let a = RunRecord::new(0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(0), 1, false, dt_ids::INTEGER, 0, None);
        let b = RunRecord::new(0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(0), 1, true, dt_ids::INTEGER, 0, None);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_spot_ordering_by_graph() {
        let a = RunRecord::new(0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(0), 1, true, dt_ids::INTEGER, 0, None);
        let b = RunRecord::new(1, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(0), 1, true, dt_ids::INTEGER, 0, None);
        assert_eq!(cmp_spot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_sort_unstable_by_spot() {
        let mut records = vec![
            make_record(3, 1, 0, dt_ids::INTEGER, 1),
            make_record(1, 2, 0, dt_ids::INTEGER, 1),
            make_record(1, 1, 0, dt_ids::INTEGER, 1),
            make_record(2, 1, 0, dt_ids::INTEGER, 1),
            make_record(1, 1, 10, dt_ids::INTEGER, 1),
        ];
        records.sort_unstable_by(cmp_spot);

        assert_eq!(records[0].s_id, 1);
        assert_eq!(records[0].p_id, 1);
        assert_eq!(records[0].o_key, ObjKey::encode_i64(0).as_u64());
        assert_eq!(records[1].s_id, 1);
        assert_eq!(records[1].p_id, 1);
        assert_eq!(records[1].o_key, ObjKey::encode_i64(10).as_u64());
        assert_eq!(records[2].s_id, 1);
        assert_eq!(records[2].p_id, 2);
        assert_eq!(records[3].s_id, 2);
        assert_eq!(records[4].s_id, 3);
    }

    #[test]
    fn test_no_list_index_sentinel() {
        let rec = RunRecord::new(0, 1, 1, ObjKind::NULL, ObjKey::ZERO, 1, true, dt_ids::STRING, 0, None);
        assert_eq!(rec.i, NO_LIST_INDEX);

        let rec2 = RunRecord::new(0, 1, 1, ObjKind::NULL, ObjKey::ZERO, 1, true, dt_ids::STRING, 0, Some(5));
        assert_eq!(rec2.i, 5);
    }

    #[test]
    fn test_run_sort_order_round_trip() {
        assert_eq!(RunSortOrder::from_u8(0), Some(RunSortOrder::Spot));
        assert_eq!(RunSortOrder::from_u8(1), Some(RunSortOrder::Psot));
        assert_eq!(RunSortOrder::from_u8(2), Some(RunSortOrder::Post));
        assert_eq!(RunSortOrder::from_u8(3), Some(RunSortOrder::Opst));
        assert_eq!(RunSortOrder::from_u8(4), None);
        assert_eq!(RunSortOrder::from_u8(255), None);
    }

    #[test]
    fn test_sort_order_dir_names() {
        assert_eq!(RunSortOrder::Spot.dir_name(), "spot");
        assert_eq!(RunSortOrder::Psot.dir_name(), "psot");
        assert_eq!(RunSortOrder::Post.dir_name(), "post");
        assert_eq!(RunSortOrder::Opst.dir_name(), "opst");
    }

    #[test]
    fn test_all_build_orders() {
        let orders = RunSortOrder::all_build_orders();
        assert_eq!(orders.len(), 4);
        assert_eq!(orders[0], RunSortOrder::Spot);
        assert_eq!(orders[1], RunSortOrder::Psot);
        assert_eq!(orders[2], RunSortOrder::Post);
        assert_eq!(orders[3], RunSortOrder::Opst);
    }

    // ---- PSOT comparator tests ----

    #[test]
    fn test_psot_ordering_predicate_first() {
        let a = make_record(2, 1, 0, dt_ids::INTEGER, 1);
        let b = make_record(1, 2, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_psot_ordering_subject_within_predicate() {
        let a = make_record(1, 5, 0, dt_ids::INTEGER, 1);
        let b = make_record(2, 5, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_psot_ordering_object() {
        let a = make_record(1, 5, 10, dt_ids::INTEGER, 1);
        let b = make_record(1, 5, 20, dt_ids::INTEGER, 1);
        assert_eq!(cmp_psot(&a, &b), Ordering::Less);
    }

    // ---- POST comparator tests ----

    #[test]
    fn test_post_ordering_predicate_first() {
        let a = make_record(2, 1, 100, dt_ids::INTEGER, 1);
        let b = make_record(1, 2, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_post_ordering_object_before_subject() {
        let a = make_record(2, 5, 10, dt_ids::INTEGER, 1);
        let b = make_record(1, 5, 20, dt_ids::INTEGER, 1);
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_post_ordering_dt_before_subject() {
        let a = RunRecord::new(
            0, 2, 5,
            ObjKind::NUM_INT, ObjKey::encode_i64(10),
            1, true,
            dt_ids::INTEGER, 0, None,
        );
        let b = RunRecord::new(
            0, 1, 5,
            ObjKind::NUM_INT, ObjKey::encode_i64(10),
            1, true,
            dt_ids::LONG, 0, None,
        );
        assert_eq!(cmp_post(&a, &b), Ordering::Less);
    }

    // ---- OPST comparator tests ----

    #[test]
    fn test_opst_ordering_object_first() {
        let a = make_record(10, 10, 1, dt_ids::INTEGER, 1);
        let b = make_record(1, 1, 2, dt_ids::INTEGER, 1);
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_opst_ordering_dt_before_predicate() {
        let a = RunRecord::new(
            0, 10, 10,
            ObjKind::NUM_INT, ObjKey::encode_i64(5),
            1, true,
            dt_ids::INTEGER, 0, None,
        );
        let b = RunRecord::new(
            0, 1, 1,
            ObjKind::NUM_INT, ObjKey::encode_i64(5),
            1, true,
            dt_ids::LONG, 0, None,
        );
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_opst_ordering_predicate_before_subject() {
        let a = make_record(10, 1, 5, dt_ids::INTEGER, 1);
        let b = make_record(1, 2, 5, dt_ids::INTEGER, 1);
        assert_eq!(cmp_opst(&a, &b), Ordering::Less);
    }

    // ---- cmp_for_order dispatch ----

    #[test]
    fn test_cmp_for_order_dispatches_correctly() {
        let a = make_record(2, 1, 0, dt_ids::INTEGER, 1);
        let b = make_record(1, 2, 0, dt_ids::INTEGER, 1);
        assert_eq!(cmp_for_order(RunSortOrder::Spot)(&a, &b), Ordering::Greater);
        assert_eq!(cmp_for_order(RunSortOrder::Psot)(&a, &b), Ordering::Less);
    }

    // ---- is_iri_ref ----

    #[test]
    fn test_is_iri_ref_true_for_iri() {
        let rec = RunRecord::new(
            0, 1, 1,
            ObjKind::REF_ID, ObjKey::encode_u32_id(42),
            1, true,
            dt_ids::ID, 0, None,
        );
        assert!(rec.is_iri_ref());
    }

    #[test]
    fn test_is_iri_ref_false_for_integer() {
        let rec = make_record(1, 1, 42, dt_ids::INTEGER, 1);
        assert!(!rec.is_iri_ref());
    }

    #[test]
    fn test_is_iri_ref_false_for_null() {
        let rec = RunRecord::new(
            0, 1, 1,
            ObjKind::NULL, ObjKey::ZERO,
            1, true,
            dt_ids::STRING, 0, None,
        );
        assert!(!rec.is_iri_ref());
    }

    // ---- Cross-kind ordering in comparators ----

    #[test]
    fn test_spot_ordering_cross_kind() {
        // NumInt should sort before NumF64 (0x03 < 0x04)
        let int_rec = RunRecord::new(
            0, 1, 1,
            ObjKind::NUM_INT, ObjKey::encode_i64(100),
            1, true, dt_ids::INTEGER, 0, None,
        );
        let f64_rec = RunRecord::new(
            0, 1, 1,
            ObjKind::NUM_F64, ObjKey::encode_f64(0.001).unwrap(),
            1, true, dt_ids::DOUBLE, 0, None,
        );
        assert_eq!(cmp_spot(&int_rec, &f64_rec), Ordering::Less);
    }

    // ---- FactKey tests ----

    #[test]
    fn test_fact_key_same_identity() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 99, false,
            dt_ids::INTEGER, 0, None,
        ));
        assert_eq!(a, b);
    }

    #[test]
    fn test_fact_key_different_subject() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0, 11, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        assert_ne!(a, b);
    }

    #[test]
    fn test_fact_key_different_dt() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::LONG, 0, None,
        ));
        assert_ne!(a, b);
    }

    #[test]
    fn test_fact_key_lang_effective() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::LEX_ID, ObjKey::encode_u32_id(5), 1, true,
            dt_ids::LANG_STRING, 3, None,
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::LEX_ID, ObjKey::encode_u32_id(5), 1, true,
            dt_ids::LANG_STRING, 4, None,
        ));
        assert_ne!(a, b);

        let c = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 3, None,
        ));
        let d = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 99, None,
        ));
        assert_eq!(c, d);
        assert_eq!(c.lang_id, 0);
    }

    #[test]
    fn test_fact_key_list_index() {
        let a = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, Some(0),
        ));
        let b = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, Some(1),
        ));
        assert_ne!(a, b);

        let c = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        let d = FactKey::from_run_record(&RunRecord::new(
            0, 1, 1, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, Some(0),
        ));
        assert_ne!(c, d);
    }

    #[test]
    fn test_fact_key_from_decoded_row() {
        let key = FactKey::from_decoded_row(
            10, 5,
            ObjKind::NUM_INT.as_u8(),
            ObjKey::encode_i64(42).as_u64(),
            dt_ids::INTEGER as u32,
            0,
            NO_LIST_INDEX,
        );
        let from_record = FactKey::from_run_record(&RunRecord::new(
            0, 10, 5, ObjKind::NUM_INT, ObjKey::encode_i64(42), 1, true,
            dt_ids::INTEGER, 0, None,
        ));
        assert_eq!(key, from_record);
    }
}
