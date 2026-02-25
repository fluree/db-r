//! Novelty merge: apply sorted novelty RunRecords to a decoded leaflet.
//!
//! Merges a batch of novelty operations (asserts and retracts) into decoded
//! leaflet columns, producing updated Regions 1+2 and new Region 3 entries.
//!
//! Used during incremental re-indexing when new commits are merged into
//! existing leaf files. The merge preserves the leaflet's sort order and
//! maintains the Region 3 operation log in reverse chronological order.

use fluree_db_binary_index::format::leaflet::Region3Entry;
use fluree_db_binary_index::format::run_record::{
    FactKey, RunRecord, RunSortOrder, LIST_INDEX_NONE,
};
use fluree_db_binary_index::read::leaflet_cache::{SparseIColumn, SparseU16Column};
use fluree_db_core::subject_id::SubjectIdColumn;
use fluree_db_core::ListIndex;
use std::cmp::Ordering;

// ============================================================================
// MergeInput / MergeOutput
// ============================================================================

/// Input to the novelty merge: decoded leaflet columns + novelty operations.
pub struct MergeInput<'a> {
    // Decoded current leaflet Regions 1+2
    pub r1_s_ids: &'a SubjectIdColumn,
    pub r1_p_ids: &'a [u32],
    pub r1_o_kinds: &'a [u8],
    pub r1_o_keys: &'a [u64],
    pub r2_dt: &'a [u32],
    pub r2_t: &'a [u32],
    pub r2_lang: Option<&'a SparseU16Column>,
    pub r2_i: Option<&'a SparseIColumn>,
    // Existing Region 3 entries (carry forward, reverse chronological)
    pub existing_r3: &'a [Region3Entry],
    // Sorted novelty operations to apply (must be sorted by `order`)
    pub novelty: &'a [RunRecord],
    // Sort order (determines merge comparison)
    pub order: RunSortOrder,
}

/// Output of the novelty merge: updated leaflet columns + new Region 3.
pub struct MergeOutput {
    // Updated Region 1+2 columns (new current state)
    pub s_ids: Vec<u64>,
    pub p_ids: Vec<u32>,
    pub o_kinds: Vec<u8>,
    pub o_keys: Vec<u64>,
    pub dt: Vec<u32>,
    pub t: Vec<u32>,
    pub lang: Vec<u16>,
    pub i_vals: Vec<i32>,
    // New Region 3 entries (new ops ++ existing, reverse chronological)
    pub region3: Vec<Region3Entry>,
    pub row_count: usize,
}

struct OutputCols {
    s: Vec<u64>,
    p: Vec<u32>,
    o_kind: Vec<u8>,
    o_key: Vec<u64>,
    dt: Vec<u32>,
    t: Vec<u32>,
    lang: Vec<u16>,
    i: Vec<i32>,
}

impl OutputCols {
    fn with_capacity(cap: usize) -> Self {
        Self {
            s: Vec::with_capacity(cap),
            p: Vec::with_capacity(cap),
            o_kind: Vec::with_capacity(cap),
            o_key: Vec::with_capacity(cap),
            dt: Vec::with_capacity(cap),
            t: Vec::with_capacity(cap),
            lang: Vec::with_capacity(cap),
            i: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn push_existing(&mut self, input: &MergeInput<'_>, row: usize) {
        self.s.push(input.r1_s_ids.get(row).as_u64());
        self.p.push(input.r1_p_ids[row]);
        self.o_kind.push(input.r1_o_kinds[row]);
        self.o_key.push(input.r1_o_keys[row]);
        self.dt.push(input.r2_dt[row]);
        self.t.push(input.r2_t[row]);
        self.lang.push(input.lang_id(row));
        self.i.push(input.list_index(row));
    }

    #[inline]
    fn push_novelty(&mut self, rec: &RunRecord) {
        self.s.push(rec.s_id.as_u64());
        self.p.push(rec.p_id);
        self.o_kind.push(rec.o_kind);
        self.o_key.push(rec.o_key);
        self.dt.push(rec.dt as u32);
        self.t.push(rec.t);
        self.lang.push(rec.lang_id);
        self.i.push(if rec.i == LIST_INDEX_NONE {
            ListIndex::none().as_i32()
        } else {
            debug_assert!(
                rec.i <= i32::MAX as u32,
                "list index {} exceeds i32::MAX, would wrap in novelty merge",
                rec.i
            );
            rec.i as i32
        });
    }

    fn row_count(&self) -> usize {
        self.s.len()
    }
}

impl MergeInput<'_> {
    /// Get lang_id at row from sparse column (0 if absent).
    #[inline]
    fn lang_id(&self, row: usize) -> u16 {
        self.r2_lang.as_ref().map_or(0, |c| c.get(row as u16))
    }

    /// Get list index at row from sparse column (ListIndex::none() if absent).
    #[inline]
    fn list_index(&self, row: usize) -> i32 {
        self.r2_i
            .as_ref()
            .map_or(ListIndex::none().as_i32(), |c| c.get(row as u16))
    }
}

// ============================================================================
// Sort-order comparison for decoded rows vs RunRecords
// ============================================================================

/// Compare a decoded row (from R1 columns) against a RunRecord using the
/// given sort order's column priority.
///
/// Only compares the identity columns (s_id, p_id, o, dt) — not t/op.
/// Returns `Ordering::Equal` when both represent the same fact identity.
fn cmp_row_vs_record(
    s_id: u64,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt: u32,
    rec: &RunRecord,
    order: RunSortOrder,
) -> Ordering {
    let rec_s_id = rec.s_id.as_u64();
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&rec_s_id)
            .then(p_id.cmp(&rec.p_id))
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt)),
        RunSortOrder::Psot => p_id
            .cmp(&rec.p_id)
            .then(s_id.cmp(&rec_s_id))
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt)),
        RunSortOrder::Post => p_id
            .cmp(&rec.p_id)
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt))
            .then(s_id.cmp(&rec_s_id)),
        RunSortOrder::Opst => o_kind
            .cmp(&rec.o_kind)
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt))
            .then(p_id.cmp(&rec.p_id))
            .then(s_id.cmp(&rec_s_id)),
    }
}

// ============================================================================
// Merge algorithm
// ============================================================================

/// Merge sorted novelty operations into decoded leaflet columns.
///
/// Walks the existing R1 cursor and novelty cursor together in sort order:
///
/// - **R1 row < novelty**: Emit R1 row unchanged.
/// - **novelty < R1 row**:
///   - Assert: Emit novelty to output; record in new R3.
///   - Retract: Skip (retract of non-existent fact); still record in R3.
/// - **Same identity**:
///   - Assert: Emit novelty (update); record old value retraction + new assert in R3.
///   - Retract: Omit from output; record retraction in R3.
///
/// After the walk, assembles Region 3 as `new_r3 ++ existing_r3` (newest first),
/// with adjacent duplicate-assert dedup.
pub fn merge_novelty(input: &MergeInput<'_>) -> MergeOutput {
    let existing_len = input.r1_s_ids.len();
    let novelty_len = input.novelty.len();

    // Output columns (may grow or shrink slightly)
    let mut out = OutputCols::with_capacity(existing_len + novelty_len);

    // New R3 entries from this merge (will be prepended to existing R3).
    // Updates produce an additional "retract old" entry, so allow some headroom.
    let mut new_r3: Vec<Region3Entry> = Vec::with_capacity(novelty_len * 2);

    let mut ei = 0usize; // existing row index
    let mut ni = 0usize; // novelty index

    while ei < existing_len && ni < novelty_len {
        let nov = &input.novelty[ni];
        let s_id = input.r1_s_ids.get(ei).as_u64();
        let p_id = input.r1_p_ids[ei];
        let o_kind = input.r1_o_kinds[ei];
        let o_key = input.r1_o_keys[ei];
        let dt = input.r2_dt[ei];
        let cmp = cmp_row_vs_record(s_id, p_id, o_kind, o_key, dt, nov, input.order);

        match cmp {
            Ordering::Less => {
                // Existing row comes first — emit unchanged
                out.push_existing(input, ei);
                ei += 1;
            }
            Ordering::Greater => {
                // Novelty comes first (not in existing data)
                if nov.op == 1 {
                    // Assert of new fact — emit to output
                    out.push_novelty(nov);
                }
                // Record in R3 regardless (retract of non-existent is still logged)
                new_r3.push(Region3Entry::from_run_record(nov));
                ni += 1;
            }
            Ordering::Equal => {
                // Sort-order position match — check full identity
                let row_lang_id = input.lang_id(ei);
                let row_i = input.list_index(ei);
                let row_key =
                    FactKey::from_decoded_row(s_id, p_id, o_kind, o_key, dt, row_lang_id, row_i);
                if row_key == FactKey::from_run_record(nov) {
                    // Same fact identity
                    if nov.op == 1 {
                        // Assert (update) — emit novelty, record old retraction + new assert
                        out.push_novelty(nov);

                        // Record retraction of old value in R3
                        new_r3.push(Region3Entry {
                            s_id,
                            p_id,
                            o_kind,
                            o_key,
                            t: input.r2_t[ei],
                            op: 0, // retract old
                            dt: dt as u16,
                            lang_id: row_lang_id,
                            i: row_i,
                        });
                    }
                    // else: Retract — omit from output
                    // Record the operation in R3
                    new_r3.push(Region3Entry::from_run_record(nov));
                    ei += 1;
                    ni += 1;
                } else {
                    // Same sort position but different identity (e.g., different lang_id or i).
                    // This is an edge case: the sort-order comparison doesn't include
                    // lang_id/i, so we can have equal sort position but distinct facts.
                    // Emit the existing row and try the novelty again on the next iteration.
                    out.push_existing(input, ei);
                    ei += 1;
                }
            }
        }
    }

    // Drain remaining existing rows
    while ei < existing_len {
        out.push_existing(input, ei);
        ei += 1;
    }

    // Drain remaining novelty
    while ni < novelty_len {
        let nov = &input.novelty[ni];
        if nov.op == 1 {
            out.push_novelty(nov);
        }
        new_r3.push(Region3Entry::from_run_record(nov));
        ni += 1;
    }

    let region3 = assemble_region3(new_r3, input.existing_r3);
    let row_count = out.row_count();
    MergeOutput {
        s_ids: out.s,
        p_ids: out.p,
        o_kinds: out.o_kind,
        o_keys: out.o_key,
        dt: out.dt,
        t: out.t,
        lang: out.lang,
        i_vals: out.i,
        region3,
        row_count,
    }
}

fn assemble_region3(
    mut new_r3: Vec<Region3Entry>,
    existing_r3: &[Region3Entry],
) -> Vec<Region3Entry> {
    // Sort new_r3 by t descending for proper reverse-chronological order.
    new_r3.sort_by(|a, b| {
        b.t.cmp(&a.t).then_with(|| {
            // Tie-break: retracts before asserts at same t (retract op=0 < assert op=1)
            a.op.cmp(&b.op)
        })
    });

    // Adjacent dedup within new_r3: if both entries have the same FactKey
    // and both are asserts, drop the newer one (oldest assert wins).
    dedup_adjacent_asserts(&mut new_r3);

    // Concatenate: new_r3 ++ existing_r3 with boundary dedup for duplicate asserts.
    let mut region3 = new_r3;
    let skip_first_old = match (region3.last(), existing_r3.first()) {
        (Some(last_new), Some(first_old))
            if FactKey::from_region3(last_new) == FactKey::from_region3(first_old)
                && last_new.is_assert()
                && first_old.is_assert() =>
        {
            if last_new.abs_t() >= first_old.abs_t() {
                // last_new is newer (or equal) — drop it, keep first_old
                region3.pop();
                false
            } else {
                // first_old is newer — skip it when extending
                true
            }
        }
        _ => false,
    };
    if skip_first_old {
        region3.extend_from_slice(&existing_r3[1..]);
    } else {
        region3.extend_from_slice(existing_r3);
    }
    region3
}

/// Remove adjacent duplicate asserts within a reverse-chronological R3 list.
///
/// For any adjacent pair where both entries have the same FactKey and both are
/// asserts, the newer one (earlier index = higher abs_t) is dropped. This
/// prevents duplicate asserts from accumulating in the history log.
fn dedup_adjacent_asserts(entries: &mut Vec<Region3Entry>) {
    if entries.len() < 2 {
        return;
    }
    let mut write = 0;
    for read in 1..entries.len() {
        let is_dup = FactKey::from_region3(&entries[write])
            == FactKey::from_region3(&entries[read])
            && entries[write].is_assert()
            && entries[read].is_assert();
        if is_dup {
            // Drop the newer one (at write position in reverse-chrono list),
            // keep the older one (at read position).
            entries[write] = entries[read];
        } else {
            write += 1;
            entries[write] = entries[read];
        }
    }
    entries.truncate(write + 1);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::subject_id::{SubjectId, SubjectIdColumn, SubjectIdEncoding};
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::{DatatypeDictId, ListIndex};

    /// Helper: build a RunRecord for testing.
    fn rec(s_id: u64, p_id: u32, val: i64, t: u32, assert: bool) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            assert,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    /// Helper: build MergeInput from vecs (takes ownership via slices).
    struct TestLeaflet {
        s_ids: SubjectIdColumn,
        p_ids: Vec<u32>,
        o_kinds: Vec<u8>,
        o_keys: Vec<u64>,
        dt: Vec<u32>,
        t: Vec<u32>,
        lang: Option<SparseU16Column>,
        i_col: Option<SparseIColumn>,
    }

    impl TestLeaflet {
        fn from_records(records: &[RunRecord]) -> Self {
            let s_id_u64s: Vec<u64> = records.iter().map(|r| r.s_id.as_u64()).collect();
            let row_count = records.len();

            // Build sparse lang column from records
            let lang_positions: Vec<u16> = records
                .iter()
                .enumerate()
                .filter(|(_, r)| r.lang_id != 0)
                .map(|(idx, _)| idx as u16)
                .collect();
            let lang_values: Vec<u16> = records
                .iter()
                .filter(|r| r.lang_id != 0)
                .map(|r| r.lang_id)
                .collect();
            let lang = if lang_positions.is_empty() {
                None
            } else {
                Some(SparseU16Column {
                    positions: lang_positions.into(),
                    values: lang_values.into(),
                })
            };

            // Build sparse i column from records
            let i_positions: Vec<u16> = records
                .iter()
                .enumerate()
                .filter(|(_, r)| r.i != LIST_INDEX_NONE)
                .map(|(idx, _)| idx as u16)
                .collect();
            let i_values: Vec<u32> = records
                .iter()
                .filter(|r| r.i != LIST_INDEX_NONE)
                .map(|r| r.i)
                .collect();
            let i_col = if i_positions.is_empty() {
                None
            } else {
                Some(SparseIColumn::U32 {
                    positions: i_positions.into(),
                    values: i_values.into(),
                })
            };

            let _ = row_count; // used above for clarity
            Self {
                s_ids: SubjectIdColumn::from_u64_vec(s_id_u64s, SubjectIdEncoding::Wide),
                p_ids: records.iter().map(|r| r.p_id).collect(),
                o_kinds: records.iter().map(|r| r.o_kind).collect(),
                o_keys: records.iter().map(|r| r.o_key).collect(),
                dt: records.iter().map(|r| r.dt as u32).collect(),
                t: records.iter().map(|r| r.t).collect(),
                lang,
                i_col,
            }
        }

        fn as_input<'a>(
            &'a self,
            novelty: &'a [RunRecord],
            existing_r3: &'a [Region3Entry],
        ) -> MergeInput<'a> {
            MergeInput {
                r1_s_ids: &self.s_ids,
                r1_p_ids: &self.p_ids,
                r1_o_kinds: &self.o_kinds,
                r1_o_keys: &self.o_keys,
                r2_dt: &self.dt,
                r2_t: &self.t,
                r2_lang: self.lang.as_ref(),
                r2_i: self.i_col.as_ref(),
                existing_r3,
                novelty,
                order: RunSortOrder::Spot,
            }
        }
    }

    #[test]
    fn test_merge_empty_novelty() {
        // No novelty → output should match input exactly
        let existing = vec![rec(1, 1, 10, 1, true), rec(2, 1, 20, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty: Vec<RunRecord> = vec![];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![1, 2]);
        assert_eq!(out.region3.len(), 0);
    }

    #[test]
    fn test_merge_assert_new_fact() {
        // Existing: s=1, s=3. Novelty: assert s=2 (inserts between)
        let existing = vec![rec(1, 1, 10, 1, true), rec(3, 1, 30, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(2, 1, 20, 5, true)];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 2, 3]);
        assert_eq!(out.o_keys[1], ObjKey::encode_i64(20).as_u64());
        assert_eq!(out.t[1], 5); // novelty's t

        // R3 should have 1 entry (the assert)
        assert_eq!(out.region3.len(), 1);
        assert!(out.region3[0].is_assert());
        assert_eq!(out.region3[0].s_id, 2);
    }

    #[test]
    fn test_merge_retract_existing_fact() {
        // Existing: s=1, s=2, s=3. Novelty: retract s=2
        let existing = vec![
            rec(1, 1, 10, 1, true),
            rec(2, 1, 20, 1, true),
            rec(3, 1, 30, 1, true),
        ];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(2, 1, 20, 5, false)]; // retract
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![1, 3]); // s=2 removed

        // R3 should have 1 entry (the retraction)
        assert_eq!(out.region3.len(), 1);
        assert!(!out.region3[0].is_assert());
        assert_eq!(out.region3[0].s_id, 2);
    }

    #[test]
    fn test_merge_update_existing_fact() {
        // Existing: s=1 with val=10 at t=1
        // Novelty: s=1 assert with val=10 at t=5 (update — same identity, new t)
        let existing = vec![rec(1, 1, 10, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(1, 1, 10, 5, true)]; // update
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 1);
        assert_eq!(out.s_ids, vec![1]);
        assert_eq!(out.t[0], 5); // updated t

        // R3 should have 2 entries: retraction of old + assert of new
        assert_eq!(out.region3.len(), 2);
    }

    #[test]
    fn test_merge_retract_nonexistent() {
        // Retract of a fact not in existing → no output row, but R3 entry logged
        let existing = vec![rec(1, 1, 10, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(0, 1, 5, 5, false)]; // retract before s=1
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 1); // existing row survives
        assert_eq!(out.s_ids, vec![1]);

        // R3 still records the retraction
        assert_eq!(out.region3.len(), 1);
        assert!(!out.region3[0].is_assert());
    }

    #[test]
    fn test_merge_append_after_existing() {
        // Novelty inserts after all existing rows
        let existing = vec![rec(1, 1, 10, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(5, 1, 50, 5, true), rec(6, 1, 60, 5, true)];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 5, 6]);
    }

    #[test]
    fn test_merge_prepend_before_existing() {
        // Novelty inserts before all existing rows
        let existing = vec![rec(5, 1, 50, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(1, 1, 10, 5, true), rec(2, 1, 20, 5, true)];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 2, 5]);
    }

    #[test]
    fn test_merge_preserves_existing_r3() {
        // Existing R3 entries should be carried forward after new ones
        let existing = vec![rec(1, 1, 10, 3, true)];
        let leaflet = TestLeaflet::from_records(&existing);

        let old_r3 = vec![Region3Entry {
            s_id: 1,
            p_id: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(5).as_u64(),
            t: 2,
            op: 0, // retraction at t=2
            dt: DatatypeDictId::INTEGER.as_u16(),
            lang_id: 0,
            i: ListIndex::none().as_i32(),
        }];

        let novelty = vec![rec(2, 1, 20, 5, true)]; // new fact
        let input = leaflet.as_input(&novelty, &old_r3);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 2);

        // R3: new assert for s=2 (t=5) first, then old retraction (t=2)
        assert_eq!(out.region3.len(), 2);
        assert_eq!(out.region3[0].s_id, 2); // new entry
        assert_eq!(out.region3[0].abs_t(), 5);
        assert_eq!(out.region3[1].s_id, 1); // old entry
        assert_eq!(out.region3[1].abs_t(), 2);
    }

    #[test]
    fn test_merge_mixed_operations() {
        // Complex scenario: insert + update + retract in one merge
        let existing = vec![
            rec(1, 1, 10, 1, true),
            rec(2, 1, 20, 1, true),
            rec(3, 1, 30, 1, true),
            rec(5, 1, 50, 1, true),
        ];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![
            rec(2, 1, 20, 5, false), // retract s=2
            rec(3, 1, 30, 5, true),  // update s=3 (assert same identity)
            rec(4, 1, 40, 5, true),  // insert s=4
        ];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        // s=1 (unchanged), s=3 (updated), s=4 (new), s=5 (unchanged)
        // s=2 retracted
        assert_eq!(out.row_count, 4);
        assert_eq!(out.s_ids, vec![1, 3, 4, 5]);
        assert_eq!(out.t[1], 5); // s=3 updated to t=5
        assert_eq!(out.t[2], 5); // s=4 at t=5
    }

    #[test]
    fn test_merge_into_empty_leaflet() {
        let leaflet = TestLeaflet::from_records(&[]);
        let novelty = vec![rec(1, 1, 10, 1, true), rec(2, 1, 20, 1, true)];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![1, 2]);
        assert_eq!(out.region3.len(), 2);
    }

    #[test]
    fn test_merge_retract_all() {
        // Retract every existing fact → empty output
        let existing = vec![rec(1, 1, 10, 1, true), rec(2, 1, 20, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);
        let novelty = vec![rec(1, 1, 10, 5, false), rec(2, 1, 20, 5, false)];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 0);
        assert_eq!(out.region3.len(), 2);
    }

    #[test]
    fn test_dedup_adjacent_asserts() {
        // Two asserts with same FactKey → newer one dropped
        let mut entries = vec![
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t: 5,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t: 3,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t, 3); // older assert kept
    }

    #[test]
    fn test_dedup_keeps_retract_assert_pair() {
        // Retract then assert with same FactKey → both kept (different ops)
        let mut entries = vec![
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t: 5,
                op: 0, // retract
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t: 3,
                op: 1, // assert
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 2); // retract + assert both kept
    }

    #[test]
    fn test_merge_lang_string_distinct_identity() {
        // Two facts with same (s, p, o, dt=LANG_STRING) but different lang_id
        // should be treated as distinct identities
        let r1 = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            1,
            None, // lang_id=1
        );
        let r2 = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            2,
            None, // lang_id=2
        );
        let leaflet = TestLeaflet::from_records(&[r1, r2]);

        // Retract only lang_id=1 version
        let novelty = vec![RunRecord::new(
            0,
            SubjectId::from_u64(1),
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            5,
            false,
            DatatypeDictId::LANG_STRING.as_u16(),
            1,
            None,
        )];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 1); // lang_id=2 survives
        assert_eq!(out.lang[0], 2);
    }

    #[test]
    fn test_boundary_dedup_new_is_newer() {
        // new_r3 has an assert at t=10, existing_r3 has an assert at t=3 (same FactKey).
        // At the boundary, new is newer → drop new, keep old.
        let existing = vec![rec(1, 1, 10, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);

        let old_r3 = vec![Region3Entry {
            s_id: 1,
            p_id: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(10).as_u64(),
            t: 3,
            op: 1, // assert
            dt: DatatypeDictId::INTEGER.as_u16(),
            lang_id: 0,
            i: ListIndex::none().as_i32(),
        }];

        // Novelty asserts same fact at t=10 (also triggers an update in the merge walk,
        // producing new R3 entries). We add a second novelty to ensure we get an
        // assert entry in new_r3 that would collide at the boundary.
        let novelty = vec![rec(1, 1, 10, 10, true)];
        let input = leaflet.as_input(&novelty, &old_r3);

        let out = merge_novelty(&input);
        // Verify old_r3's entry (t=3) survives — it's the oldest assert
        let assert_entries: Vec<_> = out
            .region3
            .iter()
            .filter(|e| e.is_assert() && e.s_id == 1 && e.p_id == 1)
            .collect();
        // Should have exactly one assert for this FactKey (not duplicated)
        // The oldest (t=3) should be present
        assert!(
            assert_entries.iter().any(|e| e.abs_t() == 3),
            "oldest assert (t=3) should survive boundary dedup"
        );
    }

    #[test]
    fn test_boundary_dedup_old_is_newer() {
        // existing_r3 has an assert at t=10, new_r3's oldest entry is an assert at t=3
        // (same FactKey). At the boundary, old is newer → skip old, keep new.
        let existing = vec![rec(1, 1, 10, 1, true)];
        let leaflet = TestLeaflet::from_records(&existing);

        let old_r3 = vec![Region3Entry {
            s_id: 1,
            p_id: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            o_key: ObjKey::encode_i64(10).as_u64(),
            t: 10,
            op: 1, // assert
            dt: DatatypeDictId::INTEGER.as_u16(),
            lang_id: 0,
            i: ListIndex::none().as_i32(),
        }];

        // Novelty that produces an assert at t=3 for same identity
        let novelty = vec![rec(1, 1, 10, 3, true)];
        let input = leaflet.as_input(&novelty, &old_r3);

        let out = merge_novelty(&input);
        // The assert at t=3 (from new_r3) should survive; t=10 (from existing) dropped
        let assert_entries: Vec<_> = out
            .region3
            .iter()
            .filter(|e| e.is_assert() && e.s_id == 1 && e.p_id == 1)
            .collect();
        assert!(
            assert_entries.iter().any(|e| e.abs_t() == 3),
            "oldest assert (t=3) should survive boundary dedup"
        );
    }

    #[test]
    fn test_triple_adjacent_dedup() {
        // Three consecutive asserts with same FactKey at t=7, t=5, t=3
        // Should collapse to just t=3 (the oldest)
        let ok = ObjKind::NUM_INT.as_u8();
        let okey = ObjKey::encode_i64(10).as_u64();
        let mut entries = vec![
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ok,
                o_key: okey,
                t: 7,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ok,
                o_key: okey,
                t: 5,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ok,
                o_key: okey,
                t: 3,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t, 3); // oldest survives
    }
}
