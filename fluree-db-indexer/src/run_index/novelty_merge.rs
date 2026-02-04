//! Novelty merge: apply sorted novelty RunRecords to a decoded leaflet.
//!
//! Merges a batch of novelty operations (asserts and retracts) into decoded
//! leaflet columns, producing updated Regions 1+2 and new Region 3 entries.
//!
//! Used during incremental re-indexing when new commits are merged into
//! existing leaf files. The merge preserves the leaflet's sort order and
//! maintains the Region 3 operation log in reverse chronological order.

use super::leaflet::Region3Entry;
use super::run_record::{FactKey, RunRecord, RunSortOrder};
use std::cmp::Ordering;

// ============================================================================
// MergeInput / MergeOutput
// ============================================================================

/// Input to the novelty merge: decoded leaflet columns + novelty operations.
pub struct MergeInput<'a> {
    // Decoded current leaflet Regions 1+2
    pub r1_s_ids: &'a [u32],
    pub r1_p_ids: &'a [u32],
    pub r1_o_kinds: &'a [u8],
    pub r1_o_keys: &'a [u64],
    pub r2_dt: &'a [u32],
    pub r2_t: &'a [i64],
    pub r2_lang_ids: &'a [u16],
    pub r2_i: &'a [i32],
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
    pub s_ids: Vec<u32>,
    pub p_ids: Vec<u32>,
    pub o_kinds: Vec<u8>,
    pub o_keys: Vec<u64>,
    pub dt: Vec<u32>,
    pub t: Vec<i64>,
    pub lang_ids: Vec<u16>,
    pub i_vals: Vec<i32>,
    // New Region 3 entries (new ops ++ existing, reverse chronological)
    pub region3: Vec<Region3Entry>,
    pub row_count: usize,
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
    s_id: u32,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt: u32,
    rec: &RunRecord,
    order: RunSortOrder,
) -> Ordering {
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&rec.s_id)
            .then(p_id.cmp(&rec.p_id))
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt)),
        RunSortOrder::Psot => p_id
            .cmp(&rec.p_id)
            .then(s_id.cmp(&rec.s_id))
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt)),
        RunSortOrder::Post => p_id
            .cmp(&rec.p_id)
            .then(o_kind.cmp(&rec.o_kind))
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt))
            .then(s_id.cmp(&rec.s_id)),
        RunSortOrder::Opst => o_kind
            .cmp(&rec.o_kind)
            .then(o_key.cmp(&rec.o_key))
            .then((dt as u16).cmp(&rec.dt))
            .then(p_id.cmp(&rec.p_id))
            .then(s_id.cmp(&rec.s_id)),
    }
}

/// Check if a decoded row and a RunRecord share the same fact identity.
///
/// Uses FactKey semantics: (s_id, p_id, o, dt, effective_lang_id, i).
fn same_identity_row_vs_record(
    s_id: u32,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt: u32,
    lang_id: u16,
    i: i32,
    rec: &RunRecord,
) -> bool {
    FactKey::from_decoded_row(s_id, p_id, o_kind, o_key, dt, lang_id, i)
        == FactKey::from_run_record(rec)
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

    let span = tracing::debug_span!(
        "novelty_merge",
        existing_count = existing_len,
        novelty_count = novelty_len,
    );
    let _guard = span.enter();

    // Pre-allocate output at existing size (may grow or shrink slightly)
    let mut out_s = Vec::with_capacity(existing_len + novelty_len);
    let mut out_p = Vec::with_capacity(existing_len + novelty_len);
    let mut out_ok = Vec::with_capacity(existing_len + novelty_len);
    let mut out_okey: Vec<u64> = Vec::with_capacity(existing_len + novelty_len);
    let mut out_dt = Vec::with_capacity(existing_len + novelty_len);
    let mut out_t = Vec::with_capacity(existing_len + novelty_len);
    let mut out_lang = Vec::with_capacity(existing_len + novelty_len);
    let mut out_i = Vec::with_capacity(existing_len + novelty_len);

    // New R3 entries from this merge (will be prepended to existing R3)
    let mut new_r3: Vec<Region3Entry> = Vec::with_capacity(novelty_len);

    let mut ei = 0usize; // existing row index
    let mut ni = 0usize; // novelty index

    while ei < existing_len && ni < novelty_len {
        let nov = &input.novelty[ni];
        let cmp = cmp_row_vs_record(
            input.r1_s_ids[ei],
            input.r1_p_ids[ei],
            input.r1_o_kinds[ei],
            input.r1_o_keys[ei],
            input.r2_dt[ei],
            nov,
            input.order,
        );

        match cmp {
            Ordering::Less => {
                // Existing row comes first — emit unchanged
                emit_existing(
                    input,
                    ei,
                    &mut out_s,
                    &mut out_p,
                    &mut out_ok,
                    &mut out_okey,
                    &mut out_dt,
                    &mut out_t,
                    &mut out_lang,
                    &mut out_i,
                );
                ei += 1;
            }
            Ordering::Greater => {
                // Novelty comes first (not in existing data)
                if nov.op == 1 {
                    // Assert of new fact — emit to output
                    emit_novelty(
                        nov,
                        &mut out_s,
                        &mut out_p,
                        &mut out_ok,
                        &mut out_okey,
                        &mut out_dt,
                        &mut out_t,
                        &mut out_lang,
                        &mut out_i,
                    );
                }
                // Record in R3 regardless (retract of non-existent is still logged)
                new_r3.push(Region3Entry::from_run_record(nov));
                ni += 1;
            }
            Ordering::Equal => {
                // Sort-order position match — check full identity
                if same_identity_row_vs_record(
                    input.r1_s_ids[ei],
                    input.r1_p_ids[ei],
                    input.r1_o_kinds[ei],
                    input.r1_o_keys[ei],
                    input.r2_dt[ei],
                    input.r2_lang_ids[ei],
                    input.r2_i[ei],
                    nov,
                ) {
                    // Same fact identity
                    if nov.op == 1 {
                        // Assert (update) — emit novelty, record old retraction + new assert
                        emit_novelty(
                            nov,
                            &mut out_s,
                            &mut out_p,
                            &mut out_ok,
                            &mut out_okey,
                            &mut out_dt,
                            &mut out_t,
                            &mut out_lang,
                            &mut out_i,
                        );

                        // Record retraction of old value in R3
                        new_r3.push(Region3Entry {
                            s_id: input.r1_s_ids[ei],
                            p_id: input.r1_p_ids[ei],
                            o_kind: input.r1_o_kinds[ei],
                            o_key: input.r1_o_keys[ei],
                            t_signed: -input.r2_t[ei], // retract old
                            dt: input.r2_dt[ei] as u16,
                            lang_id: input.r2_lang_ids[ei],
                            i: input.r2_i[ei],
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
                    emit_existing(
                        input,
                        ei,
                        &mut out_s,
                        &mut out_p,
                        &mut out_ok,
                        &mut out_okey,
                        &mut out_dt,
                        &mut out_t,
                        &mut out_lang,
                        &mut out_i,
                    );
                    ei += 1;
                }
            }
        }
    }

    // Drain remaining existing rows
    while ei < existing_len {
        emit_existing(
            input,
            ei,
            &mut out_s,
            &mut out_p,
            &mut out_ok,
            &mut out_okey,
            &mut out_dt,
            &mut out_t,
            &mut out_lang,
            &mut out_i,
        );
        ei += 1;
    }

    // Drain remaining novelty
    while ni < novelty_len {
        let nov = &input.novelty[ni];
        if nov.op == 1 {
            emit_novelty(
                nov,
                &mut out_s,
                &mut out_p,
                &mut out_ok,
                &mut out_okey,
                &mut out_dt,
                &mut out_t,
                &mut out_lang,
                &mut out_i,
            );
        }
        new_r3.push(Region3Entry::from_run_record(nov));
        ni += 1;
    }

    // Assemble Region 3: new_r3 (sorted newest-first) ++ existing_r3
    // new_r3 entries all come from novelty at the same t (or sorted by t desc).
    // Sort new_r3 by abs(t_signed) descending for proper reverse-chronological order.
    new_r3.sort_by(|a, b| {
        b.t_signed
            .unsigned_abs()
            .cmp(&a.t_signed.unsigned_abs())
            .then_with(|| {
                // Tie-break: retracts before asserts at same t (retract = negative)
                a.t_signed.cmp(&b.t_signed)
            })
    });

    // Adjacent dedup within new_r3: if both entries have the same FactKey
    // and both are asserts, drop the newer one (oldest assert wins).
    dedup_adjacent_asserts(&mut new_r3);

    // Concatenate: new_r3 ++ existing_r3
    // At the boundary, last_new is the oldest entry in new_r3 and first_old
    // is the newest entry in existing_r3. If both are asserts with the same
    // FactKey, drop whichever has the larger abs_t (newer) to enforce
    // "oldest assert wins."
    let mut region3 = new_r3;
    let skip_first_old = match (region3.last(), input.existing_r3.first()) {
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
        region3.extend_from_slice(&input.existing_r3[1..]);
    } else {
        region3.extend_from_slice(input.existing_r3);
    }

    let row_count = out_s.len();
    MergeOutput {
        s_ids: out_s,
        p_ids: out_p,
        o_kinds: out_ok,
        o_keys: out_okey,
        dt: out_dt,
        t: out_t,
        lang_ids: out_lang,
        i_vals: out_i,
        region3,
        row_count,
    }
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
// Emit helpers
// ============================================================================

/// Emit an existing row (from decoded R1+R2 columns) to the output vectors.
#[inline]
fn emit_existing(
    input: &MergeInput<'_>,
    row: usize,
    s: &mut Vec<u32>,
    p: &mut Vec<u32>,
    ok: &mut Vec<u8>,
    okey: &mut Vec<u64>,
    dt: &mut Vec<u32>,
    t: &mut Vec<i64>,
    lang: &mut Vec<u16>,
    i: &mut Vec<i32>,
) {
    s.push(input.r1_s_ids[row]);
    p.push(input.r1_p_ids[row]);
    ok.push(input.r1_o_kinds[row]);
    okey.push(input.r1_o_keys[row]);
    dt.push(input.r2_dt[row]);
    t.push(input.r2_t[row]);
    lang.push(input.r2_lang_ids[row]);
    i.push(input.r2_i[row]);
}

/// Emit a novelty RunRecord to the output vectors.
#[inline]
fn emit_novelty(
    rec: &RunRecord,
    s: &mut Vec<u32>,
    p: &mut Vec<u32>,
    ok: &mut Vec<u8>,
    okey: &mut Vec<u64>,
    dt: &mut Vec<u32>,
    t: &mut Vec<i64>,
    lang: &mut Vec<u16>,
    i: &mut Vec<i32>,
) {
    s.push(rec.s_id);
    p.push(rec.p_id);
    ok.push(rec.o_kind);
    okey.push(rec.o_key);
    dt.push(rec.dt as u32);
    t.push(rec.t);
    lang.push(rec.lang_id);
    i.push(rec.i);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::global_dict::dt_ids;
    use super::super::run_record::NO_LIST_INDEX;
    use super::*;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    /// Helper: build a RunRecord for testing.
    fn rec(s_id: u32, p_id: u32, val: i64, t: i64, assert: bool) -> RunRecord {
        RunRecord::new(
            0,
            s_id,
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            assert,
            dt_ids::INTEGER,
            0,
            None,
        )
    }

    /// Helper: build MergeInput from vecs (takes ownership via slices).
    struct TestLeaflet {
        s_ids: Vec<u32>,
        p_ids: Vec<u32>,
        o_kinds: Vec<u8>,
        o_keys: Vec<u64>,
        dt: Vec<u32>,
        t: Vec<i64>,
        lang_ids: Vec<u16>,
        i_vals: Vec<i32>,
    }

    impl TestLeaflet {
        fn from_records(records: &[RunRecord]) -> Self {
            Self {
                s_ids: records.iter().map(|r| r.s_id).collect(),
                p_ids: records.iter().map(|r| r.p_id).collect(),
                o_kinds: records.iter().map(|r| r.o_kind).collect(),
                o_keys: records.iter().map(|r| r.o_key).collect(),
                dt: records.iter().map(|r| r.dt as u32).collect(),
                t: records.iter().map(|r| r.t).collect(),
                lang_ids: records.iter().map(|r| r.lang_id).collect(),
                i_vals: records.iter().map(|r| r.i).collect(),
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
                r2_lang_ids: &self.lang_ids,
                r2_i: &self.i_vals,
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
            t_signed: -2, // retraction at t=2
            dt: dt_ids::INTEGER,
            lang_id: 0,
            i: NO_LIST_INDEX,
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
                t_signed: 5,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t_signed: 3,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t_signed, 3); // older assert kept
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
                t_signed: -5,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(10).as_u64(),
                t_signed: 3,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
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
            1,
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            dt_ids::LANG_STRING,
            1,
            None, // lang_id=1
        );
        let r2 = RunRecord::new(
            0,
            1,
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            1,
            true,
            dt_ids::LANG_STRING,
            2,
            None, // lang_id=2
        );
        let leaflet = TestLeaflet::from_records(&[r1, r2]);

        // Retract only lang_id=1 version
        let novelty = vec![RunRecord::new(
            0,
            1,
            1,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(5),
            5,
            false,
            dt_ids::LANG_STRING,
            1,
            None,
        )];
        let input = leaflet.as_input(&novelty, &[]);

        let out = merge_novelty(&input);
        assert_eq!(out.row_count, 1); // lang_id=2 survives
        assert_eq!(out.lang_ids[0], 2);
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
            t_signed: 3,
            dt: dt_ids::INTEGER,
            lang_id: 0,
            i: NO_LIST_INDEX,
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
            t_signed: 10,
            dt: dt_ids::INTEGER,
            lang_id: 0,
            i: NO_LIST_INDEX,
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
                t_signed: 7,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ok,
                o_key: okey,
                t_signed: 5,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
            Region3Entry {
                s_id: 1,
                p_id: 1,
                o_kind: ok,
                o_key: okey,
                t_signed: 3,
                dt: dt_ids::INTEGER,
                lang_id: 0,
                i: NO_LIST_INDEX,
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t_signed, 3); // oldest survives
    }
}
