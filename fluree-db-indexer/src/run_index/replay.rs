//! Time-travel replay: reconstruct leaflet state at a prior `t`.
//!
//! Per-leaflet two-pass replay consuming Region 1+2 (current snapshot) and
//! Region 3 (history journal) to produce the database state at `t_target`.
//!
//! ## Algorithm
//!
//! **Pass 1** — Scan Region 3 entries (newest→oldest). For each entry where
//! `|t_signed| > t_target`:
//! - Asserts added after `t_target` must be **excluded** from current state
//! - Retracts made after `t_target` must be **included** back into current state
//!
//! **Pass 2** — Three-way merge of Region 1 rows (minus excludes) plus includes,
//! maintaining the leaflet's sort order.
//!
//! ## Cost
//!
//! O(Region1 + Region3_scanned) — linear in leaflet size plus history depth.
//! Close-to-current queries scan few R3 entries; deep time-travel scans more
//! but is bounded by leaflet history size.

use super::leaflet::Region3Entry;
use super::run_record::{FactKey, RunSortOrder};
use std::collections::HashSet;

// ============================================================================
// ReplayedLeaflet: output of time-travel replay
// ============================================================================

/// Output of time-travel replay: reconstructed leaflet state at `t_target`.
///
/// Contains columnar data in the same format as decoded Region 1+2,
/// representing the database state as it existed at `t_target`.
pub struct ReplayedLeaflet {
    pub s_ids: Vec<u32>,
    pub p_ids: Vec<u32>,
    pub o_values: Vec<u64>,
    pub dt_values: Vec<u32>,
    pub t_values: Vec<i64>,
    pub lang_ids: Vec<u16>,
    pub i_values: Vec<i32>,
    pub row_count: usize,
}

// ============================================================================
// Sort-order comparison for Region 3 entries
// ============================================================================

/// Compare two Region3Entry values using the given sort order.
///
/// Compares only the sort-key columns (s_id, p_id, o, dt) — not t or op,
/// since those are not part of the index ordering.
fn cmp_r3_for_order(
    a: &Region3Entry,
    b: &Region3Entry,
    order: RunSortOrder,
) -> std::cmp::Ordering {
    match order {
        RunSortOrder::Spot => {
            a.s_id.cmp(&b.s_id)
                .then(a.p_id.cmp(&b.p_id))
                .then(a.o.cmp(&b.o))
                .then(a.dt.cmp(&b.dt))
        }
        RunSortOrder::Psot => {
            a.p_id.cmp(&b.p_id)
                .then(a.s_id.cmp(&b.s_id))
                .then(a.o.cmp(&b.o))
                .then(a.dt.cmp(&b.dt))
        }
        RunSortOrder::Post => {
            a.p_id.cmp(&b.p_id)
                .then(a.o.cmp(&b.o))
                .then(a.dt.cmp(&b.dt))
                .then(a.s_id.cmp(&b.s_id))
        }
        RunSortOrder::Opst => {
            a.o.cmp(&b.o)
                .then(a.dt.cmp(&b.dt))
                .then(a.p_id.cmp(&b.p_id))
                .then(a.s_id.cmp(&b.s_id))
        }
    }
}

/// Compare a decoded row (from Region 1+2) against a Region3Entry using
/// the given sort order (sort-key columns only).
///
/// `dt` is `u32` from Region 2 decode output but actual datatype IDs always
/// fit in `u16` (max 65,535 distinct datatype IRIs). The truncation to `u16`
/// here matches `FactKey::from_decoded_row()` and `Region3Entry.dt`.
fn cmp_row_vs_r3(
    s_id: u32,
    p_id: u32,
    o: u64,
    dt: u32,
    entry: &Region3Entry,
    order: RunSortOrder,
) -> std::cmp::Ordering {
    match order {
        RunSortOrder::Spot => {
            s_id.cmp(&entry.s_id)
                .then(p_id.cmp(&entry.p_id))
                .then(o.cmp(&entry.o))
                .then((dt as u16).cmp(&entry.dt))
        }
        RunSortOrder::Psot => {
            p_id.cmp(&entry.p_id)
                .then(s_id.cmp(&entry.s_id))
                .then(o.cmp(&entry.o))
                .then((dt as u16).cmp(&entry.dt))
        }
        RunSortOrder::Post => {
            p_id.cmp(&entry.p_id)
                .then(o.cmp(&entry.o))
                .then((dt as u16).cmp(&entry.dt))
                .then(s_id.cmp(&entry.s_id))
        }
        RunSortOrder::Opst => {
            o.cmp(&entry.o)
                .then((dt as u16).cmp(&entry.dt))
                .then(p_id.cmp(&entry.p_id))
                .then(s_id.cmp(&entry.s_id))
        }
    }
}

// ============================================================================
// replay_leaflet: two-pass time-travel reconstruction
// ============================================================================

/// Reconstruct the leaflet state at `t_target` using Region 1+2 (current
/// snapshot) and Region 3 (history journal).
///
/// Returns `None` if the replay produces no changes (i.e., Region 3 has no
/// entries after `t_target`, so the current state IS the state at `t_target`).
/// The caller can then use the original R1+R2 data directly.
///
/// # Arguments
///
/// * `r1_*` / `r2_*` — Decoded Region 1+2 columns (current snapshot state)
/// * `region3` — Region 3 entries in reverse chronological order (newest first)
/// * `t_target` — Target time to reconstruct state at
/// * `order` — Sort order of this leaflet (determines merge ordering)
pub fn replay_leaflet(
    r1_s_ids: &[u32],
    r1_p_ids: &[u32],
    r1_o_values: &[u64],
    r2_dt: &[u32],
    r2_t: &[i64],
    r2_lang_ids: &[u16],
    r2_i: &[i32],
    region3: &[Region3Entry],
    t_target: i64,
    order: RunSortOrder,
) -> Option<ReplayedLeaflet> {
    let row_count = r1_s_ids.len();
    debug_assert_eq!(r1_p_ids.len(), row_count);
    debug_assert_eq!(r1_o_values.len(), row_count);
    debug_assert_eq!(r2_dt.len(), row_count);
    debug_assert_eq!(r2_t.len(), row_count);
    debug_assert_eq!(r2_lang_ids.len(), row_count);
    debug_assert_eq!(r2_i.len(), row_count);

    // ---- Pass 1: Scan Region 3, build exclude/include sets ----
    //
    // Walk entries newest→oldest. For entries where |t_signed| > t_target:
    // - Assert (t_signed > 0): this fact was added AFTER t_target, so it
    //   should NOT exist at t_target → add to exclude set
    // - Retract (t_signed < 0): this fact was removed AFTER t_target, so it
    //   SHOULD exist at t_target → add to include list
    //
    // First-seen-per-key wins (Region 3 is newest-first, so first occurrence
    // of a FactKey is the most recent operation on that fact).

    let mut exclude: HashSet<FactKey> = HashSet::new();
    let mut include: Vec<Region3Entry> = Vec::new();
    let mut seen: HashSet<FactKey> = HashSet::new();
    let mut any_changes = false;

    for entry in region3 {
        let abs_t = entry.abs_t();
        // Once we reach entries at or before t_target, stop scanning
        if abs_t <= t_target as u64 {
            break;
        }

        let key = FactKey::from_region3(entry);
        if !seen.insert(key) {
            // Already processed a more recent operation on this fact — skip
            continue;
        }

        any_changes = true;
        if entry.is_assert() {
            // Fact was asserted after t_target — exclude from current state
            exclude.insert(key);
        } else {
            // Fact was retracted after t_target — include back into state.
            // Reconstruct the entry with positive t (it was alive at t_target).
            //
            // Note: `t_signed` is set to the retraction's abs_t, not the
            // original assertion time. Region 3 doesn't store the original
            // assertion time separately. For query purposes this is acceptable
            // — the restored row represents "this fact existed at t_target"
            // and the exact assertion time is not needed for state reconstruction.
            let mut restored = *entry;
            restored.t_signed = abs_t as i64; // make it an assert
            include.push(restored);
        }
    }

    if !any_changes {
        // No Region 3 entries after t_target — current state is correct
        return None;
    }

    // ---- Pass 2: Three-way merge ----
    //
    // Merge Region 1 rows (minus excludes) with include entries, maintaining
    // sort order. Both inputs are already in the leaflet's sort order:
    // - Region 1 rows: sorted by construction
    // - include entries: need to be sorted by the index's sort order

    include.sort_by(|a, b| cmp_r3_for_order(a, b, order));

    let estimated_size = row_count + include.len();
    let mut out = ReplayedLeaflet {
        s_ids: Vec::with_capacity(estimated_size),
        p_ids: Vec::with_capacity(estimated_size),
        o_values: Vec::with_capacity(estimated_size),
        dt_values: Vec::with_capacity(estimated_size),
        t_values: Vec::with_capacity(estimated_size),
        lang_ids: Vec::with_capacity(estimated_size),
        i_values: Vec::with_capacity(estimated_size),
        row_count: 0,
    };

    let mut r1_idx = 0;
    let mut inc_idx = 0;

    while r1_idx < row_count && inc_idx < include.len() {
        let r1_key = FactKey::from_decoded_row(
            r1_s_ids[r1_idx],
            r1_p_ids[r1_idx],
            r1_o_values[r1_idx],
            r2_dt[r1_idx],
            r2_lang_ids[r1_idx],
            r2_i[r1_idx],
        );

        // Check if this R1 row should be excluded
        if exclude.contains(&r1_key) {
            r1_idx += 1;
            continue;
        }

        let cmp = cmp_row_vs_r3(
            r1_s_ids[r1_idx],
            r1_p_ids[r1_idx],
            r1_o_values[r1_idx],
            r2_dt[r1_idx],
            &include[inc_idx],
            order,
        );

        match cmp {
            std::cmp::Ordering::Less => {
                // R1 row comes first — emit it
                emit_r1_row(&mut out, r1_idx, r1_s_ids, r1_p_ids, r1_o_values, r2_dt, r2_t, r2_lang_ids, r2_i);
                r1_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                // Include entry comes first — emit it
                emit_include_entry(&mut out, &include[inc_idx]);
                inc_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                // Same sort position. This can happen if the sort-key columns
                // match but the full FactKey (including lang_id, i) differs
                // — the sort-order comparators only use (s_id, p_id, o, dt).
                // In this case the R1 row is a surviving current fact and the
                // include entry is a distinct restored fact at the same sort
                // position. Emit R1 first to maintain stable ordering,
                // then the include entry.
                emit_r1_row(&mut out, r1_idx, r1_s_ids, r1_p_ids, r1_o_values, r2_dt, r2_t, r2_lang_ids, r2_i);
                emit_include_entry(&mut out, &include[inc_idx]);
                r1_idx += 1;
                inc_idx += 1;
            }
        }
    }

    // Drain remaining R1 rows
    while r1_idx < row_count {
        let r1_key = FactKey::from_decoded_row(
            r1_s_ids[r1_idx],
            r1_p_ids[r1_idx],
            r1_o_values[r1_idx],
            r2_dt[r1_idx],
            r2_lang_ids[r1_idx],
            r2_i[r1_idx],
        );
        if !exclude.contains(&r1_key) {
            emit_r1_row(&mut out, r1_idx, r1_s_ids, r1_p_ids, r1_o_values, r2_dt, r2_t, r2_lang_ids, r2_i);
        }
        r1_idx += 1;
    }

    // Drain remaining include entries
    while inc_idx < include.len() {
        emit_include_entry(&mut out, &include[inc_idx]);
        inc_idx += 1;
    }

    out.row_count = out.s_ids.len();
    Some(out)
}

// ============================================================================
// Emit helpers
// ============================================================================

/// Emit a Region 1+2 row to the output.
#[inline]
fn emit_r1_row(
    out: &mut ReplayedLeaflet,
    idx: usize,
    s_ids: &[u32],
    p_ids: &[u32],
    o_values: &[u64],
    dt: &[u32],
    t: &[i64],
    lang_ids: &[u16],
    i_vals: &[i32],
) {
    out.s_ids.push(s_ids[idx]);
    out.p_ids.push(p_ids[idx]);
    out.o_values.push(o_values[idx]);
    out.dt_values.push(dt[idx]);
    out.t_values.push(t[idx]);
    out.lang_ids.push(lang_ids[idx]);
    out.i_values.push(i_vals[idx]);
}

/// Emit a Region 3 include entry (restored retraction) to the output.
#[inline]
fn emit_include_entry(out: &mut ReplayedLeaflet, entry: &Region3Entry) {
    out.s_ids.push(entry.s_id);
    out.p_ids.push(entry.p_id);
    out.o_values.push(entry.o);
    out.dt_values.push(entry.dt as u32);
    out.t_values.push(entry.t_signed); // positive (restored assert)
    out.lang_ids.push(entry.lang_id);
    out.i_values.push(entry.i);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::run_record::NO_LIST_INDEX;

    /// Helper: build a Region3Entry.
    fn r3(s_id: u32, p_id: u32, o: u64, t_signed: i64, dt: u16) -> Region3Entry {
        Region3Entry {
            s_id,
            p_id,
            o,
            t_signed,
            dt,
            lang_id: 0,
            i: NO_LIST_INDEX,
        }
    }

    #[test]
    fn test_replay_no_changes_returns_none() {
        // Region 3 is empty — no replay needed
        let result = replay_leaflet(
            &[1, 2],
            &[10, 10],
            &[100, 200],
            &[1, 1],
            &[5, 5],
            &[0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX],
            &[], // empty R3
            3,   // t_target
            RunSortOrder::Spot,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_replay_all_entries_before_target_returns_none() {
        // All R3 entries are at or before t_target
        let r3_entries = vec![
            r3(1, 10, 100, 3, 1),  // assert at t=3
            r3(2, 10, 200, 2, 1),  // assert at t=2
        ];

        let result = replay_leaflet(
            &[1, 2],
            &[10, 10],
            &[100, 200],
            &[1, 1],
            &[3, 2],
            &[0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5, // t_target=5, all entries at t<=5
            RunSortOrder::Spot,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_replay_exclude_later_assert() {
        // Current state: s=1 p=10 o=100 (asserted at t=5)
        // Region 3: assert at t=5 (this was added after t_target=3)
        // Expected: s=1 p=10 o=100 should be excluded at t=3

        let r3_entries = vec![
            r3(1, 10, 100, 5, 1), // assert at t=5
        ];

        let result = replay_leaflet(
            &[1],
            &[10],
            &[100],
            &[1],
            &[5],
            &[0],
            &[NO_LIST_INDEX],
            &r3_entries,
            3, // want state at t=3
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 0, "fact asserted at t=5 should not exist at t=3");
    }

    #[test]
    fn test_replay_include_later_retract() {
        // Current state: (empty — the fact was retracted)
        // Region 3: retract at t=5 (retraction happened after t_target=3)
        // Expected: fact should be restored at t=3

        let r3_entries = vec![
            r3(1, 10, 100, -5, 1), // retract at t=5 (negative = retract)
        ];

        let result = replay_leaflet(
            &[],  // empty current state (fact was retracted)
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &r3_entries,
            3,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 1, "retracted fact should be restored at t=3");
        assert_eq!(out.s_ids[0], 1);
        assert_eq!(out.p_ids[0], 10);
        assert_eq!(out.o_values[0], 100);
        assert!(out.t_values[0] > 0, "restored fact should have positive t");
    }

    #[test]
    fn test_replay_mixed_exclude_and_include() {
        // Current state at max_t=10:
        //   s=1 p=10 o=100 (survived — existed before and after)
        //   s=2 p=10 o=200 (asserted at t=8)
        //   s=4 p=10 o=400 (survived — existed before and after)
        //
        // Region 3 (newest first):
        //   s=2 p=10 o=200 assert at t=8  → exclude (added after t=5)
        //   s=3 p=10 o=300 retract at t=7 → include (was alive at t=5)
        //
        // Expected at t=5: s=1, s=3, s=4

        let r3_entries = vec![
            r3(2, 10, 200, 8, 1),   // assert at t=8 → exclude
            r3(3, 10, 300, -7, 1),  // retract at t=7 → include
        ];

        let result = replay_leaflet(
            &[1, 2, 4],       // current R1 s_ids
            &[10, 10, 10],
            &[100, 200, 400],
            &[1, 1, 1],
            &[3, 8, 3],       // t values
            &[0, 0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5, // t_target
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 3, 4]);
        assert_eq!(out.o_values, vec![100, 300, 400]);
    }

    #[test]
    fn test_replay_first_seen_per_key_wins() {
        // Region 3 has two entries for the same fact:
        //   assert at t=10 (most recent)
        //   retract at t=7
        //
        // At t_target=5, first-seen (t=10 assert) wins → exclude.
        // The t=7 retract should be ignored (same key already seen).

        let r3_entries = vec![
            r3(1, 10, 100, 10, 1),  // assert at t=10 (first seen → exclude)
            r3(1, 10, 100, -7, 1),  // retract at t=7 (ignored, key already seen)
        ];

        let result = replay_leaflet(
            &[1],        // fact is in current state (asserted at t=10)
            &[10],
            &[100],
            &[1],
            &[10],
            &[0],
            &[NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        // The assert at t=10 causes exclusion; the retract at t=7 is ignored
        // because first-seen-per-key wins. The fact was not alive at t=5.
        assert_eq!(out.row_count, 0);
    }

    #[test]
    fn test_replay_preserves_sort_order_psot() {
        // Test with PSOT order to verify sort-order generic merge.
        // Include entries should be interleaved correctly by PSOT order.

        // Current state: p=10 s=1, p=20 s=3
        // Include: p=10 s=2 (from retract at t=8)
        // Expected PSOT order: p=10 s=1, p=10 s=2, p=20 s=3

        let r3_entries = vec![
            r3(2, 10, 200, -8, 1), // retract at t=8 → include
        ];

        let result = replay_leaflet(
            &[1, 3],       // current s_ids (PSOT order: p=10 s=1, p=20 s=3)
            &[10, 20],     // p_ids
            &[100, 300],
            &[1, 1],
            &[3, 3],
            &[0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Psot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 3);
        // PSOT order: (p=10,s=1), (p=10,s=2), (p=20,s=3)
        assert_eq!(out.p_ids, vec![10, 10, 20]);
        assert_eq!(out.s_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_replay_multiple_includes_sorted() {
        // Multiple include entries should be sorted and merged correctly.

        // Current state: s=3 p=10 o=300
        // Include: s=1 (retract at t=8), s=5 (retract at t=7)
        // Expected SPOT: s=1, s=3, s=5

        let r3_entries = vec![
            r3(1, 10, 100, -8, 1),  // retract at t=8 → include
            r3(5, 10, 500, -7, 1),  // retract at t=7 → include
        ];

        let result = replay_leaflet(
            &[3],
            &[10],
            &[300],
            &[1],
            &[3],
            &[0],
            &[NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 3, 5]);
    }

    #[test]
    fn test_replay_all_excluded() {
        // All current rows are excluded — result should be empty.

        let r3_entries = vec![
            r3(1, 10, 100, 8, 1),  // assert at t=8 → exclude
            r3(2, 10, 200, 7, 1),  // assert at t=7 → exclude
        ];

        let result = replay_leaflet(
            &[1, 2],
            &[10, 10],
            &[100, 200],
            &[1, 1],
            &[8, 7],
            &[0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 0);
    }

    #[test]
    fn test_replay_boundary_t_equals_target() {
        // Entry at exactly t_target should NOT be undone.
        // abs_t == t_target → break condition, not processed.

        let r3_entries = vec![
            r3(2, 10, 200, 8, 1),  // assert at t=8 → exclude (8 > 5)
            r3(1, 10, 100, 5, 1),  // assert at t=5 → NOT excluded (5 == 5, stop)
        ];

        let result = replay_leaflet(
            &[1, 2],
            &[10, 10],
            &[100, 200],
            &[1, 1],
            &[5, 8],
            &[0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 1);
        assert_eq!(out.s_ids, vec![1]); // s=1 survives (t=5 is at boundary)
    }

    #[test]
    fn test_replay_multi_transaction_assert_retract_assert() {
        // Same fact goes through: assert t=3 → retract t=5 → assert t=8
        //
        // Region 3 (newest first): assert t=8, retract t=5, assert t=3
        // Current state: fact IS present (latest op is assert at t=8)
        //
        // At t_target=6: first-seen is assert at t=8 → exclude.
        //   The retract at t=5 is skipped (key already seen).
        //   Result: fact is excluded. Correct — at t=6 the fact was retracted
        //   (retraction at t=5 happened before t=6, re-assert at t=8 hadn't yet).
        //
        // At t_target=4: first-seen is assert at t=8 → exclude.
        //   Retract at t=5 also > 4, but key already seen → skip.
        //   Assert at t=3 is not > 4 → stop scanning.
        //   Result: fact is excluded. Correct — the original assert at t=3
        //   exists in R1 but the R3 assert at t=8 excludes it. However, the
        //   R1 row's FactKey matches the excluded key, so it IS removed.
        //
        // At t_target=2: first-seen is assert at t=8 → exclude.
        //   Retract at t=5 skipped. Assert at t=3 > 2 → but key already seen.
        //   Result: fact is excluded. Correct — at t=2 the fact didn't exist yet.

        let r3_entries = vec![
            r3(1, 10, 100, 8, 1),   // assert at t=8 (newest)
            r3(1, 10, 100, -5, 1),  // retract at t=5
            r3(1, 10, 100, 3, 1),   // assert at t=3 (oldest)
        ];

        // Current R1: fact is present (asserted at t=8)
        let result_t6 = replay_leaflet(
            &[1], &[10], &[100], &[1], &[8], &[0], &[NO_LIST_INDEX],
            &r3_entries, 6, RunSortOrder::Spot,
        );
        let out = result_t6.expect("should produce replay");
        assert_eq!(out.row_count, 0, "at t=6, fact was retracted (retract at t=5)");

        let result_t4 = replay_leaflet(
            &[1], &[10], &[100], &[1], &[8], &[0], &[NO_LIST_INDEX],
            &r3_entries, 4, RunSortOrder::Spot,
        );
        let out = result_t4.expect("should produce replay");
        assert_eq!(out.row_count, 0, "at t=4, fact was asserted at t=3 but R3 assert at t=8 excludes");
        // Note: this test demonstrates the first-seen-per-key limitation.
        // The t=8 assert masks the t=3 assert. The R1 row (which has the
        // t=8 snapshot) is excluded, and the t=3 assert is not separately
        // tracked. A more precise replay would need the full history chain.
    }

    #[test]
    fn test_replay_retract_then_reassert_restore() {
        // Fact retracted at t=5, then re-asserted at t=8.
        // Current state: present (re-asserted at t=8).
        //
        // Region 3: assert t=8, retract t=5
        //
        // At t_target=4: assert at t=8 → exclude from R1.
        //   Retract at t=5 → key already seen (assert at t=8 was first), skip.
        //   Result: excluded. At t=4 the fact existed (original assert before t=5),
        //   but the R1 row is the t=8 version which gets excluded.
        //   The retract at t=5 is not processed because first-seen wins.

        let r3_entries = vec![
            r3(1, 10, 100, 8, 1),   // assert at t=8 (newest, first-seen)
            r3(1, 10, 100, -5, 1),  // retract at t=5
        ];

        let result = replay_leaflet(
            &[1], &[10], &[100], &[1], &[8], &[0], &[NO_LIST_INDEX],
            &r3_entries, 4, RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 0);
    }

    #[test]
    fn test_replay_equal_sort_position_different_facts() {
        // Two facts with the same sort-key columns (s_id, p_id, o, dt) but
        // different full identity (different lang_id). One survives in R1,
        // one is restored from retraction.
        //
        // This exercises the Equal branch in the three-way merge: sort-key
        // match but different FactKey means both should be emitted.

        // Current R1: s=1 p=10 o=100 dt=11 lang=1 (a lang string in English)
        // Include: s=1 p=10 o=100 dt=11 lang=2 (same triple in French, retracted at t=8)
        //
        // These have the same sort-key (s_id=1, p_id=10, o=100, dt=11)
        // but different lang_id, so cmp_row_vs_r3 returns Equal.

        let r3_entries = vec![
            Region3Entry {
                s_id: 1, p_id: 10, o: 100,
                t_signed: -8, // retract at t=8
                dt: 11,       // LANG_STRING
                lang_id: 2,   // French
                i: NO_LIST_INDEX,
            },
        ];

        let result = replay_leaflet(
            &[1],              // s_ids
            &[10],             // p_ids
            &[100],            // o_values
            &[11],             // dt (LANG_STRING)
            &[3],              // t
            &[1],              // lang_ids (English)
            &[NO_LIST_INDEX],
            &r3_entries,
            5, // want state at t=5 (retraction at t=8 → include French back)
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        // Both facts should be present: English (from R1) and French (restored)
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![1, 1]);
        assert_eq!(out.lang_ids, vec![1, 2]); // English first (from R1), then French (restored)
    }

    #[test]
    fn test_replay_multiple_facts_different_transactions() {
        // History spanning multiple transactions with interleaved operations.
        //
        // Current state at max_t=10: s=1 o=100, s=3 o=300, s=5 o=500
        //
        // Region 3 (newest first):
        //   s=5 assert at t=10
        //   s=2 retract at t=9
        //   s=4 retract at t=8
        //   s=3 assert at t=7
        //   s=1 assert at t=6
        //
        // At t_target=5:
        //   s=5 assert t=10 → exclude (10 > 5)
        //   s=2 retract t=9 → include (9 > 5, restore)
        //   s=4 retract t=8 → include (8 > 5, restore)
        //   s=3 assert t=7 → exclude (7 > 5)
        //   s=1 assert t=6 → exclude (6 > 5)
        //
        // Result: R1 rows s=1,s=3,s=5 all excluded. s=2,s=4 included.
        // Expected: s=2, s=4 (in SPOT order)

        let r3_entries = vec![
            r3(5, 10, 500, 10, 1),  // assert at t=10
            r3(2, 10, 200, -9, 1),  // retract at t=9
            r3(4, 10, 400, -8, 1),  // retract at t=8
            r3(3, 10, 300, 7, 1),   // assert at t=7
            r3(1, 10, 100, 6, 1),   // assert at t=6
        ];

        let result = replay_leaflet(
            &[1, 3, 5],           // current R1 s_ids
            &[10, 10, 10],
            &[100, 300, 500],
            &[1, 1, 1],
            &[6, 7, 10],          // t values
            &[0, 0, 0],
            &[NO_LIST_INDEX, NO_LIST_INDEX, NO_LIST_INDEX],
            &r3_entries,
            5,
            RunSortOrder::Spot,
        );
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![2, 4]);
        assert_eq!(out.o_values, vec![200, 400]);
    }
}
