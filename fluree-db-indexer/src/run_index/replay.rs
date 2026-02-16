//! Time-travel replay: reconstruct leaflet state at a prior `t`.
//!
//! Per-leaflet replay consuming Region 1+2 (current snapshot) and
//! Region 3 (history journal) to produce the database state at `t_target`.
//!
//! ## Algorithm
//!
//! **Step 0** — Build membership set from R1 (current state FactKeys).
//!
//! **Step 1** — Collect all events with `t > t_target`:
//! - R3 history entries (asserts and retracts from prior operations)
//! - R1 synthetic asserts (rows whose `t > t_target`)
//!
//! **Step 2** — Apply undo events in reverse-chronological order (newest first).
//! For each touched FactKey, track presence starting from R1 membership:
//! - Undo assert → set present=false
//! - Undo retract → set present=true (with source for materialization)
//!
//! **Step 3** — Derive exclude/include from final vs starting state:
//! - start=present, end=absent → exclude from R1
//! - start=absent, end=present → include (restore from R3 retract source)
//!
//! **Step 4** — Three-way merge of R1 rows (minus excludes) plus includes,
//! maintaining the leaflet's sort order.
//!
//! ## Correctness
//!
//! Processing ALL events (no first-seen-per-key shortcutting) correctly
//! handles arbitrary assert→retract→reassert chains. `lang_id` and `i`
//! participate in FactKey identity, so language variants and list positions
//! are retracted/restored independently.
//!
//! ## Cost
//!
//! O(R1 + R3_scanned) — linear in leaflet size plus history depth.
//! Close-to-current queries scan few events; deep time-travel scans more
//! but is bounded by leaflet history size.

use super::leaflet::Region3Entry;
use super::run_record::{FactKey, RunSortOrder};
use super::types::RowColumnSlice;
use std::collections::{HashMap, HashSet};

// ============================================================================
// ReplayedLeaflet: output of time-travel replay
// ============================================================================

/// Output of time-travel replay: reconstructed leaflet state at `t_target`.
///
/// Contains columnar data in the same format as decoded Region 1+2,
/// representing the database state as it existed at `t_target`.
pub struct ReplayedLeaflet {
    pub s_ids: Vec<u64>,
    pub p_ids: Vec<u32>,
    pub o_kinds: Vec<u8>,
    pub o_keys: Vec<u64>,
    pub dt_values: Vec<u32>,
    pub t_values: Vec<u32>,
    pub lang_ids: Vec<u16>,
    pub i_values: Vec<i32>,
    pub row_count: usize,
}

// ============================================================================
// Sort-order comparison for Region 3 entries
// ============================================================================

/// Compare two Region3Entry values using the given sort order.
///
/// Compares only the sort-key columns (s_id, p_id, o_kind, o_key, dt) — not t
/// or op, since those are not part of the index ordering.
fn cmp_r3_for_order(a: &Region3Entry, b: &Region3Entry, order: RunSortOrder) -> std::cmp::Ordering {
    match order {
        RunSortOrder::Spot => a
            .s_id
            .cmp(&b.s_id)
            .then(a.p_id.cmp(&b.p_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Psot => a
            .p_id
            .cmp(&b.p_id)
            .then(a.s_id.cmp(&b.s_id))
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt)),
        RunSortOrder::Post => a
            .p_id
            .cmp(&b.p_id)
            .then(a.o_kind.cmp(&b.o_kind))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.s_id.cmp(&b.s_id)),
        RunSortOrder::Opst => a
            .o_kind
            .cmp(&b.o_kind)
            .then(a.o_key.cmp(&b.o_key))
            .then(a.dt.cmp(&b.dt))
            .then(a.p_id.cmp(&b.p_id))
            .then(a.s_id.cmp(&b.s_id)),
    }
}

/// Compare a decoded row (from Region 1+2) against a Region3Entry using
/// the given sort order (sort-key columns only).
///
/// `dt` is `u32` from Region 2 decode output but actual datatype IDs always
/// fit in `u16` (max 65,535 distinct datatype IRIs). The truncation to `u16`
/// here matches `FactKey::from_decoded_row()` and `Region3Entry.dt`.
fn cmp_row_vs_r3(
    s_id: u64,
    p_id: u32,
    o_kind: u8,
    o_key: u64,
    dt: u32,
    entry: &Region3Entry,
    order: RunSortOrder,
) -> std::cmp::Ordering {
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&entry.s_id)
            .then(p_id.cmp(&entry.p_id))
            .then(o_kind.cmp(&entry.o_kind))
            .then(o_key.cmp(&entry.o_key))
            .then((dt as u16).cmp(&entry.dt)),
        RunSortOrder::Psot => p_id
            .cmp(&entry.p_id)
            .then(s_id.cmp(&entry.s_id))
            .then(o_kind.cmp(&entry.o_kind))
            .then(o_key.cmp(&entry.o_key))
            .then((dt as u16).cmp(&entry.dt)),
        RunSortOrder::Post => p_id
            .cmp(&entry.p_id)
            .then(o_kind.cmp(&entry.o_kind))
            .then(o_key.cmp(&entry.o_key))
            .then((dt as u16).cmp(&entry.dt))
            .then(s_id.cmp(&entry.s_id)),
        RunSortOrder::Opst => o_kind
            .cmp(&entry.o_kind)
            .then(o_key.cmp(&entry.o_key))
            .then((dt as u16).cmp(&entry.dt))
            .then(p_id.cmp(&entry.p_id))
            .then(s_id.cmp(&entry.s_id)),
    }
}

// ============================================================================
// replay_leaflet: reverse-play time-travel reconstruction
// ============================================================================

/// Reconstruct the leaflet state at `t_target` using Region 1+2 (current
/// snapshot) and Region 3 (history journal).
///
/// Returns `None` if no changes are needed — meaning no R3 entries have
/// `t > t_target` and no R1 rows have `t > t_target`. The caller can
/// then use the original R1+R2 data directly.
///
/// Collects all events (R3 history + R1 synthetic asserts) with `t > t_target`,
/// then undoes them in reverse-chronological order to derive which facts
/// to exclude from R1 and which retracted facts to restore. See module docs.
///
/// # Arguments
///
/// * `input` — Decoded Region 1+2 columns (current snapshot state)
/// * `region3` — Region 3 entries in reverse chronological order (newest first)
/// * `t_target` — Target time to reconstruct state at
/// * `order` — Sort order of this leaflet (determines merge ordering)
pub fn replay_leaflet(
    input: &RowColumnSlice<'_>,
    region3: &[Region3Entry],
    t_target: i64,
    order: RunSortOrder,
) -> Option<ReplayedLeaflet> {
    let row_count = input.len();

    // ---- Step 0: Build R1 current membership set ----
    //
    // R1 rows represent the current state. Every row is an implicit assert.
    // We need to know which FactKeys are currently present to determine
    // the starting state for undo processing.

    let mut r1_keys: HashMap<FactKey, u32> = HashMap::with_capacity(row_count);
    for idx in 0..row_count {
        let key = FactKey::from_decoded_row(
            input.s[idx],
            input.p[idx],
            input.o_kinds[idx],
            input.o_keys[idx],
            input.dt[idx],
            input.lang[idx],
            input.i[idx],
        );
        r1_keys.insert(key, input.t[idx]);
    }

    // ---- Step 1: Collect all events with t > t_target ----
    //
    // Two sources:
    // 1A) R3 history entries (explicit assert/retract operations)
    // 1B) R1 rows with t > t_target (implicit "assert at row.t" events)
    //
    // All events are processed in reverse-chronological order (t desc)
    // to correctly undo operations back to t_target.

    struct UndoEvent {
        abs_t: u32,
        is_assert: bool,
        key: FactKey,
        /// For R3 retract entries: the source entry for include materialization.
        r3_source: Option<Region3Entry>,
    }

    let mut events: Vec<UndoEvent> = Vec::new();

    // 1A: R3 history entries
    // Track break index so Step 3 can scan R3 below t_target for base assert times.
    let mut r3_break_idx = region3.len();
    for (i, entry) in region3.iter().enumerate() {
        let abs_t = entry.abs_t();
        if abs_t <= t_target as u64 {
            r3_break_idx = i;
            break; // R3 is sorted by abs_t desc
        }
        events.push(UndoEvent {
            abs_t: abs_t as u32,
            is_assert: entry.is_assert(),
            key: FactKey::from_region3(entry),
            r3_source: if entry.is_assert() { None } else { Some(*entry) },
        });
    }

    // 1B: Synthetic assert events from R1's t column
    for idx in 0..row_count {
        if (input.t[idx] as i64) > t_target {
            events.push(UndoEvent {
                abs_t: input.t[idx],
                is_assert: true, // R1 rows are implicit asserts
                key: FactKey::from_decoded_row(
                    input.s[idx],
                    input.p[idx],
                    input.o_kinds[idx],
                    input.o_keys[idx],
                    input.dt[idx],
                    input.lang[idx],
                    input.i[idx],
                ),
                r3_source: None,
            });
        }
    }

    if events.is_empty() {
        return None;
    }

    // Sort by abs_t descending. For same abs_t: assert before retract
    // (undo assert first, then undo retract, for consistent same-t behavior).
    // FactKey tie-breaker ensures deterministic ordering for events at the
    // same (t, op) — required because sort_unstable_by is non-deterministic
    // for ties.
    events.sort_unstable_by(|a, b| {
        b.abs_t
            .cmp(&a.abs_t)
            .then_with(|| b.is_assert.cmp(&a.is_assert))
            .then_with(|| a.key.cmp(&b.key))
    });

    // ---- Step 2: Apply undo events in reverse-chronological order ----
    //
    // For each touched FactKey, track whether it's present or absent after
    // undoing all events. Starting state comes from R1 membership.
    //
    // Undo semantics:
    // - Undo assert → fact becomes absent
    // - Undo retract → fact becomes present (with source for materialization)

    struct KeyState {
        present: bool,
        include_src: Option<Region3Entry>,
    }

    let mut touched: HashMap<FactKey, KeyState> = HashMap::new();

    for event in &events {
        let state = touched.entry(event.key).or_insert_with(|| KeyState {
            present: r1_keys.contains_key(&event.key),
            include_src: None,
        });

        if event.is_assert {
            // Undo assert → fact becomes absent
            state.present = false;
            state.include_src = None;
        } else {
            // Undo retract → fact becomes present
            state.present = true;
            state.include_src = event.r3_source;
        }
    }

    // ---- Step 3: Derive exclude and include sets ----
    //
    // Compare starting state (R1 membership) with final state after undo:
    // - start=present, end=absent → exclude (fact shouldn't exist at t_target)
    // - start=absent, end=present → include (fact should exist at t_target)
    // - same start and end → no change needed

    let mut exclude: HashSet<FactKey> = HashSet::new();
    let mut include: Vec<Region3Entry> = Vec::new();

    for (key, state) in &touched {
        let start_present = r1_keys.contains_key(key);
        match (start_present, state.present) {
            (true, false) => {
                exclude.insert(*key);
            }
            (false, true) => {
                let mut restored = state
                    .include_src
                    .expect("include requires R3 retract source");
                restored.op = 1; // flip retract to assert
                include.push(restored);
            }
            (true, true) => {
                // Fact is present at both start and end of undo chain.
                // If R1's t > t_target, the output t_value would be wrong
                // (it would show the reassert time, not the effective assert
                // time at t_target). Correct by excluding the R1 row and
                // including a version with the base assert time from R3.
                let r1_t = r1_keys[key];
                if (r1_t as i64) > t_target {
                    exclude.insert(*key);

                    // Scan R3 below break for the most recent assert matching
                    // this key — that's the effective assert time at t_target.
                    let base_t = region3[r3_break_idx..]
                        .iter()
                        .find(|e| e.is_assert() && FactKey::from_region3(e) == *key)
                        .map(|e| e.t);

                    let effective_t = base_t.unwrap_or_else(|| {
                        debug_assert!(
                            false,
                            "expected base assert in R3 below break for \
                             (true,true) with R1 t > t_target"
                        );
                        // Fallback: use include_src t if available, else R1 t
                        state.include_src.map(|e| e.t).unwrap_or(r1_t)
                    });

                    include.push(Region3Entry {
                        s_id: key.s_id.as_u64(),
                        p_id: key.p_id.as_u32(),
                        o_kind: key.o.kind.as_u8(),
                        o_key: key.o.key.as_u64(),
                        t: effective_t,
                        op: 1,
                        dt: key.dt.as_u16(),
                        lang_id: key.lang_id.as_u16(),
                        i: key.i.as_i32(),
                    });
                }
                // else: R1 t ≤ t_target — row stays unchanged, no delta.
            }
            (false, false) => {} // absent at both start and end: no delta
        }
    }

    if exclude.is_empty() && include.is_empty() {
        // All undo operations resulted in no net state change (start==end for
        // every touched key). We can only return None if ALL R1 rows have
        // t <= t_target. When R1 contains rows with t > t_target, we must
        // return Some(...) so the binary_cursor defense check doesn't fire
        // on a None result with future-t rows in R1.
        let has_future_rows = (0..row_count).any(|idx| (input.t[idx] as i64) > t_target);
        if !has_future_rows {
            return None;
        }
        // Fall through: produce a ReplayedLeaflet (a copy of R1 in this case)
    }

    // ---- Step 4: Three-way merge ----
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
        o_kinds: Vec::with_capacity(estimated_size),
        o_keys: Vec::with_capacity(estimated_size),
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
            input.s[r1_idx],
            input.p[r1_idx],
            input.o_kinds[r1_idx],
            input.o_keys[r1_idx],
            input.dt[r1_idx],
            input.lang[r1_idx],
            input.i[r1_idx],
        );

        // Check if this R1 row should be excluded
        if exclude.contains(&r1_key) {
            r1_idx += 1;
            continue;
        }

        let cmp = cmp_row_vs_r3(
            input.s[r1_idx],
            input.p[r1_idx],
            input.o_kinds[r1_idx],
            input.o_keys[r1_idx],
            input.dt[r1_idx],
            &include[inc_idx],
            order,
        );

        match cmp {
            std::cmp::Ordering::Less => {
                // R1 row comes first — emit it
                emit_r1_row(&mut out, r1_idx, input);
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
                // — the sort-order comparators only use (s_id, p_id, o_kind, o_key, dt).
                // In this case the R1 row is a surviving current fact and the
                // include entry is a distinct restored fact at the same sort
                // position. Emit R1 first to maintain stable ordering,
                // then the include entry.
                emit_r1_row(&mut out, r1_idx, input);
                emit_include_entry(&mut out, &include[inc_idx]);
                r1_idx += 1;
                inc_idx += 1;
            }
        }
    }

    // Drain remaining R1 rows
    while r1_idx < row_count {
        let r1_key = FactKey::from_decoded_row(
            input.s[r1_idx],
            input.p[r1_idx],
            input.o_kinds[r1_idx],
            input.o_keys[r1_idx],
            input.dt[r1_idx],
            input.lang[r1_idx],
            input.i[r1_idx],
        );
        if !exclude.contains(&r1_key) {
            emit_r1_row(&mut out, r1_idx, input);
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
fn emit_r1_row(out: &mut ReplayedLeaflet, idx: usize, input: &RowColumnSlice<'_>) {
    out.s_ids.push(input.s[idx]);
    out.p_ids.push(input.p[idx]);
    out.o_kinds.push(input.o_kinds[idx]);
    out.o_keys.push(input.o_keys[idx]);
    out.dt_values.push(input.dt[idx]);
    out.t_values.push(input.t[idx]);
    out.lang_ids.push(input.lang[idx]);
    out.i_values.push(input.i[idx]);
}

/// Emit a Region 3 include entry (restored retraction) to the output.
#[inline]
fn emit_include_entry(out: &mut ReplayedLeaflet, entry: &Region3Entry) {
    out.s_ids.push(entry.s_id);
    out.p_ids.push(entry.p_id);
    out.o_kinds.push(entry.o_kind);
    out.o_keys.push(entry.o_key);
    out.dt_values.push(entry.dt as u32);
    out.t_values.push(entry.t); // positive (restored assert)
    out.lang_ids.push(entry.lang_id);
    out.i_values.push(entry.i);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ListIndex;

    /// Helper: build a Region3Entry.
    ///
    /// Accepts `t_signed: i64` for test ergonomics: positive = assert, negative = retract.
    /// Internally maps to `t: u32` + `op: u8` (1=assert, 0=retract).
    fn r3(s_id: u64, p_id: u32, o_kind: u8, o_key: u64, t_signed: i64, dt: u16) -> Region3Entry {
        Region3Entry {
            s_id,
            p_id,
            o_kind,
            o_key,
            t: t_signed.unsigned_abs() as u32,
            op: if t_signed >= 0 { 1 } else { 0 },
            dt,
            lang_id: 0,
            i: ListIndex::none().as_i32(),
        }
    }

    /// Helper macro to create RowColumnSlice from arrays for tests.
    macro_rules! row_slice {
        ($s:expr, $p:expr, $ok:expr, $okey:expr, $dt:expr, $t:expr, $lang:expr, $i:expr) => {
            RowColumnSlice {
                s: $s,
                p: $p,
                o_kinds: $ok,
                o_keys: $okey,
                dt: $dt,
                t: $t,
                lang: $lang,
                i: $i,
            }
        };
    }

    #[test]
    fn test_replay_no_changes_returns_none() {
        // Region 3 is empty and all R1 rows have t <= t_target — no replay needed
        let input = row_slice!(
            &[1u64, 2],
            &[10u32, 10],
            &[0u8, 0],
            &[100u64, 200],
            &[1u32, 1],
            &[3u32, 3],
            &[0u16, 0],
            &[ListIndex::none().as_i32(), ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &[], 5, RunSortOrder::Spot);
        assert!(result.is_none());
    }

    #[test]
    fn test_replay_all_entries_before_target_returns_none() {
        // All R3 entries are at or before t_target
        let r3_entries = vec![
            r3(1, 10, 0, 100, 3, 1), // assert at t=3
            r3(2, 10, 0, 200, 2, 1), // assert at t=2
        ];

        let input = row_slice!(
            &[1u64, 2],
            &[10u32, 10],
            &[0u8, 0],
            &[100u64, 200],
            &[1u32, 1],
            &[3u32, 2],
            &[0u16, 0],
            &[ListIndex::none().as_i32(), ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        assert!(result.is_none());
    }

    #[test]
    fn test_replay_exclude_later_assert() {
        // Current state: s=1 p=10 o=100 (asserted at t=5)
        // Region 3 is EMPTY (single-version fact — no history)
        // Synthetic event: undo assert at t=5 → present=false. Exclude.
        // Expected: s=1 p=10 o=100 should be excluded at t=3

        let input = row_slice!(
            &[1u64],
            &[10u32],
            &[0u8],
            &[100u64],
            &[1u32],
            &[5u32],
            &[0u16],
            &[ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &[], 3, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(
            out.row_count, 0,
            "fact asserted at t=5 should not exist at t=3"
        );
    }

    #[test]
    fn test_replay_include_later_retract() {
        // Current state: (empty — the fact was retracted)
        // Region 3: retract at t=5 (retraction happened after t_target=3)
        // Expected: fact should be restored at t=3

        let r3_entries = vec![
            r3(1, 10, 0, 100, -5, 1), // retract at t=5 (negative = retract)
        ];

        let empty: &[u64] = &[];
        let empty32: &[u32] = &[];
        let empty8: &[u8] = &[];
        let empty16: &[u16] = &[];
        let emptyi: &[i32] = &[];
        let emptyt: &[u32] = &[];
        let input = row_slice!(empty, empty32, empty8, empty, empty32, emptyt, empty16, emptyi);
        let result = replay_leaflet(&input, &r3_entries, 3, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 1, "retracted fact should be restored at t=3");
        assert_eq!(out.s_ids[0], 1);
        assert_eq!(out.p_ids[0], 10);
        assert_eq!(out.o_keys[0], 100);
        assert!(out.t_values[0] > 0, "restored fact should have positive t");
    }

    #[test]
    fn test_replay_mixed_exclude_and_include() {
        // Current state at max_t=10:
        //   s=1 p=10 o=100 (survived — existed before and after, t=3)
        //   s=2 p=10 o=200 (asserted at t=8, single-version)
        //   s=4 p=10 o=400 (survived — existed before and after, t=3)
        //
        // Region 3: s=3 retract at t=7
        //
        // Events at t_target=5:
        //   s=2: undo synthetic assert t=8 → present=false. Exclude.
        //   s=3: undo R3 retract t=7 → present=true. Include (restore).
        //
        // Expected at t=5: s=1, s=3, s=4

        let r3_entries = vec![
            r3(3, 10, 0, 300, -7, 1), // retract at t=7 → include
        ];

        let input = row_slice!(
            &[1u64, 2, 4],
            &[10u32, 10, 10],
            &[0u8, 0, 0],
            &[100u64, 200, 400],
            &[1u32, 1, 1],
            &[3u32, 8, 3],
            &[0u16, 0, 0],
            &[
                ListIndex::none().as_i32(),
                ListIndex::none().as_i32(),
                ListIndex::none().as_i32()
            ]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 3, 4]);
        assert_eq!(out.o_keys, vec![100, 300, 400]);
    }

    #[test]
    fn test_replay_retract_undo_restores_fact() {
        // Fact history: asserted at t=3, retracted at t=7, reasserted at t=10.
        // R1: fact present at t=10 (current state).
        // R3 (history): retract at t=7, assert at t=3.
        //
        // At t_target=5:
        //   Events (t desc): synthetic assert t=10, R3 retract t=7
        //   Undo t=10 assert → present=false
        //   Undo t=7 retract → present=true
        //   Final: start=true, end=true, R1 t=10 > 5 → exclude R1, include
        //   with base assert t=3 from R3 below break.
        //   Result: fact present with t=3. Correct.

        let r3_entries = vec![
            r3(1, 10, 0, 100, -7, 1), // retract at t=7
            r3(1, 10, 0, 100, 3, 1),  // assert at t=3 (original)
        ];

        let input = row_slice!(
            &[1u64], // fact is in current state (reasserted at t=10)
            &[10u32],
            &[0u8],
            &[100u64],
            &[1u32],
            &[10u32],
            &[0u16],
            &[ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 1);
        assert_eq!(out.s_ids[0], 1);
        assert_eq!(
            out.t_values[0], 3,
            "effective assert time at t_target=5 should be t=3, not t=10"
        );
    }

    #[test]
    fn test_replay_preserves_sort_order_psot() {
        // Test with PSOT order to verify sort-order generic merge.
        // Include entries should be interleaved correctly by PSOT order.

        // Current state: p=10 s=1, p=20 s=3
        // Include: p=10 s=2 (from retract at t=8)
        // Expected PSOT order: p=10 s=1, p=10 s=2, p=20 s=3

        let r3_entries = vec![
            r3(2, 10, 0, 200, -8, 1), // retract at t=8 → include
        ];

        let input = row_slice!(
            &[1u64, 3],   // current s_ids (PSOT order: p=10 s=1, p=20 s=3)
            &[10u32, 20], // p_ids
            &[0u8, 0],
            &[100u64, 300],
            &[1u32, 1],
            &[3u32, 3],
            &[0u16, 0],
            &[ListIndex::none().as_i32(), ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Psot);
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
            r3(1, 10, 0, 100, -8, 1), // retract at t=8 → include
            r3(5, 10, 0, 500, -7, 1), // retract at t=7 → include
        ];

        let input = row_slice!(
            &[3u64],
            &[10u32],
            &[0u8],
            &[300u64],
            &[1u32],
            &[3u32],
            &[0u16],
            &[ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 3);
        assert_eq!(out.s_ids, vec![1, 3, 5]);
    }

    #[test]
    fn test_replay_all_excluded() {
        // All current rows have t > t_target — undo of synthetic asserts
        // makes all facts absent. R3 is empty (single-version facts).

        let input = row_slice!(
            &[1u64, 2],
            &[10u32, 10],
            &[0u8, 0],
            &[100u64, 200],
            &[1u32, 1],
            &[8u32, 7],
            &[0u16, 0],
            &[ListIndex::none().as_i32(), ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &[], 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 0);
    }

    #[test]
    fn test_replay_boundary_t_equals_target() {
        // Entry at exactly t_target should NOT be undone.
        // Synthetic events only for t > t_target (strict), not t == t_target.
        //
        // R3 is empty (no historical entries — both facts are single-version).
        // s=2 has t=8 > 5 → undo assert → excluded.
        // s=1 has t=5 == 5 → no event → kept.

        let r3_entries: Vec<Region3Entry> = vec![];

        let input = row_slice!(
            &[1u64, 2],
            &[10u32, 10],
            &[0u8, 0],
            &[100u64, 200],
            &[1u32, 1],
            &[5u32, 8],
            &[0u16, 0],
            &[ListIndex::none().as_i32(), ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 1);
        assert_eq!(out.s_ids, vec![1]); // s=1 survives (t=5 is at boundary)
    }

    #[test]
    fn test_replay_multi_transaction_assert_retract_assert() {
        // Same fact goes through: assert t=3 → retract t=5 → reassert t=8
        //
        // Region 3 (history only, no current-state assert):
        //   retract t=5, assert t=3
        // R1: fact is present at t=8 (current state)
        //
        // At t_target=6:
        //   Events: synthetic assert t=8 (only event; R3 retract t=5 ≤ 6)
        //   Undo t=8 assert → present=false
        //   Final: start=true, end=false → exclude. row_count=0
        //   Correct — at t=6, retraction at t=5 already happened.
        //
        // At t_target=4:
        //   Events (t desc): synthetic assert t=8, R3 retract t=5
        //   Undo t=8 assert → present=false
        //   Undo t=5 retract → present=true
        //   Final: start=true, end=true, R1 t=8 > 4 → exclude R1, include
        //   with base assert t=3 from R3 below break. row_count=1, t=3.
        //   Correct — at t=4, original assert at t=3 was still active.
        //
        // At t_target=2:
        //   Events (t desc): synthetic assert t=8, R3 retract t=5, R3 assert t=3
        //   Undo t=8 assert → present=false
        //   Undo t=5 retract → present=true
        //   Undo t=3 assert → present=false
        //   Final: start=true, end=false → exclude. row_count=0
        //   Correct — at t=2, the fact didn't exist yet (asserted at t=3).

        let r3_entries = vec![
            r3(1, 10, 0, 100, -5, 1), // retract at t=5
            r3(1, 10, 0, 100, 3, 1),  // assert at t=3 (oldest)
        ];

        // Current R1: fact is present (reasserted at t=8)
        let input = row_slice!(
            &[1u64],
            &[10u32],
            &[0u8],
            &[100u64],
            &[1u32],
            &[8u32],
            &[0u16],
            &[ListIndex::none().as_i32()]
        );

        // t_target=6: fact was retracted at t=5
        let result_t6 = replay_leaflet(&input, &r3_entries, 6, RunSortOrder::Spot);
        let out = result_t6.expect("should produce replay");
        assert_eq!(
            out.row_count, 0,
            "at t=6, fact was retracted (retract at t=5)"
        );

        // t_target=4: fact was alive (original assert at t=3)
        let result_t4 = replay_leaflet(&input, &r3_entries, 4, RunSortOrder::Spot);
        let out = result_t4.expect("should produce replay");
        assert_eq!(
            out.row_count, 1,
            "at t=4, fact was alive (original assert at t=3, retract at t=5 not yet)"
        );
        assert_eq!(out.s_ids, vec![1]);
        assert_eq!(
            out.t_values[0], 3,
            "effective assert time at t_target=4 should be t=3, not t=8"
        );

        // t_target=2: fact didn't exist yet (asserted at t=3)
        let result_t2 = replay_leaflet(&input, &r3_entries, 2, RunSortOrder::Spot);
        let out = result_t2.expect("should produce replay");
        assert_eq!(
            out.row_count, 0,
            "at t=2, fact didn't exist yet (first asserted at t=3)"
        );
    }

    #[test]
    fn test_replay_retract_then_reassert_restore() {
        // Fact asserted at t=2, retracted at t=5, then reasserted at t=8.
        // Current state: present (reasserted at t=8).
        //
        // Region 3 (history): retract t=5, assert t=2
        // R1: fact present at t=8
        //
        // At t_target=4:
        //   Events (t desc): synthetic assert t=8, R3 retract t=5
        //   Undo t=8 assert → present=false
        //   Undo t=5 retract → present=true
        //   Final: start=true, end=true, R1 t=8 > 4 → exclude R1, include
        //   with base assert t=2 from R3 below break.
        //   Result: row_count=1, t=2. Correct — at t=4 the fact existed
        //   (original assert at t=2, retraction at t=5 not yet happened).

        let r3_entries = vec![
            r3(1, 10, 0, 100, -5, 1), // retract at t=5
            r3(1, 10, 0, 100, 2, 1),  // assert at t=2 (original)
        ];

        let input = row_slice!(
            &[1u64],
            &[10u32],
            &[0u8],
            &[100u64],
            &[1u32],
            &[8u32],
            &[0u16],
            &[ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(&input, &r3_entries, 4, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(
            out.row_count, 1,
            "at t=4, fact existed (retraction at t=5 undone, restoring fact)"
        );
        assert_eq!(out.s_ids, vec![1]);
        assert_eq!(
            out.t_values[0], 2,
            "effective assert time at t_target=4 should be t=2, not t=8"
        );
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

        let r3_entries = vec![Region3Entry {
            s_id: 1,
            p_id: 10,
            o_kind: 0,
            o_key: 100,
            t: 8,       // retract at t=8
            op: 0,      // retract
            dt: 11,     // LANG_STRING
            lang_id: 2, // French
            i: ListIndex::none().as_i32(),
        }];

        let input = row_slice!(
            &[1u64],   // s_ids
            &[10u32],  // p_ids
            &[0u8],    // o_kinds
            &[100u64], // o_keys
            &[11u32],  // dt (LANG_STRING)
            &[3u32],   // t
            &[1u16],   // lang_ids (English)
            &[ListIndex::none().as_i32()]
        );
        let result = replay_leaflet(
            &input,
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
        // Region 3 (history only): s=2 retract at t=9, s=4 retract at t=8
        // R1 rows: s=1 (t=6), s=3 (t=7), s=5 (t=10)
        //
        // At t_target=5:
        //   s=1: undo synthetic assert t=6 → exclude (start=true, end=false)
        //   s=2: undo R3 retract t=9 → include (start=false, end=true)
        //   s=3: undo synthetic assert t=7 → exclude
        //   s=4: undo R3 retract t=8 → include
        //   s=5: undo synthetic assert t=10 → exclude
        //
        // Expected: s=2, s=4 (in SPOT order)

        let r3_entries = vec![
            r3(2, 10, 0, 200, -9, 1), // retract at t=9
            r3(4, 10, 0, 400, -8, 1), // retract at t=8
        ];

        let input = row_slice!(
            &[1u64, 3, 5], // current R1 s_ids
            &[10u32, 10, 10],
            &[0u8, 0, 0],
            &[100u64, 300, 500],
            &[1u32, 1, 1],
            &[6u32, 7, 10], // t values
            &[0u16, 0, 0],
            &[
                ListIndex::none().as_i32(),
                ListIndex::none().as_i32(),
                ListIndex::none().as_i32()
            ]
        );
        let result = replay_leaflet(&input, &r3_entries, 5, RunSortOrder::Spot);
        let out = result.expect("should produce replay");
        assert_eq!(out.row_count, 2);
        assert_eq!(out.s_ids, vec![2, 4]);
        assert_eq!(out.o_keys, vec![200, 400]);
    }
}
