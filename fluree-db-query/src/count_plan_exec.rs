//! Count-only plan executor — evaluates a `CountPlan` against a `BinaryIndexStore`.
//!
//! The executor wraps as a `FastPathOperator` closure. During `open()`:
//! 1. Call `fast_path_store(ctx)` — return `Ok(None)` if not binary-index (triggers fallback)
//! 2. Resolve all `Ref` predicates to `p_id`s
//! 3. Recursively evaluate the plan tree using existing iterator primitives
//! 4. Return single count batch
//!
//! See `count_plan.rs` for the IR definition and planner.

use crate::count_plan::{
    ChainFold, CountPlan, CountPlanRoot, KeySetNode, ScalarNode, StreamNode, TailWeight,
};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_for_predicate_sorted,
    collect_subjects_with_object_in_set, count_rows_for_predicate_psot, fast_path_store,
    intersect_many_sorted, normalize_pred_sid, FastPathOperator, PostObjectGroupCountIter,
    PsotSubjectCountIter, PsotSubjectWeightedSumIter,
};
use crate::operator::BoxedOperator;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;

/// Create a `FastPathOperator` that executes a `CountPlan`.
pub(crate) fn count_plan_operator(plan: CountPlan, fallback: Option<BoxedOperator>) -> BoxedOperator {
    let out_var = plan.out_var;
    Box::new(FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let g_id = ctx.binary_g_id;

            match execute_plan(&plan.root, store, g_id)? {
                Some(count) => {
                    let count_i64 = i64::try_from(count).map_err(|_| {
                        QueryError::execution("COUNT(*) exceeds i64 in count plan")
                    })?;
                    Ok(Some(build_count_batch(out_var, count_i64)?))
                }
                None => Ok(None), // Fall through to general pipeline.
            }
        },
        fallback,
        "count-plan",
    ))
}

// ===========================================================================
// Plan evaluation
// ===========================================================================

fn execute_plan(
    root: &CountPlanRoot,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    match root {
        CountPlanRoot::Scalar(scalar) => execute_scalar(scalar, store, g_id),
        CountPlanRoot::Chain(chain) => execute_chain(chain, store, g_id),
    }
}

// ---------------------------------------------------------------------------
// Scalar evaluation
// ---------------------------------------------------------------------------

fn execute_scalar(
    node: &ScalarNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    match node {
        ScalarNode::TotalRowCount { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            Ok(Some(count_rows_for_predicate_psot(store, g_id, p_id)?))
        }

        ScalarNode::Sum { source } => {
            let total = sum_stream(source, store, g_id, None, None)?;
            Ok(total)
        }

        ScalarNode::SumExcluding { source, excluded } => {
            let exclude_sorted = execute_keyset_as_sorted(excluded, store, g_id)?;
            let exclude_sorted = match exclude_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            let total = sum_stream(source, store, g_id, Some(&exclude_sorted), None)?;
            Ok(total)
        }

        ScalarNode::SumFiltered { source, filter } => {
            let filter_sorted = execute_keyset_as_sorted(filter, store, g_id)?;
            let filter_sorted = match filter_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            let total = sum_stream(source, store, g_id, None, Some(&filter_sorted))?;
            Ok(total)
        }
    }
}

// ---------------------------------------------------------------------------
// Stream evaluation — produces (key, count) pairs, summed with optional filters
// ---------------------------------------------------------------------------

/// Sum all `(key, count)` from a stream, optionally filtering by exclude/include sets.
///
/// For MINUS/EXISTS modifiers on star/single-triple shapes, we pre-compute a sorted
/// exclusion or inclusion list and use a running index pointer for O(1) amortized
/// per-subject filtering (matching `fast_minus_count_all.rs` and `fast_exists_count_all.rs`).
fn sum_stream(
    node: &StreamNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    match node {
        StreamNode::SubjectCountScan { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            let mut iter = PsotSubjectCountIter::new(store, g_id, p_id)?;
            let mut excl_idx: usize = 0;
            let mut incl_idx: usize = 0;
            let mut total: u128 = 0;
            while let Some((s, count)) = iter.next_group()? {
                if is_excluded(s, exclude_sorted, &mut excl_idx) {
                    continue;
                }
                if !is_included(s, include_sorted, &mut incl_idx) {
                    continue;
                }
                total = total.saturating_add(count as u128);
            }
            Ok(Some(total.min(u64::MAX as u128) as u64))
        }

        StreamNode::StarJoin { children } => {
            sum_star_join(children, store, g_id, exclude_sorted, include_sorted)
        }

        StreamNode::OptionalJoin {
            required,
            optional_groups,
        } => sum_optional_join(
            required,
            optional_groups,
            store,
            g_id,
            exclude_sorted,
            include_sorted,
        ),

        StreamNode::AntiJoin { source, excluded } => {
            let exclude_list = execute_keyset_as_sorted(excluded, store, g_id)?;
            let exclude_list = match exclude_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Merge with any existing sorted exclusion list.
            let merged = merge_sorted_lists(exclude_sorted, &exclude_list);
            sum_stream(source, store, g_id, Some(&merged), include_sorted)
        }

        StreamNode::SemiJoin { source, filter } => {
            let filter_list = execute_keyset_as_sorted(filter, store, g_id)?;
            let filter_list = match filter_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Intersect with any existing sorted inclusion list.
            let merged = match include_sorted {
                Some(existing) => intersect_sorted_pair(existing, &filter_list),
                None => filter_list,
            };
            sum_stream(source, store, g_id, exclude_sorted, Some(&merged))
        }
    }
}

/// Check if `key` is in the sorted exclusion list, advancing the index pointer.
#[inline]
fn is_excluded(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return false };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Check if `key` is in the sorted inclusion list, advancing the index pointer.
/// Returns true if there is no inclusion list (no filter) or key is present.
#[inline]
fn is_included(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return true };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Merge two sorted lists into a deduplicated sorted union.
fn merge_sorted_lists(existing: Option<&[u64]>, new: &[u64]) -> Vec<u64> {
    let Some(existing) = existing else {
        return new.to_vec();
    };
    let mut result = Vec::with_capacity(existing.len() + new.len());
    let (mut i, mut j) = (0, 0);
    while i < existing.len() && j < new.len() {
        if existing[i] < new[j] {
            result.push(existing[i]);
            i += 1;
        } else if existing[i] > new[j] {
            result.push(new[j]);
            j += 1;
        } else {
            result.push(existing[i]);
            i += 1;
            j += 1;
        }
    }
    result.extend_from_slice(&existing[i..]);
    result.extend_from_slice(&new[j..]);
    result
}

/// Intersect two sorted lists into a sorted intersection.
fn intersect_sorted_pair(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            i += 1;
        } else if a[i] > b[j] {
            j += 1;
        } else {
            result.push(a[i]);
            i += 1;
            j += 1;
        }
    }
    result
}

/// N-way merge-join on subject count iterators, multiplying counts per key.
///
/// Uses sorted-list-based exclusion/inclusion with running index pointers for O(1)
/// amortized per-subject filtering (matching `fast_minus_count_all::count_property_join_all`).
///
/// Formula: `Σ_{s in all, not excluded, included} Π_i count_i(s)`
fn sum_star_join(
    children: &[StreamNode],
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // All children must be SubjectCountScan for the streaming N-way merge.
    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(children.len());
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(store, pred)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            return Ok(Some(0));
        };
        iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(|c| c.is_none()) {
            break;
        }

        let max_s = curr
            .iter()
            .filter_map(|c| c.map(|(s, _)| s))
            .max()
            .unwrap();

        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                total = total.saturating_add(product);
            }

            for (i, it) in iters.iter_mut().enumerate() {
                curr[i] = it.next_group()?;
            }
        } else {
            for (i, it) in iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = curr[i] {
                    if s_id < max_s {
                        curr[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

/// Fully streaming merge-join with OPTIONAL semantics.
///
/// Interleaves optional group cursor advancement with the required N-way merge,
/// matching the algorithm in `PropertyJoinCountAllOperator::count_psot_iters`.
/// No HashMap materialization for required or optional streams.
///
/// Formula: `Σ_s req_product(s) × Π_g max(1, Π_i opt_gi(s))`
fn sum_optional_join(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // Collect required iterators (single scan or star join children).
    let mut req_iters: Vec<PsotSubjectCountIter<'_>> = Vec::new();
    match required {
        StreamNode::SubjectCountScan { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(0));
            };
            req_iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
        }
        StreamNode::StarJoin { children } => {
            for child in children {
                let StreamNode::SubjectCountScan { pred } = child else {
                    return Ok(None);
                };
                let sid = normalize_pred_sid(store, pred)?;
                let Some(p_id) = store.sid_to_p_id(&sid) else {
                    return Ok(Some(0));
                };
                req_iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
            }
        }
        _ => return Ok(None),
    }

    // Optional groups: each group is a same-subject star; multiplier is max(1, Π counts).
    // An optional predicate that is absent in the store makes the entire group `always_one`.
    struct OptGroup<'a> {
        always_one: bool,
        iters: Vec<PsotSubjectCountIter<'a>>,
        cur: Vec<Option<(u64, u64)>>,
    }

    let mut opt_groups: Vec<OptGroup<'_>> = Vec::with_capacity(optional_groups.len());
    for grp in optional_groups {
        let mut always_one = false;
        let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(grp.len());
        for node in grp {
            let StreamNode::SubjectCountScan { pred } = node else {
                return Ok(None);
            };
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                // Absent optional predicate => group never matches => multiplier 1.
                always_one = true;
                iters.clear();
                break;
            };
            iters.push(PsotSubjectCountIter::new(store, g_id, p_id)?);
        }
        let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
        for it in &mut iters {
            cur.push(it.next_group()?);
        }
        opt_groups.push(OptGroup {
            always_one,
            iters,
            cur,
        });
    }

    // Prime required cursors.
    let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_iters.len());
    for it in &mut req_iters {
        req_cur.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if req_cur.iter().any(|c| c.is_none()) {
            break;
        }

        let max_s = req_cur
            .iter()
            .filter_map(|c| c.map(|(s, _)| s))
            .max()
            .unwrap();

        if req_cur.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                // Required product at this subject.
                let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();

                // Multiply OPTIONAL group factors for this subject (streaming).
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    let mut g_prod: u128 = 1;
                    for i in 0..g.iters.len() {
                        // Advance optional cursor to >= max_s.
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        let c = match g.cur[i] {
                            Some((sid2, c)) if sid2 == max_s => {
                                g.cur[i] = g.iters[i].next_group()?;
                                c
                            }
                            _ => 0u64,
                        };
                        if c == 0 {
                            g_prod = 0;
                            break;
                        }
                        g_prod = g_prod.saturating_mul(c as u128);
                    }
                    let mult = if g_prod == 0 { 1u128 } else { g_prod };
                    product = product.saturating_mul(mult);
                }

                total = total.saturating_add(product);
            } else {
                // Still need to advance optional cursors past this subject
                // to keep them in sync even when the required subject is skipped.
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    for i in 0..g.iters.len() {
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        if let Some((sid2, _)) = g.cur[i] {
                            if sid2 == max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                            }
                        }
                    }
                }
            }

            // Advance required iterators.
            for (i, it) in req_iters.iter_mut().enumerate() {
                req_cur[i] = it.next_group()?;
            }
        } else {
            // Advance smaller required subjects up to the current max.
            for (i, it) in req_iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = req_cur[i] {
                    if s_id < max_s {
                        req_cur[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

// ---------------------------------------------------------------------------
// KeySet evaluation — produces materialized sorted lists or hash sets
// ---------------------------------------------------------------------------

/// Primary keyset evaluator: returns a sorted `Vec<u64>`.
///
/// All callers in Phase B use sorted lists for streaming merge-skip/merge-keep.
fn execute_keyset_as_sorted(
    node: &KeySetNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<Vec<u64>>> {
    match node {
        KeySetNode::SubjectsSorted { pred } | KeySetNode::SubjectSet { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(Vec::new()));
            };
            Ok(Some(collect_subjects_for_predicate_sorted(
                store, g_id, p_id,
            )?))
        }
        KeySetNode::SubjectsWithObjectIn { pred, object_set } => {
            // Need a hash set for the object filter, then sort the result.
            let obj_set = execute_keyset_as_hash_set(object_set, store, g_id)?;
            let obj_set = match obj_set {
                Some(s) => s,
                None => return Ok(None),
            };
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(Vec::new()));
            };
            let Some(mut subjects) =
                collect_subjects_with_object_in_set(store, g_id, p_id, &obj_set)?
            else {
                return Ok(None);
            };
            subjects.sort_unstable();
            subjects.dedup();
            Ok(Some(subjects))
        }
        KeySetNode::IntersectSorted { children } => {
            let mut lists: Vec<Vec<u64>> = Vec::with_capacity(children.len());
            for child in children {
                let sorted = execute_keyset_as_sorted(child, store, g_id)?;
                let sorted = match sorted {
                    Some(s) => s,
                    None => return Ok(None),
                };
                lists.push(sorted);
            }
            Ok(Some(intersect_many_sorted(lists)))
        }
    }
}

/// Hash-set evaluator for keysets — used only by `SubjectsWithObjectIn` which
/// needs `collect_subjects_with_object_in_set(&FxHashSet)`.
fn execute_keyset_as_hash_set(
    node: &KeySetNode,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<FxHashSet<u64>>> {
    match node {
        KeySetNode::SubjectSet { pred } => {
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(FxHashSet::default()));
            };
            Ok(Some(collect_subjects_for_predicate_set(store, g_id, p_id)?))
        }
        _ => {
            // Fall back: get sorted, convert to set.
            let sorted = execute_keyset_as_sorted(node, store, g_id)?;
            match sorted {
                Some(v) => Ok(Some(v.into_iter().collect())),
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Chain evaluation
// ---------------------------------------------------------------------------

fn execute_chain(
    chain: &ChainFold,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    assert!(
        chain.predicates.len() >= 2,
        "chain must have at least 2 predicates"
    );

    let n = chain.predicates.len();

    // Resolve all predicate Refs to p_ids.
    let mut p_ids: Vec<u32> = Vec::with_capacity(n);
    for pred in &chain.predicates {
        let sid = normalize_pred_sid(store, pred)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            // Missing predicate in an inner join chain → 0.
            return Ok(Some(0));
        };
        p_ids.push(p_id);
    }

    // Step 1: Build initial weights from the rightmost predicate.
    let mut weights: FxHashMap<u64, u64> = FxHashMap::default();
    {
        let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
        while let Some((s, count)) = iter.next_group()? {
            if count > 0 {
                weights.insert(s, count);
            }
        }
    }

    // Apply tail weight modifier.
    match &chain.tail_weight {
        TailWeight::None => {}
        TailWeight::Optional { tail_pred } => {
            let sid = normalize_pred_sid(store, tail_pred)?;
            if let Some(p_id) = store.sid_to_p_id(&sid) {
                let mut tail_counts: FxHashMap<u64, u64> = FxHashMap::default();
                let mut iter = PsotSubjectCountIter::new(store, g_id, p_id)?;
                while let Some((s, count)) = iter.next_group()? {
                    if count > 0 {
                        tail_counts.insert(s, count);
                    }
                }
                // Multiply each weight by max(1, tail_count).
                // Also add weight entries for subjects not in weights but in tail_counts
                // (OPTIONAL semantics: if the chain tail has no match, weight = 1).
                for (s, w) in weights.iter_mut() {
                    let tc = tail_counts.get(s).copied().unwrap_or(0);
                    let factor = if tc == 0 { 1 } else { tc };
                    *w = w.saturating_mul(factor);
                }
            }
            // If tail predicate doesn't exist, all weights stay as-is (OPTIONAL: factor = 1).
        }
        TailWeight::Minus { tail_pred } => {
            let sid = normalize_pred_sid(store, tail_pred)?;
            if let Some(p_id) = store.sid_to_p_id(&sid) {
                let excluded = collect_subjects_for_predicate_set(store, g_id, p_id)?;
                weights.retain(|k, _| !excluded.contains(k));
            }
            // If tail predicate doesn't exist, nothing to exclude.
        }
        TailWeight::Exists { tail_pred } => {
            let sid = normalize_pred_sid(store, tail_pred)?;
            if let Some(p_id) = store.sid_to_p_id(&sid) {
                let included = collect_subjects_for_predicate_set(store, g_id, p_id)?;
                weights.retain(|k, _| included.contains(k));
            } else {
                // EXISTS with missing predicate → no matches → 0.
                return Ok(Some(0));
            }
        }
    }

    if weights.is_empty() {
        return Ok(Some(0));
    }

    // Step 2: Fold right-to-left through intermediate predicates (indices n-2 down to 1).
    for i in (1..n - 1).rev() {
        let Some(mut iter) =
            PsotSubjectWeightedSumIter::new(store, g_id, p_ids[i], &weights, 0)?
        else {
            return Ok(None);
        };
        let mut new_weights: FxHashMap<u64, u64> = FxHashMap::default();
        while let Some((b, sum)) = iter.next_group()? {
            if sum > 0 {
                new_weights.insert(b, sum);
            }
        }
        if new_weights.is_empty() {
            return Ok(Some(0));
        }
        weights = new_weights;
    }

    // Step 3: Final merge — stream POST(p1) grouped by object, multiply by weights.
    let Some(mut it1) = PostObjectGroupCountIter::new(store, g_id, p_ids[0])? else {
        return Ok(None);
    };

    let mut total: u64 = 0;
    while let Some((b, n1)) = it1.next_group()? {
        if let Some(&w) = weights.get(&b) {
            total = total.saturating_add(n1.saturating_mul(w));
        }
    }

    Ok(Some(total))
}
