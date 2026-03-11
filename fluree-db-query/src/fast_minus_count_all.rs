//! Consolidated fast paths for `COUNT(*)` queries with `MINUS` constraints.
//!
//! These operators target benchmark-style queries where a `COUNT(*)` aggregate is
//! applied over a basic graph pattern with a `MINUS { ... }` constraint and no
//! other modifiers (GROUP BY, ORDER BY, DISTINCT, etc.).
//!
//! All MINUS shapes reduce to "compute exclusion set, count non-excluded outer rows"
//! using metadata-light PSOT/POST scans and merge-joins on encoded IDs.
//!
//! All operators are wrapped as `FastPathOperator`s and fall back safely when
//! the required binary-store context isn't available (history/policy/overlay/etc).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_for_predicate_sorted,
    collect_subjects_with_object_in_set, count_rows_for_predicate_psot,
    count_rows_psot_for_subjects_sorted, fast_path_store, intersect_many_sorted,
    normalize_pred_sid, sum_post_object_counts_filtered, FastPathOperator, ObjectFilterMode,
    PostObjectGroupCountIter, PsotObjectFilterCountIter, PsotSubjectCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

// ---------------------------------------------------------------------------
// Shape 1: single outer triple + MINUS(star on same subject)
//
// `?s <p_outer> ?o . MINUS { ?s <p1> ?o1 . ?s <p2> ?o2 . ... }`
//
// Algorithm: total_rows(p_outer) - rows_for_subjects_in(S_minus)
// where S_minus = intersection of subject sets for each MINUS predicate.
// ---------------------------------------------------------------------------

pub fn minus_join_count_all_operator(
    outer_predicate: Ref,
    minus_predicates: Vec<Ref>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_minus_outer_rows_psot(
                store,
                ctx.binary_g_id,
                &outer_predicate,
                &minus_predicates,
            )?;
            let n_i64 = i64::try_from(count).unwrap_or(i64::MAX);
            Ok(Some(build_count_batch(out_var, n_i64)?))
        },
        fallback,
        "MINUS COUNT(*)",
    )
}

fn count_minus_outer_rows_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicate: &Ref,
    minus_predicates: &[Ref],
) -> Result<u64> {
    let outer_sid = normalize_pred_sid(store, outer_predicate)?;
    let Some(p_outer) = store.sid_to_p_id(&outer_sid) else {
        return Ok(0);
    };

    let total = count_rows_for_predicate_psot(store, g_id, p_outer)?;

    if minus_predicates.is_empty() {
        return Ok(total);
    }

    let mut subject_lists: Vec<Vec<u64>> = Vec::with_capacity(minus_predicates.len());
    for p in minus_predicates {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(total);
        };
        subject_lists.push(collect_subjects_for_predicate_sorted(store, g_id, pid)?);
    }

    let subjects = intersect_many_sorted(subject_lists);
    if subjects.is_empty() {
        return Ok(total);
    }

    let removed = count_rows_psot_for_subjects_sorted(store, g_id, p_outer, &subjects)?;
    Ok(total.saturating_sub(removed))
}

// ---------------------------------------------------------------------------
// Shape 2: 2-hop chain + MINUS on tail
//
// `?a <p1> ?b . ?b <p2> ?c . MINUS { ?c <p3> ?d . }`
//
// Algorithm: merge-join (b, count) streams from p1→objects and p2→subjects,
// filtering p2's objects to exclude those in S3 = subjects(p3).
// Answer = Σ_b count_p1(b) × count_p2_filtered(b)
// ---------------------------------------------------------------------------

pub fn chain_minus_count_all_operator(
    p1: Ref,
    p2: Ref,
    p3_minus: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_chain_minus(store, ctx.binary_g_id, &p1, &p2, &p3_minus)? {
                Some(count) => {
                    let n_i64 = i64::try_from(count).unwrap_or(i64::MAX);
                    Ok(Some(build_count_batch(out_var, n_i64)?))
                }
                None => Ok(None),
            }
        },
        fallback,
        "chain+minus COUNT(*)",
    )
}

fn count_chain_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_minus: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3_minus)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        // If the minus predicate doesn't exist, it removes nothing (plain chain join COUNT(*)).
        // We don't currently have a dedicated fast path for the 2-hop chain, so fall back.
        return Ok(None);
    };

    let excluded_c = collect_subjects_for_predicate_set(store, g_id, p3_id)?;

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?
        .ok_or_else(|| QueryError::Internal("chain+minus requires IRI_REF objects in p1".into()))?;
    let mut it2 = PsotObjectFilterCountIter::new(
        store,
        g_id,
        p2_id,
        &excluded_c,
        ObjectFilterMode::NotInSet,
    )?
    .ok_or_else(|| QueryError::Internal("chain+minus requires IRI_REF objects in p2".into()))?;

    let mut a = it1.next_group()?;
    let mut b = it2.next_group()?;

    let mut total: u128 = 0;
    while let (Some((b1, c1)), Some((b2, c2))) = (a, b) {
        if b1 < b2 {
            a = it1.next_group()?;
        } else if b1 > b2 {
            b = it2.next_group()?;
        } else {
            total = total.saturating_add((c1 as u128).saturating_mul(c2 as u128));
            a = it1.next_group()?;
            b = it2.next_group()?;
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

// ---------------------------------------------------------------------------
// Shape 3: outer triple + MINUS(2-hop chain on object)
//
// `?a <p_outer> ?b . MINUS { ?b <p2> ?c . ?c <p3> ?d . }`
//
// Algorithm:
//   C = subjects(p3)
//   B = subjects with p2-object in C
//   Answer = total_rows(p_outer) - rows_with_object_in(B)
// ---------------------------------------------------------------------------

pub fn object_chain_minus_count_all_operator(
    p_outer: Ref,
    p2: Ref,
    p3: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_object_chain_minus(store, ctx.binary_g_id, &p_outer, &p2, &p3)? {
                Some(count) => {
                    let n_i64 = i64::try_from(count).unwrap_or(i64::MAX);
                    Ok(Some(build_count_batch(out_var, n_i64)?))
                }
                None => Ok(None),
            }
        },
        fallback,
        "object-chain MINUS COUNT(*)",
    )
}

fn count_object_chain_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_outer: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let outer_sid = normalize_pred_sid(store, p_outer)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p_outer_id) = store.sid_to_p_id(&outer_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };

    let total = count_rows_for_predicate_psot(store, g_id, p_outer_id)?;

    let c_set = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_set.is_empty() {
        return Ok(Some(total));
    }

    let Some(mut b_list) = collect_subjects_with_object_in_set(store, g_id, p2_id, &c_set)? else {
        return Ok(None);
    };
    if b_list.is_empty() {
        return Ok(Some(total));
    }
    b_list.sort_unstable();
    b_list.dedup();

    let Some(in_set) = sum_post_object_counts_filtered(store, g_id, p_outer_id, &b_list)? else {
        return Ok(None);
    };

    Ok(Some(total.saturating_sub(in_set)))
}

// ---------------------------------------------------------------------------
// Shape 4: same-subject star join + MINUS on subject
//
// `?s <p1> ?o1 . ?s <p2> ?o2 . ... MINUS { ?s <p_minus> ?x . }`
//
// Algorithm: N-way merge-join on PsotSubjectCountIter streams,
// skipping subjects in S_minus = subjects(p_minus).
// Answer = Σ_{s ∉ S_minus} Π_i count_pi(s)
// ---------------------------------------------------------------------------

pub fn property_minus_count_all_operator(
    outer_predicates: Vec<Ref>,
    minus_predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count =
                count_property_minus(store, ctx.binary_g_id, &outer_predicates, &minus_predicate)?;
            Ok(Some(build_count_batch(
                out_var,
                i64::try_from(count).unwrap_or(i64::MAX),
            )?))
        },
        fallback,
        "property+MINUS COUNT(*)",
    )
}

fn count_property_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicates: &[Ref],
    minus_predicate: &Ref,
) -> Result<u64> {
    if outer_predicates.len() < 2 {
        return Err(QueryError::Internal(
            "property+minus fast path requires 2+ outer predicates".to_string(),
        ));
    }

    let minus_sid = normalize_pred_sid(store, minus_predicate)?;
    let Some(p_minus) = store.sid_to_p_id(&minus_sid) else {
        return count_property_join_all(store, g_id, outer_predicates, &[]);
    };
    let excluded = collect_subjects_for_predicate_sorted(store, g_id, p_minus)?;
    count_property_join_all(store, g_id, outer_predicates, &excluded)
}

fn count_property_join_all(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicates: &[Ref],
    excluded_subjects_sorted: &[u64],
) -> Result<u64> {
    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(outer_predicates.len());
    for p in outer_predicates {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(0);
        };
        iters.push(PsotSubjectCountIter::new(store, g_id, pid)?);
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(|c| c.is_none()) {
            break;
        }

        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            while excl_idx < excluded_subjects_sorted.len()
                && excluded_subjects_sorted[excl_idx] < max_s
            {
                excl_idx += 1;
            }
            let excluded = excl_idx < excluded_subjects_sorted.len()
                && excluded_subjects_sorted[excl_idx] == max_s;
            if !excluded {
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

    Ok(total.min(u64::MAX as u128) as u64)
}
