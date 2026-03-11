//! Consolidated fast paths for `COUNT(*)` queries with `EXISTS` constraints.
//!
//! These operators target benchmark-style queries where a `COUNT(*)` aggregate is
//! applied over a basic graph pattern with a correlated `FILTER EXISTS { ... }`
//! constraint and no other modifiers (GROUP BY, ORDER BY, DISTINCT, etc.).
//!
//! The goal is to avoid scanning and materializing the full outer solution set.
//! Instead we:
//! - precompute a small set of qualifying IDs from the EXISTS side (subjects or objects)
//! - count matching outer rows using metadata-light scans and merge-joins on encoded IDs
//!
//! All operators are wrapped as `FastPathOperator`s and fall back safely when
//! the required binary-store context isn't available (history/policy/overlay/etc).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_for_predicate_sorted,
    collect_subjects_with_object_in_set, fast_path_store, normalize_pred_sid,
    sum_post_object_counts_filtered, FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter,
    PsotObjectFilterCountIter, PsotSubjectCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use rustc_hash::FxHashSet;

/// COUNT(*) where the outer triple is restricted by an EXISTS(single-triple) on the same subject.
///
/// Shape:
/// `?s <p_outer> ?o1 . FILTER EXISTS { ?s <p_exists> ?o2 }`
pub fn exists_join_count_all_operator(
    outer_predicate: Ref,
    exists_predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_exists_join_rows_psot(
                store,
                ctx.binary_g_id,
                &outer_predicate,
                &exists_predicate,
            )?;
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "EXISTS-join COUNT(*)",
    )
}

fn count_exists_join_rows_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicate: &Ref,
    exists_predicate: &Ref,
) -> Result<u64> {
    let outer_sid = normalize_pred_sid(store, outer_predicate)?;
    let exists_sid = normalize_pred_sid(store, exists_predicate)?;

    let Some(p_outer) = store.sid_to_p_id(&outer_sid) else {
        return Ok(0);
    };
    let Some(p_exists) = store.sid_to_p_id(&exists_sid) else {
        return Ok(0);
    };

    let subjects_sorted = collect_subjects_for_predicate_sorted(store, g_id, p_exists)?;
    crate::fast_path_common::count_rows_psot_for_subjects_sorted(
        store,
        g_id,
        p_outer,
        &subjects_sorted,
    )
}

/// COUNT(*) for a 2-hop join chain with EXISTS(single-triple) on the tail.
///
/// Shape:
/// `?a <p1> ?b . ?b <p2> ?c . FILTER EXISTS { ?c <p3> ?d }`
pub fn chain_exists_join_count_all_operator(
    p1: Ref,
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
            let Some(count) = count_chain_exists_join(store, ctx.binary_g_id, &p1, &p2, &p3)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "chain+exists COUNT(*)",
    )
}

fn count_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // EXISTS subjects are `?c` in: ?c p3 ?d .
    let exists_subjects = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if exists_subjects.is_empty() {
        return Ok(Some(0));
    }

    let mut ab = match PostObjectGroupCountIter::new(store, g_id, p1_id)? {
        Some(it) => it,
        None => return Ok(None),
    };
    let mut bc = match PsotObjectFilterCountIter::new(
        store,
        g_id,
        p2_id,
        &exists_subjects,
        ObjectFilterMode::InSet,
    )? {
        Some(it) => it,
        None => return Ok(None),
    };

    let mut left = ab.next_group()?;
    let mut right = bc.next_group()?;
    let mut total: u64 = 0;

    while let (Some((b1, a_count)), Some((b2, c_count))) = (left, right) {
        if b1 < b2 {
            left = ab.next_group()?;
        } else if b1 > b2 {
            right = bc.next_group()?;
        } else {
            total = total.saturating_add(a_count.saturating_mul(c_count));
            left = ab.next_group()?;
            right = bc.next_group()?;
        }
    }

    Ok(Some(total))
}

/// COUNT(*) for a same-subject star join with EXISTS(single-triple) on the subject.
///
/// Shape:
/// `?s <p1> ?o1 . ?s <p2> ?o2 . ... FILTER EXISTS { ?s <p_exists> ?oX }`
pub fn star_exists_join_count_all_operator(
    preds: Vec<Ref>,
    p_exists: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_star_exists_join(store, ctx.binary_g_id, &preds, &p_exists)?;
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "star+exists COUNT(*)",
    )
}

fn count_star_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    preds: &[Ref],
    p_exists: &Ref,
) -> Result<u64> {
    if preds.len() < 2 {
        return Err(QueryError::Internal(
            "star+exists fast path requires 2+ join predicates".to_string(),
        ));
    }

    let p_exists_sid = normalize_pred_sid(store, p_exists)?;
    let Some(p_exists_id) = store.sid_to_p_id(&p_exists_sid) else {
        return Ok(0);
    };
    let s_ok = collect_subjects_for_predicate_sorted(store, g_id, p_exists_id)?;
    if s_ok.is_empty() {
        return Ok(0);
    }

    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(preds.len());
    for p in preds {
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

    let mut ok_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(|c| c.is_none()) {
            break;
        }

        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            while ok_idx < s_ok.len() && s_ok[ok_idx] < max_s {
                ok_idx += 1;
            }
            if ok_idx < s_ok.len() && s_ok[ok_idx] == max_s {
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

/// COUNT(*) where the OUTER triple's object must satisfy a 2-hop EXISTS chain.
///
/// Shape:
/// `?a <p_outer> ?b . FILTER EXISTS { ?b <p2> ?c . ?c <p3> ?d . }`
pub fn object_chain_exists_join_count_all_operator(
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
            let Some(count) =
                count_object_chain_exists_join(store, ctx.binary_g_id, &p_outer, &p2, &p3)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "object-chain+exists COUNT(*)",
    )
}

fn count_object_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_outer: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p_outer_sid = normalize_pred_sid(store, p_outer)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p_outer_id) = store.sid_to_p_id(&p_outer_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // `c_ok` are the `?c` subjects that satisfy `?c p3 ?d`.
    let c_ok: FxHashSet<u64> = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_ok.is_empty() {
        return Ok(Some(0));
    }

    // `b_ok` are the `?b` subjects that have ANY `p2` object in `c_ok`.
    let Some(mut b_ok) = collect_subjects_with_object_in_set(store, g_id, p2_id, &c_ok)? else {
        return Ok(None);
    };
    if b_ok.is_empty() {
        return Ok(Some(0));
    }
    b_ok.sort_unstable();
    b_ok.dedup();

    // Count outer rows `?a p_outer ?b` with `b in b_ok` by scanning POST groups.
    sum_post_object_counts_filtered(store, g_id, p_outer_id, &b_ok)
}

/// COUNT(*) where an OUTER triple is restricted by an EXISTS-star block on the same subject.
///
/// Shape:
/// `?s <p_outer> ?o . FILTER EXISTS { ?s <p2> ?x . ?s <p3> ?y . ... }`
pub fn exists_star_join_count_all_operator(
    p_outer: Ref,
    exists_preds: Vec<Ref>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let Some(count) =
                count_exists_star_join(store, ctx.binary_g_id, &p_outer, &exists_preds)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "exists-star COUNT(*)",
    )
}

fn count_exists_star_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_outer: &Ref,
    exists_preds: &[Ref],
) -> Result<Option<u64>> {
    if exists_preds.len() < 2 {
        return Err(QueryError::Internal(
            "exists-star fast path requires 2+ EXISTS predicates".to_string(),
        ));
    }

    let p_outer_sid = normalize_pred_sid(store, p_outer)?;
    let Some(p_outer_id) = store.sid_to_p_id(&p_outer_sid) else {
        return Ok(Some(0));
    };

    // Compute subjects that satisfy ALL predicates in the EXISTS block.
    let mut lists: Vec<Vec<u64>> = Vec::with_capacity(exists_preds.len());
    for p in exists_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(Some(0));
        };
        lists.push(collect_subjects_for_predicate_sorted(store, g_id, pid)?);
    }
    let subjects_sorted = crate::fast_path_common::intersect_many_sorted(lists);
    if subjects_sorted.is_empty() {
        return Ok(Some(0));
    }

    let count = crate::fast_path_common::count_rows_psot_for_subjects_sorted(
        store,
        g_id,
        p_outer_id,
        &subjects_sorted,
    )?;
    Ok(Some(count))
}
