//! Consolidated fast paths for `COUNT(*)` queries with `OPTIONAL` constraints.
//!
//! These operators target benchmark-style queries where a `COUNT(*)` aggregate is
//! applied over a basic graph pattern with an `OPTIONAL { ... }` block and no
//! other modifiers (GROUP BY, ORDER BY, DISTINCT, etc.).
//!
//! All OPTIONAL shapes exploit: for each required row, the OPTIONAL contributes
//! `max(1, count_optional)` rows (one poisoned row when no match, or fan-out
//! when matches exist). The operators stream `(key, count)` pairs from PSOT/POST
//! metadata and merge-join them without materializing rows.
//!
//! All operators are wrapped as `FastPathOperator`s and fall back safely when
//! the required binary-store context isn't available (history/policy/overlay/etc).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, normalize_pred_sid, FastPathOperator,
    PostObjectGroupCountIter, PsotSubjectCountIter, PsotSubjectWeightedSumIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use rustc_hash::FxHashMap;

// ---------------------------------------------------------------------------
// Shape 1: single required triple + single-triple OPTIONAL on same subject
//
// `?s <p1> ?o1 . OPTIONAL { ?s <p2> ?o2 }`
//
// total = Σ_s count_p1(s) * max(1, count_p2(s))
// ---------------------------------------------------------------------------

pub fn predicate_optional_join_count_all(
    pred_required: Ref,
    pred_optional: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_optional_join(
                store,
                ctx.binary_g_id,
                &pred_required,
                &pred_optional,
            )?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in OPTIONAL join fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "OPTIONAL join COUNT(*)",
    )
}

fn count_optional_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    pred_required: &Ref,
    pred_optional: &Ref,
) -> Result<u64> {
    let sid_req = normalize_pred_sid(store, pred_required)?;
    let sid_opt = normalize_pred_sid(store, pred_optional)?;

    let Some(p_req) = store.sid_to_p_id(&sid_req) else {
        return Ok(0);
    };
    let Some(p_opt) = store.sid_to_p_id(&sid_opt) else {
        // Optional predicate missing => all required rows contribute exactly 1.
        let mut it_req = PsotSubjectCountIter::new(store, g_id, p_req)?;
        let mut total = 0u64;
        while let Some((_s, c1)) = it_req.next_group()? {
            total += c1;
        }
        return Ok(total);
    };

    let mut it_req = PsotSubjectCountIter::new(store, g_id, p_req)?;
    let mut it_opt = PsotSubjectCountIter::new(store, g_id, p_opt)?;

    let mut opt_cur = it_opt.next_group()?;
    let mut total = 0u64;

    while let Some((s, c1)) = it_req.next_group()? {
        while let Some((s2, _c2)) = opt_cur {
            if s2 < s {
                opt_cur = it_opt.next_group()?;
                continue;
            }
            break;
        }

        let c2 = match opt_cur {
            Some((s2, c2)) if s2 == s => {
                opt_cur = it_opt.next_group()?;
                c2
            }
            _ => 0u64,
        };

        let mult = if c2 == 0 { 1 } else { c2 };
        total = total.saturating_add(c1.saturating_mul(mult));
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// Shape 2: single required triple + OPTIONAL 2-hop chain (head position)
//
// `?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d . }`
//
// total = Σ_b count_p1(b) * max(1, Σ_{c in p2(b)} count_p3(c))
// ---------------------------------------------------------------------------

pub fn predicate_optional_chain_head_count_all(
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
            match count_optional_chain_head(store, ctx.binary_g_id, &p1, &p2, &p3)? {
                Some(count) => Ok(Some(build_count_batch(
                    out_var,
                    i64::try_from(count).unwrap_or(i64::MAX),
                )?)),
                None => Ok(None),
            }
        },
        fallback,
        "optional chain-head COUNT(*)",
    )
}

fn count_optional_chain_head(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&sid2) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&sid3) else {
        // Optional chain can never match => multiplier is 1 for all b.
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
        )?;
        let mut total = 0u64;
        while let Some((_b, w)) = it1.next_group()? {
            total += w;
        }
        return Ok(Some(total));
    };

    // Precompute n3(c) = count_{p3}(c).
    let mut n3: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    while let Some((c, n)) = it3.next_group()? {
        n3.insert(c, n);
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
    )?;
    // default_weight=0: objects not in n3 contribute nothing to the sum
    let mut it2 = PsotSubjectWeightedSumIter::new(store, g_id, p2_id, &n3, 0)?.ok_or(
        QueryError::Internal("optional chain-head: PSOT iterator unavailable".into()),
    )?;

    let mut p2_cur = it2.next_group()?;
    let mut total = 0u64;

    while let Some((b, w)) = it1.next_group()? {
        while let Some((b2, _)) = p2_cur {
            if b2 < b {
                p2_cur = it2.next_group()?;
                continue;
            }
            break;
        }
        let sum_n3 = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        let mult = if sum_n3 == 0 { 1 } else { sum_n3 };
        total = total.saturating_add(w.saturating_mul(mult));
    }

    Ok(Some(total))
}

// ---------------------------------------------------------------------------
// Shape 3: 2-hop required chain + OPTIONAL tail on chain output
//
// `?a <p1> ?b . ?b <p2> ?c . OPTIONAL { ?c <p3> ?d . }`
//
// total = Σ_b count_p1(b) * Σ_{c in p2(b)} max(1, count_p3(c))
// ---------------------------------------------------------------------------

pub fn predicate_chain_optional_tail_count_all(
    p1: Ref,
    p2: Ref,
    p3_opt: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_chain_optional_tail(store, ctx.binary_g_id, &p1, &p2, &p3_opt)? {
                Some(count) => Ok(Some(build_count_batch(
                    out_var,
                    i64::try_from(count).unwrap_or(i64::MAX),
                )?)),
                None => Ok(None),
            }
        },
        fallback,
        "chain+optional COUNT(*)",
    )
}

fn count_chain_optional_tail(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_opt: &Ref,
) -> Result<Option<u64>> {
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3_opt)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&sid2) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&sid3) else {
        // Optional predicate missing => multiplier = 1 for all c.
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("chain+optional: POST iterator unavailable".into()),
        )?;
        let mut it2 = PsotSubjectCountIter::new(store, g_id, p2_id)?;

        let mut p2_cur = it2.next_group()?;
        let mut total = 0u64;
        while let Some((b, w)) = it1.next_group()? {
            while let Some((b2, _)) = p2_cur {
                if b2 < b {
                    p2_cur = it2.next_group()?;
                    continue;
                }
                break;
            }
            let count_edges = match p2_cur {
                Some((b2, n)) if b2 == b => {
                    p2_cur = it2.next_group()?;
                    n
                }
                _ => 0u64,
            };
            total = total.saturating_add(w.saturating_mul(count_edges));
        }
        return Ok(Some(total));
    };

    // Precompute m(c) = max(1, count_p3(c)).
    let mut mult_c: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    while let Some((c, n)) = it3.next_group()? {
        mult_c.insert(c, n.max(1));
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("chain+optional: POST iterator unavailable".into()),
    )?;
    // default_weight=1: OPTIONAL semantics — missing objects get multiplier 1
    let mut it2 = PsotSubjectWeightedSumIter::new(store, g_id, p2_id, &mult_c, 1)?.ok_or(
        QueryError::Internal("chain+optional: PSOT iterator unavailable".into()),
    )?;

    let mut p2_cur = it2.next_group()?;
    let mut total = 0u64;

    while let Some((b, w)) = it1.next_group()? {
        while let Some((b2, _)) = p2_cur {
            if b2 < b {
                p2_cur = it2.next_group()?;
                continue;
            }
            break;
        }
        let sum_m = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        total = total.saturating_add(w.saturating_mul(sum_m));
    }

    Ok(Some(total))
}
