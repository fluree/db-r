//! Fast-path: `COUNT(*)` for an N-hop linear join chain of bound predicates (N ≥ 2).
//!
//! Targets queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count) WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   ?c <p3> ?d .        -- 3-hop example; handles 2-hop, 4-hop, etc.
//! }
//! ```
//!
//! The generic pipeline executes this via nested-loop joins and can end up
//! materializing very large intermediate results.
//!
//! This operator computes the join cardinality without materializing join rows
//! via a right-to-left fold:
//!
//! 1. Rightmost predicate pN: `weights[x] = count_{pN}(x)` (subject outdegree)
//! 2. Each intermediate p_i (i = N-1 down to 2): `weights[b] = Σ_{b->c in p_i} weights[c]`
//! 3. Leftmost p1: `total = Σ_{b} count_{p1_objects=b} × weights[b]`
//!
//! Step 2 uses `PsotSubjectWeightedSumIter` (default_weight=0 for inner join semantics).
//! Step 3 uses `PostObjectGroupCountIter` merge-joined with the final weight map.

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

/// Create a fast-path operator for `COUNT(*)` over a linear chain of N ≥ 2 predicates.
///
/// `predicates` must be in chain order: `[p1, p2, ..., pN]` matching
/// `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ... ?v_{N-1} <pN> ?vN`.
pub fn linear_chain_count_all_operator(
    predicates: Vec<Ref>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };

            // Resolve all predicate Refs to p_ids.
            let mut p_ids: Vec<u32> = Vec::with_capacity(predicates.len());
            for pred in &predicates {
                let sid = normalize_pred_sid(store, pred)?;
                let Some(p_id) = store.sid_to_p_id(&sid) else {
                    // Any missing predicate in an inner join chain → 0 results.
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                p_ids.push(p_id);
            }

            match count_linear_chain(store, ctx.binary_g_id, &p_ids)? {
                Some(n) => {
                    let n_i64 = i64::try_from(n)
                        .map_err(|_| QueryError::execution("COUNT(*) exceeds i64 in chain join"))?;
                    Ok(Some(build_count_batch(out_var, n_i64)?))
                }
                None => Ok(None),
            }
        },
        fallback,
        "chain join COUNT(*)",
    )
}

/// Compute `COUNT(*)` for a linear chain `p_ids[0] -> p_ids[1] -> ... -> p_ids[N-1]`.
///
/// Returns `None` if the fast path is not applicable (e.g., non-IRI join keys).
fn count_linear_chain(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_ids: &[u32],
) -> Result<Option<u64>> {
    assert!(p_ids.len() >= 2, "chain must have at least 2 predicates");

    let n = p_ids.len();

    // Step 1: Build initial weights from the rightmost predicate.
    // weights[subject] = outdegree of subject in pN.
    let mut weights: FxHashMap<u64, u64> = FxHashMap::default();
    {
        let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
        while let Some((s, count)) = iter.next_group()? {
            if count > 0 {
                weights.insert(s, count);
            }
        }
    }
    if weights.is_empty() {
        return Ok(Some(0));
    }

    // Step 2: Fold right-to-left through intermediate predicates (indices n-2 down to 1).
    // For each p_i, compute new_weights[b] = Σ_{b->c in p_i} weights[c].
    for i in (1..n - 1).rev() {
        let Some(mut iter) = PsotSubjectWeightedSumIter::new(store, g_id, p_ids[i], &weights, 0)?
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
