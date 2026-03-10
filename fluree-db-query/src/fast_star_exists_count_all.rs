//! Fast-path for `COUNT(*)` with a 2-predicate star join and an EXISTS constraint.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p1> ?o1 .
//!   ?s <p2> ?o2 .
//!   FILTER EXISTS { ?s <p3> ?o3 . }
//! }
//! ```
//!
//! Semantics:
//! - For each subject `s`, there are `count_p1(s) * count_p2(s)` join rows.
//! - The EXISTS constraint keeps only subjects that have at least one `<p3>` row.
//!
//! This operator computes:
//!
//! \[
//!   \sum_{s \in S3} count_{p1}(s) \times count_{p2}(s)
//! \]
//!
//! where `S3 = { s | s p3 ?o3 }`.
//!
//! Implementation:
//! - Build `S3` as a sorted `Vec<s_id>` by scanning PSOT(p3) (SId column only).
//! - Stream grouped subject counts for p1 and p2 from PSOT (SId column only).
//! - Merge-join the two count streams on `s_id` to compute products.
//! - Merge-filter by `S3` (also sorted) and sum products.
//!
//! Avoids per-row EXISTS evaluation, join-row materialization, and value decoding.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, fast_path_store, normalize_pred_sid,
    FastPathOperator, PsotSubjectCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

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
