//! Fast-path for `COUNT(*)` where an outer triple is filtered by an EXISTS-star block.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p_outer> ?o1 .
//!   FILTER EXISTS { ?s <p2> ?o2 . ?s <p3> ?o3 . }
//! }
//! ```
//!
//! Semantics:
//! - The EXISTS block is a conjunctive constraint on `?s`: keep outer rows whose subject has
//!   at least one match for each inner predicate.
//! - COUNT(*) counts outer solution rows, so the answer is:
//!   `sum_{s in (S2 ∩ S3)} count_{p_outer}(s)`
//!   where `Si = { s | s pi ?oi }`.
//!
//! Implementation:
//! - Build sorted distinct subject lists for each inner predicate from PSOT (SId only).
//! - Intersect them (streaming two-pointer intersection).
//! - Stream grouped subject counts for the outer predicate from PSOT and merge-join with the
//!   intersected subject list to sum counts.
//!
//! Avoids per-row EXISTS evaluation, join row materialization, and value decoding.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, count_rows_psot_for_subjects_sorted,
    fast_path_store, intersect_many_sorted, normalize_pred_sid, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub fn exists_star_join_count_all_operator(
    outer_predicate: Ref,
    exists_predicates: Vec<Ref>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_exists_star_rows_psot(
                store,
                ctx.binary_g_id,
                &outer_predicate,
                &exists_predicates,
            )?;
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "EXISTS-star COUNT(*)",
    )
}

fn count_exists_star_rows_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicate: &Ref,
    exists_preds: &[Ref],
) -> Result<u64> {
    let outer_sid = normalize_pred_sid(store, outer_predicate)?;
    let Some(p_outer) = store.sid_to_p_id(&outer_sid) else {
        return Ok(0);
    };

    if exists_preds.is_empty() {
        return Err(QueryError::Internal(
            "EXISTS-star fast path requires at least one predicate".to_string(),
        ));
    }

    let mut subject_lists: Vec<Vec<u64>> = Vec::with_capacity(exists_preds.len());
    for p in exists_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(0);
        };
        subject_lists.push(collect_subjects_for_predicate_sorted(store, g_id, pid)?);
    }

    let subjects = intersect_many_sorted(subject_lists);
    count_rows_psot_for_subjects_sorted(store, g_id, p_outer, &subjects)
}
