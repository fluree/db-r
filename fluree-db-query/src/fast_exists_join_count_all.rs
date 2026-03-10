//! Fast-path for `COUNT(*)` with a simple correlated `FILTER EXISTS`.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE { ?s <p_outer> ?o1 . FILTER EXISTS { ?s <p_exists> ?o2 } }
//! ```
//!
//! The generic pipeline would:
//! - scan the outer predicate and materialize `Sid` / literal values
//! - evaluate EXISTS per row (or even with a semijoin cache, still needs subject decoding)
//!
//! This operator instead:
//! - scans PSOT for `<p_exists>` once to build a set of matching subject IDs (`s_id`)
//! - scans PSOT for `<p_outer>` and counts rows whose `s_id` is in that set
//! - never decodes subject/object values
//!
//! This preserves SPARQL multiplicity semantics: COUNT(*) counts one row per outer match.

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, count_rows_psot_for_subjects_sorted,
    fast_path_store, normalize_pred_sid, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

/// Create a fused operator that outputs a single-row batch with the COUNT(*) result.
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

/// COUNT(*) for `?s p_outer ?o1` restricted to subjects that satisfy `?s p_exists ?o2`.
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
    count_rows_psot_for_subjects_sorted(store, g_id, p_outer, &subjects_sorted)
}
