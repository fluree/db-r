//! Fast-path: `COUNT(*)` for a single transitive property path (`+`) with a fixed endpoint.
//!
//! Targets queries like:
//! - `SELECT (COUNT(*) AS ?count) WHERE { <S> <p>+ ?o }`
//! - (future) `SELECT (COUNT(*) AS ?count) WHERE { ?s <p>+ <O> }`
//!
//! This avoids the generic `PropertyPathOperator`'s repeated range scans by:
//! - scanning PSOT(p) once to build an adjacency map of ref-only edges
//! - running a BFS/visited traversal from the fixed seed and counting unique reachable endpoints
//!
//! Semantics for `+` (one-or-more):
//! - does NOT include the start node unless there is a non-zero-length cycle back to it
//! - traverses only IRI_REF edges (ref-only), matching existing property path behavior

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, build_iri_adjacency_from_cursor, build_psot_cursor_for_predicate,
    cursor_projection_sid_otype_okey, fast_path_store, reach_count_plus, subject_ref_to_s_id,
    FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;

/// Create a fused operator that outputs a single-row batch with the COUNT(*) result.
pub fn property_path_plus_count_all_operator(
    predicate: fluree_db_core::Sid,
    subject: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_reachable_plus_from_fixed_subject(
                store,
                ctx,
                ctx.binary_g_id,
                &predicate,
                &subject,
            )?;
            match count {
                Some(n) => Ok(Some(build_count_batch(
                    out_var,
                    i64::try_from(n).unwrap_or(i64::MAX),
                )?)),
                None => Ok(None),
            }
        },
        fallback,
        "property-path+ COUNT(*)",
    )
}

fn count_reachable_plus_from_fixed_subject(
    store: &std::sync::Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &crate::context::ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    pred_sid: &fluree_db_core::Sid,
    subj: &Ref,
) -> Result<Option<u64>> {
    let Some(seed) = subject_ref_to_s_id(store, subj)? else {
        return Ok(None);
    };
    let Some(p_id) = store.sid_to_p_id(pred_sid) else {
        return if ctx.overlay.is_some() {
            Ok(None)
        } else {
            Ok(Some(0))
        };
    };

    let projection = cursor_projection_sid_otype_okey();
    let Some(mut cursor) =
        build_psot_cursor_for_predicate(ctx, store, g_id, pred_sid.clone(), p_id, projection)?
    else {
        return Ok(None);
    };

    let adj = build_iri_adjacency_from_cursor(&mut cursor)?;
    Ok(Some(reach_count_plus(&adj, seed)))
}
