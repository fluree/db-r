//! Fast-path: `COUNT(*)` for `MINUS` where the outer pattern is a single triple.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE { ?s <p_outer> ?o1 MINUS { ?s <p1> ?o2 . ?s <p2> ?o3 . } }
//! ```
//!
//! For the supported shapes here, MINUS depends only on the shared subject `?s`:
//! the MINUS block either yields at least one solution for a subject or it doesn't.
//! Therefore the query reduces to:
//!
//! - Let `Sminus = { s | s p1 ?x AND s p2 ?y AND ... }`
//! - Answer = `count_{p_outer}(*) - count_{p_outer}(s in Sminus)`
//!
//! This operator:
//! - Computes `Sminus` from PSOT as sorted subject lists and intersects them.
//! - Counts outer rows for those subjects via a streaming PSOT merge-count.
//! - Gets total outer rows from PSOT leaflet directory metadata (no row scan).
//!
//! Correctness constraints (planner must enforce):
//! - Query is `SELECT (COUNT(*) AS ?count)` with no modifiers.
//! - Outer is exactly one triple `?s <p_outer> ?o` (no dt/lang constraint).
//! - MINUS block contains 1+ triple patterns, all `?s <pi> ?oi` (same subject var),
//!   bound predicates, var objects, no dt/lang constraints.

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, count_rows_for_predicate_psot,
    count_rows_psot_for_subjects_sorted, fast_path_store, intersect_many_sorted,
    normalize_pred_sid, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

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

    // Total outer rows is metadata-only.
    let total = count_rows_for_predicate_psot(store, g_id, p_outer)?;

    // No MINUS predicates means "remove nothing" (should be rejected by planner, but be safe).
    if minus_predicates.is_empty() {
        return Ok(total);
    }

    // Build Sminus = intersection of subject sets for each predicate in the MINUS block.
    let mut subject_lists: Vec<Vec<u64>> = Vec::with_capacity(minus_predicates.len());
    for p in minus_predicates {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            // A missing predicate makes the MINUS block empty, therefore it removes nothing.
            return Ok(total);
        };
        subject_lists.push(collect_subjects_for_predicate_sorted(store, g_id, pid)?);
    }

    let subjects = intersect_many_sorted(subject_lists);
    if subjects.is_empty() {
        return Ok(total);
    }

    // Count how many outer rows have subject in Sminus and subtract.
    let removed = count_rows_psot_for_subjects_sorted(store, g_id, p_outer, &subjects)?;
    Ok(total.saturating_sub(removed))
}
