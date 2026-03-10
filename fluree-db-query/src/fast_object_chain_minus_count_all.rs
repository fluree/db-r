//! Fast-path: `COUNT(*)` for an outer triple with a 2-hop `MINUS` chain on the object.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p_outer> ?b .
//!   MINUS { ?b <p2> ?c . ?c <p3> ?d . }
//! }
//! ```
//!
//! Supported semantics:
//! - Shared var between outer and MINUS is `?b` (the outer object).
//! - MINUS removes an outer solution iff `?b` has at least one `p2` edge to a `?c`
//!   that has at least one `p3` edge.
//!
//! Therefore:
//! - Let `C = { c | c p3 ?d }`
//! - Let `B = { b | exists c in C: b p2 c }`
//! - Answer = `count_{p_outer}(*) - count_{p_outer}(object in B)`
//!
//! Correctness constraints (planner must enforce):
//! - All relevant objects are IRIs (`o_type_const == IRI_REF`) so join keys are stored in `o_key`.

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_with_object_in_set,
    count_rows_for_predicate_psot, fast_path_store, normalize_pred_sid,
    sum_post_object_counts_filtered, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

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
        // MINUS block empty => removes nothing => count is total outer.
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };

    // Total outer rows.
    let total = count_rows_for_predicate_psot(store, g_id, p_outer_id)?;

    let c_set = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_set.is_empty() {
        return Ok(Some(total));
    }

    let Some(mut b_list) =
        collect_subjects_with_object_in_set(store, g_id, p2_id, &c_set)?
    else {
        return Ok(None);
    };
    if b_list.is_empty() {
        return Ok(Some(total));
    }
    b_list.sort_unstable();
    b_list.dedup();

    let Some(in_set) = sum_post_object_counts_filtered(store, g_id, p_outer_id, &b_list)?
    else {
        return Ok(None);
    };

    Ok(Some(total.saturating_sub(in_set)))
}
