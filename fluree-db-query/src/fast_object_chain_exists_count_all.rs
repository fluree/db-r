//! Fast-path for `COUNT(*)` with an outer triple and a 2-hop EXISTS chain on the object.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p_outer> ?b .
//!   FILTER EXISTS { ?b <p2> ?c . ?c <p3> ?d . }
//! }
//! ```
//!
//! This computes:
//! - Let `C = { c | c p3 ?d }`  (subjects that have any `p3` edge)
//! - Let `B = { b | b p2 c and c in C }`
//! - Answer = number of rows in `?a p_outer ?b` where `b in B`
//!
//! Implementation details (streaming, no row materialization):
//! - Build `C` by scanning PSOT(p3) and collecting distinct subject IDs.
//! - Build `B` by scanning PSOT(p2), grouping by subject `b`, and checking whether any
//!   object `c` is in `C`. Output `B` as a sorted Vec.
//! - Scan POST(p_outer), grouped by object `b`, summing group counts where `b in B`.
//!
//! Requires constant `o_type_const == IRI_REF` on the relevant leaflets so that `o_key`
//! is a subject ID (join key).

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, collect_subjects_with_object_in_set,
    fast_path_store, normalize_pred_sid, sum_post_object_counts_filtered, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub fn predicate_object_chain_exists_join_count_all_operator(
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
        "object-chain EXISTS COUNT(*)",
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

    // C = subjects that have any p3 edge.
    let c_subjects = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_subjects.is_empty() {
        return Ok(Some(0));
    }

    // B = subjects b that have any (b p2 c) where c in C. Output as sorted Vec.
    let Some(mut b_subjects) =
        collect_subjects_with_object_in_set(store, g_id, p2_id, &c_subjects)?
    else {
        return Ok(None);
    };
    if b_subjects.is_empty() {
        return Ok(Some(0));
    }
    // It is naturally produced in sorted s_id order; keep it monotone.
    // (Defensive: if future code changes ordering, sort.)
    if !b_subjects.windows(2).all(|w| w[0] <= w[1]) {
        b_subjects.sort_unstable();
        b_subjects.dedup();
    }

    sum_post_object_counts_filtered(store, g_id, p_outer_id, &b_subjects)
}
