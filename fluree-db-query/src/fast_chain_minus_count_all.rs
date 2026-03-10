//! Fast-path: `COUNT(*)` for a 2-hop join chain with a `MINUS` constraint on the tail.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   MINUS { ?c <p3> ?d . }
//! }
//! ```
//!
//! Supported semantics:
//! - Shared var between outer and MINUS is `?c`.
//! - MINUS removes an outer solution iff `?c` has at least one `<p3>` row.
//!
//! Therefore:
//! - Let `S3 = { c | c p3 ?d }`
//! - Answer = Σ_b count_{p1}(b) * count_{p2, c∉S3}(b)
//!
//! Implementation avoids materializing join rows and avoids decoding values.
//!
//! Correctness constraints (planner must enforce):
//! - Predicates are bound.
//! - `?b` and `?c` are IRIs (so they are stored as `o_key` with `o_type_const = IRI_REF`).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store, normalize_pred_sid,
    FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter, PsotObjectFilterCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub fn chain_minus_count_all_operator(
    p1: Ref,
    p2: Ref,
    p3_minus: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_chain_minus(store, ctx.binary_g_id, &p1, &p2, &p3_minus)? {
                Some(count) => {
                    let n_i64 = i64::try_from(count).unwrap_or(i64::MAX);
                    Ok(Some(build_count_batch(out_var, n_i64)?))
                }
                None => Ok(None),
            }
        },
        fallback,
        "chain+minus COUNT(*)",
    )
}

fn count_chain_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_minus: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3_minus)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        // If the minus predicate doesn't exist, it removes nothing (plain chain join COUNT(*)).
        // We don't currently have a dedicated fast path for the 2-hop chain, so fall back.
        return Ok(None);
    };

    let excluded_c = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    // Even if excluded is empty, the algorithm below still works (counts all p2 edges).

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?
        .ok_or_else(|| QueryError::Internal("chain+minus requires IRI_REF objects in p1".into()))?;
    let mut it2 =
        PsotObjectFilterCountIter::new(store, g_id, p2_id, &excluded_c, ObjectFilterMode::NotInSet)?
            .ok_or_else(|| {
                QueryError::Internal("chain+minus requires IRI_REF objects in p2".into())
            })?;

    let mut a = it1.next_group()?;
    let mut b = it2.next_group()?;

    let mut total: u128 = 0;
    while let (Some((b1, c1)), Some((b2, c2))) = (a, b) {
        if b1 < b2 {
            a = it1.next_group()?;
        } else if b1 > b2 {
            b = it2.next_group()?;
        } else {
            total = total.saturating_add((c1 as u128).saturating_mul(c2 as u128));
            a = it1.next_group()?;
            b = it2.next_group()?;
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}
