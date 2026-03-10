//! Fast-path for `COUNT(*)` with a 2-hop join chain and a simple EXISTS.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   FILTER EXISTS { ?c <p3> ?d . }
//! }
//! ```
//!
//! This operator computes:
//!
//! \[
//!   \sum_{b} \bigl( count_{p1}(b) \times count_{p2,exists}(b) \bigr)
//! \]
//!
//! where:
//! - `count_{p1}(b)` is the number of `?a` such that `?a p1 b`
//! - `count_{p2,exists}(b)` is the number of `?c` such that `?b p2 c` and `c` satisfies the EXISTS.
//!
//! It avoids materializing join rows and never decodes to IRIs/strings.

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store, normalize_pred_sid,
    FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter, PsotObjectFilterCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub fn predicate_chain_exists_join_count_all_operator(
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
            let Some(count) = count_chain_exists_join(store, ctx.binary_g_id, &p1, &p2, &p3)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "chain+exists COUNT(*)",
    )
}

fn count_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // EXISTS subjects are `?c` in: ?c p3 ?d .
    let exists_subjects = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if exists_subjects.is_empty() {
        return Ok(Some(0));
    }

    let mut ab = match PostObjectGroupCountIter::new(store, g_id, p1_id)? {
        Some(it) => it,
        None => return Ok(None),
    };
    let mut bc = match PsotObjectFilterCountIter::new(
        store,
        g_id,
        p2_id,
        &exists_subjects,
        ObjectFilterMode::InSet,
    )? {
        Some(it) => it,
        None => return Ok(None),
    };

    let mut left = ab.next_group()?;
    let mut right = bc.next_group()?;
    let mut total: u64 = 0;

    while let (Some((b1, a_count)), Some((b2, c_count))) = (left, right) {
        if b1 < b2 {
            left = ab.next_group()?;
        } else if b1 > b2 {
            right = bc.next_group()?;
        } else {
            total = total.saturating_add(a_count.saturating_mul(c_count));
            left = ab.next_group()?;
            right = bc.next_group()?;
        }
    }

    Ok(Some(total))
}
