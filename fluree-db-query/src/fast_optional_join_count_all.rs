//! Fast-path: COUNT(*) for a single required triple plus a single-triple OPTIONAL.
//!
//! Targets benchmark query shape:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p1> ?o1 . OPTIONAL { ?s <p2> ?o2 } }`
//!
//! Semantics:
//! For each required row `(s,o1)`, the OPTIONAL contributes `max(1, count_p2(s))`
//! rows (one poisoned row when no match, or fan-out when matches exist).
//!
//! Therefore:
//!   total = sum_s  count_p1(s) * max(1, count_p2(s))
//!
//! We compute this by streaming per-subject row counts from PSOT for both predicates
//! and merge-joining on `s_id` (metadata-friendly, no value decoding).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, normalize_pred_sid, FastPathOperator, PsotSubjectCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_core::GraphId;

pub fn predicate_optional_join_count_all(
    pred_required: Ref,
    pred_optional: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_optional_join_count_all_psot(
                store,
                ctx.binary_g_id,
                &pred_required,
                &pred_optional,
            )?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in OPTIONAL join fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "OPTIONAL join COUNT(*)",
    )
}

fn count_optional_join_count_all_psot(
    store: &fluree_db_binary_index::BinaryIndexStore,
    g_id: GraphId,
    pred_required: &Ref,
    pred_optional: &Ref,
) -> Result<u64> {
    let sid_req = normalize_pred_sid(store, pred_required)?;
    let sid_opt = normalize_pred_sid(store, pred_optional)?;

    let Some(p_req) = store.sid_to_p_id(&sid_req) else {
        return Ok(0);
    };
    let Some(p_opt) = store.sid_to_p_id(&sid_opt) else {
        // Optional predicate missing => all required rows contribute exactly 1.
        let mut it_req = PsotSubjectCountIter::new(store, g_id, p_req)?;
        let mut total = 0u64;
        while let Some((_s, c1)) = it_req.next_group()? {
            total += c1;
        }
        return Ok(total);
    };

    let mut it_req = PsotSubjectCountIter::new(store, g_id, p_req)?;
    let mut it_opt = PsotSubjectCountIter::new(store, g_id, p_opt)?;

    let mut opt_cur = it_opt.next_group()?;
    let mut total = 0u64;

    while let Some((s, c1)) = it_req.next_group()? {
        while let Some((s2, _c2)) = opt_cur {
            if s2 < s {
                opt_cur = it_opt.next_group()?;
                continue;
            }
            break;
        }

        let c2 = match opt_cur {
            Some((s2, c2)) if s2 == s => {
                // Advance for next iteration (each subject appears once).
                opt_cur = it_opt.next_group()?;
                c2
            }
            _ => 0u64,
        };

        let mult = if c2 == 0 { 1 } else { c2 };
        total = total.saturating_add(c1.saturating_mul(mult));
    }

    Ok(total)
}
