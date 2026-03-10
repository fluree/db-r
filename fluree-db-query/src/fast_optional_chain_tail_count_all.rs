//! Fast-path: COUNT(*) for a 2-hop required join chain with an OPTIONAL tail on the chain output.
//!
//! Targets benchmark query shape:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?a <p1> ?b . ?b <p2> ?c . OPTIONAL { ?c <p3> ?d . } }`
//!
//! Semantics:
//! For each required chain row `(a,b,c)`, the OPTIONAL contributes `max(1, count_p3(c))` rows.
//! Therefore:
//!
//! - Let `w(b) = count_{p1}(?a -> b)` (group p1 by object `b`)
//! - Let `m(c) = max(1, count_{p3}(c -> ?d))`
//! - Let `S(b) = Σ_{c in p2(b)} m(c)` for the `p2` edges out of `b`
//! - Answer = Σ_b w(b) * S(b)
//!
//! We compute this streaming:
//! - Build a small map `m(c)` by scanning PSOT(p3) grouped by subject.
//! - Stream POST(p1) grouped by object to get `w(b)` in sorted `b` order.
//! - Stream PSOT(p2) grouped by subject `b` and sum `m(c)` across its objects.
//! - Merge-join on `b`.
//!
//! This avoids materializing the join rows and avoids decoding values.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, normalize_pred_sid, FastPathOperator,
    PostObjectGroupCountIter, PsotSubjectCountIter, PsotSubjectWeightedSumIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use rustc_hash::FxHashMap;

pub fn predicate_chain_optional_tail_count_all(
    p1: Ref,
    p2: Ref,
    p3_opt: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_chain_optional_tail(store, ctx.binary_g_id, &p1, &p2, &p3_opt)? {
                Some(count) => Ok(Some(build_count_batch(
                    out_var,
                    i64::try_from(count).unwrap_or(i64::MAX),
                )?)),
                None => Ok(None),
            }
        },
        fallback,
        "chain+optional COUNT(*)",
    )
}

fn count_chain_optional_tail(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_opt: &Ref,
) -> Result<Option<u64>> {
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3_opt)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&sid2) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&sid3) else {
        // Optional predicate missing => behaves like missing OPTIONAL matches: multiplier = 1 for all c.
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("chain+optional: POST iterator unavailable".into()),
        )?;
        let mut it2 = PsotSubjectCountIter::new(store, g_id, p2_id)?;

        // Collapse p2 to (b, count_edges) and merge with p1: total = Σ_b w(b) * count_{p2}(b).
        // This is still exact when m(c)=1 for all c.
        let mut p2_cur = it2.next_group()?;
        let mut total = 0u64;
        while let Some((b, w)) = it1.next_group()? {
            while let Some((b2, _)) = p2_cur {
                if b2 < b {
                    p2_cur = it2.next_group()?;
                    continue;
                }
                break;
            }
            let count_edges = match p2_cur {
                Some((b2, n)) if b2 == b => {
                    p2_cur = it2.next_group()?;
                    n
                }
                _ => 0u64,
            };
            total = total.saturating_add(w.saturating_mul(count_edges));
        }
        return Ok(Some(total));
    };

    // Precompute m(c) = max(1, count_p3(c)) for all c that appear as subject in p3.
    let mut mult_c: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    while let Some((c, n)) = it3.next_group()? {
        // n is >= 1; store as-is.
        mult_c.insert(c, n.max(1));
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("chain+optional: POST iterator unavailable".into()),
    )?;
    // default_weight=1: OPTIONAL semantics — missing objects get multiplier 1
    let mut it2 = PsotSubjectWeightedSumIter::new(store, g_id, p2_id, &mult_c, 1)?.ok_or(
        QueryError::Internal("chain+optional: PSOT iterator unavailable".into()),
    )?;

    let mut p2_cur = it2.next_group()?;
    let mut total = 0u64;

    while let Some((b, w)) = it1.next_group()? {
        while let Some((b2, _)) = p2_cur {
            if b2 < b {
                p2_cur = it2.next_group()?;
                continue;
            }
            break;
        }
        let sum_m = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        total = total.saturating_add(w.saturating_mul(sum_m));
    }

    Ok(Some(total))
}
