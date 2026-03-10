//! Fast-path: COUNT(*) for a single required triple with an OPTIONAL 2-hop chain.
//!
//! Targets benchmark query shape:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d . } }`
//!
//! Semantics:
//! For each required row `(a,b)`, the OPTIONAL contributes `max(1, count_chain(b))` rows where
//! `count_chain(b) = Σ_{c in p2(b)} count_{p3}(c)`.
//!
//! Therefore:
//!   total = Σ_b count_{p1}(b) * max(1, Σ_{c in p2(b)} count_{p3}(c))
//!
//! We compute this streaming:
//! - Build `n3(c) = count_{p3}(c)` by scanning PSOT(p3) grouped by subject.
//! - Stream POST(p1) grouped by object `b` to get `w(b) = count_{p1}(b)`.
//! - Stream PSOT(p2) grouped by subject `b` and sum `n3(c)` across its objects `c`.
//! - Merge-join on `b` and apply `max(1, ...)`.
//!
//! This avoids materializing join rows and avoids per-row OPTIONAL execution.

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

pub fn predicate_optional_chain_head_count_all(
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
                tracing::debug!(
                    "optional chain-head COUNT(*) fast-path disabled by execution context, falling back (history_mode={}, from_t={:?}, has_policy={}, overlay_epoch={:?})",
                    ctx.history_mode,
                    ctx.from_t,
                    ctx.policy_enforcer.is_some(),
                    ctx.overlay.map(|o| o.epoch())
                );
                return Ok(None);
            };
            if let Some(count) = count_optional_chain_head(store, ctx.binary_g_id, &p1, &p2, &p3)? {
                tracing::debug!(
                    "using optional chain-head COUNT(*) fast-path (p1={:?}, p2={:?}, p3={:?})",
                    p1,
                    p2,
                    p3
                );
                Ok(Some(build_count_batch(
                    out_var,
                    i64::try_from(count).unwrap_or(i64::MAX),
                )?))
            } else {
                tracing::debug!(
                    "optional chain-head COUNT(*) fast-path not applicable, falling back (p1={:?}, p2={:?}, p3={:?})",
                    p1,
                    p2,
                    p3
                );
                Ok(None)
            }
        },
        fallback,
        "optional chain-head COUNT(*)",
    )
}

fn count_optional_chain_head(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&sid2) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&sid3) else {
        // Optional chain can never match => multiplier is 1 for all b.
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
        )?;
        let mut total = 0u64;
        while let Some((_b, w)) = it1.next_group()? {
            total += w;
        }
        return Ok(Some(total));
    };

    // Precompute n3(c) = count_{p3}(c).
    let mut n3: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    while let Some((c, n)) = it3.next_group()? {
        n3.insert(c, n);
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
    )?;
    // default_weight=0: objects not in n3 contribute nothing to the sum
    let mut it2 = PsotSubjectWeightedSumIter::new(store, g_id, p2_id, &n3, 0)?.ok_or(
        QueryError::Internal("optional chain-head: PSOT iterator unavailable".into()),
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
        let sum_n3 = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        let mult = if sum_n3 == 0 { 1 } else { sum_n3 };
        total = total.saturating_add(w.saturating_mul(mult));
    }

    Ok(Some(total))
}
