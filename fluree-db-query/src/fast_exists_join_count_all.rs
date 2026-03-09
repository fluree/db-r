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

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, count_rows_psot_for_subjects_sorted,
    fast_path_store, normalize_pred_sid, PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

/// Fused operator that outputs a single-row batch with the COUNT(*) result.
pub struct PredicateExistsJoinCountAllOperator {
    outer_predicate: Ref,
    exists_predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateExistsJoinCountAllOperator {
    pub fn new(
        outer_predicate: Ref,
        exists_predicate: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            outer_predicate,
            exists_predicate,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateExistsJoinCountAllOperator {
    fn schema(&self) -> &[VarId] {
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        if let Some(store) = fast_path_store(ctx) {
            let count = count_exists_join_rows_psot(
                store,
                ctx.binary_g_id,
                &self.outer_predicate,
                &self.exists_predicate,
            )?;
            self.state = OperatorState::Open;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                build_count_batch(self.out_var, count as i64)?,
            )));
            return Ok(());
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "EXISTS-join COUNT(*) fast-path unavailable and no fallback provided".to_string(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let Some(fallback) = &mut self.fallback else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        let b = fallback.next_batch(ctx).await?;
        if b.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(b)
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
    }
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
