//! Fast-path for `COUNT(*)` where an outer triple is filtered by an EXISTS-star block.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p_outer> ?o1 .
//!   FILTER EXISTS { ?s <p2> ?o2 . ?s <p3> ?o3 . }
//! }
//! ```
//!
//! Semantics:
//! - The EXISTS block is a conjunctive constraint on `?s`: keep outer rows whose subject has
//!   at least one match for each inner predicate.
//! - COUNT(*) counts outer solution rows, so the answer is:
//!   `sum_{s in (S2 ∩ S3)} count_{p_outer}(s)`
//!   where `Si = { s | s pi ?oi }`.
//!
//! Implementation:
//! - Build sorted distinct subject lists for each inner predicate from PSOT (SId only).
//! - Intersect them (streaming two-pointer intersection).
//! - Stream grouped subject counts for the outer predicate from PSOT and merge-join with the
//!   intersected subject list to sum counts.
//!
//! Avoids per-row EXISTS evaluation, join row materialization, and value decoding.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, count_rows_psot_for_subjects_sorted,
    fast_path_store, intersect_many_sorted, normalize_pred_sid, PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub struct PredicateExistsStarJoinCountAllOperator {
    outer_predicate: Ref,
    exists_predicates: Vec<Ref>,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateExistsStarJoinCountAllOperator {
    pub fn new(
        outer_predicate: Ref,
        exists_predicates: Vec<Ref>,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            outer_predicate,
            exists_predicates,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateExistsStarJoinCountAllOperator {
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
            let count = count_exists_star_rows_psot(
                store,
                ctx.binary_g_id,
                &self.outer_predicate,
                &self.exists_predicates,
            )?;
            self.state = OperatorState::Open;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                build_count_batch(self.out_var, count as i64)?,
            )));
            return Ok(());
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "EXISTS-star COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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

fn count_exists_star_rows_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicate: &Ref,
    exists_preds: &[Ref],
) -> Result<u64> {
    let outer_sid = normalize_pred_sid(store, outer_predicate)?;
    let Some(p_outer) = store.sid_to_p_id(&outer_sid) else {
        return Ok(0);
    };

    if exists_preds.is_empty() {
        return Err(QueryError::Internal(
            "EXISTS-star fast path requires at least one predicate".to_string(),
        ));
    }

    let mut subject_lists: Vec<Vec<u64>> = Vec::with_capacity(exists_preds.len());
    for p in exists_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(0);
        };
        subject_lists.push(collect_subjects_for_predicate_sorted(store, g_id, pid)?);
    }

    let subjects = intersect_many_sorted(subject_lists);
    count_rows_psot_for_subjects_sorted(store, g_id, p_outer, &subjects)
}
