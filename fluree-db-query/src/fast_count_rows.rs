//! Fast-path: `COUNT(*)` / `COUNT(?x)` for a single triple `?s <p> ?o`.
//!
//! For a single triple pattern with a bound predicate and no filters, every
//! matching row produces exactly one solution mapping with all vars bound.
//! Therefore:
//! - `COUNT(*) == COUNT(?s) == COUNT(?o)`
//!   and the answer can be obtained directly from PSOT leaflet directory metadata
//!   (`row_count`) without scanning or decoding any columns (including strings).

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, count_rows_for_predicate_psot, fast_path_store, normalize_pred_sid,
    PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;

/// Fused operator that outputs a single-row batch containing the count.
pub struct PredicateCountRowsOperator {
    predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateCountRowsOperator {
    pub fn new(predicate: Ref, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            predicate,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateCountRowsOperator {
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
            let pred_sid = normalize_pred_sid(store, &self.predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                // Predicate not present in the persisted dict — empty result.
                self.state = OperatorState::Open;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                    build_count_batch(self.out_var, 0)?,
                )));
                return Ok(());
            };

            let count = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
            self.state = OperatorState::Open;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                build_count_batch(self.out_var, count as i64)?,
            )));
            return Ok(());
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "COUNT rows fast-path unavailable and no fallback provided".to_string(),
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
