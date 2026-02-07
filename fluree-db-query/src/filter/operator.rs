//! Filter operator
//!
//! Wraps a child operator and filters rows based on a predicate expression.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::FilterExpr;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::Storage;
use std::sync::Arc;
use tracing::Instrument;

use super::eval::evaluate_with_context;

/// Filter operator - applies a predicate to each row from child
///
/// Rows where the filter evaluates to `false` or encounters an error
/// (type mismatch, unbound var) are filtered out.
pub struct FilterOperator<S: Storage + 'static> {
    /// Child operator providing input rows
    child: BoxedOperator<S>,
    /// Filter expression to evaluate
    expr: FilterExpr,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl<S: Storage + 'static> FilterOperator<S> {
    /// Create a new filter operator
    pub fn new(child: BoxedOperator<S>, expr: FilterExpr) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            expr,
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the filter expression
    pub fn expr(&self) -> &FilterExpr {
        &self.expr
    }
}

#[async_trait]
impl<S: Storage + 'static> Operator<S> for FilterOperator<S> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
        async {
            self.child.open(ctx).await?;
            self.state = OperatorState::Open;
            Ok(())
        }
        .instrument(tracing::trace_span!("filter"))
        .await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if batch.is_empty() {
                continue;
            }

            // Collect row indices where filter evaluates to true
            let keep_indices: Vec<usize> = (0..batch.len())
                .filter_map(|row_idx| {
                    let row = batch.row_view(row_idx)?;
                    evaluate_with_context(&self.expr, &row, ctx)
                        .ok()
                        .filter(|&pass| pass)
                        .map(|_| row_idx)
                })
                .collect();

            if keep_indices.is_empty() {
                continue;
            }

            // Build filtered batch by selecting kept rows from each column
            let num_cols = self.schema.len();
            let columns: Vec<Vec<Binding>> = (0..num_cols)
                .map(|col_idx| {
                    let src_col = batch
                        .column_by_idx(col_idx)
                        .expect("child batch schema must match FilterOperator schema");
                    keep_indices
                        .iter()
                        .map(|&row_idx| src_col[row_idx].clone())
                        .collect()
                })
                .collect();

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: child rows * selectivity (assume 50% for now)
        self.child.estimated_rows().map(|r| r / 2)
    }
}
