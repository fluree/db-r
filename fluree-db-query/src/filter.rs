//! Filter operator
//!
//! This module provides the FilterOperator which wraps a child operator
//! and filters rows based on a predicate expression.
//!
//! # Filter Evaluation Semantics
//!
//! This uses **two-valued logic** (true/false), not SQL 3-valued NULL logic:
//!
//! - **Unbound variables**: Comparisons involving unbound vars yield `false`
//! - **Type mismatches**: Comparisons between incompatible types yield `false`
//!   (except `!=` which yields `true` for mismatched types)
//! - **NaN**: Comparisons involving NaN yield `false` (except `!=` → `true`)
//! - **Logical operators**: Standard boolean logic (AND, OR, NOT)
//!
//! Note: `NOT(unbound_comparison)` evaluates to `true` because the inner
//! comparison returns `false`, which is then negated. This differs from
//! SQL NULL semantics where NULL comparisons propagate.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// Filter rows from a batch using two-valued logic.
///
/// Evaluates `expr` for each row in `batch`. Rows where the expression evaluates
/// to `true` are kept; rows that evaluate to `false` or encounter an error
/// (type mismatch, unbound variable) are filtered out.
///
/// Returns `None` if no rows pass the filter.
pub fn filter_batch(
    batch: &Batch,
    expr: &Expression,
    schema: &Arc<[VarId]>,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Batch>> {
    let keep_indices: Vec<usize> = (0..batch.len())
        .filter_map(|row_idx| {
            let row = batch.row_view(row_idx)?;
            expr.eval_to_bool(&row, Some(ctx))
                .ok()
                .filter(|&pass| pass)
                .map(|_| row_idx)
        })
        .collect();

    if keep_indices.is_empty() {
        return Ok(None);
    }

    let columns: Vec<Vec<Binding>> = (0..schema.len())
        .map(|col_idx| {
            let src_col = batch
                .column_by_idx(col_idx)
                .expect("batch schema must match operator schema");
            keep_indices
                .iter()
                .map(|&row_idx| src_col[row_idx].clone())
                .collect()
        })
        .collect();

    Ok(Some(Batch::new(schema.clone(), columns)?))
}

/// Filter operator - applies a predicate to each row from child
///
/// Rows where the filter evaluates to `false` or encounters an error
/// (type mismatch, unbound var) are filtered out.
pub struct FilterOperator {
    /// Child operator providing input rows
    child: BoxedOperator,
    /// Filter expression to evaluate
    expr: Expression,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(child: BoxedOperator, expr: Expression) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            expr,
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the filter expression
    pub fn expr(&self) -> &Expression {
        &self.expr
    }
}

#[async_trait]
impl Operator for FilterOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
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

            if let Some(filtered) = filter_batch(&batch, &self.expr, &self.schema, ctx)? {
                return Ok(Some(filtered));
            }
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
