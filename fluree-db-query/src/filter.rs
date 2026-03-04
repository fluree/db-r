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
use crate::execute::build_where_operators_seeded;
use crate::ir::{Expression, FilterValue, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::SeedOperator;
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

/// Check if an expression tree contains any `Expression::Exists` nodes.
pub fn contains_exists(expr: &Expression) -> bool {
    match expr {
        Expression::Exists { .. } => true,
        Expression::Call { args, .. } => args.iter().any(contains_exists),
        Expression::Var(_) | Expression::Const(_) => false,
    }
}

/// Evaluate an EXISTS subquery for a given row.
///
/// Seeds the subquery with the current row's bindings and checks if any
/// result is produced.
async fn eval_exists_for_row(
    patterns: &[Pattern],
    negated: bool,
    batch: &Batch,
    row_idx: usize,
    ctx: &ExecutionContext<'_>,
) -> Result<bool> {
    let seed = SeedOperator::from_batch_row(batch, row_idx);
    let mut exists_op = build_where_operators_seeded(Some(Box::new(seed)), patterns, None, None)?;

    exists_op.open(ctx).await?;

    let has_match = loop {
        match exists_op.next_batch(ctx).await? {
            Some(b) if !b.is_empty() => break true,
            Some(_) => continue,
            None => break false,
        }
    };

    exists_op.close();
    Ok(if negated { !has_match } else { has_match })
}

/// Replace all `Expression::Exists` nodes with pre-computed boolean constants.
///
/// Recursively walks the expression tree. For each EXISTS node, evaluates the
/// subquery for the given row and replaces it with `Const(Bool(result))`.
fn resolve_exists<'a>(
    expr: &'a Expression,
    batch: &'a Batch,
    row_idx: usize,
    ctx: &'a ExecutionContext<'a>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Expression>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expression::Exists { patterns, negated } => {
                let result = eval_exists_for_row(patterns, *negated, batch, row_idx, ctx).await?;
                Ok(Expression::Const(FilterValue::Bool(result)))
            }
            Expression::Call { func, args } => {
                let mut resolved_args = Vec::with_capacity(args.len());
                for arg in args {
                    resolved_args.push(resolve_exists(arg, batch, row_idx, ctx).await?);
                }
                Ok(Expression::Call {
                    func: func.clone(),
                    args: resolved_args,
                })
            }
            // Var and Const have no EXISTS nodes
            _ => Ok(expr.clone()),
        }
    })
}

/// Filter a batch using an expression that contains EXISTS subexpressions.
///
/// For each row, pre-evaluates all EXISTS subqueries (async), substitutes
/// their results as boolean constants, then evaluates the full expression.
async fn filter_batch_with_exists(
    batch: &Batch,
    expr: &Expression,
    schema: &Arc<[VarId]>,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Batch>> {
    let mut keep_indices: Vec<usize> = Vec::new();

    for row_idx in 0..batch.len() {
        let resolved_expr = resolve_exists(expr, batch, row_idx, ctx).await?;
        let Some(row) = batch.row_view(row_idx) else {
            continue;
        };
        let pass = resolved_expr.eval_to_bool(&row, Some(ctx)).unwrap_or(false);
        if pass {
            keep_indices.push(row_idx);
        }
    }

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
    /// Whether the expression contains EXISTS subexpressions (cached)
    has_exists: bool,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(child: BoxedOperator, expr: Expression) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        let has_exists = contains_exists(&expr);
        Self {
            child,
            expr,
            schema,
            state: OperatorState::Created,
            has_exists,
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

            let filtered = if self.has_exists {
                filter_batch_with_exists(&batch, &self.expr, &self.schema, ctx).await?
            } else {
                filter_batch(&batch, &self.expr, &self.schema, ctx)?
            };

            if let Some(filtered) = filtered {
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
