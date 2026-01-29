//! BIND operator - evaluates expression and binds result to variable
//!
//! The BindOperator evaluates a filter expression and binds the result
//! to a variable with clobber prevention:
//!
//! - If variable is Unbound: bind the computed value
//! - If variable has the SAME value: pass through (compatible)
//! - If variable has a DIFFERENT value: drop the row (clobber prevention)
//!
//! Evaluation errors produce `Binding::Unbound` (NOT `Binding::Poisoned`).
//! Poisoned is reserved strictly for OPTIONAL semantics.
//!
//! For example, `BIND(?x + 10 AS ?y)` evaluates the expression for each input row and binds the result to `?y`.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::filter::{evaluate_to_binding_with_context, evaluate_to_binding_with_context_strict};
use crate::ir::FilterExpr;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{NodeCache, Storage};
use std::sync::Arc;

/// BIND operator - evaluates expression and binds to variable
///
/// Implements SPARQL BIND semantics with clobber prevention:
/// - New variable: bind computed value
/// - Same value: pass through
/// - Different value: drop row
pub struct BindOperator<S: Storage + 'static, C: NodeCache + 'static> {
    /// Child operator providing input solutions
    child: BoxedOperator<S, C>,
    /// Variable to bind the result to
    var: VarId,
    /// Expression to evaluate
    expr: FilterExpr,
    /// Output schema (child schema with var added if new)
    schema: Arc<[VarId]>,
    /// Position of var in output schema
    var_position: usize,
    /// Whether this is a new variable (not in child schema)
    is_new_var: bool,
    /// Operator state
    state: OperatorState,
}

impl<S: Storage + 'static, C: NodeCache + 'static> BindOperator<S, C> {
    /// Create a new BIND operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator providing input solutions
    /// * `var` - Variable to bind the computed value to
    /// * `expr` - Expression to evaluate
    pub fn new(child: BoxedOperator<S, C>, var: VarId, expr: FilterExpr) -> Self {
        let child_schema = child.schema();

        // Check if var already exists in child schema
        let existing_pos = child_schema.iter().position(|&v| v == var);

        let (schema, var_position, is_new_var) = match existing_pos {
            Some(pos) => {
                // Variable exists - schema stays the same
                (Arc::from(child_schema.to_vec().into_boxed_slice()), pos, false)
            }
            None => {
                // New variable - add to schema
                let mut new_schema = child_schema.to_vec();
                let pos = new_schema.len();
                new_schema.push(var);
                (Arc::from(new_schema.into_boxed_slice()), pos, true)
            }
        };

        Self {
            child,
            var,
            expr,
            schema,
            var_position,
            is_new_var,
            state: OperatorState::Created,
        }
    }

}

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for BindOperator<S, C> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            // Get next batch from child
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if input_batch.is_empty() {
                continue;
            }

            let child_num_cols = self.child.schema().len();
            let num_output_cols = self.schema.len();

            // Build output columns
            let mut output_columns: Vec<Vec<Binding>> = (0..num_output_cols)
                .map(|_| Vec::with_capacity(input_batch.len()))
                .collect();

            // Process each row
            for row_idx in 0..input_batch.len() {
                let row_view = input_batch.row_view(row_idx).unwrap();

                // Evaluate expression (errors become Unbound)
                let computed = if ctx.strict_bind_errors {
                    evaluate_to_binding_with_context_strict(&self.expr, &row_view, Some(ctx))?
                } else {
                    evaluate_to_binding_with_context(&self.expr, &row_view, Some(ctx))
                };

                // Check clobber prevention if variable already exists
                let keep_row = if self.is_new_var {
                    // New variable - always keep
                    true
                } else {
                    // Existing variable - check compatibility
                    let existing = row_view.get(self.var);
                    match existing {
                        Some(Binding::Unbound) | None => true, // Unbound - can bind
                        Some(existing_val) => {
                            // Check if same value
                            existing_val == &computed || matches!(computed, Binding::Unbound)
                        }
                    }
                };

                if !keep_row {
                    // Clobber detected - drop this row
                    continue;
                }

                // Copy child columns
                for col_idx in 0..child_num_cols {
                    let binding = input_batch.get_by_col(row_idx, col_idx).clone();
                    output_columns[col_idx].push(binding);
                }

                // Add bound value
                if self.is_new_var {
                    // New column at the end
                    output_columns[self.var_position].push(computed);
                } else {
                    // Existing variable:
                    // - If computed is Unbound (e.g. eval error), do NOT clobber an existing bound value.
                    // - Otherwise, overwrite the existing column for this row with the computed value.
                    if matches!(computed, Binding::Unbound) {
                        // Keep the existing value we already pushed.
                    } else {
                        // Replace the last pushed value for this column.
                        output_columns[self.var_position].pop();
                        output_columns[self.var_position].push(computed);
                    }
                }
            }

            // Check if any rows remain after filtering
            if output_columns.first().map(|c| c.is_empty()).unwrap_or(true) {
                continue;
            }

            return Ok(Some(Batch::new(self.schema.clone(), output_columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Some rows may be dropped due to clobber prevention
        // Estimate same as child (upper bound)
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::ir::FilterValue;
    use fluree_db_core::{MemoryStorage, NoCache};

    type TestCache = NoCache;

    #[test]
    fn test_bind_operator_new_var_schema() {
        // Child has ?a, BIND adds ?b
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema { schema: child_schema });

        let expr = FilterExpr::Const(FilterValue::Long(42));
        let op = BindOperator::<MemoryStorage, TestCache>::new(child, VarId(1), expr);

        // Output schema should be [?a, ?b]
        assert_eq!(op.schema().len(), 2);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert!(op.is_new_var);
    }

    #[test]
    fn test_bind_operator_existing_var_schema() {
        // Child has ?a ?b, BIND to ?a (existing)
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema { schema: child_schema });

        let expr = FilterExpr::Const(FilterValue::Long(42));
        let op = BindOperator::<MemoryStorage, TestCache>::new(child, VarId(0), expr);

        // Schema should stay [?a, ?b]
        assert_eq!(op.schema().len(), 2);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert!(!op.is_new_var);
        assert_eq!(op.var_position, 0);
    }

    // Helper struct for testing
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }
}
