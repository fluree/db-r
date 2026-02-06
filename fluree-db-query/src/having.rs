//! HAVING operator - filters grouped/aggregated results
//!
//! Implements SPARQL HAVING semantics:
//! - Filters rows after GROUP BY and aggregation
//! - Uses the same filter expression evaluation as WHERE FILTER
//! - Operates on aggregate results (not Grouped values)
//!
//! # Example
//!
//! ```text
//! SELECT ?city (COUNT(?person) AS ?count)
//! WHERE { ?person :city ?city }
//! GROUP BY ?city
//! HAVING (COUNT(?person) > 10)
//! ```
//!
//! The HAVING clause filters out cities with 10 or fewer people.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::filter::evaluate_with_context;
use crate::ir::FilterExpr;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::Storage;
use std::sync::Arc;

/// HAVING operator - filters rows after GROUP BY/aggregation
///
/// This is functionally identical to FilterOperator but conceptually
/// applies to grouped/aggregated results.
pub struct HavingOperator<S: Storage + 'static> {
    /// Child operator (typically AggregateOperator or GroupByOperator)
    child: BoxedOperator<S>,
    /// Filter expression to evaluate
    expr: FilterExpr,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl<S: Storage + 'static> HavingOperator<S> {
    /// Create a new HAVING operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator (typically GroupByOperator or AggregateOperator)
    /// * `expr` - Filter expression to evaluate
    pub fn new(child: BoxedOperator<S>, expr: FilterExpr) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            expr,
            schema,
            state: OperatorState::Created,
        }
    }
}

#[async_trait]
impl<S: Storage + 'static> Operator<S> for HavingOperator<S> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
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

            // Filter rows that match the HAVING expression
            let mut output_columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(batch.len()))
                .collect();
            let mut rows_added = 0;

            for row_idx in 0..batch.len() {
                let row = match batch.row_view(row_idx) {
                    Some(r) => r,
                    None => continue,
                };

                // Evaluate the HAVING expression
                let passes = evaluate_with_context(&self.expr, &row, ctx)?;

                if passes {
                    // Copy this row to output
                    for (col_idx, output_col) in output_columns.iter_mut().enumerate() {
                        output_col.push(batch.get_by_col(row_idx, col_idx).clone());
                    }
                    rows_added += 1;
                }
            }

            if rows_added > 0 {
                return Ok(Some(Batch::new(self.schema.clone(), output_columns)?));
            }
            // If no rows passed the filter, continue to next batch
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // HAVING typically reduces row count, but we can't know by how much
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::CompareOp;
    use crate::seed::SeedOperator;
    use fluree_db_core::{Db, FlakeValue, MemoryStorage, Sid};

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn make_test_db() -> Db<MemoryStorage> {
        Db::genesis(MemoryStorage::new(), "test/main")
    }

    #[tokio::test]
    async fn test_having_filters_rows() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars);

        // Create a batch with counts: 5, 15, 8, 20
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("NYC".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("LA".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("CHI".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("SF".into()), Sid::new(2, "string")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(5), xsd_long()),
                Binding::lit(FlakeValue::Long(15), xsd_long()),
                Binding::lit(FlakeValue::Long(8), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        // Create an operator that yields this batch
        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl<S: Storage + 'static> Operator<S> for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_, S>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator<MemoryStorage> = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // HAVING ?count > 10
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(1))), // ?count
            right: Box::new(FilterExpr::Const(FilterValue::Long(10))),
        };

        let mut op = HavingOperator::new(child, expr);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        // Should only have 2 rows (LA=15, SF=20)
        assert_eq!(result.len(), 2);

        // Verify the cities
        let city0 = result.get_by_col(0, 0);
        let city1 = result.get_by_col(1, 0);
        if let Binding::Lit { val, .. } = city0 {
            assert_eq!(*val, FlakeValue::String("LA".into()));
        } else {
            panic!("Expected Lit binding");
        }
        if let Binding::Lit { val, .. } = city1 {
            assert_eq!(*val, FlakeValue::String("SF".into()));
        } else {
            panic!("Expected Lit binding");
        }

        op.close();
    }

    #[tokio::test]
    async fn test_having_no_matches() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars);

        // Create a batch with all small counts
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("A".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("B".into()), Sid::new(2, "string")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl<S: Storage + 'static> Operator<S> for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_, S>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator<MemoryStorage> = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // HAVING ?count > 100 (no rows match)
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(1))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(100))),
        };

        let mut op = HavingOperator::new(child, expr);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        // No rows should match
        assert!(result.is_none());

        op.close();
    }

    #[tokio::test]
    async fn test_having_schema_preserved() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(3), xsd_long())],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator<MemoryStorage> = Box::new(SeedOperator::from_batch_row(&batch, 0));

        // Any expression that passes
        let expr = FilterExpr::Const(FilterValue::Bool(true));

        let mut op = HavingOperator::new(seed, expr);
        op.open(&ctx).await.unwrap();

        // Schema should be preserved
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert_eq!(op.schema()[2], VarId(2));

        op.close();
    }
}
