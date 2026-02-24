//! MINUS operator - anti-join semantics
//!
//! Implements SPARQL MINUS semantics (set difference):
//! - For each input row, execute the MINUS patterns with empty seed (fresh scope)
//! - If any result matches the input row on shared variables, filter out that input row
//! - Return rows that don't match anything in the MINUS subtree
//!
//! Key semantics:
//! - MINUS executes with **empty bindings** (fresh scope) - does NOT see outer variables
//! - Matching is done on **shared variables only** (vars in both outer and MINUS)
//! - Empty MINUS results do NOT remove input rows (guard: ignore empty solutions)

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::EmptyOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::sync::Arc;

/// MINUS operator - anti-join semantics (set difference)
///
/// For each input row, executes the MINUS patterns with an empty seed (fresh scope).
/// If any result matches the input row on shared variables, the input row is filtered out.
pub struct MinusOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// MINUS patterns to execute
    minus_patterns: Vec<Pattern>,
    /// Shared variables (appear in both child schema and MINUS patterns)
    shared_vars: Vec<VarId>,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Optional stats for nested query optimization (Arc for cheap cloning in nested operators)
    stats: Option<Arc<StatsView>>,
}

impl MinusOperator {
    /// Create a new MINUS operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `minus_patterns` - Patterns to execute for anti-join matching
    /// * `stats` - Optional stats for nested query optimization (Arc for cheap cloning)
    pub fn new(
        child: BoxedOperator,
        minus_patterns: Vec<Pattern>,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());
        let child_vars: HashSet<VarId> = child.schema().iter().copied().collect();

        // Compute variables referenced in MINUS patterns
        let mut minus_vars: HashSet<VarId> = HashSet::new();
        collect_vars_from_patterns(&minus_patterns, &mut minus_vars);

        // Shared vars are the intersection
        let shared_vars: Vec<VarId> = child_vars.intersection(&minus_vars).copied().collect();

        Self {
            child,
            minus_patterns,
            shared_vars,
            schema,
            state: OperatorState::Created,
            stats,
        }
    }

    /// Check if an input row matches a MINUS result row on shared variables
    fn rows_match(
        &self,
        input_batch: &Batch,
        input_row_idx: usize,
        minus_batch: &Batch,
        minus_row_idx: usize,
    ) -> bool {
        self.shared_vars.iter().all(|&var| {
            let input_binding = input_batch.column(var).map(|col| &col[input_row_idx]);
            let minus_binding = minus_batch.column(var).map(|col| &col[minus_row_idx]);

            // Both must be bound, non-Unbound, and equal for a match (SPARQL semantics)
            matches!(
                (input_binding, minus_binding),
                (Some(i), Some(m)) if !matches!(i, Binding::Unbound)
                                   && !matches!(m, Binding::Unbound)
                                   && i == m
            )
        })
    }
}

#[async_trait]
impl Operator for MinusOperator {
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
            // Get next batch from child
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue, // Skip empty batches
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            // If no shared variables, MINUS can't match anything - return input unchanged
            if self.shared_vars.is_empty() {
                return Ok(Some(input_batch));
            }

            // For each input row, check if it should be kept
            let mut keep_rows: Vec<bool> = vec![true; input_batch.len()];

            #[allow(clippy::needless_range_loop)]
            for row_idx in 0..input_batch.len() {
                // Execute MINUS patterns with empty seed (fresh scope)
                #[allow(clippy::box_default)]
                let seed: BoxedOperator = Box::new(EmptyOperator::new());
                let mut minus_op = build_where_operators_seeded(
                    Some(seed),
                    &self.minus_patterns,
                    self.stats.clone(),
                )?;

                minus_op.open(ctx).await?;

                // Check all MINUS results for a match
                'minus_loop: while let Some(minus_batch) = minus_op.next_batch(ctx).await? {
                    if minus_batch.is_empty() {
                        continue;
                    }

                    // Check each row in the MINUS result
                    for minus_row_idx in 0..minus_batch.len() {
                        if self.rows_match(&input_batch, row_idx, &minus_batch, minus_row_idx) {
                            // Match found - filter out this input row
                            keep_rows[row_idx] = false;
                            break 'minus_loop;
                        }
                    }
                }

                minus_op.close();
            }

            // Build output batch with only kept rows
            let kept_count = keep_rows.iter().filter(|&&k| k).count();
            if kept_count == 0 {
                // All rows filtered out, try next input batch
                continue;
            }

            if kept_count == input_batch.len() {
                // All rows kept, return unchanged
                return Ok(Some(input_batch));
            }

            // Build filtered batch
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(kept_count))
                .collect();

            for (row_idx, keep) in keep_rows.iter().enumerate() {
                if *keep {
                    for (col, var) in columns.iter_mut().zip(self.schema.iter()) {
                        if let Some(input_col) = input_batch.column(*var) {
                            col.push(input_col[row_idx].clone());
                        }
                    }
                }
            }

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could be 0 to child rows, return child estimate as upper bound
        self.child.estimated_rows()
    }
}

/// Collect all variables referenced in a list of patterns
fn collect_vars_from_patterns(patterns: &[Pattern], vars: &mut HashSet<VarId>) {
    for p in patterns {
        for v in p.variables() {
            vars.insert(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;

    #[test]
    fn test_shared_vars_computation() {
        // Create a child with schema [?s, ?name]
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        // MINUS pattern references ?s and ?age
        let minus_patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)), // ?s - shared
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age - not shared
        ))];

        let op = MinusOperator::new(child, minus_patterns, None);

        // Only ?s should be shared
        assert_eq!(op.shared_vars.len(), 1);
        assert!(op.shared_vars.contains(&VarId(0)));
    }

    #[test]
    fn test_minus_schema_preserved() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema.clone(),
        });

        let op = MinusOperator::new(child, vec![], None);

        // Output schema should match child schema
        assert_eq!(op.schema(), &*child_schema);
    }

    // Helper struct for testing
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }
}
