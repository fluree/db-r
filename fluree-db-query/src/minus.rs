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
/// Executes the MINUS patterns once with an empty seed (fresh scope), materializes
/// all results, then filters input rows that match on shared variables.
///
/// The MINUS subtree is always uncorrelated (empty seed, no outer variable access),
/// so its results are invariant across input rows. Materializing once converts the
/// cost from O(N * subtree_execution) to O(subtree_execution + N * M) where
/// M = materialized MINUS rows.
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
    /// Materialized MINUS results (populated in open(), used in next_batch())
    materialized_minus: Vec<Batch>,
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
            materialized_minus: Vec::new(),
        }
    }

    /// Check if an input row matches a MINUS result row on shared variables.
    ///
    /// Per W3C SPARQL §8.3, MINUS removes an input row µ when there exists a
    /// MINUS row µ' such that:
    ///   1. µ and µ' are **compatible**: for every variable in dom(µ) ∩ dom(µ'),
    ///      µ(v) = µ'(v).
    ///   2. dom(µ) ∩ dom(µ') ≠ ∅: at least one shared variable is bound in both.
    ///
    /// A variable that is Unbound (e.g. from an unsatisfied OPTIONAL) is NOT in
    /// the solution's domain, so it is trivially compatible — it must not block
    /// the match.
    fn rows_match(
        &self,
        input_batch: &Batch,
        input_row_idx: usize,
        minus_batch: &Batch,
        minus_row_idx: usize,
    ) -> bool {
        let mut has_shared_bound = false;

        let compatible = self.shared_vars.iter().all(|&var| {
            let input_binding = input_batch.column(var).map(|col| &col[input_row_idx]);
            let minus_binding = minus_batch.column(var).map(|col| &col[minus_row_idx]);

            match (input_binding, minus_binding) {
                // Both matchable (not Unbound/Poisoned): check equality
                (Some(i), Some(m)) if i.is_matchable() && m.is_matchable() => {
                    has_shared_bound = true;
                    i == m
                }
                // One or both absent/Unbound/Poisoned: not in dom intersection,
                // trivially compatible
                _ => true,
            }
        });

        // MINUS fires only if compatible AND at least one shared variable is bound in both
        compatible && has_shared_bound
    }
}

#[async_trait]
impl Operator for MinusOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Materialize the MINUS subtree once with an empty seed (fresh scope).
        // MINUS is always uncorrelated — the subtree doesn't see outer variables.
        if !self.shared_vars.is_empty() {
            #[allow(clippy::box_default)]
            let seed: BoxedOperator = Box::new(EmptyOperator::new());
            let mut minus_op = build_where_operators_seeded(
                Some(seed),
                &self.minus_patterns,
                self.stats.clone(),
                None,
            )?;

            minus_op.open(ctx).await?;

            while let Some(batch) = minus_op.next_batch(ctx).await? {
                if !batch.is_empty() {
                    self.materialized_minus.push(batch);
                }
            }

            minus_op.close();
        }

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

            // If MINUS subtree produced no results, nothing can be removed
            if self.materialized_minus.is_empty() {
                return Ok(Some(input_batch));
            }

            // For each input row, check against materialized MINUS results
            let mut keep_rows: Vec<bool> = vec![true; input_batch.len()];

            #[allow(clippy::needless_range_loop)]
            for row_idx in 0..input_batch.len() {
                'outer: for minus_batch in &self.materialized_minus {
                    for minus_row_idx in 0..minus_batch.len() {
                        if self.rows_match(&input_batch, row_idx, minus_batch, minus_row_idx) {
                            keep_rows[row_idx] = false;
                            break 'outer;
                        }
                    }
                }
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

    /// Helper: build a MinusOperator with a given child schema and shared vars
    fn make_minus_with_shared(shared: Vec<VarId>) -> MinusOperator {
        let child_schema: Arc<[VarId]> = Arc::from(shared.clone().into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema.clone(),
        });
        MinusOperator {
            child,
            minus_patterns: vec![],
            shared_vars: shared,
            schema: child_schema,
            state: OperatorState::Created,
            stats: None,
            materialized_minus: Vec::new(),
        }
    }

    /// Helper: build a 1-row Batch with given bindings
    fn batch_1row(schema: &[VarId], bindings: Vec<Binding>) -> Batch {
        let arc_schema: Arc<[VarId]> = Arc::from(schema.to_vec().into_boxed_slice());
        let columns: Vec<Vec<Binding>> = bindings.into_iter().map(|b| vec![b]).collect();
        Batch::new(arc_schema, columns).unwrap()
    }

    #[test]
    fn rows_match_both_bound_equal() {
        let op = make_minus_with_shared(vec![VarId(0)]);
        let sid = Sid::new(100, "x");
        let input = batch_1row(&[VarId(0)], vec![Binding::Sid(sid.clone())]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Sid(sid)]);
        assert!(op.rows_match(&input, 0, &minus, 0));
    }

    #[test]
    fn rows_match_both_bound_unequal() {
        let op = make_minus_with_shared(vec![VarId(0)]);
        let input = batch_1row(&[VarId(0)], vec![Binding::Sid(Sid::new(100, "x"))]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Sid(Sid::new(200, "y"))]);
        assert!(!op.rows_match(&input, 0, &minus, 0));
    }

    #[test]
    fn rows_match_input_unbound_trivially_compatible() {
        // Input has Unbound, MINUS has a value — trivially compatible
        // but no shared bound variables → match should NOT fire
        let op = make_minus_with_shared(vec![VarId(0)]);
        let input = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Sid(Sid::new(100, "x"))]);
        assert!(
            !op.rows_match(&input, 0, &minus, 0),
            "no shared bound var → match must not fire"
        );
    }

    #[test]
    fn rows_match_minus_unbound_trivially_compatible() {
        // MINUS has Unbound, input has a value — trivially compatible
        // but no shared bound variables → match should NOT fire
        let op = make_minus_with_shared(vec![VarId(0)]);
        let input = batch_1row(&[VarId(0)], vec![Binding::Sid(Sid::new(100, "x"))]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        assert!(
            !op.rows_match(&input, 0, &minus, 0),
            "no shared bound var → match must not fire"
        );
    }

    #[test]
    fn rows_match_both_unbound() {
        let op = make_minus_with_shared(vec![VarId(0)]);
        let input = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Unbound]);
        assert!(
            !op.rows_match(&input, 0, &minus, 0),
            "both unbound → no shared bound var → no match"
        );
    }

    #[test]
    fn rows_match_poisoned_trivially_compatible() {
        // Poisoned (from failed OPTIONAL) is not in domain
        let op = make_minus_with_shared(vec![VarId(0)]);
        let input = batch_1row(&[VarId(0)], vec![Binding::Poisoned]);
        let minus = batch_1row(&[VarId(0)], vec![Binding::Sid(Sid::new(100, "x"))]);
        assert!(
            !op.rows_match(&input, 0, &minus, 0),
            "poisoned is not matchable → no shared bound var"
        );
    }

    #[test]
    fn rows_match_multi_var_one_unbound_one_equal() {
        // Two shared vars: var0 is bound+equal, var1 has input Unbound
        // Compatible (unbound is trivially ok) AND has shared bound (var0)
        let op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let sid = Sid::new(100, "x");
        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::Sid(sid.clone()), Binding::Unbound],
        );
        let minus = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::Sid(sid), Binding::Sid(Sid::new(200, "y"))],
        );
        assert!(
            op.rows_match(&input, 0, &minus, 0),
            "var0 is shared+equal, var1 unbound in input → compatible + has_shared_bound"
        );
    }

    #[test]
    fn rows_match_multi_var_one_equal_one_unequal() {
        // Two shared vars: var0 bound+equal, var1 bound+unequal → NOT compatible
        let op = make_minus_with_shared(vec![VarId(0), VarId(1)]);
        let sid = Sid::new(100, "x");
        let input = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::Sid(sid.clone()), Binding::Sid(Sid::new(300, "a"))],
        );
        let minus = batch_1row(
            &[VarId(0), VarId(1)],
            vec![Binding::Sid(sid), Binding::Sid(Sid::new(400, "b"))],
        );
        assert!(
            !op.rows_match(&input, 0, &minus, 0),
            "var1 disagrees → incompatible"
        );
    }
}
