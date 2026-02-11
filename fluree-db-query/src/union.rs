//! UNION operator - executes branches with correlated input
//!
//! Implements SPARQL UNION semantics:
//! - For each input row, execute each branch with that row as a seed
//! - Concatenate results from all branches
//! - Normalize output batches to a unified schema (padding missing vars with Unbound)
//!
//! Correlation is essential: each branch must see the bindings from the current
//! input solution (row).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::SeedOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

/// UNION operator - executes branches for each input row (correlated).
pub struct UnionOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Branch patterns (each branch is its own pattern list)
    branches: Vec<Vec<Pattern>>,
    /// Unified schema across child + all branch patterns
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered output batches produced from processing input rows
    output_buffer: VecDeque<Batch>,
    /// Current input batch being processed
    current_input_batch: Option<Batch>,
    /// Current row index in the input batch
    current_input_row: usize,
}

impl UnionOperator {
    /// Create a new correlated UNION operator.
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `branches` - Branch pattern lists (at least one required)
    ///
    pub fn new(child: BoxedOperator, branches: Vec<Vec<Pattern>>) -> Self {
        assert!(!branches.is_empty(), "UNION requires at least one branch");

        // Build unified schema: start with child schema (preserve order),
        // then add any vars referenced/introduced in branch patterns.
        let mut unified_vars: Vec<VarId> = child.schema().to_vec();
        let mut seen: HashSet<VarId> = unified_vars.iter().copied().collect();

        for branch in &branches {
            extend_schema_from_patterns(&mut unified_vars, &mut seen, branch);
        }

        Self {
            child,
            branches,
            schema: Arc::from(unified_vars.into_boxed_slice()),
            state: OperatorState::Created,
            output_buffer: VecDeque::new(),
            current_input_batch: None,
            current_input_row: 0,
        }
    }

    /// Normalize a batch to the unified schema (pad missing vars with Unbound).
    fn normalize_batch(&self, batch: Batch) -> Result<Batch> {
        if batch.is_empty() {
            return Ok(Batch::empty(self.schema.clone())?);
        }

        // Map each output var to its source column (if present) or Unbound padding
        let columns: Vec<Vec<Binding>> = self
            .schema
            .iter()
            .map(|&var| {
                batch
                    .schema()
                    .iter()
                    .position(|&v| v == var)
                    .and_then(|src_idx| batch.column_by_idx(src_idx))
                    .map(|src_col| src_col.to_vec())
                    .unwrap_or_else(|| vec![Binding::Unbound; batch.len()])
            })
            .collect();

        Ok(Batch::new(self.schema.clone(), columns)?)
    }
}

#[async_trait]
impl Operator for UnionOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.output_buffer.clear();
        self.current_input_batch = None;
        self.current_input_row = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            // If we already have buffered output, return it.
            if let Some(batch) = self.output_buffer.pop_front() {
                return Ok(Some(batch));
            }

            // Ensure we have an input batch to process.
            if self.current_input_batch.is_none()
                || self.current_input_row
                    >= self
                        .current_input_batch
                        .as_ref()
                        .map(|b| b.len())
                        .unwrap_or(0)
            {
                // Fetch next non-empty batch from child.
                let next = match self.child.next_batch(ctx).await? {
                    Some(b) if !b.is_empty() => b,
                    Some(_) => continue,
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                };
                self.current_input_batch = Some(next);
                self.current_input_row = 0;
            }

            // Process one input row: execute all branches with this row as seed.
            let input_batch = self.current_input_batch.as_ref().unwrap().clone();
            let row_idx = self.current_input_row;
            self.current_input_row += 1;

            for branch_patterns in &self.branches {
                let seed = SeedOperator::from_batch_row(&input_batch, row_idx);
                let mut branch_op =
                    build_where_operators_seeded(Some(Box::new(seed)), branch_patterns, None)?;

                branch_op.open(ctx).await?;
                while let Some(batch) = branch_op.next_batch(ctx).await? {
                    if batch.is_empty() {
                        continue;
                    }
                    self.output_buffer.push_back(self.normalize_batch(batch)?);
                }
                branch_op.close();
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.output_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Upper bound: child rows * number of branches.
        self.child
            .estimated_rows()
            .map(|r| r.saturating_mul(self.branches.len()))
    }
}

fn extend_schema_from_patterns(
    schema: &mut Vec<VarId>,
    seen: &mut HashSet<VarId>,
    patterns: &[Pattern],
) {
    for p in patterns {
        match p {
            Pattern::Triple(tp) => {
                for v in tp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Filter(_) => {}
            Pattern::Optional(inner)
            | Pattern::Minus(inner)
            | Pattern::Exists(inner)
            | Pattern::NotExists(inner) => extend_schema_from_patterns(schema, seen, inner),
            Pattern::Union(branches) => {
                for b in branches {
                    extend_schema_from_patterns(schema, seen, b);
                }
            }
            Pattern::Bind { var, .. } => {
                if seen.insert(*var) {
                    schema.push(*var);
                }
            }
            Pattern::Values { vars, .. } => {
                for v in vars {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
            }
            Pattern::PropertyPath(pp) => {
                for v in pp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Subquery(sq) => {
                // Subquery contributes its select variables to the schema
                for v in &sq.select {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
            }
            Pattern::IndexSearch(isp) => {
                // Index search contributes id, score, and ledger variables to the schema
                for v in isp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::VectorSearch(vsp) => {
                // Vector search contributes id, score, and ledger variables to the schema
                for v in vsp.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::R2rml(r2rml) => {
                // R2RML pattern contributes subject and object variables to the schema
                for v in r2rml.variables() {
                    if seen.insert(v) {
                        schema.push(v);
                    }
                }
            }
            Pattern::Graph {
                name,
                patterns: inner,
            } => {
                // Graph pattern contributes variables from inner patterns and the graph variable (if any)
                if let crate::ir::GraphName::Var(v) = name {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
                extend_schema_from_patterns(schema, seen, inner);
            }
            Pattern::Service(sp) => {
                // Service pattern contributes variables from inner patterns and the endpoint variable (if any)
                if let crate::ir::ServiceEndpoint::Var(v) = &sp.endpoint {
                    if seen.insert(*v) {
                        schema.push(*v);
                    }
                }
                extend_schema_from_patterns(schema, seen, &sp.patterns);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed::EmptyOperator;
    use fluree_db_core::Sid;
    use std::sync::Arc;

    #[test]
    fn test_union_operator_schema_computation() {
        // Child schema has ?s, branches introduce ?n and ?e.
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![
            vec![Pattern::Triple(crate::pattern::TriplePattern::new(
                crate::pattern::Term::Var(VarId(0)),
                crate::pattern::Term::Sid(Sid::new(100, "name")),
                crate::pattern::Term::Var(VarId(1)),
            ))],
            vec![Pattern::Triple(crate::pattern::TriplePattern::new(
                crate::pattern::Term::Var(VarId(0)),
                crate::pattern::Term::Sid(Sid::new(100, "email")),
                crate::pattern::Term::Var(VarId(2)),
            ))],
        ];

        let op = UnionOperator::new(child, branches);
        assert_eq!(op.schema(), &[VarId(0), VarId(1), VarId(2)]);
    }

    #[test]
    fn test_union_operator_allows_position_0_via_empty_seed_child() {
        // UNION at position 0 should still be able to run using an EmptyOperator child.
        // Here we only validate it constructs; runtime behavior is covered by execute.rs integration tests.
        let empty = EmptyOperator::new();
        let child: BoxedOperator = Box::new(empty);
        let branches = vec![vec![], vec![]];
        let op = UnionOperator::new(child, branches);
        assert_eq!(op.schema().len(), 0);
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
