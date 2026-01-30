//! Project operator - selects and reorders columns
//!
//! The ProjectOperator takes a child operator and produces batches
//! containing only the specified variables in the specified order.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{NodeCache, Storage};

/// Project operator - selects and reorders columns from child
pub struct ProjectOperator<S: Storage + 'static, C: NodeCache + 'static> {
    /// Child operator
    child: BoxedOperator<S, C>,
    /// Variables to project (in output order)
    vars: Vec<VarId>,
    /// Operator state
    state: OperatorState,
}

impl<S: Storage + 'static, C: NodeCache + 'static> ProjectOperator<S, C> {
    /// Create a new project operator
    pub fn new(child: BoxedOperator<S, C>, vars: Vec<VarId>) -> Self {
        Self {
            child,
            vars,
            state: OperatorState::Created,
        }
    }
}

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for ProjectOperator<S, C> {
    fn schema(&self) -> &[VarId] {
        &self.vars
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        let _span = tracing::trace_span!("project").entered();
        drop(_span);
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // Get batch from child
        let batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        // Project to requested variables
        // If the projection matches exactly, return as-is
        if batch.schema() == self.vars.as_slice() {
            return Ok(Some(batch));
        }

        // Otherwise project
        match batch.project(&self.vars) {
            Some(projected) => Ok(Some(projected)),
            None => {
                // Variable not found in batch schema
                Err(QueryError::VariableNotFound(
                    "Projected variable not in child schema".to_string(),
                ))
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Same as child (projection doesn't change row count)
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    // Tests require a mock operator - will be added with integration tests
}
