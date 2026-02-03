//! Operator trait and base types for query execution
//!
//! Operators form a tree that produces batches of results through
//! the `open/next_batch/close` lifecycle pattern.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::Storage;

/// Query execution operator
///
/// Operators follow a lifecycle pattern for resource control:
/// 1. `open()` - Initialize state, allocate buffers
/// 2. `next_batch()` - Pull batches until exhausted (returns None)
/// 3. `close()` - Release resources
///
/// # Schema Contract
///
/// - `schema()` returns the output variables, fixed at construction
/// - All batches from `next_batch()` have columns in schema order
/// - Schema contains no duplicate VarIds
///
/// Call `open`, then loop on `next_batch` until `None`, then `close`.
#[async_trait]
pub trait Operator<S: Storage + 'static>: Send + Sync {
    /// Output schema - which variables this operator produces
    ///
    /// Fixed at construction time (does not change across batches).
    /// Batch columns are in this order.
    fn schema(&self) -> &[VarId];

    /// Initialize operator state
    ///
    /// Called once before `next_batch()`. Allocates buffers, opens
    /// child operators, etc.
    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()>;

    /// Pull next batch of results
    ///
    /// Returns `Ok(Some(batch))` with results, or `Ok(None)` when exhausted.
    /// Batch columns are ordered according to `schema()`.
    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>>;

    /// Release resources
    ///
    /// Called when operator is no longer needed. Closes child operators,
    /// releases buffers, etc.
    fn close(&mut self);

    /// Estimated cardinality (for planning/EXPLAIN)
    ///
    /// Returns estimated number of rows this operator will produce.
    /// Used by the planner for cost estimation and by EXPLAIN for display.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}

/// Boxed operator for dynamic dispatch
pub type BoxedOperator<S> = Box<dyn Operator<S> + Send + Sync>;

/// Operator state for lifecycle tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorState {
    /// Not yet opened
    Created,
    /// Opened and ready to produce batches
    Open,
    /// Exhausted (next_batch returned None)
    Exhausted,
    /// Closed
    Closed,
}

impl OperatorState {
    /// Check if operator can be opened
    pub fn can_open(&self) -> bool {
        matches!(self, OperatorState::Created)
    }

    /// Check if operator can produce batches
    pub fn can_next(&self) -> bool {
        matches!(self, OperatorState::Open)
    }

    /// Check if operator is closed
    pub fn is_closed(&self) -> bool {
        matches!(self, OperatorState::Closed)
    }
}
