//! Fast-path: count total triples across all patterns.
//!
//! Targets benchmark query:
//! `SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }`
//! (and equivalent `COUNT(*)` / `COUNT(?p)` / `COUNT(?o)` when all variables are bound).
//!
//! This is answered **metadata-only** by summing `row_count` across the branch
//! manifest leaves for any available permutation.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{build_count_batch, fast_path_store, PrecomputedSingleBatchOperator};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub struct CountTriplesOperator {
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl CountTriplesOperator {
    pub fn new(out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for CountTriplesOperator {
    fn schema(&self) -> &[VarId] {
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        if let Some(store) = fast_path_store(ctx) {
            let count = count_triples_from_branch_manifest(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count)
                .map_err(|_| QueryError::execution("COUNT exceeds i64 in triples fast-path"))?;
            let batch = build_count_batch(self.out_var, count_i64)?;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
            self.state = OperatorState::Open;
            return Ok(());
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "triples COUNT fast-path unavailable and no fallback provided".into(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let Some(fallback) = self.fallback.as_mut() else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        let b = fallback.next_batch(ctx).await?;
        if b.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(b)
    }

    fn close(&mut self) {
        if let Some(fb) = self.fallback.as_mut() {
            fb.close();
        }
        self.state = OperatorState::Closed;
    }
}

fn count_triples_from_branch_manifest(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    // Any permutation's leaf `row_count` sums to the total number of triples.
    // Prefer PSOT (commonly present and predicate-segmented).
    let order_preference = [
        RunSortOrder::Psot,
        RunSortOrder::Spot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];
    for order in order_preference {
        if let Some(branch) = store.branch_for_order(g_id, order) {
            return Ok(branch.leaves.iter().map(|l| l.row_count).sum());
        }
    }
    Ok(0)
}
