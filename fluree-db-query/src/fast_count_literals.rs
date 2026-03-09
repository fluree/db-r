//! Fast-path: count triples with literal objects.
//!
//! Targets benchmark query:
//! `SELECT (COUNT(?o) AS ?count) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`
//!
//! Since every solution binds `?o` in a triple pattern, `COUNT(?o)` is equivalent to `COUNT(*)`.
//! We can answer this by counting rows whose encoded `o_type` is not a node ref (IRI/blank node),
//! using leaflet directory `o_type_const` + `row_count` where possible, and decoding only the
//! `OType` column for mixed-type leaflets.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, projection_otype_only, PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;

pub struct CountLiteralObjectsOperator {
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl CountLiteralObjectsOperator {
    pub fn new(out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for CountLiteralObjectsOperator {
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
            let count = count_literal_rows_psot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count)
                .map_err(|_| QueryError::execution("COUNT exceeds i64 in literal fast-path"))?;
            let batch = build_count_batch(self.out_var, count_i64)?;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
            self.state = OperatorState::Open;
            return Ok(());
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "literal COUNT fast-path unavailable and no fallback provided".into(),
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

fn is_literal_otype(ot_u16: u16) -> bool {
    let ot = OType::from_u16(ot_u16);
    !ot.is_node_ref()
}

fn count_literal_rows_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };
    let projection = projection_otype_only();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if let Some(ot) = entry.o_type_const {
                if is_literal_otype(ot) {
                    total += entry.row_count as u64;
                }
                continue;
            }

            // Mixed types: decode OType column only.
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                if is_literal_otype(batch.o_type.get(row)) {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}
