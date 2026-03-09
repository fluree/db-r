//! Fast-path: count distinct predicates across all triples.
//!
//! Targets benchmark query:
//! `SELECT (COUNT(DISTINCT ?p) AS ?count) WHERE { ?s ?p ?o }`
//!
//! Uses PSOT (predicate-primary) segmentation. Each leaflet is predicate-homogeneous
//! (`p_const`), and leaflets are encountered in increasing `p_id` order. We can
//! count distinct predicates by counting transitions of `p_const` across the
//! leaflet directory entries (metadata-only).

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

pub struct CountDistinctPredicatesOperator {
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl CountDistinctPredicatesOperator {
    pub fn new(out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for CountDistinctPredicatesOperator {
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
            let count = count_distinct_predicates_psot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(DISTINCT) exceeds i64 in distinct-predicate fast-path")
            })?;
            let batch = build_count_batch(self.out_var, count_i64)?;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
            self.state = OperatorState::Open;
            return Ok(());
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "distinct predicate COUNT fast-path unavailable and no fallback provided".into(),
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

fn count_distinct_predicates_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };

    let mut prev_p: Option<u32> = None;
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }

            // Leaflets in PSOT are predicate-homogeneous.
            let p_id = entry.p_const.unwrap_or_else(|| {
                // PSOT ordered key starts with p_id (big-endian).
                let bytes: [u8; 4] = entry.first_key[0..4].try_into().unwrap();
                u32::from_be_bytes(bytes)
            });

            if prev_p != Some(p_id) {
                total += 1;
                prev_p = Some(p_id);
            }
        }
    }

    Ok(total)
}
