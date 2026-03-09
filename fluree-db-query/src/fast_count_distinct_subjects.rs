//! Fast-path: count distinct subjects across all triples.
//!
//! Targets benchmark query:
//! `SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ?p ?o }`
//!
//! Uses SPOT (subject-primary) index order so subjects are grouped contiguously.
//! Computes the count **metadata-only** using each leaflet's `lead_group_count`
//! (distinct `s_id` groups within the leaflet), then corrects over-counting at
//! leaflet boundaries when the same subject spans two adjacent leaflets.

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

pub struct CountDistinctSubjectsOperator {
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl CountDistinctSubjectsOperator {
    pub fn new(out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for CountDistinctSubjectsOperator {
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
            let count = count_distinct_subjects_spot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(DISTINCT) exceeds i64 in distinct-subject fast-path")
            })?;
            let batch = build_count_batch(self.out_var, count_i64)?;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
            self.state = OperatorState::Open;
            return Ok(());
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "distinct subject COUNT fast-path unavailable and no fallback provided".into(),
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

fn count_distinct_subjects_spot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Spot) else {
        return Ok(0);
    };

    // SPOT key layout: s_id(8) + p_id(4) + o_type(2) + o_key(8) + o_i(4) = 26 bytes.
    // The "lead" key for distinct subjects is s_id, which is bytes [0..8].
    const LEAD_START: usize = 0;
    const LEAD_LEN: usize = 8;

    let mut prev_lead_last: Option<[u8; LEAD_LEN]> = None;
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.lead_group_count == 0 {
                continue;
            }

            let mut lead_first = [0u8; LEAD_LEN];
            lead_first.copy_from_slice(&entry.first_key[LEAD_START..LEAD_START + LEAD_LEN]);
            let mut lead_last = [0u8; LEAD_LEN];
            lead_last.copy_from_slice(&entry.last_key[LEAD_START..LEAD_START + LEAD_LEN]);

            total += u64::from(entry.lead_group_count);
            if prev_lead_last == Some(lead_first) {
                total = total.saturating_sub(1);
            }
            prev_lead_last = Some(lead_last);
        }
    }

    Ok(total)
}
