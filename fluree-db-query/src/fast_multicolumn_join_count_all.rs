//! Fast-path: `COUNT(*)` for a 2-pattern multicolumn join on `(?s, ?o)`.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE { ?s <p1> ?o . ?s <p2> ?o }
//! ```
//!
//! This is a natural join on both variables (not a cartesian star join).
//! We can compute it as the size of the intersection of the two predicate relations
//! on the composite key `(s_id, o_type, o_key)` using a streaming merge join in PSOT order.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    projection_sid_otype_okey, PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use std::cmp::Ordering;

pub struct PredicateMultiColumnJoinCountAllOperator {
    p1: Ref,
    p2: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateMultiColumnJoinCountAllOperator {
    pub fn new(p1: Ref, p2: Ref, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            p1,
            p2,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SoKey {
    s: u64,
    o_type: u16,
    o_key: u64,
}

impl Ord for SoKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.s
            .cmp(&other.s)
            .then_with(|| self.o_type.cmp(&other.o_type))
            .then_with(|| self.o_key.cmp(&other.o_key))
    }
}

impl PartialOrd for SoKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct PsotSoIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
    projection: fluree_db_binary_index::ColumnProjection,
}

impl<'a> PsotSoIter<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Self {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            projection: projection_sid_otype_okey(),
        }
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let batch = handle
                    .load_columns(idx, &self.projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    fn next_row(&mut self) -> Result<Option<SoKey>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            let key = SoKey {
                s: batch.s_id.get(self.row),
                o_type: batch.o_type.get(self.row),
                o_key: batch.o_key.get(self.row),
            };
            self.row += 1;
            return Ok(Some(key));
        }
    }
}

fn count_multicolumn_join_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
) -> Result<u64> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(0);
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(0);
    };

    let mut it1 = PsotSoIter::new(store, g_id, p1_id);
    let mut it2 = PsotSoIter::new(store, g_id, p2_id);

    let mut a = it1.next_row()?;
    let mut b = it2.next_row()?;
    let mut count: u64 = 0;

    while let (Some(ka), Some(kb)) = (a, b) {
        match ka.cmp(&kb) {
            Ordering::Less => a = it1.next_row()?,
            Ordering::Greater => b = it2.next_row()?,
            Ordering::Equal => {
                count = count.saturating_add(1);
                a = it1.next_row()?;
                b = it2.next_row()?;
            }
        }
    }

    Ok(count)
}

#[async_trait]
impl Operator for PredicateMultiColumnJoinCountAllOperator {
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
            let n = count_multicolumn_join_psot(store, ctx.binary_g_id, &self.p1, &self.p2)?;
            let n_i64 = i64::try_from(n).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in multicolumn join fast-path")
            })?;
            let batch = build_count_batch(self.out_var, n_i64)?;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
            self.state = OperatorState::Open;
            return Ok(());
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "multicolumn join COUNT(*) fast-path unavailable and no fallback provided".into(),
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
