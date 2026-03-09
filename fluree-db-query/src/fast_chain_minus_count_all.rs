//! Fast-path: `COUNT(*)` for a 2-hop join chain with a `MINUS` constraint on the tail.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   MINUS { ?c <p3> ?d . }
//! }
//! ```
//!
//! Supported semantics:
//! - Shared var between outer and MINUS is `?c`.
//! - MINUS removes an outer solution iff `?c` has at least one `<p3>` row.
//! Therefore:
//! - Let `S3 = { c | c p3 ?d }`
//! - Answer = Σ_b count_{p1}(b) * count_{p2, c∉S3}(b)
//!
//! Implementation avoids materializing join rows and avoids decoding values.
//!
//! Correctness constraints (planner must enforce):
//! - Predicates are bound.
//! - `?b` and `?c` are IRIs (so they are stored as `o_key` with `o_type_const = IRI_REF`).

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store,
    leaf_entries_for_predicate, normalize_pred_sid, projection_sid_okey, PostObjectGroupCountIter,
    PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch};
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashSet;

pub struct PredicateChainMinusCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3_minus: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateChainMinusCountAllOperator {
    pub fn new(
        p1: Ref,
        p2: Ref,
        p3_minus: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            p1,
            p2,
            p3_minus,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateChainMinusCountAllOperator {
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
            if let Some(count) =
                count_chain_minus(store, ctx.binary_g_id, &self.p1, &self.p2, &self.p3_minus)?
            {
                self.state = OperatorState::Open;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                    build_count_batch(self.out_var, i64::try_from(count).unwrap_or(i64::MAX))?,
                )));
                return Ok(());
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "chain+minus COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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

        let Some(fallback) = &mut self.fallback else {
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
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
    }
}

/// Yield `(b, count)` groups from PSOT(p2) counting only edges where `c` is NOT in `excluded_c`.
struct PsotObjectNotInSetCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    excluded_c: &'a FxHashSet<u64>,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
}

impl<'a> PsotObjectNotInSetCountIter<'a> {
    fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        excluded_c: &'a FxHashSet<u64>,
    ) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            excluded_c,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = projection_sid_okey();
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
                if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                    return Ok(None);
                }
                let batch = handle
                    .load_columns(idx, &projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            let b = batch.s_id.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count && batch.s_id.get(self.row) == b {
                let c = batch.o_key.get(self.row);
                if !self.excluded_c.contains(&c) {
                    count += 1;
                }
                self.row += 1;
            }
            if count > 0 {
                return Ok(Some((b, count)));
            }
            // If count == 0, skip this b group and keep scanning.
        }
    }
}

fn count_chain_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_minus: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3_minus)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        // If the minus predicate doesn't exist, it removes nothing (plain chain join COUNT(*)).
        // We don't currently have a dedicated fast path for the 2-hop chain, so fall back.
        return Ok(None);
    };

    let excluded_c = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    // Even if excluded is empty, the algorithm below still works (counts all p2 edges).

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?
        .ok_or_else(|| QueryError::Internal("chain+minus requires IRI_REF objects in p1".into()))?;
    let mut it2 = PsotObjectNotInSetCountIter::new(store, g_id, p2_id, &excluded_c)?
        .ok_or_else(|| QueryError::Internal("chain+minus requires IRI_REF objects in p2".into()))?;

    let mut a = it1.next_group()?;
    let mut b = it2.next_group()?;

    let mut total: u128 = 0;
    while let (Some((b1, c1)), Some((b2, c2))) = (a, b) {
        if b1 < b2 {
            a = it1.next_group()?;
        } else if b1 > b2 {
            b = it2.next_group()?;
        } else {
            total = total.saturating_add((c1 as u128).saturating_mul(c2 as u128));
            a = it1.next_group()?;
            b = it2.next_group()?;
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}
