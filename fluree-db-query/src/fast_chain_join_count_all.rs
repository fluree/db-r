//! Fast-path: `COUNT(*)` for a 3-hop join chain of bound predicates.
//!
//! Targets queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count) WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   ?c <p3> ?d .
//! }
//! ```
//!
//! The generic pipeline executes this via nested-loop joins and can end up
//! materializing very large intermediate results.
//!
//! This operator computes the join cardinality without materializing join rows:
//!
//! Let:
//! - `n3(c) = count_{p3}(c -> *)`
//! - For each `b`, `w(b) = sum_{edges b->c in p2} n3(c)`
//! - Final `COUNT(*) = sum_{edges a->b in p1} w(b)`
//!
//! For DBLP-like data, `p3` is much smaller than `p2` (`rdf:type`), so we also
//! restrict the `p2` scan to only those `c` that appear in `p3` by merge-filtering
//! in POST order (p,o,s).

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    projection_okey_only, projection_sid_okey, PrecomputedSingleBatchOperator,
    PsotSubjectCountIter,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashMap;

pub struct PredicateChainJoinCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateChainJoinCountAllOperator {
    pub fn new(p1: Ref, p2: Ref, p3: Ref, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            p1,
            p2,
            p3,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

enum FastChainJoinCount {
    Supported(u64),
    Unsupported,
}

fn collect_c_counts_from_p3_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p3_id: u32,
) -> Result<Vec<(u64, u64)>> {
    let mut iter = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    let mut out: Vec<(u64, u64)> = Vec::new();
    while let Some((c, n)) = iter.next_group()? {
        if n > 0 {
            out.push((c, n));
        }
    }
    Ok(out)
}

fn c_count_map(c_counts: &[(u64, u64)]) -> FxHashMap<u64, u64> {
    let mut m: FxHashMap<u64, u64> = FxHashMap::default();
    m.reserve(c_counts.len());
    for (c, n) in c_counts {
        m.insert(*c, *n);
    }
    m
}

/// Cursor over a predicate's PSOT rows that can advance to a requested subject id.
///
/// Specialized for the `?b <p2> ?c` use case:
/// - PSOT order groups by subject `b`
/// - object `c` is expected to be IRI_REF (so `o_key` is a subject id)
struct PsotSeekSumCursor<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaves: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
    // Projection cached (SId + OKey).
    projection: fluree_db_binary_index::ColumnProjection,
    // Require homogeneous IRI_REF objects for fast path.
    required_o_type: u16,
}

impl<'a> PsotSeekSumCursor<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Self {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Self {
            store,
            p_id,
            leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            projection: projection_sid_okey(),
            required_o_type: OType::IRI_REF.as_u16(),
        }
    }

    fn load_next_batch(&mut self, target_b: u64) -> Result<Option<()>> {
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaves.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaves[self.leaf_pos];
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
                // Skip leaflets that cannot contain the target subject by inspecting last key.
                let last = read_ordered_key_v2(RunSortOrder::Psot, &entry.last_key);
                let last_b = last.s_id.as_u64();
                if last_b < target_b {
                    continue;
                }
                // Require homogeneous IRI_REF objects.
                if entry.o_type_const != Some(self.required_o_type) {
                    return Err(QueryError::Internal(
                        "chain-join fast path requires o_type_const=IRI_REF for PSOT leaflets"
                            .into(),
                    ));
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

    /// Return `Some(sum)` if `target_b` exists, otherwise `None`.
    fn seek_sum_for_subject(
        &mut self,
        target_b: u64,
        c_to_n3: &FxHashMap<u64, u64>,
    ) -> Result<Option<u64>> {
        let mut found = false;
        let mut sum: u64 = 0;

        loop {
            if self.batch.is_none() && self.load_next_batch(target_b)?.is_none() {
                return Ok(found.then_some(sum));
            }
            let batch = self.batch.as_ref().unwrap();

            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            if !found {
                // Advance to first row with s_id >= target_b.
                while self.row < batch.row_count && batch.s_id.get(self.row) < target_b {
                    let cur = batch.s_id.get(self.row);
                    while self.row < batch.row_count && batch.s_id.get(self.row) == cur {
                        self.row += 1;
                    }
                }
                if self.row >= batch.row_count {
                    self.batch = None;
                    continue;
                }
                let b = batch.s_id.get(self.row);
                if b > target_b {
                    return Ok(None);
                }
                found = true;
            } else if batch.s_id.get(self.row) > target_b {
                return Ok(Some(sum));
            }

            // We are at `target_b` (or end-of-batch).
            while self.row < batch.row_count && batch.s_id.get(self.row) == target_b {
                let c = batch.o_key.get(self.row);
                if let Some(n3) = c_to_n3.get(&c) {
                    sum = sum
                        .checked_add(*n3)
                        .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
                }
                self.row += 1;
            }

            if self.row < batch.row_count {
                // Next subject > target_b inside the same batch.
                return Ok(Some(sum));
            }

            // Subject group may continue in the next leaflet/batch.
            self.batch = None;
        }
    }
}

fn count_chain_join_all(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1_id: u32,
    p2_id: u32,
    p3_id: u32,
) -> Result<FastChainJoinCount> {
    // Step 1: collect subjects `c` from p3 and their outdegree n3(c).
    let c_counts = collect_c_counts_from_p3_psot(store, g_id, p3_id)?;
    if c_counts.is_empty() {
        return Ok(FastChainJoinCount::Supported(0));
    }

    // Build lookup map for n3(c).
    let c_to_n3 = c_count_map(&c_counts);

    // Step 2+3 fused: stream p1 in POST grouped by object `b`, and for each `b` seek the
    // corresponding group in p2 PSOT to compute `w(b) = Σ_{b->c} n3(c)`.
    //
    // This avoids building `w_by_b` for subjects that never appear as p1 objects.
    let iri_ref = OType::IRI_REF.as_u16();
    let mut p2_cursor = PsotSeekSumCursor::new(store, g_id, p2_id);

    let leaves_p1 = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p1_id);
    let proj_p1 = projection_okey_only(); // POST: o_key = object (b)
    let mut total: u64 = 0;

    for leaf_entry in leaves_p1 {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p1_id) {
                continue;
            }
            if entry.o_type_const != Some(iri_ref) {
                return Ok(FastChainJoinCount::Unsupported);
            }
            let batch = handle
                .load_columns(leaflet_idx, &proj_p1, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            let mut row: usize = 0;
            while row < batch.row_count {
                let b = batch.o_key.get(row);
                let mut n1: u64 = 0;
                while row < batch.row_count && batch.o_key.get(row) == b {
                    n1 += 1;
                    row += 1;
                }

                let Some(w) = p2_cursor.seek_sum_for_subject(b, &c_to_n3)? else {
                    continue;
                };
                if w == 0 {
                    continue;
                }
                let add = n1
                    .checked_mul(w)
                    .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
                total = total
                    .checked_add(add)
                    .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
            }
        }
    }

    Ok(FastChainJoinCount::Supported(total))
}

#[async_trait]
impl Operator for PredicateChainJoinCountAllOperator {
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
            let p1_sid = normalize_pred_sid(store, &self.p1)?;
            let p2_sid = normalize_pred_sid(store, &self.p2)?;
            let p3_sid = normalize_pred_sid(store, &self.p3)?;
            let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
                let batch = build_count_batch(self.out_var, 0)?;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
                self.state = OperatorState::Open;
                return Ok(());
            };
            let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
                let batch = build_count_batch(self.out_var, 0)?;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
                self.state = OperatorState::Open;
                return Ok(());
            };
            let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
                let batch = build_count_batch(self.out_var, 0)?;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
                self.state = OperatorState::Open;
                return Ok(());
            };

            match count_chain_join_all(store, ctx.binary_g_id, p1_id, p2_id, p3_id)? {
                FastChainJoinCount::Supported(n) => {
                    let n_i64 = i64::try_from(n)
                        .map_err(|_| QueryError::execution("COUNT(*) exceeds i64 in chain join"))?;
                    let batch = build_count_batch(self.out_var, n_i64)?;
                    self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
                    self.state = OperatorState::Open;
                    return Ok(());
                }
                FastChainJoinCount::Unsupported => {
                    // Fall through to planned pipeline.
                }
            }
        }

        let Some(fallback) = self.fallback.as_mut() else {
            return Err(QueryError::Internal(
                "chain join COUNT(*) fast-path unavailable and no fallback provided".into(),
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
