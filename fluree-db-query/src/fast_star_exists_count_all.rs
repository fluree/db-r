//! Fast-path for `COUNT(*)` with a 2-predicate star join and an EXISTS constraint.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p1> ?o1 .
//!   ?s <p2> ?o2 .
//!   FILTER EXISTS { ?s <p3> ?o3 . }
//! }
//! ```
//!
//! Semantics:
//! - For each subject `s`, there are `count_p1(s) * count_p2(s)` join rows.
//! - The EXISTS constraint keeps only subjects that have at least one `<p3>` row.
//!
//! This operator computes:
//!
//! \[
//!   \sum_{s \in S3} count_{p1}(s) \times count_{p2}(s)
//! \]
//!
//! where `S3 = { s | s p3 ?o3 }`.
//!
//! Implementation:
//! - Build `S3` as a sorted `Vec<s_id>` by scanning PSOT(p3) (SId column only).
//! - Stream grouped subject counts for p1 and p2 from PSOT (SId column only).
//! - Merge-join the two count streams on `s_id` to compute products.
//! - Merge-filter by `S3` (also sorted) and sum products.
//!
//! Avoids per-row EXISTS evaluation, join-row materialization, and value decoding.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::{BinaryIndexStore, ColumnProjection, ColumnSet};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use std::sync::Arc;

pub struct PredicateStarExistsJoinCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3_exists: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateStarExistsJoinCountAllOperator {
    pub fn new(
        p1: Ref,
        p2: Ref,
        p3_exists: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            p1,
            p2,
            p3_exists,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }

    fn schema_arc(&self) -> Arc<[VarId]> {
        Arc::from(vec![self.out_var].into_boxed_slice())
    }

    fn build_output_batch(&self, count: i64) -> Result<Batch> {
        let schema = self.schema_arc();
        let col = vec![Binding::lit(FlakeValue::Long(count), Sid::xsd_integer())];
        Batch::new(schema, vec![col])
            .map_err(|e| QueryError::execution(format!("fast star-exists count batch build: {e}")))
    }
}

#[async_trait]
impl Operator for PredicateStarExistsJoinCountAllOperator {
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

        let allow_fast = !ctx.history_mode
            && ctx.from_t.is_none()
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
            && ctx.overlay.map(|o| o.epoch()).unwrap_or(0) == 0;

        if allow_fast {
            if let Some(store) = ctx.binary_store.as_ref() {
                if ctx.to_t == store.max_t() {
                    let count = count_star_exists_join(
                        store,
                        ctx.binary_g_id,
                        &self.p1,
                        &self.p2,
                        &self.p3_exists,
                    )?;
                    self.state = OperatorState::Open;
                    self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                        self.build_output_batch(count as i64)?,
                    )));
                    return Ok(());
                }
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "star+exists COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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

/// Tiny helper operator: yields exactly one precomputed batch.
struct PrecomputedSingleBatchOperator {
    batch: Option<Batch>,
    state: OperatorState,
}

impl PrecomputedSingleBatchOperator {
    fn new(batch: Batch) -> Self {
        Self {
            batch: Some(batch),
            state: OperatorState::Open,
        }
    }
}

#[async_trait]
impl Operator for PrecomputedSingleBatchOperator {
    fn schema(&self) -> &[VarId] {
        self.batch.as_ref().map(|b| b.schema()).unwrap_or(&[])
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        let out = self.batch.take();
        if out.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.batch = None;
        self.state = OperatorState::Closed;
    }
}

fn normalize_pred_sid(store: &BinaryIndexStore, pred: &Ref) -> Result<Sid> {
    Ok(match pred {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "star+exists fast-path requires bound predicates".to_string(),
            ))
        }
    })
}

fn collect_subjects_for_predicate_psot_sorted(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Vec<u64>> {
    let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
        Some(b) => b,
        None => return Ok(Vec::new()),
    };

    let cmp = cmp_v2_for_order(RunSortOrder::Psot);
    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s
        },
    };

    let mut out: Vec<u64> = Vec::new();
    let mut prev: Option<u64> = None;
    for leaf_entry in &branch.leaves[leaf_range] {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if prev != Some(sid) {
                    out.push(sid);
                    prev = Some(sid);
                }
            }
        }
    }
    Ok(out)
}

struct PsotSubjectCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
}

impl<'a> PsotSubjectCountIter<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Self> {
        let branch = store
            .branch_for_order(g_id, RunSortOrder::Psot)
            .ok_or_else(|| QueryError::Internal("missing PSOT branch".to_string()))?;

        let cmp = cmp_v2_for_order(RunSortOrder::Psot);
        let min_key = RunRecordV2 {
            s_id: SubjectId(0),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId(u64::MAX),
            o_key: u64::MAX,
            p_id,
            t: u32::MAX,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };
        let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
        let leaves = &branch.leaves[leaf_range];

        Ok(Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        })
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: {
                let mut s = ColumnSet::EMPTY;
                s.insert(ColumnId::SId);
                s
            },
        };

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
            let s_id = batch.s_id.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count && batch.s_id.get(self.row) == s_id {
                count += 1;
                self.row += 1;
            }
            return Ok(Some((s_id, count)));
        }
    }
}

fn count_star_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_exists: &Ref,
) -> Result<u64> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3_exists)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(0);
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(0);
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(0);
    };

    let s3 = collect_subjects_for_predicate_psot_sorted(store, g_id, p3_id)?;
    if s3.is_empty() {
        return Ok(0);
    }

    let mut it1 = PsotSubjectCountIter::new(store, g_id, p1_id)?;
    let mut it2 = PsotSubjectCountIter::new(store, g_id, p2_id)?;
    let mut a = it1.next_group()?;
    let mut b = it2.next_group()?;

    // First merge: p1 and p2 counts → stream of (s, prod).
    let mut prod_stream: Vec<(u64, u64)> = Vec::new();
    while let (Some((s1, c1)), Some((s2, c2))) = (a, b) {
        if s1 < s2 {
            a = it1.next_group()?;
        } else if s1 > s2 {
            b = it2.next_group()?;
        } else {
            prod_stream.push((s1, c1.saturating_mul(c2)));
            a = it1.next_group()?;
            b = it2.next_group()?;
        }
    }
    if prod_stream.is_empty() {
        return Ok(0);
    }

    // Second merge: filter by s3 (both sorted).
    let mut total: u64 = 0;
    let mut i: usize = 0;
    let mut j: usize = 0;
    while i < prod_stream.len() && j < s3.len() {
        let (s, prod) = prod_stream[i];
        let s_ok = s3[j];
        if s < s_ok {
            i += 1;
        } else if s > s_ok {
            j += 1;
        } else {
            total = total.saturating_add(prod);
            i += 1;
            j += 1;
        }
    }
    Ok(total)
}
