//! Fast-path for `COUNT(*)` with a 2-hop join chain and a simple EXISTS.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   FILTER EXISTS { ?c <p3> ?d . }
//! }
//! ```
//!
//! This operator computes:
//!
//! \[
//!   \sum_{b} \bigl( count_{p1}(b) \times count_{p2,exists}(b) \bigr)
//! \]
//!
//! where:
//! - `count_{p1}(b)` is the number of `?a` such that `?a p1 b`
//! - `count_{p2,exists}(b)` is the number of `?c` such that `?b p2 c` and `c` satisfies the EXISTS.
//!
//! It avoids materializing join rows and never decodes to IRIs/strings.

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
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::FxHashSet;
use std::sync::Arc;

pub struct PredicateChainExistsJoinCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateChainExistsJoinCountAllOperator {
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

    fn schema_arc(&self) -> Arc<[VarId]> {
        Arc::from(vec![self.out_var].into_boxed_slice())
    }

    fn build_output_batch(&self, count: i64) -> Result<Batch> {
        let schema = self.schema_arc();
        let col = vec![Binding::lit(FlakeValue::Long(count), Sid::xsd_integer())];
        Batch::new(schema, vec![col])
            .map_err(|e| QueryError::execution(format!("fast chain+exists count batch build: {e}")))
    }
}

#[async_trait]
impl Operator for PredicateChainExistsJoinCountAllOperator {
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
                    if let Some(count) = count_chain_exists_join(
                        store,
                        ctx.binary_g_id,
                        &self.p1,
                        &self.p2,
                        &self.p3,
                    )? {
                        self.state = OperatorState::Open;
                        self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                            self.build_output_batch(count as i64)?,
                        )));
                        return Ok(());
                    }
                }
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "chain+exists COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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
                "chain+exists fast-path requires bound predicates".to_string(),
            ))
        }
    })
}

fn collect_subjects_for_predicate_psot_set(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<FxHashSet<u64>> {
    let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
        Some(b) => b,
        None => return Ok(FxHashSet::default()),
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

    let mut out: FxHashSet<u64> = FxHashSet::default();
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
                    out.insert(sid);
                    prev = Some(sid);
                }
            }
        }
    }

    Ok(out)
}

struct PostGroupCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
}

impl<'a> PostGroupCountIter<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Option<Self>> {
        let branch = match store.branch_for_order(g_id, RunSortOrder::Post) {
            Some(b) => b,
            None => {
                return Ok(Some(Self {
                    store,
                    p_id,
                    leaf_entries: &[],
                    leaf_pos: 0,
                    leaflet_idx: 0,
                    row: 0,
                    handle: None,
                    batch: None,
                }))
            }
        };

        let cmp = cmp_v2_for_order(RunSortOrder::Post);
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

        Ok(Some(Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: {
                let mut s = ColumnSet::EMPTY;
                s.insert(ColumnId::OKey);
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
                // Require IRI refs for join key `?b` (object of p1).
                if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                    return Ok(None);
                }
                let batch = handle
                    .load_columns(idx, &projection, RunSortOrder::Post)
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

            let key = batch.o_key.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count {
                let k = batch.o_key.get(self.row);
                if k != key {
                    break;
                }
                count += 1;
                self.row += 1;
            }
            return Ok(Some((key, count)));
        }
    }
}

struct PsotFilteredCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    exists_subjects: &'a FxHashSet<u64>,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
}

impl<'a> PsotFilteredCountIter<'a> {
    fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        exists_subjects: &'a FxHashSet<u64>,
    ) -> Result<Option<Self>> {
        let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
            Some(b) => b,
            None => {
                return Ok(Some(Self {
                    store,
                    p_id,
                    exists_subjects,
                    leaf_entries: &[],
                    leaf_pos: 0,
                    leaflet_idx: 0,
                    row: 0,
                    handle: None,
                    batch: None,
                }))
            }
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
        let leaves = &branch.leaves[leaf_range];

        Ok(Some(Self {
            store,
            p_id,
            exists_subjects,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: {
                let mut s = ColumnSet::EMPTY;
                s.insert(ColumnId::SId);
                s.insert(ColumnId::OKey);
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
                // Require IRI refs for join key `?c` (object of p2), so o_key is a subject id.
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

            let b_id = batch.s_id.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count {
                let sid = batch.s_id.get(self.row);
                if sid != b_id {
                    break;
                }
                let c_id = batch.o_key.get(self.row);
                if self.exists_subjects.contains(&c_id) {
                    count += 1;
                }
                self.row += 1;
            }

            if count > 0 {
                return Ok(Some((b_id, count)));
            }
            // else: skip this b_id entirely (no qualifying c values).
        }
    }
}

fn count_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // EXISTS subjects are `?c` in: ?c p3 ?d .
    let exists_subjects = collect_subjects_for_predicate_psot_set(store, g_id, p3_id)?;
    if exists_subjects.is_empty() {
        return Ok(Some(0));
    }

    let mut ab = match PostGroupCountIter::new(store, g_id, p1_id)? {
        Some(it) => it,
        None => return Ok(None),
    };
    let mut bc = match PsotFilteredCountIter::new(store, g_id, p2_id, &exists_subjects)? {
        Some(it) => it,
        None => return Ok(None),
    };

    let mut left = ab.next_group()?;
    let mut right = bc.next_group()?;
    let mut total: u64 = 0;

    while let (Some((b1, a_count)), Some((b2, c_count))) = (left, right) {
        if b1 < b2 {
            left = ab.next_group()?;
        } else if b1 > b2 {
            right = bc.next_group()?;
        } else {
            total = total.saturating_add(a_count.saturating_mul(c_count));
            left = ab.next_group()?;
            right = bc.next_group()?;
        }
    }

    Ok(Some(total))
}
