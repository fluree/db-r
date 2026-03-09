//! Fast-path for `COUNT(DISTINCT ?o)` with an existence-only join on the same subject.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(DISTINCT ?o1) AS ?count)
//! WHERE { ?s <p_count> ?o1 . ?s <p_exists> ?o2 . }
//! ```
//!
//! When `?o2` is not otherwise needed, the second triple is an existence constraint
//! on `?s`. The generic pipeline would materialize bindings and hash decoded
//! values for distinctness.
//!
//! This operator instead:
//! - builds a subject set for `<p_exists>` by scanning PSOT (SId column only)
//! - scans POST for `<p_count>` and streams through sorted `(o_key, s_id)` rows
//! - increments the distinct counter once per object group that has any subject in the set
//! - never decodes subject/object values

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

pub struct PredicateExistsJoinCountDistinctObjectOperator {
    count_predicate: Ref,
    exists_predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateExistsJoinCountDistinctObjectOperator {
    pub fn new(
        count_predicate: Ref,
        exists_predicate: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            count_predicate,
            exists_predicate,
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
        Batch::new(schema, vec![col]).map_err(|e| {
            QueryError::execution(format!("fast exists-join count-distinct batch build: {e}"))
        })
    }
}

#[async_trait]
impl Operator for PredicateExistsJoinCountDistinctObjectOperator {
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
                    match count_distinct_object_with_exists_subject_post(
                        store,
                        ctx.binary_g_id,
                        &self.count_predicate,
                        &self.exists_predicate,
                    )? {
                        Some(count) => {
                            self.state = OperatorState::Open;
                            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                                self.build_output_batch(count as i64)?,
                            )));
                            return Ok(());
                        }
                        None => {
                            // Unsupported at runtime — fall through to planned pipeline.
                        }
                    }
                }
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "EXISTS-join COUNT(DISTINCT) fast-path unavailable and no fallback provided"
                    .to_string(),
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
                "EXISTS-join fast-path requires bound predicates".to_string(),
            ))
        }
    })
}

fn collect_subjects_for_predicate_psot(
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
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_id) {
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

/// COUNT DISTINCT objects for a bound predicate by scanning POST, restricted by a subject set.
///
/// Returns `None` when the fast-path cannot guarantee correctness (e.g., mixed o_type).
fn count_distinct_object_with_exists_subject_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    count_predicate: &Ref,
    exists_predicate: &Ref,
) -> Result<Option<u64>> {
    let count_sid = normalize_pred_sid(store, count_predicate)?;
    let exists_sid = normalize_pred_sid(store, exists_predicate)?;

    let Some(p_count) = store.sid_to_p_id(&count_sid) else {
        return Ok(Some(0));
    };
    let Some(p_exists) = store.sid_to_p_id(&exists_sid) else {
        return Ok(Some(0));
    };

    let subjects = collect_subjects_for_predicate_psot(store, g_id, p_exists)?;
    if subjects.is_empty() {
        return Ok(Some(0));
    }

    let branch = match store.branch_for_order(g_id, RunSortOrder::Post) {
        Some(b) => b,
        None => return Ok(Some(0)),
    };

    let cmp = cmp_v2_for_order(RunSortOrder::Post);
    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: 0,
        p_id: p_count,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id: p_count,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    // For now: only handle IRI_REF objects (benchmark predicates like dblp:bibtexType).
    let required_o_type = OType::IRI_REF.as_u16();

    let projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OKey);
            s.insert(ColumnId::SId);
            s
        },
    };

    let mut distinct: u64 = 0;
    let mut current_okey: Option<u64> = None;
    let mut group_has_match = false;

    for leaf_entry in &branch.leaves[leaf_range] {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_count) {
                continue;
            }
            if entry.o_type_const != Some(required_o_type) {
                return Ok(None);
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let okey = batch.o_key.get(row);
                if current_okey != Some(okey) {
                    if current_okey.is_some() && group_has_match {
                        distinct += 1;
                    }
                    current_okey = Some(okey);
                    group_has_match = false;
                }

                let s_id = batch.s_id.get(row);
                if subjects.contains(&s_id) {
                    group_has_match = true;
                }
            }
        }
    }

    if current_okey.is_some() && group_has_match {
        distinct += 1;
    }

    Ok(Some(distinct))
}
