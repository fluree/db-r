//! Fast-path for `COUNT(*)` with a simple correlated `FILTER EXISTS`.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE { ?s <p_outer> ?o1 . FILTER EXISTS { ?s <p_exists> ?o2 } }
//! ```
//!
//! The generic pipeline would:
//! - scan the outer predicate and materialize `Sid` / literal values
//! - evaluate EXISTS per row (or even with a semijoin cache, still needs subject decoding)
//!
//! This operator instead:
//! - scans PSOT for `<p_exists>` once to build a set of matching subject IDs (`s_id`)
//! - scans PSOT for `<p_outer>` and counts rows whose `s_id` is in that set
//! - never decodes subject/object values
//!
//! This preserves SPARQL multiplicity semantics: COUNT(*) counts one row per outer match.

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

/// Fused operator that outputs a single-row batch with the COUNT(*) result.
pub struct PredicateExistsJoinCountAllOperator {
    outer_predicate: Ref,
    exists_predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateExistsJoinCountAllOperator {
    pub fn new(
        outer_predicate: Ref,
        exists_predicate: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            outer_predicate,
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
        Batch::new(schema, vec![col])
            .map_err(|e| QueryError::execution(format!("fast exists-join count batch build: {e}")))
    }
}

#[async_trait]
impl Operator for PredicateExistsJoinCountAllOperator {
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
                    let count = count_exists_join_rows_psot(
                        store,
                        ctx.binary_g_id,
                        &self.outer_predicate,
                        &self.exists_predicate,
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
                "EXISTS-join COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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

    // PSOT is sorted by s_id (within a fixed predicate), so we can stream
    // unique subject IDs into a sorted Vec without hashing.
    let mut out: Vec<u64> = Vec::new();
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
                    out.push(sid);
                    prev = Some(sid);
                }
            }
        }
    }

    Ok(out)
}

fn count_rows_for_predicate_with_subject_filter_psot_merge(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    subjects_sorted: &[u64],
) -> Result<u64> {
    if subjects_sorted.is_empty() {
        return Ok(0);
    }

    let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
        Some(b) => b,
        None => return Ok(0),
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

    let mut count: u64 = 0;
    let mut subj_idx: usize = 0;
    let mut cur_s: Option<u64> = None;
    let mut cur_count: u64 = 0;

    let flush_group = |s_id: u64, n: u64, subj_idx: &mut usize, count: &mut u64| {
        while *subj_idx < subjects_sorted.len() && subjects_sorted[*subj_idx] < s_id {
            *subj_idx += 1;
        }
        if *subj_idx < subjects_sorted.len() && subjects_sorted[*subj_idx] == s_id {
            *count += n;
        }
    };
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
                match cur_s {
                    None => {
                        cur_s = Some(sid);
                        cur_count = 1;
                    }
                    Some(prev) if prev == sid => {
                        cur_count += 1;
                    }
                    Some(prev) => {
                        flush_group(prev, cur_count, &mut subj_idx, &mut count);
                        cur_s = Some(sid);
                        cur_count = 1;
                    }
                }
            }
        }
    }

    if let Some(last) = cur_s {
        flush_group(last, cur_count, &mut subj_idx, &mut count);
    }
    Ok(count)
}

/// COUNT(*) for `?s p_outer ?o1` restricted to subjects that satisfy `?s p_exists ?o2`.
fn count_exists_join_rows_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicate: &Ref,
    exists_predicate: &Ref,
) -> Result<u64> {
    let outer_sid = normalize_pred_sid(store, outer_predicate)?;
    let exists_sid = normalize_pred_sid(store, exists_predicate)?;

    let Some(p_outer) = store.sid_to_p_id(&outer_sid) else {
        return Ok(0);
    };
    let Some(p_exists) = store.sid_to_p_id(&exists_sid) else {
        return Ok(0);
    };

    let subjects_sorted = collect_subjects_for_predicate_psot_sorted(store, g_id, p_exists)?;
    count_rows_for_predicate_with_subject_filter_psot_merge(store, g_id, p_outer, &subjects_sorted)
}
