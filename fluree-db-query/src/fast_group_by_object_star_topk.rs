//! Fast-path: GROUP BY ?o with top-k ORDER BY DESC(count) for a same-subject star join.
//!
//! Targets queries like:
//! ```sparql
//! SELECT ?o1 (COUNT(?s) AS ?count)
//! WHERE { ?s <p_group> ?o1 . ?s <p_filter> ?x . }
//! GROUP BY ?o1 ORDER BY DESC(?count) LIMIT k
//! ```
//!
//! And optionally additional subject aggregates:
//! `MIN(?s)`, `MAX(?s)`, `SAMPLE(?s)`.
//!
//! This avoids materializing the join result and avoids generic hash-join + group-by.
//! We compute:
//! - Let `S = { s | s <p_filter_i> ?x_i for all i }` (existence semijoin).
//! - For each group key `o`: count rows where `s <p_group> o` and `s ∈ S`.
//! - For subject aggregates, we use the encoded `s_id` ordering (matches late-materialized `Binding::EncodedSid`).
//!
//! Implementation:
//! - Build `S` by scanning PSOT for each filter predicate and intersecting subject sets.
//! - Scan PSOT(p_group) once and accumulate per-object aggregations for subjects in `S`.
//! - Select top-k by count and emit one batch.

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnBatch, ColumnProjection,
    ColumnSet,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ObjKey {
    o_type: u16,
    o_key: u64,
}

#[derive(Clone, Debug)]
struct AggState {
    count: u64,
    min_s: Option<u64>,
    max_s: Option<u64>,
    sample_s: Option<u64>,
}

impl AggState {
    fn new() -> Self {
        Self {
            count: 0,
            min_s: None,
            max_s: None,
            sample_s: None,
        }
    }

    fn observe(&mut self, s_id: u64, want_min: bool, want_max: bool, want_sample: bool) {
        self.count = self.count.saturating_add(1);
        if want_sample && self.sample_s.is_none() {
            self.sample_s = Some(s_id);
        }
        if want_min {
            self.min_s = Some(self.min_s.map_or(s_id, |m| m.min(s_id)));
        }
        if want_max {
            self.max_s = Some(self.max_s.map_or(s_id, |m| m.max(s_id)));
        }
    }
}

pub struct GroupByObjectStarTopKOperator {
    group_pred: Ref,
    filter_preds: Vec<Ref>,
    group_var: VarId,
    count_var: VarId,
    min_var: Option<VarId>,
    max_var: Option<VarId>,
    sample_var: Option<VarId>,
    limit: usize,
    schema: Arc<[VarId]>,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    emitted: bool,
    result: Option<Batch>,
}

impl GroupByObjectStarTopKOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_pred: Ref,
        filter_preds: Vec<Ref>,
        group_var: VarId,
        count_var: VarId,
        min_var: Option<VarId>,
        max_var: Option<VarId>,
        sample_var: Option<VarId>,
        limit: usize,
        schema: Arc<[VarId]>,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            group_pred,
            filter_preds,
            group_var,
            count_var,
            min_var,
            max_var,
            sample_var,
            limit: limit.max(1),
            schema,
            state: OperatorState::Created,
            fallback,
            emitted: false,
            result: None,
        }
    }
}

#[async_trait]
impl Operator for GroupByObjectStarTopKOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        let allow_fast = !ctx.history_mode && ctx.from_t.is_none() && !ctx.has_policy();
        if allow_fast {
            if let Some(store) = ctx.binary_store.as_ref() {
                let batch = compute_topk(
                    store,
                    ctx,
                    ctx.binary_g_id,
                    &self.group_pred,
                    &self.filter_preds,
                    Arc::clone(&self.schema),
                    self.group_var,
                    self.count_var,
                    self.min_var,
                    self.max_var,
                    self.sample_var,
                    self.limit,
                )?;
                self.result = Some(batch);
                self.emitted = false;
                self.fallback = None;
                self.state = OperatorState::Open;
                return Ok(());
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "group-by-object star topk fast-path unavailable and no fallback provided".into(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if let Some(fb) = &mut self.fallback {
            return fb.next_batch(ctx).await;
        }
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        self.emitted = true;
        Ok(self.result.take())
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
        self.emitted = false;
        self.result = None;
    }
}

fn ref_to_sid(store: &BinaryIndexStore, r: &Ref) -> Result<Sid> {
    Ok(match r {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "group-by-object star fast path requires bound predicates".into(),
            ))
        }
    })
}

fn build_psot_cursor_for_predicate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(None);
    };
    let branch = Arc::new(branch.clone());

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
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Psot,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );
    cursor.set_to_t(ctx.to_t);

    // Overlay merge — pre-filter by predicate.
    if ctx.overlay.is_some() {
        use std::collections::HashMap;
        let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
            Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
        });
        let mut ephemeral_preds: HashMap<String, u32> = HashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();

        ctx.overlay().for_each_overlay_flake(
            g_id,
            fluree_db_core::IndexType::Psot,
            None,
            None,
            true,
            ctx.to_t,
            &mut |flake| {
                if flake.p != pred_sid {
                    return;
                }
                match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    Some(&dn),
                    &mut ephemeral_preds,
                    &mut next_ep,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        tracing::warn!(error = %e, "group-by-object star: failed to translate overlay flake");
                    }
                }
            },
        );

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, RunSortOrder::Psot);
            cursor.set_overlay_ops(ops);
        }
        cursor.set_epoch(ctx.overlay().epoch());
    }

    Ok(Some(cursor))
}

fn collect_subject_set_for_predicate(
    store: &Arc<BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: GraphId,
    pred: &Ref,
    restrict_to: Option<&FxHashSet<u64>>,
) -> Result<FxHashSet<u64>> {
    let sid = ref_to_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return Ok(FxHashSet::default());
    };
    let mut out = ColumnSet::EMPTY;
    out.insert(ColumnId::SId);
    let projection = ColumnProjection {
        output: out,
        internal: ColumnSet::EMPTY,
    };
    let mut cursor = build_psot_cursor_for_predicate(ctx, store, g_id, sid, p_id, projection)?
        .ok_or_else(|| QueryError::Internal("group-by-object star: missing PSOT branch".into()))?;

    let mut set: FxHashSet<u64> = FxHashSet::default();
    let mut last_s: Option<u64> = None;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for i in 0..batch.row_count {
            let s = batch.s_id.get(i);
            if last_s == Some(s) {
                continue;
            }
            last_s = Some(s);
            if let Some(r) = restrict_to {
                if !r.contains(&s) {
                    continue;
                }
            }
            set.insert(s);
        }
    }
    Ok(set)
}

#[allow(clippy::too_many_arguments)]
fn compute_topk(
    store: &Arc<BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: GraphId,
    group_pred: &Ref,
    filter_preds: &[Ref],
    schema: Arc<[VarId]>,
    group_var: VarId,
    count_var: VarId,
    min_var: Option<VarId>,
    max_var: Option<VarId>,
    sample_var: Option<VarId>,
    limit: usize,
) -> Result<Batch> {
    // Scan group predicate PSOT for (s_id, o_type, o_key).
    let sid = ref_to_sid(store, group_pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return Ok(Batch::empty(schema)?);
    };
    let mut out = ColumnSet::EMPTY;
    out.insert(ColumnId::SId);
    out.insert(ColumnId::OType);
    out.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: out,
        internal: ColumnSet::EMPTY,
    };
    let mut cursor = build_psot_cursor_for_predicate(ctx, store, g_id, sid, p_id, projection)?
        .ok_or_else(|| QueryError::Internal("group-by-object star: missing PSOT branch".into()))?;

    let want_min = min_var.is_some();
    let want_max = max_var.is_some();
    let want_sample = sample_var.is_some();

    let mut aggs: FxHashMap<ObjKey, AggState> = FxHashMap::default();

    // Preferred path (fast + low-memory): merge-join on subject IDs when there is exactly
    // one filter predicate. Both scans are PSOT-sorted by subject.
    if filter_preds.len() == 1 {
        let fp = &filter_preds[0];
        let fp_sid = ref_to_sid(store, fp)?;
        let Some(fp_id) = store.sid_to_p_id(&fp_sid) else {
            return Ok(Batch::empty(schema)?);
        };
        let mut fp_out = ColumnSet::EMPTY;
        fp_out.insert(ColumnId::SId);
        let fp_proj = ColumnProjection {
            output: fp_out,
            internal: ColumnSet::EMPTY,
        };
        let mut fcur = build_psot_cursor_for_predicate(ctx, store, g_id, fp_sid, fp_id, fp_proj)?
            .ok_or_else(|| {
            QueryError::Internal("group-by-object star: missing PSOT branch".into())
        })?;

        let mut g_batch: Option<ColumnBatch> = None;
        let mut g_i: usize = 0;
        let mut f_batch: Option<ColumnBatch> = None;
        let mut f_i: usize = 0;
        let mut f_last: Option<u64> = None;

        let next_filter_subject = |fcur: &mut BinaryCursor,
                                   f_batch: &mut Option<ColumnBatch>,
                                   f_i: &mut usize,
                                   f_last: &mut Option<u64>|
         -> Result<Option<u64>> {
            loop {
                if f_batch.is_none() || *f_i >= f_batch.as_ref().unwrap().row_count {
                    *f_batch = fcur
                        .next_batch()
                        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                    *f_i = 0;
                    if f_batch.is_none() {
                        return Ok(None);
                    }
                }
                let b = f_batch.as_ref().unwrap();
                let s = b.s_id.get(*f_i);
                *f_i += 1;
                if *f_last == Some(s) {
                    continue;
                }
                *f_last = Some(s);
                return Ok(Some(s));
            }
        };

        let peek_group_subject = |cursor: &mut BinaryCursor,
                                  g_batch: &mut Option<ColumnBatch>,
                                  g_i: &mut usize|
         -> Result<Option<u64>> {
            if g_batch.is_none() || *g_i >= g_batch.as_ref().unwrap().row_count {
                *g_batch = cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                *g_i = 0;
                if g_batch.is_none() {
                    return Ok(None);
                }
            }
            let b = g_batch.as_ref().unwrap();
            Ok(Some(b.s_id.get(*g_i)))
        };

        let mut fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
        while let (Some(gs), Some(cur_fs)) =
            (peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?, fs)
        {
            match gs.cmp(&cur_fs) {
                Ordering::Less => {
                    // Skip group rows until gs >= cur_fs (skipping whole subject runs).
                    let skip_s = gs;
                    loop {
                        let Some(cur_gs) = peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?
                        else {
                            break;
                        };
                        if cur_gs != skip_s {
                            break;
                        }
                        g_i += 1;
                    }
                }
                Ordering::Greater => {
                    fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
                }
                Ordering::Equal => {
                    let s = gs;
                    // Process all group rows for this subject.
                    loop {
                        let Some(cur_gs) = peek_group_subject(&mut cursor, &mut g_batch, &mut g_i)?
                        else {
                            break;
                        };
                        if cur_gs != s {
                            break;
                        }
                        let b = g_batch.as_ref().unwrap();
                        let k = ObjKey {
                            o_type: b.o_type.get(g_i),
                            o_key: b.o_key.get(g_i),
                        };
                        aggs.entry(k).or_insert_with(AggState::new).observe(
                            s,
                            want_min,
                            want_max,
                            want_sample,
                        );
                        g_i += 1;
                    }
                    fs = next_filter_subject(&mut fcur, &mut f_batch, &mut f_i, &mut f_last)?;
                }
            }
        }
    } else {
        // General path: build subject set S by intersecting filter predicates.
        let mut s_set: Option<FxHashSet<u64>> = None;
        for p in filter_preds.iter() {
            let next = collect_subject_set_for_predicate(
                store,
                ctx,
                g_id,
                p,
                s_set.as_ref().map(|s| s as &FxHashSet<u64>),
            )?;
            s_set = Some(next);
            if s_set.as_ref().is_some_and(|s| s.is_empty()) {
                break;
            }
        }
        let s_set = s_set.unwrap_or_default();
        if s_set.is_empty() {
            return Batch::empty(schema)
                .map_err(|e| QueryError::execution(format!("empty batch: {e}")));
        }

        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
        {
            for i in 0..batch.row_count {
                let s = batch.s_id.get(i);
                if !s_set.contains(&s) {
                    continue;
                }
                let k = ObjKey {
                    o_type: batch.o_type.get(i),
                    o_key: batch.o_key.get(i),
                };
                aggs.entry(k).or_insert_with(AggState::new).observe(
                    s,
                    want_min,
                    want_max,
                    want_sample,
                );
            }
        }
    }

    if aggs.is_empty() {
        return Ok(Batch::empty(schema)?);
    }

    // Select top-k by count desc.
    let mut rows: Vec<(ObjKey, AggState)> = aggs.into_iter().collect();
    rows.sort_unstable_by(|a, b| {
        b.1.count.cmp(&a.1.count).then_with(|| {
            a.0.o_type
                .cmp(&b.0.o_type)
                .then_with(|| a.0.o_key.cmp(&b.0.o_key))
        })
    });
    if rows.len() > limit {
        rows.truncate(limit);
    }

    // Build output columns.
    let view = BinaryGraphView::new(Arc::clone(store), g_id);
    let dt_count = WellKnownDatatypes::new().xsd_long;

    let mut col_o1: Vec<Binding> = Vec::with_capacity(rows.len());
    let mut col_count: Vec<Binding> = Vec::with_capacity(rows.len());
    let mut col_min: Vec<Binding> = Vec::new();
    let mut col_max: Vec<Binding> = Vec::new();
    let mut col_sample: Vec<Binding> = Vec::new();
    if want_min {
        col_min = Vec::with_capacity(rows.len());
    }
    if want_max {
        col_max = Vec::with_capacity(rows.len());
    }
    if want_sample {
        col_sample = Vec::with_capacity(rows.len());
    }

    for (k, st) in rows {
        if k.o_type == OType::IRI_REF.as_u16() {
            col_o1.push(Binding::EncodedSid { s_id: k.o_key });
        } else {
            let val = view
                .decode_value(k.o_type, k.o_key, p_id)
                .map_err(|e| QueryError::Internal(format!("decode_value: {e}")))?;
            let dt = store
                .resolve_datatype_sid(k.o_type)
                .unwrap_or_else(|| Sid::new(0, ""));
            let lang = store.resolve_lang_tag(k.o_type).map(Arc::from);
            col_o1.push(Binding::Lit {
                val,
                dt,
                lang,
                t: None,
                op: None,
                p_id: None,
            });
        }
        col_count.push(Binding::Lit {
            val: FlakeValue::Long(st.count as i64),
            dt: dt_count.clone(),
            lang: None,
            t: None,
            op: None,
            p_id: None,
        });
        if want_min {
            col_min.push(
                st.min_s
                    .map_or(Binding::Unbound, |s| Binding::EncodedSid { s_id: s }),
            );
        }
        if want_max {
            col_max.push(
                st.max_s
                    .map_or(Binding::Unbound, |s| Binding::EncodedSid { s_id: s }),
            );
        }
        if want_sample {
            col_sample.push(
                st.sample_s
                    .map_or(Binding::Unbound, |s| Binding::EncodedSid { s_id: s }),
            );
        }
    }

    // Assemble columns in the SELECT schema order.
    let mut cols: Vec<Vec<Binding>> = Vec::with_capacity(schema.len());
    for v in schema.iter().copied() {
        if v == group_var {
            cols.push(col_o1.clone());
        } else if v == count_var {
            cols.push(col_count.clone());
        } else if Some(v) == min_var {
            cols.push(col_min.clone());
        } else if Some(v) == max_var {
            cols.push(col_max.clone());
        } else if Some(v) == sample_var {
            cols.push(col_sample.clone());
        } else {
            return Err(QueryError::Internal(format!(
                "group-by-object star: schema var {v:?} not produced by fast path"
            )));
        }
    }
    Batch::new(schema, cols).map_err(|e| QueryError::execution(format!("batch build: {e}")))
}
