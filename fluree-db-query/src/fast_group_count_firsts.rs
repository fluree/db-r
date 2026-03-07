use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::BoxedOperator;
use crate::operator::{Operator, OperatorState};
use crate::triple::Term;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafletDirEntryV3,
};
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::read::column_loader::{
    load_leaflet_columns, load_leaflet_columns_cached,
};
// V5 leaflet internals (LeafletHeader, decode_leaflet_region1/2, read_leaf_header,
// CachedRegion1, CachedRegion2) have been removed. V5 fast-paths that directly
// accessed raw leaflet bytes are stubbed with todo!("V3 leaflet fast-path").
use fluree_db_binary_index::{
    BinaryGraphView, BinaryIndexStore, ColumnProjection, ColumnSet, OverlayOp, RunSortOrder,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{GraphId, StatsView};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct FactKey {
    s_id: u64,
    o_type: u16,
    o_key: u64,
    o_i: u32,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
enum GroupKey {
    Ref(u64),
    Lit { o_type: u16, o_key: u64 },
}

// ---------------------------------------------------------------------------
// Shared free functions (extracted from duplicate `impl` methods)
// ---------------------------------------------------------------------------

// Kept for: V3 leaflet fast-path port of group-count and bound-object-count operators.
// Use when: the V3 fast-path is implemented to replace the current fallback-to-scan path.
// How: these functions implement overlay delta merging and group-key accumulation
// that will be needed by any direct-leaflet-scan optimization.
#[allow(dead_code)]
fn overlay_ops_for_ctx(ctx: &ExecutionContext<'_>, gv: &BinaryGraphView) -> Vec<OverlayOp> {
    let Some(overlay) = ctx.overlay else {
        return Vec::new();
    };
    crate::binary_scan::translate_overlay_flakes(
        overlay,
        &gv.clone_store(),
        ctx.dict_novelty.as_ref(),
        ctx.to_t,
        gv.g_id(),
    )
}

#[expect(dead_code)]
#[inline]
fn group_key_for_fact(key: &FactKey) -> GroupKey {
    if key.o_type == OType::IRI_REF.as_u16() {
        GroupKey::Ref(key.o_key)
    } else {
        GroupKey::Lit {
            o_type: key.o_type,
            o_key: key.o_key,
        }
    }
}

// V5 leaflet first-record helpers (read_leaflet_first, prefix_from_leaflet_first,
// prefix_from_dir_or_leaflet, prefix_from_run_record) have been removed.
// The V5 fast-path that used them is stubbed below with todo!("V3 leaflet fast-path").

#[expect(dead_code)]
fn add_count(map: &mut HashMap<GroupKey, i64>, key: GroupKey, add: i64) {
    *map.entry(key).or_insert(0) += add;
}

#[expect(dead_code)]
#[inline]
fn io_to_query(where_: &'static str, e: std::io::Error) -> QueryError {
    QueryError::execution(format!("{where_}: {e}"))
}

#[inline]
fn should_fallback(ctx: &ExecutionContext<'_>) -> bool {
    // Fast-path is available if either V5 or V6 store is loaded.
    let has_store = ctx.graph_view().is_some() || ctx.binary_store.is_some();
    !has_store || ctx.history_mode || ctx.policy_enforcer.is_some()
}

/// Resolve a predicate [`Ref`] to its binary index `p_id`.
#[expect(dead_code)]
fn resolve_predicate_id(
    predicate: &crate::triple::Ref,
    store: &BinaryIndexStore,
    context_label: &str,
) -> Result<u32> {
    match predicate {
        crate::triple::Ref::Sid(sid) => store.sid_to_p_id(sid).ok_or_else(|| {
            QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
        }),
        crate::triple::Ref::Iri(iri) => store.find_predicate_id(iri).ok_or_else(|| {
            QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
        }),
        _ => Err(QueryError::InvalidQuery(format!(
            "{context_label} requires a bound predicate"
        ))),
    }
}

// V5 helpers removed: open_and_read_leaf, DecodedR1, decode_r1_cached, decode_r2_cached.
// These used V5 APIs (resolved_path, ensure_index_leaf_cached, read_leaf_header,
// LeafFileHeader, LeafletHeader, decode_leaflet_region1/2, CachedRegion1/2).
// The V5 fast-paths that used them are stubbed with todo!("V3 leaflet fast-path").

/// Merge novelty overlay deltas into an accumulator.
///
/// Translates overlay ops, filters by `p_id` and an additional caller-supplied `filter`,
/// then replays assert/retract ops in time order and calls `apply_delta` for each net change.
#[expect(dead_code)]
fn merge_overlay_deltas<F, A>(
    ctx: &ExecutionContext<'_>,
    gv: &BinaryGraphView,
    p_id: u32,
    filter: F,
    mut apply_delta: A,
) where
    F: Fn(&OverlayOp) -> bool,
    A: FnMut(&FactKey, i64),
{
    if ctx.overlay.is_none() {
        return;
    }
    let overlay_ops = overlay_ops_for_ctx(ctx, gv);
    if overlay_ops.is_empty() {
        return;
    }

    let mut ops: Vec<(FactKey, i64, bool)> = overlay_ops
        .into_iter()
        .filter(|op| op.p_id == p_id && filter(op))
        .map(|op| {
            (
                FactKey {
                    s_id: op.s_id,
                    o_type: op.o_type,
                    o_key: op.o_key,
                    o_i: op.o_i,
                },
                op.t,
                op.op,
            )
        })
        .collect();

    // Group by fact identity and apply ops in time order (retract before assert for same t).
    ops.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });

    let mut idx = 0usize;
    while idx < ops.len() {
        let key = ops[idx].0;
        // If the first op is a retract, the fact must have existed before overlay.
        let mut present = !ops[idx].2;

        while idx < ops.len() && ops[idx].0 == key {
            let op_is_assert = ops[idx].2;
            if op_is_assert {
                if !present {
                    present = true;
                    apply_delta(&key, 1);
                }
            } else if present {
                present = false;
                apply_delta(&key, -1);
            }
            idx += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Operator 1: PredicateGroupCountFirstsOperator
// ---------------------------------------------------------------------------

/// Fast-path: `?s <p> ?o GROUP BY ?o (COUNT(?s) AS ?count)` with `ORDER BY DESC(?count) LIMIT k`.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// If stats indicate this predicate has exactly one datatype for this graph, the operator never
/// decodes Region 2 (dt/lang are treated as constant). Otherwise it falls back to decoding Region 2
/// and grouping by full RDF literal identity (dt/lang).
///
/// Requires:
/// - POST order access
pub struct PredicateGroupCountFirstsOperator {
    /// Output schema: [object_var, count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    object_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::triple::Ref,
    /// LIMIT k (top-k by count)
    limit: usize,
    /// Optional stats view used to detect single-datatype predicates.
    // Kept for: V3 leaflet fast-path may use stats for single-datatype optimization.
    // Use when: V3 direct-leaflet-scan fast-path is implemented.
    #[expect(dead_code)]
    stats: Option<Arc<StatsView>>,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// Materialized results (already sorted and truncated to limit)
    results: Vec<(GroupKey, i64)>,
    /// V6 results: (o_type, o_key, count). When present, used instead of `results`.
    results_v6: Option<Vec<(u16, u64, i64)>>,
    /// Next result to emit
    pos: usize,
}

impl PredicateGroupCountFirstsOperator {
    pub fn new(
        subject_var: VarId,
        object_var: VarId,
        count_var: VarId,
        predicate: crate::triple::Ref,
        limit: usize,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        Self {
            schema: Arc::from(vec![object_var, count_var].into_boxed_slice()),
            subject_var,
            object_var,
            count_var,
            predicate,
            limit: limit.max(1),
            stats,
            state: OperatorState::Created,
            fallback: None,
            results: Vec::new(),
            results_v6: None,
            pos: 0,
        }
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::ScanOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::limit::LimitOperator;
        use crate::sort::{SortDirection, SortOperator, SortSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            Term::Var(self.object_var),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // RangeScanOperator (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(ScanOperator::new(tp, None, Vec::new()));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
            distinct: false,
        }];
        let grouped: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![self.object_var],
            agg_specs,
            None,
            false,
        ));

        let sorted: BoxedOperator = Box::new(SortOperator::new(
            grouped,
            vec![SortSpec {
                var: self.count_var,
                direction: SortDirection::Descending,
            }],
        ));

        let mut limited: BoxedOperator = Box::new(LimitOperator::new(sorted, self.limit));
        limited.open(ctx).await?;
        self.fallback = Some(limited);
        Ok(())
    }
}

#[async_trait]
impl Operator for PredicateGroupCountFirstsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.results.clear();
        self.pos = 0;
        self.fallback = None;

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no overlay — overlay delta merge not yet implemented).
        if ctx.overlay.is_none() {
            if let Some(binary_index_store) = ctx.binary_store.as_ref() {
                match group_count_v6(
                    binary_index_store,
                    ctx.binary_g_id,
                    &self.predicate,
                    self.limit,
                ) {
                    Ok(v6_results) => {
                        self.results_v6 = Some(v6_results);
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        // V5 leaf-scanning fast-path removed. Fall back to generic scan/aggregate.
        // TODO: V3 leaflet fast-path for group-count-firsts (port boundary-equality
        // optimization to V3 column-based format).
        self.open_fallback(ctx).await
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }

        // V6 results path: eagerly decode (o_type, o_key) → FlakeValue.
        if let Some(v6_results) = &self.results_v6 {
            if self.pos >= v6_results.len() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            let binary_index_store = ctx.binary_store.as_ref().ok_or_else(|| {
                QueryError::Internal("V6 group-count results but no V6 store".to_string())
            })?;
            let g_id = ctx.binary_g_id;
            let p_id = resolve_predicate_id_v6(&self.predicate, binary_index_store)?;
            let view =
                fluree_db_binary_index::BinaryGraphView::new(Arc::clone(binary_index_store), g_id);

            let batch_size = ctx.batch_size;
            let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
            let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

            while self.pos < v6_results.len() && col_o.len() < batch_size {
                let (o_type, o_key, count) = v6_results[self.pos];
                self.pos += 1;

                if o_type == OType::IRI_REF.as_u16() {
                    col_o.push(Binding::EncodedSid { s_id: o_key });
                } else {
                    let val = view
                        .decode_value(o_type, o_key, p_id)
                        .map_err(|e| QueryError::Internal(format!("V6 decode_value: {e}")))?;
                    let dt = binary_index_store
                        .resolve_datatype_sid(o_type)
                        .unwrap_or_else(|| fluree_db_core::Sid::new(0, ""));
                    let lang: Option<Arc<str>> =
                        binary_index_store.resolve_lang_tag(o_type).map(Arc::from);
                    col_o.push(Binding::Lit {
                        val,
                        dt,
                        lang,
                        t: None,
                        op: None,
                        p_id: None,
                    });
                }
                col_c.push(Binding::Lit {
                    val: fluree_db_core::FlakeValue::Long(count),
                    dt: fluree_db_core::Sid::xsd_integer(),
                    lang: None,
                    t: None,
                    op: None,
                    p_id: None,
                });
            }

            return Ok(Some(crate::binding::Batch::new(
                self.schema.clone(),
                vec![col_o, col_c],
            )?));
        }

        // V5 results path.
        if self.pos >= self.results.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let Some(gv) = ctx.graph_view() else {
            return Err(QueryError::InvalidQuery(
                "predicate group-count fast-path requires binary index store".to_string(),
            ));
        };
        let store = gv.clone_store();

        let batch_size = ctx.batch_size;
        let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
        let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

        while self.pos < self.results.len() && col_o.len() < batch_size {
            let (key, count) = self.results[self.pos];
            self.pos += 1;

            match key {
                GroupKey::Ref(s_id) => col_o.push(Binding::EncodedSid { s_id }),
                GroupKey::Lit { o_type, o_key } => {
                    // V5 results path: decode to FlakeValue from o_type/o_key.
                    let val = store
                        .decode_value_no_graph(o_type, o_key)
                        .map_err(|e| QueryError::Internal(format!("decode_value: {e}")))?;
                    let dt = store
                        .resolve_datatype_sid(o_type)
                        .unwrap_or_else(|| fluree_db_core::Sid::new(0, ""));
                    let lang: Option<Arc<str>> = store.resolve_lang_tag(o_type).map(Arc::from);
                    col_o.push(Binding::Lit {
                        val,
                        dt,
                        lang,
                        t: None,
                        op: None,
                        p_id: None,
                    });
                }
            }
            col_c.push(Binding::Lit {
                val: fluree_db_core::FlakeValue::Long(count),
                dt: fluree_db_core::Sid::xsd_integer(),
                lang: None,
                t: None,
                op: None,
                p_id: None,
            });
        }

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_o, col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.results.clear();
        self.results_v6 = None;
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.limit)
    }
}

// ---------------------------------------------------------------------------
// Operator 2: PredicateObjectCountFirstsOperator
// ---------------------------------------------------------------------------

/// Fast-path: `SELECT (COUNT(?s) AS ?count) WHERE { ?s <p> <o> }` in POST order.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// Semantics: matches the current "loose" mode when no datatype/lang constraint is specified:
/// compare on Region1 `(o_kind, o_key)` only (dt/lang are ignored).
pub struct PredicateObjectCountFirstsOperator {
    /// Output schema: [count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::triple::Ref,
    /// Bound object term (Sid/Iri/Value).
    object: Term,
    /// Optional stats view (reserved for future scan-shape pruning).
    // Kept for: future scan-shape pruning (e.g. datatype-gated equality / pruning).
    // Use when: we extend this operator to apply StatsView-derived pruning beyond FIRST-based skipping.
    #[expect(dead_code)]
    stats: Option<Arc<StatsView>>,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// Computed count (materialized at open)
    count: i64,
    /// Whether the single row has been emitted
    emitted: bool,
}

impl PredicateObjectCountFirstsOperator {
    pub fn new(
        predicate: crate::triple::Ref,
        subject_var: VarId,
        object: Term,
        count_var: VarId,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        Self {
            schema: Arc::from(vec![count_var].into_boxed_slice()),
            subject_var,
            count_var,
            predicate,
            object,
            stats,
            state: OperatorState::Created,
            fallback: None,
            count: 0,
            emitted: false,
        }
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::ScanOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            self.object.clone(),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // RangeScanOperator (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(ScanOperator::new(tp, None, Vec::new()));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
            distinct: false,
        }];
        let mut op: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![],
            agg_specs,
            None,
            false,
        ));
        op.open(ctx).await?;
        self.fallback = Some(op);
        Ok(())
    }
}

#[async_trait]
impl Operator for PredicateObjectCountFirstsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.count = 0;
        self.emitted = false;
        self.fallback = None;

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no overlay — overlay delta merge not yet implemented).
        if ctx.overlay.is_none() {
            if let Some(binary_index_store) = ctx.binary_store.as_ref() {
                match count_bound_object_v6(
                    binary_index_store,
                    ctx.binary_g_id,
                    &self.predicate,
                    &self.object,
                ) {
                    Ok(total) => {
                        self.count = total;
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        // V5 leaf-scanning fast-path removed. Fall back to generic scan/aggregate.
        // TODO: V3 leaflet fast-path for predicate-object count (port boundary-equality
        // optimization to V3 column-based format).
        return self.open_fallback(ctx).await;
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        self.emitted = true;

        let col_c = vec![Binding::Lit {
            val: fluree_db_core::FlakeValue::Long(self.count),
            dt: fluree_db_core::Sid::xsd_integer(),
            lang: None,
            t: None,
            op: None,
            p_id: None,
        }];

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.count = 0;
        self.emitted = false;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

// ---------------------------------------------------------------------------
// V6 fast-path implementations
// ---------------------------------------------------------------------------

/// Extract the object prefix `(o_type, o_key)` from a V3 leaflet directory entry's
/// `first_key` field, interpreted in POST order.
#[inline]
fn prefix_v6_from_entry(entry: &LeafletDirEntryV3) -> (u16, u64) {
    let rec = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
    (rec.o_type, rec.o_key)
}

/// Load a V3 leaflet's columns, using the `LeafletCache` when available.
fn load_v6_batch(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntryV3,
    payload_base: usize,
    order: RunSortOrder,
    cache: &Option<&Arc<fluree_db_binary_index::LeafletCache>>,
    leaf_id: u128,
    leaflet_idx: u8,
) -> Result<fluree_db_binary_index::ColumnBatch> {
    if let Some(c) = cache {
        load_leaflet_columns_cached(
            leaf_bytes,
            entry,
            payload_base,
            order,
            c,
            leaf_id,
            leaflet_idx,
        )
        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))
    } else {
        let mut needed = ColumnSet::EMPTY;
        needed.insert(ColumnId::OKey);
        if entry.o_type_const.is_none() {
            needed.insert(ColumnId::OType);
        }
        let projection = ColumnProjection {
            output: ColumnSet::EMPTY,
            internal: needed,
        };
        load_leaflet_columns(leaf_bytes, entry, payload_base, &projection, order)
            .map_err(|e| QueryError::Internal(format!("load columns: {e}")))
    }
}

/// Resolve a predicate [`Ref`] to its V6 binary index `p_id`.
fn resolve_predicate_id_v6(
    predicate: &crate::triple::Ref,
    store: &BinaryIndexStore,
) -> Result<u32> {
    let sid = match predicate {
        crate::triple::Ref::Sid(s) => s.clone(),
        crate::triple::Ref::Iri(i) => store.encode_iri(i),
        crate::triple::Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast-path requires bound predicate".to_string(),
            ))
        }
    };
    store
        .sid_to_p_id(&sid)
        .ok_or_else(|| QueryError::Internal("predicate not found in V6 dictionary".to_string()))
}

/// V6 fast-path: count rows for a bound `(predicate, object)` triple.
///
/// Scans the POST leaf range for the predicate, uses boundary-equality on
/// `(o_type, o_key)` to skip whole leaflets, and decodes only `o_key` + `o_type`
/// columns when needed.
fn count_bound_object_v6(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &crate::triple::Ref,
    object: &Term,
) -> Result<i64> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    // Translate the bound object term into V6 (o_type, o_key).
    let (target_o_type, target_o_key) = translate_term_to_v6(object, store, p_id, g_id)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: 0,
        o_type: target_o_type,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: target_o_type,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let target_prefix = (target_o_type, target_o_key);
    let mut total: i64 = 0;
    let cache = store.leaflet_cache();

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let prefix = prefix_v6_from_entry(entry);
            if prefix < target_prefix {
                continue;
            }
            if prefix > target_prefix {
                break;
            }

            // Boundary-equality: check the next leaflet's first prefix.
            let next_prefix = if i + 1 < dir.entries.len() {
                Some(prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next_entry = &branch.leaves[leaf_idx + 1].first_key;
                Some((next_entry.o_type, next_entry.o_key))
            } else {
                None
            };

            if next_prefix == Some(target_prefix) {
                total += entry.row_count as i64;
                continue;
            }

            // Decode columns (cached when available).
            let batch = load_v6_batch(
                &bytes,
                entry,
                dir.payload_base,
                header.order,
                &cache,
                leaf_id,
                i as u8,
            )?;

            for row in 0..batch.row_count {
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                if ot == target_o_type && batch.o_key.get(row) == target_o_key {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// V6 fast-path: GROUP BY ?o COUNT(?s) for a predicate.
///
/// Returns `Vec<(o_type, o_key, count)>` sorted by count descending, truncated to `limit`.
fn group_count_v6(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &crate::triple::Ref,
    limit: usize,
) -> Result<Vec<(u16, u64, i64)>> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
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
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let mut counts: HashMap<(u16, u64), i64> = HashMap::new();
    let cache = store.leaflet_cache();

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;
        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_entry.leaf_cid.to_bytes().as_ref());

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let prefix = prefix_v6_from_entry(entry);

            // Boundary-equality: check if this leaflet is entirely one object value.
            let next_prefix = if i + 1 < dir.entries.len() {
                Some(prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next_entry = &branch.leaves[leaf_idx + 1].first_key;
                Some((next_entry.o_type, next_entry.o_key))
            } else {
                None
            };

            if next_prefix == Some(prefix) {
                // Entire leaflet is one object value.
                *counts.entry(prefix).or_insert(0) += entry.row_count as i64;
                continue;
            }

            // Decode columns (cached when available).
            let batch = load_v6_batch(
                &bytes,
                entry,
                dir.payload_base,
                header.order,
                &cache,
                leaf_id,
                i as u8,
            )?;

            for row in 0..batch.row_count {
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                let ok = batch.o_key.get(row);
                *counts.entry((ot, ok)).or_insert(0) += 1;
            }
        }
    }

    // Sort by count desc, truncate.
    let mut rows: Vec<(u16, u64, i64)> = counts
        .into_iter()
        .map(|((ot, ok), c)| (ot, ok, c))
        .collect();
    rows.sort_unstable_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1)));
    rows.truncate(limit);

    Ok(rows)
}

/// Translate a bound object `Term` to V6 `(o_type, o_key)`.
fn translate_term_to_v6(
    term: &Term,
    store: &BinaryIndexStore,
    _p_id: u32,
    _g_id: GraphId,
) -> Result<(u16, u64)> {
    match term {
        Term::Sid(sid) => {
            let s_id = store
                .sid_to_s_id(sid)
                .map_err(|e| QueryError::execution(format!("sid_to_s_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object SID not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Iri(iri) => {
            let s_id = store
                .find_subject_id(iri)
                .map_err(|e| QueryError::execution(format!("find_subject_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object IRI not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Value(val) => {
            // For literal values, we need the FlakeValue → (o_type, o_key) translation.
            // Use the Sid-based dt info from the FlakeValue if available.
            let (ot, ok) = crate::binary_scan::value_to_otype_okey_simple(val, store)?;
            Ok((ot.as_u16(), ok))
        }
        Term::Var(_) => Err(QueryError::InvalidQuery(
            "fast-path requires a bound object".to_string(),
        )),
    }
}
