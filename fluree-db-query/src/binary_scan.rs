//! Binary scan operator — eagerly materializes `ColumnBatch` rows into `Binding` values.
//!
//! - Uses `BinaryCursor` (leaflet-at-a-time columnar batches)
//! - Uses `o_type` for value dispatch
//! - Eagerly materializes all values (no EncodedLit/EncodedSid)
//!
//! The eager approach trades some allocation for simplicity. Deferred decoding
//! can be added in a follow-up when perf requires it.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::read::column_types::ColumnSet;
use fluree_db_binary_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnBatch,
    ColumnProjection, OverlayOp,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{
    dt_compatible, range_with_overlay, Flake, FlakeValue, GraphId, IndexType, LedgerSnapshot,
    NoOverlay, ObjectBounds, OverlayProvider, RangeMatch, RangeOptions, RangeTest, Sid,
};

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{Operator, OperatorState};
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;

// ============================================================================
// Shared types and utilities
// ============================================================================

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::value_id::ObjKind;

/// Mask indicating which triple components should be emitted as columns.
///
/// Used by plan-time optimizations to prune unused output columns.
#[derive(Debug, Clone, Copy)]
pub struct EmitMask {
    pub s: bool,
    pub p: bool,
    pub o: bool,
}

impl EmitMask {
    pub const ALL: EmitMask = EmitMask {
        s: true,
        p: true,
        o: true,
    };
}

/// Convert `IndexType` (query-layer) to `RunSortOrder` (binary-index-layer).
pub fn index_type_to_sort_order(index: IndexType) -> RunSortOrder {
    match index {
        IndexType::Spot => RunSortOrder::Spot,
        IndexType::Psot => RunSortOrder::Psot,
        IndexType::Post => RunSortOrder::Post,
        IndexType::Opst => RunSortOrder::Opst,
    }
}

/// Build a schema vector from a triple pattern, respecting `EmitMask` pruning.
///
/// Returns `(schema, s_var_pos, p_var_pos, o_var_pos)` where positions are
/// indices into the schema if the corresponding variable is emitted.
pub fn schema_from_pattern_with_emit(
    pattern: &TriplePattern,
    emit: EmitMask,
) -> (Vec<VarId>, Option<usize>, Option<usize>, Option<usize>) {
    let mut schema = Vec::new();
    let mut s_pos = None;
    let mut p_pos = None;
    let mut o_pos = None;

    let mut find_or_push = |v: VarId| -> usize {
        if let Some(idx) = schema.iter().position(|x| *x == v) {
            idx
        } else {
            let idx = schema.len();
            schema.push(v);
            idx
        }
    };

    if emit.s {
        if let Ref::Var(v) = &pattern.s {
            s_pos = Some(find_or_push(*v));
        }
    }
    if emit.p {
        if let Ref::Var(v) = &pattern.p {
            p_pos = Some(find_or_push(*v));
        }
    }
    if emit.o {
        if let Term::Var(v) = &pattern.o {
            o_pos = Some(find_or_push(*v));
        }
    }

    (schema, s_pos, p_pos, o_pos)
}

#[inline]
fn expr_needs_t(expr: &Expression) -> bool {
    match expr {
        Expression::Var(_) | Expression::Const(_) => false,
        Expression::Call { func, args } => {
            matches!(func, Function::T) || args.iter().any(expr_needs_t)
        }
        Expression::Exists { .. } => false,
    }
}

#[inline]
fn inline_ops_need_t(ops: &[InlineOperator]) -> bool {
    ops.iter().any(|op| match op {
        InlineOperator::Filter(e) => expr_needs_t(e),
        InlineOperator::Bind { expr, .. } => expr_needs_t(expr),
    })
}

// `translate_overlay_flakes` lives below, after BinaryScanOperator.

/// Public type alias — `ScanOperator` is the sole scan implementation.
pub type ScanOperator = BinaryScanOperator;

// ============================================================================
// BinaryScanOperator
// ============================================================================

/// Scan operator: streams leaflets from `BinaryCursor`, eagerly decoding
/// `ColumnBatch` rows into `Binding::Sid` / `Binding::Lit` values.
pub struct BinaryScanOperator {
    pattern: TriplePattern,
    index: IndexType,
    schema: Arc<[VarId]>,
    s_var_pos: Option<usize>,
    p_var_pos: Option<usize>,
    o_var_pos: Option<usize>,
    state: OperatorState,
    /// Set in `open()` from `ExecutionContext`.
    store: Option<Arc<BinaryIndexStore>>,
    g_id: GraphId,
    cursor: Option<BinaryCursor>,
    /// Pre-computed p_id → Sid (all predicates, done once at open).
    p_sids: Vec<Sid>,
    /// Cached s_id → Sid for amortized IRI resolution.
    sid_cache: HashMap<u64, Sid>,
    /// Whether predicate is a variable (for internal predicate filtering).
    p_is_var: bool,
    inline_ops: Vec<InlineOperator>,
    /// Encoded pre-filters compiled from inline filter expressions.
    ///
    /// Evaluated on `(s_id, o_type, o_key)` before any value decoding.
    encoded_pre_filters: Vec<EncodedPreFilter>,
    // Kept for: plan-time emit pruning and index override during query optimization.
    // Use when: planner emits BinaryScanOperator with pruned columns or forced index.
    #[expect(dead_code)]
    emit: EmitMask,
    #[expect(dead_code)]
    index_hint: Option<IndexType>,
    object_bounds: Option<ObjectBounds>,
    /// Bound object value, if the triple pattern's object is a constant.
    bound_o: Option<FlakeValue>,
    /// Pre-computed repeated-variable flags from the triple pattern.
    check_s_eq_o: bool,
    check_s_eq_p: bool,
    check_p_eq_o: bool,
    /// Range-scan fallback iterator (used when no binary store is attached).
    range_iter: Option<std::vec::IntoIter<RangeFlake>>,
}

#[derive(Clone, Debug)]
struct RangeFlake {
    flake: Flake,
    /// When present (dataset mode), identifies the originating ledger for SID decoding
    /// and provenance-carrying bindings (`Binding::IriMatch`).
    ledger_alias: Option<Arc<str>>,
}

/// A filter that can be evaluated on encoded index columns (no term decoding).
#[derive(Clone, Debug)]
enum EncodedPreFilter {
    /// `FILTER(LANG(?o) = "<tag>")` for the object var `?o` in this scan.
    LangEqualsOType { required_otype: u16 },
    /// `FILTER(ISBLANK(?o))` for the object var `?o` in this scan.
    ///
    /// Blank nodes are currently encoded as `OType::IRI_REF` with a `sid64` whose
    /// `SubjectId.ns_code == namespaces::BLANK_NODE` (not as `OType::BLANK_NODE`).
    ObjectIsBlankNode,
    /// `FILTER(?s = ?o)` where `?o` is a REF (IRI or bnode) and equals the subject id.
    SubjectEqObjectRef,
    /// `FILTER(?s != ?o)` under two-valued logic: false only when both sides are comparable+equal.
    SubjectNeObjectRef,
}

impl EncodedPreFilter {
    #[inline]
    fn eval_row(&self, s_id: u64, o_type: u16, o_key: u64) -> bool {
        match self {
            EncodedPreFilter::LangEqualsOType { required_otype } => o_type == *required_otype,
            EncodedPreFilter::ObjectIsBlankNode => {
                if o_type != fluree_db_core::o_type::OType::IRI_REF.as_u16() {
                    return false;
                }
                fluree_db_core::subject_id::SubjectId::from_u64(o_key).ns_code()
                    == fluree_vocab::namespaces::BLANK_NODE
            }
            EncodedPreFilter::SubjectEqObjectRef => {
                let is_ref = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16()
                    || o_type == fluree_db_core::o_type::OType::BLANK_NODE.as_u16();
                is_ref && s_id == o_key
            }
            EncodedPreFilter::SubjectNeObjectRef => {
                let is_ref = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16()
                    || o_type == fluree_db_core::o_type::OType::BLANK_NODE.as_u16();
                !(is_ref && s_id == o_key)
            }
        }
    }
}

fn compile_encoded_pre_filters_and_prune_inline_ops(
    inline_ops: &[InlineOperator],
    pattern: &TriplePattern,
    store: &BinaryIndexStore,
) -> (Vec<EncodedPreFilter>, Vec<InlineOperator>) {
    use crate::ir::{Expression, FilterValue, Function};

    let obj_var = match &pattern.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    let subj_var = match &pattern.s {
        Ref::Var(v) => Some(*v),
        _ => None,
    };

    let mut out = Vec::new();
    let mut pruned = Vec::with_capacity(inline_ops.len());
    for op in inline_ops {
        let InlineOperator::Filter(expr) = op else {
            pruned.push(op.clone());
            continue;
        };
        let Expression::Call { func, args } = expr else {
            pruned.push(op.clone());
            continue;
        };
        if args.len() == 1 {
            // FILTER(ISBLANK(?o))
            if *func == Function::IsBlank {
                if let (Some(ov), Expression::Var(v)) = (obj_var, &args[0]) {
                    if *v == ov {
                        out.push(EncodedPreFilter::ObjectIsBlankNode);
                        continue;
                    }
                }
            }
            pruned.push(op.clone());
            continue;
        }
        if args.len() != 2 {
            pruned.push(op.clone());
            continue;
        }

        // FILTER(LANG(?o) = "en")  (either side order)
        let is_lang_o = |e: &Expression| match (e, obj_var) {
            (Expression::Call { func, args }, Some(ov)) => {
                *func == Function::Lang
                    && args.len() == 1
                    && matches!(&args[0], Expression::Var(v) if *v == ov)
            }
            _ => false,
        };
        if is_lang_o(&args[0]) {
            if let Expression::Const(FilterValue::String(tag)) = &args[1] {
                if let Some(lang_id) = store.resolve_lang_id(tag) {
                    let required_otype =
                        fluree_db_core::o_type::OType::lang_string(lang_id).as_u16();
                    out.push(EncodedPreFilter::LangEqualsOType { required_otype });
                    continue;
                }
            }
            pruned.push(op.clone());
            continue;
        }
        if is_lang_o(&args[1]) {
            if let Expression::Const(FilterValue::String(tag)) = &args[0] {
                if let Some(lang_id) = store.resolve_lang_id(tag) {
                    let required_otype =
                        fluree_db_core::o_type::OType::lang_string(lang_id).as_u16();
                    out.push(EncodedPreFilter::LangEqualsOType { required_otype });
                    continue;
                }
            }
            pruned.push(op.clone());
            continue;
        }

        // FILTER(?s = ?o) / FILTER(?s != ?o) (either side order)
        let (Some(sv), Some(ov)) = (subj_var, obj_var) else {
            pruned.push(op.clone());
            continue;
        };
        let is_s = |e: &Expression| matches!(e, Expression::Var(v) if *v == sv);
        let is_o = |e: &Expression| matches!(e, Expression::Var(v) if *v == ov);
        if !(is_s(&args[0]) && is_o(&args[1]) || is_o(&args[0]) && is_s(&args[1])) {
            pruned.push(op.clone());
            continue;
        }
        match func {
            Function::Eq => out.push(EncodedPreFilter::SubjectEqObjectRef),
            Function::Ne => out.push(EncodedPreFilter::SubjectNeObjectRef),
            _ => {
                pruned.push(op.clone());
            }
        }
    }
    (out, pruned)
}

impl BinaryScanOperator {
    /// Create a new scan operator. The `store` and `g_id` are resolved from
    /// `ExecutionContext` during `open()`.
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
    ) -> Self {
        Self::new_with_emit_and_index(pattern, object_bounds, inline_ops, EmitMask::ALL, None)
    }

    /// Create a scan operator with explicit emit mask and index hint.
    pub fn new_with_emit_and_index(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
    ) -> Self {
        let s_bound = pattern.s_bound();
        let p_bound = pattern.p_bound();
        let o_bound = pattern.o_bound();
        let o_is_ref = pattern.o_is_ref();

        let mut index = IndexType::for_query(s_bound, p_bound, o_bound, o_is_ref);
        if object_bounds.is_some() && index == IndexType::Psot {
            index = IndexType::Post;
        }
        // Generic optimization: if the object is a constant (and can be safely encoded),
        // prefer the object-leading OPST index when the subject is unbound. This avoids
        // pathological scans like PSOT(p, *, o_const) that can't narrow by o_key.
        //
        // IMPORTANT: plain strings without a datatype constraint are ambiguous (xsd:string
        // vs rdf:langString). In that case we don't force OPST because we may be unable to
        // encode (o_type, o_key) during open(), and OPST would devolve into a wide scan.
        if index_hint.is_none()
            && object_bounds.is_none()
            && !s_bound
            && o_bound
            && (pattern.dtc.is_some() || !matches!(&pattern.o, Term::Value(FlakeValue::String(_))))
        {
            index = IndexType::Opst;
        }
        if let Some(hint) = index_hint {
            index = hint;
        }

        let (base_schema, s_var_pos, p_var_pos, o_var_pos) =
            schema_from_pattern_with_emit(&pattern, emit);
        let p_is_var = pattern.p.is_var();
        let schema: Arc<[VarId]> = extend_schema(&base_schema, &inline_ops).into();
        let (check_s_eq_o, check_s_eq_p, check_p_eq_o) = repeated_var_flags(&pattern);

        Self {
            pattern,
            index,
            schema,
            s_var_pos,
            p_var_pos,
            o_var_pos,
            state: OperatorState::Created,
            store: None,
            g_id: 0,
            cursor: None,
            p_sids: Vec::new(),
            sid_cache: HashMap::new(),
            p_is_var,
            inline_ops,
            encoded_pre_filters: Vec::new(),
            emit,
            index_hint,
            object_bounds,
            bound_o: None,
            check_s_eq_o,
            check_s_eq_p,
            check_p_eq_o,
            range_iter: None,
        }
    }

    /// Helper to get the store ref, panics if not yet set (before open).
    fn store(&self) -> &Arc<BinaryIndexStore> {
        self.store.as_ref().expect("store set in open()")
    }

    /// Base schema length (max var position + 1).
    fn base_schema_len(&self) -> usize {
        [self.s_var_pos, self.p_var_pos, self.o_var_pos]
            .into_iter()
            .flatten()
            .max()
            .map_or(0, |m| m + 1)
    }

    /// Convert collected columns into a Batch, handling empty-schema and exhaustion.
    fn finalize_columns(
        &mut self,
        columns: Vec<Vec<Binding>>,
        produced: usize,
    ) -> Result<Option<Batch>> {
        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if self.schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Ok(Some(Batch::new(self.schema.clone(), columns)?))
    }

    /// Convert Flakes from the range_iter fallback into bindings, mirroring
    /// `batch_to_bindings` but for pre-decoded Flake values.
    fn flakes_to_bindings(
        &mut self,
        columns: &mut [Vec<Binding>],
        ctx: &ExecutionContext<'_>,
        batch_size: usize,
    ) -> Result<usize> {
        let base_len = self.base_schema_len();
        let num_vars = columns.len();
        let mut produced = 0;

        while produced < batch_size {
            let Some(rf) = self.range_iter.as_mut().and_then(|it| it.next()) else {
                break;
            };
            let flake = rf.flake;
            let ledger_alias = rf.ledger_alias;

            // Repeated-variable checks.
            if self.check_s_eq_p && flake.s != flake.p {
                continue;
            }
            if self.check_s_eq_o {
                match &flake.o {
                    FlakeValue::Ref(o) if *o == flake.s => {}
                    _ => continue,
                }
            }
            if self.check_p_eq_o {
                match &flake.o {
                    FlakeValue::Ref(o) if *o == flake.p => {}
                    _ => continue,
                }
            }

            // Datatype / language constraint checks (range fallback path).
            if let Some(dtc) = &self.pattern.dtc {
                if !dt_compatible(dtc.datatype(), &flake.dt) {
                    continue;
                }
                if let Some(tag) = dtc.lang_tag() {
                    let flake_lang = flake.m.as_ref().and_then(|m| m.lang.as_ref());
                    if flake_lang.map(|s| s.as_str()) != Some(tag) {
                        continue;
                    }
                }
            }

            let mut bindings: Vec<Binding> = vec![Binding::Unbound; base_len];

            if let Some(pos) = self.s_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = sid_binding(ctx, &flake.s, ledger_alias.as_ref());
            }
            if let Some(pos) = self.p_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = sid_binding(ctx, &flake.p, ledger_alias.as_ref());
            }
            if let Some(pos) = self.o_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = match &flake.o {
                    FlakeValue::Ref(r) => sid_binding(ctx, r, ledger_alias.as_ref()),
                    v => {
                        let dtc = match flake
                            .m
                            .as_ref()
                            .and_then(|m| m.lang.as_ref())
                            .map(|s| Arc::<str>::from(s.as_str()))
                        {
                            Some(lang) => DatatypeConstraint::LangTag(lang),
                            None => DatatypeConstraint::Explicit(flake.dt.clone()),
                        };
                        Binding::Lit {
                            val: v.clone(),
                            dtc,
                            t: Some(flake.t),
                            op: if ctx.history_mode {
                                Some(flake.op)
                            } else {
                                None
                            },
                            p_id: None,
                        }
                    }
                };
            }

            if !apply_inline(&self.inline_ops, &self.schema, &mut bindings, Some(ctx))? {
                continue;
            }
            if bindings.len() < num_vars {
                bindings.resize(num_vars, Binding::Unbound);
            }

            for (col, binding) in columns.iter_mut().zip(bindings) {
                col.push(binding);
            }
            produced += 1;
        }

        Ok(produced)
    }

    async fn open_range_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let no_overlay = NoOverlay;
        let mut out: Vec<RangeFlake> = Vec::new();

        // Dataset-aware range fallback:
        // - In single-db mode, scan only `ctx.snapshot` / `self.g_id`.
        // - In dataset mode, scan all active graphs and union results.
        match ctx.active_graphs() {
            crate::dataset::ActiveGraphs::Single => {
                let overlay: &dyn OverlayProvider = ctx.overlay.unwrap_or(&no_overlay);
                let match_val = build_match_val_for_snapshot(ctx, ctx.snapshot, &self.pattern)?;
                let opts = RangeOptions {
                    to_t: Some(ctx.to_t),
                    from_t: ctx.from_t,
                    object_bounds: self.object_bounds.clone(),
                    history_mode: ctx.history_mode,
                    ..Default::default()
                };
                let mut flakes = range_with_overlay(
                    ctx.snapshot,
                    self.g_id,
                    overlay,
                    self.index,
                    RangeTest::Eq,
                    match_val,
                    opts,
                )
                .await
                .map_err(|e| QueryError::Internal(format!("range_with_overlay: {e}")))?;

                // Apply policy filtering (including f:query) when present.
                flakes = Self::filter_flakes_by_policy(
                    ctx,
                    ctx.snapshot,
                    overlay,
                    ctx.to_t,
                    self.g_id,
                    flakes,
                )
                .await?;
                out.extend(flakes.into_iter().map(|flake| RangeFlake {
                    flake,
                    ledger_alias: None,
                }));
            }
            crate::dataset::ActiveGraphs::Many(graphs) => {
                for graph in graphs {
                    let match_val =
                        build_match_val_for_snapshot(ctx, graph.snapshot, &self.pattern)?;
                    let opts = RangeOptions {
                        to_t: Some(graph.to_t),
                        from_t: ctx.from_t,
                        object_bounds: self.object_bounds.clone(),
                        history_mode: ctx.history_mode,
                        ..Default::default()
                    };
                    let mut flakes = range_with_overlay(
                        graph.snapshot,
                        graph.g_id,
                        graph.overlay,
                        self.index,
                        RangeTest::Eq,
                        match_val,
                        opts,
                    )
                    .await
                    .map_err(|e| QueryError::Internal(format!("range_with_overlay: {e}")))?;

                    // Apply graph-scoped policy filtering when present.
                    flakes = Self::filter_flakes_by_policy(
                        ctx,
                        graph.snapshot,
                        graph.overlay,
                        graph.to_t,
                        graph.g_id,
                        flakes,
                    )
                    .await?;
                    let alias = Arc::clone(&graph.ledger_id);
                    out.extend(flakes.into_iter().map(|flake| RangeFlake {
                        flake,
                        ledger_alias: Some(Arc::clone(&alias)),
                    }));
                }
            }
        }

        self.range_iter = Some(out.into_iter());
        self.cursor = None;
        self.state = OperatorState::Open;
        Ok(())
    }

    /// Apply policy filtering to a batch of flakes for a specific graph.
    ///
    /// When a policy enforcer is present on the execution context, we:
    /// 1) Populate the class cache for subjects in this batch (required for f:onClass)
    /// 2) Filter flakes (async, supports f:query) using the graph's snapshot/overlay/to_t.
    async fn filter_flakes_by_policy(
        ctx: &ExecutionContext<'_>,
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        g_id: GraphId,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        let Some(enforcer) = ctx.policy_enforcer.as_ref() else {
            return Ok(flakes);
        };
        if enforcer.is_root() || flakes.is_empty() {
            return Ok(flakes);
        }

        // Populate class cache for all subjects in this batch (deduped).
        let mut subjects: Vec<Sid> = flakes.iter().map(|f| f.s.clone()).collect();
        subjects.sort();
        subjects.dedup();
        let db = fluree_db_core::GraphDbRef::new(snapshot, g_id, overlay, to_t);
        enforcer
            .populate_class_cache_for_graph(db, &subjects)
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))?;

        enforcer
            .filter_flakes_for_graph(snapshot, overlay, to_t, &ctx.tracker, flakes)
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))
    }

    /// Extract bound Sids from the pattern, normalizing to the V6 store's
    /// namespace encoding.
    ///
    /// SPARQL lowers IRIs to `Ref::Sid` / `Term::Sid` using the snapshot's
    /// namespace codes, which may differ from the V6 store's codes after a
    /// rebuild. Re-encoding through the store ensures the Sid matches decoded
    /// values from the index.
    fn extract_bound_terms(&self) -> (Option<Sid>, Option<Sid>, Option<FlakeValue>) {
        let s_sid = match &self.pattern.s {
            Ref::Sid(s) => Some(self.re_encode_sid(s)),
            Ref::Iri(iri) => Some(self.store().encode_iri(iri)),
            _ => None,
        };
        let p_sid = match &self.pattern.p {
            Ref::Sid(s) => Some(self.re_encode_sid(s)),
            Ref::Iri(iri) => Some(self.store().encode_iri(iri)),
            _ => None,
        };
        let o_val = match &self.pattern.o {
            Term::Sid(sid) => Some(FlakeValue::Ref(self.re_encode_sid(sid))),
            Term::Iri(iri) => Some(FlakeValue::Ref(self.store().encode_iri(iri))),
            Term::Value(v) => Some(v.clone()),
            Term::Var(_) => None,
        };
        (s_sid, p_sid, o_val)
    }

    /// Re-encode a Sid from the pattern's namespace space into the V6 store's
    /// namespace space. Reconstructs the full IRI and re-encodes via the store's
    /// prefix trie, ensuring namespace codes match decoded index values.
    #[inline]
    fn re_encode_sid(&self, sid: &Sid) -> Sid {
        let iri = self.store().sid_to_iri(sid);
        self.store().encode_iri(&iri)
    }

    /// Build a `BinaryFilter` from bound pattern terms.
    fn build_filter(
        &self,
        s_sid: &Option<Sid>,
        p_sid: &Option<Sid>,
    ) -> std::io::Result<BinaryFilter> {
        let s_id = if let Some(sid) = s_sid {
            self.store().sid_to_s_id(sid)?
        } else {
            None
        };
        let p_id = p_sid.as_ref().and_then(|sid| self.store().sid_to_p_id(sid));

        Ok(BinaryFilter {
            s_id,
            p_id,
            o_type: None,
            o_key: None,
            o_i: None,
        })
    }

    /// Resolve s_id → Sid with caching.
    fn resolve_s_id(&mut self, s_id: u64) -> Result<Sid> {
        if let Some(sid) = self.sid_cache.get(&s_id) {
            return Ok(sid.clone());
        }
        let iri = self
            .store()
            .resolve_subject_iri(s_id)
            .map_err(|e| QueryError::Internal(format!("resolve s_id={s_id}: {e}")))?;
        let sid = self.store().encode_iri(&iri);
        self.sid_cache.insert(s_id, sid.clone());
        Ok(sid)
    }

    /// Resolve p_id → Sid (pre-computed, O(1)).
    #[inline]
    fn resolve_p_id(&self, p_id: u32) -> Sid {
        self.p_sids
            .get(p_id as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, ""))
    }

    /// Filter: skip internal db: predicates when predicate is a variable.
    #[inline]
    fn is_internal_predicate(&self, p_id: u32) -> bool {
        if !self.p_is_var || self.g_id != 0 {
            return false;
        }
        self.p_sids
            .get(p_id as usize)
            .is_some_and(|s| s.namespace_code == fluree_vocab::namespaces::FLUREE_DB)
    }

    /// Enforce within-pattern repeated-variable constraints.
    ///
    /// SPARQL allows the same variable to appear in multiple positions of a triple pattern,
    /// e.g. `?x <p> ?x` or `?x ?x ?o`. These are equality constraints that must be applied
    /// even when emission pruning omits one of the positions.
    fn within_row_var_equality_ok(
        &mut self,
        s_id: u64,
        p_id: u32,
        o_type: u16,
        o_key: u64,
    ) -> Result<bool> {
        // Fast path: no repeated variables in this pattern (the common case).
        if !self.check_s_eq_o && !self.check_s_eq_p && !self.check_p_eq_o {
            return Ok(true);
        }

        if self.check_s_eq_o {
            // s==o only possible if o is a ref pointing at s_id.
            let ot = fluree_db_core::o_type::OType::from_u16(o_type);
            if !(ot.is_iri_ref() || ot.is_blank_node()) {
                return Ok(false);
            }
            if o_key != s_id {
                return Ok(false);
            }
        }

        // Comparisons involving predicate IDs require Sid materialization (different ID domains).
        if self.check_s_eq_p {
            let s = self.resolve_s_id(s_id)?;
            let p = self.resolve_p_id(p_id);
            if s != p {
                return Ok(false);
            }
        }
        if self.check_p_eq_o {
            let ot = fluree_db_core::o_type::OType::from_u16(o_type);
            if !(ot.is_iri_ref() || ot.is_blank_node()) {
                return Ok(false);
            }
            let o_sid = self.resolve_s_id(o_key)?;
            let p_sid = self.resolve_p_id(p_id);
            if o_sid != p_sid {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[inline]
    fn set_binding_at(slots: &mut [Binding], pos: usize, b: Binding) -> bool {
        match &slots[pos] {
            Binding::Unbound => {
                slots[pos] = b;
                true
            }
            existing => existing == &b,
        }
    }

    /// Check whether this row matches the triple pattern's datatype constraint (if any).
    #[inline]
    fn matches_datatype_constraint(&self, o_type: u16) -> bool {
        let Some(dtc) = &self.pattern.dtc else {
            return true;
        };

        let Some(dt_sid) = self.store().resolve_datatype_sid(o_type) else {
            return false;
        };
        if !dt_compatible(dtc.datatype(), &dt_sid) {
            return false;
        }

        if let Some(tag) = dtc.lang_tag() {
            self.store().resolve_lang_tag(o_type) == Some(tag)
        } else {
            true
        }
    }

    /// Convert a ColumnBatch into columnar Bindings.
    fn batch_to_bindings(
        &mut self,
        batch: &ColumnBatch,
        columns: &mut [Vec<Binding>],
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<usize> {
        let mut produced = 0;
        let ncols = self.schema.len();
        let base_len = self.base_schema_len();
        let mut bindings = Vec::with_capacity(ncols.max(base_len));
        let store_arc: Arc<BinaryIndexStore> = Arc::clone(self.store());
        let view = BinaryGraphView::new(Arc::clone(&store_arc), self.g_id);
        let dict_overlay = ctx.and_then(|c| c.dict_novelty.clone()).map(|dn| {
            crate::dict_overlay::DictOverlay::new(
                BinaryGraphView::new(Arc::clone(&store_arc), self.g_id),
                dn,
            )
        });

        // Late materialization is safe only when the BinaryIndexStore is authoritative
        // for decoding (no novelty overlay with ephemeral IDs).
        //
        // Note: ExecutionContext always carries an overlay provider; `NoOverlay` has epoch=0.
        let late_materialize = ctx.is_some_and(|c| c.overlay.map(|o| o.epoch()).unwrap_or(0) == 0)
            // If a repeated variable forces two components into the same output slot,
            // late-materialization must produce comparable binding representations.
            // In particular, `?x ?x ?o` would otherwise compare EncodedSid vs EncodedPid.
            && !self.check_s_eq_p
            && !self.check_p_eq_o;

        let encode_object =
            |o_type: u16, o_key: u64, p_id: u32, t: i64, o_i: u32| -> Option<Binding> {
                let ot = OType::from_u16(o_type);
                match ot.decode_kind() {
                    fluree_db_core::o_type::DecodeKind::IriRef => {
                        Some(Binding::EncodedSid { s_id: o_key })
                    }
                    fluree_db_core::o_type::DecodeKind::BlankNode => {
                        // Blank nodes aren't subject-dict IDs in V3; materialize to a Sid now.
                        Some(Binding::Sid(Sid::new(0, format!("_:b{}", o_key))))
                    }
                    fluree_db_core::o_type::DecodeKind::StringDict => {
                        let (dt_id, lang_id) = if ot.is_lang_string() {
                            (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
                        } else if o_type == OType::FULLTEXT.as_u16() {
                            (DatatypeDictId::FULL_TEXT.as_u16(), 0)
                        } else {
                            // Default string dict values to xsd:string for late materialization.
                            (DatatypeDictId::STRING.as_u16(), 0)
                        };
                        Some(Binding::EncodedLit {
                            o_kind: ObjKind::LEX_ID.as_u8(),
                            o_key,
                            p_id,
                            dt_id,
                            lang_id,
                            i_val: if o_i == u32::MAX {
                                i32::MIN
                            } else {
                                o_i as i32
                            },
                            t,
                        })
                    }
                    fluree_db_core::o_type::DecodeKind::JsonArena => Some(Binding::EncodedLit {
                        o_kind: ObjKind::JSON_ID.as_u8(),
                        o_key,
                        p_id,
                        dt_id: DatatypeDictId::JSON.as_u16(),
                        lang_id: 0,
                        i_val: if o_i == u32::MAX {
                            i32::MIN
                        } else {
                            o_i as i32
                        },
                        t,
                    }),
                    fluree_db_core::o_type::DecodeKind::VectorArena => Some(Binding::EncodedLit {
                        o_kind: ObjKind::VECTOR_ID.as_u8(),
                        o_key,
                        p_id,
                        dt_id: DatatypeDictId::VECTOR.as_u16(),
                        lang_id: 0,
                        i_val: if o_i == u32::MAX {
                            i32::MIN
                        } else {
                            o_i as i32
                        },
                        t,
                    }),
                    fluree_db_core::o_type::DecodeKind::NumBigArena => Some(Binding::EncodedLit {
                        o_kind: ObjKind::NUM_BIG.as_u8(),
                        o_key,
                        p_id,
                        // Best-effort: treat as decimal for late materialization; NUM_BIG identity
                        // relies on (kind,key,dt/lang) and includes p_id in eq/hash when needed.
                        dt_id: DatatypeDictId::DECIMAL.as_u16(),
                        lang_id: 0,
                        i_val: if o_i == u32::MAX {
                            i32::MIN
                        } else {
                            o_i as i32
                        },
                        t,
                    }),
                    _ => None,
                }
            };

        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            let p_id = batch.p_id.get_or(row, 0);
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);
            let o_i = batch.o_i.get_or(row, u32::MAX);
            let t_opt = if batch.t.is_absent() {
                None
            } else {
                Some(batch.t.get(row) as i64)
            };
            let t_enc = t_opt.unwrap_or(0);

            // Skip internal db: predicates on wildcard scans.
            if self.is_internal_predicate(p_id) {
                continue;
            }

            // Encoded pre-filters: run before any decoding work.
            if !self
                .encoded_pre_filters
                .iter()
                .all(|f| f.eval_row(s_id, o_type, o_key))
            {
                continue;
            }

            if !self.within_row_var_equality_ok(s_id, p_id, o_type, o_key)? {
                continue;
            }

            // Enforce datatype constraints before decoding into bindings.
            if !self.matches_datatype_constraint(o_type) {
                continue;
            }

            // Decode object when needed:
            // - object is bound (must filter)
            // - object bounds are present (must filter)
            // - object is emitted but late-materialization is disabled (e.g., overlay)
            let needs_o_decode = self.bound_o.is_some()
                || self.object_bounds.is_some()
                || (!late_materialize && self.o_var_pos.is_some());
            let decode_value = |o_type: u16, o_key: u64, p_id: u32| -> Result<FlakeValue> {
                use fluree_db_core::o_type::{DecodeKind, OType};
                let ot = OType::from_u16(o_type);
                match (ot.decode_kind(), dict_overlay.as_ref()) {
                    (DecodeKind::IriRef, Some(ov)) => {
                        let iri = ov.resolve_subject_iri(o_key).map_err(|e| {
                            QueryError::Internal(format!("resolve_subject_iri: {e}"))
                        })?;
                        Ok(FlakeValue::Ref(store_arc.encode_iri(&iri)))
                    }
                    (DecodeKind::StringDict, Some(ov)) => {
                        let s = ov.resolve_string_value(o_key as u32).map_err(|e| {
                            QueryError::Internal(format!("resolve_string_value: {e}"))
                        })?;
                        Ok(FlakeValue::String(s))
                    }
                    _ => Ok(view
                        .decode_value(o_type, o_key, p_id)
                        .map_err(|e| QueryError::Internal(format!("decode_value_v3: {e}")))?),
                }
            };

            let decoded_o = if needs_o_decode {
                Some(decode_value(o_type, o_key, p_id)?)
            } else {
                None
            };

            if let Some(bound) = &self.bound_o {
                let Some(val) = decoded_o.as_ref() else {
                    return Err(QueryError::Internal(
                        "bound object requires object decoding".to_string(),
                    ));
                };
                if val != bound {
                    continue;
                }
            }

            if let Some(bounds) = &self.object_bounds {
                let Some(val) = decoded_o.as_ref() else {
                    return Err(QueryError::Internal(
                        "object bounds require object decoding".to_string(),
                    ));
                };
                if !bounds.matches(val) {
                    continue;
                }
            }

            bindings.clear();
            bindings.resize(base_len, Binding::Unbound);

            // Subject binding.
            if let Some(pos) = self.s_var_pos {
                let binding = if late_materialize {
                    Binding::EncodedSid { s_id }
                } else {
                    match dict_overlay.as_ref() {
                        Some(ov) => {
                            let iri = ov.resolve_subject_iri(s_id).map_err(|e| {
                                QueryError::Internal(format!("resolve_subject_iri: {e}"))
                            })?;
                            Binding::Sid(store_arc.encode_iri(&iri))
                        }
                        None => Binding::Sid(self.resolve_s_id(s_id)?),
                    }
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Predicate binding.
            if let Some(pos) = self.p_var_pos {
                let binding = if late_materialize {
                    Binding::EncodedPid { p_id }
                } else {
                    Binding::Sid(self.resolve_p_id(p_id))
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Object binding.
            if let Some(pos) = self.o_var_pos {
                let binding = if needs_o_decode || !late_materialize {
                    let val = decoded_o.expect("decoded object required");
                    match &val {
                        FlakeValue::Ref(sid) => Binding::Sid(sid.clone()),
                        _ => {
                            let dtc = match self.store().resolve_lang_tag(o_type).map(Arc::from) {
                                Some(lang) => DatatypeConstraint::LangTag(lang),
                                None => DatatypeConstraint::Explicit(
                                    self.store()
                                        .resolve_datatype_sid(o_type)
                                        .unwrap_or_else(|| Sid::new(0, "")),
                                ),
                            };
                            Binding::Lit {
                                val,
                                dtc,
                                t: t_opt,
                                op: None,
                                p_id: Some(p_id),
                            }
                        }
                    }
                } else {
                    encode_object(o_type, o_key, p_id, t_enc, o_i).unwrap_or_else(|| {
                        // Fallback: decode if we don't have a safe encoded representation.
                        // This preserves correctness for uncommon/custom OTypes.
                        match decode_value(o_type, o_key, p_id) {
                            Ok(FlakeValue::Ref(sid)) => Binding::Sid(sid),
                            Ok(val) => {
                                let dtc = match self.store().resolve_lang_tag(o_type).map(Arc::from)
                                {
                                    Some(lang) => DatatypeConstraint::LangTag(lang),
                                    None => DatatypeConstraint::Explicit(
                                        self.store()
                                            .resolve_datatype_sid(o_type)
                                            .unwrap_or_else(|| Sid::new(0, "")),
                                    ),
                                };
                                Binding::Lit {
                                    val,
                                    dtc,
                                    t: t_opt,
                                    op: None,
                                    p_id: Some(p_id),
                                }
                            }
                            Err(_) => Binding::Unbound,
                        }
                    })
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Apply inline operators.
            if !apply_inline(&self.inline_ops, &self.schema, &mut bindings, ctx)? {
                continue;
            }

            // Push to columns.
            for (i, binding) in bindings.drain(..).enumerate() {
                columns[i].push(binding);
            }
            produced += 1;
        }

        Ok(produced)
    }
}

impl BinaryScanOperator {
    async fn open_overlay_only_fallback(
        &mut self,
        ctx: &ExecutionContext<'_>,
        s_sid: &Option<Sid>,
        p_sid: &Option<Sid>,
    ) -> Result<()> {
        let Some(overlay) = ctx.overlay else {
            self.range_iter = Some(Vec::<RangeFlake>::new().into_iter());
            self.cursor = None;
            self.state = OperatorState::Open;
            return Ok(());
        };

        let to_t = ctx.to_t;
        let from_t = ctx.from_t;
        let cmp = self.index.comparator();

        // Collect all overlay flakes for this graph+index (novelty is expected to be small),
        // then narrow by equality match.
        let mut flakes: Vec<Flake> = Vec::new();
        overlay.for_each_overlay_flake(self.g_id, self.index, None, None, true, to_t, &mut |f| {
            if f.t <= to_t && from_t.is_none_or(|ft| f.t >= ft) {
                flakes.push(f.clone());
            }
        });

        flakes.sort_by(cmp);
        flakes = remove_stale_overlay_flakes(flakes);

        // Apply equality match (subject/predicate/object).
        if s_sid.is_some() || p_sid.is_some() || self.bound_o.is_some() {
            flakes.retain(|f| {
                if let Some(s) = s_sid.as_ref() {
                    if &f.s != s {
                        return false;
                    }
                }
                if let Some(p) = p_sid.as_ref() {
                    if &f.p != p {
                        return false;
                    }
                }
                if let Some(o) = self.bound_o.as_ref() {
                    if &f.o != o {
                        return false;
                    }
                }
                true
            });
        }

        // Apply object bounds (post-filter) when present.
        if let Some(bounds) = self.object_bounds.as_ref() {
            flakes.retain(|f| bounds.matches(&f.o));
        }

        self.range_iter = Some(
            flakes
                .into_iter()
                .map(|flake| RangeFlake {
                    flake,
                    ledger_alias: None,
                })
                .collect::<Vec<_>>()
                .into_iter(),
        );
        self.cursor = None;
        self.state = OperatorState::Open;
        Ok(())
    }
}

fn remove_stale_overlay_flakes(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashSet;

    #[derive(Clone, Copy, Hash, PartialEq, Eq)]
    struct FactKeyRef<'a> {
        s: &'a Sid,
        p: &'a Sid,
        o: &'a FlakeValue,
        dt: &'a Sid,
    }

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut keep = vec![false; flakes.len()];

    for (idx, f) in flakes.iter().enumerate().rev() {
        let key = FactKeyRef {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
        };
        if !seen.insert(key) {
            continue;
        }
        if f.op {
            keep[idx] = true;
        }
    }

    flakes
        .into_iter()
        .zip(keep)
        .filter_map(|(f, k)| k.then_some(f))
        .collect()
}

#[inline]
fn sid_binding(ctx: &ExecutionContext<'_>, sid: &Sid, ledger_alias: Option<&Arc<str>>) -> Binding {
    if ctx.is_multi_ledger() {
        if let Some(alias) = ledger_alias {
            if let Some(iri) = ctx.decode_sid_in_ledger(sid, alias.as_ref()) {
                return Binding::iri_match(
                    Arc::<str>::from(iri.as_str()),
                    sid.clone(),
                    alias.clone(),
                );
            }
        }
    }
    Binding::Sid(sid.clone())
}

fn build_match_val_for_snapshot(
    ctx: &ExecutionContext<'_>,
    snapshot: &fluree_db_core::LedgerSnapshot,
    pattern: &TriplePattern,
) -> Result<RangeMatch> {
    let mut match_val = RangeMatch::new();

    let reencode_sid = |sid: &Sid| -> Option<Sid> {
        // Pattern SIDs are encoded in the primary snapshot's namespace space.
        // Decode to canonical IRI and re-encode into the target snapshot.
        if let Some(iri) = ctx.snapshot.decode_sid(sid) {
            snapshot.encode_iri(&iri)
        } else {
            // If the SID can't be decoded (namespace code missing), preserve the
            // raw SID. This is important when the namespace table has been
            // extended in novelty but the snapshot's namespace map is not yet
            // able to decode the SID. Range scans can still match by raw SID.
            Some(sid.clone())
        }
    };

    match &pattern.s {
        Ref::Sid(s) => match_val.s = reencode_sid(s),
        Ref::Var(_) => {}
        Ref::Iri(iri) => match_val.s = snapshot.encode_iri(iri),
    }

    match &pattern.p {
        Ref::Sid(p) => match_val.p = reencode_sid(p),
        Ref::Var(_) => {}
        Ref::Iri(iri) => match_val.p = snapshot.encode_iri(iri),
    }

    match &pattern.o {
        Term::Sid(o) => match_val.o = reencode_sid(o).map(FlakeValue::Ref),
        Term::Value(v) => match_val.o = Some(v.clone()),
        Term::Var(_) => {}
        Term::Iri(iri) => match_val.o = snapshot.encode_iri(iri).map(FlakeValue::Ref),
    }

    Ok(match_val)
}

/// Pre-compute which repeated-variable equality checks are needed for a pattern.
///
/// Returns `(s==o, s==p, p==o)` flags.
fn repeated_var_flags(pattern: &TriplePattern) -> (bool, bool, bool) {
    let s_var = match &pattern.s {
        Ref::Var(v) => Some(*v),
        _ => None,
    };
    let p_var = match &pattern.p {
        Ref::Var(v) => Some(*v),
        _ => None,
    };
    let o_var = match &pattern.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    (
        s_var.is_some_and(|v| o_var == Some(v)),
        s_var.is_some_and(|v| p_var == Some(v)),
        p_var.is_some_and(|v| o_var == Some(v)),
    )
}

#[async_trait]
impl Operator for BinaryScanOperator {
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

        // Resolve store and g_id from context.
        self.store = ctx.binary_store.clone();
        self.g_id = ctx.binary_g_id;

        if self.store.is_none() {
            return self.open_range_fallback(ctx).await;
        }
        // Policy enforcement requires async per-flake checks (including f:query)
        // and class-cache population. The binary cursor path currently does not
        // apply policy filtering, so force the range fallback when a non-root
        // policy enforcer is present.
        if ctx.policy_enforcer.as_ref().is_some_and(|p| !p.is_root()) {
            return self.open_range_fallback(ctx).await;
        }

        // Pre-compute p_id → Sid table.
        let mut p_sids = Vec::new();
        let store = self.store.as_ref().ok_or_else(|| {
            QueryError::Internal(
                "BinaryScanOperator::open: no binary_store on ExecutionContext".into(),
            )
        })?;
        for p_id in 0u32.. {
            match store.resolve_predicate_iri(p_id) {
                Some(iri) => p_sids.push(store.encode_iri(iri)),
                None => break,
            }
        }
        self.p_sids = p_sids;

        // Extract bound terms and build filter.
        let (s_sid, p_sid, o_val) = self.extract_bound_terms();
        self.bound_o = o_val;
        let mut filter = self
            .build_filter(&s_sid, &p_sid)
            .map_err(|e| QueryError::Internal(format!("build_filter: {e}")))?;
        tracing::debug!(
            ?self.pattern,
            s_bound = s_sid.is_some(),
            p_bound = p_sid.is_some(),
            ?filter.s_id,
            ?filter.p_id,
            "BinaryScanOperator::open"
        );

        // Bound-object fast path: if the triple pattern has a constant object, encode it into
        // (o_type, o_key) so the cursor can seek directly to the relevant leaf range.
        //
        // This is safe for graph pattern matching (RDF term equality is type-aware), and avoids
        // pathological full-predicate scans like `?paper <publishedIn> "SIGIR"`.
        //
        // For overlay/novelty queries, we keep this conservative: if the value isn't present in
        // the persisted dictionaries, fall back to overlay-only to avoid a wide base scan.
        if let Some(bound_o) = self.bound_o.as_ref() {
            let dtc = self.pattern.dtc.as_ref();
            let lang = dtc.and_then(|d| d.lang_tag());
            let dt_sid = dtc.map(|d| d.datatype());
            let dict_novelty = ctx.dict_novelty.as_ref();
            let store_ref = store.as_ref();

            let encoded = match (dt_sid, lang) {
                (Some(dt_sid), lang) => {
                    value_to_otype_okey(bound_o, dt_sid, lang, store_ref, dict_novelty)
                }
                (None, None) => {
                    // Without a datatype constraint, we can only safely encode non-string
                    // values. String values are ambiguous — could be xsd:string or
                    // rdf:langString — so skip them to avoid type mismatch.
                    match bound_o {
                        FlakeValue::String(_) => Err(std::io::Error::other(
                            "string without dtc: type ambiguous (could be langString)",
                        )),
                        _ => value_to_otype_okey_simple(bound_o, store_ref)
                            .map_err(|e| std::io::Error::other(e.to_string())),
                    }
                }
                (None, Some(_lang)) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "lang tag requires datatype constraint",
                )),
            };

            match encoded {
                Ok((ot, key)) => {
                    filter.o_type = Some(ot.as_u16());
                    filter.o_key = Some(key);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Novelty may contain the value, but base index can't; avoid wide base scan.
                    return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
                }
                Err(_) => {
                    // If encoding fails, keep correctness by leaving the filter un-narrowed.
                }
            }
        }

        // If a subject/predicate is bound but not present in the binary dictionaries
        // (common when querying freshly-inserted novelty before the next index build),
        // a binary scan would devolve into a full index scan because the filter can't
        // constrain by IDs.
        //
        // IMPORTANT: RangeProvider-based scans also require dictionary IDs to constrain
        // base index scans. If a SID isn't in the base dictionaries, a range scan can
        // still devolve into a wide base scan. In this case we want an **overlay-only**
        // fallback to return only novelty matches.
        if s_sid.is_some() && filter.s_id.is_none() || p_sid.is_some() && filter.p_id.is_none() {
            return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
        }

        let order = index_type_to_sort_order(self.index);
        // Decode only columns needed for correctness:
        // - `o_i` is part of the V3 identity model (list semantics)
        // - `t` is only required for history mode
        let mut output = ColumnSet::CORE;
        output.insert(ColumnId::OI);
        if ctx.history_mode || inline_ops_need_t(&self.inline_ops) {
            output.insert(ColumnId::T);
        }
        let projection = ColumnProjection {
            output,
            internal: ColumnSet::EMPTY,
        };

        // Get branch manifest (clone into Arc for cursor ownership).
        let store_arc = Arc::clone(self.store.as_ref().expect("store set above"));
        let store_ref = store_arc.as_ref();
        let Some(branch_ref) = store_ref.branch_for_order(self.g_id, order) else {
            // The index root may omit graphs (or specific sort orders) that have
            // zero indexed rows. Queries over such graphs should return empty
            // results (or overlay-only results when novelty is present), not
            // fail with an internal error.
            return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
        };
        let branch: Arc<fluree_db_binary_index::format::branch::BranchManifest> =
            Arc::new(branch_ref.clone());

        // If this scan has range bounds on the object variable and we're scanning in POST order,
        // narrow the cursor's leaf range by object-key range.
        //
        // IMPORTANT: SPARQL numeric comparisons are cross-type (integer bounds match double
        // values), and ObjKey encodings differ between types. For correctness, we only apply
        // range narrowing for temporal types where cross-type comparison does not apply.
        let mut range_min_okey: Option<u64> = None;
        let mut range_max_okey: Option<u64> = None;
        let mut range_o_type: Option<u16> = None;
        if order == RunSortOrder::Post && filter.p_id.is_some() && self.bound_o.is_none() {
            if let Some(bounds) = self.object_bounds.as_ref() {
                let supports_range = |ot: OType| -> bool {
                    matches!(
                        ot,
                        OType::XSD_DATE
                            | OType::XSD_DATE_TIME
                            | OType::XSD_TIME
                            | OType::XSD_G_YEAR
                            | OType::XSD_G_YEAR_MONTH
                            | OType::XSD_G_MONTH
                            | OType::XSD_G_DAY
                            | OType::XSD_G_MONTH_DAY
                    )
                };

                let encode = |v: &FlakeValue| -> Option<(u16, u64)> {
                    let (ot, key) = value_to_otype_okey_simple(v, store_ref).ok()?;
                    supports_range(ot).then_some((ot.as_u16(), key))
                };

                let mut ot: Option<u16> = None;
                if let Some((v, _inclusive)) = bounds.lower.as_ref() {
                    if let Some((o_type, key)) = encode(v) {
                        ot = Some(o_type);
                        range_min_okey = Some(key);
                    }
                }
                if let Some((v, _inclusive)) = bounds.upper.as_ref() {
                    if let Some((o_type, key)) = encode(v) {
                        if ot.is_some() && ot != Some(o_type) {
                            // Mixed type bounds; don't attempt range narrowing.
                            ot = None;
                            range_min_okey = None;
                            range_max_okey = None;
                        } else {
                            ot = Some(o_type);
                            range_max_okey = Some(key);
                        }
                    }
                }

                if let Some(o_type) = ot {
                    range_o_type = Some(o_type);
                    // Also set the filter o_type so directory-level pre-skip can eliminate non-matching leaflets.
                    filter.o_type = Some(o_type);
                }
            }
        }

        // Create cursor. If any of (s_id, p_id, o_type, o_key) are bound OR we have a
        // temporal object-key range (POST + bounds), construct a narrow min/max key range
        // so we can seek into the branch manifest rather than scanning all leaves.
        let use_range = filter.s_id.is_some()
            || filter.p_id.is_some()
            || filter.o_type.is_some()
            || filter.o_key.is_some()
            || range_min_okey.is_some()
            || range_max_okey.is_some();

        let mut cursor = if use_range {
            let min_key = RunRecordV2 {
                s_id: SubjectId(filter.s_id.unwrap_or(0)),
                o_key: filter.o_key.or(range_min_okey).unwrap_or(0),
                p_id: filter.p_id.unwrap_or(0),
                t: 0,
                o_i: 0,
                o_type: filter.o_type.or(range_o_type).unwrap_or(0),
                g_id: self.g_id,
            };
            let max_key = RunRecordV2 {
                s_id: SubjectId(filter.s_id.unwrap_or(u64::MAX)),
                o_key: filter.o_key.or(range_max_okey).unwrap_or(u64::MAX),
                p_id: filter.p_id.unwrap_or(u32::MAX),
                t: u32::MAX,
                o_i: u32::MAX,
                o_type: filter.o_type.or(range_o_type).unwrap_or(u16::MAX),
                g_id: self.g_id,
            };
            BinaryCursor::new(
                Arc::clone(&store_arc),
                order,
                branch,
                &min_key,
                &max_key,
                filter,
                projection,
            )
        } else {
            BinaryCursor::scan_all(Arc::clone(&store_arc), order, branch, filter, projection)
        };

        // Overlay: translate novelty flakes to OverlayOp and attach to cursor.
        if ctx.overlay.is_some() {
            let (mut ops, mut untranslated, ephemeral_preds) =
                translate_overlay_flakes_with_untranslated(
                    ctx.overlay(),
                    store_ref,
                    ctx.dict_novelty.as_ref(),
                    ctx.to_t,
                    self.g_id,
                );

            // Extend p_sids table with novelty-only predicates so that ephemeral
            // p_ids from overlay ops can be decoded back to Sids during row binding.
            for (iri, ep_id) in &ephemeral_preds {
                let idx = *ep_id as usize;
                if idx >= self.p_sids.len() {
                    self.p_sids.resize(idx + 1, Sid::new(0, ""));
                }
                self.p_sids[idx] = store_ref.encode_iri(iri);
            }

            if !ops.is_empty() {
                sort_overlay_ops(&mut ops, order);
                let epoch = ctx.overlay().epoch();
                cursor.set_overlay_ops(ops);
                cursor.set_epoch(epoch);
            }

            // Some overlay flakes cannot be represented in V3 overlay ops (e.g., @vector).
            // Keep them as materialized flakes and stream them after the cursor completes.
            if !untranslated.is_empty() {
                let cmp = self.index.comparator();
                untranslated.sort_by(cmp);
                untranslated = remove_stale_overlay_flakes(untranslated);

                // Apply equality match (subject/predicate/object) against pattern constants.
                let s_sid = match &self.pattern.s {
                    Ref::Sid(s) => Some(s.clone()),
                    _ => None,
                };
                let p_sid = match &self.pattern.p {
                    Ref::Sid(p) => Some(p.clone()),
                    _ => None,
                };

                if s_sid.is_some() || p_sid.is_some() || self.bound_o.is_some() {
                    untranslated.retain(|f| {
                        if let Some(s) = s_sid.as_ref() {
                            if &f.s != s {
                                return false;
                            }
                        }
                        if let Some(p) = p_sid.as_ref() {
                            if &f.p != p {
                                return false;
                            }
                        }
                        if let Some(o) = self.bound_o.as_ref() {
                            if &f.o != o {
                                return false;
                            }
                        }
                        true
                    });
                }

                if let Some(bounds) = self.object_bounds.as_ref() {
                    untranslated.retain(|f| bounds.matches(&f.o));
                }

                if !untranslated.is_empty() {
                    self.range_iter = Some(
                        untranslated
                            .into_iter()
                            .map(|flake| RangeFlake {
                                flake,
                                ledger_alias: None,
                            })
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                }
            }
        }
        cursor.set_to_t(ctx.to_t);

        self.cursor = Some(cursor);
        self.state = OperatorState::Open;

        // Compile pre-filters that can run on encoded columns (no decoding).
        let (encoded, pruned) = compile_encoded_pre_filters_and_prune_inline_ops(
            &self.inline_ops,
            &self.pattern,
            store_ref,
        );
        self.encoded_pre_filters = encoded;
        self.inline_ops = pruned;

        tracing::debug!(
            index = ?self.index,
            order = ?order,
            g_id = self.g_id,
            pattern = ?self.pattern,
            "BinaryScanOperator::open"
        );

        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let batch_size = ctx.batch_size;
        let num_vars = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_vars)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();

        let mut produced = 0usize;

        // Prefer binary cursor (indexed data), then drain any overlay-only fallback flakes.
        while produced < batch_size {
            let Some(cursor) = &mut self.cursor else {
                break;
            };

            match cursor.next_batch() {
                Ok(Some(batch)) => {
                    let n = self.batch_to_bindings(&batch, &mut columns, Some(ctx))?;
                    for _ in 0..n {
                        ctx.tracker.consume_fuel_one()?;
                    }
                    produced += n;
                }
                Ok(None) => {
                    // Cursor exhausted — drop it so we can proceed to `range_iter`.
                    self.cursor = None;
                    break;
                }
                Err(e) => {
                    return Err(QueryError::Internal(format!("V3 cursor: {}", e)));
                }
            }
        }

        if produced < batch_size && self.range_iter.is_some() {
            let n = self.flakes_to_bindings(&mut columns, ctx, batch_size - produced)?;
            for _ in 0..n {
                ctx.tracker.consume_fuel_one()?;
            }
            produced += n;
        }

        self.finalize_columns(columns, produced)
    }

    fn close(&mut self) {
        self.cursor = None;
        self.range_iter = None;
        self.store = None;
        self.sid_cache.clear();
        self.p_sids.clear();
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// Overlay translation: Flake → OverlayOp
// ============================================================================

/// Translate overlay flakes to V3 integer-ID space.
///
/// Uses the V6 store for persisted dictionary lookups and DictNovelty for
/// ephemeral IDs from uncommitted transactions.
pub fn translate_overlay_flakes(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    to_t: i64,
    g_id: GraphId,
) -> Vec<OverlayOp> {
    let mut ops = Vec::new();
    let mut ephemeral_preds: HashMap<String, u32> = HashMap::new();
    let mut next_ephemeral_p_id = store.predicate_count();

    overlay.for_each_overlay_flake(
        g_id,
        fluree_db_core::IndexType::Spot,
        None,
        None,
        true,
        to_t,
        &mut |flake| match translate_one_flake_v3_pub(
            flake,
            store,
            dict_novelty,
            &mut ephemeral_preds,
            &mut next_ephemeral_p_id,
        ) {
            Ok(op) => ops.push(op),
            Err(e) => {
                tracing::warn!(error = %e, "failed to translate overlay flake to V3");
            }
        },
    );

    ops
}

/// Translate overlay flakes to V3 overlay ops, also returning flakes that cannot be translated
/// and the mapping of novelty-only predicate IRIs to ephemeral p_ids.
///
/// Some FlakeValue variants (notably `FlakeValue::Vector`) are not representable in the V3
/// overlay encoding. Those flakes are returned as fully materialized overlay-only rows so the
/// query engine can still see them (after the indexed cursor is exhausted).
///
/// The `ephemeral_preds` map contains predicate IRI → ephemeral p_id for predicates that
/// don't exist in the persisted index dictionary. Callers must use this to extend their
/// p_id → Sid lookup tables so that novelty-only predicates can be resolved during decode.
fn translate_overlay_flakes_with_untranslated(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    to_t: i64,
    g_id: GraphId,
) -> (Vec<OverlayOp>, Vec<Flake>, HashMap<String, u32>) {
    let mut ops = Vec::new();
    let mut untranslated = Vec::new();
    let mut ephemeral_preds: HashMap<String, u32> = HashMap::new();
    let mut next_ephemeral_p_id = store.predicate_count();

    overlay.for_each_overlay_flake(
        g_id,
        fluree_db_core::IndexType::Spot,
        None,
        None,
        true,
        to_t,
        &mut |flake| match translate_one_flake_v3_pub(
            flake,
            store,
            dict_novelty,
            &mut ephemeral_preds,
            &mut next_ephemeral_p_id,
        ) {
            Ok(op) => ops.push(op),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::Unsupported {
                    untranslated.push(flake.clone());
                } else {
                    tracing::warn!(error = %e, "failed to translate overlay flake to V3");
                }
            }
        },
    );

    (ops, untranslated, ephemeral_preds)
}

/// Translate a single Flake to an OverlayOp.
///
/// `pub(crate)` so `binary_range` can reuse it for overlay translation.
pub(crate) fn translate_one_flake_v3_pub(
    flake: &fluree_db_core::Flake,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    ephemeral_preds: &mut HashMap<String, u32>,
    next_ephemeral_p_id: &mut u32,
) -> std::io::Result<OverlayOp> {
    // Subject: persisted → DictNovelty → error
    let s_id = resolve_subject_v3(&flake.s, store, dict_novelty)?;

    // Predicate: persisted → ephemeral
    let p_iri = store.sid_to_iri(&flake.p);
    let p_id = resolve_predicate_v3(&p_iri, store, ephemeral_preds, next_ephemeral_p_id);

    // Object value → (o_type, o_key), using flake.dt + lang for proper OType.
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    let (o_type, o_key) = value_to_otype_okey(&flake.o, &flake.dt, lang, store, dict_novelty)?;

    // List index
    let o_i = flake
        .m
        .as_ref()
        .and_then(|m| m.i)
        .map(|i| i as u32)
        .unwrap_or(u32::MAX);

    Ok(OverlayOp {
        s_id,
        p_id,
        o_type: o_type.as_u16(),
        o_key,
        o_i,
        t: flake.t,
        op: flake.op,
    })
}

/// Resolve a predicate IRI to p_id.
///
/// Predicates are not tracked in DictNovelty (they're per-query ephemeral in V5).
/// For V3 overlay, novel predicates get an ephemeral p_id above the persisted count.
/// These ephemeral IDs won't match any persisted data (correct: the predicate
/// is novelty-only), but will match overlay ops that use the same ID.
fn resolve_predicate_v3(
    iri: &str,
    store: &BinaryIndexStore,
    ephemeral_preds: &mut HashMap<String, u32>,
    next_ephemeral_p_id: &mut u32,
) -> u32 {
    if let Some(id) = store.find_predicate_id(iri) {
        return id;
    }
    // Ephemeral allocation for novel predicates.
    *ephemeral_preds.entry(iri.to_string()).or_insert_with(|| {
        let id = *next_ephemeral_p_id;
        *next_ephemeral_p_id += 1;
        id
    })
}

/// Resolve a subject Sid to s_id using persisted dict then DictNovelty.
fn resolve_subject_v3(
    sid: &Sid,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<u64> {
    // 1. Persisted
    if let Some(id) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name)? {
        return Ok(id);
    }
    // 2. DictNovelty
    if let Some(dn) = dict_novelty {
        if dn.is_initialized() {
            if let Some(id) = dn.subjects.find_subject(sid.namespace_code, &sid.name) {
                return Ok(id);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "subject not found in persisted or novelty dict: ns={} name={}",
            sid.namespace_code, sid.name
        ),
    ))
}

/// Resolve a string value to a string_id using persisted dict then DictNovelty.
fn resolve_string_v3(
    value: &str,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<u32> {
    // 1. Persisted
    if let Some(id) = store.find_string_id(value)? {
        return Ok(id);
    }
    // 2. DictNovelty
    if let Some(dn) = dict_novelty {
        if dn.is_initialized() {
            if let Some(id) = dn.strings.find_string(value) {
                return Ok(id);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "string not found in dict: {}",
            &value[..value.len().min(50)]
        ),
    ))
}

/// Convert a FlakeValue to `(OType, o_key)` in V3 encoding.
///
/// Uses `dt_sid` (the flake's datatype Sid) and `lang` (from FlakeMeta) to derive
/// the correct OType, rather than inferring purely from the FlakeValue variant.
/// This is critical for:
/// - langString: OType must embed the lang_id, not use XSD_STRING
/// - numeric subtypes: xsd:int vs xsd:integer can share the same FlakeValue::Long
/// - string subtypes: xsd:anyURI vs xsd:string share FlakeValue::String
fn value_to_otype_okey(
    val: &FlakeValue,
    dt_sid: &Sid,
    lang: Option<&str>,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<(OType, u64)> {
    // If the value has a language tag, it's rdf:langString — encode lang_id into OType.
    if let Some(lang_tag) = lang {
        let str_id = resolve_string_v3(
            match val {
                FlakeValue::String(s) => s,
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "langString value must be FlakeValue::String",
                    ))
                }
            },
            store,
            dict_novelty,
        )?;
        let lang_id = store.resolve_lang_id(lang_tag).unwrap_or_else(|| {
            tracing::warn!(
                tag = lang_tag,
                "language tag not found in persisted dict, using 1"
            );
            1
        });
        return Ok((OType::lang_string(lang_id), str_id as u64));
    }

    // For value types that are dt-dependent (Long, Double, String), resolve
    // the exact OType from the datatype Sid IRI. For value types with 1:1
    // OType mapping (Bool, Date, Ref, etc.), the FlakeValue variant suffices.
    let dt_otype = otype_from_dt_sid(dt_sid, store);

    match val {
        FlakeValue::Null => Ok((OType::NULL, 0)),
        FlakeValue::Boolean(b) => Ok((OType::XSD_BOOLEAN, *b as u64)),
        FlakeValue::Long(n) => {
            // Use dt-derived OType for integer subtypes (xsd:int, xsd:short, etc.)
            let ot = dt_otype.unwrap_or(OType::XSD_INTEGER);
            Ok((ot, ObjKey::encode_i64(*n).as_u64()))
        }
        FlakeValue::Double(d) => {
            if d.is_finite() && d.fract() == 0.0 {
                let as_i64 = *d as i64;
                if (as_i64 as f64) == *d {
                    let ot = dt_otype.unwrap_or(OType::XSD_INTEGER);
                    return Ok((ot, ObjKey::encode_i64(as_i64).as_u64()));
                }
            }
            if d.is_finite() {
                let ot = dt_otype.unwrap_or(OType::XSD_DOUBLE);
                match ObjKey::encode_f64(*d) {
                    Ok(key) => Ok((ot, key.as_u64())),
                    Err(_) => Ok((OType::NULL, 0)),
                }
            } else {
                Ok((OType::NULL, 0))
            }
        }
        FlakeValue::Ref(sid) => {
            let s_id = resolve_subject_v3(sid, store, dict_novelty)?;
            Ok((OType::IRI_REF, s_id))
        }
        FlakeValue::String(s) => {
            let str_id = resolve_string_v3(s, store, dict_novelty)?;
            // Use dt-derived OType for string subtypes (xsd:anyURI, xsd:token, etc.)
            let ot = dt_otype.unwrap_or(OType::XSD_STRING);
            Ok((ot, str_id as u64))
        }
        FlakeValue::Json(s) => {
            let str_id = resolve_string_v3(s, store, dict_novelty)?;
            Ok((OType::RDF_JSON, str_id as u64))
        }
        FlakeValue::Date(d) => {
            let days = d.days_since_epoch();
            Ok((OType::XSD_DATE, ObjKey::encode_date(days).as_u64()))
        }
        FlakeValue::DateTime(dt) => {
            let micros = dt.epoch_micros();
            Ok((
                OType::XSD_DATE_TIME,
                ObjKey::encode_datetime(micros).as_u64(),
            ))
        }
        FlakeValue::Time(t) => {
            let micros = t.micros_since_midnight();
            Ok((OType::XSD_TIME, ObjKey::encode_time(micros).as_u64()))
        }
        FlakeValue::GYear(g) => Ok((OType::XSD_G_YEAR, ObjKey::encode_g_year(g.year()).as_u64())),
        FlakeValue::GYearMonth(g) => Ok((
            OType::XSD_G_YEAR_MONTH,
            ObjKey::encode_g_year_month(g.year(), g.month()).as_u64(),
        )),
        FlakeValue::GMonth(g) => Ok((
            OType::XSD_G_MONTH,
            ObjKey::encode_g_month(g.month()).as_u64(),
        )),
        FlakeValue::GDay(g) => Ok((OType::XSD_G_DAY, ObjKey::encode_g_day(g.day()).as_u64())),
        FlakeValue::GMonthDay(g) => Ok((
            OType::XSD_G_MONTH_DAY,
            ObjKey::encode_g_month_day(g.month(), g.day()).as_u64(),
        )),
        FlakeValue::YearMonthDuration(d) => Ok((
            OType::XSD_YEAR_MONTH_DURATION,
            ObjKey::encode_year_month_dur(d.months()).as_u64(),
        )),
        FlakeValue::DayTimeDuration(d) => Ok((
            OType::XSD_DAY_TIME_DURATION,
            ObjKey::encode_day_time_dur(d.micros()).as_u64(),
        )),
        FlakeValue::GeoPoint(bits) => Ok((OType::GEO_POINT, bits.0)),
        // Types not yet handled: BigInt, Decimal, Vector, Duration
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("unsupported FlakeValue variant for V3 overlay: {:?}", val),
        )),
    }
}

/// Resolve a datatype Sid to its OType constant.
///
/// Reconstructs the IRI from the Sid and matches against well-known XSD types.
/// Returns `None` for unrecognized datatypes (caller uses FlakeValue-inferred default).
fn otype_from_dt_sid(dt_sid: &Sid, store: &BinaryIndexStore) -> Option<OType> {
    let iri = store.sid_to_iri(dt_sid);
    fluree_db_core::o_type_registry::resolve_iri_to_otype_option(&iri)
}

/// Simplified FlakeValue → (OType, o_key) translation for fast-path operators.
///
/// Uses default OType for each value variant (no dt_sid/lang context needed).
/// Works for the common cases in bound-object count queries. Does not handle
/// langString (no language tag available) or custom datatypes.
pub(crate) fn value_to_otype_okey_simple(
    val: &FlakeValue,
    store: &BinaryIndexStore,
) -> Result<(OType, u64)> {
    use fluree_db_core::value_id::ObjKey;

    match val {
        FlakeValue::Null => Ok((OType::NULL, 0)),
        FlakeValue::Boolean(b) => Ok((OType::XSD_BOOLEAN, *b as u64)),
        FlakeValue::Long(n) => Ok((OType::XSD_INTEGER, ObjKey::encode_i64(*n).as_u64())),
        FlakeValue::Double(d) => {
            if d.is_finite() {
                ObjKey::encode_f64(*d)
                    .map(|key| (OType::XSD_DOUBLE, key.as_u64()))
                    .map_err(|_| {
                        QueryError::execution("cannot encode f64 for V6 index".to_string())
                    })
            } else {
                Err(QueryError::execution(
                    "non-finite double in bound object".to_string(),
                ))
            }
        }
        FlakeValue::Ref(sid) => {
            let s_id = store
                .sid_to_s_id(sid)
                .map_err(|e| QueryError::execution(format!("sid_to_s_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("ref object not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF, s_id))
        }
        FlakeValue::String(s) => {
            let str_id = store
                .find_string_id(s)
                .map_err(|e| QueryError::execution(format!("find_string_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("string value not found in V6 dict".to_string())
                })?;
            Ok((OType::XSD_STRING, str_id as u64))
        }
        FlakeValue::Json(s) => {
            let str_id = store
                .find_string_id(s)
                .map_err(|e| QueryError::execution(format!("find_string_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("JSON value not found in V6 dict".to_string())
                })?;
            Ok((OType::RDF_JSON, str_id as u64))
        }
        FlakeValue::Date(d) => Ok((
            OType::XSD_DATE,
            ObjKey::encode_date(d.days_since_epoch()).as_u64(),
        )),
        FlakeValue::DateTime(dt) => Ok((
            OType::XSD_DATE_TIME,
            ObjKey::encode_datetime(dt.epoch_micros()).as_u64(),
        )),
        FlakeValue::Time(t) => Ok((
            OType::XSD_TIME,
            ObjKey::encode_time(t.micros_since_midnight()).as_u64(),
        )),
        FlakeValue::GYear(g) => Ok((OType::XSD_G_YEAR, ObjKey::encode_g_year(g.year()).as_u64())),
        FlakeValue::GYearMonth(g) => Ok((
            OType::XSD_G_YEAR_MONTH,
            ObjKey::encode_g_year_month(g.year(), g.month()).as_u64(),
        )),
        FlakeValue::GMonth(g) => Ok((
            OType::XSD_G_MONTH,
            ObjKey::encode_g_month(g.month()).as_u64(),
        )),
        FlakeValue::GDay(g) => Ok((OType::XSD_G_DAY, ObjKey::encode_g_day(g.day()).as_u64())),
        FlakeValue::GMonthDay(g) => Ok((
            OType::XSD_G_MONTH_DAY,
            ObjKey::encode_g_month_day(g.month(), g.day()).as_u64(),
        )),
        _ => Err(QueryError::execution(format!(
            "unsupported FlakeValue variant for V6 fast-path: {:?}",
            val
        ))),
    }
}
