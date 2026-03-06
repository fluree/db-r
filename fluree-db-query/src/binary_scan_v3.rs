//! V3 binary scan operator — eagerly materializes `ColumnBatch` rows into `Binding` values.
//!
//! This is the V3 counterpart of `BinaryScanOperator` (V5). Key differences:
//! - Uses `BinaryCursorV3` (leaflet-at-a-time columnar batches)
//! - Uses `o_type` for value dispatch (replaces ObjKind/dt_id/lang_id)
//! - Eagerly materializes all values (no EncodedLit/EncodedSid)
//! - No overlay merge (import-only for initial milestone)
//!
//! The eager approach trades some allocation for simplicity. Deferred decoding
//! (EncodedLitV3) can be added in a follow-up when overlay and perf are needed.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::read::types_v3::{sort_overlay_ops_v3, OverlayOpV3};
use fluree_db_binary_index::{
    BinaryCursorV3, BinaryFilterV3, BinaryGraphViewV3, BinaryIndexStoreV6, ColumnBatch,
    ColumnProjection,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::{
    dt_compatible, FlakeValue, GraphId, IndexType, ObjectBounds, OverlayProvider, Sid,
};

use crate::binary_scan::{index_type_to_sort_order, schema_from_pattern_with_emit, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{Operator, OperatorState};
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;

// ============================================================================
// BinaryScanOperatorV3
// ============================================================================

/// V3 scan operator: streams leaflets from `BinaryCursorV3`, eagerly decoding
/// `ColumnBatch` rows into `Binding::Sid` / `Binding::Lit` values.
pub(crate) struct BinaryScanOperatorV3 {
    pattern: TriplePattern,
    index: IndexType,
    schema: Arc<[VarId]>,
    s_var_pos: Option<usize>,
    p_var_pos: Option<usize>,
    o_var_pos: Option<usize>,
    state: OperatorState,
    store: Arc<BinaryIndexStoreV6>,
    g_id: GraphId,
    cursor: Option<BinaryCursorV3>,
    /// Pre-computed p_id → Sid (all predicates, done once at open).
    p_sids: Vec<Sid>,
    /// Cached s_id → Sid for amortized IRI resolution.
    sid_cache: HashMap<u64, Sid>,
    /// Whether predicate is a variable (for internal predicate filtering).
    p_is_var: bool,
    inline_ops: Vec<InlineOperator>,
    // Kept for: future V3 projection push-down (Phase 7) and plan-time index override.
    // Use when: V3 scan supports selective projection and index hints.
    #[expect(dead_code)]
    emit: EmitMask,
    #[expect(dead_code)]
    index_hint: Option<IndexType>,
    // Kept for: V3 range query support (object bounds on POST order).
    // Use when: V3 cursor supports bounded range scans.
    object_bounds: Option<ObjectBounds>,
    /// Bound object value, if the triple pattern's object is a constant.
    ///
    /// Used to filter scan results when `?o` is not a variable.
    bound_o: Option<FlakeValue>,
    /// Pre-computed repeated-variable flags from the triple pattern.
    ///
    /// When all are false (the common case), within-row equality checks short-circuit.
    check_s_eq_o: bool,
    check_s_eq_p: bool,
    check_p_eq_o: bool,
}

impl BinaryScanOperatorV3 {
    pub(crate) fn new(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStoreV6>,
        g_id: GraphId,
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
            store,
            g_id,
            cursor: None,
            p_sids: Vec::new(),
            sid_cache: HashMap::new(),
            p_is_var,
            inline_ops,
            emit,
            index_hint,
            object_bounds,
            bound_o: None,
            check_s_eq_o,
            check_s_eq_p,
            check_p_eq_o,
        }
    }

    /// Base schema length (max var position + 1).
    fn base_schema_len(&self) -> usize {
        [self.s_var_pos, self.p_var_pos, self.o_var_pos]
            .into_iter()
            .flatten()
            .max()
            .map_or(0, |m| m + 1)
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
            Ref::Iri(iri) => Some(self.store.encode_iri(iri)),
            _ => None,
        };
        let p_sid = match &self.pattern.p {
            Ref::Sid(s) => Some(self.re_encode_sid(s)),
            Ref::Iri(iri) => Some(self.store.encode_iri(iri)),
            _ => None,
        };
        let o_val = match &self.pattern.o {
            Term::Sid(sid) => Some(FlakeValue::Ref(self.re_encode_sid(sid))),
            Term::Iri(iri) => Some(FlakeValue::Ref(self.store.encode_iri(iri))),
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
        let iri = self.store.sid_to_iri(sid);
        self.store.encode_iri(&iri)
    }

    /// Build a `BinaryFilterV3` from bound pattern terms.
    fn build_filter(
        &self,
        s_sid: &Option<Sid>,
        p_sid: &Option<Sid>,
    ) -> std::io::Result<BinaryFilterV3> {
        let s_id = if let Some(sid) = s_sid {
            self.store.sid_to_s_id(sid)?
        } else {
            None
        };
        let p_id = p_sid.as_ref().and_then(|sid| self.store.sid_to_p_id(sid));

        Ok(BinaryFilterV3 {
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
            .store
            .resolve_subject_iri(s_id)
            .map_err(|e| QueryError::Internal(format!("resolve s_id={s_id}: {e}")))?;
        let sid = self.store.encode_iri(&iri);
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

        let Some(dt_sid) = self.store.resolve_datatype_sid(o_type) else {
            return false;
        };
        if !dt_compatible(dtc.datatype(), &dt_sid) {
            return false;
        }

        if let Some(tag) = dtc.lang_tag() {
            self.store.resolve_lang_tag(o_type) == Some(tag)
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
        let view = BinaryGraphViewV3::new(Arc::clone(&self.store), self.g_id);

        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            let p_id = batch.p_id.get_or(row, 0);
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);
            let t = batch.t.get_or(row, 0) as i64;

            // Skip internal db: predicates on wildcard scans.
            if self.is_internal_predicate(p_id) {
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
            // - object is a variable (must emit)
            // - object is bound (must filter)
            // - object bounds are present (must filter)
            let needs_o_decode =
                self.o_var_pos.is_some() || self.bound_o.is_some() || self.object_bounds.is_some();
            let decoded_o = if needs_o_decode {
                Some(
                    view.decode_value(o_type, o_key, p_id)
                        .map_err(|e| QueryError::Internal(format!("decode_value_v3: {e}")))?,
                )
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
                let sid = self.resolve_s_id(s_id)?;
                if !Self::set_binding_at(&mut bindings, pos, Binding::Sid(sid)) {
                    continue;
                }
            }

            // Predicate binding.
            if let Some(pos) = self.p_var_pos {
                let sid = self.resolve_p_id(p_id);
                if !Self::set_binding_at(&mut bindings, pos, Binding::Sid(sid)) {
                    continue;
                }
            }

            // Object binding.
            if let Some(pos) = self.o_var_pos {
                let val = decoded_o.expect("o_var_pos implies decoded_o");
                let binding = match &val {
                    FlakeValue::Ref(sid) => Binding::Sid(sid.clone()),
                    _ => {
                        let dt = self
                            .store
                            .resolve_datatype_sid(o_type)
                            .unwrap_or_else(|| Sid::new(0, ""));
                        let lang = self.store.resolve_lang_tag(o_type).map(Arc::from);
                        Binding::Lit {
                            val,
                            dt,
                            lang,
                            t: Some(t),
                            op: None,
                            p_id: Some(p_id),
                        }
                    }
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
impl Operator for BinaryScanOperatorV3 {
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

        // Pre-compute p_id → Sid table.
        let mut p_sids = Vec::new();
        for p_id in 0u32.. {
            match self.store.resolve_predicate_iri(p_id) {
                Some(iri) => p_sids.push(self.store.encode_iri(iri)),
                None => break,
            }
        }
        self.p_sids = p_sids;

        // Extract bound terms and build filter.
        let (s_sid, p_sid, o_val) = self.extract_bound_terms();
        self.bound_o = o_val;
        let filter = self
            .build_filter(&s_sid, &p_sid)
            .map_err(|e| QueryError::Internal(format!("build_filter: {e}")))?;

        let order = index_type_to_sort_order(self.index);
        let projection = ColumnProjection::all();

        // Get branch manifest (clone into Arc for cursor ownership).
        let branch_ref = self
            .store
            .branch_for_order(self.g_id, order)
            .ok_or_else(|| {
                QueryError::Internal(format!(
                    "no V3 branch for g_id={}, order={:?}",
                    self.g_id, order
                ))
            })?;
        let branch: Arc<fluree_db_binary_index::format::branch_v3::BranchManifestV3> =
            Arc::new(branch_ref.clone());

        // Create cursor: full scan with filter.
        let mut cursor =
            BinaryCursorV3::scan_all(Arc::clone(&self.store), order, branch, filter, projection);

        // Overlay: translate novelty flakes to OverlayOpV3 and attach to cursor.
        if ctx.overlay.is_some() {
            let mut ops = translate_overlay_flakes_v3(
                ctx.overlay(),
                &self.store,
                ctx.dict_novelty.as_ref(),
                ctx.to_t,
                self.g_id,
            );
            if !ops.is_empty() {
                sort_overlay_ops_v3(&mut ops, order);
                let epoch = ctx.overlay().epoch();
                cursor.set_overlay_ops(ops);
                cursor.set_epoch(epoch);
            }
        }
        cursor.set_to_t(ctx.to_t);

        self.cursor = Some(cursor);
        self.state = OperatorState::Open;

        tracing::debug!(
            index = ?self.index,
            order = ?order,
            g_id = self.g_id,
            pattern = ?self.pattern,
            "BinaryScanOperatorV3::open"
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

        while produced < batch_size {
            let cursor = match &mut self.cursor {
                Some(c) => c,
                None => break,
            };

            match cursor.next_batch() {
                Ok(Some(batch)) => {
                    let n = self.batch_to_bindings(&batch, &mut columns, Some(ctx))?;
                    for _ in 0..n {
                        ctx.tracker.consume_fuel_one()?;
                    }
                    produced += n;
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(QueryError::Internal(format!("V3 cursor: {}", e)));
                }
            }
        }

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        if self.schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }

        let batch = Batch::new(self.schema.clone(), columns)?;
        Ok(Some(batch))
    }

    fn close(&mut self) {
        self.cursor = None;
        self.sid_cache.clear();
        self.p_sids.clear();
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// Overlay translation: Flake → OverlayOpV3
// ============================================================================

/// Translate overlay flakes to V3 integer-ID space.
///
/// Uses the V6 store for persisted dictionary lookups and DictNovelty for
/// ephemeral IDs from uncommitted transactions.
fn translate_overlay_flakes_v3(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStoreV6,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    to_t: i64,
    g_id: GraphId,
) -> Vec<OverlayOpV3> {
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

/// Translate a single Flake to an OverlayOpV3.
///
/// `pub(crate)` so `binary_range_v3` can reuse it for overlay translation.
pub(crate) fn translate_one_flake_v3_pub(
    flake: &fluree_db_core::Flake,
    store: &BinaryIndexStoreV6,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    ephemeral_preds: &mut HashMap<String, u32>,
    next_ephemeral_p_id: &mut u32,
) -> std::io::Result<OverlayOpV3> {
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

    Ok(OverlayOpV3 {
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
    store: &BinaryIndexStoreV6,
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
    store: &BinaryIndexStoreV6,
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
    store: &BinaryIndexStoreV6,
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
    store: &BinaryIndexStoreV6,
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
fn otype_from_dt_sid(dt_sid: &Sid, store: &BinaryIndexStoreV6) -> Option<OType> {
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
    store: &BinaryIndexStoreV6,
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
        _ => Err(QueryError::execution(format!(
            "unsupported FlakeValue variant for V6 fast-path: {:?}",
            val
        ))),
    }
}
