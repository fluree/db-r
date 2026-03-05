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
use fluree_db_binary_index::{
    BinaryCursorV3, BinaryFilterV3, BinaryGraphViewV3, BinaryIndexStoreV6, ColumnBatch,
    ColumnProjection,
};
use fluree_db_core::{dt_compatible, FlakeValue, GraphId, IndexType, ObjectBounds, Sid};

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

    /// Extract bound Sids from the pattern.
    fn extract_bound_terms(&self) -> (Option<Sid>, Option<Sid>, Option<FlakeValue>) {
        let s_sid = match &self.pattern.s {
            Ref::Sid(s) => Some(s.clone()),
            Ref::Iri(iri) => Some(self.store.encode_iri(iri)),
            _ => None,
        };
        let p_sid = match &self.pattern.p {
            Ref::Sid(s) => Some(s.clone()),
            Ref::Iri(iri) => Some(self.store.encode_iri(iri)),
            _ => None,
        };
        let o_val = match &self.pattern.o {
            Term::Sid(sid) => Some(FlakeValue::Ref(sid.clone())),
            Term::Iri(iri) => Some(FlakeValue::Ref(self.store.encode_iri(iri))),
            Term::Value(v) => Some(v.clone()),
            Term::Var(_) => None,
        };
        (s_sid, p_sid, o_val)
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

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
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
        let cursor =
            BinaryCursorV3::scan_all(Arc::clone(&self.store), order, branch, filter, projection);
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
