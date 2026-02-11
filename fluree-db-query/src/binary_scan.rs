//! Scan operators for WHERE-clause triple-pattern evaluation.
//!
//! `ScanOperator` is the entry point — it selects between `BinaryScanOperator`
//! (fast streaming cursors) and `RangeScanOperator` (range_with_overlay fallback).
//! Works entirely in integer-ID space (s_id, p_id, ObjKind/ObjKey) and only resolves
//! to Sid/FlakeValue at the Binding output boundary.
//!
//! # Integer-ID Pipeline
//!
//! 1. Pattern's bound terms → integer IDs (s_id, p_id, ObjKind/ObjKey) via `BinaryIndexStore`
//! 2. `BinaryCursor` iterates leaves using integer comparators and columnar filters
//! 3. `DecodedBatch` rows converted to `Binding` columns:
//!    - s_id → Sid (cached in `sid_cache`)
//!    - p_id → Sid (pre-computed in `p_sids`)
//!    - o (ObjKind, ObjKey) → FlakeValue via `store.decode_value()`
//!    - dt_id → Sid via `store.dt_sids()`
//!
//! # Design Principles
//!
//! - **Sync local-file reads** via `BinaryCursor` (no async Storage needed)
//! - **Columnar integer filtering** in the cursor (no Flake conversion in hot path)
//! - **Pre-computed dictionaries** for fast ID→Sid conversion
//! - **Subject Sid cache** for amortized IRI resolution
//! - **Overlay merge** — translates Flake overlay ops to integer-ID space
//!   and merges with decoded leaflet columns at query time

use crate::binding::{Batch, Binding, BindingRow};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::ListIndex;
use fluree_db_core::{
    dt_compatible, range_with_overlay, Db, Flake, FlakeValue, IndexType, ObjectBounds,
    OverlayProvider, RangeMatch, RangeOptions, RangeTest, Sid,
};
use fluree_db_indexer::run_index::numfloat_dict::NumericShape;
use fluree_db_indexer::run_index::run_record::RunSortOrder;
use fluree_db_indexer::run_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryIndexStore, DecodedBatch, OverlayOp,
};
use fluree_vocab::namespaces::FLUREE_DB;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::policy::QueryPolicyEnforcer;

// ============================================================================
// IndexType → RunSortOrder mapping
// ============================================================================

/// Map from fluree-db-core's `IndexType` to `RunSortOrder`.
///
pub(crate) fn index_type_to_sort_order(idx: IndexType) -> RunSortOrder {
    match idx {
        IndexType::Spot => RunSortOrder::Spot,
        IndexType::Psot => RunSortOrder::Psot,
        IndexType::Post => RunSortOrder::Post,
        IndexType::Opst => RunSortOrder::Opst,
    }
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Build output schema and variable positions from a triple pattern.
///
/// Returns `(schema, s_var_pos, p_var_pos, o_var_pos)` where each position
/// is the column index of that variable in the schema (None if bound).
fn schema_from_pattern(
    pattern: &TriplePattern,
) -> (Arc<[VarId]>, Option<usize>, Option<usize>, Option<usize>) {
    let mut schema_vec = Vec::with_capacity(3);
    let mut s_var_pos = None;
    let mut p_var_pos = None;
    let mut o_var_pos = None;

    if let Term::Var(v) = &pattern.s {
        s_var_pos = Some(schema_vec.len());
        schema_vec.push(*v);
    }
    if let Term::Var(v) = &pattern.p {
        p_var_pos = Some(schema_vec.len());
        schema_vec.push(*v);
    }
    if let Term::Var(v) = &pattern.o {
        o_var_pos = Some(schema_vec.len());
        schema_vec.push(*v);
    }

    (
        Arc::from(schema_vec.into_boxed_slice()),
        s_var_pos,
        p_var_pos,
        o_var_pos,
    )
}

// ============================================================================
// BinaryScanOperator
// ============================================================================

/// Scan operator backed by binary columnar indexes.
///
/// Uses `BinaryCursor` to iterate leaf files in the selected sort order,
/// decoding columnar integer IDs into `Binding` values at the output boundary.
///
/// # When to Use
///
/// Used by `ScanOperator` when a `BinaryIndexStore` is available and the query
/// mode is compatible (single-ledger, non-history, time within index coverage).
/// Otherwise `ScanOperator` falls back to `RangeScanOperator`.
///
/// # Single-Ledger Only
///
/// This operator is designed for single-ledger mode (no cross-ledger joins).
/// It creates `Binding::Sid` for IRI positions. Multi-ledger `IriMatch` bindings
/// are not yet supported.
pub struct BinaryScanOperator {
    /// The triple pattern to match.
    pattern: TriplePattern,
    /// Selected index type (SPOT, PSOT, POST, OPST).
    index: IndexType,
    /// Output schema (variables from pattern).
    schema: Arc<[VarId]>,
    /// Position of s variable in schema (None if s is bound).
    s_var_pos: Option<usize>,
    /// Position of p variable in schema (None if p is bound).
    p_var_pos: Option<usize>,
    /// Position of o variable in schema (None if o is bound).
    o_var_pos: Option<usize>,
    /// Operator lifecycle state.
    state: OperatorState,
    /// The binary index store (shared, immutable).
    store: Arc<BinaryIndexStore>,
    /// Graph ID to query.
    g_id: u32,
    /// Cursor for leaf iteration (created during open).
    cursor: Option<BinaryCursor>,
    /// Pre-computed p_id → Sid (all predicates, done once at open).
    p_sids: Vec<Sid>,
    /// Cached s_id → Sid for amortized IRI resolution.
    sid_cache: HashMap<u64, Sid>,
    /// Whether predicate is a variable (for internal predicate filtering).
    p_is_var: bool,
    /// Bound object value for post-filtering when the value cannot be translated
    /// to an (ObjKind, ObjKey) pair (e.g., string literals without a reverse index).
    /// When set, `batch_to_bindings` filters rows whose decoded object != this value.
    bound_o_filter: Option<FlakeValue>,
    /// Object bounds for range post-filtering (set when binary path handles range queries).
    object_bounds: Option<ObjectBounds>,
    /// Pre-translated overlay operations (set by ScanOperator).
    overlay_ops: Vec<OverlayOp>,
    /// Overlay epoch for cache key differentiation.
    overlay_epoch: u64,
    /// Dictionary overlay for decode-time ephemeral ID resolution.
    ///
    /// When set, all ID→value decoding goes through DictOverlay instead of
    /// directly through the store. This handles ephemeral subjects, predicates,
    /// strings, and lang tags that are in novelty but not yet in the persisted
    /// binary index.
    dict_overlay: Option<crate::dict_overlay::DictOverlay>,
    /// Filter expressions to evaluate during batch processing.
    /// Applied per-row before adding to output columns.
    filters: Vec<crate::ir::Expression>,
}

impl BinaryScanOperator {
    /// Create a new BinaryScanOperator for a triple pattern.
    ///
    /// The `store` must be loaded with indexes for the relevant sort orders.
    /// The `g_id` selects which graph to scan (typically 0 for default graph).
    /// Filters are evaluated per-row before adding bindings to output columns.
    pub fn new(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        object_bounds: Option<ObjectBounds>,
        filters: Vec<crate::ir::Expression>,
    ) -> Self {
        let mut index = IndexType::for_query(
            pattern.s_bound(),
            pattern.p_bound(),
            pattern.o_bound(),
            pattern.o_is_ref(),
        );

        // When object bounds are present and default index is PSOT, switch to POST
        // for object-range scanning.
        if object_bounds.is_some() && index == IndexType::Psot {
            index = IndexType::Post;
        }

        let (schema, s_var_pos, p_var_pos, o_var_pos) = schema_from_pattern(&pattern);
        let p_is_var = matches!(pattern.p, Term::Var(_));

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
            bound_o_filter: None,
            object_bounds,
            overlay_ops: Vec::new(),
            overlay_epoch: 0,
            dict_overlay: None,
            filters,
        }
    }

    /// Create with explicit index selection (for testing or forced order).
    pub fn with_index(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        index: IndexType,
        filters: Vec<crate::ir::Expression>,
    ) -> Self {
        let mut op = Self::new(pattern, store, g_id, None, filters);
        op.index = index;
        op
    }

    /// Get the index type being used.
    pub fn index_type(&self) -> IndexType {
        self.index
    }

    /// Set pre-translated overlay operations and epoch for query-time merge.
    ///
    /// Called by `ScanOperator` after translating overlay Flakes to
    /// integer-ID space. The ops are sorted by the cursor's sort order during
    /// `open()` and passed to the `BinaryCursor` for per-leaf merge.
    pub fn set_overlay(&mut self, ops: Vec<OverlayOp>, epoch: u64) {
        self.overlay_ops = ops;
        self.overlay_epoch = epoch;
    }

    /// Set the dictionary overlay for ephemeral ID resolution at decode time.
    ///
    /// Must be the same DictOverlay that was used for `translate_overlay_flakes()`
    /// so ephemeral IDs in overlay ops resolve correctly.
    pub fn set_dict_overlay(&mut self, overlay: crate::dict_overlay::DictOverlay) {
        self.dict_overlay = Some(overlay);
    }

    /// Resolve s_id to Sid, using cache for amortized lookups.
    ///
    /// When a DictOverlay is present, delegates to it (handles both persisted
    /// and ephemeral subject IDs). Otherwise, uses the store directly.
    ///
    /// Note: Used for early materialization of ephemeral subjects from novelty.
    fn resolve_s_id(&mut self, s_id: u64) -> Result<Sid> {
        if let Some(sid) = self.sid_cache.get(&s_id) {
            return Ok(sid.clone());
        }

        let sid = if let Some(dict_ov) = &self.dict_overlay {
            dict_ov
                .resolve_subject_sid(s_id)
                .map_err(|e| QueryError::Internal(format!("resolve s_id {}: {}", s_id, e)))?
        } else {
            let iri = self
                .store
                .resolve_subject_iri(s_id)
                .map_err(|e| QueryError::Internal(format!("resolve s_id {}: {}", s_id, e)))?;
            self.store.encode_iri(&iri)
        };
        self.sid_cache.insert(s_id, sid.clone());
        Ok(sid)
    }

    /// Check if a predicate p_id corresponds to a fluree:ledger internal predicate.
    ///
    /// Internal predicates are filtered out when the pattern's predicate is a variable
    /// (wildcard) to skip internal predicates.
    #[inline]
    fn is_internal_predicate(&self, p_id: u32) -> bool {
        self.p_is_var
            && self
                .p_sids
                .get(p_id as usize)
                .is_some_and(|s| s.namespace_code == FLUREE_DB)
    }

    /// Decode an object value, routing through DictOverlay when present.
    #[inline]
    fn decode_obj(&self, o_kind: u8, o_key: u64, p_id: u32) -> std::io::Result<FlakeValue> {
        match &self.dict_overlay {
            Some(ov) => ov.decode_value(o_kind, o_key, p_id),
            None => self.store.decode_value(o_kind, o_key, p_id),
        }
    }

    /// Check if a subject ID is ephemeral (from novelty overlay).
    #[inline]
    fn is_ephemeral_subject(&self, s_id: u64) -> bool {
        self.dict_overlay
            .as_ref()
            .is_some_and(|ov| ov.is_ephemeral_subject(s_id))
    }

    /// Check if a predicate ID is ephemeral (from novelty overlay).
    #[inline]
    fn is_ephemeral_predicate(&self, p_id: u32) -> bool {
        self.dict_overlay
            .as_ref()
            .is_some_and(|ov| ov.is_ephemeral_predicate(p_id))
    }

    /// Resolve a predicate ID to Sid, handling ephemeral IDs via DictOverlay.
    ///
    /// Note: Used for early materialization of ephemeral predicates from novelty.
    #[inline]
    fn resolve_p_id(&self, p_id: u32) -> Sid {
        let idx = p_id as usize;
        if idx < self.p_sids.len() {
            return self.p_sids[idx].clone();
        }
        // Ephemeral or out-of-range p_id: try DictOverlay, then store
        if let Some(ov) = &self.dict_overlay {
            if let Some(sid) = ov.resolve_predicate_sid(p_id) {
                return sid;
            }
        }
        match self.store.resolve_predicate_iri(p_id) {
            Some(iri) => self.store.encode_iri(iri),
            None => {
                tracing::warn!(p_id, "unresolvable predicate ID in batch_to_bindings");
                Sid::new(0, "")
            }
        }
    }

    // ========================================================================
    // Row filtering helpers
    // ========================================================================

    /// Check if a row should be skipped based on all active filters.
    ///
    /// Returns `Ok(true)` if the row should be filtered out.
    #[inline]
    fn should_skip_row(&self, decoded: &DecodedBatch, row: usize) -> Result<bool> {
        let skip = self.is_internal_predicate(decoded.p_ids[row])
            || !self.matches_bound_object(decoded, row)?
            || !self.matches_object_bounds(decoded, row)?;
        Ok(skip)
    }

    /// Check if a row's object matches the bound object filter.
    ///
    /// Returns `Ok(true)` if there's no filter or if the object matches.
    /// Used for post-filtering when an object value couldn't be translated to an
    /// (ObjKind, ObjKey) pair (e.g. string literal — no reverse string index).
    #[inline]
    fn matches_bound_object(&self, decoded: &DecodedBatch, row: usize) -> Result<bool> {
        let Some(filter_val) = &self.bound_o_filter else {
            return Ok(true);
        };
        let decoded_val = self
            .decode_obj(
                decoded.o_kinds[row],
                decoded.o_keys[row],
                decoded.p_ids[row],
            )
            .map_err(|e| QueryError::Internal(format!("decode object for filter: {}", e)))?;
        Ok(decoded_val == *filter_val)
    }

    /// Check if a row's object matches the ObjectBounds range filter.
    ///
    /// Returns `Ok(true)` if there are no bounds or if the object is within bounds.
    /// POST key range provides coarse leaf filtering; this handles
    /// inclusive/exclusive boundaries and cross-type comparisons exactly.
    #[inline]
    fn matches_object_bounds(&self, decoded: &DecodedBatch, row: usize) -> Result<bool> {
        let Some(obj_bounds) = &self.object_bounds else {
            return Ok(true);
        };
        let decoded_val = self
            .decode_obj(
                decoded.o_kinds[row],
                decoded.o_keys[row],
                decoded.p_ids[row],
            )
            .map_err(|e| QueryError::Internal(format!("decode object for bounds: {}", e)))?;
        Ok(obj_bounds.matches(&decoded_val))
    }

    // ========================================================================
    // Subject binding helpers
    // ========================================================================

    /// Build a subject binding if the subject is a variable.
    ///
    /// Emits `EncodedSid` for late materialization, or `Sid` for ephemeral subjects.
    /// Returns `None` if the subject position is bound (not a variable).
    #[inline]
    fn build_subject_binding(&mut self, s_id: u64) -> Result<Option<Binding>> {
        if self.s_var_pos.is_none() {
            return Ok(None);
        }

        let is_ephemeral = self.is_ephemeral_subject(s_id);

        tracing::trace!(
            s_id,
            is_ephemeral,
            has_dict_overlay = self.dict_overlay.is_some(),
            "binary_scan building subject binding"
        );

        let binding = if is_ephemeral {
            // Ephemeral subject: resolve now while dict_overlay is available
            let sid = self.resolve_s_id(s_id)?;
            tracing::trace!(
                ?sid,
                s_id,
                "binary_scan early-materializing ephemeral subject"
            );
            Binding::Sid(sid)
        } else {
            tracing::trace!(s_id, "binary_scan emitting EncodedSid for subject");
            Binding::EncodedSid { s_id }
        };

        Ok(Some(binding))
    }

    // ========================================================================
    // Predicate binding helpers
    // ========================================================================

    /// Build a predicate binding if the predicate is a variable.
    ///
    /// Emits `EncodedPid` for late materialization, or `Sid` for ephemeral predicates.
    /// Returns `None` if the predicate position is bound (not a variable).
    #[inline]
    fn build_predicate_binding(&self, p_id: u32) -> Option<Binding> {
        self.p_var_pos?;

        let binding = if self.is_ephemeral_predicate(p_id) {
            // Ephemeral predicate: resolve now while dict_overlay is available
            let sid = self.resolve_p_id(p_id);
            tracing::trace!(
                ?sid,
                p_id,
                "binary_scan early-materializing ephemeral predicate"
            );
            Binding::Sid(sid)
        } else {
            tracing::trace!(p_id, "binary_scan emitting EncodedPid for predicate");
            Binding::EncodedPid { p_id }
        };

        Some(binding)
    }

    // ========================================================================
    // Object binding helpers
    // ========================================================================

    /// Build an object binding if the object is a variable.
    ///
    /// Handles three cases:
    /// - Ref objects: `EncodedSid` for late materialization
    /// - Ephemeral values: immediate materialization to `Sid` or `Lit`
    /// - Literals: `EncodedLit` for late materialization
    ///
    /// Returns `None` if the object position is bound (not a variable).
    #[inline]
    fn build_object_binding(&self, decoded: &DecodedBatch, row: usize) -> Result<Option<Binding>> {
        if self.o_var_pos.is_none() {
            return Ok(None);
        }

        let o_kind = decoded.o_kinds[row];
        let o_key = decoded.o_keys[row];
        let p_id = decoded.p_ids[row];

        // Check if early materialization is needed (ephemeral IDs from novelty)
        let needs_early = self
            .dict_overlay
            .as_ref()
            .map(|ov| ov.needs_early_materialize(o_kind, o_key))
            .unwrap_or(false);

        let binding = if o_kind == ObjKind::REF_ID.as_u8() && !needs_early {
            // Ref object: use EncodedSid for late materialization
            tracing::trace!(o_key, "binary_scan emitting EncodedSid for ref object");
            Binding::EncodedSid { s_id: o_key }
        } else if needs_early {
            self.build_early_materialized_object(decoded, row)?
        } else {
            // Literal: use EncodedLit (avoids string dictionary lookups)
            // This is the main late materialization win - literal strings
            // are only decoded when needed for FILTER/ORDER BY/output.
            Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id: decoded.dt_values[row] as u16,
                lang_id: decoded.lang_ids[row],
                i_val: decoded.i_values[row],
                t: decoded.t_values[row],
            }
        };

        Ok(Some(binding))
    }

    /// Build an early-materialized object binding for ephemeral values.
    ///
    /// Called when the object contains ephemeral IDs from novelty that must
    /// be resolved immediately while the dict_overlay is available.
    #[inline]
    fn build_early_materialized_object(
        &self,
        decoded: &DecodedBatch,
        row: usize,
    ) -> Result<Binding> {
        let o_kind = decoded.o_kinds[row];
        let o_key = decoded.o_keys[row];
        let p_id = decoded.p_ids[row];
        let dt_id = decoded.dt_values[row] as u16;
        let lang_id = decoded.lang_ids[row];
        let i_val = decoded.i_values[row];
        let t = decoded.t_values[row];

        let val = self
            .decode_obj(o_kind, o_key, p_id)
            .map_err(|e| QueryError::Internal(format!("early materialize: {}", e)))?;

        let dt_sid = self.resolve_dt_sid(dt_id);
        let lang = self.resolve_lang(lang_id, i_val);

        tracing::trace!(?val, "binary_scan early-materializing ephemeral value");

        let binding = match val {
            FlakeValue::Ref(sid) => Binding::Sid(sid),
            other => Binding::Lit {
                val: other,
                dt: dt_sid,
                lang,
                t: Some(t),
                op: None,
            },
        };

        Ok(binding)
    }

    /// Resolve a datatype ID to its Sid representation.
    #[inline]
    fn resolve_dt_sid(&self, dt_id: u16) -> Sid {
        self.dict_overlay
            .as_ref()
            .map(|ov| ov.decode_dt_sid(dt_id))
            .unwrap_or_else(|| {
                self.store
                    .dt_sids()
                    .get(dt_id as usize)
                    .cloned()
                    .unwrap_or_else(|| Sid::new(0, ""))
            })
    }

    /// Resolve a language tag from the lang_id and i_val metadata.
    #[inline]
    fn resolve_lang(&self, lang_id: u16, i_val: i32) -> Option<Arc<str>> {
        self.dict_overlay
            .as_ref()
            .and_then(|ov| ov.decode_meta(lang_id, i_val))
            .and_then(|m| m.lang.map(Arc::from))
    }

    // ========================================================================
    // Main batch processing
    // ========================================================================

    /// Convert a DecodedBatch into columnar Bindings.
    ///
    /// Processes all rows in the batch, converting integer IDs to Binding values.
    /// Only produces columns for variable positions in the schema.
    /// Filters out internal predicates when predicate is a variable.
    ///
    /// When inline filters are present, each row's bindings are evaluated
    /// against filters and only added to columns if all pass.
    fn batch_to_bindings(
        &mut self,
        decoded: &DecodedBatch,
        columns: &mut [Vec<Binding>],
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<usize> {
        let mut produced = 0;
        let has_filters = !self.filters.is_empty();

        for row in 0..decoded.row_count {
            if self.should_skip_row(decoded, row)? {
                continue;
            }

            // Build bindings into stack array (at most 3: s, p, o)
            let mut bindings: [Binding; 3] = [Binding::Unbound, Binding::Unbound, Binding::Unbound];
            let mut count = 0;

            if let Some(b) = self.build_subject_binding(decoded.s_ids[row])? {
                bindings[count] = b;
                count += 1;
            }
            if let Some(b) = self.build_predicate_binding(decoded.p_ids[row]) {
                bindings[count] = b;
                count += 1;
            }
            if let Some(b) = self.build_object_binding(decoded, row)? {
                bindings[count] = b;
                count += 1;
            }

            // Evaluate filters if present
            if has_filters {
                let binding_row = BindingRow::new(&self.schema, &bindings[..count]);
                let passes = self
                    .filters
                    .iter()
                    .all(|expr| expr.eval_to_bool::<_>(&binding_row, ctx).unwrap_or(false));

                if !passes {
                    continue;
                }
            }

            // Push to columns
            for i in 0..count {
                columns[i].push(std::mem::replace(&mut bindings[i], Binding::Unbound));
            }

            produced += 1;
        }

        Ok(produced)
    }

    /// Extract bound Sids from the pattern, handling Term::Iri by encoding through the store.
    ///
    /// Returns (s_sid, p_sid, o_val) as owned optionals for use with translate_range.
    fn extract_bound_terms(&self) -> (Option<Sid>, Option<Sid>, Option<FlakeValue>) {
        let s_sid = match &self.pattern.s {
            Term::Sid(s) => Some(s.clone()),
            Term::Iri(iri) => Some(self.store.encode_iri(iri)),
            _ => None,
        };

        let p_sid = match &self.pattern.p {
            Term::Sid(s) => Some(s.clone()),
            Term::Iri(iri) => Some(self.store.encode_iri(iri)),
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
}

/// Derive per-predicate numeric shape from real DB stats (no run-dir files).
///
/// Uses graph-scoped property datatype counts from `IndexStats.graphs`.
/// Returns `None` when stats are unavailable or the predicate is not present.
fn numeric_shape_from_db_stats(
    ctx: &ExecutionContext<'_>,
    _pred_iri: &str,
    pred_sid_binary: &Sid,
) -> Option<NumericShape> {
    let stats = ctx.db.stats.as_ref()?;

    // Prefer graph-scoped stats when available (authoritative ID-based view).
    // (Not always present yet; many deployments only have class-property stats.)
    if let Some(graphs) = stats.graphs.as_ref() {
        let g = graphs.iter().find(|g| g.g_id == ctx.binary_g_id)?;
        let p_id = ctx
            .binary_store
            .as_ref()
            .and_then(|s| s.sid_to_p_id(pred_sid_binary))?;
        if let Some(p) = g.properties.iter().find(|p| p.p_id == p_id) {
            let mut has_int = false;
            let mut has_float = false;
            let mut has_decimal = false;
            for &(dt_raw, count) in &p.datatypes {
                if count == 0 {
                    continue;
                }
                let dt = ValueTypeTag::from_u8(dt_raw);
                if dt.is_integer_type() {
                    has_int = true;
                } else if dt.is_float_type() {
                    has_float = true;
                } else if dt == ValueTypeTag::DECIMAL {
                    has_decimal = true;
                }
            }
            if has_decimal {
                return None;
            }
            return match (has_int, has_float) {
                (true, false) => Some(NumericShape::IntOnly),
                (false, true) => Some(NumericShape::FloatOnly),
                (true, true) => Some(NumericShape::Mixed),
                (false, false) => None,
            };
        }
    }

    // No fallback: class-property stats no longer carry datatype breakdowns.
    None
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

        // Pre-compute p_sids for all predicates (typically ~96 for DBLP)
        let pred_count = self.store.predicate_count();
        self.p_sids = (0..pred_count)
            .map(|p_id| match self.store.resolve_predicate_iri(p_id) {
                Some(iri) => self.store.encode_iri(iri),
                None => Sid::new(0, ""),
            })
            .collect();

        // Extract bound terms from the pattern
        let (s_sid, p_sid, o_val) = self.extract_bound_terms();

        tracing::trace!(
            ?s_sid,
            ?p_sid,
            overlay_ops = self.overlay_ops.len(),
            "BinaryScanOperator::open: bound terms extracted"
        );

        let order = index_type_to_sort_order(self.index);

        // Translate to RunRecord bounds
        let bounds = self
            .store
            .translate_range(
                s_sid.as_ref(),
                p_sid.as_ref(),
                o_val.as_ref(),
                order,
                self.g_id,
            )
            .map_err(|e| QueryError::Internal(format!("translate_range: {}", e)))?;

        tracing::trace!(
            has_bounds = bounds.is_some(),
            "BinaryScanOperator::open: translate_range"
        );

        // If translate_range returned None but we had an object value,
        // the value may be untranslatable (e.g., string literal with no
        // reverse index). Retry without the object bound and enable
        // post-filtering in batch_to_bindings instead.
        let bounds = match bounds {
            some @ Some(_) => some,
            None => {
                if o_val.is_some() {
                    let retry = self
                        .store
                        .translate_range(s_sid.as_ref(), p_sid.as_ref(), None, order, self.g_id)
                        .map_err(|e| QueryError::Internal(format!("translate_range: {}", e)))?;
                    if retry.is_some() {
                        self.bound_o_filter = o_val.clone();
                        tracing::debug!("object value untranslatable, using post-filter scan");
                    }
                    retry
                } else {
                    None
                }
            }
        };

        // If bounds is still None but we have overlay ops and a bound subject,
        // the subject may only exist in the overlay (e.g., novelty subject not
        // yet indexed). Try DictOverlay for subject translation and create
        // bounds for the overlay-only subject.
        tracing::trace!(
            bounds_present = bounds.is_some(),
            overlay_ops_len = self.overlay_ops.len(),
            s_sid = ?s_sid,
            "binary_scan: checking overlay-only subject fallback"
        );
        let bounds = match bounds {
            some @ Some(_) => some,
            None if !self.overlay_ops.is_empty() && s_sid.is_some() => {
                tracing::trace!(?s_sid, "binary_scan: trying overlay-only subject fallback");
                if let Some(dict_ov) = &mut self.dict_overlay {
                    tracing::trace!("binary_scan: have dict_overlay for overlay-only fallback");
                    if let Some(s) = &s_sid {
                        // Try to get s_id from DictOverlay (handles ephemeral subjects)
                        match dict_ov.assign_subject_id_from_sid(s) {
                            Ok(s_id) => {
                                tracing::trace!(s_id, sid = ?s, "binary_scan: got s_id for overlay-only subject");
                                // Create bounds for this specific subject
                                use fluree_db_core::subject_id::SubjectId;
                                use fluree_db_indexer::run_index::RunRecord;
                                let p_id = p_sid.as_ref().and_then(|p| self.store.sid_to_p_id(p));

                                let (min_o_kind, min_o_key, max_o_kind, max_o_key) =
                                    if let Some(val) = &o_val {
                                        if let Ok(Some((ok, okey))) =
                                            self.store.value_to_obj_pair(val)
                                        {
                                            (ok.as_u8(), okey.as_u64(), ok.as_u8(), okey.as_u64())
                                        } else {
                                            self.bound_o_filter = o_val.clone();
                                            (
                                                ObjKind::MIN.as_u8(),
                                                0u64,
                                                ObjKind::MAX.as_u8(),
                                                u64::MAX,
                                            )
                                        }
                                    } else {
                                        (ObjKind::MIN.as_u8(), 0u64, ObjKind::MAX.as_u8(), u64::MAX)
                                    };

                                let min_key = RunRecord {
                                    g_id: self.g_id,
                                    s_id: SubjectId::from_u64(s_id),
                                    p_id: p_id.unwrap_or(0),
                                    dt: 0,
                                    o_kind: min_o_kind,
                                    op: 0,
                                    o_key: min_o_key,
                                    t: i64::MIN,
                                    lang_id: 0,
                                    i: ListIndex::none().as_i32(),
                                };
                                let max_key = RunRecord {
                                    g_id: self.g_id,
                                    s_id: SubjectId::from_u64(s_id),
                                    p_id: p_id.unwrap_or(u32::MAX),
                                    dt: u16::MAX,
                                    o_kind: max_o_kind,
                                    op: 1,
                                    o_key: max_o_key,
                                    t: i64::MAX,
                                    lang_id: u16::MAX,
                                    i: i32::MAX,
                                };
                                tracing::trace!(s_id, "binary_scan: created overlay-only bounds");
                                Some((min_key, max_key))
                            }
                            Err(e) => {
                                tracing::trace!(error = %e, "binary_scan: assign_subject_id_from_sid failed");
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    tracing::trace!(
                        "binary_scan: no dict_overlay available for overlay-only fallback"
                    );
                    None
                }
            }
            None => None,
        };

        match bounds {
            Some((mut min_key, mut max_key)) => {
                // Narrow key range with ObjectBounds when available.
                // Requires a bound predicate (need p_id for shape lookup and POST order).
                if let Some(obj_bounds) = &self.object_bounds {
                    if let Some(p) = &p_sid {
                        if let Some(p_id) = self.store.sid_to_p_id(p) {
                            let shape = {
                                let pred_iri_for_stats = self.store.sid_to_iri(p);
                                numeric_shape_from_db_stats(ctx, &pred_iri_for_stats, p)
                            };
                            let mut narrowed: Option<(ObjKind, ObjKey, ObjKind, ObjKey)> = None;
                            if let Some(shape) = shape {
                                match self.store.translate_object_bounds(obj_bounds, p_id, shape) {
                                    Some((min_ok, min_okey, max_ok, max_okey)) => {
                                        narrowed = Some((min_ok, min_okey, max_ok, max_okey));
                                        min_key.o_kind = min_ok.as_u8();
                                        min_key.o_key = min_okey.as_u64();
                                        max_key.o_kind = max_ok.as_u8();
                                        max_key.o_key = max_okey.as_u64();
                                    }
                                    None => {
                                        // Cannot safely narrow (e.g., Mixed numeric predicates or
                                        // bounds that don't translate cleanly). Fall back to
                                        // post-filtering with ObjectBounds after decoding.
                                        //
                                        // IMPORTANT: `translate_object_bounds` returning `None`
                                        // does NOT necessarily mean "empty range" in the current
                                        // API — it can also mean "no safe narrowing".
                                    }
                                }
                            }

                            // One-per-scan debug line to verify pushdown + float-only narrowing.
                            // This should be low volume: it only triggers when ObjectBounds exist.
                            let pred_iri = self.store.sid_to_iri(p);
                            let lower = obj_bounds
                                .lower
                                .as_ref()
                                .map(|(v, inc)| (format!("{:?}", v), *inc));
                            let upper = obj_bounds
                                .upper
                                .as_ref()
                                .map(|(v, inc)| (format!("{:?}", v), *inc));
                            let stats_graphs_len = ctx
                                .db
                                .stats
                                .as_ref()
                                .and_then(|s| s.graphs.as_ref())
                                .map(|g| g.len())
                                .unwrap_or(0);
                            let stats_classes_len = ctx
                                .db
                                .stats
                                .as_ref()
                                .and_then(|s| s.classes.as_ref())
                                .map(|c| c.len())
                                .unwrap_or(0);
                            match (shape, narrowed) {
                                (Some(shape), Some((min_ok, _, max_ok, _))) => {
                                    tracing::debug!(
                                        predicate = %pred_iri,
                                        p_id,
                                        index = ?self.index,
                                        shape = ?shape,
                                        narrowed_min_kind = %min_ok,
                                        narrowed_max_kind = %max_ok,
                                        lower = ?lower,
                                        upper = ?upper,
                                        stats_graphs = stats_graphs_len,
                                        stats_classes = stats_classes_len,
                                        "binary scan: object bounds pushed down (narrowed key range)"
                                    );
                                }
                                (Some(shape), None) => {
                                    tracing::debug!(
                                        predicate = %pred_iri,
                                        p_id,
                                        index = ?self.index,
                                        shape = ?shape,
                                        lower = ?lower,
                                        upper = ?upper,
                                        stats_graphs = stats_graphs_len,
                                        stats_classes = stats_classes_len,
                                        "binary scan: object bounds pushed down (no safe narrowing; post-filter)"
                                    );
                                }
                                (None, _) => {
                                    tracing::debug!(
                                        predicate = %pred_iri,
                                        p_id,
                                        index = ?self.index,
                                        lower = ?lower,
                                        upper = ?upper,
                                        stats_graphs = stats_graphs_len,
                                        stats_classes = stats_classes_len,
                                        "binary scan: object bounds pushed down (no stats-derived numeric shape; post-filter)"
                                    );
                                }
                            }
                        }
                    }
                }

                // Build BinaryFilter from bound terms
                let mut filter = BinaryFilter::new();

                if let Some(sid) = &s_sid {
                    // First try persisted index
                    if let Ok(Some(s_id)) = self.store.sid_to_s_id(sid) {
                        filter.s_id = Some(s_id);
                    } else if let Some(dict_ov) = &mut self.dict_overlay {
                        // Fallback to DictOverlay for novelty-only subjects
                        if let Ok(s_id) = dict_ov.assign_subject_id_from_sid(sid) {
                            filter.s_id = Some(s_id);
                        }
                    }
                }
                let resolved_p_id = p_sid.as_ref().and_then(|sid| self.store.sid_to_p_id(sid));
                if let Some(p_id) = resolved_p_id {
                    filter.p_id = Some(p_id);
                }
                if let Some(val) = &o_val {
                    let pair_result = if let Some(p_id) = resolved_p_id {
                        self.store.value_to_obj_pair_for_predicate(val, p_id)
                    } else {
                        self.store.value_to_obj_pair(val)
                    };
                    if let Ok(Some((ok, okey))) = pair_result {
                        filter.o_kind = Some(ok.as_u8());
                        filter.o_key = Some(okey.as_u64());
                    }
                }

                // Use optimized subject lookup for SPOT with bound subject
                let need_region2 = self.o_var_pos.is_some();
                let cursor = if order == RunSortOrder::Spot {
                    if let Some(s_id) = filter.s_id {
                        BinaryCursor::for_subject(
                            self.store.clone(),
                            self.g_id,
                            s_id,
                            filter.p_id,
                            need_region2,
                        )
                    } else {
                        BinaryCursor::new(
                            self.store.clone(),
                            order,
                            self.g_id,
                            &min_key,
                            &max_key,
                            filter,
                            need_region2,
                        )
                    }
                } else {
                    BinaryCursor::new(
                        self.store.clone(),
                        order,
                        self.g_id,
                        &min_key,
                        &max_key,
                        filter,
                        need_region2,
                    )
                };

                // Propagate time-travel target and overlay state to the cursor.
                let mut cursor = cursor;
                cursor.set_to_t(ctx.to_t);

                // Always propagate overlay epoch for cache key correctness.
                if self.overlay_epoch > 0 {
                    cursor.set_epoch(self.overlay_epoch);
                }

                // Sort and pass pre-translated overlay ops for query-time merge.
                if !self.overlay_ops.is_empty() {
                    sort_overlay_ops(&mut self.overlay_ops, order);
                    cursor.set_overlay_ops(std::mem::take(&mut self.overlay_ops));
                }

                self.cursor = Some(cursor);
                self.state = OperatorState::Open;
            }
            None => {
                // No results possible (lookup failed or OPST guard)
                self.state = OperatorState::Exhausted;
            }
        }

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
        let mut leaves_scanned = 0usize;
        let scan_start = std::time::Instant::now();

        // Pull decoded batches from cursor until we fill a batch or exhaust cursor
        while produced < batch_size {
            let cursor = match &mut self.cursor {
                Some(c) => c,
                None => break,
            };

            match cursor.next_leaf() {
                Ok(Some(decoded)) => {
                    leaves_scanned += 1;
                    let n = self.batch_to_bindings(&decoded, &mut columns, Some(ctx))?;
                    for _ in 0..n {
                        ctx.tracker.consume_fuel_one()?;
                    }
                    produced += n;
                }
                Ok(None) => {
                    // Cursor exhausted
                    break;
                }
                Err(e) => {
                    return Err(QueryError::Internal(format!("binary cursor: {}", e)));
                }
            }
        }

        let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;
        if produced > 0 {
            tracing::info!(
                leaves_scanned,
                rows = produced,
                scan_ms = format!("{:.2}", scan_ms),
                sid_cache_size = self.sid_cache.len(),
                index = ?self.index,
                "binary_scan batch"
            );
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
        self.bound_o_filter = None;
        self.dict_overlay = None;
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// Overlay Flake → OverlayOp translation
// ============================================================================

/// Translate overlay flakes from Sid/FlakeValue space to integer-ID space.
///
/// Uses `DictOverlay` to allocate ephemeral IDs for subjects, predicates,
/// strings, and language tags not yet in the persisted binary dictionaries.
/// This makes overlay translation infallible — every flake is always
/// translatable.
///
/// Flakes are filtered by `g_id`:
/// - `g_id == 0`: only flakes in the default graph (`flake.g.is_none()`)
/// - `g_id > 0`: only flakes in the named graph matching `g_id`
pub(crate) fn translate_overlay_flakes(
    overlay: &dyn OverlayProvider,
    dict_overlay: &mut crate::dict_overlay::DictOverlay,
    to_t: i64,
    g_id: u32,
) -> Vec<OverlayOp> {
    let mut ops = Vec::new();
    let mut io_error: Option<std::io::Error> = None;

    tracing::trace!(
        epoch = overlay.epoch(),
        to_t,
        g_id,
        "translate_overlay_flakes: starting"
    );

    // Collect all overlay flakes (no boundary filtering — the cursor
    // partitions per-leaf internally via find_overlay_for_leaf).
    overlay.for_each_overlay_flake(IndexType::Spot, None, None, true, to_t, &mut |flake| {
        // Filter by graph ID
        let flake_g_id = match &flake.g {
            None => 0, // default graph
            Some(g_sid) => {
                // Reconstruct the graph IRI from Sid and look up the g_id
                let graph_iri = dict_overlay.store().sid_to_iri(g_sid);
                dict_overlay
                    .store()
                    .graph_id_for_iri(&graph_iri)
                    .unwrap_or(u32::MAX) // Unknown graph → never match
            }
        };
        if flake_g_id != g_id {
            return; // Skip flakes from other graphs
        }

        tracing::trace!(
            s = ?flake.s,
            p = ?flake.p,
            o = ?flake.o,
            t = flake.t,
            op = ?flake.op,
            g_id = flake_g_id,
            "translate_overlay_flakes: processing flake"
        );
        if io_error.is_some() {
            return;
        }
        match translate_one_flake(flake, dict_overlay) {
            Ok(op) => ops.push(op),
            Err(e) => {
                // IO errors from mmap reads are truly exceptional; log and
                // stop collecting (remaining flakes will be missing from
                // overlay, but this is a storage-level failure, not a
                // dictionary gap).
                tracing::error!(%e, "IO error during overlay translation");
                io_error = Some(e);
            }
        }
    });

    tracing::trace!(count = ops.len(), "translate_overlay_flakes: collected ops");
    ops
}

/// Translate a single Flake to an OverlayOp using the DictOverlay.
///
/// All dictionary lookups that previously returned `Err("unknown ...")` now
/// delegate to `DictOverlay::assign_*` methods, which allocate ephemeral IDs
/// for entities not in the persisted dictionaries. Only true IO errors
/// (mmap read failures) propagate.
fn translate_one_flake(
    flake: &Flake,
    dict_overlay: &mut crate::dict_overlay::DictOverlay,
) -> std::io::Result<OverlayOp> {
    let s_id = dict_overlay.assign_subject_id_from_sid(&flake.s)?;
    let p_id = dict_overlay.assign_predicate_id_from_sid(&flake.p);
    let (o_kind, o_key) = dict_overlay.value_to_obj_pair(&flake.o)?;
    let dt = dict_overlay.assign_dt_id(&flake.dt);

    let (lang_id, i_val) = match &flake.m {
        Some(meta) => {
            let lang_id = match &meta.lang {
                Some(tag) => dict_overlay.assign_lang_id(tag),
                None => 0,
            };
            let i_val = meta.i.unwrap_or(ListIndex::none().as_i32());
            (lang_id, i_val)
        }
        None => (0, ListIndex::none().as_i32()),
    };

    Ok(OverlayOp {
        s_id,
        p_id,
        o_kind: o_kind.as_u8(),
        o_key: o_key.as_u64(),
        t: flake.t,
        op: flake.op,
        dt,
        lang_id,
        i_val,
    })
}

// ============================================================================
// ScanOperator
// ============================================================================

/// The sole scan operator for WHERE-clause triple patterns.
///
/// Created at plan time (before the `ExecutionContext` exists) with just the
/// pattern and optional object bounds. At `open()` time it inspects the context
/// and selects the appropriate scan strategy:
///
/// - **Binary cursor path**: When `ctx.binary_store` is present and the query
///   mode is compatible (not multi-ledger, not history, time within index
///   coverage). This is the fast streaming path via `BinaryScanOperator`.
///
/// - **Range-based fallback**: When the binary cursor path is not available —
///   pre-index databases, history mode, time-travel before `base_t`. Delegates
///   to `range_with_overlay()` via `RangeScanOperator`.
///
/// Overlay flakes are always translatable via `DictOverlay` (ephemeral IDs
/// are allocated for entities not yet in the persisted dictionaries).
pub struct ScanOperator {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    schema: Arc<[VarId]>,
    inner: Option<BoxedOperator>,
    state: OperatorState,
    /// Inline filter expressions to evaluate during batch processing.
    /// Applied after building bindings, before returning the batch.
    /// This reduces operator overhead by avoiding separate FilterOperator nodes.
    filters: Vec<crate::ir::Expression>,
}

impl ScanOperator {
    /// Create a new scan operator for a triple pattern.
    ///
    /// Schema is computed from the pattern variables. Filters are evaluated
    /// inline during batch processing, which is more efficient than wrapping
    /// with a separate FilterOperator.
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        filters: Vec<crate::ir::Expression>,
    ) -> Self {
        let (schema, _, _, _) = schema_from_pattern(&pattern);
        Self {
            pattern,
            object_bounds,
            schema,
            inner: None,
            state: OperatorState::Created,
            filters,
        }
    }
}

#[async_trait]
impl Operator for ScanOperator {
    fn schema(&self) -> &[VarId] {
        match &self.inner {
            Some(op) => op.schema(),
            None => &self.schema,
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Determine whether the binary cursor path can handle this query.
        // Binary cursors require: store present, single-ledger, time within
        // coverage, no history mode, no time-range queries.  Everything else
        // falls back to range_with_overlay() which works for multi-ledger,
        // pre-index, history, and time-travel-before-base_t via the
        // RangeProvider trait.
        let use_binary = ctx.has_binary_store() && {
            let s = ctx.binary_store.as_ref().unwrap();
            ctx.to_t >= s.base_t()
                && !ctx.history_mode
                && ctx.from_t.is_none()
                // Policy enforcement currently operates on materialized flakes.
                // When a non-root policy is present, fall back to the flake-based
                // range path (`range_with_overlay`) so policy filtering is applied.
                && ctx
                    .policy_enforcer
                    .as_ref()
                    .is_none_or(|enf| enf.is_root())
        };

        tracing::trace!(
            pattern = ?self.pattern,
            use_binary,
            has_overlay = ctx.overlay.is_some(),
            to_t = ctx.to_t,
            "ScanOperator::open"
        );
        if let Some(s) = ctx.binary_store.as_ref() {
            tracing::trace!(
                base_t = s.base_t(),
                "ScanOperator::open: binary_store present"
            );
        }

        // When the binary path is selected, create a DictOverlay for ephemeral
        // ID resolution during both overlay translation and result decoding.
        // If overlay exists, translate flakes to integer-ID space (infallible —
        // ephemeral IDs are allocated for entities not in persisted dictionaries).
        let (overlay_data, dict_overlay) = if use_binary && ctx.overlay.is_some() {
            let store = ctx.binary_store.as_ref().unwrap().clone();
            let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
                Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
            });
            let mut dict_ov = crate::dict_overlay::DictOverlay::new(store, dn);
            let ops =
                translate_overlay_flakes(ctx.overlay(), &mut dict_ov, ctx.to_t, ctx.binary_g_id);
            tracing::trace!(
                overlay_ops = ops.len(),
                g_id = ctx.binary_g_id,
                "ScanOperator::open: translated overlay ops"
            );
            let epoch = ctx.overlay().epoch();
            if ops.is_empty() {
                (None, Some(dict_ov))
            } else {
                (Some((ops, epoch)), Some(dict_ov))
            }
        } else if use_binary {
            let store = ctx.binary_store.as_ref().unwrap().clone();
            let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
                Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
            });
            (None, Some(crate::dict_overlay::DictOverlay::new(store, dn)))
        } else {
            (None, None)
        };

        let mut inner: BoxedOperator = if use_binary {
            let store = ctx.binary_store.as_ref().unwrap().clone();
            let mut op = BinaryScanOperator::new(
                self.pattern.clone(),
                store,
                ctx.binary_g_id,
                self.object_bounds.clone(),
                std::mem::take(&mut self.filters),
            );
            if let Some((ops, epoch)) = overlay_data {
                op.set_overlay(ops, epoch);
            }
            if let Some(dict_ov) = dict_overlay {
                op.set_dict_overlay(dict_ov);
            }
            Box::new(op)
        } else {
            // Fallback: range_with_overlay() for pre-index, history, or
            // time-travel-before-base_t queries.
            Box::new(RangeScanOperator::new(
                self.pattern.clone(),
                self.object_bounds.clone(),
                std::mem::take(&mut self.filters),
            ))
        };

        inner.open(ctx).await?;
        self.inner = Some(inner);
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        match &mut self.inner {
            Some(op) => op.next_batch(ctx).await,
            None => Err(QueryError::OperatorNotOpened),
        }
    }

    fn close(&mut self) {
        if let Some(op) = &mut self.inner {
            op.close();
        }
        self.inner = None;
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// RangeScanOperator — range_with_overlay() path
// ============================================================================

/// Range-based scan operator that delegates to `range_with_overlay()`.
///
/// Used when the binary cursor path is not available:
/// - Pre-index databases (no `BinaryIndexStore` yet — indexing is async)
/// - History mode queries
/// - Time-travel before `base_t`
///
/// When a `RangeProvider` is attached to the `Db` (the normal post-index
/// state), `range_with_overlay()` routes through it, so queries still
/// execute against the binary index — just via materialized collection
/// rather than streaming cursors.
///
/// For genesis databases (t=0, no index, no provider), returns overlay-only
/// flakes.
struct RangeScanOperator {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    schema: Arc<[VarId]>,
    s_var_pos: Option<usize>,
    p_var_pos: Option<usize>,
    o_var_pos: Option<usize>,
    /// Whether predicate is a variable (for internal predicate filtering).
    ///
    /// Mirrors `BinaryScanOperator` behavior: when `?p` is unbound, skip
    /// internal fluree:ledger predicates (commit metadata) so wildcard
    /// patterns like `?s ?p ?o` don't surface internal rows.
    p_is_var: bool,
    state: OperatorState,
    batches: VecDeque<Batch>,
    /// Inline filter expressions to evaluate during batch building.
    filters: Vec<crate::ir::Expression>,
}

impl RangeScanOperator {
    fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        filters: Vec<crate::ir::Expression>,
    ) -> Self {
        let p_is_var = matches!(pattern.p, Term::Var(_));
        let (schema, s_var_pos, p_var_pos, o_var_pos) = schema_from_pattern(&pattern);

        Self {
            pattern,
            object_bounds,
            schema,
            s_var_pos,
            p_var_pos,
            o_var_pos,
            p_is_var,
            state: OperatorState::Created,
            batches: VecDeque::new(),
            filters,
        }
    }

    /// Build a `RangeMatch` from the pattern's bound terms.
    fn build_range_match(&self, db: &Db) -> RangeMatch {
        let mut rm = RangeMatch::new();

        match &self.pattern.s {
            Term::Sid(sid) => rm.s = Some(sid.clone()),
            Term::Iri(iri) => {
                if let Some(sid) = db.encode_iri(iri) {
                    rm.s = Some(sid);
                }
            }
            _ => {}
        }

        match &self.pattern.p {
            Term::Sid(sid) => rm.p = Some(sid.clone()),
            Term::Iri(iri) => {
                if let Some(sid) = db.encode_iri(iri) {
                    rm.p = Some(sid);
                }
            }
            _ => {}
        }

        match &self.pattern.o {
            Term::Sid(sid) => rm.o = Some(FlakeValue::Ref(sid.clone())),
            Term::Value(val) => rm.o = Some(val.clone()),
            Term::Iri(iri) => {
                if let Some(sid) = db.encode_iri(iri) {
                    rm.o = Some(FlakeValue::Ref(sid));
                }
            }
            _ => {}
        }

        if let Some(dt) = &self.pattern.dt {
            rm.dt = Some(dt.clone());
        }

        rm
    }

    /// Build `RangeOptions` from execution context and this operator's bounds.
    fn build_range_opts(&self, to_t: i64, ctx: &ExecutionContext<'_>) -> RangeOptions {
        let mut opts = RangeOptions::new().with_to_t(to_t);
        if let Some(from_t) = ctx.from_t {
            opts = opts.with_from_t(from_t);
        }
        if ctx.history_mode {
            opts = opts.with_history_mode();
        }
        if let Some(bounds) = &self.object_bounds {
            opts = opts.with_object_bounds(bounds.clone());
        }
        opts
    }

    /// Scan one graph: range query + policy enforcement + post-filtering.
    ///
    /// Shared by the default-graphs, named-graphs, and single-db paths in `open()`.
    async fn scan_one_graph(
        &self,
        db: &Db,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        index: IndexType,
        ctx: &ExecutionContext<'_>,
        policy_enforcer: Option<&Arc<QueryPolicyEnforcer>>,
    ) -> Result<Vec<Flake>> {
        let range_match = self.build_range_match(db);
        let opts = self.build_range_opts(to_t, ctx);
        let flakes = range_with_overlay(db, overlay, index, RangeTest::Eq, range_match, opts)
            .await
            .map_err(|e| QueryError::execution(e.to_string()))?;

        // Policy filter (skip for root policies).
        let flakes = match policy_enforcer {
            Some(enforcer) if !enforcer.is_root() => {
                let subjects: Vec<fluree_db_core::Sid> = flakes
                    .iter()
                    .map(|f| f.s.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                enforcer
                    .populate_class_cache_for_graph(db, overlay, to_t, &subjects)
                    .await?;
                enforcer
                    .filter_flakes_for_graph(db, overlay, to_t, &ctx.tracker, flakes)
                    .await?
            }
            _ => flakes,
        };

        // Post-filter (overlay may return a superset).
        Ok(flakes
            .into_iter()
            .filter(|f| self.flake_matches(f, db))
            .collect())
    }

    /// Check if a flake matches the pattern's bound terms.
    ///
    /// `range_with_overlay` may return a superset (especially in the
    /// overlay-only genesis path), so we post-filter here.
    fn flake_matches(&self, f: &Flake, db: &Db) -> bool {
        match &self.pattern.s {
            Term::Sid(sid) if &f.s != sid => return false,
            Term::Iri(iri) => match db.encode_iri(iri) {
                Some(sid) if f.s != sid => return false,
                None => return false,
                _ => {}
            },
            _ => {}
        }

        match &self.pattern.p {
            Term::Sid(sid) if &f.p != sid => return false,
            Term::Iri(iri) => match db.encode_iri(iri) {
                Some(sid) if f.p != sid => return false,
                None => return false,
                _ => {}
            },
            _ => {}
        }

        match &self.pattern.o {
            Term::Sid(sid) => {
                if f.o != FlakeValue::Ref(sid.clone()) {
                    return false;
                }
            }
            Term::Value(val) if &f.o != val => return false,
            Term::Iri(iri) => match db.encode_iri(iri) {
                Some(sid) if f.o != FlakeValue::Ref(sid.clone()) => return false,
                None => return false,
                _ => {}
            },
            _ => {}
        }

        if let Some(dt) = &self.pattern.dt {
            if !dt_compatible(dt, &f.dt) {
                return false;
            }
        }

        if let Some(bounds) = &self.object_bounds {
            if !bounds.matches(&f.o) {
                return false;
            }
        }

        true
    }

    /// Convert a single flake to binding values matching the schema.
    fn flake_to_row(&self, f: &Flake, history_mode: bool) -> Vec<Binding> {
        let mut row = Vec::with_capacity(self.schema.len());

        if self.s_var_pos.is_some() {
            row.push(Binding::Sid(f.s.clone()));
        }
        if self.p_var_pos.is_some() {
            row.push(Binding::Sid(f.p.clone()));
        }
        if self.o_var_pos.is_some() {
            let binding = match &f.o {
                FlakeValue::Ref(sid) => Binding::Sid(sid.clone()),
                val => Binding::Lit {
                    val: val.clone(),
                    dt: f.dt.clone(),
                    lang: f
                        .m
                        .as_ref()
                        .and_then(|m| m.lang.as_ref().map(|s| Arc::from(s.as_str()))),
                    // Always attach `t` for literal bindings so `t(?var)` works even
                    // outside history mode when explicitly requested via `@t`.
                    t: Some(f.t),
                    op: if history_mode { Some(f.op) } else { None },
                },
            };
            row.push(binding);
        }

        row
    }
}

#[async_trait]
impl Operator for RangeScanOperator {
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

        let mut index = IndexType::for_query(
            self.pattern.s_bound(),
            self.pattern.p_bound(),
            self.pattern.o_bound(),
            self.pattern.o_is_ref(),
        );

        // When object bounds are present and default index is PSOT, switch to POST
        // for object-range scanning (matches BinaryScanOperator behavior).
        if self.object_bounds.is_some() && index == IndexType::Psot {
            index = IndexType::Post;
        }

        // Collect matching flakes.
        //
        // All paths delegate to scan_one_graph() which handles the range query,
        // policy enforcement, and post-filtering in a single call.
        //
        // Dataset mode:
        // - ActiveGraph::Default → union across all default graphs (fast path via slice)
        // - ActiveGraph::Named   → scan only the selected named graph(s)
        //
        // Single-db mode:
        // - scan `ctx.db`
        let flakes = if let Some(graphs) = ctx.default_graphs_slice() {
            let mut all_flakes = Vec::new();
            for graph in graphs {
                let enforcer = graph
                    .policy_enforcer
                    .as_ref()
                    .or(ctx.policy_enforcer.as_ref());
                let graph_flakes = self
                    .scan_one_graph(graph.db, graph.overlay, graph.to_t, index, ctx, enforcer)
                    .await?;
                all_flakes.extend(graph_flakes);
            }
            all_flakes
        } else if ctx.dataset.is_some() {
            let active = ctx.active_graphs();
            let graphs = active.as_many().unwrap_or(&[]);
            let mut all_flakes = Vec::new();
            for graph in graphs {
                let graph = *graph;
                let enforcer = graph
                    .policy_enforcer
                    .as_ref()
                    .or(ctx.policy_enforcer.as_ref());
                let graph_flakes = self
                    .scan_one_graph(graph.db, graph.overlay, graph.to_t, index, ctx, enforcer)
                    .await?;
                all_flakes.extend(graph_flakes);
            }
            all_flakes
        } else {
            self.scan_one_graph(
                ctx.db,
                ctx.overlay(),
                ctx.to_t,
                index,
                ctx,
                ctx.policy_enforcer.as_ref(),
            )
            .await?
        };

        // Build batches from collected flakes (already post-filtered by scan_one_graph).
        let batch_size = ctx.batch_size;
        let ncols = self.schema.len();

        if ncols == 0 {
            // All terms bound (existence check): count matches, emit empty-schema batch.
            let match_count = if self.p_is_var {
                flakes
                    .iter()
                    .filter(|f| f.p.namespace_code != FLUREE_DB)
                    .count()
            } else {
                flakes.len()
            };
            for _ in 0..match_count {
                ctx.tracker.consume_fuel_one()?;
            }
            if match_count > 0 {
                self.batches
                    .push_back(Batch::empty_schema_with_len(match_count));
            }
        } else {
            let mut columns: Vec<Vec<Binding>> =
                (0..ncols).map(|_| Vec::with_capacity(batch_size)).collect();
            let mut row_count = 0;

            let has_filters = !self.filters.is_empty();

            for f in &flakes {
                // Filter internal predicates (commit metadata) for wildcard predicate patterns.
                // Matches `BinaryScanOperator` behavior.
                if self.p_is_var && f.p.namespace_code == FLUREE_DB {
                    continue;
                }

                let row = self.flake_to_row(f, ctx.history_mode);

                // Apply inline filters before adding to batch
                if has_filters {
                    let binding_row = BindingRow::new(&self.schema, &row);
                    let passes = self.filters.iter().all(|expr| {
                        expr.eval_to_bool::<_>(&binding_row, Some(ctx))
                            .unwrap_or(false)
                    });
                    if !passes {
                        continue;
                    }
                }

                ctx.tracker.consume_fuel_one()?;
                for (col_idx, binding) in row.into_iter().enumerate() {
                    columns[col_idx].push(binding);
                }
                row_count += 1;

                if row_count >= batch_size {
                    let batch = Batch::new(self.schema.clone(), columns)
                        .map_err(|e| QueryError::execution(format!("batch construction: {e}")))?;
                    self.batches.push_back(batch);
                    columns = (0..ncols).map(|_| Vec::with_capacity(batch_size)).collect();
                    row_count = 0;
                }
            }

            // Final partial batch
            if row_count > 0 {
                let batch = Batch::new(self.schema.clone(), columns)
                    .map_err(|e| QueryError::execution(format!("batch construction: {e}")))?;
                self.batches.push_back(batch);
            }
        }

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        Ok(self.batches.pop_front())
    }

    fn close(&mut self) {
        self.batches.clear();
        self.state = OperatorState::Closed;
    }
}
