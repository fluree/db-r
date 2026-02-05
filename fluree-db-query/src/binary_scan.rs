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

use crate::binding::{Batch, Binding};
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
    range_with_overlay, Db, Flake, FlakeValue, IndexType, ObjectBounds, OverlayProvider,
    RangeMatch, RangeOptions, RangeTest, Sid, Storage,
};
use fluree_db_indexer::run_index::numfloat_dict::NumericShape;
use fluree_db_indexer::run_index::run_record::RunSortOrder;
use fluree_db_indexer::run_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryIndexStore, DecodedBatch, OverlayOp,
};
use fluree_vocab::namespaces::FLUREE_LEDGER;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

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
}

impl BinaryScanOperator {
    /// Create a new BinaryScanOperator for a triple pattern.
    ///
    /// The `store` must be loaded with indexes for the relevant sort orders.
    /// The `g_id` selects which graph to scan (typically 0 for default graph).
    pub fn new(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        object_bounds: Option<ObjectBounds>,
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

        // Build schema from pattern variables (same logic as ScanOperator)
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

        let p_is_var = matches!(pattern.p, Term::Var(_));

        Self {
            pattern,
            index,
            schema: Arc::from(schema_vec.into_boxed_slice()),
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
        }
    }

    /// Create with explicit index selection (for testing or forced order).
    pub fn with_index(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        index: IndexType,
    ) -> Self {
        let mut op = Self::new(pattern, store, g_id, None);
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
    /// Note: Currently unused with late materialization (EncodedSid emitted instead),
    /// but kept for future materialization needs.
    #[allow(dead_code)]
    fn resolve_s_id(&mut self, s_id: u64) -> Result<Sid> {
        if let Some(sid) = self.sid_cache.get(&s_id) {
            return Ok(sid.clone());
        }

        let sid = if let Some(ref dict_ov) = self.dict_overlay {
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
                .map_or(false, |s| s.namespace_code == FLUREE_LEDGER)
    }

    /// Decode an object value, routing through DictOverlay when present.
    #[inline]
    fn decode_obj(&self, o_kind: u8, o_key: u64, p_id: u32) -> std::io::Result<FlakeValue> {
        match &self.dict_overlay {
            Some(ov) => ov.decode_value(o_kind, o_key, p_id),
            None => self.store.decode_value(o_kind, o_key, p_id),
        }
    }

    /// Resolve a predicate ID to Sid, handling ephemeral IDs via DictOverlay.
    ///
    /// Note: Currently unused with late materialization (EncodedPid emitted instead),
    /// but kept for future materialization needs.
    #[allow(dead_code)]
    #[inline]
    fn resolve_p_id(&self, p_id: u32) -> Sid {
        let idx = p_id as usize;
        if idx < self.p_sids.len() {
            return self.p_sids[idx].clone();
        }
        // Ephemeral or out-of-range p_id: try DictOverlay, then store
        if let Some(ref ov) = self.dict_overlay {
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

    /// Convert a DecodedBatch into columnar Bindings.
    ///
    /// Processes all rows in the batch, converting integer IDs to Binding values.
    /// Only produces columns for variable positions in the schema.
    /// Filters out internal predicates when predicate is a variable.
    fn batch_to_bindings(
        &mut self,
        decoded: &DecodedBatch,
        columns: &mut [Vec<Binding>],
    ) -> Result<usize> {
        let mut produced = 0;

        for row in 0..decoded.row_count {
            // Filter out fluree:ledger internal predicates for wildcard patterns
            if self.is_internal_predicate(decoded.p_ids[row]) {
                continue;
            }

            // Post-filter: when an object value couldn't be translated to an
            // (ObjKind, ObjKey) pair (e.g. string literal — no reverse string index),
            // decode each row's object and skip non-matching rows.
            if let Some(ref filter_val) = self.bound_o_filter {
                let decoded_val = self
                    .decode_obj(
                        decoded.o_kinds[row],
                        decoded.o_keys[row],
                        decoded.p_ids[row],
                    )
                    .map_err(|e| {
                        QueryError::Internal(format!("decode object for filter: {}", e))
                    })?;
                if decoded_val != *filter_val {
                    continue;
                }
            }

            // Range post-filter: ObjectBounds applied after decoding.
            // POST key range provides coarse leaf filtering; this handles
            // inclusive/exclusive boundaries and cross-type comparisons exactly.
            if let Some(ref obj_bounds) = self.object_bounds {
                let decoded_val = self
                    .decode_obj(
                        decoded.o_kinds[row],
                        decoded.o_keys[row],
                        decoded.p_ids[row],
                    )
                    .map_err(|e| {
                        QueryError::Internal(format!("decode object for bounds: {}", e))
                    })?;
                if !obj_bounds.matches(&decoded_val) {
                    continue;
                }
            }

            let mut col_idx = 0;

            // Subject binding - emit EncodedSid for late materialization
            if self.s_var_pos.is_some() {
                let s_id = decoded.s_ids[row] as u64;
                tracing::trace!(s_id, "binary_scan emitting EncodedSid for subject");
                columns[col_idx].push(Binding::EncodedSid { s_id });
                col_idx += 1;
            }

            // Predicate binding - emit EncodedPid for late materialization
            if self.p_var_pos.is_some() {
                let p_id = decoded.p_ids[row];
                tracing::trace!(p_id, "binary_scan emitting EncodedPid for predicate");
                columns[col_idx].push(Binding::EncodedPid { p_id });
                col_idx += 1;
            }

            // Object binding - emit EncodedSid for refs, EncodedLit for literals
            if self.o_var_pos.is_some() {
                let o_kind = decoded.o_kinds[row];
                let binding = if o_kind == ObjKind::REF_ID.as_u8() {
                    // Ref object: use EncodedSid for late materialization
                    let ref_s_id = decoded.o_keys[row];
                    tracing::trace!(ref_s_id, "binary_scan emitting EncodedSid for ref object");
                    Binding::EncodedSid { s_id: ref_s_id }
                } else {
                    // Literal: use EncodedLit (avoids string dictionary lookups)
                    // This is the main late materialization win - literal strings
                    // are only decoded when needed for FILTER/ORDER BY/output.
                    Binding::EncodedLit {
                        o_kind,
                        o_key: decoded.o_keys[row],
                        p_id: decoded.p_ids[row],
                        dt_id: decoded.dt_values[row] as u16,
                        lang_id: decoded.lang_ids[row],
                        i_val: decoded.i_values[row],
                        t: decoded.t_values[row],
                    }
                };
                columns[col_idx].push(binding);
                // col_idx += 1; // last column
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
fn numeric_shape_from_db_stats<S: Storage + 'static>(
    ctx: &ExecutionContext<'_, S>,
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
impl<S: Storage + 'static> Operator<S> for BinaryScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
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

        match bounds {
            Some((mut min_key, mut max_key)) => {
                // Narrow key range with ObjectBounds when available.
                // Requires a bound predicate (need p_id for shape lookup and POST order).
                if let Some(ref obj_bounds) = self.object_bounds {
                    if let Some(ref p) = p_sid {
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

                if let Some(ref sid) = s_sid {
                    if let Ok(Some(s_id)) = self.store.sid_to_s_id(sid) {
                        filter.s_id = Some(s_id);
                    }
                }
                let resolved_p_id = p_sid.as_ref().and_then(|sid| self.store.sid_to_p_id(sid));
                if let Some(p_id) = resolved_p_id {
                    filter.p_id = Some(p_id);
                }
                if let Some(ref val) = o_val {
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

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
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
                    let n = self.batch_to_bindings(&decoded, &mut columns)?;
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
pub(crate) fn translate_overlay_flakes(
    overlay: &dyn OverlayProvider,
    dict_overlay: &mut crate::dict_overlay::DictOverlay,
    to_t: i64,
) -> Vec<OverlayOp> {
    let mut ops = Vec::new();
    let mut io_error: Option<std::io::Error> = None;

    // Collect all overlay flakes (no boundary filtering — the cursor
    // partitions per-leaf internally via find_overlay_for_leaf).
    overlay.for_each_overlay_flake(IndexType::Spot, None, None, true, to_t, &mut |flake| {
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
pub struct ScanOperator<S: Storage + 'static> {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    schema: Arc<[VarId]>,
    inner: Option<BoxedOperator<S>>,
    state: OperatorState,
}

impl<S: Storage + 'static> ScanOperator<S> {
    /// Create a new scan operator for a triple pattern.
    ///
    /// Schema is computed from the pattern variables.
    pub fn new(pattern: TriplePattern, object_bounds: Option<ObjectBounds>) -> Self {
        let mut schema_vec = Vec::with_capacity(3);
        if let Term::Var(v) = &pattern.s {
            schema_vec.push(*v);
        }
        if let Term::Var(v) = &pattern.p {
            schema_vec.push(*v);
        }
        if let Term::Var(v) = &pattern.o {
            schema_vec.push(*v);
        }

        Self {
            pattern,
            object_bounds,
            schema: Arc::from(schema_vec.into_boxed_slice()),
            inner: None,
            state: OperatorState::Created,
        }
    }
}

#[async_trait]
impl<S: Storage + 'static> Operator<S> for ScanOperator<S> {
    fn schema(&self) -> &[VarId] {
        match &self.inner {
            Some(op) => op.schema(),
            None => &self.schema,
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
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
        let use_binary = ctx.binary_store.as_ref().map_or(false, |s| {
            !ctx.is_multi_ledger()
                && ctx.to_t >= s.base_t()
                && !ctx.history_mode
                && ctx.from_t.is_none()
        });

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
            let ops = translate_overlay_flakes(ctx.overlay(), &mut dict_ov, ctx.to_t);
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

        let mut inner: BoxedOperator<S> = if use_binary {
            let store = ctx.binary_store.as_ref().unwrap().clone();
            let mut op = BinaryScanOperator::new(
                self.pattern.clone(),
                store,
                ctx.binary_g_id,
                self.object_bounds.clone(),
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
            Box::new(RangeScanOperator::<S>::new(
                self.pattern.clone(),
                self.object_bounds.clone(),
            ))
        };

        inner.open(ctx).await?;
        self.inner = Some(inner);
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
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
struct RangeScanOperator<S: Storage + 'static> {
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
    _marker: std::marker::PhantomData<S>,
}

impl<S: Storage + 'static> RangeScanOperator<S> {
    fn new(pattern: TriplePattern, object_bounds: Option<ObjectBounds>) -> Self {
        let p_is_var = matches!(pattern.p, Term::Var(_));
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

        Self {
            pattern,
            object_bounds,
            schema: Arc::from(schema_vec.into_boxed_slice()),
            s_var_pos,
            p_var_pos,
            o_var_pos,
            p_is_var,
            state: OperatorState::Created,
            batches: VecDeque::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Build a `RangeMatch` from the pattern's bound terms.
    fn build_range_match(&self, db: &Db<S>) -> RangeMatch {
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

    /// Check if a flake matches the pattern's bound terms.
    ///
    /// `range_with_overlay` may return a superset (especially in the
    /// overlay-only genesis path), so we post-filter here.
    fn flake_matches(&self, f: &Flake, db: &Db<S>) -> bool {
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
            if &f.dt != dt {
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
                    t: if history_mode { Some(f.t) } else { None },
                    op: if history_mode { Some(f.op) } else { None },
                },
            };
            row.push(binding);
        }

        row
    }
}

#[async_trait]
impl<S: Storage + 'static> Operator<S> for RangeScanOperator<S> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
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

        // Collect matching flakes — iterate over all active default graphs
        // in dataset mode so multi-ledger union queries return results from
        // every graph, not just the primary.
        let flakes = if let Some(graphs) = ctx.default_graphs_slice() {
            let mut all_flakes = Vec::new();
            for graph in graphs {
                let range_match = self.build_range_match(graph.db);
                let mut opts = RangeOptions::new().with_to_t(graph.to_t);
                if let Some(from_t) = ctx.from_t {
                    opts = opts.with_from_t(from_t);
                }
                if ctx.history_mode {
                    opts = opts.with_history_mode();
                }
                if let Some(ref bounds) = self.object_bounds {
                    opts = opts.with_object_bounds(bounds.clone());
                }
                let graph_flakes = range_with_overlay(
                    graph.db,
                    graph.overlay,
                    index,
                    RangeTest::Eq,
                    range_match,
                    opts,
                )
                .await
                .map_err(|e| QueryError::execution(e.to_string()))?;
                // Policy filter per graph (before pattern matching).
                let graph_flakes = match graph
                    .policy_enforcer
                    .as_ref()
                    .or(ctx.policy_enforcer.as_ref())
                {
                    Some(enforcer) if !enforcer.is_root() => {
                        enforcer
                            .filter_flakes_for_graph(
                                graph.db,
                                graph.overlay,
                                graph.to_t,
                                &ctx.tracker,
                                graph_flakes,
                            )
                            .await?
                    }
                    _ => graph_flakes,
                };
                // Pre-filter per graph (overlay may return a superset).
                all_flakes.extend(
                    graph_flakes
                        .into_iter()
                        .filter(|f| self.flake_matches(f, graph.db)),
                );
            }
            all_flakes
        } else {
            let range_match = self.build_range_match(ctx.db);
            let mut opts = RangeOptions::new().with_to_t(ctx.to_t);
            if let Some(from_t) = ctx.from_t {
                opts = opts.with_from_t(from_t);
            }
            if ctx.history_mode {
                opts = opts.with_history_mode();
            }
            if let Some(ref bounds) = self.object_bounds {
                opts = opts.with_object_bounds(bounds.clone());
            }
            let flakes = range_with_overlay(
                ctx.db,
                ctx.overlay(),
                index,
                RangeTest::Eq,
                range_match,
                opts,
            )
            .await
            .map_err(|e| QueryError::execution(e.to_string()))?;
            // Policy filter (single-graph mode).
            match ctx.policy_enforcer.as_ref() {
                Some(enforcer) if !enforcer.is_root() => {
                    enforcer
                        .filter_flakes_for_graph(
                            ctx.db,
                            ctx.overlay(),
                            ctx.to_t,
                            &ctx.tracker,
                            flakes,
                        )
                        .await?
                }
                _ => flakes,
            }
        };

        let is_multi_graph = ctx.default_graphs_slice().is_some();

        // Post-filter: overlay-only path may return a superset of matches.
        // In multi-graph mode, flakes were already pre-filtered per graph above.
        let batch_size = ctx.batch_size;
        let ncols = self.schema.len();

        if ncols == 0 {
            // All terms bound (existence check): count matches, emit empty-schema batch
            let match_count = if is_multi_graph {
                if self.p_is_var {
                    flakes
                        .iter()
                        .filter(|f| f.p.namespace_code != FLUREE_LEDGER)
                        .count()
                } else {
                    flakes.len()
                }
            } else {
                flakes
                    .iter()
                    .filter(|f| {
                        (!self.p_is_var || f.p.namespace_code != FLUREE_LEDGER)
                            && self.flake_matches(f, ctx.db)
                    })
                    .count()
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

            for f in &flakes {
                if !is_multi_graph && !self.flake_matches(f, ctx.db) {
                    continue;
                }
                // Filter internal predicates (commit metadata) for wildcard predicate patterns.
                // Matches `BinaryScanOperator` behavior.
                if self.p_is_var && f.p.namespace_code == FLUREE_LEDGER {
                    continue;
                }

                ctx.tracker.consume_fuel_one()?;
                let row = self.flake_to_row(f, ctx.history_mode);
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

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
        Ok(self.batches.pop_front())
    }

    fn close(&mut self) {
        self.batches.clear();
        self.state = OperatorState::Closed;
    }
}
