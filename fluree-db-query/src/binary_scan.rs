//! BinaryScanOperator: reads from binary columnar indexes and converts to bindings.
//!
//! Replaces `ScanOperator` for queries when a `BinaryIndexStore` is available.
//! Works entirely in integer-ID space (s_id, p_id, ValueId) and only resolves
//! to Sid/FlakeValue at the Binding output boundary.
//!
//! # Integer-ID Pipeline
//!
//! 1. Pattern's bound terms → integer IDs (s_id, p_id, ValueId) via `BinaryIndexStore`
//! 2. `BinaryCursor` iterates leaves using integer comparators and columnar filters
//! 3. `DecodedBatch` rows converted to `Binding` columns:
//!    - s_id → Sid (cached in `sid_cache`)
//!    - p_id → Sid (pre-computed in `p_sids`)
//!    - o (ValueId) → FlakeValue via `store.decode_value()`
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
use crate::scan::ScanOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::value_id::ValueId;
use fluree_db_core::{Flake, FlakeValue, IndexType, NodeCache, ObjectBounds, OverlayProvider, Sid, Storage};
use fluree_db_indexer::run_index::{
    BinaryCursor, BinaryFilter, BinaryIndexStore, DecodedBatch, OverlayOp,
};
use fluree_db_indexer::run_index::run_record::{RunSortOrder, NO_LIST_INDEX};
use fluree_vocab::namespaces::FLUREE_LEDGER;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// IndexType → RunSortOrder mapping
// ============================================================================

/// Map from fluree-db-core's `IndexType` to `RunSortOrder`.
///
/// TSPO (time-ordered) is not built in the binary indexes, so it falls
/// back to SPOT. Callers requiring time-travel should use `ScanOperator`.
fn index_type_to_sort_order(idx: IndexType) -> RunSortOrder {
    match idx {
        IndexType::Spot => RunSortOrder::Spot,
        IndexType::Psot => RunSortOrder::Psot,
        IndexType::Post => RunSortOrder::Post,
        IndexType::Opst => RunSortOrder::Opst,
        IndexType::Tspo => RunSortOrder::Spot, // fallback: no TSPO in binary indexes
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
/// Created instead of `ScanOperator` when a `BinaryIndexStore` is available
/// on the execution context. Falls back to `ScanOperator` for normal B-tree
/// index queries (e.g., when no binary index is loaded).
///
/// # Single-Ledger Only
///
/// This operator is designed for single-ledger mode (no cross-ledger joins).
/// It creates `Binding::Sid` for IRI positions. Multi-ledger `IriMatch` bindings
/// are not supported — use `ScanOperator` for dataset queries.
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
    sid_cache: HashMap<u32, Sid>,
    /// Whether predicate is a variable (for internal predicate filtering).
    p_is_var: bool,
    /// Bound object value for post-filtering when the value cannot be translated
    /// to a ValueId (e.g., string literals without a reverse index).
    /// When set, `batch_to_bindings` filters rows whose decoded object != this value.
    bound_o_filter: Option<FlakeValue>,
    /// Pre-translated overlay operations (set by DeferredScanOperator).
    overlay_ops: Vec<OverlayOp>,
    /// Overlay epoch for cache key differentiation.
    overlay_epoch: u64,
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
    ) -> Self {
        let index = IndexType::for_query(
            pattern.s_bound(),
            pattern.p_bound(),
            pattern.o_bound(),
            pattern.o_is_ref(),
        );

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
            overlay_ops: Vec::new(),
            overlay_epoch: 0,
        }
    }

    /// Create with explicit index selection (for testing or forced order).
    pub fn with_index(
        pattern: TriplePattern,
        store: Arc<BinaryIndexStore>,
        g_id: u32,
        index: IndexType,
    ) -> Self {
        let mut op = Self::new(pattern, store, g_id);
        op.index = index;
        op
    }

    /// Get the index type being used.
    pub fn index_type(&self) -> IndexType {
        self.index
    }

    /// Set pre-translated overlay operations and epoch for query-time merge.
    ///
    /// Called by `DeferredScanOperator` after translating overlay Flakes to
    /// integer-ID space. The ops are sorted by the cursor's sort order during
    /// `open()` and passed to the `BinaryCursor` for per-leaf merge.
    pub fn set_overlay(&mut self, ops: Vec<OverlayOp>, epoch: u64) {
        self.overlay_ops = ops;
        self.overlay_epoch = epoch;
    }

    /// Resolve s_id to Sid, using cache for amortized lookups.
    ///
    /// On cache miss: s_id → IRI (forward file) → encode_iri → Sid.
    fn resolve_s_id(&mut self, s_id: u32) -> Result<Sid> {
        if let Some(sid) = self.sid_cache.get(&s_id) {
            return Ok(sid.clone());
        }

        let iri = self.store.resolve_subject_iri(s_id)
            .map_err(|e| QueryError::Internal(format!("resolve s_id {}: {}", s_id, e)))?;
        let sid = self.store.encode_iri(&iri);
        self.sid_cache.insert(s_id, sid.clone());
        Ok(sid)
    }

    /// Check if a predicate p_id corresponds to a fluree:ledger internal predicate.
    ///
    /// Internal predicates are filtered out when the pattern's predicate is a variable
    /// (wildcard), matching ScanOperator's `skip_internal_predicate` behavior.
    #[inline]
    fn is_internal_predicate(&self, p_id: u32) -> bool {
        self.p_is_var
            && self.p_sids.get(p_id as usize)
                .map_or(false, |s| s.namespace_code == FLUREE_LEDGER)
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

            // Post-filter: when an object value couldn't be translated to a
            // ValueId key (e.g. string literal — no reverse string index),
            // decode each row's object and skip non-matching rows.
            if let Some(ref filter_val) = self.bound_o_filter {
                let vid = ValueId::from_u64(decoded.o_values[row]);
                let decoded_val = self.store.decode_value(vid)
                    .map_err(|e| QueryError::Internal(
                        format!("decode object for filter: {}", e),
                    ))?;
                if decoded_val != *filter_val {
                    continue;
                }
            }

            let mut col_idx = 0;

            // Subject binding
            if self.s_var_pos.is_some() {
                let sid = self.resolve_s_id(decoded.s_ids[row])?;
                columns[col_idx].push(Binding::Sid(sid));
                col_idx += 1;
            }

            // Predicate binding
            if self.p_var_pos.is_some() {
                let p_id = decoded.p_ids[row] as usize;
                let sid = if p_id < self.p_sids.len() {
                    self.p_sids[p_id].clone()
                } else {
                    // Fallback (shouldn't happen with valid indexes)
                    match self.store.resolve_predicate_iri(p_id as u32) {
                        Some(iri) => self.store.encode_iri(iri),
                        None => Sid::new(0, ""),
                    }
                };
                columns[col_idx].push(Binding::Sid(sid));
                col_idx += 1;
            }

            // Object binding
            if self.o_var_pos.is_some() {
                let vid = ValueId::from_u64(decoded.o_values[row]);
                let val = self.store.decode_value(vid)
                    .map_err(|e| QueryError::Internal(format!("decode object: {}", e)))?;

                let binding = match val {
                    FlakeValue::Ref(ref sid) => Binding::Sid(sid.clone()),
                    other => {
                        // Literal: requires datatype/lang/index/t metadata.
                        let dt_id = decoded.dt_values[row] as usize;
                        let dt_sid = self.store
                            .dt_sids()
                            .get(dt_id)
                            .cloned()
                            .unwrap_or_else(|| Sid::new(0, ""));

                        let lang_id = decoded.lang_ids[row];
                        let i_val = decoded.i_values[row];
                        let meta = self.store.decode_meta(lang_id, i_val);
                        let t = decoded.t_values[row];

                        Binding::Lit {
                            val: other,
                            dt: dt_sid,
                            lang: meta.and_then(|m| m.lang.map(Arc::from)),
                            t: Some(t),
                            op: None, // snapshot-only, no retractions
                        }
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

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for BinaryScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Pre-compute p_sids for all predicates (typically ~96 for DBLP)
        let pred_count = self.store.predicate_count();
        self.p_sids = (0..pred_count).map(|p_id| {
            match self.store.resolve_predicate_iri(p_id) {
                Some(iri) => self.store.encode_iri(iri),
                None => Sid::new(0, ""),
            }
        }).collect();

        // Extract bound terms from the pattern
        let (s_sid, p_sid, o_val) = self.extract_bound_terms();

        let order = index_type_to_sort_order(self.index);

        // Translate to RunRecord bounds
        let bounds = self.store.translate_range(
            s_sid.as_ref(),
            p_sid.as_ref(),
            o_val.as_ref(),
            order,
            self.g_id,
        ).map_err(|e| QueryError::Internal(format!("translate_range: {}", e)))?;

        // If translate_range returned None but we had an object value,
        // the value may be untranslatable (e.g., string literal with no
        // reverse index). Retry without the object bound and enable
        // post-filtering in batch_to_bindings instead.
        let bounds = match bounds {
            some @ Some(_) => some,
            None => {
                if o_val.is_some() {
                    let retry = self.store.translate_range(
                        s_sid.as_ref(),
                        p_sid.as_ref(),
                        None,
                        order,
                        self.g_id,
                    ).map_err(|e| QueryError::Internal(format!("translate_range: {}", e)))?;
                    if retry.is_some() {
                        self.bound_o_filter = o_val.clone();
                        tracing::debug!(
                            "object value untranslatable, using post-filter scan"
                        );
                    }
                    retry
                } else {
                    None
                }
            }
        };

        match bounds {
            Some((min_key, max_key)) => {
                // Build BinaryFilter from bound terms
                let mut filter = BinaryFilter::new();

                if let Some(ref sid) = s_sid {
                    if let Ok(Some(s_id)) = self.store.sid_to_s_id(sid) {
                        filter.s_id = Some(s_id);
                    }
                }
                if let Some(ref sid) = p_sid {
                    if let Some(p_id) = self.store.sid_to_p_id(sid) {
                        filter.p_id = Some(p_id);
                    }
                }
                if let Some(ref val) = o_val {
                    if let Ok(Some(vid)) = self.store.value_to_value_id(val) {
                        filter.o = Some(vid);
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
                    self.overlay_ops.sort_by(|a, b| {
                        match order {
                            RunSortOrder::Spot => a.s_id.cmp(&b.s_id)
                                .then(a.p_id.cmp(&b.p_id))
                                .then(a.o.cmp(&b.o))
                                .then(a.dt.cmp(&b.dt)),
                            RunSortOrder::Psot => a.p_id.cmp(&b.p_id)
                                .then(a.s_id.cmp(&b.s_id))
                                .then(a.o.cmp(&b.o))
                                .then(a.dt.cmp(&b.dt)),
                            RunSortOrder::Post => a.p_id.cmp(&b.p_id)
                                .then(a.o.cmp(&b.o))
                                .then(a.dt.cmp(&b.dt))
                                .then(a.s_id.cmp(&b.s_id)),
                            RunSortOrder::Opst => a.o.cmp(&b.o)
                                .then(a.dt.cmp(&b.dt))
                                .then(a.p_id.cmp(&b.p_id))
                                .then(a.s_id.cmp(&b.s_id)),
                        }
                    });
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

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
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

        // Pull decoded batches from cursor until we fill a batch or exhaust cursor
        while produced < batch_size {
            let cursor = match &mut self.cursor {
                Some(c) => c,
                None => break,
            };

            match cursor.next_leaf() {
                Ok(Some(decoded)) => {
                    produced += self.batch_to_bindings(&decoded, &mut columns)?;
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
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// Overlay Flake → OverlayOp translation
// ============================================================================

/// Translate overlay flakes from Sid/FlakeValue space to integer-ID space.
///
/// Collects all overlay flakes via the provider's push API and translates each
/// to an `OverlayOp` using the store's reverse dictionaries. Returns an error
/// description if any flake references a subject, predicate, value, datatype,
/// or language tag not present in the binary index dictionaries.
fn translate_overlay_flakes(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStore,
    to_t: i64,
) -> std::result::Result<Vec<OverlayOp>, String> {
    let mut ops = Vec::new();
    let mut error: Option<String> = None;

    // Collect all overlay flakes (no boundary filtering — the cursor
    // partitions per-leaf internally via find_overlay_for_leaf).
    overlay.for_each_overlay_flake(
        IndexType::Spot, None, None, true, to_t,
        &mut |flake| {
            if error.is_some() { return; }
            match translate_one_flake(flake, store) {
                Ok(op) => ops.push(op),
                Err(reason) => error = Some(reason),
            }
        },
    );

    match error {
        Some(reason) => Err(reason),
        None => Ok(ops),
    }
}

/// Translate a single Flake to an OverlayOp using the store's dictionaries.
fn translate_one_flake(
    flake: &Flake,
    store: &BinaryIndexStore,
) -> std::result::Result<OverlayOp, String> {
    let s_id = store.sid_to_s_id(&flake.s)
        .map_err(|e| format!("s_id lookup: {}", e))?
        .ok_or_else(|| format!("unknown subject: {}", flake.s))?;

    let p_id = store.sid_to_p_id(&flake.p)
        .ok_or_else(|| format!("unknown predicate: {}", flake.p))?;

    let vid = store.value_to_value_id(&flake.o)
        .map_err(|e| format!("value lookup: {}", e))?
        .ok_or_else(|| "unknown object value".to_string())?;

    let dt = store.find_dt_id(&flake.dt)
        .ok_or_else(|| format!("unknown datatype: {}", flake.dt))?;

    let (lang_id, i_val) = match &flake.m {
        Some(meta) => {
            let lang_id = match &meta.lang {
                Some(tag) => store.find_lang_id(tag)
                    .ok_or_else(|| format!("unknown language tag: {}", tag))?,
                None => 0,
            };
            let i_val = meta.i.unwrap_or(NO_LIST_INDEX);
            (lang_id, i_val)
        }
        None => (0, NO_LIST_INDEX),
    };

    Ok(OverlayOp {
        s_id,
        p_id,
        o: vid.as_u64(),
        t: flake.t,
        op: flake.op,
        dt,
        lang_id,
        i_val,
    })
}

// ============================================================================
// DeferredScanOperator
// ============================================================================

/// Deferred scan operator that selects between `BinaryScanOperator` and
/// `ScanOperator` at `open()` time based on the `ExecutionContext`.
///
/// The query planner creates operators at plan time, before the
/// `ExecutionContext` exists.  `DeferredScanOperator` bridges this gap by
/// holding the pattern and optional object bounds, then materializing the
/// appropriate scan operator once `open()` provides access to the context.
///
/// # Binary path eligibility
///
/// The binary path is selected when **all** of the following hold:
/// - `ctx.binary_store` is `Some`
/// - Not in multi-ledger (dataset) mode
/// - Not in history mode
/// - Time-travel is within binary index coverage (`to_t >= store.base_t()`)
/// - No history range query (`from_t` is `None`)
/// - No `ObjectBounds` (range filters are not yet supported in binary path)
/// - If overlay is set: all overlay flakes are translatable to binary dict IDs
///   (falls back to `ScanOperator` if overlay contains new subjects/predicates)
///
/// Otherwise, falls back to the standard `ScanOperator`.
pub struct DeferredScanOperator<S: Storage + 'static, C: NodeCache + 'static> {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    schema: Arc<[VarId]>,
    inner: Option<BoxedOperator<S, C>>,
    state: OperatorState,
}

impl<S: Storage + 'static, C: NodeCache + 'static> DeferredScanOperator<S, C> {
    /// Create a new deferred scan operator.
    ///
    /// Schema is computed from the pattern variables (matches both
    /// `ScanOperator` and `BinaryScanOperator` schema computation).
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
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for DeferredScanOperator<S, C> {
    fn schema(&self) -> &[VarId] {
        match &self.inner {
            Some(op) => op.schema(),
            None => &self.schema,
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Check if the binary index covers the requested time range.
        // Point-in-time time-travel (to_t < max_t) requires Region 3 history,
        // which is only available for t >= base_t (the initial full build's max_t).
        let binary_covers_time = ctx.binary_store.as_ref()
            .map_or(false, |s| ctx.to_t >= s.base_t());

        let use_binary = ctx.binary_store.is_some()
            && binary_covers_time
            && !ctx.is_multi_ledger()
            && !ctx.history_mode
            && ctx.from_t.is_none()
            && self.object_bounds.is_none();

        // When binary path is selected and overlay exists, translate overlay
        // flakes to integer-ID space. Fall back to ScanOperator if any flake
        // uses subjects/predicates/values not in the binary dictionaries.
        let (use_binary, overlay_data) = if use_binary && ctx.overlay.is_some() {
            let store = ctx.binary_store.as_ref().unwrap();
            match translate_overlay_flakes(ctx.overlay(), store, ctx.to_t) {
                Ok(ops) => (true, Some((ops, ctx.overlay().epoch()))),
                Err(reason) => {
                    tracing::debug!(%reason, "overlay untranslatable for binary path");
                    (false, None)
                }
            }
        } else {
            (use_binary, None)
        };

        let mut inner: BoxedOperator<S, C> = if use_binary {
            let store = ctx.binary_store.as_ref().unwrap().clone();
            let mut op = BinaryScanOperator::new(
                self.pattern.clone(),
                store,
                ctx.binary_g_id,
            );
            if let Some((ops, epoch)) = overlay_data {
                op.set_overlay(ops, epoch);
            }
            Box::new(op)
        } else {
            let scan = ScanOperator::new(self.pattern.clone());
            match &self.object_bounds {
                Some(bounds) => Box::new(scan.with_object_bounds(bounds.clone())),
                None => Box::new(scan),
            }
        };

        inner.open(ctx).await?;
        self.inner = Some(inner);
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
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
