//! Scan operator - reads flakes from an index and converts to bindings
//!
//! The ScanOperator wraps the core `range()` API and converts flakes
//! into columnar batches of bindings.
//!
//! # Chunked Scanning
//!
//! The scan operator uses `RangeCursor` to iterate over index leaves one at a
//! time, providing bounded memory usage regardless of result set size. This
//! enables efficient LIMIT queries and streaming.
//!
//! # Multi-Graph Support
//!
//! When a dataset is active, the scan operator queries the appropriate graph(s):
//! - `ActiveGraphs::Single`: Uses single `RangeCursor` for chunked iteration
//! - `ActiveGraphs::Many` with one graph: Uses single `RangeCursor` for that graph
//! - `ActiveGraphs::Many` with multiple graphs: Uses `MultiChunked` mode with
//!   one cursor per graph, draining each graph completely before moving to the next
//!
//! **Important**: Multiple default graphs are **unioned** (no deduplication).
//! Identical triples from different graphs both appear in results.
//!
//! # Per-Graph Policy and Provenance
//!
//! In multi-graph mode, each graph's flakes are filtered using that graph's
//! db/overlay/to_t (not ctx-level values). This ensures f:query policies
//! run against the correct snapshot. The `ledger_context` is set per-graph
//! to enable correct `IriMatch` bindings for cross-ledger joins.

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::operator::{Operator, OperatorState};
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarId;
use async_trait::async_trait;
#[cfg(feature = "native")]
use fluree_db_core::PrefetchRequest;
use fluree_db_core::{
    Db, Flake, FlakeMeta, FlakeValue, IndexType, NodeCache, ObjectBounds, OverlayProvider,
    RangeCursor, RangeMatch, RangeOptions, RangeTest, Sid, Storage, Tracker,
};
use fluree_vocab::namespaces::FLUREE_LEDGER;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

// Global scan timing aggregation
static SCAN_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static SCAN_TOTAL_TIME_US: AtomicU64 = AtomicU64::new(0);
static SCAN_BATCH_TIME_US: AtomicU64 = AtomicU64::new(0);

/// Reset and get aggregated scan statistics (count, total_ms, batch_ms)
pub fn scan_stats_reset() -> (u64, f64, f64) {
    let count = SCAN_TOTAL_COUNT.swap(0, AtomicOrdering::Relaxed);
    let total_us = SCAN_TOTAL_TIME_US.swap(0, AtomicOrdering::Relaxed);
    let batch_us = SCAN_BATCH_TIME_US.swap(0, AtomicOrdering::Relaxed);
    (count, total_us as f64 / 1000.0, batch_us as f64 / 1000.0)
}

/// Scan mode for the operator
enum ScanMode {
    /// Not yet initialized
    Uninitialized,
    /// Chunked iteration using RangeCursor (for single-graph mode)
    ///
    /// Memory model: Only one leaf's worth of flakes in memory at a time (~3000 flakes).
    /// The cursor manages B-tree traversal state and loads leaves on demand.
    Chunked {
        /// Current leaf's flakes being drained
        current_leaf: VecDeque<Flake>,
    },
    /// Multi-graph chunked iteration using multiple RangeCursors
    ///
    /// Each graph has its own cursor. We drain one graph completely before
    /// moving to the next (concatenation, not interleaving). This provides:
    /// - Bounded memory: O(#graphs × leaf_size) instead of O(total_flakes)
    /// - Correct provenance: ledger_context set per-graph before draining
    /// - Per-graph policy: filtering uses correct db/overlay/to_t
    ///
    /// Note: Results are NOT globally sorted across graphs. Each graph's results
    /// are sorted within that graph. Graphs are processed sequentially (Graph A's
    /// results, then Graph B's results, etc.). This matches the "union without
    /// deduplication" semantics.
    MultiChunked {
        /// Current leaf's flakes being drained
        current_leaf: VecDeque<Flake>,
        /// Index of current cursor being drained
        current_cursor_idx: usize,
        /// Per-graph cursor state
        cursors: Vec<GraphCursorState>,
    },
}

/// Per-graph cursor state for MultiChunked mode
struct GraphCursorState {
    /// The RangeCursor for this graph
    cursor: RangeCursor,
    /// Index into the dataset's default_graphs slice
    graph_idx: usize,
    /// Ledger alias for provenance (cloned from GraphRef)
    ledger_alias: Arc<str>,
    /// Effective policy enforcer for this graph (graph-level preferred, else ctx-level)
    effective_enforcer: Option<Arc<crate::policy::QueryPolicyEnforcer>>,
}

/// Scan operator - reads flakes from an index
///
/// Converts flakes matching a triple pattern into columnar batches
/// of bindings for the pattern's variables.
///
/// # Memory Model
///
/// Uses chunked iteration via `RangeCursor` for all modes:
/// - Only one leaf's worth of flakes in memory at a time (~3000 flakes)
/// - Bounded memory regardless of total result size: O(#graphs × leaf_size)
///
/// # Multi-Ledger Support
///
/// When scanning in dataset mode (multi-ledger), the operator creates
/// `Binding::IriMatch` for subject/predicate/object-ref positions instead
/// of plain `Binding::Sid`. This ensures correct cross-ledger joins by
/// carrying the canonical IRI along with the ledger-specific SID.
///
/// The `ledger_context` is set per-graph in multi-graph mode, ensuring
/// each graph's flakes are decoded using that graph's namespace table.
pub struct ScanOperator {
    /// The triple pattern to match
    pattern: TriplePattern,
    /// Index type to use
    index: IndexType,
    /// Output schema (variables from pattern)
    schema: Arc<[VarId]>,
    /// Positions of s, p, o variables in schema (-1 if not a variable)
    s_var_pos: Option<usize>,
    p_var_pos: Option<usize>,
    o_var_pos: Option<usize>,
    /// Operator state
    state: OperatorState,
    /// Scan mode (chunked or full load)
    scan_mode: ScanMode,
    /// RangeCursor for chunked iteration (created during open, used during next_batch)
    /// Note: This is Option because we can't create it until we have the ExecutionContext
    cursor: Option<RangeCursor>,
    /// Well-known datatypes for datatype equivalence checking
    datatypes: WellKnownDatatypes,
    /// Estimated row count (set after open, may be updated during iteration)
    estimated_rows: Option<usize>,
    /// Optional object value bounds for filter pushdown
    ///
    /// When set, the range query will filter results to only include flakes
    /// whose object value satisfies these bounds.
    object_bounds: Option<ObjectBounds>,
    /// History mode flag - when true, captures op metadata in bindings
    history_mode: bool,
    /// Current ledger context for multi-ledger mode
    ///
    /// When scanning from a dataset graph, this holds the ledger alias and
    /// a flag indicating multi-ledger mode. Used to create IriMatch bindings
    /// with proper provenance for cross-ledger joins.
    ledger_context: Option<LedgerContext>,
    /// Timing: when this operator was opened
    open_time: Option<std::time::Instant>,
    /// Timing: total time spent in next_batch
    batch_time_us: u64,
}

/// Context for creating bindings with ledger provenance
///
/// Used in multi-ledger mode to track which ledger a scan is operating on,
/// enabling creation of IriMatch bindings for correct cross-ledger joins.
#[derive(Clone, Debug)]
struct LedgerContext {
    /// Ledger alias (e.g., "orders:main")
    alias: Arc<str>,
}

impl ScanOperator {
    /// Create a new scan operator for a triple pattern
    pub fn new(pattern: TriplePattern) -> Self {
        // Determine index from bound components
        let index = IndexType::for_query(
            pattern.s_bound(),
            pattern.p_bound(),
            pattern.o_bound(),
            pattern.o_is_ref(),
        );

        // Build schema from pattern variables
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
            index,
            schema: Arc::from(schema_vec.into_boxed_slice()),
            s_var_pos,
            p_var_pos,
            o_var_pos,
            state: OperatorState::Created,
            scan_mode: ScanMode::Uninitialized,
            cursor: None,
            datatypes: WellKnownDatatypes::new(),
            estimated_rows: None,
            object_bounds: None,
            history_mode: false,  // Will be set from context during open()
            ledger_context: None, // Will be set from context during open() for dataset mode
            open_time: None,
            batch_time_us: 0,
        }
    }

    /// Create with explicit index selection
    pub fn with_index(pattern: TriplePattern, index: IndexType) -> Self {
        let mut op = Self::new(pattern);
        op.index = index;
        op
    }

    /// Set object value bounds for filter pushdown
    ///
    /// When object bounds are set, the range query will filter results to only
    /// include flakes whose object value satisfies the bounds. This enables
    /// filter pushdown for predicates like `?age > 18 AND ?age < 65`.
    ///
    /// Pass an `ObjectBounds` to restrict the range scan to flakes whose object value falls within the bounds.
    ///
    /// **Index optimization**: When object bounds are provided and the current index
    /// is PSOT (predicate-subject-object-time), this switches to POST (predicate-object-subject-time).
    /// POST allows seeking directly to the object value range in the B-tree, rather than
    /// scanning all subjects and filtering. This matches the Clojure planner behavior.
    pub fn with_object_bounds(mut self, bounds: ObjectBounds) -> Self {
        // Switch from PSOT to POST when object bounds are provided.
        // PSOT orders by subject first, so we'd have to scan all subjects.
        // POST orders by object first, allowing us to seek directly to the range.
        if self.index == IndexType::Psot {
            self.index = IndexType::Post;
        }

        self.object_bounds = Some(bounds);
        self
    }

    /// Get the index type being used
    pub fn index_type(&self) -> IndexType {
        self.index
    }

    /// Get the output schema (inherent method for use without trait bounds)
    pub fn schema(&self) -> &[VarId] {
        &self.schema
    }

    /// Build RangeMatch from the pattern's bound terms
    ///
    /// Handles Term::Sid directly. For Term::Iri (used in cross-ledger joins),
    /// use `build_range_match_for_db` to encode IRIs for a specific database.
    fn build_range_match(&self) -> RangeMatch {
        let mut rm = RangeMatch::new();

        if let Term::Sid(s) = &self.pattern.s {
            rm = rm.with_subject(s.clone());
        }

        if let Term::Sid(p) = &self.pattern.p {
            rm = rm.with_predicate(p.clone());
        }

        match &self.pattern.o {
            Term::Sid(sid) => {
                rm = rm.with_object(FlakeValue::Ref(sid.clone()));
            }
            Term::Value(v) => {
                rm = rm.with_object(v.clone());
            }
            Term::Var(_) | Term::Iri(_) => {}
        }

        // NOTE: dt constraint is NOT pushed to range query.
        // We use post-filter with family equivalence instead, which allows:
        // - xsd:integer to match xsd:int, xsd:long, xsd:short, xsd:byte
        // - xsd:double to match xsd:float
        // This preserves data fidelity while enabling flexible querying.

        rm
    }

    /// Build RangeMatch with IRI terms encoded for a specific database
    ///
    /// Used in cross-ledger joins where Term::Iri needs to be encoded using
    /// the target ledger's namespace table.
    ///
    /// Returns `None` if any IRI term cannot be encoded in this database,
    /// indicating that this ledger cannot have any matching flakes (the IRI
    /// doesn't exist in this ledger's namespace table).
    fn build_range_match_for_db<S: Storage, C: NodeCache>(
        &self,
        db: &Db<S, C>,
    ) -> Option<RangeMatch> {
        let mut rm = RangeMatch::new();

        // Subject
        match &self.pattern.s {
            Term::Sid(s) => {
                rm = rm.with_subject(s.clone());
            }
            Term::Iri(iri) => {
                // Encode IRI for this database's namespace table
                // If encoding fails, this ledger has no matches for this IRI
                let sid = db.encode_iri(iri)?;
                rm = rm.with_subject(sid);
            }
            Term::Var(_) | Term::Value(_) => {}
        }

        // Predicate
        match &self.pattern.p {
            Term::Sid(p) => {
                rm = rm.with_predicate(p.clone());
            }
            Term::Iri(iri) => {
                let sid = db.encode_iri(iri)?;
                rm = rm.with_predicate(sid);
            }
            Term::Var(_) | Term::Value(_) => {}
        }

        // Object
        match &self.pattern.o {
            Term::Sid(sid) => {
                rm = rm.with_object(FlakeValue::Ref(sid.clone()));
            }
            Term::Iri(iri) => {
                let sid = db.encode_iri(iri)?;
                rm = rm.with_object(FlakeValue::Ref(sid));
            }
            Term::Value(v) => {
                rm = rm.with_object(v.clone());
            }
            Term::Var(_) => {}
        }

        Some(rm)
    }

    /// Check if the pattern contains any Term::Iri terms that need per-db encoding
    fn has_iri_terms(&self) -> bool {
        matches!(&self.pattern.s, Term::Iri(_))
            || matches!(&self.pattern.p, Term::Iri(_))
            || matches!(&self.pattern.o, Term::Iri(_))
    }

    #[inline]
    fn skip_internal_predicate(&self, pred: &Sid) -> bool {
        matches!(self.pattern.p, Term::Var(_)) && pred.namespace_code == FLUREE_LEDGER
    }

    /// Convert a flake to bindings for the schema variables
    ///
    /// In multi-ledger mode (when ledger_context is set), creates IriMatch bindings
    /// for subject/predicate/object-ref positions to enable correct cross-ledger joins.
    /// In single-ledger mode, creates plain Sid bindings for efficiency.
    fn push_flake_bindings_into_columns<S: Storage + 'static, C: NodeCache + 'static>(
        &self,
        flake: Flake,
        columns: &mut [Vec<Binding>],
        ctx: &ExecutionContext<'_, S, C>,
    ) {
        // Columns are created in the same order as schema: s, p, o (when those are vars).
        // We push directly into the per-column vectors to avoid per-flake allocations.
        let mut col_idx = 0;

        if self.s_var_pos.is_some() {
            let binding = self.create_sid_binding(&flake.s, ctx);
            columns[col_idx].push(binding);
            col_idx += 1;
        }

        if self.p_var_pos.is_some() {
            let binding = self.create_sid_binding(&flake.p, ctx);
            columns[col_idx].push(binding);
            col_idx += 1;
        }

        if self.o_var_pos.is_some() {
            // Move object + datatype + transaction time into the binding (no cloning of strings/bytes).
            // The `t` field enables @t bindings in queries (e.g., {"@value": "?v", "@t": "?txn"}).
            // The `op` field enables @op bindings in history mode (e.g., {"@value": "?v", "@op": "?operation"}).
            let binding =
                self.create_object_binding(flake.o, flake.dt, flake.m, flake.t, flake.op, ctx);
            columns[col_idx].push(binding);
            // col_idx += 1;
        }
    }

    /// Create a binding for a SID (subject or predicate position)
    ///
    /// In multi-ledger mode, decodes to IRI and creates IriMatch for correct joins.
    /// In single-ledger mode, creates plain Sid binding for efficiency.
    #[inline]
    fn create_sid_binding<S: Storage + 'static, C: NodeCache + 'static>(
        &self,
        sid: &Sid,
        ctx: &ExecutionContext<'_, S, C>,
    ) -> Binding {
        if let Some(ref ledger_ctx) = self.ledger_context {
            // Multi-ledger mode: decode to IRI for correct cross-ledger joins
            if let Some(iri) = ctx.decode_sid_in_ledger(sid, &ledger_ctx.alias) {
                Binding::iri_match(iri, sid.clone(), ledger_ctx.alias.clone())
            } else {
                // Fallback if decode fails (shouldn't happen normally)
                Binding::Sid(sid.clone())
            }
        } else {
            // Single-ledger mode: plain Sid is sufficient
            Binding::Sid(sid.clone())
        }
    }

    /// Create a binding for an object value
    ///
    /// Handles both literal values and references. For references in multi-ledger
    /// mode, creates IriMatch for correct cross-ledger joins.
    #[inline]
    fn create_object_binding<S: Storage + 'static, C: NodeCache + 'static>(
        &self,
        val: FlakeValue,
        dt: Sid,
        meta: Option<FlakeMeta>,
        t: i64,
        op: bool,
        ctx: &ExecutionContext<'_, S, C>,
    ) -> Binding {
        match val {
            FlakeValue::Ref(ref sid) => {
                // Object is a reference - same treatment as subject/predicate
                if let Some(ref ledger_ctx) = self.ledger_context {
                    if let Some(iri) = ctx.decode_sid_in_ledger(sid, &ledger_ctx.alias) {
                        Binding::iri_match(iri, sid.clone(), ledger_ctx.alias.clone())
                    } else {
                        Binding::Sid(sid.clone())
                    }
                } else {
                    Binding::Sid(sid.clone())
                }
            }
            other => {
                // Literal value - no special handling needed
                if self.history_mode {
                    Binding::Lit {
                        val: other,
                        dt,
                        lang: meta.and_then(|m| m.lang.map(Arc::from)),
                        t: Some(t),
                        op: Some(op),
                    }
                } else {
                    Binding::Lit {
                        val: other,
                        dt,
                        lang: meta.and_then(|m| m.lang.map(Arc::from)),
                        t: Some(t),
                        op: None,
                    }
                }
            }
        }
    }

    /// Check if a flake should be included after applying filters.
    ///
    /// Applies datatype, language, and policy filters.
    ///
    /// Returns `true` if the flake should be included, `false` to skip.
    fn should_include_flake<S: Storage + 'static, C: NodeCache + 'static>(
        &self,
        flake: &Flake,
        _ctx: &ExecutionContext<'_, S, C>,
    ) -> Result<bool> {
        // Clojure parity: wildcard predicate patterns should not surface internal
        // fluree:ledger predicates unless explicitly requested.
        if self.skip_internal_predicate(&flake.p) {
            return Ok(false);
        }

        // Datatype filter with family equivalence
        // xsd:integer ≈ xsd:int ≈ xsd:long ≈ xsd:short ≈ xsd:byte
        // xsd:double ≈ xsd:float
        if let Some(dt) = &self.pattern.dt {
            if !self.datatypes.datatypes_equivalent(&flake.dt, dt) {
                return Ok(false);
            }
        }

        // Language filter
        if let Some(lang) = &self.pattern.lang {
            // Only apply language filter if it's not a variable (variables are handled by BIND)
            if !lang.starts_with('?') {
                let flake_lang = flake
                    .m
                    .as_ref()
                    .and_then(|m| m.lang.as_deref())
                    .unwrap_or("");
                if flake_lang != lang.as_ref() {
                    return Ok(false);
                }
            }
        }

        // Policy filter: skip flakes that don't pass policy check
        //
        // Policy filtering is handled by policy_enforcer in next_batch via filter_flakes.
        // No sync fallback is needed since all policy enforcement uses the async path.
        Ok(true)
    }

    /// Apply policy filtering to a leaf of flakes.
    ///
    /// Handles both class cache population (if needed) and the actual filtering.
    /// Returns the filtered flakes.
    async fn apply_policy_to_leaf<S2: Storage + 'static, C2: NodeCache + 'static>(
        mut leaf_flakes: Vec<Flake>,
        enforcer: &crate::policy::QueryPolicyEnforcer,
        db: &Db<S2, C2>,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        tracker: &Tracker,
    ) -> Result<Vec<Flake>> {
        if enforcer.is_root() {
            return Ok(leaf_flakes);
        }

        // Populate class cache if enforcer has f:onClass policies
        if enforcer.policy().wrapper().has_class_policies() {
            let unique_subjects: Vec<Sid> = leaf_flakes
                .iter()
                .map(|f| f.s.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            enforcer
                .populate_class_cache_for_graph(db, overlay, to_t, &unique_subjects)
                .await?;
        }

        // Apply policy filtering
        leaf_flakes = enforcer
            .filter_flakes_for_graph(db, overlay, to_t, tracker, leaf_flakes)
            .await?;

        Ok(leaf_flakes)
    }
}

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for ScanOperator {
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

        // Start timing
        self.open_time = Some(std::time::Instant::now());
        self.batch_time_us = 0;

        // Capture history mode from context for @op bindings
        self.history_mode = ctx.history_mode;

        // Check if we have IRI terms that need per-db encoding
        let has_iris = self.has_iri_terms();

        // Query the appropriate graph(s) based on active graph state
        match ctx.active_graphs() {
            ActiveGraphs::Single => {
                // Single-graph mode: use RangeCursor for chunked iteration
                // Use build_range_match_for_db if pattern has IRI terms
                let range_match = if has_iris {
                    match self.build_range_match_for_db(ctx.db) {
                        Some(rm) => rm,
                        None => {
                            // IRI encoding failed - this ledger has no matches
                            self.scan_mode = ScanMode::Chunked {
                                current_leaf: VecDeque::new(),
                            };
                            self.state = OperatorState::Exhausted;
                            return Ok(());
                        }
                    }
                } else {
                    self.build_range_match()
                };

                let mut opts = RangeOptions::new().with_to_t(ctx.to_t);
                if let Some(from_t) = ctx.from_t {
                    opts = opts.with_from_t(from_t);
                }
                if let Some(ref bounds) = self.object_bounds {
                    opts = opts.with_object_bounds(bounds.clone());
                }
                if self.history_mode {
                    opts = opts.with_history_mode();
                }

                // Create cursor for chunked iteration
                let cursor = RangeCursor::new(ctx.db, self.index, RangeTest::Eq, range_match, opts)
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create range cursor: {}", e))
                    })?;

                self.cursor = Some(cursor);
                self.scan_mode = ScanMode::Chunked {
                    current_leaf: VecDeque::new(),
                };
                // estimated_rows unknown until we scan (set to None for chunked mode)
                self.estimated_rows = None;
            }
            ActiveGraphs::Many(graphs) if graphs.len() == 1 => {
                // Single graph from dataset: use RangeCursor for chunked iteration
                let graph = graphs[0];

                // Use build_range_match_for_db if pattern has IRI terms
                let range_match = if has_iris {
                    match self.build_range_match_for_db(graph.db) {
                        Some(rm) => rm,
                        None => {
                            // IRI encoding failed - this ledger has no matches
                            self.scan_mode = ScanMode::Chunked {
                                current_leaf: VecDeque::new(),
                            };
                            self.state = OperatorState::Exhausted;
                            return Ok(());
                        }
                    }
                } else {
                    self.build_range_match()
                };

                let mut opts = RangeOptions::new().with_to_t(graph.to_t);
                if let Some(from_t) = ctx.from_t {
                    opts = opts.with_from_t(from_t);
                }
                if let Some(ref bounds) = self.object_bounds {
                    opts = opts.with_object_bounds(bounds.clone());
                }
                if self.history_mode {
                    opts = opts.with_history_mode();
                }

                // Create cursor for the single graph
                let cursor =
                    RangeCursor::new(graph.db, self.index, RangeTest::Eq, range_match, opts)
                        .map_err(|e| {
                            QueryError::Internal(format!("Failed to create range cursor: {}", e))
                        })?;

                self.cursor = Some(cursor);
                self.scan_mode = ScanMode::Chunked {
                    current_leaf: VecDeque::new(),
                };
                self.estimated_rows = None;

                // Set ledger context for multi-ledger mode
                // This enables IriMatch binding creation for correct cross-ledger joins
                self.ledger_context = Some(LedgerContext {
                    alias: graph.ledger_alias.clone(),
                });
            }
            ActiveGraphs::Many(_) => {
                // Multiple graphs: use MultiChunked mode with one cursor per graph
                //
                // This provides:
                // - Bounded memory: O(#graphs × leaf_size) instead of O(total_flakes)
                // - Correct provenance: ledger_context set per-graph before draining
                // - Per-graph policy: filtering uses correct db/overlay/to_t
                //
                // Note: Results are NOT globally sorted across graphs. Each graph is
                // drained completely before moving to the next (concatenation semantics).
                // This matches the "union without deduplication" semantics.
                //
                // IMPORTANT: Use default_graphs_slice() directly (not active_graphs())
                // to ensure graph_idx is stable and matches what next_batch() uses.
                let graphs = ctx.default_graphs_slice().ok_or_else(|| {
                    QueryError::Internal(
                        "MultiChunked requires dataset with default graphs".to_string(),
                    )
                })?;

                let mut cursors = Vec::with_capacity(graphs.len());

                for (graph_idx, graph) in graphs.iter().enumerate() {
                    // Encode IRI terms for each graph's namespace table
                    // Skip graphs where encoding fails (IRI doesn't exist in that ledger)
                    let range_match = if has_iris {
                        match self.build_range_match_for_db(graph.db) {
                            Some(rm) => rm,
                            None => continue, // Skip this graph - IRI not in this ledger
                        }
                    } else {
                        self.build_range_match()
                    };

                    let mut opts = RangeOptions::new().with_to_t(graph.to_t);
                    if let Some(from_t) = ctx.from_t {
                        opts = opts.with_from_t(from_t);
                    }
                    if let Some(ref bounds) = self.object_bounds {
                        opts = opts.with_object_bounds(bounds.clone());
                    }
                    if self.history_mode {
                        opts = opts.with_history_mode();
                    }

                    // Create cursor for this graph
                    let cursor =
                        RangeCursor::new(graph.db, self.index, RangeTest::Eq, range_match, opts)
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to create range cursor for graph '{}': {}",
                                    graph.ledger_alias, e
                                ))
                            })?;

                    // Determine effective enforcer (graph-level preferred, else ctx-level)
                    let effective_enforcer = graph
                        .policy_enforcer
                        .clone()
                        .or_else(|| ctx.policy_enforcer.clone());

                    cursors.push(GraphCursorState {
                        cursor,
                        graph_idx,
                        ledger_alias: graph.ledger_alias.clone(),
                        effective_enforcer,
                    });
                }

                // If all graphs were skipped (no IRI matches), we're done
                if cursors.is_empty() {
                    self.scan_mode = ScanMode::MultiChunked {
                        current_leaf: VecDeque::new(),
                        current_cursor_idx: 0,
                        cursors: Vec::new(),
                    };
                } else {
                    self.scan_mode = ScanMode::MultiChunked {
                        current_leaf: VecDeque::new(),
                        current_cursor_idx: 0,
                        cursors,
                    };
                }
                self.estimated_rows = None;
            }
        };

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
        let batch_start = std::time::Instant::now();

        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            // Exhausted or closed
            return Ok(None);
        }

        let batch_size = ctx.batch_size;
        let num_vars = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_vars)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();

        let mut produced = 0usize;

        // Check scan mode type first (without holding mutable borrow)
        let is_chunked = matches!(self.scan_mode, ScanMode::Chunked { .. });
        let is_multi_chunked = matches!(self.scan_mode, ScanMode::MultiChunked { .. });

        if matches!(self.scan_mode, ScanMode::Uninitialized) {
            return Err(QueryError::Internal(
                "Scan mode not initialized".to_string(),
            ));
        }

        if is_chunked {
            // Chunked mode: pull from cursor leaf-by-leaf
            while produced < batch_size {
                // Try to get a flake from current leaf
                let maybe_flake =
                    if let ScanMode::Chunked { current_leaf, .. } = &mut self.scan_mode {
                        current_leaf.pop_front()
                    } else {
                        None
                    };

                if let Some(flake) = maybe_flake {
                    if self.should_include_flake(&flake, ctx)? {
                        ctx.tracker.consume_fuel_one()?;
                        self.push_flake_bindings_into_columns(flake, &mut columns, ctx);
                        produced += 1;
                    }
                    continue;
                }

                // Current leaf exhausted, try to load next leaf from cursor
                // Use next_leaf_with_prefetch to fire prefetch at branch expansion time (optimal)
                let next_leaf = if let Some(ref mut cursor) = self.cursor {
                    // Get the appropriate db/overlay based on active graphs
                    // We create the prefetch callback per-branch to ensure overlay epochs match
                    match ctx.active_graphs() {
                        ActiveGraphs::Single => {
                            #[cfg(feature = "native")]
                            {
                                // Build prefetch callback only if:
                                // 1. Prefetch service is available
                                // 2. Overlay epochs match (no ReasoningOverlay or other overlay transformation)
                                let prefetch_enabled = ctx.prefetch.is_some()
                                    && ctx.prefetch_db.is_some()
                                    && ctx
                                        .prefetch_overlay
                                        .as_ref()
                                        .map(|po| po.epoch() == ctx.overlay().epoch())
                                        .unwrap_or(false);

                                if prefetch_enabled {
                                    let prefetch = ctx.prefetch.clone().unwrap();
                                    let prefetch_db = ctx.prefetch_db.clone().unwrap();
                                    let prefetch_overlay = ctx.prefetch_overlay.clone().unwrap();
                                    let to_t = ctx.to_t;
                                    let from_t = ctx.from_t;
                                    let history_mode = ctx.history_mode;

                                    let callback = move |nodes: Vec<fluree_db_core::IndexNode>| {
                                        for node in nodes {
                                            prefetch.try_enqueue(PrefetchRequest {
                                                db: prefetch_db.clone(),
                                                overlay: prefetch_overlay.clone(),
                                                node,
                                                to_t,
                                                from_t,
                                                history_mode,
                                            });
                                        }
                                    };
                                    cursor
                                        .next_leaf_with_prefetch(ctx.db, ctx.overlay(), callback)
                                        .await
                                        .map_err(|e| {
                                            QueryError::Internal(format!("Cursor error: {}", e))
                                        })?
                                } else {
                                    // Prefetch disabled or overlay mismatch - fall back to non-prefetch path
                                    cursor.next_leaf(ctx.db, ctx.overlay()).await.map_err(|e| {
                                        QueryError::Internal(format!("Cursor error: {}", e))
                                    })?
                                }
                            }
                            #[cfg(not(feature = "native"))]
                            {
                                cursor.next_leaf(ctx.db, ctx.overlay()).await.map_err(|e| {
                                    QueryError::Internal(format!("Cursor error: {}", e))
                                })?
                            }
                        }
                        ActiveGraphs::Many(graphs) if graphs.len() == 1 => {
                            let graph = graphs[0];
                            #[cfg(feature = "native")]
                            {
                                // For dataset mode, check if we can prefetch:
                                // - Need prefetch service
                                // - Need overlay epochs to match between prefetch_overlay and graph.overlay
                                // - Need db to match (same cache)
                                let prefetch_enabled = ctx.prefetch.is_some()
                                    && ctx.prefetch_db.is_some()
                                    && ctx
                                        .prefetch_overlay
                                        .as_ref()
                                        .map(|po| po.epoch() == graph.overlay.epoch())
                                        .unwrap_or(false);

                                if prefetch_enabled {
                                    let prefetch = ctx.prefetch.clone().unwrap();
                                    let prefetch_db = ctx.prefetch_db.clone().unwrap();
                                    let prefetch_overlay = ctx.prefetch_overlay.clone().unwrap();
                                    let to_t = graph.to_t;
                                    let from_t = ctx.from_t; // GraphRef doesn't track from_t separately
                                    let history_mode = ctx.history_mode;

                                    let callback = move |nodes: Vec<fluree_db_core::IndexNode>| {
                                        for node in nodes {
                                            prefetch.try_enqueue(PrefetchRequest {
                                                db: prefetch_db.clone(),
                                                overlay: prefetch_overlay.clone(),
                                                node,
                                                to_t,
                                                from_t,
                                                history_mode,
                                            });
                                        }
                                    };
                                    cursor
                                        .next_leaf_with_prefetch(graph.db, graph.overlay, callback)
                                        .await
                                        .map_err(|e| {
                                            QueryError::Internal(format!("Cursor error: {}", e))
                                        })?
                                } else {
                                    // Prefetch disabled or context mismatch - fall back to non-prefetch path
                                    cursor.next_leaf(graph.db, graph.overlay).await.map_err(
                                        |e| QueryError::Internal(format!("Cursor error: {}", e)),
                                    )?
                                }
                            }
                            #[cfg(not(feature = "native"))]
                            {
                                cursor
                                    .next_leaf(graph.db, graph.overlay)
                                    .await
                                    .map_err(|e| {
                                        QueryError::Internal(format!("Cursor error: {}", e))
                                    })?
                            }
                        }
                        _ => {
                            // Multi-graph should use MultiChunked mode, not Chunked
                            return Err(QueryError::Internal(
                                "Chunked mode used with multi-graph dataset".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };

                match next_leaf {
                    Some(mut leaf_flakes) => {
                        // Apply async policy filtering per-leaf if enforcer is present.
                        // CRITICAL: Use the GRAPH's db/overlay/to_t for policy evaluation.
                        match ctx.active_graphs() {
                            ActiveGraphs::Single => {
                                if let Some(enforcer) = ctx.policy_enforcer.as_ref() {
                                    leaf_flakes = Self::apply_policy_to_leaf(
                                        leaf_flakes,
                                        enforcer,
                                        ctx.db,
                                        ctx.overlay(),
                                        ctx.to_t,
                                        &ctx.tracker,
                                    )
                                    .await?;
                                }
                            }
                            ActiveGraphs::Many(graphs) if graphs.len() == 1 => {
                                let graph = graphs[0];
                                // Prefer graph-level enforcer over context-level
                                let enforcer = graph
                                    .policy_enforcer
                                    .as_ref()
                                    .or(ctx.policy_enforcer.as_ref());
                                if let Some(enforcer) = enforcer {
                                    leaf_flakes = Self::apply_policy_to_leaf(
                                        leaf_flakes,
                                        enforcer,
                                        graph.db,
                                        graph.overlay,
                                        graph.to_t,
                                        &ctx.tracker,
                                    )
                                    .await?;
                                }
                            }
                            _ => {
                                // Multi-graph uses MultiChunked, shouldn't reach chunked mode
                            }
                        }

                        // Store the new leaf
                        if let ScanMode::Chunked { current_leaf, .. } = &mut self.scan_mode {
                            *current_leaf = VecDeque::from(leaf_flakes);
                        }
                    }
                    None => {
                        // Cursor exhausted
                        break;
                    }
                }
            }

            // Check if we're fully exhausted
            if produced == 0 {
                let leaf_empty = if let ScanMode::Chunked { current_leaf, .. } = &self.scan_mode {
                    current_leaf.is_empty()
                } else {
                    true
                };

                let cursor_exhausted = self.cursor.as_ref().map_or(true, |c| c.is_exhausted());

                if leaf_empty && cursor_exhausted {
                    self.state = OperatorState::Exhausted;
                }
            }
        } else if is_multi_chunked {
            // Multi-chunked mode: iterate through multiple graph cursors
            // Each graph is drained completely before moving to the next

            // Get the dataset's default graphs slice (non-allocating)
            let graphs = ctx.default_graphs_slice().ok_or_else(|| {
                QueryError::Internal("MultiChunked mode requires dataset".to_string())
            })?;

            'batch_loop: while produced < batch_size {
                // Try to get a flake from current leaf
                let maybe_flake =
                    if let ScanMode::MultiChunked { current_leaf, .. } = &mut self.scan_mode {
                        current_leaf.pop_front()
                    } else {
                        None
                    };

                if let Some(flake) = maybe_flake {
                    if self.should_include_flake(&flake, ctx)? {
                        ctx.tracker.consume_fuel_one()?;
                        self.push_flake_bindings_into_columns(flake, &mut columns, ctx);
                        produced += 1;
                    }
                    continue;
                }

                // Current leaf exhausted, try to load next leaf from current cursor
                // or advance to next cursor
                loop {
                    // Get current cursor state
                    let (cursor_idx, graph_idx, ledger_alias, effective_enforcer) = {
                        let (current_cursor_idx, cursors) = match &self.scan_mode {
                            ScanMode::MultiChunked {
                                current_cursor_idx,
                                cursors,
                                ..
                            } => (*current_cursor_idx, cursors),
                            _ => break 'batch_loop, // Shouldn't happen
                        };

                        if current_cursor_idx >= cursors.len() {
                            // All cursors exhausted
                            break 'batch_loop;
                        }

                        let cursor_state = &cursors[current_cursor_idx];
                        (
                            current_cursor_idx,
                            cursor_state.graph_idx,
                            cursor_state.ledger_alias.clone(),
                            cursor_state.effective_enforcer.clone(),
                        )
                    };

                    // Get the graph reference
                    let graph = &graphs[graph_idx];

                    // Get mutable cursor to call next_leaf
                    let next_leaf_result = {
                        if let ScanMode::MultiChunked { cursors, .. } = &mut self.scan_mode {
                            let cursor = &mut cursors[cursor_idx].cursor;
                            // No prefetch in multi-graph mode (would need per-graph prefetch db/overlay)
                            cursor
                                .next_leaf(graph.db, graph.overlay)
                                .await
                                .map_err(|e| {
                                    QueryError::Internal(format!(
                                        "Cursor error for graph '{}': {}",
                                        ledger_alias, e
                                    ))
                                })?
                        } else {
                            None
                        }
                    };

                    match next_leaf_result {
                        Some(mut leaf_flakes) => {
                            // Apply policy filtering if enforcer is present
                            if let Some(ref enforcer) = effective_enforcer {
                                leaf_flakes = Self::apply_policy_to_leaf(
                                    leaf_flakes,
                                    enforcer,
                                    graph.db,
                                    graph.overlay,
                                    graph.to_t,
                                    &ctx.tracker,
                                )
                                .await?;
                            }

                            // If filtering emptied the leaf, continue to next leaf
                            if leaf_flakes.is_empty() {
                                continue; // Try next leaf from same or next cursor
                            }

                            // Set ledger context for correct provenance in bindings
                            self.ledger_context = Some(LedgerContext {
                                alias: ledger_alias,
                            });

                            // Store the leaf and break inner loop to start draining
                            if let ScanMode::MultiChunked { current_leaf, .. } = &mut self.scan_mode
                            {
                                *current_leaf = VecDeque::from(leaf_flakes);
                            }
                            break; // Break inner loop, continue batch_loop
                        }
                        None => {
                            // Current cursor exhausted, advance to next
                            if let ScanMode::MultiChunked {
                                current_cursor_idx, ..
                            } = &mut self.scan_mode
                            {
                                *current_cursor_idx += 1;
                            }
                            // Continue inner loop to try next cursor
                        }
                    }
                }
            }

            // Check if we're fully exhausted
            let all_exhausted = if let ScanMode::MultiChunked {
                current_leaf,
                cursors,
                current_cursor_idx,
            } = &self.scan_mode
            {
                current_leaf.is_empty() && *current_cursor_idx >= cursors.len()
            } else {
                true
            };

            if produced == 0 && all_exhausted {
                self.state = OperatorState::Exhausted;
            }
        }

        // Track batch time
        self.batch_time_us += batch_start.elapsed().as_micros() as u64;

        if produced == 0 {
            return Ok(None);
        }

        // Create batch
        if self.schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }

        let batch = Batch::new(self.schema.clone(), columns)?;
        Ok(Some(batch))
    }

    fn close(&mut self) {
        // Aggregate timing into global counters
        if let Some(start) = self.open_time.take() {
            let total_us = start.elapsed().as_micros() as u64;
            SCAN_TOTAL_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
            SCAN_TOTAL_TIME_US.fetch_add(total_us, AtomicOrdering::Relaxed);
            SCAN_BATCH_TIME_US.fetch_add(self.batch_time_us, AtomicOrdering::Relaxed);
        }

        self.cursor = None;
        self.scan_mode = ScanMode::Uninitialized;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    impl ScanOperator {
        /// Test-only method for single-ledger mode binding creation
        /// Uses plain Sid bindings without context (backward-compatible with old behavior)
        fn push_flake_bindings_into_columns_test(
            &self,
            flake: Flake,
            columns: &mut [Vec<Binding>],
        ) {
            let mut col_idx = 0;

            if self.s_var_pos.is_some() {
                columns[col_idx].push(Binding::Sid(flake.s.clone()));
                col_idx += 1;
            }

            if self.p_var_pos.is_some() {
                columns[col_idx].push(Binding::Sid(flake.p.clone()));
                col_idx += 1;
            }

            if self.o_var_pos.is_some() {
                let binding = match flake.o {
                    FlakeValue::Ref(ref sid) => Binding::Sid(sid.clone()),
                    other => Binding::Lit {
                        val: other,
                        dt: flake.dt,
                        lang: flake.m.and_then(|m| m.lang.map(Arc::from)),
                        t: Some(flake.t),
                        op: if self.history_mode {
                            Some(flake.op)
                        } else {
                            None
                        },
                    },
                };
                columns[col_idx].push(binding);
            }
        }
    }

    fn test_pattern() -> TriplePattern {
        TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        )
    }

    #[test]
    fn test_scan_operator_schema() {
        let pattern = test_pattern();
        let scan = ScanOperator::new(pattern);

        assert_eq!(scan.schema().len(), 2);
        assert_eq!(scan.schema()[0], VarId(0));
        assert_eq!(scan.schema()[1], VarId(1));
    }

    #[test]
    fn test_scan_operator_index_selection() {
        // Pattern with bound subject -> SPOT
        let pattern = TriplePattern::new(
            Term::Sid(Sid::new(1, "alice")),
            Term::Var(VarId(0)),
            Term::Var(VarId(1)),
        );
        let scan = ScanOperator::new(pattern);
        assert_eq!(scan.index, IndexType::Spot);

        // Pattern with bound predicate, unbound object -> PSOT (property-join)
        let pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );
        let scan = ScanOperator::new(pattern);
        assert_eq!(scan.index, IndexType::Psot);

        // Pattern with bound ref object -> OPST
        let pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Var(VarId(1)),
            Term::Sid(Sid::new(1, "target")),
        );
        let scan = ScanOperator::new(pattern);
        assert_eq!(scan.index, IndexType::Opst);
    }

    #[test]
    fn test_flake_to_bindings() {
        let pattern = test_pattern();
        let scan = ScanOperator::new(pattern);

        let flake = Flake::new(
            Sid::new(1, "alice"),
            Sid::new(100, "name"),
            FlakeValue::String("Alice".to_string()),
            Sid::new(2, "string"),
            1,
            true,
            None,
        );

        let mut columns: Vec<Vec<Binding>> = (0..scan.schema.len()).map(|_| Vec::new()).collect();
        scan.push_flake_bindings_into_columns_test(flake, &mut columns);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].len(), 1);
        assert_eq!(columns[1].len(), 1);
        let bindings = vec![columns[0][0].clone(), columns[1][0].clone()];

        // First binding is subject (Sid)
        assert!(bindings[0].is_sid());
        assert_eq!(bindings[0].as_sid().unwrap().name.as_ref(), "alice");

        // Second binding is object (Lit with string value)
        assert!(bindings[1].is_lit());
        let (val, dt, _) = bindings[1].as_lit().unwrap();
        assert_eq!(*val, FlakeValue::String("Alice".to_string()));
        assert_eq!(dt.name.as_ref(), "string");
    }

    #[test]
    fn test_flake_to_bindings_ref_object() {
        // Pattern where object is a variable
        let pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "knows")),
            Term::Var(VarId(1)),
        );
        let scan = ScanOperator::new(pattern);

        // Flake with ref object
        let flake = Flake::new(
            Sid::new(1, "alice"),
            Sid::new(100, "knows"),
            FlakeValue::Ref(Sid::new(2, "bob")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        );

        let mut columns: Vec<Vec<Binding>> = (0..scan.schema.len()).map(|_| Vec::new()).collect();
        scan.push_flake_bindings_into_columns_test(flake, &mut columns);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].len(), 1);
        assert_eq!(columns[1].len(), 1);
        let bindings = vec![columns[0][0].clone(), columns[1][0].clone()];

        // Object binding should be Sid (not Lit) for ref values
        assert!(bindings[1].is_sid());
        assert_eq!(bindings[1].as_sid().unwrap().name.as_ref(), "bob");
    }
}
