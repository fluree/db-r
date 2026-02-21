//! Core view types
//!
//! Defines `GraphDb`, `ReasoningModePrecedence`, and `DerivedFactsHandle`.

use std::sync::Arc;

use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::ids::GraphId;
use fluree_db_core::{LedgerSnapshot, NoOverlay, OverlayProvider};
use fluree_db_indexer::run_index::{BinaryIndexStore, GraphView};
use fluree_db_ledger::{HistoricalLedgerView, LedgerState};
use fluree_db_novelty::Novelty;
use fluree_db_policy::PolicyContext;
use fluree_db_query::policy::QueryPolicyEnforcer;
use fluree_db_query::rewrite::ReasoningModes;

/// How wrapper-provided reasoning interacts with query-specified reasoning.
///
/// Controls the precedence when both the view wrapper and the query itself
/// specify reasoning modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReasoningModePrecedence {
    /// Use wrapper modes only if query doesn't specify reasoning.
    ///
    /// This is the default, matching Clojure's ergonomic defaults:
    /// the wrapper provides convenient defaults, but power users can
    /// override via query JSON.
    #[default]
    DefaultUnlessQueryOverrides,

    /// Always use wrapper modes, ignoring query-specified reasoning.
    ///
    /// Use this for server-policy enforcement where reasoning modes
    /// should not be overridable by clients.
    Force,
}

/// A first-class, composable view of a ledger snapshot.
///
/// This is the "db value" you pass to queries. It owns all pieces needed
/// to execute queries without lifetime complexity.
///
/// # Type Parameters
///
/// - `S`: Storage backend (e.g., `FileStorage`, `MemoryStorage`)
///
/// # Composition
///
/// Views support Clojure-style wrapper composition via builder methods:
///
/// ```ignore
/// let view = GraphDb::from_ledger_state(&ledger)
///     .with_policy(policy)
///     .with_reasoning(ReasoningModes::owl2ql());
/// ```
///
/// Wrappers are order-independent; the same semantics apply regardless
/// of composition order.
///
/// # Clone Semantics
///
/// `GraphDb` is cheap to clone (all fields are `Arc`-wrapped or `Copy`).
/// Cloning a view creates a new handle to the same underlying data.
#[derive(Clone)]
pub struct GraphDb {
    // ========================================================================
    // Core components (required)
    // ========================================================================
    /// The indexed database snapshot.
    pub db: Arc<LedgerSnapshot>,

    /// Overlay provider for uncommitted/derived flakes.
    ///
    /// This is typically the novelty layer, but may be composed with
    /// derived facts overlays for reasoning.
    pub overlay: Arc<dyn OverlayProvider>,

    /// Time bound for all queries.
    ///
    /// Queries will only see flakes with `t <= to_t`.
    pub to_t: i64,

    /// Ledger ID (e.g., "mydb:main").
    pub ledger_id: Arc<str>,

    /// Graph ID within the ledger (0 = default graph).
    ///
    /// This is used to select the correct graph when querying a ledger that
    /// contains multiple named graphs (e.g., `txn-meta` at g_id=1).
    ///
    /// Note: This is *not* the same thing as a SPARQL "named graph IRI" — it is
    /// the internal numeric graph selector used by the binary indexes.
    pub graph_id: GraphId,

    // ========================================================================
    // Novelty (for policy stats and time resolution)
    // ========================================================================
    /// The concrete novelty overlay, if available.
    ///
    /// This is kept separately from `overlay` because:
    /// 1. Policy stats (`f:onClass`) need the concrete `Novelty` type
    /// 2. Time resolution functions need novelty for overlay queries
    ///
    /// For views created from `LedgerState`, this is always `Some`.
    /// For views created from `HistoricalLedgerView`, this mirrors
    /// `view.overlay()`.
    pub(crate) novelty: Option<Arc<Novelty>>,

    // ========================================================================
    // Policy wrapper (optional)
    // ========================================================================
    /// Policy context for access control.
    policy: Option<Arc<PolicyContext>>,

    /// Policy enforcer for async `f:query` support.
    policy_enforcer: Option<Arc<QueryPolicyEnforcer>>,

    // ========================================================================
    // Reasoning wrapper (optional)
    // ========================================================================
    /// Default reasoning modes to apply to queries.
    reasoning: Option<ReasoningModes>,

    /// Precedence for reasoning mode resolution.
    reasoning_precedence: ReasoningModePrecedence,

    // ========================================================================
    // Binary index store (optional, v2 only)
    // ========================================================================
    /// Binary columnar index store for `BinaryScanOperator`.
    ///
    /// When set, `ScanOperator` uses this for direct columnar scans
    /// via `BinaryScanOperator`.
    pub(crate) binary_store: Option<Arc<BinaryIndexStore>>,

    /// Dictionary novelty layer for binary scan subject/string lookups.
    pub(crate) dict_novelty: Option<Arc<DictNovelty>>,

    /// Default JSON-LD context for queries that don't provide their own.
    ///
    /// Populated from turtle `@prefix` declarations captured during import.
    /// When a query has no `@context`, this is injected automatically.
    pub default_context: Option<serde_json::Value>,
}

impl std::fmt::Debug for GraphDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphDb")
            .field("ledger_id", &self.ledger_id)
            .field("graph_id", &self.graph_id)
            .field("to_t", &self.to_t)
            .field("db_t", &self.db.t)
            .field("has_novelty", &self.novelty.is_some())
            .field("has_policy", &self.policy.is_some())
            .field("has_reasoning", &self.reasoning.is_some())
            .field("reasoning_precedence", &self.reasoning_precedence)
            .finish()
    }
}

// ============================================================================
// Constructors
// ============================================================================

impl GraphDb {
    /// Create a base view from components.
    ///
    /// This is the low-level constructor. Prefer `from_ledger_state` or
    /// `from_historical` for most use cases.
    ///
    /// # Arguments
    ///
    /// * `db` - The indexed database snapshot
    /// * `overlay` - Overlay provider for uncommitted flakes
    /// * `novelty` - Optional concrete novelty (for policy stats)
    /// * `to_t` - Time bound for queries
    /// * `ledger_id` - Ledger ID (e.g., "mydb:main")
    pub fn new(
        db: Arc<LedgerSnapshot>,
        overlay: Arc<dyn OverlayProvider>,
        novelty: Option<Arc<Novelty>>,
        to_t: i64,
        ledger_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            db,
            overlay,
            novelty,
            to_t,
            ledger_id: ledger_id.into(),
            graph_id: 0,
            policy: None,
            policy_enforcer: None,
            reasoning: None,
            reasoning_precedence: ReasoningModePrecedence::default(),
            binary_store: None,
            dict_novelty: None,
            default_context: None,
        }
    }

    /// Create a view from a `LedgerState` (head snapshot with novelty).
    ///
    /// This is the most common constructor for querying current ledger state.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ledger = fluree.ledger("mydb:main").await?;
    /// let view = GraphDb::from_ledger_state(&ledger);
    /// ```
    pub fn from_ledger_state(ledger: &LedgerState) -> Self {
        let novelty = ledger.novelty.clone();
        let mut view = Self::new(
            Arc::new(ledger.db.clone()),
            novelty.clone() as Arc<dyn OverlayProvider>,
            Some(novelty),
            ledger.t(),
            ledger.ledger_id(),
        );
        view.dict_novelty = Some(ledger.dict_novelty.clone());
        // Extract binary_store from LedgerState's TypeErasedStore
        view.binary_store = ledger
            .binary_store
            .as_ref()
            .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());
        view.default_context = ledger.default_context.clone();
        view
    }

    /// Create a view from a `HistoricalLedgerView` (time-travel snapshot).
    ///
    /// Use this for querying ledger state at a specific point in time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let historical = fluree.ledger_view_at("mydb:main", 50).await?;
    /// let view = GraphDb::from_historical(&historical);
    /// ```
    pub fn from_historical(view: &HistoricalLedgerView) -> Self {
        let has_overlay = view.overlay().is_some();
        tracing::trace!(to_t = view.to_t(), has_overlay, "GraphDb::from_historical");
        let (overlay, novelty): (Arc<dyn OverlayProvider>, Option<Arc<Novelty>>) =
            match view.overlay() {
                Some(nov) => (nov.clone() as Arc<dyn OverlayProvider>, Some(nov.clone())),
                None => (Arc::new(NoOverlay) as Arc<dyn OverlayProvider>, None),
            };

        Self::new(
            Arc::new(view.db.clone()),
            overlay,
            novelty,
            view.to_t(),
            view.db.ledger_id.as_str(),
        )
    }
}

impl GraphDb {
    /// Create a view from a [`Staged`](crate::tx_builder::Staged) transaction
    /// that includes the staged (uncommitted) changes.
    ///
    /// The returned view merges base novelty with staged flakes, so queries
    /// against it will see the effect of the staged transaction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let staged = fluree.stage_owned(ledger).insert(&data).stage().await?;
    /// let preview = GraphDb::from_staged(&staged)?;
    /// let result = preview.query(&fluree).jsonld(&q).execute().await?;
    /// ```
    pub fn from_staged(
        staged: &crate::tx_builder::Staged,
    ) -> std::result::Result<Self, crate::ApiError> {
        let base = staged.view.base();
        let staged_t = base.t() + 1;

        // Clone base novelty and merge staged flakes into it so queries see
        // both committed and staged data.
        let mut combined = (*base.novelty).clone();
        let staged_flakes = staged.view.staged_flakes().to_vec();
        if !staged_flakes.is_empty() {
            combined
                .apply_commit(staged_flakes, staged_t)
                .map_err(|e| {
                    crate::ApiError::internal(format!(
                        "Failed to merge staged flakes into novelty: {}",
                        e
                    ))
                })?;
        }

        let combined = Arc::new(combined);
        let mut view = Self::new(
            Arc::new(base.db.clone()),
            combined.clone() as Arc<dyn OverlayProvider>,
            Some(combined),
            staged_t,
            base.ledger_id(),
        );
        view.dict_novelty = Some(base.dict_novelty.clone());
        Ok(view)
    }

    /// Create a view from the **base** (pre-transaction) state of a
    /// [`Staged`](crate::tx_builder::Staged) transaction.
    ///
    /// Unlike [`from_staged`](Self::from_staged), this does **not** include
    /// the staged changes — useful for comparison queries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let staged = fluree.stage_owned(ledger).insert(&data).stage().await?;
    /// let before = GraphDb::from_staged_base(&staged);
    /// let after  = GraphDb::from_staged(&staged);
    /// ```
    pub fn from_staged_base(staged: &crate::tx_builder::Staged) -> Self {
        let base = staged.view.base();
        let novelty = base.novelty.clone();
        Self::new(
            Arc::new(base.db.clone()),
            novelty.clone() as Arc<dyn OverlayProvider>,
            Some(novelty),
            base.t(),
            base.ledger_id(),
        )
    }
}

// ============================================================================
// Time Travel
// ============================================================================

impl GraphDb {
    /// Adjust the view's time bound.
    ///
    /// **Important**: This only adjusts the `to_t` filter; it doesn't reload
    /// the underlying index. For proper historical queries with index pruning,
    /// construct the view from `HistoricalLedgerView` instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to only see flakes at t <= 50
    /// let view = view.as_of(50);
    /// ```
    pub fn as_of(mut self, to_t: i64) -> Self {
        self.to_t = to_t;
        self
    }
}

// ============================================================================
// Graph selection
// ============================================================================

impl GraphDb {
    /// Select a graph ID within this ledger view.
    ///
    /// This does **not** reload the underlying ledger; it only adjusts the
    /// internal graph selector used by binary scans. Callers that rely on
    /// `range_with_overlay()` must ensure the underlying `LedgerSnapshot.range_provider`
    /// is scoped appropriately for the chosen graph.
    pub fn with_graph_id(mut self, graph_id: GraphId) -> Self {
        self.graph_id = graph_id;
        self
    }
}

// ============================================================================
// Binary Store
// ============================================================================

impl GraphDb {
    /// Attach a binary index store for `BinaryScanOperator`.
    pub fn with_binary_store(mut self, store: Arc<BinaryIndexStore>) -> Self {
        self.binary_store = Some(store);
        self
    }

    /// Get the binary index store (if any).
    pub fn binary_store(&self) -> Option<&Arc<BinaryIndexStore>> {
        self.binary_store.as_ref()
    }

    /// Build a `GraphView` combining the binary store with the view's graph ID.
    ///
    /// Returns `None` if no binary store is attached.
    pub fn binary_graph(&self) -> Option<GraphView> {
        self.binary_store
            .as_ref()
            .map(|store| GraphView::new(store.clone(), self.graph_id))
    }
}

// ============================================================================
// Policy Wrapper
// ============================================================================

impl GraphDb {
    /// Attach a policy context to the view.
    ///
    /// Policy is enforced during query execution and result formatting.
    /// This mirrors Clojure's `wrap-policy` / `policy-enforce-db`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let policy = build_policy_context_from_opts(&db, overlay, novelty, t, &opts).await?;
    /// let view = view.with_policy(Arc::new(policy));
    /// ```
    pub fn with_policy(mut self, policy: Arc<PolicyContext>) -> Self {
        let enforcer = Arc::new(QueryPolicyEnforcer::new(Arc::clone(&policy)));
        self.policy = Some(policy);
        self.policy_enforcer = Some(enforcer);
        self
    }

    /// Remove policy enforcement from the view.
    ///
    /// Returns a new view that executes queries without policy checks.
    pub fn without_policy(mut self) -> Self {
        self.policy = None;
        self.policy_enforcer = None;
        self
    }

    /// Check if policy is attached to this view.
    pub fn has_policy(&self) -> bool {
        self.policy.is_some()
    }

    /// Get the policy context (if any).
    pub fn policy(&self) -> Option<&PolicyContext> {
        self.policy.as_deref()
    }

    /// Get the policy enforcer (if any).
    ///
    /// Used internally by query execution for `f:query` policy support.
    pub fn policy_enforcer(&self) -> Option<&Arc<QueryPolicyEnforcer>> {
        self.policy_enforcer.as_ref()
    }

    /// Check if this is a root/unrestricted policy view.
    ///
    /// Returns `true` if no policy is attached, or if the policy is root.
    pub fn is_root(&self) -> bool {
        match &self.policy_enforcer {
            Some(enforcer) => enforcer.is_root(),
            None => true,
        }
    }
}

// ============================================================================
// Reasoning Wrapper
// ============================================================================

impl GraphDb {
    /// Apply default reasoning modes to queries on this view.
    ///
    /// This mirrors Clojure's `wrap-reasoning`. The reasoning modes apply
    /// to all queries executed against this view, subject to precedence rules.
    ///
    /// Uses `DefaultUnlessQueryOverrides` precedence by default.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = view.with_reasoning(ReasoningModes::owl2ql());
    /// ```
    pub fn with_reasoning(mut self, modes: ReasoningModes) -> Self {
        self.reasoning = Some(modes);
        self.reasoning_precedence = ReasoningModePrecedence::DefaultUnlessQueryOverrides;
        self
    }

    /// Apply reasoning modes with explicit precedence.
    ///
    /// # Arguments
    ///
    /// * `modes` - The reasoning modes to apply
    /// * `precedence` - How to resolve conflicts with query-specified reasoning
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Force OWL2-RL reasoning even if queries specify different modes
    /// let view = view.with_reasoning_precedence(
    ///     ReasoningModes::default().with_owl2rl(),
    ///     ReasoningModePrecedence::Force,
    /// );
    /// ```
    pub fn with_reasoning_precedence(
        mut self,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> Self {
        self.reasoning = Some(modes);
        self.reasoning_precedence = precedence;
        self
    }

    /// Remove reasoning defaults from the view.
    ///
    /// Queries will use their own reasoning modes (or auto-RDFS).
    pub fn without_reasoning(mut self) -> Self {
        self.reasoning = None;
        self
    }

    /// Get the wrapper's reasoning modes (if any).
    pub fn reasoning(&self) -> Option<&ReasoningModes> {
        self.reasoning.as_ref()
    }

    /// Get the reasoning precedence mode.
    pub fn reasoning_precedence(&self) -> ReasoningModePrecedence {
        self.reasoning_precedence
    }

    /// Compute effective reasoning modes for query execution.
    ///
    /// This applies precedence rules to merge wrapper and query reasoning:
    ///
    /// - `DefaultUnlessQueryOverrides`: Query modes win if the query has any
    ///   reasoning enabled or explicitly disabled
    /// - `Force`: Wrapper modes always win
    ///
    /// Returns `None` if no reasoning should be applied (let query engine
    /// use its auto-RDFS behavior).
    pub fn effective_reasoning(
        &self,
        query_has_reasoning: bool,
        query_reasoning_disabled: bool,
    ) -> Option<&ReasoningModes> {
        match self.reasoning_precedence {
            ReasoningModePrecedence::Force => {
                // Wrapper always wins
                self.reasoning.as_ref()
            }
            ReasoningModePrecedence::DefaultUnlessQueryOverrides => {
                // Query wins if it specifies anything
                if query_has_reasoning || query_reasoning_disabled {
                    None // Let the query's reasoning take effect
                } else {
                    self.reasoning.as_ref()
                }
            }
        }
    }
}

// ============================================================================
// Accessors
// ============================================================================

impl GraphDb {
    /// Get the concrete novelty overlay (if available).
    ///
    /// This is needed for policy stats (`f:onClass`) and some time resolution
    /// paths. Returns `None` for views without novelty (e.g., pure historical
    /// views at an indexed point).
    pub fn novelty(&self) -> Option<&Arc<Novelty>> {
        self.novelty.as_ref()
    }

    /// Get the novelty as a reference for passing to policy building.
    ///
    /// Returns `Some(&Novelty)` if novelty is available, `None` otherwise.
    pub fn novelty_for_stats(&self) -> Option<&Novelty> {
        self.novelty.as_ref().map(|n| n.as_ref())
    }
}

// ============================================================================
// DerivedFactsHandle (Phase 2 - optional)
// ============================================================================

/// A cached/materialized derived-facts overlay handle.
///
/// This enables "compute once, reuse across many queries" for OWL2-RL
/// materialization and datalog rules.
///
/// # Usage
///
/// ```ignore
/// // Materialize derived facts once
/// let derived = fluree.materialize_derived_facts(&view, modes).await?;
///
/// // Reuse across multiple queries
/// let view1 = view.clone().with_derived_facts(derived.clone());
/// let view2 = view.clone().with_derived_facts(derived.clone());
/// ```
#[derive(Clone)]
pub struct DerivedFactsHandle {
    /// The derived facts overlay.
    pub overlay: Arc<dyn OverlayProvider>,
    /// The reasoning modes used to compute these facts.
    pub modes: ReasoningModes,
    /// The time bound at which facts were computed.
    pub to_t: i64,
}

impl std::fmt::Debug for DerivedFactsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DerivedFactsHandle")
            .field("modes", &self.modes)
            .field("to_t", &self.to_t)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_db() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test:main")
    }

    #[test]
    fn test_view_new() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main");

        assert_eq!(view.to_t, 5);
        assert_eq!(&*view.ledger_id, "test:main");
        assert!(view.novelty().is_some());
        assert!(!view.has_policy());
        assert!(view.reasoning().is_none());
    }

    #[test]
    fn test_view_as_of() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 10, "test:main");
        assert_eq!(view.to_t, 10);

        let view = view.as_of(5);
        assert_eq!(view.to_t, 5);
    }

    #[test]
    fn test_view_with_reasoning() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main");
        assert!(view.reasoning().is_none());

        let view = view.with_reasoning(ReasoningModes::owl2ql());
        assert!(view.reasoning().is_some());
        assert!(view.reasoning().unwrap().owl2ql);
        assert_eq!(
            view.reasoning_precedence(),
            ReasoningModePrecedence::DefaultUnlessQueryOverrides
        );

        let view = view.without_reasoning();
        assert!(view.reasoning().is_none());
    }

    #[test]
    fn test_view_with_reasoning_precedence() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main");

        let view = view.with_reasoning_precedence(
            ReasoningModes::default().with_owl2rl(),
            ReasoningModePrecedence::Force,
        );

        assert_eq!(view.reasoning_precedence(), ReasoningModePrecedence::Force);
    }

    #[test]
    fn test_effective_reasoning_default_precedence() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main")
            .with_reasoning(ReasoningModes::owl2ql());

        // No query reasoning: wrapper wins
        assert!(view.effective_reasoning(false, false).is_some());

        // Query has reasoning: query wins
        assert!(view.effective_reasoning(true, false).is_none());

        // Query explicitly disabled: query wins
        assert!(view.effective_reasoning(false, true).is_none());
    }

    #[test]
    fn test_effective_reasoning_force_precedence() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main")
            .with_reasoning_precedence(ReasoningModes::owl2ql(), ReasoningModePrecedence::Force);

        // Force: wrapper always wins
        assert!(view.effective_reasoning(false, false).is_some());
        assert!(view.effective_reasoning(true, false).is_some());
        assert!(view.effective_reasoning(false, true).is_some());
    }

    #[test]
    fn test_view_is_root() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main");

        // No policy = root
        assert!(view.is_root());
    }

    #[test]
    fn test_view_debug() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main");
        let debug = format!("{:?}", view);

        assert!(debug.contains("GraphDb"));
        assert!(debug.contains("test:main"));
        assert!(debug.contains("to_t: 5"));
    }

    #[test]
    fn test_view_clone() {
        let db = make_test_db();
        let novelty = Arc::new(Novelty::new(0));
        let overlay = novelty.clone() as Arc<dyn OverlayProvider>;

        let view1 = GraphDb::new(Arc::new(db), overlay, Some(novelty), 5, "test:main")
            .with_reasoning(ReasoningModes::rdfs());

        let view2 = view1.clone();

        assert_eq!(view1.to_t, view2.to_t);
        assert_eq!(&*view1.ledger_id, &*view2.ledger_id);
        assert!(view2.reasoning().is_some());
    }
}
