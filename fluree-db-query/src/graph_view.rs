//! Composable graph view abstractions
//!
//! This module provides a composable wrapper pattern for graph views that mirrors
//! Clojure's approach (independent wrappers for policy, time travel, reasoning)
//! while maintaining Rust performance (resolution happens once per graph/leaf,
//! not per flake).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Wrapper Composition                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │   Graph A: WithPolicy(AsOf(BaseView))                          │
//! │   Graph B: AsOf(WithPolicy(BaseView))                          │
//! │   Graph C: WithReasoning(WithPolicy(AsOf(BaseView)))           │
//! │                                                                 │
//! │   All resolve() to → ResolvedGraphView                         │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Types
//!
//! - [`ResolvedGraphView`]: POD-like struct used by operators in hot loops (no clones)
//! - [`GraphView`]: Object-safe trait for composable view wrappers
//! - [`BaseView`]: Raw db + overlay + time
//! - [`AsOf`]: Time travel wrapper (overrides `to_t`)
//! - [`WithPolicy`]: Policy wrapper (attaches enforcer)
//! - [`WithReasoning`]: Reasoning wrapper (overrides overlay)
//!
//! # Object Safety
//!
//! The `GraphView` trait is object-safe (with concrete `S`), enabling heterogeneous
//! collections of views via `Arc<dyn GraphView<S>>`.
//!
//! # Performance
//!
//! `ResolvedGraphView` uses references, not owned `Arc`s, to avoid clones on the
//! hot path. Call `resolve()` once per graph/leaf, then use the resolved view.
//!
//! # Usage
//!
//! Create a `BaseView`, optionally wrap with `AsOf`, `WithPolicy`, or `WithReasoning`,
//! then call `resolve()` to obtain a `ResolvedGraphView` for use in operators.

use crate::policy::QueryPolicyEnforcer;
use fluree_db_core::{Db, OverlayProvider, Storage};
use std::sync::Arc;

// ============================================================================
// ResolvedGraphView - the resolved POD used by operators
// ============================================================================

/// Resolved graph view - POD-like struct for operator hot paths.
///
/// This is what operators (ScanOperator, history, etc.) use after resolving
/// a graph view. It contains all the fields needed for scanning and filtering,
/// already resolved from whatever wrapper stack was applied.
///
/// # Performance
///
/// Uses **references only** - no Arc clones on resolve(). This is critical for
/// hot paths. Resolution should happen once per graph/leaf, not per flake.
///
/// # Lifetime
///
/// The lifetime `'a` ties this view to the underlying `GraphView` it was
/// resolved from. The resolved view is valid as long as the source view exists.
#[derive(Copy, Clone)]
pub struct ResolvedGraphView<'a, S: Storage> {
    /// The database for this graph
    pub db: &'a Db<S>,
    /// Overlay provider (novelty layer, possibly with reasoning)
    pub overlay: &'a dyn OverlayProvider,
    /// Target transaction time (possibly overridden by AsOf)
    pub to_t: i64,
    /// Optional policy enforcer (attached by WithPolicy) - reference, no clone
    pub policy_enforcer: Option<&'a QueryPolicyEnforcer>,
    /// Ledger alias for provenance tracking - reference to Arc, no clone
    pub ledger_alias: &'a Arc<str>,
}

impl<'a, S: Storage> ResolvedGraphView<'a, S> {
    /// Create a new resolved view with no policy
    pub fn new(
        db: &'a Db<S>,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        ledger_alias: &'a Arc<str>,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            policy_enforcer: None,
            ledger_alias,
        }
    }

    /// Create a resolved view with policy
    pub fn with_policy(
        db: &'a Db<S>,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        ledger_alias: &'a Arc<str>,
        policy_enforcer: &'a QueryPolicyEnforcer,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            policy_enforcer: Some(policy_enforcer),
            ledger_alias,
        }
    }

    /// Check if this view has an active (non-root) policy
    pub fn has_policy(&self) -> bool {
        self.policy_enforcer.map(|e| !e.is_root()).unwrap_or(false)
    }
}

impl<'a, S: Storage> std::fmt::Debug for ResolvedGraphView<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedGraphView")
            .field("db", &"<Db>")
            .field("overlay", &"<dyn OverlayProvider>")
            .field("to_t", &self.to_t)
            .field("has_policy", &self.policy_enforcer.is_some())
            .field("ledger_alias", self.ledger_alias)
            .finish()
    }
}

// ============================================================================
// GraphView trait - object-safe composable view abstraction
// ============================================================================

/// Object-safe trait for composable graph view wrappers.
///
/// This enables the Clojure-style wrapper composition pattern where policy,
/// time travel, and reasoning are independent wrappers that can be applied
/// in any order.
///
/// # Object Safety
///
/// The trait is object-safe when `S` and `C` are concrete, which allows
/// `DataSet` to store heterogeneous wrapper stacks via `Arc<dyn GraphView<S>>`.
///
/// # Performance
///
/// `resolve()` returns a view with **references only** - no allocations.
/// Call it once per graph/leaf, then use the resolved view.
pub trait GraphView<S: Storage> {
    /// Resolve this view to a [`ResolvedGraphView`] for use by operators.
    ///
    /// This should be called **once per graph/leaf**, not per flake.
    /// The returned view borrows from `self` and contains no owned data.
    fn resolve(&self) -> ResolvedGraphView<'_, S>;

    /// Get the ledger alias for this view (for provenance tracking).
    fn ledger_alias(&self) -> &Arc<str>;
}

// ============================================================================
// BaseView - raw db + overlay + time
// ============================================================================

/// Base graph view - holds raw db/overlay/to_t references.
///
/// This is the foundation for all wrapper compositions. It holds the
/// fundamental components of a graph view without any transformations.
///
/// # Important: `to_t` must be passed explicitly
///
/// Do **not** use `db.t` as `to_t` when you have a novelty overlay - that
/// ignores unindexed commits. Use `ledger.t()` (which is `max(db.t, novelty.t)`)
/// or pass the correct time explicitly.
pub struct BaseView<'a, S: Storage> {
    /// The database
    pub db: &'a Db<S>,
    /// The overlay provider (novelty)
    pub overlay: &'a dyn OverlayProvider,
    /// Target transaction time - must include novelty commits if present
    pub to_t: i64,
    /// Ledger alias for provenance tracking
    pub ledger_alias: Arc<str>,
}

impl<'a, S: Storage> BaseView<'a, S> {
    /// Create a new base view.
    ///
    /// # Arguments
    ///
    /// * `db` - The database
    /// * `overlay` - The overlay provider (novelty)
    /// * `to_t` - Target transaction time. **Important**: If using a novelty
    ///   overlay, this should be `ledger.t()` (not `db.t`) to include
    ///   unindexed commits.
    /// * `ledger_alias` - Ledger alias for provenance tracking
    pub fn new(
        db: &'a Db<S>,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        ledger_alias: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            ledger_alias: ledger_alias.into(),
        }
    }
}

impl<'a, S: Storage> GraphView<S> for BaseView<'a, S> {
    fn resolve(&self) -> ResolvedGraphView<'_, S> {
        ResolvedGraphView::new(self.db, self.overlay, self.to_t, &self.ledger_alias)
    }

    fn ledger_alias(&self) -> &Arc<str> {
        &self.ledger_alias
    }
}

// ============================================================================
// AsOf - time travel wrapper
// ============================================================================

/// Time travel wrapper - overrides the target transaction time.
///
/// This wrapper changes the `to_t` used for scanning without modifying
/// the underlying database or overlay.
///
/// Wrap any `GraphView` with `AsOf::new(inner, earlier_t)` to override its `to_t`.
pub struct AsOf<V> {
    /// The inner view being wrapped
    inner: V,
    /// The overridden transaction time
    to_t: i64,
}

impl<V> AsOf<V> {
    /// Create a time travel wrapper with a specific `to_t`.
    pub fn new(inner: V, to_t: i64) -> Self {
        Self { inner, to_t }
    }

    /// Get the overridden transaction time.
    pub fn to_t(&self) -> i64 {
        self.to_t
    }

    /// Get a reference to the inner view.
    pub fn inner(&self) -> &V {
        &self.inner
    }
}

impl<S: Storage, V: GraphView<S>> GraphView<S> for AsOf<V> {
    fn resolve(&self) -> ResolvedGraphView<'_, S> {
        let mut resolved = self.inner.resolve();
        resolved.to_t = self.to_t;
        resolved
    }

    fn ledger_alias(&self) -> &Arc<str> {
        self.inner.ledger_alias()
    }
}

// ============================================================================
// WithPolicy - policy wrapper
// ============================================================================

/// Policy wrapper - attaches a policy enforcer to the view.
///
/// This wrapper adds policy enforcement without modifying db/overlay/to_t.
/// The policy enforcer will evaluate queries against the same snapshot
/// that produces the flakes.
///
/// Wrap any `GraphView` with `WithPolicy::new(inner, &enforcer)` to attach policy enforcement.
pub struct WithPolicy<'a, V> {
    /// The inner view being wrapped
    inner: V,
    /// The policy enforcer (reference to Arc, stored externally)
    enforcer: &'a QueryPolicyEnforcer,
}

impl<'a, V> WithPolicy<'a, V> {
    /// Create a policy wrapper with an enforcer reference.
    ///
    /// The enforcer is borrowed, not cloned - the caller must ensure the
    /// enforcer outlives this wrapper.
    pub fn new(inner: V, enforcer: &'a QueryPolicyEnforcer) -> Self {
        Self { inner, enforcer }
    }

    /// Get the policy enforcer.
    pub fn enforcer(&self) -> &QueryPolicyEnforcer {
        self.enforcer
    }

    /// Get a reference to the inner view.
    pub fn inner(&self) -> &V {
        &self.inner
    }
}

impl<'a, S: Storage, V: GraphView<S>> GraphView<S> for WithPolicy<'a, V> {
    fn resolve(&self) -> ResolvedGraphView<'_, S> {
        let mut resolved = self.inner.resolve();
        resolved.policy_enforcer = Some(self.enforcer);
        resolved
    }

    fn ledger_alias(&self) -> &Arc<str> {
        self.inner.ledger_alias()
    }
}

// ============================================================================
// WithReasoning - reasoning overlay wrapper
// ============================================================================

/// Reasoning wrapper - overrides the overlay with a reasoning overlay.
///
/// This wrapper changes the overlay to include derived facts from reasoning
/// (e.g., OWL inference). The derived facts are layered on top of the
/// base overlay.
///
/// Wrap any `GraphView` with `WithReasoning::new(inner, &reasoning_overlay)` to include derived facts in the overlay.
pub struct WithReasoning<'a, V> {
    /// The inner view being wrapped
    inner: V,
    /// The reasoning overlay (includes derived facts)
    overlay: &'a dyn OverlayProvider,
}

impl<'a, V> WithReasoning<'a, V> {
    /// Create a reasoning wrapper with an overlay that includes derived facts.
    pub fn new(inner: V, overlay: &'a dyn OverlayProvider) -> Self {
        Self { inner, overlay }
    }

    /// Get a reference to the inner view.
    pub fn inner(&self) -> &V {
        &self.inner
    }
}

impl<'a, S: Storage, V: GraphView<S>> GraphView<S> for WithReasoning<'a, V> {
    fn resolve(&self) -> ResolvedGraphView<'_, S> {
        let mut resolved = self.inner.resolve();
        resolved.overlay = self.overlay;
        resolved
    }

    fn ledger_alias(&self) -> &Arc<str> {
        self.inner.ledger_alias()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Verify object-safety: the trait can be used as a trait object
    fn _assert_object_safe<S: Storage + 'static>(_: &dyn GraphView<S>) {}

    #[test]
    fn test_wrapper_composition_compiles() {
        // This test just verifies that the wrapper composition types compile
        // and the trait bounds are correct.
        fn _assert_graph_view<V: GraphView<fluree_db_core::MemoryStorage>>(_: &V) {}

        // Object-safety check (doesn't run, just compiles)
        fn _check_dyn<S: Storage + 'static>(v: &dyn GraphView<S>) {
            let _resolved = v.resolve();
            let _alias = v.ledger_alias();
        }
    }
}
