//! Policy enforcer for batch filtering
//!
//! Provides the `QueryPolicyEnforcer` which filters flakes by policy with caching.

use super::QueryPolicyExecutor;
use crate::error::Result;
use crate::graph_view::ResolvedGraphView;
use fluree_db_core::{Db, Flake, OverlayProvider, Storage, Tracker};
use fluree_db_policy::{is_schema_flake, PolicyContext};
use std::sync::Arc;

/// Policy enforcer for query execution
///
/// Wraps a `PolicyContext` and provides async batch filtering for flakes.
/// Designed to be used by scan operators for per-leaf filtering.
///
/// # Caching (TODO)
///
/// Future versions will cache f:query results to avoid re-executing
/// the same policy query for every flake.
#[derive(Clone)]
pub struct QueryPolicyEnforcer {
    /// The policy context containing restrictions and identity
    policy: Arc<PolicyContext>,
    // TODO: Add PolicyQueryCache for memoization
    // cache: Arc<PolicyQueryCache>,
}

impl QueryPolicyEnforcer {
    /// Create a new policy enforcer
    pub fn new(policy: Arc<PolicyContext>) -> Self {
        Self { policy }
    }

    /// Get the underlying policy context
    pub fn policy(&self) -> &PolicyContext {
        &self.policy
    }

    /// Check if this is a root policy (bypasses all checks)
    pub fn is_root(&self) -> bool {
        self.policy.wrapper().is_root()
    }

    /// Filter a batch of flakes by policy using explicit graph parameters.
    ///
    /// This is the **correct** method for dataset mode - it uses the graph's
    /// db/overlay/to_t, ensuring `f:query` policies run against the same
    /// snapshot that produced the flakes.
    ///
    /// # Arguments
    ///
    /// * `db` - The database for this graph
    /// * `overlay` - The overlay provider for this graph
    /// * `to_t` - Target transaction time for this graph
    /// * `tracker` - Fuel tracker for limits
    /// * `flakes` - Flakes to filter
    ///
    /// # Returns
    ///
    /// Filtered flakes that pass policy checks
    pub async fn filter_flakes_for_graph<S: Storage + 'static>(
        &self,
        db: &Db<S>,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        tracker: &Tracker,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        let span = tracing::debug_span!(
            "policy_eval",
            input_flakes = flakes.len(),
            allowed_flakes = tracing::field::Empty,
        );
        let _guard = span.enter();

        // Root policy bypasses all checks
        if self.policy.wrapper().is_root() {
            span.record("allowed_flakes", flakes.len());
            return Ok(flakes);
        }

        // Create executor using the GRAPH's db/overlay/to_t (not ctx-level!)
        let executor = QueryPolicyExecutor::with_overlay(db, overlay, to_t);

        let mut result = Vec::with_capacity(flakes.len());

        for flake in flakes {
            // Schema flakes always allowed
            if is_schema_flake(&flake.p, &flake.o) {
                result.push(flake);
                continue;
            }

            // Get subject classes from cache
            let subject_classes = self
                .policy
                .get_cached_subject_classes(&flake.s)
                .unwrap_or_default();

            // Async policy check with f:query support
            match self
                .policy
                .allow_view_flake_async(
                    &flake.s,
                    &flake.p,
                    &flake.o,
                    &subject_classes,
                    &executor,
                    tracker,
                )
                .await
            {
                Ok(true) => result.push(flake),
                Ok(false) => {} // Filtered out
                Err(_) => {}    // On error, conservatively deny
            }
        }

        span.record("allowed_flakes", result.len());
        Ok(result)
    }

    /// Filter flakes using a resolved graph view.
    ///
    /// Convenience method that extracts db/overlay/to_t from a `ResolvedGraphView`.
    /// This is the preferred API for Phase 2 dataset integration.
    pub async fn filter_flakes_for_view<S: Storage + 'static>(
        &self,
        view: &ResolvedGraphView<'_, S>,
        tracker: &Tracker,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        self.filter_flakes_for_graph(view.db, view.overlay, view.to_t, tracker, flakes)
            .await
    }

    /// Check if a single flake is allowed by policy using explicit graph parameters.
    ///
    /// This is the correct method for dataset mode.
    pub async fn allow_flake_for_graph<S: Storage + 'static>(
        &self,
        db: &Db<S>,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        tracker: &Tracker,
        flake: &Flake,
    ) -> Result<bool> {
        // Root policy bypasses all checks
        if self.policy.wrapper().is_root() {
            return Ok(true);
        }

        // Schema flakes always allowed
        if is_schema_flake(&flake.p, &flake.o) {
            return Ok(true);
        }

        // Create executor using the GRAPH's db/overlay/to_t
        let executor = QueryPolicyExecutor::with_overlay(db, overlay, to_t);

        // Get subject classes from cache
        let subject_classes = self
            .policy
            .get_cached_subject_classes(&flake.s)
            .unwrap_or_default();

        // Async policy check
        self.policy
            .allow_view_flake_async(
                &flake.s,
                &flake.p,
                &flake.o,
                &subject_classes,
                &executor,
                tracker,
            )
            .await
            .map_err(|e| crate::error::QueryError::Policy(e.to_string()))
    }

    /// Check if a single flake is allowed using a resolved graph view.
    pub async fn allow_flake_for_view<S: Storage + 'static>(
        &self,
        view: &ResolvedGraphView<'_, S>,
        tracker: &Tracker,
        flake: &Flake,
    ) -> Result<bool> {
        self.allow_flake_for_graph(view.db, view.overlay, view.to_t, tracker, flake)
            .await
    }

    /// Populate the class cache for subjects using explicit graph parameters.
    ///
    /// Call this before filtering to ensure class lookups are cached.
    /// Uses the graph's snapshot for class membership lookups.
    pub async fn populate_class_cache_for_graph<S: Storage + 'static>(
        &self,
        db: &Db<S>,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        subjects: &[fluree_db_core::Sid],
    ) -> Result<()> {
        fluree_db_policy::populate_class_cache(subjects, db, overlay, to_t, &self.policy)
            .await
            .map_err(|e| crate::error::QueryError::Policy(e.to_string()))?;
        Ok(())
    }

    /// Populate the class cache using a resolved graph view.
    pub async fn populate_class_cache_for_view<S: Storage + 'static>(
        &self,
        view: &ResolvedGraphView<'_, S>,
        subjects: &[fluree_db_core::Sid],
    ) -> Result<()> {
        self.populate_class_cache_for_graph(view.db, view.overlay, view.to_t, subjects)
            .await
    }
}

impl std::fmt::Debug for QueryPolicyEnforcer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryPolicyEnforcer")
            .field("is_root", &self.is_root())
            .finish()
    }
}
