//! Fluree extension methods for view construction
//!
//! Provides convenience methods on `Fluree` for loading and wrapping views.

use std::sync::Arc;

use chrono::DateTime;

use crate::view::{FlureeView, ReasoningModePrecedence};
use crate::{
    time_resolve, ApiError, Fluree, NameService, QueryConnectionOptions, Result, SimpleCache,
    Storage, TimeSpec,
};
use fluree_db_query::rewrite::ReasoningModes;

// ============================================================================
// View Loading
// ============================================================================

impl<S, N> Fluree<S, SimpleCache, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Load the current view (immutable snapshot) from a ledger.
    ///
    /// Uses the connection-level ledger cache when available (check cache first,
    /// load + cache if not present). Falls back to a fresh load when caching
    /// is disabled.
    ///
    /// This is the internal loading method. For the public API, use
    /// [`graph()`](Self::graph) which returns a lazy [`Graph`](crate::Graph) handle.
    pub(crate) async fn load_view(&self, alias: &str) -> Result<FlureeView<S, SimpleCache>> {
        let handle = self.ledger_cached(alias).await?;
        let snapshot = handle.snapshot().await;
        let ledger = snapshot.to_ledger_state();
        Ok(FlureeView::from_ledger_state(&ledger))
    }

    /// Load a historical view at a specific transaction time.
    pub(crate) async fn load_view_at_t(
        &self,
        alias: &str,
        target_t: i64,
    ) -> Result<FlureeView<S, SimpleCache>> {
        let historical = self.ledger_view_at(alias, target_t).await?;
        Ok(FlureeView::from_historical(&historical))
    }

    /// Load a view at a flexible time specification.
    ///
    /// Resolves `@t:`, `@iso:`, `@sha:`, or `latest` time specifications.
    pub(crate) async fn load_view_at(
        &self,
        alias: &str,
        spec: TimeSpec,
    ) -> Result<FlureeView<S, SimpleCache>> {
        match spec {
            TimeSpec::Latest => self.load_view(alias).await,
            TimeSpec::AtT(t) => self.load_view_at_t(alias, t).await,
            TimeSpec::AtTime(iso) => {
                let handle = self.ledger_cached(alias).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let target_epoch_ms = DateTime::parse_from_rfc3339(&iso)
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "Invalid ISO-8601 timestamp for time travel: {} ({})",
                            iso, e
                        ))
                    })?
                    .timestamp_millis();
                let resolved_t = time_resolve::datetime_to_t(
                    &ledger.db,
                    Some(ledger.novelty.as_ref()),
                    target_epoch_ms,
                    current_t,
                )
                .await?;
                self.load_view_at_t(alias, resolved_t).await
            }
            TimeSpec::AtCommit(sha_prefix) => {
                let handle = self.ledger_cached(alias).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let resolved_t = time_resolve::sha_to_t(
                    &ledger.db,
                    Some(ledger.novelty.as_ref()),
                    &sha_prefix,
                    current_t,
                )
                .await?;
                self.load_view_at_t(alias, resolved_t).await
            }
        }
    }

    /// Load the current snapshot from a ledger.
    ///
    /// Returns a [`FlureeView`] — an immutable, point-in-time snapshot.
    /// For the lazy API, use [`graph()`](Self::graph) instead.
    pub async fn view(&self, alias: &str) -> Result<FlureeView<S, SimpleCache>> {
        self.load_view(alias).await
    }

    /// Load a historical snapshot at a specific transaction time.
    pub async fn view_at_t(
        &self,
        alias: &str,
        target_t: i64,
    ) -> Result<FlureeView<S, SimpleCache>> {
        self.load_view_at_t(alias, target_t).await
    }

    /// Load a snapshot at a flexible time specification.
    pub async fn view_at(
        &self,
        alias: &str,
        spec: TimeSpec,
    ) -> Result<FlureeView<S, SimpleCache>> {
        self.load_view_at(alias, spec).await
    }
}

// ============================================================================
// Policy Wrapping
// ============================================================================

impl<S, N> Fluree<S, SimpleCache, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Build policy from options and wrap a view.
    ///
    /// This is the primary way to add policy enforcement to a view.
    /// The policy context is built from `QueryConnectionOptions` which supports:
    /// - Identity-based policy (`identity` field)
    /// - Class-based policy (`policy_class` field)
    /// - Inline policy JSON-LD (`policy` field)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = fluree.view("mydb:main").await?;
    /// let opts = QueryConnectionOptions {
    ///     identity: Some("did:example:user".into()),
    ///     ..Default::default()
    /// };
    /// let view = fluree.wrap_policy(view, &opts).await?;
    /// ```
    pub async fn wrap_policy(
        &self,
        view: FlureeView<S, SimpleCache>,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S, SimpleCache>> {
        let policy_ctx = crate::policy_builder::build_policy_context_from_opts(
            &view.db,
            view.overlay.as_ref(),
            view.novelty_for_stats(),
            view.to_t,
            opts,
        )
        .await?;
        Ok(view.with_policy(Arc::new(policy_ctx)))
    }

    /// Load a view at head with policy applied.
    ///
    /// Convenience method that combines `view()` + `wrap_policy()`.
    pub async fn view_with_policy(
        &self,
        alias: &str,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S, SimpleCache>> {
        let view = self.view(alias).await?;
        self.wrap_policy(view, opts).await
    }

    /// Load a view at a specific time with policy applied.
    pub async fn view_at_t_with_policy(
        &self,
        alias: &str,
        target_t: i64,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S, SimpleCache>> {
        let view = self.view_at_t(alias, target_t).await?;
        self.wrap_policy(view, opts).await
    }
}

// ============================================================================
// Reasoning Wrapping
// ============================================================================

impl<S, N> Fluree<S, SimpleCache, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// Wrap a view with default reasoning modes.
    ///
    /// This is a pure function (no async) since it just attaches metadata.
    /// Uses `DefaultUnlessQueryOverrides` precedence.
    pub fn wrap_reasoning(
        &self,
        view: FlureeView<S, SimpleCache>,
        modes: ReasoningModes,
    ) -> FlureeView<S, SimpleCache> {
        view.with_reasoning(modes)
    }

    /// Wrap a view with reasoning modes and explicit precedence.
    pub fn wrap_reasoning_with_precedence(
        &self,
        view: FlureeView<S, SimpleCache>,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> FlureeView<S, SimpleCache> {
        view.with_reasoning_precedence(modes, precedence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_view_not_found() {
        let fluree = FlureeBuilder::memory().build_memory();

        let result = fluree.view("nonexistent:main").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_view_after_create() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Load as view
        let view = fluree.view("testdb:main").await.unwrap();

        assert_eq!(&*view.ledger_alias, "testdb:main");
        assert_eq!(view.to_t, 0); // Genesis
        assert!(view.novelty().is_some());
    }

    #[tokio::test]
    async fn test_view_at_t() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().build_memory();

        // Create and transact
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({ "insert": [{"@id": "ex:a", "ex:name": "Alice"}] });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Load at t=0 (before transaction)
        let view = fluree.view_at_t("testdb:main", 0).await.unwrap();
        assert_eq!(view.to_t, 0);

        // Load at t=1 (after transaction)
        let view = fluree.view_at_t("testdb:main", 1).await.unwrap();
        assert_eq!(view.to_t, 1);
    }

    #[tokio::test]
    async fn test_view_at_future_time_error() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger at t=0
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Try to load at t=100 (future)
        let result = fluree.view_at_t("testdb:main", 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wrap_reasoning() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        assert!(view.reasoning().is_none());

        let view = fluree.wrap_reasoning(view, ReasoningModes::owl2ql());
        assert!(view.reasoning().is_some());
        assert!(view.reasoning().unwrap().owl2ql);
    }
}
