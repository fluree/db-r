//! Fluree extension methods for view construction
//!
//! Provides convenience methods on `Fluree` for loading and wrapping views.

use std::sync::Arc;

use chrono::DateTime;

use crate::view::{FlureeView, ReasoningModePrecedence};
use crate::{
    time_resolve, ApiError, Fluree, NameService, QueryConnectionOptions, Result,
    Storage, TimeSpec,
};
use fluree_db_indexer::run_index::{BinaryIndexRootV2, BinaryIndexStore, BINARY_INDEX_ROOT_VERSION_V2};
use fluree_db_query::BinaryRangeProvider;
use fluree_db_query::rewrite::ReasoningModes;

// ============================================================================
// View Loading
// ============================================================================

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Split a graph reference like `ledger:main#txn-meta` into (ledger_alias, graph_id).
    ///
    /// Currently supported fragments:
    /// - *(none)* → default graph (g_id = 0)
    /// - `#txn-meta` → txn metadata graph (g_id = 1)
    fn parse_graph_ref(alias: &str) -> Result<(&str, u32)> {
        match alias.split_once('#') {
            None => Ok((alias, 0)),
            Some((ledger_alias, frag)) => {
                if ledger_alias.is_empty() {
                    return Err(ApiError::query("Missing ledger before '#'"));
                }
                if frag.is_empty() {
                    return Err(ApiError::query("Missing named graph after '#'"));
                }
                match frag {
                    "txn-meta" => Ok((ledger_alias, 1)),
                    other => Err(ApiError::query(format!(
                        "Unknown named graph '#{}' (supported: #txn-meta)",
                        other
                    ))),
                }
            }
        }
    }

    /// Apply a graph ID selection to a loaded view.
    ///
    /// For non-default graphs, this re-scopes the view's `Db.range_provider` and
    /// sets `view.graph_id` so both range queries and binary scans use the same graph.
    fn select_graph_id(mut view: FlureeView<S>, graph_id: u32) -> Result<FlureeView<S>> {
        if graph_id == 0 {
            return Ok(view);
        }

        let Some(store) = view.binary_store.clone() else {
            return Err(ApiError::internal(
                "Named graph queries require a binary index store".to_string(),
            ));
        };
        let Some(dict_novelty) = view.dict_novelty.clone() else {
            return Err(ApiError::internal(
                "Named graph queries require dict novelty".to_string(),
            ));
        };

        // Re-scope the range provider to the requested graph_id.
        // This ensures callers of `range_with_overlay()` (planner, policy, etc.)
        // see the correct graph.
        let provider = BinaryRangeProvider::new(store, dict_novelty, graph_id);
        let mut db = (*view.db).clone();
        db.range_provider = Some(Arc::new(provider));
        view.db = Arc::new(db);

        Ok(view.with_graph_id(graph_id))
    }

    /// Load the current view (immutable snapshot) from a ledger.
    ///
    /// Uses the connection-level ledger cache when available (check cache first,
    /// load + cache if not present). Falls back to a fresh load when caching
    /// is disabled.
    ///
    /// This is the internal loading method. For the public API, use
    /// [`graph()`](Self::graph) which returns a lazy [`Graph`](crate::Graph) handle.
    pub(crate) async fn load_view(&self, alias: &str) -> Result<FlureeView<S>> {
        let handle = self.ledger_cached(alias).await?;
        let mut snapshot = handle.snapshot().await;

        // If no binary store attached but nameservice has an index address,
        // load the BinaryIndexStore and attach BinaryRangeProvider.
        // This handles the non-cached path (FlureeBuilder::file() without ledger_manager).
        if snapshot.binary_store.is_none() {
            if let Some(index_addr) = snapshot
                .ns_record
                .as_ref()
                .and_then(|r| r.index_address.as_ref())
                .cloned()
            {
                let storage = self.storage();
                let bytes = storage
                    .read_bytes(&index_addr)
                    .await
                    .map_err(|e| ApiError::internal(format!("read index root: {}", e)))?;
                if let Ok(root) = serde_json::from_slice::<BinaryIndexRootV2>(&bytes) {
                    if root.version == BINARY_INDEX_ROOT_VERSION_V2 {
                        let cache_dir = std::env::temp_dir().join("fluree-cache");
                        let store = BinaryIndexStore::load_from_root_default(
                            storage,
                            &root,
                            &cache_dir,
                        )
                        .await
                        .map_err(|e| {
                            ApiError::internal(format!("load binary index: {}", e))
                        })?;
                        let arc_store = Arc::new(store);
                        let dn = snapshot.dict_novelty.clone();
                        let provider =
                            BinaryRangeProvider::new(Arc::clone(&arc_store), dn, 0);
                        snapshot.db.range_provider = Some(Arc::new(provider));
                        snapshot.binary_store = Some(arc_store);
                    }
                }
            }
        }

        let binary_store = snapshot.binary_store.clone();
        let ledger = snapshot.to_ledger_state();
        let view = FlureeView::from_ledger_state(&ledger);
        Ok(match binary_store {
            Some(store) => view.with_binary_store(store),
            None => view,
        })
    }

    /// Load a historical view at a specific transaction time.
    pub(crate) async fn load_view_at_t(
        &self,
        alias: &str,
        target_t: i64,
    ) -> Result<FlureeView<S>> {
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
    ) -> Result<FlureeView<S>> {
        match spec {
            TimeSpec::Latest => self.load_view(alias).await,
            TimeSpec::AtT(t) => self.load_view_at_t(alias, t).await,
            TimeSpec::AtTime(iso) => {
                let handle = self.ledger_cached(alias).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let dt = DateTime::parse_from_rfc3339(&iso)
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "Invalid ISO-8601 timestamp for time travel: {} ({})",
                            iso, e
                        ))
                    })?
                    ;
                // `ledger#time` flakes store epoch milliseconds. If the ISO timestamp includes
                // sub-millisecond precision, `timestamp_millis()` truncates, which can push the
                // target *slightly before* the intended instant. To avoid off-by-one-ms
                // resolution (especially around the first commit after genesis), we ceiling
                // to the next millisecond when sub-ms precision is present.
                let mut target_epoch_ms = dt.timestamp_millis();
                if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
                    target_epoch_ms += 1;
                }
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
    pub async fn view(&self, alias: &str) -> Result<FlureeView<S>> {
        let (ledger_alias, graph_id) = Self::parse_graph_ref(alias)?;
        let view = self.load_view(ledger_alias).await?;
        Self::select_graph_id(view, graph_id)
    }

    /// Load a historical snapshot at a specific transaction time.
    pub async fn view_at_t(
        &self,
        alias: &str,
        target_t: i64,
    ) -> Result<FlureeView<S>> {
        let (ledger_alias, graph_id) = Self::parse_graph_ref(alias)?;
        let view = self.load_view_at_t(ledger_alias, target_t).await?;
        Self::select_graph_id(view, graph_id)
    }

    /// Load a snapshot at a flexible time specification.
    pub async fn view_at(
        &self,
        alias: &str,
        spec: TimeSpec,
    ) -> Result<FlureeView<S>> {
        let (ledger_alias, graph_id) = Self::parse_graph_ref(alias)?;
        let view = self.load_view_at(ledger_alias, spec).await?;
        Self::select_graph_id(view, graph_id)
    }
}

// ============================================================================
// Policy Wrapping
// ============================================================================

impl<S, N> Fluree<S, N>
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
        view: FlureeView<S>,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S>> {
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
    ) -> Result<FlureeView<S>> {
        let view = self.view(alias).await?;
        self.wrap_policy(view, opts).await
    }

    /// Load a view at a specific time with policy applied.
    pub async fn view_at_t_with_policy(
        &self,
        alias: &str,
        target_t: i64,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S>> {
        let view = self.view_at_t(alias, target_t).await?;
        self.wrap_policy(view, opts).await
    }
}

// ============================================================================
// Reasoning Wrapping
// ============================================================================

impl<S, N> Fluree<S, N>
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
        view: FlureeView<S>,
        modes: ReasoningModes,
    ) -> FlureeView<S> {
        view.with_reasoning(modes)
    }

    /// Wrap a view with reasoning modes and explicit precedence.
    pub fn wrap_reasoning_with_precedence(
        &self,
        view: FlureeView<S>,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> FlureeView<S> {
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
