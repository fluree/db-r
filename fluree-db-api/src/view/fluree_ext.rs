//! Fluree extension methods for view construction
//!
//! Provides convenience methods on `Fluree` for loading and wrapping views.

use std::sync::Arc;

use chrono::DateTime;

use crate::view::{GraphDb, ReasoningModePrecedence};
use crate::{
    config_resolver, time_resolve, ApiError, Fluree, NameService, QueryConnectionOptions, Result,
    Storage, TimeSpec,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::GraphId;
use fluree_db_core::{ContentStore, DictNovelty};
use fluree_db_query::rewrite::ReasoningModes;
use fluree_db_query::BinaryRangeProvider;

// ============================================================================
// View Loading
// ============================================================================

/// Reference to a named graph, parsed from a fragment but not yet resolved to g_id.
#[derive(Debug)]
enum GraphRef {
    /// Default graph (g_id = 0)
    Default,
    /// Transaction metadata graph (g_id = 1)
    TxnMeta,
    /// User-defined named graph by exact IRI
    Named(String),
}

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Split a graph reference like `ledger:main#txn-meta` into (ledger_id, graph_ref).
    ///
    /// Supported fragments:
    /// - *(none)* → default graph (g_id = 0)
    /// - `#txn-meta` → txn metadata graph (g_id = 1)
    /// - `#<iri>` → user-defined named graph by exact IRI
    fn parse_graph_ref(ledger_id: &str) -> Result<(&str, GraphRef)> {
        match ledger_id.split_once('#') {
            None => Ok((ledger_id, GraphRef::Default)),
            Some((ledger_id, frag)) => {
                if ledger_id.is_empty() {
                    return Err(ApiError::query("Missing ledger before '#'"));
                }
                if frag.is_empty() {
                    return Err(ApiError::query("Missing named graph after '#'"));
                }
                match frag {
                    "txn-meta" => Ok((ledger_id, GraphRef::TxnMeta)),
                    // Any other fragment is treated as a graph IRI (exact match).
                    other => Ok((ledger_id, GraphRef::Named(other.to_string()))),
                }
            }
        }
    }

    /// Apply a graph selection to a loaded view.
    ///
    /// Resolves the `GraphRef` to a concrete g_id, then re-scopes the view's
    /// `Db.range_provider` and sets `view.graph_id` so both range queries
    /// and binary scans use the same graph.
    fn select_graph(mut view: GraphDb, graph_ref: GraphRef) -> Result<GraphDb> {
        let g_id: GraphId = match graph_ref {
            GraphRef::Default => 0,
            GraphRef::TxnMeta => 1,
            GraphRef::Named(iri) => view
                .snapshot
                .graph_registry
                .graph_id_for_iri(&iri)
                // Fallback for safety: if registry is missing an entry but a binary store
                // has it (should not happen in a consistent ledger), use the store.
                .or_else(|| {
                    view.binary_store
                        .as_ref()
                        .and_then(|s| s.graph_id_for_iri(&iri))
                })
                .ok_or_else(|| ApiError::query(format!("Unknown named graph '#{}'", iri)))?,
        };

        if g_id != 0 && view.binary_store.is_some() && view.dict_novelty.is_some() {
            let store = view.binary_store.clone().unwrap();
            let dict_novelty = view.dict_novelty.clone().unwrap();
            let provider = BinaryRangeProvider::new(store, dict_novelty);
            let mut db = (*view.snapshot).clone();
            db.range_provider = Some(Arc::new(provider));
            view.snapshot = Arc::new(db);
        }

        Ok(view.with_graph_id(g_id))
    }

    /// Read the config graph (g_id=2) and attach effective config to the view.
    ///
    /// This is called after graph selection so the resolved config reflects
    /// the correct per-graph overrides. Returns the view unchanged if the
    /// config graph is empty.
    ///
    /// Note: Reasoning defaults are NOT applied here — they are applied at
    /// the request boundary via `config_resolver::merge_reasoning()` which
    /// respects override control and server-verified identity.
    pub(crate) async fn resolve_and_attach_config(&self, view: GraphDb) -> Result<GraphDb> {
        // Config reads are best-effort. If the config graph is unqueryable
        // (e.g., historical snapshot without a range_provider for g_id=2),
        // treat it as "no config" and apply system defaults.
        let config =
            match config_resolver::resolve_ledger_config(&view.snapshot, &*view.overlay, view.t)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(error = %e, "Config graph read failed — using system defaults");
                    return Ok(view);
                }
            };

        let config = match config {
            Some(c) => Arc::new(c),
            None => return Ok(view),
        };

        // Resolve effective config for this view's graph
        let graph_iri = if view.graph_id == 0 {
            None
        } else {
            view.snapshot.graph_registry.iri_for_graph_id(view.graph_id)
        };
        let resolved = config_resolver::resolve_effective_config(&config, graph_iri);

        Ok(view
            .with_ledger_config(config)
            .with_resolved_config(resolved))
    }

    /// Load the current view (immutable snapshot) from a ledger.
    ///
    /// Uses the connection-level ledger cache when available (check cache first,
    /// load + cache if not present). Falls back to a fresh load when caching
    /// is disabled.
    ///
    /// This is the internal loading method. For the public API, use
    /// [`graph()`](Self::graph) which returns a lazy [`Graph`](crate::Graph) handle.
    pub(crate) async fn load_graph_db(&self, ledger_id: &str) -> Result<GraphDb> {
        let handle = self.ledger_cached(ledger_id).await?;
        let mut snapshot = handle.snapshot().await;

        // If no binary store attached but nameservice has an index address,
        // load the BinaryIndexStore and attach BinaryRangeProvider.
        // This handles the non-cached path (FlureeBuilder::file() without ledger_manager).
        if snapshot.binary_store.is_none() {
            if let Some(index_cid) = snapshot
                .ns_record
                .as_ref()
                .and_then(|r| r.index_head_id.as_ref())
                .cloned()
            {
                let storage = self.storage();
                let cs = fluree_db_core::content_store_for(
                    storage.clone(),
                    &snapshot.snapshot.ledger_id,
                );
                let bytes = cs
                    .get(&index_cid)
                    .await
                    .map_err(|e| ApiError::internal(format!("read index root: {}", e)))?;
                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let cs = std::sync::Arc::new(fluree_db_core::content_store_for(
                    storage.clone(),
                    &snapshot.snapshot.ledger_id,
                ));
                let mut store =
                    BinaryIndexStore::load_from_root_bytes_default(cs, &bytes, &cache_dir)
                        .await
                        .map_err(|e| ApiError::internal(format!("load binary index: {}", e)))?;

                // Augment namespace codes with entries from novelty commits.
                store.augment_namespace_codes(&snapshot.snapshot.namespace_codes);

                let arc_store = Arc::new(store);
                let dn = snapshot.dict_novelty.clone();
                let provider = BinaryRangeProvider::new(Arc::clone(&arc_store), dn);
                snapshot.snapshot.range_provider = Some(Arc::new(provider));
                snapshot.binary_store = Some(arc_store);
            }
        }

        // Load default context from CAS if not already loaded.
        if snapshot.default_context.is_none() {
            if let Some(ctx_id) = snapshot
                .ns_record
                .as_ref()
                .and_then(|r| r.default_context.as_ref())
            {
                let cs = fluree_db_core::content_store_for(
                    self.storage().clone(),
                    &snapshot.snapshot.ledger_id,
                );
                if let Ok(bytes) = cs.get(ctx_id).await {
                    if let Ok(ctx) = serde_json::from_slice(&bytes) {
                        snapshot.default_context = Some(ctx);
                    }
                }
            }
        }

        let binary_store = snapshot.binary_store.clone();
        let ledger = snapshot.to_ledger_state();
        let view = GraphDb::from_ledger_state(&ledger);
        Ok(match binary_store {
            Some(store) => view.with_binary_store(store),
            None => view,
        })
    }

    /// Load a historical view at a specific transaction time.
    ///
    /// For named graph queries (e.g., `#txn-meta`), this also loads the binary
    /// index store if available, enabling graph-scoped queries.
    pub(crate) async fn load_graph_db_at_t(
        &self,
        ledger_id: &str,
        target_t: i64,
    ) -> Result<GraphDb> {
        let historical = self.ledger_view_at(ledger_id, target_t).await?;
        let mut view = GraphDb::from_historical(&historical);

        // Attach a dict_novelty derived from the historical Db's watermarks.
        // This avoids relying on potentially-stale cached handle state and is
        // sufficient for binary overlay translation when an overlay is present.
        view.dict_novelty = Some(Arc::new(DictNovelty::with_watermarks(
            view.snapshot.subject_watermarks.clone(),
            view.snapshot.string_watermark,
        )));

        // Load the binary index store (for index-backed historical queries only).
        //
        // When the historical view is overlay-only (genesis Db + commit replay),
        // we intentionally skip attaching a binary store so the query engine
        // takes the overlay/range path instead of the binary scan path.
        if view.snapshot.t > 0 {
            // Use nameservice record (not cached handle) to avoid stale index.
            if let Some(record) = self.nameservice.lookup(ledger_id).await? {
                if let Some(index_cid) = record.index_head_id.as_ref() {
                    let storage = self.storage();
                    let cs = fluree_db_core::content_store_for(storage.clone(), &record.ledger_id);
                    let bytes = cs.get(index_cid).await.map_err(|e| {
                        ApiError::internal(format!(
                            "failed to read index root {}: {}",
                            index_cid, e
                        ))
                    })?;
                    let cache_dir = std::env::temp_dir().join("fluree-cache");
                    let cs = std::sync::Arc::new(fluree_db_core::content_store_for(
                        storage.clone(),
                        &record.ledger_id,
                    ));
                    let mut store =
                        BinaryIndexStore::load_from_root_bytes_default(cs, &bytes, &cache_dir)
                            .await
                            .map_err(|e| {
                                ApiError::internal(format!(
                                    "load binary index store from {}: {}",
                                    index_cid, e
                                ))
                            })?;

                    // Augment namespace codes with entries from novelty commits.
                    store.augment_namespace_codes(&view.snapshot.namespace_codes);

                    view.binary_store = Some(Arc::new(store));
                }
            }
        }

        Ok(view)
    }

    /// Load a view at a flexible time specification.
    ///
    /// Resolves `@t:`, `@iso:`, `@commit:`, or `latest` time specifications.
    pub(crate) async fn load_graph_db_at(
        &self,
        ledger_id: &str,
        spec: TimeSpec,
    ) -> Result<GraphDb> {
        match spec {
            TimeSpec::Latest => self.load_graph_db(ledger_id).await,
            TimeSpec::AtT(t) => self.load_graph_db_at_t(ledger_id, t).await,
            TimeSpec::AtTime(iso) => {
                let handle = self.ledger_cached(ledger_id).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let dt = DateTime::parse_from_rfc3339(&iso).map_err(|e| {
                    ApiError::internal(format!(
                        "Invalid ISO-8601 timestamp for time travel: {} ({})",
                        iso, e
                    ))
                })?;
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
                    &ledger.snapshot,
                    Some(ledger.novelty.as_ref()),
                    target_epoch_ms,
                    current_t,
                )
                .await?;
                self.load_graph_db_at_t(ledger_id, resolved_t).await
            }
            TimeSpec::AtCommit(commit_prefix) => {
                let handle = self.ledger_cached(ledger_id).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let resolved_t = time_resolve::commit_to_t(
                    &ledger.snapshot,
                    Some(ledger.novelty.as_ref()),
                    &commit_prefix,
                    current_t,
                )
                .await?;
                self.load_graph_db_at_t(ledger_id, resolved_t).await
            }
        }
    }

    /// Load the current snapshot from a ledger.
    ///
    /// Returns a [`GraphDb`] — an immutable, point-in-time snapshot.
    /// For the lazy API, use [`graph()`](Self::graph) instead.
    pub async fn db(&self, ledger_id: &str) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db(ledger_id).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Load a historical snapshot at a specific transaction time.
    pub async fn db_at_t(&self, ledger_id: &str, target_t: i64) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db_at_t(ledger_id, target_t).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Load a snapshot at a flexible time specification.
    pub async fn db_at(&self, ledger_id: &str, spec: TimeSpec) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db_at(ledger_id, spec).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Apply a graph selector from a dataset GraphSource to a view.
    ///
    /// Converts the dataset-layer `GraphSelector` to the internal `GraphRef`
    /// and applies graph selection to the view.
    ///
    /// This is called by `load_view_from_source` when a `GraphSource` has
    /// an explicit `graph_selector` set.
    pub(crate) fn apply_graph_selector(
        view: GraphDb,
        selector: &crate::dataset::GraphSelector,
    ) -> Result<GraphDb> {
        let graph_ref = match selector {
            crate::dataset::GraphSelector::Default => GraphRef::Default,
            crate::dataset::GraphSelector::TxnMeta => GraphRef::TxnMeta,
            crate::dataset::GraphSelector::Iri(iri) => GraphRef::Named(iri.clone()),
        };
        Self::select_graph(view, graph_ref)
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
    /// If the view has a `ResolvedConfig`, config defaults are merged with query
    /// opts and override control is checked against `server_identity`.
    ///
    /// `server_identity` is the auth-layer-verified identity — NOT `opts.identity`
    /// which is the user-settable policy evaluation context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = fluree.db("mydb:main").await?;
    /// let opts = QueryConnectionOptions {
    ///     identity: Some("did:example:user".into()),
    ///     ..Default::default()
    /// };
    /// let view = fluree.wrap_policy(view, &opts, None).await?;
    /// ```
    pub async fn wrap_policy(
        &self,
        view: GraphDb,
        opts: &QueryConnectionOptions,
        server_identity: Option<&str>,
    ) -> Result<GraphDb> {
        let effective_opts = if let Some(ref resolved) = view.resolved_config {
            config_resolver::merge_policy_opts(resolved, opts, server_identity)
        } else {
            opts.clone()
        };
        let policy_ctx = crate::policy_builder::build_policy_context_from_opts(
            &view.snapshot,
            view.overlay.as_ref(),
            view.novelty_for_stats(),
            view.t,
            &effective_opts,
        )
        .await?;
        Ok(view.with_policy(Arc::new(policy_ctx)))
    }

    /// Load a view at head with policy applied.
    ///
    /// Convenience method that combines `db()` + `wrap_policy()`.
    /// Passes `None` for server identity (no auth layer plumbing yet).
    pub async fn db_with_policy(
        &self,
        ledger_id: &str,
        opts: &QueryConnectionOptions,
    ) -> Result<GraphDb> {
        let view = self.db(ledger_id).await?;
        self.wrap_policy(view, opts, None).await
    }

    /// Load a db at a specific time with policy applied.
    ///
    /// Passes `None` for server identity (no auth layer plumbing yet).
    pub async fn db_at_t_with_policy(
        &self,
        ledger_id: &str,
        target_t: i64,
        opts: &QueryConnectionOptions,
    ) -> Result<GraphDb> {
        let view = self.db_at_t(ledger_id, target_t).await?;
        self.wrap_policy(view, opts, None).await
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
    pub fn wrap_reasoning(&self, view: GraphDb, modes: ReasoningModes) -> GraphDb {
        view.with_reasoning(modes)
    }

    /// Wrap a view with reasoning modes and explicit precedence.
    pub fn wrap_reasoning_with_precedence(
        &self,
        view: GraphDb,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> GraphDb {
        view.with_reasoning_precedence(modes, precedence)
    }

    /// Apply config-graph reasoning defaults to a view.
    ///
    /// Reads `ResolvedConfig.reasoning` and converts to reasoning wrapper
    /// with the appropriate precedence based on override control.
    ///
    /// `server_identity` is the auth-layer-verified identity (NOT opts.identity).
    /// Pass `None` when no auth layer is present (Phase 1).
    pub fn apply_config_reasoning(&self, view: GraphDb, server_identity: Option<&str>) -> GraphDb {
        let resolved = match &view.resolved_config {
            Some(r) => r,
            None => return view,
        };

        match config_resolver::merge_reasoning(resolved, server_identity) {
            Some((mode_strings, precedence)) => {
                let modes = ReasoningModes::from_mode_strings(&mode_strings);
                // Always wrap if modes has enabled flags or explicit_none=true
                // (config can force-disable reasoning via "none").
                // Only skip if from_mode_strings produced a truly empty default.
                if modes.has_any_enabled() || modes.is_disabled() {
                    view.with_reasoning_precedence(modes, precedence)
                } else {
                    view
                }
            }
            None => view,
        }
    }

    /// Apply config-graph datalog defaults to a view.
    ///
    /// Stores resolved datalog config on the view. Enforcement happens
    /// at query execution time, not here.
    pub fn apply_config_datalog(&self, view: GraphDb, server_identity: Option<&str>) -> GraphDb {
        let resolved = match &view.resolved_config {
            Some(r) => r,
            None => return view,
        };

        match config_resolver::merge_datalog_opts(resolved, server_identity) {
            Some(config) => view
                .with_datalog_enabled(config.enabled)
                .with_query_time_rules_allowed(config.allow_query_time_rules)
                .with_datalog_override_allowed(config.override_allowed),
            None => view,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_view_not_found() {
        let fluree = FlureeBuilder::memory().build_memory();

        let result = fluree.db("nonexistent:main").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_view_after_create() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Load as view
        let view = fluree.db("testdb:main").await.unwrap();

        assert_eq!(&*view.ledger_id, "testdb:main");
        assert_eq!(view.t, 0); // Genesis
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
        let view = fluree.db_at_t("testdb:main", 0).await.unwrap();
        assert_eq!(view.t, 0);

        // Load at t=1 (after transaction)
        let view = fluree.db_at_t("testdb:main", 1).await.unwrap();
        assert_eq!(view.t, 1);
    }

    #[tokio::test]
    async fn test_view_at_future_time_error() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger at t=0
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Try to load at t=100 (future)
        let result = fluree.db_at_t("testdb:main", 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wrap_reasoning() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.db("testdb:main").await.unwrap();
        assert!(view.reasoning().is_none());

        let view = fluree.wrap_reasoning(view, ReasoningModes::owl2ql());
        assert!(view.reasoning().is_some());
        assert!(view.reasoning().unwrap().owl2ql);
    }
}
