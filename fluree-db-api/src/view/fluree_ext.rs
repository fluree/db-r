//! Fluree extension methods for view construction
//!
//! Provides convenience methods on `Fluree` for loading and wrapping views.

use std::sync::Arc;

use chrono::DateTime;

use crate::view::{FlureeView, ReasoningModePrecedence};
use crate::{
    time_resolve, ApiError, Fluree, NameService, QueryConnectionOptions, Result, Storage, TimeSpec,
};
use fluree_db_core::ids::GraphId;
use fluree_db_core::{ContentStore, DictNovelty, Flake, IndexType, OverlayProvider, Sid};
use fluree_db_indexer::run_index::BinaryIndexStore;
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
    /// User-defined named graph by IRI (needs resolution via binary index store)
    Named(String),
}

// ============================================================================
// Overlay graph filtering (for overlay-only historical time travel)
// ============================================================================

#[derive(Clone)]
struct GraphIriFilteredOverlay {
    inner: Arc<dyn OverlayProvider>,
    // namespace_code -> prefix mapping for reconstructing IRIs from Sids
    namespace_codes: std::collections::HashMap<u16, String>,
    // Target graph IRI (exact match)
    target_graph_iri: Arc<str>,
}

impl GraphIriFilteredOverlay {
    fn new(
        inner: Arc<dyn OverlayProvider>,
        namespace_codes: std::collections::HashMap<u16, String>,
        target_graph_iri: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            inner,
            namespace_codes,
            target_graph_iri: target_graph_iri.into(),
        }
    }

    fn flake_in_target_graph(&self, flake: &Flake) -> bool {
        let Some(g) = flake.g.as_ref() else {
            return false; // default graph
        };
        let Some(prefix) = self.namespace_codes.get(&g.namespace_code) else {
            return false;
        };
        // Compare without allocating: target == prefix + name
        let target = self.target_graph_iri.as_ref();
        let name = g.name.as_ref();
        target.starts_with(prefix) && &target[prefix.len()..] == name
    }
}

impl OverlayProvider for GraphIriFilteredOverlay {
    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    fn for_each_overlay_flake(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.inner
            .for_each_overlay_flake(index, first, rhs, leftmost, to_t, &mut |flake| {
                if self.flake_in_target_graph(flake) {
                    callback(flake);
                }
            })
    }
}

#[derive(Clone)]
struct GraphSidFilteredOverlay {
    inner: Arc<dyn OverlayProvider>,
    target_graph: Sid,
}

impl GraphSidFilteredOverlay {
    fn new(inner: Arc<dyn OverlayProvider>, target_graph: Sid) -> Self {
        Self {
            inner,
            target_graph,
        }
    }

    fn flake_in_target_graph(&self, flake: &Flake) -> bool {
        flake.g.as_ref() == Some(&self.target_graph)
    }
}

impl OverlayProvider for GraphSidFilteredOverlay {
    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    fn for_each_overlay_flake(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.inner
            .for_each_overlay_flake(index, first, rhs, leftmost, to_t, &mut |flake| {
                if self.flake_in_target_graph(flake) {
                    callback(flake);
                }
            })
    }
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
    /// - `#<iri>` → user-defined named graph (resolved via binary index store)
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
                    // Any other fragment is treated as a graph IRI (or suffix)
                    other => Ok((ledger_id, GraphRef::Named(other.to_string()))),
                }
            }
        }
    }

    /// Resolve a GraphRef to a concrete g_id using the binary index store.
    ///
    /// For `Named` graphs, the IRI must be an exact match in the binary index.
    /// No prefix expansion or guessing is performed - use the structured
    /// `graph` field in dataset specs for cleaner IRI handling.
    fn resolve_graph_ref(view: &FlureeView, graph_ref: GraphRef) -> Result<GraphId> {
        match graph_ref {
            GraphRef::Default => Ok(0),
            GraphRef::TxnMeta => Ok(1),
            GraphRef::Named(iri) => {
                let Some(store) = view.binary_store.as_ref() else {
                    return Err(ApiError::query(format!(
                        "Named graph queries require a binary index store (graph: #{})",
                        iri
                    )));
                };

                // Exact IRI match only - no prefix guessing
                store
                    .graph_id_for_iri(&iri)
                    .ok_or_else(|| ApiError::query(format!("Unknown named graph '#{}'", iri)))
            }
        }
    }

    /// Apply a graph selection to a loaded view.
    ///
    /// Resolves the `GraphRef` to a concrete g_id, then re-scopes the view's
    /// `Db.range_provider` and sets `view.graph_id` so both range queries
    /// and binary scans use the same graph.
    fn select_graph(mut view: FlureeView, graph_ref: GraphRef) -> Result<FlureeView> {
        match graph_ref {
            GraphRef::Default => Ok(view),
            GraphRef::TxnMeta => {
                // Prefer index-backed graph scoping when we have the binary store.
                // In overlay-only historical mode, we don't attach a binary store on purpose,
                // so we fall back to filtering overlay flakes by the txn-meta graph IRI.
                if view.binary_store.is_some() && view.dict_novelty.is_some() {
                    let graph_id = Self::resolve_graph_ref(&view, GraphRef::TxnMeta)?;
                    if graph_id == 0 {
                        return Ok(view);
                    }
                    let store = view.binary_store.clone().unwrap();
                    let dict_novelty = view.dict_novelty.clone().unwrap();
                    let provider = BinaryRangeProvider::new(store, dict_novelty);
                    let mut db = (*view.db).clone();
                    db.range_provider = Some(Arc::new(provider));
                    view.db = Arc::new(db);
                    Ok(view.with_graph_id(graph_id))
                } else {
                    // Overlay-only historical path: filter overlay flakes to txn-meta graph.
                    // This enables `ledger#txn-meta` time travel even when we intentionally
                    // don't attach the binary store (overlay replay path).
                    let inner = Arc::clone(&view.overlay);
                    let txn_meta_graph = Sid::new(fluree_vocab::namespaces::FLUREE_DB, "txn-meta");
                    view.overlay = Arc::new(GraphSidFilteredOverlay::new(inner, txn_meta_graph));
                    Ok(view.with_graph_id(1))
                }
            }
            GraphRef::Named(iri) => {
                // Index-backed path: resolve to g_id + rescope provider for graph-aware range queries.
                if view.binary_store.is_some() && view.dict_novelty.is_some() {
                    let graph_id = Self::resolve_graph_ref(&view, GraphRef::Named(iri.clone()))?;
                    if graph_id == 0 {
                        return Ok(view);
                    }
                    let store = view.binary_store.clone().unwrap();
                    let dict_novelty = view.dict_novelty.clone().unwrap();
                    let provider = BinaryRangeProvider::new(store, dict_novelty);
                    let mut db = (*view.db).clone();
                    db.range_provider = Some(Arc::new(provider));
                    view.db = Arc::new(db);
                    Ok(view.with_graph_id(graph_id))
                } else {
                    // Overlay-only historical path: filter overlay flakes by graph IRI.
                    // This supports named graph time travel even when the binary index
                    // cannot answer historical queries (no Region 3 coverage).
                    let inner = Arc::clone(&view.overlay);
                    let ns_codes = view.db.namespace_codes.clone();
                    view.overlay =
                        Arc::new(GraphIriFilteredOverlay::new(inner, ns_codes, iri.clone()));
                    Ok(view)
                }
            }
        }
    }

    /// Load the current view (immutable snapshot) from a ledger.
    ///
    /// Uses the connection-level ledger cache when available (check cache first,
    /// load + cache if not present). Falls back to a fresh load when caching
    /// is disabled.
    ///
    /// This is the internal loading method. For the public API, use
    /// [`graph()`](Self::graph) which returns a lazy [`Graph`](crate::Graph) handle.
    pub(crate) async fn load_view(&self, ledger_id: &str) -> Result<FlureeView> {
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
                let cs = fluree_db_core::content_store_for(storage.clone(), &snapshot.db.ledger_id);
                let bytes = cs
                    .get(&index_cid)
                    .await
                    .map_err(|e| ApiError::internal(format!("read index root: {}", e)))?;
                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let cs = std::sync::Arc::new(fluree_db_core::content_store_for(
                    storage.clone(),
                    &snapshot.db.ledger_id,
                ));
                let mut store =
                    BinaryIndexStore::load_from_root_bytes_default(cs, &bytes, &cache_dir)
                        .await
                        .map_err(|e| ApiError::internal(format!("load binary index: {}", e)))?;

                // Augment namespace codes with entries from novelty commits.
                store.augment_namespace_codes(&snapshot.db.namespace_codes);

                let arc_store = Arc::new(store);
                let dn = snapshot.dict_novelty.clone();
                let provider = BinaryRangeProvider::new(Arc::clone(&arc_store), dn);
                snapshot.db.range_provider = Some(Arc::new(provider));
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
                    &snapshot.db.ledger_id,
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
        let view = FlureeView::from_ledger_state(&ledger);
        Ok(match binary_store {
            Some(store) => view.with_binary_store(store),
            None => view,
        })
    }

    /// Load a historical view at a specific transaction time.
    ///
    /// For named graph queries (e.g., `#txn-meta`), this also loads the binary
    /// index store if available, enabling graph-scoped queries.
    pub(crate) async fn load_view_at_t(
        &self,
        ledger_id: &str,
        target_t: i64,
    ) -> Result<FlureeView> {
        let historical = self.ledger_view_at(ledger_id, target_t).await?;
        let mut view = FlureeView::from_historical(&historical);

        // Attach a dict_novelty derived from the historical Db's watermarks.
        // This avoids relying on potentially-stale cached handle state and is
        // sufficient for binary overlay translation when an overlay is present.
        view.dict_novelty = Some(Arc::new(DictNovelty::with_watermarks(
            view.db.subject_watermarks.clone(),
            view.db.string_watermark,
        )));

        // Load the binary index store (for index-backed historical queries only).
        //
        // When the historical view is overlay-only (genesis Db + commit replay),
        // we intentionally skip attaching a binary store so the query engine
        // takes the overlay/range path instead of the binary scan path.
        if view.db.t > 0 {
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
                    store.augment_namespace_codes(&view.db.namespace_codes);

                    view.binary_store = Some(Arc::new(store));
                }
            }
        }

        Ok(view)
    }

    /// Load a view at a flexible time specification.
    ///
    /// Resolves `@t:`, `@iso:`, `@commit:`, or `latest` time specifications.
    pub(crate) async fn load_view_at(&self, ledger_id: &str, spec: TimeSpec) -> Result<FlureeView> {
        match spec {
            TimeSpec::Latest => self.load_view(ledger_id).await,
            TimeSpec::AtT(t) => self.load_view_at_t(ledger_id, t).await,
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
                    &ledger.db,
                    Some(ledger.novelty.as_ref()),
                    target_epoch_ms,
                    current_t,
                )
                .await?;
                self.load_view_at_t(ledger_id, resolved_t).await
            }
            TimeSpec::AtCommit(commit_prefix) => {
                let handle = self.ledger_cached(ledger_id).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let resolved_t = time_resolve::commit_to_t(
                    &ledger.db,
                    Some(ledger.novelty.as_ref()),
                    &commit_prefix,
                    current_t,
                )
                .await?;
                self.load_view_at_t(ledger_id, resolved_t).await
            }
        }
    }

    /// Load the current snapshot from a ledger.
    ///
    /// Returns a [`FlureeView`] — an immutable, point-in-time snapshot.
    /// For the lazy API, use [`graph()`](Self::graph) instead.
    pub async fn view(&self, ledger_id: &str) -> Result<FlureeView> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_view(ledger_id).await?;
        Self::select_graph(view, graph_ref)
    }

    /// Load a historical snapshot at a specific transaction time.
    pub async fn view_at_t(&self, ledger_id: &str, target_t: i64) -> Result<FlureeView> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_view_at_t(ledger_id, target_t).await?;
        Self::select_graph(view, graph_ref)
    }

    /// Load a snapshot at a flexible time specification.
    pub async fn view_at(&self, ledger_id: &str, spec: TimeSpec) -> Result<FlureeView> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_view_at(ledger_id, spec).await?;
        Self::select_graph(view, graph_ref)
    }

    /// Apply a graph selector from a dataset GraphSource to a view.
    ///
    /// Converts the dataset-layer `GraphSelector` to the internal `GraphRef`
    /// and applies graph selection to the view.
    ///
    /// This is called by `load_view_from_source` when a `GraphSource` has
    /// an explicit `graph_selector` set.
    pub(crate) fn apply_graph_selector(
        view: FlureeView,
        selector: &crate::dataset::GraphSelector,
    ) -> Result<FlureeView> {
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
        view: FlureeView,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView> {
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
        ledger_id: &str,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView> {
        let view = self.view(ledger_id).await?;
        self.wrap_policy(view, opts).await
    }

    /// Load a view at a specific time with policy applied.
    pub async fn view_at_t_with_policy(
        &self,
        ledger_id: &str,
        target_t: i64,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeView> {
        let view = self.view_at_t(ledger_id, target_t).await?;
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
    pub fn wrap_reasoning(&self, view: FlureeView, modes: ReasoningModes) -> FlureeView {
        view.with_reasoning(modes)
    }

    /// Wrap a view with reasoning modes and explicit precedence.
    pub fn wrap_reasoning_with_precedence(
        &self,
        view: FlureeView,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> FlureeView {
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

        assert_eq!(&*view.ledger_id, "testdb:main");
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
