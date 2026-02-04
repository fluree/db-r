//! Dataset view for multi-ledger queries
//!
//! Provides `FlureeDataSetView`, a collection of `FlureeView`s representing
//! a SPARQL-style dataset with default and named graphs.

use std::collections::HashMap;
use std::sync::Arc;

use crate::view::FlureeView;
use crate::{OverlayProvider, Storage};

/// A dataset view composed of multiple `FlureeView`s.
///
/// This mirrors SPARQL dataset semantics:
/// - `default`: graphs merged for the default graph (FROM clauses)
/// - `named`: graphs available as named graphs (FROM NAMED clauses)
///
/// # Usage
///
/// ```ignore
/// // Build dataset from multiple views
/// let view1 = fluree.view("ledger1:main").await?;
/// let view2 = fluree.view("ledger2:main").await?;
///
/// let dataset = FlureeDataSetView::new()
///     .with_default(view1)
///     .with_named("http://example.org/graph2", view2);
///
/// // Query across the dataset
/// let result = fluree.query_dataset_view(&dataset, sparql_query).await?;
/// ```
#[derive(Clone)]
pub struct FlureeDataSetView<S: Storage + 'static> {
    /// Default graph views (merged for queries without GRAPH clause)
    pub default: Vec<FlureeView<S>>,
    /// Named graph views (accessed via GRAPH clause)
    pub named: HashMap<Arc<str>, FlureeView<S>>,
    /// Deterministic named graph insertion order (for primary selection).
    ///
    /// We keep this because `HashMap` does not preserve insertion order, but
    /// connection queries need stable "primary" selection for parsing/formatting.
    pub named_order: Vec<Arc<str>>,
    /// Optional history range (from_t, to_t) for history/changes queries.
    ///
    /// When set, query execution runs in history mode (`@op` support) and applies
    /// the time range bounds. This is a Fluree extension detected from dataset
    /// specs like: `from: ["ledger@t:1", "ledger@t:latest"]`.
    pub history_range: Option<(i64, i64)>,
}

impl<S: Storage + Clone + 'static> FlureeDataSetView<S> {
    /// Create an empty dataset view.
    pub fn new() -> Self {
        Self {
            default: Vec::new(),
            named: HashMap::new(),
            named_order: Vec::new(),
            history_range: None,
        }
    }

    /// Create a dataset view with a single default graph.
    pub fn single(view: FlureeView<S>) -> Self {
        Self {
            default: vec![view],
            named: HashMap::new(),
            named_order: Vec::new(),
            history_range: None,
        }
    }

    /// Add a view to the default graph.
    pub fn with_default(mut self, view: FlureeView<S>) -> Self {
        self.default.push(view);
        self
    }

    /// Add a view as a named graph.
    pub fn with_named(mut self, name: impl Into<Arc<str>>, view: FlureeView<S>) -> Self {
        let key: Arc<str> = name.into();
        if !self.named.contains_key(&key) {
            self.named_order.push(Arc::clone(&key));
        }
        self.named.insert(key, view);
        self
    }

    /// Attach a history time range (from_t, to_t) for history/changes queries.
    pub fn with_history_range(mut self, from_t: i64, to_t: i64) -> Self {
        self.history_range = Some((from_t, to_t));
        self
    }

    /// Returns true if this dataset represents a history/changes query.
    pub fn is_history_mode(&self) -> bool {
        self.history_range.is_some()
    }

    /// Get the history time range, if present.
    pub fn history_time_range(&self) -> Option<(i64, i64)> {
        self.history_range
    }

    /// Add multiple views to the default graph.
    pub fn with_defaults(mut self, views: impl IntoIterator<Item = FlureeView<S>>) -> Self {
        self.default.extend(views);
        self
    }

    /// Check if the dataset has any graphs.
    pub fn is_empty(&self) -> bool {
        self.default.is_empty() && self.named.is_empty()
    }

    /// Get a "primary" graph view for parsing/formatting.
    ///
    /// Mirrors the legacy `LoadedDataset` primary selection behavior:
    /// - first default if present
    /// - else first named (in insertion order)
    /// - else None
    pub fn primary(&self) -> Option<&FlureeView<S>> {
        if let Some(v) = self.default.first() {
            return Some(v);
        }
        let iri = self.named_order.first()?;
        self.named.get(iri)
    }

    /// Get the "primary" graph view, mutably.
    pub fn primary_mut(&mut self) -> Option<&mut FlureeView<S>> {
        if let Some(v) = self.default.first_mut() {
            return Some(v);
        }
        let iri = self.named_order.first()?.clone();
        self.named.get_mut(&iri)
    }

    /// Get a named graph by IRI.
    pub fn get_named(&self, name: &str) -> Option<&FlureeView<S>> {
        self.named.get(name)
    }

    /// Get the maximum `to_t` across all views in the dataset.
    ///
    /// This is useful for result metadata when querying across multiple ledgers.
    pub fn max_t(&self) -> i64 {
        let default_max = self.default.iter().map(|v| v.to_t).max().unwrap_or(0);
        let named_max = self.named.values().map(|v| v.to_t).max().unwrap_or(0);
        default_max.max(named_max)
    }

    /// Build a runtime `fluree-db-query` dataset from this view.
    ///
    /// This is the internal bridge to the query engine. Each graph keeps its own
    /// `to_t` (per-view), and policy enforcement is carried via `GraphRef::policy_enforcer`.
    pub(crate) fn as_runtime_dataset<'a>(&'a self) -> fluree_db_query::DataSet<'a, S> {
        let mut ds = fluree_db_query::DataSet::new();

        for view in &self.default {
            let mut graph = fluree_db_query::GraphRef::new(
                view.db.as_ref(),
                view.overlay.as_ref(),
                view.to_t,
                Arc::clone(&view.ledger_alias),
            );
            graph.policy_enforcer = view.policy_enforcer().cloned();
            ds = ds.with_default_graph(graph);
        }

        for iri in &self.named_order {
            let view = self
                .named
                .get(iri)
                .expect("named_order key must exist in named map");
            let mut graph = fluree_db_query::GraphRef::new(
                view.db.as_ref(),
                view.overlay.as_ref(),
                view.to_t,
                Arc::clone(&view.ledger_alias),
            );
            graph.policy_enforcer = view.policy_enforcer().cloned();
            ds = ds.with_named_graph(Arc::clone(iri), graph);
        }

        ds
    }

    /// Build a composite overlay across all graphs (for graph crawl formatting).
    pub(crate) fn composite_overlay(&self) -> Option<Arc<dyn OverlayProvider>> {
        let mut overlays: Vec<Arc<dyn OverlayProvider>> = Vec::new();

        for v in &self.default {
            overlays.push(Arc::clone(&v.overlay));
        }
        for iri in &self.named_order {
            if let Some(v) = self.named.get(iri) {
                overlays.push(Arc::clone(&v.overlay));
            }
        }

        if overlays.is_empty() {
            return None;
        }

        let composite = crate::overlay::CompositeOverlay::new(overlays);
        Some(Arc::new(composite))
    }

    /// Get the number of views in the dataset.
    pub fn len(&self) -> usize {
        self.default.len() + self.named.len()
    }

    /// Check if this is a single-ledger dataset.
    ///
    /// Returns `true` if there's exactly one default graph and no named graphs.
    pub fn is_single_ledger(&self) -> bool {
        self.default.len() == 1 && self.named.is_empty() && self.history_range.is_none()
    }

    /// Unwrap a single-ledger dataset into its view.
    ///
    /// Returns `None` if this is a multi-ledger dataset.
    pub fn into_single(mut self) -> Option<FlureeView<S>> {
        if self.is_single_ledger() {
            self.default.pop()
        } else {
            None
        }
    }
}

impl<S: Storage + Clone + 'static> Default for FlureeDataSetView<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage + Clone + 'static> std::fmt::Debug for FlureeDataSetView<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlureeDataSetView")
            .field("default_count", &self.default.len())
            .field("named_graphs", &self.named_order)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_dataset_view_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        let dataset = FlureeDataSetView::single(view);

        assert!(dataset.is_single_ledger());
        assert_eq!(dataset.len(), 1);
        assert!(dataset.primary().is_some());
    }

    #[tokio::test]
    async fn test_dataset_view_builder() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger1 = fluree.create_ledger("db1").await.unwrap();
        let _ledger2 = fluree.create_ledger("db2").await.unwrap();

        let view1 = fluree.view("db1:main").await.unwrap();
        let view2 = fluree.view("db2:main").await.unwrap();

        let dataset = FlureeDataSetView::new()
            .with_default(view1)
            .with_named("http://example.org/graph2", view2);

        assert!(!dataset.is_single_ledger());
        assert_eq!(dataset.len(), 2);
        assert!(dataset.get_named("http://example.org/graph2").is_some());
    }

    #[tokio::test]
    async fn test_dataset_view_into_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        let alias = view.ledger_alias.clone();
        let dataset = FlureeDataSetView::single(view);

        let unwrapped = dataset.into_single();
        assert!(unwrapped.is_some());
        assert_eq!(unwrapped.unwrap().ledger_alias, alias);
    }

    #[tokio::test]
    async fn test_dataset_view_max_t() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("dataset_max_t_test").await.unwrap();

        // Transact to get t=1 (use same pattern as working test)
        let txn = json!({ "insert": [{"@id": "ex:a", "ex:name": "Alice"}] });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Use explicit t values to test max_t logic
        // (view_at_t uses historical loading which correctly sets to_t)
        let view_t1 = fluree
            .view_at_t("dataset_max_t_test:main", 1)
            .await
            .unwrap();
        let view_t0 = fluree
            .view_at_t("dataset_max_t_test:main", 0)
            .await
            .unwrap();

        assert_eq!(view_t1.to_t, 1);
        assert_eq!(view_t0.to_t, 0);

        let dataset = FlureeDataSetView::new()
            .with_default(view_t1)
            .with_named("http://example.org/old", view_t0);

        assert_eq!(dataset.max_t(), 1);
    }
}
