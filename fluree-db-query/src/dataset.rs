//! Dataset types for multi-graph query execution
//!
//! This module provides the runtime dataset types used by the query executor
//! when querying across multiple graphs (SPARQL datasets).
//!
//! Key types:
//! - [`GraphRef`]: A borrowed reference to a single graph (db + overlay + time bounds)
//! - [`DataSet`]: An immutable collection of default and named graphs
//! - [`ActiveGraph`]: Enum indicating which graph(s) are currently active for scanning
//!
//! # Architecture
//!
//! `DataSet` is **immutable** - active graph state is stored in `ExecutionContext`,
//! not here. This avoids borrow/clone issues when switching graphs during GRAPH
//! pattern evaluation.
//!
//! Graph names are stored as `Arc<str>` (IRI strings), not `Sid`, because graph
//! identifiers may not be encodable via any single DB's namespace table.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use fluree_db_core::{Db, NodeCache, OverlayProvider, Storage};

use crate::policy::QueryPolicyEnforcer;

/// Reference to a single graph view (borrowed, for execution)
///
/// Contains all the information needed to scan a single graph:
/// - The database (index storage)
/// - The overlay provider (novelty layer)
/// - Time bounds for the query
/// - Ledger alias for provenance tracking in multi-ledger joins
/// - Optional policy enforcer for per-graph policy enforcement
///
/// # Per-Graph Policy
///
/// Each graph in a dataset can have its own policy enforcer, supporting
/// the "wrap-first, then compose" model where individual views are
/// policy-wrapped before being assembled into a dataset.
pub struct GraphRef<'a, S: Storage, C: NodeCache> {
    /// The database for this graph
    pub db: &'a Db<S, C>,
    /// Overlay provider (novelty) - NOT optional, LedgerState always has novelty
    pub overlay: &'a dyn OverlayProvider,
    /// Target transaction time for this graph
    pub to_t: i64,
    /// Ledger alias for provenance tracking (e.g., "orders:main", "customers:main")
    ///
    /// Used when creating `Binding::IriMatch` in multi-ledger mode to track
    /// which ledger a SID came from. This enables correct re-encoding when
    /// joining across ledgers with different namespace tables.
    pub ledger_alias: Arc<str>,
    /// Optional per-graph policy enforcer
    ///
    /// When present, this graph's data is filtered by the enforcer's policy.
    /// Enables per-graph policy in datasets (e.g., different policies for
    /// different named graphs).
    pub policy_enforcer: Option<Arc<QueryPolicyEnforcer>>,
}

impl<'a, S: Storage, C: NodeCache> GraphRef<'a, S, C> {
    /// Create a new graph reference
    ///
    /// # Arguments
    ///
    /// * `db` - The database for this graph
    /// * `overlay` - Overlay provider (novelty layer)
    /// * `to_t` - Target transaction time
    /// * `ledger_alias` - Ledger alias for provenance tracking (e.g., "orders:main")
    pub fn new(
        db: &'a Db<S, C>,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        ledger_alias: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            ledger_alias: ledger_alias.into(),
            policy_enforcer: None,
        }
    }

    /// Create a new graph reference with a policy enforcer
    ///
    /// # Arguments
    ///
    /// * `db` - The database for this graph
    /// * `overlay` - Overlay provider (novelty layer)
    /// * `to_t` - Target transaction time
    /// * `ledger_alias` - Ledger alias for provenance tracking
    /// * `policy_enforcer` - Policy enforcer for this graph
    pub fn with_policy(
        db: &'a Db<S, C>,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
        ledger_alias: impl Into<Arc<str>>,
        policy_enforcer: Arc<QueryPolicyEnforcer>,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            ledger_alias: ledger_alias.into(),
            policy_enforcer: Some(policy_enforcer),
        }
    }

    /// Create a graph reference using the db's alias as the ledger alias
    ///
    /// Convenience method when the db's alias is the appropriate identifier.
    pub fn from_db(db: &'a Db<S, C>, overlay: &'a dyn OverlayProvider, to_t: i64) -> Self {
        Self {
            db,
            overlay,
            to_t,
            ledger_alias: Arc::from(db.alias.as_str()),
            policy_enforcer: None,
        }
    }

    /// Check if this graph has a policy enforcer attached
    pub fn has_policy(&self) -> bool {
        self.policy_enforcer
            .as_ref()
            .map(|e| !e.is_root())
            .unwrap_or(false)
    }
}

impl<'a, S: Storage, C: NodeCache> fmt::Debug for GraphRef<'a, S, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphRef")
            .field("db", &"<Db>")
            .field("overlay", &"<dyn OverlayProvider>")
            .field("to_t", &self.to_t)
            .field("ledger_alias", &self.ledger_alias)
            .field("has_policy", &self.policy_enforcer.is_some())
            .finish()
    }
}

// Implement GraphView for GraphRef to enable composability
impl<'a, S: Storage, C: NodeCache> crate::graph_view::GraphView<S, C> for GraphRef<'a, S, C> {
    fn resolve(&self) -> crate::graph_view::ResolvedGraphView<'_, S, C> {
        crate::graph_view::ResolvedGraphView {
            db: self.db,
            overlay: self.overlay,
            to_t: self.to_t,
            // Convert Option<Arc<T>> to Option<&T>
            policy_enforcer: self.policy_enforcer.as_deref(),
            ledger_alias: &self.ledger_alias,
        }
    }

    fn ledger_alias(&self) -> &Arc<str> {
        &self.ledger_alias
    }
}

/// Runtime dataset for query execution (borrowed references)
///
/// `fluree-db-query` receives this from `fluree-db-api` after ledger resolution.
///
/// # Important
///
/// `DataSet` is **immutable**. Active graph state (`ActiveGraph::Default` or
/// `ActiveGraph::Named(iri)`) is stored in `ExecutionContext`, not here.
/// This avoids borrow/clone issues and makes `with_active_graph()` trivial.
///
/// Construct via `DataSet::new()` and add graphs with `with_default_graph` and `with_named_graph`.
#[derive(Debug)]
pub struct DataSet<'a, S: Storage, C: NodeCache> {
    /// Default graphs - unioned for non-GRAPH patterns
    default_graphs: Vec<GraphRef<'a, S, C>>,
    /// Named graphs keyed by IRI string (not Sid)
    named_graphs: HashMap<Arc<str>, GraphRef<'a, S, C>>,
}

impl<'a, S: Storage, C: NodeCache> DataSet<'a, S, C> {
    /// Create a new empty dataset
    pub fn new() -> Self {
        Self {
            default_graphs: Vec::new(),
            named_graphs: HashMap::new(),
        }
    }

    /// Add a default graph
    pub fn with_default_graph(mut self, graph: GraphRef<'a, S, C>) -> Self {
        self.default_graphs.push(graph);
        self
    }

    /// Add a named graph
    pub fn with_named_graph(mut self, iri: impl Into<Arc<str>>, graph: GraphRef<'a, S, C>) -> Self {
        self.named_graphs.insert(iri.into(), graph);
        self
    }

    /// Get default graph references
    pub fn default_graphs(&self) -> &[GraphRef<'a, S, C>] {
        &self.default_graphs
    }

    /// Get a named graph by IRI (None if not found)
    pub fn named_graph(&self, iri: &str) -> Option<&GraphRef<'a, S, C>> {
        self.named_graphs.get(iri)
    }

    /// Get all named graph IRIs (for GRAPH ?g iteration)
    pub fn named_graph_iris(&self) -> Vec<Arc<str>> {
        self.named_graphs.keys().cloned().collect()
    }

    /// Check if a named graph exists
    pub fn has_named_graph(&self, iri: &str) -> bool {
        self.named_graphs.contains_key(iri)
    }

    /// Check if the dataset is empty (no graphs)
    pub fn is_empty(&self) -> bool {
        self.default_graphs.is_empty() && self.named_graphs.is_empty()
    }

    /// Get the number of default graphs
    pub fn num_default_graphs(&self) -> usize {
        self.default_graphs.len()
    }

    /// Get the number of named graphs
    pub fn num_named_graphs(&self) -> usize {
        self.named_graphs.len()
    }

    /// Iterate over all named graphs (IRI, GraphRef pairs)
    ///
    /// Used when searching for a graph by ledger_alias rather than by IRI.
    pub fn named_graphs_iter(&self) -> impl Iterator<Item = (&Arc<str>, &GraphRef<'a, S, C>)> {
        self.named_graphs.iter()
    }

    /// Find a graph by ledger alias (searching both default and named graphs)
    ///
    /// Returns the first graph whose `ledger_alias` matches the given alias.
    /// This is used for cross-ledger SID encoding/decoding where we need to
    /// find the db associated with a specific ledger alias.
    ///
    /// # Invariant
    ///
    /// Ledger aliases should be unique within a dataset. If multiple graphs
    /// have the same ledger alias, this method returns the first match
    /// (checking default graphs before named graphs). This could lead to
    /// incorrect encoding/decoding if the invariant is violated.
    ///
    /// The dataset construction code should ensure uniqueness, or the caller
    /// should be aware that duplicate aliases may cause ambiguous behavior.
    pub fn find_by_ledger_alias(&self, ledger_alias: &str) -> Option<&GraphRef<'a, S, C>> {
        // Check default graphs first
        for graph in &self.default_graphs {
            if graph.ledger_alias.as_ref() == ledger_alias {
                return Some(graph);
            }
        }
        // Check named graphs
        for (_, graph) in &self.named_graphs {
            if graph.ledger_alias.as_ref() == ledger_alias {
                return Some(graph);
            }
        }
        None
    }
}

impl<'a, S: Storage, C: NodeCache> Default for DataSet<'a, S, C> {
    fn default() -> Self {
        Self::new()
    }
}

/// Currently active graph for scanning
///
/// Stored in `ExecutionContext`, not `DataSet`. This enum indicates
/// which graph(s) should be used when `ScanOperator` fetches data.
///
/// - `Default`: Use all default graphs (union their results)
/// - `Named(iri)`: Use only the specified named graph
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ActiveGraph {
    /// Query default graph(s) - results are unioned if multiple
    #[default]
    Default,
    /// Query a specific named graph by IRI
    Named(Arc<str>),
}

impl ActiveGraph {
    /// Create an active graph for a named graph
    pub fn named(iri: impl Into<Arc<str>>) -> Self {
        Self::Named(iri.into())
    }

    /// Check if this is the default graph
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    /// Check if this is a named graph
    pub fn is_named(&self) -> bool {
        matches!(self, Self::Named(_))
    }

    /// Get the named graph IRI if this is a named graph
    pub fn as_named(&self) -> Option<&str> {
        match self {
            Self::Named(iri) => Some(iri),
            Self::Default => None,
        }
    }
}

/// Active graphs for scanning - avoids "empty vec means single" footgun
///
/// This enum explicitly distinguishes between:
/// - Single-db mode (no dataset) where callers should use `ctx.db`
/// - Dataset mode where callers should use the provided `GraphRef`s
///
/// NOTE: `Many(Vec<...>)` allocates on each call. Future optimization:
/// return slices where possible (`&[GraphRef]` for defaults, `Option<&GraphRef>` for named).
/// Fine for MVP.
#[derive(Debug)]
pub enum ActiveGraphs<'a, 'b, S: Storage, C: NodeCache> {
    /// Single-db mode (no dataset) - use `ctx.db`/`ctx.overlay()`/`ctx.to_t`
    Single,
    /// Multiple graphs from dataset
    Many(Vec<&'b GraphRef<'a, S, C>>),
}

impl<'a, 'b, S: Storage, C: NodeCache> ActiveGraphs<'a, 'b, S, C> {
    /// Check if this is single-db mode
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Single)
    }

    /// Check if this is dataset mode (one or more graphs)
    pub fn is_many(&self) -> bool {
        matches!(self, Self::Many(_))
    }

    /// Get the graphs if in dataset mode
    pub fn as_many(&self) -> Option<&[&'b GraphRef<'a, S, C>]> {
        match self {
            Self::Many(graphs) => Some(graphs),
            Self::Single => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_graph_default() {
        let ag = ActiveGraph::default();
        assert!(ag.is_default());
        assert!(!ag.is_named());
        assert_eq!(ag.as_named(), None);
    }

    #[test]
    fn test_active_graph_named() {
        let ag = ActiveGraph::named("http://example.org/graph1");
        assert!(!ag.is_default());
        assert!(ag.is_named());
        assert_eq!(ag.as_named(), Some("http://example.org/graph1"));
    }
}
