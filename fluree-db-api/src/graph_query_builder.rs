//! Query builders for the [`Graph`] and [`Db`] API.
//!
//! - [`GraphQueryBuilder`] — lazy query from a [`Graph`] handle (defers view load)
//! - [`GraphSnapshotQueryBuilder`] — query from a materialized snapshot or [`StagedGraph`]

use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::error::BuilderErrors;
use crate::format::FormatterConfig;
use crate::graph::Graph;
use crate::query::builder::QueryCore;
use crate::view::GraphDb;
use crate::{
    ApiError, Fluree, NameService, QueryResult, Result, Storage, TrackedErrorResponse,
    TrackedQueryResponse, TrackingOptions,
};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};

// ============================================================================
// GraphQueryBuilder (lazy — defers view load to terminal)
// ============================================================================

/// Query builder from a lazy [`Graph`] handle.
///
/// No I/O occurs until a terminal method (`.execute()`, `.execute_formatted()`,
/// `.execute_tracked()`) is called.
///
/// # Example
///
/// ```ignore
/// let result = fluree
///     .graph("mydb:main")
///     .query()
///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
///     .execute()
///     .await?;
/// ```
pub struct GraphQueryBuilder<'a, 'g, S: Storage + 'static, N> {
    graph: &'g Graph<'a, S, N>,
    core: QueryCore<'g>,
}

impl<'a, 'g, S, N> GraphQueryBuilder<'a, 'g, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Graph::query()`).
    pub(crate) fn new(graph: &'g Graph<'a, S, N>) -> Self {
        Self {
            graph,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD.
    pub fn jsonld(mut self, json: &'g JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'g str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics (fuel, time, policy).
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration (used by `.execute_formatted()`).
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers for graph source queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    ///
    /// Attaches actual R2RML provider objects so that GRAPH patterns
    /// targeting graph sources resolve via the R2RML/Iceberg engine.
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self
    where
        N: crate::GraphSourcePublisher,
    {
        let shared = Arc::new(crate::graph_source::FlureeR2rmlProvider::new(
            self.graph.fluree,
        ));
        let provider: Arc<dyn R2rmlProvider + 'g> = shared.clone();
        let table_provider: Arc<dyn R2rmlTableProvider + 'g> = shared;
        self.core.r2rml = Some((provider, table_provider));
        self.core.set_r2rml();
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.core.validate();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Execute the query and return raw [`QueryResult`].
    ///
    /// Loads the graph snapshot internally, then runs the query.
    /// When R2RML is enabled, uses graph source fallback for resolution
    /// and routes through the R2RML-aware execution path.
    pub async fn execute(mut self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let view = self
            .graph
            .fluree
            .load_graph_db_at(&self.graph.ledger_id, self.graph.time_spec.clone())
            .await?;
        let r2rml = self.core.r2rml.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_view_with_r2rml(&view, input, provider.as_ref(), table_provider.as_ref())
                    .await
            }
            None => self.graph.fluree.query(&view, input).await,
        }
    }

    /// Execute and return formatted JSON output.
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let view = self
            .graph
            .fluree
            .load_graph_db_at(&self.graph.ledger_id, self.graph.time_spec.clone())
            .await?;
        let r2rml = self.core.r2rml.take();
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.take().unwrap();
        let result = match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_view_with_r2rml(&view, input, provider.as_ref(), table_provider.as_ref())
                    .await?
            }
            None => self.graph.fluree.query(&view, input).await?,
        };
        match view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(view.as_graph_db_ref(), &format_config, policy)
                .await?),
            None => Ok(result
                .format_async(view.as_graph_db_ref(), &format_config)
                .await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        mut self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let db = self
            .graph
            .fluree
            .load_graph_db_at(&self.graph.ledger_id, self.graph.time_spec.clone())
            .await
            .map_err(|e| TrackedErrorResponse::new(404, e.to_string(), None))?;
        let r2rml = self.core.r2rml.take();
        let format_config = self.core.format.take();
        let tracking = self.core.tracking.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_tracked_with_r2rml(
                        &db,
                        input,
                        format_config,
                        tracking,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => {
                self.graph
                    .fluree
                    .query_tracked(&db, input, format_config, tracking)
                    .await
            }
        }
    }
}

// ============================================================================
// GraphSnapshotQueryBuilder (materialized — view already loaded)
// ============================================================================

/// Query builder from a materialized snapshot (used by both [`GraphSnapshot`] and [`StagedGraph`]).
///
/// The view is already loaded, so no additional I/O occurs for the load step.
///
/// # Example
///
/// ```ignore
/// let snapshot = fluree.graph("mydb:main").load().await?;
/// let result = snapshot.query().jsonld(&q).execute().await?;
/// ```
pub struct GraphSnapshotQueryBuilder<'a, 'v, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    view: &'v GraphDb,
    core: QueryCore<'v>,
}

impl<'a: 'v, 'v, S, N> GraphSnapshotQueryBuilder<'a, 'v, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder from a fluree reference and a view.
    pub fn new_from_parts(fluree: &'a Fluree<S, N>, view: &'v GraphDb) -> Self {
        Self {
            fluree,
            view,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD.
    pub fn jsonld(mut self, json: &'v JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'v str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics (fuel, time, policy).
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration (used by `.execute_formatted()`).
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers for graph source queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self
    where
        N: crate::GraphSourcePublisher,
    {
        let shared = Arc::new(crate::graph_source::FlureeR2rmlProvider::new(self.fluree));
        let provider: Arc<dyn R2rmlProvider + 'v> = shared.clone();
        let table_provider: Arc<dyn R2rmlTableProvider + 'v> = shared;
        self.core.r2rml = Some((provider, table_provider));
        self.core.set_r2rml();
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.core.validate();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Execute the query and return raw [`QueryResult`].
    pub async fn execute(mut self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let r2rml = self.core.r2rml.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_view_with_r2rml(
                        self.view,
                        input,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => self.fluree.query(self.view, input).await,
        }
    }

    /// Execute and return formatted JSON output.
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let r2rml = self.core.r2rml.take();
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.take().unwrap();
        let result = match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_view_with_r2rml(
                        self.view,
                        input,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await?
            }
            None => self.fluree.query(self.view, input).await?,
        };
        match self.view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(self.view.as_graph_db_ref(), &format_config, policy)
                .await?),
            None => Ok(result
                .format_async(self.view.as_graph_db_ref(), &format_config)
                .await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        mut self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let r2rml = self.core.r2rml.take();
        let format_config = self.core.format.take();
        let tracking = self.core.tracking.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_tracked_with_r2rml(
                        self.view,
                        input,
                        format_config,
                        tracking,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => {
                self.fluree
                    .query_tracked(self.view, input, format_config, tracking)
                    .await
            }
        }
    }
}
