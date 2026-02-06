//! Query builders for the [`Graph`] and [`Db`] API.
//!
//! - [`GraphQueryBuilder`] — lazy query from a [`Graph`] handle (defers view load)
//! - [`GraphSnapshotQueryBuilder`] — query from a materialized snapshot or [`StagedGraph`]

use serde_json::Value as JsonValue;

use crate::error::BuilderErrors;
use crate::format::FormatterConfig;
use crate::graph::Graph;
use crate::query::builder::QueryCore;
use crate::view::FlureeView;
use crate::{
    ApiError, Fluree, NameService, QueryResult, Result, Storage, TrackedErrorResponse,
    TrackedQueryResponse, TrackingOptions,
};

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

    /// Set the query input as JSON-LD/FQL.
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

    /// Enable BM25/Vector index providers for virtual graph queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self {
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
    pub async fn execute(self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let view = self
            .graph
            .fluree
            .load_view_at(&self.graph.alias, self.graph.time_spec.clone())
            .await?;
        let input = self.core.input.unwrap();
        self.graph.fluree.query_view(&view, input).await
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
            .load_view_at(&self.graph.alias, self.graph.time_spec.clone())
            .await?;
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.unwrap();
        let result = self.graph.fluree.query_view(&view, input).await?;
        let config = format_config.with_select_mode(result.select_mode);
        match view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(&view.db, &config, policy)
                .await?),
            None => Ok(result.format_async(&view.db, &config).await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let view = self
            .graph
            .fluree
            .load_view_at(&self.graph.alias, self.graph.time_spec.clone())
            .await
            .map_err(|e| TrackedErrorResponse::from_error(404, e.to_string(), None))?;
        let input = self.core.input.unwrap();
        self.graph.fluree.query_view_tracked(&view, input).await
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
    view: &'v FlureeView<S>,
    core: QueryCore<'v>,
}

impl<'a, 'v, S, N> GraphSnapshotQueryBuilder<'a, 'v, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder from a fluree reference and a view.
    pub(crate) fn new_from_parts(fluree: &'a Fluree<S, N>, view: &'v FlureeView<S>) -> Self {
        Self {
            fluree,
            view,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD/FQL.
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

    /// Enable BM25/Vector index providers for virtual graph queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self {
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
    pub async fn execute(self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let input = self.core.input.unwrap();
        self.fluree.query_view(self.view, input).await
    }

    /// Execute and return formatted JSON output.
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.unwrap();
        let result = self.fluree.query_view(self.view, input).await?;
        let config = format_config.with_select_mode(result.select_mode);
        match self.view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(&self.view.db, &config, policy)
                .await?),
            None => Ok(result.format_async(&self.view.db, &config).await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let input = self.core.input.unwrap();
        self.fluree.query_view_tracked(self.view, input).await
    }
}
