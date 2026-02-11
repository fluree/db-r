//! Query builders: context-first, compile-time-safe query construction.
//!
//! Three separate builder types, one per target:
//! - [`ViewQueryBuilder`] — query a single graph/view
//! - [`DatasetQueryBuilder`] — query a composed dataset
//! - [`FromQueryBuilder`] — FROM-driven query (query body specifies ledgers)
//!
//! # Design
//!
//! - **Infallible setters**: All setters return `Self`; errors are accumulated
//!   and reported at `.execute()` / `.validate()`.
//! - **Compile-time target safety**: `.policy()` is only on `FromQueryBuilder`.
//! - **Composition**: All builders share `QueryCore` for common fields.

use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::error::{BuilderError, BuilderErrors};
use crate::format::FormatterConfig;
use crate::query::helpers::parse_dataset_spec;
use crate::view::{FlureeDataSetView, FlureeView, QueryInput};
use crate::{
    ApiError, Fluree, NameService, PolicyContext, QueryResult, Result, Storage,
    TrackedErrorResponse, TrackedQueryResponse, TrackingOptions,
};

// ============================================================================
// GraphSourceMode
// ============================================================================

/// Graph source integration mode for query builders.
#[derive(Debug, Clone, Default)]
pub enum GraphSourceMode {
    /// No graph source integration (default).
    #[default]
    None,
    /// Enable BM25/Vector index providers.
    IndexProviders,
    /// Enable R2RML/Iceberg support (feature-gated).
    #[cfg(feature = "iceberg")]
    R2rml,
}

// ============================================================================
// QueryCore (shared, private)
// ============================================================================

/// Shared fields across all three query builders.
///
/// Not exported. Each builder contains one and delegates shared setters to it.
pub(crate) struct QueryCore<'a> {
    pub(crate) input: Option<QueryInput<'a>>,
    pub(crate) tracking: Option<TrackingOptions>,
    pub(crate) format: Option<FormatterConfig>,
    pub(crate) graph_sources: GraphSourceMode,
    errors: Vec<BuilderError>,
}

impl<'a> QueryCore<'a> {
    pub(crate) fn new() -> Self {
        Self {
            input: None,
            tracking: None,
            format: None,
            graph_sources: GraphSourceMode::None,
            errors: Vec::new(),
        }
    }

    pub(crate) fn set_jsonld(&mut self, json: &'a JsonValue) {
        if self.input.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "input",
                message: "query input already set; cannot call .jsonld() after .sparql() or another .jsonld()".into(),
            });
            return;
        }
        self.input = Some(QueryInput::JsonLd(json));
    }

    pub(crate) fn set_sparql(&mut self, sparql: &'a str) {
        if self.input.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "input",
                message: "query input already set; cannot call .sparql() after .jsonld() or another .sparql()".into(),
            });
            return;
        }
        self.input = Some(QueryInput::Sparql(sparql));
    }

    pub(crate) fn set_tracking(&mut self, opts: TrackingOptions) {
        self.tracking = Some(opts);
    }

    pub(crate) fn set_track_all(&mut self) {
        self.tracking = Some(TrackingOptions {
            track_time: true,
            track_fuel: true,
            track_policy: true,
            max_fuel: None,
        });
    }

    pub(crate) fn set_format(&mut self, config: FormatterConfig) {
        self.format = Some(config);
    }

    pub(crate) fn set_index_providers(&mut self) {
        self.graph_sources = GraphSourceMode::IndexProviders;
    }

    #[cfg(feature = "iceberg")]
    pub(crate) fn set_r2rml(&mut self) {
        self.graph_sources = GraphSourceMode::R2rml;
    }

    /// Validate that required fields are set. Returns accumulated errors.
    pub(crate) fn validate(&self) -> Vec<BuilderError> {
        let mut errs = self.errors.clone();
        if self.input.is_none() {
            errs.push(BuilderError::Missing {
                field: "input",
                hint: "call .jsonld(&query) or .sparql(\"SELECT ...\")",
            });
        }
        if !matches!(self.graph_sources, GraphSourceMode::None) {
            errs.push(BuilderError::Invalid {
                field: "graph_sources",
                message: "Graph source modes (.with_index_providers(), .with_r2rml()) are not yet supported by query builders; use the direct query methods instead".into(),
            });
        }
        errs
    }

    /// Default format config based on input type.
    pub(crate) fn default_format(&self) -> FormatterConfig {
        match &self.input {
            Some(QueryInput::Sparql(_)) => FormatterConfig::sparql_json(),
            _ => FormatterConfig::jsonld(),
        }
    }
}

// ============================================================================
// ViewQueryBuilder
// ============================================================================

/// Builder for queries against a single graph/view.
///
/// Created via [`FlureeView::query()`].
///
/// # Example
///
/// ```ignore
/// let graph = fluree.view("mydb:main").await?;
/// let result = graph.query(&fluree)
///     .jsonld(&query)
///     .execute().await?;
/// ```
pub struct ViewQueryBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    view: &'a FlureeView,
    core: QueryCore<'a>,
}

impl<'a, S, N> ViewQueryBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    /// Create a new builder (called by `FlureeView::query()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, view: &'a FlureeView) -> Self {
        Self {
            fluree,
            view,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters (delegated to QueryCore) ---

    /// Set the query input as JSON-LD/FQL.
    pub fn jsonld(mut self, json: &'a JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'a str) -> Self {
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
    pub fn with_r2rml(mut self) -> Self {
        self.core.set_r2rml();
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    ///
    /// Returns all accumulated errors at once.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.core.validate();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Execute the query and return raw `QueryResult`.
    pub async fn execute(self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let input = self.core.input.unwrap(); // safe: validated
        self.fluree.query_view(self.view, input).await
    }

    /// Execute and return formatted JSON output.
    ///
    /// Uses `.format()` config if set, otherwise defaults based on input type
    /// (JSON-LD for `.jsonld()`, SPARQL JSON for `.sparql()`).
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
    ///
    /// **Note**: Custom `.tracking()` options are not yet wired through this
    /// path. Fuel limits are extracted from the JSON-LD query body (if present);
    /// SPARQL queries use default tracking. This will be addressed in a future
    /// iteration when the tracked execution internals are refactored to accept
    /// external tracker configuration.
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

// ============================================================================
// DatasetQueryBuilder
// ============================================================================

/// Builder for queries against a composed dataset (multiple graphs/views).
///
/// Created via [`FlureeDataSetView::query()`].
///
/// # Example
///
/// ```ignore
/// let dataset = FlureeDataSetView::new()
///     .with_default(view_a)
///     .with_named("other", view_b);
/// let result = dataset.query(&fluree)
///     .jsonld(&query)
///     .execute().await?;
/// ```
pub struct DatasetQueryBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    dataset: &'a FlureeDataSetView,
    core: QueryCore<'a>,
}

impl<'a, S, N> DatasetQueryBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    /// Create a new builder (called by `FlureeDataSetView::query()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, dataset: &'a FlureeDataSetView) -> Self {
        Self {
            fluree,
            dataset,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD/FQL.
    pub fn jsonld(mut self, json: &'a JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'a str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics.
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration.
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers.
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

    /// Execute the query and return raw `QueryResult`.
    pub async fn execute(self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let input = self.core.input.unwrap();
        self.fluree.query_dataset_view(self.dataset, input).await
    }

    /// Execute and return formatted JSON output.
    ///
    /// Uses `.format()` config if set, otherwise defaults based on input type.
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
        let result = self.fluree.query_dataset_view(self.dataset, input).await?;

        // Use primary view's db for formatting
        if let Some(primary) = self.dataset.primary() {
            let config = format_config.with_select_mode(result.select_mode);
            match primary.policy() {
                Some(policy) => Ok(result
                    .format_async_with_policy(&primary.db, &config, policy)
                    .await?),
                None => Ok(result.format_async(&primary.db, &config).await?),
            }
        } else {
            Err(ApiError::query("No primary view in dataset for formatting"))
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    ///
    /// **Note**: Custom `.tracking()` options are not yet wired through this
    /// path. See [`ViewQueryBuilder::execute_tracked()`] for details.
    pub async fn execute_tracked(
        self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let input = self.core.input.unwrap();
        self.fluree
            .query_dataset_view_tracked(self.dataset, input)
            .await
    }
}

// ============================================================================
// FromQueryBuilder
// ============================================================================

/// Builder for FROM-driven queries (query body specifies target ledgers).
///
/// Created via [`Fluree::query_from()`].
///
/// This is the only query builder that supports `.policy()`, because
/// FROM-driven queries resolve ledgers at execution time from the connection
/// and may need connection-level policy enforcement.
///
/// # Example
///
/// ```ignore
/// let result = fluree.query_from()
///     .jsonld(&query_with_from)
///     .policy(ctx)
///     .execute().await?;
/// ```
pub struct FromQueryBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    core: QueryCore<'a>,
    policy: Option<Arc<PolicyContext>>,
}

impl<'a, S, N> FromQueryBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Fluree::query_from()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>) -> Self {
        Self {
            fluree,
            core: QueryCore::new(),
            policy: None,
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD/FQL.
    pub fn jsonld(mut self, json: &'a JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'a str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics.
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration.
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers.
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

    // --- FromQueryBuilder-only setters ---

    /// Set the policy context for this query.
    ///
    /// Only available on `FromQueryBuilder` — for view/dataset queries,
    /// policy is applied at the view level (Tier 1).
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.policy = Some(Arc::new(ctx));
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

    /// Execute the query and return raw `QueryResult`.
    ///
    /// Resolves ledgers from the query body's `from` / `FROM` clauses,
    /// applies policy if set, and executes.
    pub async fn execute(self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let input = self.core.input.unwrap();
        match input {
            QueryInput::JsonLd(json) => match &self.policy {
                Some(policy) => self.fluree.query_connection_with_policy(json, policy).await,
                None => self.fluree.query_connection(json).await,
            },
            QueryInput::Sparql(sparql) => match &self.policy {
                Some(policy) => {
                    self.fluree
                        .query_connection_sparql_with_policy(sparql, policy)
                        .await
                }
                None => self.fluree.query_connection_sparql(sparql).await,
            },
        }
    }

    /// Execute and return formatted JSON output.
    ///
    /// Uses `.format()` config if set, otherwise defaults based on input type
    /// (JSON-LD for `.jsonld()`, SPARQL JSON for `.sparql()`).
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
        match input {
            QueryInput::JsonLd(json) => {
                let result = match &self.policy {
                    Some(policy) => {
                        self.fluree
                            .query_connection_with_policy(json, policy)
                            .await?
                    }
                    None => self.fluree.query_connection(json).await?,
                };
                let (spec, _) = parse_dataset_spec(json)?;
                if let Some(alias) = spec.default_graphs.first() {
                    let view = self.fluree.view(alias.identifier.as_str()).await?;
                    let config = format_config.with_select_mode(result.select_mode);
                    Ok(result.format_async(&view.db, &config).await?)
                } else {
                    Err(ApiError::query("No default graph for formatting"))
                }
            }
            QueryInput::Sparql(sparql) => {
                let result = match &self.policy {
                    Some(policy) => {
                        self.fluree
                            .query_connection_sparql_with_policy(sparql, policy)
                            .await?
                    }
                    None => self.fluree.query_connection_sparql(sparql).await?,
                };
                let ast = crate::query::helpers::parse_and_validate_sparql(sparql)?;
                let spec = crate::query::helpers::extract_sparql_dataset_spec(&ast)?;
                if let Some(alias) = spec.default_graphs.first() {
                    let view = self.fluree.view(alias.identifier.as_str()).await?;
                    let config = format_config.with_select_mode(result.select_mode);
                    Ok(result.format_async(&view.db, &config).await?)
                } else {
                    Err(ApiError::query("No default graph for formatting"))
                }
            }
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    ///
    /// **Note**: Custom `.tracking()` options are not yet wired through this
    /// path. See [`ViewQueryBuilder::execute_tracked()`] for details.
    pub async fn execute_tracked(
        self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let input = self.core.input.unwrap();
        match input {
            QueryInput::JsonLd(json) => match &self.policy {
                Some(policy) => {
                    self.fluree
                        .query_connection_jsonld_tracked_with_policy(json, policy)
                        .await
                }
                None => self.fluree.query_connection_jsonld_tracked(json).await,
            },
            QueryInput::Sparql(sparql) => match &self.policy {
                Some(policy) => {
                    self.fluree
                        .query_connection_sparql_tracked_with_policy(sparql, policy)
                        .await
                }
                None => self.fluree.query_connection_sparql_tracked(sparql).await,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;
    use serde_json::json;

    // ========================================================================
    // Validation tests
    // ========================================================================

    #[test]
    fn test_query_core_missing_input() {
        let core = QueryCore::new();
        let errs = core.validate();
        assert_eq!(errs.len(), 1);
        match &errs[0] {
            BuilderError::Missing { field, .. } => assert_eq!(*field, "input"),
            _ => panic!("Expected Missing error"),
        }
    }

    #[test]
    fn test_query_core_jsonld_then_sparql_conflict() {
        let json = json!({"select": ["?s"], "where": [{"@id": "?s"}]});
        let mut core = QueryCore::new();
        core.set_jsonld(&json);
        core.set_sparql("SELECT ?s WHERE { ?s ?p ?o }");
        let errs = core.validate();
        assert_eq!(errs.len(), 1);
        match &errs[0] {
            BuilderError::Conflict { field, .. } => assert_eq!(*field, "input"),
            _ => panic!("Expected Conflict error"),
        }
    }

    #[test]
    fn test_query_core_sparql_then_jsonld_conflict() {
        let json = json!({"select": ["?s"]});
        let mut core = QueryCore::new();
        core.set_sparql("SELECT ?s WHERE { ?s ?p ?o }");
        core.set_jsonld(&json);
        let errs = core.validate();
        assert_eq!(errs.len(), 1);
        match &errs[0] {
            BuilderError::Conflict { field, .. } => assert_eq!(*field, "input"),
            _ => panic!("Expected Conflict error"),
        }
    }

    #[test]
    fn test_query_core_double_jsonld_conflict() {
        let json1 = json!({"select": ["?s"]});
        let json2 = json!({"select": ["?o"]});
        let mut core = QueryCore::new();
        core.set_jsonld(&json1);
        core.set_jsonld(&json2);
        let errs = core.validate();
        assert_eq!(errs.len(), 1);
        match &errs[0] {
            BuilderError::Conflict { field, .. } => assert_eq!(*field, "input"),
            _ => panic!("Expected Conflict error"),
        }
    }

    #[test]
    fn test_query_core_valid_jsonld() {
        let json = json!({"select": ["?s"]});
        let mut core = QueryCore::new();
        core.set_jsonld(&json);
        let errs = core.validate();
        assert!(errs.is_empty());
    }

    #[test]
    fn test_query_core_valid_sparql() {
        let mut core = QueryCore::new();
        core.set_sparql("SELECT ?s WHERE { ?s ?p ?o }");
        let errs = core.validate();
        assert!(errs.is_empty());
    }

    // ========================================================================
    // Builder construction tests
    // ========================================================================

    #[test]
    fn test_view_query_builder_validate_missing_input() {
        let fluree = FlureeBuilder::memory().build_memory();
        // We can't create a view without a ledger, so test validate on FromQueryBuilder instead
        let builder = FromQueryBuilder::new(&fluree);
        let result = builder.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert_eq!(errs.0.len(), 1);
        assert!(matches!(
            &errs.0[0],
            BuilderError::Missing { field: "input", .. }
        ));
    }

    #[test]
    fn test_from_query_builder_validate_with_input() {
        let fluree = FlureeBuilder::memory().build_memory();
        let query = json!({
            "from": "test:main",
            "select": ["?s"],
            "where": [{"@id": "?s"}]
        });
        let builder = fluree.query_from().jsonld(&query);
        let result = builder.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_from_query_builder_validate_conflict() {
        let fluree = FlureeBuilder::memory().build_memory();
        let query = json!({"from": "test:main", "select": ["?s"]});
        let builder = fluree
            .query_from()
            .jsonld(&query)
            .sparql("SELECT ?s WHERE { ?s ?p ?o }");
        let result = builder.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs
            .0
            .iter()
            .any(|e| matches!(e, BuilderError::Conflict { field: "input", .. })));
    }

    // ========================================================================
    // Integration tests (requires async runtime)
    // ========================================================================

    #[tokio::test]
    async fn test_view_query_builder_execute() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        // Insert data so the query has something to find
        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let _result = fluree.update(ledger, &data).await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?s"],
            "where": [{"@id": "?s", "ex:name": "?name"}]
        });

        let result = view.query(&fluree).jsonld(&query).execute().await;
        assert!(
            result.is_ok(),
            "ViewQueryBuilder execute failed: {:?}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_view_query_builder_sparql() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        let result = view
            .query(&fluree)
            .sparql("SELECT ?s WHERE { ?s ?p ?o }")
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_view_query_builder_missing_input_returns_error() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.view("testdb:main").await.unwrap();
        // No .jsonld() or .sparql() call
        let result = view.query(&fluree).execute().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code(), 400);
    }

    #[tokio::test]
    async fn test_from_query_builder_execute() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        // Insert data so the query has something to find
        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let _result = fluree.update(ledger, &data).await.unwrap();

        let query = json!({
            "from": "testdb:main",
            "select": ["?s"],
            "where": [{"@id": "?s", "ex:name": "?name"}]
        });

        let result = fluree.query_from().jsonld(&query).execute().await;
        assert!(
            result.is_ok(),
            "FromQueryBuilder execute failed: {:?}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_view_query_equivalence_with_convenience() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        // Insert some data
        let data = json!({
            "insert": [{"@id": "ex:a", "ex:name": "Alice"}]
        });
        let result = fluree.update(ledger, &data).await.unwrap();
        let ledger = result.ledger;

        let query = json!({
            "select": ["?name"],
            "where": [{"@id": "?s", "ex:name": "?name"}]
        });

        // Via convenience method
        let result_convenience = fluree.query(&ledger, &query).await.unwrap();

        // Via builder
        let view = fluree.view("testdb:main").await.unwrap();
        let result_builder = view.query(&fluree).jsonld(&query).execute().await.unwrap();

        // Both should produce results at the same t
        assert_eq!(result_convenience.t, result_builder.t);
    }
}
