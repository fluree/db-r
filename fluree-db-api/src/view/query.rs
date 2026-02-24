//! Query execution against GraphDb
//!
//! Provides `query_view` and related methods that execute queries against
//! a composed view, respecting policy and reasoning wrappers.

use crate::query::helpers::{
    build_query_result, parse_and_validate_sparql, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits,
    tracker_for_tracked_endpoint,
};
use crate::view::{GraphDb, QueryInput};
use crate::{
    ApiError, ExecutableQuery, Fluree, NameService, QueryResult, Result, Storage, Tracker,
    TrackingOptions,
};
use fluree_db_query::execute::{execute_prepared, prepare_execution, ContextConfig};

// ============================================================================
// Query Execution
// ============================================================================

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    /// Execute a query against a composed view.
    ///
    /// Accepts JSON-LD or SPARQL via `QueryInput`. Wrapper settings
    /// (policy, reasoning) are applied automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use serde_json::json;
    ///
    /// let view = fluree.db("mydb:main").await?
    ///     .with_reasoning(ReasoningModes::owl2ql());
    ///
    /// // JSON-LD query
    /// let query = json!({"select": ["?s"], "where": [["?s", "?p", "?o"]]});
    /// let result = fluree.query_view(&view, &query).await?;
    ///
    /// // SPARQL query
    /// let result = fluree.query_view(&view, "SELECT * WHERE { ?s ?p ?o }").await?;
    /// ```
    ///
    /// # SPARQL Dataset Clause Restriction
    ///
    /// A `GraphDb` represents a single ledger. SPARQL queries with
    /// `FROM` or `FROM NAMED` clauses will be rejected. Use
    /// `query_connection_sparql` for multi-ledger queries.
    pub async fn query_view(
        &self,
        view: &GraphDb,
        q: impl Into<QueryInput<'_>>,
    ) -> Result<QueryResult> {
        let input = q.into();

        // 1. Parse to common IR
        let parse_start = std::time::Instant::now();
        let (vars, parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &view.snapshot, view.default_context.as_ref())?
            }
            QueryInput::Sparql(sparql) => {
                // Validate no dataset clauses
                self.validate_sparql_for_view(sparql)?;
                parse_sparql_to_ir(sparql, &view.snapshot, view.default_context.as_ref())?
            }
        };
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        // 2. Build executable with optional reasoning override
        let plan_start = std::time::Instant::now();
        let executable = self.build_executable_for_view(view, &parsed)?;
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        // 3. Get tracker for fuel limits only (no tracking overhead for non-tracked calls)
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute
        let exec_start = std::time::Instant::now();
        let batches = self
            .execute_view_internal(view, &vars, &executable, &tracker)
            .await?;
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        tracing::info!(
            parse_ms = format!("{:.2}", parse_ms),
            plan_ms = format!("{:.2}", plan_ms),
            exec_ms = format!("{:.2}", exec_ms),
            "query_view phases"
        );

        // 5. Build result
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            view.t,
            Some(view.overlay.clone()),
            view.binary_graph(),
        ))
    }

    /// Execute a query with tracking (Clojure parity).
    ///
    /// Returns a tracked response with fuel, time, and policy statistics.
    pub(crate) async fn query_view_tracked(
        &self,
        view: &GraphDb,
        q: impl Into<QueryInput<'_>>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        // Get tracker - use tracked endpoint helpers that default to all tracking enabled
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_tracked_endpoint(json),
            QueryInput::Sparql(_) => Tracker::new(TrackingOptions::all_enabled()),
        };

        // Parse
        let (vars, parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &view.snapshot, view.default_context.as_ref()).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
            QueryInput::Sparql(sparql) => {
                self.validate_sparql_for_view(sparql).map_err(|e| {
                    crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                })?;
                parse_sparql_to_ir(sparql, &view.snapshot, view.default_context.as_ref()).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
        };

        // Build executable with reasoning
        let executable = self.build_executable_for_view(view, &parsed).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
        })?;

        // Execute with tracking (use tracked variant for policy)
        let batches = self
            .execute_view_tracked(view, &vars, &executable, &tracker)
            .await
            .map_err(|e| {
                let status = query_error_to_status(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        // Build result
        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            view.t,
            Some(view.overlay.clone()),
            view.binary_graph(),
        );

        // Format with tracking
        let result_json = match view.policy() {
            Some(policy) => query_result
                .to_jsonld_async_with_policy_tracked(view.as_graph_db_ref(), policy, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .to_jsonld_async_tracked(view.as_graph_db_ref(), &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
        };

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Validate that SPARQL doesn't have dataset clauses (FROM/FROM NAMED).
    ///
    /// A GraphDb is single-ledger; dataset clauses would conflict with
    /// the view's ledger alias.
    fn validate_sparql_for_view(&self, sparql: &str) -> Result<()> {
        let ast = parse_and_validate_sparql(sparql)?;

        // Check for dataset clauses
        let has_dataset = match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Update(_) => false,
        };

        if has_dataset {
            return Err(ApiError::query(
                "SPARQL FROM/FROM NAMED clauses are not supported on a single-ledger view. \
                 Use query_connection_sparql for multi-ledger queries.",
            ));
        }

        Ok(())
    }

    /// Build an ExecutableQuery with optional reasoning override from view wrapper.
    fn build_executable_for_view(
        &self,
        view: &GraphDb,
        parsed: &fluree_db_query::parse::ParsedQuery,
    ) -> Result<ExecutableQuery> {
        // Start with the standard executable
        let mut executable = prepare_for_execution(parsed);

        // Apply wrapper reasoning if applicable
        if view.reasoning().is_some() {
            // Check query's reasoning state
            let query_has_reasoning = executable.options.reasoning.has_any_enabled();
            let query_disabled = executable.options.reasoning.is_disabled();

            // Apply precedence rules
            if let Some(effective) = view.effective_reasoning(query_has_reasoning, query_disabled) {
                executable.options.reasoning = effective.clone();
            }
        }

        Ok(executable)
    }

    /// Execute against view with policy awareness.
    ///
    /// Single internal path that handles both policy and non-policy execution.
    /// Threads `binary_store` from the view into `ContextConfig` so that
    /// `ScanOperator` can use `BinaryScanOperator` when available.
    pub(crate) async fn execute_view_internal(
        &self,
        view: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        let db = view.as_graph_db_ref();
        let prepared = prepare_execution(db, executable)
            .await
            .map_err(query_error_to_api_error)?;

        let spatial_map = view.binary_store.as_ref().map(|s| s.spatial_provider_map());
        let fulltext_map = view
            .binary_store
            .as_ref()
            .map(|s| s.fulltext_provider_map());

        let config = ContextConfig {
            tracker: Some(tracker),
            policy_enforcer: view.policy_enforcer().cloned(),
            binary_store: view.binary_store.clone(),
            binary_g_id: view.graph_id,
            dict_novelty: view.dict_novelty.clone(),
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(db, vars, prepared, config)
            .await
            .map_err(query_error_to_api_error)
    }

    /// Execute against view with policy awareness (tracked variant).
    ///
    /// Uses tracked execution functions to properly record fuel/time/policy stats.
    pub(crate) async fn execute_view_tracked(
        &self,
        view: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let db = view.as_graph_db_ref();
        let prepared = prepare_execution(db, executable).await?;

        let spatial_map = view.binary_store.as_ref().map(|s| s.spatial_provider_map());
        let fulltext_map = view
            .binary_store
            .as_ref()
            .map(|s| s.fulltext_provider_map());

        let config = ContextConfig {
            tracker: Some(tracker),
            policy_enforcer: view.policy_enforcer().cloned(),
            binary_store: view.binary_store.clone(),
            binary_g_id: view.graph_id,
            dict_novelty: view.dict_novelty.clone(),
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(db, vars, prepared, config).await
    }
}

// ============================================================================
// Error Conversion Helpers
// ============================================================================

fn query_error_to_api_error(err: fluree_db_query::QueryError) -> ApiError {
    ApiError::query(err.to_string())
}

/// Map QueryError to HTTP-ish status code.
fn query_error_to_status(err: &fluree_db_query::QueryError) -> u16 {
    status_for_query_error(err)
}

#[cfg(test)]
mod tests {

    use crate::FlureeBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_query_view_jsonld() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data (using full IRIs)
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query via view (using object format for where)
        let view = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = fluree.query_view(&view, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_view_sparql() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query via view with SPARQL (using full IRIs)
        let view = fluree.db("testdb:main").await.unwrap();
        let result = fluree
            .query_view(
                &view,
                "SELECT ?name WHERE { <http://example.org/alice> <http://example.org/name> ?name }",
            )
            .await
            .unwrap();

        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_view_sparql_with_dataset_clause_rejected() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.db("testdb:main").await.unwrap();

        // SPARQL with FROM clause should be rejected
        let result = fluree
            .query_view(
                &view,
                "SELECT * FROM <http://other.org/ledger> WHERE { ?s ?p ?o }",
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("FROM"));
    }

    #[tokio::test]
    async fn test_query_view_jsonld_format() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let view = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = view
            .query(&fluree)
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        // Should be JSON-LD formatted
        assert!(result.is_array() || result.is_object());
    }

    #[tokio::test]
    async fn test_query_view_with_time_travel() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data at t=1
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query at t=0 (before insert)
        let view = fluree.db_at_t("testdb:main", 0).await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });
        let result = fluree.query_view(&view, &query).await.unwrap();
        assert!(result.batches.is_empty() || result.batches[0].is_empty());

        // Query at t=1 (after insert)
        let view = fluree.db_at_t("testdb:main", 1).await.unwrap();
        let result = fluree.query_view(&view, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }
}
