//! Query execution against FlureeDataSetView
//!
//! Provides `query_dataset_view` for multi-ledger queries.

use crate::query::helpers::{
    build_query_result, parse_and_validate_sparql, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits, tracker_for_tracked_endpoint,
};
use crate::view::{FlureeDataSetView, QueryInput};
use crate::{
    ApiError, ExecutableQuery, Fluree, NameService, QueryResult, Result, Storage,
    Tracker, TrackingOptions,
};
use fluree_db_query::execute::{ContextConfig, execute_prepared, prepare_execution};

// ============================================================================
// Dataset Query Execution
// ============================================================================

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    /// Execute a query against a dataset view (multi-ledger).
    ///
    /// For single-ledger datasets, this delegates to `query_view`.
    /// For multi-ledger datasets, this executes against the merged default graphs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view1 = fluree.view("ledger1:main").await?;
    /// let view2 = fluree.view("ledger2:main").await?;
    ///
    /// let dataset = FlureeDataSetView::new()
    ///     .with_default(view1)
    ///     .with_default(view2);
    ///
    /// let result = fluree.query_dataset_view(&dataset, &query).await?;
    /// ```
    pub async fn query_dataset_view(
        &self,
        dataset: &FlureeDataSetView<S>,
        q: impl Into<QueryInput<'_>>,
    ) -> Result<QueryResult> {
        let input = q.into();

        // Single-ledger fast path (only safe for JSON-LD or SPARQL without dataset clauses).
        if dataset.is_single_ledger() {
            if let Some(view) = dataset.primary() {
                match &input {
                    QueryInput::JsonLd(_) => return self.query_view(view, input).await,
                    QueryInput::Sparql(sparql) => {
                        let ast = parse_and_validate_sparql(sparql)?;
                        let has_dataset = match &ast.body {
                            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Update(_) => false,
                        };
                        if !has_dataset {
                            return self.query_view(view, input).await;
                        }
                    }
                }
            }
        }

        // Require at least one default graph.
        //
        // IMPORTANT (multi-ledger semantics):
        // - We intentionally treat the *first* default graph as the "primary" view.
        // - The primary db is used for:
        //   - parsing / namespace resolution
        //   - reasoning defaults
        //   - query planning / optimization stats (HLL/NDV)
        //
        // Execution still scans *all* default graphs in the dataset (union semantics),
        // but optimization is driven by the primary graph under the assumption that
        // default graphs in a dataset represent similarly-shaped data.
        let primary = dataset.primary().ok_or_else(|| {
            ApiError::query("Dataset has no graphs for query execution")
        })?;

        // 1. Parse to common IR (using primary db for namespace resolution).
        let (vars, parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(json, &primary.db)?,
            QueryInput::Sparql(sparql) => {
                // For dataset view, SPARQL FROM/FROM NAMED are allowed
                // (they were validated when building the dataset)
                parse_sparql_to_ir(sparql, &primary.db)?
            }
        };

        // 2. Build executable with optional reasoning override from primary view
        let executable = self.build_executable_for_dataset(dataset, &parsed)?;

        // 3. Get tracker for fuel limits
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute against merged dataset
        let batches = self
            .execute_dataset_internal(dataset, &vars, &executable, &tracker)
            .await?;

        // 5. Build result with max_t across all views
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            dataset.max_t(),
            dataset.composite_overlay(),
        ))
    }

    /// Execute a dataset query with tracking.
    pub(crate) async fn query_dataset_view_tracked(
        &self,
        dataset: &FlureeDataSetView<S>,
        q: impl Into<QueryInput<'_>>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        // Get tracker - use tracked endpoint helpers that default to all tracking enabled
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_tracked_endpoint(json),
            QueryInput::Sparql(_) => Tracker::new(TrackingOptions::all_enabled()),
        };

        // Require primary
        let primary = dataset.primary().ok_or_else(|| {
            crate::query::TrackedErrorResponse::from_error(
                400,
                "Dataset has no graphs",
                tracker.tally(),
            )
        })?;

        // Parse
        let (vars, parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(json, &primary.db).map_err(|e| {
                crate::query::TrackedErrorResponse::from_error(400, e.to_string(), tracker.tally())
            })?,
            QueryInput::Sparql(sparql) => parse_sparql_to_ir(sparql, &primary.db).map_err(|e| {
                crate::query::TrackedErrorResponse::from_error(400, e.to_string(), tracker.tally())
            })?,
        };

        // Build executable
        let executable = self
            .build_executable_for_dataset(dataset, &parsed)
            .map_err(|e| {
                crate::query::TrackedErrorResponse::from_error(400, e.to_string(), tracker.tally())
            })?;

        // Execute with tracking
        let batches = self
            .execute_dataset_tracked(dataset, &vars, &executable, &tracker)
            .await
            .map_err(|e| {
                let status = status_for_query_error(&e);
                crate::query::TrackedErrorResponse::from_error(
                    status,
                    e.to_string(),
                    tracker.tally(),
                )
            })?;

        // Build result
        let query_result = build_query_result(vars, parsed, batches, dataset.max_t(), None);

        // Format with tracking
        let result_json = match primary.policy() {
            Some(policy) => query_result
                .to_jsonld_async_with_policy_tracked(&primary.db, policy, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::from_error(
                        500,
                        e.to_string(),
                        tracker.tally(),
                    )
                })?,
            None => query_result
                .to_jsonld_async_tracked(&primary.db, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::from_error(
                        500,
                        e.to_string(),
                        tracker.tally(),
                    )
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

    /// Build an ExecutableQuery for dataset queries.
    ///
    /// Applies reasoning from the primary view if set.
    fn build_executable_for_dataset(
        &self,
        dataset: &FlureeDataSetView<S>,
        parsed: &fluree_db_query::parse::ParsedQuery,
    ) -> Result<ExecutableQuery> {
        let mut executable = prepare_for_execution(parsed);

        // Apply reasoning from primary view if set
        if let Some(primary) = dataset.primary() {
            if primary.reasoning().is_some() {
                let query_has_reasoning = executable.options.reasoning.has_any_enabled();
                let query_disabled = executable.options.reasoning.is_disabled();

                if let Some(effective) =
                    primary.effective_reasoning(query_has_reasoning, query_disabled)
                {
                    executable.options.reasoning = effective.clone();
                }
            }
        }

        Ok(executable)
    }

    /// Execute against dataset (multi-ledger).
    ///
    /// Calls `prepare_execution` + `execute_prepared` directly so that
    /// `binary_store` from the primary view is threaded into the
    /// `ExecutionContext` for `BinaryScanOperator`.
    async fn execute_dataset_internal(
        &self,
        dataset: &FlureeDataSetView<S>,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        // Primary default graph drives planning/optimization.
        //
        // NOTE: We pass `primary.db` to the query engine as the "planning db".
        // The engine will attach `runtime_dataset` to the ExecutionContext and scans
        // will union across all default graphs, but planning (including stats-based
        // reordering) is intentionally based on this primary graph for now.
        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no default graphs"))?;

        let runtime_dataset = dataset.as_runtime_dataset();

        let prepared = prepare_execution(&primary.db, primary.overlay.as_ref(), executable, primary.to_t)
            .await
            .map_err(query_error_to_api_error)?;

        let (from_t, to_t, history_mode) = match dataset.history_time_range() {
            Some((hist_from, hist_to)) => (Some(hist_from), hist_to, true),
            None => (None, primary.to_t, false),
        };

        let config = ContextConfig {
            tracker: if tracker.is_enabled() { Some(tracker) } else { None },
            dataset: Some(&runtime_dataset),
            binary_store: primary.binary_store.clone(),
            dict_novelty: primary.dict_novelty.clone(),
            history_mode,
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(
            &primary.db,
            vars,
            primary.overlay.as_ref(),
            prepared,
            to_t,
            from_t,
            config,
        )
        .await
        .map_err(query_error_to_api_error)
    }

    /// Execute against dataset with tracking.
    ///
    /// Threads `binary_store` from the primary view into the execution context.
    async fn execute_dataset_tracked(
        &self,
        dataset: &FlureeDataSetView<S>,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let primary = dataset.primary().ok_or_else(|| {
            fluree_db_query::QueryError::InvalidQuery("Dataset has no default graphs".into())
        })?;

        let runtime_dataset = dataset.as_runtime_dataset();

        let prepared = prepare_execution(&primary.db, primary.overlay.as_ref(), executable, primary.to_t)
            .await?;

        let (from_t, to_t, history_mode) = match dataset.history_time_range() {
            Some((hist_from, hist_to)) => (Some(hist_from), hist_to, true),
            None => (None, primary.to_t, false),
        };

        let config = ContextConfig {
            tracker: Some(tracker),
            dataset: Some(&runtime_dataset),
            binary_store: primary.binary_store.clone(),
            dict_novelty: primary.dict_novelty.clone(),
            history_mode,
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(
            &primary.db,
            vars,
            primary.overlay.as_ref(),
            prepared,
            to_t,
            from_t,
            config,
        )
        .await
    }
}

fn query_error_to_api_error(err: fluree_db_query::QueryError) -> ApiError {
    ApiError::query(err.to_string())
}

#[cfg(test)]
mod tests {
    
    use crate::view::FlureeDataSetView;
    use crate::FlureeBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_query_dataset_view_single_ledger() {
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

        // Query via dataset view (single ledger)
        let view = fluree.view("testdb:main").await.unwrap();
        let dataset = FlureeDataSetView::single(view);

        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = fluree.query_dataset_view(&dataset, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_dataset_view_formatted() {
        let fluree = FlureeBuilder::memory().build_memory();

        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/bob",
                "http://example.org/name": "Bob"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let view = fluree.view("testdb:main").await.unwrap();
        let dataset = FlureeDataSetView::single(view);

        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/bob", "http://example.org/name": "?name"}
        });

        let result = dataset
            .query(&fluree)
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();
        assert!(result.is_array() || result.is_object());
    }

    #[tokio::test]
    async fn test_query_dataset_view_multi_ledger_union() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Two independent ledgers with distinct subjects
        let ledger1 = fluree.create_ledger("db1").await.unwrap();
        let ledger2 = fluree.create_ledger("db2").await.unwrap();

        let txn1 = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger1 = fluree.update(ledger1, &txn1).await.unwrap().ledger;

        let txn2 = json!({
            "insert": [{
                "@id": "http://example.org/bob",
                "http://example.org/name": "Bob"
            }]
        });
        let _ledger2 = fluree.update(ledger2, &txn2).await.unwrap().ledger;

        let view1 = fluree.view("db1:main").await.unwrap();
        let view2 = fluree.view("db2:main").await.unwrap();

        let dataset = FlureeDataSetView::new()
            .with_default(view1)
            .with_default(view2);

        let query = json!({
            "select": ["?s", "?name"],
            "where": {
                "@id": "?s",
                "http://example.org/name": "?name"
            }
        });

        let result = fluree.query_dataset_view(&dataset, &query).await.unwrap();
        let total_solutions: usize = result.batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_solutions, 2);
    }

    #[tokio::test]
    async fn test_query_empty_dataset_error() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let dataset: FlureeDataSetView<_> = FlureeDataSetView::new();
        let query = json!({ "select": ["?s"], "where": {"@id": "?s"} });

        let result = fluree.query_dataset_view(&dataset, &query).await;
        assert!(result.is_err());
    }
}
