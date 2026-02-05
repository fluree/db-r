use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::query::helpers::{
    build_query_result, build_sparql_result, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits, tracker_from_query_json,
};
use crate::{
    DataSource, ExecutableQuery, Fluree, FormatterConfig, HistoricalLedgerView, LedgerState,
    NoOpR2rmlProvider, OverlayProvider, PolicyContext, QueryResult, Result, Storage, Tracker,
    TrackingOptions, TrackingTally, VarRegistry,
};

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: crate::NameService,
{
    /// Execute a JSON-LD query against a ledger
    pub async fn query(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_jsonld_query(query_json, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let tracker = tracker_for_limits(query_json);

        let batches = self
            .execute_query_internal(ledger, &vars, &executable, &tracker)
            .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
        ))
    }

    /// Internal helper for query execution.
    ///
    /// Note: This path uses `LedgerState` which does not carry `binary_store`.
    /// Queries still execute correctly via `Db.range_provider` (which serves
    /// all `range_with_overlay()` callers automatically). The faster
    /// `BinaryScanOperator` path is available through the view-based API
    /// (`query_view` / `execute_view_internal`) which threads `binary_store`
    /// from `LedgerSnapshot` into `ContextConfig`.
    async fn execute_query_internal(
        &self,
        ledger: &LedgerState<S>,
        vars: &VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        let r2rml_provider = NoOpR2rmlProvider::new();
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());

        let batches = crate::execute_with_r2rml(
            source,
            vars,
            executable,
            tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(batches)
    }

    /// Explain a JSON-LD query (query optimization plan).
    pub async fn explain(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> Result<JsonValue> {
        crate::explain::explain_jsonld(&ledger.db, query_json).await
    }

    /// Execute a JSON-LD query and return formatted JSON-LD output.
    pub async fn query_jsonld(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> Result<JsonValue> {
        let result = self.query(ledger, query_json).await?;
        Ok(result.to_jsonld_async(&ledger.db).await?)
    }

    /// Clojure-parity alias: tracked query entrypoint for a loaded ledger.
    pub async fn query_tracked(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_jsonld_tracked(ledger, query_json).await
    }

    /// Execute a JSON-LD query and return a Clojure-parity tracked response.
    pub async fn query_jsonld_tracked(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let tracker = tracker_from_query_json(query_json);

        let (vars, parsed) = parse_jsonld_query(query_json, &ledger.db).map_err(|e| {
            crate::query::TrackedErrorResponse::from_error(400, e.to_string(), tracker.tally())
        })?;

        let executable = prepare_for_execution(&parsed);
        let r2rml_provider = NoOpR2rmlProvider::new();
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());

        let batches = crate::execute_with_r2rml(
            source,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await
        .map_err(|e| {
            crate::query::TrackedErrorResponse::from_error(
                status_for_query_error(&e),
                e.to_string(),
                tracker.tally(),
            )
        })?;

        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
        );

        let result_json = query_result
            .to_jsonld_async_tracked(&ledger.db, &tracker)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::from_error(500, e.to_string(), tracker.tally())
            })?;

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    /// Execute a JSON-LD query and format results using a custom formatter config (async).
    pub async fn query_format(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
        config: &FormatterConfig,
    ) -> Result<JsonValue> {
        let result = self.query(ledger, query_json).await?;
        Ok(result.format_async(&ledger.db, config).await?)
    }

    /// Execute a JSON-LD query with policy enforcement
    pub async fn query_with_policy(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_jsonld_query(query_json, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());

        let batches = fluree_db_query::execute_with_policy(source, &vars, &executable, policy)
            .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
        ))
    }

    /// Execute a SPARQL query with policy enforcement
    pub async fn query_sparql_with_policy(
        &self,
        ledger: &LedgerState<S>,
        sparql: &str,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());

        let batches = fluree_db_query::execute_with_policy(source, &vars, &executable, policy)
            .await?;

        Ok(build_sparql_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
        ))
    }

    /// Execute a pre-built triple pattern query (advanced/testing API)
    pub async fn query_pattern(
        &self,
        ledger: &LedgerState<S>,
        vars: &VarRegistry,
        pattern: crate::TriplePattern,
    ) -> Result<Vec<crate::Batch>> {
        let batches = crate::execute_pattern_with_overlay_at(
            &ledger.db,
            ledger.novelty.as_ref(),
            vars,
            pattern,
            ledger.t(),
            None,
        )
        .await?;
        Ok(batches)
    }

    /// Execute a SPARQL query against a ledger
    pub async fn query_sparql(&self, ledger: &LedgerState<S>, sparql: &str) -> Result<QueryResult> {
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let tracker = Tracker::disabled();

        let batches = self
            .execute_query_internal(ledger, &vars, &executable, &tracker)
            .await?;

        Ok(build_sparql_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
        ))
    }

    /// Execute a SPARQL query with tracking (fuel counting, time, policy stats).
    pub async fn query_sparql_tracked(
        &self,
        ledger: &LedgerState<S>,
        sparql: &str,
        options: TrackingOptions,
    ) -> Result<(QueryResult, Option<TrackingTally>)> {
        let tracker = Tracker::new(options);
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let batches = self
            .execute_query_internal(ledger, &vars, &executable, &tracker)
            .await?;

        let tally = tracker.tally();

        Ok((
            build_sparql_result(
                vars,
                parsed,
                batches,
                ledger.t(),
                Some(ledger.novelty.clone()),
            ),
            tally,
        ))
    }

    /// Execute a JSON-LD query against a historical ledger view
    pub async fn query_historical(
        &self,
        view: &HistoricalLedgerView<S>,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_jsonld_query(query_json, &view.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let r2rml_provider = NoOpR2rmlProvider::new();
        let tracker = Tracker::disabled();
        let overlay: &dyn fluree_db_core::OverlayProvider = view
            .overlay()
            .map(|n| n.as_ref() as &dyn fluree_db_core::OverlayProvider)
            .unwrap_or(&fluree_db_core::NoOverlay);
        let source = DataSource::new(&view.db, overlay, view.to_t());

        let batches = crate::execute_with_r2rml(
            source,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            view.to_t(),
            view.overlay()
                .map(|n| Arc::clone(n) as Arc<dyn OverlayProvider>),
        ))
    }
}
