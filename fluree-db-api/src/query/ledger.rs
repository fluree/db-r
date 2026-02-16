use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::query::helpers::{
    build_query_result, build_sparql_result, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits, tracker_from_query_json,
};
use crate::{
    ExecutableQuery, Fluree, FormatterConfig, HistoricalLedgerView, LedgerState, NoOpR2rmlProvider,
    OverlayProvider, PolicyContext, QueryResult, Result, Storage, Tracker, TrackingOptions,
    TrackingTally, VarRegistry,
};
use fluree_db_indexer::run_index::BinaryIndexStore;
use fluree_db_query::execute::{execute_prepared, prepare_execution, ContextConfig, DataSource};

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: crate::NameService,
{
    /// Execute a JSON-LD query against a ledger
    pub async fn query(&self, ledger: &LedgerState, query_json: &JsonValue) -> Result<QueryResult> {
        let (vars, parsed) =
            parse_jsonld_query(query_json, &ledger.db, ledger.default_context.as_ref())?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let tracker = tracker_for_limits(query_json);

        let batches = self
            .execute_query_internal(ledger, &vars, &executable, &tracker)
            .await?;

        let binary_store = ledger
            .binary_store
            .as_ref()
            .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
            binary_store,
        ))
    }

    /// Internal helper for query execution.
    ///
    /// When `LedgerState.binary_store` is available, threads it into
    /// `ContextConfig` so `ScanOperator` uses `BinaryScanOperator` for
    /// correct IRI-to-SID resolution at scan time.
    async fn execute_query_internal(
        &self,
        ledger: &LedgerState,
        vars: &VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        let binary_store = ledger
            .binary_store
            .as_ref()
            .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());

        let prepared =
            prepare_execution(&ledger.db, ledger.novelty.as_ref(), executable, ledger.t()).await?;

        let r2rml_provider = NoOpR2rmlProvider::new();

        let config = ContextConfig {
            tracker: Some(tracker),
            r2rml: Some((&r2rml_provider, &r2rml_provider)),
            binary_store: binary_store.clone(),
            dict_novelty: if binary_store.is_some() {
                Some(ledger.dict_novelty.clone())
            } else {
                None
            },
            strict_bind_errors: true,
            ..Default::default()
        };

        let source = DataSource {
            db: &ledger.db,
            overlay: ledger.novelty.as_ref(),
            to_t: ledger.t(),
            from_t: None,
        };
        let batches = execute_prepared(source, vars, prepared, config).await?;

        Ok(batches)
    }

    /// Explain a JSON-LD query (query optimization plan).
    pub async fn explain(&self, ledger: &LedgerState, query_json: &JsonValue) -> Result<JsonValue> {
        crate::explain::explain_jsonld(&ledger.db, query_json).await
    }

    /// Explain a SPARQL query (query optimization plan).
    pub async fn explain_sparql(&self, ledger: &LedgerState, sparql: &str) -> Result<JsonValue> {
        crate::explain::explain_sparql(&ledger.db, sparql).await
    }

    /// Execute a JSON-LD query and return formatted JSON-LD output.
    pub async fn query_jsonld(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> Result<JsonValue> {
        let result = self.query(ledger, query_json).await?;
        Ok(result.to_jsonld_async(&ledger.db).await?)
    }

    /// Clojure-parity alias: tracked query entrypoint for a loaded ledger.
    pub async fn query_tracked(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_jsonld_tracked(ledger, query_json).await
    }

    /// Execute a JSON-LD query and return a Clojure-parity tracked response.
    pub async fn query_jsonld_tracked(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let tracker = tracker_from_query_json(query_json);

        let (vars, parsed) =
            parse_jsonld_query(query_json, &ledger.db, ledger.default_context.as_ref()).map_err(
                |e| crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally()),
            )?;

        let executable = prepare_for_execution(&parsed);
        let r2rml_provider = NoOpR2rmlProvider::new();

        let source = DataSource {
            db: &ledger.db,
            overlay: ledger.novelty.as_ref(),
            to_t: ledger.t(),
            from_t: None,
        };
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
            crate::query::TrackedErrorResponse::new(
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
            None,
        );

        let result_json = query_result
            .to_jsonld_async_tracked(&ledger.db, &tracker)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
            })?;

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    /// Execute a JSON-LD query and format results using a custom formatter config (async).
    pub async fn query_format(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
        config: &FormatterConfig,
    ) -> Result<JsonValue> {
        let result = self.query(ledger, query_json).await?;
        Ok(result.format_async(&ledger.db, config).await?)
    }

    /// Execute a JSON-LD query with policy enforcement
    pub async fn query_with_policy(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let (vars, parsed) =
            parse_jsonld_query(query_json, &ledger.db, ledger.default_context.as_ref())?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let source = DataSource {
            db: &ledger.db,
            overlay: ledger.novelty.as_ref(),
            to_t: ledger.t(),
            from_t: None,
        };
        let batches =
            fluree_db_query::execute_with_policy(source, &vars, &executable, policy).await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
            None,
        ))
    }

    /// Execute a SPARQL query with policy enforcement
    pub async fn query_sparql_with_policy(
        &self,
        ledger: &LedgerState,
        sparql: &str,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db, ledger.default_context.as_ref())?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let source = DataSource {
            db: &ledger.db,
            overlay: ledger.novelty.as_ref(),
            to_t: ledger.t(),
            from_t: None,
        };
        let batches =
            fluree_db_query::execute_with_policy(source, &vars, &executable, policy).await?;

        Ok(build_sparql_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
            None,
        ))
    }

    /// Execute a pre-built triple pattern query (advanced/testing API)
    pub async fn query_pattern(
        &self,
        ledger: &LedgerState,
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
    pub async fn query_sparql(&self, ledger: &LedgerState, sparql: &str) -> Result<QueryResult> {
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db, ledger.default_context.as_ref())?;
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
            None,
        ))
    }

    /// Execute a SPARQL query with tracking (fuel counting, time, policy stats).
    pub async fn query_sparql_tracked(
        &self,
        ledger: &LedgerState,
        sparql: &str,
        options: TrackingOptions,
    ) -> Result<(QueryResult, Option<TrackingTally>)> {
        let tracker = Tracker::new(options);
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db, ledger.default_context.as_ref())?;
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
                None,
            ),
            tally,
        ))
    }

    /// Execute a JSON-LD query against a historical ledger view
    pub async fn query_historical(
        &self,
        view: &HistoricalLedgerView,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_jsonld_query(query_json, &view.db, None)?;
        let executable = ExecutableQuery::simple(parsed.clone());
        let r2rml_provider = NoOpR2rmlProvider::new();
        let tracker = Tracker::disabled();

        let batches = if let Some(novelty) = view.overlay() {
            let source = DataSource {
                db: &view.db,
                overlay: novelty.as_ref(),
                to_t: view.to_t(),
                from_t: None,
            };
            crate::execute_with_r2rml(
                source,
                &vars,
                &executable,
                &tracker,
                &r2rml_provider,
                &r2rml_provider,
            )
            .await?
        } else {
            let source = DataSource {
                db: &view.db,
                overlay: &fluree_db_core::NoOverlay,
                to_t: view.to_t(),
                from_t: None,
            };
            crate::execute_with_r2rml(
                source,
                &vars,
                &executable,
                &tracker,
                &r2rml_provider,
                &r2rml_provider,
            )
            .await?
        };

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            view.to_t(),
            view.overlay()
                .map(|n| Arc::clone(n) as Arc<dyn OverlayProvider>),
            None,
        ))
    }
}
