use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::query::helpers::{
    extract_sparql_dataset_spec, parse_and_validate_sparql, parse_dataset_spec,
};
use crate::view::DataSetDb;
use crate::{ApiError, Fluree, PolicyContext, QueryResult, Result, Storage};

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: crate::NameService + Clone + Send + Sync + 'static,
{
    /// Execute a JSON-LD query via connection.
    ///
    /// This is the unified entry point for connection queries. It:
    /// 1. Parses dataset spec and options from query JSON
    /// 2. For single-ledger: builds a `GraphDb` and uses view API
    /// 3. For multi-ledger: builds a `DataSetDb` for proper merge
    /// 4. Applies policy wrappers if policy options are present
    pub async fn query_connection(&self, query_json: &JsonValue) -> Result<QueryResult> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        // Single-ledger fast path: use GraphDb API
        if Self::is_single_ledger_fast_path(&spec) {
            let source = &spec.default_graphs[0];
            let alias = source.identifier.as_str();
            let mut view = self.db(alias).await?;

            // Apply graph selector if specified in structured from
            if let Some(selector) = &source.graph_selector {
                view = Self::apply_graph_selector(view, selector)?;
            }

            // Apply policy: per-source overrides global
            let view = self
                .apply_source_or_global_policy(view, source, &qc_opts)
                .await?;

            // Apply config-graph reasoning defaults
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);

            return self.query(&view, query_json).await;
        }

        // Single-ledger with time travel: use GraphDb API
        if let Some(mut view) = self.try_single_view_from_spec(&spec).await? {
            let source = &spec.default_graphs[0];

            // Apply graph selector if specified in structured from
            if let Some(selector) = &source.graph_selector {
                view = Self::apply_graph_selector(view, selector)?;
            }

            // Apply policy: per-source overrides global
            let view = self
                .apply_source_or_global_policy(view, source, &qc_opts)
                .await?;

            // Apply config-graph reasoning defaults
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);

            return self.query(&view, query_json).await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(&spec, &qc_opts).await?
        } else {
            self.build_dataset_view(&spec).await?
        };

        self.query_dataset(&dataset, query_json).await
    }

    /// Execute a connection query and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked(
        &self,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
            ));
        }

        // Single-ledger fast path (no time override): use GraphDb API
        if Self::is_single_ledger_fast_path(&spec) {
            let source = &spec.default_graphs[0];
            let alias = source.identifier.as_str();
            let mut view = self
                .db(alias)
                .await
                .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

            // Apply graph selector if specified in structured from
            if let Some(selector) = &source.graph_selector {
                view = Self::apply_graph_selector(view, selector).map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), None)
                })?;
            }

            // Apply policy: per-source overrides global
            let view = self
                .apply_source_or_global_policy(view, source, &qc_opts)
                .await
                .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

            // Apply config-graph reasoning defaults
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);

            return self.query_tracked(&view, query_json, None).await;
        }

        // Single-ledger with time travel: use GraphDb API
        let single_view = self
            .try_single_view_from_spec(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        if let Some(mut view) = single_view {
            let source = &spec.default_graphs[0];

            // Apply graph selector if specified in structured from
            if let Some(selector) = &source.graph_selector {
                view = Self::apply_graph_selector(view, selector).map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), None)
                })?;
            }

            // Apply policy: per-source overrides global
            let view = self
                .apply_source_or_global_policy(view, source, &qc_opts)
                .await
                .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

            // Apply config-graph reasoning defaults
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);

            return self.query_tracked(&view, query_json, None).await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(&spec, &qc_opts)
                .await
                .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?
        } else {
            self.build_dataset_view(&spec)
                .await
                .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?
        };

        self.query_dataset_tracked(&dataset, query_json, None).await
    }

    /// Clojure-parity alias: tracked connection query entrypoint.
    pub async fn query_connection_tracked(
        &self,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_connection_jsonld_tracked(query_json).await
    }

    /// Execute a JSON-LD query via connection with explicit policy context.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_with_policy(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        // Try single-ledger path (including with time spec)
        if let Some(view) = self.try_single_view_from_spec(&spec).await? {
            let view = view.with_policy(Arc::new(policy.clone()));
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);
            return self.query(&view, query_json).await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset(&dataset, query_json).await
    }

    /// Execute a connection query with explicit policy context and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked_with_policy(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
            ));
        }

        // Try single-ledger path (including with time spec)
        let single_view = self
            .try_single_view_from_spec(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        if let Some(view) = single_view {
            let view = view.with_policy(Arc::new(policy.clone()));
            let view = self.apply_config_reasoning(view, None);
            let view = self.apply_config_datalog(view, None);
            return self.query_tracked(&view, query_json, None).await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_tracked(&dataset, query_json, None).await
    }

    /// Execute a SPARQL query via connection (dataset specified via SPARQL `FROM` / `FROM NAMED`).
    ///
    /// Note: SPARQL connection queries allow dataset clauses because the dataset
    /// is being specified at the connection level.
    pub async fn query_connection_sparql(&self, sparql: &str) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        self.query_dataset(&dataset, sparql).await
    }

    /// Execute a SPARQL query via connection with explicit policy context.
    pub(crate) async fn query_connection_sparql_with_policy(
        &self,
        sparql: &str,
        policy: &PolicyContext,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset(&dataset, sparql).await
    }

    /// Execute a SPARQL query via connection with tracking (dataset specified via SPARQL `FROM` / `FROM NAMED`).
    ///
    /// Note: Unlike JSON-LD connection queries, SPARQL always uses the dataset path because
    /// SPARQL FROM clauses specify the dataset and are incompatible with the single-view path
    /// (which validates against FROM clauses).
    pub async fn query_connection_sparql_tracked(
        &self,
        sparql: &str,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        self.query_dataset_tracked(&dataset, sparql, None).await
    }

    /// Execute a SPARQL query via connection with explicit policy context and tracking.
    ///
    /// Note: Unlike JSON-LD connection queries, SPARQL always uses the dataset path because
    /// SPARQL FROM clauses specify the dataset and are incompatible with the single-view path
    /// (which validates against FROM clauses).
    pub(crate) async fn query_connection_sparql_tracked_with_policy(
        &self,
        sparql: &str,
        policy: &PolicyContext,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);

        self.query_dataset_tracked(&dataset, sparql, None).await
    }

    /// Apply per-source or global policy to a view.
    ///
    /// Per-source policy takes precedence if present, otherwise global policy is used.
    /// If neither has policy, returns the view unchanged.
    async fn apply_source_or_global_policy(
        &self,
        view: crate::view::GraphDb,
        source: &crate::dataset::GraphSource,
        global_opts: &crate::QueryConnectionOptions,
    ) -> Result<crate::view::GraphDb> {
        // Per-source policy takes precedence
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts, None).await;
            }
        }
        // Fall back to global policy if present
        if global_opts.has_any_policy_inputs() {
            self.wrap_policy(view, global_opts, None).await
        } else {
            Ok(view)
        }
    }
}

fn apply_policy_to_dataset(mut dataset: DataSetDb, policy: &PolicyContext) -> DataSetDb {
    let policy = Arc::new(policy.clone());

    dataset.default = dataset
        .default
        .into_iter()
        .map(|v| v.with_policy(Arc::clone(&policy)))
        .collect();

    dataset.named = dataset
        .named
        .into_iter()
        .map(|(k, v)| (k, v.with_policy(Arc::clone(&policy))))
        .collect();

    dataset
}
