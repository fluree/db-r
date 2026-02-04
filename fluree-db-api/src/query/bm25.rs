use serde_json::Value as JsonValue;

use crate::query::helpers::{parse_dataset_spec, tracker_for_limits};
use crate::{
    ApiError, ExecutableQuery, Fluree, FlureeDataSetView, FlureeIndexProvider, QueryResult, Result,
    SimpleCache, Storage, StorageWrite, VarRegistry,
};

use fluree_db_query::parse::parse_query;

impl<S, N> Fluree<S, SimpleCache, N>
where
    S: Storage + StorageWrite + Clone + Send + Sync + 'static,
    N: crate::NameService
        + crate::Publisher
        + crate::VirtualGraphPublisher
        + Clone
        + Send
        + Sync
        + 'static,
{
    /// Execute a query against a loaded dataset with BM25 and vector index provider support.
    ///
    /// This enables both `idx:search` (BM25) and `idx:vector` (similarity search) patterns
    /// in queries against virtual graphs.
    pub async fn query_dataset_with_bm25(
        &self,
        dataset: &FlureeDataSetView<S, SimpleCache>,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        // Get the primary graph for parsing/encoding
        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no graphs for query execution"))?;
        let primary_t = primary.to_t;

        // Parse the query using the primary ledger's DB for IRI encoding
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, primary.db.as_ref(), &mut vars)?;

        // Build the runtime dataset
        let runtime_dataset = dataset.as_runtime_dataset();

        // Build executable query
        let executable = ExecutableQuery::simple(parsed.clone());

        // Create index provider for virtual graph support (implements both BM25 and Vector)
        let provider = FlureeIndexProvider::new(self);

        // Execute with dataset and BM25 provider.
        //
        // Vector provider support is feature-gated. When disabled,
        // idx:vector patterns are not available and we run the BM25-only path.
        let tracker = tracker_for_limits(query_json);
        let batches = {
            #[cfg(feature = "vector")]
            {
                crate::execute_with_dataset_and_providers(
                    primary.db.as_ref(),
                    primary.overlay.as_ref(),
                    &vars,
                    &executable,
                    primary_t,
                    None,
                    &runtime_dataset,
                    &provider,
                    &provider,
                    if tracker.is_enabled() {
                        Some(&tracker)
                    } else {
                        None
                    },
                )
                .await?
            }
            #[cfg(not(feature = "vector"))]
            {
                crate::execute_with_dataset_and_bm25(
                    primary.db.as_ref(),
                    primary.overlay.as_ref(),
                    &vars,
                    &executable,
                    primary_t,
                    None,
                    &runtime_dataset,
                    &provider,
                    if tracker.is_enabled() {
                        Some(&tracker)
                    } else {
                        None
                    },
                )
                .await?
            }
        };

        // Dataset graph crawl formatting may need to see flakes from multiple ledgers (union),
        // and each ledger may have a different `t`. We therefore:
        // - use a composite overlay (union of novelty overlays)
        // - use max_t across the dataset (safe upper bound for overlay filtering)
        let novelty = dataset.composite_overlay();
        let max_t = dataset.max_t();

        Ok(QueryResult {
            vars,
            t: max_t,
            novelty,
            context: parsed.context,
            orig_context: parsed.orig_context,
            batches,
            select: parsed.select,
            select_mode: parsed.select_mode,
            construct_template: parsed.construct_template,
            graph_select: parsed.graph_select,
        })
    }

    /// Execute a connection query with index provider support (BM25 + Vector).
    ///
    /// This method enables both `idx:search` (BM25 full-text search) and `idx:vector`
    /// (similarity search) patterns in queries. Despite the name (kept for backwards
    /// compatibility), it supports all virtual graph index types.
    ///
    /// For queries that don't use virtual graph patterns, prefer `query_connection()`
    /// as it may take faster code paths for simple single-ledger queries.
    pub async fn query_connection_with_bm25(&self, query_json: &JsonValue) -> Result<QueryResult> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        // NOTE: Unlike query_connection(), we do NOT take the single-ledger fast path here.
        // BM25 queries with idx:* patterns require the full dataset execution path with
        // FlureeIndexProvider wired into the execution context. Always use dataset path.

        let dataset = if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(&spec, &qc_opts).await?
        } else {
            self.build_dataset_view(&spec).await?
        };

        self.query_dataset_with_bm25(&dataset, query_json).await
    }
}
