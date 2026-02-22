mod bm25;
pub mod builder;
mod connection;
#[cfg(feature = "credential")]
mod credential;
mod graph_source;
pub(crate) mod helpers;
mod ledger;
pub mod nameservice_builder;

use serde_json::Value as JsonValue;

use crate::{
    format, Batch, FormatterConfig, FuelExceededError, OverlayProvider, PolicyContext, PolicyStats,
    SelectMode, Tracker, TrackingTally, VarRegistry,
};

use fluree_db_core::{GraphDbRef, LedgerSnapshot};
use fluree_db_indexer::run_index::BinaryGraphView;

use fluree_db_query::parse::{ConstructTemplate, GraphSelectSpec};

/// Result of a query execution
pub struct QueryResult {
    /// Variable registry mapping names to IDs
    pub vars: VarRegistry,
    /// Effective "as-of" time boundary for this result (used for formatting with overlays).
    pub t: i64,
    /// Novelty overlay used during execution (for graph crawl formatting).
    ///
    /// Most query execution runs against `LedgerSnapshot + Novelty` (range_with_overlay). Graph crawl formatting
    /// must use the same overlay to see unindexed flakes in memory-backed tests.
    pub novelty: Option<std::sync::Arc<dyn OverlayProvider>>,
    /// Parsed JSON-LD context from the query (for IRI compaction in formatters)
    pub context: crate::ParsedContext,
    /// Original JSON context from the query (for CONSTRUCT output)
    pub orig_context: Option<JsonValue>,
    /// Selected variable IDs from the query
    pub select: Vec<fluree_db_query::VarId>,
    /// Select mode (select vs selectOne vs wildcard vs construct)
    pub select_mode: SelectMode,
    /// Result batches
    pub batches: Vec<Batch>,
    /// Graph-scoped binary index view for late materialization of encoded bindings.
    ///
    /// When present, formatters decode `Binding::EncodedLit` values through
    /// `BinaryGraphView::decode_value` — routing per-graph specialty kinds (NUM_BIG,
    /// VECTOR_ID) through the correct arenas.  When absent, all bindings must
    /// already be fully materialized.
    pub binary_graph: Option<BinaryGraphView>,
    /// CONSTRUCT template (None for SELECT queries)
    pub construct_template: Option<ConstructTemplate>,
    /// Graph crawl select specification (None for flat SELECT or CONSTRUCT)
    ///
    /// When present, controls nested JSON-LD object expansion during formatting.
    pub graph_select: Option<GraphSelectSpec>,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("t", &self.t)
            .field("select_mode", &self.select_mode)
            .field("select_len", &self.select.len())
            .field("batches_len", &self.batches.len())
            .field("has_binary_graph", &self.binary_graph.is_some())
            .field("has_novelty", &self.novelty.is_some())
            .field("has_graph_select", &self.graph_select.is_some())
            .finish()
    }
}

/// Query response with optional tracking (Clojure parity: top-level siblings).
#[derive(Debug, serde::Serialize)]
pub struct TrackedQueryResponse {
    pub status: u16,
    pub result: JsonValue,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<std::collections::HashMap<String, PolicyStats>>,
}

impl TrackedQueryResponse {
    /// Create a successful response with optional tracking tally
    pub fn success(result: JsonValue, tally: Option<TrackingTally>) -> Self {
        match tally {
            Some(TrackingTally { time, fuel, policy }) => Self {
                status: 200,
                result,
                time,
                fuel,
                policy,
            },
            None => Self {
                status: 200,
                result,
                time: None,
                fuel: None,
                policy: None,
            },
        }
    }
}

/// Error response with optional tracking (Clojure parity: top-level siblings).
#[derive(Debug, serde::Serialize)]
pub struct TrackedErrorResponse {
    pub status: u16,
    pub error: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<std::collections::HashMap<String, PolicyStats>>,
}

impl TrackedErrorResponse {
    /// Create an error response with optional tracking tally
    pub fn new(status: u16, error: impl Into<String>, tally: Option<TrackingTally>) -> Self {
        match tally {
            Some(TrackingTally { time, fuel, policy }) => Self {
                status,
                error: error.into(),
                time,
                fuel,
                policy,
            },
            None => Self {
                status,
                error: error.into(),
                time: None,
                fuel: None,
                policy: None,
            },
        }
    }

    /// Create a fuel exceeded error response
    pub fn fuel_exceeded(err: &FuelExceededError, tally: Option<TrackingTally>) -> Self {
        Self::new(400, err.to_string(), tally)
    }
}

impl QueryResult {
    /// Get total row count across all batches
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.len()).sum()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.row_count() == 0
    }

    /// Format as JSON-LD Query JSON (default - array rows, Clojure parity)
    ///
    /// Returns simple JSON values with compact IRIs using the @context prefixes.
    /// Rows are arrays aligned to the select order.
    pub fn to_jsonld(&self, snapshot: &LedgerSnapshot) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld().with_select_mode(self.select_mode);
        format::format_results(self, &self.context, snapshot, &config)
    }

    /// Format as JSON-LD Query JSON with object rows (API-friendly)
    ///
    /// Rows are maps keyed by variable name (e.g., `{"?s": "ex:alice", ...}`).
    pub fn to_jsonld_objects(&self, snapshot: &LedgerSnapshot) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld_objects().with_select_mode(self.select_mode);
        format::format_results(self, &self.context, snapshot, &config)
    }

    /// Format as SPARQL 1.1 Query Results JSON
    ///
    /// Returns W3C standard format with `{"head": {"vars": [...]}, "results": {"bindings": [...]}}`.
    pub fn to_sparql_json(&self, snapshot: &LedgerSnapshot) -> format::Result<JsonValue> {
        let config = FormatterConfig::sparql_json().with_select_mode(self.select_mode);
        format::format_results(self, &self.context, snapshot, &config)
    }

    /// Format as TypedJson (always include explicit datatype)
    ///
    /// Every value includes `@type` annotation, even for inferable types.
    pub fn to_typed_json(&self, snapshot: &LedgerSnapshot) -> format::Result<JsonValue> {
        let config = FormatterConfig::typed_json().with_select_mode(self.select_mode);
        format::format_results(self, &self.context, snapshot, &config)
    }

    /// Format CONSTRUCT query results as JSON-LD graph
    ///
    /// Returns `{"@context": {...}, "@graph": [...]}` format.
    /// Only valid for CONSTRUCT queries (select_mode == Construct).
    ///
    /// # Errors
    ///
    /// Returns error if this is not a CONSTRUCT query result.
    pub fn to_construct(&self, snapshot: &LedgerSnapshot) -> format::Result<JsonValue> {
        if self.select_mode != SelectMode::Construct {
            return Err(format::FormatError::InvalidBinding(
                "to_construct() only valid for CONSTRUCT queries".to_string(),
            ));
        }
        let config = FormatterConfig::jsonld().with_select_mode(SelectMode::Construct);
        format::format_results(self, &self.context, snapshot, &config)
    }

    /// Format with custom configuration
    pub fn format(
        &self,
        snapshot: &LedgerSnapshot,
        config: &FormatterConfig,
    ) -> format::Result<JsonValue> {
        format::format_results(self, &self.context, snapshot, config)
    }

    // ========================================================================
    // TSV formatting (high-performance path)
    // ========================================================================

    /// Format as tab-separated values (high-performance path).
    ///
    /// Bypasses JSON DOM construction entirely. IRIs are compacted via `@context`.
    pub fn to_tsv(&self, snapshot: &LedgerSnapshot) -> format::Result<String> {
        format::delimited::format_tsv(self, snapshot)
    }

    /// Format as TSV bytes (high-performance path for server use).
    ///
    /// Returns raw `Vec<u8>` suitable for direct HTTP response body.
    pub fn to_tsv_bytes(&self, snapshot: &LedgerSnapshot) -> format::Result<Vec<u8>> {
        format::delimited::format_tsv_bytes(self, snapshot)
    }

    /// Format as TSV string with a row limit (for benchmark/preview).
    ///
    /// Returns `(tsv_string, total_row_count)`.
    pub fn to_tsv_limited(
        &self,
        snapshot: &LedgerSnapshot,
        limit: usize,
    ) -> format::Result<(String, usize)> {
        format::delimited::format_tsv_limited(self, snapshot, limit)
    }

    /// Format as TSV bytes with a row limit (for server benchmark/preview).
    ///
    /// Returns `(tsv_bytes, total_row_count)`.
    pub fn to_tsv_bytes_limited(
        &self,
        snapshot: &LedgerSnapshot,
        limit: usize,
    ) -> format::Result<(Vec<u8>, usize)> {
        format::delimited::format_tsv_bytes_limited(self, snapshot, limit)
    }

    // ========================================================================
    // CSV formatting (high-performance path)
    // ========================================================================

    /// Format as comma-separated values (high-performance path).
    ///
    /// Bypasses JSON DOM construction entirely. IRIs are compacted via `@context`.
    /// Uses RFC 4180 quoting for values containing commas, quotes, or newlines.
    pub fn to_csv(&self, snapshot: &LedgerSnapshot) -> format::Result<String> {
        format::delimited::format_csv(self, snapshot)
    }

    /// Format as CSV bytes (high-performance path for server use).
    ///
    /// Returns raw `Vec<u8>` suitable for direct HTTP response body.
    pub fn to_csv_bytes(&self, snapshot: &LedgerSnapshot) -> format::Result<Vec<u8>> {
        format::delimited::format_csv_bytes(self, snapshot)
    }

    /// Format as CSV string with a row limit (for benchmark/preview).
    ///
    /// Returns `(csv_string, total_row_count)`.
    pub fn to_csv_limited(
        &self,
        snapshot: &LedgerSnapshot,
        limit: usize,
    ) -> format::Result<(String, usize)> {
        format::delimited::format_csv_limited(self, snapshot, limit)
    }

    /// Format as CSV bytes with a row limit (for server benchmark/preview).
    ///
    /// Returns `(csv_bytes, total_row_count)`.
    pub fn to_csv_bytes_limited(
        &self,
        snapshot: &LedgerSnapshot,
        limit: usize,
    ) -> format::Result<(Vec<u8>, usize)> {
        format::delimited::format_csv_bytes_limited(self, snapshot, limit)
    }

    // ========================================================================
    // Async formatting methods (required for graph crawl queries)
    // ========================================================================

    /// Format as JSON-LD Query JSON with async DB access
    ///
    /// This is the async version of `to_jsonld()`. Required for graph crawl
    /// queries which need to fetch additional data from the database during
    /// formatting. For regular SELECT queries, the sync version works fine.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.query(&ledger, &json!({
    ///     "select": {"?person": ["*", {"ex:friend": ["*"]}]},
    ///     "where": {"@id": "?person", "@type": "ex:User"}
    /// })).await?;
    ///
    /// // Graph crawl requires async formatting
    /// let json = result.to_jsonld_async(ledger.as_graph_db_ref(0)).await?;
    /// ```
    pub async fn to_jsonld_async(&self, db: GraphDbRef<'_>) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld().with_select_mode(self.select_mode);
        format::format_results_async(self, &self.context, db, &config, None, None).await
    }

    /// Format as JSON-LD Query JSON with object rows (async version)
    ///
    /// Async version of `to_jsonld_objects()`. Required for graph crawl queries.
    pub async fn to_jsonld_objects_async(&self, db: GraphDbRef<'_>) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld_objects().with_select_mode(self.select_mode);
        format::format_results_async(self, &self.context, db, &config, None, None).await
    }

    /// Format with custom configuration (async version)
    ///
    /// Async version of `format()`. Required for graph crawl queries.
    pub async fn format_async(
        &self,
        db: GraphDbRef<'_>,
        config: &FormatterConfig,
    ) -> format::Result<JsonValue> {
        format::format_results_async(self, &self.context, db, config, None, None).await
    }

    // ========================================================================
    // Policy-aware async formatting methods
    // ========================================================================

    /// Format as JSON-LD Query JSON with policy filtering (async version)
    ///
    /// When `policy` is provided, graph crawl queries filter flakes according to
    /// view policies during formatting. This ensures that nested objects fetched
    /// during graph crawl also respect policy restrictions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.query_with_policy(&ledger, &query, &policy_ctx).await?;
    /// // Graph crawl formatting also applies policy
    /// let json = result.to_jsonld_async_with_policy(ledger.as_graph_db_ref(0), &policy_ctx).await?;
    /// ```
    pub async fn to_jsonld_async_with_policy(
        &self,
        db: GraphDbRef<'_>,
        policy: &PolicyContext,
    ) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld().with_select_mode(self.select_mode);
        format::format_results_async(self, &self.context, db, &config, Some(policy), None).await
    }

    /// Format with custom configuration and policy filtering (async version)
    ///
    /// Combines custom formatting options with policy-aware graph crawl.
    pub async fn format_async_with_policy(
        &self,
        db: GraphDbRef<'_>,
        config: &FormatterConfig,
        policy: &PolicyContext,
    ) -> format::Result<JsonValue> {
        format::format_results_async(self, &self.context, db, config, Some(policy), None).await
    }

    /// Tracked async JSON-LD formatting (graph crawl counts fuel/policy).
    pub async fn to_jsonld_async_tracked(
        &self,
        db: GraphDbRef<'_>,
        tracker: &Tracker,
    ) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld().with_select_mode(self.select_mode);
        format::format_results_async(self, &self.context, db, &config, None, Some(tracker)).await
    }

    /// Tracked async JSON-LD formatting with policy (graph crawl counts fuel/policy).
    pub async fn to_jsonld_async_with_policy_tracked(
        &self,
        db: GraphDbRef<'_>,
        policy: &PolicyContext,
        tracker: &Tracker,
    ) -> format::Result<JsonValue> {
        let config = FormatterConfig::jsonld().with_select_mode(self.select_mode);
        format::format_results_async(
            self,
            &self.context,
            db,
            &config,
            Some(policy),
            Some(tracker),
        )
        .await
    }
}
