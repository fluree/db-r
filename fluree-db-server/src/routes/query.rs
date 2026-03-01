//! Query endpoints: /fluree/query, /fluree/explain, /:ledger/query
//!
//! Supports both JSON-LD and SPARQL query content types:
//! - `application/json`: JSON-LD query format (JSON body with "from" field)
//! - `application/sparql-query`: SPARQL query syntax (raw SPARQL string in body)
//!
//! For SPARQL UPDATE operations, use the transact endpoints instead.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{tracking_headers, FlureeHeaders, MaybeCredential, MaybeDataBearer};
// Note: NeedsRefresh is no longer used - replaced by FreshnessSource trait
use crate::state::AppState;
use crate::state::FlureeInstance;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, log_query_text, set_span_error_code,
    should_log_query_text,
};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::dataset::GraphSelector;
use fluree_db_api::{
    DatasetSpec, FreshnessCheck, FreshnessSource, GraphDb, GraphSource, LedgerState, TimeSpec,
    TrackingTally,
};
use serde_json::Value as JsonValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument;

// ============================================================================
// Data API Auth Helpers
// ============================================================================

/// Resolve the effective request identity for policy enforcement.
///
/// Precedence:
/// 1) Signed request DID (credential)
/// 2) Bearer token identity (fluree.identity ?? sub)
fn effective_identity(credential: &MaybeCredential, bearer: &MaybeDataBearer) -> Option<String> {
    credential
        .did()
        .map(|d| d.to_string())
        .or_else(|| bearer.0.as_ref().and_then(|p| p.identity.clone()))
}

/// Force auth identity/policy-class into JSON-LD query opts, overriding client-provided values.
fn force_query_auth_opts(
    query: &mut JsonValue,
    identity: Option<&str>,
    policy_class: Option<&str>,
) {
    let Some(obj) = query.as_object_mut() else {
        return;
    };
    let opts = obj
        .entry("opts")
        .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
    let Some(opts_obj) = opts.as_object_mut() else {
        return;
    };

    if let Some(id) = identity {
        opts_obj.insert("identity".to_string(), JsonValue::String(id.to_string()));
    }
    if let Some(pc) = policy_class {
        opts_obj.insert(
            "policy-class".to_string(),
            JsonValue::String(pc.to_string()),
        );
    }
}

/// Check if tracking is requested in query opts
fn has_tracking_opts(query_json: &JsonValue) -> bool {
    let Some(opts) = query_json.get("opts") else {
        return false;
    };

    // Check for meta (tracking) options
    // - meta: true enables all tracking
    // - meta: {time: true, ...} enables selective tracking
    // - meta: false or meta: {} should NOT enable tracking
    if let Some(meta) = opts.get("meta") {
        match meta {
            JsonValue::Bool(true) => return true,
            JsonValue::Object(obj) if !obj.is_empty() => return true,
            _ => {} // meta: false or meta: {} - don't enable tracking
        }
    }

    // max-fuel implicitly enables fuel tracking
    if opts.get("max-fuel").is_some()
        || opts.get("max_fuel").is_some()
        || opts.get("maxFuel").is_some()
    {
        return true;
    }

    false
}

/// Helper to extract ledger ID from request (for JSON-LD queries)
fn get_ledger_id(
    path_ledger: Option<&str>,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > header > body.from
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
    }

    if let Some(ledger) = &headers.ledger {
        return Ok(ledger.clone());
    }

    if let Some(from) = body.get("from").and_then(|v| v.as_str()) {
        return Ok(from.to_string());
    }

    Err(ServerError::MissingLedger)
}

/// Inject header values into query JSON (modifies the query in place)
fn inject_headers_into_query(query: &mut JsonValue, headers: &FlureeHeaders) {
    if let Some(obj) = query.as_object_mut() {
        // Get or create opts object
        let opts = obj
            .entry("opts")
            .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));

        if let Some(opts_obj) = opts.as_object_mut() {
            headers.inject_into_opts(opts_obj);
        }
    }
}

/// Execute a query
///
/// POST /fluree/query
/// GET /fluree/query
///
/// Supports:
/// - JSON-LD queries (JSON body with "from" field for ledger)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format with Content-Type: application/jwt)
///   - Connection-scoped: requires FROM clause in SPARQL to specify ledger
pub async fn query(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    // Detect input format before span creation so otel.name is set at open time
    let input_format = if headers.is_sparql_query() || credential.is_sparql {
        "sparql"
    } else {
        "fql"
    };

    let span = create_request_span(
        "query",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger ID determined later
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
    let span = tracing::Span::current();

    tracing::info!(status = "start", "query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
        set_span_error_code(&span, "error:Unauthorized");
        return Err(ServerError::unauthorized(
            "Authentication required (signed request or Bearer token)",
        ));
    }
    // SPARQL UPDATE should use the transact endpoint, not query
    if headers.is_sparql_update() || credential.is_sparql_update {
        let error = ServerError::bad_request(
            "SPARQL UPDATE requests should use the /v1/fluree/transact endpoint, not /v1/fluree/query",
        );
        set_span_error_code(&span, "error:BadRequest");
        tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
        return Err(error);
    }

    let delimited = wants_delimited(&headers);

    // Handle SPARQL query
    if headers.is_sparql_query() || credential.is_sparql {
        // Connection-scoped SPARQL returns pre-formatted JSON — delimited not supported
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for connection-scoped SPARQL queries. \
                     Use the /:ledger/query endpoint instead.",
                fmt.name().to_uppercase()
            )));
        }

        let sparql = credential.body_string()?;

        // Log query text according to configuration
        log_query_text(&sparql, &state.telemetry_config, &span);

        // Connection-scoped SPARQL requires a FROM/FROM NAMED clause to specify the ledger.
        //
        // NOTE: We intentionally do NOT fall back to the fluree-ledger header here.
        // Ledger-scoped SPARQL without FROM is supported via the /:ledger/query route.

        // Enforce bearer ledger scope for unsigned SPARQL requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() {
                // Extract ledger IDs from FROM/FROM NAMED clauses.
                // Parse failure → fall through (let the engine produce a proper error).
                if let Ok(ledger_ids) = fluree_db_api::sparql_dataset_ledger_ids(&sparql) {
                    for ledger_id in &ledger_ids {
                        if !p.can_read(ledger_id) {
                            return Err(ServerError::not_found("Ledger not found"));
                        }
                    }
                }
            }
        }

        match state.fluree.query_connection_sparql_jsonld(&sparql).await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "sparql",
                    result_count = result.as_array().map(|a| a.len()).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)).into_response())
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, query_kind = "sparql", "query failed");
                Err(server_error)
            }
        }
    } else {
        // Handle JSON-LD query (JSON body)
        let mut query_json: JsonValue = credential.body_json()?;

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        // Get ledger id
        let ledger_id = match get_ledger_id(None, &headers, &query_json) {
            Ok(ledger_id) => {
                span.record("ledger_id", ledger_id.as_str());
                ledger_id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger_id) {
                set_span_error_code(&span, "error:Forbidden");
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Force auth-derived identity and policy-class into opts (non-spoofable)
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        force_query_auth_opts(&mut query_json, identity.as_deref(), policy_class);

        execute_query(&state, &ledger_id, &query_json, delimited).await
    }
    }
    .instrument(span)
    .await
}

/// Execute a query with ledger in path
///
/// POST /:ledger/query
/// GET /:ledger/query
///
/// Supports:
/// - JSON-LD queries (JSON body)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format)
///   - Ledger-scoped: FROM clause is optional (ledger from path is used)
pub async fn query_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let input_format = if headers.is_sparql_query() || credential.is_sparql {
        "sparql"
    } else {
        "fql"
    };

    let span = create_request_span(
        "query",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
    let span = tracing::Span::current();

    tracing::info!(status = "start", "ledger query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
        set_span_error_code(&span, "error:Unauthorized");
        return Err(ServerError::unauthorized(
            "Authentication required (signed request or Bearer token)",
        ));
    }

    // SPARQL UPDATE should use the transact endpoint, not query
    if headers.is_sparql_update() || credential.is_sparql_update {
        let error = ServerError::bad_request(
            "SPARQL UPDATE requests should use the /v1/fluree/transact/<ledger...> endpoint, not /v1/fluree/query/<ledger...>",
        );
        set_span_error_code(&span, "error:BadRequest");
        tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
        return Err(error);
    }

    let delimited = wants_delimited(&headers);

    // Handle SPARQL query - ledger is known from path
    if headers.is_sparql_query() || credential.is_sparql {
        let sparql = credential.body_string()?;

        // Log query text according to configuration
        log_query_text(&sparql, &state.telemetry_config, &span);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let identity = effective_identity(&credential, &bearer);
        return execute_sparql_ledger(&state, &ledger, &sparql, identity.as_deref(), delimited)
            .await;
    }

    // Handle JSON-LD query (JSON body)
    let mut query_json: JsonValue = credential.body_json()?;

    // Log query text according to configuration (only serialize if needed)
    if should_log_query_text(&state.telemetry_config) {
        if let Ok(query_text) = serde_json::to_string(&query_json) {
            log_query_text(&query_text, &state.telemetry_config, &span);
        }
    }

    // Get ledger id (path takes precedence)
    let ledger_id = match get_ledger_id(Some(&ledger), &headers, &query_json) {
        Ok(ledger_id) => {
            span.record("ledger_id", ledger_id.as_str());
            ledger_id
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger ID mismatch");
            return Err(e);
        }
    };

    // Ledger-scoped endpoint: allow `from` as a named-graph selector, but reject
    // attempts to target a different ledger than the URL.
    normalize_ledger_scoped_from(&ledger_id, &mut query_json)?;

    // Inject header values into query opts
    inject_headers_into_query(&mut query_json, &headers);

    // Enforce bearer ledger scope for unsigned requests
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_read(&ledger) {
            set_span_error_code(&span, "error:Forbidden");
            return Err(ServerError::not_found("Ledger not found"));
        }
    }

    // Force auth-derived identity and policy-class into opts (non-spoofable)
    let identity = effective_identity(&credential, &bearer);
    let policy_class = data_auth.default_policy_class.as_deref();
    force_query_auth_opts(&mut query_json, identity.as_deref(), policy_class);

    execute_query(&state, &ledger_id, &query_json, delimited).await
    }
    .instrument(span)
    .await
}

/// Execute a query with ledger as greedy tail segment.
///
/// POST /fluree/query/<ledger...>
/// GET /fluree/query/<ledger...>
///
/// This avoids ambiguity when ledger names contain `/`.
pub async fn query_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<impl IntoResponse> {
    query_ledger(State(state), Path(ledger), headers, bearer, credential).await
}

/// Check if a query requires dataset features (multi-ledger, named graphs, etc.)
///
/// Dataset features that require the connection execution path:
/// - `from-named`: Named graphs in the dataset
/// - `from` as array: Multiple default graphs
/// - `from` as object with special fields: graph selector, alias, time-travel
fn requires_dataset_features(query: &JsonValue) -> bool {
    // Check for from-named
    if query.get("from-named").is_some() {
        return true;
    }

    // Check the structure of "from"
    if let Some(from) = query.get("from") {
        // Array of sources = multiple default graphs
        if from.is_array() {
            return true;
        }

        // String with time-travel or graph fragment requires dataset parsing
        // so the server can apply time travel and/or named graph selection.
        if let Some(s) = from.as_str() {
            if s.contains('@') || s.contains('#') {
                return true;
            }
        }

        // Object form with special keys (graph, alias, t, iso, sha, etc.)
        if let Some(obj) = from.as_object() {
            // Any key other than just @id indicates dataset features
            let has_special_keys = obj.keys().any(|k| !matches!(k.as_str(), "@id"));
            if has_special_keys {
                return true;
            }
        }
    }

    false
}

fn iri_to_string(iri: &fluree_db_sparql::ast::Iri) -> String {
    use fluree_db_sparql::ast::IriValue;
    match &iri.value {
        IriValue::Full(s) => s.to_string(),
        IriValue::Prefixed { prefix, local } => {
            if prefix.is_empty() {
                format!(":{}", local)
            } else {
                format!("{}:{}", prefix, local)
            }
        }
    }
}

fn split_graph_fragment(s: &str) -> (&str, Option<&str>) {
    match s.split_once('#') {
        Some((base, frag)) => (base, Some(frag)),
        None => (s, None),
    }
}

fn base_ledger_id(s: &str) -> Result<String> {
    let (no_frag, _frag) = split_graph_fragment(s);
    let (base, _time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in ledger ref: {e}")))?;
    Ok(base)
}

fn looks_like_graph_selector_only(s: &str) -> bool {
    // Ledger IDs typically look like `name:branch` and do NOT include `://`.
    // Graph IRIs commonly include `://` or `urn:` and should be treated as selectors.
    matches!(s, "default" | "txn-meta")
        || s.contains("://")
        || s.starts_with("urn:")
        || (!s.contains(':') && !s.contains('@') && !s.contains('#'))
}

fn normalize_ledger_scoped_from(ledger_id: &str, query: &mut JsonValue) -> Result<()> {
    let Some(obj) = query.as_object_mut() else {
        return Ok(());
    };
    let Some(from_val) = obj.get("from").cloned() else {
        return Ok(());
    };

    match from_val {
        JsonValue::String(s) => {
            // 1) If it's a pure graph selector (e.g. txn-meta / default / graph name),
            // treat it as "graph within this ledger".
            if looks_like_graph_selector_only(&s) {
                let mut src = serde_json::Map::new();
                src.insert("@id".to_string(), JsonValue::String(ledger_id.to_string()));
                src.insert("graph".to_string(), JsonValue::String(s));
                obj.insert("from".to_string(), JsonValue::Object(src));
                return Ok(());
            }

            // 2) If it encodes ledger + optional time/fragment, require base ledger match.
            let base = base_ledger_id(&s)?;
            let base_path = base_ledger_id(ledger_id)?;
            if base != base_path {
                return Err(ServerError::bad_request(format!(
                    "Ledger mismatch: endpoint ledger is '{ledger_id}' but query 'from' targets '{s}'"
                )));
            }
        }
        JsonValue::Object(m) => {
            // Object form must name this ledger in @id (time/graph selectors ok).
            if let Some(id) = m.get("@id").and_then(|v| v.as_str()) {
                let base = base_ledger_id(id)?;
                let base_path = base_ledger_id(ledger_id)?;
                if base != base_path {
                    return Err(ServerError::bad_request(format!(
                        "Ledger mismatch: endpoint ledger is '{ledger_id}' but query 'from.@id' targets '{id}'"
                    )));
                }
            }
        }
        JsonValue::Array(_) => {
            // Allow arrays only if caller explicitly provides ledger refs per-entry.
            // (Graph-only entries are ambiguous in this endpoint.)
            // Mismatch will be enforced by the connection parsing path if present.
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod ledger_scoped_from_tests {
    use super::{normalize_ledger_scoped_from, requires_dataset_features};
    use serde_json::json;

    #[test]
    fn normalize_from_txn_meta_string_rewrites_to_object() {
        let mut q = json!({"select": ["*"], "from": "txn-meta"});
        normalize_ledger_scoped_from("myledger:main", &mut q).unwrap();
        assert_eq!(
            q.get("from").unwrap(),
            &json!({"@id": "myledger:main", "graph": "txn-meta"})
        );
        assert!(requires_dataset_features(&q));
    }

    #[test]
    fn normalize_from_different_ledger_errors() {
        let mut q = json!({"select": ["*"], "from": "other:main"});
        let err = normalize_ledger_scoped_from("myledger:main", &mut q).unwrap_err();
        assert!(err.to_string().contains("Ledger mismatch"));
    }

    #[test]
    fn requires_dataset_features_for_time_travel_and_fragment() {
        let q1 = json!({"select": ["*"], "from": "myledger:main@t:1"});
        assert!(requires_dataset_features(&q1));
        let q2 = json!({"select": ["*"], "from": "myledger:main#txn-meta"});
        assert!(requires_dataset_features(&q2));
    }
}

/// Delimited format requested by the client (TSV or CSV).
#[derive(Debug, Clone, Copy)]
enum DelimitedFormat {
    Tsv,
    Csv,
}

impl DelimitedFormat {
    fn name(self) -> &'static str {
        match self {
            DelimitedFormat::Tsv => "tsv",
            DelimitedFormat::Csv => "csv",
        }
    }
}

/// Check if the client requested a delimited format (TSV or CSV).
fn wants_delimited(headers: &FlureeHeaders) -> Option<DelimitedFormat> {
    if headers.wants_tsv() {
        Some(DelimitedFormat::Tsv)
    } else if headers.wants_csv() {
        Some(DelimitedFormat::Csv)
    } else {
        None
    }
}

/// Build an HTTP response with delimited body and appropriate Content-Type.
fn delimited_response(bytes: Vec<u8>, format: DelimitedFormat) -> Response {
    let content_type = match format {
        DelimitedFormat::Tsv => "text/tab-separated-values; charset=utf-8",
        DelimitedFormat::Csv => "text/csv; charset=utf-8",
    };
    ([(axum::http::header::CONTENT_TYPE, content_type)], bytes).into_response()
}

async fn execute_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    delimited: Option<DelimitedFormat>,
) -> Result<Response> {
    // Create execution span
    let span = tracing::debug_span!(
        "query_execute",
        ledger_id = ledger_id,
        query_kind = "jsonld",
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );
    async move {
    let span = tracing::Span::current();

    // Check for history query: explicit "to" key indicates history mode
    // History queries must go through the dataset/connection path for correct index selection
    if query_json.get("to").is_some() {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for history queries",
                fmt.name().to_uppercase()
            )));
        }
        return execute_history_query(state, ledger_id, query_json, &span)
            .await
            .map(IntoResponse::into_response);
    }

    // Check for dataset features (from-named, from array, from object with graph/alias/time)
    // These require the connection execution path for proper dataset handling
    if requires_dataset_features(query_json) {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for dataset queries",
                fmt.name().to_uppercase()
            )));
        }
        return execute_dataset_query(state, ledger_id, query_json, &span)
            .await
            .map(IntoResponse::into_response);
    }

    // In proxy mode, use the unified FlureeInstance methods (no local freshness checking)
    if state.config.is_proxy_storage_mode() {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported in proxy mode",
                fmt.name().to_uppercase()
            )));
        }
        return execute_query_proxy(state, ledger_id, query_json, &span)
            .await
            .map(IntoResponse::into_response);
    }

    // Shared storage mode: use load_ledger_for_query with freshness checking
    let ledger = load_ledger_for_query(state, ledger_id, &span).await?;
    let graph = GraphDb::from_ledger_state(&ledger);
    let fluree = state.fluree.as_file();

    // Check if tracking is requested
    if has_tracking_opts(query_json) {
        if let Some(fmt) = delimited {
            return Err(ServerError::not_acceptable(format!(
                "{} format not supported for tracked queries",
                fmt.name().to_uppercase()
            )));
        }
        // Execute tracked query via builder
        let response = match graph
            .query(fluree.as_ref())
            .jsonld(query_json)
            .execute_tracked()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                // TrackedErrorResponse has status and error fields
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "tracked query failed");
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        // Extract tracking info for headers
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        // Serialize TrackedQueryResponse to JSON
        let json = match serde_json::to_value(&response) {
            Ok(json) => json,
            Err(e) => {
                let server_error =
                    ServerError::internal(format!("Failed to serialize response: {}", e));
                set_span_error_code(&span, "error:InternalError");
                tracing::error!(error = %server_error, "response serialization failed");
                return Err(server_error);
            }
        };

        tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
        return Ok((headers, Json(json)).into_response());
    }

    // Delimited fast path: execute raw query and format as TSV/CSV bytes
    if let Some(fmt) = delimited {
        let result = graph
            .query(fluree.as_ref())
            .jsonld(query_json)
            .execute()
            .await
            .map_err(|e| {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "query execution failed");
                server_error
            })?;

        let row_count = result.row_count();
        let bytes = match fmt {
            DelimitedFormat::Tsv => result.to_tsv_bytes(&graph.snapshot),
            DelimitedFormat::Csv => result.to_csv_bytes(&graph.snapshot),
        }
        .map_err(|e| {
            ServerError::internal(format!(
                "{} formatting error: {}",
                fmt.name().to_uppercase(),
                e
            ))
        })?;

        tracing::info!(status = "success", format = fmt.name(), row_count);
        return Ok(delimited_response(bytes, fmt));
    }

    // Execute query via builder - formatted JSON-LD output
    let result = match graph
        .query(fluree.as_ref())
        .jsonld(query_json)
        .execute_formatted()
        .await
    {
        Ok(result) => {
            tracing::info!(
                status = "success",
                tracked = false,
                result_count = result.as_array().map(|a| a.len()).unwrap_or(0)
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:InvalidQuery");
            tracing::error!(error = %server_error, "query execution failed");
            return Err(server_error);
        }
    };
    Ok((HeaderMap::new(), Json(result)).into_response())
    }
    .instrument(span)
    .await
}

/// Execute a JSON-LD query in proxy mode (uses FlureeInstance wrapper methods)
async fn execute_query_proxy(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Check if tracking is requested
    if has_tracking_opts(query_json) {
        // Execute tracked query via FlureeInstance wrapper
        let response = match state
            .fluree
            .query_ledger_tracked(ledger_id, query_json)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "tracked query failed (proxy)");
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        // Extract tracking info for headers
        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        // Serialize TrackedQueryResponse to JSON
        let json = match serde_json::to_value(&response) {
            Ok(json) => json,
            Err(e) => {
                let server_error =
                    ServerError::internal(format!("Failed to serialize response: {}", e));
                set_span_error_code(span, "error:InternalError");
                tracing::error!(error = %server_error, "response serialization failed");
                return Err(server_error);
            }
        };

        tracing::info!(status = "success", tracked = true, time = ?response.time, fuel = response.fuel);
        return Ok((headers, Json(json)));
    }

    // Execute query via FlureeInstance wrapper
    let result = match state
        .fluree
        .query_ledger_jsonld(ledger_id, query_json)
        .await
    {
        Ok(result) => {
            tracing::info!(
                status = "success",
                tracked = false,
                result_count = result.as_array().map(|a| a.len()).unwrap_or(0)
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(span, "error:InvalidQuery");
            tracing::error!(error = %server_error, "query execution failed (proxy)");
            return Err(server_error);
        }
    };
    Ok((HeaderMap::new(), Json(result)))
}

/// Execute a SPARQL query against a specific ledger and return result
async fn execute_sparql_ledger(
    state: &AppState,
    ledger_id: &str,
    sparql: &str,
    identity: Option<&str>,
    delimited: Option<DelimitedFormat>,
) -> Result<Response> {
    // Create span for peer mode loading
    let span = tracing::debug_span!("sparql_execute", ledger_id = ledger_id);
    async move {
        let span = tracing::Span::current();

        // If the SPARQL includes FROM/FROM NAMED, interpret them as dataset clauses
        // selecting named graphs *within this ledger* (multi-named-graph support).
        //
        // - `FROM <ledger>` selects the default graph (ledger is optional in this route).
        // - `FROM <txn-meta>` selects the txn-meta graph within this ledger.
        // - `FROM <graphIRI>` selects the named graph IRI within this ledger.
        // - `FROM NAMED <graphIRI>` makes that named graph available via GRAPH <graphIRI>.
        //
        // If a FROM IRI looks like another ledger ID, reject with a ledger mismatch error.
        let parsed = fluree_db_sparql::parse_sparql(sparql);
        let dataset_clause = parsed.ast.as_ref().and_then(|ast| match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
            fluree_db_sparql::ast::QueryBody::Update(_) => None,
        });

        let has_dataset_clause = dataset_clause
            .map(|d| !d.default_graphs.is_empty() || !d.named_graphs.is_empty() || d.to_graph.is_some())
            .unwrap_or(false);

        // In proxy mode, use the unified FlureeInstance method (returns pre-formatted JSON)
        if state.config.is_proxy_storage_mode() && !has_dataset_clause {
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported in proxy mode",
                    fmt.name().to_uppercase()
                )));
            }
            let result = match identity {
                Some(id) => state
                    .fluree
                    .query_ledger_sparql_with_identity(ledger_id, sparql, Some(id))
                    .await
                    .inspect_err(|_| {
                        set_span_error_code(&span, "error:QueryFailed");
                    })?,
                None => state
                    .fluree
                    .query_ledger_sparql_jsonld(ledger_id, sparql)
                    .await
                    .inspect_err(|_| {
                        set_span_error_code(&span, "error:QueryFailed");
                    })?,
            };
            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Identity-based queries go through connection path (returns pre-formatted JSON)
        if let Some(id) = identity {
            if has_dataset_clause {
                return Err(ServerError::not_acceptable(
                    "FROM/FROM NAMED is not currently supported with identity-scoped SPARQL on the ledger-scoped endpoint".to_string(),
                ));
            }
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported for identity-scoped SPARQL queries",
                    fmt.name().to_uppercase()
                )));
            }
            let result = state
                .fluree
                .query_ledger_sparql_with_identity(ledger_id, sparql, Some(id))
                .await
                .inspect_err(|_| {
                    set_span_error_code(&span, "error:QueryFailed");
                })?;
            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Ledger-scoped SPARQL with dataset clauses (FROM/FROM NAMED): build a dataset
        // of graphs within this ledger and execute as a dataset query.
        if has_dataset_clause {
            if let Some(fmt) = delimited {
                return Err(ServerError::not_acceptable(format!(
                    "{} format not supported for SPARQL dataset clauses on the ledger endpoint",
                    fmt.name().to_uppercase()
                )));
            }

            let Some(dc) = dataset_clause else {
                // Should be unreachable given has_dataset_clause
                return Err(ServerError::bad_request("Invalid SPARQL dataset clause"));
            };

            if dc.to_graph.is_some() {
                return Err(ServerError::bad_request(
                    "SPARQL history range (FROM <...> TO <...>) is not supported on the ledger-scoped endpoint; use /fluree/query instead",
                ));
            }

            // Ensure head is fresh in shared storage mode before time-travel view loading.
            if !state.config.is_proxy_storage_mode() {
                let _ = load_ledger_for_query(state, ledger_id, &span).await?;
            }

            let base_path = base_ledger_id(ledger_id)?;

            let mut spec = DatasetSpec::new();

            let mut add_default = |raw: &str| -> Result<()> {
                // If FROM explicitly names this ledger, treat as default graph.
                if raw == ledger_id {
                    spec.default_graphs.push(GraphSource::new(ledger_id).with_graph(GraphSelector::Default));
                    return Ok(());
                }

                // If it looks like a ledger ref (name:branch or has @ / #), enforce mismatch rules.
                let looks_like_ledger_ref = raw.contains('@')
                    || raw.contains('#')
                    || (raw.contains(':') && !raw.contains("://") && !raw.starts_with("urn:"));

                if looks_like_ledger_ref {
                    let (no_frag, frag) = split_graph_fragment(raw);
                    let (base, time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
                        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in FROM: {e}")))?;
                    if base != base_path {
                        return Err(ServerError::bad_request(format!(
                            "Ledger mismatch: endpoint ledger is '{ledger_id}' but SPARQL FROM targets '{raw}'"
                        )));
                    }
                    let time_spec = time.map(|t| match t {
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtIso(iso) => TimeSpec::AtTime(iso),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtCommit(c) => TimeSpec::AtCommit(c),
                    });
                    let selector = frag.map(GraphSelector::from_str).unwrap_or(GraphSelector::Default);
                    let mut src = GraphSource::new(ledger_id).with_graph(selector);
                    if let Some(ts) = time_spec {
                        src = src.with_time(ts);
                    }
                    spec.default_graphs.push(src);
                    return Ok(());
                }

                // Otherwise treat as a graph selector within this ledger.
                let selector = GraphSelector::from_str(raw);
                spec.default_graphs
                    .push(GraphSource::new(ledger_id).with_graph(selector));
                Ok(())
            };

            let mut add_named = |raw: &str| -> Result<()> {
                let looks_like_ledger_ref = raw.contains('@')
                    || raw.contains('#')
                    || (raw.contains(':') && !raw.contains("://") && !raw.starts_with("urn:"));

                if looks_like_ledger_ref {
                    let (no_frag, frag) = split_graph_fragment(raw);
                    let (base, time) = fluree_db_core::ledger_id::split_time_travel_suffix(no_frag)
                        .map_err(|e| ServerError::bad_request(format!("Invalid time travel in FROM NAMED: {e}")))?;
                    if base != base_path {
                        return Err(ServerError::bad_request(format!(
                            "Ledger mismatch: endpoint ledger is '{ledger_id}' but SPARQL FROM NAMED targets '{raw}'"
                        )));
                    }
                    let time_spec = time.map(|t| match t {
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtIso(iso) => TimeSpec::AtTime(iso),
                        fluree_db_core::ledger_id::LedgerIdTimeSpec::AtCommit(c) => TimeSpec::AtCommit(c),
                    });
                    let selector = frag.map(GraphSelector::from_str).unwrap_or(GraphSelector::Default);
                    let mut src = GraphSource::new(ledger_id)
                        .with_graph(selector)
                        .with_alias(raw);
                    if let Some(ts) = time_spec {
                        src = src.with_time(ts);
                    }
                    spec.named_graphs.push(src);
                    return Ok(());
                }

                // Named graph selector within this ledger; alias must match query's GRAPH IRI.
                let selector = GraphSelector::from_str(raw);
                spec.named_graphs.push(
                    GraphSource::new(ledger_id)
                        .with_graph(selector)
                        .with_alias(raw),
                );
                Ok(())
            };

            // Default graphs: if none specified, use this ledger's default graph.
            if dc.default_graphs.is_empty() {
                spec.default_graphs
                    .push(GraphSource::new(ledger_id).with_graph(GraphSelector::Default));
            } else {
                for iri in &dc.default_graphs {
                    add_default(&iri_to_string(iri))?;
                }
            }

            for iri in &dc.named_graphs {
                add_named(&iri_to_string(iri))?;
            }

            let result = match &state.fluree {
                FlureeInstance::File(f) => {
                    let dataset = f.build_dataset_view(&spec).await.map_err(ServerError::Api)?;
                    dataset
                        .query(f.as_ref())
                        .sparql(sparql)
                        .execute_formatted()
                        .await
                        .map_err(ServerError::Api)?
                }
                FlureeInstance::Proxy(p) => {
                    let dataset = p.build_dataset_view(&spec).await.map_err(ServerError::Api)?;
                    dataset
                        .query(p.as_ref())
                        .sparql(sparql)
                        .execute_formatted()
                        .await
                        .map_err(ServerError::Api)?
                }
            };

            return Ok((HeaderMap::new(), Json(result)).into_response());
        }

        // Shared storage mode: use load_ledger_for_query with freshness checking
        let ledger = load_ledger_for_query(state, ledger_id, &span)
            .await
            .inspect_err(|_| {
                set_span_error_code(&span, "error:LedgerLoad");
            })?;
        let graph = GraphDb::from_ledger_state(&ledger);
        let fluree = state.fluree.as_file();

        // Delimited fast path: execute raw query and format as TSV/CSV bytes
        if let Some(fmt) = delimited {
            let result = graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .execute()
                .await
                .map_err(|e| {
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %e, "SPARQL query execution failed");
                    ServerError::Api(e)
                })?;

            let row_count = result.row_count();
            let bytes = match fmt {
                DelimitedFormat::Tsv => result.to_tsv_bytes(&graph.snapshot),
                DelimitedFormat::Csv => result.to_csv_bytes(&graph.snapshot),
            }
            .map_err(|e| {
                ServerError::internal(format!(
                    "{} formatting error: {}",
                    fmt.name().to_uppercase(),
                    e
                ))
            })?;

            tracing::info!(status = "success", format = fmt.name(), row_count);
            return Ok(delimited_response(bytes, fmt));
        }

        // Execute SPARQL query via builder - formatted JSON output
        let result = graph
            .query(fluree.as_ref())
            .sparql(sparql)
            .execute_formatted()
            .await
            .inspect_err(|_| {
                set_span_error_code(&span, "error:QueryFailed");
            })?;
        Ok((HeaderMap::new(), Json(result)).into_response())
    }
    .instrument(span)
    .await
}

/// Explain a query
///
/// POST /fluree/explain
/// GET /fluree/explain
///
/// Returns the query execution plan without executing.
/// Supports signed requests (JWS/VC format).
pub async fn explain(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    credential: MaybeCredential,
) -> Result<Json<JsonValue>> {
    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "explain",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger ID determined later
        None, // tenant_id not yet supported
        None, // explain is the operation, no input format needed
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "explain request received");

        // Enforce data auth if configured (Bearer token OR signed request)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required
            && !credential.is_signed()
            && bearer.0.is_none()
        {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized(
                "Authentication required (signed request or Bearer token)",
            ));
        }

        // Parse body as JSON
        let mut query_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in request body");
                return Err(e);
            }
        };

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        // Get ledger id
        let ledger_id = match get_ledger_id(None, &headers, &query_json) {
            Ok(ledger_id) => {
                span.record("ledger_id", ledger_id.as_str());
                ledger_id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger_id) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Force auth-derived identity and policy-class into opts (non-spoofable)
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        force_query_auth_opts(&mut query_json, identity.as_deref(), policy_class);

        // Execute explain
        let result = if state.config.is_proxy_storage_mode() {
            // Proxy mode: use FlureeInstance wrapper
            match state.fluree.explain_ledger(&ledger_id, &query_json).await {
                Ok(result) => {
                    tracing::info!(status = "success", "explain completed (proxy)");
                    result
                }
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %server_error, "explain execution failed (proxy)");
                    return Err(server_error);
                }
            }
        } else {
            // Shared storage mode: use load_ledger_for_query with freshness checking
            let ledger = load_ledger_for_query(&state, &ledger_id, &span).await?;
            match state.fluree.as_file().explain(&ledger, &query_json).await {
                Ok(result) => {
                    tracing::info!(status = "success", "explain completed");
                    result
                }
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&span, "error:InvalidQuery");
                    tracing::error!(error = %server_error, "explain execution failed");
                    return Err(server_error);
                }
            }
        };

        Ok(Json(result))
    }
    .instrument(span)
    .await
}

// ===== Peer mode support =====

/// Load a ledger for query, handling peer mode freshness checking.
///
/// **Note**: This function is only used for non-proxy storage modes (file-backed Fluree).
/// In proxy mode, routes use `FlureeInstance` wrapper methods instead.
///
/// Load a ledger for query execution
///
/// In transaction mode, simply loads the ledger via ledger_cached().
/// In peer mode with shared storage, checks if the local ledger is stale vs SSE watermarks
/// and reloads if needed using LedgerManager::reload() for coalesced reloading.
pub(crate) async fn load_ledger_for_query(
    state: &AppState,
    ledger_id: &str,
    span: &tracing::Span,
) -> Result<LedgerState> {
    let fluree = state.fluree.as_file();

    // Get cached handle (loads if not cached)
    let handle = fluree.ledger_cached(ledger_id).await.map_err(|e| {
        set_span_error_code(span, "error:NotFound");
        tracing::error!(error = %e, "ledger not found");
        ServerError::Api(e)
    })?;

    // In transaction mode, just return the cached state
    if state.config.server_role != ServerRole::Peer {
        return Ok(handle.snapshot().await.to_ledger_state());
    }

    // In peer mode (shared storage), check freshness and potentially reload
    let peer_state = state
        .peer_state
        .as_ref()
        .expect("peer_state should exist in peer mode");

    // Check freshness using FreshnessSource trait
    // If no watermark available (SSE hasn't seen ledger), treat as current (lenient policy)
    if let Some(watermark) = peer_state.watermark(ledger_id) {
        match handle.check_freshness(&watermark).await {
            FreshnessCheck::Stale => {
                // Remote is ahead - reload ledger from shared storage
                // Uses LedgerManager::reload() which handles coalescing
                tracing::info!(
                    ledger_id = ledger_id,
                    remote_index_t = watermark.index_t,
                    "Refreshing ledger for peer query"
                );

                if let Some(mgr) = fluree.ledger_manager() {
                    mgr.reload(ledger_id).await.map_err(ServerError::Api)?;
                    state.refresh_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            FreshnessCheck::Current => {
                // Local is fresh, use cached
            }
        }
    } else {
        // No watermark = lenient policy: proceed with cached state
        tracing::debug!(
            ledger_id = ledger_id,
            "Ledger not yet seen in SSE, using cached state"
        );
    }

    Ok(handle.snapshot().await.to_ledger_state())
}

// Note: reload_ledger_coalesced has been removed in favor of LedgerManager::reload()
// which provides built-in coalescing of concurrent reload requests.

/// Execute a history query (query with explicit `to` key)
///
/// History queries must go through the connection/dataset path to properly handle:
/// - Dataset parsing with `from` and `to` keys for history time ranges
/// - Correct index selection for history mode (includes retracted data)
/// - `@op` binding population (true = assert, false = retract)
///
/// If the query doesn't have a `from` key, the ledger ID from the URL path is injected.
async fn execute_history_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger ID from the URL path
    // This allows users to POST to /:ledger/query with just `{ "to": "...", ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(ledger_id.to_string()));
        }
    }

    // Execute through the connection path which handles dataset/history parsing
    if has_tracking_opts(&query) {
        let response = match state.fluree.query_connection_jsonld_tracked(&query).await {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "history",
                    "tracked history query failed"
                );
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        // Serialize TrackedQueryResponse to JSON
        let json = match serde_json::to_value(&response) {
            Ok(json) => json,
            Err(e) => {
                let server_error =
                    ServerError::internal(format!("Failed to serialize response: {}", e));
                set_span_error_code(span, "error:InternalError");
                tracing::error!(error = %server_error, "response serialization failed");
                return Err(server_error);
            }
        };

        tracing::info!(
            status = "success",
            tracked = true,
            query_kind = "history",
            time = ?response.time,
            fuel = response.fuel
        );
        Ok((headers, Json(json)))
    } else {
        match state.fluree.query_connection_jsonld(&query).await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "history",
                    result_count = result.as_array().map(|a| a.len()).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)))
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "history",
                    "history query failed"
                );
                Err(server_error)
            }
        }
    }
}

/// Execute a dataset query (query with from-named, from array, or structured from object)
///
/// Dataset queries must go through the connection/dataset path to properly handle:
/// - Multiple default graphs (from array)
/// - Named graphs (from-named)
/// - Graph selectors (from object with graph field)
/// - Dataset-local aliases for GRAPH patterns
///
/// If the query doesn't have a `from` key, the ledger ID from the URL path is injected.
async fn execute_dataset_query(
    state: &AppState,
    ledger_id: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger ID from the URL path
    // This allows users to POST to /:ledger/query with just `{ "from-named": [...], ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(ledger_id.to_string()));
        }
    }

    // Execute through the connection path which handles dataset parsing
    if has_tracking_opts(&query) {
        let response = match state.fluree.query_connection_jsonld_tracked(&query).await {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "dataset",
                    "tracked dataset query failed"
                );
                return Err(server_error);
            }
        };

        // Record tracker fields on the execution span
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

        let tally = TrackingTally {
            time: response.time.clone(),
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        // Serialize TrackedQueryResponse to JSON
        let json = match serde_json::to_value(&response) {
            Ok(json) => json,
            Err(e) => {
                let server_error =
                    ServerError::internal(format!("Failed to serialize response: {}", e));
                set_span_error_code(span, "error:InternalError");
                tracing::error!(error = %server_error, "response serialization failed");
                return Err(server_error);
            }
        };

        tracing::info!(
            status = "success",
            tracked = true,
            query_kind = "dataset",
            time = ?response.time,
            fuel = response.fuel
        );
        Ok((headers, Json(json)))
    } else {
        match state.fluree.query_connection_jsonld(&query).await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    query_kind = "dataset",
                    result_count = result.as_array().map(|a| a.len()).unwrap_or(0)
                );
                Ok((HeaderMap::new(), Json(result)))
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(
                    error = %server_error,
                    query_kind = "dataset",
                    "dataset query failed"
                );
                Err(server_error)
            }
        }
    }
}
