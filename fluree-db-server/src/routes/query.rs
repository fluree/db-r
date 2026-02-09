//! Query endpoints: /fluree/query, /fluree/explain, /:ledger/query
//!
//! Supports both JSON (FQL) and SPARQL query content types:
//! - `application/json`: FQL query format (JSON body with "from" field)
//! - `application/sparql-query`: SPARQL query syntax (raw SPARQL string in body)
//!
//! For SPARQL UPDATE operations, use the transact endpoints instead.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{tracking_headers, FlureeHeaders, MaybeCredential, MaybeDataBearer};
// Note: NeedsRefresh is no longer used - replaced by FreshnessSource trait
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, log_query_text, set_span_error_code,
    should_log_query_text,
};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::Json;
use fluree_db_api::{FlureeView, FreshnessCheck, FreshnessSource, LedgerState, TrackingTally};
use serde_json::Value as JsonValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

/// Force auth identity/policy-class into FQL query opts, overriding client-provided values.
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

/// Helper to extract ledger alias from request (for FQL queries)
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
/// - FQL queries (JSON body with "from" field for ledger)
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

    let span = create_request_span(
        "query",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None, // tenant_id not yet supported
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
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

    // Handle SPARQL query
    if headers.is_sparql_query() || credential.is_sparql {
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
                // Extract ledger aliases from FROM/FROM NAMED clauses.
                // Parse failure → fall through (let the engine produce a proper error).
                if let Ok(aliases) = fluree_db_api::sparql_dataset_aliases(&sparql) {
                    for alias in &aliases {
                        if !p.can_read(alias) {
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
                Ok((HeaderMap::new(), Json(result)))
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidQuery");
                tracing::error!(error = %server_error, query_kind = "sparql", "query failed");
                Err(server_error)
            }
        }
    } else {
        // Handle FQL query (JSON body)
        let mut query_json: JsonValue = credential.body_json()?;

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &span);
            }
        }

        // Get ledger alias
        let alias = match get_ledger_id(None, &headers, &query_json) {
            Ok(alias) => {
                span.record("ledger_id", alias.as_str());
                alias
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&alias) {
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Force auth-derived identity and policy-class into opts (non-spoofable)
        let identity = effective_identity(&credential, &bearer);
        let policy_class = data_auth.default_policy_class.as_deref();
        force_query_auth_opts(&mut query_json, identity.as_deref(), policy_class);

        execute_query(&state, &alias, &query_json).await
    }
}

/// Execute a query with ledger in path
///
/// POST /:ledger/query
/// GET /:ledger/query
///
/// Supports:
/// - FQL queries (JSON body)
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

    let span = create_request_span(
        "query_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None, // tenant_id not yet supported
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "ledger query request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
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

    // Handle SPARQL query - ledger is known from path
    if headers.is_sparql_query() || credential.is_sparql {
        let sparql = credential.body_string()?;

        // Log query text according to configuration
        log_query_text(&sparql, &state.telemetry_config, &span);

        // Enforce bearer ledger scope for unsigned requests
        if let Some(p) = bearer.0.as_ref() {
            if !credential.is_signed() && !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let identity = effective_identity(&credential, &bearer);
        return execute_sparql_ledger(&state, &ledger, &sparql, identity.as_deref()).await;
    }

    // Handle FQL query (JSON body)
    let mut query_json: JsonValue = credential.body_json()?;

    // Log query text according to configuration (only serialize if needed)
    if should_log_query_text(&state.telemetry_config) {
        if let Ok(query_text) = serde_json::to_string(&query_json) {
            log_query_text(&query_text, &state.telemetry_config, &span);
        }
    }

    // Get ledger alias (path takes precedence)
    let alias = match get_ledger_id(Some(&ledger), &headers, &query_json) {
        Ok(alias) => {
            span.record("ledger_id", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    // Inject header values into query opts
    inject_headers_into_query(&mut query_json, &headers);

    // Enforce bearer ledger scope for unsigned requests
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_read(&ledger) {
            return Err(ServerError::not_found("Ledger not found"));
        }
    }

    // Force auth-derived identity and policy-class into opts (non-spoofable)
    let identity = effective_identity(&credential, &bearer);
    let policy_class = data_auth.default_policy_class.as_deref();
    force_query_auth_opts(&mut query_json, identity.as_deref(), policy_class);

    execute_query(&state, &alias, &query_json).await
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

async fn execute_query(
    state: &AppState,
    alias: &str,
    query_json: &JsonValue,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Create execution span
    let span = tracing::info_span!("query_execute", ledger_id = alias, query_kind = "fql");
    let _guard = span.enter();

    // Check for history query: explicit "to" key indicates history mode
    // History queries must go through the dataset/connection path for correct index selection
    if query_json.get("to").is_some() {
        return execute_history_query(state, alias, query_json, &span).await;
    }

    // Check for dataset features (from-named, from array, from object with graph/alias/time)
    // These require the connection execution path for proper dataset handling
    if requires_dataset_features(query_json) {
        return execute_dataset_query(state, alias, query_json, &span).await;
    }

    // In proxy mode, use the unified FlureeInstance methods (no local freshness checking)
    if state.config.is_proxy_storage_mode() {
        return execute_query_proxy(state, alias, query_json, &span).await;
    }

    // Shared storage mode: use load_ledger_for_query with freshness checking
    let ledger = load_ledger_for_query(state, alias, &span).await?;
    let graph = FlureeView::from_ledger_state(&ledger);
    let fluree = state.fluree.as_file();

    // Check if tracking is requested
    if has_tracking_opts(query_json) {
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
        return Ok((headers, Json(json)));
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
    Ok((HeaderMap::new(), Json(result)))
}

/// Execute an FQL query in proxy mode (uses FlureeInstance wrapper methods)
async fn execute_query_proxy(
    state: &AppState,
    alias: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Check if tracking is requested
    if has_tracking_opts(query_json) {
        // Execute tracked query via FlureeInstance wrapper
        let response = match state.fluree.query_ledger_tracked(alias, query_json).await {
            Ok(response) => response,
            Err(e) => {
                let server_error =
                    ServerError::Api(fluree_db_api::ApiError::http(e.status, e.error));
                set_span_error_code(span, "error:InvalidQuery");
                tracing::error!(error = %server_error, "tracked query failed (proxy)");
                return Err(server_error);
            }
        };

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
    let result = match state.fluree.query_ledger_jsonld(alias, query_json).await {
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

/// Execute a SPARQL query against a specific ledger and return JSON result
async fn execute_sparql_ledger(
    state: &AppState,
    alias: &str,
    sparql: &str,
    identity: Option<&str>,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Create span for peer mode loading
    let span = tracing::info_span!("sparql_execute", ledger_id = alias);
    let _guard = span.enter();

    // In proxy mode, use the unified FlureeInstance method
    if state.config.is_proxy_storage_mode() {
        let result = match identity {
            Some(id) => {
                state
                    .fluree
                    .query_ledger_sparql_with_identity(alias, sparql, Some(id))
                    .await?
            }
            None => {
                state
                    .fluree
                    .query_ledger_sparql_jsonld(alias, sparql)
                    .await?
            }
        };
        return Ok((HeaderMap::new(), Json(result)));
    }

    // Shared storage mode: use load_ledger_for_query with freshness checking
    let ledger = load_ledger_for_query(state, alias, &span).await?;
    let graph = FlureeView::from_ledger_state(&ledger);
    let fluree = state.fluree.as_file();

    // Execute SPARQL query via builder
    // Note: SPARQL tracking not yet implemented - returns empty headers
    let result = match identity {
        Some(id) => {
            state
                .fluree
                .query_ledger_sparql_with_identity(alias, sparql, Some(id))
                .await?
        }
        None => {
            graph
                .query(fluree.as_ref())
                .sparql(sparql)
                .execute_formatted()
                .await?
        }
    };
    Ok((HeaderMap::new(), Json(result)))
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
        None, // ledger alias determined later
        None, // tenant_id not yet supported
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "explain request received");

    // Enforce data auth if configured (Bearer token OR signed request)
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required
        && !credential.is_signed()
        && bearer.0.is_none()
    {
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

    // Get ledger alias
    let alias = match get_ledger_id(None, &headers, &query_json) {
        Ok(alias) => {
            span.record("ledger_id", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    // Inject header values into query opts
    inject_headers_into_query(&mut query_json, &headers);

    // Enforce bearer ledger scope for unsigned requests
    if let Some(p) = bearer.0.as_ref() {
        if !credential.is_signed() && !p.can_read(&alias) {
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
        match state.fluree.explain_ledger(&alias, &query_json).await {
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
        let ledger = load_ledger_for_query(&state, &alias, &span).await?;
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
    alias: &str,
    span: &tracing::Span,
) -> Result<LedgerState<fluree_db_api::FileStorage>> {
    let fluree = state.fluree.as_file();

    // Get cached handle (loads if not cached)
    let handle = fluree.ledger_cached(alias).await.map_err(|e| {
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
    if let Some(watermark) = peer_state.watermark(alias) {
        match handle.check_freshness(&watermark).await {
            FreshnessCheck::Stale => {
                // Remote is ahead - reload ledger from shared storage
                // Uses LedgerManager::reload() which handles coalescing
                tracing::info!(
                    alias = alias,
                    remote_index_t = watermark.index_t,
                    "Refreshing ledger for peer query"
                );

                if let Some(mgr) = fluree.ledger_manager() {
                    mgr.reload(alias).await.map_err(ServerError::Api)?;
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
            alias = alias,
            "Ledger not yet seen in SSE, using cached state"
        );
    }

    // Return snapshot as LedgerState for backward compatibility
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
/// If the query doesn't have a `from` key, the ledger alias from the URL path is injected.
async fn execute_history_query(
    state: &AppState,
    alias: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger alias from the URL path
    // This allows users to POST to /:ledger/query with just `{ "to": "...", ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(alias.to_string()));
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
/// If the query doesn't have a `from` key, the ledger alias from the URL path is injected.
async fn execute_dataset_query(
    state: &AppState,
    alias: &str,
    query_json: &JsonValue,
    span: &tracing::Span,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Clone the query so we can potentially inject the `from` key
    let mut query = query_json.clone();

    // If query doesn't have a `from` key, inject the ledger alias from the URL path
    // This allows users to POST to /:ledger/query with just `{ "from-named": [...], ... }`
    if query.get("from").is_none() {
        if let Some(obj) = query.as_object_mut() {
            obj.insert("from".to_string(), JsonValue::String(alias.to_string()));
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
