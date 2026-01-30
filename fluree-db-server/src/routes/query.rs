//! Query endpoints: /fluree/query, /fluree/explain, /:ledger/query
//!
//! Supports both JSON (FQL) and SPARQL query content types:
//! - `application/json`: FQL query format (JSON body with "from" field)
//! - `application/sparql-query`: SPARQL query syntax (raw SPARQL string in body)
//!
//! For SPARQL UPDATE operations, use the transact endpoints instead.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{tracking_headers, FlureeHeaders, MaybeCredential};
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
use tracing::Instrument;

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
fn get_ledger_alias(
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

/// Inject credential DID as identity (takes precedence over header identity)
fn inject_credential_did(query: &mut JsonValue, did: &str) {
    if let Some(obj) = query.as_object_mut() {
        // Get or create opts object
        let opts = obj
            .entry("opts")
            .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));

        if let Some(opts_obj) = opts.as_object_mut() {
            // Credential DID overrides header identity
            opts_obj.insert("identity".to_string(), JsonValue::String(did.to_string()));
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

    async {
        tracing::info!(status = "start", "query request received");
        // SPARQL UPDATE should use the transact endpoint, not query
        if headers.is_sparql_update() || credential.is_sparql_update {
            let error = ServerError::bad_request(
                "SPARQL UPDATE requests should use the /fluree/transact endpoint, not /fluree/query",
            );
            set_span_error_code(&tracing::Span::current(), "error:BadRequest");
            tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
            return Err(error);
        }

        // Handle SPARQL query
        if headers.is_sparql_query() || credential.is_sparql {
            let sparql = credential.body_string()?;

            // Log query text according to configuration
            log_query_text(&sparql, &state.telemetry_config, &tracing::Span::current());

            // Connection-scoped SPARQL requires a FROM/FROM NAMED clause to specify the ledger.
            //
            // NOTE: We intentionally do NOT fall back to the fluree-ledger header here.
            // Ledger-scoped SPARQL without FROM is supported via the /:ledger/query route.
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
                    set_span_error_code(&tracing::Span::current(), "error:InvalidQuery");
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
                    log_query_text(&query_text, &state.telemetry_config, &tracing::Span::current());
                }
            }

            // Get ledger alias
            let alias = match get_ledger_alias(None, &headers, &query_json) {
                Ok(alias) => {
                    tracing::Span::current().record("ledger_alias", &alias.as_str());
                    alias
                }
                Err(e) => {
                    set_span_error_code(&tracing::Span::current(), "error:BadRequest");
                    tracing::warn!(error = %e, "missing ledger alias");
                    return Err(e);
                }
            };

            // Inject header values into query opts
            inject_headers_into_query(&mut query_json, &headers);

            // If request was signed, credential DID takes precedence as identity
            if let Some(did) = credential.did() {
                inject_credential_did(&mut query_json, did);
            }

            execute_query(&state, &alias, &query_json).await
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
/// - FQL queries (JSON body)
/// - SPARQL queries (Content-Type: application/sparql-query)
/// - Signed requests (JWS/VC format)
///   - Ledger-scoped: FROM clause is optional (ledger from path is used)
pub async fn query_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
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

    async {
        tracing::info!(status = "start", "ledger query request received");

        // SPARQL UPDATE should use the transact endpoint, not query
        if headers.is_sparql_update() || credential.is_sparql_update {
            let error = ServerError::bad_request(
                "SPARQL UPDATE requests should use the /:ledger/transact endpoint, not /:ledger/query",
            );
            set_span_error_code(&tracing::Span::current(), "error:BadRequest");
            tracing::warn!(error = %error, "SPARQL UPDATE sent to query endpoint");
            return Err(error);
        }

        // Handle SPARQL query - ledger is known from path
        if headers.is_sparql_query() || credential.is_sparql {
            let sparql = credential.body_string()?;

            // Log query text according to configuration
            log_query_text(&sparql, &state.telemetry_config, &tracing::Span::current());

            return execute_sparql_ledger(&state, &ledger, &sparql).await;
        }

        // Handle FQL query (JSON body)
        let mut query_json: JsonValue = credential.body_json()?;

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &tracing::Span::current());
            }
        }

        // Get ledger alias (path takes precedence)
        let alias = match get_ledger_alias(Some(&ledger), &headers, &query_json) {
            Ok(alias) => {
                tracing::Span::current().record("ledger_alias", &alias.as_str());
                alias
            }
            Err(e) => {
                set_span_error_code(&tracing::Span::current(), "error:BadRequest");
                tracing::warn!(error = %e, "ledger alias mismatch");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // If request was signed, credential DID takes precedence as identity
        if let Some(did) = credential.did() {
            inject_credential_did(&mut query_json, did);
        }

        execute_query(&state, &alias, &query_json).await
    }
    .instrument(span)
    .await
}

/// Execute an FQL query and return JSON result with tracking headers
///
/// When tracking options are present (via headers or body), returns a tracked response:
/// - Body: `{status, result, time?, fuel?, policy?}`
/// - Headers: `x-fdb-time`, `x-fdb-fuel`, `x-fdb-policy` (Clojure parity)
async fn execute_query(
    state: &AppState,
    alias: &str,
    query_json: &JsonValue,
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Create execution span with tracker fields for OTEL bridge
    let span = tracing::info_span!(
        "query_execute",
        ledger_alias = alias,
        query_kind = "fql",
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );

    async {
        // In proxy mode, use the unified FlureeInstance methods (no local freshness checking)
        if state.config.is_proxy_storage_mode() {
            return execute_query_proxy(state, alias, query_json, &tracing::Span::current()).await;
        }

        // Check for history query: explicit "to" key indicates history mode
        // History queries must go through the dataset/connection path for correct index selection
        if query_json.get("to").is_some() {
            return execute_history_query(state, alias, query_json, &tracing::Span::current())
                .await;
        }

        // Shared storage mode: use load_ledger_for_query with freshness checking
        let ledger =
            load_ledger_for_query(state, alias, &tracing::Span::current()).await?;
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
                    set_span_error_code(&tracing::Span::current(), "error:InvalidQuery");
                    tracing::error!(error = %server_error, "tracked query failed");
                    return Err(server_error);
                }
            };

            // Extract tracking info for headers
            let tally = TrackingTally {
                time: response.time.clone(),
                time_ms: None,
                fuel: response.fuel,
                policy: response.policy.clone(),
            };
            let headers = tracking_headers(&tally);

            // Bridge tracker metrics to span for OTEL export
            if let Some(ref time) = response.time {
                tracing::Span::current().record("tracker_time", time.as_str());
            }
            if let Some(fuel) = response.fuel {
                tracing::Span::current().record("tracker_fuel", fuel);
            }

            // Serialize TrackedQueryResponse to JSON
            let json = match serde_json::to_value(&response) {
                Ok(json) => json,
                Err(e) => {
                    let server_error =
                        ServerError::internal(format!("Failed to serialize response: {}", e));
                    set_span_error_code(&tracing::Span::current(), "error:InternalError");
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
                set_span_error_code(&tracing::Span::current(), "error:InvalidQuery");
                tracing::error!(error = %server_error, "query execution failed");
                return Err(server_error);
            }
        };
        Ok((HeaderMap::new(), Json(result)))
    }
    .instrument(span)
    .await
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
            time_ms: None,
            fuel: response.fuel,
            policy: response.policy.clone(),
        };
        let headers = tracking_headers(&tally);

        // Bridge tracker metrics to span for OTEL export
        if let Some(ref time) = response.time {
            span.record("tracker_time", time.as_str());
        }
        if let Some(fuel) = response.fuel {
            span.record("tracker_fuel", fuel);
        }

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
) -> Result<(HeaderMap, Json<JsonValue>)> {
    // Create span for peer mode loading
    let span = tracing::info_span!("sparql_execute", ledger_alias = alias);

    async {
        // In proxy mode, use the unified FlureeInstance method
        if state.config.is_proxy_storage_mode() {
            let result = state
                .fluree
                .query_ledger_sparql_jsonld(alias, sparql)
                .await?;
            return Ok((HeaderMap::new(), Json(result)));
        }

        // Shared storage mode: use load_ledger_for_query with freshness checking
        let ledger =
            load_ledger_for_query(state, alias, &tracing::Span::current()).await?;
        let graph = FlureeView::from_ledger_state(&ledger);
        let fluree = state.fluree.as_file();

        // Execute SPARQL query via builder
        // Note: SPARQL tracking not yet implemented - returns empty headers
        let result = graph
            .query(fluree.as_ref())
            .sparql(sparql)
            .execute_formatted()
            .await?;
        Ok((HeaderMap::new(), Json(result)))
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

    async {
        tracing::info!(status = "start", "explain request received");

        // Parse body as JSON
        let mut query_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&tracing::Span::current(), "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in request body");
                return Err(e);
            }
        };

        // Log query text according to configuration (only serialize if needed)
        if should_log_query_text(&state.telemetry_config) {
            if let Ok(query_text) = serde_json::to_string(&query_json) {
                log_query_text(&query_text, &state.telemetry_config, &tracing::Span::current());
            }
        }

        // Get ledger alias
        let alias = match get_ledger_alias(None, &headers, &query_json) {
            Ok(alias) => {
                tracing::Span::current().record("ledger_alias", &alias.as_str());
                alias
            }
            Err(e) => {
                set_span_error_code(&tracing::Span::current(), "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias");
                return Err(e);
            }
        };

        // Inject header values into query opts
        inject_headers_into_query(&mut query_json, &headers);

        // If request was signed, credential DID takes precedence as identity
        if let Some(did) = credential.did() {
            inject_credential_did(&mut query_json, did);
        }

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
                    set_span_error_code(&tracing::Span::current(), "error:InvalidQuery");
                    tracing::error!(error = %server_error, "explain execution failed (proxy)");
                    return Err(server_error);
                }
            }
        } else {
            // Shared storage mode: use load_ledger_for_query with freshness checking
            let ledger =
                load_ledger_for_query(&state, &alias, &tracing::Span::current()).await?;
            match state.fluree.as_file().explain(&ledger, &query_json).await {
                Ok(result) => {
                    tracing::info!(status = "success", "explain completed");
                    result
                }
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&tracing::Span::current(), "error:InvalidQuery");
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
    alias: &str,
    span: &tracing::Span,
) -> Result<LedgerState<fluree_db_api::FileStorage, fluree_db_api::SimpleCache>> {
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
                    mgr.reload(alias).await.map_err(|e| ServerError::Api(e))?;
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
            tracing::error!(error = %server_error, query_kind = "history", "history query failed");
            Err(server_error)
        }
    }
}
