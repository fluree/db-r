//! Transaction endpoints: /fluree/transact, /fluree/update, /fluree/insert, /fluree/upsert
//!
//! Supports multiple content types:
//! - `application/json`: JSON-LD transaction format
//! - `application/sparql-update`: SPARQL UPDATE syntax
//! - `text/turtle`: Turtle RDF format (for all transaction endpoints)
//! - `application/trig`: TriG format with named graphs (transact/upsert only)
//!
//! # Ledger Selection Priority
//!
//! For non-path endpoints (/fluree/transact, etc.), ledger is resolved in this order:
//! 1. Path parameter (/:ledger/transact, etc.)
//! 2. Query parameter (?ledger=mydb:main)
//! 3. Header (Fluree-Ledger: mydb:main)
//! 4. Body field ("ledger" or "from")
//!
//! # Turtle vs TriG Semantics
//!
//! - **Turtle on `/insert`**: Uses fast direct flake path. Pure insert semantics.
//! - **TriG on `/insert`**: Returns 400 error. Named graphs require upsert path.
//! - **Turtle/TriG on `/transact` or `/upsert`**: Uses upsert path with GRAPH block extraction for named graphs.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeCredential};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{
    lower_sparql_update, parse_sparql, CommitOpts, NamespaceRegistry, SparqlQueryBody, TxnOpts,
    TxnType,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Query parameters for transaction endpoints
#[derive(Debug, Deserialize, Default)]
pub struct TransactQueryParams {
    /// Target ledger (format: name:branch)
    pub ledger: Option<String>,
}

/// Commit information in transaction response
#[derive(Serialize)]
pub struct CommitInfo {
    /// Commit storage address
    pub address: String,
    /// Commit hash (SHA-256)
    pub hash: String,
}

/// Transaction response - matches Clojure server format
#[derive(Serialize)]
pub struct TransactResponse {
    /// Ledger alias
    pub ledger: String,
    /// Transaction time (t value)
    pub t: i64,
    /// Transaction ID (SHA-256 hash of transaction data)
    #[serde(rename = "tx-id")]
    pub tx_id: String,
    /// Commit information
    pub commit: CommitInfo,
}

/// Compute transaction ID from request body (SHA-256 hash)
///
/// This matches Clojure's derive-tx-id which hashes the JSON-LD normalized data.
/// For simplicity we hash the raw JSON bytes - this is deterministic for the same input.
fn compute_tx_id(body: &JsonValue) -> String {
    let json_bytes = serde_json::to_vec(body).unwrap_or_default();
    let hash = Sha256::digest(&json_bytes);
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// Compute transaction ID from SPARQL UPDATE string
fn compute_tx_id_sparql(sparql: &str) -> String {
    let hash = Sha256::digest(sparql.as_bytes());
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// If the request was signed (credentialed), return the *original* signed envelope
/// to store for provenance (JWS string or VC JSON).
fn raw_txn_from_credential(credential: &MaybeCredential) -> Option<JsonValue> {
    let extracted = credential.credential.as_ref()?;
    let raw = extracted.raw_body.as_ref();

    // Prefer JSON if it parses, otherwise store as string.
    if let Ok(s) = std::str::from_utf8(raw) {
        let trimmed = s.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            if let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) {
                return Some(json);
            }
        }
        return Some(JsonValue::String(trimmed.to_string()));
    }

    // Fallback for non-UTF8: store base64 string for auditability.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    Some(JsonValue::String(format!("base64:{}", b64)))
}

/// Extract query params from request URI before consuming the request
fn extract_query_params(request: &Request) -> TransactQueryParams {
    request
        .uri()
        .query()
        .and_then(|q| serde_urlencoded::from_str(q).ok())
        .unwrap_or_default()
}

/// Helper to extract ledger alias from request
///
/// Priority: path > query param > header > body.ledger > body.from
fn get_ledger_alias(
    path_ledger: Option<&str>,
    query_params: &TransactQueryParams,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > query param > header > body.ledger > body.from
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
    }

    if let Some(ledger) = &query_params.ledger {
        return Ok(ledger.clone());
    }

    if let Some(ledger) = &headers.ledger {
        return Ok(ledger.clone());
    }

    if let Some(ledger) = body.get("ledger").and_then(|v| v.as_str()) {
        return Ok(ledger.to_string());
    }

    if let Some(from) = body.get("from").and_then(|v| v.as_str()) {
        return Ok(from.to_string());
    }

    Err(ServerError::MissingLedger)
}

/// Execute a transaction
///
/// POST /fluree/transact
///
/// Executes a full transaction with insert, delete, and where clauses.
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn transact(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    // Transaction mode: process locally
    transact_local(state, request).await.into_response()
}

/// Local implementation of transact (transaction mode only)
async fn transact_local(state: Arc<AppState>, request: Request) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);
    // Extract headers
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Extract credential (consumes the request body)
    let credential = MaybeCredential::extract(request).await?;

    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "transact",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None, // tenant_id not yet supported
    );
    let _guard = span.enter();

    // Check if this is a SPARQL UPDATE request
    if credential.is_sparql_update() {
        tracing::info!(
            status = "start",
            format = "sparql-update",
            "SPARQL UPDATE request received"
        );
        return execute_sparql_update_request(
            &state,
            None,
            &query_params,
            &headers,
            &credential,
            &span,
        )
        .await;
    }

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "Turtle/TriG transaction received"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        // For Turtle/TriG, ledger must come from query param or header
        let alias = match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
            Some(ledger) => {
                span.record("ledger_alias", ledger.as_str());
                ledger.clone()
            }
            None => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!("missing ledger alias for Turtle/TriG transact");
                return Err(ServerError::MissingLedger);
            }
        };

        // /transact uses Update (upsert) semantics for Turtle/TriG
        return execute_turtle_transaction(&state, &alias, TxnType::Upsert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "transaction request received");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in transaction body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Update, &body_json, &credential).await
}

/// Execute a transaction with ledger in path
///
/// POST /:ledger/transact
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn transact_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    // Transaction mode: process locally
    transact_ledger_local(state, ledger, request)
        .await
        .into_response()
}

/// Local implementation of transact_ledger
async fn transact_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    request: Request,
) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "transact_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
    );
    let _guard = span.enter();

    // Check if this is a SPARQL UPDATE request
    if credential.is_sparql_update() {
        tracing::info!(
            status = "start",
            format = "sparql-update",
            "SPARQL UPDATE request received"
        );
        return execute_sparql_update_request(
            &state,
            Some(&ledger),
            &query_params,
            &headers,
            &credential,
            &span,
        )
        .await;
    }

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "Turtle/TriG ledger transaction received"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        // /transact uses Update (upsert) semantics for Turtle/TriG
        return execute_turtle_transaction(&state, &ledger, TxnType::Upsert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "ledger transaction request received");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in transaction body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Update, &body_json, &credential).await
}

/// Insert data
///
/// POST /fluree/insert
///
/// Convenience endpoint for insert-only transactions.
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn insert(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    insert_local(state, request).await.into_response()
}

/// Local implementation of insert
async fn insert_local(state: Arc<AppState>, request: Request) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "insert",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
    );
    let _guard = span.enter();

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "insert transaction requested"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        // For Turtle/TriG, ledger must come from query param or header
        let alias = match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
            Some(ledger) => {
                span.record("ledger_alias", ledger.as_str());
                ledger.clone()
            }
            None => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!("missing ledger alias for Turtle/TriG insert");
                return Err(ServerError::MissingLedger);
            }
        };

        return execute_turtle_transaction(&state, &alias, TxnType::Insert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "insert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in insert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Insert, &body_json, &credential).await
}

/// Upsert data
///
/// POST /fluree/upsert
///
/// Convenience endpoint for upsert transactions (insert or update).
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn upsert(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    upsert_local(state, request).await.into_response()
}

/// Local implementation of upsert
async fn upsert_local(state: Arc<AppState>, request: Request) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "upsert",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
    );
    let _guard = span.enter();

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "upsert transaction requested"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        // For Turtle/TriG, ledger must come from query param or header
        let alias = match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
            Some(ledger) => {
                span.record("ledger_alias", ledger.as_str());
                ledger.clone()
            }
            None => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!("missing ledger alias for Turtle/TriG upsert");
                return Err(ServerError::MissingLedger);
            }
        };

        return execute_turtle_transaction(&state, &alias, TxnType::Upsert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "upsert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in upsert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Upsert, &body_json, &credential).await
}

/// Insert data with ledger in path
///
/// POST /:ledger/insert
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn insert_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    insert_ledger_local(state, ledger, request)
        .await
        .into_response()
}

/// Local implementation of insert_ledger
async fn insert_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    request: Request,
) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "insert_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
    );
    let _guard = span.enter();

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "ledger insert transaction requested"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        return execute_turtle_transaction(&state, &ledger, TxnType::Insert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "ledger insert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in insert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Insert, &body_json, &credential).await
}

/// Upsert data with ledger in path
///
/// POST /:ledger/upsert
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn upsert_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    upsert_ledger_local(state, ledger, request)
        .await
        .into_response()
}

/// Local implementation of upsert_ledger
async fn upsert_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    request: Request,
) -> Result<Json<TransactResponse>> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);
    let trace_id = extract_trace_id(&credential.headers);

    let span = create_request_span(
        "upsert_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
    );
    let _guard = span.enter();

    // Check if this is a Turtle or TriG request
    if credential.is_turtle_or_trig() {
        let format = if credential.is_trig() {
            "trig"
        } else {
            "turtle"
        };
        tracing::info!(
            status = "start",
            format = format,
            "ledger upsert transaction requested"
        );

        let turtle = match credential.body_string() {
            Ok(s) => s,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                return Err(e);
            }
        };

        return execute_turtle_transaction(&state, &ledger, TxnType::Upsert, &turtle, &credential)
            .await;
    }

    tracing::info!(status = "start", "ledger upsert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in upsert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &query_params, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    execute_transaction(&state, &alias, TxnType::Upsert, &body_json, &credential).await
}

/// Execute a transaction with the given type
async fn execute_transaction(
    state: &AppState,
    alias: &str,
    txn_type: TxnType,
    body: &JsonValue,
    credential: &MaybeCredential,
) -> Result<Json<TransactResponse>> {
    // Create execution span
    let span = tracing::info_span!("transact_execute", ledger_alias = alias, txn_type = ?txn_type);
    let _guard = span.enter();

    // Compute tx-id from request body (before any modification)
    let tx_id = compute_tx_id(body);

    tracing::debug!(tx_id = %tx_id, "computed transaction ID");

    // Get cached ledger handle (loads if not cached)
    // Transaction execution is only in transaction mode (peers forward)
    let handle = match state.fluree.as_file().ledger_cached(alias).await {
        Ok(handle) => handle,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:NotFound");
            tracing::error!(error = %server_error, "ledger not found");
            return Err(server_error);
        }
    };

    let did = credential.did().map(String::from);

    // Build transaction options with DID as author if credential was signed
    let txn_opts = match &did {
        Some(d) => TxnOpts::default().author(d.clone()),
        None => TxnOpts::default(),
    };

    // If the request was signed, ALWAYS store the original signed envelope for provenance.
    // (No opt-in needed; this is the primary reason to store txn payloads.)
    let mut commit_opts = match &did {
        Some(d) => CommitOpts::default().author(d.clone()),
        None => CommitOpts::default(),
    };
    if let Some(raw_txn) = raw_txn_from_credential(credential) {
        commit_opts = commit_opts.with_raw_txn(raw_txn);
    }

    // Build and execute the transaction via the builder API
    let fluree = state.fluree.as_file();
    let builder = fluree.stage(&handle);
    let builder = match txn_type {
        TxnType::Insert => builder.insert(body),
        TxnType::Upsert => builder.upsert(body),
        TxnType::Update => builder.update(body),
    };
    let mut builder = builder.txn_opts(txn_opts).commit_opts(commit_opts);
    if let Some(config) = &state.index_config {
        builder = builder.index_config(config.clone());
    }

    let result = match builder.execute().await {
        Ok(result) => {
            tracing::info!(
                status = "success",
                commit_t = result.receipt.t,
                commit_address = %result.receipt.address,
                "transaction committed"
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:InvalidTransaction");
            tracing::error!(error = %server_error, "transaction failed");
            return Err(server_error);
        }
    };

    Ok(Json(TransactResponse {
        ledger: alias.to_string(),
        t: result.receipt.t,
        tx_id,
        commit: CommitInfo {
            address: result.receipt.address,
            hash: result.receipt.commit_id,
        },
    }))
}

// ===== Turtle/TriG execution =====

/// Compute transaction ID from Turtle/TriG string
fn compute_tx_id_turtle(turtle: &str) -> String {
    let hash = Sha256::digest(turtle.as_bytes());
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// Execute a Turtle/TriG transaction
///
/// This function handles both:
/// - text/turtle: Standard Turtle format (insert or upsert)
/// - application/trig: TriG format with GRAPH blocks for named graphs (upsert only)
///
/// # Insert vs Upsert Semantics
///
/// - **Insert with Turtle** (`text/turtle` on `/insert`): Uses direct flake parsing (fast path).
///   Pure insert - will fail if subjects already exist with conflicting data.
/// - **Insert with TriG** (`application/trig` on `/insert`): Not supported - returns 400.
///   Named graphs require the upsert path for GRAPH block extraction.
/// - **Upsert with Turtle/TriG** (`/upsert`): Uses `upsert_turtle` which handles GRAPH blocks
///   and supports named graph ingestion. For each (subject, predicate) pair, existing values
///   are retracted before new values are asserted.
async fn execute_turtle_transaction(
    state: &AppState,
    alias: &str,
    txn_type: TxnType,
    turtle: &str,
    credential: &MaybeCredential,
) -> Result<Json<TransactResponse>> {
    let is_trig = credential.is_trig();

    // Create execution span
    let format = if is_trig { "trig" } else { "turtle" };
    let span = tracing::info_span!("transact_execute", ledger_alias = alias, txn_type = ?txn_type, format = format);
    let _guard = span.enter();

    // TriG on /insert is not supported - named graphs require upsert path
    if is_trig && txn_type == TxnType::Insert {
        set_span_error_code(&span, "error:BadRequest");
        tracing::warn!("TriG format not supported on insert endpoint");
        return Err(ServerError::bad_request(
            "TriG format (application/trig) is not supported on the insert endpoint. \
             Named graph ingestion requires the upsert endpoint (/upsert or /:ledger/upsert).",
        ));
    }

    // Compute tx-id from Turtle string
    let tx_id = compute_tx_id_turtle(turtle);

    tracing::debug!(tx_id = %tx_id, "computed transaction ID");

    // Get cached ledger handle (loads if not cached)
    let handle = match state.fluree.as_file().ledger_cached(alias).await {
        Ok(handle) => handle,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:NotFound");
            tracing::error!(error = %server_error, "ledger not found");
            return Err(server_error);
        }
    };

    let did = credential.did().map(String::from);

    // Build transaction options with DID as author if credential was signed
    let txn_opts = match &did {
        Some(d) => TxnOpts::default().author(d.clone()),
        None => TxnOpts::default(),
    };

    // If the request was signed, ALWAYS store the original signed envelope for provenance.
    let mut commit_opts = match &did {
        Some(d) => CommitOpts::default().author(d.clone()),
        None => CommitOpts::default(),
    };
    if let Some(raw_txn) = raw_txn_from_credential(credential) {
        commit_opts = commit_opts.with_raw_txn(raw_txn);
    }

    // Build and execute the transaction via the builder API
    let fluree = state.fluree.as_file();
    let builder = fluree.stage(&handle);
    let builder = match txn_type {
        // Insert with plain Turtle: use fast direct flake path
        TxnType::Insert => builder.insert_turtle(turtle),
        // Upsert: use upsert_turtle which handles GRAPH blocks for named graphs
        TxnType::Upsert => builder.upsert_turtle(turtle),
        TxnType::Update => {
            // Update with Turtle is not supported - use SPARQL UPDATE instead
            set_span_error_code(&span, "error:BadRequest");
            return Err(ServerError::bad_request(
                "Turtle format is not supported for update transactions. Use SPARQL UPDATE instead.",
            ));
        }
    };
    let mut builder = builder.txn_opts(txn_opts).commit_opts(commit_opts);
    if let Some(config) = &state.index_config {
        builder = builder.index_config(config.clone());
    }

    let result = match builder.execute().await {
        Ok(result) => {
            tracing::info!(
                status = "success",
                commit_t = result.receipt.t,
                commit_address = %result.receipt.address,
                "Turtle/TriG transaction committed"
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:InvalidTransaction");
            tracing::error!(error = %server_error, "Turtle/TriG transaction failed");
            return Err(server_error);
        }
    };

    Ok(Json(TransactResponse {
        ledger: alias.to_string(),
        t: result.receipt.t,
        tx_id,
        commit: CommitInfo {
            address: result.receipt.address,
            hash: result.receipt.commit_id,
        },
    }))
}

// ===== SPARQL UPDATE execution =====

/// Execute a SPARQL UPDATE request
///
/// This function:
/// 1. Parses the SPARQL UPDATE string
/// 2. Extracts the UpdateOperation from the AST
/// 3. Lowers to Txn IR using lower_sparql_update
/// 4. Executes via the stage/commit pipeline
async fn execute_sparql_update_request(
    state: &AppState,
    path_ledger: Option<&str>,
    query_params: &TransactQueryParams,
    headers: &FlureeHeaders,
    credential: &MaybeCredential,
    parent_span: &tracing::Span,
) -> Result<Json<TransactResponse>> {
    // Extract SPARQL string from body
    let sparql = match credential.body_string() {
        Ok(s) => s,
        Err(e) => {
            set_span_error_code(parent_span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid SPARQL UPDATE body");
            return Err(e);
        }
    };

    // Compute tx-id from SPARQL string
    let tx_id = compute_tx_id_sparql(&sparql);

    // Get ledger alias from path, query param, or header (SPARQL UPDATE body doesn't contain ledger)
    let alias = match path_ledger {
        Some(ledger) => ledger.to_string(),
        None => match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
            Some(ledger) => ledger.clone(),
            None => {
                set_span_error_code(parent_span, "error:BadRequest");
                tracing::warn!("missing ledger alias for SPARQL UPDATE");
                return Err(ServerError::MissingLedger);
            }
        },
    };

    parent_span.record("ledger_alias", alias.as_str());

    // Parse SPARQL
    let parse_output = parse_sparql(&sparql);
    if parse_output.has_errors() {
        let errors: Vec<String> = parse_output
            .diagnostics
            .iter()
            .filter(|d| d.is_error())
            .map(|d| d.message.clone())
            .collect();
        set_span_error_code(parent_span, "error:SparqlParse");
        tracing::warn!(errors = ?errors, "SPARQL UPDATE parse errors");
        return Err(ServerError::bad_request(format!(
            "SPARQL UPDATE parse error: {}",
            errors.join("; ")
        )));
    }

    let ast = match parse_output.ast {
        Some(ast) => ast,
        None => {
            set_span_error_code(parent_span, "error:SparqlParse");
            return Err(ServerError::bad_request("Failed to parse SPARQL UPDATE"));
        }
    };

    // Verify this is an UPDATE operation
    let update_op = match &ast.body {
        SparqlQueryBody::Update(op) => op,
        _ => {
            set_span_error_code(parent_span, "error:BadRequest");
            tracing::warn!("Expected SPARQL UPDATE, got query");
            return Err(ServerError::bad_request(
                "Expected SPARQL UPDATE operation, got query. Use the /query endpoint for SELECT/CONSTRUCT/ASK/DESCRIBE.",
            ));
        }
    };

    // Get ledger handle
    let handle = match state.fluree.as_file().ledger_cached(&alias).await {
        Ok(handle) => handle,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(parent_span, "error:NotFound");
            tracing::error!(error = %server_error, "ledger not found");
            return Err(server_error);
        }
    };

    // Get namespace registry from the ledger's DB
    let snapshot = handle.snapshot().await;
    let mut ns = NamespaceRegistry::from_db(&snapshot.db);

    // Build transaction options
    let did = credential.did().map(String::from);
    let txn_opts = match did {
        Some(d) => TxnOpts::default().author(d),
        None => TxnOpts::default(),
    };

    // Lower SPARQL UPDATE to Txn IR
    let txn = match lower_sparql_update(update_op, &ast.prologue, &mut ns, txn_opts) {
        Ok(txn) => txn,
        Err(e) => {
            set_span_error_code(parent_span, "error:SparqlLower");
            tracing::warn!(error = %e, "SPARQL UPDATE lowering failed");
            return Err(ServerError::SparqlUpdateLower(e));
        }
    };

    tracing::debug!(
        tx_id = %tx_id,
        txn_type = ?txn.txn_type,
        where_patterns = txn.where_patterns.len(),
        delete_templates = txn.delete_templates.len(),
        insert_templates = txn.insert_templates.len(),
        "SPARQL UPDATE lowered to Txn IR"
    );

    // Execute the transaction using the Txn IR directly
    let fluree = state.fluree.as_file();
    let mut builder = fluree.stage(&handle).txn(txn);
    // If the request was signed, ALWAYS store the original signed envelope for provenance.
    if let Some(raw_txn) = raw_txn_from_credential(credential) {
        let mut commit_opts = CommitOpts::default();
        if let Some(d) = credential.did().map(String::from) {
            commit_opts = commit_opts.author(d);
        }
        builder = builder.commit_opts(commit_opts.with_raw_txn(raw_txn));
    }
    if let Some(config) = &state.index_config {
        builder = builder.index_config(config.clone());
    }

    let result = match builder.execute().await {
        Ok(result) => {
            tracing::info!(
                status = "success",
                commit_t = result.receipt.t,
                commit_address = %result.receipt.address,
                "SPARQL UPDATE committed"
            );
            result
        }
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(parent_span, "error:InvalidTransaction");
            tracing::error!(error = %server_error, "SPARQL UPDATE failed");
            return Err(server_error);
        }
    };

    Ok(Json(TransactResponse {
        ledger: alias,
        t: result.receipt.t,
        tx_id,
        commit: CommitInfo {
            address: result.receipt.address,
            hash: result.receipt.commit_id,
        },
    }))
}

// ===== Peer mode forwarding =====

/// Forward a write request to the transaction server (peer mode)
async fn forward_write_request(state: &AppState, request: Request) -> Response {
    let client = match state.forwarding_client.as_ref() {
        Some(c) => c,
        None => {
            return ServerError::internal("Forwarding client not configured").into_response();
        }
    };

    tracing::debug!("Forwarding write request to transaction server");

    // Forward the request and return the response directly
    // This preserves the upstream status codes (including 502/504 for errors)
    match client.forward(request).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}
