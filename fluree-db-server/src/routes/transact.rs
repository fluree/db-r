//! Transaction endpoints: /fluree/transact, /fluree/update, /fluree/insert, /fluree/upsert

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
use fluree_db_api::{TxnOpts, TxnType};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;

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

/// Helper to extract ledger alias from request
fn get_ledger_alias(
    path_ledger: Option<&str>,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > header > body.ledger > body.from
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
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
pub async fn transact(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    // Transaction mode: process locally
    transact_local(state, request).await.into_response()
}

/// Local implementation of transact (transaction mode only)
async fn transact_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<Json<TransactResponse>> {
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

    tracing::info!(status = "start", "transaction request received");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in transaction body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Update, &body_json, did).await
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

    tracing::info!(status = "start", "ledger transaction request received");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in transaction body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Update, &body_json, did).await
}

/// Insert data
///
/// POST /fluree/insert
///
/// Convenience endpoint for insert-only transactions.
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn insert(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    insert_local(state, request).await.into_response()
}

/// Local implementation of insert
async fn insert_local(state: Arc<AppState>, request: Request) -> Result<Json<TransactResponse>> {
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

    tracing::info!(status = "start", "insert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in insert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Insert, &body_json, did).await
}

/// Upsert data
///
/// POST /fluree/upsert
///
/// Convenience endpoint for upsert transactions (insert or update).
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn upsert(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    upsert_local(state, request).await.into_response()
}

/// Local implementation of upsert
async fn upsert_local(state: Arc<AppState>, request: Request) -> Result<Json<TransactResponse>> {
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

    tracing::info!(status = "start", "upsert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in upsert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(None, &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Upsert, &body_json, did).await
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

    tracing::info!(status = "start", "ledger insert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in insert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Insert, &body_json, did).await
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

    tracing::info!(status = "start", "ledger upsert transaction requested");

    let body_json = match credential.body_json() {
        Ok(json) => json,
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid JSON in upsert request body");
            return Err(e);
        }
    };

    let alias = match get_ledger_alias(Some(&ledger), &headers, &body_json) {
        Ok(alias) => {
            span.record("ledger_alias", &alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "ledger alias mismatch");
            return Err(e);
        }
    };

    let did = credential.did().map(String::from);

    execute_transaction(&state, &alias, TxnType::Upsert, &body_json, did).await
}

/// Execute a transaction with the given type
async fn execute_transaction(
    state: &AppState,
    alias: &str,
    txn_type: TxnType,
    body: &JsonValue,
    did: Option<String>,
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

    // Build transaction options with DID as author if credential was signed
    let txn_opts = match did {
        Some(d) => TxnOpts::default().author(d),
        None => TxnOpts::default(),
    };

    // Build and execute the transaction via the builder API
    let fluree = state.fluree.as_file();
    let builder = fluree.stage(&handle);
    let builder = match txn_type {
        TxnType::Insert => builder.insert(body),
        TxnType::Upsert => builder.upsert(body),
        TxnType::Update => builder.update(body),
    };
    let mut builder = builder.txn_opts(txn_opts);
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
