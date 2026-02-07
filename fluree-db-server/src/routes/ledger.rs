//! Ledger management endpoints: /fluree/create, /fluree/drop, /fluree/ledger-info

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{ApiError, DropMode, DropReport, DropStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Commit information in create response
#[derive(Serialize)]
pub struct CommitInfo {
    /// Commit storage address
    pub address: String,
    /// Commit hash (SHA-256) - empty for genesis
    pub hash: String,
}

/// Create ledger response - matches Clojure server format
#[derive(Serialize)]
pub struct CreateResponse {
    /// Ledger alias
    pub ledger: String,
    /// Transaction time (t=0 for new empty ledger)
    pub t: i64,
    /// Transaction ID (SHA-256 hash of create request)
    #[serde(rename = "tx-id")]
    pub tx_id: String,
    /// Commit information
    pub commit: CommitInfo,
}

/// Compute transaction ID from request body (SHA-256 hash)
fn compute_tx_id(body: &JsonValue) -> String {
    let json_bytes = serde_json::to_vec(body).unwrap_or_default();
    let hash = Sha256::digest(&json_bytes);
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// Create a new ledger
///
/// POST /fluree/create
///
/// Creates a new empty ledger with genesis state.
/// To add data, use /fluree/insert, /fluree/upsert, or /fluree/update after creation.
///
/// Request body:
/// - `ledger`: Required ledger alias (e.g., "mydb" or "mydb:main")
///
/// Returns 201 Created on success, 409 Conflict if ledger already exists.
/// In peer mode, forwards the request to the transaction server.
pub async fn create(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    create_local(state, request).await.into_response()
}

/// Local implementation of create
async fn create_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Read and parse body
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {}", e)))?;
    let body: JsonValue = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {}", e)))?;

    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "create_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "ledger creation requested");

    // Extract ledger alias from body
    let alias = match body
        .get("ledger")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ServerError::bad_request("Missing required field: ledger"))
    {
        Ok(alias) => {
            span.record("ledger_address", alias);
            alias.to_string()
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias in create request");
            return Err(e);
        }
    };

    // Compute tx-id from the request body
    let tx_id = compute_tx_id(&body);

    // Create the ledger (empty, t=0)
    // Ledger creation is only in transaction mode (peers forward)
    let ledger = match state.fluree.as_file().create_ledger(&alias).await {
        Ok(ledger) => ledger,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:AlreadyExists");
            tracing::error!(error = %server_error, "ledger creation failed");
            return Err(server_error);
        }
    };
    let ledger_address = ledger.ledger_address().to_string();

    let response = CreateResponse {
        ledger: ledger_address.clone(),
        t: 0,
        tx_id,
        commit: CommitInfo {
            // Genesis state address
            address: format!("fluree:memory://{}/main/head", ledger_address),
            hash: String::new(),
        },
    };

    tracing::info!(status = "success", "ledger created");
    Ok((StatusCode::CREATED, Json(response)))
}

/// Drop ledger request body
#[derive(Deserialize)]
pub struct DropRequest {
    /// Ledger alias
    pub ledger: String,
    /// Hard drop (delete files) - default false
    #[serde(default)]
    pub hard: bool,
}

/// Drop ledger response
#[derive(Serialize)]
pub struct DropResponse {
    /// Ledger alias
    pub ledger: String,
    /// Drop status
    pub status: String,
    /// Files deleted (hard mode only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_deleted: Option<usize>,
    /// Warnings (if any)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl From<DropReport> for DropResponse {
    fn from(report: DropReport) -> Self {
        let status = match report.status {
            DropStatus::Dropped => "dropped",
            DropStatus::AlreadyRetracted => "already_retracted",
            DropStatus::NotFound => "not_found",
        };

        let files_deleted = if report.commit_files_deleted > 0 || report.index_files_deleted > 0 {
            Some(report.commit_files_deleted + report.index_files_deleted)
        } else {
            None
        };

        DropResponse {
            ledger: report.ledger_address,
            status: status.to_string(),
            files_deleted,
            warnings: report.warnings,
        }
    }
}

/// Drop a ledger
///
/// POST /fluree/drop
///
/// Retracts a ledger from the nameservice.
/// With `hard: true`, also deletes all storage artifacts.
/// In peer mode, forwards the request to the transaction server.
pub async fn drop(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    drop_local(state, request).await.into_response()
}

/// Local implementation of drop
async fn drop_local(state: Arc<AppState>, request: Request) -> Result<Json<DropResponse>> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Read and parse body
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {}", e)))?;
    let req: DropRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {}", e)))?;

    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "drop_ledger",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
    );
    let _guard = span.enter();

    tracing::info!(
        status = "start",
        hard_drop = req.hard,
        "ledger drop requested"
    );

    let mode = if req.hard {
        DropMode::Hard
    } else {
        DropMode::Soft
    };

    // Ledger drop is only in transaction mode (peers forward)
    let report = match state.fluree.as_file().drop_ledger(&req.ledger, mode).await {
        Ok(report) => report,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:NotFound");
            tracing::error!(error = %server_error, "ledger drop failed");
            return Err(server_error);
        }
    };

    tracing::info!(status = "success", drop_status = ?report.status, "ledger dropped");
    Ok(Json(DropResponse::from(report)))
}

/// Ledger info response (simplified, used in proxy storage mode fallback)
#[derive(Serialize)]
pub struct LedgerInfoResponse {
    /// Ledger alias
    pub ledger: String,
    /// Current transaction time
    pub t: i64,
    /// Commit address (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Index address (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
}

/// Get ledger information
///
/// GET /fluree/ledger-info?ledger=<alias>
/// or with fluree-ledger header
///
/// In non-proxy mode (transaction server or peer with shared storage), returns
/// comprehensive ledger metadata including commit info, namespace codes, and
/// statistics (properties, classes with counts and hierarchy).
///
/// In proxy storage mode, returns simplified nameservice-only response since
/// the peer doesn't have local ledger state to compute stats from.
pub async fn info(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Response> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger_info",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "ledger info requested");

    // Get ledger alias from query param or header
    let alias = match query
        .ledger
        .as_ref()
        .or(headers.ledger.as_ref())
        .ok_or(ServerError::MissingLedger)
    {
        Ok(alias) => {
            span.record("ledger_address", alias.as_str());
            alias
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias in info request");
            return Err(e);
        }
    };

    // In proxy storage mode, return simplified nameservice-only response
    // (peer doesn't have local ledger state to compute full stats)
    if state.config.is_proxy_storage_mode() {
        return info_simplified(&state, alias, &span).await;
    }

    // Non-proxy mode: load ledger and return comprehensive info
    let ledger_state = super::query::load_ledger_for_query(&state, alias, &span).await?;

    // Get t value for backwards compatibility
    let t = ledger_state.db.t;

    // Build comprehensive ledger info (at parity with Clojure)
    //
    // By default we return the optimized base payload. Callers can opt into
    // heavier/real-time property details via query params.
    let opts = fluree_db_api::ledger_info::LedgerInfoOptions {
        realtime_property_details: query.realtime_property_details.unwrap_or(false),
        include_property_datatypes: query.include_property_datatypes.unwrap_or(false)
            || query.realtime_property_details.unwrap_or(false),
    };
    let mut info =
        fluree_db_api::ledger_info::build_ledger_info_with_options(&ledger_state, None, opts)
            .await
            .map_err(|e| {
                set_span_error_code(&span, "error:InternalError");
                tracing::error!(error = %e, "failed to build ledger info");
                ServerError::internal(format!("Failed to build ledger info: {}", e))
            })?;

    // Add top-level ledger alias and t for backwards compatibility
    if let Some(obj) = info.as_object_mut() {
        obj.insert(
            "ledger".to_string(),
            serde_json::Value::String(alias.to_string()),
        );
        obj.insert("t".to_string(), serde_json::Value::Number(t.into()));
    }

    tracing::info!(status = "success", "ledger info retrieved");
    Ok(Json(info).into_response())
}

/// Simplified ledger info for proxy storage mode (nameservice lookup only)
async fn info_simplified(state: &AppState, alias: &str, span: &tracing::Span) -> Result<Response> {
    // Lookup ledger in nameservice
    let record = match state.fluree.nameservice_lookup(alias).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            let server_error = ServerError::Api(ApiError::NotFound(alias.to_string()));
            set_span_error_code(span, "error:NotFound");
            tracing::warn!(error = %server_error, "ledger not found in nameservice");
            return Err(server_error);
        }
        Err(e) => {
            let server_error = ServerError::Api(ApiError::NameService(e));
            set_span_error_code(span, "error:InternalError");
            tracing::error!(error = %server_error, "nameservice lookup failed");
            return Err(server_error);
        }
    };

    // Return simplified ledger info from nameservice record
    tracing::info!(
        status = "success",
        commit_t = record.commit_t,
        "ledger info retrieved (simplified)"
    );
    Ok(Json(LedgerInfoResponse {
        ledger: record.address.clone(),
        t: record.commit_t,
        commit: record.commit_address.clone(),
        index: record.index_address.clone(),
    })
    .into_response())
}

/// Query parameters for ledger-info
#[derive(Deserialize)]
pub struct LedgerInfoQuery {
    pub ledger: Option<String>,
    /// When true, merge novelty deltas into property “details” (real-time).
    pub realtime_property_details: Option<bool>,
    /// When true, include `datatypes` under `stats.properties[*]` (indexed view by default).
    pub include_property_datatypes: Option<bool>,
}

/// Ledger exists response
#[derive(Serialize)]
pub struct ExistsResponse {
    /// Ledger alias (echoed back)
    pub ledger: String,
    /// Whether the ledger exists
    pub exists: bool,
}

/// Check if a ledger exists
///
/// GET /fluree/exists?ledger=<alias>
/// or with fluree-ledger header
///
/// Returns a simple boolean response indicating whether the ledger
/// is registered in the nameservice. This is a lightweight check
/// that does not load the ledger data.
pub async fn exists(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Json<ExistsResponse>> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger_exists",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
    );
    let _guard = span.enter();

    tracing::info!(status = "start", "ledger exists check requested");

    // Get ledger alias from query param or header
    let alias = match query
        .ledger
        .as_ref()
        .or(headers.ledger.as_ref())
        .ok_or(ServerError::MissingLedger)
    {
        Ok(alias) => {
            span.record("ledger_address", alias.as_str());
            alias.clone()
        }
        Err(e) => {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!(error = %e, "missing ledger alias in exists request");
            return Err(e);
        }
    };

    // Check if ledger exists via nameservice lookup
    let exists = match state.fluree.ledger_exists(&alias).await {
        Ok(exists) => exists,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(&span, "error:InternalError");
            tracing::error!(error = %server_error, "ledger exists check failed");
            return Err(server_error);
        }
    };

    tracing::info!(
        status = "success",
        exists = exists,
        "ledger exists check completed"
    );
    Ok(Json(ExistsResponse {
        ledger: alias,
        exists,
    }))
}

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
