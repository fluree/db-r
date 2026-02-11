//! Ledger management endpoints: /fluree/create, /fluree/drop, /fluree/ledger-info

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{ApiError, DropMode, DropReport, DropStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::Instrument;

/// Commit information in create response
#[derive(Serialize)]
pub struct CommitInfo {
    /// Commit content identifier (CID), None for genesis (t=0, no commit exists)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    /// Commit hash (SHA-256) - empty for genesis
    pub hash: String,
}

/// Create ledger response - matches Clojure server format
#[derive(Serialize)]
pub struct CreateResponse {
    /// Ledger identifier
    pub ledger_id: String,
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
        "ledger:create",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
        None, // no input format for ledger ops
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger creation requested");

        // Extract ledger alias from body
        let alias = match body
            .get("ledger")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ServerError::bad_request("Missing required field: ledger"))
        {
            Ok(alias) => {
                span.record("ledger_id", alias);
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
        let ledger_id = ledger.ledger_id().to_string();

        let response = CreateResponse {
            ledger_id: ledger_id.clone(),
            t: 0,
            tx_id,
            commit: CommitInfo {
                commit_id: None, // genesis has no commit
                hash: String::new(),
            },
        };

        tracing::info!(status = "success", "ledger created");
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
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
    /// Ledger identifier
    pub ledger_id: String,
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
            ledger_id: report.ledger_id,
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
        "ledger:drop",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

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
    .instrument(span)
    .await
}

/// Ledger info response (simplified, used in proxy storage mode fallback)
#[derive(Serialize)]
pub struct LedgerInfoResponse {
    /// Ledger identifier
    pub ledger_id: String,
    /// Current transaction time
    pub t: i64,
    /// Head commit ContentId (storage-agnostic identity), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_head_id: Option<fluree_db_core::ContentId>,
    /// Head index ContentId (storage-agnostic identity), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_head_id: Option<fluree_db_core::ContentId>,
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
    bearer: MaybeDataBearer,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Response> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:info",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger info requested");

        // Get ledger alias from query param or header
        let alias = match query
            .ledger
            .as_ref()
            .or(headers.ledger.as_ref())
            .ok_or(ServerError::MissingLedger)
        {
            Ok(alias) => {
                span.record("ledger_id", alias.as_str());
                alias
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias in info request");
                return Err(e);
            }
        };

        // Enforce data auth (ledger-info is a read operation; Bearer token only)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(alias) {
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // In proxy storage mode, return simplified nameservice-only response
        // (peer doesn't have local ledger state to compute full stats)
        if state.config.is_proxy_storage_mode() {
            return info_simplified(&state, alias, &span).await;
        }

        // Non-proxy mode: load ledger and return comprehensive info
        let ledger_state = super::query::load_ledger_for_query(&state, alias, &span).await?;

        let t = ledger_state.db.t;

        // Build comprehensive ledger info (at parity with Clojure)
        //
        // By default we return the optimized base payload. Callers can opt into
        // heavier/real-time property details via query params.
        let realtime_details = query.realtime_property_details.unwrap_or(false);
        let opts = fluree_db_api::ledger_info::LedgerInfoOptions {
            realtime_property_details: realtime_details,
            include_property_datatypes: query.include_property_datatypes.unwrap_or(false)
                || realtime_details,
        };
        let mut info = match &state.fluree {
            crate::state::FlureeInstance::File(f) => {
                fluree_db_api::ledger_info::build_ledger_info_with_options(
                    &ledger_state,
                    f.storage(),
                    None,
                    opts,
                )
                .await
            }
            crate::state::FlureeInstance::Proxy(p) => {
                fluree_db_api::ledger_info::build_ledger_info_with_options(
                    &ledger_state,
                    p.storage(),
                    None,
                    opts,
                )
                .await
            }
        }
        .map_err(|e| {
            set_span_error_code(&span, "error:InternalError");
            tracing::error!(error = %e, "failed to build ledger info");
            ServerError::internal(format!("Failed to build ledger info: {}", e))
        })?;

        if let Some(obj) = info.as_object_mut() {
            obj.insert(
                "ledger_id".to_string(),
                serde_json::Value::String(alias.to_string()),
            );
            obj.insert("t".to_string(), serde_json::Value::Number(t.into()));
        }

        tracing::info!(status = "success", "ledger info retrieved");
        Ok(Json(info).into_response())
    }
    .instrument(span)
    .await
}

/// Get ledger information with ledger as greedy tail segment.
///
/// GET /fluree/info/<ledger...>
pub async fn info_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(mut query): Query<LedgerInfoQuery>,
) -> Result<Response> {
    query.ledger = Some(ledger);
    info(State(state), headers, bearer, axum::extract::Query(query)).await
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
        ledger_id: record.ledger_id.clone(),
        t: record.commit_t,
        commit_head_id: record.commit_head_id.clone(),
        index_head_id: record.index_head_id.clone(),
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
    /// Ledger identifier (echoed back)
    pub ledger_id: String,
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
    bearer: MaybeDataBearer,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Json<ExistsResponse>> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:exists",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger exists check requested");

        // Get ledger alias from query param or header
        let alias = match query
            .ledger
            .as_ref()
            .or(headers.ledger.as_ref())
            .ok_or(ServerError::MissingLedger)
        {
            Ok(alias) => {
                span.record("ledger_id", alias.as_str());
                alias.clone()
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias in exists request");
                return Err(e);
            }
        };

        // Enforce data auth (exists is a read operation; Bearer token only)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&alias) {
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

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
            ledger_id: alias,
            exists,
        }))
    }
    .instrument(span)
    .await
}

/// Check ledger existence with ledger as greedy tail segment.
///
/// GET /fluree/exists/<ledger...>
pub async fn exists_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(mut query): Query<LedgerInfoQuery>,
) -> Result<Json<ExistsResponse>> {
    query.ledger = Some(ledger);
    exists(State(state), headers, bearer, axum::extract::Query(query)).await
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
