//! Bulk import endpoint.
//!
//! POST /v1/fluree/import — accepts Turtle, TriG, or JSON-LD data and runs
//! the import pipeline for efficient bulk loading. Admin-auth protected.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ImportParams {
    /// Target ledger (e.g., "mydb" or "mydb:main").
    pub ledger: String,
    /// Format override: "ttl", "trig", or "jsonld".
    pub format: Option<String>,
}

#[derive(Serialize)]
pub struct ImportResponse {
    pub ledger_id: String,
    pub t: i64,
    pub flake_count: u64,
}

/// POST /v1/fluree/import
pub async fn import(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
) -> Response {
    // Extract query parameters from the URI before consuming the request
    let params: ImportParams = match axum::extract::Query::try_from_uri(request.uri()) {
        Ok(axum::extract::Query(p)) => p,
        Err(e) => {
            return ServerError::bad_request(format!("Invalid query params: {}", e)).into_response()
        }
    };
    match import_inner(&state, params, request).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn import_inner(
    state: &AppState,
    params: ImportParams,
    request: axum::extract::Request,
) -> Result<ImportResponse> {
    super::admin::check_maintenance(state)?;

    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body = axum::body::to_bytes(request.into_body(), state.config.body_limit)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read request body: {}", e)))?;

    if body.is_empty() {
        return Err(ServerError::bad_request("Request body is empty"));
    }

    // Determine file extension from format param or content-type
    let extension = if let Some(ref fmt) = params.format {
        match fmt.as_str() {
            "ttl" | "turtle" => "ttl",
            "trig" => "trig",
            "jsonld" | "json-ld" => "jsonld",
            _ => {
                return Err(ServerError::bad_request(format!(
                    "Unknown format '{}'. Use: ttl, trig, or jsonld",
                    fmt
                )))
            }
        }
    } else if content_type.contains("trig") {
        "trig"
    } else if content_type.contains("json") {
        "jsonld"
    } else {
        "ttl"
    };

    // Write body to temp file for the import pipeline
    let tmp_dir = std::env::temp_dir().join("fluree_import");
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| ServerError::internal(format!("Failed to create temp directory: {}", e)))?;
    let tmp_path = tmp_dir.join(format!("import_{}.{}", uuid_simple(), extension,));
    std::fs::write(&tmp_path, &body)
        .map_err(|e| ServerError::internal(format!("Failed to write temp file: {}", e)))?;

    let fluree = state.fluree.clone();
    let ledger = params.ledger.clone();
    let import_path = tmp_path.clone();

    // Run import pipeline on a blocking thread — the import pipeline uses
    // crossbeam channels internally which aren't Send-compatible with async.
    let result = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { fluree.create(&ledger).import(&import_path).execute().await })
    })
    .await
    .map_err(|e| ServerError::internal(format!("Import task panicked: {}", e)))?;

    // Clean up temp file (best-effort, regardless of result)
    let _ = std::fs::remove_file(&tmp_path);

    let result =
        result.map_err(|e| ServerError::Api(fluree_db_api::ApiError::internal(e.to_string())))?;

    tracing::info!(
        ledger = %result.ledger_id,
        t = result.t,
        flakes = result.flake_count,
        "Bulk import complete"
    );

    Ok(ImportResponse {
        ledger_id: result.ledger_id,
        t: result.t,
        flake_count: result.flake_count,
    })
}

/// Simple unique ID for temp files (avoid collisions on concurrent imports).
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}
