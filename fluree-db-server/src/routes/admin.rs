//! Admin endpoints: /health, /fluree/stats, /swagger.json

use crate::error::Result;
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

/// Health check endpoint
///
/// GET /health
///
/// Returns a simple health check response to verify the server is running.
pub async fn health() -> Json<HealthResponse> {
    // Simple health check - no complex span needed
    tracing::debug!("health check requested");
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Server statistics response
#[derive(Serialize)]
pub struct StatsResponse {
    /// Server uptime in seconds
    pub uptime_secs: u64,
    /// Storage type (memory or file)
    pub storage_type: &'static str,
    /// Whether indexing is enabled
    pub indexing_enabled: bool,
    /// Number of cached ledgers
    pub cached_ledgers: usize,
    /// Server version
    pub version: &'static str,
}

/// Server statistics endpoint
///
/// GET /fluree/stats
///
/// Returns server statistics including uptime, storage type, and cache info.
pub async fn stats(
    State(state): State<Arc<AppState>>,
    _headers: FlureeHeaders,
) -> Json<StatsResponse> {
    let span = tracing::info_span!("stats");
    let _guard = span.enter();

    tracing::info!("server stats requested");

    Json(StatsResponse {
        uptime_secs: state.uptime_secs(),
        storage_type: state.config.storage_type_str(),
        indexing_enabled: state.config.indexing_enabled,
        cached_ledgers: state.fluree.cached_ledger_count().await,
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// OpenAPI specification endpoint
///
/// GET /swagger.json
///
/// Returns the OpenAPI specification for the Fluree server API.
/// TODO: Generate from utoipa annotations
pub async fn openapi_spec() -> Result<Json<serde_json::Value>> {
    // Minimal OpenAPI spec - will be expanded with utoipa
    let spec = serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Fluree DB Server",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "HTTP REST API for Fluree DB"
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {
                        "200": {
                            "description": "Server is healthy"
                        }
                    }
                }
            },
            "/fluree/create": {
                "post": {
                    "summary": "Create a new ledger",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "ledger": {
                                            "type": "string",
                                            "description": "Ledger alias"
                                        }
                                    },
                                    "required": ["ledger"]
                                }
                            }
                        }
                    }
                }
            },
            "/fluree/query": {
                "post": {
                    "summary": "Execute a query",
                    "description": "Execute FQL or SPARQL queries"
                }
            },
            "/fluree/transact": {
                "post": {
                    "summary": "Execute a transaction"
                }
            }
        }
    });

    Ok(Json(spec))
}
