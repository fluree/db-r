//! Graph source lifecycle endpoints: BM25 full-text index management.
//!
//! These endpoints allow creating, syncing, checking, and dropping BM25
//! full-text indexes via the HTTP API. All are admin-auth protected.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ===== BM25 Create =====

#[derive(Deserialize)]
pub struct Bm25CreateRequest {
    /// Name for the graph source (e.g., "my-search")
    pub name: String,
    /// Source ledger alias (e.g., "docs:main")
    pub ledger: String,
    /// Indexing query defining what to index (must include @id in select)
    pub query: serde_json::Value,
    /// Optional branch name (defaults to "main")
    pub branch: Option<String>,
    /// BM25 k1 parameter (term frequency saturation). Default: 1.2
    pub k1: Option<f64>,
    /// BM25 b parameter (document length normalization). Default: 0.75
    pub b: Option<f64>,
}

#[derive(Serialize)]
pub struct Bm25CreateResponse {
    pub graph_source_id: String,
    pub doc_count: usize,
    pub term_count: usize,
    pub index_t: i64,
}

/// POST /v1/fluree/graph-source/bm25/create
pub async fn bm25_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Bm25CreateRequest>,
) -> Result<Json<Bm25CreateResponse>> {
    super::admin::check_maintenance(&state)?;
    let mut config = fluree_db_api::Bm25CreateConfig::new(req.name, req.ledger, req.query);
    if let Some(branch) = req.branch {
        config = config.with_branch(branch);
    }
    if let Some(k1) = req.k1 {
        config = config.with_k1(k1);
    }
    if let Some(b) = req.b {
        config = config.with_b(b);
    }

    let result = state
        .fluree
        .create_full_text_index(config)
        .await
        .map_err(ServerError::Api)?;

    Ok(Json(Bm25CreateResponse {
        graph_source_id: result.graph_source_id,
        doc_count: result.doc_count,
        term_count: result.term_count,
        index_t: result.index_t,
    }))
}

// ===== BM25 Sync =====

#[derive(Deserialize)]
pub struct Bm25GraphSourceRequest {
    /// Graph source ID (e.g., "my-search:main")
    pub graph_source_id: String,
}

#[derive(Serialize)]
pub struct Bm25SyncResponse {
    pub graph_source_id: String,
    pub upserted: usize,
    pub removed: usize,
    pub old_watermark: i64,
    pub new_watermark: i64,
    pub was_full_resync: bool,
}

/// POST /v1/fluree/graph-source/bm25/sync
pub async fn bm25_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Bm25GraphSourceRequest>,
) -> Result<Json<Bm25SyncResponse>> {
    super::admin::check_maintenance(&state)?;
    let result = state
        .fluree
        .sync_bm25_index(&req.graph_source_id)
        .await
        .map_err(ServerError::Api)?;

    Ok(Json(Bm25SyncResponse {
        graph_source_id: result.graph_source_id,
        upserted: result.upserted,
        removed: result.removed,
        old_watermark: result.old_watermark,
        new_watermark: result.new_watermark,
        was_full_resync: result.was_full_resync,
    }))
}

// ===== BM25 Status =====

#[derive(Serialize)]
pub struct Bm25StatusResponse {
    pub graph_source_id: String,
    pub source_ledger: String,
    pub index_t: i64,
    pub ledger_t: i64,
    pub is_stale: bool,
    pub lag: i64,
}

/// POST /v1/fluree/graph-source/bm25/status
pub async fn bm25_status(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Bm25GraphSourceRequest>,
) -> Result<Json<Bm25StatusResponse>> {
    let result = state
        .fluree
        .check_bm25_staleness(&req.graph_source_id)
        .await
        .map_err(ServerError::Api)?;

    Ok(Json(Bm25StatusResponse {
        graph_source_id: result.graph_source_id,
        source_ledger: result.source_ledger,
        index_t: result.index_t,
        ledger_t: result.ledger_t,
        is_stale: result.is_stale,
        lag: result.lag,
    }))
}

// ===== BM25 Drop =====

#[derive(Serialize)]
pub struct Bm25DropResponse {
    pub graph_source_id: String,
    pub deleted_snapshots: usize,
    pub was_already_retracted: bool,
}

/// POST /v1/fluree/graph-source/bm25/drop
pub async fn bm25_drop(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Bm25GraphSourceRequest>,
) -> Result<Json<Bm25DropResponse>> {
    super::admin::check_maintenance(&state)?;
    let result = state
        .fluree
        .drop_full_text_index(&req.graph_source_id)
        .await
        .map_err(ServerError::Api)?;

    Ok(Json(Bm25DropResponse {
        graph_source_id: result.graph_source_id,
        deleted_snapshots: result.deleted_snapshots,
        was_already_retracted: result.was_already_retracted,
    }))
}
