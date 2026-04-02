//! Streaming RDF export endpoint: GET /v1/fluree/export/*ledger
//!
//! Leverages the `ExportBuilder` API for memory-efficient streaming export
//! from the binary SPOT index.

use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use fluree_db_api::export::ExportFormat;
use fluree_db_api::TimeSpec;
use std::sync::Arc;
use tracing::Instrument;

/// Query parameters for the export endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct ExportParams {
    /// Output format: turtle (default), ntriples, nquads, trig, jsonld
    #[serde(default = "default_format")]
    pub format: String,
    /// Export as-of a specific time: transaction number, ISO-8601 datetime, or commit CID prefix
    #[serde(rename = "at")]
    pub at: Option<String>,
    /// Export a specific named graph by IRI
    pub graph: Option<String>,
    /// Export all named graphs (dataset export)
    #[serde(default)]
    pub all_graphs: bool,
    /// JSON-LD context override (JSON object as string)
    pub context: Option<String>,
}

fn default_format() -> String {
    "turtle".to_string()
}

fn parse_format(s: &str) -> Result<ExportFormat> {
    match s.to_lowercase().as_str() {
        "turtle" | "ttl" => Ok(ExportFormat::Turtle),
        "ntriples" | "nt" => Ok(ExportFormat::NTriples),
        "nquads" | "nq" => Ok(ExportFormat::NQuads),
        "trig" => Ok(ExportFormat::TriG),
        "jsonld" | "json-ld" => Ok(ExportFormat::JsonLd),
        _ => Err(ServerError::BadRequest(format!(
            "unsupported export format '{}'; use turtle, ntriples, nquads, trig, or jsonld",
            s
        ))),
    }
}

fn content_type_for(format: &ExportFormat) -> &'static str {
    match format {
        ExportFormat::Turtle => "text/turtle; charset=utf-8",
        ExportFormat::NTriples => "application/n-triples; charset=utf-8",
        ExportFormat::NQuads => "application/n-quads; charset=utf-8",
        ExportFormat::TriG => "application/trig; charset=utf-8",
        ExportFormat::JsonLd => "application/ld+json; charset=utf-8",
    }
}

fn parse_time_spec(s: &str) -> TimeSpec {
    // Try parsing as integer (transaction number)
    if let Ok(t) = s.parse::<i64>() {
        return TimeSpec::AtT(t);
    }
    // If it looks like a hex string (commit CID prefix), treat as commit
    if s.len() >= 6 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        return TimeSpec::AtCommit(s.to_string());
    }
    // Otherwise treat as ISO-8601 datetime
    TimeSpec::AtTime(s.to_string())
}

/// GET /v1/fluree/export/*ledger
///
/// Stream RDF data from a ledger's binary index.
///
/// Query parameters:
/// - `format`: turtle (default), ntriples, nquads, trig, jsonld
/// - `at`: time-travel to a specific point (transaction number, ISO-8601, commit CID prefix)
/// - `graph`: export a specific named graph by IRI
/// - `all_graphs`: export all named graphs (requires trig or nquads format)
/// - `context`: JSON-LD context override (JSON object string)
pub async fn export(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<ExportParams>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "export",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    async move {
        // Enforce data auth (read operation)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let format = parse_format(&params.format)?;
        let content_type = content_type_for(&format);

        let mut builder = state.fluree.export(&ledger).format(format);

        if params.all_graphs {
            builder = builder.all_graphs();
        }
        if let Some(ref graph_iri) = params.graph {
            builder = builder.graph(graph_iri);
        }
        if let Some(ref at) = params.at {
            builder = builder.as_of(parse_time_spec(at));
        }
        if let Some(ref ctx_str) = params.context {
            let ctx: serde_json::Value = serde_json::from_str(ctx_str)
                .map_err(|e| ServerError::BadRequest(format!("invalid context JSON: {e}")))?;
            builder = builder.context(&ctx);
        }

        // Export to an in-memory buffer (streaming to HTTP response body would
        // require ExportBuilder changes to support async Write; for now, buffer
        // is acceptable since export is a batch operation, not a hot path).
        let mut buf = Vec::new();
        let stats = builder.write_to(&mut buf).await.map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            triples = stats.triples_written,
            skipped = stats.rows_skipped,
            format = params.format,
            "export complete"
        );

        Ok((
            [
                (header::CONTENT_TYPE, content_type),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"export\"",
                ),
            ],
            buf,
        )
            .into_response())
    }
    .instrument(span)
    .await
}
