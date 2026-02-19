//! Query result formatting
//!
//! This module provides formatters for converting `QueryResult` to various output formats:
//!
//! **JSON formats** (produce `serde_json::Value`):
//! - **JSON-LD Query** (`OutputFormat::JsonLd`): Simple JSON with compact IRIs
//! - **SPARQL JSON** (`OutputFormat::SparqlJson`): W3C SPARQL 1.1 Query Results JSON
//! - **TypedJson** (`OutputFormat::TypedJson`): Always includes explicit datatypes
//!
//! **Delimited-text formats** (produce `String` / `Vec<u8>` directly):
//! - **TSV** (`OutputFormat::Tsv`): Tab-separated values
//! - **CSV** (`OutputFormat::Csv`): Comma-separated values (RFC 4180)
//!
//! # Sync vs Async Formatting
//!
//! Most queries can use the synchronous `format_results()` function. However, **graph crawl
//! queries** require async database access for property expansion and must use
//! `format_results_async()` instead.
//!
//! # Delimited-Text Fast Path
//!
//! TSV and CSV bypass JSON DOM construction and JSON serialization.
//! Use `format_results_string()` or `QueryResult::to_tsv()` / `to_csv()` —
//! **not** `format_results()` (which returns `JsonValue`).
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::format::{format_results, format_results_async, FormatterConfig};
//!
//! // For regular SELECT queries (sync)
//! let json = format_results(&result, &parsed.context, &ledger.db, &FormatterConfig::jsonld())?;
//!
//! // For graph crawl queries (async)
//! let json = format_results_async(&result, &parsed.context, &ledger.db, &FormatterConfig::jsonld()).await?;
//!
//! // For TSV/CSV (high-performance)
//! let tsv = result.to_tsv(&ledger.db)?;
//! let csv = result.to_csv(&ledger.db)?;
//! ```

pub mod config;
mod construct;
pub mod datatype;
pub mod delimited;
mod graph_crawl;
pub mod iri;
mod jsonld;
mod materialize;
mod sparql;
mod typed;

pub use config::{FormatterConfig, JsonLdRowShape, OutputFormat, SelectMode};
pub use iri::IriCompactor;

use crate::QueryResult;
use fluree_db_core::Db;
use fluree_db_core::{FuelExceededError, Tracker};
use fluree_graph_json_ld::ParsedContext;
use serde_json::Value as JsonValue;

/// Error type for formatting operations
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// JSON serialization error
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Unknown namespace code (Sid could not be decoded to IRI)
    #[error("Unknown namespace code: {0}")]
    UnknownNamespace(u16),

    /// Invalid binding state encountered
    #[error("Invalid binding state: {0}")]
    InvalidBinding(String),

    /// Fuel limit exceeded during formatting (graph crawl)
    #[error(transparent)]
    FuelExceeded(#[from] FuelExceededError),
}

/// Result type for formatting operations
pub type Result<T> = std::result::Result<T, FormatError>;

/// Format query results to JSON using the specified configuration
///
/// This is the main entry point for formatting. It dispatches to the appropriate
/// formatter based on `config.format`.
///
/// # Arguments
///
/// * `result` - Query result to format
/// * `context` - Parsed @context from the query (for IRI compaction)
/// * `db` - Database (for namespace code lookup)
/// * `config` - Formatting configuration
///
/// # Returns
///
/// A `serde_json::Value` containing the formatted results
pub fn format_results(
    result: &QueryResult,
    context: &ParsedContext,
    db: &Db,
    config: &FormatterConfig,
) -> Result<JsonValue> {
    // Delimited-text formats produce bytes/String, not JsonValue. Reject early.
    if matches!(config.format, OutputFormat::Tsv | OutputFormat::Csv) {
        return Err(FormatError::InvalidBinding(format!(
            "{:?} format produces bytes/String, not JsonValue. \
             Use format_results_string() or QueryResult::to_tsv()/to_csv() instead.",
            config.format
        )));
    }

    let compactor = IriCompactor::new(db.namespaces(), context);

    // CONSTRUCT queries have dedicated output format
    // Guard on BOTH config.select_mode AND result.select_mode for safety
    if config.select_mode == SelectMode::Construct {
        // Validate that result is actually a CONSTRUCT query
        if result.select_mode != SelectMode::Construct || result.construct_template.is_none() {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT format requested but query is not CONSTRUCT".to_string(),
            ));
        }
        // Only JSON-LD makes sense for CONSTRUCT
        if config.format != OutputFormat::JsonLd {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT queries only support JSON-LD output format".to_string(),
            ));
        }
        return construct::format(result, &compactor);
    }

    // Graph crawl queries require async formatting for database access
    if result.graph_select.is_some() {
        return Err(FormatError::InvalidBinding(
            "Graph crawl queries require async database access for property expansion. \
             Use format_results_async() instead of format_results()."
                .to_string(),
        ));
    }

    // SELECT query dispatch
    match config.format {
        OutputFormat::JsonLd => jsonld::format(result, &compactor, config),
        OutputFormat::SparqlJson => sparql::format(result, &compactor, config),
        OutputFormat::TypedJson => typed::format(result, &compactor, config),
        OutputFormat::Tsv | OutputFormat::Csv => {
            unreachable!("Delimited formats rejected before dispatch")
        }
    }
}

/// Format query results to a JSON string
///
/// Convenience function that formats and serializes in one step.
/// Respects `config.pretty` for formatting.
///
/// Note: For graph crawl queries, use `format_results_string_async()` instead.
pub fn format_results_string(
    result: &QueryResult,
    context: &ParsedContext,
    db: &Db,
    config: &FormatterConfig,
) -> Result<String> {
    // Delimited-text fast-path: skip JSON DOM and JSON serialization entirely
    match config.format {
        OutputFormat::Tsv => return delimited::format_tsv(result, db),
        OutputFormat::Csv => return delimited::format_csv(result, db),
        _ => {}
    }

    let value = format_results(result, context, db, config)?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

// ============================================================================
// Async formatting (required for graph crawl)
// ============================================================================

/// Format query results to JSON using async database access
///
/// This is the async entry point for formatting. It supports all query types including
/// **graph crawl queries** which require database access during formatting for property
/// expansion.
///
/// For non-graph-crawl queries, this delegates to the sync formatters internally.
///
/// # Arguments
///
/// * `result` - Query result to format
/// * `context` - Parsed @context from the query (for IRI compaction)
/// * `db` - Database (for namespace code lookup and property fetching)
/// * `config` - Formatting configuration
///
/// # Returns
///
/// A `serde_json::Value` containing the formatted results
///
/// # Policy Support
///
/// When `policy` is `Some`, graph crawl queries filter flakes according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead for the common case).
pub async fn format_results_async(
    result: &QueryResult,
    context: &ParsedContext,
    db: &Db,
    config: &FormatterConfig,
    policy: Option<&fluree_db_policy::PolicyContext>,
    tracker: Option<&Tracker>,
) -> Result<JsonValue> {
    // Delimited-text formats produce bytes/String, not JsonValue. Reject early.
    if matches!(config.format, OutputFormat::Tsv | OutputFormat::Csv) {
        return Err(FormatError::InvalidBinding(format!(
            "{:?} format produces bytes/String, not JsonValue. \
             Use format_results_string() or QueryResult::to_tsv()/to_csv() instead.",
            config.format
        )));
    }

    let compactor = IriCompactor::new(db.namespaces(), context);

    // CONSTRUCT queries have dedicated output format (sync, no DB access needed)
    if config.select_mode == SelectMode::Construct {
        if result.select_mode != SelectMode::Construct || result.construct_template.is_none() {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT format requested but query is not CONSTRUCT".to_string(),
            ));
        }
        if config.format != OutputFormat::JsonLd {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT queries only support JSON-LD output format".to_string(),
            ));
        }
        return construct::format(result, &compactor);
    }

    // Graph crawl queries use async formatter with DB access
    if result.graph_select.is_some() {
        if config.format != OutputFormat::JsonLd {
            return Err(FormatError::InvalidBinding(
                "Graph crawl select only supports JSON-LD output format".to_string(),
            ));
        }
        let v = graph_crawl::format_async(result, db, &compactor, config, policy, tracker).await?;
        // Graph crawl formatter returns an array of rows; honor selectOne by
        // returning the first row (or null if empty).
        return match config.select_mode {
            SelectMode::One => match v {
                JsonValue::Array(mut rows) => Ok(rows.drain(..).next().unwrap_or(JsonValue::Null)),
                other => Ok(other),
            },
            _ => Ok(v),
        };
    }

    // SELECT query dispatch (sync formatters)
    match config.format {
        OutputFormat::JsonLd => jsonld::format(result, &compactor, config),
        OutputFormat::SparqlJson => sparql::format(result, &compactor, config),
        OutputFormat::TypedJson => typed::format(result, &compactor, config),
        OutputFormat::Tsv | OutputFormat::Csv => {
            unreachable!("Delimited formats rejected before dispatch")
        }
    }
}

/// Format query results to a JSON string using async database access
///
/// Async convenience function that formats and serializes in one step.
/// Respects `config.pretty` for formatting.
///
/// Required for graph crawl queries. For other queries, can use sync version.
///
/// # Policy Support
///
/// When `policy` is `Some`, graph crawl queries filter flakes according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead).
pub async fn format_results_string_async(
    result: &QueryResult,
    context: &ParsedContext,
    db: &Db,
    config: &FormatterConfig,
    policy: Option<&fluree_db_policy::PolicyContext>,
) -> Result<String> {
    // Delimited-text fast-path: skip JSON DOM and JSON serialization entirely
    match config.format {
        OutputFormat::Tsv => return delimited::format_tsv(result, db),
        OutputFormat::Csv => return delimited::format_csv(result, db),
        _ => {}
    }

    let value = format_results_async(result, context, db, config, policy, None).await?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}
