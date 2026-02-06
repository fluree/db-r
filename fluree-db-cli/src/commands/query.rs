use crate::commands::insert::resolve_positional_args;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use crate::output::{self, OutputFormatKind};
use std::path::Path;

/// Parse a `--at` value into a `TimeSpec`.
///
/// Accepts:
/// - Integer → `TimeSpec::AtT(n)`
/// - ISO-8601 datetime string (contains `-` and `:`) → `TimeSpec::AtTime(s)`
/// - Otherwise → `TimeSpec::AtCommit(s)` (commit hash prefix)
pub fn parse_time_spec(at: &str) -> fluree_db_api::TimeSpec {
    if let Ok(t) = at.parse::<i64>() {
        fluree_db_api::TimeSpec::at_t(t)
    } else if at.contains('-') && at.contains(':') {
        // Looks like ISO-8601 timestamp (e.g., "2024-01-15T10:30:00Z")
        fluree_db_api::TimeSpec::at_time(at.to_string())
    } else {
        // Treat as commit hash prefix
        fluree_db_api::TimeSpec::at_commit(at.to_string())
    }
}

pub async fn run(
    args: &[String],
    expr: Option<&str>,
    format_str: &str,
    sparql_flag: bool,
    fql_flag: bool,
    at: Option<&str>,
    fluree_dir: &Path,
) -> CliResult<()> {
    let (explicit_ledger, file_path) = resolve_positional_args(args);
    let alias = context::resolve_ledger(explicit_ledger, fluree_dir)?;
    let fluree = context::build_fluree(fluree_dir)?;

    // Resolve input
    let source = input::resolve_input(file_path.as_deref(), expr)?;
    let content = input::read_input(&source)?;

    // Detect query format
    let query_format =
        detect::detect_query_format(file_path.as_deref(), &content, sparql_flag, fql_flag)?;

    // Parse output format
    let output_format = match format_str.to_lowercase().as_str() {
        "json" => OutputFormatKind::Json,
        "table" => OutputFormatKind::Table,
        "csv" => OutputFormatKind::Csv,
        other => {
            return Err(CliError::Usage(format!(
                "unknown output format '{other}'; valid formats: json, table, csv"
            )));
        }
    };

    // Build graph (with optional time travel)
    let graph = match at {
        Some(at_str) => {
            let spec = parse_time_spec(at_str);
            fluree.graph_at(&alias, spec)
        }
        None => fluree.graph(&alias),
    };

    // Execute query
    let ledger = fluree.ledger(&alias).await?;

    let (result, formatted_json) = match query_format {
        detect::QueryFormat::Sparql => {
            let result = graph.query().sparql(&content).execute().await?;
            let json = result.to_sparql_json(&ledger.db)?;
            (query_format, json)
        }
        detect::QueryFormat::Fql => {
            let json_query: serde_json::Value = serde_json::from_str(&content)?;
            let result = graph.query().jsonld(&json_query).execute().await?;
            let json = result.to_jsonld(&ledger.db)?;
            (query_format, json)
        }
    };

    // Format and print
    let output = output::format_result(&formatted_json, output_format, result)?;
    println!("{output}");

    Ok(())
}
