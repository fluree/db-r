use crate::detect::QueryFormat;
use crate::error::CliResult;
use comfy_table::{ContentArrangement, Table};

/// Output format for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormatKind {
    Json,
    Table,
    Csv,
}

/// Format a query result JSON value for display.
pub fn format_result(
    json: &serde_json::Value,
    format: OutputFormatKind,
    query_format: QueryFormat,
) -> CliResult<String> {
    match format {
        OutputFormatKind::Json => {
            Ok(serde_json::to_string_pretty(json).unwrap_or_else(|_| json.to_string()))
        }
        OutputFormatKind::Table => format_as_table(json, query_format),
        OutputFormatKind::Csv => format_as_csv(json, query_format),
    }
}

fn format_as_table(json: &serde_json::Value, query_format: QueryFormat) -> CliResult<String> {
    match query_format {
        QueryFormat::Sparql => format_sparql_table(json),
        QueryFormat::JsonLd => format_jsonld_table(json),
    }
}

fn format_as_csv(json: &serde_json::Value, query_format: QueryFormat) -> CliResult<String> {
    match query_format {
        QueryFormat::Sparql => format_sparql_csv(json),
        QueryFormat::JsonLd => format_jsonld_csv(json),
    }
}

/// Escape a value for CSV output.
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn format_sparql_table(json: &serde_json::Value) -> CliResult<String> {
    // SPARQL JSON results format:
    // { "head": {"vars": [...]}, "results": {"bindings": [{...}, ...]} }
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let vars = json
        .pointer("/head/vars")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if vars.is_empty() {
        return Ok(serde_json::to_string_pretty(json).unwrap_or_default());
    }

    table.set_header(&vars);

    let bindings = json.pointer("/results/bindings").and_then(|v| v.as_array());

    if let Some(rows) = bindings {
        for row in rows {
            let cells: Vec<String> = vars
                .iter()
                .map(|var| {
                    row.get(var)
                        .and_then(|b| b.get("value"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            table.add_row(cells);
        }
    }

    Ok(table.to_string())
}

fn format_jsonld_table(json: &serde_json::Value) -> CliResult<String> {
    // JSON-LD query results: array of objects
    let arr = match json.as_array() {
        Some(a) => a,
        None => return Ok(serde_json::to_string_pretty(json).unwrap_or_default()),
    };

    if arr.is_empty() {
        return Ok("(empty result set)".to_string());
    }

    // Collect all keys from all objects for column headers
    let mut columns: Vec<String> = Vec::new();
    for obj in arr {
        if let Some(map) = obj.as_object() {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(&columns);

    for obj in arr {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                obj.get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        table.add_row(cells);
    }

    Ok(table.to_string())
}

fn format_sparql_csv(json: &serde_json::Value) -> CliResult<String> {
    let vars = json
        .pointer("/head/vars")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if vars.is_empty() {
        return Ok(serde_json::to_string_pretty(json).unwrap_or_default());
    }

    let mut lines = Vec::new();
    lines.push(
        vars.iter()
            .map(|v| csv_escape(v))
            .collect::<Vec<_>>()
            .join(","),
    );

    let bindings = json.pointer("/results/bindings").and_then(|v| v.as_array());

    if let Some(rows) = bindings {
        for row in rows {
            let cells: Vec<String> = vars
                .iter()
                .map(|var| {
                    let val = row
                        .get(var)
                        .and_then(|b| b.get("value"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    csv_escape(val)
                })
                .collect();
            lines.push(cells.join(","));
        }
    }

    Ok(lines.join("\n"))
}

fn format_jsonld_csv(json: &serde_json::Value) -> CliResult<String> {
    let arr = match json.as_array() {
        Some(a) => a,
        None => return Ok(serde_json::to_string_pretty(json).unwrap_or_default()),
    };

    if arr.is_empty() {
        return Ok(String::new());
    }

    // Collect all keys
    let mut columns: Vec<String> = Vec::new();
    for obj in arr {
        if let Some(map) = obj.as_object() {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let mut lines = Vec::new();
    lines.push(
        columns
            .iter()
            .map(|c| csv_escape(c))
            .collect::<Vec<_>>()
            .join(","),
    );

    for obj in arr {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                let val = obj
                    .get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                csv_escape(&val)
            })
            .collect();
        lines.push(cells.join(","));
    }

    Ok(lines.join("\n"))
}
