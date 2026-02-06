use crate::context;
use crate::error::{CliError, CliResult};
use std::path::Path;

pub async fn run(
    explicit_ledger: Option<&str>,
    format_str: &str,
    at: Option<&str>,
    fluree_dir: &Path,
) -> CliResult<()> {
    let alias = context::resolve_ledger(explicit_ledger, fluree_dir)?;
    let fluree = context::build_fluree(fluree_dir)?;

    let graph = match at {
        Some(at_str) => {
            let spec = super::query::parse_time_spec(at_str);
            fluree.graph_at(&alias, spec)
        }
        None => fluree.graph(&alias),
    };

    let ledger = fluree.ledger(&alias).await?;

    match format_str.to_lowercase().as_str() {
        "jsonld" | "json-ld" | "json" => {
            // CONSTRUCT all triples as JSON-LD graph
            let result = graph
                .query()
                .sparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
                .execute()
                .await?;
            let json = result.to_construct(&ledger.db)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
            );
        }
        "turtle" | "ttl" => {
            // SELECT all triples and format as N-Triples
            let result = graph
                .query()
                .sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
                .execute()
                .await?;
            let json = result.to_sparql_json(&ledger.db)?;
            let output = format_ntriples(&json);
            print!("{output}");
        }
        other => {
            return Err(CliError::Usage(format!(
                "unknown export format '{other}'; valid formats: turtle, jsonld"
            )));
        }
    }

    Ok(())
}

/// Format SPARQL JSON bindings as N-Triples.
fn format_ntriples(json: &serde_json::Value) -> String {
    let bindings = match json.pointer("/results/bindings").and_then(|v| v.as_array()) {
        Some(b) => b,
        None => return String::new(),
    };

    let mut lines = Vec::new();
    for row in bindings {
        let s = extract_ntriples_term(row.get("s"));
        let p = extract_ntriples_term(row.get("p"));
        let o = extract_ntriples_term(row.get("o"));
        if !s.is_empty() && !p.is_empty() && !o.is_empty() {
            lines.push(format!("{s} {p} {o} ."));
        }
    }
    if !lines.is_empty() {
        lines.push(String::new()); // trailing newline
    }
    lines.join("\n")
}

/// Convert a SPARQL JSON binding value to N-Triples term syntax.
fn extract_ntriples_term(binding: Option<&serde_json::Value>) -> String {
    let b = match binding {
        Some(v) => v,
        None => return String::new(),
    };

    let value = b.get("value").and_then(|v| v.as_str()).unwrap_or("");
    let typ = b.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match typ {
        "uri" => format!("<{value}>"),
        "literal" => {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(lang) = b.get("xml:lang").and_then(|v| v.as_str()) {
                format!("\"{escaped}\"@{lang}")
            } else if let Some(dt) = b.get("datatype").and_then(|v| v.as_str()) {
                if dt == "http://www.w3.org/2001/XMLSchema#string" {
                    format!("\"{escaped}\"")
                } else {
                    format!("\"{escaped}\"^^<{dt}>")
                }
            } else {
                format!("\"{escaped}\"")
            }
        }
        "bnode" => format!("_:{value}"),
        _ => format!("<{value}>"),
    }
}
