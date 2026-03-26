//! AgentJson format — optimized for LLM/agent consumption
//!
//! Produces a self-describing envelope with:
//! - **schema**: per-variable datatype (extracted in a single pass)
//! - **rows**: compact JSON objects with native types
//! - **rowCount**: number of rows included
//! - **t** / **iso**: time-pinning metadata
//! - **hasMore** / **message** / **resume**: pagination when truncated

use std::collections::{BTreeSet, HashMap};

use serde_json::{json, Map, Value as JsonValue};

use super::iri::IriCompactor;
use super::Result;
use crate::QueryResult;
use fluree_db_query::binding::Binding;
use fluree_db_query::VarId;

/// Format query results as an AgentJson envelope
pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &super::config::FormatterConfig,
) -> Result<JsonValue> {
    let select_vars = if result.output.is_wildcard() {
        None
    } else {
        Some(result.output.select_vars_or_empty())
    };

    let max_bytes = config.max_bytes;
    let total_row_hint = result.batches.iter().map(|b| b.len()).sum::<usize>();
    let mut rows = Vec::with_capacity(if max_bytes.is_some() {
        total_row_hint.min(256) // don't over-allocate when truncating
    } else {
        total_row_hint
    });
    let mut type_map: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut cumulative_bytes: usize = 0;
    let mut has_more = false;
    let mut total_rows: usize = 0;
    // Scratch buffer for byte measurement — reused across rows to avoid allocation
    let mut size_buf: Vec<u8> = if max_bytes.is_some() {
        Vec::with_capacity(512)
    } else {
        Vec::new()
    };

    'outer: for batch in &result.batches {
        for row_idx in 0..batch.len() {
            total_rows += 1;

            let vars_to_scan = select_vars.unwrap_or_else(|| batch.schema());

            // Single pass: format row values AND extract types simultaneously
            let row = format_row_with_types(
                result,
                batch,
                row_idx,
                vars_to_scan,
                &result.vars,
                compactor,
                &mut type_map,
            )?;

            // Check byte budget using scratch buffer
            if let Some(budget) = max_bytes {
                size_buf.clear();
                if serde_json::to_writer(&mut size_buf, &row).is_ok() {
                    if cumulative_bytes + size_buf.len() > budget && !rows.is_empty() {
                        has_more = true;
                        break 'outer;
                    }
                    cumulative_bytes += size_buf.len();
                }
            }

            rows.push(row);
        }
    }

    let row_count = rows.len();

    // Build schema from collected types
    let schema = build_schema(&type_map, &result.vars);

    // Build envelope
    let mut envelope = Map::new();
    envelope.insert("schema".to_string(), schema);
    envelope.insert("rows".to_string(), JsonValue::Array(rows));
    envelope.insert("rowCount".to_string(), json!(row_count));

    // Time-pinning metadata
    // `t` is only present for single-ledger queries (Option<i64>)
    if let Some(t) = result.t {
        let include_t = match config.agent_json_context {
            Some(ref ctx) => ctx.from_count <= 1,
            None => true, // no context: single-ledger assumed
        };
        if include_t {
            envelope.insert("t".to_string(), json!(t));
        }
    }
    if let Some(ref ctx) = config.agent_json_context {
        if let Some(ref iso) = ctx.iso_timestamp {
            envelope.insert("iso".to_string(), JsonValue::String(iso.clone()));
        }
    }

    envelope.insert("hasMore".to_string(), JsonValue::Bool(has_more));

    if has_more {
        let budget_str = max_bytes.map(|b| b.to_string()).unwrap_or_default();
        let mut msg = format!(
            "Response truncated due to size limit of {} bytes. {} of {} rows included.",
            budget_str, row_count, total_rows
        );

        if let Some(ref ctx) = config.agent_json_context {
            if ctx.from_count <= 1 {
                if let (Some(ref sparql), Some(t)) = (&ctx.sparql_text, result.t) {
                    if let Some(resume) = generate_resume_query(sparql, t, row_count) {
                        msg = format!(
                            "Response truncated due to size limit of {} bytes. \
                             Use the query below to retrieve the next batch.",
                            budget_str
                        );
                        envelope.insert("resume".to_string(), JsonValue::String(resume));
                    }
                }
            } else {
                // Multi-ledger: advise using @iso: for time-pinning
                if let Some(ref iso) = ctx.iso_timestamp {
                    msg.push_str(&format!(
                        " To retrieve the next batch, re-issue your query with \
                         @iso:{} on each FROM clause and add OFFSET {} LIMIT 100.",
                        iso, row_count
                    ));
                }
            }
        }

        envelope.insert("message".to_string(), JsonValue::String(msg));
    }

    Ok(JsonValue::Object(envelope))
}

/// Format a single row as a JSON object AND extract type info in one pass.
///
/// Each binding is visited exactly once: its value is formatted and its datatype
/// is recorded in `type_map`.
fn format_row_with_types(
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    vars: &[VarId],
    registry: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
    type_map: &mut HashMap<VarId, BTreeSet<String>>,
) -> Result<JsonValue> {
    let mut obj = Map::new();

    for &var_id in vars {
        let var_name = registry.name(var_id);

        // Skip internal variables
        if var_name.starts_with("?__") {
            continue;
        }

        let binding = match batch.get(row_idx, var_id) {
            Some(b) => b,
            None => {
                obj.insert(var_name.to_string(), JsonValue::Null);
                continue;
            }
        };

        if matches!(binding, Binding::Unbound | Binding::Poisoned) {
            obj.insert(var_name.to_string(), JsonValue::Null);
            continue;
        }

        // Handle encoded bindings: materialize once, then format + extract type
        let (value, type_label) = if binding.is_encoded() {
            let materialized = super::materialize::materialize_binding(result, binding)?;
            let val = super::jsonld::format_binding_with_result(result, &materialized, compactor)?;
            let tl = binding_type_label(&materialized, compactor)?;
            (val, tl)
        } else {
            let val = super::jsonld::format_binding_with_result(result, binding, compactor)?;
            let tl = binding_type_label(binding, compactor)?;
            (val, tl)
        };

        obj.insert(var_name.to_string(), value);

        if let Some(label) = type_label {
            type_map.entry(var_id).or_default().insert(label);
        }
    }

    Ok(JsonValue::Object(obj))
}

/// Extract the compact datatype label from a (non-encoded) binding.
///
/// Returns `None` for Unbound/Poisoned (caller already handles those).
fn binding_type_label(binding: &Binding, compactor: &IriCompactor) -> Result<Option<String>> {
    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(None),
        Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) => Ok(Some("uri".to_string())),
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => Ok(Some("uri".to_string())),
        Binding::Lit { dtc, .. } => {
            if dtc.lang_tag().is_some() {
                Ok(Some("rdf:langString".to_string()))
            } else {
                Ok(Some(compactor.compact_sid(dtc.datatype())?))
            }
        }
        Binding::EncodedLit { .. } => Ok(None), // shouldn't reach here after materialization
        Binding::Grouped(_) => Ok(Some("grouped".to_string())),
    }
}

/// Build the schema JSON from collected type information
fn build_schema(
    type_map: &HashMap<VarId, BTreeSet<String>>,
    vars: &fluree_db_query::VarRegistry,
) -> JsonValue {
    let mut schema = Map::new();

    // Sort by variable name for deterministic output
    let mut entries: Vec<_> = type_map.iter().collect();
    entries.sort_by_key(|(vid, _)| vars.name(**vid).to_string());

    for (var_id, types) in entries {
        let var_name = vars.name(*var_id);
        if var_name.starts_with("?__") {
            continue;
        }
        let type_val = if types.len() == 1 {
            JsonValue::String(types.iter().next().unwrap().clone())
        } else {
            JsonValue::Array(types.iter().map(|t| JsonValue::String(t.clone())).collect())
        };
        schema.insert(var_name.to_string(), type_val);
    }

    JsonValue::Object(schema)
}

/// Generate a resume SPARQL query for single-FROM pagination
///
/// Rewrites the original SPARQL to pin time with `@t:` and add OFFSET/LIMIT.
/// Returns `None` if the query has zero or multiple FROM clauses.
fn generate_resume_query(sparql: &str, t: i64, row_count: usize) -> Option<String> {
    // Find all FROM <...> occurrences (case insensitive)
    let lower = sparql.to_lowercase();
    let from_positions: Vec<usize> = lower
        .match_indices("from")
        .filter(|(pos, _)| {
            // Must be followed by whitespace then '<' (not "from named")
            let rest = &lower[pos + 4..];
            let trimmed = rest.trim_start();
            trimmed.starts_with('<') && !rest.trim_start().starts_with("named")
        })
        .map(|(pos, _)| pos)
        .collect();

    if from_positions.len() != 1 {
        return None;
    }

    let from_pos = from_positions[0];

    // Find the angle-bracket IRI
    let open = sparql[from_pos..].find('<')? + from_pos;
    let close = sparql[open..].find('>')? + open;
    let iri = &sparql[open + 1..close];

    // Pin with @t: (replace existing time-travel suffix if present)
    let base_iri = if let Some(at_pos) = iri.rfind('@') {
        &iri[..at_pos]
    } else {
        iri
    };
    let pinned_iri = format!("{}@t:{}", base_iri, t);

    // Rebuild query with pinned IRI
    let mut result = String::with_capacity(sparql.len() + 32);
    result.push_str(&sparql[..open + 1]);
    result.push_str(&pinned_iri);
    result.push_str(&sparql[close..]);

    // Strip existing OFFSET and LIMIT (case insensitive)
    result = strip_clause(&result, "offset");
    result = strip_clause(&result, "limit");

    // Append new OFFSET and LIMIT
    let trimmed = result.trim_end();
    format!("{} OFFSET {} LIMIT 100", trimmed, row_count).into()
}

/// Remove a SPARQL clause like "OFFSET 10" or "LIMIT 50" (case insensitive)
fn strip_clause(sparql: &str, keyword: &str) -> String {
    let lower = sparql.to_lowercase();
    if let Some(pos) = lower.find(keyword) {
        let before = &sparql[..pos];
        let after = &sparql[pos + keyword.len()..];
        // Skip whitespace and digits after the keyword
        let rest = after.trim_start();
        let digit_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let consumed = after.len() - rest.len() + digit_end;
        format!(
            "{}{}",
            before.trim_end(),
            &sparql[pos + keyword.len() + consumed..]
        )
    } else {
        sparql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_resume_basic() {
        let sparql = "SELECT ?s ?p ?o FROM <mydb:main> WHERE { ?s ?p ?o }";
        let result = generate_resume_query(sparql, 5, 47).unwrap();
        assert!(result.contains("mydb:main@t:5"));
        assert!(result.contains("OFFSET 47"));
        assert!(result.contains("LIMIT 100"));
    }

    #[test]
    fn test_generate_resume_existing_time_suffix() {
        let sparql = "SELECT ?s FROM <mydb:main@t:3> WHERE { ?s ?p ?o }";
        let result = generate_resume_query(sparql, 5, 10).unwrap();
        assert!(result.contains("mydb:main@t:5"));
        assert!(!result.contains("@t:3"));
    }

    #[test]
    fn test_generate_resume_existing_offset_limit() {
        let sparql = "SELECT ?s FROM <mydb:main> WHERE { ?s ?p ?o } OFFSET 10 LIMIT 50";
        let result = generate_resume_query(sparql, 5, 47).unwrap();
        assert!(result.contains("OFFSET 47"));
        assert!(result.contains("LIMIT 100"));
        assert!(!result.contains("OFFSET 10"));
        assert!(!result.contains("LIMIT 50"));
    }

    #[test]
    fn test_generate_resume_multi_from_returns_none() {
        let sparql = "SELECT ?s FROM <db1:main> FROM <db2:main> WHERE { ?s ?p ?o }";
        assert!(generate_resume_query(sparql, 5, 10).is_none());
    }

    #[test]
    fn test_generate_resume_no_from_returns_none() {
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
        assert!(generate_resume_query(sparql, 5, 10).is_none());
    }

    #[test]
    fn test_build_schema_single_type() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?name");
        let v2 = vars.get_or_insert("?age");

        let mut type_map = HashMap::new();
        type_map
            .entry(v1)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:string".to_string());
        type_map
            .entry(v2)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:integer".to_string());

        let schema = build_schema(&type_map, &vars);
        assert_eq!(schema["?name"], json!("xsd:string"));
        assert_eq!(schema["?age"], json!("xsd:integer"));
    }

    #[test]
    fn test_build_schema_mixed_types() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?value");

        let mut type_map = HashMap::new();
        let types = type_map.entry(v1).or_insert_with(BTreeSet::new);
        types.insert("xsd:string".to_string());
        types.insert("xsd:integer".to_string());

        let schema = build_schema(&type_map, &vars);
        assert_eq!(schema["?value"], json!(["xsd:integer", "xsd:string"]));
    }

    #[test]
    fn test_build_schema_skips_internal_vars() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?name");
        let v_internal = vars.get_or_insert("?__pp0");

        let mut type_map = HashMap::new();
        type_map
            .entry(v1)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:string".to_string());
        type_map
            .entry(v_internal)
            .or_insert_with(BTreeSet::new)
            .insert("uri".to_string());

        let schema = build_schema(&type_map, &vars);
        assert!(schema.get("?name").is_some());
        assert!(schema.get("?__pp0").is_none());
    }
}
