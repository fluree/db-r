//! VALUES clause parsing
//!
//! Parses FQL VALUES clauses which provide inline data in queries.
//!
//! # Syntax
//!
//! ```json
//! {
//!   "select": ["?x", "?name"],
//!   "values": ["?x", [1, 2, 3]]
//! }
//! ```
//!
//! Or with multiple variables:
//!
//! ```json
//! {
//!   "select": ["?x", "?y"],
//!   "values": [
//!     ["?x", "?y"],
//!     [[1, "Alice"], [2, "Bob"]]
//!   ]
//! }
//! ```
//!
//! # Cell Types
//!
//! VALUES cells support:
//! - JSON scalars: number/string/bool
//! - `null` => UNDEF (Unbound)
//! - JSON-LD typed value objects: `{"@value": ..., "@type": ...}` with optional `"@language"`
//! - JSON-LD IRI objects: `{"@id": "..."}` or `{"@value": "...", "@type": "@id"}`
//! - Vector literals (when explicitly typed): `{"@value": [0.7, 0.6], "@type": "fluree:vector"}`

use super::ast::{LiteralValue, UnresolvedPattern, UnresolvedValue};
use super::error::{ParseError, Result};
use fluree_graph_json_ld::{details, ParsedContext};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Validate that a string looks like a variable (starts with ?)
fn validate_var_name(name: &str) -> Result<()> {
    if !name.starts_with('?') {
        return Err(ParseError::InvalidVariable(name.to_string()));
    }
    Ok(())
}

/// Parse a FQL VALUES clause.
///
/// Accepts the same structure as Clojure's `util.parse/normalize-values`:
/// - `[vars, vals]` where vars is a string or array of strings
/// - vals is an array of rows; each row is either:
///   - an array of length |vars|
///   - a scalar value when |vars| == 1
///
/// The JSON input `["?x", [1, 2, 3]]` produces a values pattern binding
/// `?x` to three rows.
pub fn parse_values_clause(
    values: &JsonValue,
    context: &ParsedContext,
) -> Result<UnresolvedPattern> {
    let arr = values.as_array().ok_or_else(|| {
        ParseError::InvalidWhere("values must be a 2-element array: [vars, vals]".to_string())
    })?;
    if arr.len() != 2 {
        return Err(ParseError::InvalidWhere(
            "values must be a 2-element array: [vars, vals]".to_string(),
        ));
    }

    // vars
    let vars_val = &arr[0];
    let vars: Vec<Arc<str>> = match vars_val {
        JsonValue::String(s) => {
            validate_var_name(s)?;
            vec![Arc::from(s.as_str())]
        }
        JsonValue::Array(vs) => {
            let mut out = Vec::with_capacity(vs.len());
            for v in vs {
                let s = v.as_str().ok_or_else(|| {
                    ParseError::InvalidWhere("values vars must be strings".to_string())
                })?;
                validate_var_name(s)?;
                out.push(Arc::from(s));
            }
            out
        }
        _ => {
            return Err(ParseError::InvalidWhere(
                "values vars must be a string or array of strings".to_string(),
            ))
        }
    };
    let var_count = vars.len();

    // vals
    let vals_val = &arr[1];
    let vals_arr = vals_val
        .as_array()
        .ok_or_else(|| ParseError::InvalidWhere("values rows must be an array".to_string()))?;

    let mut rows: Vec<Vec<UnresolvedValue>> = Vec::with_capacity(vals_arr.len());
    for row_val in vals_arr {
        let cells: Vec<&JsonValue> = match row_val {
            JsonValue::Array(cells) => cells.iter().collect(),
            _ if var_count == 1 => vec![row_val],
            _ => {
                return Err(ParseError::InvalidWhere(
                    "values row must be an array (or scalar when one var)".to_string(),
                ))
            }
        };

        if cells.len() != var_count {
            return Err(ParseError::InvalidWhere(format!(
                "Invalid value binding: number of variables and values don't match (vars={}, row={})",
                var_count,
                cells.len()
            )));
        }

        let mut out_row = Vec::with_capacity(var_count);
        for cell in cells {
            out_row.push(parse_values_cell(cell, context)?);
        }
        rows.push(out_row);
    }

    Ok(UnresolvedPattern::values(vars, rows))
}

/// Parse a single cell in a VALUES row.
///
/// Supports:
/// - `null` → Unbound
/// - Booleans → Boolean literal
/// - Numbers → Long or Double literal
/// - Strings → String literal
/// - Objects with `@id` → IRI
/// - Objects with `@value` and `@type` → Typed literal
/// - Objects with `@language` → Language-tagged string
/// - Arrays (only for vector type) → Vector literal
fn parse_values_cell(cell: &JsonValue, context: &ParsedContext) -> Result<UnresolvedValue> {
    match cell {
        JsonValue::Null => Ok(UnresolvedValue::Unbound),
        JsonValue::Bool(b) => Ok(UnresolvedValue::Literal {
            value: LiteralValue::Boolean(*b),
            dt_iri: None,
            lang: None,
        }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(UnresolvedValue::Literal {
                    value: LiteralValue::Long(i),
                    dt_iri: None,
                    lang: None,
                })
            } else if let Some(f) = n.as_f64() {
                Ok(UnresolvedValue::Literal {
                    value: LiteralValue::Double(f),
                    dt_iri: None,
                    lang: None,
                })
            } else {
                Err(ParseError::InvalidWhere(format!(
                    "Unsupported number type in values: {}",
                    n
                )))
            }
        }
        JsonValue::String(s) => Ok(UnresolvedValue::Literal {
            value: LiteralValue::String(Arc::from(s.as_str())),
            dt_iri: None,
            lang: None,
        }),
        JsonValue::Object(map) => parse_jsonld_object(map, context),
        JsonValue::Array(_) => Err(ParseError::InvalidWhere(
            "values cell cannot be an array (rows use arrays)".to_string(),
        )),
    }
}

/// Parse a JSON-LD object in VALUES cell
///
/// Supports:
/// - `{"@id": "..."}` - IRI binding
/// - `{"@value": ..., "@type": ..., "@language": ...}` - Typed literal
fn parse_jsonld_object(
    map: &serde_json::Map<String, JsonValue>,
    context: &ParsedContext,
) -> Result<UnresolvedValue> {
    // Handle @id shorthand
    if let Some(id_val) = map.get("@id") {
        return parse_iri_binding(id_val, context);
    }

    // Handle @value with @type and @language
    parse_typed_literal(map, context)
}

/// Parse IRI binding from `{"@id": "..."}`
fn parse_iri_binding(id_val: &JsonValue, context: &ParsedContext) -> Result<UnresolvedValue> {
    let id_str = id_val
        .as_str()
        .ok_or_else(|| ParseError::InvalidWhere("@id in values must be a string".to_string()))?;
    let (expanded, _) = details(id_str, context);
    Ok(UnresolvedValue::Iri(Arc::from(expanded)))
}

/// Parse typed literal from `{"@value": ..., "@type": ..., "@language": ...}`
fn parse_typed_literal(
    map: &serde_json::Map<String, JsonValue>,
    context: &ParsedContext,
) -> Result<UnresolvedValue> {
    let value_val = map.get("@value").ok_or_else(|| {
        ParseError::InvalidWhere("values object must contain @id or @value/@type".to_string())
    })?;

    let lang = map.get("@language").and_then(|v| v.as_str()).map(Arc::from);

    let dt_iri = map.get("@type").and_then(|v| v.as_str()).map(|t| {
        if t == "@id" {
            Arc::from("@id")
        } else {
            let (expanded, _) = details(t, context);
            Arc::from(expanded)
        }
    });

    // If datatype is @id, treat @value as IRI string
    if matches!(dt_iri.as_deref(), Some("@id")) {
        return parse_iri_from_value(value_val, context);
    }

    // Otherwise, parse as literal
    let lit = parse_literal_value(value_val, dt_iri.as_deref())?;
    Ok(UnresolvedValue::Literal {
        value: lit,
        dt_iri,
        lang,
    })
}

/// Parse IRI from @value when @type is @id
fn parse_iri_from_value(value_val: &JsonValue, context: &ParsedContext) -> Result<UnresolvedValue> {
    let s = value_val.as_str().ok_or_else(|| {
        ParseError::InvalidWhere("@value must be a string when @type is @id".to_string())
    })?;
    let (expanded, _) = details(s, context);
    Ok(UnresolvedValue::Iri(Arc::from(expanded)))
}

/// Parse literal value from @value field
fn parse_literal_value(value_val: &JsonValue, dt_iri: Option<&str>) -> Result<LiteralValue> {
    match value_val {
        JsonValue::String(s) => Ok(LiteralValue::String(Arc::from(s.as_str()))),
        JsonValue::Bool(b) => Ok(LiteralValue::Boolean(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LiteralValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(LiteralValue::Double(f))
            } else {
                Err(ParseError::InvalidWhere(format!(
                    "Unsupported number type in values: {}",
                    n
                )))
            }
        }
        JsonValue::Array(arr) => parse_vector_literal(arr, dt_iri),
        _ => Err(ParseError::InvalidWhere(
            "@value must be a string, number, bool, or array (vector)".to_string(),
        )),
    }
}

/// Parse vector literal from array @value
fn parse_vector_literal(arr: &[JsonValue], dt_iri: Option<&str>) -> Result<LiteralValue> {
    // Allow vector literals only when explicitly typed
    let is_vec = dt_iri.is_some_and(|dt| {
        dt == fluree_vocab::fluree::EMBEDDING_VECTOR
            || (dt.ends_with("#embeddingVector") && dt.contains("ns.flur.ee/db"))
    });

    if !is_vec {
        return Err(ParseError::InvalidWhere(
            "Array @value is only supported for https://ns.flur.ee/db#embeddingVector typed literals"
                .to_string(),
        ));
    }

    let mut vec = Vec::with_capacity(arr.len());
    for v in arr {
        let f = v.as_f64().ok_or_else(|| {
            ParseError::InvalidWhere("Vector @value array items must be numbers".to_string())
        })?;
        vec.push(f);
    }
    Ok(LiteralValue::Vector(vec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_json_ld::parse_context;
    use serde_json::json;

    fn test_context() -> ParsedContext {
        let ctx_json = json!({
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        });
        parse_context(&ctx_json).unwrap()
    }

    #[test]
    fn test_parse_values_single_var() {
        let context = test_context();
        let values_json = json!(["?x", [1, 2, 3]]);
        let pattern = parse_values_clause(&values_json, &context).unwrap();

        match pattern {
            UnresolvedPattern::Values { vars, rows } => {
                assert_eq!(vars.len(), 1);
                assert_eq!(vars[0].as_ref(), "?x");
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Values pattern"),
        }
    }

    #[test]
    fn test_parse_values_multiple_vars() {
        let context = test_context();
        let values_json = json!([["?x", "?y"], [[1, "Alice"], [2, "Bob"]]]);
        let pattern = parse_values_clause(&values_json, &context).unwrap();

        match pattern {
            UnresolvedPattern::Values { vars, rows } => {
                assert_eq!(vars.len(), 2);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
            }
            _ => panic!("Expected Values pattern"),
        }
    }

    #[test]
    fn test_parse_values_with_null() {
        let context = test_context();
        let values_json = json!(["?x", [1, null, 3]]);
        let pattern = parse_values_clause(&values_json, &context).unwrap();

        match pattern {
            UnresolvedPattern::Values { vars: _, rows } => {
                assert!(matches!(rows[1][0], UnresolvedValue::Unbound));
            }
            _ => panic!("Expected Values pattern"),
        }
    }

    #[test]
    fn test_parse_values_with_iri() {
        let context = test_context();
        let values_json = json!([
            "?x",
            [{"@id": "ex:alice"}, {"@id": "ex:bob"}]
        ]);
        let pattern = parse_values_clause(&values_json, &context).unwrap();

        match pattern {
            UnresolvedPattern::Values { vars: _, rows } => match &rows[0][0] {
                UnresolvedValue::Iri(iri) => {
                    assert_eq!(iri.as_ref(), "http://example.org/alice");
                }
                _ => panic!("Expected IRI"),
            },
            _ => panic!("Expected Values pattern"),
        }
    }

    #[test]
    fn test_parse_values_mismatched_columns() {
        let context = test_context();
        let values_json = json!([
            ["?x", "?y"],
            [[1, "Alice"], [2]] // Second row has wrong length
        ]);
        let result = parse_values_clause(&values_json, &context);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_values_invalid_var_name() {
        let context = test_context();
        let values_json = json!(["x", [1, 2]]); // Missing '?'
        let result = parse_values_clause(&values_json, &context);
        assert!(result.is_err());
    }
}
