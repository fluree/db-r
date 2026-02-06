//! Transaction metadata extraction from JSON-LD
//!
//! This module extracts user-provided transaction metadata from JSON-LD
//! transaction documents. Metadata appears as top-level non-`@*` keys in
//! "envelope form" documents (those containing `@graph`).
//!
//! # Envelope Form Requirement
//!
//! Txn-meta is **only** extracted from transactions with an explicit `@graph`:
//!
//! ```json
//! {
//!   "@context": {"ex": "http://example.org/"},
//!   "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }],
//!   "ex:machine": "server-01",
//!   "ex:batchId": 42
//! }
//! ```
//!
//! In this example, `ex:machine` and `ex:batchId` become txn-meta entries.
//! The `@graph` contents become normal data.
//!
//! Single-object transactions (no `@graph`) have **no metadata** — all
//! properties are data:
//!
//! ```json
//! {
//!   "@context": {"ex": "http://example.org/"},
//!   "@id": "ex:alice",
//!   "ex:name": "Alice"
//! }
//! ```

use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue, MAX_TXN_META_BYTES, MAX_TXN_META_ENTRIES};
use fluree_graph_json_ld::{details, ParsedContext};
use serde_json::Value;

/// Reserved keys that are never transaction metadata
const RESERVED_KEYS: &[&str] = &["@context", "@graph", "@id", "@type", "@base", "@vocab"];

/// Extract transaction metadata from a JSON-LD document.
///
/// Returns an empty `Vec` if:
/// - The document is not in envelope form (no `@graph`)
/// - There are no non-reserved top-level keys
///
/// # Errors
///
/// Returns an error if:
/// - A metadata value cannot be converted (e.g., nested object without @value/@id)
/// - A double value is non-finite (NaN, +Inf, -Inf)
/// - Entry count exceeds `MAX_TXN_META_ENTRIES`
/// - Estimated encoded size exceeds `MAX_TXN_META_BYTES`
pub fn extract_txn_meta(
    json: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
) -> Result<Vec<TxnMetaEntry>> {
    // 1. Check for envelope form (must have @graph)
    let obj = match json.as_object() {
        Some(o) if o.contains_key("@graph") => o,
        _ => return Ok(Vec::new()), // Single object = no txn-meta
    };

    let mut entries = Vec::new();

    for (key, value) in obj {
        // Skip @ keys and reserved keys
        if key.starts_with('@') || RESERVED_KEYS.contains(&key.as_str()) {
            continue;
        }

        // Expand key to full IRI using context
        let (expanded_iri, _) = details(key, context);

        // Split to ns_code + local name via registry
        let sid = ns_registry.sid_for_iri(&expanded_iri);
        let predicate_ns = sid.namespace_code;
        let predicate_name = sid.name.to_string();

        // Convert value(s) to TxnMetaValue(s)
        let meta_values = json_to_txn_meta_values(value, context, ns_registry)?;

        for mv in meta_values {
            entries.push(TxnMetaEntry::new(predicate_ns, predicate_name.clone(), mv));
        }
    }

    validate_limits(&entries)?;
    Ok(entries)
}

/// Convert a JSON value to txn-meta values.
///
/// Arrays produce multiple values; scalars produce one.
fn json_to_txn_meta_values(
    value: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
) -> Result<Vec<TxnMetaValue>> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::with_capacity(arr.len());
            for item in arr {
                results.push(json_to_single_txn_meta_value(item, context, ns_registry)?);
            }
            Ok(results)
        }
        _ => Ok(vec![json_to_single_txn_meta_value(
            value, context, ns_registry,
        )?]),
    }
}

/// Convert a single JSON value to a TxnMetaValue.
fn json_to_single_txn_meta_value(
    value: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TxnMetaValue> {
    match value {
        Value::Null => Err(TransactError::Parse(
            "txn-meta value cannot be null".to_string(),
        )),

        Value::Bool(b) => Ok(TxnMetaValue::Boolean(*b)),

        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TxnMetaValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values (NaN, Inf)".to_string(),
                    ));
                }
                Ok(TxnMetaValue::Double(f))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number type in txn-meta: {}",
                    n
                )))
            }
        }

        Value::String(s) => {
            // Plain strings are always literals in JSON-LD semantics.
            // Use {"@id": "..."} to create IRI references.
            // This avoids surprising behavior with strings like "acct:123" or "foo:bar".
            Ok(TxnMetaValue::String(s.clone()))
        }

        Value::Object(obj) => {
            // @id object → IRI reference
            if let Some(id_val) = obj.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    TransactError::Parse("@id in txn-meta must be a string".to_string())
                })?;
                let (expanded, _) = details(id_str, context);
                let sid = ns_registry.sid_for_iri(&expanded);
                return Ok(TxnMetaValue::Ref {
                    ns: sid.namespace_code,
                    name: sid.name.to_string(),
                });
            }

            // @value object → literal with optional @type or @language
            if let Some(val) = obj.get("@value") {
                return parse_value_object(val, obj, context, ns_registry);
            }

            // Other object shapes not supported in txn-meta
            Err(TransactError::Parse(
                "txn-meta objects must contain @id or @value; nested objects not supported"
                    .to_string(),
            ))
        }

        Value::Array(_) => {
            // Should not reach here - arrays handled by caller
            Err(TransactError::Parse(
                "Unexpected array in single value position".to_string(),
            ))
        }
    }
}

/// Parse a `{"@value": ..., "@type"?: ..., "@language"?: ...}` object.
fn parse_value_object(
    val: &Value,
    obj: &serde_json::Map<String, Value>,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TxnMetaValue> {
    // @type present → typed literal
    if let Some(type_val) = obj.get("@type") {
        let type_iri = type_val.as_str().ok_or_else(|| {
            TransactError::Parse("@type in txn-meta must be a string".to_string())
        })?;

        // Expand the datatype IRI
        let (expanded_type, _) = details(type_iri, context);
        let dt_sid = ns_registry.sid_for_iri(&expanded_type);

        // Get the string value
        let value_str = val.as_str().ok_or_else(|| {
            TransactError::Parse(
                "txn-meta typed literal @value must be a string".to_string(),
            )
        })?;

        return Ok(TxnMetaValue::TypedLiteral {
            value: value_str.to_string(),
            dt_ns: dt_sid.namespace_code,
            dt_name: dt_sid.name.to_string(),
        });
    }

    // @language present → language-tagged string
    if let Some(lang_val) = obj.get("@language") {
        let lang = lang_val.as_str().ok_or_else(|| {
            TransactError::Parse("@language in txn-meta must be a string".to_string())
        })?;

        let value_str = val.as_str().ok_or_else(|| {
            TransactError::Parse(
                "txn-meta language-tagged @value must be a string".to_string(),
            )
        })?;

        return Ok(TxnMetaValue::LangString {
            value: value_str.to_string(),
            lang: lang.to_string(),
        });
    }

    // Plain @value (no @type or @language)
    match val {
        Value::String(s) => Ok(TxnMetaValue::String(s.clone())),
        Value::Bool(b) => Ok(TxnMetaValue::Boolean(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TxnMetaValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values (NaN, Inf)".to_string(),
                    ));
                }
                Ok(TxnMetaValue::Double(f))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in txn-meta @value: {}",
                    n
                )))
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Unsupported @value type in txn-meta: {:?}",
            val
        ))),
    }
}

/// Validate that txn-meta entries are within limits.
fn validate_limits(entries: &[TxnMetaEntry]) -> Result<()> {
    // Entry count limit
    if entries.len() > MAX_TXN_META_ENTRIES {
        return Err(TransactError::Parse(format!(
            "txn-meta entry count {} exceeds maximum {}",
            entries.len(),
            MAX_TXN_META_ENTRIES
        )));
    }

    // Estimate encoded size (conservative estimate)
    let mut estimated_bytes: usize = 0;
    for entry in entries {
        // predicate: 2 (ns) + 4 (len) + name bytes
        estimated_bytes += 6 + entry.predicate_name.len();

        // value: tag byte + payload
        estimated_bytes += 1 + estimate_value_size(&entry.value);
    }

    if estimated_bytes > MAX_TXN_META_BYTES {
        return Err(TransactError::Parse(format!(
            "txn-meta estimated size {} bytes exceeds maximum {} bytes",
            estimated_bytes, MAX_TXN_META_BYTES
        )));
    }

    Ok(())
}

/// Estimate the encoded size of a TxnMetaValue.
fn estimate_value_size(value: &TxnMetaValue) -> usize {
    match value {
        TxnMetaValue::String(s) => 4 + s.len(),
        TxnMetaValue::Long(_) => 8,
        TxnMetaValue::Double(_) => 8,
        TxnMetaValue::Boolean(_) => 1,
        TxnMetaValue::Ref { name, .. } => 6 + name.len(),
        TxnMetaValue::LangString { value, lang } => 8 + value.len() + lang.len(),
        TxnMetaValue::TypedLiteral {
            value, dt_name, ..
        } => 10 + value.len() + dt_name.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_registry() -> NamespaceRegistry {
        NamespaceRegistry::new()
    }

    fn empty_context() -> ParsedContext {
        ParsedContext::new()
    }

    #[test]
    fn test_no_graph_returns_empty() {
        let mut ns = test_registry();
        let ctx = empty_context();

        // Single object (no @graph) → no metadata
        let json = json!({
            "@id": "http://example.org/alice",
            "http://example.org/name": "Alice"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_envelope_form_extracts_metadata() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [{ "@id": "http://example.org/alice" }],
            "http://example.org/machine": "server-01",
            "http://example.org/batchId": 42
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 2);

        // Find machine entry
        let machine = result.iter().find(|e| e.predicate_name == "machine").unwrap();
        assert!(matches!(&machine.value, TxnMetaValue::String(s) if s == "server-01"));

        // Find batchId entry
        let batch = result.iter().find(|e| e.predicate_name == "batchId").unwrap();
        assert!(matches!(&batch.value, TxnMetaValue::Long(42)));
    }

    #[test]
    fn test_reserved_keys_skipped() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@context": {},
            "@graph": [],
            "@id": "ignored",
            "@type": "ignored",
            "@base": "ignored",
            "@vocab": "ignored",
            "http://example.org/keep": "this one"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "keep");
    }

    #[test]
    fn test_boolean_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/active": true
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, TxnMetaValue::Boolean(true)));
    }

    #[test]
    fn test_double_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/ratio": 1.23
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::Double(f) = &result[0].value {
            assert!((f - 1.23).abs() < 0.001);
        } else {
            panic!("Expected double");
        }
    }

    #[test]
    fn test_array_values() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/tags": ["a", "b", "c"]
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|e| e.predicate_name == "tags"));
    }

    #[test]
    fn test_id_object_becomes_ref() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/author": { "@id": "http://example.org/alice" }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::Ref { name, .. } = &result[0].value {
            assert_eq!(name, "alice");
        } else {
            panic!("Expected ref");
        }
    }

    #[test]
    fn test_iri_string_stays_string() {
        // Plain strings are always literals, even if they look like IRIs.
        // Use {"@id": "..."} to create IRI references.
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/related": "http://example.org/bob"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        // Should be a string, not a ref
        assert!(
            matches!(&result[0].value, TxnMetaValue::String(s) if s == "http://example.org/bob")
        );
    }

    #[test]
    fn test_plain_string_stays_string() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/note": "hello world"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, TxnMetaValue::String(s) if s == "hello world"));
    }

    #[test]
    fn test_typed_literal() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/date": {
                "@value": "2025-01-15",
                "@type": "http://www.w3.org/2001/XMLSchema#date"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::TypedLiteral { value, dt_name, .. } = &result[0].value {
            assert_eq!(value, "2025-01-15");
            assert_eq!(dt_name, "date");
        } else {
            panic!("Expected typed literal");
        }
    }

    #[test]
    fn test_language_tagged_string() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/title": {
                "@value": "Bonjour",
                "@language": "fr"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::LangString { value, lang } = &result[0].value {
            assert_eq!(value, "Bonjour");
            assert_eq!(lang, "fr");
        } else {
            panic!("Expected lang string");
        }
    }

    #[test]
    fn test_reject_nested_object() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/nested": {
                "foo": "bar"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nested objects not supported"));
    }

    #[test]
    fn test_reject_null_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/bad": null
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null"));
    }

    #[test]
    fn test_entry_count_limit() {
        let mut ns = test_registry();
        let ctx = empty_context();

        // Create JSON with too many entries
        let mut obj = serde_json::Map::new();
        obj.insert("@graph".to_string(), json!([]));
        for i in 0..300 {
            obj.insert(format!("http://example.org/key{}", i), json!(i));
        }
        let json = Value::Object(obj);

        let result = extract_txn_meta(&json, &ctx, &mut ns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_context_expansion() {
        let mut ns = test_registry();

        // Use a context with prefix
        let ctx_json = json!({
            "ex": "http://example.org/"
        });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "ex:machine": "server-01"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "machine");
        // The ns code should be for http://example.org/
        assert!(ns.has_prefix("http://example.org/"));
    }
}
