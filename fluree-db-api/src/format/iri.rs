//! IRI compaction utilities
//!
//! This module provides `IriCompactor` for converting Sids to IRIs and
//! compacting them using @context prefix mappings via the json-ld library.

use fluree_db_core::Sid;
use fluree_graph_json_ld::{ContextCompactor, ParsedContext};
use std::collections::HashMap;

use super::{FormatError, Result};

/// Context for compacting IRIs using the json-ld library and Db namespace codes.
///
/// The compactor performs two operations:
/// 1. **Decode**: Convert a Sid (namespace_code + name) to a full IRI using Db.namespaces()
/// 2. **Compact**: Use a precomputed [`ContextCompactor`] to replace IRI prefixes with @context aliases
///
/// The reverse lookup table is built once at construction time and reused
/// for every IRI compacted through this instance.
#[derive(Debug, Clone)]
pub struct IriCompactor {
    /// Namespace code -> IRI prefix (from Db.namespaces())
    ///
    /// Example: `2 -> "http://www.w3.org/2001/XMLSchema#"`
    namespace_codes: HashMap<i32, String>,

    /// Parsed @context from the query (for advanced access / @reverse lookups)
    context: ParsedContext,

    /// Precomputed reverse lookup for fast IRI → compact-form compaction
    compactor: ContextCompactor,

    /// @reverse term definitions: maps full IRI → compact term name.
    /// Built once at construction for reverse property compaction.
    reverse_terms: HashMap<String, String>,
}

impl IriCompactor {
    /// Build a compactor from Db namespace codes and a ParsedContext.
    ///
    /// Precomputes the reverse lookup tables so that every subsequent
    /// `compact_vocab_iri` / `compact_id_iri` call is a pure lookup.
    pub fn new(namespace_codes: &HashMap<i32, String>, context: &ParsedContext) -> Self {
        let compactor = ContextCompactor::new(context);
        let reverse_terms = build_reverse_terms(context);
        Self {
            namespace_codes: namespace_codes.clone(),
            context: context.clone(),
            compactor,
            reverse_terms,
        }
    }

    /// Build a compactor with just namespace codes (no @context compaction).
    ///
    /// Useful when the query has no @context, or for testing.
    pub fn from_namespaces(namespace_codes: &HashMap<i32, String>) -> Self {
        let default_ctx = ParsedContext::default();
        let compactor = ContextCompactor::new(&default_ctx);
        Self {
            namespace_codes: namespace_codes.clone(),
            context: default_ctx,
            compactor,
            reverse_terms: HashMap::new(),
        }
    }

    /// Decode a Sid to a full IRI.
    ///
    /// Returns an error if the namespace code is not registered (this indicates
    /// a serious invariant violation: we should never have Sids we cannot decode).
    pub fn decode_sid(&self, sid: &Sid) -> Result<String> {
        let prefix = self
            .namespace_codes
            .get(&sid.namespace_code)
            .ok_or(FormatError::UnknownNamespace(sid.namespace_code))?;
        Ok(format!("{}{}", prefix, sid.name))
    }

    /// Compact an IRI using the @context (vocab rules).
    ///
    /// Handles:
    /// - @reverse term definitions (checked first)
    /// - Exact term matches (e.g., `"Person"` ← `"http://schema.org/Person"`)
    /// - Prefix matches (e.g., `"schema:xyz"` ← `"http://schema.org/xyz"`)
    /// - @vocab handling (bare terms for vocab-prefixed IRIs)
    ///
    /// Returns the compacted form or the full IRI if no match.
    pub fn compact_vocab_iri(&self, iri: &str) -> String {
        // Prefer @reverse term definitions
        if let Some(term) = self.reverse_terms.get(iri) {
            return term.clone();
        }
        self.compactor.compact_vocab(iri)
    }

    /// Compact an IRI for an `@id` position.
    ///
    /// Per JSON-LD rules, `@vocab` must NOT compact node identifiers; only explicit
    /// prefixes/terms and `@base` are allowed.
    pub fn compact_id_iri(&self, iri: &str) -> String {
        self.compactor.compact_id(iri)
    }

    /// Decode a Sid and compact in one step.
    ///
    /// This is the most common operation for formatting.
    pub fn compact_sid(&self, sid: &Sid) -> Result<String> {
        let iri = self.decode_sid(sid)?;
        Ok(self.compact_vocab_iri(&iri))
    }

    /// Compact an IRI string (already decoded).
    ///
    /// Used for IriMatch bindings where the canonical IRI is already available.
    pub fn compact_iri(&self, iri: &str) -> Result<String> {
        Ok(self.compact_vocab_iri(iri))
    }

    /// Check if a namespace code is registered
    pub fn has_namespace(&self, code: i32) -> bool {
        self.namespace_codes.contains_key(&code)
    }

    /// Get the ParsedContext (for advanced use)
    pub fn context(&self) -> &ParsedContext {
        &self.context
    }

    /// Get the precomputed compactor (for constructing closures)
    pub fn ctx_compactor(&self) -> &ContextCompactor {
        &self.compactor
    }
}

/// Build a map of @reverse IRI → term name for fast reverse-property lookup.
///
/// If multiple terms define @reverse for the same IRI, the lexicographically
/// smallest term name wins (deterministic).
fn build_reverse_terms(context: &ParsedContext) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, entry) in &context.terms {
        if let Some(ref rev_iri) = entry.reverse {
            map.entry(rev_iri.clone())
                .and_modify(|existing| {
                    if key < existing {
                        *existing = key.clone();
                    }
                })
                .or_insert_with(|| key.clone());
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_test_namespaces() -> HashMap<i32, String> {
        let mut map = HashMap::new();
        map.insert(0, "".to_string());
        map.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        map.insert(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
        map.insert(17, "http://schema.org/".to_string());
        map.insert(100, "http://example.org/".to_string());
        map
    }

    fn make_test_context() -> ParsedContext {
        ParsedContext::parse(
            None,
            &json!({
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "schema": "http://schema.org/",
                "ex": "http://example.org/"
            }),
        )
        .unwrap()
    }

    #[test]
    fn test_decode_sid() {
        let compactor = IriCompactor::from_namespaces(&make_test_namespaces());

        let sid = Sid::new(2, "string");
        assert_eq!(
            compactor.decode_sid(&sid).unwrap(),
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        );

        let sid = Sid::new(100, "Person");
        assert_eq!(
            compactor.decode_sid(&sid).unwrap(),
            "http://example.org/Person".to_string()
        );

        // Unknown namespace (truly unknown code, not the sentinel)
        let sid = Sid::new(999, "unknown");
        assert!(matches!(
            compactor.decode_sid(&sid),
            Err(FormatError::UnknownNamespace(999))
        ));

        // EMPTY namespace (code 0) fallback decodes to the full IRI stored as name
        let sid = Sid::new(0, "http://unregistered.org/Thing");
        assert_eq!(
            compactor.decode_sid(&sid).unwrap(),
            "http://unregistered.org/Thing"
        );
    }

    #[test]
    fn test_compact_iri_with_context() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // Prefix matches via @context
        assert_eq!(
            compactor.compact_vocab_iri("http://www.w3.org/2001/XMLSchema#string"),
            "xsd:string"
        );

        assert_eq!(
            compactor.compact_vocab_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "rdf:type"
        );

        assert_eq!(
            compactor.compact_vocab_iri("http://schema.org/Person"),
            "schema:Person"
        );

        assert_eq!(
            compactor.compact_vocab_iri("http://example.org/myThing"),
            "ex:myThing"
        );
    }

    #[test]
    fn test_compact_iri_no_match() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // No matching prefix - returns full IRI
        assert_eq!(
            compactor.compact_vocab_iri("http://unknown.org/something"),
            "http://unknown.org/something"
        );
    }

    #[test]
    fn test_compact_sid() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // Known namespace with @context prefix
        let sid = Sid::new(2, "string");
        assert_eq!(compactor.compact_sid(&sid).unwrap(), "xsd:string");

        let sid = Sid::new(17, "Person");
        assert_eq!(compactor.compact_sid(&sid).unwrap(), "schema:Person");
    }

    #[test]
    fn test_compact_without_context() {
        let compactor = IriCompactor::from_namespaces(&make_test_namespaces());

        // No @context, so IRIs come through uncompacted
        let sid = Sid::new(2, "string");
        // json_ld::compact with empty context returns the IRI as-is
        assert_eq!(
            compactor.compact_sid(&sid).unwrap(),
            "http://www.w3.org/2001/XMLSchema#string"
        );
    }
}
