//! Well-known datatype constants and utilities
//!
//! This module provides datatype IRI constants and helper functions
//! for determining formatting behavior based on datatype.

// Re-export vocabulary constants from the vocab crate for convenience
pub use fluree_vocab::fluree;
pub use fluree_vocab::rdf;
pub use fluree_vocab::xsd;

/// JSON-LD internal types
pub mod jsonld {
    /// @json - JSON literal
    pub const JSON: &str = "@json";
}

/// Check if a datatype is "inferable" from the JSON value.
///
/// SPARQL 1.1 JSON Results format allows omitting the datatype for types
/// that can be inferred from the JSON representation:
/// - xsd:string - plain string in JSON
/// - xsd:integer/xsd:long - whole number in JSON
/// - xsd:double/xsd:decimal - floating point in JSON
/// - xsd:boolean - true/false in JSON
/// - fluree:vector - JSON array of floats (Clojure parity)
///
/// These types are automatically inferred by JSON parsers.
pub fn is_inferable_datatype(dt_iri: &str) -> bool {
    matches!(
        dt_iri,
        xsd::STRING
            | xsd::LONG
            | xsd::INTEGER
            | xsd::DOUBLE
            | xsd::BOOLEAN
            | xsd::DECIMAL
            | fluree::VECTOR
    )
}

// Note: is_reference_datatype is NOT needed - Binding::Sid already indicates references.
// The Rust invariant (Binding::Lit never contains FlakeValue::Ref) eliminates the need
// for datatype checks to identify references.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_inferable_datatype() {
        // Inferable types
        assert!(is_inferable_datatype(xsd::STRING));
        assert!(is_inferable_datatype(xsd::LONG));
        assert!(is_inferable_datatype(xsd::INTEGER));
        assert!(is_inferable_datatype(xsd::DOUBLE));
        assert!(is_inferable_datatype(xsd::BOOLEAN));
        assert!(is_inferable_datatype(xsd::DECIMAL));

        assert!(is_inferable_datatype(fluree::VECTOR));

        // Non-inferable types
        assert!(!is_inferable_datatype(xsd::DATE_TIME));
        assert!(!is_inferable_datatype(xsd::DATE));
        assert!(!is_inferable_datatype(rdf::LANG_STRING));
        assert!(!is_inferable_datatype(jsonld::JSON));
        assert!(!is_inferable_datatype("http://example.org/customType"));
    }
}
