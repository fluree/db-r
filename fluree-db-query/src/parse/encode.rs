//! IRI encoding trait for WASM-compatible abstraction
//!
//! This trait allows the parser to remain runtime-agnostic by abstracting
//! the IRI-to-SID encoding step. Native implementations can use the database's
//! namespace codes, while WASM/offline implementations can use stubs or
//! context-only encoders.

use fluree_db_core::{Db, Sid, Storage};
use fluree_vocab::{rdf, xsd};

/// Trait for encoding IRIs to SIDs
///
/// Keeps the parser runtime-agnostic - the actual encoding implementation
/// can be provided by the database (native) or a stub (WASM/offline).
pub trait IriEncoder {
    /// Encode an IRI to a SID
    ///
    /// Returns `None` if the IRI's namespace is not registered.
    fn encode_iri(&self, iri: &str) -> Option<Sid>;
}

// Native: Db implements IriEncoder
impl<S: Storage + 'static> IriEncoder for Db<S> {
    fn encode_iri(&self, iri: &str) -> Option<Sid> {
        // Delegates to the existing Db::encode_iri method
        Db::encode_iri(self, iri)
    }
}

/// Stub encoder that always fails
///
/// Useful for testing parsing without a database, or for WASM contexts
/// where namespace encoding isn't available.
pub struct NoEncoder;

impl IriEncoder for NoEncoder {
    fn encode_iri(&self, _iri: &str) -> Option<Sid> {
        None
    }
}

/// In-memory encoder with a fixed namespace mapping
///
/// Useful for testing or when the full database isn't available.
#[derive(Debug, Default)]
pub struct MemoryEncoder {
    namespaces: std::collections::HashMap<String, u16>,
}

impl MemoryEncoder {
    /// Create a new empty encoder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a namespace mapping
    pub fn add_namespace(&mut self, prefix: impl Into<String>, code: u16) -> &mut Self {
        self.namespaces.insert(prefix.into(), code);
        self
    }

    /// Create an encoder with common namespaces pre-registered
    pub fn with_common_namespaces() -> Self {
        let mut encoder = Self::new();
        encoder
            .add_namespace("", 0)
            .add_namespace("@", 1)
            .add_namespace(xsd::NS, 2)
            .add_namespace(rdf::NS, 3);
        encoder
    }
}

impl IriEncoder for MemoryEncoder {
    fn encode_iri(&self, iri: &str) -> Option<Sid> {
        // Find the longest matching namespace prefix
        let mut best_match: Option<(u16, usize)> = None;

        for (prefix, &code) in &self.namespaces {
            if iri.starts_with(prefix) && prefix.len() > best_match.map(|(_, l)| l).unwrap_or(0) {
                best_match = Some((code, prefix.len()));
            }
        }

        best_match.map(|(code, prefix_len)| {
            let name = &iri[prefix_len..];
            Sid::new(code, name)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_encoder() {
        let encoder = NoEncoder;
        assert!(encoder.encode_iri("http://example.org/test").is_none());
    }

    #[test]
    fn test_memory_encoder() {
        let mut encoder = MemoryEncoder::new();
        encoder.add_namespace("http://example.org/", 100);

        let sid = encoder.encode_iri("http://example.org/Person").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Person");

        // Unknown namespace returns None
        assert!(encoder.encode_iri("http://other.org/Thing").is_none());
    }

    #[test]
    fn test_memory_encoder_common_namespaces() {
        let encoder = MemoryEncoder::with_common_namespaces();

        let xsd_string = encoder.encode_iri(xsd::STRING).unwrap();
        assert_eq!(xsd_string.namespace_code, 2);
        assert_eq!(xsd_string.name.as_ref(), "string");

        let rdf_type = encoder.encode_iri(rdf::TYPE).unwrap();
        assert_eq!(rdf_type.namespace_code, 3);
        assert_eq!(rdf_type.name.as_ref(), "type");
    }

    #[test]
    fn test_memory_encoder_longest_prefix() {
        let mut encoder = MemoryEncoder::new();
        encoder.add_namespace("http://example.org/", 100);
        encoder.add_namespace("http://example.org/ns/", 101);

        // Should match the longer prefix
        let sid = encoder.encode_iri("http://example.org/ns/Thing").unwrap();
        assert_eq!(sid.namespace_code, 101);
        assert_eq!(sid.name.as_ref(), "Thing");

        // Should match the shorter prefix
        let sid2 = encoder.encode_iri("http://example.org/Other").unwrap();
        assert_eq!(sid2.namespace_code, 100);
        assert_eq!(sid2.name.as_ref(), "Other");
    }
}
