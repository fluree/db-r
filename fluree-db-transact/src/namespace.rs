//! Namespace registry for IRI prefix code allocation
//!
//! This module provides `NamespaceRegistry` for managing namespace codes
//! during transaction processing. New IRIs with unknown prefixes get new
//! codes allocated, and these allocations are tracked for persistence
//! in the commit record.
//!
//! ## Predefined Namespace Codes
//!
//! Fluree uses predefined codes for common namespaces to ensure compatibility
//! with existing databases. User-supplied namespaces start at code 101.

use fluree_db_core::{Db, NodeCache, Sid, Storage};
use std::collections::HashMap;

// ============================================================================
// Predefined Namespace Codes (must match Clojure implementation)
// ============================================================================

/// Empty/relative IRI prefix
pub const NS_EMPTY: i32 = 0;
/// JSON-LD keywords (@id, @type, etc.)
pub const NS_JSONLD: i32 = 1;
/// XSD datatypes (http://www.w3.org/2001/XMLSchema#)
pub const NS_XSD: i32 = 2;
/// RDF namespace (http://www.w3.org/1999/02/22-rdf-syntax-ns#)
pub const NS_RDF: i32 = 3;
/// RDFS namespace (http://www.w3.org/2000/01/rdf-schema#)
pub const NS_RDFS: i32 = 4;
/// SHACL namespace (http://www.w3.org/ns/shacl#)
pub const NS_SHACL: i32 = 5;
/// OWL namespace (http://www.w3.org/2002/07/owl#)
pub const NS_OWL: i32 = 6;
/// W3C Credentials (https://www.w3.org/2018/credentials#)
pub const NS_CREDENTIALS: i32 = 7;
/// Fluree ledger namespace (https://ns.flur.ee/ledger#)
pub const NS_FLUREE_LEDGER: i32 = 8;
// 9 is unused
/// Fluree DB address (fluree:db:sha256:)
pub const NS_FLUREE_DB: i32 = 10;
/// DID key (did:key:)
pub const NS_DID_KEY: i32 = 11;
/// Fluree commit address (fluree:commit:sha256:)
pub const NS_FLUREE_COMMIT: i32 = 12;
/// Fluree memory storage (fluree:memory://)
pub const NS_FLUREE_MEMORY: i32 = 13;
/// Fluree file storage (fluree:file://)
pub const NS_FLUREE_FILE: i32 = 14;
/// Fluree IPFS storage (fluree:ipfs://)
pub const NS_FLUREE_IPFS: i32 = 15;
/// Fluree S3 storage (fluree:s3://)
pub const NS_FLUREE_S3: i32 = 16;
/// Schema.org (http://schema.org/)
pub const NS_SCHEMA_ORG: i32 = 17;
/// Wikidata (https://www.wikidata.org/wiki/)
pub const NS_WIKIDATA: i32 = 18;
/// FOAF (http://xmlns.com/foaf/0.1/)
pub const NS_FOAF: i32 = 19;
/// SKOS (http://www.w3.org/2008/05/skos#)
pub const NS_SKOS: i32 = 20;
/// UUID URN (urn:uuid)
pub const NS_UUID: i32 = 21;
/// ISBN URN (urn:isbn:)
pub const NS_ISBN: i32 = 22;
/// ISSN URN (urn:issn:)
pub const NS_ISSN: i32 = 23;
/// Blank nodes (_:)
pub const NS_BLANK_NODE: i32 = 24;
/// Fluree index namespace (https://ns.flur.ee/index#)
pub const NS_FLUREE_INDEX: i32 = 25;

/// First code available for user-defined namespaces
pub const USER_NS_START: i32 = 101;

/// Blank node prefix (standard RDF blank node syntax)
pub const BLANK_NODE_PREFIX: &str = "_:";

/// Fluree blank node ID prefix (used in generated blank node names)
pub const BLANK_NODE_ID_PREFIX: &str = "fdb";

/// Build the default namespace mappings
fn default_namespaces() -> HashMap<i32, String> {
    let mut map = HashMap::new();
    map.insert(NS_EMPTY, "".to_string());
    map.insert(NS_JSONLD, "@".to_string());
    map.insert(NS_XSD, "http://www.w3.org/2001/XMLSchema#".to_string());
    map.insert(NS_RDF, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
    map.insert(NS_RDFS, "http://www.w3.org/2000/01/rdf-schema#".to_string());
    map.insert(NS_SHACL, "http://www.w3.org/ns/shacl#".to_string());
    map.insert(NS_OWL, "http://www.w3.org/2002/07/owl#".to_string());
    map.insert(NS_CREDENTIALS, "https://www.w3.org/2018/credentials#".to_string());
    map.insert(NS_FLUREE_LEDGER, "https://ns.flur.ee/ledger#".to_string());
    map.insert(NS_FLUREE_DB, "fluree:db:sha256:".to_string());
    map.insert(NS_DID_KEY, "did:key:".to_string());
    map.insert(NS_FLUREE_COMMIT, "fluree:commit:sha256:".to_string());
    map.insert(NS_FLUREE_MEMORY, "fluree:memory://".to_string());
    map.insert(NS_FLUREE_FILE, "fluree:file://".to_string());
    map.insert(NS_FLUREE_IPFS, "fluree:ipfs://".to_string());
    map.insert(NS_FLUREE_S3, "fluree:s3://".to_string());
    map.insert(NS_SCHEMA_ORG, "http://schema.org/".to_string());
    map.insert(NS_WIKIDATA, "https://www.wikidata.org/wiki/".to_string());
    map.insert(NS_FOAF, "http://xmlns.com/foaf/0.1/".to_string());
    map.insert(NS_SKOS, "http://www.w3.org/2008/05/skos#".to_string());
    map.insert(NS_UUID, "urn:uuid".to_string());
    map.insert(NS_ISBN, "urn:isbn:".to_string());
    map.insert(NS_ISSN, "urn:issn:".to_string());
    map.insert(NS_BLANK_NODE, BLANK_NODE_PREFIX.to_string());
    map.insert(NS_FLUREE_INDEX, "https://ns.flur.ee/index#".to_string());
    map
}

/// Registry for namespace prefix codes
///
/// During transaction processing, new IRIs may introduce prefixes not yet
/// in the database's namespace table. This registry:
/// 1. Starts with predefined codes for common namespaces
/// 2. Loads existing codes from `db.namespace_codes`
/// 3. Allocates new codes as needed (starting at USER_NS_START)
/// 4. Tracks new allocations in `delta` for commit persistence
#[derive(Debug, Clone)]
pub struct NamespaceRegistry {
    /// Prefix → code mapping (for lookups)
    codes: HashMap<String, i32>,

    /// Code → prefix mapping (for encoding, matches Db.namespace_codes)
    names: HashMap<i32, String>,

    /// Next available code for allocation (>= USER_NS_START)
    next_code: i32,

    /// New allocations this transaction (code → prefix)
    pub(crate) delta: HashMap<i32, String>,
}

impl NamespaceRegistry {
    /// Create a new registry with default namespaces
    pub fn new() -> Self {
        let names = default_namespaces();
        let codes: HashMap<String, i32> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        Self {
            codes,
            names,
            next_code: USER_NS_START,
            delta: HashMap::new(),
        }
    }

    /// Create a registry seeded from a database's namespace codes
    ///
    /// This merges the database's codes with the predefined defaults,
    /// with the database taking precedence for any conflicts.
    pub fn from_db<S: Storage, C: NodeCache>(db: &Db<S, C>) -> Self {
        // Start with defaults
        let mut names = default_namespaces();

        // Merge in database codes (overwriting defaults if needed)
        for (code, prefix) in &db.namespace_codes {
            names.insert(*code, prefix.clone());
        }

        let codes: HashMap<String, i32> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        // Next code is max of existing codes + 1, but at least USER_NS_START
        let max_code = names.keys().max().copied().unwrap_or(0);
        let next_code = (max_code + 1).max(USER_NS_START);

        Self {
            codes,
            names,
            next_code,
            delta: HashMap::new(),
        }
    }

    /// Get the code for a prefix, allocating a new one if needed
    ///
    /// If the prefix is not yet registered, a new code is allocated
    /// and recorded in the delta for persistence.
    pub fn get_or_allocate(&mut self, prefix: &str) -> i32 {
        if let Some(&code) = self.codes.get(prefix) {
            return code;
        }

        // Allocate new code
        let code = self.next_code;
        self.next_code += 1;

        self.codes.insert(prefix.to_string(), code);
        self.names.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());

        code
    }

    /// Get the namespace code for blank nodes (always NS_BLANK_NODE = 24)
    pub fn blank_node_code(&self) -> i32 {
        NS_BLANK_NODE
    }

    /// Look up a code without allocating
    pub fn get_code(&self, prefix: &str) -> Option<i32> {
        self.codes.get(prefix).copied()
    }

    /// Look up a prefix by code
    pub fn get_prefix(&self, code: i32) -> Option<&str> {
        self.names.get(&code).map(|s| s.as_str())
    }

    /// Check if a prefix is registered
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.codes.contains_key(prefix)
    }

    /// Take the delta (new allocations) and reset it
    ///
    /// Returns the map of new allocations (code → prefix) for
    /// inclusion in the commit record.
    pub fn take_delta(&mut self) -> HashMap<i32, String> {
        std::mem::take(&mut self.delta)
    }

    /// Get a reference to the delta without consuming it
    pub fn delta(&self) -> &HashMap<i32, String> {
        &self.delta
    }

    /// Check if there are any new allocations
    pub fn has_delta(&self) -> bool {
        !self.delta.is_empty()
    }

    /// Create a Sid for an IRI, allocating namespace code if needed
    ///
    /// The IRI is split into prefix and local name. The prefix gets
    /// a namespace code (existing or newly allocated), and the result
    /// is a Sid with that code and the local name.
    ///
    /// This uses longest-prefix matching first (checking against known prefixes),
    /// then falls back to splitting on the last `/` or `#` for unknown prefixes.
    /// This ensures parity with `Db::encode_iri()` at query time.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        // First, try to find the longest known prefix that matches this IRI.
        // This ensures consistency with Db::encode_iri() at query time.
        let mut best_match: Option<(&str, i32)> = None;
        for (prefix, &code) in &self.codes {
            if iri.starts_with(prefix) {
                let prefix_len = prefix.len();
                if best_match.map(|(p, _)| p.len()).unwrap_or(0) < prefix_len {
                    best_match = Some((prefix.as_str(), code));
                }
            }
        }

        if let Some((prefix, code)) = best_match {
            let local = &iri[prefix.len()..];
            return Sid::new(code, local);
        }

        // No known prefix matched; fall back to split heuristic for new prefixes.
        // Find split point (last / or #)
        let split_pos = iri.rfind(|c| c == '/' || c == '#');

        let (prefix, local) = match split_pos {
            Some(pos) => {
                let prefix = &iri[..=pos]; // Include the delimiter
                let local = &iri[pos + 1..];
                (prefix, local)
            }
            None => {
                // No delimiter found, use empty prefix
                ("", iri)
            }
        };

        let code = self.get_or_allocate(prefix);
        Sid::new(code, local)
    }

    /// Create a Sid for a blank node
    ///
    /// Blank nodes use the predefined namespace code 24 (_:) and generate
    /// a unique local name in the format: `fdb-{unique_id}`
    ///
    /// The `unique_id` should be globally unique (e.g., timestamp + random suffix).
    /// The returned IRI will look like `_:fdb-1672531200000-a1b2c3d4`.
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{}-{}", BLANK_NODE_ID_PREFIX, unique_id);
        Sid::new(NS_BLANK_NODE, local)
    }
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a unique blank node ID using ULID
///
/// Returns a ULID string. Combined with the `fdb` prefix, this creates
/// IDs like: `_:fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV`
///
/// ULIDs are:
/// - Lexicographically sortable (timestamp-based prefix)
/// - 128-bit compatible with UUID
/// - Case insensitive (canonical is uppercase)
/// - URL-safe (no special characters)
pub fn generate_blank_node_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Check if an IRI is a Fluree-generated blank node ID
pub fn is_blank_node_id(iri: &str) -> bool {
    iri.starts_with("_:fdb")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predefined_codes() {
        let registry = NamespaceRegistry::new();

        // Check predefined codes are present
        assert_eq!(registry.get_code(""), Some(NS_EMPTY));
        assert_eq!(registry.get_code("@"), Some(NS_JSONLD));
        assert_eq!(
            registry.get_code("http://www.w3.org/2001/XMLSchema#"),
            Some(NS_XSD)
        );
        assert_eq!(registry.get_code("_:"), Some(NS_BLANK_NODE));

        // Check reverse lookup
        assert_eq!(registry.get_prefix(NS_BLANK_NODE), Some("_:"));
        assert_eq!(registry.get_prefix(NS_XSD), Some("http://www.w3.org/2001/XMLSchema#"));
    }

    #[test]
    fn test_blank_node_code_is_fixed() {
        let registry = NamespaceRegistry::new();
        assert_eq!(registry.blank_node_code(), NS_BLANK_NODE);
        assert_eq!(registry.blank_node_code(), 24);
    }

    #[test]
    fn test_allocate_new_code() {
        let mut registry = NamespaceRegistry::new();

        let code1 = registry.get_or_allocate("http://example.org/");
        let code2 = registry.get_or_allocate("http://other.org/");
        let code1_again = registry.get_or_allocate("http://example.org/");

        assert_eq!(code1, code1_again);
        assert_ne!(code1, code2);
        assert!(code1 >= USER_NS_START);
        assert!(code2 >= USER_NS_START);
        assert_eq!(registry.delta.len(), 2);
    }

    #[test]
    fn test_blank_node_sid() {
        let registry = NamespaceRegistry::new();

        // Test with a ULID-style ID
        let sid = registry.blank_node_sid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(sid.namespace_code, NS_BLANK_NODE);
        assert_eq!(sid.name.as_ref(), "fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }

    #[test]
    fn test_is_blank_node_id() {
        assert!(is_blank_node_id("_:fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(is_blank_node_id("_:fdb"));
        assert!(!is_blank_node_id("_:b1"));
        assert!(!is_blank_node_id("http://example.org/thing"));
    }

    #[test]
    fn test_sid_for_iri() {
        let mut registry = NamespaceRegistry::new();

        let sid1 = registry.sid_for_iri("http://example.org/Person");
        let sid2 = registry.sid_for_iri("http://example.org/name");

        // Same prefix, should have same code
        assert_eq!(sid1.namespace_code, sid2.namespace_code);
        assert_eq!(sid1.name.as_ref(), "Person");
        assert_eq!(sid2.name.as_ref(), "name");
    }

    #[test]
    fn test_sid_for_iri_uses_longest_prefix_match() {
        let mut registry = NamespaceRegistry::new();

        // did:key: is a predefined prefix (code 11), so did:key:z6Mk... should use it
        let sid = registry.sid_for_iri("did:key:z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");
        assert_eq!(sid.namespace_code, NS_DID_KEY);
        assert_eq!(sid.name.as_ref(), "z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");

        // fluree:db:sha256: is predefined (code 10)
        let sid2 = registry.sid_for_iri("fluree:db:sha256:abc123");
        assert_eq!(sid2.namespace_code, NS_FLUREE_DB);
        assert_eq!(sid2.name.as_ref(), "abc123");

        // XSD namespace ends with #, should still work
        let sid3 = registry.sid_for_iri("http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(sid3.namespace_code, NS_XSD);
        assert_eq!(sid3.name.as_ref(), "string");

        // No delta should be created since all prefixes are predefined
        assert!(registry.delta.is_empty());
    }

    #[test]
    fn test_take_delta() {
        let mut registry = NamespaceRegistry::new();

        registry.get_or_allocate("http://example.org/");
        registry.get_or_allocate("http://other.org/");

        let delta = registry.take_delta();
        assert_eq!(delta.len(), 2);
        assert!(registry.delta.is_empty());
    }

    #[test]
    fn test_generate_blank_node_id() {
        let id1 = generate_blank_node_id();
        let id2 = generate_blank_node_id();

        // ULIDs are 26 characters (Crockford Base32)
        assert_eq!(id1.len(), 26);
        assert_eq!(id2.len(), 26);

        // IDs should be different (ULIDs include random component)
        assert_ne!(id1, id2);

        // Should be valid ULIDs (parseable)
        assert!(ulid::Ulid::from_string(&id1).is_ok());
        assert!(ulid::Ulid::from_string(&id2).is_ok());
    }
}
