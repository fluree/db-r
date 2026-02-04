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
//! with existing databases. User-supplied namespaces start at `USER_START`.

use fluree_db_core::{Db, Sid, Storage};
use fluree_vocab::namespaces::{BLANK_NODE, OVERFLOW, USER_START};
use std::collections::HashMap;

/// First code available for user-defined namespaces.
/// Re-exported from `fluree_vocab::namespaces::USER_START`.
pub const USER_NS_START: u16 = USER_START;

/// Blank node prefix (standard RDF blank node syntax)
pub const BLANK_NODE_PREFIX: &str = "_:";

/// Fluree blank node ID prefix (used in generated blank node names)
pub const BLANK_NODE_ID_PREFIX: &str = "fdb";

// ============================================================================
// Prefix Trie — O(len(iri)) longest-prefix matching
// ============================================================================

/// A node in the byte-level prefix trie.
///
/// Children are stored as a sorted `Vec<(u8, u32)>` instead of a HashMap.
/// Most nodes in URI prefix tries have 1-3 children, where a linear scan
/// beats HashMap's hashing + heap allocation overhead. The `u32` index
/// supports up to 4B nodes (far more than needed).
#[derive(Debug, Clone)]
struct TrieNode {
    /// Namespace code if a registered prefix ends at this node.
    code: Option<u16>,
    /// Children sorted by byte value. Typically 1-3 entries for URI prefixes.
    children: Vec<(u8, u32)>,
}

/// Byte-level trie for longest-prefix matching of IRI strings.
///
/// Each registered namespace prefix is inserted byte-by-byte. Lookup walks
/// the trie following the IRI's bytes, tracking the deepest node that has a
/// namespace code set. This gives O(len(iri)) lookup time independent of the
/// number of registered prefixes — critical when namespace counts grow to
/// thousands (e.g. DBLP imports with ~90K namespaces).
#[derive(Debug, Clone)]
struct PrefixTrie {
    nodes: Vec<TrieNode>,
}

impl PrefixTrie {
    fn new() -> Self {
        Self {
            nodes: vec![TrieNode {
                code: None,
                children: Vec::new(),
            }],
        }
    }

    /// Insert a prefix string with its namespace code.
    fn insert(&mut self, prefix: &str, code: u16) {
        let mut node_idx: u32 = 0;
        for &byte in prefix.as_bytes() {
            let children = &self.nodes[node_idx as usize].children;
            node_idx = match children.iter().find(|(b, _)| *b == byte) {
                Some(&(_, child_idx)) => child_idx,
                None => {
                    let new_idx = self.nodes.len() as u32;
                    self.nodes.push(TrieNode {
                        code: None,
                        children: Vec::new(),
                    });
                    let node = &mut self.nodes[node_idx as usize];
                    let pos = node.children.partition_point(|(b, _)| *b < byte);
                    node.children.insert(pos, (byte, new_idx));
                    new_idx
                }
            };
        }
        self.nodes[node_idx as usize].code = Some(code);
    }

    /// Find the longest registered prefix that matches the start of `iri`.
    ///
    /// Returns `(namespace_code, prefix_byte_length)` or `None` if no
    /// non-empty prefix matches. The empty prefix (code 0) is intentionally
    /// not stored in the trie — unmatched IRIs fall through to the split
    /// heuristic in `sid_for_iri`.
    fn longest_match(&self, iri: &str) -> Option<(u16, usize)> {
        let mut node_idx: u32 = 0;
        let mut best: Option<(u16, usize)> = None;

        for (i, &byte) in iri.as_bytes().iter().enumerate() {
            let children = &self.nodes[node_idx as usize].children;
            match children.iter().find(|(b, _)| *b == byte) {
                Some(&(_, child_idx)) => {
                    node_idx = child_idx;
                    if let Some(code) = self.nodes[node_idx as usize].code {
                        best = Some((code, i + 1));
                    }
                }
                None => break,
            }
        }

        best
    }
}

/// Build the default namespace mappings (single source of truth: `fluree-db-core`).
fn default_namespaces() -> HashMap<u16, String> {
    fluree_db_core::default_namespace_codes()
}

/// Registry for namespace prefix codes
///
/// During transaction processing, new IRIs may introduce prefixes not yet
/// in the database's namespace table. This registry:
/// 1. Starts with predefined codes for common namespaces
/// 2. Loads existing codes from `db.namespace_codes`
/// 3. Allocates new codes as needed (starting at USER_NS_START)
/// 4. Tracks new allocations in `delta` for commit persistence
///
/// Prefix lookups use a byte-level trie for O(len(iri)) longest-prefix
/// matching, independent of the number of registered namespaces.
#[derive(Debug, Clone)]
pub struct NamespaceRegistry {
    /// Prefix → code mapping (for exact lookups in get_or_allocate)
    codes: HashMap<String, u16>,

    /// Code → prefix mapping (for encoding, matches Db.namespace_codes)
    names: HashMap<u16, String>,

    /// Next available code for allocation (>= USER_NS_START)
    next_code: u16,

    /// New allocations this transaction (code → prefix)
    pub(crate) delta: HashMap<u16, String>,

    /// Byte-level trie for O(len(iri)) longest-prefix matching.
    /// The empty prefix (code 0) is NOT stored in the trie — unmatched
    /// IRIs fall through to the split heuristic which may allocate it.
    trie: PrefixTrie,
}

impl NamespaceRegistry {
    /// Create a new registry with default namespaces
    pub fn new() -> Self {
        let names = default_namespaces();
        let codes: HashMap<String, u16> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        let mut trie = PrefixTrie::new();
        for (prefix, &code) in &codes {
            if !prefix.is_empty() {
                trie.insert(prefix, code);
            }
        }

        Self {
            codes,
            names,
            next_code: USER_NS_START,
            delta: HashMap::new(),
            trie,
        }
    }

    /// Create a registry seeded from a database's namespace codes
    ///
    /// This merges the database's codes with the predefined defaults,
    /// with the database taking precedence for any conflicts.
    pub fn from_db<S: Storage>(db: &Db<S>) -> Self {
        // Start with defaults
        let mut names = default_namespaces();

        // Merge in database codes (overwriting defaults if needed)
        for (code, prefix) in &db.namespace_codes {
            names.insert(*code, prefix.clone());
        }

        let codes: HashMap<String, u16> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        // Next code is max of existing non-sentinel codes + 1, but at least USER_NS_START.
        // Exclude OVERFLOW from max_code since it's a sentinel, not an allocated code.
        let max_code = names
            .keys()
            .filter(|&&c| c < OVERFLOW)
            .max()
            .copied()
            .unwrap_or(0);
        let next_code = (max_code + 1).max(USER_NS_START);

        let mut trie = PrefixTrie::new();
        for (prefix, &code) in &codes {
            if !prefix.is_empty() {
                trie.insert(prefix, code);
            }
        }

        Self {
            codes,
            names,
            next_code,
            delta: HashMap::new(),
            trie,
        }
    }

    /// Get the code for a prefix, allocating a new one if needed.
    ///
    /// If the prefix is not yet registered, a new code is allocated
    /// and recorded in the delta for persistence. If all codes in
    /// `USER_START..OVERFLOW` are exhausted, returns `OVERFLOW` as a
    /// pure sentinel — OVERFLOW is never inserted into codes/names/delta/trie.
    /// The caller (`sid_for_iri`) handles overflow by using the full IRI as
    /// the SID name.
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        if let Some(&code) = self.codes.get(prefix) {
            return code;
        }

        // All codes exhausted — return OVERFLOW sentinel without registering.
        // The caller will use the full IRI as the SID name.
        if self.next_code >= OVERFLOW {
            return OVERFLOW;
        }

        let code = self.next_code;
        self.next_code += 1;

        self.codes.insert(prefix.to_string(), code);
        self.names.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());

        // Insert into trie (skip empty prefix — handled by split heuristic)
        if !prefix.is_empty() {
            self.trie.insert(prefix, code);
        }

        code
    }

    /// Get the namespace code for blank nodes.
    pub fn blank_node_code(&self) -> u16 {
        BLANK_NODE
    }

    /// Look up a code without allocating
    pub fn get_code(&self, prefix: &str) -> Option<u16> {
        self.codes.get(prefix).copied()
    }

    /// Look up a prefix by code
    pub fn get_prefix(&self, code: u16) -> Option<&str> {
        self.names.get(&code).map(|s| s.as_str())
    }

    /// Check if a prefix is registered
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.codes.contains_key(prefix)
    }

    /// Number of registered namespace codes (predefined + user-allocated).
    pub fn code_count(&self) -> usize {
        self.codes.len()
    }

    /// Take the delta (new allocations) and reset it
    ///
    /// Returns the map of new allocations (code → prefix) for
    /// inclusion in the commit record.
    pub fn take_delta(&mut self) -> HashMap<u16, String> {
        std::mem::take(&mut self.delta)
    }

    /// Get a reference to the delta without consuming it
    pub fn delta(&self) -> &HashMap<u16, String> {
        &self.delta
    }

    /// Check if there are any new allocations
    pub fn has_delta(&self) -> bool {
        !self.delta.is_empty()
    }

    /// Create a Sid for an IRI, allocating namespace code if needed
    ///
    /// Uses a byte-level trie for O(len(iri)) longest-prefix matching,
    /// independent of the number of registered namespaces. Falls back to
    /// splitting on the last `/` or `#` for IRIs with no matching prefix.
    ///
    /// When namespace codes are exhausted (`get_or_allocate` returns `OVERFLOW`),
    /// the full IRI is used as the SID name — no prefix splitting.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        // Trie lookup: O(len(iri)), handles nested prefixes correctly
        if let Some((code, prefix_len)) = self.trie.longest_match(iri) {
            return Sid::new(code, &iri[prefix_len..]);
        }

        // No known prefix matched; fall back to split heuristic for new prefixes.
        let split_pos = iri.rfind(|c| c == '/' || c == '#');
        let (prefix, local) = match split_pos {
            Some(pos) => (&iri[..=pos], &iri[pos + 1..]),
            None => ("", iri),
        };

        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            // Overflow: store the full IRI as name, no prefix splitting
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, local)
        }
    }

    /// Register a namespace code if not already present.
    ///
    /// Used to merge allocations from a parallel parser clone back into the
    /// main registry. If the prefix is already registered (under any code),
    /// this is a no-op. OVERFLOW codes are ignored since they are pure
    /// sentinels and should never be registered.
    pub fn ensure_code(&mut self, code: u16, prefix: &str) {
        if code >= OVERFLOW {
            return; // OVERFLOW is a sentinel, never register it
        }
        if self.codes.contains_key(prefix) {
            return;
        }
        self.codes.insert(prefix.to_string(), code);
        self.names.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());
        if !prefix.is_empty() {
            self.trie.insert(prefix, code);
        }
        if code >= self.next_code {
            self.next_code = code + 1;
        }
    }

    /// Create a Sid for a blank node.
    ///
    /// Blank nodes use the predefined `BLANK_NODE` namespace code and generate
    /// a unique local name in the format: `fdb-{unique_id}`
    ///
    /// The `unique_id` should be globally unique (e.g., timestamp + random suffix).
    /// The returned IRI will look like `_:fdb-1672531200000-a1b2c3d4`.
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{}-{}", BLANK_NODE_ID_PREFIX, unique_id);
        Sid::new(BLANK_NODE, local)
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
    use fluree_vocab::namespaces::{DID_KEY, EMPTY, JSON_LD, XSD};

    #[test]
    fn test_predefined_codes() {
        let registry = NamespaceRegistry::new();

        // Check predefined codes are present
        assert_eq!(registry.get_code(""), Some(EMPTY));
        assert_eq!(registry.get_code("@"), Some(JSON_LD));
        assert_eq!(
            registry.get_code("http://www.w3.org/2001/XMLSchema#"),
            Some(XSD)
        );
        assert_eq!(registry.get_code("_:"), Some(BLANK_NODE));

        // Check reverse lookup
        assert_eq!(registry.get_prefix(BLANK_NODE), Some("_:"));
        assert_eq!(
            registry.get_prefix(XSD),
            Some("http://www.w3.org/2001/XMLSchema#")
        );
    }

    #[test]
    fn test_blank_node_code_is_fixed() {
        let registry = NamespaceRegistry::new();
        assert_eq!(registry.blank_node_code(), BLANK_NODE);
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
        assert_eq!(sid.namespace_code, BLANK_NODE);
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

        // did:key: is a predefined prefix, so did:key:z6Mk... should use it
        let sid = registry.sid_for_iri("did:key:z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");
        assert_eq!(sid.namespace_code, DID_KEY);
        assert_eq!(sid.name.as_ref(), "z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");

        // XSD namespace ends with #, should still work
        let sid3 = registry.sid_for_iri("http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(sid3.namespace_code, XSD);
        assert_eq!(sid3.name.as_ref(), "string");

        // No delta should be created since all prefixes are predefined
        assert!(registry.delta.is_empty());
    }

    #[test]
    fn test_nested_prefixes_use_longest_match() {
        let mut registry = NamespaceRegistry::new();

        // Register a short prefix, then a longer nested prefix
        let short_code = registry.get_or_allocate("http://ex.org/");
        let long_code = registry.get_or_allocate("http://ex.org/foo/");
        assert_ne!(short_code, long_code);

        // IRI matching only the short prefix
        let sid1 = registry.sid_for_iri("http://ex.org/bar");
        assert_eq!(sid1.namespace_code, short_code);
        assert_eq!(sid1.name.as_ref(), "bar");

        // IRI matching BOTH — must use the longer prefix
        let sid2 = registry.sid_for_iri("http://ex.org/foo/bar");
        assert_eq!(sid2.namespace_code, long_code);
        assert_eq!(sid2.name.as_ref(), "bar");

        // Call again — trie always finds longest match, no cache issues
        let sid3 = registry.sid_for_iri("http://ex.org/foo/baz");
        assert_eq!(sid3.namespace_code, long_code);
        assert_eq!(sid3.name.as_ref(), "baz");

        // And the short prefix still works correctly after
        let sid4 = registry.sid_for_iri("http://ex.org/qux");
        assert_eq!(sid4.namespace_code, short_code);
        assert_eq!(sid4.name.as_ref(), "qux");
    }

    #[test]
    fn test_nested_prefixes_registered_in_reverse_order() {
        let mut registry = NamespaceRegistry::new();

        // Register LONG prefix first, then the short one
        let long_code = registry.get_or_allocate("http://ex.org/foo/");
        let short_code = registry.get_or_allocate("http://ex.org/");
        assert_ne!(short_code, long_code);

        // Warm the cache with the short prefix
        let sid1 = registry.sid_for_iri("http://ex.org/bar");
        assert_eq!(sid1.namespace_code, short_code);

        // Must still find the long prefix even though short was just used
        let sid2 = registry.sid_for_iri("http://ex.org/foo/bar");
        assert_eq!(sid2.namespace_code, long_code);
        assert_eq!(sid2.name.as_ref(), "bar");
    }

    #[test]
    fn test_trie_basic() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://example.org/", 101);
        trie.insert("http://other.org/", 102);

        assert_eq!(
            trie.longest_match("http://example.org/foo"),
            Some((101, 19))
        );
        assert_eq!(
            trie.longest_match("http://other.org/bar"),
            Some((102, 17))
        );
        assert_eq!(trie.longest_match("http://unknown.org/baz"), None);
    }

    #[test]
    fn test_trie_nested_prefixes() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://ex.org/", 101);
        trie.insert("http://ex.org/foo/", 102);

        // Short prefix only
        assert_eq!(trie.longest_match("http://ex.org/bar"), Some((101, 14)));
        // Longest match wins
        assert_eq!(
            trie.longest_match("http://ex.org/foo/bar"),
            Some((102, 18))
        );
    }

    #[test]
    fn test_trie_nested_reverse_insertion() {
        let mut trie = PrefixTrie::new();
        // Insert long first, then short
        trie.insert("http://ex.org/foo/", 102);
        trie.insert("http://ex.org/", 101);

        // Still finds longest match
        assert_eq!(
            trie.longest_match("http://ex.org/foo/bar"),
            Some((102, 18))
        );
        assert_eq!(trie.longest_match("http://ex.org/bar"), Some((101, 14)));
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
