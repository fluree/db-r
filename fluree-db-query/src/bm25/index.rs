//! BM25 Index Data Structures
//!
//! This module defines the core data structures for BM25 full-text search
//! with Clojure parity. Key design decisions:
//!
//! - `DocKey` uses canonical IRI strings (not `Sid`) for multi-ledger safety
//! - Sparse vector representation for term frequencies
//! - Per-ledger watermarks for multi-source graph sources

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::Flake;
use serde::{Deserialize, Serialize};

// ============================================================================
// Document Identity
// ============================================================================

/// Document key for BM25 index entries.
///
/// Uses canonical IRI string (not `Sid`) because `Sid` values are ledger-local:
/// two ledgers can encode the same IRI to different Sids. Cross-ledger identity
/// requires the canonical IRI. Convert to/from `Sid` only at ledger query boundaries.
///
/// Implements `Ord` to support deterministic serialization via `BTreeMap`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocKey {
    /// Ledger alias including branch (e.g., "source-ledger:main")
    pub ledger_alias: Arc<str>,
    /// Canonical IRI string for the subject
    pub subject_iri: Arc<str>,
}

impl DocKey {
    /// Create a new document key
    pub fn new(ledger_alias: impl Into<Arc<str>>, subject_iri: impl Into<Arc<str>>) -> Self {
        Self {
            ledger_alias: ledger_alias.into(),
            subject_iri: subject_iri.into(),
        }
    }
}

// ============================================================================
// Sparse Vector Representation
// ============================================================================

/// Sparse vector for document term frequencies.
///
/// Stores only non-zero term frequencies as (term_idx, tf) pairs,
/// sorted by term_idx for efficient intersection operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SparseVector {
    /// (term_idx, term_frequency) pairs, sorted by term_idx
    pub entries: Vec<(u32, u32)>,
}

impl SparseVector {
    /// Create a new empty sparse vector
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create a sparse vector from (term_idx, tf) pairs.
    /// The entries will be sorted by term_idx.
    pub fn from_entries(mut entries: Vec<(u32, u32)>) -> Self {
        entries.sort_by_key(|(idx, _)| *idx);
        Self { entries }
    }

    /// Get the term frequency for a given term index, or 0 if not present
    pub fn get(&self, term_idx: u32) -> u32 {
        match self
            .entries
            .binary_search_by_key(&term_idx, |(idx, _)| *idx)
        {
            Ok(pos) => self.entries[pos].1,
            Err(_) => 0,
        }
    }

    /// Number of unique terms in the document
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the vector has no terms
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total number of terms in the document (sum of all term frequencies)
    pub fn doc_length(&self) -> u32 {
        self.entries.iter().map(|(_, tf)| tf).sum()
    }

    /// Iterate over (term_idx, tf) pairs
    pub fn iter(&self) -> impl Iterator<Item = &(u32, u32)> {
        self.entries.iter()
    }
}

// ============================================================================
// Term Index Entry
// ============================================================================

/// Entry in the term index.
///
/// Maps a term to its global index and tracks document frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermEntry {
    /// Global index for this term (used in sparse vectors)
    pub idx: u32,
    /// Number of documents containing this term
    pub doc_freq: u32,
}

impl TermEntry {
    /// Create a new term entry with the given index
    pub fn new(idx: u32) -> Self {
        Self { idx, doc_freq: 0 }
    }

    /// Increment the document frequency
    pub fn inc_doc_freq(&mut self) {
        self.doc_freq += 1;
    }

    /// Decrement the document frequency (for document removal)
    pub fn dec_doc_freq(&mut self) {
        self.doc_freq = self.doc_freq.saturating_sub(1);
    }
}

// ============================================================================
// BM25 Configuration
// ============================================================================

/// BM25 scoring parameters.
///
/// Default values match Clojure implementation:
/// - k1 = 1.2 (term frequency saturation)
/// - b = 0.75 (document length normalization)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bm25Config {
    /// Term frequency saturation parameter (default: 1.2)
    pub k1: f64,
    /// Document length normalization parameter (default: 0.75)
    pub b: f64,
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl Bm25Config {
    /// Create a new BM25 config with custom parameters
    pub fn new(k1: f64, b: f64) -> Self {
        Self { k1, b }
    }
}

// ============================================================================
// Corpus Statistics
// ============================================================================

/// Statistics about the indexed corpus.
///
/// Used for BM25 IDF calculation and document length normalization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bm25Stats {
    /// Total number of documents in the index
    pub num_docs: u64,
    /// Total number of terms across all documents
    pub total_terms: u64,
}

impl Bm25Stats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Average document length
    pub fn avg_doc_len(&self) -> f64 {
        if self.num_docs == 0 {
            0.0
        } else {
            self.total_terms as f64 / self.num_docs as f64
        }
    }

    /// Add a document with the given length to the statistics
    pub fn add_doc(&mut self, doc_len: u32) {
        self.num_docs += 1;
        self.total_terms += doc_len as u64;
    }

    /// Remove a document with the given length from the statistics
    pub fn remove_doc(&mut self, doc_len: u32) {
        self.num_docs = self.num_docs.saturating_sub(1);
        self.total_terms = self.total_terms.saturating_sub(doc_len as u64);
    }
}

// ============================================================================
// Watermarks (Multi-Ledger Support)
// ============================================================================

/// Watermark tracking for multi-ledger graph sources.
///
/// BM25 is valid at `T` iff ALL dependency ledgers have watermark >= T.
///
/// Uses `BTreeMap` for deterministic serialization (content-addressable snapshots).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphSourceWatermark {
    /// Per-ledger watermarks (ledger_alias -> commit_t)
    pub ledger_watermarks: BTreeMap<String, i64>,
}

impl GraphSourceWatermark {
    /// Create a new empty watermark tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with initial ledger watermarks
    pub fn with_watermarks(watermarks: BTreeMap<String, i64>) -> Self {
        Self {
            ledger_watermarks: watermarks,
        }
    }

    /// Effective t for "as-of" queries (minimum of all ledgers)
    pub fn effective_t(&self) -> i64 {
        self.ledger_watermarks.values().copied().min().unwrap_or(0)
    }

    /// Check if graph source can answer query at target_t
    pub fn is_valid_at(&self, target_t: i64) -> bool {
        self.ledger_watermarks.values().all(|&t| t >= target_t)
    }

    /// Update watermark for a specific ledger
    pub fn update(&mut self, ledger_alias: &str, t: i64) {
        self.ledger_watermarks.insert(ledger_alias.to_string(), t);
    }

    /// Get watermark for a specific ledger
    pub fn get(&self, ledger_alias: &str) -> Option<i64> {
        self.ledger_watermarks.get(ledger_alias).copied()
    }
}

// ============================================================================
// Property Dependencies
// ============================================================================

/// Property dependencies for incremental updates.
///
/// Stores IRIs (config portable), compiled to SIDs per-ledger at runtime
/// for fast flake filtering.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PropertyDeps {
    /// IRIs of properties that trigger reindexing (config-level)
    pub property_iris: HashSet<Arc<str>>,
}

impl PropertyDeps {
    /// Create new empty property dependencies
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with initial property IRIs
    pub fn with_iris(iris: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Self {
        Self {
            property_iris: iris.into_iter().map(Into::into).collect(),
        }
    }

    /// Add a property IRI
    pub fn add(&mut self, iri: impl Into<Arc<str>>) {
        self.property_iris.insert(iri.into());
    }

    /// Check if a property IRI is tracked
    pub fn contains(&self, iri: &str) -> bool {
        self.property_iris.iter().any(|i| i.as_ref() == iri)
    }

    /// Number of tracked properties
    pub fn len(&self) -> usize {
        self.property_iris.len()
    }

    /// Check if no properties are tracked
    pub fn is_empty(&self) -> bool {
        self.property_iris.is_empty()
    }

    /// Extract property dependencies from a BM25 indexing query configuration.
    ///
    /// The indexing query format is:
    /// ```json
    /// {
    ///   "ledger": "docs",
    ///   "query": {
    ///     "@context": {"ex": "http://example.org/"},
    ///     "where": [{"@id": "?x", "@type": "ex:Article"}],
    ///     "select": {"?x": ["@id", "ex:title", "ex:content"]}
    ///   }
    /// }
    /// ```
    ///
    /// Extracts property IRIs from:
    /// - WHERE clause patterns (predicate keys like "@type", "ex:title")
    /// - SELECT clause property arrays (excluding "@id")
    ///
    /// The `@context` is used to expand prefixed IRIs to full IRIs.
    pub fn from_indexing_query(config: &serde_json::Value) -> Self {
        let mut deps = PropertyDeps::new();

        // Extract @context for prefix expansion
        let query = config.get("query").unwrap_or(config);
        let context = query.get("@context");

        // Helper to expand a prefixed IRI using context
        let expand_iri = |key: &str| -> Option<Arc<str>> {
            // Variables are not predicates.
            if key.starts_with('?') {
                return None;
            }

            // JSON-LD @type is rdf:type, and must be tracked for incremental updates.
            if key == "@type" {
                return Some(Arc::from(fluree_vocab::rdf::TYPE));
            }

            // Other JSON-LD keywords are not tracked as predicates.
            if key.starts_with('@') {
                return None;
            }

            // Handle prefixed IRIs (e.g., "ex:title")
            if let Some(colon_pos) = key.find(':') {
                let prefix = &key[..colon_pos];
                let local = &key[colon_pos + 1..];

                // Look up prefix in context
                if let Some(ctx) = context {
                    if let Some(base) = ctx.get(prefix).and_then(|v| v.as_str()) {
                        return Some(Arc::from(format!("{}{}", base, local)));
                    }
                }
            }

            // Already a full IRI or no context match - use as-is
            Some(Arc::from(key))
        };

        // Extract properties from WHERE clause patterns
        if let Some(where_clause) = query.get("where") {
            Self::extract_where_properties(where_clause, &expand_iri, &mut deps);
        }

        // Extract properties from SELECT clause
        if let Some(select) = query.get("select") {
            Self::extract_select_properties(select, &expand_iri, &mut deps);
        }

        deps
    }

    /// Extract properties from WHERE clause patterns (recursive)
    fn extract_where_properties<F>(
        value: &serde_json::Value,
        expand_iri: &F,
        deps: &mut PropertyDeps,
    ) where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        match value {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    Self::extract_where_properties(item, expand_iri, deps);
                }
            }
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    // Extract property IRI from predicate key
                    if let Some(iri) = expand_iri(key) {
                        deps.add(iri);
                    }
                    // Recurse into nested patterns
                    Self::extract_where_properties(val, expand_iri, deps);
                }
            }
            _ => {}
        }
    }

    /// Extract properties from SELECT clause
    fn extract_select_properties<F>(
        value: &serde_json::Value,
        expand_iri: &F,
        deps: &mut PropertyDeps,
    ) where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        match value {
            // SELECT as object: {"?x": ["@id", "ex:title", "ex:content"]}
            serde_json::Value::Object(map) => {
                for (_var, props) in map {
                    if let serde_json::Value::Array(arr) = props {
                        for prop in arr {
                            if let Some(s) = prop.as_str() {
                                if let Some(iri) = expand_iri(s) {
                                    deps.add(iri);
                                }
                            }
                        }
                    }
                }
            }
            // SELECT as array of property strings
            serde_json::Value::Array(arr) => {
                for prop in arr {
                    if let Some(s) = prop.as_str() {
                        if let Some(iri) = expand_iri(s) {
                            deps.add(iri);
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

// ============================================================================
// Compiled Property Dependencies (Per-Ledger)
// ============================================================================

/// Compiled property dependencies for a specific ledger.
///
/// Converts IRI-based `PropertyDeps` to SID-based for efficient flake filtering.
/// This allows O(1) lookup when checking if a flake's predicate triggers reindexing.
#[derive(Debug, Clone, Default)]
pub struct CompiledPropertyDeps {
    /// Predicate SIDs that trigger reindexing for this ledger
    pub predicate_sids: HashSet<fluree_db_core::Sid>,
}

impl CompiledPropertyDeps {
    /// Create new empty compiled deps
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile PropertyDeps to SIDs using a ledger's namespace encoding.
    ///
    /// IRIs that cannot be encoded are silently skipped (they don't exist in
    /// this ledger's namespace table yet).
    pub fn compile<F>(deps: &PropertyDeps, encode_iri: F) -> Self
    where
        F: Fn(&str) -> Option<fluree_db_core::Sid>,
    {
        let predicate_sids = deps
            .property_iris
            .iter()
            .filter_map(|iri| encode_iri(iri.as_ref()))
            .collect();
        Self { predicate_sids }
    }

    /// Check if a predicate SID triggers reindexing
    pub fn contains(&self, sid: &fluree_db_core::Sid) -> bool {
        self.predicate_sids.contains(sid)
    }

    /// Number of compiled predicates
    pub fn len(&self) -> usize {
        self.predicate_sids.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.predicate_sids.is_empty()
    }

    /// Find subjects affected by a set of flakes.
    ///
    /// Filters flakes by predicate SID (O(1) lookup) and collects unique subject SIDs.
    /// This is the core of incremental update detection: given the flakes from a commit,
    /// determine which subjects need to be reindexed.
    ///
    /// # Arguments
    ///
    /// * `flakes` - Flakes from a commit (typically from novelty or commit delta)
    ///
    /// # Returns
    ///
    /// Set of subject SIDs that have changes to tracked properties.
    pub fn affected_subjects(&self, flakes: &[Flake]) -> HashSet<fluree_db_core::Sid> {
        flakes
            .iter()
            .filter(|f| self.predicate_sids.contains(&f.p))
            .map(|f| f.s.clone())
            .collect()
    }

    /// Find subjects affected by a set of flakes, filtering by transaction time range.
    ///
    /// Like `affected_subjects`, but only considers flakes with `from_t < t <= to_t`.
    /// This is useful for catch-up scenarios where we need to reindex for a range of commits.
    ///
    /// # Arguments
    ///
    /// * `flakes` - Flakes to filter
    /// * `from_t` - Exclusive lower bound (flakes at this t are NOT included)
    /// * `to_t` - Inclusive upper bound (flakes at this t ARE included)
    ///
    /// # Returns
    ///
    /// Set of subject SIDs that have changes to tracked properties in the time range.
    pub fn affected_subjects_in_range(
        &self,
        flakes: &[Flake],
        from_t: i64,
        to_t: i64,
    ) -> HashSet<fluree_db_core::Sid> {
        flakes
            .iter()
            .filter(|f| f.t > from_t && f.t <= to_t)
            .filter(|f| self.predicate_sids.contains(&f.p))
            .map(|f| f.s.clone())
            .collect()
    }
}

// ============================================================================
// Main BM25 Index
// ============================================================================

/// BM25 Full-Text Search Index
///
/// Main index structure containing:
/// - Term dictionary mapping terms to global indices
/// - Document vectors (sparse tf representations)
/// - Corpus statistics for scoring
/// - Multi-ledger watermarks
/// - Property dependencies for incremental updates
///
/// Uses `BTreeMap` for deterministic serialization (content-addressable snapshots).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bm25Index {
    /// Term dictionary: term -> TermEntry (BTreeMap for deterministic order)
    pub terms: BTreeMap<Arc<str>, TermEntry>,
    /// Document vectors: DocKey -> SparseVector (BTreeMap for deterministic order)
    pub doc_vectors: BTreeMap<DocKey, SparseVector>,
    /// Corpus statistics
    pub stats: Bm25Stats,
    /// BM25 configuration parameters
    pub config: Bm25Config,
    /// Multi-ledger watermarks
    pub watermark: GraphSourceWatermark,
    /// Property dependencies for incremental updates
    pub property_deps: PropertyDeps,
    /// Next term index to allocate
    next_term_idx: u32,
}

impl Default for Bm25Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Bm25Index {
    /// Create a new empty BM25 index with default configuration
    pub fn new() -> Self {
        Self::with_config(Bm25Config::default())
    }

    /// Create a new empty BM25 index with custom configuration
    pub fn with_config(config: Bm25Config) -> Self {
        Self {
            terms: BTreeMap::new(),
            doc_vectors: BTreeMap::new(),
            stats: Bm25Stats::new(),
            config,
            watermark: GraphSourceWatermark::new(),
            property_deps: PropertyDeps::new(),
            next_term_idx: 0,
        }
    }

    /// Get or create a term entry, returning its global index
    pub fn get_or_create_term(&mut self, term: &str) -> u32 {
        if let Some(entry) = self.terms.get(term) {
            return entry.idx;
        }

        let idx = self.next_term_idx;
        self.next_term_idx += 1;

        self.terms.insert(Arc::from(term), TermEntry::new(idx));

        idx
    }

    /// Get the term entry for a given term (if it exists)
    pub fn get_term(&self, term: &str) -> Option<&TermEntry> {
        self.terms.get(term)
    }

    /// Get the global index for a term (if it exists)
    pub fn term_idx(&self, term: &str) -> Option<u32> {
        self.terms.get(term).map(|e| e.idx)
    }

    /// Add a document to the index with pre-computed term frequencies.
    ///
    /// `term_freqs` maps term strings to their frequency in the document.
    pub fn add_document(&mut self, doc_key: DocKey, term_freqs: HashMap<&str, u32>) {
        // Convert terms to indices and build sparse vector
        let mut entries: Vec<(u32, u32)> = Vec::with_capacity(term_freqs.len());

        for (term, tf) in term_freqs {
            let idx = self.get_or_create_term(term);
            entries.push((idx, tf));

            // Update document frequency for this term
            if let Some(entry) = self.terms.get_mut(term) {
                entry.inc_doc_freq();
            }
        }

        // Sort entries by term index
        entries.sort_by_key(|(idx, _)| *idx);

        let vec = SparseVector { entries };
        let doc_len = vec.doc_length();

        // Update corpus statistics
        self.stats.add_doc(doc_len);

        // Store document vector
        self.doc_vectors.insert(doc_key, vec);
    }

    /// Upsert a document: if it exists, remove it first to maintain correct stats.
    ///
    /// This is the preferred method for incremental updates where a document's
    /// content may have changed. It ensures:
    /// - Document frequency counts remain accurate
    /// - Corpus statistics (num_docs, total_terms) remain accurate
    ///
    /// Returns `true` if this was an update (document existed), `false` if insert.
    pub fn upsert_document(&mut self, doc_key: DocKey, term_freqs: HashMap<&str, u32>) -> bool {
        let was_update = self.remove_document(&doc_key);
        self.add_document(doc_key, term_freqs);
        was_update
    }

    /// Remove a document from the index
    pub fn remove_document(&mut self, doc_key: &DocKey) -> bool {
        if let Some(vec) = self.doc_vectors.remove(doc_key) {
            let doc_len = vec.doc_length();
            self.stats.remove_doc(doc_len);

            // Update document frequencies for terms in this document
            for (term_idx, _) in vec.entries {
                // Find the term by its index (inefficient, but removal is rare)
                for entry in self.terms.values_mut() {
                    if entry.idx == term_idx {
                        entry.dec_doc_freq();
                        break;
                    }
                }
            }

            true
        } else {
            false
        }
    }

    /// Number of documents in the index
    pub fn num_docs(&self) -> u64 {
        self.stats.num_docs
    }

    /// Number of unique terms in the index
    pub fn num_terms(&self) -> usize {
        self.terms.len()
    }

    /// Check if a document exists in the index
    pub fn contains_doc(&self, doc_key: &DocKey) -> bool {
        self.doc_vectors.contains_key(doc_key)
    }

    /// Get the document vector for a document (if it exists)
    pub fn get_doc_vector(&self, doc_key: &DocKey) -> Option<&SparseVector> {
        self.doc_vectors.get(doc_key)
    }

    /// Iterate over all documents in the index
    pub fn iter_docs(&self) -> impl Iterator<Item = (&DocKey, &SparseVector)> {
        self.doc_vectors.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_key_equality() {
        let k1 = DocKey::new("ledger:main", "http://example.org/1");
        let k2 = DocKey::new("ledger:main", "http://example.org/1");
        let k3 = DocKey::new("ledger:main", "http://example.org/2");
        let k4 = DocKey::new("other:main", "http://example.org/1");

        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
    }

    #[test]
    fn test_sparse_vector_get() {
        let vec = SparseVector::from_entries(vec![(1, 3), (5, 2), (10, 1)]);

        assert_eq!(vec.get(1), 3);
        assert_eq!(vec.get(5), 2);
        assert_eq!(vec.get(10), 1);
        assert_eq!(vec.get(0), 0); // Not present
        assert_eq!(vec.get(7), 0); // Not present
    }

    #[test]
    fn test_sparse_vector_doc_length() {
        let vec = SparseVector::from_entries(vec![(1, 3), (5, 2), (10, 1)]);
        assert_eq!(vec.doc_length(), 6);
    }

    #[test]
    fn test_bm25_stats_avg_doc_len() {
        let mut stats = Bm25Stats::new();
        assert_eq!(stats.avg_doc_len(), 0.0);

        stats.add_doc(10);
        stats.add_doc(20);
        stats.add_doc(30);

        assert_eq!(stats.num_docs, 3);
        assert_eq!(stats.total_terms, 60);
        assert!((stats.avg_doc_len() - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_graph_source_watermark() {
        let mut wm = GraphSourceWatermark::new();

        wm.update("ledger1:main", 10);
        wm.update("ledger2:main", 20);

        assert_eq!(wm.effective_t(), 10);
        assert!(wm.is_valid_at(5));
        assert!(wm.is_valid_at(10));
        assert!(!wm.is_valid_at(15));
    }

    #[test]
    fn test_bm25_index_add_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut term_freqs = HashMap::new();
        term_freqs.insert("hello", 2);
        term_freqs.insert("world", 1);

        index.add_document(doc_key.clone(), term_freqs);

        assert_eq!(index.num_docs(), 1);
        assert_eq!(index.num_terms(), 2);
        assert!(index.contains_doc(&doc_key));

        let vec = index.get_doc_vector(&doc_key).unwrap();
        assert_eq!(vec.doc_length(), 3);
    }

    #[test]
    fn test_bm25_index_remove_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");
        let mut term_freqs = HashMap::new();
        term_freqs.insert("hello", 2);
        term_freqs.insert("world", 1);

        index.add_document(doc_key.clone(), term_freqs);
        assert_eq!(index.num_docs(), 1);

        let removed = index.remove_document(&doc_key);
        assert!(removed);
        assert_eq!(index.num_docs(), 0);
        assert!(!index.contains_doc(&doc_key));
    }

    #[test]
    fn test_bm25_index_upsert_document() {
        let mut index = Bm25Index::new();

        let doc_key = DocKey::new("ledger:main", "http://example.org/doc1");

        // Initial insert
        let mut term_freqs1 = HashMap::new();
        term_freqs1.insert("hello", 2);
        term_freqs1.insert("world", 1);

        let was_update = index.upsert_document(doc_key.clone(), term_freqs1);
        assert!(!was_update, "First insert should not be an update");
        assert_eq!(index.num_docs(), 1);
        assert_eq!(index.stats.total_terms, 3); // 2 + 1

        // Verify initial doc_freq for "hello"
        let hello_doc_freq = index.get_term("hello").map(|e| e.doc_freq);
        assert_eq!(hello_doc_freq, Some(1));

        // Upsert with different content
        let mut term_freqs2 = HashMap::new();
        term_freqs2.insert("goodbye", 1);
        term_freqs2.insert("world", 2);
        term_freqs2.insert("moon", 1);

        let was_update = index.upsert_document(doc_key.clone(), term_freqs2);
        assert!(was_update, "Second upsert should be an update");
        assert_eq!(index.num_docs(), 1); // Still 1 document
        assert_eq!(index.stats.total_terms, 4); // 1 + 2 + 1

        // Verify doc_freq updated correctly
        let hello_doc_freq = index.get_term("hello").map(|e| e.doc_freq);
        assert_eq!(
            hello_doc_freq,
            Some(0),
            "hello should have doc_freq 0 after upsert"
        );

        let goodbye_doc_freq = index.get_term("goodbye").map(|e| e.doc_freq);
        assert_eq!(goodbye_doc_freq, Some(1), "goodbye should have doc_freq 1");

        let world_doc_freq = index.get_term("world").map(|e| e.doc_freq);
        assert_eq!(
            world_doc_freq,
            Some(1),
            "world should still have doc_freq 1"
        );
    }

    #[test]
    fn test_property_deps() {
        let mut deps = PropertyDeps::new();

        deps.add("http://schema.org/name");
        deps.add("http://schema.org/description");

        assert!(deps.contains("http://schema.org/name"));
        assert!(!deps.contains("http://schema.org/title"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_property_deps_from_indexing_query_basic() {
        use serde_json::json;

        let config = json!({
            "ledger": "docs",
            "query": {
                "@context": {"ex": "http://example.org/"},
                "where": [{"@id": "?x", "@type": "ex:Article"}],
                "select": {"?x": ["@id", "ex:title", "ex:content"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract ex:title and ex:content from SELECT (not @id)
        assert!(deps.contains("http://example.org/title"));
        assert!(deps.contains("http://example.org/content"));

        // @id should be excluded; @type should be tracked as rdf:type
        assert!(!deps.contains("@id"));
        assert!(
            deps.contains(fluree_vocab::rdf::TYPE),
            "should include rdf:type when query uses @type"
        );

        // Should have 3 properties (type + title + content)
        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_property_deps_from_indexing_query_with_where_properties() {
        use serde_json::json;

        let config = json!({
            "query": {
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "where": [
                    {"@id": "?x", "schema:name": "?name"},
                    {"@id": "?x", "ex:category": "news"}
                ],
                "select": {"?x": ["@id", "ex:body"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract properties from WHERE clause
        assert!(deps.contains("http://schema.org/name"));
        assert!(deps.contains("http://example.org/category"));

        // And from SELECT clause
        assert!(deps.contains("http://example.org/body"));

        assert_eq!(deps.len(), 3);
    }

    #[test]
    fn test_property_deps_from_indexing_query_no_context() {
        use serde_json::json;

        // Query with full IRIs (no context)
        let config = json!({
            "query": {
                "where": [{"@id": "?x", "http://schema.org/name": "?name"}],
                "select": {"?x": ["@id", "http://schema.org/description"]}
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should preserve full IRIs
        assert!(deps.contains("http://schema.org/name"));
        assert!(deps.contains("http://schema.org/description"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_property_deps_from_indexing_query_nested_where() {
        use serde_json::json;

        let config = json!({
            "query": {
                "@context": {"ex": "http://example.org/"},
                "where": [
                    {"@id": "?x", "ex:author": {"@id": "?author", "ex:name": "?authorName"}}
                ],
                "select": ["?x", "?authorName"]
            }
        });

        let deps = PropertyDeps::from_indexing_query(&config);

        // Should extract properties from nested patterns
        assert!(deps.contains("http://example.org/author"));
        assert!(deps.contains("http://example.org/name"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_compiled_property_deps() {
        use fluree_db_core::Sid;

        let mut deps = PropertyDeps::new();
        deps.add("http://schema.org/name");
        deps.add("http://schema.org/description");
        deps.add("http://example.org/unknown");

        // Mock encode_iri function - simulates ledger namespace encoding
        let encode_iri = |iri: &str| -> Option<Sid> {
            match iri {
                "http://schema.org/name" => Some(Sid::new(100, "name")),
                "http://schema.org/description" => Some(Sid::new(100, "description")),
                _ => None, // Unknown IRI
            }
        };

        let compiled = CompiledPropertyDeps::compile(&deps, encode_iri);

        // Should have 2 SIDs (unknown IRI skipped)
        assert_eq!(compiled.len(), 2);

        // Should contain the encoded SIDs
        assert!(compiled.contains(&Sid::new(100, "name")));
        assert!(compiled.contains(&Sid::new(100, "description")));

        // Should not contain unknown IRI
        assert!(!compiled.contains(&Sid::new(100, "unknown")));
    }

    #[test]
    fn test_affected_subjects_basic() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        // Set up compiled deps tracking "name" and "title" predicates
        let mut deps = PropertyDeps::new();
        deps.add("http://schema.org/name");
        deps.add("http://schema.org/title");

        let name_sid = Sid::new(100, "name");
        let title_sid = Sid::new(100, "title");
        let other_sid = Sid::new(100, "other");
        let dt_string = Sid::new(3, "string");

        let encode_iri = |iri: &str| -> Option<Sid> {
            match iri {
                "http://schema.org/name" => Some(name_sid.clone()),
                "http://schema.org/title" => Some(title_sid.clone()),
                _ => None,
            }
        };

        let compiled = CompiledPropertyDeps::compile(&deps, encode_iri);

        // Create test flakes
        let subject1 = Sid::new(1, "alice");
        let subject2 = Sid::new(1, "bob");
        let subject3 = Sid::new(1, "charlie");

        let flakes = vec![
            // alice has name changed - should be affected
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            // bob has title changed - should be affected
            Flake::new(
                subject2.clone(),
                title_sid.clone(),
                FlakeValue::String("CEO".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            // charlie has "other" property changed - NOT affected
            Flake::new(
                subject3.clone(),
                other_sid.clone(),
                FlakeValue::String("data".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
        ];

        let affected = compiled.affected_subjects(&flakes);

        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&subject1));
        assert!(affected.contains(&subject2));
        assert!(!affected.contains(&subject3));
    }

    #[test]
    fn test_affected_subjects_deduplication() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");

        let mut compiled = CompiledPropertyDeps::new();
        compiled.predicate_sids.insert(name_sid.clone());

        // Multiple flakes for the same subject
        let flakes = vec![
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                1,
                true,
                None,
            ),
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alicia".into()),
                dt_string.clone(),
                2,
                true,
                None,
            ),
        ];

        let affected = compiled.affected_subjects(&flakes);

        // Should be deduplicated to 1 subject
        assert_eq!(affected.len(), 1);
        assert!(affected.contains(&subject1));
    }

    #[test]
    fn test_affected_subjects_in_range() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");
        let subject2 = Sid::new(1, "bob");
        let subject3 = Sid::new(1, "charlie");

        let mut compiled = CompiledPropertyDeps::new();
        compiled.predicate_sids.insert(name_sid.clone());

        let flakes = vec![
            // t=5, outside range
            Flake::new(
                subject1.clone(),
                name_sid.clone(),
                FlakeValue::String("Alice".into()),
                dt_string.clone(),
                5,
                true,
                None,
            ),
            // t=10, at lower bound (exclusive, NOT included)
            Flake::new(
                subject2.clone(),
                name_sid.clone(),
                FlakeValue::String("Bob".into()),
                dt_string.clone(),
                10,
                true,
                None,
            ),
            // t=15, in range
            Flake::new(
                subject3.clone(),
                name_sid.clone(),
                FlakeValue::String("Charlie".into()),
                dt_string.clone(),
                15,
                true,
                None,
            ),
        ];

        // Range: 10 < t <= 20
        let affected = compiled.affected_subjects_in_range(&flakes, 10, 20);

        assert_eq!(affected.len(), 1);
        assert!(!affected.contains(&subject1)); // t=5 < 10
        assert!(!affected.contains(&subject2)); // t=10 == from_t (exclusive)
        assert!(affected.contains(&subject3)); // t=15, in range
    }

    #[test]
    fn test_affected_subjects_empty_deps() {
        use fluree_db_core::{Flake, FlakeValue, Sid};

        let compiled = CompiledPropertyDeps::new();

        let name_sid = Sid::new(100, "name");
        let dt_string = Sid::new(3, "string");
        let subject1 = Sid::new(1, "alice");

        let flakes = vec![Flake::new(
            subject1.clone(),
            name_sid.clone(),
            FlakeValue::String("Alice".into()),
            dt_string.clone(),
            1,
            true,
            None,
        )];

        // With no tracked predicates, no subjects should be affected
        let affected = compiled.affected_subjects(&flakes);
        assert!(affected.is_empty());
    }
}
