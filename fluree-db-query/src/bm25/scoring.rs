//! BM25 Scoring Algorithm
//!
//! Implements BM25 scoring with Clojure parity:
//! - IDF: log(1 + (N - n + 0.5) / (n + 0.5))
//! - Score: Σ IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))
//!
//! Default parameters: k1=1.2, b=0.75
//!
//! Scoring iterates posting lists (term → docs) rather than scanning all documents,
//! making query time proportional to the number of matching postings rather than O(N).

use std::collections::BTreeMap;

use super::index::{Bm25Config, Bm25Index, DocKey};

/// BM25 scorer for computing document relevance scores.
///
/// Iterates posting lists for query terms, accumulating scores in a dense Vec.
/// Tombstoned documents (lazy-deleted) are skipped during scoring.
pub struct Bm25Scorer<'a> {
    index: &'a Bm25Index,
    /// Precomputed IDF values for query terms (term_idx -> idf)
    /// Deduplicated: each term appears at most once.
    query_idfs: Vec<(u32, f64)>,
}

impl<'a> Bm25Scorer<'a> {
    /// Create a new scorer for the given query terms.
    ///
    /// `query_terms` should be the analyzed (tokenized, filtered, stemmed) query terms.
    /// Duplicate terms are automatically deduplicated (matching Clojure's `distinct`).
    pub fn new(index: &'a Bm25Index, query_terms: &[&str]) -> Self {
        // Deduplicate query terms using BTreeMap for deterministic order
        // (term_idx -> idf), where each term contributes only once
        let mut term_map: BTreeMap<u32, f64> = BTreeMap::new();

        for term in query_terms {
            if let Some(entry) = index.get_term(term) {
                // Only insert if not already present (first occurrence wins, though IDF is same)
                term_map
                    .entry(entry.idx)
                    .or_insert_with(|| compute_idf(index.stats.num_docs, entry.doc_freq));
            }
        }

        let query_idfs: Vec<(u32, f64)> = term_map.into_iter().collect();

        Self { index, query_idfs }
    }

    /// Score a single document against the query.
    ///
    /// Returns the BM25 score, or 0.0 if the document has no matching terms.
    pub fn score(&self, doc_key: &DocKey) -> f64 {
        let Some(doc_id) = self.index.doc_id_for(doc_key) else {
            return 0.0;
        };

        let Some(Some(meta)) = self.index.doc_meta.get(doc_id as usize) else {
            return 0.0; // Tombstoned
        };

        let config = &self.index.config;
        let avg_dl = self.index.stats.avg_doc_len();
        let doc_len = meta.doc_len as f64;

        let mut score = 0.0;

        for &(term_idx, idf) in &self.query_idfs {
            if let Some(posting_list) = self.index.get_posting_list(term_idx) {
                // Binary search for this doc_id in the posting list
                if let Ok(pos) = posting_list
                    .postings
                    .binary_search_by_key(&doc_id, |p| p.doc_id)
                {
                    let tf = posting_list.postings[pos].term_freq as f64;
                    score += compute_term_score(tf, idf, doc_len, avg_dl, config);
                }
            }
        }

        score
    }

    /// Score all documents in the index, returning results sorted by score (descending).
    ///
    /// Only returns documents with score > 0. Uses a dense Vec accumulator
    /// indexed by doc_id for efficient scoring. Tombstoned docs are skipped.
    /// Ties are broken by DocKey ascending for deterministic output.
    pub fn score_all(&self) -> Vec<(DocKey, f64)> {
        let config = &self.index.config;
        let avg_dl = self.index.stats.avg_doc_len();
        let next_doc_id = self.index.next_doc_id() as usize;

        // Dense score accumulator — O(1) per posting
        let mut scores = vec![0.0f64; next_doc_id];

        for &(term_idx, idf) in &self.query_idfs {
            if let Some(posting_list) = self.index.get_posting_list(term_idx) {
                for posting in &posting_list.postings {
                    let doc_id = posting.doc_id as usize;
                    if doc_id >= next_doc_id {
                        continue;
                    }
                    // Check if doc is live (not tombstoned)
                    if let Some(Some(meta)) = self.index.doc_meta.get(doc_id) {
                        let tf = posting.term_freq as f64;
                        let doc_len = meta.doc_len as f64;
                        scores[doc_id] += compute_term_score(tf, idf, doc_len, avg_dl, config);
                    }
                }
            }
        }

        // Collect non-zero scores with DocKeys
        let mut results: Vec<(DocKey, f64)> = scores
            .iter()
            .enumerate()
            .filter(|(_, &score)| score > 0.0)
            .filter_map(|(doc_id, &score)| {
                self.index
                    .doc_meta
                    .get(doc_id)
                    .and_then(|opt| opt.as_ref())
                    .map(|meta| (meta.doc_key.clone(), score))
            })
            .collect();

        // Sort by score descending, then DocKey ascending for deterministic tie-breaking
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        results
    }

    /// Score all documents and return top-k results.
    pub fn top_k(&self, k: usize) -> Vec<(DocKey, f64)> {
        let mut results = self.score_all();
        results.truncate(k);
        results
    }
}

/// Compute IDF (Inverse Document Frequency) for a term.
///
/// Uses the formula: log(1 + (N - n + 0.5) / (n + 0.5))
/// where N is total number of documents and n is document frequency of the term.
///
/// This matches the Clojure implementation.
#[inline]
pub fn compute_idf(total_docs: u64, doc_freq: u32) -> f64 {
    let n = total_docs as f64;
    let df = doc_freq as f64;

    // IDF formula matching Clojure: log(1 + (N - n + 0.5) / (n + 0.5))
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// Compute the BM25 score contribution for a single term.
///
/// Uses the formula: IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len)))
#[inline]
pub fn compute_term_score(
    tf: f64,
    idf: f64,
    doc_len: f64,
    avg_doc_len: f64,
    config: &Bm25Config,
) -> f64 {
    let k1 = config.k1;
    let b = config.b;

    // Normalize document length
    let len_norm = if avg_doc_len > 0.0 {
        doc_len / avg_doc_len
    } else {
        1.0
    };

    // BM25 score contribution for this term
    let numerator = tf * (k1 + 1.0);
    let denominator = tf + k1 * (1.0 - b + b * len_norm);

    idf * numerator / denominator
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn build_test_index() -> Bm25Index {
        let mut index = Bm25Index::new();

        // Document 1: "the quick brown fox"
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("the", 1);
        tf1.insert("quick", 1);
        tf1.insert("brown", 1);
        tf1.insert("fox", 1);
        index.add_document(doc1, tf1);

        // Document 2: "the lazy brown dog"
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("the", 1);
        tf2.insert("lazy", 1);
        tf2.insert("brown", 1);
        tf2.insert("dog", 1);
        index.add_document(doc2, tf2);

        // Document 3: "the quick fox jumps" (more foxes!)
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let mut tf3 = HashMap::new();
        tf3.insert("the", 1);
        tf3.insert("quick", 1);
        tf3.insert("fox", 2); // Higher frequency
        tf3.insert("jumps", 1);
        index.add_document(doc3, tf3);

        index
    }

    #[test]
    fn test_idf_calculation() {
        // Term appearing in 1 of 10 documents
        let idf = compute_idf(10, 1);
        assert!(idf > 0.0);

        // Term appearing in all documents has lower IDF
        let idf_common = compute_idf(10, 10);
        assert!(idf_common < idf);

        // Term appearing in half the documents
        let idf_half = compute_idf(10, 5);
        assert!(idf_half > idf_common);
        assert!(idf_half < idf);
    }

    #[test]
    fn test_scorer_basic() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["fox"]);

        // Doc1 has "fox" once
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let score1 = scorer.score(&doc1);

        // Doc3 has "fox" twice
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let score3 = scorer.score(&doc3);

        // Doc2 has no "fox"
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let score2 = scorer.score(&doc2);

        assert!(score1 > 0.0);
        assert!(score3 > score1); // Higher TF should give higher score
        assert_eq!(score2, 0.0);
    }

    #[test]
    fn test_scorer_multi_term() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["quick", "fox"]);

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");

        let score1 = scorer.score(&doc1);
        let score2 = scorer.score(&doc2);
        let score3 = scorer.score(&doc3);

        // Doc1 and Doc3 have both terms, Doc2 has neither
        assert!(score1 > 0.0);
        assert!(score3 > 0.0);
        assert_eq!(score2, 0.0);

        // Doc3 should score higher (has "fox" twice)
        assert!(score3 > score1);
    }

    #[test]
    fn test_top_k() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["fox"]);

        let results = scorer.top_k(2);

        assert_eq!(results.len(), 2);
        // Results should be sorted by score descending
        assert!(results[0].1 >= results[1].1);
        // Doc3 (with "fox" twice) should be first
        assert_eq!(results[0].0.subject_iri.as_ref(), "http://example.org/doc3");
    }

    #[test]
    fn test_score_all() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["the"]);

        let results = scorer.score_all();

        // All 3 documents have "the"
        assert_eq!(results.len(), 3);
        // All scores should be positive
        assert!(results.iter().all(|(_, s)| *s > 0.0));
    }

    #[test]
    fn test_unknown_query_terms() {
        let index = build_test_index();
        let scorer = Bm25Scorer::new(&index, &["nonexistent", "terms"]);

        let results = scorer.score_all();
        assert!(results.is_empty());
    }

    #[test]
    fn test_duplicate_query_terms_deduplicated() {
        let index = build_test_index();

        // Create scorer with duplicate terms
        let scorer_with_dupes = Bm25Scorer::new(&index, &["fox", "fox", "fox"]);
        // Create scorer with single term
        let scorer_without_dupes = Bm25Scorer::new(&index, &["fox"]);

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");

        // Scores should be identical - duplicates are deduplicated
        let score_with_dupes = scorer_with_dupes.score(&doc1);
        let score_without_dupes = scorer_without_dupes.score(&doc1);

        assert!(
            (score_with_dupes - score_without_dupes).abs() < 1e-10,
            "Duplicate query terms should be deduplicated: {} vs {}",
            score_with_dupes,
            score_without_dupes
        );
    }

    #[test]
    fn test_bm25_config() {
        let mut index = Bm25Index::with_config(Bm25Config::new(2.0, 0.5));

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("test", 1);
        index.add_document(doc1.clone(), tf1);

        let scorer = Bm25Scorer::new(&index, &["test"]);
        let score = scorer.score(&doc1);

        // With different k1/b, score should still be positive
        assert!(score > 0.0);
    }

    #[test]
    fn test_scorer_skips_tombstoned_docs() {
        let mut index = Bm25Index::new();

        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 1);
        index.add_document(doc1.clone(), tf1);

        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 2);
        index.add_document(doc2.clone(), tf2);

        // Remove doc1 (lazy — posting stays)
        index.remove_document(&doc1);

        let scorer = Bm25Scorer::new(&index, &["hello"]);
        let results = scorer.score_all();

        // Only doc2 should appear
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, doc2);
    }
}
