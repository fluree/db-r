//! Fulltext scoring function implementation
//!
//! Implements `fulltext(?content, "query")` — a scoring function for
//! `@fulltext`-typed literals. Returns a numeric score (f64) for `@fulltext`
//! values, `0.0` when no terms match, or `None` (unbound) for non-`@fulltext` values.
//!
//! Predicate scoping is implicit: when `?content` is bound from a where-clause
//! pattern, the binding is an `EncodedLit` which carries `p_id` and `dt_id`.
//! The function reads the raw `Binding` to access this metadata rather than
//! going through `eval_to_comparable()` which strips it.
//!
//! **Scoring strategy**:
//! - When a `FulltextArena` is available for `(g_id, p_id)`, uses full BM25
//!   scoring with corpus-wide IDF and avgdl normalization. This avoids
//!   decoding the string entirely — scores come from `string_id → BoW`.
//! - Falls back to per-document TF-saturation when no arena is available
//!   (e.g., novelty-only data not yet indexed).

use once_cell::sync::Lazy;

use crate::binding::RowAccess;
use crate::bm25::analyzer::Analyzer;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;

use super::helpers::check_arity;
use super::value::ComparableValue;

use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::value_id::ObjKind;
use fluree_vocab::namespaces::FLUREE_DB;

/// Lazily-initialized English analyzer (reused across all fulltext calls).
static ENGLISH_ANALYZER: Lazy<Analyzer> = Lazy::new(Analyzer::clojure_parity_english);

/// TF-saturation parameters (BM25 TF component, used in fallback path)
const K1: f64 = 1.2;
const B: f64 = 0.75;

/// Evaluate `fulltext(?var, "query")`.
///
/// Return contract:
/// - `Some(Double(score))` for `@fulltext` values (score >= 0.0, including 0.0 for no match)
/// - `None` (unbound) for non-`@fulltext` values or type mismatches
///
/// This lets users sort all `@fulltext` rows by score without needing
/// `(bound ?score)` — non-`@fulltext` rows naturally drop via unbound.
pub fn eval_fulltext<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "fulltext")?;

    // Extract the query string from args[1]
    // Type mismatch → None (graceful, consistent with vector functions)
    let query_str = match args[1].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::String(s)) => s,
        _ => return Ok(None),
    };

    // Read the raw binding for args[0] — we need p_id and dt_id from EncodedLit
    let var_id = match &args[0] {
        Expression::Var(v) => *v,
        _ => {
            // Non-variable first arg: can't determine predicate context
            return Ok(None);
        }
    };

    let binding = match row.get(var_id) {
        Some(b) => b,
        None => return Ok(None),
    };

    match binding {
        crate::binding::Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            ..
        } => {
            // Only score @fulltext-typed values
            if *dt_id != DatatypeDictId::FULL_TEXT.as_u16() {
                return Ok(None);
            }

            // Try arena-based BM25 scoring first (avoids string decode entirely).
            // Guard on LEX_ID: the arena maps string_id → BoW, so o_key must be
            // a string dict ID. Non-LEX values (shouldn't reach here given the
            // indexer hook guard, but be resilient) fall through to decode path.
            if ObjKind::from_u8(*o_kind) == ObjKind::LEX_ID {
                if let Some(ctx) = ctx {
                    let g_id = ctx.binary_g_id;
                    if let Some(arena) = ctx
                        .fulltext_providers
                        .and_then(|providers| providers.get(&(g_id, *p_id)))
                    {
                        // Only use arena BM25 if the doc is actually in the arena.
                        // Novelty docs (not yet indexed) won't be in the arena and
                        // must fall through to the decode + TF-saturation path.
                        if arena.doc_bow(*o_key as u32).is_some() {
                            let query_term_ids =
                                analyze_query_to_term_ids(arena, &query_str);
                            if query_term_ids.is_empty() {
                                return Ok(Some(ComparableValue::Double(0.0)));
                            }
                            let score =
                                arena.score_bm25(*o_key as u32, &query_term_ids);
                            return Ok(Some(ComparableValue::Double(score)));
                        }
                        // Doc not in arena (novelty) → fall through to decode path
                    }
                }
            }

            // Fallback: decode string and score with TF-saturation
            let gv = match ctx.and_then(|c| c.graph_view()) {
                Some(gv) => gv,
                None => return Ok(None),
            };

            let val = gv
                .decode_value(*o_kind, *o_key, *p_id)
                .map_err(|e| {
                    crate::error::QueryError::Internal(format!("fulltext decode_value: {}", e))
                })?;

            let text = match &val {
                fluree_db_core::FlakeValue::String(s) => s.as_str(),
                _ => return Ok(None),
            };

            let score = score_tf_saturation(text, &query_str);
            Ok(Some(ComparableValue::Double(score)))
        }
        crate::binding::Binding::Lit { val, dt, .. } => {
            // Check if the datatype matches @fulltext (Sid fields: namespace_code, name)
            if !(dt.namespace_code == FLUREE_DB && dt.name.as_ref() == "fullText") {
                return Ok(None);
            }

            let text = match val {
                fluree_db_core::FlakeValue::String(s) => s.as_str(),
                _ => return Ok(None),
            };

            let score = score_tf_saturation(text, &query_str);
            Ok(Some(ComparableValue::Double(score)))
        }
        _ => Ok(None),
    }
}

/// Analyze query text and resolve terms to arena term_ids.
///
/// Returns only term_ids that exist in the arena's dictionary. Unknown query
/// terms (not in any indexed document) are silently dropped since they have
/// zero DF and contribute nothing to BM25.
fn analyze_query_to_term_ids(
    arena: &fluree_db_binary_index::FulltextArena,
    query_text: &str,
) -> Vec<u32> {
    let analyzer = &*ENGLISH_ANALYZER;
    let query_terms = analyzer.analyze_to_strings(query_text);
    query_terms
        .iter()
        .filter_map(|term| arena.term_id(term))
        .collect()
}

/// Score a document against a query using TF-saturation (BM25 TF component).
///
/// Fallback path used when no FulltextArena is available. Uses only per-document
/// term-frequency saturation without corpus-wide IDF or avgdl normalization.
fn score_tf_saturation(doc_text: &str, query_text: &str) -> f64 {
    let analyzer = &*ENGLISH_ANALYZER;

    let query_terms = analyzer.analyze_to_strings(query_text);
    if query_terms.is_empty() {
        return 0.0;
    }

    let doc_term_freqs = analyzer.analyze_to_term_freqs(doc_text);
    if doc_term_freqs.is_empty() {
        return 0.0;
    }

    // Document length = total term count
    let dl: f64 = doc_term_freqs.values().sum::<u32>() as f64;
    let avgdl = dl; // single-doc; no corpus stats yet

    let mut score = 0.0;
    for qt in &query_terms {
        if let Some(&tf) = doc_term_freqs.get(qt) {
            let tf = tf as f64;
            // TF saturation: tf*(k1+1) / (tf + k1*(1-b+b*dl/avgdl))
            // With dl == avgdl this simplifies to tf*(k1+1) / (tf + k1)
            let tf_component = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
            score += tf_component;
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_basic_match() {
        let score = score_tf_saturation("cargo testing with nextest", "cargo testing");
        assert!(score > 0.0, "Should have positive score for matching terms");
    }

    #[test]
    fn test_score_no_match() {
        let score = score_tf_saturation("hello world", "cargo testing");
        assert_eq!(score, 0.0, "Should have zero score for no matching terms");
    }

    #[test]
    fn test_score_empty_query() {
        let score = score_tf_saturation("cargo testing", "");
        assert_eq!(score, 0.0, "Empty query should produce zero score");
    }

    #[test]
    fn test_score_empty_doc() {
        let score = score_tf_saturation("", "cargo testing");
        assert_eq!(score, 0.0, "Empty document should produce zero score");
    }

    #[test]
    fn test_score_stopwords_only_query() {
        // "the" and "is" are English stopwords; "test" is also a stopword
        // in the Fluree English stopword list
        let score = score_tf_saturation("cargo testing with nextest", "the is");
        assert_eq!(
            score, 0.0,
            "Stopwords-only query should produce zero score"
        );
    }

    #[test]
    fn test_score_stemming() {
        // "indexing" and "indexed" should both stem to "index"
        let score1 = score_tf_saturation("indexing documents", "indexed");
        let score2 = score_tf_saturation("indexing documents", "indexing");
        assert!(score1 > 0.0, "Stemmed match should score > 0");
        assert!(score2 > 0.0, "Same-form match should score > 0");
        assert_eq!(score1, score2, "Same stem should produce same score");
    }

    #[test]
    fn test_score_more_matches_higher() {
        let score_one = score_tf_saturation("cargo nextest runner", "cargo");
        let score_two = score_tf_saturation("cargo nextest runner", "cargo nextest");
        assert!(
            score_two > score_one,
            "More matching terms should produce higher score: {} vs {}",
            score_two,
            score_one
        );
    }

    #[test]
    fn test_score_returns_zero_for_no_overlap() {
        // Verify 0.0 (not None) is returned for analyzed-but-non-matching content
        let score = score_tf_saturation("database query engine", "weather forecast");
        assert_eq!(score, 0.0);
    }

    /// Helper to build a FulltextArena from text strings (analyze + insert).
    ///
    /// Uses two-pass approach to avoid term_id shifting:
    /// 1. Collect all unique terms from all documents
    /// 2. Build BoWs with stable term_ids
    fn build_test_arena(docs: &[(u32, &str)]) -> fluree_db_binary_index::FulltextArena {
        let analyzer = &*ENGLISH_ANALYZER;
        let mut arena = fluree_db_binary_index::FulltextArena::new();

        // Pass 1: collect all unique terms across all documents
        let mut all_terms = std::collections::BTreeSet::new();
        let per_doc: Vec<_> = docs
            .iter()
            .map(|&(string_id, text)| {
                let term_freqs = analyzer.analyze_to_term_freqs(text);
                for term in term_freqs.keys() {
                    all_terms.insert(term.clone());
                }
                (string_id, term_freqs)
            })
            .collect();

        // Insert all terms in sorted order (stable IDs)
        for term in &all_terms {
            arena.get_or_insert_term(term);
        }

        // Pass 2: build BoWs using stable term_ids
        for (string_id, term_freqs) in per_doc {
            let mut bow: Vec<(u32, u16)> = term_freqs
                .iter()
                .map(|(term, &tf)| {
                    let tid = arena.term_id(term).unwrap();
                    (tid, tf as u16)
                })
                .collect();
            bow.sort_by_key(|(tid, _)| *tid);
            arena.inc_string(string_id, &bow);
        }
        arena.finalize_stats();
        arena
    }

    #[test]
    fn test_analyze_query_to_term_ids() {
        let arena = build_test_arena(&[
            (1, "cargo nextest runner"),
        ]);

        // Query terms that exist in the arena
        let ids = analyze_query_to_term_ids(&arena, "cargo runner");
        assert_eq!(ids.len(), 2, "Should find two matching terms");

        // Query with unknown terms
        let ids = analyze_query_to_term_ids(&arena, "unknown words");
        assert!(ids.is_empty(), "Unknown terms should produce empty vec");

        // Query with stopwords only
        let ids = analyze_query_to_term_ids(&arena, "the is");
        assert!(ids.is_empty(), "Stopwords should produce empty vec");
    }

    #[test]
    fn test_arena_bm25_scoring() {
        let arena = build_test_arena(&[
            (10, "cargo nextest runner for fast testing"),
            (20, "cargo build optimizations and cargo features"),
            (30, "database query engine performance"),
        ]);

        // Score doc 10 for "cargo"
        let term_ids = analyze_query_to_term_ids(&arena, "cargo");
        let score_10 = arena.score_bm25(10, &term_ids);
        let score_20 = arena.score_bm25(20, &term_ids);
        let score_30 = arena.score_bm25(30, &term_ids);

        assert!(score_10 > 0.0, "Doc with 'cargo' should score > 0");
        assert!(score_20 > 0.0, "Doc with 'cargo' should score > 0");
        assert_eq!(score_30, 0.0, "Doc without 'cargo' should score 0");

        // Doc 20 mentions "cargo" twice → higher TF
        assert!(
            score_20 > score_10,
            "Doc with higher TF should score higher: {} vs {}",
            score_20,
            score_10
        );

        // Unknown string_id
        assert_eq!(arena.score_bm25(99, &term_ids), 0.0);
    }
}
