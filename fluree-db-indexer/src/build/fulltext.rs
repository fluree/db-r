//! Fulltext arena building (per-predicate BoW + BM25 stats).
//!
//! Groups fulltext entries by `(g_id, p_id)`, builds one `FulltextArena` per
//! group using the BM25 text analyzer, and uploads FTA1 blobs to CAS.
//!
//! Text analysis uses the shared pipeline from `fluree_db_binary_index::analyzer`
//! to ensure scoring consistency between index-time and query-time.

use fluree_db_binary_index::analyzer::analyze_to_term_freqs;
use fluree_db_binary_index::arena::fulltext::FulltextArena;
use fluree_db_binary_index::FulltextArenaRefV5;
use fluree_db_core::{ContentId, ContentKind, GraphId, Storage};
use std::collections::HashMap;

use crate::error::{IndexerError, Result};
use crate::fulltext_hook::FulltextEntry;

// ============================================================================
// Build + upload pipeline
// ============================================================================

/// Build fulltext arenas from collected entries and upload to CAS.
///
/// Groups entries by `(g_id, p_id)`, builds one `FulltextArena` per group
/// (resolving string text from the string dict for analysis), and uploads
/// FTA1 blobs to CAS.
///
/// Returns per-graph fulltext arena refs for inclusion in `IndexRootV5`.
pub(crate) async fn build_and_upload_fulltext_arenas<S: Storage>(
    entries: &[FulltextEntry],
    string_dict: &dyn StringLookup,
    ledger_id: &str,
    storage: &S,
) -> Result<Vec<(GraphId, Vec<FulltextArenaRefV5>)>> {
    use std::collections::BTreeMap;

    // Group entries by (g_id, p_id).
    let mut grouped: BTreeMap<(GraphId, u32), Vec<&FulltextEntry>> = BTreeMap::new();
    for entry in entries {
        grouped
            .entry((entry.g_id, entry.p_id))
            .or_default()
            .push(entry);
    }

    let mut per_graph: BTreeMap<GraphId, Vec<FulltextArenaRefV5>> = BTreeMap::new();

    for ((g_id, p_id), group_entries) in grouped {
        // Two-pass build to avoid term_id shifting.
        //
        // Pass 1: Analyze all assertion texts, collect the union of all terms,
        // and cache per-entry BoWs (keyed by string_id) to avoid re-analysis.
        use std::collections::BTreeSet;

        let mut all_terms: BTreeSet<String> = BTreeSet::new();
        // Cache: string_id → analyzed term frequencies (only for assertions).
        let mut analyzed: HashMap<u32, HashMap<String, u32>> = HashMap::new();

        for entry in &group_entries {
            if !entry.is_assert {
                continue;
            }
            // Only analyze each string_id once (dedup).
            if analyzed.contains_key(&entry.string_id) {
                continue;
            }
            let text = match string_dict.lookup_string(entry.string_id) {
                Some(t) => t,
                None => {
                    tracing::warn!(
                        string_id = entry.string_id,
                        p_id = p_id,
                        "fulltext: string_id not found in dict, skipping"
                    );
                    continue;
                }
            };
            let term_freqs = analyze_to_term_freqs(&text);
            for term in term_freqs.keys() {
                all_terms.insert(term.clone());
            }
            analyzed.insert(entry.string_id, term_freqs);
        }

        // Build arena with complete sorted term dictionary (no shifting).
        let mut arena = FulltextArena::new();
        for term in &all_terms {
            arena.get_or_insert_term(term);
        }

        // Pass 2: Build BoWs with stable term_ids and apply assert/retract stream.
        for entry in &group_entries {
            if entry.is_assert {
                if let Some(term_freqs) = analyzed.get(&entry.string_id) {
                    let mut bow: Vec<(u32, u16)> = term_freqs
                        .iter()
                        .map(|(term, &tf)| {
                            let tid = arena
                                .term_id(term)
                                .expect("all terms were pre-inserted in Pass 1");
                            (tid, tf.min(u16::MAX as u32) as u16)
                        })
                        .collect();
                    bow.sort_by_key(|(tid, _)| *tid);
                    arena.inc_string(entry.string_id, &bow);
                }
                // else: string_id lookup failed in Pass 1, already warned
            } else {
                arena.dec_string(entry.string_id);
            }
        }

        if arena.is_empty() {
            continue;
        }

        arena.finalize_stats();

        // Encode FTA1 and upload to CAS.
        let blob = arena.encode();
        let cas_result = storage
            .content_write_bytes(ContentKind::IndexLeaf, ledger_id, &blob)
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("fulltext CAS write: {}", e)))?;

        let codec = ContentKind::IndexLeaf.to_codec();
        let arena_cid =
            ContentId::from_hex_digest(codec, &cas_result.content_hash).ok_or_else(|| {
                IndexerError::Other(format!(
                    "invalid fulltext arena hash: {}",
                    cas_result.content_hash
                ))
            })?;

        per_graph
            .entry(g_id)
            .or_default()
            .push(FulltextArenaRefV5 { p_id, arena_cid });

        tracing::info!(
            g_id,
            p_id,
            docs = arena.doc_count(),
            terms = arena.terms().len(),
            bytes = blob.len(),
            "fulltext arena built for (graph, predicate)"
        );
    }

    Ok(per_graph.into_iter().collect())
}

/// Trait for looking up string text by string dictionary ID.
///
/// Abstracts over the string dict implementation (disk-backed or in-memory)
/// so the fulltext builder doesn't depend on specific dict internals.
pub(crate) trait StringLookup {
    fn lookup_string(&self, string_id: u32) -> Option<String>;
}

/// Adapter for `StringMergeResult` (full rebuild path).
impl StringLookup for crate::run_index::dict_merge::StringMergeResult {
    fn lookup_string(&self, string_id: u32) -> Option<String> {
        self.forward_entries
            .get(string_id as usize)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(|s| s.to_string())
    }
}

/// Adapter for `HashMap<u32, Vec<u8>>` (incremental path — string bytes captured during reconciliation).
impl StringLookup for HashMap<u32, Vec<u8>> {
    fn lookup_string(&self, string_id: u32) -> Option<String> {
        self.get(&string_id)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(|s| s.to_string())
    }
}

/// Build an incremental fulltext arena by merging a prior FTA1 arena with novelty entries.
///
/// Steps:
/// 1. Collect all terms (old + new from novelty) into a merged sorted list
/// 2. Build term_id remap for old arena's term_ids → merged term_ids
/// 3. Copy existing DocBoW entries with remapped term_ids
/// 4. Apply novelty assertions (inc_string) and retractions (dec_string)
/// 5. Finalize corpus stats
pub(crate) fn build_incremental_fulltext_arena(
    prior: &FulltextArena,
    entries: &[&FulltextEntry],
    string_lookup: &dyn StringLookup,
) -> FulltextArena {
    use std::collections::BTreeSet;

    // Phase 1: Collect all terms — old terms + new terms from novelty assertions.
    let mut all_terms: BTreeSet<String> = BTreeSet::new();
    for term in prior.terms() {
        all_terms.insert(term.clone());
    }
    for entry in entries {
        if !entry.is_assert {
            continue;
        }
        if let Some(text) = string_lookup.lookup_string(entry.string_id) {
            let term_freqs = analyze_to_term_freqs(&text);
            for term in term_freqs.keys() {
                all_terms.insert(term.clone());
            }
        }
    }

    // Phase 2: Build new arena with merged sorted term list + remap.
    let mut arena = FulltextArena::new();
    // Insert all terms in sorted order (BTreeSet iterates sorted).
    for term in &all_terms {
        arena.get_or_insert_term(term);
    }

    // Build remap: old_term_id → new_term_id.
    let term_remap: Vec<u32> = prior
        .terms()
        .iter()
        .map(|t| arena.term_id(t).expect("all old terms were inserted"))
        .collect();

    // Phase 3: Copy existing DocBoW entries with remapped term_ids.
    for (&string_id, doc) in prior.docs() {
        let remapped_terms: Vec<(u32, u16)> = doc
            .terms
            .iter()
            .map(|&(old_tid, tf)| (term_remap[old_tid as usize], tf))
            .collect();
        // terms are already sorted by new term_id because the remap preserves
        // sorted order (both old and new term dicts are sorted lexicographically).
        arena.insert_doc_raw(
            string_id,
            fluree_db_binary_index::arena::fulltext::DocBoW {
                triple_count: doc.triple_count,
                doc_len: doc.doc_len,
                terms: remapped_terms,
            },
        );
    }

    // Phase 4: Apply novelty entries.
    for entry in entries {
        if entry.is_assert {
            if let Some(text) = string_lookup.lookup_string(entry.string_id) {
                let term_freqs = analyze_to_term_freqs(&text);
                let mut bow: Vec<(u32, u16)> = term_freqs
                    .into_iter()
                    .map(|(term, tf)| {
                        let tid = arena
                            .term_id(&term)
                            .expect("all terms were pre-inserted in Phase 1");
                        (tid, tf.min(u16::MAX as u32) as u16)
                    })
                    .collect();
                bow.sort_by_key(|(tid, _)| *tid);
                arena.inc_string(entry.string_id, &bow);
            } else {
                tracing::warn!(
                    string_id = entry.string_id,
                    "fulltext incremental: string not found, skipping assertion"
                );
            }
        } else {
            arena.dec_string(entry.string_id);
        }
    }

    // Phase 5: Finalize corpus stats.
    arena.finalize_stats();
    arena
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_basic() {
        let freqs = analyze_to_term_freqs("The quick brown fox jumps over the lazy dog");
        // "the" and "over" are stopwords
        assert!(!freqs.contains_key("the"));
        assert!(!freqs.contains_key("over"));
        // "quick", "brown", "fox", "jump" (stemmed), "lazi" (stemmed), "dog"
        assert!(freqs.contains_key("quick"));
        assert!(freqs.contains_key("fox"));
        // "jumps" stems to "jump"
        assert!(freqs.contains_key("jump"));
    }

    #[test]
    fn test_analyze_stemming() {
        let freqs = analyze_to_term_freqs("indexing indexed indexes");
        // All should stem to "index"
        assert_eq!(freqs.len(), 1);
        assert_eq!(freqs["index"], 3);
    }

    #[test]
    fn test_analyze_empty() {
        let freqs = analyze_to_term_freqs("");
        assert!(freqs.is_empty());

        // All stopwords
        let freqs = analyze_to_term_freqs("the a an is are was");
        assert!(freqs.is_empty());
    }

    // ========================================================================
    // Incremental arena builder tests
    // ========================================================================

    fn make_string_map(entries: &[(u32, &str)]) -> HashMap<u32, Vec<u8>> {
        entries
            .iter()
            .map(|&(id, s)| (id, s.as_bytes().to_vec()))
            .collect()
    }

    fn make_entry(g_id: u16, p_id: u32, string_id: u32, t: i64, is_assert: bool) -> FulltextEntry {
        FulltextEntry {
            g_id,
            p_id,
            string_id,
            t,
            is_assert,
        }
    }

    #[test]
    fn test_incremental_from_empty_prior() {
        let prior = FulltextArena::new();
        let entries = [make_entry(0, 1, 10, 1, true), make_entry(0, 1, 20, 1, true)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings = make_string_map(&[(10, "hello world rust"), (20, "rust programming")]);

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        assert_eq!(arena.doc_count(), 2);
        assert!(arena.doc_bow(10).is_some());
        assert!(arena.doc_bow(20).is_some());
        assert_eq!(arena.stats().n, 2); // 2 triples total
    }

    #[test]
    fn test_incremental_adds_to_existing() {
        // Build a prior arena with one doc.
        let mut prior = FulltextArena::new();
        let hello = prior.get_or_insert_term("hello");
        let world = prior.get_or_insert_term("world");
        prior.inc_string(10, &[(hello, 1), (world, 1)]);
        prior.finalize_stats();
        assert_eq!(prior.stats().n, 1);

        // Add a new doc via incremental update.
        let entries = [make_entry(0, 1, 20, 2, true)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings = make_string_map(&[(20, "hello rust")]);

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        assert_eq!(arena.doc_count(), 2);
        assert_eq!(arena.stats().n, 2);
        // Old doc preserved.
        let doc10 = arena.doc_bow(10).unwrap();
        assert_eq!(doc10.triple_count, 1);
        // New doc added.
        let doc20 = arena.doc_bow(20).unwrap();
        assert_eq!(doc20.triple_count, 1);
        // New term "rust" was added alongside old terms.
        assert!(arena.term_id("rust").is_some());
        assert!(arena.term_id("hello").is_some());
        assert!(arena.term_id("world").is_some());
    }

    #[test]
    fn test_incremental_retraction_removes_doc() {
        // Build a prior arena with two docs.
        let mut prior = FulltextArena::new();
        let hello = prior.get_or_insert_term("hello");
        let world = prior.get_or_insert_term("world");
        prior.inc_string(10, &[(hello, 1), (world, 1)]);
        prior.inc_string(20, &[(hello, 1)]);
        prior.finalize_stats();
        assert_eq!(prior.stats().n, 2);

        // Retract doc 20.
        let entries = [make_entry(0, 1, 20, 2, false)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings: HashMap<u32, Vec<u8>> = HashMap::new();

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        assert_eq!(arena.doc_count(), 1);
        assert_eq!(arena.stats().n, 1);
        assert!(arena.doc_bow(10).is_some());
        assert!(arena.doc_bow(20).is_none());
    }

    #[test]
    fn test_incremental_existing_string_gets_more_triples() {
        // Prior arena: string_id=10 has 1 triple.
        let mut prior = FulltextArena::new();
        let hello = prior.get_or_insert_term("hello");
        prior.inc_string(10, &[(hello, 1)]);
        prior.finalize_stats();

        // Another triple asserts the same string_id.
        let entries = [make_entry(0, 1, 10, 2, true)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings = make_string_map(&[(10, "hello")]);

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        assert_eq!(arena.doc_count(), 1);
        let doc = arena.doc_bow(10).unwrap();
        assert_eq!(doc.triple_count, 2); // 1 prior + 1 novelty
        assert_eq!(arena.stats().n, 2);
    }

    #[test]
    fn test_incremental_term_remap_preserves_scoring() {
        // Prior arena has terms ["cherry", "dog"].
        let mut prior = FulltextArena::new();
        let cherry = prior.get_or_insert_term("cherri"); // stemmed
        let dog = prior.get_or_insert_term("dog");
        prior.inc_string(10, &[(cherry, 2), (dog, 1)]);
        prior.finalize_stats();

        // Novelty adds a term "banana" which sorts BEFORE "cherry",
        // forcing a term_id remap.
        let entries = [make_entry(0, 1, 20, 2, true)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings = make_string_map(&[(20, "banana cherry")]);

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        // Verify old doc's terms still resolve correctly after remap.
        let doc10 = arena.doc_bow(10).unwrap();
        assert_eq!(doc10.triple_count, 1);
        // The "cherri" term should still have tf=2 in doc10.
        let cherri_id = arena.term_id("cherri").unwrap();
        let tf = doc10
            .terms
            .iter()
            .find(|(tid, _)| *tid == cherri_id)
            .map(|(_, tf)| *tf);
        assert_eq!(tf, Some(2));

        // New doc should have both "banana" (stemmed: "banana") and "cherri" (stemmed: "cherri").
        let doc20 = arena.doc_bow(20).unwrap();
        assert_eq!(doc20.triple_count, 1);
    }

    #[test]
    fn test_incremental_encode_decode_roundtrip() {
        // Build a prior arena.
        let mut prior = FulltextArena::new();
        let hello = prior.get_or_insert_term("hello");
        prior.inc_string(10, &[(hello, 1)]);
        prior.finalize_stats();

        // Incremental update.
        let entries = [make_entry(0, 1, 20, 2, true)];
        let entry_refs: Vec<&FulltextEntry> = entries.iter().collect();
        let strings = make_string_map(&[(20, "world")]);

        let arena = build_incremental_fulltext_arena(&prior, &entry_refs, &strings);

        // Encode and decode should roundtrip.
        let bytes = arena.encode();
        let decoded = FulltextArena::decode(&bytes).unwrap();
        assert_eq!(decoded.doc_count(), arena.doc_count());
        assert_eq!(decoded.terms().len(), arena.terms().len());
        assert_eq!(decoded.stats().n, arena.stats().n);
    }
}
