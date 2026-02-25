//! Integration tests for fulltext scoring via `fulltext()` function.
//!
//! These tests exercise the full pipeline: transact `@fulltext` data →
//! build binary index (including FTA1 fulltext arenas) → query with
//! `fulltext(?var, "query")` in bind expressions → verify BM25 scoring.
//!
//! Tests cover:
//! - Basic arena-based BM25 scoring (positive scores for matching docs)
//! - Non-matching documents produce score 0
//! - Multi-document ranking (more/better matches → higher scores)
//! - Retraction removes documents from the arena
//! - Multiple predicates produce independent arenas
//!
//! These tests require the binary index to be built, so they use the native feature.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Value as JsonValue};
use support::start_background_indexer_local;

fn fulltext_context() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#"
    })
}

/// Helper to insert a document with @fulltext content.
async fn insert_doc(
    fluree: &support::MemoryFluree,
    ledger: support::MemoryLedger,
    id: &str,
    title: &str,
    content: &str,
) -> support::MemoryLedger {
    let tx = json!({
        "@context": fulltext_context(),
        "@id": id,
        "ex:title": title,
        "ex:content": {
            "@value": content,
            "@type": "@fulltext"
        }
    });

    fluree.insert(ledger, &tx).await.expect("insert doc").ledger
}

/// Helper to run a fulltext query and return (title, score) pairs ordered by score desc.
async fn query_fulltext(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    query_text: &str,
) -> Vec<(String, f64)> {
    let bind_expr = format!("(fulltext ?content \"{}\")", query_text);

    let query = json!({
        "@context": fulltext_context(),
        "select": ["?title", "?score"],
        "where": [
            { "@id": "?doc", "ex:content": "?content", "ex:title": "?title" },
            ["bind", "?score", bind_expr],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]]
    });

    let result = fluree.query(ledger, &query).await;
    match result {
        Ok(r) => {
            let json_rows = r.to_jsonld(&ledger.snapshot).expect("jsonld");
            json_rows
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|row| {
                            let arr = row.as_array()?;
                            let title = arr.first()?.as_str()?.to_string();
                            let score = arr.get(1)?.as_f64()?;
                            Some((title, score))
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        Err(e) => {
            panic!("Fulltext query failed: {}", e);
        }
    }
}

/// Trigger indexing and wait for completion.
async fn index_and_load(
    fluree: &support::MemoryFluree,
    handle: &fluree_db_indexer::IndexerHandle,
    alias: &str,
    t: i64,
) -> LedgerState {
    let completion = handle.trigger(alias, t).await;
    match completion.wait().await {
        fluree_db_api::IndexOutcome::Completed { .. } => {}
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    }
    fluree.ledger(alias).await.expect("load ledger")
}

// =============================================================================
// Basic scoring tests
// =============================================================================

#[tokio::test]
async fn fulltext_basic_scoring_returns_positive_for_matching_doc() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-basic:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Rust Guide",
                "Rust is a systems programming language focused on safety and performance",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            let results = query_fulltext(&fluree, &loaded, "Rust programming").await;

            assert!(
                !results.is_empty(),
                "Should find at least one matching document"
            );
            assert_eq!(results[0].0, "Rust Guide");
            assert!(
                results[0].1 > 0.0,
                "Matching doc should have positive score: {}",
                results[0].1
            );
        })
        .await;
}

#[tokio::test]
async fn fulltext_non_matching_query_excluded_by_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-nomatch:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Rust Guide",
                "Rust is a systems programming language",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Query for terms not in the document
            let results = query_fulltext(&fluree, &loaded, "cooking recipes").await;

            assert!(
                results.is_empty(),
                "Non-matching query should return no results (filtered by > 0)"
            );
        })
        .await;
}

// =============================================================================
// Ranking tests
// =============================================================================

#[tokio::test]
async fn fulltext_ranking_more_relevant_doc_scores_higher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-ranking:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Doc 1: mentions "database" once
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Intro",
                "This guide covers database fundamentals and design patterns",
            )
            .await;

            // Doc 2: mentions "database" multiple times
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "Deep Dive",
                "Database indexing strategies for database performance optimization in database systems",
            )
            .await;

            // Doc 3: no match
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc3",
                "Unrelated",
                "Cooking recipes for pasta and bread",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            let results = query_fulltext(&fluree, &loaded, "database").await;

            assert_eq!(
                results.len(),
                2,
                "Should find exactly two matching docs, got: {:?}",
                results
            );

            // The doc with more occurrences of "database" should rank higher
            assert_eq!(
                results[0].0, "Deep Dive",
                "Doc with higher TF should rank first"
            );
            assert_eq!(
                results[1].0, "Intro",
                "Doc with lower TF should rank second"
            );
            assert!(
                results[0].1 > results[1].1,
                "Higher TF doc should have higher score: {} vs {}",
                results[0].1,
                results[1].1
            );
        })
        .await;
}

// =============================================================================
// Retraction tests
// =============================================================================

#[tokio::test]
async fn fulltext_retraction_removes_doc_from_results() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-retract:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert two documents
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Keeper",
                "Rust programming language guide",
            )
            .await;
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "ToRemove",
                "Rust compiler optimization techniques",
            )
            .await;

            // Index after initial inserts
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;
            let results_before = query_fulltext(&fluree, &loaded, "Rust").await;
            assert_eq!(
                results_before.len(),
                2,
                "Should find both docs before retraction"
            );

            // Retract the content of doc2 by updating it to a non-fulltext value
            let retract_tx = json!({
                "@context": fulltext_context(),
                "where": {
                    "@id": "ex:doc2",
                    "ex:content": "?old"
                },
                "delete": {
                    "@id": "ex:doc2",
                    "ex:content": "?old"
                }
            });
            let ledger = fluree
                .update(loaded, &retract_tx)
                .await
                .expect("retract")
                .ledger;

            // Re-index after retraction
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;
            let results_after = query_fulltext(&fluree, &loaded, "Rust").await;

            assert_eq!(
                results_after.len(),
                1,
                "Should find only one doc after retraction, got: {:?}",
                results_after
            );
            assert_eq!(results_after[0].0, "Keeper");
        })
        .await;
}

// =============================================================================
// Novelty overlay test
// =============================================================================

#[tokio::test]
async fn fulltext_novelty_docs_scored_when_arena_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-novelty:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert two @fulltext docs (arena will exist for ex:content)
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Indexed Doc",
                "Rust programming language systems performance safety",
            )
            .await;
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "Also Indexed",
                "Rust compiler optimization techniques for fast builds",
            )
            .await;

            // Seed a *persisted* string dict entry that is NOT indexed as @fulltext.
            //
            // This ensures the novelty doc below reuses an existing string_id that:
            // - is <= persisted string watermark (so it will be emitted as EncodedLit)
            // - is NOT present in the fulltext arena (no DocBoW), reproducing the bug
            //   that previously forced arena BM25 scoring to 0.0.
            let seeded_plain_text = "Rust async runtime tokio concurrent programming patterns";
            let seed_tx = json!({
                "@context": fulltext_context(),
                "@id": "ex:seed",
                "ex:title": "Seed Plain",
                // Plain string (NOT @fulltext) — should not be indexed into the arena.
                "ex:content": seeded_plain_text
            });
            let ledger = fluree
                .insert(ledger, &seed_tx)
                .await
                .expect("seed insert")
                .ledger;

            // Index → arenas are built for docs 1 and 2 (but not for the seeded plain string)
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Verify both indexed docs are found
            let results = query_fulltext(&fluree, &loaded, "Rust").await;
            assert_eq!(
                results.len(),
                2,
                "Should find both indexed docs before novelty insert"
            );

            // Now insert a THIRD doc WITHOUT re-indexing (this is in novelty).
            // IMPORTANT: it reuses the seeded string value so the string_id is persisted,
            // but the doc is not present in the arena (novelty assertion).
            let ledger =
                insert_doc(&fluree, loaded, "ex:doc3", "Novelty Doc", seeded_plain_text).await;

            // Query the ledger with novelty — should find all 3 docs
            let results = query_fulltext(&fluree, &ledger, "Rust").await;
            assert_eq!(
                results.len(),
                3,
                "Should find indexed AND novelty docs, got: {:?}",
                results
            );

            // The novelty doc should appear with a positive score
            let novelty_result = results.iter().find(|(title, _)| title == "Novelty Doc");
            assert!(
                novelty_result.is_some(),
                "Novelty doc should appear in results"
            );
            assert!(
                novelty_result.unwrap().1 > 0.0,
                "Novelty doc should have positive score: {}",
                novelty_result.unwrap().1
            );
        })
        .await;
}

// =============================================================================
// Multiple predicates test
// =============================================================================

#[tokio::test]
async fn fulltext_multiple_predicates_independent_arenas() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-multi-pred:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert a doc with two different @fulltext predicates
            let tx = json!({
                "@context": fulltext_context(),
                "@id": "ex:doc1",
                "ex:title": "Multi-field Doc",
                "ex:content": {
                    "@value": "Rust programming language guide for beginners",
                    "@type": "@fulltext"
                },
                "ex:summary": {
                    "@value": "A comprehensive overview of Rust fundamentals",
                    "@type": "@fulltext"
                }
            });
            let ledger = fluree.insert(ledger, &tx).await.expect("insert").ledger;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Query against ex:content
            let results_content = query_fulltext(&fluree, &loaded, "programming").await;

            // Query against ex:summary using a custom query
            let bind_expr = "(fulltext ?summary \"comprehensive overview\")";
            let query = json!({
                "@context": fulltext_context(),
                "select": ["?title", "?score"],
                "where": [
                    { "@id": "?doc", "ex:summary": "?summary", "ex:title": "?title" },
                    ["bind", "?score", bind_expr],
                    ["filter", "(> ?score 0)"]
                ]
            });
            let result = fluree.query(&loaded, &query).await.expect("query summary");
            let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
            let results_summary: Vec<(String, f64)> = json_rows
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|row| {
                            let arr = row.as_array()?;
                            let title = arr.first()?.as_str()?.to_string();
                            let score = arr.get(1)?.as_f64()?;
                            Some((title, score))
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Both predicates should return results
            assert!(
                !results_content.is_empty(),
                "ex:content query should find results"
            );
            assert!(
                !results_summary.is_empty(),
                "ex:summary query should find results"
            );
        })
        .await;
}
