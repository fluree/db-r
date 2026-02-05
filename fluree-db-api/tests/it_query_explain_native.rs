//! Explain API integration tests that require persisted index statistics.
//!
//! Mirrors `db-clojure/test/fluree/db/query/explain_test.clj` cases that depend on indexing/stats.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{
    tx::IndexingMode, CommitOpts, FlureeBuilder, IndexConfig, LedgerState, Novelty,
};
use fluree_db_core::Db;
use fluree_db_transact::TxnOpts;
use serde_json::json;
use support::start_background_indexer_local;

async fn index_and_load_db(
    fluree: &fluree_db_api::Fluree<
        fluree_db_core::MemoryStorage,
        fluree_db_nameservice::memory::MemoryNameService,
    >,
    handle: &fluree_db_indexer::IndexerHandle,
    ledger: LedgerState<fluree_db_core::MemoryStorage>,
    t: i64,
) -> LedgerState<fluree_db_core::MemoryStorage> {
    let completion = handle.trigger(ledger.alias(), t).await;
    let root = match completion.wait().await {
        fluree_db_api::IndexOutcome::Completed { root_address, .. } => root_address,
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    };

    let loaded = Db::load(fluree.storage().clone(), &root)
        .await
        .expect("Db::load(root)");
    LedgerState::new(loaded, Novelty::new(0))
}

#[tokio::test]
async fn explain_no_optimization_when_equal_selectivity() {
    // Clojure: explain-no-optimization-test
    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let alias = "test/explain:main";
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
                ..Default::default()
            };

            let txn = json!({
                "@context": {"ex":"http://example.org/"},
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:age":30},
                    {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob","ex:age":25}
                ]
            });
            let r = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let ledger = index_and_load_db(&fluree, &handle, r.ledger, r.receipt.t).await;

            let q = json!({
                "@context": {"ex":"http://example.org/"},
                "select": ["?person","?name"],
                "where": [
                    {"@id":"?person","@type":"ex:Person"},
                    {"@id":"?person","ex:name":"?name"}
                ]
            });

            let resp = fluree.explain(&ledger, &q).await.expect("explain");
            assert_eq!(resp["plan"]["optimization"], "unchanged");
            assert_eq!(resp["plan"]["original"], resp["plan"]["optimized"]);

            // SPARQL equivalent
            let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person ?name WHERE { ?person a ex:Person . ?person ex:name ?name }";
            let resp_s = fluree.explain_sparql(&ledger, sparql).await.expect("explain_sparql");
            assert_eq!(resp_s["plan"]["optimization"], "unchanged");
            assert_eq!(resp_s["plan"]["original"], resp_s["plan"]["optimized"]);
        })
        .await;
}

#[tokio::test]
async fn explain_reorders_bound_object_email_first() {
    // Clojure: explain-value-lookup-optimization-test
    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let alias = "test/optimize:main";
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
                ..Default::default()
            };

            let mut people = vec![
                json!({"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:email":"rare@example.org"}),
                json!({"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob","ex:email":"rare@example.org"}),
            ];
            for i in 2..100 {
                people.push(json!({"@id":format!("ex:person{i}"),"@type":"ex:Person","ex:name":format!("Person{i}"),"ex:email":format!("person{i}@example.org")}));
            }

            let txn = json!({"@context":{"ex":"http://example.org/"},"@graph": people});
            let r = fluree
                .insert_with_opts(ledger0, &txn, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .expect("insert");

            let ledger = index_and_load_db(&fluree, &handle, r.ledger, r.receipt.t).await;

            let q = json!({
                "@context": {"ex":"http://example.org/"},
                "select": ["?person"],
                "where": [
                    {"@id":"?person","@type":"ex:Person"},
                    {"@id":"?person","ex:email":"rare@example.org"}
                ]
            });

            let resp = fluree.explain(&ledger, &q).await.expect("explain");
            assert_eq!(resp["plan"]["optimization"], "reordered");
            assert_eq!(resp["plan"]["optimized"][0]["pattern"]["property"], "ex:email");

            // SPARQL equivalent
            let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person WHERE { ?person a ex:Person . ?person ex:email \"rare@example.org\" }";
            let resp_s = fluree.explain_sparql(&ledger, sparql).await.expect("explain_sparql");
            assert_eq!(resp_s["plan"]["optimization"], "reordered");
            assert_eq!(resp_s["plan"]["optimized"][0]["pattern"]["property"], "ex:email");
        })
        .await;
}

#[tokio::test]
async fn explain_reorders_badge_property_scan_before_class_scan() {
    // Clojure: explain-property-count-optimization-test
    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let alias = "test/property-opt:main";
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
                ..Default::default()
            };

            let mut people = Vec::new();
            for i in 0..5 {
                people.push(json!({"@id":format!("ex:person{i}"),"@type":"ex:Person","ex:name":format!("Person{i}"),"ex:badge":format!("Badge{i}")}));
            }
            for i in 5..50 {
                people.push(json!({"@id":format!("ex:person{i}"),"@type":"ex:Person","ex:name":format!("Person{i}")}));
            }
            let txn = json!({"@context":{"ex":"http://example.org/"},"@graph": people});
            let r = fluree
                .insert_with_opts(ledger0, &txn, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .expect("insert");

            let ledger = index_and_load_db(&fluree, &handle, r.ledger, r.receipt.t).await;

            let q = json!({
                "@context": {"ex":"http://example.org/"},
                "select": ["?person","?badge"],
                "where": [
                    {"@id":"?person","@type":"ex:Person"},
                    {"@id":"?person","ex:badge":"?badge"}
                ]
            });

            let resp = fluree.explain(&ledger, &q).await.expect("explain");
            assert_eq!(resp["plan"]["optimization"], "reordered");
            assert_eq!(resp["plan"]["optimized"][0]["pattern"]["property"], "ex:badge");

            // SPARQL equivalent
            let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person ?badge WHERE { ?person a ex:Person . ?person ex:badge ?badge }";
            let resp_s = fluree.explain_sparql(&ledger, sparql).await.expect("explain_sparql");
            assert_eq!(resp_s["plan"]["optimization"], "reordered");
            assert_eq!(resp_s["plan"]["optimized"][0]["pattern"]["property"], "ex:badge");
        })
        .await;
}

#[tokio::test]
async fn explain_includes_inputs_fields_and_flags() {
    // Clojure: explain-inputs-field-test
    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let alias = "test/inputs:main";
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
                ..Default::default()
            };

            let mut people = Vec::new();
            for i in 0..20 {
                people.push(json!({"@id":format!("ex:person{i}"),"@type":"ex:Person","ex:name":format!("Person{i}"),"ex:email":format!("person{i}@example.org")}));
            }
            let txn = json!({"@context":{"ex":"http://example.org/"},"@graph": people});
            let r = fluree
                .insert_with_opts(ledger0, &txn, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .expect("insert");

            let ledger = index_and_load_db(&fluree, &handle, r.ledger, r.receipt.t).await;

            let q = json!({
                "@context": {"ex":"http://example.org/"},
                "select": ["?person"],
                "where": [
                    {"@id":"?person","@type":"ex:Person"},
                    {"@id":"?person","ex:email":"person0@example.org"}
                ]
            });
            let resp = fluree.explain(&ledger, &q).await.expect("explain");
            let original = resp["plan"]["original"].as_array().expect("original array");
            let optimized = resp["plan"]["optimized"].as_array().expect("optimized array");

            assert!(original.iter().all(|p| p.get("inputs").is_some()));
            assert!(optimized.iter().all(|p| p.get("inputs").is_some()));

            // Bound object pattern should have used-values-ndv? + clamped-to-one? flags.
            let email_pat = original
                .iter()
                .find(|p| p["pattern"]["property"] == "ex:email")
                .expect("email pattern exists");
            let inputs = email_pat["inputs"].as_object().expect("inputs object");
            assert!(inputs.get("used-values-ndv?").is_some());
            assert!(inputs.get("clamped-to-one?").is_some());

            // SPARQL equivalent
            let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person WHERE { ?person a ex:Person . ?person ex:email \"person0@example.org\" }";
            let resp_s = fluree.explain_sparql(&ledger, sparql).await.expect("explain_sparql");
            let original_s = resp_s["plan"]["original"].as_array().expect("original array");
            let optimized_s = resp_s["plan"]["optimized"].as_array().expect("optimized array");
            assert!(original_s.iter().all(|p| p.get("inputs").is_some()));
            assert!(optimized_s.iter().all(|p| p.get("inputs").is_some()));
            let email_pat_s = original_s
                .iter()
                .find(|p| p["pattern"]["property"] == "ex:email")
                .expect("email pattern exists");
            let inputs_s = email_pat_s["inputs"].as_object().expect("inputs object");
            assert!(inputs_s.get("used-values-ndv?").is_some());
            assert!(inputs_s.get("clamped-to-one?").is_some());
        })
        .await;
}
