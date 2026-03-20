//! Regression tests: querying data in a namespace introduced AFTER the binary
//! index was built must return results — both on a fresh reload (cold path)
//! and on the commit result without reload (hot path).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{FlureeBuilder, LedgerState, Novelty, TriggerIndexOptions};
use fluree_db_core::LedgerSnapshot;
use serde_json::json;

/// Cold path: insert ns1 → index → insert ns2 (no binary store on this
/// LedgerState, since no reload happened between index and t=2) → reload →
/// query ns2.
///
/// `augment_namespace_codes` in `loading.rs:73` should merge ns2 codes into
/// the binary store during the fresh reload.
#[tokio::test]
async fn cold_path_new_namespace_queryable_after_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(&path).build().expect("build");
    let (local, handle) = support::start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/cold-ns:main";
            let ledger = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));

            // t=1: insert with namespace A
            let r1 = fluree
                .insert(
                    ledger,
                    &json!({
                        "@context": { "ns1": "http://ns1.example.org/" },
                        "@graph": [{ "@id": "ns1:alice", "ns1:name": "Alice" }]
                    }),
                )
                .await
                .expect("t=1");

            // Index at t=1
            fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("index");

            // t=2: insert with NEW namespace B on the pre-reload LedgerState
            // (r1.ledger has NO binary_store — it was never reloaded after indexing)
            assert!(
                r1.ledger.binary_store.is_none(),
                "pre-reload state should not have binary store"
            );
            let _r2 = fluree
                .insert(
                    r1.ledger,
                    &json!({
                        "@context": {
                            "ns1": "http://ns1.example.org/",
                            "ns2": "http://ns2.example.org/"
                        },
                        "@graph": [{ "@id": "ns2:dave", "ns2:role": "Engineer" }]
                    }),
                )
                .await
                .expect("t=2");

            // Cold reload: loading.rs calls augment_namespace_codes with
            // snapshot.namespace_codes (which includes ns2 from commit chain)
            let loaded = fluree.ledger(ledger_id).await.expect("cold reload");
            assert!(loaded.binary_store.is_some(), "binary store after reload");

            // Query ns2 data on the freshly-loaded state
            let result = support::query_jsonld(
                &fluree,
                &loaded,
                &json!({
                    "@context": { "ns2": "http://ns2.example.org/" },
                    "select": ["?s", "?role"],
                    "where": { "@id": "?s", "ns2:role": "?role" }
                }),
            )
            .await
            .expect("query ns2");

            let rows = result.to_jsonld(&loaded.snapshot).expect("format");
            let arr = rows.as_array().expect("array");
            assert_eq!(
                arr.len(),
                1,
                "cold path: expected 1 result for ns2:dave, got: {rows}"
            );
        })
        .await;
}

/// Hot path: insert ns1 → index → reload (binary store attached) → insert ns2
/// on the reloaded state → query ns2 on the commit result (NO reload).
///
/// This is the bug scenario: the commit carries forward the Arc'd binary store
/// without augmenting its namespace codes.
#[tokio::test]
async fn hot_path_new_namespace_queryable_without_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(&path).build().expect("build");
    let (local, handle) = support::start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/hot-ns:main";
            let ledger = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));

            // t=1: insert with namespace A
            let _r1 = fluree
                .insert(
                    ledger,
                    &json!({
                        "@context": { "ns1": "http://ns1.example.org/" },
                        "@graph": [{ "@id": "ns1:alice", "ns1:name": "Alice" }]
                    }),
                )
                .await
                .expect("t=1");

            // Index at t=1, then reload to attach binary store
            fluree
                .trigger_index(ledger_id, TriggerIndexOptions::default())
                .await
                .expect("index");
            let loaded = fluree.ledger(ledger_id).await.expect("reload");
            assert!(loaded.binary_store.is_some(), "binary store after reload");

            // t=2: insert with NEW namespace B on the reloaded state
            // (binary store is attached but only knows ns1)
            let r2 = fluree
                .insert(
                    loaded,
                    &json!({
                        "@context": {
                            "ns1": "http://ns1.example.org/",
                            "ns2": "http://ns2.example.org/"
                        },
                        "@graph": [{ "@id": "ns2:dave", "ns2:role": "Engineer" }]
                    }),
                )
                .await
                .expect("t=2");

            // Query ns2 data on the commit result — NO reload
            let result = support::query_jsonld(
                &fluree,
                &r2.ledger,
                &json!({
                    "@context": { "ns2": "http://ns2.example.org/" },
                    "select": ["?s", "?role"],
                    "where": { "@id": "?s", "ns2:role": "?role" }
                }),
            )
            .await
            .expect("query ns2");

            let rows = result.to_jsonld(&r2.ledger.snapshot).expect("format");
            let arr = rows.as_array().expect("array");
            assert_eq!(
                arr.len(),
                1,
                "hot path: expected 1 result for ns2:dave, got: {rows}"
            );

            // Also verify ns1 data is still queryable
            let result_ns1 = support::query_jsonld(
                &fluree,
                &r2.ledger,
                &json!({
                    "@context": { "ns1": "http://ns1.example.org/" },
                    "select": ["?s", "?name"],
                    "where": { "@id": "?s", "ns1:name": "?name" }
                }),
            )
            .await
            .expect("query ns1");

            let rows_ns1 = result_ns1.to_jsonld(&r2.ledger.snapshot).expect("format");
            let arr_ns1 = rows_ns1.as_array().expect("array");
            assert_eq!(
                arr_ns1.len(),
                1,
                "hot path: ns1 data should still be queryable, got: {rows_ns1}"
            );
        })
        .await;
}
