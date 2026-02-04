//! Clojure-parity integration test: file-backed insert then JSON-LD query.
//!
//! Mirrors the minimal pattern from `db-clojure/test/fluree/db/transact/insert_test.clj`:
//! - connect-file (here: `FlureeBuilder::file`)
//! - create empty ledger (genesis)
//! - insert JSON-LD data
//! - query via JSON-LD query syntax
//! - reload from file-backed nameservice + storage and re-run the query

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState, Novelty, SimpleCache};
use fluree_db_core::Db;
use serde_json::json;

#[tokio::test]
async fn jsonld_insert_then_query_file_backed() {
    // Use tmpdir-backed file storage (most stable for early integration tests)
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Clojure parity defaults (see fluree/db `add-reindex-thresholds`)
    let cfg = IndexConfig::default();
    assert_eq!(cfg.reindex_min_bytes, 100_000);
    assert_eq!(cfg.reindex_max_bytes, 1_000_000);

    let alias = "rust-port/jsonld-insert-query:main";

    // Create a brand-new ledger state (genesis).
    //
    // This is the Rust equivalent of `(fluree/create conn "ledger")` prior to the first commit:
    // the nameservice has no record yet, and `commit()` will create one via publish_commit().
    let db = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db, Novelty::new(0));

    // Insert JSON-LD data (string-key syntax, no EDN).
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:age": 42
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:age": 22
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed");

    assert_eq!(committed.receipt.t, 1, "first commit should advance t to 1");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "schema:name": "?name"
        }
    });

    // Query against the returned ledger
    let result0 = fluree
        .query(&committed.ledger, &query)
        .await
        .expect("query should succeed");
    let json0 = result0
        .to_jsonld(&committed.ledger.db)
        .expect("format to jsonld");

    // Reload ledger from file-backed nameservice+storage and re-run query
    let loaded = fluree.ledger(alias).await.expect("reload ledger");
    assert_eq!(loaded.t(), 1);

    let result1 = fluree
        .query(&loaded, &query)
        .await
        .expect("query after reload");
    let json1 = result1.to_jsonld(&loaded.db).expect("format to jsonld");

    fn extract_names(v: &serde_json::Value) -> Vec<String> {
        let mut names = vec![];
        let rows = v.as_array().expect("rows should be an array");
        for row in rows {
            // 1-col SELECTs are formatted as a flat array of values; older tests used `[["Alice"], ...]`.
            let name = if let Some(cols) = row.as_array() {
                cols.get(0)
                    .and_then(|x| x.as_str())
                    .expect("name should be a string")
            } else {
                row.as_str().expect("name should be a string")
            };
            names.push(name.to_string());
        }
        names.sort();
        names
    }

    assert_eq!(
        extract_names(&json0),
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    assert_eq!(
        extract_names(&json1),
        vec!["Alice".to_string(), "Bob".to_string()]
    );
}
