//! Stable hashing integration tests (Clojure parity)
//!
//! Mirrors `db-clojure/test/fluree/db/query/stable_hashes_test.clj` at a parity level we can
//! support in Rust today:
//! - ensure identical transactions produce identical commit IDs and commit addresses

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

async fn run_once() -> (String, String) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/stable-commit-id:main";

    let ledger0 = support::genesis_ledger(&fluree, alias);

    let tx = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@flur.ee",
                "schema:age": 42
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:age": 22
            },
            {
                "@id": "ex:jane",
                "@type": "ex:User",
                "schema:name": "Jane",
                "schema:email": "jane@flur.ee",
                "schema:age": 30
            }
        ]
    });

    let result = fluree.insert(ledger0, &tx).await.expect("insert");
    assert_eq!(result.receipt.t, 1, "first commit should be at t=1");

    (result.receipt.commit_id, result.receipt.address)
}

#[tokio::test]
async fn stable_commit_id_and_address_for_identical_transaction() {
    let (id1, addr1) = run_once().await;
    let (id2, addr2) = run_once().await;

    assert_eq!(
        id1, id2,
        "commit IDs should be stable for identical transactions"
    );
    assert_eq!(
        addr1, addr2,
        "commit addresses should be stable for identical transactions"
    );
}
