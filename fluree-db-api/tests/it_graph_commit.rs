#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, start_background_indexer_local, trigger_index_and_wait, MemoryFluree};

async fn seed_two_commits(
    fluree: &MemoryFluree,
    ledger_id: &str,
) -> (fluree_db_api::LedgerState, i64, i64) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let tx1 = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.expect("tx1").ledger;
    let t1 = ledger1.t();

    let tx2 = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:bob", "ex:name": "Bob"}
        ]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.expect("tx2").ledger;
    let t2 = ledger2.t();

    (ledger2, t1, t2)
}

#[tokio::test]
async fn commit_t_resolves_latest_unindexed_commit_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-commit-t-novelty:main";

    let (_ledger, _t1, t2) = seed_two_commits(&fluree, ledger_id).await;

    let detail = fluree
        .graph(ledger_id)
        .commit_t(t2)
        .execute()
        .await
        .expect("resolve commit by t from novelty");

    assert_eq!(detail.t, t2);
    assert!(!detail.id.is_empty(), "commit detail should include CID");
    assert!(
        !detail.flakes.is_empty(),
        "commit detail should include commit flakes"
    );
}

#[tokio::test]
async fn commit_t_resolves_indexed_commit_from_txn_meta_post_lookup() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-commit-t-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let (_ledger, t1, t2) = seed_two_commits(&fluree, ledger_id).await;

            trigger_index_and_wait(&handle, ledger_id, t2).await;

            let detail_latest = fluree
                .graph(ledger_id)
                .commit_t(t2)
                .execute()
                .await
                .expect("resolve indexed latest commit by t");
            assert_eq!(detail_latest.t, t2);

            let detail_earlier = fluree
                .graph(ledger_id)
                .commit_t(t1)
                .execute()
                .await
                .expect("resolve indexed earlier commit by t");
            assert_eq!(detail_earlier.t, t1);
        })
        .await;
}
