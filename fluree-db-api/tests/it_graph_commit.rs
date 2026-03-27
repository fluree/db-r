#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::{
    range_with_overlay, Flake, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
    TXN_META_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};
use serde_json::json;
use support::{
    genesis_ledger, start_background_indexer_local, trigger_index_and_wait, MemoryFluree,
};

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

async fn txn_meta_commit_flakes_for_t(
    snapshot: &fluree_db_core::LedgerSnapshot,
    overlay: &Novelty,
    target_t: i64,
    current_t: i64,
) -> Vec<Flake> {
    let predicate = Sid::new(FLUREE_DB, fluree_vocab::db::T);
    let range_match = RangeMatch::predicate_object(predicate, FlakeValue::Long(target_t));
    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(16);

    range_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Post,
        RangeTest::Eq,
        range_match,
        opts,
    )
    .await
    .expect("txn-meta POST lookup by db:t")
}

async fn assert_txn_meta_lookup_contains_commit(ledger: &LedgerState, target_t: i64) {
    let flakes = txn_meta_commit_flakes_for_t(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        target_t,
        ledger.t(),
    )
    .await;

    assert!(
        flakes.iter().any(|flake| {
            flake.p.namespace_code == FLUREE_DB
                && flake.p.name.as_ref() == fluree_vocab::db::T
                && flake.o == FlakeValue::Long(target_t)
                && flake.s.namespace_code == FLUREE_COMMIT
        }),
        "expected txn-meta POST lookup to return commit metadata for t={target_t}, got: {flakes:?}"
    );
}

#[tokio::test]
async fn commit_t_resolves_latest_unindexed_commit_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-commit-t-novelty:main";

    let (ledger, _t1, t2) = seed_two_commits(&fluree, ledger_id).await;

    assert_txn_meta_lookup_contains_commit(&ledger, t2).await;

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

            let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");

            assert_txn_meta_lookup_contains_commit(&indexed, t2).await;
            assert_txn_meta_lookup_contains_commit(&indexed, t1).await;

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
