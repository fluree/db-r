//! Background indexing wait integration test (Clojure parity)
//!
//! Clojure tests commonly do:
//! - commit with `{:index-files-ch index-ch}`
//! - block on `(block-until-index-complete index-ch)`
//! - then reload/query/assert against the persisted index
//!
//! Rust equivalent:
//! - transact (capture `receipt.t`)
//! - `handle.trigger(alias, receipt.t)`
//! - `completion.wait().await`
//! - then load `Db` from `root_address` and assert `db.t >= receipt.t`

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::Db;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::start_background_indexer_local;

#[tokio::test]
async fn background_indexing_trigger_wait_then_load_index_root() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    // Build file-backed Fluree (so we can load the index root from storage).
    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    // Start background indexing worker + handle (LocalSet since worker may be !Send).
    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            // Genesis ledger state (uncommitted; nameservice record created on first commit).
            let alias = "it/index-wait:main";
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger0 = fluree_db_api::LedgerState::new(db0, fluree_db_api::Novelty::new(0));

            // Force indexing_needed=true for the test.
            // Must be large enough to allow the novelty write; we just want min_bytes=0
            // so background indexing is always triggered.
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };

            // 1) Transact
            let tx = json!({
                "@context": {"ex":"http://example.org/"},
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            let commit_t = result.receipt.t;
            assert!(commit_t >= 0);

            // 2) Trigger indexing predicate: index_t >= commit_t
            let completion = handle.trigger(result.ledger.alias(), commit_t).await;

            // 3) Wait + assert we can load the persisted root
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed {
                    index_t,
                    root_address,
                } => {
                    assert!(
                        index_t >= commit_t,
                        "index_t ({index_t}) should be >= commit_t ({commit_t})"
                    );
                    assert!(
                        !root_address.is_empty(),
                        "expected a non-empty root_address after indexing"
                    );

                    let loaded = Db::load(fluree.storage().clone(), &root_address)
                        .await
                        .expect("Db::load(root_address)");
                    assert!(
                        loaded.t >= commit_t,
                        "loaded db.t ({}) should be >= commit_t ({})",
                        loaded.t,
                        commit_t
                    );
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }
        })
        .await;
}
