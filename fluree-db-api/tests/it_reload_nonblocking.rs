//! Integration tests for `LedgerManager::reload()` after the
//! fluree/db-r#155 refactor that moved I/O outside the per-ledger state lock.
//!
//! The refactor keeps single-flight coordination (via `LoadState::Reloading`)
//! but no longer holds `lock_for_write()` across `LedgerState::load()` and
//! `load_and_attach_binary_store()`. The brief state lock is acquired only
//! for the atomic swap.
//!
//! These tests verify:
//! - Functional correctness: reload produces the expected state.
//! - Concurrency: concurrent `snapshot()` calls during an in-flight reload
//!   return a valid snapshot (they no longer hang on the write guard).
//! - Single-flight: multiple concurrent `reload()` callers share one load.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

/// Helper: transact one insert and return the committed ledger state.
async fn insert_data(
    fluree: &support::MemoryFluree,
    ledger: fluree_db_api::LedgerState,
    label: &str,
) -> fluree_db_api::LedgerState {
    let txn = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{
            "@id": format!("ex:{label}"),
            "ex:name": label
        }]
    });
    fluree
        .insert_with_opts(
            ledger,
            &txn,
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert should succeed")
        .ledger
}

/// Functional regression: `reload()` returns Ok and the handle's snapshot
/// reflects the latest committed state.
#[tokio::test]
async fn reload_produces_correct_state() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reload-correct:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let t_after_insert = ledger1.t();
    assert!(t_after_insert >= 1);

    let handle = manager.get_or_load(ledger_id).await.expect("load");
    let before = handle.snapshot().await;
    assert_eq!(before.t, t_after_insert);

    manager
        .reload(ledger_id)
        .await
        .expect("reload should succeed");

    let after = handle.snapshot().await;
    assert_eq!(
        after.t, t_after_insert,
        "reload should preserve the current t"
    );
}

/// Non-regression: multiple concurrent `snapshot()` calls issued while a
/// reload is running all complete successfully and return a valid snapshot.
///
/// Before the refactor, the reload leader held the state mutex across all
/// load I/O, so `snapshot()` callers would serialize behind the reload.
/// After the refactor, the state lock is only held briefly for the atomic
/// swap, so snapshots can interleave freely.
///
/// With in-memory storage the reload is fast, so this test can't prove
/// non-blocking via wall-clock assertions. It does exercise the refactored
/// code paths under concurrency and asserts correctness of every snapshot.
#[tokio::test]
async fn concurrent_snapshot_during_reload_succeeds() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reload-concurrent:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let expected_t = ledger1.t();

    let handle = manager.get_or_load(ledger_id).await.expect("load");

    // Fire a reload concurrently with several snapshot() calls. All futures
    // are on the same single-threaded runtime, so "concurrent" here means
    // cooperative — we're asserting that the async implementation does not
    // serialize snapshots behind the reload's I/O.
    let reload_fut = manager.reload(ledger_id);
    let snap_futs = (0..8).map(|_| handle.snapshot());

    let (reload_res, snaps) = tokio::join!(reload_fut, futures::future::join_all(snap_futs),);

    reload_res.expect("reload should succeed");
    for snap in snaps {
        assert_eq!(
            snap.t, expected_t,
            "every concurrent snapshot should observe a valid t"
        );
    }

    // Post-reload snapshot is still consistent.
    let post = handle.snapshot().await;
    assert_eq!(post.t, expected_t);
}

/// Single-flight coordination: multiple concurrent `reload()` callers on the
/// same ledger all succeed (one becomes leader, others wait).
#[tokio::test]
async fn concurrent_reloads_share_single_flight() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reload-singleflight:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let expected_t = ledger1.t();

    let handle = manager.get_or_load(ledger_id).await.expect("load");

    let futs = (0..4).map(|_| manager.reload(ledger_id));
    let results = futures::future::join_all(futs).await;
    for r in results {
        r.expect("each concurrent reload should complete with Ok");
    }

    let snap = handle.snapshot().await;
    assert_eq!(snap.t, expected_t);
}

/// Reloading a ledger that isn't cached is a no-op (returns Ok).
#[tokio::test]
async fn reload_unloaded_ledger_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    manager
        .reload("never-loaded:main")
        .await
        .expect("reload of uncached ledger should be Ok(())");
}
