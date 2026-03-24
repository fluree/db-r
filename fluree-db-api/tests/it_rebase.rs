//! Rebase integration tests
//!
//! Tests the branch rebase lifecycle: fast-forward, clean replay,
//! conflict detection with various resolution strategies, and edge cases.

mod support;

use fluree_db_api::{ConflictStrategy, FlureeBuilder};
use fluree_db_nameservice::NameService;
use serde_json::json;

/// Extract sorted name strings from query result rows.
fn extract_names(rows: &serde_json::Value) -> Vec<String> {
    let mut names: Vec<String> = rows
        .as_array()
        .expect("query result should be an array")
        .iter()
        .map(|r| {
            r.as_str()
                .map(|s| s.to_string())
                .or_else(|| {
                    r.as_array()
                        .and_then(|a| a.first().and_then(|v| v.as_str()).map(|s| s.to_string()))
                })
                .expect("each row should contain a string value")
        })
        .collect();
    names.sort();
    names
}

/// Query all ex:name values on a branch.
async fn query_all_names(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let result = support::query_jsonld(fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    extract_names(&rows)
}

/// Fast-forward rebase: branch has no unique commits, source advanced.
/// After rebase, the branch sees the source's new data.
#[tokio::test]
async fn rebase_fast_forward() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Advance main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase dev (no unique commits → fast-forward)
    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.replayed, 0);
    assert_eq!(report.total_commits, 0);

    // Dev should now see Carol (from source) + Alice (base)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert_eq!(names, vec!["Alice", "Carol"]);
}

/// Clean replay: non-overlapping changes on both branches.
/// After rebase, the branch sees both its data and the source's new data.
#[tokio::test]
async fn rebase_clean_replay() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Transact on dev (non-overlapping)
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Advance main with different data
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase
    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 1);
    assert!(report.conflicts.is_empty());
    assert!(report.failures.is_empty());

    // Dev should see Alice (base) + Carol (source) + Dave (replayed)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert_eq!(names, vec!["Alice", "Carol", "Dave"]);
}

/// Abort strategy: fail on first conflict, no changes applied.
/// After abort, the branch state is unchanged.
#[tokio::test]
async fn rebase_abort_on_conflict() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Overlapping data on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Capture pre-rebase state
    let pre_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();

    // Rebase with abort should fail
    let err = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Abort)
        .await
        .expect_err("abort strategy should fail on conflict");

    assert!(
        err.to_string().to_lowercase().contains("abort")
            || err.to_string().to_lowercase().contains("conflict"),
        "expected conflict/abort error, got: {err}"
    );

    // Branch should be unchanged (no commits written)
    let post_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pre_record.commit_t, post_record.commit_t);
    assert_eq!(pre_record.commit_head_id, post_record.commit_head_id);
}

/// TakeSource strategy: source's values win, conflicting flakes dropped.
/// Non-conflicting flakes from the branch are kept.
#[tokio::test]
async fn rebase_take_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Dev: overlapping + unique data in one commit
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice-dev"},
            {"@id": "ex:dave", "ex:name": "Dave"}
        ]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Main: overlapping data
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeSource)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());

    // Dev should have: Alice-main (source wins), Dave (non-conflicting kept)
    // The conflicting "Alice-dev" flakes were dropped.
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(names.contains(&"Dave".to_string()));
    assert!(names.contains(&"Alice-main".to_string()));
    assert!(!names.contains(&"Alice-dev".to_string()));
}

/// TakeBranch strategy: branch's values win, source's conflicting values retracted.
#[tokio::test]
async fn rebase_take_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Dev: change Alice's name
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Main: also change Alice's name differently
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());
    assert_eq!(report.replayed, 1);

    // Dev should have Alice-dev (branch wins), NOT Alice-main
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Alice-dev".to_string()),
        "expected Alice-dev in {names:?}"
    );
    assert!(
        !names.contains(&"Alice-main".to_string()),
        "Alice-main should be retracted, got {names:?}"
    );
}

/// TakeBoth strategy: both values coexist after rebase.
#[tokio::test]
async fn rebase_take_both() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Overlapping data on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());
    assert_eq!(report.replayed, 1);

    // Dev should have both Alice-dev and Alice-main (multi-cardinality)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Alice-dev".to_string()),
        "expected Alice-dev in {names:?}"
    );
    assert!(
        names.contains(&"Alice-main".to_string()),
        "expected Alice-main in {names:?}"
    );
}

/// Skip strategy: skip conflicting commits, replay non-conflicting.
/// After rebase, the non-conflicting data is present, conflicting data is not.
#[tokio::test]
async fn rebase_skip_conflicting() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Two commits on dev: first overlaps, second doesn't
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    let result = fluree.insert(dev_ledger, &dev_data1).await.unwrap();

    let dev_data2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:eve", "ex:name": "Eve"}]
    });
    fluree.insert(result.ledger, &dev_data2).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.skipped, 1);
    assert_eq!(report.replayed, 1);
    assert_eq!(report.total_commits, 2);

    // Dev should have Eve (non-conflicting, replayed onto source state)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Eve".to_string()),
        "expected Eve in {names:?}"
    );
}

/// Branch point is updated after rebase.
#[tokio::test]
async fn rebase_branch_point_updated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Advance main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let source_after = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();

    // Rebase
    fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();

    // Verify branch point updated
    let dev_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    let bp = dev_record.branch_point.expect("should have branch_point");
    assert_eq!(bp.t, source_after.commit_t);
}

/// Cannot rebase the main branch.
#[tokio::test]
async fn rebase_main_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .rebase_branch("mydb", "main", ConflictStrategy::default())
        .await
        .expect_err("rebasing main should fail");

    assert!(
        err.to_string().contains("main"),
        "expected error about main branch, got: {err}"
    );
}
