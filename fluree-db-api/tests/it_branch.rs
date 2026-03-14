//! Branch integration tests
//!
//! Tests the branch lifecycle: creating branches, transacting on branches
//! independently, and verifying data isolation between branches.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Extract sorted name strings from query result rows.
///
/// Handles both flat strings and single-element arrays, which are the two
/// formats that single-variable `select` may return.
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
                        .and_then(|a| a[0].as_str().map(|s| s.to_string()))
                })
                .expect("each row should contain a string value")
        })
        .collect();
    names.sort();
    names
}

/// Create a branch and verify it appears in the branch list.
#[tokio::test]
async fn create_and_list_branches() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger and transact initial data so commit_head_id is set
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Create a branch
    let record = fluree.create_branch("mydb", "dev", None).await.unwrap();
    assert_eq!(record.branch, "dev");
    assert_eq!(record.ledger_id, "mydb:dev");
    let bp = record.branch_point.expect("branch should have a branch_point");
    assert_eq!(bp.source, "main");

    // List branches
    let branches = fluree.list_branches("mydb").await.unwrap();
    let mut names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["dev", "main"]);
}

/// Creating a duplicate branch returns a LedgerExists error.
#[tokio::test]
async fn create_branch_duplicate_fails() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();
    let err = fluree
        .create_branch("mydb", "dev", None)
        .await
        .expect_err("duplicate branch creation should fail");
    assert!(
        err.to_string().contains("already exists"),
        "expected LedgerExists error, got: {err}"
    );
}

/// Creating a branch from a non-existent source returns not-found.
#[tokio::test]
async fn create_branch_missing_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .create_branch("mydb", "dev", Some("nonexistent"))
        .await
        .expect_err("missing source branch should fail");
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("Not found"),
        "expected NotFound error, got: {err}"
    );
}

/// Transact divergent data on two branches and verify isolation.
///
/// This is the core branching test: after branching, transactions on one
/// branch must not be visible on the other.
#[tokio::test]
async fn branch_data_isolation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Create ledger and insert shared base data on main
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"}
        ]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_after_base = result.ledger;

    // 2. Create branch "dev" from main
    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // 3. Transact data only on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:bob", "ex:name": "Bob"}
        ]
    });
    let result = fluree.insert(main_after_base, &main_data).await.unwrap();
    let main_latest = result.ledger;

    // 4. Transact different data only on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:carol", "ex:name": "Carol"}
        ]
    });
    let result = fluree.insert(dev_ledger, &dev_data).await.unwrap();
    let dev_latest = result.ledger;

    // 5. Query both branches for all names
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });

    let main_result = support::query_jsonld(&fluree, &main_latest, &query)
        .await
        .unwrap();
    let main_rows = main_result.to_jsonld(&main_latest.snapshot).unwrap();

    let dev_result = support::query_jsonld(&fluree, &dev_latest, &query)
        .await
        .unwrap();
    let dev_rows = dev_result.to_jsonld(&dev_latest.snapshot).unwrap();

    // Main has Alice (base) + Bob (main-only)
    assert_eq!(extract_names(&main_rows), vec!["Alice", "Bob"]);

    // Dev has Alice (base) + Carol (dev-only), but NOT Bob
    assert_eq!(extract_names(&dev_rows), vec!["Alice", "Carol"]);
}

/// A branch starts at the same t as the source and advances independently.
#[tokio::test]
async fn branch_t_advances_independently() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    let result = fluree.insert(ledger, &txn).await.unwrap();
    assert_eq!(result.receipt.t, 1);

    // Branch at t=1
    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Transact twice on dev
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let txn2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:a", "ex:val": 2}]
    });
    let result = fluree.insert(dev, &txn2).await.unwrap();
    assert_eq!(result.receipt.t, 2);

    let txn3 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:b", "ex:val": 3}]
    });
    let result = fluree.insert(result.ledger, &txn3).await.unwrap();
    assert_eq!(result.receipt.t, 3);

    // Main is still at t=1
    let main = fluree.ledger("mydb:main").await.unwrap();
    assert_eq!(main.t(), 1);

    // Dev is at t=3
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    assert_eq!(dev.t(), 3);
}

/// Branching from a branch (nested branches) correctly chains namespace fallback.
///
/// main (t=1: seed) -> dev (t=2: dev-data) -> feature (t=2: feature-data)
/// Feature should see seed from main, dev-data from dev, and feature-data from itself.
#[tokio::test]
async fn nested_branch_data_isolation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Create ledger and insert seed data on main
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &seed).await.unwrap();
    let main_ledger = result.ledger;

    // 2. Branch dev from main
    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // 3. Transact on dev
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev, &dev_data).await.unwrap();

    // 4. Branch feature from dev (nested branch)
    fluree
        .create_branch("mydb", "feature", Some("dev"))
        .await
        .unwrap();

    // 5. Transact on feature
    let feature = fluree.ledger("mydb:feature").await.unwrap();
    let feature_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(feature, &feature_data).await.unwrap();

    // 6. Also transact something on main that should NOT appear on dev or feature
    let main_only = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(main_ledger, &main_only).await.unwrap();

    // 7. Query all three branches for names
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });

    // Main: Alice (seed) + Dave (main-only)
    let main = fluree.ledger("mydb:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main, &query).await.unwrap();
    let rows = result.to_jsonld(&main.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Dave"]);

    // Dev: Alice (seed from main) + Bob (dev-only), NOT Dave or Carol
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let result = support::query_jsonld(&fluree, &dev, &query).await.unwrap();
    let rows = result.to_jsonld(&dev.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Bob"]);

    // Feature: Alice (seed from main->dev chain) + Bob (from dev) + Carol (feature-only)
    let feature = fluree.ledger("mydb:feature").await.unwrap();
    let result = support::query_jsonld(&fluree, &feature, &query).await.unwrap();
    let rows = result.to_jsonld(&feature.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Bob", "Carol"]);
}
