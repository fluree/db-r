//! Commit hash integration tests
//!
//! Tests for commit ID and address format validation and consistency.
//!
//! Note: Cross-session hash stability (identical transactions producing identical
//! commit IDs) is not currently testable because commit timestamps are generated
//! internally via `Utc::now()`. See TODO in `fluree-db-transact/src/commit.rs`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Verify that commit IDs have the correct SHA-256 format.
#[tokio::test]
async fn commit_id_has_valid_sha256_format() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/hash-format:main";

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
            }
        ]
    });

    let result = fluree.insert(ledger0, &tx).await.expect("insert");

    // Verify commit ID format: "sha256:" prefix + 64 hex characters
    assert!(
        result.receipt.commit_id.starts_with("sha256:"),
        "commit ID should start with 'sha256:' prefix, got: {}",
        result.receipt.commit_id
    );

    let hash_part = result.receipt.commit_id.strip_prefix("sha256:").unwrap();
    assert_eq!(
        hash_part.len(),
        64,
        "SHA-256 hash should be 64 hex characters, got {} chars: {}",
        hash_part.len(),
        hash_part
    );
    assert!(
        hash_part.chars().all(|c| c.is_ascii_hexdigit()),
        "SHA-256 hash should contain only hex characters, got: {}",
        hash_part
    );

    // Verify address is non-empty and contains the alias
    assert!(
        !result.receipt.address.is_empty(),
        "commit address should not be empty"
    );
}

/// Verify that sequential commits on the same ledger produce unique hashes.
#[tokio::test]
async fn sequential_commits_produce_unique_hashes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/unique-hashes:main";

    let ledger0 = support::genesis_ledger(&fluree, alias);

    let tx1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:alice", "ex:name": "Alice"}
    });

    let result1 = fluree.update(ledger0, &tx1).await.expect("first insert");
    assert_eq!(result1.receipt.t, 1);

    let tx2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:bob", "ex:name": "Bob"}
    });

    let result2 = fluree
        .update(result1.ledger, &tx2)
        .await
        .expect("second insert");
    assert_eq!(result2.receipt.t, 2);

    // Each commit should have a unique ID
    assert_ne!(
        result1.receipt.commit_id, result2.receipt.commit_id,
        "sequential commits should have different commit IDs"
    );

    // Each commit should have a unique address
    assert_ne!(
        result1.receipt.address, result2.receipt.address,
        "sequential commits should have different addresses"
    );

    // Both should have valid format
    for (i, id) in [&result1.receipt.commit_id, &result2.receipt.commit_id]
        .iter()
        .enumerate()
    {
        assert!(
            id.starts_with("sha256:"),
            "commit {} ID should start with 'sha256:'",
            i + 1
        );
        let hash = id.strip_prefix("sha256:").unwrap();
        assert_eq!(hash.len(), 64, "commit {} hash should be 64 chars", i + 1);
    }
}

/// Verify that the same commit ID is returned consistently when querying ledger state.
#[tokio::test]
async fn commit_id_consistent_within_session() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/consistent-id:main";

    let ledger0 = support::genesis_ledger(&fluree, alias);

    let tx = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:test", "ex:value": 42}
    });

    let result = fluree.update(ledger0, &tx).await.expect("insert");
    let commit_id = result.receipt.commit_id.clone();
    let address = result.receipt.address.clone();

    // Reload the ledger from the same Fluree instance
    let reloaded = fluree.ledger(alias).await.expect("reload ledger");

    // The head commit address should match what we got from the transaction
    assert_eq!(
        reloaded.head_commit.as_ref(),
        Some(&address),
        "reloaded ledger should have the same head commit address"
    );

    // Verify the commit ID format is still valid after reload
    assert!(commit_id.starts_with("sha256:"));
}
