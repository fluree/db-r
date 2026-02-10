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

    // Verify commit ID is a valid ContentId with SHA-256 digest
    let commit_id_str = result.receipt.commit_id.to_string();
    let digest_hex = result.receipt.commit_id.digest_hex();

    // CID string should be non-empty
    assert!(
        !commit_id_str.is_empty(),
        "commit ID string should not be empty"
    );

    // digest_hex should be 64 hex characters (SHA-256)
    assert_eq!(
        digest_hex.len(),
        64,
        "SHA-256 digest should be 64 hex characters, got {} chars: {}",
        digest_hex.len(),
        digest_hex
    );
    assert!(
        digest_hex.chars().all(|c| c.is_ascii_hexdigit()),
        "SHA-256 digest should contain only hex characters, got: {}",
        digest_hex
    );

    // Verify address is present and non-empty
    assert!(
        result
            .receipt
            .address
            .as_ref()
            .is_some_and(|a| !a.is_empty()),
        "commit address should be present and non-empty"
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

    // Both should have valid format (ContentId with 64-char hex digest)
    for (i, id) in [&result1.receipt.commit_id, &result2.receipt.commit_id]
        .iter()
        .enumerate()
    {
        let digest = id.digest_hex();
        assert_eq!(
            digest.len(),
            64,
            "commit {} digest should be 64 hex chars",
            i + 1
        );
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
        address.as_ref(),
        "reloaded ledger should have the same head commit address"
    );

    // Verify the commit ID format is still valid after reload
    assert_eq!(commit_id.digest_hex().len(), 64);
}
