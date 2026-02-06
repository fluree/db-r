//! Transact upsert integration tests (Clojure parity)
//!
//! Ports integration tests from `db-clojure/test/fluree/db/transact/upsert_test.clj`.
//! Tests upsert functionality where existing data gets replaced rather than merged.

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::comparator::IndexType;
use serde_json::json;
use support::normalize_rows;

// Helper function to create a standard context
fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    })
}

/// Test that OPTIONAL patterns work in transaction WHERE clauses.
///
/// This test verifies that the query parser's full pattern support (including OPTIONAL)
/// is now available in transactions after the parser unification refactor.
#[tokio::test]
async fn upsert_parsing() {
    // Clojure parity: transactions with OPTIONAL patterns in WHERE clause.
    // The key behavior is that OPTIONAL allows "delete if exists" semantics
    // without failing when the data doesn't exist.

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-parsing:main")
        .await
        .unwrap();

    // Insert initial data - only alice has both name and age
    let initial_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "schema:name": "Alice", "ex:age": 30}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Update with OPTIONAL pattern - should work even for fields that don't exist
    // This is the Clojure upsert pattern: use OPTIONAL so missing fields don't fail
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "where": [
            ["optional", {"@id": "ex:alice", "schema:name": "?name"}],
            ["optional", {"@id": "ex:alice", "ex:nickname": "?nick"}]
        ],
        "delete": [
            {"@id": "ex:alice", "schema:name": "?name"},
            {"@id": "ex:alice", "ex:nickname": "?nick"}
        ],
        "insert": [
            {"@id": "ex:alice", "schema:name": "Alice Updated", "ex:nickname": "Ali"}
        ]
    });

    // This should succeed - OPTIONAL allows missing patterns
    let result = fluree.update(ledger1, &update_txn).await;
    assert!(
        result.is_ok(),
        "Update with OPTIONAL should succeed: {:?}",
        result.err()
    );

    let ledger2 = result.unwrap().ledger;

    // Query to verify the update worked
    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "where": {"@id": "ex:alice"},
        "select": {"ex:alice": ["*"]}
    });

    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();

    let alice = &jsonld[0];
    // Name should be updated
    assert_eq!(alice["schema:name"], json!("Alice Updated"));
    // Nickname should be added
    assert_eq!(alice["ex:nickname"], json!("Ali"));
    // Age should still be there (wasn't touched by the update)
    assert_eq!(alice["ex:age"], json!(30));
}

#[tokio::test]
async fn upsert_data() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree.create_ledger("tx/upsert-test:main").await.unwrap();

    // First insert some initial data
    let initial_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });

    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Now upsert - this should replace existing data
    let upsert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:nums": [4, 5, 6], "schema:name": "Alice2"},
            {"@id": "ex:bob", "ex:nums": [4, 5, 6], "schema:name": "Bob2"},
            {"@id": "ex:jane", "ex:nums": [4, 5, 6], "schema:name": "Jane2"}
        ]
    });

    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query to verify upsert behavior
    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });

    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();

    // Should have 3 users with updated data
    assert_eq!(jsonld.as_array().unwrap().len(), 3);

    let mut users: Vec<_> = jsonld
        .as_array()
        .unwrap()
        .iter()
        .map(|user| {
            let obj = user.as_object().unwrap();
            let id = obj["@id"].as_str().unwrap();
            let name = obj["schema:name"].as_str().unwrap();
            let nums = obj["ex:nums"].clone();
            (id.to_string(), name.to_string(), nums)
        })
        .collect();

    users.sort_by(|a, b| a.0.cmp(&b.0));

    // Alice should have updated name and nums, but keep original age and type
    assert_eq!(users[0].0, "ex:alice");
    assert_eq!(users[0].1, "Alice2");
    assert_eq!(users[0].2, json!([4, 5, 6]));

    // Bob should have updated name and nums, but keep original age and type
    assert_eq!(users[1].0, "ex:bob");
    assert_eq!(users[1].1, "Bob2");
    assert_eq!(users[1].2, json!([4, 5, 6]));

    // Jane should be new with just name and nums
    assert_eq!(users[2].0, "ex:jane");
    assert_eq!(users[2].1, "Jane2");
    assert_eq!(users[2].2, json!([4, 5, 6]));
}

#[tokio::test]
async fn upsert_no_changes() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = fluree.create_ledger("tx/upsert2:main").await.unwrap();
    let sample_insert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });
    let ledger1 = fluree
        .insert(ledger0, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });
    let result1 = fluree.query(&ledger1, &query).await.unwrap();
    let jsonld1 = result1.to_jsonld_async(&ledger1.db).await.unwrap();

    let ledger2 = fluree
        .upsert(ledger1, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;
    let result2 = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld2 = result2.to_jsonld_async(&ledger2.db).await.unwrap();

    let ledger3 = fluree
        .upsert(ledger2, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;
    let result3 = fluree.query(&ledger3, &query).await.unwrap();
    let jsonld3 = result3.to_jsonld_async(&ledger3.db).await.unwrap();

    assert_eq!(normalize_rows(&jsonld1), normalize_rows(&jsonld2));
    assert_eq!(normalize_rows(&jsonld1), normalize_rows(&jsonld3));
}

#[tokio::test]
async fn upsert_multicardinal_data() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree.create_ledger("tx/upsert3:main").await.unwrap();

    // Insert initial multicardinal data
    let initial_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "ex:letter": ["a", "b", "c", "d"], "ex:num": [2, 4, 6, 8]},
            {"@id": "ex:bob", "@type": "ex:User", "ex:letter": ["a", "b", "c", "d"], "ex:num": [2, 4, 6, 8]}
        ]
    });

    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Upsert to replace multicardinal properties
    let upsert_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:letter": ["e", "f", "g", "h"], "ex:num": [3, 5, 7, 9]},
            {"@id": "ex:bob", "ex:letter": ["e", "f", "g", "h"], "ex:num": [3, 5, 7, 9]}
        ]
    });

    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query to verify multicardinal upsert worked
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {"@id": "?s", "@type": "ex:User"},
        "select": {"?s": ["*"]}
    });

    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();

    // Should have 2 users with updated multicardinal data
    assert_eq!(jsonld.as_array().unwrap().len(), 2);

    for user in jsonld.as_array().unwrap() {
        let obj = user.as_object().unwrap();
        assert_eq!(obj["ex:letter"], json!(["e", "f", "g", "h"]));
        assert_eq!(obj["ex:num"], json!([3, 5, 7, 9]));
        assert_eq!(obj["@type"], json!("ex:User"));
    }
}

#[tokio::test]
async fn upsert_cancels_identical_pairs_in_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-cancel-pairs:main")
        .await
        .unwrap();

    let ctx = json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    });
    let insert = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:nums": [1, 2]
        }]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let upsert = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:alice",
            "schema:name": "Alice2",
            "ex:nums": [1, 2, 3]
        }]
    });
    let ledger2 = fluree.upsert(ledger1, &upsert).await.unwrap().ledger;

    let s = ledger2
        .db
        .encode_iri("http://example.org/ns/alice")
        .expect("subject sid");
    let p_name = ledger2
        .db
        .encode_iri("http://schema.org/name")
        .expect("name sid");
    let p_nums = ledger2
        .db
        .encode_iri("http://example.org/ns/nums")
        .expect("nums sid");

    let spot_ids: Vec<_> = ledger2.novelty.iter_index(IndexType::Spot).collect();
    let mut name_flakes = 0;
    let mut nums_flakes = 0;
    for id in spot_ids {
        let flake = ledger2.novelty.get_flake(id);
        if flake.s == s {
            if flake.p == p_name {
                name_flakes += 1;
            } else if flake.p == p_nums {
                nums_flakes += 1;
            }
        }
    }

    assert_eq!(
        name_flakes, 3,
        "schema:name asserts Alice, then retracts Alice and asserts Alice2"
    );
    assert_eq!(
        nums_flakes, 3,
        "ex:nums went from [1 2] to [1 2 3], total of 3 flakes"
    );
}

#[tokio::test]
async fn upsert_and_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/upsert:main").await.unwrap();

    let sample_insert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });
    let ledger1 = fluree
        .insert(ledger0, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;

    let sample_upsert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:nums": [4, 5, 6], "schema:name": "Alice2"},
            {"@id": "ex:bob", "ex:nums": [4, 5, 6], "schema:name": "Bob2"},
            {"@id": "ex:jane", "ex:nums": [4, 5, 6], "schema:name": "Jane2"}
        ]
    });
    let ledger2 = fluree
        .upsert(ledger1, &sample_upsert_txn)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });
    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            {"@id":"ex:alice","@type":"ex:User","schema:age":42,"ex:nums":[4,5,6],"schema:name":"Alice2"},
            {"@id":"ex:bob","@type":"ex:User","schema:age":22,"ex:nums":[4,5,6],"schema:name":"Bob2"},
            {"@id":"ex:jane","ex:nums":[4,5,6],"schema:name":"Jane2"}
        ]))
    );
}
