#![cfg(feature = "vector")]
//! Vector flatrank integration tests (Clojure parity)
//
//! Ports from `db-clojure/test/fluree/db/vector/flatrank_test.clj`.
//! Tests vector search functionality with dot product, cosine similarity,
//! and euclidean distance scoring functions.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Integration test for basic vector search with dot product scoring
#[tokio::test]
async fn vector_search_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-score:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data with vectors
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"},
                "ex:age": 36
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"},
                "ex:age": "forever 10"
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Test query with dot product scoring
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ]
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Expected: [["ex:bart" 0.61 [0.1, 0.9]], ["ex:homer" 0.72 [0.6, 0.5]]]
    // Sort by score for consistent comparison
    let mut results: Vec<(String, f64, Vec<f64>)> = arr
        .iter()
        .map(|row| {
            let row_arr = row.as_array().unwrap();
            let id = row_arr[0].as_str().unwrap().to_string();
            let score = row_arr[1].as_f64().unwrap();
            let vec = row_arr[2]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect::<Vec<f64>>();
            (id, score, vec)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap()); // Sort by score descending

    assert_eq!(results[0].0, "ex:homer");
    assert!((results[0].1 - 0.72).abs() < 0.001);
    assert_eq!(results[0].2, vec![0.6, 0.5]);

    assert_eq!(results[1].0, "ex:bart");
    assert!((results[1].1 - 0.61).abs() < 0.001);
    assert_eq!(results[1].2, vec![0.1, 0.9]);
}

/// Test filtering results based on other properties
#[tokio::test]
async fn vector_search_with_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-score-filter:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"},
                "ex:age": 36
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"},
                "ex:age": "forever 10"
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with age filter
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:age": 36, "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ]
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should return only Homer (age 36)");

    let row = &arr[0];
    let row_arr = row.as_array().unwrap();
    assert_eq!(row_arr[0], "ex:homer");
    assert!((row_arr[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}

/// Test applying filters to score values
#[tokio::test]
async fn vector_search_score_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-score-threshold:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"}
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with score threshold filter
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]],
            ["filter", [">", "?score", 0.7]]
        ]
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should return only results with score > 0.7");

    let row = &arr[0];
    let row_arr = row.as_array().unwrap();
    assert_eq!(row_arr[0], "ex:homer");
    assert!(row_arr[1].as_f64().unwrap() > 0.7);
}

/// Test multi-cardinality vector values
#[tokio::test]
async fn vector_search_multi_cardinality() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-score-multi:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data with multiple vectors per entity
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": [
                    {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"},
                    {"@value": [0.2, 0.9], "@type": "https://ns.flur.ee/ledger#vector"}
                ]
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with dot product scoring - should return multiple results for Bart
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(
        arr.len(),
        3,
        "Should return 3 results (1 for Homer, 2 for Bart)"
    );

    // Expected order by score: [Bart(0.61), Bart(0.68), Homer(0.72)]
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:bart");
    assert!((row0[1].as_f64().unwrap() - 0.61).abs() < 0.001);

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:bart");
    assert!((row1[1].as_f64().unwrap() - 0.68).abs() < 0.001);

    let row2 = arr[2].as_array().unwrap();
    assert_eq!(row2[0], "ex:homer");
    assert!((row2[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}

/// Test cosine similarity scoring
#[tokio::test]
async fn vector_search_cosine_similarity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-cosine:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with cosine similarity
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["cosineSimilarity", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Results should be ordered by cosine similarity
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:bart");

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:homer");
}

/// Test euclidean distance scoring
#[tokio::test]
async fn vector_search_euclidean_distance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-euclidean:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with euclidean distance
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["euclideanDistance", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Results should be ordered by euclidean distance (ascending)
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:homer"); // Homer should be closer

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:bart"); // Bart should be farther
}

/// Test mixed datatypes (vectors and non-vectors)
#[tokio::test]
async fn vector_search_mixed_datatypes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_alias = "test/vector-mixed:main";
    let ledger0 = fluree.create_ledger(ledger_alias).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/ledger#"}
    ]);

    // Insert test data with mixed datatypes
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/ledger#vector"}
            },
            {
                "@id": "ex:lucy",
                "ex:xVec": "Not a Vector"
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/ledger#vector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query should handle mixed datatypes gracefully
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/ledger#vector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(
        arr.len(),
        3,
        "Should return 3 results (including non-vector)"
    );

    // Lucy should have null score due to non-vector value
    let lucy_row = arr
        .iter()
        .find(|row| row.as_array().unwrap()[0] == "ex:lucy")
        .unwrap();
    let lucy_arr = lucy_row.as_array().unwrap();
    assert_eq!(lucy_arr[1], serde_json::Value::Null);
    assert_eq!(lucy_arr[2], "Not a Vector");

    // Vector results should be properly scored
    let homer_row = arr
        .iter()
        .find(|row| row.as_array().unwrap()[0] == "ex:homer")
        .unwrap();
    let homer_arr = homer_row.as_array().unwrap();
    assert!((homer_arr[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}
