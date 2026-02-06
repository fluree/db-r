//! FQL (Fluree Query Language) integration tests (Clojure parity)
//!
//! Mirrors `db-clojure/test/fluree/db/query/fql_test.clj` using JSON inputs only.
//! All inserts and queries are explicit with `@context`.
//!
//! FQL features covered:
//! - Grouping with single/multiple fields
//! - Having clauses with aggregate functions
//! - Ordering with single/multiple fields and directions
//! - Pagination (offset/limit)
//! - DISTINCT
//! - UNION queries
//! - Filter functions
//! - Select variations
//! - Where clause patterns

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, MemoryFluree, MemoryLedger};

async fn seed_people(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);
    let ctx = context_ex_schema();

    // Matches `db-clojure/test/fluree/db/test_utils.cljc` `people-strings`
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 50,
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:favNums": [5, 10]
            },
            {
                "@id": "ex:liam",
                "@type": "ex:User",
                "schema:name": "Liam",
                "schema:email": "liam@example.org",
                "schema:age": 13,
                "ex:favNums": [42, 11]
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn grouping_single_field() {
    // Clojure: "with a single grouped-by field"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/grouping:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age", "?favNums"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age",
            "ex:favNums": "?favNums"
        },
        "group-by": "?name",
        "order-by": "?name"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected grouped results per Clojure test
    let expected = json!([
        [
            "Alice",
            [
                "alice@example.org",
                "alice@example.org",
                "alice@example.org"
            ],
            [50, 50, 50],
            [9, 42, 76]
        ],
        ["Brian", ["brian@example.org"], [50], [7]],
        [
            "Cam",
            ["cam@example.org", "cam@example.org"],
            [34, 34],
            [5, 10]
        ],
        [
            "Liam",
            ["liam@example.org", "liam@example.org"],
            [13, 13],
            [11, 42]
        ]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn grouping_multiple_fields() {
    // Clojure: "with multiple grouped-by fields"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/grouping-multi:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age", "?favNums"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age",
            "ex:favNums": "?favNums"
        },
        "group-by": ["?name", "?email", "?age"],
        "order-by": "?name"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected grouped results per Clojure test
    let expected = json!([
        ["Alice", "alice@example.org", 50, [9, 42, 76]],
        ["Brian", "brian@example.org", 50, [7]],
        ["Cam", "cam@example.org", 34, [5, 10]],
        ["Liam", "liam@example.org", 13, [11, 42]]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn having_count_filter() {
    // Clojure: "with having clauses" - count filter
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/having-count:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favNums"],
        "where": {
            "schema:name": "?name",
            "ex:favNums": "?favNums"
        },
        "group-by": "?name",
        "having": "(>= (count ?favNums) 2)"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected filtered results per Clojure test
    let expected = json!([["Alice", [9, 42, 76]], ["Cam", [5, 10]], ["Liam", [11, 42]]]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn having_avg_filter() {
    // Clojure: "with having clauses" - avg filter
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/having-avg:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favNums"],
        "where": {
            "schema:name": "?name",
            "ex:favNums": "?favNums"
        },
        "group-by": "?name",
        "having": "(>= (avg ?favNums) 10)"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected filtered results per Clojure test
    let expected = json!([["Alice", [9, 42, 76]], ["Liam", [11, 42]]]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn ordering_single_field_asc() {
    // Clojure: "with a single ordered field" (ascending)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/ordering-asc:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age"
        },
        "order-by": "?name"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected ordered results per Clojure test
    let expected = json!([
        ["Alice", "alice@example.org", 50],
        ["Brian", "brian@example.org", 50],
        ["Cam", "cam@example.org", 34],
        ["Liam", "liam@example.org", 13]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn ordering_single_field_desc() {
    // Clojure: "with a specified direction" (descending)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/ordering-desc:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age"
        },
        "order-by": "(desc ?name)"
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected ordered results per Clojure test
    let expected = json!([
        ["Liam", "liam@example.org", 13],
        ["Cam", "cam@example.org", 34],
        ["Brian", "brian@example.org", 50],
        ["Alice", "alice@example.org", 50]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn ordering_multiple_fields_asc() {
    // Clojure: "with multiple ordered fields" (ascending)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/ordering-multi-asc:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age"
        },
        "order-by": ["?age", "?name"]
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected ordered results per Clojure test
    let expected = json!([
        ["Liam", "liam@example.org", 13],
        ["Cam", "cam@example.org", 34],
        ["Alice", "alice@example.org", 50],
        ["Brian", "brian@example.org", 50]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn ordering_multiple_fields_mixed() {
    // Clojure: "with a specified direction" (multiple fields, mixed directions)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/ordering-multi-mixed:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email", "?age"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "schema:age": "?age"
        },
        "order-by": ["(desc ?age)", "?name"]
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected ordered results per Clojure test
    let expected = json!([
        ["Alice", "alice@example.org", 50],
        ["Brian", "brian@example.org", 50],
        ["Cam", "cam@example.org", 34],
        ["Liam", "liam@example.org", 13]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn select_distinct_basic() {
    // Clojure: "distinct results"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/select-distinct:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select-distinct": ["?name", "?email"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "ex:favNums": "?favNum"
        }
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected distinct results per Clojure test
    let expected = json!([
        ["Alice", "alice@example.org"],
        ["Brian", "brian@example.org"],
        ["Cam", "cam@example.org"],
        ["Liam", "liam@example.org"]
    ]);

    assert_eq!(rows, expected);
}

#[tokio::test]
async fn select_distinct_with_limit_offset() {
    // Clojure: "distinct results with limit and offset"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/fql/select-distinct-limit:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select-distinct": ["?name", "?email"],
        "where": {
            "schema:name": "?name",
            "schema:email": "?email",
            "ex:favNums": "?favNum"
        },
        "limit": 2,
        "offset": 1
    });

    let result = fluree.query(&ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.db).unwrap();

    // Expected distinct results with limit/offset per Clojure test
    let expected = json!([["Brian", "brian@example.org"], ["Cam", "cam@example.org"]]);

    assert_eq!(rows, expected);
}
