//! Integration tests for FQL `ask` boolean queries.
//!
//! Tests the `"ask": true` query form which returns a bare boolean
//! indicating whether the WHERE patterns have any solution.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_people() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/ask:people");

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30 },
            { "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 25 }
        ]
    });

    let committed = fluree.insert(ledger, &tx).await.expect("insert people");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn ask_true_when_match_exists() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:name": "Alice" }
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));
}

#[tokio::test]
async fn ask_false_when_no_match() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:name": "Charlie" }
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_filter() {
    let (fluree, ledger) = seed_people().await;

    // Alice is 30, Bob is 25 — only Alice matches > 28
    let query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:age": "?age" },
            ["filter", "(> ?age 28)"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));

    // Nobody is older than 50
    let query_no_match = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:age": "?age" },
            ["filter", "(> ?age 50)"]
        ]
    });

    let result = fluree
        .query(&ledger, &query_no_match)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_type_pattern() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "@type": "ex:Person" }
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));

    // No Animals exist
    let query_no_match = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?x", "@type": "ex:Animal" }
        ]
    });

    let result = fluree
        .query(&ledger, &query_no_match)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_optional() {
    let (fluree, ledger) = seed_people().await;

    // Base pattern matches (Alice exists), OPTIONAL just adds more bindings
    let query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:name": "Alice" },
            ["optional", { "@id": "?person", "ex:email": "?email" }]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));
}

#[tokio::test]
async fn ask_requires_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/ask:no-where");

    let query = json!({
        "@context": ctx(),
        "ask": true
    });

    let result = fluree.query(&ledger, &query).await;
    assert!(result.is_err(), "ask without where should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("where"),
        "error should mention 'where': {err}"
    );
}

#[tokio::test]
async fn ask_rejects_non_true() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/ask:non-true");

    // "ask": 1 — not exactly true
    let result = fluree
        .query(&ledger, &json!({"ask": 1, "where": []}))
        .await;
    assert!(result.is_err(), "ask: 1 should be rejected");

    // "ask": "yes" — not exactly true
    let result = fluree
        .query(&ledger, &json!({"ask": "yes", "where": []}))
        .await;
    assert!(result.is_err(), "ask: \"yes\" should be rejected");

    // "ask": false — not exactly true
    let result = fluree
        .query(&ledger, &json!({"ask": false, "where": []}))
        .await;
    assert!(result.is_err(), "ask: false should be rejected");
}

#[tokio::test]
async fn ask_sparql_parity() {
    let (fluree, ledger) = seed_people().await;

    // FQL ask
    let fql_query = json!({
        "@context": ctx(),
        "ask": true,
        "where": [
            { "@id": "?person", "ex:name": "Alice" }
        ]
    });

    let fql_result = fluree.query(&ledger, &fql_query).await.expect("fql query");
    let fql_json = fql_result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("fql format");

    // SPARQL ASK
    let sparql_result = fluree
        .query_sparql(
            &ledger,
            "PREFIX ex: <http://example.org/ns/> ASK { ?person ex:name \"Alice\" }",
        )
        .await
        .expect("sparql query");

    // SPARQL ASK returns W3C envelope via to_sparql_json
    let sparql_json = sparql_result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql format");

    // Both should indicate true — FQL as bare bool, SPARQL as W3C envelope
    assert_eq!(fql_json, JsonValue::Bool(true));
    assert_eq!(sparql_json["boolean"], true);
}
