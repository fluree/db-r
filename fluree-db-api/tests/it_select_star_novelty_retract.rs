//! Regression tests for novelty retraction handling in FQL graph crawl.
//!
//! When an entity is created and then updated (upserted) within novelty
//! (both transactions after the last index), the graph crawl `select *`
//! must only return the current (post-upsert) values, not both old and new.
//!
//! SPARQL SELECT handles this correctly because the query engine deduplicates
//! overlay facts. The FQL graph crawl path goes through BinaryRangeProvider
//! which uses BinaryCursor overlay merge — and the cursor does not deduplicate
//! intra-overlay assert/retract pairs for the same fact.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, ReindexOptions};
use serde_json::{json, Value};

/// Minimal schema to populate the index before the test entity is introduced.
fn schema() -> Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:Task", "@type": "rdfs:Class", "rdfs:label": "Task"},
            {"@id": "ex:description", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}},
            {"@id": "ex:status", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}}
        ]
    })
}

fn test_context() -> Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

const TASK_IRI: &str = "urn:test:task-1";

// =============================================================================
// Core regression test: upsert in novelty → graph crawl must show only new value
// =============================================================================

/// Reproduces the bug where FQL `select *` returns both old and new values
/// after an upsert when both the original insert and the upsert are in novelty
/// (not yet indexed).
///
/// Scenario:
/// 1. Schema insert → reindex (creates binary index)
/// 2. Insert task with description "original" (novelty only)
/// 3. Upsert task with description "updated" (novelty only)
/// 4. FQL `select {IRI: ["*"]}` should return ONLY "updated"
#[tokio::test]
async fn graph_crawl_applies_novelty_retractions() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // t=1: Insert schema
    let receipt = fluree.insert(ledger0, &schema()).await.expect("schema");

    // Force index at t=1 — all subsequent transactions are novelty-only.
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // t=2: Insert task entity (in novelty, not indexed).
    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree
        .insert(receipt.ledger, &insert_txn)
        .await
        .expect("insert task");

    // t=3: Upsert — update description (retract old + assert new in novelty).
    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert task");

    // -------------------------------------------------------------------------
    // SPARQL SELECT (control): should return only the updated value.
    // -------------------------------------------------------------------------
    let sparql = format!(
        r#"PREFIX ex: <http://example.org/>
        SELECT ?desc WHERE {{ <{TASK_IRI}> ex:description ?desc }}"#,
    );
    let sparql_result = support::query_sparql(&fluree, &receipt.ledger, &sparql)
        .await
        .expect("sparql query");
    let sparql_json = sparql_result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format sparql");

    let rows = sparql_json.as_array().expect("sparql rows");
    assert_eq!(
        rows.len(),
        1,
        "SPARQL should return exactly 1 row, got {}: {:?}",
        rows.len(),
        rows
    );
    // Single-column SELECT flattens to scalar in JSON-LD format.
    assert_eq!(
        rows[0],
        json!("updated description"),
        "SPARQL should return only the updated description"
    );

    // -------------------------------------------------------------------------
    // FQL graph crawl: select {IRI: ["*"]}
    // -------------------------------------------------------------------------
    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let crawl_result = fluree
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl query");

    let formatted = serde_json::to_value(&crawl_result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    // The description should be ONLY the updated value, not an array of both.
    let desc = &node["ex:description"];
    let desc_arr = desc.as_array().expect("description should be an array");
    assert_eq!(
        desc_arr.len(),
        1,
        "FQL graph crawl should return exactly 1 description value, not both old and new.\n\
         Got: {desc}\nFull node: {node}"
    );
    let desc_val = desc_arr[0]["@value"].as_str().unwrap();
    assert_eq!(
        desc_val, "updated description",
        "Description should be the updated value"
    );

    // Status should be unchanged.
    let status = &node["ex:status"];
    let status_arr = status.as_array().expect("status should be an array");
    assert_eq!(status_arr.len(), 1);
    assert_eq!(status_arr[0]["@value"].as_str().unwrap(), "pending");
}

/// Same test but with a fresh reader instance (simulates Lambda/separate process).
/// This is closer to the production scenario where the reader loads state from storage.
#[tokio::test]
async fn graph_crawl_novelty_retractions_fresh_reader() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    // Writer: create schema, reindex, insert, upsert.
    {
        let writer = FlureeBuilder::file(path).build().expect("build writer");
        let ledger0 = writer.create_ledger("test:main").await.expect("create");

        let receipt = writer.insert(ledger0, &schema()).await.expect("schema");
        let _index = writer.reindex("test:main", ReindexOptions::default()).await;

        let insert_txn = json!({
            "@context": test_context(),
            "@id": TASK_IRI,
            "@type": "ex:Task",
            "ex:description": "original description",
            "ex:status": "pending"
        });
        let receipt = writer
            .insert(receipt.ledger, &insert_txn)
            .await
            .expect("insert");

        let upsert_txn = json!({
            "@context": test_context(),
            "@id": TASK_IRI,
            "ex:description": "updated description"
        });
        let _receipt = writer
            .upsert(receipt.ledger, &upsert_txn)
            .await
            .expect("upsert");
    }
    // Writer dropped.

    // Reader: fresh instance from storage.
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    let desc = &node["ex:description"];
    let desc_arr = desc.as_array().expect("description should be an array");
    assert_eq!(
        desc_arr.len(),
        1,
        "Fresh reader graph crawl should return exactly 1 description, not both old and new.\n\
         Got: {desc}\nFull node: {node}"
    );
    assert_eq!(
        desc_arr[0]["@value"].as_str().unwrap(),
        "updated description",
        "Description should be the updated value"
    );
}

/// Memory-backed baseline: same scenario without binary index.
/// This should always pass because the overlay-only path uses `remove_stale_flakes`.
#[tokio::test]
async fn memory_graph_crawl_novelty_retractions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree.insert(ledger0, &insert_txn).await.expect("insert");

    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert");

    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let result = support::query_jsonld(&fluree, &receipt.ledger, &crawl_query)
        .await
        .expect("query");
    let jsonld = result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format");

    let node = jsonld
        .as_array()
        .and_then(|arr| arr.first())
        .expect("one result");

    assert_eq!(
        node["ex:description"],
        json!("updated description"),
        "Memory-backed graph crawl should return only the updated description.\n\
         Got: {}\nFull: {}",
        node["ex:description"],
        node
    );
}
