//! Regression tests for GitHub issue #95:
//! `select *` crawl returns empty properties for novelty-only entities when
//! `index_t < commit_t`.
//!
//! Faithfully migrated from fluree-lambda-router's select_star_crawl.rs tests.
//! These reproduce the exact production pattern:
//!   1. Bootstrap a `_system` ledger with a large ontology
//!   2. Reindex (creating index_t = ontology commit)
//!   3. Insert entity data (novelty only, not indexed)
//!   4. Query with `select *` via various paths

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, GraphDb, ReindexOptions};
use serde_json::{json, Value};

/// The Solo system ontology (~24KB JSON-LD with classes and properties).
const ONTOLOGY_JSON: &str = include_str!("fixtures/solo_ontology.json");

/// The @context used in Solo system queries.
fn fsys_context() -> Value {
    json!({
        "fsys": "https://ns.flur.ee/system#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "schema": "http://schema.org/",
        "f": "https://ns.flur.ee/db#"
    })
}

/// Build the entity JSON-LD matching IdentityProviderConfig::to_json_ld().
fn test_idp_json_ld() -> Value {
    json!({
        "@context": fsys_context(),
        "@id": "urn:fsys:idp:saml-CrawlTest",
        "@type": "fsys:IdentityProviderConfig",
        "fsys:providerId": "saml-CrawlTest",
        "fsys:providerName": "Crawl Test",
        "fsys:providerType": "saml",
        "fsys:enabled": true,
        "fsys:cognitoProviderName": "saml-CrawlTest",
        "fsys:attributeMapping": {
            "@value": {"email": "email", "groups": "groups"},
            "@type": "@json"
        },
        "fsys:createdBy": {"@id": "urn:fsys:user:user-1"},
        "fsys:metadataUrl": {
            "@value": "https://example.com/metadata",
            "@type": "xsd:string"
        },
        "schema:dateCreated": "2026-01-01T00:00:00Z",
        "schema:dateModified": "2026-01-01T00:00:00Z"
    })
}

const IRI: &str = "urn:fsys:idp:saml-CrawlTest";

fn assert_has_properties(node: &Value, label: &str) {
    let keys: Vec<&String> = node.as_object().unwrap().keys().collect();
    assert!(
        keys.len() > 1,
        "{label}: select * should return properties beyond @id, got only: {keys:?}\nfull: {node}"
    );
}

// =============================================================================
// Memory-backed tests (baseline — these should pass)
// =============================================================================

#[tokio::test]
async fn memory_select_star_with_receipt_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_idp_json_ld();
    let receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let db = GraphDb::from_ledger_state(&receipt.ledger);
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree.query(&db, &query).await.expect("query");
    let formatted = result
        .format_async(db.as_graph_db_ref(), &config)
        .await
        .expect("format");

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "memory select * with receipt");
}

#[tokio::test]
async fn memory_select_star_via_query_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_idp_json_ld();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "memory select * via query_from");
}

// =============================================================================
// File-backed tests (same index/commit path as S3)
// =============================================================================

#[tokio::test]
async fn file_select_star_with_receipt_ledger() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build file-backed fluree");

    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_idp_json_ld();
    let receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let db = GraphDb::from_ledger_state(&receipt.ledger);
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree.query(&db, &query).await.expect("query");
    let formatted = result
        .format_async(db.as_graph_db_ref(), &config)
        .await
        .expect("format");

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * with receipt");
}

#[tokio::test]
async fn file_select_star_with_rediscovered_ledger() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build file-backed fluree");

    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_idp_json_ld();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    // Re-discover the ledger state from disk (simulates Query Lambda)
    let ledger_state = fluree
        .ledger("_system:main")
        .await
        .expect("re-discover ledger");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let db = GraphDb::from_ledger_state(&ledger_state);
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree.query(&db, &query).await.expect("query");
    let formatted = result
        .format_async(db.as_graph_db_ref(), &config)
        .await
        .expect("format");

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * with re-discovered ledger");
}

#[tokio::test]
async fn file_select_star_via_query_from() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build file-backed fluree");

    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_idp_json_ld();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    eprintln!(
        "file query_from result: {}",
        serde_json::to_string_pretty(&formatted).unwrap()
    );

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * via query_from");
}

/// Simulate the two-process pattern: write with one Fluree instance,
/// read with a completely separate instance pointing at the same directory.
#[tokio::test]
async fn file_select_star_separate_reader() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    // ── Writer ──
    {
        let writer = FlureeBuilder::file(path).build().expect("build writer");
        let ledger0 = writer.create_ledger("_system:main").await.expect("create");
        let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
        let receipt = writer.insert(ledger0, &ontology).await.expect("bootstrap");

        let entity = test_idp_json_ld();
        let _receipt = writer
            .insert(receipt.ledger, &entity)
            .await
            .expect("insert");
    }
    // Writer is dropped — all state is on disk

    // ── Reader (separate instance, same directory) ──
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from with separate reader");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    eprintln!(
        "separate reader result: {}",
        serde_json::to_string_pretty(&formatted).unwrap()
    );

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * with separate reader");
}

// =============================================================================
// Index gap tests: reproduce production condition (index_t < commit_t)
// =============================================================================

/// Reproduce the production _system ledger state:
///   t=1: ontology insert (INDEXED)
///   t=2+: entity data (NOT indexed, in novelty only)
///
/// Then query with select * to see if the crawl can find novelty data.
///
/// Uses file storage + manual reindex to control exactly when indexing happens.
#[tokio::test]
async fn file_select_star_after_index_gap() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");

    // t=0: create ledger
    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");

    // t=1: insert ontology
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");
    let ledger_after_ontology = receipt.ledger;

    // Force an index at t=1 (like production _system)
    let index_result = fluree
        .reindex("_system:main", ReindexOptions::default())
        .await;
    eprintln!("Reindex result: {index_result:?}");

    // t=2: insert entity data (this will be in novelty, NOT indexed)
    let entity = test_idp_json_ld();
    let _receipt = fluree
        .insert(ledger_after_ontology, &entity)
        .await
        .expect("insert entity");

    // Now query with a FRESH instance (simulates Query Lambda)
    drop(fluree);
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = fsys_context();

    // Test 1: select * (the broken pattern in production)
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let config_fmt = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config_fmt)
        .execute_tracked()
        .await
        .expect("query_from select *");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    eprintln!(
        "Index-gap select * result: {}",
        serde_json::to_string_pretty(&formatted).unwrap()
    );

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    let keys: Vec<&String> = node.as_object().unwrap().keys().collect();

    // Test 2: where ?p ?o (the working pattern)
    let where_query = json!({
        "@context": ctx,
        "from": "_system:main",
        "where": {"@id": IRI, "?p": "?o"},
        "select": ["?p", "?o"]
    });
    let where_result = reader
        .query_from()
        .jsonld(&where_query)
        .execute_tracked()
        .await
        .expect("where query");

    let where_formatted = serde_json::to_value(&where_result.result).expect("serialize");
    let triples = where_formatted.as_array().expect("array");
    eprintln!("Index-gap where ?p ?o found {} triples", triples.len());

    // Both should work
    assert!(
        triples.len() > 3,
        "where ?p ?o should find triples even with index gap"
    );
    assert!(
        keys.len() > 1,
        "select * should return properties even with index gap, got only: {keys:?}\nfull: {formatted}"
    );
}

/// Narrowing: does the bug reproduce via GraphDb::from_ledger_state path too?
/// If so, the issue is in the range query layer, not the connection query path.
#[tokio::test]
async fn file_select_star_after_index_gap_direct_graphdb() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("_system:main").await.expect("create");
    let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    fluree
        .reindex("_system:main", ReindexOptions::default())
        .await
        .expect("reindex");

    let entity = test_idp_json_ld();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert entity");

    // Fresh load via fluree.ledger() then direct GraphDb path
    drop(fluree);
    let reader = FlureeBuilder::file(path).build().expect("build reader");
    let loaded = reader.ledger("_system:main").await.expect("load ledger");

    // Check novelty state
    eprintln!(
        "loaded.t() = {}, snapshot.t = {}, novelty empty = {}",
        loaded.t(),
        loaded.snapshot.t,
        loaded.novelty.is_empty()
    );

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let db = GraphDb::from_ledger_state(&loaded);
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader.query(&db, &query).await.expect("query");
    let formatted = result
        .format_async(db.as_graph_db_ref(), &config)
        .await
        .expect("format");

    eprintln!(
        "direct GraphDb result: {}",
        serde_json::to_string_pretty(&formatted).unwrap()
    );

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "direct GraphDb select * with index gap");
}

/// Same as above but writer and reader are separate Fluree instances
/// (closer to cross-process pattern).
#[tokio::test]
async fn file_separate_reader_select_star_after_index_gap() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    // Writer: concrete FileStorage (needed for reindex)
    {
        let writer = FlureeBuilder::file(path).build().expect("build writer");
        let ledger0 = writer.create_ledger("_system:main").await.expect("create");
        let ontology: Value = serde_json::from_str(ONTOLOGY_JSON).expect("parse ontology");
        let receipt = writer.insert(ledger0, &ontology).await.expect("bootstrap");

        // Force index at t=1
        let index_result = writer
            .reindex("_system:main", ReindexOptions::default())
            .await;
        eprintln!("separate_reader reindex result: {index_result:?}");

        // t=2: insert entity (novelty only, NOT indexed)
        let entity = test_idp_json_ld();
        let _receipt = writer
            .insert(receipt.ledger, &entity)
            .await
            .expect("insert entity");
    }

    // Reader: separate instance
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = fsys_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "_system:main"});
    let config_fmt = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config_fmt)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    eprintln!(
        "separate_reader index-gap result: {}",
        serde_json::to_string_pretty(&formatted).unwrap()
    );

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "separate reader select * with index gap");
}
