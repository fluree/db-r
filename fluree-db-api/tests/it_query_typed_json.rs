//! Integration tests for TypedJson graph crawl formatting and normalize_arrays.
//!
//! Verifies that `to_typed_json_async()` and `FormatterConfig::typed_json()` produce
//! explicit `@type` annotations in graph crawl results, and that `normalize_arrays`
//! forces array wrapping for single-valued properties.

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value as JsonValue};
use support::{
    genesis_ledger, query_jsonld_format, query_jsonld_formatted, MemoryFluree, MemoryLedger,
};

fn ctx() -> JsonValue {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Seed a small graph with various datatypes for testing typed output.
async fn seed_typed_graph() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/typed-json:test");

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice",
                "schema:age": {"@value": 30, "@type": "xsd:long"},
                "schema:knows": {"@id": "ex:bob"},
                "ex:tags": ["rust", "wasm"]
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob",
                "schema:age": {"@value": 25, "@type": "xsd:long"},
                "ex:single_tag": "only-one"
            },
            {
                "@id": "ex:config",
                "ex:data": {"@value": {"key": "val", "n": 42}, "@type": "@json"}
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert typed graph");
    (fluree, committed.ledger)
}

// ============================================================================
// TypedJson graph crawl
// ============================================================================

#[tokio::test]
async fn typed_json_graph_crawl_includes_type_annotations() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json graph crawl");

    let arr = result.as_array().expect("result is array");
    assert_eq!(arr.len(), 1, "single root entity");
    let alice = &arr[0];

    // @id should still be present
    assert!(alice.get("@id").is_some(), "has @id");

    // schema:name should have explicit @type
    let name = alice.get("schema:name").expect("has schema:name");
    assert!(name.get("@value").is_some(), "name has @value: {name}");
    assert!(name.get("@type").is_some(), "name has @type: {name}");
    assert_eq!(
        name["@value"].as_str(),
        Some("Alice"),
        "name value is Alice"
    );

    // schema:age should have explicit @type
    let age = alice.get("schema:age").expect("has schema:age");
    assert!(age.get("@value").is_some(), "age has @value: {age}");
    assert!(age.get("@type").is_some(), "age has @type: {age}");

    // schema:knows should be a reference with @id
    let knows = alice.get("schema:knows").expect("has schema:knows");
    assert!(knows.get("@id").is_some(), "knows has @id");
}

#[tokio::test]
async fn typed_json_graph_crawl_json_datatype_preserved() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json graph crawl with @json");

    let arr = result.as_array().expect("result is array");
    let config_obj = &arr[0];

    let data = config_obj.get("ex:data").expect("has ex:data");
    assert_eq!(
        data.get("@type").and_then(|t| t.as_str()),
        Some("@json"),
        "@json datatype preserved: {data}"
    );
    assert!(data.get("@value").is_some(), "@json value wrapped: {data}");
    // The @value should be parsed JSON, not a string
    let inner = &data["@value"];
    assert!(inner.is_object(), "@json inner is object: {inner}");
    assert_eq!(inner["key"], "val");
    assert_eq!(inner["n"], 42);
}

#[tokio::test]
async fn typed_json_graph_crawl_nested_entities_are_typed() {
    let (fluree, ledger) = seed_typed_graph().await;

    // Graph crawl with nested expansion
    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*", {"schema:knows": ["*"]}]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json nested graph crawl");

    let alice = &result.as_array().unwrap()[0];
    let knows = alice.get("schema:knows").expect("has schema:knows");

    // The nested entity (bob) should also have typed literals
    let bob_name = knows
        .get("schema:name")
        .expect("nested bob has schema:name");
    assert!(
        bob_name.get("@value").is_some(),
        "nested name has @value: {bob_name}"
    );
    assert!(
        bob_name.get("@type").is_some(),
        "nested name has @type: {bob_name}"
    );
}

// ============================================================================
// JSON-LD graph crawl (default) does NOT include types for inferable datatypes
// ============================================================================

#[tokio::test]
async fn jsonld_graph_crawl_omits_inferable_types() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]},
        "from": "it/typed-json:test"
    });

    let result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("jsonld graph crawl");

    let alice = &result.as_array().unwrap()[0];
    let name = alice.get("schema:name").expect("has schema:name");
    // Default JSON-LD: inferable string type → bare string, no @value/@type
    assert!(
        name.is_string(),
        "default jsonld returns bare string for name: {name}"
    );
}

// ============================================================================
// normalize_arrays
// ============================================================================

#[tokio::test]
async fn normalize_arrays_forces_array_for_single_values() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:bob": ["*"]},
        "from": "it/typed-json:test"
    });

    // Without normalize_arrays: single-valued property is a scalar
    let default_result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("default graph crawl");
    let bob_default = &default_result.as_array().unwrap()[0];
    let single_tag_default = bob_default.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        !single_tag_default.is_array(),
        "default: single value is scalar: {single_tag_default}"
    );

    // With normalize_arrays: even single-valued property is an array
    let config = FormatterConfig::jsonld().with_normalize_arrays();
    let norm_result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("normalized graph crawl");
    let bob_norm = &norm_result.as_array().unwrap()[0];
    let single_tag_norm = bob_norm.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        single_tag_norm.is_array(),
        "normalized: single value is array: {single_tag_norm}"
    );
    assert_eq!(single_tag_norm.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn normalize_arrays_combined_with_typed_json() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:bob": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed + normalized graph crawl");
    let bob = &result.as_array().unwrap()[0];

    // Single-valued property is an array
    let single_tag = bob.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        single_tag.is_array(),
        "typed+normalized: single value is array"
    );

    // Each element in the array has @value/@type
    let first = &single_tag.as_array().unwrap()[0];
    assert!(first.get("@value").is_some(), "element has @value: {first}");
    assert!(first.get("@type").is_some(), "element has @type: {first}");
}

// ============================================================================
// Tabular (non-crawl) typed json still works
// ============================================================================

#[tokio::test]
async fn typed_json_tabular_query_works() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": ["?name", "?age"],
        "where": {
            "@id": "ex:alice",
            "schema:name": "?name",
            "schema:age": "?age"
        },
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json tabular");

    let row = &result.as_array().unwrap()[0];
    let name = row.get("?name").expect("has ?name");
    assert!(name.get("@value").is_some(), "tabular name has @value");
    assert!(name.get("@type").is_some(), "tabular name has @type");
}
