//! Transact object variable parsing integration tests (Clojure parity)
//!
//! Ports integration tests from `db-clojure/test/fluree/db/transact/object_var_parsing_test.clj`.
//! Tests how variables in transaction objects are handled.
//!
//! Note: Internal parsing tests are marked as ignored since they test transaction parser
//! internals that may not be exposed in the public Rust API.

mod support;

use fluree_db_api::{FlureeBuilder, TxnOpts};
use serde_json::json;

// Helper function to create a standard context
fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

#[tokio::test]
async fn insert_does_not_parse_bare_var_by_default() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/obj-var-insert-default:main")
        .await
        .unwrap();

    let txn = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:s", "schema:text": "?age"}]
    });
    let ledger1 = fluree.insert(ledger0, &txn).await.unwrap().ledger;

    let query = json!({
        "@context": ctx(),
        "select": ["?val"],
        "where": [{"@id": "ex:s", "schema:text": "?val"}]
    });
    let result = fluree.query(&ledger1, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger1.db).unwrap();
    assert_eq!(jsonld, json!(["?age"]));
}

#[tokio::test]
async fn object_var_parsing_update_opt() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/obj-var-update-opt:main")
        .await
        .unwrap();

    let update = json!({
        "@context": ctx(),
        "insert": [{"@id": "ex:s", "schema:text": "?age"}]
    });

    let ledger1 = fluree
        .update_with_opts(
            ledger0,
            &update,
            TxnOpts {
                object_var_parsing: Some(true),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": ["?val"],
        "where": [{"@id": "ex:s", "schema:text": "?val"}]
    });
    let jsonld = fluree
        .query(&ledger1, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger1.db)
        .unwrap();
    assert_eq!(jsonld, json!(["?age"]));

    let ledger2 = fluree
        .update_with_opts(
            ledger1,
            &update,
            TxnOpts {
                object_var_parsing: Some(false),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;
    let jsonld2 = fluree
        .query(&ledger2, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger2.db)
        .unwrap();
    assert_eq!(jsonld2, json!(["?age"]));
}

#[tokio::test]
async fn update_with_object_var_parsing_false_treats_bare_var_as_literal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/obj-var-update-false:main")
        .await
        .unwrap();

    let update = json!({
        "@context": ctx(),
        "insert": [{"@id": "ex:s", "schema:text": "?not-a-var"}]
    });
    let ledger1 = fluree
        .update_with_opts(
            ledger0,
            &update,
            TxnOpts {
                object_var_parsing: Some(false),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": ["?val"],
        "where": [{"@id": "ex:s", "schema:text": "?val"}]
    });
    let jsonld = fluree
        .query(&ledger1, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger1.db)
        .unwrap();
    assert_eq!(jsonld, json!(["?not-a-var"]));
}

#[tokio::test]
async fn update_explicit_variable_map_parses_when_flag_false_and_bound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/obj-var-explicit:main")
        .await
        .unwrap();

    let insert = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:s", "schema:date": "2020-01-01"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let update = json!({
        "@context": ctx(),
        "where": [{"@id": "ex:s", "schema:date": {"@variable": "?d"}}],
        "insert": [{"@id": "ex:s", "schema:foo": {"@variable": "?d", "@type": "xsd:dateTime"}}]
    });
    let ledger2 = fluree
        .update_with_opts(
            ledger1,
            &update,
            TxnOpts {
                object_var_parsing: Some(false),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": ["?val"],
        "where": [{
            "@id": "ex:s",
            "schema:foo": {"@value": "?val"}
        }]
    });
    let jsonld = fluree
        .query(&ledger2, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger2.db)
        .unwrap();
    assert_eq!(jsonld, json!(["2020-01-01"]));
}

#[tokio::test]
async fn update_id_var_still_parses_when_flag_false() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/obj-var-id:main").await.unwrap();

    let insert = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:s", "schema:text": "?not-a-var"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let update = json!({
        "@context": ctx(),
        "where": [{"@id": "?is-a-var", "schema:text": "?not-a-var"}],
        "insert": [{"@id": "?is-a-var", "schema:newProp": "new"}]
    });
    let ledger2 = fluree
        .update_with_opts(
            ledger1,
            &update,
            TxnOpts {
                object_var_parsing: Some(false),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:s": ["*"]},
        "where": [{"@id": "ex:s"}]
    });
    let jsonld = fluree
        .query(&ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(&ledger2.db)
        .await
        .unwrap();
    assert_eq!(
        jsonld,
        json!([{"@id":"ex:s","schema:text":"?not-a-var","schema:newProp":"new"}])
    );
}

#[tokio::test]
async fn update_predicate_var_still_parses_when_flag_false() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/obj-var-pred:main").await.unwrap();

    let insert = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:s", "schema:text": "?not-a-var"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let update = json!({
        "@context": ctx(),
        "where": [{"@id": "ex:s", "?is-a-var": "?not-a-var"}],
        "insert": [{"@id": "ex:s", "?is-a-var": "?not-a-var"}]
    });
    let ledger2 = fluree
        .update_with_opts(
            ledger1,
            &update,
            TxnOpts {
                object_var_parsing: Some(false),
                ..Default::default()
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": [{"@id":"ex:s","?p":"?o"}]
    });
    let jsonld = fluree
        .query(&ledger2, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger2.db)
        .unwrap();
    assert!(jsonld.as_array().unwrap().contains(&json!("schema:text")));
}

#[tokio::test]
async fn insert_literal_qmark_string_has_xsd_string_type() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree
        .create_ledger("tx/obj-var-insert:main")
        .await
        .unwrap();

    // Insert data with a literal "?not-a-var" (should be treated as string)
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "?not-a-var"}]
    });

    let ledger1 = fluree.insert(ledger0, &txn).await.unwrap().ledger;

    // Query to check the stored value and its datatype
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "select": ["?val", "?dt"],
        "where": [{
            "@id": "ex:s",
            "ex:prop": {"@value": "?val", "@type": "?dt"}
        }]
    });

    let result = fluree.query(&ledger1, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger1.db).unwrap();

    // Should return the literal string "?not-a-var" with xsd:string type
    assert_eq!(jsonld, json!([["?not-a-var", "xsd:string"]]));
}

#[tokio::test]
async fn upsert_literal_qmark_string_has_xsd_string_type() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree
        .create_ledger("tx/obj-var-upsert:main")
        .await
        .unwrap();

    // First insert some initial data
    let initial_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "String val to be replaced"}]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Then upsert with literal "?not-a-var"
    let upsert_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "?not-a-var"}]
    });

    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query to check the stored value and its datatype
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "select": ["?val", "?dt"],
        "where": [{
            "@id": "ex:s",
            "ex:prop": {"@value": "?val", "@type": "?dt"}
        }]
    });

    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();

    // Should return the literal string "?not-a-var" with xsd:string type
    assert_eq!(jsonld, json!([["?not-a-var", "xsd:string"]]));
}

#[tokio::test]
async fn query_literal_qmark_string_with_flag_false_requires_literal_match() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = fluree
        .create_ledger("tx/obj-var-query-literal:main")
        .await
        .unwrap();
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "?not-a-var"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "opts": {"objectVarParsing": false},
        "select": ["?s"],
        "where": [{"@id": "?s", "ex:prop": "?not-a-var"}]
    });

    let result = fluree.query(&ledger1, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger1.db).unwrap();
    assert_eq!(jsonld, json!(["ex:s"]));
}

#[tokio::test]
async fn query_explicit_variable_in_where_still_parses_when_flag_false() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = fluree
        .create_ledger("tx/obj-var-query-var:main")
        .await
        .unwrap();
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "?not-a-var"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "opts": {"objectVarParsing": false},
        "select": ["?v"],
        "where": [{"@id": "ex:s", "ex:prop": {"@variable": "?v"}}]
    });

    let result = fluree.query(&ledger1, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger1.db).unwrap();
    assert_eq!(jsonld, json!(["?not-a-var"]));
}

#[tokio::test]
async fn update_literal_qmark_string_where_binds_and_updates() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = fluree
        .create_ledger("tx/obj-var-update:main")
        .await
        .unwrap();
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:s", "ex:prop": "?not-a-var"}]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let update = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": [{"@id": "?s", "ex:prop": "?not-a-var"}],
        "insert": [{"@id": "?s", "ex:newProp": "new"}]
    });
    let txn_opts = TxnOpts {
        object_var_parsing: Some(false),
        ..Default::default()
    };
    let ledger2 = fluree
        .update_with_opts(
            ledger1,
            &update,
            txn_opts,
            Default::default(),
            &Default::default(),
        )
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": {"ex:s": ["*"]},
        "where": [{"@id": "ex:s"}]
    });
    let result = fluree.query(&ledger2, &query).await.unwrap();
    let jsonld = result.to_jsonld_async(&ledger2.db).await.unwrap();
    assert_eq!(
        jsonld,
        json!([{"@id": "ex:s", "ex:prop": "?not-a-var", "ex:newProp": "new"}])
    );
}
