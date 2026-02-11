//! Datatype-focused query integration tests (Clojure parity)
//!
//! Mirrors supported portions of `db-clojure/test/fluree/db/query/datatype_test.clj`.
//! Always uses explicit `@context` and JSON inputs.
//!
//! NOTE:
//! - @json datatype is supported (insert + query + datatype() binding + filter).
//! - Binding @type to a variable inside a value-object is supported.
//! - Transaction @t bindings are supported.
//! - Numeric datatype normalization (xsd:int family -> xsd:integer) is an intentional divergence.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

fn ctx_datatype() -> JsonValue {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_people_for_datatype(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = ctx_datatype();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:homer","ex:name":"Homer","ex:age":36},
            {"@id":"ex:marge","ex:name":"Marge","ex:age":{"@value":36,"@type":"xsd:int"}},
            {"@id":"ex:bart","ex:name":"Bart","ex:age":"forever 10"}
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger
}

#[tokio::test]
async fn mixed_datatypes_query_matches_only_requested_type() {
    // Clojure: mixed-datatypes-test (adapted to analytical select; avoids subject crawl)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "ledger/datatype:main");
    let ctx = ctx_datatype();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:coco","@type":"schema:Person","schema:name":"Coco"},
            {"@id":"ex:halie","@type":"schema:Person","schema:name":"Halie"},
            {"@id":"ex:john","@type":"schema:Person","schema:name":3}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q1 = json!({
        "@context": ctx,
        "select": ["?u","?name"],
        "where": [{"@id":"?u","schema:name":"Halie"},{"@id":"?u","schema:name":"?name"}]
    });
    let r1 = fluree
        .query(&ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(r1, json!([["ex:halie", "Halie"]]));

    let q2 = json!({
        "@context": ctx,
        "select": ["?u"],
        "where": {"@id":"?u","schema:name":"a"}
    });
    let r2 = fluree
        .query(&ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(r2, json!([]));

    let q3 = json!({
        "@context": ctx,
        "select": ["?u","?name"],
        "where": [{"@id":"?u","schema:name":3},{"@id":"?u","schema:name":"?name"}]
    });
    let r3 = fluree
        .query(&ledger, &q3)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(r3, json!([["ex:john", 3]]));
}

#[tokio::test]
async fn datatype_query_explicit_typed_value_object_matches() {
    // Clojure: datatype-test / "specifying an explicit data type (compatible)"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name"],
        // Rust normalizes xsd:int to xsd:integer (parity gap vs Clojure).
        "where": {"ex:name":"?name","ex:age":{"@value":36,"@type":"xsd:integer"}}
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    // NOTE(parity): Rust currently normalizes numeric datatypes so untyped integers and
    // explicitly-typed integer-family values share `xsd:integer`. This means both Homer
    // (untyped 36) and Marge (typed xsd:int → normalized) match here.
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["Homer", "Marge"]))
    );
}

#[tokio::test]
async fn datatype_bind_datatype_function_includes_dt_in_results() {
    // Clojure: datatype-test / datatype() bound to variable and returned
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name","?age","?dt"],
        "where": [
            {"ex:name":"?name","ex:age":"?age"},
            ["bind","?dt",["expr",["datatype","?age"]]]
        ]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    // Marge's age was inserted with xsd:int - preserved for data fidelity
    // Non-default datatypes are wrapped in value objects per JSON-LD spec
    // Homer's age was inserted as untyped integer → stored as xsd:integer (default, no wrapper)
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Bart","forever 10","xsd:string"],
            ["Homer",36,"xsd:integer"],
            ["Marge",{"@value":36,"@type":"xsd:int"},"xsd:int"]
        ]))
    );
}

#[tokio::test]
async fn datatype_filter_with_datatype_function() {
    // Clojure: datatype-test / filtered with the datatype function
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name","?age","?dt"],
        "where": [
            {"ex:name":"?name","ex:age":"?age"},
            ["bind","?dt",["expr",["datatype","?age"]]],
            ["filter", "(= \"xsd:integer\" ?dt)"]
        ]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(rows, json!([["Homer", 36, "xsd:integer"]]));
}

#[tokio::test]
async fn datatype_query_incompatible_type_returns_no_matches() {
    // Incompatible @value/@type should throw a type coercion error.
    // e.g., {"@value": 36, "@type": "xsd:string"} - number cannot be coerced to string.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name"],
        "where": {"ex:name":"?name","ex:age":{"@value":36,"@type":"xsd:string"}}
    });

    let err = fluree.query(&ledger, &q).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("TypeCoercion") || msg.contains("coerce"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn datatype_filter_value_object_by_type_constant() {
    // Clojure: datatype-test / "filtered in value maps (explicit type IRIs)"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name","?age"],
        "where": {"ex:name":"?name","ex:age":{"@value":"?age","@type":"xsd:string"}}
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(rows, json!([["Bart", "forever 10"]]));
}

#[tokio::test]
async fn language_binding_lang_function() {
    // Clojure: language-binding-test / LANG(?val)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "lang-test:main");
    let ctx = ctx_datatype();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:greeting","ex:hello":{"@value":"Hello","@language":"en"}},
            {"@id":"ex:salutation","ex:hello":{"@value":"Bonjour","@language":"fr"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": ctx,
        "select": ["?id","?val","?lang"],
        "where": [
            {"@id":"?id","ex:hello":"?val"},
            ["bind","?lang",["expr",["lang","?val"]]]
        ]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let set: std::collections::HashSet<Vec<JsonValue>> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_array().unwrap().clone())
        .collect();
    // Note: JSON-LD formatter represents language-tagged strings as value objects.
    assert_eq!(
        set,
        std::collections::HashSet::from([
            vec![
                json!("ex:greeting"),
                json!({"@value":"Hello","@language":"en"}),
                json!("en")
            ],
            vec![
                json!("ex:salutation"),
                json!({"@value":"Bonjour","@language":"fr"}),
                json!("fr")
            ]
        ])
    );
}

#[tokio::test]
async fn language_binding_value_object_language_variable() {
    // Test binding @language to a variable: {"@value": "?val", "@language": "?lang"}
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "lang-test:main");
    let ctx = ctx_datatype();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:greeting","ex:hello":{"@value":"Hello","@language":"en"}},
            {"@id":"ex:salutation","ex:hello":{"@value":"Bonjour","@language":"fr"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": ctx,
        "select": ["?id","?val","?lang"],
        "where": [{"@id":"?id","ex:hello":{"@value":"?val","@language":"?lang"}}]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let set: std::collections::HashSet<Vec<JsonValue>> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_array().unwrap().clone())
        .collect();
    // Note: JSON-LD formatter represents language-tagged strings as value objects.
    assert_eq!(
        set,
        std::collections::HashSet::from([
            vec![
                json!("ex:greeting"),
                json!({"@value":"Hello","@language":"en"}),
                json!("en")
            ],
            vec![
                json!("ex:salutation"),
                json!({"@value":"Bonjour","@language":"fr"}),
                json!("fr")
            ]
        ])
    );
}

#[tokio::test]
async fn json_datatype_insert_query_and_filter() {
    // Test @json datatype: store arbitrary JSON, deserialize on query
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "json-test:main");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:doc1",
                "ex:name": "Document 1",
                "ex:data": {"@value": {"name": "John", "age": 30}, "@type": "@json"}
            },
            {
                "@id": "ex:doc2",
                "ex:name": "Document 2",
                "ex:data": "plain string data"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query with datatype() function to verify both records
    let q = json!({
        "@context": ctx,
        "select": ["?name", "?data", "?dt"],
        "where": [
            {"ex:name": "?name", "ex:data": "?data"},
            ["bind", "?dt", ["expr", ["datatype", "?data"]]]
        ]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let rows_arr = rows.as_array().unwrap();
    assert_eq!(rows_arr.len(), 2, "should return both documents");

    // Check that doc1 has @json datatype and deserialized JSON object
    let doc1_row = rows_arr
        .iter()
        .find(|r| r.as_array().unwrap()[0] == "Document 1")
        .expect("Document 1 should be in results");
    let doc1_data = &doc1_row.as_array().unwrap()[1];
    let doc1_dt = &doc1_row.as_array().unwrap()[2];
    assert_eq!(
        doc1_data,
        &json!({"age": 30, "name": "John"}),
        "should deserialize JSON object"
    );
    // @json can be represented as "@json" or the full IRI
    let dt_str = doc1_dt.as_str().unwrap();
    assert!(
        dt_str == "@json" || dt_str.contains("JSON") || dt_str.contains("json"),
        "should have @json datatype, got: {}",
        dt_str
    );

    // Check that doc2 has xsd:string datatype
    let doc2_row = rows_arr
        .iter()
        .find(|r| r.as_array().unwrap()[0] == "Document 2")
        .expect("Document 2 should be in results");
    let doc2_data = &doc2_row.as_array().unwrap()[1];
    let doc2_dt = &doc2_row.as_array().unwrap()[2];
    assert_eq!(doc2_data, "plain string data");
    let dt2_str = doc2_dt.as_str().unwrap();
    assert!(
        dt2_str == "xsd:string" || dt2_str.contains("string"),
        "should have xsd:string datatype, got: {}",
        dt2_str
    );

    let q2 = json!({
        "@context": ctx,
        "select": ["?name", "?data"],
        "where": [
            {"ex:name": "?name", "ex:data": "?data"},
            ["bind", "?dt", ["expr", ["datatype", "?data"]]],
            ["filter", "(= \"@json\" ?dt)"]
        ]
    });
    let rows2 = fluree
        .query(&ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    assert_eq!(
        rows2,
        json!([["Document 1", {"age": 30, "name": "John"}]]),
        "should return only document with @json datatype"
    );
}

#[tokio::test]
async fn value_type_binding_variable_in_value_object() {
    // Test binding @type to a variable: {"@value": "?val", "@type": "?type"}
    // This should work similar to the Clojure test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_for_datatype(&fluree, "people:datatype").await;
    let ctx = ctx_datatype();

    let q = json!({
        "@context": ctx,
        "select": ["?name","?age","?ageType"],
        "where": [{"ex:name":"?name","ex:age":{"@value":"?age","@type":"?ageType"}}]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();

    // Convert to set for order-insensitive comparison
    let set: std::collections::HashSet<Vec<JsonValue>> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_array().unwrap().clone())
        .collect();

    // Marge's age was inserted with xsd:int - preserved for data fidelity
    // Non-default datatypes are wrapped in value objects per JSON-LD spec
    // Homer's age was inserted as untyped integer → stored as xsd:integer (default, no wrapper)
    assert_eq!(
        set,
        std::collections::HashSet::from([
            vec![json!("Bart"), json!("forever 10"), json!("xsd:string")],
            vec![json!("Homer"), json!(36), json!("xsd:integer")],
            vec![
                json!("Marge"),
                json!({"@value":36,"@type":"xsd:int"}),
                json!("xsd:int")
            ],
        ])
    );
}

#[tokio::test]
async fn transaction_binding_at_t_variable() {
    // Test @t binding: {"@value": "?val", "@t": "?txn"} binds the transaction time
    // This is a Fluree-specific extension to JSON-LD query syntax.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "t-binding-test:main");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // First transaction: insert initial data at t=1
    let insert1 = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice", "ex:age": 30}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    // Second transaction: add more data at t=2
    let insert2 = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:bob", "ex:name": "Bob", "ex:age": 25}
        ]
    });
    let ledger2 = fluree.insert(ledger1, &insert2).await.unwrap().ledger;

    // Query with @t binding to get transaction times for each person's age
    let q = json!({
        "@context": ctx,
        "select": ["?name", "?age", "?txn"],
        "where": [
            {"@id": "?person", "ex:name": "?name"},
            {"@id": "?person", "ex:age": {"@value": "?age", "@t": "?txn"}}
        ],
        "orderBy": "?name"
    });

    let rows = fluree
        .query(&ledger2, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger2.db)
        .unwrap();
    let arr = rows.as_array().expect("Should be an array");

    assert_eq!(arr.len(), 2, "Should have two results (Alice and Bob)");

    // Alice was inserted at t=1, Bob at t=2
    // Note: t values are negative in Fluree's convention (more negative = earlier)
    // t=1 is represented as -1, t=2 as -2, etc.
    let alice_row = &arr[0];
    let bob_row = &arr[1];

    assert_eq!(alice_row[0], "Alice", "First row should be Alice");
    assert_eq!(alice_row[1], 30, "Alice's age should be 30");
    let alice_t = alice_row[2].as_i64().expect("Alice's txn should be i64");

    assert_eq!(bob_row[0], "Bob", "Second row should be Bob");
    assert_eq!(bob_row[1], 25, "Bob's age should be 25");
    let bob_t = bob_row[2].as_i64().expect("Bob's txn should be i64");

    // Verify they have different transaction times, and Alice's is "earlier"
    assert_ne!(
        alice_t, bob_t,
        "Alice and Bob should have different transaction times"
    );

    // In Fluree Rust, t values are positive with t=1 being the genesis transaction.
    // Alice (first insert after genesis) should have a smaller t than Bob (second insert)
    assert!(
        alice_t < bob_t,
        "Alice's t should be less than Bob's t since she was inserted first. alice_t={}, bob_t={}",
        alice_t,
        bob_t
    );
}

// =============================================================================
// xsd:decimal tests - comprehensive coverage
// =============================================================================

#[tokio::test]
async fn decimal_string_input_becomes_bigdecimal_preserves_precision() {
    // String input with xsd:decimal → BigDecimal with precision preserved
    // This tests the datatype-aware deserialization from storage
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:precision");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // Insert decimal values as strings with explicit xsd:decimal type
    // These should be stored as BigDecimal and preserve full precision
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:item1",
                "ex:price": {"@value": "123.456789012345678901234567890", "@type": "xsd:decimal"}
            },
            {
                "@id": "ex:item2",
                "ex:price": {"@value": "0.1", "@type": "xsd:decimal"}
            },
            {
                "@id": "ex:item3",
                "ex:price": {"@value": "999999999999999999999999999.999", "@type": "xsd:decimal"}
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query and verify precision is preserved
    let q = json!({
        "@context": ctx,
        "select": ["?id", "?price"],
        "where": {"@id": "?id", "ex:price": "?price"},
        "orderBy": "?id"
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().unwrap();

    // String decimals should preserve precision exactly
    // Note: JSON-LD serialization of BigDecimal should use string representation
    assert_eq!(arr.len(), 3);

    // Verify item1's high-precision decimal
    let item1 = &arr[0];
    assert_eq!(item1[0], "ex:item1");
    // BigDecimal serializes as string to preserve precision
    let price1 = &item1[1];
    // Could be {"@value": "123.456...", "@type": "xsd:decimal"} or just the string
    if let Some(s) = price1.as_str() {
        assert!(
            s.starts_with("123.45678901234567"),
            "Should preserve precision: {}",
            s
        );
    } else if let Some(obj) = price1.as_object() {
        let val = obj.get("@value").and_then(|v| v.as_str()).unwrap();
        assert!(
            val.starts_with("123.45678901234567"),
            "Should preserve precision: {}",
            val
        );
    }
}

#[tokio::test]
async fn decimal_json_number_input_becomes_double() {
    // JSON number input with xsd:decimal → Double (lossy, per policy)
    // JSON parsing already lost precision, so we keep it as f64
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:json-number");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // Insert decimal as JSON number (not string) with xsd:decimal type
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:item1",
                // JSON number 3.13 loses precision during JSON parsing
                // Per policy: JSON numbers with xsd:decimal become Double
                "ex:value": {"@value": 3.13, "@type": "xsd:decimal"}
            },
            {
                "@id": "ex:item2",
                // Integer JSON number with xsd:decimal → also Double
                "ex:value": {"@value": 42, "@type": "xsd:decimal"}
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": ctx,
        "select": ["?id", "?value", "?dt"],
        "where": [
            {"@id": "?id", "ex:value": "?value"},
            ["bind", "?dt", ["expr", ["datatype", "?value"]]]
        ],
        "orderBy": "?id"
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2);
    // JSON numbers become Double, which serializes as JSON number
    assert_eq!(arr[0][0], "ex:item1");
    // 3.13 as f64 should round-trip to 3.13
    assert_eq!(arr[0][1].as_f64(), Some(3.13), "item1 value should be 3.13");

    assert_eq!(arr[1][0], "ex:item2");
    // Integer 42 as Double - JSON may serialize as 42 or 42.0
    assert_eq!(arr[1][1].as_f64(), Some(42.0), "item2 value should be 42.0");

    // Verify explicit datatype xsd:decimal is preserved even when value is Double
    let dt1 = arr[0][2].as_str().unwrap();
    let dt2 = arr[1][2].as_str().unwrap();
    assert!(
        dt1.ends_with("#decimal") || dt1 == "xsd:decimal",
        "3.13 should preserve xsd:decimal datatype, got: {}",
        dt1
    );
    assert!(
        dt2.ends_with("#decimal") || dt2 == "xsd:decimal",
        "42 should preserve xsd:decimal datatype, got: {}",
        dt2
    );
}

#[tokio::test]
async fn decimal_sort_order_with_mixed_numeric_types() {
    // Test sort order works correctly across Long, Double, and BigDecimal
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:sort");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:a", "ex:score": 1},  // Long
            {"@id": "ex:b", "ex:score": {"@value": "2.5", "@type": "xsd:decimal"}},  // BigDecimal
            {"@id": "ex:c", "ex:score": 3.0},  // Double
            {"@id": "ex:d", "ex:score": {"@value": "1.5", "@type": "xsd:decimal"}},  // BigDecimal
            {"@id": "ex:e", "ex:score": 2}   // Long
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Order by score ascending - should work across types
    let q = json!({
        "@context": ctx,
        "select": ["?id", "?score"],
        "where": {"@id": "?id", "ex:score": "?score"},
        "orderBy": "?score"
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().unwrap();

    // Expected order: 1, 1.5, 2, 2.5, 3.0
    assert_eq!(arr.len(), 5);

    // Collect actual order for debugging
    let actual_ids: Vec<&str> = arr.iter().map(|r| r[0].as_str().unwrap()).collect();

    assert_eq!(
        actual_ids[0], "ex:a",
        "First should be ex:a (score 1), got {:?}",
        actual_ids
    );
    assert_eq!(
        actual_ids[1], "ex:d",
        "Second should be ex:d (score 1.5), got {:?}",
        actual_ids
    );
    assert_eq!(
        actual_ids[2], "ex:e",
        "Third should be ex:e (score 2), got {:?}",
        actual_ids
    );
    assert_eq!(
        actual_ids[3], "ex:b",
        "Fourth should be ex:b (score 2.5), got {:?}",
        actual_ids
    );
    assert_eq!(
        actual_ids[4], "ex:c",
        "Fifth should be ex:c (score 3.0), got {:?}",
        actual_ids
    );
}

#[tokio::test]
async fn decimal_equality_across_types() {
    // Test that querying for 3 (Long) matches 3.0 (Double) and "3.00" (BigDecimal)
    // This works because:
    // - FlakeValue::Ord uses numeric_cmp for cross-type comparison
    // - The B-tree index uses this ordering, so Long(3), Double(3.0), BigDecimal(3) are adjacent
    // - trim_to_range includes all flakes where cmp(flake, bound) == Equal
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:equality");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:long", "ex:value": 3},
            {"@id": "ex:double", "ex:value": 3.0},
            {"@id": "ex:decimal", "ex:value": {"@value": "3.00", "@type": "xsd:decimal"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query for value = 3 (Long) - ideally should match all mathematically equal values
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": {"@id": "?id", "ex:value": 3}
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().expect("Result should be an array");

    // Extract IDs - result format is [["ex:long"], ["ex:double"], ...]
    let ids: std::collections::HashSet<String> = arr
        .iter()
        .filter_map(|r| {
            if let Some(inner) = r.as_array() {
                inner
                    .first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                r.as_str().map(|s| s.to_string())
            }
        })
        .collect();

    // All three should match since they're mathematically equal
    assert!(
        ids.contains("ex:long"),
        "Long(3) should match, got: {:?}",
        ids
    );
    assert!(
        ids.contains("ex:double"),
        "Double(3.0) should match, got: {:?}",
        ids
    );
    assert!(
        ids.contains("ex:decimal"),
        "Decimal(3.00) should match, got: {:?}",
        ids
    );
}

#[tokio::test]
async fn decimal_filter_comparison_across_types() {
    // Test filter comparisons work across Long, Double, BigDecimal
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:filter");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:a", "ex:amount": 100},  // Long
            {"@id": "ex:b", "ex:amount": 150.5},  // Double
            {"@id": "ex:c", "ex:amount": {"@value": "200.00", "@type": "xsd:decimal"}},  // BigDecimal
            {"@id": "ex:d", "ex:amount": {"@value": "75.25", "@type": "xsd:decimal"}}  // BigDecimal
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Filter: amount > 100 (Long comparison against mixed types)
    let q = json!({
        "@context": ctx,
        "select": ["?id", "?amount"],
        "where": [
            {"@id": "?id", "ex:amount": "?amount"},
            ["filter", [">", "?amount", 100]]
        ],
        "orderBy": "?amount"
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().unwrap();

    // Should return: 150.5, 200.00 (not 100 or 75.25)
    assert_eq!(arr.len(), 2, "Should have 2 results, got: {:?}", arr);

    // Collect IDs for verification
    let ids: std::collections::HashSet<&str> = arr.iter().map(|r| r[0].as_str().unwrap()).collect();

    assert!(ids.contains("ex:b"), "Should contain ex:b (150.5)");
    assert!(ids.contains("ex:c"), "Should contain ex:c (200.00)");
}

#[tokio::test]
async fn decimal_invalid_string_should_error() {
    // Invalid string for xsd:decimal should error during query coercion
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:invalid");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item", "ex:value": 42}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query with invalid decimal string
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": {"@id": "?id", "ex:value": {"@value": "not-a-number", "@type": "xsd:decimal"}}
    });

    let result = fluree.query(&ledger, &q).await;
    assert!(result.is_err(), "Should error on invalid decimal string");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("coerce") || err_msg.contains("Coercion") || err_msg.contains("parse"),
        "Error should mention coercion or parsing: {}",
        err_msg
    );
}

#[tokio::test]
async fn decimal_non_integral_to_integer_should_error() {
    // Non-integral value with xsd:integer should error
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:non-integral");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item", "ex:value": 42}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query with non-integral double as xsd:integer
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": {"@id": "?id", "ex:value": {"@value": 3.13, "@type": "xsd:integer"}}
    });

    let result = fluree.query(&ledger, &q).await;
    assert!(
        result.is_err(),
        "Should error on non-integral double to integer"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("non-integer")
            || err_msg.contains("Coercion")
            || err_msg.contains("whole number"),
        "Error should mention non-integer: {}",
        err_msg
    );
}

#[tokio::test]
async fn decimal_number_to_boolean_should_error() {
    // Number with xsd:boolean should error
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "decimal-test:num-to-bool");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item", "ex:flag": true}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query with number as xsd:boolean
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": {"@id": "?id", "ex:flag": {"@value": 1, "@type": "xsd:boolean"}}
    });

    let result = fluree.query(&ledger, &q).await;
    assert!(result.is_err(), "Should error on number to boolean");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("coerce") || err_msg.contains("Coercion") || err_msg.contains("boolean"),
        "Error should mention coercion: {}",
        err_msg
    );
}

// =============================================================================
// VALUES clause typed literal coercion tests
// =============================================================================

#[tokio::test]
async fn values_typed_literal_string_to_integer_coercion() {
    // VALUES clause should apply the same coercion as WHERE patterns
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "values-test:coerce");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item1", "ex:score": 42},
            {"@id": "ex:item2", "ex:score": 100}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Use VALUES with typed literal - string "42" with xsd:integer should coerce to Long(42)
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": [
            ["values", ["?score", [{"@value": "42", "@type": "xsd:integer"}]]],
            {"@id": "?id", "ex:score": "?score"}
        ]
    });

    let result = fluree.query(&ledger, &q).await;
    let rows = result.unwrap().to_jsonld(&ledger.db).unwrap();
    let arr = rows.as_array().unwrap();

    // Should match ex:item1 because "42" coerced to Long(42)
    // Note: single-column results may be flattened to ["ex:item1"] not [["ex:item1"]]
    assert_eq!(arr.len(), 1, "Should find one match, got: {:?}", arr);
    let first = &arr[0];
    let id = if let Some(inner) = first.as_array() {
        inner[0].as_str()
    } else {
        first.as_str()
    };
    assert_eq!(
        id,
        Some("ex:item1"),
        "Should match ex:item1, got: {:?}",
        arr
    );
}

#[tokio::test]
async fn values_incompatible_type_returns_no_matches() {
    // VALUES clause with incompatible @value/@type should throw a type coercion error.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "values-test:no-match");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item1", "ex:name": "test"}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // VALUES with number → xsd:string should return no matches
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": [
            ["values", ["?name", [{"@value": 42, "@type": "xsd:string"}]]],
            {"@id": "?id", "ex:name": "?name"}
        ]
    });

    let err = fluree.query(&ledger, &q).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("TypeCoercion") || msg.contains("coerce"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn values_decimal_string_becomes_bigdecimal() {
    // VALUES with string + xsd:decimal should create BigDecimal
    // Both transaction and query now coerce string decimals to BigDecimal
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "values-test:decimal");
    let ctx = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:item1", "ex:price": {"@value": "19.99", "@type": "xsd:decimal"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Query using VALUES with same decimal value
    let q = json!({
        "@context": ctx,
        "select": ["?id"],
        "where": [
            ["values", ["?price", [{"@value": "19.99", "@type": "xsd:decimal"}]]],
            {"@id": "?id", "ex:price": "?price"}
        ]
    });

    let rows = fluree
        .query(&ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.db)
        .unwrap();
    let arr = rows.as_array().unwrap();

    // Should match because both are BigDecimal("19.99")
    assert_eq!(arr.len(), 1, "Should find one match, got: {:?}", arr);
    let first = &arr[0];
    let id = if let Some(inner) = first.as_array() {
        inner[0].as_str()
    } else {
        first.as_str()
    };
    assert_eq!(
        id,
        Some("ex:item1"),
        "Should match ex:item1, got: {:?}",
        arr
    );
}
