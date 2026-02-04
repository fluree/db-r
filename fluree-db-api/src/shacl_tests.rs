use crate::{ApiError, FlureeBuilder};
use fluree_db_transact::TransactError;
use serde_json::{json, Value as JsonValue};

fn default_context() -> JsonValue {
    json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "sh": "http://www.w3.org/ns/shacl#",
        "schema": "http://schema.org/",
        "skos": "http://www.w3.org/2008/05/skos#",
        "wiki": "https://www.wikidata.org/wiki/",
        "f": "https://ns.flur.ee/ledger#"
    })
}

fn shacl_context() -> JsonValue {
    json!([default_context(), {"ex": "http://example.org/ns/"}])
}

fn assert_shacl_violation(err: ApiError, expected: &str) {
    match err {
        ApiError::Transact(TransactError::ShaclViolation(message)) => {
            assert!(
                message.contains(expected),
                "expected violation to contain '{}', got: {}",
                expected,
                message
            );
        }
        other => panic!("expected SHACL violation, got {:?}", other),
    }
}

#[tokio::test]
async fn shacl_cardinality_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:UserShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:minCount": 1,
            "sh:maxCount": 1,
            "sh:datatype": {"@id": "xsd:string"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/cardinality-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?name"],
        "where": {"@id": "ex:john", "schema:name": "?name"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["John"]));

    let ledger_min = fluree
        .create_ledger("shacl/cardinality-min:main")
        .await
        .unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alex",
                "@type": "ex:User"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected at least 1 value(s) but found 0");

    let ledger_max = fluree
        .create_ledger("shacl/cardinality-max:main")
        .await
        .unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": ["Brian", "Bri"]
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected at most 1 value(s) but found 2");
}

#[tokio::test]
async fn shacl_datatype_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:UserShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:datatype": {"@id": "xsd:string"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/datatype-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?name"],
        "where": {"@id": "ex:john", "schema:name": "?name"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["John"]));

    let ledger_bad = fluree
        .create_ledger("shacl/datatype-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": { "@value": 42, "@type": "xsd:integer" }
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected datatype");
}

#[tokio::test]
async fn shacl_range_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:AgeShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:age"},
            "sh:minExclusive": 1,
            "sh:maxInclusive": 100
        }]
    });

    let ledger_min = fluree.create_ledger("shacl/range-min:main").await.unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:age": 1
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "must be greater than");

    let ledger_max = fluree.create_ledger("shacl/range-max:main").await.unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:age": 101
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "exceeds maximum");
}

#[tokio::test]
async fn shacl_length_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:NameShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"},
            "sh:minLength": 4,
            "sh:maxLength": 10
        }]
    });

    let ledger_min = fluree.create_ledger("shacl/length-min:main").await.unwrap();
    let ledger_min = fluree.upsert(ledger_min, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_min,
            &json!({
                "@context": context.clone(),
                "@id": "ex:al",
                "@type": "ex:User",
                "schema:name": "Al"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "less than minimum");

    let ledger_max = fluree.create_ledger("shacl/length-max:main").await.unwrap();
    let ledger_max = fluree.upsert(ledger_max, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_max,
            &json!({
                "@context": context.clone(),
                "@id": "ex:jean-claude",
                "@type": "ex:User",
                "schema:name": "Jean-Claude"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "exceeds maximum");
}

#[tokio::test]
async fn shacl_pattern_constraints() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:GreetingShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:greeting"},
            "sh:pattern": "hello .* world"
        }]
    });

    let ledger_ok = fluree.create_ledger("shacl/pattern-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:greeting": "hello big world"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?greeting"],
        "where": {"@id": "ex:alice", "ex:greeting": "?greeting"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["hello big world"]));

    let ledger_bad = fluree
        .create_ledger("shacl/pattern-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:greeting": "goodbye world"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not match pattern");
}

#[tokio::test]
async fn shacl_has_value_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:RoleShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:role"},
            "sh:hasValue": "admin"
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/has-value-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:role": "admin"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?role"],
        "where": {"@id": "ex:alice", "schema:role": "?role"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["admin"]));

    let ledger_bad = fluree
        .create_ledger("shacl/has-value-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:role": "user"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Required value");
}

#[tokio::test]
async fn shacl_node_kind_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:HomepageShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:homepage"},
            "sh:nodeKind": {"@id": "sh:IRI"}
        }]
    });

    let ledger_ok = fluree
        .create_ledger("shacl/node-kind-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:homepage": {"@id": "ex:homepage"}
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?home"],
        "where": {"@id": "ex:alice", "schema:homepage": "?home"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["ex:homepage"]));

    let ledger_bad = fluree
        .create_ledger("shacl/node-kind-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:homepage": "http://example.org"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "Expected node kind");
}

// =============================================================================
// sh:closed + sh:ignoredProperties tests
// =============================================================================

#[tokio::test]
async fn shacl_closed_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape that only allows name and age properties
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:closed": true,
        "sh:property": [
            {
                "@id": "ex:pshape1",
                "sh:path": {"@id": "schema:name"}
            },
            {
                "@id": "ex:pshape2",
                "sh:path": {"@id": "schema:age"}
            }
        ]
    });

    // Valid: only uses declared properties
    let ledger_ok = fluree.create_ledger("shacl/closed-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice",
                "schema:age": 30
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?name"],
        "where": {"@id": "ex:alice", "schema:name": "?name"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["Alice"]));

    // Invalid: uses undeclared property (schema:email)
    let ledger_bad = fluree.create_ledger("shacl/closed-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:Person",
                "schema:name": "Bob",
                "schema:email": "bob@example.org"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not allowed by closed shape");
}

#[tokio::test]
async fn shacl_closed_with_ignored_properties() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape that allows name, plus ignores rdf:type
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:closed": true,
        "sh:ignoredProperties": { "@list": [{"@id": "rdf:type"}] },
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "schema:name"}
        }]
    });

    // Valid: rdf:type is ignored even though not declared
    let ledger_ok = fluree
        .create_ledger("shacl/closed-ignored-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let _ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Person",
                "schema:name": "Alice"
            }),
        )
        .await
        .unwrap()
        .ledger;
}

// =============================================================================
// sh:pattern with sh:flags tests
// =============================================================================

#[tokio::test]
async fn shacl_pattern_with_flags() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape with case-insensitive pattern (sh:flags "i")
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:GreetingShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Message"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:text"},
            "sh:pattern": "hello",
            "sh:flags": "i"
        }]
    });

    // Valid: "HELLO" matches "hello" with case-insensitive flag
    let ledger_ok = fluree
        .create_ledger("shacl/pattern-flags-ok:main")
        .await
        .unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:msg1",
                "@type": "ex:Message",
                "ex:text": "HELLO WORLD"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?text"],
        "where": {"@id": "ex:msg1", "ex:text": "?text"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["HELLO WORLD"]));

    // Invalid: "goodbye" doesn't match pattern
    let ledger_bad = fluree
        .create_ledger("shacl/pattern-flags-bad:main")
        .await
        .unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:msg2",
                "@type": "ex:Message",
                "ex:text": "GOODBYE WORLD"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not match pattern");
}

// =============================================================================
// sh:in list semantics tests
// =============================================================================

#[tokio::test]
async fn shacl_in_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape with sh:in list of allowed values
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:StatusShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Task"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:status"},
            "sh:in": { "@list": ["pending", "active", "completed"] }
        }]
    });

    // Valid: "active" is in the allowed list
    let ledger_ok = fluree.create_ledger("shacl/in-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:task1",
                "@type": "ex:Task",
                "ex:status": "active"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?status"],
        "where": {"@id": "ex:task1", "ex:status": "?status"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["active"]));

    // Invalid: "cancelled" is not in the allowed list
    let ledger_bad = fluree.create_ledger("shacl/in-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:task2",
                "@type": "ex:Task",
                "ex:status": "cancelled"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "not in the allowed set");
}

// =============================================================================
// sh:equals constraint tests
// =============================================================================

#[tokio::test]
async fn shacl_equals_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Shape where startDate must equal endDate (for single-day events)
    let shape_txn = json!({
        "@context": context.clone(),
        "@id": "ex:SingleDayEventShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:SingleDayEvent"},
        "sh:property": [{
            "@id": "ex:pshape1",
            "sh:path": {"@id": "ex:startDate"},
            "sh:equals": {"@id": "ex:endDate"}
        }]
    });

    // Valid: startDate equals endDate
    let ledger_ok = fluree.create_ledger("shacl/equals-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shape_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:event1",
                "@type": "ex:SingleDayEvent",
                "ex:startDate": "2024-01-15",
                "ex:endDate": "2024-01-15"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?date"],
        "where": {"@id": "ex:event1", "ex:startDate": "?date"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["2024-01-15"]));

    // Invalid: startDate does not equal endDate
    let ledger_bad = fluree.create_ledger("shacl/equals-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shape_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:event2",
                "@type": "ex:SingleDayEvent",
                "ex:startDate": "2024-01-15",
                "ex:endDate": "2024-01-16"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "does not equal");
}

// =============================================================================
// Logical constraint tests (sh:not, sh:and, sh:or, sh:xone)
// =============================================================================

#[tokio::test]
async fn shacl_not_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Define a "forbidden" shape and a main shape that uses sh:not
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:ForbiddenShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:forbidden_pshape",
                "sh:path": {"@id": "ex:status"},
                "sh:hasValue": "banned"
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:UserShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:User"},
            "sh:not": {"@id": "ex:ForbiddenShape"}
        }
    ]);

    // Valid: user with status "active" (not "banned")
    let ledger_ok = fluree.create_ledger("shacl/not-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:status": "active"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?status"],
        "where": {"@id": "ex:alice", "ex:status": "?status"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["active"]));

    // Invalid: user with status "banned" matches the forbidden shape
    let ledger_bad = fluree.create_ledger("shacl/not-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:User",
                "ex:status": "banned"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:not");
}

#[tokio::test]
async fn shacl_and_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes that must both be satisfied
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:HasNameShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:name_pshape",
                "sh:path": {"@id": "schema:name"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:HasEmailShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:email_pshape",
                "sh:path": {"@id": "schema:email"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:UserShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:User"},
            "sh:and": { "@list": [
                {"@id": "ex:HasNameShape"},
                {"@id": "ex:HasEmailShape"}
            ]}
        }
    ]);

    // Valid: has both name and email
    let ledger_ok = fluree.create_ledger("shacl/and-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?name"],
        "where": {"@id": "ex:alice", "schema:name": "?name"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["Alice"]));

    // Invalid: missing email (only has name)
    let ledger_bad = fluree.create_ledger("shacl/and-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:and");
}

#[tokio::test]
async fn shacl_or_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes - at least one must be satisfied
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:HasPhoneShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:phone_pshape",
                "sh:path": {"@id": "ex:phone"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:HasEmailShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:email_pshape",
                "sh:path": {"@id": "schema:email"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:ContactShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Contact"},
            "sh:or": { "@list": [
                {"@id": "ex:HasPhoneShape"},
                {"@id": "ex:HasEmailShape"}
            ]}
        }
    ]);

    // Valid: has email (satisfies one option)
    let ledger_ok = fluree.create_ledger("shacl/or-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:alice",
                "@type": "ex:Contact",
                "schema:email": "alice@example.org"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?email"],
        "where": {"@id": "ex:alice", "schema:email": "?email"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["alice@example.org"]));

    // Invalid: has neither phone nor email
    let ledger_bad = fluree.create_ledger("shacl/or-bad:main").await.unwrap();
    let ledger_bad = fluree.upsert(ledger_bad, &shapes_txn).await.unwrap().ledger;
    let err = fluree
        .upsert(
            ledger_bad,
            &json!({
                "@context": context.clone(),
                "@id": "ex:bob",
                "@type": "ex:Contact",
                "schema:name": "Bob"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:or");
}

#[tokio::test]
async fn shacl_xone_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = shacl_context();

    // Two shapes - exactly one must be satisfied (not both, not neither)
    let shapes_txn = json!([
        {
            "@context": context.clone(),
            "@id": "ex:PersonalAccountShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:personal_pshape",
                "sh:path": {"@id": "ex:personalId"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:BusinessAccountShape",
            "@type": "sh:NodeShape",
            "sh:property": [{
                "@id": "ex:business_pshape",
                "sh:path": {"@id": "ex:businessId"},
                "sh:minCount": 1
            }]
        },
        {
            "@context": context.clone(),
            "@id": "ex:AccountShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Account"},
            "sh:xone": { "@list": [
                {"@id": "ex:PersonalAccountShape"},
                {"@id": "ex:BusinessAccountShape"}
            ]}
        }
    ]);

    // Valid: has only personalId (exactly one shape matches)
    let ledger_ok = fluree.create_ledger("shacl/xone-ok:main").await.unwrap();
    let ledger_ok = fluree.upsert(ledger_ok, &shapes_txn).await.unwrap().ledger;
    let ledger_ok = fluree
        .upsert(
            ledger_ok,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct1",
                "@type": "ex:Account",
                "ex:personalId": "P12345"
            }),
        )
        .await
        .unwrap()
        .ledger;
    let query = json!({
        "@context": context.clone(),
        "select": ["?id"],
        "where": {"@id": "ex:acct1", "ex:personalId": "?id"}
    });
    let result = fluree.query(&ledger_ok, &query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger_ok.db).unwrap();
    assert_eq!(jsonld, json!(["P12345"]));

    // Invalid: has both personalId AND businessId (both shapes match)
    let ledger_both = fluree.create_ledger("shacl/xone-both:main").await.unwrap();
    let ledger_both = fluree
        .upsert(ledger_both, &shapes_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_both,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct2",
                "@type": "ex:Account",
                "ex:personalId": "P12345",
                "ex:businessId": "B67890"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:xone");

    // Invalid: has neither (no shapes match)
    let ledger_none = fluree.create_ledger("shacl/xone-none:main").await.unwrap();
    let ledger_none = fluree
        .upsert(ledger_none, &shapes_txn)
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .upsert(
            ledger_none,
            &json!({
                "@context": context.clone(),
                "@id": "ex:acct3",
                "@type": "ex:Account",
                "schema:name": "Anonymous"
            }),
        )
        .await
        .unwrap_err();
    assert_shacl_violation(err, "sh:xone");
}
