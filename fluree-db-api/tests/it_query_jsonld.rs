//! JSON-LD query integration tests (Clojure parity)
//!
//! Focus: query semantics (filters / optionals / union) using JSON inputs only.
//! Mirrors query namespaces under `db-clojure/test/fluree/db/query/*_test.clj`,
//! converting EDN query maps to JSON objects and always providing explicit `@context`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_people_filter_dataset(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:last": "Smith",
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 42,
                "ex:last": "Smith",
                "ex:favColor": "Green",
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:last": "Jones",
                "ex:favColor": "Blue",
                "ex:favNums": [5, 10],
                "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]
            },
            {
                "@id": "ex:david",
                "@type": "ex:User",
                "schema:name": "David",
                "schema:email": "david@example.org",
                "schema:age": 46,
                "ex:last": "Jones",
                "ex:favNums": [15, 70],
                "ex:friend": [{"@id": "ex:cam"}]
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

async fn assert_query_bind_error(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    query: serde_json::Value,
    expected: &str,
) {
    let result = fluree.query(ledger, &query).await;
    assert!(result.is_err(), "expected query error");
    if let Err(err) = result {
        assert!(
            err.to_string().contains(expected),
            "unexpected error: {}",
            err
        );
    }
}

#[tokio::test]
async fn jsonld_filter_single_filter() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("single filter")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {
                "@type": "ex:User",
                "schema:age": "?age",
                "schema:name": "?name"
            },
            ["filter", "(> ?age 45)"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", 50],
            ["David", 46]
        ]))
    );
}

#[tokio::test]
async fn jsonld_bind_error_invalid_iri_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:iri-type";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(iri 42)"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "IRI requires a string or IRI argument").await;
}

#[tokio::test]
async fn jsonld_bind_error_invalid_datatype_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:dt-iri";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(str-dt ?name \"bad:datatype\")"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "Unknown datatype IRI").await;
}

#[tokio::test]
async fn jsonld_bind_error_strlang_non_string() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:strlang-non-string";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(str-lang 42 \"en\")"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "STRLANG requires a string lexical form").await;
}

#[tokio::test]
async fn jsonld_bind_error_iri_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:iri-arity";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(iri ?name ?name)"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "IRI requires exactly 1 argument").await;
}

#[tokio::test]
async fn jsonld_bind_error_strdt_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:strdt-arity";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(str-dt ?name)"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "STRDT requires exactly 2 arguments").await;
}

#[tokio::test]
async fn jsonld_bind_error_strlang_arity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:strlang-arity";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(str-lang ?name)"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "STRLANG requires exactly 2 arguments").await;
}

#[tokio::test]
async fn jsonld_bind_error_in_requires_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/bind-error:in-list";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id": "ex:alice", "schema:name": "?name"},
            ["bind", "?err", "(in ?name 1)"]
        ],
        "select": "?err"
    });

    assert_query_bind_error(&fluree, &ledger, query, "in requires a list literal").await;
}

#[tokio::test]
async fn jsonld_filter_single_filter_different_vars() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("single filter, different vars")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter-different-vars:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?last"],
        "where": [
            {
                "@type": "ex:User",
                "schema:age": "?age",
                "schema:name": "?name",
                "ex:last": "?last"
            },
            ["filter", "(and (> ?age 45) (strEnds ?last \"ith\"))"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "Smith"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_filter_multiple_filters_same_var() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("multiple filters on same var")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter-multi-same:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {
                "@type": "ex:User",
                "schema:age": "?age",
                "schema:name": "?name"
            },
            ["filter", "(> ?age 45)", "(< ?age 50)"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["David", 46]
        ]))
    );
}

#[tokio::test]
async fn jsonld_filter_multiple_filters_different_vars() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("multiple filters, different vars")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter-multi-different:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?last"],
        "where": [
            {
                "@type": "ex:User",
                "schema:age": "?age",
                "schema:name": "?name",
                "ex:last": "?last"
            },
            ["filter", "(> ?age 45)", "(strEnds ?last \"ith\")"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "Smith"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_filter_nested_filters() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("nested filters")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter-nested:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {
                "@type": "ex:User",
                "schema:age": "?age",
                "schema:name": "?name"
            },
            ["filter", "(> ?age (/ (+ ?age 47) 2))"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", 50]
        ]))
    );
}

#[tokio::test]
async fn jsonld_filter_filtering_for_absence() {
    // Mirrors `fluree.db.query.filter-query-test/filter-test` ("filtering for absence")
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/filter-absence:main";
    let ledger = seed_people_filter_dataset(&fluree, alias).await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": "?name",
        "where": [
            {
                "@id": "?s",
                "@type": "ex:User",
                "schema:name": "?name"
            },
            ["optional", {"@id": "?s", "ex:favColor": "?favColor"}],
            ["filter", "(not (bound ?favColor))"]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Flatten single column results
    let names: Vec<String> = json_rows.as_array().unwrap().iter()
        .map(|row| {
            if let Some(s) = row.as_str() {
                s.to_string()
            } else if let Some(arr) = row.as_array() {
                arr[0].as_str().unwrap().to_string()
            } else {
                panic!("Unexpected row format")
            }
        })
        .collect();

    assert_eq!(names, vec!["Brian", "David"]);
}

#[tokio::test]
async fn jsonld_optional_basic_left_join() {
    // Mirrors `fluree.db.query.optional-query-test/optional-queries` (basic single optional)
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional", {"@id": "?s", "ex:favColor": "?favColor"}]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green"],
            ["Brian", null],
            ["Cam", null]
        ]))
    );
}

#[tokio::test]
async fn jsonld_optional_with_passthrough() {
    // Mirrors "including another pass-through variable - note Brian doesn't have an email"
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-passthrough:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name", "schema:email": "?email"},
            ["optional", {"@id": "?s", "ex:favColor": "?favColor"}]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green", "alice@flur.ee"],
            ["Cam", null, "cam@flur.ee"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_optional_sandwiched() {
    // Mirrors "including another pass-through variable, but with 'optional' sandwiched"
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-sandwiched:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional", {"@id": "?s", "ex:favColor": "?favColor"}],
            {"@id": "?s", "schema:email": "?email"}
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green", "alice@flur.ee"],
            ["Cam", null, "cam@flur.ee"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_optional_two_separate() {
    // Mirrors "query with two optionals!"
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-two-separate:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional", {"@id": "?s", "ex:favColor": "?favColor"}],
            ["optional", {"@id": "?s", "schema:email": "?email"}]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green", "alice@flur.ee"],
            ["Brian", null, null],
            ["Cam", null, "cam@flur.ee"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_optional_two_in_same_vector() {
    // Mirrors "query with two optionals in the same vector"
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-two-same-vector:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional",
             {"@id": "?s", "ex:favColor": "?favColor"},
             {"@id": "?s", "schema:email": "?email"}
            ]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green", "alice@flur.ee"],
            ["Brian", null, null],
            ["Cam", null, "cam@flur.ee"]
        ]))
    );
}

#[tokio::test]
async fn jsonld_optional_multiple_clauses_left_join() {
    // Mirrors "Multiple optional clauses should work as a left outer join between them"
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-left-join:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:friend": [{"@id": "ex:alice"}]},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@flur.ee", "ex:favColor": "Green"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "schema:email": "cam@flur.ee", "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?favColor", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional", {
                "@id": "?s",
                "ex:favColor": "?favColor",
                "schema:email": "?email"
            }]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "Green", "alice@flur.ee"],
            ["Brian", null, null],
            ["Cam", null, null]
        ]))
    );
}

#[tokio::test]
async fn jsonld_nested_optionals() {
    // Mirrors `nested-optionals` deftest with deeply nested optional clauses
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/nested-optionals:main";

    let ledger0 = genesis_ledger(&fluree, alias);
    let insert = json!({
        "@context": {"ex": "http://example.com/"},
        "@graph": [
            {
                "@id": "ex:1",
                "ex:lit": "literal1",
                "ex:ref": {
                    "@id": "ex:2",
                    "ex:lit": "literal2",
                    "ex:ref": {
                        "@id": "ex:3",
                        "ex:lit": "literal3",
                        "ex:ref": {
                            "@id": "ex:4",
                            "ex:lit": "literal4",
                            "ex:ref": {"@id": "ex:5"}
                        }
                    }
                }
            }
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": {"ex": "http://example.com/"},
        "where": [
            {"@id": "?s1", "ex:lit": "literal1"},
            {"@id": "?s1", "?p1": "?o1"},
            ["optional",
             {"@id": "?o1", "?p2": "?o2"},
             ["optional",
              {"@id": "?o2", "?p3": "?o3"},
              ["optional",
               {"@id": "?o3", "?p4": "?o4"}
              ]
             ]
            ]
        ],
        "select": ["?s1", "?p1", "?o1", "?p2", "?o2", "?p3", "?o3", "?p4", "?o4"]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Expected results with nested optionals creating multiple levels
    let expected = json!([
        ["ex:1", "ex:lit", "literal1", null, null, null, null, null, null],
        ["ex:1", "ex:ref", "ex:2", "ex:lit", "literal2", null, null, null, null],
        ["ex:1", "ex:ref", "ex:2", "ex:ref", "ex:3", "ex:lit", "literal3", null, null],
        ["ex:1", "ex:ref", "ex:2", "ex:ref", "ex:3", "ex:ref", "ex:4", "ex:lit", "literal4"],
        ["ex:1", "ex:ref", "ex:2", "ex:ref", "ex:3", "ex:ref", "ex:4", "ex:ref", "ex:5"]
    ]);

    assert_eq!(json_rows, expected);
}

#[tokio::test]
async fn jsonld_union_basic_passthrough() {
    // Mirrors `fluree.db.query.union-query-test/union-queries` (basic combine emails into one var)
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/union:main";
    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "schema:email": "brian@example.org"},
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:email": "alice@example.org"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "ex:email": "cam@example.org"}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["union",
                {"@id": "?s", "ex:email": "?email"},
                {"@id": "?s", "schema:email": "?email"}
            ]
        ]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", "alice@example.org"],
            ["Brian", "brian@example.org"],
            ["Cam", "cam@example.org"]
        ]))
    );
}

// ============================================================================
// Multi-pattern OPTIONAL tests
// ============================================================================

#[tokio::test]
async fn jsonld_optional_with_filter() {
    // OPTIONAL with FILTER - only include optional bindings where filter passes
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-filter:main";
    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:age": 25},
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:age": 15},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam"}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Query: select users, optionally get age if >= 18
    let query = json!({
        "@context": ctx,
        "select": ["?name", "?age"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional",
                {"@id": "?s", "ex:age": "?age"},
                ["filter", [">=", "?age", 18]]
            ]
        ],
        "orderBy": "?name"
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Alice's age passes filter (25 >= 18), Brian's doesn't (15 < 18), Cam has no age
    assert_eq!(json_rows, json!([
        ["Alice", 25],
        ["Brian", null],
        ["Cam", null]
    ]));
}

#[tokio::test]
async fn jsonld_optional_with_multiple_triples() {
    // OPTIONAL with multiple node patterns (separate objects) - each is a separate optional
    // Fluree semantics: ["optional", {node1}, {node2}] means two separate left joins
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-multi-triple:main";
    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:age": 25, "ex:city": "NYC"},
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:age": 30},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam", "ex:city": "LA"}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Query: select users, optionally get age, optionally get city (separate optionals)
    // Two node-map objects = two separate optional joins
    let query = json!({
        "@context": ctx,
        "select": ["?name", "?age", "?city"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional",
                {"@id": "?s", "ex:age": "?age"},
                {"@id": "?s", "ex:city": "?city"}
            ]
        ],
        "orderBy": "?name"
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Each optional is independent: Alice has both, Brian has only age, Cam has only city
    assert_eq!(json_rows, json!([
        ["Alice", 25, "NYC"],
        ["Brian", 30, null],
        ["Cam", null, "LA"]
    ]));
}

#[tokio::test]
async fn jsonld_optional_with_bind() {
    // OPTIONAL with BIND - compute a value within the optional block
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-bind:main";
    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:price": 100},
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian", "ex:price": 200},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam"}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Query: select users, optionally compute discounted price
    let query = json!({
        "@context": ctx,
        "select": ["?name", "?price", "?discounted"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional",
                {"@id": "?s", "ex:price": "?price"},
                ["bind", "?discounted", ["expr", ["*", "?price", 0.9]]]
            ]
        ],
        "orderBy": "?name"
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Alice and Brian have prices, Cam doesn't
    assert_eq!(json_rows, json!([
        ["Alice", 100, 90.0],
        ["Brian", 200, 180.0],
        ["Cam", null, null]
    ]));
}

#[tokio::test]
async fn jsonld_optional_with_subquery() {
    // OPTIONAL containing a subquery - uses ["query", {...}] syntax
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "query/optional-subquery:main";
    let ledger0 = genesis_ledger(&fluree, alias);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice"},
            {"@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian"},
            {"@id": "ex:cam", "@type": "ex:User", "schema:name": "Cam"},
            {"@id": "ex:order1", "@type": "ex:Order", "ex:user": {"@id": "ex:alice"}, "ex:amount": 50},
            {"@id": "ex:order2", "@type": "ex:Order", "ex:user": {"@id": "ex:alice"}, "ex:amount": 75},
            {"@id": "ex:order3", "@type": "ex:Order", "ex:user": {"@id": "ex:brian"}, "ex:amount": 100}
        ]
    });

    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Query: select users with optional order info via subquery
    let query = json!({
        "@context": ctx,
        "select": ["?name", "?amt"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["optional",
                ["query", {
                    "@context": ctx,
                    "select": ["?s", "?amt"],
                    "where": [
                        {"@id": "?order", "@type": "ex:Order", "ex:user": "?s", "ex:amount": "?amt"}
                    ]
                }]
            ]
        ],
        "orderBy": ["?name", "?amt"]
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_rows = result.to_jsonld(&ledger.db).expect("jsonld");

    // Alice has 2 orders (50, 75), Brian has 1 order (100), Cam has none
    assert_eq!(json_rows, json!([
        ["Alice", 50],
        ["Alice", 75],
        ["Brian", 100],
        ["Cam", null]
    ]));
}

// =============================================================================
// Query entrypoint parity tests (from it_query_jsonld_auto.rs)
// =============================================================================

#[tokio::test]
async fn query_jsonld_works_for_values_only_query() {
    let fluree: MemoryFluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/main");

    // No WHERE, only VALUES (allowed in Clojure; we support this now)
    let q = json!({
        "@context": context_ex_schema(),
        "select": ["?x"],
        "values": ["?x", [1, 2, 3]]
    });

    let v = fluree.query_jsonld(&ledger, &q).await.unwrap();
    assert!(v.is_array());
}

#[tokio::test]
async fn query_format_async_works_for_non_crawl_queries() {
    use fluree_db_api::format::FormatterConfig;

    let fluree: MemoryFluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/main");

    let q = json!({
        "@context": context_ex_schema(),
        "select": ["?x"],
        "values": ["?x", [1]]
    });

    let cfg = FormatterConfig::jsonld();
    let v = fluree.query_format(&ledger, &q, &cfg).await.unwrap();
    assert!(v.is_array());
}

