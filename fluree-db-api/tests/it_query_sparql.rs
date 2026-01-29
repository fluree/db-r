//! SPARQL integration tests (Clojure parity)
//!
//! Mirrors `db-clojure/test/fluree/db/query/sparql_test.cljc` at a high level:
//! - Create a ledger
//! - Seed data (the Clojure integration test uses memory-backed storage)
//! - Query using SPARQL
//! - Exercise update semantics via JSON-LD Update transactions (DELETE/INSERT/WHERE behavior)
//!
//! Note: The original Clojure integration test seeds data via SPARQL UPDATE (INSERT DATA).
//! In Rust, we seed via JSON-LD insert and then validate SPARQL query behavior.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{
    assert_index_defaults, genesis_ledger, normalize_rows_array, normalize_sparql_bindings,
    MemoryFluree, MemoryLedger,
};

fn normalize_object_rows(value: &JsonValue) -> Vec<String> {
    let Some(array) = value.as_array() else {
        return Vec::new();
    };
    let mut rows: Vec<String> = array
        .iter()
        .map(|row| serde_json::to_string(row).expect("serialize row"))
        .collect();
    rows.sort();
    rows
}

async fn seed_people(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);

    // Seed dataset roughly equivalent to the Clojure SPARQL INSERT DATA payload.
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "person": "http://example.org/Person#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:jdoe",
                "@type": "ex:Person",
                "person:handle": "jdoe",
                "person:fullName": "Jane Doe",
                "person:favNums": [3, 7, 42, 99]
            },
            {
                "@id": "ex:bbob",
                "@type": "ex:Person",
                "person:handle": "bbob",
                "person:fullName": "Billy Bob",
                "person:favNums": [23]
            },
            {
                "@id": "ex:jbob",
                "@type": "ex:Person",
                "person:handle": "jbob",
                "person:fullName": "Jenny Bob",
                "person:favNums": [8, 6, 7, 5, 3, 0, 9]
            },
            {
                "@id": "ex:fbueller",
                "@type": "ex:Person",
                "person:handle": "dankeshön",
                "person:fullName": "Ferris Bueller",
                "person:email": "fb@example.com"
            },
            { "@id": "ex:alice", "foaf:givenname": "Alice", "foaf:family_name": "Hacker" },
            { "@id": "ex:bob", "foaf:firstname": "Bob", "foaf:surname": "Hacker" },
            {
                "@id": "ex:carol",
                "ex:catchphrase": [
                    {"@value": "Heyyyy", "@language": "en"},
                    {"@value": "¡Eyyyy!", "@language": "es"}
                ]
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed");
    committed.ledger
}

async fn seed_books(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);

    let insert = json!({
        "@context": {
            "book": "http://example.org/book/",
            "ex": "http://example.org/book/"
        },
        "@graph": [
            {
                "@id": "book:1",
                "@type": "book:Book",
                "book:title": "For Whom the Bell Tolls"
            },
            {
                "@id": "book:2",
                "@type": "book:Book",
                "book:title": "The Hitchhiker's Guide to the Galaxy"
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert books")
        .ledger
}

#[tokio::test]
async fn sparql_basic_query_outputs_jsonld_and_sparql_json() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?fullName
        WHERE {
          ?person person:handle "jdoe" .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = fluree
        .query_sparql(&ledger, query)
        .await
        .expect("sparql query should succeed");

    // Clojure parity default output (array rows).
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");
    assert_eq!(jsonld, json!([["ex:jdoe", "Jane Doe"]]));

    // SPARQL JSON output (Clojure parity uses compact IRIs).
    let sparql_json = result.to_sparql_json(&ledger.db).expect("to_sparql_json");
    assert_eq!(
        sparql_json,
        json!({
            "head": {"vars": ["fullName", "person"]},
            "results": {"bindings": [
                {
                    "person": {"type": "uri", "value": "ex:jdoe"},
                    "fullName": {"type": "literal", "value": "Jane Doe"}
                }
            ]}
        })
    );
}

#[tokio::test]
async fn sparql_filter_query_outputs_jsonld_and_sparql_json() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle ?favNum
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
          FILTER ( ?favNum > 10 ) .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();

    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");
    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([["bbob", 23], ["jdoe", 42], ["jdoe", 99]]))
    );

    let sparql_json = result.to_sparql_json(&ledger.db).expect("to_sparql_json");
    // Order is not guaranteed; compare bindings as a set.
    assert_eq!(
        normalize_sparql_bindings(&sparql_json),
        normalize_sparql_bindings(&json!({
            "head": {"vars": ["favNum", "handle"]},
            "results": {"bindings": [
                {
                    "handle": {"type": "literal", "value": "bbob"},
                    "favNum": {"type": "literal", "value": "23", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                },
                {
                    "handle": {"type": "literal", "value": "jdoe"},
                    "favNum": {"type": "literal", "value": "42", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                },
                {
                    "handle": {"type": "literal", "value": "jdoe"},
                    "favNum": {"type": "literal", "value": "99", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
                }
            ]}
        }))
    );
}

#[tokio::test]
async fn sparql_count_star_counts_solutions() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT (COUNT(*) AS ?cnt)
        WHERE { ?p a ex:Person . }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");
    // Single-variable queries return flat array
    assert_eq!(jsonld, json!([4]));
}

#[tokio::test]
async fn sparql_count_distinct_with_group_by_and_order_by() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    // Test the user's exact query pattern:
    // SELECT ?handle (COUNT(DISTINCT ?favNum) AS ?distinctCount)
    // WHERE { ... } GROUP BY ?handle ORDER BY DESC(?distinctCount) LIMIT 10
    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle (COUNT(DISTINCT ?favNum) AS ?distinctCount)
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNum .
        }
        GROUP BY ?handle
        ORDER BY DESC(?distinctCount)
        LIMIT 10
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Expected: jbob has 7 distinct favNums, jdoe has 4, bbob has 1
    // fbueller has no favNums so won't appear
    // ORDER BY DESC means jbob first, then jdoe, then bbob
    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([["jbob", 7], ["jdoe", 4], ["bbob", 1]]))
    );
}

#[tokio::test]
async fn sparql_delete_data_removes_specified_triples() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    // Equivalent to Clojure's SPARQL: DELETE DATA { ex:jdoe person:favNums 3 . ex:jdoe person:favNums 7 . }
    // Represented as a JSON-LD Update transaction (no WHERE needed).
    let delete_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "person": "http://example.org/Person#"
        },
        "delete": [
            {"@id": "ex:jdoe", "person:favNums": 3},
            {"@id": "ex:jdoe", "person:favNums": 7}
        ]
    });

    let ledger2 = fluree.update(ledger, &delete_txn).await.unwrap().ledger;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNum
        WHERE { ex:jdoe person:favNums ?favNum }
        ORDER BY ?favNum
    "#;

    let result = fluree.query_sparql(&ledger2, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger2.db).expect("to_jsonld");
    // Single-variable queries return flat array
    assert_eq!(jsonld, json!([42, 99]));
}

#[tokio::test]
async fn sparql_select_star_returns_object_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT *
        WHERE {
          ?person person:handle ?handle ;
                  person:favNums ?favNums .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        {"?person": "ex:bbob", "?handle": "bbob", "?favNums": 23},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 3},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 7},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 42},
        {"?person": "ex:jdoe", "?handle": "jdoe", "?favNums": 99},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 0},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 3},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 5},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 6},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 7},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 8},
        {"?person": "ex:jbob", "?handle": "jbob", "?favNums": 9}
    ]);

    assert_eq!(normalize_object_rows(&jsonld), normalize_object_rows(&expected));
}

#[tokio::test]
async fn sparql_lang_filter_limits_language_tagged_literals() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?phrase
        WHERE {
          ex:carol ex:catchphrase ?phrase .
          FILTER ( LANG(?phrase) = "en" ) .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");
    assert_eq!(
        jsonld,
        json!([{"@value": "Heyyyy", "@language": "en"}])
    );
}

#[tokio::test]
async fn sparql_union_combines_unioned_patterns() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name
        WHERE {
          { ?s foaf:givenname ?name }
          UNION
          { ?s foaf:firstname ?name }
        }
        ORDER BY ?name
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");
    assert_eq!(jsonld, json!(["Alice", "Bob"]));
}

#[tokio::test]
async fn sparql_optional_includes_unbound_values_as_null() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL { ?person person:favNums ?favNums . }
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", 23],
        ["ex:fbueller", null],
        ["ex:jbob", 0],
        ["ex:jbob", 3],
        ["ex:jbob", 5],
        ["ex:jbob", 6],
        ["ex:jbob", 7],
        ["ex:jbob", 8],
        ["ex:jbob", 9],
        ["ex:jdoe", 3],
        ["ex:jdoe", 7],
        ["ex:jdoe", 42],
        ["ex:jdoe", 99]
    ]);

    assert_eq!(normalize_rows_array(&jsonld), normalize_rows_array(&expected));
}

#[tokio::test]
async fn sparql_optional_multi_pattern_allows_partial_binding() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums ?email
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL {
            ?person person:favNums ?favNums .
            ?person person:email ?email .
          }
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", 23, null],
        ["ex:fbueller", null, "fb@example.com"],
        ["ex:jbob", 0, null],
        ["ex:jbob", 3, null],
        ["ex:jbob", 5, null],
        ["ex:jbob", 6, null],
        ["ex:jbob", 7, null],
        ["ex:jbob", 8, null],
        ["ex:jbob", 9, null],
        ["ex:jdoe", 3, null],
        ["ex:jdoe", 7, null],
        ["ex:jdoe", 42, null],
        ["ex:jdoe", 99, null]
    ]);

    assert_eq!(normalize_rows_array(&jsonld), normalize_rows_array(&expected));
}

#[tokio::test]
async fn sparql_group_by_with_optional_preserves_grouped_lists() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?favNums
        WHERE {
          ?person person:handle ?handle .
          OPTIONAL { ?person person:favNums ?favNums . }
        }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        ["ex:bbob", [23]],
        ["ex:fbueller", [null]],
        ["ex:jbob", [0, 3, 5, 6, 7, 8, 9]],
        ["ex:jdoe", [3, 7, 42, 99]]
    ]);

    assert_eq!(normalize_rows_array(&jsonld), normalize_rows_array(&expected));
}

#[tokio::test]
async fn sparql_omitted_subjects_match_expanded_subject_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person ?fullName ?favNums
        WHERE {
          ?person person:handle "jdoe" ;
                  person:fullName ?fullName ;
                  person:favNums ?favNums .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        ["ex:jdoe", "Jane Doe", 3],
        ["ex:jdoe", "Jane Doe", 7],
        ["ex:jdoe", "Jane Doe", 42],
        ["ex:jdoe", "Jane Doe", 99]
    ]);

    assert_eq!(normalize_rows_array(&jsonld), normalize_rows_array(&expected));
}

#[tokio::test]
async fn sparql_scalar_sha512_function_binds_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (SHA512(?handle) AS ?handleHash)
        WHERE { ?person person:handle ?handle . }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let expected = json!([
        "f162b1f2b3a824f459164fe40ffc24a019993058061ca1bf90eca98a4652f98ccaa5f17496be3da45ce30a1f79f45d82d8b8b532c264d4455babc1359aaa461d",
        "eca2f5ab92fddbf2b1c51a60f5269086ce2415cb37964a05ae8a0b999625a8a50df876e97d34735ebae3fa3abb088fca005a596312fdf3326c4e73338f4c8c90",
        "696ba1c7597f0d80287b8f0917317a904fa23a8c25564331a0576a482342d3807c61eff8e50bf5cf09859cfdeb92d448490073f34fb4ea4be43663d2359b51a9",
        "fee256e1850ef33410630557356ea3efd56856e9045e59350dbceb6b5794041d50991093c07ad871e1124e6961f2198c178057cf391435051ac24eb8952bc401"
    ]);

    assert_eq!(normalize_rows_array(&jsonld), normalize_rows_array(&expected));
}

#[tokio::test]
async fn sparql_aggregate_avg_over_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav)
        WHERE { ?person person:favNums ?favNums . }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let avg = jsonld
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_f64())
        .expect("avg result");
    assert!((avg - 17.66666666666667).abs() < 1e-12);
}

#[tokio::test]
async fn sparql_group_by_having_filters_groups() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
        HAVING (AVG(?favNums) > 10)
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let mut values: Vec<f64> = jsonld
        .as_array()
        .expect("avg rows array")
        .iter()
        .filter_map(|v| v.as_f64())
        .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let expected = vec![23.0, 37.75];
    assert_eq!(values.len(), expected.len());
    for (actual, target) in values.iter().zip(expected.iter()) {
        assert!((*actual - *target).abs() < 1e-12);
    }
}

#[tokio::test]
async fn sparql_having_with_multiple_string_constraints() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        GROUP BY ?person ?handle
        HAVING (STRLEN(?handle) < 5 && (STRSTARTS(?handle, "foo") || STRSTARTS(?handle, "bar")))
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(jsonld, json!([]));
}

#[tokio::test]
async fn sparql_having_aggregate_without_select_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?person
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
        HAVING (COUNT(?favNums) > 4)
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(jsonld, json!(["ex:jbob"]));
}

#[tokio::test]
async fn sparql_multiple_select_expressions_with_aggregate_alias() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (AVG(?favNums) AS ?avgFav) (CEIL(?avgFav) AS ?caf)
        WHERE { ?person person:favNums ?favNums . }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let rows = normalize_rows_array(&jsonld);
    assert_eq!(rows.len(), 1);
    let avg = rows[0][0].as_f64().expect("avg");
    let ceil = rows[0][1].as_f64().expect("ceil");
    assert!((avg - 17.66666666666667).abs() < 1e-12);
    assert!((ceil - 18.0).abs() < 1e-12);
}

#[tokio::test]
async fn sparql_group_concat_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (GROUP_CONCAT(?favNums; separator=", ") AS ?nums)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["0, 3, 5, 6, 7, 8, 9"],
            ["3, 7, 42, 99"],
            ["23"]
        ]))
    );
}

#[tokio::test]
async fn sparql_concat_function_formats_strings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (CONCAT(?handle, "-", ?fullName) AS ?hfn)
        WHERE {
          ?person person:handle ?handle .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["bbob-Billy Bob"],
            ["dankeshön-Ferris Bueller"],
            ["jbob-Jenny Bob"],
            ["jdoe-Jane Doe"]
        ]))
    );
}

#[tokio::test]
async fn sparql_mix_of_grouped_values_and_aggregates() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX person: <http://example.org/Person#>
        SELECT ?favNums (AVG(?favNums) AS ?avg) ?person ?handle (MAX(?favNums) AS ?max)
        WHERE {
          ?person person:handle ?handle .
          ?person person:favNums ?favNums .
        }
        GROUP BY ?person ?handle
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let mut rows: Vec<(String, String, Vec<i64>, f64, i64)> = normalize_rows_array(&jsonld)
        .into_iter()
        .map(|row| {
            let fav_nums = row[0]
                .as_array()
                .expect("favNums array")
                .iter()
                .map(|v| v.as_i64().expect("favNum"))
                .collect::<Vec<_>>();
            let avg = row[1].as_f64().expect("avg");
            let person = row[2].as_str().expect("person").to_string();
            let handle = row[3].as_str().expect("handle").to_string();
            let max = row[4].as_i64().expect("max");
            (person, handle, fav_nums, avg, max)
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let expected = vec![
        (
            "ex:bbob".to_string(),
            "bbob".to_string(),
            vec![23],
            23.0,
            23,
        ),
        (
            "ex:jbob".to_string(),
            "jbob".to_string(),
            vec![0, 3, 5, 6, 7, 8, 9],
            5.428571428571429,
            9,
        ),
        (
            "ex:jdoe".to_string(),
            "jdoe".to_string(),
            vec![3, 7, 42, 99],
            37.75,
            99,
        ),
    ];

    assert_eq!(rows.len(), expected.len());
    for (actual, target) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.0, target.0);
        assert_eq!(actual.1, target.1);
        assert_eq!(actual.2, target.2);
        assert!((actual.3 - target.3).abs() < 1e-12);
        assert_eq!(actual.4, target.4);
    }
}

#[tokio::test]
async fn sparql_count_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (COUNT(?favNums) AS ?numFavs)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([[7], [4], [1]]))
    );
}

#[tokio::test]
async fn sparql_count_star_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (COUNT(*) AS ?count)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([[7], [4], [1]]))
    );
}

#[tokio::test]
async fn sparql_sample_aggregate_returns_one_value() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (SAMPLE(?favNums) AS ?favNum)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    let rows = normalize_rows_array(&jsonld);
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert!(row[0].as_i64().is_some());
    }
}

#[tokio::test]
async fn sparql_sum_aggregate_per_group() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(?favNums) AS ?favNum)
        WHERE { ?person person:favNums ?favNums . }
        GROUP BY ?person
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([[38], [151], [23]]))
    );
}

#[tokio::test]
async fn sparql_order_by_ascending_sorts_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        ORDER BY ?handle
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!(["bbob", "dankeshön", "jbob", "jdoe"])
    );
}

#[tokio::test]
async fn sparql_order_by_descending_sorts_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE { ?person person:handle ?handle . }
        ORDER BY DESC(?handle)
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!(["jdoe", "jbob", "dankeshön", "bbob"])
    );
}

#[tokio::test]
async fn sparql_values_filters_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT ?handle
        WHERE {
          VALUES ?handle { "jdoe" "bbob" }
          ?person person:handle ?handle .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!(["bbob", "jdoe"]))
    );
}

#[tokio::test]
async fn sparql_construct_query_outputs_jsonld_graph() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        CONSTRUCT {
          ?x ex:givenName ?gname .
          ?x ex:familyName ?fname .
        }
        WHERE {
          { ?x foaf:firstname ?gname } UNION { ?x foaf:givenname ?gname } .
          { ?x foaf:surname ?fname } UNION { ?x foaf:family_name ?fname } .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_construct(&ledger.db).expect("to_construct");

    let expected = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:givenName": ["Alice"],
                "ex:familyName": ["Hacker"]
            },
            {
                "@id": "ex:bob",
                "ex:givenName": ["Bob"],
                "ex:familyName": ["Hacker"]
            }
        ]
    });

    let mut json_graph = jsonld
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    let mut expected_graph = expected
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();

    let sort_by_id = |a: &JsonValue, b: &JsonValue| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    };
    json_graph.sort_by(sort_by_id);
    expected_graph.sort_by(sort_by_id);

    assert_eq!(json_graph, expected_graph);
}

#[tokio::test]
async fn sparql_construct_where_outputs_graph() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        CONSTRUCT WHERE { ?x foaf:firstname ?fname }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_construct(&ledger.db).expect("to_construct");

    let mut json_graph = jsonld
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    json_graph.sort_by(|a, b| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    });

    let expected = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "foaf": "http://xmlns.com/foaf/0.1/"
        },
        "@graph": [
            {
                "@id": "ex:bob",
                "foaf:firstname": ["Bob"]
            }
        ]
    });

    let mut expected_graph = expected
        .get("@graph")
        .and_then(|v| v.as_array())
        .expect("@graph array")
        .clone();
    expected_graph.sort_by(|a, b| {
        a.get("@id")
            .and_then(|v| v.as_str())
            .cmp(&b.get("@id").and_then(|v| v.as_str()))
    });

    assert_eq!(json_graph, expected_graph);
}

#[tokio::test]
async fn sparql_base_iri_compacts_relative_ids() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "books:main";
    let ledger = seed_books(&fluree, alias).await;

    let query = r#"
        BASE <http://example.org/book/>
        SELECT ?book ?title
        WHERE { ?book <title> ?title . }
        ORDER BY ?book
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["1", "For Whom the Bell Tolls"],
            ["2", "The Hitchhiker's Guide to the Galaxy"]
        ]))
    );
}

#[tokio::test]
async fn sparql_prefix_declarations_compact_ids() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "books:main";
    let ledger = seed_books(&fluree, alias).await;

    let query = r#"
        PREFIX book: <http://example.org/book/>
        SELECT ?book ?title
        WHERE { ?book book:title ?title . }
        ORDER BY ?book
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["book:1", "For Whom the Bell Tolls"],
            ["book:2", "The Hitchhiker's Guide to the Galaxy"]
        ]))
    );
}

#[tokio::test]
async fn sparql_sparql_json_language_tags() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?catchphrase
        WHERE { ex:carol ex:catchphrase ?catchphrase }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let sparql_json = result.to_sparql_json(&ledger.db).expect("to_sparql_json");

    let bindings = sparql_json
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .expect("bindings array");

    assert_eq!(bindings.len(), 2);
    for binding in bindings {
        let lang = binding
            .get("catchphrase")
            .and_then(|v| v.get("xml:lang"))
            .and_then(|v| v.as_str())
            .expect("xml:lang");
        assert!(lang == "en" || lang == "es");
    }
}

#[tokio::test]
async fn sparql_concat_with_langtag_argument() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "people:main";
    let ledger = seed_people(&fluree, alias).await;

    let query = r#"
        PREFIX person: <http://example.org/Person#>
        SELECT (CONCAT(?fullName, "'s handle is "@en, ?handle) AS ?hfn)
        WHERE {
          ?person person:handle ?handle .
          ?person person:fullName ?fullName .
        }
    "#;

    let result = fluree.query_sparql(&ledger, query).await.unwrap();
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows_array(&jsonld),
        normalize_rows_array(&json!([
            ["Billy Bob's handle is bbob"],
            ["Ferris Bueller's handle is dankeshön"],
            ["Jenny Bob's handle is jbob"],
            ["Jane Doe's handle is jdoe"]
        ]))
    );
}

