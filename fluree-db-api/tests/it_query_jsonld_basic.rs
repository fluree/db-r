//! JSON-LD basic integration tests (Clojure parity)
//!
//! Ports from `db-clojure/test/fluree/db/query/json_ld_basic_test.clj`.

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::{Db, SimpleCache};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    // Keep this explicit and stable:
    // - no @vocab (so @id compaction stays prefix-based only)
    // - define "id" -> "@id" for faux-compact-iri parity
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "wiki": "http://www.wikidata.org/entity/",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "id": "@id"
    })
}

async fn seed_movie_graph() -> (fluree_db_api::Fluree<fluree_db_core::MemoryStorage, SimpleCache, fluree_db_nameservice::memory::MemoryNameService>, LedgerState<fluree_db_core::MemoryStorage, SimpleCache>) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/jsonld-basic:movie";

    let db0 = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    // Minimal “movie -> book -> author” shape to exercise graph crawl + depth.
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "wiki:Qmovie",
                "@type": "schema:Movie",
                "schema:name": "The Hitchhiker's Guide to the Galaxy",
                "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
                "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                "schema:isBasedOn": {"@id": "wiki:Qbook"}
            },
            {
                "@id": "wiki:Qbook",
                "@type": "schema:Book",
                "schema:name": "The Hitchhiker's Guide to the Galaxy",
                "schema:isbn": "0-330-25864-8",
                "schema:author": {"@id": "wiki:Qauthor"}
            },
            {
                "@id": "wiki:Qauthor",
                "@type": "schema:Person",
                "schema:name": "Douglas Adams"
            }
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert movie graph");
    (fluree, committed.ledger)
}

fn normalize_object_arrays(value: &mut JsonValue) {
    match value {
        JsonValue::Array(arr) => {
            for item in arr.iter_mut() {
                normalize_object_arrays(item);
            }

            if arr.iter().all(|v| v.is_number()) {
                arr.sort_by(|a, b| {
                    a.as_f64()
                        .unwrap_or_default()
                        .partial_cmp(&b.as_f64().unwrap_or_default())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else if arr.iter().all(|v| v.as_object().is_some()) {
                arr.sort_by(|a, b| {
                    let a_id = a
                        .as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let b_id = b
                        .as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    a_id.cmp(b_id)
                });
            }
        }
        JsonValue::Object(map) => {
            for value in map.values_mut() {
                normalize_object_arrays(value);
            }
        }
        _ => {}
    }
}

async fn seed_simple_subject_crawl() -> (fluree_db_api::Fluree<fluree_db_core::MemoryStorage, SimpleCache, fluree_db_nameservice::memory::MemoryNameService>, LedgerState<fluree_db_core::MemoryStorage, SimpleCache>) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/jsonld-basic:ssc";
    let db0 = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "ex:last": "Smith",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favColor": "Green",
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "ex:last": "Smith",
                "schema:email": "alice@example.org",
                "ex:favColor": "Green",
                "schema:age": 42,
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "ex:last": "Jones",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:favColor": "Blue",
                "ex:favNums": [5, 10],
                "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]
            },
            {
                "@id": "ex:david",
                "@type": "ex:User",
                "schema:name": "David",
                "ex:last": "Jones",
                "schema:email": "david@example.org",
                "schema:age": 46,
                "ex:favNums": [15, 70],
                "ex:friend": {"@id": "ex:cam"}
            }
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert ssc data");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn jsonld_basic_wildcard_single_subject_query() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": { "wiki:Qmovie": ["*"] }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let arr = json.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("object row");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert!(obj.get("schema:name").is_some(), "expected schema:name");
    assert!(
        obj.get("schema:isBasedOn").is_some(),
        "expected schema:isBasedOn"
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_explicit_fields() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": { "wiki:Qmovie": ["@id", "schema:name"] }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let arr = json.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("object row");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert_eq!(
        obj.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_select_one() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["@id", "schema:name"] }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    let obj = json.as_object().expect("object result");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert_eq!(
        obj.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_graph_crawl() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*", {"schema:isBasedOn": ["*"]}] }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:name": "The Hitchhiker's Guide to the Galaxy",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {
            "@id": "wiki:Qbook",
            "@type": "schema:Book",
            "schema:name": "The Hitchhiker's Guide to the Galaxy",
            "schema:isbn": "0-330-25864-8",
            "schema:author": {"@id": "wiki:Qauthor"}
        }
    });

    assert_eq!(json, expected);
}

#[tokio::test]
async fn jsonld_basic_single_subject_graph_crawl_with_depth() {
    // Mirrors the “depth graph crawl” behavior in Clojure:
    // with depth=3 and wildcard selection, refs should auto-expand transitively.
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*"] },
        "depth": 3
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let movie = json.as_object().expect("movie object");
    assert_eq!(movie.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));

    // isBasedOn should be an expanded object (depth expansion)
    let book = movie
        .get("schema:isBasedOn")
        .and_then(|v| v.as_object())
        .expect("schema:isBasedOn object");
    assert_eq!(book.get("@id").and_then(|v| v.as_str()), Some("wiki:Qbook"));

    // author should be expanded as well
    let author = book
        .get("schema:author")
        .and_then(|v| v.as_object())
        .expect("schema:author object");
    assert_eq!(
        author.get("@id").and_then(|v| v.as_str()),
        Some("wiki:Qauthor")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_graph_crawl_with_depth_and_subselection() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*", {"schema:isBasedOn": ["*"]}] },
        "depth": 3
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:name": "The Hitchhiker's Guide to the Galaxy",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {
            "@id": "wiki:Qbook",
            "@type": "schema:Book",
            "schema:name": "The Hitchhiker's Guide to the Galaxy",
            "schema:isbn": "0-330-25864-8",
            "schema:author": {
                "@id": "wiki:Qauthor",
                "@type": "schema:Person",
                "schema:name": "Douglas Adams"
            }
        }
    });

    assert_eq!(json, expected);
}

#[tokio::test]
async fn jsonld_query_with_faux_compact_iri_ids() {
    // Mirrors `query-with-faux-compact-iri`:
    // subjects can have ids that look compact ("foaf:bar") without being real IRIs.
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/jsonld-basic:faux-compact";

    let db0 = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {"id":"foo","ex:name":"Foo"},
            {"id":"foaf:bar","ex:name":"Bar"}
        ]
    });

    let _committed = fluree.insert(ledger0, &tx).await.expect("insert faux ids");
    let loaded = fluree.ledger(alias).await.expect("reload ledger");

    // Analytical SELECT (order not guaranteed; normalize)
    let q1 = json!({
        "@context": ctx(),
        "select": ["?f","?n"],
        "where": {"id":"?f","ex:name":"?n"}
    });

    let r1 = fluree.query(&loaded, &q1).await.expect("query select");
    let mut rows = r1.to_jsonld(&loaded.db).expect("to_jsonld");
    let arr = rows.as_array_mut().expect("rows array");
    arr.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    assert_eq!(
        rows,
        json!([
            ["foaf:bar","Bar"],
            ["foo","Foo"]
        ])
    );

    // Subject crawl SELECT
    let q2 = json!({
        "@context": ctx(),
        "select": {"foo": ["*"]}
    });
    let r2 = fluree.query(&loaded, &q2).await.expect("query crawl");
    let json2 = r2.to_jsonld_async(&loaded.db).await.expect("to_jsonld_async");
    let arr2 = json2.as_array().expect("array result");
    assert_eq!(arr2.len(), 1);
    let obj = arr2[0].as_object().expect("object");
    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("foo"));
    assert_eq!(obj.get("ex:name").and_then(|v| v.as_str()), Some("Foo"));
}

#[tokio::test]
async fn jsonld_expanding_literal_nodes_wildcard() {
    // Mirrors "expanding literal nodes - with wildcard"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "selectOne": {
            "wiki:Qmovie": ["*", {"schema:name": ["*"]}]
        }
    });

    let result = fluree.query(&ledger, &q).await.expect("query expanding literal");
    let json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    // Expected result with expanded literal node
    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:name": {
            "@value": "The Hitchhiker's Guide to the Galaxy",
            "@type": "xsd:string"
        },
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {"@id": "wiki:Qbook"}
    });

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_rdf_type_query_analytical() {
    // Mirrors "json-ld rdf type queries - basic analytical RDF type query"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "select": {"?s": ["*", {"schema:isBasedOn": ["*"]}]},
        "where": {
            "@id": "?s",
            "@type": "schema:Movie"
        }
    });

    let result = fluree.query(&ledger, &q).await.expect("query rdf type");
    let json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    // Should return the movie with its properties
    let arr = json_result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);

    let movie = &arr[0];
    assert_eq!(movie.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert_eq!(movie.get("@type").and_then(|v| v.as_str()), Some("schema:Movie"));
    assert_eq!(movie.get("schema:name").and_then(|v| v.as_str()), Some("The Hitchhiker's Guide to the Galaxy"));
}

#[tokio::test]
async fn jsonld_list_order_preservation_context_container() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/jsonld-basic:list-container";
    let db0 = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "ex:list": {"@container": "@list"}
        },
        "@graph": [
            {"@id": "list-test", "ex:list": [42, 2, 88, 1]}
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert list container");
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "ex:list": {"@container": "@list"}
        },
        "selectOne": { "list-test": ["*"] }
    });

    let result = fluree.query(&committed.ledger, &query).await.expect("query list container");
    let json_result = result.to_jsonld_async(&committed.ledger.db).await.expect("to_jsonld_async");
    assert_eq!(
        json_result,
        json!({"@id": "list-test", "ex:list": [42, 2, 88, 1]})
    );
}

#[tokio::test]
async fn jsonld_list_order_preservation_explicit_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/jsonld-basic:list-explicit";
    let db0 = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id"
        },
        "@graph": [
            {"@id": "list-test2", "ex:list": {"@list": [42, 2, 88, 1]}}
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert list");
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id"
        },
        "selectOne": { "list-test2": ["*"] }
    });

    let result = fluree.query(&committed.ledger, &query).await.expect("query list");
    let json_result = result.to_jsonld_async(&committed.ledger.db).await.expect("to_jsonld_async");
    assert_eq!(
        json_result,
        json!({"@id": "list-test2", "ex:list": [42, 2, 88, 1]})
    );
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_direct_id() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "ex:brian": ["*"] }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    assert_eq!(
        json_result,
        json!([{
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favColor": "Green",
            "ex:favNums": 7
        }])
    );
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_where_type() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "@type": "ex:User" }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "ex:last": "Jones",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": [{"@id": "ex:alice"}, {"@id": "ex:brian"}],
            "ex:favColor": "Blue"
        },
        {
            "@id": "ex:david",
            "@type": "ex:User",
            "schema:name": "David",
            "ex:last": "Jones",
            "schema:email": "david@example.org",
            "schema:age": 46,
            "ex:favNums": [15, 70],
            "ex:friend": {"@id": "ex:cam"}
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_tuple_name() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:name": "Alice" }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [42, 76, 9],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut json_result);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_tuple_fav_color() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "ex:favColor": "?color" }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "ex:last": "Jones",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": [{"@id": "ex:alice"}, {"@id": "ex:brian"}],
            "ex:favColor": "Blue"
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_limit_two() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "ex:favColor": "?color" },
        "orderBy": ["?s"],
        "limit": 2
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_age_and_fav_color() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:age": 42, "ex:favColor": "Green" }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [9, 42, 76],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_age_only() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:age": 42 }
    });

    let result = fluree.query(&ledger, &query).await.expect("query");
    let mut json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [9, 42, 76],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_expanding_literal_nodes_specific_properties() {
    // Mirrors "expanding literal nodes - with specific virtual properties"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "selectOne": {
            "wiki:Qmovie": ["*", {"schema:name": ["@type"]}]
        }
    });

    let result = fluree.query(&ledger, &q).await.expect("query expanding literal specific");
    let json_result = result.to_jsonld_async(&ledger.db).await.expect("to_jsonld_async");

    // Expected result with only @type from the expanded literal
    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:name": {"@type": "xsd:string"},
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {"@id": "wiki:Qbook"}
    });

    assert_eq!(json_result, expected);
}

