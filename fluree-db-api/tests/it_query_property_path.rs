//! Property path integration tests (Clojure parity)
//!
//! Mirrors `db-clojure/test/fluree/db/query/property_path_test.clj` using JSON inputs only.
//! Rust *does* implement transitive paths, so where Clojure asserts an "unsupported" error
//! we assert the actual transitive semantics.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_knows_chain(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:b","ex:knows":[{"@id":"ex:c"},{"@id":"ex:d"}]},
            {"@id":"ex:d","ex:knows":{"@id":"ex:e"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

async fn seed_y_chain(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, alias);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:y":[{"@id":"ex:b"},{"@id":"ex:g"}]},
            {"@id":"ex:b","ex:y":{"@id":"ex:c"}},
            {"@id":"ex:c","ex:y":{"@id":"ex:d"}},
            {"@id":"ex:d","ex:y":{"@id":"ex:e"}},
            {"@id":"ex:e","ex:y":{"@id":"ex:f"}},
            {"@id":"ex:g","ex:y":[{"@id":"ex:h"},{"@id":"ex:j"}]},
            {"@id":"ex:h","ex:y":{"@id":"ex:i"}},
            {"@id":"ex:j","ex:y":{"@id":"ex:k"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn property_path_one_or_more_no_vars_matches_transitively() {
    // Clojure: transitive-paths / one+ / no variables
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_y_chain(&fluree, "property/path-oneplus-no-vars:main").await;

    // Sanity: ensure the chain exists in the DB.
    let sanity = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?o"],
        "where": {"@id":"ex:e","ex:y":"?o"}
    });
    let sanity_rows = fluree.query(&ledger, &sanity).await.unwrap().to_jsonld(&ledger.db).unwrap();
    assert_eq!(sanity_rows, json!(["ex:f"]));

    // Sanity: property path traversal from ex:a should reach ex:f.
    let sanity_path = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?o"],
        "where": [{"@id":"ex:a","<ex:y+>":"?o"}]
    });
    let sanity_path_rows = fluree.query(&ledger, &sanity_path).await.unwrap().to_jsonld(&ledger.db).unwrap();
    assert!(normalize_rows(&sanity_path_rows).contains(&json!("ex:f")));

    // Use VALUES to seed a single solution row and treat the (non-)transitive pattern
    // as a filter, so we can assert reachability without relying on graph crawl output.
    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "values": [["?dummy"], [[1]]],
        "where": [{"@id":"ex:a","ex:y":{"@id":"ex:f"}}],
        "select": ["?dummy"]
    });
    let r_non = fluree.query(&ledger, &q_non).await.unwrap().to_jsonld(&ledger.db).unwrap();
    assert_eq!(r_non, json!([]));

    let q_plus = json!({
        "@context": {"ex":"http://example.org/"},
        "values": [["?dummy"], [[1]]],
        "where": [{"@id":"ex:a","<ex:y+>":{"@id":"ex:f"}}],
        "select": ["?dummy"]
    });
    let r_plus = fluree.query(&ledger, &q_plus).await.unwrap().to_jsonld(&ledger.db).unwrap();
    assert_eq!(r_plus, json!([1]));
}

#[tokio::test]
async fn property_path_one_or_more_object_var_with_and_without_cycle() {
    // Clojure: transitive-paths / one+ / object variable
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-oneplus-o:main").await;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"ex:a","ex:knows":"?who"}],
        "select": ["?who"]
    });
    let non = fluree.query(&ledger1, &q_non).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(non, json!(["ex:b"]));

    // Accept both `<ex:knows+>` and the Clojure-style `<<ex:knows>+>` spelling.
    let q_plus = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"ex:a","<<ex:knows>+>":"?who"}],
        "select": ["?who"]
    });
    let plus = fluree.query(&ledger1, &q_plus).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!(["ex:b","ex:c","ex:d","ex:e"]))
    );

    // Add cycle: e knows a. One-or-more from a should now include a as reachable.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:e","ex:knows":{"@id":"ex:a"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let q_plus2 = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"ex:a","<ex:knows+>":"?who"}],
        "select": ["?who"]
    });
    let plus2 = fluree.query(&ledger2, &q_plus2).await.unwrap().to_jsonld(&ledger2.db).unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!(["ex:a","ex:b","ex:c","ex:d","ex:e"]))
    );
}

#[tokio::test]
async fn property_path_one_or_more_subject_var_with_and_without_cycle() {
    // Clojure: transitive-paths / one+ / subject variable
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-oneplus-s:main").await;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?who","ex:knows":{"@id":"ex:e"}}],
        "select": ["?who"]
    });
    let non = fluree.query(&ledger1, &q_non).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(non, json!(["ex:d"]));

    let q_plus = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?who","<ex:knows+>":{"@id":"ex:e"}}],
        "select": ["?who"]
    });
    let plus = fluree.query(&ledger1, &q_plus).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!(["ex:d","ex:b","ex:a"]))
    );

    // Add cycle: e knows a. Now e is also in the reverse transitive set.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:e","ex:knows":{"@id":"ex:a"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let plus2 = fluree.query(&ledger2, &q_plus).await.unwrap().to_jsonld(&ledger2.db).unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!(["ex:d","ex:b","ex:a","ex:e"]))
    );
}

#[tokio::test]
async fn property_path_one_or_more_subject_and_object_vars_transitive_closure() {
    // Clojure: transitive-paths / one+ / subject and object variable
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-oneplus-xy:main");

    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:1","ex:knows":{"@id":"ex:2"}},
            {"@id":"ex:2","ex:knows":{"@id":"ex:3"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?s","ex:knows":"?o"}],
        "select": ["?s","?o"]
    });
    let non = fluree.query(&ledger1, &q_non).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(
        normalize_rows(&non),
        normalize_rows(&json!([["ex:1","ex:2"],["ex:2","ex:3"]]))
    );

    let q_plus = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?x","<ex:knows+>":"?y"}],
        "select": ["?x","?y"]
    });
    let plus = fluree.query(&ledger1, &q_plus).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!([["ex:1","ex:2"],["ex:2","ex:3"],["ex:1","ex:3"]]))
    );

    // Add cycle 3 -> 1, producing reachability including self via non-zero paths.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:3","ex:knows":{"@id":"ex:1"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let plus2 = fluree.query(&ledger2, &q_plus).await.unwrap().to_jsonld(&ledger2.db).unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!([
            ["ex:1","ex:1"],["ex:1","ex:2"],["ex:1","ex:3"],
            ["ex:2","ex:1"],["ex:2","ex:2"],["ex:2","ex:3"],
            ["ex:3","ex:1"],["ex:3","ex:2"],["ex:3","ex:3"]
        ]))
    );
}

#[tokio::test]
async fn property_path_zero_or_more_object_var_and_subject_object_vars() {
    // Clojure: transitive-paths / zero+ / object variable and subject+object variable
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-zeroplus-o:main").await;

    let q_star = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"ex:a","<ex:knows*>":"?who"}],
        "select": ["?who"]
    });
    let star = fluree.query(&ledger1, &q_star).await.unwrap().to_jsonld(&ledger1.db).unwrap();
    assert_eq!(
        normalize_rows(&star),
        normalize_rows(&json!(["ex:a","ex:b","ex:c","ex:d","ex:e"]))
    );

    // Subject+object vars, disjoint graphs
    let ledger0 = genesis_ledger(&fluree, "property/path-zeroplus-xy:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:1","ex:knows":{"@id":"ex:2"}},
            {"@id":"ex:2","ex:knows":{"@id":"ex:3"}},
            {"@id":"ex:4","ex:knows":{"@id":"ex:5"}},
            {"@id":"ex:5","ex:knows":{"@id":"ex:6"}}
        ]
    });
    let ledger2 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_xy = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?x","<ex:knows*>":"?y"}],
        "select": ["?x","?y"]
    });
    let xy = fluree.query(&ledger2, &q_xy).await.unwrap().to_jsonld(&ledger2.db).unwrap();
    assert_eq!(
        normalize_rows(&xy),
        normalize_rows(&json!([
            ["ex:1","ex:1"],["ex:1","ex:2"],["ex:1","ex:3"],
            ["ex:2","ex:2"],["ex:2","ex:3"],
            ["ex:3","ex:3"],
            ["ex:4","ex:4"],["ex:4","ex:5"],["ex:4","ex:6"],
            ["ex:5","ex:5"],["ex:5","ex:6"],
            ["ex:6","ex:6"]
        ]))
    );
}

