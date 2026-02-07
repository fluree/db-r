//! Deep tracing span verification tests.
//!
//! Tests that the tracing spans instrumented throughout the query, transaction,
//! and indexing paths are emitted correctly at the expected levels with the
//! expected parent-child relationships.
//!
//! AC-1: Query waterfall — debug spans inside query_prepare + query_run
//! AC-2: Transaction waterfall — debug spans inside txn_stage + txn_commit
//! AC-5: Zero noise — no debug/trace spans appear at INFO level

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::Db;
use serde_json::json;
use support::tracing::{init_info_only_tracing, init_test_tracing};

type MemoryFluree = fluree_db_api::Fluree<
    fluree_db_core::MemoryStorage,
    fluree_db_nameservice::memory::MemoryNameService,
>;
type MemoryLedger = LedgerState<fluree_db_core::MemoryStorage>;

fn genesis(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let db = Db::genesis(fluree.storage().clone(), alias);
    LedgerState::new(db, Novelty::new(0))
}

/// Seed a small dataset for query tests.
async fn seed_people(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let ledger = genesis(fluree, alias);
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "schema:age": 22}
        ]
    });
    fluree
        .insert(ledger, &insert)
        .await
        .expect("seed insert")
        .ledger
}

// =============================================================================
// AC-1: Query span waterfall
// =============================================================================

#[tokio::test]
async fn ac1_query_sparql_emits_debug_spans() {
    let (store, _guard) = init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "it/tracing:ac1-sparql").await;

    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE { ?s schema:name ?name }
    "#;
    let _result = fluree
        .query_sparql(&ledger, sparql)
        .await
        .expect("sparql query");

    // Core query-path spans should be present
    assert!(
        store.has_span("query_prepare"),
        "expected query_prepare span; got: {:?}",
        store.span_names()
    );
    assert!(
        store.has_span("query_run"),
        "expected query_run span; got: {:?}",
        store.span_names()
    );

    // Debug sub-spans inside query_prepare
    assert!(
        store.has_span("reasoning_prep"),
        "expected reasoning_prep debug span; got: {:?}",
        store.span_names()
    );
    assert!(
        store.has_span("pattern_rewrite"),
        "expected pattern_rewrite debug span; got: {:?}",
        store.span_names()
    );
    assert!(
        store.has_span("plan"),
        "expected plan debug span; got: {:?}",
        store.span_names()
    );

    // Verify levels
    let prep = store.find_span("query_prepare").unwrap();
    assert_eq!(prep.level, tracing::Level::DEBUG);

    let reasoning = store.find_span("reasoning_prep").unwrap();
    assert_eq!(reasoning.level, tracing::Level::DEBUG);

    let rewrite = store.find_span("pattern_rewrite").unwrap();
    assert_eq!(rewrite.level, tracing::Level::DEBUG);

    let plan = store.find_span("plan").unwrap();
    assert_eq!(plan.level, tracing::Level::DEBUG);

    // query_run is INFO level
    let run = store.find_span("query_run").unwrap();
    assert_eq!(run.level, tracing::Level::INFO);

    // Verify parent-child: reasoning_prep, pattern_rewrite, plan should be
    // children of query_prepare
    assert_eq!(
        reasoning.parent_name.as_deref(),
        Some("query_prepare"),
        "reasoning_prep parent"
    );
    assert_eq!(
        rewrite.parent_name.as_deref(),
        Some("query_prepare"),
        "pattern_rewrite parent"
    );
    assert_eq!(
        plan.parent_name.as_deref(),
        Some("query_prepare"),
        "plan parent"
    );
}

#[tokio::test]
async fn ac1_query_fql_emits_debug_spans() {
    let (store, _guard) = init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "it/tracing:ac1-fql").await;

    let q = json!({
        "@context": {
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {"schema:name": "?name"}
    });
    let _result = fluree.query(&ledger, &q).await.expect("fql query");

    assert!(store.has_span("query_prepare"), "expected query_prepare");
    assert!(store.has_span("query_run"), "expected query_run");
    assert!(store.has_span("reasoning_prep"), "expected reasoning_prep");
    assert!(
        store.has_span("pattern_rewrite"),
        "expected pattern_rewrite"
    );
    assert!(store.has_span("plan"), "expected plan");
}

#[tokio::test]
async fn ac1_query_view_emits_parse_and_format_spans() {
    let (store, _guard) = init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people(&fluree, "it/tracing:ac1-view").await;

    // query_view path includes parse + format spans (view loads from nameservice)
    let view = fluree
        .view("it/tracing:ac1-view")
        .await
        .expect("build view");

    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE { ?s schema:name ?name }
    "#;
    let _result = fluree.query_view(&view, sparql).await.expect("view query");

    assert!(
        store.has_span("parse"),
        "expected parse debug span from query_view; got: {:?}",
        store.span_names()
    );
    assert!(store.has_span("query_prepare"), "expected query_prepare");
    assert!(store.has_span("query_run"), "expected query_run");

    // Verify parse has correct field
    let parse = store.find_span("parse").unwrap();
    assert_eq!(parse.level, tracing::Level::DEBUG);
    assert_eq!(
        parse.fields.get("input_format").map(|s| s.as_str()),
        Some("sparql")
    );
}

// =============================================================================
// AC-3: Core operator debug spans (scan, join, filter, project, sort)
// =============================================================================

#[tokio::test]
async fn ac3_query_emits_operator_trace_spans() {
    let (store, _guard) = init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "it/tracing:ac3").await;

    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name
        WHERE { ?s schema:name ?name }
    "#;
    let _result = fluree.query_sparql(&ledger, sparql).await.expect("query");

    // Core operators are debug-level spans (promoted from trace for visibility)
    let debug_spans = store.debug_spans();
    let debug_names: Vec<&str> = debug_spans.iter().map(|s| s.name).collect();

    // A simple SELECT with one pattern should produce at least scan + project
    assert!(
        debug_names.contains(&"scan"),
        "expected scan debug span; got debug spans: {:?}",
        debug_names
    );
    assert!(
        debug_names.contains(&"project"),
        "expected project debug span; got debug spans: {:?}",
        debug_names
    );
}

// =============================================================================
// AC-2: Transaction span waterfall
// =============================================================================

#[tokio::test]
async fn ac2_insert_emits_staging_spans() {
    let (store, _guard) = init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis(&fluree, "it/tracing:ac2-insert");

    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:a", "ex:name": "A"},
            {"@id": "ex:b", "ex:name": "B"}
        ]
    });
    let _result = fluree.insert(ledger, &insert).await.expect("insert");

    // txn_stage (info) should be present
    assert!(
        store.has_span("txn_stage"),
        "expected txn_stage span; got: {:?}",
        store.span_names()
    );

    // For a simple insert: insert_gen and cancellation are always called
    assert!(
        store.has_span("insert_gen"),
        "expected insert_gen debug span; got: {:?}",
        store.span_names()
    );
    assert!(
        store.has_span("cancellation"),
        "expected cancellation debug span; got: {:?}",
        store.span_names()
    );

    // txn_commit (info) should be present
    assert!(
        store.has_span("txn_commit"),
        "expected txn_commit span; got: {:?}",
        store.span_names()
    );

    // Verify levels
    let stage = store.find_span("txn_stage").unwrap();
    assert_eq!(stage.level, tracing::Level::INFO);

    let insert_gen = store.find_span("insert_gen").unwrap();
    assert_eq!(insert_gen.level, tracing::Level::DEBUG);

    let cancel = store.find_span("cancellation").unwrap();
    assert_eq!(cancel.level, tracing::Level::DEBUG);
}

#[tokio::test]
async fn ac2_update_emits_full_staging_waterfall() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis(&fluree, "it/tracing:ac2-update");

    // First insert some data (with a throwaway tracing guard)
    let ledger = {
        let (_store, _guard) = init_test_tracing();
        let insert = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [
                {"@id": "ex:alice", "ex:name": "Alice", "ex:age": 42},
                {"@id": "ex:bob", "ex:name": "Bob", "ex:age": 22}
            ]
        });
        fluree.insert(ledger, &insert).await.expect("seed").ledger
    };

    // Fresh tracing for the update
    let (store, _guard) = init_test_tracing();

    // Update: WHERE + DELETE + INSERT exercises all sub-spans
    let update = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {"@id": "ex:alice", "ex:age": "?age"},
        "delete": {"@id": "ex:alice", "ex:age": "?age"},
        "insert": {"@id": "ex:alice", "ex:age": 43}
    });
    let _result = fluree.update(ledger, &update).await.expect("update");

    // Full staging waterfall
    assert!(store.has_span("txn_stage"), "expected txn_stage");
    assert!(
        store.has_span("where_exec"),
        "expected where_exec debug span"
    );
    assert!(
        store.has_span("delete_gen"),
        "expected delete_gen debug span"
    );
    assert!(
        store.has_span("insert_gen"),
        "expected insert_gen debug span"
    );
    assert!(
        store.has_span("cancellation"),
        "expected cancellation debug span"
    );
    assert!(store.has_span("txn_commit"), "expected txn_commit");

    // Verify debug levels
    let where_exec = store.find_span("where_exec").unwrap();
    assert_eq!(where_exec.level, tracing::Level::DEBUG);

    let delete_gen = store.find_span("delete_gen").unwrap();
    assert_eq!(delete_gen.level, tracing::Level::DEBUG);

    // Verify parent-child for staging sub-spans
    assert_eq!(
        where_exec.parent_name.as_deref(),
        Some("txn_stage"),
        "where_exec should be child of txn_stage"
    );
    assert_eq!(
        delete_gen.parent_name.as_deref(),
        Some("txn_stage"),
        "delete_gen should be child of txn_stage"
    );

    // Verify commit sub-spans exist
    assert!(
        store.has_span("commit_nameservice_lookup"),
        "expected commit_nameservice_lookup"
    );
}

// =============================================================================
// AC-5: Zero noise at INFO level
// =============================================================================

#[tokio::test]
async fn ac5_info_level_captures_no_debug_or_trace_spans() {
    let (store, _guard) = init_info_only_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis(&fluree, "it/tracing:ac5");

    // Insert
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:a", "ex:name": "A"}]
    });
    let ledger = fluree.insert(ledger, &insert).await.expect("insert").ledger;

    // Query
    let sparql = "PREFIX ex: <http://example.org/ns/> SELECT ?name WHERE { ?s ex:name ?name }";
    let _result = fluree.query_sparql(&ledger, sparql).await.expect("query");

    // No debug-level spans should be captured
    let debug = store.debug_spans();
    assert!(
        debug.is_empty(),
        "at INFO level, no debug spans should appear; got: {:?}",
        debug.iter().map(|s| s.name).collect::<Vec<_>>()
    );

    // No trace-level spans should be captured
    let trace = store.trace_spans();
    assert!(
        trace.is_empty(),
        "at INFO level, no trace spans should appear; got: {:?}",
        trace.iter().map(|s| s.name).collect::<Vec<_>>()
    );

    // Info spans SHOULD still be present
    assert!(
        store.has_span("txn_stage"),
        "info-level txn_stage should be captured"
    );
    assert!(
        store.has_span("query_run"),
        "info-level query_run should be captured"
    );
}
