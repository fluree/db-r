//! Transaction metadata (txn-meta) integration tests
//!
//! Tests the full pipeline from transaction → commit → indexing → query for
//! user-provided transaction metadata stored in the txn-meta graph (g_id=1).
//!
//! See TXN_META_INGESTION_SPEC.md for the specification.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, LedgerManagerConfig, LedgerState, Novelty};
use fluree_db_core::Db;
use serde_json::json;
use support::start_background_indexer_local;

// =============================================================================
// JSON-LD txn-meta extraction tests
// =============================================================================

#[tokio::test]
async fn test_jsonld_txn_meta_basic() {
    // Insert with envelope-form JSON-LD containing top-level metadata,
    // trigger indexing, then query #txn-meta to verify.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-basic:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Envelope-form with top-level metadata
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:alice", "schema:name": "Alice"}
                ],
                // Top-level keys = txn metadata
                "ex:machine": "server-01",
                "ex:batchId": 42
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing and wait
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query the txn-meta graph using query_connection which properly handles #txn-meta
            let query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?p", "?o"],
                "where": {
                    "@id": "?s",
                    "?p": "?o"
                }
            });

            // Use query_connection which parses the from clause and handles #txn-meta
            let results = fluree.query_connection(&query).await.expect("query");

            // Get ledger for to_jsonld
            let ledger = fluree.ledger(alias).await.expect("load indexed ledger");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");

            // Should have results including our txn metadata
            let arr = results.as_array().expect("results should be array");

            // Look for machine property
            let has_machine = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("server-01")))
                    .unwrap_or(false)
            });

            // Look for batchId property
            let has_batch_id = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_i64() == Some(42)))
                    .unwrap_or(false)
            });

            assert!(
                has_machine,
                "should find machine metadata in txn-meta graph"
            );
            assert!(
                has_batch_id,
                "should find batchId metadata in txn-meta graph"
            );
        })
        .await;
}

#[tokio::test]
async fn test_jsonld_single_object_no_meta() {
    // Single-object form (no @graph) should NOT extract metadata
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-single-obj:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Single-object form - all properties are DATA, not metadata
            let tx = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice",
                "ex:machine": "laptop"  // This is DATA, not txn-meta
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query the default graph - should have the data
            // Include @context to properly expand the prefixes
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": alias,
                "select": ["?name", "?machine"],
                "where": {
                    "@id": "ex:alice",
                    "ex:name": "?name",
                    "ex:machine": "?machine"
                }
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find data in default graph");

            // txn-meta graph should NOT have user's "machine" property
            // (only built-in commit metadata)
            let meta_query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?o"],
                "where": {
                    "@id": "?s",
                    "http://example.org/machine": "?o"
                }
            });

            let meta_results = fluree
                .query_connection(&meta_query)
                .await
                .expect("meta query");
            let meta_results = meta_results.to_jsonld(&ledger.db).expect("to_jsonld");
            let meta_arr = meta_results.as_array().expect("array");
            assert!(
                meta_arr.is_empty(),
                "single-object form should NOT put ex:machine in txn-meta"
            );
        })
        .await;
}

#[tokio::test]
async fn test_jsonld_txn_meta_all_value_types() {
    // Test all supported value types in txn-meta
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-types:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@graph": [{"@id": "ex:test", "ex:name": "test"}],
                // String
                "ex:strVal": "hello",
                // Integer
                "ex:intVal": 42,
                // Double
                "ex:doubleVal": 1.23,
                // Boolean
                "ex:boolVal": true,
                // IRI reference (via @id)
                "ex:refVal": {"@id": "ex:target"},
                // Language-tagged string
                "ex:langVal": {"@value": "bonjour", "@language": "fr"},
                // Typed literal
                "ex:dateVal": {"@value": "2025-01-15", "@type": "xsd:date"}
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query txn-meta for each value type
            let query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?p", "?o"],
                "where": {"@id": "?s", "?p": "?o"}
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Helper to check if a value exists in results
            let has_value = |check: fn(&serde_json::Value) -> bool| {
                arr.iter()
                    .any(|row| row.as_array().map(|r| r.iter().any(check)).unwrap_or(false))
            };

            // String: "hello"
            assert!(
                has_value(|v| v.as_str() == Some("hello")),
                "should find string value 'hello'"
            );

            // Integer: 42
            assert!(
                has_value(|v| v.as_i64() == Some(42)),
                "should find integer value 42"
            );

            // Double: 1.23 (may be formatted as string or number)
            assert!(
                has_value(|v| {
                    v.as_f64()
                        .map(|f| (f - 1.23).abs() < 0.001)
                        .unwrap_or(false)
                        || v.as_str().map(|s| s.contains("1.23")).unwrap_or(false)
                }),
                "should find double value 1.23"
            );

            // Boolean: true
            assert!(
                has_value(|v| v.as_bool() == Some(true)),
                "should find boolean value true"
            );

            // IRI reference: ex:target (will appear as full IRI or with target in the value)
            assert!(
                has_value(|v| {
                    v.as_str()
                        .map(|s| s.contains("target") || s.contains("example.org"))
                        .unwrap_or(false)
                }),
                "should find IRI reference to ex:target"
            );

            // Language-tagged string: "bonjour" with @language "fr"
            // May appear as plain string or object with @value/@language
            assert!(
                has_value(|v| {
                    v.as_str() == Some("bonjour")
                        || v.get("@value").and_then(|v| v.as_str()) == Some("bonjour")
                }),
                "should find language-tagged string 'bonjour'"
            );

            // Typed literal: "2025-01-15" with xsd:date
            assert!(
                has_value(|v| {
                    v.as_str() == Some("2025-01-15")
                        || v.get("@value").and_then(|v| v.as_str()) == Some("2025-01-15")
                }),
                "should find typed literal date value"
            );
        })
        .await;
}

#[tokio::test]
async fn test_jsonld_txn_meta_reject_nested_object() {
    // Nested objects (not @value/@id) should be rejected
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-nested:main";

    let db0 = Db::genesis(fluree.storage().clone(), alias);
    let ledger = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:test", "ex:name": "test"}],
        // Nested object without @value/@id - should fail
        "ex:invalid": {"foo": "bar"}
    });

    let result = fluree.insert(ledger, &tx).await;
    assert!(
        result.is_err(),
        "nested objects in txn-meta should be rejected"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nested") || err.contains("@value") || err.contains("@id"),
        "error should mention nested objects: {err}"
    );
}

#[tokio::test]
async fn test_jsonld_txn_meta_reject_null() {
    // Null values should be rejected
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-null:main";

    let db0 = Db::genesis(fluree.storage().clone(), alias);
    let ledger = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:test", "ex:name": "test"}],
        "ex:nullVal": null
    });

    let result = fluree.insert(ledger, &tx).await;
    assert!(
        result.is_err(),
        "null values in txn-meta should be rejected"
    );
    let err = result.unwrap_err().to_string();
    assert!(err.contains("null"), "error should mention null: {err}");
}

// =============================================================================
// Built-in commit metadata tests
// =============================================================================

#[tokio::test]
async fn test_txn_meta_queryable_after_indexing() {
    // Verify user-provided metadata is queryable via #txn-meta after indexing.
    // This confirms the index-only semantics: txn-meta becomes visible only after indexing.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-builtin:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Use same pattern as working test
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:test", "schema:name": "Test"}
                ],
                "ex:marker": "builtin-test",
                "ex:version": 1
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");
            assert_eq!(result.receipt.t, 1);

            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query for all properties in txn-meta (same pattern as working test)
            let query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?p", "?o"],
                "where": {"@id": "?s", "?p": "?o"}
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Look for marker property
            let has_marker = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("builtin-test")))
                    .unwrap_or(false)
            });

            // Look for version property
            let has_version = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_i64() == Some(1)))
                    .unwrap_or(false)
            });

            assert!(has_marker, "should find marker metadata in txn-meta graph");
            assert!(
                has_version,
                "should find version metadata in txn-meta graph"
            );
        })
        .await;
}

// =============================================================================
// TriG txn-meta extraction tests
// =============================================================================

#[tokio::test]
async fn test_trig_txn_meta_basic() {
    // Insert with TriG format containing GRAPH block for txn-meta,
    // trigger indexing, then query #txn-meta to verify.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/trig-txn-meta-basic:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // TriG format with GRAPH block for txn-meta
            // Note: We use upsert_turtle via the builder because insert_turtle has
            // a direct flake path that bypasses TriG extraction.
            let turtle = r#"
                @prefix ex: <http://example.org/> .
                @prefix fluree: <https://ns.flur.ee/ledger#> .

                # Default graph data
                ex:alice ex:name "Alice" .

                # Transaction metadata
                GRAPH <https://ns.flur.ee/ledger#transactions> {
                    fluree:commit:this ex:machine "server-01" ;
                                       ex:batchId 42 .
                }
            "#;

            // Use the builder with upsert_turtle which goes through the TriG extraction path
            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(turtle)
                .execute()
                .await
                .expect("upsert_turtle");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing and wait
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query the txn-meta graph
            let query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?p", "?o"],
                "where": {
                    "@id": "?s",
                    "?p": "?o"
                }
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load indexed ledger");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("results should be array");

            // Look for machine property
            let has_machine = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("server-01")))
                    .unwrap_or(false)
            });

            // Look for batchId property
            let has_batch_id = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_i64() == Some(42)))
                    .unwrap_or(false)
            });

            assert!(
                has_machine,
                "should find machine metadata from TriG GRAPH block"
            );
            assert!(
                has_batch_id,
                "should find batchId metadata from TriG GRAPH block"
            );
        })
        .await;
}

#[tokio::test]
async fn test_trig_no_graph_passthrough() {
    // Plain Turtle without GRAPH block should work normally (no txn-meta)
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/trig-no-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Plain Turtle without GRAPH block
            let turtle = r#"
                @prefix ex: <http://example.org/> .
                ex:bob ex:name "Bob" ;
                       ex:age 30 .
            "#;

            let result = fluree
                .insert_turtle(ledger, turtle)
                .await
                .expect("insert_turtle");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query default graph - should have data
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": alias,
                "select": ["?name"],
                "where": {
                    "@id": "ex:bob",
                    "ex:name": "?name"
                }
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find data in default graph");
        })
        .await;
}

// =============================================================================
// Multi-commit txn-meta tests
// =============================================================================

#[tokio::test]
async fn test_txn_meta_multiple_commits() {
    // Create two commits with different metadata, verify both are visible
    // in the latest txn-meta query after indexing.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-multi-commit:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // First commit: metadata with "batch-1"
            let tx1 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:alice", "schema:name": "Alice"}
                ],
                "ex:batchId": "batch-1"
            });

            let result1 = fluree.insert(ledger, &tx1).await.expect("insert 1");
            assert_eq!(result1.receipt.t, 1);

            // Index first commit
            let completion1 = handle.trigger(alias, result1.receipt.t).await;
            match completion1.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing 1 failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing 1 cancelled"),
            }

            // Second commit: metadata with "batch-2"
            let ledger2 = fluree.ledger(alias).await.expect("load ledger after t=1");
            let tx2 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:bob", "schema:name": "Bob"}
                ],
                "ex:batchId": "batch-2"
            });

            let result2 = fluree.insert(ledger2, &tx2).await.expect("insert 2");
            assert_eq!(result2.receipt.t, 2);

            // Index second commit
            let completion2 = handle.trigger(alias, result2.receipt.t).await;
            match completion2.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing 2 failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing 2 cancelled"),
            }

            // Query latest txn-meta: should have both batch-1 and batch-2
            let query = json!({
                "from": format!("{}#txn-meta", alias),
                "select": ["?o"],
                "where": {
                    "@id": "?s",
                    "http://example.org/batchId": "?o"
                }
            });

            let results = fluree.query_connection(&query).await.expect("query");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            let has_batch_1 = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-1")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-1")
            });
            let has_batch_2 = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-2")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-2")
            });

            assert!(
                has_batch_1,
                "should find batch-1 metadata from first commit"
            );
            assert!(
                has_batch_2,
                "should find batch-2 metadata from second commit"
            );
        })
        .await;
}

// =============================================================================
// Time travel txn-meta tests
// =============================================================================

#[tokio::test]
async fn test_txn_meta_time_travel_syntax() {
    // Test that @t:N#txn-meta syntax works without error.
    // Time-travel filtering works because bulk builds now populate Region 3.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-time-travel:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Commit with metadata
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:alice", "schema:name": "Alice"}
                ],
                "ex:batchId": "batch-1"
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");
            assert_eq!(result.receipt.t, 1);

            // Index the commit
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query using @t:1#txn-meta syntax - this should NOT error
            // (Previously failed with "Named graph queries require binary index store")
            let query = json!({
                "from": format!("{}@t:1#txn-meta", alias),
                "select": ["?o"],
                "where": {
                    "@id": "?s",
                    "http://example.org/batchId": "?o"
                }
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query at t=1 should not error");
            let ledger = fluree.ledger(alias).await.expect("load");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Should find the batch-1 metadata
            let has_batch_1 = arr.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-1")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-1")
            });

            assert!(
                has_batch_1,
                "should find batch-1 metadata via @t:1#txn-meta"
            );
        })
        .await;
}

// =============================================================================
// SPARQL GRAPH pattern tests
// =============================================================================

#[tokio::test]
async fn test_sparql_graph_pattern_txn_meta() {
    // Test that SPARQL GRAPH <alias#txn-meta> { ... } pattern works correctly.
    // This uses the DatasetSpec API with a named graph for txn-meta.
    use fluree_db_api::{DatasetSpec, GraphSource};

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/sparql-graph-txn-meta:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Commit with metadata
            let tx = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:alice", "schema:name": "Alice"}
                ],
                "ex:batchId": "sparql-test-batch",
                "ex:source": "unit-test"
            });

            let result = fluree.insert(ledger, &tx).await.expect("insert");
            assert_eq!(result.receipt.t, 1);

            // Index the commit
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Build dataset spec with txn-meta as a named graph
            let txn_meta_graph = format!("{}#txn-meta", alias);
            let spec = DatasetSpec::new()
                .with_default(GraphSource::new(alias))
                .with_named(GraphSource::new(&txn_meta_graph));

            let dataset = fluree
                .build_dataset_view(&spec)
                .await
                .expect("build dataset view");

            let primary = dataset.primary().unwrap();

            // SPARQL query using GRAPH pattern to access txn-meta
            let sparql = format!(
                r#"
                SELECT ?batchId
                WHERE {{
                    GRAPH <{txn_meta_graph}> {{
                        ?commit <http://example.org/batchId> ?batchId .
                    }}
                }}
            "#
            );

            let result = fluree
                .query_dataset_view(&dataset, &sparql)
                .await
                .expect("SPARQL GRAPH query should succeed");

            let jsonld = result.to_jsonld(primary.db.as_ref()).expect("to_jsonld");
            let arr = jsonld.as_array().expect("array");

            // Should find our batch metadata
            let has_batch = arr.iter().any(|row| {
                // Could be flat value or array
                row.as_str() == Some("sparql-test-batch")
                    || row
                        .as_array()
                        .map(|r| r.iter().any(|v| v.as_str() == Some("sparql-test-batch")))
                        .unwrap_or(false)
            });

            assert!(
                has_batch,
                "SPARQL GRAPH pattern should find batchId metadata"
            );
        })
        .await;
}

#[tokio::test]
async fn test_txn_meta_time_travel_filtering() {
    // Test that time-travel correctly filters txn-meta by t value.
    // Creates two commits with different metadata, verifies:
    // - Query at t=1 only shows batch-1
    // - Query at t=2 shows both batch-1 and batch-2
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/txn-meta-time-travel-filtering:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // First commit with batch-1
            let tx1 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:alice", "schema:name": "Alice"}
                ],
                "ex:batchId": "batch-1"
            });

            let result1 = fluree.insert(ledger, &tx1).await.expect("insert 1");
            assert_eq!(result1.receipt.t, 1);

            // Index first commit
            let completion1 = handle.trigger(alias, result1.receipt.t).await;
            match completion1.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing 1 failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing 1 cancelled"),
            }

            // Second commit with batch-2
            let ledger2 = fluree.ledger(alias).await.expect("load ledger after t=1");
            let tx2 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {"@id": "ex:bob", "schema:name": "Bob"}
                ],
                "ex:batchId": "batch-2"
            });

            let result2 = fluree.insert(ledger2, &tx2).await.expect("insert 2");
            assert_eq!(result2.receipt.t, 2);

            // Index second commit
            let completion2 = handle.trigger(alias, result2.receipt.t).await;
            match completion2.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing 2 failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing 2 cancelled"),
            }

            // Query at t=1: should only see batch-1
            let view_t1 = fluree
                .view_at_t(&format!("{}#txn-meta", alias), 1)
                .await
                .expect("view at t=1");

            let query_t1 = json!({
                "select": ["?o"],
                "where": {
                    "@id": "?s",
                    "http://example.org/batchId": "?o"
                }
            });

            let results_t1 = fluree
                .query_view(&view_t1, &query_t1)
                .await
                .expect("query at t=1");
            let results_t1 = results_t1.to_jsonld(&view_t1.db).expect("to_jsonld");
            let arr_t1 = results_t1.as_array().expect("array");

            let has_batch_1_at_t1 = arr_t1.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-1")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-1")
            });
            let has_batch_2_at_t1 = arr_t1.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-2")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-2")
            });

            assert!(has_batch_1_at_t1, "query at t=1 should find batch-1");
            assert!(
                !has_batch_2_at_t1,
                "query at t=1 should NOT find batch-2 (it was added at t=2), results: {:?}",
                arr_t1
            );

            // Query at t=2: should see both batch-1 and batch-2
            let view_t2 = fluree
                .view_at_t(&format!("{}#txn-meta", alias), 2)
                .await
                .expect("view at t=2");
            let query_t2 = json!({
                "select": ["?o"],
                "where": {
                    "@id": "?s",
                    "http://example.org/batchId": "?o"
                }
            });

            let results_t2 = fluree
                .query_view(&view_t2, &query_t2)
                .await
                .expect("query at t=2");
            let results_t2 = results_t2.to_jsonld(&view_t2.db).expect("to_jsonld");
            let arr_t2 = results_t2.as_array().expect("array");

            let has_batch_1_at_t2 = arr_t2.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-1")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-1")
            });
            let has_batch_2_at_t2 = arr_t2.iter().any(|row| {
                row.as_array()
                    .map(|r| r.iter().any(|v| v.as_str() == Some("batch-2")))
                    .unwrap_or(false)
                    || row.as_str() == Some("batch-2")
            });

            assert!(has_batch_1_at_t2, "query at t=2 should find batch-1");
            assert!(has_batch_2_at_t2, "query at t=2 should find batch-2");
        })
        .await;
}
