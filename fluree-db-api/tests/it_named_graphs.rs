//! Named graph integration tests
//!
//! Tests the full pipeline from TriG/JSON-LD with named graphs → commit → indexing → query.
//!
//! These tests verify that:
//! - Named graphs are parsed correctly from TriG GRAPH blocks
//! - Graph IRIs are encoded in the commit's graph_delta field
//! - Indexed data is queryable via the #<graph-iri> fragment
//!
//! Named graphs use g_id 2+ (0 = default, 1 = txn-meta).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, LedgerManagerConfig, LedgerState, Novelty};
use fluree_db_core::Db;
use serde_json::json;
use support::start_background_indexer_local;

// =============================================================================
// TriG named graph parsing tests
// =============================================================================

#[tokio::test]
async fn test_trig_named_graph_basic() {
    // Insert TriG with a GRAPH block containing named graph data.
    // Verify that the data is stored in the named graph.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/named-graph-basic:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // TriG with a named graph block - use upsert_turtle which processes GRAPH blocks
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                # Default graph data
                ex:alice schema:name "Alice" .

                # Named graph data
                GRAPH <http://example.org/graphs/audit> {
                    ex:event1 schema:description "User login" .
                    ex:event1 ex:timestamp "2025-01-01T00:00:00Z" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing and wait
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query the default graph - should see Alice
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": alias,
                "select": ["?name"],
                "where": {"@id": "ex:alice", "schema:name": "?name"}
            });

            let results = fluree.query_connection(&query).await.expect("query default");
            let ledger = fluree.ledger(alias).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find Alice in default graph");
            assert_eq!(arr[0], "Alice");

            // Query the named graph via fragment - should see the event
            let named_graph_alias = format!("{}#http://example.org/graphs/audit", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &named_graph_alias,
                "select": ["?desc"],
                "where": {"@id": "ex:event1", "schema:description": "?desc"}
            });

            let results = fluree.query_connection(&query).await.expect("query named graph");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find event in named graph");
            assert_eq!(arr[0], "User login");
        })
        .await;
}

#[tokio::test]
async fn test_trig_multiple_named_graphs() {
    // Insert TriG with multiple GRAPH blocks.
    // Verify each graph is isolated and queryable.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/named-graph-multi:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // TriG with multiple named graphs
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/users> {
                    ex:alice schema:name "Alice" .
                    ex:bob schema:name "Bob" .
                }

                GRAPH <http://example.org/graphs/products> {
                    ex:prod1 schema:name "Widget" .
                    ex:prod1 ex:price 99 .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let ledger = fluree.ledger(alias).await.expect("load ledger");

            // Query users graph
            let users_alias = format!("{}#http://example.org/graphs/users", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &users_alias,
                "select": ["?s", "?name"],
                "where": {"@id": "?s", "schema:name": "?name"}
            });

            let results = fluree.query_connection(&query).await.expect("query users");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            // Should have 2 names (Alice, Bob)
            assert_eq!(arr.len(), 2, "should find 2 users: {:?}", arr);

            // Query products graph
            let products_alias = format!("{}#http://example.org/graphs/products", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &products_alias,
                "select": ["?name"],
                "where": {"@id": "ex:prod1", "schema:name": "?name"}
            });

            let results = fluree.query_connection(&query).await.expect("query products");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find product");
            assert_eq!(arr[0], "Widget");
        })
        .await;
}

#[tokio::test]
async fn test_unknown_named_graph_error() {
    // Attempting to query a non-existent named graph should error.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/named-graph-unknown:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Just insert some data
            let tx = json!({
                "@context": {"ex": "http://example.org/"},
                "insert": [{"@id": "ex:alice", "ex:name": "Alice"}]
            });
            let result = fluree.update(ledger, &tx).await.expect("update");

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Query a non-existent named graph - should error
            let unknown_alias = format!("{}#http://example.org/nonexistent", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &unknown_alias,
                "select": ["?s"],
                "where": {"@id": "?s", "ex:name": "?name"}
            });

            let result = fluree.query_connection(&query).await;
            assert!(result.is_err(), "should error on unknown named graph");
            let err_msg = format!("{}", result.unwrap_err());
            assert!(
                err_msg.contains("Unknown named graph"),
                "error should mention unknown graph: {}",
                err_msg
            );
        })
        .await;
}

#[tokio::test]
async fn test_default_graph_isolation() {
    // Data in named graphs should not appear in default graph queries.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/named-graph-isolation:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // TriG with data only in a named graph
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/private> {
                    ex:secret schema:value "confidential" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let ledger = fluree.ledger(alias).await.expect("load ledger");

            // Query default graph - should NOT find the secret
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": alias,
                "select": ["?val"],
                "where": {"@id": "ex:secret", "schema:value": "?val"}
            });

            let results = fluree.query_connection(&query).await.expect("query default");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "default graph should not contain named graph data: {:?}",
                arr
            );

            // Query named graph - should find the secret
            let private_alias = format!("{}#http://example.org/graphs/private", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &private_alias,
                "select": ["?val"],
                "where": {"@id": "ex:secret", "schema:value": "?val"}
            });

            let results = fluree.query_connection(&query).await.expect("query private");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find secret in named graph");
            assert_eq!(arr[0], "confidential");
        })
        .await;
}

#[tokio::test]
async fn test_txn_meta_and_named_graph_coexist() {
    // TriG can have both txn-meta GRAPH and user named graphs.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let alias = "it/named-graph-coexist:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = Db::genesis(fluree.storage().clone(), alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // TriG with txn-meta and a user named graph
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix fluree: <https://ns.flur.ee/ledger#> .

                # Default graph
                ex:alice schema:name "Alice" .

                # txn-meta graph
                GRAPH fluree:transactions {
                    fluree:commit:this ex:batchId "batch-123" .
                }

                # User named graph
                GRAPH <http://example.org/graphs/audit> {
                    ex:log1 ex:action "user created" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            let completion = handle.trigger(alias, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let ledger = fluree.ledger(alias).await.expect("load ledger");

            // Query default graph
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": alias,
                "select": ["?name"],
                "where": {"@id": "ex:alice", "schema:name": "?name"}
            });

            let results = fluree.query_connection(&query).await.expect("query default");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find Alice in default graph");
            assert_eq!(arr[0], "Alice");

            // Query txn-meta graph
            let meta_alias = format!("{}#txn-meta", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &meta_alias,
                "select": ["?batch"],
                "where": {"@id": "?commit", "ex:batchId": "?batch"}
            });

            let results = fluree.query_connection(&query).await.expect("query txn-meta");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find batch in txn-meta");
            assert_eq!(arr[0], "batch-123");

            // Query audit graph
            let audit_alias = format!("{}#http://example.org/graphs/audit", alias);
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &audit_alias,
                "select": ["?action"],
                "where": {"@id": "ex:log1", "ex:action": "?action"}
            });

            let results = fluree.query_connection(&query).await.expect("query audit");
            let results = results.to_jsonld(&ledger.db).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find action in audit graph");
            assert_eq!(arr[0], "user created");
        })
        .await;
}
