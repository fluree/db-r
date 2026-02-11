//! Indexing statistics integration tests (Clojure parity)
//!
//! Mirrors `db-clojure/test/fluree/db/property_class_statistics_test.clj`.
//!
//! Includes tests for:
//! - Property and class statistics persistence
//! - Statistics decrement after delete
//! - Memory storage statistics
//! - `ledger_info` API (basic structure and context compaction)

#![cfg(feature = "native")]

mod support;

use std::sync::Arc;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState};
use fluree_db_core::serde::json::{
    raw_schema_to_index_schema, raw_stats_to_index_stats, RawDbRootSchema, RawDbRootStats,
};
use fluree_db_core::{load_db, Db, DbMetadata, DictNovelty, Storage};
use fluree_db_indexer::run_index::{BinaryIndexRoot, BinaryIndexStore};
use fluree_db_query::BinaryRangeProvider;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value as JsonValue};
use support::{
    genesis_ledger_for_fluree, start_background_indexer_local, trigger_index_and_wait_outcome,
};

/// Apply a v2 binary index root to a ledger, loading the full BinaryIndexStore
/// and attaching a BinaryRangeProvider so subsequent queries work correctly.
async fn apply_index_v2<S: Storage + Clone + 'static>(
    ledger: &mut LedgerState,
    root_id: &fluree_db_core::ContentId,
    ledger_id: &str,
    storage: &S,
    cache_dir: &std::path::Path,
) {
    let root_address = fluree_db_core::storage::content_address(
        storage.storage_method(),
        fluree_db_core::ContentKind::IndexRoot,
        ledger_id,
        &root_id.digest_hex(),
    );
    let bytes = storage
        .read_bytes(&root_address)
        .await
        .expect("read index root");
    let root: BinaryIndexRoot = serde_json::from_slice(&bytes).expect("parse v2 root");

    let cs = fluree_db_core::content_store_for(storage.clone(), &root.ledger_id);
    let store = BinaryIndexStore::load_from_root(&cs, &root, cache_dir, None)
        .await
        .expect("load binary index");
    let arc_store = Arc::new(store);
    let dn = Arc::new(DictNovelty::new_uninitialized());
    let provider = BinaryRangeProvider::new(Arc::clone(&arc_store), dn, 0);

    let ns_codes = root.namespace_codes.into_iter().collect();
    let stats = root
        .stats
        .as_ref()
        .and_then(|s| serde_json::from_value::<RawDbRootStats>(s.clone()).ok())
        .and_then(|raw| raw_stats_to_index_stats(&raw));
    let schema = root
        .schema
        .as_ref()
        .and_then(|s| serde_json::from_value::<RawDbRootSchema>(s.clone()).ok())
        .map(|raw| raw_schema_to_index_schema(&raw));
    let meta = DbMetadata {
        ledger_id: root.ledger_id,
        t: root.index_t,
        namespace_codes: ns_codes,
        stats,
        schema,
        subject_watermarks: root.subject_watermarks,
        string_watermark: root.string_watermark,
    };
    let mut db = Db::new_meta(meta);
    db.range_provider = Some(Arc::new(provider));

    ledger
        .apply_loaded_db(db, Some(root_id))
        .expect("apply_loaded_db");
}

fn property_count(db: &Db, iri: &str) -> Option<u64> {
    let stats = db.stats.as_ref()?;
    let props = stats.properties.as_ref()?;
    for p in props {
        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
        if let Some(full) = db.decode_sid(&sid) {
            if full == iri {
                return Some(p.count);
            }
        }
    }
    None
}

fn class_count(db: &Db, iri: &str) -> Option<u64> {
    let stats = db.stats.as_ref()?;
    let classes = stats.classes.as_ref()?;
    for c in classes {
        if let Some(full) = db.decode_sid(&c.class_sid) {
            if full == iri {
                return Some(c.count);
            }
        }
    }
    None
}

#[tokio::test]
async fn property_and_class_statistics_persist_in_db_root() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id ="it/indexing-stats:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let txn1 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:age":30,"ex:email":"alice@example.com"},
                    {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob","ex:age":25,"ex:email":"bob@example.com"},
                    {"@id":"ex:acme","@type":"ex:Organization","ex:name":"Acme Corp","ex:founded":1990}
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            let commit_t = result.receipt.t;
            let outcome =
                trigger_index_and_wait_outcome(&handle, result.ledger.ledger_id(), commit_t).await;
            let fluree_db_api::IndexOutcome::Completed {
                index_t,
                root_id,
            } = outcome
            else {
                unreachable!("helper only returns Completed")
            };

            assert!(index_t >= commit_t);
            let root_cid = root_id.expect("expected root_id after indexing");

            let loaded = load_db(fluree.storage(), &root_cid, ledger_id)
            .await
            .expect("load_db(root_cid)");

            let loaded_stats = loaded.stats.as_ref().expect("db.stats should be Some after indexing");
            assert!(loaded_stats.properties.is_some(), "expected db.stats.properties");
            assert!(loaded_stats.classes.is_some(), "expected db.stats.classes");

            assert_eq!(property_count(&loaded, "http://example.org/name"), Some(3));
            assert_eq!(property_count(&loaded, "http://example.org/age"), Some(2));
            assert_eq!(property_count(&loaded, "http://example.org/email"), Some(2));
            assert_eq!(property_count(&loaded, "http://example.org/founded"), Some(1));

            assert_eq!(class_count(&loaded, "http://example.org/Person"), Some(2));
            assert_eq!(
                class_count(&loaded, "http://example.org/Organization"),
                Some(1)
            );

        })
        .await;
}

#[tokio::test]
async fn class_statistics_decrement_after_delete_refresh() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/indexing-stats-retracts:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let txn1 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice"},
                    {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"},
                    {"@id":"ex:carol","@type":"ex:Person","ex:name":"Carol"}
                ]
            });
            let r1 = fluree
                .insert_with_opts(
                    ledger0,
                    &txn1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert txn1");

            let _ =
                trigger_index_and_wait_outcome(&handle, r1.ledger.ledger_id(), r1.receipt.t).await;

            let del = json!({
                "@context": { "ex": "http://example.org/" },
                "where": {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"},
                "delete": {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"}
            });
            let r2 = fluree
                .update_with_opts(
                    r1.ledger,
                    &del,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("delete bob");

            let outcome =
                trigger_index_and_wait_outcome(&handle, r2.ledger.ledger_id(), r2.receipt.t).await;
            let fluree_db_api::IndexOutcome::Completed { root_id, .. } = outcome else {
                unreachable!("helper only returns Completed")
            };
            let root_cid = root_id.expect("expected root_id");

            let loaded2 = load_db(fluree.storage(), &root_cid, ledger_id)
                .await
                .expect("load_db(root_cid)");
            assert_eq!(class_count(&loaded2, "http://example.org/Person"), Some(2));
        })
        .await;
}

#[tokio::test]
async fn statistics_work_with_memory_storage_when_indexed() {
    // Clojure: `property-class-statistics-memory-storage-test`
    let mut fluree = FlureeBuilder::memory().build_memory();

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/indexing-stats-memory:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:age":30},
                    {"@id":"ex:product1","@type":"ex:Product","ex:name":"Widget","ex:price":19.99}
                ]
            });
            let r = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let outcome =
                trigger_index_and_wait_outcome(&handle, r.ledger.ledger_id(), r.receipt.t).await;
            let fluree_db_api::IndexOutcome::Completed { root_id, .. } = outcome else {
                unreachable!("helper only returns Completed")
            };
            let root_cid = root_id.expect("expected root_id");

            let loaded = load_db(fluree.storage(), &root_cid, ledger_id)
                .await
                .expect("load_db(root_cid)");

            assert_eq!(property_count(&loaded, "http://example.org/name"), Some(2));
            assert_eq!(property_count(&loaded, "http://example.org/age"), Some(1));
            assert_eq!(property_count(&loaded, "http://example.org/price"), Some(1));
            assert_eq!(class_count(&loaded, "http://example.org/Person"), Some(1));
            assert_eq!(class_count(&loaded, "http://example.org/Product"), Some(1));
        })
        .await;
}

// ============================================================================
// ledger-info API tests
// Clojure: `ledger-info-api-test` and `ledger-info-api-with-context-test`
// ============================================================================

/// Helper to check that a JSON value at a path exists and is not null
fn json_path_exists(value: &JsonValue, path: &[&str]) -> bool {
    let mut current = value;
    for key in path {
        match current.get(*key) {
            Some(v) if !v.is_null() => current = v,
            _ => return false,
        }
    }
    true
}

#[tokio::test]
async fn ledger_info_api_returns_expected_structure() {
    // Clojure: `ledger-info-api-test`
    // Tests that ledger_info returns a response with all expected top-level keys
    // and proper nested structures after indexing.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id ="test/ledger-info:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Insert test data (like Clojure test), including a ref property so we can
            // validate class→property ref target stats in ledger_info.
            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id": "ex:acme", "@type": "ex:Organization", "ex:name": "Acme"},
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:worksFor": {"@id":"ex:acme"}},
                    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:worksFor": {"@id":"ex:acme"}}
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            // Trigger indexing and wait for completion
            let _ =
                trigger_index_and_wait_outcome(&handle, result.ledger.ledger_id(), result.receipt.t)
                    .await;

            // Call ledger_info without context
            let info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info");

            // ================================================================
            // Verify top-level keys exist
            // ================================================================
            assert!(info.get("commit").is_some(), "should have 'commit' key");
            assert!(
                info.get("nameservice").is_some(),
                "should have 'nameservice' key"
            );
            assert!(info.get("stats").is_some(), "should have 'stats' key");
            assert!(info.get("index").is_some(), "should have 'index' key");

            // ================================================================
            // Verify commit structure (Clojure parity)
            // ================================================================
            let commit = &info["commit"];
            assert_eq!(commit["@context"], "https://ns.flur.ee/db/v1");
            assert!(
                commit["type"]
                    .as_array()
                    .map(|a| a.contains(&json!("Commit")))
                    .unwrap_or(false),
                "commit.type should be ['Commit']"
            );
            assert!(
                json_path_exists(commit, &["id"]),
                "commit should have id"
            );
            assert!(
                json_path_exists(commit, &["ledger_id"]),
                "commit should have ledger_id"
            );
            assert!(
                json_path_exists(commit, &["data"]),
                "commit should have data"
            );
            assert!(
                json_path_exists(commit, &["data", "t"]),
                "commit.data should have t"
            );
            assert!(
                json_path_exists(commit, &["data", "type"]),
                "commit.data should have type"
            );
            assert!(
                commit["data"]["type"]
                    .as_array()
                    .map(|a| a.contains(&json!("DB")))
                    .unwrap_or(false),
                "commit.data.type should be ['DB']"
            );

            // ================================================================
            // Verify nameservice structure (uses @id, @type, @context keywords)
            // ================================================================
            let ns = &info["nameservice"];
            assert!(
                ns.get("@context").is_some(),
                "nameservice should have @context"
            );
            assert!(ns.get("@id").is_some(), "nameservice should have @id");
            assert!(ns.get("@type").is_some(), "nameservice should have @type");
            assert!(ns.get("f:t").is_some(), "nameservice should have f:t");
            assert_eq!(
                ns["f:status"], "ready",
                "nameservice f:status should be 'ready'"
            );

            // ================================================================
            // Verify stats structure
            // ================================================================
            let stats = &info["stats"];
            assert!(stats.get("flakes").is_some(), "stats should have flakes");
            assert!(stats.get("size").is_some(), "stats should have size");
            assert!(stats.get("indexed").is_some(), "stats should have indexed");
            assert!(
                stats.get("properties").is_some(),
                "stats should have properties"
            );
            assert!(stats.get("classes").is_some(), "stats should have classes");

            // Verify stats.indexed is the index t (integer), not boolean
            assert!(
                stats["indexed"].is_i64(),
                "stats.indexed should be integer (index t)"
            );

            // Verify property stats have expected fields
            let props = &stats["properties"];
            assert!(props.is_object(), "stats.properties should be object");

            // Check ex:name property exists with expected stats fields
            if let Some(name_stats) = props.get("http://example.org/name") {
                assert!(
                    name_stats.get("count").is_some(),
                    "property should have count"
                );
                assert!(
                    name_stats.get("ndv-values").is_some(),
                    "property should have ndv-values"
                );
                assert!(
                    name_stats.get("ndv-subjects").is_some(),
                    "property should have ndv-subjects"
                );
                assert!(
                    name_stats.get("last-modified-t").is_some(),
                    "property should have last-modified-t"
                );
                assert!(
                    name_stats.get("selectivity-value").is_some(),
                    "property should have selectivity-value"
                );
                assert!(
                    name_stats.get("selectivity-subject").is_some(),
                    "property should have selectivity-subject"
                );

                // Verify selectivity is integer
                assert!(
                    name_stats["selectivity-value"].is_i64(),
                    "selectivity-value should be integer"
                );

                // ex:name has count 3 (Alice, Bob, and Acme)
                assert_eq!(name_stats["count"], 3, "ex:name count should be 3");
            } else {
                panic!("stats.properties should contain http://example.org/name");
            }

            // Verify class stats have expected fields
            let classes = &stats["classes"];
            assert!(classes.is_object(), "stats.classes should be object");

            // Check ex:Person class exists
            if let Some(person_stats) = classes.get("http://example.org/Person") {
                assert!(
                    person_stats.get("count").is_some(),
                    "class should have count"
                );
                assert!(
                    person_stats.get("properties").is_some(),
                    "class should have properties"
                );
                assert_eq!(person_stats["count"], 2, "ex:Person count should be 2");

                // Class properties are a map keyed by property IRI.
                if let Some(class_props) = person_stats.get("properties") {
                    assert!(class_props.is_object(), "class properties should be an object map");
                    assert!(
                        class_props.get("http://example.org/name").is_some(),
                        "Person should have ex:name property"
                    );
                    // Validate ref stats for worksFor: Person -> Organization count 2
                    let works_for = class_props
                        .get("http://example.org/worksFor")
                        .expect("Person should have ex:worksFor property");
                    let refs = works_for
                        .get("refs")
                        .or_else(|| works_for.get("ref-classes"))
                        .and_then(|v| v.as_object())
                        .expect("worksFor should have refs/ref-classes map");
                    assert_eq!(
                        refs.get("http://example.org/Organization"),
                        Some(&json!(2)),
                        "worksFor should point to Organization twice"
                    );
                }
            } else {
                panic!("stats.classes should contain http://example.org/Person");
            }

            // ================================================================
            // Verify index section
            // ================================================================
            let index = &info["index"];
            assert!(index.get("t").is_some(), "index should have t");
            assert!(index.get("id").is_some(), "index should have id");
            // index.id is optional but should be present if commit has index.id
        })
        .await;
}

#[tokio::test]
async fn ledger_info_api_with_context_compacts_stats_iris() {
    // Clojure: `ledger-info-api-with-context-test`
    // Tests that ledger_info with context returns compacted IRIs in stats

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id ="test/ledger-info-ctx:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Insert test data
            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            // Trigger indexing
            let _ = trigger_index_and_wait_outcome(
                &handle,
                result.ledger.ledger_id(),
                result.receipt.t,
            )
            .await;

            // Define context for compaction
            let context = json!({
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            });

            // Call ledger_info WITH context
            let info = fluree.ledger_info(ledger_id)
                .with_context(&context)
                .execute()
                .await
                .expect("ledger_info");

            // ================================================================
            // Verify stats IRIs are compacted
            // ================================================================
            let stats = &info["stats"];
            let props = &stats["properties"];

            // With context, "http://example.org/name" should be compacted to "ex:name"
            assert!(props.get("ex:name").is_some(),
                "With context, http://example.org/name should be compacted to ex:name. Got keys: {:?}",
                props.as_object().map(|o| o.keys().collect::<Vec<_>>()));

            // Verify the compacted property has all expected fields
            if let Some(name_stats) = props.get("ex:name") {
                assert_eq!(name_stats["count"], 2, "ex:name count should be 2");
                assert!(name_stats.get("selectivity-value").is_some(),
                    "compacted property should still have selectivity-value");
            }

            // Verify class IRIs are compacted
            let classes = &stats["classes"];
            assert!(classes.get("ex:Person").is_some(),
                "With context, http://example.org/Person should be compacted to ex:Person. Got keys: {:?}",
                classes.as_object().map(|o| o.keys().collect::<Vec<_>>()));

            // Verify class properties are also compacted
            if let Some(person_stats) = classes.get("ex:Person") {
                if let Some(class_props) = person_stats.get("properties") {
                    assert!(
                        class_props.get("ex:name").is_some(),
                        "Class property key should be compacted to ex:name"
                    );
                    // property-list should also be present (array) for convenience.
                    if let Some(list) = person_stats.get("property-list") {
                        let arr = list
                            .as_array()
                            .expect("property-list should be an array of compacted property IRIs");
                        assert!(
                            arr.iter().any(|v| v.as_str() == Some("ex:name")),
                            "property-list should include ex:name"
                        );
                    }
                }
            }

            // ================================================================
            // Verify commit and nameservice are NOT compacted
            // (they use full IRIs regardless of context)
            // ================================================================
            let commit = &info["commit"];
            // commit.ledger_id should still be full "test/ledger-info-ctx:main" not compacted
            assert_eq!(commit["ledger_id"], "test/ledger-info-ctx:main");

            // nameservice @id should still be full
            let ns = &info["nameservice"];
            assert!(ns["@id"].as_str().map(|s| s.contains("ledger-info-ctx")).unwrap_or(false),
                "nameservice @id should not be compacted");
        })
        .await;
}

#[tokio::test]
async fn ledger_info_before_commit_returns_null_commit() {
    // Tests that ledger_info returns commit: null when there's no commit yet
    // (Clojure parity: commit key is always present)

    let fluree = FlureeBuilder::memory().build_memory();

    let ledger_id = "test/ledger-info-genesis:main";
    let _ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create_ledger");

    // Call ledger_info on genesis ledger (no commits)
    let info = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info");

    // commit key should exist but be null
    assert!(info.get("commit").is_some(), "should have 'commit' key");
    assert!(
        info["commit"].is_null(),
        "commit should be null when no commits"
    );

    // Other keys should still be present
    assert!(info.get("stats").is_some(), "should have stats");
}

#[tokio::test]
async fn ledger_info_property_datatypes_option_merges_novelty() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id ="test/ledger-info-datatypes:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // 1) Seed and index a float-valued property.
            let txn1 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id": "ex:prod1", "@type": "ex:Product", "ex:price": 1.25}
                ]
            });
            let result1 = fluree
                .insert_with_opts(
                    ledger0,
                    &txn1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert txn1");
            let _ = trigger_index_and_wait_outcome(&handle, result1.ledger.ledger_id(), result1.receipt.t)
                .await;

            // 2) Add an integer-valued price in novelty (do NOT index).
            let ledger1 = fluree.ledger(ledger_id).await.expect("reload ledger after indexing");
            let txn2 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id": "ex:prod2", "@type": "ex:Product", "ex:price": 3}
                ]
            });
            let _result2 = fluree
                .insert(ledger1, &txn2)
                .await
                .expect("insert txn2 (novelty)");

            // 3) Indexed view: include datatypes but do not merge novelty deltas.
            let indexed_info = fluree
                .ledger_info(ledger_id)
                .with_property_datatypes(true)
                .execute()
                .await
                .expect("ledger_info indexed view");
            let indexed_dts = indexed_info["stats"]["properties"]["http://example.org/price"]["datatypes"]
                .as_object()
                .expect("datatypes should be an object map");
            assert!(
                indexed_dts.contains_key("xsd:double") || indexed_dts.contains_key("xsd:float"),
                "expected indexed ex:price to have float datatypes; got keys: {:?}",
                indexed_dts.keys().collect::<Vec<_>>()
            );
            assert!(
                !(indexed_dts.contains_key("xsd:integer")
                    || indexed_dts.contains_key("xsd:long")
                    || indexed_dts.contains_key("xsd:int")
                    || indexed_dts.contains_key("xsd:short")
                    || indexed_dts.contains_key("xsd:byte")),
                "expected indexed ex:price to NOT include integer-like datatypes before novelty merge; got keys: {:?}",
                indexed_dts.keys().collect::<Vec<_>>()
            );

            // 4) Real-time view: merge novelty datatype deltas.
            let realtime_info = fluree
                .ledger_info(ledger_id)
                .with_realtime_property_details(true)
                .execute()
                .await
                .expect("ledger_info realtime property details");
            let realtime_dts = realtime_info["stats"]["properties"]["http://example.org/price"]["datatypes"]
                .as_object()
                .expect("datatypes should be an object map");
            assert!(
                realtime_dts.contains_key("xsd:double") || realtime_dts.contains_key("xsd:float"),
                "expected realtime ex:price to keep float datatypes; got keys: {:?}",
                realtime_dts.keys().collect::<Vec<_>>()
            );
            assert!(
                realtime_dts.contains_key("xsd:integer")
                    || realtime_dts.contains_key("xsd:long")
                    || realtime_dts.contains_key("xsd:int")
                    || realtime_dts.contains_key("xsd:short")
                    || realtime_dts.contains_key("xsd:byte"),
                "expected realtime ex:price to include integer-like datatype after novelty merge; got keys: {:?}",
                realtime_dts.keys().collect::<Vec<_>>()
            );
        })
        .await;
}

#[tokio::test]
async fn ledger_info_realtime_edges_merge_novelty_ref_counts() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/ledger-info-edges:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // 1) Seed and index a single Person -> Organization edge.
            let txn1 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:acme","@type":"ex:Organization","ex:name":"Acme"},
                    {"@id":"ex:alice","@type":"ex:Person","ex:worksFor":{"@id":"ex:acme"}}
                ]
            });
            let result1 = fluree
                .insert_with_opts(
                    ledger0,
                    &txn1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert txn1");
            let _ = trigger_index_and_wait_outcome(
                &handle,
                result1.ledger.ledger_id(),
                result1.receipt.t,
            )
            .await;

            // 2) Add a second edge in novelty (do NOT index).
            let ledger1 = fluree
                .ledger(ledger_id)
                .await
                .expect("reload ledger after indexing");
            let txn2 = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id":"ex:bob","@type":"ex:Person","ex:worksFor":{"@id":"ex:acme"}}
                ]
            });
            let _result2 = fluree
                .insert(ledger1, &txn2)
                .await
                .expect("insert txn2 (novelty)");

            // 3) Base payload: edges are as-of last index (should still be 1).
            let base_info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info base");

            let base_refs = base_info["stats"]["classes"]["http://example.org/Person"]
                ["properties"]["http://example.org/worksFor"]["refs"]
                .as_object()
                .expect("expected refs map in base payload");
            assert_eq!(
                base_refs.get("http://example.org/Organization"),
                Some(&json!(1)),
                "base payload should report indexed edge count only"
            );

            // 4) Real-time edges: merge novelty ref deltas (should be 2).
            let rt_info = fluree
                .ledger_info(ledger_id)
                .with_realtime_property_details(true)
                .execute()
                .await
                .expect("ledger_info realtime edges");

            let rt_refs = rt_info["stats"]["classes"]["http://example.org/Person"]["properties"]
                ["http://example.org/worksFor"]["refs"]
                .as_object()
                .expect("expected refs map in realtime payload");
            assert_eq!(
                rt_refs.get("http://example.org/Organization"),
                Some(&json!(2)),
                "realtime payload should include novelty edge count"
            );
        })
        .await;
}

// ============================================================================
// Additional statistics parity tests
// ============================================================================

#[tokio::test]
async fn ndv_cardinality_estimates_are_accurate() {
    // Tests that HLL cardinality estimates (ndv-values, ndv-subjects) are accurate
    // HLL at precision=8 has ~6.5% standard error, so we allow 10% tolerance for safety.
    // Clojure parity: ndv (number of distinct values) statistics.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/ndv-accuracy:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Create 20 people with:
            // - 20 distinct names (ndv_values = 20, ndv_subjects = 20)
            // - 5 distinct status values shared across 20 people (ndv_values = 5, ndv_subjects = 20)
            let mut graph = vec![];
            let statuses = ["active", "pending", "inactive", "archived", "deleted"];
            for i in 0..20 {
                graph.push(json!({
                    "@id": format!("ex:person{i}"),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {i}"),
                    "ex:status": statuses[i % 5]
                }));
            }

            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": graph
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let outcome = trigger_index_and_wait_outcome(
                &handle,
                result.ledger.ledger_id(),
                result.receipt.t,
            )
            .await;
            let fluree_db_api::IndexOutcome::Completed { root_id, .. } = outcome else {
                unreachable!("helper only returns Completed")
            };
            let root_cid = root_id.expect("expected root_id");

            let loaded = load_db(fluree.storage(), &root_cid, ledger_id)
                .await
                .expect("load_db(root_cid)");

            // Check ex:name: 20 distinct values, 20 distinct subjects
            let name_ndv_values = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/name")
                            .unwrap_or(false)
                    })
                })
                .map(|p| p.ndv_values)
                .expect("name property should exist");

            let name_ndv_subjects = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/name")
                            .unwrap_or(false)
                    })
                })
                .map(|p| p.ndv_subjects)
                .expect("name property should exist");

            // Allow 10% error margin for HLL estimates
            assert!(
                (18..=22).contains(&name_ndv_values),
                "ex:name ndv_values should be ~20, got {name_ndv_values}"
            );
            assert!(
                (18..=22).contains(&name_ndv_subjects),
                "ex:name ndv_subjects should be ~20, got {name_ndv_subjects}"
            );

            // Check ex:status: 5 distinct values, 20 distinct subjects
            let status_ndv_values = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/status")
                            .unwrap_or(false)
                    })
                })
                .map(|p| p.ndv_values)
                .expect("status property should exist");

            let status_ndv_subjects = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/status")
                            .unwrap_or(false)
                    })
                })
                .map(|p| p.ndv_subjects)
                .expect("status property should exist");

            // ex:status has 5 distinct values but 20 subjects
            assert!(
                (4..=6).contains(&status_ndv_values),
                "ex:status ndv_values should be ~5, got {status_ndv_values}"
            );
            assert!(
                (18..=22).contains(&status_ndv_subjects),
                "ex:status ndv_subjects should be ~20, got {status_ndv_subjects}"
            );
        })
        .await;
}

#[tokio::test]
async fn selectivity_calculation_is_correct() {
    // Tests that selectivity is computed as ceil(count / ndv), minimum 1.
    // Clojure parity: selectivity statistics for query planning.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/selectivity:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Create data where:
            // - ex:uniqueId: 10 flakes, 10 distinct values, 10 subjects → selectivity = 1
            // - ex:category: 10 flakes, 2 distinct values, 10 subjects → selectivity-value = 5
            let mut graph = vec![];
            let categories = ["A", "B"];
            for i in 0..10 {
                graph.push(json!({
                    "@id": format!("ex:item{i}"),
                    "@type": "ex:Item",
                    "ex:uniqueId": format!("UID-{i}"),
                    "ex:category": categories[i % 2]
                }));
            }

            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": graph
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let _ = trigger_index_and_wait_outcome(
                &handle,
                result.ledger.ledger_id(),
                result.receipt.t,
            )
            .await;

            // Reload to get ledger_info
            let info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info");

            let stats = &info["stats"];
            let props = &stats["properties"];

            // Verify selectivity formula: selectivity = ceil(count / ndv), minimum 1
            // HLL at precision=8 can have ~10% variance at small cardinalities,
            // so we test that selectivity is within expected ranges.

            // ex:uniqueId: 10 flakes, 10 distinct values, 10 subjects
            // Expected: selectivity-value ≈ 1 (could be 1-2 due to HLL variance)
            // Expected: selectivity-subject ≈ 1 (could be 1-2 due to HLL variance)
            let unique_id_stats = props
                .get("http://example.org/uniqueId")
                .expect("uniqueId property should exist");
            let uid_sel_val = unique_id_stats["selectivity-value"].as_i64().unwrap();
            let uid_sel_sub = unique_id_stats["selectivity-subject"].as_i64().unwrap();
            assert!(
                (1..=2).contains(&uid_sel_val),
                "uniqueId selectivity-value should be 1-2, got {}",
                uid_sel_val
            );
            assert!(
                (1..=2).contains(&uid_sel_sub),
                "uniqueId selectivity-subject should be 1-2, got {}",
                uid_sel_sub
            );

            // ex:category: 10 flakes, 2 distinct values, 10 subjects
            // Expected: selectivity-value ≈ 5 (could be 4-6 due to HLL variance)
            // Expected: selectivity-subject ≈ 1 (could be 1-2 due to HLL variance)
            let category_stats = props
                .get("http://example.org/category")
                .expect("category property should exist");
            let cat_sel_val = category_stats["selectivity-value"].as_i64().unwrap();
            let cat_sel_sub = category_stats["selectivity-subject"].as_i64().unwrap();
            assert!(
                (4..=7).contains(&cat_sel_val),
                "category selectivity-value should be 4-7 (10 flakes / ~2 values), got {}",
                cat_sel_val
            );
            assert!(
                (1..=2).contains(&cat_sel_sub),
                "category selectivity-subject should be 1-2 (10 flakes / ~10 subjects), got {}",
                cat_sel_sub
            );
        })
        .await;
}

#[tokio::test]
async fn multi_class_entities_tracked_correctly() {
    // Tests that entities with multiple @type values are counted in each class.
    // Clojure parity: multi-class instance statistics.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/multi-class:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Create entities with multiple types:
            // - alice: Person, Employee (2 classes)
            // - bob: Person only (1 class)
            // - acme: Organization, LegalEntity (2 classes)
            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": ["ex:Person", "ex:Employee"],
                        "ex:name": "Alice"
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": "Bob"
                    },
                    {
                        "@id": "ex:acme",
                        "@type": ["ex:Organization", "ex:LegalEntity"],
                        "ex:name": "Acme Corp"
                    }
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let _ = trigger_index_and_wait_outcome(
                &handle,
                result.ledger.ledger_id(),
                result.receipt.t,
            )
            .await;

            // Reload to get ledger_info
            let info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info");

            let stats = &info["stats"];
            let classes = &stats["classes"];

            // ex:Person should have count 2 (alice, bob)
            let person_stats = classes
                .get("http://example.org/Person")
                .expect("Person class should exist");
            assert_eq!(
                person_stats["count"].as_i64(),
                Some(2),
                "Person should have 2 instances (alice, bob)"
            );

            // ex:Employee should have count 1 (alice only)
            let employee_stats = classes
                .get("http://example.org/Employee")
                .expect("Employee class should exist");
            assert_eq!(
                employee_stats["count"].as_i64(),
                Some(1),
                "Employee should have 1 instance (alice)"
            );

            // ex:Organization should have count 1 (acme)
            let org_stats = classes
                .get("http://example.org/Organization")
                .expect("Organization class should exist");
            assert_eq!(
                org_stats["count"].as_i64(),
                Some(1),
                "Organization should have 1 instance (acme)"
            );

            // ex:LegalEntity should have count 1 (acme)
            let legal_stats = classes
                .get("http://example.org/LegalEntity")
                .expect("LegalEntity class should exist");
            assert_eq!(
                legal_stats["count"].as_i64(),
                Some(1),
                "LegalEntity should have 1 instance (acme)"
            );
        })
        .await;
}

#[tokio::test]
async fn class_property_type_distribution_tracked() {
    // Tests that class→property presence is tracked in class statistics.
    //
    // NOTE: DB-R no longer tracks per-class datatype distributions / langs.
    // Ref target counts are tracked separately for ref-valued properties.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/type-distribution:main";
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Create entities with various datatypes:
            // - ex:name: string type
            // - ex:age: integer type
            // - ex:salary: decimal type
            // - ex:active: boolean type
            let txn = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:age": 30,
                        "ex:salary": {"@value": "75000.50", "@type": "xsd:decimal"},
                        "ex:active": true
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": "Bob",
                        "ex:age": 25,
                        "ex:salary": {"@value": "60000.00", "@type": "xsd:decimal"},
                        "ex:active": false
                    }
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            let _ = trigger_index_and_wait_outcome(
                &handle,
                result.ledger.ledger_id(),
                result.receipt.t,
            )
            .await;

            // Reload to get ledger_info
            let info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info");

            let stats = &info["stats"];
            let classes = &stats["classes"];

            let person_stats = classes
                .get("http://example.org/Person")
                .expect("Person class should exist");

            // Verify class properties contain the expected properties
            let class_props = person_stats
                .get("properties")
                .expect("Person should have properties");

            assert!(
                class_props.get("http://example.org/name").is_some(),
                "Person should include ex:name in class properties"
            );
            assert!(
                class_props.get("http://example.org/age").is_some(),
                "Person should include ex:age in class properties"
            );
            assert!(
                class_props.get("http://example.org/salary").is_some(),
                "Person should include ex:salary in class properties"
            );
            assert!(
                class_props.get("http://example.org/active").is_some(),
                "Person should include ex:active in class properties"
            );
        })
        .await;
}

#[tokio::test]
async fn large_dataset_statistics_accuracy() {
    // Tests HLL accuracy with a larger dataset (100 entities) across multiple transactions.
    // HLL at precision=8 has ~6.5% standard error, so estimates should be close.
    // Clojure parity: statistics remain accurate at scale.

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let cache_dir = tmp.path().to_path_buf();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "test/large-dataset:main";
            let mut ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

            // This test is intentionally about accumulation across *indexes* (Clojure parity),
            // not about background indexing races. Disable auto-trigger and explicitly
            // `trigger+wait` after each transaction batch.
            let index_cfg = IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            };

            // Insert 100 entities in 5 batches of 20, indexing after each batch,
            // to test HLL accumulation across 5 index refreshes.
            let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
            for batch in 0..5 {
                let mut graph = vec![];
                for i in 0..20 {
                    let idx = batch * 20 + i;
                    graph.push(json!({
                        "@id": format!("ex:employee{idx}"),
                        "@type": "ex:Employee",
                        "ex:name": format!("Employee {idx}"),
                        "ex:department": departments[idx % 5],
                        "ex:employeeId": idx
                    }));
                }

                let txn = json!({
                    "@context": { "ex": "http://example.org/" },
                    "@graph": graph
                });

                let result = fluree
                    .insert_with_opts(
                        ledger,
                        &txn,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert batch");
                let commit_t = result.receipt.t;
                ledger = result.ledger;

                // Trigger indexing to at least this commit_t, wait for completion,
                // then apply the persisted index to the in-memory ledger state so the
                // next batch starts from the newly indexed db (true "across indexes").
                let outcome =
                    trigger_index_and_wait_outcome(&handle, ledger.ledger_id(), commit_t).await;
                let fluree_db_api::IndexOutcome::Completed { index_t, root_id } = outcome else {
                    unreachable!("helper only returns Completed")
                };
                assert!(
                    index_t >= commit_t,
                    "index_t ({index_t}) should be >= commit_t ({commit_t})"
                );
                let root_cid = root_id.expect("expected root_id");
                apply_index_v2(
                    &mut ledger,
                    &root_cid,
                    ledger_id,
                    fluree.storage(),
                    &cache_dir,
                )
                .await;
            }

            // Final sync point (should already be indexed, but keep this to ensure
            // we load stats from a persisted index root).
            let outcome =
                trigger_index_and_wait_outcome(&handle, ledger.ledger_id(), ledger.t()).await;
            let fluree_db_api::IndexOutcome::Completed { root_id, .. } = outcome else {
                unreachable!("helper only returns Completed")
            };
            let root_cid = root_id.expect("expected root_id");

            let loaded = load_db(fluree.storage(), &root_cid, ledger_id)
                .await
                .expect("load_db(root_cid)");

            // Verify counts
            // ex:name: 100 distinct values, 100 subjects
            let name_prop = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/name")
                            .unwrap_or(false)
                    })
                })
                .expect("name property should exist");

            assert_eq!(name_prop.count, 100, "ex:name should have 100 flakes");
            // Allow 15% error for HLL at this scale
            assert!(
                (85..=115).contains(&name_prop.ndv_values),
                "ex:name ndv_values should be ~100, got {}",
                name_prop.ndv_values
            );
            assert!(
                (85..=115).contains(&name_prop.ndv_subjects),
                "ex:name ndv_subjects should be ~100, got {}",
                name_prop.ndv_subjects
            );

            // ex:department: 5 distinct values, 100 subjects
            let dept_prop = loaded
                .stats
                .as_ref()
                .and_then(|s| s.properties.as_ref())
                .and_then(|props| {
                    props.iter().find(|p| {
                        let sid = fluree_db_core::Sid::new(p.sid.0, &p.sid.1);
                        loaded
                            .decode_sid(&sid)
                            .map(|iri| iri == "http://example.org/department")
                            .unwrap_or(false)
                    })
                })
                .expect("department property should exist");

            assert_eq!(dept_prop.count, 100, "ex:department should have 100 flakes");
            assert!(
                (4..=6).contains(&dept_prop.ndv_values),
                "ex:department ndv_values should be ~5, got {}",
                dept_prop.ndv_values
            );
            assert!(
                (85..=115).contains(&dept_prop.ndv_subjects),
                "ex:department ndv_subjects should be ~100, got {}",
                dept_prop.ndv_subjects
            );

            // Verify class count
            let info = fluree
                .ledger_info(ledger_id)
                .execute()
                .await
                .expect("ledger_info");

            let employee_count = info["stats"]["classes"]
                .get("http://example.org/Employee")
                .and_then(|c| c["count"].as_i64())
                .expect("Employee class should exist");

            assert_eq!(
                employee_count, 100,
                "Employee class should have 100 instances"
            );
        })
        .await;
}
