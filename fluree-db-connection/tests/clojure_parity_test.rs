//! Clojure parity tests for fluree-db-connection
//!
//! These tests verify that Rust can parse Clojure-style JSON-LD configs
//! and load the same databases.
//!
//! Run with: cargo test -p fluree-db-connection --test clojure_parity_test -- --ignored

use fluree_db_connection::{connect, ConnectionConfig, ConnectionHandle, StorageType};
use fluree_db_core::{ContentId, ContentKind};
use fluree_db_query::{execute_pattern, RowAccess, Term, TriplePattern, VarRegistry};
use serde_json::json;
use std::path::{Path, PathBuf};

fn get_test_db_path() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-database");

    if path.exists() {
        Some(path)
    } else {
        None
    }
}

/// Ledger alias embedded in the test database path layout.
const TEST_LEDGER_ID: &str = "test/range-scan:main";

fn find_root_file(test_db_path: &Path) -> Option<ContentId> {
    let root_dir = test_db_path.join("test/range-scan/index/root");

    if !root_dir.exists() {
        return None;
    }

    let entry = std::fs::read_dir(&root_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))?;

    let bytes = std::fs::read(entry.path()).ok()?;
    Some(ContentId::new(ContentKind::IndexRoot, &bytes))
}

/// Test that Rust can parse Clojure-style JSON-LD config and load same database
///
/// Run with: cargo test -p fluree-db-connection --test clojure_parity_test -- --ignored --nocapture
#[tokio::test]
#[ignore = "Requires external test-database/ directory"]
async fn test_connection_parity() {
    let test_db_path = match get_test_db_path() {
        Some(p) => p,
        None => {
            eprintln!("Test database not found at ../test-database/, skipping");
            return;
        }
    };

    let test_db_str = test_db_path.to_str().unwrap();

    // 1. Use Clojure-style JSON-LD config
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@id": "file",
        "@graph": [
            {
                "@id": "fileStorage",
                "@type": "Storage",
                "filePath": test_db_str
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 4,
                "cacheMaxMb": 1000,
                "indexStorage": {"@id": "fileStorage"},
                "commitStorage": {"@id": "fileStorage"}
            }
        ]
    });

    // 2. Connect using JSON-LD config (same code path as Clojure)
    let conn = connect(&config).expect("Failed to parse JSON-LD config");

    // Verify config was parsed correctly
    let parsed_config = conn.config();
    assert_eq!(parsed_config.parallelism, 4);
    assert_eq!(parsed_config.cache.max_mb, 1000);
    assert!(matches!(
        parsed_config.index_storage.storage_type,
        StorageType::File
    ));

    // 3. Load database
    let root_id = match find_root_file(&test_db_path) {
        Some(r) => r,
        None => {
            eprintln!("No root file found, skipping");
            return;
        }
    };

    println!("Test database path: {}", test_db_path.display());
    println!("Root CID: {}", root_id);

    let db = match conn {
        ConnectionHandle::File(c) => c
            .load_ledger_snapshot_fresh_cache(&root_id, TEST_LEDGER_ID)
            .await
            .unwrap(),
        _ => panic!("Expected FileConnection"),
    };

    println!(
        "Database loaded: alias={}, t={}, version={}",
        db.ledger_id, db.t, db.version
    );

    // 4. Execute query via execute_pattern (bypasses JSON query parser)
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(Term::Var(s), Term::Var(p), Term::Var(o));
    let batches = execute_pattern(&db, 0, &vars, pattern).await.unwrap();

    assert!(!batches.is_empty(), "Should have at least one batch");
    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert!(total > 0, "Should have results");

    println!("Connection parity test: {} triples loaded", total);

    // Verify first row has all bindings
    if let Some(batch) = batches.first() {
        if let Some(row) = batch.row_view(0) {
            assert!(row.get(s).is_some(), "Subject should be bound");
            assert!(row.get(p).is_some(), "Predicate should be bound");
            assert!(row.get(o).is_some(), "Object should be bound");
        }
    }

    println!("Connection parity test passed!");
}

/// Test parsing JSON-LD config with extra components we don't support yet
///
/// This should parse without error and ignore the publisher node.
#[test]
fn test_config_with_unsupported_components() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fileStorage", "@type": "Storage", "filePath": "/tmp/test"},
            {
                "@id": "publisher",
                "@type": "Publisher",
                "storage": {"@id": "fileStorage"}
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "fileStorage"},
                "primaryPublisher": {"@id": "publisher"}  // We don't use this yet
            }
        ]
    });

    // Should parse without error (ignores publisher)
    let conn = connect(&config);
    assert!(conn.is_ok(), "Should parse config with extra components");

    let conn = conn.unwrap();
    let parsed = conn.config();
    assert!(matches!(
        parsed.index_storage.storage_type,
        StorageType::File
    ));
    assert_eq!(parsed.index_storage.path.as_deref(), Some("/tmp/test"));
}

/// Test parsing JSON-LD config without commitStorage
#[test]
fn test_config_index_storage_only() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fileStorage", "@type": "Storage", "filePath": "/data/fluree"},
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 8,
                "indexStorage": {"@id": "fileStorage"}
            }
        ]
    });

    let conn = connect(&config).expect("Should parse config");
    let parsed = conn.config();
    assert_eq!(parsed.parallelism, 8);
    assert!(parsed.commit_storage.is_none());
}

/// Test parsing memory storage config
#[test]
fn test_memory_storage_config() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "memStorage", "@type": "Storage"},
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "memStorage"}
            }
        ]
    });

    let conn = connect(&config).expect("Should parse memory config");
    assert!(matches!(conn, ConnectionHandle::Memory(_)));

    let parsed = conn.config();
    assert!(matches!(
        parsed.index_storage.storage_type,
        StorageType::Memory
    ));
}

/// Test that from_json_ld correctly handles array-wrapped values
#[test]
fn test_json_ld_expansion() {
    // This tests that our code handles the post-expansion JSON-LD format
    // where values are wrapped in arrays like [{"@value": 4}]
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fs", "@type": "Storage", "filePath": "/path/to/data"},
            {
                "@id": "conn",
                "@type": "Connection",
                "parallelism": 16,
                "cacheMaxMb": 2000,
                "indexStorage": {"@id": "fs"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
    assert_eq!(parsed.parallelism, 16);
    assert_eq!(parsed.cache.max_mb, 2000);
    assert_eq!(parsed.index_storage.path.as_deref(), Some("/path/to/data"));
}

/// Test error when no Connection node found
#[test]
fn test_error_no_connection_node() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fileStorage", "@type": "Storage", "filePath": "/tmp/test"}
        ]
    });

    let result = connect(&config);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("No Connection node"));
}

/// Test error when no indexStorage specified
#[test]
fn test_error_no_index_storage() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 4
            }
        ]
    });

    let result = connect(&config);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("indexStorage required"));
}

/// Test error when multiple Connection nodes found
#[test]
fn test_error_multiple_connection_nodes() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "fs", "@type": "Storage", "filePath": "/tmp/test"},
            {
                "@id": "conn1",
                "@type": "Connection",
                "indexStorage": {"@id": "fs"}
            },
            {
                "@id": "conn2",
                "@type": "Connection",
                "indexStorage": {"@id": "fs"}
            }
        ]
    });

    let result = connect(&config);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Multiple Connection nodes"));
}

/// Test parsing addressIdentifiers map
#[test]
fn test_address_identifiers_parsing() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "memStorage", "@type": "Storage"},
            {"@id": "fileStorage", "@type": "Storage", "filePath": "/data/commits"},
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "memStorage"},
                "addressIdentifiers": {
                    "commit-storage": {"@id": "fileStorage"},
                    "index-storage": {"@id": "memStorage"}
                }
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    let addr_ids = parsed
        .address_identifiers
        .as_ref()
        .expect("addressIdentifiers should be present");
    assert_eq!(addr_ids.len(), 2);

    // Check commit-storage points to file storage
    let commit_storage = addr_ids
        .get("commit-storage")
        .expect("commit-storage should exist");
    assert!(matches!(commit_storage.storage_type, StorageType::File));
    assert_eq!(commit_storage.path.as_deref(), Some("/data/commits"));

    // Check index-storage points to memory storage
    let index_storage = addr_ids
        .get("index-storage")
        .expect("index-storage should exist");
    assert!(matches!(index_storage.storage_type, StorageType::Memory));
}

/// Test backward compatibility: configs without addressIdentifiers work unchanged
#[test]
fn test_no_address_identifiers_backward_compat() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "storage", "@type": "Storage"},
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "storage"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
    assert!(parsed.address_identifiers.is_none());
}

/// Test multiple identifiers in addressIdentifiers
#[test]
fn test_multiple_address_identifiers() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "storage1", "@type": "Storage", "filePath": "/data/store1"},
            {"@id": "storage2", "@type": "Storage", "filePath": "/data/store2"},
            {"@id": "storage3", "@type": "Storage", "filePath": "/data/store3"},
            {"@id": "memStorage", "@type": "Storage"},
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "memStorage"},
                "addressIdentifiers": {
                    "store-1": {"@id": "storage1"},
                    "store-2": {"@id": "storage2"},
                    "store-3": {"@id": "storage3"}
                }
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    let addr_ids = parsed
        .address_identifiers
        .as_ref()
        .expect("addressIdentifiers should be present");
    assert_eq!(addr_ids.len(), 3);

    // All three should be file storages with correct paths
    assert_eq!(
        addr_ids.get("store-1").unwrap().path.as_deref(),
        Some("/data/store1")
    );
    assert_eq!(
        addr_ids.get("store-2").unwrap().path.as_deref(),
        Some("/data/store2")
    );
    assert_eq!(
        addr_ids.get("store-3").unwrap().path.as_deref(),
        Some("/data/store3")
    );
}

/// Test ConfigGraph correctly resolves array-wrapped references
///
/// After JSON-LD expansion, indexStorage becomes:
/// `"https://ns.flur.ee/system#indexStorage": [{"@id": "https://..."}]`
/// This test verifies resolve_first handles the array wrapper correctly.
#[test]
fn test_resolve_first_handles_array_wrapped_refs() {
    use fluree_db_connection::ConfigGraph;

    // Simulate already-expanded JSON-LD (what json_ld::expand returns)
    let expanded = json!([
        {
            "@id": "https://ns.flur.ee/config/connection/fileStorage",
            "@type": ["https://ns.flur.ee/system#Storage"],
            "https://ns.flur.ee/system#filePath": [{"@value": "/data/fluree"}]
        },
        {
            "@id": "https://ns.flur.ee/config/connection/connection",
            "@type": ["https://ns.flur.ee/system#Connection"],
            "https://ns.flur.ee/system#indexStorage": [
                {"@id": "https://ns.flur.ee/config/connection/fileStorage"}
            ]
        }
    ]);

    let graph = ConfigGraph::from_expanded(&expanded).unwrap();

    // Get connection node
    let conn = graph
        .get("https://ns.flur.ee/config/connection/connection")
        .unwrap();

    // Get the indexStorage field (array-wrapped reference)
    let storage_field = conn.get("https://ns.flur.ee/system#indexStorage").unwrap();

    // resolve_first should unwrap the array and resolve the reference
    let resolved = graph.resolve_first(storage_field);
    assert!(resolved.is_some());

    let storage_node = resolved.unwrap();
    assert!(storage_node
        .get("https://ns.flur.ee/system#filePath")
        .is_some());
}
