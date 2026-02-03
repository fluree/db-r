//! End-to-end integration tests for fluree-db-api
//!
//! These tests demonstrate the full stack: connection, ledger loading,
//! novelty overlay, and query execution.

use fluree_db_api::{
    Fluree, MemoryStorage, Term, TriplePattern, VarRegistry,
};
use fluree_db_connection::{Connection, ConnectionConfig};
use fluree_db_core::{Flake, FlakeValue, Sid};
use fluree_db_nameservice::memory::MemoryNameService;
use fluree_db_nameservice::Publisher;
use fluree_db_novelty::Commit;

fn make_flake(s: i32, p: i32, o: i64, t: i64) -> Flake {
    Flake::new(
        Sid::new(s, format!("s{}", s)),
        Sid::new(p, format!("p{}", p)),
        FlakeValue::Long(o),
        Sid::new(2, "long"),
        t,
        true,
        None,
    )
}

/// Helper to create a test Fluree instance with pre-populated storage
fn create_test_fluree(
    storage: MemoryStorage,
    commits: Vec<(&str, Commit)>,
) -> Fluree<MemoryStorage, MemoryNameService> {
    // Insert commits into storage
    for (addr, commit) in commits {
        storage.insert(addr, serde_json::to_vec(&commit).unwrap());
    }

    let config = ConnectionConfig::default();
    let connection = Connection::new(config, storage);
    let nameservice = MemoryNameService::new();

    Fluree::new(connection, nameservice)
}

/// Test the full query flow with in-memory storage and novelty
#[tokio::test]
async fn test_query_with_novelty_overlay() {
    let flakes = vec![
        make_flake(1, 10, 100, 1),
        make_flake(2, 10, 200, 1),
        make_flake(3, 10, 300, 1),
    ];

    let commit = Commit::new("commit-1", 1, flakes);
    let fluree = create_test_fluree(MemoryStorage::new(), vec![("commit-1", commit)]);

    // Publish to nameservice
    fluree
        .nameservice()
        .publish_commit("test:main", "commit-1", 1)
        .await
        .unwrap();

    // Load the ledger
    let ledger = fluree.ledger("test:main").await.unwrap();

    // Verify ledger state
    assert_eq!(ledger.alias(), "test:main");
    assert_eq!(ledger.t(), 1);
    assert_eq!(ledger.novelty.len(), 3); // 3 flakes in novelty

    // Set up query variables
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    // Query all triples (?s ?p ?o) - use query_pattern for synthetic Sids
    let pattern = TriplePattern::new(Term::Var(s), Term::Var(p), Term::Var(o));
    let batches = fluree.query_pattern(&ledger, &vars, pattern).await.unwrap();

    // Should get results from novelty
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total_rows, 3, "Should get 3 results from novelty");

    // Verify we can access the results
    if let Some(batch) = batches.first() {
        if let Some(row) = batch.row_view(0) {
            assert!(row.get(s).is_some(), "Subject should be bound");
            assert!(row.get(p).is_some(), "Predicate should be bound");
            assert!(row.get(o).is_some(), "Object should be bound");
        }
    }
}

/// Test query with specific predicate filter
#[tokio::test]
async fn test_query_specific_predicate() {
    // Create flakes with different predicates
    let flakes = vec![
        make_flake(1, 10, 100, 1), // predicate 10
        make_flake(2, 20, 200, 1), // predicate 20
        make_flake(3, 10, 300, 1), // predicate 10
    ];

    let commit = Commit::new("commit-1", 1, flakes);
    let fluree = create_test_fluree(MemoryStorage::new(), vec![("commit-1", commit)]);

    fluree
        .nameservice()
        .publish_commit("test:main", "commit-1", 1)
        .await
        .unwrap();
    let ledger = fluree.ledger("test:main").await.unwrap();

    // Query with specific predicate (p=10)
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let o = vars.get_or_insert("?o");

    let predicate_sid = Sid::new(10, "p10");
    let pattern = TriplePattern::new(Term::Var(s), Term::Sid(predicate_sid), Term::Var(o));

    let batches = fluree.query_pattern(&ledger, &vars, pattern).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();

    // Should only get flakes with predicate 10
    assert_eq!(total_rows, 2, "Should get 2 results with predicate 10");
}

/// Test multiple commits building on each other
#[tokio::test]
async fn test_multiple_commits() {
    // First commit
    let commit1 = Commit::new("commit-1", 1, vec![make_flake(1, 10, 100, 1)]);

    // Second commit (links to first)
    let mut commit2 = Commit::new("commit-2", 2, vec![make_flake(2, 10, 200, 2)]);
    commit2.previous = Some("commit-1".to_string());

    let fluree = create_test_fluree(
        MemoryStorage::new(),
        vec![("commit-1", commit1), ("commit-2", commit2)],
    );

    // Publish both commits
    fluree
        .nameservice()
        .publish_commit("test:main", "commit-1", 1)
        .await
        .unwrap();
    fluree
        .nameservice()
        .publish_commit("test:main", "commit-2", 2)
        .await
        .unwrap();

    // Load ledger - should see both commits
    let ledger = fluree.ledger("test:main").await.unwrap();

    assert_eq!(ledger.t(), 2, "Should be at t=2");
    assert_eq!(ledger.novelty.len(), 2, "Should have 2 flakes in novelty");

    // Query should return both
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(Term::Var(s), Term::Var(p), Term::Var(o));
    let batches = fluree.query_pattern(&ledger, &vars, pattern).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();

    assert_eq!(total_rows, 2, "Should get 2 results from both commits");
}

/// Test query with multiple results
#[tokio::test]
async fn test_query_multiple_results() {
    let flakes = vec![make_flake(1, 10, 100, 1), make_flake(2, 10, 200, 1)];

    let commit = Commit::new("commit-1", 1, flakes);
    let fluree = create_test_fluree(MemoryStorage::new(), vec![("commit-1", commit)]);

    fluree
        .nameservice()
        .publish_commit("test:main", "commit-1", 1)
        .await
        .unwrap();
    let ledger = fluree.ledger("test:main").await.unwrap();

    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(Term::Var(s), Term::Var(p), Term::Var(o));

    let batches = fluree.query_pattern(&ledger, &vars, pattern).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();

    assert_eq!(total_rows, 2, "Should have 2 rows total");
}

/// Test empty ledger query
#[tokio::test]
async fn test_empty_ledger_query() {
    // Publish an empty commit (genesis-like)
    let commit = Commit::new("commit-1", 0, vec![]);
    let fluree = create_test_fluree(MemoryStorage::new(), vec![("commit-1", commit)]);

    fluree
        .nameservice()
        .publish_commit("test:main", "commit-1", 0)
        .await
        .unwrap();
    let ledger = fluree.ledger("test:main").await.unwrap();

    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(Term::Var(s), Term::Var(p), Term::Var(o));
    let batches = fluree.query_pattern(&ledger, &vars, pattern).await.unwrap();

    assert!(batches.is_empty(), "Empty ledger should return no batches");
}

/// Test ledger not found error
#[tokio::test]
async fn test_ledger_not_found() {
    let fluree = create_test_fluree(MemoryStorage::new(), vec![]);

    // Try to load a ledger that doesn't exist
    let result = fluree.ledger("nonexistent:main").await;

    assert!(result.is_err(), "Should error for non-existent ledger");
}

// =============================================================================
// File-based integration tests (require test-database)
// =============================================================================

#[cfg(feature = "native")]
mod file_tests {
    use fluree_db_api::fluree_file;
    use serde_json::json;
    use std::path::PathBuf;

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

    /// End-to-end test using the fluree-db-api with a real file-backed database.
    ///
    /// This test mirrors what Clojure's API does:
    /// 1. Create connection via `connect-file`
    /// 2. Load db via `(fluree/db conn "ledger-alias")`
    /// 3. Execute query via `(fluree/query db query-json)`
    ///
    /// Run with: cargo test -p fluree-db-api --test integration_test file_tests -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "Requires external test-database/ directory"]
    async fn test_file_backed_query() {
        let test_db_path = match get_test_db_path() {
            Some(p) => p,
            None => {
                eprintln!("Test database not found at ../test-database/, skipping");
                return;
            }
        };

        println!("Test database path: {}", test_db_path.display());

        // 1. Create connection (equivalent to Clojure's connect-file)
        let fluree = fluree_file(test_db_path.to_str().unwrap()).expect("Failed to create Fluree");
        println!("Fluree connection created");

        // 2. Load db (equivalent to Clojure's (fluree/db conn "test/range-scan"))
        let db = fluree
            .ledger("test/range-scan:main")
            .await
            .expect("Failed to load db");

        println!(
            "Db loaded: alias={}, t={}, indexed_t={}",
            db.alias(),
            db.t(),
            db.db.t
        );

        // 3. Execute JSON-LD query (equivalent to Clojure's (fluree/query db {...}))
        // Query for all subjects and their types using @type (rdf:type)
        let query = json!({
            "select": ["?s", "?type"],
            "where": { "@id": "?s", "@type": "?type" }
        });

        let result = fluree
            .query(&db, &query)
            .await
            .expect("Query failed");

        println!(
            "Query returned {} rows in {} batches",
            result.row_count(),
            result.batches.len()
        );

        assert!(!result.is_empty(), "Should have results from test database");

        // Verify we can access results via the variable registry
        let s_var = result.vars.get("?s").expect("?s should be registered");
        let type_var = result.vars.get("?type").expect("?type should be registered");

        if let Some(batch) = result.batches.first() {
            if let Some(row) = batch.row_view(0) {
                assert!(row.get(s_var).is_some(), "Subject should be bound");
                assert!(row.get(type_var).is_some(), "Type should be bound");

                println!(
                    "First row: s={:?}, type={:?}",
                    row.get(s_var),
                    row.get(type_var)
                );
            }
        }

        println!("File-backed query test passed!");
    }

    /// Benchmark-style test comparing with Clojure
    ///
    /// Run with: cargo test -p fluree-db-api --test integration_test file_tests::test_query_benchmark -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "Benchmark: requires external test-database/ directory"]
    async fn test_query_benchmark() {
        use std::time::Instant;

        let test_db_path = match get_test_db_path() {
            Some(p) => p,
            None => {
                eprintln!("Test database not found, skipping");
                return;
            }
        };

        let fluree = fluree_file(test_db_path.to_str().unwrap()).expect("Failed to create Fluree");
        let db = fluree
            .ledger("test/range-scan:main")
            .await
            .expect("Failed to load db");

        println!("Db loaded: t={}", db.t());

        // Query to benchmark
        let query = json!({
            "select": ["?s", "?type"],
            "where": { "@id": "?s", "@type": "?type" }
        });

        // Warmup
        println!("Warming up...");
        for _ in 0..10 {
            let _ = fluree.query(&db, &query).await.unwrap();
        }

        // Benchmark
        let iterations = 100;
        let start = Instant::now();
        let mut total_rows = 0;

        for _ in 0..iterations {
            let result = fluree.query(&db, &query).await.unwrap();
            total_rows += result.row_count();
        }

        let elapsed = start.elapsed();
        let rows_per_iter = total_rows / iterations;
        let queries_per_sec = iterations as f64 / elapsed.as_secs_f64();

        println!("=== Fluree API Query Benchmark ===");
        println!("  Iterations: {}", iterations);
        println!("  Rows per query: {}", rows_per_iter);
        println!("  Total time: {:?}", elapsed);
        println!("  Time per query: {:?}", elapsed / iterations as u32);
        println!("  Queries/sec: {:.0}", queries_per_sec);
    }
}
