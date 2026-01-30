//! Shared test harness for fluree-db-api integration tests.
//!
//! Provides type aliases, helpers, and utilities that match the Clojure test patterns.

// Many helpers are used by *some* integration test crates but not others.
// Keep them centralized here and silence dead_code warnings in crates that
// don't reference every helper.
#![allow(dead_code)]

use fluree_db_api::{IndexConfig, LedgerState, Novelty, SimpleCache};
use fluree_db_core::{Db, MemoryStorage};
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;

#[cfg(feature = "native")]
use tokio::task::LocalSet;

// =============================================================================
// Type aliases (reduce boilerplate in test signatures)
// =============================================================================

/// Type alias for memory-backed Fluree instance.
pub type MemoryFluree = fluree_db_api::Fluree<
    MemoryStorage,
    SimpleCache,
    fluree_db_nameservice::memory::MemoryNameService,
>;

/// Type alias for memory-backed ledger state.
pub type MemoryLedger = LedgerState<MemoryStorage, SimpleCache>;

// =============================================================================
// Context helpers
// =============================================================================

/// Standard default context used in Fluree tests (matches Clojure `test-utils/default-context`)
pub fn default_context() -> JsonValue {
    json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "sh": "http://www.w3.org/ns/shacl#",
        "schema": "http://schema.org/",
        "skos": "http://www.w3.org/2008/05/skos#",
        "wiki": "https://www.wikidata.org/wiki/",
        "f": "https://ns.flur.ee/ledger#"
    })
}

// =============================================================================
// Ledger helpers
// =============================================================================

/// Create a genesis ledger state for the given alias.
///
/// This is the Rust equivalent of `(fluree/create conn "ledger")` prior to the first commit:
/// the nameservice has no record yet, and `commit()` will create one via `publish_commit()`.
pub fn genesis_ledger(fluree: &MemoryFluree, alias: &str) -> MemoryLedger {
    let db = Db::genesis(fluree.storage().clone(), SimpleCache::new(10_000), alias);
    LedgerState::new(db, Novelty::new(0))
}

// =============================================================================
// Background indexing helpers (tests)
// =============================================================================

/// Start a `BackgroundIndexerWorker` on a `tokio::task::LocalSet` and return the handle.
///
/// This is the Rust equivalent of Clojure tests that pass an `index-files-ch` to `commit!`
/// and then block until indexing completes. In Rust, tests should:
/// - transact (get `receipt.t`)
/// - `handle.trigger(alias, receipt.t)`
/// - `completion.wait().await`
#[cfg(feature = "native")]
pub fn start_background_indexer_local<S, N>(
    storage: S,
    nameservice: N,
    config: fluree_db_indexer::IndexerConfig,
) -> (LocalSet, fluree_db_indexer::IndexerHandle)
where
    S: fluree_db_core::Storage + Clone + Send + Sync + 'static,
    N: fluree_db_nameservice::NameService + fluree_db_nameservice::Publisher + Clone + 'static,
{
    let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
        storage,
        Arc::new(nameservice),
        config,
    );

    let local = LocalSet::new();
    local.spawn_local(worker.run());

    (local, handle)
}

// =============================================================================
// Index config assertions
// =============================================================================

/// Assert that IndexConfig defaults match Clojure parity.
///
/// Clojure defaults from `add-reindex-thresholds`:
/// - min: 100_000
/// - max: 1_000_000
pub fn assert_index_defaults() {
    let cfg = IndexConfig::default();
    assert_eq!(cfg.reindex_min_bytes, 100_000);
    assert_eq!(cfg.reindex_max_bytes, 1_000_000);
}

// =============================================================================
// Result normalization (for unordered comparisons)
// =============================================================================

/// Normalize JSON-LD row results for unordered comparison.
///
/// Sorts rows by their JSON string representation so tests can compare
/// result sets without relying on a specific order.
pub fn normalize_rows(v: &JsonValue) -> Vec<JsonValue> {
    let mut rows = v
        .as_array()
        .expect("expected JSON array of rows")
        .iter()
        .cloned()
        .collect::<Vec<_>>();

    rows.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default())
    });

    rows
}

/// Normalize JSON-LD row results as nested arrays for unordered comparison.
///
/// Similar to `normalize_rows` but preserves the inner array structure.
pub fn normalize_rows_array(v: &JsonValue) -> Vec<Vec<JsonValue>> {
    let mut rows: Vec<Vec<JsonValue>> = v
        .as_array()
        .expect("rows should be an array")
        .iter()
        .map(|row| {
            // JSON-LD formatter flattens single-column selects to a flat array of values.
            // Preserve a consistent nested-array shape for callers by wrapping scalars.
            match row.as_array() {
                Some(arr) => arr.iter().cloned().collect::<Vec<_>>(),
                None => vec![row.clone()],
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap()
            .cmp(&serde_json::to_string(b).unwrap())
    });
    rows
}

/// Normalize SPARQL JSON bindings for unordered comparison.
///
/// Extracts `results.bindings` from a SPARQL JSON response and sorts
/// the bindings by their JSON string representation.
pub fn normalize_sparql_bindings(v: &JsonValue) -> Vec<JsonValue> {
    let bindings = v
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .expect("SPARQL JSON results.bindings should be an array");
    let mut out: Vec<JsonValue> = bindings.iter().cloned().collect();
    out.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap()
            .cmp(&serde_json::to_string(b).unwrap())
    });
    out
}

// =============================================================================
// Common @context helpers
// =============================================================================

/// Common @context for ex: and schema: prefixes.
///
/// Returns:
/// ```json
/// {
///   "ex": "http://example.org/ns/",
///   "schema": "http://schema.org/",
///   "xsd": "http://www.w3.org/2001/XMLSchema#"
/// }
/// ```
pub fn context_ex_schema() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

// =============================================================================
// Test data fixtures
// =============================================================================

/// People test data (matches Clojure `test-utils/people`)
pub fn people_data() -> JsonValue {
    json!([
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7
        },
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "schema:email": "alice@example.org",
            "schema:age": 50,
            "ex:favNums": [42, 76, 9],
            "schema:birthDate": {"@value": "1974-09-26", "@type": "xsd:date"}
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": ["ex:brian", "ex:alice"]
        },
        {
            "@id": "ex:liam",
            "@type": "ex:User",
            "schema:name": "Liam",
            "schema:email": "liam@example.org",
            "schema:age": 13,
            "ex:favNums": [42, 11],
            "ex:friend": ["ex:brian", "ex:alice", "ex:cam"],
            "schema:birthDate": {"@value": "2011-09-26", "@type": "xsd:date"}
        }
    ])
}

/// Load people test data into a new ledger (matches Clojure `test-utils/load-people`)
pub async fn load_people(fluree: &MemoryFluree) -> Result<String, Box<dyn std::error::Error>> {
    let ledger_alias = "test/people:main";
    let ledger = fluree.create_ledger(ledger_alias).await?;

    let ctx = json!([
        default_context(),
        {"ex": "http://example.org/ns/"}
    ]);

    let insert_txn = json!({
        "@context": ctx,
        "@graph": people_data()
    });

    fluree.insert(ledger, &insert_txn).await?;
    Ok(ledger_alias.to_string())
}
