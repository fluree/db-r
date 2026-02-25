//! SPARQL query regression tests exercising the **binary index** path.
//!
//! These tests mirror the memory-only regression tests in `it_query_sparql.rs`
//! but trigger indexing so queries go through `BinaryScanOperator` instead of
//! `RangeScanOperator`. This is important because the CLI/server always use the
//! binary path once data is indexed.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, QueryInput};
use fluree_db_core::MemoryStorage;
use fluree_db_nameservice::memory::MemoryNameService;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    assert_index_defaults, genesis_ledger_for_fluree, normalize_rows_array,
    start_background_indexer_local, trigger_index_and_wait_outcome,
};

type MemoryFluree = fluree_db_api::Fluree<MemoryStorage, MemoryNameService>;
type MemoryLedger = LedgerState;

// =============================================================================
// Shared seeding
// =============================================================================

/// Insert custom-namespace data with forced indexing after each commit.
async fn seed_custom_ns_indexed(
    fluree: &MemoryFluree,
    ledger_id: &str,
    index_cfg: &IndexConfig,
) -> MemoryLedger {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "cust": "http://example.org/custom/",
            "skos": "http://www.w3.org/2008/05/skos#"
        },
        "@graph": [
            {
                "@id": "cust:pkg1",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-1",
                "skos:broader": {"@id": "cust:pkg2"}
            },
            {
                "@id": "cust:pkg2",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-2",
                "skos:broader": {"@id": "cust:pkg3"}
            },
            {
                "@id": "cust:pkg3",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-3"
            }
        ]
    });

    let result = fluree
        .insert_with_opts(
            ledger,
            &insert,
            TxnOpts::default(),
            CommitOpts::default(),
            index_cfg,
        )
        .await
        .expect("insert");
    let ledger = result.ledger;

    assert_eq!(ledger.t(), 1, "should be at t=1 after seeding");
    ledger
}

// =============================================================================
// Bug regression: custom namespace predicate without rdf:type (indexed path)
// =============================================================================

/// Regression: `SELECT ?s ?o WHERE { ?s cust:anchor ?o }` must return rows
/// when the data is in the binary index (not just novelty).
///
/// The memory-only test passes because `RangeScanOperator` works correctly.
/// This test catches bugs that only manifest in the `BinaryScanOperator` path
/// which is what the CLI and server use after indexing.
#[tokio::test]
async fn indexed_sparql_custom_predicate_without_type_returns_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-custom-pred-idx:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = seed_custom_ns_indexed(&fluree, ledger_id, &index_cfg).await;

            // Trigger indexing and wait
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Load the indexed view (GraphDb) — properly threads binary_store
            let view = fluree.db(ledger_id).await.expect("load view");

            // Baseline: query WITH rdf:type pattern
            let with_type = r#"
                PREFIX cust: <http://example.org/custom/>
                SELECT ?s ?o
                WHERE { ?s a cust:CoveragePackage ; cust:anchor ?o . }
                ORDER BY ?o
            "#;
            let result = fluree
                .query_view(&view, QueryInput::Sparql(with_type))
                .await
                .expect("query with type");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (with type)");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([
                    ["cust:pkg1", "anchor-value-1"],
                    ["cust:pkg2", "anchor-value-2"],
                    ["cust:pkg3", "anchor-value-3"]
                ])),
                "baseline: query WITH rdf:type should return 3 rows (indexed)"
            );

            // Bug: query WITHOUT rdf:type pattern
            let without_type = r#"
                PREFIX cust: <http://example.org/custom/>
                SELECT ?s ?o
                WHERE { ?s cust:anchor ?o . }
                ORDER BY ?o
            "#;
            let result = fluree
                .query_view(&view, QueryInput::Sparql(without_type))
                .await
                .expect("query without type");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (without type)");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([
                    ["cust:pkg1", "anchor-value-1"],
                    ["cust:pkg2", "anchor-value-2"],
                    ["cust:pkg3", "anchor-value-3"]
                ])),
                "BUG: query WITHOUT rdf:type should also return 3 rows (indexed path)"
            );

            // Also verify standard namespace predicate works (control)
            let std_pred = r#"
                PREFIX cust: <http://example.org/custom/>
                PREFIX skos: <http://www.w3.org/2008/05/skos#>
                SELECT ?s ?o
                WHERE { ?s skos:broader ?o . }
                ORDER BY ?s
            "#;
            let result = fluree
                .query_view(&view, QueryInput::Sparql(std_pred))
                .await
                .expect("query std predicate");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (std pred)");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([
                    ["cust:pkg1", "cust:pkg2"],
                    ["cust:pkg2", "cust:pkg3"]
                ])),
                "standard namespace predicate should work without rdf:type (indexed)"
            );
        })
        .await;
}

// =============================================================================
// Bug regression: UNION with partially-bound SELECT variable (indexed path)
// =============================================================================

/// Regression: UNION with a SELECT variable only bound in one branch should
/// produce null/unbound rows, not a "Variable not found" error — via indexed path.
#[tokio::test]
async fn indexed_sparql_union_partial_select_var() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-union-idx:main";

    let (local, handle) = start_background_indexer_local(
        fluree.storage().clone(),
        (*fluree.nameservice()).clone(),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = seed_custom_ns_indexed(&fluree, ledger_id, &index_cfg).await;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree.db(ledger_id).await.expect("load view");

            let query = r#"
                PREFIX cust: <http://example.org/custom/>
                PREFIX skos: <http://www.w3.org/2008/05/skos#>
                SELECT ?s ?val ?role
                WHERE {
                  {
                    ?s cust:anchor ?val .
                    BIND("anchor" AS ?role)
                  }
                  UNION
                  {
                    ?s skos:broader ?val .
                  }
                }
                ORDER BY ?s ?val
            "#;

            let result = fluree
                .query_view(&view, QueryInput::Sparql(query))
                .await
                .expect("UNION with partially-bound SELECT var should not error (indexed)");

            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("should be array of rows");

            // 3 anchor rows + 2 broader rows = 5 total
            assert!(
                rows.len() >= 5,
                "expected at least 5 rows from both UNION branches (indexed), got {}",
                rows.len()
            );
        })
        .await;
}
