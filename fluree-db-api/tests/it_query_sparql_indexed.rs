//! SPARQL query regression tests exercising the **binary index** path.
//!
//! These tests mirror the memory-only regression tests in `it_query_sparql.rs`
//! but trigger indexing so queries go through `BinaryScanOperator` instead of
//! `RangeScanOperator`. This is important because the CLI/server always use the
//! binary path once data is indexed.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{
    FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, QueryInput, QueryResult,
};
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
                .query(&view, QueryInput::Sparql(with_type))
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
                .query(&view, QueryInput::Sparql(without_type))
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
                .query(&view, QueryInput::Sparql(std_pred))
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
                .query(&view, QueryInput::Sparql(query))
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

// =============================================================================
// Bug 1 repro: novelty-only custom NS predicate after index (user's exact path)
// =============================================================================

/// The user's scenario: import + index → insert with custom NS predicates →
/// query. The custom predicates exist only in novelty, not the binary index.
/// `BinaryScanOperator::translate_range` returned None because `sid_to_p_id`
/// only checks the persisted index, missing novelty-only predicates.
#[tokio::test]
async fn indexed_then_insert_novelty_custom_pred_returns_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-novelty-pred:main";

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

            // Phase 1: Seed baseline data and index it
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": {
                    "skos": "http://www.w3.org/2004/02/skos/core#",
                    "ex": "http://example.org/ns/"
                },
                "@graph": [
                    {"@id": "ex:concept1", "@type": "skos:Concept", "skos:prefLabel": "One"},
                    {"@id": "ex:concept2", "@type": "skos:Concept", "skos:prefLabel": "Two"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            // Index baseline
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 2: Insert data with a NEW custom NS predicate (only in novelty)
            let custom_insert = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "@graph": [{
                    "@id": "cbc:assoc/coverage-001",
                    "@type": "cust:CoveragePackage",
                    "cust:anchor": {"@id": "cbc:e9235fd0"},
                    "cust:member": [{"@id": "cbc:5b33544d"}, {"@id": "cbc:0476a33f"}]
                }]
            });
            let result = fluree
                .insert(ledger, &custom_insert)
                .await
                .expect("custom ns insert");
            let _ledger = result.ledger;

            // Phase 3: Query via view (same path as CLI)
            let view = fluree.db(ledger_id).await.expect("load view");

            // Bug 1: custom NS predicate without rdf:type returns 0 rows
            let query = r#"
                PREFIX cust: <https://taxo.cbcrc.ca/ns/>
                SELECT ?s ?o
                WHERE { ?s cust:anchor ?o . }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("query novelty-only custom pred");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "novelty-only custom NS predicate should return 1 row; got: {:?}",
                jsonld
            );
        })
        .await;
}

// =============================================================================
// Bug 2 repro: graph crawl empty for custom NS type after index
// =============================================================================

/// After index + insert with a custom namespace rdf:type, graph crawl returns
/// only `{"@id": "..."}` with no properties. The `decode_batch_to_flakes_filtered`
/// function in `binary_range.rs` only handled REF_ID values through DictOverlay
/// fallback, missing DictOverlay-assigned string value IDs.
#[tokio::test]
async fn indexed_then_insert_graph_crawl_custom_type_returns_properties() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-crawl-type:main";

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

            // Phase 1: Seed baseline and index
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": {
                    "skos": "http://www.w3.org/2004/02/skos/core#",
                    "ex": "http://example.org/ns/"
                },
                "@graph": [
                    {"@id": "ex:concept1", "@type": "skos:Concept", "skos:prefLabel": "One"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 2: Insert with custom NS type (only in novelty)
            let custom_insert = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "@graph": [{
                    "@id": "cbc:assoc/coverage-001",
                    "@type": "cust:CoveragePackage",
                    "cust:associationType": {"@id": "cust:assocType/coverage"},
                    "cust:anchor": {"@id": "cbc:e9235fd0"}
                }]
            });
            let result = fluree
                .insert(ledger, &custom_insert)
                .await
                .expect("custom type insert");
            let _ledger = result.ledger;

            // Phase 3: Graph crawl via view
            let view = fluree.db(ledger_id).await.expect("load view");

            let query = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "select": {"?s": ["*"]},
                "values": ["?s", [{"@id": "cbc:assoc/coverage-001"}]]
            });
            let result: QueryResult = fluree.query(&view, &query).await.expect("graph crawl");
            let jsonld = result
                .to_jsonld_async(view.as_graph_db_ref())
                .await
                .expect("to_jsonld_async");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(rows.len(), 1, "should find 1 entity");
            let obj = rows[0].as_object().expect("object");
            assert!(
                obj.len() > 1,
                "graph crawl should return properties, not just @id; got: {:?}",
                obj
            );
        })
        .await;
}

// Regression: repeated vars in a triple pattern must not create duplicate schema
// =============================================================================

/// Regression: indexed/binary-scan path must handle repeated variables in a single triple pattern
/// (e.g. `?x ex:self ?x` or `?x ?x ?o`) without producing a Batch schema containing duplicate VarIds.
#[tokio::test]
async fn indexed_repeated_vars_in_triple_pattern_do_not_duplicate_schema() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-repeated-vars:main";

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

            // Seed and index.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    // Used for ?x ex:self ?x
                    {"@id": "ex:a", "ex:self": {"@id": "ex:a"}},
                    // Used for ?x ?x ?o (predicate IRI equals subject IRI)
                    {"@id": "ex:a", "ex:a": {"@id": "ex:b"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            // 1) subject==object repeated var
            let q1 = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?x WHERE { ?x ex:self ?x }
            "#;
            let r1 = fluree
                .query(&view, QueryInput::Sparql(q1))
                .await
                .expect("query 1 should succeed");
            let jsonld1 = r1.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld1),
                normalize_rows_array(&json!([["ex:a"]])),
                "expected ?x=ex:a"
            );

            // 2) subject==predicate repeated var
            let q2 = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?x ?o WHERE { ?x ?x ?o }
            "#;
            let r2 = fluree
                .query(&view, QueryInput::Sparql(q2))
                .await
                .expect("query 2 should succeed");
            let jsonld2 = r2.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld2),
                normalize_rows_array(&json!([["ex:a", "ex:b"]])),
                "expected (?x,?o)=(ex:a,ex:b)"
            );
        })
        .await;
}

/// Regression: a two-pattern join that shares both subject and object variables
/// (e.g. `?s p1 ?o . ?s p2 ?o`) must not be planned as a PropertyJoinOperator
/// (which assumes distinct object vars) and must execute without duplicate schema.
#[tokio::test]
async fn indexed_multicolumn_join_shared_object_var_executes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-multicolumn-join:main";

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

            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:s1", "ex:p1": {"@id": "ex:o1"}, "ex:p2": {"@id": "ex:o1"}},
                    {"@id": "ex:s2", "ex:p1": {"@id": "ex:o2"}, "ex:p2": {"@id": "ex:o2"}},
                    {"@id": "ex:s3", "ex:p1": {"@id": "ex:o3"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o . ?s ex:p2 ?o . }
            "#;
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("multicolumn join query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([2])),
                "expected two matching (s,o) pairs"
            );
        })
        .await;
}

// =============================================================================
// Overlay correctness: COUNT fast paths must incorporate novelty
// =============================================================================

/// Regression: COUNT queries should return correct results when the binary index
/// is present but changes are in novelty (overlay), including retraction of an
/// indexed fact and re-assertion in novelty.
#[tokio::test]
async fn indexed_overlay_count_reflects_retract_and_reassert() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-count:main";

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

            // Phase 1: Seed and index 4 ex:Person facts.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "@type": "ex:Person"},
                    {"@id": "ex:p2", "@type": "ex:Person"},
                    {"@id": "ex:p3", "@type": "ex:Person"},
                    {"@id": "ex:p4", "@type": "ex:Person"}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?p a ex:Person . }
            "#;

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([4])),
                "baseline count should be 4"
            );

            // Phase 2: Retract one indexed fact in novelty (overlay).
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:p1", "@type": "ex:Person"}
                ]
            });
            let ledger2 = fluree
                .update(ledger1, &retract)
                .await
                .expect("retract in novelty")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([3])),
                "count should reflect novelty retraction"
            );

            // Phase 3: Re-assert the same fact in novelty.
            let reassert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "@type": "ex:Person"}
                ]
            });
            let ledger3 = fluree
                .insert(ledger2, &reassert)
                .await
                .expect("re-assert in novelty")
                .ledger;

            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("load view at t=3");
            let result = fluree
                .query(&view3, QueryInput::Sparql(query))
                .await
                .expect("count at t=3");
            let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([4])),
                "count should reflect novelty re-assertion"
            );
        })
        .await;
}

/// Regression: GROUP BY + COUNT top-k should reflect novelty deltas even when
/// the binary index is present and overlay introduces retractions/assertions.
#[tokio::test]
async fn indexed_overlay_group_by_count_topk_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-group-count-topk:main";

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

            // Phase 1: Seed and index baseline policyState distribution:
            // CA = 3, WA = 2
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:policyState": "CA"},
                    {"@id": "ex:b", "ex:policyState": "CA"},
                    {"@id": "ex:c", "ex:policyState": "CA"},
                    {"@id": "ex:d", "ex:policyState": "WA"},
                    {"@id": "ex:e", "ex:policyState": "WA"}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?o (COUNT(?s) AS ?cnt)
                WHERE { ?s ex:policyState ?o . }
                GROUP BY ?o
                ORDER BY DESC(?cnt)
                LIMIT 2
            "#;

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("group count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([["CA", 3], ["WA", 2]])),
                "baseline group counts should be CA=3, WA=2"
            );

            // Phase 2: Overlay changes:
            // - Move ex:e from WA -> CA (retract WA, assert CA)
            // - Insert a new subject ex:f with CA (assert-only)
            // Final: CA = 4, WA = 1
            let overlay_tx = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:e", "ex:policyState": "WA"}
                ],
                "insert": [
                    {"@id": "ex:e", "ex:policyState": "CA"},
                    {"@id": "ex:f", "ex:policyState": "CA"}
                ]
            });
            let ledger2 = fluree
                .update(ledger1, &overlay_tx)
                .await
                .expect("overlay update")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("group count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows_array(&jsonld),
                normalize_rows_array(&json!([["CA", 5], ["WA", 1]])),
                "group counts should reflect overlay deltas"
            );
        })
        .await;
}
