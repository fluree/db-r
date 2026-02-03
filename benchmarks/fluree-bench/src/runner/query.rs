//! Query benchmark matrix runner.
//!
//! Executes predefined queries across a matrix of:
//! - Query complexity (simple count, filtered, multi-hop, aggregate)
//! - Concurrency levels (configurable)
//! - Cache state (cold vs warm)

use std::sync::Arc;
use std::time::Instant;

use fluree_db_api::FlureeClient;

use crate::metrics::collector::{CacheState, MetricsCollector, QueryTiming};
use crate::setup;
use crate::QueryArgs;

/// A predefined benchmark query.
struct BenchQuery {
    name: &'static str,
    sparql: String,
    /// Expected row count (if verification is enabled). None = skip check.
    expected_rows: Option<usize>,
}

/// Run the query benchmark matrix.
pub async fn run(
    fluree: &Arc<FlureeClient>,
    args: &QueryArgs,
    collector: &mut MetricsCollector,
) -> Result<(), Box<dyn std::error::Error>> {
    let alias = setup::normalize_alias(&args.common.ledger);
    let units = crate::datagen::compute_units(args.common.data_size_mb);

    let concurrency_levels = parse_concurrency(&args.query_concurrency);
    let queries = build_queries(units);

    tracing::info!(
        queries = queries.len(),
        iterations = args.query_iterations,
        concurrency_levels = ?concurrency_levels,
        "Starting query matrix"
    );

    for query in &queries {
        for &conc in &concurrency_levels {
            // Cold run — the NodeCache generic was removed from Connection in
            // the perf-optimize refactor, so we can no longer clear the cache
            // from the public API. The "cold" label is kept for structural
            // parity but the run may benefit from a warm cache.
            // TODO: re-add cache clearing once a public API is available.
            for _ in 0..args.query_iterations {
                let timing = run_query_at_concurrency(
                    fluree,
                    &alias,
                    query,
                    conc,
                    CacheState::Cold,
                    !args.skip_verify,
                )
                .await?;
                collector.record_query(query.name, timing);
            }

            // Warm run — cache is populated from the last cold iteration.
            // No cache clearing here; each iteration benefits from prior runs.
            for _ in 0..args.query_iterations {
                let timing = run_query_at_concurrency(
                    fluree,
                    &alias,
                    query,
                    conc,
                    CacheState::Warm,
                    !args.skip_verify,
                )
                .await?;
                collector.record_query(query.name, timing);
            }
        }
    }

    Ok(())
}

async fn run_query_at_concurrency(
    fluree: &Arc<FlureeClient>,
    alias: &str,
    query: &BenchQuery,
    concurrency: usize,
    cache_state: CacheState,
    verify: bool,
) -> Result<QueryTiming, Box<dyn std::error::Error>> {
    let t0 = Instant::now();

    // For concurrency > 1, spawn N copies and take the slowest.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..concurrency {
        let fluree = Arc::clone(fluree);
        let alias = alias.to_string();
        let sparql = query.sparql.clone();

        tasks.spawn(async move {
            fluree
                .graph(&alias)
                .query()
                .sparql(&sparql)
                .execute_formatted()
                .await
        });
    }

    let mut row_count = None;
    while let Some(join_result) = tasks.join_next().await {
        let result = join_result??;
        // Extract row count from SPARQL JSON results format.
        if row_count.is_none() {
            if let Some(results) = result.get("results") {
                if let Some(bindings) = results.get("bindings") {
                    row_count = bindings.as_array().map(|a| a.len());
                }
            }
            // Fallback: top-level array (FQL-style result)
            if row_count.is_none() {
                row_count = result.as_array().map(|a| a.len());
            }
        }
    }

    let elapsed = t0.elapsed();

    let verified = if verify {
        query
            .expected_rows
            .map(|expected| row_count == Some(expected))
    } else {
        None
    };

    if let (Some(false), Some(expected)) = (verified, query.expected_rows) {
        tracing::warn!(
            query = query.name,
            expected,
            actual = ?row_count,
            "Query verification failed"
        );
    }

    Ok(QueryTiming {
        cache_state,
        concurrency,
        duration: elapsed,
        row_count,
        verified,
    })
}

fn build_queries(units: usize) -> Vec<BenchQuery> {
    let total_products = 200 * units;
    let total_orders = 1_000 * units;

    vec![
        BenchQuery {
            name: "count_orders",
            sparql: "SELECT (COUNT(?o) AS ?n) WHERE { ?o a <http://example.org/Order> }"
                .to_string(),
            expected_rows: Some(1), // COUNT returns 1 row
        },
        BenchQuery {
            name: "products_by_mfr",
            sparql: format!(
                "SELECT ?prod ?name WHERE {{ \
                 ?prod <http://example.org/madeBy> <http://example.org/mfr_0_0> ; \
                        <http://schema.org/name> ?name . \
                 }} LIMIT {total_products}"
            ),
            expected_rows: None, // complex to precompute; skip verification
        },
        BenchQuery {
            name: "supply_path",
            sparql: "SELECT ?mfr ?wh ?dist ?ret WHERE { \
                     ?ret a <http://example.org/Retailer> ; \
                          <http://example.org/source> ?dist . \
                     ?dist <http://example.org/suppliedBy> ?wh . \
                     ?wh <http://example.org/operator> ?mfr . \
                     } LIMIT 100"
                .to_string(),
            expected_rows: None,
        },
        BenchQuery {
            name: "order_count_by_status",
            sparql: format!(
                "SELECT ?status (COUNT(?o) AS ?n) WHERE {{ \
                 ?o a <http://example.org/Order> ; \
                    <http://example.org/status> ?status . \
                 }} GROUP BY ?status LIMIT {total_orders}"
            ),
            // 5 statuses -> 5 rows
            expected_rows: Some(5),
        },
    ]
}

fn parse_concurrency(s: &str) -> Vec<usize> {
    s.split(',')
        .filter_map(|v| v.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
        .collect()
}
