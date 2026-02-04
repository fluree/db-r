//! Query execution engine
//!
//! This module provides the query runner that builds operator trees from
//! `ParsedQuery` and executes them with optional solution modifiers.
//!
//! # Architecture
//!
//! The execution pipeline applies operators in this order:
//! ```text
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
//! ```
//!
//! - GROUP BY partitions solutions and creates Grouped values for non-key vars
//! - Aggregates compute COUNT, SUM, AVG, etc. on Grouped values
//! - HAVING filters grouped/aggregated results
//! - ORDER BY before PROJECT because sort may reference vars not in SELECT
//! - DISTINCT applies to projected output (post-select)
//! - OFFSET/LIMIT apply last for pagination
//!
//! # Module Organization
//!
//! The execution engine is split into focused submodules:
//!
//! - `reasoning_prep`: Schema hierarchy, reasoning modes, derived facts
//! - `rewrite_glue`: Pattern rewriting for RDFS/OWL expansion
//! - `pushdown`: Filter bounds extraction for index-level filtering
//! - `where_plan`: WHERE clause operator building
//! - `operator_tree`: Complete operator tree construction
//! - `runner`: Unified execution runner (eliminates duplication)
//!
//! Use `execute_query` for simple execution or build an `ExecutableQuery` with custom `QueryOptions` for full control.

mod operator_tree;
mod pushdown;
mod reasoning_prep;
mod rewrite_glue;
mod runner;
mod where_plan;

// Re-export public types
pub use runner::ContextConfig;
pub use runner::ExecutableQuery;
pub use runner::execute_prepared;

// Re-export internal helpers for use in lib.rs
pub use where_plan::build_where_operators_seeded;

// Re-export operator tree builder and runner for custom execution pipelines
// (e.g., binary index queries in fluree-db-ingest)
pub use operator_tree::build_operator_tree;
pub use runner::run_operator;

// Re-export pushdown utilities for tests
pub use pushdown::{
    count_filter_vars, extract_bounds_from_filters, extract_lookahead_bounds_with_consumption,
    merge_lower_bound, merge_object_bounds, merge_upper_bound,
};

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::dataset::DataSet;
use crate::error::Result;
use crate::ir::Pattern;
use crate::parse::ParsedQuery;
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarRegistry;
use fluree_db_core::{Db, StatsView, Storage, Tracker};
use std::sync::Arc;
use tracing::Instrument;

use reasoning_prep::effective_reasoning_modes;
use rewrite_glue::rewrite_query_patterns;
pub use runner::prepare_execution;
use runner::{
    execute_prepared_with_dataset, execute_prepared_with_dataset_and_bm25,
    execute_prepared_with_dataset_and_policy, execute_prepared_with_dataset_and_policy_and_bm25,
    execute_prepared_with_dataset_and_policy_and_providers, execute_prepared_with_dataset_and_providers,
    execute_prepared_with_dataset_history, execute_prepared_with_overlay,
    execute_prepared_with_overlay_tracked, execute_prepared_with_policy, execute_prepared_with_r2rml,
};

/// Execute a query with full modifier support
///
/// Builds an operator tree from the query and options, executes it,
/// and collects all result batches.
///
/// # Arguments
///
/// * `db` - Database to query
/// * `vars` - Variable registry for name resolution
/// * `query` - Executable query with modifiers
///
/// # Returns
///
/// Vector of result batches. Empty vector if no results.
pub async fn execute<S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
) -> Result<Vec<Batch>> {
    let span = tracing::debug_span!(
        "query_execute",
        db_t = db.t,
        pattern_count = query.query.patterns.len()
    );
    async {
    tracing::debug!("starting query execution");

    let ctx = ExecutionContext::new(db, vars).with_strict_bind_errors();

    // Steps 1-2: Reasoning preparation (hierarchy, modes, ontology)
    let (hierarchy, reasoning, ontology) = async {
        let hierarchy = db.schema_hierarchy();

        // Compute effective reasoning modes (auto-RDFS when hierarchy exists)
        let reasoning = effective_reasoning_modes(&query.options.reasoning, hierarchy.is_some());

        tracing::Span::current().record("rdfs", reasoning.rdfs);
        tracing::Span::current().record("owl2ql", reasoning.owl2ql);
        tracing::Span::current().record("owl2rl", reasoning.owl2rl);

        if reasoning.rdfs || reasoning.owl2ql || reasoning.owl2rl {
            tracing::debug!("reasoning enabled");
        }

        // Build ontology for OWL2-QL mode (if enabled)
        let ontology = if reasoning.owl2ql {
            tracing::debug!("building OWL2-QL ontology");
            Some(crate::rewrite_owl_ql::Ontology::from_db(db, db.t as u64).await?)
        } else {
            None
        };

        Ok::<_, crate::error::QueryError>((hierarchy, reasoning, ontology))
    }
    .instrument(tracing::debug_span!(
        "reasoning_prep",
        rdfs = tracing::field::Empty,
        owl2ql = tracing::field::Empty,
        owl2rl = tracing::field::Empty,
    ))
    .await?;

    // Step 3: Pattern rewriting for reasoning (RDFS/OWL expansion)
    let rewritten_patterns = {
        let span = tracing::debug_span!(
            "pattern_rewrite",
            patterns_before = query.query.patterns.len(),
            patterns_after = tracing::field::Empty,
        );
        let _guard = span.enter();

        fn encode_term<S: Storage + 'static>(db: &Db<S>, t: &Term) -> Term {
            match t {
                Term::Iri(iri) => db
                    .encode_iri(iri)
                    .map(Term::Sid)
                    .unwrap_or_else(|| t.clone()),
                _ => t.clone(),
            }
        }

        fn encode_patterns_for_reasoning<S: Storage + 'static>(
            db: &Db<S>,
            patterns: &[Pattern],
        ) -> Vec<Pattern> {
            patterns
                .iter()
                .map(|p| match p {
                    Pattern::Triple(tp) => Pattern::Triple(TriplePattern {
                        s: encode_term(db, &tp.s),
                        p: encode_term(db, &tp.p),
                        o: encode_term(db, &tp.o),
                        dt: tp.dt.clone(),
                        lang: tp.lang.clone(),
                    }),
                    Pattern::Optional(inner) => {
                        Pattern::Optional(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Union(branches) => Pattern::Union(
                        branches
                            .iter()
                            .map(|b| encode_patterns_for_reasoning(db, b))
                            .collect(),
                    ),
                    Pattern::Minus(inner) => {
                        Pattern::Minus(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Exists(inner) => {
                        Pattern::Exists(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::NotExists(inner) => {
                        Pattern::NotExists(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Graph { name, patterns } => Pattern::Graph {
                        name: name.clone(),
                        patterns: encode_patterns_for_reasoning(db, patterns),
                    },
                    _ => p.clone(),
                })
                .collect()
        }

        let patterns_for_rewrite = if reasoning.rdfs || reasoning.owl2ql {
            encode_patterns_for_reasoning(db, &query.query.patterns)
        } else {
            query.query.patterns.clone()
        };
        let (rewritten_patterns, _diag) = rewrite_query_patterns(
            &patterns_for_rewrite,
            hierarchy,
            &reasoning,
            ontology.as_ref(),
        );

        tracing::Span::current().record("patterns_after", rewritten_patterns.len());

        if rewritten_patterns.len() != query.query.patterns.len() {
            tracing::debug!("patterns rewritten for reasoning");
        }

        rewritten_patterns
    };

    // Build query with rewritten patterns
    let rewritten_query = query.query.with_patterns(rewritten_patterns);

    // Step 4: Build the operator tree (planning)
    let operator = {
        let span = tracing::debug_span!("plan", pattern_count = rewritten_query.patterns.len(),);
        let _guard = span.enter();

        let stats_view = db.stats.as_ref().map(|s| {
            Arc::new(StatsView::from_db_stats_with_namespaces(
                s,
                &db.namespace_codes,
            ))
        });
        build_operator_tree(&rewritten_query, &query.options, stats_view)?
    };

    // Execute: open, drain batches, close
    run_operator(operator, &ctx).await
    }
    .instrument(span)
    .await
}

/// Execute a parsed query with default options
///
/// Convenience function for simple queries without modifiers.
///
/// # Arguments
///
/// * `db` - Database to query
/// * `vars` - Variable registry for name resolution
/// * `query` - Parsed query
///
/// # Returns
///
/// Vector of result batches.
pub async fn execute_query<S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    query: &ParsedQuery,
) -> Result<Vec<Batch>> {
    execute(db, vars, &ExecutableQuery::simple(query.clone())).await
}

/// Execute a query with an overlay
///
/// This is the primary execution path for queries over indexed databases
/// with uncommitted changes (novelty).
///
/// # Arguments
///
/// * `db` - The indexed database to query
/// * `overlay` - Overlay provider (e.g., `Novelty` from `fluree-db-novelty`)
/// * `vars` - Variable registry for name resolution
/// * `query` - Executable query with modifiers
///
/// # Returns
///
/// Vector of result batches.
pub async fn execute_with_overlay<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, i64::MAX).await?;
    execute_prepared_with_overlay(db, vars, overlay, prepared, i64::MAX, None).await
}

/// Execute a query with an overlay and time-travel settings
///
/// Combines overlay support with time-travel queries. The `to_t` parameter
/// limits results to flakes with `t <= to_t`, and the optional `from_t`
/// enables history range queries.
///
/// # Arguments
///
/// * `db` - The indexed database to query
/// * `overlay` - Overlay provider (e.g., `Novelty` from `fluree-db-novelty`)
/// * `vars` - Variable registry for name resolution
/// * `query` - Executable query with modifiers
/// * `to_t` - Upper time bound (inclusive)
/// * `from_t` - Optional lower time bound for history queries
///
/// # Returns
///
/// Vector of result batches.
pub async fn execute_with_overlay_at<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_overlay(db, vars, overlay, prepared, to_t, from_t).await
}

/// Execute a query with an overlay and time-travel settings, with optional tracking.
///
/// This mirrors `execute_with_overlay_at`, but attaches `tracker` to the execution context.
pub async fn execute_with_overlay_at_tracked<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    tracker: &Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_overlay_tracked(db, vars, overlay, prepared, to_t, from_t, Some(tracker))
        .await
}

/// Execute a query with policy enforcement
///
/// This function applies access control policies during query execution,
/// filtering results based on the provided `PolicyContext`.
///
/// # Arguments
///
/// * `db` - Database to query
/// * `overlay` - Overlay provider (novelty)
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Upper time bound (inclusive)
/// * `from_t` - Optional lower time bound for history queries
/// * `policy` - Policy context for access control filtering
///
/// # Returns
///
/// Vector of result batches, filtered by policy.
pub async fn execute_with_policy<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    policy: &fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_policy(db, vars, overlay, prepared, to_t, from_t, policy, None).await
}

/// Execute a query with policy enforcement, with optional tracking.
///
/// This mirrors `execute_with_policy`, but attaches `tracker` to the execution context.
pub async fn execute_with_policy_tracked<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    policy: &fluree_db_policy::PolicyContext,
    tracker: &Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_policy(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        policy,
        Some(tracker),
    )
    .await
}

/// Execute a query with R2RML providers (for virtual graph support).
///
/// This mirrors `execute_with_overlay_at_tracked`, but adds R2RML providers
/// to the execution context for GRAPH patterns that target R2RML virtual graphs.
///
/// # Arguments
///
/// * `db` - Database to query
/// * `overlay` - Overlay provider (novelty)
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Upper time bound (inclusive)
/// * `from_t` - Optional lower time bound for history queries
/// * `tracker` - Execution tracker
/// * `r2rml_provider` - Provider for R2RML mapping lookups
/// * `r2rml_table_provider` - Provider for Iceberg table scanning
pub async fn execute_with_r2rml<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    tracker: &Tracker,
    r2rml_provider: &dyn crate::r2rml::R2rmlProvider,
    r2rml_table_provider: &dyn crate::r2rml::R2rmlTableProvider,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_r2rml(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        tracker,
        r2rml_provider,
        r2rml_table_provider,
    )
    .await
}

/// Execute a query against a dataset (multi-graph query)
///
/// This is the primary execution path for dataset queries, supporting:
/// - Multiple default graphs (unioned at scan level)
/// - Named graphs (accessible via GRAPH patterns)
///
/// # Arguments
///
/// * `db` - Primary database for IRI encoding/decoding
/// * `overlay` - Primary overlay for novelty data
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Time boundary for primary db
/// * `from_t` - Optional start time for range queries
/// * `dataset` - The dataset providing graph sources
///
/// # Returns
///
/// Vector of result batches
pub async fn execute_with_dataset<'a, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset(db, vars, overlay, prepared, to_t, from_t, dataset, None).await
}

/// Execute a query against a dataset (multi-graph), with optional tracking.
///
/// This mirrors `execute_with_dataset`, but attaches `tracker` to the execution context.
pub async fn execute_with_dataset_tracked<'a, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    tracker: &Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        Some(tracker),
    )
    .await
}

/// Execute a query against a dataset in history mode
///
/// History mode enables `@op` bindings that capture whether each flake is an
/// assertion or retraction. Used for unified history queries with time ranges.
pub async fn execute_with_dataset_history<'a, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_history(
        db, vars, overlay, prepared, to_t, from_t, dataset, tracker, true,
    )
    .await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement
///
/// This combines dataset execution (multiple default/named graphs) with access-control
/// filtering via `PolicyContext`, mirroring the behavior of `execute_with_dataset` and
/// `execute_with_policy` together.
pub async fn execute_with_dataset_and_policy<'a, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_policy(
        db, vars, overlay, prepared, to_t, from_t, dataset, policy, None,
    )
    .await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement, with optional tracking.
///
/// This mirrors `execute_with_dataset_and_policy`, but attaches `tracker` to the execution context.
pub async fn execute_with_dataset_and_policy_tracked<
    'a,
    S: Storage + 'static,
>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &fluree_db_policy::PolicyContext,
    tracker: &Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_policy(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        policy,
        Some(tracker),
    )
    .await
}

/// Execute a query against a dataset with BM25 provider (for virtual graph BM25 queries)
///
/// This combines dataset execution (multiple default/named graphs) with BM25 index
/// provider support, enabling `idx:*` patterns in queries to resolve against
/// virtual graph BM25 indexes.
///
/// # Arguments
///
/// * `db` - Primary database for IRI encoding/decoding
/// * `overlay` - Primary overlay for novelty data
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Time boundary for primary db
/// * `from_t` - Optional start time for range queries
/// * `dataset` - The dataset providing graph sources
/// * `bm25_provider` - Provider for BM25 index lookups
/// * `tracker` - Optional execution tracker
pub async fn execute_with_dataset_and_bm25<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_bm25(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        bm25_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with policy enforcement and BM25 provider
///
/// This combines dataset execution (multiple default/named graphs) with policy
/// enforcement and BM25 index provider support, enabling `idx:*` patterns in
/// queries with policy controls.
///
/// # Arguments
///
/// * `db` - Primary database for IRI encoding/decoding
/// * `overlay` - Primary overlay for novelty data
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Time boundary for primary db
/// * `from_t` - Optional start time for range queries
/// * `dataset` - The dataset providing graph sources
/// * `policy` - Policy context for access control
/// * `bm25_provider` - Provider for BM25 index lookups
/// * `tracker` - Optional execution tracker
pub async fn execute_with_dataset_and_policy_and_bm25<
    'a,
    'b,
    S: Storage + 'static,
>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &fluree_db_policy::PolicyContext,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_policy_and_bm25(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        policy,
        bm25_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with both BM25 and vector providers (for virtual graph queries)
///
/// This combines dataset execution (multiple default/named graphs) with both BM25 and
/// vector index provider support, enabling both `idx:search` and `idx:vector` patterns
/// in queries.
///
/// # Arguments
///
/// * `db` - Primary database for IRI encoding/decoding
/// * `overlay` - Primary overlay for novelty data
/// * `vars` - Variable registry
/// * `query` - The query to execute
/// * `to_t` - Time boundary for primary db
/// * `from_t` - Optional start time for range queries
/// * `dataset` - The dataset providing graph sources
/// * `bm25_provider` - Provider for BM25 index lookups
/// * `vector_provider` - Provider for vector similarity search
/// * `tracker` - Optional execution tracker
pub async fn execute_with_dataset_and_providers<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_providers(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        bm25_provider,
        vector_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with policy enforcement and both providers
///
/// This combines dataset execution with policy enforcement and both BM25 and
/// vector index provider support.
pub async fn execute_with_dataset_and_policy_and_providers<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &fluree_db_policy::PolicyContext,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, overlay, query, to_t).await?;
    execute_prepared_with_dataset_and_policy_and_providers(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        dataset,
        policy,
        bm25_provider,
        vector_provider,
        tracker,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{CompareOp, FilterExpr, FilterValue, Pattern};
    use crate::options::QueryOptions;
    use crate::parse::SelectMode;
    use crate::pattern::{Term, TriplePattern};
    use crate::planner::reorder_patterns;
    use crate::sort::SortSpec;
    use crate::var_registry::VarId;
    use fluree_db_core::{Db, FlakeValue, MemoryStorage, PropertyStatData, Sid, StatsView};
    use fluree_graph_json_ld::ParsedContext;
    use where_plan::collect_inner_join_block;

    fn make_test_db() -> Db<MemoryStorage> {
        Db::genesis(MemoryStorage::new(), "test/main")
    }

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Term::Var(s_var),
            Term::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    fn make_simple_query(select: Vec<VarId>, patterns: Vec<Pattern>) -> ParsedQuery {
        ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            select,
            patterns,
            options: QueryOptions::default(),
            select_mode: SelectMode::default(),
            construct_template: None,
            graph_select: None,
        }
    }

    #[tokio::test]
    async fn test_empty_patterns_returns_one_row() {
        let db = make_test_db();
        let vars = VarRegistry::new();

        let query = make_simple_query(vec![], vec![]);
        let results = execute_query(&db, &vars, &query).await.unwrap();

        // Empty WHERE returns 1 batch with a single empty solution
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].schema().len(), 0); // Empty schema
        assert_eq!(results[0].len(), 1);
    }

    #[tokio::test]
    async fn test_query_options_builder() {
        let opts = QueryOptions::new()
            .with_limit(10)
            .with_offset(5)
            .with_distinct()
            .with_order_by(vec![SortSpec::asc(VarId(0))]);

        assert_eq!(opts.limit, Some(10));
        assert_eq!(opts.offset, Some(5));
        assert!(opts.distinct);
        assert_eq!(opts.order_by.len(), 1);
        assert!(opts.has_modifiers());
    }

    #[tokio::test]
    async fn test_query_options_default_no_modifiers() {
        let opts = QueryOptions::default();
        assert!(!opts.has_modifiers());
    }

    #[test]
    fn test_build_operator_tree_validates_select_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            select: vec![VarId(99)], // Variable not in pattern
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            select_mode: SelectMode::default(),
            construct_template: None,
            graph_select: None,
        };

        let result =
            build_operator_tree::<MemoryStorage>(&query, &QueryOptions::default(), None);
        match result {
            Err(e) => assert!(e.to_string().contains("not found")),
            Ok(_) => panic!("Expected error for invalid select var"),
        }
    }

    #[test]
    fn test_build_operator_tree_validates_sort_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            select: vec![VarId(0)],
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            select_mode: SelectMode::default(),
            construct_template: None,
            graph_select: None,
        };

        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(99))]); // Invalid var

        let result = build_operator_tree::<MemoryStorage>(&query, &options, None);
        match result {
            Err(e) => assert!(e.to_string().contains("Sort variable")),
            Ok(_) => panic!("Expected error for invalid sort var"),
        }
    }

    #[test]
    fn test_build_where_operators_single_triple() {
        let patterns = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];

        let result = where_plan::build_where_operators::<MemoryStorage>(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_where_operators_with_filter() {
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Filter(FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(1))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            }),
        ];

        let result = where_plan::build_where_operators::<MemoryStorage>(&patterns, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_collects_and_reorders_triples_across_safe_filter_boundary_with_stats() {
        let score = VarId(0);
        let score_v = VarId(1);
        let concept = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(score, "hasScore", score_v)),
            Pattern::Filter(FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(score_v)),
                right: Box::new(FilterExpr::Const(FilterValue::Double(0.4))),
            }),
            Pattern::Triple(TriplePattern::new(
                Term::Var(score),
                Term::Sid(Sid::new(100, "refersInstance")),
                Term::Var(concept),
            )),
            Pattern::Triple(TriplePattern::new(
                Term::Var(concept),
                Term::Sid(Sid::new(100, "notation")),
                Term::Value(FlakeValue::String("LVL1".to_string())),
            )),
        ];

        let (end, values, triples, binds, filters) = collect_inner_join_block(&patterns, 0);
        assert_eq!(end, patterns.len(), "block should consume all patterns");
        assert_eq!(values.len(), 0, "expected 0 VALUES in the block");
        assert_eq!(binds.len(), 0, "expected 0 BINDs in the block");
        assert_eq!(triples.len(), 3, "expected 3 triples in the block");
        assert_eq!(filters.len(), 1, "expected 1 filter in the block");

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "notation"),
            PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000,
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "hasScore"),
            PropertyStatData {
                count: 1_000_000_000,
                ndv_values: 900_000_000,
                ndv_subjects: 900_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "refersInstance"),
            PropertyStatData {
                count: 800_000_000,
                ndv_values: 700_000_000,
                ndv_subjects: 700_000_000,
            },
        );

        let ordered = reorder_patterns(triples, Some(&stats));
        let first_pred = ordered[0].p.as_sid().expect("predicate should be Sid");
        assert_eq!(
            &*first_pred.name, "notation",
            "expected optimizer to start from the most selective triple"
        );
    }

    #[test]
    fn test_extract_lookahead_bounds_simple_range() {
        let triples = vec![make_pattern(VarId(0), "age", VarId(1))];
        let remaining = vec![Pattern::Filter(FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(1))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(1))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(65))),
            },
        ]))];

        let (bounds, _consumed) = extract_lookahead_bounds_with_consumption(&triples, &remaining);

        assert!(bounds.contains_key(&VarId(1)));
        let obj_bounds = bounds.get(&VarId(1)).unwrap();

        assert!(obj_bounds.lower.is_some());
        let (lower_val, lower_inclusive) = obj_bounds.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(18));
        assert!(!lower_inclusive);

        assert!(obj_bounds.upper.is_some());
        let (upper_val, upper_inclusive) = obj_bounds.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(65));
        assert!(!upper_inclusive);
    }

    #[test]
    fn test_merge_lower_bound_takes_higher_value() {
        let a = Some((FlakeValue::Long(10), false));
        let b = Some((FlakeValue::Long(20), false));

        let merged = merge_lower_bound(a.as_ref(), b.as_ref());

        assert!(merged.is_some());
        let (val, _inclusive) = merged.unwrap();
        assert_eq!(val, FlakeValue::Long(20));
    }

    #[test]
    fn test_merge_object_bounds_full() {
        use fluree_db_core::ObjectBounds;

        let a = ObjectBounds {
            lower: Some((FlakeValue::Long(10), false)),
            upper: Some((FlakeValue::Long(100), true)),
        };
        let b = ObjectBounds {
            lower: Some((FlakeValue::Long(20), true)),
            upper: Some((FlakeValue::Long(80), false)),
        };

        let merged = merge_object_bounds(&a, &b);

        let (lower_val, _) = merged.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(20));

        let (upper_val, _) = merged.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(80));
    }
}
