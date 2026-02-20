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
pub use runner::execute_prepared;
pub use runner::ContextConfig;
pub use runner::ExecutableQuery;
pub use runner::QueryContextParams;

// Re-export internal helpers for use in lib.rs
pub use where_plan::build_where_operators_seeded;

// Re-export operator tree builder and runner for custom execution pipelines
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
use fluree_db_core::{Db, StatsView, Tracker};
use std::sync::Arc;
use tracing::Instrument;

/// Data source for query execution.
///
/// Describes the data being queried: the database, overlay (novelty layer),
/// and time bounds for the query.
#[derive(Clone, Copy)]
pub struct DataSource<'a> {
    /// The database to query
    pub db: &'a Db,
    /// Overlay provider for novelty data
    pub overlay: &'a dyn fluree_db_core::OverlayProvider,
    /// Upper time bound (inclusive)
    pub to_t: i64,
    /// Optional lower time bound for history/range queries
    pub from_t: Option<i64>,
}

impl<'a> DataSource<'a> {
    /// Create a new data source with no lower time bound.
    pub fn new(db: &'a Db, overlay: &'a dyn fluree_db_core::OverlayProvider, to_t: i64) -> Self {
        Self {
            db,
            overlay,
            to_t,
            from_t: None,
        }
    }

    /// Create a new data source with a time range.
    pub fn with_range(
        db: &'a Db,
        overlay: &'a dyn fluree_db_core::OverlayProvider,
        to_t: i64,
        from_t: i64,
    ) -> Self {
        Self {
            db,
            overlay,
            to_t,
            from_t: Some(from_t),
        }
    }
}

use reasoning_prep::effective_reasoning_modes;
use rewrite_glue::rewrite_query_patterns;
pub use runner::prepare_execution;
use runner::{
    execute_prepared_with_dataset, execute_prepared_with_dataset_and_bm25,
    execute_prepared_with_dataset_and_policy, execute_prepared_with_dataset_and_policy_and_bm25,
    execute_prepared_with_dataset_and_policy_and_providers,
    execute_prepared_with_dataset_and_providers, execute_prepared_with_dataset_history,
    execute_prepared_with_overlay, execute_prepared_with_overlay_tracked,
    execute_prepared_with_policy, execute_prepared_with_r2rml,
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
pub async fn execute(db: &Db, vars: &VarRegistry, query: &ExecutableQuery) -> Result<Vec<Batch>> {
    let span = tracing::debug_span!(
        "query_execute",
        db_t = db.t,
        pattern_count = query.query.patterns.len()
    );
    // Use an async block with .instrument() so the span is NOT held
    // across .await via a thread-local guard (which would cause cross-request
    // trace contamination in tokio's multi-threaded runtime).
    async move {
        tracing::debug!("starting query execution");

        let ctx = ExecutionContext::new(db, vars).with_strict_bind_errors();
        let hierarchy = db.schema_hierarchy();

        // Compute effective reasoning modes (auto-RDFS when hierarchy exists)
        let reasoning = effective_reasoning_modes(&query.options.reasoning, hierarchy.is_some());

        if reasoning.rdfs || reasoning.owl2ql || reasoning.owl2rl {
            tracing::debug!(
                rdfs = reasoning.rdfs,
                owl2ql = reasoning.owl2ql,
                owl2rl = reasoning.owl2rl,
                "reasoning enabled"
            );
        }

        // Build ontology for OWL2-QL mode (if enabled)
        let ontology = if reasoning.owl2ql {
            tracing::debug!("building OWL2-QL ontology");
            Some(crate::rewrite_owl_ql::Ontology::from_db(db, db.t as u64).await?)
        } else {
            None
        };

        // Apply pattern rewriting for reasoning (RDFS/OWL expansion)
        fn encode_term(db: &Db, t: &Term) -> Term {
            match t {
                Term::Iri(iri) => db
                    .encode_iri(iri)
                    .map(Term::Sid)
                    .unwrap_or_else(|| t.clone()),
                _ => t.clone(),
            }
        }

        fn encode_patterns_for_reasoning(db: &Db, patterns: &[Pattern]) -> Vec<Pattern> {
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

        if rewritten_patterns.len() != query.query.patterns.len() {
            tracing::debug!(
                original_count = query.query.patterns.len(),
                rewritten_count = rewritten_patterns.len(),
                "patterns rewritten for reasoning"
            );
        }

        // Build query with rewritten patterns
        let rewritten_query = query.query.with_patterns(rewritten_patterns);

        // Build stats view for selectivity-based optimization
        //
        // Note: lowering may keep IRIs as `Term::Iri` (cross-ledger). Build an IRI-keyed
        // stats view so planning can still consult stats for IRI predicates.
        let stats_view = db.stats.as_ref().map(|s| {
            Arc::new(StatsView::from_db_stats_with_namespaces(
                s,
                &db.namespace_codes,
            ))
        });

        // Build the operator tree
        tracing::debug!("building operator tree");
        let operator = build_operator_tree(&rewritten_query, &query.options, stats_view)?;

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
pub async fn execute_query(db: &Db, vars: &VarRegistry, query: &ParsedQuery) -> Result<Vec<Batch>> {
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
pub async fn execute_with_overlay(
    source: DataSource<'_>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_overlay(source, vars, prepared).await
}

/// Execute a query with an overlay and time-travel settings, with optional tracking.
///
/// This mirrors `execute_with_overlay`, but attaches `tracker` to the execution context.
pub async fn execute_with_overlay_tracked<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_overlay_tracked(source, vars, prepared, tracker).await
}

/// Execute a query with policy enforcement
///
/// This function applies access control policies during query execution,
/// filtering results based on the provided `PolicyContext`.
pub async fn execute_with_policy<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    policy: &'a fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_policy(source, vars, prepared, policy, None).await
}

/// Execute a query with policy enforcement, with optional tracking.
///
/// This mirrors `execute_with_policy`, but attaches `tracker` to the execution context.
pub async fn execute_with_policy_tracked(
    source: DataSource<'_>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    policy: &fluree_db_policy::PolicyContext,
    tracker: &Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_policy(source, vars, prepared, policy, Some(tracker)).await
}

/// Execute a query with R2RML providers (for graph source support).
pub async fn execute_with_r2rml<'a, 'b>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    tracker: &'a Tracker,
    r2rml_provider: &'b dyn crate::r2rml::R2rmlProvider,
    r2rml_table_provider: &'b dyn crate::r2rml::R2rmlTableProvider,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_r2rml(
        source,
        vars,
        prepared,
        tracker,
        r2rml_provider,
        r2rml_table_provider,
    )
    .await
}

/// Execute a query against a dataset (multi-graph query)
pub async fn execute_with_dataset<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset(source, vars, prepared, dataset, None).await
}

/// Execute a query against a dataset (multi-graph), with optional tracking.
pub async fn execute_with_dataset_tracked<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    tracker: &'a Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset(source, vars, prepared, dataset, Some(tracker)).await
}

/// Execute a query against a dataset in history mode
pub async fn execute_with_dataset_history<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_history(source, vars, prepared, dataset, tracker, true).await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement
pub async fn execute_with_dataset_and_policy<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_policy(source, vars, prepared, dataset, policy, None).await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement, with optional tracking.
pub async fn execute_with_dataset_and_policy_tracked<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: &'a Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_policy(source, vars, prepared, dataset, policy, Some(tracker))
        .await
}

/// Execute a query against a dataset with BM25 provider (for graph source BM25 queries)
///
/// This combines dataset execution (multiple default/named graphs) with BM25 index
/// provider support, enabling `f:searchText` patterns in queries to resolve against
/// graph source BM25 indexes.
pub async fn execute_with_dataset_and_bm25<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    bm25_provider: &dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_bm25(source, vars, prepared, dataset, bm25_provider, tracker)
        .await
}

/// Execute a query against a dataset with policy enforcement and BM25 provider
///
/// This combines dataset execution (multiple default/named graphs) with policy
/// enforcement and BM25 index provider support, enabling `f:searchText` patterns in
/// queries with policy controls.
pub async fn execute_with_dataset_and_policy_and_bm25<'a>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
    bm25_provider: &dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_policy_and_bm25(
        source,
        vars,
        prepared,
        dataset,
        policy,
        bm25_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with both BM25 and vector providers (for graph source queries)
///
/// This combines dataset execution (multiple default/named graphs) with both BM25 and
/// vector index provider support, enabling both `f:searchText` and `f:queryVector` patterns
/// in queries.
pub async fn execute_with_dataset_and_providers<'a, 'b>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_providers(
        source,
        vars,
        prepared,
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
pub async fn execute_with_dataset_and_policy_and_providers<'a, 'b>(
    source: DataSource<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    params: QueryContextParams<'a, 'b>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(source.db, source.overlay, query, source.to_t).await?;
    execute_prepared_with_dataset_and_policy_and_providers(source, vars, prepared, params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::options::QueryOptions;
    use crate::parse::SelectMode;
    use crate::pattern::{Term, TriplePattern};
    use crate::planner::reorder_patterns;
    use crate::sort::SortSpec;
    use crate::var_registry::VarId;
    use fluree_db_core::{Db, FlakeValue, PropertyStatData, Sid, StatsView};
    use fluree_graph_json_ld::ParsedContext;
    use where_plan::collect_inner_join_block;

    fn make_test_db() -> Db {
        Db::genesis("test/main")
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

        let result = build_operator_tree(&query, &QueryOptions::default(), None);
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

        let result = build_operator_tree(&query, &options, None);
        match result {
            Err(e) => assert!(e.to_string().contains("Sort variable")),
            Ok(_) => panic!("Expected error for invalid sort var"),
        }
    }

    #[test]
    fn test_build_where_operators_single_triple() {
        let patterns = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];

        let result = where_plan::build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_where_operators_with_filter() {
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(18)),
            )),
        ];

        let result = where_plan::build_where_operators(&patterns, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_collects_and_reorders_triples_across_safe_filter_boundary_with_stats() {
        let score = VarId(0);
        let score_v = VarId(1);
        let concept = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(score, "hasScore", score_v)),
            Pattern::Filter(Expression::gt(
                Expression::Var(score_v),
                Expression::Const(FilterValue::Double(0.4)),
            )),
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

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "block should consume all patterns"
        );
        assert_eq!(block.values.len(), 0, "expected 0 VALUES in the block");
        assert_eq!(block.binds.len(), 0, "expected 0 BINDs in the block");
        assert_eq!(block.triples.len(), 3, "expected 3 triples in the block");
        assert_eq!(block.filters.len(), 1, "expected 1 filter in the block");

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

        let ordered = reorder_patterns(block.triples, Some(&stats));
        let first_pred = ordered[0].p.as_sid().expect("predicate should be Sid");
        assert_eq!(
            &*first_pred.name, "notation",
            "expected optimizer to start from the most selective triple"
        );
    }

    #[test]
    fn test_extract_lookahead_bounds_simple_range() {
        let triples = vec![make_pattern(VarId(0), "age", VarId(1))];
        let remaining = vec![Pattern::Filter(Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(65)),
            ),
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
