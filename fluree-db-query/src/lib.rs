//! # Fluree DB Query
//!
//! Query execution engine for Fluree DB.
//!
//! This crate provides:
//! - Columnar batch-based execution model
//! - Operator trait with `open/next_batch/close` lifecycle
//! - ScanOperator for triple-pattern evaluation (binary cursor + range fallback)
//! - Variable registry for compact binding indices
//!
//! ## Quick Start
//!
//! Build a `TriplePattern` with a `VarRegistry`, then call `execute_pattern` with a `GraphDbRef` to get result batches.

pub mod aggregate;
pub mod binary_range;
pub mod binary_scan;
pub mod bind;
pub mod binding;
pub mod bm25;
pub mod context;
pub mod datalog_rules;
pub mod dataset;
pub mod dict_overlay;
pub mod distinct;
pub mod error;
pub mod execute;
pub mod exists;
pub mod explain;
pub mod expression;
pub mod filter;
pub mod geo_rewrite;
pub mod geo_search;
pub mod graph;
pub mod group_aggregate;
pub mod groupby;
pub mod having;
pub mod ir;
pub mod join;
pub mod limit;
pub mod materializer;
pub mod minus;
pub mod offset;
pub mod operator;
pub mod optional;
pub mod options;
pub mod parse;
pub mod pattern;
pub mod planner;
pub mod policy;
pub mod project;
pub mod property_join;
pub mod property_join_count_all;
pub mod property_path;
pub mod r2rml;
pub mod reasoning;
pub mod rewrite;
pub mod rewrite_owl_ql;
pub mod s2_search;
pub mod seed;
pub mod service;
pub mod sort;
pub mod stats_query;
pub mod subquery;
pub mod union;
pub mod values;
pub mod var_registry;
pub mod vector;

// Re-exports
pub use aggregate::{apply_aggregate, AggregateFn, AggregateOperator, AggregateSpec};
pub use binary_range::BinaryRangeProvider;
pub use binary_scan::{BinaryScanOperator, ScanOperator};
pub use bind::BindOperator;
pub use binding::{Batch, BatchError, BatchView, Binding, RowAccess, RowView};
pub use context::{ExecutionContext, WellKnownDatatypes};
pub use dataset::{ActiveGraph, ActiveGraphs, DataSet, GraphRef};
pub use distinct::DistinctOperator;
pub use error::{QueryError, Result};
pub use execute::{
    build_operator_tree, execute_with_dataset, execute_with_dataset_and_bm25,
    execute_with_dataset_and_policy, execute_with_dataset_and_policy_and_bm25,
    execute_with_dataset_and_policy_and_providers, execute_with_dataset_and_policy_tracked,
    execute_with_dataset_and_providers, execute_with_dataset_history, execute_with_dataset_tracked,
    execute_with_overlay, execute_with_overlay_tracked, execute_with_policy,
    execute_with_policy_tracked, execute_with_r2rml, run_operator, ExecutableQuery,
    QueryContextParams,
};
pub use exists::ExistsOperator;
pub use explain::{
    explain_patterns, ExplainPlan, FallbackReason, OptimizationStatus, PatternDisplay,
    SelectivityInputs,
};
pub use filter::FilterOperator;
pub use geo_rewrite::rewrite_geo_patterns;
pub use graph::GraphOperator;
pub use group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
pub use groupby::GroupByOperator;
pub use having::HavingOperator;
pub use ir::{
    CompareOp, Expression, FilterValue, Function, PathModifier, Pattern, PropertyPathPattern,
    Query, R2rmlPattern, ServiceEndpoint, ServicePattern, SubqueryPattern,
};
pub use join::{BindInstruction, NestedLoopJoinOperator, PatternPosition, UnifyInstruction};
pub use limit::LimitOperator;
pub use materializer::{ComparableValue, JoinKey, Materializer};
pub use minus::MinusOperator;
pub use offset::OffsetOperator;
pub use operator::{BoxedOperator, Operator, OperatorState};
pub use optional::OptionalOperator;
pub use options::QueryOptions;
pub use pattern::{Term, TriplePattern};
pub use planner::{
    extract_object_bounds_for_var, extract_range_constraints, is_property_join, PatternType,
    RangeConstraint, RangeValue,
};
pub use policy::{QueryPolicyEnforcer, QueryPolicyExecutor};
pub use project::ProjectOperator;
pub use property_join::PropertyJoinOperator;
pub use property_join_count_all::PropertyJoinCountAllOperator;
pub use property_path::{PropertyPathOperator, DEFAULT_MAX_VISITED};
pub use r2rml::{NoOpR2rmlProvider, R2rmlProvider, R2rmlScanOperator, R2rmlTableProvider};
pub use reasoning::{global_reasoning_cache, ReasoningOverlay};
pub use rewrite::{
    rewrite_patterns, Diagnostics as RewriteDiagnostics, PlanContext, PlanLimits, ReasoningModes,
};
pub use rewrite_owl_ql::{rewrite_owl_ql_patterns, Ontology, OwlQlContext};
pub use seed::{EmptyOperator, SeedOperator};
pub use sort::{compare_bindings, compare_flake_values, SortDirection, SortOperator, SortSpec};
pub use stats_query::StatsCountByPredicateOperator;
pub use subquery::SubqueryOperator;
pub use union::UnionOperator;
pub use values::ValuesOperator;

// Re-export from fluree-db-core for convenience
pub use fluree_db_core::ObjectBounds;
pub use var_registry::{VarId, VarRegistry};

// Re-export parse types for query parsing
pub use parse::{parse_query, ParsedQuery, SelectMode};

use execute::build_where_operators_seeded;
use fluree_db_core::GraphDbRef;
use std::sync::Arc;

/// Execute a single triple pattern query
///
/// Returns all batches of results for the pattern.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot + graph id + overlay + as-of time)
/// * `vars` - Variable registry containing the pattern's variables
/// * `pattern` - Triple pattern to match
///
pub async fn execute_pattern(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref(db, vars);
    let mut scan = ScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern and collect all results into a single batch
///
/// Convenience function when you want all results at once.
pub async fn execute_pattern_all(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Option<Batch>> {
    let batches = execute_pattern(db, vars, pattern).await?;

    if batches.is_empty() {
        return Ok(None);
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next());
    }

    // Merge multiple batches into one
    let schema_vec = batches[0].schema().to_vec();
    let schema: Arc<[VarId]> = Arc::from(schema_vec.into_boxed_slice());
    let num_cols = schema.len();

    // Merge columns across all batches
    let columns: Vec<Vec<Binding>> = (0..num_cols)
        .map(|col_idx| {
            batches
                .iter()
                .filter_map(|batch| batch.column_by_idx(col_idx))
                .flat_map(|src_col| src_col.iter().cloned())
                .collect()
        })
        .collect();

    Ok(Some(Batch::new(schema, columns)?))
}

/// Execute a pattern with a `GraphDbRef` and an optional `from_t` for history queries.
///
/// `to_t` comes from `db.t`; `from_t` is the lower time bound for history.
pub async fn execute_pattern_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    let mut scan = ScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern against a `GraphDbRef`.
///
/// The `db` bundles snapshot, graph id, overlay (novelty), and as-of time.
/// This replaces the old `execute_pattern_with_overlay` and
/// `execute_pattern_with_overlay_at` functions.
pub async fn execute_pattern_with_overlay(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref(db, vars);
    let mut scan = ScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern with a `GraphDbRef` and an optional `from_t` for history queries.
///
/// `to_t` comes from `db.t`; `from_t` is the lower time bound for history.
pub async fn execute_pattern_with_overlay_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    let mut scan = ScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute WHERE patterns with overlay and time-travel support
///
/// This is the entry point for transaction WHERE clause execution.
/// Returns all matching bindings as batches.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot + graph id + overlay + as-of time)
/// * `vars` - Variable registry for the patterns
/// * `patterns` - WHERE patterns to execute
/// * `from_t` - Optional lower time bound for history queries
///
/// # Returns
///
/// Vector of result batches. If patterns is empty, returns a single batch
/// with one empty solution (row with no columns).
///
pub async fn execute_where_with_overlay_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    if patterns.is_empty() {
        // Empty WHERE = single empty solution (one row, zero columns)
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(vec![Batch::empty(schema)?]);
    }

    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    let mut operator = build_where_operators_seeded(None, patterns, None)?;

    operator.open(&ctx).await?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch(&ctx).await? {
        batches.push(batch);
    }
    operator.close();

    Ok(batches)
}

/// Execute WHERE patterns with strict bind error handling.
pub async fn execute_where_with_overlay_at_strict(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    if patterns.is_empty() {
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(vec![Batch::empty(schema)?]);
    }

    let ctx =
        ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t).with_strict_bind_errors();
    let mut operator = build_where_operators_seeded(None, patterns, None)?;

    operator.open(&ctx).await?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch(&ctx).await? {
        batches.push(batch);
    }
    operator.close();

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_api() {
        // Ensure public API is accessible
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");

        let _pattern = TriplePattern::new(
            Term::Var(s),
            Term::Sid(fluree_db_core::Sid::new(100, "name")),
            Term::Var(o),
        );
    }
}
