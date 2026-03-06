//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::aggregate::AggregateFn;
use crate::aggregate::AggregateOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::fast_group_count_firsts::{
    PredicateGroupCountFirstsOperator, PredicateObjectCountFirstsOperator,
};
use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::ir::Pattern;
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::{ParsedQuery, QueryOutput};
use crate::project::ProjectOperator;
use crate::sort::{SortDirection, SortOperator};
use crate::stats_query::StatsCountByPredicateOperator;
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;
use crate::PropertyJoinCountAllOperator;
use fluree_db_core::StatsView;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::dependency::{compute_variable_deps, VariableDeps};
use super::where_plan::build_where_operators_with_needed;
use super::where_plan::collect_var_stats;

fn detect_partitioned_group_by(query: &ParsedQuery, options: &QueryOptions) -> bool {
    if options.group_by.len() != 1 {
        return false;
    }
    let gb = options.group_by[0];

    // Strict: only a single triple pattern plus order-preserving operators (FILTER/BIND).
    let mut triple: Option<&TriplePattern> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => {
                if triple.is_some() {
                    return false;
                }
                triple = Some(tp);
            }
            Pattern::Filter(_) | Pattern::Bind { .. } => {}
            _ => return false,
        }
    }
    let Some(tp) = triple else {
        return false;
    };

    // Must be ?s <p> ?o and group key must be either ?s or ?o.
    if !tp.p_bound() {
        return false;
    }
    let Ref::Var(sv) = &tp.s else {
        return false;
    };
    let Term::Var(ov) = &tp.o else {
        return false;
    };
    gb == *sv || gb == *ov
}

fn detect_predicate_group_by_object_count_topk(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, VarId, VarId, VarId, usize)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean
    ) {
        return None;
    }
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let pred = match &tp.p {
        Ref::Sid(_) | Ref::Iri(_) => tp.p.clone(),
        _ => return None,
    };
    if tp.dtc.is_some() {
        return None;
    }
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let Term::Var(o_var) = &tp.o else {
        return None;
    };

    // GROUP BY ?object
    if options.group_by.len() != 1 || options.group_by[0] != *o_var {
        return None;
    }
    // Exactly one COUNT aggregate on ?subject (or COUNT(*) which is equivalent here).
    if options.aggregates.len() != 1 {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct {
        return None;
    }
    if !matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll) {
        return None;
    }
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(*s_var) {
        return None;
    }
    if options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }
    // ORDER BY DESC(?count) and LIMIT k required so we can do top-k directly.
    let limit = options.limit?;
    let [ob] = options.order_by.as_slice() else {
        return None;
    };
    if ob.var != agg.output_var || ob.direction != SortDirection::Descending {
        return None;
    }
    Some((pred, *s_var, *o_var, agg.output_var, limit))
}

fn detect_predicate_object_count(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, VarId, Term, VarId)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };

    // Must be ?s <p> <o> (subject var, predicate bound, object bound).
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let pred = match &tp.p {
        Ref::Sid(_) | Ref::Iri(_) => tp.p.clone(),
        _ => return None,
    };
    if matches!(tp.o, Term::Var(_)) {
        return None;
    }
    // Loose semantics only (no explicit dt/lang constraint).
    if tp.dtc.is_some() {
        return None;
    }
    // No GROUP BY.
    if !options.group_by.is_empty() {
        return None;
    }
    // Exactly one COUNT aggregate on ?s (or COUNT(*) which is equivalent here).
    if options.aggregates.len() != 1 {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct {
        return None;
    }
    if !matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll) {
        return None;
    }
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(*s_var) {
        return None;
    }
    if options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }

    // SELECT must be exactly the count output var.
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((pred, *s_var, tp.o.clone(), agg.output_var))
}

/// Detect if this is a stats fast-path query: `SELECT ?p (COUNT(?x) as ?c) WHERE { ?s ?p ?o } GROUP BY ?p`
///
/// Returns `Some((predicate_var, count_output_var))` if the query matches the pattern.
fn detect_stats_count_by_predicate(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(VarId, VarId)> {
    // Must have stats available (checked by caller)
    // Must have exactly one triple pattern with all variables
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };

    // All three positions must be variables
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let Ref::Var(p_var) = &tp.p else {
        return None;
    };
    let Term::Var(o_var) = &tp.o else {
        return None;
    };

    // GROUP BY must be exactly the predicate variable
    if options.group_by.len() != 1 || options.group_by[0] != *p_var {
        return None;
    }

    // Must have exactly one COUNT aggregate (not COUNT(*))
    if options.aggregates.len() != 1 {
        return None;
    }
    let agg = &options.aggregates[0];
    if !matches!(agg.function, AggregateFn::Count) {
        return None;
    }

    // COUNT input must be a non-predicate variable (subject or object)
    let input_var = agg.input_var?;
    if input_var != *s_var && input_var != *o_var {
        return None;
    }

    // No HAVING (for simplicity)
    if options.having.is_some() {
        return None;
    }

    // No post_binds (for simplicity)
    if !options.post_binds.is_empty() {
        return None;
    }

    tracing::debug!(
        predicate_var = ?p_var,
        count_var = ?agg.output_var,
        "detected stats count-by-predicate fast-path"
    );

    Some((*p_var, agg.output_var))
}

/// Detect a same-subject, N-predicate COUNT(*) query that can be answered without
/// materializing the cartesian join rows.
///
/// Matches:
/// - all patterns are triple patterns
/// - all share the same subject var
/// - all predicates are bound (Sid/Iri)
/// - all objects are vars (and object vars are distinct)
/// - SELECT is exactly the COUNT(*) output var
/// - options: one CountAll aggregate, no GROUP BY / HAVING / post-binds
///
/// Returns `(subject_var, predicate/object pairs, count_var)`.
#[allow(clippy::type_complexity)]
fn detect_property_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(VarId, Vec<(Ref, VarId)>, VarId)> {
    let select_vars = query.output.select_vars()?;
    if query.patterns.len() < 2 {
        return None;
    }

    // Must have exactly one COUNT(*) aggregate and no grouping.
    if !options.group_by.is_empty() || options.aggregates.len() != 1 {
        return None;
    }
    let agg = &options.aggregates[0];
    if !matches!(agg.function, AggregateFn::CountAll) || agg.input_var.is_some() {
        return None;
    }
    if options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }

    // SELECT must be exactly the aggregate output var (avoid surprising projections).
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    let mut subject_var: Option<VarId> = None;
    let mut preds: Vec<(Ref, VarId)> = Vec::with_capacity(query.patterns.len());
    let mut seen_obj: HashSet<VarId> = HashSet::new();

    for p in &query.patterns {
        let Pattern::Triple(tp) = p else {
            return None;
        };

        // Subject must be a variable, shared across all patterns.
        let Ref::Var(s) = &tp.s else {
            return None;
        };
        match subject_var {
            None => subject_var = Some(*s),
            Some(existing) if existing != *s => return None,
            Some(_) => {}
        }

        // Predicates must be bound, objects must be vars, and no dt/lang constraints.
        let pred = match &tp.p {
            Ref::Sid(_) | Ref::Iri(_) => tp.p.clone(),
            _ => return None,
        };
        let Term::Var(o) = &tp.o else {
            return None;
        };
        if tp.dtc.is_some() {
            return None;
        }
        // Object vars must be distinct; otherwise join is not a cartesian product.
        if !seen_obj.insert(*o) {
            return None;
        }
        if subject_var == Some(*o) {
            return None;
        }

        preds.push((pred, *o));
    }

    Some((subject_var?, preds, agg.output_var))
}

/// Apply ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT without validation.
///
/// Used by fast-path operators that skip grouping/aggregation.
fn apply_fast_path_tail(
    mut operator: BoxedOperator,
    query: &ParsedQuery,
    options: &QueryOptions,
) -> BoxedOperator {
    if !options.order_by.is_empty() {
        operator = Box::new(SortOperator::new(operator, options.order_by.clone()));
    }
    if let Some(vars) = query.output.select_vars().filter(|v| !v.is_empty()) {
        operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
    }
    if options.distinct {
        operator = Box::new(DistinctOperator::new(operator));
    }
    if let Some(offset) = options.offset.filter(|&o| o > 0) {
        operator = Box::new(OffsetOperator::new(operator, offset));
    }
    if let Some(limit) = options.limit {
        operator = Box::new(LimitOperator::new(operator, limit));
    }
    operator
}

/// Try all fast-path optimizations, returning `Some` if one matches.
fn try_fast_path(
    query: &ParsedQuery,
    options: &QueryOptions,
    stats: &Option<Arc<StatsView>>,
) -> Option<Result<BoxedOperator>> {
    // `?s <p> ?o GROUP BY ?o COUNT(?s)` top-k using leaflet FIRST headers.
    if let Some((pred, s_var, o_var, count_var, limit)) =
        detect_predicate_group_by_object_count_topk(query, options)
    {
        return Some(Ok(Box::new(PredicateGroupCountFirstsOperator::new(
            s_var,
            o_var,
            count_var,
            pred,
            limit,
            stats.clone(),
        ))));
    }

    // `SELECT (COUNT(?s) AS ?c) WHERE { ?s <p> <o> }` using leaflet FIRST headers.
    if let Some((pred, s_var, obj, count_var)) = detect_predicate_object_count(query, options) {
        let op = Box::new(PredicateObjectCountFirstsOperator::new(
            pred,
            s_var,
            obj,
            count_var,
            stats.clone(),
        ));
        return Some(Ok(apply_fast_path_tail(op, query, options)));
    }

    // Stats-based count-by-predicate: answer directly from IndexStats.
    if let Some(ref stats_view) = stats {
        if let Some((pred_var, count_var)) = detect_stats_count_by_predicate(query, options) {
            let op = Box::new(StatsCountByPredicateOperator::new(
                Arc::clone(stats_view),
                pred_var,
                count_var,
            ));
            return Some(Ok(apply_fast_path_tail(op, query, options)));
        }
    }

    // Same-subject N-predicate COUNT(*) without join-row materialization.
    // COUNT(*) = sum_s Π_i count_pi(s)
    if let Some((s, preds, count_var)) = detect_property_join_count_all(query, options) {
        tracing::debug!("detected property-join COUNT(*) fast-path");
        let op = Box::new(PropertyJoinCountAllOperator::new(s, preds, count_var));
        return Some(Ok(apply_fast_path_tail(op, query, options)));
    }

    None
}

/// Build GROUP BY + Aggregate operators, choosing streaming vs traditional path.
///
/// Streaming (O(groups) memory) is used when all aggregates support it and the
/// SELECT clause doesn't need grouped (non-key, non-aggregate) variables.
fn build_grouping_operators(
    mut operator: BoxedOperator,
    query: &ParsedQuery,
    options: &QueryOptions,
    variable_deps: &Option<VariableDeps>,
) -> Result<BoxedOperator> {
    let current_schema = operator.schema().to_vec();

    // Validate group vars exist in schema
    for var in &options.group_by {
        if !current_schema.contains(var) {
            return Err(QueryError::VariableNotFound(format!(
                "GROUP BY variable {:?} not found in query schema",
                var
            )));
        }
    }

    // Validate aggregates
    let group_by_set: HashSet<VarId> = options.group_by.iter().copied().collect();
    let mut seen_output_vars: HashSet<VarId> = HashSet::new();

    for spec in &options.aggregates {
        if let Some(input_var) = spec.input_var {
            if !current_schema.contains(&input_var) {
                return Err(QueryError::VariableNotFound(format!(
                    "Aggregate input variable {:?} not found in schema",
                    input_var
                )));
            }
            if !options.group_by.is_empty() && group_by_set.contains(&input_var) {
                return Err(QueryError::InvalidQuery(format!(
                    "Aggregate input variable {:?} is a GROUP BY key and will not be grouped",
                    input_var
                )));
            }
            if spec.output_var != input_var && current_schema.contains(&spec.output_var) {
                return Err(QueryError::InvalidQuery(format!(
                    "Aggregate output variable {:?} already exists in schema",
                    spec.output_var
                )));
            }
        } else if current_schema.contains(&spec.output_var) {
            return Err(QueryError::InvalidQuery(format!(
                "Aggregate output variable {:?} already exists in schema",
                spec.output_var
            )));
        }
        if !seen_output_vars.insert(spec.output_var) {
            return Err(QueryError::InvalidQuery(format!(
                "Duplicate aggregate output variable {:?}",
                spec.output_var
            )));
        }
    }

    // Try streaming path: GroupAggregateOperator replaces both GroupBy + Aggregate
    // when all aggregates are streamable (COUNT, SUM, AVG, MIN, MAX).
    let streaming_specs: Vec<StreamingAggSpec> = options
        .aggregates
        .iter()
        .map(|spec| {
            let input_col = spec
                .input_var
                .and_then(|v| current_schema.iter().position(|&sv| sv == v));
            StreamingAggSpec {
                function: spec.function.clone(),
                input_col,
                output_var: spec.output_var,
                distinct: spec.distinct,
            }
        })
        .collect();

    // The streaming operator only outputs GROUP BY keys + aggregate outputs.
    // If SELECT projects any *grouped* variables (non-key, non-aggregate),
    // we must use the traditional path so those vars become `Binding::Grouped`.
    let select_needs_grouped_vars = query.output.select_vars().is_some_and(|vars| {
        vars.iter().any(|v| {
            !options.group_by.contains(v)
                && !options.aggregates.iter().any(|a| a.output_var == *v)
        })
    });

    let use_streaming = !options.aggregates.is_empty()
        && GroupAggregateOperator::all_streamable(&streaming_specs)
        && !select_needs_grouped_vars;

    if use_streaming {
        let partitioned = detect_partitioned_group_by(query, options);
        tracing::debug!(
            group_by_count = options.group_by.len(),
            agg_count = streaming_specs.len(),
            partitioned,
            "using streaming GroupAggregateOperator"
        );
        operator = Box::new(
            GroupAggregateOperator::new(
                operator,
                options.group_by.clone(),
                streaming_specs,
                None,
                partitioned,
            )
            .with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_aggregate_vars.as_slice()),
            ),
        );
    } else {
        operator = Box::new(
            GroupByOperator::new(operator, options.group_by.clone()).with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_groupby_vars.as_slice()),
            ),
        );
        if !options.aggregates.is_empty() {
            operator = Box::new(
                AggregateOperator::new(operator, options.aggregates.clone()).with_out_schema(
                    variable_deps
                        .as_ref()
                        .map(|d| d.required_aggregate_vars.as_slice()),
                ),
            );
        }
    }

    Ok(operator)
}

/// Build the complete operator tree for a query.
///
/// Constructs operators in the order:
/// WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
pub fn build_operator_tree(
    query: &ParsedQuery,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator> {
    if let Some(result) = try_fast_path(query, options, &stats) {
        return result;
    }

    // Compute per-operator downstream dependency sets for trimming.
    let variable_deps = compute_variable_deps(query, options);

    // Needed-vars for WHERE planning: derived from variable_deps when available,
    // otherwise treat all WHERE-bound vars as needed (wildcard/boolean/construct).
    let needed_where_vars: HashSet<VarId> = if let Some(ref deps) = variable_deps {
        deps.required_where_vars.iter().copied().collect()
    } else {
        let mut counts = HashMap::new();
        let mut vars = HashSet::new();
        collect_var_stats(&query.patterns, &mut counts, &mut vars);
        vars.extend(counts.keys().copied());
        vars
    };

    let mut operator = build_where_operators_with_needed(
        &query.patterns,
        stats,
        &needed_where_vars,
        &options.group_by,
        variable_deps
            .as_ref()
            .map(|d| d.required_where_vars.as_slice()),
    )?;

    // GROUP BY + Aggregates
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();
    if needs_grouping {
        operator = build_grouping_operators(operator, query, options, &variable_deps)?;
    }

    // HAVING
    if let Some(ref having_expr) = options.having {
        operator = Box::new(
            HavingOperator::new(operator, having_expr.clone()).with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_having_vars.as_slice()),
            ),
        );
    }

    // Post-aggregation BINDs (e.g., SELECT (CEIL(?avg) AS ?ceil))
    for (i, (var, expr)) in options.post_binds.iter().enumerate() {
        operator = Box::new(
            crate::bind::BindOperator::new(operator, *var, expr.clone(), vec![]).with_out_schema(
                variable_deps
                    .as_ref()
                    .and_then(|d| d.required_bind_vars.get(i))
                    .map(|v| v.as_slice()),
            ),
        );
    }

    // Schema after grouping/aggregation/binds — used for ORDER BY + PROJECT validation.
    let post_group_schema = operator.schema().to_vec();

    // ORDER BY (before projection — may reference vars not in SELECT)
    if !options.order_by.is_empty() {
        // Disallow sorting on Grouped variables (non-key, non-aggregated) because
        // comparison is undefined.
        let allowed_sort_vars: Option<HashSet<VarId>> = if needs_grouping {
            let mut allowed: HashSet<VarId> = options.group_by.iter().copied().collect();
            allowed.extend(options.aggregates.iter().map(|a| a.output_var));
            Some(allowed)
        } else {
            None
        };

        for spec in &options.order_by {
            if !post_group_schema.contains(&spec.var) {
                return Err(QueryError::VariableNotFound(format!(
                    "Sort variable {:?} not found in query schema",
                    spec.var
                )));
            }
            if let Some(ref allowed) = allowed_sort_vars {
                if !allowed.contains(&spec.var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Cannot ORDER BY variable {:?} because it is grouped (non-key, non-aggregate)",
                        spec.var
                    )));
                }
            }
        }

        operator = Box::new(
            SortOperator::new(operator, options.order_by.clone()).with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_sort_vars.as_slice()),
            ),
        );
    }

    // PROJECT (Select/SelectOne only — CONSTRUCT/Wildcard/Boolean skip this)
    if let Some(vars) = query.output.select_vars().filter(|v| !v.is_empty()) {
        for var in vars {
            if !post_group_schema.contains(var) {
                return Err(QueryError::VariableNotFound(format!(
                    "Selected variable {:?} not found in query schema",
                    var
                )));
            }
        }
        operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
    }

    // DISTINCT → OFFSET → LIMIT
    if options.distinct {
        operator = Box::new(DistinctOperator::new(operator));
    }
    if let Some(offset) = options.offset.filter(|&o| o > 0) {
        operator = Box::new(OffsetOperator::new(operator, offset));
    }
    if let Some(limit) = options.limit {
        operator = Box::new(LimitOperator::new(operator, limit));
    }

    Ok(operator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Pattern;
    use crate::options::QueryOptions;
    use crate::parse::{ParsedQuery, QueryOutput};
    use crate::sort::SortSpec;
    use crate::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    fn make_simple_query(select: Vec<VarId>, patterns: Vec<Pattern>) -> ParsedQuery {
        let output = if select.is_empty() {
            QueryOutput::Wildcard
        } else {
            QueryOutput::Select(select)
        };
        ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output,
            patterns,
            options: QueryOptions::default(),
            graph_select: None,
        }
    }

    #[test]
    fn test_build_operator_tree_validates_select_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Select(vec![VarId(99)]), // Variable not in pattern
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            graph_select: None,
        };

        let result = build_operator_tree(&query, &QueryOptions::default(), None);
        let Err(e) = result else {
            panic!("Expected error for invalid select var");
        };
        assert!(e.to_string().contains("not found"));
    }

    #[test]
    fn test_build_operator_tree_validates_sort_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Select(vec![VarId(0)]),
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            graph_select: None,
        };

        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(99))]);

        let result = build_operator_tree(&query, &options, None);
        let Err(e) = result else {
            panic!("Expected error for invalid sort var");
        };
        assert!(e.to_string().contains("Sort variable"));
    }

    #[test]
    fn test_build_operator_tree_empty_patterns() {
        let query = make_simple_query(vec![], vec![]);
        let result = build_operator_tree(&query, &QueryOptions::default(), None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Empty patterns should produce EmptyOperator with empty schema
        assert_eq!(op.schema().len(), 0);
    }
}
