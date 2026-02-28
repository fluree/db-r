//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::aggregate::AggregateFn;
use crate::aggregate::AggregateOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::ir::Pattern;
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::ParsedQuery;
use crate::project::ProjectOperator;
use crate::sort::SortOperator;
use crate::stats_query::StatsCountByPredicateOperator;
use crate::triple::{Ref, Term};
use crate::var_registry::VarId;
use crate::PropertyJoinCountAllOperator;
use fluree_db_core::StatsView;
use std::sync::Arc;

use super::where_plan::build_where_operators;

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
) -> Option<(VarId, Vec<(crate::triple::Ref, VarId)>, VarId)> {
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
    let mut seen_obj: std::collections::HashSet<VarId> = std::collections::HashSet::new();

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

/// Build the complete operator tree for a query
///
/// Constructs operators in the order:
/// WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
pub fn build_operator_tree(
    query: &ParsedQuery,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator> {
    // Fast-path: stats-based count-by-predicate query
    // This avoids scanning all triples when we can answer directly from IndexStats.
    if let Some(ref stats_view) = stats {
        if let Some((pred_var, count_var)) = detect_stats_count_by_predicate(query, options) {
            let mut operator: BoxedOperator = Box::new(StatsCountByPredicateOperator::new(
                Arc::clone(stats_view),
                pred_var,
                count_var,
            ));

            // ORDER BY (on predicate or count)
            if !options.order_by.is_empty() {
                operator = Box::new(SortOperator::new(operator, options.order_by.clone()));
            }

            // PROJECT (select specific columns)
            if let Some(vars) = query.output.select_vars() {
                if !vars.is_empty() {
                    operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
                }
            }

            // DISTINCT
            if options.distinct {
                operator = Box::new(crate::distinct::DistinctOperator::new(operator));
            }

            // OFFSET
            if let Some(offset) = options.offset {
                if offset > 0 {
                    operator = Box::new(OffsetOperator::new(operator, offset));
                }
            }

            // LIMIT
            if let Some(limit) = options.limit {
                operator = Box::new(LimitOperator::new(operator, limit));
            }

            return Ok(operator);
        }
    }

    // Fast-path: same-subject N-predicate COUNT(*) without join-row materialization.
    //
    // This is safe because it preserves SPARQL solution multiplicity semantics:
    // COUNT(*) = sum_s Π_i count_pi(s)
    if let Some((s, preds, count_var)) = detect_property_join_count_all(query, options) {
        tracing::debug!("detected property-join COUNT(*) fast-path");
        let mut operator: BoxedOperator =
            Box::new(PropertyJoinCountAllOperator::new(s, preds, count_var));

        // ORDER BY (on count)
        if !options.order_by.is_empty() {
            operator = Box::new(SortOperator::new(operator, options.order_by.clone()));
        }

        // PROJECT
        if let Some(vars) = query.output.select_vars() {
            if !vars.is_empty() {
                operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
            }
        }

        // DISTINCT
        if options.distinct {
            operator = Box::new(DistinctOperator::new(operator));
        }

        // OFFSET
        if let Some(offset) = options.offset {
            if offset > 0 {
                operator = Box::new(OffsetOperator::new(operator, offset));
            }
        }

        // LIMIT
        if let Some(limit) = options.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        return Ok(operator);
    }

    // Build WHERE clause operators
    let mut operator = build_where_operators(&query.patterns, stats)?;

    // Get the schema after WHERE (before grouping)
    let where_schema: Arc<[VarId]> = Arc::from(operator.schema().to_vec().into_boxed_slice());

    // GROUP BY + Aggregates
    // We use streaming GroupAggregateOperator when all aggregates are streamable
    // (COUNT, SUM, AVG, MIN, MAX). This is O(groups) memory instead of O(rows).
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();
    if needs_grouping {
        // Validate group vars exist in where schema
        for var in &options.group_by {
            if !where_schema.contains(var) {
                return Err(QueryError::VariableNotFound(format!(
                    "GROUP BY variable {:?} not found in query schema",
                    var
                )));
            }
        }

        // Validate aggregates
        let current_schema = operator.schema();
        let group_by_set: std::collections::HashSet<VarId> =
            options.group_by.iter().copied().collect();
        let mut seen_output_vars: std::collections::HashSet<VarId> =
            std::collections::HashSet::new();

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
                }
            })
            .collect();

        // The streaming GroupAggregateOperator only outputs GROUP BY keys + aggregate outputs.
        // If the SELECT projects any *grouped* variables (non-key, non-aggregate),
        // we must use the traditional GroupByOperator path so those vars become
        // `Binding::Grouped(Vec<Binding>)` and remain selectable.
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
            // Streaming path: O(groups) memory
            tracing::debug!(
                group_by_count = options.group_by.len(),
                agg_count = streaming_specs.len(),
                "using streaming GroupAggregateOperator"
            );
            operator = Box::new(GroupAggregateOperator::new(
                operator,
                options.group_by.clone(),
                streaming_specs,
                None, // graph_view - will be set from context if needed
            ));
        } else {
            // Traditional path: GroupByOperator + AggregateOperator
            operator = Box::new(GroupByOperator::new(operator, options.group_by.clone()));
            if !options.aggregates.is_empty() {
                operator = Box::new(AggregateOperator::new(operator, options.aggregates.clone()));
            }
        }
    }

    // HAVING (filter on aggregated results)
    if let Some(ref having_expr) = options.having {
        operator = Box::new(HavingOperator::new(operator, having_expr.clone()));
    }

    // Post-aggregation BINDs (e.g., SELECT (CEIL(?avg) AS ?ceil))
    if !options.post_binds.is_empty() {
        for (var, expr) in &options.post_binds {
            operator = Box::new(crate::bind::BindOperator::new(
                operator,
                *var,
                expr.clone(),
                vec![],
            ));
        }
    }

    // Get the schema after grouping/aggregation/binds (for validation)
    let post_group_schema: Arc<[VarId]> = Arc::from(operator.schema().to_vec().into_boxed_slice());

    // ORDER BY (before projection - may reference vars not in SELECT)
    if !options.order_by.is_empty() {
        // Validate sort vars exist in current schema
        // Disallow sorting on Grouped variables (non-key, non-aggregated) because comparison is undefined.
        let mut allowed_sort_vars: Option<std::collections::HashSet<VarId>> = None;
        if needs_grouping {
            let mut allowed = std::collections::HashSet::new();
            // GROUP BY keys are scalar
            for v in &options.group_by {
                allowed.insert(*v);
            }
            // Aggregate outputs are scalar
            for spec in &options.aggregates {
                allowed.insert(spec.output_var);
            }
            allowed_sort_vars = Some(allowed);
        }
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
        operator = Box::new(SortOperator::new(operator, options.order_by.clone()));
    }

    // PROJECT (select specific columns)
    // Skip projection for CONSTRUCT/Wildcard/Boolean - only Select/SelectOne project
    if let Some(vars) = query.output.select_vars() {
        if !vars.is_empty() {
            // Validate all select vars exist in schema
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
    }

    // DISTINCT (after projection)
    if options.distinct {
        operator = Box::new(DistinctOperator::new(operator));
    }

    // OFFSET
    if let Some(offset) = options.offset {
        if offset > 0 {
            operator = Box::new(OffsetOperator::new(operator, offset));
        }
    }

    // LIMIT
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
            output: QueryOutput::Select(vec![VarId(0)]),
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
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
    fn test_build_operator_tree_empty_patterns() {
        let query = make_simple_query(vec![], vec![]);
        let result = build_operator_tree(&query, &QueryOptions::default(), None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Empty patterns should produce EmptyOperator with empty schema
        assert_eq!(op.schema().len(), 0);
    }
}
