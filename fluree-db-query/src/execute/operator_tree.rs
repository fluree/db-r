//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::aggregate::AggregateOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::{ParsedQuery, SelectMode};
use crate::project::ProjectOperator;
use crate::sort::SortOperator;
use crate::var_registry::VarId;
use fluree_db_core::{StatsView, Storage};
use std::sync::Arc;

use super::where_plan::build_where_operators;

/// Build the complete operator tree for a query
///
/// Constructs operators in the order:
/// WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
pub fn build_operator_tree<S: Storage + 'static>(
    query: &ParsedQuery,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator<S>> {
    // Build WHERE clause operators
    let mut operator = build_where_operators(&query.patterns, stats)?;

    // Get the schema after WHERE (before grouping)
    let where_schema: Arc<[VarId]> = Arc::from(operator.schema().to_vec().into_boxed_slice());

    // GROUP BY (partitions solutions by group key)
    // Also add if there are aggregates but no explicit GROUP BY (implicit single group)
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
        operator = Box::new(GroupByOperator::new(operator, options.group_by.clone()));
    }

    // Aggregates (compute COUNT, SUM, AVG, etc. on Grouped values)
    if !options.aggregates.is_empty() {
        // Validate aggregate input vars exist in current schema and will be Grouped.
        let current_schema = operator.schema();
        let group_by_set: std::collections::HashSet<VarId> =
            options.group_by.iter().copied().collect();

        // Validate output var uniqueness to avoid duplicate VarIds in schema.
        let mut seen_output_vars: std::collections::HashSet<VarId> =
            std::collections::HashSet::new();

        for spec in &options.aggregates {
            // For regular aggregates with input variables, validate that the input exists
            // COUNT(*) has input_var = None and doesn't need this check
            if let Some(input_var) = spec.input_var {
                if !current_schema.contains(&input_var) {
                    return Err(QueryError::VariableNotFound(format!(
                        "Aggregate input variable {:?} not found in schema",
                        input_var
                    )));
                }

                // If grouping by explicit keys, group-by vars remain scalar. Aggregating them would
                // silently pass-through today (since they aren't Grouped), which is incorrect.
                // Disallow to avoid wrong answers.
                if !options.group_by.is_empty() && group_by_set.contains(&input_var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Aggregate input variable {:?} is a GROUP BY key and will not be grouped",
                        input_var
                    )));
                }

                // If output var is different than input var, it must not already exist in schema,
                // otherwise we'd create duplicate VarIds (Batch invariant).
                if spec.output_var != input_var && current_schema.contains(&spec.output_var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Aggregate output variable {:?} already exists in schema",
                        spec.output_var
                    )));
                }
            } else {
                // COUNT(*) - output var must not already exist in schema
                if current_schema.contains(&spec.output_var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Aggregate output variable {:?} already exists in schema",
                        spec.output_var
                    )));
                }
            }

            if !seen_output_vars.insert(spec.output_var) {
                return Err(QueryError::InvalidQuery(format!(
                    "Duplicate aggregate output variable {:?}",
                    spec.output_var
                )));
            }
        }
        operator = Box::new(AggregateOperator::new(operator, options.aggregates.clone()));
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
    // Skip projection for CONSTRUCT - we need all bindings for templating
    if query.select_mode != SelectMode::Construct && !query.select.is_empty() {
        // Validate all select vars exist in schema
        for var in &query.select {
            if !post_group_schema.contains(var) {
                return Err(QueryError::VariableNotFound(format!(
                    "Selected variable {:?} not found in query schema",
                    var
                )));
            }
        }
        operator = Box::new(ProjectOperator::new(operator, query.select.clone()));
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
    use crate::parse::ParsedQuery;
    use crate::pattern::{Term, TriplePattern};
    use crate::sort::SortSpec;
    use fluree_db_core::{MemoryStorage, Sid};
    use fluree_graph_json_ld::ParsedContext;

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
    fn test_build_operator_tree_empty_patterns() {
        let query = make_simple_query(vec![], vec![]);
        let result =
            build_operator_tree::<MemoryStorage>(&query, &QueryOptions::default(), None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Empty patterns should produce EmptyOperator with empty schema
        assert_eq!(op.schema().len(), 0);
    }
}
