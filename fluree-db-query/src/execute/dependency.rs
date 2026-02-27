//! Variable dependency tracking for projection pushdown.
//!
//! Computes which variables the query output depends on by working backward
//! from the final output through ORDER BY, post-binds, HAVING, aggregates,
//! and GROUP BY. Variables without downstream dependencies are dead and can
//! be projected away early.

use crate::options::QueryOptions;
use crate::parse::{ParsedQuery, SelectMode};
use crate::var_registry::VarId;
use std::collections::HashSet;

/// Compute the set of variables the query output depends on from WHERE.
///
/// Works backward from the query output (SELECT, CONSTRUCT, ORDER BY, etc.)
/// through post-binds, HAVING, aggregates, and GROUP BY to determine which
/// WHERE-produced variables are actually needed downstream.
///
/// Returns `None` for `Wildcard` mode (all WHERE vars are needed).
pub fn compute_where_deps(query: &ParsedQuery, options: &QueryOptions) -> Option<HashSet<VarId>> {
    // Start with variables needed at the final output.
    let mut deps: HashSet<VarId> = match query.select_mode {
        // Wildcard: all WHERE vars are output — nothing to drop.
        SelectMode::Wildcard => return None,
        // CONSTRUCT needs all template variables.
        SelectMode::Construct => match &query.construct_template {
            Some(ct) => ct.variables(),
            None => return None,
        },
        // Many | One: deps = query.select.
        // Empty select means "no explicit projection" — keep all vars.
        _ => {
            if query.select.is_empty() {
                return None;
            }
            query.select.iter().copied().collect()
        }
    };

    // ORDER BY vars must survive to the sort operator.
    for spec in &options.order_by {
        deps.insert(spec.var);
    }

    // Post-binds (reverse order): if the output var is a dependency, trace its inputs.
    // Reverse order is required so chained binds resolve correctly. E.g. given
    // binds [?b = f(?a), ?c = g(?b)], processing ?c first adds ?b to the set,
    // then processing ?b traces through to ?a. Forward order would miss the chain.
    for (var, expr) in options.post_binds.iter().rev() {
        if deps.remove(var) {
            deps.extend(expr.variables());
        }
    }

    // HAVING: its expression variables are dependencies.
    if let Some(ref having_expr) = options.having {
        deps.extend(having_expr.variables());
    }

    // Aggregates: for each aggregate whose output is a dependency, replace the
    // output with the input variable (the aggregate consumes grouped input).
    for spec in &options.aggregates {
        if deps.remove(&spec.output_var) {
            if let Some(input_var) = spec.input_var {
                deps.insert(input_var);
            }
        }
    }

    // GROUP BY: all group key variables must survive WHERE.
    deps.extend(options.group_by.iter().copied());

    Some(deps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{AggregateFn, AggregateSpec};
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::options::QueryOptions;
    use crate::parse::{ConstructTemplate, ParsedQuery, SelectMode};
    use crate::sort::SortSpec;
    use crate::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_query(
        select: Vec<VarId>,
        patterns: Vec<Pattern>,
        select_mode: SelectMode,
    ) -> ParsedQuery {
        ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            select,
            patterns,
            options: QueryOptions::default(),
            select_mode,
            construct_template: None,
            graph_select: None,
        }
    }

    fn make_tp(s: VarId, p: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, p)), Term::Var(o))
    }

    #[test]
    fn wildcard_returns_none() {
        let query = make_query(vec![], vec![], SelectMode::Wildcard);
        let result = compute_where_deps(&query, &QueryOptions::default());
        assert!(result.is_none());
    }

    #[test]
    fn simple_select_returns_select_vars() {
        let query = make_query(vec![VarId(1), VarId(2)], vec![], SelectMode::Many);
        let deps = compute_where_deps(&query, &QueryOptions::default()).unwrap();
        assert_eq!(deps, HashSet::from([VarId(1), VarId(2)]));
    }

    #[test]
    fn order_by_adds_vars() {
        let query = make_query(vec![VarId(1)], vec![], SelectMode::Many);
        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(3))]);
        let deps = compute_where_deps(&query, &options).unwrap();
        assert!(deps.contains(&VarId(1)));
        assert!(deps.contains(&VarId(3)));
    }

    #[test]
    fn aggregate_replaces_output_with_input() {
        // SELECT ?city (AVG(?age) AS ?avg) ... GROUP BY ?city
        let query = make_query(vec![VarId(2), VarId(3)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(2)])
            .with_aggregates(vec![AggregateSpec {
                function: AggregateFn::Avg,
                input_var: Some(VarId(1)),
                output_var: VarId(3),
            }]);

        let deps = compute_where_deps(&query, &options).unwrap();
        // ?city (group key) and ?age (aggregate input) are dependencies
        assert!(deps.contains(&VarId(2)));
        assert!(deps.contains(&VarId(1)));
        // ?avg (aggregate output) is NOT a dependency at WHERE level
        assert!(!deps.contains(&VarId(3)));
    }

    #[test]
    fn post_bind_traces_dependencies() {
        // SELECT ?x (CEIL(?avg) AS ?ceil)
        // post_bind: ?ceil = CEIL(?avg)
        let query = make_query(vec![VarId(0), VarId(2)], vec![], SelectMode::Many);
        let options = QueryOptions {
            post_binds: vec![(
                VarId(2),
                Expression::Call {
                    func: crate::ir::Function::Ceil,
                    args: vec![Expression::Var(VarId(1))],
                },
            )],
            ..Default::default()
        };

        let deps = compute_where_deps(&query, &options).unwrap();
        assert!(deps.contains(&VarId(0)));
        // ?avg (input to post-bind) is a dependency, ?ceil is not (it's computed)
        assert!(deps.contains(&VarId(1)));
        assert!(!deps.contains(&VarId(2)));
    }

    #[test]
    fn having_adds_vars() {
        let query = make_query(vec![VarId(0)], vec![], SelectMode::Many);
        let options = QueryOptions::new()
            .with_group_by(vec![VarId(0)])
            .with_having(Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(10)),
            ));

        let deps = compute_where_deps(&query, &options).unwrap();
        assert!(deps.contains(&VarId(0)));
        assert!(deps.contains(&VarId(1)));
    }

    #[test]
    fn construct_uses_template_vars() {
        let mut query = make_query(vec![], vec![], SelectMode::Construct);
        query.construct_template = Some(ConstructTemplate::new(vec![make_tp(
            VarId(0),
            "name",
            VarId(1),
        )]));

        let deps = compute_where_deps(&query, &QueryOptions::default()).unwrap();
        assert!(deps.contains(&VarId(0)));
        assert!(deps.contains(&VarId(1)));
    }
}
