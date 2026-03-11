//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::aggregate::AggregateFn;
use crate::aggregate::AggregateOperator;
use crate::binary_scan::EmitMask;
use crate::count_rows::CountRowsOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::fast_chain_join_count_all::linear_chain_count_all_operator;
use crate::fast_count::{
    count_blank_node_subjects_operator, count_distinct_object_operator,
    count_distinct_objects_operator, count_distinct_predicates_operator,
    count_distinct_subjects_operator, count_literal_objects_operator, count_rows_operator,
    count_triples_operator,
};
use crate::fast_exists_count_all::{
    chain_exists_join_count_all_operator, exists_join_count_all_operator,
    exists_star_join_count_all_operator, object_chain_exists_join_count_all_operator,
    star_exists_join_count_all_operator,
};
use crate::fast_exists_join_count_distinct_object::exists_join_count_distinct_object_operator;
use crate::fast_fused_scan_sum::{
    fused_scan_sum_i64_operator, DateComponentFn, NumericUnaryFn, ScalarI64Fn,
};
use crate::fast_group_count_firsts::{
    GroupByObjectStarTopKOperator, PredicateGroupCountFirstsOperator,
    PredicateObjectCountFirstsOperator,
};
use crate::fast_min_max_string::{predicate_min_max_string_operator, MinMaxMode};
use crate::fast_minus_count_all::{
    chain_minus_count_all_operator, minus_join_count_all_operator,
    object_chain_minus_count_all_operator, property_minus_count_all_operator,
};
use crate::fast_multicolumn_join_count_all::multicolumn_join_count_all_operator;
use crate::fast_optional_count_all::{
    predicate_chain_optional_tail_count_all, predicate_optional_chain_head_count_all,
    predicate_optional_join_count_all,
};
use crate::fast_property_path_plus_count_all::property_path_plus_count_all_operator;
use crate::fast_sum_strlen_group_concat::sum_strlen_group_concat_operator;
use crate::fast_transitive_path_plus_count_all::transitive_path_plus_count_all_operator;
use crate::fast_union_star_count_all::{UnionCountMode, UnionStarCountAllOperator};
use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::ir::{PathModifier, Pattern};
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::{ParsedQuery, QueryOutput};
use crate::project::ProjectOperator;
use crate::sort::SortOperator;
use crate::stats_query::StatsCountByPredicateOperator;
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;
use crate::BinaryScanOperator;
use crate::PropertyJoinCountAllOperator;
use fluree_db_core::StatsView;
use std::sync::Arc;

use super::dependency::compute_variable_deps;
use super::where_plan::build_where_operators_with_needed;
use super::where_plan::collect_var_stats;

// ---------------------------------------------------------------------------
// Shared detection helpers
// ---------------------------------------------------------------------------

/// Extract a bound predicate (IRI or SID) from a triple pattern's predicate position.
/// Returns `None` if the predicate is a variable or other non-bound form.
fn extract_bound_predicate(p: &Ref) -> Option<Ref> {
    match p {
        Ref::Sid(_) | Ref::Iri(_) => Some(p.clone()),
        _ => None,
    }
}

/// Validate a triple pattern as `?s <bound_pred> ?o` with no datatype constraint.
/// Returns `(subject_var, bound_predicate, object_var)`.
fn validate_simple_triple(tp: &TriplePattern) -> Option<(VarId, Ref, VarId)> {
    let Ref::Var(sv) = &tp.s else { return None };
    let pred = extract_bound_predicate(&tp.p)?;
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }
    Some((*sv, pred, *ov))
}

/// Resolve an EXISTS block from either `Pattern::Filter(Expression::Exists { .. })`
/// or `Pattern::Exists(inner_patterns)` representation.
///
/// Returns `(inner_patterns, negated)` or `None` if neither representation is present.
fn resolve_exists_block<'a>(
    exists_expr: Option<&'a crate::ir::Expression>,
    exists_patterns: Option<&'a [Pattern]>,
) -> Option<(&'a [Pattern], bool)> {
    if let Some(expr) = exists_expr {
        let crate::ir::Expression::Exists { patterns, negated } = expr else {
            return None;
        };
        Some((patterns.as_slice(), *negated))
    } else if let Some(pats) = exists_patterns {
        Some((pats, false))
    } else {
        None
    }
}

/// Find a 2-hop chain pattern `?a <p1> ?b . ?b <p2> ?c` in two triples (trying both orderings).
///
/// Returns `(a_var, pred1, b_var, pred2, c_var)` or `None` if neither ordering forms a chain.
/// Both triples must be simple (var subject, bound pred, var object, no dtc).
fn find_two_hop_chain(
    t1: &TriplePattern,
    t2: &TriplePattern,
) -> Option<(VarId, Ref, VarId, Ref, VarId)> {
    let try_order =
        |x: &TriplePattern, y: &TriplePattern| -> Option<(VarId, Ref, VarId, Ref, VarId)> {
            let (a, p1, b1) = validate_simple_triple(x)?;
            let (b2, p2, c) = validate_simple_triple(y)?;
            if b1 != b2 {
                return None;
            }
            Some((a, p1, b1, p2, c))
        };
    try_order(t1, t2).or_else(|| try_order(t2, t1))
}

/// Validate that a query has a single `COUNT(*)` aggregate with standard constraints.
///
/// Returns `Some(output_var)` if the query has:
/// - SELECT output (not CONSTRUCT/BOOLEAN/WILDCARD)
/// - Exactly one aggregate: `COUNT(*)` (not distinct, no input var)
/// - No group_by, having, post_binds, order_by, offset, or DISTINCT
/// - LIMIT >= 1 (or no limit)
/// - SELECT vars == `[agg.output_var]`
fn detect_count_all_aggregate(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
        || options.distinct
    {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct || !matches!(agg.function, AggregateFn::CountAll) || agg.input_var.is_some() {
        return None;
    }
    if options.limit == Some(0) {
        return None;
    }
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some(agg.output_var)
}

/// Validate that a query has a single `COUNT(DISTINCT ?var)` aggregate with standard constraints.
///
/// Returns `Some((input_var, output_var))` if the query has:
/// - SELECT output (not CONSTRUCT/BOOLEAN/WILDCARD)
/// - Exactly one aggregate: `COUNT(DISTINCT ?var)`
/// - No group_by, having, post_binds, order_by, offset, or DISTINCT
/// - LIMIT >= 1 (or no limit)
/// - SELECT vars == `[agg.output_var]`
fn detect_count_distinct_aggregate(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(VarId, VarId)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
        || options.distinct
    {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct || !matches!(agg.function, AggregateFn::CountDistinct) {
        return None;
    }
    let in_var = agg.input_var?;
    if options.limit == Some(0) {
        return None;
    }
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((in_var, agg.output_var))
}

/// Validate that a query has a single `COUNT(*)` or `COUNT(?var)` aggregate.
///
/// Returns `Some((input_var, output_var))` where `input_var` is `None` for `COUNT(*)`.
/// Same standard constraints as [`detect_count_all_aggregate`].
fn detect_count_aggregate(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Option<VarId>, VarId)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
        || options.distinct
    {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct {
        return None;
    }
    let input_var = match agg.function {
        AggregateFn::CountAll => {
            if agg.input_var.is_some() {
                return None;
            }
            None
        }
        AggregateFn::Count => Some(agg.input_var?),
        _ => return None,
    };
    if options.limit == Some(0) {
        return None;
    }
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((input_var, agg.output_var))
}

fn detect_partitioned_group_by(query: &ParsedQuery, options: &QueryOptions) -> bool {
    if options.group_by.len() != 1 {
        return false;
    }
    let gb = options.group_by[0];

    // Strict: only a single triple pattern plus order-preserving operators (FILTER/BIND).
    let mut triple: Option<&crate::triple::TriplePattern> = None;
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
    let (s_var, pred, o_var) = validate_simple_triple(tp)?;

    // GROUP BY ?object
    if options.group_by.len() != 1 || options.group_by[0] != o_var {
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
    let is_count = matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll);
    if !is_count {
        return None;
    }
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(s_var) {
        return None;
    }
    if options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }
    // ORDER BY DESC(?count) and LIMIT k required so we can do top-k directly.
    let limit = options.limit?;
    if options.order_by.len() != 1 {
        return None;
    }
    let ob = &options.order_by[0];
    if ob.var != agg.output_var || ob.direction != crate::sort::SortDirection::Descending {
        return None;
    }
    Some((pred, s_var, o_var, agg.output_var, limit))
}

/// Detect `GROUP BY ?o` top-k where WHERE is a same-subject star join:
/// `?s <p_group> ?o . ?s <p_filter1> ?x1 . ...`
///
/// Supports subject aggregates: MIN(?s), MAX(?s), SAMPLE(?s) in addition to COUNT.
#[allow(clippy::type_complexity)]
fn detect_group_by_object_star_topk(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(
    Ref,
    Vec<Ref>,
    Arc<[VarId]>,
    VarId,
    VarId,
    VarId,
    Option<VarId>,
    Option<VarId>,
    Option<VarId>,
    usize,
)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    let select_vars: Arc<[VarId]> =
        Arc::from(query.output.select_vars()?.to_vec().into_boxed_slice());
    if options.group_by.len() != 1 {
        return None;
    }
    let group_var = options.group_by[0];
    if options.distinct || options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }
    if options.offset.is_some() {
        return None;
    }
    let limit = options.limit?;
    if options.order_by.len() != 1 {
        return None;
    }
    if query.patterns.len() < 2 {
        return None;
    }

    // All patterns must be triples with the same subject var.
    let mut subj_var: Option<VarId> = None;
    let mut group_tp: Option<&TriplePattern> = None;
    let mut filter_preds: Vec<Ref> = Vec::new();
    for p in &query.patterns {
        let Pattern::Triple(tp) = p else {
            return None;
        };
        let (sv, pred, ov) = validate_simple_triple(tp)?;
        if subj_var.is_none() {
            subj_var = Some(sv);
        } else if subj_var != Some(sv) {
            return None;
        }
        if ov == group_var {
            if group_tp.is_some() {
                return None;
            }
            group_tp = Some(tp);
        } else {
            filter_preds.push(pred);
        }
    }
    let subj_var = subj_var?;
    let group_tp = group_tp?;
    let group_pred = extract_bound_predicate(&group_tp.p)?;
    if filter_preds.is_empty() {
        return None;
    }

    // Aggregates: require COUNT (or COUNT(*)) and allow MIN/MAX/SAMPLE on ?s.
    let mut count_out: Option<VarId> = None;
    let mut min_out: Option<VarId> = None;
    let mut max_out: Option<VarId> = None;
    let mut sample_out: Option<VarId> = None;
    for agg in &options.aggregates {
        if agg.distinct {
            return None;
        }
        match agg.function {
            AggregateFn::CountAll => {
                if count_out.is_some() {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Count => {
                if count_out.is_some() {
                    return None;
                }
                if agg.input_var != Some(subj_var) {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Min => {
                if min_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                min_out = Some(agg.output_var);
            }
            AggregateFn::Max => {
                if max_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                max_out = Some(agg.output_var);
            }
            AggregateFn::Sample => {
                if sample_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                sample_out = Some(agg.output_var);
            }
            _ => return None,
        }
    }
    let count_out = count_out?;

    // ORDER BY DESC(?count).
    let ob = &options.order_by[0];
    if ob.var != count_out || ob.direction != crate::sort::SortDirection::Descending {
        return None;
    }

    // SELECT vars must be exactly the group var + the aggregate output vars (in any order).
    let mut expected: Vec<VarId> = vec![group_var, count_out];
    if let Some(v) = min_out {
        expected.push(v);
    }
    if let Some(v) = max_out {
        expected.push(v);
    }
    if let Some(v) = sample_out {
        expected.push(v);
    }
    expected.sort_unstable();
    let mut actual: Vec<VarId> = select_vars.iter().copied().collect();
    actual.sort_unstable();
    if actual != expected {
        return None;
    }

    Some((
        group_pred,
        filter_preds,
        select_vars,
        subj_var,
        group_var,
        count_out,
        min_out,
        max_out,
        sample_out,
        limit,
    ))
}

fn detect_sum_strlen_group_concat_subquery(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Arc<str>, VarId)> {
    use crate::ir::{Expression, Function, Pattern};

    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    if !options.group_by.is_empty() {
        return None;
    }
    if options.aggregates.len() != 1 {
        return None;
    }
    if options.distinct || options.having.is_some() || !options.post_binds.is_empty() {
        return None;
    }
    if !options.order_by.is_empty() || options.offset.is_some() {
        return None;
    }
    if options.limit == Some(0) {
        return None;
    }

    // Outer aggregate must be SUM(?v) (where ?v is the STRLEN bind var).
    let outer_agg = &options.aggregates[0];
    if outer_agg.distinct || outer_agg.function != AggregateFn::Sum {
        return None;
    }
    let strlen_var = outer_agg.input_var?;

    // Patterns: one Subquery + one Bind(strlen_var = STRLEN(?cat)).
    if query.patterns.len() != 2 {
        return None;
    }
    let mut subq: Option<&crate::ir::SubqueryPattern> = None;
    let mut bind: Option<(VarId, &Expression)> = None;
    for p in &query.patterns {
        match p {
            Pattern::Subquery(sq) => subq = Some(sq),
            Pattern::Bind { var, expr } => bind = Some((*var, expr)),
            _ => return None,
        }
    }
    let (Some(sq), Some((bind_var, bind_expr))) = (subq, bind) else {
        return None;
    };
    if bind_var != strlen_var {
        return None;
    }
    let Expression::Call { func, args } = bind_expr else {
        return None;
    };
    if *func != Function::Strlen || args.len() != 1 {
        return None;
    }
    let Expression::Var(cat_var) = &args[0] else {
        return None;
    };

    // Inner subquery must be GROUP BY ?s with GROUP_CONCAT(?o; sep) AS ?cat.
    if sq.group_by.len() != 1 {
        return None;
    }
    if sq.aggregates.len() != 1 {
        return None;
    }
    let inner_agg = &sq.aggregates[0];
    let (sep, input_var) = match &inner_agg.function {
        AggregateFn::GroupConcat { separator } => (separator.as_str(), inner_agg.input_var?),
        _ => return None,
    };
    if inner_agg.distinct || inner_agg.output_var != *cat_var {
        return None;
    }

    // Inner WHERE must be a single triple: ?s <p> ?o (predicate bound).
    if sq.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &sq.patterns[0] else {
        return None;
    };
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let pred = match &tp.p {
        Ref::Sid(_) | Ref::Iri(_) => tp.p.clone(),
        _ => return None,
    };
    let Term::Var(o_var) = &tp.o else {
        return None;
    };
    if tp.dtc.is_some() {
        return None;
    }
    if sq.group_by[0] != *s_var {
        return None;
    }
    if input_var != *o_var {
        return None;
    }

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != outer_agg.output_var {
        return None;
    }

    Some((pred, Arc::from(sep), outer_agg.output_var))
}

fn detect_predicate_object_count(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, VarId, crate::triple::Term, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

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
    let pred = extract_bound_predicate(&tp.p)?;
    if matches!(&tp.o, Term::Var(_)) {
        return None;
    }
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?s (the only var in this single-triple pattern).
    if let Some(v) = input_var {
        if v != *s_var {
            return None;
        }
    }

    Some((pred, *s_var, tp.o.clone(), out_var))
}

fn detect_predicate_count_rows(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (s_var, pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(?var) must reference ?s or ?o (both always bound in a single triple).
    if let Some(v) = input_var {
        if v != s_var && v != o_var {
            return None;
        }
    }

    Some((pred, out_var))
}

fn detect_predicate_count_distinct_object(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(DISTINCT ?o) must reference the object var.
    if in_var != o_var {
        return None;
    }

    Some((pred, out_var))
}

fn detect_predicate_minmax_string(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, MinMaxMode, VarId)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    // Must be single aggregate, no grouping/having/binds/etc.
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
        || options.distinct
    {
        return None;
    }
    if options.limit == Some(0) {
        return None;
    }
    // WHERE must be a single triple.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;

    // Aggregate must be MIN(?o) or MAX(?o) (not distinct).
    let agg = &options.aggregates[0];
    if agg.distinct {
        return None;
    }
    let mode = match agg.function {
        AggregateFn::Min => MinMaxMode::Min,
        AggregateFn::Max => MinMaxMode::Max,
        _ => return None,
    };
    if agg.input_var? != o_var {
        return None;
    }

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((pred, mode, agg.output_var))
}

fn detect_count_rows_with_encoded_filters(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(
    crate::triple::TriplePattern,
    Vec<crate::ir::Expression>,
    VarId,
)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }

    // Must be single COUNT aggregate, no grouping/having/binds/etc.
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
        || options.distinct
    {
        return None;
    }
    if options.limit == Some(0) {
        return None;
    }
    let agg = &options.aggregates[0];
    if agg.distinct {
        return None;
    }
    let is_count = matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll);
    if !is_count {
        return None;
    }

    // WHERE must be: one triple + one or more FILTER(...) patterns, and nothing else.
    if query.patterns.len() < 2 {
        return None;
    }
    let mut triple: Option<&crate::triple::TriplePattern> = None;
    let mut filters: Vec<&crate::ir::Expression> = Vec::new();
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triple = Some(tp),
            Pattern::Filter(expr) => filters.push(expr),
            _ => return None,
        }
    }
    let tp = triple?;
    if filters.is_empty() {
        return None;
    }

    let (s_var, _pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(?s) or COUNT(*) only.
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(s_var) {
        return None;
    }

    // All FILTERs must be compilable as encoded prefilters.
    let is_s =
        |e: &crate::ir::Expression| matches!(e, crate::ir::Expression::Var(v) if *v == s_var);
    let is_o =
        |e: &crate::ir::Expression| matches!(e, crate::ir::Expression::Var(v) if *v == o_var);
    let is_lang_call = |e: &crate::ir::Expression| match e {
        crate::ir::Expression::Call { func, args } => {
            *func == crate::ir::Function::Lang
                && args.len() == 1
                && matches!(&args[0], crate::ir::Expression::Var(v) if *v == o_var)
        }
        _ => false,
    };
    let is_lang_eq_const = |expr: &crate::ir::Expression| match expr {
        crate::ir::Expression::Call { func, args } => {
            if *func != crate::ir::Function::Eq || args.len() != 2 {
                return false;
            }
            let has_lang = is_lang_call(&args[0]) || is_lang_call(&args[1]);
            let has_const = matches!(
                (&args[0], &args[1]),
                (
                    crate::ir::Expression::Const(crate::ir::FilterValue::String(_)),
                    _
                ) | (
                    _,
                    crate::ir::Expression::Const(crate::ir::FilterValue::String(_))
                )
            );
            has_lang && has_const
        }
        _ => false,
    };
    for expr in &filters {
        match expr {
            crate::ir::Expression::Call { func, args }
                if *func == crate::ir::Function::IsBlank
                    && args.len() == 1
                    && matches!(&args[0], crate::ir::Expression::Var(v) if *v == o_var) =>
            {
                continue;
            }
            crate::ir::Expression::Call { func, args } if args.len() == 2 => {
                if matches!(func, crate::ir::Function::Eq | crate::ir::Function::Ne)
                    && ((is_s(&args[0]) && is_o(&args[1])) || (is_o(&args[0]) && is_s(&args[1])))
                {
                    continue;
                }
                if is_lang_eq_const(expr) {
                    continue;
                }
                return None;
            }
            _ => return None,
        }
    }

    // SELECT must be exactly the count output var.
    let select_vars = query.output.select_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((
        tp.clone(),
        filters.into_iter().cloned().collect(),
        agg.output_var,
    ))
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

/// Detect a same-subject star-join COUNT(*) query that can be answered without
/// materializing join rows.
///
/// Matches:
/// - required patterns are triple patterns sharing the same subject var
/// - OPTIONAL patterns are allowed when each OPTIONAL group contains 1+ triples that all
///   share the same subject var
/// - all predicates are bound (Sid/Iri)
/// - all objects are vars (and object vars are distinct across required+optional, and within each OPTIONAL group)
/// - SELECT is exactly the COUNT(*) output var
/// - options: one CountAll aggregate, no GROUP BY / HAVING / post-binds
///
/// Returns `(subject_var, required predicate/object pairs, optional groups (each a list of predicate/object pairs), count_var)`.
#[allow(clippy::type_complexity)]
fn detect_property_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(
    VarId,
    Vec<(crate::triple::Ref, VarId)>,
    Vec<Vec<(crate::triple::Ref, VarId)>>,
    VarId,
)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() < 2 {
        return None;
    }

    let mut subject_var: Option<VarId> = None;
    let mut required: Vec<(Ref, VarId)> = Vec::with_capacity(query.patterns.len());
    let mut optional_groups: Vec<Vec<(Ref, VarId)>> = Vec::new();
    let mut seen_obj: std::collections::HashSet<VarId> = std::collections::HashSet::new();

    let mut any_required = false;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => {
                any_required = true;
                let (s, pred, o) = validate_simple_triple(tp)?;
                match subject_var {
                    None => subject_var = Some(s),
                    Some(existing) if existing != s => return None,
                    Some(_) => {}
                }
                if !seen_obj.insert(o) {
                    return None;
                }
                if subject_var == Some(o) {
                    return None;
                }
                required.push((pred, o));
            }
            Pattern::Optional(inner) => {
                if inner.is_empty() {
                    return None;
                }
                let mut group: Vec<(Ref, VarId)> = Vec::with_capacity(inner.len());
                let mut group_seen: std::collections::HashSet<VarId> =
                    std::collections::HashSet::with_capacity(inner.len());
                for pat in inner {
                    let Pattern::Triple(tp) = pat else {
                        return None;
                    };
                    let (s, pred, o) = validate_simple_triple(tp)?;
                    match subject_var {
                        None => subject_var = Some(s),
                        Some(existing) if existing != s => return None,
                        Some(_) => {}
                    }
                    if !group_seen.insert(o) {
                        return None;
                    }
                    if !seen_obj.insert(o) {
                        return None;
                    }
                    if subject_var == Some(o) {
                        return None;
                    }
                    group.push((pred, o));
                }
                optional_groups.push(group);
            }
            _ => return None,
        }
    }

    if !any_required || required.is_empty() {
        return None;
    }

    Some((subject_var?, required, optional_groups, out_var))
}

fn detect_fused_scan_sum_i64(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, ScalarI64Fn, VarId)> {
    if matches!(
        query.output,
        QueryOutput::Construct(_) | QueryOutput::Boolean | QueryOutput::Wildcard
    ) {
        return None;
    }
    // Must be single aggregate, no grouping/having/binds/etc.
    if !options.group_by.is_empty()
        || options.aggregates.len() != 1
        || options.having.is_some()
        || !options.post_binds.is_empty()
        || !options.order_by.is_empty()
        || options.offset.is_some()
    {
        return None;
    }
    // LIMIT is fine as long as it's >= 1 (single row output). OFFSET is disallowed above.
    if let Some(lim) = options.limit {
        if lim == 0 {
            return None;
        }
    }

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.select_vars()?;
    let agg = &options.aggregates[0];
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    if agg.distinct || !matches!(agg.function, AggregateFn::Sum) {
        return None;
    }

    // Pattern shape: one Triple + one Bind (desugared aggregate expr input).
    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, bind_var, bind_expr) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Bind { var, expr }) => (tp, *var, expr),
        // Be conservative: only accept the canonical lowering order.
        _ => return None,
    };

    // Triple must be ?s <p> ?o with bound predicate and var object.
    let pred = extract_bound_predicate(&tp.p)?;
    let Term::Var(o_var) = &tp.o else {
        return None;
    };

    // Bind must define the aggregate input var, and SUM must use it.
    if agg.input_var != Some(bind_var) {
        return None;
    }

    // Bind expression must be scalar(?o).
    let crate::ir::Expression::Call { func, args } = bind_expr else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    if !matches!(&args[0], crate::ir::Expression::Var(v) if v == o_var) {
        return None;
    }

    let scalar = match func {
        crate::ir::Function::Year => ScalarI64Fn::DateComponent(DateComponentFn::Year),
        crate::ir::Function::Month => ScalarI64Fn::DateComponent(DateComponentFn::Month),
        crate::ir::Function::Day => ScalarI64Fn::DateComponent(DateComponentFn::Day),
        crate::ir::Function::Abs => ScalarI64Fn::NumericUnary(NumericUnaryFn::Abs),
        crate::ir::Function::Ceil => ScalarI64Fn::NumericUnary(NumericUnaryFn::Ceil),
        crate::ir::Function::Floor => ScalarI64Fn::NumericUnary(NumericUnaryFn::Floor),
        crate::ir::Function::Round => ScalarI64Fn::NumericUnary(NumericUnaryFn::Round),
        _ => return None,
    };

    Some((pred, scalar, agg.output_var))
}

fn detect_exists_join_count_distinct_object(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // WHERE must be exactly two triples: ?s <p1> ?o1 . ?s <p2> ?o2 .
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(a) = &query.patterns[0] else {
        return None;
    };
    let Pattern::Triple(b) = &query.patterns[1] else {
        return None;
    };

    let (sv_a, pred_a, ov_a) = validate_simple_triple(a)?;
    let (sv_b, pred_b, _ov_b) = validate_simple_triple(b)?;
    if sv_a != sv_b {
        return None;
    }

    // One of the object vars must be the COUNT DISTINCT input.
    if ov_a == in_var {
        Some((pred_a, pred_b, out_var))
    } else {
        None
    }
}

fn detect_exists_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be: Triple + FILTER(EXISTS{single triple}) in either order.
    if query.patterns.len() != 2 {
        return None;
    }

    let mut outer: Option<&crate::triple::TriplePattern> = None;
    let mut exists_expr: Option<&crate::ir::Expression> = None;
    let mut exists_patterns: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => {
                if outer.is_some() {
                    return None;
                }
                outer = Some(tp);
            }
            Pattern::Filter(expr) => {
                if exists_expr.is_some() {
                    return None;
                }
                exists_expr = Some(expr);
            }
            Pattern::Exists(inner) => {
                if exists_patterns.is_some() {
                    return None;
                }
                exists_patterns = Some(inner);
            }
            _ => return None,
        }
    }
    let outer = outer?;
    let (sv_outer, outer_pred, _ov1) = validate_simple_triple(outer)?;

    let (patterns, negated) = resolve_exists_block(exists_expr, exists_patterns)?;
    if negated || patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp2) = &patterns[0] else {
        return None;
    };
    let (sv2, exists_pred, _ov2) = validate_simple_triple(tp2)?;
    if sv2 != sv_outer {
        return None;
    }

    Some((outer_pred, exists_pred, out_var))
}

fn detect_chain_exists_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be: 2 outer triples + EXISTS(single triple), any order.
    if query.patterns.len() != 3 {
        return None;
    }

    let mut triples: Vec<&crate::triple::TriplePattern> = Vec::new();
    let mut exists_inner: Option<&[Pattern]> = None;
    let mut exists_expr: Option<&crate::ir::Expression> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Exists(inner) => exists_inner = Some(inner),
            Pattern::Filter(expr) => exists_expr = Some(expr),
            _ => return None,
        }
    }
    if triples.len() != 2 {
        return None;
    }

    let (exists_patterns, negated) = resolve_exists_block(exists_expr, exists_inner)?;
    if negated || exists_patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp3) = &exists_patterns[0] else {
        return None;
    };
    let (c_var, pred3, _d_var) = validate_simple_triple(tp3)?;

    // Find chain: (?a p1 ?b) and (?b p2 ?c) where ?c links to EXISTS subject.
    let (_a, pred1, _b, pred2, c2) = find_two_hop_chain(triples[0], triples[1])?;
    if c2 != c_var {
        return None;
    }

    Some((pred1, pred2, pred3, out_var))
}

fn detect_star_exists_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Vec<Ref>, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be: 2+ outer triples sharing same subject var + EXISTS(single triple) on same subject.
    if query.patterns.len() < 3 {
        return None;
    }

    let mut triples: Vec<&crate::triple::TriplePattern> = Vec::new();
    let mut exists_inner: Option<&[Pattern]> = None;
    let mut exists_expr: Option<&crate::ir::Expression> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Exists(inner) => exists_inner = Some(inner),
            Pattern::Filter(expr) => exists_expr = Some(expr),
            _ => return None,
        }
    }
    if triples.len() < 2 {
        return None;
    }

    let (exists_patterns, negated) = resolve_exists_block(exists_expr, exists_inner)?;
    if negated || exists_patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(ex) = &exists_patterns[0] else {
        return None;
    };
    let (sv_ex, p_exists, _ov3) = validate_simple_triple(ex)?;

    // Triples must be ?s p ?o with same ?s and variable object.
    let mut preds: Vec<Ref> = Vec::with_capacity(triples.len());
    for t in &triples {
        let (sv, p, _o) = validate_simple_triple(t)?;
        if sv != sv_ex {
            return None;
        }
        preds.push(p);
    }

    Some((preds, p_exists, out_var))
}

fn detect_object_chain_exists_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be: outer Triple + EXISTS block with 2 triples forming a chain, any order.
    if query.patterns.len() != 2 {
        return None;
    }

    let mut outer: Option<&crate::triple::TriplePattern> = None;
    let mut exists_inner: Option<&[Pattern]> = None;
    let mut exists_expr: Option<&crate::ir::Expression> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => outer = Some(tp),
            Pattern::Exists(inner) => exists_inner = Some(inner),
            Pattern::Filter(expr) => exists_expr = Some(expr),
            _ => return None,
        }
    }
    let outer = outer?;
    let (_sv_outer, pred_outer, b_var) = validate_simple_triple(outer)?;

    let (patterns, negated) = resolve_exists_block(exists_expr, exists_inner)?;
    if negated || patterns.len() != 2 {
        return None;
    }

    let (t1, t2) = match (&patterns[0], &patterns[1]) {
        (Pattern::Triple(a), Pattern::Triple(b)) => (a, b),
        _ => return None,
    };

    // Match chain: ?b p2 ?c . ?c p3 ?d starting at ?b from outer.
    let (b1, pred2, _c, pred3, _d) = find_two_hop_chain(t1, t2)?;
    if b1 != b_var {
        return None;
    }

    Some((pred_outer, pred2, pred3, out_var))
}

fn detect_exists_star_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Vec<Ref>, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be: outer Triple + EXISTS block with 2+ triples, all sharing the same subject var.
    if query.patterns.len() != 2 {
        return None;
    }

    let mut outer: Option<&crate::triple::TriplePattern> = None;
    let mut exists_inner: Option<&[Pattern]> = None;
    let mut exists_expr: Option<&crate::ir::Expression> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => outer = Some(tp),
            Pattern::Exists(inner) => exists_inner = Some(inner),
            Pattern::Filter(expr) => exists_expr = Some(expr),
            _ => return None,
        }
    }
    let outer = outer?;
    let (sv_outer, pred_outer, _ov) = validate_simple_triple(outer)?;

    let (patterns, negated) = resolve_exists_block(exists_expr, exists_inner)?;
    if negated || patterns.len() < 2 {
        return None;
    }

    let mut exists_preds: Vec<Ref> = Vec::with_capacity(patterns.len());
    for pat in patterns {
        let Pattern::Triple(tp) = pat else {
            return None;
        };
        let (sv, pred, _o) = validate_simple_triple(tp)?;
        if sv != sv_outer {
            return None;
        }
        exists_preds.push(pred);
    }

    Some((pred_outer, exists_preds, out_var))
}

/// Detect a linear chain of N ≥ 2 bound-predicate triples for COUNT(*) pushdown.
///
/// Matches: `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ... ?v_{N-1} <pN> ?vN`
/// where all predicates are bound (IRI/SID), all subjects/objects are variables,
/// each triple's object variable equals the next triple's subject variable,
/// and no triple has a datatype constraint.
///
/// Returns `(predicates_in_chain_order, count_output_var)`.
fn detect_linear_chain_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Vec<Ref>, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be all triples, at least 2.
    if query.patterns.len() < 2 {
        return None;
    }
    let mut triples: Vec<&crate::triple::TriplePattern> = Vec::with_capacity(query.patterns.len());
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            _ => return None,
        }
    }

    // All predicates must be bound (IRI/SID), all s/o must be variables, no DTC.
    let n = triples.len();
    let mut subject_vars: Vec<VarId> = Vec::with_capacity(n);
    let mut object_vars: Vec<VarId> = Vec::with_capacity(n);
    for tp in &triples {
        let (sv, _pred, ov) = validate_simple_triple(tp)?;
        subject_vars.push(sv);
        object_vars.push(ov);
    }

    // Find the chain head: the triple whose subject var doesn't appear as any object var.
    let mut head_idx = None;
    for (i, sv) in subject_vars.iter().enumerate() {
        if !object_vars.contains(sv) {
            if head_idx.is_some() {
                return None; // Multiple heads → not a single chain.
            }
            head_idx = Some(i);
        }
    }
    let mut cur = head_idx?;

    // Follow the chain: current object var = next subject var.
    let mut chain: Vec<usize> = Vec::with_capacity(n);
    let mut used = vec![false; n];
    for _ in 0..n {
        chain.push(cur);
        used[cur] = true;
        let ov = object_vars[cur];
        let next = subject_vars
            .iter()
            .enumerate()
            .find(|(j, sv)| !used[*j] && **sv == ov);
        match next {
            Some((j, _)) => cur = j,
            None => break,
        }
    }

    if chain.len() != n {
        return None; // Not all triples form a single chain.
    }

    // Ensure all variables are distinct (no self-joins or cycles).
    let mut seen_vars = std::collections::HashSet::new();
    for &idx in &chain {
        if !seen_vars.insert(subject_vars[idx]) {
            return None;
        }
    }
    if !seen_vars.insert(object_vars[chain[n - 1]]) {
        return None;
    }

    let predicates: Vec<Ref> = chain.iter().map(|&idx| triples[idx].p.clone()).collect();
    Some((predicates, out_var))
}

fn detect_multicolumn_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be exactly two triple patterns and nothing else.
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(t1) = &query.patterns[0] else {
        return None;
    };
    let Pattern::Triple(t2) = &query.patterns[1] else {
        return None;
    };

    // Must be ?s p1 ?o . ?s p2 ?o (same subject var, same object var).
    let (s1, p1, o1) = validate_simple_triple(t1)?;
    let (s2, p2, o2) = validate_simple_triple(t2)?;
    if s1 != s2 {
        return None;
    }
    if o1 != o2 {
        return None;
    }
    if o1 == s1 {
        return None;
    }

    Some((p1, p2, out_var))
}

fn detect_count_blank_node_subjects(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: one Triple + one Filter(ISBLANK(?s)) in canonical order.
    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        _ => return None,
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?s.
    if let Some(v) = input_var {
        if v != *sv {
            return None;
        }
    }

    let crate::ir::Expression::Call { func, args } = filter else {
        return None;
    };
    if *func != crate::ir::Function::IsBlank || args.len() != 1 {
        return None;
    }
    if !matches!(&args[0], crate::ir::Expression::Var(v) if *v == *sv) {
        return None;
    }

    Some(out_var)
}

fn detect_count_literal_objects(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: one Triple + one Filter(ISLITERAL(?o)) in canonical order.
    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        _ => return None,
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?o.
    if let Some(v) = input_var {
        if v != *ov {
            return None;
        }
    }

    let crate::ir::Expression::Call { func, args } = filter else {
        return None;
    };
    if *func != crate::ir::Function::IsLiteral || args.len() != 1 {
        return None;
    }
    if !matches!(&args[0], crate::ir::Expression::Var(v) if *v == *ov) {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_objects(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?o) specifically.
    if in_var != *ov {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_subjects(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?s) specifically.
    if in_var != *sv {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_predicates(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?p) specifically.
    if in_var != *pv {
        return None;
    }

    Some(out_var)
}

fn detect_count_triples(query: &ParsedQuery, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference one of the triple's vars; all are always bound.
    if let Some(v) = input_var {
        if v != *sv && v != *pv && v != *ov {
            return None;
        }
    }

    Some(out_var)
}

fn detect_optional_single_triple_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // Pattern shape: exactly one required triple plus one OPTIONAL with a single triple.
    if query.patterns.len() != 2 {
        return None;
    }

    let mut required: Option<&crate::triple::TriplePattern> = None;
    let mut opt_inner: Option<&crate::triple::TriplePattern> = None;

    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => required = Some(tp),
            Pattern::Optional(inner) => {
                if inner.len() != 1 {
                    return None;
                }
                let Pattern::Triple(tp) = &inner[0] else {
                    return None;
                };
                opt_inner = Some(tp);
            }
            _ => return None,
        }
    }

    let req = required?;
    let opt = opt_inner?;

    let (s1, pred_req, _o1) = validate_simple_triple(req)?;
    let (s2, pred_opt, _o2) = validate_simple_triple(opt)?;
    if s2 != s1 {
        return None;
    }

    Some((pred_req, pred_opt, out_var))
}

fn detect_chain_optional_tail_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // Pattern shape: exactly two required triples + one OPTIONAL with a single triple.
    if query.patterns.len() != 3 {
        return None;
    }

    let mut required_triples: Vec<&crate::triple::TriplePattern> = Vec::new();
    let mut opt_inner: Option<&crate::triple::TriplePattern> = None;

    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => required_triples.push(tp),
            Pattern::Optional(inner) => {
                if inner.len() != 1 {
                    return None;
                }
                let Pattern::Triple(tp) = &inner[0] else {
                    return None;
                };
                opt_inner = Some(tp);
            }
            _ => return None,
        }
    }

    if required_triples.len() != 2 {
        return None;
    }
    let opt = opt_inner?;

    // Identify the 2-hop chain: ?a p1 ?b . ?b p2 ?c
    let (t1, t2) = (required_triples[0], required_triples[1]);
    let (_a, pred1, _b, pred2, c_var) = find_two_hop_chain(t1, t2)?;

    // OPTIONAL tail must be: ?c p3 ?d
    let (c2, pred3, _d) = validate_simple_triple(opt)?;
    if c2 != c_var {
        return None;
    }

    Some((pred1, pred2, pred3, out_var))
}

fn detect_optional_chain_head_join_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // Pattern shape: one required triple + OPTIONAL with two triples (order-independent).
    if query.patterns.len() != 2 {
        return None;
    }

    let mut req: Option<&crate::triple::TriplePattern> = None;
    let mut inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => req = Some(tp),
            Pattern::Optional(v) => inner = Some(v),
            _ => return None,
        }
    }
    let req = req?;
    let inner = inner?;
    if inner.len() != 2 {
        return None;
    }
    let (t1, t2) = match (&inner[0], &inner[1]) {
        (Pattern::Triple(a), Pattern::Triple(b)) => (a, b),
        _ => return None,
    };

    // Required: ?a <p1> ?b
    let (_a, p1, b_var) = validate_simple_triple(req)?;

    // Optional must be a 2-hop chain starting at ?b: ?b <p2> ?c . ?c <p3> ?d (either order).
    let (b1, p2, _c, p3, _d) = find_two_hop_chain(t1, t2)?;
    if b1 != b_var {
        return None;
    }

    tracing::debug!(
        "detected optional chain-head COUNT(*) fast-path (p1={:?}, p2={:?}, p3={:?})",
        p1,
        p2,
        p3
    );
    Some((p1, p2, p3, out_var))
}

fn detect_transitive_path_plus_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(t1) = &query.patterns[0] else {
        return None;
    };
    let Pattern::PropertyPath(pp) = &query.patterns[1] else {
        return None;
    };

    // ?s <p1> ?x
    let (_s, p1, x1) = validate_simple_triple(t1)?;

    // ?x <p2>+ ?o
    let Ref::Var(x2) = &pp.subject else {
        return None;
    };
    if *x2 != x1 {
        return None;
    }
    if pp.modifier != PathModifier::OneOrMore {
        return None;
    }
    let Ref::Var(_o) = &pp.object else {
        return None;
    };

    Some((p1, Ref::Sid(pp.predicate.clone()), out_var))
}

fn detect_property_path_plus_fixed_subject_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(fluree_db_core::Sid, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::PropertyPath(pp) = &query.patterns[0] else {
        return None;
    };
    if pp.modifier != PathModifier::OneOrMore {
        return None;
    }
    if !pp.subject.is_bound() {
        return None;
    }
    let Ref::Var(_o) = &pp.object else {
        return None;
    };
    Some((pp.predicate.clone(), pp.subject.clone(), out_var))
}

fn detect_union_star_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Vec<Ref>, Vec<Ref>, UnionCountMode, VarId)> {
    use crate::ir::{Expression, Function};
    let out_var = detect_count_all_aggregate(query, options)?;

    // Find exactly one UNION pattern.
    let mut union: Option<&Vec<Vec<Pattern>>> = None;
    let mut other: Vec<&Pattern> = Vec::new();
    for p in &query.patterns {
        match p {
            Pattern::Union(branches) => {
                if union.is_some() {
                    return None;
                }
                union = Some(branches);
            }
            _ => other.push(p),
        }
    }
    let branches = union?;
    if branches.len() < 2 {
        return None;
    }

    // Each branch must be exactly one triple: ?s <p> ?o1 (same ?s and same ?o1 across branches).
    let mut subj: Option<VarId> = None;
    let mut obj: Option<VarId> = None;
    let mut union_preds: Vec<Ref> = Vec::with_capacity(branches.len());
    for b in branches {
        if b.len() != 1 {
            return None;
        }
        let Pattern::Triple(tp) = &b[0] else {
            return None;
        };
        let (s, pred, o) = validate_simple_triple(tp)?;
        match subj {
            None => subj = Some(s),
            Some(x) if x != s => return None,
            _ => {}
        }
        match obj {
            None => obj = Some(o),
            Some(x) if x != o => return None,
            _ => {}
        }
        union_preds.push(pred);
    }
    let subj = subj?;
    let obj = obj?;

    // Optional FILTER(?s = ?o1) (either arg order).
    let mut mode = UnionCountMode::AllRows;
    let mut extra_preds: Vec<Ref> = Vec::new();

    for p in other {
        match p {
            Pattern::Filter(expr) => {
                let Expression::Call { func, args } = expr else {
                    return None;
                };
                if *func != Function::Eq || args.len() != 2 {
                    return None;
                }
                let is_s_o = |a: &Expression, b: &Expression| {
                    matches!(a, Expression::Var(v) if *v == subj)
                        && matches!(b, Expression::Var(v) if *v == obj)
                };
                if !(is_s_o(&args[0], &args[1]) || is_s_o(&args[1], &args[0])) {
                    return None;
                }
                mode = UnionCountMode::SubjectEqObject;
            }
            Pattern::Triple(tp) => {
                // Extra required same-subject star predicate(s): ?s <p> ?o2
                let (s, pred, o2) = validate_simple_triple(tp)?;
                if s != subj {
                    return None;
                }
                if o2 == subj || o2 == obj {
                    return None;
                }
                extra_preds.push(pred);
            }
            _ => return None,
        }
    }

    tracing::debug!(
        union_pred_count = union_preds.len(),
        extra_pred_count = extra_preds.len(),
        ?mode,
        "detected UNION-star COUNT(*) fast-path"
    );
    Some((union_preds, extra_preds, mode, out_var))
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
    build_operator_tree_inner(query, options, stats, true)
}

fn detect_minus_outer_single_triple_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Vec<Ref>, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    if query.patterns.len() != 2 {
        return None;
    }

    let mut outer: Option<&crate::triple::TriplePattern> = None;
    let mut minus_inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => outer = Some(tp),
            Pattern::Minus(inner) => minus_inner = Some(inner),
            _ => return None,
        }
    }
    let outer = outer?;
    let minus_inner = minus_inner?;
    let (sv_outer, pred_outer, _ov) = validate_simple_triple(outer)?;

    // MINUS: 1+ triples, all share same subject var as outer.
    if minus_inner.is_empty() {
        return None;
    }
    let mut preds: Vec<Ref> = Vec::with_capacity(minus_inner.len());
    for p in minus_inner {
        let Pattern::Triple(tp) = p else {
            return None;
        };
        let (sv, pred, _o) = validate_simple_triple(tp)?;
        if sv != sv_outer {
            return None;
        }
        preds.push(pred);
    }

    Some((pred_outer, preds, out_var))
}

fn detect_property_minus_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Vec<Ref>, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    if query.patterns.len() < 3 {
        return None;
    }

    let mut triples: Vec<&crate::triple::TriplePattern> = Vec::new();
    let mut minus_inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Minus(inner) => minus_inner = Some(inner),
            _ => return None,
        }
    }
    let minus_inner = minus_inner?;

    // MINUS must be a single triple on the same subject.
    if minus_inner.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp_minus) = &minus_inner[0] else {
        return None;
    };
    let (sv_minus, pred_minus, _o_minus) = validate_simple_triple(tp_minus)?;

    // Outer must be a same-subject star join of 2+ triples, predicates bound, object vars distinct.
    if triples.len() < 2 {
        return None;
    }
    let mut preds: Vec<Ref> = Vec::with_capacity(triples.len());
    let mut seen_obj: std::collections::HashSet<VarId> = std::collections::HashSet::new();

    for tp in triples {
        let (sv, pred, o) = validate_simple_triple(tp)?;
        if sv != sv_minus {
            return None;
        }
        if !seen_obj.insert(o) {
            return None;
        }
        preds.push(pred);
    }

    Some((preds, pred_minus, out_var))
}

fn detect_chain_minus_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    if query.patterns.len() != 3 {
        return None;
    }

    let mut triples: Vec<&crate::triple::TriplePattern> = Vec::new();
    let mut minus_inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Minus(inner) => minus_inner = Some(inner),
            _ => return None,
        }
    }
    if triples.len() != 2 {
        return None;
    }
    let minus_inner = minus_inner?;
    if minus_inner.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp_minus) = &minus_inner[0] else {
        return None;
    };
    let (c_minus, p3, _d) = validate_simple_triple(tp_minus)?;

    // Find chain (?a p1 ?b) and (?b p2 ?c) where ?c matches MINUS subject.
    let (_a, p1, _b, p2, c_var) = find_two_hop_chain(triples[0], triples[1])?;
    if c_var != c_minus {
        return None;
    }

    Some((p1, p2, p3, out_var))
}

fn detect_object_chain_minus_count_all(
    query: &ParsedQuery,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    if query.patterns.len() != 2 {
        return None;
    }

    let mut outer: Option<&crate::triple::TriplePattern> = None;
    let mut minus_inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => outer = Some(tp),
            Pattern::Minus(inner) => minus_inner = Some(inner),
            _ => return None,
        }
    }
    let outer = outer?;
    let minus_inner = minus_inner?;
    if minus_inner.len() != 2 {
        return None;
    }
    let Pattern::Triple(t1) = &minus_inner[0] else {
        return None;
    };
    let Pattern::Triple(t2) = &minus_inner[1] else {
        return None;
    };

    // Outer: ?a p_outer ?b
    let (_a, pred_outer, b_var) = validate_simple_triple(outer)?;

    // MINUS chain: ?b p2 ?c . ?c p3 ?d (must start at ?b from outer)
    let (b1, pred2, _c, pred3, _d) = find_two_hop_chain(t1, t2)?;
    if b1 != b_var {
        return None;
    }

    Some((pred_outer, pred2, pred3, out_var))
}

fn build_operator_tree_inner(
    query: &ParsedQuery,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
    enable_fused_fast_paths: bool,
) -> Result<BoxedOperator> {
    if enable_fused_fast_paths {
        tracing::debug!(
            patterns = ?query.patterns,
            group_by = ?options.group_by,
            agg_count = options.aggregates.len(),
            "operator_tree: considering fused fast paths"
        );
    }

    // Fast-path: `SELECT (SUM(DAY(?o)) AS ?sum) WHERE { ?s <p> ?o }` and friends.
    //
    // These are lowered as: Triple + Bind(expr) + SUM(synthetic_var).
    // This operator scans the predicate's POST range and aggregates directly from encoded values.
    if enable_fused_fast_paths {
        if let Some((pred, scalar, out_var)) = detect_fused_scan_sum_i64(query, options) {
            // Build fallback operator tree without this fast path to preserve correctness in
            // pre-index / history / policy contexts.
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(fused_scan_sum_i64_operator(
                pred,
                scalar,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <p> ?o }`
    // by scanning POST and counting distinct encoded object IDs.
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_distinct_object(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_distinct_object_operator(
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (MIN(?o) AS ?min) WHERE { ?s <p> ?o }` and MAX(...)
    // when the object is string-dict-backed. This inspects only POST leaflet directory keys.
    if enable_fused_fast_paths {
        if let Some((pred, mode, out_var)) = detect_predicate_minmax_string(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(predicate_min_max_string_operator(
                pred,
                mode,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(?s)` / `COUNT(*)` on a single predicate with FILTERs that
    // can be pushed down to encoded pre-filters in `BinaryScanOperator`:
    // - FILTER(?s = ?o)
    // - FILTER(?s != ?o)
    // - FILTER(LANG(?o) = "en")
    //
    // We build a scan that emits no bindings (empty schema) and counts rows.
    if enable_fused_fast_paths {
        if let Some((tp, filters, out_var)) = detect_count_rows_with_encoded_filters(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            let inline_ops: Vec<InlineOperator> =
                filters.into_iter().map(InlineOperator::Filter).collect();
            let scan: BoxedOperator = Box::new(BinaryScanOperator::new_with_emit_and_index(
                tp,
                None,
                inline_ops,
                EmitMask {
                    s: false,
                    p: false,
                    o: false,
                },
                None,
            ));
            return Ok(Box::new(CountRowsOperator::new(
                scan,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?x) AS ?c) WHERE { ?s <p> ?o }` (and COUNT(*))
    // answered from PSOT leaflet directory row counts (no scan / no decoding).
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_rows(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_rows_operator(pred, out_var, Some(fallback))));
        }
    }

    // Count-only plan: generic join-aware count planner that replaces many bespoke
    // detect_*/fast_* pairs below. Fires after trivial metadata-only counts but before
    // the per-shape cascade.
    if enable_fused_fast_paths {
        if let Some(plan) = crate::count_plan::try_build_count_plan(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(crate::count_plan_exec::count_plan_operator(plan, Some(fallback)));
        }
    }

    // Fast-path: `COUNT(*)` with correlated EXISTS constraints.
    //
    // These cover the sparqloscope `exists-*` DBLP benchmark family and avoid scanning
    // the full outer solution stream just to count rows.
    if enable_fused_fast_paths {
        if let Some((outer_pred, exists_pred, out_var)) =
            detect_exists_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(exists_join_count_all_operator(
                outer_pred,
                exists_pred,
                out_var,
                Some(fallback),
            )));
        }
        if let Some((p1, p2, p3, out_var)) = detect_chain_exists_join_count_all(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(chain_exists_join_count_all_operator(
                p1,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
        if let Some((preds, p_exists, out_var)) = detect_star_exists_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(star_exists_join_count_all_operator(
                preds,
                p_exists,
                out_var,
                Some(fallback),
            )));
        }
        if let Some((p_outer, p2, p3, out_var)) =
            detect_object_chain_exists_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(object_chain_exists_join_count_all_operator(
                p_outer,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
        if let Some((p_outer, exists_preds, out_var)) =
            detect_exists_star_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(exists_star_join_count_all_operator(
                p_outer,
                exists_preds,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` for a single-triple outer pattern with a MINUS star block
    // on the same subject var (removes subjects with any MINUS match).
    if enable_fused_fast_paths {
        if let Some((outer_pred, minus_preds, out_var)) =
            detect_minus_outer_single_triple_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(minus_join_count_all_operator(
                outer_pred,
                minus_preds,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` for a same-subject star join with a MINUS single-triple constraint.
    if enable_fused_fast_paths {
        if let Some((outer_preds, minus_pred, out_var)) =
            detect_property_minus_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(property_minus_count_all_operator(
                outer_preds,
                minus_pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` for a 2-hop chain with MINUS on the tail var.
    if enable_fused_fast_paths {
        if let Some((p1, p2, p3, out_var)) = detect_chain_minus_count_all(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(chain_minus_count_all_operator(
                p1,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` where outer object must NOT satisfy a 2-hop chain (MINUS chain).
    if enable_fused_fast_paths {
        if let Some((p_outer, p2, p3, out_var)) =
            detect_object_chain_minus_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(object_chain_minus_count_all_operator(
                p_outer,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` for an N-hop linear join chain (?a p1 ?b . ?b p2 ?c . ...).
    if enable_fused_fast_paths {
        if let Some((predicates, out_var)) = detect_linear_chain_count_all(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(linear_chain_count_all_operator(
                predicates,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` for a 2-pattern multicolumn join `?s p1 ?o . ?s p2 ?o`.
    if enable_fused_fast_paths {
        if let Some((p1, p2, out_var)) = detect_multicolumn_join_count_all(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(multicolumn_join_count_all_operator(
                p1,
                p2,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(DISTINCT ?o1)` with an existence-only same-subject join.
    if enable_fused_fast_paths {
        if let Some((count_pred, exists_pred, out_var)) =
            detect_exists_join_count_distinct_object(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(exists_join_count_distinct_object_operator(
                count_pred,
                exists_pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o FILTER ISBLANK(?s) }`
    // answered from SPOT leaflet metadata by scanning the blank-node SubjectId range.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_blank_node_subjects(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_blank_node_subjects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?o) AS ?c) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`
    // answered from PSOT leaflet metadata by counting non-node-ref `o_type` rows.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_literal_objects(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_literal_objects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from SPOT leaflet `lead_group_count` + boundary correction.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_subjects(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_distinct_subjects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from PSOT leaflet `p_const` transitions.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_predicates(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_distinct_predicates_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only by summing leaf row_count across a branch manifest.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_triples(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_triples_operator(out_var, Some(fallback))));
        }
    }

    // NOTE: star+OPTIONAL single-triple COUNT(*) queries are now covered by the more generic
    // `PropertyJoinCountAllOperator` fast path (same-subject star join with OPTIONAL factors),
    // so this specialized fast path can be retired once we have enough coverage confidence.
    if enable_fused_fast_paths {
        if let Some((pred_required, pred_optional, out_var)) =
            detect_optional_single_triple_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(predicate_optional_join_count_all(
                pred_required,
                pred_optional,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?a <p1> ?b . ?b <p2> ?c . OPTIONAL { ?c <p3> ?d } }`
    // answered by streaming group counts and a small `p3` multiplier map.
    if enable_fused_fast_paths {
        if let Some((p1, p2, p3, out_var)) =
            detect_chain_optional_tail_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(predicate_chain_optional_tail_count_all(
                p1,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d } }`
    // answered by streaming group counts and an `n3(c)` map.
    if enable_fused_fast_paths {
        if let Some((p1, p2, p3, out_var)) =
            detect_optional_chain_head_join_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(predicate_optional_chain_head_count_all(
                p1,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { <S> <p>+ ?o }`
    // Avoids repeated range scans by building adjacency once and traversing.
    if enable_fused_fast_paths {
        if let Some((pred_sid, subject, out_var)) =
            detect_property_path_plus_fixed_subject_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(property_path_plus_count_all_operator(
                pred_sid,
                subject,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: UNION-of-triples optionally constrained by same-subject star joins and/or FILTER(?s = ?o).
    if enable_fused_fast_paths {
        if let Some((union_preds, extra_preds, mode, out_var)) =
            detect_union_star_count_all(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(UnionStarCountAllOperator::new(
                union_preds,
                extra_preds,
                mode,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?s <p1> ?x . ?x <p2>+ ?o }`
    // Avoids closure materialization by counting reachability.
    if enable_fused_fast_paths {
        if let Some((p1, p2, out_var)) = detect_transitive_path_plus_count_all(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(transitive_path_plus_count_all_operator(
                p1,
                p2,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from OPST leaflet `lead_group_count` + boundary correction.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_objects(query, options) {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(count_distinct_objects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `?s <p_group> ?o GROUP BY ?o` top-k with same-subject star constraints:
    // `?s <p_group> ?o . ?s <p_filter1> ?x1 . ...`
    //
    // Avoids join materialization and generic group-by for common benchmark shapes.
    if enable_fused_fast_paths {
        if let Some((
            group_pred,
            filter_preds,
            select_schema,
            _s_var,
            o_var,
            count_var,
            min_var,
            max_var,
            sample_var,
            limit,
        )) = detect_group_by_object_star_topk(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(GroupByObjectStarTopKOperator::new(
                group_pred,
                filter_preds,
                o_var,
                count_var,
                min_var,
                max_var,
                sample_var,
                limit,
                select_schema,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `?s <p> ?o GROUP BY ?o COUNT(?s)` top-k using leaflet FIRST headers.
    //
    // This avoids decoding leaflets for long (p,o) runs that span leaflet boundaries.
    if let Some((pred, s_var, o_var, count_var, limit)) =
        detect_predicate_group_by_object_count_topk(query, options)
    {
        return Ok(Box::new(PredicateGroupCountFirstsOperator::new(
            s_var,
            o_var,
            count_var,
            pred,
            limit,
            stats.clone(),
        )));
    }

    // Fast-path: SUM(STRLEN(GROUP_CONCAT(...))) over a single predicate.
    if enable_fused_fast_paths {
        if let Some((pred, sep, out_var)) = detect_sum_strlen_group_concat_subquery(query, options)
        {
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(sum_strlen_group_concat_operator(
                pred,
                sep,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s <p> <o> }` using leaflet FIRST headers.
    if let Some((pred, s_var, obj, count_var)) = detect_predicate_object_count(query, options) {
        let mut operator: BoxedOperator = Box::new(PredicateObjectCountFirstsOperator::new(
            pred,
            s_var,
            obj,
            count_var,
            stats.clone(),
        ));

        // ORDER BY
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

    // Fast-path: same-subject star join COUNT(*) (required triples + OPTIONAL single-triple groups)
    // without join-row materialization.
    //
    // Safe for SPARQL multiplicity semantics:
    // - required: COUNT(*) = sum_s Π_i count_pi(s)
    // - OPTIONAL single triple: multiply by max(1, count_p(s)) per optional predicate.
    if enable_fused_fast_paths {
        if let Some((s, required, optional_groups, count_var)) =
            detect_property_join_count_all(query, options)
        {
            tracing::debug!("detected property-join COUNT(*) fast-path");
            let fallback = build_operator_tree_inner(query, options, stats.clone(), false)?;
            return Ok(Box::new(PropertyJoinCountAllOperator::new(
                s,
                required,
                optional_groups,
                count_var,
                Some(fallback),
            )));
        }
    }

    // Compute per-operator downstream dependency sets for trimming.
    // Done before building WHERE operators so we can push projection into the WHERE clause.
    let variable_deps = compute_variable_deps(query, options);

    // Build WHERE clause operators with projection pushdown
    let required_where_vars = variable_deps
        .as_ref()
        .map(|d| d.required_where_vars.as_slice());
    // needed-vars for WHERE planning: derived from variable_deps when available,
    // otherwise treat all WHERE-bound vars as needed (wildcard/boolean/construct cases).
    let mut needed_where_vars: std::collections::HashSet<VarId> = std::collections::HashSet::new();
    if let Some(req) = required_where_vars {
        needed_where_vars.extend(req.iter().copied());
    } else {
        let mut counts: std::collections::HashMap<VarId, usize> = std::collections::HashMap::new();
        let mut vars: std::collections::HashSet<VarId> = std::collections::HashSet::new();
        collect_var_stats(&query.patterns, &mut counts, &mut vars);
        vars.extend(counts.keys().copied());
        needed_where_vars = vars;
    }

    let mut operator = build_where_operators_with_needed(
        &query.patterns,
        stats,
        &needed_where_vars,
        &options.group_by,
        required_where_vars,
    )?;

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
                    distinct: spec.distinct,
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
            let partitioned = detect_partitioned_group_by(query, options);
            tracing::debug!(
                group_by_count = options.group_by.len(),
                agg_count = streaming_specs.len(),
                partitioned,
                "using streaming GroupAggregateOperator"
            );
            // GroupAggregateOperator replaces both GroupBy and Aggregate,
            // so use required_aggregate_vars (what the combined output must contain).
            operator = Box::new(
                GroupAggregateOperator::new(
                    operator,
                    options.group_by.clone(),
                    streaming_specs,
                    None, // graph_view - will be set from context if needed
                    partitioned,
                )
                .with_out_schema(
                    variable_deps
                        .as_ref()
                        .map(|d| d.required_aggregate_vars.as_slice()),
                ),
            );
        } else {
            // Traditional path: GroupByOperator + AggregateOperator
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
    }

    // HAVING (filter on aggregated results)
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
    if !options.post_binds.is_empty() {
        for (i, (var, expr)) in options.post_binds.iter().enumerate() {
            operator = Box::new(
                crate::bind::BindOperator::new(operator, *var, expr.clone(), vec![])
                    .with_out_schema(
                        variable_deps
                            .as_ref()
                            .and_then(|d| d.required_bind_vars.get(i))
                            .map(|v| v.as_slice()),
                    ),
            );
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
        operator = Box::new(
            SortOperator::new(operator, options.order_by.clone()).with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_sort_vars.as_slice()),
            ),
        );
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
