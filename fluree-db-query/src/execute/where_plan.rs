//! WHERE clause planning and operator building
//!
//! Builds operators for WHERE clause patterns including:
//! - Triple patterns (ScanOperator, NestedLoopJoinOperator)
//! - Filter patterns (FilterOperator)
//! - Optional patterns (OptionalOperator)
//! - Values patterns (ValuesOperator)
//! - Bind patterns (BindOperator)
//! - Union patterns (UnionOperator)
//! - And more...

use crate::bind::BindOperator;
use crate::bm25::Bm25SearchOperator;
use crate::error::{QueryError, Result};
use crate::exists::ExistsOperator;
use crate::filter::FilterOperator;
use crate::ir::{Expression, Pattern};
use crate::join::NestedLoopJoinOperator;
use crate::minus::MinusOperator;
use crate::operator::BoxedOperator;
use crate::optional::{OptionalOperator, PlanTreeOptionalBuilder};
use crate::planner::{is_property_join, reorder_patterns};
use crate::property_join::PropertyJoinOperator;
use crate::property_path::{PropertyPathOperator, DEFAULT_MAX_VISITED};
use crate::seed::EmptyOperator;
use crate::subquery::SubqueryOperator;
use crate::triple::TriplePattern;
use crate::union::UnionOperator;
use crate::values::ValuesOperator;
use crate::var_registry::VarId;
use fluree_db_core::{ObjectBounds, StatsView};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::pushdown::extract_bounds_from_filters;

// ============================================================================
// Inner join block result type
// ============================================================================

/// A single VALUES clause: its bound variables and constant rows.
pub struct ValuesPattern {
    pub vars: Vec<VarId>,
    pub rows: Vec<Vec<crate::binding::Binding>>,
}

impl ValuesPattern {
    pub fn new(vars: Vec<VarId>, rows: Vec<Vec<crate::binding::Binding>>) -> Self {
        Self { vars, rows }
    }
}

/// Result of collecting an inner-join block from a pattern list.
///
/// Contains all the components needed to build a joined block of patterns.
pub struct InnerJoinBlock {
    /// Index past the last consumed pattern
    pub end_index: usize,
    /// VALUES patterns
    pub values: Vec<ValuesPattern>,
    /// Triple patterns
    pub triples: Vec<TriplePattern>,
    /// BIND patterns (var and expression)
    pub binds: Vec<(VarId, Expression)>,
    /// FILTER expressions
    pub filters: Vec<Expression>,
}

// ============================================================================
// Helper functions to reduce duplication in build_where_operators_seeded
// ============================================================================

/// Require a child operator, returning an error if None.
///
/// This helper eliminates the repeated pattern of:
/// ```ignore
/// let child = operator.ok_or_else(|| {
///     QueryError::InvalidQuery("XXX has no input operator".to_string())
/// })?;
/// ```
#[inline]
fn require_child(operator: Option<BoxedOperator>, pattern_name: &str) -> Result<BoxedOperator> {
    operator
        .ok_or_else(|| QueryError::InvalidQuery(format!("{} has no input operator", pattern_name)))
}

/// Get an operator or create an empty seed if None.
///
/// Used for patterns that can appear at position 0 and need an initial solution.
#[inline]
fn get_or_empty_seed(operator: Option<BoxedOperator>) -> BoxedOperator {
    operator.unwrap_or_else(|| Box::new(EmptyOperator::new()))
}

/// Get bound variables from an operator's schema.
#[inline]
fn bound_vars_from_operator(operator: &Option<BoxedOperator>) -> HashSet<VarId> {
    operator
        .as_ref()
        .map(|op| op.schema().iter().copied().collect())
        .unwrap_or_default()
}

/// Apply VALUES patterns on top of an existing operator.
///
/// Each VALUES pattern wraps the current operator with a `ValuesOperator`,
/// creating an empty seed if no operator exists yet.
fn apply_values(
    operator: Option<BoxedOperator>,
    block_values: Vec<ValuesPattern>,
) -> Option<BoxedOperator> {
    let mut operator = operator;
    for vp in block_values {
        let child = get_or_empty_seed(operator.take());
        operator = Some(Box::new(ValuesOperator::new(child, vp.vars, vp.rows)));
    }
    operator
}

/// Pending BIND expression waiting to be applied once its required variables are bound.
///
/// BINDs are applied in order after the triple patterns that bind their dependencies.
#[derive(Debug, Clone)]
struct PendingBind {
    /// Variables that must be bound before this BIND can execute
    required_vars: HashSet<VarId>,
    /// The variable being bound by this expression
    target_var: VarId,
    /// The expression to evaluate
    expr: Expression,
}

impl PendingBind {
    fn new(var: VarId, expr: Expression) -> Self {
        let required_vars = expr.variables().into_iter().collect();
        Self {
            required_vars,
            target_var: var,
            expr,
        }
    }
}

/// Pending FILTER expression waiting to be applied once its required variables are bound.
///
/// Filters are tracked with their original index so that filters "consumed" by pushdown
/// (converted to index scan bounds) can be identified and skipped during operator application.
#[derive(Debug, Clone)]
struct PendingFilter {
    /// Original index in the block's filter list (for pushdown tracking)
    original_idx: usize,
    /// Variables that must be bound before this filter can execute
    required_vars: HashSet<VarId>,
    /// The filter expression to evaluate
    expr: Expression,
}

impl PendingFilter {
    fn new(original_idx: usize, expr: Expression) -> Self {
        let required_vars = expr.variables().into_iter().collect();
        Self {
            original_idx,
            required_vars,
            expr,
        }
    }
}

/// Partition pending filters into those eligible for inline evaluation and those still waiting.
///
/// Filters consumed by pushdown are silently dropped. Filters whose required
/// variables are all in `bound` are returned as the first element (ready);
/// the rest are returned as the second element (still pending).
fn partition_eligible_filters(
    filters: Vec<PendingFilter>,
    bound: &HashSet<VarId>,
    filter_idxs_consumed: &[usize],
) -> (Vec<Expression>, Vec<PendingFilter>) {
    let mut ready = Vec::new();
    let mut pending = Vec::new();
    for pf in filters {
        if filter_idxs_consumed.contains(&pf.original_idx) {
            continue;
        }
        if pf.required_vars.is_subset(bound) {
            ready.push(pf.expr);
        } else {
            pending.push(pf);
        }
    }
    (ready, pending)
}

/// Apply eligible BINDs whose required variables are all bound.
///
/// Each ready BIND is fused with any filters that become ready once the BIND's
/// target variable enters `bound`.  Returns the updated operator, any BINDs
/// whose dependencies are not yet satisfied, and the remaining pending filters.
fn apply_eligible_binds(
    mut child: BoxedOperator,
    bound: &mut HashSet<VarId>,
    pending_binds: Vec<PendingBind>,
    mut pending_filters: Vec<PendingFilter>,
    filter_idxs_consumed: &[usize],
) -> (BoxedOperator, Vec<PendingBind>, Vec<PendingFilter>) {
    let mut remaining_binds = Vec::new();

    for pending in pending_binds {
        if pending.required_vars.is_subset(bound) {
            bound.insert(pending.target_var);

            let (bind_filters, still_pending) =
                partition_eligible_filters(pending_filters, bound, filter_idxs_consumed);
            pending_filters = still_pending;

            child = Box::new(BindOperator::new(
                child,
                pending.target_var,
                pending.expr,
                bind_filters,
            ));
        } else {
            remaining_binds.push(pending);
        }
    }

    (child, remaining_binds, pending_filters)
}

/// Apply pending BINDs and FILTERs that are ready (all required vars are bound).
///
/// Returns the updated operator and the remaining pending items.
fn apply_deferred_patterns(
    child: BoxedOperator,
    bound: &mut HashSet<VarId>,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    filter_idxs_consumed: &[usize],
) -> (BoxedOperator, Vec<PendingBind>, Vec<PendingFilter>) {
    let (mut child, remaining_binds, pending_filters) = apply_eligible_binds(
        child,
        bound,
        pending_binds,
        pending_filters,
        filter_idxs_consumed,
    );

    let (ready, remaining_filters) =
        partition_eligible_filters(pending_filters, bound, filter_idxs_consumed);
    for expr in ready {
        child = Box::new(FilterOperator::new(child, expr));
    }

    (child, remaining_binds, remaining_filters)
}

/// Apply all remaining BINDs and FILTERs at the end of a block.
///
/// Filters are fused into each BindOperator when the BIND's target variable
/// is the last dependency the filter was waiting on.  Any filters still
/// pending after all BINDs are applied as standalone FilterOperators.
fn apply_all_remaining(
    child: BoxedOperator,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    filter_idxs_consumed: &[usize],
) -> BoxedOperator {
    let mut bound: HashSet<VarId> = child.schema().iter().copied().collect();

    let (mut child, _, remaining_filters) = apply_eligible_binds(
        child,
        &mut bound,
        pending_binds,
        pending_filters,
        filter_idxs_consumed,
    );
    for pending in remaining_filters {
        if !filter_idxs_consumed.contains(&pending.original_idx) {
            child = Box::new(FilterOperator::new(child, pending.expr));
        }
    }
    child
}

/// Build an operator for a single non-block pattern (BIND, VALUES, or Triple).
///
/// Used when `collect_inner_join_block` consumes zero patterns because the
/// current pattern is not safe to hoist. Processes one pattern and returns.
fn build_single_pattern(
    operator: Option<BoxedOperator>,
    pattern: &Pattern,
) -> Option<BoxedOperator> {
    match pattern {
        Pattern::Bind { var, expr } => {
            let child = get_or_empty_seed(operator);
            Some(Box::new(BindOperator::new(
                child,
                *var,
                expr.clone(),
                vec![],
            )))
        }
        Pattern::Values { vars, rows } => {
            let child = get_or_empty_seed(operator);
            Some(Box::new(ValuesOperator::new(
                child,
                vars.clone(),
                rows.clone(),
            )))
        }
        Pattern::Triple(tp) => Some(build_scan_or_join(
            operator,
            tp,
            &HashMap::new(),
            Vec::new(),
        )),
        _ => operator,
    }
}

/// Build an operator tree for a property-join-eligible block of triples.
///
/// Constructs a `PropertyJoinOperator` for the triples, then layers deferred
/// VALUES and any ready BINDs/FILTERs on top.
fn build_property_join_block(
    operator: Option<BoxedOperator>,
    triples: &[TriplePattern],
    block_values: Vec<ValuesPattern>,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    filter_idxs_consumed: &[usize],
) -> Result<Option<BoxedOperator>> {
    let mut operator = Some(build_triple_operators(operator, triples, object_bounds)?);

    if !block_values.is_empty() {
        operator = apply_values(operator, block_values);
    }

    let mut bound = bound_vars_from_operator(&operator);
    if let Some(child) = operator.take() {
        let (child, _, _) = apply_deferred_patterns(
            child,
            &mut bound,
            pending_binds,
            pending_filters,
            filter_idxs_consumed,
        );
        operator = Some(child);
    }

    Ok(operator)
}

/// Build an operator tree for a sequential scan/join block of triples.
///
/// Applies VALUES first (if any), then iterates triples building scan/join
/// operators, inlining eligible filters into each step and applying deferred
/// BINDs/FILTERs as their dependencies become bound.
fn build_sequential_join_block(
    operator: Option<BoxedOperator>,
    triples: &[TriplePattern],
    block_values: Vec<ValuesPattern>,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    filter_idxs_consumed: &[usize],
) -> Result<Option<BoxedOperator>> {
    let mut operator = operator;

    if !block_values.is_empty() {
        operator = apply_values(operator, block_values);
    }

    let mut bound = bound_vars_from_operator(&operator);
    let mut pending_binds = pending_binds;
    let mut pending_filters = pending_filters;

    for tp in triples {
        let mut vars_after: HashSet<VarId> = bound.clone();
        for v in tp.variables() {
            vars_after.insert(v);
        }

        let (inlined, remaining) =
            partition_eligible_filters(pending_filters, &vars_after, filter_idxs_consumed);
        pending_filters = remaining;

        operator = Some(build_scan_or_join(operator, tp, object_bounds, inlined));

        for v in tp.variables() {
            bound.insert(v);
        }

        if let Some(child) = operator.take() {
            let (child, new_binds, new_filters) = apply_deferred_patterns(
                child,
                &mut bound,
                pending_binds,
                pending_filters,
                filter_idxs_consumed,
            );
            pending_binds = new_binds;
            pending_filters = new_filters;
            operator = Some(child);
        }
    }

    if !pending_binds.is_empty() || !pending_filters.is_empty() {
        let child = require_child(operator, "Filters")?;
        operator = Some(apply_all_remaining(
            child,
            pending_binds,
            pending_filters,
            filter_idxs_consumed,
        ));
    }

    Ok(operator)
}

/// Build operators for WHERE clause patterns
///
/// Handles pattern types and builds appropriate operators:
/// - Triple patterns: ScanOperator or NestedLoopJoinOperator
/// - Filter patterns: FilterOperator
/// - Optional patterns: OptionalOperator
/// - Values patterns: ValuesOperator
/// - Bind patterns: BindOperator
/// - Union patterns: UnionOperator
///
/// # Non-Triple Start Support
///
/// When the first pattern is not a Triple (e.g., VALUES, BIND, UNION, FILTER),
/// we start with an EmptyOperator that yields a single empty solution.
/// This allows these patterns to work at position 0.
///
/// # Scoped Reordering
///
/// Pattern reordering is scoped and *explicitly bounded*:
/// - We only reorder within a contiguous block of `Triple` patterns (and `VALUES`, and "safe" `Filter`s)
/// - We stop the block at the first "unsafe" FILTER (references vars not yet bound)
/// - We do not reorder across non-inner-join patterns (OPTIONAL/UNION/MINUS/EXISTS/GRAPH/BIND/etc.)
///
/// This keeps semantics stable while still allowing aggressive optimization inside
/// the regions where it is safe.
pub fn build_where_operators(
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator> {
    build_where_operators_seeded(None, patterns, stats)
}

/// Collect an optimizable inner-join block consisting of:
/// - `VALUES` (SPARQL VALUES)
/// - `Triple`
/// - `BIND` (when safe - all referenced variables already bound)
/// - `FILTER`s (all filters, regardless of variable binding status)
///
/// FILTERs are collected unconditionally. Filters referencing variables not yet bound
/// in left-to-right order will be tracked via `PendingFilter` and applied later when
/// their required variables become bound. This allows users to write FILTER patterns
/// anywhere in the WHERE clause - the system automatically moves each filter to execute
/// immediately after all of its required variables are bound.
///
/// A BIND is considered safe to include in the block if **all** variables referenced
/// by its expression are already bound by preceding patterns (original order). BINDs
/// with unbound inputs cannot be deferred - they must fail at their original position.
///
/// `VALUES` is always safe to include because it is an inner-join constraint/seed.
pub fn collect_inner_join_block(patterns: &[Pattern], start: usize) -> InnerJoinBlock {
    let mut i = start;
    let mut values: Vec<ValuesPattern> = Vec::new();
    let mut triples: Vec<TriplePattern> = Vec::new();
    let mut binds: Vec<(VarId, Expression)> = Vec::new();
    let mut filters: Vec<Expression> = Vec::new();
    let mut bound_vars: HashSet<VarId> = HashSet::new();

    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Values { vars, rows } => {
                // VALUES binds its vars immediately (join seed/constraint).
                bound_vars.extend(vars.iter().copied());
                values.push(ValuesPattern::new(vars.clone(), rows.clone()));
                i += 1;
            }
            Pattern::Triple(tp) => {
                // Triples add bindings (subject/object vars) to the local bound set.
                bound_vars.extend(tp.variables());
                triples.push(tp.clone());
                i += 1;
            }
            Pattern::Bind { var, expr } => {
                let vars: HashSet<VarId> = expr.variables().into_iter().collect();
                if vars.is_subset(&bound_vars) {
                    // Safe to move within block: inputs already bound.
                    binds.push((*var, expr.clone()));
                    bound_vars.insert(*var);
                    i += 1;
                } else {
                    // Unsafe to move this BIND: it depends on vars not yet bound.
                    break;
                }
            }
            Pattern::Filter(expr) => {
                // Collect all filters unconditionally. Filters referencing variables
                // not yet bound will be tracked via PendingFilter and applied later
                // when their required variables become bound (after subsequent triples).
                // This allows users to write FILTER patterns anywhere in the WHERE
                // clause - the system automatically moves each filter to execute
                // immediately after all of its required variables are bound.
                filters.push(expr.clone());
                i += 1;
            }
            _ => break,
        }
    }

    InnerJoinBlock {
        end_index: i,
        values,
        triples,
        binds,
        filters,
    }
}

/// Internal helper to build WHERE operators with an optional initial seed operator.
///
/// - If `seed` is `Some`, it is used as the starting operator for the pattern list.
/// - If `seed` is `None` and the first pattern is non-triple, an `EmptyOperator` is used.
/// - `stats` provides property/class statistics for selectivity-based pattern reordering.
///   Using `Arc` avoids expensive HashMap cloning when threading stats to nested operators.
pub fn build_where_operators_seeded(
    seed: Option<BoxedOperator>,
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator> {
    if patterns.is_empty() {
        // Empty patterns = one row with empty schema
        return Ok(seed.unwrap_or_else(|| Box::new(EmptyOperator::new())));
    }

    // Apply generalized pattern reordering upfront for all pattern lists.
    //
    // reorder_patterns determines optimal placement of all patterns
    // (triples, compound patterns like UNION/OPTIONAL/MINUS/EXISTS/Subquery)
    // using selectivity-based cost estimation. This subsumes the per-block
    // reorder_patterns_seeded calls that previously handled triple-only blocks.
    let initial_bound = seed
        .as_ref()
        .map(|op| op.schema().iter().copied().collect::<HashSet<_>>())
        .unwrap_or_default();
    let reordered_storage = reorder_patterns(patterns, stats.as_deref(), &initial_bound);
    let patterns = &reordered_storage;

    // If no explicit seed, determine if we need an empty seed.
    //
    // We only need a synthetic empty seed when the first pattern *requires* an upstream
    // operator. Triples/VALUES/BIND can build their own empty seed on demand via
    // `get_or_empty_seed(...)`, and pre-inserting an EmptyOperator would incorrectly
    // block PropertyJoinOperator selection (because `operator.is_none()` would be false).
    let needs_empty_seed = seed.is_none()
        && !matches!(
            patterns.first(),
            Some(Pattern::Triple(_)) | Some(Pattern::Values { .. }) | Some(Pattern::Bind { .. })
        );

    // Start with provided seed, else start with empty operator if needed
    let mut operator: Option<BoxedOperator> = if let Some(seed) = seed {
        Some(seed)
    } else if needs_empty_seed {
        Some(Box::new(EmptyOperator::new()))
    } else {
        None
    };

    let mut i = 0;
    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Triple(_) | Pattern::Values { .. } | Pattern::Bind { .. } => {
                let start = i;
                let block = collect_inner_join_block(patterns, start);
                let end = block.end_index;

                // `collect_inner_join_block` may consume *zero* patterns when the
                // current pattern is not safe to hoist (e.g. BIND with unbound vars).
                // Fall back to processing one pattern to ensure `i` advances.
                if end == start {
                    operator = build_single_pattern(operator.take(), &patterns[start]);
                    i = start + 1;
                    continue;
                }
                i = end;

                // Hot path: triples only (no BIND/FILTER).
                // Skip dependency bookkeeping entirely.
                if block.binds.is_empty() && block.filters.is_empty() {
                    if !block.values.is_empty() {
                        operator = apply_values(operator.take(), block.values);
                    }
                    operator = Some(build_triple_operators(
                        operator,
                        &block.triples,
                        &HashMap::new(),
                    )?);
                    continue;
                }

                let pending_binds: Vec<PendingBind> = block
                    .binds
                    .into_iter()
                    .map(|(var, expr)| PendingBind::new(var, expr))
                    .collect();
                let pending_filters: Vec<PendingFilter> = block
                    .filters
                    .into_iter()
                    .enumerate()
                    .map(|(idx, expr)| PendingFilter::new(idx, expr))
                    .collect();

                // Push down range-safe filters into object bounds (when possible).
                let filters_for_pushdown: Vec<Expression> =
                    pending_filters.iter().map(|f| f.expr.clone()).collect();
                let (object_bounds, filter_idxs_consumed) =
                    extract_bounds_from_filters(&block.triples, &filters_for_pushdown);

                // Ensure a concrete seed when only BIND/FILTER remain (no triples, no upstream).
                if block.triples.is_empty() && operator.is_none() {
                    operator = Some(Box::new(EmptyOperator::new()));
                }

                let can_property_join = operator.is_none()
                    && block.triples.len() >= 2
                    && is_property_join(&block.triples);

                if can_property_join {
                    operator = build_property_join_block(
                        operator,
                        &block.triples,
                        block.values,
                        pending_binds,
                        pending_filters,
                        &object_bounds,
                        &filter_idxs_consumed,
                    )?;
                } else {
                    operator = build_sequential_join_block(
                        operator,
                        &block.triples,
                        block.values,
                        pending_binds,
                        pending_filters,
                        &object_bounds,
                        &filter_idxs_consumed,
                    )?;
                }
            }

            Pattern::Filter(expr) => {
                // Wrap current operator with filter
                let child = require_child(operator, "Filter pattern")?;
                operator = Some(Box::new(FilterOperator::new(child, expr.clone())));
                i += 1;
            }

            Pattern::Optional(_inner_patterns) => {
                // OPTIONAL with conjunctive semantics: all inner patterns must match together.
                //
                // The parser now creates separate Optional patterns for each node-map in
                // `["optional", {node1}, {node2}]`, so by the time we get here, each Optional
                // contains patterns from a SINGLE node-map (conjunctive group).
                //
                // Two paths:
                // 1. Fast path: single triple pattern uses PatternOptionalBuilder (direct scan)
                // 2. General path: multi-pattern uses PlanTreeOptionalBuilder (full operator tree)
                let child = require_child(operator, "OPTIONAL pattern")?;

                if let Pattern::Optional(inner_patterns) = &patterns[i] {
                    let required_schema = Arc::from(child.schema().to_vec().into_boxed_slice());

                    // Fast path: single triple pattern
                    if inner_patterns.len() == 1 {
                        if let Some(inner_triple) = inner_patterns[0].as_triple().cloned() {
                            operator = Some(Box::new(OptionalOperator::new(
                                child,
                                required_schema,
                                inner_triple,
                            )));
                            i += 1;
                            continue;
                        }
                    }

                    // General path: use PlanTreeOptionalBuilder for multi-pattern or
                    // non-triple single patterns (VALUES, BIND, subquery, etc.)
                    let builder = PlanTreeOptionalBuilder::new(
                        required_schema.clone(),
                        inner_patterns.clone(),
                        stats.clone(),
                    );
                    operator = Some(Box::new(OptionalOperator::with_builder(
                        child,
                        required_schema,
                        Box::new(builder),
                    )));
                    i += 1;
                    continue;
                }

                unreachable!("match arm ensures Pattern::Optional")
            }

            Pattern::Union(branches) => {
                let child = require_child(operator, "UNION pattern")?;
                if branches.is_empty() {
                    return Err(QueryError::InvalidQuery(
                        "UNION requires at least one branch".to_string(),
                    ));
                }
                // Correlated UNION: execute each branch per input row (seeded from child).
                operator = Some(Box::new(UnionOperator::new(
                    child,
                    branches.clone(),
                    stats.clone(),
                )));
                i += 1;
            }

            Pattern::Minus(inner_patterns) => {
                // MINUS - anti-join semantics (set difference)
                let child = require_child(operator, "MINUS pattern")?;
                operator = Some(Box::new(MinusOperator::new(
                    child,
                    inner_patterns.clone(),
                    stats.clone(),
                )));
                i += 1;
            }

            // EXISTS and NOT EXISTS share the same operator, differing only by a boolean flag
            Pattern::Exists(inner_patterns) | Pattern::NotExists(inner_patterns) => {
                let child = require_child(operator, "EXISTS pattern")?;
                let negated = matches!(&patterns[i], Pattern::NotExists(_));
                operator = Some(Box::new(ExistsOperator::new(
                    child,
                    inner_patterns.clone(),
                    negated,
                    stats.clone(),
                )));
                i += 1;
            }

            Pattern::PropertyPath(pp) => {
                // Property path - transitive graph traversal
                // Pass existing operator as child for correlation
                operator = Some(Box::new(PropertyPathOperator::new(
                    operator,
                    pp.clone(),
                    DEFAULT_MAX_VISITED,
                )));
                i += 1;
            }

            Pattern::Subquery(sq) => {
                // Subquery - execute nested query and merge results
                let child = require_child(operator, "SUBQUERY pattern")?;
                operator = Some(Box::new(SubqueryOperator::new(
                    child,
                    sq.clone(),
                    stats.clone(),
                )));
                i += 1;
            }

            Pattern::IndexSearch(isp) => {
                // BM25 full-text search against a graph source
                // If no child operator, use EmptyOperator as seed (allows IndexSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(Bm25SearchOperator::new(child, isp.clone())));
                i += 1;
            }

            Pattern::VectorSearch(vsp) => {
                // Vector similarity search against a vector graph source
                // If no child operator, use EmptyOperator as seed (allows VectorSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(crate::vector::VectorSearchOperator::new(
                    child,
                    vsp.clone(),
                )));
                i += 1;
            }

            Pattern::R2rml(r2rml_pattern) => {
                // R2RML scan against an Iceberg graph source
                let child = require_child(operator, "R2RML pattern")?;
                operator = Some(Box::new(crate::r2rml::R2rmlScanOperator::new(
                    child,
                    r2rml_pattern.clone(),
                )));
                i += 1;
            }

            Pattern::GeoSearch(gsp) => {
                // Geographic proximity search against binary index
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(crate::geo_search::GeoSearchOperator::new(
                    child,
                    gsp.clone(),
                )));
                i += 1;
            }

            Pattern::S2Search(s2p) => {
                // S2 spatial search against spatial index sidecar
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(crate::s2_search::S2SearchOperator::new(
                    child,
                    s2p.clone(),
                )));
                i += 1;
            }

            Pattern::Graph {
                name,
                patterns: inner_patterns,
            } => {
                // GRAPH pattern - scope inner patterns to a named graph
                let child = require_child(operator, "GRAPH pattern")?;
                operator = Some(Box::new(crate::graph::GraphOperator::new(
                    child,
                    name.clone(),
                    inner_patterns.clone(),
                )));
                i += 1;
            }

            Pattern::Service(service_pattern) => {
                // SERVICE pattern - execute patterns against another ledger
                let child = require_child(operator, "SERVICE pattern")?;
                operator = Some(Box::new(crate::service::ServiceOperator::new(
                    child,
                    service_pattern.clone(),
                )));
                i += 1;
            }
        }
    }

    operator.ok_or_else(|| QueryError::InvalidQuery("No patterns produced an operator".to_string()))
}

/// Create a first-pattern scan operator.
///
/// Creates a `ScanOperator` that selects between `BinaryScanOperator` (streaming
/// cursor) and `RangeScanOperator` (range fallback) at `open()` time based on
/// the `ExecutionContext`.
fn make_first_scan(
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    inline_filters: Vec<Expression>,
) -> BoxedOperator {
    let obj_bounds = tp.o.as_var().and_then(|v| object_bounds.get(&v).cloned());
    Box::new(crate::binary_scan::ScanOperator::new(
        tp.clone(),
        obj_bounds,
        inline_filters,
    ))
}

/// Build a single scan or join operator for a triple pattern
///
/// This is the extracted helper that eliminates the duplication between
/// `build_where_operators_seeded` (incremental path) and `build_triple_operators`.
///
/// - If `left` is None, creates a `ScanOperator` for the first pattern with inline filters
/// - If `left` is Some, creates a NestedLoopJoinOperator joining to the existing operator
/// - Applies object bounds from filters when available
///
/// The `inline_filters` are evaluated inline on the operator: baked into `ScanOperator`
/// for the first scan, or evaluated per combined row in `NestedLoopJoinOperator` for joins.
pub fn build_scan_or_join(
    left: Option<BoxedOperator>,
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    inline_filters: Vec<Expression>,
) -> BoxedOperator {
    match left {
        None => make_first_scan(tp, object_bounds, inline_filters),
        Some(left) => {
            // Subsequent patterns: use NestedLoopJoinOperator with optional bounds pushdown
            let left_schema = Arc::from(left.schema().to_vec().into_boxed_slice());

            // Extract object bounds if available for this pattern's object variable
            let bounds = tp.o.as_var().and_then(|v| object_bounds.get(&v).cloned());

            Box::new(NestedLoopJoinOperator::new(
                left,
                left_schema,
                tp.clone(),
                bounds,
                inline_filters,
            ))
        }
    }
}

/// Build operators for a sequence of triple patterns
///
/// Uses property join optimization when applicable.
/// When `object_bounds` is provided, range constraints are pushed down to ScanOperator
/// for the first pattern, enabling index-level filtering.
pub fn build_triple_operators(
    existing: Option<BoxedOperator>,
    triples: &[TriplePattern],
    object_bounds: &HashMap<VarId, ObjectBounds>,
) -> Result<BoxedOperator> {
    if triples.is_empty() {
        return existing
            .ok_or_else(|| QueryError::InvalidQuery("No triple patterns to process".to_string()));
    }

    let mut operator = existing;

    // Check for property join optimization
    //
    // PropertyJoinOperator scans each predicate independently and applies per-predicate
    // object bounds during its scan phase, then intersects subjects. This is far cheaper
    // than falling back to NestedLoopJoin which does correlated novelty traversals.
    if operator.is_none() && triples.len() >= 2 && is_property_join(triples) {
        // Use PropertyJoinOperator for multi-property patterns
        let pj = PropertyJoinOperator::new(triples, object_bounds.clone());
        return Ok(Box::new(pj));
    }

    // Build chain of scan/join operators using the shared helper
    for pattern in triples {
        operator = Some(build_scan_or_join(
            operator,
            pattern,
            object_bounds,
            Vec::new(),
        ));
    }

    Ok(operator.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::triple::Term;
    use fluree_db_core::{FlakeValue, PropertyStatData, Sid, StatsView};

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Term::Var(s_var),
            Term::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    #[test]
    fn test_build_where_operators_single_triple() {
        let patterns = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];

        let result = build_where_operators(&patterns, None);
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

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());
    }

    /// Regression: top-level VALUES must not block PropertyJoinOperator selection.
    ///
    /// Previously, we eagerly inserted an EmptyOperator seed whenever the first pattern
    /// was non-triple. With VALUES at position 0, that made `operator.is_none()` false
    /// and disabled PropertyJoinOperator, causing a catastrophic fallback to nested-loop
    /// joins on multi-property patterns (e.g. vector score + date filter).
    #[test]
    fn test_values_does_not_block_property_join_schema_order() {
        use crate::binding::Binding;

        // VALUES ?queryVec { 0 } .
        // ?article :date ?date .
        // ?article :vec ?vec .
        // FILTER(?date >= "2026-01-01")
        //
        // We assert schema order to distinguish the plan shape:
        // - Bad (VALUES first): schema starts with ?queryVec
        // - Good (PropertyJoin first, then VALUES wrap): schema starts with ?article
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(9)],
                rows: vec![vec![Binding::lit(FlakeValue::Long(0), Sid::new(2, "long"))]],
            },
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Filter(Expression::ge(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::String("2026-01-01".to_string())),
            )),
        ];

        let op = build_where_operators(&patterns, None).unwrap();
        let schema = op.schema();
        assert!(
            !schema.is_empty(),
            "schema should be non-empty for VALUES + triples"
        );
        assert_eq!(
            schema[0],
            VarId(0),
            "expected plan to start from subject var (?article), not VALUES var"
        );
        assert!(
            schema.contains(&VarId(9)),
            "expected VALUES var (?queryVec) to appear in schema"
        );
    }

    #[test]
    fn test_collects_and_reorders_triples_across_safe_filter_boundary_with_stats() {
        // Roughly matches the user's SPARQL shape:
        //   ?score :hasScore ?scoreV .
        //   FILTER(?scoreV > 0.4)
        //   ?score :refersInstance ?concept .
        //   ?concept :notation "LVL1" .
        //
        // The key expectation: we can treat (Triple + FILTER + Triple + Triple) as a single block
        // and reorder the triples (with stats) to start from the most selective predicate ("notation").
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

        // Stats: make "notation" look far more selective than the score predicates.
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "notation"),
            PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000, // selectivity ~ 1
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "hasScore"),
            PropertyStatData {
                count: 1_000_000_000, // very unselective property scan
                ndv_values: 900_000_000,
                ndv_subjects: 900_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "refersInstance"),
            PropertyStatData {
                count: 800_000_000, // unselective property scan
                ndv_values: 700_000_000,
                ndv_subjects: 700_000_000,
            },
        );

        let as_patterns: Vec<Pattern> = block.triples.into_iter().map(Pattern::Triple).collect();
        let ordered = reorder_patterns(&as_patterns, Some(&stats), &HashSet::new());
        let first_triple = ordered[0]
            .as_triple()
            .expect("first reordered pattern should be a triple");
        let first_pred = first_triple.p.as_sid().expect("predicate should be Sid");
        assert_eq!(
            &*first_pred.name, "notation",
            "expected optimizer to start from the most selective triple"
        );
    }

    #[test]
    fn test_collect_block_includes_values_and_marks_filter_safe() {
        use crate::binding::Binding;

        // VALUES ?x { 1 } . FILTER(?x = 1) . ?s :p ?x
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
            },
            Pattern::Filter(Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(1)),
            )),
            Pattern::Triple(TriplePattern::new(
                Term::Var(VarId(1)),
                Term::Sid(Sid::new(100, "p")),
                Term::Var(VarId(0)),
            )),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, patterns.len());
        assert_eq!(block.values.len(), 1, "VALUES should be included in block");
        assert_eq!(block.binds.len(), 0, "expected 0 BINDs in the block");
        assert_eq!(block.triples.len(), 1);
        assert_eq!(
            block.filters.len(),
            1,
            "FILTER referencing VALUES var should be safe"
        );
    }

    #[test]
    fn test_collect_block_includes_safe_bind() {
        // ?s :age ?age . BIND(?age + 1 AS ?age2) . FILTER(?age2 > 0)
        //
        // BIND is "safe" here because ?age is bound by the preceding triple in original order.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Bind {
                var: VarId(2),
                expr: Expression::add(
                    Expression::Var(VarId(1)),
                    Expression::Const(FilterValue::Long(1)),
                ),
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FilterValue::Long(0)),
            )),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, patterns.len());
        assert_eq!(block.values.len(), 0);
        assert_eq!(block.triples.len(), 1);
        assert_eq!(
            block.binds.len(),
            1,
            "expected BIND to be included in inner-join block"
        );
        assert_eq!(block.filters.len(), 1);
    }

    #[test]
    fn test_build_where_operators_filter_before_triple_allowed() {
        // FILTER at position 0 is now allowed with empty seed support
        let patterns = vec![
            Pattern::Filter(Expression::eq(
                Expression::Const(FilterValue::Long(1)),
                Expression::Const(FilterValue::Long(1)),
            )),
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
        ];

        let result = build_where_operators(&patterns, None);
        // Now succeeds - empty seed provides initial solution
        assert!(result.is_ok());
    }

    #[test]
    fn test_values_pattern_builds() {
        use crate::binding::Binding;

        // VALUES at position 0 should work
        // xsd:long is namespace code 2
        let patterns = vec![Pattern::Values {
            vars: vec![VarId(0)],
            rows: vec![vec![Binding::lit(
                FlakeValue::Long(42),
                Sid::new(2, "long"),
            )]],
        }];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should include the VALUES var
        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_bind_pattern_builds() {
        // BIND at position 0 should work
        let patterns = vec![Pattern::Bind {
            var: VarId(0),
            expr: Expression::Const(FilterValue::Long(42)),
        }];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should include the BIND var
        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_union_pattern_builds() {
        // UNION at position 0 should work
        let patterns = vec![Pattern::Union(vec![
            vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            vec![Pattern::Triple(make_pattern(VarId(0), "email", VarId(2)))],
        ])];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Unified schema should have all vars: ?0, ?1, ?2
        assert_eq!(op.schema().len(), 3);
    }

    #[test]
    fn test_values_then_triple_pattern() {
        use crate::binding::Binding;

        // VALUES followed by triple pattern
        // xsd:long is namespace code 2
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![
                    vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                    vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                ],
            },
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
        ];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should have both vars
        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_empty_operator_schema() {
        use crate::seed::EmptyOperator;

        let op = EmptyOperator::new();
        // EmptyOperator has empty schema
        assert_eq!(op.schema().len(), 0);
        assert_eq!(op.estimated_rows(), Some(1));
    }

    /// Regression: PropertyJoinOperator must be used even when object bounds are
    /// non-empty (bounds are applied per-predicate inside the property-join scan).
    /// Previously, `build_triple_operators` fell back to NestedLoopJoin whenever
    /// `object_bounds` was non-empty, causing 1000x+ slowdowns on vector queries
    /// with FILTER clauses.
    #[test]
    fn test_property_join_used_with_object_bounds() {
        use fluree_db_core::ObjectBounds;

        let tp1 = make_pattern(VarId(0), "date", VarId(1));
        let tp2 = make_pattern(VarId(0), "vec", VarId(2));
        let triples = vec![tp1, tp2];

        // Non-empty bounds on one of the object variables
        let mut bounds = HashMap::new();
        bounds.insert(
            VarId(1),
            ObjectBounds {
                lower: Some((FlakeValue::String("2026-01-01".to_string()), true)),
                upper: None,
            },
        );

        let op = build_triple_operators(None, &triples, &bounds).unwrap();

        // PropertyJoinOperator schema is [subject, obj1, obj2] in declaration order.
        // If NestedLoopJoin were used instead, all three vars would still appear but
        // the operator would be a chain rather than a single PropertyJoinOperator.
        assert_eq!(
            op.schema(),
            &[VarId(0), VarId(1), VarId(2)],
            "PropertyJoinOperator should be used (schema = [subject, obj1, obj2])"
        );
    }

    /// Verify that `build_where_operators` uses PropertyJoinOperator for a multi-property
    /// pattern with a FILTER that produces object bounds.
    #[test]
    fn test_where_operators_property_join_with_filter_bounds() {
        // Pattern: ?s :date ?date . ?s :vec ?vec . FILTER(?date >= "2026-01-01")
        // This should use PropertyJoinOperator (with bounds on ?date pushed into its
        // per-predicate scan) rather than falling back to NestedLoopJoin.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Filter(Expression::ge(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::String("2026-01-01".to_string())),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "should build successfully");

        let op = result.unwrap();
        // All three variables should be in the schema
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "date var present");
        assert!(schema.contains(&VarId(2)), "vec var present");
    }

    #[test]
    fn test_build_scan_or_join_first_pattern() {
        let tp = make_pattern(VarId(0), "name", VarId(1));
        let bounds = HashMap::new();

        let op: BoxedOperator = build_scan_or_join(None, &tp, &bounds, Vec::new());

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_scan_or_join_with_left() {
        let tp1 = make_pattern(VarId(0), "name", VarId(1));
        let tp2 = make_pattern(VarId(0), "age", VarId(2));
        let bounds = HashMap::new();

        let first: BoxedOperator = build_scan_or_join(None, &tp1, &bounds, Vec::new());
        let second = build_scan_or_join(Some(first), &tp2, &bounds, Vec::new());

        // Schema should include all vars from both patterns
        assert_eq!(second.schema().len(), 3);
        assert!(second.schema().contains(&VarId(0)));
        assert!(second.schema().contains(&VarId(1)));
        assert!(second.schema().contains(&VarId(2)));
    }

    // ========================================================================
    // Filter optimization tests - Phase 1: dependency-based filter injection
    // ========================================================================

    #[test]
    fn test_filter_before_triple_collected_in_block() {
        // FILTER(?x > 0) . ?s :p ?x
        // Filter references ?x which is bound by the subsequent triple.
        // The filter should be collected and applied after ?x is bound.
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(0)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "value", VarId(0))),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, 2, "should consume both filter and triple");
        assert_eq!(block.filters.len(), 1, "should include the filter");
        assert_eq!(block.triples.len(), 1, "should include the triple");
    }

    #[test]
    fn test_filter_referencing_later_pattern_vars() {
        // ?s :age ?age . FILTER(?age > 18 AND ?name != "") . ?s :name ?name
        // Filter references both ?age (already bound) and ?name (bound later).
        // All patterns should be collected in the same block.
        let age = VarId(0);
        let name = VarId(1);
        let s = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "age", age)),
            Pattern::Filter(Expression::and(vec![
                Expression::gt(
                    Expression::Var(age),
                    Expression::Const(FilterValue::Long(18)),
                ),
                Expression::ne(
                    Expression::Var(name),
                    Expression::Const(FilterValue::String("".to_string())),
                ),
            ])),
            Pattern::Triple(make_pattern(s, "name", name)),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "should consume all patterns"
        );
        assert_eq!(block.triples.len(), 2, "should include both triples");
        assert_eq!(block.filters.len(), 1, "should include the filter");
    }

    #[test]
    fn test_filter_only_patterns_are_collected() {
        // Multiple filters before any triple - all should be collected
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(0)),
            )),
            Pattern::Filter(Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(100)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "value", VarId(0))),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, 3, "should consume all patterns");
        assert_eq!(block.filters.len(), 2, "should include both filters");
        assert_eq!(block.triples.len(), 1, "should include the triple");
    }

    #[test]
    fn test_build_operators_filter_before_triple_is_valid() {
        // FILTER(?x > 18) before the triple that binds ?x should now succeed
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "age", VarId(0))),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Filter before its bound variable should be allowed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_filter_with_multiple_unbound_vars_collected() {
        // FILTER(?x > ?y) where both vars are bound by later triples
        let x = VarId(0);
        let y = VarId(1);
        let s = VarId(2);

        let patterns = vec![
            Pattern::Filter(Expression::gt(Expression::Var(x), Expression::Var(y))),
            Pattern::Triple(make_pattern(s, "valueX", x)),
            Pattern::Triple(make_pattern(s, "valueY", y)),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "should consume all patterns"
        );
        assert_eq!(block.filters.len(), 1);
        assert_eq!(block.triples.len(), 2);

        // Verify the operator tree builds successfully
        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Should build successfully: {:?}",
            result.err()
        );
    }

    // =========================================================================
    // Generalized reordering integration tests
    // =========================================================================

    #[test]
    fn test_build_operators_with_union_builds_successfully() {
        // Triple followed by UNION should build successfully
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(VarId(0), "type", VarId(2)))],
                vec![Pattern::Triple(make_pattern(VarId(0), "class", VarId(3)))],
            ]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        assert!(op.schema().contains(&VarId(0)));
    }

    #[test]
    fn test_build_operators_optional_after_triples() {
        // OPTIONAL should work after triple patterns
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(2))),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(3),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "name var present");
        assert!(schema.contains(&VarId(2)), "age var present");
        assert!(
            schema.contains(&VarId(3)),
            "email var from OPTIONAL present"
        );
    }

    #[test]
    fn test_fast_path_triple_only_unchanged() {
        // Triple-only patterns should work identically to before
        // (reorder_patterns handles all pattern types uniformly)
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(2))),
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FilterValue::Long(18)),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        assert!(op.schema().contains(&VarId(0)));
        assert!(op.schema().contains(&VarId(1)));
        assert!(op.schema().contains(&VarId(2)));
    }

    #[test]
    fn test_property_join_still_works_with_compound_patterns_elsewhere() {
        // When compound patterns exist but triples still qualify for property join,
        // the fast path should detect property join within the triple block.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(3),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)));
        assert!(schema.contains(&VarId(1)));
        assert!(schema.contains(&VarId(2)));
    }

    #[test]
    fn test_build_operators_minus_after_triple() {
        // MINUS after a triple should work
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Minus(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "deleted",
                VarId(2),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());
    }

    #[test]
    fn test_build_operators_exists_after_triple() {
        // EXISTS after a triple should work
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Exists(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "verified",
                VarId(2),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());
    }

    #[test]
    fn test_build_operators_complex_mix() {
        // Complex mix: Triple, UNION, Triple, MINUS, OPTIONAL
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(VarId(0), "type", VarId(2)))],
                vec![Pattern::Triple(make_pattern(VarId(0), "class", VarId(3)))],
            ]),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(4))),
            Pattern::Minus(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "deleted",
                VarId(5),
            ))]),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(6),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Complex mix should build: {:?}",
            result.err()
        );

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)));
        assert!(schema.contains(&VarId(1)));
    }

    #[test]
    fn test_bind_with_post_filter_builds_successfully() {
        // ?s :age ?x . BIND(?x + 10 AS ?y) . FILTER(?y > 25)
        // The filter depends on ?y which is the BIND output.
        // It should be fused into the BindOperator.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Bind {
                var: VarId(2),
                expr: Expression::add(
                    Expression::Var(VarId(1)),
                    Expression::Const(FilterValue::Long(10)),
                ),
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FilterValue::Long(25)),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "BIND + post-BIND FILTER should build: {:?}",
            result.err()
        );

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "?x var present");
        assert!(schema.contains(&VarId(2)), "?y (BIND output) var present");
    }
}
