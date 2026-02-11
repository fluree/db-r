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
use crate::pattern::TriplePattern;
use crate::planner::{is_property_join, reorder_patterns_seeded};
use crate::property_join::PropertyJoinOperator;
use crate::property_path::{PropertyPathOperator, DEFAULT_MAX_VISITED};
use crate::seed::EmptyOperator;
use crate::subquery::SubqueryOperator;
use crate::union::UnionOperator;
use crate::values::ValuesOperator;
use crate::var_registry::VarId;
use fluree_db_core::{ObjectBounds, StatsView, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::pushdown::extract_bounds_from_filters;

// ============================================================================
// Inner join block result type
// ============================================================================

/// Result of collecting an inner-join block from a pattern list.
///
/// Contains all the components needed to build a joined block of patterns.
pub struct InnerJoinBlock {
    /// Index past the last consumed pattern
    pub end_index: usize,
    /// VALUES patterns (vars and rows)
    pub values: Vec<(Vec<VarId>, Vec<Vec<crate::binding::Binding>>)>,
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
fn require_child<S: Storage + 'static>(
    operator: Option<BoxedOperator<S>>,
    pattern_name: &str,
) -> Result<BoxedOperator<S>> {
    operator
        .ok_or_else(|| QueryError::InvalidQuery(format!("{} has no input operator", pattern_name)))
}

/// Get an operator or create an empty seed if None.
///
/// Used for patterns that can appear at position 0 and need an initial solution.
#[inline]
fn get_or_empty_seed<S: Storage + 'static>(operator: Option<BoxedOperator<S>>) -> BoxedOperator<S> {
    operator.unwrap_or_else(|| Box::new(EmptyOperator::new()))
}

/// Get bound variables from an operator's schema.
#[inline]
fn bound_vars_from_operator<S: Storage + 'static>(
    operator: &Option<BoxedOperator<S>>,
) -> HashSet<VarId> {
    operator
        .as_ref()
        .map(|op| op.schema().iter().copied().collect())
        .unwrap_or_default()
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

/// Apply pending BINDs and FILTERs that are ready (all required vars are bound).
///
/// Returns the updated operator and the remaining pending items.
fn apply_ready_binds_and_filters<S: Storage + 'static>(
    mut child: BoxedOperator<S>,
    bound: &mut HashSet<VarId>,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    filter_idxs_consumed: &[usize],
) -> (BoxedOperator<S>, Vec<PendingBind>, Vec<PendingFilter>) {
    let mut remaining_binds = Vec::new();
    let mut remaining_filters = Vec::new();

    // Apply ready BINDs
    for pending in pending_binds {
        if pending.required_vars.is_subset(bound) {
            child = Box::new(BindOperator::new(child, pending.target_var, pending.expr));
            bound.insert(pending.target_var);
        } else {
            remaining_binds.push(pending);
        }
    }

    // Apply ready FILTERs (skip those consumed by pushdown)
    for pending in pending_filters {
        if filter_idxs_consumed.contains(&pending.original_idx) {
            continue;
        }
        if pending.required_vars.is_subset(bound) {
            child = Box::new(FilterOperator::new(child, pending.expr));
        } else {
            remaining_filters.push(pending);
        }
    }

    (child, remaining_binds, remaining_filters)
}

/// Apply all remaining BINDs and FILTERs (assumes all vars are now bound).
fn apply_all_remaining<S: Storage + 'static>(
    mut child: BoxedOperator<S>,
    pending_binds: Vec<PendingBind>,
    pending_filters: Vec<PendingFilter>,
    filter_idxs_consumed: &[usize],
) -> BoxedOperator<S> {
    for pending in pending_binds {
        child = Box::new(BindOperator::new(child, pending.target_var, pending.expr));
    }
    for pending in pending_filters {
        if !filter_idxs_consumed.contains(&pending.original_idx) {
            child = Box::new(FilterOperator::new(child, pending.expr));
        }
    }
    child
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
pub fn build_where_operators<S: Storage + 'static>(
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator<S>> {
    build_where_operators_seeded::<S>(None, patterns, stats)
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
    let mut values: Vec<(Vec<VarId>, Vec<Vec<crate::binding::Binding>>)> = Vec::new();
    let mut triples: Vec<TriplePattern> = Vec::new();
    let mut binds: Vec<(VarId, Expression)> = Vec::new();
    let mut filters: Vec<Expression> = Vec::new();
    let mut bound_vars: HashSet<VarId> = HashSet::new();

    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Values { vars, rows } => {
                // VALUES binds its vars immediately (join seed/constraint).
                bound_vars.extend(vars.iter().copied());
                values.push((vars.clone(), rows.clone()));
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
pub fn build_where_operators_seeded<S: Storage + 'static>(
    seed: Option<BoxedOperator<S>>,
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
) -> Result<BoxedOperator<S>> {
    if patterns.is_empty() {
        // Empty patterns = one row with empty schema
        return Ok(seed.unwrap_or_else(|| Box::new(EmptyOperator::new())));
    }

    // If no explicit seed, determine if we need an empty seed (first pattern is not a triple)
    let needs_empty_seed = seed.is_none() && !matches!(patterns.first(), Some(Pattern::Triple(_)));

    // Start with provided seed, else start with empty operator if needed
    let mut operator: Option<BoxedOperator<S>> = if let Some(seed) = seed {
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
                // Collect an inner-join block of Triples, VALUES, safe BINDs, and safe FILTERs.
                // This allows:
                // - hoisting VALUES earlier within the block (inner-join safe)
                // - hoisting safe BIND/FILTER earlier (only when their inputs are already bound)
                // - reordering triples for join efficiency
                // - delaying filters until their vars are bound (never earlier)
                let start = i;
                let block = collect_inner_join_block(patterns, start);
                let end = block.end_index;
                let block_values = block.values;
                let triples = block.triples;
                let block_binds = block.binds;
                let block_filters = block.filters;
                // IMPORTANT: `collect_inner_join_block` may consume *zero* patterns when the
                // current pattern is a BIND/FILTER that is not safe to hoist into a block
                // (e.g., it references vars that are not yet bound in left-to-right order).
                //
                // In that case, we must fall back to processing the single pattern in-order,
                // otherwise `i` will not advance and we'll spin forever (100% CPU).
                if end == start {
                    match &patterns[start] {
                        Pattern::Bind { var, expr } => {
                            let child = get_or_empty_seed(operator.take());
                            operator = Some(Box::new(BindOperator::new(child, *var, expr.clone())));
                            i = start + 1;
                            continue;
                        }
                        Pattern::Values { vars, rows } => {
                            let child = get_or_empty_seed(operator.take());
                            operator = Some(Box::new(ValuesOperator::new(
                                child,
                                vars.clone(),
                                rows.clone(),
                            )));
                            i = start + 1;
                            continue;
                        }
                        Pattern::Triple(tp) => {
                            operator = Some(build_scan_or_join(operator, tp, &HashMap::new()));
                            i = start + 1;
                            continue;
                        }
                        _ => {
                            // Should be unreachable due to match arm, but avoid accidental spins.
                            i = start + 1;
                            continue;
                        }
                    }
                }

                i = end;

                // Apply any VALUES patterns first (they are inner-join constraints and beneficial
                // to apply early to reduce intermediate results).
                //
                // If there is no upstream operator yet, start from an empty seed solution so
                // VALUES can act as the initial binding source.
                if !block_values.is_empty() {
                    for (vars, rows) in block_values {
                        let child = get_or_empty_seed(operator.take());
                        operator = Some(Box::new(ValuesOperator::new(child, vars, rows)));
                    }
                }

                // Start bound vars from upstream + VALUES.
                let mut bound = bound_vars_from_operator(&operator);

                // Hot path: block is *only* triples (no VALUES/BIND/FILTER).
                // Avoid any dependency bookkeeping and just do seeded reorder + build.
                if block_binds.is_empty() && block_filters.is_empty() {
                    let reordered = reorder_patterns_seeded(triples, stats.as_deref(), &bound);
                    operator = Some(build_triple_operators(
                        operator,
                        &reordered,
                        &HashMap::new(),
                    )?);
                    continue;
                }

                // Pending actions (safe BINDs + safe FILTERs), applied when their vars are bound.
                // We keep them in original order and only apply when ready.
                let mut pending_binds: Vec<PendingBind> = block_binds
                    .into_iter()
                    .map(|(var, expr)| {
                        let required_vars: HashSet<VarId> = expr.variables().into_iter().collect();
                        PendingBind {
                            required_vars,
                            target_var: var,
                            expr,
                        }
                    })
                    .collect();
                // Track original indices so "pushdown-consumed" filters can be skipped even
                // after we delay/reorder/filter the pending list.
                let mut pending_filters: Vec<PendingFilter> = block_filters
                    .into_iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        let required_vars: HashSet<VarId> = expr.variables().into_iter().collect();
                        PendingFilter {
                            original_idx: idx,
                            required_vars,
                            expr,
                        }
                    })
                    .collect();

                // Reorder triples using stats-based selectivity when available.
                //
                // IMPORTANT: Seed the optimizer with the variables already bound by the current
                // upstream operator. This avoids starting the block with a pattern that doesn't
                // join with existing bindings (cartesian explosion), matching the Clojure optimizer.
                let reordered = reorder_patterns_seeded(triples, stats.as_deref(), &bound);

                // Push down range-safe filters into object bounds (when possible)
                let filters_for_pushdown: Vec<Expression> =
                    pending_filters.iter().map(|f| f.expr.clone()).collect();
                let (object_bounds, filter_idxs_consumed) =
                    extract_bounds_from_filters(&reordered, &filters_for_pushdown);

                // If this is a property-join candidate from the start of a block, keep the existing
                // property-join fast path. We'll apply any remaining filters after the join.
                // PropertyJoinOperator scans each predicate independently across all subjects.
                // When we have object bounds (from FILTER pushdown), it is often far better to:
                // 1) run the bounded scan first to produce a selective subject set, then
                // 2) join subsequent predicates correlated to that subject set.
                //
                // So: only use the property-join fast path when there are no object bounds.
                let can_property_join = operator.is_none()
                    && reordered.len() >= 2
                    && is_property_join(&reordered)
                    && object_bounds.is_empty();
                if can_property_join {
                    operator = Some(build_triple_operators(
                        operator,
                        &reordered,
                        &object_bounds,
                    )?);
                    // Apply any BINDs/FILTERs that are ready after property join.
                    bound = bound_vars_from_operator(&operator);
                    if let Some(child) = operator.take() {
                        let (child, _, _) = apply_ready_binds_and_filters(
                            child,
                            &mut bound,
                            pending_binds,
                            pending_filters,
                            &filter_idxs_consumed,
                        );
                        // We don't attempt to delay remaining binds/filters in the property-join fast-path;
                        // any not-yet-ready will be applied later as separate patterns (i.e. not hoisted).
                        operator = Some(child);
                    }
                } else {
                    for tp in &reordered {
                        // Build scan/join for this triple with bounds (if any)
                        operator = Some(build_scan_or_join(operator, tp, &object_bounds));

                        // Update bound vars after this triple
                        for v in tp.variables() {
                            bound.insert(v);
                        }

                        // Apply any pending binds/filters that are now safe (all vars bound).
                        if let Some(child) = operator.take() {
                            let (child, new_binds, new_filters) = apply_ready_binds_and_filters(
                                child,
                                &mut bound,
                                pending_binds,
                                pending_filters,
                                &filter_idxs_consumed,
                            );
                            pending_binds = new_binds;
                            pending_filters = new_filters;
                            operator = Some(child);
                        }
                    }

                    // Any remaining binds/filters should now be bound; apply them.
                    if !pending_binds.is_empty() || !pending_filters.is_empty() {
                        let child = require_child(operator, "Filters")?;
                        operator = Some(apply_all_remaining(
                            child,
                            pending_binds,
                            pending_filters,
                            &filter_idxs_consumed,
                        ));
                    }
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
                operator = Some(Box::new(UnionOperator::new(child, branches.clone())));
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
                operator = Some(Box::new(SubqueryOperator::new(child, sq.clone())));
                i += 1;
            }

            Pattern::IndexSearch(isp) => {
                // BM25 full-text search against a virtual graph
                // If no child operator, use EmptyOperator as seed (allows IndexSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(Bm25SearchOperator::new(child, isp.clone())));
                i += 1;
            }

            Pattern::VectorSearch(vsp) => {
                // Vector similarity search against a vector virtual graph
                // If no child operator, use EmptyOperator as seed (allows VectorSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                operator = Some(Box::new(crate::vector::VectorSearchOperator::new(
                    child,
                    vsp.clone(),
                )));
                i += 1;
            }

            Pattern::R2rml(r2rml_pattern) => {
                // R2RML scan against an Iceberg virtual graph
                let child = require_child(operator, "R2RML pattern")?;
                operator = Some(Box::new(crate::r2rml::R2rmlScanOperator::new(
                    child,
                    r2rml_pattern.clone(),
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
fn make_first_scan<S: Storage + 'static>(
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
) -> BoxedOperator<S> {
    let obj_bounds = tp.o.as_var().and_then(|v| object_bounds.get(&v).cloned());
    Box::new(crate::binary_scan::ScanOperator::<S>::new(
        tp.clone(),
        obj_bounds,
        Vec::new(),
    ))
}

/// Build a single scan or join operator for a triple pattern
///
/// This is the extracted helper that eliminates the duplication between
/// `build_where_operators_seeded` (incremental path) and `build_triple_operators`.
///
/// - If `left` is None, creates a `ScanOperator` for the first pattern
/// - If `left` is Some, creates a NestedLoopJoinOperator joining to the existing operator
/// - Applies object bounds from filters when available
pub fn build_scan_or_join<S: Storage + 'static>(
    left: Option<BoxedOperator<S>>,
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
) -> BoxedOperator<S> {
    match left {
        None => make_first_scan(tp, object_bounds),
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
            ))
        }
    }
}

/// Build operators for a sequence of triple patterns
///
/// Uses property join optimization when applicable.
/// When `object_bounds` is provided, range constraints are pushed down to ScanOperator
/// for the first pattern, enabling index-level filtering.
pub fn build_triple_operators<S: Storage + 'static>(
    existing: Option<BoxedOperator<S>>,
    triples: &[TriplePattern],
    object_bounds: &HashMap<VarId, ObjectBounds>,
) -> Result<BoxedOperator<S>> {
    if triples.is_empty() {
        return existing
            .ok_or_else(|| QueryError::InvalidQuery("No triple patterns to process".to_string()));
    }

    let mut operator = existing;

    // Check for property join optimization
    //
    // IMPORTANT: only apply when there are no object bounds.
    // If bounds exist (from FILTER pushdown), a correlated plan (bounded scan -> join)
    // is usually much cheaper than scanning the other predicate(s) globally.
    if operator.is_none()
        && triples.len() >= 2
        && is_property_join(triples)
        && object_bounds.is_empty()
    {
        // Use PropertyJoinOperator for multi-property patterns
        let pj = PropertyJoinOperator::new(triples, object_bounds.clone());
        return Ok(Box::new(pj));
    }

    // Build chain of scan/join operators using the shared helper
    for pattern in triples {
        operator = Some(build_scan_or_join(operator, pattern, object_bounds));
    }

    Ok(operator.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::pattern::Term;
    use fluree_db_core::{FlakeValue, MemoryStorage, PropertyStatData, Sid, StatsView};

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

        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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

        let result = build_where_operators::<MemoryStorage>(&patterns, None);
        assert!(result.is_ok());
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

        let ordered = reorder_patterns_seeded(block.triples, Some(&stats), &HashSet::new());
        let first_pred = ordered[0].p.as_sid().expect("predicate should be Sid");
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

        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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
        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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
        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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
        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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
        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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

    #[test]
    fn test_build_scan_or_join_first_pattern() {
        let tp = make_pattern(VarId(0), "name", VarId(1));
        let bounds = HashMap::new();

        let op: BoxedOperator<MemoryStorage> = build_scan_or_join(None, &tp, &bounds);

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_scan_or_join_with_left() {
        let tp1 = make_pattern(VarId(0), "name", VarId(1));
        let tp2 = make_pattern(VarId(0), "age", VarId(2));
        let bounds = HashMap::new();

        let first: BoxedOperator<MemoryStorage> = build_scan_or_join(None, &tp1, &bounds);
        let second = build_scan_or_join(Some(first), &tp2, &bounds);

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

        let result = build_where_operators::<MemoryStorage>(&patterns, None);
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
        let result = build_where_operators::<MemoryStorage>(&patterns, None);
        assert!(
            result.is_ok(),
            "Should build successfully: {:?}",
            result.err()
        );
    }
}
