//! Query planner - transforms logical IR to physical operators
//!
//! The planner takes a logical `Query` IR and produces a physical operator tree.
//!
//! # Design
//!
//! - Tracks three binding states: Unbound, Bound, Poisoned
//! - Reorders patterns for optimal join order using statistics-based selectivity
//! - Returns `PlanResult` with operator, schema, and binding states
//!
//! # Statistics-Based Optimization
//!
//! When a `StatsView` is provided, the planner uses HLL-derived cardinality
//! estimates to compute selectivity scores for each pattern. Lower scores
//! indicate more selective patterns that should be executed first.

use crate::ir::{CompareOp, Function, Pattern};
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, PropertyStatData, StatsView};
use std::collections::{HashMap, HashSet};

// =============================================================================
// Statistics-Based Selectivity Estimation
// =============================================================================

/// Pattern type classification for selectivity scoring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    /// All three components bound (s p o) - most selective
    ExactMatch,
    /// Subject bound, predicate bound, object variable (s p ?o)
    BoundSubject,
    /// Subject variable, predicate bound, object bound (?s p o)
    BoundObject,
    /// rdf:type pattern with class bound (?s rdf:type Class)
    ClassPattern,
    /// Only predicate bound (?s p ?o)
    PropertyScan,
    /// All variables (?s ?p ?o) - full scan, least selective
    FullScan,
}

// Row count estimate constants used by estimate_triple_row_count
/// Highest selectivity - exact match or minimum row estimate
const HIGHLY_SELECTIVE: f64 = 1.0;
/// Medium selectivity - fallback for bound-subject patterns
const MODERATELY_SELECTIVE: f64 = 10.0;
/// Fallback for bound-object and property-scan patterns
const DEFAULT_SELECTIVITY: f64 = 1000.0;
/// Full scan - all variables unbound
const FULL_SCAN: f64 = 1e12;

// Clojure fallback caps differ by pattern type:
// - bound-subject (s p ?o): min count 10
// - bound-object (?s p o): min count 1000
const BOUND_SUBJECT_FALLBACK_CAP: f64 = 10.0;
const BOUND_OBJECT_FALLBACK_CAP: f64 = 1000.0;

/// Classify a triple pattern for selectivity scoring, considering which
/// variables are already bound from previous patterns in the execution pipeline.
///
/// Treats variables present in `bound_vars` as effectively bound, producing
/// a more accurate pattern type for cardinality estimation during join ordering.
pub(crate) fn classify_pattern(
    pattern: &TriplePattern,
    bound_vars: &HashSet<VarId>,
) -> PatternType {
    let s_bound = pattern.s_bound() || pattern.s.as_var().is_some_and(|v| bound_vars.contains(&v));
    let p_bound = pattern.p_bound() || pattern.p.as_var().is_some_and(|v| bound_vars.contains(&v));
    let o_bound = pattern.o_bound() || pattern.o.as_var().is_some_and(|v| bound_vars.contains(&v));

    // rdf:type with a literal class object → ClassPattern (can look up specific class count).
    // If the object is a runtime-bound variable, we can't look up the class, so fall through
    // to BoundObject which uses the generic ndv_values-based estimate.
    if p_bound && o_bound && !s_bound && pattern.p.is_rdf_type() && pattern.o_bound() {
        return PatternType::ClassPattern;
    }

    match (s_bound, p_bound, o_bound) {
        (true, true, true) => PatternType::ExactMatch,
        (true, true, false) => PatternType::BoundSubject,
        (false, true, true) => PatternType::BoundObject,
        (false, true, false) => PatternType::PropertyScan,
        (false, false, false) => PatternType::FullScan,
        // Edge cases: subject bound but predicate not, etc.
        (true, false, _) => PatternType::BoundSubject, // Subject bound is selective
        (false, false, true) => PatternType::FullScan, // Only object bound, unusual
    }
}

/// Look up property statistics by predicate term (SID or IRI).
fn property_stats<'a>(stats: &'a StatsView, pred: &Term) -> Option<&'a PropertyStatData> {
    if let Some(sid) = pred.as_sid() {
        return stats.get_property(sid);
    }
    if let Some(iri) = pred.as_iri() {
        return stats.get_property_by_iri(iri);
    }
    None
}

/// Look up class instance count by class term (SID or IRI).
fn class_count(stats: &StatsView, class: &Term) -> Option<u64> {
    if let Some(sid) = class.as_sid() {
        return stats.get_class_count(sid);
    }
    if let Some(iri) = class.as_iri() {
        return stats.get_class_count_by_iri(iri);
    }
    None
}

/// Estimate the number of result rows a triple pattern adds to the pipeline.
///
/// This is context-aware: it considers which variables are already bound from
/// previous patterns. A triple `?s :name ?name` is a full PropertyScan (count rows)
/// when `?s` is unbound, but only ~ceil(count/ndv_subjects) rows per incoming row
/// when `?s` is already bound from an earlier pattern.
pub(crate) fn estimate_triple_row_count(
    pattern: &TriplePattern,
    bound_vars: &HashSet<VarId>,
    stats: Option<&StatsView>,
) -> f64 {
    match classify_pattern(pattern, bound_vars) {
        PatternType::ExactMatch => HIGHLY_SELECTIVE,

        PatternType::ClassPattern => {
            if let Some(s) = stats {
                if let Some(count) = class_count(s, &pattern.o) {
                    return count as f64;
                }
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        return (prop.count as f64 / prop.ndv_values as f64)
                            .ceil()
                            .max(HIGHLY_SELECTIVE);
                    }
                }
            }
            DEFAULT_SELECTIVITY
        }

        PatternType::BoundSubject => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_subjects > 0 {
                        return (prop.count as f64 / prop.ndv_subjects as f64)
                            .ceil()
                            .max(HIGHLY_SELECTIVE);
                    }
                    return (prop.count as f64).min(BOUND_SUBJECT_FALLBACK_CAP);
                }
            }
            MODERATELY_SELECTIVE
        }

        PatternType::BoundObject => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        return (prop.count as f64 / prop.ndv_values as f64)
                            .ceil()
                            .max(HIGHLY_SELECTIVE);
                    }
                    return (prop.count as f64).min(BOUND_OBJECT_FALLBACK_CAP);
                }
            }
            DEFAULT_SELECTIVITY
        }

        PatternType::PropertyScan => {
            if let Some(s) = stats {
                if let Some(prop) = property_stats(s, &pattern.p) {
                    return prop.count as f64;
                }
            }
            DEFAULT_SELECTIVITY
        }

        PatternType::FullScan => FULL_SCAN,
    }
}

/// Detect property-join pattern
///
/// Same ?s var, multiple distinct specified predicates, object vars.
/// Used by the planner to choose PropertyJoinOperator.
pub fn is_property_join(patterns: &[TriplePattern]) -> bool {
    use crate::pattern::Term;

    if patterns.len() < 2 {
        return false;
    }

    // All patterns share same subject var
    let first_s = match &patterns[0].s {
        Term::Var(v) => *v,
        _ => return false,
    };

    let same_subject = patterns.iter().all(|p| match &p.s {
        Term::Var(v) => *v == first_s,
        _ => false,
    });

    // All predicates are bound (not vars) - can be Term::Sid or Term::Iri
    let predicates_bound = patterns.iter().all(|p| p.p_bound());

    // All objects are vars - not literal/SID constants
    let objects_are_vars = patterns.iter().all(|p| matches!(&p.o, Term::Var(_)));

    // Predicates are distinct (avoid weird duplicate shapes)
    // Accept both Term::Sid and Term::Iri as distinct predicate keys
    let predicates: HashSet<String> = patterns
        .iter()
        .filter_map(|p| match &p.p {
            Term::Sid(sid) => Some(format!("sid:{}", sid)),
            Term::Iri(iri) => Some(format!("iri:{}", iri)),
            _ => None,
        })
        .collect();
    let predicates_distinct = predicates.len() == patterns.len();

    same_subject && predicates_bound && objects_are_vars && predicates_distinct
}

/// Represents a range constraint extracted from a filter expression
///
/// Used for filter pushdown to convert filters like `?age > 18 AND ?age < 65`
/// into index range bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeConstraint {
    /// The variable this constraint applies to
    pub var: VarId,
    /// Lower bound: (value, inclusive)
    pub lower: Option<(RangeValue, bool)>,
    /// Upper bound: (value, inclusive)
    pub upper: Option<(RangeValue, bool)>,
}

/// A value that can be used in range constraints
///
/// Simplified version of FlakeValue for filter pushdown.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeValue {
    Long(i64),
    Double(f64),
    String(String),
    /// Temporal value for range pushdown (NOT used for xsd:duration — it has no total order)
    Temporal(fluree_db_core::value::FlakeValue),
}

impl RangeConstraint {
    /// Create a new range constraint for a variable
    pub fn new(var: VarId) -> Self {
        Self {
            var,
            lower: None,
            upper: None,
        }
    }

    /// Set the lower bound
    pub fn with_lower(mut self, value: RangeValue, inclusive: bool) -> Self {
        self.lower = Some((value, inclusive));
        self
    }

    /// Set the upper bound
    pub fn with_upper(mut self, value: RangeValue, inclusive: bool) -> Self {
        self.upper = Some((value, inclusive));
        self
    }

    /// Merge another constraint into this one (tighten bounds)
    ///
    /// Takes the tighter of the two bounds. For lower bounds,
    /// the higher value is tighter. For upper bounds, the lower value is tighter.
    pub fn merge(&mut self, other: &RangeConstraint) {
        if self.var != other.var {
            return;
        }

        // Merge lower bounds: take the higher (tighter) one
        if let Some((other_val, other_incl)) = &other.lower {
            match &self.lower {
                None => self.lower = other.lower.clone(),
                Some((self_val, self_incl)) => {
                    if compare_range_values(other_val, self_val) == std::cmp::Ordering::Greater
                        || (compare_range_values(other_val, self_val) == std::cmp::Ordering::Equal
                            && !other_incl
                            && *self_incl)
                    {
                        self.lower = other.lower.clone();
                    }
                }
            }
        }

        // Merge upper bounds: take the lower (tighter) one
        if let Some((other_val, other_incl)) = &other.upper {
            match &self.upper {
                None => self.upper = other.upper.clone(),
                Some((self_val, self_incl)) => {
                    if compare_range_values(other_val, self_val) == std::cmp::Ordering::Less
                        || (compare_range_values(other_val, self_val) == std::cmp::Ordering::Equal
                            && !other_incl
                            && *self_incl)
                    {
                        self.upper = other.upper.clone();
                    }
                }
            }
        }
    }

    /// Check if this constraint is unsatisfiable (contradictory bounds)
    ///
    /// Returns true if:
    /// - lower > upper (impossible range)
    /// - lower == upper but either bound is exclusive (empty range)
    ///
    /// This enables early short-circuit when filters produce impossible ranges
    /// like `?x > 10 AND ?x < 5`.
    pub fn is_unsatisfiable(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Some((lower_val, lower_incl)), Some((upper_val, upper_incl))) => {
                match compare_range_values(lower_val, upper_val) {
                    std::cmp::Ordering::Greater => true, // lower > upper
                    std::cmp::Ordering::Equal => {
                        // lower == upper: only satisfiable if both inclusive
                        !(*lower_incl && *upper_incl)
                    }
                    std::cmp::Ordering::Less => false, // normal range
                }
            }
            _ => false, // Open-ended ranges are always satisfiable
        }
    }
}

/// Compare two range values
fn compare_range_values(a: &RangeValue, b: &RangeValue) -> std::cmp::Ordering {
    match (a, b) {
        (RangeValue::Long(a), RangeValue::Long(b)) => a.cmp(b),
        (RangeValue::Double(a), RangeValue::Double(b)) => {
            // Treat NaN as not comparable; avoid pretending NaN == anything.
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (RangeValue::String(a), RangeValue::String(b)) => a.cmp(b),
        // Cross-type: Long <-> Double
        (RangeValue::Long(a), RangeValue::Double(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (RangeValue::Double(a), RangeValue::Long(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        // Different types that can't be compared
        _ => std::cmp::Ordering::Equal,
    }
}

/// Extract range constraints from a pushdown-safe filter expression
///
/// Returns `None` if the filter is not range-safe (contains OR, NOT, functions, etc.).
/// Returns `Some(vec)` with extracted constraints for each variable.
///
/// # Supported patterns
///
/// - `?var op const` where op is `<`, `<=`, `>`, `>=`, `=`
/// - `const op ?var` (reversed comparison)
/// - `(op const ?var const)` — "sandwich" pattern producing two-sided bounds
/// - `AND` of the above (constraints are merged for tighter bounds)
pub fn extract_range_constraints(expr: &Expression) -> Option<Vec<RangeConstraint>> {
    if !expr.is_range_safe() {
        return None;
    }

    match expr {
        Expression::Call { func, args } => match func {
            // Comparison operators
            Function::Eq | Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                let op = func_to_compare_op(func);

                // 3-arg sandwich: (op const ?var const) → two-sided bounds
                if args.len() == 3 {
                    if let (Some(lo_val), Some(var), Some(hi_val)) = (
                        extract_const(&args[0]),
                        extract_var(&args[1]),
                        extract_const(&args[2]),
                    ) {
                        // Pair 1: const op ?var → ?var reversed_op const
                        let left_constraint =
                            create_constraint(var, reverse_compare_op(op), lo_val, false);
                        // Pair 2: ?var op const
                        let right_constraint = create_constraint(var, op, hi_val, false);
                        // Merge both into one constraint
                        let mut merged = left_constraint;
                        merged.merge(&right_constraint);
                        return Some(vec![merged]);
                    }
                    return None;
                }

                // 2-arg: ?var op const or const op ?var
                if args.len() != 2 {
                    return None;
                }
                let (left, right) = (&args[0], &args[1]);

                // Try ?var op const
                if let (Some(var), Some(val)) = (extract_var(left), extract_const(right)) {
                    return Some(vec![create_constraint(var, op, val, false)]);
                }
                // Try const op ?var (reverse the comparison)
                if let (Some(val), Some(var)) = (extract_const(left), extract_var(right)) {
                    let reversed_op = reverse_compare_op(op);
                    return Some(vec![create_constraint(var, reversed_op, val, false)]);
                }
                None
            }

            Function::And => {
                let mut all_constraints: HashMap<VarId, RangeConstraint> = HashMap::new();

                for e in args {
                    if let Some(constraints) = extract_range_constraints(e) {
                        for constraint in constraints {
                            all_constraints
                                .entry(constraint.var)
                                .and_modify(|existing| existing.merge(&constraint))
                                .or_insert(constraint);
                        }
                    }
                }

                if all_constraints.is_empty() {
                    None
                } else {
                    Some(all_constraints.into_values().collect())
                }
            }

            _ => None,
        },

        _ => None,
    }
}

/// Convert a Function comparison operator to CompareOp
fn func_to_compare_op(func: &Function) -> CompareOp {
    match func {
        Function::Eq => CompareOp::Eq,
        Function::Ne => CompareOp::Ne,
        Function::Lt => CompareOp::Lt,
        Function::Le => CompareOp::Le,
        Function::Gt => CompareOp::Gt,
        Function::Ge => CompareOp::Ge,
        _ => panic!("func_to_compare_op called with non-comparison function"),
    }
}

/// Extract a VarId from an expression if it's a simple variable reference
fn extract_var(expr: &Expression) -> Option<VarId> {
    match expr {
        Expression::Var(v) => Some(*v),
        _ => None,
    }
}

/// Extract a RangeValue from an expression if it's a constant
fn extract_const(expr: &Expression) -> Option<RangeValue> {
    use crate::ir::FilterValue;

    match expr {
        Expression::Const(FilterValue::Long(n)) => Some(RangeValue::Long(*n)),
        // NaN is not a meaningful range bound.
        Expression::Const(FilterValue::Double(d)) if d.is_nan() => None,
        Expression::Const(FilterValue::Double(d)) => Some(RangeValue::Double(*d)),
        Expression::Const(FilterValue::String(s)) => Some(RangeValue::String(s.clone())),
        Expression::Const(FilterValue::Temporal(fv)) => {
            // Duration (non-totally-orderable) should NOT be pushed down as a range constraint
            if matches!(fv, FlakeValue::Duration(_)) {
                None
            } else {
                Some(RangeValue::Temporal(fv.clone()))
            }
        }
        _ => None,
    }
}

/// Reverse a comparison operator (for const op var -> var op const)
fn reverse_compare_op(op: crate::ir::CompareOp) -> crate::ir::CompareOp {
    use crate::ir::CompareOp;
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

/// Create a range constraint from a comparison
fn create_constraint(
    var: VarId,
    op: crate::ir::CompareOp,
    val: RangeValue,
    _from_reversed: bool,
) -> RangeConstraint {
    use crate::ir::CompareOp;

    let mut constraint = RangeConstraint::new(var);

    match op {
        CompareOp::Eq => {
            // Equality: lower = upper = val, both inclusive
            constraint.lower = Some((val.clone(), true));
            constraint.upper = Some((val, true));
        }
        CompareOp::Lt => {
            // ?var < val: upper bound exclusive
            constraint.upper = Some((val, false));
        }
        CompareOp::Le => {
            // ?var <= val: upper bound inclusive
            constraint.upper = Some((val, true));
        }
        CompareOp::Gt => {
            // ?var > val: lower bound exclusive
            constraint.lower = Some((val, false));
        }
        CompareOp::Ge => {
            // ?var >= val: lower bound inclusive
            constraint.lower = Some((val, true));
        }
        CompareOp::Ne => {
            // Not equal: cannot be represented as range constraint
            // (would need to split into two ranges)
        }
    }

    constraint
}

use crate::ir::Expression;
use fluree_db_core::ObjectBounds;

impl RangeValue {
    /// Convert to FlakeValue for use with ObjectBounds
    pub fn to_flake_value(&self) -> FlakeValue {
        match self {
            RangeValue::Long(n) => FlakeValue::Long(*n),
            RangeValue::Double(d) => FlakeValue::Double(*d),
            RangeValue::String(s) => FlakeValue::String(s.clone()),
            RangeValue::Temporal(fv) => fv.clone(),
        }
    }
}

impl RangeConstraint {
    /// Convert to ObjectBounds for filter pushdown
    ///
    /// Returns None if the constraint has no bounds (would match everything).
    pub fn to_object_bounds(&self) -> Option<ObjectBounds> {
        if self.lower.is_none() && self.upper.is_none() {
            return None;
        }

        let mut bounds = ObjectBounds::new();

        if let Some((val, inclusive)) = &self.lower {
            bounds = bounds.with_lower(val.to_flake_value(), *inclusive);
        }

        if let Some((val, inclusive)) = &self.upper {
            bounds = bounds.with_upper(val.to_flake_value(), *inclusive);
        }

        Some(bounds)
    }
}

/// Extract object bounds for a pattern's object variable from a filter
///
/// Given a filter expression and a variable ID (typically the object variable
/// of a triple pattern), extracts range bounds that can be pushed down to
/// the scan operator.
///
/// # Returns
///
/// - `Some(ObjectBounds)` if the filter has range-safe constraints on the variable
/// - `None` if no pushdown is possible (not range-safe, wrong variable, etc.)
///
/// Extracts range-safe constraints from a filter expression for the given object variable and converts them to `ObjectBounds` for scan pushdown.
pub fn extract_object_bounds_for_var(
    filter: &Expression,
    object_var: VarId,
) -> Option<ObjectBounds> {
    // Only proceed if filter is range-safe
    let constraints = extract_range_constraints(filter)?;

    // Find constraint for our object variable
    let constraint = constraints.into_iter().find(|c| c.var == object_var)?;

    // Check if the range is satisfiable
    if constraint.is_unsatisfiable() {
        // Unsatisfiable range means the query will return no results.
        // For now, return None (filter won't be pushed down, will filter to empty later).
        // A future optimization could short-circuit the entire query.
        return None;
    }

    constraint.to_object_bounds()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::Term;
    use fluree_db_core::{PropertyStatData, Sid, StatsView};
    use std::sync::Arc;

    fn make_pattern(s: VarId, p_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(Term::Var(s), Term::Sid(Sid::new(100, p_name)), Term::Var(o))
    }

    #[test]
    fn test_reorder_patterns() {
        // Create patterns that should be reordered
        // p1: ?s :name ?name (shares ?s)
        // p2: ?x :type ?t (no sharing - will go last)
        // p3: ?s :age ?age (shares ?s)

        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(2), "type", VarId(3));
        let p3 = make_pattern(VarId(0), "age", VarId(4));

        let patterns = vec![p2.clone(), p1.clone(), p3.clone()];
        let ordered = reorder_patterns(patterns, None);

        // Without stats, all PropertyScan patterns get DEFAULT_SELECTIVITY (1000)
        // The tie-breaker orders by predicate name, so "age" < "name" < "type"
        // First pattern: p3 (age) comes first due to tie-breaker
        // Second: p1 (name) shares ?s with p3
        // Third: p2 (type) has no vars in common - Cartesian fallback
        assert_eq!(ordered.len(), 3);

        // Verify p2 is last (has no vars in common with others)
        let last_vars: HashSet<_> = ordered[2].variables().into_iter().collect();
        assert!(last_vars.contains(&VarId(2)) && last_vars.contains(&VarId(3)));
    }

    #[test]
    fn test_reorder_patterns_seeded_prefers_joinable_over_more_selective_cartesian() {
        // If vars are already bound from an upstream operator, prefer patterns that
        // join with those vars to avoid cartesian explosions (Clojure parity).

        let s = VarId(0);
        let o1 = VarId(1);
        let x = VarId(2);
        let y = VarId(3);

        // Joinable with seed (?s)
        let joinable = TriplePattern::new(
            Term::Var(s),
            Term::Sid(Sid::new(100, "wide")),
            Term::Var(o1),
        );

        // Not joinable with seed, but extremely selective
        let non_joining = TriplePattern::new(
            Term::Var(x),
            Term::Sid(Sid::new(100, "narrow")),
            Term::Var(y),
        );

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "wide"),
            fluree_db_core::PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000,
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "narrow"),
            fluree_db_core::PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );

        let mut seed = HashSet::new();
        seed.insert(s);

        let ordered = reorder_patterns_seeded(vec![non_joining, joinable], Some(&stats), &seed);

        assert!(
            ordered[0].variables().contains(&s),
            "expected first pattern to join with seeded bound vars"
        );
    }

    #[test]
    fn test_reorder_patterns_with_stats_but_iri_predicates_dont_use_stats_yet() {
        // This test is intentionally designed to expose a current gap:
        //
        // - The query parser/lowering emits `Term::Iri` for IRIs to support cross-ledger joins.
        // - Property statistics in `StatsView` are keyed by `Sid`.
        // - The planner's selectivity calculation (`calculate_selectivity`) only consults stats
        //   when it can extract a `Sid` via `pattern.p.as_sid()`.
        //
        // As a result, even when stats *exist*, planning may silently fall back to default scoring
        // for parsed queries, because their predicates are `Term::Iri`.
        //
        // Desired behavior (future fix): if stats exist for a predicate IRI, we should use them
        // so that join order reflects cardinality/selectivity.

        let s = VarId(0);
        let o1 = VarId(1);
        let o2 = VarId(2);

        // Two property-scan patterns with IRI predicates.
        // Tie-breaker (when stats aren't used) will sort by predicate IRI string.
        let p_a = TriplePattern::new(
            Term::Var(s),
            Term::Iri(Arc::from("http://example.org/a")),
            Term::Var(o1),
        );
        let p_z = TriplePattern::new(
            Term::Var(s),
            Term::Iri(Arc::from("http://example.org/z")),
            Term::Var(o2),
        );

        // Stats say predicate "z" is far more selective (count 1 vs 1000).
        // If stats were consulted during planning, p_z should be ordered first.
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "a"),
            PropertyStatData {
                count: 1000,
                ndv_values: 1000,
                ndv_subjects: 1000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "z"),
            PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );
        // Populate IRI-keyed maps as a stand-in for the real `from_db_stats_with_namespaces`
        // construction used in execution.
        stats.properties_by_iri.insert(
            Arc::from("http://example.org/a"),
            PropertyStatData {
                count: 1000,
                ndv_values: 1000,
                ndv_subjects: 1000,
            },
        );
        stats.properties_by_iri.insert(
            Arc::from("http://example.org/z"),
            PropertyStatData {
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
            },
        );

        let ordered = reorder_patterns(vec![p_a, p_z], Some(&stats));

        // EXPECTATION (desired): stats-driven selectivity should pick predicate ".../z" first.
        //
        // CURRENT (bug/gap): because predicates are Term::Iri, stats lookups miss and ordering
        // falls back to tie-breaker, which will choose ".../a" first. This assertion should fail
        // until we fix stats usage for IRI predicates.
        assert!(
            matches!(&ordered[0].p, Term::Iri(iri) if iri.as_ref() == "http://example.org/z"),
            "expected stats-driven ordering to pick the most selective predicate first; got ordered[0]={:?}",
            ordered[0]
        );
    }

    #[test]
    fn test_can_match_pattern() {
        let pattern = make_pattern(VarId(0), "name", VarId(1));

        // All unbound: can match
        let states = HashMap::new();
        assert!(can_match_pattern(&pattern, &states));

        // Some bound: can match
        let mut states2 = HashMap::new();
        states2.insert(VarId(0), BindingState::Bound);
        assert!(can_match_pattern(&pattern, &states2));

        // Poisoned var: cannot match
        let mut states3 = HashMap::new();
        states3.insert(VarId(0), BindingState::Poisoned);
        assert!(!can_match_pattern(&pattern, &states3));
    }

    #[test]
    fn test_is_property_join() {
        // Valid property join: ?s :name ?n, ?s :age ?a
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "age", VarId(2));
        assert!(is_property_join(&[p1.clone(), p2.clone()]));

        // Not property join: different subjects
        let p3 = make_pattern(VarId(3), "type", VarId(4));
        assert!(!is_property_join(&[p1.clone(), p3]));

        // Not property join: single pattern
        assert!(!is_property_join(std::slice::from_ref(&p1)));

        // Not property join: predicate is var
        let p4 = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Var(VarId(5)),
            Term::Var(VarId(1)),
        );
        assert!(!is_property_join(&[p1, p4]));
    }

    #[test]
    fn test_plan_basic() {
        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        let result = plan(&query, &HashMap::new());

        assert_eq!(result.schema.len(), 2);
        assert_eq!(result.ordered_patterns.len(), 1);
        assert!(result.var_states.get(&VarId(0)).unwrap().is_bound());
        assert!(result.var_states.get(&VarId(1)).unwrap().is_bound());
    }

    #[test]
    fn test_plan_multiple_patterns() {
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "age", VarId(2));
        let query = Query::new(
            vec![VarId(0), VarId(1), VarId(2)],
            vec![Pattern::Triple(p1), Pattern::Triple(p2)],
        );

        let result = plan(&query, &HashMap::new());

        assert_eq!(result.ordered_patterns.len(), 2);
        assert_eq!(result.bound_vars().len(), 3);
    }

    #[test]
    fn test_plan_upgrades_unbound_to_bound() {
        // Input: ?s is Unbound, ?name is missing
        // Pattern: ?s :name ?name
        // Output: both should be Bound
        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        let mut input_states = HashMap::new();
        input_states.insert(VarId(0), BindingState::Unbound);

        let result = plan(&query, &input_states);

        // Unbound -> Bound after pattern execution
        assert_eq!(
            result.var_states.get(&VarId(0)),
            Some(&BindingState::Bound),
            "Unbound should be upgraded to Bound"
        );
        // Missing -> Bound
        assert_eq!(
            result.var_states.get(&VarId(1)),
            Some(&BindingState::Bound),
            "Missing var should become Bound"
        );
    }

    #[test]
    fn test_plan_preserves_poisoned() {
        // Input: ?s is Poisoned (from failed OPTIONAL)
        // Pattern: ?s :name ?name
        // Output: ?s stays Poisoned, ?name becomes Bound
        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        let mut input_states = HashMap::new();
        input_states.insert(VarId(0), BindingState::Poisoned);

        let result = plan(&query, &input_states);

        // Poisoned stays Poisoned (not upgraded!)
        assert_eq!(
            result.var_states.get(&VarId(0)),
            Some(&BindingState::Poisoned),
            "Poisoned should NOT be upgraded to Bound"
        );
        // New var becomes Bound
        assert_eq!(
            result.var_states.get(&VarId(1)),
            Some(&BindingState::Bound),
            "New var should become Bound"
        );
    }

    #[test]
    fn test_plan_bound_stays_bound() {
        // Input: ?s is already Bound
        // Pattern: ?s :name ?name
        // Output: ?s stays Bound
        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        let mut input_states = HashMap::new();
        input_states.insert(VarId(0), BindingState::Bound);

        let result = plan(&query, &input_states);

        assert_eq!(
            result.var_states.get(&VarId(0)),
            Some(&BindingState::Bound),
            "Bound should stay Bound"
        );
    }

    // Range extraction tests
    use crate::ir::{Expression, FilterValue, Function};

    #[test]
    fn test_extract_range_simple_gt() {
        // ?age > 18
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(18)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), false))); // > is exclusive
        assert_eq!(c.upper, None);
    }

    #[test]
    fn test_extract_range_simple_le() {
        // ?age <= 65
        let expr = Expression::le(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(65)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        assert_eq!(c.lower, None);
        assert_eq!(c.upper, Some((RangeValue::Long(65), true))); // <= is inclusive
    }

    #[test]
    fn test_extract_range_eq() {
        // ?status = "active"
        let expr = Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::String("active".to_string())),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        // Equality means lower = upper = val, both inclusive
        assert_eq!(
            c.lower,
            Some((RangeValue::String("active".to_string()), true))
        );
        assert_eq!(
            c.upper,
            Some((RangeValue::String("active".to_string()), true))
        );
    }

    #[test]
    fn test_extract_range_reversed_comparison() {
        // 18 < ?age (means ?age > 18)
        let expr = Expression::lt(
            Expression::Const(FilterValue::Long(18)),
            Expression::Var(VarId(0)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        // 18 < ?age means ?age > 18
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), false))); // > is exclusive
        assert_eq!(c.upper, None);
    }

    #[test]
    fn test_extract_range_and_merges() {
        // ?age >= 18 AND ?age < 65
        let expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]);

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(18), true))); // >= inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(65), false))); // < exclusive
    }

    #[test]
    fn test_extract_range_and_multiple_vars() {
        // ?age >= 18 AND ?score > 100
        let expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(100)),
            ),
        ]);

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 2);

        // Check both vars have constraints
        let vars: Vec<_> = constraints.iter().map(|c| c.var).collect();
        assert!(vars.contains(&VarId(0)));
        assert!(vars.contains(&VarId(1)));
    }

    #[test]
    fn test_extract_range_or_not_supported() {
        // OR is not range-safe
        let expr = Expression::or(vec![
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(1)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(2)),
            ),
        ]);

        assert!(extract_range_constraints(&expr).is_none());
    }

    #[test]
    fn test_extract_range_double_values() {
        // ?price > 19.99
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Double(19.99)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        assert_eq!(c.lower, Some((RangeValue::Double(19.99), false)));
    }

    #[test]
    fn test_range_constraint_merge_tighter_lower() {
        // ?age > 10 AND ?age > 20 => lower should be 20
        let mut c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(10), false);
        let c2 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(20), false);

        c1.merge(&c2);
        assert_eq!(c1.lower, Some((RangeValue::Long(20), false)));
    }

    #[test]
    fn test_range_constraint_merge_tighter_upper() {
        // ?age < 100 AND ?age < 65 => upper should be 65
        let mut c1 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(100), false);
        let c2 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(65), false);

        c1.merge(&c2);
        assert_eq!(c1.upper, Some((RangeValue::Long(65), false)));
    }

    #[test]
    fn test_range_constraint_merge_exclusivity() {
        // ?age >= 18 AND ?age > 18 => should be exclusive (tighter)
        let mut c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), true); // inclusive
        let c2 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), false); // exclusive

        c1.merge(&c2);
        // Exclusive is tighter than inclusive at the same value
        assert_eq!(c1.lower, Some((RangeValue::Long(18), false)));
    }

    #[test]
    fn test_range_constraint_unsatisfiable_lower_gt_upper() {
        // ?x > 10 AND ?x < 5 => unsatisfiable (lower > upper)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(5), false);

        assert!(c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_unsatisfiable_equal_exclusive() {
        // ?x > 10 AND ?x < 10 => unsatisfiable (equal but both exclusive)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(10), false);

        assert!(c.is_unsatisfiable());

        // ?x >= 10 AND ?x < 10 => unsatisfiable (equal, one exclusive)
        let c2 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), true)
            .with_upper(RangeValue::Long(10), false);

        assert!(c2.is_unsatisfiable());

        // ?x > 10 AND ?x <= 10 => unsatisfiable (equal, one exclusive)
        let c3 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), false)
            .with_upper(RangeValue::Long(10), true);

        assert!(c3.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_equal_inclusive() {
        // ?x >= 10 AND ?x <= 10 => satisfiable (exactly 10)
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(10), true)
            .with_upper(RangeValue::Long(10), true);

        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_normal_range() {
        // ?x >= 5 AND ?x <= 10 => satisfiable
        let c = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(5), true)
            .with_upper(RangeValue::Long(10), true);

        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_range_constraint_satisfiable_open_ended() {
        // Only lower bound => always satisfiable
        let c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(10), false);
        assert!(!c1.is_unsatisfiable());

        // Only upper bound => always satisfiable
        let c2 = RangeConstraint::new(VarId(0)).with_upper(RangeValue::Long(10), false);
        assert!(!c2.is_unsatisfiable());

        // No bounds => always satisfiable
        let c3 = RangeConstraint::new(VarId(0));
        assert!(!c3.is_unsatisfiable());
    }

    // Tests for object bounds conversion and pushdown

    #[test]
    fn test_range_value_to_flake_value() {
        assert_eq!(RangeValue::Long(42).to_flake_value(), FlakeValue::Long(42));
        assert_eq!(
            RangeValue::String("hello".to_string()).to_flake_value(),
            FlakeValue::String("hello".to_string())
        );
        // Double conversion
        let d = RangeValue::Double(3.13).to_flake_value();
        match d {
            FlakeValue::Double(v) => assert!((v - 3.13).abs() < 0.001),
            _ => panic!("Expected Double"),
        }
    }

    #[test]
    fn test_range_constraint_to_object_bounds() {
        // Lower bound only
        let c1 = RangeConstraint::new(VarId(0)).with_lower(RangeValue::Long(18), false);
        let bounds = c1.to_object_bounds().expect("should have bounds");
        assert!(!bounds.is_empty());
        // Verify it filters correctly
        assert!(!bounds.matches(&FlakeValue::Long(18))); // exclusive
        assert!(bounds.matches(&FlakeValue::Long(19)));

        // Two-sided bounds
        let c2 = RangeConstraint::new(VarId(0))
            .with_lower(RangeValue::Long(18), false)
            .with_upper(RangeValue::Long(65), true);
        let bounds = c2.to_object_bounds().expect("should have bounds");
        assert!(!bounds.matches(&FlakeValue::Long(18))); // exclusive lower
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(65))); // inclusive upper
        assert!(!bounds.matches(&FlakeValue::Long(66)));
    }

    #[test]
    fn test_range_constraint_to_object_bounds_empty() {
        // No bounds => None
        let c = RangeConstraint::new(VarId(0));
        assert!(c.to_object_bounds().is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_simple() {
        // ?age > 18
        let filter = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(18)),
        );

        // Extract for ?age (VarId(0))
        let bounds =
            extract_object_bounds_for_var(&filter, VarId(0)).expect("should extract bounds");

        // Should filter 18 out (exclusive) but include 19
        assert!(!bounds.matches(&FlakeValue::Long(18)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
    }

    #[test]
    fn test_extract_object_bounds_for_var_two_sided() {
        // ?age > 18 AND ?age < 65
        let filter = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]);

        let bounds =
            extract_object_bounds_for_var(&filter, VarId(0)).expect("should extract bounds");

        assert!(!bounds.matches(&FlakeValue::Long(18)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(bounds.matches(&FlakeValue::Long(64)));
        assert!(!bounds.matches(&FlakeValue::Long(65)));
    }

    #[test]
    fn test_extract_object_bounds_for_var_wrong_var() {
        // ?age > 18 - but we ask for bounds on ?name (VarId(1))
        let filter = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(18)),
        );

        // No bounds for VarId(1)
        assert!(extract_object_bounds_for_var(&filter, VarId(1)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_unsatisfiable() {
        // ?x > 10 AND ?x < 5 => unsatisfiable, returns None
        let filter = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(10)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(5)),
            ),
        ]);

        // Unsatisfiable range returns None (no point in pushdown)
        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_not_range_safe() {
        // OR is not range-safe
        let filter = Expression::or(vec![
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(21)),
            ),
        ]);

        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }

    // =========================================================================
    // Sandwich (3-arg variadic comparison) tests
    // =========================================================================

    /// Build a 3-arg comparison expression: (op a b c)
    fn sandwich(func: Function, a: Expression, b: Expression, c: Expression) -> Expression {
        Expression::Call {
            func,
            args: vec![a, b, c],
        }
    }

    #[test]
    fn test_is_range_safe_sandwich() {
        // (< 10 ?x 20) → range-safe
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (<= 10 ?x 20) → range-safe
        let expr = sandwich(
            Function::Le,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );
        assert!(expr.is_range_safe());

        // (> 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Gt,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (>= 20 ?x 10) → range-safe
        let expr = sandwich(
            Function::Ge,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // (= 5 ?x 5) → range-safe
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(5)),
        );
        assert!(expr.is_range_safe());
    }

    #[test]
    fn test_is_range_safe_non_sandwich_variadic() {
        // (< ?x ?y 20) → NOT range-safe (var var const)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Var(VarId(0)),
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(20)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< 10 20 ?x) → NOT range-safe (const const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Const(FilterValue::Long(10)),
                Expression::Const(FilterValue::Long(20)),
                Expression::Var(VarId(0)),
            ],
        };
        assert!(!expr.is_range_safe());

        // (< ?x 10 ?y) → NOT range-safe (var const var)
        let expr = Expression::Call {
            func: Function::Lt,
            args: vec![
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(10)),
                Expression::Var(VarId(1)),
            ],
        };
        assert!(!expr.is_range_safe());
    }

    #[test]
    fn test_extract_range_sandwich_lt() {
        // (< 10 ?x 20) → lower=10 exclusive, upper=20 exclusive
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(10), false))); // 10 < ?x → exclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), false))); // ?x < 20 → exclusive
    }

    #[test]
    fn test_extract_range_sandwich_le() {
        // (<= 10 ?x 20) → lower=10 inclusive, upper=20 inclusive
        let expr = sandwich(
            Function::Le,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(10), true))); // 10 <= ?x → inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), true))); // ?x <= 20 → inclusive
    }

    #[test]
    fn test_extract_range_sandwich_gt() {
        // (> 20 ?x 10) → 20 > ?x > 10 → upper=20 exclusive, lower=10 exclusive
        let expr = sandwich(
            Function::Gt,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        // Pair 1: 20 > ?x → ?x < 20 → upper=20 exclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), false)));
        // Pair 2: ?x > 10 → lower=10 exclusive
        assert_eq!(c.lower, Some((RangeValue::Long(10), false)));
    }

    #[test]
    fn test_extract_range_sandwich_ge() {
        // (>= 20 ?x 10) → 20 >= ?x >= 10 → upper=20 inclusive, lower=10 inclusive
        let expr = sandwich(
            Function::Ge,
            Expression::Const(FilterValue::Long(20)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        // Pair 1: 20 >= ?x → ?x <= 20 → upper=20 inclusive
        assert_eq!(c.upper, Some((RangeValue::Long(20), true)));
        // Pair 2: ?x >= 10 → lower=10 inclusive
        assert_eq!(c.lower, Some((RangeValue::Long(10), true)));
    }

    #[test]
    fn test_extract_range_sandwich_eq() {
        // (= 5 ?x 5) → point range: lower=5 inclusive, upper=5 inclusive
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(5)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        assert_eq!(c.var, VarId(0));
        assert_eq!(c.lower, Some((RangeValue::Long(5), true)));
        assert_eq!(c.upper, Some((RangeValue::Long(5), true)));
        assert!(!c.is_unsatisfiable());
    }

    #[test]
    fn test_extract_range_sandwich_eq_unsatisfiable() {
        // (= 5 ?x 10) → both constants must be equal for satisfiability
        let expr = sandwich(
            Function::Eq,
            Expression::Const(FilterValue::Long(5)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );

        let constraints = extract_range_constraints(&expr).expect("should extract");
        assert_eq!(constraints.len(), 1);

        let c = &constraints[0];
        // lower=5 inclusive, upper=10 inclusive (from two Eq constraints)
        // But Eq sets BOTH lower and upper, so merge produces:
        // lower = max(5, 10) = 10 inclusive, upper = min(5, 10) = 5 inclusive
        // This is unsatisfiable (lower > upper)
        assert!(c.is_unsatisfiable());
    }

    #[test]
    fn test_extract_range_sandwich_object_bounds() {
        // (< 10 ?x 20) should produce correct ObjectBounds
        let expr = sandwich(
            Function::Lt,
            Expression::Const(FilterValue::Long(10)),
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );

        let bounds = extract_object_bounds_for_var(&expr, VarId(0)).expect("should extract bounds");

        assert!(!bounds.matches(&FlakeValue::Long(10))); // exclusive lower
        assert!(bounds.matches(&FlakeValue::Long(11)));
        assert!(bounds.matches(&FlakeValue::Long(19)));
        assert!(!bounds.matches(&FlakeValue::Long(20))); // exclusive upper
        assert!(!bounds.matches(&FlakeValue::Long(5))); // below range
        assert!(!bounds.matches(&FlakeValue::Long(25))); // above range
    }
}
