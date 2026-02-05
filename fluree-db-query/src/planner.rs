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

use crate::ir::{Pattern, Query};
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, StatsView};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Three binding states for var tracking
///
/// Used during planning to track which variables are:
/// - Unbound: Not yet bound, can be bound by future patterns
/// - Bound: Guaranteed bound from a pattern or VALUES
/// - Poisoned: From failed OPTIONAL, blocks future matching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingState {
    /// Not yet bound, can be bound
    Unbound,
    /// Guaranteed bound
    Bound,
    /// From failed OPTIONAL, blocks future matching
    Poisoned,
}

impl BindingState {
    /// Check if this state is poisoned
    pub fn is_poisoned(&self) -> bool {
        matches!(self, BindingState::Poisoned)
    }

    /// Check if this state represents a bound variable
    pub fn is_bound(&self) -> bool {
        matches!(self, BindingState::Bound)
    }
}

/// Result of planning a query
///
/// Contains the planned execution information and metadata.
/// Note: Planner functions are generic over S, C to produce typed operators.
/// PlanResult stores the operator but otherwise stays small and local.
#[derive(Debug)]
pub struct PlanResult {
    /// Output schema (variables in output order)
    pub schema: Arc<[VarId]>,
    /// Full state tracking for all variables
    pub var_states: HashMap<VarId, BindingState>,
    /// Estimated row count (if available)
    pub estimated_rows: Option<usize>,
    /// Ordered triple patterns after reordering
    pub ordered_patterns: Vec<TriplePattern>,
}

impl PlanResult {
    /// Vars guaranteed bound (not Unbound, not Poisoned)
    ///
    /// Note: this is "guaranteed bound", not "present in schema"
    pub fn bound_vars(&self) -> HashSet<VarId> {
        self.var_states
            .iter()
            .filter(|(_, s)| **s == BindingState::Bound)
            .map(|(v, _)| *v)
            .collect()
    }
}

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

// Selectivity score constants (matching Clojure behavior)
/// Most selective - exact match
const HIGHLY_SELECTIVE: i64 = 0;
/// Medium selectivity - fallback for bound-subject patterns
const MODERATELY_SELECTIVE: i64 = 10;
/// Fallback for bound-object and property-scan patterns
const DEFAULT_SELECTIVITY: i64 = 1000;
/// Full scan - all variables unbound
const FULL_SCAN: i64 = 1_000_000_000_000; // 1e12

// Clojure fallback caps differ by pattern type:
// - bound-subject (s p ?o): min count 10
// - bound-object (?s p o): min count 1000
const BOUND_SUBJECT_FALLBACK_CAP: i64 = 10;
const BOUND_OBJECT_FALLBACK_CAP: i64 = 1000;

/// Classify a triple pattern for selectivity scoring
pub(crate) fn classify_pattern(pattern: &TriplePattern) -> PatternType {
    let s_bound = pattern.s_bound();
    let p_bound = pattern.p_bound();
    let o_bound = pattern.o_bound();

    // Check for rdf:type predicate with bound object → Class pattern
    // Use Term::is_rdf_type() to handle both Term::Sid and Term::Iri predicates
    if p_bound && o_bound && !s_bound && pattern.p.is_rdf_type() {
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

/// Calculate selectivity score for a pattern using statistics
///
/// Lower score = more selective = should be executed first.
/// When stats are not available, uses conservative fallback values.
pub(crate) fn calculate_selectivity(pattern: &TriplePattern, stats: Option<&StatsView>) -> i64 {
    let pattern_type = classify_pattern(pattern);

    // Helper: get property stats for a predicate represented as Sid or IRI.
    fn get_prop<'a>(
        stats: &'a StatsView,
        pred: &Term,
    ) -> Option<&'a fluree_db_core::PropertyStatData> {
        if let Some(sid) = pred.as_sid() {
            return stats.get_property(sid);
        }
        if let Some(iri) = pred.as_iri() {
            return stats.get_property_by_iri(iri);
        }
        None
    }

    // Helper: get class count for a class represented as Sid or IRI.
    fn get_class(stats: &StatsView, class: &Term) -> Option<u64> {
        if let Some(sid) = class.as_sid() {
            return stats.get_class_count(sid);
        }
        if let Some(iri) = class.as_iri() {
            return stats.get_class_count_by_iri(iri);
        }
        None
    }

    match pattern_type {
        PatternType::ExactMatch => HIGHLY_SELECTIVE,

        PatternType::ClassPattern => {
            // Use class count from stats
            if let Some(s) = stats {
                if let Some(count) = get_class(s, &pattern.o) {
                    return count as i64;
                }
            }
            // If class counts are not available, approximate using rdf:type property stats:
            // expected per-class count ~= ceil(type_count / ndv_classes).
            if let Some(s) = stats {
                if let Some(prop) = get_prop(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        let sel = (prop.count as f64 / prop.ndv_values as f64).ceil() as i64;
                        return sel.max(1);
                    }
                }
            }
            // Fallback: distinguish class-pattern fallback from other default-selectivity
            // fallbacks so ties don't arbitrarily prefer class scans.
            DEFAULT_SELECTIVITY + 1
        }

        PatternType::BoundSubject => {
            // Subject + predicate bound: selectivity = ceil(count / ndv_subjects)
            // This estimates "how many values per subject" for this property
            if let Some(s) = stats {
                if let Some(prop) = get_prop(s, &pattern.p) {
                    if prop.ndv_subjects > 0 {
                        let sel = (prop.count as f64 / prop.ndv_subjects as f64).ceil() as i64;
                        return sel.max(1);
                    }
                    // Have count but no NDV - use count capped (Clojure: min count 10)
                    return (prop.count as i64).min(BOUND_SUBJECT_FALLBACK_CAP);
                }
            }
            // Fallback: subject-bound patterns are usually selective
            MODERATELY_SELECTIVE
        }

        PatternType::BoundObject => {
            // Object + predicate bound: selectivity = ceil(count / ndv_values)
            // This estimates "how many subjects have this value" for this property
            if let Some(s) = stats {
                if let Some(prop) = get_prop(s, &pattern.p) {
                    if prop.ndv_values > 0 {
                        let sel = (prop.count as f64 / prop.ndv_values as f64).ceil() as i64;
                        return sel.max(1);
                    }
                    // Have count but no NDV - use count capped (Clojure: min count 1000)
                    return (prop.count as i64).min(BOUND_OBJECT_FALLBACK_CAP);
                }
            }
            // Fallback: object-bound can vary widely
            DEFAULT_SELECTIVITY
        }

        PatternType::PropertyScan => {
            // Only predicate bound: use property count
            if let Some(s) = stats {
                if let Some(prop) = get_prop(s, &pattern.p) {
                    return prop.count as i64;
                }
            }
            DEFAULT_SELECTIVITY
        }

        PatternType::FullScan => FULL_SCAN,
    }
}

/// Score a pattern based purely on selectivity
///
/// Lower score = more selective = better.
/// The join-preferring behavior is handled by the "filter joinable candidates first"
/// logic in `reorder_patterns`, not by score adjustments.
fn score_pattern_selectivity(pattern: &TriplePattern, stats: Option<&StatsView>) -> i64 {
    calculate_selectivity(pattern, stats)
}

/// Compare two Terms for deterministic ordering (tie-breaker)
///
/// Provides stable ordering: Var < Sid < Iri < Value, then by inner value.
pub(crate) fn compare_term(a: &Term, b: &Term) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        // Variables sort before non-variables, then by VarId
        (Term::Var(va), Term::Var(vb)) => va.cmp(vb),
        (Term::Var(_), _) => Ordering::Less,
        (_, Term::Var(_)) => Ordering::Greater,
        // SIDs sort by namespace then name
        (Term::Sid(sa), Term::Sid(sb)) => sa.cmp(sb),
        (Term::Sid(_), _) => Ordering::Less,
        // IRIs sort after SIDs but before Values
        (Term::Iri(ia), Term::Iri(ib)) => ia.cmp(ib),
        (Term::Iri(_), Term::Value(_)) => Ordering::Less,
        (Term::Iri(_), Term::Sid(_)) => Ordering::Greater,
        // Values sort last
        (Term::Value(_), Term::Sid(_) | Term::Iri(_)) => Ordering::Greater,
        // Values: compare debug representation for determinism
        // A cleaner solution would use canonical_hash() if available
        (Term::Value(va), Term::Value(vb)) => format!("{:?}", va).cmp(&format!("{:?}", vb)),
    }
}

/// Deterministic tie-breaker for equal selectivity scores
///
/// Clojure parity priority, then lexicographic (p, s, o) for reproducibility.
pub(crate) fn compare_patterns_tiebreaker(
    a: &TriplePattern,
    b: &TriplePattern,
) -> std::cmp::Ordering {
    // Prefer patterns that are "cheaper" even when stats tie.
    //
    // This matters in cases where selectivity scores collide (e.g. missing NDV),
    // but we still want consistent improvements:
    // - bound-object lookups should precede class scans
    // - class scans should precede property scans when scores tie
    //
    // This also prevents meaningless reorders for equal-selectivity class vs property
    // patterns when class is already first (Clojure explain-no-optimization-test).
    fn rank(p: &TriplePattern) -> u8 {
        match classify_pattern(p) {
            PatternType::ExactMatch => 0,
            PatternType::BoundObject => 1,
            PatternType::BoundSubject => 2,
            PatternType::ClassPattern => 3,
            PatternType::PropertyScan => 4,
            PatternType::FullScan => 5,
        }
    }

    let ra = rank(a);
    let rb = rank(b);
    match ra.cmp(&rb) {
        std::cmp::Ordering::Equal => {}
        other => return other,
    }

    let pred_cmp = compare_term(&a.p, &b.p);
    if pred_cmp != std::cmp::Ordering::Equal {
        return pred_cmp;
    }
    let subj_cmp = compare_term(&a.s, &b.s);
    if subj_cmp != std::cmp::Ordering::Equal {
        return subj_cmp;
    }
    compare_term(&a.o, &b.o)
}

/// Check if a pattern shares any variables with the bound set
pub(crate) fn shares_variables(pattern: &TriplePattern, bound_vars: &HashSet<VarId>) -> bool {
    pattern.variables().iter().any(|v| bound_vars.contains(v))
}

/// Reorder patterns for optimal join order using statistics-based selectivity
///
/// Uses a greedy algorithm that:
/// 1. First filters for patterns sharing variables with already-bound set (join candidates)
/// 2. Within candidates, picks the lowest selectivity score (most selective)
/// 3. Uses deterministic tie-breaker for equal scores
/// 4. Falls back to all remaining if no joining patterns (Cartesian fallback)
///
/// When `stats` is `None`, falls back to conservative selectivity estimates.
pub fn reorder_patterns(
    patterns: Vec<TriplePattern>,
    stats: Option<&StatsView>,
) -> Vec<TriplePattern> {
    reorder_patterns_seeded(patterns, stats, &HashSet::new())
}

/// Reorder patterns for optimal join order using statistics-based selectivity,
/// seeded with an initial set of already-bound variables.
///
/// This matches the Clojure optimizer behavior more closely: when some variables
/// are already bound from a prior operator (VALUES/BIND/previous clause), prefer
/// patterns that *join* with those bindings rather than starting with a pattern
/// that would create a cartesian explosion.
///
/// The algorithm is the same as `reorder_patterns`, except `bound_vars` starts from
/// `initial_bound_vars` rather than empty.
pub fn reorder_patterns_seeded(
    patterns: Vec<TriplePattern>,
    stats: Option<&StatsView>,
    initial_bound_vars: &HashSet<VarId>,
) -> Vec<TriplePattern> {
    if patterns.len() <= 1 {
        return patterns;
    }

    tracing::debug!(
        pattern_count = patterns.len(),
        has_stats = stats.is_some(),
        initial_bound = initial_bound_vars.len(),
        "reordering patterns for optimal join order"
    );

    let mut remaining: Vec<_> = patterns.into_iter().collect();
    let mut ordered = Vec::with_capacity(remaining.len());
    let mut bound_vars: HashSet<VarId> = initial_bound_vars.clone();

    while !remaining.is_empty() {
        // Find candidates that share variables with bound set (join-friendly)
        let has_bound = !bound_vars.is_empty();
        let candidates: Vec<usize> = remaining
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                if !has_bound {
                    true // First pattern: all candidates
                } else {
                    shares_variables(p, &bound_vars)
                }
            })
            .map(|(i, _)| i)
            .collect();

        // If no joining candidates, use all remaining (Cartesian fallback)
        let pool: Vec<usize> = if candidates.is_empty() {
            if tracing::enabled!(tracing::Level::TRACE) {
                tracing::trace!(
                    step = ordered.len() + 1,
                    "no joining candidates, using Cartesian fallback"
                );
            }
            (0..remaining.len()).collect()
        } else {
            candidates
        };

        // Pick best (lowest score) from pool with tie-breaker
        let best_idx = pool
            .into_iter()
            .min_by(|&i, &j| {
                let score_i = score_pattern_selectivity(&remaining[i], stats);
                let score_j = score_pattern_selectivity(&remaining[j], stats);
                match score_i.cmp(&score_j) {
                    std::cmp::Ordering::Equal => {
                        compare_patterns_tiebreaker(&remaining[i], &remaining[j])
                    }
                    other => other,
                }
            })
            .unwrap();

        let chosen = remaining.remove(best_idx);
        let selectivity = score_pattern_selectivity(&chosen, stats);

        if tracing::enabled!(tracing::Level::TRACE) {
            let pattern_type = classify_pattern(&chosen);
            tracing::trace!(
                step = ordered.len() + 1,
                selectivity = selectivity,
                pattern_type = ?pattern_type,
                bound_vars = bound_vars.len(),
                "selected pattern"
            );
        }

        // Update bound variables
        for var in chosen.variables() {
            bound_vars.insert(var);
        }
        ordered.push(chosen);
    }

    tracing::debug!(
        reordered_count = ordered.len(),
        "pattern reordering completed"
    );

    ordered
}

/// Check if a pattern can be matched given current binding states
///
/// If any pattern var is in Poisoned state, this pattern cannot match.
pub fn can_match_pattern(
    pattern: &TriplePattern,
    var_states: &HashMap<VarId, BindingState>,
) -> bool {
    pattern.variables().iter().all(|v| {
        var_states.get(v).map(|s| !s.is_poisoned()).unwrap_or(true) // Unknown vars are fine
    })
}

/// Plan a query for execution
///
/// Currently performs pattern reordering for triple patterns only.
/// Full operator tree generation will be added in Phase 2 (Join) and beyond.
///
/// # Limitations (Phase 1)
///
/// - Only extracts top-level triple patterns from where clause
/// - Does not handle nested Optional/Union patterns (they are skipped)
/// - Does not generate physical operators (returns ordered patterns)
///
/// # Var State Semantics
///
/// - Missing vars become `Bound` after pattern execution
/// - `Unbound` vars are upgraded to `Bound`
/// - `Poisoned` vars stay `Poisoned` (never upgraded)
pub fn plan(query: &Query, input_states: &HashMap<VarId, BindingState>) -> PlanResult {
    // Extract triple patterns from where clause
    let triple_patterns: Vec<TriplePattern> = query
        .where_
        .iter()
        .filter_map(|p| match p {
            Pattern::Triple(tp) => Some(tp.clone()),
            _ => None,
        })
        .collect();

    // Reorder for optimal join order (plan() doesn't have stats access yet)
    let ordered_patterns = reorder_patterns(triple_patterns, None);

    // Compute output schema from select
    let schema: Arc<[VarId]> = Arc::from(query.select.clone().into_boxed_slice());

    // Compute var states after executing all patterns
    // Triple patterns upgrade: Missing/Unbound -> Bound, but Poisoned stays Poisoned
    let mut var_states = input_states.clone();
    for pattern in &ordered_patterns {
        for var in pattern.variables() {
            var_states
                .entry(var)
                .and_modify(|state| {
                    // Upgrade Unbound to Bound; Poisoned stays Poisoned
                    if *state == BindingState::Unbound {
                        *state = BindingState::Bound;
                    }
                })
                .or_insert(BindingState::Bound);
        }
    }

    PlanResult {
        schema,
        var_states,
        estimated_rows: None,
        ordered_patterns,
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
/// - `AND` of the above (constraints are merged for tighter bounds)
pub fn extract_range_constraints(expr: &FilterExpr) -> Option<Vec<RangeConstraint>> {
    if !expr.is_range_safe() {
        return None;
    }

    match expr {
        FilterExpr::Compare { op, left, right } => {
            // Try ?var op const
            if let (Some(var), Some(val)) = (extract_var(left), extract_const(right)) {
                return Some(vec![create_constraint(var, *op, val, false)]);
            }
            // Try const op ?var (reverse the comparison)
            if let (Some(val), Some(var)) = (extract_const(left), extract_var(right)) {
                let reversed_op = reverse_compare_op(*op);
                return Some(vec![create_constraint(var, reversed_op, val, false)]);
            }
            None
        }

        FilterExpr::And(exprs) => {
            let mut all_constraints: HashMap<VarId, RangeConstraint> = HashMap::new();

            for e in exprs {
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
    }
}

/// Extract a VarId from an expression if it's a simple variable reference
fn extract_var(expr: &FilterExpr) -> Option<VarId> {
    match expr {
        FilterExpr::Var(v) => Some(*v),
        _ => None,
    }
}

/// Extract a RangeValue from an expression if it's a constant
fn extract_const(expr: &FilterExpr) -> Option<RangeValue> {
    use crate::ir::FilterValue;

    match expr {
        FilterExpr::Const(FilterValue::Long(n)) => Some(RangeValue::Long(*n)),
        // NaN is not a meaningful range bound.
        FilterExpr::Const(FilterValue::Double(d)) if d.is_nan() => None,
        FilterExpr::Const(FilterValue::Double(d)) => Some(RangeValue::Double(*d)),
        FilterExpr::Const(FilterValue::String(s)) => Some(RangeValue::String(s.clone())),
        FilterExpr::Const(FilterValue::Temporal(fv)) => {
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

use crate::ir::FilterExpr;
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
    filter: &FilterExpr,
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
    use crate::ir::{CompareOp, FilterExpr, FilterValue};

    #[test]
    fn test_extract_range_simple_gt() {
        // ?age > 18
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
        };

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
        let expr = FilterExpr::Compare {
            op: CompareOp::Le,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(65))),
        };

        let constraints = extract_range_constraints(&expr).expect("should extract");
        let c = &constraints[0];

        assert_eq!(c.lower, None);
        assert_eq!(c.upper, Some((RangeValue::Long(65), true))); // <= is inclusive
    }

    #[test]
    fn test_extract_range_eq() {
        // ?status = "active"
        let expr = FilterExpr::Compare {
            op: CompareOp::Eq,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::String("active".to_string()))),
        };

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
        let expr = FilterExpr::Compare {
            op: CompareOp::Lt,
            left: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            right: Box::new(FilterExpr::Var(VarId(0))),
        };

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
        let expr = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Ge,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(65))),
            },
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
        let expr = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Ge,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(1))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(100))),
            },
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
        let expr = FilterExpr::Or(vec![
            FilterExpr::Compare {
                op: CompareOp::Eq,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(1))),
            },
            FilterExpr::Compare {
                op: CompareOp::Eq,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(2))),
            },
        ]);

        assert!(extract_range_constraints(&expr).is_none());
    }

    #[test]
    fn test_extract_range_double_values() {
        // ?price > 19.99
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Double(19.99))),
        };

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
        let filter = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
        };

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
        let filter = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(65))),
            },
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
        let filter = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
        };

        // No bounds for VarId(1)
        assert!(extract_object_bounds_for_var(&filter, VarId(1)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_unsatisfiable() {
        // ?x > 10 AND ?x < 5 => unsatisfiable, returns None
        let filter = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(10))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(5))),
            },
        ]);

        // Unsatisfiable range returns None (no point in pushdown)
        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }

    #[test]
    fn test_extract_object_bounds_for_var_not_range_safe() {
        // OR is not range-safe
        let filter = FilterExpr::Or(vec![
            FilterExpr::Compare {
                op: CompareOp::Eq,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Eq,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(21))),
            },
        ]);

        assert!(extract_object_bounds_for_var(&filter, VarId(0)).is_none());
    }
}
