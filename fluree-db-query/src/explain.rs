//! Query plan explanation for debugging optimization decisions
//!
//! Provides visibility into how the query optimizer reorders patterns
//! and computes selectivity scores.
//!
//! Call `explain_patterns` with a set of patterns and optional stats to get an `ExplainPlan`.

use crate::pattern::{Term, TriplePattern};
use crate::planner::{calculate_selectivity, classify_pattern, compare_patterns_tiebreaker, shares_variables, PatternType};
use crate::var_registry::VarId;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::fmt;

/// Explanation of query optimization decisions
#[derive(Debug, Clone)]
pub struct ExplainPlan {
    /// Whether patterns were reordered
    pub optimization: OptimizationStatus,
    /// Whether statistics were available for optimization
    pub statistics_available: bool,
    /// Original pattern order with selectivity info
    pub original_patterns: Vec<PatternDisplay>,
    /// Optimized pattern order with selectivity info
    pub optimized_patterns: Vec<PatternDisplay>,
}

/// Whether optimization changed the pattern order
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationStatus {
    /// Patterns were reordered for better performance
    Reordered,
    /// Pattern order unchanged (already optimal or single pattern)
    Unchanged,
}

/// Display information for a single pattern
#[derive(Debug, Clone)]
pub struct PatternDisplay {
    /// Human-readable pattern representation
    pub pattern: String,
    /// Classification of the pattern
    pub pattern_type: PatternType,
    /// Computed selectivity score (lower = more selective)
    pub selectivity_score: i64,
    /// Inputs used for selectivity calculation
    pub inputs: SelectivityInputs,
    /// Variables in this pattern
    pub variables: Vec<VarId>,
}

/// Reason why fallback scoring was used instead of stats-based scoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum FallbackReason {
    /// No stats available at all for this property
    NoPropertyStats,
    /// Property stats exist but NDV is zero (can't compute selectivity ratio)
    MissingNdv,
    /// Full scan pattern always uses fallback (no predicate to look up)
    FullScanPattern,
    /// Class pattern but class not found in stats
    ClassNotInStats,
}

/// Inputs used for selectivity calculation
#[derive(Debug, Clone, Default)]
pub struct SelectivityInputs {
    /// Property SID (if predicate is bound)
    pub property_sid: Option<String>,
    /// Property count from stats
    pub count: Option<u64>,
    /// Number of distinct values from stats
    pub ndv_values: Option<u64>,
    /// Number of distinct subjects from stats
    pub ndv_subjects: Option<u64>,
    /// Class count from stats (for rdf:type patterns)
    pub class_count: Option<u64>,
    /// Why fallback was used, or None if full stats were available
    pub fallback: Option<FallbackReason>,
}

/// Generate an explanation of pattern optimization
///
/// Analyzes the input patterns and shows how they would be reordered
/// based on selectivity scores.
pub fn explain_patterns(patterns: &[TriplePattern], stats: Option<&StatsView>) -> ExplainPlan {
    // Check both property and class stats
    let statistics_available = stats
        .map(|s| s.has_property_stats() || s.has_class_stats())
        .unwrap_or(false);

    // Build original pattern displays
    let original_patterns: Vec<PatternDisplay> = patterns
        .iter()
        .map(|p| build_pattern_display(p, stats))
        .collect();

    // Reorder patterns using the same algorithm as planner
    let optimized = reorder_for_explain(patterns.to_vec(), stats);
    let optimized_patterns: Vec<PatternDisplay> = optimized
        .iter()
        .map(|p| build_pattern_display(p, stats))
        .collect();

    // Check if order changed using structural comparison (PartialEq)
    let optimization = if patterns.len() <= 1 {
        OptimizationStatus::Unchanged
    } else {
        let same_order = patterns
            .iter()
            .zip(optimized.iter())
            .all(|(a, b)| a == b);
        if same_order {
            OptimizationStatus::Unchanged
        } else {
            OptimizationStatus::Reordered
        }
    };

    ExplainPlan {
        optimization,
        statistics_available,
        original_patterns,
        optimized_patterns,
    }
}

/// Build display info for a single pattern
fn build_pattern_display(pattern: &TriplePattern, stats: Option<&StatsView>) -> PatternDisplay {
    let pattern_type = classify_pattern(pattern);
    let selectivity_score = calculate_selectivity(pattern, stats);
    let inputs = capture_selectivity_inputs(pattern, pattern_type, stats);

    PatternDisplay {
        pattern: format_pattern(pattern),
        pattern_type,
        selectivity_score,
        inputs,
        variables: pattern.variables(),
    }
}

/// Capture the inputs used for selectivity calculation (for debugging)
fn capture_selectivity_inputs(
    pattern: &TriplePattern,
    pattern_type: PatternType,
    stats: Option<&StatsView>,
) -> SelectivityInputs {
    let mut inputs = SelectivityInputs::default();

    // Capture property SID if predicate is bound
    if let Some(s) = stats {
        // Prefer SID formatting when available (stable + compact).
        if let Some(pred_sid) = pattern.p.as_sid() {
            inputs.property_sid = Some(format!("{}:{}", pred_sid.namespace_code, &pred_sid.name));
            if let Some(prop) = s.get_property(pred_sid) {
                inputs.count = Some(prop.count);
                inputs.ndv_values = Some(prop.ndv_values);
                inputs.ndv_subjects = Some(prop.ndv_subjects);
            }
        } else if let Some(pred_iri) = pattern.p.as_iri() {
            if let Some(prop) = s.get_property_by_iri(pred_iri) {
                inputs.count = Some(prop.count);
                inputs.ndv_values = Some(prop.ndv_values);
                inputs.ndv_subjects = Some(prop.ndv_subjects);
            }
        }
    }

    // Capture class count for class patterns
    if pattern_type == PatternType::ClassPattern {
        if let Some(s) = stats {
            if let Some(class_sid) = pattern.o.as_sid() {
                if let Some(count) = s.get_class_count(class_sid) {
                    inputs.class_count = Some(count);
                }
            } else if let Some(class_iri) = pattern.o.as_iri() {
                if let Some(count) = s.get_class_count_by_iri(class_iri) {
                    inputs.class_count = Some(count);
                }
            }
        }
    }

    // Determine if and why fallback was used based on pattern type and available stats
    inputs.fallback = match pattern_type {
        PatternType::ExactMatch => None, // No stats needed for exact match
        PatternType::FullScan => Some(FallbackReason::FullScanPattern),
        PatternType::ClassPattern => {
            if inputs.class_count.is_none() {
                Some(FallbackReason::ClassNotInStats)
            } else {
                None
            }
        }
        PatternType::BoundSubject => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else if inputs.ndv_subjects.is_none_or(|n| n == 0) {
                Some(FallbackReason::MissingNdv)
            } else {
                None
            }
        }
        PatternType::BoundObject => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else if inputs.ndv_values.is_none_or(|n| n == 0) {
                Some(FallbackReason::MissingNdv)
            } else {
                None
            }
        }
        PatternType::PropertyScan => {
            if inputs.count.is_none() {
                Some(FallbackReason::NoPropertyStats)
            } else {
                None // PropertyScan just uses count, no NDV needed
            }
        }
    };

    inputs
}

/// Format a pattern as a human-readable string
pub fn format_pattern(pattern: &TriplePattern) -> String {
    format!(
        "{} {} {}",
        format_term(&pattern.s),
        format_term(&pattern.p),
        format_term(&pattern.o)
    )
}

/// Format a term as a human-readable string
fn format_term(term: &Term) -> String {
    match term {
        Term::Var(v) => format!("?v{}", v.0),
        Term::Sid(sid) => format!("<{}:{}>", sid.namespace_code, &sid.name),
        Term::Iri(iri) => format!("<{}>", iri),
        Term::Value(val) => format!("{:?}", val),
    }
}

/// Reorder patterns for explain (reuses planner's algorithm via shared helpers)
fn reorder_for_explain(patterns: Vec<TriplePattern>, stats: Option<&StatsView>) -> Vec<TriplePattern> {
    if patterns.len() <= 1 {
        return patterns;
    }

    // Clojure parity: if *all* patterns are using fallback scoring (no relevant stats),
    // don't reorder (optimization would be arbitrary/noisy).
    let all_fallback = patterns.iter().all(|p| {
        let ty = classify_pattern(p);
        let inputs = capture_selectivity_inputs(p, ty, stats);
        inputs.fallback.is_some()
    });
    if all_fallback {
        return patterns;
    }

    // If all patterns have identical selectivity, don't reorder (no benefit).
    let mut first_score: Option<i64> = None;
    let mut all_equal = true;
    for p in &patterns {
        let s = calculate_selectivity(p, stats);
        match first_score {
            None => first_score = Some(s),
            Some(fs) if fs == s => {}
            Some(_) => all_equal = false,
        }
    }
    if all_equal {
        return patterns;
    }

    let mut remaining: Vec<_> = patterns.into_iter().collect();
    let mut ordered = Vec::with_capacity(remaining.len());
    let mut bound_vars: HashSet<VarId> = HashSet::new();

    while !remaining.is_empty() {
        let has_bound = !bound_vars.is_empty();
        let candidates: Vec<usize> = remaining
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                if !has_bound {
                    true
                } else {
                    shares_variables(p, &bound_vars)
                }
            })
            .map(|(i, _)| i)
            .collect();

        let pool: Vec<usize> = if candidates.is_empty() {
            (0..remaining.len()).collect()
        } else {
            candidates
        };

        let best_idx = pool
            .into_iter()
            .min_by(|&i, &j| {
                let score_i = calculate_selectivity(&remaining[i], stats);
                let score_j = calculate_selectivity(&remaining[j], stats);
                match score_i.cmp(&score_j) {
                    std::cmp::Ordering::Equal => {
                        compare_patterns_tiebreaker(&remaining[i], &remaining[j])
                    }
                    other => other,
                }
            })
            .unwrap();

        let chosen = remaining.remove(best_idx);
        for var in chosen.variables() {
            bound_vars.insert(var);
        }
        ordered.push(chosen);
    }

    ordered
}

impl fmt::Display for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Query Optimization Explain ===")?;
        writeln!(f)?;
        writeln!(
            f,
            "Statistics available: {}",
            if self.statistics_available {
                "yes"
            } else {
                "no (using fallback estimates)"
            }
        )?;
        writeln!(
            f,
            "Optimization: {}",
            match self.optimization {
                OptimizationStatus::Reordered => "patterns reordered",
                OptimizationStatus::Unchanged => "order unchanged",
            }
        )?;
        writeln!(f)?;

        writeln!(f, "--- Original Pattern Order ---")?;
        for (i, p) in self.original_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }
        writeln!(f)?;

        writeln!(f, "--- Optimized Pattern Order ---")?;
        for (i, p) in self.optimized_patterns.iter().enumerate() {
            writeln!(f, "  [{}] {}", i + 1, p)?;
        }

        Ok(())
    }
}

impl fmt::Display for PatternDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} | type={:?} score={}",
            self.pattern, self.pattern_type, self.selectivity_score
        )?;

        if let Some(reason) = &self.inputs.fallback {
            let reason_str = match reason {
                FallbackReason::NoPropertyStats => "no-property-stats",
                FallbackReason::MissingNdv => "missing-ndv",
                FallbackReason::FullScanPattern => "full-scan",
                FallbackReason::ClassNotInStats => "class-not-in-stats",
            };
            write!(f, " (fallback: {})", reason_str)?;
        }

        if let Some(ref prop) = self.inputs.property_sid {
            write!(f, " prop={}", prop)?;
        }

        if let Some(count) = self.inputs.count {
            write!(f, " count={}", count)?;
        }

        if let Some(ndv) = self.inputs.ndv_values {
            write!(f, " ndv_val={}", ndv)?;
        }

        if let Some(ndv) = self.inputs.ndv_subjects {
            write!(f, " ndv_subj={}", ndv)?;
        }

        if let Some(cc) = self.inputs.class_count {
            write!(f, " class_count={}", cc)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn make_pattern(s: VarId, pred_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(
            Term::Var(s),
            Term::Sid(Sid::new(100, pred_name)),
            Term::Var(o),
        )
    }

    fn make_bound_subject_pattern(s_sid: Sid, pred_name: &str, o: VarId) -> TriplePattern {
        TriplePattern::new(
            Term::Sid(s_sid),
            Term::Sid(Sid::new(100, pred_name)),
            Term::Var(o),
        )
    }

    #[test]
    fn test_explain_single_pattern() {
        let patterns = vec![make_pattern(VarId(0), "name", VarId(1))];
        let explain = explain_patterns(&patterns, None);

        assert_eq!(explain.optimization, OptimizationStatus::Unchanged);
        assert!(!explain.statistics_available);
        assert_eq!(explain.original_patterns.len(), 1);
        assert_eq!(explain.optimized_patterns.len(), 1);
    }

    #[test]
    fn test_explain_reordering() {
        // Pattern with higher selectivity should be reordered to come first
        // p1: ?s :name ?name (PropertyScan, score=1000)
        // p2: ex:person1 :age ?age (BoundSubject, score=10)
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_bound_subject_pattern(Sid::new(50, "person1"), "age", VarId(2));

        let patterns = vec![p1, p2];
        let explain = explain_patterns(&patterns, None);

        // Clojure parity: when *all* patterns are using fallback scoring (no relevant stats),
        // we don't reorder to avoid noisy/unstable explain output.
        assert_eq!(explain.optimization, OptimizationStatus::Unchanged);
    }

    #[test]
    fn test_explain_with_stats() {
        use fluree_db_core::PropertyStatData;
        use std::collections::HashMap;

        let mut properties = HashMap::new();
        properties.insert(
            Sid::new(100, "name"),
            PropertyStatData {
                count: 5000,
                ndv_values: 4500,
                ndv_subjects: 5000,
            },
        );
        properties.insert(
            Sid::new(100, "age"),
            PropertyStatData {
                count: 100,
                ndv_values: 80,
                ndv_subjects: 100,
            },
        );

        let stats = StatsView {
            properties,
            classes: HashMap::new(),
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
        };

        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "age", VarId(2));

        let patterns = vec![p1, p2];
        let explain = explain_patterns(&patterns, Some(&stats));

        assert!(explain.statistics_available);

        // age has lower count (100) vs name (5000), so age should come first
        assert_eq!(explain.optimized_patterns[0].selectivity_score, 100);
        assert_eq!(explain.optimized_patterns[1].selectivity_score, 5000);
    }

    #[test]
    fn test_explain_with_class_stats_only() {
        use std::collections::HashMap;

        // Only class stats, no property stats
        let mut classes = HashMap::new();
        classes.insert(Sid::new(200, "Person"), 500);

        let stats = StatsView {
            properties: HashMap::new(),
            classes,
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
        };

        let patterns = vec![make_pattern(VarId(0), "name", VarId(1))];
        let explain = explain_patterns(&patterns, Some(&stats));

        // Should report statistics_available=true because class stats exist
        assert!(explain.statistics_available);
    }

    #[test]
    fn test_explain_display() {
        let patterns = vec![
            make_pattern(VarId(0), "name", VarId(1)),
            make_pattern(VarId(0), "age", VarId(2)),
        ];
        let explain = explain_patterns(&patterns, None);

        let output = format!("{}", explain);
        assert!(output.contains("Query Optimization Explain"));
        assert!(output.contains("Statistics available: no"));
        assert!(output.contains("Original Pattern Order"));
        assert!(output.contains("Optimized Pattern Order"));
    }

    #[test]
    fn test_pattern_type_classification() {
        // PropertyScan: ?s :p ?o
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        assert_eq!(classify_pattern(&p1), PatternType::PropertyScan);

        // BoundSubject: s :p ?o
        let p2 = make_bound_subject_pattern(Sid::new(50, "person1"), "name", VarId(1));
        assert_eq!(classify_pattern(&p2), PatternType::BoundSubject);

        // BoundObject: ?s :p o
        let p3 = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "name")),
            Term::Value(fluree_db_core::FlakeValue::String("Alice".into())),
        );
        assert_eq!(classify_pattern(&p3), PatternType::BoundObject);

        // ExactMatch: s :p o
        let p4 = TriplePattern::new(
            Term::Sid(Sid::new(50, "person1")),
            Term::Sid(Sid::new(100, "name")),
            Term::Value(fluree_db_core::FlakeValue::String("Alice".into())),
        );
        assert_eq!(classify_pattern(&p4), PatternType::ExactMatch);
    }

    #[test]
    fn test_selectivity_inputs_captured() {
        use fluree_db_core::PropertyStatData;
        use std::collections::HashMap;

        let mut properties = HashMap::new();
        properties.insert(
            Sid::new(100, "name"),
            PropertyStatData {
                count: 5000,
                ndv_values: 4500,
                ndv_subjects: 5000,
            },
        );

        let stats = StatsView {
            properties,
            classes: HashMap::new(),
            properties_by_iri: HashMap::new(),
            classes_by_iri: HashMap::new(),
        };

        let pattern = make_pattern(VarId(0), "name", VarId(1));
        let display = build_pattern_display(&pattern, Some(&stats));

        assert_eq!(display.selectivity_score, 5000); // PropertyScan uses count
        assert_eq!(display.inputs.property_sid, Some("100:name".to_string()));
        assert_eq!(display.inputs.count, Some(5000));
        assert_eq!(display.inputs.ndv_values, Some(4500));
        assert_eq!(display.inputs.ndv_subjects, Some(5000));
        assert!(display.inputs.fallback.is_none()); // Full stats available, no fallback
    }

    #[test]
    fn test_structural_equality_for_order_detection() {
        // Test that order change detection uses structural equality
        let p1 = make_pattern(VarId(0), "name", VarId(1));
        let p2 = make_pattern(VarId(0), "name", VarId(1));

        // Same pattern should be equal
        assert_eq!(p1, p2);

        let p3 = make_pattern(VarId(0), "age", VarId(2));
        // Different pattern should not be equal
        assert_ne!(p1, p3);
    }
}
