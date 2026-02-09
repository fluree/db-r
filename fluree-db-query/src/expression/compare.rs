//! Value comparison logic
//!
//! This module contains functions for comparing ComparableValues,
//! including numeric, temporal, and string comparisons.

use crate::ir::CompareOp;
use fluree_db_core::FlakeValue;
use std::cmp::Ordering;

use super::value::ComparableValue;

/// Compare two values with the given operator
///
/// Delegates to FlakeValue's comparison methods to avoid duplicating logic.
/// Returns `false` for type mismatches (except `!=` which returns `true`).
pub fn compare_values(left: &ComparableValue, right: &ComparableValue, op: CompareOp) -> bool {
    // Convert to FlakeValue and use its comparison methods
    let left_fv: FlakeValue = left.into();
    let right_fv: FlakeValue = right.into();

    // Try numeric comparison first (handles all numeric cross-type comparisons)
    if let Some(ordering) = left_fv.numeric_cmp(&right_fv) {
        return apply_ordering(ordering, op);
    }

    // Try temporal comparison (same-type temporal only)
    if let Some(ordering) = left_fv.temporal_cmp(&right_fv) {
        return apply_ordering(ordering, op);
    }

    // Cross-type coercion: when one side is a temporal type and the other
    // is a string, try to parse the string as that temporal type. This
    // handles values stored as LEX_ID (string dict) with a temporal
    // datatype annotation (e.g. gYear values in bulk-imported data).
    if let Some(ordering) = try_coerce_temporal_string_cmp(&left_fv, &right_fv) {
        return apply_ordering(ordering, op);
    }

    // Fall back to same-type comparisons for non-numeric, non-temporal types
    let ordering = match (left, right) {
        (ComparableValue::String(a), ComparableValue::String(b)) => a.cmp(b),
        (ComparableValue::Bool(a), ComparableValue::Bool(b)) => a.cmp(b),
        (ComparableValue::Sid(a), ComparableValue::Sid(b)) => a.cmp(b),
        // Raw IRIs from VGs compare by string value
        (ComparableValue::Iri(a), ComparableValue::Iri(b)) => a.cmp(b),
        // Type mismatch -> always not equal
        _ => return matches!(op, CompareOp::Ne),
    };

    apply_ordering(ordering, op)
}

/// Apply a comparison operator to an ordering result
#[inline]
pub fn apply_ordering(ordering: Ordering, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    }
}

/// Try to compare a temporal FlakeValue against a String by parsing the
/// string as the matching temporal type. Returns `None` if neither side is
/// a string or parsing fails.
///
/// This handles values stored as LEX_ID (string dict entry) with a temporal
/// datatype annotation — the index stores the raw string but the FILTER
/// constant is a properly-typed temporal value.
pub fn try_coerce_temporal_string_cmp(left: &FlakeValue, right: &FlakeValue) -> Option<Ordering> {
    use fluree_db_core::temporal;

    match (left, right) {
        // String on left, temporal on right → parse left as temporal
        (FlakeValue::String(s), FlakeValue::GYear(g)) => temporal::GYear::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GYearMonth(g)) => temporal::GYearMonth::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GMonth(g)) => temporal::GMonth::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GDay(g)) => temporal::GDay::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::GMonthDay(g)) => temporal::GMonthDay::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(g.as_ref())),
        (FlakeValue::String(s), FlakeValue::DateTime(dt)) => temporal::DateTime::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(dt.as_ref())),
        (FlakeValue::String(s), FlakeValue::Date(d)) => temporal::Date::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(d.as_ref())),
        (FlakeValue::String(s), FlakeValue::Time(t)) => temporal::Time::parse(s)
            .ok()
            .map(|parsed| parsed.cmp(t.as_ref())),
        // Temporal on left, string on right → parse right as temporal
        (FlakeValue::GYear(g), FlakeValue::String(s)) => temporal::GYear::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GYearMonth(g), FlakeValue::String(s)) => temporal::GYearMonth::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GMonth(g), FlakeValue::String(s)) => temporal::GMonth::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GDay(g), FlakeValue::String(s)) => temporal::GDay::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::GMonthDay(g), FlakeValue::String(s)) => temporal::GMonthDay::parse(s)
            .ok()
            .map(|parsed| g.as_ref().cmp(&parsed)),
        (FlakeValue::DateTime(dt), FlakeValue::String(s)) => temporal::DateTime::parse(s)
            .ok()
            .map(|parsed| dt.as_ref().cmp(&parsed)),
        (FlakeValue::Date(d), FlakeValue::String(s)) => temporal::Date::parse(s)
            .ok()
            .map(|parsed| d.as_ref().cmp(&parsed)),
        (FlakeValue::Time(t), FlakeValue::String(s)) => temporal::Time::parse(s)
            .ok()
            .map(|parsed| t.as_ref().cmp(&parsed)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_compare_longs() {
        let a = ComparableValue::Long(10);
        let b = ComparableValue::Long(20);
        assert!(compare_values(&a, &b, CompareOp::Lt));
        assert!(compare_values(&b, &a, CompareOp::Gt));
        assert!(compare_values(&a, &a, CompareOp::Eq));
        assert!(compare_values(&a, &b, CompareOp::Ne));
    }

    #[test]
    fn test_compare_strings() {
        let a = ComparableValue::String(Arc::from("alpha"));
        let b = ComparableValue::String(Arc::from("beta"));
        assert!(compare_values(&a, &b, CompareOp::Lt));
        assert!(compare_values(&a, &a, CompareOp::Eq));
    }

    #[test]
    fn test_type_mismatch() {
        let long = ComparableValue::Long(10);
        let string = ComparableValue::String(Arc::from("10"));
        // Type mismatch: only != is true
        assert!(!compare_values(&long, &string, CompareOp::Eq));
        assert!(compare_values(&long, &string, CompareOp::Ne));
        assert!(!compare_values(&long, &string, CompareOp::Lt));
    }

    #[test]
    fn test_apply_ordering() {
        assert!(apply_ordering(Ordering::Less, CompareOp::Lt));
        assert!(apply_ordering(Ordering::Less, CompareOp::Le));
        assert!(!apply_ordering(Ordering::Less, CompareOp::Eq));
        assert!(apply_ordering(Ordering::Equal, CompareOp::Le));
        assert!(apply_ordering(Ordering::Equal, CompareOp::Ge));
    }
}
