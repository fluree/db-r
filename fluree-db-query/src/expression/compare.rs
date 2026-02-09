//! Value comparison logic
//!
//! This module contains functions for comparing ComparableValues,
//! including numeric, temporal, and string comparisons.

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use fluree_db_core::{FlakeValue, Storage};
use std::cmp::Ordering;

use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate equality comparison
pub fn eval_eq<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Eq")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        (Some(l), Some(r)) => cmp_values(&l, &r).is_some_and(|o| o == Ordering::Equal),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Evaluate inequality comparison
pub fn eval_ne<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Ne")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        // Type mismatch (None) means not equal, so true for Ne
        (Some(l), Some(r)) => cmp_values(&l, &r) != Some(Ordering::Equal),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Evaluate less-than comparison
pub fn eval_lt<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Lt")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        (Some(l), Some(r)) => cmp_values(&l, &r).is_some_and(|o| o == Ordering::Less),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Evaluate less-than-or-equal comparison
pub fn eval_le<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Le")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        (Some(l), Some(r)) => cmp_values(&l, &r).is_some_and(|o| o != Ordering::Greater),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Evaluate greater-than comparison
pub fn eval_gt<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Gt")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        (Some(l), Some(r)) => cmp_values(&l, &r).is_some_and(|o| o == Ordering::Greater),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Evaluate greater-than-or-equal comparison
pub fn eval_ge<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "Ge")?;
    let result = match (
        args[0].eval_to_comparable(row, ctx)?,
        args[1].eval_to_comparable(row, ctx)?,
    ) {
        (Some(l), Some(r)) => cmp_values(&l, &r).is_some_and(|o| o != Ordering::Less),
        _ => false,
    };
    Ok(Some(ComparableValue::Bool(result)))
}

/// Compare two values and return their ordering.
///
/// Returns `None` for type mismatches (incomparable types).
/// Delegates to FlakeValue's comparison methods for numeric and temporal types.
fn cmp_values(left: &ComparableValue, right: &ComparableValue) -> Option<Ordering> {
    let left_fv: FlakeValue = left.into();
    let right_fv: FlakeValue = right.into();

    // Try numeric comparison first (handles all numeric cross-type comparisons)
    if let Some(ordering) = left_fv.numeric_cmp(&right_fv) {
        return Some(ordering);
    }

    // Try temporal comparison (same-type temporal only)
    if let Some(ordering) = left_fv.temporal_cmp(&right_fv) {
        return Some(ordering);
    }

    // Cross-type coercion: when one side is a temporal type and the other
    // is a string, try to parse the string as that temporal type.
    if let Some(ordering) = try_coerce_temporal_string_cmp(&left_fv, &right_fv) {
        return Some(ordering);
    }

    // Fall back to same-type comparisons for non-numeric, non-temporal types
    match (left, right) {
        (ComparableValue::String(a), ComparableValue::String(b)) => Some(a.cmp(b)),
        (ComparableValue::Bool(a), ComparableValue::Bool(b)) => Some(a.cmp(b)),
        (ComparableValue::Sid(a), ComparableValue::Sid(b)) => Some(a.cmp(b)),
        (ComparableValue::Iri(a), ComparableValue::Iri(b)) => Some(a.cmp(b)),
        // Type mismatch
        _ => None,
    }
}

/// Try to compare a temporal FlakeValue against a String by parsing the
/// string as the matching temporal type. Returns `None` if neither side is
/// a string or parsing fails.
///
/// This handles values stored as LEX_ID (string dict entry) with a temporal
/// datatype annotation — the index stores the raw string but the FILTER
/// constant is a properly-typed temporal value.
fn try_coerce_temporal_string_cmp(left: &FlakeValue, right: &FlakeValue) -> Option<Ordering> {
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
    fn test_cmp_longs() {
        let a = ComparableValue::Long(10);
        let b = ComparableValue::Long(20);
        assert_eq!(cmp_values(&a, &b), Some(Ordering::Less));
        assert_eq!(cmp_values(&b, &a), Some(Ordering::Greater));
        assert_eq!(cmp_values(&a, &a), Some(Ordering::Equal));
    }

    #[test]
    fn test_cmp_strings() {
        let a = ComparableValue::String(Arc::from("alpha"));
        let b = ComparableValue::String(Arc::from("beta"));
        assert_eq!(cmp_values(&a, &b), Some(Ordering::Less));
        assert_eq!(cmp_values(&a, &a), Some(Ordering::Equal));
    }

    #[test]
    fn test_type_mismatch() {
        let long = ComparableValue::Long(10);
        let string = ComparableValue::String(Arc::from("10"));
        // Type mismatch returns None
        assert_eq!(cmp_values(&long, &string), None);
    }
}
