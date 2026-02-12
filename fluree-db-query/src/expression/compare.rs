//! Value comparison logic
//!
//! This module contains functions for comparing ComparableValues,
//! including numeric, temporal, and string comparisons.
//!
//! Comparison operations are variadic (chained pairwise):
//! - 1 arg → vacuously true
//! - 2+ args → every consecutive pair must satisfy the relation:
//!   `(< a b c)` means `a < b AND b < c`

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use fluree_db_core::FlakeValue;
use std::cmp::Ordering;

use super::helpers::check_min_arity;
use super::value::ComparableValue;

/// Evaluate equality comparison
pub fn eval_eq<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Eq", |ord| ord == Ordering::Equal)
}

/// Evaluate inequality comparison
pub fn eval_ne<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Ne", |ord| ord != Ordering::Equal)
}

/// Evaluate less-than comparison
pub fn eval_lt<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Lt", |ord| ord == Ordering::Less)
}

/// Evaluate less-than-or-equal comparison
pub fn eval_le<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Le", |ord| ord != Ordering::Greater)
}

/// Evaluate greater-than comparison
pub fn eval_gt<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Gt", |ord| ord == Ordering::Greater)
}

/// Evaluate greater-than-or-equal comparison
pub fn eval_ge<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_chained_comparison(args, row, ctx, "Ge", |ord| ord != Ordering::Less)
}

/// Chained pairwise comparison over variadic arguments
///
/// - 1 arg → vacuously true
/// - 2+ args → checks every consecutive pair with the given predicate,
///   short-circuiting on the first failure
fn eval_chained_comparison<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    name: &str,
    predicate: fn(Ordering) -> bool,
) -> Result<Option<ComparableValue>> {
    check_min_arity(args, 1, name)?;

    // 1 arg → vacuously true
    if args.len() == 1 {
        // Still evaluate the arg (may have side effects / unbound check)
        return match args[0].eval_to_comparable(row, ctx)? {
            Some(_) => Ok(Some(ComparableValue::Bool(true))),
            None => Ok(Some(ComparableValue::Bool(false))),
        };
    }

    let mut prev = match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => v,
        None => return Ok(Some(ComparableValue::Bool(false))),
    };

    for arg in &args[1..] {
        let curr = match arg.eval_to_comparable(row, ctx)? {
            Some(v) => v,
            None => return Ok(Some(ComparableValue::Bool(false))),
        };

        let satisfied = cmp_values(&prev, &curr).is_some_and(predicate);
        if !satisfied {
            return Ok(Some(ComparableValue::Bool(false)));
        }

        prev = curr;
    }

    Ok(Some(ComparableValue::Bool(true)))
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

    // =========================================================================
    // Variadic eval tests
    // =========================================================================

    use crate::binding::BindingRow;
    use crate::ir::FilterValue;

    fn long(v: i64) -> Expression {
        Expression::Const(FilterValue::Long(v))
    }

    fn empty_row() -> BindingRow<'static> {
        BindingRow::new(&[], &[])
    }

    #[test]
    fn test_chained_lt_holds() {
        let row = empty_row();
        // 1 < 2 < 3 → true
        let args = vec![long(1), long(2), long(3)];
        let result = eval_lt(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_lt_breaks() {
        let row = empty_row();
        // 1 < 3 < 2 → false (3 < 2 fails)
        let args = vec![long(1), long(3), long(2)];
        let result = eval_lt(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_chained_eq_all_same() {
        let row = empty_row();
        // 5 = 5 = 5 → true
        let args = vec![long(5), long(5), long(5)];
        let result = eval_eq(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_eq_not_all_same() {
        let row = empty_row();
        // 5 = 5 = 6 → false
        let args = vec![long(5), long(5), long(6)];
        let result = eval_eq(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(false)));
    }

    #[test]
    fn test_single_arg_vacuously_true() {
        let row = empty_row();
        let args = vec![long(42)];
        let result = eval_lt(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_zero_args_error() {
        let row = empty_row();
        let args: Vec<Expression> = vec![];
        assert!(eval_lt(&args, &row, None).is_err());
    }

    #[test]
    fn test_chained_ge_holds() {
        let row = empty_row();
        // 5 >= 3 >= 3 → true
        let args = vec![long(5), long(3), long(3)];
        let result = eval_ge(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_chained_ne() {
        let row = empty_row();
        // 1 != 2 != 3 → true (all consecutive pairs differ)
        let args = vec![long(1), long(2), long(3)];
        let result = eval_ne(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
