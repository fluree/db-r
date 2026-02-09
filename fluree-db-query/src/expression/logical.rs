//! Logical operator implementations
//!
//! Implements logical operators: AND, OR, NOT

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use fluree_db_core::Storage;

use super::value::ComparableValue;

/// Evaluate logical AND
///
/// Returns true if all arguments evaluate to true.
/// Short-circuits on first false value.
pub fn eval_and<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    for arg in args {
        if !arg.eval_to_bool(row, ctx)? {
            return Ok(Some(ComparableValue::Bool(false)));
        }
    }
    Ok(Some(ComparableValue::Bool(true)))
}

/// Evaluate logical OR
///
/// Returns true if any argument evaluates to true.
/// Short-circuits on first true value.
pub fn eval_or<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    for arg in args {
        if arg.eval_to_bool(row, ctx)? {
            return Ok(Some(ComparableValue::Bool(true)));
        }
    }
    Ok(Some(ComparableValue::Bool(false)))
}

/// Evaluate logical NOT
///
/// Returns the logical negation of the single argument.
pub fn eval_not<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true))); // NOT of nothing is true
    }
    let result = args[0].eval_to_bool(row, ctx)?;
    Ok(Some(ComparableValue::Bool(!result)))
}

/// Evaluate IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value equals any set value.
pub fn eval_in<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(false)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            let found = args[1..].iter().any(|v| {
                v.eval_to_comparable(row, ctx)
                    .ok()
                    .flatten()
                    .map(|cv| cv == tv)
                    .unwrap_or(false)
            });
            Ok(Some(ComparableValue::Bool(found)))
        }
        None => Ok(Some(ComparableValue::Bool(false))), // Unbound value -> not in list
    }
}

/// Evaluate NOT IN expression
///
/// First argument is the test value, remaining arguments are the set values.
/// Returns true if test value does not equal any set value.
pub fn eval_not_in<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    if args.is_empty() {
        return Ok(Some(ComparableValue::Bool(true)));
    }

    let test_val = args[0].eval_to_comparable(row, ctx)?;
    match test_val {
        Some(tv) => {
            let found = args[1..].iter().any(|v| {
                v.eval_to_comparable(row, ctx)
                    .ok()
                    .flatten()
                    .map(|cv| cv == tv)
                    .unwrap_or(false)
            });
            Ok(Some(ComparableValue::Bool(!found)))
        }
        None => Ok(Some(ComparableValue::Bool(true))), // Unbound value -> not in list (vacuously true)
    }
}
