//! Arithmetic operator implementations
//!
//! Implements arithmetic operators: Add, Sub, Mul, Div, Negate

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{ArithmeticOp, Expression};
use fluree_db_core::Storage;

use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate addition
pub fn eval_add<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_arithmetic(args, row, ctx, ArithmeticOp::Add, "Add")
}

/// Evaluate subtraction
pub fn eval_sub<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_arithmetic(args, row, ctx, ArithmeticOp::Sub, "Sub")
}

/// Evaluate multiplication
pub fn eval_mul<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_arithmetic(args, row, ctx, ArithmeticOp::Mul, "Mul")
}

/// Evaluate division
pub fn eval_div<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_arithmetic(args, row, ctx, ArithmeticOp::Div, "Div")
}

/// Evaluate unary negation
pub fn eval_negate<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "Negate")?;

    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(-n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(-d))),
        Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(-(*n))))),
        Some(ComparableValue::Decimal(d)) => Ok(Some(ComparableValue::Decimal(Box::new(-(*d))))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "Negate requires a numeric argument".to_string(),
        )),
    }
}

/// Helper for binary arithmetic operations
fn eval_binary_arithmetic<S: Storage>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
    op: ArithmeticOp,
    name: &str,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, name)?;

    let left = args[0].eval_to_comparable(row, ctx)?;
    let right = args[1].eval_to_comparable(row, ctx)?;

    match (left, right) {
        (Some(l), Some(r)) => Ok(Some(op.apply(l, r)?)),
        _ => Ok(None),
    }
}
