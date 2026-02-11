//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use rand::random;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_abs<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ABS")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n.abs()))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.abs()))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "ABS requires a numeric argument".to_string(),
        )),
    }
}

pub fn eval_round<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ROUND")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.round()))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "ROUND requires a numeric argument".to_string(),
        )),
    }
}

pub fn eval_ceil<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "CEIL")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.ceil()))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "CEIL requires a numeric argument".to_string(),
        )),
    }
}

pub fn eval_floor<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "FLOOR")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.floor()))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "FLOOR requires a numeric argument".to_string(),
        )),
    }
}

pub fn eval_rand(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "RAND")?;
    Ok(Some(ComparableValue::Double(random::<f64>())))
}
