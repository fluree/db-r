//! Conditional function implementations
//!
//! Implements SPARQL conditional functions: IF, COALESCE

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_if<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 3, "IF")?;
    let cond = args[0].eval_to_bool(row, ctx)?;
    if cond {
        args[1].eval_to_comparable(row, ctx)
    } else {
        args[2].eval_to_comparable(row, ctx)
    }
}

pub fn eval_coalesce<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    for arg in args {
        let val = arg.eval_to_comparable(row, ctx)?;
        if val.is_some() {
            return Ok(val);
        }
    }
    Ok(None)
}
