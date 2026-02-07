//! Conditional function implementations
//!
//! Implements SPARQL conditional functions: IF, COALESCE

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, FunctionName};
use fluree_db_core::Storage;

use super::eval::{eval_to_comparable, evaluate};
use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a conditional function
pub fn eval_conditional_function<S: Storage>(
    name: &FunctionName,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::If => {
            check_arity(args, 3, "IF")?;
            let cond = evaluate(&args[0], row, ctx)?;
            if cond {
                eval_to_comparable(&args[1], row, ctx)
            } else {
                eval_to_comparable(&args[2], row, ctx)
            }
        }

        FunctionName::Coalesce => {
            for arg in args {
                let val = eval_to_comparable(arg, row, ctx)?;
                if val.is_some() {
                    return Ok(val);
                }
            }
            Ok(None)
        }

        _ => unreachable!(
            "Non-conditional function routed to conditional module: {:?}",
            name
        ),
    }
}
