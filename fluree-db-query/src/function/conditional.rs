//! Conditional function implementations
//!
//! Implements SPARQL conditional functions: IF, COALESCE

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;

use super::eval::evaluate;
use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a conditional function
pub fn eval_conditional_function<S: Storage>(
    name: &Function,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        Function::If => {
            check_arity(args, 3, "IF")?;
            let cond = evaluate(&args[0], row, ctx)?;
            if cond {
                args[1].eval_to_comparable(row, ctx)
            } else {
                args[2].eval_to_comparable(row, ctx)
            }
        }

        Function::Coalesce => {
            for arg in args {
                let val = arg.eval_to_comparable(row, ctx)?;
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
