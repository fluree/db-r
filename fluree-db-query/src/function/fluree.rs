//! Fluree-specific function implementations
//!
//! Implements Fluree-specific functions: T (transaction time), OP (operation type)

use crate::binding::{Binding, RowView};
use crate::error::Result;
use crate::ir::{Expression, FunctionName};
use std::sync::Arc;

use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a Fluree-specific function
pub fn eval_fluree_function(
    name: &FunctionName,
    args: &[Expression],
    row: &RowView,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::T => {
            check_arity(args, 1, "T")?;
            if let Expression::Var(var_id) = &args[0] {
                if let Some(binding) = row.get(*var_id) {
                    match binding {
                        Binding::Lit { t: Some(t), .. } => {
                            return Ok(Some(ComparableValue::Long(*t)));
                        }
                        // Late-materialized binary bindings still carry `t` directly.
                        Binding::EncodedLit { t, .. } => {
                            return Ok(Some(ComparableValue::Long(*t)));
                        }
                        _ => {}
                    }
                }
            }
            Ok(None)
        }

        FunctionName::Op => {
            check_arity(args, 1, "OP")?;
            if let Expression::Var(var_id) = &args[0] {
                if let Some(Binding::Lit { op: Some(op), .. }) = row.get(*var_id) {
                    let op_str = if *op { "assert" } else { "retract" };
                    return Ok(Some(ComparableValue::String(Arc::from(op_str))));
                }
            }
            Ok(None)
        }

        _ => unreachable!("Non-Fluree function routed to fluree module: {:?}", name),
    }
}
