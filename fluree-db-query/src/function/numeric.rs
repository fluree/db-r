//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;
use rand::random;

use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a numeric function
pub fn eval_numeric_function<S: Storage>(
    name: &Function,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        Function::Abs => {
            check_arity(args, 1, "ABS")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n.abs())),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.abs())),
                _ => None,
            })
        }

        Function::Round => {
            check_arity(args, 1, "ROUND")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.round())),
                _ => None,
            })
        }

        Function::Ceil => {
            check_arity(args, 1, "CEIL")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.ceil())),
                _ => None,
            })
        }

        Function::Floor => {
            check_arity(args, 1, "FLOOR")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.floor())),
                _ => None,
            })
        }

        Function::Rand => {
            check_arity(args, 0, "RAND")?;
            Ok(Some(ComparableValue::Double(random::<f64>())))
        }

        _ => unreachable!("Non-numeric function routed to numeric module: {:?}", name),
    }
}
