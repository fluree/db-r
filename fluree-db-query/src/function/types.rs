//! Type-checking function implementations
//!
//! Implements SPARQL type-checking functions: BOUND, isIRI, isLiteral, isNumeric, isBlank

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FunctionName};
use fluree_db_core::Storage;

use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a type-checking function
pub fn eval_type_function<S: Storage>(
    name: &FunctionName,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Bound => {
            check_arity(args, 1, "BOUND")?;
            match &args[0] {
                Expression::Var(var) => Ok(Some(ComparableValue::Bool(!matches!(
                    row.get(*var),
                    Some(Binding::Unbound) | Some(Binding::Poisoned) | None
                )))),
                _ => Err(QueryError::InvalidFilter(
                    "BOUND argument must be a variable".to_string(),
                )),
            }
        }

        FunctionName::IsIri => {
            check_arity(args, 1, "isIRI")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(v, ComparableValue::Sid(_) | ComparableValue::Iri(_))
            }))))
        }

        FunctionName::IsLiteral => {
            check_arity(args, 1, "isLiteral")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(
                    v,
                    ComparableValue::Long(_)
                        | ComparableValue::Double(_)
                        | ComparableValue::String(_)
                        | ComparableValue::Bool(_)
                )
            }))))
        }

        FunctionName::IsNumeric => {
            check_arity(args, 1, "isNumeric")?;
            let val = args[0].eval_to_comparable(row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(v, ComparableValue::Long(_) | ComparableValue::Double(_))
            }))))
        }

        FunctionName::IsBlank => Ok(Some(ComparableValue::Bool(false))),

        _ => unreachable!("Non-type function routed to types module: {:?}", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, Sid};
    use std::sync::Arc;

    fn make_string_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit(
            FlakeValue::String("Hello World".to_string()),
            Sid::new(2, "string"),
        )];
        Batch::new(schema, vec![col]).unwrap()
    }

    #[test]
    fn test_bound() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_type_function::<fluree_db_core::MemoryStorage>(
            &FunctionName::Bound,
            &[Expression::Var(VarId(0))],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
