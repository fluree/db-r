//! Type-checking function implementations
//!
//! Implements SPARQL type-checking functions: BOUND, isIRI, isLiteral, isNumeric, isBlank

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;

use super::helpers::check_arity;
use super::value::ComparableValue;

impl Function {
    pub(super) fn eval_bound(
        &self,
        args: &[Expression],
        row: &RowView,
    ) -> Result<Option<ComparableValue>> {
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

    pub(super) fn eval_is_iri<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "isIRI")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
            matches!(v, ComparableValue::Sid(_) | ComparableValue::Iri(_))
        }))))
    }

    pub(super) fn eval_is_literal<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
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

    pub(super) fn eval_is_numeric<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "isNumeric")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
            matches!(v, ComparableValue::Long(_) | ComparableValue::Double(_))
        }))))
    }

    pub(super) fn eval_is_blank(&self) -> Result<Option<ComparableValue>> {
        Ok(Some(ComparableValue::Bool(false)))
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
        let result = Function::Bound
            .eval_bound(&[Expression::Var(VarId(0))], &row)
            .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
