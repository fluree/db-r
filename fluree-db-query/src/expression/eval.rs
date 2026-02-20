//! Core filter expression evaluation
//!
//! This module provides the main evaluation methods on Expression:
//! - `eval_to_bool()` - evaluate to boolean
//! - `eval_to_binding*()` - evaluate to Binding for BIND operator
//! - `eval_to_comparable()` - evaluate to ComparableValue

use crate::binding::{Binding, RowAccess, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FilterValue, Function};
use std::sync::Arc;

use super::helpers::has_unbound_vars;
use super::value::ComparableValue;

impl Expression {
    /// Evaluate a filter expression against a row.
    ///
    /// Returns `true` if the row passes the filter, `false` otherwise.
    /// Type mismatches and unbound variables result in `false`.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization). Pass `None` if no
    /// context is available (e.g., in tests).
    ///
    /// This method is generic over `RowAccess`, allowing it to work with both
    /// `RowView` (batch rows) and `BindingRow` (pre-batch filtering).
    pub fn eval_to_bool<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        match self {
            Expression::Var(var) => Ok(row.get(*var).is_some_and(Into::into)),

            Expression::Const(val) => {
                // Constant as boolean
                match val {
                    FilterValue::Bool(b) => Ok(*b),
                    _ => Ok(true), // Non-bool constants are truthy
                }
            }

            Expression::Call { func, args } => func.eval_to_bool(args, row, ctx),
        }
    }

    /// Evaluate expression to a comparable value.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization). Pass `None` if no
    /// context is available.
    ///
    /// This method is generic over `RowAccess`, allowing it to work with both
    /// `RowView` (batch rows) and `BindingRow` (pre-batch filtering).
    pub fn eval_to_comparable<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Option<ComparableValue>> {
        match self {
            Expression::Var(var) => match row.get(*var) {
                Some(Binding::Lit { val, .. }) => Ok(ComparableValue::try_from(val).ok()),
                Some(Binding::EncodedLit {
                    o_kind,
                    o_key,
                    p_id,
                    ..
                }) => {
                    let Some(gv) = ctx.and_then(|c| c.graph_view()) else {
                        return Ok(None);
                    };
                    let val = gv
                        .decode_value(*o_kind, *o_key, *p_id)
                        .map_err(|e| QueryError::Internal(format!("decode_value: {}", e)))?;
                    Ok(ComparableValue::try_from(&val).ok())
                }
                Some(Binding::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid.clone()))),
                Some(Binding::IriMatch { iri, .. }) => {
                    Ok(Some(ComparableValue::Iri(Arc::clone(iri))))
                }
                Some(Binding::Iri(iri)) => Ok(Some(ComparableValue::Iri(Arc::clone(iri)))),
                Some(Binding::EncodedSid { s_id }) => {
                    let Some(gv) = ctx.and_then(|c| c.graph_view()) else {
                        return Ok(None);
                    };
                    match gv.store().resolve_subject_iri(*s_id) {
                        Ok(iri) => Ok(Some(ComparableValue::Iri(Arc::from(iri)))),
                        Err(e) => Err(QueryError::Internal(format!("resolve_subject_iri: {}", e))),
                    }
                }
                Some(Binding::EncodedPid { p_id }) => {
                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Ok(None);
                    };
                    match store.resolve_predicate_iri(*p_id) {
                        Some(iri) => Ok(Some(ComparableValue::Iri(Arc::from(iri)))),
                        None => Err(QueryError::Internal(format!(
                            "resolve_predicate_iri: unknown p_id {}",
                            p_id
                        ))),
                    }
                }
                Some(Binding::Unbound) | Some(Binding::Poisoned) | None => Ok(None),
                Some(Binding::Grouped(_)) => {
                    debug_assert!(false, "Grouped binding in filter evaluation");
                    Ok(None)
                }
            },

            Expression::Const(val) => Ok(Some(val.into())),

            Expression::Call { func, args } => func.eval(args, row, ctx),
        }
    }

    /// Evaluate expression and return a Binding value.
    ///
    /// This is used by BIND operator to compute values for binding to variables.
    /// Returns `Binding::Unbound` on evaluation errors (type mismatches, unbound vars, etc.)
    /// rather than `Binding::Poisoned` - Poisoned is reserved for OPTIONAL semantics.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization).
    pub fn eval_to_binding(&self, row: &RowView, ctx: Option<&ExecutionContext<'_>>) -> Binding {
        match self.try_eval_to_binding(row, ctx) {
            Ok(binding) => binding,
            Err(_) => Binding::Unbound,
        }
    }

    /// Evaluate to binding with strict error handling.
    ///
    /// Unlike [`eval_to_binding`], this returns errors rather than converting
    /// them to `Binding::Unbound`.
    pub fn try_eval_to_binding(
        &self,
        row: &RowView,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Binding> {
        let comparable = match self.eval_to_comparable(row, ctx) {
            Ok(Some(val)) => val,
            Ok(None) => {
                if has_unbound_vars(self, row) {
                    return Ok(Binding::Unbound);
                }
                // Vector functions return None for type mismatches or
                // mathematically undefined cases. Treat as Unbound.
                if matches!(
                    self,
                    Expression::Call {
                        func: Function::CosineSimilarity
                            | Function::DotProduct
                            | Function::EuclideanDistance,
                        ..
                    }
                ) {
                    return Ok(Binding::Unbound);
                }
                return Err(QueryError::InvalidFilter(format!(
                    "bind evaluation failed for expression: {:?}",
                    self
                )));
            }
            Err(err) => return Err(err),
        };
        comparable.to_binding(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_test_batch() -> Batch {
        let schema: Arc<[crate::var_registry::VarId]> =
            Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());

        let age_col = vec![
            Binding::lit(FlakeValue::Long(25), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(30), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(18), Sid::new(2, "long")),
            Binding::Unbound,
        ];

        let name_col = vec![
            Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            ),
            Binding::lit(FlakeValue::String("Bob".to_string()), Sid::new(2, "string")),
            Binding::lit(
                FlakeValue::String("Carol".to_string()),
                Sid::new(2, "string"),
            ),
            Binding::lit(
                FlakeValue::String("Dave".to_string()),
                Sid::new(2, "string"),
            ),
        ];

        Batch::new(schema, vec![age_col, name_col]).unwrap()
    }

    #[test]
    fn test_evaluate_comparison_gt() {
        let batch = make_test_batch();

        // ?age > 20
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(20)),
        );

        // Row 0: age=25 > 20 → true
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 2: age=18 > 20 → false
        let row2 = batch.row_view(2).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row2, None).unwrap());

        // Row 3: age=Unbound → false
        let row3 = batch.row_view(3).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row3, None).unwrap());
    }

    #[test]
    fn test_evaluate_and() {
        let batch = make_test_batch();

        // ?age > 20 AND ?age < 28
        let expr = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(20)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(28)),
            ),
        ]);

        // Row 0: age=25 → true (25 > 20 AND 25 < 28)
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → false (30 > 20 but 30 < 28 is false)
        let row1 = batch.row_view(1).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row1, None).unwrap());
    }

    #[test]
    fn test_evaluate_or() {
        let batch = make_test_batch();

        // ?age < 20 OR ?age > 28
        let expr = Expression::or(vec![
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(20)),
            ),
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(28)),
            ),
        ]);

        // Row 0: age=25 → false
        let row0 = batch.row_view(0).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → true (30 > 28)
        let row1 = batch.row_view(1).unwrap();
        assert!(expr.eval_to_bool::<_>(&row1, None).unwrap());

        // Row 2: age=18 → true (18 < 20)
        let row2 = batch.row_view(2).unwrap();
        assert!(expr.eval_to_bool::<_>(&row2, None).unwrap());
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_test_batch();

        // NOT(?age > 25)
        let expr = Expression::not(Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(25)),
        ));

        // Row 0: age=25 → NOT(25 > 25) = NOT(false) = true
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → NOT(30 > 25) = NOT(true) = false
        let row1 = batch.row_view(1).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row1, None).unwrap());
    }
}
