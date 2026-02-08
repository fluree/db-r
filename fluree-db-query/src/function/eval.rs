//! Core filter expression evaluation
//!
//! This module provides the main evaluation functions:
//! - `evaluate()` - evaluate to boolean
//! - `Expression::eval_to_binding*()` - evaluate to Binding for BIND operator
//! - `Expression::eval_to_comparable()` - evaluate to ComparableValue

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FilterValue, Function};
use fluree_db_core::Storage;
use std::sync::Arc;

use super::compare::compare_values;
use super::helpers::has_unbound_vars;
use super::value::ComparableValue;

// =============================================================================
// Public API: evaluate (boolean)
// =============================================================================

/// Evaluate a filter expression against a row.
///
/// Returns `true` if the row passes the filter, `false` otherwise.
/// Type mismatches and unbound variables result in `false`.
///
/// The `ctx` parameter provides access to the execution context for resolving
/// `Binding::EncodedLit` values (late materialization). Pass `None` if no
/// context is available (e.g., in tests).
pub fn evaluate<S: Storage>(
    expr: &Expression,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<bool> {
    match expr {
        Expression::Var(var) => Ok(row.get(*var).is_some_and(Into::into)),

        Expression::Const(val) => {
            // Constant as boolean
            match val {
                FilterValue::Bool(b) => Ok(*b),
                _ => Ok(true), // Non-bool constants are truthy
            }
        }

        Expression::Compare { op, left, right } => {
            let left_val = left.eval_to_comparable(row, ctx)?;
            let right_val = right.eval_to_comparable(row, ctx)?;

            match (left_val, right_val) {
                (Some(l), Some(r)) => Ok(compare_values(&l, &r, *op)),
                // Either side unbound/null -> comparison is false
                _ => Ok(false),
            }
        }

        Expression::And(exprs) => {
            for e in exprs {
                if !evaluate(e, row, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        Expression::Or(exprs) => {
            for e in exprs {
                if evaluate(e, row, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        Expression::Not(inner) => Ok(!evaluate(inner, row, ctx)?),

        Expression::Arithmetic { .. } => {
            // Arithmetic as boolean: check if result is truthy (non-zero)
            match expr.eval_to_comparable(row, ctx)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(d != 0.0),
                Some(ComparableValue::Bool(b)) => Ok(b),
                _ => Ok(false),
            }
        }

        Expression::Negate(_) => {
            // Negation as boolean: check if result is truthy (non-zero)
            match expr.eval_to_comparable(row, ctx)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(d != 0.0),
                _ => Ok(false),
            }
        }

        Expression::If {
            condition,
            then_expr,
            else_expr,
        } => {
            let cond = evaluate(condition, row, ctx)?;
            if cond {
                evaluate(then_expr, row, ctx)
            } else {
                evaluate(else_expr, row, ctx)
            }
        }

        Expression::In {
            expr: test_expr,
            values,
            negated,
        } => {
            let test_val = test_expr.eval_to_comparable(row, ctx)?;
            match test_val {
                Some(tv) => {
                    let found = values.iter().any(|v| {
                        v.eval_to_comparable(row, ctx)
                            .ok()
                            .flatten()
                            .map(|cv| cv == tv)
                            .unwrap_or(false)
                    });
                    Ok(if *negated { !found } else { found })
                }
                None => Ok(false), // Unbound value -> not in list
            }
        }

        Expression::Call { func, args } => func.eval_to_bool(args, row, ctx),
    }
}

// =============================================================================
// Value Evaluation: Expression::eval_to_comparable
// =============================================================================

impl Expression {
    /// Evaluate expression to a comparable value.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization). Pass `None` if no
    /// context is available.
    pub fn eval_to_comparable<S: Storage>(
        &self,
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
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
                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Ok(None);
                    };
                    let val = store
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
                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Ok(None);
                    };
                    match store.resolve_subject_iri(*s_id) {
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

            Expression::Compare { .. } => {
                // Comparison result is boolean
                Ok(Some(ComparableValue::Bool(evaluate(self, row, ctx)?)))
            }

            Expression::Arithmetic { op, left, right } => {
                let l = left.eval_to_comparable(row, ctx)?;
                let r = right.eval_to_comparable(row, ctx)?;
                match (l, r) {
                    (Some(lv), Some(rv)) => Ok(Some(op.apply(lv, rv)?)),
                    _ => Ok(None),
                }
            }

            Expression::Negate(inner) => match inner.eval_to_comparable(row, ctx)? {
                Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(-n))),
                Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(-d))),
                Some(ComparableValue::BigInt(n)) => {
                    Ok(Some(ComparableValue::BigInt(Box::new(-(*n)))))
                }
                Some(ComparableValue::Decimal(d)) => {
                    Ok(Some(ComparableValue::Decimal(Box::new(-(*d)))))
                }
                _ => Ok(None),
            },

            Expression::And(_) | Expression::Or(_) | Expression::Not(_) => {
                Ok(Some(ComparableValue::Bool(evaluate(self, row, ctx)?)))
            }

            Expression::If {
                condition,
                then_expr,
                else_expr,
            } => {
                let cond = evaluate(condition, row, ctx)?;
                if cond {
                    then_expr.eval_to_comparable(row, ctx)
                } else {
                    else_expr.eval_to_comparable(row, ctx)
                }
            }

            Expression::In { .. } => {
                // IN expression evaluates to boolean
                Ok(Some(ComparableValue::Bool(evaluate(self, row, ctx)?)))
            }

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
    pub fn eval_to_binding<S: Storage>(
        &self,
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Binding {
        match self.eval_to_binding_strict(row, ctx) {
            Ok(binding) => binding,
            Err(_) => Binding::Unbound,
        }
    }

    /// Evaluate to binding with strict error handling.
    ///
    /// Unlike [`eval_to_binding`], this returns errors rather than converting
    /// them to `Binding::Unbound`.
    pub fn eval_to_binding_strict<S: Storage>(
        &self,
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Binding> {
        let comparable = match self.eval_to_comparable(row, ctx) {
            Ok(Some(val)) => val,
            Ok(None) => {
                if has_unbound_vars(self, row) {
                    return Ok(Binding::Unbound);
                }
                // cosineSimilarity returns None for zero-magnitude vectors
                // (mathematically undefined). Treat as Unbound.
                if matches!(
                    self,
                    Expression::Call { func: Function::CosineSimilarity, .. }
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
    use crate::ir::CompareOp;
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, MemoryStorage, Sid};

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
        let expr = Expression::Compare {
            op: CompareOp::Gt,
            left: Box::new(Expression::Var(VarId(0))),
            right: Box::new(Expression::Const(FilterValue::Long(20))),
        };

        // Row 0: age=25 > 20 → true
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate::<MemoryStorage>(&expr, &row0, None).unwrap());

        // Row 2: age=18 > 20 → false
        let row2 = batch.row_view(2).unwrap();
        assert!(!evaluate::<MemoryStorage>(&expr, &row2, None).unwrap());

        // Row 3: age=Unbound → false
        let row3 = batch.row_view(3).unwrap();
        assert!(!evaluate::<MemoryStorage>(&expr, &row3, None).unwrap());
    }

    #[test]
    fn test_evaluate_and() {
        let batch = make_test_batch();

        // ?age > 20 AND ?age < 28
        let expr = Expression::And(vec![
            Expression::Compare {
                op: CompareOp::Gt,
                left: Box::new(Expression::Var(VarId(0))),
                right: Box::new(Expression::Const(FilterValue::Long(20))),
            },
            Expression::Compare {
                op: CompareOp::Lt,
                left: Box::new(Expression::Var(VarId(0))),
                right: Box::new(Expression::Const(FilterValue::Long(28))),
            },
        ]);

        // Row 0: age=25 → true (25 > 20 AND 25 < 28)
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate::<MemoryStorage>(&expr, &row0, None).unwrap());

        // Row 1: age=30 → false (30 > 20 but 30 < 28 is false)
        let row1 = batch.row_view(1).unwrap();
        assert!(!evaluate::<MemoryStorage>(&expr, &row1, None).unwrap());
    }

    #[test]
    fn test_evaluate_or() {
        let batch = make_test_batch();

        // ?age < 20 OR ?age > 28
        let expr = Expression::Or(vec![
            Expression::Compare {
                op: CompareOp::Lt,
                left: Box::new(Expression::Var(VarId(0))),
                right: Box::new(Expression::Const(FilterValue::Long(20))),
            },
            Expression::Compare {
                op: CompareOp::Gt,
                left: Box::new(Expression::Var(VarId(0))),
                right: Box::new(Expression::Const(FilterValue::Long(28))),
            },
        ]);

        // Row 0: age=25 → false
        let row0 = batch.row_view(0).unwrap();
        assert!(!evaluate::<MemoryStorage>(&expr, &row0, None).unwrap());

        // Row 1: age=30 → true (30 > 28)
        let row1 = batch.row_view(1).unwrap();
        assert!(evaluate::<MemoryStorage>(&expr, &row1, None).unwrap());

        // Row 2: age=18 → true (18 < 20)
        let row2 = batch.row_view(2).unwrap();
        assert!(evaluate::<MemoryStorage>(&expr, &row2, None).unwrap());
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_test_batch();

        // NOT(?age > 25)
        let expr = Expression::Not(Box::new(Expression::Compare {
            op: CompareOp::Gt,
            left: Box::new(Expression::Var(VarId(0))),
            right: Box::new(Expression::Const(FilterValue::Long(25))),
        }));

        // Row 0: age=25 → NOT(25 > 25) = NOT(false) = true
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate::<MemoryStorage>(&expr, &row0, None).unwrap());

        // Row 1: age=30 → NOT(30 > 25) = NOT(true) = false
        let row1 = batch.row_view(1).unwrap();
        assert!(!evaluate::<MemoryStorage>(&expr, &row1, None).unwrap());
    }
}
