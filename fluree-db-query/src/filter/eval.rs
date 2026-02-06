//! Core filter expression evaluation
//!
//! This module provides the main evaluation functions:
//! - `evaluate()` / `evaluate_with_context()` - evaluate to boolean
//! - `evaluate_to_binding*()` - evaluate to Binding for BIND operator
//! - `eval_to_comparable*()` - evaluate to ComparableValue

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{FilterExpr, FilterValue};
use fluree_db_core::{FlakeValue, Storage};
use std::sync::Arc;

use super::compare::compare_values;
use super::functions::{eval_function, eval_function_to_bool};
use super::helpers::{has_unbound_vars, WELL_KNOWN_DATATYPES};
use super::value::{
    eval_arithmetic, filter_value_to_comparable, flake_value_to_comparable, ComparableValue,
};

// =============================================================================
// Public API: evaluate_to_binding
// =============================================================================

/// Evaluate a filter expression and return a Binding value
///
/// This is used by BIND operator to compute values for binding to variables.
/// Returns `Binding::Unbound` on evaluation errors (type mismatches, unbound vars, etc.)
/// rather than `Binding::Poisoned` - Poisoned is reserved for OPTIONAL semantics.
pub fn evaluate_to_binding(expr: &FilterExpr, row: &RowView) -> Binding {
    evaluate_to_binding_with_context::<fluree_db_core::MemoryStorage>(expr, row, None)
}

/// Evaluate to binding with execution context (for EncodedLit support)
pub fn evaluate_to_binding_with_context<S: Storage>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Binding {
    match evaluate_to_binding_with_context_strict(expr, row, ctx) {
        Ok(binding) => binding,
        Err(_) => Binding::Unbound,
    }
}

/// Evaluate to binding with strict error handling
pub fn evaluate_to_binding_with_context_strict<S: Storage>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Binding> {
    // Evaluate to comparable value
    let comparable = match eval_to_comparable_inner(expr, row, ctx) {
        Ok(Some(val)) => val,
        Ok(None) => {
            if has_unbound_vars(expr, row) {
                return Ok(Binding::Unbound);
            }
            return Err(QueryError::InvalidFilter(format!(
                "bind evaluation failed for expression: {:?}",
                expr
            )));
        }
        Err(err) => return Err(err),
    };

    // Convert ComparableValue to Binding
    let datatypes = &*WELL_KNOWN_DATATYPES;
    match comparable {
        ComparableValue::Long(n) => Ok(Binding::lit(
            FlakeValue::Long(n),
            datatypes.xsd_long.clone(),
        )),
        ComparableValue::Double(d) => Ok(Binding::lit(
            FlakeValue::Double(d),
            datatypes.xsd_double.clone(),
        )),
        ComparableValue::String(s) => Ok(Binding::lit(
            FlakeValue::String(s.to_string()),
            datatypes.xsd_string.clone(),
        )),
        ComparableValue::Bool(b) => Ok(Binding::lit(
            FlakeValue::Boolean(b),
            datatypes.xsd_boolean.clone(),
        )),
        ComparableValue::Sid(sid) => Ok(Binding::Sid(sid)),
        ComparableValue::Vector(v) => Ok(Binding::lit(
            FlakeValue::Vector(v.to_vec()),
            datatypes.fluree_vector.clone(),
        )),
        ComparableValue::BigInt(n) => Ok(Binding::lit(
            FlakeValue::BigInt(n),
            datatypes.xsd_integer.clone(),
        )),
        ComparableValue::Decimal(d) => Ok(Binding::lit(
            FlakeValue::Decimal(d),
            datatypes.xsd_decimal.clone(),
        )),
        ComparableValue::DateTime(dt) => Ok(Binding::lit(
            FlakeValue::DateTime(dt),
            datatypes.xsd_datetime.clone(),
        )),
        ComparableValue::Date(d) => Ok(Binding::lit(
            FlakeValue::Date(d),
            datatypes.xsd_date.clone(),
        )),
        ComparableValue::Time(t) => Ok(Binding::lit(
            FlakeValue::Time(t),
            datatypes.xsd_time.clone(),
        )),
        ComparableValue::GeoPoint(bits) => Ok(Binding::lit(
            FlakeValue::GeoPoint(bits),
            datatypes.geo_wkt_literal.clone(),
        )),
        ComparableValue::Iri(iri) => {
            let Some(ctx) = ctx else {
                return Err(QueryError::InvalidFilter(
                    "bind evaluation requires database context for iri()".to_string(),
                ));
            };
            match ctx.db.encode_iri(&iri) {
                Some(sid) => Ok(Binding::Sid(sid)),
                None => Err(QueryError::InvalidFilter(format!(
                    "Unknown IRI or namespace: {}",
                    iri
                ))),
            }
        }
        ComparableValue::TypedLiteral { val, dt_iri, lang } => {
            let Some(ctx) = ctx else {
                return Err(QueryError::InvalidFilter(
                    "bind evaluation requires database context for str-dt/str-lang".to_string(),
                ));
            };
            Ok(if let Some(lang) = lang {
                let dt = fluree_db_core::Sid::new(3, "langString");
                Binding::lit_lang(val, dt, lang)
            } else if let Some(dt_iri) = dt_iri {
                match ctx.db.encode_iri(&dt_iri) {
                    Some(dt) => Binding::lit(val, dt),
                    None => {
                        return Err(QueryError::InvalidFilter(format!(
                            "Unknown datatype IRI: {}",
                            dt_iri
                        )));
                    }
                }
            } else {
                Binding::lit(val, datatypes.xsd_string.clone())
            })
        }
    }
}

// =============================================================================
// Public API: evaluate (boolean)
// =============================================================================

/// Evaluate a filter expression against a row
///
/// Returns `true` if the row passes the filter, `false` otherwise.
/// Type mismatches and unbound variables result in `false`.
pub fn evaluate(expr: &FilterExpr, row: &RowView) -> Result<bool> {
    evaluate_inner::<fluree_db_core::MemoryStorage>(expr, row, None)
}

/// Evaluate a filter expression with access to execution context.
///
/// This is required for correct evaluation when a row contains
/// `Binding::EncodedLit` (late materialization), e.g. for `REGEX`, `STR`,
/// and comparisons.
pub fn evaluate_with_context<S: Storage>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: &ExecutionContext<'_, S>,
) -> Result<bool> {
    evaluate_inner(expr, row, Some(ctx))
}

/// Core boolean evaluation (internal)
pub(crate) fn evaluate_inner<S: Storage>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<bool> {
    match expr {
        FilterExpr::Var(var) => {
            // Var as boolean: check if bound and truthy
            match row.get(*var) {
                Some(Binding::Lit { val, .. }) => match val {
                    FlakeValue::Boolean(b) => Ok(*b),
                    _ => Ok(true), // Non-bool literal is truthy
                },
                Some(Binding::EncodedLit { .. }) => Ok(true), // Encoded literal is truthy
                Some(Binding::Sid(_)) => Ok(true),
                Some(Binding::IriMatch { .. }) => Ok(true),
                Some(Binding::Iri(_)) => Ok(true),
                Some(Binding::EncodedSid { .. }) => Ok(true),
                Some(Binding::EncodedPid { .. }) => Ok(true),
                Some(Binding::Unbound) | Some(Binding::Poisoned) | None => Ok(false),
                Some(Binding::Grouped(_)) => {
                    debug_assert!(false, "Grouped binding in filter evaluation");
                    Ok(false)
                }
            }
        }

        FilterExpr::Const(val) => {
            // Constant as boolean
            match val {
                FilterValue::Bool(b) => Ok(*b),
                _ => Ok(true), // Non-bool constants are truthy
            }
        }

        FilterExpr::Compare { op, left, right } => {
            let left_val = eval_to_comparable_inner(left, row, ctx)?;
            let right_val = eval_to_comparable_inner(right, row, ctx)?;

            match (left_val, right_val) {
                (Some(l), Some(r)) => Ok(compare_values(&l, &r, *op)),
                // Either side unbound/null -> comparison is false
                _ => Ok(false),
            }
        }

        FilterExpr::And(exprs) => {
            for e in exprs {
                if !evaluate_inner(e, row, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        FilterExpr::Or(exprs) => {
            for e in exprs {
                if evaluate_inner(e, row, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        FilterExpr::Not(inner) => Ok(!evaluate_inner(inner, row, ctx)?),

        FilterExpr::Arithmetic { .. } => {
            // Arithmetic as boolean: check if result is truthy (non-zero)
            match eval_to_comparable_inner(expr, row, ctx)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(d != 0.0),
                Some(ComparableValue::Bool(b)) => Ok(b),
                _ => Ok(false),
            }
        }

        FilterExpr::Negate(_) => {
            // Negation as boolean: check if result is truthy (non-zero)
            match eval_to_comparable_inner(expr, row, ctx)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(d != 0.0),
                _ => Ok(false),
            }
        }

        FilterExpr::If {
            condition,
            then_expr,
            else_expr,
        } => {
            let cond = evaluate_inner(condition, row, ctx)?;
            if cond {
                evaluate_inner(then_expr, row, ctx)
            } else {
                evaluate_inner(else_expr, row, ctx)
            }
        }

        FilterExpr::In {
            expr: test_expr,
            values,
            negated,
        } => {
            let test_val = eval_to_comparable_inner(test_expr, row, ctx)?;
            match test_val {
                Some(tv) => {
                    let found = values.iter().any(|v| {
                        eval_to_comparable_inner(v, row, ctx)
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

        FilterExpr::Function { name, args } => eval_function_to_bool(name, args, row, ctx),
    }
}

// =============================================================================
// Value Evaluation: eval_to_comparable
// =============================================================================

/// Evaluate expression to a comparable value (contextless).
///
/// Prefer [`eval_to_comparable_inner`] when `Binding::EncodedLit` may appear.
pub fn eval_to_comparable(expr: &FilterExpr, row: &RowView) -> Result<Option<ComparableValue>> {
    eval_to_comparable_inner::<fluree_db_core::MemoryStorage>(expr, row, None)
}

/// Evaluate expression to a comparable value with context (for EncodedLit support)
pub fn eval_to_comparable_inner<S: Storage>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match expr {
        FilterExpr::Var(var) => match row.get(*var) {
            Some(Binding::Lit { val, .. }) => Ok(flake_value_to_comparable(val)),
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
                Ok(flake_value_to_comparable(&val))
            }
            Some(Binding::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid.clone()))),
            Some(Binding::IriMatch { iri, .. }) => Ok(Some(ComparableValue::Iri(Arc::clone(iri)))),
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

        FilterExpr::Const(val) => Ok(Some(filter_value_to_comparable(val))),

        FilterExpr::Compare { .. } => {
            // Comparison result is boolean
            Ok(Some(ComparableValue::Bool(evaluate_inner(expr, row, ctx)?)))
        }

        FilterExpr::Arithmetic { op, left, right } => {
            let l = eval_to_comparable_inner(left, row, ctx)?;
            let r = eval_to_comparable_inner(right, row, ctx)?;
            match (l, r) {
                (Some(lv), Some(rv)) => Ok(eval_arithmetic(*op, lv, rv)),
                _ => Ok(None),
            }
        }

        FilterExpr::Negate(inner) => match eval_to_comparable_inner(inner, row, ctx)? {
            Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(-n))),
            Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(-d))),
            Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(-(*n))))),
            Some(ComparableValue::Decimal(d)) => {
                Ok(Some(ComparableValue::Decimal(Box::new(-(*d)))))
            }
            _ => Ok(None),
        },

        FilterExpr::And(_) | FilterExpr::Or(_) | FilterExpr::Not(_) => {
            Ok(Some(ComparableValue::Bool(evaluate_inner(expr, row, ctx)?)))
        }

        FilterExpr::If {
            condition,
            then_expr,
            else_expr,
        } => {
            let cond = evaluate_inner(condition, row, ctx)?;
            if cond {
                eval_to_comparable_inner(then_expr, row, ctx)
            } else {
                eval_to_comparable_inner(else_expr, row, ctx)
            }
        }

        FilterExpr::In { .. } => {
            // IN expression evaluates to boolean
            Ok(Some(ComparableValue::Bool(evaluate_inner(expr, row, ctx)?)))
        }

        FilterExpr::Function { name, args } => eval_function(name, args, row, ctx),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::ir::CompareOp;
    use crate::var_registry::VarId;
    use fluree_db_core::Sid;

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
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(20))),
        };

        // Row 0: age=25 > 20 → true
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate(&expr, &row0).unwrap());

        // Row 2: age=18 > 20 → false
        let row2 = batch.row_view(2).unwrap();
        assert!(!evaluate(&expr, &row2).unwrap());

        // Row 3: age=Unbound → false
        let row3 = batch.row_view(3).unwrap();
        assert!(!evaluate(&expr, &row3).unwrap());
    }

    #[test]
    fn test_evaluate_and() {
        let batch = make_test_batch();

        // ?age > 20 AND ?age < 28
        let expr = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(20))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(28))),
            },
        ]);

        // Row 0: age=25 → true (25 > 20 AND 25 < 28)
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate(&expr, &row0).unwrap());

        // Row 1: age=30 → false (30 > 20 but 30 < 28 is false)
        let row1 = batch.row_view(1).unwrap();
        assert!(!evaluate(&expr, &row1).unwrap());
    }

    #[test]
    fn test_evaluate_or() {
        let batch = make_test_batch();

        // ?age < 20 OR ?age > 28
        let expr = FilterExpr::Or(vec![
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(20))),
            },
            FilterExpr::Compare {
                op: CompareOp::Gt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(28))),
            },
        ]);

        // Row 0: age=25 → false
        let row0 = batch.row_view(0).unwrap();
        assert!(!evaluate(&expr, &row0).unwrap());

        // Row 1: age=30 → true (30 > 28)
        let row1 = batch.row_view(1).unwrap();
        assert!(evaluate(&expr, &row1).unwrap());

        // Row 2: age=18 → true (18 < 20)
        let row2 = batch.row_view(2).unwrap();
        assert!(evaluate(&expr, &row2).unwrap());
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_test_batch();

        // NOT(?age > 25)
        let expr = FilterExpr::Not(Box::new(FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(25))),
        }));

        // Row 0: age=25 → NOT(25 > 25) = NOT(false) = true
        let row0 = batch.row_view(0).unwrap();
        assert!(evaluate(&expr, &row0).unwrap());

        // Row 1: age=30 → NOT(30 > 25) = NOT(true) = false
        let row1 = batch.row_view(1).unwrap();
        assert!(!evaluate(&expr, &row1).unwrap());
    }
}
