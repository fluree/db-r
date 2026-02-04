//! Filter operator and expression evaluation
//!
//! This module provides:
//! - `FilterOperator`: Wraps a child operator and filters rows
//! - `evaluate()`: Evaluates FilterExpr against RowView
//! - Range extraction for filter pushdown (Phase 4)
//!
//! # Filter Evaluation Semantics
//!
//! This uses **two-valued logic** (true/false), not SQL 3-valued NULL logic:
//!
//! - **Unbound variables**: Comparisons involving unbound vars yield `false`
//! - **Type mismatches**: Comparisons between incompatible types yield `false`
//!   (except `!=` which yields `true` for mismatched types)
//! - **NaN**: Comparisons involving NaN yield `false` (except `!=` → `true`)
//! - **Logical operators**: Standard boolean logic (AND, OR, NOT)
//!
//! Note: `NOT(unbound_comparison)` evaluates to `true` because the inner
//! comparison returns `false`, which is then negated. This differs from
//! SQL NULL semantics where NULL comparisons propagate.

use crate::binding::{Batch, Binding, RowView};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::ir::{ArithmeticOp, CompareOp, FilterExpr, FilterValue, FunctionName};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use chrono::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Timelike,
    Utc,
};
use rand::random;
use fluree_db_core::{FlakeValue, NodeCache, Storage};
use fluree_db_core::temporal::{DateTime as FlureeDateTime, Date as FlureeDate, Time as FlureeTime};
use num_bigint::BigInt;
use bigdecimal::BigDecimal;
use md5::{Digest as Md5Digest, Md5};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use regex::RegexBuilder;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::cmp::Ordering;
use std::sync::Arc;
use uuid::Uuid;

/// Filter operator - applies a predicate to each row from child
///
/// Rows where the filter evaluates to `false` or encounters an error
/// (type mismatch, unbound var) are filtered out.
pub struct FilterOperator<S: Storage + 'static, C: NodeCache + 'static> {
    /// Child operator providing input rows
    child: BoxedOperator<S, C>,
    /// Filter expression to evaluate
    expr: FilterExpr,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl<S: Storage + 'static, C: NodeCache + 'static> FilterOperator<S, C> {
    /// Create a new filter operator
    pub fn new(child: BoxedOperator<S, C>, expr: FilterExpr) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            expr,
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the filter expression
    pub fn expr(&self) -> &FilterExpr {
        &self.expr
    }
}

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for FilterOperator<S, C> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if batch.is_empty() {
                continue;
            }

            // Collect row indices where filter evaluates to true
            let mut keep_indices: Vec<usize> = Vec::new();
            for row_idx in 0..batch.len() {
                let row = match batch.row_view(row_idx) {
                    Some(r) => r,
                    None => continue,
                };
                match evaluate(&self.expr, &row) {
                    Ok(true) => keep_indices.push(row_idx),
                    Ok(false) => {} // Filter out
                    Err(e) => return Err(e), // Propagate error
                }
            }

            if keep_indices.is_empty() {
                continue;
            }

            // Build filtered batch by selecting kept rows from each column
            let num_cols = self.schema.len();
            let columns: Vec<Vec<Binding>> = (0..num_cols)
                .map(|col_idx| {
                    let src_col = batch
                        .column_by_idx(col_idx)
                        .expect("child batch schema must match FilterOperator schema");
                    keep_indices
                        .iter()
                        .map(|&row_idx| src_col[row_idx].clone())
                        .collect()
                })
                .collect();

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: child rows * selectivity (assume 50% for now)
        self.child.estimated_rows().map(|r| r / 2)
    }
}

/// Evaluate a filter expression and return a Binding value
///
/// This is used by BIND operator to compute values for binding to variables.
/// Returns `Binding::Unbound` on evaluation errors (type mismatches, unbound vars, etc.)
/// rather than `Binding::Poisoned` - Poisoned is reserved for OPTIONAL semantics.
///
/// Evaluates the expression against a row and returns the computed `Binding`, or `Binding::Unbound` on error.
pub fn evaluate_to_binding(expr: &FilterExpr, row: &RowView) -> Binding {
    // Use MemoryStorage as a placeholder type since ctx is None
    // The generic types don't matter when ctx is None - they're only used for IRI encoding
    evaluate_to_binding_with_context::<fluree_db_core::MemoryStorage, fluree_db_core::NoCache>(expr, row, None)
}

pub fn evaluate_to_binding_with_context<S: Storage, C: NodeCache>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S, C>>,
) -> Binding {
    match evaluate_to_binding_with_context_strict(expr, row, ctx) {
        Ok(binding) => binding,
        Err(_) => Binding::Unbound,
    }
}

pub fn evaluate_to_binding_with_context_strict<S: Storage, C: NodeCache>(
    expr: &FilterExpr,
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S, C>>,
) -> Result<Binding> {
    // Evaluate to comparable value, errors become None
    let comparable = match eval_to_comparable(expr, row) {
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

    comparable.try_into_binding(ctx)
}

fn has_unbound_vars(expr: &FilterExpr, row: &RowView) -> bool {
    expr.variables().into_iter().any(|var| {
        matches!(row.get(var), None | Some(Binding::Unbound) | Some(Binding::Poisoned))
    })
}

/// Evaluate a filter expression against a row
///
/// Returns `true` if the row passes the filter, `false` otherwise.
/// Type mismatches and unbound variables result in `false`.
pub fn evaluate(expr: &FilterExpr, row: &RowView) -> Result<bool> {
    match expr {
        FilterExpr::Var(var) => {
            // Var as boolean: check if bound and truthy
            match row.get(*var) {
                Some(Binding::Lit { val, .. }) => match val {
                    FlakeValue::Boolean(b) => Ok(*b),
                    _ => Ok(true), // Non-bool literal is truthy
                },
                Some(Binding::Sid(_)) => Ok(true), // SID is truthy
                Some(Binding::IriMatch { .. }) => Ok(true), // IriMatch is truthy
                Some(Binding::Iri(_)) => Ok(true), // IRI is truthy
                Some(Binding::Unbound) | Some(Binding::Poisoned) | None => Ok(false),
                Some(Binding::Grouped(_)) => {
                    // Grouped bindings shouldn't appear in filter evaluation
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
            let left_val = eval_to_comparable(left, row)?;
            let right_val = eval_to_comparable(right, row)?;

            match (left_val, right_val) {
                (Some(l), Some(r)) => Ok(compare_values(&l, &r, *op)),
                // Either side unbound/null -> comparison is false
                _ => Ok(false),
            }
        }

        FilterExpr::And(exprs) => {
            for e in exprs {
                if !evaluate(e, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        FilterExpr::Or(exprs) => {
            for e in exprs {
                if evaluate(e, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        FilterExpr::Not(inner) => Ok(!evaluate(inner, row)?),

        FilterExpr::Arithmetic { .. } => {
            // Arithmetic as boolean: check if result is truthy (non-zero)
            match eval_to_comparable(expr, row)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(d != 0.0),
                Some(ComparableValue::Bool(b)) => Ok(b),
                _ => Ok(false),
            }
        }

        FilterExpr::Negate(_) => {
            // Negation as boolean: check if result is truthy (non-zero)
            match eval_to_comparable(expr, row)? {
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
            let cond = evaluate(condition, row)?;
            if cond {
                evaluate(then_expr, row)
            } else {
                evaluate(else_expr, row)
            }
        }

        FilterExpr::In {
            expr: test_expr,
            values,
            negated,
        } => {
            let test_val = eval_to_comparable(test_expr, row)?;
            match test_val {
                Some(tv) => {
                    let found = values.iter().any(|v| {
                        eval_to_comparable(v, row)
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

        FilterExpr::Function { name, args } => evaluate_function(name, args, row),
    }
}

/// Comparable value extracted from expression evaluation
#[derive(Debug, Clone, PartialEq)]
enum ComparableValue {
    Long(i64),
    Double(f64),
    String(Arc<str>),
    Bool(bool),
    Sid(fluree_db_core::Sid),
    Vector(Arc<[f64]>),
    // Extended numeric types
    BigInt(Box<BigInt>),
    Decimal(Box<BigDecimal>),
    // Temporal types
    DateTime(Box<FlureeDateTime>),
    Date(Box<FlureeDate>),
    Time(Box<FlureeTime>),
    // IRI string (to be encoded at bind time)
    Iri(Arc<str>),
    // Typed literal with explicit datatype or language
    TypedLiteral {
        val: FlakeValue,
        dt_iri: Option<Arc<str>>,
        lang: Option<Arc<str>>,
    },
}

impl ComparableValue {
    /// Get a human-readable type name for this value
    fn type_name(&self) -> &'static str {
        match self {
            ComparableValue::Long(_) => "Long",
            ComparableValue::Double(_) => "Double",
            ComparableValue::String(_) => "String",
            ComparableValue::Bool(_) => "Bool",
            ComparableValue::Sid(_) => "IRI (Subject ID)",
            ComparableValue::Vector(_) => "Vector",
            ComparableValue::BigInt(_) => "BigInt",
            ComparableValue::Decimal(_) => "Decimal",
            ComparableValue::DateTime(_) => "DateTime",
            ComparableValue::Date(_) => "Date",
            ComparableValue::Time(_) => "Time",
            ComparableValue::Iri(_) => "IRI",
            ComparableValue::TypedLiteral { .. } => "TypedLiteral",
        }
    }

    fn try_into_binding<S: Storage, C: NodeCache>(
        self,
        ctx: Option<&ExecutionContext<'_, S, C>>,
    ) -> Result<Binding> {
        let datatypes = WellKnownDatatypes::new();
        match self {
            ComparableValue::Long(n) => Ok(Binding::lit(FlakeValue::Long(n), datatypes.xsd_long)),
            ComparableValue::Double(d) => {
                Ok(Binding::lit(FlakeValue::Double(d), datatypes.xsd_double))
            }
            ComparableValue::String(s) => Ok(Binding::lit(
                FlakeValue::String(s.to_string()),
                datatypes.xsd_string,
            )),
            ComparableValue::Bool(b) => {
                Ok(Binding::lit(FlakeValue::Boolean(b), datatypes.xsd_boolean))
            }
            ComparableValue::Sid(sid) => Ok(Binding::Sid(sid)),
            ComparableValue::Vector(v) => Ok(Binding::lit(
                FlakeValue::Vector(v.to_vec()),
                datatypes.fluree_vector,
            )),
            ComparableValue::BigInt(n) => {
                Ok(Binding::lit(FlakeValue::BigInt(n), datatypes.xsd_integer))
            }
            ComparableValue::Decimal(d) => {
                Ok(Binding::lit(FlakeValue::Decimal(d), datatypes.xsd_decimal))
            }
            ComparableValue::DateTime(dt) => {
                Ok(Binding::lit(FlakeValue::DateTime(dt), datatypes.xsd_datetime))
            }
            ComparableValue::Date(d) => {
                Ok(Binding::lit(FlakeValue::Date(d), datatypes.xsd_date))
            }
            ComparableValue::Time(t) => {
                Ok(Binding::lit(FlakeValue::Time(t), datatypes.xsd_time))
            }
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
                    Binding::lit(val, datatypes.xsd_string)
                })
            }
        }
    }

    fn from_flake_value(val: &FlakeValue) -> Option<Self> {
        match val {
            FlakeValue::Long(n) => Some(ComparableValue::Long(*n)),
            FlakeValue::Double(d) => Some(ComparableValue::Double(*d)),
            FlakeValue::String(s) => Some(ComparableValue::String(Arc::from(s.as_str()))),
            FlakeValue::Json(s) => Some(ComparableValue::String(Arc::from(s.as_str()))),
            FlakeValue::Boolean(b) => Some(ComparableValue::Bool(*b)),
            FlakeValue::Ref(sid) => Some(ComparableValue::Sid(sid.clone())),
            FlakeValue::Null => None,
            FlakeValue::Vector(v) => Some(ComparableValue::Vector(Arc::from(v.as_slice()))),
            FlakeValue::BigInt(n) => Some(ComparableValue::BigInt(n.clone())),
            FlakeValue::Decimal(d) => Some(ComparableValue::Decimal(d.clone())),
            FlakeValue::DateTime(dt) => Some(ComparableValue::DateTime(dt.clone())),
            FlakeValue::Date(d) => Some(ComparableValue::Date(d.clone())),
            FlakeValue::Time(t) => Some(ComparableValue::Time(t.clone())),
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            ComparableValue::String(s) => Some(s.as_ref()),
            ComparableValue::Iri(s) => Some(s.as_ref()),
            ComparableValue::TypedLiteral {
                val: FlakeValue::String(s),
                ..
            } => Some(s.as_str()),
            _ => None,
        }
    }
}

impl From<&FilterValue> for ComparableValue {
    fn from(val: &FilterValue) -> Self {
        match val {
            FilterValue::Long(n) => ComparableValue::Long(*n),
            FilterValue::Double(d) => ComparableValue::Double(*d),
            FilterValue::String(s) => ComparableValue::String(Arc::from(s.as_str())),
            FilterValue::Bool(b) => ComparableValue::Bool(*b),
        }
    }
}

/// Evaluate expression to a comparable value
fn eval_to_comparable(expr: &FilterExpr, row: &RowView) -> Result<Option<ComparableValue>> {
    match expr {
        FilterExpr::Var(var) => {
            match row.get(*var) {
                Some(Binding::Lit { val, .. }) => Ok(ComparableValue::from_flake_value(val)),
                Some(Binding::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid.clone()))),
                Some(Binding::IriMatch { iri, .. }) => {
                    // IriMatch: use canonical IRI for comparisons (cross-ledger safe)
                    Ok(Some(ComparableValue::Iri(Arc::clone(iri))))
                }
                Some(Binding::Iri(iri)) => {
                    // Raw IRI from VG - use Iri comparable value
                    Ok(Some(ComparableValue::Iri(Arc::clone(iri))))
                }
                Some(Binding::Unbound) | Some(Binding::Poisoned) | None => Ok(None),
                Some(Binding::Grouped(_)) => {
                    // Grouped bindings shouldn't appear in filter evaluation
                    debug_assert!(false, "Grouped binding in filter evaluation");
                    Ok(None)
                }
            }
        }

        FilterExpr::Const(val) => Ok(Some(val.into())),

        FilterExpr::Compare { .. } => {
            // Comparison result is boolean
            Ok(Some(ComparableValue::Bool(evaluate(expr, row)?)))
        }

        FilterExpr::Arithmetic { op, left, right } => {
            let l = eval_to_comparable(left, row)?;
            let r = eval_to_comparable(right, row)?;
            match (l, r) {
                (Some(lv), Some(rv)) => Ok(eval_arithmetic(*op, lv, rv)),
                _ => Ok(None),
            }
        }

        FilterExpr::Negate(inner) => {
            match eval_to_comparable(inner, row)? {
                Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(-n))),
                Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(-d))),
                Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(-(*n))))),
                Some(ComparableValue::Decimal(d)) => Ok(Some(ComparableValue::Decimal(Box::new(-(*d))))),
                _ => Ok(None),
            }
        }

        FilterExpr::And(_) | FilterExpr::Or(_) | FilterExpr::Not(_) => {
            Ok(Some(ComparableValue::Bool(evaluate(expr, row)?)))
        }

        FilterExpr::If { condition, then_expr, else_expr } => {
            let cond = evaluate(condition, row)?;
            if cond {
                eval_to_comparable(then_expr, row)
            } else {
                eval_to_comparable(else_expr, row)
            }
        }

        FilterExpr::In { .. } => {
            // IN expression evaluates to boolean
            Ok(Some(ComparableValue::Bool(evaluate(expr, row)?)))
        }

        FilterExpr::Function { name, args } => {
            // Evaluate functions to their actual value type
            eval_function_to_value(name, args, row)
        }
    }
}

/// Evaluate arithmetic operation on two comparable values
fn eval_arithmetic(op: ArithmeticOp, left: ComparableValue, right: ComparableValue) -> Option<ComparableValue> {
    use num_traits::Zero;

    match (left, right) {
        // Long + Long = Long
        (ComparableValue::Long(a), ComparableValue::Long(b)) => {
            let result = match op {
                ArithmeticOp::Add => a.checked_add(b)?,
                ArithmeticOp::Sub => a.checked_sub(b)?,
                ArithmeticOp::Mul => a.checked_mul(b)?,
                ArithmeticOp::Div => {
                    if b == 0 {
                        return None;
                    }
                    a.checked_div(b)?
                }
            };
            Some(ComparableValue::Long(result))
        }
        // Double + Double = Double
        (ComparableValue::Double(a), ComparableValue::Double(b)) => {
            let result = match op {
                ArithmeticOp::Add => a + b,
                ArithmeticOp::Sub => a - b,
                ArithmeticOp::Mul => a * b,
                ArithmeticOp::Div => {
                    if b == 0.0 {
                        return None;
                    }
                    a / b
                }
            };
            Some(ComparableValue::Double(result))
        }
        // BigInt + BigInt = BigInt
        (ComparableValue::BigInt(a), ComparableValue::BigInt(b)) => {
            let result = match op {
                ArithmeticOp::Add => *a + &*b,
                ArithmeticOp::Sub => *a - &*b,
                ArithmeticOp::Mul => *a * &*b,
                ArithmeticOp::Div => {
                    if b.is_zero() {
                        return None;
                    }
                    *a / &*b
                }
            };
            Some(ComparableValue::BigInt(Box::new(result)))
        }
        // Decimal + Decimal = Decimal
        (ComparableValue::Decimal(a), ComparableValue::Decimal(b)) => {
            let result = match op {
                ArithmeticOp::Add => &*a + &*b,
                ArithmeticOp::Sub => &*a - &*b,
                ArithmeticOp::Mul => &*a * &*b,
                ArithmeticOp::Div => {
                    if b.is_zero() {
                        return None;
                    }
                    &*a / &*b
                }
            };
            Some(ComparableValue::Decimal(Box::new(result)))
        }
        // Mixed numeric types -> promote to higher precision
        // Long <-> Double -> Double
        (ComparableValue::Long(a), ComparableValue::Double(b)) => {
            eval_arithmetic(op, ComparableValue::Double(a as f64), ComparableValue::Double(b))
        }
        (ComparableValue::Double(a), ComparableValue::Long(b)) => {
            eval_arithmetic(op, ComparableValue::Double(a), ComparableValue::Double(b as f64))
        }
        // Long <-> BigInt -> BigInt
        (ComparableValue::Long(a), ComparableValue::BigInt(b)) => {
            eval_arithmetic(op, ComparableValue::BigInt(Box::new(BigInt::from(a))), ComparableValue::BigInt(b))
        }
        (ComparableValue::BigInt(a), ComparableValue::Long(b)) => {
            eval_arithmetic(op, ComparableValue::BigInt(a), ComparableValue::BigInt(Box::new(BigInt::from(b))))
        }
        // Long <-> Decimal -> Decimal
        (ComparableValue::Long(a), ComparableValue::Decimal(b)) => {
            eval_arithmetic(op, ComparableValue::Decimal(Box::new(BigDecimal::from(a))), ComparableValue::Decimal(b))
        }
        (ComparableValue::Decimal(a), ComparableValue::Long(b)) => {
            eval_arithmetic(op, ComparableValue::Decimal(a), ComparableValue::Decimal(Box::new(BigDecimal::from(b))))
        }
        // BigInt <-> Decimal -> Decimal
        (ComparableValue::BigInt(a), ComparableValue::Decimal(b)) => {
            eval_arithmetic(op, ComparableValue::Decimal(Box::new(BigDecimal::from((*a).clone()))), ComparableValue::Decimal(b))
        }
        (ComparableValue::Decimal(a), ComparableValue::BigInt(b)) => {
            eval_arithmetic(op, ComparableValue::Decimal(a), ComparableValue::Decimal(Box::new(BigDecimal::from((*b).clone()))))
        }
        // Double <-> BigInt -> Double (lossy)
        (ComparableValue::Double(a), ComparableValue::BigInt(b)) => {
            use num_traits::ToPrimitive;
            b.to_f64().and_then(|bf| eval_arithmetic(op, ComparableValue::Double(a), ComparableValue::Double(bf)))
        }
        (ComparableValue::BigInt(a), ComparableValue::Double(b)) => {
            use num_traits::ToPrimitive;
            a.to_f64().and_then(|af| eval_arithmetic(op, ComparableValue::Double(af), ComparableValue::Double(b)))
        }
        // Double <-> Decimal -> Decimal (if possible)
        (ComparableValue::Double(a), ComparableValue::Decimal(b)) => {
            BigDecimal::try_from(a).ok().and_then(|ad| {
                eval_arithmetic(op, ComparableValue::Decimal(Box::new(ad)), ComparableValue::Decimal(b))
            })
        }
        (ComparableValue::Decimal(a), ComparableValue::Double(b)) => {
            BigDecimal::try_from(b).ok().and_then(|bd| {
                eval_arithmetic(op, ComparableValue::Decimal(a), ComparableValue::Decimal(Box::new(bd)))
            })
        }
        // Non-numeric types can't do arithmetic
        _ => None,
    }
}

impl From<&ComparableValue> for FlakeValue {
    fn from(val: &ComparableValue) -> Self {
        match val {
            ComparableValue::Long(n) => FlakeValue::Long(*n),
            ComparableValue::Double(d) => FlakeValue::Double(*d),
            ComparableValue::String(s) => FlakeValue::String(s.to_string()),
            ComparableValue::Bool(b) => FlakeValue::Boolean(*b),
            ComparableValue::Sid(sid) => FlakeValue::Ref(sid.clone()),
            ComparableValue::Vector(v) => FlakeValue::Vector(v.to_vec()),
            ComparableValue::BigInt(n) => FlakeValue::BigInt(n.clone()),
            ComparableValue::Decimal(d) => FlakeValue::Decimal(d.clone()),
            ComparableValue::DateTime(dt) => FlakeValue::DateTime(dt.clone()),
            ComparableValue::Date(d) => FlakeValue::Date(d.clone()),
            ComparableValue::Time(t) => FlakeValue::Time(t.clone()),
            ComparableValue::Iri(s) => FlakeValue::String(s.to_string()),
            ComparableValue::TypedLiteral { val, .. } => val.clone(),
        }
    }
}

/// Compare two values with the given operator
///
/// Delegates to FlakeValue's comparison methods to avoid duplicating logic.
fn compare_values(left: &ComparableValue, right: &ComparableValue, op: CompareOp) -> bool {
    // Convert to FlakeValue and use its comparison methods
    let left_fv: FlakeValue = left.into();
    let right_fv: FlakeValue = right.into();

    // Try numeric comparison first (handles all numeric cross-type comparisons)
    if let Some(ordering) = left_fv.numeric_cmp(&right_fv) {
        return match op {
            CompareOp::Eq => ordering == Ordering::Equal,
            CompareOp::Ne => ordering != Ordering::Equal,
            CompareOp::Lt => ordering == Ordering::Less,
            CompareOp::Le => ordering != Ordering::Greater,
            CompareOp::Gt => ordering == Ordering::Greater,
            CompareOp::Ge => ordering != Ordering::Less,
        };
    }

    // Try temporal comparison (same-type temporal only)
    if let Some(ordering) = left_fv.temporal_cmp(&right_fv) {
        return match op {
            CompareOp::Eq => ordering == Ordering::Equal,
            CompareOp::Ne => ordering != Ordering::Equal,
            CompareOp::Lt => ordering == Ordering::Less,
            CompareOp::Le => ordering != Ordering::Greater,
            CompareOp::Gt => ordering == Ordering::Greater,
            CompareOp::Ge => ordering != Ordering::Less,
        };
    }

    // Fall back to same-type comparisons for non-numeric, non-temporal types
    let ordering = match (left, right) {
        (ComparableValue::String(a), ComparableValue::String(b)) => a.cmp(b),
        (ComparableValue::Bool(a), ComparableValue::Bool(b)) => a.cmp(b),
        (ComparableValue::Sid(a), ComparableValue::Sid(b)) => a.cmp(b),
        // Raw IRIs from VGs compare by string value
        (ComparableValue::Iri(a), ComparableValue::Iri(b)) => a.cmp(b),
        // Type mismatch -> always not equal
        _ => return matches!(op, CompareOp::Ne),
    };

    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    }
}

/// Build a regex with optional flags
///
/// Supported flags: i (case-insensitive), m (multiline), s (dot-all), x (ignore whitespace)
/// Returns an error for unknown flags (not silent ignore).
fn build_regex_with_flags(pattern: &str, flags: &str) -> Result<regex::Regex> {
    let mut builder = RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            c => {
                return Err(QueryError::InvalidFilter(format!(
                    "Unknown regex flag: '{}'",
                    c
                )));
            }
        }
    }
    builder
        .build()
        .map_err(|e| QueryError::InvalidFilter(format!("Invalid regex: {}", e)))
}

/// Parse a datetime from a binding, respecting datatype
///
/// Returns None if not a datetime type or parse fails.
/// Only parses when binding has XSD datetime/date/time datatype.
fn parse_datetime_from_binding(binding: &Binding) -> Option<DateTime<FixedOffset>> {
    let datatypes = WellKnownDatatypes::new();

    match binding {
        Binding::Lit { val, dt, .. } => {
            // Check datatype is datetime/date/time using known Sids (no IRI decoding)
            let is_datetime_type =
                *dt == datatypes.xsd_datetime || *dt == datatypes.xsd_date || *dt == datatypes.xsd_time;

            if !is_datetime_type {
                return None;
            }

            match val {
                FlakeValue::DateTime(dt) => {
                    let offset = dt
                        .tz_offset()
                        .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
                    Some(dt.instant().with_timezone(&offset))
                }
                FlakeValue::Date(d) => {
                    let offset = d
                        .tz_offset()
                        .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
                    let naive = d.date().and_hms_opt(0, 0, 0)?;
                    Some(
                        offset
                            .from_local_datetime(&naive)
                            .single()
                            .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
                    )
                }
                FlakeValue::Time(t) => {
                    let offset = t
                        .tz_offset()
                        .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
                    let date = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                    let naive = NaiveDateTime::new(date, t.time());
                    Some(
                        offset
                            .from_local_datetime(&naive)
                            .single()
                            .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
                    )
                }
                FlakeValue::String(s) => DateTime::parse_from_rfc3339(s).ok().or_else(|| {
                    // Try parsing as date only (add T00:00:00+00:00)
                    let with_time = format!("{}T00:00:00+00:00", s);
                    DateTime::parse_from_rfc3339(&with_time).ok()
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

// =============================================================================
// SPARQL Function Evaluation Helpers
// =============================================================================
//
// These helpers reduce boilerplate in the massive eval_function_to_value and
// evaluate_function match statements by extracting common patterns.

/// Check that a function has the expected number of arguments
#[inline]
fn check_arity(args: &[FilterExpr], expected: usize, fn_name: &str) -> Result<()> {
    if args.len() != expected {
        Err(QueryError::InvalidFilter(format!(
            "{} requires exactly {} argument{}",
            fn_name,
            expected,
            if expected == 1 { "" } else { "s" }
        )))
    } else {
        Ok(())
    }
}

/// Evaluate a unary numeric function (Long -> Long, Double -> Double)
///
/// Used for ABS, ROUND, CEIL, FLOOR, etc.
fn eval_unary_numeric<FL, FD>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    long_op: FL,
    double_op: FD,
) -> Result<Option<ComparableValue>>
where
    FL: Fn(i64) -> i64,
    FD: Fn(f64) -> f64,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable(&args[0], row)?;
    match val {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(long_op(n)))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(double_op(d)))),
        Some(other) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "numeric (Long or Double)".to_string(),
            actual: other.type_name().to_string(),
        }),
        None => Ok(None), // Unbound variable - not a type error
    }
}

/// Evaluate a unary string transformation function (String -> String)
///
/// Used for UCASE, LCASE, etc.
fn eval_unary_string_transform<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    transform: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> String,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable(&args[0], row)?;
    match val {
        Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::String(Arc::from(transform(&s))))),
        Some(other) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "String".to_string(),
            actual: other.type_name().to_string(),
        }),
        None => Ok(None),
    }
}

/// Evaluate a unary string-to-long function (String -> Long)
///
/// Used for STRLEN.
fn eval_string_to_long<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    transform: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> i64,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable(&args[0], row)?;
    match val {
        Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::Long(transform(&s)))),
        Some(other) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "String".to_string(),
            actual: other.type_name().to_string(),
        }),
        None => Ok(None),
    }
}

/// Evaluate a binary string predicate (String, String -> Bool)
///
/// Used for CONTAINS, STRSTARTS, STRENDS.
fn eval_binary_string_pred<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    pred: F,
) -> Result<bool>
where
    F: Fn(&str, &str) -> bool,
{
    check_arity(args, 2, fn_name)?;
    let arg1 = eval_to_comparable(&args[0], row)?;
    let arg2 = eval_to_comparable(&args[1], row)?;
    match (&arg1, &arg2) {
        (Some(ComparableValue::String(h)), Some(ComparableValue::String(n))) => Ok(pred(h, n)),
        (None, _) | (_, None) => Ok(false), // Unbound - semantic false
        (Some(a), Some(b)) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "String, String".to_string(),
            actual: format!("{}, {}", a.type_name(), b.type_name()),
        }),
    }
}

/// Evaluate a binary string function (String, String -> String)
///
/// Used for STRBEFORE, STRAFTER.
fn eval_binary_string_fn<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    func: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str, &str) -> Option<String>,
{
    check_arity(args, 2, fn_name)?;
    let arg1 = eval_to_comparable(&args[0], row)?;
    let arg2 = eval_to_comparable(&args[1], row)?;
    match (&arg1, &arg2) {
        (Some(ComparableValue::String(s1)), Some(ComparableValue::String(s2))) => {
            Ok(func(s1, s2).map(|r| ComparableValue::String(Arc::from(r))))
        }
        (None, _) | (_, None) => Ok(None), // Unbound
        (Some(a), Some(b)) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "String, String".to_string(),
            actual: format!("{}, {}", a.type_name(), b.type_name()),
        }),
    }
}

/// Evaluate a string hash function
///
/// Helper that handles arity check and string extraction for hash functions.
/// The caller provides the actual hashing logic.
fn eval_hash_string<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    hash_fn: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> String,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable(&args[0], row)?;
    Ok(match val {
        Some(ComparableValue::String(s)) => Some(ComparableValue::String(Arc::from(hash_fn(&s)))),
        _ => None,
    })
}

/// Extract a datetime component (year, month, day, etc.)
fn eval_datetime_component<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    extract: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&DateTime<FixedOffset>) -> i64,
{
    check_arity(args, 1, fn_name)?;
    if let FilterExpr::Var(var) = &args[0] {
        if let Some(binding) = row.get(*var) {
            if let Some(dt) = parse_datetime_from_binding(binding) {
                return Ok(Some(ComparableValue::Long(extract(&dt))));
            }
        }
    }
    Ok(None)
}

/// Evaluate a function that checks if the first argument is valid (truthy check)
fn eval_truthy_if_arg_valid(
    args: &[FilterExpr],
    row: &RowView,
) -> Result<bool> {
    if args.is_empty() {
        Ok(false)
    } else {
        let val = eval_to_comparable(&args[0], row)?;
        Ok(val.is_some())
    }
}

type VectorPair = (Arc<[f64]>, Arc<[f64]>);

/// Extract two vectors from binary function arguments
///
/// Returns None if args don't evaluate to two equal-length vectors.
fn extract_vector_pair(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
) -> Result<Option<VectorPair>> {
    check_arity(args, 2, fn_name)?;
    let v1 = eval_to_comparable(&args[0], row)?;
    let v2 = eval_to_comparable(&args[1], row)?;
    match (&v1, &v2) {
        (Some(ComparableValue::Vector(a)), Some(ComparableValue::Vector(b))) => {
            if a.len() != b.len() {
                Err(QueryError::FunctionTypeMismatch {
                    function: fn_name.to_string(),
                    expected: format!("vectors of same dimension (got {} and {})", a.len(), b.len()),
                    actual: "vectors of different dimensions".to_string(),
                })
            } else {
                Ok(Some((Arc::clone(a), Arc::clone(b))))
            }
        }
        (None, _) | (_, None) => Ok(None), // Unbound
        (Some(a), Some(b)) => Err(QueryError::FunctionTypeMismatch {
            function: fn_name.to_string(),
            expected: "Vector, Vector".to_string(),
            actual: format!("{}, {}", a.type_name(), b.type_name()),
        }),
    }
}

/// Evaluate a binary vector function (Vector, Vector -> Double)
///
/// Used for dotProduct, cosineSimilarity, euclideanDistance.
fn eval_binary_vector_fn<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    compute: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&[f64], &[f64]) -> Option<f64>,
{
    match extract_vector_pair(args, row, fn_name)? {
        Some((a, b)) => Ok(compute(&a, &b).map(ComparableValue::Double)),
        None => Ok(None),
    }
}

/// Extract a variable binding's metadata field
///
/// Used for LANG, DATATYPE, T, OP which operate on binding metadata.
fn eval_var_metadata<F, T>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    extractor: F,
) -> Result<Option<T>>
where
    F: Fn(&Binding) -> Option<T>,
{
    check_arity(args, 1, fn_name)?;
    if let FilterExpr::Var(var_id) = &args[0] {
        if let Some(binding) = row.get(*var_id) {
            return Ok(extractor(binding));
        }
    }
    Ok(None)
}

/// Format a datatype Sid as a ComparableValue
///
/// Returns compact string representations for well-known datatypes.
fn format_datatype_sid(dt: &fluree_db_core::Sid) -> ComparableValue {
    let datatypes = WellKnownDatatypes::new();
    if *dt == datatypes.rdf_json {
        ComparableValue::String(Arc::from("@json"))
    } else if *dt == datatypes.id_type {
        ComparableValue::String(Arc::from("@id"))
    } else if dt.namespace_code == datatypes.xsd_string.namespace_code {
        ComparableValue::String(Arc::from(format!("xsd:{}", dt.name_str())))
    } else if dt.namespace_code == datatypes.rdf_json.namespace_code {
        ComparableValue::String(Arc::from(format!("rdf:{}", dt.name_str())))
    } else {
        ComparableValue::Sid(dt.clone())
    }
}

/// Convert a ComparableValue to its string representation for STR()
fn comparable_to_str_value(val: ComparableValue) -> Option<ComparableValue> {
    let s: Arc<str> = match val {
        ComparableValue::String(s) => return Some(ComparableValue::String(s)),
        ComparableValue::Iri(s) => return Some(ComparableValue::String(s)),
        ComparableValue::Long(n) => Arc::from(n.to_string()),
        ComparableValue::Double(d) => Arc::from(d.to_string()),
        ComparableValue::Bool(b) => Arc::from(b.to_string()),
        ComparableValue::Sid(sid) => Arc::from(sid.name.as_ref()),
        ComparableValue::Vector(v) => {
            Arc::from(format!("[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")))
        }
        ComparableValue::BigInt(n) => Arc::from(n.to_string()),
        ComparableValue::Decimal(d) => Arc::from(d.to_string()),
        ComparableValue::DateTime(dt) => Arc::from(dt.to_string()),
        ComparableValue::Date(d) => Arc::from(d.to_string()),
        ComparableValue::Time(t) => Arc::from(t.to_string()),
        ComparableValue::TypedLiteral { val, .. } => match val {
            FlakeValue::String(s) => Arc::from(s.as_str()),
            FlakeValue::Long(n) => Arc::from(n.to_string()),
            FlakeValue::Double(d) => Arc::from(d.to_string()),
            FlakeValue::Boolean(b) => Arc::from(b.to_string()),
            _ => return None,
        },
    };
    Some(ComparableValue::String(s))
}

/// Evaluate a type-checking function (isIRI, isLiteral, isNumeric)
fn eval_type_check<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    check: F,
) -> Result<bool>
where
    F: Fn(&ComparableValue) -> bool,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable(&args[0], row)?;
    Ok(val.is_some_and(|v| check(&v)))
}

/// Evaluate a function to its actual value type (for use in expressions)
///
/// This is used when functions appear in arithmetic contexts like `STRLEN(?x) + 1`.
fn eval_function_to_value(
    name: &FunctionName,
    args: &[FilterExpr],
    row: &RowView,
) -> Result<Option<ComparableValue>> {
    match name {
        // String functions that return integers
        FunctionName::Strlen => eval_string_to_long(args, row, "STRLEN", |s| s.len() as i64),

        // Numeric functions that return numbers
        FunctionName::Abs => eval_unary_numeric(args, row, "ABS", |n| n.abs(), |d| d.abs()),
        FunctionName::Round => eval_unary_numeric(args, row, "ROUND", |n| n, |d| d.round()),
        FunctionName::Ceil => eval_unary_numeric(args, row, "CEIL", |n| n, |d| d.ceil()),
        FunctionName::Floor => eval_unary_numeric(args, row, "FLOOR", |n| n, |d| d.floor()),

        FunctionName::Rand => {
            check_arity(args, 0, "RAND")?;
            Ok(Some(ComparableValue::Double(random::<f64>())))
        }

        FunctionName::Power => {
            check_arity(args, 2, "POWER")?;
            let base = eval_to_comparable(&args[0], row)?;
            let exp = eval_to_comparable(&args[1], row)?;
            Ok(match (base, exp) {
                (Some(ComparableValue::Long(b)), Some(ComparableValue::Long(e))) => {
                    if e >= 0 {
                        Some(ComparableValue::Long(b.saturating_pow(e as u32)))
                    } else {
                        Some(ComparableValue::Double((b as f64).powf(e as f64)))
                    }
                }
                (Some(ComparableValue::Double(b)), Some(ComparableValue::Double(e))) => {
                    Some(ComparableValue::Double(b.powf(e)))
                }
                (Some(ComparableValue::Long(b)), Some(ComparableValue::Double(e))) => {
                    Some(ComparableValue::Double((b as f64).powf(e)))
                }
                (Some(ComparableValue::Double(b)), Some(ComparableValue::Long(e))) => {
                    Some(ComparableValue::Double(b.powf(e as f64)))
                }
                _ => None,
            })
        }

        // String functions that return strings
        FunctionName::Ucase => eval_unary_string_transform(args, row, "UCASE", |s| s.to_uppercase()),
        FunctionName::Lcase => eval_unary_string_transform(args, row, "LCASE", |s| s.to_lowercase()),

        // Conditional functions
        FunctionName::If => {
            check_arity(args, 3, "IF")?;
            let cond = evaluate(&args[0], row)?;
            if cond {
                eval_to_comparable(&args[1], row)
            } else {
                eval_to_comparable(&args[2], row)
            }
        }

        FunctionName::Coalesce => {
            for arg in args {
                let val = eval_to_comparable(arg, row)?;
                if val.is_some() {
                    return Ok(val);
                }
            }
            Ok(None)
        }

        // Boolean functions - evaluate to Bool
        FunctionName::Bound
        | FunctionName::IsIri
        | FunctionName::IsBlank
        | FunctionName::IsLiteral
        | FunctionName::IsNumeric
        | FunctionName::Contains
        | FunctionName::StrStarts
        | FunctionName::StrEnds
        | FunctionName::Regex => {
            Ok(Some(ComparableValue::Bool(evaluate_function(name, args, row)?)))
        }

        // String functions - P7
        FunctionName::Concat => {
            // Variadic concatenation
            let mut result = String::new();
            for arg in args {
                if let Some(val) = eval_to_comparable(arg, row)? {
                    if let Some(s) = val.as_str() {
                        result.push_str(s);
                    }
                }
            }
            Ok(Some(ComparableValue::String(Arc::from(result))))
        }

        FunctionName::StrBefore => eval_binary_string_fn(args, row, "STRBEFORE", |s, d| {
            Some(s.find(d).map(|pos| &s[..pos]).unwrap_or("").to_string())
        }),

        FunctionName::StrAfter => eval_binary_string_fn(args, row, "STRAFTER", |s, d| {
            Some(s.find(d).map(|pos| &s[pos + d.len()..]).unwrap_or("").to_string())
        }),

        FunctionName::Replace => {
            // Regex replacement with optional flags
            if args.len() < 3 {
                return Err(QueryError::InvalidFilter(
                    "REPLACE requires 3-4 arguments".to_string(),
                ));
            }
            let input = eval_to_comparable(&args[0], row)?;
            let pattern = eval_to_comparable(&args[1], row)?;
            let replacement = eval_to_comparable(&args[2], row)?;
            let flags = if args.len() > 3 {
                eval_to_comparable(&args[3], row)?
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            match (&input, &pattern, &replacement) {
                (
                    Some(ComparableValue::String(s)),
                    Some(ComparableValue::String(p)),
                    Some(ComparableValue::String(r)),
                ) => {
                    let re = build_regex_with_flags(p, &flags)?;
                    Ok(Some(ComparableValue::String(Arc::from(
                        re.replace_all(s, r.as_ref()).into_owned(),
                    ))))
                }
                (None, _, _) | (_, None, _) | (_, _, None) => Ok(None), // Unbound
                (Some(a), Some(b), Some(c)) => Err(QueryError::FunctionTypeMismatch {
                    function: "REPLACE".to_string(),
                    expected: "String, String, String".to_string(),
                    actual: format!("{}, {}, {}", a.type_name(), b.type_name(), c.type_name()),
                }),
            }
        }

        // DateTime functions - P8
        FunctionName::Now => {
            // Return current timestamp with fractional seconds
            let now = Utc::now();
            let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
            let parsed = FlureeDateTime::parse(&formatted).map_err(|e| {
                QueryError::InvalidFilter(format!("now parse error: {}", e))
            })?;
            Ok(Some(ComparableValue::DateTime(Box::new(parsed))))
        }

        FunctionName::Year => eval_datetime_component(args, row, "YEAR", |dt| dt.year() as i64),
        FunctionName::Month => eval_datetime_component(args, row, "MONTH", |dt| dt.month() as i64),
        FunctionName::Day => eval_datetime_component(args, row, "DAY", |dt| dt.day() as i64),
        FunctionName::Hours => eval_datetime_component(args, row, "HOURS", |dt| dt.hour() as i64),
        FunctionName::Minutes => eval_datetime_component(args, row, "MINUTES", |dt| dt.minute() as i64),
        FunctionName::Seconds => eval_datetime_component(args, row, "SECONDS", |dt| dt.second() as i64),

        FunctionName::Tz => eval_var_metadata(args, row, "TZ", |binding| {
            parse_datetime_from_binding(binding).map(|dt| {
                let offset = dt.offset();
                let total_secs = offset.local_minus_utc();
                let hours = total_secs / 3600;
                let mins = (total_secs.abs() % 3600) / 60;
                let sign = if total_secs >= 0 { '+' } else { '-' };
                let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                ComparableValue::String(Arc::from(tz_str))
            })
        }),

        // RDF term functions - P9
        FunctionName::Lang => {
            check_arity(args, 1, "LANG")?;
            // Only Var bindings have lang metadata; all else returns ""
            let tag = match &args[0] {
                FilterExpr::Var(var_id) => match row.get(*var_id) {
                    Some(Binding::Lit { lang, .. }) => {
                        lang.as_ref().map(|l| l.as_ref()).unwrap_or("")
                    }
                    _ => "",
                },
                _ => "",
            };
            Ok(Some(ComparableValue::String(Arc::from(tag))))
        }

        FunctionName::Datatype => eval_var_metadata(args, row, "DATATYPE", |binding| {
            match binding {
                Binding::Lit { dt, .. } => Some(format_datatype_sid(dt)),
                Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) => {
                    // IRIs: return "@id" type
                    Some(ComparableValue::String(Arc::from("@id")))
                }
                _ => None,
            }
        }),

        // T(?var) - Fluree-specific: get transaction time from binding
        FunctionName::T => eval_var_metadata(args, row, "T", |binding| match binding {
            Binding::Lit { t: Some(t), .. } => Some(ComparableValue::Long(*t)),
            _ => None,
        }),

        // OP(?var) - Fluree-specific: get operation type from binding for history queries
        // Returns "assert" or "retract" as a string based on the flake's op flag.
        FunctionName::Op => eval_var_metadata(args, row, "OP", |binding| match binding {
            Binding::Lit { op: Some(op), .. } => {
                let op_str = if *op { "assert" } else { "retract" };
                Some(ComparableValue::String(Arc::from(op_str)))
            }
            _ => None,
        }),

        // SUBSTR(string, start, [length])
        // SPARQL uses 1-based indexing
        FunctionName::Substr => {
            if args.len() < 2 || args.len() > 3 {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires 2-3 arguments".to_string(),
                ));
            }
            let input = eval_to_comparable(&args[0], row)?;
            let start = eval_to_comparable(&args[1], row)?;
            let length = if args.len() > 2 {
                eval_to_comparable(&args[2], row)?
            } else {
                None
            };

            match (&input, &start) {
                (Some(ComparableValue::String(s)), Some(ComparableValue::Long(start_1))) => {
                    // SPARQL uses 1-based indexing, convert to 0-based
                    // Handle negative/zero start: clamp to 1
                    let start_0 = if *start_1 < 1 { 0 } else { (*start_1 - 1) as usize };

                    // Handle start beyond string length
                    if start_0 >= s.len() {
                        return Ok(Some(ComparableValue::String(Arc::from(""))));
                    }

                    let result = match &length {
                        Some(ComparableValue::Long(len)) if *len > 0 => {
                            let end = start_0 + (*len as usize);
                            let end = end.min(s.len());
                            &s[start_0..end]
                        }
                        Some(ComparableValue::Long(_)) => "", // Non-positive length
                        None => &s[start_0..], // No length = rest of string
                        Some(other) => return Err(QueryError::FunctionTypeMismatch {
                            function: "SUBSTR".to_string(),
                            expected: "Long (length)".to_string(),
                            actual: other.type_name().to_string(),
                        }),
                    };
                    Ok(Some(ComparableValue::String(Arc::from(result))))
                }
                (None, _) | (_, None) => Ok(None), // Unbound
                (Some(a), Some(b)) => Err(QueryError::FunctionTypeMismatch {
                    function: "SUBSTR".to_string(),
                    expected: "String, Long".to_string(),
                    actual: format!("{}, {}", a.type_name(), b.type_name()),
                }),
            }
        }

        // STR(value) - convert to string representation
        FunctionName::Str => {
            check_arity(args, 1, "STR")?;
            let val = eval_to_comparable(&args[0], row)?;
            Ok(val.and_then(comparable_to_str_value))
        }

        // ENCODE_FOR_URI(string) - percent-encode for URIs
        FunctionName::EncodeForUri => {
            eval_unary_string_transform(args, row, "ENCODE_FOR_URI", |s| {
                utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
            })
        }

        // LANGMATCHES(lang_tag, lang_range) - language tag matching
        FunctionName::LangMatches => {
            check_arity(args, 2, "LANGMATCHES")?;
            let tag = eval_to_comparable(&args[0], row)?;
            let range = eval_to_comparable(&args[1], row)?;
            let result = match (tag, range) {
                (Some(ComparableValue::String(t)), Some(ComparableValue::String(r))) => {
                    if r.as_ref() == "*" {
                        !t.is_empty() // "*" matches any non-empty tag
                    } else {
                        // Case-insensitive prefix match with '-' boundary
                        let t_lower = t.to_lowercase();
                        let r_lower = r.to_lowercase();
                        t_lower == r_lower
                            || (t_lower.starts_with(&r_lower)
                                && t_lower.chars().nth(r_lower.len()) == Some('-'))
                    }
                }
                _ => false,
            };
            Ok(Some(ComparableValue::Bool(result)))
        }

        // SAMETERM(term1, term2) - RDF term equality (stricter than =)
        FunctionName::SameTerm => {
            check_arity(args, 2, "SAMETERM")?;
            let v1 = eval_to_comparable(&args[0], row)?;
            let v2 = eval_to_comparable(&args[1], row)?;
            let same = matches!((v1, v2), (Some(a), Some(b)) if a == b);
            Ok(Some(ComparableValue::Bool(same)))
        }

        // Hash functions
        FunctionName::Md5 => eval_hash_string(args, row, "MD5", |s| {
            let mut hasher = Md5::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),
        FunctionName::Sha1 => eval_hash_string(args, row, "SHA1", |s| {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),
        FunctionName::Sha256 => eval_hash_string(args, row, "SHA256", |s| {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),
        FunctionName::Sha384 => eval_hash_string(args, row, "SHA384", |s| {
            let mut hasher = Sha384::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),
        FunctionName::Sha512 => eval_hash_string(args, row, "SHA512", |s| {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        // UUID functions
        FunctionName::Uuid => {
            // Returns a fresh IRI with UUID - but we return as string since we can't create Sids
            let uuid = Uuid::new_v4();
            Ok(Some(ComparableValue::String(Arc::from(format!(
                "urn:uuid:{}",
                uuid
            )))))
        }

        FunctionName::StrUuid => {
            // Returns UUID as plain string (no urn: prefix)
            let uuid = Uuid::new_v4();
            Ok(Some(ComparableValue::String(Arc::from(uuid.to_string()))))
        }

        // Vector/embedding similarity functions
        FunctionName::DotProduct => eval_binary_vector_fn(args, row, "dotProduct", |a, b| {
            Some(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
        }),

        FunctionName::CosineSimilarity => {
            eval_binary_vector_fn(args, row, "cosineSimilarity", |a, b| {
                let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
                let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
                if mag_a == 0.0 || mag_b == 0.0 {
                    None // Cannot compute similarity with zero vector
                } else {
                    Some(dot / (mag_a * mag_b))
                }
            })
        }

        FunctionName::EuclideanDistance => {
            eval_binary_vector_fn(args, row, "euclideanDistance", |a, b| {
                let sum_sq: f64 = a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| {
                        let diff = x - y;
                        diff * diff
                    })
                    .sum();
                Some(sum_sq.sqrt())
            })
        }

        // STRDT(lexical, datatype) - construct typed literal
        FunctionName::StrDt => {
            check_arity(args, 2, "STRDT")?;
            let val = eval_to_comparable(&args[0], row)?;
            let dt = eval_to_comparable(&args[1], row)?;
            match (&val, &dt) {
                (Some(ComparableValue::String(s)), Some(dt_val)) => {
                    Ok(Some(ComparableValue::TypedLiteral {
                        val: FlakeValue::String(s.to_string()),
                        dt_iri: dt_val.as_str().map(Arc::from),
                        lang: None,
                    }))
                }
                (None, _) | (_, None) => Ok(None), // Unbound
                (Some(other), Some(_)) => Err(QueryError::FunctionTypeMismatch {
                    function: "STRDT".to_string(),
                    expected: "String (lexical form)".to_string(),
                    actual: other.type_name().to_string(),
                }),
            }
        }

        // STRLANG(lexical, lang) - construct language-tagged literal
        FunctionName::StrLang => {
            check_arity(args, 2, "STRLANG")?;
            let val = eval_to_comparable(&args[0], row)?;
            let lang = eval_to_comparable(&args[1], row)?;
            match (&val, &lang) {
                (Some(ComparableValue::String(s)), Some(lang_val)) => {
                    Ok(Some(ComparableValue::TypedLiteral {
                        val: FlakeValue::String(s.to_string()),
                        dt_iri: None,
                        lang: lang_val.as_str().map(Arc::from),
                    }))
                }
                (None, _) | (_, None) => Ok(None), // Unbound
                (Some(other), Some(_)) => Err(QueryError::FunctionTypeMismatch {
                    function: "STRLANG".to_string(),
                    expected: "String (lexical form)".to_string(),
                    actual: other.type_name().to_string(),
                }),
            }
        }

        // IRI(string) / URI(string) - construct IRI from string
        FunctionName::Iri => {
            check_arity(args, 1, "IRI")?;
            match eval_to_comparable(&args[0], row)? {
                Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::Iri(s))),
                Some(ComparableValue::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid))),
                Some(ComparableValue::Iri(iri)) => Ok(Some(ComparableValue::Iri(iri))),
                Some(other) => Err(QueryError::FunctionTypeMismatch {
                    function: "IRI".to_string(),
                    expected: "String or IRI".to_string(),
                    actual: other.type_name().to_string(),
                }),
                None => Ok(None),
            }
        }

        // BNODE() - construct blank node (label argument not supported)
        FunctionName::Bnode => {
            if !args.is_empty() {
                return Err(QueryError::InvalidFilter(
                    "BNODE requires no arguments".to_string(),
                ));
            }
            Ok(Some(ComparableValue::Iri(Arc::from(format!(
                "_:fdb-{}",
                Uuid::new_v4()
            )))))
        }

        // Custom/unknown function - return error
        FunctionName::Custom(name) => Err(QueryError::UnknownFunction(name.clone())),
    }
}

/// Evaluate a function call (boolean context)
fn evaluate_function(name: &FunctionName, args: &[FilterExpr], row: &RowView) -> Result<bool> {
    match name {
        FunctionName::Bound => {
            check_arity(args, 1, "BOUND")?;
            match &args[0] {
                FilterExpr::Var(var) => Ok(!matches!(
                    row.get(*var),
                    Some(Binding::Unbound) | Some(Binding::Poisoned) | None
                )),
                _ => Err(QueryError::InvalidFilter(
                    "BOUND argument must be a variable".to_string(),
                )),
            }
        }

        FunctionName::IsIri => eval_type_check(args, row, "isIRI", |val| {
            matches!(val, ComparableValue::Sid(_) | ComparableValue::Iri(_))
        }),

        FunctionName::IsLiteral => eval_type_check(args, row, "isLiteral", |val| {
            matches!(
                val,
                ComparableValue::Long(_)
                    | ComparableValue::Double(_)
                    | ComparableValue::String(_)
                    | ComparableValue::Bool(_)
            )
        }),

        FunctionName::IsNumeric => eval_type_check(args, row, "isNumeric", |val| {
            matches!(val, ComparableValue::Long(_) | ComparableValue::Double(_))
        }),

        FunctionName::IsBlank => {
            // Fluree doesn't really have blank nodes, so always false
            Ok(false)
        }

        FunctionName::Contains => eval_binary_string_pred(args, row, "CONTAINS", |h, n| h.contains(n)),
        FunctionName::StrStarts => eval_binary_string_pred(args, row, "STRSTARTS", |h, p| h.starts_with(p)),
        FunctionName::StrEnds => eval_binary_string_pred(args, row, "STRENDS", |h, s| h.ends_with(s)),

        FunctionName::Regex => {
            // REGEX(text, pattern, [flags]) - regex pattern matching
            if args.len() < 2 {
                return Err(QueryError::InvalidFilter(
                    "REGEX requires 2-3 arguments".to_string(),
                ));
            }
            let text = eval_to_comparable(&args[0], row)?;
            let pattern = eval_to_comparable(&args[1], row)?;
            let flags = if args.len() > 2 {
                eval_to_comparable(&args[2], row)?
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            match (text, pattern) {
                (Some(ComparableValue::String(t)), Some(ComparableValue::String(p))) => {
                    let re = build_regex_with_flags(&p, &flags)?;
                    Ok(re.is_match(&t))
                }
                _ => Ok(false),
            }
        }

        // Functions that return non-boolean need special handling
        // For filter context, we treat non-zero/non-empty as true
        FunctionName::Strlen => eval_truthy_if_arg_valid(args, row),

        FunctionName::If => {
            check_arity(args, 3, "IF")?;
            if evaluate(&args[0], row)? {
                evaluate(&args[1], row)
            } else {
                evaluate(&args[2], row)
            }
        }

        FunctionName::Coalesce => {
            // In filter context: truthy if any arg is non-null
            for arg in args {
                if eval_to_comparable(arg, row)?.is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        // Functions returning values - truthy in filter context if computable
        FunctionName::Abs
        | FunctionName::Round
        | FunctionName::Ceil
        | FunctionName::Floor
        | FunctionName::Ucase
        | FunctionName::Lcase
        | FunctionName::Concat
        | FunctionName::StrBefore
        | FunctionName::StrAfter
        | FunctionName::Replace
        | FunctionName::Year
        | FunctionName::Month
        | FunctionName::Day
        | FunctionName::Hours
        | FunctionName::Minutes
        | FunctionName::Seconds
        | FunctionName::Tz
        | FunctionName::Lang
        | FunctionName::Datatype
        | FunctionName::Iri
        | FunctionName::StrDt
        | FunctionName::StrLang
        | FunctionName::Str
        | FunctionName::EncodeForUri
        | FunctionName::Substr => eval_truthy_if_arg_valid(args, row),

        // No-arg functions always truthy (always produce a value)
        FunctionName::Now | FunctionName::Rand | FunctionName::Uuid | FunctionName::StrUuid | FunctionName::Bnode => Ok(true),

        // Hash functions - truthy if input is a valid string
        FunctionName::Md5
        | FunctionName::Sha1
        | FunctionName::Sha256
        | FunctionName::Sha384
        | FunctionName::Sha512 => {
            let val = eval_to_comparable(args.first().ok_or_else(|| {
                QueryError::InvalidFilter("Hash function requires 1 argument".to_string())
            })?, row)?;
            Ok(matches!(val, Some(ComparableValue::String(_))))
        }

        // Boolean-returning functions - evaluate via eval_function_to_value
        FunctionName::LangMatches | FunctionName::SameTerm => {
            match eval_function_to_value(name, args, row)? {
                Some(ComparableValue::Bool(b)) => Ok(b),
                _ => Ok(false),
            }
        }

        // Power - numeric truthiness
        FunctionName::Power => {
            match eval_function_to_value(name, args, row)? {
                Some(ComparableValue::Long(n)) => Ok(n != 0),
                Some(ComparableValue::Double(d)) => Ok(!d.is_nan() && d != 0.0),
                _ => Ok(false),
            }
        }

        // Vector functions - truthy if they produce a value
        FunctionName::DotProduct
        | FunctionName::CosineSimilarity
        | FunctionName::EuclideanDistance => {
            match eval_function_to_value(name, args, row)? {
                // Follow SPARQL EBV-ish numeric truthiness:
                //  - NaN => false
                //  - 0.0 => false
                //  - otherwise => true
                Some(ComparableValue::Double(d)) => Ok(!d.is_nan() && d != 0.0),
                _ => Ok(false),
            }
        }

        // T(?var) - Fluree-specific transaction time
        // In boolean context: t=0 is falsy, non-zero is truthy (follows numeric EBV)
        FunctionName::T => {
            match eval_function_to_value(name, args, row)? {
                Some(ComparableValue::Long(t)) => Ok(t != 0),
                _ => Ok(false),
            }
        }

        // OP(?var) - Fluree-specific operation type for history queries
        // In boolean context: always truthy if op metadata exists (assert or retract)
        FunctionName::Op => {
            match eval_function_to_value(name, args, row)? {
                Some(ComparableValue::String(_)) => Ok(true),
                _ => Ok(false),
            }
        }

        FunctionName::Custom(name) => Err(QueryError::InvalidFilter(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use fluree_db_core::Sid;

    fn make_test_batch() -> Batch {
        // Schema: [?age (idx 0), ?name (idx 1)]
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());

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

        // Row 0: age=25 > 20 -> true
        assert!(evaluate(&expr, &batch.row_view(0).unwrap()).unwrap());
        // Row 1: age=30 > 20 -> true
        assert!(evaluate(&expr, &batch.row_view(1).unwrap()).unwrap());
        // Row 2: age=18 > 20 -> false
        assert!(!evaluate(&expr, &batch.row_view(2).unwrap()).unwrap());
        // Row 3: age=Unbound -> false
        assert!(!evaluate(&expr, &batch.row_view(3).unwrap()).unwrap());
    }

    #[test]
    fn test_evaluate_comparison_eq() {
        let batch = make_test_batch();

        // ?age = 30
        let expr = FilterExpr::Compare {
            op: CompareOp::Eq,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(30))),
        };

        assert!(!evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // 25 != 30
        assert!(evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // 30 == 30
        assert!(!evaluate(&expr, &batch.row_view(2).unwrap()).unwrap()); // 18 != 30
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

        assert!(evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // 25: 20 < 25 < 28
        assert!(!evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // 30: not < 28
        assert!(!evaluate(&expr, &batch.row_view(2).unwrap()).unwrap()); // 18: not > 20
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

        assert!(!evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // 25: neither
        assert!(evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // 30: > 28
        assert!(evaluate(&expr, &batch.row_view(2).unwrap()).unwrap()); // 18: < 20
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_test_batch();

        // NOT(?age > 20)
        let expr = FilterExpr::Not(Box::new(FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(20))),
        }));

        assert!(!evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // 25 > 20, NOT -> false
        assert!(!evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // 30 > 20, NOT -> false
        assert!(evaluate(&expr, &batch.row_view(2).unwrap()).unwrap()); // 18 not > 20, NOT -> true
    }

    #[test]
    fn test_evaluate_bound_function() {
        let batch = make_test_batch();

        // BOUND(?age)
        let expr = FilterExpr::Function {
            name: FunctionName::Bound,
            args: vec![FilterExpr::Var(VarId(0))],
        };

        assert!(evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // age=25 is bound
        assert!(evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // age=30 is bound
        assert!(!evaluate(&expr, &batch.row_view(3).unwrap()).unwrap()); // age=Unbound
    }

    #[test]
    fn test_evaluate_string_contains() {
        let batch = make_test_batch();

        // CONTAINS(?name, "li")
        let expr = FilterExpr::Function {
            name: FunctionName::Contains,
            args: vec![
                FilterExpr::Var(VarId(1)),
                FilterExpr::Const(FilterValue::String("li".to_string())),
            ],
        };

        assert!(evaluate(&expr, &batch.row_view(0).unwrap()).unwrap()); // "Alice" contains "li"
        assert!(!evaluate(&expr, &batch.row_view(1).unwrap()).unwrap()); // "Bob" doesn't contain "li"
    }

    #[test]
    fn test_compare_values_cross_type_numeric() {
        // Long vs Double
        assert!(compare_values(
            &ComparableValue::Long(10),
            &ComparableValue::Double(10.0),
            CompareOp::Eq
        ));
        assert!(compare_values(
            &ComparableValue::Long(10),
            &ComparableValue::Double(10.5),
            CompareOp::Lt
        ));
    }

    #[test]
    fn test_compare_values_type_mismatch() {
        // String vs Long -> only Ne is true
        assert!(!compare_values(
            &ComparableValue::String(Arc::from("10")),
            &ComparableValue::Long(10),
            CompareOp::Eq
        ));
        assert!(compare_values(
            &ComparableValue::String(Arc::from("10")),
            &ComparableValue::Long(10),
            CompareOp::Ne
        ));
    }

    #[test]
    fn test_evaluate_to_binding_arithmetic() {
        let batch = make_test_batch();

        // ?age + 10
        let expr = FilterExpr::Arithmetic {
            op: crate::ir::ArithmeticOp::Add,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(10))),
        };

        // Row 0: age=25, result should be 35
        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert!(result.is_lit());
        let (val, _, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(35));

        // Row 3: age=Unbound, result should be Unbound
        let result = evaluate_to_binding(&expr, &batch.row_view(3).unwrap());
        assert_eq!(result, Binding::Unbound);
    }

    #[test]
    fn test_evaluate_to_binding_constant() {
        let batch = make_test_batch();

        // Constant 42
        let expr = FilterExpr::Const(FilterValue::Long(42));

        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert!(result.is_lit());
        let (val, _, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(42));
    }

    #[test]
    fn test_evaluate_to_binding_string() {
        let batch = make_test_batch();

        // String constant
        let expr = FilterExpr::Const(FilterValue::String("hello".to_string()));

        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert!(result.is_lit());
        let (val, dt, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::String("hello".to_string()));
        assert_eq!(dt.name.as_ref(), "string");
    }

    #[test]
    fn test_evaluate_to_binding_comparison_bool() {
        let batch = make_test_batch();

        // ?age > 20 evaluates to bool
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(20))),
        };

        // Row 0: age=25 > 20 -> true
        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert!(result.is_lit());
        let (val, _, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Boolean(true));

        // Row 2: age=18 > 20 -> false
        let result = evaluate_to_binding(&expr, &batch.row_view(2).unwrap());
        let (val, _, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Boolean(false));
    }

    #[test]
    fn test_evaluate_to_binding_var_direct() {
        let batch = make_test_batch();

        // Just return the variable's value
        let expr = FilterExpr::Var(VarId(0));  // ?age

        // Row 0: age=25
        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert!(result.is_lit());
        let (val, _, _) = result.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(25));

        // Row 3: age=Unbound
        let result = evaluate_to_binding(&expr, &batch.row_view(3).unwrap());
        assert_eq!(result, Binding::Unbound);
    }

    #[test]
    fn test_evaluate_to_binding_error_returns_unbound() {
        let batch = make_test_batch();

        // Division by zero
        let expr = FilterExpr::Arithmetic {
            op: crate::ir::ArithmeticOp::Div,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(0))),
        };

        // Should return Unbound, NOT Poisoned
        let result = evaluate_to_binding(&expr, &batch.row_view(0).unwrap());
        assert_eq!(result, Binding::Unbound);
        assert!(!result.is_poisoned());
    }
}
