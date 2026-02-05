//! ComparableValue type and conversions
//!
//! This module contains the intermediate value type used during filter evaluation,
//! along with conversions to/from FlakeValue and FilterValue.

use crate::ir::FilterValue;
use bigdecimal::BigDecimal;
use fluree_db_core::temporal::{
    Date as FlureeDate, DateTime as FlureeDateTime, Time as FlureeTime,
};
use fluree_db_core::FlakeValue;
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;

use crate::ir::ArithmeticOp;

/// Comparable value extracted from expression evaluation
///
/// This is the internal representation used during filter evaluation.
/// It normalizes different binding types into a common format for comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum ComparableValue {
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
    /// Effective Boolean Value (EBV) for two-valued filter logic.
    ///
    /// Returns `true` for "truthy" values, `false` for "falsy" values.
    /// This is used when a value appears in boolean context (e.g., FILTER(?x)).
    ///
    /// EBV rules:
    /// - Bool(b) → b
    /// - Long(n) → n != 0
    /// - Double(d) → !d.is_nan() && d != 0.0
    /// - String(s) → !s.is_empty()
    /// - All other non-null values → true
    pub fn ebv(&self) -> bool {
        match self {
            ComparableValue::Bool(b) => *b,
            ComparableValue::Long(n) => *n != 0,
            ComparableValue::Double(d) => !d.is_nan() && *d != 0.0,
            ComparableValue::String(s) => !s.is_empty(),
            ComparableValue::BigInt(n) => !n.is_zero(),
            ComparableValue::Decimal(d) => !d.is_zero(),
            // IRIs, Sids, temporal values, vectors are truthy
            _ => true,
        }
    }
}

/// Convert FlakeValue to ComparableValue
pub fn flake_value_to_comparable(val: &FlakeValue) -> Option<ComparableValue> {
    match val {
        FlakeValue::Long(n) => Some(ComparableValue::Long(*n)),
        FlakeValue::Double(d) => Some(ComparableValue::Double(*d)),
        FlakeValue::String(s) => Some(ComparableValue::String(Arc::from(s.as_str()))),
        FlakeValue::Json(s) => Some(ComparableValue::String(Arc::from(s.as_str()))), // JSON compared as string
        FlakeValue::Boolean(b) => Some(ComparableValue::Bool(*b)),
        FlakeValue::Ref(sid) => Some(ComparableValue::Sid(sid.clone())),
        FlakeValue::Null => None,
        FlakeValue::Vector(v) => Some(ComparableValue::Vector(Arc::from(v.as_slice()))),
        // Extended numeric types
        FlakeValue::BigInt(n) => Some(ComparableValue::BigInt(n.clone())),
        FlakeValue::Decimal(d) => Some(ComparableValue::Decimal(d.clone())),
        // Temporal types
        FlakeValue::DateTime(dt) => Some(ComparableValue::DateTime(dt.clone())),
        FlakeValue::Date(d) => Some(ComparableValue::Date(d.clone())),
        FlakeValue::Time(t) => Some(ComparableValue::Time(t.clone())),
        // Calendar fragments and durations: route through TypedLiteral
        FlakeValue::GYear(_)
        | FlakeValue::GYearMonth(_)
        | FlakeValue::GMonth(_)
        | FlakeValue::GDay(_)
        | FlakeValue::GMonthDay(_)
        | FlakeValue::YearMonthDuration(_)
        | FlakeValue::DayTimeDuration(_)
        | FlakeValue::Duration(_) => Some(ComparableValue::TypedLiteral {
            val: val.clone(),
            dt_iri: None,
            lang: None,
        }),
    }
}

/// Convert FilterValue (AST constant) to ComparableValue
pub fn filter_value_to_comparable(val: &FilterValue) -> ComparableValue {
    match val {
        FilterValue::Long(n) => ComparableValue::Long(*n),
        FilterValue::Double(d) => ComparableValue::Double(*d),
        FilterValue::String(s) => ComparableValue::String(Arc::from(s.as_str())),
        FilterValue::Bool(b) => ComparableValue::Bool(*b),
        FilterValue::Temporal(fv) => ComparableValue::TypedLiteral {
            val: fv.clone(),
            dt_iri: None,
            lang: None,
        },
    }
}

/// Convert ComparableValue back to FlakeValue for comparison delegation
pub fn comparable_to_flake(val: &ComparableValue) -> FlakeValue {
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

/// Helper to extract a string from a comparable value
pub fn comparable_to_string(val: &ComparableValue) -> Option<&str> {
    match val {
        ComparableValue::String(s) => Some(s.as_ref()),
        ComparableValue::Iri(s) => Some(s.as_ref()),
        ComparableValue::TypedLiteral { val, .. } => match val {
            FlakeValue::String(s) => Some(s.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// Convert a comparable value to a string-typed comparable value (for STR function)
pub fn comparable_to_str_value(val: ComparableValue) -> Option<ComparableValue> {
    match val {
        ComparableValue::String(s) => Some(ComparableValue::String(s)),
        ComparableValue::Iri(s) => Some(ComparableValue::String(s)),
        ComparableValue::Sid(sid) => Some(ComparableValue::String(Arc::from(format!(
            "{}:{}",
            sid.namespace_code, sid.name
        )))),
        ComparableValue::Long(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
        ComparableValue::Double(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
        ComparableValue::Bool(b) => Some(ComparableValue::String(Arc::from(b.to_string()))),
        ComparableValue::BigInt(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
        ComparableValue::Decimal(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
        ComparableValue::DateTime(dt) => Some(ComparableValue::String(Arc::from(dt.to_string()))),
        ComparableValue::Date(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
        ComparableValue::Time(t) => Some(ComparableValue::String(Arc::from(t.to_string()))),
        ComparableValue::Vector(_) => None, // Vectors don't have a string representation
        ComparableValue::TypedLiteral { val, .. } => match val {
            FlakeValue::String(s) => Some(ComparableValue::String(Arc::from(s))),
            FlakeValue::Long(n) => Some(ComparableValue::String(Arc::from(n.to_string()))),
            FlakeValue::Double(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
            FlakeValue::Boolean(b) => Some(ComparableValue::String(Arc::from(b.to_string()))),
            _ => None,
        },
    }
}

/// Evaluate arithmetic operation on two comparable values
pub fn eval_arithmetic(
    op: ArithmeticOp,
    left: ComparableValue,
    right: ComparableValue,
) -> Option<ComparableValue> {
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
        (ComparableValue::Long(a), ComparableValue::Double(b)) => eval_arithmetic(
            op,
            ComparableValue::Double(a as f64),
            ComparableValue::Double(b),
        ),
        (ComparableValue::Double(a), ComparableValue::Long(b)) => eval_arithmetic(
            op,
            ComparableValue::Double(a),
            ComparableValue::Double(b as f64),
        ),
        // Long <-> BigInt -> BigInt
        (ComparableValue::Long(a), ComparableValue::BigInt(b)) => eval_arithmetic(
            op,
            ComparableValue::BigInt(Box::new(BigInt::from(a))),
            ComparableValue::BigInt(b),
        ),
        (ComparableValue::BigInt(a), ComparableValue::Long(b)) => eval_arithmetic(
            op,
            ComparableValue::BigInt(a),
            ComparableValue::BigInt(Box::new(BigInt::from(b))),
        ),
        // Long <-> Decimal -> Decimal
        (ComparableValue::Long(a), ComparableValue::Decimal(b)) => eval_arithmetic(
            op,
            ComparableValue::Decimal(Box::new(BigDecimal::from(a))),
            ComparableValue::Decimal(b),
        ),
        (ComparableValue::Decimal(a), ComparableValue::Long(b)) => eval_arithmetic(
            op,
            ComparableValue::Decimal(a),
            ComparableValue::Decimal(Box::new(BigDecimal::from(b))),
        ),
        // BigInt <-> Decimal -> Decimal
        (ComparableValue::BigInt(a), ComparableValue::Decimal(b)) => eval_arithmetic(
            op,
            ComparableValue::Decimal(Box::new(BigDecimal::from((*a).clone()))),
            ComparableValue::Decimal(b),
        ),
        (ComparableValue::Decimal(a), ComparableValue::BigInt(b)) => eval_arithmetic(
            op,
            ComparableValue::Decimal(a),
            ComparableValue::Decimal(Box::new(BigDecimal::from((*b).clone()))),
        ),
        // Double <-> BigInt -> Double (lossy)
        (ComparableValue::Double(a), ComparableValue::BigInt(b)) => {
            use num_traits::ToPrimitive;
            b.to_f64().and_then(|bf| {
                eval_arithmetic(op, ComparableValue::Double(a), ComparableValue::Double(bf))
            })
        }
        (ComparableValue::BigInt(a), ComparableValue::Double(b)) => {
            use num_traits::ToPrimitive;
            a.to_f64().and_then(|af| {
                eval_arithmetic(op, ComparableValue::Double(af), ComparableValue::Double(b))
            })
        }
        // Double <-> Decimal -> Decimal (if possible)
        (ComparableValue::Double(a), ComparableValue::Decimal(b)) => {
            BigDecimal::try_from(a).ok().and_then(|ad| {
                eval_arithmetic(
                    op,
                    ComparableValue::Decimal(Box::new(ad)),
                    ComparableValue::Decimal(b),
                )
            })
        }
        (ComparableValue::Decimal(a), ComparableValue::Double(b)) => {
            BigDecimal::try_from(b).ok().and_then(|bd| {
                eval_arithmetic(
                    op,
                    ComparableValue::Decimal(a),
                    ComparableValue::Decimal(Box::new(bd)),
                )
            })
        }
        // Non-numeric types can't do arithmetic
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ebv_bool() {
        assert!(ComparableValue::Bool(true).ebv());
        assert!(!ComparableValue::Bool(false).ebv());
    }

    #[test]
    fn test_ebv_numeric() {
        assert!(ComparableValue::Long(1).ebv());
        assert!(!ComparableValue::Long(0).ebv());
        assert!(ComparableValue::Double(0.1).ebv());
        assert!(!ComparableValue::Double(0.0).ebv());
        assert!(!ComparableValue::Double(f64::NAN).ebv());
    }

    #[test]
    fn test_ebv_string() {
        assert!(ComparableValue::String(Arc::from("hello")).ebv());
        assert!(!ComparableValue::String(Arc::from("")).ebv());
    }

    #[test]
    fn test_arithmetic_long() {
        let a = ComparableValue::Long(10);
        let b = ComparableValue::Long(3);
        assert_eq!(
            eval_arithmetic(ArithmeticOp::Add, a.clone(), b.clone()),
            Some(ComparableValue::Long(13))
        );
        assert_eq!(
            eval_arithmetic(ArithmeticOp::Sub, a.clone(), b.clone()),
            Some(ComparableValue::Long(7))
        );
        assert_eq!(
            eval_arithmetic(ArithmeticOp::Mul, a.clone(), b.clone()),
            Some(ComparableValue::Long(30))
        );
        assert_eq!(
            eval_arithmetic(ArithmeticOp::Div, a, b),
            Some(ComparableValue::Long(3))
        );
    }

    #[test]
    fn test_arithmetic_div_by_zero() {
        assert_eq!(
            eval_arithmetic(
                ArithmeticOp::Div,
                ComparableValue::Long(10),
                ComparableValue::Long(0)
            ),
            None
        );
    }
}
