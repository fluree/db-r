//! ComparableValue type and conversions
//!
//! This module contains the intermediate value type used during filter evaluation,
//! along with conversions to/from FlakeValue and FilterValue.

use crate::ir::{ArithmeticOp, FilterValue};
use bigdecimal::BigDecimal;
use fluree_db_core::temporal::{
    Date as FlureeDate, DateTime as FlureeDateTime, Time as FlureeTime,
};
use fluree_db_core::{FlakeValue, GeoPointBits};
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;

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
                val: FlakeValue::String(s),
                ..
            } => Some(s.as_str()),
            _ => None,
        }
    }

    /// Convert this value to a string-typed ComparableValue (for STR function).
    ///
    /// Consumes self and returns a `ComparableValue::String` containing the
    /// string representation of the value. Returns `None` for types that
    /// cannot be converted to strings (e.g., vectors).
    pub fn into_string_value(self) -> Option<ComparableValue> {
        match self {
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
            ComparableValue::DateTime(dt) => {
                Some(ComparableValue::String(Arc::from(dt.to_string())))
            }
            ComparableValue::Date(d) => Some(ComparableValue::String(Arc::from(d.to_string()))),
            ComparableValue::Time(t) => Some(ComparableValue::String(Arc::from(t.to_string()))),
            ComparableValue::GeoPoint(bits) => {
                // Convert to WKT format: POINT(lng lat)
                Some(ComparableValue::String(Arc::from(bits.to_string())))
            }
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


    /// Apply an arithmetic operation between this value and another.
    ///
    /// Returns `None` if the operation cannot be performed (e.g., type mismatch,
    /// division by zero, or non-numeric types).
    pub fn apply_arithmetic(self, op: ArithmeticOp, other: ComparableValue) -> Option<Self> {
        match (self, other) {
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
                ComparableValue::Double(a as f64).apply_arithmetic(op, ComparableValue::Double(b))
            }
            (ComparableValue::Double(a), ComparableValue::Long(b)) => {
                ComparableValue::Double(a).apply_arithmetic(op, ComparableValue::Double(b as f64))
            }
            // Long <-> BigInt -> BigInt
            (ComparableValue::Long(a), ComparableValue::BigInt(b)) => {
                ComparableValue::BigInt(Box::new(BigInt::from(a)))
                    .apply_arithmetic(op, ComparableValue::BigInt(b))
            }
            (ComparableValue::BigInt(a), ComparableValue::Long(b)) => ComparableValue::BigInt(a)
                .apply_arithmetic(op, ComparableValue::BigInt(Box::new(BigInt::from(b)))),
            // Long <-> Decimal -> Decimal
            (ComparableValue::Long(a), ComparableValue::Decimal(b)) => {
                ComparableValue::Decimal(Box::new(BigDecimal::from(a)))
                    .apply_arithmetic(op, ComparableValue::Decimal(b))
            }
            (ComparableValue::Decimal(a), ComparableValue::Long(b)) => ComparableValue::Decimal(a)
                .apply_arithmetic(op, ComparableValue::Decimal(Box::new(BigDecimal::from(b)))),
            // BigInt <-> Decimal -> Decimal
            (ComparableValue::BigInt(a), ComparableValue::Decimal(b)) => {
                ComparableValue::Decimal(Box::new(BigDecimal::from((*a).clone())))
                    .apply_arithmetic(op, ComparableValue::Decimal(b))
            }
            (ComparableValue::Decimal(a), ComparableValue::BigInt(b)) => {
                ComparableValue::Decimal(a).apply_arithmetic(
                    op,
                    ComparableValue::Decimal(Box::new(BigDecimal::from((*b).clone()))),
                )
            }
            // Double <-> BigInt -> Double (lossy)
            (ComparableValue::Double(a), ComparableValue::BigInt(b)) => {
                use num_traits::ToPrimitive;
                b.to_f64().and_then(|bf| {
                    ComparableValue::Double(a).apply_arithmetic(op, ComparableValue::Double(bf))
                })
            }
            (ComparableValue::BigInt(a), ComparableValue::Double(b)) => {
                use num_traits::ToPrimitive;
                a.to_f64().and_then(|af| {
                    ComparableValue::Double(af).apply_arithmetic(op, ComparableValue::Double(b))
                })
            }
            // Double <-> Decimal -> Decimal (if possible)
            (ComparableValue::Double(a), ComparableValue::Decimal(b)) => {
                BigDecimal::try_from(a).ok().and_then(|ad| {
                    ComparableValue::Decimal(Box::new(ad))
                        .apply_arithmetic(op, ComparableValue::Decimal(b))
                })
            }
            (ComparableValue::Decimal(a), ComparableValue::Double(b)) => {
                BigDecimal::try_from(b).ok().and_then(|bd| {
                    ComparableValue::Decimal(a)
                        .apply_arithmetic(op, ComparableValue::Decimal(Box::new(bd)))
                })
            }
            // Non-numeric types can't do arithmetic
            _ => None,
        }
    }

    /// Convert a FlakeValue to a ComparableValue.
    ///
    /// Returns `None` for `FlakeValue::Null`.
    pub fn from_flake_value(val: &FlakeValue) -> Option<ComparableValue> {
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
            FlakeValue::GeoPoint(bits) => Some(ComparableValue::GeoPoint(*bits)),
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
}

impl From<&FilterValue> for ComparableValue {
    fn from(val: &FilterValue) -> Self {
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
            ComparableValue::GeoPoint(bits) => FlakeValue::GeoPoint(*bits),
            ComparableValue::Iri(s) => FlakeValue::String(s.to_string()),
            ComparableValue::TypedLiteral { val, .. } => val.clone(),
        }
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
            a.clone().apply_arithmetic(ArithmeticOp::Add, b.clone()),
            Some(ComparableValue::Long(13))
        );
        assert_eq!(
            a.clone().apply_arithmetic(ArithmeticOp::Sub, b.clone()),
            Some(ComparableValue::Long(7))
        );
        assert_eq!(
            a.clone().apply_arithmetic(ArithmeticOp::Mul, b.clone()),
            Some(ComparableValue::Long(30))
        );
        assert_eq!(
            a.apply_arithmetic(ArithmeticOp::Div, b),
            Some(ComparableValue::Long(3))
        );
    }

    #[test]
    fn test_arithmetic_div_by_zero() {
        assert_eq!(
            ComparableValue::Long(10).apply_arithmetic(ArithmeticOp::Div, ComparableValue::Long(0)),
            None
        );
    }

    #[test]
    fn test_from_flake_value() {
        let fv = FlakeValue::Long(42);
        let cv = ComparableValue::from_flake_value(&fv);
        assert_eq!(cv, Some(ComparableValue::Long(42)));

        let fv_null = FlakeValue::Null;
        let cv_null = ComparableValue::from_flake_value(&fv_null);
        assert_eq!(cv_null, None);
    }

    #[test]
    fn test_from_filter_value() {
        let fv = FilterValue::String("hello".to_string());
        let cv: ComparableValue = (&fv).into();
        assert_eq!(cv, ComparableValue::String(Arc::from("hello")));
    }

    #[test]
    fn test_into_flake_value() {
        let cv = ComparableValue::Long(42);
        let fv: FlakeValue = (&cv).into();
        assert_eq!(fv, FlakeValue::Long(42));
    }

    #[test]
    fn test_as_str() {
        let cv = ComparableValue::String(Arc::from("hello"));
        assert_eq!(cv.as_str(), Some("hello"));

        let cv_long = ComparableValue::Long(42);
        assert_eq!(cv_long.as_str(), None);
    }

    #[test]
    fn test_into_string_value() {
        let cv = ComparableValue::Long(42);
        let sv = cv.into_string_value();
        assert_eq!(sv, Some(ComparableValue::String(Arc::from("42"))));
    }
}
