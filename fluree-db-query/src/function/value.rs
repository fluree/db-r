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

impl ArithmeticOp {
    /// Apply this arithmetic operation to two ComparableValue operands.
    ///
    /// Returns `None` if the operation cannot be performed (e.g., type mismatch,
    /// division by zero, or non-numeric types).
    pub fn apply(self, left: ComparableValue, right: ComparableValue) -> Option<ComparableValue> {
        use num_traits::ToPrimitive;

        match (left, right) {
            // Long + Long = Long
            (ComparableValue::Long(a), ComparableValue::Long(b)) => {
                let result = match self {
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
                let result = match self {
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
                let result = match self {
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
                let result = match self {
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
                self.apply(ComparableValue::Double(a as f64), ComparableValue::Double(b))
            }
            (ComparableValue::Double(a), ComparableValue::Long(b)) => {
                self.apply(ComparableValue::Double(a), ComparableValue::Double(b as f64))
            }
            // Long <-> BigInt -> BigInt
            (ComparableValue::Long(a), ComparableValue::BigInt(b)) => self.apply(
                ComparableValue::BigInt(Box::new(BigInt::from(a))),
                ComparableValue::BigInt(b),
            ),
            (ComparableValue::BigInt(a), ComparableValue::Long(b)) => self.apply(
                ComparableValue::BigInt(a),
                ComparableValue::BigInt(Box::new(BigInt::from(b))),
            ),
            // Long <-> Decimal -> Decimal
            (ComparableValue::Long(a), ComparableValue::Decimal(b)) => self.apply(
                ComparableValue::Decimal(Box::new(BigDecimal::from(a))),
                ComparableValue::Decimal(b),
            ),
            (ComparableValue::Decimal(a), ComparableValue::Long(b)) => self.apply(
                ComparableValue::Decimal(a),
                ComparableValue::Decimal(Box::new(BigDecimal::from(b))),
            ),
            // BigInt <-> Decimal -> Decimal
            (ComparableValue::BigInt(a), ComparableValue::Decimal(b)) => self.apply(
                ComparableValue::Decimal(Box::new(BigDecimal::from((*a).clone()))),
                ComparableValue::Decimal(b),
            ),
            (ComparableValue::Decimal(a), ComparableValue::BigInt(b)) => self.apply(
                ComparableValue::Decimal(a),
                ComparableValue::Decimal(Box::new(BigDecimal::from((*b).clone()))),
            ),
            // Double <-> BigInt -> Double (lossy)
            (ComparableValue::Double(a), ComparableValue::BigInt(b)) => b
                .to_f64()
                .and_then(|bf| self.apply(ComparableValue::Double(a), ComparableValue::Double(bf))),
            (ComparableValue::BigInt(a), ComparableValue::Double(b)) => a
                .to_f64()
                .and_then(|af| self.apply(ComparableValue::Double(af), ComparableValue::Double(b))),
            // Double <-> Decimal -> Decimal (if possible)
            (ComparableValue::Double(a), ComparableValue::Decimal(b)) => {
                BigDecimal::try_from(a).ok().and_then(|ad| {
                    self.apply(
                        ComparableValue::Decimal(Box::new(ad)),
                        ComparableValue::Decimal(b),
                    )
                })
            }
            (ComparableValue::Decimal(a), ComparableValue::Double(b)) => {
                BigDecimal::try_from(b).ok().and_then(|bd| {
                    self.apply(
                        ComparableValue::Decimal(a),
                        ComparableValue::Decimal(Box::new(bd)),
                    )
                })
            }
            // Non-numeric types can't do arithmetic
            _ => None,
        }
    }
}

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
    DateTime(FlureeDateTime),
    Date(FlureeDate),
    Time(FlureeTime),
    // Geo types
    GeoPoint(GeoPointBits),
    // IRI/URI
    Iri(Arc<str>),
    // Typed literal with optional datatype IRI and language tag
    TypedLiteral {
        val: FlakeValue,
        dt_iri: Option<Arc<str>>,
        lang: Option<Arc<str>>,
    },
}

impl ComparableValue {
    /// Compute the Effective Boolean Value (EBV) of this value.
    ///
    /// EBV is used in SPARQL FILTER and conditional expressions.
    /// See: <https://www.w3.org/TR/sparql11-query/#ebv>
    pub fn ebv(&self) -> bool {
        match self {
            ComparableValue::Bool(b) => *b,
            ComparableValue::String(s) => !s.is_empty(),
            ComparableValue::Iri(s) => !s.is_empty(),
            ComparableValue::Long(n) => *n != 0,
            ComparableValue::Double(d) => !d.is_nan() && *d != 0.0,
            ComparableValue::BigInt(n) => !n.is_zero(),
            ComparableValue::Decimal(d) => !d.is_zero(),
            // Other types: Sid, Vector, DateTime, etc. are truthy if present
            _ => true,
        }
    }

    /// Get a string slice if this value is a String, Iri, or TypedLiteral containing a string.
    pub fn as_str(&self) -> Option<&str> {
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
            FlakeValue::DateTime(dt) => Some(ComparableValue::DateTime(dt.as_ref().clone())),
            FlakeValue::Date(d) => Some(ComparableValue::Date(d.as_ref().clone())),
            FlakeValue::Time(t) => Some(ComparableValue::Time(t.as_ref().clone())),
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
            ComparableValue::DateTime(dt) => FlakeValue::DateTime(Box::new(dt.clone())),
            ComparableValue::Date(d) => FlakeValue::Date(Box::new(d.clone())),
            ComparableValue::Time(t) => FlakeValue::Time(Box::new(t.clone())),
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
            ArithmeticOp::Add.apply(a.clone(), b.clone()),
            Some(ComparableValue::Long(13))
        );
        assert_eq!(
            ArithmeticOp::Sub.apply(a.clone(), b.clone()),
            Some(ComparableValue::Long(7))
        );
        assert_eq!(
            ArithmeticOp::Mul.apply(a.clone(), b.clone()),
            Some(ComparableValue::Long(30))
        );
        assert_eq!(
            ArithmeticOp::Div.apply(a, b),
            Some(ComparableValue::Long(3))
        );
    }

    #[test]
    fn test_arithmetic_div_by_zero() {
        assert_eq!(
            ArithmeticOp::Div.apply(ComparableValue::Long(10), ComparableValue::Long(0)),
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
