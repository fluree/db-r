//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use bigdecimal::{BigDecimal, RoundingMode};
use num_traits::ToPrimitive;
use rand::random;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_abs<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ABS")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n.abs()))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.abs()))),
        Some(ComparableValue::Decimal(d)) => Ok(Some(ComparableValue::Decimal(Box::new(d.abs())))),
        Some(ComparableValue::BigInt(n)) => Ok(Some(ComparableValue::BigInt(Box::new(
            n.magnitude().clone().into(),
        )))),
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_round<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ROUND")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => {
            // W3C: round half toward positive infinity (not away from zero).
            // f64::round() rounds half away from zero, which is wrong for
            // negative values (e.g., -2.5 → -3 instead of -2).
            Ok(Some(ComparableValue::Double((d + 0.5).floor())))
        }
        Some(ComparableValue::Decimal(d)) => {
            // W3C: round half toward positive infinity.
            // RoundingMode::HalfUp rounds half away from zero, which is wrong
            // for negative values. Instead: add 0.5 then floor.
            let half = BigDecimal::new(5.into(), 1); // 0.5
            let rounded = (&*d + &half).with_scale_round(0, RoundingMode::Floor);
            Ok(Some(ComparableValue::Decimal(Box::new(rounded))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_ceil<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "CEIL")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.ceil()))),
        Some(ComparableValue::Decimal(d)) => {
            let ceiled = d.with_scale_round(0, RoundingMode::Ceiling);
            Ok(Some(ComparableValue::Decimal(Box::new(ceiled))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_floor<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "FLOOR")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::Long(n)) => Ok(Some(ComparableValue::Long(n))),
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.floor()))),
        Some(ComparableValue::Decimal(d)) => {
            let floored = d.with_scale_round(0, RoundingMode::Floor);
            Ok(Some(ComparableValue::Decimal(Box::new(floored))))
        }
        None => Ok(None),
        Some(_) => Ok(None),
    }
}

pub fn eval_rand(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "RAND")?;
    Ok(Some(ComparableValue::Double(random::<f64>())))
}

pub fn eval_xsd_integer<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "xsd:integer")?;
    let value = args[0].eval_to_comparable(row, ctx)?;

    let Some(v) = value else {
        return Ok(None);
    };

    let n = match v {
        ComparableValue::Long(n) => n,
        ComparableValue::Bool(b) => i64::from(b),
        ComparableValue::Double(d) => {
            if !d.is_finite() {
                return Err(QueryError::InvalidFilter(
                    "xsd:integer cannot cast NaN or infinite values".to_string(),
                ));
            }
            if d > i64::MAX as f64 || d < i64::MIN as f64 {
                return Err(QueryError::InvalidFilter(
                    "xsd:integer cast overflow".to_string(),
                ));
            }
            d.trunc() as i64
        }
        ComparableValue::BigInt(bi) => bi
            .to_i64()
            .ok_or_else(|| QueryError::InvalidFilter("xsd:integer cast overflow".to_string()))?,
        ComparableValue::Decimal(dec) => {
            // Prefer exact integral conversion; fall back to truncation if needed.
            if let Some(i) = dec.to_i64() {
                i
            } else if let Some(f) = dec.to_f64() {
                if !f.is_finite() || f > i64::MAX as f64 || f < i64::MIN as f64 {
                    return Err(QueryError::InvalidFilter(
                        "xsd:integer cast overflow".to_string(),
                    ));
                }
                f.trunc() as i64
            } else {
                return Err(QueryError::InvalidFilter(
                    "xsd:integer cannot cast decimal value".to_string(),
                ));
            }
        }
        ComparableValue::String(s) => s.parse::<i64>().map_err(|_| {
            QueryError::InvalidFilter("xsd:integer requires an integer lexical value".to_string())
        })?,
        ComparableValue::TypedLiteral { val, .. } => match val {
            fluree_db_core::FlakeValue::Long(n) => n,
            fluree_db_core::FlakeValue::Boolean(b) => i64::from(b),
            fluree_db_core::FlakeValue::Double(d) => {
                if !d.is_finite() || d > i64::MAX as f64 || d < i64::MIN as f64 {
                    return Err(QueryError::InvalidFilter(
                        "xsd:integer cast overflow".to_string(),
                    ));
                }
                d.trunc() as i64
            }
            fluree_db_core::FlakeValue::String(s) => s.parse::<i64>().map_err(|_| {
                QueryError::InvalidFilter(
                    "xsd:integer requires an integer lexical value".to_string(),
                )
            })?,
            other => {
                return Err(QueryError::InvalidFilter(format!(
                    "xsd:integer cannot cast typed literal {:?}",
                    other
                )))
            }
        },
        other => {
            return Err(QueryError::InvalidFilter(format!(
                "xsd:integer cannot cast {}",
                other.type_name()
            )))
        }
    };

    Ok(Some(ComparableValue::Long(n)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::BindingRow;
    use crate::ir::FilterValue;

    fn long(v: i64) -> Expression {
        Expression::Const(FilterValue::Long(v))
    }

    fn double(v: f64) -> Expression {
        Expression::Const(FilterValue::Double(v))
    }

    fn bool_expr(v: bool) -> Expression {
        Expression::Const(FilterValue::Bool(v))
    }

    fn string(s: &str) -> Expression {
        Expression::Const(FilterValue::String(s.to_string()))
    }

    fn empty_row() -> BindingRow<'static> {
        BindingRow::new(&[], &[])
    }

    // -- identity / passthrough --

    #[test]
    fn xsd_integer_from_long() {
        let row = empty_row();
        let result = eval_xsd_integer(&[long(42)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(42)));
    }

    #[test]
    fn xsd_integer_from_negative_long() {
        let row = empty_row();
        let result = eval_xsd_integer(&[long(-7)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(-7)));
    }

    // -- bool → 0/1 --

    #[test]
    fn xsd_integer_from_true() {
        let row = empty_row();
        let result = eval_xsd_integer(&[bool_expr(true)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(1)));
    }

    #[test]
    fn xsd_integer_from_false() {
        let row = empty_row();
        let result = eval_xsd_integer(&[bool_expr(false)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(0)));
    }

    // -- double truncation --

    #[test]
    fn xsd_integer_from_double_truncates() {
        let row = empty_row();
        let result = eval_xsd_integer(&[double(3.9)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(3)));
    }

    #[test]
    fn xsd_integer_from_negative_double_truncates() {
        let row = empty_row();
        let result = eval_xsd_integer(&[double(-2.7)], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(-2)));
    }

    #[test]
    fn xsd_integer_from_nan_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[double(f64::NAN)], &row, None);
        assert!(result.is_err());
    }

    #[test]
    fn xsd_integer_from_infinity_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[double(f64::INFINITY)], &row, None);
        assert!(result.is_err());
    }

    // -- string parsing --

    #[test]
    fn xsd_integer_from_string() {
        let row = empty_row();
        let result = eval_xsd_integer(&[string("123")], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(123)));
    }

    #[test]
    fn xsd_integer_from_non_numeric_string_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[string("abc")], &row, None);
        assert!(result.is_err());
    }

    #[test]
    fn xsd_integer_from_float_string_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[string("3.14")], &row, None);
        assert!(result.is_err());
    }

    // -- arity --

    #[test]
    fn xsd_integer_wrong_arity_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[long(1), long(2)], &row, None);
        assert!(result.is_err());
    }

    #[test]
    fn xsd_integer_zero_args_errors() {
        let row = empty_row();
        let result = eval_xsd_integer(&[], &row, None);
        assert!(result.is_err());
    }
}
