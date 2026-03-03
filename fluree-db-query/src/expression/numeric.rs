//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
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
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "ABS requires a numeric argument".to_string(),
        )),
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
        Some(ComparableValue::Double(d)) => Ok(Some(ComparableValue::Double(d.round()))),
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "ROUND requires a numeric argument".to_string(),
        )),
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
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "CEIL requires a numeric argument".to_string(),
        )),
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
        None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "FLOOR requires a numeric argument".to_string(),
        )),
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
        ComparableValue::BigInt(bi) => bi.to_i64().ok_or_else(|| {
            QueryError::InvalidFilter("xsd:integer cast overflow".to_string())
        })?,
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
