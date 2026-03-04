//! DateTime function implementations
//!
//! Implements SPARQL datetime functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ, TIMEZONE

use crate::binding::RowAccess;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::DateTime as FlureeDateTime;
use std::sync::Arc;

use super::helpers::{check_arity, parse_datetime_from_binding};
use super::value::ComparableValue;
use crate::triple::DatatypeIriConstraint;

pub fn eval_now(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "NOW")?;
    let now = Utc::now();
    let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let parsed = FlureeDateTime::parse(&formatted)
        .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {}", e)))?;
    Ok(Some(ComparableValue::DateTime(parsed)))
}

pub fn eval_year<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "YEAR", |dt| dt.year() as i64)
}

pub fn eval_month<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "MONTH", |dt| dt.month() as i64)
}

pub fn eval_day<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "DAY", |dt| dt.day() as i64)
}

pub fn eval_hours<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "HOURS", |dt| dt.hour() as i64)
}

pub fn eval_minutes<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "MINUTES", |dt| dt.minute() as i64)
}

pub fn eval_seconds<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    // W3C: SECONDS returns xsd:decimal (fractional seconds)
    check_arity(args, 1, "SECONDS")?;
    if let Expression::Var(var) = &args[0] {
        match row.get(*var) {
            Some(binding) => match parse_datetime_from_binding(binding) {
                Some(dt) => {
                    let secs = dt.second() as i64;
                    let nanos = dt.nanosecond() as i64;
                    let decimal = if nanos == 0 {
                        BigDecimal::from(secs)
                    } else {
                        let total_nanos = secs * 1_000_000_000 + nanos;
                        BigDecimal::new(total_nanos.into(), 9)
                    };
                    Ok(Some(ComparableValue::Decimal(Box::new(decimal))))
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "SECONDS requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_tz<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "TZ")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => {
                let has_tz = has_timezone_info(binding);
                match parse_datetime_from_binding(binding) {
                    Some(dt) => {
                        if !has_tz {
                            // No timezone info in the original value
                            Ok(Some(ComparableValue::String(Arc::from(""))))
                        } else {
                            let total_secs = dt.offset().local_minus_utc();
                            if total_secs == 0 {
                                Ok(Some(ComparableValue::String(Arc::from("Z"))))
                            } else {
                                let hours = total_secs / 3600;
                                let mins = (total_secs.abs() % 3600) / 60;
                                let sign = if total_secs >= 0 { '+' } else { '-' };
                                let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                                Ok(Some(ComparableValue::String(Arc::from(tz_str))))
                            }
                        }
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TZ requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_timezone<R: RowAccess>(
    args: &[Expression],
    row: &R,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "TIMEZONE")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => {
                let has_tz = has_timezone_info(binding);
                match parse_datetime_from_binding(binding) {
                    Some(dt) => {
                        if !has_tz {
                            // No timezone → unbound per W3C
                            return Ok(None);
                        }
                        let total_secs = dt.offset().local_minus_utc();
                        let duration = format_day_time_duration(total_secs);
                        Ok(Some(ComparableValue::TypedLiteral {
                            val: fluree_db_core::FlakeValue::String(duration),
                            dtc: Some(DatatypeIriConstraint::Explicit(Arc::from(
                                "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
                            ))),
                        }))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TIMEZONE requires a variable argument".to_string(),
        ))
    }
}

/// Check if a datetime binding carries explicit timezone information.
fn has_timezone_info(binding: &crate::binding::Binding) -> bool {
    use crate::binding::Binding;
    match binding {
        Binding::Lit {
            val: fluree_db_core::FlakeValue::DateTime(dt),
            ..
        } => dt.tz_offset().is_some(),
        _ => false,
    }
}

/// Format seconds as xsd:dayTimeDuration: "PT0S", "-PT8H", "PT5H30M", etc.
fn format_day_time_duration(total_secs: i32) -> String {
    if total_secs == 0 {
        return "PT0S".to_string();
    }
    let negative = total_secs < 0;
    let abs_secs = total_secs.unsigned_abs();
    let hours = abs_secs / 3600;
    let minutes = (abs_secs % 3600) / 60;
    let secs = abs_secs % 60;

    let mut result = String::new();
    if negative {
        result.push('-');
    }
    result.push_str("PT");
    if hours > 0 {
        result.push_str(&format!("{}H", hours));
    }
    if minutes > 0 {
        result.push_str(&format!("{}M", minutes));
    }
    if secs > 0 {
        result.push_str(&format!("{}S", secs));
    }
    result
}

/// Extract a datetime component from a binding
fn eval_datetime_component<R: RowAccess, F>(
    args: &[Expression],
    row: &R,
    fn_name: &str,
    extract: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&DateTime<FixedOffset>) -> i64,
{
    check_arity(args, 1, fn_name)?;
    if let Expression::Var(var) = &args[0] {
        match row.get(*var) {
            Some(binding) => match parse_datetime_from_binding(binding) {
                Some(dt) => Ok(Some(ComparableValue::Long(extract(&dt)))),
                None => Ok(None),
            },
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidFilter(format!(
            "{} requires a variable argument",
            fn_name
        )))
    }
}
