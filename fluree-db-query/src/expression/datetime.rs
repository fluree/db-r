//! DateTime function implementations
//!
//! Implements SPARQL datetime functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ

use crate::binding::RowView;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::DateTime as FlureeDateTime;
use std::sync::Arc;

use super::helpers::{check_arity, parse_datetime_from_binding};
use super::value::ComparableValue;

pub fn eval_now(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "NOW")?;
    let now = Utc::now();
    let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let parsed = FlureeDateTime::parse(&formatted)
        .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {}", e)))?;
    Ok(Some(ComparableValue::DateTime(parsed)))
}

pub fn eval_year(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "YEAR", |dt| dt.year() as i64)
}

pub fn eval_month(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "MONTH", |dt| dt.month() as i64)
}

pub fn eval_day(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "DAY", |dt| dt.day() as i64)
}

pub fn eval_hours(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "HOURS", |dt| dt.hour() as i64)
}

pub fn eval_minutes(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "MINUTES", |dt| dt.minute() as i64)
}

pub fn eval_seconds(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    eval_datetime_component(args, row, "SECONDS", |dt| dt.second() as i64)
}

pub fn eval_tz(args: &[Expression], row: &RowView) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "TZ")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => match parse_datetime_from_binding(binding) {
                Some(dt) => {
                    let offset = dt.offset();
                    let total_secs = offset.local_minus_utc();
                    let hours = total_secs / 3600;
                    let mins = (total_secs.abs() % 3600) / 60;
                    let sign = if total_secs >= 0 { '+' } else { '-' };
                    let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                    Ok(Some(ComparableValue::String(Arc::from(tz_str))))
                }
                None => Err(QueryError::InvalidFilter(
                    "TZ requires a datetime argument".to_string(),
                )),
            },
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidFilter(
            "TZ requires a variable argument".to_string(),
        ))
    }
}

/// Extract a datetime component from a binding
fn eval_datetime_component<F>(
    args: &[Expression],
    row: &RowView,
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
                None => Err(QueryError::InvalidFilter(format!(
                    "{} requires a datetime argument",
                    fn_name
                ))),
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
