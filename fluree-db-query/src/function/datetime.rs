//! DateTime function implementations
//!
//! Implements SPARQL datetime functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{FilterExpr, FunctionName};
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::DateTime as FlureeDateTime;
use fluree_db_core::Storage;
use std::sync::Arc;

use super::helpers::{check_arity, parse_datetime_from_binding};
use super::value::ComparableValue;

/// Evaluate a datetime function
pub fn eval_datetime_function<S: Storage>(
    name: &FunctionName,
    args: &[FilterExpr],
    row: &RowView,
    _ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Now => {
            let now = Utc::now();
            let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
            let parsed = FlureeDateTime::parse(&formatted)
                .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {}", e)))?;
            Ok(Some(ComparableValue::DateTime(Box::new(parsed))))
        }

        FunctionName::Year => eval_datetime_component(args, row, "YEAR", |dt| dt.year() as i64),
        FunctionName::Month => eval_datetime_component(args, row, "MONTH", |dt| dt.month() as i64),
        FunctionName::Day => eval_datetime_component(args, row, "DAY", |dt| dt.day() as i64),
        FunctionName::Hours => eval_datetime_component(args, row, "HOURS", |dt| dt.hour() as i64),
        FunctionName::Minutes => {
            eval_datetime_component(args, row, "MINUTES", |dt| dt.minute() as i64)
        }
        FunctionName::Seconds => {
            eval_datetime_component(args, row, "SECONDS", |dt| dt.second() as i64)
        }

        FunctionName::Tz => {
            check_arity(args, 1, "TZ")?;
            if let FilterExpr::Var(var_id) = &args[0] {
                if let Some(binding) = row.get(*var_id) {
                    if let Some(dt) = parse_datetime_from_binding(binding) {
                        let offset = dt.offset();
                        let total_secs = offset.local_minus_utc();
                        let hours = total_secs / 3600;
                        let mins = (total_secs.abs() % 3600) / 60;
                        let sign = if total_secs >= 0 { '+' } else { '-' };
                        let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                        return Ok(Some(ComparableValue::String(Arc::from(tz_str))));
                    }
                }
            }
            Ok(None)
        }

        _ => unreachable!(
            "Non-datetime function routed to datetime module: {:?}",
            name
        ),
    }
}

/// Extract a datetime component from a binding
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
