//! Shared helper functions for filter evaluation
//!
//! Contains arity checks, regex caching, datetime parsing, and other utilities.

use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::context::WellKnownDatatypes;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use fluree_db_core::FlakeValue;
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use std::cell::RefCell;
use std::sync::Arc;

use super::value::ComparableValue;

// =============================================================================
// Static WellKnownDatatypes (optimization to avoid repeated construction)
// =============================================================================

/// Lazily initialized well-known datatypes.
///
/// This avoids creating a new WellKnownDatatypes instance on every function call.
pub static WELL_KNOWN_DATATYPES: Lazy<WellKnownDatatypes> = Lazy::new(WellKnownDatatypes::new);

// =============================================================================
// Regex Caching
// =============================================================================

// Thread-local cache for compiled regexes to avoid recompiling on every row.
// SPARQL REGEX patterns are typically constant across a query, so caching
// provides significant speedup for filter-heavy queries.
thread_local! {
    static REGEX_CACHE: RefCell<lru::LruCache<(String, String), Regex>> =
        RefCell::new(lru::LruCache::new(std::num::NonZeroUsize::new(32).unwrap()));
}

/// Build a regex with optional flags (cached)
///
/// Supported flags: i (case-insensitive), m (multiline), s (dot-all), x (ignore whitespace)
/// Returns an error for unknown flags (not silent ignore).
///
/// Uses a thread-local LRU cache to avoid recompiling the same pattern+flags
/// on every row. Regex::clone is cheap (Arc internally).
pub fn build_regex_with_flags(pattern: &str, flags: &str) -> Result<Regex> {
    // Check cache first
    let cache_key = (pattern.to_string(), flags.to_string());
    let cached = REGEX_CACHE.with(|cache| cache.borrow_mut().get(&cache_key).cloned());

    if let Some(re) = cached {
        return Ok(re);
    }

    // Not in cache - compile and store
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
    let re = builder
        .build()
        .map_err(|e| QueryError::InvalidFilter(format!("Invalid regex: {}", e)))?;

    // Cache for future use
    REGEX_CACHE.with(|cache| {
        cache.borrow_mut().put(cache_key, re.clone());
    });

    Ok(re)
}

// =============================================================================
// Arity Checking
// =============================================================================

/// Check that a function has the expected number of arguments
#[inline]
pub fn check_arity(args: &[Expression], expected: usize, fn_name: &str) -> Result<()> {
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

/// Check that a function has at least the minimum number of arguments
#[inline]
pub fn check_min_arity(args: &[Expression], min: usize, fn_name: &str) -> Result<()> {
    if args.len() < min {
        Err(QueryError::InvalidFilter(format!(
            "{} requires at least {} argument{}",
            fn_name,
            min,
            if min == 1 { "" } else { "s" }
        )))
    } else {
        Ok(())
    }
}

// =============================================================================
// DateTime Parsing
// =============================================================================

/// Parse a datetime from a binding, respecting datatype
///
/// Returns None if not a datetime type or parse fails.
/// Handles xsd:dateTime, xsd:date, xsd:time, and calendar fragment types
/// (xsd:gYear, xsd:gYearMonth, xsd:gMonth, xsd:gDay, xsd:gMonthDay).
/// Fragment types are promoted to full DateTime with sensible defaults
/// for missing components (e.g., gYear -> Jan 1 at 00:00:00).
pub fn parse_datetime_from_binding(
    binding: &Binding,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<DateTime<FixedOffset>> {
    let datatypes = &*WELL_KNOWN_DATATYPES;

    match binding {
        Binding::Lit { val, dt, .. } => {
            let is_datetime_type = *dt == datatypes.xsd_datetime
                || *dt == datatypes.xsd_date
                || *dt == datatypes.xsd_time
                || *dt == datatypes.xsd_g_year
                || *dt == datatypes.xsd_g_year_month
                || *dt == datatypes.xsd_g_month
                || *dt == datatypes.xsd_g_day
                || *dt == datatypes.xsd_g_month_day;

            if !is_datetime_type {
                return None;
            }

            flake_value_to_datetime(val, Some(dt), datatypes)
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            ..
        } => {
            let ctx = ctx?;
            let store = ctx.binary_store.as_deref()?;
            let dt_sid = store.dt_sids().get(*dt_id as usize)?.clone();

            let is_datetime_type = dt_sid == datatypes.xsd_datetime
                || dt_sid == datatypes.xsd_date
                || dt_sid == datatypes.xsd_time
                || dt_sid == datatypes.xsd_g_year
                || dt_sid == datatypes.xsd_g_year_month
                || dt_sid == datatypes.xsd_g_month
                || dt_sid == datatypes.xsd_g_day
                || dt_sid == datatypes.xsd_g_month_day;
            if !is_datetime_type {
                return None;
            }

            let gv = ctx.graph_view()?;
            let val = gv.decode_value(*o_kind, *o_key, *p_id).ok()?;

            flake_value_to_datetime(&val, Some(&dt_sid), datatypes)
        }
        _ => None,
    }
}

/// Convert a FlakeValue to a DateTime, handling all XSD temporal types.
///
/// The `dt_sid` parameter is used only for the `FlakeValue::Long` fallback
/// (numeric gYear encoding); all other variants are self-describing.
fn flake_value_to_datetime(
    val: &FlakeValue,
    dt_sid: Option<&fluree_db_core::Sid>,
    datatypes: &WellKnownDatatypes,
) -> Option<DateTime<FixedOffset>> {
    let utc = FixedOffset::east_opt(0).unwrap();

    match val {
        FlakeValue::DateTime(dt) => {
            let offset = dt.tz_offset().unwrap_or(utc);
            Some(dt.instant().with_timezone(&offset))
        }
        FlakeValue::Date(d) => {
            let offset = d.tz_offset().unwrap_or(utc);
            let naive = d.date().and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::Time(t) => {
            let offset = t.tz_offset().unwrap_or(utc);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let naive = NaiveDateTime::new(date, t.time());
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GYear(gy) => {
            let offset = gy.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(gy.year(), 1, 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GYearMonth(gym) => {
            let offset = gym.tz_offset().unwrap_or(utc);
            let naive =
                NaiveDate::from_ymd_opt(gym.year(), gym.month(), 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GMonth(gm) => {
            let offset = gm.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(1970, gm.month(), 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GDay(gd) => {
            let offset = gd.tz_offset().unwrap_or(utc);
            let naive = NaiveDate::from_ymd_opt(1970, 1, gd.day())?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::GMonthDay(gmd) => {
            let offset = gmd.tz_offset().unwrap_or(utc);
            let naive =
                NaiveDate::from_ymd_opt(1970, gmd.month(), gmd.day())?.and_hms_opt(0, 0, 0)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| offset.from_utc_datetime(&naive)),
            )
        }
        FlakeValue::String(s) => DateTime::parse_from_rfc3339(s).ok().or_else(|| {
            let with_time = format!("{}T00:00:00+00:00", s);
            DateTime::parse_from_rfc3339(&with_time).ok()
        }),
        FlakeValue::Long(y) if dt_sid == Some(&datatypes.xsd_g_year) => {
            let year = i32::try_from(*y).ok()?;
            let naive = NaiveDate::from_ymd_opt(year, 1, 1)?.and_hms_opt(0, 0, 0)?;
            Some(
                utc.from_local_datetime(&naive)
                    .single()
                    .unwrap_or_else(|| utc.from_utc_datetime(&naive)),
            )
        }
        _ => None,
    }
}

/// Format a datatype Sid as a ComparableValue
///
/// Returns compact string representations for well-known datatypes.
pub fn format_datatype_sid(dt: &fluree_db_core::Sid) -> ComparableValue {
    let datatypes = &*WELL_KNOWN_DATATYPES;
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
