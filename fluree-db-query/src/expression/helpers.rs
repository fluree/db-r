//! Shared helper functions for filter evaluation
//!
//! Contains arity checks, regex caching, datetime parsing, and other utilities.

use crate::binding::{Binding, RowView};
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

// =============================================================================
// DateTime Parsing
// =============================================================================

/// Parse a datetime from a binding, respecting datatype
///
/// Returns None if not a datetime type or parse fails.
/// Only parses when binding has XSD datetime/date/time datatype.
pub fn parse_datetime_from_binding(binding: &Binding) -> Option<DateTime<FixedOffset>> {
    let datatypes = &*WELL_KNOWN_DATATYPES;

    match binding {
        Binding::Lit { val, dt, .. } => {
            // Check datatype is datetime/date/time using known Sids (no IRI decoding)
            let is_datetime_type = *dt == datatypes.xsd_datetime
                || *dt == datatypes.xsd_date
                || *dt == datatypes.xsd_time;

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
// Variable Metadata Extraction
// =============================================================================

/// Check if any variables in the expression are unbound
pub fn has_unbound_vars(expr: &Expression, row: &RowView) -> bool {
    expr.variables().into_iter().any(|var| {
        matches!(
            row.get(var),
            None | Some(Binding::Unbound) | Some(Binding::Poisoned)
        )
    })
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
