//! Temporal types for XSD dateTime, date, and time
//!
//! This module provides structured temporal types that:
//! - Preserve the original lexical form for round-trip serialization
//! - Normalize to UTC instants for consistent comparison
//! - Support SPARQL accessor functions (YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ)
//!
//! ## Comparison Semantics
//!
//! Temporal values are compared by their normalized UTC instant, not by lexical form.
//! This means `"2024-01-01T05:00:00Z"` equals `"2024-01-01T00:00:00-05:00"` (same instant).
//!
//! ## Timezone Handling
//!
//! - DateTime: Normalize to UTC instant; preserve original timezone for output
//! - Date: If timezone present, compare by instant at midnight in that offset
//! - Time: If timezone present, compare by UTC-normalized time-of-day
//!
//! Values without timezone are treated as UTC for comparison purposes.

use chrono::{
    DateTime as ChronoDateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime,
    Timelike, Utc,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;

/// Serde helper for Option<FixedOffset> - serializes as Option<i32> (seconds from UTC)
mod tz_offset_serde {
    use super::*;

    pub fn serialize<S>(offset: &Option<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match offset {
            Some(o) => serializer.serialize_some(&o.local_minus_utc()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FixedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<i32> = Option::deserialize(deserializer)?;
        Ok(opt.and_then(FixedOffset::east_opt))
    }
}

/// XSD dateTime with timezone preservation
///
/// Stores both the normalized UTC instant (for comparison) and the original
/// string representation (for serialization).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTime {
    /// Normalized UTC instant for comparison
    instant: ChronoDateTime<Utc>,
    /// Original timezone offset (None = no timezone in input, treated as UTC)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl DateTime {
    /// Parse an XSD dateTime string
    ///
    /// Accepts:
    /// - RFC3339/ISO8601 with timezone: `2024-01-15T10:30:00Z`, `2024-01-15T10:30:00+05:00`
    /// - Without timezone (treated as UTC): `2024-01-15T10:30:00`
    /// - With fractional seconds: `2024-01-15T10:30:00.123Z`
    pub fn parse(s: &str) -> Result<Self, String> {
        // Try RFC3339/ISO8601 with timezone
        if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(s) {
            return Ok(Self {
                instant: dt.with_timezone(&Utc),
                tz_offset: Some(*dt.offset()),
                original: s.to_string(),
            });
        }

        // Try with explicit timezone offset formats not covered by RFC3339
        // e.g., "2024-01-15T10:30:00+0500" (no colon in offset)
        for fmt in &[
            "%Y-%m-%dT%H:%M:%S%.f%z",
            "%Y-%m-%dT%H:%M:%S%z",
        ] {
            if let Ok(dt) = ChronoDateTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    instant: dt.with_timezone(&Utc),
                    tz_offset: Some(*dt.offset()),
                    original: s.to_string(),
                });
            }
        }

        // Try without timezone - multiple formats
        for fmt in &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
        ] {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    instant: ndt.and_utc(),
                    tz_offset: None,
                    original: s.to_string(),
                });
            }
        }

        Err(format!("Cannot parse dateTime: {}", s))
    }

    /// Get the normalized UTC instant
    pub fn instant(&self) -> ChronoDateTime<Utc> {
        self.instant
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    // === SPARQL accessor functions ===

    /// Get the year component
    pub fn year(&self) -> i32 {
        self.instant.year()
    }

    /// Get the month component (1-12)
    pub fn month(&self) -> u32 {
        self.instant.month()
    }

    /// Get the day component (1-31)
    pub fn day(&self) -> u32 {
        self.instant.day()
    }

    /// Get the hour component (0-23)
    pub fn hours(&self) -> u32 {
        self.instant.hour()
    }

    /// Get the minute component (0-59)
    pub fn minutes(&self) -> u32 {
        self.instant.minute()
    }

    /// Get the seconds component with fractional part
    pub fn seconds(&self) -> f64 {
        self.instant.second() as f64 + self.instant.nanosecond() as f64 / 1e9
    }

    /// Get the timezone string (e.g., "+05:00", "Z") or None if no timezone
    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{}{:02}:{:02}", sign, hours, mins)
            }
        })
    }

    /// Get epoch milliseconds (for Parquet storage)
    pub fn epoch_millis(&self) -> i64 {
        self.instant.timestamp_millis()
    }

    /// Get epoch microseconds (for higher precision Parquet storage)
    pub fn epoch_micros(&self) -> i64 {
        self.instant.timestamp_micros()
    }
}

impl PartialEq for DateTime {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl Eq for DateTime {}

impl Ord for DateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.instant.cmp(&other.instant)
    }
}

impl PartialOrd for DateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for DateTime {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.instant.timestamp_nanos_opt().hash(state);
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// XSD date (year-month-day with optional timezone)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Date {
    /// The date value
    date: NaiveDate,
    /// Original timezone offset (None = no timezone in input)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl Date {
    /// Parse an XSD date string
    ///
    /// Accepts:
    /// - With timezone: `2024-01-15Z`, `2024-01-15+05:00`
    /// - Without timezone: `2024-01-15`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !is_strict_date_lexical(s) {
            return Err(format!("Cannot parse date: {}", s));
        }

        // Try parsing with timezone suffix
        if let Some(date_part) = s.strip_suffix('Z') {
            if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
                return Ok(Self {
                    date,
                    tz_offset: Some(FixedOffset::east_opt(0).unwrap()),
                    original: s.to_string(),
                });
            }
        }

        // Try parsing with explicit offset (e.g., +05:00 or -05:00)
        if let Some(offset_start) = s.rfind(['+', '-']) {
            // Make sure this is actually a timezone, not just a negative year
            if offset_start > 0 && s[offset_start..].contains(':') {
                let date_part = &s[..offset_start];
                let offset_part = &s[offset_start..];
                
                if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
                    // Parse the offset
                    let sign = if offset_part.starts_with('-') { -1 } else { 1 };
                    let offset_str = &offset_part[1..];
                    if let Some((hours_str, mins_str)) = offset_str.split_once(':') {
                        if let (Ok(hours), Ok(mins)) = (hours_str.parse::<i32>(), mins_str.parse::<i32>()) {
                            let total_secs = sign * (hours * 3600 + mins * 60);
                            if let Some(offset) = FixedOffset::east_opt(total_secs) {
                                return Ok(Self {
                                    date,
                                    tz_offset: Some(offset),
                                    original: s.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Try without timezone
        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return Ok(Self {
                date,
                tz_offset: None,
                original: s.to_string(),
            });
        }

        Err(format!("Cannot parse date: {}", s))
    }

    /// Get the date value
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Convert to UTC instant at midnight for timezone-aware comparison
    fn to_instant(&self) -> ChronoDateTime<Utc> {
        let midnight = self.date.and_hms_opt(0, 0, 0).unwrap();
        match self.tz_offset {
            Some(offset) => midnight
                .and_local_timezone(offset)
                .single()
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|| midnight.and_utc()),
            None => midnight.and_utc(),
        }
    }

    // === SPARQL accessor functions ===

    pub fn year(&self) -> i32 {
        self.date.year()
    }

    pub fn month(&self) -> u32 {
        self.date.month()
    }

    pub fn day(&self) -> u32 {
        self.date.day()
    }

    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{}{:02}:{:02}", sign, hours, mins)
            }
        })
    }

    /// Get days since epoch (for Parquet storage)
    pub fn days_since_epoch(&self) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        self.date.signed_duration_since(epoch).num_days() as i32
    }
}

impl PartialEq for Date {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Date {}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        // If either side has a timezone, compare by instant at midnight UTC
        match (self.tz_offset, other.tz_offset) {
            (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => {
                self.to_instant().cmp(&other.to_instant())
            }
            (None, None) => self.date.cmp(&other.date),
        }
    }
}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Date {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.date.hash(state);
        self.tz_offset.map(|o| o.local_minus_utc()).hash(state);
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// XSD time (hour:minute:second with optional timezone)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Time {
    /// The time value
    time: NaiveTime,
    /// Original timezone offset (None = no timezone in input)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl Time {
    /// Parse an XSD time string
    ///
    /// Accepts:
    /// - With timezone: `10:30:00Z`, `10:30:00+05:00`
    /// - Without timezone: `10:30:00`
    /// - With fractional seconds: `10:30:00.123Z`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !is_strict_time_lexical(s) {
            return Err(format!("Cannot parse time: {}", s));
        }

        // Try parsing with Z suffix
        if let Some(time_part) = s.strip_suffix('Z') {
            for fmt in &["%H:%M:%S%.f", "%H:%M:%S"] {
                if let Ok(time) = NaiveTime::parse_from_str(time_part, fmt) {
                    return Ok(Self {
                        time,
                        tz_offset: Some(FixedOffset::east_opt(0).unwrap()),
                        original: s.to_string(),
                    });
                }
            }
        }

        // Try parsing with explicit offset
        if let Some(offset_start) = s.rfind(['+', '-']) {
            if s[offset_start..].contains(':') {
                let time_part = &s[..offset_start];
                let offset_part = &s[offset_start..];

                for fmt in &["%H:%M:%S%.f", "%H:%M:%S"] {
                    if let Ok(time) = NaiveTime::parse_from_str(time_part, fmt) {
                        let sign = if offset_part.starts_with('-') { -1 } else { 1 };
                        let offset_str = &offset_part[1..];
                        if let Some((hours_str, mins_str)) = offset_str.split_once(':') {
                            if let (Ok(hours), Ok(mins)) = (hours_str.parse::<i32>(), mins_str.parse::<i32>()) {
                                let total_secs = sign * (hours * 3600 + mins * 60);
                                if let Some(offset) = FixedOffset::east_opt(total_secs) {
                                    return Ok(Self {
                                        time,
                                        tz_offset: Some(offset),
                                        original: s.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Try without timezone
        for fmt in &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
            if let Ok(time) = NaiveTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    time,
                    tz_offset: None,
                    original: s.to_string(),
                });
            }
        }

        Err(format!("Cannot parse time: {}", s))
    }

    /// Get the time value
    pub fn time(&self) -> NaiveTime {
        self.time
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Normalize to UTC time-of-day for timezone-aware comparison
    fn to_utc_time(&self) -> NaiveTime {
        match self.tz_offset {
            Some(offset) => {
                let secs = self.time.num_seconds_from_midnight() as i32 - offset.local_minus_utc();
                let normalized_secs = secs.rem_euclid(86400) as u32;
                NaiveTime::from_num_seconds_from_midnight_opt(normalized_secs, self.time.nanosecond())
                    .unwrap_or(self.time)
            }
            None => self.time,
        }
    }

    // === SPARQL accessor functions ===

    pub fn hours(&self) -> u32 {
        self.time.hour()
    }

    pub fn minutes(&self) -> u32 {
        self.time.minute()
    }

    pub fn seconds(&self) -> f64 {
        self.time.second() as f64 + self.time.nanosecond() as f64 / 1e9
    }

    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{}{:02}:{:02}", sign, hours, mins)
            }
        })
    }

    /// Get microseconds since midnight (for Parquet storage)
    pub fn micros_since_midnight(&self) -> i64 {
        let secs = self.time.num_seconds_from_midnight() as i64;
        let nanos = self.time.nanosecond() as i64;
        secs * 1_000_000 + nanos / 1000
    }
}

fn is_strict_date_lexical(s: &str) -> bool {
    let (date_part, tz_part) = if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, Some("Z"))
    } else if let Some(idx) = s.rfind(['+', '-']) {
        if idx == 10 {
            (&s[..idx], Some(&s[idx..]))
        } else {
            (s, None)
        }
    } else {
        (s, None)
    };

    if date_part.len() != 10 {
        return false;
    }
    let bytes = date_part.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes[0..4].iter().all(|b| b.is_ascii_digit())
        || !bytes[5..7].iter().all(|b| b.is_ascii_digit())
        || !bytes[8..10].iter().all(|b| b.is_ascii_digit())
    {
        return false;
    }

    if let Some(tz) = tz_part {
        if tz == "Z" {
            return true;
        }
        if tz.len() != 6 {
            return false;
        }
        let tzb = tz.as_bytes();
        if (tzb[0] != b'+' && tzb[0] != b'-') || tzb[3] != b':' {
            return false;
        }
        return tzb[1..3].iter().all(|b| b.is_ascii_digit())
            && tzb[4..6].iter().all(|b| b.is_ascii_digit());
    }

    true
}

fn is_strict_time_lexical(s: &str) -> bool {
    let (time_part, tz_part) = if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, Some("Z"))
    } else if s.len() >= 6 {
        let tail = &s[s.len() - 6..];
        if (tail.starts_with('+') || tail.starts_with('-')) && tail.as_bytes()[3] == b':' {
            (&s[..s.len() - 6], Some(tail))
        } else {
            (s, None)
        }
    } else {
        (s, None)
    };

    let (main, _frac) = if let Some((m, f)) = time_part.split_once('.') {
        if f.is_empty() || !f.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        (m, Some(f))
    } else {
        (time_part, None)
    };

    if main.len() != 8 {
        return false;
    }
    let bytes = main.as_bytes();
    if bytes[2] != b':' || bytes[5] != b':' {
        return false;
    }
    if !bytes[0..2].iter().all(|b| b.is_ascii_digit())
        || !bytes[3..5].iter().all(|b| b.is_ascii_digit())
        || !bytes[6..8].iter().all(|b| b.is_ascii_digit())
    {
        return false;
    }

    if let Some(tz) = tz_part {
        if tz == "Z" {
            return true;
        }
        if tz.len() != 6 {
            return false;
        }
        let tzb = tz.as_bytes();
        if (tzb[0] != b'+' && tzb[0] != b'-') || tzb[3] != b':' {
            return false;
        }
        return tzb[1..3].iter().all(|b| b.is_ascii_digit())
            && tzb[4..6].iter().all(|b| b.is_ascii_digit());
    }

    true
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Time {}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        // If either side has a timezone, compare by normalized UTC time-of-day
        match (self.tz_offset, other.tz_offset) {
            (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => {
                self.to_utc_time().cmp(&other.to_utc_time())
            }
            (None, None) => self.time.cmp(&other.time),
        }
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Time {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.time.hash(state);
        self.tz_offset.map(|o| o.local_minus_utc()).hash(state);
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_parse_rfc3339() {
        let dt = DateTime::parse("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hours(), 10);
        assert_eq!(dt.minutes(), 30);
        assert_eq!(dt.timezone(), Some("Z".to_string()));
    }

    #[test]
    fn test_datetime_parse_with_offset() {
        let dt = DateTime::parse("2024-01-15T10:30:00+05:00").unwrap();
        assert_eq!(dt.hours(), 5); // Normalized to UTC
        assert_eq!(dt.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_datetime_parse_no_timezone() {
        let dt = DateTime::parse("2024-01-15T10:30:00").unwrap();
        assert_eq!(dt.hours(), 10);
        assert!(dt.timezone().is_none());
    }

    #[test]
    fn test_datetime_equality_same_instant() {
        // These represent the same instant
        let dt1 = DateTime::parse("2024-01-01T05:00:00Z").unwrap();
        let dt2 = DateTime::parse("2024-01-01T00:00:00-05:00").unwrap();
        assert_eq!(dt1, dt2);
    }

    #[test]
    fn test_datetime_ordering() {
        let dt1 = DateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let dt2 = DateTime::parse("2024-01-01T01:00:00Z").unwrap();
        assert!(dt1 < dt2);
    }

    #[test]
    fn test_date_parse() {
        let d = Date::parse("2024-01-15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
        assert!(d.timezone().is_none());
    }

    #[test]
    fn test_date_parse_with_timezone() {
        let d = Date::parse("2024-01-15Z").unwrap();
        assert_eq!(d.timezone(), Some("Z".to_string()));

        let d2 = Date::parse("2024-01-15+05:00").unwrap();
        assert_eq!(d2.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_time_parse() {
        let t = Time::parse("10:30:00").unwrap();
        assert_eq!(t.hours(), 10);
        assert_eq!(t.minutes(), 30);
        assert!(t.timezone().is_none());
    }

    #[test]
    fn test_time_parse_with_timezone() {
        let t = Time::parse("10:30:00Z").unwrap();
        assert_eq!(t.timezone(), Some("Z".to_string()));

        let t2 = Time::parse("10:30:00+05:00").unwrap();
        assert_eq!(t2.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_time_ordering_with_timezone() {
        // 10:00:00Z and 15:00:00+05:00 are the same UTC time
        let t1 = Time::parse("10:00:00Z").unwrap();
        let t2 = Time::parse("15:00:00+05:00").unwrap();
        assert_eq!(t1, t2);
    }
}
