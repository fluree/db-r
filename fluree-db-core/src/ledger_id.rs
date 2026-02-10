//! Ledger ID parsing and normalization utilities.
//!
//! Centralizes default-branch handling and time-travel parsing so all
//! callers use consistent rules.

use std::fmt;

/// Default branch name used when none is specified.
pub const DEFAULT_BRANCH: &str = "main";

/// Time-travel specification parsed from a ledger ID suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerIdTimeSpec {
    /// @t:<transaction>
    AtT(i64),
    /// @iso:<timestamp>
    AtIso(String),
    /// @commit:<cid>
    AtCommit(String),
}

/// Parsed ledger ID parts with optional time-travel spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLedgerId {
    pub name: String,
    pub branch: String,
    pub time: Option<LedgerIdTimeSpec>,
}

/// Error returned when ledger ID parsing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerIdParseError {
    message: String,
}

impl LedgerIdParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LedgerIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LedgerIdParseError {}

/// Split a `name[:branch]` ledger ID into (name, branch), applying the default branch.
pub fn split_ledger_id(ledger_id: &str) -> Result<(String, String), LedgerIdParseError> {
    let parts: Vec<&str> = ledger_id.splitn(2, ':').collect();

    match parts.as_slice() {
        [name] if !name.is_empty() => Ok((name.to_string(), DEFAULT_BRANCH.to_string())),
        [name, branch] if !name.is_empty() && !branch.is_empty() => {
            // Reject multiple colons (e.g., "name:branch:extra") - branch must not contain ':'.
            if branch.contains(':') {
                return Err(LedgerIdParseError::new(format!(
                    "Invalid ledger ID format '{}': expected 'name' or 'name:branch'",
                    ledger_id
                )));
            }
            Ok((name.to_string(), branch.to_string()))
        }
        _ => Err(LedgerIdParseError::new(format!(
            "Invalid ledger ID format '{}': expected 'name' or 'name:branch'",
            ledger_id
        ))),
    }
}

/// Normalize a ledger ID to `name:branch` form using the default branch.
pub fn normalize_ledger_id(ledger_id: &str) -> Result<String, LedgerIdParseError> {
    let (name, branch) = split_ledger_id(ledger_id)?;
    Ok(format_ledger_id(&name, &branch))
}

/// Format a canonical `name:branch` ledger ID string.
pub fn format_ledger_id(name: &str, branch: &str) -> String {
    format!("{}:{}", name, branch)
}

/// Parse a ledger ID with optional `@t:`, `@iso:`, or `@commit:` time-travel suffix.
pub fn parse_ledger_id_with_time(ledger_id: &str) -> Result<ParsedLedgerId, LedgerIdParseError> {
    let (base, time) = split_time_travel_suffix(ledger_id)?;

    let (name, branch) = split_ledger_id(&base)?;

    Ok(ParsedLedgerId { name, branch, time })
}

/// Split a ledger ID string into its base and optional time-travel suffix.
///
/// This does not interpret `:`; it only handles `@t:`, `@iso:`, and `@commit:`.
pub fn split_time_travel_suffix(
    ledger_id: &str,
) -> Result<(String, Option<LedgerIdTimeSpec>), LedgerIdParseError> {
    if let Some(at_idx) = ledger_id.find('@') {
        let base = &ledger_id[..at_idx];
        let time_str = &ledger_id[at_idx + 1..];

        if base.is_empty() {
            return Err(LedgerIdParseError::new(
                "Ledger ID cannot be empty before '@'".to_string(),
            ));
        }

        let time = if let Some(val) = time_str.strip_prefix("t:") {
            if val.is_empty() {
                return Err(LedgerIdParseError::new("Missing value after '@t:'"));
            }
            let t: i64 = val.parse().map_err(|_| {
                LedgerIdParseError::new(format!("Invalid integer for @t: '{}'", val))
            })?;
            Some(LedgerIdTimeSpec::AtT(t))
        } else if let Some(val) = time_str.strip_prefix("iso:") {
            if val.is_empty() {
                return Err(LedgerIdParseError::new("Missing value after '@iso:'"));
            }
            Some(LedgerIdTimeSpec::AtIso(val.to_string()))
        } else if let Some(val) = time_str.strip_prefix("commit:") {
            if val.is_empty() {
                return Err(LedgerIdParseError::new("Missing value after '@commit:'"));
            }
            if val.len() < 6 {
                return Err(LedgerIdParseError::new(
                    "Commit prefix must be at least 6 characters",
                ));
            }
            Some(LedgerIdTimeSpec::AtCommit(val.to_string()))
        } else {
            return Err(LedgerIdParseError::new(format!(
                "Invalid time travel format: '{}'. Expected @t:, @iso:, or @commit: prefix",
                time_str
            )));
        };

        Ok((base.to_string(), time))
    } else {
        Ok((ledger_id.to_string(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ledger_id_with_branch() {
        let (name, branch) = split_ledger_id("mydb:main").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_split_ledger_id_without_branch() {
        let (name, branch) = split_ledger_id("mydb").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, DEFAULT_BRANCH);
    }

    #[test]
    fn test_parse_ledger_id_with_time_t() {
        let parsed = parse_ledger_id_with_time("ledger:main@t:42").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, "main");
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtT(42))));
    }

    #[test]
    fn test_parse_ledger_id_with_time_iso() {
        let parsed = parse_ledger_id_with_time("ledger@iso:2025-01-01T00:00:00Z").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtIso(_))));
    }

    #[test]
    fn test_parse_ledger_id_with_time_commit() {
        let parsed = parse_ledger_id_with_time("ledger@commit:abc123").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(LedgerIdTimeSpec::AtCommit(_))));
    }
}
