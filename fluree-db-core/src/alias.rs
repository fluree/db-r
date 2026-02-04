//! Alias parsing and normalization utilities.
//!
//! Centralizes default-branch handling and time-travel parsing so all
//! callers use consistent rules.

use std::fmt;

/// Default branch name used when none is specified.
pub const DEFAULT_BRANCH: &str = "main";

/// Time-travel specification parsed from an alias suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AliasTimeSpec {
    /// @t:<transaction>
    AtT(i64),
    /// @iso:<timestamp>
    AtIso(String),
    /// @sha:<commit>
    AtSha(String),
}

/// Parsed alias parts with optional time-travel spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAlias {
    pub name: String,
    pub branch: String,
    pub time: Option<AliasTimeSpec>,
}

/// Error returned when alias parsing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasParseError {
    message: String,
}

impl AliasParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AliasParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AliasParseError {}

/// Split a `name[:branch]` alias into (name, branch), applying the default branch.
pub fn split_alias(alias: &str) -> Result<(String, String), AliasParseError> {
    let parts: Vec<&str> = alias.splitn(2, ':').collect();

    match parts.as_slice() {
        [name] if !name.is_empty() => Ok((name.to_string(), DEFAULT_BRANCH.to_string())),
        [name, branch] if !name.is_empty() && !branch.is_empty() => {
            // Reject multiple colons (e.g., "name:branch:extra") - branch must not contain ':'.
            if branch.contains(':') {
                return Err(AliasParseError::new(format!(
                    "Invalid alias format '{}': expected 'name' or 'name:branch'",
                    alias
                )));
            }
            Ok((name.to_string(), branch.to_string()))
        }
        _ => Err(AliasParseError::new(format!(
            "Invalid alias format '{}': expected 'name' or 'name:branch'",
            alias
        ))),
    }
}

/// Normalize an alias to `name:branch` form using the default branch.
pub fn normalize_alias(alias: &str) -> Result<String, AliasParseError> {
    let (name, branch) = split_alias(alias)?;
    Ok(format_alias(&name, &branch))
}

/// Format a canonical `name:branch` alias string.
pub fn format_alias(name: &str, branch: &str) -> String {
    format!("{}:{}", name, branch)
}

/// Parse a ledger alias with optional `@t:`, `@iso:`, or `@sha:` time-travel suffix.
pub fn parse_alias_with_time(alias: &str) -> Result<ParsedAlias, AliasParseError> {
    let (base, time) = split_time_travel_suffix(alias)?;

    let (name, branch) = split_alias(&base)?;

    Ok(ParsedAlias { name, branch, time })
}

/// Split an alias string into its base and optional time-travel suffix.
///
/// This does not interpret `:`; it only handles `@t:`, `@iso:`, and `@sha:`.
pub fn split_time_travel_suffix(
    alias: &str,
) -> Result<(String, Option<AliasTimeSpec>), AliasParseError> {
    if let Some(at_idx) = alias.find('@') {
        let base = &alias[..at_idx];
        let time_str = &alias[at_idx + 1..];

        if base.is_empty() {
            return Err(AliasParseError::new(
                "Alias cannot be empty before '@'".to_string(),
            ));
        }

        let time = if let Some(val) = time_str.strip_prefix("t:") {
            if val.is_empty() {
                return Err(AliasParseError::new("Missing value after '@t:'"));
            }
            let t: i64 = val
                .parse()
                .map_err(|_| AliasParseError::new(format!("Invalid integer for @t: '{}'", val)))?;
            Some(AliasTimeSpec::AtT(t))
        } else if let Some(val) = time_str.strip_prefix("iso:") {
            if val.is_empty() {
                return Err(AliasParseError::new("Missing value after '@iso:'"));
            }
            Some(AliasTimeSpec::AtIso(val.to_string()))
        } else if let Some(val) = time_str.strip_prefix("sha:") {
            if val.is_empty() {
                return Err(AliasParseError::new("Missing value after '@sha:'"));
            }
            if val.len() < 6 {
                return Err(AliasParseError::new(
                    "SHA prefix must be at least 6 characters",
                ));
            }
            Some(AliasTimeSpec::AtSha(val.to_string()))
        } else {
            return Err(AliasParseError::new(format!(
                "Invalid time travel format: '{}'. Expected @t:, @iso:, or @sha: prefix",
                time_str
            )));
        };

        Ok((base.to_string(), time))
    } else {
        Ok((alias.to_string(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_alias_with_branch() {
        let (name, branch) = split_alias("mydb:main").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_split_alias_without_branch() {
        let (name, branch) = split_alias("mydb").unwrap();
        assert_eq!(name, "mydb");
        assert_eq!(branch, DEFAULT_BRANCH);
    }

    #[test]
    fn test_parse_alias_with_time_t() {
        let parsed = parse_alias_with_time("ledger:main@t:42").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, "main");
        assert!(matches!(parsed.time, Some(AliasTimeSpec::AtT(42))));
    }

    #[test]
    fn test_parse_alias_with_time_iso() {
        let parsed = parse_alias_with_time("ledger@iso:2025-01-01T00:00:00Z").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(AliasTimeSpec::AtIso(_))));
    }

    #[test]
    fn test_parse_alias_with_time_sha() {
        let parsed = parse_alias_with_time("ledger@sha:abc123").unwrap();
        assert_eq!(parsed.name, "ledger");
        assert_eq!(parsed.branch, DEFAULT_BRANCH);
        assert!(matches!(parsed.time, Some(AliasTimeSpec::AtSha(_))));
    }
}
