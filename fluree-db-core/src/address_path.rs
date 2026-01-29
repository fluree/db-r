//! Canonical alias-to-path helpers for storage addresses.
//!
//! We avoid putting `:` in storage paths for cross-platform portability
//! (Windows/macOS filesystem restrictions). Instead we normalize a ledger alias
//! `name[:branch]` into `name/branch` (default branch applied).

use crate::alias::{split_alias, AliasParseError};

/// Convert a ledger alias `name[:branch]` into a portable path prefix `name/branch`.
///
/// - Applies the default branch if missing.
/// - Never includes `:` in the output.
pub fn alias_to_path_prefix(alias: &str) -> Result<String, AliasParseError> {
    // If caller already provided a storage-path style alias (`name/branch`),
    // preserve it as-is. This is used in various places (tests, file layouts)
    // and is already portable (no ':').
    if !alias.contains(':') && alias.contains('/') {
        return Ok(alias.to_string());
    }

    let (name, branch) = split_alias(alias)?;
    Ok(format!("{}/{}", name, branch))
}

