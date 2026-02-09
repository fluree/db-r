use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_nameservice::NameService;
use futures::StreamExt;
use std::path::Path;

pub async fn run(
    ledger: Option<&str>,
    oneline: bool,
    count: Option<usize>,
    fluree_dir: &Path,
) -> CliResult<()> {
    // Check for tracked ledger — log requires local commit chain access
    let store = crate::config::TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let alias = context::resolve_ledger(ledger, fluree_dir)?;
    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "commit log is not available for tracked ledgers (no local commit chain).\n  \
             Use `fluree track status` to check remote state instead."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(fluree_dir)?;
    let address = context::to_ledger_id(&alias);

    // Get head commit address from nameservice
    let record = fluree
        .nameservice()
        .lookup(&address)
        .await?
        .ok_or_else(|| CliError::NotFound(format!("ledger '{}' not found", alias)))?;

    let head_address = record
        .commit_address
        .ok_or_else(|| CliError::NotFound(format!("ledger '{}' has no commits", alias)))?;

    // Walk commit chain
    let storage = fluree.storage().clone();
    let stream = fluree_db_novelty::trace_commits(storage, head_address, 0);
    let mut stream = std::pin::pin!(stream);
    let mut shown = 0usize;
    let limit = count.unwrap_or(usize::MAX);

    while let Some(result) = stream.next().await {
        if shown >= limit {
            break;
        }

        let commit = result?;

        let short_hash = abbreviate_hash(&commit.address);

        if oneline {
            // Note: commit messages are not currently persisted in the commit
            // format, so we show the timestamp instead.
            let time_str = commit.time.as_deref().unwrap_or("");
            println!("t={:<4}  {}  {}", commit.t, short_hash, time_str);
        } else {
            println!("commit {}", short_hash);
            if let Some(ref time) = commit.time {
                println!("Date:    {}", time);
            }
            println!("t:       {}", commit.t);
            println!("Flakes:  {}", commit.flakes.len());
            println!();
        }

        shown += 1;
    }

    if shown == 0 {
        println!("No commits found for ledger '{}'", alias);
    }

    Ok(())
}

/// Extract a short commit hash from a content address for display.
///
/// Handles formats like:
/// - `fluree:file://ledger/main/commit/<hash>.json`
/// - `fluree:commit:sha256:<hex>`
/// - Plain hex strings
fn abbreviate_hash(address: &str) -> String {
    // Try to extract hash from path-style addresses (e.g., .../commit/<hash>.json)
    if let Some(pos) = address.rfind("/commit/") {
        let after = &address[pos + 8..];
        let hash = after.trim_end_matches(".json");
        if hash.len() >= 7 {
            return hash[..7].to_string();
        }
        return hash.to_string();
    }

    // Try sha256: prefix style
    if let Some(pos) = address.find("sha256:") {
        let hex = &address[pos + 7..];
        if hex.len() >= 7 {
            return hex[..7].to_string();
        }
        return hex.to_string();
    }

    // Fallback: last path segment or first 7 chars
    if let Some(last) = address.rsplit('/').next() {
        let clean = last.trim_end_matches(".json");
        if clean.len() >= 7 {
            return clean[..7].to_string();
        }
        return clean.to_string();
    }

    if address.len() >= 7 {
        address[..7].to_string()
    } else {
        address.to_string()
    }
}
