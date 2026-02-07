//! Sync commands: fetch, pull, push

use crate::config::{storage_path, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_nameservice::{FileTrackingStore, RefPublisher, RemoteName, RemoteTrackingStore};
use fluree_db_nameservice_sync::{
    FetchResult, HttpRemoteClient, PullResult, PushResult, RemoteEndpoint, SyncConfigStore,
    SyncDriver,
};
use std::path::Path;
use std::sync::Arc;

/// Build a SyncDriver with all configured remotes
async fn build_sync_driver(fluree_dir: &Path) -> CliResult<(SyncDriver, Arc<TomlSyncConfigStore>)> {
    let fluree = context::build_fluree(fluree_dir)?;
    let config_store = Arc::new(TomlSyncConfigStore::new(fluree_dir.to_path_buf()));

    // Get the nameservice as RefPublisher
    let ns = fluree.nameservice();
    let local: Arc<dyn RefPublisher> = Arc::new(ns.clone());

    // Create a FileTrackingStore using the same storage path
    let storage = storage_path(fluree_dir);
    let tracking: Arc<dyn RemoteTrackingStore> = Arc::new(FileTrackingStore::new(&storage));

    let mut driver = SyncDriver::new(
        local,
        tracking,
        config_store.clone() as Arc<dyn SyncConfigStore>,
    );

    // Add HTTP clients for all configured remotes
    let remotes = config_store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    for remote in remotes {
        match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => {
                let client = Arc::new(HttpRemoteClient::new(
                    base_url.clone(),
                    remote.auth.token.clone(),
                ));
                driver.add_client(&remote.name, client);
            }
            RemoteEndpoint::Sse { .. } | RemoteEndpoint::Storage { .. } => {
                // Skip non-HTTP remotes for now
                eprintln!(
                    "{} skipping non-HTTP remote '{}'",
                    "warning:".yellow().bold(),
                    remote.name.as_str()
                );
            }
        }
    }

    Ok((driver, config_store))
}

/// Fetch refs from a remote (like git fetch)
pub async fn run_fetch(remote: &str, fluree_dir: &Path) -> CliResult<()> {
    let (driver, _config) = build_sync_driver(fluree_dir).await?;
    let remote_name = RemoteName::new(remote);

    println!("Fetching from '{}'...", remote.cyan());

    let result = driver
        .fetch_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(format!("fetch failed: {e}")))?;

    print_fetch_result(&result);
    Ok(())
}

fn print_fetch_result(result: &FetchResult) {
    if result.updated.is_empty() && result.unchanged.is_empty() {
        println!("No ledgers found on remote.");
        return;
    }

    if !result.updated.is_empty() {
        println!("{}", "Updated:".green().bold());
        for (ledger_address, tracking) in &result.updated {
            let t = tracking.commit_ref.as_ref().map(|r| r.t).unwrap_or(0);
            println!("  {} -> t={}", ledger_address, t);
        }
    }

    if !result.unchanged.is_empty() {
        println!(
            "{} {} ledger(s) unchanged",
            "Already up to date:".dimmed(),
            result.unchanged.len()
        );
    }
}

/// Pull (fetch + fast-forward) a ledger from its upstream
pub async fn run_pull(ledger: Option<&str>, fluree_dir: &Path) -> CliResult<()> {
    let ledger_address = context::resolve_ledger(ledger, fluree_dir)?;
    let ledger_address = context::to_ledger_address(&ledger_address);

    let (driver, config_store) = build_sync_driver(fluree_dir).await?;

    // Get upstream config to know which remote to fetch from
    let upstream = config_store
        .get_upstream(&ledger_address)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let Some(upstream) = upstream else {
        return Err(CliError::Config(format!(
            "no upstream configured for '{}'\n  {} fluree upstream set {} <remote>",
            ledger_address,
            "hint:".cyan().bold(),
            ledger_address
        )));
    };

    // Fetch first
    println!("Fetching from '{}'...", upstream.remote.as_str().cyan());
    let _fetch_result = driver
        .fetch_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(format!("fetch failed: {e}")))?;

    // Then pull
    println!("Pulling '{}'...", ledger_address.cyan());
    let result = driver
        .pull_tracked(&ledger_address)
        .await
        .map_err(|e| CliError::Config(format!("pull failed: {e}")))?;

    print_pull_result(&result);

    match &result {
        PullResult::Diverged { .. } => Err(CliError::Config(
            "cannot fast-forward; histories have diverged".into(),
        )),
        _ => Ok(()),
    }
}

fn print_pull_result(result: &PullResult) {
    match result {
        PullResult::FastForwarded {
            ledger_address,
            from,
            to,
        } => {
            println!(
                "{} '{}' fast-forwarded: t={} -> t={}",
                "✓".green(),
                ledger_address,
                from.t,
                to.t
            );
        }
        PullResult::Current { ledger_address } => {
            println!("{} '{}' is already up to date", "✓".green(), ledger_address);
        }
        PullResult::Diverged {
            ledger_address,
            local,
            remote,
        } => {
            println!(
                "{} '{}' has diverged: local t={}, remote t={}",
                "✗".red(),
                ledger_address,
                local.t,
                remote.t
            );
            println!(
                "  {} your local has commits the remote does not have",
                "hint:".cyan().bold()
            );
        }
        PullResult::NoUpstream { ledger_address } => {
            println!(
                "{} '{}' has no upstream configured",
                "✗".red(),
                ledger_address
            );
        }
        PullResult::NoTracking { ledger_address } => {
            println!(
                "{} '{}' has no tracking data; run 'fluree fetch' first",
                "✗".red(),
                ledger_address
            );
        }
    }
}

/// Push a ledger to its upstream remote
pub async fn run_push(ledger: Option<&str>, fluree_dir: &Path) -> CliResult<()> {
    let ledger_address = context::resolve_ledger(ledger, fluree_dir)?;
    let ledger_address = context::to_ledger_address(&ledger_address);

    let (driver, config_store) = build_sync_driver(fluree_dir).await?;

    // Get upstream config
    let upstream = config_store
        .get_upstream(&ledger_address)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let Some(upstream) = upstream else {
        return Err(CliError::Config(format!(
            "no upstream configured for '{}'\n  {} fluree upstream set {} <remote>",
            ledger_address,
            "hint:".cyan().bold(),
            ledger_address
        )));
    };

    println!(
        "Pushing '{}' to '{}'...",
        ledger_address.cyan(),
        upstream.remote.as_str()
    );

    let result = driver
        .push_tracked(&ledger_address)
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;

    print_push_result(&result);

    match &result {
        PushResult::Rejected { .. } => Err(CliError::Config(
            "push rejected; fetch and pull first, then retry".into(),
        )),
        _ => Ok(()),
    }
}

fn print_push_result(result: &PushResult) {
    match result {
        PushResult::Pushed {
            ledger_address,
            value,
        } => {
            println!(
                "{} '{}' pushed successfully (t={})",
                "✓".green(),
                ledger_address,
                value.t
            );
        }
        PushResult::Rejected {
            ledger_address,
            local,
            remote,
        } => {
            println!(
                "{} '{}' rejected: local t={}, remote t={}",
                "✗".red(),
                ledger_address,
                local.t,
                remote.t
            );
            println!(
                "  {} the remote has commits you don't have; pull first",
                "hint:".cyan().bold()
            );
        }
        PushResult::NoUpstream { ledger_address } => {
            println!(
                "{} '{}' has no upstream configured",
                "✗".red(),
                ledger_address
            );
        }
    }
}
