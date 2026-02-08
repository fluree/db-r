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

fn token_has_storage_permissions(token: &str) -> Option<bool> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;

    let storage_all = claims
        .get("fluree.storage.all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let storage_ledgers_len = claims
        .get("fluree.storage.ledgers")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    Some(storage_all || storage_ledgers_len > 0)
}

fn replication_permission_error(remote: &str) -> CliError {
    CliError::Config(format!(
        "this operation replicates ledger refs (pull/push/fetch) and requires a root token with `fluree.storage.*` permissions.\n  {} if you only have query access, use `fluree track` (or `--remote`) and run queries/transactions remotely.\n  {} fluree auth login --remote {}",
        "hint:".cyan().bold(),
        "hint:".cyan().bold(),
        remote
    ))
}

fn map_sync_auth_error(remote: &str, err: &str) -> Option<CliError> {
    // `fluree-db-nameservice-sync` reports remote failures as strings; match the common
    // permission-related server errors and provide a clearer CLI message.
    if err.contains("401")
        || err.contains("403")
        || err.contains("Bearer token required")
        || err.contains("Untrusted issuer")
        || err.contains("Token lacks storage proxy permissions")
        || err.contains("Storage proxy not enabled")
    {
        Some(replication_permission_error(remote))
    } else {
        None
    }
}

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
    // Proactively fail with a clear message for query-only tokens.
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let remote_cfg = store
        .get_remote(&RemoteName::new(remote))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", remote)))?;
    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(remote));
        }
    }

    let (driver, _config) = build_sync_driver(fluree_dir).await?;
    let remote_name = RemoteName::new(remote);

    println!("Fetching from '{}'...", remote.cyan());

    let result = driver.fetch_remote(&remote_name).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(remote, &msg)
            .unwrap_or_else(|| CliError::Config(format!("fetch failed: {msg}")))
    })?;

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

    // Proactively fail with a clear message for query-only tokens.
    if let Some(remote_cfg) = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
    {
        if let Some(tok) = &remote_cfg.auth.token {
            if let Some(false) = token_has_storage_permissions(tok) {
                return Err(replication_permission_error(upstream.remote.as_str()));
            }
        }
    }

    let _fetch_result = driver.fetch_remote(&upstream.remote).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(upstream.remote.as_str(), &msg)
            .unwrap_or_else(|| CliError::Config(format!("fetch failed: {msg}")))
    })?;

    // Then pull
    println!("Pulling '{}'...", ledger_address.cyan());
    let result = driver.pull_tracked(&ledger_address).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(upstream.remote.as_str(), &msg)
            .unwrap_or_else(|| CliError::Config(format!("pull failed: {msg}")))
    })?;

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

    // Proactively fail with a clear message for query-only tokens.
    if let Some(remote_cfg) = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
    {
        if let Some(tok) = &remote_cfg.auth.token {
            if let Some(false) = token_has_storage_permissions(tok) {
                return Err(replication_permission_error(upstream.remote.as_str()));
            }
        }
    }

    let result = driver.push_tracked(&ledger_address).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(upstream.remote.as_str(), &msg)
            .unwrap_or_else(|| CliError::Config(format!("push failed: {msg}")))
    })?;

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
