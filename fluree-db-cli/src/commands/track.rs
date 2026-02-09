//! Track commands: add, remove, list, status
//!
//! Tracked ledgers are remote-only — all operations (query, insert, upsert)
//! are forwarded to the remote server via HTTP. No local ledger storage or
//! blocks are needed.

use crate::cli::TrackAction;
use crate::config::{TomlSyncConfigStore, TrackedLedgerConfig};
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{RemoteEndpoint, SyncConfigStore};
use std::path::Path;

pub async fn run(action: TrackAction, fluree_dir: &Path) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());

    match action {
        TrackAction::Add {
            ledger,
            remote,
            remote_alias,
        } => {
            run_add(
                &store,
                &ledger,
                remote.as_deref(),
                remote_alias.as_deref(),
                fluree_dir,
            )
            .await
        }
        TrackAction::Remove { ledger } => run_remove(&store, &ledger),
        TrackAction::List => run_list(&store),
        TrackAction::Status { ledger } => run_status(&store, ledger.as_deref()).await,
    }
}

async fn run_add(
    store: &TomlSyncConfigStore,
    ledger: &str,
    remote_name: Option<&str>,
    remote_alias: Option<&str>,
    fluree_dir: &Path,
) -> CliResult<()> {
    // Resolve remote: explicit arg, or default if exactly one remote configured
    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let remote = match remote_name {
        Some(name) => {
            let rn = RemoteName::new(name);
            store
                .get_remote(&rn)
                .await
                .map_err(|e| CliError::Config(e.to_string()))?
                .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?
        }
        None => {
            if remotes.is_empty() {
                return Err(CliError::Config(
                    "no remotes configured. Add one with `fluree remote add <name> <url>`"
                        .to_string(),
                ));
            }
            if remotes.len() > 1 {
                return Err(CliError::Usage(format!(
                    "multiple remotes configured ({}). Specify one with --remote <name>",
                    remotes
                        .iter()
                        .map(|r| r.name.as_str().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            remotes.into_iter().next().unwrap()
        }
    };

    let effective_remote_alias = remote_alias.unwrap_or(ledger);

    // Check mutual exclusion: refuse if local ledger exists
    let fluree = crate::context::build_fluree(fluree_dir)?;
    let local_address = crate::context::to_ledger_id(ledger);
    if fluree.ledger_exists(&local_address).await.unwrap_or(false) {
        return Err(CliError::Config(format!(
            "ledger '{}' already exists locally. \
             Remove it first, or use a different local alias with `--remote-alias`.",
            ledger
        )));
    }

    // Build client to validate ledger exists on remote
    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        other => {
            return Err(CliError::Config(format!(
                "remote '{}' uses {:?} endpoint; tracking requires an HTTP remote",
                remote.name.as_str(),
                other
            )));
        }
    };

    let client = RemoteLedgerClient::new(&base_url, remote.auth.token.clone());

    // Check ledger exists on remote
    match client.ledger_exists(effective_remote_alias).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(CliError::NotFound(format!(
                "ledger '{}' not found on remote '{}'",
                effective_remote_alias,
                remote.name.as_str()
            )));
        }
        Err(e) => {
            return Err(CliError::Remote(format!(
                "failed to check ledger on remote '{}': {}",
                remote.name.as_str(),
                e
            )));
        }
    }

    // Check not already tracked
    if store.get_tracked(ledger).is_some() {
        // Replace existing tracking
        eprintln!(
            "{} replacing existing tracking for '{}'",
            "note:".cyan().bold(),
            ledger
        );
    }

    let config = TrackedLedgerConfig {
        local_alias: ledger.to_string(),
        remote: remote.name.as_str().to_string(),
        remote_alias: effective_remote_alias.to_string(),
    };

    store.add_tracked(config)?;

    println!(
        "Tracking '{}' via remote '{}' ({})",
        ledger.green(),
        remote.name.as_str().green(),
        effective_remote_alias
    );
    Ok(())
}

fn run_remove(store: &TomlSyncConfigStore, ledger: &str) -> CliResult<()> {
    let removed = store.remove_tracked(ledger)?;
    if removed {
        println!("Removed tracking for '{}'", ledger);
    } else {
        return Err(CliError::NotFound(format!(
            "ledger '{}' is not tracked",
            ledger
        )));
    }
    Ok(())
}

fn run_list(store: &TomlSyncConfigStore) -> CliResult<()> {
    let tracked = store.tracked_ledgers();

    if tracked.is_empty() {
        println!("No tracked ledgers.");
        println!(
            "  {} fluree track add <ledger> --remote <name>",
            "hint:".cyan().bold()
        );
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Local Alias", "Remote", "Remote Alias"]);

    for t in tracked {
        table.add_row(vec![
            Cell::new(&t.local_alias),
            Cell::new(&t.remote),
            Cell::new(&t.remote_alias),
        ]);
    }

    println!("{}", table);
    Ok(())
}

async fn run_status(store: &TomlSyncConfigStore, ledger: Option<&str>) -> CliResult<()> {
    let tracked = match ledger {
        Some(alias) => {
            let t = store
                .get_tracked(alias)
                .ok_or_else(|| CliError::NotFound(format!("ledger '{}' is not tracked", alias)))?;
            vec![t]
        }
        None => {
            let all = store.tracked_ledgers();
            if all.is_empty() {
                println!("No tracked ledgers.");
                return Ok(());
            }
            all
        }
    };

    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    for t in &tracked {
        let remote = remotes
            .iter()
            .find(|r| r.name.as_str() == t.remote)
            .ok_or_else(|| {
                CliError::Config(format!(
                    "remote '{}' referenced by tracked ledger '{}' not found in config",
                    t.remote, t.local_alias
                ))
            })?;

        let base_url = match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => base_url.clone(),
            _ => {
                eprintln!(
                    "  {} '{}' remote '{}' is not HTTP, skipping",
                    "warn:".yellow().bold(),
                    t.local_alias,
                    t.remote
                );
                continue;
            }
        };

        let client = RemoteLedgerClient::new(&base_url, remote.auth.token.clone());

        println!(
            "{} {} (via {})",
            "Ledger:".bold(),
            t.local_alias.green(),
            t.remote
        );

        match client.ledger_info(&t.remote_alias).await {
            Ok(info) => {
                if let Some(t_val) = info.get("t").and_then(|v| v.as_i64()) {
                    println!("  t: {}", t_val);
                }
                if let Some(commit) = info.get("commit_address").and_then(|v| v.as_str()) {
                    println!("  commit: {}", commit);
                }
                if let Some(index) = info.get("index_address").and_then(|v| v.as_str()) {
                    println!("  index:  {}", index);
                }
                println!("  status: {}", "reachable".green());
            }
            Err(e) => {
                println!("  status: {} ({})", "unreachable".red(), e);
            }
        }

        if tracked.len() > 1 {
            println!();
        }
    }

    Ok(())
}
