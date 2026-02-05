//! Remote management commands: add, remove, list, show

use crate::cli::RemoteAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{RemoteAuth, RemoteConfig, RemoteEndpoint, SyncConfigStore};
use std::fs;
use std::path::Path;

pub async fn run(action: RemoteAction, fluree_dir: &Path) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());

    match action {
        RemoteAction::Add { name, url, token } => run_add(&store, &name, &url, token).await,
        RemoteAction::Remove { name } => run_remove(&store, &name).await,
        RemoteAction::List => run_list(&store).await,
        RemoteAction::Show { name } => run_show(&store, &name).await,
    }
}

async fn run_add(
    store: &TomlSyncConfigStore,
    name: &str,
    url: &str,
    token: Option<String>,
) -> CliResult<()> {
    // Parse URL to ensure it's valid
    let base_url = url.trim_end_matches('/').to_string();

    // Load token from file if @filepath
    let auth_token = match token {
        Some(t) if t.starts_with('@') => {
            let path = t.strip_prefix('@').unwrap();
            let expanded = shellexpand::tilde(path);
            Some(
                fs::read_to_string(expanded.as_ref())
                    .map_err(|e| CliError::Input(format!("failed to read token file: {e}")))?
                    .trim()
                    .to_string(),
            )
        }
        Some(t) => Some(t),
        None => None,
    };

    let config = RemoteConfig {
        name: RemoteName::new(name),
        endpoint: RemoteEndpoint::Http { base_url },
        auth: RemoteAuth { token: auth_token },
        fetch_interval_secs: None,
    };

    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Added remote '{}'", name.green());
    Ok(())
}

async fn run_remove(store: &TomlSyncConfigStore, name: &str) -> CliResult<()> {
    let remote_name = RemoteName::new(name);

    // Check if exists
    let existing = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if existing.is_none() {
        return Err(CliError::NotFound(format!("remote '{}' not found", name)));
    }

    store
        .remove_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Removed remote '{}'", name);
    Ok(())
}

async fn run_list(store: &TomlSyncConfigStore) -> CliResult<()> {
    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    if remotes.is_empty() {
        println!("No remotes configured.");
        println!("  {} fluree remote add <name> <url>", "hint:".cyan().bold());
        return Ok(());
    }

    let mut table = Table::new();
    table.set_header(vec!["Name", "URL", "Auth"]);

    for remote in remotes {
        let url = match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => base_url.clone(),
            RemoteEndpoint::Sse { events_url } => format!("(sse) {}", events_url),
            RemoteEndpoint::Storage { prefix } => format!("(storage) {}", prefix),
        };
        let auth = if remote.auth.token.is_some() {
            "token"
        } else {
            "none"
        };
        table.add_row(vec![
            Cell::new(remote.name.as_str()),
            Cell::new(url),
            Cell::new(auth),
        ]);
    }

    println!("{}", table);
    Ok(())
}

async fn run_show(store: &TomlSyncConfigStore, name: &str) -> CliResult<()> {
    let remote_name = RemoteName::new(name);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    println!("{}", "Remote:".bold());
    println!("  Name: {}", remote.name.as_str().green());

    match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => {
            println!("  Type: HTTP");
            println!("  URL:  {}", base_url);
        }
        RemoteEndpoint::Sse { events_url } => {
            println!("  Type: SSE");
            println!("  URL:  {}", events_url);
        }
        RemoteEndpoint::Storage { prefix } => {
            println!("  Type: Storage");
            println!("  Prefix: {}", prefix);
        }
    }

    println!(
        "  Auth: {}",
        if remote.auth.token.is_some() {
            "token configured"
        } else {
            "none"
        }
    );

    if let Some(interval) = remote.fetch_interval_secs {
        println!("  Fetch interval: {}s", interval);
    }

    Ok(())
}
