//! Remote management commands: add, remove, list, show

use crate::cli::RemoteAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};
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

    // Build initial auth config
    let mut auth = RemoteAuth {
        token: auth_token,
        ..Default::default()
    };

    // Attempt auth discovery from /.well-known/fluree.json (non-fatal)
    if auth.token.is_none() {
        match discover_auth(&base_url).await {
            Ok(Some(discovered)) => {
                auth = discovered;
                eprintln!(
                    "  {} auto-discovered OIDC auth from server",
                    "info:".cyan().bold()
                );
                if let Some(ref issuer) = auth.issuer {
                    eprintln!("  Issuer: {}", issuer);
                }
                eprintln!(
                    "  Run `fluree auth login --remote {}` to authenticate",
                    name
                );
            }
            Ok(None) => {
                // No discovery endpoint or unsupported type — that's fine
            }
            Err(msg) => {
                eprintln!(
                    "  {} auth discovery failed: {}",
                    "warn:".yellow().bold(),
                    msg
                );
            }
        }
    }

    let config = RemoteConfig {
        name: RemoteName::new(name),
        endpoint: RemoteEndpoint::Http { base_url },
        auth,
        fetch_interval_secs: None,
    };

    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Added remote '{}'", name.green());
    Ok(())
}

/// Attempt to fetch `/.well-known/fluree.json` from the remote and parse
/// OIDC auth configuration. Returns `Ok(None)` if the endpoint doesn't
/// exist or the auth type is not supported.
pub(crate) async fn discover_auth(base_url: &str) -> Result<Option<RemoteAuth>, String> {
    let discovery_url = format!("{}/.well-known/fluree.json", base_url);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| e.to_string())?;

    let resp = match client.get(&discovery_url).send().await {
        Ok(r) => r,
        Err(e) if e.is_connect() || e.is_timeout() => {
            // Server not reachable yet — perfectly normal during setup
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };

    if !resp.status().is_success() {
        return Ok(None);
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;

    // Parse the discovery payload
    let auth_obj = match body.get("auth") {
        Some(a) if a.is_object() => a,
        _ => return Ok(None),
    };

    let auth_type_str = auth_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match auth_type_str {
        "oidc_device" => {
            let issuer = auth_obj
                .get("issuer")
                .and_then(|v| v.as_str())
                .ok_or("oidc_device discovery missing 'issuer' field")?
                .to_string();

            let client_id = auth_obj
                .get("client_id")
                .and_then(|v| v.as_str())
                .ok_or("oidc_device discovery missing 'client_id' field")?
                .to_string();

            let exchange_url = auth_obj
                .get("exchange_url")
                .and_then(|v| v.as_str())
                .ok_or("oidc_device discovery missing 'exchange_url' field")?
                .to_string();

            Ok(Some(RemoteAuth {
                auth_type: Some(RemoteAuthType::OidcDevice),
                issuer: Some(issuer),
                client_id: Some(client_id),
                exchange_url: Some(exchange_url),
                ..Default::default()
            }))
        }
        "token" | "" => Ok(None), // manual token mode — nothing to auto-configure
        other => {
            eprintln!(
                "  {} unknown auth type '{}' in discovery — ignoring",
                "warn:".yellow().bold(),
                other
            );
            Ok(None)
        }
    }
}

async fn run_remove(store: &TomlSyncConfigStore, name: &str) -> CliResult<()> {
    let remote_name = RemoteName::new(name);

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
        let auth = auth_display_short(&remote.auth);
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

    // Auth details
    let auth = &remote.auth;
    match auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            println!("  Auth: {}", "oidc_device".cyan());
            if let Some(ref issuer) = auth.issuer {
                println!("  Issuer: {}", issuer);
            }
            if let Some(ref client_id) = auth.client_id {
                println!("  Client ID: {}", client_id);
            }
            if auth.token.is_some() {
                println!("  Token: {}", "cached".green());
            } else {
                println!("  Token: {}", "not logged in".yellow());
            }
        }
        Some(RemoteAuthType::Token) | None => {
            if auth.token.is_some() {
                println!("  Auth: token configured");
            } else {
                println!("  Auth: none");
            }
        }
    }

    if let Some(interval) = remote.fetch_interval_secs {
        println!("  Fetch interval: {}s", interval);
    }

    Ok(())
}

/// Short auth description for the list table.
fn auth_display_short(auth: &RemoteAuth) -> &'static str {
    match auth.auth_type.as_ref() {
        Some(RemoteAuthType::OidcDevice) => {
            if auth.token.is_some() {
                "oidc (logged in)"
            } else {
                "oidc (not logged in)"
            }
        }
        Some(RemoteAuthType::Token) | None => {
            if auth.token.is_some() {
                "token"
            } else {
                "none"
            }
        }
    }
}
