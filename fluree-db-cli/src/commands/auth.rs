//! Auth management commands: status, login, logout
//!
//! Manages bearer tokens stored in remote configs. Tokens are stored
//! in `.fluree/config.toml` as part of the remote's `auth.token` field.
//!
//! Token values are never printed to stdout — `status` shows presence,
//! expiry, and identity only.

use crate::cli::AuthAction;
use crate::config::TomlSyncConfigStore;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::SyncConfigStore;
use std::io::{self, Read};
use std::path::Path;

pub async fn run(action: AuthAction, fluree_dir: &Path) -> CliResult<()> {
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());

    match action {
        AuthAction::Status { remote } => run_status(&store, remote.as_deref()).await,
        AuthAction::Login { remote, token } => run_login(&store, remote.as_deref(), token).await,
        AuthAction::Logout { remote } => run_logout(&store, remote.as_deref()).await,
    }
}

/// Resolve which remote to use: explicit name, or the only configured remote.
async fn resolve_remote_name(
    store: &TomlSyncConfigStore,
    explicit: Option<&str>,
) -> CliResult<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    let remotes = store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    match remotes.len() {
        0 => Err(CliError::Config(
            "no remotes configured. Use `fluree remote add <name> <url>` first.".to_string(),
        )),
        1 => Ok(remotes[0].name.as_str().to_string()),
        _ => Err(CliError::Usage(
            "multiple remotes configured; specify one with --remote <name>".to_string(),
        )),
    }
}

async fn run_status(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    println!("{}", "Auth Status:".bold());
    println!("  Remote: {}", name.green());

    match &config.auth.token {
        Some(token) => {
            println!("  Token:  {}", "configured".green());

            // Decode token claims without verification to show expiry/identity
            match decode_token_summary(token) {
                Ok(summary) => {
                    if let Some(exp) = summary.expiry {
                        println!("  Expiry: {}", exp);
                    } else {
                        println!("  Expiry: {}", "no expiry claim".yellow());
                    }
                    if let Some(identity) = &summary.identity {
                        println!("  Identity: {}", identity);
                    }
                    if let Some(issuer) = &summary.issuer {
                        println!("  Issuer: {}", issuer);
                    }
                    if let Some(subject) = &summary.subject {
                        println!("  Subject: {}", subject);
                    }
                }
                Err(_) => {
                    println!(
                        "  {}",
                        "(could not decode token claims)".yellow()
                    );
                }
            }
        }
        None => {
            println!("  Token:  {}", "not configured".yellow());
            println!(
                "  {} fluree auth login --remote {}",
                "hint:".cyan().bold(),
                name
            );
        }
    }

    Ok(())
}

async fn run_login(
    store: &TomlSyncConfigStore,
    remote: Option<&str>,
    token_arg: Option<String>,
) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    // Read token from argument, file, or stdin
    let token = match token_arg {
        Some(t) if t == "@-" => {
            // Read from stdin
            let mut buf = String::new();
            io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token from stdin: {e}")))?;
            buf.trim().to_string()
        }
        Some(t) if t.starts_with('@') => {
            // Read from file
            let path = t.strip_prefix('@').unwrap();
            let expanded = shellexpand::tilde(path);
            std::fs::read_to_string(expanded.as_ref())
                .map_err(|e| CliError::Input(format!("failed to read token file: {e}")))?
                .trim()
                .to_string()
        }
        Some(t) => t,
        None => {
            // Prompt: read from stdin without echo hint
            eprintln!("Paste token (then press Enter):");
            let mut buf = String::new();
            io::stdin()
                .read_line(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read token: {e}")))?;
            buf.trim().to_string()
        }
    };

    if token.is_empty() {
        return Err(CliError::Input("token cannot be empty".to_string()));
    }

    config.auth.token = Some(token);
    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token stored for remote '{}'", name.green());

    // Show a brief summary of what was stored
    if let Some(ref tok) = config.auth.token {
        if let Ok(summary) = decode_token_summary(tok) {
            if let Some(exp) = summary.expiry {
                println!("  Expiry: {}", exp);
            }
            if let Some(identity) = &summary.identity {
                println!("  Identity: {}", identity);
            }
        }
    }

    Ok(())
}

async fn run_logout(store: &TomlSyncConfigStore, remote: Option<&str>) -> CliResult<()> {
    let name = resolve_remote_name(store, remote).await?;
    let remote_name = RemoteName::new(&name);
    let mut config = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", name)))?;

    if config.auth.token.is_none() {
        println!("No token stored for remote '{}'", name);
        return Ok(());
    }

    config.auth.token = None;
    store
        .set_remote(&config)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    println!("Token cleared for remote '{}'", name.green());
    Ok(())
}

// =============================================================================
// Token summary decoding (no verification)
// =============================================================================

struct TokenSummary {
    expiry: Option<String>,
    identity: Option<String>,
    issuer: Option<String>,
    subject: Option<String>,
}

/// Decode a JWT/JWS payload without verification to extract summary info.
fn decode_token_summary(token: &str) -> Result<TokenSummary, ()> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(());
    }

    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| ())?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).map_err(|_| ())?;

    let expiry = claims.get("exp").and_then(|v| v.as_u64()).map(|exp| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if exp < now {
            format!("{} (expired)", format_timestamp(exp)).red().to_string()
        } else {
            format_timestamp(exp)
        }
    });

    let identity = claims
        .get("fluree.identity")
        .and_then(|v| v.as_str())
        .map(String::from);

    let issuer = claims.get("iss").and_then(|v| v.as_str()).map(String::from);

    let subject = claims.get("sub").and_then(|v| v.as_str()).map(String::from);

    Ok(TokenSummary {
        expiry,
        identity,
        issuer,
        subject,
    })
}

/// Format a unix timestamp as a human-readable string.
fn format_timestamp(ts: u64) -> String {
    // Use chrono-free formatting: just show the UTC time
    let secs = ts;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;

    // Simple date calculation (good enough for display)
    // Days from 1970-01-01
    let mut y = 1970i64;
    let mut remaining = days_since_epoch as i64;
    loop {
        let days_in_year = if is_leap_year(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let mut m = 1u32;
    let days_in_month = [31, 28 + i64::from(is_leap_year(y)), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for &dim in &days_in_month {
        if remaining < dim {
            break;
        }
        remaining -= dim;
        m += 1;
    }
    let d = remaining + 1;

    format!("{y:04}-{m:02}-{d:02} {hours:02}:{minutes:02} UTC")
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
