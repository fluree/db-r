use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::admin::DropStatus;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    name: &str,
    force: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of ledger '{name}'"
        )));
    }

    if let Some(remote_name) = remote_flag {
        return run_remote(name, remote_name, dirs).await;
    }

    let fluree = context::build_fluree(dirs)?;
    let report = fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await?;

    match report.status {
        DropStatus::Dropped => {
            // If dropped ledger was active, clear it
            let active = config::read_active_ledger(dirs.data_dir());
            if active.as_deref() == Some(name) {
                config::clear_active_ledger(dirs.data_dir())?;
            }
            if report.artifacts_deleted > 0 {
                println!(
                    "Dropped ledger '{name}' (deleted {} artifacts)",
                    report.artifacts_deleted
                );
            } else {
                println!("Dropped ledger '{name}'");
            }
            for w in &report.warnings {
                eprintln!("  warning: {w}");
            }
        }
        DropStatus::AlreadyRetracted => {
            println!("Ledger '{name}' was already dropped");
        }
        DropStatus::NotFound => {
            return Err(CliError::NotFound(format!("ledger '{name}' not found")));
        }
    }

    Ok(())
}

async fn run_remote(ledger: &str, remote_name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let client = context::build_remote_client(remote_name, dirs).await?;
    let result = client.drop_ledger(ledger, true).await?;

    context::persist_refreshed_tokens(&client, remote_name, dirs).await;

    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match status {
        "dropped" => {
            let msg = if let Some(n) = result.get("files_deleted").and_then(|v| v.as_u64()) {
                format!("Dropped ledger '{ledger}' on remote '{remote_name}' ({n} files deleted)")
            } else {
                format!("Dropped ledger '{ledger}' on remote '{remote_name}'")
            };
            println!("{msg}");
            if let Some(warnings) = result.get("warnings").and_then(|v| v.as_array()) {
                for w in warnings {
                    if let Some(s) = w.as_str() {
                        eprintln!("  warning: {s}");
                    }
                }
            }
        }
        "already_retracted" => {
            println!("Ledger '{ledger}' was already dropped on remote '{remote_name}'");
        }
        "not_found" => {
            return Err(CliError::NotFound(format!(
                "ledger '{ledger}' not found on remote '{remote_name}'"
            )));
        }
        other => {
            println!("Remote drop returned status: {other}");
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_default()
            );
        }
    }

    Ok(())
}
