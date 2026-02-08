use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_nameservice::NameService;
use std::path::Path;

pub async fn run(
    ledger: Option<&str>,
    fluree_dir: &Path,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    // Resolve ledger mode: --remote flag, local, or tracked
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, fluree_dir)?;
        context::build_remote_mode(remote_name, &alias, fluree_dir).await?
    } else {
        context::resolve_ledger_mode(ledger, fluree_dir).await?
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            local_alias,
            remote_name,
        } => {
            let info = client.ledger_info(&remote_alias).await?;

            context::persist_refreshed_tokens(&client, &remote_name, fluree_dir).await;

            println!(
                "Ledger:         {} (tracked)",
                info.get("ledger")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&local_alias)
            );
            if let Some(t) = info.get("t").and_then(|v| v.as_i64()) {
                println!("t:              {}", t);
            }
            if let Some(commit) = info.get("commit_address").and_then(|v| v.as_str()) {
                println!("Commit address: {}", commit);
            }
            if let Some(index) = info.get("index_address").and_then(|v| v.as_str()) {
                println!("Index address:  {}", index);
            }

            // Print full JSON if there are stats
            if info.get("stats").is_some() {
                println!();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&info).unwrap_or_default()
                );
            }
        }
        LedgerMode::Local { fluree, alias } => {
            let address = context::to_ledger_address(&alias);
            let record = fluree
                .nameservice()
                .lookup(&address)
                .await?
                .ok_or_else(|| CliError::NotFound(format!("ledger '{}' not found", alias)))?;

            println!("Ledger:         {}", record.name);
            println!("Branch:         {}", record.branch);
            println!("Address:        {}", record.address);
            println!("Commit t:       {}", record.commit_t);
            println!(
                "Commit address: {}",
                record.commit_address.as_deref().unwrap_or("(none)")
            );
            println!("Index t:        {}", record.index_t);
            println!(
                "Index address:  {}",
                record.index_address.as_deref().unwrap_or("(none)")
            );
        }
    }

    Ok(())
}
