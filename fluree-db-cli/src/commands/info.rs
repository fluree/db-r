use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::NameService;

pub async fn run(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    // Resolve ledger mode: --remote flag, local, or tracked
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        context::resolve_ledger_mode(ledger, dirs).await?
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            local_alias,
            remote_name,
        } => {
            let info = client.ledger_info(&remote_alias).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            println!(
                "Ledger:         {} (tracked)",
                info.get("ledger")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&local_alias)
            );
            if let Some(t) = info.get("t").and_then(|v| v.as_i64()) {
                println!("t:              {}", t);
            }
            if let Some(commit) = info
                .get("commitId")
                .and_then(|v| v.as_str())
                .or_else(|| info.get("commit_head_id").and_then(|v| v.as_str()))
            {
                println!("Commit ID:      {}", commit);
            }
            if let Some(index) = info
                .get("indexId")
                .and_then(|v| v.as_str())
                .or_else(|| info.get("index_head_id").and_then(|v| v.as_str()))
            {
                println!("Index ID:       {}", index);
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
            let ledger_id = context::to_ledger_id(&alias);
            let record = fluree
                .nameservice()
                .lookup(&ledger_id)
                .await?
                .ok_or_else(|| CliError::NotFound(format!("ledger '{}' not found", alias)))?;

            println!("Ledger:         {}", record.name);
            println!("Branch:         {}", record.branch);
            println!("Ledger ID:      {}", record.ledger_id);
            println!("Commit t:       {}", record.commit_t);
            println!(
                "Commit ID:      {}",
                record
                    .commit_head_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .as_deref()
                    .unwrap_or("(none)")
            );
            println!("Index t:        {}", record.index_t);
            println!(
                "Index ID:       {}",
                record
                    .index_head_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .as_deref()
                    .unwrap_or("(none)")
            );
        }
    }

    Ok(())
}
