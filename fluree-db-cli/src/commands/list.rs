use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_nameservice::NameService;

pub async fn run(dirs: &FlureeDir, remote_flag: Option<&str>) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        return run_remote(remote_name, dirs).await;
    }

    let fluree = context::build_fluree(dirs)?;
    let active = config::read_active_ledger(dirs.data_dir());
    let records = fluree.nameservice().all_records().await?;

    // Filter out retracted records
    let active_records: Vec<_> = records.iter().filter(|r| !r.retracted).collect();

    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let tracked = store.tracked_ledgers();

    if active_records.is_empty() && tracked.is_empty() {
        println!("No ledgers found. Run 'fluree create <name>' to create one.");
        return Ok(());
    }

    // Local ledgers
    if !active_records.is_empty() {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec!["", "LEDGER", "BRANCH", "T"]);

        for record in &active_records {
            let marker = if active.as_deref() == Some(&record.name) {
                "*"
            } else {
                " "
            };
            table.add_row(vec![
                marker.to_string(),
                record.name.clone(),
                record.branch.clone(),
                record.commit_t.to_string(),
            ]);
        }

        println!("{table}");
    }

    // Tracked ledgers
    if !tracked.is_empty() {
        if !active_records.is_empty() {
            println!();
        }
        println!("Tracked:");
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec!["", "LEDGER", "REMOTE", "REMOTE ALIAS"]);

        for t in &tracked {
            let marker = if active.as_deref() == Some(&t.local_alias) {
                "*"
            } else {
                " "
            };
            table.add_row(vec![
                marker.to_string(),
                t.local_alias.clone(),
                t.remote.clone(),
                t.remote_alias.clone(),
            ]);
        }

        println!("{table}");
    }

    Ok(())
}

/// List ledgers on a remote server.
async fn run_remote(remote_name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let client = context::build_remote_client(remote_name, dirs).await?;

    let result = client.list_ledgers().await.map_err(|e| {
        CliError::Remote(format!(
            "failed to list ledgers on '{}': {}",
            remote_name, e
        ))
    })?;

    context::persist_refreshed_tokens(&client, remote_name, dirs).await;

    // Response should be a JSON array of ledger objects
    let ledgers = match result.as_array() {
        Some(arr) => arr,
        None => {
            return Err(CliError::Remote(
                "unexpected response format: expected JSON array".into(),
            ));
        }
    };

    if ledgers.is_empty() {
        println!("No ledgers on remote '{}'.", remote_name);
        return Ok(());
    }

    println!("Ledgers on remote '{}':", remote_name.green());

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["LEDGER", "T"]);

    for ledger in ledgers {
        let name = ledger
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown)");
        let t = ledger
            .get("t")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![name.to_string(), t]);
    }

    println!("{table}");
    Ok(())
}
