use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::CliResult;
use comfy_table::{ContentArrangement, Table};
use fluree_db_nameservice::NameService;
use std::path::Path;

pub async fn run(fluree_dir: &Path) -> CliResult<()> {
    let fluree = context::build_fluree(fluree_dir)?;
    let active = config::read_active_ledger(fluree_dir);
    let records = fluree.nameservice().all_records().await?;

    // Filter out retracted records
    let active_records: Vec<_> = records.iter().filter(|r| !r.retracted).collect();

    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
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
