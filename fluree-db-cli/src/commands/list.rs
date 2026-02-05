use crate::config;
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

    if active_records.is_empty() {
        println!("No ledgers found. Run 'fluree create <name>' to create one.");
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["", "LEDGER", "BRANCH", "T"]);

    for record in &active_records {
        let marker = if active.as_deref() == Some(&record.alias) {
            "*"
        } else {
            " "
        };
        table.add_row(vec![
            marker.to_string(),
            record.alias.clone(),
            record.branch.clone(),
            record.commit_t.to_string(),
        ]);
    }

    println!("{table}");
    Ok(())
}
