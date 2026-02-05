use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_nameservice::NameService;
use std::path::Path;

pub async fn run(ledger: Option<&str>, fluree_dir: &Path) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, fluree_dir)?;
    let fluree = context::build_fluree(fluree_dir)?;
    let address = context::to_ledger_address(&alias);

    let record = fluree
        .nameservice()
        .lookup(&address)
        .await?
        .ok_or_else(|| CliError::NotFound(format!("ledger '{}' not found", alias)))?;

    println!("Ledger:         {}", record.alias);
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

    Ok(())
}
