use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_nameservice::NameService;
use std::path::Path;

pub async fn run(ledger: &str, fluree_dir: &Path) -> CliResult<()> {
    let fluree = context::build_fluree(fluree_dir)?;
    let address = context::to_ledger_address(ledger);

    // Validate ledger exists
    let record = fluree.nameservice().lookup(&address).await?;
    if record.is_none() {
        return Err(CliError::NotFound(format!("ledger '{}' not found", ledger)));
    }

    config::write_active_ledger(fluree_dir, ledger)?;
    println!("Now using ledger '{}'", ledger);
    Ok(())
}
