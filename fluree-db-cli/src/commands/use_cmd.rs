use crate::config::{self, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_nameservice::NameService;
use std::path::Path;

pub async fn run(ledger: &str, fluree_dir: &Path) -> CliResult<()> {
    let fluree = context::build_fluree(fluree_dir)?;
    let ledger_id = context::to_ledger_id(ledger);

    // Check if it's a local ledger
    let record = fluree.nameservice().lookup(&ledger_id).await?;
    if record.is_some() {
        config::write_active_ledger(fluree_dir, ledger)?;
        println!("Now using ledger '{}'", ledger);
        return Ok(());
    }

    // Check if it's a tracked ledger
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    if store.get_tracked(ledger).is_some() || store.get_tracked(&ledger_id).is_some() {
        config::write_active_ledger(fluree_dir, ledger)?;
        println!("Now using tracked ledger '{}'", ledger);
        return Ok(());
    }

    Err(CliError::NotFound(format!("ledger '{}' not found", ledger)))
}
