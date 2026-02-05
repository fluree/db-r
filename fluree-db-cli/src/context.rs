use crate::config;
use crate::error::{CliError, CliResult};
use fluree_db_api::{FileStorage, Fluree, FlureeBuilder};
use fluree_db_nameservice::file::FileNameService;
use std::path::Path;

/// Resolve which ledger to operate on.
///
/// Priority: explicit argument > active ledger > error.
pub fn resolve_ledger(explicit: Option<&str>, fluree_dir: &Path) -> CliResult<String> {
    if let Some(alias) = explicit {
        return Ok(alias.to_string());
    }
    config::read_active_ledger(fluree_dir).ok_or(CliError::NoActiveLedger)
}

/// Build a Fluree instance backed by the `.fluree/storage/` directory.
pub fn build_fluree(fluree_dir: &Path) -> CliResult<Fluree<FileStorage, FileNameService>> {
    let storage = config::storage_path(fluree_dir);
    let storage_str = storage.to_string_lossy().to_string();
    FlureeBuilder::file(storage_str)
        .build()
        .map_err(|e| CliError::Config(format!("failed to initialize Fluree: {e}")))
}

/// Normalize an alias to include a branch suffix if missing.
///
/// The nameservice uses canonical addresses like `mydb:main`.
/// When users provide just `mydb`, we append `:main`.
pub fn to_ledger_address(alias: &str) -> String {
    if alias.contains(':') {
        alias.to_string()
    } else {
        format!("{alias}:main")
    }
}
