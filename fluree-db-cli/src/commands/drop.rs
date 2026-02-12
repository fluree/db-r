use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::admin::DropStatus;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(name: &str, force: bool, dirs: &FlureeDir) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of ledger '{name}'"
        )));
    }

    let fluree = context::build_fluree(dirs)?;
    let report = fluree
        .drop_ledger(name, fluree_db_api::DropMode::Soft)
        .await?;

    match report.status {
        DropStatus::Dropped => {
            // If dropped ledger was active, clear it
            let active = config::read_active_ledger(dirs.data_dir());
            if active.as_deref() == Some(name) {
                config::clear_active_ledger(dirs.data_dir())?;
            }
            println!("Dropped ledger '{name}'");
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
