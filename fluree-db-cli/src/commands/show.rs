//! `fluree show <commit>` — display a decoded commit with resolved IRIs.

use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(commit: &str, ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    let fluree = context::build_fluree(dirs)?;

    // Accepts: "t:N" for transaction number, full CID, or abbreviated hex prefix
    let detail = if let Some(t_str) = commit.strip_prefix("t:") {
        let t: i64 = t_str
            .parse()
            .map_err(|_| CliError::Input(format!("Invalid transaction number: '{}'", t_str)))?;
        fluree
            .graph(&ledger_id)
            .commit_t(t)
            .execute()
            .await
            .map_err(CliError::Api)?
    } else {
        fluree
            .graph(&ledger_id)
            .commit_prefix(commit)
            .execute()
            .await
            .map_err(CliError::Api)?
    };

    // Output as pretty-printed JSON
    let json = serde_json::to_string_pretty(&detail)
        .map_err(|e| CliError::Input(format!("JSON serialization failed: {}", e)))?;
    println!("{}", json);

    Ok(())
}
