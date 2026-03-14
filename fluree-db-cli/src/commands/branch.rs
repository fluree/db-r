use crate::context::{self, LedgerMode};
use crate::error::CliResult;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    name: &str,
    ledger: Option<&str>,
    from: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let ledger_name = extract_ledger_name(&alias);
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.create_branch(ledger_name, name, from).await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        print_branch_created(&result)?;
        return Ok(());
    }

    // Auto-route to local server if available
    let mode = {
        let mode = context::resolve_ledger_mode(ledger, dirs).await?;
        if direct {
            mode
        } else {
            context::try_server_route(mode, dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let ledger_name = extract_ledger_name(&remote_alias);
            let result = client.create_branch(ledger_name, name, from).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_branch_created(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_name = extract_ledger_name(&alias);
            let record = fluree
                .create_branch(ledger_name, name, from)
                .await?;

            let source = record
                .branch_point
                .as_ref()
                .map(|bp| bp.source.as_str())
                .unwrap_or("main");
            let t = record.branch_point.as_ref().map(|bp| bp.t).unwrap_or(0);

            println!("Created branch '{}' from '{}' at t={}", name, source, t);
            println!("Ledger ID: {}", record.ledger_id);
        }
    }

    Ok(())
}

/// Extract the ledger name (without branch suffix) from a ledger alias.
fn extract_ledger_name(alias: &str) -> &str {
    alias.split(':').next().unwrap_or(alias)
}

fn print_branch_created(result: &serde_json::Value) -> CliResult<()> {
    let branch = result
        .get("branch")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let source = result
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("main");
    let t = result
        .get("t")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let ledger_id = result
        .get("ledger_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    println!("Created branch '{}' from '{}' at t={}", branch, source, t);
    println!("Ledger ID: {}", ledger_id);
    Ok(())
}
