use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let ledger_name = extract_ledger_name(&alias);
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.list_branches(ledger_name).await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        return print_branch_list_json(&result);
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
            let result = client.list_branches(ledger_name).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_branch_list_json(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_name = extract_ledger_name(&alias);
            let records = fluree.list_branches(ledger_name).await?;

            if records.is_empty() {
                println!("No branches found for '{}'.", ledger_name);
                return Ok(());
            }

            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec!["BRANCH", "T", "SOURCE"]);

            for record in &records {
                let source = record
                    .branch_point
                    .as_ref()
                    .map(|bp| bp.source.as_str())
                    .unwrap_or("-");
                table.add_row(vec![
                    record.branch.clone(),
                    record.commit_t.to_string(),
                    source.to_string(),
                ]);
            }

            println!("{table}");
        }
    }

    Ok(())
}

/// Extract the ledger name (without branch suffix) from a ledger alias.
fn extract_ledger_name(alias: &str) -> &str {
    alias.split(':').next().unwrap_or(alias)
}

fn print_branch_list_json(result: &serde_json::Value) -> CliResult<()> {
    let branches = match result.as_array() {
        Some(arr) => arr,
        None => {
            return Err(CliError::Remote(
                "unexpected response format: expected JSON array".into(),
            ));
        }
    };

    if branches.is_empty() {
        println!("No branches found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["BRANCH", "T", "SOURCE"]);

    for branch in branches {
        let name = branch
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown)");
        let t = branch
            .get("t")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let source = branch
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        table.add_row(vec![name.to_string(), t, source.to_string()]);
    }

    println!("{table}");
    Ok(())
}
