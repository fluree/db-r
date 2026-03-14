use crate::cli::BranchAction;
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(action: BranchAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        BranchAction::Create {
            name,
            ledger,
            from,
            remote,
        } => {
            run_create(
                &name,
                ledger.as_deref(),
                from.as_deref(),
                dirs,
                remote.as_deref(),
                direct,
            )
            .await
        }
        BranchAction::List { ledger, remote } => {
            run_list(ledger.as_deref(), dirs, remote.as_deref(), direct).await
        }
    }
}

// =============================================================================
// Create
// =============================================================================

async fn run_create(
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
            let record = fluree.create_branch(ledger_name, name, from).await?;

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

fn print_branch_created(result: &serde_json::Value) -> CliResult<()> {
    let branch = result
        .get("branch")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let source = result
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("main");
    let t = result.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
    let ledger_id = result
        .get("ledger_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    println!("Created branch '{}' from '{}' at t={}", branch, source, t);
    println!("Ledger ID: {}", ledger_id);
    Ok(())
}

// =============================================================================
// List
// =============================================================================

async fn run_list(
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

/// Extract the ledger name (without branch suffix) from a ledger alias.
fn extract_ledger_name(alias: &str) -> &str {
    alias.split(':').next().unwrap_or(alias)
}
