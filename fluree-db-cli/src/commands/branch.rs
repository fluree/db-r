use crate::cli::BranchAction;
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::ledger_id::split_ledger_id;

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
        BranchAction::Drop {
            name,
            ledger,
            remote,
        } => run_drop(&name, ledger.as_deref(), dirs, remote.as_deref(), direct).await,
        BranchAction::Rebase {
            name,
            ledger,
            strategy,
            remote,
        } => {
            run_rebase(
                &name,
                ledger.as_deref(),
                &strategy,
                dirs,
                remote.as_deref(),
                direct,
            )
            .await
        }
        BranchAction::Merge {
            source,
            target,
            ledger,
            remote,
        } => {
            run_merge(
                &source,
                target.as_deref(),
                ledger.as_deref(),
                dirs,
                remote.as_deref(),
                direct,
            )
            .await
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
        let (ledger_name, _) = split_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.create_branch(&ledger_name, name, from).await?;

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
            let (ledger_name, _) = split_ledger_id(&remote_alias)?;
            let result = client.create_branch(&ledger_name, name, from).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_branch_created(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let (ledger_name, _) = split_ledger_id(&alias)?;
            let record = fluree.create_branch(&ledger_name, name, from).await?;

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
        let (ledger_name, _) = split_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.list_branches(&ledger_name).await?;

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
            let (ledger_name, _) = split_ledger_id(&remote_alias)?;
            let result = client.list_branches(&ledger_name).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_branch_list_json(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let (ledger_name, _) = split_ledger_id(&alias)?;
            let records = fluree.list_branches(&ledger_name).await?;

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
        let source = branch.get("source").and_then(|v| v.as_str()).unwrap_or("-");
        table.add_row(vec![name.to_string(), t, source.to_string()]);
    }

    println!("{table}");
    Ok(())
}

// =============================================================================
// Drop
// =============================================================================

async fn run_drop(
    name: &str,
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let (ledger_name, _) = split_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.drop_branch(&ledger_name, name).await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        print_branch_dropped(&result)?;
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
            let (ledger_name, _) = split_ledger_id(&remote_alias)?;
            let result = client.drop_branch(&ledger_name, name).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_branch_dropped(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let (ledger_name, _) = split_ledger_id(&alias)?;
            let report = fluree.drop_branch(&ledger_name, name).await?;

            if report.deferred {
                println!(
                    "Branch '{}' retracted (has children, storage preserved).",
                    name
                );
            } else {
                println!("Dropped branch '{}'.", name);
            }
            if report.artifacts_deleted > 0 {
                println!("  Artifacts deleted: {}", report.artifacts_deleted);
            }
            if !report.cascaded.is_empty() {
                println!("  Cascaded drops: {}", report.cascaded.join(", "));
            }
            for warning in &report.warnings {
                eprintln!("  Warning: {}", warning);
            }
        }
    }

    Ok(())
}

// =============================================================================
// Rebase
// =============================================================================

async fn run_rebase(
    name: &str,
    ledger: Option<&str>,
    strategy: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let (ledger_name, _) = split_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client
            .rebase_branch(&ledger_name, name, Some(strategy))
            .await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        print_rebase_result(&result)?;
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

    let conflict_strategy = fluree_db_api::ConflictStrategy::from_str_name(strategy)
        .ok_or_else(|| CliError::Config(format!("Unknown conflict strategy: {}", strategy)))?;

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let (ledger_name, _) = split_ledger_id(&remote_alias)?;
            let result = client
                .rebase_branch(&ledger_name, name, Some(strategy))
                .await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_rebase_result(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let (ledger_name, _) = split_ledger_id(&alias)?;
            let report = fluree
                .rebase_branch(&ledger_name, name, conflict_strategy)
                .await?;

            if report.fast_forward {
                println!(
                    "Fast-forward rebase of '{}' to t={}.",
                    name, report.new_branch_point_t
                );
            } else {
                println!(
                    "Rebased '{}': {} commits replayed, {} skipped, {} conflicts, {} failures.",
                    name,
                    report.replayed,
                    report.skipped,
                    report.conflicts.len(),
                    report.failures.len(),
                );
                println!("  New branch point: t={}", report.new_branch_point_t);
            }
        }
    }

    Ok(())
}

fn print_rebase_result(result: &serde_json::Value) -> CliResult<()> {
    let fast_forward = result
        .get("fast_forward")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let branch = result
        .get("branch")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let new_t = result
        .get("new_branch_point_t")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if fast_forward {
        println!("Fast-forward rebase of '{}' to t={}.", branch, new_t);
    } else {
        let replayed = result.get("replayed").and_then(|v| v.as_u64()).unwrap_or(0);
        let skipped = result.get("skipped").and_then(|v| v.as_u64()).unwrap_or(0);
        let conflicts = result
            .get("conflicts")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let failures = result.get("failures").and_then(|v| v.as_u64()).unwrap_or(0);

        println!(
            "Rebased '{}': {} commits replayed, {} skipped, {} conflicts, {} failures.",
            branch, replayed, skipped, conflicts, failures,
        );
        println!("  New branch point: t={}", new_t);
    }
    Ok(())
}

async fn run_merge(
    source: &str,
    target: Option<&str>,
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let (ledger_name, _) = split_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.merge_branch(&ledger_name, source, target).await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        print_merge_result(&result)?;
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
            let (ledger_name, _) = split_ledger_id(&remote_alias)?;
            let result = client.merge_branch(&ledger_name, source, target).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_merge_result(&result)?;
        }
        LedgerMode::Local { fluree, alias } => {
            let (ledger_name, _) = split_ledger_id(&alias)?;

            let report = fluree.merge_branch(&ledger_name, source, target).await?;

            println!(
                "Merged '{}' into '{}' (fast-forward to t={}, {} commits copied).",
                report.source, report.target, report.new_head_t, report.commits_copied,
            );
        }
    }

    Ok(())
}

fn print_merge_result(result: &serde_json::Value) -> CliResult<()> {
    let source = result
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let target = result
        .get("target")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let new_t = result
        .get("new_head_t")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let commits_copied = result
        .get("commits_copied")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    println!(
        "Merged '{}' into '{}' (fast-forward to t={}, {} commits copied).",
        source, target, new_t, commits_copied,
    );
    Ok(())
}

fn print_branch_dropped(result: &serde_json::Value) -> CliResult<()> {
    let ledger_id = result
        .get("ledger_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let deferred = result
        .get("deferred")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if deferred {
        println!(
            "Branch retracted (has children, storage preserved): {}",
            ledger_id
        );
    } else {
        println!("Dropped branch: {}", ledger_id);
    }

    if let Some(artifacts) = result.get("files_deleted").and_then(|v| v.as_u64()) {
        if artifacts > 0 {
            println!("  Artifacts deleted: {}", artifacts);
        }
    }
    if let Some(cascaded) = result.get("cascaded").and_then(|v| v.as_array()) {
        if !cascaded.is_empty() {
            let names: Vec<&str> = cascaded.iter().filter_map(|v| v.as_str()).collect();
            println!("  Cascaded drops: {}", names.join(", "));
        }
    }
    if let Some(warnings) = result.get("warnings").and_then(|v| v.as_array()) {
        for w in warnings {
            if let Some(msg) = w.as_str() {
                eprintln!("  Warning: {}", msg);
            }
        }
    }
    Ok(())
}
