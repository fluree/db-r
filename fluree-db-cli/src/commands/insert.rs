use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::CliResult;
use crate::input;
use fluree_db_api::CommitOpts;
use std::path::{Path, PathBuf};

/// Resolve positional args for insert/query commands.
///
/// - 0 args: active ledger + stdin/-e
/// - 1 arg: if file exists → active ledger + that file; else → ledger alias + stdin/-e
/// - 2 args: first = ledger alias, second = file path
pub fn resolve_positional_args(args: &[String]) -> (Option<&str>, Option<PathBuf>) {
    match args.len() {
        0 => (None, None),
        1 => {
            let p = Path::new(&args[0]);
            if p.is_file() {
                (None, Some(p.to_path_buf()))
            } else {
                (Some(&args[0]), None)
            }
        }
        _ => (Some(&args[0]), Some(PathBuf::from(&args[1]))),
    }
}

pub async fn run(
    args: &[String],
    expr: Option<&str>,
    message: Option<&str>,
    format_flag: Option<&str>,
    fluree_dir: &Path,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    let (explicit_ledger, file_path) = resolve_positional_args(args);

    // Resolve input
    let source = input::resolve_input(file_path.as_deref(), expr)?;
    let content = input::read_input(&source)?;

    // Detect format
    let data_format = detect::detect_data_format(file_path.as_deref(), &content, format_flag)?;

    // Resolve ledger mode: --remote flag, local, or tracked
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(explicit_ledger, fluree_dir)?;
        context::build_remote_mode(remote_name, &alias, fluree_dir).await?
    } else {
        context::resolve_ledger_mode(explicit_ledger, fluree_dir).await?
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let result = match data_format {
                detect::DataFormat::Turtle => client.insert_turtle(&remote_alias, &content).await?,
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    client.insert_jsonld(&remote_alias, &json).await?
                }
            };

            context::persist_refreshed_tokens(&client, &remote_name, fluree_dir).await;

            // Display server response fields
            print_txn_result(&result);
        }
        LedgerMode::Local { fluree, alias } => {
            let commit_opts = CommitOpts {
                message: message.map(String::from),
                ..Default::default()
            };

            let result = match data_format {
                detect::DataFormat::Turtle => {
                    fluree
                        .graph(&alias)
                        .transact()
                        .insert_turtle(&content)
                        .commit_opts(commit_opts)
                        .commit()
                        .await?
                }
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    fluree
                        .graph(&alias)
                        .transact()
                        .insert(&json)
                        .commit_opts(commit_opts)
                        .commit()
                        .await?
                }
            };

            println!(
                "Committed t={}, {} flakes",
                result.receipt.t, result.receipt.flake_count
            );
        }
    }

    Ok(())
}

/// Print transaction result from remote server JSON response.
pub fn print_txn_result(result: &serde_json::Value) {
    if let Some(t) = result.get("t").and_then(|v| v.as_i64()) {
        print!("Committed t={t}");
    }
    if let Some(tx_id) = result.get("tx-id").and_then(|v| v.as_str()) {
        print!(" tx={tx_id}");
    }
    println!();
}
