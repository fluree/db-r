use crate::commands::insert::{print_txn_result, resolve_positional_args};
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::CliResult;
use crate::input;
use fluree_db_api::CommitOpts;
use std::path::Path;

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
            ..
        } => {
            let result = match data_format {
                detect::DataFormat::Turtle => client.upsert_turtle(&remote_alias, &content).await?,
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    client.upsert_jsonld(&remote_alias, &json).await?
                }
            };

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
                        .upsert_turtle(&content)
                        .commit_opts(commit_opts)
                        .commit()
                        .await?
                }
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    fluree
                        .graph(&alias)
                        .transact()
                        .upsert(&json)
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
