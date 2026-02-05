use crate::commands::insert::resolve_positional_args;
use crate::context;
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
) -> CliResult<()> {
    let (explicit_ledger, file_path) = resolve_positional_args(args);
    let alias = context::resolve_ledger(explicit_ledger, fluree_dir)?;
    let fluree = context::build_fluree(fluree_dir)?;

    // Resolve input
    let source = input::resolve_input(file_path.as_deref(), expr)?;
    let content = input::read_input(&source)?;

    // Detect format
    let data_format = detect::detect_data_format(
        file_path.as_deref(),
        &content,
        format_flag,
    )?;

    // Build commit options
    let commit_opts = CommitOpts {
        message: message.map(String::from),
        ..Default::default()
    };

    // Execute upsert transaction
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

    Ok(())
}
