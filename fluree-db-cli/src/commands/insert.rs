use crate::context;
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
) -> CliResult<()> {
    let (explicit_ledger, file_path) = resolve_positional_args(args);
    let alias = context::resolve_ledger(explicit_ledger, fluree_dir)?;
    let fluree = context::build_fluree(fluree_dir)?;

    // Resolve input
    let source = input::resolve_input(file_path.as_deref(), expr)?;
    let content = input::read_input(&source)?;

    // Detect format
    let data_format = detect::detect_data_format(file_path.as_deref(), &content, format_flag)?;

    // Build commit options
    let commit_opts = CommitOpts {
        message: message.map(String::from),
        ..Default::default()
    };

    // Execute transaction
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

    Ok(())
}
