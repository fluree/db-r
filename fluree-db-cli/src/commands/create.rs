use crate::config;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use std::path::Path;

pub async fn run(
    ledger: &str,
    from: Option<&Path>,
    fluree_dir: &Path,
    verbose: bool,
) -> CliResult<()> {
    let fluree = context::build_fluree(fluree_dir)?;

    match from {
        Some(path) if path.is_dir() => {
            // Bulk import from directory
            if verbose {
                println!("Importing from directory: {}", path.display());
            }
            let result = fluree
                .create(ledger)
                .import(path)
                .execute()
                .await?;

            config::write_active_ledger(fluree_dir, ledger)?;
            println!(
                "Created ledger '{}' (imported {} flakes, t={})",
                ledger, result.flake_count, result.t
            );
        }
        Some(path) => {
            // Single file import
            let content = std::fs::read_to_string(path).map_err(|e| {
                CliError::Input(format!("failed to read {}: {e}", path.display()))
            })?;
            let format = detect::detect_data_format(Some(path), &content, None)?;

            // Create the ledger first
            fluree.create_ledger(ledger).await?;

            // Insert data
            let result = match format {
                detect::DataFormat::Turtle => {
                    fluree
                        .graph(ledger)
                        .transact()
                        .insert_turtle(&content)
                        .commit()
                        .await?
                }
                detect::DataFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    fluree
                        .graph(ledger)
                        .transact()
                        .insert(&json)
                        .commit()
                        .await?
                }
            };

            config::write_active_ledger(fluree_dir, ledger)?;
            println!(
                "Created ledger '{}' ({} flakes, t={})",
                ledger, result.receipt.flake_count, result.receipt.t
            );
        }
        None => {
            // Create empty ledger
            fluree.create_ledger(ledger).await?;
            config::write_active_ledger(fluree_dir, ledger)?;
            println!("Created ledger '{}'", ledger);
        }
    }

    Ok(())
}
