use crate::config;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use std::path::Path;

/// Import tuning options passed from global + create-specific CLI flags.
pub struct ImportOpts {
    pub memory_budget_mb: usize,
    pub parallelism: usize,
    pub chunk_size_mb: usize,
}

pub async fn run(
    ledger: &str,
    from: Option<&Path>,
    fluree_dir: &Path,
    verbose: bool,
    import_opts: &ImportOpts,
) -> CliResult<()> {
    // Refuse if this alias is already tracked (mutual exclusion)
    let store = config::TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    if store.get_tracked(ledger).is_some() {
        return Err(CliError::Usage(format!(
            "alias '{}' is already used by a tracked ledger.\n  \
             Run `fluree track remove {}` first, or choose a different name.",
            ledger, ledger
        )));
    }

    let fluree = context::build_fluree(fluree_dir)?;

    match from {
        Some(path) if is_import_path(path)? => {
            // Bulk import: directory or Turtle file (any size).
            // The import pipeline handles both small (single-chunk) and large
            // (auto-split) files via resolve_chunk_source.
            if verbose {
                println!("Importing from: {}", path.display());
            }

            let mut builder = fluree.create(ledger).import(path);
            if import_opts.parallelism > 0 {
                builder = builder.parallelism(import_opts.parallelism);
            }
            if import_opts.memory_budget_mb > 0 {
                builder = builder.memory_budget_mb(import_opts.memory_budget_mb);
            }
            if import_opts.chunk_size_mb > 0 {
                builder = builder.chunk_size_mb(import_opts.chunk_size_mb);
            }
            let settings = builder.effective_import_settings();
            let mem_auto = import_opts.memory_budget_mb == 0;
            let par_auto = import_opts.parallelism == 0;
            eprintln!(
                "Import settings: memory budget {} MB{}, parallelism {}{}, chunk size {} MB, run budget {} MB",
                settings.memory_budget_mb,
                if mem_auto { " (auto)" } else { "" },
                settings.parallelism,
                if par_auto { " (auto)" } else { "" },
                settings.chunk_size_mb,
                settings.run_budget_mb,
            );
            let result = builder.execute().await?;

            config::write_active_ledger(fluree_dir, ledger)?;
            println!(
                "Created ledger '{}' (imported {} flakes, t={})",
                ledger, result.flake_count, result.t
            );
        }
        Some(path) => {
            // Non-Turtle single file (JSON-LD): staging path.
            let content = std::fs::read_to_string(path)
                .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))?;
            let format = detect::detect_data_format(Some(path), &content, None)?;

            // Create the ledger first
            fluree.create_ledger(ledger).await?;

            // Insert data
            let result = match format {
                detect::DataFormat::Turtle => {
                    // Shouldn't reach here (is_import_path catches .ttl), but handle anyway.
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

/// Whether this path should use the import pipeline.
///
/// - Directories → import (discovers chunk_*.ttl / chunk_*.trig files)
/// - `.ttl` files (case-insensitive) → import (auto-splits large files)
/// - `.ttl.gz` → error with helpful message
/// - Everything else → staging path (JSON-LD read + insert)
fn is_import_path(path: &Path) -> CliResult<bool> {
    if path.is_dir() {
        return Ok(true);
    }

    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let name_lower = name.to_ascii_lowercase();

    // Reject compressed Turtle with a clear message.
    if name_lower.ends_with(".ttl.gz")
        || name_lower.ends_with(".ttl.zst")
        || name_lower.ends_with(".ttl.bz2")
    {
        return Err(CliError::Input(format!(
            "compressed Turtle files are not yet supported; decompress first: {}",
            path.display()
        )));
    }

    // Case-insensitive .ttl check.
    if name_lower.ends_with(".ttl") {
        return Ok(true);
    }

    Ok(false)
}
