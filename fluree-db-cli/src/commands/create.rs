use crate::config;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
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
    dirs: &FlureeDir,
    verbose: bool,
    quiet: bool,
    import_opts: &ImportOpts,
) -> CliResult<()> {
    // Refuse if this alias is already tracked (mutual exclusion)
    let store = config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(ledger).is_some() {
        return Err(CliError::Usage(format!(
            "alias '{}' is already used by a tracked ledger.\n  \
             Run `fluree track remove {}` first, or choose a different name.",
            ledger, ledger
        )));
    }

    let fluree = context::build_fluree(dirs)?;

    match from {
        Some(path) if is_import_path(path)? => {
            // Bulk import: directory or Turtle file (any size).
            // The import pipeline handles both small (single-chunk) and large
            // (auto-split) files via resolve_chunk_source.
            run_bulk_import(
                &fluree,
                ledger,
                path,
                dirs.data_dir(),
                verbose,
                quiet,
                import_opts,
            )
            .await?;
        }
        Some(path) => {
            // Non-Turtle single file: detect format.
            let content = std::fs::read_to_string(path)
                .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))?;
            let format = detect::detect_data_format(Some(path), &content, None)?;

            match format {
                detect::DataFormat::Turtle => {
                    // Safety redirect: if a .ttl file reaches this branch
                    // (e.g., due to path/extension edge cases), always route
                    // through the import pipeline to avoid novelty limits.
                    run_bulk_import(
                        &fluree,
                        ledger,
                        path,
                        dirs.data_dir(),
                        verbose,
                        quiet,
                        import_opts,
                    )
                    .await?;
                }
                detect::DataFormat::JsonLd => {
                    // JSON-LD: create ledger + transact
                    fluree.create_ledger(ledger).await?;

                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    let result = fluree
                        .graph(ledger)
                        .transact()
                        .insert(&json)
                        .commit()
                        .await?;

                    config::write_active_ledger(dirs.data_dir(), ledger)?;
                    println!(
                        "Created ledger '{}' ({} flakes, t={})",
                        ledger, result.receipt.flake_count, result.receipt.t
                    );
                }
            }
        }
        None => {
            // Create empty ledger
            fluree.create_ledger(ledger).await?;
            config::write_active_ledger(dirs.data_dir(), ledger)?;
            println!("Created ledger '{}'", ledger);
        }
    }

    Ok(())
}

/// Run the bulk import pipeline for a Turtle file or directory.
///
/// Prints effective import settings (memory budget, parallelism, chunk size,
/// run budget) to stderr so the user can cancel if the values look excessive.
/// Shows a live progress bar unless `quiet` is set.
async fn run_bulk_import<S, N>(
    fluree: &fluree_db_api::Fluree<S, N>,
    ledger: &str,
    path: &Path,
    fluree_dir: &Path,
    verbose: bool,
    quiet: bool,
    import_opts: &ImportOpts,
) -> CliResult<()>
where
    S: fluree_db_core::storage::Storage + Clone + Send + Sync + 'static,
    N: fluree_db_nameservice::NameService
        + fluree_db_nameservice::Publisher
        + fluree_db_nameservice::ConfigPublisher
        + Clone
        + Send
        + Sync
        + 'static,
{
    use colored::Colorize;
    use fluree_db_api::ImportPhase;
    use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

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
    if !quiet {
        eprintln!(
            "Import settings: memory budget {} MB{}, parallelism {}{}, chunk size {} MB, run budget {} MB",
            settings.memory_budget_mb,
            if mem_auto { " (auto)" } else { "" },
            settings.parallelism,
            if par_auto { " (auto)" } else { "" },
            settings.chunk_size_mb,
            settings.run_budget_mb,
        );
        if mem_auto || par_auto {
            eprintln!("  Override with --memory-budget-mb and --parallelism");
        }
    }

    // Two progress bars shown simultaneously: Committing and Indexing.
    // The active phase advances while the other stays at 0% or 100%.
    let multi = if quiet {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    } else {
        MultiProgress::new()
    };

    let style =
        ProgressStyle::with_template("{prefix:12} {spinner:.dim} [{bar:25}] {percent:>3}%  {msg}")
            .unwrap()
            .tick_strings(&["|", "/", "-", "\\", " "])
            .progress_chars("=>-");

    let scan_bar = multi.add(ProgressBar::new(100));
    scan_bar.set_style(style.clone());
    scan_bar.set_prefix(format!("{}", "Reading".green().bold()));
    scan_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let commit_bar = multi.add(ProgressBar::new(100));
    commit_bar.set_style(style.clone());
    commit_bar.set_prefix(format!("{}", "Committing".green().bold()));
    commit_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let index_bar = multi.add(ProgressBar::new(100));
    index_bar.set_style(style);
    index_bar.set_prefix(format!("{}", "Indexing".green().bold()));
    index_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let sb = scan_bar.clone();
    let cb = commit_bar.clone();
    let ib = index_bar.clone();
    builder = builder.on_progress(move |phase| match phase {
        ImportPhase::Parsing {
            chunk,
            total,
            chunk_bytes,
        } => {
            let mb = chunk_bytes as f64 / (1024.0 * 1024.0);
            cb.set_length(total as u64);
            cb.set_position(chunk.saturating_sub(1) as u64);
            cb.set_message(format!("Parsing chunk {} ({:.0} MB)...", chunk, mb));
        }
        ImportPhase::Scanning {
            bytes_read,
            total_bytes,
        } => {
            sb.set_length(total_bytes);
            sb.set_position(bytes_read);
            let gb_read = bytes_read as f64 / (1024.0 * 1024.0 * 1024.0);
            let gb_total = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            sb.set_message(format!("{:.1} / {:.1} GB", gb_read, gb_total));
            if bytes_read >= total_bytes {
                sb.finish_with_message(format!("{:.1} GB", gb_total));
            }
        }
        ImportPhase::Committing {
            chunk,
            total,
            cumulative_flakes,
            elapsed_secs,
        } => {
            cb.set_length(total as u64);
            cb.set_position(chunk as u64);
            let rate = if elapsed_secs > 0.0 {
                cumulative_flakes as f64 / elapsed_secs / 1_000_000.0
            } else {
                0.0
            };
            cb.set_message(format!("{:.2} M flakes/s", rate));
        }
        ImportPhase::Indexing {
            merged_flakes,
            total_flakes,
            elapsed_secs,
        } => {
            cb.finish();
            ib.set_length(total_flakes);
            // Start at 1% minimum so the bar shows activity immediately
            let pos = if merged_flakes == 0 && total_flakes > 0 {
                total_flakes / 100
            } else {
                merged_flakes
            };
            ib.set_position(pos);
            let merged_m = merged_flakes as f64 / 1_000_000.0;
            let total_m = total_flakes as f64 / 1_000_000.0;
            let rate = if elapsed_secs > 0.0 {
                merged_m / elapsed_secs
            } else {
                0.0
            };
            ib.set_message(format!(
                "{:.1}M / {:.1}M flakes  {:.2} M/s",
                merged_m, total_m, rate
            ));
        }
        ImportPhase::Done => {
            ib.finish();
        }
    });

    let start = std::time::Instant::now();
    let result = builder.execute().await?;
    let elapsed = start.elapsed();

    config::write_active_ledger(fluree_dir, ledger)?;

    let secs = elapsed.as_secs_f64();
    println!(
        "Created ledger '{}' (imported {} flakes, t={}, {:.2}s)",
        ledger,
        format_with_commas(result.flake_count),
        result.t,
        secs
    );
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

/// Format a u64 with comma-separated thousands (e.g. 543_174_590 → "543,174,590").
fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}
