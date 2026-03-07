use crate::config;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Import tuning options passed from global + create-specific CLI flags.
pub struct ImportOpts {
    pub memory_budget_mb: usize,
    pub parallelism: usize,
    pub chunk_size_mb: usize,
    pub leaflet_rows: usize,
    pub leaflets_per_leaf: usize,
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
        Some(path) if path.is_dir() => {
            // Validate directory format (catches mixed formats & empty dirs).
            fluree_db_api::scan_directory_format(path)?;
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
        Some(path) if is_import_path(path)? => {
            // Bulk import: Turtle or JSON-LD file (any size).
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

    let ledger_owned = ledger.to_string();

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
    if import_opts.leaflet_rows != 25_000 {
        builder = builder.leaflet_rows(import_opts.leaflet_rows);
    }
    if import_opts.leaflets_per_leaf != 10 {
        builder = builder.leaflets_per_leaf(import_opts.leaflets_per_leaf);
    }
    let settings = builder.effective_import_settings();
    let mem_auto = import_opts.memory_budget_mb == 0;
    let par_auto = import_opts.parallelism == 0;
    if !quiet {
        eprintln!(
            "Import settings: memory budget {} MB{}, parallelism {}{}, chunk size {} MB",
            settings.memory_budget_mb,
            if mem_auto { " (auto)" } else { "" },
            settings.parallelism,
            if par_auto { " (auto)" } else { "" },
            settings.chunk_size_mb,
        );
        if mem_auto || par_auto {
            eprintln!("  Override with --memory-budget-mb and --parallelism");
        }
    }

    // ------------------------------------------------------------------------
    // Crash breadcrumb for customer support (survives SIGSEGV/OOM-kill).
    //
    // The CLI defaults to "no logs" for UX; when the process dies hard (e.g.
    // SIGSEGV under memory pressure), there is no Rust panic message.
    //
    // We write a small JSON "breadcrumb" file periodically during import so
    // users can attach it to bug reports. This is intentionally minimal and
    // low-frequency (<= 1 write/sec).
    // ------------------------------------------------------------------------
    let breadcrumb_path: Option<std::path::PathBuf> = {
        let crash_dir = fluree_dir.join("crash");
        if std::fs::create_dir_all(&crash_dir).is_ok() {
            let pid = std::process::id();
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let name = format!(
                "import_{}_{}_{}.json",
                sanitize_for_filename(ledger),
                ts,
                pid
            );
            let p = crash_dir.join(name);
            // Initial record (best-effort).
            let started_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let init = serde_json::json!({
                "kind": "bulk_import",
                "ledger": ledger_owned,
                "pid": pid,
                "started_epoch_ms": started_ms,
                "source_path": path.display().to_string(),
                "settings": {
                    "memory_budget_mb": settings.memory_budget_mb,
                    "parallelism": settings.parallelism,
                    "chunk_size_mb": settings.chunk_size_mb,
                    "max_inflight_chunks": settings.max_inflight_chunks,
                    "leaflet_rows": import_opts.leaflet_rows,
                    "leaflets_per_leaf": import_opts.leaflets_per_leaf
                },
                "status": "running",
                "last_phase": "starting"
            });
            let _ = std::fs::write(&p, serde_json::to_vec_pretty(&init).unwrap_or_default());
            Some(p)
        } else {
            None
        }
    };
    let breadcrumb_last_write: std::sync::Arc<std::sync::Mutex<std::time::Instant>> =
        std::sync::Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

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
    // Track when the commit phase actually starts (first Committing event),
    // so M flakes/s reflects commit throughput, not reading/parsing time.
    let commit_start: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let breadcrumb_path_for_cb = breadcrumb_path.clone();
    let breadcrumb_last_write_for_cb = std::sync::Arc::clone(&breadcrumb_last_write);
    let breadcrumb_ledger_for_cb = ledger_owned.clone();
    builder = builder.on_progress(move |phase| {
        // Best-effort: update crash breadcrumb at most once per second.
        // Avoid heavy work in the callback when the progress bars are active.
        if let Some(ref p) = breadcrumb_path_for_cb {
            if let Ok(mut last) = breadcrumb_last_write_for_cb.lock() {
                if last.elapsed() >= Duration::from_secs(1) {
                    *last = std::time::Instant::now();
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let phase_str = format!("{phase:?}");
                    let doc = serde_json::json!({
                        "kind": "bulk_import",
                        "ledger": breadcrumb_ledger_for_cb,
                        "pid": std::process::id(),
                        "updated_epoch_ms": now_ms,
                        "status": "running",
                        "last_phase": phase_str
                    });
                    let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
                }
            }
        }
        // Continue with normal progress handling below.
        match phase {
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
                ..
            } => {
                let t0 = *commit_start.get_or_init(std::time::Instant::now);
                cb.set_length(total as u64);
                cb.set_position(chunk as u64);
                let secs = t0.elapsed().as_secs_f64();
                let rate = if secs > 0.0 {
                    cumulative_flakes as f64 / secs / 1_000_000.0
                } else {
                    0.0
                };
                cb.set_message(format!("{:.2} M flakes/s", rate));
            }
            ImportPhase::PreparingIndex { stage } => {
                cb.finish();
                // Show activity immediately (avoid "Indexing 0%" during merge/remap).
                ib.set_length(100);
                ib.set_position(1);
                ib.set_message(stage.to_string());
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
                // Rate in real flakes/s (total_flakes is 2× because SPOT +
                // secondary pipelines both contribute to progress).
                let rate = if elapsed_secs > 0.0 {
                    merged_flakes as f64 / 2.0 / 1_000_000.0 / elapsed_secs
                } else {
                    0.0
                };
                ib.set_message(format!("{:.2} M flakes/s", rate));
            }
            ImportPhase::Done => {
                ib.finish();
                // Mark breadcrumb as complete (best-effort).
                if let Some(ref p) = breadcrumb_path_for_cb {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let doc = serde_json::json!({
                        "kind": "bulk_import",
                        "ledger": breadcrumb_ledger_for_cb,
                        "pid": std::process::id(),
                        "updated_epoch_ms": now_ms,
                        "status": "done"
                    });
                    let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
                }
            }
        }
    });

    let start = std::time::Instant::now();
    let result = match builder.execute().await {
        Ok(r) => r,
        Err(e) => {
            // Persist failure marker for customer bug reports (best-effort).
            if let Some(ref p) = breadcrumb_path {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let doc = serde_json::json!({
                    "kind": "bulk_import",
                    "ledger": ledger_owned,
                    "pid": std::process::id(),
                    "updated_epoch_ms": now_ms,
                    "status": "error",
                    "error": e.to_string()
                });
                let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
            }
            return Err(e.into());
        }
    };
    let elapsed = start.elapsed();

    config::write_active_ledger(fluree_dir, ledger)?;

    let secs = elapsed.as_secs_f64();
    let total_m = result.flake_count as f64 / 1_000_000.0;
    let mflakes_per_sec = total_m / secs;
    println!(
        "\n\nAbout ledger '{}':\nImported {:.1}M flakes in {:.2}s ({:.2} M flakes/s) across {} commits (t={})",
        ledger, total_m, secs, mflakes_per_sec, result.t, result.t
    );

    if let Some(ref summary) = result.summary {
        if !summary.top_classes.is_empty() {
            println!("\n  Top classes:");
            for (iri, count) in &summary.top_classes {
                println!("    {:>12}  {}", format_with_commas(*count), iri);
            }
        }
        if !summary.top_properties.is_empty() {
            println!("\n  Top properties:");
            for (iri, count) in &summary.top_properties {
                println!("    {:>12}  {}", format_with_commas(*count), iri);
            }
        }
        if !summary.top_connections.is_empty() {
            println!("\n  Top connections:");
            for (src, prop, tgt, count) in &summary.top_connections {
                println!(
                    "    {:>12}  {} -> {} -> {}",
                    format_with_commas(*count),
                    src,
                    prop,
                    tgt
                );
            }
        }
        println!();
    }

    // Success: remove the crash breadcrumb so the presence of files in
    // `<data_dir>/crash/` continues to be a strong signal of *failed/crashed*
    // runs that need investigation.
    if let Some(p) = breadcrumb_path {
        let _ = std::fs::remove_file(p);
    }

    Ok(())
}

// ============================================================================
// Import path detection (single files only)
// ============================================================================

/// Whether this single-file path should use the import pipeline.
///
/// - `.ttl` files (case-insensitive) → import (auto-splits large files)
/// - `.jsonld` files (case-insensitive) → import (bypasses novelty)
/// - `.ttl.gz` → error with helpful message
/// - Everything else (e.g. `.json`) → detect-based transact path
///
/// Note: directories are handled separately in `run()` via `fluree_db_api::scan_directory_format()`.
fn is_import_path(path: &Path) -> CliResult<bool> {
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

    // Case-insensitive .ttl / .jsonld check.
    if name_lower.ends_with(".ttl") || name_lower.ends_with(".jsonld") {
        return Ok(true);
    }

    Ok(false)
}

/// Replace non-alphanumeric characters with underscores for safe filenames.
fn sanitize_for_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
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
