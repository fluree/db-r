//! `fluree commit upgrade <LEDGER> [--dry-run|--force] [--remote <name>]`
//!
//! See fluree-db-api::commit_upgrade for the audit algorithm and fluree/db-r#152
//! for the bug being repaired.

use std::time::Duration;

use crate::context::{self, build_fluree};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{CommitUpgradeAudit, CommitUpgradeReport, MissingKind};

pub async fn run(
    ledger: &str,
    dry_run: bool,
    force: bool,
    remote_flag: Option<&str>,
    status_correlation_id: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // --dry-run is the default when neither flag is given.
    let effective_dry_run = dry_run || !force;

    if let Some(remote_name) = remote_flag {
        return run_remote(
            ledger,
            effective_dry_run,
            remote_name,
            status_correlation_id,
            dirs,
        )
        .await;
    }

    if status_correlation_id.is_some() {
        return Err(CliError::Usage("--status requires --remote".to_string()));
    }

    let alias = context::resolve_ledger(Some(ledger), dirs)?;
    let fluree = build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    if !fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Err(CliError::NotFound(format!("ledger '{}' not found", alias)));
    }

    eprintln!(
        "  {} auditing {}...",
        "commit upgrade:".cyan().bold(),
        alias
    );

    let audit = fluree
        .audit_commit_upgrade(&ledger_id)
        .await
        .map_err(CliError::Api)?;

    print_audit_summary(&audit);

    if audit.is_clean() {
        println!("\n{}", "Nothing to repair.".green().bold());
        return Ok(());
    }

    print_missing_detail(&audit);

    if effective_dry_run {
        println!(
            "\n{} this was a dry run. Pass {} to emit a repair commit.",
            "note:".yellow().bold(),
            "--force".cyan()
        );
        return Ok(());
    }

    // --force path — migrate the chain via decode-reauthor, then re-run
    // the audit so the operator sees the post-migration state.
    let auto_before = audit.auto_repairable().count();
    if auto_before == 0 {
        println!(
            "\n{} nothing auto-repairable; skipping migration. \
             Review the items above if any remain.",
            "note:".yellow().bold()
        );
        return Ok(());
    }

    println!(
        "\n  {} running commit-chain upgrade for {}...",
        "migrating:".cyan().bold(),
        alias
    );

    let report = fluree
        .upgrade_commit_chain(&ledger_id)
        .await
        .map_err(CliError::Api)?;

    print_upgrade_report(&report);

    println!(
        "\n  {} re-auditing migrated chain...",
        "verifying:".cyan().bold()
    );

    let post = fluree
        .audit_commit_upgrade(&ledger_id)
        .await
        .map_err(CliError::Api)?;

    print_audit_summary(&post);

    let auto_after = post.auto_repairable().count();
    if auto_after > 0 {
        println!(
            "\n{} {} auto-repairable entries remain after migration. \
             This is unexpected on a clean branch — see issue #152.",
            "warning:".red().bold(),
            auto_after
        );
    } else {
        println!(
            "\n{}",
            "Migration complete — chain is clean.".green().bold()
        );
    }

    if post.review_required().count() > 0 {
        println!(
            "{} {} entries still require operator review (see Review Required section above).",
            "note:".yellow().bold(),
            post.review_required().count()
        );
    }

    Ok(())
}

/// `--remote <name>` path.
///
/// Two modes:
/// - **Dispatch + poll** (default): POST to
///   `/v1/admin/commit-upgrade/{ledgerId}`, receive 202 ACCEPTED with a
///   correlation ID, then poll `/v1/fluree/status/{correlationId}` until
///   the row reports a terminal state.
/// - **Resume** (`--status <correlationId>`): skip dispatch and poll an
///   existing correlation ID. Useful when an earlier invocation was
///   Ctrl-C'd while polling — the migration continues on the server
///   regardless, so resuming picks up the result once it lands.
///
/// Output mirrors the local path's formatting (audit summary, migration
/// report, post-audit), driven from the JSON in the status row's
/// `result` field.
async fn run_remote(
    ledger: &str,
    dry_run: bool,
    remote_name: &str,
    status_correlation_id: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let client = context::build_remote_client(remote_name, dirs).await?;

    let correlation_id = match status_correlation_id {
        Some(id) => {
            eprintln!(
                "  {} resuming poll for {} on remote {}...",
                "commit upgrade:".cyan().bold(),
                id.dimmed(),
                remote_name.cyan()
            );
            id.to_string()
        }
        None => {
            eprintln!(
                "  {} dispatching {} of {} on remote {}...",
                "commit upgrade:".cyan().bold(),
                if dry_run {
                    "audit"
                } else {
                    "audit + migration"
                },
                ledger,
                remote_name.cyan()
            );
            let dispatch = client
                .commit_upgrade_dispatch(ledger, dry_run)
                .await
                .map_err(|e| {
                    CliError::Usage(format!("remote commit-upgrade dispatch failed: {e}"))
                })?;
            let id = dispatch
                .get("correlationId")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CliError::Usage(
                        "dispatch response missing `correlationId` — is the remote on an \
                         old build?"
                            .to_string(),
                    )
                })?
                .to_string();
            eprintln!(
                "  {} dispatched (correlation: {})",
                "accepted:".green().bold(),
                id.dimmed()
            );
            eprintln!(
                "  {} Ctrl-C cancels polling only; the migration continues on the server.",
                "note:".yellow().bold()
            );
            eprintln!(
                "         Resume polling with: fluree commit upgrade --remote {} {} --status {}",
                remote_name, ledger, id
            );
            id
        }
    };

    eprint!("  {} polling", "waiting:".cyan().bold());
    let _ = std::io::Write::flush(&mut std::io::stderr());
    let row = client
        .poll_until_complete(
            &correlation_id,
            Duration::from_secs(2),
            Duration::from_secs(15 * 60),
            || {
                eprint!(".");
                let _ = std::io::Write::flush(&mut std::io::stderr());
            },
        )
        .await
        .map_err(|e| CliError::Usage(format!("remote commit-upgrade polling failed: {e}")))?;
    eprintln!();

    // Row shape: { correlationId, status, result, error, ... }
    // `result` is the JSON document IndexingLambda wrote — the same
    // shape the former sync endpoint returned (preAudit, migrated,
    // report, postAudit). `result` may be a JSON string or an object
    // depending on how the server serialized it; handle both.
    let raw_result = row
        .get("result")
        .cloned()
        .ok_or_else(|| CliError::Usage("status row missing `result` field".to_string()))?;
    let response = match raw_result {
        serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(&s)
            .map_err(|e| CliError::Usage(format!("status `result` not valid JSON: {e}")))?,
        other => other,
    };

    render_remote_result(&response, remote_name);
    Ok(())
}

/// Render the `result` JSON from a terminal status row. Extracted so
/// dispatch-then-poll and resume-poll paths share the same UX.
fn render_remote_result(response: &serde_json::Value, remote_name: &str) {
    let pre_audit = match response.get("preAudit") {
        Some(a) => a,
        None => {
            eprintln!(
                "{} status row's `result` missing `preAudit` — raw result: {}",
                "warning:".yellow().bold(),
                response
            );
            return;
        }
    };
    print_remote_audit_summary("pre-migration", pre_audit);

    let migrated = response
        .get("migrated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if !migrated {
        if let Some(note) = response.get("note").and_then(|v| v.as_str()) {
            println!("\n{} {}", "note:".yellow().bold(), note);
        }
        return;
    }

    if let Some(report) = response.get("report") {
        print_remote_upgrade_report(report);
    }

    if let Some(post_audit) = response.get("postAudit") {
        println!(
            "\n  {} post-migration state ({}):",
            "verifying:".cyan().bold(),
            remote_name.cyan()
        );
        print_remote_audit_summary("post-migration", post_audit);

        let post_auto = post_audit
            .get("autoRepairableTotal")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if post_auto > 0 {
            println!(
                "\n{} {} auto-repairable entries remain after migration (remote).",
                "warning:".red().bold(),
                post_auto
            );
        } else {
            println!(
                "\n{}",
                "Migration complete — remote chain is clean.".green().bold()
            );
        }
    }
}

fn print_remote_audit_summary(label: &str, audit: &serde_json::Value) {
    let ledger_id = audit
        .get("ledgerId")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    let commits_walked = audit
        .get("commitsWalked")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let canonical = audit
        .get("canonicalCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let spot = audit
        .get("spotScanCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let psot = audit
        .get("psotScanCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total = audit
        .get("totalMissing")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let counts = audit
        .get("countsByKind")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let n = |key: &str| -> u64 { counts.get(key).and_then(|v| v.as_u64()).unwrap_or(0) };

    println!();
    println!("Ledger ({label}):          {ledger_id}");
    println!("Commits walked:           {commits_walked}");
    println!("Canonical (should exist): {canonical}");
    println!("SPOT scan flakes:         {spot}");
    println!("PSOT scan flakes:         {psot}");
    println!();
    println!("Missing total:            {total}");
    println!(
        "  {}: {}",
        "auto-repairable via commit migration".green().bold(),
        audit
            .get("autoRepairableTotal")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!("    {:<20} {}", "cancellation-bug", n("cancellationBug"));
    println!("    {:<20} {}", "spot-dropout", n("spotDropout"));
    println!("    {:<20} {}", "psot-dropout", n("psotDropout"));
    println!(
        "  {}: {}",
        "operator review required".yellow().bold(),
        audit
            .get("reviewRequiredTotal")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!("    {:<20} {}", "ambiguous-intent", n("ambiguousIntent"));
    println!("    {:<20} {}", "unknown-both", n("unknownBoth"));
}

fn print_remote_upgrade_report(report: &serde_json::Value) {
    let get_str = |k: &str| -> &str {
        report
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("<unknown>")
    };
    let get_u64 = |k: &str| -> u64 { report.get(k).and_then(|v| v.as_u64()).unwrap_or(0) };
    let get_i64 = |k: &str| -> i64 { report.get(k).and_then(|v| v.as_i64()).unwrap_or(0) };

    println!();
    println!("Ledger:              {}", get_str("ledgerId"));
    println!(
        "Source commits:      {} walked, {} migrated ({} skipped as empty)",
        get_u64("sourceCommitCount"),
        get_u64("migratedCommits"),
        get_u64("emptyCommitsSkipped")
    );
    println!("Flakes written:      {}", get_u64("totalFlakes"));
    println!(
        "Old commit head:     {} {}",
        get_str("oldCommitHeadId"),
        format!("@ t={}", get_i64("oldCommitT")).dimmed()
    );
    println!(
        "New commit head:     {} {}",
        get_str("newCommitHeadId").green(),
        format!("@ t={}", get_i64("newCommitT")).dimmed()
    );
    println!(
        "New index head:      {} {}",
        get_str("newIndexHeadId").green(),
        format!("@ t={}", get_i64("newIndexT")).dimmed()
    );
}

fn print_upgrade_report(report: &CommitUpgradeReport) {
    println!();
    println!("Ledger:              {}", report.ledger_id);
    println!(
        "Source commits:      {} walked, {} migrated ({} skipped as empty)",
        report.source_commit_count, report.migrated_commits, report.empty_commits_skipped
    );
    println!("Flakes written:      {}", report.total_flakes);
    println!(
        "Old commit head:     {} {}",
        report.old_commit_head_id,
        format!("@ t={}", report.old_commit_t).dimmed()
    );
    println!(
        "New commit head:     {} {}",
        report.new_commit_head_id.to_string().green(),
        format!("@ t={}", report.new_commit_t).dimmed()
    );
    println!(
        "New index head:      {} {}",
        report.new_index_head_id.to_string().green(),
        format!("@ t={}", report.new_index_t).dimmed()
    );
}

fn print_audit_summary(audit: &CommitUpgradeAudit) {
    let counts = audit.counts_by_kind();
    println!();
    println!("Ledger:                   {}", audit.ledger_id);
    println!("Commits walked:           {}", audit.commits_walked);
    println!("Canonical (should exist): {}", audit.canonical_count);
    println!("SPOT scan flakes:         {}", audit.spot_scan_count);
    println!("PSOT scan flakes:         {}", audit.psot_scan_count);
    println!();
    println!("Missing total:            {}", audit.missing_len());
    println!(
        "  {}: {}",
        "auto-repairable via commit migration".green().bold(),
        counts.auto_repairable_total()
    );
    println!(
        "    {:<20} {}  {}",
        "cancellation-bug",
        counts.cancellation_bug,
        "— #152 same-commit retract+assert pair".dimmed()
    );
    println!(
        "    {:<20} {}  {}",
        "spot-dropout",
        counts.spot_dropout,
        "— pre-148 pure insert, missing from SPOT".dimmed()
    );
    println!(
        "    {:<20} {}  {}",
        "psot-dropout",
        counts.psot_dropout,
        "— symmetric, missing from PSOT".dimmed()
    );
    println!(
        "  {}: {}",
        "operator review required".yellow().bold(),
        counts.review_required_total()
    );
    println!(
        "    {:<20} {}  {}",
        "ambiguous-intent",
        counts.ambiguous_intent,
        "— multi-value at head, intent unknown".dimmed()
    );
    println!(
        "    {:<20} {}  {}",
        "unknown-both",
        counts.unknown_both,
        "— missing both, no known fingerprint".dimmed()
    );
}

fn print_missing_detail(audit: &CommitUpgradeAudit) {
    println!();
    let auto_count = audit.auto_repairable().count();
    let review_count = audit.review_required().count();

    if auto_count > 0 {
        println!("{} ({})", "Auto-repairable:".green().bold(), auto_count);
        for (i, m) in audit.auto_repairable().enumerate() {
            print_flake_row(i + 1, m);
        }
    }

    if review_count > 0 {
        println!();
        println!(
            "{} ({})",
            "Review required (not auto-repaired):".yellow().bold(),
            review_count
        );
        for (i, m) in audit.review_required().enumerate() {
            print_flake_row(i + 1, m);
        }
    }
}

fn print_flake_row(n: usize, m: &fluree_db_api::MissingFlake) {
    let label = m.kind.label();
    let colored_label = if m.kind.auto_repairable() {
        label.green().bold()
    } else {
        label.yellow().bold()
    };
    let detail = match &m.kind {
        MissingKind::CancellationBug { same_commit_t } => {
            format!(" t={:?}", same_commit_t).dimmed().to_string()
        }
        MissingKind::AmbiguousIntent {
            canonical_multiplicity,
        } => format!(" (×{} coexisting values)", canonical_multiplicity)
            .dimmed()
            .to_string(),
        _ => String::new(),
    };
    println!(
        "  {:>3}. [{}] {} {} {} ({}){}",
        n,
        colored_label,
        m.subject.bright_white(),
        m.predicate.bright_blue(),
        format_value_abbrev(&m.value),
        m.datatype.dimmed(),
        detail
    );
}

fn format_value_abbrev(v: &fluree_db_core::FlakeValue) -> String {
    use fluree_db_core::FlakeValue;
    const MAX_LEN: usize = 80;
    let raw = match v {
        FlakeValue::String(s) => format!("\"{}\"", s),
        FlakeValue::Json(s) => format!("@json:{}", s),
        FlakeValue::Long(n) => n.to_string(),
        FlakeValue::Double(d) => d.to_string(),
        FlakeValue::Boolean(b) => b.to_string(),
        FlakeValue::Ref(sid) => format!("<{}>", sid),
        other => format!("{:?}", other),
    };
    if raw.chars().count() > MAX_LEN {
        let truncated: String = raw.chars().take(MAX_LEN).collect();
        format!("{}…", truncated)
    } else {
        raw
    }
}
