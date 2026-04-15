//! `fluree commit upgrade <LEDGER> [--dry-run|--force] [--remote <name>]`
//!
//! See fluree-db-api::commit_upgrade for the audit algorithm and fluree/db-r#152
//! for the bug being repaired.

use crate::context::{self, build_fluree};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{CommitUpgradeAudit, MissingKind};

pub async fn run(
    ledger: &str,
    dry_run: bool,
    force: bool,
    remote_flag: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    if remote_flag.is_some() {
        return Err(CliError::Usage(
            "--remote is not yet implemented for `fluree commit upgrade`; \
             run locally against the ledger's .fluree/ directory for now"
                .to_string(),
        ));
    }

    // --dry-run is the default when neither flag is given.
    let effective_dry_run = dry_run || !force;

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

    // --force path — phase 2; not yet implemented.
    Err(CliError::Usage(
        "--force (repair commit emission) is not yet implemented; \
         use --dry-run to audit for now"
            .to_string(),
    ))
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
