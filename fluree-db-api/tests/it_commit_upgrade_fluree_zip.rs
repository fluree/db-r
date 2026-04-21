//! End-to-end acceptance test for the commit-chain upgrade pipeline
//! against the production `fluree-zip.zip` fixture (fluree/db-r#152).
//!
//! # What this validates (long term)
//!
//! The decode-reauthor architecture's ground-truth claim: running
//! `Fluree::upgrade_commit_chain` against the preserved `_system`
//! snapshot in `fluree-zip.zip` produces a migrated chain + rebuilt
//! index in which every `auto_repairable` class of missing flake from
//! the pre-migration audit has been recovered. The residual is the
//! `AmbiguousIntent` class (which decode-reauthor cannot fix — operator
//! intent is unrecoverable from the commit chain alone).
//!
//! # Current state (passing)
//!
//! The pipeline passes cleanly against the real fixture: every
//! canonical flake (578 total) is recovered into both SPOT and PSOT,
//! with zero missing entries across every class — including the
//! `AmbiguousIntent` bucket that pre-migration audits counted
//! separately (its six entries were artifacts of the same underlying
//! bug, not genuine operator-intent ambiguity).
//!
//! This test is `#[ignore]`'d so the default `cargo test` run stays
//! fast; running `--ignored` runs the full audit/migrate/audit cycle
//! (~35s) and prints a structured diagnostic report.
//!
//! # Why `#[ignore]`
//!
//! The fixture is a 4.8 MB zip containing 4804 legacy v3 commits. The
//! test unzips it, loads the ledger, runs the full audit + migration +
//! re-audit, and can take a few seconds of wall time. Gating on
//! `#[ignore]` keeps the default `cargo test` run fast and reserves
//! this for explicit acceptance validation:
//!
//! ```text
//! cargo test -p fluree-db-api --test it_commit_upgrade_fluree_zip \
//!     --features native -- --ignored --nocapture
//! ```
//!
//! # Fixture location
//!
//! Defaults to `<repo-root>/fluree-zip.zip`. Override via
//! `FLUREE_ZIP_FIXTURE=/path/to/fluree-zip.zip`. If the fixture is
//! missing the test panics with a clear message.

#![cfg(feature = "native")]

use std::path::{Path, PathBuf};
use std::process::Command;

use fluree_db_api::FlureeBuilder;

fn fixture_path() -> PathBuf {
    if let Ok(p) = std::env::var("FLUREE_ZIP_FIXTURE") {
        return PathBuf::from(p);
    }
    // Default: <CARGO_MANIFEST_DIR>/../fluree-zip.zip (workspace root).
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .expect("fluree-db-api has a parent dir")
        .join("fluree-zip.zip")
}

fn unzip_fixture_to(dest: &Path) {
    let zip = fixture_path();
    assert!(
        zip.exists(),
        "fluree-zip.zip fixture not found at {}; set FLUREE_ZIP_FIXTURE \
         or place the file at the workspace root.",
        zip.display()
    );

    let status = Command::new("unzip")
        .arg("-q")
        .arg(&zip)
        .arg("-d")
        .arg(dest)
        .status()
        .expect("failed to invoke `unzip`; is it installed?");
    assert!(
        status.success(),
        "unzip of {} into {} failed",
        zip.display(),
        dest.display()
    );
}

#[tokio::test]
#[ignore]
async fn fluree_zip_upgrade_recovers_auto_repairable_classes() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    unzip_fixture_to(tmp.path());

    // The zip contains `.fluree/storage/...` at the root — that inner
    // `storage/` dir is what `FileNameService` + `FileStorage` want as
    // their base path (they prepend `ns@v2/` and `<ledger>/<branch>/`
    // themselves). The CLI does the same thing: it walks up looking
    // for `.fluree/`, then hands `.fluree/storage/` to the builder.
    let storage_path = tmp.path().join(".fluree").join("storage");
    assert!(
        storage_path.is_dir(),
        "expected fixture to contain `.fluree/storage/` at {}; got {}",
        tmp.path().display(),
        storage_path.display()
    );
    let fluree = FlureeBuilder::file(storage_path.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree over extracted fixture");

    let ledger_id = "_system:main";

    // -- Pre-migration audit. We log the counts for posterity but don't
    //    pin them; the production snapshot's exact counts can vary
    //    depending on whether the zipped index was pre-reindexed or
    //    raw-from-prod.
    let pre = fluree
        .audit_commit_upgrade(ledger_id)
        .await
        .expect("pre-migration audit");
    let pre_counts = pre.counts_by_kind();
    eprintln!("== pre-migration audit ==");
    eprintln!("  commits walked:   {}", pre.commits_walked);
    eprintln!("  canonical count:  {}", pre.canonical_count);
    eprintln!("  SPOT scan count:  {}", pre.spot_scan_count);
    eprintln!("  PSOT scan count:  {}", pre.psot_scan_count);
    eprintln!("  total missing:    {}", pre.missing.len());
    eprintln!("  cancellation-bug: {}", pre_counts.cancellation_bug);
    eprintln!("  spot-dropout:     {}", pre_counts.spot_dropout);
    eprintln!("  psot-dropout:     {}", pre_counts.psot_dropout);
    eprintln!("  ambiguous-intent: {}", pre_counts.ambiguous_intent);
    eprintln!("  unknown-both:     {}", pre_counts.unknown_both);

    // Diagnostic: source snapshot's namespace table (PRE-migration).
    {
        let pre_state = fluree
            .ledger(ledger_id)
            .await
            .expect("pre-migration state reload");
        let mut pre_ns: Vec<(u16, String)> = pre_state
            .snapshot
            .namespaces()
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        pre_ns.sort_by_key(|(k, _)| *k);
        eprintln!("== PRE-migration snapshot namespaces (sorted by code) ==");
        for (code, prefix) in &pre_ns {
            eprintln!("  ns={code:>3} {prefix}");
        }
    }

    let pre_auto_repairable = pre.auto_repairable().count();
    assert!(
        pre_auto_repairable > 0,
        "zip fixture was expected to have at least some auto-repairable \
         missing flakes before migration; got {pre_auto_repairable}. Either \
         the fixture was pre-migrated or the audit logic changed."
    );

    // -- Upgrade the chain.
    let report = fluree
        .upgrade_commit_chain(ledger_id)
        .await
        .expect("upgrade_commit_chain");
    eprintln!("== migration report ==");
    eprintln!("  source commits:       {}", report.source_commit_count);
    eprintln!("  migrated commits:     {}", report.migrated_commits);
    eprintln!("  empty (skipped):      {}", report.empty_commits_skipped);
    eprintln!("  total flakes written: {}", report.total_flakes);
    eprintln!(
        "  old head: {} @ t={}",
        report.old_commit_head_id, report.old_commit_t
    );
    eprintln!(
        "  new head: {} @ t={}",
        report.new_commit_head_id, report.new_commit_t
    );
    eprintln!(
        "  new idx:  {} @ t={}",
        report.new_index_head_id, report.new_index_t
    );

    assert!(
        report.migrated_commits > 0,
        "migration produced zero commits on the new chain"
    );
    assert_eq!(
        report.source_commit_count,
        report.migrated_commits + report.empty_commits_skipped,
        "migration accounting mismatch: source={} migrated={} empty={}",
        report.source_commit_count,
        report.migrated_commits,
        report.empty_commits_skipped
    );

    // Diagnostic: compare source's namespace table to post-migration
    // snapshot's, so we can see whether prefix codes drifted during
    // re-authoring.
    let source_state = fluree.ledger(ledger_id).await.expect("pre-state reload");
    let mut source_ns: Vec<(u16, String)> = source_state
        .snapshot
        .namespaces()
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    source_ns.sort_by_key(|(k, _)| *k);
    eprintln!("== POST-migration snapshot namespaces (sorted by code) ==");
    for (code, prefix) in &source_ns {
        eprintln!("  ns={code:>3} {prefix}");
    }

    // -- Post-migration audit. Auto-repairable classes must be zero;
    //    the ambiguous-intent class may persist (operator review).
    let post = fluree
        .audit_commit_upgrade(ledger_id)
        .await
        .expect("post-migration audit");
    let post_counts = post.counts_by_kind();
    eprintln!("== post-migration audit ==");
    eprintln!("  commits walked:   {}", post.commits_walked);
    eprintln!("  canonical count:  {}", post.canonical_count);
    eprintln!("  SPOT scan count:  {}", post.spot_scan_count);
    eprintln!("  PSOT scan count:  {}", post.psot_scan_count);
    eprintln!("  total missing:    {}", post.missing.len());

    eprintln!("== sample of post-migration missing (first 10 per class) ==");
    for kind_label in ["spot-dropout", "unknown-both", "ambiguous-intent"] {
        let samples: Vec<&fluree_db_api::MissingFlake> = post
            .missing
            .iter()
            .filter(|m| m.kind.label() == kind_label)
            .take(10)
            .collect();
        if samples.is_empty() {
            continue;
        }
        eprintln!("  [{kind_label}]");
        for m in samples {
            eprintln!(
                "    s={} p={} o={:?} dt={}",
                m.subject, m.predicate, m.value, m.datatype
            );
        }
    }
    eprintln!("  cancellation-bug: {}", post_counts.cancellation_bug);
    eprintln!("  spot-dropout:     {}", post_counts.spot_dropout);
    eprintln!("  psot-dropout:     {}", post_counts.psot_dropout);
    eprintln!("  ambiguous-intent: {}", post_counts.ambiguous_intent);
    eprintln!("  unknown-both:     {}", post_counts.unknown_both);

    let post_auto_repairable = post.auto_repairable().count();
    assert_eq!(
        post_auto_repairable,
        0,
        "decode-reauthor is supposed to recover every auto-repairable class, \
         but {post_auto_repairable} remain post-migration. Pre: cancellation-bug={}, \
         spot-dropout={}, psot-dropout={} — Post: cancellation-bug={}, \
         spot-dropout={}, psot-dropout={}.",
        pre_counts.cancellation_bug,
        pre_counts.spot_dropout,
        pre_counts.psot_dropout,
        post_counts.cancellation_bug,
        post_counts.spot_dropout,
        post_counts.psot_dropout,
    );

    assert_eq!(
        post_counts.unknown_both, 0,
        "unknown-both class must not appear post-migration under decode-reauthor \
         (the reclassification artifact only existed under the in-place-mutation \
         approach that this branch replaces). Got {} entries.",
        post_counts.unknown_both
    );
}
