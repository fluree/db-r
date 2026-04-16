//! Commit-chain upgrade migration pipeline (fluree/db-r#152).
//!
//! Ties `decode::decode_commit` and `reauthor::build_canonical_flakes`
//! together into an end-to-end migration that walks a legacy commit
//! chain ascending, re-authors each source commit as canonical flakes
//! through the current post-#148 write path, rebuilds the binary index
//! over the migrated chain, and atomically swaps the real nameservice's
//! `commit_head_id` + `index_head_id` to point at the new chain.
//!
//! # Isolation via a stub nameservice
//!
//! The migration loop publishes each per-commit advance to a transient
//! `MemoryNameService`, not to the real nameservice. Every new commit
//! blob still lands in the production `ContentStore` (so the source
//! chain and the migrated chain coexist as parallel DAGs under the same
//! storage namespace until the final swap), but the real nameservice's
//! `commit_head_id` remains pointed at the source chain throughout the
//! migration. Only the final `AdminPublisher::reset_head` step moves
//! the real nameservice — atomically, and only once we've verified a
//! clean index build.
//!
//! If the migration fails mid-loop, the only leftover state is orphaned
//! commit/index blobs in the production content store. The nameservice
//! never saw the partial chain, so operators can simply re-run the
//! migration from a clean source state (garbage-collection of orphaned
//! blobs is a separate follow-on, tracked under #152).
//!
//! # Namespace pre-seeding is load-bearing
//!
//! Before the loop starts, we call `NamespaceRegistry::ensure_code` for
//! every `(code, prefix)` in the source snapshot's namespace table. This
//! populates the registry's delta (emitted into the very first migrated
//! commit's `namespace_delta`) with every prefix the source chain ever
//! registered. Without this step, `canonical_split` at t=1 would only
//! see the built-in prefixes and would split long IRIs against shorter
//! registered prefixes — exactly the unsplit/split asymmetry that
//! caused the bug on the source chain in the first place.
//!
//! # Rebuilding the registry per iteration
//!
//! `fluree_db_transact::commit::commit` consumes `NamespaceRegistry` by
//! value. The returned `LedgerState` has the delta folded into its
//! snapshot's `namespace_codes`, so we reconstruct the registry from
//! that snapshot for the next iteration via `NamespaceRegistry::from_db`.
//! The re-built registry has the same prefix set as before (plus any
//! new codes the commit introduced), so `sid_for_iri` remains
//! deterministic across iterations.
//!
//! # Branched ledgers
//!
//! Rejected up front. A branched ledger (`source_branch.is_some()` on
//! its `NsRecord`) shares a commit chain with its parent via
//! `BranchedContentStore`, and migrating a child without also migrating
//! the parent would produce a chain that can't be resolved. Follow-on
//! issue per #152.

use std::sync::Arc;

use fluree_db_core::{collect_dag_cids, load_commit_by_id, ContentId, LedgerSnapshot};
use fluree_db_indexer::{rebuild_index_from_commits, IndexerConfig};
use fluree_db_ledger::{IndexConfig, LedgerState};
use fluree_db_nameservice::{
    memory::MemoryNameService, AdminPublisher, NameService, NsRecordSnapshot, Publisher,
};
use fluree_db_novelty::Novelty;
use fluree_db_transact::{
    apply_cancellation, commit as transact_commit, stage_flakes, CommitOpts, NamespaceRegistry,
    StageOptions,
};
use tracing::{debug, info};

use crate::{ApiError, Fluree, Result};

use super::decode::decode_commit;
use super::reauthor::build_canonical_flakes;

/// Summary of a completed commit-chain upgrade.
#[derive(Debug, Clone)]
pub struct CommitUpgradeReport {
    /// Normalized ledger id (e.g. `_system:main`).
    pub ledger_id: String,
    /// Number of commits walked in the source DAG.
    pub source_commit_count: usize,
    /// Number of migrated commits actually written to the new chain.
    /// Can be less than `source_commit_count` if any source commit
    /// collapsed to empty under `apply_cancellation` (fully no-op
    /// retract+assert pairs).
    pub migrated_commits: usize,
    /// Source commits that collapsed to empty under `apply_cancellation`
    /// and were skipped (not written to the new chain).
    pub empty_commits_skipped: usize,
    /// Total flakes written across the migrated chain.
    pub total_flakes: usize,
    /// Source chain's commit head before migration.
    pub old_commit_head_id: ContentId,
    pub old_commit_t: i64,
    /// New (migrated) commit head after the atomic `reset_head` swap.
    pub new_commit_head_id: ContentId,
    pub new_commit_t: i64,
    /// New index root after rebuild over the migrated chain.
    pub new_index_head_id: ContentId,
    pub new_index_t: i64,
}

/// Upgrade a legacy commit chain to canonical post-#148 encoding.
///
/// See module docs for the isolation, seeding, and rejection rules.
pub(crate) async fn upgrade_commit_chain_impl<N>(
    fluree: &Fluree<N>,
    ledger_id: &str,
) -> Result<CommitUpgradeReport>
where
    N: NameService + Publisher + AdminPublisher + Send + Sync + 'static,
{
    info!(ledger_id = %ledger_id, "Starting commit-chain upgrade");

    // -- 1. Load the source ledger state. Its snapshot carries every
    //    namespace prefix the source chain ever registered (accumulated
    //    from commit deltas during load), which is exactly the basis
    //    decode needs.
    let source_state = fluree.ledger(ledger_id).await?;
    let source_snapshot = source_state.snapshot.clone();
    let normalized_id = source_snapshot.ledger_id.clone();

    if source_state
        .ns_record
        .as_ref()
        .and_then(|r| r.source_branch.as_ref())
        .is_some()
    {
        return Err(ApiError::internal(format!(
            "commit-upgrade: branched ledger {normalized_id} is unsupported; \
             migrate the parent ledger first (follow-on #152)"
        )));
    }

    let source_record = source_state
        .ns_record
        .as_ref()
        .ok_or_else(|| ApiError::internal(format!("ledger {normalized_id} has no ns record")))?;
    let old_commit_head_id = source_record.commit_head_id.clone().ok_or_else(|| {
        ApiError::internal(format!(
            "ledger {normalized_id} has no commit head to migrate"
        ))
    })?;
    let old_commit_t = source_record.commit_t;

    // -- 2. Walk the source DAG. `collect_dag_cids` returns descending
    //    t; reverse for forward replay.
    let content_store = fluree.content_store(&normalized_id);
    let mut cids = collect_dag_cids(content_store.as_ref(), &old_commit_head_id, -1).await?;
    cids.reverse();
    let source_commit_count = cids.len();
    debug!(source_commit_count, "Source DAG walked");

    // -- 3. Set up the stub nameservice + a genesis migration state.
    //    Commits still land in the production content store; the stub
    //    just tracks the new chain's head as we build it.
    let stub_ns = MemoryNameService::new();
    stub_ns
        .publish_ledger_init(&normalized_id)
        .await
        .map_err(|e| ApiError::internal(format!("stub publish_ledger_init failed: {e}")))?;

    let mut migration_state =
        LedgerState::new(LedgerSnapshot::genesis(&normalized_id), Novelty::new(0));

    // -- 4. Seed the migration registry with every source prefix. These
    //    become the first migrated commit's `namespace_delta` (via
    //    `take_delta()` on the first `commit()` call), so the entire
    //    prefix table is present in the migrated chain from t=1.
    let mut ns_registry = NamespaceRegistry::new();
    for (&code, prefix) in source_snapshot.namespaces().iter() {
        ns_registry
            .ensure_code(code, prefix)
            .map_err(|e| ApiError::internal(format!("ns seed failed for {code}={prefix}: {e}")))?;
    }

    // -- 5. Migration loop. Each iteration: decode → reauthor →
    //    apply_cancellation → stage_flakes → commit, producing one new
    //    commit on the stub chain. Empty commits (fully-canceled pairs)
    //    are skipped per the audit's cancellation-bug semantics.
    let index_config = IndexConfig::default();
    let mut migrated_commits: usize = 0;
    let mut empty_commits_skipped: usize = 0;
    let mut total_flakes: usize = 0;

    for (_t, cid) in cids.iter() {
        let source_commit = load_commit_by_id(content_store.as_ref(), cid).await?;

        let decoded = decode_commit(&source_snapshot, &source_commit)?;
        let canonical_flakes = build_canonical_flakes(decoded, &mut ns_registry);
        let flakes = apply_cancellation(canonical_flakes);

        if flakes.is_empty() {
            empty_commits_skipped += 1;
            debug!(cid = %cid, "Source commit collapsed to empty — skipping");
            continue;
        }

        total_flakes += flakes.len();

        let view = stage_flakes(migration_state, flakes, StageOptions::new())
            .await
            .map_err(|e| ApiError::internal(format!("stage_flakes failed: {e}")))?;

        let mut opts = CommitOpts::default()
            .with_skip_sequencing()
            .with_skip_backpressure();
        if let Some(ts) = &source_commit.time {
            opts = opts.with_timestamp(ts.clone());
        }

        // NB: passing `&content_store` (i.e. `&Arc<dyn ContentStore>`)
        // rather than `content_store.as_ref()` because `transact::commit`'s
        // `C: ContentStore` generic has an implicit `Sized` bound. `Arc<dyn
        // ContentStore>` is Sized AND implements `ContentStore` (blanket
        // impl in fluree-db-core/src/storage.rs), so the `&Arc<...>` form
        // threads through without needing to relax the bound upstream.
        let (_receipt, new_state) = transact_commit(
            view,
            ns_registry,
            &content_store,
            &stub_ns,
            &index_config,
            opts,
        )
        .await
        .map_err(|e| ApiError::internal(format!("transact commit failed: {e}")))?;

        migration_state = new_state;
        // `commit()` consumed `ns_registry`. Rebuild from the just-advanced
        // snapshot. Since the registry's delta was taken by commit() and
        // written into the commit envelope, the new registry starts with
        // the full accumulated namespace table and an empty delta —
        // correct for the next iteration.
        ns_registry = NamespaceRegistry::from_db(&migration_state.snapshot);
        migrated_commits += 1;
    }

    info!(
        migrated_commits,
        empty_commits_skipped, total_flakes, "Source chain fully re-authored"
    );

    // -- 6. Fetch the stub's final head + rebuild the binary index from
    //    the new chain.
    let stub_record = stub_ns
        .lookup(&normalized_id)
        .await
        .map_err(|e| ApiError::internal(format!("stub lookup failed: {e}")))?
        .ok_or_else(|| ApiError::internal("stub lost its record mid-migration".to_string()))?;
    let new_commit_head_id = stub_record.commit_head_id.clone().ok_or_else(|| {
        ApiError::internal("migration produced no commits on the stub chain".to_string())
    })?;
    let new_commit_t = stub_record.commit_t;

    let indexer_config = IndexerConfig::default();
    let index_result = rebuild_index_from_commits(
        Arc::clone(&content_store),
        &normalized_id,
        &stub_record,
        indexer_config,
    )
    .await
    .map_err(|e| ApiError::internal(format!("rebuild_index_from_commits failed: {e}")))?;

    let new_index_head_id = index_result.root_id;
    let new_index_t = index_result.index_t;

    // -- 7. Atomic swap on the real nameservice. From this point the
    //    migrated chain is the operational head; the source chain is
    //    now orphaned (but still on disk for safety — GC is follow-on).
    fluree
        .nameservice()
        .reset_head(
            &normalized_id,
            NsRecordSnapshot {
                commit_head_id: Some(new_commit_head_id.clone()),
                commit_t: new_commit_t,
                index_head_id: Some(new_index_head_id.clone()),
                index_t: new_index_t,
            },
        )
        .await
        .map_err(|e| ApiError::internal(format!("real nameservice reset_head failed: {e}")))?;

    info!(
        ledger_id = %normalized_id,
        %new_commit_head_id,
        new_commit_t,
        %new_index_head_id,
        new_index_t,
        "Commit-chain upgrade complete"
    );

    Ok(CommitUpgradeReport {
        ledger_id: normalized_id,
        source_commit_count,
        migrated_commits,
        empty_commits_skipped,
        total_flakes,
        old_commit_head_id,
        old_commit_t,
        new_commit_head_id,
        new_commit_t,
        new_index_head_id,
        new_index_t,
    })
}
