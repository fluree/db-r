//! Branch rebase support
//!
//! Provides types and orchestration for replaying a branch's commits on top
//! of its source branch's current HEAD, bringing the branch up to date with
//! upstream changes.

use crate::error::{ApiError, Result};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{
    range_with_overlay, ConflictKey, ContentAddressedWrite, ContentId, Flake, IndexType,
    RangeMatch, RangeOptions, RangeTest, Storage,
};
use fluree_db_ledger::{LedgerState, LedgerView};
use fluree_db_nameservice::{BranchPoint, NameService, Publisher};
use fluree_db_novelty::{compute_delta_keys, trace_commits_by_id, Commit};
use fluree_db_transact::{CommitOpts, NamespaceRegistry};
use futures::TryStreamExt;
use rustc_hash::FxHashSet;
use serde::Serialize;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Strategy for resolving conflicts when branch and source modifications
/// overlap on the same (subject, predicate, graph) tuple.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Replay as-is, both values coexist (multi-cardinality). Default.
    #[default]
    TakeBoth,
    /// Fail on first conflict, no changes applied.
    Abort,
    /// Drop branch's conflicting flakes from the replayed commit (source wins).
    TakeSource,
    /// Keep branch's flakes and add retractions for source's conflicting values (branch wins).
    TakeBranch,
    /// Skip the entire commit if any flakes conflict.
    Skip,
}

impl ConflictStrategy {
    /// Parse a strategy name from a string (case-insensitive).
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "take-both" | "takeboth" | "take_both" => Some(Self::TakeBoth),
            "abort" => Some(Self::Abort),
            "take-source" | "takesource" | "take_source" | "theirs" => Some(Self::TakeSource),
            "take-branch" | "takebranch" | "take_branch" | "ours" => Some(Self::TakeBranch),
            "skip" => Some(Self::Skip),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TakeBoth => "take-both",
            Self::Abort => "abort",
            Self::TakeSource => "take-source",
            Self::TakeBranch => "take-branch",
            Self::Skip => "skip",
        }
    }
}

/// Record of a conflict detected during rebase of a single commit.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseConflict {
    pub original_t: i64,
    #[serde(skip)]
    pub keys: Vec<ConflictKey>,
    pub conflict_count: usize,
    pub resolution: &'static str,
}

/// Record of a commit that failed validation after replay.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseFailure {
    pub original_t: i64,
    pub error: String,
}

/// Summary report of a completed rebase operation.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseReport {
    pub replayed: usize,
    pub conflicts: Vec<RebaseConflict>,
    pub failures: Vec<RebaseFailure>,
    pub new_branch_point_t: i64,
    pub new_branch_point_id: ContentId,
    pub fast_forward: bool,
    pub total_commits: usize,
    pub skipped: usize,
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher + 'static,
{
    /// Rebase a branch onto its source branch's current HEAD.
    ///
    /// Replays the branch's unique commits on top of the source's current
    /// state, detecting and resolving conflicts according to `strategy`.
    pub async fn rebase_branch(
        &self,
        ledger_name: &str,
        branch: &str,
        strategy: ConflictStrategy,
    ) -> Result<RebaseReport> {
        let span = tracing::debug_span!(
            "rebase_branch",
            ledger_name,
            branch,
            strategy = strategy.as_str()
        );
        async move {
            self.rebase_branch_inner(ledger_name, branch, strategy)
                .await
        }
        .instrument(span)
        .await
    }

    async fn rebase_branch_inner(
        &self,
        ledger_name: &str,
        branch: &str,
        strategy: ConflictStrategy,
    ) -> Result<RebaseReport> {
        if branch == "main" {
            return Err(ApiError::Http {
                status: 400,
                message: "Cannot rebase the main branch".to_string(),
            });
        }

        let branch_id = format_ledger_id(ledger_name, branch);
        let branch_record = self
            .nameservice
            .lookup(&branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.clone()))?;

        let bp = branch_record
            .branch_point
            .as_ref()
            .ok_or_else(|| ApiError::Http {
                status: 400,
                message: format!("Branch {branch_id} has no branch point"),
            })?;

        let source_id = format_ledger_id(ledger_name, &bp.source);
        let source_record = self
            .nameservice
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        let source_head_id = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::internal(format!("Source branch {source_id} has no commit head"))
        })?;
        let source_head_t = source_record.commit_t;

        // Disconnect branch from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&branch_id).await;
        }

        // Fast-forward: branch has no unique commits.
        let is_fast_forward = branch_record
            .commit_head_id
            .as_ref()
            .is_none_or(|id| *id == bp.commit_id);

        if is_fast_forward {
            return self
                .fast_forward_rebase(
                    &branch_id,
                    &source_id,
                    &source_record,
                    &bp.source,
                    source_head_id,
                    source_head_t,
                )
                .await;
        }

        // Build a BranchedContentStore for reading commits across namespaces.
        let branch_store = LedgerState::build_branched_store(
            &self.nameservice,
            &branch_record,
            self.connection.storage(),
        )
        .await?;

        // Compute source delta: all (s,p,g) tuples modified on source since branch point.
        let source_store =
            fluree_db_core::content_store_for(self.connection.storage().clone(), &source_id);
        let source_delta = compute_delta_keys(source_store, source_head_id.clone(), bp.t).await?;

        // Pass 1: stream branch commits to collect lightweight summaries
        // (CID, t, conflict keys) without retaining flake payloads in memory.
        let summaries = scan_branch_commits(
            branch_store.clone(),
            branch_record.commit_head_id.clone().unwrap(),
            bp.t,
            &source_delta,
        )
        .await?;
        let total_commits = summaries.len();

        // Abort upfront if any commit conflicts — no commits will be written.
        if strategy == ConflictStrategy::Abort {
            if let Some(summary) = summaries.iter().find(|s| !s.conflict_keys.is_empty()) {
                return Err(ApiError::Http {
                    status: 409,
                    message: format!(
                        "Rebase aborted: {} conflict(s) at t={} with abort strategy",
                        summary.conflict_keys.len(),
                        summary.t
                    ),
                });
            }
        }

        // Copy the source index into the branch namespace before replay.
        // This gives the branch an index to build incrementally from when
        // novelty grows too large mid-rebase.
        self.copy_source_index(&source_id, &branch_id, &source_record)
            .await;

        // Load source state as the replay base.
        let mut current_state = LedgerState::load(
            &self.nameservice,
            &source_id,
            self.connection.storage().clone(),
        )
        .await?;

        // Commits publish to the branch namespace, not the source.
        current_state.snapshot.ledger_id = branch_id.clone();

        let mut report = RebaseReport {
            replayed: 0,
            conflicts: Vec::new(),
            failures: Vec::new(),
            new_branch_point_t: source_head_t,
            new_branch_point_id: source_head_id.clone(),
            fast_forward: false,
            total_commits,
            skipped: 0,
        };

        // Pass 2: replay each commit by loading its full payload on demand.
        for summary in &summaries {
            let has_conflicts = !summary.conflict_keys.is_empty();

            if has_conflicts && strategy == ConflictStrategy::Skip {
                report.conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: strategy.as_str(),
                });
                report.skipped += 1;
                continue;
            }

            // Load the full commit payload for this single commit.
            let commit =
                fluree_db_novelty::load_commit_by_id(&branch_store, &summary.commit_id).await?;

            // Build flakes for replay, applying conflict resolution.
            let flakes = self
                .resolve_flakes(
                    &commit.flakes,
                    &summary.conflict_keys,
                    &strategy,
                    &current_state,
                )
                .await?;

            if has_conflicts {
                report.conflicts.push(RebaseConflict {
                    original_t: summary.t,
                    conflict_count: summary.conflict_keys.len(),
                    keys: summary.conflict_keys.clone(),
                    resolution: strategy.as_str(),
                });
            }

            if flakes.is_empty() {
                continue;
            }

            // Stage and commit. Any failure aborts the entire rebase.
            current_state = self.replay_commit(current_state, flakes, &commit).await?;
            report.replayed += 1;

            // Flush novelty when it exceeds the soft threshold by building
            // an inline index and reloading the state. The reload picks up
            // the new index and reconstructs novelty only from commits since
            // the index point, bounding memory usage during large rebases.
            if current_state.should_reindex(&self.index_config) {
                current_state = self
                    .flush_rebase_novelty(&branch_id, &branch_record)
                    .await?;
            }
        }

        // Finalize: update branch point and force reload.
        let new_bp = BranchPoint {
            source: bp.source.clone(),
            commit_id: source_head_id,
            t: source_head_t,
        };
        self.nameservice
            .update_branch_point(&branch_id, new_bp)
            .await?;

        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&branch_id).await;
        }

        Ok(report)
    }

    async fn fast_forward_rebase(
        &self,
        branch_id: &str,
        source_id: &str,
        source_record: &fluree_db_nameservice::NsRecord,
        source_branch: &str,
        source_head_id: ContentId,
        source_head_t: i64,
    ) -> Result<RebaseReport> {
        self.nameservice
            .publish_commit(branch_id, source_head_t, &source_head_id)
            .await?;

        self.copy_source_index(source_id, branch_id, source_record)
            .await;

        let new_bp = BranchPoint {
            source: source_branch.to_string(),
            commit_id: source_head_id.clone(),
            t: source_head_t,
        };
        self.nameservice
            .update_branch_point(branch_id, new_bp)
            .await?;

        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(branch_id).await;
        }

        Ok(RebaseReport {
            replayed: 0,
            conflicts: Vec::new(),
            failures: Vec::new(),
            new_branch_point_t: source_head_t,
            new_branch_point_id: source_head_id,
            fast_forward: true,
            total_commits: 0,
            skipped: 0,
        })
    }

    /// Stage flakes and commit as a replay of the original commit.
    async fn replay_commit(
        &self,
        state: LedgerState,
        flakes: Vec<Flake>,
        original_commit: &Commit,
    ) -> Result<LedgerState> {
        let reverse_graph = state.snapshot.build_reverse_graph().map_err(|e| {
            ApiError::internal(format!("Failed to build reverse graph during rebase: {e}"))
        })?;

        let view = LedgerView::stage(state, flakes, &reverse_graph).map_err(|e| {
            ApiError::internal(format!("Failed to stage flakes during rebase: {e}"))
        })?;

        let ns_registry = NamespaceRegistry::from_db(view.db());
        let commit_opts = CommitOpts::default()
            .with_skip_backpressure()
            .with_skip_sequencing()
            .with_namespace_delta(original_commit.namespace_delta.clone())
            .with_graph_delta(original_commit.graph_delta.clone());

        let (_receipt, new_state) = fluree_db_transact::commit(
            view,
            ns_registry,
            self.connection.storage(),
            &self.nameservice,
            &self.index_config,
            commit_opts,
        )
        .await?;

        Ok(new_state)
    }

    /// Filter flakes based on the conflict strategy, generating retractions
    /// for `TakeBranch`.
    async fn resolve_flakes(
        &self,
        flakes: &[Flake],
        conflicting_keys: &[ConflictKey],
        strategy: &ConflictStrategy,
        source_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        if conflicting_keys.is_empty() {
            return Ok(flakes.to_vec());
        }

        let conflict_set: FxHashSet<&ConflictKey> = conflicting_keys.iter().collect();

        match strategy {
            ConflictStrategy::TakeSource => {
                // Drop branch's conflicting flakes (source wins).
                Ok(flakes
                    .iter()
                    .filter(|f| {
                        let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
                        !conflict_set.contains(&key)
                    })
                    .cloned()
                    .collect())
            }
            ConflictStrategy::TakeBranch => {
                // Keep branch's flakes + retract source's conflicting values.
                let retractions = self
                    .build_source_retractions(conflicting_keys, source_state)
                    .await?;
                let mut result = flakes.to_vec();
                result.extend(retractions);
                Ok(result)
            }
            // TakeBoth: keep all branch flakes, both values coexist.
            // Abort/Skip: handled before this function is called.
            _ => Ok(flakes.to_vec()),
        }
    }

    /// Look up the source state's current flakes for the given conflict keys
    /// and generate retraction flakes (`op: false`) for each.
    async fn build_source_retractions(
        &self,
        conflicting_keys: &[ConflictKey],
        source_state: &LedgerState,
    ) -> Result<Vec<Flake>> {
        let t = source_state.t();
        let mut retractions = Vec::new();

        for key in conflicting_keys {
            let g_id = match &key.g {
                None => fluree_db_core::DEFAULT_GRAPH_ID,
                Some(g_sid) => source_state
                    .snapshot
                    .decode_sid(g_sid)
                    .and_then(|iri| source_state.snapshot.graph_registry.graph_id_for_iri(&iri))
                    .unwrap_or(fluree_db_core::DEFAULT_GRAPH_ID),
            };

            let match_val = RangeMatch::subject_predicate(key.s.clone(), key.p.clone());
            let opts = RangeOptions {
                to_t: Some(t),
                ..Default::default()
            };

            let source_flakes = range_with_overlay(
                &source_state.snapshot,
                g_id,
                source_state.novelty.as_ref(),
                IndexType::Spot,
                RangeTest::Eq,
                match_val,
                opts,
            )
            .await?;

            for flake in source_flakes {
                if flake.op && flake.g == key.g {
                    retractions.push(Flake {
                        op: false,
                        t: 0, // overwritten by commit
                        ..flake
                    });
                }
            }
        }

        Ok(retractions)
    }

    /// Build an inline index for the branch mid-rebase to flush novelty.
    ///
    /// Uses `rebuild_index_from_commits_with_store` with a `BranchedContentStore`
    /// so the indexer can follow the branch's commit chain through parent
    /// namespaces. Publishes the result to the nameservice, then reloads the
    /// `LedgerState` so novelty only contains commits since the new index.
    async fn flush_rebase_novelty(
        &self,
        branch_id: &str,
        branch_record: &fluree_db_nameservice::NsRecord,
    ) -> Result<LedgerState> {
        tracing::debug!(
            branch_id,
            "building inline index mid-rebase to flush novelty"
        );

        let branch_store = LedgerState::build_branched_store(
            &self.nameservice,
            branch_record,
            self.connection.storage(),
        )
        .await?;

        let record = self
            .nameservice
            .lookup(branch_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(branch_id.to_string()))?;

        let indexer_config = crate::build_indexer_config(self.connection.config());

        let index_result = fluree_db_indexer::rebuild_index_from_commits_with_store(
            self.connection.storage(),
            branch_store,
            branch_id,
            &record,
            indexer_config,
        )
        .await
        .map_err(|e| ApiError::internal(format!("Mid-rebase index build failed: {e}")))?;

        self.nameservice
            .publish_index(branch_id, index_result.index_t, &index_result.root_id)
            .await?;

        LedgerState::load(
            &self.nameservice,
            branch_id,
            self.connection.storage().clone(),
        )
        .await
        .map_err(Into::into)
    }

    /// Copy index artifacts from source to branch (best-effort).
    async fn copy_source_index(
        &self,
        source_id: &str,
        branch_id: &str,
        source_record: &fluree_db_nameservice::NsRecord,
    ) {
        if let Some(ref index_cid) = source_record.index_head_id {
            if let Err(e) = self
                .copy_index_to_branch(source_id, branch_id, index_cid)
                .await
            {
                tracing::warn!(
                    %e, source = %source_id, branch = %branch_id,
                    "failed to copy index during rebase; branch will replay from genesis"
                );
            } else if let Err(e) = self
                .nameservice
                .publish_index(branch_id, source_record.index_t, index_cid)
                .await
            {
                tracing::warn!(%e, "failed to publish index for rebased branch");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Lightweight summary of a branch commit, collected during the scan pass.
/// Holds only the CID, `t`, and pre-computed conflict keys — not the full
/// flake payload, so the entire branch history fits in memory.
struct CommitSummary {
    commit_id: ContentId,
    t: i64,
    conflict_keys: Vec<ConflictKey>,
}

/// Stream branch commits HEAD→oldest, extract conflict keys, and return
/// lightweight summaries in oldest-first order. Full flake payloads are
/// dropped after conflict key extraction so only summaries remain in memory.
async fn scan_branch_commits<C: fluree_db_core::ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
    source_delta: &FxHashSet<ConflictKey>,
) -> Result<Vec<CommitSummary>> {
    let stream = trace_commits_by_id(store, head_id, stop_at_t);
    futures::pin_mut!(stream);

    let mut summaries = Vec::new();
    while let Some(commit) = stream.try_next().await? {
        let conflict_keys = find_conflicting_keys(&commit.flakes, source_delta);
        summaries.push(CommitSummary {
            commit_id: commit.id.expect("loaded commit should have an id"),
            t: commit.t,
            conflict_keys,
        });
    }

    summaries.reverse();
    Ok(summaries)
}

/// Find (s, p, g) keys from flakes that overlap with the source delta.
fn find_conflicting_keys(
    flakes: &[Flake],
    source_delta: &FxHashSet<ConflictKey>,
) -> Vec<ConflictKey> {
    let mut seen = FxHashSet::default();
    flakes
        .iter()
        .filter_map(|f| {
            let key = ConflictKey::new(f.s.clone(), f.p.clone(), f.g.clone());
            if source_delta.contains(&key) && seen.insert(key.clone()) {
                Some(key)
            } else {
                None
            }
        })
        .collect()
}
