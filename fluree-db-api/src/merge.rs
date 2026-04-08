//! Branch merge support.
//!
//! Merges a source branch into a target branch. Currently supports
//! fast-forward merges only: the target's HEAD must be the common
//! ancestor of both branches.

use crate::error::{ApiError, Result};
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{ContentId, ContentStore, Storage};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{
    CasResult, NameService, NsRecordSnapshot, Publisher, RefKind, RefPublisher, RefValue,
};
use fluree_db_novelty::commit_v2::read_commit_envelope;
use serde::Serialize;
use tracing::Instrument;

/// Summary report of a completed merge operation.
#[derive(Clone, Debug, Serialize)]
pub struct MergeReport {
    /// Target branch that was merged into.
    pub target: String,
    /// Source branch that was merged from.
    pub source: String,
    /// Whether this was a fast-forward merge.
    pub fast_forward: bool,
    /// New commit HEAD of the target after merge.
    pub new_head_t: i64,
    /// New commit HEAD CID of the target after merge.
    pub new_head_id: ContentId,
    /// Number of commit blobs copied to the target namespace.
    pub commits_copied: usize,
}

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + Publisher + RefPublisher + 'static,
{
    /// Merge a source branch into a target branch.
    ///
    /// Currently only fast-forward merges are supported: the target branch
    /// must not have any new commits since the source branch was created
    /// from it. If the target has diverged, this returns an error.
    ///
    /// If `target_branch` is `None`, the source's parent branch (from its
    /// branch point) is used as the target.
    pub async fn merge_branch(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
    ) -> Result<MergeReport> {
        let span = tracing::debug_span!("merge_branch", ledger_name, source_branch, ?target_branch);
        async move {
            self.merge_branch_inner(ledger_name, source_branch, target_branch)
                .await
        }
        .instrument(span)
        .await
    }

    async fn merge_branch_inner(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: Option<&str>,
    ) -> Result<MergeReport> {
        let source_id = format_ledger_id(ledger_name, source_branch);
        let source_record = self
            .nameservice
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        // Resolve target: explicit or from source's parent branch.
        let source_parent = source_record.source_branch.as_deref().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Branch {source_branch} has no source branch; \
                     only branches created from another branch can be merged"
            ))
        })?;

        let resolved_target = target_branch.unwrap_or(source_parent);

        if source_branch == resolved_target {
            return Err(ApiError::InvalidBranch(
                "Cannot merge a branch into itself".to_string(),
            ));
        }

        let target_id = format_ledger_id(ledger_name, resolved_target);
        let target_record = self
            .nameservice
            .lookup(&target_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(target_id.clone()))?;

        let source_head_id = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::InvalidBranch(format!(
                "Source branch {source_branch} has no commits to merge"
            ))
        })?;
        let source_head_t = source_record.commit_t;

        // Compute common ancestor to determine fast-forward eligibility.
        // Build a BranchedContentStore for the source so we can walk both
        // commit chains through parent namespaces.
        let source_store = LedgerState::build_branched_store(
            &self.nameservice,
            &source_record,
            self.connection.storage(),
        )
        .await?;

        let target_head = target_record.commit_head_id.as_ref();
        let ancestor = match target_head {
            Some(target_head_id) => Some(
                fluree_db_novelty::find_common_ancestor(
                    &source_store,
                    &source_head_id,
                    target_head_id,
                )
                .await?,
            ),
            None => None,
        };

        // Fast-forward check: target HEAD must be the common ancestor.
        let is_fast_forward = match (&ancestor, target_head) {
            (Some(a), Some(tid)) => a.commit_id == *tid,
            (None, None) => true,
            _ => false,
        };

        if !is_fast_forward {
            return Err(ApiError::BranchConflict(format!(
                "Cannot fast-forward merge: {resolved_target} has diverged since \
                 {source_branch} was created. Rebase the source branch first."
            )));
        }

        // Snapshot target nameservice state before mutations.
        // If any step fails after publish_commit, we roll back.
        let target_snapshot = NsRecordSnapshot::from_record(&target_record);
        let expected_target_head = RefValue {
            id: target_record.commit_head_id.clone(),
            t: target_record.commit_t,
        };

        // Already merged: source and target point at the same head, so this is a
        // successful no-op rather than a concurrent advancement conflict.
        if expected_target_head.t == source_head_t
            && expected_target_head.id.as_ref() == Some(&source_head_id)
        {
            return Ok(MergeReport {
                source: source_branch.to_string(),
                target: resolved_target.to_string(),
                fast_forward: true,
                commits_copied: 0,
                new_head_t: source_head_t,
                new_head_id: source_head_id,
            });
        }

        // Disconnect target from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&target_id).await;
        }

        let stop_at_t = ancestor.map(|a| a.t).unwrap_or(0);

        let result: Result<MergeReport> = async {
            // Copy commit and txn blobs from the source namespace into the target
            // namespace so the target is self-contained (no fallback reads needed).
            let copy_store =
                fluree_db_core::content_store_for(self.connection.storage().clone(), &source_id);
            let commits_copied = self
                .copy_commit_chain(&copy_store, &source_head_id, stop_at_t, &target_id)
                .await?;

            // Advance target's HEAD to source's HEAD.
            match self
                .nameservice
                .compare_and_set_ref(
                    &target_id,
                    RefKind::CommitHead,
                    Some(&expected_target_head),
                    &RefValue {
                        id: Some(source_head_id.clone()),
                        t: source_head_t,
                    },
                )
                .await?
            {
                CasResult::Updated => {}
                CasResult::Conflict { actual } => {
                    let actual_t = actual.as_ref().map(|r| r.t).unwrap_or(0);
                    let actual_id = actual
                        .and_then(|r| r.id)
                        .map(|cid| cid.to_string())
                        .unwrap_or_else(|| "None".to_string());
                    return Err(ApiError::BranchConflict(format!(
                        "Merge aborted: target branch advanced concurrently (expected head t={} id={}, found t={} id={})",
                        expected_target_head.t,
                        expected_target_head
                            .id
                            .as_ref()
                            .map(|cid| cid.to_string())
                            .unwrap_or_else(|| "None".to_string()),
                        actual_t,
                        actual_id
                    )));
                }
            }

            // Copy source's index to target namespace.
            if let Some(ref index_cid) = source_record.index_head_id {
                if let Err(e) = self
                    .copy_index_to_branch(&source_id, &target_id, index_cid)
                    .await
                {
                    tracing::warn!(
                        %e, source = %source_id, target = %target_id,
                        "failed to copy index during merge; target will rebuild from commits"
                    );
                } else if let Err(e) = self
                    .nameservice
                    .publish_index(&target_id, source_record.index_t, index_cid)
                    .await
                {
                    tracing::warn!(%e, "failed to publish index for merged target");
                }
            }

            Ok(MergeReport {
                target: resolved_target.to_string(),
                source: source_branch.to_string(),
                fast_forward: true,
                new_head_t: source_head_t,
                new_head_id: source_head_id,
                commits_copied,
            })
        }
        .await;

        match result {
            Ok(report) => Ok(report),
            Err(e) => {
                tracing::warn!(
                    source = %source_id,
                    target = %target_id,
                    error = %e,
                    "merge failed, rolling back nameservice state"
                );
                if let Err(rollback_err) = self
                    .nameservice
                    .reset_head(&target_id, target_snapshot)
                    .await
                {
                    tracing::error!(
                        target = %target_id,
                        error = %rollback_err,
                        "failed to roll back target nameservice state after merge failure"
                    );
                }
                Err(e)
            }
        }
    }

    /// Copy commit blobs (and their referenced txn blobs) from a source
    /// content store into the target's storage namespace.
    ///
    /// Walks the commit chain from `head_id` backwards, stopping at
    /// `stop_at_t` (the branch point). Each commit blob is read once,
    /// the envelope is parsed from the raw bytes to find the previous
    /// commit and any txn reference, and then the blob is written to
    /// the target namespace.
    ///
    /// Within each iteration the txn blob copy (read + write) runs
    /// concurrently with the next commit blob read, since neither
    /// depends on the other.
    async fn copy_commit_chain(
        &self,
        source_store: &impl ContentStore,
        head_id: &ContentId,
        stop_at_t: i64,
        target_ledger_id: &str,
    ) -> Result<usize> {
        let storage = self.connection.storage();
        let mut copied = 0;

        /// Read a blob from the source store, treating `NotFound` as `Ok(None)`
        /// (the commit lives in the parent namespace — we've reached the
        /// branch boundary).
        async fn try_get(store: &impl ContentStore, cid: &ContentId) -> Result<Option<Vec<u8>>> {
            match store.get(cid).await {
                Ok(b) => Ok(Some(b)),
                Err(fluree_db_core::error::Error::NotFound(_)) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }

        // Bootstrap: read the first commit before entering the loop.
        let mut pending_bytes = try_get(source_store, head_id).await?;
        let mut current_cid = head_id.clone();

        while let Some(bytes) = pending_bytes.take() {
            let envelope = read_commit_envelope(&bytes).map_err(|e| {
                ApiError::internal(format!("failed to read commit envelope {current_cid}: {e}"))
            })?;

            if envelope.t <= stop_at_t {
                break;
            }

            // Write commit blob to target namespace.
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    target_ledger_id,
                    &current_cid.digest_hex(),
                    &bytes,
                )
                .await?;

            // Overlap: copy the txn blob concurrently with reading the
            // next commit blob.  The two operations are independent.
            let txn_fut = async {
                let Some(ref txn_cid) = envelope.txn else {
                    return Ok(());
                };
                let txn_bytes = source_store.get(txn_cid).await?;
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::Txn,
                        target_ledger_id,
                        &txn_cid.digest_hex(),
                        &txn_bytes,
                    )
                    .await?;
                Ok::<_, crate::error::ApiError>(())
            };

            let next_commit_fut = async {
                let Some(prev_cid) = envelope.previous_id() else {
                    return Ok::<_, crate::error::ApiError>((current_cid.clone(), None));
                };
                Ok((prev_cid.clone(), try_get(source_store, prev_cid).await?))
            };

            let (txn_result, next_result) = tokio::join!(txn_fut, next_commit_fut);
            txn_result?;
            let (next_cid, next_bytes) = next_result?;
            current_cid = next_cid;
            pending_bytes = next_bytes;
            copied += 1;
        }

        tracing::debug!(commits = copied, "copied commit chain to target namespace");
        Ok(copied)
    }
}
