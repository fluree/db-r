//! Branch merge support.
//!
//! Merges a source branch into a target branch. Currently supports
//! fast-forward merges only: the target must not have advanced since
//! the source branch was created.

use crate::error::{ApiError, Result};
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::{ContentId, ContentStore, Storage};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::trace_commit_envelopes_by_id;
use futures::TryStreamExt;
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
    N: NameService + Publisher + 'static,
{
    /// Merge a source branch into a target branch.
    ///
    /// Currently only fast-forward merges are supported: the target branch
    /// must not have any new commits since the source branch was created
    /// from it. If the target has diverged, this returns an error.
    pub async fn merge_branch(
        &self,
        ledger_name: &str,
        source_branch: &str,
        target_branch: &str,
    ) -> Result<MergeReport> {
        let span = tracing::debug_span!("merge_branch", ledger_name, source_branch, target_branch,);
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
        target_branch: &str,
    ) -> Result<MergeReport> {
        if source_branch == target_branch {
            return Err(ApiError::Http {
                status: 400,
                message: "Cannot merge a branch into itself".to_string(),
            });
        }

        let source_id = format_ledger_id(ledger_name, source_branch);
        let source_record = self
            .nameservice
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        let bp = source_record
            .branch_point
            .as_ref()
            .ok_or_else(|| ApiError::Http {
                status: 400,
                message: format!(
                    "Branch {source_branch} has no branch point; \
                     only branches created from another branch can be merged"
                ),
            })?;

        if bp.source != target_branch {
            return Err(ApiError::Http {
                status: 400,
                message: format!(
                    "Branch {source_branch} was created from '{}', not '{target_branch}'",
                    bp.source
                ),
            });
        }

        let target_id = format_ledger_id(ledger_name, target_branch);
        let target_record = self
            .nameservice
            .lookup(&target_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(target_id.clone()))?;

        // Fast-forward check: target must not have advanced since the branch
        // was created.
        let target_head = target_record.commit_head_id.as_ref();
        let is_fast_forward = target_head.is_none_or(|id| *id == bp.commit_id);

        if !is_fast_forward {
            return Err(ApiError::Http {
                status: 409,
                message: format!(
                    "Cannot fast-forward merge: {target_branch} has diverged since \
                     {source_branch} was created. Rebase the source branch first."
                ),
            });
        }

        let source_head_id =
            source_record
                .commit_head_id
                .clone()
                .ok_or_else(|| ApiError::Http {
                    status: 400,
                    message: format!("Source branch {source_branch} has no commits to merge"),
                })?;
        let source_head_t = source_record.commit_t;

        // Disconnect target from ledger manager to prevent stale reads.
        if let Some(ref lm) = self.ledger_manager {
            lm.disconnect(&target_id).await;
        }

        // Copy commit and txn blobs from the source namespace into the target
        // namespace so the target is self-contained (no fallback reads needed).
        let source_store =
            fluree_db_core::content_store_for(self.connection.storage().clone(), &source_id);
        let commits_copied = self
            .copy_commit_chain(source_store, &source_head_id, bp.t, &target_id)
            .await?;

        // Advance target's HEAD to source's HEAD.
        self.nameservice
            .publish_commit(&target_id, source_head_t, &source_head_id)
            .await?;

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

        // Advance the source branch's branch_point so subsequent merges
        // from the same branch see the correct divergence point.
        let new_bp = fluree_db_nameservice::BranchPoint {
            source: target_branch.to_string(),
            commit_id: source_head_id.clone(),
            t: source_head_t,
        };
        self.nameservice
            .update_branch_point(&source_id, new_bp)
            .await?;

        Ok(MergeReport {
            target: target_branch.to_string(),
            source: source_branch.to_string(),
            fast_forward: true,
            new_head_t: source_head_t,
            new_head_id: source_head_id,
            commits_copied,
        })
    }

    /// Copy commit blobs (and their referenced txn blobs) from a source
    /// content store into the target's storage namespace.
    ///
    /// Walks the commit chain from `head_id` backwards, stopping at
    /// `stop_at_t` (the branch point). Each commit blob and its optional
    /// txn blob are read from the source store and written to the target
    /// namespace via `content_write_bytes_with_hash`.
    async fn copy_commit_chain<C: ContentStore + Clone + 'static>(
        &self,
        source_store: C,
        head_id: &ContentId,
        stop_at_t: i64,
        target_ledger_id: &str,
    ) -> Result<usize> {
        let storage = self.connection.storage();

        let stream = trace_commit_envelopes_by_id(source_store.clone(), head_id.clone(), stop_at_t);
        futures::pin_mut!(stream);

        let mut copied = 0;
        while let Some((cid, envelope)) = stream.try_next().await? {
            // Copy the commit blob.
            let bytes = source_store.get(&cid).await?;
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    target_ledger_id,
                    &cid.digest_hex(),
                    &bytes,
                )
                .await?;

            // Copy the txn blob if present.
            if let Some(ref txn_cid) = envelope.txn {
                let txn_bytes = source_store.get(txn_cid).await?;
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::Txn,
                        target_ledger_id,
                        &txn_cid.digest_hex(),
                        &txn_bytes,
                    )
                    .await?;
            }

            copied += 1;
        }

        tracing::debug!(commits = copied, "copied commit chain to target namespace");
        Ok(copied)
    }
}
