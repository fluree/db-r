//! Commit chain walking helpers.
//!
//! Provides `walk_commit_chain_full` to walk the commit chain backward from
//! HEAD to genesis and return CIDs in chronological (genesis-first) order.

use fluree_db_core::storage::ContentStore;
use fluree_db_core::ContentId;
use fluree_db_novelty::commit_v2::read_commit_envelope;

use crate::error::{IndexerError, Result};

/// Walk the commit chain backward from `head` to genesis, returning CIDs
/// in chronological order (genesis first).
// Kept for: shared commit chain walking for both rebuild and incremental pipelines.
// Use when: rebuild.rs is refactored to use shared helpers instead of inline logic.
#[expect(dead_code)]
pub(crate) async fn walk_commit_chain_full(
    content_store: &dyn ContentStore,
    head_commit_id: &ContentId,
) -> Result<Vec<ContentId>> {
    let mut cids = Vec::new();
    let mut frontier = vec![head_commit_id.clone()];
    let mut visited = std::collections::HashSet::new();

    while let Some(cid) = frontier.pop() {
        if !visited.insert(cid.clone()) {
            continue;
        }
        let bytes = content_store
            .get(&cid)
            .await
            .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;
        let envelope =
            read_commit_envelope(&bytes).map_err(|e| IndexerError::StorageRead(e.to_string()))?;
        for parent_id in envelope.parent_ids() {
            frontier.push(parent_id.clone());
        }
        cids.push(cid);
    }

    cids.reverse(); // chronological order (genesis first)
    Ok(cids)
}
