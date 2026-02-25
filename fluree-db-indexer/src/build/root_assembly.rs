//! Root encode, CAS write, garbage chain, and IndexResult derivation.
//!
//! Both the full-rebuild and incremental pipelines end by encoding an
//! `IndexRootV5`, optionally attaching a garbage manifest, writing the
//! root to CAS, and deriving an `IndexResult`. This module provides a
//! shared helper to avoid duplicating that logic.

use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef, IndexRootV5};
use fluree_db_core::{ContentId, ContentKind, Storage};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::{IndexResult, IndexStats};

/// Context for linking the GC chain to a previous index root.
///
/// Used by both pipelines, but computed differently:
/// - **Rebuild**: loads the old root from CAS, computes `all_cas_ids()` set
///   difference to find garbage CIDs.
/// - **Incremental**: `IncrementalRootBuilder` tracks replaced CIDs explicitly.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use encode_and_write_root().
pub(crate) struct GarbageContext {
    /// CIDs that should be recorded as garbage (replaced by this new root).
    pub garbage_cids: Vec<ContentId>,
    /// Previous root linkage (for GC chain traversal).
    pub prev_index: Option<BinaryPrevIndexRef>,
}

/// Encode an `IndexRootV5`, attach garbage/prev_index, write to CAS,
/// and return an `IndexResult`.
///
/// This is the shared "last mile" for both rebuild and incremental pipelines.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use this shared helper.
#[expect(dead_code)]
pub(crate) async fn encode_and_write_root<S: Storage>(
    storage: &S,
    ledger_id: &str,
    mut root: IndexRootV5,
    garbage_ctx: Option<GarbageContext>,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    // Attach garbage manifest and prev_index if provided.
    if let Some(ctx) = garbage_ctx {
        if let Some(prev) = ctx.prev_index {
            root.prev_index = Some(prev);
        }

        if !ctx.garbage_cids.is_empty() {
            let garbage_strings: Vec<String> =
                ctx.garbage_cids.iter().map(|c| c.to_string()).collect();
            root.garbage =
                gc::write_garbage_record(storage, ledger_id, root.index_t, garbage_strings)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                    .map(|id| BinaryGarbageRef { id });

            tracing::info!(
                garbage_count = ctx.garbage_cids.len(),
                "GC chain: garbage record written"
            );
        }
    }

    tracing::info!(
        index_t = root.index_t,
        default_orders = root.default_graph_orders.len(),
        named_graphs = root.named_graphs.len(),
        "encoding and writing IRB1 root to CAS"
    );

    // Encode and write root.
    let root_bytes = root.encode();
    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, ledger_id, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    // Derive ContentId from the root's content hash.
    let root_id = ContentId::from_hex_digest(
        fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
        &write_result.content_hash,
    )
    .ok_or_else(|| {
        IndexerError::StorageWrite(format!(
            "invalid content_hash from write result: {}",
            write_result.content_hash
        ))
    })?;

    tracing::info!(
        %root_id,
        index_t = root.index_t,
        root_bytes = root_bytes.len(),
        "index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: ledger_id.to_string(),
        stats: IndexStats {
            total_bytes: root_bytes.len(),
            ..result_stats
        },
    })
}

/// Compute garbage CIDs by comparing old root's CAS IDs with new root's CAS IDs.
///
/// Used by the full-rebuild pipeline which has access to the previous root via CAS.
// Kept for: shared GC chain computation for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F.7 is refactored to use this shared helper.
#[expect(dead_code)]
pub(crate) async fn compute_garbage_from_prev_root(
    content_store: &dyn fluree_db_core::storage::ContentStore,
    new_root: &IndexRootV5,
    prev_root_id: &ContentId,
) -> Option<GarbageContext> {
    let prev_bytes = content_store.get(prev_root_id).await.ok()?;
    let prev_root = IndexRootV5::decode(&prev_bytes).ok()?;

    let prev_t = prev_root.index_t;
    let old_ids: std::collections::HashSet<ContentId> =
        prev_root.all_cas_ids().into_iter().collect();
    let new_ids: std::collections::HashSet<ContentId> =
        new_root.all_cas_ids().into_iter().collect();
    let garbage_cids: Vec<ContentId> = old_ids.difference(&new_ids).cloned().collect();

    Some(GarbageContext {
        garbage_cids,
        prev_index: Some(BinaryPrevIndexRef {
            t: prev_t,
            id: prev_root_id.clone(),
        }),
    })
}
