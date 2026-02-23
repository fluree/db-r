//! CAS upload primitives and index artifact upload.
//!
//! Contains low-level helpers for writing content to CAS (`cid_from_write`,
//! `upload_dict_blob`, `upload_dict_file`) and the bounded-parallelism
//! `upload_indexes_to_cas` function for uploading index branches and leaves.

use fluree_db_binary_index::{InlineOrderRouting, NamedGraphRouting, RunSortOrder};
use fluree_db_core::{ContentId, ContentKind, ContentWriteResult, GraphId, Storage};

use crate::error::{IndexerError, Result};
use crate::run_index;

use super::types::UploadedIndexes;

/// Derive a `ContentId` from a `ContentWriteResult`.
///
/// Every `content_write_bytes{,_with_hash}` call returns a SHA-256 hex digest.
/// This helper wraps `ContentId::from_hex_digest` so callers don't repeat
/// the pattern.
pub(crate) fn cid_from_write(kind: ContentKind, result: &ContentWriteResult) -> ContentId {
    ContentId::from_hex_digest(kind.to_codec(), &result.content_hash)
        .expect("storage produced a valid SHA-256 hex digest")
}

/// Upload a single dict blob (already in memory) to CAS and return (cid, write_result).
pub(crate) async fn upload_dict_blob<S: Storage>(
    storage: &S,
    ledger_id: &str,
    dict: fluree_db_core::DictKind,
    bytes: &[u8],
    msg: &'static str,
) -> Result<(ContentId, ContentWriteResult)> {
    let kind = ContentKind::DictBlob { dict };
    let result = storage
        .content_write_bytes(kind, ledger_id, bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    tracing::debug!(
        address = %result.address,
        bytes = result.size_bytes,
        "{msg}"
    );
    let cid = cid_from_write(kind, &result);
    Ok((cid, result))
}

/// Read a dict artifact file from disk and upload it to CAS.
pub(crate) async fn upload_dict_file<S: Storage>(
    storage: &S,
    ledger_id: &str,
    path: &std::path::Path,
    dict: fluree_db_core::DictKind,
    msg: &'static str,
) -> Result<(ContentId, ContentWriteResult)> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
    let (cid, wr) = upload_dict_blob(storage, ledger_id, dict, &bytes, msg).await?;
    tracing::debug!(path = %path.display(), "dict artifact source path");
    Ok((cid, wr))
}

/// Upload all index artifacts (branches + leaves) to CAS.
///
/// For the default graph (g_id=0), leaves are uploaded but the branch is NOT —
/// leaf routing is embedded inline in the root. For named graphs, both branches
/// and leaves are uploaded.
pub(crate) async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    build_results: &[(RunSortOrder, run_index::IndexBuildResult)],
) -> Result<UploadedIndexes> {
    use fluree_db_core::ContentKind;
    use futures::StreamExt;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    // Accumulators for default graph inline routing and named graph branch pointers.
    let mut default_orders: Vec<InlineOrderRouting> = Vec::new();
    let mut named_map: BTreeMap<GraphId, Vec<(RunSortOrder, ContentId)>> = BTreeMap::new();

    // Bounded parallelism for leaf uploads.
    let leaf_upload_concurrency: usize = std::env::var("FLUREE_CAS_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(16)
        .clamp(1, 128);
    let leaf_sem = Arc::new(Semaphore::new(leaf_upload_concurrency));

    let total_order_count = build_results.len();
    for (order_idx, (order, result)) in build_results.iter().enumerate() {
        let order_name = order.dir_name().to_string();
        let order_name_for_tasks: Arc<str> = Arc::from(order_name.clone());
        let order_graphs = result.graphs.len();
        let order_total_leaves: usize = result.graphs.iter().map(|g| g.leaf_count as usize).sum();
        tracing::info!(
            order = %order_name.to_uppercase(),
            graphs = order_graphs,
            leaves = order_total_leaves,
            progress = format!("{}/{}", order_idx + 1, total_order_count),
            "uploading index order to CAS"
        );

        for graph_result in &result.graphs {
            let g_id = graph_result.g_id;
            let graph_dir = &graph_result.graph_dir;
            let is_default_graph = g_id == 0;

            // Named graphs: upload branch manifest to CAS.
            // Default graph: skip branch upload (routing is inline in root).
            let uploaded_branch_cid = if !is_default_graph {
                let branch_cid = &graph_result.branch_cid;
                let branch_path = graph_dir.join(branch_cid.to_string());
                let branch_bytes = tokio::fs::read(&branch_path).await.map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read branch {}: {}",
                        branch_path.display(),
                        e
                    ))
                })?;
                let branch_write = storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexBranch,
                        ledger_id,
                        &branch_cid.digest_hex(),
                        &branch_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Some(cid_from_write(ContentKind::IndexBranch, &branch_write))
            } else {
                None
            };

            // Upload leaf files (all graphs, bounded parallelism).
            // Use leaf_entries from build results instead of re-parsing the branch.
            let total_leaves = graph_result.leaf_entries.len();
            let completed = Arc::new(AtomicUsize::new(0));

            let mut futs = futures::stream::FuturesUnordered::new();
            for (leaf_idx, leaf_entry) in graph_result.leaf_entries.iter().enumerate() {
                let sem = Arc::clone(&leaf_sem);
                let completed = Arc::clone(&completed);
                let path = graph_dir.join(leaf_entry.leaf_cid.to_string());
                let content_hash = leaf_entry.leaf_cid.digest_hex();
                let order_name = Arc::clone(&order_name_for_tasks);

                futs.push(async move {
                    let _permit = sem.acquire_owned().await.map_err(|_| {
                        IndexerError::StorageWrite("leaf upload semaphore closed".into())
                    })?;

                    let leaf_bytes = tokio::fs::read(&path).await.map_err(|e| {
                        IndexerError::StorageRead(format!("read leaf {}: {}", path.display(), e))
                    })?;
                    storage
                        .content_write_bytes_with_hash(
                            ContentKind::IndexLeaf,
                            ledger_id,
                            &content_hash,
                            &leaf_bytes,
                        )
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if total_leaves >= 100 && done.is_multiple_of(500) {
                        tracing::info!(
                            order = %order_name.as_ref(),
                            g_id,
                            leaf = done,
                            total_leaves,
                            "leaf upload progress"
                        );
                    }

                    Ok::<usize, IndexerError>(leaf_idx)
                });
            }

            while let Some(res) = futs.next().await {
                res?;
            }

            if is_default_graph {
                // Default graph: collect inline leaf entries for root embedding.
                tracing::info!(
                    g_id = 0,
                    order = %order_name,
                    leaves = total_leaves,
                    "default graph leaves uploaded (inline routing)"
                );
                default_orders.push(InlineOrderRouting {
                    order: *order,
                    leaves: graph_result.leaf_entries.clone(),
                });
            } else {
                // Named graph: record branch CID.
                let branch_cid = uploaded_branch_cid.expect("named graph must have branch CID");
                tracing::info!(
                    g_id,
                    order = %order_name,
                    leaves = total_leaves,
                    branch = %branch_cid,
                    "named graph index uploaded to CAS"
                );
                named_map
                    .entry(g_id)
                    .or_default()
                    .push((*order, branch_cid));
            }
        }
    }

    let named_graphs: Vec<NamedGraphRouting> = named_map
        .into_iter()
        .map(|(g_id, orders)| NamedGraphRouting { g_id, orders })
        .collect();

    let total_default_leaves: usize = default_orders.iter().map(|o| o.leaves.len()).sum();
    let total_named_branches: usize = named_graphs.iter().map(|ng| ng.orders.len()).sum();
    tracing::info!(
        default_graph_orders = default_orders.len(),
        default_graph_leaves = total_default_leaves,
        named_graphs = named_graphs.len(),
        named_branches = total_named_branches,
        "index artifacts uploaded to CAS"
    );

    Ok(UploadedIndexes {
        default_graph_orders: default_orders,
        named_graphs,
    })
}
