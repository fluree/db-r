//! CAS upload primitives and index artifact upload.
//!
//! Contains low-level helpers for writing content to CAS (`cid_from_write`,
//! `upload_dict_blob`, `upload_dict_file`) and the bounded-parallelism
//! `upload_indexes_to_cas` function for uploading index branches and leaves.

use fluree_db_binary_index::RunSortOrder;
use fluree_db_core::{ContentId, ContentKind, ContentWriteResult, GraphId, Storage};

use crate::error::{IndexerError, Result};

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

/// Upload index artifacts (FLI3 leaves, FHS1 sidecars, FBR3 branches) to CAS.
///
/// Default graph (g_id=0) collects inline `LeafEntry` for root embedding.
/// Named graphs upload branch manifests and return branch CIDs.
pub(crate) async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    build_result: &crate::BuildResult,
) -> Result<UploadedIndexes> {
    use fluree_db_binary_index::format::branch::LeafEntry;
    use fluree_db_binary_index::format::index_root::NamedGraphRouting;
    use std::collections::BTreeMap;

    let mut default_orders: Vec<(RunSortOrder, Vec<LeafEntry>)> = Vec::new();
    let mut named_map: BTreeMap<GraphId, Vec<(RunSortOrder, ContentId)>> = BTreeMap::new();

    for (order, order_result) in &build_result.order_results {
        for graph in &order_result.graphs {
            let g_id = graph.g_id;
            let is_default_graph = g_id == 0;

            // Upload leaf blobs + sidecar blobs.
            for leaf_info in &graph.leaf_infos {
                // Guard: sidecar_cid and sidecar_bytes must agree.
                match (&leaf_info.sidecar_cid, &leaf_info.sidecar_bytes) {
                    (Some(_), None) => {
                        return Err(IndexerError::StorageWrite(
                            "leaf has sidecar_cid but no sidecar_bytes".into(),
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(IndexerError::StorageWrite(
                            "leaf has sidecar_bytes but no sidecar_cid".into(),
                        ));
                    }
                    _ => {}
                }

                // Sidecar first (CAS ordering: sidecar must exist before leaf references it).
                if let (Some(sc_cid), Some(sc_bytes)) =
                    (&leaf_info.sidecar_cid, &leaf_info.sidecar_bytes)
                {
                    storage
                        .content_write_bytes_with_hash(
                            ContentKind::HistorySidecar,
                            ledger_id,
                            &sc_cid.digest_hex(),
                            sc_bytes,
                        )
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }

                // Leaf blob.
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexLeaf,
                        ledger_id,
                        &leaf_info.leaf_cid.digest_hex(),
                        &leaf_info.leaf_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            }

            // Upload branch manifest for named graphs.
            let uploaded_branch_cid = if !is_default_graph {
                storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexBranch,
                        ledger_id,
                        &graph.branch_cid.digest_hex(),
                        &graph.branch_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Some(graph.branch_cid.clone())
            } else {
                None
            };

            if is_default_graph {
                default_orders.push((*order, graph.leaf_entries.clone()));
            } else {
                let branch_cid = uploaded_branch_cid.expect("named graph must have branch CID");
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

    Ok(UploadedIndexes {
        default_graph_orders: default_orders,
        named_graphs,
    })
}
