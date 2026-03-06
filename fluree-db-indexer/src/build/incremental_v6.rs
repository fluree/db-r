//! V6 (FIR6) incremental index pipeline.
//!
//! This is the V3 index format counterpart of `incremental.rs` (V5).
//! Resolves only new commits since the last indexed `t`, merges novelty into
//! affected FLI3 leaves via the V3 incremental modules, updates dictionaries,
//! and publishes a new FIR6 root.
//!
//! ## Phases
//!
//! 1. **Resolve**: Load V6 root, walk new commits, produce `RunRecordV2` + ops
//! 2. **Branch updates**: For each (graph, order), sort novelty by order,
//!    fetch existing branch, call `update_branch_v3`, upload new blobs
//! 3. **Dict updates**: Update reverse trees + forward packs for new subjects/strings
//! 4. **Root assembly**: `IncrementalRootBuilderV6` → encode → CAS write → publish
//!
//! Deferred to later iterations:
//! - Arena updates (numbig, vectors, spatial, fulltext)
//! - Stats / HLL refresh
//! - Schema refresh (rdfs:subClassOf / rdfs:subPropertyOf)
//! - Named graph support (currently default graph only)

use std::sync::Arc;

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef};
use fluree_db_core::{ContentId, ContentKind, Storage};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::run_index::build::incremental_branch_v3::{
    update_branch_v3, BranchUpdateConfigV3, BranchUpdateResultV3,
};
use crate::run_index::build::incremental_resolve_v6::{
    resolve_incremental_commits_v6, IncrementalResolveConfigV6,
};
use crate::run_index::build::incremental_root_v6::IncrementalRootBuilderV6;
use crate::{IndexResult, IndexStats, IndexerConfig};

/// Entry point for V6 incremental indexing.
///
/// Called from `build_index_for_ledger` when a V3/V6 index exists and
/// incremental conditions are met.
pub async fn incremental_index_v6<S>(
    storage: &S,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let base_root_id = record.index_head_id.clone().ok_or(IndexerError::NoIndex)?;
    let head_commit_id = record
        .commit_head_id
        .clone()
        .ok_or(IndexerError::NoCommits)?;
    let from_t = record.index_t;

    let content_store: Arc<dyn fluree_db_core::storage::ContentStore> = Arc::new(
        fluree_db_core::storage::content_store_for(storage.clone(), ledger_id),
    );

    // ---- Phase 1: Resolve incremental commits ----
    let resolve_config = IncrementalResolveConfigV6 {
        base_root_id: base_root_id.clone(),
        head_commit_id,
        from_t,
    };
    let novelty = resolve_incremental_commits_v6(content_store.clone(), resolve_config)
        .await
        .map_err(|e| IndexerError::StorageWrite(format!("V6 incremental resolve: {e}")))?;

    if novelty.records.is_empty() {
        tracing::info!("no new records resolved; returning existing V6 root");
        return Ok(IndexResult {
            root_id: base_root_id,
            index_t: novelty.max_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats::default(),
        });
    }

    tracing::info!(
        records = novelty.records.len(),
        new_subjects = novelty.new_subjects.len(),
        new_strings = novelty.new_strings.len(),
        max_t = novelty.max_t,
        "V6 Phase 1 complete: incremental resolve"
    );

    // ---- Phase 2: Per-(graph, order) branch updates ----
    let all_orders = [
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];

    // Group records + ops by g_id.
    let mut by_graph: std::collections::BTreeMap<u16, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, rec) in novelty.records.iter().enumerate() {
        by_graph.entry(rec.g_id).or_default().push(idx);
    }

    let base_root = &novelty.base_root;
    let mut root_builder = IncrementalRootBuilderV6::from_old_root(novelty.base_root.clone());
    root_builder.set_index_t(novelty.max_t);
    root_builder.add_commit_stats(
        novelty.delta_commit_size,
        novelty.delta_asserts,
        novelty.delta_retracts,
    );

    let mut total_new_leaves = 0usize;

    for (&g_id, indices) in &by_graph {
        // Extract this graph's records + ops.
        let graph_records: Vec<RunRecordV2> = indices.iter().map(|&i| novelty.records[i]).collect();
        let graph_ops: Vec<u8> = indices.iter().map(|&i| novelty.ops[i]).collect();

        let is_default_graph = g_id == 0;

        for &order in &all_orders {
            // Sort records for this order.
            let cmp = cmp_v2_for_order(order);
            let mut sorted_indices: Vec<usize> = (0..graph_records.len()).collect();
            sorted_indices.sort_unstable_by(|&a, &b| cmp(&graph_records[a], &graph_records[b]));
            let sorted_records: Vec<RunRecordV2> =
                sorted_indices.iter().map(|&i| graph_records[i]).collect();
            let sorted_ops: Vec<u8> = sorted_indices.iter().map(|&i| graph_ops[i]).collect();

            if is_default_graph {
                // Default graph: leaf entries are inline in the root.
                let existing_leaves = base_root
                    .default_graph_orders
                    .iter()
                    .find(|o| o.order == order)
                    .map(|o| &o.leaves);

                if let Some(leaves) = existing_leaves {
                    // Build a temporary FBR3 manifest for update_branch_v3.
                    let branch_bytes =
                        fluree_db_binary_index::format::branch_v3::build_branch_v3_bytes(
                            order, g_id, leaves,
                        );

                    let branch_config = BranchUpdateConfigV3 {
                        order,
                        g_id,
                        zstd_level: 1,
                        leaflet_target_rows: config.leaflet_rows.max(1),
                        leaf_target_rows: config
                            .leaflet_rows
                            .max(1)
                            .saturating_mul(config.leaflets_per_leaf.max(1)),
                    };

                    let cs = content_store.clone();
                    let cs2 = content_store.clone();
                    let result = update_branch_v3(
                        &branch_bytes,
                        &sorted_records,
                        &sorted_ops,
                        &branch_config,
                        &|cid| {
                            futures::executor::block_on(async { cs.get(cid).await })
                                .map_err(std::io::Error::other)
                        },
                        &|cid| {
                            futures::executor::block_on(async { cs2.get(cid).await })
                                .map(Some)
                                .or(Ok(None))
                        },
                    )
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!(
                            "V6 branch update g={g_id} {order:?}: {e}"
                        ))
                    })?;

                    // Upload new leaf + sidecar blobs.
                    for blob in &result.new_leaf_blobs {
                        storage
                            .content_write_bytes(
                                ContentKind::IndexLeafV3,
                                ledger_id,
                                &blob.info.leaf_bytes,
                            )
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                        if let Some(ref sc_bytes) = blob.info.sidecar_bytes {
                            storage
                                .content_write_bytes(
                                    ContentKind::HistorySidecar,
                                    ledger_id,
                                    sc_bytes,
                                )
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        }
                    }

                    total_new_leaves += result.new_leaf_blobs.len();

                    // Update root with new leaf entries + GC.
                    root_builder.set_default_graph_order(order, result.leaf_entries);
                    root_builder.add_replaced_cids(result.replaced_leaf_cids);
                    root_builder.add_replaced_cids(result.replaced_sidecar_cids);
                } else {
                    // No existing branch for this order — build from scratch.
                    let result = build_fresh_default_graph_v3(
                        &sorted_records,
                        &sorted_ops,
                        order,
                        g_id,
                        &config,
                    )?;

                    // Upload blobs.
                    for blob in &result.new_leaf_blobs {
                        storage
                            .content_write_bytes(
                                ContentKind::IndexLeafV3,
                                ledger_id,
                                &blob.info.leaf_bytes,
                            )
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                        if let Some(ref sc_bytes) = blob.info.sidecar_bytes {
                            storage
                                .content_write_bytes(
                                    ContentKind::HistorySidecar,
                                    ledger_id,
                                    sc_bytes,
                                )
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        }
                    }

                    total_new_leaves += result.new_leaf_blobs.len();
                    root_builder.set_default_graph_order(order, result.leaf_entries);
                }
            } else {
                // Named graph: branch stored as separate CAS object (FBR3).
                let branch_cid = base_root
                    .named_graphs
                    .iter()
                    .find(|ng| ng.g_id == g_id)
                    .and_then(|ng| ng.orders.iter().find(|(o, _)| *o == order))
                    .map(|(_, cid)| cid);

                if let Some(existing_branch_cid) = branch_cid {
                    // Fetch existing branch manifest.
                    let branch_bytes =
                        content_store.get(existing_branch_cid).await.map_err(|e| {
                            IndexerError::StorageRead(format!(
                                "fetch V3 branch g_id={g_id} {order:?}: {e}"
                            ))
                        })?;

                    let branch_config = BranchUpdateConfigV3 {
                        order,
                        g_id,
                        zstd_level: 1,
                        leaflet_target_rows: config.leaflet_rows.max(1),
                        leaf_target_rows: config
                            .leaflet_rows
                            .max(1)
                            .saturating_mul(config.leaflets_per_leaf.max(1)),
                    };

                    let cs = content_store.clone();
                    let cs2 = content_store.clone();
                    let result = update_branch_v3(
                        &branch_bytes,
                        &sorted_records,
                        &sorted_ops,
                        &branch_config,
                        &|cid| {
                            futures::executor::block_on(async { cs.get(cid).await })
                                .map_err(std::io::Error::other)
                        },
                        &|cid| {
                            futures::executor::block_on(async { cs2.get(cid).await })
                                .map(Some)
                                .or(Ok(None))
                        },
                    )
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!(
                            "V6 branch update g={g_id} {order:?}: {e}"
                        ))
                    })?;

                    // Upload new leaf + sidecar blobs.
                    for blob in &result.new_leaf_blobs {
                        storage
                            .content_write_bytes(
                                ContentKind::IndexLeafV3,
                                ledger_id,
                                &blob.info.leaf_bytes,
                            )
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                        if let Some(ref sc_bytes) = blob.info.sidecar_bytes {
                            storage
                                .content_write_bytes(
                                    ContentKind::HistorySidecar,
                                    ledger_id,
                                    sc_bytes,
                                )
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        }
                    }

                    // Upload new branch manifest.
                    storage
                        .content_write_bytes(
                            ContentKind::IndexBranchV3,
                            ledger_id,
                            &result.branch_bytes,
                        )
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    total_new_leaves += result.new_leaf_blobs.len();
                    root_builder.set_named_graph_branch(g_id, order, result.branch_cid);
                    root_builder.add_replaced_cids(result.replaced_leaf_cids);
                    root_builder.add_replaced_cids(result.replaced_sidecar_cids);
                } else {
                    // No existing branch for this named graph + order — build from scratch.
                    let result = build_fresh_named_graph_v3(&sorted_records, order, g_id, &config)?;

                    // Upload blobs.
                    for blob in &result.new_leaf_blobs {
                        storage
                            .content_write_bytes(
                                ContentKind::IndexLeafV3,
                                ledger_id,
                                &blob.info.leaf_bytes,
                            )
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                        if let Some(ref sc_bytes) = blob.info.sidecar_bytes {
                            storage
                                .content_write_bytes(
                                    ContentKind::HistorySidecar,
                                    ledger_id,
                                    sc_bytes,
                                )
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        }
                    }

                    // Upload branch manifest.
                    storage
                        .content_write_bytes(
                            ContentKind::IndexBranchV3,
                            ledger_id,
                            &result.branch_bytes,
                        )
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    total_new_leaves += result.new_leaf_blobs.len();
                    root_builder.set_named_graph_branch(g_id, order, result.branch_cid);
                }
            }
        }
    }

    tracing::info!(
        new_leaves = total_new_leaves,
        graphs = by_graph.len(),
        "V6 Phase 2 complete: branch updates"
    );

    // ---- Phase 3: Dict updates ----
    // Reuse V5 incremental dict tree update functions (same DictRefsV5 format).
    use super::dicts::{
        upload_incremental_reverse_tree_async, upload_incremental_reverse_tree_async_strings,
    };
    let mut new_dict_refs = base_root.dict_refs.clone();

    if !novelty.new_subjects.is_empty() {
        tracing::info!(
            count = novelty.new_subjects.len(),
            "V6 Phase 3: updating subject reverse tree"
        );
        let updated = upload_incremental_reverse_tree_async(
            storage,
            ledger_id,
            fluree_db_core::DictKind::SubjectReverse,
            &content_store,
            &base_root.dict_refs.subject_reverse,
            novelty.new_subjects.clone(),
        )
        .await?;
        root_builder.add_replaced_cids(updated.replaced_cids);
        new_dict_refs.subject_reverse = updated.tree_refs;
    }

    if !novelty.new_strings.is_empty() {
        tracing::info!(
            count = novelty.new_strings.len(),
            "V6 Phase 3: updating string reverse tree"
        );
        let updated = upload_incremental_reverse_tree_async_strings(
            storage,
            ledger_id,
            fluree_db_core::DictKind::StringReverse,
            &content_store,
            &base_root.dict_refs.string_reverse,
            novelty.new_strings.clone(),
        )
        .await?;
        root_builder.add_replaced_cids(updated.replaced_cids);
        new_dict_refs.string_reverse = updated.tree_refs;
    }

    // Forward pack updates (FPK1): append new pack artifacts for new subjects/strings.
    // Forward packs are append-only: existing pack refs are preserved, new entries get
    // their own pack artifacts appended to the routing list.
    {
        use super::upload::cid_from_write;
        use fluree_db_binary_index::dict::incremental::{
            build_incremental_string_packs, build_incremental_subject_packs_for_ns,
        };
        use fluree_db_binary_index::format::index_root::PackBranchEntry;

        // String forward packs.
        // Invariant: new_strings is sorted by string_id ascending (enforced by resolver).
        if !novelty.new_strings.is_empty() {
            debug_assert!(
                novelty.new_strings.windows(2).all(|w| w[0].0 < w[1].0),
                "new_strings must be sorted by string_id ascending"
            );

            let existing_refs = &base_root.dict_refs.forward_packs.string_fwd_packs;
            let entries: Vec<(u32, &[u8])> = novelty
                .new_strings
                .iter()
                .map(|(id, val)| (*id, val.as_slice()))
                .collect();

            let pack_result = build_incremental_string_packs(existing_refs, &entries)
                .map_err(|e| IndexerError::StorageWrite(format!("string fwd pack build: {e}")))?;

            // Upload new pack artifacts and build updated refs.
            let mut updated_refs = existing_refs.clone();
            let kind = ContentKind::DictBlob {
                dict: fluree_db_core::DictKind::StringForward,
            };
            for pack in &pack_result.new_packs {
                let cas_result = storage
                    .content_write_bytes(kind, ledger_id, &pack.bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                updated_refs.push(PackBranchEntry {
                    first_id: pack.first_id,
                    last_id: pack.last_id,
                    pack_cid: cid_from_write(kind, &cas_result),
                });
            }
            new_dict_refs.forward_packs.string_fwd_packs = updated_refs;

            tracing::info!(
                new_packs = pack_result.new_packs.len(),
                new_strings = novelty.new_strings.len(),
                "V6 Phase 3: string forward packs updated"
            );
        }

        // Subject forward packs (per namespace).
        // Invariant: new_subjects is sorted by (ns_code, local_id) ascending (enforced by resolver).
        if !novelty.new_subjects.is_empty() {
            debug_assert!(
                novelty
                    .new_subjects
                    .windows(2)
                    .all(|w| (w[0].0, w[0].1) <= (w[1].0, w[1].1)),
                "new_subjects must be sorted by (ns_code, local_id) ascending"
            );

            // Group by namespace. BTreeMap preserves ns_code order;
            // within each namespace, local_ids are already sorted.
            let mut by_ns: std::collections::BTreeMap<u16, Vec<(u64, Vec<u8>)>> =
                std::collections::BTreeMap::new();
            for (ns_code, local_id, suffix) in &novelty.new_subjects {
                by_ns
                    .entry(*ns_code)
                    .or_default()
                    .push((*local_id, suffix.clone()));
            }

            let kind = ContentKind::DictBlob {
                dict: fluree_db_core::DictKind::SubjectForward,
            };

            for (ns_code, entries) in &by_ns {
                // Find existing refs for this namespace.
                let existing_ns_refs: Vec<PackBranchEntry> = new_dict_refs
                    .forward_packs
                    .subject_fwd_ns_packs
                    .iter()
                    .find(|(ns, _)| ns == ns_code)
                    .map(|(_, refs)| refs.clone())
                    .unwrap_or_default();

                let entry_refs: Vec<(u64, &[u8])> = entries
                    .iter()
                    .map(|(id, val)| (*id, val.as_slice()))
                    .collect();

                let pack_result = build_incremental_subject_packs_for_ns(
                    *ns_code,
                    &existing_ns_refs,
                    &entry_refs,
                )
                .map_err(|e| {
                    IndexerError::StorageWrite(format!("subject fwd pack build ns={ns_code}: {e}"))
                })?;

                // Upload new packs and build updated refs.
                let mut updated_refs = existing_ns_refs;
                for pack in &pack_result.new_packs {
                    let cas_result = storage
                        .content_write_bytes(kind, ledger_id, &pack.bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    updated_refs.push(PackBranchEntry {
                        first_id: pack.first_id,
                        last_id: pack.last_id,
                        pack_cid: cid_from_write(kind, &cas_result),
                    });
                }

                // Update or insert namespace entry.
                if let Some(entry) = new_dict_refs
                    .forward_packs
                    .subject_fwd_ns_packs
                    .iter_mut()
                    .find(|(ns, _)| ns == ns_code)
                {
                    entry.1 = updated_refs;
                } else {
                    new_dict_refs
                        .forward_packs
                        .subject_fwd_ns_packs
                        .push((*ns_code, updated_refs));
                }

                tracing::info!(
                    ns_code,
                    new_packs = pack_result.new_packs.len(),
                    new_subjects = entries.len(),
                    "V6 Phase 3: subject forward packs updated"
                );
            }
        }
    }

    root_builder.set_dict_refs(new_dict_refs);

    // Update metadata from resolver state.
    let new_ns_codes: std::collections::BTreeMap<u16, String> = novelty
        .shared
        .ns_prefixes
        .iter()
        .map(|(&code, prefix)| (code, prefix.clone()))
        .collect();
    root_builder.set_namespace_codes(new_ns_codes.clone());

    let new_pred_sids = build_predicate_sids(&novelty.shared, &new_ns_codes);
    root_builder.set_predicate_sids(new_pred_sids);

    let new_graph_iris: Vec<String> = (0..novelty.shared.graphs.len())
        .filter_map(|id| novelty.shared.graphs.resolve(id).map(|s| s.to_string()))
        .collect();
    root_builder.set_graph_iris(new_graph_iris);

    let new_datatype_iris: Vec<String> = (0..novelty.shared.datatypes.len())
        .filter_map(|id| novelty.shared.datatypes.resolve(id).map(|s| s.to_string()))
        .collect();
    root_builder.set_datatype_iris(new_datatype_iris);

    let new_language_tags: Vec<String> = novelty
        .shared
        .languages
        .iter()
        .map(|(_, tag)| tag.to_string())
        .collect();
    root_builder.set_language_tags(new_language_tags);

    root_builder.set_watermarks(
        novelty.updated_watermarks.clone(),
        novelty.updated_string_watermark,
    );

    // ---- Phase 4: Root assembly ----
    root_builder.set_prev_index(Some(BinaryPrevIndexRef {
        t: base_root.index_t,
        id: base_root_id.clone(),
    }));

    let (new_root, replaced_cids) = root_builder.build();

    // Write garbage manifest.
    if !replaced_cids.is_empty() {
        let garbage_strings: Vec<String> = replaced_cids.iter().map(|c| c.to_string()).collect();
        let garbage_ref =
            gc::write_garbage_record(storage, ledger_id, new_root.index_t, garbage_strings)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        // Set garbage on the root before encoding.
        let mut final_root = new_root;
        final_root.garbage = garbage_ref.map(|id| BinaryGarbageRef { id });

        let root_bytes = final_root.encode();
        let write_result = storage
            .content_write_bytes(ContentKind::IndexRootV6, ledger_id, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        let root_id = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT_V6,
            &write_result.content_hash,
        )
        .ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "invalid root digest for FIR6: {}",
                write_result.content_hash
            ))
        })?;

        tracing::info!(
            %root_id,
            index_t = final_root.index_t,
            replaced = replaced_cids.len(),
            new_leaves = total_new_leaves,
            "V6 incremental index root published"
        );

        Ok(IndexResult {
            root_id,
            index_t: final_root.index_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats {
                flake_count: novelty.records.len(),
                leaf_count: total_new_leaves,
                branch_count: by_graph.len(),
                total_bytes: root_bytes.len(),
            },
        })
    } else {
        let root_bytes = new_root.encode();
        let write_result = storage
            .content_write_bytes(ContentKind::IndexRootV6, ledger_id, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        let root_id = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT_V6,
            &write_result.content_hash,
        )
        .ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "invalid root digest for FIR6: {}",
                write_result.content_hash
            ))
        })?;

        tracing::info!(
            %root_id,
            index_t = new_root.index_t,
            new_leaves = total_new_leaves,
            "V6 incremental index root published (no garbage)"
        );

        Ok(IndexResult {
            root_id,
            index_t: new_root.index_t,
            ledger_id: ledger_id.to_string(),
            stats: IndexStats {
                flake_count: novelty.records.len(),
                leaf_count: total_new_leaves,
                branch_count: by_graph.len(),
                total_bytes: root_bytes.len(),
            },
        })
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Build a fresh V3 branch from pure novelty for the default graph.
fn build_fresh_default_graph_v3(
    sorted_records: &[RunRecordV2],
    _sorted_ops: &[u8],
    order: RunSortOrder,
    g_id: u16,
    config: &IndexerConfig,
) -> Result<BranchUpdateResultV3> {
    use crate::run_index::build::incremental_leaf_v3::NewLeafBlobV3;
    use fluree_db_binary_index::format::branch_v3::{build_branch_v3_bytes, LeafEntryV3};
    use fluree_db_binary_index::format::leaf_v3::LeafWriterV3;
    use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;

    let leaflet_target = config.leaflet_rows.max(1);
    let leaf_target = leaflet_target.saturating_mul(config.leaflets_per_leaf.max(1));

    let mut writer = LeafWriterV3::new(order, leaflet_target, leaf_target, 1);
    for rec in sorted_records {
        writer
            .push_record(*rec)
            .map_err(|e| IndexerError::StorageWrite(format!("V3 fresh branch writer: {e}")))?;
    }

    let leaf_infos = writer
        .finish()
        .map_err(|e| IndexerError::StorageWrite(format!("V3 fresh branch finish: {e}")))?;

    let mut leaf_entries = Vec::with_capacity(leaf_infos.len());
    let mut new_blobs = Vec::with_capacity(leaf_infos.len());

    for info in leaf_infos {
        let header =
            fluree_db_binary_index::format::leaf_v3::decode_leaf_header_v3(&info.leaf_bytes)
                .map_err(|e| {
                    IndexerError::StorageWrite(format!("decode fresh leaf header: {e}"))
                })?;
        let first_key = read_ordered_key_v2(order, &header.first_key);
        let last_key = read_ordered_key_v2(order, &header.last_key);

        leaf_entries.push(LeafEntryV3 {
            first_key,
            last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid.clone(),
            sidecar_cid: info.sidecar_cid.clone(),
        });

        new_blobs.push(NewLeafBlobV3 { info });
    }

    let branch_bytes = build_branch_v3_bytes(order, g_id, &leaf_entries);
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    let branch_cid = ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH_V3,
        &branch_hash,
    )
    .ok_or_else(|| IndexerError::StorageWrite(format!("invalid V3 branch hash: {branch_hash}")))?;

    Ok(BranchUpdateResultV3 {
        leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids: Vec::new(),
        replaced_sidecar_cids: Vec::new(),
        branch_bytes,
        branch_cid,
    })
}

/// Build a fresh V3 branch from pure novelty for a named graph.
///
/// Same as `build_fresh_default_graph_v3` — the build pipeline is graph-agnostic.
fn build_fresh_named_graph_v3(
    sorted_records: &[RunRecordV2],
    order: RunSortOrder,
    g_id: u16,
    config: &IndexerConfig,
) -> Result<BranchUpdateResultV3> {
    build_fresh_default_graph_v3(sorted_records, &[], order, g_id, config)
}

/// Build predicate SIDs from resolver state for the index root.
fn build_predicate_sids(
    shared: &crate::run_index::resolve::resolver::SharedResolverState,
    namespace_codes: &std::collections::BTreeMap<u16, String>,
) -> Vec<(u16, String)> {
    use fluree_db_core::PrefixTrie;

    let ns_map: std::collections::HashMap<u16, String> = namespace_codes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    let trie = PrefixTrie::from_namespace_codes(&ns_map);

    (0..shared.predicates.len())
        .map(|p_id| {
            let iri = shared.predicates.resolve(p_id).unwrap_or("");
            if let Some((ns_code, prefix_len)) = trie.longest_match(iri) {
                let suffix = &iri[prefix_len..];
                (ns_code, suffix.to_string())
            } else {
                (0, iri.to_string())
            }
        })
        .collect()
}
