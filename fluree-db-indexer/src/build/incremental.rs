//! Incremental index pipeline (Phase 1..5).
//!
//! Instead of rebuilding from genesis, loads the existing `IndexRootV5`,
//! resolves only new commits since the last indexed t, merges novelty into
//! affected leaves, updates dictionaries incrementally, and publishes a
//! new root that references mostly-unchanged CAS artifacts.
//!
//! Falls back to full rebuild on any error -- correctness is never at risk.

use fluree_db_binary_index::{
    BinaryGarbageRef, BinaryIndexStore, BinaryPrevIndexRef, PackBranchEntry, RunRecord,
    RunSortOrder, SpatialArenaRefV5,
};
use fluree_db_core::{ContentId, ContentKind, Storage};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::run_index;
use crate::{IndexResult, IndexStats, IndexerConfig};

use super::upload::{cid_from_write, upload_dict_blob};

pub async fn incremental_index_from_root<S>(
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
    let storage = storage.clone();
    let ledger_id = ledger_id.to_string();
    let handle = tokio::runtime::Handle::current();

    let span = tracing::info_span!(
        "incremental_index",
        ledger_id = %ledger_id,
        from_t = from_t,
        head = %head_commit_id,
    );

    tokio::task::spawn_blocking(move || {
        let _guard = span.enter();
        handle.block_on(async {
            incremental_index_inner(
                &storage,
                &ledger_id,
                base_root_id,
                head_commit_id,
                from_t,
                config,
            )
            .await
        })
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("incremental index task panicked: {}", e)))?
}

/// Inner async implementation of incremental indexing.
///
/// Separated from `incremental_index_from_root` to keep the spawn_blocking
/// wrapper clean and enable direct async calls from tests.
async fn incremental_index_inner<S>(
    storage: &S,
    ledger_id: &str,
    base_root_id: ContentId,
    head_commit_id: ContentId,
    from_t: i64,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use crate::build::dicts::{
        upload_incremental_reverse_tree_async, upload_incremental_reverse_tree_async_strings,
    };
    use crate::build::types::UpdatedReverseTree;
    use fluree_db_binary_index::format::branch::read_branch_v2_from_bytes;
    use fluree_db_binary_index::format::run_record::{cmp_for_order, RunSortOrder};
    use run_index::incremental_branch::{
        update_branch, IncrementalBranchConfig, IncrementalBranchError,
    };
    use run_index::incremental_resolve::{resolve_incremental_commits, IncrementalResolveConfig};
    use run_index::incremental_root::IncrementalRootBuilder;
    use std::sync::Arc;

    let content_store: Arc<dyn fluree_db_core::storage::ContentStore> = Arc::new(
        fluree_db_core::storage::content_store_for(storage.clone(), ledger_id),
    );

    // ---- Phase 1: Resolve incremental commits ----
    let resolve_config = IncrementalResolveConfig {
        base_root_id: base_root_id.clone(),
        head_commit_id,
        from_t,
    };
    let novelty = resolve_incremental_commits(content_store.clone(), resolve_config)
        .await
        .map_err(|e| IndexerError::StorageWrite(format!("incremental resolve: {e}")))?;

    if novelty.records.is_empty() {
        tracing::info!("no new records resolved; returning existing root");
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
        "Phase 1 complete: incremental resolve"
    );
    let base_root = &novelty.base_root;

    // ---- Phase 2-3: Per-(graph, order) branch updates ----
    // Partition records by graph, then build 4 order-sorted copies per graph.
    let all_orders: &[RunSortOrder] = &[
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];

    // Group records by g_id.
    let mut by_graph: std::collections::BTreeMap<u16, Vec<&RunRecord>> =
        std::collections::BTreeMap::new();
    for record in &novelty.records {
        by_graph.entry(record.g_id).or_default().push(record);
    }

    // Format-level constants for leaflet/leaf sizing.
    // These match the full-rebuild pipeline and are tied to the FLI2 format.
    //
    // Note: these can be overridden via IndexerConfig for testing/specialized builds.
    let branch_config = IncrementalBranchConfig {
        zstd_level: 1,
        max_parallel_leaves: 4,
        leaflet_target_rows: config.leaflet_rows.max(1),
        leaflet_split_rows: (config.leaflet_rows.max(1) * 3) / 2,
        leaflets_per_leaf: config.leaflets_per_leaf.max(1),
        leaf_split_leaflets: config.leaflets_per_leaf.max(1) * 2,
    };

    let p_width = fluree_db_binary_index::format::leaflet::p_width_for_max(
        novelty.shared.predicates.len().saturating_sub(1) as u32,
    );

    // Detect width promotion: if the new predicate or datatype count requires
    // a wider encoding than the base root used, fall back to full rebuild.
    // Existing leaves are encoded with the old width; re-encoding merged
    // leaflets with a new width within the same leaf file is not supported.
    let base_pred_count = base_root.predicate_sids.len() as u32;
    let base_p_width =
        fluree_db_binary_index::format::leaflet::p_width_for_max(base_pred_count.saturating_sub(1));
    if p_width > base_p_width {
        return Err(IndexerError::IncrementalAbort(format!(
            "predicate width promotion ({base_p_width} -> {p_width}) requires full rebuild"
        )));
    }
    let dt_count = novelty.shared.datatypes.len() as u32;
    let base_dt_count = base_root.datatype_iris.len() as u32;
    let dt_width =
        fluree_db_binary_index::format::leaflet::dt_width_for_max(dt_count.saturating_sub(1));
    let base_dt_width =
        fluree_db_binary_index::format::leaflet::dt_width_for_max(base_dt_count.saturating_sub(1));
    if dt_width > base_dt_width {
        return Err(IndexerError::IncrementalAbort(format!(
            "datatype width promotion ({base_dt_width} -> {dt_width}) requires full rebuild"
        )));
    }

    let mut root_builder = IncrementalRootBuilder::from_old_root(novelty.base_root.clone());
    root_builder.set_index_t(novelty.max_t);
    root_builder.add_commit_stats(
        novelty.delta_commit_size,
        novelty.delta_asserts,
        novelty.delta_retracts,
    );

    // ---- Build unified work queue: branch updates + dict updates ----
    //
    // Both branch and dict work run concurrently in a single JoinSet, bounded
    // by the same semaphore. Results are collected, sorted deterministically,
    // and applied to IncrementalRootBuilder after all tasks complete.

    // -- Work item / result types --

    struct BranchWorkItem {
        g_id: u16,
        order: RunSortOrder,
        /// Shared unsorted records for this graph — sorting is deferred until
        /// the semaphore permit is acquired so only `max_concurrency` sorted
        /// copies exist at once.
        graph_records: Arc<Vec<RunRecord>>,
        existing_manifest: Option<fluree_db_binary_index::format::branch::BranchManifest>,
        old_branch_cid: Option<ContentId>,
    }

    struct BranchWorkResult {
        g_id: u16,
        order: RunSortOrder,
        update: run_index::incremental_branch::BranchUpdateResult,
        old_branch_cid: Option<ContentId>,
    }

    /// Forward pack result for a single dict (string or subject ns).
    struct FwdPackResult {
        all_pack_refs: Vec<PackBranchEntry>,
    }

    enum DictWorkResult {
        StringForwardPacks(FwdPackResult),
        SubjectForwardPacks { ns_code: u16, result: FwdPackResult },
        SubjectReverseTree(UpdatedReverseTree),
        StringReverseTree(UpdatedReverseTree),
    }

    /// Sort key for deterministic result application.
    /// Branch results sort before dict results; within each category,
    /// items sort by their natural keys.
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum WorkResultKey {
        Branch { g_id: u16, order: RunSortOrder },
        DictStringFwdPacks,
        DictSubjectFwdPacks { ns_code: u16 },
        DictSubjectReverseTree,
        DictStringReverseTree,
    }

    enum WorkResult {
        Branch(BranchWorkResult),
        Dict(DictWorkResult),
    }

    impl WorkResult {
        fn sort_key(&self) -> WorkResultKey {
            match self {
                WorkResult::Branch(b) => WorkResultKey::Branch {
                    g_id: b.g_id,
                    order: b.order,
                },
                WorkResult::Dict(d) => match d {
                    DictWorkResult::StringForwardPacks(_) => WorkResultKey::DictStringFwdPacks,
                    DictWorkResult::SubjectForwardPacks { ns_code, .. } => {
                        WorkResultKey::DictSubjectFwdPacks { ns_code: *ns_code }
                    }
                    DictWorkResult::SubjectReverseTree(_) => WorkResultKey::DictSubjectReverseTree,
                    DictWorkResult::StringReverseTree(_) => WorkResultKey::DictStringReverseTree,
                },
            }
        }
    }

    // -- Shared state for all tasks --

    let max_concurrency = config.incremental_max_concurrency.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrency));
    let content_store_shared = content_store.clone();
    let storage_shared = Arc::new(storage.clone());
    let branch_config = Arc::new(branch_config);
    let ledger_id_shared: Arc<str> = Arc::from(ledger_id);

    let mut join_set: tokio::task::JoinSet<Result<WorkResult>> = tokio::task::JoinSet::new();

    // -- Build branch work items --

    // Build per-graph shared record vectors (one copy per graph, shared across
    // 4 orders via Arc). Sorting is deferred to inside each task.
    let mut shared_graph_records: std::collections::BTreeMap<u16, Arc<Vec<RunRecord>>> =
        std::collections::BTreeMap::new();
    for (&g_id, refs) in &by_graph {
        let records: Vec<RunRecord> = refs.iter().map(|r| **r).collect();
        shared_graph_records.insert(g_id, Arc::new(records));
    }

    let mut n_branch_items = 0usize;
    for (&g_id, graph_records) in &shared_graph_records {
        let is_default_graph = g_id == 0;

        for &order in all_orders {
            let (existing_manifest, old_branch_cid) = if is_default_graph {
                let manifest = base_root
                    .default_graph_orders
                    .iter()
                    .find(|o| o.order == order)
                    .map(|o| fluree_db_binary_index::format::branch::BranchManifest {
                        leaves: o.leaves.clone(),
                    });
                (manifest, None)
            } else {
                let branch_ref = base_root
                    .named_graphs
                    .iter()
                    .find(|ng| ng.g_id == g_id)
                    .and_then(|ng| ng.orders.iter().find(|(o, _)| *o == order))
                    .map(|(_, cid)| cid);

                if let Some(cid) = branch_ref {
                    let branch_bytes = content_store.get(cid).await.map_err(|e| {
                        IndexerError::StorageRead(format!(
                            "load branch g_id={g_id} order={order:?}: {e}"
                        ))
                    })?;
                    let manifest = read_branch_v2_from_bytes(&branch_bytes).map_err(|e| {
                        IndexerError::StorageRead(format!(
                            "decode branch g_id={g_id} order={order:?}: {e}"
                        ))
                    })?;
                    (Some(manifest), Some(cid.clone()))
                } else {
                    (None, None)
                }
            };

            let item = BranchWorkItem {
                g_id,
                order,
                graph_records: Arc::clone(graph_records),
                existing_manifest,
                old_branch_cid,
            };

            n_branch_items += 1;
            let sem = semaphore.clone();
            let cs = content_store_shared.clone();
            let st = storage_shared.clone();
            let cfg = branch_config.clone();
            let lid = ledger_id_shared.clone();

            join_set.spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|_| IndexerError::StorageWrite("semaphore closed".into()))?;

                let g_id = item.g_id;
                let order = item.order;
                let is_default = g_id == 0;

                // Sort records for this order (deferred from work-item construction).
                let mut sorted_records: Vec<RunRecord> = (*item.graph_records).clone();
                drop(item.graph_records);
                let cmp = cmp_for_order(order);
                sorted_records.sort_unstable_by(cmp);

                let update_result = if let Some(manifest) = item.existing_manifest {
                    // Pre-fetch affected leaves from CAS (async I/O).
                    let cmp = cmp_for_order(order);
                    let mut prefetched: std::collections::HashMap<ContentId, Vec<u8>> =
                        std::collections::HashMap::new();

                    let n_leaves = manifest.leaves.len();
                    let mut start = 0usize;
                    for (i, leaf) in manifest.leaves.iter().enumerate() {
                        let end = if i == n_leaves - 1 {
                            sorted_records.len()
                        } else {
                            let next_key = &manifest.leaves[i + 1].first_key;
                            start
                                + sorted_records[start..].partition_point(|r| {
                                    cmp(r, next_key) == std::cmp::Ordering::Less
                                })
                        };
                        let has_novelty = end > start;
                        if has_novelty && !prefetched.contains_key(&leaf.leaf_cid) {
                            let bytes = cs.get(&leaf.leaf_cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "fetch leaf {}: {e}",
                                    leaf.leaf_cid
                                ))
                            })?;
                            prefetched.insert(leaf.leaf_cid.clone(), bytes);
                        }
                        start = end;
                    }

                    // CPU-bound merge/encode in spawn_blocking.
                    let cfg_inner = cfg.clone();
                    tokio::task::spawn_blocking(move || {
                        let mut fetch_leaf = |cid: &ContentId| -> std::result::Result<
                            Vec<u8>,
                            IncrementalBranchError,
                        > {
                            prefetched.get(cid).cloned().ok_or_else(|| {
                                IncrementalBranchError::Io(std::io::Error::other(format!(
                                    "leaf not pre-fetched: {cid}"
                                )))
                            })
                        };

                        update_branch(
                            &manifest,
                            &sorted_records,
                            order,
                            g_id,
                            &cfg_inner,
                            &mut fetch_leaf,
                        )
                        .map_err(|e| match e {
                            IncrementalBranchError::EmptyLeafletWithHistory => {
                                IndexerError::StorageWrite(
                                    "incremental: empty leaflet with history".to_string(),
                                )
                            }
                            IncrementalBranchError::Io(io_err) => {
                                IndexerError::StorageWrite(format!("incremental branch: {io_err}"))
                            }
                        })
                    })
                    .await
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!("branch update task panicked: {e}"))
                    })??
                } else {
                    let cfg_inner = cfg.clone();
                    tokio::task::spawn_blocking(move || {
                        build_fresh_branch(
                            &sorted_records,
                            order,
                            g_id,
                            &cfg_inner,
                            p_width,
                            base_dt_width,
                        )
                    })
                    .await
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!("fresh branch build task panicked: {e}"))
                    })??
                };

                // Upload new leaf blobs to CAS.
                for blob in &update_result.new_leaf_blobs {
                    st.content_write_bytes_with_hash(
                        ContentKind::IndexLeaf,
                        &lid,
                        &blob.cid.digest_hex(),
                        &blob.bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }

                // Upload branch manifest for named graphs.
                if !is_default {
                    st.content_write_bytes_with_hash(
                        ContentKind::IndexBranch,
                        &lid,
                        &update_result.branch_cid.digest_hex(),
                        &update_result.branch_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }

                Ok(WorkResult::Branch(BranchWorkResult {
                    g_id,
                    order,
                    update: update_result,
                    old_branch_cid: item.old_branch_cid,
                }))
            });
        }
    }

    // Drop shared_graph_records — each work item holds its own Arc.
    drop(shared_graph_records);

    // -- Build dict work items --

    let mut n_dict_items = 0usize;

    // 4a: String forward packs.
    if !novelty.new_strings.is_empty() {
        n_dict_items += 1;
        let sem = semaphore.clone();
        let st = storage_shared.clone();
        let lid = ledger_id_shared.clone();
        let existing_refs = base_root.dict_refs.forward_packs.string_fwd_packs.clone();
        // Own the entries so they're Send + 'static.
        let new_entries: Vec<(u32, Vec<u8>)> = novelty
            .new_strings
            .iter()
            .map(|(id, val)| (*id, val.clone()))
            .collect();

        join_set.spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|_| IndexerError::StorageWrite("semaphore closed".into()))?;

            // CPU-bound pack building in spawn_blocking.
            let existing_refs_inner = existing_refs.clone();
            let pack_result = tokio::task::spawn_blocking(move || {
                let refs: Vec<(u32, &[u8])> = new_entries
                    .iter()
                    .map(|(id, v)| (*id, v.as_slice()))
                    .collect();
                fluree_db_binary_index::dict::incremental::build_incremental_string_packs(
                    &existing_refs_inner,
                    &refs,
                )
            })
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("string fwd pack task panicked: {e}")))?
            .map_err(|e| IndexerError::StorageWrite(format!("incremental string packs: {e}")))?;

            // Upload new pack artifacts.
            let kind = ContentKind::DictBlob {
                dict: fluree_db_core::DictKind::StringForward,
            };
            let mut updated_refs = existing_refs;
            for pack in &pack_result.new_packs {
                let cas_result = st
                    .content_write_bytes(kind, &lid, &pack.bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                updated_refs.push(PackBranchEntry {
                    first_id: pack.first_id,
                    last_id: pack.last_id,
                    pack_cid: cid_from_write(kind, &cas_result),
                });
            }

            Ok(WorkResult::Dict(DictWorkResult::StringForwardPacks(
                FwdPackResult {
                    all_pack_refs: updated_refs,
                },
            )))
        });
    }

    // 4a: Subject forward packs (one task per ns_code).
    if !novelty.new_subjects.is_empty() {
        // Group by ns_code.
        let mut by_ns: std::collections::BTreeMap<u16, Vec<(u64, Vec<u8>)>> =
            std::collections::BTreeMap::new();
        for (ns_code, local_id, suffix) in &novelty.new_subjects {
            by_ns
                .entry(*ns_code)
                .or_default()
                .push((*local_id, suffix.clone()));
        }

        for (ns_code, entries) in by_ns {
            n_dict_items += 1;
            let sem = semaphore.clone();
            let st = storage_shared.clone();
            let lid = ledger_id_shared.clone();
            let existing_ns_refs: Vec<PackBranchEntry> = base_root
                .dict_refs
                .forward_packs
                .subject_fwd_ns_packs
                .iter()
                .find(|(ns, _)| *ns == ns_code)
                .map(|(_, refs)| refs.clone())
                .unwrap_or_default();

            join_set.spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|_| IndexerError::StorageWrite("semaphore closed".into()))?;

                // CPU-bound pack building in spawn_blocking.
                let existing_inner = existing_ns_refs.clone();
                let pack_result = tokio::task::spawn_blocking(move || {
                    let refs: Vec<(u64, &[u8])> =
                        entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
                    fluree_db_binary_index::dict::incremental::build_incremental_subject_packs_for_ns(
                        ns_code,
                        &existing_inner,
                        &refs,
                    )
                })
                .await
                .map_err(|e| {
                    IndexerError::StorageWrite(format!(
                        "subject fwd pack ns={ns_code} task panicked: {e}"
                    ))
                })?
                .map_err(|e| {
                    IndexerError::StorageWrite(format!(
                        "incremental subject packs ns={ns_code}: {e}"
                    ))
                })?;

                let kind = ContentKind::DictBlob {
                    dict: fluree_db_core::DictKind::SubjectForward,
                };
                let mut updated_refs = existing_ns_refs;
                for pack in &pack_result.new_packs {
                    let cas_result = st
                        .content_write_bytes(kind, &lid, &pack.bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    updated_refs.push(PackBranchEntry {
                        first_id: pack.first_id,
                        last_id: pack.last_id,
                        pack_cid: cid_from_write(kind, &cas_result),
                    });
                }

                Ok(WorkResult::Dict(DictWorkResult::SubjectForwardPacks {
                    ns_code,
                    result: FwdPackResult {
                        all_pack_refs: updated_refs,
                    },
                }))
            });
        }
    }

    // 4b: Subject reverse tree.
    if !novelty.new_subjects.is_empty() {
        n_dict_items += 1;
        let sem = semaphore.clone();
        let cs = content_store_shared.clone();
        let st = storage_shared.clone();
        let lid = ledger_id_shared.clone();
        let existing_refs = base_root.dict_refs.subject_reverse.clone();
        // Own the entries.
        let new_subjects: Vec<(u16, u64, Vec<u8>)> = novelty.new_subjects.clone();

        join_set.spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|_| IndexerError::StorageWrite("semaphore closed".into()))?;

            let updated = upload_incremental_reverse_tree_async(
                &*st,
                &lid,
                fluree_db_core::DictKind::SubjectReverse,
                &cs,
                &existing_refs,
                new_subjects,
            )
            .await?;

            Ok(WorkResult::Dict(DictWorkResult::SubjectReverseTree(
                updated,
            )))
        });
    }

    // 4b: String reverse tree.
    if !novelty.new_strings.is_empty() {
        n_dict_items += 1;
        let sem = semaphore.clone();
        let cs = content_store_shared.clone();
        let st = storage_shared.clone();
        let lid = ledger_id_shared.clone();
        let existing_refs = base_root.dict_refs.string_reverse.clone();
        let new_strings: Vec<(u32, Vec<u8>)> = novelty.new_strings.clone();

        join_set.spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|_| IndexerError::StorageWrite("semaphore closed".into()))?;

            let updated = upload_incremental_reverse_tree_async_strings(
                &*st,
                &lid,
                fluree_db_core::DictKind::StringReverse,
                &cs,
                &existing_refs,
                new_strings,
            )
            .await?;

            Ok(WorkResult::Dict(DictWorkResult::StringReverseTree(updated)))
        });
    }

    let n_total_items = n_branch_items + n_dict_items;

    // ---- Collect all results and apply deterministically ----

    let mut results: Vec<WorkResult> = Vec::with_capacity(n_total_items);
    while let Some(join_result) = join_set.join_next().await {
        let result = join_result.map_err(|e| {
            IndexerError::StorageWrite(format!("incremental work task panicked: {e}"))
        })??;
        results.push(result);
    }

    // Sort by deterministic key: branches by (g_id, order), then dict items
    // in a fixed canonical order.
    results.sort_by_key(|a| a.sort_key());

    let mut total_new_leaves = 0usize;
    let mut total_replaced_leaves = 0usize;
    let mut new_dict_refs = base_root.dict_refs.clone();

    for result in results {
        match result {
            WorkResult::Branch(b) => {
                total_new_leaves += b.update.new_leaf_blobs.len();
                total_replaced_leaves += b.update.replaced_leaf_cids.len();
                root_builder.add_replaced_cids(b.update.replaced_leaf_cids);

                if b.g_id == 0 {
                    root_builder.set_default_graph_order(b.order, b.update.leaf_entries);
                } else {
                    root_builder.set_named_graph_branch(b.g_id, b.order, b.update.branch_cid);
                    if let Some(old_cid) = b.old_branch_cid {
                        root_builder.add_replaced_cids([old_cid]);
                    }
                }
            }
            WorkResult::Dict(d) => match d {
                DictWorkResult::StringForwardPacks(fwd) => {
                    new_dict_refs.forward_packs.string_fwd_packs = fwd.all_pack_refs;
                }
                DictWorkResult::SubjectForwardPacks { ns_code, result } => {
                    if let Some(entry) = new_dict_refs
                        .forward_packs
                        .subject_fwd_ns_packs
                        .iter_mut()
                        .find(|(ns, _)| *ns == ns_code)
                    {
                        entry.1 = result.all_pack_refs;
                    } else {
                        new_dict_refs
                            .forward_packs
                            .subject_fwd_ns_packs
                            .push((ns_code, result.all_pack_refs));
                    }
                }
                DictWorkResult::SubjectReverseTree(updated) => {
                    root_builder.add_replaced_cids(updated.replaced_cids);
                    new_dict_refs.subject_reverse = updated.tree_refs;
                }
                DictWorkResult::StringReverseTree(updated) => {
                    root_builder.add_replaced_cids(updated.replaced_cids);
                    new_dict_refs.string_reverse = updated.tree_refs;
                }
            },
        }
    }

    tracing::info!(
        graphs = by_graph.len(),
        branch_items = n_branch_items,
        dict_items = n_dict_items,
        max_concurrency = max_concurrency,
        new_leaves = total_new_leaves,
        replaced_leaves = total_replaced_leaves,
        new_strings = novelty.new_strings.len(),
        new_subjects = novelty.new_subjects.len(),
        "Phase 2-4 complete: branch + dict updates"
    );

    // ---- Phase 4.5: Arena updates (numbig + vectors) ----
    //
    // Build updated graph_arenas by starting from the base root's arenas
    // and patching any (g_id, p_id) that have new/extended data.
    {
        use fluree_db_binary_index::format::index_root::{GraphArenaRefsV5, VectorDictRefV5};
        use std::collections::BTreeMap;

        // Index base arenas by g_id for efficient lookup.
        let mut arenas_by_gid: BTreeMap<u16, GraphArenaRefsV5> = BTreeMap::new();
        for ga in &base_root.graph_arenas {
            arenas_by_gid.insert(ga.g_id, ga.clone());
        }

        // Track which (g_id, p_id) have new numbig arenas from the resolver.
        // The resolver's numbigs include pre-seeded arenas (old + new entries)
        // when the base root had numbigs, and fresh arenas when the base root
        // had none. We re-serialize the full arena in both cases because
        // numbig arenas are small (kilobytes).
        let has_new_numbigs = !novelty.shared.numbigs.is_empty();
        let has_new_vectors = !novelty.shared.vectors.is_empty();
        let has_new_spatial = novelty
            .shared
            .spatial_hook
            .as_ref()
            .is_some_and(|h| !h.is_empty());

        if has_new_numbigs || has_new_vectors || has_new_spatial {
            // ---- NumBig arena upload ----
            for (&g_id, per_pred) in &novelty.shared.numbigs {
                let ga = arenas_by_gid
                    .entry(g_id)
                    .or_insert_with(|| GraphArenaRefsV5 {
                        g_id,
                        numbig: Vec::new(),
                        vectors: Vec::new(),
                        spatial: Vec::new(),
                    });

                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }
                    // No-op guard: if arena.len() == base count, no new entries were
                    // added during resolution (the arena was pre-seeded but not extended).
                    // Reuse the existing CID to avoid GC churn.
                    let base_nb_count = novelty
                        .base_numbig_counts
                        .get(&(g_id, p_id))
                        .copied()
                        .unwrap_or(0);
                    if arena.len() == base_nb_count {
                        continue; // Unchanged — existing CID already in ga.numbig
                    }

                    let bytes =
                        fluree_db_binary_index::arena::numbig::write_numbig_arena_to_bytes(arena)
                            .map_err(|e| {
                            IndexerError::StorageWrite(format!("numbig arena serialize: {e}"))
                        })?;
                    let dict_kind = fluree_db_core::DictKind::NumBig { p_id };
                    let (cid, _) = upload_dict_blob(
                        storage,
                        ledger_id,
                        dict_kind,
                        &bytes,
                        "incremental numbig arena uploaded",
                    )
                    .await?;

                    // Replace or insert the (p_id, cid) entry, collecting old CID for GC.
                    if let Some(pos) = ga.numbig.iter().position(|(pid, _)| *pid == p_id) {
                        let old_cid = ga.numbig[pos].1.clone();
                        root_builder.add_replaced_cids([old_cid]);
                        ga.numbig[pos].1 = cid;
                    } else {
                        ga.numbig.push((p_id, cid));
                        ga.numbig.sort_by_key(|(pid, _)| *pid);
                    }
                }
            }

            // ---- Vector arena upload ----
            for (&g_id, per_pred) in &novelty.shared.vectors {
                let ga = arenas_by_gid
                    .entry(g_id)
                    .or_insert_with(|| GraphArenaRefsV5 {
                        g_id,
                        numbig: Vec::new(),
                        vectors: Vec::new(),
                        spatial: Vec::new(),
                    });

                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }

                    // Handle space overflow guard.
                    let base_count = novelty
                        .base_vector_counts
                        .get(&(g_id, p_id))
                        .copied()
                        .unwrap_or(0);
                    if (base_count as u64) + (arena.len() as u64) > u32::MAX as u64 {
                        return Err(IndexerError::IncrementalAbort(format!(
                            "vector handle overflow for g_id={g_id}, p_id={p_id}: \
                             base={base_count} + new={} exceeds u32::MAX",
                            arena.len()
                        )));
                    }

                    if let Some(pos) = ga.vectors.iter().position(|v| v.p_id == p_id) {
                        // Extending an existing vector arena.
                        let existing = &ga.vectors[pos];

                        let old_manifest_bytes =
                            content_store.get(&existing.manifest).await.map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "read existing vector manifest: {e}"
                                ))
                            })?;
                        let old_manifest =
                            fluree_db_binary_index::arena::vector::read_vector_manifest(
                                &old_manifest_bytes,
                            )
                            .map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "decode existing vector manifest: {e}"
                                ))
                            })?;

                        if old_manifest.dims != arena.dims() {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector dims mismatch for g_id={g_id}, p_id={p_id}: existing={}, new={}",
                                old_manifest.dims,
                                arena.dims()
                            )));
                        }
                        if old_manifest.shard_capacity
                            != fluree_db_binary_index::arena::vector::SHARD_CAPACITY
                        {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector shard_capacity mismatch for g_id={g_id}, p_id={p_id}: existing={}, expected={}",
                                old_manifest.shard_capacity,
                                fluree_db_binary_index::arena::vector::SHARD_CAPACITY
                            )));
                        }
                        if old_manifest.normalized != arena.is_normalized() {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector normalization mismatch for g_id={g_id}, p_id={p_id}: existing={}, new={}",
                                old_manifest.normalized,
                                arena.is_normalized()
                            )));
                        }

                        if existing.shards.len() != old_manifest.shards.len() {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector shard list length mismatch for g_id={g_id}, p_id={p_id}: shards={}, manifest={}",
                                existing.shards.len(),
                                old_manifest.shards.len()
                            )));
                        }

                        let shard_cap = old_manifest.shard_capacity;
                        let dims_usize = arena.dims() as usize;

                        let mut combined_shards = existing.shards.clone();
                        let mut combined_shard_infos = old_manifest.shards.clone();

                        // If the existing last shard is partially filled, we must fill/replace it
                        // before appending new shards. Otherwise the handle -> (shard_idx, offset)
                        // arithmetic breaks (partial shard becomes "middle").
                        let mut consumed_new: u32 = 0;
                        if let Some(last_info) = combined_shard_infos.last().cloned() {
                            if last_info.count < shard_cap {
                                let remaining = shard_cap - last_info.count;
                                let take = remaining.min(arena.len());
                                if take > 0 {
                                    let last_idx = combined_shard_infos.len() - 1;
                                    let old_last_cid = combined_shards[last_idx].clone();

                                    let old_last_bytes =
                                        content_store.get(&old_last_cid).await.map_err(|e| {
                                            IndexerError::StorageRead(format!(
                                                "read existing vector last shard: {e}"
                                            ))
                                        })?;
                                    let old_last_shard =
                                        fluree_db_binary_index::arena::vector::read_vector_shard_from_bytes(
                                            &old_last_bytes,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageRead(format!(
                                                "decode existing vector last shard: {e}"
                                            ))
                                        })?;
                                    if old_last_shard.dims != old_manifest.dims
                                        || old_last_shard.count != last_info.count
                                    {
                                        return Err(IndexerError::IncrementalAbort(format!(
                                            "existing vector last shard metadata mismatch for g_id={g_id}, p_id={p_id}: \
                                             shard(dims={}, count={}) manifest(dims={}, count={})",
                                            old_last_shard.dims,
                                            old_last_shard.count,
                                            old_manifest.dims,
                                            last_info.count
                                        )));
                                    }

                                    let take_f32 = take as usize * dims_usize;
                                    let mut merged: Vec<f32> =
                                        Vec::with_capacity(old_last_shard.values.len() + take_f32);
                                    merged.extend_from_slice(&old_last_shard.values);
                                    merged.extend_from_slice(&arena.raw_values()[0..take_f32]);

                                    let shard_bytes =
                                        fluree_db_binary_index::arena::vector::write_vector_shard_to_bytes(
                                            old_manifest.dims,
                                            &merged,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageWrite(format!(
                                                "vector last shard serialize: {e}"
                                            ))
                                        })?;

                                    let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                                    let (new_last_cid, wr) = upload_dict_blob(
                                        storage,
                                        ledger_id,
                                        dict_kind,
                                        &shard_bytes,
                                        "incremental vector last shard replaced",
                                    )
                                    .await?;

                                    // Replace last shard CID + info and collect old CID for GC.
                                    combined_shards[last_idx] = new_last_cid;
                                    combined_shard_infos[last_idx].cas = wr.address;
                                    combined_shard_infos[last_idx].count = last_info.count + take;
                                    root_builder.add_replaced_cids([old_last_cid]);
                                    consumed_new = take;
                                }
                            }
                        }

                        // Serialize remaining new vectors to new shards (may be empty if all were
                        // consumed filling the prior last shard).
                        let start_f32 = consumed_new as usize * dims_usize;
                        let remaining_raw = &arena.raw_values()[start_f32..];
                        let shard_results =
                            fluree_db_binary_index::arena::vector::write_vector_shards_from_raw(
                                arena.dims(),
                                remaining_raw,
                            )
                            .map_err(|e| {
                                IndexerError::StorageWrite(format!("vector shard serialize: {e}"))
                            })?;

                        let mut new_shard_cids = Vec::with_capacity(shard_results.len());
                        let mut new_shard_infos = Vec::with_capacity(shard_results.len());

                        for (shard_bytes, mut shard_info) in shard_results {
                            let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                            let (shard_cid, wr) = upload_dict_blob(
                                storage,
                                ledger_id,
                                dict_kind,
                                &shard_bytes,
                                "incremental vector shard uploaded",
                            )
                            .await?;
                            shard_info.cas = wr.address;
                            new_shard_cids.push(shard_cid);
                            new_shard_infos.push(shard_info);
                        }

                        combined_shards.extend(new_shard_cids);
                        combined_shard_infos.extend(new_shard_infos);

                        let combined_manifest =
                            fluree_db_binary_index::arena::vector::VectorManifest {
                                version: 1,
                                dims: old_manifest.dims,
                                dtype: "f32".to_string(),
                                normalized: old_manifest.normalized,
                                shard_capacity: old_manifest.shard_capacity,
                                total_count: base_count + arena.len(),
                                shards: combined_shard_infos,
                            };

                        let manifest_json =
                            serde_json::to_vec_pretty(&combined_manifest).map_err(|e| {
                                IndexerError::StorageWrite(format!(
                                    "serialize combined vector manifest: {e}"
                                ))
                            })?;

                        let dict_kind = fluree_db_core::DictKind::VectorManifest { p_id };
                        let (manifest_cid, _) = upload_dict_blob(
                            storage,
                            ledger_id,
                            dict_kind,
                            &manifest_json,
                            "incremental vector manifest uploaded",
                        )
                        .await?;

                        // GC the old manifest (old shards are still referenced by combined;
                        // any replaced shard CIDs were added above).
                        root_builder.add_replaced_cids([existing.manifest.clone()]);

                        ga.vectors[pos] = VectorDictRefV5 {
                            p_id,
                            manifest: manifest_cid,
                            shards: combined_shards,
                        };
                    } else {
                        // Brand new vector arena for this (g_id, p_id).
                        if base_count > 0 {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "base vector count exists but no prior vector refs found for g_id={g_id}, p_id={p_id}"
                            )));
                        }

                        let shard_results =
                            fluree_db_binary_index::arena::vector::write_vector_shards_to_bytes(
                                arena,
                            )
                            .map_err(|e| {
                                IndexerError::StorageWrite(format!("vector shard serialize: {e}"))
                            })?;

                        let mut new_shard_cids = Vec::with_capacity(shard_results.len());
                        let mut new_shard_infos = Vec::with_capacity(shard_results.len());

                        for (shard_bytes, mut shard_info) in shard_results {
                            let dict_kind = fluree_db_core::DictKind::VectorShard { p_id };
                            let (shard_cid, wr) = upload_dict_blob(
                                storage,
                                ledger_id,
                                dict_kind,
                                &shard_bytes,
                                "incremental vector shard uploaded",
                            )
                            .await?;
                            shard_info.cas = wr.address;
                            new_shard_cids.push(shard_cid);
                            new_shard_infos.push(shard_info);
                        }

                        let manifest = fluree_db_binary_index::arena::vector::VectorManifest {
                            version: 1,
                            dims: arena.dims(),
                            dtype: "f32".to_string(),
                            normalized: arena.is_normalized(),
                            shard_capacity: fluree_db_binary_index::arena::vector::SHARD_CAPACITY,
                            total_count: arena.len(),
                            shards: new_shard_infos,
                        };

                        let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(|e| {
                            IndexerError::StorageWrite(format!(
                                "serialize new vector manifest: {e}"
                            ))
                        })?;

                        let dict_kind = fluree_db_core::DictKind::VectorManifest { p_id };
                        let (manifest_cid, _) = upload_dict_blob(
                            storage,
                            ledger_id,
                            dict_kind,
                            &manifest_json,
                            "incremental vector manifest uploaded",
                        )
                        .await?;

                        ga.vectors.push(VectorDictRefV5 {
                            p_id,
                            manifest: manifest_cid,
                            shards: new_shard_cids,
                        });
                        ga.vectors.sort_by_key(|v| v.p_id);
                    }
                }
            }

            // ---- Spatial arena rebuild (per affected predicate) ----
            //
            // For each (g_id, p_id) with novelty spatial entries, rebuild the
            // spatial index from scratch: load all prior entries from the existing
            // snapshot, combine with novelty, build + upload a new index.
            // Unchanged spatial arenas carry forward by CID.
            if has_new_spatial {
                use std::collections::BTreeMap;

                let spatial_entries = novelty
                    .shared
                    .spatial_hook
                    .as_ref()
                    .map(|h| h.entries())
                    .unwrap_or(&[]);

                // Group novelty entries by (g_id, p_id).
                let mut grouped: BTreeMap<(u16, u32), Vec<&crate::spatial_hook::SpatialEntry>> =
                    BTreeMap::new();
                for entry in spatial_entries {
                    grouped
                        .entry((entry.g_id, entry.p_id))
                        .or_default()
                        .push(entry);
                }

                for ((g_id, p_id), new_entries) in grouped {
                    let pred_iri = novelty
                        .shared
                        .predicates
                        .resolve(p_id)
                        .unwrap_or("unknown")
                        .to_string();

                    let config = fluree_db_spatial::SpatialCreateConfig::new(
                        format!("spatial:g{}p{}", g_id, p_id),
                        ledger_id.to_string(),
                        pred_iri.clone(),
                    );
                    let mut builder = fluree_db_spatial::SpatialIndexBuilder::new(config);

                    // Load prior entries from the existing spatial snapshot (if any).
                    let ga = arenas_by_gid.get(&g_id);
                    let existing_ref = ga.and_then(|a| a.spatial.iter().find(|s| s.p_id == p_id));
                    let mut prior_count = 0u64;

                    if let Some(sp_ref) = existing_ref {
                        // Load the SpatialIndexRoot + snapshot from CAS.
                        let root_bytes =
                            content_store.get(&sp_ref.root_cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "spatial root load for g{}:p{}: {e}",
                                    g_id, p_id
                                ))
                            })?;
                        let spatial_root: fluree_db_spatial::SpatialIndexRoot =
                            serde_json::from_slice(&root_bytes).map_err(|e| {
                                IndexerError::StorageRead(format!(
                                    "spatial root decode for g{}:p{}: {e}",
                                    g_id, p_id
                                ))
                            })?;

                        // Pre-fetch all blobs for the sync load_from_cas closure.
                        let mut blob_cache: std::collections::HashMap<String, Vec<u8>> =
                            std::collections::HashMap::new();
                        // Manifest + arena
                        for cid in [&sp_ref.manifest, &sp_ref.arena] {
                            let bytes = content_store.get(cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!("spatial blob fetch: {e}"))
                            })?;
                            blob_cache.insert(cid.digest_hex(), bytes);
                        }
                        // Leaflets
                        for leaflet_cid in &sp_ref.leaflets {
                            let bytes = content_store.get(leaflet_cid).await.map_err(|e| {
                                IndexerError::StorageRead(format!("spatial leaflet fetch: {e}"))
                            })?;
                            blob_cache.insert(leaflet_cid.digest_hex(), bytes);
                        }

                        let cache_arc = std::sync::Arc::new(blob_cache);
                        match fluree_db_spatial::SpatialIndexSnapshot::load_from_cas(
                            spatial_root,
                            move |hash| {
                                cache_arc.get(hash).cloned().ok_or_else(|| {
                                    fluree_db_spatial::error::SpatialError::ChunkNotFound(
                                        hash.to_string(),
                                    )
                                })
                            },
                        ) {
                            Ok(snapshot) => {
                                // Scan all existing entries and feed into builder.
                                let all_entries = snapshot
                                    .cell_index()
                                    .scan_range(0, u64::MAX)
                                    .unwrap_or_default();
                                for ce in &all_entries {
                                    if let Some(arena_entry) = snapshot.arena().get(ce.geo_handle) {
                                        if let Ok(wkt_str) = std::str::from_utf8(&arena_entry.wkt) {
                                            let _ = builder.add_geometry(
                                                ce.subject_id,
                                                wkt_str,
                                                ce.t,
                                                ce.is_assert(),
                                            );
                                            prior_count += 1;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(IndexerError::IncrementalAbort(format!(
                                    "spatial snapshot load failed for g_id={g_id}, p_id={p_id}: {e}; \
                                     falling back to full rebuild for correctness"
                                )));
                            }
                        }
                    }

                    // Feed novelty entries.
                    for entry in &new_entries {
                        let _ = builder.add_geometry(
                            entry.subject_id,
                            &entry.wkt,
                            entry.t,
                            entry.is_assert,
                        );
                    }

                    let build_result = builder.build().map_err(|e| {
                        IndexerError::Other(format!("spatial build g{}:p{}: {e}", g_id, p_id))
                    })?;

                    if build_result.entries.is_empty() {
                        continue;
                    }

                    // Upload via the same two-phase pattern as the full build.
                    let mut pending_blobs: Vec<(String, Vec<u8>)> = Vec::new();
                    let write_result = build_result
                        .write_to_cas(|bytes| {
                            use sha2::{Digest, Sha256};
                            let hash_hex = hex::encode(Sha256::digest(bytes));
                            pending_blobs.push((hash_hex.clone(), bytes.to_vec()));
                            Ok(hash_hex)
                        })
                        .map_err(|e| IndexerError::Other(format!("spatial CAS build: {e}")))?;

                    for (_hash, blob_bytes) in &pending_blobs {
                        storage
                            .content_write_bytes(ContentKind::SpatialIndex, ledger_id, blob_bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    }

                    // Build CIDs.
                    let spatial_codec = ContentKind::SpatialIndex.to_codec();
                    let root_json = serde_json::to_vec(&write_result.root)
                        .map_err(|e| IndexerError::Other(format!("spatial root serialize: {e}")))?;
                    let root_cas = storage
                        .content_write_bytes(ContentKind::SpatialIndex, ledger_id, &root_json)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let root_cid =
                        ContentId::from_hex_digest(spatial_codec, &root_cas.content_hash)
                            .ok_or_else(|| {
                                IndexerError::StorageWrite(format!(
                                    "invalid spatial root hash for g_id={g_id}, p_id={p_id}: {}",
                                    root_cas.content_hash
                                ))
                            })?;
                    let manifest_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.manifest_address)
                            .ok_or_else(|| {
                            IndexerError::StorageWrite(format!(
                                "invalid spatial manifest hash for g_id={g_id}, p_id={p_id}: {}",
                                write_result.manifest_address
                            ))
                        })?;
                    let arena_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.arena_address)
                            .ok_or_else(|| {
                                IndexerError::StorageWrite(format!(
                                    "invalid spatial arena hash for g_id={g_id}, p_id={p_id}: {}",
                                    write_result.arena_address
                                ))
                            })?;
                    let leaflet_cids: Vec<ContentId> = write_result
                        .leaflet_addresses
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            ContentId::from_hex_digest(spatial_codec, h).ok_or_else(|| {
                                IndexerError::StorageWrite(format!(
                                    "invalid spatial leaflet hash [{i}] for g_id={g_id}, p_id={p_id}: {h}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let new_ref = SpatialArenaRefV5 {
                        p_id,
                        root_cid,
                        manifest: manifest_cid,
                        arena: arena_cid,
                        leaflets: leaflet_cids,
                    };

                    // Replace or insert in graph arenas.
                    let ga = arenas_by_gid
                        .entry(g_id)
                        .or_insert_with(|| GraphArenaRefsV5 {
                            g_id,
                            numbig: Vec::new(),
                            vectors: Vec::new(),
                            spatial: Vec::new(),
                        });

                    if let Some(pos) = ga.spatial.iter().position(|s| s.p_id == p_id) {
                        // GC old spatial CIDs.
                        let old = &ga.spatial[pos];
                        root_builder.add_replaced_cids([
                            old.root_cid.clone(),
                            old.manifest.clone(),
                            old.arena.clone(),
                        ]);
                        root_builder.add_replaced_cids(old.leaflets.clone());
                        ga.spatial[pos] = new_ref;
                    } else {
                        ga.spatial.push(new_ref);
                        ga.spatial.sort_by_key(|s| s.p_id);
                    }

                    tracing::info!(
                        g_id,
                        p_id,
                        predicate = %pred_iri,
                        prior_entries = prior_count,
                        novelty_entries = new_entries.len(),
                        "spatial index rebuilt for (graph, predicate)"
                    );
                }
            }

            let updated_arenas: Vec<GraphArenaRefsV5> = arenas_by_gid.into_values().collect();
            root_builder.set_graph_arenas(updated_arenas);

            tracing::info!("Phase 4.5 complete: arena updates (numbig + vectors + spatial)");
        }
    }

    // ---- Phase 4.6: Incremental stats refresh ----
    //
    // Build predicate SIDs first (needed for stats p_id → SID conversion).
    let new_ns_codes: std::collections::BTreeMap<u16, String> = novelty
        .shared
        .ns_prefixes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    let new_pred_sids = build_predicate_sids(&novelty.shared, &new_ns_codes);

    {
        use crate::stats::{
            load_sketch_blob, value_hash, GraphPropertyKey, HllSketchBlob, IdPropertyHll,
            IdStatsHook, StatsRecord,
        };
        use fluree_db_core::value_id::ValueTypeTag;
        use std::collections::{HashMap, HashSet};

        // 4.6a: Load prior HLL sketches from CAS.
        //
        // If the base root has stats but we cannot load sketches, we must NOT
        // "start fresh" (delta-only), because that would produce inconsistent
        // totals (base per-graph flakes + delta-only per-property counts/NDV).
        //
        // Instead, we fall back to carrying forward base stats unchanged
        // (optionally applying class-count deltas below).
        let mut can_refresh_hll = base_root.stats.is_some();
        let prior_properties: HashMap<GraphPropertyKey, IdPropertyHll> = if let Some(ref cid) =
            base_root.sketch_ref
        {
            match load_sketch_blob(content_store.as_ref(), cid).await {
                Ok(Some(blob)) => {
                    match blob.into_properties() {
                        Ok(props) => props,
                        Err(e) => {
                            tracing::warn!("failed to decode prior sketch blob: {e}; carrying forward base stats");
                            can_refresh_hll = false;
                            HashMap::new()
                        }
                    }
                }
                Ok(None) => {
                    tracing::warn!("prior sketch blob not found; carrying forward base stats");
                    can_refresh_hll = false;
                    HashMap::new()
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to load prior sketch blob: {e}; carrying forward base stats"
                    );
                    can_refresh_hll = false;
                    HashMap::new()
                }
            }
        } else {
            tracing::warn!("base root has no sketch_ref; carrying forward base stats");
            can_refresh_hll = false;
            HashMap::new()
        };

        // 4.6b: Seed IdStatsHook with priors + graph_flakes from base root.
        let rdf_type_p_id = novelty.shared.predicates.get(fluree_vocab::rdf::TYPE);

        let mut stats_hook = IdStatsHook::with_prior_properties(prior_properties);
        if let Some(pid) = rdf_type_p_id {
            stats_hook.set_rdf_type_p_id(pid);
        }
        // Enable ref-target tracking in incremental path.
        // The batched PSOT lookup for ref object SIDs provides the class context
        // needed to compute class→property→ref-class edges incrementally.
        stats_hook.set_track_ref_targets(true);

        // Seed per-graph flake totals from base root so finalize produces
        // base+delta, not delta-only.
        if let Some(ref stats) = base_root.stats {
            if let Some(ref graphs) = stats.graphs {
                for g in graphs {
                    *stats_hook.graph_flakes_mut().entry(g.g_id).or_insert(0) += g.flakes as i64;
                }
            } else {
                // Fallback for older roots that only carry sketches (no per-graph stats):
                // derive base per-graph flake totals from prior per-(g_id, p_id) counts.
                let mut derived: HashMap<u16, i64> = HashMap::new();
                for (k, hll) in stats_hook.properties() {
                    *derived.entry(k.g_id).or_insert(0) += hll.count.max(0);
                }
                for (g_id, flakes) in derived {
                    *stats_hook.graph_flakes_mut().entry(g_id).or_insert(0) += flakes;
                }
            }
        }

        // 4.6c: Feed novelty records into the hook.
        for record in &novelty.records {
            let dt = novelty
                .shared
                .dt_tags
                .get(record.dt as usize)
                .copied()
                .unwrap_or(ValueTypeTag::UNKNOWN);
            stats_hook.on_record(&StatsRecord {
                g_id: record.g_id,
                p_id: record.p_id,
                s_id: record.s_id.as_u64(),
                dt,
                o_hash: value_hash(record.o_kind, record.o_key),
                o_kind: record.o_kind,
                o_key: record.o_key,
                t: record.t as i64,
                op: record.op != 0,
                lang_id: record.lang_id,
            });
        }

        // 4.6d: Class-property attribution via batched PSOT lookup.
        //
        // Replaces the simpler "carry forward + count deltas" approach with full
        // class-property attribution: for each class, we compute both instance
        // counts and which properties appear on instances of that class.
        //
        // Strategy:
        //   1. Capture subject_class_deltas and subject_props from the hook
        //      BEFORE finalize consumes them.
        //   2. Load a BinaryIndexStore from the base root for PSOT scans.
        //   3. Batched PSOT lookup for base class memberships of novelty subjects.
        //   4. Merge base classes + novelty rdf:type deltas -> subject->classes.
        //   5. Cross-reference with subject->properties -> class->properties.
        //   6. Merge with prior class stats and convert to ClassStatEntry.
        let class_deltas: HashMap<(u16, u64), i64> = stats_hook.class_count_deltas().clone();
        let novelty_subject_class_deltas: HashMap<(u16, u64), HashMap<u64, i64>> =
            stats_hook.subject_class_deltas().clone();
        let novelty_subject_props: HashMap<(u16, u64), HashSet<u32>> =
            stats_hook.subject_props().clone();
        let novelty_subject_prop_dts: HashMap<(u16, u64), HashMap<u32, HashMap<u8, i64>>> =
            stats_hook.subject_prop_dts().clone();
        let novelty_subject_prop_langs: HashMap<(u16, u64), HashMap<u32, HashMap<u16, i64>>> =
            stats_hook.subject_prop_langs().clone();

        // Build language tag list for resolving lang_id → string in class stats.
        let incr_language_tags: Vec<String> = novelty
            .shared
            .languages
            .iter()
            .map(|(_, tag)| tag.to_string())
            .collect();

        // per_graph_classes: HashMap<GraphId, Vec<ClassStatEntry>> — graph-scoped class stats
        // root_classes: Option<Vec<ClassStatEntry>> — union for backward compat
        let (per_graph_classes, root_classes) = {
            use fluree_db_binary_index::dict::reverse_leaf::subject_reverse_key;
            use fluree_db_binary_index::read::batched_lookup::batched_lookup_predicate_refs;

            let base_classes = base_root.stats.as_ref().and_then(|s| s.classes.as_ref());
            // Also check per-graph classes from base root
            let base_per_graph_classes: HashMap<u16, &Vec<fluree_db_core::ClassStatEntry>> =
                base_root
                    .stats
                    .as_ref()
                    .and_then(|s| s.graphs.as_ref())
                    .map(|gs| {
                        gs.iter()
                            .filter_map(|g| g.classes.as_ref().map(|c| (g.g_id, c)))
                            .collect()
                    })
                    .unwrap_or_default();

            let has_novelty_class_changes =
                !class_deltas.is_empty() || !novelty_subject_class_deltas.is_empty();

            if !has_novelty_class_changes && novelty_subject_props.is_empty() {
                // No class or property changes in novelty — carry forward base stats.
                let pgc: HashMap<u16, Vec<fluree_db_core::ClassStatEntry>> = base_per_graph_classes
                    .into_iter()
                    .map(|(g_id, v)| (g_id, v.clone()))
                    .collect();
                (pgc, base_classes.cloned())
            } else {
                // Load the subject reverse tree for Sid<->sid64 conversion.
                let subject_tree = run_index::incremental_resolve::load_reverse_tree(
                    &content_store,
                    &base_root.dict_refs.subject_reverse,
                )
                .await
                .map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "load subject reverse tree for class stats: {e}"
                    ))
                })?;

                // Build (g_id, sid64) -> ClassStatEntry map from base per-graph class stats.
                // Fall back to root-level base_classes with g_id=0 if no per-graph data.
                let mut entries_by_key: HashMap<(u16, u64), fluree_db_core::ClassStatEntry> =
                    HashMap::new();

                if !base_per_graph_classes.is_empty() {
                    for (&g_id, class_entries) in &base_per_graph_classes {
                        for entry in *class_entries {
                            let key = subject_reverse_key(
                                entry.class_sid.namespace_code,
                                entry.class_sid.name.as_bytes(),
                            );
                            if let Ok(Some(sid64)) = subject_tree.reverse_lookup(&key) {
                                entries_by_key.insert((g_id, sid64), entry.clone());
                            } else {
                                entries_by_key.insert(
                                    (g_id, u64::MAX - entries_by_key.len() as u64),
                                    entry.clone(),
                                );
                            }
                        }
                    }
                } else if let Some(base_cls) = base_classes {
                    // Legacy: root-level classes only, assign to g_id=0
                    for entry in base_cls {
                        let key = subject_reverse_key(
                            entry.class_sid.namespace_code,
                            entry.class_sid.name.as_bytes(),
                        );
                        if let Ok(Some(sid64)) = subject_tree.reverse_lookup(&key) {
                            entries_by_key.insert((0, sid64), entry.clone());
                        } else {
                            entries_by_key
                                .insert((0, u64::MAX - entries_by_key.len() as u64), entry.clone());
                        }
                    }
                }

                // Apply graph-scoped class count deltas to existing entries.
                for (&(g_id, sid64), &delta) in &class_deltas {
                    if let Some(entry) = entries_by_key.get_mut(&(g_id, sid64)) {
                        entry.count = (entry.count as i64 + delta).max(0) as u64;
                    } else if delta > 0 {
                        // New class not in base stats — create a placeholder entry.
                        entries_by_key.insert(
                            (g_id, sid64),
                            fluree_db_core::ClassStatEntry {
                                class_sid: fluree_db_core::sid::Sid::new(0, ""),
                                count: delta as u64,
                                properties: Vec::new(),
                            },
                        );
                    }
                }

                // ---- Batched PSOT lookup for base class memberships (graph-scoped) ----
                let mut subject_classes: HashMap<(u16, u64), HashSet<u64>> = HashMap::new();

                if let Some(rdf_type_pid) = rdf_type_p_id {
                    // Collect distinct novelty subjects that might have class memberships.
                    let novelty_s_ids: Vec<u64> = {
                        let mut ids: Vec<u64> =
                            novelty.records.iter().map(|r| r.s_id.as_u64()).collect();
                        ids.sort_unstable();
                        ids.dedup();
                        ids
                    };

                    // Collect ref object SIDs for batched class lookup (needed for ref-class edges).
                    let ref_object_sids: Vec<u64> = {
                        let rdf_type_pid_val = rdf_type_pid;
                        let mut ids: Vec<u64> = novelty
                            .records
                            .iter()
                            .filter(|r| {
                                r.o_kind == fluree_db_core::value_id::ObjKind::REF_ID.as_u8()
                                    && r.p_id != rdf_type_pid_val
                            })
                            .map(|r| r.o_key)
                            .collect();
                        ids.sort_unstable();
                        ids.dedup();
                        ids
                    };

                    if !novelty_s_ids.is_empty() || !ref_object_sids.is_empty() {
                        // Load BinaryIndexStore from base root for PSOT scan.
                        let cache_dir = config
                            .data_dir
                            .as_deref()
                            .map(|d| d.join("_class_cache"))
                            .unwrap_or_else(|| std::env::temp_dir().join("fluree-class-cache"));

                        // Map of new subject IDs to suffix strings for Sid resolution.
                        let new_subject_suffix: HashMap<(u16, u64), String> = novelty
                            .new_subjects
                            .iter()
                            .filter_map(|(ns_code, local_id, suffix)| {
                                let s = std::str::from_utf8(suffix).ok()?.to_string();
                                Some(((*ns_code, *local_id), s))
                            })
                            .collect();

                        match BinaryIndexStore::load_from_root_v5(
                            content_store.clone(),
                            base_root,
                            &cache_dir,
                            None,
                        )
                        .await
                        {
                            Ok(store) => {
                                let store = Arc::new(store);
                                let mut graphs_to_scan: Vec<u16> = Vec::new();
                                graphs_to_scan.push(0);
                                for ng in &base_root.named_graphs {
                                    if ng.g_id != 1 {
                                        graphs_to_scan.push(ng.g_id);
                                    }
                                }
                                graphs_to_scan.sort_unstable();
                                graphs_to_scan.dedup();

                                // Batched PSOT lookup for novelty subjects (graph-scoped).
                                for scan_g_id in &graphs_to_scan {
                                    if !novelty_s_ids.is_empty() {
                                        match batched_lookup_predicate_refs(
                                            &store,
                                            *scan_g_id,
                                            rdf_type_pid,
                                            &novelty_s_ids,
                                            base_root.index_t,
                                        ) {
                                            Ok(base_map) => {
                                                for (s_id, classes) in base_map {
                                                    subject_classes
                                                        .entry((*scan_g_id, s_id))
                                                        .or_default()
                                                        .extend(classes);
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    g_id = scan_g_id,
                                                    %e,
                                                    "batched PSOT class lookup failed for graph; continuing"
                                                );
                                            }
                                        }
                                    }

                                    // Batched PSOT lookup for ref object SIDs (for ref-class edges).
                                    if !ref_object_sids.is_empty() {
                                        match batched_lookup_predicate_refs(
                                            &store,
                                            *scan_g_id,
                                            rdf_type_pid,
                                            &ref_object_sids,
                                            base_root.index_t,
                                        ) {
                                            Ok(ref_map) => {
                                                for (s_id, classes) in ref_map {
                                                    subject_classes
                                                        .entry((*scan_g_id, s_id))
                                                        .or_default()
                                                        .extend(classes);
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    g_id = scan_g_id,
                                                    %e,
                                                    "batched PSOT ref-object class lookup failed; continuing"
                                                );
                                            }
                                        }
                                    }
                                }
                                tracing::debug!(
                                    subjects = novelty_s_ids.len(),
                                    ref_objects = ref_object_sids.len(),
                                    classes_found = subject_classes.len(),
                                    "batched PSOT class lookup complete (graph-scoped)"
                                );

                                // Resolve Sids for new class entries using the loaded store.
                                for (&(_g_id, sid64), entry) in entries_by_key.iter_mut() {
                                    if entry.class_sid.name.is_empty()
                                        && entry.class_sid.namespace_code == 0
                                    {
                                        let sid =
                                            fluree_db_core::subject_id::SubjectId::from_u64(sid64);
                                        let ns_code = sid.ns_code();
                                        let local_id = sid.local_id();
                                        if let Some(suffix) =
                                            new_subject_suffix.get(&(ns_code, local_id))
                                        {
                                            entry.class_sid =
                                                fluree_db_core::sid::Sid::new(ns_code, suffix);
                                        } else {
                                            match store.resolve_subject_iri(sid64) {
                                                Ok(iri) => {
                                                    entry.class_sid = store.encode_iri(&iri);
                                                }
                                                Err(e) => {
                                                    tracing::trace!(
                                                        sid64,
                                                        %e,
                                                        "failed to resolve class IRI"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    %e,
                                    "failed to load BinaryIndexStore for class lookup; \
                                     using novelty-only class data"
                                );
                            }
                        }
                    }

                    // Apply novelty rdf:type deltas on top of base memberships (graph-scoped).
                    for (&(g_id, subj_sid64), class_map) in &novelty_subject_class_deltas {
                        let classes = subject_classes.entry((g_id, subj_sid64)).or_default();
                        for (&class_sid64, &delta) in class_map {
                            if delta > 0 {
                                classes.insert(class_sid64);
                            } else {
                                classes.remove(&class_sid64);
                            }
                        }
                    }
                }

                // ---- Class-property attribution (graph-scoped) ----
                let mut class_properties: HashMap<(u16, u64), HashSet<u32>> = HashMap::new();

                // Reverse lookup for property Sid -> p_id.
                let pred_sid_to_pid: HashMap<(u16, String), u32> = new_pred_sids
                    .iter()
                    .enumerate()
                    .map(|(p_id, (ns_code, suffix))| ((*ns_code, suffix.clone()), p_id as u32))
                    .collect();

                // Seed from prior per-graph class stats.
                for (&(g_id, _sid64), entry) in &entries_by_key {
                    if !entry.properties.is_empty() {
                        for prop_usage in &entry.properties {
                            let key = (
                                prop_usage.property_sid.namespace_code,
                                prop_usage.property_sid.name.to_string(),
                            );
                            if let Some(&p_id) = pred_sid_to_pid.get(&key) {
                                // Resolve the class's sid64 from entry
                                let class_key = subject_reverse_key(
                                    entry.class_sid.namespace_code,
                                    entry.class_sid.name.as_bytes(),
                                );
                                if let Ok(Some(class_sid64)) =
                                    subject_tree.reverse_lookup(&class_key)
                                {
                                    class_properties
                                        .entry((g_id, class_sid64))
                                        .or_default()
                                        .insert(p_id);
                                }
                            }
                        }
                    }
                }

                // Attribute novelty properties to classes (graph-scoped).
                for (&(g_id, subj_sid64), props) in &novelty_subject_props {
                    if let Some(classes) = subject_classes.get(&(g_id, subj_sid64)) {
                        for &class_sid64 in classes {
                            class_properties
                                .entry((g_id, class_sid64))
                                .or_default()
                                .extend(props.iter().copied());
                        }
                    }
                }

                // ---- Compute ref edges (graph-scoped) ----
                let mut ref_edges: HashMap<(u16, u64), HashMap<u32, HashMap<u64, i64>>> =
                    HashMap::new();
                for (&(g_id, subj), per_prop) in stats_hook.subject_ref_history() {
                    let Some(subj_classes) = subject_classes.get(&(g_id, subj)) else {
                        continue;
                    };
                    for (&p_id, objs) in per_prop {
                        for (&obj, &delta) in objs {
                            if delta == 0 {
                                continue;
                            }
                            let Some(obj_classes) = subject_classes.get(&(g_id, obj)) else {
                                continue;
                            };
                            for &sc in subj_classes {
                                for &oc in obj_classes {
                                    *ref_edges
                                        .entry((g_id, sc))
                                        .or_default()
                                        .entry(p_id)
                                        .or_default()
                                        .entry(oc)
                                        .or_insert(0) += delta;
                                }
                            }
                        }
                    }
                }

                // ---- Compute class-scoped datatype/lang deltas (graph-scoped) ----
                // Attribute per-subject dt/lang observations to their classes
                // using the PSOT-built subject_classes map (base + novelty).
                let mut class_prop_dt_deltas: HashMap<(u16, u64), HashMap<u32, HashMap<u8, i64>>> =
                    HashMap::new();
                for (&(g_id, subj), per_prop) in &novelty_subject_prop_dts {
                    let Some(subj_classes) = subject_classes.get(&(g_id, subj)) else {
                        continue;
                    };
                    for (&p_id, dt_map) in per_prop {
                        for (&dt_tag, &delta) in dt_map {
                            if delta == 0 {
                                continue;
                            }
                            for &sc in subj_classes {
                                *class_prop_dt_deltas
                                    .entry((g_id, sc))
                                    .or_default()
                                    .entry(p_id)
                                    .or_default()
                                    .entry(dt_tag)
                                    .or_insert(0) += delta;
                            }
                        }
                    }
                }

                let mut class_prop_lang_deltas: HashMap<
                    (u16, u64),
                    HashMap<u32, HashMap<u16, i64>>,
                > = HashMap::new();
                for (&(g_id, subj), per_prop) in &novelty_subject_prop_langs {
                    let Some(subj_classes) = subject_classes.get(&(g_id, subj)) else {
                        continue;
                    };
                    for (&p_id, lang_map) in per_prop {
                        for (&lang_id, &delta) in lang_map {
                            if delta == 0 {
                                continue;
                            }
                            for &sc in subj_classes {
                                *class_prop_lang_deltas
                                    .entry((g_id, sc))
                                    .or_default()
                                    .entry(p_id)
                                    .or_default()
                                    .entry(lang_id)
                                    .or_insert(0) += delta;
                            }
                        }
                    }
                }

                // ---- Build per-graph ClassStatEntry lists ----

                // Pre-build sid64 → Sid lookup map from all known entries.
                let sid64_to_sid: HashMap<u64, fluree_db_core::sid::Sid> = entries_by_key
                    .iter()
                    .filter(|(_, e)| !e.class_sid.name.is_empty())
                    .map(|(&(_g, sid64), e)| (sid64, e.class_sid.clone()))
                    .collect();

                // Pre-extract prior ref_classes, datatypes, and langs keyed by (g_id, class_sid64, p_id).
                let mut prior_ref_counts: HashMap<(u16, u64, u32), Vec<(u64, i64)>> =
                    HashMap::new();
                let mut prior_dt_counts: HashMap<(u16, u64, u32), Vec<(u8, i64)>> = HashMap::new();
                let mut prior_lang_counts: HashMap<(u16, u64, u32), Vec<(String, i64)>> =
                    HashMap::new();
                for (&(g_id, _class_sid64), entry) in &entries_by_key {
                    let class_key = subject_reverse_key(
                        entry.class_sid.namespace_code,
                        entry.class_sid.name.as_bytes(),
                    );
                    let class_sid64 = subject_tree
                        .reverse_lookup(&class_key)
                        .ok()
                        .flatten()
                        .unwrap_or(_class_sid64);
                    for pu in &entry.properties {
                        if let Some(&p_id) = pred_sid_to_pid.get(&(
                            pu.property_sid.namespace_code,
                            pu.property_sid.name.to_string(),
                        )) {
                            // Prior ref_classes
                            let prior = prior_ref_counts
                                .entry((g_id, class_sid64, p_id))
                                .or_default();
                            for rc in &pu.ref_classes {
                                let rc_key = subject_reverse_key(
                                    rc.class_sid.namespace_code,
                                    rc.class_sid.name.as_bytes(),
                                );
                                if let Ok(Some(rc_sid64)) = subject_tree.reverse_lookup(&rc_key) {
                                    prior.push((rc_sid64, rc.count as i64));
                                }
                            }
                            // Prior datatypes
                            if !pu.datatypes.is_empty() {
                                let prior_dts = prior_dt_counts
                                    .entry((g_id, class_sid64, p_id))
                                    .or_default();
                                for &(tag, count) in &pu.datatypes {
                                    prior_dts.push((tag, count as i64));
                                }
                            }
                            // Prior langs
                            if !pu.langs.is_empty() {
                                let prior_langs = prior_lang_counts
                                    .entry((g_id, class_sid64, p_id))
                                    .or_default();
                                for (lang, count) in &pu.langs {
                                    prior_langs.push((lang.clone(), *count as i64));
                                }
                            }
                        }
                    }
                }

                // Merge class_properties and ref_edges into entries_by_key.
                for (&(g_id, class_sid64), prop_ids) in &class_properties {
                    let entry = entries_by_key
                        .entry((g_id, class_sid64))
                        .or_insert_with(|| fluree_db_core::ClassStatEntry {
                            class_sid: fluree_db_core::sid::Sid::new(0, ""),
                            count: 0,
                            properties: Vec::new(),
                        });

                    let mut props: Vec<fluree_db_core::ClassPropertyUsage> = Vec::new();
                    for &p_id in prop_ids {
                        if let Some((ns_code, suffix)) = new_pred_sids.get(p_id as usize) {
                            let mut ref_counts: HashMap<u64, i64> = HashMap::new();

                            // Seed from prior ref_classes.
                            if let Some(prior) = prior_ref_counts.get(&(g_id, class_sid64, p_id)) {
                                for &(target_sid64, count) in prior {
                                    *ref_counts.entry(target_sid64).or_insert(0) += count;
                                }
                            }

                            // Apply ref edge deltas.
                            if let Some(prop_edges) = ref_edges
                                .get(&(g_id, class_sid64))
                                .and_then(|m| m.get(&p_id))
                            {
                                for (&target_class, &delta) in prop_edges {
                                    *ref_counts.entry(target_class).or_insert(0) += delta;
                                }
                            }

                            // Convert to ClassRefCount using the pre-built sid64→Sid map.
                            let mut ref_classes: Vec<fluree_db_core::ClassRefCount> = ref_counts
                                .into_iter()
                                .filter(|(_, count)| *count > 0)
                                .filter_map(|(target_sid64, count)| {
                                    sid64_to_sid.get(&target_sid64).map(|sid| {
                                        fluree_db_core::ClassRefCount {
                                            class_sid: sid.clone(),
                                            count: count as u64,
                                        }
                                    })
                                })
                                .collect();
                            ref_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

                            // Build datatypes: merge prior + novelty deltas, clamp to >= 0.
                            let mut dt_accum: HashMap<u8, i64> = HashMap::new();
                            if let Some(prior_dts) = prior_dt_counts.get(&(g_id, class_sid64, p_id))
                            {
                                for &(tag, count) in prior_dts {
                                    *dt_accum.entry(tag).or_insert(0) += count;
                                }
                            }
                            if let Some(dt_deltas) = class_prop_dt_deltas
                                .get(&(g_id, class_sid64))
                                .and_then(|m| m.get(&p_id))
                            {
                                for (&tag, &delta) in dt_deltas {
                                    *dt_accum.entry(tag).or_insert(0) += delta;
                                }
                            }
                            let mut datatypes: Vec<(u8, u64)> = dt_accum
                                .into_iter()
                                .filter(|(_, count)| *count > 0)
                                .map(|(tag, count)| (tag, count as u64))
                                .collect();
                            datatypes.sort_by_key(|d| d.0);

                            // Build langs: merge prior + novelty deltas, resolve lang_id → string, clamp to >= 0.
                            let mut lang_accum: HashMap<String, i64> = HashMap::new();
                            if let Some(prior_langs) =
                                prior_lang_counts.get(&(g_id, class_sid64, p_id))
                            {
                                for (lang, count) in prior_langs {
                                    *lang_accum.entry(lang.clone()).or_insert(0) += count;
                                }
                            }
                            if let Some(lang_deltas) = class_prop_lang_deltas
                                .get(&(g_id, class_sid64))
                                .and_then(|m| m.get(&p_id))
                            {
                                for (&lang_id, &delta) in lang_deltas {
                                    // Resolve lang_id (1-indexed) to string.
                                    if let Some(lang_str) =
                                        incr_language_tags.get((lang_id as usize).wrapping_sub(1))
                                    {
                                        *lang_accum.entry(lang_str.clone()).or_insert(0) += delta;
                                    }
                                }
                            }
                            let mut langs: Vec<(String, u64)> = lang_accum
                                .into_iter()
                                .filter(|(_, count)| *count > 0)
                                .map(|(lang, count)| (lang, count as u64))
                                .collect();
                            langs.sort_by(|a, b| a.0.cmp(&b.0));

                            props.push(fluree_db_core::ClassPropertyUsage {
                                property_sid: fluree_db_core::sid::Sid::new(*ns_code, suffix),
                                datatypes,
                                langs,
                                ref_classes,
                            });
                        }
                    }
                    props.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
                    entry.properties = props;
                }

                // Group entries_by_key into per-graph maps.
                let mut per_graph: HashMap<u16, Vec<fluree_db_core::ClassStatEntry>> =
                    HashMap::new();
                for ((g_id, _sid64), entry) in entries_by_key {
                    if (entry.count > 0 || !entry.properties.is_empty())
                        && !entry.class_sid.name.is_empty()
                    {
                        per_graph.entry(g_id).or_default().push(entry);
                    }
                }
                // Sort each graph's classes for determinism.
                for entries in per_graph.values_mut() {
                    entries.sort_by(|a, b| {
                        a.class_sid
                            .namespace_code
                            .cmp(&b.class_sid.namespace_code)
                            .then_with(|| a.class_sid.name.cmp(&b.class_sid.name))
                    });
                }

                // Derive root-level classes as union across graphs for backward compat.
                let slices: Vec<&[fluree_db_core::ClassStatEntry]> =
                    per_graph.values().map(|v| v.as_slice()).collect();
                let root_classes = fluree_db_core::index_stats::union_class_stat_slices(&slices);

                (per_graph, root_classes)
            }
        };

        // If we can't refresh HLL, carry forward base stats (but with updated class counts).
        if !can_refresh_hll {
            if let Some(mut base_stats) = base_root.stats.clone() {
                base_stats.classes = root_classes.clone();
                // Also attach per-graph classes to graph entries.
                if let Some(ref mut graphs) = base_stats.graphs {
                    for g in graphs.iter_mut() {
                        g.classes = per_graph_classes.get(&g.g_id).cloned();
                    }
                }
                root_builder.set_stats(base_stats);
            }
            // Preserve sketch_ref (don't overwrite or GC).
            root_builder.set_sketch_ref(base_root.sketch_ref.clone());
            tracing::info!("Phase 4.6 complete: sketches unavailable; carried forward base stats");
            // Skip the HLL refresh path.
            // (Remaining root assembly continues below.)
            //
            // NOTE: We intentionally do NOT try to "rebuild" HLL from base stats,
            // because NDV estimates are not reversible from the stored aggregates.
        } else {
            let sketch_blob =
                HllSketchBlob::from_properties(novelty.max_t, stats_hook.properties());
            let sketch_ref = if !sketch_blob.entries.is_empty() {
                let sketch_bytes = sketch_blob
                    .to_json_bytes()
                    .map_err(|e| IndexerError::StorageWrite(format!("sketch serialize: {e}")))?;
                let sketch_wr = storage
                    .content_write_bytes(ContentKind::StatsSketch, ledger_id, &sketch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                let cid = cid_from_write(ContentKind::StatsSketch, &sketch_wr);
                tracing::debug!(
                    %cid,
                    bytes = sketch_wr.size_bytes,
                    entries = sketch_blob.entries.len(),
                    "incremental HLL sketch blob uploaded"
                );
                if let Some(old) = &base_root.sketch_ref {
                    if old != &cid {
                        root_builder.add_replaced_cids([old.clone()]);
                    }
                }
                Some(cid)
            } else {
                base_root.sketch_ref.clone()
            };

            // 4.6e: Finalize and build IndexStats.
            let (id_result, agg_props, _class_counts, _class_properties, _class_ref_targets) =
                stats_hook.finalize_with_aggregate_properties();

            let graphs: Vec<fluree_db_core::GraphStatsEntry> = {
                let base_sizes: HashMap<u16, u64> = base_root
                    .stats
                    .as_ref()
                    .and_then(|s| s.graphs.as_ref())
                    .map(|graphs| graphs.iter().map(|g| (g.g_id, g.size)).collect())
                    .unwrap_or_default();

                id_result
                    .graphs
                    .into_iter()
                    .map(|mut g| {
                        if let Some(sz) = base_sizes.get(&g.g_id) {
                            g.size = *sz;
                        }
                        // Attach per-graph class stats.
                        g.classes = per_graph_classes.get(&g.g_id).cloned();
                        g
                    })
                    .collect()
            };

            let properties: Vec<fluree_db_core::index_stats::PropertyStatEntry> = agg_props
                .iter()
                .filter_map(|p| {
                    let sid = new_pred_sids.get(p.p_id as usize)?;
                    Some(fluree_db_core::index_stats::PropertyStatEntry {
                        sid: (sid.0, sid.1.clone()),
                        count: p.count,
                        ndv_values: p.ndv_values,
                        ndv_subjects: p.ndv_subjects,
                        last_modified_t: p.last_modified_t,
                        datatypes: p.datatypes.clone(),
                    })
                })
                .collect();

            let base_size = base_root.stats.as_ref().map_or(0, |s| s.size);

            let updated_stats = fluree_db_core::index_stats::IndexStats {
                flakes: id_result.total_flakes,
                size: base_size,
                properties: if properties.is_empty() {
                    None
                } else {
                    Some(properties)
                },
                classes: root_classes,
                graphs: if graphs.is_empty() {
                    None
                } else {
                    Some(graphs)
                },
            };

            tracing::info!(
                total_flakes = updated_stats.flakes,
                property_count = updated_stats.properties.as_ref().map_or(0, |p| p.len()),
                graph_count = updated_stats.graphs.as_ref().map_or(0, |g| g.len()),
                class_count = updated_stats.classes.as_ref().map_or(0, |c| c.len()),
                "Phase 4.6 complete: incremental stats refresh"
            );

            root_builder.set_stats(updated_stats);
            root_builder.set_sketch_ref(sketch_ref);
        }
    }

    // ---- Phase 4.7: Incremental schema (rdfs:subClassOf / rdfs:subPropertyOf) ----
    //
    // SchemaExtractor tracks class/property hierarchy from schema-relevant flakes.
    // We seed it from the base root's schema, then feed only novelty records that
    // match rdfs:subClassOf or rdfs:subPropertyOf. These are rare in typical novelty.
    //
    // Sid resolution for subject/object requires the subject forward dict (via
    // BinaryIndexStore), so we only attempt schema extraction when the base store
    // is available. Schema changes are extremely rare in incremental batches.
    {
        use crate::stats::SchemaExtractor;
        use fluree_db_core::subject_id::SubjectId;
        use fluree_db_core::{Flake, FlakeValue, Sid};
        use std::collections::HashMap;

        let rdfs_subclass_iri = format!("{}subClassOf", fluree_vocab::rdfs::NS);
        let rdfs_subprop_iri = format!("{}subPropertyOf", fluree_vocab::rdfs::NS);

        let subclass_p_id = novelty.shared.predicates.get(&rdfs_subclass_iri);
        let subprop_p_id = novelty.shared.predicates.get(&rdfs_subprop_iri);

        // Only run schema extraction if the schema predicates exist in the dict
        // AND there are novelty records that match them.
        let has_schema_records = novelty.records.iter().any(|r| {
            (subclass_p_id == Some(r.p_id) || subprop_p_id == Some(r.p_id))
                && r.o_kind == fluree_db_core::value_id::ObjKind::REF_ID.as_u8()
        });

        if has_schema_records {
            // Load a BinaryIndexStore for Sid resolution (subject/object IRIs).
            let cache_dir = config
                .data_dir
                .as_ref()
                .map(|d| d.join("schema_cache"))
                .unwrap_or_else(|| std::env::temp_dir().join("fluree_schema_cache"));
            let _ = std::fs::create_dir_all(&cache_dir);

            match BinaryIndexStore::load_from_root_v5(
                content_store.clone(),
                base_root,
                &cache_dir,
                None,
            )
            .await
            {
                Ok(store) => {
                    // Map new subjects in this incremental window for local sid64 -> Sid resolution.
                    // Base-root forward dicts won't include these until the new root is published.
                    let new_subject_suffix: HashMap<(u16, u64), String> = novelty
                        .new_subjects
                        .iter()
                        .filter_map(|(ns_code, local_id, suffix)| {
                            let s = std::str::from_utf8(suffix).ok()?.to_string();
                            Some(((*ns_code, *local_id), s))
                        })
                        .collect();

                    let resolve_sid64 = |sid64: u64| -> Option<Sid> {
                        let sid = SubjectId::from_u64(sid64);
                        let ns_code = sid.ns_code();
                        let local_id = sid.local_id();
                        if let Some(suffix) = new_subject_suffix.get(&(ns_code, local_id)) {
                            return Some(Sid::new(ns_code, suffix));
                        }
                        store
                            .resolve_subject_iri(sid64)
                            .ok()
                            .map(|iri| store.encode_iri(&iri))
                    };

                    let mut extractor = SchemaExtractor::from_prior(base_root.schema.as_ref());

                    for record in &novelty.records {
                        let is_subclass = subclass_p_id == Some(record.p_id);
                        let is_subprop = subprop_p_id == Some(record.p_id);
                        if !is_subclass && !is_subprop {
                            continue;
                        }
                        if record.o_kind != fluree_db_core::value_id::ObjKind::REF_ID.as_u8() {
                            continue;
                        }

                        let Some(s_sid) = resolve_sid64(record.s_id.as_u64()) else {
                            continue;
                        };
                        let Some(o_sid) = resolve_sid64(record.o_key) else {
                            continue;
                        };

                        // Build predicate Sid
                        let p_sid = if is_subclass {
                            fluree_db_core::Sid::new(fluree_vocab::namespaces::RDFS, "subClassOf")
                        } else {
                            fluree_db_core::Sid::new(
                                fluree_vocab::namespaces::RDFS,
                                "subPropertyOf",
                            )
                        };

                        let flake = Flake::new(
                            s_sid,
                            p_sid,
                            FlakeValue::Ref(o_sid),
                            Sid::new(0, ""),
                            record.t as i64,
                            record.op != 0,
                            None,
                        );
                        extractor.on_flake(&flake);
                    }

                    // finalize() returns None when all relationships were removed.
                    // In that case we must clear the schema section.
                    let updated_schema = extractor.finalize(novelty.max_t);
                    root_builder.set_schema_opt(updated_schema);
                    tracing::info!("Phase 4.7: incremental schema refreshed");
                }
                Err(e) => {
                    return Err(IndexerError::IncrementalAbort(format!(
                        "Phase 4.7: store load for schema extraction failed: {e}; \
                         aborting incremental to ensure schema correctness"
                    )));
                }
            }
        }
    }

    // ---- Phase 5: Root assembly ----
    root_builder.set_dict_refs(new_dict_refs);
    root_builder.set_watermarks(
        novelty.updated_watermarks.clone(),
        novelty.updated_string_watermark,
    );

    root_builder.set_predicate_sids(new_pred_sids);
    root_builder.set_namespace_codes(new_ns_codes);

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

    // Link to previous index root.
    root_builder.set_prev_index(BinaryPrevIndexRef {
        t: base_root.index_t,
        id: base_root_id.clone(),
    });

    // Build garbage manifest.
    let (new_root, replaced_cids) = root_builder.build();

    // Write garbage record if there are replaced CIDs.
    if !replaced_cids.is_empty() {
        let garbage_strings: Vec<String> = replaced_cids.iter().map(|c| c.to_string()).collect();
        let garbage_ref =
            gc::write_garbage_record(storage, ledger_id, new_root.index_t, garbage_strings)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        // We need to set garbage on the root, but we already consumed the builder.
        // Build a mutable root and set garbage + encode.
        let mut final_root = new_root;
        final_root.garbage = garbage_ref.map(|id| BinaryGarbageRef { id });

        // Encode and upload root.
        let root_bytes = final_root.encode();
        let write_result = storage
            .content_write_bytes(ContentKind::IndexRoot, ledger_id, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        let root_id = ContentId::from_hex_digest(
            fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
            &write_result.content_hash,
        )
        .ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "invalid content_hash: {}",
                write_result.content_hash
            ))
        })?;

        tracing::info!(
            %root_id,
            index_t = final_root.index_t,
            replaced = replaced_cids.len(),
            "incremental index root published"
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
        // No garbage — encode and upload root directly.
        let root_bytes = new_root.encode();
        let write_result = storage
            .content_write_bytes(ContentKind::IndexRoot, ledger_id, &root_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        let root_id = ContentId::from_hex_digest(
            fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
            &write_result.content_hash,
        )
        .ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "invalid content_hash: {}",
                write_result.content_hash
            ))
        })?;

        tracing::info!(
            %root_id,
            index_t = new_root.index_t,
            "incremental index root published (no garbage)"
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

/// Build predicate SIDs from resolver state for the index root.
fn build_predicate_sids(
    shared: &run_index::resolver::SharedResolverState,
    namespace_codes: &std::collections::BTreeMap<u16, String>,
) -> Vec<(u16, String)> {
    use fluree_db_core::PrefixTrie;

    // PrefixTrie::from_namespace_codes expects HashMap.
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

/// Build a fresh branch from pure novelty (no existing branch).
///
/// Used when novelty touches a graph+order that didn't exist in the base root.
/// Uses `LeafWriter` with a temp directory, reads back leaf files, cleans up.
fn build_fresh_branch(
    sorted_records: &[RunRecord],
    order: RunSortOrder,
    g_id: u16,
    config: &run_index::incremental_branch::IncrementalBranchConfig,
    p_width: u8,
    dt_width: u8,
) -> Result<run_index::incremental_branch::BranchUpdateResult> {
    use fluree_db_binary_index::format::branch::{build_branch_v2_bytes, LeafEntry};
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use run_index::incremental_leaf::NewLeafBlob;

    let tmp_dir = std::env::temp_dir().join(format!(
        "fluree-fresh-branch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| IndexerError::StorageWrite(format!("create fresh branch tmp dir: {e}")))?;

    let mut leaf_writer = LeafWriter::with_widths(
        tmp_dir.clone(),
        config.leaflet_target_rows,
        config.leaflets_per_leaf,
        config.zstd_level,
        dt_width,
        p_width,
        order,
    );

    for record in sorted_records {
        leaf_writer
            .push_record(*record)
            .map_err(|e| IndexerError::StorageWrite(format!("leaf writer: {e}")))?;
    }

    let leaf_infos = leaf_writer
        .finish()
        .map_err(|e| IndexerError::StorageWrite(format!("leaf writer finish: {e}")))?;

    let mut leaf_entries = Vec::with_capacity(leaf_infos.len());
    let mut new_blobs = Vec::with_capacity(leaf_infos.len());

    for info in &leaf_infos {
        // Read the leaf file from disk.
        let leaf_path = tmp_dir.join(info.leaf_cid.to_string());
        let leaf_bytes = std::fs::read(&leaf_path).map_err(|e| {
            IndexerError::StorageRead(format!("read fresh leaf {}: {e}", leaf_path.display()))
        })?;

        leaf_entries.push(LeafEntry {
            first_key: info.first_key,
            last_key: info.last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid.clone(),
            resolved_path: None,
        });

        new_blobs.push(NewLeafBlob {
            bytes: leaf_bytes,
            cid: info.leaf_cid.clone(),
            first_key: info.first_key,
            last_key: info.last_key,
            row_count: info.total_rows,
        });
    }

    // Clean up temp dir.
    let _ = std::fs::remove_dir_all(&tmp_dir);

    let branch_bytes = build_branch_v2_bytes(order, g_id, &leaf_entries);
    let branch_hash = fluree_db_core::sha256_hex(&branch_bytes);
    let branch_cid = ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH,
        &branch_hash,
    )
    .ok_or_else(|| IndexerError::StorageWrite(format!("invalid branch hash: {branch_hash}")))?;

    Ok(run_index::incremental_branch::BranchUpdateResult {
        leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids: Vec::new(),
        branch_bytes,
        branch_cid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that build_predicate_sids correctly decomposes predicates using the
    /// provided namespace table. If a new namespace is introduced, predicates
    /// using that namespace should be split correctly.
    #[test]
    fn test_build_predicate_sids_with_new_namespace() {
        use run_index::resolver::SharedResolverState;

        // Create a SharedResolverState with predicates spanning two namespaces.
        let mut shared = SharedResolverState::new_for_ledger("test:main");
        // p_id 0: predicate in ns 1 ("http://example.org/")
        let _p0 = shared.predicates.get_or_insert("http://example.org/name");
        // p_id 1: predicate in ns 1
        let _p1 = shared.predicates.get_or_insert("http://example.org/age");
        // p_id 2: predicate in ns 2 ("http://schema.org/") — new namespace
        let _p2 = shared.predicates.get_or_insert("http://schema.org/knows");

        // Namespace table with ns 0 = "" (default), ns 1 = "http://example.org/", ns 2 = "http://schema.org/"
        let ns_codes: std::collections::BTreeMap<u16, String> = [
            (0, "".to_string()),
            (1, "http://example.org/".to_string()),
            (2, "http://schema.org/".to_string()),
        ]
        .into();

        let result = build_predicate_sids(&shared, &ns_codes);

        // p_id 0: "http://example.org/name" matches ns 1 "http://example.org/" -> (1, "name")
        assert_eq!(result[0], (1, "name".to_string()));
        // p_id 1: "http://example.org/age" matches ns 1 -> (1, "age")
        assert_eq!(result[1], (1, "age".to_string()));
        // p_id 2: "http://schema.org/knows" matches ns 2 "http://schema.org/" -> (2, "knows")
        assert_eq!(result[2], (2, "knows".to_string()));

        // Now test with STALE namespace table (missing ns 2).
        // This simulates the bug: using base_root.namespace_codes instead of updated ones.
        let stale_ns: std::collections::BTreeMap<u16, String> =
            [(0, "".to_string()), (1, "http://example.org/".to_string())].into();
        let stale_result = build_predicate_sids(&shared, &stale_ns);

        // p_id 2 should fall back to (0, full_iri) — wrong encoding
        assert_eq!(stale_result[2], (0, "http://schema.org/knows".to_string()));
        // This demonstrates why using updated ns_codes is critical
    }
}
