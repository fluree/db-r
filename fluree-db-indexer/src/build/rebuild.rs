//! Full index rebuild pipeline (Phase A..F).
//!
//! Walks the entire commit chain from genesis, resolves all commits into
//! sorted run files, builds per-graph leaf/branch indexes for all sort
//! orders, and writes an `IndexRootV5` (IRB1) descriptor to storage.

use fluree_db_binary_index::{
    BinaryGarbageRef, BinaryIndexStore, BinaryPrevIndexRef, DictRefsV5, FulltextArenaRefV5,
    GraphArenaRefsV5, IndexRootV5, RunRecord, RunSortOrder, SpatialArenaRefV5, VectorDictRefV5,
};
use fluree_db_core::{ContentId, ContentKind, ContentStore, GraphId, Storage};

use crate::error::{IndexerError, Result};
use crate::run_index;
use crate::{IndexResult, IndexStats, IndexerConfig};

use super::spatial::build_and_upload_spatial_indexes;
use super::upload::cid_from_write;
use super::upload::upload_indexes_to_cas;
use super::upload_dicts::upload_dicts_from_disk;

use crate::gc;
use tracing::Instrument;

///
/// Unlike `build_index_for_ledger`, this skips the nameservice lookup and
/// the "already current" early-return check. Use this when you already have
/// the `NsRecord` and want to force a rebuild (e.g., `reindex`).
///
/// Runs the entire pipeline on a blocking thread via `spawn_blocking` +
/// `handle.block_on()` because internal dictionaries contain non-Send types
/// held across await points.
///
/// Pipeline:
/// 1. Walk commit chain backward → forward CID list
/// 2. Resolve commits into batched chunks with per-chunk local dicts
/// 3. Dict merge (subjects + strings) → global IDs + remap tables
/// 4. Build SPOT from sorted commit files (k-way merge with g_id)
/// 5. Remap + build secondary indexes (PSOT/POST/OPST)
/// 6. Upload artifacts to CAS and write BinaryIndexRoot
pub async fn rebuild_index_from_commits<S>(
    storage: &S,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_novelty::commit_v2::read_commit_envelope;
    use run_index::resolver::{RebuildChunk, SharedResolverState};
    use run_index::spool::SortedCommitInfo;

    let head_commit_id = record
        .commit_head_id
        .clone()
        .ok_or(IndexerError::NoCommits)?;

    // Determine output directory for binary index artifacts
    let data_dir = config
        .data_dir
        .unwrap_or_else(|| std::env::temp_dir().join("fluree-index"));
    let ledger_id_path = fluree_db_core::address_path::ledger_id_to_path_prefix(ledger_id)
        .unwrap_or_else(|_| ledger_id.replace(':', "/"));
    let session_id = uuid::Uuid::new_v4().to_string();
    let run_dir = data_dir
        .join(&ledger_id_path)
        .join("tmp_import")
        .join(&session_id);
    let index_dir = data_dir.join(&ledger_id_path).join("index");

    tracing::info!(
        %head_commit_id,
        ?run_dir,
        ?index_dir,
        "starting binary index rebuild from commits"
    );

    // Capture values for the blocking task
    let storage = storage.clone();
    let ledger_id = ledger_id.to_string();
    let prev_root_id = record.index_head_id.clone();
    let handle = tokio::runtime::Handle::current();
    let parent_span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        let _guard = parent_span.enter(); // safe: spawn_blocking pins to one thread
        handle.block_on(async {
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Build a content store bridge for CID → address resolution
            let content_store =
                fluree_db_core::storage::content_store_for(storage.clone(), &ledger_id);

            // Phase spans below use .entered() — safe because block_on inside
            // spawn_blocking pins this async task to a single OS thread.

            // ---- Phase A: Walk commit chain backward to collect CIDs ----
            let _span_a = tracing::debug_span!("commit_chain_walk").entered();
            let commit_cids = {
                let mut cids = Vec::new();
                let mut current = Some(head_commit_id.clone());

                while let Some(cid) = current {
                    let bytes = content_store
                        .get(&cid)
                        .await
                        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;
                    let envelope = read_commit_envelope(&bytes)
                        .map_err(|e| IndexerError::StorageRead(e.to_string()))?;
                    current = envelope.previous_id().cloned();
                    cids.push(cid);
                }

                cids.reverse(); // chronological order (genesis first)
                cids
            };
            drop(_span_a);

            // ---- Phase B: Resolve commits into batched chunks ----
            let _span_b =
                tracing::debug_span!("commit_resolve", commits = commit_cids.len()).entered();
            let mut shared = SharedResolverState::new_for_ledger(&ledger_id);

            // Pre-insert rdf:type into predicate dictionary so class tracking
            // works from the very first commit.
            let rdf_type_p_id = shared.predicates.get_or_insert(fluree_vocab::rdf::TYPE);

            // Enable spatial geometry collection during resolution.
            shared.spatial_hook = Some(crate::spatial_hook::SpatialHook::new());

            // Enable fulltext collection during resolution.
            shared.fulltext_hook = Some(crate::fulltext_hook::FulltextHook::new());

            let chunk_max_flakes: u64 = 5_000_000; // ~5M flakes per chunk
            let mut chunk = RebuildChunk::new();
            let mut chunks: Vec<RebuildChunk> = Vec::new();

            // Track spatial entry ranges per chunk for subject ID remapping.
            // Each entry is (start_idx, end_idx) into spatial_hook.entries().
            let mut spatial_chunk_ranges: Vec<(usize, usize)> = Vec::new();
            let mut spatial_cursor: usize = 0;

            // Track fulltext entry ranges per chunk for string ID remapping.
            let mut fulltext_chunk_ranges: Vec<(usize, usize)> = Vec::new();
            let mut fulltext_cursor: usize = 0;

            // Accumulate commit statistics for index root
            let mut total_commit_size = 0u64;
            let mut total_asserts = 0u64;
            let mut total_retracts = 0u64;

            for (i, cid) in commit_cids.iter().enumerate() {
                // If chunk is non-empty and near budget, flush before processing
                // the next commit to avoid memory bloat on large commits.
                if !chunk.is_empty() && chunk.flake_count() >= chunk_max_flakes {
                    let spatial_end = shared.spatial_hook.as_ref().map_or(0, |h| h.entry_count());
                    spatial_chunk_ranges.push((spatial_cursor, spatial_end));
                    spatial_cursor = spatial_end;
                    let fulltext_end = shared.fulltext_hook.as_ref().map_or(0, |h| h.entry_count());
                    fulltext_chunk_ranges.push((fulltext_cursor, fulltext_end));
                    fulltext_cursor = fulltext_end;
                    chunks.push(std::mem::take(&mut chunk));
                }

                let bytes = content_store
                    .get(cid)
                    .await
                    .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;

                let resolved = shared
                    .resolve_commit_into_chunk(&bytes, &cid.digest_hex(), &mut chunk)
                    .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

                // Accumulate totals
                total_commit_size += resolved.size;
                total_asserts += resolved.asserts as u64;
                total_retracts += resolved.retracts as u64;

                tracing::debug!(
                    commit = i + 1,
                    t = resolved.t,
                    ops = resolved.total_records,
                    chunk_flakes = chunk.flake_count(),
                    "commit resolved into chunk"
                );

                // Post-commit flush check.
                if chunk.flake_count() >= chunk_max_flakes {
                    let spatial_end = shared.spatial_hook.as_ref().map_or(0, |h| h.entry_count());
                    spatial_chunk_ranges.push((spatial_cursor, spatial_end));
                    spatial_cursor = spatial_end;
                    let fulltext_end = shared.fulltext_hook.as_ref().map_or(0, |h| h.entry_count());
                    fulltext_chunk_ranges.push((fulltext_cursor, fulltext_end));
                    fulltext_cursor = fulltext_end;
                    chunks.push(std::mem::take(&mut chunk));
                }
            }

            // Push final chunk if non-empty.
            if !chunk.is_empty() {
                let spatial_end = shared.spatial_hook.as_ref().map_or(0, |h| h.entry_count());
                spatial_chunk_ranges.push((spatial_cursor, spatial_end));
                let fulltext_end = shared.fulltext_hook.as_ref().map_or(0, |h| h.entry_count());
                fulltext_chunk_ranges.push((fulltext_cursor, fulltext_end));
                chunks.push(chunk);
            }

            tracing::info!(
                chunks = chunks.len(),
                total_asserts,
                total_retracts,
                predicates = shared.predicates.len(),
                graphs = shared.graphs.len(),
                "Phase B complete: all commits resolved into chunks"
            );
            drop(_span_b);

            // ---- Phase C: Dict merge → global IDs + remap tables ----
            let _span_c = tracing::debug_span!("dict_merge_and_remap").entered();
            // Separate dicts from records so merge can borrow owned dicts.
            let mut subject_dicts = Vec::with_capacity(chunks.len());
            let mut string_dicts = Vec::with_capacity(chunks.len());
            let mut chunk_records: Vec<Vec<RunRecord>> = Vec::with_capacity(chunks.len());

            for chunk in chunks {
                subject_dicts.push(chunk.subjects);
                string_dicts.push(chunk.strings);
                chunk_records.push(chunk.records);
            }

            let (subject_merge, subject_remaps) =
                run_index::dict_merge::merge_subject_dicts(&subject_dicts);
            let (string_merge, string_remaps) =
                run_index::dict_merge::merge_string_dicts(&string_dicts);

            // Remap spatial entries' chunk-local subject IDs → global sid64.
            // The spatial hook accumulated entries with chunk-local s_id values;
            // spatial_chunk_ranges[ci] = (start, end) into entries for chunk ci.
            let spatial_entries: Vec<crate::spatial_hook::SpatialEntry> = {
                let mut all_entries = shared
                    .spatial_hook
                    .take()
                    .map(|h| h.into_entries())
                    .unwrap_or_default();

                for (ci, &(start, end)) in spatial_chunk_ranges.iter().enumerate() {
                    let s_remap = &subject_remaps[ci];
                    for entry in &mut all_entries[start..end] {
                        let local_s = entry.subject_id as usize;
                        if let Some(&global_s) = s_remap.get(local_s) {
                            entry.subject_id = global_s;
                        }
                    }
                }

                if !all_entries.is_empty() {
                    tracing::info!(
                        spatial_entries = all_entries.len(),
                        "spatial entries collected and remapped to global IDs"
                    );
                }
                all_entries
            };

            // Remap fulltext entries' chunk-local string IDs → global string IDs.
            // The fulltext hook accumulated entries with chunk-local string_id values;
            // fulltext_chunk_ranges[ci] = (start, end) into entries for chunk ci.
            let fulltext_entries: Vec<crate::fulltext_hook::FulltextEntry> = {
                let mut all_entries = shared
                    .fulltext_hook
                    .take()
                    .map(|h| h.into_entries())
                    .unwrap_or_default();

                for (ci, &(start, end)) in fulltext_chunk_ranges.iter().enumerate() {
                    let str_remap = &string_remaps[ci];
                    for entry in &mut all_entries[start..end] {
                        let local_str = entry.string_id as usize;
                        if let Some(&global_str) = str_remap.get(local_str) {
                            entry.string_id = global_str;
                        } else {
                            tracing::warn!(
                                chunk = ci,
                                local_str,
                                "fulltext entry string_id remap miss; skipping"
                            );
                            // Mark as retraction so it's skipped by the builder.
                            entry.is_assert = false;
                            entry.string_id = u32::MAX;
                        }
                    }
                }

                if !all_entries.is_empty() {
                    tracing::info!(
                        fulltext_entries = all_entries.len(),
                        "fulltext entries collected and remapped to global IDs"
                    );
                }
                all_entries
            };

            // Remap records to global IDs in-place, sort by cmp_g_spot, write .fsc files.
            let commits_dir = run_dir.join("sorted_commits");
            std::fs::create_dir_all(&commits_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            let mut sorted_commit_infos: Vec<SortedCommitInfo> = Vec::new();

            for (ci, records) in chunk_records.iter_mut().enumerate() {
                let s_remap = &subject_remaps[ci];
                let str_remap = &string_remaps[ci];

                // Remap chunk-local IDs → global IDs in-place.
                for record in records.iter_mut() {
                    // Subject: chunk-local u64 → global sid64
                    let local_s = record.s_id.as_u64() as usize;
                    let global_s = *s_remap.get(local_s).ok_or_else(|| {
                        IndexerError::StorageWrite(format!(
                            "subject remap miss: chunk {ci}, local_s={local_s}"
                        ))
                    })?;
                    record.s_id = fluree_db_core::subject_id::SubjectId::from_u64(global_s);

                    // Object: remap if REF_ID (subject) or LEX_ID/JSON_ID (string)
                    let kind = fluree_db_core::value_id::ObjKind::from_u8(record.o_kind);
                    if kind == fluree_db_core::value_id::ObjKind::REF_ID {
                        let local_o = record.o_key as usize;
                        record.o_key = *s_remap.get(local_o).ok_or_else(|| {
                            IndexerError::StorageWrite(format!(
                                "subject remap miss: chunk {ci}, local_o={local_o}"
                            ))
                        })?;
                    } else if kind == fluree_db_core::value_id::ObjKind::LEX_ID
                        || kind == fluree_db_core::value_id::ObjKind::JSON_ID
                    {
                        let local_str = fluree_db_core::value_id::ObjKey::from_u64(record.o_key)
                            .decode_u32_id() as usize;
                        let global_str = *str_remap.get(local_str).ok_or_else(|| {
                            IndexerError::StorageWrite(format!(
                                "string remap miss: chunk {ci}, local_str={local_str}"
                            ))
                        })?;
                        record.o_key =
                            fluree_db_core::value_id::ObjKey::encode_u32_id(global_str).as_u64();
                    }
                    // else: inline types, no remap needed
                }

                // Sort by (g_id, SPOT).
                records.sort_unstable_by(fluree_db_binary_index::format::run_record::cmp_g_spot);

                // Write sorted commit file (.fsc) via SpoolWriter.
                let fsc_path = commits_dir.join(format!("chunk_{ci:05}.fsc"));
                let mut spool_writer = run_index::spool::SpoolWriter::new(&fsc_path, ci)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                for record in records.iter() {
                    spool_writer
                        .push(record)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }
                let spool_info = spool_writer
                    .finish()
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                // Extract rdf:type edges into .types sidecar (for ClassBitsetTable).
                // Records are already global IDs, so sidecar entries are global too.
                let ref_id = fluree_db_core::value_id::ObjKind::REF_ID.as_u8();
                let types_path = commits_dir.join(format!("chunk_{ci:05}.types"));
                {
                    let file = std::fs::File::create(&types_path)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let mut bw = std::io::BufWriter::new(file);
                    for record in records.iter() {
                        if record.p_id == rdf_type_p_id && record.o_kind == ref_id && record.op == 1
                        {
                            std::io::Write::write_all(&mut bw, &record.g_id.to_le_bytes())
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                            std::io::Write::write_all(&mut bw, &record.s_id.as_u64().to_le_bytes())
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                            std::io::Write::write_all(&mut bw, &record.o_key.to_le_bytes())
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        }
                    }
                    std::io::Write::flush(&mut bw)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }

                sorted_commit_infos.push(SortedCommitInfo {
                    path: fsc_path,
                    record_count: spool_info.record_count,
                    byte_len: spool_info.byte_len,
                    chunk_idx: ci,
                    subject_count: subject_dicts[ci].len(),
                    string_count: string_dicts[ci].len() as u64,
                    types_map_path: Some(types_path),
                });
            }

            // Persist global dicts to disk for index-store loading + CAS upload.
            {
                use run_index::dict_io::{write_language_dict, write_predicate_dict};

                let preds: Vec<&str> = (0..shared.predicates.len())
                    .map(|p_id| shared.predicates.resolve(p_id).unwrap_or(""))
                    .collect();
                std::fs::write(
                    run_dir.join("predicates.json"),
                    serde_json::to_vec(&preds)
                        .map_err(|e| IndexerError::Serialization(e.to_string()))?,
                )
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                write_predicate_dict(&run_dir.join("graphs.dict"), &shared.graphs)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                write_predicate_dict(&run_dir.join("datatypes.dict"), &shared.datatypes)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                run_index::persist_namespaces(&shared.ns_prefixes, &run_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                write_language_dict(&run_dir.join("languages.dict"), &shared.languages)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            }

            // Write subject/string forward files + indexes from merge results.
            run_index::dict_merge::persist_merge_artifacts(
                &run_dir,
                &subject_merge,
                &string_merge,
                &shared.ns_prefixes,
            )
            .map_err(|e: std::io::Error| IndexerError::StorageWrite(e.to_string()))?;

            // Write numbig arenas (per-graph subdirectories)
            for (&g_id, per_pred) in &shared.numbigs {
                if per_pred.is_empty() {
                    continue;
                }
                let nb_dir = run_dir.join(format!("g_{}", g_id)).join("numbig");
                std::fs::create_dir_all(&nb_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                for (&p_id, arena) in per_pred {
                    fluree_db_binary_index::arena::numbig::write_numbig_arena(
                        &nb_dir.join(format!("p_{}.nba", p_id)),
                        arena,
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }
            }

            // Write vector arenas (per-graph subdirectories, shards + manifests per predicate)
            for (&g_id, per_pred) in &shared.vectors {
                if per_pred.is_empty() {
                    continue;
                }
                let vec_dir = run_dir.join(format!("g_{}", g_id)).join("vectors");
                std::fs::create_dir_all(&vec_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }
                    let shard_paths = fluree_db_binary_index::arena::vector::write_vector_shards(
                        &vec_dir, p_id, arena,
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let shard_infos: Vec<fluree_db_binary_index::arena::vector::ShardInfo> =
                        shard_paths
                            .iter()
                            .enumerate()
                            .map(|(i, path)| {
                                let cap = fluree_db_binary_index::arena::vector::SHARD_CAPACITY;
                                let start = i as u32 * cap;
                                let count = (arena.len() - start).min(cap);
                                fluree_db_binary_index::arena::vector::ShardInfo {
                                    cas: path.display().to_string(),
                                    count,
                                }
                            })
                            .collect();
                    fluree_db_binary_index::arena::vector::write_vector_manifest(
                        &vec_dir.join(format!("p_{}.vam", p_id)),
                        arena,
                        &shard_infos,
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }
            }

            tracing::info!(
                subjects = subject_merge.total_subjects,
                strings = string_merge.total_strings,
                "Phase C complete: dict merge done"
            );
            drop(_span_c);

            // ---- Phase D: Build SPOT from sorted commits ----
            // Records are already remapped to global IDs, so use identity remaps.
            let p_width = fluree_db_binary_index::format::leaflet::p_width_for_max(
                shared.predicates.len().saturating_sub(1),
            );
            if shared.datatypes.len() > 256 {
                return Err(IndexerError::StorageWrite(format!(
                    "datatype dictionary too large for u8 encoding ({} > 256)",
                    shared.datatypes.len()
                )));
            }
            let dt_width: u8 = 1; // datatypes u8-encoded (validated above)

            // Build ClassBitsetTable from .types sidecars (IDs are already global).
            let identity_remap = run_index::spool::IdentitySubjectRemap;
            let bitset_inputs: Vec<(&std::path::Path, &run_index::spool::IdentitySubjectRemap)> =
                sorted_commit_infos
                    .iter()
                    .filter_map(|info| {
                        info.types_map_path
                            .as_ref()
                            .map(|p| (p.as_path(), &identity_remap))
                    })
                    .collect();

            let class_bitset = if !bitset_inputs.is_empty() {
                let table = run_index::ClassBitsetTable::build_from_type_maps(&bitset_inputs)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                tracing::info!(
                    classes = table.class_count(),
                    "class bitset table built for SPOT merge"
                );
                Some(table)
            } else {
                None
            };

            let spot_inputs: Vec<
                run_index::SortedCommitInput<
                    run_index::spool::IdentitySubjectRemap,
                    run_index::spool::IdentityStringRemap,
                >,
            > = sorted_commit_infos
                .iter()
                .map(|info| run_index::SortedCommitInput {
                    commit_path: info.path.clone(),
                    subject_remap: run_index::spool::IdentitySubjectRemap,
                    string_remap: run_index::spool::IdentityStringRemap,
                    lang_remap: vec![], // language tags are global, no remap needed
                })
                .collect();

            let spot_config = run_index::SpotFromCommitsConfig {
                index_dir: index_dir.clone(),
                p_width,
                dt_width,
                leaflet_rows: config.leaflet_rows,
                leaflets_per_leaf: config.leaflets_per_leaf,
                zstd_level: 1,
                progress: None,
                skip_dedup: false,   // rebuild needs dedup
                skip_region3: false, // rebuild needs history journal
                rdf_type_p_id: Some(rdf_type_p_id),
                class_bitset,
            };

            let (spot_result, spot_class_stats) =
                run_index::build_spot_from_sorted_commits(spot_inputs, spot_config)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            tracing::info!(
                graphs = spot_result.graphs.len(),
                total_rows = spot_result.graphs.iter().map(|g| g.total_rows).sum::<u64>(),
                "Phase D complete: SPOT built"
            );

            // ---- Phase E: Build secondary indexes (PSOT/POST/OPST) ----
            // Read sorted commit files, partition records by graph, collect
            // per-property HLL stats, then build per-graph secondary indexes.
            //
            // Each graph's indexes live in their own directory — the run wire
            // format (34 bytes) does not carry g_id because it is implicit
            // from the directory path. We use g_id_override in IndexBuildConfig
            // so the index builder creates the correct graph_{g_id}/ output.

            let dt_tags: &[fluree_db_core::value_id::ValueTypeTag] = &shared.dt_tags;

            let mut stats_hook = crate::stats::IdStatsHook::new();
            stats_hook.set_rdf_type_p_id(rdf_type_p_id);

            let secondary_orders = RunSortOrder::secondary_orders();
            let phase_e_start = std::time::Instant::now();

            // E.1: Read sorted commit files, partition by g_id, collect stats,
            //      and write per-graph run files via MultiOrderRunWriter.
            let _span_e12 = tracing::debug_span!("secondary_partition").entered();
            let remap_dir = run_dir.join("remap");
            std::fs::create_dir_all(&remap_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            let mut graph_writers: std::collections::BTreeMap<u16, run_index::MultiOrderRunWriter> =
                std::collections::BTreeMap::new();
            let mut lang_dict = shared.languages.clone();

            for info in &sorted_commit_infos {
                let reader = run_index::SpoolReader::open(&info.path, info.record_count)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                for result in reader {
                    let record = result.map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    // Collect per-property HLL stats from globally-remapped records.
                    let sr = run_index::spool::stats_record_for_remapped_run_record(
                        &record,
                        Some(dt_tags),
                    );
                    stats_hook.on_record(&sr);

                    // Push to per-graph writer (creates lazily on first record).
                    let g_id = record.g_id;
                    let writer = match graph_writers.entry(g_id) {
                        std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::btree_map::Entry::Vacant(e) => {
                            let graph_run_dir = remap_dir.join(format!("graph_{g_id}"));
                            let mo_config = run_index::MultiOrderConfig {
                                orders: secondary_orders.to_vec(),
                                base_run_dir: graph_run_dir,
                                total_budget_bytes: config.run_budget_bytes,
                            };
                            let w = run_index::MultiOrderRunWriter::new(mo_config)
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                            e.insert(w)
                        }
                    };
                    writer
                        .push(record, &mut lang_dict)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }
            }

            // E.2: Flush per-graph writers → per-graph run files.
            let mut graph_run_results: std::collections::BTreeMap<
                u16,
                Vec<(RunSortOrder, run_index::RunWriterResult)>,
            > = std::collections::BTreeMap::new();

            for (g_id, writer) in graph_writers {
                let results = writer
                    .finish(&mut lang_dict)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                graph_run_results.insert(g_id, results);
            }
            drop(_span_e12);

            // E.3: Build per-graph secondary indexes from the run files.
            let mut secondary_results: Vec<(RunSortOrder, run_index::IndexBuildResult)> =
                Vec::new();

            for &order in secondary_orders {
                let mut all_graph_results: Vec<run_index::GraphIndexResult> = Vec::new();
                let mut total_rows: u64 = 0;

                for (&g_id, results) in &graph_run_results {
                    let order_result = results.iter().find(|(o, _)| *o == order);
                    let Some((_, writer_result)) = order_result else {
                        continue;
                    };
                    if writer_result.run_files.is_empty() {
                        continue;
                    }

                    let run_paths: Vec<std::path::PathBuf> = writer_result
                        .run_files
                        .iter()
                        .map(|rf| rf.path.clone())
                        .collect();

                    let config = run_index::IndexBuildConfig {
                        run_dir: remap_dir
                            .join(format!("graph_{g_id}"))
                            .join(order.dir_name()),
                        dicts_dir: run_dir.clone(),
                        index_dir: index_dir.clone(),
                        sort_order: order,
                        leaflet_rows: config.leaflet_rows,
                        leaflets_per_leaf: config.leaflets_per_leaf,
                        zstd_level: 1,
                        persist_lang_dict: false,
                        progress: None,
                        skip_dedup: false,
                        skip_region3: false,
                        g_id_override: Some(g_id),
                    };

                    let result = run_index::build_index_from_run_paths(config, run_paths)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    total_rows += result.total_rows;
                    all_graph_results.extend(result.graphs);
                }

                secondary_results.push((
                    order,
                    run_index::IndexBuildResult {
                        graphs: all_graph_results,
                        total_rows,
                        index_dir: index_dir.clone(),
                        elapsed: phase_e_start.elapsed(),
                    },
                ));
            }

            // Combine SPOT + secondary results.
            let mut build_results: Vec<(RunSortOrder, run_index::IndexBuildResult)> = Vec::new();
            build_results.push((RunSortOrder::Spot, spot_result));
            build_results.extend(secondary_results);

            let total_records: u64 = build_results
                .iter()
                .filter(|(o, _)| *o == RunSortOrder::Spot)
                .flat_map(|(_, r)| r.graphs.iter())
                .map(|g| g.total_rows)
                .sum();

            tracing::info!(
                secondary_count = build_results.len() - 1,
                "Phase E complete: secondary indexes built"
            );

            // ---- Phase E.5: Build spatial indexes from collected geometry entries ----
            let spatial_arena_refs: Vec<(GraphId, Vec<SpatialArenaRefV5>)> = {
                if spatial_entries.is_empty() {
                    vec![]
                } else {
                    build_and_upload_spatial_indexes(
                        &spatial_entries,
                        &shared.predicates,
                        &ledger_id,
                        &storage,
                    )
                    .await?
                }
            };

            // ---- Phase E.6: Build fulltext arenas from collected entries ----
            let fulltext_arena_refs: Vec<(GraphId, Vec<FulltextArenaRefV5>)> = {
                if fulltext_entries.is_empty() {
                    vec![]
                } else {
                    super::fulltext::build_and_upload_fulltext_arenas(
                        &fulltext_entries,
                        &string_merge,
                        &ledger_id,
                        &storage,
                    )
                    .await?
                }
            };

            // ---- Phase F: Upload artifacts to CAS and write v4 root ----

            // F.1: Load store for max_t / base_t / namespace_codes
            let store = BinaryIndexStore::load(&run_dir, &index_dir)
                .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

            // Build predicate p_id -> (ns_code, suffix) mapping for the root (compact).
            let predicate_sids: Vec<(u16, String)> = (0..shared.predicates.len())
                .map(|p_id| {
                    let iri = shared.predicates.resolve(p_id).unwrap_or("");
                    let sid = store.encode_iri(iri);
                    (sid.namespace_code, sid.name.as_ref().to_string())
                })
                .collect();

            // F.2: Upload dictionary artifacts to CAS
            // The rebuild pipeline uses SharedResolverState instead of GlobalDicts,
            // so we use upload_dicts_from_disk which reads the flat files we already wrote.
            let dict_addresses =
                upload_dicts_from_disk(&storage, &ledger_id, &run_dir, store.namespace_codes())
                    .instrument(tracing::debug_span!("upload_dicts"))
                    .await?;

            // F.3: Upload index artifacts (branches + leaves) to CAS.
            // Default graph (g_id=0): leaves uploaded, branch NOT (inline in root).
            // Named graphs: both branches and leaves uploaded.
            let uploaded_indexes = upload_indexes_to_cas(&storage, &ledger_id, &build_results)
                .instrument(tracing::debug_span!("upload_indexes"))
                .await?;

            // F.4: Build HLL sketch blob and IndexStats from IdStatsHook + class stats.
            let _span_f = tracing::debug_span!("build_index_root").entered();

            // Create sketch blob BEFORE finalize_with_aggregate_properties() consumes
            // the hook. This borrows properties() which is consumed by finalize.
            let sketch_blob = crate::stats::HllSketchBlob::from_properties(
                store.max_t(),
                stats_hook.properties(),
            );
            let sketch_ref = if !sketch_blob.entries.is_empty() {
                let sketch_bytes = sketch_blob
                    .to_json_bytes()
                    .map_err(|e| IndexerError::StorageWrite(format!("sketch serialize: {e}")))?;
                let sketch_wr = storage
                    .content_write_bytes(ContentKind::StatsSketch, &ledger_id, &sketch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                let cid = cid_from_write(ContentKind::StatsSketch, &sketch_wr);
                tracing::debug!(
                    %cid,
                    bytes = sketch_wr.size_bytes,
                    entries = sketch_blob.entries.len(),
                    "HLL sketch blob uploaded"
                );
                Some(cid)
            } else {
                None
            };
            let index_stats: fluree_db_core::index_stats::IndexStats = {
                let (id_result, agg_props, _class_counts, _class_properties, _class_ref_targets) =
                    stats_hook.finalize_with_aggregate_properties();

                // Per-graph stats (already the correct type).
                let mut graphs = id_result.graphs;

                // Aggregate properties: convert from p_id-keyed to SID-keyed.
                let properties: Vec<fluree_db_core::index_stats::PropertyStatEntry> = agg_props
                    .iter()
                    .filter_map(|p| {
                        let sid = predicate_sids.get(p.p_id as usize)?;
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

                // Class stats from SPOT merge (per-graph).
                let mut per_graph_classes = if let Some(ref cs) = spot_class_stats {
                    crate::stats::build_class_stat_entries(
                        cs,
                        &predicate_sids,
                        &shared.dt_tags,
                        &dict_addresses.language_tags,
                        &run_dir,
                        store.namespace_codes(),
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                } else {
                    std::collections::HashMap::new()
                };

                // Inject per-graph class stats into each GraphStatsEntry.
                for g in &mut graphs {
                    g.classes = per_graph_classes.remove(&g.g_id);
                }

                // Derive root-level classes as union of all per-graph classes.
                let classes = fluree_db_core::index_stats::union_per_graph_classes(&graphs);

                tracing::info!(
                    property_stats = properties.len(),
                    graph_count = graphs.len(),
                    class_count = classes.as_ref().map_or(0, |c| c.len()),
                    total_flakes = id_result.total_flakes,
                    "stats collected from IdStatsHook"
                );

                fluree_db_core::index_stats::IndexStats {
                    flakes: id_result.total_flakes,
                    size: total_commit_size,
                    properties: if properties.is_empty() {
                        None
                    } else {
                        Some(properties)
                    },
                    classes,
                    graphs: Some(graphs),
                }
            };

            // F.5: Convert DictRefs (string-keyed maps) → DictRefsV5 + GraphArenaRefsV5.
            //
            // numbig and vectors are now per-graph in DictRefs.
            let (dict_refs_v5, graph_arenas) = {
                let dr = dict_addresses.dict_refs;

                let dict_refs = DictRefsV5 {
                    forward_packs: dr.forward_packs,
                    subject_reverse: dr.subject_reverse,
                    string_reverse: dr.string_reverse,
                };

                // Collect all graph IDs that have numbig or vector arenas.
                let mut graph_ids = std::collections::BTreeSet::new();
                for g_id_str in dr.numbig.keys() {
                    if let Ok(g_id) = g_id_str.parse::<u16>() {
                        graph_ids.insert(g_id);
                    }
                }
                for g_id_str in dr.vectors.keys() {
                    if let Ok(g_id) = g_id_str.parse::<u16>() {
                        graph_ids.insert(g_id);
                    }
                }

                let mut arenas: Vec<GraphArenaRefsV5> = graph_ids
                    .into_iter()
                    .map(|g_id| {
                        let g_id_str = g_id.to_string();
                        let numbig: Vec<(u32, ContentId)> = dr
                            .numbig
                            .get(&g_id_str)
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| (k.parse::<u32>().unwrap_or(0), v.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let vectors: Vec<VectorDictRefV5> = dr
                            .vectors
                            .get(&g_id_str)
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| VectorDictRefV5 {
                                        p_id: k.parse::<u32>().unwrap_or(0),
                                        manifest: v.manifest.clone(),
                                        shards: v.shards.clone(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        GraphArenaRefsV5 {
                            g_id,
                            numbig,
                            vectors,
                            spatial: Vec::new(),
                            fulltext: vec![],
                        }
                    })
                    .collect();
                // Merge spatial arena refs into graph arenas.
                for (g_id, spatial_refs) in spatial_arena_refs {
                    if let Some(ga) = arenas.iter_mut().find(|ga| ga.g_id == g_id) {
                        ga.spatial = spatial_refs;
                    } else {
                        arenas.push(GraphArenaRefsV5 {
                            g_id,
                            numbig: vec![],
                            vectors: vec![],
                            spatial: spatial_refs,
                            fulltext: vec![],
                        });
                    }
                }
                // Merge fulltext arena refs into graph arenas.
                for (g_id, ft_refs) in fulltext_arena_refs {
                    if let Some(ga) = arenas.iter_mut().find(|ga| ga.g_id == g_id) {
                        ga.fulltext = ft_refs;
                    } else {
                        arenas.push(GraphArenaRefsV5 {
                            g_id,
                            numbig: vec![],
                            vectors: vec![],
                            spatial: vec![],
                            fulltext: ft_refs,
                        });
                    }
                }

                (dict_refs, arenas)
            };

            // F.6: Build IndexRootV5 (binary IRB1) from upload results.
            let ns_codes: std::collections::BTreeMap<u16, String> = store
                .namespace_codes()
                .iter()
                .map(|(&k, v)| (k, v.clone()))
                .collect();

            let mut root = IndexRootV5 {
                ledger_id: ledger_id.clone(),
                index_t: store.max_t(),
                base_t: store.base_t(),
                subject_id_encoding: dict_addresses.subject_id_encoding,
                namespace_codes: ns_codes,
                predicate_sids,
                graph_iris: dict_addresses.graph_iris,
                datatype_iris: dict_addresses.datatype_iris,
                language_tags: dict_addresses.language_tags,
                dict_refs: dict_refs_v5,
                subject_watermarks: dict_addresses.subject_watermarks,
                string_watermark: dict_addresses.string_watermark,
                total_commit_size,
                total_asserts,
                total_retracts,
                graph_arenas,
                default_graph_orders: uploaded_indexes.default_graph_orders,
                named_graphs: uploaded_indexes.named_graphs,
                stats: Some(index_stats),
                schema: None, // schema: requires predicate definitions (future)
                prev_index: None,
                garbage: None,
                sketch_ref,
            };

            // F.7: Compute garbage and link prev_index for GC chain.
            //
            // Strategy: use all_cas_ids() on both old and new roots to
            // compute the set difference. Both sides use the same enumeration.
            //
            // IRB1-only: decode the previous root to compute garbage set difference.
            // If the previous root cannot be decoded, GC chain starts fresh here.
            if let Some(prev_id) = prev_root_id.as_ref() {
                let prev_bytes = content_store.get(prev_id).await.ok();
                let prev_cas_ids: Option<(i64, Vec<ContentId>)> =
                    prev_bytes.as_deref().and_then(|b| {
                        IndexRootV5::decode(b)
                            .ok()
                            .map(|v5| (v5.index_t, v5.all_cas_ids()))
                    });

                if let Some((prev_t, old_ids_vec)) = prev_cas_ids {
                    let old_ids: std::collections::HashSet<ContentId> =
                        old_ids_vec.into_iter().collect();
                    let new_ids: std::collections::HashSet<ContentId> =
                        root.all_cas_ids().into_iter().collect();
                    let garbage_cids: Vec<ContentId> =
                        old_ids.difference(&new_ids).cloned().collect();

                    let garbage_count = garbage_cids.len();

                    let garbage_strings: Vec<String> =
                        garbage_cids.iter().map(|c| c.to_string()).collect();
                    root.garbage = gc::write_garbage_record(
                        &storage,
                        &ledger_id,
                        store.max_t(),
                        garbage_strings,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                    .map(|id| BinaryGarbageRef { id });

                    root.prev_index = Some(BinaryPrevIndexRef {
                        t: prev_t,
                        id: prev_id.clone(),
                    });

                    tracing::info!(
                        prev_t,
                        garbage_count,
                        "GC chain linked to previous index root"
                    );
                }
            }

            tracing::info!(
                index_t = root.index_t,
                base_t = root.base_t,
                default_orders = root.default_graph_orders.len(),
                named_graphs = root.named_graphs.len(),
                "binary index built (IRB1), writing CAS root"
            );

            // F.8: Encode IRB1 root and write to CAS.
            let root_bytes = root.encode();
            let write_result = storage
                .content_write_bytes(
                    fluree_db_core::ContentKind::IndexRoot,
                    &ledger_id,
                    &root_bytes,
                )
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            drop(_span_f);

            // Clean up ephemeral tmp_import session directory
            if let Err(e) = std::fs::remove_dir_all(&run_dir) {
                tracing::warn!(?run_dir, %e, "failed to clean up tmp_import session dir");
            }

            // Compute stats from build results
            let total_leaves: usize = build_results
                .iter()
                .flat_map(|(_, r)| r.graphs.iter())
                .map(|g| g.leaf_count as usize)
                .sum();

            // Derive ContentId from the root's content hash
            let root_id = fluree_db_core::ContentId::from_hex_digest(
                fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
                &write_result.content_hash,
            )
            .ok_or_else(|| {
                IndexerError::StorageWrite(format!(
                    "invalid content_hash from write result: {}",
                    write_result.content_hash
                ))
            })?;

            Ok(IndexResult {
                root_id,
                index_t: root.index_t,
                ledger_id: ledger_id.to_string(),
                stats: IndexStats {
                    flake_count: total_records as usize,
                    leaf_count: total_leaves,
                    branch_count: build_results.len(),
                    total_bytes: root_bytes.len(),
                },
            })
        })
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("index build task panicked: {}", e)))?
}
