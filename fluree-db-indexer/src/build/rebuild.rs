//! Full index rebuild pipeline (Phase A..F).
//!
//! Walks the entire commit chain from genesis, resolves all commits into
//! sorted run files, builds per-graph leaf/branch indexes for all sort
//! orders, and writes an `IndexRoot` (FIR6) descriptor to storage.

use fluree_db_binary_index::{GraphArenaRefs, RunRecord, VectorDictRef};
use fluree_db_core::{ContentId, ContentKind, ContentStore, Storage};

use crate::error::{IndexerError, Result};
use crate::run_index;
use crate::{IndexResult, IndexStats, IndexerConfig};

use super::mem_profile as mp;
use super::upload::cid_from_write;
use super::upload_dicts::upload_dicts_from_disk;

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
    let content_store = fluree_db_core::storage::content_store_for(storage.clone(), ledger_id);
    rebuild_index_from_commits_with_store(storage, content_store, ledger_id, record, config).await
}

/// Like [`rebuild_index_from_commits`], but accepts a caller-provided
/// [`ContentStore`] for reading commit blobs. Use this when commit history
/// spans multiple storage namespaces (e.g. rebasing a branch whose commit
/// chain falls through to parent namespaces via `BranchedContentStore`).
pub async fn rebuild_index_from_commits_with_store<S, C>(
    storage: &S,
    commit_store: C,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: ContentStore + Clone + Send + Sync + 'static,
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
    let _prev_root_id = record.index_head_id.clone();
    let commit_t = record.commit_t;
    let handle = tokio::runtime::Handle::current();
    let parent_span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        let _guard = parent_span.enter(); // safe: spawn_blocking pins to one thread
        handle.block_on(async {
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            let content_store = commit_store;

            // Phase spans below use .entered() — safe because block_on inside
            // spawn_blocking pins this async task to a single OS thread.

            mp::log_rss("BASELINE — before Phase A");

            // ---- Phase A: Walk commit chain backward to collect CIDs ----
            let _span_a = tracing::debug_span!("commit_chain_walk").entered();
            let (commit_cids, ledger_split_mode) = {
                const INFO_PROGRESS_EVERY: usize = 500;
                const DEBUG_PROGRESS_EVERY: usize = 100;

                let mut cids = Vec::new();
                let mut current = Some(head_commit_id.clone());
                let mut split_mode = fluree_db_core::ns_encoding::NsSplitMode::default();
                let walk_started = std::time::Instant::now();
                let mut head_t: Option<i64> = None;
                let mut genesis_t: Option<i64> = None;

                while let Some(cid) = current {
                    let bytes = content_store
                        .get(&cid)
                        .await
                        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;
                    let envelope = read_commit_envelope(&bytes)
                        .map_err(|e| IndexerError::StorageRead(e.to_string()))?;
                    // Extract ns_split_mode (last seen = genesis = authoritative).
                    if let Some(mode) = envelope.ns_split_mode {
                        split_mode = mode;
                    }
                    if head_t.is_none() {
                        head_t = Some(envelope.t);
                    }
                    genesis_t = Some(envelope.t);
                    current = envelope.previous_id().cloned();
                    cids.push(cid);

                    let walked = cids.len();
                    if walked % INFO_PROGRESS_EVERY == 0 {
                        tracing::info!(
                            commits_walked = walked,
                            current_t = envelope.t,
                            elapsed_ms = walk_started.elapsed().as_millis() as u64,
                            "Phase A progress: walked commit chain"
                        );
                    } else if walked % DEBUG_PROGRESS_EVERY == 0 {
                        tracing::debug!(
                            commits_walked = walked,
                            current_t = envelope.t,
                            elapsed_ms = walk_started.elapsed().as_millis() as u64,
                            "Phase A progress: walked commit chain"
                        );
                    }
                }

                cids.reverse(); // chronological order (genesis first)
                tracing::info!(
                    commits = cids.len(),
                    genesis_t = ?genesis_t,
                    head_t = ?head_t,
                    elapsed_ms = walk_started.elapsed().as_millis() as u64,
                    "Phase A complete: commit chain collected"
                );
                (cids, split_mode)
            };
            drop(_span_a);

            mp::log_rss("Phase A complete — commit chain collected");
            tracing::info!(
                commit_cids_count = commit_cids.len(),
                commit_cids_bytes = mp::vec_heap_bytes(&commit_cids),
                commit_cids_human = %mp::human(mp::vec_heap_bytes(&commit_cids)),
                "MEM_PROFILE: Phase A data structures"
            );

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

            // Enable schema hierarchy extraction during resolution.
            shared.schema_hook = Some(crate::stats::SchemaExtractor::new());

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
            let resolve_started = std::time::Instant::now();

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
                if (i + 1) % 500 == 0 {
                    tracing::info!(
                        commits_resolved = i + 1,
                        total_commits = commit_cids.len(),
                        t = resolved.t,
                        chunk_flakes = chunk.flake_count(),
                        elapsed_ms = resolve_started.elapsed().as_millis() as u64,
                        "Phase B progress: resolved commits into chunks"
                    );
                }

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

            // ---- MEM_PROFILE: Phase B data structures ----
            {
                mp::log_rss("Phase B complete — all chunks resolved");
                let mut total_records = 0usize;
                let mut total_record_bytes = 0u64;
                for (ci, chunk) in chunks.iter().enumerate() {
                    let rec_bytes = mp::vec_heap_bytes(&chunk.records);
                    total_records += chunk.records.len();
                    total_record_bytes += rec_bytes;
                    tracing::info!(
                        chunk_idx = ci,
                        subjects = chunk.subjects.len(),
                        strings = chunk.strings.len(),
                        records = chunk.records.len(),
                        records_bytes = rec_bytes,
                        records_human = %mp::human(rec_bytes),
                        flake_count = chunk.flake_count,
                        "MEM_PROFILE: Phase B chunk detail"
                    );
                }
                let spatial_count = shared.spatial_hook.as_ref().map_or(0, |h| h.entry_count());
                let fulltext_count = shared.fulltext_hook.as_ref().map_or(0, |h| h.entry_count());
                tracing::info!(
                    total_chunks = chunks.len(),
                    total_records,
                    total_record_bytes,
                    total_record_human = %mp::human(total_record_bytes),
                    spatial_entries = spatial_count,
                    fulltext_entries = fulltext_count,
                    "MEM_PROFILE: Phase B totals"
                );
            }

            drop(_span_b);

            // Finalize schema extraction from rebuild ops.
            let db_schema: Option<fluree_db_core::IndexSchema> = shared
                .schema_hook
                .take()
                .and_then(|ex| ex.finalize(commit_t));

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

            mp::log_rss("Phase C start — before dict merge");
            tracing::info!(
                chunk_records_count = chunk_records.len(),
                chunk_records_total = chunk_records.iter().map(|v| v.len()).sum::<usize>(),
                chunk_records_bytes = chunk_records.iter().map(|v| mp::vec_heap_bytes(v)).sum::<u64>(),
                chunk_records_human = %mp::human(chunk_records.iter().map(|v| mp::vec_heap_bytes(v)).sum::<u64>()),
                "MEM_PROFILE: Phase C chunk_records before merge"
            );

            let (subject_merge, subject_remaps) =
                run_index::dict_merge::merge_subject_dicts(&subject_dicts);
            let (string_merge, string_remaps) =
                run_index::dict_merge::merge_string_dicts(&string_dicts);

            mp::log_rss("Phase C — after dict merge, before remap");

            // Remap spatial entries' chunk-local subject IDs → global sid64.
            // The spatial hook accumulated entries with chunk-local s_id values;
            // spatial_chunk_ranges[ci] = (start, end) into entries for chunk ci.
            let _spatial_entries: Vec<crate::spatial_hook::SpatialEntry> = {
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
            let _fulltext_entries: Vec<crate::fulltext_hook::FulltextEntry> = {
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

            // Records are persisted to .fsc files on disk — free the in-memory
            // copies immediately. For large datasets (e.g. 60M flakes) this
            // reclaims ~2-6 GB of heap before the index build phase.
            drop(chunk_records);
            drop(subject_remaps);
            drop(string_remaps);
            drop(subject_dicts);
            drop(string_dicts);

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

            // ---- Build FLI3/FBR3 + FIR6 root ----
            let _span_v3 = tracing::debug_span!("v3_rebuild").entered();

            // Build OTypeRegistry from custom datatype IRIs.
            let reserved = fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize;
            let custom_dt_iris: Vec<String> = (reserved..shared.datatypes.len() as usize)
                .filter_map(|i| shared.datatypes.resolve(i as u32).map(|s| s.to_string()))
                .collect();
            let registry = fluree_db_core::o_type_registry::OTypeRegistry::new(&custom_dt_iris);

            // Collect all graph IDs: g_id 0 (default) + named graphs (1+).
            // Graph IRI list maps dict_index → graph IRI, where g_id = dict_index + 1
            // (g_id=0 is always the default graph, g_id=1 is always txn-meta).
            let all_g_ids: Vec<u16> = {
                let mut ids = vec![0u16]; // default graph
                for g_idx in 0..shared.graphs.len() {
                    let g_id = (g_idx + 1) as u16;
                    if !ids.contains(&g_id) {
                        ids.push(g_id);
                    }
                }
                ids
            };

            tracing::info!(
                graph_count = all_g_ids.len(),
                g_ids = ?all_g_ids,
                "V3 rebuild: building indexes for all graphs"
            );

            // Phase D-V3: Build FLI3/FBR3 per graph from globally-remapped .fsc files.
            // Each graph gets its own run dir and build call, then results are merged.
            let mut merged_order_results: Vec<(
                fluree_db_binary_index::format::run_record::RunSortOrder,
                crate::run_index::build::index_build::IndexBuildResult,
            )> = Vec::new();
            let mut total_rows = 0u64;
            let mut total_remap_ms = 0u128;
            let mut total_build_ms = 0u128;

            for &g_id in &all_g_ids {
                mp::log_rss(&format!("Phase D-V3 — before building graph g_id={g_id}"));
                let v3_run_dir = run_dir.join(format!("v3_runs_g{g_id}"));
                std::fs::create_dir_all(&v3_run_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                let v3_config = crate::BuildConfig {
                    run_dir: v3_run_dir,
                    index_dir: index_dir.clone(),
                    g_id,
                    leaflet_target_rows: config.leaflet_rows,
                    leaf_target_rows: config.leaflet_rows * config.leaflets_per_leaf,
                    zstd_level: 1,
                    run_budget_bytes: config.run_budget_bytes,
                    worker_count: 1,
                    remap_progress: None,
                    build_progress: None,
                    stage_marker: None,
                };

                let v3_result = crate::build_indexes_from_remapped_commits(
                    &sorted_commit_infos,
                    &registry,
                    &v3_config,
                )
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                tracing::info!(
                    g_id,
                    total_rows = v3_result.total_rows,
                    orders = v3_result.order_results.len(),
                    remap_ms = v3_result.remap_elapsed.as_millis(),
                    build_ms = v3_result.build_elapsed.as_millis(),
                    "Phase D-V3: graph indexes built"
                );

                if g_id == 0 {
                    total_rows = v3_result.total_rows;
                }
                total_remap_ms += v3_result.remap_elapsed.as_millis();
                total_build_ms += v3_result.build_elapsed.as_millis();

                // Merge order results: append graph results into matching orders.
                for (order, order_result) in v3_result.order_results {
                    if let Some((_, existing)) =
                        merged_order_results.iter_mut().find(|(o, _)| *o == order)
                    {
                        existing.graphs.extend(order_result.graphs);
                        existing.total_rows += order_result.total_rows;
                    } else {
                        merged_order_results.push((order, order_result));
                    }
                }
            }

            // Wrap merged results into a BuildResult for the upload path.
            let v3_result = crate::BuildResult {
                order_results: merged_order_results,
                total_rows,
                total_remapped: 0,
                remap_elapsed: std::time::Duration::from_millis(total_remap_ms as u64),
                build_elapsed: std::time::Duration::from_millis(total_build_ms as u64),
            };

            tracing::info!(
                total_rows = v3_result.total_rows,
                graphs = all_g_ids.len(),
                "Phase D-V3 complete: all graph indexes built"
            );
            mp::log_rss("Phase D-V3 complete — before stats collection");

            // ---- Phase D-V3 stats: Streaming HLL + class stats ----
            //
            // Uses two separate passes over the .fsc files:
            //   Pass 1 (HLL): feeds ALL records to IdStatsHook in HLL-only mode
            //                  (no per-subject maps — bounded memory).
            //   Pass 2 (class): k-way merges .fsc files, deduplicates, feeds
            //                   winning assertions to SpotClassStatsCollector
            //                   (O(1) per-subject, class-level accumulators).
            //
            // This replaces the old approach that accumulated ~16 GB of per-subject
            // HashMaps. Total stats-phase memory is now ~200 MB - 1 GB.

            let rdf_type_p_id = shared
                .predicates
                .get_or_insert("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

            // ---- Pass 1: HLL sketches (all records, no ordering needed) ----
            let mut stats_hook = crate::stats::IdStatsHook::new_hll_only();
            stats_hook.set_rdf_type_p_id(rdf_type_p_id);

            for info in &sorted_commit_infos {
                let reader = run_index::SpoolReader::open(&info.path, info.record_count)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                for result in reader {
                    let record = result.map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let v2 = fluree_db_binary_index::format::run_record_v2::RunRecordV2::from_v1(
                        &record, &registry,
                    );
                    let sr = crate::stats::stats_record_from_v2(&v2, record.op);
                    stats_hook.on_record(&sr);
                }
            }

            mp::log_rss("Phase D stats — HLL pass complete (hll_only mode)");
            tracing::info!(
                properties_count = stats_hook.properties().len(),
                class_counts_entries = stats_hook.class_count_deltas().len(),
                "MEM_PROFILE: IdStatsHook HLL-only sizes"
            );

            // Upload HLL sketches to CAS.
            let sketch_ref = {
                let sketch_blob =
                    crate::stats::HllSketchBlob::from_properties(commit_t, stats_hook.properties());
                if !sketch_blob.entries.is_empty() {
                    let sketch_bytes = sketch_blob.to_json_bytes().map_err(|e| {
                        IndexerError::StorageWrite(format!("sketch serialize: {e}"))
                    })?;
                    let sketch_wr = storage
                        .content_write_bytes(ContentKind::StatsSketch, &ledger_id, &sketch_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let cid = cid_from_write(ContentKind::StatsSketch, &sketch_wr);
                    tracing::info!(
                        %cid,
                        bytes = sketch_wr.size_bytes,
                        entries = sketch_blob.entries.len(),
                        "Phase D-V3 stats: HLL sketch uploaded"
                    );
                    Some(cid)
                } else {
                    None
                }
            };

            // Finalize HLL stats (no per-subject maps to move — hll_only mode).
            let id_stats_result = stats_hook.finalize();

            // ---- Pass 2: Streaming class stats via k-way merge ----
            //
            // Build ClassBitsetTable from .types sidecars (global IDs), then
            // k-way merge .fsc files in cmp_v2_g_spot order with dedup. Feed
            // deduped winning assertions to SpotClassStatsCollector.

            let types_paths: Vec<std::path::PathBuf> = sorted_commit_infos
                .iter()
                .filter_map(|info| info.types_map_path.clone())
                .collect();
            let class_bitset =
                crate::run_index::build::ClassBitsetTable::build_from_global_types(&types_paths)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            mp::log_rss("Phase D stats — before streaming class stats pass");

            let spot_class_stats = {
                use crate::run_index::build::SpotClassStatsCollector;
                use crate::run_index::runs::spool::V1SpoolMergeAdapter;
                use fluree_db_binary_index::format::run_record_v2::cmp_v2_g_spot;

                let mut collector = SpotClassStatsCollector::new(rdf_type_p_id, class_bitset);

                // Open V1 spool merge adapters for all .fsc files.
                let mut streams: Vec<V1SpoolMergeAdapter> = Vec::with_capacity(sorted_commit_infos.len());
                for info in &sorted_commit_infos {
                    let adapter = V1SpoolMergeAdapter::open(
                        &info.path,
                        info.record_count,
                        registry.clone(),
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    streams.push(adapter);
                }

                let mut merge =
                    crate::run_index::build::merge::KWayMerge::new(streams, cmp_v2_g_spot)
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                // Iterate with dedup: next_deduped() returns the winning record
                // per identity group (highest t wins). Feed assertions to collector.
                while let Some((winner, op)) = merge
                    .next_deduped()
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                {
                    if op == 1 {
                        collector.on_record(&winner);
                    }
                }

                collector.finish()
            };

            mp::log_rss("Phase D stats — streaming class stats complete");
            tracing::info!(
                class_counts = spot_class_stats.class_counts.len(),
                class_prop_dts = spot_class_stats.class_prop_dts.len(),
                class_prop_refs = spot_class_stats.class_prop_refs.len(),
                "MEM_PROFILE: SpotClassStats sizes"
            );

            // ---- Build IndexStats for FIR6 root ----
            let trie_for_stats =
                fluree_db_core::PrefixTrie::from_namespace_codes(&shared.ns_prefixes);
            let db_stats = {
                use fluree_db_core::index_stats as is;

                let properties = crate::stats::aggregate_property_entries_from_graphs(
                    &id_stats_result.graphs,
                    &trie_for_stats,
                    |p_id| shared.predicates.resolve(p_id).map(ToString::to_string),
                );

                // Convert SpotClassStats → per-graph ClassStatEntry using the
                // existing build_class_stat_entries() (shared with import path).
                let predicate_sids: Vec<(u16, String)> = (0..shared.predicates.len())
                    .map(|p_id| {
                        let iri = shared.predicates.resolve(p_id).unwrap_or("");
                        match trie_for_stats.longest_match(iri) {
                            Some((code, prefix_len)) => (code, iri[prefix_len..].to_string()),
                            None => (0u16, iri.to_string()),
                        }
                    })
                    .collect();

                let language_tags: Vec<String> = {
                    let mut tags: Vec<(u16, String)> = shared
                        .languages
                        .iter()
                        .map(|(id, tag)| (id, tag.to_string()))
                        .collect();
                    tags.sort_by_key(|(id, _)| *id);
                    tags.into_iter().map(|(_, tag)| tag).collect()
                };

                let mut per_graph_classes = crate::stats::build_class_stat_entries(
                    &spot_class_stats,
                    &predicate_sids,
                    &shared.dt_tags,
                    &language_tags,
                    &run_dir,
                    &shared.ns_prefixes,
                )
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                mp::log_rss("Phase D stats — after build_class_stat_entries");

                // Attach class stats onto per-graph stats entries.
                let mut final_graphs = id_stats_result.graphs;
                for g in &mut final_graphs {
                    if let Some(mut classes) = per_graph_classes.remove(&g.g_id) {
                        classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                        g.classes = Some(classes);
                    }
                }

                let root_classes =
                    fluree_db_core::index_stats::union_per_graph_classes(&final_graphs);

                is::IndexStats {
                    flakes: id_stats_result.total_flakes,
                    size: total_commit_size,
                    properties: Some(properties),
                    classes: root_classes,
                    graphs: Some(final_graphs),
                }
            };

            tracing::info!(
                total_flakes = db_stats.flakes,
                property_count = db_stats.properties.as_ref().map_or(0, |p| p.len()),
                graph_count = db_stats.graphs.as_ref().map_or(0, |g| g.len()),
                "Phase D-V3 stats: collected"
            );

            mp::log_rss("Phase D stats complete — before Phase E upload");

            // Phase E-V3: Upload V3 artifacts to CAS.
            let v3_uploaded =
                super::upload::upload_indexes_to_cas(&storage, &ledger_id, &v3_result)
                    .instrument(tracing::debug_span!("upload_v3_indexes"))
                    .await?;

            mp::log_rss("Phase E complete — before Phase F");

            // Phase F-V3: Upload dicts + assemble FIR6 root.
            let uploaded_dicts =
                upload_dicts_from_disk(&storage, &ledger_id, &run_dir, &shared.ns_prefixes, false)
                    .instrument(tracing::debug_span!("upload_dicts_v3"))
                    .await?;

            // Build namespace codes BTreeMap from shared.ns_prefixes.
            let ns_codes: std::collections::BTreeMap<u16, String> = shared
                .ns_prefixes
                .iter()
                .map(|(&k, v)| (k, v.clone()))
                .collect();

            // Build predicate_sids from shared.predicates + PrefixTrie.
            let trie = fluree_db_core::PrefixTrie::from_namespace_codes(&shared.ns_prefixes);
            let predicate_sids: Vec<(u16, String)> = (0..shared.predicates.len())
                .map(|p_id| {
                    let iri = shared.predicates.resolve(p_id).unwrap_or("");
                    match trie.longest_match(iri) {
                        Some((code, prefix_len)) => (code, iri[prefix_len..].to_string()),
                        None => (0u16, iri.to_string()),
                    }
                })
                .collect();

            // Datatype and language tag lists for the root.
            let datatype_iris = uploaded_dicts.datatype_iris.clone();
            let language_tags = uploaded_dicts.language_tags.clone();

            // Graph arenas (numbig, vectors) — build from uploaded_dicts CIDs.
            let graph_arenas: Vec<GraphArenaRefs> = {
                let mut graph_ids = std::collections::BTreeSet::new();
                for g_id_str in uploaded_dicts.numbig.keys() {
                    if let Ok(g_id) = g_id_str.parse::<u16>() {
                        graph_ids.insert(g_id);
                    }
                }
                for g_id_str in uploaded_dicts.vectors.keys() {
                    if let Ok(g_id) = g_id_str.parse::<u16>() {
                        graph_ids.insert(g_id);
                    }
                }
                graph_ids
                    .into_iter()
                    .map(|g_id| {
                        let g_id_str = g_id.to_string();
                        let numbig: Vec<(u32, ContentId)> = uploaded_dicts
                            .numbig
                            .get(&g_id_str)
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| (k.parse::<u32>().unwrap_or(0), v.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let vectors: Vec<VectorDictRef> = uploaded_dicts
                            .vectors
                            .get(&g_id_str)
                            .map(|m| m.values().cloned().collect())
                            .unwrap_or_default();
                        GraphArenaRefs {
                            g_id,
                            numbig,
                            vectors,
                            spatial: Vec::new(),
                            fulltext: vec![],
                        }
                    })
                    .collect()
            };

            // Compute total_rows for stats.
            let total_rows = v3_result.total_rows;

            let fir6_inputs = super::root_assembly::Fir6Inputs {
                ledger_id: ledger_id.clone(),
                index_t: commit_t,
                namespace_codes: ns_codes,
                // Namespace reconciliation at publish time: `shared.ns_prefixes` is the
                // commit-derived namespace table at `commit_t` (after applying all
                // commit namespace deltas in forward order with bimap conflict validation).
                // Root assembly will diff this against the root's materialized table
                // and fail fast on divergence (indexer/publisher bug).
                commit_derived_ns: shared.ns_prefixes.clone(),
                ns_split_mode: ledger_split_mode,
                predicate_sids,
                uploaded_dicts,
                v3_uploaded,
                graph_arenas,
                datatype_iris,
                language_tags,
                total_commit_size,
                total_asserts,
                total_retracts,
                db_stats: Some(db_stats),
                db_schema,
                sketch_ref,
            };

            let result = super::root_assembly::encode_and_write_root_v6(
                &storage,
                fir6_inputs,
                None, // GC chain deferred for V3 milestone.
                IndexStats {
                    flake_count: total_rows as usize,
                    leaf_count: v3_result
                        .order_results
                        .iter()
                        .flat_map(|(_, r)| r.graphs.iter())
                        .map(|g| g.leaf_infos.len())
                        .sum(),
                    branch_count: v3_result.order_results.len(),
                    total_bytes: 0, // Will be filled from root_bytes.
                },
            )
            .await?;

            drop(_span_v3);

            mp::log_rss("Phase F complete — FIR6 root written");

            // Clean up ephemeral tmp_import session directory.
            if let Err(e) = std::fs::remove_dir_all(&run_dir) {
                tracing::warn!(?run_dir, %e, "failed to clean up tmp_import session dir");
            }

            Ok(result)
        })
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("index build task panicked: {}", e)))?
}
