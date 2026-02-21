//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Binary columnar index building (`run_index` module)
//! - Background indexing orchestration
//! - Namespace delta replay
//! - Garbage collection support
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! The binary index pipeline (`run_index::index_build`) is the sole indexing path.

pub mod config;
pub mod dict_tree;
pub mod error;
pub mod gc;
pub mod hll;
pub mod orchestrator;
pub mod run_index;
pub mod spatial_hook;
pub mod stats;

// Re-export main types
pub use config::IndexerConfig;
pub use error::{IndexerError, Result};
pub use gc::{
    clean_garbage, load_garbage_record, write_garbage_record, CleanGarbageConfig,
    CleanGarbageResult, GarbageRecord, DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS,
};
#[cfg(feature = "embedded-orchestrator")]
pub use orchestrator::{
    maybe_refresh_after_commit, require_refresh_before_commit, PostCommitIndexResult,
};
pub use orchestrator::{
    BackgroundIndexerWorker, IndexCompletion, IndexOutcome, IndexPhase, IndexStatusSnapshot,
    IndexerHandle, IndexerOrchestrator,
};
pub use stats::{IndexStatsHook, NoOpStatsHook, StatsArtifacts, StatsSummary};

// Note: The following types/functions are defined in this module and are automatically public:
// - build_index_for_ledger (nameservice-aware entry point)
// - rebuild_index_from_commits (direct entry point given an NsRecord)
// - CURRENT_INDEX_VERSION

use fluree_db_core::{ContentId, ContentKind, ContentStore, ContentWriteResult, GraphId, Storage};
use fluree_db_nameservice::{NameService, Publisher};

/// Derive a `ContentId` from a `ContentWriteResult`.
///
/// Every `content_write_bytes{,_with_hash}` call returns a SHA-256 hex digest.
/// This helper wraps `ContentId::from_hex_digest` so callers don't repeat
/// the pattern.
fn cid_from_write(kind: ContentKind, result: &ContentWriteResult) -> ContentId {
    ContentId::from_hex_digest(kind.to_codec(), &result.content_hash)
        .expect("storage produced a valid SHA-256 hex digest")
}

/// Upload a single dict blob (already in memory) to CAS and return (cid, write_result).
async fn upload_dict_blob<S: Storage>(
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
async fn upload_dict_file<S: Storage>(
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

#[inline]
fn flush_reverse_leaf<F>(
    leaf_offsets: &mut Vec<u32>,
    leaf_data: &mut Vec<u8>,
    first_key: &mut Option<Vec<u8>>,
    chunk_bytes: &mut usize,
    mut last_key: F,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)>
where
    F: FnMut() -> Vec<u8>,
{
    if leaf_offsets.is_empty() {
        return None;
    }
    let entry_count = leaf_offsets.len() as u32;
    let header_size = 8;
    let offset_table_size = leaf_offsets.len() * 4;
    let total = header_size + offset_table_size + leaf_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&dict_tree::reverse_leaf::REVERSE_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());
    for off in leaf_offsets.iter() {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(leaf_data);
    debug_assert_eq!(buf.len(), total);

    let fk = first_key.take().unwrap_or_default();
    let lk = last_key();

    leaf_offsets.clear();
    leaf_data.clear();
    *chunk_bytes = 0;

    Some((buf, fk, lk))
}

/// Normalize a ledger ID for comparison purposes
///
/// Handles both canonical `name:branch` format and storage-path style `name/branch`.
/// This is necessary because the address may be stored in either format depending
/// on the code path that created it.
///
/// # Algorithm
///
/// 1. If the address contains `:`, use canonical parsing via `normalize_ledger_id`
///    - If parsing fails, return the original string unchanged (don't manufacture addresses)
/// 2. If the address has exactly one `/` (storage-path style), convert to `name:branch`
/// 3. Falls back to treating the whole string as the name with default branch
///
/// This ONLY treats a single `/` as a branch separator when there's no `:`.
/// For ledger names that legitimately contain `/` (e.g., "org/project:main"),
/// the canonical format with `:` must be used.
#[cfg(test)]
fn normalize_id_for_comparison(ledger_id: &str) -> String {
    use fluree_db_core::ledger_id::{format_ledger_id, normalize_ledger_id, DEFAULT_BRANCH};

    // If it has a colon, use canonical parsing
    if ledger_id.contains(':') {
        // If canonical parse succeeds, use it. If it fails (malformed address),
        // return the original string unchanged rather than manufacturing a new address.
        return normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
    }

    // Check for storage-path style "name/branch" (exactly one slash, no colon)
    let slash_count = ledger_id.chars().filter(|c| *c == '/').count();
    if slash_count == 1 {
        if let Some(slash_idx) = ledger_id.rfind('/') {
            let name = &ledger_id[..slash_idx];
            let branch = &ledger_id[slash_idx + 1..];
            if !name.is_empty() && !branch.is_empty() {
                return format_ledger_id(name, branch);
            }
        }
    }

    // Last resort: treat entire string as name with default branch
    format_ledger_id(ledger_id, DEFAULT_BRANCH)
}

/// Result of building an index
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Content identifier of the index root (derived from SHA-256 of root bytes).
    ///
    /// Always present — derived from the content hash of the index root during build,
    /// or from the persisted CID / address hash during early-return.
    pub root_id: fluree_db_core::ContentId,
    /// Transaction time the index is current through
    pub index_t: i64,
    /// Ledger ID (name:branch format)
    pub ledger_id: String,
    /// Index build statistics
    pub stats: IndexStats,
}

/// Statistics from index building
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (after dedup)
    pub flake_count: usize,
    /// Number of leaf nodes created
    pub leaf_count: usize,
    /// Number of branch nodes created
    pub branch_count: usize,
    /// Total bytes written
    pub total_bytes: usize,
}

/// Current index version for compatibility checking
pub const CURRENT_INDEX_VERSION: i32 = 2;

/// External indexer entry point
///
/// Builds a binary columnar index from the commit chain. The pipeline:
/// 1. Walks the commit chain and generates sorted run files
/// 2. Builds per-graph leaf/branch indexes for all sort orders
/// 3. Creates an `IndexRootV5` (IRB1) descriptor and writes it to storage
///
/// Returns early if the index is already current (no work needed).
/// Use `rebuild_index_from_commits` directly to force a rebuild regardless.
pub async fn build_index_for_ledger<S, N>(
    storage: &S,
    nameservice: &N,
    ledger_id: &str,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    let span = tracing::info_span!("index_build", ledger_id = ledger_id);
    let _guard = span.enter();

    // Look up the ledger record
    let record = nameservice
        .lookup(ledger_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(ledger_id.to_string()))?;

    // If index is already current, return it
    if let Some(ref root_id) = record.index_head_id {
        if record.index_t >= record.commit_t {
            return Ok(IndexResult {
                root_id: root_id.clone(),
                index_t: record.index_t,
                ledger_id: ledger_id.to_string(),
                stats: IndexStats::default(),
            });
        }
    }

    // Try incremental indexing if conditions are met
    let commit_gap = record.commit_t - record.index_t;
    let can_incremental = config.incremental_enabled
        && record.index_head_id.is_some()
        && record.index_t > 0
        && commit_gap <= config.incremental_max_commits as i64;

    if can_incremental {
        tracing::info!(
            from_t = record.index_t,
            to_t = record.commit_t,
            commit_gap = commit_gap,
            "attempting incremental index"
        );
        match incremental_index_from_root(storage, ledger_id, &record, config.clone()).await {
            Ok(result) => {
                return Ok(result);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "incremental indexing failed, falling back to full rebuild"
                );
            }
        }
    } else if config.incremental_enabled && record.index_head_id.is_some() && record.index_t > 0 {
        tracing::info!(
            commit_gap = commit_gap,
            max = config.incremental_max_commits,
            "commit gap exceeds incremental limit, using full rebuild"
        );
    }

    rebuild_index_from_commits(storage, ledger_id, &record, config).await
}

/// Build a binary index from an existing nameservice record.
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

    tokio::task::spawn_blocking(move || {
        handle.block_on(async {
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Build a content store bridge for CID → address resolution
            let content_store =
                fluree_db_core::storage::content_store_for(storage.clone(), &ledger_id);

            // ---- Phase A: Walk commit chain backward to collect CIDs ----
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

            // ---- Phase B: Resolve commits into batched chunks ----
            let mut shared = SharedResolverState::new();

            // Pre-insert rdf:type into predicate dictionary so class tracking
            // works from the very first commit.
            let rdf_type_p_id = shared.predicates.get_or_insert(fluree_vocab::rdf::TYPE);

            // Enable spatial geometry collection during resolution.
            shared.spatial_hook = Some(crate::spatial_hook::SpatialHook::new());

            let chunk_max_flakes: u64 = 5_000_000; // ~5M flakes per chunk
            let mut chunk = RebuildChunk::new();
            let mut chunks: Vec<RebuildChunk> = Vec::new();

            // Track spatial entry ranges per chunk for subject ID remapping.
            // Each entry is (start_idx, end_idx) into spatial_hook.entries().
            let mut spatial_chunk_ranges: Vec<(usize, usize)> = Vec::new();
            let mut spatial_cursor: usize = 0;

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
                    chunks.push(std::mem::take(&mut chunk));
                }

                let bytes = content_store
                    .get(cid)
                    .await
                    .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;

                let resolved = shared
                    .resolve_commit_into_chunk(&bytes, &cid.digest_hex(), &mut chunk)
                    .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

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
                    chunks.push(std::mem::take(&mut chunk));
                }
            }

            // Push final chunk if non-empty.
            if !chunk.is_empty() {
                let spatial_end = shared.spatial_hook.as_ref().map_or(0, |h| h.entry_count());
                spatial_chunk_ranges.push((spatial_cursor, spatial_end));
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

            // ---- Phase C: Dict merge → global IDs + remap tables ----
            // Separate dicts from records so merge can borrow owned dicts.
            let mut subject_dicts = Vec::with_capacity(chunks.len());
            let mut string_dicts = Vec::with_capacity(chunks.len());
            let mut chunk_records: Vec<Vec<run_index::RunRecord>> =
                Vec::with_capacity(chunks.len());

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
                records.sort_unstable_by(run_index::run_record::cmp_g_spot);

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
                    run_index::numbig_dict::write_numbig_arena(
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
                    let shard_paths =
                        run_index::vector_arena::write_vector_shards(&vec_dir, p_id, arena)
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let shard_infos: Vec<run_index::vector_arena::ShardInfo> = shard_paths
                        .iter()
                        .enumerate()
                        .map(|(i, path)| {
                            let cap = run_index::vector_arena::SHARD_CAPACITY;
                            let start = i as u32 * cap;
                            let count = (arena.len() - start).min(cap);
                            run_index::vector_arena::ShardInfo {
                                cas: path.display().to_string(),
                                count,
                            }
                        })
                        .collect();
                    run_index::vector_arena::write_vector_manifest(
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

            // ---- Phase D: Build SPOT from sorted commits ----
            // Records are already remapped to global IDs, so use identity remaps.
            let p_width =
                run_index::leaflet::p_width_for_max(shared.predicates.len().saturating_sub(1));
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
                leaflet_rows: 25_000,
                leaflets_per_leaf: 10,
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

            let secondary_orders = run_index::RunSortOrder::secondary_orders();
            let phase_e_start = std::time::Instant::now();

            // E.1: Read sorted commit files, partition by g_id, collect stats,
            //      and write per-graph run files via MultiOrderRunWriter.
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
                Vec<(run_index::RunSortOrder, run_index::RunWriterResult)>,
            > = std::collections::BTreeMap::new();

            for (g_id, writer) in graph_writers {
                let results = writer
                    .finish(&mut lang_dict)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                graph_run_results.insert(g_id, results);
            }

            // E.3: Build per-graph secondary indexes from the run files.
            let mut secondary_results: Vec<(run_index::RunSortOrder, run_index::IndexBuildResult)> =
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
                        leaflet_rows: 25_000,
                        leaflets_per_leaf: 10,
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
            let mut build_results: Vec<(run_index::RunSortOrder, run_index::IndexBuildResult)> =
                Vec::new();
            build_results.push((run_index::RunSortOrder::Spot, spot_result));
            build_results.extend(secondary_results);

            let total_records: u64 = build_results
                .iter()
                .filter(|(o, _)| *o == run_index::RunSortOrder::Spot)
                .flat_map(|(_, r)| r.graphs.iter())
                .map(|g| g.total_rows)
                .sum();

            tracing::info!(
                secondary_count = build_results.len() - 1,
                "Phase E complete: secondary indexes built"
            );

            // ---- Phase E.5: Build spatial indexes from collected geometry entries ----
            let spatial_arena_refs: Vec<(GraphId, Vec<run_index::SpatialArenaRefV5>)> = {
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

            // ---- Phase F: Upload artifacts to CAS and write v4 root ----

            // F.1: Load store for max_t / base_t / namespace_codes
            let store = run_index::BinaryIndexStore::load(&run_dir, &index_dir)
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
                    .await?;

            // F.3: Upload index artifacts (branches + leaves) to CAS.
            // Default graph (g_id=0): leaves uploaded, branch NOT (inline in root).
            // Named graphs: both branches and leaves uploaded.
            let uploaded_indexes =
                upload_indexes_to_cas(&storage, &ledger_id, &build_results).await?;

            // F.4: Build HLL sketch blob and IndexStats from IdStatsHook + class stats.

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

                let dict_refs = run_index::DictRefsV5 {
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

                let mut arenas: Vec<run_index::GraphArenaRefsV5> = graph_ids
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
                        let vectors: Vec<run_index::VectorDictRefV5> = dr
                            .vectors
                            .get(&g_id_str)
                            .map(|m| {
                                m.iter()
                                    .map(|(k, v)| run_index::VectorDictRefV5 {
                                        p_id: k.parse::<u32>().unwrap_or(0),
                                        manifest: v.manifest.clone(),
                                        shards: v.shards.clone(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        run_index::GraphArenaRefsV5 {
                            g_id,
                            numbig,
                            vectors,
                            spatial: Vec::new(),
                        }
                    })
                    .collect();
                // Merge spatial arena refs into graph arenas.
                for (g_id, spatial_refs) in spatial_arena_refs {
                    if let Some(ga) = arenas.iter_mut().find(|ga| ga.g_id == g_id) {
                        ga.spatial = spatial_refs;
                    } else {
                        arenas.push(run_index::GraphArenaRefsV5 {
                            g_id,
                            numbig: vec![],
                            vectors: vec![],
                            spatial: spatial_refs,
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

            let mut root = run_index::IndexRootV5 {
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
                        run_index::IndexRootV5::decode(b)
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
                    .map(|id| run_index::BinaryGarbageRef { id });

                    root.prev_index = Some(run_index::BinaryPrevIndexRef {
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

/// Build an incremental index from an existing root, resolving only new commits.
///
/// This is the fast path: instead of rebuilding from genesis, it loads the
/// existing `IndexRootV5`, resolves only commits `(root.index_t .. record.commit_t]`,
/// merges novelty into affected leaves, updates dictionaries incrementally,
/// and publishes a new root that references mostly-unchanged CAS artifacts.
///
/// Falls back to full rebuild on any error — correctness is never at risk.
///
/// Uses the same `spawn_blocking` + `handle.block_on()` pattern as
/// `rebuild_index_from_commits` because internal dictionaries contain non-Send
/// types held across await points.
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
    use run_index::branch::read_branch_v2_from_bytes;
    use run_index::incremental_branch::{
        update_branch, IncrementalBranchConfig, IncrementalBranchError,
    };
    use run_index::incremental_resolve::{resolve_incremental_commits, IncrementalResolveConfig};
    use run_index::incremental_root::IncrementalRootBuilder;
    use run_index::run_record::{cmp_for_order, RunSortOrder};
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
    let mut by_graph: std::collections::BTreeMap<u16, Vec<&run_index::RunRecord>> =
        std::collections::BTreeMap::new();
    for record in &novelty.records {
        by_graph.entry(record.g_id).or_default().push(record);
    }

    // Format-level constants for leaflet/leaf sizing.
    // These match the full-rebuild pipeline and are tied to the FLI2 format.
    let branch_config = IncrementalBranchConfig {
        zstd_level: 1,
        leaflet_split_rows: 37_500,
        leaflet_target_rows: 25_000,
        leaflets_per_leaf: 10,
        leaf_split_leaflets: 20,
    };

    let p_width = run_index::leaflet::p_width_for_max(
        novelty.shared.predicates.len().saturating_sub(1) as u32,
    );

    // Detect width promotion: if the new predicate or datatype count requires
    // a wider encoding than the base root used, fall back to full rebuild.
    // Existing leaves are encoded with the old width; re-encoding merged
    // leaflets with a new width within the same leaf file is not supported.
    let base_pred_count = base_root.predicate_sids.len() as u32;
    let base_p_width = run_index::leaflet::p_width_for_max(base_pred_count.saturating_sub(1));
    if p_width > base_p_width {
        return Err(IndexerError::IncrementalAbort(format!(
            "predicate width promotion ({base_p_width} -> {p_width}) requires full rebuild"
        )));
    }
    let dt_count = novelty.shared.datatypes.len() as u32;
    let base_dt_count = base_root.datatype_iris.len() as u32;
    let dt_width = run_index::leaflet::dt_width_for_max(dt_count.saturating_sub(1));
    let base_dt_width = run_index::leaflet::dt_width_for_max(base_dt_count.saturating_sub(1));
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
        graph_records: Arc<Vec<run_index::RunRecord>>,
        existing_manifest: Option<run_index::branch::BranchManifest>,
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
        all_pack_refs: Vec<run_index::PackBranchEntry>,
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
    let mut shared_graph_records: std::collections::BTreeMap<u16, Arc<Vec<run_index::RunRecord>>> =
        std::collections::BTreeMap::new();
    for (&g_id, refs) in &by_graph {
        let records: Vec<run_index::RunRecord> = refs.iter().map(|r| **r).collect();
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
                    .map(|o| run_index::branch::BranchManifest {
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
                let mut sorted_records: Vec<run_index::RunRecord> = (*item.graph_records).clone();
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
                dict_tree::incremental::build_incremental_string_packs(&existing_refs_inner, &refs)
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
                updated_refs.push(run_index::PackBranchEntry {
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
            let existing_ns_refs: Vec<run_index::PackBranchEntry> = base_root
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
                    dict_tree::incremental::build_incremental_subject_packs_for_ns(
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
                    updated_refs.push(run_index::PackBranchEntry {
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
        use run_index::index_root::{GraphArenaRefsV5, VectorDictRefV5};
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

                    let bytes = run_index::numbig_dict::write_numbig_arena_to_bytes(arena)
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
                            run_index::vector_arena::read_vector_manifest(&old_manifest_bytes)
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
                        if old_manifest.shard_capacity != run_index::vector_arena::SHARD_CAPACITY {
                            return Err(IndexerError::IncrementalAbort(format!(
                                "vector shard_capacity mismatch for g_id={g_id}, p_id={p_id}: existing={}, expected={}",
                                old_manifest.shard_capacity,
                                run_index::vector_arena::SHARD_CAPACITY
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
                                        run_index::vector_arena::read_vector_shard_from_bytes(
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
                                        run_index::vector_arena::write_vector_shard_to_bytes(
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
                        let shard_results = run_index::vector_arena::write_vector_shards_from_raw(
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

                        let combined_manifest = run_index::vector_arena::VectorManifest {
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

                        let shard_results = run_index::vector_arena::write_vector_shards_to_bytes(
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

                        let manifest = run_index::vector_arena::VectorManifest {
                            version: 1,
                            dims: arena.dims(),
                            dtype: "f32".to_string(),
                            normalized: arena.is_normalized(),
                            shard_capacity: run_index::vector_arena::SHARD_CAPACITY,
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
                                tracing::warn!(
                                    g_id,
                                    p_id,
                                    %e,
                                    "failed to load prior spatial snapshot; \
                                     rebuilding from novelty only"
                                );
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
                            .expect("valid hash");
                    let manifest_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.manifest_address)
                            .expect("valid hash");
                    let arena_cid =
                        ContentId::from_hex_digest(spatial_codec, &write_result.arena_address)
                            .expect("valid hash");
                    let leaflet_cids: Vec<ContentId> = write_result
                        .leaflet_addresses
                        .iter()
                        .map(|h| ContentId::from_hex_digest(spatial_codec, h).expect("valid hash"))
                        .collect();

                    let new_ref = run_index::SpatialArenaRefV5 {
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
            use crate::dict_tree::reverse_leaf::subject_reverse_key;
            use crate::run_index::batched_lookup::batched_lookup_predicate_refs;

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

                        match run_index::BinaryIndexStore::load_from_root_v5(
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

            match run_index::BinaryIndexStore::load_from_root_v5(
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
                    tracing::warn!(
                        %e,
                        "Phase 4.7: failed to load store for schema extraction; \
                         carrying forward base schema"
                    );
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
    root_builder.set_prev_index(run_index::BinaryPrevIndexRef {
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
        final_root.garbage = garbage_ref.map(|id| run_index::BinaryGarbageRef { id });

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

/// Result of uploading an incrementally-updated reverse dictionary tree.
struct UpdatedReverseTree {
    tree_refs: run_index::DictTreeRefs,
    replaced_cids: Vec<ContentId>,
}

/// Async version of reverse tree upload for **subject** dictionaries.
///
/// Builds `ReverseEntry` from `(ns_code, local_id, suffix)`, pre-fetches
/// affected leaves, runs the CPU-bound CoW update in `spawn_blocking`,
/// then async-uploads new artifacts.
async fn upload_incremental_reverse_tree_async<S: Storage>(
    storage: &S,
    ledger_id: &str,
    dict: fluree_db_core::DictKind,
    content_store: &std::sync::Arc<dyn fluree_db_core::storage::ContentStore>,
    existing_refs: &run_index::DictTreeRefs,
    new_subjects: Vec<(u16, u64, Vec<u8>)>,
) -> Result<UpdatedReverseTree> {
    use dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};

    let mut entries: Vec<ReverseEntry> = new_subjects
        .iter()
        .map(|(ns_code, local_id, suffix)| ReverseEntry {
            key: subject_reverse_key(*ns_code, suffix),
            id: fluree_db_core::subject_id::SubjectId::new(*ns_code, *local_id).as_u64(),
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    upload_incremental_reverse_tree_core(
        storage,
        ledger_id,
        dict,
        content_store,
        existing_refs,
        entries,
    )
    .await
}

/// Async version of reverse tree upload for **string** dictionaries.
///
/// Builds `ReverseEntry` from `(string_id, value)`, pre-fetches affected
/// leaves, runs the CPU-bound CoW update in `spawn_blocking`, then
/// async-uploads new artifacts.
async fn upload_incremental_reverse_tree_async_strings<S: Storage>(
    storage: &S,
    ledger_id: &str,
    dict: fluree_db_core::DictKind,
    content_store: &std::sync::Arc<dyn fluree_db_core::storage::ContentStore>,
    existing_refs: &run_index::DictTreeRefs,
    new_strings: Vec<(u32, Vec<u8>)>,
) -> Result<UpdatedReverseTree> {
    use dict_tree::reverse_leaf::ReverseEntry;

    let mut entries: Vec<ReverseEntry> = new_strings
        .iter()
        .map(|(string_id, value)| ReverseEntry {
            key: value.clone(),
            id: *string_id as u64,
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    upload_incremental_reverse_tree_core(
        storage,
        ledger_id,
        dict,
        content_store,
        existing_refs,
        entries,
    )
    .await
}

/// Core async reverse tree upload: pre-fetch affected leaves, spawn_blocking
/// for CoW update, async-upload new artifacts.
async fn upload_incremental_reverse_tree_core<S: Storage>(
    storage: &S,
    ledger_id: &str,
    dict: fluree_db_core::DictKind,
    content_store: &std::sync::Arc<dyn fluree_db_core::storage::ContentStore>,
    existing_refs: &run_index::DictTreeRefs,
    entries: Vec<dict_tree::reverse_leaf::ReverseEntry>,
) -> Result<UpdatedReverseTree> {
    let kind = ContentKind::DictBlob { dict };

    // 1. Async fetch existing branch.
    let existing_branch_bytes = content_store
        .get(&existing_refs.branch)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("load reverse branch: {e}")))?;
    let existing_branch = dict_tree::branch::DictBranch::decode(&existing_branch_bytes)
        .map_err(|e| IndexerError::StorageRead(format!("decode reverse branch: {e}")))?;

    // 2. Build address→CID map for existing leaves.
    let mut address_to_cid: std::collections::HashMap<String, ContentId> =
        std::collections::HashMap::new();
    for (i, entry) in existing_branch.leaves.iter().enumerate() {
        if let Some(cid) = existing_refs.leaves.get(i) {
            address_to_cid.insert(entry.address.clone(), cid.clone());
        }
    }

    // 3. Pre-fetch only affected leaves (those that will receive novelty).
    let affected_indices = compute_affected_leaf_indices(&entries, &existing_branch.leaves);
    let mut prefetched: std::collections::HashMap<usize, Vec<u8>> =
        std::collections::HashMap::with_capacity(affected_indices.len());
    for &idx in &affected_indices {
        let cid = existing_refs.leaves.get(idx).ok_or_else(|| {
            IndexerError::StorageRead(format!("reverse leaf index {idx} out of bounds"))
        })?;
        let bytes = content_store
            .get(cid)
            .await
            .map_err(|e| IndexerError::StorageRead(format!("fetch reverse leaf: {e}")))?;
        prefetched.insert(idx, bytes);
    }

    // 4. CPU-bound CoW update in spawn_blocking.
    let existing_branch_owned = existing_branch;
    let tree_result = tokio::task::spawn_blocking(move || {
        let mut fetch_leaf = |idx: usize| -> std::result::Result<Vec<u8>, std::io::Error> {
            prefetched
                .remove(&idx)
                .ok_or_else(|| std::io::Error::other(format!("reverse leaf {idx} not prefetched")))
        };
        dict_tree::incremental::update_reverse_tree(
            &existing_branch_owned,
            &entries,
            dict_tree::builder::DEFAULT_TARGET_LEAF_BYTES,
            &mut fetch_leaf,
        )
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("reverse tree task panicked: {e}")))?
    .map_err(|e| IndexerError::StorageWrite(format!("incremental reverse tree: {e}")))?;

    // 5. Async upload new leaf artifacts.
    let mut hash_to_address: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for leaf_art in &tree_result.new_leaves {
        let cas_result = storage
            .content_write_bytes(kind, ledger_id, &leaf_art.bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let cid = cid_from_write(kind, &cas_result);
        address_to_cid.insert(cas_result.address.clone(), cid);
        hash_to_address.insert(leaf_art.hash.clone(), cas_result.address);
    }

    // 6. Finalize branch (replace pending:hash → real addresses).
    let (finalized_branch, finalized_bytes, _) =
        dict_tree::builder::finalize_branch(tree_result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(format!("finalize reverse branch: {e}")))?;

    // 7. Async upload finalized branch.
    let branch_result = storage
        .content_write_bytes(kind, ledger_id, &finalized_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    let new_branch_cid = cid_from_write(kind, &branch_result);

    // 8. Build leaf CID list from finalized branch.
    let mut leaf_cids: Vec<ContentId> = Vec::with_capacity(finalized_branch.leaves.len());
    for entry in &finalized_branch.leaves {
        let cid = address_to_cid.get(&entry.address).ok_or_else(|| {
            IndexerError::StorageWrite(format!(
                "no CID for reverse leaf address: {}",
                entry.address
            ))
        })?;
        leaf_cids.push(cid.clone());
    }

    // 9. Collect replaced CIDs for GC.
    let mut replaced_cids = vec![existing_refs.branch.clone()];
    for &idx in &tree_result.replaced_leaf_indices {
        if let Some(cid) = existing_refs.leaves.get(idx) {
            replaced_cids.push(cid.clone());
        }
    }

    Ok(UpdatedReverseTree {
        tree_refs: run_index::DictTreeRefs {
            branch: new_branch_cid,
            leaves: leaf_cids,
        },
        replaced_cids,
    })
}

/// Compute which leaf indices in a reverse tree branch are affected by new entries.
///
/// Uses the same half-open interval logic as the internal `slice_entries_to_leaves`:
/// leaf `i` owns keys in `[leaf[i].first_key, leaf[i+1].first_key)`, and the last
/// leaf owns `[leaf[last].first_key, +∞)`.
fn compute_affected_leaf_indices(
    entries: &[dict_tree::reverse_leaf::ReverseEntry],
    leaves: &[dict_tree::branch::BranchLeafEntry],
) -> Vec<usize> {
    let n = leaves.len();
    if n == 0 || entries.is_empty() {
        return Vec::new();
    }

    let mut affected = Vec::new();
    let mut start = 0;

    for i in 0..n {
        if i == n - 1 {
            // Last leaf: anything remaining goes here.
            if start < entries.len() {
                affected.push(i);
            }
        } else {
            let next_key = &leaves[i + 1].first_key;
            let end = start
                + entries[start..].partition_point(|e| e.key.as_slice() < next_key.as_slice());
            if end > start {
                affected.push(i);
            }
            start = end;
        }
    }

    affected
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
    sorted_records: &[run_index::RunRecord],
    order: run_index::RunSortOrder,
    g_id: u16,
    config: &run_index::incremental_branch::IncrementalBranchConfig,
    p_width: u8,
    dt_width: u8,
) -> Result<run_index::incremental_branch::BranchUpdateResult> {
    use run_index::branch::{build_branch_v2_bytes, LeafEntry};
    use run_index::incremental_leaf::NewLeafBlob;
    use run_index::leaf::LeafWriter;

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

/// Result of uploading index artifacts to CAS.
///
/// Separates default graph (g_id=0) from named graphs because the root format
/// inlines leaf routing for the default graph (no branch fetch needed) while
/// named graphs use branch CID pointers.
pub struct UploadedIndexes {
    /// Default graph (g_id=0): inline leaf routing per sort order.
    /// Leaves are uploaded to CAS; branch is NOT uploaded (routing is inline in root).
    pub default_graph_orders: Vec<run_index::InlineOrderRouting>,
    /// Named graphs (g_id!=0): branch CID per sort order per graph.
    /// Both branches and leaves are uploaded to CAS.
    pub named_graphs: Vec<run_index::NamedGraphRouting>,
}

/// Upload all index artifacts (branches + leaves) to CAS.
///
/// For the default graph (g_id=0), leaves are uploaded but the branch is NOT —
/// leaf routing is embedded inline in the root. For named graphs, both branches
/// and leaves are uploaded.
pub async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    build_results: &[(run_index::RunSortOrder, run_index::IndexBuildResult)],
) -> Result<UploadedIndexes> {
    use fluree_db_core::ContentKind;
    use futures::StreamExt;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    // Accumulators for default graph inline routing and named graph branch pointers.
    let mut default_orders: Vec<run_index::InlineOrderRouting> = Vec::new();
    let mut named_map: BTreeMap<GraphId, Vec<(run_index::RunSortOrder, ContentId)>> =
        BTreeMap::new();

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
                default_orders.push(run_index::InlineOrderRouting {
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

    let named_graphs: Vec<run_index::NamedGraphRouting> = named_map
        .into_iter()
        .map(|(g_id, orders)| run_index::NamedGraphRouting { g_id, orders })
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

/// Result of uploading persisted dict flat files to CAS.
///
/// Contains the CAS addresses for all dictionary artifacts plus derived metadata
/// needed for building the `IndexRootV5` (IRB1) root.
#[derive(Debug)]
pub struct UploadedDicts {
    pub dict_refs: run_index::DictRefs,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,
    /// Graph IRIs by dict_index (0-based). `g_id = dict_index + 1`.
    pub graph_iris: Vec<String>,
    /// Datatype IRIs by dt_id (0-based).
    pub datatype_iris: Vec<String>,
    /// Language tags by (lang_id - 1). `lang_id = index + 1`, 0 = "no tag".
    pub language_tags: Vec<String>,
}

/// Upload dictionary artifacts from persisted flat files to CAS.
///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
///
/// Required files in `run_dir`:
///   - `subjects.fwd`, `subjects.idx`, `subjects.sids`
///   - `strings.fwd`, `strings.idx`
///   - `graphs.dict`, `datatypes.dict`, `languages.dict`
///   - `numbig/p_*.nba` (zero or more)
///
/// Watermark derivation from `subjects.sids`:
///   - Decode each sid64 via `SubjectId::from_u64` → `(ns_code, local_id)`
///   - `subject_watermarks[ns_code]` = max local_id for that ns_code
///   - Overflow ns_code (0xFFFF): always wide, watermark = 0
///   - `needs_wide` = any local_id exceeds `u16::MAX`
///   - `string_watermark` = string entry count − 1 (IDs are 0..=N contiguous)
#[allow(clippy::type_complexity)]
pub async fn upload_dicts_from_disk<S: Storage>(
    storage: &S,
    ledger_id: &str,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<UploadedDicts> {
    use dict_tree::branch::{BranchLeafEntry, DictBranch};
    use dict_tree::builder;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind, SubjectIdEncoding};
    use std::collections::BTreeMap;

    // ---- 1. Read small dicts for v4 root inlining ----
    tracing::info!("reading small dictionaries for v4 root (graphs, datatypes, languages)");

    let graphs_path = run_dir.join("graphs.dict");
    let graphs_dict = run_index::dict_io::read_predicate_dict(&graphs_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", graphs_path.display(), e)))?;
    let graph_iris: Vec<String> = (0..graphs_dict.len())
        .filter_map(|id| graphs_dict.resolve(id).map(|iri| iri.to_string()))
        .collect();

    let datatypes_path = run_dir.join("datatypes.dict");
    let datatypes_dict = run_index::dict_io::read_predicate_dict(&datatypes_path).map_err(|e| {
        IndexerError::StorageRead(format!("read {}: {}", datatypes_path.display(), e))
    })?;
    let datatype_iris: Vec<String> = (0..datatypes_dict.len())
        .filter_map(|id| datatypes_dict.resolve(id).map(|iri| iri.to_string()))
        .collect();

    let languages_path = run_dir.join("languages.dict");
    let language_tags = if languages_path.exists() {
        let lang_dict = run_index::dict_io::read_language_dict(&languages_path).map_err(|e| {
            IndexerError::StorageRead(format!("read {}: {}", languages_path.display(), e))
        })?;
        let mut tags: Vec<(u16, String)> = lang_dict
            .iter()
            .map(|(id, tag)| (id, tag.to_string()))
            .collect();
        tags.sort_unstable_by_key(|(id, _)| *id);
        tags.into_iter().map(|(_, tag)| tag).collect()
    } else {
        Vec::new()
    };

    // ---- 2. Read sids (needed for both subject trees and watermark computation) ----
    let sids_path = run_dir.join("subjects.sids");
    let sids: Vec<u64> = run_index::dict_io::read_subject_sid_map(&sids_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", sids_path.display(), e)))?;

    // ---- 3. Upload subject trees, string trees, numbig, vectors in parallel ----
    tracing::info!(
        "uploading subject trees, string trees, numbig arenas, and vector arenas in parallel"
    );

    let (subject_trees, string_result, numbig, vectors) = tokio::try_join!(
        // Task A: Subject forward + reverse trees
        async {
            let subj_idx_path = run_dir.join("subjects.idx");
            let (subj_offsets, subj_lens) = run_index::dict_io::read_forward_index(&subj_idx_path)
                .map_err(|e| {
                    IndexerError::StorageRead(format!("read {}: {}", subj_idx_path.display(), e))
                })?;
            let subj_fwd_path = run_dir.join("subjects.fwd");
            let subj_fwd_file = std::fs::File::open(&subj_fwd_path).map_err(|e| {
                IndexerError::StorageRead(format!("open {}: {}", subj_fwd_path.display(), e))
            })?;
            // SAFETY: The file is opened read-only and is not concurrently modified.
            // The forward-dict file is an immutable index artifact written before this point.
            let subj_fwd_data = unsafe { memmap2::Mmap::map(&subj_fwd_file) }.map_err(|e| {
                IndexerError::StorageRead(format!("mmap {}: {}", subj_fwd_path.display(), e))
            })?;
            tracing::info!(
                subjects = sids.len(),
                fwd_bytes = subj_fwd_data.len(),
                "subject dict files loaded"
            );

            // Precompute suffix ranges so we can sort/build trees without allocating per-entry Vecs.
            let mut ns_codes: Vec<u16> = Vec::with_capacity(sids.len());
            let mut suf_offs: Vec<u64> = Vec::with_capacity(sids.len());
            let mut suf_lens: Vec<u32> = Vec::with_capacity(sids.len());
            for (&sid, (&off, &len)) in sids.iter().zip(subj_offsets.iter().zip(subj_lens.iter())) {
                let ns_code = SubjectId::from_u64(sid).ns_code();
                let iri = &subj_fwd_data[off as usize..(off as usize + len as usize)];
                let prefix_bytes = namespace_codes
                    .get(&ns_code)
                    .map(|s| s.as_bytes())
                    .unwrap_or(b"");
                if !prefix_bytes.is_empty() && iri.starts_with(prefix_bytes) {
                    ns_codes.push(ns_code);
                    suf_offs.push(off + prefix_bytes.len() as u64);
                    suf_lens.push(len.saturating_sub(prefix_bytes.len() as u32));
                } else {
                    ns_codes.push(ns_code);
                    suf_offs.push(off);
                    suf_lens.push(len);
                }
            }

            // Pass 1: forward packs (FPK1 format, one stream per namespace)
            tracing::info!("building subject forward packs");
            let sids_sorted = sids.windows(2).all(|w| w[0] <= w[1]);

            let id_order: Option<Vec<usize>> = if sids_sorted {
                None
            } else {
                let mut v: Vec<usize> = (0..sids.len()).collect();
                v.sort_unstable_by_key(|&i| sids[i]);
                Some(v)
            };
            let subject_fwd_pack_refs = {
                use dict_tree::pack_builder;

                // Group entries by ns_code, collect (local_id, suffix) per namespace.
                let mut ns_entries: BTreeMap<u16, Vec<(u64, &[u8])>> = BTreeMap::new();
                let iter_fn = |i: usize| {
                    let sid = sids[i];
                    let off = suf_offs[i] as usize;
                    let len = suf_lens[i] as usize;
                    let ns_code = SubjectId::from_u64(sid).ns_code();
                    let local_id = SubjectId::from_u64(sid).local_id();
                    let suffix = &subj_fwd_data[off..off + len];
                    (ns_code, local_id, suffix)
                };

                match &id_order {
                    None => {
                        for i in 0..sids.len() {
                            let (ns_code, local_id, suffix) = iter_fn(i);
                            ns_entries
                                .entry(ns_code)
                                .or_default()
                                .push((local_id, suffix));
                        }
                    }
                    Some(order) => {
                        for &i in order {
                            let (ns_code, local_id, suffix) = iter_fn(i);
                            ns_entries
                                .entry(ns_code)
                                .or_default()
                                .push((local_id, suffix));
                        }
                    }
                }

                // Build and upload packs per namespace.
                let kind = ContentKind::DictBlob {
                    dict: DictKind::SubjectForward,
                };
                let mut subject_fwd_ns_packs: Vec<(u16, Vec<run_index::PackBranchEntry>)> =
                    Vec::new();
                for (ns_code, entries) in &ns_entries {
                    // Entries should already be sorted by local_id (ascending sid order within ns).
                    let pack_result = pack_builder::build_subject_forward_packs_for_ns(
                        *ns_code,
                        entries,
                        pack_builder::DEFAULT_TARGET_PAGE_BYTES,
                        pack_builder::DEFAULT_TARGET_PACK_BYTES,
                    )
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!("subject pack build: {}", e))
                    })?;

                    let mut ns_pack_refs = Vec::with_capacity(pack_result.packs.len());
                    for pack_artifact in pack_result.packs {
                        let cas_result = storage
                            .content_write_bytes(kind, ledger_id, &pack_artifact.bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        ns_pack_refs.push(run_index::PackBranchEntry {
                            first_id: pack_artifact.first_id,
                            last_id: pack_artifact.last_id,
                            pack_cid: cid_from_write(kind, &cas_result),
                        });
                    }
                    subject_fwd_ns_packs.push((*ns_code, ns_pack_refs));
                }
                subject_fwd_ns_packs
            };

            // Pass 2: reverse tree
            // Fast path: when subjects were produced by vocab-merge, the file order is already
            // sorted by `(ns_code, suffix)` (which matches the reverse-tree key ordering).
            // Avoid sorting indices by comparing huge byte slices.
            let keys_sorted = {
                let mut ok = true;
                for i in 1..sids.len() {
                    let prev_ns = ns_codes[i - 1];
                    let curr_ns = ns_codes[i];
                    if prev_ns < curr_ns {
                        continue;
                    }
                    if prev_ns > curr_ns {
                        ok = false;
                        break;
                    }
                    let a = &subj_fwd_data[suf_offs[i - 1] as usize
                        ..(suf_offs[i - 1] as usize + suf_lens[i - 1] as usize)];
                    let b = &subj_fwd_data
                        [suf_offs[i] as usize..(suf_offs[i] as usize + suf_lens[i] as usize)];
                    if a > b {
                        ok = false;
                        break;
                    }
                }
                ok
            };
            tracing::info!(
                sids_sorted,
                keys_sorted,
                "subject dict ordering check (fast-path eligibility)"
            );

            let rev_order: Option<Vec<usize>> = if keys_sorted {
                None
            } else {
                tracing::info!("building subject reverse tree (fallback index-sort)");
                let mut v: Vec<usize> = (0..sids.len()).collect();
                v.sort_unstable_by(|&a, &b| {
                    let na = ns_codes[a];
                    let nb = ns_codes[b];
                    match na.cmp(&nb) {
                        std::cmp::Ordering::Equal => {
                            let sa = &subj_fwd_data[suf_offs[a] as usize
                                ..(suf_offs[a] as usize + suf_lens[a] as usize)];
                            let sb = &subj_fwd_data[suf_offs[b] as usize
                                ..(suf_offs[b] as usize + suf_lens[b] as usize)];
                            sa.cmp(sb)
                        }
                        other => other,
                    }
                });
                Some(v)
            };
            let subject_reverse = {
                let kind = ContentKind::DictBlob {
                    dict: DictKind::SubjectReverse,
                };
                let mut leaf_cids: Vec<ContentId> = Vec::new();
                let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                let mut leaf_offsets: Vec<u32> = Vec::new();
                let mut leaf_data: Vec<u8> = Vec::new();
                let mut chunk_bytes: usize = 0;
                let mut first_key: Option<Vec<u8>> = None;
                // Track last key parts for branch boundary (avoid per-entry allocation).
                let mut last_ns: u16 = 0;
                let mut last_off: u64 = 0;
                let mut last_len: u32 = 0;

                match &rev_order {
                    None => {
                        for i in 0..sids.len() {
                            let ns = ns_codes[i];
                            let suffix = &subj_fwd_data[suf_offs[i] as usize
                                ..(suf_offs[i] as usize + suf_lens[i] as usize)];
                            let sid = sids[i];

                            // Key = [ns_code BE][suffix]
                            let key_len = 2 + suffix.len();
                            let entry_size = 12 + key_len;
                            leaf_offsets.push(chunk_bytes as u32);
                            chunk_bytes += entry_size;

                            leaf_data.extend_from_slice(&(key_len as u32).to_le_bytes());
                            leaf_data.extend_from_slice(&ns.to_be_bytes());
                            leaf_data.extend_from_slice(suffix);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());

                            if first_key.is_none() {
                                let mut k = Vec::with_capacity(key_len);
                                k.extend_from_slice(&ns.to_be_bytes());
                                k.extend_from_slice(suffix);
                                first_key = Some(k);
                            }
                            last_ns = ns;
                            last_off = suf_offs[i];
                            last_len = suf_lens[i];

                            if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                                    &mut leaf_offsets,
                                    &mut leaf_data,
                                    &mut first_key,
                                    &mut chunk_bytes,
                                    || {
                                        let suffix = &subj_fwd_data[last_off as usize
                                            ..(last_off as usize + last_len as usize)];
                                        let mut lk = Vec::with_capacity(2 + suffix.len());
                                        lk.extend_from_slice(&last_ns.to_be_bytes());
                                        lk.extend_from_slice(suffix);
                                        lk
                                    },
                                ) {
                                    let cas_result = storage
                                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    leaf_cids.push(cid_from_write(kind, &cas_result));
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: fk,
                                        last_key: lk,
                                        entry_count: 0,
                                        address: cas_result.address,
                                    });
                                }
                            }
                        }
                    }
                    Some(order) => {
                        for &i in order {
                            let ns = ns_codes[i];
                            let suffix = &subj_fwd_data[suf_offs[i] as usize
                                ..(suf_offs[i] as usize + suf_lens[i] as usize)];
                            let sid = sids[i];

                            // Key = [ns_code BE][suffix]
                            let key_len = 2 + suffix.len();
                            let entry_size = 12 + key_len;
                            leaf_offsets.push(chunk_bytes as u32);
                            chunk_bytes += entry_size;

                            leaf_data.extend_from_slice(&(key_len as u32).to_le_bytes());
                            leaf_data.extend_from_slice(&ns.to_be_bytes());
                            leaf_data.extend_from_slice(suffix);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());

                            if first_key.is_none() {
                                let mut k = Vec::with_capacity(key_len);
                                k.extend_from_slice(&ns.to_be_bytes());
                                k.extend_from_slice(suffix);
                                first_key = Some(k);
                            }
                            last_ns = ns;
                            last_off = suf_offs[i];
                            last_len = suf_lens[i];

                            if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                                    &mut leaf_offsets,
                                    &mut leaf_data,
                                    &mut first_key,
                                    &mut chunk_bytes,
                                    || {
                                        let suffix = &subj_fwd_data[last_off as usize
                                            ..(last_off as usize + last_len as usize)];
                                        let mut lk = Vec::with_capacity(2 + suffix.len());
                                        lk.extend_from_slice(&last_ns.to_be_bytes());
                                        lk.extend_from_slice(suffix);
                                        lk
                                    },
                                ) {
                                    let cas_result = storage
                                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    leaf_cids.push(cid_from_write(kind, &cas_result));
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: fk,
                                        last_key: lk,
                                        entry_count: 0,
                                        address: cas_result.address,
                                    });
                                }
                            }
                        }
                    }
                }

                if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                    &mut leaf_offsets,
                    &mut leaf_data,
                    &mut first_key,
                    &mut chunk_bytes,
                    || {
                        let suffix = &subj_fwd_data
                            [last_off as usize..(last_off as usize + last_len as usize)];
                        let mut lk = Vec::with_capacity(2 + suffix.len());
                        lk.extend_from_slice(&last_ns.to_be_bytes());
                        lk.extend_from_slice(suffix);
                        lk
                    },
                ) {
                    let cas_result = storage
                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    leaf_cids.push(cid_from_write(kind, &cas_result));
                    branch_entries.push(BranchLeafEntry {
                        first_key: fk,
                        last_key: lk,
                        entry_count: 0,
                        address: cas_result.address,
                    });
                }

                let branch = DictBranch {
                    leaves: branch_entries,
                };
                let branch_bytes = branch.encode();
                let branch_result = storage
                    .content_write_bytes(kind, ledger_id, &branch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                Ok::<_, IndexerError>(run_index::DictTreeRefs {
                    branch: cid_from_write(kind, &branch_result),
                    leaves: leaf_cids,
                })?
            };

            tracing::info!("subject dict artifacts uploaded");
            Ok::<_, IndexerError>((subject_fwd_pack_refs, subject_reverse))
        },
        // Task B: String forward + reverse trees
        async {
            let str_idx_path = run_dir.join("strings.idx");
            let str_fwd_path = run_dir.join("strings.fwd");
            if str_idx_path.exists() && str_fwd_path.exists() {
                let (str_offsets, str_lens) = run_index::dict_io::read_forward_index(&str_idx_path)
                    .map_err(|e| {
                        IndexerError::StorageRead(format!("read {}: {}", str_idx_path.display(), e))
                    })?;
                let str_fwd_file = std::fs::File::open(&str_fwd_path).map_err(|e| {
                    IndexerError::StorageRead(format!("open {}: {}", str_fwd_path.display(), e))
                })?;
                // SAFETY: The file is opened read-only and is not concurrently modified.
                // The forward-dict file is an immutable index artifact written before this point.
                let str_fwd_data = unsafe { memmap2::Mmap::map(&str_fwd_file) }.map_err(|e| {
                    IndexerError::StorageRead(format!("mmap {}: {}", str_fwd_path.display(), e))
                })?;
                let count = str_offsets.len();
                tracing::info!(
                    strings = count,
                    fwd_bytes = str_fwd_data.len(),
                    "string dict files loaded"
                );

                // Pass 1: forward packs (FPK1 format)
                tracing::info!("building string forward packs");
                // IDs are 0..count contiguous and already in order of the forward file.
                let string_fwd_pack_refs = {
                    use dict_tree::pack_builder;
                    let kind = ContentKind::DictBlob {
                        dict: DictKind::StringForward,
                    };
                    let entries: Vec<(u32, &[u8])> = (0..count)
                        .map(|i| {
                            let off = str_offsets[i] as usize;
                            let len = str_lens[i] as usize;
                            (i as u32, &str_fwd_data[off..off + len])
                        })
                        .collect();
                    let pack_result = pack_builder::build_string_forward_packs(
                        &entries,
                        pack_builder::DEFAULT_TARGET_PAGE_BYTES,
                        pack_builder::DEFAULT_TARGET_PACK_BYTES,
                    )
                    .map_err(|e| IndexerError::StorageWrite(format!("string pack build: {}", e)))?;

                    let mut pack_refs = Vec::with_capacity(pack_result.packs.len());
                    for pack_artifact in pack_result.packs {
                        let cas_result = storage
                            .content_write_bytes(kind, ledger_id, &pack_artifact.bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        pack_refs.push(run_index::PackBranchEntry {
                            first_id: pack_artifact.first_id,
                            last_id: pack_artifact.last_id,
                            pack_cid: cid_from_write(kind, &cas_result),
                        });
                    }
                    pack_refs
                };

                // Pass 2: reverse tree
                // Fast path: when strings were produced by vocab-merge, IDs are assigned
                // in lexicographic key order, so the forward file order is already the
                // correct reverse-tree key order. Avoid sorting indices (O(n log n)).
                let strings_sorted = {
                    let mut ok = true;
                    for i in 1..count {
                        let oa = str_offsets[i - 1] as usize;
                        let la = str_lens[i - 1] as usize;
                        let ob = str_offsets[i] as usize;
                        let lb = str_lens[i] as usize;
                        if str_fwd_data[oa..oa + la] > str_fwd_data[ob..ob + lb] {
                            ok = false;
                            break;
                        }
                    }
                    ok
                };
                tracing::info!(
                    strings_sorted,
                    "string dict ordering check (fast-path eligibility)"
                );

                let rev_order: Option<Vec<usize>> = if strings_sorted {
                    None
                } else {
                    tracing::info!("building string reverse tree (fallback index-sort)");
                    let mut v: Vec<usize> = (0..count).collect();
                    v.sort_unstable_by(|&a, &b| {
                        let oa = str_offsets[a] as usize;
                        let la = str_lens[a] as usize;
                        let ob = str_offsets[b] as usize;
                        let lb = str_lens[b] as usize;
                        str_fwd_data[oa..oa + la].cmp(&str_fwd_data[ob..ob + lb])
                    });
                    Some(v)
                };

                let sr = {
                    let kind = ContentKind::DictBlob {
                        dict: DictKind::StringReverse,
                    };
                    let mut leaf_cids: Vec<ContentId> = Vec::new();
                    let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                    let mut leaf_offsets: Vec<u32> = Vec::new();
                    let mut leaf_data: Vec<u8> = Vec::new();
                    let mut chunk_bytes: usize = 0;
                    let mut first_key: Option<Vec<u8>> = None;
                    // Track last key slice for boundary without cloning per entry.
                    let mut last_off: usize = 0;
                    let mut last_len: usize = 0;

                    match &rev_order {
                        None => {
                            for i in 0..count {
                                let off = str_offsets[i] as usize;
                                let len = str_lens[i] as usize;
                                let key = &str_fwd_data[off..off + len];
                                let id = i as u64;

                                let entry_size = 12 + key.len();
                                leaf_offsets.push(chunk_bytes as u32);
                                chunk_bytes += entry_size;

                                leaf_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                                leaf_data.extend_from_slice(key);
                                leaf_data.extend_from_slice(&id.to_le_bytes());

                                if first_key.is_none() {
                                    first_key = Some(key.to_vec());
                                }
                                last_off = off;
                                last_len = len;

                                if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                    if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                                        &mut leaf_offsets,
                                        &mut leaf_data,
                                        &mut first_key,
                                        &mut chunk_bytes,
                                        || str_fwd_data[last_off..(last_off + last_len)].to_vec(),
                                    ) {
                                        let cas_result = storage
                                            .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                            .await
                                            .map_err(|e| {
                                                IndexerError::StorageWrite(e.to_string())
                                            })?;
                                        leaf_cids.push(cid_from_write(kind, &cas_result));
                                        branch_entries.push(BranchLeafEntry {
                                            first_key: fk,
                                            last_key: lk,
                                            entry_count: 0,
                                            address: cas_result.address,
                                        });
                                    }
                                }
                            }
                        }
                        Some(order) => {
                            for &i in order {
                                let off = str_offsets[i] as usize;
                                let len = str_lens[i] as usize;
                                let key = &str_fwd_data[off..off + len];
                                let id = i as u64;

                                let entry_size = 12 + key.len();
                                leaf_offsets.push(chunk_bytes as u32);
                                chunk_bytes += entry_size;

                                leaf_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                                leaf_data.extend_from_slice(key);
                                leaf_data.extend_from_slice(&id.to_le_bytes());

                                if first_key.is_none() {
                                    first_key = Some(key.to_vec());
                                }
                                last_off = off;
                                last_len = len;

                                if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                    if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                                        &mut leaf_offsets,
                                        &mut leaf_data,
                                        &mut first_key,
                                        &mut chunk_bytes,
                                        || str_fwd_data[last_off..(last_off + last_len)].to_vec(),
                                    ) {
                                        let cas_result = storage
                                            .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                            .await
                                            .map_err(|e| {
                                                IndexerError::StorageWrite(e.to_string())
                                            })?;
                                        leaf_cids.push(cid_from_write(kind, &cas_result));
                                        branch_entries.push(BranchLeafEntry {
                                            first_key: fk,
                                            last_key: lk,
                                            entry_count: 0,
                                            address: cas_result.address,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    if let Some((leaf_bytes, fk, lk)) = flush_reverse_leaf(
                        &mut leaf_offsets,
                        &mut leaf_data,
                        &mut first_key,
                        &mut chunk_bytes,
                        || str_fwd_data[last_off..(last_off + last_len)].to_vec(),
                    ) {
                        let cas_result = storage
                            .content_write_bytes(kind, ledger_id, &leaf_bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        leaf_cids.push(cid_from_write(kind, &cas_result));
                        branch_entries.push(BranchLeafEntry {
                            first_key: fk,
                            last_key: lk,
                            entry_count: 0,
                            address: cas_result.address,
                        });
                    }

                    let branch = DictBranch {
                        leaves: branch_entries,
                    };
                    let branch_bytes = branch.encode();
                    let branch_result = storage
                        .content_write_bytes(kind, ledger_id, &branch_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    Ok::<_, IndexerError>(run_index::DictTreeRefs {
                        branch: cid_from_write(kind, &branch_result),
                        leaves: leaf_cids,
                    })?
                };

                tracing::info!("string dict artifacts uploaded");
                Ok::<_, IndexerError>((count, string_fwd_pack_refs, sr))
            } else {
                // No strings persisted — empty packs + empty reverse tree
                let kind_rev = ContentKind::DictBlob {
                    dict: DictKind::StringReverse,
                };
                let empty_branch = DictBranch { leaves: vec![] };
                let empty_bytes = empty_branch.encode();
                let wr_rev = storage
                    .content_write_bytes(kind_rev, ledger_id, &empty_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Ok((
                    0,
                    vec![], // no forward packs
                    run_index::DictTreeRefs {
                        branch: cid_from_write(kind_rev, &wr_rev),
                        leaves: vec![],
                    },
                ))
            }
        },
        // Task C: Numbig arenas (per-graph subdirectories)
        async {
            let mut numbig: BTreeMap<String, BTreeMap<String, ContentId>> = BTreeMap::new();
            // Scan for g_{id}/numbig/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {}", e)))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {}", e)))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let nb_dir = dir_entry.path().join("numbig");
                    if nb_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&nb_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read numbig dir: {}", e))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read numbig entry: {}", e))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".nba") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let (cid, _) = upload_dict_file(
                                            storage,
                                            ledger_id,
                                            &entry.path(),
                                            DictKind::NumBig { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;
                                        per_pred.insert(p_id.to_string(), cid);
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            numbig.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(numbig)
        },
        // Task D: Vector arenas (per-graph subdirectories)
        async {
            let mut vectors: BTreeMap<String, BTreeMap<String, run_index::VectorDictRef>> =
                BTreeMap::new();
            // Scan for g_{id}/vectors/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {}", e)))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {}", e)))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let vec_dir = dir_entry.path().join("vectors");
                    if vec_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&vec_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read vectors dir: {}", e))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read vectors entry: {}", e))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".vam") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let manifest_bytes =
                                            tokio::fs::read(entry.path()).await.map_err(|e| {
                                                IndexerError::StorageRead(format!(
                                                    "read vector manifest: {}",
                                                    e
                                                ))
                                            })?;
                                        let manifest =
                                            run_index::vector_arena::read_vector_manifest(
                                                &manifest_bytes,
                                            )
                                            .map_err(
                                                |e| {
                                                    IndexerError::StorageRead(format!(
                                                        "parse vector manifest: {}",
                                                        e
                                                    ))
                                                },
                                            )?;

                                        let mut shard_cids =
                                            Vec::with_capacity(manifest.shards.len());
                                        let mut shard_infos =
                                            Vec::with_capacity(manifest.shards.len());
                                        for (shard_idx, shard_info) in
                                            manifest.shards.iter().enumerate()
                                        {
                                            let shard_path = vec_dir
                                                .join(format!("p_{}_s_{}.vas", p_id, shard_idx));
                                            let (shard_cid, shard_wr) = upload_dict_file(
                                                storage,
                                                ledger_id,
                                                &shard_path,
                                                DictKind::VectorShard { p_id },
                                                "dict artifact uploaded to CAS (from disk)",
                                            )
                                            .await?;
                                            shard_infos.push(run_index::vector_arena::ShardInfo {
                                                cas: shard_wr.address,
                                                count: shard_info.count,
                                            });
                                            shard_cids.push(shard_cid);
                                        }

                                        let final_manifest =
                                            run_index::vector_arena::VectorManifest {
                                                shards: shard_infos,
                                                ..manifest
                                            };
                                        let manifest_json = serde_json::to_vec_pretty(
                                            &final_manifest,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageWrite(format!(
                                                "serialize vector manifest: {}",
                                                e
                                            ))
                                        })?;
                                        let final_manifest_path =
                                            vec_dir.join(format!("p_{}_final.vam", p_id));
                                        std::fs::write(&final_manifest_path, &manifest_json)
                                            .map_err(|e| {
                                                IndexerError::StorageWrite(format!(
                                                    "write final vector manifest: {}",
                                                    e
                                                ))
                                            })?;
                                        let (manifest_cid, _) = upload_dict_file(
                                            storage,
                                            ledger_id,
                                            &final_manifest_path,
                                            DictKind::VectorManifest { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;

                                        per_pred.insert(
                                            p_id.to_string(),
                                            run_index::VectorDictRef {
                                                manifest: manifest_cid,
                                                shards: shard_cids,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            vectors.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(vectors)
        },
    )?;

    let (subject_fwd_ns_packs, subject_reverse) = subject_trees;
    let (string_count, string_fwd_packs, string_reverse) = string_result;

    // ---- 4. Compute subject_id_encoding + watermarks from sids ----
    let overflow_ns: u16 = 0xFFFF;
    let mut needs_wide = false;
    let mut max_ns_code: u16 = 0;
    let mut watermark_map: BTreeMap<u16, u64> = BTreeMap::new();

    for &sid in &sids {
        let subject_id = SubjectId::from_u64(sid);
        let ns_code = subject_id.ns_code();
        let local_id = subject_id.local_id();

        if ns_code == overflow_ns {
            needs_wide = true;
            continue;
        }

        if local_id > u16::MAX as u64 {
            needs_wide = true;
        }

        if ns_code > max_ns_code {
            max_ns_code = ns_code;
        }

        let entry = watermark_map.entry(ns_code).or_insert(0);
        if local_id > *entry {
            *entry = local_id;
        }
    }

    let subject_id_encoding = if needs_wide {
        SubjectIdEncoding::Wide
    } else {
        SubjectIdEncoding::Narrow
    };

    let watermark_len = if watermark_map.is_empty() {
        0
    } else {
        max_ns_code as usize + 1
    };
    let mut subject_watermarks: Vec<u64> = vec![0; watermark_len];
    for (&ns_code, &max_local) in &watermark_map {
        subject_watermarks[ns_code as usize] = max_local;
    }

    let string_watermark = if string_count > 0 {
        (string_count - 1) as u32
    } else {
        0
    };

    tracing::info!(
        subjects = sids.len(),
        strings = string_count,
        numbig_graphs = numbig.len(),
        vector_graphs = vectors.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        "dictionary trees built and uploaded to CAS (from disk)"
    );

    Ok(UploadedDicts {
        dict_refs: run_index::DictRefs {
            forward_packs: run_index::DictPackRefs {
                string_fwd_packs,
                subject_fwd_ns_packs,
            },
            subject_reverse,
            string_reverse,
            numbig,
            vectors,
        },
        subject_id_encoding,
        subject_watermarks,
        string_watermark,
        graph_iris,
        datatype_iris,
        language_tags,
    })
}

/// Build spatial indexes from collected geometry entries and upload to CAS.
///
/// Groups entries by `(g_id, p_id)`, builds one spatial index per group,
/// and uploads cell index leaflets, manifests, and geometry arenas to CAS.
///
/// Two-phase approach:
/// 1. Build the index and collect all serialized blobs with locally-computed
///    SHA-256 hashes (synchronous — no async needed).
/// 2. Upload all blobs to CAS using `content_write_bytes` (async).
///
/// This avoids re-entering `block_on` from within a sync closure, which would
/// deadlock inside the `spawn_blocking` + `handle.block_on()` pattern.
///
/// Returns per-graph spatial arena refs for inclusion in `IndexRootV5`.
async fn build_and_upload_spatial_indexes<S: Storage>(
    entries: &[crate::spatial_hook::SpatialEntry],
    predicates: &run_index::PredicateDict,
    ledger_id: &str,
    storage: &S,
) -> Result<Vec<(GraphId, Vec<run_index::SpatialArenaRefV5>)>> {
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    // Group entries by (g_id, p_id).
    let mut grouped: BTreeMap<(GraphId, u32), Vec<&crate::spatial_hook::SpatialEntry>> =
        BTreeMap::new();
    for entry in entries {
        grouped
            .entry((entry.g_id, entry.p_id))
            .or_default()
            .push(entry);
    }

    let mut per_graph: BTreeMap<GraphId, Vec<run_index::SpatialArenaRefV5>> = BTreeMap::new();

    for ((g_id, p_id), group_entries) in grouped {
        // Resolve predicate IRI for SpatialCreateConfig.
        let pred_iri = predicates.resolve(p_id).unwrap_or("unknown").to_string();

        let config = fluree_db_spatial::SpatialCreateConfig::new(
            format!("spatial:g{}p{}", g_id, p_id),
            ledger_id.to_string(),
            pred_iri.clone(),
        );
        let mut builder = fluree_db_spatial::SpatialIndexBuilder::new(config);

        for entry in &group_entries {
            // add_geometry returns Ok(false) for skipped entries (e.g., parse errors).
            // We log but do not fail the entire build for individual geometry errors.
            if let Err(e) =
                builder.add_geometry(entry.subject_id, &entry.wkt, entry.t, entry.is_assert)
            {
                tracing::warn!(
                    subject_id = entry.subject_id,
                    p_id = p_id,
                    error = %e,
                    "spatial: failed to add geometry, skipping"
                );
            }
        }

        let build_result = builder
            .build()
            .map_err(|e| IndexerError::Other(format!("spatial build error: {}", e)))?;

        if build_result.entries.is_empty() {
            continue;
        }

        // Phase 1: Build the index and collect all serialized blobs.
        // We compute SHA-256 locally so write_to_cas gets the correct hashes
        // without needing async CAS writes during the build.
        let mut pending_blobs: Vec<(String, Vec<u8>)> = Vec::new();
        let write_result = build_result
            .write_to_cas(|bytes| {
                let hash_hex = hex::encode(Sha256::digest(bytes));
                pending_blobs.push((hash_hex.clone(), bytes.to_vec()));
                Ok(hash_hex)
            })
            .map_err(|e| IndexerError::Other(format!("spatial build: {}", e)))?;

        // Phase 2: Upload all collected blobs to CAS.
        for (expected_hash, blob_bytes) in &pending_blobs {
            let cas_result = storage
                .content_write_bytes(ContentKind::SpatialIndex, ledger_id, blob_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(format!("spatial CAS write: {}", e)))?;
            debug_assert_eq!(
                &cas_result.content_hash, expected_hash,
                "CAS content_hash mismatch for spatial blob"
            );
        }

        // Construct ContentIds from the content hashes.
        let spatial_codec = ContentKind::SpatialIndex.to_codec();
        let manifest_cid =
            ContentId::from_hex_digest(spatial_codec, &write_result.manifest_address).ok_or_else(
                || {
                    IndexerError::Other(format!(
                        "invalid spatial manifest hash: {}",
                        write_result.manifest_address
                    ))
                },
            )?;
        let arena_cid = ContentId::from_hex_digest(spatial_codec, &write_result.arena_address)
            .ok_or_else(|| {
                IndexerError::Other(format!(
                    "invalid spatial arena hash: {}",
                    write_result.arena_address
                ))
            })?;
        let leaflet_cids: Vec<ContentId> = write_result
            .leaflet_addresses
            .iter()
            .map(|hash| {
                ContentId::from_hex_digest(spatial_codec, hash).ok_or_else(|| {
                    IndexerError::Other(format!("invalid spatial leaflet hash: {}", hash))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Serialize SpatialIndexRoot as JSON and write to CAS.
        let root_json = serde_json::to_vec(&write_result.root)
            .map_err(|e| IndexerError::Other(format!("spatial root serialize: {}", e)))?;
        let root_cas = storage
            .content_write_bytes(ContentKind::SpatialIndex, ledger_id, &root_json)
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("spatial root CAS write: {}", e)))?;
        let root_cid = ContentId::from_hex_digest(spatial_codec, &root_cas.content_hash)
            .ok_or_else(|| {
                IndexerError::Other(format!(
                    "invalid spatial root hash: {}",
                    root_cas.content_hash
                ))
            })?;

        per_graph
            .entry(g_id)
            .or_default()
            .push(run_index::SpatialArenaRefV5 {
                p_id,
                root_cid,
                manifest: manifest_cid,
                arena: arena_cid,
                leaflets: leaflet_cids,
            });

        tracing::info!(
            g_id,
            p_id,
            predicate = %pred_iri,
            geometries = write_result.root.geometry_count,
            cell_entries = write_result.root.entry_count,
            blobs_uploaded = pending_blobs.len(),
            "spatial index built for (graph, predicate)"
        );
    }

    Ok(per_graph.into_iter().collect())
}

/// Publish index result to nameservice
pub async fn publish_index_result<P: Publisher>(publisher: &P, result: &IndexResult) -> Result<()> {
    publisher
        .publish_index(&result.ledger_id, result.index_t, &result.root_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_default() {
        let stats = IndexStats::default();
        assert_eq!(stats.flake_count, 0);
        assert_eq!(stats.leaf_count, 0);
        assert_eq!(stats.branch_count, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_normalize_id_for_comparison() {
        // Canonical format passes through
        assert_eq!(normalize_id_for_comparison("test:main"), "test:main");
        assert_eq!(
            normalize_id_for_comparison("my-ledger:dev"),
            "my-ledger:dev"
        );

        // Storage-path format with single slash converts to canonical
        assert_eq!(normalize_id_for_comparison("test/main"), "test:main");
        assert_eq!(
            normalize_id_for_comparison("my-ledger/dev"),
            "my-ledger:dev"
        );

        // Both formats normalize to the same canonical form
        assert_eq!(
            normalize_id_for_comparison("test/main"),
            normalize_id_for_comparison("test:main")
        );

        // Name without branch gets default branch
        assert_eq!(normalize_id_for_comparison("test"), "test:main");

        // Canonical format with explicit branch takes precedence over slashes in name
        // "org/project:main" - the colon is the branch separator, not the slash
        assert_eq!(
            normalize_id_for_comparison("org/project:main"),
            "org/project:main"
        );

        // Multiple slashes without colon - treated as name with default branch
        // (we don't know which slash is the "branch separator")
        assert_eq!(normalize_id_for_comparison("a/b/c"), "a/b/c:main");
    }

    /// Verify that build_predicate_sids correctly decomposes predicates using the
    /// provided namespace table. If a new namespace is introduced, predicates
    /// using that namespace should be split correctly.
    #[test]
    fn test_build_predicate_sids_with_new_namespace() {
        use run_index::resolver::SharedResolverState;

        // Create a SharedResolverState with predicates spanning two namespaces.
        let mut shared = SharedResolverState::new();
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
