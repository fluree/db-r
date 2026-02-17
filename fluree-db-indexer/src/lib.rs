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

use tracing::Instrument;

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
fn flush_forward_leaf(
    leaf_offsets: &mut Vec<u32>,
    leaf_data: &mut Vec<u8>,
    leaf_count: &mut u32,
    data_offset: &mut u32,
    leaf_first: &mut u64,
    leaf_last: &mut u64,
) -> Option<(Vec<u8>, u64, u64, u32)> {
    if *leaf_count == 0 {
        return None;
    }
    let entry_count = *leaf_count;
    let header_size = 8;
    let offset_table_size = leaf_offsets.len() * 4;
    let total = header_size + offset_table_size + leaf_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&dict_tree::forward_leaf::FORWARD_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());
    for off in leaf_offsets.iter() {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(leaf_data);
    let out = (buf, *leaf_first, *leaf_last, entry_count);
    leaf_offsets.clear();
    leaf_data.clear();
    *data_offset = 0;
    *leaf_count = 0;
    *leaf_first = 0;
    *leaf_last = 0;
    Some(out)
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
    async move {
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

        rebuild_index_from_commits(storage, ledger_id, &record, config).await
    }
    .instrument(span)
    .await
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
    let parent_span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        let _guard = parent_span.enter(); // safe: spawn_blocking pins to one thread
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

            let chunk_max_flakes: u64 = 5_000_000; // ~5M flakes per chunk
            let mut chunk = RebuildChunk::new();
            let mut chunks: Vec<RebuildChunk> = Vec::new();

            // Accumulate commit statistics for index root
            let mut total_commit_size = 0u64;
            let mut total_asserts = 0u64;
            let mut total_retracts = 0u64;

            for (i, cid) in commit_cids.iter().enumerate() {
                // If chunk is non-empty and near budget, flush before processing
                // the next commit to avoid memory bloat on large commits.
                if !chunk.is_empty() && chunk.flake_count() >= chunk_max_flakes {
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
                    chunks.push(std::mem::take(&mut chunk));
                }
            }

            // Push final chunk if non-empty.
            if !chunk.is_empty() {
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

            // Write numbig arenas
            if !shared.numbigs.is_empty() {
                let nb_dir = run_dir.join("numbig");
                std::fs::create_dir_all(&nb_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                for (&p_id, arena) in &shared.numbigs {
                    run_index::numbig_dict::write_numbig_arena(
                        &nb_dir.join(format!("p_{}.nba", p_id)),
                        arena,
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                }
            }

            // Write vector arenas (shards + manifests per predicate)
            if !shared.vectors.is_empty() {
                let vec_dir = run_dir.join("vectors");
                std::fs::create_dir_all(&vec_dir)
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                for (&p_id, arena) in &shared.vectors {
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

            // F.4: Build IndexStats directly from IdStatsHook + class stats.
            let index_stats: fluree_db_core::index_stats::IndexStats = {
                let (id_result, agg_props, _class_counts, _class_properties, _class_ref_targets) =
                    stats_hook.finalize_with_aggregate_properties();

                // Per-graph stats (already the correct type).
                let graphs = id_result.graphs;

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

                // Class stats from SPOT merge.
                let classes = if let Some(ref cs) = spot_class_stats {
                    let entries = crate::stats::build_class_stat_entries(
                        cs,
                        &predicate_sids,
                        &run_dir,
                        store.namespace_codes(),
                    )
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    if entries.is_empty() {
                        None
                    } else {
                        Some(entries)
                    }
                } else {
                    None
                };

                tracing::info!(
                    property_stats = properties.len(),
                    graph_count = graphs.len(),
                    class_count = classes.as_ref().map_or(0, |c| c.len()),
                    total_flakes = id_result.total_flakes,
                    "stats collected from IdStatsHook"
                );

                fluree_db_core::index_stats::IndexStats {
                    flakes: id_result.total_flakes,
                    size: 0,
                    properties: if properties.is_empty() {
                        None
                    } else {
                        Some(properties)
                    },
                    classes,
                    graphs: Some(graphs),
                }
            };

            // F.5: Convert DictRefs (v4, string-keyed maps) → DictRefsV5 (u32 vecs).
            let dict_refs_v5 = {
                let dr = dict_addresses.dict_refs;
                let numbig: Vec<(u32, ContentId)> = dr
                    .numbig
                    .iter()
                    .map(|(k, v)| (k.parse::<u32>().unwrap_or(0), v.clone()))
                    .collect();
                let vectors: Vec<run_index::VectorDictRefV5> = dr
                    .vectors
                    .iter()
                    .map(|(k, v)| run_index::VectorDictRefV5 {
                        p_id: k.parse::<u32>().unwrap_or(0),
                        manifest: v.manifest.clone(),
                        shards: v.shards.clone(),
                    })
                    .collect();
                run_index::DictRefsV5 {
                    subject_forward: dr.subject_forward,
                    subject_reverse: dr.subject_reverse,
                    string_forward: dr.string_forward,
                    string_reverse: dr.string_reverse,
                    numbig,
                    vectors,
                }
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
                default_graph_orders: uploaded_indexes.default_graph_orders,
                named_graphs: uploaded_indexes.named_graphs,
                stats: Some(index_stats),
                schema: None, // schema: requires predicate definitions (future)
                prev_index: None,
                garbage: None,
                sketch_ref: None,
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

/// Build dictionary CoW trees and upload all dictionary artifacts to CAS.
///
/// Subject and string dictionaries are built into CoW trees (branch + leaves)
/// for O(log n) lookup at query time.
///
/// Small dictionaries (graphs, datatypes, languages) are embedded inline in the
/// v4 index root and are not uploaded as separate dict blobs.
// Kept for: reference implementation of in-memory dict → CAS upload.
// Use when: a new pipeline needs to upload GlobalDicts without flat-file persistence.
// Superseded by: upload_dicts_from_disk (reads flat files, computes watermarks).
#[expect(dead_code)]
async fn upload_dicts_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    run_dir: &std::path::Path,
    dicts: &run_index::GlobalDicts,
    numbig_p_ids: &[u32],
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<run_index::DictRefs> {
    use dict_tree::builder::{self, TreeBuildResult};
    use dict_tree::forward_leaf::ForwardEntry;
    use dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind};
    use std::collections::BTreeMap;

    /// Upload a tree build result (branch + leaves) to CAS, returning CID refs.
    async fn upload_tree<S: Storage>(
        storage: &S,
        ledger_id: &str,
        result: TreeBuildResult,
        dict: DictKind,
    ) -> Result<run_index::DictTreeRefs> {
        let kind = ContentKind::DictBlob { dict };
        let mut leaf_cids = Vec::with_capacity(result.leaves.len());
        let mut hash_to_address =
            std::collections::HashMap::with_capacity(result.leaves.len().saturating_mul(2));

        for leaf in &result.leaves {
            let cas_result = storage
                .content_write_bytes(kind, ledger_id, &leaf.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            leaf_cids.push(cid_from_write(kind, &cas_result));
            hash_to_address.insert(leaf.hash.clone(), cas_result.address);
        }

        // Finalize branch with actual CAS addresses, then upload
        let (_, branch_bytes, _) = builder::finalize_branch(result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let branch_result = storage
            .content_write_bytes(kind, ledger_id, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        Ok(run_index::DictTreeRefs {
            branch: cid_from_write(kind, &branch_result),
            leaves: leaf_cids,
        })
    }

    // Build subject + string trees (CPU-bound, synchronous)
    let subject_pairs = dicts
        .subjects
        .read_all_entries()
        .map_err(|e| IndexerError::StorageRead(format!("read subject entries: {}", e)))?;

    let subject_suffix = |sid: u64, iri: &[u8]| -> (u16, Vec<u8>) {
        let ns_code = SubjectId::from_u64(sid).ns_code();
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        let prefix_bytes = prefix.as_bytes();
        let suffix = if !prefix_bytes.is_empty() && iri.starts_with(prefix_bytes) {
            &iri[prefix_bytes.len()..]
        } else {
            iri
        };
        (ns_code, suffix.to_vec())
    };

    let mut subj_fwd: Vec<ForwardEntry> = subject_pairs
        .iter()
        .map(|(sid, iri)| {
            let (_ns, suffix) = subject_suffix(*sid, iri);
            ForwardEntry {
                id: *sid,
                value: suffix,
            }
        })
        .collect();
    subj_fwd.sort_unstable_by_key(|e| e.id);

    let mut subj_rev: Vec<ReverseEntry> = subject_pairs
        .iter()
        .map(|(sid, iri)| {
            let (ns_code, suffix) = subject_suffix(*sid, iri);
            ReverseEntry {
                key: subject_reverse_key(ns_code, &suffix),
                id: *sid,
            }
        })
        .collect();
    subj_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let sf_tree = builder::build_forward_tree(subj_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject fwd tree: {}", e)))?;
    let sr_tree = builder::build_reverse_tree(subj_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject rev tree: {}", e)))?;

    let string_pairs = dicts
        .strings
        .all_entries()
        .map_err(|e| IndexerError::StorageWrite(format!("read string entries: {}", e)))?;

    let mut str_fwd: Vec<ForwardEntry> = string_pairs
        .iter()
        .map(|(id, val)| ForwardEntry {
            id: *id,
            value: val.clone(),
        })
        .collect();
    str_fwd.sort_unstable_by_key(|e| e.id);

    let mut str_rev: Vec<ReverseEntry> = string_pairs
        .into_iter()
        .map(|(id, val)| ReverseEntry { key: val, id })
        .collect();
    str_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let stf_tree = builder::build_forward_tree(str_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string fwd tree: {}", e)))?;
    let str_tree = builder::build_reverse_tree(str_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string rev tree: {}", e)))?;

    // Upload all trees + numbig + vectors in parallel
    let (subject_trees, string_trees, numbig, vectors) = tokio::try_join!(
        // Task A: subject trees
        async {
            let subject_forward =
                upload_tree(storage, ledger_id, sf_tree, DictKind::SubjectForward).await?;
            let subject_reverse =
                upload_tree(storage, ledger_id, sr_tree, DictKind::SubjectReverse).await?;
            Ok::<_, IndexerError>((subject_forward, subject_reverse))
        },
        // Task B: string trees
        async {
            let string_forward =
                upload_tree(storage, ledger_id, stf_tree, DictKind::StringForward).await?;
            let string_reverse =
                upload_tree(storage, ledger_id, str_tree, DictKind::StringReverse).await?;
            Ok::<_, IndexerError>((string_forward, string_reverse))
        },
        // Task C: numbig arenas
        async {
            let mut numbig = BTreeMap::new();
            let nb_dir = run_dir.join("numbig");
            for &p_id in numbig_p_ids {
                let path = nb_dir.join(format!("p_{}.nba", p_id));
                if path.exists() {
                    let (cid, _) = upload_dict_file(
                        storage,
                        ledger_id,
                        &path,
                        DictKind::NumBig { p_id },
                        "dict artifact uploaded to CAS",
                    )
                    .await?;
                    numbig.insert(p_id.to_string(), cid);
                }
            }
            Ok::<_, IndexerError>(numbig)
        },
        // Task D: vector arenas
        async {
            let mut vectors = BTreeMap::new();
            let vec_dir = run_dir.join("vectors");
            if vec_dir.exists() {
                for (&p_id, arena) in &dicts.vectors {
                    if arena.is_empty() {
                        continue;
                    }
                    let cap = run_index::vector_arena::SHARD_CAPACITY as usize;
                    let total = arena.len() as usize;
                    let num_shards = total.div_ceil(cap);
                    let mut shard_cids = Vec::with_capacity(num_shards);
                    let mut shard_infos = Vec::with_capacity(num_shards);

                    for shard_idx in 0..num_shards {
                        let shard_path = vec_dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
                        let (shard_cid, shard_wr) = upload_dict_file(
                            storage,
                            ledger_id,
                            &shard_path,
                            DictKind::VectorShard { p_id },
                            "dict artifact uploaded to CAS",
                        )
                        .await?;
                        let start_vec = shard_idx * cap;
                        let count = (total - start_vec).min(cap) as u32;
                        shard_infos.push(run_index::vector_arena::ShardInfo {
                            cas: shard_wr.address,
                            count,
                        });
                        shard_cids.push(shard_cid);
                    }

                    let manifest_path = vec_dir.join(format!("p_{}.vam", p_id));
                    run_index::vector_arena::write_vector_manifest(
                        &manifest_path,
                        arena,
                        &shard_infos,
                    )
                    .map_err(|e| {
                        IndexerError::StorageWrite(format!("write vector manifest: {}", e))
                    })?;
                    let (manifest_cid, _) = upload_dict_file(
                        storage,
                        ledger_id,
                        &manifest_path,
                        DictKind::VectorManifest { p_id },
                        "dict artifact uploaded to CAS",
                    )
                    .await?;

                    vectors.insert(
                        p_id.to_string(),
                        run_index::VectorDictRef {
                            manifest: manifest_cid,
                            shards: shard_cids,
                        },
                    );
                }
            }
            Ok::<_, IndexerError>(vectors)
        },
    )?;

    let (subject_forward, subject_reverse) = subject_trees;
    let (string_forward, string_reverse) = string_trees;

    tracing::info!(
        subject_count = dicts.subjects.len(),
        string_count = dicts.strings.len(),
        numbig_count = numbig.len(),
        vector_count = vectors.len(),
        "dictionary trees built and uploaded to CAS"
    );

    Ok(run_index::DictRefs {
        subject_forward,
        subject_reverse,
        string_forward,
        string_reverse,
        numbig,
        vectors,
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
    use dict_tree::branch::{id_to_branch_key, BranchLeafEntry, DictBranch};
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

            // Pass 1: forward tree
            tracing::info!("building subject forward tree");
            // Fast path: when subjects were produced by vocab-merge, `subjects.sids` and
            // the forward file are already in ascending sid64 order (and in `(ns_code, suffix)`
            // key order). Avoid an O(n log n) sort of indices in that case.
            let sids_sorted = sids.windows(2).all(|w| w[0] <= w[1]);

            let id_order: Option<Vec<usize>> = if sids_sorted {
                None
            } else {
                let mut v: Vec<usize> = (0..sids.len()).collect();
                v.sort_unstable_by_key(|&i| sids[i]);
                Some(v)
            };
            let subject_forward = {
                let kind = ContentKind::DictBlob {
                    dict: DictKind::SubjectForward,
                };
                let mut leaf_cids: Vec<ContentId> = Vec::new();
                let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                let mut leaf_offsets: Vec<u32> = Vec::new();
                let mut leaf_data: Vec<u8> = Vec::new();
                let mut data_offset: u32 = 0;
                let mut leaf_first: u64 = 0;
                let mut leaf_last: u64 = 0;
                let mut leaf_count: u32 = 0;

                match &id_order {
                    None => {
                        for i in 0..sids.len() {
                            let sid = sids[i];
                            let off = suf_offs[i] as usize;
                            let len = suf_lens[i] as usize;
                            let suffix = &subj_fwd_data[off..off + len];

                            if leaf_count == 0 {
                                leaf_first = sid;
                            }
                            leaf_last = sid;

                            leaf_offsets.push(data_offset);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());
                            leaf_data.extend_from_slice(&(suffix.len() as u32).to_le_bytes());
                            leaf_data.extend_from_slice(suffix);
                            data_offset += (12 + suffix.len()) as u32;
                            leaf_count += 1;

                            if (leaf_data.len() + leaf_offsets.len() * 4 + 8)
                                >= builder::DEFAULT_TARGET_LEAF_BYTES
                            {
                                if let Some((leaf_bytes, first_id, last_id, cnt)) =
                                    flush_forward_leaf(
                                        &mut leaf_offsets,
                                        &mut leaf_data,
                                        &mut leaf_count,
                                        &mut data_offset,
                                        &mut leaf_first,
                                        &mut leaf_last,
                                    )
                                {
                                    let cas_result = storage
                                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    leaf_cids.push(cid_from_write(kind, &cas_result));
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: id_to_branch_key(first_id),
                                        last_key: id_to_branch_key(last_id),
                                        entry_count: cnt,
                                        address: cas_result.address,
                                    });
                                }
                            }
                        }
                    }
                    Some(order) => {
                        for &i in order {
                            let sid = sids[i];
                            let off = suf_offs[i] as usize;
                            let len = suf_lens[i] as usize;
                            let suffix = &subj_fwd_data[off..off + len];

                            if leaf_count == 0 {
                                leaf_first = sid;
                            }
                            leaf_last = sid;

                            leaf_offsets.push(data_offset);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());
                            leaf_data.extend_from_slice(&(suffix.len() as u32).to_le_bytes());
                            leaf_data.extend_from_slice(suffix);
                            data_offset += (12 + suffix.len()) as u32;
                            leaf_count += 1;

                            if (leaf_data.len() + leaf_offsets.len() * 4 + 8)
                                >= builder::DEFAULT_TARGET_LEAF_BYTES
                            {
                                if let Some((leaf_bytes, first_id, last_id, cnt)) =
                                    flush_forward_leaf(
                                        &mut leaf_offsets,
                                        &mut leaf_data,
                                        &mut leaf_count,
                                        &mut data_offset,
                                        &mut leaf_first,
                                        &mut leaf_last,
                                    )
                                {
                                    let cas_result = storage
                                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    leaf_cids.push(cid_from_write(kind, &cas_result));
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: id_to_branch_key(first_id),
                                        last_key: id_to_branch_key(last_id),
                                        entry_count: cnt,
                                        address: cas_result.address,
                                    });
                                }
                            }
                        }
                    }
                }

                if let Some((leaf_bytes, first_id, last_id, cnt)) = flush_forward_leaf(
                    &mut leaf_offsets,
                    &mut leaf_data,
                    &mut leaf_count,
                    &mut data_offset,
                    &mut leaf_first,
                    &mut leaf_last,
                ) {
                    let cas_result = storage
                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    leaf_cids.push(cid_from_write(kind, &cas_result));
                    branch_entries.push(BranchLeafEntry {
                        first_key: id_to_branch_key(first_id),
                        last_key: id_to_branch_key(last_id),
                        entry_count: cnt,
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

                run_index::DictTreeRefs {
                    branch: cid_from_write(kind, &branch_result),
                    leaves: leaf_cids,
                }
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

            tracing::info!("subject CoW trees uploaded");
            Ok::<_, IndexerError>((subject_forward, subject_reverse))
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

                // Pass 1: forward tree
                tracing::info!("building string forward tree (streaming, zero-copy)");
                // IDs are 0..count contiguous and already in order of the forward file.
                let mut leaf_offsets: Vec<u32> = Vec::new();
                let mut leaf_data: Vec<u8> = Vec::new();
                let mut data_offset: u32 = 0;
                let mut leaf_first: u64 = 0;
                let mut leaf_last: u64 = 0;
                let mut leaf_count: u32 = 0;
                let kind = ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                };
                let mut leaf_cids: Vec<ContentId> = Vec::new();
                let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                for i in 0..count {
                    let off = str_offsets[i] as usize;
                    let len = str_lens[i] as usize;
                    let bytes = &str_fwd_data[off..off + len];
                    let id = i as u64;
                    if leaf_count == 0 {
                        leaf_first = id;
                    }
                    leaf_last = id;
                    leaf_offsets.push(data_offset);
                    leaf_data.extend_from_slice(&id.to_le_bytes());
                    leaf_data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    leaf_data.extend_from_slice(bytes);
                    data_offset += (12 + bytes.len()) as u32;
                    leaf_count += 1;

                    if (leaf_data.len() + leaf_offsets.len() * 4 + 8)
                        >= builder::DEFAULT_TARGET_LEAF_BYTES
                    {
                        if let Some((leaf_bytes, first_id, last_id, cnt)) = flush_forward_leaf(
                            &mut leaf_offsets,
                            &mut leaf_data,
                            &mut leaf_count,
                            &mut data_offset,
                            &mut leaf_first,
                            &mut leaf_last,
                        ) {
                            let cas_result = storage
                                .content_write_bytes(kind, ledger_id, &leaf_bytes)
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                            leaf_cids.push(cid_from_write(kind, &cas_result));
                            branch_entries.push(BranchLeafEntry {
                                first_key: id_to_branch_key(first_id),
                                last_key: id_to_branch_key(last_id),
                                entry_count: cnt,
                                address: cas_result.address,
                            });
                        }
                    }
                }

                if let Some((leaf_bytes, first_id, last_id, cnt)) = flush_forward_leaf(
                    &mut leaf_offsets,
                    &mut leaf_data,
                    &mut leaf_count,
                    &mut data_offset,
                    &mut leaf_first,
                    &mut leaf_last,
                ) {
                    let cas_result = storage
                        .content_write_bytes(kind, ledger_id, &leaf_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    leaf_cids.push(cid_from_write(kind, &cas_result));
                    branch_entries.push(BranchLeafEntry {
                        first_key: id_to_branch_key(first_id),
                        last_key: id_to_branch_key(last_id),
                        entry_count: cnt,
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
                let sf = run_index::DictTreeRefs {
                    branch: cid_from_write(kind, &branch_result),
                    leaves: leaf_cids,
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

                tracing::info!("string CoW trees uploaded");
                Ok::<_, IndexerError>((count, sf, sr))
            } else {
                // No strings persisted — empty trees
                // No strings persisted — empty trees (branch with no leaves)
                let kind_fwd = ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                };
                let kind_rev = ContentKind::DictBlob {
                    dict: DictKind::StringReverse,
                };
                let empty_branch = DictBranch { leaves: vec![] };
                let empty_bytes = empty_branch.encode();
                let wr_fwd = storage
                    .content_write_bytes(kind_fwd, ledger_id, &empty_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                let wr_rev = storage
                    .content_write_bytes(kind_rev, ledger_id, &empty_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Ok((
                    0,
                    run_index::DictTreeRefs {
                        branch: cid_from_write(kind_fwd, &wr_fwd),
                        leaves: vec![],
                    },
                    run_index::DictTreeRefs {
                        branch: cid_from_write(kind_rev, &wr_rev),
                        leaves: vec![],
                    },
                ))
            }
        },
        // Task C: Numbig arenas
        async {
            let mut numbig = BTreeMap::new();
            let nb_dir = run_dir.join("numbig");
            if nb_dir.exists() {
                for entry in std::fs::read_dir(&nb_dir)
                    .map_err(|e| IndexerError::StorageRead(format!("read numbig dir: {}", e)))?
                {
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
                                numbig.insert(p_id.to_string(), cid);
                            }
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(numbig)
        },
        // Task D: Vector arenas
        async {
            let mut vectors = BTreeMap::new();
            let vec_dir = run_dir.join("vectors");
            if vec_dir.exists() {
                for entry in std::fs::read_dir(&vec_dir)
                    .map_err(|e| IndexerError::StorageRead(format!("read vectors dir: {}", e)))?
                {
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
                                    run_index::vector_arena::read_vector_manifest(&manifest_bytes)
                                        .map_err(|e| {
                                            IndexerError::StorageRead(format!(
                                                "parse vector manifest: {}",
                                                e
                                            ))
                                        })?;

                                let mut shard_cids = Vec::with_capacity(manifest.shards.len());
                                let mut shard_infos = Vec::with_capacity(manifest.shards.len());
                                for (shard_idx, shard_info) in manifest.shards.iter().enumerate() {
                                    let shard_path =
                                        vec_dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
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

                                let final_manifest = run_index::vector_arena::VectorManifest {
                                    shards: shard_infos,
                                    ..manifest
                                };
                                let manifest_json = serde_json::to_vec_pretty(&final_manifest)
                                    .map_err(|e| {
                                        IndexerError::StorageWrite(format!(
                                            "serialize vector manifest: {}",
                                            e
                                        ))
                                    })?;
                                let final_manifest_path =
                                    vec_dir.join(format!("p_{}_final.vam", p_id));
                                std::fs::write(&final_manifest_path, &manifest_json).map_err(
                                    |e| {
                                        IndexerError::StorageWrite(format!(
                                            "write final vector manifest: {}",
                                            e
                                        ))
                                    },
                                )?;
                                let (manifest_cid, _) = upload_dict_file(
                                    storage,
                                    ledger_id,
                                    &final_manifest_path,
                                    DictKind::VectorManifest { p_id },
                                    "dict artifact uploaded to CAS (from disk)",
                                )
                                .await?;

                                vectors.insert(
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
            }
            Ok::<_, IndexerError>(vectors)
        },
    )?;

    let (subject_forward, subject_reverse) = subject_trees;
    let (string_count, string_forward, string_reverse) = string_result;

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
        numbig_count = numbig.len(),
        vector_count = vectors.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        "dictionary trees built and uploaded to CAS (from disk)"
    );

    Ok(UploadedDicts {
        dict_refs: run_index::DictRefs {
            subject_forward,
            subject_reverse,
            string_forward,
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
}
