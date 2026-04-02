//! Incremental commit resolution for the V6 (FIR6) index root.
//!
//! V6-native version of `incremental_resolve.rs`. Loads an `IndexRoot`
//! instead of `IndexRoot`, builds an `OTypeRegistry`, and produces
//! `RunRecordV2` output with per-record op bytes.
//!
//! The commit chain walking, reconciliation, and remap logic is reused from
//! the V5 module — those are format-agnostic. The differences are:
//!
//! 1. Root type: `IndexRoot` (shared `DictRefs` for dict trees)
//! 2. Output records: `Vec<RunRecordV2>` + `Vec<u8>` (parallel ops)
//! 3. `OTypeRegistry` built from root's `datatype_iris` + `language_tags`

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_binary_index::dict::DictTreeReader;
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_binary_index::format::run_record::{cmp_for_order, RunRecord, RunSortOrder};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_core::content_id::ContentId;
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::DatatypeDictId;

use crate::run_index::resolve::resolver::{RebuildChunk, ResolverError, SharedResolverState};

// ============================================================================
// Error type
// ============================================================================

/// Errors specific to incremental resolution.
#[derive(Debug)]
pub enum IncrementalResolveError {
    /// Root loading or decoding failed.
    RootLoad(String),
    /// Dict tree loading failed.
    DictTreeLoad(String),
    /// Commit chain walking failed.
    CommitChain(String),
    /// Resolution failed.
    Resolve(ResolverError),
    /// I/O error.
    Io(io::Error),
}

impl std::fmt::Display for IncrementalResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RootLoad(msg) => write!(f, "root load: {msg}"),
            Self::DictTreeLoad(msg) => write!(f, "dict tree load: {msg}"),
            Self::CommitChain(msg) => write!(f, "commit chain: {msg}"),
            Self::Resolve(e) => write!(f, "resolve: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
        }
    }
}

impl std::error::Error for IncrementalResolveError {}

impl From<io::Error> for IncrementalResolveError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ResolverError> for IncrementalResolveError {
    fn from(e: ResolverError) -> Self {
        Self::Resolve(e)
    }
}

// ============================================================================
// Types
// ============================================================================

/// Configuration for V6 incremental commit resolution.
pub struct IncrementalResolveConfig {
    /// CID of the base FIR6 index root.
    pub base_root_id: ContentId,
    /// CID of the head commit (latest commit to include).
    pub head_commit_id: ContentId,
    /// Only include commits with `t > from_t` (typically `root.index_t`).
    pub from_t: i64,
}

/// Result of V6 incremental commit resolution.
///
/// Contains globally-addressed `RunRecordV2` records sorted by graph+SPOT,
/// with parallel `ops` array, plus metadata for downstream phases.
pub struct IncrementalNovelty {
    /// Globally-addressed RunRecordV2 records, sorted by (g_id, SPOT).
    pub records: Vec<RunRecordV2>,
    /// Parallel ops array: 1=assert, 0=retract. Same length as `records`.
    pub ops: Vec<u8>,
    /// The decoded base root (needed by downstream phases).
    pub base_root: IndexRoot,
    /// Updated resolver state.
    pub shared: SharedResolverState,
    /// New subject entries not found in reverse tree.
    ///
    /// **Invariant**: sorted by `(ns_code, local_id)` ascending. Within each
    /// namespace, local_ids are contiguous starting at `watermark + 1`.
    /// Consumed directly by forward pack builders (no re-sorting needed).
    pub new_subjects: Vec<(u16, u64, Vec<u8>)>,
    /// New string entries not found in reverse tree.
    ///
    /// **Invariant**: sorted by `string_id` ascending. IDs are contiguous
    /// starting at `string_watermark + 1`. Consumed directly by forward
    /// pack builders (no re-sorting needed).
    pub new_strings: Vec<(u32, Vec<u8>)>,
    /// Updated subject watermarks.
    pub updated_watermarks: Vec<u64>,
    /// Updated string watermark.
    pub updated_string_watermark: u32,
    /// Maximum t value across all resolved commits.
    pub max_t: i64,
    /// Cumulative commit blob size.
    pub delta_commit_size: u64,
    /// Total assertions.
    pub delta_asserts: u64,
    /// Total retractions.
    pub delta_retracts: u64,
    /// Base vector arena counts per (g_id, p_id) for handle offsetting.
    pub base_vector_counts: HashMap<(u16, u32), u32>,
    /// Base numbig arena counts per (g_id, p_id).
    pub base_numbig_counts: HashMap<(u16, u32), usize>,
    /// String text bytes for fulltext assertion entries.
    pub fulltext_string_bytes: HashMap<u32, Vec<u8>>,
}

// ============================================================================
// Public API
// ============================================================================

/// Resolve incremental commits against a V6 (FIR6) index root.
///
/// This is the V6-native Phase 1 for the incremental indexing pipeline.
pub async fn resolve_incremental_commits_v6(
    cs: Arc<dyn ContentStore>,
    config: IncrementalResolveConfig,
) -> Result<IncrementalNovelty, IncrementalResolveError> {
    let t_start = Instant::now();
    tracing::info!(
        base_root = %config.base_root_id,
        head = %config.head_commit_id,
        from_t = config.from_t,
        "V6 incremental resolve: starting"
    );

    // 1. Load and decode IndexRoot.
    let (root_bytes, t_root_load_ms) = {
        let t0 = Instant::now();
        let bytes = cs.get(&config.base_root_id).await.map_err(|e| {
            IncrementalResolveError::RootLoad(format!(
                "failed to load FIR6 root {}: {}",
                config.base_root_id, e
            ))
        })?;
        (bytes, t0.elapsed().as_millis() as u64)
    };

    let (root, t_root_decode_ms) = {
        let t0 = Instant::now();
        let root = IndexRoot::decode(&root_bytes).map_err(|e| {
            IncrementalResolveError::RootLoad(format!("failed to decode FIR6: {}", e))
        })?;
        (root, t0.elapsed().as_millis() as u64)
    };

    tracing::info!(
        index_t = root.index_t,
        from_t = config.from_t,
        head = %config.head_commit_id,
        "V6 incremental resolve: loaded base root"
    );

    // 2. Build OTypeRegistry from root's datatype and language metadata.
    let custom_dt_iris: Vec<String> = root
        .datatype_iris
        .iter()
        .skip(DatatypeDictId::RESERVED_COUNT as usize)
        .cloned()
        .collect();
    let o_type_registry = OTypeRegistry::new(&custom_dt_iris);

    // 3. Load subject + string reverse dict trees (same DictRefs as V5).
    let (subject_tree, string_tree, t_dict_load_ms) = {
        let t0 = Instant::now();
        let subject_tree = DictTreeReader::from_refs(&cs, &root.dict_refs.subject_reverse, None)
            .await
            .map_err(|e| IncrementalResolveError::DictTreeLoad(format!("subject reverse: {e}")))?;
        let string_tree = DictTreeReader::from_refs(&cs, &root.dict_refs.string_reverse, None)
            .await
            .map_err(|e| IncrementalResolveError::DictTreeLoad(format!("string reverse: {e}")))?;
        (subject_tree, string_tree, t0.elapsed().as_millis() as u64)
    };

    // 4. Seed SharedResolverState from V6 root.
    let mut shared = SharedResolverState::from_index_root(&root)?;

    // Enable spatial hook for non-POINT geometry detection.
    shared.spatial_hook = Some(crate::spatial_hook::SpatialHook::new());
    // Enable fulltext hook.
    shared.fulltext_hook = Some(crate::fulltext_hook::FulltextHook::new());

    // 4a. Pre-seed numbig arenas.
    let (base_numbig_counts, base_vector_counts, t_seed_arenas_ms) = {
        let t0 = Instant::now();

        let mut base_numbig_counts: HashMap<(u16, u32), usize> = HashMap::new();
        for ga in &root.graph_arenas {
            if ga.numbig.is_empty() {
                continue;
            }
            let nb_map = shared.numbigs.entry(ga.g_id).or_default();
            for (p_id, cid) in &ga.numbig {
                let bytes = cs.get(cid).await.map_err(|e| {
                    IncrementalResolveError::RootLoad(format!(
                        "numbig arena load for g_id={}, p_id={}: {}",
                        ga.g_id, p_id, e
                    ))
                })?;
                let arena =
                    fluree_db_binary_index::arena::numbig::read_numbig_arena_from_bytes(&bytes)
                        .map_err(|e| {
                            IncrementalResolveError::RootLoad(format!("numbig arena decode: {}", e))
                        })?;
                base_numbig_counts.insert((ga.g_id, *p_id), arena.len());
                nb_map.insert(*p_id, arena);
            }
        }

        // 4b. Collect base vector counts.
        let mut base_vector_counts: HashMap<(u16, u32), u32> = HashMap::new();
        for ga in &root.graph_arenas {
            for vref in &ga.vectors {
                let manifest_bytes = cs.get(&vref.manifest).await.map_err(|e| {
                    IncrementalResolveError::RootLoad(format!(
                        "vector manifest load for g_id={}, p_id={}: {}",
                        ga.g_id, vref.p_id, e
                    ))
                })?;
                let manifest =
                    fluree_db_binary_index::arena::vector::read_vector_manifest(&manifest_bytes)
                        .map_err(|e| {
                            IncrementalResolveError::RootLoad(format!(
                                "vector manifest decode: {}",
                                e
                            ))
                        })?;
                base_vector_counts.insert((ga.g_id, vref.p_id), manifest.total_count);
            }
        }

        (
            base_numbig_counts,
            base_vector_counts,
            t0.elapsed().as_millis() as u64,
        )
    };

    // 5. Walk commit chain (commit format is version-independent).
    let (commit_cids, t_walk_chain_ms) = {
        let t0 = Instant::now();
        let cids =
            walk_commit_chain_since(cs.as_ref(), &config.head_commit_id, config.from_t).await?;
        (cids, t0.elapsed().as_millis() as u64)
    };

    // 6. Resolve commits into chunk.
    let (
        chunk,
        max_t,
        delta_commit_size,
        delta_asserts,
        delta_retracts,
        commit_count,
        t_commit_resolve_ms,
    ) = {
        let t0 = Instant::now();
        let mut chunk = RebuildChunk::new();
        let mut max_t: i64 = root.index_t;
        let mut delta_commit_size = 0u64;
        let mut delta_asserts = 0u64;
        let mut delta_retracts = 0u64;
        let mut commit_count = 0usize;

        for cid in &commit_cids {
            let bytes = cs.get(cid).await.map_err(|e| {
                IncrementalResolveError::CommitChain(format!(
                    "failed to load commit {}: {}",
                    cid, e
                ))
            })?;
            let envelope =
                fluree_db_novelty::commit_v2::read_commit_envelope(&bytes).map_err(|e| {
                    IncrementalResolveError::CommitChain(format!(
                        "failed to decode envelope for {}: {}",
                        cid, e
                    ))
                })?;
            let resolved = shared
                .resolve_commit_into_chunk(&bytes, &cid.digest_hex(), &mut chunk)
                .map_err(IncrementalResolveError::Resolve)?;

            max_t = max_t.max(envelope.t);
            delta_commit_size += resolved.size;
            delta_asserts += resolved.asserts as u64;
            delta_retracts += resolved.retracts as u64;
            commit_count += 1;

            if commit_count.is_multiple_of(500) {
                tracing::info!(
                    commit_count,
                    total_commits = commit_cids.len(),
                    current_t = envelope.t,
                    chunk_records = chunk.records.len(),
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "V6 incremental resolve: commit resolution progress"
                );
            } else if commit_count.is_multiple_of(100) {
                tracing::debug!(
                    commit_count,
                    total_commits = commit_cids.len(),
                    current_t = envelope.t,
                    chunk_records = chunk.records.len(),
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "V6 incremental resolve: commit resolution progress"
                );
            }
        }

        (
            chunk,
            max_t,
            delta_commit_size,
            delta_asserts,
            delta_retracts,
            commit_count,
            t0.elapsed().as_millis() as u64,
        )
    };

    tracing::info!(
        commit_count,
        records = chunk.records.len(),
        max_t,
        "V6 incremental resolve: commits resolved into chunk"
    );

    // Cache watermarks before potential root move.
    let base_subject_watermarks = root.subject_watermarks.clone();
    let base_string_watermark = root.string_watermark;

    if chunk.records.is_empty() {
        tracing::info!(
            root_load_ms = t_root_load_ms,
            root_decode_ms = t_root_decode_ms,
            dict_load_ms = t_dict_load_ms,
            seed_arenas_ms = t_seed_arenas_ms,
            walk_chain_ms = t_walk_chain_ms,
            commit_resolve_ms = t_commit_resolve_ms,
            total_ms = t_start.elapsed().as_millis() as u64,
            commit_count,
            "V6 incremental resolve: timings (no records)"
        );
        return Ok(IncrementalNovelty {
            records: Vec::new(),
            ops: Vec::new(),
            base_root: root,
            shared,
            updated_watermarks: base_subject_watermarks,
            updated_string_watermark: base_string_watermark,
            new_subjects: Vec::new(),
            new_strings: Vec::new(),
            max_t,
            delta_commit_size,
            delta_asserts,
            delta_retracts,
            base_vector_counts,
            base_numbig_counts,
            fulltext_string_bytes: HashMap::new(),
        });
    }

    // 7. Reconcile chunk-local IDs to global IDs (same algorithm as V5).
    let reconcile = reconcile_chunk_to_global(
        &chunk,
        &subject_tree,
        &string_tree,
        &base_subject_watermarks,
        base_string_watermark,
    )?;
    let t_reconcile_ms = t_start.elapsed().as_millis().saturating_sub(
        (t_root_load_ms
            + t_root_decode_ms
            + t_dict_load_ms
            + t_seed_arenas_ms
            + t_walk_chain_ms
            + t_commit_resolve_ms) as u128,
    ) as u64;
    // Note: the above gives a conservative aggregate since start; we also measure precise
    // step timings below where possible.

    // 8. Remap fulltext hook entries.
    let t0 = Instant::now();
    let fulltext_string_bytes: HashMap<u32, Vec<u8>> = {
        let chunk_forward = chunk.strings.forward_entries();
        if let Some(ref mut ft) = shared.fulltext_hook {
            let mut map = HashMap::new();
            for entry in ft.entries_mut() {
                let local_id = entry.string_id as usize;
                let global_id = match reconcile.string_remap.get(local_id) {
                    Some(&id) => id,
                    None => {
                        tracing::warn!(local_id, "fulltext entry string_id remap miss; skipping");
                        entry.is_assert = false;
                        entry.string_id = u32::MAX;
                        continue;
                    }
                };
                if entry.is_assert {
                    if let Some(bytes) = chunk_forward.get(local_id) {
                        map.entry(global_id).or_insert_with(|| bytes.clone());
                    }
                }
                entry.string_id = global_id;
            }
            map
        } else {
            HashMap::new()
        }
    };
    let t_remap_fulltext_ms = t0.elapsed().as_millis() as u64;

    // 9. Remap all V1 records in-place, then convert to V2.
    let t0 = Instant::now();
    let mut v1_records = chunk.records;
    for record in &mut v1_records {
        remap_record(record, &reconcile.subject_remap, &reconcile.string_remap)?;
    }
    let t_remap_records_ms = t0.elapsed().as_millis() as u64;

    // Offset vector handles.
    if !base_vector_counts.is_empty() {
        for record in &mut v1_records {
            if ObjKind::from_u8(record.o_kind) == ObjKind::VECTOR_ID {
                let key = (record.g_id, record.p_id);
                if let Some(&base_count) = base_vector_counts.get(&key) {
                    record.o_key += base_count as u64;
                }
            }
        }
    }

    // 10. Sort V1 records by (g_id, SPOT) — needed for the conversion step
    //     which preserves sort order.
    let t0 = Instant::now();
    let spot_cmp = cmp_for_order(RunSortOrder::Spot);
    v1_records.sort_unstable_by(|a, b| a.g_id.cmp(&b.g_id).then_with(|| spot_cmp(a, b)));
    let t_sort_ms = t0.elapsed().as_millis() as u64;

    // 11. Convert V1 → V2 + extract ops.
    let t0 = Instant::now();
    let mut records = Vec::with_capacity(v1_records.len());
    let mut ops = Vec::with_capacity(v1_records.len());
    for v1 in &v1_records {
        records.push(RunRecordV2::from_v1(v1, &o_type_registry));
        ops.push(v1.op);
    }
    let t_convert_ms = t0.elapsed().as_millis() as u64;

    tracing::info!(
        root_load_ms = t_root_load_ms,
        root_decode_ms = t_root_decode_ms,
        dict_load_ms = t_dict_load_ms,
        seed_arenas_ms = t_seed_arenas_ms,
        walk_chain_ms = t_walk_chain_ms,
        commit_resolve_ms = t_commit_resolve_ms,
        reconcile_ms = t_reconcile_ms,
        remap_fulltext_ms = t_remap_fulltext_ms,
        remap_records_ms = t_remap_records_ms,
        sort_ms = t_sort_ms,
        convert_ms = t_convert_ms,
        total_ms = t_start.elapsed().as_millis() as u64,
        commit_count,
        record_count = records.len(),
        new_subjects = reconcile.new_subjects.len(),
        new_strings = reconcile.new_strings.len(),
        "V6 incremental resolve: timings"
    );

    Ok(IncrementalNovelty {
        records,
        ops,
        base_root: root,
        shared,
        new_subjects: reconcile.new_subjects,
        new_strings: reconcile.new_strings,
        updated_watermarks: reconcile.updated_watermarks,
        updated_string_watermark: reconcile.updated_string_watermark,
        max_t,
        delta_commit_size,
        delta_asserts,
        delta_retracts,
        base_vector_counts,
        base_numbig_counts,
        fulltext_string_bytes,
    })
}

// ============================================================================
// Internal: Commit Chain Walking
// ============================================================================

async fn walk_commit_chain_since(
    cs: &dyn ContentStore,
    head_id: &ContentId,
    from_t: i64,
) -> Result<Vec<ContentId>, IncrementalResolveError> {
    let mut cids = Vec::new();
    let mut current = Some(head_id.clone());
    let walk_started = Instant::now();

    while let Some(cid) = current {
        let bytes = cs.get(&cid).await.map_err(|e| {
            IncrementalResolveError::CommitChain(format!("failed to load commit {}: {}", cid, e))
        })?;
        let envelope = fluree_db_novelty::commit_v2::read_commit_envelope(&bytes).map_err(|e| {
            IncrementalResolveError::CommitChain(format!(
                "failed to decode envelope for {}: {}",
                cid, e
            ))
        })?;

        if envelope.t <= from_t {
            break;
        }

        current = envelope.previous_ref.map(|r| r.id);
        cids.push(cid);

        let walked = cids.len();
        if walked % 500 == 0 {
            tracing::info!(
                commits_walked = walked,
                current_t = envelope.t,
                from_t,
                elapsed_ms = walk_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: commit-chain walk progress"
            );
        } else if walked % 100 == 0 {
            tracing::debug!(
                commits_walked = walked,
                current_t = envelope.t,
                from_t,
                elapsed_ms = walk_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: commit-chain walk progress"
            );
        }
    }

    cids.reverse();
    tracing::info!(
        commits = cids.len(),
        from_t,
        head = %head_id,
        elapsed_ms = walk_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: commit-chain walk complete"
    );
    Ok(cids)
}

// ============================================================================
// Internal: Reconciliation (reused from V5 — same algorithm)
// ============================================================================

use fluree_db_binary_index::dict::reverse_leaf::subject_reverse_key;

struct ReconcileResult {
    subject_remap: Vec<u64>,
    string_remap: Vec<u32>,
    new_subjects: Vec<(u16, u64, Vec<u8>)>,
    new_strings: Vec<(u32, Vec<u8>)>,
    updated_watermarks: Vec<u64>,
    updated_string_watermark: u32,
}

fn reconcile_chunk_to_global(
    chunk: &RebuildChunk,
    subject_tree: &DictTreeReader,
    string_tree: &DictTreeReader,
    subject_watermarks: &[u64],
    string_watermark: u32,
) -> Result<ReconcileResult, IncrementalResolveError> {
    const INFO_PROGRESS_EVERY: usize = 250;
    const DEBUG_PROGRESS_EVERY: usize = 50;
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
    const SLOW_LOOKUP_WARN_MS: u64 = 250;

    // Subject reconciliation.
    let subject_entries = chunk.subjects.forward_entries();
    let subject_started = Instant::now();
    let subject_reads_before = subject_tree.disk_reads();
    let subject_local_file_reads_before = subject_tree.local_file_reads();
    let subject_remote_fetches_before = subject_tree.remote_fetches();
    let subject_hits_before = subject_tree.cache_hits();
    let subject_cache_misses_before = subject_tree.cache_misses();
    let mut subject_remap = vec![0u64; subject_entries.len()];
    let mut new_subjects = Vec::new();
    let mut ns_next_local: HashMap<u16, u64> = HashMap::new();
    let mut updated_watermarks = subject_watermarks.to_vec();
    let mut subject_existing = 0usize;
    let mut subject_new = 0usize;
    let mut subject_last_heartbeat = Instant::now();

    tracing::info!(
        subject_entries = subject_entries.len(),
        subject_tree_entries = subject_tree.total_entries(),
        subject_tree_source = subject_tree.source_kind(),
        subject_tree_leaf_count = subject_tree.leaf_count(),
        subject_tree_local_file_count = subject_tree.local_file_count(),
        subject_tree_remote_cid_count = subject_tree.remote_cid_count(),
        subject_tree_has_global_cache = subject_tree.has_global_cache(),
        subject_tree_disk_reads = subject_reads_before,
        subject_tree_local_file_reads = subject_tree.local_file_reads(),
        subject_tree_remote_fetches = subject_tree.remote_fetches(),
        subject_tree_cache_hits = subject_hits_before,
        subject_tree_cache_misses = subject_tree.cache_misses(),
        "V6 incremental resolve: subject reconciliation starting"
    );

    for (chunk_local_id, (ns_code, name_bytes)) in subject_entries.iter().enumerate() {
        let reverse_key = subject_reverse_key(*ns_code, name_bytes);
        let lookup_started = Instant::now();
        let lookup_disk_reads_before = subject_tree.disk_reads();
        let lookup_local_file_reads_before = subject_tree.local_file_reads();
        let lookup_remote_fetches_before = subject_tree.remote_fetches();
        let lookup_cache_hits_before = subject_tree.cache_hits();
        let lookup_cache_misses_before = subject_tree.cache_misses();
        let existing_id = subject_tree
            .reverse_lookup(&reverse_key)
            .map_err(IncrementalResolveError::Io)?;
        let lookup_elapsed_ms = lookup_started.elapsed().as_millis() as u64;

        let global_sid64 = match existing_id {
            Some(sid64) => {
                subject_existing += 1;
                sid64
            }
            None => {
                subject_new += 1;
                let wm = if (*ns_code as usize) < updated_watermarks.len() {
                    updated_watermarks[*ns_code as usize]
                } else {
                    updated_watermarks.resize(*ns_code as usize + 1, 0);
                    0
                };
                let next = ns_next_local.entry(*ns_code).or_insert(wm + 1);
                let local_id = *next;
                *next += 1;
                updated_watermarks[*ns_code as usize] = local_id;
                let sid64 = SubjectId::new(*ns_code, local_id).as_u64();
                new_subjects.push((*ns_code, local_id, name_bytes.clone()));
                sid64
            }
        };
        subject_remap[chunk_local_id] = global_sid64;

        let processed = chunk_local_id + 1;
        if lookup_elapsed_ms >= SLOW_LOOKUP_WARN_MS {
            tracing::warn!(
                processed,
                total = subject_entries.len(),
                ns_code = *ns_code,
                name_len = name_bytes.len(),
                key_len = reverse_key.len(),
                lookup_elapsed_ms,
                lookup_disk_reads = subject_tree
                    .disk_reads()
                    .saturating_sub(lookup_disk_reads_before),
                lookup_local_file_reads = subject_tree
                    .local_file_reads()
                    .saturating_sub(lookup_local_file_reads_before),
                lookup_remote_fetches = subject_tree
                    .remote_fetches()
                    .saturating_sub(lookup_remote_fetches_before),
                lookup_cache_hits = subject_tree
                    .cache_hits()
                    .saturating_sub(lookup_cache_hits_before),
                lookup_cache_misses = subject_tree
                    .cache_misses()
                    .saturating_sub(lookup_cache_misses_before),
                "V6 incremental resolve: slow subject reverse lookup"
            );
        }

        let should_info_progress = processed % INFO_PROGRESS_EVERY == 0
            || subject_last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL;
        if should_info_progress {
            tracing::info!(
                processed,
                total = subject_entries.len(),
                existing = subject_existing,
                new = subject_new,
                last_ns_code = *ns_code,
                last_name_len = name_bytes.len(),
                disk_reads = subject_tree
                    .disk_reads()
                    .saturating_sub(subject_reads_before),
                local_file_reads = subject_tree
                    .local_file_reads()
                    .saturating_sub(subject_local_file_reads_before),
                cache_hits = subject_tree
                    .cache_hits()
                    .saturating_sub(subject_hits_before),
                remote_fetches = subject_tree
                    .remote_fetches()
                    .saturating_sub(subject_remote_fetches_before),
                cache_misses = subject_tree
                    .cache_misses()
                    .saturating_sub(subject_cache_misses_before),
                elapsed_ms = subject_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: subject reconciliation progress"
            );
            subject_last_heartbeat = Instant::now();
        } else if processed % DEBUG_PROGRESS_EVERY == 0 {
            tracing::debug!(
                processed,
                total = subject_entries.len(),
                existing = subject_existing,
                new = subject_new,
                last_ns_code = *ns_code,
                last_name_len = name_bytes.len(),
                disk_reads = subject_tree
                    .disk_reads()
                    .saturating_sub(subject_reads_before),
                local_file_reads = subject_tree
                    .local_file_reads()
                    .saturating_sub(subject_local_file_reads_before),
                cache_hits = subject_tree
                    .cache_hits()
                    .saturating_sub(subject_hits_before),
                remote_fetches = subject_tree
                    .remote_fetches()
                    .saturating_sub(subject_remote_fetches_before),
                cache_misses = subject_tree
                    .cache_misses()
                    .saturating_sub(subject_cache_misses_before),
                elapsed_ms = subject_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: subject reconciliation progress"
            );
        }
    }

    tracing::info!(
        subject_entries = subject_entries.len(),
        existing = subject_existing,
        new = subject_new,
        disk_reads = subject_tree
            .disk_reads()
            .saturating_sub(subject_reads_before),
        local_file_reads = subject_tree
            .local_file_reads()
            .saturating_sub(subject_local_file_reads_before),
        remote_fetches = subject_tree
            .remote_fetches()
            .saturating_sub(subject_remote_fetches_before),
        cache_hits = subject_tree
            .cache_hits()
            .saturating_sub(subject_hits_before),
        cache_misses = subject_tree
            .cache_misses()
            .saturating_sub(subject_cache_misses_before),
        elapsed_ms = subject_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: subject reconciliation complete"
    );

    // String reconciliation.
    let string_entries = chunk.strings.forward_entries();
    let string_started = Instant::now();
    let string_reads_before = string_tree.disk_reads();
    let string_local_file_reads_before = string_tree.local_file_reads();
    let string_remote_fetches_before = string_tree.remote_fetches();
    let string_hits_before = string_tree.cache_hits();
    let string_cache_misses_before = string_tree.cache_misses();
    let mut string_remap = vec![0u32; string_entries.len()];
    let mut new_strings = Vec::new();
    let mut next_string_id = string_watermark + 1;
    let mut string_existing = 0usize;
    let mut string_new = 0usize;
    let mut string_last_heartbeat = Instant::now();

    tracing::info!(
        string_entries = string_entries.len(),
        string_tree_entries = string_tree.total_entries(),
        string_tree_source = string_tree.source_kind(),
        string_tree_leaf_count = string_tree.leaf_count(),
        string_tree_local_file_count = string_tree.local_file_count(),
        string_tree_remote_cid_count = string_tree.remote_cid_count(),
        string_tree_has_global_cache = string_tree.has_global_cache(),
        string_tree_disk_reads = string_reads_before,
        string_tree_local_file_reads = string_tree.local_file_reads(),
        string_tree_remote_fetches = string_tree.remote_fetches(),
        string_tree_cache_hits = string_hits_before,
        string_tree_cache_misses = string_tree.cache_misses(),
        "V6 incremental resolve: string reconciliation starting"
    );

    for (chunk_local_id, value_bytes) in string_entries.iter().enumerate() {
        let lookup_started = Instant::now();
        let lookup_disk_reads_before = string_tree.disk_reads();
        let lookup_local_file_reads_before = string_tree.local_file_reads();
        let lookup_remote_fetches_before = string_tree.remote_fetches();
        let lookup_cache_hits_before = string_tree.cache_hits();
        let lookup_cache_misses_before = string_tree.cache_misses();
        let existing_id = string_tree
            .reverse_lookup(value_bytes)
            .map_err(IncrementalResolveError::Io)?;
        let lookup_elapsed_ms = lookup_started.elapsed().as_millis() as u64;

        let global_str_id = match existing_id {
            Some(id) => {
                string_existing += 1;
                id as u32
            }
            None => {
                string_new += 1;
                let id = next_string_id;
                next_string_id += 1;
                new_strings.push((id, value_bytes.clone()));
                id
            }
        };
        string_remap[chunk_local_id] = global_str_id;

        let processed = chunk_local_id + 1;
        if lookup_elapsed_ms >= SLOW_LOOKUP_WARN_MS {
            tracing::warn!(
                processed,
                total = string_entries.len(),
                value_len = value_bytes.len(),
                lookup_elapsed_ms,
                lookup_disk_reads = string_tree
                    .disk_reads()
                    .saturating_sub(lookup_disk_reads_before),
                lookup_local_file_reads = string_tree
                    .local_file_reads()
                    .saturating_sub(lookup_local_file_reads_before),
                lookup_remote_fetches = string_tree
                    .remote_fetches()
                    .saturating_sub(lookup_remote_fetches_before),
                lookup_cache_hits = string_tree
                    .cache_hits()
                    .saturating_sub(lookup_cache_hits_before),
                lookup_cache_misses = string_tree
                    .cache_misses()
                    .saturating_sub(lookup_cache_misses_before),
                "V6 incremental resolve: slow string reverse lookup"
            );
        }

        let should_info_progress = processed % INFO_PROGRESS_EVERY == 0
            || string_last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL;
        if should_info_progress {
            tracing::info!(
                processed,
                total = string_entries.len(),
                existing = string_existing,
                new = string_new,
                last_value_len = value_bytes.len(),
                disk_reads = string_tree.disk_reads().saturating_sub(string_reads_before),
                local_file_reads = string_tree
                    .local_file_reads()
                    .saturating_sub(string_local_file_reads_before),
                cache_hits = string_tree.cache_hits().saturating_sub(string_hits_before),
                remote_fetches = string_tree
                    .remote_fetches()
                    .saturating_sub(string_remote_fetches_before),
                cache_misses = string_tree
                    .cache_misses()
                    .saturating_sub(string_cache_misses_before),
                elapsed_ms = string_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: string reconciliation progress"
            );
            string_last_heartbeat = Instant::now();
        } else if processed % DEBUG_PROGRESS_EVERY == 0 {
            tracing::debug!(
                processed,
                total = string_entries.len(),
                existing = string_existing,
                new = string_new,
                last_value_len = value_bytes.len(),
                disk_reads = string_tree.disk_reads().saturating_sub(string_reads_before),
                local_file_reads = string_tree
                    .local_file_reads()
                    .saturating_sub(string_local_file_reads_before),
                cache_hits = string_tree.cache_hits().saturating_sub(string_hits_before),
                remote_fetches = string_tree
                    .remote_fetches()
                    .saturating_sub(string_remote_fetches_before),
                cache_misses = string_tree
                    .cache_misses()
                    .saturating_sub(string_cache_misses_before),
                elapsed_ms = string_started.elapsed().as_millis() as u64,
                "V6 incremental resolve: string reconciliation progress"
            );
        }
    }

    tracing::info!(
        string_entries = string_entries.len(),
        existing = string_existing,
        new = string_new,
        disk_reads = string_tree.disk_reads().saturating_sub(string_reads_before),
        local_file_reads = string_tree
            .local_file_reads()
            .saturating_sub(string_local_file_reads_before),
        remote_fetches = string_tree
            .remote_fetches()
            .saturating_sub(string_remote_fetches_before),
        cache_hits = string_tree.cache_hits().saturating_sub(string_hits_before),
        cache_misses = string_tree
            .cache_misses()
            .saturating_sub(string_cache_misses_before),
        elapsed_ms = string_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: string reconciliation complete"
    );

    let updated_string_watermark = if next_string_id > string_watermark + 1 {
        next_string_id - 1
    } else {
        string_watermark
    };

    // Enforce sort invariants for downstream consumers (forward pack builders).
    // Strings are already sorted (sequential IDs from watermark+1).
    debug_assert!(
        new_strings.windows(2).all(|w| w[0].0 < w[1].0),
        "new_strings must be sorted by string_id ascending"
    );
    // Subjects may be interleaved across namespaces; sort by (ns_code, local_id).
    new_subjects.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    Ok(ReconcileResult {
        subject_remap,
        string_remap,
        new_subjects,
        new_strings,
        updated_watermarks,
        updated_string_watermark,
    })
}

// ============================================================================
// Internal: Record remap
// ============================================================================

fn remap_record(
    record: &mut RunRecord,
    subject_remap: &[u64],
    string_remap: &[u32],
) -> Result<(), IncrementalResolveError> {
    let local_s = record.s_id.as_u64() as usize;
    let global_s = *subject_remap.get(local_s).ok_or_else(|| {
        IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
            "subject remap out of range: local_id={}, remap_len={}",
            local_s,
            subject_remap.len()
        )))
    })?;
    record.s_id = SubjectId::from_u64(global_s);

    let o_kind = ObjKind::from_u8(record.o_kind);
    if o_kind == ObjKind::REF_ID {
        let local_o = record.o_key as usize;
        let global_o = *subject_remap.get(local_o).ok_or_else(|| {
            IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
                "ref object remap out of range: local_id={}, remap_len={}",
                local_o,
                subject_remap.len()
            )))
        })?;
        record.o_key = global_o;
    } else if o_kind == ObjKind::LEX_ID || o_kind == ObjKind::JSON_ID {
        let local_str = ObjKey::from_u64(record.o_key).decode_u32_id() as usize;
        let global_str = *string_remap.get(local_str).ok_or_else(|| {
            IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
                "string remap out of range: local_id={}, remap_len={}",
                local_str,
                string_remap.len()
            )))
        })?;
        record.o_key = ObjKey::encode_u32_id(global_str).as_u64();
    }

    Ok(())
}
