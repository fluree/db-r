//! Incremental commit resolution: resolve only new commits against an existing index root.
//!
//! Instead of walking the entire commit chain from genesis, this module:
//! 1. Loads the existing `IndexRootV5` and seeds resolver dicts from it
//! 2. Loads subject + string reverse dict trees from CAS
//! 3. Walks only commits with `t > root.index_t`
//! 4. Resolves ops into chunk-local RunRecords
//! 5. Reconciles chunk-local IDs to global IDs using reverse tree lookups
//! 6. Returns sorted, globally-addressed RunRecords ready for leaf merging

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use fluree_db_core::content_id::ContentId;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};

use crate::run_index::resolve::resolver::{RebuildChunk, ResolverError, SharedResolverState};
use fluree_db_binary_index::dict::reverse_leaf::subject_reverse_key;
use fluree_db_binary_index::dict::{DictBranch, DictTreeReader};
use fluree_db_binary_index::format::index_root::{DictTreeRefs, IndexRootV5};
use fluree_db_binary_index::format::run_record::{cmp_for_order, RunRecord, RunSortOrder};

// ============================================================================
// Types
// ============================================================================

/// Configuration for incremental commit resolution.
pub struct IncrementalResolveConfig {
    /// CID of the base index root to build incrementally from.
    pub base_root_id: ContentId,
    /// CID of the head commit (latest commit to include).
    pub head_commit_id: ContentId,
    /// Only include commits with `t > from_t` (typically `root.index_t`).
    pub from_t: i64,
}

/// Result of incremental commit resolution.
///
/// Contains globally-addressed RunRecords sorted by graph+SPOT, plus
/// updated resolver state and metadata needed by downstream phases.
pub struct IncrementalNovelty {
    /// Globally-addressed RunRecords, sorted by (g_id, SPOT).
    pub records: Vec<RunRecord>,
    /// Updated resolver state (seeded from root, extended with new commits).
    pub shared: SharedResolverState,
    /// The decoded base root (needed by downstream phases for branch manifests, etc.).
    pub base_root: IndexRootV5,
    /// New subject entries: `(ns_code, local_id_in_ns, suffix_bytes)`.
    /// Only subjects NOT found in the existing reverse tree.
    pub new_subjects: Vec<(u16, u64, Vec<u8>)>,
    /// New string entries: `(global_string_id, value_bytes)`.
    /// Only strings NOT found in the existing reverse tree.
    pub new_strings: Vec<(u32, Vec<u8>)>,
    /// Updated subject watermarks (position-indexed by ns_code).
    pub updated_watermarks: Vec<u64>,
    /// Updated string watermark.
    pub updated_string_watermark: u32,
    /// Maximum t value across all resolved commits.
    pub max_t: i64,
    /// Cumulative commit blob size for resolved commits.
    pub delta_commit_size: u64,
    /// Total assertions across resolved commits.
    pub delta_asserts: u64,
    /// Total retractions across resolved commits.
    pub delta_retracts: u64,
    /// Base vector arena counts per (g_id, p_id) for handle offsetting.
    /// Only populated when the base root has vector arenas.
    pub base_vector_counts: HashMap<(u16, u32), u32>,
    /// Base numbig arena counts per (g_id, p_id) for no-op detection.
    /// If arena.len() == base count, no new entries were added and we can
    /// reuse the existing CID instead of re-uploading.
    pub base_numbig_counts: HashMap<(u16, u32), usize>,
    /// String text bytes for fulltext assertion entries (global_string_id → bytes).
    /// Captured during reconciliation so the fulltext incremental builder can
    /// analyze text for BoW construction without loading the forward string dict.
    pub fulltext_string_bytes: HashMap<u32, Vec<u8>>,
}

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
// Public API
// ============================================================================

/// Resolve only incremental commits against an existing index root.
///
/// This is the Phase 1 entry point for the incremental indexing pipeline.
/// It loads the base root, seeds resolver state from it, walks only new
/// commits (`t > from_t`), resolves ops, and reconciles chunk-local IDs
/// to global IDs using the existing reverse dict trees.
///
/// The `cs` parameter must be an `Arc` because the reverse dict tree reader
/// may hold a reference for demand-loading leaf blobs.
///
/// # Errors
///
/// Returns `IncrementalResolveError` on any failure. The caller should
/// fall back to a full rebuild on error.
pub async fn resolve_incremental_commits(
    cs: Arc<dyn ContentStore>,
    config: IncrementalResolveConfig,
) -> Result<IncrementalNovelty, IncrementalResolveError> {
    // 1. Load and decode base IndexRootV5
    let root_bytes = cs.get(&config.base_root_id).await.map_err(|e| {
        IncrementalResolveError::RootLoad(format!(
            "failed to load root {}: {}",
            config.base_root_id, e
        ))
    })?;
    let root = IndexRootV5::decode(&root_bytes)
        .map_err(|e| IncrementalResolveError::RootLoad(format!("failed to decode root: {}", e)))?;

    tracing::info!(
        index_t = root.index_t,
        from_t = config.from_t,
        head = %config.head_commit_id,
        "incremental resolve: loaded base root"
    );

    // 2. Load subject + string reverse dict trees from CAS
    let subject_tree = load_reverse_tree(&cs, &root.dict_refs.subject_reverse).await?;
    let string_tree = load_reverse_tree(&cs, &root.dict_refs.string_reverse).await?;

    // 3. Seed SharedResolverState from root
    let mut shared = SharedResolverState::from_index_root(&root)?;

    // Enable spatial hook so we can detect non-POINT geometries in novelty.
    // If any are collected, the incremental path bails (spatial index updates
    // are not yet supported incrementally — results would be silently stale).
    shared.spatial_hook = Some(crate::spatial_hook::SpatialHook::new());

    // Enable fulltext hook to collect @fulltext-typed string entries.
    shared.fulltext_hook = Some(crate::fulltext_hook::FulltextHook::new());

    // 3a. Pre-seed numbig arenas so handles continue from existing values.
    // NumBig arenas are small (kilobytes), so loading them fully is cheap.
    // Record base counts so the upload phase can detect no-op (no new entries).
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
            let arena = fluree_db_binary_index::arena::numbig::read_numbig_arena_from_bytes(&bytes)
                .map_err(|e| {
                    IncrementalResolveError::RootLoad(format!("numbig arena decode: {}", e))
                })?;
            base_numbig_counts.insert((ga.g_id, *p_id), arena.len());
            nb_map.insert(*p_id, arena);
        }
    }

    // 3b. Collect base vector counts per (g_id, p_id) for handle offsetting.
    // We only need the total_count from each manifest, not the shard data.
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
                        IncrementalResolveError::RootLoad(format!("vector manifest decode: {}", e))
                    })?;
            base_vector_counts.insert((ga.g_id, vref.p_id), manifest.total_count);
        }
    }

    // 4. Walk commit chain backward from head, collecting only commits with `t > from_t`.
    //
    // This is critical for performance: we do NOT need to scan to genesis when
    // building incrementally from an existing root.
    let commit_cids =
        walk_commit_chain_since(cs.as_ref(), &config.head_commit_id, config.from_t).await?;

    // 5. Resolve commits into chunk (already filtered to `t > from_t`)
    let mut chunk = RebuildChunk::new();
    let mut max_t: i64 = root.index_t;
    let mut delta_commit_size = 0u64;
    let mut delta_asserts = 0u64;
    let mut delta_retracts = 0u64;
    let mut commit_count = 0usize;

    for cid in &commit_cids {
        let bytes = cs.get(cid).await.map_err(|e| {
            IncrementalResolveError::CommitChain(format!("failed to load commit {}: {}", cid, e))
        })?;

        // Peek at the commit's t via lightweight envelope decode
        let envelope = fluree_db_novelty::commit_v2::read_commit_envelope(&bytes).map_err(|e| {
            IncrementalResolveError::CommitChain(format!(
                "failed to decode envelope for {}: {}",
                cid, e
            ))
        })?;

        // Resolve this commit's ops into chunk
        let resolved = shared
            .resolve_commit_into_chunk(&bytes, &cid.digest_hex(), &mut chunk)
            .map_err(IncrementalResolveError::Resolve)?;

        max_t = max_t.max(envelope.t);
        delta_commit_size += resolved.size;
        delta_asserts += resolved.asserts as u64;
        delta_retracts += resolved.retracts as u64;
        commit_count += 1;
    }

    tracing::info!(
        commit_count,
        records = chunk.records.len(),
        max_t,
        "incremental resolve: commits resolved into chunk"
    );

    // Cache watermarks before potential root move
    let base_subject_watermarks = root.subject_watermarks.clone();
    let base_string_watermark = root.string_watermark;

    if chunk.records.is_empty() {
        return Ok(IncrementalNovelty {
            records: Vec::new(),
            shared,
            updated_watermarks: base_subject_watermarks,
            updated_string_watermark: base_string_watermark,
            base_root: root,
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

    // 6. Reconcile chunk-local IDs to global IDs
    let reconcile = reconcile_chunk_to_global(
        &chunk,
        &subject_tree,
        &string_tree,
        &base_subject_watermarks,
        base_string_watermark,
    )?;

    // 7. Remap fulltext hook entries from chunk-local to global string IDs
    //    and capture string text bytes for assertion entries (needed by the
    //    incremental fulltext arena builder for text analysis).
    let fulltext_string_bytes: HashMap<u32, Vec<u8>> = {
        let chunk_forward = chunk.strings.forward_entries();
        if let Some(ref mut ft) = shared.fulltext_hook {
            let mut map = HashMap::new();
            for entry in ft.entries_mut() {
                let local_id = entry.string_id as usize;
                let global_id = match reconcile.string_remap.get(local_id) {
                    Some(&id) => id,
                    None => {
                        tracing::warn!(
                            local_id,
                            "fulltext entry string_id remap miss; skipping"
                        );
                        // Poison the entry so it's effectively skipped.
                        entry.is_assert = false;
                        entry.string_id = u32::MAX;
                        continue;
                    }
                };
                // Capture text bytes for assertions (needed for BoW analysis).
                if entry.is_assert {
                    if let Some(bytes) = chunk_forward.get(local_id) {
                        map.entry(global_id).or_insert_with(|| bytes.clone());
                    }
                }
                // Remap to global ID.
                entry.string_id = global_id;
            }
            map
        } else {
            HashMap::new()
        }
    };

    // 8. Remap all records in-place
    let mut records = chunk.records;
    for record in &mut records {
        remap_record(record, &reconcile.subject_remap, &reconcile.string_remap)?;
    }

    // 8a. Offset vector handles: new vectors got handles 0..N during
    // resolution, but the base root already has vectors 0..base_count.
    // Shift new handles by base_count so they don't collide.
    if !base_vector_counts.is_empty() {
        for record in &mut records {
            if ObjKind::from_u8(record.o_kind) == ObjKind::VECTOR_ID {
                let key = (record.g_id, record.p_id);
                if let Some(&base_count) = base_vector_counts.get(&key) {
                    record.o_key += base_count as u64;
                }
            }
        }
    }

    // 9. Sort by (g_id, SPOT)
    let spot_cmp = cmp_for_order(RunSortOrder::Spot);
    records.sort_unstable_by(|a, b| a.g_id.cmp(&b.g_id).then_with(|| spot_cmp(a, b)));

    Ok(IncrementalNovelty {
        records,
        shared,
        base_root: root,
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

/// Walk the commit chain backward from `head_id` to genesis, returning CIDs
/// in chronological order (genesis first).
async fn walk_commit_chain_since(
    cs: &dyn ContentStore,
    head_id: &ContentId,
    from_t: i64,
) -> Result<Vec<ContentId>, IncrementalResolveError> {
    let mut cids = Vec::new();
    let mut current = Some(head_id.clone());

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

        // Commit `t` values are monotonically decreasing as we follow previous refs.
        // Once we reach `t <= from_t`, the remaining commits are already covered by
        // the base root and are not needed for incremental resolution.
        if envelope.t <= from_t {
            break;
        }

        current = envelope.previous_ref.map(|r| r.id);
        cids.push(cid);
    }

    cids.reverse(); // chronological order
    Ok(cids)
}

// ============================================================================
// Internal: Reverse Tree Loading
// ============================================================================

/// Load a reverse dict tree (DLR1 leaves + DTB1 branch) from CAS.
///
/// Builds a `DictTreeReader` with demand-loading leaf source.
pub(crate) async fn load_reverse_tree(
    cs: &Arc<dyn ContentStore>,
    refs: &DictTreeRefs,
) -> Result<DictTreeReader, IncrementalResolveError> {
    // Fetch and decode branch
    let branch_bytes = cs.get(&refs.branch).await.map_err(|e| {
        IncrementalResolveError::DictTreeLoad(format!("failed to load branch: {}", e))
    })?;
    let branch = DictBranch::decode(&branch_bytes).map_err(|e| {
        IncrementalResolveError::DictTreeLoad(format!("failed to decode branch: {}", e))
    })?;

    // Build leaf source: map branch leaf addresses to root leaf CIDs for demand-loading
    let mut local_files = HashMap::with_capacity(branch.leaves.len());
    let mut remote_cids = HashMap::new();

    for (cid, bl) in refs.leaves.iter().zip(branch.leaves.iter()) {
        if let Some(local_path) = cs.resolve_local_path(cid) {
            local_files.insert(bl.address.clone(), local_path);
        } else {
            remote_cids.insert(bl.address.clone(), cid.clone());
        }
    }

    let leaf_source = if remote_cids.is_empty() {
        fluree_db_binary_index::dict::reader::LeafSource::LocalFiles(local_files)
    } else {
        fluree_db_binary_index::dict::reader::LeafSource::CasOnDemand {
            cs: Arc::clone(cs),
            local_files,
            remote_cids,
        }
    };

    Ok(DictTreeReader::new(branch, leaf_source))
}

// ============================================================================
// Internal: Reconciliation (chunk-local → global IDs)
// ============================================================================

struct ReconcileResult {
    /// chunk-local subject index → global sid64
    subject_remap: Vec<u64>,
    /// chunk-local string index → global string ID
    string_remap: Vec<u32>,
    /// New subjects not found in reverse tree: (ns_code, local_id_in_ns, suffix_bytes)
    new_subjects: Vec<(u16, u64, Vec<u8>)>,
    /// New strings not found in reverse tree: (global_string_id, value_bytes)
    new_strings: Vec<(u32, Vec<u8>)>,
    /// Updated subject watermarks after assigning new IDs
    updated_watermarks: Vec<u64>,
    /// Updated string watermark after assigning new IDs
    updated_string_watermark: u32,
}

/// Reconcile chunk-local subject/string IDs to global IDs.
///
/// For each chunk-local subject/string:
/// 1. Look up in the reverse dict tree (existing index)
/// 2. If found → reuse existing global ID
/// 3. If not found → assign new global ID starting at watermark + 1
fn reconcile_chunk_to_global(
    chunk: &RebuildChunk,
    subject_tree: &DictTreeReader,
    string_tree: &DictTreeReader,
    subject_watermarks: &[u64],
    string_watermark: u32,
) -> Result<ReconcileResult, IncrementalResolveError> {
    // --- Subject reconciliation ---
    let subject_entries = chunk.subjects.forward_entries();
    let mut subject_remap = vec![0u64; subject_entries.len()];
    let mut new_subjects = Vec::new();

    // Per-namespace counter for new IDs: ns_code → next local_id
    let mut ns_next_local: HashMap<u16, u64> = HashMap::new();
    let mut updated_watermarks = subject_watermarks.to_vec();

    for (chunk_local_id, (ns_code, name_bytes)) in subject_entries.iter().enumerate() {
        let reverse_key = subject_reverse_key(*ns_code, name_bytes);
        let existing_id = subject_tree
            .reverse_lookup(&reverse_key)
            .map_err(IncrementalResolveError::Io)?;

        let global_sid64 = match existing_id {
            Some(sid64) => sid64,
            None => {
                // Assign new ID starting at watermark + 1
                let wm = if (*ns_code as usize) < updated_watermarks.len() {
                    updated_watermarks[*ns_code as usize]
                } else {
                    // New namespace — extend watermarks vector
                    updated_watermarks.resize(*ns_code as usize + 1, 0);
                    0
                };

                let next = ns_next_local.entry(*ns_code).or_insert(wm + 1);
                let local_id = *next;
                *next += 1;

                // Update watermark to track highest assigned local_id
                updated_watermarks[*ns_code as usize] = local_id;

                let sid64 = SubjectId::new(*ns_code, local_id).as_u64();
                new_subjects.push((*ns_code, local_id, name_bytes.clone()));
                sid64
            }
        };

        subject_remap[chunk_local_id] = global_sid64;
    }

    // --- String reconciliation ---
    let string_entries = chunk.strings.forward_entries();
    let mut string_remap = vec![0u32; string_entries.len()];
    let mut new_strings = Vec::new();
    let mut next_string_id = string_watermark + 1;

    for (chunk_local_id, value_bytes) in string_entries.iter().enumerate() {
        let existing_id = string_tree
            .reverse_lookup(value_bytes)
            .map_err(IncrementalResolveError::Io)?;

        let global_str_id = match existing_id {
            Some(id) => id as u32,
            None => {
                let id = next_string_id;
                next_string_id += 1;
                new_strings.push((id, value_bytes.clone()));
                id
            }
        };

        string_remap[chunk_local_id] = global_str_id;
    }

    let updated_string_watermark = if next_string_id > string_watermark + 1 {
        // New strings were assigned
        next_string_id - 1
    } else {
        string_watermark
    };

    Ok(ReconcileResult {
        subject_remap,
        string_remap,
        new_subjects,
        new_strings,
        updated_watermarks,
        updated_string_watermark,
    })
}

/// Remap a single RunRecord from chunk-local to global IDs.
fn remap_record(
    record: &mut RunRecord,
    subject_remap: &[u64],
    string_remap: &[u32],
) -> Result<(), IncrementalResolveError> {
    // Remap subject
    let local_s = record.s_id.as_u64() as usize;
    let global_s = *subject_remap.get(local_s).ok_or_else(|| {
        IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
            "subject remap out of range: local_id={}, remap_len={}",
            local_s,
            subject_remap.len()
        )))
    })?;
    record.s_id = SubjectId::from_u64(global_s);

    // Remap object if it holds a chunk-local ID
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::resolve::resolver::RebuildChunk;
    use fluree_db_binary_index::dict::builder;
    use fluree_db_binary_index::dict::reverse_leaf::{subject_reverse_key, ReverseEntry};
    use fluree_db_binary_index::dict::DictTreeReader;
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Build an in-memory DictTreeReader from a list of (key, id) pairs.
    fn build_reverse_reader(entries: Vec<ReverseEntry>) -> DictTreeReader {
        let result =
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();
        let mut leaf_map = std::collections::HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }
        DictTreeReader::from_memory(result.branch, leaf_map)
    }

    /// Build a subject reverse tree from (ns_code, suffix, sid64) triples.
    fn build_subject_reverse(entries: &[(u16, &[u8], u64)]) -> DictTreeReader {
        let mut rev: Vec<ReverseEntry> = entries
            .iter()
            .map(|(ns, suffix, id)| ReverseEntry {
                key: subject_reverse_key(*ns, suffix),
                id: *id,
            })
            .collect();
        rev.sort_by(|a, b| a.key.cmp(&b.key));
        build_reverse_reader(rev)
    }

    /// Build a string reverse tree from (value, string_id) pairs.
    fn build_string_reverse(entries: &[(&[u8], u64)]) -> DictTreeReader {
        let mut rev: Vec<ReverseEntry> = entries
            .iter()
            .map(|(val, id)| ReverseEntry {
                key: val.to_vec(),
                id: *id,
            })
            .collect();
        rev.sort_by(|a, b| a.key.cmp(&b.key));
        build_reverse_reader(rev)
    }

    // ========================================================================
    // Remap tests (existing)
    // ========================================================================

    #[test]
    fn test_remap_record_subject() {
        let subject_remap = vec![100, 200, 300];
        let string_remap = vec![10, 20];

        let mut record = RunRecord {
            g_id: 0,
            s_id: SubjectId::from_u64(1), // chunk-local 1 → global 200
            p_id: 5,
            dt: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            op: 1,
            o_key: ObjKey::encode_i64(42).as_u64(),
            t: 1,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        };

        remap_record(&mut record, &subject_remap, &string_remap).unwrap();
        assert_eq!(record.s_id.as_u64(), 200);
        // o_key unchanged (NUM_INT, not a ref)
        assert_eq!(record.o_key, ObjKey::encode_i64(42).as_u64());
    }

    #[test]
    fn test_remap_record_ref_object() {
        let subject_remap = vec![100, 200, 300];
        let string_remap = vec![10, 20];

        let mut record = RunRecord {
            g_id: 0,
            s_id: SubjectId::from_u64(0), // → 100
            p_id: 5,
            dt: 0,
            o_kind: ObjKind::REF_ID.as_u8(),
            op: 1,
            o_key: 2, // chunk-local ref → global 300
            t: 1,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        };

        remap_record(&mut record, &subject_remap, &string_remap).unwrap();
        assert_eq!(record.s_id.as_u64(), 100);
        assert_eq!(record.o_key, 300);
    }

    #[test]
    fn test_remap_record_string_object() {
        let subject_remap = vec![100, 200];
        let string_remap = vec![50, 60, 70];

        let mut record = RunRecord {
            g_id: 0,
            s_id: SubjectId::from_u64(0),
            p_id: 5,
            dt: 1,
            o_kind: ObjKind::LEX_ID.as_u8(),
            op: 1,
            o_key: ObjKey::encode_u32_id(2).as_u64(), // chunk-local string 2 → global 70
            t: 1,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        };

        remap_record(&mut record, &subject_remap, &string_remap).unwrap();
        assert_eq!(record.s_id.as_u64(), 100);
        assert_eq!(ObjKey::from_u64(record.o_key).decode_u32_id(), 70);
    }

    #[test]
    fn test_remap_record_out_of_range() {
        let subject_remap = vec![100]; // only 1 entry
        let string_remap = vec![50];

        let mut record = RunRecord {
            g_id: 0,
            s_id: SubjectId::from_u64(5), // out of range
            p_id: 5,
            dt: 1,
            o_kind: ObjKind::NUM_INT.as_u8(),
            op: 1,
            o_key: 0,
            t: 1,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        };

        assert!(remap_record(&mut record, &subject_remap, &string_remap).is_err());
    }

    // ========================================================================
    // Reconciliation against real reverse trees
    // ========================================================================

    /// Existing subjects are resolved to their known sid64 from the reverse tree;
    /// new subjects get IDs starting at watermark + 1.
    #[test]
    fn test_reconcile_existing_subjects_reuse_ids() {
        // Existing index has 3 subjects in ns=5:
        //   local_id 1 = "Alice", local_id 2 = "Bob", local_id 3 = "Carol"
        let ns: u16 = 5;
        let alice_sid = SubjectId::new(ns, 1).as_u64();
        let bob_sid = SubjectId::new(ns, 2).as_u64();
        let carol_sid = SubjectId::new(ns, 3).as_u64();

        let subject_tree = build_subject_reverse(&[
            (ns, b"Alice", alice_sid),
            (ns, b"Bob", bob_sid),
            (ns, b"Carol", carol_sid),
        ]);
        let string_tree = build_string_reverse(&[]);

        // Chunk references Alice (existing) + Dave (new)
        let mut chunk = RebuildChunk::new();
        chunk.subjects.get_or_insert(ns, b"Alice"); // chunk-local 0
        chunk.subjects.get_or_insert(ns, b"Dave"); // chunk-local 1

        let watermarks = {
            let mut wm = vec![0u64; ns as usize + 1];
            wm[ns as usize] = 3; // watermark = max local_id
            wm
        };

        let result =
            reconcile_chunk_to_global(&chunk, &subject_tree, &string_tree, &watermarks, 0).unwrap();

        // Alice should reuse existing sid64
        assert_eq!(result.subject_remap[0], alice_sid);
        // Dave is new — should get local_id = watermark + 1 = 4
        let dave_sid = SubjectId::new(ns, 4).as_u64();
        assert_eq!(result.subject_remap[1], dave_sid);

        // Only Dave should appear in new_subjects
        assert_eq!(result.new_subjects.len(), 1);
        assert_eq!(result.new_subjects[0].0, ns);
        assert_eq!(result.new_subjects[0].1, 4);
        assert_eq!(result.new_subjects[0].2, b"Dave");

        // Watermark for ns=5 should be updated to 4
        assert_eq!(result.updated_watermarks[ns as usize], 4);
    }

    /// Existing strings reuse their known global IDs; new strings start at
    /// watermark + 1.
    #[test]
    fn test_reconcile_existing_strings_reuse_ids() {
        let subject_tree = build_subject_reverse(&[]);
        let string_tree = build_string_reverse(&[(b"hello", 0), (b"world", 1), (b"foo", 2)]);

        let mut chunk = RebuildChunk::new();
        chunk.strings.get_or_insert(b"world"); // chunk-local 0 → existing 1
        chunk.strings.get_or_insert(b"bar"); // chunk-local 1 → new
        chunk.strings.get_or_insert(b"hello"); // chunk-local 2 → existing 0

        let result = reconcile_chunk_to_global(
            &chunk,
            &subject_tree,
            &string_tree,
            &[],
            2, // string_watermark = 2 (IDs 0,1,2 used)
        )
        .unwrap();

        assert_eq!(result.string_remap[0], 1); // "world" → existing 1
        assert_eq!(result.string_remap[1], 3); // "bar" → new, watermark+1 = 3
        assert_eq!(result.string_remap[2], 0); // "hello" → existing 0

        assert_eq!(result.new_strings.len(), 1);
        assert_eq!(result.new_strings[0].0, 3);
        assert_eq!(result.new_strings[0].1, b"bar");
        assert_eq!(result.updated_string_watermark, 3);
    }

    /// When all chunk entries already exist, watermarks don't advance and
    /// new_subjects/new_strings are empty.
    #[test]
    fn test_reconcile_all_existing_no_watermark_change() {
        let ns: u16 = 1;
        let alice_sid = SubjectId::new(ns, 1).as_u64();
        let bob_sid = SubjectId::new(ns, 2).as_u64();

        let subject_tree =
            build_subject_reverse(&[(ns, b"Alice", alice_sid), (ns, b"Bob", bob_sid)]);
        let string_tree = build_string_reverse(&[(b"hello", 0)]);

        let mut chunk = RebuildChunk::new();
        chunk.subjects.get_or_insert(ns, b"Bob");
        chunk.subjects.get_or_insert(ns, b"Alice");
        chunk.strings.get_or_insert(b"hello");

        let watermarks = vec![0, 2]; // ns=1 watermark=2

        let result =
            reconcile_chunk_to_global(&chunk, &subject_tree, &string_tree, &watermarks, 0).unwrap();

        assert_eq!(result.subject_remap[0], bob_sid);
        assert_eq!(result.subject_remap[1], alice_sid);
        assert!(result.new_subjects.is_empty());
        assert!(result.new_strings.is_empty());
        assert_eq!(result.updated_watermarks[1], 2); // unchanged
        assert_eq!(result.updated_string_watermark, 0); // unchanged
    }

    /// New namespace code (not in watermarks vec) extends the watermarks and
    /// assigns IDs starting at local_id=1.
    #[test]
    fn test_reconcile_new_namespace_extends_watermarks() {
        let existing_ns: u16 = 2;
        let new_ns: u16 = 7;

        let subject_tree =
            build_subject_reverse(&[(existing_ns, b"X", SubjectId::new(existing_ns, 1).as_u64())]);
        let string_tree = build_string_reverse(&[]);

        let mut chunk = RebuildChunk::new();
        // Reference an existing subject
        chunk.subjects.get_or_insert(existing_ns, b"X");
        // Reference a subject in a brand new namespace
        chunk.subjects.get_or_insert(new_ns, b"NewThing");

        let watermarks = vec![0, 0, 1]; // only ns 0,1,2 known; ns=2 watermark=1

        let result =
            reconcile_chunk_to_global(&chunk, &subject_tree, &string_tree, &watermarks, 0).unwrap();

        // Existing subject reused
        assert_eq!(
            result.subject_remap[0],
            SubjectId::new(existing_ns, 1).as_u64()
        );
        // New namespace — watermarks extended to ns=7, first ID = 1
        assert_eq!(result.subject_remap[1], SubjectId::new(new_ns, 1).as_u64());

        // Watermarks vec must now reach ns=7
        assert!(result.updated_watermarks.len() > new_ns as usize);
        assert_eq!(result.updated_watermarks[new_ns as usize], 1);
        // Existing namespace watermark unchanged
        assert_eq!(result.updated_watermarks[existing_ns as usize], 1);
    }

    /// Multiple new subjects in the same namespace get sequential local_ids
    /// starting at watermark + 1.
    #[test]
    fn test_reconcile_multiple_new_subjects_sequential_ids() {
        let ns: u16 = 3;

        let subject_tree =
            build_subject_reverse(&[(ns, b"Existing", SubjectId::new(ns, 10).as_u64())]);
        let string_tree = build_string_reverse(&[]);

        let mut chunk = RebuildChunk::new();
        chunk.subjects.get_or_insert(ns, b"New1"); // chunk-local 0
        chunk.subjects.get_or_insert(ns, b"New2"); // chunk-local 1
        chunk.subjects.get_or_insert(ns, b"Existing"); // chunk-local 2
        chunk.subjects.get_or_insert(ns, b"New3"); // chunk-local 3

        let mut watermarks = vec![0u64; ns as usize + 1];
        watermarks[ns as usize] = 10;

        let result =
            reconcile_chunk_to_global(&chunk, &subject_tree, &string_tree, &watermarks, 0).unwrap();

        // Existing → reuse
        assert_eq!(result.subject_remap[2], SubjectId::new(ns, 10).as_u64());
        // New subjects get sequential IDs: 11, 12, 13
        assert_eq!(result.subject_remap[0], SubjectId::new(ns, 11).as_u64());
        assert_eq!(result.subject_remap[1], SubjectId::new(ns, 12).as_u64());
        assert_eq!(result.subject_remap[3], SubjectId::new(ns, 13).as_u64());

        assert_eq!(result.new_subjects.len(), 3);
        assert_eq!(result.updated_watermarks[ns as usize], 13);
    }
}
