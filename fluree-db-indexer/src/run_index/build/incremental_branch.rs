//! Incremental branch update: merge novelty into affected leaves of an FBR2 branch.
//!
//! Given an existing [`BranchManifest`] + sorted novelty for a single (graph, order),
//! fetches only the affected leaf blobs from CAS, calls [`update_leaf`] on each,
//! and produces a new branch manifest mixing unchanged + updated leaf entries.
//!
//! Unchanged leaves are kept by CID reference (no CAS fetch). This is the core
//! savings of incremental indexing: only touched leaves are re-encoded.

use super::incremental_leaf::{update_leaf, IncrementalLeafError, LeafUpdateInput, NewLeafBlob};
use fluree_db_binary_index::format::branch::{build_branch_v2_bytes, BranchManifest, LeafEntry};
use fluree_db_binary_index::format::run_record::{cmp_for_order, RunRecord, RunSortOrder};
use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH;
use fluree_db_core::ContentId;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::io;

// ============================================================================
// Public types
// ============================================================================

/// Configuration for an incremental branch update.
pub struct IncrementalBranchConfig {
    /// zstd compression level for re-encoded leaflets.
    pub zstd_level: i32,
    /// Max number of touched leaves to rewrite in parallel within a single
    /// `(graph, order)` branch update.
    ///
    /// Note: overall incremental job concurrency is also bounded by the
    /// orchestrator semaphore; this cap prevents runaway parallelism when a
    /// single branch touches many leaves.
    pub max_parallel_leaves: usize,
    /// Threshold above which a merged leaflet should be split (default: 37_500).
    pub leaflet_split_rows: usize,
    /// Target rows per leaflet after split (default: 25_000).
    pub leaflet_target_rows: usize,
    /// Max leaflets per leaf file (default: 10).
    pub leaflets_per_leaf: usize,
    /// Max total leaflets before splitting the leaf into multiple blobs (default: 20).
    pub leaf_split_leaflets: usize,
}

/// Result of an incremental branch update.
pub struct BranchUpdateResult {
    /// Updated leaf entries for the new branch manifest.
    pub leaf_entries: Vec<LeafEntry>,
    /// New leaf blobs that need to be uploaded to CAS.
    pub new_leaf_blobs: Vec<NewLeafBlob>,
    /// Old leaf CIDs that were replaced (for GC).
    pub replaced_leaf_cids: Vec<ContentId>,
    /// Encoded FBR2 branch manifest bytes.
    pub branch_bytes: Vec<u8>,
    /// Content ID of the new branch manifest.
    pub branch_cid: ContentId,
}

/// Errors specific to incremental branch update.
#[derive(Debug)]
pub enum IncrementalBranchError {
    /// A leaf merge produced empty-after-retract; fall back to full rebuild.
    EmptyLeafletWithHistory,
    /// I/O or format error.
    Io(io::Error),
}

impl From<IncrementalLeafError> for IncrementalBranchError {
    fn from(e: IncrementalLeafError) -> Self {
        match e {
            IncrementalLeafError::EmptyLeafletWithHistory => Self::EmptyLeafletWithHistory,
            IncrementalLeafError::Io(e) => Self::Io(e),
        }
    }
}

impl From<io::Error> for IncrementalBranchError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for IncrementalBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyLeafletWithHistory => {
                write!(f, "leaf merge produced empty-after-retract")
            }
            Self::Io(e) => write!(f, "branch I/O error: {e}"),
        }
    }
}

impl std::error::Error for IncrementalBranchError {}

// ============================================================================
// Main entry point
// ============================================================================

/// Update a branch manifest by merging novelty into affected leaves.
///
/// `novelty` must be sorted by the branch's sort order. `fetch_leaf` is called
/// only for leaves that have novelty — unchanged leaves are carried through
/// by CID reference.
///
/// Returns the new branch manifest (bytes + CID), the new leaf blobs to upload,
/// and the old leaf CIDs that were replaced.
pub fn update_branch(
    existing: &BranchManifest,
    novelty: &[RunRecord],
    order: RunSortOrder,
    g_id: u16,
    config: &IncrementalBranchConfig,
    fetch_leaf: &mut dyn FnMut(&ContentId) -> Result<Vec<u8>, IncrementalBranchError>,
) -> Result<BranchUpdateResult, IncrementalBranchError> {
    let cmp = cmp_for_order(order);
    let total_leaves = existing.leaves.len();

    // 1. Slice novelty to leaves using half-open intervals on first_key
    let slices = slice_novelty_to_leaves(novelty, &existing.leaves, cmp);

    // 2. Prefetch touched leaves (fetch_leaf may perform I/O; keep it sequential).
    struct TouchedLeaf<'a> {
        idx: usize,
        old_cid: ContentId,
        leaf_bytes: Vec<u8>,
        novelty: &'a [RunRecord],
    }

    let mut untouched_count = 0usize;
    let mut touched: Vec<TouchedLeaf<'_>> = Vec::new();
    for (i, leaf) in existing.leaves.iter().enumerate() {
        let leaf_novelty = slices[i];
        if leaf_novelty.is_empty() {
            untouched_count += 1;
            continue;
        }

        let leaf_bytes = fetch_leaf(&leaf.leaf_cid)?;
        touched.push(TouchedLeaf {
            idx: i,
            old_cid: leaf.leaf_cid.clone(),
            leaf_bytes,
            novelty: leaf_novelty,
        });
    }

    // 3. Rewrite touched leaves in parallel (CPU-bound), but keep output ordering
    // deterministic by assembling results in original leaf order.
    struct LeafRewriteResult {
        idx: usize,
        old_cid: ContentId,
        new_leaves: Vec<NewLeafBlob>,
    }

    let max_parallel = config.max_parallel_leaves.max(1);
    let mut rewritten: Vec<LeafRewriteResult> = Vec::with_capacity(touched.len());
    for batch in touched.chunks(max_parallel) {
        let batch_results: Vec<Result<LeafRewriteResult, IncrementalBranchError>> = batch
            .par_iter()
            .map(|leaf| {
                let leaf_input = LeafUpdateInput {
                    leaf_bytes: &leaf.leaf_bytes,
                    novelty: leaf.novelty,
                    order,
                    g_id,
                    zstd_level: config.zstd_level,
                    leaflet_split_rows: config.leaflet_split_rows,
                    leaflet_target_rows: config.leaflet_target_rows,
                    leaflets_per_leaf: config.leaflets_per_leaf,
                    leaf_split_leaflets: config.leaf_split_leaflets,
                };
                let output = update_leaf(&leaf_input)?;
                Ok(LeafRewriteResult {
                    idx: leaf.idx,
                    old_cid: leaf.old_cid.clone(),
                    new_leaves: output.leaves,
                })
            })
            .collect();

        // Preserve early-exit behavior on error.
        for r in batch_results {
            rewritten.push(r?);
        }
    }

    // Index results by leaf index for deterministic assembly.
    let mut rewrites_by_idx: Vec<Option<LeafRewriteResult>> =
        (0..total_leaves).map(|_| None).collect();
    for r in rewritten {
        let idx = r.idx;
        rewrites_by_idx[idx] = Some(r);
    }

    // 4. Assemble new leaf routing + blobs in original leaf order.
    let mut leaf_entries: Vec<LeafEntry> = Vec::new();
    let mut new_blobs: Vec<NewLeafBlob> = Vec::new();
    let mut replaced_cids: Vec<ContentId> = Vec::new();

    for (i, leaf) in existing.leaves.iter().enumerate() {
        if slices[i].is_empty() {
            leaf_entries.push(leaf.clone());
            continue;
        }

        let rewrite = rewrites_by_idx[i]
            .take()
            .ok_or_else(|| io::Error::other("missing leaf rewrite result"))?;

        replaced_cids.push(rewrite.old_cid);
        for blob in rewrite.new_leaves {
            leaf_entries.push(LeafEntry {
                first_key: blob.first_key,
                last_key: blob.last_key,
                row_count: blob.row_count,
                leaf_cid: blob.cid.clone(),
                resolved_path: None,
            });
            new_blobs.push(blob);
        }
    }

    // 5. Build new FBR2 branch manifest
    let branch_bytes = build_branch_v2_bytes(order, g_id, &leaf_entries);
    let branch_cid = ContentId::from_hex_digest(
        CODEC_FLUREE_INDEX_BRANCH,
        &hex::encode(Sha256::digest(&branch_bytes)),
    )
    .expect("valid SHA-256 hex digest");

    tracing::debug!(
        g_id = g_id,
        order = ?order,
        total_leaves = total_leaves,
        touched = replaced_cids.len(),
        untouched = untouched_count,
        new_blobs = new_blobs.len(),
        output_leaves = leaf_entries.len(),
        novelty = novelty.len(),
        "branch update complete"
    );

    Ok(BranchUpdateResult {
        leaf_entries,
        new_leaf_blobs: new_blobs,
        replaced_leaf_cids: replaced_cids,
        branch_bytes,
        branch_cid,
    })
}

// ============================================================================
// Helpers
// ============================================================================

/// Slice novelty to leaves using half-open intervals on `first_key`.
///
/// - Leaf 0 owns `[-∞, leaf[1].first_key)` (includes all novelty before leaf 1)
/// - Leaf i owns `[leaf[i].first_key, leaf[i+1].first_key)`
/// - Last leaf owns `[leaf[last].first_key, +∞)`
fn slice_novelty_to_leaves<'a>(
    novelty: &'a [RunRecord],
    leaves: &[LeafEntry],
    cmp: fn(&RunRecord, &RunRecord) -> Ordering,
) -> Vec<&'a [RunRecord]> {
    let n = leaves.len();
    if n == 0 {
        return vec![];
    }
    if novelty.is_empty() {
        return vec![&[] as &[RunRecord]; n];
    }

    let mut slices = Vec::with_capacity(n);
    let mut start = 0;

    for i in 0..n {
        if i == n - 1 {
            // Last leaf gets everything remaining
            slices.push(&novelty[start..]);
        } else {
            // Find split point: first novelty record >= next leaf's first_key
            let end = start
                + novelty[start..]
                    .partition_point(|r| cmp(r, &leaves[i + 1].first_key) == Ordering::Less);
            slices.push(&novelty[start..end]);
            start = end;
        }
    }

    slices
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::branch::read_branch_v2_from_bytes;
    use fluree_db_binary_index::format::leaf::read_leaf_header;
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use fluree_db_binary_index::format::leaflet::decode_leaflet;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;
    use std::collections::HashMap;

    fn make_record(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    fn make_retract(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            false,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    fn default_config() -> IncrementalBranchConfig {
        IncrementalBranchConfig {
            zstd_level: 1,
            max_parallel_leaves: 4,
            leaflet_split_rows: 37_500,
            leaflet_target_rows: 25_000,
            leaflets_per_leaf: 10,
            leaf_split_leaflets: 20,
        }
    }

    /// Build leaf files and a branch manifest from records, returning the
    /// manifest + a map of CID → leaf bytes for the fake CAS.
    fn build_branch_and_leaves(
        records: &[RunRecord],
        leaflet_rows: usize,
        leaflets_per_leaf: usize,
    ) -> (BranchManifest, HashMap<ContentId, Vec<u8>>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "fluree_test_incr_branch_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = LeafWriter::new(dir.clone(), leaflet_rows, leaflets_per_leaf, 1);
        for r in records {
            writer.push_record(*r).unwrap();
        }
        let infos = writer.finish().unwrap();

        // Build CAS map + leaf entries
        let mut cas: HashMap<ContentId, Vec<u8>> = HashMap::new();
        let mut leaf_entries = Vec::new();
        for info in &infos {
            let path = dir.join(info.leaf_cid.to_string());
            let data = std::fs::read(&path).unwrap();
            cas.insert(info.leaf_cid.clone(), data);
            leaf_entries.push(LeafEntry {
                first_key: info.first_key,
                last_key: info.last_key,
                row_count: info.total_rows,
                leaf_cid: info.leaf_cid.clone(),
                resolved_path: None,
            });
        }

        let _ = std::fs::remove_dir_all(&dir);
        (
            BranchManifest {
                leaves: leaf_entries,
            },
            cas,
        )
    }

    #[test]
    fn test_branch_update_touches_one_leaf() {
        // Build a branch with 3 leaves (10 records each)
        let records: Vec<RunRecord> = (0..30)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);
        assert_eq!(manifest.leaves.len(), 3);

        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        // Novelty: insert one NEW fact in the second leaf's range (p_id=2 is new)
        let novelty = vec![make_record(15, 2, 150, 5)];
        let config = default_config();

        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("CID not found: {cid}"),
                ))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // Leaf 0 and 2 should be unchanged (same CIDs)
        assert_eq!(result.leaf_entries[0].leaf_cid, original_cids[0]);
        assert_eq!(
            result.leaf_entries.last().unwrap().leaf_cid,
            original_cids[2]
        );

        // Leaf 1 should be replaced
        assert_ne!(result.leaf_entries[1].leaf_cid, original_cids[1]);
        assert_eq!(result.replaced_leaf_cids.len(), 1);
        assert_eq!(result.replaced_leaf_cids[0], original_cids[1]);

        // New blobs should contain exactly the replaced leaf
        assert_eq!(result.new_leaf_blobs.len(), 1);

        // Total rows: 30 original + 1 new fact = 31
        let total: u64 = result.leaf_entries.iter().map(|e| e.row_count).sum();
        assert_eq!(total, 31);

        // Branch manifest should be valid FBR2
        let decoded = read_branch_v2_from_bytes(&result.branch_bytes).unwrap();
        assert_eq!(decoded.leaves.len(), result.leaf_entries.len());
    }

    #[test]
    fn test_branch_update_no_novelty_returns_same_cids() {
        let records: Vec<RunRecord> = (0..20)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, _cas) = build_branch_and_leaves(&records, 10, 1);
        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        let novelty: Vec<RunRecord> = vec![];
        let config = default_config();
        let mut fetch = |_: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            panic!("should not fetch when no novelty");
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        assert_eq!(result.leaf_entries.len(), original_cids.len());
        for (entry, orig) in result.leaf_entries.iter().zip(&original_cids) {
            assert_eq!(&entry.leaf_cid, orig);
        }
        assert!(result.new_leaf_blobs.is_empty());
        assert!(result.replaced_leaf_cids.is_empty());
    }

    #[test]
    fn test_branch_update_novelty_in_first_leaf() {
        // Novelty before all existing records routes to first leaf
        let records: Vec<RunRecord> = (10..30)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);
        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        // Insert before the first existing record (s_id=5 < s_id=10)
        let novelty = vec![make_record(5, 1, 50, 5)];
        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // First leaf should be replaced, second unchanged
        assert_ne!(result.leaf_entries[0].leaf_cid, original_cids[0]);
        assert_eq!(result.leaf_entries[1].leaf_cid, original_cids[1]);
        assert_eq!(result.replaced_leaf_cids, vec![original_cids[0].clone()]);

        // Verify first leaf has the new record
        let blob = &result.new_leaf_blobs[0];
        let hdr = read_leaf_header(&blob.bytes).unwrap();
        assert_eq!(hdr.total_rows, 11); // 10 existing + 1 new

        let d = &hdr.leaflet_dir[0];
        let ld = &blob.bytes[d.offset as usize..d.offset as usize + d.compressed_len as usize];
        let decoded = decode_leaflet(ld, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        assert_eq!(decoded.s_ids[0], 5); // New record should be first
    }

    #[test]
    fn test_branch_update_novelty_in_last_leaf() {
        // Novelty after all existing records routes to last leaf
        let records: Vec<RunRecord> = (0..20)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);
        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        // Insert after the last existing record (s_id=99 > s_id=19)
        let novelty = vec![make_record(99, 1, 990, 5)];
        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // First leaf unchanged, last leaf replaced
        assert_eq!(result.leaf_entries[0].leaf_cid, original_cids[0]);
        assert_ne!(result.leaf_entries[1].leaf_cid, original_cids[1]);
        assert_eq!(result.replaced_leaf_cids, vec![original_cids[1].clone()]);
    }

    #[test]
    fn test_branch_update_retract_propagates_error() {
        // Retract all facts in a single leaf → EmptyLeafletWithHistory
        let records = vec![make_record(1, 1, 10, 1), make_record(2, 1, 20, 1)];
        let (manifest, cas) = build_branch_and_leaves(&records, 100, 1);

        // Retract both records
        let novelty = vec![make_retract(1, 1, 10, 5), make_retract(2, 1, 20, 5)];
        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        );

        assert!(matches!(
            result,
            Err(IncrementalBranchError::EmptyLeafletWithHistory)
        ));
    }

    #[test]
    fn test_branch_update_multiple_leaves_touched() {
        // Build a branch with 3 leaves, novelty touches leaves 0 and 2
        let records: Vec<RunRecord> = (0..30)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);
        assert_eq!(manifest.leaves.len(), 3);
        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        // Novelty in first and last leaf ranges
        let mut novelty = vec![
            make_record(5, 1, 55, 5),   // leaf 0 range
            make_record(25, 1, 255, 5), // leaf 2 range
        ];
        novelty.sort_unstable_by(cmp_for_order(RunSortOrder::Spot));

        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // Leaf 1 should be unchanged
        assert_eq!(result.leaf_entries[1].leaf_cid, original_cids[1]);
        // Leaves 0 and 2 should be replaced
        assert_ne!(result.leaf_entries[0].leaf_cid, original_cids[0]);
        assert_ne!(result.leaf_entries[2].leaf_cid, original_cids[2]);
        assert_eq!(result.replaced_leaf_cids.len(), 2);
        assert_eq!(result.new_leaf_blobs.len(), 2);

        // Total: 30 + 2 = 32
        let total: u64 = result.leaf_entries.iter().map(|e| e.row_count).sum();
        assert_eq!(total, 32);
    }

    #[test]
    fn test_branch_manifest_is_valid_fbr2() {
        let records: Vec<RunRecord> = (0..20)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);

        let novelty = vec![make_record(5, 1, 55, 5)];
        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // Verify CID matches content
        assert!(result.branch_cid.verify(&result.branch_bytes));

        // Verify FBR2 decodes correctly
        let decoded = read_branch_v2_from_bytes(&result.branch_bytes).unwrap();
        assert_eq!(decoded.leaves.len(), result.leaf_entries.len());
        for (d, e) in decoded.leaves.iter().zip(&result.leaf_entries) {
            assert_eq!(d.leaf_cid, e.leaf_cid);
            assert_eq!(d.row_count, e.row_count);
        }
    }

    #[test]
    fn test_novelty_slicing_to_leaves() {
        let cmp = cmp_for_order(RunSortOrder::Spot);

        // 3 leaves with first_keys at s_id = 0, 10, 20
        let leaves = vec![
            LeafEntry {
                first_key: make_record(0, 0, 0, 0),
                last_key: make_record(9, 0, 0, 0),
                row_count: 10,
                leaf_cid: ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &"a".repeat(64))
                    .unwrap(),
                resolved_path: None,
            },
            LeafEntry {
                first_key: make_record(10, 0, 0, 0),
                last_key: make_record(19, 0, 0, 0),
                row_count: 10,
                leaf_cid: ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &"b".repeat(64))
                    .unwrap(),
                resolved_path: None,
            },
            LeafEntry {
                first_key: make_record(20, 0, 0, 0),
                last_key: make_record(29, 0, 0, 0),
                row_count: 10,
                leaf_cid: ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &"c".repeat(64))
                    .unwrap(),
                resolved_path: None,
            },
        ];

        let novelty = vec![
            make_record(5, 0, 0, 1),  // leaf 0
            make_record(10, 0, 0, 1), // leaf 1 (exactly at boundary)
            make_record(15, 0, 0, 1), // leaf 1
            make_record(25, 0, 0, 1), // leaf 2
        ];

        let slices = slice_novelty_to_leaves(&novelty, &leaves, cmp);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), 1); // s_id=5
        assert_eq!(slices[1].len(), 2); // s_id=10, 15
        assert_eq!(slices[2].len(), 1); // s_id=25
    }

    /// Proves that novelty before the first leaf's first_key routes to leaf 0.
    ///
    /// The first leaf starts at s_id=10. Novelty at s_id=3 and s_id=7 must land
    /// in leaf 0's slice (the -∞ bucket), not be dropped.
    #[test]
    fn test_novelty_slicing_before_first_leaf() {
        let cmp = cmp_for_order(RunSortOrder::Spot);

        // 2 leaves starting at s_id=10 and s_id=20 — no leaf covers [0, 10)
        let leaves = vec![
            LeafEntry {
                first_key: make_record(10, 0, 0, 0),
                last_key: make_record(19, 0, 0, 0),
                row_count: 10,
                leaf_cid: ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &"a".repeat(64))
                    .unwrap(),
                resolved_path: None,
            },
            LeafEntry {
                first_key: make_record(20, 0, 0, 0),
                last_key: make_record(29, 0, 0, 0),
                row_count: 10,
                leaf_cid: ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &"b".repeat(64))
                    .unwrap(),
                resolved_path: None,
            },
        ];

        let novelty = vec![
            make_record(3, 0, 0, 2),  // before any leaf
            make_record(7, 0, 0, 2),  // still before first leaf
            make_record(15, 0, 0, 2), // in leaf 0 (s_id 10..20)
            make_record(25, 0, 0, 2), // in leaf 1 (s_id 20..+∞)
        ];

        let slices = slice_novelty_to_leaves(&novelty, &leaves, cmp);
        assert_eq!(slices.len(), 2);
        // Leaf 0 owns [-∞, leaf[1].first_key), so s_id=3, 7, and 15 all land here
        assert_eq!(slices[0].len(), 3);
        assert_eq!(slices[0][0].s_id.as_u64(), 3);
        assert_eq!(slices[0][1].s_id.as_u64(), 7);
        assert_eq!(slices[0][2].s_id.as_u64(), 15);
        // Leaf 1 owns [leaf[1].first_key, +∞)
        assert_eq!(slices[1].len(), 1);
        assert_eq!(slices[1][0].s_id.as_u64(), 25);
    }

    /// Novelty before first leaf triggers merge into first leaf via full branch update.
    #[test]
    fn test_branch_update_novelty_before_first_leaf_merges() {
        // Build leaves starting at s_id=10 (not 0)
        let records: Vec<RunRecord> = (10..30)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let (manifest, cas) = build_branch_and_leaves(&records, 10, 1);
        let original_cids: Vec<_> = manifest.leaves.iter().map(|l| l.leaf_cid.clone()).collect();

        // Novelty below the first leaf's first_key
        let novelty = vec![make_record(3, 1, 30, 5), make_record(7, 1, 70, 5)];
        let config = default_config();
        let mut fetch = |cid: &ContentId| -> Result<Vec<u8>, IncrementalBranchError> {
            cas.get(cid).cloned().ok_or_else(|| {
                IncrementalBranchError::Io(io::Error::new(io::ErrorKind::NotFound, "not found"))
            })
        };

        let result = update_branch(
            &manifest,
            &novelty,
            RunSortOrder::Spot,
            0,
            &config,
            &mut fetch,
        )
        .unwrap();

        // First leaf should be replaced (received novelty), second unchanged
        assert_ne!(result.leaf_entries[0].leaf_cid, original_cids[0]);
        assert_eq!(result.leaf_entries[1].leaf_cid, original_cids[1]);

        // First leaf now has 10 original + 2 new = 12 rows
        assert_eq!(result.leaf_entries[0].row_count, 12);

        // Verify the new records are actually in the merged leaf
        let blob = &result.new_leaf_blobs[0];
        let hdr = read_leaf_header(&blob.bytes).unwrap();
        let d = &hdr.leaflet_dir[0];
        let ld = &blob.bytes[d.offset as usize..d.offset as usize + d.compressed_len as usize];
        let decoded = decode_leaflet(ld, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        // s_id=3 should be first in SPOT order
        assert_eq!(decoded.s_ids[0], 3);
        assert_eq!(decoded.s_ids[1], 7);
    }
}
