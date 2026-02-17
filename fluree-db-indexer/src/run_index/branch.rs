//! Branch manifest: single-level index mapping key ranges to leaf CIDs.
//!
//! Each graph's fact index has one branch manifest that stores the key range
//! and `ContentId` of every leaf. Query routing uses binary search on the
//! branch to find the leaf(s) containing a target key.
//!
//! ## Binary format (`FBR2`)
//!
//! ```text
//! [Header: 16 bytes]
//!   magic: "FBR2" (4B)
//!   version: u8 (= 1)
//!   order_id: u8        (canonical: 0=SPOT, 1=PSOT, 2=POST, 3=OPST)
//!   g_id: u16 (LE)
//!   leaf_count: u32 (LE)
//!   _reserved: u32 (= 0)
//! [Leaf entries: leaf_count × variable]
//!   first_key: RunRecord (34B, LE, graphless)
//!   last_key: RunRecord (34B, LE, graphless)
//!   row_count: u64 (LE)
//!   leaf_cid_len: u16 (LE)
//!   leaf_cid_bytes: [u8; leaf_cid_len]
//! ```
//!
//! No path table. CID bytes are the leaf identity. Decoding is O(n) linear
//! scan, then binary search for routing on the in-memory `Vec<LeafEntry>`.

use super::run_record::{cmp_spot, RunRecord, RunSortOrder, RECORD_WIRE_SIZE};
use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_BRANCH;
use fluree_db_core::ContentId;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::io;
use std::ops::Range;
use std::path::Path;

/// Magic bytes for a v2 branch manifest.
const BRANCH_V2_MAGIC: [u8; 4] = *b"FBR2";

/// Current branch manifest format version.
const BRANCH_V2_VERSION: u8 = 1;

/// Size of the branch header in bytes.
const BRANCH_V2_HEADER_LEN: usize = 16;

/// Minimum per-leaf-entry size: 2 × 34B keys + 8B row_count + 2B cid_len = 78.
/// Actual size is 78 + cid_len bytes (variable due to CID).
const LEAF_ENTRY_MIN_LEN: usize = 2 * RECORD_WIRE_SIZE + 8 + 2;

// ============================================================================
// BranchManifest (in-memory)
// ============================================================================

/// In-memory branch manifest for query routing.
#[derive(Debug)]
pub struct BranchManifest {
    pub leaves: Vec<LeafEntry>,
}

/// A single leaf entry in the branch manifest.
#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub first_key: RunRecord,
    pub last_key: RunRecord,
    pub row_count: u64,
    /// Content identifier for the leaf blob.
    pub leaf_cid: ContentId,
    /// Resolved local path to the leaf file on disk.
    /// Set during store loading; `None` after branch decode.
    pub resolved_path: Option<std::path::PathBuf>,
}

impl BranchManifest {
    /// Binary search for the leaf containing the given key (SPOT order).
    ///
    /// Returns the first leaf whose key range includes `key`, or `None`
    /// if the key is outside all leaf ranges.
    pub fn find_leaf(&self, key: &RunRecord) -> Option<&LeafEntry> {
        self.find_leaf_with_cmp(key, cmp_spot)
    }

    /// Binary search for the leaf containing the given key using an explicit comparator.
    ///
    /// This is the generic version that works with any sort order's comparator.
    pub fn find_leaf_with_cmp(
        &self,
        key: &RunRecord,
        cmp: fn(&RunRecord, &RunRecord) -> Ordering,
    ) -> Option<&LeafEntry> {
        if self.leaves.is_empty() {
            return None;
        }

        // Find the number of leaves whose first_key <= key.
        // partition_point returns the first index where the predicate is false,
        // i.e., the first leaf where first_key > key.
        let idx = self
            .leaves
            .partition_point(|entry| cmp(&entry.first_key, key) != Ordering::Greater);

        if idx == 0 {
            // All leaves have first_key > key — key is before everything.
            return None;
        }

        // The candidate is the last leaf with first_key <= key.
        let candidate = &self.leaves[idx - 1];
        if cmp(key, &candidate.last_key) != Ordering::Greater {
            Some(candidate)
        } else {
            // Key falls in a gap between leaves, or after all leaves.
            None
        }
    }

    /// Find all leaf indices whose key range overlaps [min_key, max_key].
    ///
    /// Works with any sort order via the comparator function. Returns a
    /// contiguous range of leaf indices (leaves are sorted by key).
    pub fn find_leaves_in_range(
        &self,
        min_key: &RunRecord,
        max_key: &RunRecord,
        cmp: fn(&RunRecord, &RunRecord) -> Ordering,
    ) -> Range<usize> {
        if self.leaves.is_empty() {
            return 0..0;
        }

        // Start: first leaf whose last_key >= min_key
        // (skip leaves that end before our range starts)
        let start = self
            .leaves
            .partition_point(|entry| cmp(&entry.last_key, min_key) == Ordering::Less);

        // End: first leaf whose first_key > max_key
        // (include leaves that start within or before our range)
        let end = self
            .leaves
            .partition_point(|entry| cmp(&entry.first_key, max_key) != Ordering::Greater);

        start..end
    }

    /// Find all leaves that may contain records for a given subject.
    ///
    /// Returns a range of leaf indices. A subject's facts may span multiple
    /// leaves if the subject has many predicates/objects.
    ///
    /// Note: g_id is not checked because each graph has its own branch
    /// manifest — all entries in a branch share the same g_id.
    pub fn find_leaves_for_subject(&self, s_id: u64) -> Range<usize> {
        if self.leaves.is_empty() {
            return 0..0;
        }

        // Find first leaf that could contain s_id:
        // We want the first leaf whose last_key.s_id >= s_id
        let start = self
            .leaves
            .partition_point(|entry| entry.last_key.s_id.as_u64() < s_id);

        // Find last leaf that could contain s_id:
        // We want the first leaf whose first_key.s_id > s_id
        let end = self
            .leaves
            .partition_point(|entry| entry.first_key.s_id.as_u64() <= s_id);

        start..end
    }
}

// ============================================================================
// Encode (FBR2)
// ============================================================================

/// Build FBR2 branch manifest bytes in memory.
///
/// The header embeds `order_id` and `g_id` so that readers can validate
/// context without external metadata. Keys are graphless (34B RunRecord wire
/// format); the branch context carries the graph identity.
pub fn build_branch_v2_bytes(order: RunSortOrder, g_id: u16, leaves: &[LeafEntry]) -> Vec<u8> {
    // Estimate capacity: header + entries.
    // Each CID is typically ~36-38 bytes; use 40 as conservative estimate.
    let estimated = BRANCH_V2_HEADER_LEN + leaves.len() * (LEAF_ENTRY_MIN_LEN + 40);
    let mut buf = Vec::with_capacity(estimated);

    // ---- Header (16 bytes) ----
    buf.extend_from_slice(&BRANCH_V2_MAGIC);
    buf.push(BRANCH_V2_VERSION);
    buf.push(order.to_wire_id());
    buf.extend_from_slice(&g_id.to_le_bytes());
    buf.extend_from_slice(&(leaves.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved

    // ---- Leaf entries (variable size each) ----
    let mut rec_buf = [0u8; RECORD_WIRE_SIZE];
    for leaf in leaves {
        leaf.first_key.write_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        leaf.last_key.write_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        buf.extend_from_slice(&leaf.row_count.to_le_bytes());

        let cid_bytes = leaf.leaf_cid.to_bytes();
        buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&cid_bytes);
    }

    buf
}

/// Decode an FBR2 branch manifest from bytes.
///
/// Performs O(n) linear decode of all entries, materializing the full
/// `BranchManifest`. Routing after decode is binary search.
pub fn read_branch_v2_from_bytes(data: &[u8]) -> io::Result<BranchManifest> {
    if data.len() < BRANCH_V2_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest too small for header",
        ));
    }
    if data[0..4] != BRANCH_V2_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "branch manifest: expected magic FBR2, got {:?}",
                &data[0..4]
            ),
        ));
    }
    let version = data[4];
    if version != BRANCH_V2_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("branch manifest: unsupported version {version}"),
        ));
    }

    // order_id at [5] and g_id at [6..8] are informational in the header;
    // the caller provides the routing context. We validate them if needed
    // but don't use them for in-memory construction.

    let leaf_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    let mut leaves = Vec::with_capacity(leaf_count);
    let mut pos = BRANCH_V2_HEADER_LEN;

    for _ in 0..leaf_count {
        // first_key: 34 bytes
        if pos + RECORD_WIRE_SIZE > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: entry truncated at first_key",
            ));
        }
        let first_key = RunRecord::read_le(data[pos..pos + RECORD_WIRE_SIZE].try_into().unwrap());
        pos += RECORD_WIRE_SIZE;

        // last_key: 34 bytes
        if pos + RECORD_WIRE_SIZE > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: entry truncated at last_key",
            ));
        }
        let last_key = RunRecord::read_le(data[pos..pos + RECORD_WIRE_SIZE].try_into().unwrap());
        pos += RECORD_WIRE_SIZE;

        // row_count: 8 bytes
        if pos + 8 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: entry truncated at row_count",
            ));
        }
        let row_count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // leaf_cid_len: 2 bytes + cid_bytes
        if pos + 2 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: entry truncated at cid_len",
            ));
        }
        let cid_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + cid_len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: CID bytes extend past data",
            ));
        }
        let leaf_cid = ContentId::from_bytes(&data[pos..pos + cid_len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("branch manifest: invalid CID: {e}"),
            )
        })?;
        pos += cid_len;

        leaves.push(LeafEntry {
            first_key,
            last_key,
            row_count,
            leaf_cid,
            resolved_path: None,
        });
    }

    Ok(BranchManifest { leaves })
}

/// Read the `order_id` from an FBR2 header without fully decoding.
///
/// Returns the `RunSortOrder` stored in the header, or an error if
/// the magic/version is wrong or the order ID is invalid.
pub fn read_branch_v2_order(data: &[u8]) -> io::Result<RunSortOrder> {
    if data.len() < BRANCH_V2_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest too small for header",
        ));
    }
    if data[0..4] != BRANCH_V2_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest: expected magic FBR2",
        ));
    }
    RunSortOrder::from_wire_id(data[5]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("branch manifest: invalid order_id {}", data[5]),
        )
    })
}

/// Read the `g_id` from an FBR2 header without fully decoding.
pub fn read_branch_v2_g_id(data: &[u8]) -> io::Result<u16> {
    if data.len() < BRANCH_V2_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest too small for header",
        ));
    }
    if data[0..4] != BRANCH_V2_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest: expected magic FBR2",
        ));
    }
    Ok(u16::from_le_bytes(data[6..8].try_into().unwrap()))
}

// ============================================================================
// Write to disk (CID-named, no extension)
// ============================================================================

/// Write an FBR2 branch manifest file to disk, named by CID.
///
/// Builds the branch bytes, computes SHA-256, derives the `ContentId`, and
/// writes to `{cid.to_string()}` (no extension) in `dir`.
/// Returns the branch `ContentId`.
pub fn write_branch_v2(
    dir: &Path,
    order: RunSortOrder,
    g_id: u16,
    leaves: &[LeafEntry],
) -> io::Result<ContentId> {
    let data = build_branch_v2_bytes(order, g_id, leaves);
    let digest_hex = hex::encode(Sha256::digest(&data));
    let cid = ContentId::from_hex_digest(CODEC_FLUREE_INDEX_BRANCH, &digest_hex)
        .expect("valid SHA-256 hex digest");
    let path = dir.join(cid.to_string());
    std::fs::write(&path, &data)?;
    Ok(cid)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;

    fn make_record(g_id: u16, s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            g_id,
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

    fn make_leaf_cid(index: u32) -> ContentId {
        let data = format!("leaf-{index}");
        ContentId::from_hex_digest(
            CODEC_FLUREE_INDEX_LEAF,
            &hex::encode(sha2::Sha256::digest(data.as_bytes())),
        )
        .unwrap()
    }

    fn make_leaf_entry(first: RunRecord, last: RunRecord, rows: u64, index: u32) -> LeafEntry {
        LeafEntry {
            first_key: first,
            last_key: last,
            row_count: rows,
            leaf_cid: make_leaf_cid(index),
            resolved_path: None,
        }
    }

    #[test]
    fn test_branch_v2_round_trip() {
        let leaves = vec![
            make_leaf_entry(
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 5, 99, 1),
                5000,
                0,
            ),
            make_leaf_entry(
                make_record(0, 101, 1, 0, 1),
                make_record(0, 200, 10, 50, 1),
                5000,
                1,
            ),
            make_leaf_entry(
                make_record(0, 201, 1, 0, 1),
                make_record(0, 300, 3, 0, 2),
                3000,
                2,
            ),
        ];

        let bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &leaves);
        let manifest = read_branch_v2_from_bytes(&bytes).unwrap();

        assert_eq!(manifest.leaves.len(), 3);
        assert_eq!(manifest.leaves[0].row_count, 5000);
        assert_eq!(manifest.leaves[0].first_key.s_id.as_u64(), 1);
        assert_eq!(manifest.leaves[0].last_key.s_id.as_u64(), 100);
        assert_eq!(manifest.leaves[1].first_key.s_id.as_u64(), 101);
        assert_eq!(manifest.leaves[2].last_key.s_id.as_u64(), 300);
        // Verify CIDs were preserved
        assert_eq!(manifest.leaves[0].leaf_cid, make_leaf_cid(0));
        assert_eq!(manifest.leaves[1].leaf_cid, make_leaf_cid(1));
        assert_eq!(manifest.leaves[2].leaf_cid, make_leaf_cid(2));
    }

    #[test]
    fn test_branch_v2_determinism() {
        let leaves = vec![
            make_leaf_entry(
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 5, 99, 1),
                5000,
                0,
            ),
            make_leaf_entry(
                make_record(0, 101, 1, 0, 1),
                make_record(0, 200, 10, 50, 1),
                5000,
                1,
            ),
        ];

        let bytes1 = build_branch_v2_bytes(RunSortOrder::Psot, 3, &leaves);
        let bytes2 = build_branch_v2_bytes(RunSortOrder::Psot, 3, &leaves);
        assert_eq!(bytes1, bytes2, "same inputs must produce identical bytes");
    }

    #[test]
    fn test_branch_v2_header_metadata() {
        let leaves = vec![make_leaf_entry(
            make_record(0, 1, 1, 0, 1),
            make_record(0, 100, 5, 99, 1),
            5000,
            0,
        )];

        let bytes = build_branch_v2_bytes(RunSortOrder::Post, 42, &leaves);

        // Verify header fields
        assert_eq!(&bytes[0..4], b"FBR2");
        assert_eq!(bytes[4], 1); // version
        assert_eq!(bytes[5], RunSortOrder::Post.to_wire_id()); // order_id
        assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 42); // g_id
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 1); // leaf_count

        // Verify header accessor functions
        assert_eq!(read_branch_v2_order(&bytes).unwrap(), RunSortOrder::Post);
        assert_eq!(read_branch_v2_g_id(&bytes).unwrap(), 42);
    }

    #[test]
    fn test_branch_v2_write_to_disk() {
        let dir = std::env::temp_dir().join("fluree_test_branch_v2_write");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let leaves = vec![make_leaf_entry(
            make_record(0, 1, 1, 0, 1),
            make_record(0, 50, 3, 0, 1),
            2500,
            0,
        )];

        let cid = write_branch_v2(&dir, RunSortOrder::Spot, 0, &leaves).unwrap();

        // File should exist with CID-string name (no extension)
        let path = dir.join(cid.to_string());
        assert!(path.exists(), "branch file should exist at CID-string path");

        // Read it back and verify
        let data = std::fs::read(&path).unwrap();
        let manifest = read_branch_v2_from_bytes(&data).unwrap();
        assert_eq!(manifest.leaves.len(), 1);
        assert_eq!(manifest.leaves[0].row_count, 2500);

        // CID should verify against the file contents
        assert!(cid.verify(&data));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_leaf() {
        let leaves = vec![
            make_leaf_entry(
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 5, 99, 1),
                5000,
                0,
            ),
            make_leaf_entry(
                make_record(0, 101, 1, 0, 1),
                make_record(0, 200, 10, 50, 1),
                5000,
                1,
            ),
        ];

        let bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &leaves);
        let manifest = read_branch_v2_from_bytes(&bytes).unwrap();

        // Key in leaf 0
        let key = make_record(0, 50, 1, 0, 1);
        let leaf = manifest.find_leaf(&key).unwrap();
        assert_eq!(leaf.first_key.s_id.as_u64(), 1);

        // Key in leaf 1
        let key = make_record(0, 150, 1, 0, 1);
        let leaf = manifest.find_leaf(&key).unwrap();
        assert_eq!(leaf.first_key.s_id.as_u64(), 101);

        // Key before all leaves
        let key = make_record(0, 0, 0, 0, 0);
        assert!(manifest.find_leaf(&key).is_none());

        // Key after all leaves
        let key = make_record(0, 999, 1, 0, 1);
        assert!(manifest.find_leaf(&key).is_none());
    }

    #[test]
    fn test_find_leaves_for_subject() {
        // Subject 100 spans leaves 0 and 1
        let leaves = vec![
            make_leaf_entry(
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 3, 0, 1),
                5000,
                0,
            ),
            make_leaf_entry(
                make_record(0, 100, 4, 0, 1),
                make_record(0, 200, 10, 0, 1),
                5000,
                1,
            ),
            make_leaf_entry(
                make_record(0, 201, 1, 0, 1),
                make_record(0, 300, 5, 0, 1),
                3000,
                2,
            ),
        ];

        let bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &leaves);
        let manifest = read_branch_v2_from_bytes(&bytes).unwrap();

        // Subject 100 spans leaves 0-1
        let range = manifest.find_leaves_for_subject(100);
        assert_eq!(range, 0..2);

        // Subject 50 is only in leaf 0
        let range = manifest.find_leaves_for_subject(50);
        assert_eq!(range, 0..1);

        // Subject 250 is only in leaf 2
        let range = manifest.find_leaves_for_subject(250);
        assert_eq!(range, 2..3);

        // Subject 999 is not in any leaf
        let range = manifest.find_leaves_for_subject(999);
        assert_eq!(range, 3..3);
    }

    #[test]
    fn test_empty_manifest() {
        let bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &[]);
        let manifest = read_branch_v2_from_bytes(&bytes).unwrap();

        assert!(manifest.leaves.is_empty());
        assert_eq!(manifest.find_leaves_for_subject(1), 0..0);
    }

    #[test]
    fn test_reject_fbr1_magic() {
        let mut bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &[]);
        // Tamper magic to FBR1
        bytes[0..4].copy_from_slice(b"FBR1");
        let err = read_branch_v2_from_bytes(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("FBR2"),
            "error should mention expected magic"
        );
    }

    #[test]
    fn test_reject_truncated_data() {
        let leaves = vec![make_leaf_entry(
            make_record(0, 1, 1, 0, 1),
            make_record(0, 50, 3, 0, 1),
            1000,
            0,
        )];
        let bytes = build_branch_v2_bytes(RunSortOrder::Spot, 0, &leaves);

        // Truncate in the middle of the entry
        let truncated = &bytes[..BRANCH_V2_HEADER_LEN + 10];
        let err = read_branch_v2_from_bytes(truncated).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
