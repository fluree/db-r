//! Branch manifest: single-level index mapping key ranges to leaf files.
//!
//! Each graph's SPOT index has one branch manifest (`branch.fbr`) that
//! stores the key range and path of every leaf file. Query routing uses
//! binary search on the branch to find the leaf(s) containing a target key.
//!
//! ## Binary format
//!
//! ```text
//! [BranchHeader: 16 bytes]
//!   magic: "FBR1" (4B)
//!   version: u8
//!   _pad: [u8; 3]
//!   leaf_count: u32
//!   _reserved: u32
//! [LeafEntries: leaf_count × 96 bytes]
//!   For each leaf:
//!     first_key: RunRecord (40 bytes)
//!     last_key:  RunRecord (40 bytes)
//!     row_count: u64
//!     path_offset: u32
//!     path_len: u16
//!     _pad: u16
//! [PathTable]
//!   Concatenated leaf file paths (relative, UTF-8)
//! ```

use super::leaf::LeafInfo;
use super::run_record::{cmp_spot, RunRecord};
use sha2::{Sha256, Digest};
use std::cmp::Ordering;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

/// Magic bytes for a branch manifest file.
const BRANCH_MAGIC: [u8; 4] = *b"FBR1";

/// Current branch manifest format version.
const BRANCH_VERSION: u8 = 1;

/// Size of the branch header in bytes.
const BRANCH_HEADER_LEN: usize = 16;

/// Size of each leaf entry in the manifest (excluding path).
const LEAF_ENTRY_LEN: usize = 96;

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
    /// Content hash (SHA-256 hex) of the leaf file.
    pub content_hash: String,
    /// Resolved filesystem path (set during read, relative to branch directory).
    pub path: PathBuf,
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
        let start = self.leaves.partition_point(|entry| {
            cmp(&entry.last_key, min_key) == Ordering::Less
        });

        // End: first leaf whose first_key > max_key
        // (include leaves that start within or before our range)
        let end = self.leaves.partition_point(|entry| {
            cmp(&entry.first_key, max_key) != Ordering::Greater
        });

        start..end
    }

    /// Find all leaves that may contain records for a given subject.
    ///
    /// Returns a range of leaf indices. A subject's facts may span multiple
    /// leaves if the subject has many predicates/objects.
    pub fn find_leaves_for_subject(&self, g_id: u32, s_id: u32) -> Range<usize> {
        if self.leaves.is_empty() {
            return 0..0;
        }

        // Find first leaf that could contain (g_id, s_id):
        // We want the first leaf whose last_key >= (g_id, s_id, 0, MIN, ...)
        let start = self.leaves.partition_point(|entry| {
            entry.last_key.g_id < g_id
                || (entry.last_key.g_id == g_id && entry.last_key.s_id < s_id)
        });

        // Find last leaf that could contain (g_id, s_id):
        // We want the first leaf whose first_key > (g_id, s_id, MAX, MAX, ...)
        let end = self.leaves.partition_point(|entry| {
            entry.first_key.g_id < g_id
                || (entry.first_key.g_id == g_id && entry.first_key.s_id <= s_id)
        });

        start..end
    }
}

// ============================================================================
// Write
// ============================================================================

/// Write a content-addressed branch manifest file from leaf infos.
///
/// Builds the branch bytes, computes SHA-256, writes to `{hash}.fbr` in `dir`.
/// The path table stores leaf content hashes (e.g. `abc123...ef.fli`).
/// Returns the branch content hash.
pub fn write_branch_manifest(dir: &Path, leaves: &[LeafInfo]) -> io::Result<String> {
    let data = build_branch_bytes(leaves);
    let hash = {
        let digest = Sha256::digest(&data);
        hex::encode(digest)
    };
    let path = dir.join(format!("{}.fbr", hash));
    std::fs::write(&path, &data)?;
    Ok(hash)
}

/// Build branch manifest bytes in memory.
fn build_branch_bytes(leaves: &[LeafInfo]) -> Vec<u8> {
    // ---- Build path table (leaf content hashes as filenames) ----
    let mut path_table = Vec::new();
    let mut path_entries: Vec<(u32, u16)> = Vec::with_capacity(leaves.len());

    for leaf in leaves {
        // Store content-hash filename: "{hash}.fli"
        let filename = format!("{}.fli", leaf.content_hash);
        let path_bytes = filename.as_bytes();
        let offset = path_table.len() as u32;
        let len = path_bytes.len() as u16;
        path_table.extend_from_slice(path_bytes);
        path_entries.push((offset, len));
    }

    let leaf_count = leaves.len();
    let entries_size = leaf_count * LEAF_ENTRY_LEN;
    let total_size = BRANCH_HEADER_LEN + entries_size + path_table.len();

    let mut buf = Vec::with_capacity(total_size);

    // ---- Header ----
    buf.extend_from_slice(&BRANCH_MAGIC);
    buf.push(BRANCH_VERSION);
    buf.extend_from_slice(&[0u8; 3]); // pad
    buf.extend_from_slice(&(leaf_count as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved

    // ---- Leaf entries ----
    let mut rec_buf = [0u8; 40];
    for (i, leaf) in leaves.iter().enumerate() {
        leaf.first_key.write_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        leaf.last_key.write_le(&mut rec_buf);
        buf.extend_from_slice(&rec_buf);

        buf.extend_from_slice(&leaf.total_rows.to_le_bytes());
        buf.extend_from_slice(&path_entries[i].0.to_le_bytes());
        buf.extend_from_slice(&path_entries[i].1.to_le_bytes());
        buf.extend_from_slice(&[0u8; 2]); // pad
    }

    // ---- Path table ----
    buf.extend_from_slice(&path_table);

    buf
}

// ============================================================================
// Read
// ============================================================================

/// Read a branch manifest from a file.
pub fn read_branch_manifest(path: &Path) -> io::Result<BranchManifest> {
    let data = std::fs::read(path)?;
    read_branch_manifest_from_bytes(&data, path.parent())
}

/// Find the branch manifest file (.fbr) in a directory.
///
/// Returns the path to the first `.fbr` file found, or an error if none exists.
pub fn find_branch_file(dir: &Path) -> io::Result<PathBuf> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "fbr") {
            return Ok(path);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("no .fbr branch file found in {:?}", dir),
    ))
}

pub fn read_branch_manifest_from_bytes(
    data: &[u8],
    base_dir: Option<&Path>,
) -> io::Result<BranchManifest> {
    if data.len() < BRANCH_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest too small",
        ));
    }
    if data[0..4] != BRANCH_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest: invalid magic",
        ));
    }
    let version = data[4];
    if version != BRANCH_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("branch manifest: unsupported version {}", version),
        ));
    }

    let leaf_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    let entries_end = BRANCH_HEADER_LEN + leaf_count * LEAF_ENTRY_LEN;
    if entries_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "branch manifest: entries truncated",
        ));
    }

    let path_table = &data[entries_end..];

    let mut leaves = Vec::with_capacity(leaf_count);
    let mut pos = BRANCH_HEADER_LEN;

    for _ in 0..leaf_count {
        let first_key = RunRecord::read_le(data[pos..pos + 40].try_into().unwrap());
        pos += 40;
        let last_key = RunRecord::read_le(data[pos..pos + 40].try_into().unwrap());
        pos += 40;
        let row_count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let path_offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let path_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        pos += 2; // pad

        if path_offset + path_len > path_table.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "branch manifest: path extends past table",
            ));
        }
        let relative_path = std::str::from_utf8(&path_table[path_offset..path_offset + path_len])
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("branch manifest: invalid path UTF-8: {}", e),
                )
            })?;

        let full_path = match base_dir {
            Some(dir) => dir.join(relative_path),
            None => PathBuf::from(relative_path),
        };

        // Extract content hash from filename (e.g. "abc123...ef.fli" → "abc123...ef")
        let content_hash = Path::new(relative_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        leaves.push(LeafEntry {
            first_key,
            last_key,
            row_count,
            content_hash,
            path: full_path,
        });
    }

    Ok(BranchManifest { leaves })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::global_dict::dt_ids;
    use fluree_db_core::value_id::{ObjKind, ObjKey};

    fn make_record(g_id: u32, s_id: u32, p_id: u32, val: i64, t: i64) -> RunRecord {
        RunRecord::new(
            g_id, s_id, p_id,
            ObjKind::NUM_INT, ObjKey::encode_i64(val),
            t, true, dt_ids::INTEGER, 0, None,
        )
    }

    fn make_leaf_info(
        _dir: &Path,
        index: u32,
        first: RunRecord,
        last: RunRecord,
        rows: u64,
    ) -> LeafInfo {
        // Use a fake content hash for test purposes
        let hash = format!("{:064x}", index);
        LeafInfo {
            path: _dir.join(format!("{}.fli", hash)),
            content_hash: hash,
            leaf_index: index,
            total_rows: rows,
            first_key: first,
            last_key: last,
        }
    }

    #[test]
    fn test_branch_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_branch_roundtrip");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let infos = vec![
            make_leaf_info(
                &dir, 0,
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 5, 99, 1),
                5000,
            ),
            make_leaf_info(
                &dir, 1,
                make_record(0, 101, 1, 0, 1),
                make_record(0, 200, 10, 50, 1),
                5000,
            ),
            make_leaf_info(
                &dir, 2,
                make_record(0, 201, 1, 0, 1),
                make_record(0, 300, 3, 0, 2),
                3000,
            ),
        ];

        let branch_hash = write_branch_manifest(&dir, &infos).unwrap();
        let manifest_path = dir.join(format!("{}.fbr", branch_hash));
        let manifest = read_branch_manifest(&manifest_path).unwrap();

        assert_eq!(manifest.leaves.len(), 3);
        assert_eq!(manifest.leaves[0].row_count, 5000);
        assert_eq!(manifest.leaves[0].first_key.s_id, 1);
        assert_eq!(manifest.leaves[0].last_key.s_id, 100);
        assert_eq!(manifest.leaves[1].first_key.s_id, 101);
        assert_eq!(manifest.leaves[2].last_key.s_id, 300);
        // Verify content hashes were preserved
        assert_eq!(manifest.leaves[0].content_hash, format!("{:064x}", 0));
        assert_eq!(manifest.leaves[1].content_hash, format!("{:064x}", 1));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_leaf() {
        let dir = std::env::temp_dir().join("fluree_test_branch_find");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let infos = vec![
            make_leaf_info(
                &dir, 0,
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 5, 99, 1),
                5000,
            ),
            make_leaf_info(
                &dir, 1,
                make_record(0, 101, 1, 0, 1),
                make_record(0, 200, 10, 50, 1),
                5000,
            ),
        ];

        let branch_hash = write_branch_manifest(&dir, &infos).unwrap();
        let manifest_path = dir.join(format!("{}.fbr", branch_hash));
        let manifest = read_branch_manifest(&manifest_path).unwrap();

        // Key in leaf 0
        let key = make_record(0, 50, 1, 0, 1);
        let leaf = manifest.find_leaf(&key).unwrap();
        assert_eq!(leaf.first_key.s_id, 1);

        // Key in leaf 1
        let key = make_record(0, 150, 1, 0, 1);
        let leaf = manifest.find_leaf(&key).unwrap();
        assert_eq!(leaf.first_key.s_id, 101);

        // Key before all leaves
        let key = make_record(0, 0, 0, 0, 0);
        assert!(manifest.find_leaf(&key).is_none());

        // Key after all leaves
        let key = make_record(0, 999, 1, 0, 1);
        assert!(manifest.find_leaf(&key).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_leaves_for_subject() {
        let dir = std::env::temp_dir().join("fluree_test_branch_subject");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Subject 100 spans leaves 0 and 1
        let infos = vec![
            make_leaf_info(
                &dir, 0,
                make_record(0, 1, 1, 0, 1),
                make_record(0, 100, 3, 0, 1),
                5000,
            ),
            make_leaf_info(
                &dir, 1,
                make_record(0, 100, 4, 0, 1),
                make_record(0, 200, 10, 0, 1),
                5000,
            ),
            make_leaf_info(
                &dir, 2,
                make_record(0, 201, 1, 0, 1),
                make_record(0, 300, 5, 0, 1),
                3000,
            ),
        ];

        let branch_hash = write_branch_manifest(&dir, &infos).unwrap();
        let manifest_path = dir.join(format!("{}.fbr", branch_hash));
        let manifest = read_branch_manifest(&manifest_path).unwrap();

        // Subject 100 spans leaves 0-1
        let range = manifest.find_leaves_for_subject(0, 100);
        assert_eq!(range, 0..2);

        // Subject 50 is only in leaf 0
        let range = manifest.find_leaves_for_subject(0, 50);
        assert_eq!(range, 0..1);

        // Subject 250 is only in leaf 2
        let range = manifest.find_leaves_for_subject(0, 250);
        assert_eq!(range, 2..3);

        // Subject 999 is not in any leaf
        let range = manifest.find_leaves_for_subject(0, 999);
        assert_eq!(range, 3..3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_empty_manifest() {
        let dir = std::env::temp_dir().join("fluree_test_branch_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let branch_hash = write_branch_manifest(&dir, &[]).unwrap();
        let manifest_path = dir.join(format!("{}.fbr", branch_hash));
        let manifest = read_branch_manifest(&manifest_path).unwrap();

        assert!(manifest.leaves.is_empty());
        assert_eq!(manifest.find_leaves_for_subject(0, 1), 0..0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
