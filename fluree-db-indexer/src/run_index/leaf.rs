//! Leaf file writer: groups leaflets into leaf files.
//!
//! Each leaf file contains up to 10 leaflets (~50K rows) and is named
//! sequentially: `leaf_000000.fli`, `leaf_000001.fli`, etc.
//!
//! ## Leaf file format
//!
//! ```text
//! [LeafHeader: variable size]
//!   magic: "FLI1" (4B)
//!   version: u8
//!   leaflet_count: u8
//!   dt_width: u8    (1=u8, 2=u16)
//!   p_width: u8     (2=u16, 4=u32)
//!   total_rows: u64
//!   first_key: SortKey (28 bytes)
//!   last_key:  SortKey (28 bytes)
//!   [LeafletDirectory: leaflet_count × 28 bytes]
//!     offset: u64, compressed_len: u32, row_count: u32, first_s_id: u64, first_p_id: u32
//! [Leaflet data: concatenated encoded leaflets]
//! ```

use super::leaflet::LeafletEncoder;
use super::run_record::{RunRecord, RunSortOrder};
use sha2::{Sha256, Digest};
use std::io;
use std::path::PathBuf;

/// Magic bytes for a leaf file.
const LEAF_MAGIC: [u8; 4] = *b"FLI1";

/// Current leaf file format version.
const LEAF_VERSION: u8 = 1;

/// Fixed part of the leaf header: magic(4) + version(1) + leaflet_count(1) + pad(2) + total_rows(8) + first_key(28) + last_key(28) = 72.
const LEAF_HEADER_FIXED: usize = 72;

/// Per-leaflet directory entry: offset(8) + compressed_len(4) + row_count(4) + first_s_id(8) + first_p_id(4) = 28.
const LEAFLET_DIR_ENTRY: usize = 28;

// ============================================================================
// SortKey: compact key for leaf routing
// ============================================================================

/// Compact sort key stored in leaf headers and branch entries.
/// 28 bytes: g_id(4) + s_id(8) + p_id(4) + dt(2) + o_kind(1) + _pad(1) + o_key(8) = 28.
///
/// Note: For branch routing we use full RunRecord keys (40 bytes). This
/// compact form is only used inside leaf file headers for space efficiency.
#[derive(Debug, Clone, Copy)]
pub struct SortKey {
    pub g_id: u32,
    pub s_id: u64,
    pub p_id: u32,
    pub dt: u16,
    pub o_kind: u8,
    pub _pad: u8,
    pub o_key: u64,
}
// Serialized size is SORT_KEY_BYTES (28); in-memory layout may differ.

impl SortKey {
    fn from_record(r: &RunRecord) -> Self {
        Self {
            g_id: r.g_id,
            s_id: r.s_id.as_u64(),
            p_id: r.p_id,
            dt: r.dt,
            o_kind: r.o_kind,
            _pad: 0,
            o_key: r.o_key,
        }
    }

    fn write_to(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.g_id.to_le_bytes());
        buf[4..12].copy_from_slice(&self.s_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.p_id.to_le_bytes());
        buf[16..18].copy_from_slice(&self.dt.to_le_bytes());
        buf[18] = self.o_kind;
        buf[19] = 0;
        buf[20..28].copy_from_slice(&self.o_key.to_le_bytes());
    }

    fn read_from(buf: &[u8]) -> Self {
        Self {
            g_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            s_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            dt: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
            o_kind: buf[18],
            _pad: 0,
            o_key: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        }
    }
}

/// Sort key written size in bytes (exactly 28 bytes in leaf headers).
const SORT_KEY_BYTES: usize = 28;

// ============================================================================
// Encoded leaflet (intermediate)
// ============================================================================

/// An encoded leaflet ready to be written to a leaf file.
struct EncodedLeaflet {
    data: Vec<u8>,
    row_count: u32,
    first_s_id: u64,
    first_p_id: u32,
}

// ============================================================================
// LeafInfo (output metadata)
// ============================================================================

/// Metadata about a written leaf file, used for branch manifest construction.
#[derive(Debug, Clone)]
pub struct LeafInfo {
    pub path: PathBuf,
    pub content_hash: String,
    pub leaf_index: u32,
    pub total_rows: u64,
    pub first_key: RunRecord,
    pub last_key: RunRecord,
}

/// Compute SHA-256 content hash of a byte slice, returning hex string.
fn content_hash(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

// ============================================================================
// LeafWriter
// ============================================================================

/// Groups leaflets into leaf files.
///
/// Feed sorted records via `push_records`. The writer internally accumulates
/// records into leaflets and leaflets into leaf files, flushing to disk as
/// thresholds are reached.
pub struct LeafWriter {
    leaflet_encoder: LeafletEncoder,
    output_dir: PathBuf,
    leaflet_rows: usize,
    leaflets_per_leaf: usize,
    /// Width of dt encoding in bytes (1 = u8, 2 = u16).
    dt_width: u8,
    /// Width of p_id encoding in bytes (2 = u16, 4 = u32).
    p_width: u8,
    /// Buffer of records for the current leaflet.
    record_buf: Vec<RunRecord>,
    /// Encoded leaflets for the current leaf file.
    current_leaflets: Vec<EncodedLeaflet>,
    /// First record of the current leaf (for LeafInfo).
    leaf_first_record: Option<RunRecord>,
    /// Last record seen (for LeafInfo).
    last_record: Option<RunRecord>,
    /// Running leaf counter.
    leaf_count: u32,
    /// Accumulated leaf metadata.
    leaf_infos: Vec<LeafInfo>,
}

impl LeafWriter {
    pub fn new(
        output_dir: PathBuf,
        leaflet_rows: usize,
        leaflets_per_leaf: usize,
        zstd_level: i32,
    ) -> Self {
        Self::with_widths(output_dir, leaflet_rows, leaflets_per_leaf, zstd_level, 1, 2, RunSortOrder::Spot)
    }

    /// Create a LeafWriter with explicit field widths and sort order.
    ///
    /// `dt_width`: 1 (u8), 2 (u16), or 4 (u32). Default: 1.
    /// `p_width`: 2 (u16) or 4 (u32). Default: 2.
    /// `sort_order`: determines Region 1 column layout.
    pub fn with_widths(
        output_dir: PathBuf,
        leaflet_rows: usize,
        leaflets_per_leaf: usize,
        zstd_level: i32,
        dt_width: u8,
        p_width: u8,
        sort_order: RunSortOrder,
    ) -> Self {
        Self {
            leaflet_encoder: LeafletEncoder::with_widths_and_order(zstd_level, p_width, dt_width, sort_order),
            output_dir,
            leaflet_rows,
            leaflets_per_leaf,
            dt_width,
            p_width,
            record_buf: Vec::with_capacity(leaflet_rows),
            current_leaflets: Vec::with_capacity(leaflets_per_leaf),
            leaf_first_record: None,
            last_record: None,
            leaf_count: 0,
            leaf_infos: Vec::new(),
        }
    }

    /// Push a single record. Flushes leaflet/leaf as thresholds are hit.
    pub fn push_record(&mut self, record: RunRecord) -> io::Result<()> {
        if self.leaf_first_record.is_none() {
            self.leaf_first_record = Some(record);
        }
        self.last_record = Some(record);
        self.record_buf.push(record);

        if self.record_buf.len() >= self.leaflet_rows {
            self.flush_leaflet()?;
        }

        Ok(())
    }

    /// Flush the current record buffer as a leaflet.
    fn flush_leaflet(&mut self) -> io::Result<()> {
        if self.record_buf.is_empty() {
            return Ok(());
        }

        let row_count = self.record_buf.len() as u32;
        let first_s_id = self.record_buf[0].s_id.as_u64();
        let first_p_id = self.record_buf[0].p_id;

        let data = self.leaflet_encoder.encode_leaflet(&self.record_buf);
        self.record_buf.clear();

        self.current_leaflets.push(EncodedLeaflet {
            data,
            row_count,
            first_s_id,
            first_p_id,
        });

        if self.current_leaflets.len() >= self.leaflets_per_leaf {
            self.flush_leaf()?;
        }

        Ok(())
    }

    /// Flush accumulated leaflets as a content-addressed leaf file.
    fn flush_leaf(&mut self) -> io::Result<()> {
        if self.current_leaflets.is_empty() {
            return Ok(());
        }

        let leaf_index = self.leaf_count;

        let first_key = self
            .leaf_first_record
            .expect("leaf_first_record must be set");
        let last_key = self.last_record.expect("last_record must be set");

        let total_rows: u64 = self
            .current_leaflets
            .iter()
            .map(|l| l.row_count as u64)
            .sum();

        // Build leaf bytes in memory, then hash for content-addressed filename
        let leaf_bytes = build_leaf_bytes(
            &self.current_leaflets,
            &first_key,
            &last_key,
            total_rows,
            self.dt_width,
            self.p_width,
        );
        let hash = content_hash(&leaf_bytes);
        let leaf_path = self.output_dir.join(format!("{}.fli", hash));

        std::fs::write(&leaf_path, &leaf_bytes)?;

        tracing::debug!(
            leaf = leaf_index,
            leaflets = self.current_leaflets.len(),
            rows = total_rows,
            hash = %hash,
            "leaf file written"
        );

        self.leaf_infos.push(LeafInfo {
            path: leaf_path,
            content_hash: hash,
            leaf_index,
            total_rows,
            first_key,
            last_key,
        });

        self.current_leaflets.clear();
        self.leaf_first_record = None;
        self.leaf_count += 1;

        Ok(())
    }

    /// Finish writing: flush any remaining records/leaflets, return leaf metadata.
    pub fn finish(mut self) -> io::Result<Vec<LeafInfo>> {
        // Flush remaining records as a (possibly partial) leaflet
        self.flush_leaflet()?;
        // Flush remaining leaflets as a (possibly partial) leaf
        self.flush_leaf()?;
        Ok(self.leaf_infos)
    }

    /// Number of leaf files written so far.
    pub fn leaf_count(&self) -> u32 {
        self.leaf_count
    }
}

// ============================================================================
// Write a leaf file
// ============================================================================

/// Build leaf file bytes in memory (for content hashing before writing).
fn build_leaf_bytes(
    leaflets: &[EncodedLeaflet],
    first_key: &RunRecord,
    last_key: &RunRecord,
    total_rows: u64,
    dt_width: u8,
    p_width: u8,
) -> Vec<u8> {
    let leaflet_count = leaflets.len();
    let dir_size = leaflet_count * LEAFLET_DIR_ENTRY;
    let header_size = LEAF_HEADER_FIXED + dir_size;
    let data_size: usize = leaflets.iter().map(|l| l.data.len()).sum();
    let total_size = header_size + data_size;

    let mut buf = Vec::with_capacity(total_size);

    // ---- Header ----
    buf.extend_from_slice(&LEAF_MAGIC);
    buf.push(LEAF_VERSION);
    buf.push(leaflet_count as u8);
    buf.push(dt_width);
    buf.push(p_width);
    buf.extend_from_slice(&total_rows.to_le_bytes());

    // First/last keys (28 bytes each)
    let mut key_buf = [0u8; SORT_KEY_BYTES];
    SortKey::from_record(first_key).write_to(&mut key_buf);
    buf.extend_from_slice(&key_buf);
    SortKey::from_record(last_key).write_to(&mut key_buf);
    buf.extend_from_slice(&key_buf);

    // ---- Leaflet directory ----
    let mut offset = header_size as u64;
    for l in leaflets {
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(l.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&l.row_count.to_le_bytes());
        buf.extend_from_slice(&l.first_s_id.to_le_bytes());
        buf.extend_from_slice(&l.first_p_id.to_le_bytes());
        offset += l.data.len() as u64;
    }

    // ---- Leaflet data ----
    for l in leaflets {
        buf.extend_from_slice(&l.data);
    }

    buf
}

// ============================================================================
// Read a leaf file (for query)
// ============================================================================

/// Parsed leaf file header and leaflet directory.
#[derive(Debug)]
pub struct LeafFileHeader {
    pub leaflet_count: u8,
    /// Width of dt encoding in bytes (1=u8, 2=u16).
    pub dt_width: u8,
    /// Width of p_id encoding in bytes (2 = u16, 4 = u32).
    pub p_width: u8,
    pub total_rows: u64,
    pub first_key: SortKey,
    pub last_key: SortKey,
    pub leaflet_dir: Vec<LeafletDirEntry>,
}

/// Entry in the leaflet directory.
#[derive(Debug, Clone)]
pub struct LeafletDirEntry {
    pub offset: u64,
    pub compressed_len: u32,
    pub row_count: u32,
    pub first_s_id: u64,
    pub first_p_id: u32,
}

/// Read a leaf file header + leaflet directory (no leaflet data).
pub fn read_leaf_header(data: &[u8]) -> io::Result<LeafFileHeader> {
    if data.len() < LEAF_HEADER_FIXED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "leaf file too small",
        ));
    }
    if data[0..4] != LEAF_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "leaf file: invalid magic",
        ));
    }
    let version = data[4];
    if version != LEAF_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("leaf file: unsupported version {}", version),
        ));
    }

    let leaflet_count = data[5];
    let dt_width = data[6];
    let p_width = data[7];
    if dt_width != 1 && dt_width != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("leaf file: invalid dt_width {} (expected 1 or 2)", dt_width),
        ));
    }
    if p_width != 2 && p_width != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("leaf file: invalid p_width {} (expected 2 or 4)", p_width),
        ));
    }
    let total_rows = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let first_key = SortKey::read_from(&data[16..44]);
    let last_key = SortKey::read_from(&data[44..72]);

    let dir_start = LEAF_HEADER_FIXED;
    let dir_end = dir_start + leaflet_count as usize * LEAFLET_DIR_ENTRY;
    if dir_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "leaf file: leaflet directory truncated",
        ));
    }

    let mut leaflet_dir = Vec::with_capacity(leaflet_count as usize);
    let mut pos = dir_start;
    for _ in 0..leaflet_count {
        leaflet_dir.push(LeafletDirEntry {
            offset: u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap()),
            row_count: u32::from_le_bytes(data[pos + 12..pos + 16].try_into().unwrap()),
            first_s_id: u64::from_le_bytes(data[pos + 16..pos + 24].try_into().unwrap()),
            first_p_id: u32::from_le_bytes(data[pos + 24..pos + 28].try_into().unwrap()),
        });
        pos += LEAFLET_DIR_ENTRY;
    }

    Ok(LeafFileHeader {
        leaflet_count,
        dt_width,
        p_width,
        total_rows,
        first_key,
        last_key,
        leaflet_dir,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::DatatypeDictId;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKind, ObjKey};

    fn make_record(s_id: u64, p_id: u32, val: i64, t: i64) -> RunRecord {
        RunRecord::new(
            0, SubjectId::from_u64(s_id), p_id,
            ObjKind::NUM_INT, ObjKey::encode_i64(val),
            t, true, DatatypeDictId::INTEGER.as_u16(), 0, None,
        )
    }

    #[test]
    fn test_leaf_writer_single_leaf() {
        let dir = std::env::temp_dir().join("fluree_test_leaf_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 15 records, 5 per leaflet, 10 per leaf → 1 leaf with 3 leaflets
        let mut writer = LeafWriter::new(dir.clone(), 5, 10, 1);
        for i in 0..15u64 {
            writer.push_record(make_record(i, 1, i as i64, 1)).unwrap();
        }
        let infos = writer.finish().unwrap();

        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].total_rows, 15);
        assert_eq!(infos[0].first_key.s_id.as_u64(), 0);
        assert_eq!(infos[0].last_key.s_id.as_u64(), 14);

        // Read back and verify header
        let data = std::fs::read(&infos[0].path).unwrap();
        let header = read_leaf_header(&data).unwrap();
        assert_eq!(header.leaflet_count, 3);
        assert_eq!(header.total_rows, 15);
        assert_eq!(header.leaflet_dir[0].row_count, 5);
        assert_eq!(header.leaflet_dir[1].row_count, 5);
        assert_eq!(header.leaflet_dir[2].row_count, 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_leaf_writer_multiple_leaves() {
        let dir = std::env::temp_dir().join("fluree_test_leaf_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 55 records, 5 per leaflet, 2 leaflets per leaf
        // → 5 full leaves (10 records each) + 1 partial leaf (5 records, 1 leaflet)
        let mut writer = LeafWriter::new(dir.clone(), 5, 2, 1);
        for i in 0..55u64 {
            writer.push_record(make_record(i, 1, i as i64, 1)).unwrap();
        }
        let infos = writer.finish().unwrap();

        assert_eq!(infos.len(), 6);
        for i in 0..5 {
            assert_eq!(infos[i].total_rows, 10);
        }
        assert_eq!(infos[5].total_rows, 5);

        // Total rows across all leaves
        let total: u64 = infos.iter().map(|i| i.total_rows).sum();
        assert_eq!(total, 55);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_leaf_writer_empty() {
        let dir = std::env::temp_dir().join("fluree_test_leaf_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let writer = LeafWriter::new(dir.clone(), 5000, 10, 1);
        let infos = writer.finish().unwrap();
        assert!(infos.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_leaf_file_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_leaf_roundtrip");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = LeafWriter::new(dir.clone(), 3, 10, 1);
        for i in 0..9u64 {
            writer.push_record(make_record(i, 1, i as i64, 1)).unwrap();
        }
        let infos = writer.finish().unwrap();

        assert_eq!(infos.len(), 1);

        // Read leaf and decode all leaflets
        let data = std::fs::read(&infos[0].path).unwrap();
        let header = read_leaf_header(&data).unwrap();

        assert_eq!(header.leaflet_count, 3);
        let mut all_s_ids = Vec::new();
        for entry in &header.leaflet_dir {
            let leaflet_data =
                &data[entry.offset as usize..entry.offset as usize + entry.compressed_len as usize];
            let decoded = super::super::leaflet::decode_leaflet(
                leaflet_data, header.p_width, header.dt_width,
                crate::run_index::run_record::RunSortOrder::Spot,
            ).unwrap();
            all_s_ids.extend_from_slice(&decoded.s_ids);
        }

        assert_eq!(all_s_ids, (0..9u64).collect::<Vec<_>>());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
