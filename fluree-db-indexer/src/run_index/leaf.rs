//! Leaf file writer: groups leaflets into leaf files.
//!
//! Each leaf file contains up to 10 leaflets (~50K rows) and is named
//! sequentially: `leaf_000000.fli`, `leaf_000001.fli`, etc.
//!
//! ## Leaf file format
//!
//! ```text
//! [LeafHeader: variable size]
//!   magic: "FLI2" (4B)
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

use super::leaflet::{LeafletEncoder, Region3Entry};
use super::run_record::{RunRecord, RunSortOrder};
use sha2::{Digest, Sha256};
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io::Write};

/// Magic bytes for a leaf file (v2: Region 2 sparse lang/i, u32 t; Region 3 width-aware).
const LEAF_MAGIC: [u8; 4] = *b"FLI2";

/// Current leaf file format version.
const LEAF_VERSION: u8 = 2;

/// Fixed part of the leaf header: magic(4) + version(1) + leaflet_count(1) + pad(2) + total_rows(8) + first_key(26) + last_key(26) = 68.
const LEAF_HEADER_FIXED: usize = 68;

/// Per-leaflet directory entry: offset(8) + compressed_len(4) + row_count(4) + first_s_id(8) + first_p_id(4) = 28.
const LEAFLET_DIR_ENTRY: usize = 28;

// ============================================================================
// SortKey: compact key for leaf routing
// ============================================================================

/// Compact sort key stored in leaf headers and branch entries.
/// 26 bytes: g_id(2) + s_id(8) + p_id(4) + dt(2) + o_kind(1) + _pad(1) + o_key(8) = 26.
///
/// Note: For branch routing we use full RunRecord keys (34 bytes). This
/// compact form is only used inside leaf file headers for space efficiency.
#[derive(Debug, Clone, Copy)]
pub struct SortKey {
    pub g_id: u16,
    pub s_id: u64,
    pub p_id: u32,
    pub dt: u16,
    pub o_kind: u8,
    pub _pad: u8,
    pub o_key: u64,
}
// Serialized size is SORT_KEY_BYTES (26); in-memory layout may differ.

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
        buf[0..2].copy_from_slice(&self.g_id.to_le_bytes());
        buf[2..10].copy_from_slice(&self.s_id.to_le_bytes());
        buf[10..14].copy_from_slice(&self.p_id.to_le_bytes());
        buf[14..16].copy_from_slice(&self.dt.to_le_bytes());
        buf[16] = self.o_kind;
        buf[17] = 0;
        buf[18..26].copy_from_slice(&self.o_key.to_le_bytes());
    }

    fn read_from(buf: &[u8]) -> Self {
        Self {
            g_id: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            s_id: u64::from_le_bytes(buf[2..10].try_into().unwrap()),
            p_id: u32::from_le_bytes(buf[10..14].try_into().unwrap()),
            dt: u16::from_le_bytes(buf[14..16].try_into().unwrap()),
            o_kind: buf[16],
            _pad: 0,
            o_key: u64::from_le_bytes(buf[18..26].try_into().unwrap()),
        }
    }
}

/// Sort key written size in bytes (exactly 26 bytes in leaf headers).
const SORT_KEY_BYTES: usize = 26;

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
    /// Content identifier for the leaf blob (CID-native, no path or hash string).
    pub leaf_cid: fluree_db_core::ContentId,
    pub leaf_index: u32,
    pub total_rows: u64,
    pub first_key: RunRecord,
    pub last_key: RunRecord,
}

/// Perf counters for leaf/leaflet work during index build.
#[derive(Debug, Default, Clone)]
pub struct LeafWriterPerf {
    /// Leaflets encoded (including Region 1/2 zstd + optional Region 3 zstd).
    pub leaflets_encoded: u64,
    /// Total records encoded into leaflets.
    pub leaflet_records: u64,
    /// Time spent building Region 3 entries (collect + sort).
    pub region3_build_time: Duration,
    /// Time spent encoding/compressing leaflets (all regions).
    pub leaflet_encode_time: Duration,
    /// Leaf files flushed to disk.
    pub leaves_flushed: u64,
    /// Total bytes written to leaf files on disk.
    pub leaf_bytes_written: u64,
    /// Time spent hashing+writing leaf files.
    pub leaf_flush_time: Duration,
}

/// Dummy value used for allocating radix-sort scratch buffers.
const R3_DUMMY: Region3Entry = Region3Entry {
    s_id: 0,
    p_id: 0,
    o_kind: 0,
    o_key: 0,
    t: 0,
    op: 0,
    dt: 0,
    lang_id: 0,
    i: 0,
};

/// Radix-sort Region 3 entries by abs(t) descending.
///
/// This is substantially faster than comparison sorting for large leaflets.
#[inline]
fn radix_sort_r3_abs_t_desc(entries: &mut [Region3Entry], tmp: &mut Vec<Region3Entry>) {
    let n = entries.len();
    if n <= 1 {
        return;
    }

    // Ensure tmp has space to receive writes by index.
    tmp.clear();
    tmp.resize(n, R3_DUMMY);

    // 8 passes over 8-bit digits, LSD-first.
    // For descending abs_t, sort by key = !abs_t() ascending.
    let mut write_into_tmp = true;
    for shift in (0..64).step_by(8) {
        let mut counts = [0usize; 256];

        if write_into_tmp {
            for e in entries.iter() {
                let key = !e.abs_t();
                let b = ((key >> shift) & 0xFF) as usize;
                counts[b] += 1;
            }
            let mut offsets = [0usize; 256];
            let mut sum = 0usize;
            for (i, c) in counts.iter().enumerate() {
                offsets[i] = sum;
                sum += *c;
            }
            for e in entries.iter() {
                let key = !e.abs_t();
                let b = ((key >> shift) & 0xFF) as usize;
                let pos = offsets[b];
                tmp[pos] = *e;
                offsets[b] = pos + 1;
            }
        } else {
            for e in tmp.iter() {
                let key = !e.abs_t();
                let b = ((key >> shift) & 0xFF) as usize;
                counts[b] += 1;
            }
            let mut offsets = [0usize; 256];
            let mut sum = 0usize;
            for (i, c) in counts.iter().enumerate() {
                offsets[i] = sum;
                sum += *c;
            }
            for e in tmp.iter() {
                let key = !e.abs_t();
                let b = ((key >> shift) & 0xFF) as usize;
                let pos = offsets[b];
                entries[pos] = *e;
                offsets[b] = pos + 1;
            }
        }

        write_into_tmp = !write_into_tmp;
    }

    // After 8 passes, the final write lands back in `entries`.
    debug_assert!(
        write_into_tmp,
        "radix sort should end with entries as destination"
    );
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
    /// Buffer of records for the current leaflet (Region 1 + 2).
    record_buf: Vec<RunRecord>,
    /// Buffer of history entries for the current leaflet (Region 3).
    /// Contains only non-winning duplicates from merge dedup — NOT a copy
    /// of record_buf. Empty for single-version facts.
    history_buf: Vec<Region3Entry>,
    /// Scratch buffer reused for Region 3 sorting.
    r3_tmp: Vec<Region3Entry>,
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
    perf: LeafWriterPerf,
    perf_enabled: bool,
    /// When true, skip Region 3 (history journal) entirely. Safe for
    /// append-only bulk import where all ops are asserts with unique `t`.
    skip_region3: bool,
}

impl LeafWriter {
    pub fn new(
        output_dir: PathBuf,
        leaflet_rows: usize,
        leaflets_per_leaf: usize,
        zstd_level: i32,
    ) -> Self {
        Self::with_widths(
            output_dir,
            leaflet_rows,
            leaflets_per_leaf,
            zstd_level,
            1,
            2,
            RunSortOrder::Spot,
        )
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
        let perf_enabled = std::env::var("FLUREE_INDEX_BUILD_PERF")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            leaflet_encoder: LeafletEncoder::with_widths_and_order(
                zstd_level, p_width, dt_width, sort_order,
            ),
            output_dir,
            leaflet_rows,
            leaflets_per_leaf,
            dt_width,
            p_width,
            record_buf: Vec::with_capacity(leaflet_rows),
            history_buf: Vec::new(),
            r3_tmp: Vec::with_capacity(leaflet_rows),
            current_leaflets: Vec::with_capacity(leaflets_per_leaf),
            leaf_first_record: None,
            last_record: None,
            leaf_count: 0,
            leaf_infos: Vec::new(),
            perf: LeafWriterPerf::default(),
            perf_enabled,
            skip_region3: false,
        }
    }

    /// Enable or disable Region 3 (history journal) generation.
    ///
    /// When `true`, leaflets are encoded without Region 3 — skipping the
    /// per-leaflet allocation, radix sort, and zstd compression of history
    /// entries. Safe for append-only bulk import where all ops are asserts.
    pub fn set_skip_region3(&mut self, skip: bool) {
        self.skip_region3 = skip;
    }

    /// Push a single record (no history). Flushes leaflet/leaf as thresholds are hit.
    pub fn push_record(&mut self, record: RunRecord) -> io::Result<()> {
        self.push_record_with_history(record, &[])
    }

    /// Push a record for Region 1 along with its associated history entries
    /// for Region 3. The history entries are non-winning duplicates from the
    /// merge dedup — they represent intermediate asserts and retractions for
    /// the same fact identity.
    ///
    /// Record and history are pushed atomically (before the flush threshold
    /// check) so that history entries are always flushed with the correct
    /// leaflet.
    pub fn push_record_with_history(
        &mut self,
        record: RunRecord,
        history: &[RunRecord],
    ) -> io::Result<()> {
        if self.leaf_first_record.is_none() {
            self.leaf_first_record = Some(record);
        }
        self.last_record = Some(record);
        self.record_buf.push(record);

        if !self.skip_region3 {
            for h in history {
                self.history_buf.push(Region3Entry::from_run_record(h));
            }
        }

        if self.record_buf.len() >= self.leaflet_rows {
            self.flush_leaflet()?;
        }

        Ok(())
    }

    /// Write history entries (R3) for a retract-winner without a corresponding
    /// R1 row. Used during rebuild when the merge winner is a retraction (op=0):
    /// - The retract itself is always written to R3 (it's a real event the replay
    ///   journal must record for time-travel).
    /// - Any earlier asserts/retracts from dedup history are also written.
    ///
    /// Only meaningful when `skip_region3 == false`. Import (skip_region3 == true)
    /// never calls this.
    pub fn push_history_only(
        &mut self,
        winner: &RunRecord,
        history: &[RunRecord],
    ) -> io::Result<()> {
        debug_assert!(
            !self.skip_region3,
            "push_history_only should not be called when skip_region3 is true"
        );
        // The retract winner is always a real event.
        self.history_buf.push(Region3Entry::from_run_record(winner));
        for h in history {
            self.history_buf.push(Region3Entry::from_run_record(h));
        }
        Ok(())
    }

    /// Flush the current record buffer as a leaflet.
    fn flush_leaflet(&mut self) -> io::Result<()> {
        if self.record_buf.is_empty() {
            return Ok(());
        }

        let t0 = self.perf_enabled.then(std::time::Instant::now);
        let row_count = self.record_buf.len() as u32;
        let first_s_id = self.record_buf[0].s_id.as_u64();
        let first_p_id = self.record_buf[0].p_id;

        // Build R3 from externally-provided history entries (non-winning
        // duplicates from merge dedup). Only facts with multi-version history
        // contribute entries; single-version facts produce empty R3.
        let data = if self.skip_region3 || self.history_buf.is_empty() {
            self.leaflet_encoder.encode_leaflet(&self.record_buf)
        } else {
            let r3_t0 = self.perf_enabled.then(std::time::Instant::now);
            radix_sort_r3_abs_t_desc(&mut self.history_buf, &mut self.r3_tmp);
            if let Some(t) = r3_t0 {
                self.perf.region3_build_time += t.elapsed();
            }
            self.leaflet_encoder
                .encode_leaflet_with_r3(&self.record_buf, &self.history_buf)
        };
        self.record_buf.clear();
        self.history_buf.clear();
        if let Some(t) = t0 {
            self.perf.leaflet_encode_time += t.elapsed();
        }
        if self.perf_enabled {
            self.perf.leaflets_encoded += 1;
            self.perf.leaflet_records += row_count as u64;
        }

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

        let t0 = self.perf_enabled.then(std::time::Instant::now);
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

        // Stream leaf bytes to disk while hashing, then rename to content hash.
        //
        // This avoids building a large `leaf_bytes` buffer and copying leaflet
        // bytes into it, which is expensive at scale.
        let leaflet_count = self.current_leaflets.len();
        let dir_size = leaflet_count * LEAFLET_DIR_ENTRY;
        let header_size = LEAF_HEADER_FIXED + dir_size;

        // Temporary file name (same dir so rename is atomic).
        let tmp_path = self
            .output_dir
            .join(format!(".leaf_tmp_{:06}.fli", leaf_index));

        let mut hasher = Sha256::new();
        let file = fs::File::create(&tmp_path)?;
        let mut w = io::BufWriter::new(file);

        let mut write_hashed = |bytes: &[u8]| -> io::Result<()> {
            hasher.update(bytes);
            w.write_all(bytes)
        };

        // ---- Header ----
        write_hashed(&LEAF_MAGIC)?;
        write_hashed(&[LEAF_VERSION])?;
        write_hashed(&[leaflet_count as u8])?;
        write_hashed(&[self.dt_width])?;
        write_hashed(&[self.p_width])?;
        write_hashed(&total_rows.to_le_bytes())?;

        // First/last keys (28 bytes each)
        let mut key_buf = [0u8; SORT_KEY_BYTES];
        SortKey::from_record(&first_key).write_to(&mut key_buf);
        write_hashed(&key_buf)?;
        SortKey::from_record(&last_key).write_to(&mut key_buf);
        write_hashed(&key_buf)?;

        // ---- Leaflet directory ----
        let mut offset = header_size as u64;
        for l in &self.current_leaflets {
            write_hashed(&offset.to_le_bytes())?;
            write_hashed(&(l.data.len() as u32).to_le_bytes())?;
            write_hashed(&l.row_count.to_le_bytes())?;
            write_hashed(&l.first_s_id.to_le_bytes())?;
            write_hashed(&l.first_p_id.to_le_bytes())?;
            offset += l.data.len() as u64;
        }

        // ---- Leaflet data ----
        for l in &self.current_leaflets {
            write_hashed(&l.data)?;
        }

        w.flush()?;
        if self.perf_enabled {
            // Header+dir + all leaflet data
            let bytes_written = header_size as u64
                + self
                    .current_leaflets
                    .iter()
                    .map(|l| l.data.len() as u64)
                    .sum::<u64>();
            self.perf.leaf_bytes_written += bytes_written;
        }

        let digest_hex = hex::encode(hasher.finalize());
        let leaf_cid = fluree_db_core::ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF,
            &digest_hex,
        )
        .expect("valid SHA-256 hex digest");
        let leaf_path = self.output_dir.join(leaf_cid.to_string());

        match fs::rename(&tmp_path, &leaf_path) {
            Ok(()) => {}
            Err(_e) if leaf_path.exists() => {
                // Content-addressed: if destination exists, it should be identical.
                let _ = fs::remove_file(&tmp_path);
            }
            Err(e) => return Err(e),
        }
        if let Some(t) = t0 {
            self.perf.leaf_flush_time += t.elapsed();
            self.perf.leaves_flushed += 1;
        }

        tracing::debug!(
            leaf = leaf_index,
            leaflets = self.current_leaflets.len(),
            rows = total_rows,
            cid = %leaf_cid,
            "leaf file written"
        );

        self.leaf_infos.push(LeafInfo {
            leaf_cid,
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

    /// Get perf counters accumulated so far.
    pub fn perf(&self) -> Option<&LeafWriterPerf> {
        self.perf_enabled.then_some(&self.perf)
    }

    /// Number of leaf files written so far.
    pub fn leaf_count(&self) -> u32 {
        self.leaf_count
    }
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
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;

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

        // Read back and verify header (path derived from CID)
        let leaf_path = dir.join(infos[0].leaf_cid.to_string());
        let data = std::fs::read(&leaf_path).unwrap();
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
        for info in &infos[..5] {
            assert_eq!(info.total_rows, 10);
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

        // Read leaf and decode all leaflets (path derived from CID)
        let leaf_path = dir.join(infos[0].leaf_cid.to_string());
        let data = std::fs::read(&leaf_path).unwrap();
        let header = read_leaf_header(&data).unwrap();

        assert_eq!(header.leaflet_count, 3);
        let mut all_s_ids = Vec::new();
        for entry in &header.leaflet_dir {
            let leaflet_data =
                &data[entry.offset as usize..entry.offset as usize + entry.compressed_len as usize];
            let decoded = super::super::leaflet::decode_leaflet(
                leaflet_data,
                header.p_width,
                header.dt_width,
                crate::run_index::run_record::RunSortOrder::Spot,
            )
            .unwrap();
            all_s_ids.extend_from_slice(&decoded.s_ids);
        }

        assert_eq!(all_s_ids, (0..9u64).collect::<Vec<_>>());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
