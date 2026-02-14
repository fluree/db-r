//! Buffered, forward-only reader for run files.
//!
//! Reads 8192 records at a time (~320 KB per stream) to avoid loading
//! entire 1 GB run files into memory. Suitable for k-way merge where
//! 21 concurrent streams would otherwise require 21 GB.

use super::global_dict::LanguageTagDict;
use super::run_file::{deserialize_lang_dict, RunFileHeader, RUN_HEADER_LEN};
use super::run_record::{RunRecord, RECORD_WIRE_SIZE};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Number of records to buffer per read. 8192 × 34 bytes ≈ 272 KB.
const BUFFER_SIZE: usize = 8192;

/// Run file flags (must match `run_file.rs`).
const RUN_FLAG_ZSTD_BLOCKS: u8 = 1 << 0;

static PERF_READ_BYTES: AtomicU64 = AtomicU64::new(0);
static PERF_FILL_CALLS: AtomicU64 = AtomicU64::new(0);
static PERF_FILL_NS: AtomicU64 = AtomicU64::new(0);

/// Buffered, forward-only reader for run files.
///
/// Reads records in batches of [`BUFFER_SIZE`] and optionally remaps
/// `lang_id` values using a per-run remap table (built during lang
/// tag reconciliation).
pub struct StreamingRunReader {
    file: BufReader<std::fs::File>,
    header: RunFileHeader,
    /// Per-run language tag dict (kept for diagnostics).
    #[allow(dead_code)]
    lang_dict: LanguageTagDict,
    /// Remap table: `remap[local_lang_id] = global_lang_id`.
    /// Empty means no remapping needed.
    lang_remap: Vec<u16>,
    /// Read buffer.
    buffer: Vec<RunRecord>,
    /// Raw byte buffer reused across reads.
    raw_buf: Vec<u8>,
    /// Temporary buffer for compressed blocks.
    zstd_buf: Vec<u8>,
    /// Decompressed bytes of the current block (compressed run files).
    block_raw: Vec<u8>,
    /// Current position within `block_raw` (in bytes).
    block_pos: usize,
    /// Current position within buffer.
    buf_pos: usize,
    /// Number of records still in the file (not yet read into buffer).
    remaining: u64,
}

impl StreamingRunReader {
    /// Open a run file for streaming.
    ///
    /// `lang_remap` maps per-run lang_id → global lang_id. Pass empty
    /// vec if no remapping is needed. Must have `remap[0] = 0`.
    pub fn open(path: &Path, lang_remap: Vec<u16>) -> io::Result<Self> {
        let mut file = BufReader::new(std::fs::File::open(path)?);

        // Read header
        let mut header_buf = [0u8; RUN_HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = RunFileHeader::read_from(&header_buf)?;

        // Read lang dict
        let ld_len = header.lang_dict_len as usize;
        let mut ld_buf = vec![0u8; ld_len];
        file.seek(SeekFrom::Start(header.lang_dict_offset))?;
        file.read_exact(&mut ld_buf)?;
        let lang_dict = deserialize_lang_dict(&ld_buf)?;

        // Position at start of records
        file.seek(SeekFrom::Start(header.records_offset))?;

        let mut reader = Self {
            file,
            header,
            lang_dict,
            lang_remap,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            raw_buf: Vec::with_capacity(BUFFER_SIZE * RECORD_WIRE_SIZE),
            zstd_buf: Vec::new(),
            block_raw: Vec::new(),
            block_pos: 0,
            buf_pos: 0,
            remaining: 0,
        };
        reader.remaining = reader.header.record_count;

        // Fill initial buffer
        reader.fill_buffer()?;

        Ok(reader)
    }

    /// Peek at the current record without advancing.
    /// Returns `None` if exhausted.
    #[inline]
    pub fn peek(&self) -> Option<&RunRecord> {
        if self.buf_pos < self.buffer.len() {
            Some(&self.buffer[self.buf_pos])
        } else {
            None
        }
    }

    /// Advance to the next record. Refills buffer from disk if needed.
    pub fn advance(&mut self) -> io::Result<()> {
        self.buf_pos += 1;
        if self.buf_pos >= self.buffer.len() && self.remaining > 0 {
            self.fill_buffer()?;
        }
        Ok(())
    }

    /// True when all records have been consumed.
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.buf_pos >= self.buffer.len() && self.remaining == 0
    }

    /// Number of records in this run file (from the header).
    pub fn record_count(&self) -> u64 {
        self.header.record_count
    }

    /// The sort order of this run file.
    pub fn sort_order(&self) -> super::run_record::RunSortOrder {
        self.header.sort_order
    }

    /// Fill the buffer with the next batch of records from disk.
    fn fill_buffer(&mut self) -> io::Result<()> {
        let perf_enabled = std::env::var("FLUREE_INDEX_BUILD_PERF")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let t0 = perf_enabled.then(Instant::now);

        let to_read = (self.remaining as usize).min(BUFFER_SIZE);
        if to_read == 0 {
            self.buffer.clear();
            self.buf_pos = 0;
            return Ok(());
        }

        if (self.header.flags & RUN_FLAG_ZSTD_BLOCKS) == 0 {
            // Read raw bytes
            let byte_count = to_read * RECORD_WIRE_SIZE;
            self.raw_buf.resize(byte_count, 0u8);
            self.file.read_exact(&mut self.raw_buf)?;

            // Decode records
            self.buffer.clear();
            self.buffer.reserve(to_read);
            for i in 0..to_read {
                let offset = i * RECORD_WIRE_SIZE;
                let buf: &[u8; RECORD_WIRE_SIZE] = self.raw_buf[offset..offset + RECORD_WIRE_SIZE]
                    .try_into()
                    .unwrap();
                let mut rec = RunRecord::read_le(buf);

                // Apply lang_id remapping
                if !self.lang_remap.is_empty() && rec.lang_id != 0 {
                    if let Some(&global_id) = self.lang_remap.get(rec.lang_id as usize) {
                        rec.lang_id = global_id;
                    }
                    // If lang_id exceeds remap table, leave as-is (shouldn't happen)
                }

                self.buffer.push(rec);
            }
        } else {
            // Compressed block stream. Maintain an internal decompressed block buffer.
            self.buffer.clear();
            self.buffer.reserve(to_read);

            while self.buffer.len() < to_read {
                // If current block is exhausted, read next block.
                if self.block_pos >= self.block_raw.len() {
                    self.block_raw.clear();
                    self.block_pos = 0;

                    let mut hdr = [0u8; 12];
                    self.file.read_exact(&mut hdr)?;
                    let n_records = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
                    let raw_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
                    let z_len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;

                    if raw_len != n_records * RECORD_WIRE_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "run file: invalid compressed block raw_len",
                        ));
                    }

                    self.zstd_buf.resize(z_len, 0u8);
                    self.file.read_exact(&mut self.zstd_buf)?;
                    self.block_raw = zstd::bulk::decompress(&self.zstd_buf, raw_len)?;
                    if self.block_raw.len() != raw_len {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "run file: decompressed length mismatch",
                        ));
                    }
                }

                // Decode as many records as we can from the current block.
                while self.buffer.len() < to_read
                    && self.block_pos + RECORD_WIRE_SIZE <= self.block_raw.len()
                {
                    let buf: &[u8; RECORD_WIRE_SIZE] = self.block_raw
                        [self.block_pos..self.block_pos + RECORD_WIRE_SIZE]
                        .try_into()
                        .unwrap();
                    self.block_pos += RECORD_WIRE_SIZE;
                    let mut rec = RunRecord::read_le(buf);

                    // Apply lang_id remapping
                    if !self.lang_remap.is_empty() && rec.lang_id != 0 {
                        if let Some(&global_id) = self.lang_remap.get(rec.lang_id as usize) {
                            rec.lang_id = global_id;
                        }
                    }
                    self.buffer.push(rec);
                }
            }
        }

        self.remaining -= to_read as u64;
        self.buf_pos = 0;

        if let Some(t) = t0 {
            PERF_FILL_CALLS.fetch_add(1, Ordering::Relaxed);
            // We don't track compressed bytes here; this is best-effort perf logging.
            let approx_bytes = (to_read * RECORD_WIRE_SIZE) as u64;
            PERF_READ_BYTES.fetch_add(approx_bytes, Ordering::Relaxed);
            PERF_FILL_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Drop for StreamingRunReader {
    fn drop(&mut self) {
        let perf_enabled = std::env::var("FLUREE_INDEX_BUILD_PERF")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !perf_enabled {
            return;
        }
        // Aggregate across all reader instances; only emit once when the last one drops
        // is non-trivial to detect, so we emit at drop with totals (dup logs are OK).
        let calls = PERF_FILL_CALLS.load(std::sync::atomic::Ordering::Relaxed);
        let bytes = PERF_READ_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let ns = PERF_FILL_NS.load(std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            fill_calls = calls,
            read_mb = (bytes as f64) / (1024.0 * 1024.0),
            fill_s = (ns as f64) / 1e9,
            "streaming run reader perf (aggregate)"
        );
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::{cmp_spot, RunSortOrder};
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;
    use std::cmp::Ordering;

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
    fn test_streaming_reader_basic() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let lang_dict = LanguageTagDict::new();
        let mut records: Vec<RunRecord> =
            (0..100).map(|i| make_record(i, 1, i as i64, 1)).collect();
        records.sort_unstable_by(cmp_spot);

        write_run_file(&path, &records, &lang_dict, RunSortOrder::Spot, 1, 1).unwrap();

        let mut reader = StreamingRunReader::open(&path, vec![]).unwrap();
        assert_eq!(reader.record_count(), 100);

        let mut count = 0;
        let mut prev: Option<RunRecord> = None;
        while let Some(rec) = reader.peek() {
            if let Some(p) = prev {
                assert_ne!(cmp_spot(&p, rec), Ordering::Greater, "records not sorted");
            }
            prev = Some(*rec);
            count += 1;
            reader.advance().unwrap();
        }

        assert_eq!(count, 100);
        assert!(reader.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_streaming_reader_large() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_large");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        // More records than BUFFER_SIZE to test buffer refill
        let n = BUFFER_SIZE * 2 + 500;
        let lang_dict = LanguageTagDict::new();
        let mut records: Vec<RunRecord> = (0..n as u64)
            .map(|i| make_record(i, 1, i as i64, 1))
            .collect();
        records.sort_unstable_by(cmp_spot);

        write_run_file(&path, &records, &lang_dict, RunSortOrder::Spot, 1, 1).unwrap();

        let mut reader = StreamingRunReader::open(&path, vec![]).unwrap();
        let mut count = 0u64;
        while reader.peek().is_some() {
            count += 1;
            reader.advance().unwrap();
        }

        assert_eq!(count, n as u64);
        assert!(reader.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_streaming_reader_lang_remap() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_lang");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let mut lang_dict = LanguageTagDict::new();
        lang_dict.get_or_insert(Some("en")); // local id 1
        lang_dict.get_or_insert(Some("fr")); // local id 2

        let records = vec![
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                1,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(0),
                1,
                true,
                DatatypeDictId::LANG_STRING.as_u16(),
                1,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(1),
                1,
                true,
                DatatypeDictId::LANG_STRING.as_u16(),
                2,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                1,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(2),
                1,
                true,
                DatatypeDictId::STRING.as_u16(),
                0,
                None,
            ),
        ];

        write_run_file(&path, &records, &lang_dict, RunSortOrder::Spot, 1, 1).unwrap();

        // Remap: local 0→0 (sentinel), local 1→5, local 2→3
        let remap = vec![0u16, 5, 3];
        let mut reader = StreamingRunReader::open(&path, remap).unwrap();

        let r0 = *reader.peek().unwrap();
        assert_eq!(r0.lang_id, 5); // was 1, remapped to 5
        reader.advance().unwrap();

        let r1 = *reader.peek().unwrap();
        assert_eq!(r1.lang_id, 3); // was 2, remapped to 3
        reader.advance().unwrap();

        let r2 = *reader.peek().unwrap();
        assert_eq!(r2.lang_id, 0); // was 0, stays 0 (no remap for sentinel)
        reader.advance().unwrap();

        assert!(reader.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_streaming_reader_empty() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.frn");

        let lang_dict = LanguageTagDict::new();
        write_run_file(&path, &[], &lang_dict, RunSortOrder::Spot, 0, 0).unwrap();

        let reader = StreamingRunReader::open(&path, vec![]).unwrap();
        assert!(reader.is_exhausted());
        assert!(reader.peek().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
