//! Buffered, forward-only reader for run files.
//!
//! Reads 8192 records at a time (~320 KB per stream) to avoid loading
//! entire 1 GB run files into memory. Suitable for k-way merge where
//! 21 concurrent streams would otherwise require 21 GB.

use super::run_file::{deserialize_lang_dict, RunFileHeader, RUN_HEADER_LEN};
use super::run_record::RunRecord;
use super::global_dict::LanguageTagDict;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

/// Number of records to buffer per read. 8192 × 40 bytes = 320 KB.
const BUFFER_SIZE: usize = 8192;

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
        let to_read = (self.remaining as usize).min(BUFFER_SIZE);
        if to_read == 0 {
            self.buffer.clear();
            self.buf_pos = 0;
            return Ok(());
        }

        // Read raw bytes
        let byte_count = to_read * 40;
        let mut raw = vec![0u8; byte_count];
        self.file.read_exact(&mut raw)?;

        // Decode records
        self.buffer.clear();
        self.buffer.reserve(to_read);
        for i in 0..to_read {
            let offset = i * 40;
            let buf: &[u8; 40] = raw[offset..offset + 40].try_into().unwrap();
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

        self.remaining -= to_read as u64;
        self.buf_pos = 0;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::{RunSortOrder, cmp_spot};
    use crate::run_index::global_dict::dt_ids;
    use fluree_db_core::value_id::ValueId;
    use std::cmp::Ordering;

    fn make_record(s_id: u32, p_id: u32, val: i64, t: i64) -> RunRecord {
        RunRecord::new(
            0, s_id, p_id,
            ValueId::num_int(val).unwrap(),
            t, true, dt_ids::INTEGER, 0, None,
        )
    }

    #[test]
    fn test_streaming_reader_basic() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let lang_dict = LanguageTagDict::new();
        let mut records: Vec<RunRecord> = (0..100)
            .map(|i| make_record(i, 1, i as i64, 1))
            .collect();
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
        let mut records: Vec<RunRecord> = (0..n as u32)
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
            RunRecord::new(0, 1, 1, ValueId::lex_id(0), 1, true, dt_ids::LANG_STRING, 1, None),
            RunRecord::new(0, 1, 2, ValueId::lex_id(1), 1, true, dt_ids::LANG_STRING, 2, None),
            RunRecord::new(0, 2, 1, ValueId::lex_id(2), 1, true, dt_ids::STRING, 0, None),
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
