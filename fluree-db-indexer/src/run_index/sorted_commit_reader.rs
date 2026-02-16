//! Buffered, forward-only reader for sorted commit files (`.fsc`).
//!
//! Reads 36-byte spool-wire-format records from sorted commit files written
//! by [`sort_remap_and_write_sorted_commit()`]. These files contain records
//! sorted by `(g_id, SPOT)` with chunk-local sorted-position IDs.
//!
//! During `fill_buffer()`, each record's subject and string IDs are remapped
//! from sorted-position chunk-local IDs to global IDs using the provided
//! remap tables (produced by the vocabulary merge in Phase B). This amortizes
//! the remap cost across buffer refills.
//!
//! Implements [`MergeSource`] for use with [`KWayMerge`] — enabling direct
//! k-way merge of sorted commit files into SPOT index leaflets (Phase C).
//!
//! [`sort_remap_and_write_sorted_commit()`]: super::spool::sort_remap_and_write_sorted_commit
//! [`MergeSource`]: super::merge::MergeSource
//! [`KWayMerge`]: super::merge::KWayMerge

use super::merge::MergeSource;
use super::run_record::{RunRecord, SPOOL_RECORD_WIRE_SIZE};
use super::spool::{remap_record, StringRemap, SubjectRemap};
use std::io::{self, BufReader, Read};
use std::path::Path;

/// Number of records to buffer per read. 8192 × 36 bytes ≈ 288 KB.
const BUFFER_SIZE: usize = 8192;

/// Spool file flags (must match `spool.rs`).
const SPOOL_FLAG_ZSTD: u8 = 1 << 0;

/// Buffered, forward-only reader for sorted commit files with on-the-fly remap.
///
/// Reads 36-byte spool-wire-format records in batches and applies
/// subject + string + language remap during buffer refills.
pub struct StreamingSortedCommitReader {
    file: BufReaderVariant,
    record_count: u64,
    /// Read buffer (post-remap records).
    buffer: Vec<RunRecord>,
    /// Raw byte buffer reused across reads.
    raw_buf: Vec<u8>,
    /// Current position within buffer.
    buf_pos: usize,
    /// Number of records still in the file (not yet read into buffer).
    remaining: u64,
    /// Subject remap: sorted-local → global sid64.
    subject_remap: Box<dyn SubjectRemap>,
    /// String remap: sorted-local → global string ID.
    string_remap: Box<dyn StringRemap>,
    /// Language tag remap: chunk-local lang_id → global lang_id.
    /// `remap[0] = 0` (sentinel for "no tag"). Empty means no remap.
    lang_remap: Vec<u16>,
}

enum BufReaderVariant {
    Raw(BufReader<std::fs::File>),
    Zstd(zstd::stream::read::Decoder<'static, BufReader<std::fs::File>>),
}

impl Read for BufReaderVariant {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            BufReaderVariant::Raw(r) => r.read(buf),
            BufReaderVariant::Zstd(r) => r.read(buf),
        }
    }
}

impl StreamingSortedCommitReader {
    /// Open a sorted commit file for streaming with on-the-fly remap.
    ///
    /// The file must have been written by [`sort_remap_and_write_sorted_commit()`]
    /// using the spool wire format (header + 36-byte records).
    ///
    /// `subject_remap` maps sorted-local subject IDs → global sid64.
    /// `string_remap` maps sorted-local string IDs → global string IDs.
    /// `lang_remap` maps chunk-local lang_id → global lang_id (empty = no remap).
    pub fn open(
        path: &Path,
        subject_remap: Box<dyn SubjectRemap>,
        string_remap: Box<dyn StringRemap>,
        lang_remap: Vec<u16>,
    ) -> io::Result<Self> {
        let mut file = std::fs::File::open(path)?;

        // Read spool header (32 bytes)
        let mut header_buf = [0u8; super::spool::SPOOL_HEADER_LEN];
        file.read_exact(&mut header_buf)?;

        // Validate magic
        if header_buf[0..4] != *b"FSP2" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sorted commit: invalid magic bytes",
            ));
        }
        let version = header_buf[4];
        if version != super::spool::SPOOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("sorted commit: unsupported version {}", version),
            ));
        }
        let flags = header_buf[5];
        let record_count = u64::from_le_bytes(header_buf[12..20].try_into().unwrap());

        // File cursor is now at byte 32 (start of records).
        let file = if (flags & SPOOL_FLAG_ZSTD) != 0 {
            // Decoder::new() internally wraps in a BufReader.
            let decoder = zstd::stream::read::Decoder::new(file)?;
            BufReaderVariant::Zstd(decoder)
        } else {
            BufReaderVariant::Raw(BufReader::new(file))
        };

        let mut reader = Self {
            file,
            record_count,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            raw_buf: Vec::with_capacity(BUFFER_SIZE * SPOOL_RECORD_WIRE_SIZE),
            buf_pos: 0,
            remaining: record_count,
            subject_remap,
            string_remap,
            lang_remap,
        };

        // Fill initial buffer
        reader.fill_buffer()?;

        Ok(reader)
    }

    /// Number of records in this sorted commit file (from the header).
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Fill the buffer with the next batch of records from disk, applying remap.
    fn fill_buffer(&mut self) -> io::Result<()> {
        let to_read = (self.remaining as usize).min(BUFFER_SIZE);
        if to_read == 0 {
            self.buffer.clear();
            self.buf_pos = 0;
            return Ok(());
        }

        // Read raw bytes
        let byte_count = to_read * SPOOL_RECORD_WIRE_SIZE;
        self.raw_buf.resize(byte_count, 0u8);
        self.file.read_exact(&mut self.raw_buf)?;

        // Decode records and apply remap
        self.buffer.clear();
        self.buffer.reserve(to_read);
        for i in 0..to_read {
            let offset = i * SPOOL_RECORD_WIRE_SIZE;
            let buf: &[u8; SPOOL_RECORD_WIRE_SIZE] = self.raw_buf
                [offset..offset + SPOOL_RECORD_WIRE_SIZE]
                .try_into()
                .unwrap();
            let mut rec = RunRecord::read_spool_le(buf);

            // Apply subject + string remap (sorted-local → global)
            remap_record(
                &mut rec,
                self.subject_remap.as_ref(),
                self.string_remap.as_ref(),
            )?;

            // Apply language tag remap (chunk-local → global)
            if !self.lang_remap.is_empty() && rec.lang_id != 0 {
                if let Some(&global_id) = self.lang_remap.get(rec.lang_id as usize) {
                    rec.lang_id = global_id;
                }
            }

            self.buffer.push(rec);
        }

        self.remaining -= to_read as u64;
        self.buf_pos = 0;

        Ok(())
    }
}

impl MergeSource for StreamingSortedCommitReader {
    #[inline]
    fn peek(&self) -> Option<&RunRecord> {
        if self.buf_pos < self.buffer.len() {
            Some(&self.buffer[self.buf_pos])
        } else {
            None
        }
    }

    fn advance(&mut self) -> io::Result<()> {
        self.buf_pos += 1;
        if self.buf_pos >= self.buffer.len() && self.remaining > 0 {
            self.fill_buffer()?;
        }
        Ok(())
    }

    fn is_exhausted(&self) -> bool {
        self.buf_pos >= self.buffer.len() && self.remaining == 0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::chunk_dict::{ChunkStringDict, ChunkSubjectDict};
    use crate::run_index::run_record::LIST_INDEX_NONE;
    use crate::run_index::spool::sort_remap_and_write_sorted_commit;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;

    fn make_record(s_id: u64, p_id: u32, o_kind: ObjKind, o_key: u64, t: u32) -> RunRecord {
        RunRecord {
            g_id: 0,
            s_id: SubjectId::from_u64(s_id),
            p_id,
            dt: DatatypeDictId::STRING.as_u16(),
            o_kind: o_kind.as_u8(),
            op: 1,
            o_key,
            t,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        }
    }

    #[test]
    fn test_streaming_sorted_commit_reader_basic() {
        let dir = std::env::temp_dir().join("fluree_test_scr_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create chunk dicts.
        // Subjects: insertion 0="ns10:B", insertion 1="ns10:A"
        // Canonical sort: A(1) < B(0) → sorted remap: [1, 0]
        let mut subj_dict = ChunkSubjectDict::new();
        subj_dict.get_or_insert(10, b"B"); // insertion 0 → sorted 1
        subj_dict.get_or_insert(10, b"A"); // insertion 1 → sorted 0

        // Strings: insertion 0="xyz", insertion 1="abc"
        // Lex sort: abc(1) < xyz(0) → sorted remap: [1, 0]
        let mut str_dict = ChunkStringDict::new();
        str_dict.get_or_insert(b"xyz"); // insertion 0 → sorted 1
        str_dict.get_or_insert(b"abc"); // insertion 1 → sorted 0

        // Records with insertion-order IDs.
        let records = vec![
            make_record(0, 10, ObjKind::LEX_ID, ObjKey::encode_u32_id(0).as_u64(), 1),
            make_record(1, 20, ObjKind::NUM_INT, ObjKey::encode_i64(42).as_u64(), 1),
        ];

        let subj_vocab = dir.join("subjects.voc");
        let str_vocab = dir.join("strings.voc");
        let commit_path = dir.join("commit_0.fsc");

        let info = sort_remap_and_write_sorted_commit(
            records,
            subj_dict,
            str_dict,
            &subj_vocab,
            &str_vocab,
            &commit_path,
            0,
            None,
            None,
        )
        .unwrap();
        assert_eq!(info.record_count, 2);

        // After sort_remap_and_write_sorted_commit:
        //   rec0: s_id becomes sorted 1 (B), LEX_ID obj becomes sorted 1 (xyz)
        //   rec1: s_id becomes sorted 0 (A), NUM_INT obj unchanged
        // SPOT sort: A(s=0) < B(s=1) → rec1 first

        // Now create global remap tables (sorted-local → global).
        // Global subject IDs: sorted 0 (A) → 1000, sorted 1 (B) → 2000
        let global_subj_remap: Vec<u64> = vec![1000, 2000];
        // Global string IDs: sorted 0 (abc) → 50, sorted 1 (xyz) → 99
        let global_str_remap: Vec<u32> = vec![50, 99];

        let mut reader = StreamingSortedCommitReader::open(
            &commit_path,
            Box::new(global_subj_remap),
            Box::new(global_str_remap),
            vec![],
        )
        .unwrap();
        assert_eq!(reader.record_count(), 2);

        // First record: A (sorted 0 → global 1000), p_id=20, NUM_INT
        let r0 = reader.peek().unwrap();
        assert_eq!(r0.s_id, SubjectId::from_u64(1000));
        assert_eq!(r0.p_id, 20);
        assert_eq!(r0.o_kind, ObjKind::NUM_INT.as_u8());
        assert_eq!(r0.o_key, ObjKey::encode_i64(42).as_u64()); // unchanged
        reader.advance().unwrap();

        // Second record: B (sorted 1 → global 2000), p_id=10, LEX_ID
        let r1 = reader.peek().unwrap();
        assert_eq!(r1.s_id, SubjectId::from_u64(2000));
        assert_eq!(r1.p_id, 10);
        assert_eq!(r1.o_kind, ObjKind::LEX_ID.as_u8());
        assert_eq!(ObjKey::from_u64(r1.o_key).decode_u32_id(), 99); // xyz → global 99
        reader.advance().unwrap();

        assert!(reader.is_exhausted());
        assert!(reader.peek().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_streaming_sorted_commit_reader_ref_remap() {
        let dir = std::env::temp_dir().join("fluree_test_scr_ref");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Subjects: insertion 0="ns10:X", insertion 1="ns10:Y"
        // Canonical sort: X(0) < Y(1) → sorted remap: identity [0, 1]
        let mut subj_dict = ChunkSubjectDict::new();
        subj_dict.get_or_insert(10, b"X");
        subj_dict.get_or_insert(10, b"Y");

        let str_dict = ChunkStringDict::new();

        // Record: s_id=0 (X), REF_ID to subject 1 (Y)
        let records = vec![make_record(0, 10, ObjKind::REF_ID, 1, 1)];

        let subj_vocab = dir.join("subjects.voc");
        let str_vocab = dir.join("strings.voc");
        let commit_path = dir.join("commit_0.fsc");

        sort_remap_and_write_sorted_commit(
            records,
            subj_dict,
            str_dict,
            &subj_vocab,
            &str_vocab,
            &commit_path,
            0,
            None,
            None,
        )
        .unwrap();

        // Global remap: sorted 0 (X) → 500, sorted 1 (Y) → 600
        let global_subj_remap: Vec<u64> = vec![500, 600];
        let global_str_remap: Vec<u32> = vec![];

        let mut reader = StreamingSortedCommitReader::open(
            &commit_path,
            Box::new(global_subj_remap),
            Box::new(global_str_remap),
            vec![],
        )
        .unwrap();

        let rec = reader.peek().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(500)); // X → 500
        assert_eq!(rec.o_kind, ObjKind::REF_ID.as_u8());
        assert_eq!(rec.o_key, 600); // Y → 600
        reader.advance().unwrap();

        assert!(reader.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_streaming_sorted_commit_reader_with_kway_merge() {
        use crate::run_index::merge::KWayMerge;
        use crate::run_index::run_record::cmp_g_spot;

        let dir = std::env::temp_dir().join("fluree_test_scr_merge");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Chunk 0: subjects A, C
        let mut subj0 = ChunkSubjectDict::new();
        subj0.get_or_insert(10, b"A"); // sorted 0
        subj0.get_or_insert(10, b"C"); // sorted 1
        let str0 = ChunkStringDict::new();
        let recs0 = vec![
            make_record(0, 10, ObjKind::NUM_INT, 1, 1), // A
            make_record(1, 10, ObjKind::NUM_INT, 3, 1), // C
        ];
        sort_remap_and_write_sorted_commit(
            recs0,
            subj0,
            str0,
            &dir.join("subj0.voc"),
            &dir.join("str0.voc"),
            &dir.join("commit_0.fsc"),
            0,
            None,
            None,
        )
        .unwrap();

        // Chunk 1: subjects B, D
        let mut subj1 = ChunkSubjectDict::new();
        subj1.get_or_insert(10, b"B"); // sorted 0
        subj1.get_or_insert(10, b"D"); // sorted 1
        let str1 = ChunkStringDict::new();
        let recs1 = vec![
            make_record(0, 10, ObjKind::NUM_INT, 2, 2), // B
            make_record(1, 10, ObjKind::NUM_INT, 4, 2), // D
        ];
        sort_remap_and_write_sorted_commit(
            recs1,
            subj1,
            str1,
            &dir.join("subj1.voc"),
            &dir.join("str1.voc"),
            &dir.join("commit_1.fsc"),
            1,
            None,
            None,
        )
        .unwrap();

        // Global remaps (monotone — preserves SPOT order):
        // Chunk 0: sorted 0 (A) → 100, sorted 1 (C) → 300
        // Chunk 1: sorted 0 (B) → 200, sorted 1 (D) → 400
        let r0 = StreamingSortedCommitReader::open(
            &dir.join("commit_0.fsc"),
            Box::new(vec![100u64, 300]),
            Box::new(Vec::<u32>::new()),
            vec![],
        )
        .unwrap();
        let r1 = StreamingSortedCommitReader::open(
            &dir.join("commit_1.fsc"),
            Box::new(vec![200u64, 400]),
            Box::new(Vec::<u32>::new()),
            vec![],
        )
        .unwrap();

        let mut merge = KWayMerge::new(vec![r0, r1], cmp_g_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_record().unwrap() {
            results.push(rec);
        }

        assert_eq!(results.len(), 4);
        // Global SPOT order: A(100) < B(200) < C(300) < D(400)
        assert_eq!(results[0].s_id, SubjectId::from_u64(100));
        assert_eq!(results[1].s_id, SubjectId::from_u64(200));
        assert_eq!(results[2].s_id, SubjectId::from_u64(300));
        assert_eq!(results[3].s_id, SubjectId::from_u64(400));

        assert!(merge.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
