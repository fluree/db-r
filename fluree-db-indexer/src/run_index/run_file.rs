//! Run file binary format: header + language dictionary + records.
//!
//! ```text
//! [Header: 64 bytes]
//!   magic: "FRN1" (4B), version: u8, sort_order: u8, flags: u8, _pad: u8
//!   record_count: u64
//!   lang_dict_offset: u64
//!   lang_dict_len: u64
//!   records_offset: u64
//!   min_t: u32, max_t: u32
//!   _reserved: [u8; 16]
//! [Language tag dictionary]
//!   count: u16
//!   entries: [len: u8, utf8_bytes]*
//! [Records: record_count × RECORD_WIRE_SIZE bytes]
//! ```

use super::global_dict::LanguageTagDict;
use super::run_record::{RunRecord, RunSortOrder, RECORD_WIRE_SIZE};
use std::io::{self, Write};
use std::path::Path;

#[cfg(target_os = "macos")]
fn try_disable_os_cache(file: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    // Best-effort: if this fails, fall back to normal cached I/O.
    // Disabling cache helps prevent huge RSS spikes on large sequential writes.
    unsafe {
        let fd = file.as_raw_fd();
        let _ = libc::fcntl(fd, libc::F_NOCACHE, 1);
    }
}

#[cfg(not(target_os = "macos"))]
fn try_disable_os_cache(_file: &std::fs::File) {}

/// Magic bytes for a run file.
pub const RUN_MAGIC: [u8; 4] = *b"FRN1";

/// Current run file format version.
///
/// Version history:
/// - 1: header + lang dict + raw record stream (44-byte records)
/// - 2: header + lang dict + (optional) zstd-compressed record blocks (44-byte records)
/// - 3: 34-byte records (g_id removed from wire; t→u32, i→u32)
pub const RUN_VERSION: u8 = 3;

/// Run file flags.
const RUN_FLAG_ZSTD_BLOCKS: u8 = 1 << 0;

/// Header size in bytes.
pub const RUN_HEADER_LEN: usize = 64;

/// Run file header.
#[derive(Debug, Clone)]
pub struct RunFileHeader {
    pub version: u8,
    pub sort_order: RunSortOrder,
    pub flags: u8,
    pub record_count: u64,
    pub lang_dict_offset: u64,
    pub lang_dict_len: u64,
    pub records_offset: u64,
    pub min_t: u32,
    pub max_t: u32,
}

impl RunFileHeader {
    /// Write the header to the first 64 bytes of `buf`.
    ///
    /// `min_t`/`max_t` are stored as `u32` at offsets 40..44 and 44..48 (v3+).
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RUN_HEADER_LEN);
        buf[0..4].copy_from_slice(&RUN_MAGIC);
        buf[4] = self.version;
        buf[5] = self.sort_order as u8;
        buf[6] = self.flags;
        buf[7] = 0; // pad
        buf[8..16].copy_from_slice(&self.record_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.lang_dict_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.lang_dict_len.to_le_bytes());
        buf[32..40].copy_from_slice(&self.records_offset.to_le_bytes());
        buf[40..44].copy_from_slice(&self.min_t.to_le_bytes());
        buf[44..48].copy_from_slice(&self.max_t.to_le_bytes());
        buf[48..64].fill(0); // reserved
    }

    /// Read the header from the first 64 bytes of `buf`.
    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < RUN_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "run file header too small: {} < {}",
                    buf.len(),
                    RUN_HEADER_LEN
                ),
            ));
        }
        if buf[0..4] != RUN_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: invalid magic bytes",
            ));
        }
        let version = buf[4];
        if !(1..=3).contains(&version) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("run file: unsupported version {}", version),
            ));
        }
        let sort_order = RunSortOrder::from_u8(buf[5]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("run file: unknown sort order {}", buf[5]),
            )
        })?;

        // v1/v2 stored min_t/max_t as i64 at [40..56]; v3 stores as u32 at [40..48].
        let (min_t, max_t) = if version <= 2 {
            let min = i64::from_le_bytes(buf[40..48].try_into().unwrap());
            let max = i64::from_le_bytes(buf[48..56].try_into().unwrap());
            (min as u32, max as u32)
        } else {
            let min = u32::from_le_bytes(buf[40..44].try_into().unwrap());
            let max = u32::from_le_bytes(buf[44..48].try_into().unwrap());
            (min, max)
        };

        Ok(Self {
            version,
            sort_order,
            flags: buf[6],
            record_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            lang_dict_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            lang_dict_len: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            records_offset: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            min_t,
            max_t,
        })
    }
}

// ============================================================================
// Language dictionary serialization
// ============================================================================

/// Serialize a LanguageTagDict to bytes.
///
/// Format: `count: u16` then `[len: u8, utf8_bytes]` for each entry.
pub fn serialize_lang_dict(dict: &LanguageTagDict) -> Vec<u8> {
    let count = dict.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&count.to_le_bytes());
    for (_id, tag) in dict.iter() {
        let tag_bytes = tag.as_bytes();
        debug_assert!(tag_bytes.len() <= 255, "language tag too long");
        buf.push(tag_bytes.len() as u8);
        buf.extend_from_slice(tag_bytes);
    }
    buf
}

/// Deserialize a LanguageTagDict from bytes.
pub fn deserialize_lang_dict(data: &[u8]) -> io::Result<LanguageTagDict> {
    if data.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run file: lang dict too small",
        ));
    }
    let count = u16::from_le_bytes(data[0..2].try_into().unwrap());
    let mut dict = LanguageTagDict::new();
    let mut pos = 2;
    for _ in 0..count {
        if pos >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: lang dict truncated",
            ));
        }
        let len = data[pos] as usize;
        pos += 1;
        if pos + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: lang dict entry truncated",
            ));
        }
        let tag = std::str::from_utf8(&data[pos..pos + len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("run file: invalid UTF-8 in lang dict: {}", e),
            )
        })?;
        dict.get_or_insert(Some(tag));
        pos += len;
    }
    Ok(dict)
}

// ============================================================================
// Write a complete run file
// ============================================================================

/// Write a sorted run file to disk.
///
/// The records must already be sorted in the given `sort_order`.
pub fn write_run_file(
    path: &Path,
    records: &[RunRecord],
    lang_dict: &LanguageTagDict,
    sort_order: RunSortOrder,
    min_t: u32,
    max_t: u32,
) -> io::Result<RunFileInfo> {
    let raw = std::fs::File::create(path)?;
    try_disable_os_cache(&raw);
    let mut file = io::BufWriter::new(raw);

    let compress_zstd = std::env::var("FLUREE_RUN_ZSTD")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true);
    let zstd_level = std::env::var("FLUREE_RUN_ZSTD_LEVEL")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    // Serialize lang dict
    let lang_bytes = serialize_lang_dict(lang_dict);

    // Compute offsets
    let lang_dict_offset = RUN_HEADER_LEN as u64;
    let lang_dict_len = lang_bytes.len() as u64;
    let records_offset = lang_dict_offset + lang_dict_len;

    // Write header
    let header = RunFileHeader {
        version: RUN_VERSION,
        sort_order,
        flags: if compress_zstd {
            RUN_FLAG_ZSTD_BLOCKS
        } else {
            0
        },
        record_count: records.len() as u64,
        lang_dict_offset,
        lang_dict_len,
        records_offset,
        min_t,
        max_t,
    };
    let mut header_buf = [0u8; RUN_HEADER_LEN];
    header.write_to(&mut header_buf);
    file.write_all(&header_buf)?;

    // Write lang dict
    file.write_all(&lang_bytes)?;

    // Write records (either raw stream or compressed blocks).
    if !compress_zstd {
        let mut rec_buf = [0u8; RECORD_WIRE_SIZE];
        for rec in records {
            rec.write_le(&mut rec_buf);
            file.write_all(&rec_buf)?;
        }
    } else {
        // Compressed blocks: each block contains N records encoded as raw wire bytes,
        // then compressed with zstd.
        //
        // Block header:
        // - n_records: u32
        // - raw_len:   u32  (= n_records * RECORD_WIRE_SIZE)
        // - z_len:     u32  (= compressed payload length)
        //
        // Then `z_len` bytes of zstd payload.
        const BLOCK_RECORDS: usize = 8192;
        let mut raw_buf: Vec<u8> = Vec::with_capacity(BLOCK_RECORDS * RECORD_WIRE_SIZE);
        let mut rec_buf = [0u8; RECORD_WIRE_SIZE];

        for chunk in records.chunks(BLOCK_RECORDS) {
            raw_buf.clear();
            raw_buf.resize(chunk.len() * RECORD_WIRE_SIZE, 0u8);
            for (i, rec) in chunk.iter().enumerate() {
                rec.write_le(&mut rec_buf);
                let off = i * RECORD_WIRE_SIZE;
                raw_buf[off..off + RECORD_WIRE_SIZE].copy_from_slice(&rec_buf);
            }

            let compressed = zstd::bulk::compress(&raw_buf, zstd_level)?;
            let n_records = chunk.len() as u32;
            let raw_len = raw_buf.len() as u32;
            let z_len = compressed.len() as u32;
            file.write_all(&n_records.to_le_bytes())?;
            file.write_all(&raw_len.to_le_bytes())?;
            file.write_all(&z_len.to_le_bytes())?;
            file.write_all(&compressed)?;
        }
    }

    file.flush()?;

    Ok(RunFileInfo {
        path: path.to_path_buf(),
        record_count: records.len() as u64,
        sort_order,
        min_t,
        max_t,
    })
}

// ============================================================================
// Read a run file
// ============================================================================

/// Read a run file header + lang dict + all records.
pub fn read_run_file(path: &Path) -> io::Result<(RunFileHeader, LanguageTagDict, Vec<RunRecord>)> {
    let data = std::fs::read(path)?;
    if data.len() < RUN_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run file too small",
        ));
    }

    let header = RunFileHeader::read_from(&data)?;

    // Read lang dict
    let ld_start = header.lang_dict_offset as usize;
    let ld_end = ld_start + header.lang_dict_len as usize;
    if ld_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run file: lang dict extends past end",
        ));
    }
    let lang_dict = deserialize_lang_dict(&data[ld_start..ld_end])?;

    // Read records
    let rec_start = header.records_offset as usize;
    let mut records = Vec::with_capacity(header.record_count as usize);
    if (header.flags & RUN_FLAG_ZSTD_BLOCKS) == 0 {
        let rec_byte_count = (header.record_count as usize) * RECORD_WIRE_SIZE;
        if rec_start + rec_byte_count > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: records extend past end",
            ));
        }
        let mut pos = rec_start;
        for _ in 0..header.record_count {
            let buf: &[u8; RECORD_WIRE_SIZE] =
                data[pos..pos + RECORD_WIRE_SIZE].try_into().unwrap();
            records.push(RunRecord::read_le(buf));
            pos += RECORD_WIRE_SIZE;
        }
    } else {
        // Compressed blocks stream.
        let mut pos = rec_start;
        while (records.len() as u64) < header.record_count {
            if pos + 12 > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run file: truncated compressed block header",
                ));
            }
            let n_records = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let raw_len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let z_len = u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap()) as usize;
            pos += 12;
            if raw_len != n_records * RECORD_WIRE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run file: invalid compressed block raw_len",
                ));
            }
            if pos + z_len > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run file: truncated compressed block payload",
                ));
            }
            let compressed = &data[pos..pos + z_len];
            pos += z_len;
            let raw = zstd::bulk::decompress(compressed, raw_len)?;
            if raw.len() != raw_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run file: decompressed length mismatch",
                ));
            }
            for i in 0..n_records {
                let off = i * RECORD_WIRE_SIZE;
                let buf: &[u8; RECORD_WIRE_SIZE] =
                    raw[off..off + RECORD_WIRE_SIZE].try_into().unwrap();
                records.push(RunRecord::read_le(buf));
            }
        }
    }

    Ok((header, lang_dict, records))
}

// ============================================================================
// RunFileInfo
// ============================================================================

/// Metadata about a written run file.
#[derive(Debug, Clone)]
pub struct RunFileInfo {
    pub path: std::path::PathBuf,
    pub record_count: u64,
    pub sort_order: RunSortOrder,
    pub min_t: u32,
    pub max_t: u32,
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

    #[test]
    fn test_header_round_trip() {
        let header = RunFileHeader {
            version: RUN_VERSION,
            sort_order: RunSortOrder::Spot,
            flags: 0,
            record_count: 1000,
            lang_dict_offset: 64,
            lang_dict_len: 10,
            records_offset: 74,
            min_t: 1,
            max_t: 100,
        };
        let mut buf = [0u8; RUN_HEADER_LEN];
        header.write_to(&mut buf);

        let parsed = RunFileHeader::read_from(&buf).unwrap();
        assert!((1..=3).contains(&parsed.version));
        assert_eq!(parsed.sort_order, RunSortOrder::Spot);
        assert_eq!(parsed.record_count, 1000);
        assert_eq!(parsed.min_t, 1);
        assert_eq!(parsed.max_t, 100);
    }

    #[test]
    fn test_lang_dict_round_trip() {
        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        dict.get_or_insert(Some("fr"));
        dict.get_or_insert(Some("de-AT"));

        let bytes = serialize_lang_dict(&dict);
        let restored = deserialize_lang_dict(&bytes).unwrap();

        assert_eq!(restored.len(), 3);
        assert_eq!(restored.resolve(1), Some("en"));
        assert_eq!(restored.resolve(2), Some("fr"));
        assert_eq!(restored.resolve(3), Some("de-AT"));
    }

    #[test]
    fn test_write_and_read_run_file() {
        let dir = std::env::temp_dir().join("fluree_test_run_file");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let mut lang_dict = LanguageTagDict::new();
        lang_dict.get_or_insert(Some("en"));

        let records = vec![
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(10),
                1,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(5),
                1,
                true,
                DatatypeDictId::STRING.as_u16(),
                1,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(20),
                2,
                true,
                DatatypeDictId::LONG.as_u16(),
                0,
                Some(0),
            ),
        ];

        let info = write_run_file(&path, &records, &lang_dict, RunSortOrder::Spot, 1, 2).unwrap();
        assert_eq!(info.record_count, 3);
        assert_eq!(info.min_t, 1);
        assert_eq!(info.max_t, 2);

        // Read back
        let (header, restored_lang, restored_records) = read_run_file(&path).unwrap();
        assert_eq!(header.record_count, 3);
        assert_eq!(header.sort_order, RunSortOrder::Spot);
        assert_eq!(restored_lang.len(), 1);
        assert_eq!(restored_lang.resolve(1), Some("en"));

        assert_eq!(restored_records.len(), 3);
        assert_eq!(restored_records[0].s_id, SubjectId::from_u64(1));
        assert_eq!(restored_records[0].p_id, 1);
        assert_eq!(restored_records[0].o_kind, ObjKind::NUM_INT.as_u8());
        assert_eq!(restored_records[0].o_key, ObjKey::encode_i64(10).as_u64());
        assert_eq!(restored_records[1].lang_id, 1);
        assert_eq!(restored_records[2].s_id, SubjectId::from_u64(2));
        assert_eq!(restored_records[2].i, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_empty_run_file() {
        let dir = std::env::temp_dir().join("fluree_test_empty_run");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.frn");

        let lang_dict = LanguageTagDict::new();
        write_run_file(&path, &[], &lang_dict, RunSortOrder::Spot, 0, 0).unwrap();

        let (header, restored_lang, records) = read_run_file(&path).unwrap();
        assert_eq!(header.record_count, 0);
        assert!(restored_lang.is_empty());
        assert!(records.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
