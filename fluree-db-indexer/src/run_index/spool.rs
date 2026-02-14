//! Spool format for the Tier 2 parallel import pipeline.
//!
//! A spool file is a flat sequence of [`RunRecord`] spool-wire-format records
//! (36 bytes each, includes `g_id`) written during Turtle parse. Unlike run
//! files, spool files are unsorted and may contain chunk-local IDs (subjects,
//! strings) that require remapping before they can be fed into the index builder.
//!
//! ## Phase A (format + plumbing validation)
//!
//! During Phase A, spool records contain **chunk-local IDs** (not global).
//! True global IDs are only available inside the resolver's `GlobalDicts`, which
//! the spool pipeline intentionally bypasses. Phase A validates:
//! - Wire format correctness (36-byte spool RunRecord round-trip)
//! - Record count consistency (each parsed triple produces a spool record)
//! - Plumbing: spool files are created, written, and collected correctly
//!
//! Semantic equivalence (same IDs as the resolver) is proven in Phase B after
//! the merge + remap pipeline is complete.
//!
//! ## Phase B (local IDs + remap)
//!
//! In the full pipeline, `s_id` and `o_key` (for `REF_ID`/`LEX_ID` kinds) hold
//! chunk-local dictionary IDs. A merge + remap pass converts them to global IDs
//! and writes the result to sorted run files via [`MultiOrderRunWriter`].
//!
//! ## Wire format
//!
//! Each record is exactly [`SPOOL_RECORD_WIRE_SIZE`] (36) bytes, little-endian,
//! serialized via [`RunRecord::write_spool_le`] / [`RunRecord::read_spool_le`].
//! Unlike the 34-byte run wire format, spool records include `g_id: u16`
//! because spool files are pre-graph-partition.

use super::run_record::{RunRecord, SPOOL_RECORD_WIRE_SIZE};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ============================================================================
// Remap table abstractions (memory-friendly)
// ============================================================================

/// Subject remap table: chunk-local subject ID → global sid64.
///
/// Implementations may be backed by slices or memory-mapped files.
pub trait SubjectRemap {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get(&self, idx: usize) -> io::Result<u64>;
}

/// String remap table: chunk-local string ID → global string ID.
pub trait StringRemap {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get(&self, idx: usize) -> io::Result<u32>;
}

impl SubjectRemap for [u64] {
    fn len(&self) -> usize {
        <[u64]>::len(self)
    }

    fn get(&self, idx: usize) -> io::Result<u64> {
        self.get(idx).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            )
        })
    }
}

impl SubjectRemap for Vec<u64> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn get(&self, idx: usize) -> io::Result<u64> {
        self.as_slice().get(idx).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            )
        })
    }
}

impl StringRemap for [u32] {
    fn len(&self) -> usize {
        <[u32]>::len(self)
    }

    fn get(&self, idx: usize) -> io::Result<u32> {
        self.get(idx).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            )
        })
    }
}

impl StringRemap for Vec<u32> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn get(&self, idx: usize) -> io::Result<u32> {
        self.as_slice().get(idx).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            )
        })
    }
}

/// Memory-mapped subject remap table (u64 little-endian entries).
#[derive(Debug)]
pub struct MmapSubjectRemap {
    mmap: memmap2::Mmap,
}

impl MmapSubjectRemap {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = std::fs::File::open(path.as_ref())?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        if mmap.len() % 8 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject remap file length not multiple of 8: {}",
                    mmap.len()
                ),
            ));
        }
        Ok(Self { mmap })
    }
}

impl SubjectRemap for MmapSubjectRemap {
    fn len(&self) -> usize {
        self.mmap.len() / 8
    }

    fn get(&self, idx: usize) -> io::Result<u64> {
        let start = idx.checked_mul(8).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "subject remap idx overflow")
        })?;
        let end = start + 8;
        if end > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            ));
        }
        let bytes: [u8; 8] = self.mmap[start..end].try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }
}

/// Memory-mapped string remap table (u32 little-endian entries).
#[derive(Debug)]
pub struct MmapStringRemap {
    mmap: memmap2::Mmap,
}

impl MmapStringRemap {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = std::fs::File::open(path.as_ref())?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        if mmap.len() % 4 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("string remap file length not multiple of 4: {}", mmap.len()),
            ));
        }
        Ok(Self { mmap })
    }
}

impl StringRemap for MmapStringRemap {
    fn len(&self) -> usize {
        self.mmap.len() / 4
    }

    fn get(&self, idx: usize) -> io::Result<u32> {
        let start = idx.checked_mul(4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "string remap idx overflow")
        })?;
        let end = start + 4;
        if end > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string remap out of bounds: idx={}, len={}",
                    idx,
                    self.len()
                ),
            ));
        }
        let bytes: [u8; 4] = self.mmap[start..end].try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }
}

/// Magic bytes for a versioned spool file header.
pub const SPOOL_MAGIC: [u8; 4] = *b"FSP2";

/// Current version of the versioned spool format.
pub const SPOOL_VERSION: u8 = 2;

/// Fixed header length in bytes for versioned spool files.
pub const SPOOL_HEADER_LEN: usize = 32;

/// Spool file flags.
const SPOOL_FLAG_ZSTD: u8 = 1 << 0;

#[derive(Debug, Clone, Copy)]
struct SpoolHeaderV2 {
    version: u8,
    flags: u8,
    chunk_idx: u32,
    record_count: u64,
}

impl SpoolHeaderV2 {
    fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= SPOOL_HEADER_LEN);
        buf[0..4].copy_from_slice(&SPOOL_MAGIC);
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..8].fill(0); // reserved
        buf[8..12].copy_from_slice(&self.chunk_idx.to_le_bytes());
        buf[12..20].copy_from_slice(&self.record_count.to_le_bytes());
        buf[20..32].fill(0); // reserved
    }

    fn read_from(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < SPOOL_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "spool header too small: {} < {}",
                    buf.len(),
                    SPOOL_HEADER_LEN
                ),
            ));
        }
        if buf[0..4] != SPOOL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "spool: invalid magic bytes",
            ));
        }
        let version = buf[4];
        if version != SPOOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("spool: unsupported version {}", version),
            ));
        }
        Ok(Self {
            version,
            flags: buf[5],
            chunk_idx: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            record_count: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct SpoolWriteOptions {
    compress_zstd: bool,
    zstd_level: i32,
}

impl Default for SpoolWriteOptions {
    fn default() -> Self {
        // Enabled by default: spool writes are large and sequential; compression
        // typically reduces disk I/O significantly on real TTL imports.
        //
        // Override with:
        // - FLUREE_SPOOL_ZSTD=0 / false (disable)
        // - FLUREE_SPOOL_ZSTD_LEVEL=<int> (default: 1)
        let compress_zstd = std::env::var("FLUREE_SPOOL_ZSTD")
            .ok()
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        let zstd_level = std::env::var("FLUREE_SPOOL_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1);
        Self {
            compress_zstd,
            zstd_level,
        }
    }
}

// ============================================================================
// SpoolWriter
// ============================================================================

/// Buffered binary writer that appends [`RunRecord`]s to a spool file.
///
/// Records are written in arrival order (unsorted). The file begins with a
/// fixed-size header (versioned), followed by either raw `RECORD_WIRE_SIZE`-byte
/// records or a zstd-compressed stream of those record bytes.
pub struct SpoolWriter {
    inner: SpoolWriterInner,
    path: PathBuf,
    record_count: u64,
    chunk_idx: usize,
    flags: u8,
}

enum SpoolWriterInner {
    Raw(BufWriter<std::fs::File>),
    Zstd(zstd::stream::write::Encoder<'static, std::fs::File>),
}

impl SpoolWriter {
    /// Create a new spool writer targeting `path` for `chunk_idx`.
    ///
    /// The file is created (or truncated) immediately.
    pub fn new(path: impl Into<PathBuf>, chunk_idx: usize) -> io::Result<Self> {
        Self::new_with_options(path, chunk_idx, SpoolWriteOptions::default())
    }

    fn new_with_options(
        path: impl Into<PathBuf>,
        chunk_idx: usize,
        options: SpoolWriteOptions,
    ) -> io::Result<Self> {
        let path = path.into();
        let mut file = std::fs::File::create(&path)?;

        let flags = if options.compress_zstd {
            SPOOL_FLAG_ZSTD
        } else {
            0
        };

        // Write placeholder header; we overwrite record_count on finish.
        let header = SpoolHeaderV2 {
            version: SPOOL_VERSION,
            flags,
            chunk_idx: chunk_idx as u32,
            record_count: 0,
        };
        let mut header_buf = [0u8; SPOOL_HEADER_LEN];
        header.write_to(&mut header_buf);
        file.write_all(&header_buf)?;

        let inner = if options.compress_zstd {
            let mut enc = zstd::stream::write::Encoder::new(file, options.zstd_level)?;
            // Enable checksums for corruption detection; small overhead, helpful for large files.
            enc.include_checksum(true)?;
            SpoolWriterInner::Zstd(enc)
        } else {
            SpoolWriterInner::Raw(BufWriter::with_capacity(256 * 1024, file)) // 256 KB buffer
        };

        Ok(Self {
            inner,
            path,
            record_count: 0,
            chunk_idx,
            flags,
        })
    }

    /// Append a single record to the spool file.
    #[inline]
    pub fn push(&mut self, record: &RunRecord) -> io::Result<()> {
        let mut buf = [0u8; SPOOL_RECORD_WIRE_SIZE];
        record.write_spool_le(&mut buf);
        match &mut self.inner {
            SpoolWriterInner::Raw(w) => w.write_all(&buf)?,
            SpoolWriterInner::Zstd(w) => w.write_all(&buf)?,
        }
        self.record_count += 1;
        Ok(())
    }

    /// Number of records written so far.
    #[inline]
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Path to the spool file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush and close the spool writer, returning the path, record count, and chunk index.
    pub fn finish(self) -> io::Result<SpoolFileInfo> {
        // Finish inner writer and recover the file handle.
        let mut file = match self.inner {
            SpoolWriterInner::Raw(mut w) => {
                w.flush()?;
                w.into_inner().map_err(|e| e.into_error())?
            }
            SpoolWriterInner::Zstd(w) => {
                // finish() flushes and returns the underlying writer.
                w.finish()?
            }
        };

        // Rewrite header with final record count.
        file.seek(SeekFrom::Start(0))?;
        let header = SpoolHeaderV2 {
            version: SPOOL_VERSION,
            flags: self.flags,
            chunk_idx: self.chunk_idx as u32,
            record_count: self.record_count,
        };
        let mut header_buf = [0u8; SPOOL_HEADER_LEN];
        header.write_to(&mut header_buf);
        file.write_all(&header_buf)?;
        file.flush()?;

        let byte_len = file.metadata()?.len();

        Ok(SpoolFileInfo {
            path: self.path,
            record_count: self.record_count,
            byte_len,
            chunk_idx: self.chunk_idx,
        })
    }
}

/// Metadata about a completed spool file.
#[derive(Debug, Clone)]
pub struct SpoolFileInfo {
    /// Path to the spool file on disk.
    pub path: PathBuf,
    /// Number of records written.
    pub record_count: u64,
    /// File size on disk (bytes). Useful for import I/O accounting.
    pub byte_len: u64,
    /// Chunk index (0-based) that produced this spool file.
    /// Used to enforce deterministic ordering in the merge phase.
    pub chunk_idx: usize,
}

// ============================================================================
// SpoolReader
// ============================================================================

/// Sequential reader for spool files.
///
/// Reads records one at a time from a spool file.
///
/// Supports both:
/// - Legacy headerless spool files (raw record stream).
/// - Versioned spool files (header + optional zstd-compressed record stream).
pub struct SpoolReader {
    inner: SpoolReaderInner,
    remaining: u64,
}

enum SpoolReaderInner {
    Raw(io::BufReader<std::fs::File>),
    Zstd(io::BufReader<zstd::stream::read::Decoder<'static, io::BufReader<std::fs::File>>>),
}

impl std::fmt::Debug for SpoolReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpoolReader")
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl SpoolReader {
    /// Open a spool file for reading.
    ///
    /// `record_count` must match the number of records written (from
    /// [`SpoolFileInfo`]). This is used to bound the read and detect truncation.
    pub fn open(path: impl AsRef<Path>, record_count: u64) -> io::Result<Self> {
        let mut file = std::fs::File::open(path.as_ref())?;

        // Peek at first 4 bytes to detect versioned spool header.
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        file.seek(SeekFrom::Start(0))?;

        if magic == SPOOL_MAGIC {
            let mut header_buf = [0u8; SPOOL_HEADER_LEN];
            file.read_exact(&mut header_buf)?;
            let header = SpoolHeaderV2::read_from(&header_buf)?;

            if header.record_count != record_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "spool header record_count mismatch: header={}, expected={}",
                        header.record_count, record_count
                    ),
                ));
            }

            if (header.flags & SPOOL_FLAG_ZSTD) != 0 {
                // Decoder::new wraps its input in a BufReader internally, so passing the
                // raw file (positioned after the header) is sufficient.
                let dec = zstd::stream::read::Decoder::new(file)?;
                Ok(Self {
                    inner: SpoolReaderInner::Zstd(io::BufReader::with_capacity(256 * 1024, dec)),
                    remaining: record_count,
                })
            } else {
                // Validate truncation for uncompressed versioned files.
                let expected_size =
                    (SPOOL_HEADER_LEN as u64) + record_count * SPOOL_RECORD_WIRE_SIZE as u64;
                let actual_size = std::fs::metadata(path.as_ref())?.len();
                if actual_size < expected_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "spool file truncated: expected {} bytes (header {} + {} records × {}), got {}",
                            expected_size, SPOOL_HEADER_LEN, record_count, SPOOL_RECORD_WIRE_SIZE, actual_size
                        ),
                    ));
                }
                Ok(Self {
                    inner: SpoolReaderInner::Raw(io::BufReader::with_capacity(256 * 1024, file)),
                    remaining: record_count,
                })
            }
        } else {
            // Legacy headerless spool: raw record stream.
            let expected_size = record_count * SPOOL_RECORD_WIRE_SIZE as u64;
            let actual_size = file.metadata()?.len();
            if actual_size < expected_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "spool file truncated: expected {} bytes ({} records × {}), got {}",
                        expected_size, record_count, SPOOL_RECORD_WIRE_SIZE, actual_size
                    ),
                ));
            }
            Ok(Self {
                inner: SpoolReaderInner::Raw(io::BufReader::with_capacity(256 * 1024, file)),
                remaining: record_count,
            })
        }
    }

    /// Read the next record, or `None` if all records have been consumed.
    pub fn next_record(&mut self) -> io::Result<Option<RunRecord>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut buf = [0u8; SPOOL_RECORD_WIRE_SIZE];
        match &mut self.inner {
            SpoolReaderInner::Raw(r) => r.read_exact(&mut buf)?,
            SpoolReaderInner::Zstd(r) => r.read_exact(&mut buf)?,
        }
        self.remaining -= 1;
        Ok(Some(RunRecord::read_spool_le(&buf)))
    }

    /// Number of records remaining to be read.
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl Iterator for SpoolReader {
    type Item = io::Result<RunRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_record() {
            Ok(Some(rec)) => Some(Ok(rec)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

// ============================================================================
// Helpers for multi-chunk run file collection
// ============================================================================

/// Collect all run files for a given sort order across multiple chunk subdirs.
///
/// Expects a directory layout produced by parallel remap threads:
/// ```text
/// base_run_dir/
/// ├── chunk_0/{order}/run_00000.frn, ...
/// ├── chunk_1/{order}/run_00000.frn, ...
/// └── chunk_N/{order}/run_00000.frn, ...
/// ```
///
/// Returns a sorted list of all matching run file paths.
pub fn collect_chunk_run_files(
    base_run_dir: &Path,
    order: super::run_record::RunSortOrder,
) -> io::Result<Vec<PathBuf>> {
    let mut all_runs = Vec::new();
    let mut chunk_dirs: Vec<_> = std::fs::read_dir(base_run_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
                && e.file_name().to_string_lossy().starts_with("chunk_")
        })
        .map(|e| e.path())
        .collect();
    chunk_dirs.sort();

    for chunk_dir in chunk_dirs {
        let order_dir = chunk_dir.join(order.dir_name());
        if !order_dir.exists() {
            continue;
        }
        let mut runs = super::index_build::discover_run_files(&order_dir)?;
        runs.sort();
        all_runs.extend(runs);
    }
    Ok(all_runs)
}

/// Link (symlink/hardlink) all run files for `order` from `chunk_*` subdirs into `flat_dir`.
///
/// This is a **streaming** alternative to [`collect_chunk_run_files`] + linking:
/// it does *not* allocate a global `Vec<PathBuf>` of all run files (which can be
/// enormous on large imports).
///
/// Returns the number of linked run files.
pub fn link_chunk_run_files_to_flat(
    base_run_dir: &Path,
    order: super::run_record::RunSortOrder,
    flat_dir: &Path,
) -> io::Result<usize> {
    std::fs::create_dir_all(flat_dir)?;

    let mut chunk_dirs: Vec<_> = std::fs::read_dir(base_run_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
                && e.file_name().to_string_lossy().starts_with("chunk_")
        })
        .map(|e| e.path())
        .collect();
    chunk_dirs.sort();

    let mut next_idx: usize = 0;
    for chunk_dir in chunk_dirs {
        let order_dir = chunk_dir.join(order.dir_name());
        if !order_dir.exists() {
            continue;
        }
        let mut runs = super::index_build::discover_run_files(&order_dir)?;
        runs.sort();
        for src in runs {
            let dst = flat_dir.join(format!("run_{:05}.frn", next_idx));
            next_idx += 1;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&src, &dst)?;
            #[cfg(not(unix))]
            std::fs::hard_link(&src, &dst)?;
        }
    }

    Ok(next_idx)
}

// ============================================================================
// Spool-to-Run Bypass (Phase A validation)
// ============================================================================

/// Read a spool file and write all records directly to a [`MultiOrderRunWriter`].
///
/// This function validates the mechanical path: spool → reader → run writer.
/// In Phase A, spool IDs are chunk-local so the resulting run files are NOT
/// semantically equivalent to the resolver's output. Phase B adds a remap step
/// (via `remap_and_write_runs`) that converts chunk-local IDs to global IDs
/// before writing, making the output equivalent to the resolver.
///
/// Returns the number of records written.
pub fn spool_to_runs(
    spool_info: &SpoolFileInfo,
    writer: &mut super::run_writer::MultiOrderRunWriter,
    lang_dict: &mut super::global_dict::LanguageTagDict,
) -> io::Result<u64> {
    let reader = SpoolReader::open(&spool_info.path, spool_info.record_count)?;
    let mut count = 0u64;
    for result in reader {
        let record = result?;
        writer.push(record, lang_dict)?;
        count += 1;
    }
    Ok(count)
}

// ============================================================================
// Remap pass (Phase B)
// ============================================================================

/// Read a spool file, remap chunk-local IDs to global IDs, and write the
/// result to a [`MultiOrderRunWriter`].
///
/// This is the Phase B replacement for [`spool_to_runs`]. It applies:
/// - **Subject remap**: `s_id` (chunk-local → global sid64)
/// - **REF_ID remap**: `o_key` for `REF_ID` objects (chunk-local → global sid64)
/// - **String remap**: `o_key` for `LEX_ID`/`JSON_ID` objects (chunk-local → global string ID)
///
/// All other object kinds (inline numerics, booleans, dates, etc.) and fields
/// (p_id, dt, g_id, lang_id) are passed through unchanged — they already have
/// global IDs from shared allocators.
///
/// If `stats_hook` is provided, each remapped record is also fed to it for
/// HLL NDV estimation and class tracking. The hook should be per-chunk;
/// after all chunks complete, merge hooks via [`IdStatsHook::merge_from`].
///
/// The `dt_tags` table maps datatype dict IDs (the `dt` field) to
/// [`ValueTypeTag`] for stats. When `None`, `ValueTypeTag::UNKNOWN` is used.
///
/// Returns the number of records written.
#[allow(clippy::too_many_arguments)]
pub fn remap_spool_to_runs(
    spool_info: &SpoolFileInfo,
    subject_remap: &dyn SubjectRemap,
    string_remap: &dyn StringRemap,
    writer: &mut super::run_writer::MultiOrderRunWriter,
    lang_dict: &mut super::global_dict::LanguageTagDict,
    mut stats_hook: Option<&mut crate::stats::IdStatsHook>,
    dt_tags: Option<&[fluree_db_core::value_id::ValueTypeTag]>,
) -> io::Result<u64> {
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind, ValueTypeTag};

    let reader = SpoolReader::open(&spool_info.path, spool_info.record_count)?;
    let mut count = 0u64;

    for result in reader {
        let mut record = result?;

        // Remap subject: chunk-local u64 → global sid64
        let local_s = record.s_id.as_u64();
        let global_s = subject_remap.get(local_s as usize)?;
        record.s_id = SubjectId::from_u64(global_s);

        // Remap object if it holds a chunk-local ID
        let o_kind = ObjKind::from_u8(record.o_kind);
        if o_kind == ObjKind::REF_ID {
            // Object reference: chunk-local subject ID → global sid64
            let local_o = record.o_key;
            let global_o = subject_remap.get(local_o as usize)?;
            record.o_key = global_o;
        } else if o_kind == ObjKind::LEX_ID || o_kind == ObjKind::JSON_ID {
            // String literal / JSON: chunk-local string ID → global string ID
            let local_str = ObjKey::from_u64(record.o_key).decode_u32_id();
            let global_str = string_remap.get(local_str as usize)?;
            record.o_key = ObjKey::encode_u32_id(global_str).as_u64();
        }
        // All other o_kind values: inline values with no chunk-local IDs

        // Feed remapped record to stats hook (global IDs are now valid)
        if let Some(ref mut hook) = stats_hook {
            let dt = dt_tags
                .and_then(|t| t.get(record.dt as usize).copied())
                .unwrap_or(ValueTypeTag::UNKNOWN);
            hook.on_record(&crate::stats::StatsRecord {
                g_id: record.g_id as u32,
                p_id: record.p_id,
                s_id: record.s_id.as_u64(),
                dt,
                o_hash: crate::stats::value_hash(record.o_kind, record.o_key),
                o_kind: record.o_kind,
                o_key: record.o_key,
                t: record.t as i64,
                op: record.op != 0,
            });
        }

        writer.push(record, lang_dict)?;
        count += 1;
    }

    Ok(count)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::run_record::LIST_INDEX_NONE;
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
    fn test_write_read_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_spool_rt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.spool");

        let records = vec![
            make_record(1, 10, ObjKind::LEX_ID, 100, 1),
            make_record(2, 20, ObjKind::NUM_INT, 42, 1),
            make_record(3, 10, ObjKind::REF_ID, 5, 2),
        ];

        // Write
        let mut writer = SpoolWriter::new(&path, 0).unwrap();
        for rec in &records {
            writer.push(rec).unwrap();
        }
        assert_eq!(writer.record_count(), 3);
        let info = writer.finish().unwrap();
        assert_eq!(info.record_count, 3);
        assert_eq!(info.path, path);

        // Read sequentially
        let mut reader = SpoolReader::open(&path, 3).unwrap();
        for expected in &records {
            let actual = reader.next_record().unwrap().unwrap();
            assert_eq!(actual.s_id, expected.s_id);
            assert_eq!(actual.p_id, expected.p_id);
            assert_eq!(actual.o_kind, expected.o_kind);
            assert_eq!(actual.o_key, expected.o_key);
            assert_eq!(actual.t, expected.t);
            assert_eq!(actual.op, expected.op);
            assert_eq!(actual.dt, expected.dt);
            assert_eq!(actual.lang_id, expected.lang_id);
            assert_eq!(actual.i, expected.i);
            assert_eq!(actual.g_id, expected.g_id);
        }
        assert!(reader.next_record().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_empty_spool() {
        let dir = std::env::temp_dir().join("fluree_test_spool_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.spool");

        let writer = SpoolWriter::new(&path, 0).unwrap();
        assert_eq!(writer.record_count(), 0);
        let info = writer.finish().unwrap();
        assert_eq!(info.record_count, 0);

        let mut reader = SpoolReader::open(&path, 0).unwrap();
        assert!(reader.next_record().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_iterator_interface() {
        let dir = std::env::temp_dir().join("fluree_test_spool_iter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("iter.spool");

        let records = vec![
            make_record(1, 10, ObjKind::NUM_INT, 100, 1),
            make_record(2, 20, ObjKind::NUM_INT, 200, 2),
        ];

        let mut writer = SpoolWriter::new(&path, 0).unwrap();
        for rec in &records {
            writer.push(rec).unwrap();
        }
        writer.finish().unwrap();

        let reader = SpoolReader::open(&path, 2).unwrap();
        let read_records: Vec<RunRecord> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(read_records.len(), 2);
        assert_eq!(read_records[0].s_id, SubjectId::from_u64(1));
        assert_eq!(read_records[1].s_id, SubjectId::from_u64(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_truncated_spool_detected() {
        let dir = std::env::temp_dir().join("fluree_test_spool_trunc");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trunc.spool");

        // Write 1 record (36 bytes)
        let mut writer = SpoolWriter::new(&path, 0).unwrap();
        writer
            .push(&make_record(1, 1, ObjKind::NUM_INT, 1, 1))
            .unwrap();
        writer.finish().unwrap();

        // Try to open claiming 2 records (72 bytes expected)
        let result = SpoolReader::open(&path, 2);
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Versioned spool files include a header with the true record_count.
        // A mismatch is detected early and is considered equivalent to truncation.
        let msg = err.to_string();
        assert!(
            msg.contains("truncated") || msg.contains("record_count mismatch"),
            "unexpected error message: {msg}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_all_obj_kinds_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_spool_kinds");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kinds.spool");

        let records = vec![
            make_record(1, 1, ObjKind::REF_ID, 42, 1),
            make_record(1, 2, ObjKind::LEX_ID, 99, 1),
            make_record(1, 3, ObjKind::NUM_INT, ObjKey::encode_i64(-7).as_u64(), 1),
            make_record(
                1,
                4,
                ObjKind::NUM_F64,
                ObjKey::encode_f64(3.125).unwrap().as_u64(),
                1,
            ),
            make_record(1, 5, ObjKind::BOOL, ObjKey::encode_bool(true).as_u64(), 1),
            make_record(1, 6, ObjKind::GEO_POINT, 0xDEADBEEF_CAFEBABE, 1),
        ];

        let mut writer = SpoolWriter::new(&path, 0).unwrap();
        for rec in &records {
            writer.push(rec).unwrap();
        }
        writer.finish().unwrap();

        let reader = SpoolReader::open(&path, records.len() as u64).unwrap();
        let read: Vec<RunRecord> = reader.map(|r| r.unwrap()).collect();

        for (i, (orig, read)) in records.iter().zip(read.iter()).enumerate() {
            assert_eq!(orig.o_kind, read.o_kind, "o_kind mismatch at record {}", i);
            assert_eq!(orig.o_key, read.o_key, "o_key mismatch at record {}", i);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lang_id_and_list_index_preserved() {
        let dir = std::env::temp_dir().join("fluree_test_spool_meta");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("meta.spool");

        let rec = RunRecord {
            g_id: 2,
            s_id: SubjectId::from_u64(0x0003_0000_0000_0042),
            p_id: 7,
            dt: 5,
            o_kind: ObjKind::LEX_ID.as_u8(),
            op: 0, // retraction
            o_key: 123,
            t: 999,
            lang_id: 3,
            i: 42,
        };

        let mut writer = SpoolWriter::new(&path, 0).unwrap();
        writer.push(&rec).unwrap();
        writer.finish().unwrap();

        let mut reader = SpoolReader::open(&path, 1).unwrap();
        let read = reader.next_record().unwrap().unwrap();

        assert_eq!(read.g_id, 2);
        assert_eq!(read.s_id, SubjectId::from_u64(0x0003_0000_0000_0042));
        assert_eq!(read.p_id, 7);
        assert_eq!(read.dt, 5);
        assert_eq!(read.o_kind, ObjKind::LEX_ID.as_u8());
        assert_eq!(read.op, 0);
        assert_eq!(read.o_key, 123);
        assert_eq!(read.t, 999);
        assert_eq!(read.lang_id, 3);
        assert_eq!(read.i, 42);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_collect_chunk_run_files() {
        use super::super::run_record::RunSortOrder;

        let dir = std::env::temp_dir().join("fluree_test_spool_collect");
        let _ = std::fs::remove_dir_all(&dir);

        // Create chunk subdirs with fake run files
        for chunk_idx in 0..3 {
            let spot_dir = dir.join(format!("chunk_{}", chunk_idx)).join("spot");
            std::fs::create_dir_all(&spot_dir).unwrap();
            std::fs::write(spot_dir.join("run_00000.frn"), b"fake").unwrap();
            if chunk_idx == 1 {
                std::fs::write(spot_dir.join("run_00001.frn"), b"fake").unwrap();
            }
        }

        let files = collect_chunk_run_files(&dir, RunSortOrder::Spot).unwrap();
        assert_eq!(files.len(), 4); // chunk_0: 1, chunk_1: 2, chunk_2: 1

        // Verify ordering: chunk_0 files first, then chunk_1, then chunk_2
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                let chunk = p.parent().unwrap().parent().unwrap().file_name().unwrap();
                let file = p.file_name().unwrap();
                format!("{}/{}", chunk.to_string_lossy(), file.to_string_lossy())
            })
            .collect();
        assert_eq!(names[0], "chunk_0/run_00000.frn");
        assert_eq!(names[1], "chunk_1/run_00000.frn");
        assert_eq!(names[2], "chunk_1/run_00001.frn");
        assert_eq!(names[3], "chunk_2/run_00000.frn");

        // Non-existent order returns empty
        let psot_files = collect_chunk_run_files(&dir, RunSortOrder::Psot).unwrap();
        assert!(psot_files.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_spool_to_runs() {
        use super::super::global_dict::LanguageTagDict;
        use super::super::run_record::RunSortOrder;
        use super::super::run_writer::{MultiOrderConfig, MultiOrderRunWriter};
        use super::spool_to_runs;

        let dir = std::env::temp_dir().join("fluree_test_spool_to_runs");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write spool file with 5 records
        let spool_path = dir.join("test.spool");
        let records = vec![
            make_record(1, 10, ObjKind::LEX_ID, 100, 1),
            make_record(2, 20, ObjKind::NUM_INT, ObjKey::encode_i64(42).as_u64(), 1),
            make_record(3, 10, ObjKind::REF_ID, 5, 2),
            make_record(4, 30, ObjKind::BOOL, ObjKey::encode_bool(true).as_u64(), 2),
            make_record(5, 10, ObjKind::LEX_ID, 200, 3),
        ];

        let mut spool_writer = SpoolWriter::new(&spool_path, 0).unwrap();
        for rec in &records {
            spool_writer.push(rec).unwrap();
        }
        let spool_info = spool_writer.finish().unwrap();
        assert_eq!(spool_info.record_count, 5);

        // Create a MultiOrderRunWriter targeting a subdirectory
        let run_dir = dir.join("runs");
        std::fs::create_dir_all(&run_dir).unwrap();
        let mo_config = MultiOrderConfig {
            total_budget_bytes: 4 * 1024 * 1024, // 4 MB
            orders: vec![RunSortOrder::Spot],
            base_run_dir: run_dir.clone(),
        };
        let mut writer = MultiOrderRunWriter::new(mo_config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        // Feed spool records into the run writer
        let written = spool_to_runs(&spool_info, &mut writer, &mut lang_dict).unwrap();
        assert_eq!(written, 5);

        // Finish the writer to flush run files
        let results = writer.finish(&mut lang_dict).unwrap();
        assert_eq!(results.len(), 1); // only SPOT order

        let (order, result) = &results[0];
        assert_eq!(*order, RunSortOrder::Spot);
        assert_eq!(result.total_records, 5);
        assert!(!result.run_files.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_remap_spool_to_runs() {
        use super::super::global_dict::LanguageTagDict;
        use super::super::run_record::RunSortOrder;
        use super::super::run_writer::{MultiOrderConfig, MultiOrderRunWriter};
        use super::remap_spool_to_runs;

        let dir = std::env::temp_dir().join("fluree_test_spool_remap_runs");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write spool file with chunk-local IDs:
        // - s_id 0, 1 (chunk-local subject IDs)
        // - LEX_ID o_key encoded from chunk-local string ID 0
        // - REF_ID o_key = 1 (chunk-local subject ID)
        let spool_path = dir.join("chunk_0.spool");
        let records = vec![
            // Subject 0, predicate 10, object = string 0
            make_record(0, 10, ObjKind::LEX_ID, ObjKey::encode_u32_id(0).as_u64(), 1),
            // Subject 1, predicate 20, object = integer 42 (no remap)
            make_record(1, 20, ObjKind::NUM_INT, ObjKey::encode_i64(42).as_u64(), 1),
            // Subject 0, predicate 30, object = ref to subject 1
            make_record(0, 30, ObjKind::REF_ID, 1, 2),
        ];

        let mut spool_writer = SpoolWriter::new(&spool_path, 0).unwrap();
        for rec in &records {
            spool_writer.push(rec).unwrap();
        }
        let spool_info = spool_writer.finish().unwrap();

        // Remap tables:
        // Subject: local 0 → global sid64 (ns=10, local=42), local 1 → (ns=10, local=99)
        let sid_0 = (10u64 << 48) | 42;
        let sid_1 = (10u64 << 48) | 99;
        let subject_remap = vec![sid_0, sid_1];

        // String: local 0 → global string ID 77
        let string_remap = vec![77u32];

        // Create run writer
        let run_dir = dir.join("runs");
        std::fs::create_dir_all(&run_dir).unwrap();
        let mo_config = MultiOrderConfig {
            total_budget_bytes: 4 * 1024 * 1024,
            orders: vec![RunSortOrder::Spot],
            base_run_dir: run_dir.clone(),
        };
        let mut writer = MultiOrderRunWriter::new(mo_config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        let written = remap_spool_to_runs(
            &spool_info,
            &subject_remap,
            &string_remap,
            &mut writer,
            &mut lang_dict,
            None, // no stats_hook
            None, // no dt_tags
        )
        .unwrap();
        assert_eq!(written, 3);

        // Flush to get run files
        let results = writer.finish(&mut lang_dict).unwrap();
        assert_eq!(results.len(), 1);
        let (_, result) = &results[0];
        assert_eq!(result.total_records, 3);

        // Read the run file back and verify remapped values
        let run_path = &result.run_files[0];
        let (_, _, run_records) = super::super::run_file::read_run_file(&run_path.path).unwrap();

        // SPOT sort: records are sorted by s_id, so sid_0 < sid_1
        // sid_0 (ns=10, local=42) appears twice (records 0, 2), sid_1 once (record 1)
        assert_eq!(run_records.len(), 3);

        // Find the LEX_ID record (string object)
        let lex_rec = run_records
            .iter()
            .find(|r| r.o_kind == ObjKind::LEX_ID.as_u8())
            .unwrap();
        assert_eq!(lex_rec.s_id, SubjectId::from_u64(sid_0));
        assert_eq!(ObjKey::from_u64(lex_rec.o_key).decode_u32_id(), 77); // remapped

        // Find the REF_ID record (subject reference)
        let ref_rec = run_records
            .iter()
            .find(|r| r.o_kind == ObjKind::REF_ID.as_u8())
            .unwrap();
        assert_eq!(ref_rec.s_id, SubjectId::from_u64(sid_0));
        assert_eq!(ref_rec.o_key, sid_1); // remapped from local 1 → global sid_1

        // Find the NUM_INT record (no remap on object)
        let int_rec = run_records
            .iter()
            .find(|r| r.o_kind == ObjKind::NUM_INT.as_u8())
            .unwrap();
        assert_eq!(int_rec.s_id, SubjectId::from_u64(sid_1));
        assert_eq!(int_rec.o_key, ObjKey::encode_i64(42).as_u64()); // unchanged

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_remap_out_of_bounds() {
        let dir = std::env::temp_dir().join("fluree_test_spool_remap_oob");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write a spool record with s_id = 5 (but remap table only has 2 entries)
        let spool_path = dir.join("chunk_0.spool");
        let mut spool_writer = SpoolWriter::new(&spool_path, 0).unwrap();
        spool_writer
            .push(&make_record(5, 10, ObjKind::NUM_INT, 42, 1))
            .unwrap();
        let spool_info = spool_writer.finish().unwrap();

        let subject_remap = vec![100u64, 200]; // only indices 0, 1
        let string_remap: Vec<u32> = vec![];

        use super::super::global_dict::LanguageTagDict;
        use super::super::run_record::RunSortOrder;
        use super::super::run_writer::{MultiOrderConfig, MultiOrderRunWriter};

        let run_dir = dir.join("runs");
        std::fs::create_dir_all(&run_dir).unwrap();
        let mo_config = MultiOrderConfig {
            total_budget_bytes: 4 * 1024 * 1024,
            orders: vec![RunSortOrder::Spot],
            base_run_dir: run_dir,
        };
        let mut writer = MultiOrderRunWriter::new(mo_config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        let result = remap_spool_to_runs(
            &spool_info,
            &subject_remap,
            &string_remap,
            &mut writer,
            &mut lang_dict,
            None, // no stats_hook
            None, // no dt_tags
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("out of bounds"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_remap_with_stats_hook() {
        use super::super::global_dict::LanguageTagDict;
        use super::super::run_record::RunSortOrder;
        use super::super::run_writer::{MultiOrderConfig, MultiOrderRunWriter};
        use super::remap_spool_to_runs;
        use fluree_db_core::value_id::ValueTypeTag;

        let dir = std::env::temp_dir().join("fluree_test_spool_remap_stats");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write spool file with chunk-local IDs
        let spool_path = dir.join("chunk_0.spool");
        let records = vec![
            // Subject 0, predicate 10, string object (local string 0)
            make_record(0, 10, ObjKind::LEX_ID, ObjKey::encode_u32_id(0).as_u64(), 1),
            // Subject 1, predicate 20, integer object (no remap)
            make_record(1, 20, ObjKind::NUM_INT, ObjKey::encode_i64(42).as_u64(), 1),
            // Subject 0, predicate 30, ref to subject 1
            make_record(0, 30, ObjKind::REF_ID, 1, 2),
        ];

        let mut spool_writer = SpoolWriter::new(&spool_path, 0).unwrap();
        for rec in &records {
            spool_writer.push(rec).unwrap();
        }
        let spool_info = spool_writer.finish().unwrap();

        // Remap tables
        let sid_0 = (10u64 << 48) | 42;
        let sid_1 = (10u64 << 48) | 99;
        let subject_remap = vec![sid_0, sid_1];
        let string_remap = vec![77u32];

        // dt_tags table: index 0 → STRING (matches DatatypeDictId::STRING used in make_record)
        let dt_tags = vec![ValueTypeTag::STRING];

        // Create stats hook
        let mut stats_hook = crate::stats::IdStatsHook::new();

        // Create run writer
        let run_dir = dir.join("runs");
        std::fs::create_dir_all(&run_dir).unwrap();
        let mo_config = MultiOrderConfig {
            total_budget_bytes: 4 * 1024 * 1024,
            orders: vec![RunSortOrder::Spot],
            base_run_dir: run_dir,
        };
        let mut writer = MultiOrderRunWriter::new(mo_config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        let written = remap_spool_to_runs(
            &spool_info,
            &subject_remap,
            &string_remap,
            &mut writer,
            &mut lang_dict,
            Some(&mut stats_hook),
            Some(&dt_tags),
        )
        .unwrap();
        assert_eq!(written, 3);

        // Verify stats hook received records: finalize and check counts
        let (result, _graph_props, _class_counts, _class_props, _ref_targets) =
            stats_hook.finalize_with_aggregate_properties();

        // All 3 records were assertions, so total_flakes == 3
        assert_eq!(result.total_flakes, 3);

        // All records are in graph 0, so we should have exactly 1 graph entry
        assert_eq!(result.graphs.len(), 1);
        assert_eq!(result.graphs[0].g_id, 0);

        // The graph entry should contain 3 properties (p_id 10, 20, 30)
        assert_eq!(result.graphs[0].properties.len(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
