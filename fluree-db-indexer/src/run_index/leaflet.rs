//! Leaflet encoder and decoder for binary index segments.
//!
//! A leaflet is a compressed block of ~25K rows from the sorted stream.
//! It contains three regions:
//!
//! - **Region 1** (core columns): RLE-encoded `s_id`, raw `p_id[]`, raw `o[]`.
//! - **Region 2** (metadata columns): `dt[]`, `t[]`, sparse `lang[]`, sparse `i[]`.
//! - **Region 3** (history journal): empty for bulk import.
//!
//! Each region is independently zstd-compressed.

use super::leaflet_cache::{SparseIColumn, SparseU16Column};
use super::run_record::{RunRecord, RunSortOrder, LIST_INDEX_NONE};
use fluree_db_core::ListIndex;
use std::io;
use std::sync::Arc;

/// Decoded Region 1 columns: (s_ids, p_ids, o_kinds, o_keys)
pub type Region1Columns = (Vec<u64>, Vec<u32>, Vec<u8>, Vec<u64>);

/// Decoded Region 1 with header: (header, s_ids, p_ids, o_kinds, o_keys)
pub type Region1WithHeader = (LeafletHeader, Vec<u64>, Vec<u32>, Vec<u8>, Vec<u64>);

/// Decoded Region 2: dt and t as dense columns, lang and i as sparse.
pub struct DecodedRegion2 {
    pub dt_values: Vec<u32>,
    pub t_values: Vec<u32>,
    pub lang: Option<SparseU16Column>,
    pub i_col: Option<SparseIColumn>,
}

/// Size of the leaflet header in bytes.
pub const LEAFLET_HEADER_LEN: usize = 61;

// ============================================================================
// Small codec helpers (current format only)
// ============================================================================

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn checked_range<'a>(
    data: &'a [u8],
    start: usize,
    len: usize,
    what: &'static str,
) -> io::Result<&'a [u8]> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid_data(format!("{what}: range overflow")))?;
    if end > data.len() {
        return Err(invalid_data(format!("{what}: extends past end")));
    }
    Ok(&data[start..end])
}

fn zstd_decompress(
    what: &'static str,
    compressed: &[u8],
    expected_len: usize,
) -> io::Result<Vec<u8>> {
    zstd::bulk::decompress(compressed, expected_len)
        .map_err(|e| invalid_data(format!("{what}: zstd decompress: {e}")))
}

fn validate_p_width(p_width: u8) -> io::Result<u8> {
    if p_width == 2 || p_width == 4 {
        Ok(p_width)
    } else {
        Err(invalid_data(format!(
            "invalid p_width {p_width} (expected 2 or 4)"
        )))
    }
}

fn validate_dt_width(dt_width: u8) -> io::Result<u8> {
    if dt_width == 1 || dt_width == 2 {
        Ok(dt_width)
    } else {
        Err(invalid_data(format!(
            "invalid dt_width {dt_width} (expected 1 or 2)"
        )))
    }
}

// ============================================================================
// Leaflet header
// ============================================================================

/// Leaflet header (61 bytes, fixed).
#[derive(Debug, Clone)]
pub struct LeafletHeader {
    pub row_count: u32,
    pub region1_offset: u32,
    pub region1_compressed_len: u32,
    pub region1_uncompressed_len: u32,
    pub region2_offset: u32,
    pub region2_compressed_len: u32,
    pub region2_uncompressed_len: u32,
    pub region3_offset: u32,
    pub region3_compressed_len: u32,
    pub region3_uncompressed_len: u32,
    pub first_s_id: u64,
    pub first_p_id: u32,
    pub first_o_kind: u8,
    pub first_o_key: u64,
}

impl LeafletHeader {
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= LEAFLET_HEADER_LEN);
        buf[0..4].copy_from_slice(&self.row_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.region1_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.region1_compressed_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.region1_uncompressed_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.region2_offset.to_le_bytes());
        buf[20..24].copy_from_slice(&self.region2_compressed_len.to_le_bytes());
        buf[24..28].copy_from_slice(&self.region2_uncompressed_len.to_le_bytes());
        buf[28..32].copy_from_slice(&self.region3_offset.to_le_bytes());
        buf[32..36].copy_from_slice(&self.region3_compressed_len.to_le_bytes());
        buf[36..40].copy_from_slice(&self.region3_uncompressed_len.to_le_bytes());
        buf[40..48].copy_from_slice(&self.first_s_id.to_le_bytes());
        buf[48..52].copy_from_slice(&self.first_p_id.to_le_bytes());
        buf[52] = self.first_o_kind;
        buf[53..61].copy_from_slice(&self.first_o_key.to_le_bytes());
    }

    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < LEAFLET_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "leaflet header too small",
            ));
        }
        Ok(Self {
            row_count: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            region1_offset: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            region1_compressed_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            region1_uncompressed_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            region2_offset: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            region2_compressed_len: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            region2_uncompressed_len: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            region3_offset: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
            region3_compressed_len: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            region3_uncompressed_len: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            first_s_id: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            first_p_id: u32::from_le_bytes(buf[48..52].try_into().unwrap()),
            first_o_kind: buf[52],
            first_o_key: u64::from_le_bytes(buf[53..61].try_into().unwrap()),
        })
    }
}

// ============================================================================
// Encoder
// ============================================================================

/// Leaflet encoder configuration.
pub struct LeafletEncoder {
    /// zstd compression level (default: 1 for build speed).
    zstd_level: i32,
    /// Width of p_id encoding in bytes (2 = u16, 4 = u32). Default: 2.
    p_width: u8,
    /// Width of dt encoding in bytes (1 = u8, 2 = u16). Default: 1.
    dt_width: u8,
    /// Sort order for Region 1 layout.
    sort_order: RunSortOrder,
}

impl LeafletEncoder {
    pub fn new(zstd_level: i32) -> Self {
        Self {
            zstd_level,
            p_width: 2,
            dt_width: 1,
            sort_order: RunSortOrder::Spot,
        }
    }

    /// Create an encoder with explicit field widths.
    pub fn with_widths(zstd_level: i32, p_width: u8, dt_width: u8) -> Self {
        debug_assert!(
            p_width == 2 || p_width == 4,
            "p_width must be 2 or 4, got {}",
            p_width
        );
        debug_assert!(
            dt_width == 1 || dt_width == 2,
            "dt_width must be 1 or 2, got {}",
            dt_width
        );
        Self {
            zstd_level,
            p_width,
            dt_width,
            sort_order: RunSortOrder::Spot,
        }
    }

    /// Create an encoder with explicit field widths and sort order.
    pub fn with_widths_and_order(
        zstd_level: i32,
        p_width: u8,
        dt_width: u8,
        sort_order: RunSortOrder,
    ) -> Self {
        debug_assert!(
            p_width == 2 || p_width == 4,
            "p_width must be 2 or 4, got {}",
            p_width
        );
        debug_assert!(
            dt_width == 1 || dt_width == 2,
            "dt_width must be 1 or 2, got {}",
            dt_width
        );
        Self {
            zstd_level,
            p_width,
            dt_width,
            sort_order,
        }
    }

    /// Encode a batch of sorted records into a compressed leaflet.
    ///
    /// Records must be pre-sorted in `self.sort_order`. Returns the complete
    /// leaflet bytes (header + compressed regions).
    pub fn encode_leaflet(&self, records: &[RunRecord]) -> Vec<u8> {
        self.encode_leaflet_impl(records, None)
    }

    /// Encode a batch of sorted records into a compressed leaflet with Region 3 data.
    ///
    /// Region 3 entries must be pre-sorted in reverse chronological order
    /// (newest first). Pass an empty slice for leaflets with no history.
    pub fn encode_leaflet_with_r3(
        &self,
        records: &[RunRecord],
        region3: &[Region3Entry],
    ) -> Vec<u8> {
        let region3 = if region3.is_empty() {
            None
        } else {
            Some(region3)
        };
        self.encode_leaflet_impl(records, region3)
    }

    fn encode_leaflet_impl(
        &self,
        records: &[RunRecord],
        region3: Option<&[Region3Entry]>,
    ) -> Vec<u8> {
        assert!(!records.is_empty(), "cannot encode empty leaflet");
        let row_count = records.len();

        // ---- Build Region 1 (uncompressed → compressed) ----
        let r1_raw = encode_region1(records, self.p_width, self.sort_order);
        let r1_compressed =
            zstd::bulk::compress(&r1_raw, self.zstd_level).expect("zstd compress region 1");

        // ---- Build Region 2 (uncompressed → compressed) ----
        let r2_raw = encode_region2(records, self.dt_width);
        let r2_compressed =
            zstd::bulk::compress(&r2_raw, self.zstd_level).expect("zstd compress region 2");

        // ---- Build Region 3 (uncompressed → compressed) ----
        let (r3_compressed, r3_uncompressed_len) = match region3 {
            None => (Vec::new(), 0u32),
            Some(entries) => {
                let r3_raw = encode_region3(entries, self.p_width, self.dt_width);
                let r3_uncomp_len = r3_raw.len() as u32;
                let r3_comp =
                    zstd::bulk::compress(&r3_raw, self.zstd_level).expect("zstd compress region 3");
                (r3_comp, r3_uncomp_len)
            }
        };

        // ---- Compute offsets ----
        let region1_offset = LEAFLET_HEADER_LEN as u32;
        let region2_offset = region1_offset + r1_compressed.len() as u32;
        let region3_offset = region2_offset + r2_compressed.len() as u32;

        // ---- Build header ----
        let header = LeafletHeader {
            row_count: row_count as u32,
            region1_offset,
            region1_compressed_len: r1_compressed.len() as u32,
            region1_uncompressed_len: r1_raw.len() as u32,
            region2_offset,
            region2_compressed_len: r2_compressed.len() as u32,
            region2_uncompressed_len: r2_raw.len() as u32,
            region3_offset,
            region3_compressed_len: r3_compressed.len() as u32,
            region3_uncompressed_len: r3_uncompressed_len,
            first_s_id: records[0].s_id.as_u64(),
            first_p_id: records[0].p_id,
            first_o_kind: records[0].o_kind,
            first_o_key: records[0].o_key,
        };

        // ---- Assemble ----
        let total_len = region3_offset as usize + r3_compressed.len();
        let mut out = vec![0u8; total_len];
        header.write_to(&mut out[..LEAFLET_HEADER_LEN]);
        out[region1_offset as usize..region2_offset as usize].copy_from_slice(&r1_compressed);
        out[region2_offset as usize..region3_offset as usize].copy_from_slice(&r2_compressed);
        if !r3_compressed.is_empty() {
            out[region3_offset as usize..total_len].copy_from_slice(&r3_compressed);
        }

        out
    }
}

impl Default for LeafletEncoder {
    fn default() -> Self {
        Self::new(1)
    }
}

/// Determine the minimum byte width needed to represent a value.
pub fn width_for_max(max_val: u32) -> u8 {
    if max_val <= 0xFF {
        1
    } else if max_val <= 0xFFFF {
        2
    } else {
        4
    }
}

/// Determine p_width: 2 (u16) or 4 (u32).
pub fn p_width_for_max(max_p_id: u32) -> u8 {
    if max_p_id <= 0xFFFF {
        2
    } else {
        4
    }
}

/// Determine dt_width: 1 (u8), 2 (u16), or 4 (u32).
pub fn dt_width_for_max(max_dt: u32) -> u8 {
    // Datatypes widen from u8 → u16 when needed.
    // (dt is only a tie-break key; most datasets remain within u8.)
    if max_dt <= 0xFF {
        1
    } else {
        2
    }
}

// ============================================================================
// Region 1 encoding (order-dependent)
// ============================================================================

/// Encode Region 1 for the given sort order.
fn encode_region1(records: &[RunRecord], p_width: u8, order: RunSortOrder) -> Vec<u8> {
    match order {
        RunSortOrder::Spot => encode_region1_spot(records, p_width),
        RunSortOrder::Psot => encode_region1_psot(records),
        RunSortOrder::Post => encode_region1_post(records),
        RunSortOrder::Opst => encode_region1_opst(records, p_width),
    }
}

// ---- Shared RLE helpers ----

trait LeKey: Copy + Eq {
    fn write_le(self, buf: &mut Vec<u8>);
    fn read_le(data: &[u8], pos: &mut usize) -> io::Result<Self>;
}

impl LeKey for u32 {
    #[inline]
    fn write_le(self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
    #[inline]
    fn read_le(data: &[u8], pos: &mut usize) -> io::Result<Self> {
        let bytes = checked_range(data, *pos, 4, "region 1: u32 RLE key")?;
        *pos += 4;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl LeKey for u64 {
    #[inline]
    fn write_le(self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
    #[inline]
    fn read_le(data: &[u8], pos: &mut usize) -> io::Result<Self> {
        let bytes = checked_range(data, *pos, 8, "region 1: u64 RLE key")?;
        *pos += 8;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

fn build_rle<K: Copy + Eq>(records: &[RunRecord], key_fn: fn(&RunRecord) -> K) -> Vec<(K, u32)> {
    let mut runs: Vec<(K, u32)> = Vec::new();
    let mut current = key_fn(&records[0]);
    let mut run_len: u32 = 1;
    for r in &records[1..] {
        let k = key_fn(r);
        if k == current {
            run_len += 1;
        } else {
            runs.push((current, run_len));
            current = k;
            run_len = 1;
        }
    }
    runs.push((current, run_len));
    runs
}

/// Write RLE runs to buffer: `count: u32`, then `[(key: K, len: u32)] × count`.
fn write_rle<K: LeKey>(buf: &mut Vec<u8>, runs: &[(K, u32)]) {
    buf.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for &(key, len) in runs {
        key.write_le(buf);
        buf.extend_from_slice(&len.to_le_bytes());
    }
}

/// Write a variable-width p_id column.
fn write_col_p_id(buf: &mut Vec<u8>, records: &[RunRecord], p_width: u8) {
    match p_width {
        2 => {
            for r in records {
                assert!(
                    r.p_id <= 0xFFFF,
                    "p_id {} exceeds u16 max but p_width=2; widen to p_width=4",
                    r.p_id,
                );
                buf.extend_from_slice(&(r.p_id as u16).to_le_bytes());
            }
        }
        4 => {
            for r in records {
                buf.extend_from_slice(&r.p_id.to_le_bytes());
            }
        }
        _ => unreachable!("invalid p_width: {}", p_width),
    }
}

/// Write a fixed u8 column.
fn write_col_u8(buf: &mut Vec<u8>, records: &[RunRecord], field_fn: fn(&RunRecord) -> u8) {
    for r in records {
        buf.push(field_fn(r));
    }
}

/// Write a fixed u64 column.
fn write_col_u64(buf: &mut Vec<u8>, records: &[RunRecord], field_fn: fn(&RunRecord) -> u64) {
    for r in records {
        buf.extend_from_slice(&field_fn(r).to_le_bytes());
    }
}

// ---- Per-order encode functions ----

/// SPOT Region 1: RLE(s_id:u64), p_id[pw], o_kind[u8], o_key[u64]
fn encode_region1_spot(records: &[RunRecord], p_width: u8) -> Vec<u8> {
    let rle = build_rle(records, |r| r.s_id.as_u64());
    let row_count = records.len();
    let buf_size = 4 + rle.len() * 12 + row_count * (p_width as usize) + row_count + row_count * 8;
    let mut buf = Vec::with_capacity(buf_size);
    write_rle(&mut buf, &rle);
    write_col_p_id(&mut buf, records, p_width);
    write_col_u8(&mut buf, records, |r| r.o_kind);
    write_col_u64(&mut buf, records, |r| r.o_key);
    buf
}

/// PSOT Region 1: RLE(p_id:u32), s_id[u64], o_kind[u8], o_key[u64]
fn encode_region1_psot(records: &[RunRecord]) -> Vec<u8> {
    let rle = build_rle(records, |r| r.p_id);
    let row_count = records.len();
    let buf_size = 4 + rle.len() * 8 + row_count * 8 + row_count + row_count * 8;
    let mut buf = Vec::with_capacity(buf_size);
    write_rle(&mut buf, &rle);
    write_col_u64(&mut buf, records, |r| r.s_id.as_u64());
    write_col_u8(&mut buf, records, |r| r.o_kind);
    write_col_u64(&mut buf, records, |r| r.o_key);
    buf
}

/// POST Region 1: RLE(p_id:u32), o_kind[u8], o_key[u64], s_id[u64]
fn encode_region1_post(records: &[RunRecord]) -> Vec<u8> {
    let rle = build_rle(records, |r| r.p_id);
    let row_count = records.len();
    let buf_size = 4 + rle.len() * 8 + row_count + row_count * 8 + row_count * 8;
    let mut buf = Vec::with_capacity(buf_size);
    write_rle(&mut buf, &rle);
    write_col_u8(&mut buf, records, |r| r.o_kind);
    write_col_u64(&mut buf, records, |r| r.o_key);
    write_col_u64(&mut buf, records, |r| r.s_id.as_u64());
    buf
}

/// OPST Region 1: o_kind[u8], RLE(o_key:u64), p_id[pw], s_id[u64]
fn encode_region1_opst(records: &[RunRecord], p_width: u8) -> Vec<u8> {
    let rle = build_rle(records, |r| r.o_key);
    let row_count = records.len();
    // u64 RLE entries are 12 bytes each (8 key + 4 count)
    let buf_size = row_count + 4 + rle.len() * 12 + row_count * (p_width as usize) + row_count * 8;
    let mut buf = Vec::with_capacity(buf_size);
    write_col_u8(&mut buf, records, |r| r.o_kind);
    write_rle(&mut buf, &rle);
    write_col_p_id(&mut buf, records, p_width);
    write_col_u64(&mut buf, records, |r| r.s_id.as_u64());
    buf
}

// ============================================================================
// Region 2 encoding: dt[] + t[] + lang bitmap + i bitmap
// ============================================================================

/// Region 2 flags.
const R2_FLAG_HAS_LANG: u8 = 0x01;
const R2_FLAG_HAS_I: u8 = 0x02;

/// Choose the narrowest unsigned width that fits `max_val`.
fn i_width_for_max(max_val: u32) -> u8 {
    if max_val <= u8::MAX as u32 {
        1
    } else if max_val <= u16::MAX as u32 {
        2
    } else {
        4
    }
}

/// Encode Region 2 (uncompressed layout, v2 format).
///
/// Format:
/// ```text
/// region2_flags: u8          (bit0=has_lang, bit1=has_i)
/// (i_width: u8)              (only if has_i; 1/2/4)
/// dt: [dt_width] × N
/// t:  [u32] × N              (4 bytes each)
/// if has_lang:
///   lang_count: u32, lang_pos: [u16], lang_val: [u16]
/// if has_i:
///   i_count: u32, i_pos: [u16], i_val: [i_width]
/// ```
fn encode_region2(records: &[RunRecord], dt_width: u8) -> Vec<u8> {
    let row_count = records.len();
    debug_assert!(
        row_count <= u16::MAX as usize,
        "leaflet row_count {} exceeds u16::MAX (sparse positions overflow)",
        row_count
    );

    // Scan for sparse entries and compute i_width
    let mut lang_positions = Vec::new();
    let mut lang_values = Vec::new();
    let mut i_positions = Vec::new();
    let mut i_values_raw = Vec::new();
    let mut max_i: u32 = 0;

    for (idx, r) in records.iter().enumerate() {
        if r.lang_id != 0 {
            lang_positions.push(idx as u16);
            lang_values.push(r.lang_id);
        }
        if r.i != LIST_INDEX_NONE {
            i_positions.push(idx as u16);
            i_values_raw.push(r.i);
            if r.i > max_i {
                max_i = r.i;
            }
        }
    }

    let has_lang = !lang_positions.is_empty();
    let has_i = !i_positions.is_empty();
    let i_width = if has_i { i_width_for_max(max_i) } else { 0 };

    // Build flags
    let mut flags: u8 = 0;
    if has_lang {
        flags |= R2_FLAG_HAS_LANG;
    }
    if has_i {
        flags |= R2_FLAG_HAS_I;
    }

    // Estimate buffer size
    let header_size = 1 + if has_i { 1 } else { 0 };
    let dt_size = row_count * (dt_width as usize);
    let t_size = row_count * 4;
    let lang_size = if has_lang {
        4 + lang_positions.len() * 4
    } else {
        0
    };
    let i_size = if has_i {
        4 + i_positions.len() * (2 + i_width as usize)
    } else {
        0
    };
    let mut buf = Vec::with_capacity(header_size + dt_size + t_size + lang_size + i_size);

    // Write flags
    buf.push(flags);
    if has_i {
        buf.push(i_width);
    }

    // dt array (variable width: u8 or u16)
    match dt_width {
        1 => {
            for r in records {
                assert!(
                    r.dt <= 0xFF,
                    "dt {} exceeds u8 max but dt_width=1; widen to dt_width=2",
                    r.dt
                );
                buf.push(r.dt as u8);
            }
        }
        2 => {
            for r in records {
                buf.extend_from_slice(&r.dt.to_le_bytes());
            }
        }
        _ => unreachable!("invalid dt_width: {}", dt_width),
    }

    // t array (u32 per row)
    for r in records {
        buf.extend_from_slice(&r.t.to_le_bytes());
    }

    // lang: counted sparse (positions + values)
    if has_lang {
        buf.extend_from_slice(&(lang_positions.len() as u32).to_le_bytes());
        for &p in &lang_positions {
            buf.extend_from_slice(&p.to_le_bytes());
        }
        for &v in &lang_values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }

    // i: counted sparse (positions + variable-width values)
    if has_i {
        buf.extend_from_slice(&(i_positions.len() as u32).to_le_bytes());
        for &p in &i_positions {
            buf.extend_from_slice(&p.to_le_bytes());
        }
        match i_width {
            1 => {
                for &v in &i_values_raw {
                    buf.push(v as u8);
                }
            }
            2 => {
                for &v in &i_values_raw {
                    buf.extend_from_slice(&(v as u16).to_le_bytes());
                }
            }
            4 => {
                for &v in &i_values_raw {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            _ => unreachable!("invalid i_width: {}", i_width),
        }
    }

    buf
}

// ============================================================================
// Region 3: history journal (operation log)
// ============================================================================

/// A single Region 3 history entry.
///
/// Region 3 is an operation log stored in **reverse chronological order**
/// (newest first). Each entry is a self-contained fact tuple with separate
/// `t` (unsigned) and `op` fields.
///
/// On-disk wire format is variable-stride (depends on p_width, dt_width,
/// has_lang, has_i flags set in the Region 3 header).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Region3Entry {
    pub s_id: u64,
    pub p_id: u32,
    pub o_kind: u8,
    pub o_key: u64,
    pub t: u32,
    pub op: u8, // 1 = assert, 0 = retract
    pub dt: u16,
    pub lang_id: u16,
    pub i: i32, // ListIndex::none() = i32::MIN if none
}

impl Region3Entry {
    /// Build from a `RunRecord`.
    ///
    /// Converts `i` from u32 sentinel (LIST_INDEX_NONE) to i32 sentinel
    /// (ListIndex::none()) at the leaflet boundary.
    pub fn from_run_record(r: &RunRecord) -> Self {
        Self {
            s_id: r.s_id.as_u64(),
            p_id: r.p_id,
            o_kind: r.o_kind,
            o_key: r.o_key,
            t: r.t,
            op: r.op,
            dt: r.dt,
            lang_id: r.lang_id,
            i: if r.i == LIST_INDEX_NONE {
                ListIndex::none().as_i32()
            } else {
                debug_assert!(
                    r.i <= i32::MAX as u32,
                    "list index {} exceeds i32::MAX, would wrap in Region3Entry",
                    r.i
                );
                r.i as i32
            },
        }
    }

    /// Returns `true` if this entry is an assert.
    #[inline]
    pub fn is_assert(&self) -> bool {
        self.op == 1
    }

    /// Returns the transaction number as u64.
    #[inline]
    pub fn abs_t(&self) -> u64 {
        self.t as u64
    }
}

/// Region 3 flags.
const R3_FLAG_HAS_LANG: u8 = 0x01;
const R3_FLAG_HAS_I: u8 = 0x02;

/// Region 3 header size (entry_count + flags + p_width + dt_width + i_width).
const R3_HEADER_BYTES: usize = 8;

/// Encode Region 3 entries into the v2 wire format (before compression).
///
/// Wire format (self-describing header + fixed-stride entries):
/// ```text
/// entry_count: u32, r3_flags: u8, p_width: u8, dt_width: u8, i_width: u8
/// [entries × entry_count]:
///   s_id[8], p_id[pw], o_kind[1], o_key[8], t[4], dt[dw], op[1],
///   (lang_id[2] if has_lang), (i[iw] if has_i)
/// ```
///
/// Entries must be in reverse chronological order (newest first).
pub fn encode_region3(entries: &[Region3Entry], p_width: u8, dt_width: u8) -> Vec<u8> {
    if entries.is_empty() {
        let mut buf = Vec::with_capacity(R3_HEADER_BYTES);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 4]); // flags, p_width, dt_width, i_width
        return buf;
    }

    // Scan for presence and compute i_width
    let has_lang = entries.iter().any(|e| e.lang_id != 0);
    let has_i = entries.iter().any(|e| e.i != ListIndex::none().as_i32());
    let max_i = entries
        .iter()
        .filter(|e| e.i != ListIndex::none().as_i32())
        .map(|e| e.i as u32)
        .max()
        .unwrap_or(0);
    // +1 for offset encoding (0 reserved for sentinel)
    let i_width = if has_i { i_width_for_max(max_i + 1) } else { 0 };

    let mut flags: u8 = 0;
    if has_lang {
        flags |= R3_FLAG_HAS_LANG;
    }
    if has_i {
        flags |= R3_FLAG_HAS_I;
    }

    // Compute stride
    let stride = 22
        + (p_width as usize)
        + (dt_width as usize)
        + if has_lang { 2 } else { 0 }
        + if has_i { i_width as usize } else { 0 };

    let buf_size = R3_HEADER_BYTES + entries.len() * stride;
    let mut buf = Vec::with_capacity(buf_size);

    // Header
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    buf.push(flags);
    buf.push(p_width);
    buf.push(dt_width);
    buf.push(i_width);

    // Entries
    for e in entries {
        buf.extend_from_slice(&e.s_id.to_le_bytes());
        match p_width {
            2 => buf.extend_from_slice(&(e.p_id as u16).to_le_bytes()),
            4 => buf.extend_from_slice(&e.p_id.to_le_bytes()),
            _ => unreachable!("invalid p_width: {}", p_width),
        }
        buf.push(e.o_kind);
        buf.extend_from_slice(&e.o_key.to_le_bytes());
        buf.extend_from_slice(&e.t.to_le_bytes());
        match dt_width {
            1 => buf.push(e.dt as u8),
            2 => buf.extend_from_slice(&e.dt.to_le_bytes()),
            _ => unreachable!("invalid dt_width: {}", dt_width),
        }
        buf.push(e.op);
        if has_lang {
            buf.extend_from_slice(&e.lang_id.to_le_bytes());
        }
        if has_i {
            // Offset encoding: store (i + 1) so that 0 means "absent" (sentinel).
            // This distinguishes "no list index" from list index 0.
            let i_val = if e.i == ListIndex::none().as_i32() {
                0u32
            } else {
                (e.i as u32) + 1
            };
            match i_width {
                1 => buf.push(i_val as u8),
                2 => buf.extend_from_slice(&(i_val as u16).to_le_bytes()),
                4 => buf.extend_from_slice(&i_val.to_le_bytes()),
                _ => unreachable!("invalid i_width: {}", i_width),
            }
        }
    }
    buf
}

/// Decode Region 3 entries from uncompressed v2 bytes.
fn decode_region3(data: &[u8]) -> io::Result<Vec<Region3Entry>> {
    if data.len() < R3_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "region 3: header truncated",
        ));
    }
    let entry_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let flags = data[4];
    let p_width = data[5] as usize;
    let dt_width = data[6] as usize;
    let i_width = data[7] as usize;

    let has_lang = flags & R3_FLAG_HAS_LANG != 0;
    let has_i = flags & R3_FLAG_HAS_I != 0;

    if entry_count == 0 {
        return Ok(Vec::new());
    }

    // Compute stride and validate
    let stride =
        22 + p_width + dt_width + if has_lang { 2 } else { 0 } + if has_i { i_width } else { 0 };

    let expected_len = R3_HEADER_BYTES + entry_count * stride;
    if data.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "region 3: truncated (have {} bytes, need {} for {} entries, stride {})",
                data.len(),
                expected_len,
                entry_count,
                stride
            ),
        ));
    }

    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = R3_HEADER_BYTES;
    for _ in 0..entry_count {
        let s_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let p_id = match p_width {
            2 => {
                let v = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as u32;
                pos += 2;
                v
            }
            4 => {
                let v = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                v
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("region 3: invalid p_width {}", p_width),
                ))
            }
        };
        let o_kind = data[pos];
        pos += 1;
        let o_key = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let t = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let dt = match dt_width {
            1 => {
                let v = data[pos] as u16;
                pos += 1;
                v
            }
            2 => {
                let v = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
                pos += 2;
                v
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("region 3: invalid dt_width {}", dt_width),
                ))
            }
        };
        let op = data[pos];
        pos += 1;
        let lang_id = if has_lang {
            let v = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
            pos += 2;
            v
        } else {
            0
        };
        let i = if has_i {
            let v = match i_width {
                1 => data[pos] as u32,
                2 => u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as u32,
                4 => u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("region 3: invalid i_width {}", i_width),
                    ))
                }
            };
            pos += i_width;
            // Offset encoding: 0 means absent (sentinel), otherwise value is stored + 1
            if v == 0 {
                ListIndex::none().as_i32()
            } else {
                (v - 1) as i32
            }
        } else {
            ListIndex::none().as_i32()
        };
        entries.push(Region3Entry {
            s_id,
            p_id,
            o_kind,
            o_key,
            t,
            op,
            dt,
            lang_id,
            i,
        });
    }

    Ok(entries)
}

/// Decode Region 3 of a leaflet (history journal).
///
/// Returns an empty `Vec` if the leaflet has no Region 3 data
/// (`region3_compressed_len == 0`).
pub fn decode_leaflet_region3(
    data: &[u8],
    header: &LeafletHeader,
) -> io::Result<Vec<Region3Entry>> {
    if header.region3_compressed_len == 0 {
        return Ok(Vec::new());
    }
    if header.region3_uncompressed_len == 0 {
        return Err(invalid_data(
            "leaflet: region3_compressed_len > 0 but region3_uncompressed_len == 0",
        ));
    }

    let r3_start = header.region3_offset as usize;
    let r3_comp = checked_range(
        data,
        r3_start,
        header.region3_compressed_len as usize,
        "leaflet: region 3",
    )?;
    let r3_raw = zstd_decompress(
        "leaflet: region 3",
        r3_comp,
        header.region3_uncompressed_len as usize,
    )?;

    decode_region3(&r3_raw)
}

// ============================================================================
// Decoder
// ============================================================================

/// Decoded leaflet data (Region 1 + Region 2).
#[derive(Debug)]
pub struct DecodedLeaflet {
    pub row_count: usize,
    pub s_ids: Vec<u64>,
    pub p_ids: Vec<u32>,
    /// Object kind discriminant per row (see `ObjKind`).
    pub o_kinds: Vec<u8>,
    /// Object key payload per row (interpretation depends on `o_kinds`).
    pub o_keys: Vec<u64>,
    /// Datatype dict index per row. Conceptually u32; stored on disk at dt_width bytes.
    pub dt_values: Vec<u32>,
    /// Transaction number per row (u32, narrowed from i64 at encode boundary).
    pub t_values: Vec<u32>,
    /// Sparse lang_id column (None when no rows have lang).
    pub lang: Option<SparseU16Column>,
    /// Sparse list-index column (None when no rows have i).
    pub i_col: Option<SparseIColumn>,
}

/// Decode only Region 1 of a leaflet (core columns), returning the parsed header.
///
/// This is used by `BinaryCursor` to apply integer-ID filters before paying to
/// decompress/parse Region 2 metadata (dt/t/lang/i).
///
/// Returns `(header, s_ids, p_ids, o_kinds, o_keys)`.
pub fn decode_leaflet_region1(
    data: &[u8],
    p_width: u8,
    sort_order: RunSortOrder,
) -> io::Result<Region1WithHeader> {
    let p_width = validate_p_width(p_width)?;

    let header = LeafletHeader::read_from(data)?;
    let row_count = header.row_count as usize;

    // ---- Decompress Region 1 ----
    let r1_start = header.region1_offset as usize;
    let r1_comp = checked_range(
        data,
        r1_start,
        header.region1_compressed_len as usize,
        "leaflet: region 1",
    )?;
    let r1_raw = zstd_decompress(
        "leaflet: region 1",
        r1_comp,
        header.region1_uncompressed_len as usize,
    )?;

    // ---- Decode Region 1 ----
    let (s_ids, p_ids, o_kinds, o_keys) = decode_region1(&r1_raw, row_count, p_width, sort_order)?;
    Ok((header, s_ids, p_ids, o_kinds, o_keys))
}

/// Decode Region 2 of a leaflet (metadata columns) using a previously-read header.
///
/// Returns sparse `DecodedRegion2` — lang and i are NOT densified.
/// Callers should only invoke this when they actually need dt/t/lang/i for the
/// current leaflet (e.g., after Region 1 filtering yields at least one match).
pub fn decode_leaflet_region2(
    data: &[u8],
    header: &LeafletHeader,
    dt_width: u8,
) -> io::Result<DecodedRegion2> {
    let dt_width = validate_dt_width(dt_width)?;
    let row_count = header.row_count as usize;

    // ---- Decompress Region 2 ----
    let r2_start = header.region2_offset as usize;
    let r2_comp = checked_range(
        data,
        r2_start,
        header.region2_compressed_len as usize,
        "leaflet: region 2",
    )?;
    let r2_raw = zstd_decompress(
        "leaflet: region 2",
        r2_comp,
        header.region2_uncompressed_len as usize,
    )?;

    decode_region2(&r2_raw, row_count, dt_width)
}

/// Decode a leaflet using explicit field widths.
///
/// This crate only supports the current leaflet format:
/// - `p_width` is 2 or 4
/// - `dt_width` is 1 or 2
///
/// Any other value is treated as corrupt data.
pub fn decode_leaflet(
    data: &[u8],
    p_width: u8,
    dt_width: u8,
    sort_order: RunSortOrder,
) -> io::Result<DecodedLeaflet> {
    let (header, s_ids, p_ids, o_kinds, o_keys) =
        decode_leaflet_region1(data, p_width, sort_order)?;
    let r2 = decode_leaflet_region2(data, &header, dt_width)?;

    Ok(DecodedLeaflet {
        row_count: header.row_count as usize,
        s_ids,
        p_ids,
        o_kinds,
        o_keys,
        dt_values: r2.dt_values,
        t_values: r2.t_values,
        lang: r2.lang,
        i_col: r2.i_col,
    })
}

// ============================================================================
// Region 1 decoding (order-dependent)
// ============================================================================

/// Decode Region 1 for the given sort order.
///
/// Returns `(s_ids, p_ids, o_kinds, o_keys)` regardless of sort order.
fn decode_region1(
    data: &[u8],
    row_count: usize,
    p_width: u8,
    order: RunSortOrder,
) -> io::Result<Region1Columns> {
    match order {
        RunSortOrder::Spot => decode_region1_spot(data, row_count, p_width),
        RunSortOrder::Psot => decode_region1_psot(data, row_count),
        RunSortOrder::Post => decode_region1_post(data, row_count),
        RunSortOrder::Opst => decode_region1_opst(data, row_count, p_width),
    }
}

// ---- Shared decode helpers ----

/// Decode RLE: read `count: u32`, then `[(key: K, len: u32)] × count`.
fn decode_rle<K: LeKey>(data: &[u8], pos: &mut usize, row_count: usize) -> io::Result<Vec<K>> {
    let header = checked_range(data, *pos, 4, "region 1: RLE header")?;
    let rle_count = u32::from_le_bytes(header.try_into().unwrap()) as usize;
    *pos += 4;

    let mut vals = Vec::with_capacity(row_count);
    for _ in 0..rle_count {
        let key = K::read_le(data, pos)?;
        let run_len_bytes = checked_range(data, *pos, 4, "region 1: RLE run_len")?;
        let run_len = u32::from_le_bytes(run_len_bytes.try_into().unwrap()) as usize;
        *pos += 4;
        vals.extend(std::iter::repeat_n(key, run_len));
    }
    if vals.len() != row_count {
        return Err(invalid_data(format!(
            "region 1: RLE expanded to {} rows, expected {}",
            vals.len(),
            row_count
        )));
    }
    Ok(vals)
}

/// Read a variable-width p_id column.
fn read_col_p_id(
    data: &[u8],
    pos: &mut usize,
    row_count: usize,
    p_width: u8,
) -> io::Result<Vec<u32>> {
    let pw = p_width as usize;
    let mut vals = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        if *pos + pw > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 1: p_id truncated",
            ));
        }
        let val = match p_width {
            2 => u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap()) as u32,
            4 => u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("region 1: unsupported p_width {}", p_width),
                ))
            }
        };
        vals.push(val);
        *pos += pw;
    }
    Ok(vals)
}

/// Read a fixed u8 column.
fn read_col_u8(data: &[u8], pos: &mut usize, row_count: usize) -> io::Result<Vec<u8>> {
    if *pos + row_count > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "region 1: u8 col truncated",
        ));
    }
    let vals = data[*pos..*pos + row_count].to_vec();
    *pos += row_count;
    Ok(vals)
}

/// Read a fixed u64 column.
fn read_col_u64(data: &[u8], pos: &mut usize, row_count: usize) -> io::Result<Vec<u64>> {
    let mut vals = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        if *pos + 8 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 1: u64 col truncated",
            ));
        }
        vals.push(u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap()));
        *pos += 8;
    }
    Ok(vals)
}

// ---- Per-order decode functions ----

/// SPOT: RLE(s_id:u64), p_id[pw], o_kind[u8], o_key[u64]
fn decode_region1_spot(data: &[u8], row_count: usize, p_width: u8) -> io::Result<Region1Columns> {
    let mut pos = 0;
    let s_ids = decode_rle::<u64>(data, &mut pos, row_count)?;
    let p_ids = read_col_p_id(data, &mut pos, row_count, p_width)?;
    let o_kinds = read_col_u8(data, &mut pos, row_count)?;
    let o_keys = read_col_u64(data, &mut pos, row_count)?;
    Ok((s_ids, p_ids, o_kinds, o_keys))
}

/// PSOT: RLE(p_id:u32), s_id[u64], o_kind[u8], o_key[u64]
fn decode_region1_psot(data: &[u8], row_count: usize) -> io::Result<Region1Columns> {
    let mut pos = 0;
    let p_ids = decode_rle::<u32>(data, &mut pos, row_count)?;
    let s_ids = read_col_u64(data, &mut pos, row_count)?;
    let o_kinds = read_col_u8(data, &mut pos, row_count)?;
    let o_keys = read_col_u64(data, &mut pos, row_count)?;
    Ok((s_ids, p_ids, o_kinds, o_keys))
}

/// POST: RLE(p_id:u32), o_kind[u8], o_key[u64], s_id[u64]
fn decode_region1_post(data: &[u8], row_count: usize) -> io::Result<Region1Columns> {
    let mut pos = 0;
    let p_ids = decode_rle::<u32>(data, &mut pos, row_count)?;
    let o_kinds = read_col_u8(data, &mut pos, row_count)?;
    let o_keys = read_col_u64(data, &mut pos, row_count)?;
    let s_ids = read_col_u64(data, &mut pos, row_count)?;
    Ok((s_ids, p_ids, o_kinds, o_keys))
}

/// OPST: o_kind[u8], RLE(o_key:u64), p_id[pw], s_id[u64]
fn decode_region1_opst(data: &[u8], row_count: usize, p_width: u8) -> io::Result<Region1Columns> {
    let mut pos = 0;
    let o_kinds = read_col_u8(data, &mut pos, row_count)?;
    let o_keys = decode_rle::<u64>(data, &mut pos, row_count)?;
    let p_ids = read_col_p_id(data, &mut pos, row_count, p_width)?;
    let s_ids = read_col_u64(data, &mut pos, row_count)?;
    Ok((s_ids, p_ids, o_kinds, o_keys))
}

/// Decode Region 2 (v2 format): flags + dt[] + t[] + sparse lang + sparse i.
///
/// Returns sparse structures directly — no densification.
fn decode_region2(data: &[u8], row_count: usize, dt_width: u8) -> io::Result<DecodedRegion2> {
    let dt_width =
        validate_dt_width(dt_width).map_err(|e| invalid_data(format!("region 2: {e}")))?;
    let dw = dt_width as usize;

    if data.is_empty() {
        return Err(invalid_data("region 2: empty data"));
    }

    let mut pos = 0;

    // Read flags
    let flags = data[pos];
    pos += 1;
    let has_lang = flags & R2_FLAG_HAS_LANG != 0;
    let has_i = flags & R2_FLAG_HAS_I != 0;

    // Read i_width (only if has_i)
    let i_width = if has_i {
        if pos >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: i_width truncated",
            ));
        }
        let w = data[pos];
        pos += 1;
        if w != 1 && w != 2 && w != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("region 2: invalid i_width {} (expected 1, 2, or 4)", w),
            ));
        }
        w as usize
    } else {
        0
    };

    // dt array (u8/u16, zero-extended to u32)
    let dt_total = row_count * dw;
    if pos + dt_total > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "region 2: dt truncated",
        ));
    }
    let mut dt_values = Vec::with_capacity(row_count);
    match dt_width {
        1 => {
            for _ in 0..row_count {
                dt_values.push(data[pos] as u32);
                pos += 1;
            }
        }
        2 => {
            for _ in 0..row_count {
                let v = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as u32;
                dt_values.push(v);
                pos += 2;
            }
        }
        _ => unreachable!("invalid dt_width: {}", dt_width),
    }

    // t array (u32 per row)
    let t_total = row_count * 4;
    if pos + t_total > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "region 2: t truncated",
        ));
    }
    let mut t_values = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        t_values.push(u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
        pos += 4;
    }

    // lang: counted sparse (positions + values)
    let lang = if has_lang {
        if pos + 4 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: lang_count truncated",
            ));
        }
        let lang_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // positions
        let positions_bytes = lang_count * 2;
        if pos + positions_bytes > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: lang positions truncated",
            ));
        }
        let mut positions = Vec::with_capacity(lang_count);
        for _ in 0..lang_count {
            positions.push(u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()));
            pos += 2;
        }

        // values
        let values_bytes = lang_count * 2;
        if pos + values_bytes > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: lang values truncated",
            ));
        }
        let mut values = Vec::with_capacity(lang_count);
        for _ in 0..lang_count {
            values.push(u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()));
            pos += 2;
        }

        Some(SparseU16Column {
            positions: positions.into(),
            values: values.into(),
        })
    } else {
        None
    };

    // i: counted sparse (positions + variable-width values)
    let i_col = if has_i {
        if pos + 4 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: i_count truncated",
            ));
        }
        let i_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // positions
        let positions_bytes = i_count * 2;
        if pos + positions_bytes > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: i positions truncated",
            ));
        }
        let mut positions = Vec::with_capacity(i_count);
        for _ in 0..i_count {
            positions.push(u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()));
            pos += 2;
        }

        // values (variable width)
        let values_bytes = i_count * i_width;
        if pos + values_bytes > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region 2: i values truncated",
            ));
        }
        let positions_arc: Arc<[u16]> = positions.into();
        let col = match i_width {
            1 => {
                let mut values = Vec::with_capacity(i_count);
                for _ in 0..i_count {
                    values.push(data[pos]);
                    pos += 1;
                }
                SparseIColumn::U8 {
                    positions: positions_arc,
                    values: values.into(),
                }
            }
            2 => {
                let mut values = Vec::with_capacity(i_count);
                for _ in 0..i_count {
                    values.push(u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()));
                    pos += 2;
                }
                SparseIColumn::U16 {
                    positions: positions_arc,
                    values: values.into(),
                }
            }
            4 => {
                let mut values = Vec::with_capacity(i_count);
                for _ in 0..i_count {
                    values.push(u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
                    pos += 4;
                }
                SparseIColumn::U32 {
                    positions: positions_arc,
                    values: values.into(),
                }
            }
            _ => unreachable!("i_width already validated"),
        };
        Some(col)
    } else {
        None
    };

    Ok(DecodedRegion2 {
        dt_values,
        t_values,
        lang,
        i_col,
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
    fn test_encode_decode_round_trip() {
        let records = vec![
            make_record(1, 1, 10, 1),
            make_record(1, 2, 20, 1),
            make_record(2, 1, 30, 2),
            make_record(3, 1, 40, 2),
        ];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);

        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Spot).unwrap();
        assert_eq!(decoded.row_count, 4);
        assert_eq!(decoded.s_ids, vec![1, 1, 2, 3]);
        assert_eq!(decoded.p_ids, vec![1, 2, 1, 1]);
        assert_eq!(decoded.o_kinds[0], ObjKind::NUM_INT.as_u8());
        assert_eq!(decoded.o_keys[0], ObjKey::encode_i64(10).as_u64());
        assert_eq!(decoded.o_keys[1], ObjKey::encode_i64(20).as_u64());
        assert_eq!(decoded.t_values, vec![1u32, 1, 2, 2]);
        assert_eq!(
            decoded.dt_values,
            vec![DatatypeDictId::INTEGER.as_u16() as u32; 4]
        );
    }

    #[test]
    fn test_rle_compression() {
        // Subject 1 has 3 facts, subject 2 has 2 facts → 2 RLE runs
        let records = vec![
            make_record(1, 1, 10, 1),
            make_record(1, 2, 20, 1),
            make_record(1, 3, 30, 1),
            make_record(2, 1, 40, 1),
            make_record(2, 2, 50, 1),
        ];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Spot).unwrap();

        assert_eq!(decoded.row_count, 5);
        assert_eq!(decoded.s_ids, vec![1, 1, 1, 2, 2]);
    }

    #[test]
    fn test_sparse_lang_and_i() {
        let records = vec![
            // Normal record (no lang, no list index)
            make_record(1, 1, 10, 1),
            // Lang string
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(5),
                1,
                true,
                DatatypeDictId::LANG_STRING.as_u16(),
                3,
                None,
            ),
            // List entry
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(42),
                1,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                Some(7),
            ),
            // Both lang and list index
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(8),
                1,
                true,
                DatatypeDictId::LANG_STRING.as_u16(),
                2,
                Some(3),
            ),
        ];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Spot).unwrap();

        assert_eq!(decoded.row_count, 4);
        // lang: rows 1 and 3 have lang_ids 3 and 2
        let lang = decoded.lang.as_ref().expect("should have lang");
        assert_eq!(lang.count(), 2);
        assert_eq!(lang.get(0), 0); // no lang
        assert_eq!(lang.get(1), 3); // lang_id = 3
        assert_eq!(lang.get(2), 0); // no lang
        assert_eq!(lang.get(3), 2); // lang_id = 2
                                    // i: rows 2 and 3 have list indices 7 and 3
        let i_col = decoded.i_col.as_ref().expect("should have i");
        assert_eq!(i_col.count(), 2);
        assert_eq!(i_col.get(0), ListIndex::none().as_i32());
        assert_eq!(i_col.get(1), ListIndex::none().as_i32());
        assert_eq!(i_col.get(2), 7);
        assert_eq!(i_col.get(3), 3);
    }

    #[test]
    fn test_header_fields() {
        let records = vec![make_record(5, 3, 100, 1), make_record(5, 4, 200, 2)];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);
        let header = LeafletHeader::read_from(&encoded).unwrap();

        assert_eq!(header.row_count, 2);
        assert_eq!(header.first_s_id, 5);
        assert_eq!(header.first_p_id, 3);
        assert_eq!(header.first_o_kind, ObjKind::NUM_INT.as_u8());
        assert_eq!(header.first_o_key, ObjKey::encode_i64(100).as_u64());
        assert_eq!(header.region3_compressed_len, 0);
    }

    #[test]
    fn test_single_record_leaflet() {
        let records = vec![make_record(1, 1, 42, 1)];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Spot).unwrap();

        assert_eq!(decoded.row_count, 1);
        assert_eq!(decoded.s_ids, vec![1]);
        assert_eq!(decoded.p_ids, vec![1]);
    }

    // ---- Multi-order round-trip tests ----

    fn make_records_for_order_tests() -> Vec<RunRecord> {
        vec![
            // s=1, p=5, o=100
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                5,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(100),
                1,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
            // s=1, p=5, o=200
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                5,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(200),
                2,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
            // s=2, p=3, o=50
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                3,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(50),
                1,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
            // s=3, p=5, o=100
            RunRecord::new(
                0,
                SubjectId::from_u64(3),
                5,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(100),
                3,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
        ]
    }

    fn assert_round_trip(records: &[RunRecord], decoded: &DecodedLeaflet) {
        assert_eq!(decoded.row_count, records.len());
        for (i, r) in records.iter().enumerate() {
            assert_eq!(
                decoded.s_ids[i],
                r.s_id.as_u64(),
                "s_id mismatch at row {}",
                i
            );
            assert_eq!(decoded.p_ids[i], r.p_id, "p_id mismatch at row {}", i);
            assert_eq!(decoded.o_kinds[i], r.o_kind, "o_kind mismatch at row {}", i);
            assert_eq!(decoded.o_keys[i], r.o_key, "o_key mismatch at row {}", i);
            assert_eq!(decoded.t_values[i], r.t, "t mismatch at row {}", i);
            assert_eq!(
                decoded.dt_values[i], r.dt as u32,
                "dt mismatch at row {}",
                i
            );
        }
    }

    #[test]
    fn test_psot_encode_decode_round_trip() {
        let records = make_records_for_order_tests();
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Psot);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Psot).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_post_encode_decode_round_trip() {
        let records = make_records_for_order_tests();
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Post);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Post).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_opst_encode_decode_round_trip() {
        // OPST uses u64 RLE on o_key values
        let records = vec![
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                5,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(10),
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                5,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(10),
                2,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(3),
                3,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(20),
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
        ];
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Opst);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Opst).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_opst_single_record() {
        let records = vec![RunRecord::new(
            0,
            SubjectId::from_u64(1),
            5,
            ObjKind::REF_ID,
            ObjKey::encode_u32_id(42),
            1,
            true,
            DatatypeDictId::ID.as_u16(),
            0,
            None,
        )];
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Opst);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Opst).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_opst_all_same_object() {
        // All records have the same o — collapses to a single RLE entry
        let o_kind = ObjKind::REF_ID;
        let o_key = ObjKey::encode_u32_id(99);
        let records = vec![
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                1,
                o_kind,
                o_key,
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                1,
                o_kind,
                o_key,
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(3),
                2,
                o_kind,
                o_key,
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(4),
                2,
                o_kind,
                o_key,
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
        ];
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Opst);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Opst).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_opst_alternating_objects() {
        // Worst case for RLE: every record has a different o
        let records = vec![
            RunRecord::new(
                0,
                SubjectId::from_u64(1),
                1,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(1),
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(2),
                1,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(2),
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId::from_u64(3),
                1,
                ObjKind::REF_ID,
                ObjKey::encode_u32_id(3),
                1,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
        ];
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Opst);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Opst).unwrap();
        assert_round_trip(&records, &decoded);
    }

    #[test]
    fn test_psot_rle_collapses_predicate() {
        // All records have the same p_id — should collapse to 1 RLE entry
        let records = vec![
            make_record(1, 5, 10, 1),
            make_record(2, 5, 20, 1),
            make_record(3, 5, 30, 1),
        ];
        let encoder = LeafletEncoder::with_widths_and_order(1, 2, 1, RunSortOrder::Psot);
        let encoded = encoder.encode_leaflet(&records);
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Psot).unwrap();
        assert_round_trip(&records, &decoded);
    }

    // ---- Region 3 tests ----

    #[test]
    fn test_region3_empty_round_trip() {
        let entries: Vec<Region3Entry> = vec![];
        let encoded = encode_region3(&entries, 2, 1);
        // Wire: R3_HEADER_BYTES (8 bytes)
        assert_eq!(encoded.len(), 8);
        let decoded = decode_region3(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_region3_multi_entry_round_trip() {
        let entries = vec![
            Region3Entry {
                s_id: 10,
                p_id: 5,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(100).as_u64(),
                t: 42,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 10,
                p_id: 5,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(100).as_u64(),
                t: 40,
                op: 0,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 7,
                p_id: 3,
                o_kind: ObjKind::LEX_ID.as_u8(),
                o_key: ObjKey::encode_u32_id(99).as_u64(),
                t: 38,
                op: 1,
                dt: DatatypeDictId::LANG_STRING.as_u16(),
                lang_id: 2,
                i: 5,
            },
        ];
        let encoded = encode_region3(&entries, 2, 1);
        let decoded = decode_region3(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded, entries);
    }

    #[test]
    fn test_region3_t_op_encoding() {
        // Assert: op=1
        let assert_rec = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            2,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            42,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let e = Region3Entry::from_run_record(&assert_rec);
        assert_eq!(e.t, 42);
        assert_eq!(e.op, 1);
        assert!(e.is_assert());
        assert_eq!(e.abs_t(), 42);

        // Retract: op=0
        let retract_rec = RunRecord::new(
            0,
            SubjectId::from_u64(1),
            2,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(10),
            42,
            false,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );
        let e = Region3Entry::from_run_record(&retract_rec);
        assert_eq!(e.t, 42);
        assert_eq!(e.op, 0);
        assert!(!e.is_assert());
        assert_eq!(e.abs_t(), 42);
    }

    #[test]
    fn test_encode_leaflet_with_r3_empty_region3() {
        // encode_leaflet_with_r3 with empty R3 should produce same result as encode_leaflet
        let records = vec![make_record(1, 1, 10, 1), make_record(1, 2, 20, 1)];
        let encoder = LeafletEncoder::new(1);

        let without_r3 = encoder.encode_leaflet(&records);
        let with_empty_r3 = encoder.encode_leaflet_with_r3(&records, &[]);

        // Headers should both have region3_compressed_len=0
        let h1 = LeafletHeader::read_from(&without_r3).unwrap();
        let h2 = LeafletHeader::read_from(&with_empty_r3).unwrap();
        assert_eq!(h1.region3_compressed_len, 0);
        assert_eq!(h2.region3_compressed_len, 0);
        assert_eq!(h2.region3_uncompressed_len, 0);
    }

    #[test]
    fn test_encode_leaflet_with_r3_full_round_trip() {
        let records = vec![
            make_record(1, 1, 10, 1),
            make_record(1, 2, 20, 2),
            make_record(2, 1, 30, 3),
        ];

        let r3_entries = vec![
            Region3Entry {
                s_id: 1,
                p_id: 2,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(20).as_u64(),
                t: 5,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
            Region3Entry {
                s_id: 1,
                p_id: 2,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(15).as_u64(),
                t: 4,
                op: 0,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: ListIndex::none().as_i32(),
            },
        ];

        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet_with_r3(&records, &r3_entries);

        // Decode Region 1+2 (existing path)
        let decoded = decode_leaflet(&encoded, 2, 1, RunSortOrder::Spot).unwrap();
        assert_eq!(decoded.row_count, 3);
        assert_eq!(decoded.s_ids, vec![1, 1, 2]);

        // Decode Region 3
        let header = LeafletHeader::read_from(&encoded).unwrap();
        assert!(header.region3_compressed_len > 0);
        assert!(header.region3_uncompressed_len > 0);

        let r3_decoded = decode_leaflet_region3(&encoded, &header).unwrap();
        assert_eq!(r3_decoded.len(), 2);
        assert_eq!(r3_decoded, r3_entries);
    }

    #[test]
    fn test_decode_leaflet_region3_empty() {
        // Normal leaflet (no R3) should return empty vec
        let records = vec![make_record(1, 1, 10, 1)];
        let encoder = LeafletEncoder::new(1);
        let encoded = encoder.encode_leaflet(&records);
        let header = LeafletHeader::read_from(&encoded).unwrap();

        let r3 = decode_leaflet_region3(&encoded, &header).unwrap();
        assert!(r3.is_empty());
    }
}
