//! Incremental leaf update: merge novelty into existing FLI2 leaf files.
//!
//! Given one FLI2 leaf file + sorted novelty for that leaf's key range,
//! produces 1+ updated leaf blobs. Untouched leaflets are carried through
//! as raw bytes (no decode/re-encode). Touched leaflets are fully decoded,
//! merged via [`merge_novelty`], and re-encoded.
//!
//! ## Safety invariant
//!
//! If any merge produces `row_count == 0` with non-empty Region 3 (all facts
//! retracted but history remains), returns [`IncrementalLeafError::EmptyLeafletWithHistory`].
//! The caller should fall back to a full rebuild.

use super::novelty_merge::{merge_novelty, MergeInput, MergeOutput};
use fluree_db_binary_index::format::leaf::{
    read_leaf_header, LeafFileHeader, LeafletDirEntry, SortKey, LEAFLET_DIR_ENTRY,
    LEAF_HEADER_FIXED, LEAF_MAGIC, LEAF_VERSION, SORT_KEY_BYTES,
};
use fluree_db_binary_index::format::leaflet::{
    decode_leaflet_region1, decode_leaflet_region2, decode_leaflet_region3, LeafletEncoder,
    LeafletHeader, Region3Entry,
};
use fluree_db_binary_index::format::run_record::{
    cmp_for_order, RunRecord, RunSortOrder, LIST_INDEX_NONE,
};
use fluree_db_core::subject_id::{SubjectId, SubjectIdColumn, SubjectIdEncoding};
use fluree_db_core::ListIndex;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::io;

// ============================================================================
// Public types
// ============================================================================

/// Configuration for an incremental leaf update.
pub struct LeafUpdateInput<'a> {
    /// Raw FLI2 leaf file bytes.
    pub leaf_bytes: &'a [u8],
    /// Novelty records sorted in `order`, sliced to this leaf's key range.
    pub novelty: &'a [RunRecord],
    /// Sort order for this index.
    pub order: RunSortOrder,
    /// Graph ID (for RunRecord reconstruction).
    pub g_id: u16,
    /// zstd compression level.
    pub zstd_level: i32,
    /// Threshold above which a merged leaflet should be split (default: 37_500).
    pub leaflet_split_rows: usize,
    /// Target rows per leaflet after split (default: 25_000).
    pub leaflet_target_rows: usize,
    /// Max leaflets per leaf file (default: 10).
    pub leaflets_per_leaf: usize,
    /// Max total leaflets before splitting the leaf into multiple blobs (default: 20).
    pub leaf_split_leaflets: usize,
}

/// A new leaf blob produced by the update.
pub struct NewLeafBlob {
    /// Complete FLI2 leaf file bytes.
    pub bytes: Vec<u8>,
    /// Content ID (SHA-256 based).
    pub cid: fluree_db_core::ContentId,
    /// First sort key of this leaf (for branch routing).
    pub first_key: RunRecord,
    /// Last sort key of this leaf (for branch routing).
    pub last_key: RunRecord,
    /// Total rows in this leaf.
    pub row_count: u64,
}

/// Output of an incremental leaf update.
pub struct LeafUpdateOutput {
    /// New leaf blobs (usually 1, may be >1 if leaflet count exceeds split threshold).
    pub leaves: Vec<NewLeafBlob>,
}

/// Errors specific to incremental leaf update.
#[derive(Debug)]
pub enum IncrementalLeafError {
    /// A leaflet ended up with 0 current rows but non-empty Region 3.
    /// Caller should fall back to full rebuild.
    EmptyLeafletWithHistory,
    /// I/O or format error.
    Io(io::Error),
}

impl From<io::Error> for IncrementalLeafError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for IncrementalLeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyLeafletWithHistory => write!(
                f,
                "leaflet has 0 rows but non-empty R3 (empty-after-retract)"
            ),
            Self::Io(e) => write!(f, "leaf I/O error: {e}"),
        }
    }
}

impl std::error::Error for IncrementalLeafError {}

// ============================================================================
// Internal types
// ============================================================================

/// Processed leaflet ready for leaf assembly.
struct ProcessedLeaflet {
    /// Encoded leaflet bytes (either passthrough or freshly encoded).
    data: Vec<u8>,
    row_count: u32,
    first_s_id: u64,
    first_p_id: u32,
    /// First record of this leaflet (None for passthrough).
    first_record: Option<RunRecord>,
    /// Last record of this leaflet (None for passthrough).
    last_record: Option<RunRecord>,
}

// ============================================================================
// Main entry point
// ============================================================================

/// Perform an incremental update of a single FLI2 leaf file.
///
/// Merges sorted `novelty` records into the leaf's leaflets. Untouched leaflets
/// are carried through as raw bytes. Returns one or more new leaf blobs.
pub fn update_leaf(input: &LeafUpdateInput<'_>) -> Result<LeafUpdateOutput, IncrementalLeafError> {
    let header = read_leaf_header(input.leaf_bytes)?;
    let cmp = cmp_for_order(input.order);

    // 1. Read each leaflet's header for boundary keys
    let boundaries = read_leaflet_boundaries(&header, input.leaf_bytes, input.g_id)?;

    // 2. Slice novelty to leaflets (half-open intervals)
    let slices = slice_novelty_to_leaflets(input.novelty, &boundaries, cmp);

    // 3. Process each leaflet
    let mut processed: Vec<ProcessedLeaflet> = Vec::new();
    let mut passthrough_count = 0usize;
    let mut merged_count = 0usize;
    let mut split_count = 0usize;
    for (i, dir_entry) in header.leaflet_dir.iter().enumerate() {
        let novelty = slices[i];
        if novelty.is_empty() {
            // Passthrough: carry raw bytes unchanged
            processed.push(passthrough_leaflet(input.leaf_bytes, dir_entry));
            passthrough_count += 1;
        } else {
            // Full decode + merge + re-encode
            let results =
                merge_and_encode_leaflet(input.leaf_bytes, dir_entry, &header, novelty, input)?;
            if results.len() > 1 {
                split_count += 1;
            }
            merged_count += 1;
            processed.extend(results);
        }
    }

    tracing::debug!(
        order = ?input.order,
        leaflets = header.leaflet_count,
        novelty = input.novelty.len(),
        passthrough = passthrough_count,
        merged = merged_count,
        splits = split_count,
        output_leaflets = processed.len(),
        "leaf update complete"
    );

    // 4. Assemble into leaf blob(s)
    assemble_output_leaves(&processed, &header, input)
}

// ============================================================================
// Leaflet boundary keys
// ============================================================================

/// Read the LeafletHeader from each leaflet to extract boundary sort keys.
fn read_leaflet_boundaries(
    header: &LeafFileHeader,
    leaf_bytes: &[u8],
    g_id: u16,
) -> io::Result<Vec<RunRecord>> {
    let mut boundaries = Vec::with_capacity(header.leaflet_count as usize);
    for entry in &header.leaflet_dir {
        let offset = entry.offset as usize;
        let end = offset + entry.compressed_len as usize;
        if end > leaf_bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "leaflet extends past leaf file end",
            ));
        }
        let leaflet_data = &leaf_bytes[offset..end];
        let lh = LeafletHeader::read_from(leaflet_data)?;
        boundaries.push(leaflet_header_to_boundary(&lh, g_id));
    }
    Ok(boundaries)
}

/// Convert a LeafletHeader's first-key fields to a boundary RunRecord.
///
/// Sets dt=0, t=0, op=0 so this sorts as the "minimum" for the identity prefix.
/// This is sufficient for half-open interval slicing because novelty with the same
/// (s_id, p_id, o_kind, o_key) but any dt will sort >= this boundary.
fn leaflet_header_to_boundary(lh: &LeafletHeader, g_id: u16) -> RunRecord {
    RunRecord {
        g_id,
        s_id: SubjectId::from_u64(lh.first_s_id),
        o_key: lh.first_o_key,
        p_id: lh.first_p_id,
        t: 0,
        i: LIST_INDEX_NONE,
        dt: 0,
        lang_id: 0,
        o_kind: lh.first_o_kind,
        op: 0,
    }
}

// ============================================================================
// Novelty slicing (half-open intervals)
// ============================================================================

/// Assign novelty records to leaflets using half-open interval model.
///
/// - Leaflet 0 owns `[-∞, boundary[1])` (includes novelty before first leaflet)
/// - Leaflet i owns `[boundary[i], boundary[i+1])`
/// - Last leaflet owns `[boundary[last], +∞)`
fn slice_novelty_to_leaflets<'a>(
    novelty: &'a [RunRecord],
    boundaries: &[RunRecord],
    cmp: fn(&RunRecord, &RunRecord) -> Ordering,
) -> Vec<&'a [RunRecord]> {
    let n = boundaries.len();
    if n == 0 {
        return vec![];
    }
    if novelty.is_empty() {
        return vec![&[] as &[RunRecord]; n];
    }

    let mut slices = Vec::with_capacity(n);
    let mut start = 0;

    for i in 0..n {
        if i == n - 1 {
            // Last leaflet gets everything remaining
            slices.push(&novelty[start..]);
        } else {
            // Find split point: first novelty record >= boundary[i+1]
            let end = start
                + novelty[start..]
                    .partition_point(|r| cmp(r, &boundaries[i + 1]) == Ordering::Less);
            slices.push(&novelty[start..end]);
            start = end;
        }
    }

    slices
}

// ============================================================================
// Passthrough leaflet
// ============================================================================

fn passthrough_leaflet(leaf_bytes: &[u8], entry: &LeafletDirEntry) -> ProcessedLeaflet {
    let start = entry.offset as usize;
    let end = start + entry.compressed_len as usize;
    ProcessedLeaflet {
        data: leaf_bytes[start..end].to_vec(),
        row_count: entry.row_count,
        first_s_id: entry.first_s_id,
        first_p_id: entry.first_p_id,
        first_record: None,
        last_record: None,
    }
}

// ============================================================================
// Merge + encode a single leaflet
// ============================================================================

/// Decode a leaflet, merge novelty, re-encode. May return 0 or more leaflets
/// (0 if all facts were retracted with no R3, >1 if a split was needed).
fn merge_and_encode_leaflet(
    leaf_bytes: &[u8],
    entry: &LeafletDirEntry,
    header: &LeafFileHeader,
    novelty: &[RunRecord],
    input: &LeafUpdateInput<'_>,
) -> Result<Vec<ProcessedLeaflet>, IncrementalLeafError> {
    let offset = entry.offset as usize;
    let leaflet_data = &leaf_bytes[offset..offset + entry.compressed_len as usize];

    // Decode all three regions
    let (_lh, s_ids, p_ids, o_kinds, o_keys) =
        decode_leaflet_region1(leaflet_data, header.p_width, input.order)?;
    let r2 = decode_leaflet_region2(leaflet_data, &_lh, header.dt_width)?;
    let existing_r3 = decode_leaflet_region3(leaflet_data, &_lh)?;

    // Build SubjectIdColumn for MergeInput
    let sid_col = SubjectIdColumn::from_u64_vec(s_ids, SubjectIdEncoding::Wide);

    let merge_input = MergeInput {
        r1_s_ids: &sid_col,
        r1_p_ids: &p_ids,
        r1_o_kinds: &o_kinds,
        r1_o_keys: &o_keys,
        r2_dt: &r2.dt_values,
        r2_t: &r2.t_values,
        r2_lang: r2.lang.as_ref(),
        r2_i: r2.i_col.as_ref(),
        existing_r3: &existing_r3,
        novelty,
        order: input.order,
    };

    let merged = merge_novelty(&merge_input);

    // Check empty-after-retract safety invariant
    if merged.row_count == 0 && !merged.region3.is_empty() {
        return Err(IncrementalLeafError::EmptyLeafletWithHistory);
    }

    if merged.row_count == 0 {
        // Fully empty leaflet (no R3 either) — drop it
        return Ok(vec![]);
    }

    let records = reconstitute_records(&merged, input.g_id);

    // Check if split needed
    if records.len() > input.leaflet_split_rows {
        Ok(split_and_encode_leaflet(
            &records,
            &merged.region3,
            &SplitConfig {
                target_rows: input.leaflet_target_rows,
                order: input.order,
                p_width: header.p_width,
                dt_width: header.dt_width,
                zstd_level: input.zstd_level,
                cmp: cmp_for_order(input.order),
            },
        ))
    } else {
        let encoder = LeafletEncoder::with_widths_and_order(
            input.zstd_level,
            header.p_width,
            header.dt_width,
            input.order,
        );
        let encoded = encoder.encode_leaflet_with_r3(&records, &merged.region3);
        let first = records[0];
        let last = *records.last().unwrap();
        Ok(vec![ProcessedLeaflet {
            data: encoded,
            row_count: records.len() as u32,
            first_s_id: first.s_id.as_u64(),
            first_p_id: first.p_id,
            first_record: Some(first),
            last_record: Some(last),
        }])
    }
}

// ============================================================================
// Record reconstitution from MergeOutput columns
// ============================================================================

/// Reconstitute `RunRecord`s from the dense column output of `merge_novelty()`.
fn reconstitute_records(out: &MergeOutput, g_id: u16) -> Vec<RunRecord> {
    (0..out.row_count)
        .map(|i| RunRecord {
            g_id,
            s_id: SubjectId::from_u64(out.s_ids[i]),
            o_key: out.o_keys[i],
            p_id: out.p_ids[i],
            t: out.t[i],
            i: if out.i_vals[i] == ListIndex::none().as_i32() {
                LIST_INDEX_NONE
            } else {
                out.i_vals[i] as u32
            },
            dt: out.dt[i] as u16,
            lang_id: out.lang[i],
            o_kind: out.o_kinds[i],
            op: 1, // All surviving R1 rows are asserts
        })
        .collect()
}

// ============================================================================
// Leaflet split with R3 partitioning
// ============================================================================

/// Config for leaflet splitting, to avoid passing too many arguments.
struct SplitConfig {
    target_rows: usize,
    order: RunSortOrder,
    p_width: u8,
    dt_width: u8,
    zstd_level: i32,
    cmp: fn(&RunRecord, &RunRecord) -> Ordering,
}

/// Split an oversized merged leaflet into multiple leaflets of `target_rows` each,
/// partitioning Region 3 entries to the correct sub-leaflet.
fn split_and_encode_leaflet(
    records: &[RunRecord],
    region3: &[Region3Entry],
    cfg: &SplitConfig,
) -> Vec<ProcessedLeaflet> {
    let encoder =
        LeafletEncoder::with_widths_and_order(cfg.zstd_level, cfg.p_width, cfg.dt_width, cfg.order);
    let chunks: Vec<&[RunRecord]> = records.chunks(cfg.target_rows).collect();

    let mut result = Vec::with_capacity(chunks.len());

    for (ci, chunk) in chunks.iter().enumerate() {
        // Partition R3 entries to this chunk based on identity sort order
        let chunk_r3 = partition_r3(
            region3,
            chunk.first().unwrap(),
            chunks.get(ci + 1).and_then(|c| c.first()),
            cfg.cmp,
        );

        let encoded = encoder.encode_leaflet_with_r3(chunk, &chunk_r3);
        let first = chunk[0];
        let last = *chunk.last().unwrap();
        result.push(ProcessedLeaflet {
            data: encoded,
            row_count: chunk.len() as u32,
            first_s_id: first.s_id.as_u64(),
            first_p_id: first.p_id,
            first_record: Some(first),
            last_record: Some(last),
        });
    }

    result
}

/// Partition Region 3 entries into a chunk's key range.
///
/// Uses identity-only comparison: constructs RunRecords from R3 entries with
/// t=0, op=0 (minimum version fields) so the order comparator reduces to
/// pure identity comparison.
fn partition_r3(
    region3: &[Region3Entry],
    lower: &RunRecord,
    upper: Option<&RunRecord>,
    cmp: fn(&RunRecord, &RunRecord) -> Ordering,
) -> Vec<Region3Entry> {
    let lower_min = record_with_min_version(lower);
    let upper_min = upper.map(record_with_min_version);

    region3
        .iter()
        .filter(|e| {
            let r3_key = r3_entry_to_min_record(e);
            let above = cmp(&r3_key, &lower_min) != Ordering::Less;
            let below = upper_min
                .as_ref()
                .is_none_or(|hi| cmp(&r3_key, hi) == Ordering::Less);
            above && below
        })
        .copied()
        .collect()
}

/// Convert a Region3Entry to a RunRecord with minimum version fields (t=0, op=0)
/// for identity-only comparison.
fn r3_entry_to_min_record(e: &Region3Entry) -> RunRecord {
    RunRecord {
        g_id: 0,
        s_id: SubjectId::from_u64(e.s_id),
        o_key: e.o_key,
        p_id: e.p_id,
        t: 0,
        i: if e.i == ListIndex::none().as_i32() {
            LIST_INDEX_NONE
        } else {
            e.i as u32
        },
        dt: e.dt,
        lang_id: e.lang_id,
        o_kind: e.o_kind,
        op: 0,
    }
}

/// Decode a passthrough leaflet to extract its first RunRecord.
///
/// Only called for the boundary leaflet at position 0 when it's passthrough.
fn decode_first_record(
    leaflet_data: &[u8],
    header: &LeafFileHeader,
    input: &LeafUpdateInput<'_>,
) -> Result<RunRecord, IncrementalLeafError> {
    let (_, s_ids, p_ids, o_kinds, o_keys) =
        decode_leaflet_region1(leaflet_data, header.p_width, input.order)?;
    let lh = LeafletHeader::read_from(leaflet_data)?;
    let r2 = decode_leaflet_region2(leaflet_data, &lh, header.dt_width)?;

    Ok(RunRecord {
        g_id: input.g_id,
        s_id: SubjectId::from_u64(s_ids[0]),
        o_key: o_keys[0],
        p_id: p_ids[0],
        t: r2.t_values[0],
        i: r2.i_col.as_ref().map_or(LIST_INDEX_NONE, |c| {
            let v = c.get(0);
            if v == ListIndex::none().as_i32() {
                LIST_INDEX_NONE
            } else {
                v as u32
            }
        }),
        dt: r2.dt_values[0] as u16,
        lang_id: r2.lang.as_ref().map_or(0, |c| c.get(0)),
        o_kind: o_kinds[0],
        op: 1,
    })
}

/// Decode a passthrough leaflet to extract its last RunRecord.
///
/// Only called for the boundary leaflet at the last position when it's passthrough.
fn decode_last_record(
    leaflet_data: &[u8],
    header: &LeafFileHeader,
    input: &LeafUpdateInput<'_>,
) -> Result<RunRecord, IncrementalLeafError> {
    let (_, s_ids, p_ids, o_kinds, o_keys) =
        decode_leaflet_region1(leaflet_data, header.p_width, input.order)?;
    let lh = LeafletHeader::read_from(leaflet_data)?;
    let r2 = decode_leaflet_region2(leaflet_data, &lh, header.dt_width)?;

    let last = s_ids.len() - 1;
    Ok(RunRecord {
        g_id: input.g_id,
        s_id: SubjectId::from_u64(s_ids[last]),
        o_key: o_keys[last],
        p_id: p_ids[last],
        t: r2.t_values[last],
        i: r2.i_col.as_ref().map_or(LIST_INDEX_NONE, |c| {
            let v = c.get(last as u16);
            if v == ListIndex::none().as_i32() {
                LIST_INDEX_NONE
            } else {
                v as u32
            }
        }),
        dt: r2.dt_values[last] as u16,
        lang_id: r2.lang.as_ref().map_or(0, |c| c.get(last as u16)),
        o_kind: o_kinds[last],
        op: 1,
    })
}

/// Zero out version fields (t, op) for identity-only comparison.
fn record_with_min_version(r: &RunRecord) -> RunRecord {
    RunRecord { t: 0, op: 0, ..*r }
}

// ============================================================================
// Leaf blob assembly
// ============================================================================

/// Group processed leaflets into leaf blobs, splitting if too many leaflets.
fn assemble_output_leaves(
    processed: &[ProcessedLeaflet],
    original_header: &LeafFileHeader,
    input: &LeafUpdateInput<'_>,
) -> Result<LeafUpdateOutput, IncrementalLeafError> {
    if processed.is_empty() {
        return Ok(LeafUpdateOutput { leaves: vec![] });
    }

    // Split into groups if total leaflet count exceeds threshold
    let groups: Vec<&[ProcessedLeaflet]> = if processed.len() > input.leaf_split_leaflets {
        processed.chunks(input.leaflets_per_leaf).collect()
    } else {
        vec![processed]
    };

    let mut leaves = Vec::with_capacity(groups.len());
    for group in groups {
        let blob = build_leaf_blob(group, original_header, input)?;
        leaves.push(blob);
    }

    Ok(LeafUpdateOutput { leaves })
}

/// Build a complete FLI2 leaf blob from processed leaflets.
///
/// For boundary leaflets (first/last in this group) that are passthrough,
/// decodes R1+R2 to extract exact first/last records rather than using the
/// incomplete SortKey fallback.
fn build_leaf_blob(
    leaflets: &[ProcessedLeaflet],
    original_header: &LeafFileHeader,
    input: &LeafUpdateInput<'_>,
) -> Result<NewLeafBlob, IncrementalLeafError> {
    assert!(!leaflets.is_empty(), "cannot build empty leaf blob");

    let first_key = if leaflets[0].first_record.is_some() {
        resolve_first_key(&leaflets[0], original_header, input.g_id)
    } else {
        // Passthrough boundary: decode to get exact record
        decode_first_record(&leaflets[0].data, original_header, input)?
    };
    let last_leaflet = leaflets.last().unwrap();
    let last_key = if last_leaflet.last_record.is_some() {
        resolve_last_key(last_leaflet, original_header, input.g_id)
    } else {
        // Passthrough boundary: decode to get exact record
        decode_last_record(&last_leaflet.data, original_header, input)?
    };

    let total_rows: u64 = leaflets.iter().map(|l| l.row_count as u64).sum();
    let leaflet_count = leaflets.len();
    let dir_size = leaflet_count * LEAFLET_DIR_ENTRY;
    let header_size = LEAF_HEADER_FIXED + dir_size;
    let data_size: usize = leaflets.iter().map(|l| l.data.len()).sum();
    let total_size = header_size + data_size;

    let mut buf = Vec::with_capacity(total_size);

    // --- Header (68 bytes fixed) ---
    buf.extend_from_slice(&LEAF_MAGIC);
    buf.push(LEAF_VERSION);
    buf.push(leaflet_count as u8);
    buf.push(original_header.dt_width);
    buf.push(original_header.p_width);
    buf.extend_from_slice(&total_rows.to_le_bytes());

    // First/last SortKeys (26 bytes each)
    let mut key_buf = [0u8; SORT_KEY_BYTES];
    SortKey::from_record(&first_key).write_to(&mut key_buf);
    buf.extend_from_slice(&key_buf);
    SortKey::from_record(&last_key).write_to(&mut key_buf);
    buf.extend_from_slice(&key_buf);

    // --- Leaflet directory ---
    let mut offset = header_size as u64;
    for l in leaflets {
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(l.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&l.row_count.to_le_bytes());
        buf.extend_from_slice(&l.first_s_id.to_le_bytes());
        buf.extend_from_slice(&l.first_p_id.to_le_bytes());
        offset += l.data.len() as u64;
    }

    // --- Leaflet data ---
    for l in leaflets {
        buf.extend_from_slice(&l.data);
    }

    // Hash the complete blob for CID
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let digest_hex = hex::encode(hasher.finalize());
    let cid = fluree_db_core::ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF,
        &digest_hex,
    )
    .expect("valid SHA-256 hex digest");

    Ok(NewLeafBlob {
        bytes: buf,
        cid,
        first_key,
        last_key,
        row_count: total_rows,
    })
}

// ============================================================================
// Key resolution helpers
// ============================================================================

/// Resolve the first_key for the leaf from its first leaflet.
///
/// If the first leaflet was merged (has first_record), use that.
/// Otherwise (passthrough), use the original leaf's first_key SortKey.
fn resolve_first_key(
    first_leaflet: &ProcessedLeaflet,
    original: &LeafFileHeader,
    g_id: u16,
) -> RunRecord {
    first_leaflet
        .first_record
        .unwrap_or_else(|| sort_key_to_record(&original.first_key, g_id))
}

/// Resolve the last_key for the leaf from its last leaflet.
///
/// If the last leaflet was merged (has last_record), use that.
/// Otherwise (passthrough), use the original leaf's last_key SortKey.
fn resolve_last_key(
    last_leaflet: &ProcessedLeaflet,
    original: &LeafFileHeader,
    g_id: u16,
) -> RunRecord {
    last_leaflet
        .last_record
        .unwrap_or_else(|| sort_key_to_record(&original.last_key, g_id))
}

/// Convert a SortKey back to a RunRecord for routing.
///
/// SortKey carries (g_id, s_id, p_id, dt, o_kind, o_key). The remaining
/// RunRecord fields (t, op, lang_id, i) are set to conservative defaults.
fn sort_key_to_record(sk: &SortKey, g_id: u16) -> RunRecord {
    RunRecord {
        g_id,
        s_id: SubjectId::from_u64(sk.s_id),
        o_key: sk.o_key,
        p_id: sk.p_id,
        t: 0,
        i: LIST_INDEX_NONE,
        dt: sk.dt,
        lang_id: 0,
        o_kind: sk.o_kind,
        op: 1,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::leaf::LeafWriter;
    use fluree_db_binary_index::format::leaflet::decode_leaflet;
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

    fn make_retract(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            false,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    /// Write records to a leaf file via LeafWriter, return the leaf bytes.
    fn build_leaf(records: &[RunRecord], leaflet_rows: usize, leaflets_per_leaf: usize) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!(
            "fluree_test_incr_leaf_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = LeafWriter::new(dir.clone(), leaflet_rows, leaflets_per_leaf, 1);
        for r in records {
            writer.push_record(*r).unwrap();
        }
        let infos = writer.finish().unwrap();
        assert_eq!(infos.len(), 1, "expected exactly 1 leaf");

        let leaf_path = dir.join(infos[0].leaf_cid.to_string());
        let data = std::fs::read(&leaf_path).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        data
    }

    fn default_input<'a>(leaf_bytes: &'a [u8], novelty: &'a [RunRecord]) -> LeafUpdateInput<'a> {
        LeafUpdateInput {
            leaf_bytes,
            novelty,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_split_rows: 37_500,
            leaflet_target_rows: 25_000,
            leaflets_per_leaf: 10,
            leaf_split_leaflets: 20,
        }
    }

    #[test]
    fn test_basic_merge_single_leaflet() {
        // Build a leaf with 5 records in 1 leaflet
        let existing = vec![
            make_record(1, 1, 10, 1),
            make_record(2, 1, 20, 1),
            make_record(4, 1, 40, 1),
            make_record(5, 1, 50, 1),
            make_record(6, 1, 60, 1),
        ];
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Novelty: insert s=3
        let novelty = vec![make_record(3, 1, 30, 5)];
        let input = default_input(&leaf_bytes, &novelty);
        let output = update_leaf(&input).unwrap();

        assert_eq!(output.leaves.len(), 1);
        let leaf = &output.leaves[0];
        assert_eq!(leaf.row_count, 6);

        // Verify the output can be parsed
        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        assert_eq!(hdr.total_rows, 6);
        assert_eq!(hdr.leaflet_count, 1);

        // Decode and verify records
        let dir = &hdr.leaflet_dir[0];
        let leaflet_data =
            &leaf.bytes[dir.offset as usize..dir.offset as usize + dir.compressed_len as usize];
        let decoded =
            decode_leaflet(leaflet_data, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        assert_eq!(decoded.s_ids, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_passthrough_untouched_leaflets() {
        // Build a leaf with 3 leaflets (3 records each)
        let existing: Vec<RunRecord> = (0..9)
            .map(|i| make_record(i as u64, 1, i as i64 * 10, 1))
            .collect();
        let leaf_bytes = build_leaf(&existing, 3, 10);

        // Novelty only touches the second leaflet range (s_ids 3..6)
        let novelty = vec![make_record(4, 1, 45, 5)]; // insert between existing s=4 and s=5
        let input = default_input(&leaf_bytes, &novelty);
        let output = update_leaf(&input).unwrap();

        assert_eq!(output.leaves.len(), 1);
        let leaf = &output.leaves[0];
        // Original: 9 rows, novelty adds 1 to second leaflet → 10 total
        assert_eq!(leaf.row_count, 10);

        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        assert_eq!(hdr.leaflet_count, 3);

        // First leaflet should be unchanged (rows 0-2)
        let d0 = &hdr.leaflet_dir[0];
        let ld0 = &leaf.bytes[d0.offset as usize..d0.offset as usize + d0.compressed_len as usize];
        let dec0 = decode_leaflet(ld0, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        assert_eq!(dec0.s_ids, vec![0, 1, 2]);

        // Third leaflet should be unchanged (rows 6-8)
        let d2 = &hdr.leaflet_dir[2];
        let ld2 = &leaf.bytes[d2.offset as usize..d2.offset as usize + d2.compressed_len as usize];
        let dec2 = decode_leaflet(ld2, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        assert_eq!(dec2.s_ids, vec![6, 7, 8]);
    }

    #[test]
    fn test_retract_existing_fact() {
        let existing = vec![
            make_record(1, 1, 10, 1),
            make_record(2, 1, 20, 1),
            make_record(3, 1, 30, 1),
        ];
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Retract s=2
        let novelty = vec![make_retract(2, 1, 20, 5)];
        let input = default_input(&leaf_bytes, &novelty);
        let output = update_leaf(&input).unwrap();

        assert_eq!(output.leaves.len(), 1);
        let leaf = &output.leaves[0];
        assert_eq!(leaf.row_count, 2);

        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        let d = &hdr.leaflet_dir[0];
        let ld = &leaf.bytes[d.offset as usize..d.offset as usize + d.compressed_len as usize];
        let decoded = decode_leaflet(ld, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();
        assert_eq!(decoded.s_ids, vec![1, 3]); // s=2 removed
    }

    #[test]
    fn test_empty_after_retract_returns_error() {
        let existing = vec![make_record(1, 1, 10, 1), make_record(2, 1, 20, 1)];
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Retract all facts
        let novelty = vec![make_retract(1, 1, 10, 5), make_retract(2, 1, 20, 5)];
        let input = default_input(&leaf_bytes, &novelty);
        let result = update_leaf(&input);

        assert!(result.is_err());
        match result {
            Err(IncrementalLeafError::EmptyLeafletWithHistory) => {} // expected
            other => panic!("expected EmptyLeafletWithHistory, got {:?}", other.err()),
        }
    }

    #[test]
    fn test_leaflet_split() {
        // Build a leaf with 20 records
        let existing: Vec<RunRecord> = (0..20)
            .map(|i| make_record(i as u64 * 2, 1, i as i64 * 10, 1))
            .collect();
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Add 15 more records — total 35, exceeding split threshold of 30
        let novelty: Vec<RunRecord> = (0..15)
            .map(|i| make_record(i as u64 * 2 + 1, 1, i as i64 * 10 + 5, 5))
            .collect();
        let mut input = default_input(&leaf_bytes, &novelty);
        input.leaflet_split_rows = 30;
        input.leaflet_target_rows = 20;
        let output = update_leaf(&input).unwrap();

        assert_eq!(output.leaves.len(), 1);
        let leaf = &output.leaves[0];
        assert_eq!(leaf.row_count, 35);

        // Should have been split into 2 leaflets (20 + 15)
        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        assert_eq!(hdr.leaflet_count, 2);
        assert_eq!(hdr.leaflet_dir[0].row_count, 20);
        assert_eq!(hdr.leaflet_dir[1].row_count, 15);
    }

    #[test]
    fn test_r3_preserved_after_merge() {
        // Start with a fact, then retract it (to test R3 generation)
        let existing = vec![
            make_record(1, 1, 10, 1),
            make_record(2, 1, 20, 1),
            make_record(3, 1, 30, 1),
        ];
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Retract s=2, update s=1 at new t
        let novelty = vec![
            make_record(1, 1, 10, 5),  // update
            make_retract(2, 1, 20, 5), // retract
        ];
        let input = default_input(&leaf_bytes, &novelty);
        let output = update_leaf(&input).unwrap();

        let leaf = &output.leaves[0];
        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        let d = &hdr.leaflet_dir[0];
        let ld = &leaf.bytes[d.offset as usize..d.offset as usize + d.compressed_len as usize];

        // Decode R3
        let lh = LeafletHeader::read_from(ld).unwrap();
        let r3 = decode_leaflet_region3(ld, &lh).unwrap();
        // Should have R3 entries for the retraction and the update
        assert!(!r3.is_empty(), "R3 should have history entries");
    }

    #[test]
    fn test_output_leaf_is_valid_fli2() {
        let existing = vec![make_record(1, 1, 10, 1), make_record(2, 1, 20, 1)];
        let leaf_bytes = build_leaf(&existing, 100, 10);

        let novelty = vec![make_record(3, 1, 30, 5)];
        let input = default_input(&leaf_bytes, &novelty);
        let output = update_leaf(&input).unwrap();

        let leaf = &output.leaves[0];

        // Verify magic + version
        assert_eq!(&leaf.bytes[0..4], b"FLI2");
        assert_eq!(leaf.bytes[4], 2); // version

        // Verify CID is consistent with content
        let mut hasher = Sha256::new();
        hasher.update(&leaf.bytes);
        let expected_hex = hex::encode(hasher.finalize());
        let expected_cid = fluree_db_core::ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_LEAF,
            &expected_hex,
        )
        .unwrap();
        assert_eq!(leaf.cid, expected_cid);

        // Verify read_leaf_header succeeds
        let hdr = read_leaf_header(&leaf.bytes).unwrap();
        assert_eq!(hdr.total_rows, 3);
    }

    #[test]
    fn test_novelty_slicing_half_open() {
        let cmp = cmp_for_order(RunSortOrder::Spot);

        // 3 boundaries at s_id = 10, 20, 30
        let boundaries = vec![
            make_record(10, 0, 0, 0),
            make_record(20, 0, 0, 0),
            make_record(30, 0, 0, 0),
        ];

        // Novelty spans all ranges
        let novelty = vec![
            make_record(5, 0, 0, 1),  // before boundary[0] → leaflet 0
            make_record(15, 0, 0, 1), // between boundary[0] and boundary[1] → leaflet 0
            make_record(20, 0, 0, 1), // exactly boundary[1] → leaflet 1
            make_record(25, 0, 0, 1), // between boundary[1] and boundary[2] → leaflet 1
            make_record(35, 0, 0, 1), // after boundary[2] → leaflet 2
        ];

        let slices = slice_novelty_to_leaflets(&novelty, &boundaries, cmp);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), 2); // s=5, s=15
        assert_eq!(slices[1].len(), 2); // s=20, s=25
        assert_eq!(slices[2].len(), 1); // s=35
    }

    #[test]
    fn test_r3_partitioning_during_split() {
        // Create a scenario where we can verify R3 entries end up in the
        // correct leaflet after a split.
        let existing: Vec<RunRecord> = (0..10)
            .map(|i| make_record(i as u64 * 10, 1, i as i64 * 100, 1))
            .collect();
        let leaf_bytes = build_leaf(&existing, 100, 10);

        // Novelty: update several records to generate R3 entries, plus inserts
        let mut novelty = Vec::new();
        for i in 0..10 {
            // Update each existing record
            novelty.push(make_record(i as u64 * 10, 1, i as i64 * 100, 5));
        }
        // Insert interleaved records to push total past split threshold
        for i in 0..15 {
            novelty.push(make_record(i as u64 * 10 + 5, 1, i as i64 * 100 + 50, 5));
        }
        novelty.sort_unstable_by(cmp_for_order(RunSortOrder::Spot));

        let mut input = default_input(&leaf_bytes, &novelty);
        input.leaflet_split_rows = 20;
        input.leaflet_target_rows = 15;
        let output = update_leaf(&input).unwrap();

        let leaf = &output.leaves[0];
        let hdr = read_leaf_header(&leaf.bytes).unwrap();

        // Should have been split into 2 leaflets
        assert!(hdr.leaflet_count >= 2, "expected split into >=2 leaflets");

        // Verify all R3 entries in each leaflet have identity within that leaflet's range
        for (li, dir) in hdr.leaflet_dir.iter().enumerate() {
            let ld =
                &leaf.bytes[dir.offset as usize..dir.offset as usize + dir.compressed_len as usize];
            let lh = LeafletHeader::read_from(ld).unwrap();
            let r3 = decode_leaflet_region3(ld, &lh).unwrap();
            let decoded =
                decode_leaflet(ld, hdr.p_width, hdr.dt_width, RunSortOrder::Spot).unwrap();

            if decoded.row_count == 0 {
                continue;
            }

            let min_s = decoded.s_ids[0];
            let max_s = *decoded.s_ids.last().unwrap();

            for entry in &r3 {
                assert!(
                    entry.s_id >= min_s && entry.s_id <= max_s,
                    "R3 entry s_id={} outside leaflet {} range [{}, {}]",
                    entry.s_id,
                    li,
                    min_s,
                    max_s
                );
            }
        }
    }

    /// When the first or last leaflet is passthrough, the leaf's boundary keys
    /// must reflect the actual first/last records (with correct `t`), not a
    /// fabricated `t=0` from the SortKey fallback.
    #[test]
    fn test_boundary_keys_with_passthrough_edges() {
        // Build 3 leaflets of 5 records each, with distinct t values.
        let mut records = Vec::new();
        // Leaflet 0: s_id 100-104, t=10
        for i in 0..5u64 {
            records.push(make_record(100 + i, 1, i as i64, 10));
        }
        // Leaflet 1: s_id 200-204, t=10
        for i in 0..5u64 {
            records.push(make_record(200 + i, 1, i as i64, 10));
        }
        // Leaflet 2: s_id 300-304, t=10
        for i in 0..5u64 {
            records.push(make_record(300 + i, 1, i as i64, 10));
        }

        let leaf_bytes = build_leaf(&records, 5, 3);

        // Novelty only for the middle leaflet (s_id 200 range).
        let novelty = vec![make_record(202, 2, 99, 20)]; // new predicate at t=20

        let input = LeafUpdateInput {
            leaf_bytes: &leaf_bytes,
            novelty: &novelty,
            order: RunSortOrder::Spot,
            g_id: 0,
            zstd_level: 1,
            leaflet_split_rows: 37_500,
            leaflet_target_rows: 25_000,
            leaflets_per_leaf: 10,
            leaf_split_leaflets: 20,
        };

        let output = update_leaf(&input).unwrap();
        assert_eq!(output.leaves.len(), 1);
        let blob = &output.leaves[0];

        // first_key must come from the first leaflet's actual first record (t=10),
        // NOT from a fabricated SortKey with t=0.
        assert_eq!(
            blob.first_key.t, 10,
            "first_key.t should be 10 from passthrough first leaflet"
        );
        assert_eq!(blob.first_key.s_id.as_u64(), 100);

        // last_key must come from the last leaflet's actual last record (t=10),
        // NOT from a fabricated SortKey with t=0.
        assert_eq!(
            blob.last_key.t, 10,
            "last_key.t should be 10 from passthrough last leaflet"
        );
        assert_eq!(blob.last_key.s_id.as_u64(), 304);
    }
}
