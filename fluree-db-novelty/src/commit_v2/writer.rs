//! Commit v2 writer: Commit -> binary blob.
//!
//! Encodes a [`Commit`] into the v2 binary format using Sid-direct encoding
//! (namespace_code + name dict entries). No NamespaceRegistry is needed —
//! Sid fields are read directly from flakes.

use super::envelope;
use super::format::{
    CommitV2Footer, CommitV2Header, DictLocation, FLAG_ZSTD, FOOTER_LEN, HASH_LEN, HEADER_LEN,
    VERSION,
};
use super::op_codec::{encode_op, CommitDicts};
use super::CommitV2Error;
use crate::Commit;
use sha2::{Digest, Sha256};

/// Result of writing a v2 commit blob.
pub struct CommitWriteResult {
    /// The complete binary blob.
    pub bytes: Vec<u8>,
    /// Hex-encoded SHA-256 of the blob (excluding the trailing 32-byte hash).
    /// Suitable for use as a content address.
    pub content_hash_hex: String,
}

/// Write a commit as a v2 binary blob.
///
/// Encodes flakes using Sid-direct encoding (namespace_code + name dict entries).
/// No NamespaceRegistry is needed — Sid fields are read directly from flakes.
///
/// Returns `CommitWriteResult` containing the blob bytes and the hex-encoded
/// SHA-256 content hash.
pub fn write_commit(commit: &Commit, compress: bool) -> Result<CommitWriteResult, CommitV2Error> {
    let op_count = commit.flakes.len();

    // 1. Serialize envelope (binary)
    let envelope_bytes = {
        let _span = tracing::debug_span!("v2_write_envelope").entered();
        let mut buf = Vec::new();
        envelope::encode_envelope(commit, &mut buf)?;
        buf
    };

    // 2. Encode all ops into a buffer, populating dictionaries
    let (ops_raw, dicts) = {
        let _span = tracing::debug_span!("v2_encode_ops", op_count).entered();
        let mut dicts = CommitDicts::new();
        let mut ops_raw = Vec::new();
        for flake in &commit.flakes {
            encode_op(flake, &mut dicts, &mut ops_raw)?;
        }
        (ops_raw, dicts)
    };

    // 3. Optionally compress ops
    let (ops_section, is_compressed) = if compress && !ops_raw.is_empty() {
        let _span = tracing::debug_span!("v2_compress_ops", raw_bytes = ops_raw.len()).entered();
        let compressed = zstd::encode_all(ops_raw.as_slice(), 3)
            .map_err(CommitV2Error::CompressionFailed)?;
        // Only use compressed if it's actually smaller
        if compressed.len() < ops_raw.len() {
            tracing::debug!(
                raw = ops_raw.len(),
                compressed = compressed.len(),
                ratio = format_args!("{:.1}x", ops_raw.len() as f64 / compressed.len() as f64),
                "ops compressed"
            );
            (compressed, true)
        } else {
            (ops_raw, false)
        }
    } else {
        (ops_raw, false)
    };

    // 4. Serialize dictionaries
    let dict_bytes: Vec<Vec<u8>> = [
        &dicts.graph,
        &dicts.subject,
        &dicts.predicate,
        &dicts.datatype,
        &dicts.object_ref,
    ]
    .iter()
    .map(|d| d.serialize())
    .collect();

    // 5. Calculate total size and allocate output
    let total_size = HEADER_LEN
        + envelope_bytes.len()
        + ops_section.len()
        + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
        + FOOTER_LEN
        + HASH_LEN;
    let mut output = Vec::with_capacity(total_size);

    // 6. Write header
    let mut flags = 0u8;
    if is_compressed {
        flags |= FLAG_ZSTD;
    }
    let header = CommitV2Header {
        version: VERSION,
        flags,
        t: commit.t,
        op_count: commit.flakes.len() as u32,
        envelope_len: envelope_bytes.len() as u32,
    };
    let mut header_buf = [0u8; HEADER_LEN];
    header.write_to(&mut header_buf);
    output.extend_from_slice(&header_buf);

    // 7. Write envelope
    output.extend_from_slice(&envelope_bytes);

    // 8. Write ops section
    output.extend_from_slice(&ops_section);

    // 9. Write dictionaries, recording locations
    let mut dict_locations = [DictLocation::default(); 5];
    for (i, bytes) in dict_bytes.iter().enumerate() {
        dict_locations[i] = DictLocation {
            offset: output.len() as u64,
            len: bytes.len() as u32,
        };
        output.extend_from_slice(bytes);
    }

    // 10. Write footer
    let footer = CommitV2Footer {
        dicts: dict_locations,
        ops_section_len: ops_section.len() as u32,
    };
    let mut footer_buf = [0u8; FOOTER_LEN];
    footer.write_to(&mut footer_buf);
    output.extend_from_slice(&footer_buf);

    // 11. Compute SHA-256 of everything so far, append as trailing hash
    let hash = {
        let _span = tracing::debug_span!("v2_write_hash", blob_bytes = output.len()).entered();
        Sha256::digest(&output)
    };
    let hash_bytes: [u8; 32] = hash.into();
    let content_hash_hex = hex::encode(hash_bytes);
    output.extend_from_slice(&hash_bytes);

    debug_assert_eq!(output.len(), total_size);

    tracing::debug!(
        blob_bytes = output.len(),
        op_count,
        envelope_bytes = envelope_bytes.len(),
        ops_bytes = ops_section.len(),
        compressed = is_compressed,
        "v2 commit written"
    );

    Ok(CommitWriteResult {
        bytes: output,
        content_hash_hex,
    })
}
