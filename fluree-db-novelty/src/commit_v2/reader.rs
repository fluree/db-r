//! Commit v2 reader: binary blob -> Commit / CommitEnvelope.
//!
//! No NamespaceRegistry needed — Sids are reconstructed directly from
//! (namespace_code, name) pairs stored in the binary format.

use super::error::CommitV2Error;
use super::format::{
    CommitV2Footer, CommitV2Header, FLAG_ZSTD, FOOTER_LEN, HASH_LEN, HEADER_LEN, MIN_COMMIT_LEN,
};
use super::op_codec::{decode_op, ReadDicts};
use super::string_dict::StringDict;
use crate::{Commit, CommitEnvelope};
use sha2::{Digest, Sha256};

/// Read a v2 commit blob and return a full `Commit` (with flakes).
///
/// No NamespaceRegistry is needed — Sids are reconstructed directly from
/// the (namespace_code, name) pairs stored in the binary format.
pub fn read_commit(bytes: &[u8]) -> Result<Commit, CommitV2Error> {
    let blob_len = bytes.len();

    // 1. Validate minimum size
    if blob_len < MIN_COMMIT_LEN {
        return Err(CommitV2Error::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN,
        });
    }

    // 2. Parse header
    let header = CommitV2Header::read_from(bytes)?;

    // 3. Verify hash: SHA-256(bytes[0..len-32]) == trailing 32 bytes
    {
        let _span = tracing::debug_span!("v2_read_verify_hash", blob_len).entered();
        let hash_offset = blob_len - HASH_LEN;
        let expected_hash: [u8; 32] = bytes[hash_offset..].try_into().unwrap();
        let actual_hash: [u8; 32] = Sha256::digest(&bytes[..hash_offset]).into();
        if expected_hash != actual_hash {
            return Err(CommitV2Error::HashMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }
    }

    // 4. Decode binary envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > blob_len {
        return Err(CommitV2Error::TooSmall {
            got: blob_len,
            min: envelope_end,
        });
    }
    let envelope = super::envelope::decode_envelope(&bytes[envelope_start..envelope_end])?;

    // 5. Parse footer
    let hash_offset = blob_len - HASH_LEN;
    let footer_start = hash_offset - FOOTER_LEN;
    let footer = CommitV2Footer::read_from(&bytes[footer_start..hash_offset])?;

    // 6. Validate ops section bounds
    let ops_start = envelope_end;
    let ops_end = ops_start + footer.ops_section_len as usize;
    if ops_end > footer_start {
        return Err(CommitV2Error::InvalidOp(
            "ops section extends into footer".into(),
        ));
    }

    // 7. Load dictionaries (with bounds validation against ops_end..footer_start)
    let dicts = load_dicts(bytes, &footer, ops_end, footer_start)?;
    let ops_bytes = &bytes[ops_start..ops_end];
    let ops_decompressed;
    let ops_data = if header.flags & FLAG_ZSTD != 0 {
        let _span =
            tracing::debug_span!("v2_read_decompress", compressed_bytes = ops_bytes.len())
                .entered();
        ops_decompressed =
            zstd::decode_all(ops_bytes).map_err(CommitV2Error::DecompressionFailed)?;
        tracing::debug!(
            compressed = ops_bytes.len(),
            decompressed = ops_decompressed.len(),
            "ops decompressed"
        );
        &ops_decompressed[..]
    } else {
        ops_bytes
    };

    // 8. Decode ops into flakes
    let flakes = {
        let _span = tracing::debug_span!(
            "v2_decode_ops",
            op_count = header.op_count,
            ops_bytes = ops_data.len()
        )
        .entered();
        let mut flakes = Vec::with_capacity(header.op_count as usize);
        let mut pos = 0;
        for _ in 0..header.op_count {
            let flake = decode_op(ops_data, &mut pos, &dicts, header.t)?;
            flakes.push(flake);
        }
        flakes
    };

    tracing::debug!(
        blob_len,
        op_count = header.op_count,
        t = header.t,
        compressed = (header.flags & FLAG_ZSTD != 0),
        "v2 commit read"
    );

    // 9. Assemble Commit
    Ok(Commit {
        address: String::new(), // Injected at read time by load_commit()
        id: None,               // Injected at read time by load_commit()
        t: header.t,
        v: envelope.v,
        time: envelope.time,
        flakes,
        previous_ref: envelope.previous_ref,
        data: envelope.data,
        index: envelope.index,
        txn: envelope.txn,
        namespace_delta: envelope.namespace_delta,
    })
}

/// Read only the envelope from a v2 commit blob (no flakes, no hash check).
///
/// This is fast because it only reads the header + binary envelope section,
/// skipping the ops, dictionaries, and footer entirely.
pub fn read_commit_envelope(bytes: &[u8]) -> Result<CommitEnvelope, CommitV2Error> {
    // 1. Validate minimum size for header
    if bytes.len() < HEADER_LEN {
        return Err(CommitV2Error::TooSmall {
            got: bytes.len(),
            min: HEADER_LEN,
        });
    }

    // 2. Parse header
    let header = CommitV2Header::read_from(bytes)?;

    // 3. Decode binary envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > bytes.len() {
        return Err(CommitV2Error::TooSmall {
            got: bytes.len(),
            min: envelope_end,
        });
    }
    let env = super::envelope::decode_envelope(&bytes[envelope_start..envelope_end])?;

    Ok(CommitEnvelope {
        t: header.t,
        v: env.v,
        previous_ref: env.previous_ref,
        index: env.index,
        namespace_delta: env.namespace_delta,
    })
}

/// Load and deserialize the 5 string dictionaries from the blob.
///
/// Validates that each dictionary is within `[valid_start..valid_end)` (the region
/// between the ops section and footer) and that dictionaries don't overlap.
pub(crate) fn load_dicts(
    bytes: &[u8],
    footer: &CommitV2Footer,
    valid_start: usize,
    valid_end: usize,
) -> Result<ReadDicts, CommitV2Error> {
    let dict_names = ["graph", "subject", "predicate", "datatype", "object_ref"];
    let mut prev_end = valid_start;

    let load_one = |loc: &super::format::DictLocation,
                    name: &str,
                    prev_end: &mut usize|
     -> Result<StringDict, CommitV2Error> {
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if start < *prev_end {
            return Err(CommitV2Error::InvalidDictionary(format!(
                "{} dict at offset {} overlaps previous region ending at {}",
                name, start, *prev_end
            )));
        }
        if end > valid_end {
            return Err(CommitV2Error::InvalidDictionary(format!(
                "{} dict at offset {} len {} extends past dict region end {}",
                name, start, loc.len, valid_end
            )));
        }
        *prev_end = end;
        StringDict::deserialize(&bytes[start..end])
    };

    Ok(ReadDicts {
        graph: load_one(&footer.dicts[0], dict_names[0], &mut prev_end)?,
        subject: load_one(&footer.dicts[1], dict_names[1], &mut prev_end)?,
        predicate: load_one(&footer.dicts[2], dict_names[2], &mut prev_end)?,
        datatype: load_one(&footer.dicts[3], dict_names[3], &mut prev_end)?,
        object_ref: load_one(&footer.dicts[4], dict_names[4], &mut prev_end)?,
    })
}
