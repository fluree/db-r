//! Commit v2 writer: Commit -> binary blob.
//!
//! Encodes a [`Commit`] into the v2 binary format using Sid-direct encoding
//! (namespace_code + name dict entries). No NamespaceRegistry is needed —
//! Sid fields are read directly from flakes.

use super::envelope;
use super::format::{
    encode_sig_block, sig_block_size, CommitSignature, CommitV2Footer, CommitV2Header,
    DictLocation, FLAG_HAS_COMMIT_SIG, FLAG_ZSTD, FOOTER_LEN, HASH_LEN, HEADER_LEN, VERSION,
};
use super::op_codec::{encode_op, CommitDicts};
use super::CommitV2Error;
use crate::Commit;
use fluree_db_credential::{did_from_pubkey, sign_commit_digest, SigningKey};
use sha2::{Digest, Sha256};

/// Result of writing a v2 commit blob.
pub struct CommitWriteResult {
    /// The complete binary blob.
    pub bytes: Vec<u8>,
    /// Hex-encoded SHA-256 of the blob (excluding the trailing 32-byte hash
    /// and any signature block). Suitable for use as a content address.
    pub content_hash_hex: String,
}

/// Write a commit as a v2 binary blob.
///
/// Encodes flakes using Sid-direct encoding (namespace_code + name dict entries).
/// No NamespaceRegistry is needed — Sid fields are read directly from flakes.
///
/// When `signing` is `Some((key, ledger_id))`, the commit is signed with
/// Ed25519 and a signature block is appended after the hash. The header's
/// `FLAG_HAS_COMMIT_SIG` flag and `sig_block_len` are set *before* hash
/// computation so they are covered by the content hash.
///
/// Returns `CommitWriteResult` containing the blob bytes and the hex-encoded
/// SHA-256 content hash.
pub fn write_commit(
    commit: &Commit,
    compress: bool,
    signing: Option<(&SigningKey, &str)>,
) -> Result<CommitWriteResult, CommitV2Error> {
    let op_count = commit.flakes.len();

    // 1. Pre-compute signing metadata (needed for header before hash)
    let (signer_did, pre_sig_block_len) = if let Some((key, _)) = &signing {
        let did = did_from_pubkey(&key.verifying_key().to_bytes());
        // Build a temporary CommitSignature to compute encoded size
        let tmp_sig = CommitSignature {
            signer: did.clone(),
            algo: super::format::ALGO_ED25519,
            signature: [0u8; 64],
            timestamp: 0,
            metadata: None,
        };
        let len = sig_block_size(&[tmp_sig]);
        (Some(did), len as u16)
    } else {
        (None, 0u16)
    };

    // 2. Serialize envelope (binary)
    let envelope_bytes = {
        let _span = tracing::debug_span!("v2_write_envelope").entered();
        let mut buf = Vec::new();
        envelope::encode_envelope(commit, &mut buf)?;
        buf
    };

    // 3. Encode all ops into a buffer, populating dictionaries
    let (ops_raw, dicts) = {
        let _span = tracing::debug_span!("v2_encode_ops", op_count).entered();
        let mut dicts = CommitDicts::new();
        let mut ops_raw = Vec::new();
        for flake in &commit.flakes {
            encode_op(flake, &mut dicts, &mut ops_raw)?;
        }
        (ops_raw, dicts)
    };

    // 4. Optionally compress ops
    let (ops_section, is_compressed) = if compress && !ops_raw.is_empty() {
        let _span = tracing::debug_span!("v2_compress_ops", raw_bytes = ops_raw.len()).entered();
        let compressed =
            zstd::encode_all(ops_raw.as_slice(), 3).map_err(CommitV2Error::CompressionFailed)?;
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

    // 5. Serialize dictionaries
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

    // 6. Calculate total size and allocate output
    let total_size = HEADER_LEN
        + envelope_bytes.len()
        + ops_section.len()
        + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
        + FOOTER_LEN
        + HASH_LEN
        + pre_sig_block_len as usize;
    let mut output = Vec::with_capacity(total_size);

    // 7. Write header (sig_block_len and FLAG_HAS_COMMIT_SIG set BEFORE hash)
    let mut flags = 0u8;
    if is_compressed {
        flags |= FLAG_ZSTD;
    }
    if signing.is_some() {
        flags |= FLAG_HAS_COMMIT_SIG;
    }
    let header = CommitV2Header {
        version: VERSION,
        flags,
        t: commit.t,
        op_count: commit.flakes.len() as u32,
        envelope_len: envelope_bytes.len() as u32,
        sig_block_len: pre_sig_block_len,
    };
    let mut header_buf = [0u8; HEADER_LEN];
    header.write_to(&mut header_buf);
    output.extend_from_slice(&header_buf);

    // 8. Write envelope
    output.extend_from_slice(&envelope_bytes);

    // 9. Write ops section
    output.extend_from_slice(&ops_section);

    // 10. Write dictionaries, recording locations
    let mut dict_locations = [DictLocation::default(); 5];
    for (i, bytes) in dict_bytes.iter().enumerate() {
        dict_locations[i] = DictLocation {
            offset: output.len() as u64,
            len: bytes.len() as u32,
        };
        output.extend_from_slice(bytes);
    }

    // 11. Write footer
    let footer = CommitV2Footer {
        dicts: dict_locations,
        ops_section_len: ops_section.len() as u32,
    };
    let mut footer_buf = [0u8; FOOTER_LEN];
    footer.write_to(&mut footer_buf);
    output.extend_from_slice(&footer_buf);

    // 12. Compute SHA-256 of everything so far, append as trailing hash
    let hash_bytes: [u8; 32] = {
        let _span = tracing::debug_span!("v2_write_hash", blob_bytes = output.len()).entered();
        Sha256::digest(&output).into()
    };
    let content_hash_hex = hex::encode(hash_bytes);
    output.extend_from_slice(&hash_bytes);

    // 13. If signing, compute domain-separated digest and append signature block
    if let Some((signing_key, ledger_id)) = signing {
        let _span = tracing::debug_span!("v2_write_sign").entered();
        let signature = sign_commit_digest(signing_key, &hash_bytes, ledger_id);
        let timestamp = chrono::Utc::now().timestamp_millis();
        let commit_sig = CommitSignature {
            signer: signer_did.expect("signer_did set when signing is Some"),
            algo: super::format::ALGO_ED25519,
            signature,
            timestamp,
            metadata: None,
        };
        encode_sig_block(&[commit_sig], &mut output);
    }

    debug_assert_eq!(output.len(), total_size);

    tracing::debug!(
        blob_bytes = output.len(),
        op_count,
        envelope_bytes = envelope_bytes.len(),
        ops_bytes = ops_section.len(),
        compressed = is_compressed,
        signed = signing.is_some(),
        "v2 commit written"
    );

    Ok(CommitWriteResult {
        bytes: output,
        content_hash_hex,
    })
}
