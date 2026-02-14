//! Binary envelope encode/decode for commit format v2.
//!
//! Replaces the JSON envelope with a compact binary encoding using
//! varint primitives. No serde_json dependency.
//!
//! ## V2 ↔ CID-based types
//!
//! The v2 on-disk format stores address strings and optional id strings.
//! This module converts between the on-disk format and the CID-based
//! in-memory types (`CommitRef`, etc.) during encode/decode.
//!
//! Layout:
//! ```text
//! v: zigzag_varint(i32)
//! flags: u8               // presence bits for 8 optional fields
//! [fields in bit order, only if corresponding bit set]
//! ```

use super::error::CommitV2Error;
use super::varint::{decode_varint, encode_varint, zigzag_decode, zigzag_encode};
use crate::{CommitRef, TxnMetaEntry, TxnMetaValue, TxnSignature, MAX_TXN_META_ENTRIES};
use fluree_db_core::{ContentId, CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN};
use std::collections::HashMap;

// --- Presence flag bits ---
// Bit 0 was previously unused; now used for txn_meta
const FLAG_TXN_META: u8 = 0x01;
const FLAG_PREVIOUS_REF: u8 = 0x02;
const FLAG_NAMESPACE_DELTA: u8 = 0x04;
const FLAG_TXN: u8 = 0x08;
const FLAG_TIME: u8 = 0x10;
const FLAG_DATA: u8 = 0x20;
const FLAG_INDEX: u8 = 0x40;
const FLAG_TXN_SIGNATURE: u8 = 0x80;

/// Maximum recursion depth for CommitData.previous chain (v2 backward compat).
const MAX_COMMIT_DATA_DEPTH: usize = 16;

/// Maximum number of named graph entries per commit.
pub const MAX_GRAPH_DELTA_ENTRIES: usize = 256;

/// Maximum length of a graph IRI in bytes.
pub const MAX_GRAPH_IRI_LENGTH: usize = 8192;

/// Legacy CommitData — kept for v2 backward-compatible decoding only.
///
/// Not part of the public API. When reading v2 commits that contain
/// embedded DB metadata (FLAG_DATA), we decode and discard it.
#[derive(Clone, Debug, Default)]
#[allow(dead_code)] // Fields populated during decode-and-discard of legacy FLAG_DATA
struct LegacyCommitData {
    id: Option<String>,
    address: Option<String>,
    flakes: u64,
    size: u64,
    previous: Option<Box<LegacyCommitData>>,
}

/// Commit envelope fields — the non-flake metadata in a v2 commit blob.
///
/// Used for both encoding (by the streaming and batch writers) and decoding.
/// The `t` field is carried here for convenience but is actually stored in the
/// header, not the envelope section.
pub struct CommitV2Envelope {
    /// Transaction `t` (stored in header, not in the envelope bytes).
    pub t: i64,
    /// Previous commit reference (CID-based)
    pub previous_ref: Option<CommitRef>,
    pub namespace_delta: HashMap<u16, String>,
    /// Transaction blob CID
    pub txn: Option<ContentId>,
    pub time: Option<String>,
    pub txn_signature: Option<TxnSignature>,
    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this commit.
    pub graph_delta: HashMap<u16, String>,
}

impl CommitV2Envelope {
    /// Build an envelope from a `Commit` reference.
    pub fn from_commit(commit: &crate::Commit) -> Self {
        Self {
            t: commit.t,
            previous_ref: commit.previous_ref.clone(),
            namespace_delta: commit.namespace_delta.clone(),
            txn: commit.txn.clone(),
            time: commit.time.clone(),
            txn_signature: commit.txn_signature.clone(),
            txn_meta: commit.txn_meta.clone(),
            graph_delta: commit.graph_delta.clone(),
        }
    }
}

// =============================================================================
// Encode (CID-based types → v2 binary wire format)
// =============================================================================

/// Encode envelope fields from a `CommitV2Envelope` into `buf`.
///
/// CID fields are converted to legacy string format for v2 wire compatibility.
pub fn encode_envelope_fields(
    envelope: &CommitV2Envelope,
    buf: &mut Vec<u8>,
) -> Result<(), CommitV2Error> {
    // v (always present) — v2 format version
    encode_varint(zigzag_encode(2), buf);

    // Build presence flags
    let mut flags: u8 = 0;
    if !envelope.txn_meta.is_empty() {
        flags |= FLAG_TXN_META;
    }
    if envelope.previous_ref.is_some() {
        flags |= FLAG_PREVIOUS_REF;
    }
    if !envelope.namespace_delta.is_empty() {
        flags |= FLAG_NAMESPACE_DELTA;
    }
    if envelope.txn.is_some() {
        flags |= FLAG_TXN;
    }
    if envelope.time.is_some() {
        flags |= FLAG_TIME;
    }
    // FLAG_DATA never set — CommitData removed from new commits
    // FLAG_INDEX never set — index pointers are nameservice-only
    if envelope.txn_signature.is_some() {
        flags |= FLAG_TXN_SIGNATURE;
    }
    buf.push(flags);

    // Fields in bit order
    if !envelope.txn_meta.is_empty() {
        encode_txn_meta(&envelope.txn_meta, buf)?;
    }
    if let Some(prev_ref) = &envelope.previous_ref {
        encode_commit_ref(prev_ref, buf);
    }
    if !envelope.namespace_delta.is_empty() {
        encode_ns_delta(&envelope.namespace_delta, buf);
    }
    if let Some(txn) = &envelope.txn {
        // Encode ContentId as its string form for v2 wire compat
        let txn_str = txn.to_string();
        encode_len_str(&txn_str, buf);
    }
    if let Some(time) = &envelope.time {
        encode_len_str(time, buf);
    }
    // FLAG_DATA: not set, no encoding needed
    // FLAG_INDEX: not set, no encoding needed (index pointers are nameservice-only)
    if let Some(txn_sig) = &envelope.txn_signature {
        encode_len_str(&txn_sig.signer, buf);
        if let Some(txn_id) = &txn_sig.txn_id {
            buf.push(1);
            encode_len_str(txn_id, buf);
        } else {
            buf.push(0);
        }
    }

    // Trailing optional extensions (not flag-controlled)
    if !envelope.graph_delta.is_empty() {
        if envelope.graph_delta.len() > MAX_GRAPH_DELTA_ENTRIES {
            return Err(CommitV2Error::LimitExceeded(format!(
                "graph_delta has {} entries, max is {}",
                envelope.graph_delta.len(),
                MAX_GRAPH_DELTA_ENTRIES
            )));
        }
        for (g_id, iri) in &envelope.graph_delta {
            if iri.len() > MAX_GRAPH_IRI_LENGTH {
                return Err(CommitV2Error::LimitExceeded(format!(
                    "graph_delta[{}] IRI is {} bytes, max is {}",
                    g_id,
                    iri.len(),
                    MAX_GRAPH_IRI_LENGTH
                )));
            }
        }
        buf.push(1);
        encode_graph_delta(&envelope.graph_delta, buf);
    } else {
        buf.push(0);
    }

    Ok(())
}

/// Encode the envelope fields of a commit into `buf`.
pub fn encode_envelope(commit: &crate::Commit, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    let envelope = CommitV2Envelope::from_commit(commit);
    encode_envelope_fields(&envelope, buf)
}

// =============================================================================
// Decode (v2 binary wire format → CID-based types)
// =============================================================================

/// Decode the envelope from a binary slice.
///
/// The returned `CommitV2Envelope` has `t = 0` because `t` is stored in the
/// header, not the envelope. The caller should populate `t` from the header.
///
/// V2 string-based references are converted to CID-based types.
pub fn decode_envelope(data: &[u8]) -> Result<CommitV2Envelope, CommitV2Error> {
    let mut pos = 0;

    // v (always present) — informational, we know it's v2
    let _v = zigzag_decode(decode_varint(data, &mut pos)?) as i32;

    // flags
    if pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let flags = data[pos];
    pos += 1;

    // Fields in bit order
    let txn_meta = if flags & FLAG_TXN_META != 0 {
        decode_txn_meta(data, &mut pos)?
    } else {
        Vec::new()
    };

    let previous_ref = if flags & FLAG_PREVIOUS_REF != 0 {
        Some(decode_commit_ref(data, &mut pos)?)
    } else {
        None
    };

    let namespace_delta = if flags & FLAG_NAMESPACE_DELTA != 0 {
        decode_ns_delta(data, &mut pos)?
    } else {
        HashMap::new()
    };

    let txn = if flags & FLAG_TXN != 0 {
        let txn_str = decode_len_str(data, &mut pos)?;
        // Try to parse as CID first, fall back to deriving from address
        txn_content_id_from_v2_string(&txn_str)
    } else {
        None
    };

    let time = if flags & FLAG_TIME != 0 {
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
    };

    // FLAG_DATA: decode and discard for backward compat
    if flags & FLAG_DATA != 0 {
        let _legacy_data = decode_legacy_commit_data(data, &mut pos, 0)?;
    }

    // FLAG_INDEX: skip for tolerant decode of stray dev artifacts
    // (index pointers are nameservice-only)
    if flags & FLAG_INDEX != 0 {
        skip_legacy_index_ref(data, &mut pos)?;
    }

    let txn_signature = if flags & FLAG_TXN_SIGNATURE != 0 {
        let signer = decode_len_str(data, &mut pos)?;
        if signer.len() > 256 {
            return Err(CommitV2Error::EnvelopeDecode(format!(
                "txn_signature signer length {} exceeds maximum 256",
                signer.len()
            )));
        }
        if pos >= data.len() {
            return Err(CommitV2Error::UnexpectedEof);
        }
        let has_txn_id = data[pos] != 0;
        pos += 1;
        let txn_id = if has_txn_id {
            let id = decode_len_str(data, &mut pos)?;
            if id.len() > 256 {
                return Err(CommitV2Error::EnvelopeDecode(format!(
                    "txn_signature txn_id length {} exceeds maximum 256",
                    id.len()
                )));
            }
            Some(id)
        } else {
            None
        };
        Some(TxnSignature { signer, txn_id })
    } else {
        None
    };

    // Trailing optional extensions
    let graph_delta = if pos < data.len() {
        let has_graph_delta = data[pos] != 0;
        pos += 1;
        if has_graph_delta {
            decode_graph_delta(data, &mut pos)?
        } else {
            HashMap::new()
        }
    } else {
        HashMap::new()
    };

    if pos != data.len() {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "trailing bytes: consumed {} of {} bytes",
            pos,
            data.len()
        )));
    }

    Ok(CommitV2Envelope {
        t: 0,
        previous_ref,
        namespace_delta,
        txn,
        time,
        txn_signature,
        txn_meta,
        graph_delta,
    })
}

// =============================================================================
// V2 → CID conversion helpers
// =============================================================================

/// Convert a v2 txn address string to a ContentId.
fn txn_content_id_from_v2_string(s: &str) -> Option<ContentId> {
    // Try parsing as CID string first (for newer data)
    if let Ok(cid) = s.parse::<ContentId>() {
        return Some(cid);
    }
    // Fall back to extracting hash from legacy address
    let path = if let Some(rest) = s.strip_prefix("fluree:") {
        let pos = rest.find("://")?;
        &rest[pos + 3..]
    } else if let Some(pos) = s.find("://") {
        &s[pos + 3..]
    } else {
        return None;
    };
    let last_segment = path.rsplit('/').next()?;
    let hash = last_segment
        .strip_suffix(".fcv2")
        .or_else(|| last_segment.strip_suffix(".json"))
        .unwrap_or(last_segment);
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        ContentId::from_hex_digest(CODEC_FLUREE_TXN, hash)
    } else {
        None
    }
}

// =============================================================================
// String helpers
// =============================================================================

fn encode_len_str(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

fn decode_len_str(data: &[u8], pos: &mut usize) -> Result<String, CommitV2Error> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| CommitV2Error::EnvelopeDecode(format!("invalid UTF-8: {}", e)))?;
    *pos += len;
    Ok(s.to_string())
}

// =============================================================================
// CommitRef (v2 wire: id_string + address → CID-based CommitRef)
// =============================================================================

fn encode_commit_ref(cr: &CommitRef, buf: &mut Vec<u8>) {
    // v2 wire format: has_id(u8) + optional id_string + address_string
    // Encode CID as the id string, and derive address from digest hex
    let id_str = cr.id.to_string();
    buf.push(1); // has_id = true
    encode_len_str(&id_str, buf);
    // For address, use the digest hex as a placeholder since v2 format requires it
    let addr = format!("cid:{}", cr.id.digest_hex());
    encode_len_str(&addr, buf);
}

fn decode_commit_ref(data: &[u8], pos: &mut usize) -> Result<CommitRef, CommitV2Error> {
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_id = data[*pos] != 0;
    *pos += 1;
    let id_str = if has_id {
        Some(decode_len_str(data, pos)?)
    } else {
        None
    };
    let address = decode_len_str(data, pos)?;

    // Convert v2 strings to ContentId
    let content_id = try_content_id_from_v2_ref(id_str.as_deref(), &address, CODEC_FLUREE_COMMIT)
        .ok_or_else(|| {
        CommitV2Error::EnvelopeDecode(format!(
            "cannot derive ContentId from commit ref (id={:?}, addr={})",
            id_str, address
        ))
    })?;

    Ok(CommitRef::new(content_id))
}

// =============================================================================
// Legacy CommitData (decode-only for backward compat)
// =============================================================================

fn decode_legacy_commit_data(
    data: &[u8],
    pos: &mut usize,
    depth: usize,
) -> Result<LegacyCommitData, CommitV2Error> {
    if depth >= MAX_COMMIT_DATA_DEPTH {
        return Err(CommitV2Error::EnvelopeDecode(
            "CommitData recursion too deep".into(),
        ));
    }

    // id
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_id = data[*pos] != 0;
    *pos += 1;
    let id = if has_id {
        Some(decode_len_str(data, pos)?)
    } else {
        None
    };

    // address
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_addr = data[*pos] != 0;
    *pos += 1;
    let address = if has_addr {
        Some(decode_len_str(data, pos)?)
    } else {
        None
    };

    let flakes = decode_varint(data, pos)?;
    let size = decode_varint(data, pos)?;

    // previous (recursive)
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_prev = data[*pos] != 0;
    *pos += 1;
    let previous = if has_prev {
        Some(Box::new(decode_legacy_commit_data(data, pos, depth + 1)?))
    } else {
        None
    };

    Ok(LegacyCommitData {
        id,
        address,
        flakes,
        size,
        previous,
    })
}

// =============================================================================
// Legacy IndexRef skip (tolerant decode for stray dev artifacts)
// =============================================================================

/// Skip past a legacy index ref in the wire format.
///
/// Index pointers are no longer part of commits — they are tracked exclusively
/// via the nameservice. This function advances the cursor past any index ref
/// bytes in old blobs without constructing a value.
fn skip_legacy_index_ref(data: &[u8], pos: &mut usize) -> Result<(), CommitV2Error> {
    // id
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_id = data[*pos] != 0;
    *pos += 1;
    if has_id {
        let _id_str = decode_len_str(data, pos)?;
    }

    // address
    let _address = decode_len_str(data, pos)?;

    // v
    let _v = decode_varint(data, pos)?;

    // t
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_t = data[*pos] != 0;
    *pos += 1;
    if has_t {
        let _t = decode_varint(data, pos)?;
    }

    Ok(())
}

/// Try to derive a ContentId from v2 id/address strings.
fn try_content_id_from_v2_ref(
    id_str: Option<&str>,
    address: &str,
    codec: u64,
) -> Option<ContentId> {
    // Try CID string first (for newer data written with CID-based encoder)
    if let Some(id) = id_str {
        if let Ok(cid) = id.parse::<ContentId>() {
            return Some(cid);
        }
    }

    // Try extracting hash from id string (legacy format: fluree:commit:sha256:HEX)
    if let Some(id) = id_str {
        let hex = id
            .strip_prefix("fluree:commit:sha256:")
            .or_else(|| id.strip_prefix("fluree:commit:"))
            .or_else(|| id.strip_prefix("fluree:index:sha256:"))
            .or_else(|| id.strip_prefix("fluree:index:"));
        if let Some(hex) = hex {
            if let Some(cid) = ContentId::from_hex_digest(codec, hex) {
                return Some(cid);
            }
        }
    }

    // Try extracting hash from address (cid:HEX or fluree:file:///path/HEX.json)
    if let Some(hex) = address.strip_prefix("cid:") {
        if let Some(cid) = ContentId::from_hex_digest(codec, hex) {
            return Some(cid);
        }
    }

    // Legacy address format: fluree:file:///path/HEX.fcv2 (or .json for backward compat)
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        rest.find("://").map(|pos| &rest[pos + 3..])
    } else {
        address.find("://").map(|pos| &address[pos + 3..])
    };
    if let Some(path) = path {
        if let Some(last) = path.rsplit('/').next() {
            let hash = last
                .strip_suffix(".fcv2")
                .or_else(|| last.strip_suffix(".json"))
                .unwrap_or(last);
            if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                return ContentId::from_hex_digest(codec, hash);
            }
        }
    }

    None
}

// =============================================================================
// namespace_delta (HashMap<u16, String>)
// =============================================================================

fn encode_ns_delta(delta: &HashMap<u16, String>, buf: &mut Vec<u8>) {
    encode_varint(delta.len() as u64, buf);
    let mut entries: Vec<_> = delta.iter().collect();
    entries.sort_by_key(|(k, _)| **k);
    for (code, prefix) in entries {
        encode_varint(*code as u64, buf);
        encode_len_str(prefix, buf);
    }
}

fn decode_ns_delta(data: &[u8], pos: &mut usize) -> Result<HashMap<u16, String>, CommitV2Error> {
    let count = decode_varint(data, pos)? as usize;
    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        let code = decode_varint(data, pos)? as u16;
        let prefix = decode_len_str(data, pos)?;
        map.insert(code, prefix);
    }
    Ok(map)
}

// =============================================================================
// graph_delta (HashMap<u16, String>)
// =============================================================================

fn encode_graph_delta(delta: &HashMap<u16, String>, buf: &mut Vec<u8>) {
    encode_varint(delta.len() as u64, buf);
    let mut entries: Vec<_> = delta.iter().collect();
    entries.sort_by_key(|(g_id, _)| **g_id);
    for (g_id, iri) in entries {
        encode_varint(*g_id as u64, buf);
        encode_len_str(iri, buf);
    }
}

fn decode_graph_delta(data: &[u8], pos: &mut usize) -> Result<HashMap<u16, String>, CommitV2Error> {
    let count = decode_varint(data, pos)? as usize;
    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        let raw = decode_varint(data, pos)?;
        let g_id = u16::try_from(raw).map_err(|_| CommitV2Error::GIdOutOfRange(raw))?;
        let iri = decode_len_str(data, pos)?;
        map.insert(g_id, iri);
    }
    Ok(map)
}

// =============================================================================
// txn_meta (Vec<TxnMetaEntry>)
// =============================================================================

const TXN_META_TAG_STRING: u8 = 0;
const TXN_META_TAG_TYPED_LITERAL: u8 = 1;
const TXN_META_TAG_LANG_STRING: u8 = 2;
const TXN_META_TAG_REF: u8 = 3;
const TXN_META_TAG_LONG: u8 = 4;
const TXN_META_TAG_DOUBLE: u8 = 5;
const TXN_META_TAG_BOOLEAN: u8 = 6;

fn encode_txn_meta(entries: &[TxnMetaEntry], buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    if entries.len() > MAX_TXN_META_ENTRIES {
        return Err(CommitV2Error::EnvelopeEncode(format!(
            "txn_meta entry count {} exceeds maximum {}",
            entries.len(),
            MAX_TXN_META_ENTRIES
        )));
    }
    encode_varint(entries.len() as u64, buf);
    for entry in entries {
        encode_varint(entry.predicate_ns as u64, buf);
        encode_len_str(&entry.predicate_name, buf);
        encode_txn_meta_value(&entry.value, buf)?;
    }
    Ok(())
}

fn encode_txn_meta_value(value: &TxnMetaValue, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    match value {
        TxnMetaValue::String(s) => {
            buf.push(TXN_META_TAG_STRING);
            encode_len_str(s, buf);
        }
        TxnMetaValue::TypedLiteral {
            value,
            dt_ns,
            dt_name,
        } => {
            buf.push(TXN_META_TAG_TYPED_LITERAL);
            encode_len_str(value, buf);
            encode_varint(*dt_ns as u64, buf);
            encode_len_str(dt_name, buf);
        }
        TxnMetaValue::LangString { value, lang } => {
            buf.push(TXN_META_TAG_LANG_STRING);
            encode_len_str(value, buf);
            encode_len_str(lang, buf);
        }
        TxnMetaValue::Ref { ns, name } => {
            buf.push(TXN_META_TAG_REF);
            encode_varint(*ns as u64, buf);
            encode_len_str(name, buf);
        }
        TxnMetaValue::Long(n) => {
            buf.push(TXN_META_TAG_LONG);
            encode_varint(zigzag_encode(*n), buf);
        }
        TxnMetaValue::Double(n) => {
            if !n.is_finite() {
                return Err(CommitV2Error::EnvelopeEncode(
                    "txn_meta does not support non-finite double values".into(),
                ));
            }
            buf.push(TXN_META_TAG_DOUBLE);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        TxnMetaValue::Boolean(b) => {
            buf.push(TXN_META_TAG_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
    }
    Ok(())
}

fn decode_txn_meta(data: &[u8], pos: &mut usize) -> Result<Vec<TxnMetaEntry>, CommitV2Error> {
    let count = decode_varint(data, pos)? as usize;
    if count > MAX_TXN_META_ENTRIES {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "txn_meta entry count {} exceeds maximum {}",
            count, MAX_TXN_META_ENTRIES
        )));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let predicate_ns = decode_varint(data, pos)? as u16;
        let predicate_name = decode_len_str(data, pos)?;
        let value = decode_txn_meta_value(data, pos)?;
        entries.push(TxnMetaEntry {
            predicate_ns,
            predicate_name,
            value,
        });
    }
    Ok(entries)
}

fn decode_txn_meta_value(data: &[u8], pos: &mut usize) -> Result<TxnMetaValue, CommitV2Error> {
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let tag = data[*pos];
    *pos += 1;

    match tag {
        TXN_META_TAG_STRING => {
            let s = decode_len_str(data, pos)?;
            Ok(TxnMetaValue::String(s))
        }
        TXN_META_TAG_TYPED_LITERAL => {
            let value = decode_len_str(data, pos)?;
            let dt_ns = decode_varint(data, pos)? as u16;
            let dt_name = decode_len_str(data, pos)?;
            Ok(TxnMetaValue::TypedLiteral {
                value,
                dt_ns,
                dt_name,
            })
        }
        TXN_META_TAG_LANG_STRING => {
            let value = decode_len_str(data, pos)?;
            let lang = decode_len_str(data, pos)?;
            Ok(TxnMetaValue::LangString { value, lang })
        }
        TXN_META_TAG_REF => {
            let ns = decode_varint(data, pos)? as u16;
            let name = decode_len_str(data, pos)?;
            Ok(TxnMetaValue::Ref { ns, name })
        }
        TXN_META_TAG_LONG => {
            let n = zigzag_decode(decode_varint(data, pos)?);
            Ok(TxnMetaValue::Long(n))
        }
        TXN_META_TAG_DOUBLE => {
            if *pos + 8 > data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
            *pos += 8;
            let n = f64::from_le_bytes(bytes);
            if !n.is_finite() {
                return Err(CommitV2Error::EnvelopeDecode(
                    "txn_meta contains non-finite double value".into(),
                ));
            }
            Ok(TxnMetaValue::Double(n))
        }
        TXN_META_TAG_BOOLEAN => {
            if *pos >= data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let b = data[*pos] != 0;
            *pos += 1;
            Ok(TxnMetaValue::Boolean(b))
        }
        _ => Err(CommitV2Error::EnvelopeDecode(format!(
            "unknown txn_meta value tag: {}",
            tag
        ))),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn make_test_cid(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn make_minimal_commit() -> crate::Commit {
        crate::Commit::new(1, vec![])
    }

    #[test]
    fn test_round_trip_minimal() {
        let commit = make_minimal_commit();
        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let decoded = decode_envelope(&buf).unwrap();
        assert!(decoded.previous_ref.is_none());
        assert!(decoded.namespace_delta.is_empty());
        assert!(decoded.txn.is_none());
        assert!(decoded.time.is_none());
        assert!(decoded.txn_meta.is_empty());
    }

    #[test]
    fn test_round_trip_with_previous_ref() {
        let prev_id = make_test_cid(ContentKind::Commit, "prev-commit");
        let mut commit = make_minimal_commit();
        commit.previous_ref = Some(CommitRef::new(prev_id.clone()));

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let decoded = decode_envelope(&buf).unwrap();
        let decoded_prev = decoded.previous_ref.unwrap();
        assert_eq!(decoded_prev.id, prev_id);
    }

    #[test]
    fn test_round_trip_namespace_delta() {
        let mut commit = make_minimal_commit();
        commit.namespace_delta = HashMap::from([(100, "ex:".into()), (200, "schema:".into())]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.namespace_delta.len(), 2);
        assert_eq!(d.namespace_delta[&100], "ex:");
        assert_eq!(d.namespace_delta[&200], "schema:");
    }

    #[test]
    fn test_round_trip_txn_meta() {
        let mut commit = make_minimal_commit();
        commit.txn_meta = vec![
            TxnMetaEntry::new(100, "machine", TxnMetaValue::String("10.2.3.4".into())),
            TxnMetaEntry::new(100, "userId", TxnMetaValue::String("user-123".into())),
        ];

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.txn_meta.len(), 2);
        assert_eq!(d.txn_meta[0].predicate_ns, 100);
        assert_eq!(d.txn_meta[0].predicate_name, "machine");
        assert_eq!(d.txn_meta[0].value, TxnMetaValue::String("10.2.3.4".into()));
    }

    #[test]
    fn test_round_trip_graph_delta() {
        let mut commit = make_minimal_commit();
        commit.graph_delta = HashMap::from([
            (2, "http://example.org/graph/products".into()),
            (3, "http://example.org/graph/orders".into()),
        ]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.graph_delta.len(), 2);
        assert_eq!(
            d.graph_delta.get(&2),
            Some(&"http://example.org/graph/products".to_string())
        );
    }

    #[test]
    fn test_decode_old_format_without_trailing_data() {
        // Simulate old commit format without trailing graph_delta
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(2), &mut buf); // v=2
        buf.push(0); // no flags

        let d = decode_envelope(&buf).unwrap();
        assert!(d.graph_delta.is_empty());
    }

    #[test]
    fn test_try_content_id_from_v2_ref_cid_string() {
        let original = make_test_cid(ContentKind::Commit, "test");
        let cid_str = original.to_string();

        let result =
            try_content_id_from_v2_ref(Some(&cid_str), "dummy-address", CODEC_FLUREE_COMMIT);
        assert_eq!(result, Some(original));
    }

    #[test]
    fn test_try_content_id_from_v2_ref_legacy_id() {
        let hash = "a".repeat(64);
        let id_str = format!("fluree:commit:sha256:{}", hash);

        let result = try_content_id_from_v2_ref(Some(&id_str), "dummy", CODEC_FLUREE_COMMIT);
        assert!(result.is_some());
        assert_eq!(result.unwrap().digest_hex(), hash);
    }

    #[test]
    fn test_try_content_id_from_v2_ref_legacy_address_json() {
        // Backward compat: .json commit addresses should still parse
        let hash = "b".repeat(64);
        let address = format!("fluree:file:///data/commit/{}.json", hash);

        let result = try_content_id_from_v2_ref(None, &address, CODEC_FLUREE_COMMIT);
        assert!(result.is_some());
        assert_eq!(result.unwrap().digest_hex(), hash);
    }

    #[test]
    fn test_try_content_id_from_v2_ref_legacy_address_fcv2() {
        let hash = "b".repeat(64);
        let address = format!("fluree:file:///data/commit/{}.fcv2", hash);

        let result = try_content_id_from_v2_ref(None, &address, CODEC_FLUREE_COMMIT);
        assert!(result.is_some());
        assert_eq!(result.unwrap().digest_hex(), hash);
    }
}
