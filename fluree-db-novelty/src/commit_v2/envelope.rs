//! Binary envelope encode/decode for commit format v2.
//!
//! Compact binary encoding using varint primitives and binary CID
//! references. No serde_json dependency.
//!
//! Layout:
//! ```text
//! v: zigzag_varint(i32)
//! flags: u8               // presence bits for optional fields
//! [fields in bit order, only if corresponding bit set]
//! ```

use super::error::CommitV2Error;
use super::varint::{decode_varint, encode_varint, zigzag_decode, zigzag_encode};
use crate::{CommitRef, TxnMetaEntry, TxnMetaValue, TxnSignature, MAX_TXN_META_ENTRIES};
use fluree_db_core::ContentId;
use std::collections::HashMap;

// --- Presence flag bits ---
const FLAG_TXN_META: u8 = 0x01;
const FLAG_PREVIOUS_REF: u8 = 0x02;
const FLAG_NAMESPACE_DELTA: u8 = 0x04;
const FLAG_TXN: u8 = 0x08;
const FLAG_TIME: u8 = 0x10;
// Bits 5 and 6 are reserved (previously FLAG_DATA / FLAG_INDEX, never shipped).
const FLAG_TXN_SIGNATURE: u8 = 0x80;

/// Mask of all flag bits the current encoder/decoder understands.
const KNOWN_FLAGS: u8 = FLAG_TXN_META
    | FLAG_PREVIOUS_REF
    | FLAG_NAMESPACE_DELTA
    | FLAG_TXN
    | FLAG_TIME
    | FLAG_TXN_SIGNATURE;

/// Maximum number of named graph entries per commit.
pub const MAX_GRAPH_DELTA_ENTRIES: usize = 256;

/// Maximum length of a graph IRI in bytes.
pub const MAX_GRAPH_IRI_LENGTH: usize = 8192;

/// Maximum byte length for a length-prefixed CID blob.
/// Fluree SHA-256 CIDs are exactly 39 bytes; 128 provides headroom
/// for future hash algorithms while guarding against corrupt lengths.
const MAX_CID_BYTES: usize = 128;

/// Commit envelope fields — the non-flake metadata in a v2 commit blob.
///
/// Used for both encoding (by the streaming and batch writers) and decoding.
/// The `t` field is carried here for convenience but is actually stored in the
/// header, not the envelope section.
#[derive(Debug)]
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
// Encode
// =============================================================================

/// Encode envelope fields from a `CommitV2Envelope` into `buf`.
pub fn encode_envelope_fields(
    envelope: &CommitV2Envelope,
    buf: &mut Vec<u8>,
) -> Result<(), CommitV2Error> {
    // v (always present) — envelope format version
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
    if envelope.txn_signature.is_some() {
        flags |= FLAG_TXN_SIGNATURE;
    }
    buf.push(flags);

    // Fields in bit order
    if !envelope.txn_meta.is_empty() {
        encode_txn_meta(&envelope.txn_meta, buf)?;
    }
    if let Some(prev_ref) = &envelope.previous_ref {
        encode_commit_ref(prev_ref, buf)?;
    }
    if !envelope.namespace_delta.is_empty() {
        encode_ns_delta(&envelope.namespace_delta, buf);
    }
    if let Some(txn) = &envelope.txn {
        encode_len_bytes(&txn.to_bytes(), buf)?;
    }
    if let Some(time) = &envelope.time {
        encode_len_str(time, buf);
    }
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
// Decode
// =============================================================================

/// Decode the envelope from a binary slice.
///
/// The returned `CommitV2Envelope` has `t = 0` because `t` is stored in the
/// header, not the envelope. The caller should populate `t` from the header.
pub fn decode_envelope(data: &[u8]) -> Result<CommitV2Envelope, CommitV2Error> {
    let mut pos = 0;

    // v (always present) — envelope format version
    let v = zigzag_decode(decode_varint(data, &mut pos)?) as i32;
    if v != 2 {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "unsupported envelope version: {v} (expected 2)"
        )));
    }

    // flags
    if pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let flags = data[pos];
    pos += 1;

    // Reject unknown flag bits (forward safety: new flags require a new decoder)
    let unknown = flags & !KNOWN_FLAGS;
    if unknown != 0 {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "unknown envelope flags: 0x{:02x}",
            unknown
        )));
    }

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
        let cid_bytes = decode_len_bytes(data, &mut pos)?;
        Some(
            ContentId::from_bytes(cid_bytes)
                .map_err(|e| CommitV2Error::EnvelopeDecode(format!("invalid txn CID: {e}")))?,
        )
    } else {
        None
    };

    let time = if flags & FLAG_TIME != 0 {
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
    };

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
// Binary length-prefixed helpers (for CID bytes)
// =============================================================================

fn encode_len_bytes(bytes: &[u8], buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    if bytes.len() > MAX_CID_BYTES {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "CID byte length {} exceeds maximum {}",
            bytes.len(),
            MAX_CID_BYTES
        )));
    }
    encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Decode a length-prefixed byte slice, returning a borrow into `data`.
/// Advances `pos` past the consumed bytes. No allocation.
fn decode_len_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8], CommitV2Error> {
    let len64 = decode_varint(data, pos)?;
    if len64 > MAX_CID_BYTES as u64 {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "CID byte length {} exceeds maximum {}",
            len64, MAX_CID_BYTES
        )));
    }
    let len = len64 as usize;
    if *pos + len > data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let slice = &data[*pos..*pos + len];
    *pos += len;
    Ok(slice)
}

// =============================================================================
// CommitRef (binary CID encoding)
// =============================================================================

fn encode_commit_ref(cr: &CommitRef, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    encode_len_bytes(&cr.id.to_bytes(), buf)
}

fn decode_commit_ref(data: &[u8], pos: &mut usize) -> Result<CommitRef, CommitV2Error> {
    let cid_bytes = decode_len_bytes(data, pos)?;
    let content_id = ContentId::from_bytes(cid_bytes)
        .map_err(|e| CommitV2Error::EnvelopeDecode(format!("invalid commit ref CID: {e}")))?;
    Ok(CommitRef::new(content_id))
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
        // Simulate envelope without trailing graph_delta
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(2), &mut buf); // v=2
        buf.push(0); // no flags

        let d = decode_envelope(&buf).unwrap();
        assert!(d.graph_delta.is_empty());
    }

    #[test]
    fn test_round_trip_with_txn() {
        let txn_id = make_test_cid(ContentKind::Txn, "my-txn-blob");
        let mut commit = make_minimal_commit();
        commit.txn = Some(txn_id.clone());

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let decoded = decode_envelope(&buf).unwrap();
        assert_eq!(decoded.txn.as_ref(), Some(&txn_id));
    }

    #[test]
    fn test_unknown_flags_rejected() {
        // Bit 5 (0x20, formerly FLAG_DATA)
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(2), &mut buf);
        buf.push(0x20);
        buf.push(0); // no graph_delta
        let err = decode_envelope(&buf).unwrap_err();
        assert!(
            err.to_string().contains("unknown envelope flags"),
            "unexpected error: {err}"
        );

        // Bit 6 (0x40, formerly FLAG_INDEX)
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(2), &mut buf);
        buf.push(0x40);
        buf.push(0);
        let err = decode_envelope(&buf).unwrap_err();
        assert!(
            err.to_string().contains("unknown envelope flags"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unsupported_envelope_version_rejected() {
        // Version 0
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(0), &mut buf);
        buf.push(0x00); // flags
        buf.push(0); // graph_delta count
        let err = decode_envelope(&buf).unwrap_err();
        assert!(
            err.to_string().contains("unsupported envelope version"),
            "unexpected error: {err}"
        );

        // Version 1
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(1), &mut buf);
        buf.push(0x00);
        buf.push(0);
        let err = decode_envelope(&buf).unwrap_err();
        assert!(
            err.to_string().contains("unsupported envelope version"),
            "unexpected error: {err}"
        );

        // Version 3 (hypothetical future)
        let mut buf = Vec::new();
        encode_varint(zigzag_encode(3), &mut buf);
        buf.push(0x00);
        buf.push(0);
        let err = decode_envelope(&buf).unwrap_err();
        assert!(
            err.to_string().contains("unsupported envelope version"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_golden_bytes_previous_ref_and_txn() {
        // Deterministic CIDs from fixed inputs
        let prev_id = ContentId::new(ContentKind::Commit, b"golden-prev");
        let txn_id = ContentId::new(ContentKind::Txn, b"golden-txn");

        let mut commit = make_minimal_commit();
        commit.previous_ref = Some(CommitRef::new(prev_id.clone()));
        commit.txn = Some(txn_id.clone());

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        // Build expected bytes manually:
        let mut expected = Vec::new();
        // v = zigzag(2) = 4
        encode_varint(zigzag_encode(2), &mut expected);
        // flags = FLAG_PREVIOUS_REF | FLAG_TXN = 0x02 | 0x08 = 0x0A
        expected.push(0x0A);
        // previous_ref: varint(len) + CID binary bytes
        let prev_bytes = prev_id.to_bytes();
        encode_varint(prev_bytes.len() as u64, &mut expected);
        expected.extend_from_slice(&prev_bytes);
        // txn: varint(len) + CID binary bytes
        let txn_bytes = txn_id.to_bytes();
        encode_varint(txn_bytes.len() as u64, &mut expected);
        expected.extend_from_slice(&txn_bytes);
        // trailing graph_delta = 0 (empty)
        expected.push(0);

        assert_eq!(
            buf, expected,
            "wire format mismatch:\n  actual:   {:02x?}\n  expected: {:02x?}",
            buf, expected
        );

        // Verify CID binary length is exactly 39 bytes (SHA-256 + Fluree codecs)
        assert_eq!(prev_bytes.len(), 39, "commit CID binary should be 39 bytes");
        assert_eq!(txn_bytes.len(), 39, "txn CID binary should be 39 bytes");

        // Decode the golden bytes back and verify CIDs
        let decoded = decode_envelope(&expected).unwrap();
        assert_eq!(decoded.previous_ref.as_ref().unwrap().id, prev_id);
        assert_eq!(decoded.txn.as_ref(), Some(&txn_id));
    }
}
