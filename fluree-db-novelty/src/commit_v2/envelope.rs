//! Binary envelope encode/decode for commit format v2.
//!
//! Replaces the JSON envelope with a compact binary encoding using
//! varint primitives. No serde_json dependency.
//!
//! Layout:
//! ```text
//! v: zigzag_varint(i32)
//! flags: u8               // presence bits for 8 optional fields
//! [fields in bit order, only if corresponding bit set]
//! ```

use super::error::CommitV2Error;
use super::varint::{decode_varint, encode_varint, zigzag_decode, zigzag_encode};
use crate::{
    Commit, CommitData, CommitRef, IndexRef, TxnMetaEntry, TxnMetaValue, TxnSignature,
    MAX_TXN_META_ENTRIES,
};
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

/// Maximum recursion depth for CommitData.previous chain.
const MAX_COMMIT_DATA_DEPTH: usize = 16;

/// Maximum number of named graph entries per commit.
///
/// This limits the number of new named graphs that can be introduced in a
/// single transaction. Transactions exceeding this limit will fail during
/// commit encoding. 256 is a generous limit that covers most use cases while
/// preventing unbounded growth.
pub const MAX_GRAPH_DELTA_ENTRIES: usize = 256;

/// Maximum length of a graph IRI in bytes.
///
/// IRIs exceeding this limit will cause commit encoding to fail. 8KB is
/// generous for IRIs while preventing abuse with extremely long values.
pub const MAX_GRAPH_IRI_LENGTH: usize = 8192;

/// Commit envelope fields — the non-flake metadata in a v2 commit blob.
///
/// Used for both encoding (by the streaming and batch writers) and decoding.
/// The `t` field is carried here for convenience but is actually stored in the
/// header, not the envelope section. `encode_envelope_fields` ignores `t`;
/// the writer puts it in the header.
pub struct CommitV2Envelope {
    /// Transaction `t` (stored in header, not in the envelope bytes).
    pub t: i64,
    pub v: i32,
    pub previous_ref: Option<CommitRef>,
    pub namespace_delta: HashMap<u16, String>,
    pub txn: Option<String>,
    pub time: Option<String>,
    pub data: Option<CommitData>,
    pub index: Option<IndexRef>,
    pub txn_signature: Option<TxnSignature>,
    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this commit.
    ///
    /// Stored as trailing optional data (not flag-controlled) for backward
    /// compatibility. When empty, nothing is written. When present, a
    /// presence byte (1) followed by the encoded map is appended.
    ///
    /// Reserved g_ids: 0=default, 1=txn-meta, 2+=user-defined.
    pub graph_delta: HashMap<u32, String>,
}

impl CommitV2Envelope {
    /// Build an envelope from a `Commit` reference, extracting only the
    /// fields that the binary envelope encodes.
    pub fn from_commit(commit: &Commit) -> Self {
        Self {
            t: commit.t,
            v: commit.v,
            previous_ref: commit.previous_ref.clone(),
            namespace_delta: commit.namespace_delta.clone(),
            txn: commit.txn.clone(),
            time: commit.time.clone(),
            data: commit.data.clone(),
            index: commit.index.clone(),
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
///
/// This is the primary encoding function. The `t` field is NOT encoded here
/// (it belongs in the header); only v + flags + conditional fields are written.
pub fn encode_envelope_fields(
    envelope: &CommitV2Envelope,
    buf: &mut Vec<u8>,
) -> Result<(), CommitV2Error> {
    // v (always present)
    encode_varint(zigzag_encode(envelope.v as i64), buf);

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
    if envelope.data.is_some() {
        flags |= FLAG_DATA;
    }
    if envelope.index.is_some() {
        flags |= FLAG_INDEX;
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
        encode_commit_ref(prev_ref, buf);
    }
    if !envelope.namespace_delta.is_empty() {
        encode_ns_delta(&envelope.namespace_delta, buf);
    }
    if let Some(txn) = &envelope.txn {
        encode_len_str(txn, buf);
    }
    if let Some(time) = &envelope.time {
        encode_len_str(time, buf);
    }
    if let Some(data) = &envelope.data {
        encode_commit_data(data, buf, 0)?;
    }
    if let Some(index) = &envelope.index {
        encode_index_ref(index, buf);
    }
    if let Some(txn_sig) = &envelope.txn_signature {
        encode_len_str(&txn_sig.signer, buf);
        // txn_id (optional string)
        if let Some(txn_id) = &txn_sig.txn_id {
            buf.push(1);
            encode_len_str(txn_id, buf);
        } else {
            buf.push(0);
        }
    }

    // Trailing optional extensions (not flag-controlled)
    // graph_delta: presence byte + encoded map (only if non-empty)
    if !envelope.graph_delta.is_empty() {
        // Validate graph_delta limits
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
        buf.push(1); // has graph_delta
        encode_graph_delta(&envelope.graph_delta, buf);
    } else {
        buf.push(0); // no graph_delta
    }

    Ok(())
}

/// Encode the envelope fields of a commit into `buf`.
///
/// Convenience wrapper that extracts fields from `&Commit` and delegates
/// to [`encode_envelope_fields`].
pub fn encode_envelope(commit: &Commit, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
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

    // v (always present)
    let v = zigzag_decode(decode_varint(data, &mut pos)?) as i32;

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
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
    };

    let time = if flags & FLAG_TIME != 0 {
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
    };

    let data_field = if flags & FLAG_DATA != 0 {
        Some(decode_commit_data(data, &mut pos, 0)?)
    } else {
        None
    };

    let index = if flags & FLAG_INDEX != 0 {
        Some(decode_index_ref(data, &mut pos)?)
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
        // txn_id (optional)
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

    // Trailing optional extensions (not flag-controlled)
    // For backward compatibility, missing trailing data means no extensions.
    let graph_delta = if pos < data.len() {
        let has_graph_delta = data[pos] != 0;
        pos += 1;
        if has_graph_delta {
            decode_graph_delta(data, &mut pos)?
        } else {
            HashMap::new()
        }
    } else {
        // Old commit format: no trailing data
        HashMap::new()
    };

    // Any remaining bytes after known extensions are an error
    if pos != data.len() {
        return Err(CommitV2Error::EnvelopeDecode(format!(
            "trailing bytes: consumed {} of {} bytes",
            pos,
            data.len()
        )));
    }

    Ok(CommitV2Envelope {
        t: 0, // populated by caller from header
        v,
        previous_ref,
        namespace_delta,
        txn,
        time,
        data: data_field,
        index,
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
// CommitRef
// =============================================================================

fn encode_commit_ref(cr: &CommitRef, buf: &mut Vec<u8>) {
    if let Some(id) = &cr.id {
        buf.push(1);
        encode_len_str(id, buf);
    } else {
        buf.push(0);
    }
    encode_len_str(&cr.address, buf);
}

fn decode_commit_ref(data: &[u8], pos: &mut usize) -> Result<CommitRef, CommitV2Error> {
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
    let address = decode_len_str(data, pos)?;
    let mut cr = CommitRef::new(address);
    if let Some(id) = id {
        cr = cr.with_id(id);
    }
    Ok(cr)
}

// =============================================================================
// CommitData (recursive)
// =============================================================================

fn encode_commit_data(
    cd: &CommitData,
    buf: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CommitV2Error> {
    if depth >= MAX_COMMIT_DATA_DEPTH {
        return Err(CommitV2Error::EnvelopeEncode(
            "CommitData recursion too deep".into(),
        ));
    }

    // id
    if let Some(id) = &cd.id {
        buf.push(1);
        encode_len_str(id, buf);
    } else {
        buf.push(0);
    }

    // address
    if let Some(addr) = &cd.address {
        buf.push(1);
        encode_len_str(addr, buf);
    } else {
        buf.push(0);
    }

    // flakes count (u64)
    encode_varint(cd.flakes, buf);

    // size (u64)
    encode_varint(cd.size, buf);

    // previous (recursive)
    if let Some(prev) = &cd.previous {
        buf.push(1);
        encode_commit_data(prev, buf, depth + 1)?;
    } else {
        buf.push(0);
    }

    Ok(())
}

fn decode_commit_data(
    data: &[u8],
    pos: &mut usize,
    depth: usize,
) -> Result<CommitData, CommitV2Error> {
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

    // flakes
    let flakes = decode_varint(data, pos)?;

    // size
    let size = decode_varint(data, pos)?;

    // previous (recursive)
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_prev = data[*pos] != 0;
    *pos += 1;
    let previous = if has_prev {
        Some(Box::new(decode_commit_data(data, pos, depth + 1)?))
    } else {
        None
    };

    Ok(CommitData {
        id,
        address,
        flakes,
        size,
        previous,
    })
}

// =============================================================================
// IndexRef
// =============================================================================

fn encode_index_ref(ir: &IndexRef, buf: &mut Vec<u8>) {
    // id
    if let Some(id) = &ir.id {
        buf.push(1);
        encode_len_str(id, buf);
    } else {
        buf.push(0);
    }
    // address
    encode_len_str(&ir.address, buf);
    // v
    encode_varint(zigzag_encode(ir.v as i64), buf);
    // t
    if let Some(t) = ir.t {
        buf.push(1);
        encode_varint(zigzag_encode(t), buf);
    } else {
        buf.push(0);
    }
}

fn decode_index_ref(data: &[u8], pos: &mut usize) -> Result<IndexRef, CommitV2Error> {
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
    let address = decode_len_str(data, pos)?;

    // v
    let v = zigzag_decode(decode_varint(data, pos)?) as i32;

    // t
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let has_t = data[*pos] != 0;
    *pos += 1;
    let t = if has_t {
        Some(zigzag_decode(decode_varint(data, pos)?))
    } else {
        None
    };

    let mut ir = IndexRef::new(address);
    if let Some(id) = id {
        ir = ir.with_id(id);
    }
    if let Some(t) = t {
        ir = ir.with_t(t);
    }
    // Restore v (IndexRef::new defaults to 2)
    ir.v = v;

    Ok(ir)
}

// =============================================================================
// namespace_delta (HashMap<u16, String>)
// =============================================================================

fn encode_ns_delta(delta: &HashMap<u16, String>, buf: &mut Vec<u8>) {
    encode_varint(delta.len() as u64, buf);
    // Sort by key for deterministic encoding
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
// graph_delta (HashMap<u32, String>) — named graph IRI to g_id mappings
// =============================================================================

fn encode_graph_delta(delta: &HashMap<u32, String>, buf: &mut Vec<u8>) {
    encode_varint(delta.len() as u64, buf);
    // Sort by g_id for deterministic encoding
    let mut entries: Vec<_> = delta.iter().collect();
    entries.sort_by_key(|(g_id, _)| **g_id);
    for (g_id, iri) in entries {
        encode_varint(*g_id as u64, buf);
        encode_len_str(iri, buf);
    }
}

fn decode_graph_delta(
    data: &[u8],
    pos: &mut usize,
) -> Result<HashMap<u32, String>, CommitV2Error> {
    let count = decode_varint(data, pos)? as usize;
    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        let g_id = decode_varint(data, pos)? as u32;
        let iri = decode_len_str(data, pos)?;
        map.insert(g_id, iri);
    }
    Ok(map)
}

// =============================================================================
// txn_meta (Vec<TxnMetaEntry>)
// =============================================================================

/// Value type tags for TxnMetaValue encoding
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
        // Predicate: ns_code + name
        encode_varint(entry.predicate_ns as u64, buf);
        encode_len_str(&entry.predicate_name, buf);
        // Value
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
        TxnMetaValue::TypedLiteral { value, dt_ns, dt_name } => {
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
            count,
            MAX_TXN_META_ENTRIES
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
            Ok(TxnMetaValue::TypedLiteral { value, dt_ns, dt_name })
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
    fn make_minimal_commit() -> Commit {
        Commit::new("", 1, vec![])
    }

    #[test]
    fn test_round_trip_minimal() {
        let commit = make_minimal_commit();
        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let decoded = decode_envelope(&buf).unwrap();
        assert_eq!(decoded.v, 2); // default version
        assert!(decoded.previous_ref.is_none());
        assert!(decoded.namespace_delta.is_empty());
        assert!(decoded.txn.is_none());
        assert!(decoded.time.is_none());
        assert!(decoded.data.is_none());
        assert!(decoded.index.is_none());
        assert!(decoded.txn_meta.is_empty());
    }

    #[test]
    fn test_round_trip_full() {
        let mut commit = make_minimal_commit();
        commit.v = 3;
        commit.previous_ref = Some(CommitRef::new("prev-addr").with_id("fluree:commit:sha256:abc"));
        commit.namespace_delta = HashMap::from([(100, "ex:".into()), (200, "schema:".into())]);
        commit.txn = Some("fluree:file://txn/xyz.json".into());
        commit.time = Some("2024-06-15T12:00:00Z".into());
        commit.data = Some(CommitData {
            id: None,
            address: Some("fluree:file://db/def.json".into()),
            flakes: 5000,
            size: 128000,
            previous: None,
        });
        commit.index = Some(
            IndexRef::new("fluree:file://index/root.json")
                .with_id("fluree:index:sha256:ghi")
                .with_t(8),
        );

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.v, 3);
        assert_eq!(d.previous_ref.as_ref().unwrap().address, "prev-addr");
        assert_eq!(
            d.previous_ref.as_ref().unwrap().id.as_deref(),
            Some("fluree:commit:sha256:abc")
        );
        assert_eq!(d.namespace_delta.len(), 2);
        assert_eq!(d.namespace_delta[&100], "ex:");
        assert_eq!(d.namespace_delta[&200], "schema:");
        assert_eq!(d.txn.as_deref(), Some("fluree:file://txn/xyz.json"));
        assert_eq!(d.time.as_deref(), Some("2024-06-15T12:00:00Z"));
        let data = d.data.as_ref().unwrap();
        assert!(data.id.is_none());
        assert_eq!(data.address.as_deref(), Some("fluree:file://db/def.json"));
        assert_eq!(data.flakes, 5000);
        assert_eq!(data.size, 128000);
        assert!(data.previous.is_none());
        let idx = d.index.as_ref().unwrap();
        assert_eq!(idx.address, "fluree:file://index/root.json");
        assert_eq!(idx.id.as_deref(), Some("fluree:index:sha256:ghi"));
        assert_eq!(idx.t, Some(8));
    }

    #[test]
    fn test_round_trip_recursive_commit_data() {
        let mut commit = make_minimal_commit();
        commit.data = Some(CommitData {
            id: Some("db:2".into()),
            address: None,
            flakes: 2000,
            size: 50000,
            previous: Some(Box::new(CommitData {
                id: Some("db:1".into()),
                address: Some("addr:1".into()),
                flakes: 1000,
                size: 25000,
                previous: None,
            })),
        });

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        let data = d.data.as_ref().unwrap();
        assert_eq!(data.flakes, 2000);
        let prev = data.previous.as_ref().unwrap();
        assert_eq!(prev.id.as_deref(), Some("db:1"));
        assert_eq!(prev.flakes, 1000);
        assert!(prev.previous.is_none());
    }

    #[test]
    fn test_round_trip_large_namespace_delta() {
        let mut commit = make_minimal_commit();
        commit.namespace_delta = (0u16..200).map(|i| (i, format!("ns{}:", i))).collect();

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.namespace_delta.len(), 200);
        for i in 0u16..200 {
            assert_eq!(d.namespace_delta[&i], format!("ns{}:", i));
        }
    }

    #[test]
    fn test_round_trip_various_ns_codes() {
        let mut commit = make_minimal_commit();
        commit.namespace_delta = HashMap::from([
            (1, "f:".into()),
            (50, "rdf:".into()),
            (0, "default:".into()),
            (999, "ex:".into()),
        ]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.namespace_delta[&1], "f:");
        assert_eq!(d.namespace_delta[&50], "rdf:");
        assert_eq!(d.namespace_delta[&0], "default:");
        assert_eq!(d.namespace_delta[&999], "ex:");
    }

    #[test]
    fn test_decode_truncated() {
        let mut commit = make_minimal_commit();
        commit.previous_ref = Some(CommitRef::new("some-long-address-value"));

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        // The last byte is the trailing graph_delta presence byte (0).
        // Truncating that byte is valid (old format compatibility), so we only
        // test truncations up to buf.len() - 1.
        let required_len = buf.len() - 1; // Everything except trailing presence byte

        // Progressively truncate and verify we get an error for required bytes
        for len in 0..required_len {
            let result = decode_envelope(&buf[..len]);
            assert!(result.is_err(), "should fail at truncated length {}", len);
        }

        // Truncating just the trailing presence byte should succeed (old format)
        assert!(decode_envelope(&buf[..required_len]).is_ok(),
            "should succeed without trailing presence byte");

        // Full buffer should succeed
        assert!(decode_envelope(&buf).is_ok());
    }

    /// A function that mutates a Commit for testing purposes
    type CommitMutator = Box<dyn Fn(&mut Commit)>;

    #[test]
    fn test_individual_flags() {
        // Test each active flag individually (legacy flags not tested here)
        let test_cases: Vec<CommitMutator> = vec![
            Box::new(|c: &mut Commit| c.previous_ref = Some(CommitRef::new("addr"))),
            Box::new(|c: &mut Commit| {
                c.namespace_delta = HashMap::from([(1, "ns:".into())]);
            }),
            Box::new(|c: &mut Commit| c.txn = Some("txn-addr".into())),
            Box::new(|c: &mut Commit| c.time = Some("2024-01-01T00:00:00Z".into())),
            Box::new(|c: &mut Commit| {
                c.data = Some(CommitData::default());
            }),
            Box::new(|c: &mut Commit| c.index = Some(IndexRef::new("idx-addr"))),
        ];

        for (i, setter) in test_cases.iter().enumerate() {
            let mut commit = make_minimal_commit();
            setter(&mut commit);

            let mut buf = Vec::new();
            encode_envelope(&commit, &mut buf).unwrap();

            let d = decode_envelope(&buf).unwrap();

            // Only the i-th field should be set
            assert_eq!(d.previous_ref.is_some(), i == 0, "flag bit {}", i);
            assert_eq!(!d.namespace_delta.is_empty(), i == 1, "flag bit {}", i);
            assert_eq!(d.txn.is_some(), i == 2, "flag bit {}", i);
            assert_eq!(d.time.is_some(), i == 3, "flag bit {}", i);
            assert_eq!(d.data.is_some(), i == 4, "flag bit {}", i);
            assert_eq!(d.index.is_some(), i == 5, "flag bit {}", i);
        }
    }

    // =========================================================================
    // txn_meta tests
    // =========================================================================

    #[test]
    fn test_round_trip_txn_meta_string() {
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
        assert_eq!(d.txn_meta[1].predicate_name, "userId");
        assert_eq!(d.txn_meta[1].value, TxnMetaValue::String("user-123".into()));
    }

    #[test]
    fn test_round_trip_txn_meta_all_types() {
        let mut commit = make_minimal_commit();
        commit.txn_meta = vec![
            TxnMetaEntry::new(100, "strVal", TxnMetaValue::String("hello".into())),
            TxnMetaEntry::new(100, "longVal", TxnMetaValue::Long(42)),
            TxnMetaEntry::new(100, "negLong", TxnMetaValue::Long(-999)),
            TxnMetaEntry::new(100, "boolTrue", TxnMetaValue::Boolean(true)),
            TxnMetaEntry::new(100, "boolFalse", TxnMetaValue::Boolean(false)),
            TxnMetaEntry::new(100, "doubleVal", TxnMetaValue::Double(1.23456)),
            TxnMetaEntry::new(100, "refVal", TxnMetaValue::Ref { ns: 50, name: "Alice".into() }),
            TxnMetaEntry::new(100, "langStr", TxnMetaValue::LangString {
                value: "bonjour".into(),
                lang: "fr".into(),
            }),
            TxnMetaEntry::new(100, "typedLit", TxnMetaValue::TypedLiteral {
                value: "2025-01-01".into(),
                dt_ns: 2,
                dt_name: "date".into(),
            }),
        ];

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.txn_meta.len(), 9);

        // Verify each value type
        assert_eq!(d.txn_meta[0].value, TxnMetaValue::String("hello".into()));
        assert_eq!(d.txn_meta[1].value, TxnMetaValue::Long(42));
        assert_eq!(d.txn_meta[2].value, TxnMetaValue::Long(-999));
        assert_eq!(d.txn_meta[3].value, TxnMetaValue::Boolean(true));
        assert_eq!(d.txn_meta[4].value, TxnMetaValue::Boolean(false));

        // Double comparison with epsilon
        if let TxnMetaValue::Double(n) = d.txn_meta[5].value {
            assert!((n - 1.23456).abs() < 1e-10);
        } else {
            panic!("expected Double");
        }

        assert_eq!(d.txn_meta[6].value, TxnMetaValue::Ref { ns: 50, name: "Alice".into() });
        assert_eq!(d.txn_meta[7].value, TxnMetaValue::LangString {
            value: "bonjour".into(),
            lang: "fr".into(),
        });
        assert_eq!(d.txn_meta[8].value, TxnMetaValue::TypedLiteral {
            value: "2025-01-01".into(),
            dt_ns: 2,
            dt_name: "date".into(),
        });
    }

    #[test]
    fn test_txn_meta_flag_only_when_non_empty() {
        // Empty txn_meta should not set FLAG_TXN_META (bit 0)
        let commit = make_minimal_commit();
        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        // buf[0] is v (zigzag varint), buf[1] is flags byte
        // v=2 encodes as zigzag 0x04 (single byte), so flags is at buf[1]
        let flags = buf[1];
        assert_eq!(flags & FLAG_TXN_META, 0, "FLAG_TXN_META should be unset for empty txn_meta");

        // With txn_meta, flag should be set
        let mut commit2 = make_minimal_commit();
        commit2.txn_meta = vec![TxnMetaEntry::new(1, "test", TxnMetaValue::Boolean(true))];
        let mut buf2 = Vec::new();
        encode_envelope(&commit2, &mut buf2).unwrap();

        let flags2 = buf2[1];
        assert_ne!(flags2 & FLAG_TXN_META, 0, "FLAG_TXN_META should be set for non-empty txn_meta");

        // Verify decode works for both
        let d = decode_envelope(&buf).unwrap();
        assert!(d.txn_meta.is_empty());
        let d2 = decode_envelope(&buf2).unwrap();
        assert_eq!(d2.txn_meta.len(), 1);
    }

    #[test]
    fn test_txn_meta_with_other_fields() {
        // Verify txn_meta works alongside other envelope fields
        let mut commit = make_minimal_commit();
        commit.previous_ref = Some(CommitRef::new("prev-addr"));
        commit.time = Some("2025-01-01T00:00:00Z".into());
        commit.txn_meta = vec![
            TxnMetaEntry::new(100, "jobId", TxnMetaValue::String("job-123".into())),
        ];

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.previous_ref.as_ref().unwrap().address, "prev-addr");
        assert_eq!(d.time.as_deref(), Some("2025-01-01T00:00:00Z"));
        assert_eq!(d.txn_meta.len(), 1);
        assert_eq!(d.txn_meta[0].value, TxnMetaValue::String("job-123".into()));
    }

    #[test]
    fn test_txn_meta_exceeds_max_entries_encode() {
        use crate::MAX_TXN_META_ENTRIES;

        let mut commit = make_minimal_commit();
        // Create MAX_TXN_META_ENTRIES + 1 entries
        commit.txn_meta = (0..=MAX_TXN_META_ENTRIES)
            .map(|i| TxnMetaEntry::new(1, format!("key{}", i), TxnMetaValue::Long(i as i64)))
            .collect();

        let mut buf = Vec::new();
        let result = encode_envelope(&commit, &mut buf);
        match result {
            Ok(_) => panic!("expected error for too many entries"),
            Err(e) => assert!(e.to_string().contains("exceeds maximum")),
        }
    }

    #[test]
    fn test_txn_meta_at_max_entries_ok() {
        use crate::MAX_TXN_META_ENTRIES;

        let mut commit = make_minimal_commit();
        // Create exactly MAX_TXN_META_ENTRIES entries
        commit.txn_meta = (0..MAX_TXN_META_ENTRIES)
            .map(|i| TxnMetaEntry::new(1, format!("key{}", i), TxnMetaValue::Long(i as i64)))
            .collect();

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.txn_meta.len(), MAX_TXN_META_ENTRIES);
    }

    #[test]
    fn test_txn_meta_decode_rejects_non_finite_double() {
        // Manually construct bytes with a non-finite double
        // This simulates a corrupted or malicious commit blob
        let mut buf = Vec::new();

        // v=2 (zigzag encoded)
        encode_varint(zigzag_encode(2), &mut buf);

        // flags with only FLAG_TXN_META set
        buf.push(FLAG_TXN_META);

        // txn_meta: 1 entry
        encode_varint(1, &mut buf);
        // predicate_ns = 1
        encode_varint(1, &mut buf);
        // predicate_name = "test"
        encode_varint(4, &mut buf);
        buf.extend_from_slice(b"test");
        // value: Double tag
        buf.push(TXN_META_TAG_DOUBLE);
        // NaN bytes
        buf.extend_from_slice(&f64::NAN.to_le_bytes());

        // Trailing graph_delta presence byte (no graph_delta)
        buf.push(0);

        let result = decode_envelope(&buf);
        match result {
            Ok(_) => panic!("expected error for non-finite double"),
            Err(e) => assert!(e.to_string().contains("non-finite")),
        }
    }

    // =========================================================================
    // graph_delta tests
    // =========================================================================

    #[test]
    fn test_round_trip_graph_delta_empty() {
        // Empty graph_delta should work and round-trip correctly
        let commit = make_minimal_commit();
        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert!(d.graph_delta.is_empty());
    }

    #[test]
    fn test_round_trip_graph_delta_single() {
        let mut commit = make_minimal_commit();
        commit.graph_delta = HashMap::from([
            (2, "http://example.org/graph/products".into()),
        ]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.graph_delta.len(), 1);
        assert_eq!(d.graph_delta.get(&2), Some(&"http://example.org/graph/products".to_string()));
    }

    #[test]
    fn test_round_trip_graph_delta_multiple() {
        let mut commit = make_minimal_commit();
        commit.graph_delta = HashMap::from([
            (2, "http://example.org/graph/products".into()),
            (3, "http://example.org/graph/orders".into()),
            (10, "http://example.org/graph/customers".into()),
        ]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.graph_delta.len(), 3);
        assert_eq!(d.graph_delta.get(&2), Some(&"http://example.org/graph/products".to_string()));
        assert_eq!(d.graph_delta.get(&3), Some(&"http://example.org/graph/orders".to_string()));
        assert_eq!(d.graph_delta.get(&10), Some(&"http://example.org/graph/customers".to_string()));
    }

    #[test]
    fn test_round_trip_graph_delta_with_other_fields() {
        // Verify graph_delta works alongside other envelope fields
        let mut commit = make_minimal_commit();
        commit.previous_ref = Some(CommitRef::new("prev-addr"));
        commit.namespace_delta = HashMap::from([(100, "ex:".into())]);
        commit.txn_meta = vec![TxnMetaEntry::new(100, "job", TxnMetaValue::String("j1".into()))];
        commit.graph_delta = HashMap::from([
            (5, "http://example.org/named-graph".into()),
        ]);

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.previous_ref.as_ref().unwrap().address, "prev-addr");
        assert_eq!(d.namespace_delta.get(&100), Some(&"ex:".to_string()));
        assert_eq!(d.txn_meta.len(), 1);
        assert_eq!(d.graph_delta.get(&5), Some(&"http://example.org/named-graph".to_string()));
    }

    #[test]
    fn test_decode_old_format_without_trailing_data() {
        // Simulate an old commit format that doesn't have trailing graph_delta
        // The decoder should handle this gracefully and return empty graph_delta
        let mut buf = Vec::new();

        // v=2 (zigzag encoded)
        encode_varint(zigzag_encode(2), &mut buf);
        // No flags set
        buf.push(0);
        // No trailing data (old format)

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.v, 2);
        assert!(d.graph_delta.is_empty());
    }

    #[test]
    fn test_graph_delta_deterministic_encoding() {
        // Verify graph_delta entries are encoded in sorted order (by g_id)
        // This ensures deterministic encoding for content-addressing
        let mut commit = make_minimal_commit();
        commit.graph_delta = HashMap::from([
            (10, "z-graph".into()),
            (2, "a-graph".into()),
            (5, "m-graph".into()),
        ]);

        let mut buf1 = Vec::new();
        encode_envelope(&commit, &mut buf1).unwrap();

        // Encode again to verify determinism
        let mut buf2 = Vec::new();
        encode_envelope(&commit, &mut buf2).unwrap();

        assert_eq!(buf1, buf2, "encoding should be deterministic");

        // Decode and verify all entries are present
        let d = decode_envelope(&buf1).unwrap();
        assert_eq!(d.graph_delta.len(), 3);
    }
}
