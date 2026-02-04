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
use crate::{Commit, CommitData, CommitRef, IndexRef};
use std::collections::HashMap;

// --- Presence flag bits ---
const FLAG_PREVIOUS: u8 = 0x01;
const FLAG_PREVIOUS_REF: u8 = 0x02;
const FLAG_NAMESPACE_DELTA: u8 = 0x04;
const FLAG_TXN: u8 = 0x08;
const FLAG_TIME: u8 = 0x10;
const FLAG_DATA: u8 = 0x20;
const FLAG_INDEX: u8 = 0x40;
const FLAG_INDEXED_AT: u8 = 0x80;

/// Maximum recursion depth for CommitData.previous chain.
const MAX_COMMIT_DATA_DEPTH: usize = 16;

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
    pub previous: Option<String>,
    pub previous_ref: Option<CommitRef>,
    pub namespace_delta: HashMap<u16, String>,
    pub txn: Option<String>,
    pub time: Option<String>,
    pub data: Option<CommitData>,
    pub index: Option<IndexRef>,
    pub indexed_at: Option<String>,
}

impl CommitV2Envelope {
    /// Build an envelope from a `Commit` reference, extracting only the
    /// fields that the binary envelope encodes.
    pub fn from_commit(commit: &Commit) -> Self {
        Self {
            t: commit.t,
            v: commit.v,
            previous: commit.previous.clone(),
            previous_ref: commit.previous_ref.clone(),
            namespace_delta: commit.namespace_delta.clone(),
            txn: commit.txn.clone(),
            time: commit.time.clone(),
            data: commit.data.clone(),
            index: commit.index.clone(),
            indexed_at: commit.indexed_at.clone(),
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
    if envelope.previous.is_some() {
        flags |= FLAG_PREVIOUS;
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
    if envelope.indexed_at.is_some() {
        flags |= FLAG_INDEXED_AT;
    }
    buf.push(flags);

    // Fields in bit order
    if let Some(prev) = &envelope.previous {
        encode_len_str(prev, buf);
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
    if let Some(indexed_at) = &envelope.indexed_at {
        encode_len_str(indexed_at, buf);
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
    let previous = if flags & FLAG_PREVIOUS != 0 {
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
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

    let indexed_at = if flags & FLAG_INDEXED_AT != 0 {
        Some(decode_len_str(data, &mut pos)?)
    } else {
        None
    };

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
        previous,
        previous_ref,
        namespace_delta,
        txn,
        time,
        data: data_field,
        index,
        indexed_at,
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

fn encode_commit_data(cd: &CommitData, buf: &mut Vec<u8>, depth: usize) -> Result<(), CommitV2Error> {
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

fn decode_ns_delta(
    data: &[u8],
    pos: &mut usize,
) -> Result<HashMap<u16, String>, CommitV2Error> {
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
        assert!(decoded.previous.is_none());
        assert!(decoded.previous_ref.is_none());
        assert!(decoded.namespace_delta.is_empty());
        assert!(decoded.txn.is_none());
        assert!(decoded.time.is_none());
        assert!(decoded.data.is_none());
        assert!(decoded.index.is_none());
        assert!(decoded.indexed_at.is_none());
    }

    #[test]
    fn test_round_trip_full() {
        let mut commit = make_minimal_commit();
        commit.v = 3;
        commit.previous = Some("prev-addr".into());
        commit.previous_ref =
            Some(CommitRef::new("prev-addr").with_id("fluree:commit:sha256:abc"));
        commit.namespace_delta = HashMap::from([(100, "ex:".into()), (200, "schema:".into())]);
        commit.txn = Some("fluree:file://txn/xyz.json".into());
        commit.time = Some("2024-06-15T12:00:00Z".into());
        commit.data = Some(CommitData {
            id: Some("fluree:db:sha256:def".into()),
            address: Some("fluree:file://db/def.json".into()),
            flakes: 5000,
            size: 128000,
            previous: None,
        });
        commit.index = Some(IndexRef::new("fluree:file://index/root.json").with_id("fluree:index:sha256:ghi").with_t(8));
        commit.indexed_at = Some("legacy-index-addr".into());

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        let d = decode_envelope(&buf).unwrap();
        assert_eq!(d.v, 3);
        assert_eq!(d.previous.as_deref(), Some("prev-addr"));
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
        assert_eq!(data.id.as_deref(), Some("fluree:db:sha256:def"));
        assert_eq!(data.address.as_deref(), Some("fluree:file://db/def.json"));
        assert_eq!(data.flakes, 5000);
        assert_eq!(data.size, 128000);
        assert!(data.previous.is_none());
        let idx = d.index.as_ref().unwrap();
        assert_eq!(idx.address, "fluree:file://index/root.json");
        assert_eq!(idx.id.as_deref(), Some("fluree:index:sha256:ghi"));
        assert_eq!(idx.t, Some(8));
        assert_eq!(d.indexed_at.as_deref(), Some("legacy-index-addr"));
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
        commit.namespace_delta = (0u16..200)
            .map(|i| (i, format!("ns{}:", i)))
            .collect();

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
        commit.previous = Some("some-long-address-value".into());

        let mut buf = Vec::new();
        encode_envelope(&commit, &mut buf).unwrap();

        // Progressively truncate and verify we get an error
        for len in 0..buf.len() {
            let result = decode_envelope(&buf[..len]);
            assert!(result.is_err(), "should fail at truncated length {}", len);
        }
        // Full buffer should succeed
        assert!(decode_envelope(&buf).is_ok());
    }

    #[test]
    fn test_individual_flags() {
        // Test each flag individually
        let test_cases: Vec<Box<dyn Fn(&mut Commit)>> = vec![
            Box::new(|c: &mut Commit| c.previous = Some("prev".into())),
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
            Box::new(|c: &mut Commit| c.indexed_at = Some("legacy".into())),
        ];

        for (i, setter) in test_cases.iter().enumerate() {
            let mut commit = make_minimal_commit();
            setter(&mut commit);

            let mut buf = Vec::new();
            encode_envelope(&commit, &mut buf).unwrap();

            let d = decode_envelope(&buf).unwrap();

            // Only the i-th field should be set
            assert_eq!(d.previous.is_some(), i == 0, "flag bit {}", i);
            assert_eq!(d.previous_ref.is_some(), i == 1, "flag bit {}", i);
            assert_eq!(!d.namespace_delta.is_empty(), i == 2, "flag bit {}", i);
            assert_eq!(d.txn.is_some(), i == 3, "flag bit {}", i);
            assert_eq!(d.time.is_some(), i == 4, "flag bit {}", i);
            assert_eq!(d.data.is_some(), i == 5, "flag bit {}", i);
            assert_eq!(d.index.is_some(), i == 6, "flag bit {}", i);
            assert_eq!(d.indexed_at.is_some(), i == 7, "flag bit {}", i);
        }
    }
}
