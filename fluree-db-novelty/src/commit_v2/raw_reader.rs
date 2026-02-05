//! Zero-allocation raw op reader for commit-v2 blobs.
//!
//! Decodes ops directly into borrowed field references (`&str`) without
//! constructing `Flake`, `Sid`, or `FlakeValue`. Designed for the dictionary
//! resolution pipeline (Phase B of INDEX_BUILD_STRATEGY.md).
//!
//! # Usage
//!
//! ```ignore
//! let ops = load_commit_ops(blob_bytes)?;
//! ops.for_each_op(|raw_op| {
//!     // raw_op.s_name, raw_op.p_name, etc. borrow from CommitOps
//!     // No heap allocation per op (except novel dictionary entries in resolver)
//!     Ok(())
//! })?;
//! ```

use super::error::CommitV2Error;
use super::format::{
    CommitV2Footer, CommitV2Header, OTag, FLAG_ZSTD, FOOTER_LEN, HASH_LEN, HEADER_LEN,
    MIN_COMMIT_LEN, OP_FLAG_ASSERT, OP_FLAG_HAS_I, OP_FLAG_HAS_LANG,
};
use super::op_codec::ReadDicts;
use super::reader::load_dicts;
use super::varint::{decode_varint, zigzag_decode};
use super::CommitV2Envelope;
use sha2::{Digest, Sha256};

// ============================================================================
// CommitOps
// ============================================================================

/// Owns decompressed ops buffer + loaded string dicts.
/// Provides zero-allocation iteration over raw decoded ops.
pub struct CommitOps {
    /// Parsed envelope (non-flake metadata: previous, namespace_delta, etc.)
    pub envelope: CommitV2Envelope,
    /// Transaction number from the header.
    pub t: i64,
    /// Number of ops in this commit.
    pub op_count: u32,
    /// The five commit-local string dictionaries (owned).
    dicts: ReadDicts,
    /// Decompressed ops bytes (owned). `RawOp` borrows from this + `dicts`.
    ops_data: Vec<u8>,
}

impl CommitOps {
    /// Iterate raw ops, calling `f` for each. No Flake/Sid construction.
    ///
    /// The callback receives a `RawOp` with borrowed `&str` fields from
    /// the commit's string dictionaries and ops buffer.
    pub fn for_each_op<F>(&self, mut f: F) -> Result<(), CommitV2Error>
    where
        F: FnMut(RawOp<'_>) -> Result<(), CommitV2Error>,
    {
        let data = &self.ops_data;
        let mut pos = 0;

        for _ in 0..self.op_count {
            let raw_op = decode_raw_op(data, &mut pos, &self.dicts)?;
            f(raw_op)?;
        }

        // All ops decoded — position should be exactly at the end of the ops buffer.
        debug_assert_eq!(
            pos,
            data.len(),
            "raw_reader: {} trailing bytes after decoding {} ops",
            data.len() - pos,
            self.op_count,
        );

        Ok(())
    }
}

// ============================================================================
// RawOp + RawObject
// ============================================================================

/// Raw decoded op — borrows from CommitOps' dicts and ops buffer.
/// Stack-allocated, no heap allocation per op.
pub struct RawOp<'a> {
    /// Graph namespace code (0 = empty prefix for default graph).
    pub g_ns_code: u16,
    /// Graph local name from graph dict ("" for default graph).
    pub g_name: &'a str,
    /// Subject namespace code.
    pub s_ns_code: u16,
    /// Subject local name from subject dict.
    pub s_name: &'a str,
    /// Predicate namespace code.
    pub p_ns_code: u16,
    /// Predicate local name from predicate dict.
    pub p_name: &'a str,
    /// Datatype namespace code.
    pub dt_ns_code: u16,
    /// Datatype local name from datatype dict.
    pub dt_name: &'a str,
    /// Object value (borrows inline strings from ops buffer).
    pub o: RawObject<'a>,
    /// true = assert, false = retract.
    pub op: bool,
    /// Language tag (if present), borrowed from ops buffer.
    pub lang: Option<&'a str>,
    /// List index (if present).
    pub i: Option<i32>,
}

/// Object value without allocation. Borrows from ops buffer or dicts.
pub enum RawObject<'a> {
    /// IRI reference: namespace code + local name from object_ref dict.
    Ref { ns_code: u16, name: &'a str },
    /// Integer value (already parsed by varint decoder).
    Long(i64),
    /// Double value (already parsed from LE bytes).
    Double(f64),
    /// String value, borrowed from ops buffer (inline UTF-8).
    Str(&'a str),
    /// Boolean value.
    Boolean(bool),
    /// DateTime lexical form, borrowed from ops buffer.
    DateTimeStr(&'a str),
    /// Date lexical form, borrowed from ops buffer.
    DateStr(&'a str),
    /// Time lexical form, borrowed from ops buffer.
    TimeStr(&'a str),
    /// BigInt lexical form, borrowed from ops buffer.
    BigIntStr(&'a str),
    /// Decimal lexical form, borrowed from ops buffer.
    DecimalStr(&'a str),
    /// JSON string, borrowed from ops buffer.
    JsonStr(&'a str),
    /// Null value.
    Null,
    /// gYear lexical form, borrowed from ops buffer.
    GYearStr(&'a str),
    /// gYearMonth lexical form, borrowed from ops buffer.
    GYearMonthStr(&'a str),
    /// gMonth lexical form, borrowed from ops buffer.
    GMonthStr(&'a str),
    /// gDay lexical form, borrowed from ops buffer.
    GDayStr(&'a str),
    /// gMonthDay lexical form, borrowed from ops buffer.
    GMonthDayStr(&'a str),
    /// yearMonthDuration lexical form, borrowed from ops buffer.
    YearMonthDurationStr(&'a str),
    /// dayTimeDuration lexical form, borrowed from ops buffer.
    DayTimeDurationStr(&'a str),
    /// duration lexical form, borrowed from ops buffer.
    DurationStr(&'a str),
}

// ============================================================================
// Load + decode
// ============================================================================

/// Load a commit blob into CommitOps.
///
/// Validates the blob, verifies SHA-256, decompresses ops, and loads dicts.
/// Does NOT decode individual ops — call `for_each_op()` for that.
pub fn load_commit_ops(bytes: &[u8]) -> Result<CommitOps, CommitV2Error> {
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

    // 3. Verify hash
    {
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

    // 4. Decode envelope
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

    // 6. Ops section bounds
    let ops_start = envelope_end;
    let ops_end = ops_start + footer.ops_section_len as usize;
    if ops_end > footer_start {
        return Err(CommitV2Error::InvalidOp(
            "ops section extends into footer".into(),
        ));
    }

    // 7. Load dicts
    let dicts = load_dicts(bytes, &footer, ops_end, footer_start)?;

    // 8. Decompress ops into owned buffer
    let ops_bytes = &bytes[ops_start..ops_end];
    let ops_data = if header.flags & FLAG_ZSTD != 0 {
        zstd::decode_all(ops_bytes).map_err(CommitV2Error::DecompressionFailed)?
    } else {
        ops_bytes.to_vec()
    };

    Ok(CommitOps {
        envelope,
        t: header.t,
        op_count: header.op_count,
        dicts,
        ops_data,
    })
}

// ============================================================================
// Internal decode (zero-alloc per op)
// ============================================================================

/// Decode a single raw op from the ops buffer. Returns borrowed references.
fn decode_raw_op<'a>(
    data: &'a [u8],
    pos: &mut usize,
    dicts: &'a ReadDicts,
) -> Result<RawOp<'a>, CommitV2Error> {
    // Graph
    let g_ns_code = decode_varint(data, pos)? as u16;
    let g_name_id = decode_varint(data, pos)? as u32;
    let g_name = if g_name_id == 0 {
        "" // default graph convention: name_id 0 = empty name
    } else {
        dicts.graph.get(g_name_id)?
    };

    // Subject
    let s_ns_code = decode_varint(data, pos)? as u16;
    let s_name_id = decode_varint(data, pos)? as u32;
    let s_name = dicts.subject.get(s_name_id)?;

    // Predicate
    let p_ns_code = decode_varint(data, pos)? as u16;
    let p_name_id = decode_varint(data, pos)? as u32;
    let p_name = dicts.predicate.get(p_name_id)?;

    // Datatype
    let dt_ns_code = decode_varint(data, pos)? as u16;
    let dt_name_id = decode_varint(data, pos)? as u32;
    let dt_name = dicts.datatype.get(dt_name_id)?;

    // Object tag
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let o_tag = OTag::from_u8(data[*pos])?;
    *pos += 1;
    let o = decode_raw_object(o_tag, data, pos, dicts)?;

    // Flags
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let flags = data[*pos];
    *pos += 1;
    let op = flags & OP_FLAG_ASSERT != 0;

    // Optional lang (borrowed from ops buffer)
    let lang = if flags & OP_FLAG_HAS_LANG != 0 {
        Some(decode_inline_str(data, pos)?)
    } else {
        None
    };

    // Optional list index
    let i = if flags & OP_FLAG_HAS_I != 0 {
        let raw = decode_varint(data, pos)?;
        Some(zigzag_decode(raw) as i32)
    } else {
        None
    };

    Ok(RawOp {
        g_ns_code,
        g_name,
        s_ns_code,
        s_name,
        p_ns_code,
        p_name,
        dt_ns_code,
        dt_name,
        o,
        op,
        lang,
        i,
    })
}

/// Decode an object value without allocation. String-like values borrow
/// from the ops data buffer.
fn decode_raw_object<'a>(
    tag: OTag,
    data: &'a [u8],
    pos: &mut usize,
    dicts: &'a ReadDicts,
) -> Result<RawObject<'a>, CommitV2Error> {
    match tag {
        OTag::Ref => {
            let ns_code = decode_varint(data, pos)? as u16;
            let name_id = decode_varint(data, pos)? as u32;
            let name = dicts.object_ref.get(name_id)?;
            Ok(RawObject::Ref { ns_code, name })
        }
        OTag::Long => {
            let raw = decode_varint(data, pos)?;
            Ok(RawObject::Long(zigzag_decode(raw)))
        }
        OTag::Double => {
            if *pos + 8 > data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
            *pos += 8;
            Ok(RawObject::Double(f64::from_le_bytes(bytes)))
        }
        OTag::String => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::Str(s))
        }
        OTag::Boolean => {
            if *pos >= data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let b = data[*pos] != 0;
            *pos += 1;
            Ok(RawObject::Boolean(b))
        }
        OTag::DateTime => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::DateTimeStr(s))
        }
        OTag::Date => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::DateStr(s))
        }
        OTag::Time => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::TimeStr(s))
        }
        OTag::BigInt => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::BigIntStr(s))
        }
        OTag::Decimal => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::DecimalStr(s))
        }
        OTag::Json => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::JsonStr(s))
        }
        OTag::Null => Ok(RawObject::Null),
        OTag::GYear => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::GYearStr(s))
        }
        OTag::GYearMonth => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::GYearMonthStr(s))
        }
        OTag::GMonth => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::GMonthStr(s))
        }
        OTag::GDay => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::GDayStr(s))
        }
        OTag::GMonthDay => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::GMonthDayStr(s))
        }
        OTag::YearMonthDuration => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::YearMonthDurationStr(s))
        }
        OTag::DayTimeDuration => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::DayTimeDurationStr(s))
        }
        OTag::Duration => {
            let s = decode_inline_str(data, pos)?;
            Ok(RawObject::DurationStr(s))
        }
    }
}

/// Decode a length-prefixed inline UTF-8 string, returning a borrowed `&str`.
///
/// This is the zero-allocation equivalent of `decode_len_prefixed_str` in op_codec.rs,
/// which returns an owned `String`.
#[inline]
fn decode_inline_str<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a str, CommitV2Error> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| CommitV2Error::InvalidOp(format!("invalid UTF-8: {}", e)))?;
    *pos += len;
    Ok(s)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_v2::envelope::encode_envelope_fields;
    use crate::commit_v2::format;
    use crate::commit_v2::format::{
        CommitV2Footer, CommitV2Header, FOOTER_LEN, HASH_LEN, HEADER_LEN,
    };
    use crate::commit_v2::op_codec::{encode_op, CommitDicts};
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;

    /// Build a minimal commit blob from flakes for testing.
    fn build_test_blob(flakes: &[Flake], t: i64) -> Vec<u8> {
        // Encode ops
        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        // Encode envelope (minimal: just v=0, no previous, no namespace_delta)
        let envelope = CommitV2Envelope {
            t,
            v: 0,
            previous_ref: None,
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            data: None,
            index: None,
            txn_signature: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        // Serialize dicts
        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(),
            dicts.subject.serialize(),
            dicts.predicate.serialize(),
            dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        // Assemble blob: header + envelope + ops + dicts + footer + hash
        let ops_section_len = ops_buf.len() as u32;
        let envelope_len = envelope_bytes.len() as u32;

        // Calculate dict offsets (relative to blob start)
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [format::DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = format::DictLocation {
                offset,
                len: d.len() as u32,
            };
            offset += d.len() as u64;
        }

        let footer = CommitV2Footer {
            dicts: dict_locations,
            ops_section_len,
        };

        let header = CommitV2Header {
            version: format::VERSION,
            flags: 0, // no compression for test simplicity
            t,
            op_count: flakes.len() as u32,
            envelope_len,
            sig_block_len: 0,
        };

        // Assemble
        let total_len = HEADER_LEN
            + envelope_bytes.len()
            + ops_buf.len()
            + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
            + FOOTER_LEN
            + HASH_LEN;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);
        pos += FOOTER_LEN;

        // Write SHA-256 hash
        let hash: [u8; 32] = Sha256::digest(&blob[..pos]).into();
        blob[pos..pos + HASH_LEN].copy_from_slice(&hash);

        blob
    }

    #[test]
    fn test_load_and_iterate_single_op() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "age"),
            FlakeValue::Long(30),
            Sid::new(2, "integer"),
            1,
            true,
            None,
        );

        let blob = build_test_blob(&[flake], 1);
        let ops = load_commit_ops(&blob).unwrap();

        assert_eq!(ops.t, 1);
        assert_eq!(ops.op_count, 1);

        let mut count = 0;
        ops.for_each_op(|raw| {
            assert_eq!(raw.s_ns_code, 101);
            assert_eq!(raw.s_name, "Alice");
            assert_eq!(raw.p_ns_code, 101);
            assert_eq!(raw.p_name, "age");
            assert_eq!(raw.dt_ns_code, 2);
            assert_eq!(raw.dt_name, "integer");
            assert!(matches!(raw.o, RawObject::Long(30)));
            assert!(raw.op); // assert
            assert!(raw.lang.is_none());
            assert!(raw.i.is_none());
            assert_eq!(raw.g_ns_code, 0);
            assert_eq!(raw.g_name, "");
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_all_object_types() {
        let flakes = vec![
            // Long
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "long_val"),
                FlakeValue::Long(42),
                Sid::new(2, "long"),
                1,
                true,
                None,
            ),
            // Double
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "dbl_val"),
                FlakeValue::Double(3.13),
                Sid::new(2, "double"),
                1,
                true,
                None,
            ),
            // String
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "str_val"),
                FlakeValue::String("hello world".into()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            // Boolean
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "bool_val"),
                FlakeValue::Boolean(true),
                Sid::new(2, "boolean"),
                1,
                true,
                None,
            ),
            // Ref
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "ref_val"),
                FlakeValue::Ref(Sid::new(101, "y")),
                Sid::new(1, "id"),
                1,
                true,
                None,
            ),
            // Null
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "null_val"),
                FlakeValue::Null,
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            // DateTime
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "dt_val"),
                FlakeValue::DateTime(Box::new(
                    fluree_db_core::DateTime::parse("2024-01-15T10:30:00Z").unwrap(),
                )),
                Sid::new(2, "dateTime"),
                1,
                true,
                None,
            ),
            // Date
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "date_val"),
                FlakeValue::Date(Box::new(fluree_db_core::Date::parse("2024-01-15").unwrap())),
                Sid::new(2, "date"),
                1,
                true,
                None,
            ),
            // Time
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "time_val"),
                FlakeValue::Time(Box::new(fluree_db_core::Time::parse("10:30:00").unwrap())),
                Sid::new(2, "time"),
                1,
                true,
                None,
            ),
            // Json
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "json_val"),
                FlakeValue::Json(r#"{"key":"val"}"#.into()),
                Sid::new(3, "JSON"),
                1,
                true,
                None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let ops = load_commit_ops(&blob).unwrap();
        assert_eq!(ops.op_count, flakes.len() as u32);

        let mut idx = 0;
        ops.for_each_op(|raw| {
            match idx {
                0 => assert!(matches!(raw.o, RawObject::Long(42))),
                1 => {
                    if let RawObject::Double(d) = raw.o {
                        assert!((d - 3.13).abs() < f64::EPSILON);
                    } else {
                        panic!("expected Double");
                    }
                }
                2 => assert!(matches!(raw.o, RawObject::Str("hello world"))),
                3 => assert!(matches!(raw.o, RawObject::Boolean(true))),
                4 => {
                    if let RawObject::Ref { ns_code, name } = raw.o {
                        assert_eq!(ns_code, 101);
                        assert_eq!(name, "y");
                    } else {
                        panic!("expected Ref");
                    }
                }
                5 => assert!(matches!(raw.o, RawObject::Null)),
                6 => assert!(matches!(raw.o, RawObject::DateTimeStr(_))),
                7 => assert!(matches!(raw.o, RawObject::DateStr(_))),
                8 => assert!(matches!(raw.o, RawObject::TimeStr(_))),
                9 => assert!(matches!(raw.o, RawObject::JsonStr(_))),
                _ => panic!("too many ops"),
            }
            idx += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(idx, 10);
    }

    #[test]
    fn test_lang_and_index_metadata() {
        let flakes = vec![
            // With lang tag
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta::with_lang("en")),
            ),
            // With list index
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "scores"),
                FlakeValue::Long(100),
                Sid::new(2, "integer"),
                1,
                true,
                Some(FlakeMeta::with_index(3)),
            ),
            // With both lang and index
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "labels"),
                FlakeValue::String("hello".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta {
                    lang: Some("fr".into()),
                    i: Some(0),
                }),
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let ops = load_commit_ops(&blob).unwrap();

        let mut idx = 0;
        ops.for_each_op(|raw| {
            match idx {
                0 => {
                    assert_eq!(raw.lang, Some("en"));
                    assert_eq!(raw.i, None);
                }
                1 => {
                    assert_eq!(raw.lang, None);
                    assert_eq!(raw.i, Some(3));
                }
                2 => {
                    assert_eq!(raw.lang, Some("fr"));
                    assert_eq!(raw.i, Some(0));
                }
                _ => panic!("too many ops"),
            }
            idx += 1;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_retract_op() {
        let flake = Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "age"),
            FlakeValue::Long(30),
            Sid::new(2, "integer"),
            1,
            false, // retract
            None,
        );

        let blob = build_test_blob(&[flake], 1);
        let ops = load_commit_ops(&blob).unwrap();

        ops.for_each_op(|raw| {
            assert!(!raw.op); // retract
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_multiple_subjects_shared_dict() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "age"),
                FlakeValue::Long(30),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "Bob"),
                Sid::new(101, "age"),
                FlakeValue::Long(25),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
            // Alice again — same dict entry
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let ops = load_commit_ops(&blob).unwrap();
        assert_eq!(ops.op_count, 3);

        let mut names = Vec::new();
        ops.for_each_op(|raw| {
            names.push(raw.s_name.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(names, vec!["Alice", "Bob", "Alice"]);
    }
}
