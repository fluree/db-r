//! Op encode/decode: Flake <-> binary op with commit-local Sid encoding.
//!
//! Each Sid field is stored as (namespace_code: varint, name_id: varint)
//! where namespace_code is a u16 and name_id is a reference into a per-field
//! string dictionary.
//!
//! Op field order:
//! ```text
//! g_ns_code, g_name_id,
//! s_ns_code, s_name_id,
//! p_ns_code, p_name_id,
//! dt_ns_code, dt_name_id,
//! o_tag, o_payload,
//! flags, [lang], [i]
//! ```

use super::error::CommitV2Error;
use super::format::{OTag, OP_FLAG_ASSERT, OP_FLAG_HAS_I, OP_FLAG_HAS_LANG};
use super::string_dict::{StringDict, StringDictBuilder};
use super::varint::{decode_varint, encode_varint, zigzag_decode, zigzag_encode};
use fluree_db_core::temporal::{
    Date, DateTime, DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, Time,
    YearMonthDuration,
};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};

// =============================================================================
// CommitDicts — dictionary set for writing
// =============================================================================

/// The five commit-local string dictionaries used during encoding.
/// Each stores Sid name parts (not full IRIs).
pub struct CommitDicts {
    pub graph: StringDictBuilder,
    pub subject: StringDictBuilder,
    pub predicate: StringDictBuilder,
    pub datatype: StringDictBuilder,
    pub object_ref: StringDictBuilder,
}

impl CommitDicts {
    pub fn new() -> Self {
        Self {
            graph: StringDictBuilder::new(),
            subject: StringDictBuilder::new(),
            predicate: StringDictBuilder::new(),
            datatype: StringDictBuilder::new(),
            object_ref: StringDictBuilder::new(),
        }
    }
}

/// Read-only dictionary set for decoding.
pub struct ReadDicts {
    pub graph: StringDict,
    pub subject: StringDict,
    pub predicate: StringDict,
    pub datatype: StringDict,
    pub object_ref: StringDict,
}

// =============================================================================
// Encode
// =============================================================================

/// Encode a single flake as a binary op, appending to `buf`.
///
/// Sids are encoded directly as (namespace_code, name dict entry) pairs.
/// No IRI reconstruction or NamespaceRegistry needed.
///
/// Returns an error if the flake contains a value type not supported
/// by the v2 format (e.g. `FlakeValue::Vector`).
pub fn encode_op(flake: &Flake, dicts: &mut CommitDicts, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    // Graph: always (0, 0) = default graph in Phase 1
    encode_varint(0, buf); // g_ns_code
    encode_varint(0, buf); // g_name_id = 0 means default graph

    // Subject
    encode_varint(flake.s.namespace_code as u64, buf);
    let s_name_id = dicts.subject.insert(flake.s.name.as_ref());
    encode_varint(s_name_id as u64, buf);

    // Predicate
    encode_varint(flake.p.namespace_code as u64, buf);
    let p_name_id = dicts.predicate.insert(flake.p.name.as_ref());
    encode_varint(p_name_id as u64, buf);

    // Datatype
    encode_varint(flake.dt.namespace_code as u64, buf);
    let dt_name_id = dicts.datatype.insert(flake.dt.name.as_ref());
    encode_varint(dt_name_id as u64, buf);

    // Object (tag + payload)
    encode_object(&flake.o, dicts, buf)?;

    // Flags
    let mut flags: u8 = 0;
    if flake.op {
        flags |= OP_FLAG_ASSERT;
    }
    let has_lang = flake.m.as_ref().and_then(|m| m.lang.as_ref()).is_some();
    let has_i = flake.m.as_ref().and_then(|m| m.i).is_some();
    if has_lang {
        flags |= OP_FLAG_HAS_LANG;
    }
    if has_i {
        flags |= OP_FLAG_HAS_I;
    }
    buf.push(flags);

    // Optional lang
    if let Some(lang) = flake.m.as_ref().and_then(|m| m.lang.as_ref()) {
        let lang_bytes = lang.as_bytes();
        encode_varint(lang_bytes.len() as u64, buf);
        buf.extend_from_slice(lang_bytes);
    }

    // Optional list index
    if let Some(i) = flake.m.as_ref().and_then(|m| m.i) {
        encode_varint(zigzag_encode(i as i64), buf);
    }

    Ok(())
}

fn encode_object(value: &FlakeValue, dicts: &mut CommitDicts, buf: &mut Vec<u8>) -> Result<(), CommitV2Error> {
    match value {
        FlakeValue::Ref(sid) => {
            buf.push(OTag::Ref as u8);
            encode_varint(sid.namespace_code as u64, buf);
            let name_id = dicts.object_ref.insert(sid.name.as_ref());
            encode_varint(name_id as u64, buf);
        }
        FlakeValue::Long(n) => {
            buf.push(OTag::Long as u8);
            encode_varint(zigzag_encode(*n), buf);
        }
        FlakeValue::Double(d) => {
            buf.push(OTag::Double as u8);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        FlakeValue::String(s) => {
            buf.push(OTag::String as u8);
            encode_len_prefixed_str(s, buf);
        }
        FlakeValue::Boolean(b) => {
            buf.push(OTag::Boolean as u8);
            buf.push(if *b { 1 } else { 0 });
        }
        FlakeValue::DateTime(dt) => {
            buf.push(OTag::DateTime as u8);
            encode_len_prefixed_str(&dt.to_string(), buf);
        }
        FlakeValue::Date(d) => {
            buf.push(OTag::Date as u8);
            encode_len_prefixed_str(&d.to_string(), buf);
        }
        FlakeValue::Time(t) => {
            buf.push(OTag::Time as u8);
            encode_len_prefixed_str(&t.to_string(), buf);
        }
        FlakeValue::BigInt(n) => {
            buf.push(OTag::BigInt as u8);
            encode_len_prefixed_str(&n.to_string(), buf);
        }
        FlakeValue::Decimal(d) => {
            buf.push(OTag::Decimal as u8);
            encode_len_prefixed_str(&d.to_string(), buf);
        }
        FlakeValue::Json(s) => {
            buf.push(OTag::Json as u8);
            encode_len_prefixed_str(s, buf);
        }
        FlakeValue::Null => {
            buf.push(OTag::Null as u8);
        }
        FlakeValue::GYear(v) => {
            buf.push(OTag::GYear as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::GYearMonth(v) => {
            buf.push(OTag::GYearMonth as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::GMonth(v) => {
            buf.push(OTag::GMonth as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::GDay(v) => {
            buf.push(OTag::GDay as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::GMonthDay(v) => {
            buf.push(OTag::GMonthDay as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::YearMonthDuration(v) => {
            buf.push(OTag::YearMonthDuration as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::DayTimeDuration(v) => {
            buf.push(OTag::DayTimeDuration as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::Duration(v) => {
            buf.push(OTag::Duration as u8);
            encode_len_prefixed_str(&v.to_string(), buf);
        }
        FlakeValue::Vector(_) => {
            return Err(CommitV2Error::UnsupportedValue(
                "Vector is not supported in commit-v2 format".into(),
            ));
        }
    }
    Ok(())
}

fn encode_len_prefixed_str(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

// =============================================================================
// Decode
// =============================================================================

/// Decode a varint as a u16 namespace code, returning an error if the value
/// exceeds `u16::MAX`.
fn decode_ns_code(data: &[u8], pos: &mut usize) -> Result<u16, CommitV2Error> {
    let raw = decode_varint(data, pos)?;
    u16::try_from(raw).map_err(|_| {
        CommitV2Error::InvalidOp(format!("namespace code {} exceeds u16::MAX", raw))
    })
}

/// Decode a single op from `data` starting at `*pos`, returning a `Flake`.
///
/// Sids are reconstructed directly from (namespace_code, name) pairs
/// without needing a NamespaceRegistry.
pub fn decode_op(
    data: &[u8],
    pos: &mut usize,
    dicts: &ReadDicts,
    t: i64,
) -> Result<Flake, CommitV2Error> {
    // Graph: validate it's the default graph (Phase 1 only supports default)
    let g_ns_code = decode_ns_code(data, pos)?;
    let g_name_id = decode_varint(data, pos)? as u32;
    if g_ns_code != 0 || g_name_id != 0 {
        return Err(CommitV2Error::NonDefaultGraph { ns_code: g_ns_code, name_id: g_name_id });
    }

    // Subject
    let s_ns_code = decode_ns_code(data, pos)?;
    let s_name_id = decode_varint(data, pos)? as u32;
    let s_name = dicts.subject.get(s_name_id)?;
    let s = Sid::new(s_ns_code, s_name);

    // Predicate
    let p_ns_code = decode_ns_code(data, pos)?;
    let p_name_id = decode_varint(data, pos)? as u32;
    let p_name = dicts.predicate.get(p_name_id)?;
    let p = Sid::new(p_ns_code, p_name);

    // Datatype
    let dt_ns_code = decode_ns_code(data, pos)?;
    let dt_name_id = decode_varint(data, pos)? as u32;
    let dt_name = dicts.datatype.get(dt_name_id)?;
    let dt = Sid::new(dt_ns_code, dt_name);

    // Object (tag + payload)
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let o_tag = OTag::from_u8(data[*pos])?;
    *pos += 1;
    let o = decode_object(o_tag, data, pos, dicts)?;

    // Flags
    if *pos >= data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let flags = data[*pos];
    *pos += 1;
    let op = flags & OP_FLAG_ASSERT != 0;

    // Optional lang
    let lang = if flags & OP_FLAG_HAS_LANG != 0 {
        Some(decode_len_prefixed_str(data, pos)?)
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

    let meta = match (&lang, i) {
        (Some(l), Some(idx)) => Some(FlakeMeta {
            lang: Some(l.clone()),
            i: Some(idx),
        }),
        (Some(l), None) => Some(FlakeMeta::with_lang(l)),
        (None, Some(idx)) => Some(FlakeMeta::with_index(idx)),
        (None, None) => None,
    };

    Ok(Flake::new(s, p, o, dt, t, op, meta))
}

fn decode_object(
    tag: OTag,
    data: &[u8],
    pos: &mut usize,
    dicts: &ReadDicts,
) -> Result<FlakeValue, CommitV2Error> {
    match tag {
        OTag::Ref => {
            let ns_code = decode_ns_code(data, pos)?;
            let name_id = decode_varint(data, pos)? as u32;
            let name = dicts.object_ref.get(name_id)?;
            Ok(FlakeValue::Ref(Sid::new(ns_code, name)))
        }
        OTag::Long => {
            let raw = decode_varint(data, pos)?;
            Ok(FlakeValue::Long(zigzag_decode(raw)))
        }
        OTag::Double => {
            if *pos + 8 > data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
            *pos += 8;
            Ok(FlakeValue::Double(f64::from_le_bytes(bytes)))
        }
        OTag::String => {
            let s = decode_len_prefixed_str(data, pos)?;
            Ok(FlakeValue::String(s))
        }
        OTag::Boolean => {
            if *pos >= data.len() {
                return Err(CommitV2Error::UnexpectedEof);
            }
            let b = data[*pos] != 0;
            *pos += 1;
            Ok(FlakeValue::Boolean(b))
        }
        OTag::DateTime => {
            let s = decode_len_prefixed_str(data, pos)?;
            DateTime::parse(&s)
                .map(|dt| FlakeValue::DateTime(Box::new(dt)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad datetime: {}", e)))
        }
        OTag::Date => {
            let s = decode_len_prefixed_str(data, pos)?;
            Date::parse(&s)
                .map(|d| FlakeValue::Date(Box::new(d)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad date: {}", e)))
        }
        OTag::Time => {
            let s = decode_len_prefixed_str(data, pos)?;
            Time::parse(&s)
                .map(|t| FlakeValue::Time(Box::new(t)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad time: {}", e)))
        }
        OTag::BigInt => {
            let s = decode_len_prefixed_str(data, pos)?;
            s.parse::<num_bigint::BigInt>()
                .map(|n| FlakeValue::BigInt(Box::new(n)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad bigint: {}", e)))
        }
        OTag::Decimal => {
            let s = decode_len_prefixed_str(data, pos)?;
            s.parse::<bigdecimal::BigDecimal>()
                .map(|d| FlakeValue::Decimal(Box::new(d)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad decimal: {}", e)))
        }
        OTag::Json => {
            let s = decode_len_prefixed_str(data, pos)?;
            Ok(FlakeValue::Json(s))
        }
        OTag::Null => Ok(FlakeValue::Null),
        OTag::GYear => {
            let s = decode_len_prefixed_str(data, pos)?;
            GYear::parse(&s)
                .map(|v| FlakeValue::GYear(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad gYear: {}", e)))
        }
        OTag::GYearMonth => {
            let s = decode_len_prefixed_str(data, pos)?;
            GYearMonth::parse(&s)
                .map(|v| FlakeValue::GYearMonth(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad gYearMonth: {}", e)))
        }
        OTag::GMonth => {
            let s = decode_len_prefixed_str(data, pos)?;
            GMonth::parse(&s)
                .map(|v| FlakeValue::GMonth(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad gMonth: {}", e)))
        }
        OTag::GDay => {
            let s = decode_len_prefixed_str(data, pos)?;
            GDay::parse(&s)
                .map(|v| FlakeValue::GDay(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad gDay: {}", e)))
        }
        OTag::GMonthDay => {
            let s = decode_len_prefixed_str(data, pos)?;
            GMonthDay::parse(&s)
                .map(|v| FlakeValue::GMonthDay(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad gMonthDay: {}", e)))
        }
        OTag::YearMonthDuration => {
            let s = decode_len_prefixed_str(data, pos)?;
            YearMonthDuration::parse(&s)
                .map(|v| FlakeValue::YearMonthDuration(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad yearMonthDuration: {}", e)))
        }
        OTag::DayTimeDuration => {
            let s = decode_len_prefixed_str(data, pos)?;
            DayTimeDuration::parse(&s)
                .map(|v| FlakeValue::DayTimeDuration(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad dayTimeDuration: {}", e)))
        }
        OTag::Duration => {
            let s = decode_len_prefixed_str(data, pos)?;
            Duration::parse(&s)
                .map(|v| FlakeValue::Duration(Box::new(v)))
                .map_err(|e| CommitV2Error::InvalidOp(format!("bad duration: {}", e)))
        }
    }
}

fn decode_len_prefixed_str(data: &[u8], pos: &mut usize) -> Result<String, CommitV2Error> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(CommitV2Error::UnexpectedEof);
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| CommitV2Error::InvalidOp(format!("invalid UTF-8: {}", e)))?;
    *pos += len;
    Ok(s.to_string())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_flake_long(s_code: u16, s_name: &str, p_code: u16, p_name: &str, val: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "integer"),
            t,
            true,
            None,
        )
    }

    fn round_trip_dicts(dicts: &CommitDicts) -> ReadDicts {
        ReadDicts {
            graph: StringDict::deserialize(&dicts.graph.serialize()).unwrap(),
            subject: StringDict::deserialize(&dicts.subject.serialize()).unwrap(),
            predicate: StringDict::deserialize(&dicts.predicate.serialize()).unwrap(),
            datatype: StringDict::deserialize(&dicts.datatype.serialize()).unwrap(),
            object_ref: StringDict::deserialize(&dicts.object_ref.serialize()).unwrap(),
        }
    }

    #[test]
    fn test_round_trip_long() {
        let flake = make_flake_long(101, "Alice", 101, "age", 30, 1);
        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert_eq!(pos, buf.len());

        assert_eq!(decoded.s.namespace_code, 101);
        assert_eq!(decoded.s.name.as_ref(), "Alice");
        assert_eq!(decoded.p.namespace_code, 101);
        assert_eq!(decoded.p.name.as_ref(), "age");
        assert_eq!(decoded.dt.namespace_code, 2);
        assert_eq!(decoded.dt.name.as_ref(), "integer");
        assert!(decoded.op); // assert
        assert!(decoded.m.is_none());
        assert!(matches!(decoded.o, FlakeValue::Long(30)));
    }

    #[test]
    fn test_round_trip_with_lang() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "name"),
            FlakeValue::String("Alice".to_string()),
            Sid::new(3, "langString"),
            1,
            true,
            Some(FlakeMeta::with_lang("en")),
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        assert!(decoded.op);
        assert!(matches!(&decoded.o, FlakeValue::String(s) if s == "Alice"));
        let meta = decoded.m.unwrap();
        assert_eq!(meta.lang.as_deref(), Some("en"));
        assert_eq!(meta.i, None);
    }

    #[test]
    fn test_round_trip_with_list_index() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "scores"),
            FlakeValue::Long(42),
            Sid::new(2, "integer"),
            1,
            true,
            Some(FlakeMeta::with_index(3)),
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        let meta = decoded.m.unwrap();
        assert_eq!(meta.i, Some(3));
        assert_eq!(meta.lang, None);
    }

    #[test]
    fn test_round_trip_retract() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "age"),
            FlakeValue::Long(30),
            Sid::new(2, "integer"),
            1,
            false, // retract
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert!(!decoded.op); // retract
    }

    #[test]
    fn test_round_trip_ref() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "knows"),
            FlakeValue::Ref(Sid::new(101, "Bob")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        // Ref objects are decoded directly as FlakeValue::Ref(Sid)
        match &decoded.o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 101);
                assert_eq!(sid.name.as_ref(), "Bob");
            }
            other => panic!("expected Ref, got {:?}", other),
        }
    }

    #[test]
    fn test_round_trip_double() {
        let flake = Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "val"),
            FlakeValue::Double(3.14159),
            Sid::new(2, "double"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        match decoded.o {
            FlakeValue::Double(d) => assert!((d - 3.14159).abs() < f64::EPSILON),
            _ => panic!("expected Double"),
        }
    }

    #[test]
    fn test_round_trip_boolean() {
        for val in [true, false] {
            let flake = Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "active"),
                FlakeValue::Boolean(val),
                Sid::new(2, "boolean"),
                1,
                true,
                None,
            );

            let mut dicts = CommitDicts::new();
            let mut buf = Vec::new();
            encode_op(&flake, &mut dicts, &mut buf).unwrap();

            let read_dicts = round_trip_dicts(&dicts);
            let mut pos = 0;
            let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
            assert_eq!(decoded.o, FlakeValue::Boolean(val));
        }
    }

    #[test]
    fn test_round_trip_multiple_ops() {
        let flakes = vec![
            make_flake_long(101, "Alice", 101, "age", 30, 1),
            make_flake_long(101, "Bob", 101, "age", 25, 1),
            make_flake_long(101, "Alice", 101, "score", 100, 1),
        ];

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        for f in &flakes {
            encode_op(f, &mut dicts, &mut buf).unwrap();
        }

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        for original in &flakes {
            let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
            assert_eq!(decoded.s.namespace_code, original.s.namespace_code);
            assert_eq!(decoded.s.name.as_ref(), original.s.name.as_ref());
            assert_eq!(decoded.p.name.as_ref(), original.p.name.as_ref());
        }
        assert_eq!(pos, buf.len());
    }
}
