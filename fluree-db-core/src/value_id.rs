//! Compact value encoding for index records.
//!
//! [`ValueId`] is a 64-bit tagged union that encodes object values for sorted
//! run files and index segments. The high 4 bits select a type tag; the low
//! 60 bits carry the payload.
//!
//! **Ordering semantics:** The natural `u64` ordering is correct *within* a
//! single tag (e.g., all NUM_INT values sort numerically via offset-binary).
//! Cross-tag ordering groups values by type, **not** by numeric equivalence.
//! `NUM_INT(3)` and `LEX_ID(some_double_dict_id)` do NOT interleave correctly
//! as raw `u64`. Numeric-class comparison (Long < Double across types) is a
//! query-layer concern implemented via multi-scan merge, not an index property.
//!
//! [`DatatypeId`] is a compact `u8` identifier for XSD/RDF datatypes, used as
//! a tie-breaker in index sort keys so that values with the same `ValueId`
//! payload but different types (e.g., `xsd:integer 3` vs `xsd:double 3.0`)
//! remain distinguishable.

use crate::Sid;
use fluree_vocab::{jsonld_names, namespaces, rdf_names, xsd_names};
use std::sync::OnceLock;

// ============================================================================
// ValueId
// ============================================================================

/// 64-bit tagged value for index records.
///
/// Layout: `[tag: 4 bits][payload: 60 bits]`
///
/// Tags are chosen so that within-tag `u64` comparison gives the correct
/// domain ordering (e.g., NUM_INT uses offset-binary encoding).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct ValueId(u64);

// Tag constants (high nibble)
const TAG_MIN: u64 = 0x0;
const TAG_NULL: u64 = 0x1;
const TAG_BOOL: u64 = 0x2;
const TAG_NUM_INT: u64 = 0x3;
const TAG_NUM_FLOAT: u64 = 0x4;
const TAG_IRI_ID: u64 = 0x5;
const TAG_LEX_ID: u64 = 0x6;
const TAG_DATE: u64 = 0x7;
const TAG_TIME: u64 = 0x8;
const TAG_DATETIME: u64 = 0x9;
#[allow(dead_code)] // Reserved for future vector store support
const TAG_VECTOR: u64 = 0xA;
const TAG_JSON: u64 = 0xB;
const TAG_MAX: u64 = 0xF;

const TAG_SHIFT: u32 = 60;
const PAYLOAD_MASK: u64 = (1u64 << TAG_SHIFT) - 1;

/// Offset for i60 offset-binary encoding: 2^59
const I60_OFFSET: u64 = 1u64 << 59;

/// Maximum representable positive i60 value: 2^59 - 1
const I60_MAX: i64 = (1i64 << 59) - 1;

/// Minimum representable negative i60 value: -2^59
const I60_MIN: i64 = -(1i64 << 59);

impl ValueId {
    // ---- Sentinel values ----

    /// Minimum possible ValueId (sorts before everything).
    pub const MIN: Self = Self(TAG_MIN << TAG_SHIFT);

    /// Null value.
    pub const NULL: Self = Self(TAG_NULL << TAG_SHIFT);

    /// Boolean false.
    pub const BOOL_FALSE: Self = Self((TAG_BOOL << TAG_SHIFT) | 0);

    /// Boolean true.
    pub const BOOL_TRUE: Self = Self((TAG_BOOL << TAG_SHIFT) | 1);

    /// Maximum possible ValueId (sorts after everything).
    pub const MAX: Self = Self((TAG_MAX << TAG_SHIFT) | PAYLOAD_MASK);

    // ---- Constructors ----

    /// Encode a signed integer as offset-binary in the NUM_INT tag.
    ///
    /// Returns `None` if the value exceeds the i60 range [-2^59, 2^59-1].
    #[inline]
    pub fn num_int(value: i64) -> Option<Self> {
        if value < I60_MIN || value > I60_MAX {
            return None;
        }
        let payload = (value as u64).wrapping_add(I60_OFFSET) & PAYLOAD_MASK;
        Some(Self((TAG_NUM_INT << TAG_SHIFT) | payload))
    }

    /// Encode a NUM_FLOAT tag with the given payload (dictionary rank or
    /// future midpoint-splitting rank). Phase 4 does not use this tag
    /// directly — non-integer floats go to LEX_ID instead.
    #[inline]
    pub fn num_float(rank: u32) -> Self {
        Self((TAG_NUM_FLOAT << TAG_SHIFT) | (rank as u64))
    }

    /// Encode an IRI reference as IRI_ID with a global subject dictionary id.
    #[inline]
    pub fn iri_id(id: u32) -> Self {
        Self((TAG_IRI_ID << TAG_SHIFT) | (id as u64))
    }

    /// Encode a lexicographic string dictionary id.
    #[inline]
    pub fn lex_id(id: u32) -> Self {
        Self((TAG_LEX_ID << TAG_SHIFT) | (id as u64))
    }

    /// Encode an xsd:date as days since Unix epoch (offset-binary i32 in 60 bits).
    #[inline]
    pub fn date(days_since_epoch: i32) -> Self {
        let payload = (days_since_epoch as i64 as u64).wrapping_add(I60_OFFSET) & PAYLOAD_MASK;
        Self((TAG_DATE << TAG_SHIFT) | payload)
    }

    /// Encode an xsd:time as microseconds since midnight (unsigned, always >= 0).
    #[inline]
    pub fn time(micros_since_midnight: i64) -> Self {
        debug_assert!(micros_since_midnight >= 0);
        debug_assert!(
            (micros_since_midnight as u64) <= PAYLOAD_MASK,
            "time micros {} exceeds 60-bit payload",
            micros_since_midnight,
        );
        let payload = (micros_since_midnight as u64) & PAYLOAD_MASK;
        Self((TAG_TIME << TAG_SHIFT) | payload)
    }

    /// Encode an xsd:dateTime as epoch microseconds (offset-binary i60).
    ///
    /// Returns `None` if the value exceeds the i60 range.
    #[inline]
    pub fn datetime(epoch_micros: i64) -> Option<Self> {
        if epoch_micros < I60_MIN || epoch_micros > I60_MAX {
            return None;
        }
        let payload = (epoch_micros as u64).wrapping_add(I60_OFFSET) & PAYLOAD_MASK;
        Some(Self((TAG_DATETIME << TAG_SHIFT) | payload))
    }

    /// Encode a JSON value as a global string dictionary id.
    #[inline]
    pub fn json(id: u32) -> Self {
        Self((TAG_JSON << TAG_SHIFT) | (id as u64))
    }

    // ---- Accessors ----

    /// Get the raw u64 representation.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct from raw u64.
    #[inline]
    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the tag nibble (0..=15).
    #[inline]
    pub fn tag(self) -> u8 {
        (self.0 >> TAG_SHIFT) as u8
    }

    /// Get the 60-bit payload.
    #[inline]
    pub fn payload(self) -> u64 {
        self.0 & PAYLOAD_MASK
    }

    /// Decode a NUM_INT payload back to i64.
    ///
    /// Only valid when `self.tag() == TAG_NUM_INT` or `TAG_DATETIME` or `TAG_DATE`.
    #[inline]
    pub fn decode_offset_binary(self) -> i64 {
        let payload = self.payload();
        // Undo offset-binary: subtract 2^59, sign-extend from 60 bits
        let raw = payload.wrapping_sub(I60_OFFSET);
        // Sign-extend: if bit 59 is set, the value is negative in the i60 domain
        ((raw as i64) << 4) >> 4
    }

    /// Check if this is a NULL value.
    #[inline]
    pub fn is_null(self) -> bool {
        self == Self::NULL
    }

    /// Check if this is a MIN sentinel.
    #[inline]
    pub fn is_min(self) -> bool {
        self == Self::MIN
    }

    /// Check if this is a MAX sentinel.
    #[inline]
    pub fn is_max(self) -> bool {
        self == Self::MAX
    }
}

impl std::fmt::Debug for ValueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tag = self.tag();
        let payload = self.payload();
        match tag {
            0x0 => write!(f, "ValueId::MIN"),
            0x1 => write!(f, "ValueId::NULL"),
            0x2 => {
                if payload == 0 {
                    write!(f, "ValueId::BOOL(false)")
                } else {
                    write!(f, "ValueId::BOOL(true)")
                }
            }
            0x3 => write!(f, "ValueId::NUM_INT({})", self.decode_offset_binary()),
            0x4 => write!(f, "ValueId::NUM_FLOAT(rank={})", payload),
            0x5 => write!(f, "ValueId::IRI_ID({})", payload),
            0x6 => write!(f, "ValueId::LEX_ID({})", payload),
            0x7 => write!(f, "ValueId::DATE(days={})", self.decode_offset_binary()),
            0x8 => write!(f, "ValueId::TIME(micros={})", payload),
            0x9 => write!(f, "ValueId::DATETIME(micros={})", self.decode_offset_binary()),
            0xA => write!(f, "ValueId::VECTOR({})", payload),
            0xB => write!(f, "ValueId::JSON({})", payload),
            0xF => write!(f, "ValueId::MAX"),
            _ => write!(f, "ValueId(tag={:#x}, payload={})", tag, payload),
        }
    }
}

impl std::fmt::Display for ValueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

// ============================================================================
// DatatypeId
// ============================================================================

/// Compact datatype identifier for index sort-key tie-breaking.
///
/// Maps (namespace_code, local_name) pairs to fixed u8 values.
/// The ordering of DatatypeId values is arbitrary but stable — it serves
/// only to distinguish types that share the same `ValueId` payload.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct DatatypeId(u8);

impl DatatypeId {
    // Fixed mapping constants
    pub const STRING: Self = Self(0);
    pub const BOOLEAN: Self = Self(1);
    pub const INTEGER: Self = Self(2);
    pub const LONG: Self = Self(3);
    pub const INT: Self = Self(4);
    pub const SHORT: Self = Self(5);
    pub const BYTE: Self = Self(6);
    pub const DOUBLE: Self = Self(7);
    pub const FLOAT: Self = Self(8);
    pub const DECIMAL: Self = Self(9);
    pub const DATE_TIME: Self = Self(10);
    pub const DATE: Self = Self(11);
    pub const TIME: Self = Self(12);
    pub const ANY_URI: Self = Self(13);
    pub const LANG_STRING: Self = Self(14);
    pub const JSON_LD_ID: Self = Self(16);
    pub const UNSIGNED_LONG: Self = Self(17);
    pub const UNSIGNED_INT: Self = Self(18);
    pub const UNSIGNED_SHORT: Self = Self(19);
    pub const UNSIGNED_BYTE: Self = Self(20);
    pub const NON_NEGATIVE_INTEGER: Self = Self(21);
    pub const POSITIVE_INTEGER: Self = Self(22);
    pub const NON_POSITIVE_INTEGER: Self = Self(23);
    pub const NEGATIVE_INTEGER: Self = Self(24);
    pub const NORMALIZED_STRING: Self = Self(25);
    pub const TOKEN: Self = Self(26);
    pub const LANGUAGE: Self = Self(27);
    pub const DURATION: Self = Self(28);
    pub const DAY_TIME_DURATION: Self = Self(29);
    pub const YEAR_MONTH_DURATION: Self = Self(30);
    pub const BASE64_BINARY: Self = Self(31);
    pub const HEX_BINARY: Self = Self(32);
    pub const RDF_JSON: Self = Self(33);
    pub const G_YEAR: Self = Self(15);
    pub const G_MONTH: Self = Self(34);
    pub const G_DAY: Self = Self(35);
    pub const G_YEAR_MONTH: Self = Self(36);
    pub const G_MONTH_DAY: Self = Self(37);
    pub const UNKNOWN: Self = Self(255);

    /// Resolve a (namespace_code, local_name) pair to a DatatypeId.
    ///
    /// This is the primary entry point for the resolver. Matches against
    /// well-known `fluree_vocab` namespace codes and local names.
    #[inline]
    pub fn from_ns_name(ns_code: i32, name: &str) -> Self {
        match ns_code {
            namespaces::XSD => Self::from_xsd_name(name),
            namespaces::RDF => Self::from_rdf_name(name),
            namespaces::JSON_LD => Self::from_jsonld_name(name),
            _ => Self::UNKNOWN,
        }
    }

    /// Resolve an XSD local name to DatatypeId.
    fn from_xsd_name(name: &str) -> Self {
        match name {
            xsd_names::STRING => Self::STRING,
            xsd_names::BOOLEAN => Self::BOOLEAN,
            xsd_names::INTEGER => Self::INTEGER,
            xsd_names::LONG => Self::LONG,
            xsd_names::INT => Self::INT,
            xsd_names::SHORT => Self::SHORT,
            xsd_names::BYTE => Self::BYTE,
            xsd_names::DOUBLE => Self::DOUBLE,
            xsd_names::FLOAT => Self::FLOAT,
            xsd_names::DECIMAL => Self::DECIMAL,
            xsd_names::DATE_TIME => Self::DATE_TIME,
            xsd_names::DATE => Self::DATE,
            xsd_names::TIME => Self::TIME,
            xsd_names::ANY_URI => Self::ANY_URI,
            xsd_names::UNSIGNED_LONG => Self::UNSIGNED_LONG,
            xsd_names::UNSIGNED_INT => Self::UNSIGNED_INT,
            xsd_names::UNSIGNED_SHORT => Self::UNSIGNED_SHORT,
            xsd_names::UNSIGNED_BYTE => Self::UNSIGNED_BYTE,
            xsd_names::NON_NEGATIVE_INTEGER => Self::NON_NEGATIVE_INTEGER,
            xsd_names::POSITIVE_INTEGER => Self::POSITIVE_INTEGER,
            xsd_names::NON_POSITIVE_INTEGER => Self::NON_POSITIVE_INTEGER,
            xsd_names::NEGATIVE_INTEGER => Self::NEGATIVE_INTEGER,
            xsd_names::NORMALIZED_STRING => Self::NORMALIZED_STRING,
            xsd_names::TOKEN => Self::TOKEN,
            xsd_names::LANGUAGE => Self::LANGUAGE,
            xsd_names::DURATION => Self::DURATION,
            xsd_names::DAY_TIME_DURATION => Self::DAY_TIME_DURATION,
            xsd_names::YEAR_MONTH_DURATION => Self::YEAR_MONTH_DURATION,
            xsd_names::BASE64_BINARY => Self::BASE64_BINARY,
            xsd_names::HEX_BINARY => Self::HEX_BINARY,
            xsd_names::G_YEAR => Self::G_YEAR,
            xsd_names::G_MONTH => Self::G_MONTH,
            xsd_names::G_DAY => Self::G_DAY,
            xsd_names::G_YEAR_MONTH => Self::G_YEAR_MONTH,
            xsd_names::G_MONTH_DAY => Self::G_MONTH_DAY,
            _ => Self::UNKNOWN,
        }
    }

    /// Resolve an RDF local name to DatatypeId.
    fn from_rdf_name(name: &str) -> Self {
        match name {
            rdf_names::LANG_STRING => Self::LANG_STRING,
            rdf_names::JSON => Self::RDF_JSON,
            _ => Self::UNKNOWN,
        }
    }

    /// Resolve a JSON-LD local name to DatatypeId.
    fn from_jsonld_name(name: &str) -> Self {
        match name {
            jsonld_names::ID => Self::JSON_LD_ID,
            _ => Self::UNKNOWN,
        }
    }

    /// Get the raw u8 value.
    #[inline]
    pub fn as_u8(self) -> u8 {
        self.0
    }

    /// Construct from raw u8.
    #[inline]
    pub fn from_u8(raw: u8) -> Self {
        Self(raw)
    }

    /// Check if this is an integer-like type (all XSD integer subtypes).
    #[inline]
    pub fn is_integer_type(self) -> bool {
        matches!(
            self.0,
            2 | 3 | 4 | 5 | 6 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24
        )
    }

    /// Check if this is a floating-point type (double or float).
    #[inline]
    pub fn is_float_type(self) -> bool {
        self == Self::DOUBLE || self == Self::FLOAT
    }

    /// Convert to the corresponding `Sid` for result formatting.
    ///
    /// Uses a process-wide `OnceLock` cache — zero allocation after first access.
    /// Returns `None` for `UNKNOWN` (255).
    pub fn to_sid(self) -> Option<&'static Sid> {
        static TABLE: OnceLock<Vec<Option<Sid>>> = OnceLock::new();

        let table = TABLE.get_or_init(|| {
            let mut t: Vec<Option<Sid>> = vec![None; 38];
            t[0] = Some(Sid::new(namespaces::XSD, xsd_names::STRING));
            t[1] = Some(Sid::new(namespaces::XSD, xsd_names::BOOLEAN));
            t[2] = Some(Sid::new(namespaces::XSD, xsd_names::INTEGER));
            t[3] = Some(Sid::new(namespaces::XSD, xsd_names::LONG));
            t[4] = Some(Sid::new(namespaces::XSD, xsd_names::INT));
            t[5] = Some(Sid::new(namespaces::XSD, xsd_names::SHORT));
            t[6] = Some(Sid::new(namespaces::XSD, xsd_names::BYTE));
            t[7] = Some(Sid::new(namespaces::XSD, xsd_names::DOUBLE));
            t[8] = Some(Sid::new(namespaces::XSD, xsd_names::FLOAT));
            t[9] = Some(Sid::new(namespaces::XSD, xsd_names::DECIMAL));
            t[10] = Some(Sid::new(namespaces::XSD, xsd_names::DATE_TIME));
            t[11] = Some(Sid::new(namespaces::XSD, xsd_names::DATE));
            t[12] = Some(Sid::new(namespaces::XSD, xsd_names::TIME));
            t[13] = Some(Sid::new(namespaces::XSD, xsd_names::ANY_URI));
            t[14] = Some(Sid::new(namespaces::RDF, rdf_names::LANG_STRING));
            t[15] = Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR));
            t[16] = Some(Sid::new(namespaces::JSON_LD, jsonld_names::ID));
            t[17] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_LONG));
            t[18] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_INT));
            t[19] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_SHORT));
            t[20] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_BYTE));
            t[21] = Some(Sid::new(namespaces::XSD, xsd_names::NON_NEGATIVE_INTEGER));
            t[22] = Some(Sid::new(namespaces::XSD, xsd_names::POSITIVE_INTEGER));
            t[23] = Some(Sid::new(namespaces::XSD, xsd_names::NON_POSITIVE_INTEGER));
            t[24] = Some(Sid::new(namespaces::XSD, xsd_names::NEGATIVE_INTEGER));
            t[25] = Some(Sid::new(namespaces::XSD, xsd_names::NORMALIZED_STRING));
            t[26] = Some(Sid::new(namespaces::XSD, xsd_names::TOKEN));
            t[27] = Some(Sid::new(namespaces::XSD, xsd_names::LANGUAGE));
            t[28] = Some(Sid::new(namespaces::XSD, xsd_names::DURATION));
            t[29] = Some(Sid::new(namespaces::XSD, xsd_names::DAY_TIME_DURATION));
            t[30] = Some(Sid::new(namespaces::XSD, xsd_names::YEAR_MONTH_DURATION));
            t[31] = Some(Sid::new(namespaces::XSD, xsd_names::BASE64_BINARY));
            t[32] = Some(Sid::new(namespaces::XSD, xsd_names::HEX_BINARY));
            t[33] = Some(Sid::new(namespaces::RDF, rdf_names::JSON));
            t[34] = Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH));
            t[35] = Some(Sid::new(namespaces::XSD, xsd_names::G_DAY));
            t[36] = Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR_MONTH));
            t[37] = Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH_DAY));
            t
        });

        let idx = self.0 as usize;
        if idx < table.len() {
            table[idx].as_ref()
        } else {
            None
        }
    }
}

impl std::fmt::Display for DatatypeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self.0 {
            0 => "xsd:string",
            1 => "xsd:boolean",
            2 => "xsd:integer",
            3 => "xsd:long",
            4 => "xsd:int",
            5 => "xsd:short",
            6 => "xsd:byte",
            7 => "xsd:double",
            8 => "xsd:float",
            9 => "xsd:decimal",
            10 => "xsd:dateTime",
            11 => "xsd:date",
            12 => "xsd:time",
            13 => "xsd:anyURI",
            14 => "rdf:langString",
            15 => "xsd:gYear",
            16 => "jsonld:id",
            17 => "xsd:unsignedLong",
            18 => "xsd:unsignedInt",
            19 => "xsd:unsignedShort",
            20 => "xsd:unsignedByte",
            21 => "xsd:nonNegativeInteger",
            22 => "xsd:positiveInteger",
            23 => "xsd:nonPositiveInteger",
            24 => "xsd:negativeInteger",
            25 => "xsd:normalizedString",
            26 => "xsd:token",
            27 => "xsd:language",
            28 => "xsd:duration",
            29 => "xsd:dayTimeDuration",
            30 => "xsd:yearMonthDuration",
            31 => "xsd:base64Binary",
            32 => "xsd:hexBinary",
            33 => "rdf:JSON",
            34 => "xsd:gMonth",
            35 => "xsd:gDay",
            36 => "xsd:gYearMonth",
            37 => "xsd:gMonthDay",
            255 => "UNKNOWN",
            n => return write!(f, "DatatypeId({})", n),
        };
        write!(f, "{}", name)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ValueId round-trip tests ----

    #[test]
    fn test_num_int_round_trip() {
        for &v in &[0i64, 1, -1, 42, -42, 1000, -1000, i32::MAX as i64, i32::MIN as i64] {
            let vid = ValueId::num_int(v).unwrap();
            assert_eq!(vid.tag(), 0x3, "tag should be NUM_INT");
            assert_eq!(vid.decode_offset_binary(), v, "round-trip failed for {}", v);
        }
    }

    #[test]
    fn test_num_int_i60_boundaries() {
        let max_i60 = I60_MAX;
        let min_i60 = I60_MIN;

        let vid_max = ValueId::num_int(max_i60).unwrap();
        assert_eq!(vid_max.decode_offset_binary(), max_i60);

        let vid_min = ValueId::num_int(min_i60).unwrap();
        assert_eq!(vid_min.decode_offset_binary(), min_i60);

        // Just outside range should return None
        assert!(ValueId::num_int(max_i60 + 1).is_none());
        assert!(ValueId::num_int(min_i60 - 1).is_none());
    }

    #[test]
    fn test_num_int_ordering() {
        let neg2 = ValueId::num_int(-2).unwrap();
        let neg1 = ValueId::num_int(-1).unwrap();
        let zero = ValueId::num_int(0).unwrap();
        let pos1 = ValueId::num_int(1).unwrap();
        let pos2 = ValueId::num_int(2).unwrap();

        assert!(neg2 < neg1);
        assert!(neg1 < zero);
        assert!(zero < pos1);
        assert!(pos1 < pos2);

        // Large range
        let big_neg = ValueId::num_int(-1_000_000).unwrap();
        let big_pos = ValueId::num_int(1_000_000).unwrap();
        assert!(big_neg < zero);
        assert!(zero < big_pos);
    }

    #[test]
    fn test_bool_encoding() {
        assert!(ValueId::BOOL_FALSE < ValueId::BOOL_TRUE);
        assert_eq!(ValueId::BOOL_FALSE.tag(), 0x2);
        assert_eq!(ValueId::BOOL_TRUE.tag(), 0x2);
        assert_eq!(ValueId::BOOL_FALSE.payload(), 0);
        assert_eq!(ValueId::BOOL_TRUE.payload(), 1);
    }

    #[test]
    fn test_null_encoding() {
        assert_eq!(ValueId::NULL.tag(), 0x1);
        assert!(ValueId::NULL.is_null());
        assert!(!ValueId::BOOL_FALSE.is_null());
    }

    #[test]
    fn test_iri_id_encoding() {
        let vid = ValueId::iri_id(42);
        assert_eq!(vid.tag(), 0x5);
        assert_eq!(vid.payload(), 42);

        let vid0 = ValueId::iri_id(0);
        let vid1 = ValueId::iri_id(1);
        let vid_max = ValueId::iri_id(u32::MAX);
        assert!(vid0 < vid1);
        assert!(vid1 < vid_max);
    }

    #[test]
    fn test_lex_id_encoding() {
        let vid = ValueId::lex_id(100);
        assert_eq!(vid.tag(), 0x6);
        assert_eq!(vid.payload(), 100);
    }

    #[test]
    fn test_date_encoding() {
        // 2024-01-15 is day 19737 since epoch
        let days = 19737i32;
        let vid = ValueId::date(days);
        assert_eq!(vid.tag(), 0x7);
        assert_eq!(vid.decode_offset_binary(), days as i64);

        // Negative days (before epoch)
        let neg_days = -365i32;
        let vid_neg = ValueId::date(neg_days);
        assert_eq!(vid_neg.decode_offset_binary(), neg_days as i64);

        // Ordering: before epoch < after epoch
        assert!(vid_neg < vid);
    }

    #[test]
    fn test_time_encoding() {
        // 10:30:00 = 37800 seconds = 37_800_000_000 micros
        let micros: i64 = 37_800_000_000;
        let vid = ValueId::time(micros);
        assert_eq!(vid.tag(), 0x8);
        assert_eq!(vid.payload(), micros as u64);

        // Midnight < 10:30
        let midnight = ValueId::time(0);
        assert!(midnight < vid);
    }

    #[test]
    fn test_datetime_encoding() {
        // Some epoch micros value
        let epoch_micros: i64 = 1_705_312_200_000_000; // 2024-01-15T10:30:00Z approx
        let vid = ValueId::datetime(epoch_micros).unwrap();
        assert_eq!(vid.tag(), 0x9);
        assert_eq!(vid.decode_offset_binary(), epoch_micros);

        // Negative epoch (before 1970)
        let neg_epoch: i64 = -86_400_000_000; // 1 day before epoch
        let vid_neg = ValueId::datetime(neg_epoch).unwrap();
        assert_eq!(vid_neg.decode_offset_binary(), neg_epoch);
        assert!(vid_neg < vid);
    }

    #[test]
    fn test_datetime_i60_overflow() {
        // Values outside i60 range should return None
        assert!(ValueId::datetime(i64::MAX).is_none());
        assert!(ValueId::datetime(i64::MIN).is_none());
        // Values at i60 boundary should work
        assert!(ValueId::datetime(I60_MAX).is_some());
        assert!(ValueId::datetime(I60_MIN).is_some());
    }

    #[test]
    fn test_json_encoding() {
        let vid = ValueId::json(5);
        assert_eq!(vid.tag(), 0xB);
        assert_eq!(vid.payload(), 5);
    }

    #[test]
    fn test_sentinels() {
        assert!(ValueId::MIN.is_min());
        assert!(ValueId::MAX.is_max());
        assert!(ValueId::MIN < ValueId::NULL);
        assert!(ValueId::NULL < ValueId::BOOL_FALSE);
        assert!(ValueId::BOOL_TRUE < ValueId::num_int(0).unwrap());
        assert!(ValueId::lex_id(u32::MAX) < ValueId::MAX);
    }

    #[test]
    fn test_cross_tag_ordering() {
        // Cross-tag ordering is by type group, not numeric value
        let null = ValueId::NULL;
        let bool_f = ValueId::BOOL_FALSE;
        let int_0 = ValueId::num_int(0).unwrap();
        let iri = ValueId::iri_id(0);
        let lex = ValueId::lex_id(0);
        let date = ValueId::date(0);
        let time = ValueId::time(0);
        let dt = ValueId::datetime(0).unwrap();
        let json = ValueId::json(0);

        assert!(null < bool_f);
        assert!(bool_f < int_0);
        assert!(int_0 < iri);
        assert!(iri < lex);
        assert!(lex < date);
        assert!(date < time);
        assert!(time < dt);
        assert!(dt < json);
    }

    #[test]
    fn test_as_u64_from_u64_round_trip() {
        let original = ValueId::num_int(42).unwrap();
        let raw = original.as_u64();
        let restored = ValueId::from_u64(raw);
        assert_eq!(original, restored);
    }

    // ---- DatatypeId tests ----

    #[test]
    fn test_datatype_from_xsd() {
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "string"), DatatypeId::STRING);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "boolean"), DatatypeId::BOOLEAN);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "integer"), DatatypeId::INTEGER);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "long"), DatatypeId::LONG);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "int"), DatatypeId::INT);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "short"), DatatypeId::SHORT);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "byte"), DatatypeId::BYTE);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "double"), DatatypeId::DOUBLE);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "float"), DatatypeId::FLOAT);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "decimal"), DatatypeId::DECIMAL);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "dateTime"), DatatypeId::DATE_TIME);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "date"), DatatypeId::DATE);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "time"), DatatypeId::TIME);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "anyURI"), DatatypeId::ANY_URI);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "unsignedLong"), DatatypeId::UNSIGNED_LONG);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "unsignedInt"), DatatypeId::UNSIGNED_INT);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "unsignedShort"), DatatypeId::UNSIGNED_SHORT);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "unsignedByte"), DatatypeId::UNSIGNED_BYTE);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "nonNegativeInteger"), DatatypeId::NON_NEGATIVE_INTEGER);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "positiveInteger"), DatatypeId::POSITIVE_INTEGER);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "nonPositiveInteger"), DatatypeId::NON_POSITIVE_INTEGER);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "negativeInteger"), DatatypeId::NEGATIVE_INTEGER);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "normalizedString"), DatatypeId::NORMALIZED_STRING);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "token"), DatatypeId::TOKEN);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "language"), DatatypeId::LANGUAGE);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "duration"), DatatypeId::DURATION);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "dayTimeDuration"), DatatypeId::DAY_TIME_DURATION);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "yearMonthDuration"), DatatypeId::YEAR_MONTH_DURATION);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "base64Binary"), DatatypeId::BASE64_BINARY);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "hexBinary"), DatatypeId::HEX_BINARY);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "gYear"), DatatypeId::G_YEAR);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "gMonth"), DatatypeId::G_MONTH);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "gDay"), DatatypeId::G_DAY);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "gYearMonth"), DatatypeId::G_YEAR_MONTH);
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "gMonthDay"), DatatypeId::G_MONTH_DAY);
    }

    #[test]
    fn test_datatype_from_rdf() {
        assert_eq!(DatatypeId::from_ns_name(namespaces::RDF, "langString"), DatatypeId::LANG_STRING);
        assert_eq!(DatatypeId::from_ns_name(namespaces::RDF, "JSON"), DatatypeId::RDF_JSON);
    }

    #[test]
    fn test_datatype_from_jsonld() {
        assert_eq!(DatatypeId::from_ns_name(namespaces::JSON_LD, "id"), DatatypeId::JSON_LD_ID);
    }

    #[test]
    fn test_datatype_unknown() {
        assert_eq!(DatatypeId::from_ns_name(namespaces::XSD, "foobar"), DatatypeId::UNKNOWN);
        assert_eq!(DatatypeId::from_ns_name(namespaces::RDF, "foobar"), DatatypeId::UNKNOWN);
        assert_eq!(DatatypeId::from_ns_name(namespaces::JSON_LD, "foobar"), DatatypeId::UNKNOWN);
        assert_eq!(DatatypeId::from_ns_name(99, "anything"), DatatypeId::UNKNOWN);
    }

    #[test]
    fn test_datatype_as_u8_from_u8_round_trip() {
        for dt in [
            DatatypeId::STRING, DatatypeId::BOOLEAN, DatatypeId::INTEGER,
            DatatypeId::LONG, DatatypeId::DOUBLE, DatatypeId::DATE_TIME,
            DatatypeId::LANG_STRING, DatatypeId::JSON_LD_ID, DatatypeId::UNKNOWN,
        ] {
            let raw = dt.as_u8();
            assert_eq!(DatatypeId::from_u8(raw), dt);
        }
    }

    #[test]
    fn test_datatype_integer_type_classification() {
        // All integer subtypes
        for dt in [
            DatatypeId::INTEGER, DatatypeId::LONG, DatatypeId::INT,
            DatatypeId::SHORT, DatatypeId::BYTE, DatatypeId::UNSIGNED_LONG,
            DatatypeId::UNSIGNED_INT, DatatypeId::UNSIGNED_SHORT,
            DatatypeId::UNSIGNED_BYTE, DatatypeId::NON_NEGATIVE_INTEGER,
            DatatypeId::POSITIVE_INTEGER, DatatypeId::NON_POSITIVE_INTEGER,
            DatatypeId::NEGATIVE_INTEGER,
        ] {
            assert!(dt.is_integer_type(), "{} should be integer type", dt);
            assert!(!dt.is_float_type(), "{} should not be float type", dt);
        }

        // Float types
        for dt in [DatatypeId::DOUBLE, DatatypeId::FLOAT] {
            assert!(dt.is_float_type(), "{} should be float type", dt);
            assert!(!dt.is_integer_type(), "{} should not be integer type", dt);
        }

        // Non-numeric types
        for dt in [
            DatatypeId::STRING, DatatypeId::BOOLEAN, DatatypeId::DATE_TIME,
            DatatypeId::DATE, DatatypeId::TIME, DatatypeId::LANG_STRING,
        ] {
            assert!(!dt.is_integer_type(), "{} should not be integer type", dt);
            assert!(!dt.is_float_type(), "{} should not be float type", dt);
        }
    }

    #[test]
    fn test_num_int_zero_is_not_all_zeros() {
        // Offset-binary: 0 maps to 2^59, not 0 payload
        let zero = ValueId::num_int(0).unwrap();
        assert_ne!(zero.payload(), 0);
        assert_eq!(zero.payload(), I60_OFFSET);
    }
}
