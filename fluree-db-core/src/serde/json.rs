//! JSON serialization and deserialization for Fluree index data

use crate::error::{Error, Result};
use crate::flake::{Flake, FlakeMeta};
use crate::index::ChildRef;
use crate::sid::Sid;
use crate::temporal::{Date, DateTime, Time};
use crate::value::FlakeValue;
use bigdecimal::BigDecimal;
use fluree_vocab::xsd_names;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use simd_json::prelude::*;
use simd_json::{Deserializer, Node, StaticNode};
use std::cell::RefCell;
use std::collections::HashMap;
use std::str::FromStr;

// Reuse simd-json parser buffers across many leaf parses (big win on cold scans).
// Using thread-local avoids cross-thread mutability issues on native runtimes.
thread_local! {
    static SIMDJSON_BUFFERS: RefCell<simd_json::Buffers> = RefCell::new(simd_json::Buffers::new(1024));
}

/// The $id datatype namespace code (for references)
const ID_NAMESPACE_CODE: i32 = 1;
const ID_NAME: &str = "id";

/// XSD namespace code
const XSD_NAMESPACE_CODE: i32 = 2;

fn is_id_dt(dt: &Sid) -> bool {
    dt.namespace_code == ID_NAMESPACE_CODE && dt.name.as_ref() == ID_NAME
}

/// Check if datatype is xsd:dateTime
fn is_datetime_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD_NAMESPACE_CODE && dt.name.as_ref() == "dateTime"
}

/// Check if datatype is xsd:date
fn is_date_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD_NAMESPACE_CODE && dt.name.as_ref() == "date"
}

/// Check if datatype is xsd:time
fn is_time_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD_NAMESPACE_CODE && dt.name.as_ref() == "time"
}

/// Check if datatype is in the XSD integer family (14 types)
///
/// This includes: integer, long, int, short, byte, unsignedLong, unsignedInt,
/// unsignedShort, unsignedByte, nonNegativeInteger, positiveInteger,
/// nonPositiveInteger, negativeInteger
///
/// All of these can be represented as BigInt and need special handling for
/// round-trip serialization (BigInt serializes as string, so on deserialization
/// we need to recognize these types and parse back to BigInt).
fn is_integer_family_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD_NAMESPACE_CODE && xsd_names::is_integer_family_name(dt.name.as_ref())
}

/// Check if datatype is xsd:decimal (arbitrary precision)
fn is_decimal_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD_NAMESPACE_CODE && dt.name.as_ref() == "decimal"
}

// === Raw JSON structures for deserialization ===

/// Raw flake as it appears in JSON (7-element array)
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct RawFlake(Vec<serde_json::Value>);

impl RawFlake {
    /// Convert to Flake
    pub fn to_flake(&self) -> Result<Flake> {
        if self.0.len() != 7 {
            return Err(Error::other(format!(
                "Flake array must have 7 elements, got {}",
                self.0.len()
            )));
        }

        let s = deserialize_sid(&self.0[0])?;
        let p = deserialize_sid(&self.0[1])?;
        let dt = deserialize_sid(&self.0[3])?;
        let o = deserialize_object(&self.0[2], &dt)?;
        let t = self.0[4]
            .as_i64()
            .ok_or_else(|| Error::other("t must be integer"))?;
        let op = self.0[5]
            .as_bool()
            .ok_or_else(|| Error::other("op must be boolean"))?;
        let m = deserialize_meta(&self.0[6])?;

        Ok(Flake::new(s, p, o, dt, t, op, m))
    }

    /// Convert to Flake with SID interning (v1 format)
    pub fn to_flake_interned(&self, interner: &crate::SidInterner) -> Result<Flake> {
        if self.0.len() != 7 {
            return Err(Error::other(format!(
                "Flake array must have 7 elements, got {}",
                self.0.len()
            )));
        }

        let s = interner.intern_sid(&deserialize_sid(&self.0[0])?);
        let p = interner.intern_sid(&deserialize_sid(&self.0[1])?);
        let dt = interner.intern_sid(&deserialize_sid(&self.0[3])?);
        let o = deserialize_object_interned(&self.0[2], &dt, interner)?;
        let t = self.0[4]
            .as_i64()
            .ok_or_else(|| Error::other("t must be integer"))?;
        let op = self.0[5]
            .as_bool()
            .ok_or_else(|| Error::other("op must be boolean"))?;
        let m = deserialize_meta(&self.0[6])?;

        Ok(Flake::new(s, p, o, dt, t, op, m))
    }
}

/// Deserialize a SID from JSON
///
/// SIDs are serialized as `[namespace_code, name]` tuples.
pub fn deserialize_sid(value: &serde_json::Value) -> Result<Sid> {
    match value {
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let ns_code = arr[0]
                .as_i64()
                .ok_or_else(|| Error::other("SID namespace_code must be integer"))?
                as i32;
            let name = arr[1]
                .as_str()
                .ok_or_else(|| Error::other("SID name must be string"))?
                .to_string();
            Ok(Sid::new(ns_code, name))
        }
        _ => Err(Error::other(format!(
            "SID must be [namespace_code, name] array, got {:?}",
            value
        ))),
    }
}

/// Deserialize an object value from JSON
///
/// The object type depends on the datatype:
/// - If `dt` is $id (namespace_code=1, name="id"), object is a SID (reference)
/// - If `dt` is xsd:dateTime/date/time, parse string as temporal type
/// - If `dt` is xsd:integer, use BigInt for arbitrary precision
/// - If `dt` is xsd:decimal, use BigDecimal for arbitrary precision
/// - Otherwise, object is a literal value based on JSON type
pub fn deserialize_object(value: &serde_json::Value, dt: &Sid) -> Result<FlakeValue> {
    // Check if this is a reference
    if is_id_dt(dt) {
        let sid = deserialize_sid(value)?;
        return Ok(FlakeValue::Ref(sid));
    }

    // Handle temporal types - parse string values
    if is_datetime_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return DateTime::parse(s)
                .map(|dt| FlakeValue::DateTime(Box::new(dt)))
                .map_err(Error::other);
        }
    }
    if is_date_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Date::parse(s)
                .map(|d| FlakeValue::Date(Box::new(d)))
                .map_err(Error::other);
        }
    }
    if is_time_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Time::parse(s)
                .map(|t| FlakeValue::Time(Box::new(t)))
                .map_err(Error::other);
        }
    }

    // Handle arbitrary precision numeric types
    if is_integer_family_dt(dt) {
        match value {
            serde_json::Value::Number(n) => {
                // Try i64 first, fall back to BigInt
                if let Some(i) = n.as_i64() {
                    return Ok(FlakeValue::Long(i));
                }
                // Parse as BigInt from string representation
                let s = n.to_string();
                return BigInt::from_str(&s)
                    .map(|bi| FlakeValue::BigInt(Box::new(bi)))
                    .map_err(|e| Error::other(format!("Invalid integer: {}", e)));
            }
            serde_json::Value::String(s) => {
                // Try i64 first, fall back to BigInt
                if let Ok(i) = s.parse::<i64>() {
                    return Ok(FlakeValue::Long(i));
                }
                return BigInt::from_str(s)
                    .map(|bi| FlakeValue::BigInt(Box::new(bi)))
                    .map_err(|e| Error::other(format!("Invalid integer: {}", e)));
            }
            _ => {}
        }
    }
    if is_decimal_dt(dt) {
        match value {
            // JSON numbers → Double (policy: JSON already lost precision, use Double)
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    return Ok(FlakeValue::Long(i));
                } else if let Some(f) = n.as_f64() {
                    return Ok(FlakeValue::Double(f));
                }
                return Err(Error::other("Invalid decimal number"));
            }
            // String literals → BigDecimal (preserves precision from source)
            serde_json::Value::String(s) => {
                return BigDecimal::from_str(s)
                    .map(|bd| FlakeValue::Decimal(Box::new(bd)))
                    .map_err(|e| Error::other(format!("Invalid decimal: {}", e)));
            }
            _ => {}
        }
    }

    // Default deserialization based on JSON type
    match value {
        serde_json::Value::Null => Ok(FlakeValue::Null),
        serde_json::Value::Bool(b) => Ok(FlakeValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FlakeValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(FlakeValue::Double(f))
            } else {
                Err(Error::other("Invalid number"))
            }
        }
        serde_json::Value::String(s) => Ok(FlakeValue::String(s.clone())),
        // Arrays could be SIDs (references) or other complex types
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            // Could be a SID even if dt isn't $id (for backwards compatibility)
            if arr[0].is_i64() && arr[1].is_string() {
                let sid = deserialize_sid(value)?;
                Ok(FlakeValue::Ref(sid))
            } else {
                Ok(FlakeValue::String(value.to_string()))
            }
        }
        _ => Ok(FlakeValue::String(value.to_string())),
    }
}

/// Deserialize an object value from JSON with SID interning
///
/// Same as `deserialize_object` but interns reference SIDs.
fn deserialize_object_interned(
    value: &serde_json::Value,
    dt: &Sid,
    interner: &crate::SidInterner,
) -> Result<FlakeValue> {
    // Check if this is a reference - these need interning
    if is_id_dt(dt) {
        let sid = deserialize_sid(value)?;
        return Ok(FlakeValue::Ref(interner.intern_sid(&sid)));
    }

    // Check for SID array format (backwards compatibility)
    if let serde_json::Value::Array(arr) = value {
        if arr.len() == 2 && arr[0].is_i64() && arr[1].is_string() {
            let sid = deserialize_sid(value)?;
            return Ok(FlakeValue::Ref(interner.intern_sid(&sid)));
        }
    }

    // For non-reference types, delegate to deserialize_object which handles
    // datatype-aware coercion (temporal, BigInt, BigDecimal, etc.)
    deserialize_object(value, dt)
}

/// Deserialize flake metadata from JSON
pub fn deserialize_meta(value: &serde_json::Value) -> Result<Option<FlakeMeta>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(map) => {
            let lang = map.get("lang").and_then(|v| v.as_str()).map(String::from);
            let i = map.get("i").and_then(|v| v.as_i64()).map(|v| v as i32);
            if lang.is_none() && i.is_none() {
                Ok(None)
            } else {
                Ok(Some(FlakeMeta { lang, i }))
            }
        }
        // Integer metadata (hash for comparison)
        serde_json::Value::Number(n) => {
            let i = n.as_i64().map(|v| v as i32);
            Ok(Some(FlakeMeta { lang: None, i }))
        }
        _ => Ok(None),
    }
}

fn borrowed_sid_interned(
    value: &simd_json::BorrowedValue<'_>,
    interner: &crate::SidInterner,
) -> Result<Sid> {
    let arr = value
        .as_array()
        .ok_or_else(|| Error::other("SID must be [namespace_code, name] array"))?;
    if arr.len() != 2 {
        return Err(Error::other(format!(
            "SID must have 2 elements, got {}",
            arr.len()
        )));
    }
    let ns_code = arr[0]
        .as_i64()
        .ok_or_else(|| Error::other("SID namespace_code must be integer"))?
        as i32;
    let name = arr[1]
        .as_str()
        .ok_or_else(|| Error::other("SID name must be string"))?;

    // Avoid allocating a temporary `Sid` just to immediately intern it.
    Ok(interner.intern(ns_code, name))
}

fn borrowed_meta(value: &simd_json::BorrowedValue<'_>) -> Result<Option<FlakeMeta>> {
    match value {
        simd_json::BorrowedValue::Static(simd_json::StaticNode::Null) => Ok(None),
        simd_json::BorrowedValue::Object(map) => {
            let lang = map.get("lang").and_then(|v| v.as_str()).map(String::from);
            let i = map.get("i").and_then(|v| v.as_i64()).map(|v| v as i32);
            if lang.is_none() && i.is_none() {
                Ok(None)
            } else {
                Ok(Some(FlakeMeta { lang, i }))
            }
        }
        // Integer metadata (hash for comparison)
        simd_json::BorrowedValue::Static(simd_json::StaticNode::I64(n)) => Ok(Some(FlakeMeta {
            lang: None,
            i: Some(*n as i32),
        })),
        simd_json::BorrowedValue::Static(simd_json::StaticNode::U64(n)) => Ok(Some(FlakeMeta {
            lang: None,
            i: Some(*n as i32),
        })),
        _ => Ok(None),
    }
}

fn borrowed_object_interned(
    value: &simd_json::BorrowedValue<'_>,
    dt: &Sid,
    interner: &crate::SidInterner,
) -> Result<FlakeValue> {
    // v1 format reference: object is a SID array when dt is $id
    if is_id_dt(dt) {
        let sid = borrowed_sid_interned(value, interner)?;
        return Ok(FlakeValue::Ref(sid));
    }

    // Handle temporal types - parse string values
    if is_datetime_dt(dt) {
        if let simd_json::BorrowedValue::String(s) = value {
            return DateTime::parse(s)
                .map(|dt| FlakeValue::DateTime(Box::new(dt)))
                .map_err(Error::other);
        }
    }
    if is_date_dt(dt) {
        if let simd_json::BorrowedValue::String(s) = value {
            return Date::parse(s)
                .map(|d| FlakeValue::Date(Box::new(d)))
                .map_err(Error::other);
        }
    }
    if is_time_dt(dt) {
        if let simd_json::BorrowedValue::String(s) = value {
            return Time::parse(s)
                .map(|t| FlakeValue::Time(Box::new(t)))
                .map_err(Error::other);
        }
    }

    // Handle arbitrary precision integer
    if is_integer_family_dt(dt) {
        match value {
            simd_json::BorrowedValue::Static(simd_json::StaticNode::I64(n)) => {
                return Ok(FlakeValue::Long(*n));
            }
            simd_json::BorrowedValue::Static(simd_json::StaticNode::U64(n)) => {
                if *n <= i64::MAX as u64 {
                    return Ok(FlakeValue::Long(*n as i64));
                }
                return Ok(FlakeValue::BigInt(Box::new(BigInt::from(*n))));
            }
            simd_json::BorrowedValue::String(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    return Ok(FlakeValue::Long(i));
                }
                return BigInt::from_str(s)
                    .map(|bi| FlakeValue::BigInt(Box::new(bi)))
                    .map_err(|e| Error::other(format!("Invalid integer: {}", e)));
            }
            _ => {}
        }
    }

    // Handle arbitrary precision decimal
    // Policy: JSON numbers → Double (precision already lost), strings → BigDecimal
    if is_decimal_dt(dt) {
        match value {
            // JSON numbers → Long or Double
            simd_json::BorrowedValue::Static(simd_json::StaticNode::I64(n)) => {
                return Ok(FlakeValue::Long(*n));
            }
            simd_json::BorrowedValue::Static(simd_json::StaticNode::U64(n)) => {
                return Ok(FlakeValue::Long(*n as i64));
            }
            simd_json::BorrowedValue::Static(simd_json::StaticNode::F64(f)) => {
                return Ok(FlakeValue::Double(*f));
            }
            // String literals → BigDecimal (preserves precision from source)
            simd_json::BorrowedValue::String(s) => {
                return BigDecimal::from_str(s)
                    .map(|bd| FlakeValue::Decimal(Box::new(bd)))
                    .map_err(|e| Error::other(format!("Invalid decimal: {}", e)));
            }
            _ => {}
        }
    }

    // Default deserialization based on JSON type
    match value {
        simd_json::BorrowedValue::Static(simd_json::StaticNode::Null) => Ok(FlakeValue::Null),
        simd_json::BorrowedValue::Static(simd_json::StaticNode::Bool(b)) => {
            Ok(FlakeValue::Boolean(*b))
        }
        simd_json::BorrowedValue::Static(simd_json::StaticNode::I64(n)) => Ok(FlakeValue::Long(*n)),
        simd_json::BorrowedValue::Static(simd_json::StaticNode::U64(n)) => {
            Ok(FlakeValue::Long(*n as i64))
        }
        simd_json::BorrowedValue::Static(simd_json::StaticNode::F64(f)) => {
            Ok(FlakeValue::Double(*f))
        }
        simd_json::BorrowedValue::String(s) => Ok(FlakeValue::String(s.to_string())),

        // Arrays: could be a SID even if dt isn't $id (backwards-compat with old encoding)
        simd_json::BorrowedValue::Array(arr) if arr.len() == 2 => {
            if arr[0].is_i64() && arr[1].is_str() {
                let sid = borrowed_sid_interned(value, interner)?;
                Ok(FlakeValue::Ref(sid))
            } else {
                let s = value.encode();
                Ok(FlakeValue::String(s))
            }
        }

        _ => {
            let s = value.encode();
            Ok(FlakeValue::String(s))
        }
    }
}

fn parse_leaf_node_interned_borrowed(
    root: &simd_json::BorrowedValue<'_>,
    interner: &crate::SidInterner,
) -> Result<Vec<Flake>> {
    let obj = root
        .as_object()
        .ok_or_else(|| Error::other("leaf node must be a JSON object"))?;

    let flakes_val = obj
        .get("flakes")
        .ok_or_else(|| Error::other("leaf node missing flakes"))?;

    let flakes_arr = flakes_val
        .as_array()
        .ok_or_else(|| Error::other("leaf flakes must be an array"))?;

    // v2 dictionary format?
    let is_v2 = obj
        .get("version")
        .and_then(|v| v.as_i64())
        .map(|v| v == 2)
        .unwrap_or(false)
        && obj.get("dict").is_some();

    if is_v2 {
        let dict_val = obj
            .get("dict")
            .ok_or_else(|| Error::other("v2 leaf missing dict"))?;
        let dict_arr = dict_val
            .as_array()
            .ok_or_else(|| Error::other("v2 dict must be an array"))?;

        // Intern dict SIDs once; flakes will clone cheap Arcs.
        let mut sid_dict: Vec<Sid> = Vec::with_capacity(dict_arr.len());
        for sid_val in dict_arr {
            sid_dict.push(borrowed_sid_interned(sid_val, interner)?);
        }

        let mut out: Vec<Flake> = Vec::with_capacity(flakes_arr.len());
        for flake_val in flakes_arr {
            let flake = flake_val
                .as_array()
                .ok_or_else(|| Error::other("Dict flake must be an array"))?;
            if flake.len() != 7 {
                return Err(Error::other("Dict flake must have 7 elements"));
            }

            let s_idx = flake[0]
                .as_i64()
                .ok_or_else(|| Error::other("s_idx must be integer"))?
                as usize;
            let p_idx = flake[1]
                .as_i64()
                .ok_or_else(|| Error::other("p_idx must be integer"))?
                as usize;
            let dt_idx = flake[3]
                .as_i64()
                .ok_or_else(|| Error::other("dt_idx must be integer"))?
                as usize;

            let s = sid_dict
                .get(s_idx)
                .cloned()
                .ok_or_else(|| Error::other(format!("Invalid s_idx: {}", s_idx)))?;
            let p = sid_dict
                .get(p_idx)
                .cloned()
                .ok_or_else(|| Error::other(format!("Invalid p_idx: {}", p_idx)))?;
            let dt = sid_dict
                .get(dt_idx)
                .cloned()
                .ok_or_else(|| Error::other(format!("Invalid dt_idx: {}", dt_idx)))?;

            let o = if is_id_dt(&dt) {
                let o_idx = flake[2]
                    .as_i64()
                    .ok_or_else(|| Error::other("o_idx must be integer"))?
                    as usize;
                let o_sid = sid_dict
                    .get(o_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid o_idx: {}", o_idx)))?;
                FlakeValue::Ref(o_sid)
            } else {
                borrowed_object_interned(&flake[2], &dt, interner)?
            };

            let t = flake[4]
                .as_i64()
                .ok_or_else(|| Error::other("t must be integer"))?;
            let op = flake[5]
                .as_bool()
                .ok_or_else(|| Error::other("op must be boolean"))?;
            let m = borrowed_meta(&flake[6])?;

            out.push(Flake::new(s, p, o, dt, t, op, m));
        }
        return Ok(out);
    }

    // v1 leaf format (inline SIDs)
    let mut out: Vec<Flake> = Vec::with_capacity(flakes_arr.len());
    for flake_val in flakes_arr {
        let flake = flake_val
            .as_array()
            .ok_or_else(|| Error::other("Flake must be an array"))?;
        if flake.len() != 7 {
            return Err(Error::other(format!(
                "Flake array must have 7 elements, got {}",
                flake.len()
            )));
        }

        let s = borrowed_sid_interned(&flake[0], interner)?;
        let p = borrowed_sid_interned(&flake[1], interner)?;
        let dt = borrowed_sid_interned(&flake[3], interner)?;
        let o = borrowed_object_interned(&flake[2], &dt, interner)?;
        let t = flake[4]
            .as_i64()
            .ok_or_else(|| Error::other("t must be integer"))?;
        let op = flake[5]
            .as_bool()
            .ok_or_else(|| Error::other("op must be boolean"))?;
        let m = borrowed_meta(&flake[6])?;

        out.push(Flake::new(s, p, o, dt, t, op, m));
    }
    Ok(out)
}

// === Leaf Node ===

/// Raw leaf node as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawLeafNode {
    pub flakes: Vec<RawFlake>,
    /// Version (1 or 2 for dictionary format)
    #[serde(default)]
    pub version: Option<i32>,
    /// Dictionary for v2 format
    #[serde(default)]
    pub dict: Option<Vec<serde_json::Value>>,
}

impl RawLeafNode {
    /// Convert to resolved flakes
    pub fn to_flakes(&self) -> Result<Vec<Flake>> {
        // Check for v2 dictionary format
        if self.version == Some(2) && self.dict.is_some() {
            return self.to_flakes_v2();
        }

        // Standard v1 format
        self.flakes.iter().map(|rf| rf.to_flake()).collect()
    }

    /// Convert to resolved flakes with SID interning.
    ///
    /// This is the optimized path that interns SIDs during parsing,
    /// eliminating post-parse interning overhead.
    pub fn to_flakes_interned(&self, interner: &crate::SidInterner) -> Result<Vec<Flake>> {
        // Check for v2 dictionary format
        if self.version == Some(2) && self.dict.is_some() {
            return self.to_flakes_v2_interned(interner);
        }

        // Standard v1 format with interning
        self.flakes
            .iter()
            .map(|rf| rf.to_flake_interned(interner))
            .collect()
    }

    /// Convert v2 dictionary format to flakes
    fn to_flakes_v2(&self) -> Result<Vec<Flake>> {
        let dict = self
            .dict
            .as_ref()
            .ok_or_else(|| Error::other("v2 leaf missing dict"))?;

        // Build SID dictionary
        let sid_dict: Vec<Sid> = dict.iter().map(deserialize_sid).collect::<Result<_>>()?;

        // Deserialize flakes using dictionary indices
        self.flakes
            .iter()
            .map(|rf| {
                if rf.0.len() != 7 {
                    return Err(Error::other("Dict flake must have 7 elements"));
                }

                // s, p, dt are indices into dictionary
                let s_idx = rf.0[0]
                    .as_i64()
                    .ok_or_else(|| Error::other("s_idx must be integer"))?
                    as usize;
                let p_idx = rf.0[1]
                    .as_i64()
                    .ok_or_else(|| Error::other("p_idx must be integer"))?
                    as usize;
                let dt_idx = rf.0[3]
                    .as_i64()
                    .ok_or_else(|| Error::other("dt_idx must be integer"))?
                    as usize;

                let s = sid_dict
                    .get(s_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid s_idx: {}", s_idx)))?;
                let p = sid_dict
                    .get(p_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid p_idx: {}", p_idx)))?;
                let dt = sid_dict
                    .get(dt_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid dt_idx: {}", dt_idx)))?;

                // o: index if reference, otherwise literal
                let o = if dt.namespace_code == ID_NAMESPACE_CODE && dt.name.as_ref() == ID_NAME {
                    let o_idx = rf.0[2]
                        .as_i64()
                        .ok_or_else(|| Error::other("o_idx must be integer"))?
                        as usize;
                    let o_sid = sid_dict
                        .get(o_idx)
                        .cloned()
                        .ok_or_else(|| Error::other(format!("Invalid o_idx: {}", o_idx)))?;
                    FlakeValue::Ref(o_sid)
                } else {
                    deserialize_object(&rf.0[2], &dt)?
                };

                let t = rf.0[4]
                    .as_i64()
                    .ok_or_else(|| Error::other("t must be integer"))?;
                let op = rf.0[5]
                    .as_bool()
                    .ok_or_else(|| Error::other("op must be boolean"))?;
                let m = deserialize_meta(&rf.0[6])?;

                Ok(Flake::new(s, p, o, dt, t, op, m))
            })
            .collect()
    }

    /// Convert v2 dictionary format to flakes with SID interning.
    ///
    /// This is the key optimization: we intern the dictionary SIDs ONCE,
    /// then all flakes reference the same interned SIDs via indices.
    /// No per-flake string allocation or hash map lookup needed.
    fn to_flakes_v2_interned(&self, interner: &crate::SidInterner) -> Result<Vec<Flake>> {
        let dict = self
            .dict
            .as_ref()
            .ok_or_else(|| Error::other("v2 leaf missing dict"))?;

        // Build interned SID dictionary - this is the key optimization.
        // We intern each unique SID exactly once, then all flakes share them.
        let sid_dict: Vec<Sid> = dict
            .iter()
            .map(|v| {
                let raw_sid = deserialize_sid(v)?;
                Ok(interner.intern_sid(&raw_sid))
            })
            .collect::<Result<_>>()?;

        // Deserialize flakes using dictionary indices.
        // Since SIDs are interned in the dictionary, we just clone the Arc pointers.
        self.flakes
            .iter()
            .map(|rf| {
                if rf.0.len() != 7 {
                    return Err(Error::other("Dict flake must have 7 elements"));
                }

                // s, p, dt are indices into dictionary
                let s_idx = rf.0[0]
                    .as_i64()
                    .ok_or_else(|| Error::other("s_idx must be integer"))?
                    as usize;
                let p_idx = rf.0[1]
                    .as_i64()
                    .ok_or_else(|| Error::other("p_idx must be integer"))?
                    as usize;
                let dt_idx = rf.0[3]
                    .as_i64()
                    .ok_or_else(|| Error::other("dt_idx must be integer"))?
                    as usize;

                // Clone from dictionary - these are cheap Arc clones of interned SIDs
                let s = sid_dict
                    .get(s_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid s_idx: {}", s_idx)))?;
                let p = sid_dict
                    .get(p_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid p_idx: {}", p_idx)))?;
                let dt = sid_dict
                    .get(dt_idx)
                    .cloned()
                    .ok_or_else(|| Error::other(format!("Invalid dt_idx: {}", dt_idx)))?;

                // o: index if reference, otherwise literal
                let o = if dt.namespace_code == ID_NAMESPACE_CODE && dt.name.as_ref() == ID_NAME {
                    let o_idx = rf.0[2]
                        .as_i64()
                        .ok_or_else(|| Error::other("o_idx must be integer"))?
                        as usize;
                    let o_sid = sid_dict
                        .get(o_idx)
                        .cloned()
                        .ok_or_else(|| Error::other(format!("Invalid o_idx: {}", o_idx)))?;
                    FlakeValue::Ref(o_sid)
                } else {
                    deserialize_object(&rf.0[2], &dt)?
                };

                let t = rf.0[4]
                    .as_i64()
                    .ok_or_else(|| Error::other("t must be integer"))?;
                let op = rf.0[5]
                    .as_bool()
                    .ok_or_else(|| Error::other("op must be boolean"))?;
                let m = deserialize_meta(&rf.0[6])?;

                Ok(Flake::new(s, p, o, dt, t, op, m))
            })
            .collect()
    }
}

// === Branch Node ===

/// Raw child node reference as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawChildRef {
    pub id: String,
    pub leaf: bool,
    #[serde(default)]
    pub first: Option<RawFlake>,
    #[serde(default)]
    pub rhs: Option<RawFlake>,
    #[serde(default)]
    pub size: u64,
    /// Serialized byte size of this node (added for accurate cache eviction)
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(rename = "leftmost?", default)]
    pub leftmost: bool,
}

impl RawChildRef {
    /// Convert to ChildRef
    pub fn to_child_ref(&self) -> Result<ChildRef> {
        let first = self.first.as_ref().map(|rf| rf.to_flake()).transpose()?;
        let rhs = self.rhs.as_ref().map(|rf| rf.to_flake()).transpose()?;

        Ok(ChildRef {
            id: self.id.clone(),
            leaf: self.leaf,
            first,
            rhs,
            size: self.size,
            bytes: self.bytes,
            leftmost: self.leftmost,
        })
    }
}

/// Raw branch node as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawBranchNode {
    pub children: Vec<RawChildRef>,
}

impl RawBranchNode {
    /// Convert to child refs
    pub fn to_children(&self) -> Result<Vec<ChildRef>> {
        self.children.iter().map(|rc| rc.to_child_ref()).collect()
    }
}

// === DB Root Stats ===

/// Per-property statistics entry
///
/// Contains HLL-derived NDV estimates for a single property.
/// Stored sorted by SID in the db-root for determinism.
#[derive(Debug, Clone)]
pub struct PropertyStatEntry {
    /// Predicate SID as (namespace_code, name)
    pub sid: (i32, String),
    /// Total number of flakes with this property (including history)
    pub count: u64,
    /// Estimated number of distinct object values (via HLL)
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (via HLL)
    pub ndv_subjects: u64,
    /// Most recent transaction time that modified this property
    pub last_modified_t: i64,
}

/// Basic index statistics (fast estimates)
///
/// These are designed to be maintained incrementally during indexing/refresh and
/// **must not require walking the full index tree** (which can be many GBs).
///
/// Semantics mirror the Clojure implementation:
/// - `flakes`: total number of flakes in the index (including history; after dedup)
/// - `size`: estimated total bytes of flakes in the index (speed over accuracy)
/// - `properties`: per-property HLL statistics (optional, requires hll-stats feature)
/// - `classes`: per-class property usage statistics (optional)
#[derive(Debug, Clone, Default)]
pub struct DbRootStats {
    /// Total number of flakes in the index (including history; after dedup)
    pub flakes: u64,
    /// Estimated total bytes of flakes in the index (not storage bytes of index nodes)
    pub size: u64,
    /// Per-property statistics (sorted by SID for determinism)
    /// Only populated when hll-stats feature is enabled during indexing.
    pub properties: Option<Vec<PropertyStatEntry>>,
    /// Per-class property usage statistics (sorted by class SID for determinism)
    /// Tracks which properties are used by instances of each class, with datatype,
    /// reference class, and language tag breakdown.
    pub classes: Option<Vec<ClassStatEntry>>,
}

// === Class-Property Statistics ===

/// Statistics for a single class (rdf:type target)
///
/// Tracks property usage patterns for instances of this class.
/// Used for query optimization (selectivity estimation) and schema inference.
#[derive(Debug, Clone)]
pub struct ClassStatEntry {
    /// The class SID (target of rdf:type assertions)
    pub class_sid: Sid,
    /// Number of instances of this class
    pub count: u64,
    /// Properties used by instances of this class (sorted by property SID)
    pub properties: Vec<ClassPropertyUsage>,
}

/// Property usage statistics within a class
///
/// For each property used by instances of a class, tracks:
/// - Datatypes used (e.g., xsd:string, xsd:integer)
/// - Referenced classes (for @id/@ref properties)
/// - Language tags (for rdf:langString values)
#[derive(Debug, Clone)]
pub struct ClassPropertyUsage {
    /// The property SID
    pub property_sid: Sid,
    /// Datatype usage: datatype SID → count
    pub types: Vec<(Sid, u64)>,
    /// Referenced classes: class SID → count (for @id datatype refs)
    pub ref_classes: Vec<(Sid, u64)>,
    /// Language tag usage: language tag → count (for rdf:langString)
    pub langs: Vec<(String, u64)>,
}

/// Raw property stat entry as it appears in JSON
///
/// Clojure format is a compact array: `[[namespace_code, "name"], [count, ndv_values, ndv_subjects, ?, ?, last_modified_t]]`
#[derive(Debug)]
pub struct RawPropertyStatEntry {
    /// SID as (namespace_code, name)
    pub sid: (i32, String),
    pub count: u64,
    pub ndv_values: u64,
    pub ndv_subjects: u64,
    pub last_modified_t: i64,
}

impl<'de> serde::Deserialize<'de> for RawPropertyStatEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};

        struct PropertyStatVisitor;

        impl<'de> Visitor<'de> for PropertyStatVisitor {
            type Value = RawPropertyStatEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a property stat entry as [[ns_code, name], [count, ndv_values, ndv_subjects, ?, ?, last_modified_t]]")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // First element: SID as [namespace_code, name]
                let sid: (i32, String) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                // Second element: stats array [count, ndv_values, ndv_subjects, ?, ?, last_modified_t]
                let stats: Vec<i64> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                if stats.len() < 6 {
                    return Err(de::Error::invalid_length(
                        stats.len(),
                        &"stats array with at least 6 elements",
                    ));
                }

                Ok(RawPropertyStatEntry {
                    sid,
                    count: stats[0] as u64,
                    ndv_values: stats[1] as u64,
                    ndv_subjects: stats[2] as u64,
                    // stats[3] and stats[4] are unknown/unused
                    last_modified_t: stats[5],
                })
            }
        }

        deserializer.deserialize_seq(PropertyStatVisitor)
    }
}

/// Raw stats as it appears in JSON
#[derive(Debug, Deserialize, Default)]
pub struct RawDbRootStats {
    #[serde(default)]
    pub flakes: Option<u64>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub properties: Option<Vec<RawPropertyStatEntry>>,
    #[serde(default)]
    pub classes: Option<Vec<RawClassStatEntry>>,
}

/// Raw class stat entry as it appears in JSON
///
/// Clojure format: `[[namespace_code, "name"], [count, [property_usages...]]]`
#[derive(Debug)]
pub struct RawClassStatEntry {
    /// Class SID as (namespace_code, name)
    pub class_sid: (i32, String),
    pub count: u64,
    pub properties: Option<Vec<RawClassPropertyUsage>>,
}

impl<'de> serde::Deserialize<'de> for RawClassStatEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};

        struct ClassStatVisitor;

        impl<'de> Visitor<'de> for ClassStatVisitor {
            type Value = RawClassStatEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a class stat entry as [[ns_code, name], [count, [property_usages...]]]",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // First element: class SID as [namespace_code, name]
                let class_sid: (i32, String) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                // Second element: [count, [property_usages...]]
                let stats: (u64, Vec<RawClassPropertyUsage>) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                Ok(RawClassStatEntry {
                    class_sid,
                    count: stats.0,
                    properties: if stats.1.is_empty() {
                        None
                    } else {
                        Some(stats.1)
                    },
                })
            }
        }

        deserializer.deserialize_seq(ClassStatVisitor)
    }
}

/// Raw class property usage as it appears in JSON
///
/// Clojure format: `[[property_sid], [[types], [ref_classes], [langs]]]`
/// Where types/ref_classes are `[[sid, count], ...]` and langs are `[[lang_string, count], ...]`
#[derive(Debug)]
pub struct RawClassPropertyUsage {
    /// Property SID as (namespace_code, name)
    pub property_sid: (i32, String),
    /// Datatype counts: (sid, count) pairs
    pub types: Option<Vec<((i32, String), u64)>>,
    /// Referenced class counts: (sid, count) pairs
    pub ref_classes: Option<Vec<((i32, String), u64)>>,
    /// Language tag counts: (lang, count) pairs
    pub langs: Option<Vec<(String, u64)>>,
}

impl<'de> serde::Deserialize<'de> for RawClassPropertyUsage {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};

        struct PropertyUsageVisitor;

        impl<'de> Visitor<'de> for PropertyUsageVisitor {
            type Value = RawClassPropertyUsage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a property usage as [[property_sid], [[types], [ref_classes], [langs]]]",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // First element: property SID as [namespace_code, name]
                let property_sid: (i32, String) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                // Second element: [[types], [ref_classes], [langs]]
                // Each is a list of [sid/string, count] pairs
                let usage_data: (
                    Vec<((i32, String), u64)>, // types: [[datatype_sid, count], ...]
                    Vec<((i32, String), u64)>, // ref_classes: [[class_sid, count], ...]
                    Vec<(String, u64)>,        // langs: [[lang_string, count], ...]
                ) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                Ok(RawClassPropertyUsage {
                    property_sid,
                    types: if usage_data.0.is_empty() {
                        None
                    } else {
                        Some(usage_data.0)
                    },
                    ref_classes: if usage_data.1.is_empty() {
                        None
                    } else {
                        Some(usage_data.1)
                    },
                    langs: if usage_data.2.is_empty() {
                        None
                    } else {
                        Some(usage_data.2)
                    },
                })
            }
        }

        deserializer.deserialize_seq(PropertyUsageVisitor)
    }
}

// === DB Root Config ===

/// Index configuration persisted in db-root
#[derive(Debug, Clone, Default)]
pub struct DbRootConfig {
    /// Soft threshold for reindexing (bytes)
    pub reindex_min_bytes: Option<u64>,
    /// Hard threshold for reindexing (bytes)
    pub reindex_max_bytes: Option<u64>,
    /// Maximum number of old indexes to keep (default: 5)
    pub max_old_indexes: Option<u32>,
    /// Minimum age in minutes before an index can be garbage collected (default: 30)
    /// This prevents GC of indexes still in use by query servers during rapid transactions
    pub min_time_garbage_mins: Option<u32>,
}

/// Raw config as it appears in JSON
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RawDbRootConfig {
    #[serde(default)]
    pub reindex_min_bytes: Option<u64>,
    #[serde(default)]
    pub reindex_max_bytes: Option<u64>,
    #[serde(default)]
    pub max_old_indexes: Option<u32>,
    #[serde(default)]
    pub min_time_garbage_mins: Option<u32>,
}

// === Previous Index Reference ===

/// Reference to previous index for GC chain
#[derive(Debug, Clone)]
pub struct PrevIndexRef {
    /// Transaction ID of the previous index
    pub t: i64,
    /// Content address of the previous index db-root
    pub address: String,
}

/// Raw prev-index as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawPrevIndex {
    pub t: i64,
    pub address: String,
}

// === Garbage Reference ===

/// Reference to garbage record for GC chain
///
/// Links the db-root to its garbage manifest file.
/// The garbage record contains addresses of obsolete nodes replaced during
/// this refresh operation.
#[derive(Debug, Clone, PartialEq)]
pub struct GarbageRef {
    /// Address of the garbage record file
    pub address: String,
}

/// Raw garbage reference as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawGarbageRef {
    pub address: String,
}

// === Schema ===

/// Predicate info entry in schema
///
/// Each entry describes a predicate/class with its relationships:
/// - id: The predicate/class SID
/// - subclass_of: Parent classes (for rdfs:subClassOf)
/// - parent_props: Parent properties (for rdfs:subPropertyOf)
/// - child_props: Child properties (inverse of subPropertyOf)
#[derive(Debug, Clone)]
pub struct SchemaPredicateInfo {
    /// Predicate/class SID
    pub id: Sid,
    /// Parent classes (from rdfs:subClassOf assertions)
    pub subclass_of: Vec<Sid>,
    /// Parent properties (from rdfs:subPropertyOf assertions)
    pub parent_props: Vec<Sid>,
    /// Child properties (inverse of subPropertyOf)
    pub child_props: Vec<Sid>,
}

/// Schema predicates structure (Clojure-compatible format)
///
/// Uses a columnar format with fixed keys and values arrays:
/// - keys: ["id", "subclassOf", "parentProps", "childProps"]
/// - vals: [[sid, [parents], [parent_props], [child_props]], ...]
#[derive(Debug, Clone, Default)]
pub struct SchemaPredicates {
    /// Fixed keys: ["id", "subclassOf", "parentProps", "childProps"]
    pub keys: Vec<String>,
    /// Values: one entry per predicate, sorted by SID for determinism
    pub vals: Vec<SchemaPredicateInfo>,
}

/// Index schema persisted in db-root
///
/// Tracks class/property hierarchy information for query optimization.
#[derive(Debug, Clone)]
pub struct DbRootSchema {
    /// Transaction ID when schema was last updated
    pub t: i64,
    /// Predicate/class metadata
    pub pred: SchemaPredicates,
}

impl Default for DbRootSchema {
    fn default() -> Self {
        Self {
            t: 0,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: Vec::new(),
            },
        }
    }
}

/// Raw schema predicates as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawSchemaPredicates {
    #[serde(default)]
    pub keys: Vec<String>,
    /// vals is an array of arrays: [[sid, [subclass_of], [parent_props], [child_props]], ...]
    #[serde(default)]
    pub vals: Vec<Vec<serde_json::Value>>,
}

/// Raw schema as it appears in JSON
#[derive(Debug, Deserialize)]
pub struct RawDbRootSchema {
    pub t: i64,
    #[serde(default)]
    pub pred: Option<RawSchemaPredicates>,
}

// === DB Root ===

/// Raw index root data
#[derive(Debug, Deserialize)]
pub struct RawIndexRoot {
    pub id: String,
    #[serde(default)]
    pub leaf: bool,
    #[serde(default)]
    pub first: Option<RawFlake>,
    #[serde(default)]
    pub rhs: Option<RawFlake>,
    #[serde(default)]
    pub size: u64,
    /// Serialized byte size of this node (added for accurate cache eviction)
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(rename = "leftmost?", default)]
    pub leftmost: bool,
}

impl RawIndexRoot {
    /// Convert to ChildRef (used as index root)
    pub fn to_child_ref(&self) -> Result<ChildRef> {
        let first = self.first.as_ref().map(|rf| rf.to_flake()).transpose()?;
        let rhs = self.rhs.as_ref().map(|rf| rf.to_flake()).transpose()?;

        Ok(ChildRef {
            id: self.id.clone(),
            leaf: self.leaf,
            first,
            rhs,
            size: self.size,
            bytes: self.bytes,
            leftmost: self.leftmost,
        })
    }
}

/// Raw DB root as it appears in JSON
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RawDbRoot {
    pub ledger_alias: String,
    pub t: i64,
    #[serde(default)]
    pub v: Option<i32>,
    #[serde(default)]
    pub namespace_codes: HashMap<String, String>,
    #[serde(default)]
    pub spot: Option<RawIndexRoot>,
    #[serde(default)]
    pub psot: Option<RawIndexRoot>,
    #[serde(default)]
    pub post: Option<RawIndexRoot>,
    #[serde(default)]
    pub opst: Option<RawIndexRoot>,
    #[serde(default)]
    pub tspo: Option<RawIndexRoot>,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub stats: Option<RawDbRootStats>,
    #[serde(default)]
    pub config: Option<RawDbRootConfig>,
    #[serde(default)]
    pub prev_index: Option<RawPrevIndex>,
    #[serde(default)]
    pub schema: Option<RawDbRootSchema>,
    #[serde(default)]
    pub garbage: Option<RawGarbageRef>,
}

/// Parsed DB root
#[derive(Debug)]
pub struct DbRoot {
    pub alias: String,
    pub t: i64,
    pub version: i32,
    /// namespace_code -> IRI prefix
    pub namespace_codes: HashMap<i32, String>,
    pub spot: Option<ChildRef>,
    pub psot: Option<ChildRef>,
    pub post: Option<ChildRef>,
    pub opst: Option<ChildRef>,
    pub tspo: Option<ChildRef>,
    pub timestamp: Option<i64>,
    /// Index statistics (flakes count, total size)
    pub stats: Option<DbRootStats>,
    /// Index configuration (reindex thresholds)
    pub config: Option<DbRootConfig>,
    /// Reference to previous index for GC chain
    pub prev_index: Option<PrevIndexRef>,
    /// Schema (class/property hierarchy)
    pub schema: Option<DbRootSchema>,
    /// Reference to garbage record for GC
    pub garbage: Option<GarbageRef>,
}

impl RawDbRoot {
    /// Convert to parsed DbRoot
    pub fn to_db_root(&self) -> Result<DbRoot> {
        // Parse namespace_codes: keys are stringified integers
        let namespace_codes: HashMap<i32, String> = self
            .namespace_codes
            .iter()
            .filter_map(|(k, v)| k.parse::<i32>().ok().map(|code| (code, v.clone())))
            .collect();

        // Convert stats if present
        let stats = self.stats.as_ref().and_then(|s| {
            // Only create DbRootStats if at least one field is present
            if s.flakes.is_some()
                || s.size.is_some()
                || s.properties.is_some()
                || s.classes.is_some()
            {
                // Convert properties if present
                let properties = s.properties.as_ref().map(|props| {
                    props
                        .iter()
                        .map(|p| PropertyStatEntry {
                            sid: p.sid.clone(),
                            count: p.count,
                            ndv_values: p.ndv_values,
                            ndv_subjects: p.ndv_subjects,
                            last_modified_t: p.last_modified_t,
                        })
                        .collect()
                });

                // Convert classes if present
                let classes = s.classes.as_ref().map(|class_list| {
                    class_list
                        .iter()
                        .map(|c| {
                            let class_sid = Sid::new(c.class_sid.0, &c.class_sid.1);
                            let properties = c
                                .properties
                                .as_ref()
                                .map(|props| {
                                    props
                                        .iter()
                                        .map(|p| {
                                            let property_sid =
                                                Sid::new(p.property_sid.0, &p.property_sid.1);
                                            let types = p
                                                .types
                                                .as_ref()
                                                .map(|t| {
                                                    t.iter()
                                                        .map(|((ns, name), count)| {
                                                            (Sid::new(*ns, name), *count)
                                                        })
                                                        .collect()
                                                })
                                                .unwrap_or_default();
                                            let ref_classes = p
                                                .ref_classes
                                                .as_ref()
                                                .map(|r| {
                                                    r.iter()
                                                        .map(|((ns, name), count)| {
                                                            (Sid::new(*ns, name), *count)
                                                        })
                                                        .collect()
                                                })
                                                .unwrap_or_default();
                                            let langs = p.langs.clone().unwrap_or_default();
                                            ClassPropertyUsage {
                                                property_sid,
                                                types,
                                                ref_classes,
                                                langs,
                                            }
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            ClassStatEntry {
                                class_sid,
                                count: c.count,
                                properties,
                            }
                        })
                        .collect()
                });

                Some(DbRootStats {
                    flakes: s.flakes.unwrap_or(0),
                    size: s.size.unwrap_or(0),
                    properties,
                    classes,
                })
            } else {
                None
            }
        });

        // Convert config if present
        let config = self.config.as_ref().and_then(|c| {
            // Only create DbRootConfig if at least one field is present
            if c.reindex_min_bytes.is_some()
                || c.reindex_max_bytes.is_some()
                || c.max_old_indexes.is_some()
                || c.min_time_garbage_mins.is_some()
            {
                Some(DbRootConfig {
                    reindex_min_bytes: c.reindex_min_bytes,
                    reindex_max_bytes: c.reindex_max_bytes,
                    max_old_indexes: c.max_old_indexes,
                    min_time_garbage_mins: c.min_time_garbage_mins,
                })
            } else {
                None
            }
        });

        // Convert prev_index if present
        let prev_index = self.prev_index.as_ref().map(|p| PrevIndexRef {
            t: p.t,
            address: p.address.clone(),
        });

        // Convert schema if present
        let schema = self.schema.as_ref().map(|s| {
            let pred = s
                .pred
                .as_ref()
                .map(|p| {
                    // Parse vals: each entry is [sid, [subclass_of], [parent_props], [child_props]]
                    let vals = p
                        .vals
                        .iter()
                        .filter_map(|entry| {
                            if entry.len() < 4 {
                                return None;
                            }
                            // Parse SID from first element
                            let id = deserialize_sid(&entry[0]).ok()?;
                            // Parse subclass_of from second element (array of SIDs)
                            let subclass_of = entry[1]
                                .as_array()
                                .map(|arr| {
                                    arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect()
                                })
                                .unwrap_or_default();
                            // Parse parent_props from third element
                            let parent_props = entry[2]
                                .as_array()
                                .map(|arr| {
                                    arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect()
                                })
                                .unwrap_or_default();
                            // Parse child_props from fourth element
                            let child_props = entry[3]
                                .as_array()
                                .map(|arr| {
                                    arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect()
                                })
                                .unwrap_or_default();
                            Some(SchemaPredicateInfo {
                                id,
                                subclass_of,
                                parent_props,
                                child_props,
                            })
                        })
                        .collect();
                    SchemaPredicates {
                        keys: p.keys.clone(),
                        vals,
                    }
                })
                .unwrap_or_default();
            DbRootSchema { t: s.t, pred }
        });

        // Convert garbage if present
        let garbage = self.garbage.as_ref().map(|g| GarbageRef {
            address: g.address.clone(),
        });

        Ok(DbRoot {
            alias: self.ledger_alias.clone(),
            t: self.t,
            version: self.v.unwrap_or(1),
            namespace_codes,
            spot: self.spot.as_ref().map(|r| r.to_child_ref()).transpose()?,
            psot: self.psot.as_ref().map(|r| r.to_child_ref()).transpose()?,
            post: self.post.as_ref().map(|r| r.to_child_ref()).transpose()?,
            opst: self.opst.as_ref().map(|r| r.to_child_ref()).transpose()?,
            tspo: self.tspo.as_ref().map(|r| r.to_child_ref()).transpose()?,
            timestamp: self.timestamp,
            stats,
            config,
            prev_index,
            schema,
            garbage,
        })
    }
}

// === Serialization structures ===

/// Serializable SID (as [namespace_code, name] array)
fn serialize_sid(sid: &Sid) -> serde_json::Value {
    serde_json::json!([sid.namespace_code, sid.name.as_ref()])
}

/// Serializable flake metadata
fn serialize_meta(meta: &Option<FlakeMeta>) -> serde_json::Value {
    match meta {
        None => serde_json::Value::Null,
        Some(m) => {
            let mut obj = serde_json::Map::new();
            if let Some(ref lang) = m.lang {
                obj.insert("lang".to_string(), serde_json::Value::String(lang.clone()));
            }
            if let Some(i) = m.i {
                obj.insert("i".to_string(), serde_json::Value::Number(i.into()));
            }
            if obj.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::Object(obj)
            }
        }
    }
}

/// Serializable flake value (literal or reference)
/// The `_dt` parameter is provided for future use (e.g., custom datatype serialization)
fn serialize_object(value: &FlakeValue, _dt: &Sid) -> serde_json::Value {
    match value {
        FlakeValue::Ref(ref_sid) => serialize_sid(ref_sid),
        FlakeValue::Null => serde_json::Value::Null,
        FlakeValue::Boolean(b) => serde_json::Value::Bool(*b),
        FlakeValue::Long(n) => serde_json::Value::Number((*n).into()),
        FlakeValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        FlakeValue::String(s) => serde_json::Value::String(s.clone()),
        FlakeValue::Json(s) => serde_json::Value::String(s.clone()), // Serialize JSON as string
        FlakeValue::Vector(v) => serde_json::Value::Array(
            v.iter()
                .map(|f| {
                    serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        FlakeValue::BigInt(n) => serde_json::Value::String(n.to_string()),
        FlakeValue::Decimal(d) => serde_json::Value::String(d.to_string()),
        FlakeValue::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        FlakeValue::Date(d) => serde_json::Value::String(d.to_string()),
        FlakeValue::Time(t) => serde_json::Value::String(t.to_string()),
    }
}

/// Build SID dictionary from flakes for v2 format serialization
///
/// Returns (dictionary Vec, SID->index mapping)
/// Dictionary is sorted by SID comparison for determinism.
fn build_sid_dictionary(flakes: &[Flake]) -> (Vec<Sid>, HashMap<Sid, usize>) {
    use std::collections::BTreeSet;

    // Collect all unique SIDs
    let mut sids: BTreeSet<Sid> = BTreeSet::new();
    for flake in flakes {
        sids.insert(flake.s.clone());
        sids.insert(flake.p.clone());
        sids.insert(flake.dt.clone());
        if let FlakeValue::Ref(ref_sid) = &flake.o {
            sids.insert(ref_sid.clone());
        }
    }

    // Build dictionary and index mapping
    let dict: Vec<Sid> = sids.into_iter().collect();
    let sid_to_idx: HashMap<Sid, usize> = dict
        .iter()
        .enumerate()
        .map(|(i, sid)| (sid.clone(), i))
        .collect();

    (dict, sid_to_idx)
}

/// Serializable index root for ChildRef
#[derive(Serialize)]
struct SerializableIndexRoot<'a> {
    id: &'a str,
    leaf: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    first: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rhs: Option<serde_json::Value>,
    size: u64,
    /// Serialized byte size (omitted for backward compatibility with old indexes)
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<u64>,
    #[serde(rename = "leftmost?")]
    leftmost: bool,
}

impl<'a> SerializableIndexRoot<'a> {
    fn from_child_ref(child: &'a ChildRef) -> Self {
        Self {
            id: &child.id,
            leaf: child.leaf,
            first: child.first.as_ref().map(serialize_flake_v1),
            rhs: child.rhs.as_ref().map(serialize_flake_v1),
            size: child.size,
            bytes: child.bytes,
            leftmost: child.leftmost,
        }
    }
}

/// Serialize a single flake in v1 format (inline SIDs)
fn serialize_flake_v1(flake: &Flake) -> serde_json::Value {
    serde_json::json!([
        serialize_sid(&flake.s),
        serialize_sid(&flake.p),
        serialize_object(&flake.o, &flake.dt),
        serialize_sid(&flake.dt),
        flake.t,
        flake.op,
        serialize_meta(&flake.m)
    ])
}

/// Serialize a single flake in v2 format (dictionary indices)
///
/// Returns an error if any SID is missing from the dictionary, which would
/// indicate a bug in dictionary building.
fn serialize_flake_v2(
    flake: &Flake,
    sid_to_idx: &HashMap<Sid, usize>,
) -> Result<serde_json::Value> {
    let s_idx = sid_to_idx
        .get(&flake.s)
        .copied()
        .ok_or_else(|| Error::other(format!("Missing subject SID in dictionary: {:?}", flake.s)))?;
    let p_idx = sid_to_idx.get(&flake.p).copied().ok_or_else(|| {
        Error::other(format!(
            "Missing predicate SID in dictionary: {:?}",
            flake.p
        ))
    })?;
    let dt_idx = sid_to_idx.get(&flake.dt).copied().ok_or_else(|| {
        Error::other(format!(
            "Missing datatype SID in dictionary: {:?}",
            flake.dt
        ))
    })?;

    // Object: if reference, use index; otherwise serialize value
    let o_value = if let FlakeValue::Ref(ref_sid) = &flake.o {
        let ref_idx = sid_to_idx.get(ref_sid).copied().ok_or_else(|| {
            Error::other(format!(
                "Missing reference SID in dictionary: {:?}",
                ref_sid
            ))
        })?;
        serde_json::Value::Number(ref_idx.into())
    } else {
        serialize_object(&flake.o, &flake.dt)
    };

    Ok(serde_json::json!([
        s_idx,
        p_idx,
        o_value,
        dt_idx,
        flake.t,
        flake.op,
        serialize_meta(&flake.m)
    ]))
}

// === Public API ===

/// Parse a leaf node from JSON bytes (uses SIMD-accelerated parsing)
///
/// Takes ownership of bytes to avoid copying - simd-json mutates in place.
pub fn parse_leaf_node(mut bytes: Vec<u8>) -> Result<Vec<Flake>> {
    let raw: RawLeafNode = simd_json::from_slice(&mut bytes)?;
    raw.to_flakes()
}

fn skip_node<'de>(de: &mut Deserializer<'de>, node: Node<'de>) -> Result<()> {
    match node {
        Node::Static(_) | Node::String(_) => Ok(()),
        Node::Array { len, .. } => {
            for _ in 0..len {
                let n = unsafe { de.next_() };
                skip_node(de, n)?;
            }
            Ok(())
        }
        Node::Object { len, .. } => {
            for _ in 0..len {
                // key
                let k = unsafe { de.next_() };
                skip_node(de, k)?;
                // value
                let v = unsafe { de.next_() };
                skip_node(de, v)?;
            }
            Ok(())
        }
    }
}

fn node_i64(node: Node<'_>) -> Result<i64> {
    match node {
        Node::Static(StaticNode::I64(n)) => Ok(n),
        Node::Static(StaticNode::U64(n)) => Ok(n as i64),
        _ => Err(Error::other("expected integer")),
    }
}

fn node_u64(node: Node<'_>) -> Result<u64> {
    match node {
        Node::Static(StaticNode::U64(n)) => Ok(n),
        Node::Static(StaticNode::I64(n)) if n >= 0 => Ok(n as u64),
        _ => Err(Error::other("expected non-negative integer")),
    }
}

fn node_bool(node: Node<'_>) -> Result<bool> {
    match node {
        Node::Static(StaticNode::Bool(b)) => Ok(b),
        _ => Err(Error::other("expected boolean")),
    }
}

fn parse_sid_inline<'de>(
    de: &mut Deserializer<'de>,
    interner: &crate::SidInterner,
    first: Node<'de>,
) -> Result<Sid> {
    match first {
        Node::Array { len: 2, .. } => {
            let ns = node_i64(unsafe { de.next_() })? as i32;
            match unsafe { de.next_() } {
                Node::String(name) => Ok(interner.intern(ns, name)),
                other => Err(Error::other(format!(
                    "SID name must be string, got {:?}",
                    other
                ))),
            }
        }
        _ => Err(Error::other("SID must be [namespace_code, name] array")),
    }
}

enum RawO {
    Null,
    Bool(bool),
    Long(i64),
    Double(f64),
    String(String),
    Sid(Sid),
}

fn parse_raw_o<'de>(
    de: &mut Deserializer<'de>,
    interner: &crate::SidInterner,
    first: Node<'de>,
) -> Result<RawO> {
    match first {
        Node::Static(StaticNode::Null) => Ok(RawO::Null),
        Node::Static(StaticNode::Bool(b)) => Ok(RawO::Bool(b)),
        Node::Static(StaticNode::I64(n)) => Ok(RawO::Long(n)),
        Node::Static(StaticNode::U64(n)) => Ok(RawO::Long(n as i64)),
        Node::Static(StaticNode::F64(f)) => Ok(RawO::Double(f)),
        Node::String(s) => Ok(RawO::String(s.to_string())),
        Node::Array { len: 2, .. } => {
            // Backwards-compat: treat 2-tuple [ns, name] as a SID reference.
            let ns = node_i64(unsafe { de.next_() })? as i32;
            match unsafe { de.next_() } {
                Node::String(name) => Ok(RawO::Sid(interner.intern(ns, name))),
                other => Err(Error::other(format!(
                    "SID name must be string, got {:?}",
                    other
                ))),
            }
        }
        // Complex objects/arrays are rare in our leaf encoding; fall back to the DOM parser.
        other => Err(Error::other(format!(
            "unsupported object value in streaming leaf parser: {:?}",
            other
        ))),
    }
}

fn parse_meta_inline<'de>(
    de: &mut Deserializer<'de>,
    first: Node<'de>,
) -> Result<Option<FlakeMeta>> {
    match first {
        Node::Static(StaticNode::Null) => Ok(None),
        Node::Static(StaticNode::I64(n)) => Ok(Some(FlakeMeta {
            lang: None,
            i: Some(n as i32),
        })),
        Node::Static(StaticNode::U64(n)) => Ok(Some(FlakeMeta {
            lang: None,
            i: Some(n as i32),
        })),
        Node::Object { len, .. } => {
            let mut lang: Option<String> = None;
            let mut i: Option<i32> = None;
            for _ in 0..len {
                let k = unsafe { de.next_() };
                let key = match k {
                    Node::String(s) => s,
                    other => {
                        // Skip invalid keys
                        skip_node(de, other)?;
                        let v = unsafe { de.next_() };
                        skip_node(de, v)?;
                        continue;
                    }
                };
                let v = unsafe { de.next_() };
                match (key, v) {
                    ("lang", Node::String(s)) => lang = Some(s.to_string()),
                    ("i", n) => {
                        if let Ok(x) = node_i64(n) {
                            i = Some(x as i32);
                        }
                    }
                    (_, other) => {
                        skip_node(de, other)?;
                    }
                }
            }
            if lang.is_none() && i.is_none() {
                Ok(None)
            } else {
                Ok(Some(FlakeMeta { lang, i }))
            }
        }
        other => {
            // Unknown meta shape; skip it and treat as None
            skip_node(de, other)?;
            Ok(None)
        }
    }
}

fn parse_leaf_node_interned_streaming<'de>(
    bytes: &'de mut [u8],
    buffers: &mut simd_json::Buffers,
    interner: &crate::SidInterner,
) -> Result<Vec<Flake>> {
    let mut de = Deserializer::from_slice_with_buffers(bytes, buffers)?;
    let root = unsafe { de.next_() };

    let Node::Object { len: obj_len, .. } = root else {
        return Err(Error::other("leaf node must be a JSON object"));
    };

    let mut version: i64 = 1;
    let mut dict: Option<Vec<Sid>> = None;
    let mut flakes_node: Option<Node<'de>> = None;

    // Read leaf object keys/values; capture flakes node and parse dict if present.
    for _ in 0..obj_len {
        let k = unsafe { de.next_() };
        let key = match k {
            Node::String(s) => s,
            other => {
                // Skip invalid key and its value.
                let v = unsafe { de.next_() };
                skip_node(&mut de, other)?;
                skip_node(&mut de, v)?;
                continue;
            }
        };

        let v = unsafe { de.next_() };
        match key {
            "version" | "v" => {
                // version is a number; treat failures as v1
                if let Ok(vn) = node_i64(v) {
                    version = vn;
                }
            }
            "dict" => {
                let Node::Array { len, .. } = v else {
                    return Err(Error::other("v2 dict must be an array"));
                };
                let mut sid_dict: Vec<Sid> = Vec::with_capacity(len);
                for _ in 0..len {
                    let sid_node = unsafe { de.next_() };
                    let sid = parse_sid_inline(&mut de, interner, sid_node)?;
                    sid_dict.push(sid);
                }
                // If a dict is present, treat as v2 regardless of key ordering.
                version = 2;
                dict = Some(sid_dict);
            }
            "flakes" => {
                flakes_node = Some(v);
            }
            _ => {
                skip_node(&mut de, v)?;
            }
        }
    }

    let flakes_node = flakes_node.ok_or_else(|| Error::other("leaf node missing flakes"))?;
    let Node::Array {
        len: flakes_len, ..
    } = flakes_node
    else {
        return Err(Error::other("leaf flakes must be an array"));
    };

    let mut out: Vec<Flake> = Vec::with_capacity(flakes_len);

    if version == 2 {
        let sid_dict =
            dict.ok_or_else(|| Error::other("v2 leaf missing dict (flakes came before dict?)"))?;
        for _ in 0..flakes_len {
            let row_node = unsafe { de.next_() };
            let Node::Array { len, .. } = row_node else {
                return Err(Error::other("Dict flake must be an array"));
            };
            if len != 7 {
                return Err(Error::other("Dict flake must have 7 elements"));
            }

            let s_idx = node_u64(unsafe { de.next_() })? as usize;
            let p_idx = node_u64(unsafe { de.next_() })? as usize;
            let o_node = unsafe { de.next_() };
            let dt_idx = node_u64(unsafe { de.next_() })? as usize;

            let s = sid_dict
                .get(s_idx)
                .cloned()
                .ok_or_else(|| Error::other("Invalid s_idx"))?;
            let p = sid_dict
                .get(p_idx)
                .cloned()
                .ok_or_else(|| Error::other("Invalid p_idx"))?;
            let dt = sid_dict
                .get(dt_idx)
                .cloned()
                .ok_or_else(|| Error::other("Invalid dt_idx"))?;

            let o = if is_id_dt(&dt) {
                let o_idx = node_u64(o_node)? as usize;
                let o_sid = sid_dict
                    .get(o_idx)
                    .cloned()
                    .ok_or_else(|| Error::other("Invalid o_idx"))?;
                FlakeValue::Ref(o_sid)
            } else {
                // Only literals here; use the existing borrowed fallback via RawO parsing.
                let raw = parse_raw_o(&mut de, interner, o_node)?;
                match raw {
                    RawO::Null => FlakeValue::Null,
                    RawO::Bool(b) => FlakeValue::Boolean(b),
                    RawO::Long(n) => FlakeValue::Long(n),
                    RawO::Double(f) => FlakeValue::Double(f),
                    RawO::String(s) => FlakeValue::String(s),
                    RawO::Sid(sid) => FlakeValue::Ref(sid),
                }
            };

            let t = node_i64(unsafe { de.next_() })?;
            let op = node_bool(unsafe { de.next_() })?;
            let m_node = unsafe { de.next_() };
            let m = parse_meta_inline(&mut de, m_node)?;

            out.push(Flake::new(s, p, o, dt, t, op, m));
        }
    } else {
        // v1: inline SIDs
        for _ in 0..flakes_len {
            let row_node = unsafe { de.next_() };
            let Node::Array { len, .. } = row_node else {
                return Err(Error::other("Flake must be an array"));
            };
            if len != 7 {
                return Err(Error::other("Flake array must have 7 elements"));
            }

            let s_node = unsafe { de.next_() };
            let s = parse_sid_inline(&mut de, interner, s_node)?;
            let p_node = unsafe { de.next_() };
            let p = parse_sid_inline(&mut de, interner, p_node)?;
            let o_node = unsafe { de.next_() };
            let o_raw = parse_raw_o(&mut de, interner, o_node)?;
            let dt_node = unsafe { de.next_() };
            let dt = parse_sid_inline(&mut de, interner, dt_node)?;

            let o = if is_id_dt(&dt) {
                match o_raw {
                    RawO::Sid(sid) => FlakeValue::Ref(sid),
                    _ => return Err(Error::other("reference object must be a SID")),
                }
            } else {
                match o_raw {
                    RawO::Null => FlakeValue::Null,
                    RawO::Bool(b) => FlakeValue::Boolean(b),
                    RawO::Long(n) => FlakeValue::Long(n),
                    RawO::Double(f) => FlakeValue::Double(f),
                    RawO::String(s) => FlakeValue::String(s),
                    RawO::Sid(sid) => FlakeValue::Ref(sid),
                }
            };

            let t = node_i64(unsafe { de.next_() })?;
            let op = node_bool(unsafe { de.next_() })?;
            let m_node = unsafe { de.next_() };
            let m = parse_meta_inline(&mut de, m_node)?;

            out.push(Flake::new(s, p, o, dt, t, op, m));
        }
    }

    Ok(out)
}

/// Parse a leaf node from JSON bytes with SID interning.
///
/// This is the optimized path that avoids redundant allocations:
/// 1. For v2 format: parses the dictionary and interns SIDs first,
///    then builds flakes using the already-interned SIDs.
/// 2. For v1 format: interns each SID as it's parsed.
///
/// Takes ownership of bytes to avoid copying - simd-json mutates in place.
/// Uses SIMD-accelerated JSON parsing for fast cold query performance.
pub fn parse_leaf_node_interned(
    mut bytes: Vec<u8>,
    interner: &crate::SidInterner,
) -> Result<Vec<Flake>> {
    // Fast path: streaming parse using simd-json's `Deserializer` (avoids building a DOM),
    // which is especially beneficial when leaf flakes arrays are very large.
    let streaming = SIMDJSON_BUFFERS.with(|b| {
        let mut b = b.borrow_mut();
        parse_leaf_node_interned_streaming(bytes.as_mut_slice(), &mut b, interner)
    });

    match streaming {
        Ok(flakes) => Ok(flakes),
        Err(_) => {
            // Fallback: BorrowedValue-based parser handles more exotic shapes
            // (complex object values, unexpected orderings, etc).
            let root: simd_json::BorrowedValue<'_> = SIMDJSON_BUFFERS.with(|b| {
                let mut b = b.borrow_mut();
                simd_json::to_borrowed_value_with_buffers(bytes.as_mut_slice(), &mut b)
            })?;
            parse_leaf_node_interned_borrowed(&root, interner)
        }
    }
}

/// Parse a branch node from JSON bytes (uses SIMD-accelerated parsing)
///
/// Takes ownership of bytes to avoid copying - simd-json mutates in place.
pub fn parse_branch_node(mut bytes: Vec<u8>) -> Result<Vec<ChildRef>> {
    let raw: RawBranchNode = simd_json::from_slice(&mut bytes)?;
    raw.to_children()
}

/// Parse a DB root from JSON bytes (uses SIMD-accelerated parsing)
///
/// Takes ownership of bytes to avoid copying - simd-json mutates in place.
pub fn parse_db_root(mut bytes: Vec<u8>) -> Result<DbRoot> {
    let raw: RawDbRoot = simd_json::from_slice(&mut bytes)?;
    raw.to_db_root()
}

/// Serialize a leaf node to JSON bytes (v2 dictionary format)
///
/// Uses the v2 dictionary format where SIDs are stored in a shared dictionary
/// and flakes reference indices into that dictionary. This is more compact
/// and matches the Clojure leaf format.
///
/// # Determinism
///
/// The SID dictionary is sorted by SID comparison for determinism.
///
/// **Important**: Flakes are serialized in the order provided. For deterministic
/// content-addressed output, callers must pre-sort flakes using the appropriate
/// index comparator (e.g., `IndexType::Spot.compare`) before calling this function.
/// The index tree builder (`fluree-db-indexer::node::build_tree`) handles this sorting.
pub fn serialize_leaf_node(flakes: &[Flake]) -> Result<Vec<u8>> {
    let (dict, sid_to_idx) = build_sid_dictionary(flakes);

    // Serialize dictionary
    let dict_json: Vec<serde_json::Value> = dict.iter().map(serialize_sid).collect();

    // Serialize flakes using indices - propagate any errors
    let flakes_json: Vec<serde_json::Value> = flakes
        .iter()
        .map(|f| serialize_flake_v2(f, &sid_to_idx))
        .collect::<Result<Vec<_>>>()?;

    let leaf = serde_json::json!({
        "version": 2,
        "dict": dict_json,
        "flakes": flakes_json
    });

    serde_json::to_vec(&leaf).map_err(Into::into)
}

/// Serialize a branch node to JSON bytes
///
/// Branch nodes contain child references with boundary flakes.
pub fn serialize_branch_node(children: &[ChildRef]) -> Result<Vec<u8>> {
    let children_json: Vec<serde_json::Value> = children
        .iter()
        .map(|c| {
            let root = SerializableIndexRoot::from_child_ref(c);
            serde_json::to_value(&root)
                .map_err(|e| Error::other(format!("Failed to serialize child ref: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    let branch = serde_json::json!({
        "children": children_json
    });

    serde_json::to_vec(&branch).map_err(Into::into)
}

/// Serialize a DbRoot to JSON bytes (deterministic)
///
/// Uses sorted keys for namespace_codes to ensure deterministic output.
/// The namespace codes are written as string keys in NUMERIC sorted order
/// (not lexicographic - "1", "2", "10" not "1", "10", "2").
pub fn serialize_db_root(root: &DbRoot) -> Result<Vec<u8>> {
    // Sort namespace_codes by numeric key value (not lexicographic string order)
    let mut sorted_ns: Vec<_> = root.namespace_codes.iter().collect();
    sorted_ns.sort_by_key(|(k, _)| *k);

    // Build the root object with sorted keys
    let mut root_obj = serde_json::Map::new();

    root_obj.insert(
        "ledger-alias".to_string(),
        serde_json::Value::String(root.alias.clone()),
    );
    root_obj.insert("t".to_string(), serde_json::Value::Number(root.t.into()));
    root_obj.insert(
        "v".to_string(),
        serde_json::Value::Number(root.version.into()),
    );

    // Namespace codes with numeric-sorted string keys
    // serde_json::Map preserves insertion order, so we insert in sorted order
    let mut ns_obj = serde_json::Map::new();
    for (k, v) in sorted_ns {
        ns_obj.insert(k.to_string(), serde_json::Value::String(v.clone()));
    }
    root_obj.insert(
        "namespace-codes".to_string(),
        serde_json::Value::Object(ns_obj),
    );

    // Index roots
    if let Some(ref spot) = root.spot {
        let spot_root = SerializableIndexRoot::from_child_ref(spot);
        root_obj.insert("spot".to_string(), serde_json::to_value(&spot_root)?);
    }
    if let Some(ref psot) = root.psot {
        let psot_root = SerializableIndexRoot::from_child_ref(psot);
        root_obj.insert("psot".to_string(), serde_json::to_value(&psot_root)?);
    }
    if let Some(ref post) = root.post {
        let post_root = SerializableIndexRoot::from_child_ref(post);
        root_obj.insert("post".to_string(), serde_json::to_value(&post_root)?);
    }
    if let Some(ref opst) = root.opst {
        let opst_root = SerializableIndexRoot::from_child_ref(opst);
        root_obj.insert("opst".to_string(), serde_json::to_value(&opst_root)?);
    }
    if let Some(ref tspo) = root.tspo {
        let tspo_root = SerializableIndexRoot::from_child_ref(tspo);
        root_obj.insert("tspo".to_string(), serde_json::to_value(&tspo_root)?);
    }

    // Optional timestamp
    if let Some(ts) = root.timestamp {
        root_obj.insert(
            "timestamp".to_string(),
            serde_json::Value::Number(ts.into()),
        );
    }

    // Stats (flakes, size, properties) - sorted keys for determinism
    if let Some(ref stats) = root.stats {
        let mut stats_obj = serde_json::Map::new();
        stats_obj.insert(
            "flakes".to_string(),
            serde_json::Value::Number(stats.flakes.into()),
        );

        // Properties - compact array format for Clojure compatibility
        // Format: [[sid], [count, ndv_values, ndv_subjects, 0, 0, last_modified_t]]
        if let Some(ref properties) = stats.properties {
            let props_array: Vec<serde_json::Value> = properties
                .iter()
                .map(|p| {
                    serde_json::json!([
                        [p.sid.0, p.sid.1],
                        [
                            p.count,
                            p.ndv_values,
                            p.ndv_subjects,
                            0,
                            0,
                            p.last_modified_t
                        ]
                    ])
                })
                .collect();
            stats_obj.insert(
                "properties".to_string(),
                serde_json::Value::Array(props_array),
            );
        }

        // Classes - compact array format for Clojure compatibility
        // Format: [[class_sid], [count, [property_usages...]]]
        // Property usage format: [[property_sid], [[types], [ref_classes], [langs]]]
        if let Some(ref classes) = stats.classes {
            let classes_array: Vec<serde_json::Value> = classes
                .iter()
                .map(|c| {
                    let props_array: Vec<serde_json::Value> = c
                        .properties
                        .iter()
                        .map(|p| {
                            let types: Vec<serde_json::Value> = p
                                .types
                                .iter()
                                .map(|(sid, count)| {
                                    serde_json::json!([[sid.namespace_code, &*sid.name], count])
                                })
                                .collect();
                            let refs: Vec<serde_json::Value> = p
                                .ref_classes
                                .iter()
                                .map(|(sid, count)| {
                                    serde_json::json!([[sid.namespace_code, &*sid.name], count])
                                })
                                .collect();
                            let langs: Vec<serde_json::Value> = p
                                .langs
                                .iter()
                                .map(|(lang, count)| serde_json::json!([lang, count]))
                                .collect();
                            serde_json::json!([
                                [p.property_sid.namespace_code, &*p.property_sid.name],
                                [types, refs, langs]
                            ])
                        })
                        .collect();
                    serde_json::json!([
                        [c.class_sid.namespace_code, &*c.class_sid.name],
                        [c.count, props_array]
                    ])
                })
                .collect();
            stats_obj.insert(
                "classes".to_string(),
                serde_json::Value::Array(classes_array),
            );
        }

        stats_obj.insert(
            "size".to_string(),
            serde_json::Value::Number(stats.size.into()),
        );
        root_obj.insert("stats".to_string(), serde_json::Value::Object(stats_obj));
    }

    // Config - sorted keys for determinism (kebab-case to match Clojure)
    if let Some(ref config) = root.config {
        let mut config_obj = serde_json::Map::new();
        // Only include fields that are present, in sorted order
        if let Some(max_old) = config.max_old_indexes {
            config_obj.insert(
                "max-old-indexes".to_string(),
                serde_json::Value::Number(max_old.into()),
            );
        }
        if let Some(min_time) = config.min_time_garbage_mins {
            config_obj.insert(
                "min-time-garbage-mins".to_string(),
                serde_json::Value::Number(min_time.into()),
            );
        }
        if let Some(max_bytes) = config.reindex_max_bytes {
            config_obj.insert(
                "reindex-max-bytes".to_string(),
                serde_json::Value::Number(max_bytes.into()),
            );
        }
        if let Some(min_bytes) = config.reindex_min_bytes {
            config_obj.insert(
                "reindex-min-bytes".to_string(),
                serde_json::Value::Number(min_bytes.into()),
            );
        }
        if !config_obj.is_empty() {
            root_obj.insert("config".to_string(), serde_json::Value::Object(config_obj));
        }
    }

    // Previous index reference - fixed field order (t, then address) for determinism
    if let Some(ref prev) = root.prev_index {
        let mut prev_obj = serde_json::Map::new();
        prev_obj.insert("t".to_string(), serde_json::Value::Number(prev.t.into()));
        prev_obj.insert(
            "address".to_string(),
            serde_json::Value::String(prev.address.clone()),
        );
        root_obj.insert(
            "prev-index".to_string(),
            serde_json::Value::Object(prev_obj),
        );
    }

    // Schema - class/property hierarchy for query optimization
    if let Some(ref schema) = root.schema {
        let mut schema_obj = serde_json::Map::new();
        schema_obj.insert("t".to_string(), serde_json::Value::Number(schema.t.into()));

        // Build pred object with keys and vals
        let mut pred_obj = serde_json::Map::new();
        // Force canonical keys for Clojure compatibility (ignore stored keys)
        pred_obj.insert(
            "keys".to_string(),
            serde_json::json!(["id", "subclassOf", "parentProps", "childProps"]),
        );

        // vals: each entry is [sid, [subclass_of], [parent_props], [child_props]]
        // Sorted by SID for determinism
        let mut sorted_vals: Vec<_> = schema.pred.vals.iter().collect();
        sorted_vals.sort_by(|a, b| a.id.cmp(&b.id));

        let vals_array: Vec<serde_json::Value> = sorted_vals
            .iter()
            .map(|entry| {
                // Sort inner SID vectors for determinism
                let mut subclass_sorted: Vec<_> = entry.subclass_of.iter().collect();
                subclass_sorted.sort();
                let subclass_of: Vec<serde_json::Value> = subclass_sorted
                    .iter()
                    .map(|sid| serialize_sid(sid))
                    .collect();

                let mut parent_sorted: Vec<_> = entry.parent_props.iter().collect();
                parent_sorted.sort();
                let parent_props: Vec<serde_json::Value> =
                    parent_sorted.iter().map(|sid| serialize_sid(sid)).collect();

                let mut child_sorted: Vec<_> = entry.child_props.iter().collect();
                child_sorted.sort();
                let child_props: Vec<serde_json::Value> =
                    child_sorted.iter().map(|sid| serialize_sid(sid)).collect();

                serde_json::json!([
                    serialize_sid(&entry.id),
                    subclass_of,
                    parent_props,
                    child_props
                ])
            })
            .collect();
        pred_obj.insert("vals".to_string(), serde_json::Value::Array(vals_array));

        schema_obj.insert("pred".to_string(), serde_json::Value::Object(pred_obj));
        root_obj.insert("schema".to_string(), serde_json::Value::Object(schema_obj));
    }

    // Garbage reference - just the address
    if let Some(ref garbage) = root.garbage {
        let mut garbage_obj = serde_json::Map::new();
        garbage_obj.insert(
            "address".to_string(),
            serde_json::Value::String(garbage.address.clone()),
        );
        root_obj.insert(
            "garbage".to_string(),
            serde_json::Value::Object(garbage_obj),
        );
    }

    serde_json::to_vec(&serde_json::Value::Object(root_obj)).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sid() {
        let json = serde_json::json!([42, "example"]);
        let sid = deserialize_sid(&json).unwrap();
        assert_eq!(sid.namespace_code, 42);
        assert_eq!(sid.name.as_ref(), "example");
    }

    #[test]
    fn test_deserialize_flake() {
        let json = serde_json::json!([
            [1, "alice"],  // s
            [2, "name"],   // p
            "Alice",       // o
            [3, "string"], // dt
            100,           // t
            true,          // op
            null           // m
        ]);

        let raw: RawFlake = serde_json::from_value(json).unwrap();
        let flake = raw.to_flake().unwrap();

        assert_eq!(flake.s.namespace_code, 1);
        assert_eq!(flake.s.name.as_ref(), "alice");
        assert_eq!(flake.p.name.as_ref(), "name");
        assert!(matches!(flake.o, FlakeValue::String(ref s) if s == "Alice"));
        assert_eq!(flake.t, 100);
        assert!(flake.op);
        assert!(flake.m.is_none());
    }

    #[test]
    fn test_deserialize_ref_flake() {
        let json = serde_json::json!([
            [1, "alice"], // s
            [2, "knows"], // p
            [1, "bob"],   // o (reference)
            [1, "id"],    // dt = $id
            100,          // t
            true,         // op
            null          // m
        ]);

        let raw: RawFlake = serde_json::from_value(json).unwrap();
        let flake = raw.to_flake().unwrap();

        assert!(flake.is_ref());
        match &flake.o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 1);
                assert_eq!(sid.name.as_ref(), "bob");
            }
            _ => panic!("Expected Ref"),
        }
    }

    #[test]
    fn test_deserialize_leaf_node() {
        let json = r#"{
            "flakes": [
                [[1, "a"], [2, "p"], "value1", [3, "string"], 1, true, null],
                [[1, "b"], [2, "p"], "value2", [3, "string"], 2, true, null]
            ]
        }"#;

        let flakes = parse_leaf_node(json.as_bytes().to_vec()).unwrap();
        assert_eq!(flakes.len(), 2);
        assert_eq!(flakes[0].s.name.as_ref(), "a");
        assert_eq!(flakes[1].s.name.as_ref(), "b");
    }

    #[test]
    fn test_deserialize_leaf_node_v2_dictionary_format() {
        // V2 format uses a shared dictionary of SIDs
        // Flakes reference dictionary indices instead of inline SIDs
        let json = r#"{
            "version": 2,
            "dict": [
                [1, "alice"],
                [1, "bob"],
                [2, "name"],
                [2, "knows"],
                [3, "string"],
                [1, "id"]
            ],
            "flakes": [
                [0, 2, "Alice", 4, 100, true, null],
                [1, 2, "Bob", 4, 101, true, null],
                [0, 3, 1, 5, 102, true, null]
            ]
        }"#;
        // Dict indices: 0=alice, 1=bob, 2=name, 3=knows, 4=string, 5=$id
        // Flake 0: alice name "Alice" string t=100
        // Flake 1: bob name "Bob" string t=101
        // Flake 2: alice knows bob (ref) t=102

        let flakes = parse_leaf_node(json.as_bytes().to_vec()).unwrap();
        assert_eq!(flakes.len(), 3);

        // First flake: alice name "Alice"
        assert_eq!(flakes[0].s.namespace_code, 1);
        assert_eq!(flakes[0].s.name.as_ref(), "alice");
        assert_eq!(flakes[0].p.name.as_ref(), "name");
        assert!(matches!(&flakes[0].o, FlakeValue::String(s) if s == "Alice"));
        assert_eq!(flakes[0].t, 100);

        // Second flake: bob name "Bob"
        assert_eq!(flakes[1].s.name.as_ref(), "bob");
        assert!(matches!(&flakes[1].o, FlakeValue::String(s) if s == "Bob"));
        assert_eq!(flakes[1].t, 101);

        // Third flake: alice knows bob (reference)
        assert_eq!(flakes[2].s.name.as_ref(), "alice");
        assert_eq!(flakes[2].p.name.as_ref(), "knows");
        assert!(flakes[2].is_ref());
        if let FlakeValue::Ref(ref_sid) = &flakes[2].o {
            assert_eq!(ref_sid.name.as_ref(), "bob");
        } else {
            panic!("Expected reference flake");
        }
        assert_eq!(flakes[2].t, 102);
    }

    #[test]
    fn test_deserialize_branch_node() {
        let json = r#"{
            "children": [
                {
                    "id": "child1",
                    "leaf": true,
                    "first": [[1, "a"], [2, "p"], "v", [3, "s"], 1, true, null],
                    "size": 100,
                    "leftmost?": true
                },
                {
                    "id": "child2",
                    "leaf": false,
                    "size": 200
                }
            ]
        }"#;

        let children = parse_branch_node(json.as_bytes().to_vec()).unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].id, "child1");
        assert!(children[0].leaf);
        assert!(children[0].leftmost);
        assert_eq!(children[1].id, "child2");
        assert!(!children[1].leaf);
    }

    #[test]
    fn test_deserialize_db_root() {
        let json = r#"{
            "ledger-alias": "test/main",
            "t": 100,
            "v": 2,
            "namespace-codes": {
                "0": "",
                "1": "@",
                "2": "http://www.w3.org/2001/XMLSchema#"
            },
            "spot": {
                "id": "spot-root-id",
                "leaf": false,
                "size": 1000
            }
        }"#;

        let root = parse_db_root(json.as_bytes().to_vec()).unwrap();
        assert_eq!(root.alias, "test/main");
        assert_eq!(root.t, 100);
        assert_eq!(root.version, 2);
        assert_eq!(
            root.namespace_codes.get(&2),
            Some(&"http://www.w3.org/2001/XMLSchema#".to_string())
        );
        assert!(root.spot.is_some());
        assert_eq!(root.spot.as_ref().unwrap().id, "spot-root-id");
    }

    // === Serialization round-trip tests ===

    #[test]
    fn test_serialize_leaf_node_roundtrip() {
        use crate::flake::Flake;
        use crate::sid::Sid;
        use crate::value::FlakeValue;

        // Create test flakes
        let flakes = vec![
            Flake::new(
                Sid::new(1, "alice"),
                Sid::new(2, "name"),
                FlakeValue::String("Alice".to_string()),
                Sid::new(3, "string"),
                100,
                true,
                None,
            ),
            Flake::new(
                Sid::new(1, "bob"),
                Sid::new(2, "name"),
                FlakeValue::String("Bob".to_string()),
                Sid::new(3, "string"),
                101,
                true,
                None,
            ),
            Flake::new(
                Sid::new(1, "alice"),
                Sid::new(2, "knows"),
                FlakeValue::Ref(Sid::new(1, "bob")),
                Sid::new(1, "id"),
                102,
                true,
                None,
            ),
        ];

        // Serialize
        let bytes = serialize_leaf_node(&flakes).unwrap();

        // Verify it's valid JSON
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["version"], 2);
        assert!(json["dict"].is_array());
        assert!(json["flakes"].is_array());

        // Parse back
        let parsed = parse_leaf_node(bytes.clone()).unwrap();
        assert_eq!(parsed.len(), flakes.len());

        // Verify content (note: SID order in dictionary may differ, but values match)
        assert_eq!(parsed[0].s.name.as_ref(), "alice");
        assert_eq!(parsed[0].p.name.as_ref(), "name");
        assert!(matches!(&parsed[0].o, FlakeValue::String(s) if s == "Alice"));
        assert_eq!(parsed[0].t, 100);

        assert_eq!(parsed[1].s.name.as_ref(), "bob");
        assert!(matches!(&parsed[1].o, FlakeValue::String(s) if s == "Bob"));

        assert_eq!(parsed[2].s.name.as_ref(), "alice");
        assert_eq!(parsed[2].p.name.as_ref(), "knows");
        assert!(parsed[2].is_ref());
        if let FlakeValue::Ref(ref_sid) = &parsed[2].o {
            assert_eq!(ref_sid.name.as_ref(), "bob");
        }
    }

    #[test]
    fn test_serialize_leaf_node_with_various_types() {
        use crate::flake::Flake;
        use crate::sid::Sid;
        use crate::value::FlakeValue;

        let flakes = vec![
            // String value
            Flake::new(
                Sid::new(1, "s1"),
                Sid::new(2, "p1"),
                FlakeValue::String("text".to_string()),
                Sid::new(3, "string"),
                1,
                true,
                None,
            ),
            // Long value
            Flake::new(
                Sid::new(1, "s1"),
                Sid::new(2, "age"),
                FlakeValue::Long(42),
                Sid::new(3, "long"),
                2,
                true,
                None,
            ),
            // Double value
            Flake::new(
                Sid::new(1, "s1"),
                Sid::new(2, "score"),
                FlakeValue::Double(3.14),
                Sid::new(3, "double"),
                3,
                true,
                None,
            ),
            // Boolean value
            Flake::new(
                Sid::new(1, "s1"),
                Sid::new(2, "active"),
                FlakeValue::Boolean(true),
                Sid::new(3, "boolean"),
                4,
                true,
                None,
            ),
            // Retraction
            Flake::new(
                Sid::new(1, "s1"),
                Sid::new(2, "p1"),
                FlakeValue::String("text".to_string()),
                Sid::new(3, "string"),
                5,
                false, // retraction
                None,
            ),
        ];

        let bytes = serialize_leaf_node(&flakes).unwrap();
        let parsed = parse_leaf_node(bytes.clone()).unwrap();

        assert_eq!(parsed.len(), 5);
        assert!(matches!(&parsed[0].o, FlakeValue::String(_)));
        assert!(matches!(&parsed[1].o, FlakeValue::Long(42)));
        assert!(matches!(&parsed[2].o, FlakeValue::Double(f) if (*f - 3.14).abs() < 0.0001));
        assert!(matches!(&parsed[3].o, FlakeValue::Boolean(true)));
        assert!(!parsed[4].op); // retraction
    }

    #[test]
    fn test_serialize_branch_node_roundtrip() {
        use crate::flake::Flake;
        use crate::index::ChildRef;
        use crate::sid::Sid;
        use crate::value::FlakeValue;

        let children = vec![
            ChildRef {
                id: "child1".to_string(),
                leaf: true,
                first: Some(Flake::new(
                    Sid::new(1, "a"),
                    Sid::new(2, "p"),
                    FlakeValue::String("v".to_string()),
                    Sid::new(3, "string"),
                    1,
                    true,
                    None,
                )),
                rhs: None,
                size: 100,
                bytes: Some(500),
                leftmost: true,
            },
            ChildRef {
                id: "child2".to_string(),
                leaf: false,
                first: None,
                rhs: None,
                size: 200,
                bytes: None,
                leftmost: false,
            },
        ];

        let bytes = serialize_branch_node(&children).unwrap();
        let parsed = parse_branch_node(bytes.clone()).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, "child1");
        assert!(parsed[0].leaf);
        assert!(parsed[0].leftmost);
        assert_eq!(parsed[0].size, 100);
        assert!(parsed[0].first.is_some());

        assert_eq!(parsed[1].id, "child2");
        assert!(!parsed[1].leaf);
        assert_eq!(parsed[1].size, 200);
    }

    #[test]
    fn test_serialize_db_root_roundtrip() {
        use crate::index::ChildRef;
        use std::collections::HashMap;

        let mut namespace_codes = HashMap::new();
        namespace_codes.insert(0, "".to_string());
        namespace_codes.insert(1, "@".to_string());
        namespace_codes.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        namespace_codes.insert(100, "http://example.org/".to_string());

        let root = DbRoot {
            alias: "test/main".to_string(),
            t: 42,
            version: 2,
            namespace_codes,
            spot: Some(ChildRef {
                id: "spot-root".to_string(),
                leaf: false,
                first: None,
                rhs: None,
                size: 1000,
                bytes: Some(2000),
                leftmost: true,
            }),
            psot: Some(ChildRef {
                id: "psot-root".to_string(),
                leaf: false,
                first: None,
                rhs: None,
                size: 1000,
                bytes: None,
                leftmost: true,
            }),
            post: None,
            opst: None,
            tspo: None,
            timestamp: Some(1234567890),
            stats: Some(DbRootStats {
                flakes: 5000,
                size: 123456,
                properties: None,
                classes: None,
            }),
            config: Some(DbRootConfig {
                reindex_min_bytes: Some(1000000),
                reindex_max_bytes: Some(10000000),
                max_old_indexes: Some(5),
                min_time_garbage_mins: Some(30),
            }),
            prev_index: Some(PrevIndexRef {
                t: 41,
                address: "fluree:file://test/index/root/prev.json".to_string(),
            }),
            schema: Some(DbRootSchema {
                t: 42,
                pred: SchemaPredicates {
                    keys: vec![
                        "id".to_string(),
                        "subclassOf".to_string(),
                        "parentProps".to_string(),
                        "childProps".to_string(),
                    ],
                    vals: vec![
                        SchemaPredicateInfo {
                            id: Sid::new(100, "Person"),
                            subclass_of: vec![Sid::new(100, "Thing")],
                            parent_props: vec![],
                            child_props: vec![],
                        },
                        SchemaPredicateInfo {
                            id: Sid::new(100, "name"),
                            subclass_of: vec![],
                            parent_props: vec![],
                            child_props: vec![],
                        },
                    ],
                },
            }),
            garbage: Some(GarbageRef {
                address: "fluree:file://test/main/index/garbage/t42.json".to_string(),
            }),
        };

        let bytes = serialize_db_root(&root).unwrap();
        let parsed = parse_db_root(bytes.clone()).unwrap();

        assert_eq!(parsed.alias, "test/main");
        assert_eq!(parsed.t, 42);
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.namespace_codes.len(), 4);
        assert_eq!(
            parsed.namespace_codes.get(&100),
            Some(&"http://example.org/".to_string())
        );
        assert!(parsed.spot.is_some());
        assert_eq!(parsed.spot.as_ref().unwrap().id, "spot-root");
        assert!(parsed.psot.is_some());
        assert!(parsed.post.is_none());

        // Verify stats
        assert!(parsed.stats.is_some());
        let stats = parsed.stats.unwrap();
        assert_eq!(stats.flakes, 5000);
        assert_eq!(stats.size, 123456);

        // Verify config
        assert!(parsed.config.is_some());
        let config = parsed.config.unwrap();
        assert_eq!(config.reindex_min_bytes, Some(1000000));
        assert_eq!(config.reindex_max_bytes, Some(10000000));
        assert_eq!(config.max_old_indexes, Some(5));

        // Verify prev_index
        assert!(parsed.prev_index.is_some());
        let prev = parsed.prev_index.unwrap();
        assert_eq!(prev.t, 41);
        assert_eq!(prev.address, "fluree:file://test/index/root/prev.json");

        // Verify schema
        assert!(parsed.schema.is_some());
        let schema = parsed.schema.unwrap();
        assert_eq!(schema.t, 42);
        assert_eq!(schema.pred.keys.len(), 4);
        assert_eq!(schema.pred.keys[0], "id");
        assert_eq!(schema.pred.keys[1], "subclassOf");
        assert_eq!(schema.pred.vals.len(), 2);
        // Vals are sorted by SID, so "Person" comes before "name"
        assert_eq!(schema.pred.vals[0].id.name.as_ref(), "Person");
        assert_eq!(schema.pred.vals[0].subclass_of.len(), 1);
        assert_eq!(schema.pred.vals[0].subclass_of[0].name.as_ref(), "Thing");
        assert_eq!(schema.pred.vals[1].id.name.as_ref(), "name");

        // Verify garbage
        assert!(parsed.garbage.is_some());
        let garbage = parsed.garbage.unwrap();
        assert_eq!(
            garbage.address,
            "fluree:file://test/main/index/garbage/t42.json"
        );
    }

    #[test]
    fn test_serialize_db_root_deterministic() {
        use std::collections::HashMap;

        // Create two identical roots with namespace_codes inserted in different order
        let mut ns1 = HashMap::new();
        ns1.insert(100, "http://a.org/".to_string());
        ns1.insert(50, "http://b.org/".to_string());
        ns1.insert(200, "http://c.org/".to_string());

        let mut ns2 = HashMap::new();
        ns2.insert(200, "http://c.org/".to_string());
        ns2.insert(100, "http://a.org/".to_string());
        ns2.insert(50, "http://b.org/".to_string());

        let root1 = DbRoot {
            alias: "test".to_string(),
            t: 1,
            version: 2,
            namespace_codes: ns1,
            spot: None,
            psot: None,
            post: None,
            opst: None,
            tspo: None,
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root2 = DbRoot {
            alias: "test".to_string(),
            t: 1,
            version: 2,
            namespace_codes: ns2,
            spot: None,
            psot: None,
            post: None,
            opst: None,
            tspo: None,
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let bytes1 = serialize_db_root(&root1).unwrap();
        let bytes2 = serialize_db_root(&root2).unwrap();

        // Serialization should be identical regardless of insertion order
        assert_eq!(bytes1, bytes2, "Serialization should be deterministic");
    }

    #[test]
    fn test_serialize_db_root_numeric_key_ordering() {
        use std::collections::HashMap;

        // Use keys that would sort differently lexicographically vs numerically
        // Lexicographic: "1", "10", "2", "3"
        // Numeric:       1, 2, 3, 10
        let mut ns = HashMap::new();
        ns.insert(10, "http://ten.org/".to_string());
        ns.insert(1, "http://one.org/".to_string());
        ns.insert(3, "http://three.org/".to_string());
        ns.insert(2, "http://two.org/".to_string());

        let root = DbRoot {
            alias: "test".to_string(),
            t: 1,
            version: 2,
            namespace_codes: ns,
            spot: None,
            psot: None,
            post: None,
            opst: None,
            tspo: None,
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let bytes = serialize_db_root(&root).unwrap();
        let json_str = String::from_utf8(bytes).unwrap();

        // The keys should appear in numeric order: 1, 2, 3, 10
        // NOT lexicographic order: 1, 10, 2, 3
        let pos_1 = json_str.find("\"1\":").expect("key 1 not found");
        let pos_2 = json_str.find("\"2\":").expect("key 2 not found");
        let pos_3 = json_str.find("\"3\":").expect("key 3 not found");
        let pos_10 = json_str.find("\"10\":").expect("key 10 not found");

        assert!(
            pos_1 < pos_2 && pos_2 < pos_3 && pos_3 < pos_10,
            "Keys should be in numeric order (1, 2, 3, 10), not lexicographic (1, 10, 2, 3). JSON: {}",
            json_str
        );
    }
}
