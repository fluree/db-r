//! JSON serialization and deserialization for Fluree index data

use crate::error::{Error, Result};
use crate::flake::{Flake, FlakeMeta};
use crate::sid::Sid;
use crate::temporal::{Date, DateTime, Time};
use crate::value::FlakeValue;
use bigdecimal::BigDecimal;
use fluree_vocab::namespaces::{JSON_LD, XSD};
use fluree_vocab::xsd_names;
use num_bigint::BigInt;
use serde::Deserialize;
use std::str::FromStr;

fn is_id_dt(dt: &Sid) -> bool {
    dt.namespace_code == JSON_LD && dt.name.as_ref() == "id"
}

fn is_datetime_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DATE_TIME
}

fn is_date_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DATE
}

fn is_time_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::TIME
}

fn is_integer_family_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && xsd_names::is_integer_family_name(dt.name.as_ref())
}

fn is_decimal_dt(dt: &Sid) -> bool {
    dt.namespace_code == XSD && dt.name.as_ref() == xsd_names::DECIMAL
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
}

/// Deserialize a SID from JSON
///
/// SIDs are serialized as `[namespace_code, name]` tuples.
pub fn deserialize_sid(value: &serde_json::Value) -> Result<Sid> {
    match value {
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let raw_code = arr[0]
                .as_u64()
                .ok_or_else(|| Error::other("SID namespace_code must be integer"))?;
            let ns_code = u16::try_from(raw_code).map_err(|_| {
                Error::other(format!("SID namespace_code {} exceeds u16::MAX", raw_code))
            })?;
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

// === Index Stats (JSON parsing) ===

use crate::index_stats::{
    ClassPropertyUsage, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry, IndexStats,
    PropertyStatEntry,
};

/// Raw property stat entry as it appears in JSON (v2 only).
///
/// Legacy compact “clojure parity” array formats are intentionally not supported.
#[derive(Debug, Deserialize)]
pub struct RawPropertyStatEntry {
    /// SID as (namespace_code, name)
    pub sid: (u16, String),
    pub count: u64,
    pub ndv_values: u64,
    pub ndv_subjects: u64,
    pub last_modified_t: i64,
    /// Optional per-datatype usage counts (ValueTypeTag.0, count).
    #[serde(default)]
    pub datatypes: Vec<(u8, u64)>,
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
    #[serde(default)]
    pub graphs: Option<Vec<RawGraphStatsEntry>>,
}

/// Raw per-property stats within a graph as it appears in JSON
///
/// JSON format: `{"p_id": u32, "count": u64, "ndv_values": u64, "ndv_subjects": u64,
///               "last_modified_t": i64, "datatypes": [[dt_id, count], ...]}`
#[derive(Debug, Deserialize)]
pub struct RawGraphPropertyStatEntry {
    pub p_id: u32,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub ndv_values: u64,
    #[serde(default)]
    pub ndv_subjects: u64,
    #[serde(default)]
    pub last_modified_t: i64,
    #[serde(default)]
    pub datatypes: Option<Vec<(u8, u64)>>,
}

/// Raw graph stats entry as it appears in JSON
///
/// JSON format: `{"g_id": u32, "flakes": u64, "size": u64, "properties": [...]}`
#[derive(Debug, Deserialize)]
pub struct RawGraphStatsEntry {
    pub g_id: u32,
    #[serde(default)]
    pub flakes: u64,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub properties: Option<Vec<RawGraphPropertyStatEntry>>,
}

/// Raw class stat entry as it appears in JSON
///
/// Clojure format: `[[namespace_code, "name"], [count, [property_usages...]]]`
#[derive(Debug)]
pub struct RawClassStatEntry {
    /// Class SID as (namespace_code, name)
    pub class_sid: (u16, String),
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
                let class_sid: (u16, String) = seq
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
/// New format: `[[property_sid]]`
///
/// We also accept the legacy Clojure format:
/// `[[property_sid], [[types], [ref_classes], [langs]]]` but we intentionally
/// ignore the detailed breakdowns.
#[derive(Debug)]
pub struct RawClassPropertyUsage {
    /// Property SID as (namespace_code, name)
    pub property_sid: (u16, String),
    /// Optional ref target classes (class SID -> count).
    ///
    /// This is emitted by DB-R when available. We also accept legacy Clojure
    /// formats where this data may appear nested in a different structure.
    pub ref_classes: Vec<((u16, String), u64)>,
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
                let property_sid: (u16, String) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                // Optional second element: may include ref-class counts.
                //
                // Accepted shapes:
                // - legacy Clojure: [[property_sid], [[types], [ref_classes], [langs]]]
                // - DB-R extended:  [[property_sid], {"ref-classes": [[[class_sid], count], ...]}]
                let details: Option<serde_json::Value> = seq.next_element()?;
                let mut ref_classes: Vec<((u16, String), u64)> = Vec::new();
                if let Some(v) = details {
                    // DB-R extended object
                    if let Some(obj) = v.as_object() {
                        if let Some(arr) = obj.get("ref-classes").and_then(|x| x.as_array()) {
                            for entry in arr {
                                let Some(pair) = entry.as_array() else {
                                    continue;
                                };
                                if pair.len() != 2 {
                                    continue;
                                }
                                let Some(class_sid_arr) = pair[0].as_array() else {
                                    continue;
                                };
                                if class_sid_arr.len() != 2 {
                                    continue;
                                }
                                let (Some(ns_u64), Some(name)) =
                                    (class_sid_arr[0].as_u64(), class_sid_arr[1].as_str())
                                else {
                                    continue;
                                };
                                let class_sid: (u16, String) = (ns_u64 as u16, name.to_string());
                                let count = pair[1].as_u64().unwrap_or(0);
                                if count > 0 {
                                    ref_classes.push((class_sid, count));
                                }
                            }
                        }
                    }
                    // Legacy nested arrays are intentionally not fully decoded here.
                }

                Ok(RawClassPropertyUsage {
                    property_sid,
                    ref_classes,
                })
            }
        }

        deserializer.deserialize_seq(PropertyUsageVisitor)
    }
}

// === Garbage Reference ===

/// Reference to garbage record for GC chain
///
/// Links a db-root / index-root to its garbage manifest file.
/// The garbage record contains addresses of obsolete nodes replaced during
/// this refresh operation.
#[derive(Debug, Clone, PartialEq)]
pub struct GarbageRef {
    /// Address of the garbage record file
    pub address: String,
    /// Content hash (SHA-256 hex) of the garbage record
    pub content_hash: String,
}

// === Index Schema (JSON parsing) ===

use crate::index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
use crate::index_stats::ClassRefCount;

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

// === Raw → Typed conversion helpers ===

/// Convert raw stats to IndexStats.
///
/// Returns None if no meaningful stat fields are present.
pub fn raw_stats_to_index_stats(s: &RawDbRootStats) -> Option<IndexStats> {
    if s.flakes.is_some()
        || s.size.is_some()
        || s.properties.is_some()
        || s.classes.is_some()
        || s.graphs.is_some()
    {
        let properties = s.properties.as_ref().map(|props| {
            props
                .iter()
                .map(|p| PropertyStatEntry {
                    sid: p.sid.clone(),
                    count: p.count,
                    ndv_values: p.ndv_values,
                    ndv_subjects: p.ndv_subjects,
                    last_modified_t: p.last_modified_t,
                    datatypes: p.datatypes.clone(),
                })
                .collect()
        });

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
                                    let mut ref_classes: Vec<ClassRefCount> = p
                                        .ref_classes
                                        .iter()
                                        .map(|(sid, count)| ClassRefCount {
                                            class_sid: Sid::new(sid.0, &sid.1),
                                            count: *count,
                                        })
                                        .collect();
                                    ref_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                                    ClassPropertyUsage {
                                        property_sid,
                                        ref_classes,
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

        let graphs = s.graphs.as_ref().map(|graph_list| {
            graph_list
                .iter()
                .map(|g| {
                    let properties = g
                        .properties
                        .as_ref()
                        .map(|props| {
                            props
                                .iter()
                                .map(|p| GraphPropertyStatEntry {
                                    p_id: p.p_id,
                                    count: p.count,
                                    ndv_values: p.ndv_values,
                                    ndv_subjects: p.ndv_subjects,
                                    last_modified_t: p.last_modified_t,
                                    datatypes: p.datatypes.clone().unwrap_or_default(),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    GraphStatsEntry {
                        g_id: g.g_id,
                        flakes: g.flakes,
                        size: g.size,
                        properties,
                    }
                })
                .collect()
        });

        Some(IndexStats {
            flakes: s.flakes.unwrap_or(0),
            size: s.size.unwrap_or(0),
            properties,
            classes,
            graphs,
        })
    } else {
        None
    }
}

/// Convert raw schema to IndexSchema.
pub fn raw_schema_to_index_schema(s: &RawDbRootSchema) -> IndexSchema {
    let pred = s
        .pred
        .as_ref()
        .map(|p| {
            let vals = p
                .vals
                .iter()
                .filter_map(|entry| {
                    if entry.len() < 4 {
                        return None;
                    }
                    let id = deserialize_sid(&entry[0]).ok()?;
                    let subclass_of = entry[1]
                        .as_array()
                        .map(|arr| arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect())
                        .unwrap_or_default();
                    let parent_props = entry[2]
                        .as_array()
                        .map(|arr| arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect())
                        .unwrap_or_default();
                    let child_props = entry[3]
                        .as_array()
                        .map(|arr| arr.iter().filter_map(|v| deserialize_sid(v).ok()).collect())
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
    IndexSchema { t: s.t, pred }
}

// NOTE: Legacy JSON leaf-node parsing/serialization was removed.
//
// The current index format stores leaf files as binary `FLI1` (leaflets inside),
// and proxy transport uses `fluree-db-core::serde::flakes_transport` (FLKB).

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
}
