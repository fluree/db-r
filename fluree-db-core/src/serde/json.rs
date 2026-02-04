//! JSON serialization and deserialization for Fluree index data

use crate::error::{Error, Result};
use crate::flake::{Flake, FlakeMeta};
use crate::sid::Sid;
use crate::temporal::{DateTime, Date, Time};
use crate::value::FlakeValue;
use bigdecimal::BigDecimal;
use fluree_vocab::xsd_names;
use num_bigint::BigInt;
use std::str::FromStr;
use serde::Deserialize;
use std::collections::HashMap;

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
                .map_err(|e| Error::other(e));
        }
    }
    if is_date_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Date::parse(s)
                .map(|d| FlakeValue::Date(Box::new(d)))
                .map_err(|e| Error::other(e));
        }
    }
    if is_time_dt(dt) {
        if let serde_json::Value::String(s) = value {
            return Time::parse(s)
                .map(|t| FlakeValue::Time(Box::new(t)))
                .map_err(|e| Error::other(e));
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

    /// Convert v2 dictionary format to flakes
    fn to_flakes_v2(&self) -> Result<Vec<Flake>> {
        let dict = self.dict.as_ref().ok_or_else(|| Error::other("v2 leaf missing dict"))?;

        // Build SID dictionary
        let sid_dict: Vec<Sid> = dict
            .iter()
            .map(deserialize_sid)
            .collect::<Result<_>>()?;

        // Deserialize flakes using dictionary indices
        self.flakes
            .iter()
            .map(|rf| {
                if rf.0.len() != 7 {
                    return Err(Error::other("Dict flake must have 7 elements"));
                }

                // s, p, dt are indices into dictionary
                let s_idx = rf.0[0].as_i64().ok_or_else(|| Error::other("s_idx must be integer"))? as usize;
                let p_idx = rf.0[1].as_i64().ok_or_else(|| Error::other("p_idx must be integer"))? as usize;
                let dt_idx = rf.0[3].as_i64().ok_or_else(|| Error::other("dt_idx must be integer"))? as usize;

                let s = sid_dict.get(s_idx).cloned().ok_or_else(|| Error::other(format!("Invalid s_idx: {}", s_idx)))?;
                let p = sid_dict.get(p_idx).cloned().ok_or_else(|| Error::other(format!("Invalid p_idx: {}", p_idx)))?;
                let dt = sid_dict.get(dt_idx).cloned().ok_or_else(|| Error::other(format!("Invalid dt_idx: {}", dt_idx)))?;

                // o: index if reference, otherwise literal
                let o = if dt.namespace_code == ID_NAMESPACE_CODE && dt.name.as_ref() == ID_NAME {
                    let o_idx = rf.0[2].as_i64().ok_or_else(|| Error::other("o_idx must be integer"))? as usize;
                    let o_sid = sid_dict.get(o_idx).cloned().ok_or_else(|| Error::other(format!("Invalid o_idx: {}", o_idx)))?;
                    FlakeValue::Ref(o_sid)
                } else {
                    deserialize_object(&rf.0[2], &dt)?
                };

                let t = rf.0[4].as_i64().ok_or_else(|| Error::other("t must be integer"))?;
                let op = rf.0[5].as_bool().ok_or_else(|| Error::other("op must be boolean"))?;
                let m = deserialize_meta(&rf.0[6])?;

                Ok(Flake::new(s, p, o, dt, t, op, m))
            })
            .collect()
    }

}

// === DB Root Stats ===
//
// Canonical types moved to `crate::index_stats`. Re-exported here for
// backward compatibility with existing `serde::json::DbRootStats` imports.

use crate::index_stats::{
    IndexStats, PropertyStatEntry, ClassStatEntry, ClassPropertyUsage,
    GraphPropertyStatEntry, GraphStatsEntry,
};

/// Backward-compatible alias for [`IndexStats`].
pub type DbRootStats = IndexStats;

/// Raw property stat entry as it appears in JSON (v2 only).
///
/// Legacy compact “clojure parity” array formats are intentionally not supported.
#[derive(Debug, Deserialize)]
pub struct RawPropertyStatEntry {
    /// SID as (namespace_code, name)
    pub sid: (i32, String),
    pub count: u64,
    pub ndv_values: u64,
    pub ndv_subjects: u64,
    pub last_modified_t: i64,
    /// Optional per-datatype usage counts (DatatypeId.0, count).
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
                formatter.write_str("a class stat entry as [[ns_code, name], [count, [property_usages...]]]")
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
                    properties: if stats.1.is_empty() { None } else { Some(stats.1) },
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
    pub property_sid: (i32, String),
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
                formatter.write_str("a property usage as [[property_sid], [[types], [ref_classes], [langs]]]")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // First element: property SID as [namespace_code, name]
                let property_sid: (i32, String) = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                // Optional second element: legacy detailed structure. Ignore if present.
                let _ignored: Option<serde_json::Value> = seq.next_element()?;

                Ok(RawClassPropertyUsage {
                    property_sid,
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
//
// Canonical types moved to `crate::index_schema`. Re-exported here for
// backward compatibility with existing `serde::json::DbRootSchema` imports.

use crate::index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};

/// Backward-compatible alias for [`IndexSchema`].
pub type DbRootSchema = IndexSchema;

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
                                    ClassPropertyUsage { property_sid }
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
        FlakeValue::Double(f) => {
            serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
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
        FlakeValue::GYear(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::GYearMonth(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::GMonth(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::GDay(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::GMonthDay(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::YearMonthDuration(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::DayTimeDuration(v) => serde_json::Value::String(v.to_string()),
        FlakeValue::Duration(v) => serde_json::Value::String(v.to_string()),
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
    let sid_to_idx: HashMap<Sid, usize> = dict.iter()
        .enumerate()
        .map(|(i, sid)| (sid.clone(), i))
        .collect();

    (dict, sid_to_idx)
}

/// Serialize a single flake in v2 format (dictionary indices)
///
/// Returns an error if any SID is missing from the dictionary, which would
/// indicate a bug in dictionary building.
fn serialize_flake_v2(flake: &Flake, sid_to_idx: &HashMap<Sid, usize>) -> Result<serde_json::Value> {
    let s_idx = sid_to_idx.get(&flake.s).copied()
        .ok_or_else(|| Error::other(format!("Missing subject SID in dictionary: {:?}", flake.s)))?;
    let p_idx = sid_to_idx.get(&flake.p).copied()
        .ok_or_else(|| Error::other(format!("Missing predicate SID in dictionary: {:?}", flake.p)))?;
    let dt_idx = sid_to_idx.get(&flake.dt).copied()
        .ok_or_else(|| Error::other(format!("Missing datatype SID in dictionary: {:?}", flake.dt)))?;

    // Object: if reference, use index; otherwise serialize value
    let o_value = if let FlakeValue::Ref(ref_sid) = &flake.o {
        let ref_idx = sid_to_idx.get(ref_sid).copied()
            .ok_or_else(|| Error::other(format!("Missing reference SID in dictionary: {:?}", ref_sid)))?;
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
    let dict_json: Vec<serde_json::Value> = dict.iter()
        .map(serialize_sid)
        .collect();

    // Serialize flakes using indices - propagate any errors
    let flakes_json: Vec<serde_json::Value> = flakes.iter()
        .map(|f| serialize_flake_v2(f, &sid_to_idx))
        .collect::<Result<Vec<_>>>()?;

    let leaf = serde_json::json!({
        "version": 2,
        "dict": dict_json,
        "flakes": flakes_json
    });

    serde_json::to_vec(&leaf).map_err(Into::into)
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
            [1, "alice"],      // s
            [2, "name"],       // p
            "Alice",           // o
            [3, "string"],     // dt
            100,               // t
            true,              // op
            null               // m
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
            [1, "alice"],      // s
            [2, "knows"],      // p
            [1, "bob"],        // o (reference)
            [1, "id"],         // dt = $id
            100,               // t
            true,              // op
            null               // m
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
                false,  // retraction
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




}
