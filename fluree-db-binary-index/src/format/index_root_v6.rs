//! V6 index root descriptor (`FIR6`) for the FLI3 columnar index format.
//!
//! Extends IndexRootV5 with:
//! - `o_type_table`: maps OType → decode kind, datatype IRI, dict/arena family
//! - Updated routing using V3 branch/leaf CIDs
//! - `language_tag_dict` and `custom_datatype_dict` are separate from the inline arrays
//!
//! For the import-only milestone, the root carries the minimal set of fields
//! needed to locate and decode V3 index artifacts. Additional sections
//! (stats, schema, prev_index, garbage) use the same types as V5.

use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use std::collections::BTreeMap;

use super::index_root::{BinaryGarbageRef, BinaryPrevIndexRef, DictRefsV5, GraphArenaRefsV5};
use super::run_record::RunSortOrder;
use fluree_db_core::index_schema::IndexSchema;
use fluree_db_core::index_stats::IndexStats;

// ============================================================================
// OType table entry
// ============================================================================

/// Entry in the `o_type` lookup table stored in the index root.
///
/// Maps an `OType` value to its decode routing information. For built-in
/// types this is derivable from the constant tables in `o_type.rs`, but
/// for customer-defined and dynamic types it must be looked up here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OTypeTableEntry {
    /// The `o_type` value (u16).
    pub o_type: u16,
    /// Decode routing kind.
    pub decode_kind: DecodeKind,
    /// Datatype IRI string (absent for refs and langString).
    pub datatype_iri: Option<String>,
    /// Which dictionary/arena family `o_key` indexes into.
    /// Absent for embedded types (o_key is the value itself).
    pub dict_family: Option<DictFamily>,
}

/// Dictionary/arena family that `o_key` indexes into for dict-backed types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DictFamily {
    /// String dictionary (xsd:string, xsd:anyURI, langString, customer types, etc.)
    StringDict = 0,
    /// Subject dictionary (IRI references)
    SubjectDict = 1,
    /// JSON arena (per-predicate)
    JsonArena = 2,
    /// Vector arena (per-predicate)
    VectorArena = 3,
    /// NumBig arena (per-predicate)
    NumBigArena = 4,
    /// Spatial arena (per-predicate)
    SpatialArena = 5,
}

impl DictFamily {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::StringDict),
            1 => Some(Self::SubjectDict),
            2 => Some(Self::JsonArena),
            3 => Some(Self::VectorArena),
            4 => Some(Self::NumBigArena),
            5 => Some(Self::SpatialArena),
            _ => None,
        }
    }
}

// ============================================================================
// V3 routing types
// ============================================================================

/// Named graph routing for V3: branch CIDs per sort order.
#[derive(Debug, Clone)]
pub struct NamedGraphRoutingV3 {
    pub g_id: GraphId,
    /// `(order, branch_cid)` pairs.
    pub orders: Vec<(RunSortOrder, ContentId)>,
}

// ============================================================================
// IndexRootV6
// ============================================================================

/// Magic bytes for the V6 index root.
pub const ROOT_V6_MAGIC: &[u8; 4] = b"FIR6";

/// V6 format version.
pub const ROOT_V6_VERSION: u8 = 1;

/// Binary index root v6 (`FIR6`).
///
/// Carries the V3 format-specific sections alongside the shared
/// infrastructure from V5 (dict refs, arena refs, stats, schema).
#[derive(Debug, Clone)]
pub struct IndexRootV6 {
    // ── Identity ───────────────────────────────────────────────────
    pub ledger_id: String,
    pub index_t: i64,
    pub base_t: i64,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,

    // ── Namespace / predicate metadata ─────────────────────────────
    pub namespace_codes: BTreeMap<u16, String>,
    pub predicate_sids: Vec<(u16, String)>,

    // ── Inline small dictionaries ──────────────────────────────────
    pub graph_iris: Vec<String>,
    /// Datatype IRIs: index 0..RESERVED_COUNT are well-known, ≥RESERVED_COUNT are custom.
    pub datatype_iris: Vec<String>,
    /// Language tags: `lang_id → BCP 47 tag string` (index = lang_id).
    pub language_tags: Vec<String>,

    // ── Dict refs (CID trees, shared with V5) ──────────────────────
    pub dict_refs: DictRefsV5,

    // ── Watermarks ─────────────────────────────────────────────────
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,

    // ── Cumulative commit stats ────────────────────────────────────
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub total_retracts: u64,

    // ── Per-graph specialty arenas (shared with V5) ────────────────
    pub graph_arenas: Vec<GraphArenaRefsV5>,

    // ── V3-specific: o_type table ──────────────────────────────────
    /// Maps OType values to decode kind, datatype IRI, and dict family.
    /// Built-in types are included for completeness; customer types
    /// and dynamic langString entries are required.
    pub o_type_table: Vec<OTypeTableEntry>,

    // ── V3-specific: graph routing using V3 branch CIDs ────────────
    /// Named graph routing: branch CIDs per sort order (FBR3 branches).
    pub named_graphs: Vec<NamedGraphRoutingV3>,

    // ── Optional sections (shared with V5) ─────────────────────────
    pub stats: Option<IndexStats>,
    pub schema: Option<IndexSchema>,
    pub prev_index: Option<BinaryPrevIndexRef>,
    pub garbage: Option<BinaryGarbageRef>,
    pub sketch_ref: Option<ContentId>,
}

impl IndexRootV6 {
    /// Build the `o_type_table` from custom datatype IRIs and language tags.
    ///
    /// Includes all built-in OType constants plus customer-defined and
    /// langString entries.
    ///
    /// - `custom_datatype_iris`: IRIs for non-reserved datatypes only
    ///   (DatatypeDictId values ≥ RESERVED_COUNT). Do NOT include the
    ///   15 reserved well-known types — those are hardcoded.
    /// - `language_tags`: BCP 47 tag strings, one per `lang_id` (index = lang_id).
    pub fn build_o_type_table(
        custom_datatype_iris: &[String],
        language_tags: &[String],
    ) -> Vec<OTypeTableEntry> {
        let mut table = Vec::new();

        // Built-in embedded types (tag 00).
        let embedded_types: &[(u16, DecodeKind, Option<&str>)] = &[
            (OType::NULL.as_u16(), DecodeKind::Null, None),
            (
                OType::XSD_BOOLEAN.as_u16(),
                DecodeKind::Bool,
                Some("xsd:boolean"),
            ),
            (
                OType::XSD_INTEGER.as_u16(),
                DecodeKind::I64,
                Some("xsd:integer"),
            ),
            (OType::XSD_LONG.as_u16(), DecodeKind::I64, Some("xsd:long")),
            (OType::XSD_INT.as_u16(), DecodeKind::I64, Some("xsd:int")),
            (
                OType::XSD_SHORT.as_u16(),
                DecodeKind::I64,
                Some("xsd:short"),
            ),
            (OType::XSD_BYTE.as_u16(), DecodeKind::I64, Some("xsd:byte")),
            (
                OType::XSD_UNSIGNED_LONG.as_u16(),
                DecodeKind::I64,
                Some("xsd:unsignedLong"),
            ),
            (
                OType::XSD_UNSIGNED_INT.as_u16(),
                DecodeKind::I64,
                Some("xsd:unsignedInt"),
            ),
            (
                OType::XSD_UNSIGNED_SHORT.as_u16(),
                DecodeKind::I64,
                Some("xsd:unsignedShort"),
            ),
            (
                OType::XSD_UNSIGNED_BYTE.as_u16(),
                DecodeKind::I64,
                Some("xsd:unsignedByte"),
            ),
            (
                OType::XSD_NON_NEGATIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some("xsd:nonNegativeInteger"),
            ),
            (
                OType::XSD_POSITIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some("xsd:positiveInteger"),
            ),
            (
                OType::XSD_NON_POSITIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some("xsd:nonPositiveInteger"),
            ),
            (
                OType::XSD_NEGATIVE_INTEGER.as_u16(),
                DecodeKind::I64,
                Some("xsd:negativeInteger"),
            ),
            (
                OType::XSD_DOUBLE.as_u16(),
                DecodeKind::F64,
                Some("xsd:double"),
            ),
            (
                OType::XSD_FLOAT.as_u16(),
                DecodeKind::F64,
                Some("xsd:float"),
            ),
            (
                OType::XSD_DECIMAL.as_u16(),
                DecodeKind::F64,
                Some("xsd:decimal"),
            ),
            (OType::XSD_DATE.as_u16(), DecodeKind::Date, Some("xsd:date")),
            (OType::XSD_TIME.as_u16(), DecodeKind::Time, Some("xsd:time")),
            (
                OType::XSD_DATE_TIME.as_u16(),
                DecodeKind::DateTime,
                Some("xsd:dateTime"),
            ),
            (
                OType::XSD_G_YEAR.as_u16(),
                DecodeKind::GYear,
                Some("xsd:gYear"),
            ),
            (
                OType::XSD_G_YEAR_MONTH.as_u16(),
                DecodeKind::GYearMonth,
                Some("xsd:gYearMonth"),
            ),
            (
                OType::XSD_G_MONTH.as_u16(),
                DecodeKind::GMonth,
                Some("xsd:gMonth"),
            ),
            (
                OType::XSD_G_DAY.as_u16(),
                DecodeKind::GDay,
                Some("xsd:gDay"),
            ),
            (
                OType::XSD_G_MONTH_DAY.as_u16(),
                DecodeKind::GMonthDay,
                Some("xsd:gMonthDay"),
            ),
            (
                OType::XSD_YEAR_MONTH_DURATION.as_u16(),
                DecodeKind::YearMonthDuration,
                Some("xsd:yearMonthDuration"),
            ),
            (
                OType::XSD_DAY_TIME_DURATION.as_u16(),
                DecodeKind::DayTimeDuration,
                Some("xsd:dayTimeDuration"),
            ),
            (
                OType::XSD_DURATION.as_u16(),
                DecodeKind::Duration,
                Some("xsd:duration"),
            ),
            (OType::GEO_POINT.as_u16(), DecodeKind::GeoPoint, None),
            (OType::BLANK_NODE.as_u16(), DecodeKind::BlankNode, None),
        ];

        for &(o_type, decode_kind, dt_iri) in embedded_types {
            table.push(OTypeTableEntry {
                o_type,
                decode_kind,
                datatype_iri: dt_iri.map(String::from),
                dict_family: None,
            });
        }

        // Fluree-reserved dict-backed types (tag 10).
        let fluree_types: &[(u16, DecodeKind, Option<&str>, DictFamily)] = &[
            (
                OType::XSD_STRING.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:string"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_ANY_URI.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:anyURI"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_NORMALIZED_STRING.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:normalizedString"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_TOKEN.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:token"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_LANGUAGE.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:language"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_BASE64_BINARY.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:base64Binary"),
                DictFamily::StringDict,
            ),
            (
                OType::XSD_HEX_BINARY.as_u16(),
                DecodeKind::StringDict,
                Some("xsd:hexBinary"),
                DictFamily::StringDict,
            ),
            (
                OType::IRI_REF.as_u16(),
                DecodeKind::IriRef,
                None,
                DictFamily::SubjectDict,
            ),
            (
                OType::RDF_JSON.as_u16(),
                DecodeKind::JsonArena,
                Some("rdf:JSON"),
                DictFamily::JsonArena,
            ),
            (
                OType::VECTOR.as_u16(),
                DecodeKind::VectorArena,
                None,
                DictFamily::VectorArena,
            ),
            (
                OType::FULLTEXT.as_u16(),
                DecodeKind::StringDict,
                None,
                DictFamily::StringDict,
            ),
            (
                OType::NUM_BIG_OVERFLOW.as_u16(),
                DecodeKind::NumBigArena,
                None,
                DictFamily::NumBigArena,
            ),
            (
                OType::SPATIAL_COMPLEX.as_u16(),
                DecodeKind::SpatialArena,
                None,
                DictFamily::SpatialArena,
            ),
        ];

        for &(o_type, decode_kind, dt_iri, dict_family) in fluree_types {
            table.push(OTypeTableEntry {
                o_type,
                decode_kind,
                datatype_iri: dt_iri.map(String::from),
                dict_family: Some(dict_family),
            });
        }

        // LangString entries (tag 11).
        for (i, tag) in language_tags.iter().enumerate() {
            let ot = OType::lang_string(i as u16);
            table.push(OTypeTableEntry {
                o_type: ot.as_u16(),
                decode_kind: DecodeKind::StringDict,
                datatype_iri: Some(format!("rdf:langString@{tag}")),
                dict_family: Some(DictFamily::StringDict),
            });
        }

        // Customer-defined datatypes (tag 01).
        for (i, iri) in custom_datatype_iris.iter().enumerate() {
            let dt_id = fluree_db_core::DatatypeDictId::RESERVED_COUNT + i as u16;
            // Only add if it's actually a customer type (not a known XSD type
            // that happened to get a non-reserved DatatypeDictId).
            let ot = OType::customer_datatype(dt_id);
            table.push(OTypeTableEntry {
                o_type: ot.as_u16(),
                decode_kind: DecodeKind::StringDict,
                datatype_iri: Some(iri.clone()),
                dict_family: Some(DictFamily::StringDict),
            });
        }

        table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn o_type_table_built_in() {
        let table = IndexRootV6::build_o_type_table(&[], &[]);
        // Should contain all 31 embedded + 13 Fluree = 44 entries.
        assert_eq!(table.len(), 44);

        // Spot-check a few entries.
        let int_entry = table
            .iter()
            .find(|e| e.o_type == OType::XSD_INTEGER.as_u16())
            .unwrap();
        assert_eq!(int_entry.decode_kind, DecodeKind::I64);
        assert_eq!(int_entry.datatype_iri.as_deref(), Some("xsd:integer"));
        assert!(int_entry.dict_family.is_none());

        let string_entry = table
            .iter()
            .find(|e| e.o_type == OType::XSD_STRING.as_u16())
            .unwrap();
        assert_eq!(string_entry.decode_kind, DecodeKind::StringDict);
        assert_eq!(string_entry.dict_family, Some(DictFamily::StringDict));

        let ref_entry = table
            .iter()
            .find(|e| e.o_type == OType::IRI_REF.as_u16())
            .unwrap();
        assert_eq!(ref_entry.decode_kind, DecodeKind::IriRef);
        assert_eq!(ref_entry.dict_family, Some(DictFamily::SubjectDict));
    }

    #[test]
    fn o_type_table_with_langs() {
        let table = IndexRootV6::build_o_type_table(&[], &["en".to_string(), "fr".to_string()]);
        // 44 built-in + 2 langString = 46.
        assert_eq!(table.len(), 46);

        let en_entry = table
            .iter()
            .find(|e| e.o_type == OType::lang_string(0).as_u16())
            .unwrap();
        assert_eq!(en_entry.decode_kind, DecodeKind::StringDict);
        assert!(en_entry.datatype_iri.as_ref().unwrap().contains("en"));
    }

    #[test]
    fn o_type_table_with_custom_types() {
        let table =
            IndexRootV6::build_o_type_table(&["http://example.org/myType".to_string()], &[]);
        // 44 built-in + 1 customer = 45.
        assert_eq!(table.len(), 45);

        let custom = table.last().unwrap();
        assert!(OType::from_u16(custom.o_type).is_customer_datatype());
        assert_eq!(
            custom.datatype_iri.as_deref(),
            Some("http://example.org/myType")
        );
    }
}
