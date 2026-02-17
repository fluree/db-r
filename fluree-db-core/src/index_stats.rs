//! Index statistics types.
//!
//! These types describe per-property, per-class, and per-graph statistics
//! collected during indexing. They are used by the query planner for
//! selectivity estimation and by the index root for metadata.

use crate::ids::GraphId;
use crate::sid::Sid;

// === Per-Property Statistics ===

/// Per-property statistics entry.
///
/// Contains HLL-derived NDV estimates for a single property.
/// Stored sorted by SID for determinism.
#[derive(Debug, Clone)]
pub struct PropertyStatEntry {
    /// Predicate SID as (namespace_code, name).
    pub sid: (u16, String),
    /// Total number of flakes with this property (including history).
    pub count: u64,
    /// Estimated number of distinct object values (via HLL).
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (via HLL).
    pub ndv_subjects: u64,
    /// Most recent transaction time that modified this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts for this property (ValueTypeTag.0, count).
    ///
    /// This is the **ledger-wide aggregate** view (across all graphs).
    /// Graph-scoped property stats (authoritative for range narrowing) live under
    /// `IndexStats.graphs[*].properties[*].datatypes`.
    pub datatypes: Vec<(u8, u64)>,
}

// === Index Statistics ===

/// Index statistics (fast estimates).
///
/// Maintained incrementally during indexing/refresh. Must not require walking
/// the full index tree.
///
/// - `flakes`: total flakes in the index (including history; after dedup)
/// - `size`: estimated total bytes of flakes (speed over accuracy)
/// - `properties`: per-property HLL statistics (optional)
/// - `classes`: per-class property usage statistics (optional)
/// - `graphs`: per-graph ID-based statistics (authoritative)
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (including history; after dedup).
    pub flakes: u64,
    /// Estimated total bytes of flakes in the index (not storage bytes of index nodes).
    pub size: u64,
    /// DEPRECATED: Sid-keyed aggregate view, kept for backward compatibility with
    /// StatsView, current_stats(), and the query planner. Derived from `graphs`
    /// when both are present. Will be removed once all consumers migrate to
    /// ID-based lookups via `graphs`.
    pub properties: Option<Vec<PropertyStatEntry>>,
    /// Per-class property usage statistics (sorted by class SID for determinism).
    /// Tracks which properties are used by instances of each class.
    ///
    /// IMPORTANT: Detailed per-property stats (counts/NDV/datatypes) must live in
    /// `graphs[*].properties` (graph-scoped) and NOT under classes.
    pub classes: Option<Vec<ClassStatEntry>>,
    /// Per-graph statistics keyed by numeric IDs (authoritative, ID-based).
    /// Each entry contains per-property stats including datatype usage.
    /// Sorted by g_id for determinism.
    pub graphs: Option<Vec<GraphStatsEntry>>,
}

// === Class-Property Statistics ===

/// Statistics for a single class (rdf:type target).
///
/// Tracks property usage patterns for instances of this class.
/// Used for query optimization (selectivity estimation) and schema inference.
#[derive(Debug, Clone)]
pub struct ClassStatEntry {
    /// The class SID (target of rdf:type assertions).
    pub class_sid: Sid,
    /// Number of instances of this class.
    pub count: u64,
    /// Properties used by instances of this class (sorted by property SID).
    pub properties: Vec<ClassPropertyUsage>,
}

/// Property usage within a class.
///
/// Intentionally avoids duplicating full per-property stats (counts/NDV/datatypes),
/// which are tracked in graph-scoped property stats (`IndexStats.graphs`).
///
/// This structure is meant for:
/// - class-policy indexing (which properties appear on instances of a class)
/// - ontology / schema visualization (class→property edges, ref target classes)
#[derive(Debug, Clone)]
pub struct ClassPropertyUsage {
    /// The property SID.
    pub property_sid: Sid,
    /// For reference-valued properties, counts by target class.
    ///
    /// Each entry indicates how many reference assertions of this property (from
    /// instances of the owning class) point to instances of the target class.
    ///
    /// Stored sorted by `class_sid` for determinism.
    pub ref_classes: Vec<ClassRefCount>,
}

/// Reference target class counts for a class-scoped property.
#[derive(Debug, Clone)]
pub struct ClassRefCount {
    /// Target class SID (rdf:type of the referenced object).
    pub class_sid: Sid,
    /// Count of reference assertions pointing to this class.
    pub count: u64,
}

// === Graph-Scoped Statistics (ID-Based) ===

/// Per-property stats within a graph, keyed by numeric IDs from GlobalDicts.
///
/// This is the authoritative ID-based stats format. The Sid-keyed
/// `PropertyStatEntry` is a deprecated interim adapter.
#[derive(Debug, Clone)]
pub struct GraphPropertyStatEntry {
    /// Predicate dictionary ID (from GlobalDicts.predicates).
    pub p_id: u32,
    /// Total number of asserted flakes with this property (after dedup; retractions decrement).
    pub count: u64,
    /// Estimated number of distinct object values (via HLL).
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (via HLL).
    pub ndv_subjects: u64,
    /// Most recent transaction time that modified this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts: (ValueTypeTag.0, count).
    pub datatypes: Vec<(u8, u64)>,
}

/// Stats for a single named graph within a ledger.
///
/// Each entry corresponds to one graph in the binary index, identified
/// by `g_id` from GlobalDicts.graphs (0 = default, 1 = txn-meta).
#[derive(Debug, Clone)]
pub struct GraphStatsEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: GraphId,
    /// Total number of flakes in this graph (after dedup).
    pub flakes: u64,
    /// Byte size of flakes in this graph in the binary index.
    /// Set to 0 in the pre-index manifest (binary index not yet built).
    /// Populated by index build/refresh.
    pub size: u64,
    /// Per-property statistics within this graph (sorted by p_id for determinism).
    pub properties: Vec<GraphPropertyStatEntry>,
}
