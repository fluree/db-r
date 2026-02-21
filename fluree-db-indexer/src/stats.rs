//! Index statistics hooks
//!
//! Provides a hook interface for collecting statistics during index building.
//! - `NoOpStatsHook`: Minimal implementation that only counts flakes
//! - `HllStatsHook`: Full HLL-based per-property NDV tracking
//!
//! ## HLL Sketch Persistence (CID-based)
//!
//! Per-property HLL sketches are serialized into a single `HllSketchBlob` and
//! stored in content-addressed storage (CAS) via `ContentKind::StatsSketch`.
//! The blob's `ContentId` is stored in `IndexRootV5.sketch_ref`.
//!
//! GC is automatic: `all_cas_ids()` includes the sketch CID, so the existing
//! set-difference garbage collection handles superseded blobs.
//!
//! For incremental refresh, use `load_sketch_blob()` + `IdStatsHook::with_prior_properties()`.

use fluree_db_core::Flake;
use fluree_db_core::GraphId;
use fluree_db_core::Sid;
use fluree_vocab::namespaces::RDF;
use std::collections::HashSet;

use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;
use fluree_db_core::PropertyStatEntry;
use std::collections::HashMap;

use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{GraphPropertyStatEntry, GraphStatsEntry};
use xxhash_rust::xxh64::xxh64;

// Schema extraction imports (always available, not feature-gated)
use fluree_db_core::{is_rdf_type, is_rdfs_subclass_of, is_rdfs_subproperty_of, FlakeValue};
use fluree_db_core::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};

/// Hook for collecting index statistics during build
///
/// Implementors receive callbacks during index building and produce
/// artifacts to persist alongside the index.
pub trait IndexStatsHook {
    /// Called for each flake during tree building
    fn on_flake(&mut self, flake: &Flake);

    /// Called after build completes, returns artifacts to persist
    fn finalize(self: Box<Self>) -> StatsArtifacts;
}

/// Artifacts produced by stats collection
#[derive(Debug, Clone, Default)]
pub struct StatsArtifacts {
    /// Summary fields for DbRoot (counts, NDV estimates)
    pub summary: StatsSummary,
}

/// Summary statistics for the index
#[derive(Debug, Clone, Default)]
pub struct StatsSummary {
    /// Total number of flakes in the index
    pub flake_count: usize,
    /// Per-property statistics (sorted by SID for determinism)
    /// Only populated when using HllStatsHook.
    pub properties: Option<Vec<PropertyStatEntry>>,
}

/// No-op implementation for Phase A
///
/// Does nothing but count flakes. Placeholder for future HLL/sketch implementations.
#[derive(Debug, Default)]
pub struct NoOpStatsHook {
    flake_count: usize,
}

impl NoOpStatsHook {
    /// Create a new no-op stats hook
    pub fn new() -> Self {
        Self::default()
    }
}

impl IndexStatsHook for NoOpStatsHook {
    fn on_flake(&mut self, _flake: &Flake) {
        self.flake_count += 1;
    }

    fn finalize(self: Box<Self>) -> StatsArtifacts {
        StatsArtifacts {
            summary: StatsSummary {
                flake_count: self.flake_count,
                properties: None,
            },
        }
    }
}

// === HLL-based Statistics Hook ===

/// Per-property HLL sketches for NDV estimation
///
/// Uses our minimal HllSketch256 (p=8, 256 registers) which supports:
/// - Direct serialization as raw bytes
/// - Register-wise merge for incremental updates
/// - Monotone NDV estimation
#[derive(Clone)]
pub struct PropertyHll {
    /// Total flake count for this property
    pub count: u64,
    /// HLL sketch for distinct object values (256 bytes)
    pub values_hll: HllSketch256,
    /// HLL sketch for distinct subjects using this property (256 bytes)
    pub subjects_hll: HllSketch256,
    /// Most recent transaction time
    pub last_modified_t: i64,
}

impl PropertyHll {
    /// Create a new empty PropertyHll
    pub fn new() -> Self {
        Self {
            count: 0,
            values_hll: HllSketch256::new(),
            subjects_hll: HllSketch256::new(),
            last_modified_t: i64::MIN,
        }
    }

    /// Create from loaded sketches (for refresh path)
    pub fn from_sketches(
        count: u64,
        values_hll: HllSketch256,
        subjects_hll: HllSketch256,
        last_modified_t: i64,
    ) -> Self {
        Self {
            count,
            values_hll,
            subjects_hll,
            last_modified_t,
        }
    }

    /// Merge another PropertyHll into this one (register-wise maximum)
    ///
    /// This is the key operation for incremental stats updates:
    /// - Counts are additive
    /// - HLL sketches merge via register-wise max
    /// - NDV is monotone (never decreases)
    pub fn merge_inplace(&mut self, other: &PropertyHll) {
        self.count += other.count;
        self.values_hll.merge_inplace(&other.values_hll);
        self.subjects_hll.merge_inplace(&other.subjects_hll);
        if other.last_modified_t > self.last_modified_t {
            self.last_modified_t = other.last_modified_t;
        }
    }
}

impl Default for PropertyHll {
    fn default() -> Self {
        Self::new()
    }
}

/// HLL-based statistics hook for per-property NDV tracking
///
/// Maintains HyperLogLog sketches for each property to estimate:
/// - Number of distinct object values (NDV values)
/// - Number of distinct subjects using the property (NDV subjects)
///
/// Uses `HllSketch256` (p=8) for direct serialization and incremental merging.
/// Full HLL-based per-property NDV tracking.
#[derive(Default)]
pub struct HllStatsHook {
    flake_count: usize,
    /// Per-property HLL sketches, keyed by predicate SID
    properties: HashMap<Sid, PropertyHll>,
}

impl HllStatsHook {
    /// Create a new HLL stats hook
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an HLL stats hook with pre-loaded property sketches
    ///
    /// Used during refresh to merge novelty with prior sketches loaded from storage.
    pub fn with_prior_properties(properties: HashMap<Sid, PropertyHll>) -> Self {
        Self {
            flake_count: 0,
            properties,
        }
    }

    /// Get the properties map (for persistence)
    pub fn properties(&self) -> &HashMap<Sid, PropertyHll> {
        &self.properties
    }

    /// Get mutable access to properties map
    pub fn properties_mut(&mut self) -> &mut HashMap<Sid, PropertyHll> {
        &mut self.properties
    }

    /// Take ownership of the properties map
    pub fn into_properties(self) -> HashMap<Sid, PropertyHll> {
        self.properties
    }
}

impl IndexStatsHook for HllStatsHook {
    fn on_flake(&mut self, flake: &Flake) {
        self.flake_count += 1;

        // Get or create property entry
        let entry = self.properties.entry(flake.p.clone()).or_default();

        // Update count
        if flake.op {
            entry.count = entry.count.saturating_add(1);
        } else {
            entry.count = entry.count.saturating_sub(1);
        }

        // Insert object value hash into values HLL
        let value_hash = flake.o.canonical_hash();
        entry.values_hll.insert_hash(value_hash);

        // Insert subject hash into subjects HLL
        let subject_hash = flake.s.canonical_hash();
        entry.subjects_hll.insert_hash(subject_hash);

        // Track most recent t
        if flake.t > entry.last_modified_t {
            entry.last_modified_t = flake.t;
        }
    }

    fn finalize(self: Box<Self>) -> StatsArtifacts {
        // Convert property HLLs to PropertyStatEntry, sorted by SID for determinism
        let mut entries: Vec<PropertyStatEntry> = self
            .properties
            .into_iter()
            .map(|(sid, hll)| PropertyStatEntry {
                sid: (sid.namespace_code, sid.name.to_string()),
                count: hll.count,
                ndv_values: hll.values_hll.estimate(),
                ndv_subjects: hll.subjects_hll.estimate(),
                last_modified_t: hll.last_modified_t,
                datatypes: vec![],
            })
            .collect();

        // Sort by SID for deterministic output: namespace_code first, then name
        entries.sort_by(|a, b| a.sid.0.cmp(&b.sid.0).then_with(|| a.sid.1.cmp(&b.sid.1)));

        StatsArtifacts {
            summary: StatsSummary {
                flake_count: self.flake_count,
                properties: if entries.is_empty() {
                    None
                } else {
                    Some(entries)
                },
            },
        }
    }
}

// =============================================================================
// ID-Based Stats Hook
// =============================================================================

/// Key for graph-scoped property stats (numeric IDs only)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct GraphPropertyKey {
    pub g_id: GraphId,
    pub p_id: u32,
}

/// Per-(graph, property) HLL state with datatype tracking.
///
/// Uses signed deltas internally; clamped to 0 at finalize.
#[derive(Debug)]
pub struct IdPropertyHll {
    /// Flake count delta (signed: retractions decrement)
    pub count: i64,
    /// HLL sketch for distinct object values
    pub values_hll: HllSketch256,
    /// HLL sketch for distinct subjects
    pub subjects_hll: HllSketch256,
    /// Most recent transaction time
    pub last_modified_t: i64,
    /// Per-datatype flake count deltas: ValueTypeTag(u8) -> signed count
    pub datatypes: HashMap<u8, i64>,
}

impl IdPropertyHll {
    fn new() -> Self {
        Self {
            count: 0,
            values_hll: HllSketch256::new(),
            subjects_hll: HllSketch256::new(),
            last_modified_t: 0,
            datatypes: HashMap::new(),
        }
    }

    /// Create from loaded sketches (for incremental refresh)
    pub fn from_sketches(
        count: i64,
        values_hll: HllSketch256,
        subjects_hll: HllSketch256,
        last_modified_t: i64,
        datatypes: HashMap<u8, i64>,
    ) -> Self {
        Self {
            count,
            values_hll,
            subjects_hll,
            last_modified_t,
            datatypes,
        }
    }

    /// Merge another IdPropertyHll into this one.
    /// HLL: register-wise max. Counts: additive. last_modified_t: max.
    pub fn merge_from(&mut self, other: &IdPropertyHll) {
        self.count += other.count;
        self.values_hll.merge_inplace(&other.values_hll);
        self.subjects_hll.merge_inplace(&other.subjects_hll);
        self.last_modified_t = self.last_modified_t.max(other.last_modified_t);
        for (&dt, &delta) in &other.datatypes {
            *self.datatypes.entry(dt).or_insert(0) += delta;
        }
    }
}

// =============================================================================
// CID-Based HLL Sketch Blob (CAS Persistence)
// =============================================================================

use serde::{Deserialize, Serialize};

/// CAS-persisted HLL sketch blob.
///
/// Contains all per-(graph, property) HLL sketches produced by `IdStatsHook`.
/// Written to CAS as a single JSON blob; its `ContentId` is stored in
/// `IndexRootV5.sketch_ref`. Counts are clamped to ≥ 0 (snapshot state,
/// not raw signed deltas).
///
/// Entries are sorted by `(g_id, p_id)` for deterministic serialization and
/// thus deterministic CID computation.
#[derive(Debug, Serialize, Deserialize)]
pub struct HllSketchBlob {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// The maximum transaction time covered (equals `index_t`).
    pub index_t: i64,
    /// Per-(graph, property) HLL entries, sorted by `(g_id, p_id)`.
    pub entries: Vec<HllPropertyEntry>,
}

/// A single property's HLL state within an [`HllSketchBlob`].
#[derive(Debug, Serialize, Deserialize)]
pub struct HllPropertyEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: GraphId,
    /// Predicate dictionary ID.
    pub p_id: u32,
    /// Flake count (clamped to ≥ 0; snapshot state, not raw delta).
    pub count: u64,
    /// Hex-encoded 256-byte values HLL register array.
    pub values_hll: String,
    /// Hex-encoded 256-byte subjects HLL register array.
    pub subjects_hll: String,
    /// Most recent transaction time for this property.
    pub last_modified_t: i64,
    /// Per-datatype flake counts, sorted by tag, clamped to ≥ 0.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datatypes: Vec<(u8, u64)>,
}

impl HllSketchBlob {
    const CURRENT_VERSION: u32 = 1;

    /// Serialize from the `IdStatsHook`'s properties map.
    ///
    /// Must be called BEFORE `finalize_with_aggregate_properties()` consumes the
    /// hook, by borrowing `hook.properties()`. Counts and per-datatype deltas are
    /// clamped to ≥ 0 (the blob represents snapshot state, not raw signed deltas).
    pub fn from_properties(
        index_t: i64,
        properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
    ) -> Self {
        let mut entries: Vec<HllPropertyEntry> = properties
            .iter()
            .map(|(key, hll)| {
                let mut dt_vec: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&k, &v)| (k, v.max(0) as u64))
                    .collect();
                dt_vec.sort_by_key(|&(tag, _)| tag);

                HllPropertyEntry {
                    g_id: key.g_id,
                    p_id: key.p_id,
                    count: hll.count.max(0) as u64,
                    values_hll: hex::encode(hll.values_hll.to_bytes()),
                    subjects_hll: hex::encode(hll.subjects_hll.to_bytes()),
                    last_modified_t: hll.last_modified_t,
                    datatypes: dt_vec,
                }
            })
            .collect();
        entries.sort_by(|a, b| (a.g_id, a.p_id).cmp(&(b.g_id, b.p_id)));

        Self {
            version: Self::CURRENT_VERSION,
            index_t,
            entries,
        }
    }

    /// Serialize to canonical JSON bytes (deterministic via sorted `entries`).
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    ///
    /// Returns an error if the version is not supported.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let blob: Self = serde_json::from_slice(bytes)
            .map_err(|e| IndexerError::Serialization(e.to_string()))?;
        if blob.version != 1 {
            return Err(IndexerError::Serialization(format!(
                "unsupported sketch blob version {} (expected 1)",
                blob.version
            )));
        }
        Ok(blob)
    }

    /// Reconstruct the `HashMap<GraphPropertyKey, IdPropertyHll>` from the blob.
    ///
    /// Used to load prior sketches for incremental refresh.
    pub fn into_properties(self) -> Result<HashMap<GraphPropertyKey, IdPropertyHll>> {
        let mut map = HashMap::with_capacity(self.entries.len());
        for entry in self.entries {
            let values_bytes = hex::decode(&entry.values_hll).map_err(|e| {
                IndexerError::Serialization(format!(
                    "bad hex values_hll for g{}:p{}: {}",
                    entry.g_id, entry.p_id, e
                ))
            })?;
            let subjects_bytes = hex::decode(&entry.subjects_hll).map_err(|e| {
                IndexerError::Serialization(format!(
                    "bad hex subjects_hll for g{}:p{}: {}",
                    entry.g_id, entry.p_id, e
                ))
            })?;

            let values_hll = decode_hll_registers(&values_bytes, entry.g_id, entry.p_id)?;
            let subjects_hll = decode_hll_registers(&subjects_bytes, entry.g_id, entry.p_id)?;
            let datatypes: HashMap<u8, i64> = entry
                .datatypes
                .into_iter()
                .map(|(k, v)| (k, v as i64))
                .collect();

            map.insert(
                GraphPropertyKey {
                    g_id: entry.g_id,
                    p_id: entry.p_id,
                },
                IdPropertyHll::from_sketches(
                    entry.count as i64,
                    values_hll,
                    subjects_hll,
                    entry.last_modified_t,
                    datatypes,
                ),
            );
        }
        Ok(map)
    }
}

/// Decode hex-decoded bytes into an `HllSketch256`, validating 256-byte length.
fn decode_hll_registers(bytes: &[u8], g_id: GraphId, p_id: u32) -> Result<HllSketch256> {
    if bytes.len() != 256 {
        return Err(IndexerError::Serialization(format!(
            "HLL registers must be 256 bytes for g{}:p{}, got {}",
            g_id,
            p_id,
            bytes.len()
        )));
    }
    let mut registers = [0u8; 256];
    registers.copy_from_slice(bytes);
    Ok(HllSketch256::from_bytes(&registers))
}

/// Load an HLL sketch blob from CAS by its `ContentId`.
///
/// Returns `None` if the blob is not found (first build, or storage migration).
/// Propagates real I/O errors.
pub async fn load_sketch_blob(
    content_store: &dyn fluree_db_core::ContentStore,
    sketch_id: &fluree_db_core::ContentId,
) -> Result<Option<HllSketchBlob>> {
    match content_store.get(sketch_id).await {
        Ok(bytes) => {
            let blob = HllSketchBlob::from_json_bytes(&bytes)?;
            Ok(Some(blob))
        }
        Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
        Err(e) => Err(IndexerError::StorageRead(format!(
            "sketch blob {sketch_id}: {e}"
        ))),
    }
}

// --- Domain-separated hashing for HLL ---

/// Domain separator for object value hashing.
const OBJ_HASH_DOMAIN: &[u8] = b"fluree:obj:";

/// Domain separator for subject HLL hashing.
const SUBJ_HASH_DOMAIN: &[u8] = b"fluree:subj:";

/// Compute a stable, endian-invariant hash of an object value.
///
/// Domain-separated by `o_kind` to prevent cross-kind collisions
/// (e.g., `NumInt(3)` vs `RefId(3)` both have `o_key=3`).
pub fn value_hash(o_kind: u8, o_key: u64) -> u64 {
    // domain(11) + kind(1) + key(8) = 20 bytes
    let mut buf = [0u8; 20];
    buf[..11].copy_from_slice(OBJ_HASH_DOMAIN);
    buf[11] = o_kind;
    buf[12..20].copy_from_slice(&o_key.to_le_bytes());
    xxh64(&buf, 0)
}

/// Compute a stable hash of a subject ID for HLL insertion.
///
/// Hashes `s_id` rather than using it directly to ensure uniform bit
/// distribution across HLL registers.
pub fn subject_hash(s_id: u64) -> u64 {
    // domain(12) + s_id(8) = 20 bytes
    let mut buf = [0u8; 20];
    buf[..12].copy_from_slice(SUBJ_HASH_DOMAIN);
    buf[12..20].copy_from_slice(&s_id.to_le_bytes());
    xxh64(&buf, 0)
}

/// A single resolved record for stats collection.
///
/// Bundles the per-op fields needed by `IdStatsHook::on_record`.
#[derive(Debug, Clone, Copy)]
pub struct StatsRecord {
    /// Graph dictionary ID (0 = default)
    pub g_id: GraphId,
    /// Predicate dictionary ID
    pub p_id: u32,
    /// Subject dictionary ID
    pub s_id: u64,
    /// Datatype ID
    pub dt: ValueTypeTag,
    /// Pre-computed object value hash (from `value_hash()`)
    pub o_hash: u64,
    /// Object kind discriminant (for class tracking)
    pub o_kind: u8,
    /// Object key payload (for class tracking; sid64 when o_kind == REF_ID)
    pub o_key: u64,
    /// Transaction time
    pub t: i64,
    /// true = assertion, false = retraction
    pub op: bool,
    /// Language tag dictionary ID (0 = no language tag, >= 1 = lang_id).
    pub lang_id: u16,
}

/// Result from `IdStatsHook::finalize()`.
pub struct IdStatsResult {
    /// Per-graph stats entries (authoritative, ID-keyed).
    /// Excludes txn-meta graph (g_id=1).
    pub graphs: Vec<GraphStatsEntry>,
    /// Total flake count (excluding txn-meta).
    pub total_flakes: u64,
}

/// ID-based stats hook for import/index paths where GlobalDicts are available.
///
/// Maintains per-(graph, property) HLL sketches and datatype usage.
/// All keys are numeric IDs — no Sid anywhere.
///
/// # Usage
///
/// ```ignore
/// let mut hook = IdStatsHook::new();
/// // Per resolved op:
/// hook.on_record(&StatsRecord { g_id, p_id, s_id, dt, o_hash, o_kind, o_key, t, op });
/// // After all ops:
/// let result = hook.finalize();
/// ```
#[derive(Debug, Default)]
pub struct IdStatsHook {
    flake_count: usize,
    properties: HashMap<GraphPropertyKey, IdPropertyHll>,
    /// Per-graph flake count (signed delta)
    graph_flakes: HashMap<GraphId, i64>,
    /// p_id for rdf:type (when set, enables class tracking)
    rdf_type_p_id: Option<u32>,
    /// Whether to track reference target-class edges (class→property→ref-class).
    ///
    /// This can be very memory-intensive for large datasets because it requires
    /// retaining per-subject reference histories until finalize time.
    ///
    /// When disabled, class counts + class→property presence still work, but
    /// `finalize_with_aggregate_properties()` will return an empty `class_ref_targets`.
    track_ref_targets: bool,
    /// Class membership counts: (g_id, class_sid64) → signed delta count.
    /// Graph-scoped so per-graph ClassStatEntry can be derived.
    class_counts: HashMap<(GraphId, u64), i64>,
    /// (g_id, subject) → class_sid64 → signed delta count (net rdf:type membership).
    ///
    /// This is used at finalize-time to derive the subject's current class set,
    /// which is then used to compute:
    /// - class→property presence
    /// - class→property→target-class ref-edge counts
    ///
    /// Keeping deltas (rather than a set) makes this merge-safe across commits.
    subject_class_deltas: HashMap<(GraphId, u64), HashMap<u64, i64>>,
    /// Per-subject property tracking (for retroactive class attribution).
    /// Graph-scoped: (g_id, subject_sid64) → set of p_ids.
    subject_props: HashMap<(GraphId, u64), HashSet<u32>>,
    /// Per-subject ref history: (g_id, subject sid64) → property → object sid64 → signed delta count.
    ///
    /// At finalize-time, this is combined with derived subject/object class sets
    /// to produce class→property→target-class ref-edge counts.
    subject_ref_history: HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
    /// Per-subject, per-property datatype tracking: (g_id, subject sid64) → p_id → dt_tag → signed delta.
    ///
    /// Used at finalize-time to derive per-class datatype distributions by
    /// cross-referencing with subject_classes.
    subject_prop_dts: HashMap<(GraphId, u64), HashMap<u32, HashMap<u8, i64>>>,
    /// Per-subject, per-property language tag tracking: (g_id, subject sid64) → p_id → lang_id → signed delta.
    ///
    /// Used at finalize-time to derive per-class language distributions by
    /// cross-referencing with subject_classes.
    subject_prop_langs: HashMap<(GraphId, u64), HashMap<u32, HashMap<u16, i64>>>,
}

impl IdStatsHook {
    pub fn new() -> Self {
        // Default behavior preserves existing incremental stats richness.
        // Import paths may disable ref-target tracking explicitly.
        Self {
            track_ref_targets: true,
            ..Self::default()
        }
    }

    /// Create a hook seeded with prior per-property HLL sketches.
    ///
    /// Enables incremental refresh: load prior sketches from a CAS blob,
    /// then process only novelty commits. The hook's `on_record()` will
    /// merge new observations into the existing registers.
    pub fn with_prior_properties(properties: HashMap<GraphPropertyKey, IdPropertyHll>) -> Self {
        Self {
            properties,
            track_ref_targets: true,
            ..Self::default()
        }
    }

    /// Set the predicate ID for rdf:type to enable class tracking.
    pub fn set_rdf_type_p_id(&mut self, p_id: u32) {
        self.rdf_type_p_id = Some(p_id);
    }

    /// Enable/disable tracking of reference target-class edges.
    pub fn set_track_ref_targets(&mut self, enabled: bool) {
        self.track_ref_targets = enabled;
    }

    /// Process a single record with resolved IDs.
    ///
    /// Called per-op after the resolver maps Sids to numeric IDs.
    pub fn on_record(&mut self, rec: &StatsRecord) {
        self.flake_count += 1;
        let delta: i64 = if rec.op { 1 } else { -1 };

        // Track per-graph flake count
        *self.graph_flakes.entry(rec.g_id).or_insert(0) += delta;

        let key = GraphPropertyKey {
            g_id: rec.g_id,
            p_id: rec.p_id,
        };
        let hll = self
            .properties
            .entry(key)
            .or_insert_with(IdPropertyHll::new);

        hll.count += delta;

        // HLL: only insert on assertions (NDV is monotone)
        if rec.op {
            hll.values_hll.insert_hash(rec.o_hash);
            hll.subjects_hll.insert_hash(subject_hash(rec.s_id));
        }

        if rec.t > hll.last_modified_t {
            hll.last_modified_t = rec.t;
        }

        // Track datatype usage
        *hll.datatypes.entry(rec.dt.as_u8()).or_insert(0) += delta;

        // Track class membership and class→property attribution (graph-scoped).
        if let Some(rdf_type_pid) = self.rdf_type_p_id {
            if rec.p_id == rdf_type_pid && rec.o_kind == 0x05 {
                // ObjKind::REF_ID == 0x05: this is an rdf:type assertion/retraction
                *self.class_counts.entry((rec.g_id, rec.o_key)).or_insert(0) += delta;
                *self
                    .subject_class_deltas
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .entry(rec.o_key)
                    .or_insert(0) += delta;
            } else if rec.op {
                // Non-rdf:type property assertion: track per-subject and per-class
                self.subject_props
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .insert(rec.p_id);
            }

            // Track per-subject datatype usage for class→property→datatype attribution.
            if rec.p_id != rdf_type_pid {
                *self
                    .subject_prop_dts
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .entry(rec.p_id)
                    .or_default()
                    .entry(rec.dt.as_u8())
                    .or_insert(0) += delta;

                // Track per-subject language tag usage.
                if rec.lang_id != 0 && rec.dt == ValueTypeTag::LANG_STRING {
                    *self
                        .subject_prop_langs
                        .entry((rec.g_id, rec.s_id))
                        .or_default()
                        .entry(rec.p_id)
                        .or_default()
                        .entry(rec.lang_id)
                        .or_insert(0) += delta;
                }
            }

            // Track reference-valued properties for class→property ref target stats.
            //
            // We track both assertions and retractions via signed deltas.
            // Only applies to ref objects (ObjKind::REF_ID).
            if self.track_ref_targets && rec.p_id != rdf_type_pid && rec.o_kind == 0x05 {
                // Record per-subject ref history (for retroactive attribution on rdf:type)
                *self
                    .subject_ref_history
                    .entry((rec.g_id, rec.s_id))
                    .or_default()
                    .entry(rec.p_id)
                    .or_default()
                    .entry(rec.o_key)
                    .or_insert(0) += delta;
            }
        }
    }

    /// Merge another hook into this one (for cross-commit accumulation).
    ///
    /// HLL: register-wise max. Counts: additive.
    pub fn merge_from(&mut self, other: IdStatsHook) {
        self.flake_count += other.flake_count;

        for (g_id, delta) in other.graph_flakes {
            *self.graph_flakes.entry(g_id).or_insert(0) += delta;
        }

        for (key, other_hll) in other.properties {
            self.properties
                .entry(key)
                .or_insert_with(IdPropertyHll::new)
                .merge_from(&other_hll);
        }

        // Merge class counts and attribution inputs
        if self.rdf_type_p_id.is_none() {
            self.rdf_type_p_id = other.rdf_type_p_id;
        }
        if !self.track_ref_targets {
            self.track_ref_targets = other.track_ref_targets;
        }
        for (key, delta) in other.class_counts {
            *self.class_counts.entry(key).or_insert(0) += delta;
        }
        for (key, class_map) in other.subject_class_deltas {
            let entry = self.subject_class_deltas.entry(key).or_default();
            for (class_sid64, delta) in class_map {
                *entry.entry(class_sid64).or_insert(0) += delta;
            }
        }
        for (key, props) in other.subject_props {
            self.subject_props.entry(key).or_default().extend(props);
        }
        // Merge per-subject ref history.
        if self.track_ref_targets {
            for (key, per_prop) in other.subject_ref_history {
                let entry = self.subject_ref_history.entry(key).or_default();
                for (p_id, objs) in per_prop {
                    let o_entry = entry.entry(p_id).or_default();
                    for (obj, d) in objs {
                        *o_entry.entry(obj).or_insert(0) += d;
                    }
                }
            }
        }
    }

    /// Borrow the internal properties map (for sketch persistence before finalize).
    pub fn properties(&self) -> &HashMap<GraphPropertyKey, IdPropertyHll> {
        &self.properties
    }

    /// Total flake count (all graphs, all ops).
    pub fn flake_count(&self) -> usize {
        self.flake_count
    }

    /// Mutable access to per-graph flake totals.
    ///
    /// Used by incremental indexing to seed base-root flake counts before
    /// feeding novelty records, so `finalize()` produces correct totals
    /// (base + delta) rather than delta-only.
    pub fn graph_flakes_mut(&mut self) -> &mut HashMap<GraphId, i64> {
        &mut self.graph_flakes
    }

    /// Read-only access to class membership count deltas.
    ///
    /// Used by incremental indexing to extract novelty-only class count deltas
    /// (before finalize consumes the hook). Keyed by `(g_id, class_sid64)`,
    /// values are signed deltas: +1 per rdf:type assertion, -1 per retraction.
    pub fn class_count_deltas(&self) -> &HashMap<(GraphId, u64), i64> {
        &self.class_counts
    }

    /// Read-only access to per-subject rdf:type deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `class_sid64 -> signed delta`.
    /// Used by incremental indexing to merge novelty rdf:type deltas with
    /// base class memberships from the PSOT index.
    pub fn subject_class_deltas(&self) -> &HashMap<(GraphId, u64), HashMap<u64, i64>> {
        &self.subject_class_deltas
    }

    /// Read-only access to per-subject property sets.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are the set of predicate IDs
    /// that subject has in novelty. Used by incremental indexing for
    /// class-property attribution.
    pub fn subject_props(&self) -> &HashMap<(GraphId, u64), HashSet<u32>> {
        &self.subject_props
    }

    /// Read-only access to per-subject ref history.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are
    /// `property_p_id -> object_sid64 -> signed delta`.
    /// Used by incremental indexing for computing ref-class edges.
    #[allow(clippy::type_complexity)]
    pub fn subject_ref_history(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>> {
        &self.subject_ref_history
    }

    /// Read-only access to per-subject, per-property datatype deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `p_id -> dt_tag(u8) -> signed delta`.
    /// Used by incremental indexing for class→property→datatype attribution.
    #[allow(clippy::type_complexity)]
    pub fn subject_prop_dts(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u8, i64>>> {
        &self.subject_prop_dts
    }

    /// Read-only access to per-subject, per-property language tag deltas.
    ///
    /// Keyed by `(g_id, subject_sid64)`, values are `p_id -> lang_id(u16) -> signed delta`.
    /// Used by incremental indexing for class→property→lang attribution.
    #[allow(clippy::type_complexity)]
    pub fn subject_prop_langs(&self) -> &HashMap<(GraphId, u64), HashMap<u32, HashMap<u16, i64>>> {
        &self.subject_prop_langs
    }

    /// Produce per-graph stats and aggregate property stats.
    ///
    /// Excludes txn-meta graph (g_id=1) from both `graphs` and aggregate
    /// `properties`. Clamps all signed deltas to 0.
    pub fn finalize(self) -> IdStatsResult {
        // Group by g_id, then by p_id
        let mut graph_map: HashMap<GraphId, Vec<(&GraphPropertyKey, &IdPropertyHll)>> =
            HashMap::new();
        for (key, hll) in &self.properties {
            graph_map.entry(key.g_id).or_default().push((key, hll));
        }

        // Build per-graph entries (excluding g_id=1 txn-meta)
        let mut graphs: Vec<GraphStatsEntry> = Vec::new();

        for (&g_id, entries) in &graph_map {
            // Skip txn-meta graph from output
            if g_id == 1 {
                continue;
            }

            let mut props: Vec<GraphPropertyStatEntry> = Vec::new();
            for (key, hll) in entries {
                // Clamp count to 0
                let count = hll.count.max(0) as u64;
                let datatypes: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&dt, &v)| (dt, v.max(0) as u64))
                    .collect();

                props.push(GraphPropertyStatEntry {
                    p_id: key.p_id,
                    count,
                    ndv_values: hll.values_hll.estimate(),
                    ndv_subjects: hll.subjects_hll.estimate(),
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                });
            }

            // Sort properties by p_id for determinism
            props.sort_by_key(|p| p.p_id);

            let graph_flake_count =
                self.graph_flakes.get(&g_id).copied().unwrap_or(0).max(0) as u64;

            graphs.push(GraphStatsEntry {
                g_id,
                flakes: graph_flake_count,
                size: 0, // Populated by index build, not available here
                properties: props,
                classes: None, // Populated by caller after finalize
            });
        }

        // Sort graphs by g_id for determinism
        graphs.sort_by_key(|g| g.g_id);

        // Total flakes excluding txn-meta
        let total_flakes: u64 = self
            .graph_flakes
            .iter()
            .filter(|(&g_id, _)| g_id != 1)
            .map(|(_, &delta)| delta.max(0) as u64)
            .sum();

        IdStatsResult {
            graphs,
            total_flakes,
        }
    }

    /// Finalize into per-graph stats plus a ledger-wide aggregate property view
    /// and graph-scoped class membership counts.
    ///
    /// The aggregate view is keyed only by `p_id` (across all graphs), with HLL sketches
    /// merged across graphs so NDV estimates remain meaningful. Datatype counts are summed
    /// across graphs.
    ///
    /// Class outputs are graph-scoped:
    /// - `class_counts`: `(g_id, class_sid64, count)` triples
    /// - `class_properties`: `(g_id, class_sid64) -> HashSet<p_id>`
    /// - `class_ref_targets`: `(g_id, class_sid64) -> p_id -> target_class_sid64 -> delta`
    ///
    /// Excludes txn-meta graph (g_id=1) from both per-graph and aggregate results.
    #[allow(clippy::type_complexity)]
    pub fn finalize_with_aggregate_properties(
        self,
    ) -> (
        IdStatsResult,
        Vec<GraphPropertyStatEntry>,
        Vec<(GraphId, u64, u64)>,
        HashMap<(GraphId, u64), HashSet<u32>>,
        HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
    ) {
        // Aggregate by p_id across all graphs (excluding txn-meta g_id=1)
        let mut agg: HashMap<u32, IdPropertyHll> = HashMap::new();
        for (key, hll) in &self.properties {
            if key.g_id == 1 {
                continue;
            }
            agg.entry(key.p_id)
                .or_insert_with(IdPropertyHll::new)
                .merge_from(hll);
        }

        let mut properties: Vec<GraphPropertyStatEntry> = agg
            .into_iter()
            .map(|(p_id, hll)| {
                let count = hll.count.max(0) as u64;
                let datatypes: Vec<(u8, u64)> = hll
                    .datatypes
                    .iter()
                    .filter(|(_, &v)| v > 0)
                    .map(|(&dt, &v)| (dt, v.max(0) as u64))
                    .collect();

                GraphPropertyStatEntry {
                    p_id,
                    count,
                    ndv_values: hll.values_hll.estimate(),
                    ndv_subjects: hll.subjects_hll.estimate(),
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                }
            })
            .collect();

        // Deterministic ordering
        properties.sort_by_key(|p| p.p_id);

        // Extract class counts ((g_id, sid64) → count), clamped to 0, excluding g_id=1
        let mut class_counts: Vec<(GraphId, u64, u64)> = self
            .class_counts
            .iter()
            .filter(|(&(g_id, _), &delta)| g_id != 1 && delta > 0)
            .map(|(&(g_id, sid64), &delta)| (g_id, sid64, delta as u64))
            .collect();
        class_counts.sort_by_key(|&(g_id, sid64, _)| (g_id, sid64));

        // Derive current (g_id, subject) → classes from rdf:type deltas (net membership).
        let mut subject_classes: HashMap<(GraphId, u64), Vec<u64>> = HashMap::new();
        for (&(g_id, subj_sid64), class_map) in &self.subject_class_deltas {
            if g_id == 1 {
                continue;
            }
            let mut classes: Vec<u64> = class_map
                .iter()
                .filter_map(|(&class_sid64, &d)| (d > 0).then_some(class_sid64))
                .collect();
            if classes.is_empty() {
                continue;
            }
            classes.sort_unstable();
            subject_classes.insert((g_id, subj_sid64), classes);
        }

        // Compute class→property presence from subject_props + current classes (graph-scoped).
        let mut class_properties: HashMap<(GraphId, u64), HashSet<u32>> = HashMap::new();
        for (&(g_id, subj_sid64), props) in &self.subject_props {
            if g_id == 1 {
                continue;
            }
            let Some(classes) = subject_classes.get(&(g_id, subj_sid64)) else {
                continue;
            };
            for &class_sid64 in classes {
                class_properties
                    .entry((g_id, class_sid64))
                    .or_default()
                    .extend(props.iter().copied());
            }
        }

        // Compute class→property→target-class ref-edge counts from subject_ref_history
        // and current (net) subject/object class sets (graph-scoped).
        let mut class_ref_targets: HashMap<(GraphId, u64), HashMap<u32, HashMap<u64, i64>>> =
            HashMap::new();
        for (&(g_id, subj_sid64), per_prop) in &self.subject_ref_history {
            if g_id == 1 {
                continue;
            }
            let Some(subj_classes) = subject_classes.get(&(g_id, subj_sid64)) else {
                continue;
            };
            for (&p_id, objs) in per_prop {
                for (&obj_sid64, &edge_delta) in objs {
                    if edge_delta == 0 {
                        continue;
                    }
                    let Some(obj_classes) = subject_classes.get(&(g_id, obj_sid64)) else {
                        continue;
                    };
                    for &subj_class_sid64 in subj_classes {
                        for &obj_class_sid64 in obj_classes {
                            *class_ref_targets
                                .entry((g_id, subj_class_sid64))
                                .or_default()
                                .entry(p_id)
                                .or_default()
                                .entry(obj_class_sid64)
                                .or_insert(0) += edge_delta;
                        }
                    }
                }
            }
        }

        let graphs = self.finalize();
        (
            graphs,
            properties,
            class_counts,
            class_properties,
            class_ref_targets,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_test_flake(t: i64) -> Flake {
        Flake::new(
            Sid::new(1, "s"),
            Sid::new(2, "p"),
            FlakeValue::Long(42),
            Sid::new(3, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_no_op_stats_hook() {
        let mut hook = NoOpStatsHook::new();

        hook.on_flake(&make_test_flake(1));
        hook.on_flake(&make_test_flake(2));
        hook.on_flake(&make_test_flake(3));

        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 3);
    }

    #[test]
    fn test_no_op_stats_hook_empty() {
        let hook = NoOpStatsHook::new();
        let artifacts = Box::new(hook).finalize();

        assert_eq!(artifacts.summary.flake_count, 0);
    }

    // === HLL Stats Hook Tests ===

    mod hll_tests {
        use super::*;

        fn make_flake(subject: &str, predicate: &str, value: i64, t: i64) -> Flake {
            Flake::new(
                Sid::new(1, subject),
                Sid::new(2, predicate),
                FlakeValue::Long(value),
                Sid::new(3, "long"),
                t,
                true,
                None,
            )
        }

        #[test]
        fn test_hll_stats_hook_basic() {
            let mut hook = HllStatsHook::new();

            // 3 flakes with same predicate, different subjects and values
            hook.on_flake(&make_flake("alice", "name", 1, 1));
            hook.on_flake(&make_flake("bob", "name", 2, 2));
            hook.on_flake(&make_flake("charlie", "name", 3, 3));

            let artifacts = Box::new(hook).finalize();

            assert_eq!(artifacts.summary.flake_count, 3);
            assert!(artifacts.summary.properties.is_some());

            let props = artifacts.summary.properties.unwrap();
            assert_eq!(props.len(), 1);

            let name_prop = &props[0];
            assert_eq!(name_prop.sid, (2, "name".to_string()));
            assert_eq!(name_prop.count, 3);
            assert_eq!(name_prop.ndv_values, 3); // 3 distinct values
            assert_eq!(name_prop.ndv_subjects, 3); // 3 distinct subjects
            assert_eq!(name_prop.last_modified_t, 3);
        }

        #[test]
        fn test_hll_stats_hook_multiple_properties() {
            let mut hook = HllStatsHook::new();

            // Property "name" with 2 subjects
            hook.on_flake(&make_flake("alice", "name", 1, 1));
            hook.on_flake(&make_flake("bob", "name", 2, 2));

            // Property "age" with 3 subjects
            hook.on_flake(&make_flake("alice", "age", 25, 3));
            hook.on_flake(&make_flake("bob", "age", 30, 4));
            hook.on_flake(&make_flake("charlie", "age", 35, 5));

            let artifacts = Box::new(hook).finalize();

            assert_eq!(artifacts.summary.flake_count, 5);

            let props = artifacts.summary.properties.unwrap();
            assert_eq!(props.len(), 2);

            // Properties should be sorted by SID
            // Both have namespace 2, so sorted by name: "age" < "name"
            assert_eq!(props[0].sid, (2, "age".to_string()));
            assert_eq!(props[0].count, 3);
            assert_eq!(props[0].ndv_subjects, 3);

            assert_eq!(props[1].sid, (2, "name".to_string()));
            assert_eq!(props[1].count, 2);
            assert_eq!(props[1].ndv_subjects, 2);
        }

        #[test]
        fn test_hll_stats_hook_duplicate_values() {
            let mut hook = HllStatsHook::new();

            // Same subject, same predicate, same value (e.g., re-assertion)
            hook.on_flake(&make_flake("alice", "status", 100, 1));
            hook.on_flake(&make_flake("alice", "status", 100, 2));
            hook.on_flake(&make_flake("alice", "status", 100, 3));

            let artifacts = Box::new(hook).finalize();

            let props = artifacts.summary.properties.unwrap();
            assert_eq!(props.len(), 1);

            let status_prop = &props[0];
            assert_eq!(status_prop.count, 3); // 3 flakes counted
            assert_eq!(status_prop.ndv_values, 1); // but only 1 distinct value
            assert_eq!(status_prop.ndv_subjects, 1); // and 1 distinct subject
            assert_eq!(status_prop.last_modified_t, 3);
        }

        #[test]
        fn test_hll_stats_hook_empty() {
            let hook = HllStatsHook::new();
            let artifacts = Box::new(hook).finalize();

            assert_eq!(artifacts.summary.flake_count, 0);
            assert!(artifacts.summary.properties.is_none()); // Empty returns None
        }

        #[test]
        fn test_hll_stats_hook_sorted_by_namespace_then_name() {
            let mut hook = HllStatsHook::new();

            // Flakes with different namespace codes
            hook.on_flake(&Flake::new(
                Sid::new(1, "s"),
                Sid::new(200, "prop_b"),
                FlakeValue::Long(1),
                Sid::new(3, "long"),
                1,
                true,
                None,
            ));
            hook.on_flake(&Flake::new(
                Sid::new(1, "s"),
                Sid::new(100, "prop_a"),
                FlakeValue::Long(2),
                Sid::new(3, "long"),
                2,
                true,
                None,
            ));
            hook.on_flake(&Flake::new(
                Sid::new(1, "s"),
                Sid::new(100, "prop_b"),
                FlakeValue::Long(3),
                Sid::new(3, "long"),
                3,
                true,
                None,
            ));

            let artifacts = Box::new(hook).finalize();
            let props = artifacts.summary.properties.unwrap();

            // Should be sorted: (100, "prop_a") < (100, "prop_b") < (200, "prop_b")
            assert_eq!(props[0].sid, (100, "prop_a".to_string()));
            assert_eq!(props[1].sid, (100, "prop_b".to_string()));
            assert_eq!(props[2].sid, (200, "prop_b".to_string()));
        }

        #[test]
        fn test_property_hll_merge() {
            let mut hll1 = PropertyHll::new();
            let mut hll2 = PropertyHll::new();

            // Use golden ratio constant to distribute hash values across buckets
            // Raw small integers (1, 2, 3, 4) would all go to bucket 0 since top 8 bits are 0
            const GOLDEN: u64 = 0x9e3779b97f4a7c15;

            // Add different values to each (multiply by golden ratio for distribution)
            hll1.values_hll.insert_hash(1u64.wrapping_mul(GOLDEN));
            hll1.values_hll.insert_hash(2u64.wrapping_mul(GOLDEN));
            hll1.subjects_hll.insert_hash(100u64.wrapping_mul(GOLDEN));
            hll1.count = 2;
            hll1.last_modified_t = 5;

            hll2.values_hll.insert_hash(3u64.wrapping_mul(GOLDEN));
            hll2.values_hll.insert_hash(4u64.wrapping_mul(GOLDEN));
            hll2.subjects_hll.insert_hash(200u64.wrapping_mul(GOLDEN));
            hll2.count = 2;
            hll2.last_modified_t = 10;

            // Merge
            hll1.merge_inplace(&hll2);

            assert_eq!(hll1.count, 4);
            assert_eq!(hll1.last_modified_t, 10);
            // HLL estimates for very small cardinalities use linear counting
            // and may not be exactly equal to the true count
            let values_est = hll1.values_hll.estimate();
            let subjects_est = hll1.subjects_hll.estimate();
            assert!(
                (2..=8).contains(&values_est),
                "values estimate {} should be in range [2, 8]",
                values_est
            );
            assert!(
                (1..=4).contains(&subjects_est),
                "subjects estimate {} should be in range [1, 4]",
                subjects_est
            );
        }

        #[test]
        fn test_with_prior_properties() {
            // Create prior properties
            let mut prior = HashMap::new();
            let mut hll = PropertyHll::new();
            hll.values_hll.insert_hash(1);
            hll.subjects_hll.insert_hash(100);
            hll.count = 5;
            hll.last_modified_t = 3;
            prior.insert(Sid::new(2, "name"), hll);

            // Create hook with prior
            let mut hook = HllStatsHook::with_prior_properties(prior);

            // Add new flakes
            hook.on_flake(&make_flake("new_subject", "name", 999, 10));

            let artifacts = Box::new(hook).finalize();
            let props = artifacts.summary.properties.unwrap();

            assert_eq!(props.len(), 1);
            let prop = &props[0];
            assert_eq!(prop.count, 6); // 5 prior + 1 new
            assert_eq!(prop.last_modified_t, 10); // Updated to new t
            assert_eq!(prop.ndv_values, 2); // 1 prior + 1 new
            assert_eq!(prop.ndv_subjects, 2); // 1 prior + 1 new
        }
    }

    // === HLL Sketch Blob Tests ===

    mod sketch_blob_tests {
        use super::*;
        use std::collections::HashMap;

        fn make_test_properties() -> HashMap<GraphPropertyKey, IdPropertyHll> {
            let mut map = HashMap::new();

            let mut hll1 = IdPropertyHll::new();
            hll1.values_hll.insert_hash(100);
            hll1.values_hll.insert_hash(200);
            hll1.subjects_hll.insert_hash(1000);
            hll1.count = 5;
            hll1.last_modified_t = 10;
            *hll1.datatypes.entry(3).or_insert(0) += 3; // 3 string values
            *hll1.datatypes.entry(5).or_insert(0) += 2; // 2 ref values
            map.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll1);

            let mut hll2 = IdPropertyHll::new();
            hll2.values_hll.insert_hash(300);
            hll2.subjects_hll.insert_hash(2000);
            hll2.count = 3;
            hll2.last_modified_t = 8;
            map.insert(GraphPropertyKey { g_id: 0, p_id: 2 }, hll2);

            map
        }

        #[test]
        fn test_sketch_blob_round_trip() {
            let props = make_test_properties();
            let blob = HllSketchBlob::from_properties(10, &props);

            let bytes = blob.to_json_bytes().unwrap();
            let parsed = HllSketchBlob::from_json_bytes(&bytes).unwrap();

            assert_eq!(parsed.version, 1);
            assert_eq!(parsed.index_t, 10);
            assert_eq!(parsed.entries.len(), 2);

            // Reconstruct back to properties
            let restored = parsed.into_properties().unwrap();
            assert_eq!(restored.len(), 2);

            let key1 = GraphPropertyKey { g_id: 0, p_id: 1 };
            let key2 = GraphPropertyKey { g_id: 0, p_id: 2 };
            assert!(restored.contains_key(&key1));
            assert!(restored.contains_key(&key2));
            assert_eq!(restored[&key1].count, 5);
            assert_eq!(restored[&key2].count, 3);
            assert_eq!(restored[&key1].last_modified_t, 10);
            assert_eq!(restored[&key2].last_modified_t, 8);
        }

        #[test]
        fn test_sketch_blob_empty() {
            let empty: HashMap<GraphPropertyKey, IdPropertyHll> = HashMap::new();
            let blob = HllSketchBlob::from_properties(5, &empty);

            assert_eq!(blob.entries.len(), 0);
            let bytes = blob.to_json_bytes().unwrap();
            let parsed = HllSketchBlob::from_json_bytes(&bytes).unwrap();
            assert_eq!(parsed.entries.len(), 0);

            let restored = parsed.into_properties().unwrap();
            assert!(restored.is_empty());
        }

        #[test]
        fn test_sketch_blob_deterministic() {
            let props = make_test_properties();
            let bytes1 = HllSketchBlob::from_properties(10, &props)
                .to_json_bytes()
                .unwrap();
            let bytes2 = HllSketchBlob::from_properties(10, &props)
                .to_json_bytes()
                .unwrap();
            assert_eq!(bytes1, bytes2, "same input must produce identical bytes");
        }

        #[test]
        fn test_sketch_blob_hll_fidelity() {
            let props = make_test_properties();
            let blob = HllSketchBlob::from_properties(10, &props);
            let bytes = blob.to_json_bytes().unwrap();
            let restored = HllSketchBlob::from_json_bytes(&bytes)
                .unwrap()
                .into_properties()
                .unwrap();

            let key = GraphPropertyKey { g_id: 0, p_id: 1 };
            let original = &props[&key];
            let round_tripped = &restored[&key];

            // HLL registers should be identical after round-trip
            assert_eq!(
                original.values_hll.registers(),
                round_tripped.values_hll.registers(),
                "values HLL registers must survive round-trip"
            );
            assert_eq!(
                original.subjects_hll.registers(),
                round_tripped.subjects_hll.registers(),
                "subjects HLL registers must survive round-trip"
            );
        }

        #[test]
        fn test_sketch_blob_clamps_negatives() {
            let mut map = HashMap::new();
            let mut hll = IdPropertyHll::new();
            hll.count = -3; // negative from retractions
            *hll.datatypes.entry(3).or_insert(0) = -2; // negative dt
            hll.last_modified_t = 5;
            map.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll);

            let blob = HllSketchBlob::from_properties(5, &map);
            assert_eq!(blob.entries[0].count, 0, "negative count clamped to 0");
            assert!(
                blob.entries[0].datatypes.is_empty()
                    || blob.entries[0].datatypes.iter().all(|(_, v)| *v == 0),
                "negative datatypes clamped to 0"
            );
        }

        #[test]
        fn test_sketch_blob_sorted_by_g_id_p_id() {
            let mut map = HashMap::new();
            // Insert in random order
            for &(g, p) in &[(1, 10), (0, 5), (0, 1), (1, 2), (0, 10)] {
                let mut hll = IdPropertyHll::new();
                hll.count = 1;
                hll.last_modified_t = 1;
                map.insert(GraphPropertyKey { g_id: g, p_id: p }, hll);
            }

            let blob = HllSketchBlob::from_properties(1, &map);
            let keys: Vec<(GraphId, u32)> = blob.entries.iter().map(|e| (e.g_id, e.p_id)).collect();
            assert_eq!(
                keys,
                vec![(0, 1), (0, 5), (0, 10), (1, 2), (1, 10)],
                "entries must be sorted by (g_id, p_id)"
            );
        }

        #[test]
        fn test_sketch_blob_rejects_unknown_version() {
            let json = r#"{"version":99,"index_t":1,"entries":[]}"#;
            let err = HllSketchBlob::from_json_bytes(json.as_bytes());
            assert!(err.is_err());
            let msg = err.unwrap_err().to_string();
            assert!(
                msg.contains("unsupported sketch blob version 99"),
                "unexpected error: {msg}"
            );
        }

        #[test]
        fn test_id_stats_hook_with_prior_properties() {
            let mut prior = HashMap::new();
            let mut hll = IdPropertyHll::new();
            hll.values_hll.insert_hash(100);
            hll.subjects_hll.insert_hash(1000);
            hll.count = 5;
            hll.last_modified_t = 3;
            prior.insert(GraphPropertyKey { g_id: 0, p_id: 1 }, hll);

            let hook = IdStatsHook::with_prior_properties(prior);
            let props = hook.properties();
            assert_eq!(props.len(), 1);

            let key = GraphPropertyKey { g_id: 0, p_id: 1 };
            assert_eq!(props[&key].count, 5);
            assert_eq!(props[&key].last_modified_t, 3);
        }
    }
}

// ============================================================================
// Schema Extraction (separate from HLL stats)
// ============================================================================

/// Schema entry for tracking class/property relationships during extraction
///
/// Tracks the relationships for a single class or property:
/// - `subclass_of`: For classes, the parent classes (rdfs:subClassOf targets)
/// - `parent_props`: For properties, the parent properties (rdfs:subPropertyOf targets)
/// - `child_props`: For properties, the child properties (inverse of subPropertyOf)
#[derive(Debug, Clone, Default)]
pub struct SchemaEntry {
    pub subclass_of: HashSet<Sid>,
    pub parent_props: HashSet<Sid>,
    pub child_props: HashSet<Sid>,
}

/// Schema extractor for tracking class/property hierarchy from flakes
///
/// Watches for rdfs:subClassOf and rdfs:subPropertyOf assertions to build
/// the schema hierarchy. Handles both assertions and retractions.
///
/// # Usage
///
/// ```ignore
/// let mut extractor = SchemaExtractor::new();
/// // Or with prior schema:
/// let mut extractor = SchemaExtractor::from_prior(Some(&db.schema));
///
/// for flake in novelty.iter() {
///     extractor.on_flake(flake);
/// }
///
/// let schema = extractor.finalize(target_t);
/// ```
#[derive(Debug, Default)]
pub struct SchemaExtractor {
    /// Schema entries keyed by class/property SID
    entries: std::collections::HashMap<Sid, SchemaEntry>,
    /// Most recent t for schema modifications
    schema_t: i64,
}

impl SchemaExtractor {
    /// Create a new empty schema extractor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a schema extractor initialized with prior schema
    ///
    /// Used during refresh to incrementally update the schema.
    pub fn from_prior(prior_schema: Option<&IndexSchema>) -> Self {
        if let Some(schema) = prior_schema {
            let mut entries = std::collections::HashMap::new();
            for info in &schema.pred.vals {
                let entry = SchemaEntry {
                    subclass_of: info.subclass_of.iter().cloned().collect(),
                    parent_props: info.parent_props.iter().cloned().collect(),
                    child_props: info.child_props.iter().cloned().collect(),
                };
                entries.insert(info.id.clone(), entry);
            }
            Self {
                entries,
                schema_t: schema.t,
            }
        } else {
            Self::default()
        }
    }

    /// Process a flake, extracting schema relationships
    ///
    /// Watches for:
    /// - `rdfs:subClassOf`: Adds/removes parent class relationship
    /// - `rdfs:subPropertyOf`: Adds/removes parent property and child property relationships
    pub fn on_flake(&mut self, flake: &Flake) {
        // rdfs:subClassOf - track class hierarchy
        if is_rdfs_subclass_of(&flake.p) {
            if let FlakeValue::Ref(parent_sid) = &flake.o {
                let class_entry = self.entries.entry(flake.s.clone()).or_default();

                if flake.op {
                    // Assertion: add parent class
                    class_entry.subclass_of.insert(parent_sid.clone());
                } else {
                    // Retraction: remove parent class
                    class_entry.subclass_of.remove(parent_sid);
                }

                // Update schema t
                if flake.t > self.schema_t {
                    self.schema_t = flake.t;
                }
            }
        }

        // rdfs:subPropertyOf - track property hierarchy (both directions)
        if is_rdfs_subproperty_of(&flake.p) {
            if let FlakeValue::Ref(parent_sid) = &flake.o {
                if flake.op {
                    // Assertion: add parent property to subject, add child to parent
                    let prop_entry = self.entries.entry(flake.s.clone()).or_default();
                    prop_entry.parent_props.insert(parent_sid.clone());

                    let parent_entry = self.entries.entry(parent_sid.clone()).or_default();
                    parent_entry.child_props.insert(flake.s.clone());
                } else {
                    // Retraction: remove relationships
                    if let Some(prop_entry) = self.entries.get_mut(&flake.s) {
                        prop_entry.parent_props.remove(parent_sid);
                    }
                    if let Some(parent_entry) = self.entries.get_mut(parent_sid) {
                        parent_entry.child_props.remove(&flake.s);
                    }
                }

                // Update schema t
                if flake.t > self.schema_t {
                    self.schema_t = flake.t;
                }
            }
        }
    }

    /// Finalize extraction and produce IndexSchema
    ///
    /// Returns None if no schema relationships were found.
    /// The schema's t is set to the maximum t seen during extraction,
    /// or falls back to `fallback_t` if no schema flakes were processed.
    pub fn finalize(self, fallback_t: i64) -> Option<IndexSchema> {
        // Filter out empty entries (all relationships removed)
        let vals: Vec<SchemaPredicateInfo> = self
            .entries
            .into_iter()
            .filter(|(_, entry)| {
                // Keep entries that have at least one relationship
                !entry.subclass_of.is_empty()
                    || !entry.parent_props.is_empty()
                    || !entry.child_props.is_empty()
            })
            .map(|(sid, entry)| SchemaPredicateInfo {
                id: sid,
                subclass_of: entry.subclass_of.into_iter().collect(),
                parent_props: entry.parent_props.into_iter().collect(),
                child_props: entry.child_props.into_iter().collect(),
            })
            .collect();

        if vals.is_empty() {
            None
        } else {
            Some(IndexSchema {
                t: if self.schema_t > 0 {
                    self.schema_t
                } else {
                    fallback_t
                },
                pred: SchemaPredicates {
                    keys: vec![
                        "id".to_string(),
                        "subclassOf".to_string(),
                        "parentProps".to_string(),
                        "childProps".to_string(),
                    ],
                    vals,
                },
            })
        }
    }

    /// Check if any schema relationships have been extracted
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// =============================================================================
// Class-Property Statistics Extractor
// =============================================================================

use fluree_db_core::{ClassPropertyUsage, ClassStatEntry};

/// Tracks property usage per class from novelty flakes
///
/// This extracts class-property statistics matching Clojure's `compute-class-property-stats-from-novelty`:
/// - Tracks rdf:type flakes to build subject→class mapping
/// - For each property used by a subject, tracks usage per class:
///   - Datatypes used (e.g., xsd:string, xsd:integer)
///   - Referenced classes (for @id refs)
///   - Language tags (for rdf:langString)
///
/// # Usage
///
/// ```ignore
/// let mut extractor = ClassPropertyExtractor::new();
/// // Or with prior stats:
/// let mut extractor = ClassPropertyExtractor::from_prior(db.stats.as_ref());
///
/// // First pass: collect rdf:type flakes to build subject→class map
/// for flake in novelty.iter() {
///     extractor.collect_type_flake(flake);
/// }
///
/// // Second pass: process all flakes with subject→class context
/// for flake in novelty.iter() {
///     extractor.process_flake(flake);
/// }
///
/// let class_stats = extractor.finalize();
/// ```
#[derive(Debug, Default)]
pub struct ClassPropertyExtractor {
    /// Subject SID → set of class SIDs (from rdf:type assertions in novelty)
    subject_classes: std::collections::HashMap<Sid, std::collections::HashSet<Sid>>,
    /// Class SID → class data (instance count, property usage)
    class_data: std::collections::HashMap<Sid, ClassData>,
}

/// Internal class data during extraction
#[derive(Debug, Default)]
struct ClassData {
    /// Number of instances of this class (delta from novelty)
    count_delta: i64,
    /// Property usage: property SID → property data
    properties: std::collections::HashMap<Sid, PropertyData>,
}

/// Internal property usage data during extraction
#[derive(Debug, Default)]
struct PropertyData {
    /// Count of asserted flakes for this (class, property) pair (delta).
    ///
    /// We intentionally do NOT track datatype/ref/lang breakdowns here; those live
    /// in graph-scoped property stats (`IndexStats.graphs[*].properties`).
    count_delta: i64,
}

impl ClassPropertyExtractor {
    /// Create a new empty class-property extractor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an extractor initialized with prior class stats
    ///
    /// Used during refresh to incrementally update class-property stats.
    pub fn from_prior(prior_stats: Option<&fluree_db_core::IndexStats>) -> Self {
        if let Some(stats) = prior_stats {
            if let Some(ref classes) = stats.classes {
                let mut class_data = std::collections::HashMap::new();
                for class_entry in classes {
                    let mut props = std::collections::HashMap::new();
                    for prop_usage in &class_entry.properties {
                        // Prior stats only carry the property identity. Treat presence as 1 so we
                        // preserve the property list, but do not attempt to “undo” it on retractions.
                        let prop_data = PropertyData { count_delta: 1 };
                        props.insert(prop_usage.property_sid.clone(), prop_data);
                    }
                    let data = ClassData {
                        count_delta: class_entry.count as i64,
                        properties: props,
                    };
                    class_data.insert(class_entry.class_sid.clone(), data);
                }
                return Self {
                    subject_classes: std::collections::HashMap::new(),
                    class_data,
                };
            }
        }
        Self::default()
    }

    /// Collect rdf:type flakes to build subject→class mapping
    ///
    /// Call this for all novelty flakes BEFORE calling `process_flake`.
    /// Only processes rdf:type flakes; other flakes are ignored.
    pub fn collect_type_flake(&mut self, flake: &Flake) {
        if !is_rdf_type(&flake.p) {
            return;
        }

        if let FlakeValue::Ref(class_sid) = &flake.o {
            let classes = self.subject_classes.entry(flake.s.clone()).or_default();

            if flake.op {
                // Assertion: subject is instance of class
                classes.insert(class_sid.clone());
                // Update class count
                let class_data = self.class_data.entry(class_sid.clone()).or_default();
                class_data.count_delta += 1;
            } else {
                // Retraction: subject is no longer instance of class
                classes.remove(class_sid);
                if let Some(class_data) = self.class_data.get_mut(class_sid) {
                    class_data.count_delta -= 1;
                }
            }
        }
    }

    /// Process a flake to update class-property stats
    ///
    /// Must be called AFTER `collect_type_flake` has processed all novelty flakes.
    /// Uses the subject→class mapping to attribute property usage to classes.
    ///
    /// Skips rdf:type flakes (already processed in collect_type_flake).
    pub fn process_flake(&mut self, flake: &Flake) {
        // Skip rdf:type flakes (handled separately)
        if is_rdf_type(&flake.p) {
            return;
        }

        // Get classes for this subject
        let classes = match self.subject_classes.get(&flake.s) {
            Some(c) if !c.is_empty() => c.clone(),
            _ => return, // No classes for this subject - skip
        };

        let delta = if flake.op { 1i64 } else { -1i64 };

        // For each class the subject belongs to, track property usage
        for class_sid in classes {
            let class_data = self.class_data.entry(class_sid).or_default();

            let prop_data = class_data.properties.entry(flake.p.clone()).or_default();

            // Track presence for this (class, property) pair.
            //
            // NOTE: If a property is fully retracted from a class, we do not have enough
            // baseline information to reliably remove it without an expensive index walk,
            // so this list is treated as a conservative superset.
            prop_data.count_delta += delta;
        }
    }

    /// Finalize and return class statistics
    ///
    /// Returns None if no class data was collected.
    pub fn finalize(self) -> Option<Vec<ClassStatEntry>> {
        if self.class_data.is_empty() {
            return None;
        }

        // Convert to sorted output (determinism)
        let mut entries: Vec<ClassStatEntry> = self
            .class_data
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0 || !data.properties.is_empty())
            .map(|(class_sid, data)| {
                // Convert properties (sorted by property SID)
                let mut properties: Vec<ClassPropertyUsage> = data
                    .properties
                    .into_iter()
                    .filter(|(_, prop_data)| prop_data.count_delta > 0)
                    .map(|(property_sid, _prop_data)| ClassPropertyUsage {
                        property_sid,
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: Vec::new(),
                    })
                    .collect();

                // Sort properties by SID for determinism
                properties.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));

                ClassStatEntry {
                    class_sid,
                    count: data.count_delta.max(0) as u64,
                    properties,
                }
            })
            .collect();

        // Sort by class SID for determinism
        entries.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));

        if entries.is_empty() {
            None
        } else {
            Some(entries)
        }
    }

    /// Check if any class data has been collected
    pub fn is_empty(&self) -> bool {
        self.class_data.is_empty() && self.subject_classes.is_empty()
    }

    /// Merge subject→class mapping from external lookup
    ///
    /// Used to incorporate class mappings retrieved from the index
    /// (subjects whose rdf:type was asserted in prior transactions).
    pub fn merge_subject_classes(
        &mut self,
        index_classes: std::collections::HashMap<Sid, HashSet<Sid>>,
    ) {
        for (subject, classes) in index_classes {
            // Only add if not already in novelty (novelty takes precedence)
            self.subject_classes.entry(subject).or_insert(classes);
        }
    }
}

// =============================================================================
// Batched PSOT Lookup for Class Retrieval
// =============================================================================

use fluree_db_core::comparator::IndexType;
use fluree_db_core::db::LedgerSnapshot;
use fluree_db_core::range::{range, RangeMatch, RangeOptions, RangeTest};
use fluree_vocab::predicates::RDF_TYPE;

/// Result of class-property stats computation
#[derive(Debug, Default)]
pub struct ClassPropertyStatsResult {
    /// Class statistics (sorted for determinism)
    pub classes: Option<Vec<fluree_db_core::ClassStatEntry>>,
}

/// Batch lookup subject classes from PSOT index
///
/// Queries the PSOT index for rdf:type predicate to find class memberships
/// for the given subjects. This is used when subjects have their rdf:type
/// asserted in prior transactions (not in current novelty).
///
/// Returns {subject_sid -> HashSet<class_sid>} mapping.
pub async fn batch_lookup_subject_classes(
    db: &LedgerSnapshot,
    g_id: GraphId,
    subjects: &HashSet<Sid>,
) -> crate::error::Result<std::collections::HashMap<Sid, HashSet<Sid>>> {
    if subjects.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    // Query PSOT for rdf:type predicate.
    //
    // Prefer an index-native batched predicate+subject lookup when available to avoid
    // scanning the full rdf:type predicate partition.
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    if let Some(provider) = db.range_provider.as_ref() {
        let subj_vec: Vec<Sid> = subjects.iter().cloned().collect();
        let opts = RangeOptions::new().with_to_t(db.t);
        let no_overlay = fluree_db_core::overlay::NoOverlay;
        match provider.lookup_subject_predicate_refs_batched(
            g_id,
            IndexType::Psot,
            &rdf_type_sid,
            &subj_vec,
            &opts,
            &no_overlay,
        ) {
            Ok(map) => {
                let mut out: std::collections::HashMap<Sid, HashSet<Sid>> =
                    std::collections::HashMap::with_capacity(map.len());
                for (s, classes) in map {
                    out.insert(s, classes.into_iter().collect());
                }
                return Ok(out);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                // Fall through to full predicate scan.
            }
            Err(e) => {
                return Err(crate::error::IndexerError::InvalidConfig(format!(
                    "batched class lookup failed: {}",
                    e
                )));
            }
        }
    }

    // Fallback: full predicate scan (correct but potentially expensive).
    let match_val = RangeMatch::predicate(rdf_type_sid);
    let opts = RangeOptions::default();
    let flakes = range(db, g_id, IndexType::Psot, RangeTest::Eq, match_val, opts)
        .await
        .map_err(|e| {
            crate::error::IndexerError::InvalidConfig(format!("PSOT range query failed: {}", e))
        })?;

    let mut result: std::collections::HashMap<Sid, HashSet<Sid>> = std::collections::HashMap::new();
    for flake in flakes {
        if subjects.contains(&flake.s) && flake.op {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                result
                    .entry(flake.s.clone())
                    .or_default()
                    .insert(class_sid.clone());
            }
        }
    }

    Ok(result)
}

/// Compute class-property statistics in parallel with index refresh
///
/// This function:
/// 1. Collects unique subjects from novelty
/// 2. Identifies subjects needing index lookup (no rdf:type in novelty)
/// 3. Queries PSOT index for missing class memberships
/// 4. Processes all novelty flakes to build class-property stats
///
/// Returns class statistics ready for inclusion in db-root.
pub async fn compute_class_property_stats_parallel(
    db: &LedgerSnapshot,
    g_id: GraphId,
    prior_stats: Option<&fluree_db_core::IndexStats>,
    novelty_flakes: &[Flake],
) -> crate::error::Result<ClassPropertyStatsResult> {
    if novelty_flakes.is_empty() {
        // No novelty - preserve prior classes
        return Ok(ClassPropertyStatsResult {
            classes: prior_stats.and_then(|s| s.classes.clone()),
        });
    }

    // Phase 1: Initialize extractor with prior stats and collect novelty type flakes.
    //
    // IMPORTANT (Clojure parity):
    // We must NOT treat "rdf:type present in novelty" as replacing prior class membership.
    // Instead, we:
    // - fetch the base classes for the subject from the persisted PSOT index (assertions only)
    // - apply novelty rdf:type asserts/retracts as a delta on top of that base set
    //
    // This matches Clojure's `batched-get-subject-classes` which applies `apply-type-novelty`
    // to the base class set returned from PSOT.
    let mut extractor = ClassPropertyExtractor::from_prior(prior_stats);

    // Collect rdf:type flakes first (builds subject→class mapping from novelty)
    for flake in novelty_flakes {
        extractor.collect_type_flake(flake);
    }

    // Phase 2: Collect subjects we may need class membership for (subjects present in novelty).
    let novelty_subjects: HashSet<Sid> = novelty_flakes.iter().map(|f| f.s.clone()).collect();

    let all_subjects_to_lookup: HashSet<Sid> = novelty_subjects;

    // Phase 3: Build novelty rdf:type deltas by subject.
    // We apply these to the base class set from PSOT (assertions only).
    let mut type_novelty_by_subject: std::collections::HashMap<Sid, Vec<(Sid, bool)>> =
        std::collections::HashMap::new();
    for flake in novelty_flakes {
        if is_rdf_type(&flake.p) {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                type_novelty_by_subject
                    .entry(flake.s.clone())
                    .or_default()
                    .push((class_sid.clone(), flake.op));
            }
        }
    }

    // Phase 4: Batch lookup base classes from PSOT index (async), then apply novelty deltas.
    //
    // NOTE: Like Clojure, base lookup uses only assertions from the persisted index.
    // It does NOT collapse historical retractions inside the index itself.
    let mut subject_classes: std::collections::HashMap<Sid, HashSet<Sid>> =
        if all_subjects_to_lookup.is_empty() {
            std::collections::HashMap::new()
        } else {
            batch_lookup_subject_classes(db, g_id, &all_subjects_to_lookup).await?
        };

    // Apply rdf:type novelty deltas to base membership (assert adds, retract removes).
    for (subject, deltas) in type_novelty_by_subject {
        let classes = subject_classes.entry(subject).or_default();
        for (class_sid, op) in deltas {
            if op {
                classes.insert(class_sid);
            } else {
                classes.remove(&class_sid);
            }
        }
    }

    // Install the final subject→classes map into the extractor (used by process_flake).
    extractor.subject_classes = subject_classes;

    // Phase 5: Process all novelty flakes with complete subject→class mapping
    for flake in novelty_flakes {
        extractor.process_flake(flake);
    }

    // Finalize and return
    let classes = extractor.finalize();

    Ok(ClassPropertyStatsResult { classes })
}

// class_property_stats_tests removed: depended on deleted builder module.
// Needs rewrite for binary pipeline.

#[cfg(test)]
mod schema_tests {
    use super::*;

    fn make_schema_flake(
        subject: &str,
        predicate_ns: u16,
        predicate_name: &str,
        object: &str,
        t: i64,
        op: bool,
    ) -> Flake {
        Flake::new(
            Sid::new(100, subject),
            Sid::new(predicate_ns, predicate_name),
            FlakeValue::Ref(Sid::new(100, object)),
            Sid::new(0, ""), // dt not relevant for schema
            t,
            op,
            None,
        )
    }

    #[test]
    fn test_schema_extractor_subclass_of() {
        let mut extractor = SchemaExtractor::new();

        // Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            1,
            true,
        ));

        // Student rdfs:subClassOf Person
        extractor.on_flake(&make_schema_flake(
            "Student",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Person",
            2,
            true,
        ));

        let schema = extractor.finalize(2).expect("should have schema");
        assert_eq!(schema.t, 2);
        assert_eq!(schema.pred.vals.len(), 2);

        // Find Person entry
        let person = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Person")
            .expect("Person should exist");
        assert_eq!(person.subclass_of.len(), 1);
        assert_eq!(person.subclass_of[0].name.as_ref(), "Thing");

        // Find Student entry
        let student = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Student")
            .expect("Student should exist");
        assert_eq!(student.subclass_of.len(), 1);
        assert_eq!(student.subclass_of[0].name.as_ref(), "Person");
    }

    #[test]
    fn test_schema_extractor_subproperty_of() {
        let mut extractor = SchemaExtractor::new();

        // givenName rdfs:subPropertyOf name
        extractor.on_flake(&make_schema_flake(
            "givenName",
            fluree_vocab::namespaces::RDFS,
            "subPropertyOf",
            "name",
            1,
            true,
        ));

        let schema = extractor.finalize(1).expect("should have schema");
        assert_eq!(schema.pred.vals.len(), 2); // givenName and name

        // givenName should have name as parent
        let given_name = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "givenName")
            .expect("givenName should exist");
        assert_eq!(given_name.parent_props.len(), 1);
        assert_eq!(given_name.parent_props[0].name.as_ref(), "name");

        // name should have givenName as child
        let name = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "name")
            .expect("name should exist");
        assert_eq!(name.child_props.len(), 1);
        assert_eq!(name.child_props[0].name.as_ref(), "givenName");
    }

    #[test]
    fn test_schema_extractor_retraction() {
        let mut extractor = SchemaExtractor::new();

        // Assert Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            1,
            true,
        ));

        // Retract Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            2,
            false,
        ));

        // Schema should be empty now
        let schema = extractor.finalize(2);
        assert!(schema.is_none(), "schema should be empty after retraction");
    }

    #[test]
    fn test_schema_extractor_from_prior() {
        // Create prior schema with Person -> Thing
        let prior = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: vec![SchemaPredicateInfo {
                    id: Sid::new(100, "Person"),
                    subclass_of: vec![Sid::new(100, "Thing")],
                    parent_props: vec![],
                    child_props: vec![],
                }],
            },
        };

        let mut extractor = SchemaExtractor::from_prior(Some(&prior));

        // Add Student -> Person
        extractor.on_flake(&make_schema_flake(
            "Student",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Person",
            2,
            true,
        ));

        let schema = extractor.finalize(2).expect("should have schema");
        assert_eq!(schema.t, 2);
        assert_eq!(schema.pred.vals.len(), 2); // Person and Student

        // Person should still have Thing as parent
        let person = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Person")
            .expect("Person should exist");
        assert_eq!(person.subclass_of.len(), 1);
        assert_eq!(person.subclass_of[0].name.as_ref(), "Thing");
    }

    #[test]
    fn test_schema_extractor_empty() {
        let extractor = SchemaExtractor::new();
        let schema = extractor.finalize(1);
        assert!(schema.is_none());
    }
}

/// Build JSON array for class→property→datatype stats from SPOT merge results.
///
/// Resolves class sid64 → (ns_code, suffix) via targeted binary search into
/// flat dict files (avoids loading the full BinaryIndexStore).
///
/// Shared between the import pipeline and rebuild pipeline.
pub fn build_class_stats_json(
    cs: &crate::run_index::SpotClassStats,
    predicate_sids: &[(u16, String)],
    dt_tags: &[ValueTypeTag],
    run_dir: &std::path::Path,
    namespace_codes: &HashMap<u16, String>,
) -> std::io::Result<Vec<serde_json::Value>> {
    use crate::run_index::dict_io;
    use crate::run_index::DT_REF_ID;
    use fluree_db_core::subject_id::SubjectId;
    use std::io::{Read as _, Seek as _, SeekFrom};

    if cs.class_counts.is_empty() {
        return Ok(Vec::new());
    }

    let sids_path = run_dir.join("subjects.sids");
    let idx_path = run_dir.join("subjects.idx");
    let fwd_path = run_dir.join("subjects.fwd");

    let sids_vec = dict_io::read_subject_sid_map(&sids_path)?;
    let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path)?;
    let mut fwd_file = std::fs::File::open(&fwd_path)?;

    // Helper: resolve sid64 → (ns_code, suffix_string).
    // subjects.sids is sorted (both vocab_merge and persist_merge_artifacts
    // guarantee monotonic sid64 order), so binary_search is safe.
    let resolve_sid = |sid64: u64, file: &mut std::fs::File| -> Option<(u16, String)> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some((ns_code, suffix.to_string()))
    };

    // Sort class entries by (g_id, class_sid64) for deterministic output.
    let mut class_entries: Vec<(&(GraphId, u64), &u64)> = cs.class_counts.iter().collect();
    class_entries.sort_by_key(|&(key, _)| *key);

    // Per-class ref-target data (if available).
    let class_refs = &cs.class_prop_refs;

    let classes_json: Vec<serde_json::Value> = class_entries
        .iter()
        .filter_map(|&(&(g_id, class_sid64), &count)| {
            let (ns_code, suffix) = resolve_sid(class_sid64, &mut fwd_file)?;

            // Look up this class's ref-target map (if any).
            let ref_map = class_refs.get(&(g_id, class_sid64));

            // Build property entries. Each property gets dt breakdown, and
            // optionally ref-target class info using the DB-R extended object
            // format: {"ref-classes": [[[ns, suffix], count], ...]}.
            let prop_json: Vec<serde_json::Value> = if let Some(prop_map) =
                cs.class_prop_dts.get(&(g_id, class_sid64))
            {
                let mut props: Vec<_> = prop_map.iter().collect();
                props.sort_by_key(|&(pid, _)| *pid);

                props
                    .iter()
                    .filter_map(|&(&p_id, dt_map)| {
                        let psid = predicate_sids.get(p_id as usize)?;

                        // Check if this property has ref-target class data.
                        let prop_refs = ref_map.and_then(|rm| rm.get(&p_id));

                        if let Some(target_map) = prop_refs {
                            // Emit DB-R extended object with ref-classes.
                            let mut targets: Vec<_> = target_map.iter().collect();
                            targets.sort_by_key(|&(sid, _)| *sid);
                            let refs_json: Vec<serde_json::Value> = targets
                                .iter()
                                .filter_map(|&(&target_sid, &tcount)| {
                                    let (tns, tsuffix) = resolve_sid(target_sid, &mut fwd_file)?;
                                    Some(serde_json::json!([[tns, tsuffix], tcount]))
                                })
                                .collect();
                            Some(serde_json::json!(
                                [[psid.0, &psid.1], {"ref-classes": refs_json}]
                            ))
                        } else {
                            // Standard format: property with datatype counts.
                            let mut dts: Vec<_> = dt_map.iter().collect();
                            dts.sort_by_key(|&(dt, _)| *dt);
                            let dt_json: Vec<serde_json::Value> = dts
                                .iter()
                                .map(|&(&dt, &count)| {
                                    if dt == DT_REF_ID {
                                        serde_json::json!(["@id", count])
                                    } else if let Some(tag) = dt_tags.get(dt as usize) {
                                        serde_json::json!([tag.as_u8(), count])
                                    } else {
                                        serde_json::json!([dt, count])
                                    }
                                })
                                .collect();
                            Some(serde_json::json!([[psid.0, &psid.1], dt_json]))
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            Some(serde_json::json!([[ns_code, suffix], [count, prop_json]]))
        })
        .collect();

    tracing::info!(classes = classes_json.len(), "class stats resolved to JSON");

    Ok(classes_json)
}

/// Build `ClassStatEntry` structs from SPOT class stats (struct-based, no JSON).
///
/// Returns a per-graph map: `GraphId → Vec<ClassStatEntry>`. Each graph gets its
/// own class stats reflecting only the subjects and properties within that graph.
///
/// Parallel to `build_class_stats_json` but returns typed structs suitable for
/// binary stats encoding in `IndexRootV5`.
pub fn build_class_stat_entries(
    cs: &crate::run_index::SpotClassStats,
    predicate_sids: &[(u16, String)],
    dt_tags: &[ValueTypeTag],
    language_tags: &[String],
    run_dir: &std::path::Path,
    namespace_codes: &HashMap<u16, String>,
) -> std::io::Result<HashMap<GraphId, Vec<fluree_db_core::ClassStatEntry>>> {
    use crate::run_index::dict_io;
    use fluree_db_core::sid::Sid;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ClassPropertyUsage, ClassRefCount, ClassStatEntry};
    use std::io::{Read as _, Seek as _, SeekFrom};

    if cs.class_counts.is_empty() {
        return Ok(HashMap::new());
    }

    let sids_path = run_dir.join("subjects.sids");
    let idx_path = run_dir.join("subjects.idx");
    let fwd_path = run_dir.join("subjects.fwd");

    let sids_vec = dict_io::read_subject_sid_map(&sids_path)?;
    let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path)?;
    let mut fwd_file = std::fs::File::open(&fwd_path)?;

    let resolve_sid = |sid64: u64, file: &mut std::fs::File| -> Option<Sid> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some(Sid::new(ns_code, suffix))
    };

    // Sort by (g_id, class_sid64) for deterministic output.
    let mut class_entries: Vec<(&(GraphId, u64), &u64)> = cs.class_counts.iter().collect();
    class_entries.sort_by_key(|&(key, _)| *key);

    let class_refs = &cs.class_prop_refs;

    let mut per_graph: HashMap<GraphId, Vec<ClassStatEntry>> = HashMap::new();

    for &(&(g_id, class_sid64), &count) in &class_entries {
        let class_sid = match resolve_sid(class_sid64, &mut fwd_file) {
            Some(s) => s,
            None => continue,
        };
        let ref_map = class_refs.get(&(g_id, class_sid64));

        let properties: Vec<ClassPropertyUsage> =
            if let Some(prop_map) = cs.class_prop_dts.get(&(g_id, class_sid64)) {
                let mut props: Vec<_> = prop_map.iter().collect();
                props.sort_by_key(|&(pid, _)| *pid);

                props
                    .iter()
                    .filter_map(|&(&p_id, dt_map)| {
                        let psid_pair = predicate_sids.get(p_id as usize)?;
                        let property_sid = Sid::new(psid_pair.0, &psid_pair.1);

                        // Build per-datatype counts.
                        let mut datatypes: Vec<(u8, u64)> = dt_map
                            .iter()
                            .map(|(&dt_dict_id, &count)| {
                                let tag = if dt_dict_id == crate::run_index::DT_REF_ID {
                                    fluree_db_core::value_id::ValueTypeTag::JSON_LD_ID.as_u8()
                                } else {
                                    dt_tags
                                        .get(dt_dict_id as usize)
                                        .map(|t| t.as_u8())
                                        .unwrap_or(
                                            fluree_db_core::value_id::ValueTypeTag::UNKNOWN.as_u8(),
                                        )
                                };
                                (tag, count)
                            })
                            .collect();
                        datatypes.sort_by_key(|d| d.0);

                        // Build per-language-tag counts.
                        let lang_map = cs.class_prop_langs.get(&(g_id, class_sid64));
                        let langs: Vec<(String, u64)> =
                            if let Some(prop_langs) = lang_map.and_then(|lm| lm.get(&p_id)) {
                                let mut lv: Vec<(String, u64)> = prop_langs
                                    .iter()
                                    .filter_map(|(&lang_id, &count)| {
                                        // lang_id is 1-indexed; 0 = no language tag.
                                        let lang_str = language_tags
                                            .get((lang_id as usize).wrapping_sub(1))?;
                                        Some((lang_str.clone(), count))
                                    })
                                    .collect();
                                lv.sort_by(|a, b| a.0.cmp(&b.0));
                                lv
                            } else {
                                Vec::new()
                            };

                        // Ref-class targets for this property (if any).
                        let ref_classes: Vec<ClassRefCount> =
                            if let Some(target_map) = ref_map.and_then(|rm| rm.get(&p_id)) {
                                let mut targets: Vec<_> = target_map.iter().collect();
                                targets.sort_by_key(|&(sid, _)| *sid);
                                targets
                                    .iter()
                                    .filter_map(|&(&target_sid, &tcount)| {
                                        let tsid = resolve_sid(target_sid, &mut fwd_file)?;
                                        Some(ClassRefCount {
                                            class_sid: tsid,
                                            count: tcount,
                                        })
                                    })
                                    .collect()
                            } else {
                                Vec::new()
                            };

                        Some(ClassPropertyUsage {
                            property_sid,
                            datatypes,
                            langs,
                            ref_classes,
                        })
                    })
                    .collect()
            } else {
                Vec::new()
            };

        per_graph.entry(g_id).or_default().push(ClassStatEntry {
            class_sid,
            count,
            properties,
        });
    }

    let total_classes: usize = per_graph.values().map(|v| v.len()).sum();
    tracing::info!(
        classes = total_classes,
        graphs = per_graph.len(),
        "class stats resolved to per-graph entries"
    );
    Ok(per_graph)
}
