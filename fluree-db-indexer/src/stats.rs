//! Index statistics hooks
//!
//! Provides a hook interface for collecting statistics during index building.
//! - `NoOpStatsHook`: Minimal implementation that only counts flakes
//! - `HllStatsHook`: Full HLL-based per-property NDV tracking
//!
//! ## HLL Sketch Persistence
//!
//! HLL sketches are persisted to storage
//! using T-based filenames. This enables true incremental stats updates during refresh:
//!
//! - Path: `<alias>/index/stats-sketches/{values|subjects}/<ns>_<name>_<t>.hll`
//! - On refresh: load prior sketches, merge with novelty, persist new sketches
//! - NDV is monotone: never decreases even with retractions
//!
//! ## Sketch Persistence API
//!
//! ```ignore
//! // Write sketches to storage (uses each property's last_modified_t)
//! let addresses = persist_hll_sketches(&storage, &alias, &properties).await?;
//!
//! // Load sketches from storage (uses each property's last_modified_t from entries)
//! let properties = load_hll_sketches(&storage, &alias, &property_entries).await?;
//! ```

use fluree_db_core::Flake;
use fluree_db_core::Sid;
use fluree_db_core::Storage;
use fluree_vocab::namespaces::RDF;
use std::collections::HashSet;

use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;
use fluree_db_core::PropertyStatEntry;
use fluree_db_core::StorageWrite;
use std::collections::HashMap;

use fluree_db_core::{GraphPropertyStatEntry, GraphStatsEntry};
use fluree_db_core::value_id::ValueTypeTag;
use xxhash_rust::xxh64::xxh64;

// Schema extraction imports (always available, not feature-gated)
use fluree_db_core::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
use fluree_db_core::{is_rdf_type, is_rdfs_subclass_of, is_rdfs_subproperty_of, FlakeValue};

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
    /// Addresses of persisted stats files (e.g., HLL sketches)
    pub artifact_addresses: Vec<String>,
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
            artifact_addresses: vec![],
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
pub struct HllStatsHook {
    flake_count: usize,
    /// Per-property HLL sketches, keyed by predicate SID
    properties: HashMap<Sid, PropertyHll>,
}

impl Default for HllStatsHook {
    fn default() -> Self {
        Self {
            flake_count: 0,
            properties: HashMap::new(),
        }
    }
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
        let entry = self.properties
            .entry(flake.p.clone())
            .or_insert_with(PropertyHll::new);

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
        let mut entries: Vec<PropertyStatEntry> = self.properties
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
        entries.sort_by(|a, b| {
            a.sid.0.cmp(&b.sid.0)
                .then_with(|| a.sid.1.cmp(&b.sid.1))
        });

        StatsArtifacts {
            artifact_addresses: vec![], // Sketch addresses added by caller if persisted
            summary: StatsSummary {
                flake_count: self.flake_count,
                properties: if entries.is_empty() { None } else { Some(entries) },
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
    pub g_id: u32,
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
/// hook.on_record(g_id, p_id, s_id, dt, value_hash(o_kind, o_key), t, op);
/// // After all ops:
/// let result = hook.finalize();
/// ```
#[derive(Debug)]
pub struct IdStatsHook {
    flake_count: usize,
    properties: HashMap<GraphPropertyKey, IdPropertyHll>,
    /// Per-graph flake count (signed delta)
    graph_flakes: HashMap<u32, i64>,
}

impl IdStatsHook {
    pub fn new() -> Self {
        Self {
            flake_count: 0,
            properties: HashMap::new(),
            graph_flakes: HashMap::new(),
        }
    }

    /// Process a single record with resolved IDs.
    ///
    /// Called per-op after the resolver maps Sids to numeric IDs.
    ///
    /// # Arguments
    /// * `g_id` - Graph dictionary ID (0 = default)
    /// * `p_id` - Predicate dictionary ID
    /// * `s_id` - Subject dictionary ID
    /// * `dt` - Datatype ID
    /// * `o_hash` - Pre-computed object value hash (from `value_hash()`)
    /// * `t` - Transaction time
    /// * `op` - true = assertion, false = retraction
    pub fn on_record(
        &mut self,
        g_id: u32,
        p_id: u32,
        s_id: u64,
        dt: ValueTypeTag,
        o_hash: u64,
        t: i64,
        op: bool,
    ) {
        self.flake_count += 1;
        let delta: i64 = if op { 1 } else { -1 };

        // Track per-graph flake count
        *self.graph_flakes.entry(g_id).or_insert(0) += delta;

        let key = GraphPropertyKey { g_id, p_id };
        let hll = self.properties.entry(key).or_insert_with(IdPropertyHll::new);

        hll.count += delta;

        // HLL: only insert on assertions (NDV is monotone)
        if op {
            hll.values_hll.insert_hash(o_hash);
            hll.subjects_hll.insert_hash(subject_hash(s_id));
        }

        if t > hll.last_modified_t {
            hll.last_modified_t = t;
        }

        // Track datatype usage
        *hll.datatypes.entry(dt.as_u8()).or_insert(0) += delta;
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
    }

    /// Borrow the internal properties map (for sketch persistence before finalize).
    pub fn properties(&self) -> &HashMap<GraphPropertyKey, IdPropertyHll> {
        &self.properties
    }

    /// Total flake count (all graphs, all ops).
    pub fn flake_count(&self) -> usize {
        self.flake_count
    }

    /// Produce per-graph stats and aggregate property stats.
    ///
    /// Excludes txn-meta graph (g_id=1) from both `graphs` and aggregate
    /// `properties`. Clamps all signed deltas to 0.
    pub fn finalize(self) -> IdStatsResult {
        // Group by g_id, then by p_id
        let mut graph_map: HashMap<u32, Vec<(&GraphPropertyKey, &IdPropertyHll)>> = HashMap::new();
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
                    ndv_values: hll.values_hll.estimate() as u64,
                    ndv_subjects: hll.subjects_hll.estimate() as u64,
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                });
            }

            // Sort properties by p_id for determinism
            props.sort_by_key(|p| p.p_id);

            let graph_flake_count = self
                .graph_flakes
                .get(&g_id)
                .copied()
                .unwrap_or(0)
                .max(0) as u64;

            graphs.push(GraphStatsEntry {
                g_id,
                flakes: graph_flake_count,
                size: 0, // Populated by index build, not available here
                properties: props,
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

    /// Finalize into per-graph stats plus a ledger-wide aggregate property view.
    ///
    /// The aggregate view is keyed only by `p_id` (across all graphs), with HLL sketches
    /// merged across graphs so NDV estimates remain meaningful. Datatype counts are summed
    /// across graphs.
    ///
    /// Excludes txn-meta graph (g_id=1) from both per-graph and aggregate results.
    pub fn finalize_with_aggregate_properties(self) -> (IdStatsResult, Vec<GraphPropertyStatEntry>) {
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
                    ndv_values: hll.values_hll.estimate() as u64,
                    ndv_subjects: hll.subjects_hll.estimate() as u64,
                    last_modified_t: hll.last_modified_t,
                    datatypes,
                }
            })
            .collect();

        // Deterministic ordering
        properties.sort_by_key(|p| p.p_id);

        let graphs = self.finalize();
        (graphs, properties)
    }
}

// === HLL Sketch Persistence (Sid-based, legacy) ===

/// Number of registers in HLL sketch (2^8 = 256)
const HLL_REGISTER_COUNT: usize = 256;

/// Generate storage address for HLL sketch
///
/// Uses pattern: `fluree:file://{alias}/index/stats-sketches/{kind}/{ns}_{name}_{t}.hll`
///
/// - `kind`: "values" or "subjects"
/// - `ns`: namespace code (u16)
/// - `name`: predicate local name (URL-encoded if needed)
/// - `t`: transaction time
fn hll_sketch_address(alias: &str, ns_code: u16, name: &str, t: i64, kind: &str) -> String {
    // Sanitize name for use in path (replace problematic characters)
    let safe_name: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();

    format!(
        "fluree:file://{}/index/stats-sketches/{}/{}_{}_t{}.hll",
        alias.replace(':', "/"),
        kind,
        ns_code,
        safe_name,
        t
    )
}

/// Persist HLL sketches to storage
///
/// Writes all property HLL sketches to storage with T-based filenames.
/// Each property's sketch is stored at its own `last_modified_t` (matching Clojure semantics).
/// Returns the list of addresses written.
///
/// # Arguments
/// * `storage` - Storage backend to write to
/// * `alias` - Ledger alias (e.g., "mydb:main")
/// * `properties` - Map of property SIDs to their HLL data (includes last_modified_t)
pub async fn persist_hll_sketches<S: StorageWrite>(
    storage: &S,
    alias: &str,
    properties: &HashMap<Sid, PropertyHll>,
) -> Result<Vec<String>> {
    persist_hll_sketches_iter(storage, alias, properties.iter()).await
}

/// Persist HLL sketches to storage from an iterator
///
/// Like `persist_hll_sketches` but accepts an iterator of (&Sid, &PropertyHll) references.
/// This avoids cloning when persisting a filtered subset of properties.
pub async fn persist_hll_sketches_iter<'a, S, I>(
    storage: &S,
    alias: &str,
    properties: I,
) -> Result<Vec<String>>
where
    S: StorageWrite,
    I: Iterator<Item = (&'a Sid, &'a PropertyHll)>,
{
    let mut addresses = Vec::new();

    for (sid, hll) in properties {
        // Use the property's last_modified_t for the filename
        let t = hll.last_modified_t;

        // Write values sketch
        let values_addr = hll_sketch_address(alias, sid.namespace_code, &sid.name, t, "values");
        let values_bytes = hll.values_hll.to_bytes();
        storage
            .write_bytes(&values_addr, &values_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("Failed to write values HLL: {}", e)))?;
        addresses.push(values_addr);

        // Write subjects sketch
        let subjects_addr = hll_sketch_address(alias, sid.namespace_code, &sid.name, t, "subjects");
        let subjects_bytes = hll.subjects_hll.to_bytes();
        storage
            .write_bytes(&subjects_addr, &subjects_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(format!("Failed to write subjects HLL: {}", e)))?;
        addresses.push(subjects_addr);
    }

    Ok(addresses)
}

/// Load HLL sketches from storage
///
/// Reads all property HLL sketches from storage using each property's `last_modified_t`
/// to locate the correct sketch file (matching Clojure semantics).
/// Returns a map of property SIDs to their HLL data.
///
/// # Arguments
/// * `storage` - Storage backend to read from
/// * `alias` - Ledger alias (e.g., "mydb:main")
/// * `property_entries` - Property stat entries from DbRoot (contains SID, count, last_modified_t)
///
/// # Returns
/// Map of property SIDs to their PropertyHll data with loaded sketches.
/// Properties whose sketches cannot be loaded are still included with empty sketches
/// but their prior count and last_modified_t are preserved (for monotonicity).
pub async fn load_hll_sketches<S: Storage>(
    storage: &S,
    alias: &str,
    property_entries: &[PropertyStatEntry],
) -> Result<HashMap<Sid, PropertyHll>> {
    let mut properties = HashMap::with_capacity(property_entries.len());

    for entry in property_entries {
        let sid = Sid::new(entry.sid.0, &entry.sid.1);

        // Use the property's last_modified_t for the filename
        let t = entry.last_modified_t;

        // Load values sketch
        let values_addr = hll_sketch_address(alias, entry.sid.0, &entry.sid.1, t, "values");
        let values_hll = match storage.read_bytes(&values_addr).await {
            Ok(bytes) if bytes.len() == HLL_REGISTER_COUNT => {
                let mut registers = [0u8; HLL_REGISTER_COUNT];
                registers.copy_from_slice(&bytes);
                HllSketch256::from_bytes(&registers)
            }
            Ok(bytes) => {
                // Wrong size - log warning and use empty sketch
                // Preserving prior stats (count, last_modified_t) maintains monotonicity
                tracing::warn!(
                    sid = ?sid,
                    expected = HLL_REGISTER_COUNT,
                    got = bytes.len(),
                    "Values HLL sketch has wrong size"
                );
                HllSketch256::new()
            }
            Err(_) => {
                // Sketch not found - use empty sketch but preserve prior stats
                HllSketch256::new()
            }
        };

        // Load subjects sketch
        let subjects_addr = hll_sketch_address(alias, entry.sid.0, &entry.sid.1, t, "subjects");
        let subjects_hll = match storage.read_bytes(&subjects_addr).await {
            Ok(bytes) if bytes.len() == HLL_REGISTER_COUNT => {
                let mut registers = [0u8; HLL_REGISTER_COUNT];
                registers.copy_from_slice(&bytes);
                HllSketch256::from_bytes(&registers)
            }
            Ok(bytes) => {
                tracing::warn!(
                    sid = ?sid,
                    expected = HLL_REGISTER_COUNT,
                    got = bytes.len(),
                    "Subjects HLL sketch has wrong size"
                );
                HllSketch256::new()
            }
            Err(_) => {
                HllSketch256::new()
            }
        };

        // Create PropertyHll with loaded sketches (or empty sketches with prior stats)
        // This preserves count and last_modified_t for monotonicity even if sketches were lost
        let hll = PropertyHll::from_sketches(
            entry.count,
            values_hll,
            subjects_hll,
            entry.last_modified_t,
        );

        properties.insert(sid, hll);
    }

    Ok(properties)
}

/// List all HLL sketch addresses for the given properties
///
/// Each property's sketch address uses its own `last_modified_t`.
/// Useful for cleanup/garbage collection.
pub fn list_hll_sketch_addresses(
    alias: &str,
    properties: &HashMap<Sid, PropertyHll>,
) -> Vec<String> {
    let mut addresses = Vec::with_capacity(properties.len() * 2);

    for (sid, hll) in properties {
        let t = hll.last_modified_t;
        addresses.push(hll_sketch_address(alias, sid.namespace_code, &sid.name, t, "values"));
        addresses.push(hll_sketch_address(alias, sid.namespace_code, &sid.name, t, "subjects"));
    }

    addresses
}

/// Compute obsolete HLL sketch addresses after a refresh operation
///
/// Compares prior property stats with updated properties to identify sketches
/// that have been superseded. A sketch is obsolete when:
/// - The property existed in prior stats
/// - The property still exists in updated properties
/// - The `last_modified_t` has changed (property was updated with new flakes)
///
/// If a property was completely removed (not in updated), its sketches are also obsolete.
///
/// # Arguments
/// * `alias` - Ledger alias (e.g., "mydb:main")
/// * `prior_properties` - Property stat entries from prior DbRoot (contains SID, last_modified_t)
/// * `updated_properties` - Updated properties after refresh (contains SID, new last_modified_t)
///
/// # Returns
/// Vector of obsolete sketch addresses (values + subjects for each obsolete property)
pub fn compute_obsolete_sketch_addresses(
    alias: &str,
    prior_properties: &[PropertyStatEntry],
    updated_properties: &HashMap<Sid, PropertyHll>,
) -> Vec<String> {
    let mut obsolete = Vec::new();

    for prior_entry in prior_properties {
        let sid = Sid::new(prior_entry.sid.0, &prior_entry.sid.1);
        let prior_t = prior_entry.last_modified_t;

        // Check if property still exists and if its last_modified_t changed
        let is_obsolete = match updated_properties.get(&sid) {
            Some(updated_hll) => {
                // Property still exists - check if last_modified_t changed
                updated_hll.last_modified_t != prior_t
            }
            None => {
                // Property was completely removed (rare but possible)
                // Its old sketches are obsolete
                true
            }
        };

        if is_obsolete {
            // Add both values and subjects sketch addresses using PRIOR t
            obsolete.push(hll_sketch_address(
                alias,
                prior_entry.sid.0,
                &prior_entry.sid.1,
                prior_t,
                "values",
            ));
            obsolete.push(hll_sketch_address(
                alias,
                prior_entry.sid.0,
                &prior_entry.sid.1,
                prior_t,
                "subjects",
            ));
        }
    }

    obsolete
}

// =============================================================================
// ID-Based HLL Sketch Persistence
// =============================================================================

/// Generate storage address for ID-based HLL sketch.
///
/// Uses pattern: `fluree:file://{alias}/index/stats-sketches/{kind}/g{g_id}/p{p_id}_t{t}.hll`
///
/// Pure numeric IDs — no IRI encoding, no escaping.
fn hll_sketch_address_id(alias: &str, g_id: u32, p_id: u32, t: i64, kind: &str) -> String {
    format!(
        "fluree:file://{}/index/stats-sketches/{}/g{}/p{}_t{}.hll",
        alias.replace(':', "/"),
        kind,
        g_id,
        p_id,
        t
    )
}

/// Persist ID-based HLL sketches to storage.
///
/// Writes all (graph, property) HLL sketches to storage with T-based filenames.
/// Returns the list of addresses written.
pub async fn persist_hll_sketches_id<S: StorageWrite>(
    storage: &S,
    alias: &str,
    properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
) -> Result<Vec<String>> {
    let mut addresses = Vec::new();

    for (key, hll) in properties {
        let t = hll.last_modified_t;

        // Write values sketch
        let values_addr = hll_sketch_address_id(alias, key.g_id, key.p_id, t, "values");
        let values_bytes = hll.values_hll.to_bytes();
        storage
            .write_bytes(&values_addr, &values_bytes)
            .await
            .map_err(|e| {
                IndexerError::StorageWrite(format!("Failed to write values HLL (g{}/p{}): {}", key.g_id, key.p_id, e))
            })?;
        addresses.push(values_addr);

        // Write subjects sketch
        let subjects_addr = hll_sketch_address_id(alias, key.g_id, key.p_id, t, "subjects");
        let subjects_bytes = hll.subjects_hll.to_bytes();
        storage
            .write_bytes(&subjects_addr, &subjects_bytes)
            .await
            .map_err(|e| {
                IndexerError::StorageWrite(format!("Failed to write subjects HLL (g{}/p{}): {}", key.g_id, key.p_id, e))
            })?;
        addresses.push(subjects_addr);
    }

    Ok(addresses)
}

/// Load ID-based HLL sketches from storage.
///
/// Reads all (graph, property) HLL sketches from storage using each entry's
/// `last_modified_t` to locate the correct sketch file.
/// Returns a map of `GraphPropertyKey` to `IdPropertyHll` with loaded sketches.
///
/// Properties whose sketches cannot be loaded are still included with empty
/// sketches but their prior count and last_modified_t are preserved (monotonicity).
pub async fn load_hll_sketches_id<S: Storage>(
    storage: &S,
    alias: &str,
    graph_entries: &[GraphStatsEntry],
) -> Result<HashMap<GraphPropertyKey, IdPropertyHll>> {
    let mut properties = HashMap::new();

    for graph in graph_entries {
        for prop in &graph.properties {
            let key = GraphPropertyKey {
                g_id: graph.g_id,
                p_id: prop.p_id,
            };
            let t = prop.last_modified_t;

            // Load values sketch
            let values_addr = hll_sketch_address_id(alias, graph.g_id, prop.p_id, t, "values");
            let values_hll = match storage.read_bytes(&values_addr).await {
                Ok(bytes) if bytes.len() == HLL_REGISTER_COUNT => {
                    let mut registers = [0u8; HLL_REGISTER_COUNT];
                    registers.copy_from_slice(&bytes);
                    HllSketch256::from_bytes(&registers)
                }
                Ok(bytes) => {
                    tracing::warn!(
                        g_id = graph.g_id,
                        p_id = prop.p_id,
                        expected = HLL_REGISTER_COUNT,
                        got = bytes.len(),
                        "Values HLL sketch has wrong size"
                    );
                    HllSketch256::new()
                }
                Err(_) => HllSketch256::new(),
            };

            // Load subjects sketch
            let subjects_addr =
                hll_sketch_address_id(alias, graph.g_id, prop.p_id, t, "subjects");
            let subjects_hll = match storage.read_bytes(&subjects_addr).await {
                Ok(bytes) if bytes.len() == HLL_REGISTER_COUNT => {
                    let mut registers = [0u8; HLL_REGISTER_COUNT];
                    registers.copy_from_slice(&bytes);
                    HllSketch256::from_bytes(&registers)
                }
                Ok(bytes) => {
                    tracing::warn!(
                        g_id = graph.g_id,
                        p_id = prop.p_id,
                        expected = HLL_REGISTER_COUNT,
                        got = bytes.len(),
                        "Subjects HLL sketch has wrong size"
                    );
                    HllSketch256::new()
                }
                Err(_) => HllSketch256::new(),
            };

            // Reconstruct datatype map from the stat entry
            let datatypes: HashMap<u8, i64> = prop
                .datatypes
                .iter()
                .map(|&(dt, count)| (dt, count as i64))
                .collect();

            let hll = IdPropertyHll::from_sketches(
                prop.count as i64,
                values_hll,
                subjects_hll,
                prop.last_modified_t,
                datatypes,
            );

            properties.insert(key, hll);
        }
    }

    Ok(properties)
}

/// Compute obsolete ID-based HLL sketch addresses after a refresh.
///
/// Compares prior graph stats with updated properties to identify sketches
/// that have been superseded. A sketch is obsolete when the `last_modified_t`
/// has changed for a given `(g_id, p_id)`.
pub fn compute_obsolete_sketch_addresses_id(
    alias: &str,
    prior_graphs: &[GraphStatsEntry],
    updated_properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
) -> Vec<String> {
    let mut obsolete = Vec::new();

    for graph in prior_graphs {
        for prop in &graph.properties {
            let key = GraphPropertyKey {
                g_id: graph.g_id,
                p_id: prop.p_id,
            };
            let prior_t = prop.last_modified_t;

            let is_obsolete = match updated_properties.get(&key) {
                Some(updated_hll) => updated_hll.last_modified_t != prior_t,
                None => true, // Property removed
            };

            if is_obsolete {
                obsolete.push(hll_sketch_address_id(
                    alias,
                    graph.g_id,
                    prop.p_id,
                    prior_t,
                    "values",
                ));
                obsolete.push(hll_sketch_address_id(
                    alias,
                    graph.g_id,
                    prop.p_id,
                    prior_t,
                    "subjects",
                ));
            }
        }
    }

    obsolete
}

/// List all ID-based HLL sketch addresses for the given properties.
///
/// Each property's sketch address uses its own `last_modified_t`.
/// Useful for cleanup/garbage collection.
pub fn list_hll_sketch_addresses_id(
    alias: &str,
    properties: &HashMap<GraphPropertyKey, IdPropertyHll>,
) -> Vec<String> {
    let mut addresses = Vec::with_capacity(properties.len() * 2);

    for (key, hll) in properties {
        let t = hll.last_modified_t;
        addresses.push(hll_sketch_address_id(alias, key.g_id, key.p_id, t, "values"));
        addresses.push(hll_sketch_address_id(alias, key.g_id, key.p_id, t, "subjects"));
    }

    addresses
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
        assert!(artifacts.artifact_addresses.is_empty());
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
                values_est >= 2 && values_est <= 8,
                "values estimate {} should be in range [2, 8]",
                values_est
            );
            assert!(
                subjects_est >= 1 && subjects_est <= 4,
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

        #[test]
        fn test_hll_sketch_address_generation() {
            let addr = super::hll_sketch_address("mydb:main", 100, "name", 42, "values");
            assert_eq!(
                addr,
                "fluree:file://mydb/main/index/stats-sketches/values/100_name_t42.hll"
            );

            // Test with special characters in name (should be sanitized)
            let addr2 = super::hll_sketch_address("test/ledger", 200, "some/prop", 10, "subjects");
            assert_eq!(
                addr2,
                "fluree:file://test/ledger/index/stats-sketches/subjects/200_some_prop_t10.hll"
            );
        }

        #[tokio::test]
        async fn test_persist_and_load_hll_sketches() {
            use fluree_db_core::MemoryStorage;

            let storage = MemoryStorage::new();
            let alias = "test/db";

            // Create some properties with sketches (each has its own last_modified_t)
            let mut properties = HashMap::new();

            let mut hll1 = PropertyHll::new();
            hll1.values_hll.insert_hash(0x1234567890abcdef);
            hll1.values_hll.insert_hash(0xfedcba0987654321);
            hll1.subjects_hll.insert_hash(0xaaaaaaaaaaaaaaaa);
            hll1.count = 10;
            hll1.last_modified_t = 5;  // prop_a was modified at t=5
            properties.insert(Sid::new(100, "prop_a"), hll1);

            let mut hll2 = PropertyHll::new();
            hll2.values_hll.insert_hash(0x1111111111111111);
            hll2.subjects_hll.insert_hash(0x2222222222222222);
            hll2.count = 5;
            hll2.last_modified_t = 3;  // prop_b was modified at t=3
            properties.insert(Sid::new(100, "prop_b"), hll2);

            // Persist sketches (each stored at its own last_modified_t)
            let addresses = super::persist_hll_sketches(&storage, alias, &properties)
                .await
                .expect("persist should succeed");

            // Should have 4 addresses (2 properties * 2 sketches each)
            assert_eq!(addresses.len(), 4);

            // Now load them back using property entries (with matching last_modified_t)
            let property_entries = vec![
                PropertyStatEntry {
                    sid: (100, "prop_a".to_string()),
                    count: 10,
                    ndv_values: 2,
                    ndv_subjects: 1,
                    last_modified_t: 5,  // Must match the t used during persist
                    datatypes: vec![],
                },
                PropertyStatEntry {
                    sid: (100, "prop_b".to_string()),
                    count: 5,
                    ndv_values: 1,
                    ndv_subjects: 1,
                    last_modified_t: 3,  // Must match the t used during persist
                    datatypes: vec![],
                },
            ];

            let loaded = super::load_hll_sketches(&storage, alias, &property_entries)
                .await
                .expect("load should succeed");

            assert_eq!(loaded.len(), 2);

            // Verify prop_a was loaded correctly
            let loaded_a = loaded.get(&Sid::new(100, "prop_a")).expect("prop_a should exist");
            assert_eq!(loaded_a.count, 10);
            assert_eq!(loaded_a.last_modified_t, 5);
            // Verify sketch data was preserved (estimates should match)
            assert_eq!(loaded_a.values_hll.estimate(), properties.get(&Sid::new(100, "prop_a")).unwrap().values_hll.estimate());

            // Verify prop_b was loaded correctly
            let loaded_b = loaded.get(&Sid::new(100, "prop_b")).expect("prop_b should exist");
            assert_eq!(loaded_b.count, 5);
            assert_eq!(loaded_b.last_modified_t, 3);
        }

        #[test]
        fn test_list_hll_sketch_addresses() {
            let mut properties = HashMap::new();

            let mut hll1 = PropertyHll::new();
            hll1.last_modified_t = 10;
            properties.insert(Sid::new(100, "name"), hll1);

            let mut hll2 = PropertyHll::new();
            hll2.last_modified_t = 15;
            properties.insert(Sid::new(200, "age"), hll2);

            let addresses = super::list_hll_sketch_addresses("test/db", &properties);

            // Should have 4 addresses (2 properties * 2 kinds)
            assert_eq!(addresses.len(), 4);

            // Each property uses its own last_modified_t
            let has_t10 = addresses.iter().any(|a| a.contains("t10.hll"));
            let has_t15 = addresses.iter().any(|a| a.contains("t15.hll"));
            assert!(has_t10, "should have address with t10");
            assert!(has_t15, "should have address with t15");
        }

        // === Obsolete Sketch Address Tests ===

        #[test]
        fn test_compute_obsolete_sketch_addresses_no_changes() {
            // Prior and updated have same last_modified_t - no obsolete
            let prior_properties = vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 10,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 5,
                datatypes: vec![],
            }];

            let mut updated_properties = HashMap::new();
            let mut hll = PropertyHll::new();
            hll.last_modified_t = 5; // Same t - no change
            updated_properties.insert(Sid::new(100, "name"), hll);

            let obsolete =
                super::compute_obsolete_sketch_addresses("test/db", &prior_properties, &updated_properties);

            assert!(obsolete.is_empty(), "No obsolete addresses when t unchanged");
        }

        #[test]
        fn test_compute_obsolete_sketch_addresses_with_update() {
            // Prior t=5, updated t=10 - prior sketches are obsolete
            let prior_properties = vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 10,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 5,
                datatypes: vec![],
            }];

            let mut updated_properties = HashMap::new();
            let mut hll = PropertyHll::new();
            hll.last_modified_t = 10; // New t - prior is obsolete
            updated_properties.insert(Sid::new(100, "name"), hll);

            let obsolete =
                super::compute_obsolete_sketch_addresses("test/db", &prior_properties, &updated_properties);

            // Should have 2 obsolete addresses (values + subjects for prior t=5)
            assert_eq!(obsolete.len(), 2);
            assert!(obsolete.iter().any(|a| a.contains("values") && a.contains("t5.hll")));
            assert!(obsolete.iter().any(|a| a.contains("subjects") && a.contains("t5.hll")));
        }

        #[test]
        fn test_compute_obsolete_sketch_addresses_property_removed() {
            // Prior has property, updated doesn't - prior is obsolete
            let prior_properties = vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 10,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 5,
                datatypes: vec![],
            }];

            let updated_properties: HashMap<Sid, PropertyHll> = HashMap::new();

            let obsolete =
                super::compute_obsolete_sketch_addresses("test/db", &prior_properties, &updated_properties);

            // Property removed, so prior sketches are obsolete
            assert_eq!(obsolete.len(), 2);
            assert!(obsolete.iter().any(|a| a.contains("values") && a.contains("t5.hll")));
            assert!(obsolete.iter().any(|a| a.contains("subjects") && a.contains("t5.hll")));
        }

        #[test]
        fn test_compute_obsolete_sketch_addresses_multiple_properties() {
            // Two properties: one updated, one unchanged
            let prior_properties = vec![
                PropertyStatEntry {
                    sid: (100, "name".to_string()),
                    count: 10,
                    ndv_values: 5,
                    ndv_subjects: 5,
                    last_modified_t: 5,
                    datatypes: vec![],
                },
                PropertyStatEntry {
                    sid: (100, "age".to_string()),
                    count: 8,
                    ndv_values: 4,
                    ndv_subjects: 4,
                    last_modified_t: 3,
                    datatypes: vec![],
                },
            ];

            let mut updated_properties = HashMap::new();

            // name: updated (t=5 -> t=10)
            let mut hll_name = PropertyHll::new();
            hll_name.last_modified_t = 10;
            updated_properties.insert(Sid::new(100, "name"), hll_name);

            // age: unchanged (t=3 -> t=3)
            let mut hll_age = PropertyHll::new();
            hll_age.last_modified_t = 3;
            updated_properties.insert(Sid::new(100, "age"), hll_age);

            let obsolete =
                super::compute_obsolete_sketch_addresses("test/db", &prior_properties, &updated_properties);

            // Only "name" at t=5 should be obsolete (2 addresses)
            // "age" at t=3 is unchanged
            assert_eq!(obsolete.len(), 2);
            assert!(
                obsolete.iter().all(|a| a.contains("100_name_t5.hll")),
                "All obsolete should be for name at t=5"
            );
        }

        #[test]
        fn test_compute_obsolete_sketch_addresses_empty_prior() {
            // No prior properties - nothing obsolete
            let prior_properties: Vec<PropertyStatEntry> = vec![];

            let mut updated_properties = HashMap::new();
            let mut hll = PropertyHll::new();
            hll.last_modified_t = 10;
            updated_properties.insert(Sid::new(100, "name"), hll);

            let obsolete =
                super::compute_obsolete_sketch_addresses("test/db", &prior_properties, &updated_properties);

            assert!(obsolete.is_empty(), "No prior properties means nothing obsolete");
        }

        #[test]
        fn test_compute_obsolete_sketch_addresses_correct_path_format() {
            let prior_properties = vec![PropertyStatEntry {
                sid: (200, "email".to_string()),
                count: 5,
                ndv_values: 3,
                ndv_subjects: 3,
                last_modified_t: 42,
                datatypes: vec![],
            }];

            let mut updated_properties = HashMap::new();
            let mut hll = PropertyHll::new();
            hll.last_modified_t = 100;
            updated_properties.insert(Sid::new(200, "email"), hll);

            let obsolete =
                super::compute_obsolete_sketch_addresses("mydb:main", &prior_properties, &updated_properties);

            assert_eq!(obsolete.len(), 2);
            // Verify correct path format with alias normalization
            assert!(obsolete.contains(
                &"fluree:file://mydb/main/index/stats-sketches/values/200_email_t42.hll".to_string()
            ));
            assert!(obsolete.contains(
                &"fluree:file://mydb/main/index/stats-sketches/subjects/200_email_t42.hll".to_string()
            ));
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
                let class_entry = self.entries
                    .entry(flake.s.clone())
                    .or_default();

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
                    let prop_entry = self.entries
                        .entry(flake.s.clone())
                        .or_default();
                    prop_entry.parent_props.insert(parent_sid.clone());

                    let parent_entry = self.entries
                        .entry(parent_sid.clone())
                        .or_default();
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
        let vals: Vec<SchemaPredicateInfo> = self.entries
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
                t: if self.schema_t > 0 { self.schema_t } else { fallback_t },
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
            let classes = self.subject_classes
                .entry(flake.s.clone())
                .or_default();

            if flake.op {
                // Assertion: subject is instance of class
                classes.insert(class_sid.clone());
                // Update class count
                let class_data = self.class_data
                    .entry(class_sid.clone())
                    .or_default();
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
            let class_data = self.class_data
                .entry(class_sid)
                .or_default();

            let prop_data = class_data.properties
                .entry(flake.p.clone())
                .or_default();

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
        let mut entries: Vec<ClassStatEntry> = self.class_data
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0 || !data.properties.is_empty())
            .map(|(class_sid, data)| {
                // Convert properties (sorted by property SID)
                let mut properties: Vec<ClassPropertyUsage> = data.properties
                    .into_iter()
                    .filter(|(_, prop_data)| prop_data.count_delta > 0)
                    .map(|(property_sid, _prop_data)| ClassPropertyUsage { property_sid })
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
    pub fn merge_subject_classes(&mut self, index_classes: std::collections::HashMap<Sid, HashSet<Sid>>) {
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
use fluree_db_core::db::Db;
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
pub async fn batch_lookup_subject_classes<S>(
    db: &Db<S>,
    subjects: &HashSet<Sid>,
) -> crate::error::Result<std::collections::HashMap<Sid, HashSet<Sid>>>
where
    S: Storage,
{
    if subjects.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    // Query PSOT for rdf:type predicate
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
    let match_val = RangeMatch::predicate(rdf_type_sid);
    let opts = RangeOptions::default();

    let flakes = range(db, IndexType::Psot, RangeTest::Eq, match_val, opts).await
        .map_err(|e| crate::error::IndexerError::InvalidConfig(format!("PSOT range query failed: {}", e)))?;

    // Filter to subjects we care about and build mapping
    let mut result: std::collections::HashMap<Sid, HashSet<Sid>> = std::collections::HashMap::new();
    for flake in flakes {
        if subjects.contains(&flake.s) && flake.op {
            // Only assertions (op=true) count
            if let FlakeValue::Ref(class_sid) = &flake.o {
                result.entry(flake.s.clone())
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
pub async fn compute_class_property_stats_parallel<S>(
    db: &Db<S>,
    prior_stats: Option<&fluree_db_core::IndexStats>,
    novelty_flakes: &[Flake],
) -> crate::error::Result<ClassPropertyStatsResult>
where
    S: Storage,
{
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
    let novelty_subjects: HashSet<Sid> = novelty_flakes
        .iter()
        .map(|f| f.s.clone())
        .collect();

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
            batch_lookup_subject_classes(db, &all_subjects_to_lookup).await?
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

#[cfg(test)]
mod class_property_stats_tests {
    use super::*;
    use fluree_db_core::IndexStats;
    use fluree_db_core::{Db, FlakeValue, MemoryStorage, Sid};
    use fluree_vocab::namespaces::{EMPTY, JSON_LD, XSD};

    fn make_type_flake(subject: &str, class: &str, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(100, subject),
            Sid::new(RDF, "type"),
            FlakeValue::Ref(Sid::new(100, class)),
            // $id datatype is `[1, "id"]` in this codebase (see fluree-db-core serde).
            Sid::new(1, "id"),
            t,
            op,
            None,
        )
    }

    fn make_prop_flake(subject: &str, prop: &str, t: i64) -> Flake {
        Flake::new(
            Sid::new(100, subject),
            Sid::new(100, prop),
            FlakeValue::String("v".to_string()),
            // Datatype SID doesn't matter for this test; use a stable dummy SID.
            Sid::new(2, "string"),
            t,
            true,
            None,
        )
    }

    // test_type_novelty_applies_on_top_of_index_base_classes removed:
    // depended on deleted builder module. Needs rewrite for binary pipeline.
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    fn make_schema_flake(subject: &str, predicate_ns: u16, predicate_name: &str, object: &str, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(100, subject),
            Sid::new(predicate_ns, predicate_name),
            FlakeValue::Ref(Sid::new(100, object)),
            Sid::new(0, ""),  // dt not relevant for schema
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
        let person = schema.pred.vals.iter()
            .find(|v| v.id.name.as_ref() == "Person")
            .expect("Person should exist");
        assert_eq!(person.subclass_of.len(), 1);
        assert_eq!(person.subclass_of[0].name.as_ref(), "Thing");

        // Find Student entry
        let student = schema.pred.vals.iter()
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
        let given_name = schema.pred.vals.iter()
            .find(|v| v.id.name.as_ref() == "givenName")
            .expect("givenName should exist");
        assert_eq!(given_name.parent_props.len(), 1);
        assert_eq!(given_name.parent_props[0].name.as_ref(), "name");

        // name should have givenName as child
        let name = schema.pred.vals.iter()
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
                keys: vec!["id".to_string(), "subclassOf".to_string(), "parentProps".to_string(), "childProps".to_string()],
                vals: vec![
                    SchemaPredicateInfo {
                        id: Sid::new(100, "Person"),
                        subclass_of: vec![Sid::new(100, "Thing")],
                        parent_props: vec![],
                        child_props: vec![],
                    },
                ],
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
        let person = schema.pred.vals.iter()
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
