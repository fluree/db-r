//! BinaryIndexStore: loads Phase C binary columnar indexes and dictionaries from disk.
//!
//! Holds everything needed to read the bulk indexes and convert numeric IDs
//! back to `Flake` values. The store is read-only and snapshot-only (latest state).
//!
//! Supports all four sort orders (SPOT, PSOT, POST, OPST) via per-graph, per-order
//! branch manifests. Dictionaries (subjects, strings, predicates, datatypes) are shared
//! across all orders.

use super::branch::{find_branch_file, read_branch_manifest, BranchManifest};
use super::dict_io::{read_forward_entry, read_forward_index, read_language_dict, read_predicate_dict};
use super::global_dict::{LanguageTagDict, PredicateDict};
use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::leaflet_cache::LeafletCache;
use super::prefix_trie::PrefixTrie;
use super::run_record::{RunRecord, RunSortOrder, NO_LIST_INDEX};
use fluree_db_core::value_id::{ObjKind, ObjKey};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use memmap2::Mmap;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

// ============================================================================
// Manifest types (deserialized from index_manifest_{order}.json)
// ============================================================================

/// Entry from index_manifest_{order}.json.
#[derive(Debug, serde::Deserialize)]
struct GraphManifestEntry {
    g_id: u32,
    #[allow(dead_code)]
    leaf_count: u32,
    #[allow(dead_code)]
    total_rows: u64,
    #[allow(dead_code)]
    #[serde(default)]
    branch_hash: Option<String>,
    directory: String,
}

/// Top-level per-order index manifest.
#[derive(Debug, serde::Deserialize)]
struct IndexManifest {
    #[allow(dead_code)]
    #[serde(default)]
    order: Option<String>,
    #[allow(dead_code)]
    total_rows: u64,
    /// Maximum transaction t across all indexed records.
    #[serde(default)]
    max_t: i64,
    #[allow(dead_code)]
    graph_count: usize,
    graphs: Vec<GraphManifestEntry>,
}

// ============================================================================
// Per-graph, per-order index data
// ============================================================================

/// Branch manifest + leaf directory for one sort order of one graph.
struct OrderIndex {
    branch: BranchManifest,
    /// Directory containing leaf files for this graph+order.
    leaf_dir: PathBuf,
}

/// Per-graph index data: holds indexes for all loaded sort orders.
struct GraphIndex {
    orders: HashMap<RunSortOrder, OrderIndex>,
}

// ============================================================================
// BinaryIndexStore
// ============================================================================

/// Read-only store for querying Phase C binary columnar indexes.
///
/// Loads branch manifests for all available sort orders (SPOT, PSOT, POST, OPST),
/// along with shared dictionaries and mmap'd forward files. Provides methods to
/// find subjects by IRI, decode leaflet rows into `Flake` values, and translate
/// between Sid/FlakeValue and binary integer IDs.
pub struct BinaryIndexStore {
    /// Per-graph indexes, each containing per-order branch manifests.
    graphs: HashMap<u32, GraphIndex>,
    /// Predicate dict (p_id → IRI).
    predicates: PredicateDict,
    /// Reverse predicate lookup (IRI → p_id).
    predicate_reverse: HashMap<String, u32>,
    /// Subject forward file (mmap'd subjects.fwd).
    subject_forward: Option<Mmap>,
    /// Subject forward index (offsets + lens).
    subject_offsets: Vec<u64>,
    subject_lens: Vec<u32>,
    /// String forward file (mmap'd strings.fwd).
    string_forward: Option<Mmap>,
    /// String forward index (offsets + lens).
    string_offsets: Vec<u64>,
    string_lens: Vec<u32>,
    /// Namespace codes (code → prefix).
    namespace_codes: HashMap<i32, String>,
    /// Reverse namespace lookup (prefix → code) for Sid → IRI reconstruction.
    #[allow(dead_code)]
    namespace_reverse: HashMap<String, i32>,
    /// PrefixTrie for O(len(iri)) longest-prefix matching.
    prefix_trie: PrefixTrie,
    /// Subject reverse hash index (mmap'd subjects.rev).
    subject_reverse: Option<Mmap>,
    subject_reverse_count: u32,
    /// String reverse hash index (mmap'd strings.rev).
    string_reverse: Option<Mmap>,
    string_reverse_count: u32,
    /// Language tag dict (id → tag string, 1-based).
    language_tags: LanguageTagDict,
    /// Datatype IRI → Sid, pre-computed from run_dir/datatypes.dict (FRD1).
    /// Index = dt_id (from leaflet Region 2; per-leaf variable width).
    /// `dt_sids[dt_id]` gives the Sid for that datatype IRI.
    dt_sids: Vec<Sid>,
    /// Maximum t seen (from manifest or branch).
    max_t: i64,
    /// Base t from the initial full index build. Time-travel via the binary
    /// path is only valid for `t_target >= base_t` (Region 3 history only
    /// covers changes after the initial build). Set to `max_t` at load time;
    /// stays fixed through novelty merges that update `max_t`.
    base_t: i64,
    /// Per-predicate numbig arenas: p_id → equality-only arena for overflow BigInt/BigDecimal.
    numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena>,
    /// Per-predicate numeric shape stats (IntOnly / FloatOnly / Mixed).
    numeric_shapes: HashMap<u32, super::numfloat_dict::NumericShape>,
    /// LRU cache for decoded leaflet regions (Region 1 and Region 2).
    leaflet_cache: Option<LeafletCache>,
}

/// Backward-compatible type alias.
pub type SpotIndexStore = BinaryIndexStore;

impl BinaryIndexStore {
    /// Load a BinaryIndexStore from run_dir (dictionaries) and index_dir (index files).
    ///
    /// Loads per-order manifests (`index_manifest_spot.json`, `index_manifest_psot.json`,
    /// etc.) and builds per-graph, per-order branch manifests. Falls back to
    /// `index_manifest.json` for backward compatibility with SPOT-only builds.
    pub fn load(run_dir: &Path, index_dir: &Path) -> io::Result<Self> {
        let _span = tracing::info_span!("BinaryIndexStore::load", ?run_dir, ?index_dir).entered();

        // ---- Load per-order index manifests ----
        let mut graphs: HashMap<u32, GraphIndex> = HashMap::new();
        let mut max_t: i64 = 0;

        let all_orders = [
            RunSortOrder::Spot,
            RunSortOrder::Psot,
            RunSortOrder::Post,
            RunSortOrder::Opst,
        ];

        let mut any_manifest_loaded = false;
        for &order in &all_orders {
            let manifest_path = index_dir.join(format!("index_manifest_{}.json", order.dir_name()));
            if !manifest_path.exists() {
                continue;
            }

            let manifest_json = std::fs::read_to_string(&manifest_path)?;
            let manifest: IndexManifest = serde_json::from_str(&manifest_json)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            if manifest.max_t > max_t {
                max_t = manifest.max_t;
            }

            for entry in &manifest.graphs {
                let graph_dir = index_dir.join(&entry.directory);
                let branch_path = if let Some(ref hash) = entry.branch_hash {
                    graph_dir.join(format!("{}.fbr", hash))
                } else {
                    find_branch_file(&graph_dir).unwrap_or_else(|_| graph_dir.join("branch.fbr"))
                };

                if branch_path.exists() {
                    let branch = read_branch_manifest(&branch_path)?;
                    tracing::info!(
                        g_id = entry.g_id,
                        order = order.dir_name(),
                        leaves = branch.leaves.len(),
                        "loaded branch manifest"
                    );

                    let graph_index = graphs
                        .entry(entry.g_id)
                        .or_insert_with(|| GraphIndex {
                            orders: HashMap::new(),
                        });

                    graph_index.orders.insert(order, OrderIndex {
                        branch,
                        leaf_dir: graph_dir,
                    });
                }
            }
            any_manifest_loaded = true;
        }

        // Fallback: try legacy index_manifest.json (SPOT-only)
        if !any_manifest_loaded {
            let legacy_path = index_dir.join("index_manifest.json");
            if legacy_path.exists() {
                let manifest_json = std::fs::read_to_string(&legacy_path)?;
                let manifest: IndexManifest = serde_json::from_str(&manifest_json)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                max_t = manifest.max_t;

                for entry in &manifest.graphs {
                    let graph_dir = index_dir.join(&entry.directory);
                    let branch_path = if let Some(ref hash) = entry.branch_hash {
                        graph_dir.join(format!("{}.fbr", hash))
                    } else {
                        find_branch_file(&graph_dir).unwrap_or_else(|_| graph_dir.join("branch.fbr"))
                    };

                    if branch_path.exists() {
                        let branch = read_branch_manifest(&branch_path)?;
                        tracing::info!(
                            g_id = entry.g_id,
                            order = "spot",
                            leaves = branch.leaves.len(),
                            "loaded branch manifest (legacy)"
                        );

                        let graph_index = graphs
                            .entry(entry.g_id)
                            .or_insert_with(|| GraphIndex {
                                orders: HashMap::new(),
                            });

                        graph_index.orders.insert(RunSortOrder::Spot, OrderIndex {
                            branch,
                            leaf_dir: graph_dir,
                        });
                    }
                }
            }
        }

        // ---- Load predicate dict ----
        let predicates = read_predicate_dict(&run_dir.join("predicates.dict"))?;
        let mut predicate_reverse = HashMap::with_capacity(predicates.len() as usize);
        for i in 0..predicates.len() {
            if let Some(iri) = predicates.resolve(i) {
                predicate_reverse.insert(iri.to_string(), i);
            }
        }
        tracing::info!(predicates = predicates.len(), "loaded predicate dict");

        // ---- Load subject forward file + index ----
        let subject_fwd_path = run_dir.join("subjects.fwd");
        let subject_idx_path = run_dir.join("subjects.idx");
        let (subject_offsets, subject_lens) = read_forward_index(&subject_idx_path)?;
        let subject_forward = if subject_fwd_path.exists() {
            let file = std::fs::File::open(&subject_fwd_path)?;
            Some(unsafe { Mmap::map(&file)? })
        } else {
            None
        };
        tracing::info!(subjects = subject_offsets.len(), "loaded subject forward index");

        // ---- Load string forward file + index ----
        let string_fwd_path = run_dir.join("strings.fwd");
        let string_idx_path = run_dir.join("strings.idx");
        let (string_offsets, string_lens) = if string_idx_path.exists() {
            read_forward_index(&string_idx_path)?
        } else {
            (Vec::new(), Vec::new())
        };
        let string_forward = if string_fwd_path.exists() {
            let file = std::fs::File::open(&string_fwd_path)?;
            Some(unsafe { Mmap::map(&file)? })
        } else {
            None
        };
        tracing::info!(strings = string_offsets.len(), "loaded string forward index");

        // ---- Load namespace codes ----
        let ns_path = run_dir.join("namespaces.json");
        let namespace_codes = if ns_path.exists() {
            load_namespace_codes(&ns_path)?
        } else {
            fluree_db_core::default_namespace_codes()
        };
        // Build reverse lookup (prefix → code)
        let namespace_reverse: HashMap<String, i32> = namespace_codes
            .iter()
            .map(|(&code, prefix)| (prefix.clone(), code))
            .collect();
        let prefix_trie = PrefixTrie::from_namespace_codes(&namespace_codes);
        tracing::info!(
            namespaces = namespace_codes.len(),
            trie_nodes = prefix_trie.node_count(),
            "loaded namespace codes + built prefix trie"
        );

        // ---- Load subject reverse hash index ----
        let rev_path = run_dir.join("subjects.rev");
        let (subject_reverse, subject_reverse_count) = if rev_path.exists() {
            let data = std::fs::read(&rev_path)?;
            if data.len() < 8 || &data[0..4] != b"SRV1" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "subjects.rev: invalid magic",
                ));
            }
            let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
            let expected_len = 8 + count as usize * 20;
            if data.len() < expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "subjects.rev truncated: {} < {} (count={})",
                        data.len(),
                        expected_len,
                        count
                    ),
                ));
            }
            let file = std::fs::File::open(&rev_path)?;
            let mmap = unsafe { Mmap::map(&file)? };
            tracing::info!(entries = count, "loaded subject reverse index");
            (Some(mmap), count)
        } else {
            (None, 0)
        };

        // ---- Load string reverse hash index ----
        let str_rev_path = run_dir.join("strings.rev");
        let (string_reverse, string_reverse_count) = if str_rev_path.exists() {
            let data = std::fs::read(&str_rev_path)?;
            if data.len() < 8 || &data[0..4] != b"LRV1" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "strings.rev: invalid magic",
                ));
            }
            let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
            let expected_len = 8 + count as usize * 20;
            if data.len() < expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "strings.rev truncated: {} < {} (count={})",
                        data.len(),
                        expected_len,
                        count
                    ),
                ));
            }
            let file = std::fs::File::open(&str_rev_path)?;
            let mmap = unsafe { Mmap::map(&file)? };
            tracing::info!(entries = count, "loaded string reverse index");
            (Some(mmap), count)
        } else {
            (None, 0)
        };

        // ---- Load language tag dict ----
        let lang_path = run_dir.join("languages.dict");
        let language_tags = if lang_path.exists() {
            read_language_dict(&lang_path)?
        } else {
            LanguageTagDict::new()
        };
        tracing::info!(tags = language_tags.len(), "loaded language dict");

        // ---- Load datatype dict → pre-compute dt_sids ----
        let dt_path = run_dir.join("datatypes.dict");
        let dt_dict = read_predicate_dict(&dt_path)?;
        let dt_sids: Vec<Sid> = (0..dt_dict.len()).map(|id| {
            let iri = dt_dict.resolve(id).unwrap_or("");
            match prefix_trie.longest_match(iri) {
                Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
                None => Sid::new(0, iri),
            }
        }).collect();
        tracing::info!(datatypes = dt_dict.len(), "loaded datatype dict → dt_sids");

        // ---- Load numbig arenas ----
        let nb_dir = run_dir.join("numbig");
        let mut numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena> =
            HashMap::new();
        if nb_dir.exists() && nb_dir.is_dir() {
            for entry in std::fs::read_dir(&nb_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("p_") && name.ends_with(".nba") {
                        let p_id_str = &name[2..name.len() - 4];
                        if let Ok(p_id) = p_id_str.parse::<u32>() {
                            let arena = super::numbig_dict::read_numbig_arena(&path)?;
                            numbig_forward.insert(p_id, arena);
                        }
                    }
                }
            }
            if !numbig_forward.is_empty() {
                tracing::info!(
                    predicates = numbig_forward.len(),
                    total_entries = numbig_forward.values().map(|a| a.len()).sum::<usize>(),
                    "loaded numbig arenas"
                );
            }
        }

        // ---- Load numeric shapes ----
        let shapes_path = run_dir.join("numeric_shapes.json");
        let numeric_shapes = if shapes_path.exists() {
            super::numfloat_dict::read_numeric_shapes(&shapes_path)?
        } else {
            HashMap::new() // old indexes: fall back to ScanOperator for all range queries
        };
        if !numeric_shapes.is_empty() {
            tracing::info!(
                predicates = numeric_shapes.len(),
                "loaded numeric shapes"
            );
        }

        // Log summary of loaded orders
        let mut order_summary = Vec::new();
        for &order in &all_orders {
            let graph_count = graphs.values().filter(|g| g.orders.contains_key(&order)).count();
            if graph_count > 0 {
                order_summary.push(format!("{}({}g)", order.dir_name(), graph_count));
            }
        }
        tracing::info!(
            orders = %order_summary.join(", "),
            graphs = graphs.len(),
            "BinaryIndexStore loaded"
        );

        // ---- Initialize leaflet cache (1.5 GB default) ----
        let leaflet_cache = Some(LeafletCache::with_max_bytes(1_500_000_000));

        Ok(Self {
            graphs,
            predicates,
            predicate_reverse,
            subject_forward,
            subject_offsets,
            subject_lens,
            string_forward,
            string_offsets,
            string_lens,
            namespace_codes,
            namespace_reverse,
            prefix_trie,
            subject_reverse,
            subject_reverse_count,
            string_reverse,
            string_reverse_count,
            language_tags,
            dt_sids,
            max_t,
            base_t: max_t,
            numbig_forward,
            numeric_shapes,
            leaflet_cache,
        })
    }

    // ========================================================================
    // IRI encoding / decoding
    // ========================================================================

    /// Encode a full IRI into a `Sid` using namespace codes.
    ///
    /// Uses the PrefixTrie for O(len(iri)) longest-prefix matching.
    /// Returns `Sid::new(0, iri)` if no prefix matches (default namespace).
    pub fn encode_iri(&self, iri: &str) -> Sid {
        match self.prefix_trie.longest_match(iri) {
            Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
            None => Sid::new(0, iri),
        }
    }

    /// Reconstruct a full IRI from a Sid (namespace_code + name).
    ///
    /// Looks up the namespace prefix by code and concatenates with the name.
    pub fn sid_to_iri(&self, sid: &Sid) -> String {
        let prefix = self.namespace_codes
            .get(&sid.namespace_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        format!("{}{}", prefix, sid.name)
    }

    /// Resolve s_id → full IRI string.
    pub fn resolve_subject_iri(&self, s_id: u32) -> io::Result<String> {
        let idx = s_id as usize;
        if idx >= self.subject_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("s_id {} out of range (max {})", s_id, self.subject_offsets.len()),
            ));
        }
        let mmap = self.subject_forward.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "subject forward file not loaded")
        })?;
        let iri = read_forward_entry(mmap, self.subject_offsets[idx], self.subject_lens[idx])?;
        Ok(iri.to_string())
    }

    /// Resolve p_id → full IRI string.
    pub fn resolve_predicate_iri(&self, p_id: u32) -> Option<&str> {
        self.predicates.resolve(p_id)
    }

    /// Find p_id from full IRI.
    pub fn find_predicate_id(&self, iri: &str) -> Option<u32> {
        self.predicate_reverse.get(iri).copied()
    }

    /// Resolve a string dict entry by ID.
    pub fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        let idx = str_id as usize;
        if idx >= self.string_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("str_id {} out of range (max {})", str_id, self.string_offsets.len()),
            ));
        }
        let mmap = self.string_forward.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "string forward file not loaded")
        })?;
        let s = read_forward_entry(mmap, self.string_offsets[idx], self.string_lens[idx])?;
        Ok(s.to_string())
    }

    // ========================================================================
    // Sid ↔ integer ID translation (Phase 2: query key translation)
    // ========================================================================

    /// Translate a Sid to s_id via the reverse hash index.
    ///
    /// Reconstructs the full IRI from the Sid, then looks it up in subjects.rev.
    pub fn sid_to_s_id(&self, sid: &Sid) -> io::Result<Option<u32>> {
        let iri = self.sid_to_iri(sid);
        self.find_subject_id(&iri)
    }

    /// Translate a Sid to p_id via the predicate reverse map.
    ///
    /// Reconstructs the full IRI from the Sid, then looks it up.
    pub fn sid_to_p_id(&self, sid: &Sid) -> Option<u32> {
        let iri = self.sid_to_iri(sid);
        self.find_predicate_id(&iri)
    }

    /// Translate a FlakeValue to `(ObjKind, ObjKey)` for index key matching.
    ///
    /// Returns `None` if the value cannot be translated (e.g., string without
    /// reverse index, unknown IRI ref). Does not require `p_id` — for
    /// predicate-specific translation (BigInt/Decimal), use
    /// `value_to_obj_pair_for_predicate`.
    pub fn value_to_obj_pair(&self, val: &FlakeValue) -> io::Result<Option<(ObjKind, ObjKey)>> {
        match val {
            FlakeValue::Null => Ok(Some((ObjKind::NULL, ObjKey::from_u64(0)))),
            FlakeValue::Boolean(b) => Ok(Some((ObjKind::BOOL, ObjKey::encode_bool(*b)))),
            FlakeValue::Long(n) => Ok(Some((ObjKind::NUM_INT, ObjKey::encode_i64(*n)))),
            FlakeValue::Ref(sid) => {
                match self.sid_to_s_id(sid)? {
                    Some(s_id) => Ok(Some((ObjKind::REF_ID, ObjKey::encode_u32_id(s_id)))),
                    None => Ok(None),
                }
            }
            FlakeValue::String(s) => {
                match self.find_string_id(s)? {
                    Some(str_id) => Ok(Some((ObjKind::LEX_ID, ObjKey::encode_u32_id(str_id)))),
                    None => Ok(None),
                }
            }
            FlakeValue::Double(d) => {
                // Integer-valued doubles that fit i64 → NUM_INT
                if d.is_finite() && d.fract() == 0.0 {
                    let as_i64 = *d as i64;
                    if (as_i64 as f64) == *d {
                        return Ok(Some((ObjKind::NUM_INT, ObjKey::encode_i64(as_i64))));
                    }
                }
                // Finite non-integer → NUM_F64 inline (full 64-bit transform)
                if d.is_finite() {
                    match ObjKey::encode_f64(*d) {
                        Ok(key) => Ok(Some((ObjKind::NUM_F64, key))),
                        Err(_) => Ok(None),
                    }
                } else {
                    Ok(None) // NaN/Inf not in index
                }
            }
            // BigInt/Decimal need p_id for numbig arena → use value_to_obj_pair_for_predicate
            FlakeValue::BigInt(_) | FlakeValue::Decimal(_) => Ok(None),
            // Date/Time/DateTime/Vector/Json: not yet supported for key translation
            _ => Ok(None),
        }
    }

    /// Translate a RangeMatch into min/max RunRecord bounds for a given sort order.
    ///
    /// Returns `None` if any required lookup fails (meaning 0 results are possible).
    /// The RunRecord bounds define the key range to search in the branch manifest.
    ///
    /// **OPST guard:** If the sort order is OPST and the object is not a Ref,
    /// returns `None` (OPST only contains IRI references).
    pub fn translate_range(
        &self,
        s: Option<&Sid>,
        p: Option<&Sid>,
        o: Option<&FlakeValue>,
        order: RunSortOrder,
        g_id: u32,
    ) -> io::Result<Option<(RunRecord, RunRecord)>> {
        use super::run_record::NO_LIST_INDEX;

        // OPST guard: only IRI refs exist in OPST index
        if order == RunSortOrder::Opst {
            if let Some(val) = o {
                if !matches!(val, FlakeValue::Ref(_)) {
                    return Ok(None);
                }
            }
        }

        // Translate bound components to integer IDs
        let s_id = match s {
            Some(sid) => match self.sid_to_s_id(sid)? {
                Some(id) => Some(id),
                None => return Ok(None),
            },
            None => None,
        };

        let p_id = match p {
            Some(sid) => match self.sid_to_p_id(sid) {
                Some(id) => Some(id),
                None => return Ok(None),
            },
            None => None,
        };

        let o_pair = match o {
            Some(val) => match self.value_to_obj_pair(val)? {
                Some(pair) => Some(pair),
                None => return Ok(None),
            },
            None => None,
        };

        // Build min/max RunRecord bounds using ObjKind/ObjKey
        let (min_o_kind, min_o_key) = o_pair
            .map(|(k, v)| (k.as_u8(), v.as_u64()))
            .unwrap_or((ObjKind::MIN.as_u8(), 0u64));
        let (max_o_kind, max_o_key) = o_pair
            .map(|(k, v)| (k.as_u8(), v.as_u64()))
            .unwrap_or((ObjKind::MAX.as_u8(), u64::MAX));

        let min_key = RunRecord {
            g_id,
            s_id: s_id.unwrap_or(0),
            p_id: p_id.unwrap_or(0),
            dt: 0,
            o_kind: min_o_kind,
            op: 0,
            o_key: min_o_key,
            t: i64::MIN,
            lang_id: 0,
            _pad: [0; 2],
            i: NO_LIST_INDEX,
        };

        let max_key = RunRecord {
            g_id,
            s_id: s_id.unwrap_or(u32::MAX),
            p_id: p_id.unwrap_or(u32::MAX),
            dt: u16::MAX,
            o_kind: max_o_kind,
            op: 1,
            o_key: max_o_key,
            t: i64::MAX,
            lang_id: u16::MAX,
            _pad: [0; 2],
            i: i32::MAX,
        };

        Ok(Some((min_key, max_key)))
    }

    // ========================================================================
    // Subject reverse lookup (IRI → s_id)
    // ========================================================================

    /// Find s_id for an IRI via the reverse hash index (O(log N) binary search).
    pub fn find_subject_id(&self, iri: &str) -> io::Result<Option<u32>> {
        let mmap = match &self.subject_reverse {
            Some(m) => m,
            None => return Ok(None),
        };
        if self.subject_reverse_count == 0 {
            return Ok(None);
        }

        let hash = xxhash_rust::xxh3::xxh3_128(iri.as_bytes());
        let target_hi = (hash >> 64) as u64;
        let target_lo = hash as u64;

        // Binary search over sorted (hash_hi, hash_lo, s_id) records.
        // Each record is 20 bytes. Data starts at offset 8 (after magic + count).
        let data = &mmap[8..];
        let count = self.subject_reverse_count as usize;
        let record_size = 20;

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = mid * record_size;
            let mid_hi = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let mid_lo = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());

            match (mid_hi, mid_lo).cmp(&(target_hi, target_lo)) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    // Verify via forward map to guard against hash collisions.
                    let s_id = u32::from_le_bytes(data[off + 16..off + 20].try_into().unwrap());
                    let resolved = self.resolve_subject_iri(s_id)?;
                    if resolved == iri {
                        return Ok(Some(s_id));
                    }
                    // Hash collision (extremely rare with xxh3_128): scan adjacent
                    // records that share the same hash.
                    // Scan backward
                    if mid > 0 {
                        let mut i = mid - 1;
                        loop {
                            let o = i * record_size;
                            let h = u64::from_le_bytes(data[o..o + 8].try_into().unwrap());
                            let l = u64::from_le_bytes(data[o + 8..o + 16].try_into().unwrap());
                            if (h, l) != (target_hi, target_lo) { break; }
                            let id = u32::from_le_bytes(data[o + 16..o + 20].try_into().unwrap());
                            if self.resolve_subject_iri(id)? == iri {
                                return Ok(Some(id));
                            }
                            if i == 0 { break; }
                            i -= 1;
                        }
                    }
                    // Scan forward
                    for i in (mid + 1)..count {
                        let o = i * record_size;
                        let h = u64::from_le_bytes(data[o..o + 8].try_into().unwrap());
                        let l = u64::from_le_bytes(data[o + 8..o + 16].try_into().unwrap());
                        if (h, l) != (target_hi, target_lo) { break; }
                        let id = u32::from_le_bytes(data[o + 16..o + 20].try_into().unwrap());
                        if self.resolve_subject_iri(id)? == iri {
                            return Ok(Some(id));
                        }
                    }
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    // ========================================================================
    // String reverse lookup (string → str_id)
    // ========================================================================

    /// Find str_id for a string value via the reverse hash index (O(log N) binary search).
    pub fn find_string_id(&self, value: &str) -> io::Result<Option<u32>> {
        let mmap = match &self.string_reverse {
            Some(m) => m,
            None => return Ok(None),
        };
        if self.string_reverse_count == 0 {
            return Ok(None);
        }

        let hash = xxhash_rust::xxh3::xxh3_128(value.as_bytes());
        let target_hi = (hash >> 64) as u64;
        let target_lo = hash as u64;

        // Binary search over sorted (hash_hi, hash_lo, str_id) records.
        // Each record is 20 bytes. Data starts at offset 8 (after magic + count).
        let data = &mmap[8..];
        let count = self.string_reverse_count as usize;
        let record_size = 20;

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = mid * record_size;
            let mid_hi = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let mid_lo = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());

            match (mid_hi, mid_lo).cmp(&(target_hi, target_lo)) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    // Verify via forward map to guard against hash collisions.
                    let str_id = u32::from_le_bytes(data[off + 16..off + 20].try_into().unwrap());
                    let resolved = self.resolve_string_value(str_id)?;
                    if resolved == value {
                        return Ok(Some(str_id));
                    }
                    // Hash collision (extremely rare with xxh3_128): scan adjacent
                    // records that share the same hash.
                    // Scan backward
                    if mid > 0 {
                        let mut i = mid - 1;
                        loop {
                            let o = i * record_size;
                            let h = u64::from_le_bytes(data[o..o + 8].try_into().unwrap());
                            let l = u64::from_le_bytes(data[o + 8..o + 16].try_into().unwrap());
                            if (h, l) != (target_hi, target_lo) { break; }
                            let id = u32::from_le_bytes(data[o + 16..o + 20].try_into().unwrap());
                            if self.resolve_string_value(id)? == value {
                                return Ok(Some(id));
                            }
                            if i == 0 { break; }
                            i -= 1;
                        }
                    }
                    // Scan forward
                    for i in (mid + 1)..count {
                        let o = i * record_size;
                        let h = u64::from_le_bytes(data[o..o + 8].try_into().unwrap());
                        let l = u64::from_le_bytes(data[o + 8..o + 16].try_into().unwrap());
                        if (h, l) != (target_hi, target_lo) { break; }
                        let id = u32::from_le_bytes(data[o + 16..o + 20].try_into().unwrap());
                        if self.resolve_string_value(id)? == value {
                            return Ok(Some(id));
                        }
                    }
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    // ========================================================================
    // Graph + branch access (multi-order)
    // ========================================================================

    /// Get the SPOT branch manifest for a graph (backward-compatible shorthand).
    pub fn branch(&self, g_id: u32) -> Option<&BranchManifest> {
        self.branch_for_order(g_id, RunSortOrder::Spot)
    }

    /// Get the branch manifest for a graph and sort order.
    pub fn branch_for_order(&self, g_id: u32, order: RunSortOrder) -> Option<&BranchManifest> {
        self.graphs.get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| &oi.branch)
    }

    /// Get the SPOT leaf directory for a graph (backward-compatible shorthand).
    pub fn leaf_dir(&self, g_id: u32) -> Option<&Path> {
        self.leaf_dir_for_order(g_id, RunSortOrder::Spot)
    }

    /// Get the leaf directory for a graph and sort order.
    pub fn leaf_dir_for_order(&self, g_id: u32, order: RunSortOrder) -> Option<&Path> {
        self.graphs.get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| oi.leaf_dir.as_path())
    }

    /// Check if a given sort order is available for a graph.
    pub fn has_order(&self, g_id: u32, order: RunSortOrder) -> bool {
        self.graphs.get(&g_id)
            .map_or(false, |g| g.orders.contains_key(&order))
    }

    /// Get the available sort orders for a graph.
    pub fn available_orders(&self, g_id: u32) -> Vec<RunSortOrder> {
        self.graphs.get(&g_id)
            .map(|g| {
                let mut orders: Vec<_> = g.orders.keys().copied().collect();
                orders.sort_by_key(|o| o.dir_name());
                orders
            })
            .unwrap_or_default()
    }

    /// Get the set of graph IDs available.
    pub fn graph_ids(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self.graphs.keys().copied().collect();
        ids.sort();
        ids
    }

    // ========================================================================
    // Namespace codes access
    // ========================================================================

    /// Get the namespace code → prefix mapping.
    pub fn namespace_codes(&self) -> &HashMap<i32, String> {
        &self.namespace_codes
    }

    /// Get the prefix trie for IRI encoding.
    pub fn prefix_trie(&self) -> &PrefixTrie {
        &self.prefix_trie
    }

    /// Number of predicates in the dictionary.
    pub fn predicate_count(&self) -> u32 {
        self.predicates.len()
    }

    /// Get pre-computed dt_sids (dt_id → Sid).
    pub fn dt_sids(&self) -> &[Sid] {
        &self.dt_sids
    }

    /// Maximum t value (transaction time) in the index.
    pub fn max_t(&self) -> i64 {
        self.max_t
    }

    /// Base t from the initial full index build.
    ///
    /// Time-travel via the binary path is only valid for `t_target >= base_t`.
    /// Before this time, Region 3 history is unavailable and the caller
    /// should fall back to the B-tree path.
    pub fn base_t(&self) -> i64 {
        self.base_t
    }

    /// Get the leaflet cache (if enabled).
    pub fn leaflet_cache(&self) -> Option<&LeafletCache> {
        self.leaflet_cache.as_ref()
    }

    /// Find dt_id for a datatype Sid.
    ///
    /// Iterates the pre-computed `dt_sids` vec (typically <20 entries).
    /// Returns `None` if the datatype is not in the dictionary.
    pub fn find_dt_id(&self, dt_sid: &Sid) -> Option<u16> {
        self.dt_sids.iter().position(|s| s == dt_sid).map(|i| i as u16)
    }

    /// Find lang_id for a language tag string.
    ///
    /// Uses the language dict's reverse lookup.
    /// Returns `None` if the tag is not found.
    pub fn find_lang_id(&self, tag: &str) -> Option<u16> {
        self.language_tags.find_id(tag)
    }

    // ========================================================================
    // Numeric shape + value → ObjKind/ObjKey translation
    // ========================================================================

    /// Get numeric shape for a predicate. Returns `None` if unknown
    /// (predicate has no numeric values or shapes file is absent).
    pub fn numeric_shape(&self, p_id: u32) -> Option<super::numfloat_dict::NumericShape> {
        self.numeric_shapes.get(&p_id).copied()
    }

    /// Translate a FlakeValue to `(ObjKind, ObjKey)` using per-predicate context.
    ///
    /// Handles Double (inline f64 encoding), BigInt/Decimal (numbig arena lookup)
    /// that `value_to_obj_pair()` returns `None` for.
    pub fn value_to_obj_pair_for_predicate(
        &self,
        val: &FlakeValue,
        p_id: u32,
    ) -> io::Result<Option<(ObjKind, ObjKey)>> {
        match val {
            FlakeValue::Double(d) => {
                // Integer-valued doubles that fit i64 → NUM_INT
                if d.is_finite() && d.fract() == 0.0 {
                    let as_i64 = *d as i64;
                    if (as_i64 as f64) == *d {
                        return Ok(Some((ObjKind::NUM_INT, ObjKey::encode_i64(as_i64))));
                    }
                }
                // Finite non-integer → NUM_F64 inline
                if d.is_finite() {
                    match ObjKey::encode_f64(*d) {
                        Ok(key) => Ok(Some((ObjKind::NUM_F64, key))),
                        Err(_) => Ok(None),
                    }
                } else {
                    Ok(None) // NaN/Inf not in index
                }
            }
            FlakeValue::BigInt(bi) => {
                // Try i64 first
                use num_traits::ToPrimitive;
                if let Some(v) = bi.to_i64() {
                    return Ok(Some((ObjKind::NUM_INT, ObjKey::encode_i64(v))));
                }
                // Overflow → numbig arena lookup (read-only)
                if let Some(arena) = self.numbig_forward.get(&p_id) {
                    if let Some(handle) = arena.find_bigint(bi) {
                        return Ok(Some((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle))));
                    }
                }
                Ok(None) // BigInt not in arena for this predicate
            }
            FlakeValue::Decimal(bd) => {
                // Decimal → numbig arena lookup (read-only)
                if let Some(arena) = self.numbig_forward.get(&p_id) {
                    if let Some(handle) = arena.find_bigdec(bd) {
                        return Ok(Some((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle))));
                    }
                }
                Ok(None) // Decimal not in arena for this predicate
            }
            // All other types: delegate to generic lookup
            _ => self.value_to_obj_pair(val),
        }
    }

    /// Translate ObjectBounds to `(min_o_kind, min_o_key, max_o_kind, max_o_key)`.
    ///
    /// Uses numeric shape to choose NUM_INT or NUM_F64 bounds.
    /// Returns `None` if the range is empty, shape is Mixed, or bounds
    /// can't be safely converted. Mixed/NumBig ranges use post-filter.
    pub fn translate_object_bounds(
        &self,
        bounds: &fluree_db_core::range::ObjectBounds,
        _p_id: u32,
        shape: super::numfloat_dict::NumericShape,
    ) -> Option<(ObjKind, ObjKey, ObjKind, ObjKey)> {
        use super::numfloat_dict::NumericShape;
        match shape {
            NumericShape::IntOnly => self.translate_int_bounds(bounds),
            NumericShape::FloatOnly => self.translate_float_bounds(bounds),
            NumericShape::Mixed => None, // post-filter
        }
    }

    /// Translate ObjectBounds to NUM_INT `(ObjKind, ObjKey)` range.
    fn translate_int_bounds(
        &self,
        bounds: &fluree_db_core::range::ObjectBounds,
    ) -> Option<(ObjKind, ObjKey, ObjKind, ObjKey)> {
        let min_key = match &bounds.lower {
            Some((val, inclusive)) => {
                let i = flakevalue_to_i64_ceil(val, *inclusive)?;
                ObjKey::encode_i64(i)
            }
            None => ObjKey::encode_i64(i64::MIN), // smallest possible i64
        };
        let max_key = match &bounds.upper {
            Some((val, inclusive)) => {
                let i = flakevalue_to_i64_floor(val, *inclusive)?;
                ObjKey::encode_i64(i)
            }
            None => ObjKey::encode_i64(i64::MAX), // largest possible i64
        };
        if min_key > max_key {
            return None;
        }
        Some((ObjKind::NUM_INT, min_key, ObjKind::NUM_INT, max_key))
    }

    /// Translate ObjectBounds to NUM_F64 `(ObjKind, ObjKey)` range via inline encoding.
    ///
    /// No dictionary lookup needed — f64 values are encoded directly with a
    /// total-order bit transform.
    fn translate_float_bounds(
        &self,
        bounds: &fluree_db_core::range::ObjectBounds,
    ) -> Option<(ObjKind, ObjKey, ObjKind, ObjKey)> {
        let min_key = match &bounds.lower {
            Some((val, inclusive)) => {
                let f = flakevalue_to_f64(val)?;
                let key = ObjKey::encode_f64(f).ok()?;
                if *inclusive {
                    key
                } else {
                    // Exclusive lower: next encoded value above
                    ObjKey::from_u64(key.as_u64().checked_add(1)?)
                }
            }
            None => ObjKey::from_u64(0), // min possible encoded f64
        };
        let max_key = match &bounds.upper {
            Some((val, inclusive)) => {
                let f = flakevalue_to_f64(val)?;
                let key = ObjKey::encode_f64(f).ok()?;
                if *inclusive {
                    key
                } else {
                    // Exclusive upper: previous encoded value below
                    ObjKey::from_u64(key.as_u64().checked_sub(1)?)
                }
            }
            None => ObjKey::from_u64(u64::MAX), // max possible encoded f64
        };
        if min_key > max_key {
            return None;
        }
        Some((ObjKind::NUM_F64, min_key, ObjKind::NUM_F64, max_key))
    }

    /// Get the numbig arena for a predicate (for decode-time lookup).
    pub fn numbig_arena(&self, p_id: u32) -> Option<&super::numbig_dict::NumBigArena> {
        self.numbig_forward.get(&p_id)
    }

    // ========================================================================
    // Row → Flake conversion
    // ========================================================================

    /// Convert a decoded leaflet row into a Flake.
    ///
    /// Does IRI round-trip: `id → IRI string → encode_iri(iri) → Sid`.
    /// PrefixTrie makes this O(len(iri)) per call.
    pub fn row_to_flake(
        &self,
        s_id: u32,
        p_id: u32,
        o_kind: u8,
        o_key: u64,
        dt_raw: u32,
        t: i64,
        lang_id: u16,
        i_val: i32,
    ) -> io::Result<Flake> {
        // Subject: s_id → IRI → Sid
        let s_iri = self.resolve_subject_iri(s_id)?;
        let s_sid = self.encode_iri(&s_iri);

        // Predicate: p_id → IRI → Sid
        let p_iri = self.resolve_predicate_iri(p_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("p_id {} not in predicate dict", p_id),
            )
        })?;
        let p_sid = self.encode_iri(p_iri);

        // Object: (ObjKind, ObjKey) → FlakeValue
        let o_val = self.decode_value(o_kind, o_key, p_id)?;

        // Datatype: dt_id → Sid via pre-computed dt_sids vec
        let dt_sid = self.dt_sids.get(dt_raw as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, ""));

        // Meta: lang + list index
        let meta = self.decode_meta(lang_id, i_val);

        Ok(Flake::new(s_sid, p_sid, o_val, dt_sid, t, true, meta))
    }

    /// Decode an `(o_kind, o_key)` pair into a FlakeValue.
    ///
    /// `p_id` is required for NUM_BIG decoding — it identifies the
    /// per-predicate numbig arena to look up the handle.
    pub fn decode_value(&self, o_kind: u8, o_key: u64, p_id: u32) -> io::Result<FlakeValue> {
        if o_kind == ObjKind::MIN.as_u8() || o_kind == ObjKind::NULL.as_u8() {
            return Ok(FlakeValue::Null);
        }
        if o_kind == ObjKind::BOOL.as_u8() {
            return Ok(FlakeValue::Boolean(o_key != 0));
        }
        if o_kind == ObjKind::NUM_INT.as_u8() {
            return Ok(FlakeValue::Long(ObjKey::from_u64(o_key).decode_i64()));
        }
        if o_kind == ObjKind::NUM_F64.as_u8() {
            return Ok(FlakeValue::Double(ObjKey::from_u64(o_key).decode_f64()));
        }
        if o_kind == ObjKind::REF_ID.as_u8() {
            let ref_s_id = o_key as u32;
            let ref_iri = self.resolve_subject_iri(ref_s_id)?;
            return Ok(FlakeValue::Ref(self.encode_iri(&ref_iri)));
        }
        if o_kind == ObjKind::LEX_ID.as_u8() {
            let str_id = o_key as u32;
            let s = self.resolve_string_value(str_id)?;
            return Ok(FlakeValue::String(s));
        }
        if o_kind == ObjKind::DATE.as_u8() {
            let days = ObjKey::from_u64(o_key).decode_date();
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(
                days + 719_163, // Unix epoch = day 719163 in CE
            )
            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let iso = date.format("%Y-%m-%d").to_string();
            return match fluree_db_core::temporal::Date::parse(&iso) {
                Ok(d) => Ok(FlakeValue::Date(Box::new(d))),
                Err(_) => Ok(FlakeValue::String(iso)),
            };
        }
        if o_kind == ObjKind::TIME.as_u8() {
            let micros = ObjKey::from_u64(o_key).decode_time();
            let secs = (micros / 1_000_000) as u32;
            let frac_micros = (micros % 1_000_000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, frac_micros * 1000)
                .unwrap_or(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
            let iso = time.format("%H:%M:%S%.6f").to_string();
            return match fluree_db_core::temporal::Time::parse(&iso) {
                Ok(t) => Ok(FlakeValue::Time(Box::new(t))),
                Err(_) => Ok(FlakeValue::String(iso)),
            };
        }
        if o_kind == ObjKind::DATE_TIME.as_u8() {
            let epoch_micros = ObjKey::from_u64(o_key).decode_datetime();
            let dt = chrono::DateTime::from_timestamp_micros(epoch_micros)
                .unwrap_or_default();
            let iso = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
            return match fluree_db_core::temporal::DateTime::parse(&iso) {
                Ok(d) => Ok(FlakeValue::DateTime(Box::new(d))),
                Err(_) => Ok(FlakeValue::String(iso)),
            };
        }
        if o_kind == ObjKind::JSON_ID.as_u8() {
            let str_id = o_key as u32;
            let s = self.resolve_string_value(str_id)?;
            return Ok(FlakeValue::Json(s));
        }
        if o_kind == ObjKind::NUM_BIG.as_u8() {
            let handle = o_key as u32;
            if let Some(arena) = self.numbig_forward.get(&p_id) {
                if let Some(stored) = arena.get_by_handle(handle) {
                    return Ok(stored.to_flake_value());
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("NUM_BIG handle {} not found for p_id={}", handle, p_id),
            ));
        }
        if o_kind == ObjKind::MAX.as_u8() {
            return Ok(FlakeValue::Null);
        }
        // Unknown kind → null fallback
        Ok(FlakeValue::Null)
    }

    /// Decode lang_id and i_val into FlakeMeta.
    pub fn decode_meta(&self, lang_id: u16, i_val: i32) -> Option<FlakeMeta> {
        let has_lang = lang_id != 0;
        let has_idx = i_val != NO_LIST_INDEX;

        if !has_lang && !has_idx {
            return None;
        }

        let mut meta = FlakeMeta::new();
        if has_lang {
            if let Some(tag) = self.language_tags.resolve(lang_id) {
                meta = FlakeMeta::with_lang(tag);
            }
        }
        if has_idx {
            meta = if has_lang {
                // Both lang and index — set index on existing meta
                FlakeMeta {
                    i: Some(i_val),
                    ..meta
                }
            } else {
                FlakeMeta::with_index(i_val)
            };
        }
        Some(meta)
    }

    // ========================================================================
    // High-level query helpers (SPOT-based, backward compatible)
    // ========================================================================

    /// Query all flakes for a subject in a graph (using SPOT index).
    ///
    /// Returns Flakes in SPOT order.
    pub fn query_subject_flakes(
        &self,
        g_id: u32,
        s_id: u32,
    ) -> io::Result<Vec<Flake>> {
        self.query_subject_predicate_flakes(g_id, s_id, None)
    }

    /// Query flakes for a subject (optionally filtered by predicate) in a graph.
    /// Uses the SPOT index.
    pub fn query_subject_predicate_flakes(
        &self,
        g_id: u32,
        s_id: u32,
        p_id: Option<u32>,
    ) -> io::Result<Vec<Flake>> {
        let branch = match self.branch_for_order(g_id, RunSortOrder::Spot) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let _leaf_dir = match self.leaf_dir_for_order(g_id, RunSortOrder::Spot) {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        let leaf_range = branch.find_leaves_for_subject(g_id, s_id);
        let mut flakes = Vec::new();

        for leaf_idx in leaf_range {
            let leaf_entry = &branch.leaves[leaf_idx];
            // leaf_entry.path is already fully resolved by the branch reader
            let leaf_data = std::fs::read(&leaf_entry.path)?;
            let header = read_leaf_header(&leaf_data)?;

            for dir_entry in &header.leaflet_dir {
                let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                if end > leaf_data.len() {
                    break;
                }
                let leaflet_bytes = &leaf_data[dir_entry.offset as usize..end];
                let decoded = decode_leaflet(leaflet_bytes, header.p_width, header.dt_width, RunSortOrder::Spot)?;

                for row in 0..decoded.row_count {
                    let row_s_id = decoded.s_ids[row];
                    if row_s_id != s_id {
                        continue;
                    }
                    let row_p_id = decoded.p_ids[row];
                    if let Some(filter_p) = p_id {
                        if row_p_id != filter_p {
                            continue;
                        }
                    }

                    let flake = self.row_to_flake(
                        row_s_id,
                        row_p_id,
                        decoded.o_kinds[row],
                        decoded.o_keys[row],
                        decoded.dt_values[row],
                        decoded.t_values[row],
                        decoded.lang_ids[row],
                        decoded.i_values[row],
                    )?;
                    flakes.push(flake);
                }
            }
        }

        Ok(flakes)
    }
}

// ============================================================================
// Numeric conversion helpers for bound translation
// ============================================================================

/// Convert a FlakeValue to f64 for float bound encoding.
///
/// Returns `None` for non-numeric or non-finite values.
fn flakevalue_to_f64(val: &FlakeValue) -> Option<f64> {
    match val {
        FlakeValue::Long(v) => Some(*v as f64),
        FlakeValue::Double(d) if d.is_finite() => Some(*d),
        _ => None,
    }
}

// ============================================================================
// i64 conversion helpers for IntOnly bound translation
// ============================================================================

/// Convert FlakeValue to i64 ceiling (for lower bounds).
///
/// `inclusive=true`: smallest integer >= val; `inclusive=false`: smallest integer > val.
/// Returns `None` if the value cannot be safely converted (e.g., BigDecimal, non-numeric).
fn flakevalue_to_i64_ceil(val: &FlakeValue, inclusive: bool) -> Option<i64> {
    match val {
        FlakeValue::Long(v) => {
            if inclusive {
                Some(*v)
            } else {
                v.checked_add(1)
            }
        }
        FlakeValue::Double(d) if d.is_finite() => {
            let c = d.ceil() as i64;
            if inclusive {
                Some(c)
            } else if (c as f64) == *d {
                c.checked_add(1)
            } else {
                Some(c)
            }
        }
        FlakeValue::BigInt(bi) => {
            use num_traits::ToPrimitive;
            let v = bi.to_i64()?;
            if inclusive {
                Some(v)
            } else {
                v.checked_add(1)
            }
        }
        // BigDecimal: to_f64() can round past the boundary, which would
        // silently exclude true matches from the scan (post-filter can't
        // recover excluded rows). Bail out → broader scan + post-filter.
        FlakeValue::Decimal(_) => None,
        _ => None,
    }
}

/// Convert FlakeValue to i64 floor (for upper bounds).
///
/// `inclusive=true`: largest integer <= val; `inclusive=false`: largest integer < val.
/// Returns `None` if the value cannot be safely converted.
fn flakevalue_to_i64_floor(val: &FlakeValue, inclusive: bool) -> Option<i64> {
    match val {
        FlakeValue::Long(v) => {
            if inclusive {
                Some(*v)
            } else {
                v.checked_sub(1)
            }
        }
        FlakeValue::Double(d) if d.is_finite() => {
            let f = d.floor() as i64;
            if inclusive {
                Some(f)
            } else if (f as f64) == *d {
                f.checked_sub(1)
            } else {
                Some(f)
            }
        }
        FlakeValue::BigInt(bi) => {
            use num_traits::ToPrimitive;
            let v = bi.to_i64()?;
            if inclusive {
                Some(v)
            } else {
                v.checked_sub(1)
            }
        }
        // BigDecimal: bail out (see ceil comment above)
        FlakeValue::Decimal(_) => None,
        _ => None,
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Load namespace codes from namespaces.json.
fn load_namespace_codes(path: &Path) -> io::Result<HashMap<i32, String>> {
    let json_str = std::fs::read_to_string(path)?;
    let entries: Vec<serde_json::Value> = serde_json::from_str(&json_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut codes = HashMap::with_capacity(entries.len());
    for entry in entries {
        let code = entry["code"]
            .as_i64()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing code"))?
            as i32;
        let prefix = entry["prefix"]
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing prefix"))?
            .to_string();
        codes.insert(code, prefix);
    }
    Ok(codes)
}
