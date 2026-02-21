//! BinaryIndexStore: loads Phase C binary columnar indexes and dictionaries from disk.
//!
//! Holds everything needed to read the bulk indexes and convert numeric IDs
//! back to `Flake` values. The store is read-only and snapshot-only (latest state).
//!
//! Supports all four sort orders (SPOT, PSOT, POST, OPST) via per-graph, per-order
//! branch manifests. Dictionaries (subjects, strings, predicates, datatypes) are shared
//! across all orders.

use super::branch::{read_branch_v2_from_bytes, BranchManifest};
use super::dict_io::{
    read_forward_index, read_language_dict, read_predicate_dict, read_subject_sid_map,
};
use super::global_dict::{LanguageTagDict, PredicateDict};
use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::leaflet_cache::LeafletCache;
use super::run_record::{RunRecord, RunSortOrder};
use super::types::DecodedRow;
use crate::dict_tree::builder;
use crate::dict_tree::forward_pack::{KIND_STRING_FWD, KIND_SUBJECT_FWD};
use crate::dict_tree::pack_builder;
use crate::dict_tree::pack_reader::ForwardPackReader;
use crate::dict_tree::reader::LeafSource;
use crate::dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};
use crate::dict_tree::{DictBranch, DictTreeReader};
use fluree_db_core::address::parse_fluree_address;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::GraphId;
use fluree_db_core::ListIndex;
use fluree_db_core::{ContentId, ContentStore};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, PrefixTrie, Sid};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

// ============================================================================
// Manifest types (deserialized from index_manifest_{order}.json)
// ============================================================================

/// Entry from index_manifest_{order}.json.
#[derive(Debug, serde::Deserialize)]
struct GraphManifestEntry {
    g_id: u16,
    // Kept for: diagnostics and future manifest validation.
    // Use when: verifying on-disk index completeness / sanity checks.
    #[expect(dead_code)]
    leaf_count: u32,
    // Kept for: diagnostics and future manifest validation.
    // Use when: verifying row counts across graphs/orders.
    #[expect(dead_code)]
    total_rows: u64,
    /// CID string of the branch manifest file (FBR2, no extension).
    branch_cid: String,
    directory: String,
}

/// Top-level per-order index manifest.
#[derive(Debug, serde::Deserialize)]
struct IndexManifest {
    // Kept for: compatibility with older/newer manifest formats.
    // Use when: validating that the manifest's order matches the file we loaded.
    #[expect(dead_code)]
    #[serde(default)]
    order: Option<String>,
    // Kept for: diagnostics and future validation.
    // Use when: validating that per-graph totals sum to the manifest total.
    #[expect(dead_code)]
    total_rows: u64,
    /// Maximum transaction t across all indexed records.
    #[serde(default)]
    max_t: i64,
    // Kept for: diagnostics (not needed for load).
    // Use when: validating expected graph counts.
    #[expect(dead_code)]
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

/// All dictionary and encoding state needed to translate between
/// human-readable IRIs/strings and compact integer IDs.
struct DictionarySet {
    predicates: PredicateDict,
    predicate_reverse: HashMap<String, u32>,
    /// Graph IRI → dict_index (0-based). g_id = dict_index + 1.
    graphs_reverse: HashMap<String, GraphId>,
    /// Subject forward packs keyed by ns_code.
    subject_forward_packs: std::collections::BTreeMap<u16, ForwardPackReader>,
    subject_reverse_tree: Option<DictTreeReader>,
    /// String forward pack reader (all string IDs in one stream).
    string_forward_packs: ForwardPackReader,
    string_reverse_tree: Option<DictTreeReader>,
    /// Total subject count across all namespaces (for DictOverlay watermark).
    subject_count: u32,
    /// Total string count (for DictOverlay watermark).
    string_count: u32,
    namespace_codes: HashMap<u16, String>,
    namespace_reverse: HashMap<String, u16>,
    prefix_trie: PrefixTrie,
    language_tags: LanguageTagDict,
    dt_sids: Vec<Sid>,
    numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena>,
    /// Per-predicate lazy vector arenas (on-demand shard loading). Key = p_id.
    vector_forward: HashMap<u32, super::vector_arena::LazyVectorArena>,
}

/// Per-graph, per-order branch manifests and leaf directories.
struct GraphIndexes {
    graphs: HashMap<GraphId, GraphIndex>,
}

/// Read-only store for querying Phase C binary columnar indexes.
///
/// Loads branch manifests for all available sort orders (SPOT, PSOT, POST, OPST),
/// along with shared dictionaries and mmap'd forward files. Provides methods to
/// find subjects by IRI, decode leaflet rows into `Flake` values, and translate
/// between Sid/FlakeValue and binary integer IDs.
pub struct BinaryIndexStore {
    dicts: DictionarySet,
    graph_indexes: GraphIndexes,
    /// Content store used to fetch CAS artifacts on demand (remote storages).
    ///
    /// When present, the store may lazily fetch dict leaves and index leaf files
    /// as they are accessed, rather than pre-downloading everything at startup.
    cas: Option<Arc<dyn ContentStore>>,
    /// Maximum t seen (from manifest or branch).
    max_t: i64,
    /// Base t from the initial full index build.
    base_t: i64,
    /// LRU cache for decoded leaflet regions.
    leaflet_cache: Option<Arc<LeafletCache>>,
}

impl BinaryIndexStore {
    /// Load a BinaryIndexStore from run_dir (dictionaries) and index_dir (index files).
    ///
    /// Loads per-order manifests (`index_manifest_spot.json`, `index_manifest_psot.json`,
    /// etc.) and builds per-graph, per-order branch manifests.
    pub fn load(run_dir: &Path, index_dir: &Path) -> io::Result<Self> {
        let _span = tracing::debug_span!("BinaryIndexStore::load", ?run_dir, ?index_dir).entered();

        // ---- Load per-order index manifests ----
        let mut graphs: HashMap<GraphId, GraphIndex> = HashMap::new();
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
                let branch_path = graph_dir.join(&entry.branch_cid);

                if branch_path.exists() {
                    let branch_bytes = std::fs::read(&branch_path)?;
                    let mut branch = read_branch_v2_from_bytes(&branch_bytes)?;

                    // Resolve leaf paths: leaf files are in graph_dir, named by CID string.
                    for leaf_entry in &mut branch.leaves {
                        leaf_entry.resolved_path =
                            Some(graph_dir.join(leaf_entry.leaf_cid.to_string()));
                    }

                    tracing::info!(
                        g_id = entry.g_id,
                        order = order.dir_name(),
                        leaves = branch.leaves.len(),
                        "loaded branch manifest"
                    );

                    let graph_index = graphs.entry(entry.g_id).or_insert_with(|| GraphIndex {
                        orders: HashMap::new(),
                    });

                    graph_index.orders.insert(
                        order,
                        OrderIndex {
                            branch,
                            leaf_dir: graph_dir,
                        },
                    );
                }
            }
            any_manifest_loaded = true;
        }

        if !any_manifest_loaded {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no per-order index manifests found (expected index_manifest_{spot,psot,post,opst}.json)",
            ));
        }

        // ---- Load predicate ids ----
        // Written by GlobalDicts::persist() as JSON array (id -> IRI).
        let predicates_json_path = run_dir.join("predicates.json");
        let (predicates, predicate_reverse) = if predicates_json_path.exists() {
            let bytes = std::fs::read(&predicates_json_path)?;
            let by_id: Vec<String> = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut dict = PredicateDict::new();
            let mut rev = HashMap::with_capacity(by_id.len());
            for (id, iri) in by_id.iter().enumerate() {
                dict.get_or_insert(iri);
                rev.insert(iri.clone(), id as u32);
            }
            tracing::info!(
                predicates = dict.len(),
                "loaded predicate ids (predicates.json)"
            );
            (dict, rev)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "missing predicates.json (predicate id→IRI table)",
            ));
        };

        // ---- Load namespace codes (before subject trees, needed for compression) ----
        let ns_path = run_dir.join("namespaces.json");
        let namespace_codes = if ns_path.exists() {
            load_namespace_codes(&ns_path)?
        } else {
            fluree_db_core::default_namespace_codes()
        };
        let namespace_reverse: HashMap<String, u16> = namespace_codes
            .iter()
            .map(|(&code, prefix)| (prefix.clone(), code))
            .collect();
        let prefix_trie = PrefixTrie::from_namespace_codes(&namespace_codes);
        tracing::info!(
            namespaces = namespace_codes.len(),
            trie_nodes = prefix_trie.node_count(),
            "loaded namespace codes + built prefix trie"
        );

        // ---- Build subject packs + reverse tree from flat files ----
        // Forward packs store suffix only (ns_code is in sid64), grouped by ns_code.
        // Reverse tree keys are [ns_code BE][suffix] for namespace compression.
        let subject_fwd_path = run_dir.join("subjects.fwd");
        let subject_idx_path = run_dir.join("subjects.idx");
        let sids_path = run_dir.join("subjects.sids");
        let (subject_forward_packs, subject_reverse_tree, subject_count) =
            if subject_idx_path.exists() && subject_fwd_path.exists() {
                let (offsets, lens) = read_forward_index(&subject_idx_path)?;
                let fwd_data = std::fs::read(&subject_fwd_path)?;
                let sids: Vec<u64> = if sids_path.exists() {
                    read_subject_sid_map(&sids_path)?
                } else {
                    (0..offsets.len() as u64).collect()
                };
                // Collect entries per namespace for forward packs, and reverse entries.
                let mut ns_entries: std::collections::BTreeMap<u16, Vec<(u64, Vec<u8>)>> =
                    std::collections::BTreeMap::new();
                let mut rev_entries: Vec<ReverseEntry> = Vec::with_capacity(sids.len());
                for (&sid, (&off, &len)) in sids.iter().zip(offsets.iter().zip(lens.iter())) {
                    let iri = &fwd_data[off as usize..(off as usize + len as usize)];
                    let iri_str = std::str::from_utf8(iri).unwrap_or("");
                    let ns_code = SubjectId::from_u64(sid).ns_code();
                    let prefix = namespace_codes
                        .get(&ns_code)
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    let suffix = if iri_str.starts_with(prefix) && !prefix.is_empty() {
                        &iri[prefix.len()..]
                    } else {
                        iri
                    };
                    let local_id = SubjectId::from_u64(sid).local_id();
                    ns_entries
                        .entry(ns_code)
                        .or_default()
                        .push((local_id, suffix.to_vec()));
                    rev_entries.push(ReverseEntry {
                        key: subject_reverse_key(ns_code, suffix),
                        id: sid,
                    });
                }
                // Build forward packs per namespace.
                let mut subject_packs = std::collections::BTreeMap::new();
                for (ns_code, mut entries) in ns_entries {
                    entries.sort_by_key(|(id, _)| *id);
                    let refs: Vec<(u64, &[u8])> =
                        entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
                    let result = pack_builder::build_subject_forward_packs_for_ns(
                        ns_code,
                        &refs,
                        pack_builder::DEFAULT_TARGET_PAGE_BYTES,
                        pack_builder::DEFAULT_TARGET_PACK_BYTES,
                    )?;
                    let pack_bytes: Vec<Arc<[u8]>> = result
                        .packs
                        .into_iter()
                        .map(|p| Arc::from(p.bytes.into_boxed_slice()))
                        .collect();
                    let reader = ForwardPackReader::from_memory(pack_bytes)?;
                    subject_packs.insert(ns_code, reader);
                }
                // Build reverse tree.
                rev_entries.sort_by(|a, b| a.key.cmp(&b.key));
                let rev_reader = build_dict_reader_reverse(rev_entries)?;
                let count = sids.len() as u32;
                tracing::info!(
                    subjects = count,
                    namespaces = subject_packs.len(),
                    "built subject packs + reverse tree from flat files"
                );
                (subject_packs, Some(rev_reader), count)
            } else {
                (std::collections::BTreeMap::new(), None, 0)
            };

        // ---- Build string packs + reverse tree from flat files ----
        // Strings use raw value bytes (no namespace compression).
        let string_fwd_path = run_dir.join("strings.fwd");
        let string_idx_path = run_dir.join("strings.idx");
        let (string_forward_packs, string_reverse_tree, string_count) =
            if string_idx_path.exists() && string_fwd_path.exists() {
                let (offsets, lens) = read_forward_index(&string_idx_path)?;
                let fwd_data = std::fs::read(&string_fwd_path)?;
                // Build pack entries: (str_id, value_bytes).
                let pack_entries: Vec<(u32, Vec<u8>)> = offsets
                    .iter()
                    .zip(lens.iter())
                    .enumerate()
                    .map(|(i, (&off, &len))| {
                        (
                            i as u32,
                            fwd_data[off as usize..(off as usize + len as usize)].to_vec(),
                        )
                    })
                    .collect();
                let pack_refs: Vec<(u32, &[u8])> = pack_entries
                    .iter()
                    .map(|(id, v)| (*id, v.as_slice()))
                    .collect();
                let result = pack_builder::build_string_forward_packs(
                    &pack_refs,
                    pack_builder::DEFAULT_TARGET_PAGE_BYTES,
                    pack_builder::DEFAULT_TARGET_PACK_BYTES,
                )?;
                let pack_bytes: Vec<Arc<[u8]>> = result
                    .packs
                    .into_iter()
                    .map(|p| Arc::from(p.bytes.into_boxed_slice()))
                    .collect();
                let fwd_reader = ForwardPackReader::from_memory(pack_bytes)?;
                // Build reverse entries for the reverse tree.
                let mut rev_entries: Vec<ReverseEntry> = pack_entries
                    .iter()
                    .map(|(id, v)| ReverseEntry {
                        key: v.clone(),
                        id: *id as u64,
                    })
                    .collect();
                rev_entries.sort_by(|a, b| a.key.cmp(&b.key));
                let rev_reader = build_dict_reader_reverse(rev_entries)?;
                let count = offsets.len() as u32;
                tracing::info!(
                    strings = count,
                    "built string packs + reverse tree from flat files"
                );
                (fwd_reader, Some(rev_reader), count)
            } else {
                (ForwardPackReader::empty(), None, 0)
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
        let dt_sids: Vec<Sid> = (0..dt_dict.len())
            .map(|id| {
                let iri = dt_dict.resolve(id).unwrap_or("");
                match prefix_trie.longest_match(iri) {
                    Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
                    None => Sid::new(0, iri),
                }
            })
            .collect();
        tracing::info!(datatypes = dt_dict.len(), "loaded datatype dict → dt_sids");

        // ---- Load graphs dict ----
        // Build reverse map: IRI → dict_index (0-based). g_id = dict_index + 1.
        let graphs_path = run_dir.join("graphs.dict");
        let graphs_reverse: HashMap<String, GraphId> = if graphs_path.exists() {
            let graphs_dict = read_predicate_dict(&graphs_path)?;
            let rev: HashMap<String, GraphId> = (0..graphs_dict.len())
                .filter_map(|id| {
                    graphs_dict
                        .resolve(id)
                        .map(|iri| (iri.to_string(), id as GraphId))
                })
                .collect();
            tracing::info!(graphs = graphs_dict.len(), "loaded graphs dict");
            rev
        } else {
            HashMap::new()
        };

        // ---- Load numbig arenas ----
        let nb_dir = run_dir.join("numbig");
        let mut numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena> = HashMap::new();
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

        // ---- Load vector arena metadata (lazy — shards loaded on demand) ----
        let vec_dir = run_dir.join("vectors");
        let mut vector_forward: HashMap<u32, super::vector_arena::LazyVectorArena> = HashMap::new();
        // Leaflet cache is created below; we build arenas after that.
        // Collect manifest + shard sources first.
        struct PendingArena {
            p_id: u32,
            manifest: super::vector_arena::VectorManifest,
            shard_sources: Vec<super::vector_arena::ShardSource>,
        }
        let mut pending_arenas: Vec<PendingArena> = Vec::new();
        if vec_dir.exists() && vec_dir.is_dir() {
            for entry in std::fs::read_dir(&vec_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("p_") && name.ends_with(".vam") {
                        let p_id_str = &name[2..name.len() - 4];
                        if let Ok(p_id) = p_id_str.parse::<u32>() {
                            let manifest_bytes = std::fs::read(&path)?;
                            let manifest =
                                super::vector_arena::read_vector_manifest(&manifest_bytes)?;

                            let mut shard_sources = Vec::with_capacity(manifest.shards.len());
                            for (shard_idx, shard_info) in manifest.shards.iter().enumerate() {
                                let shard_path =
                                    vec_dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
                                // Local builds don't have CIDs — use the manifest's
                                // CAS address as a stable identity for cache keying.
                                let cid_hash =
                                    LeafletCache::cid_cache_key(shard_info.cas.as_bytes());
                                let exists = shard_path.exists();
                                shard_sources.push(super::vector_arena::ShardSource {
                                    cid_hash,
                                    cid: None, // local FileStorage
                                    path: shard_path,
                                    on_disk: std::sync::atomic::AtomicBool::new(exists),
                                });
                            }
                            pending_arenas.push(PendingArena {
                                p_id,
                                manifest,
                                shard_sources,
                            });
                        }
                    }
                }
            }
        }

        // Log summary of loaded orders
        let mut order_summary = Vec::new();
        for &order in &all_orders {
            let graph_count = graphs
                .values()
                .filter(|g| g.orders.contains_key(&order))
                .count();
            if graph_count > 0 {
                order_summary.push(format!("{}({}g)", order.dir_name(), graph_count));
            }
        }
        tracing::info!(
            orders = %order_summary.join(", "),
            graphs = graphs.len(),
            "BinaryIndexStore loaded"
        );

        // ---- Initialize leaflet cache ----
        // Default: 8 GiB (tuned for large batched joins / repeated scans).
        // Override with `FLUREE_LEAFLET_CACHE_BYTES` (exact bytes).
        let leaflet_cache_bytes: u64 = std::env::var("FLUREE_LEAFLET_CACHE_BYTES")
            .ok()
            .and_then(|s| u64::from_str(&s).ok())
            .unwrap_or(8 * 1024 * 1024 * 1024);
        tracing::info!(
            leaflet_cache_bytes,
            leaflet_cache_gib = (leaflet_cache_bytes as f64) / (1024.0 * 1024.0 * 1024.0),
            "initializing leaflet cache"
        );
        let leaflet_cache = Some(Arc::new(LeafletCache::with_max_bytes(leaflet_cache_bytes)));

        // ---- Finalize lazy vector arenas (need cache handle) ----
        for pa in pending_arenas {
            let total = pa.manifest.total_count;
            let dims = pa.manifest.dims;
            let arena = super::vector_arena::LazyVectorArena::new(
                pa.manifest,
                pa.shard_sources,
                Arc::clone(leaflet_cache.as_ref().unwrap()),
                None, // FileStorage: all shards already on disk
            );
            vector_forward.insert(pa.p_id, arena);
            tracing::debug!(p_id = pa.p_id, total, dims, "registered lazy vector arena");
        }
        if !vector_forward.is_empty() {
            tracing::info!(
                predicates = vector_forward.len(),
                total_vectors = vector_forward
                    .values()
                    .map(|a| a.len() as usize)
                    .sum::<usize>(),
                "registered lazy vector arenas"
            );
        }

        Ok(Self {
            dicts: DictionarySet {
                predicates,
                predicate_reverse,
                graphs_reverse,
                subject_forward_packs,
                subject_reverse_tree,
                string_forward_packs,
                string_reverse_tree,
                subject_count,
                string_count,
                namespace_codes,
                namespace_reverse,
                prefix_trie,
                language_tags,
                dt_sids,
                numbig_forward,
                vector_forward,
            },
            graph_indexes: GraphIndexes { graphs },
            cas: None,
            max_t,
            // Fresh bulk builds populate R3 for all records, so time-travel
            // is supported from t=1. Legacy behavior was `base_t: max_t` which
            // disabled time-travel for bulk builds.
            base_t: 1,
            leaflet_cache,
        })
    }

    /// Load a BinaryIndexStore using a shared leaflet cache.
    ///
    /// The provided cache is shared across all stores that receive the same
    /// `Arc`, giving one global budget instead of per-store budgets. This
    /// is the recommended constructor for multi-ledger deployments.
    ///
    /// Pass `None` to disable caching entirely (e.g., during index builds).
    pub fn load_with_cache(
        run_dir: &Path,
        index_dir: &Path,
        cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let mut store = Self::load(run_dir, index_dir)?;
        store.leaflet_cache = cache;
        Ok(store)
    }

    /// Load from a v5 binary root (`IRB1`) with default leaflet cache.
    pub async fn load_from_root_v5_default(
        cs: Arc<dyn ContentStore>,
        root: &super::index_root::IndexRootV5,
        cache_dir: &Path,
    ) -> io::Result<Self> {
        let leaflet_cache_bytes: u64 = std::env::var("FLUREE_LEAFLET_CACHE_BYTES")
            .ok()
            .and_then(|s| u64::from_str(&s).ok())
            .unwrap_or(8 * 1024 * 1024 * 1024);
        let cache = Some(Arc::new(LeafletCache::with_max_bytes(leaflet_cache_bytes)));
        Self::load_from_root_v5(cs, root, cache_dir, cache).await
    }

    /// Load from raw root bytes in IRB1 binary format.
    ///
    /// Decodes an `IndexRootV5` from the given bytes and loads the store.
    /// Returns an error if the bytes are not valid IRB1 format.
    pub async fn load_from_root_bytes(
        cs: Arc<dyn ContentStore>,
        bytes: &[u8],
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let v5 = super::index_root::IndexRootV5::decode(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("index root: expected IRB1 format: {e}"),
            )
        })?;
        Self::load_from_root_v5(cs, &v5, cache_dir, leaflet_cache).await
    }

    /// Load from raw root bytes with default leaflet cache.
    pub async fn load_from_root_bytes_default(
        cs: Arc<dyn ContentStore>,
        bytes: &[u8],
        cache_dir: &Path,
    ) -> io::Result<Self> {
        let leaflet_cache_bytes: u64 = std::env::var("FLUREE_LEAFLET_CACHE_BYTES")
            .ok()
            .and_then(|s| u64::from_str(&s).ok())
            .unwrap_or(8 * 1024 * 1024 * 1024);
        let cache = Some(Arc::new(LeafletCache::with_max_bytes(leaflet_cache_bytes)));
        Self::load_from_root_bytes(cs, bytes, cache_dir, cache).await
    }

    /// Load from a v5 binary root (`IRB1`).
    ///
    /// Default graph (g_id=0) routing is inline — leaf entries are embedded in
    /// the root, saving a branch fetch. Named graphs still require a branch
    /// CID fetch.
    pub async fn load_from_root_v5(
        cs: Arc<dyn ContentStore>,
        root: &super::index_root::IndexRootV5,
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        tracing::info!("BinaryIndexStore::load_from_root_v5 starting");

        std::fs::create_dir_all(cache_dir)?;

        // ---- Load predicate ids (inline in root) ----
        let (predicates, predicate_reverse) = {
            let mut dict = PredicateDict::new();
            let mut rev = HashMap::with_capacity(root.predicate_sids.len());

            for (p_id, (ns_code, suffix)) in root.predicate_sids.iter().enumerate() {
                let prefix = root.namespace_codes.get(ns_code).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "predicate_sids[{}]: unknown namespace code {}",
                            p_id, ns_code
                        ),
                    )
                })?;
                let iri = format!("{}{}", prefix, suffix);
                dict.get_or_insert(&iri);
                rev.insert(iri, p_id as u32);
            }

            tracing::info!(predicates = dict.len(), "loaded predicate sids from root");
            (dict, rev)
        };

        // ---- Load subject forward packs + reverse tree from CAS ----
        let mut subject_forward_packs = std::collections::BTreeMap::new();
        for (ns_code, ns_refs) in &root.dict_refs.forward_packs.subject_fwd_ns_packs {
            let reader = ForwardPackReader::from_pack_refs(
                Arc::clone(&cs),
                cache_dir,
                ns_refs,
                KIND_SUBJECT_FWD,
                *ns_code,
            )
            .await?;
            subject_forward_packs.insert(*ns_code, reader);
        }
        let subject_reverse_tree = Some(
            load_dict_tree_from_cas(
                Arc::clone(&cs),
                &root.dict_refs.subject_reverse,
                cache_dir,
                "srl",
                leaflet_cache.as_ref(),
            )
            .await?,
        );
        tracing::info!(
            subj_fwd_ns = subject_forward_packs.len(),
            subj_fwd_packs = subject_forward_packs
                .values()
                .map(|r| r.pack_count())
                .sum::<usize>(),
            subj_rev_entries = subject_reverse_tree.as_ref().unwrap().total_entries(),
            "loaded subject dict packs + reverse tree"
        );

        // ---- Load string forward packs + reverse tree from CAS ----
        let string_forward_packs = ForwardPackReader::from_pack_refs(
            Arc::clone(&cs),
            cache_dir,
            &root.dict_refs.forward_packs.string_fwd_packs,
            KIND_STRING_FWD,
            0,
        )
        .await?;
        let string_reverse_tree = Some(
            load_dict_tree_from_cas(
                Arc::clone(&cs),
                &root.dict_refs.string_reverse,
                cache_dir,
                "trl",
                leaflet_cache.as_ref(),
            )
            .await?,
        );
        tracing::info!(
            str_fwd_packs = string_forward_packs.pack_count(),
            str_rev_entries = string_reverse_tree.as_ref().unwrap().total_entries(),
            "loaded string dict packs + reverse tree"
        );

        // ---- Namespace codes ----
        let namespace_codes: HashMap<u16, String> = root
            .namespace_codes
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        let namespace_reverse: HashMap<String, u16> = namespace_codes
            .iter()
            .map(|(&code, prefix)| (prefix.clone(), code))
            .collect();
        let prefix_trie = PrefixTrie::from_namespace_codes(&namespace_codes);
        tracing::info!(
            namespaces = namespace_codes.len(),
            trie_nodes = prefix_trie.node_count(),
            "loaded namespace codes + built prefix trie"
        );

        // ---- Language tags (inline) ----
        let mut language_tags = LanguageTagDict::new();
        for tag in &root.language_tags {
            language_tags.get_or_insert(Some(tag));
        }
        tracing::info!(tags = language_tags.len(), "loaded language dict");

        // ---- Datatype dict → dt_sids (inline) ----
        if root.datatype_iris.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index root missing datatype_iris",
            ));
        }
        let dt_sids: Vec<Sid> = root
            .datatype_iris
            .iter()
            .map(|iri| match prefix_trie.longest_match(iri) {
                Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
                None => Sid::new(0, iri),
            })
            .collect();
        tracing::info!(datatypes = dt_sids.len(), "loaded datatype dict → dt_sids");

        // ---- Graphs dict (inline) ----
        if root.graph_iris.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index root missing graph_iris",
            ));
        }
        let graphs_reverse: HashMap<String, GraphId> = root
            .graph_iris
            .iter()
            .enumerate()
            .map(|(id, iri)| (iri.to_string(), id as GraphId))
            .collect();
        tracing::info!(graphs = graphs_reverse.len(), "loaded graphs dict");

        // ---- Numbig arenas (u32 keys, no string parsing) ----
        let mut numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena> = HashMap::new();
        for (p_id, cid) in &root.dict_refs.numbig {
            let bytes = fetch_cached_bytes(cs.as_ref(), cid, cache_dir, "nba").await?;
            let arena = super::numbig_dict::read_numbig_arena_from_bytes(&bytes)?;
            numbig_forward.insert(*p_id, arena);
        }
        if !numbig_forward.is_empty() {
            tracing::info!(
                predicates = numbig_forward.len(),
                total_entries = numbig_forward.values().map(|a| a.len()).sum::<usize>(),
                "loaded numbig arenas"
            );
        }

        // ---- Vector arenas (lazy — only manifest loaded, shards on demand) ----
        let mut vector_forward: HashMap<u32, super::vector_arena::LazyVectorArena> = HashMap::new();
        for entry in &root.dict_refs.vectors {
            let manifest_bytes =
                fetch_cached_bytes(cs.as_ref(), &entry.manifest, cache_dir, "vam").await?;
            let manifest = super::vector_arena::read_vector_manifest(&manifest_bytes)?;

            let mut shard_sources = Vec::with_capacity(entry.shards.len());
            for shard_cid in &entry.shards {
                let cid_hash = LeafletCache::cid_cache_key(&shard_cid.to_bytes());
                if let Some(local) = cs.resolve_local_path(shard_cid) {
                    // FileStorage: direct path, on disk
                    shard_sources.push(super::vector_arena::ShardSource {
                        cid_hash,
                        cid: None,
                        path: local,
                        on_disk: std::sync::atomic::AtomicBool::new(true),
                    });
                } else {
                    // Remote: derive cache path, check if already cached
                    let cache_path = cache_dir.join(format!("{}.vas", shard_cid));
                    let exists = cache_path.exists();
                    shard_sources.push(super::vector_arena::ShardSource {
                        cid_hash,
                        cid: Some(shard_cid.clone()),
                        path: cache_path,
                        on_disk: std::sync::atomic::AtomicBool::new(exists),
                    });
                }
            }

            let arena = super::vector_arena::LazyVectorArena::new(
                manifest,
                shard_sources,
                Arc::clone(leaflet_cache.as_ref().unwrap()),
                Some(Arc::clone(&cs)),
            );
            vector_forward.insert(entry.p_id, arena);
        }
        if !vector_forward.is_empty() {
            tracing::info!(
                predicates = vector_forward.len(),
                total_vectors = vector_forward
                    .values()
                    .map(|a| a.len() as usize)
                    .sum::<usize>(),
                "registered lazy vector arenas"
            );
        }

        // ---- Build graph indexes ----
        let mut graphs: HashMap<GraphId, GraphIndex> = HashMap::new();

        // Default graph (g_id=0): leaf entries are inline in root.
        // No branch fetch required.
        {
            let mut order_indexes = HashMap::new();
            for inline_order in &root.default_graph_orders {
                let mut branch = BranchManifest {
                    leaves: inline_order.leaves.clone(),
                };

                // Resolve leaf paths
                let mut local_resolved = 0usize;
                let mut remote_mapped = 0usize;
                for leaf_entry in &mut branch.leaves {
                    if let Some(local_path) = cs.resolve_local_path(&leaf_entry.leaf_cid) {
                        leaf_entry.resolved_path = Some(local_path);
                        local_resolved += 1;
                    } else {
                        leaf_entry.resolved_path =
                            Some(cache_dir.join(leaf_entry.leaf_cid.to_string()));
                        remote_mapped += 1;
                    }
                }

                tracing::info!(
                    g_id = 0u16,
                    order = inline_order.order.dir_name(),
                    leaves = branch.leaves.len(),
                    local_resolved,
                    remote_mapped,
                    "default graph: inline routing"
                );

                order_indexes.insert(
                    inline_order.order,
                    OrderIndex {
                        branch,
                        leaf_dir: cache_dir.to_path_buf(),
                    },
                );
            }
            if !order_indexes.is_empty() {
                graphs.insert(
                    0,
                    GraphIndex {
                        orders: order_indexes,
                    },
                );
            }
        }

        // Named graphs: fetch branch CID → parse → leaf entries.
        for ng in &root.named_graphs {
            let mut order_indexes = HashMap::new();

            for (order, branch_cid) in &ng.orders {
                let branch_bytes =
                    fetch_cached_bytes_cid(cs.as_ref(), branch_cid, cache_dir).await?;
                let mut branch = read_branch_v2_from_bytes(&branch_bytes)?;

                // Resolve leaf paths
                let mut local_resolved = 0usize;
                let mut remote_mapped = 0usize;
                for leaf_entry in &mut branch.leaves {
                    if let Some(local_path) = cs.resolve_local_path(&leaf_entry.leaf_cid) {
                        leaf_entry.resolved_path = Some(local_path);
                        local_resolved += 1;
                    } else {
                        leaf_entry.resolved_path =
                            Some(cache_dir.join(leaf_entry.leaf_cid.to_string()));
                        remote_mapped += 1;
                    }
                }

                tracing::info!(
                    g_id = ng.g_id,
                    order = order.dir_name(),
                    leaves = branch.leaves.len(),
                    local_resolved,
                    remote_mapped,
                    "named graph: loaded branch manifest"
                );

                order_indexes.insert(
                    *order,
                    OrderIndex {
                        branch,
                        leaf_dir: cache_dir.to_path_buf(),
                    },
                );
            }

            graphs.insert(
                ng.g_id,
                GraphIndex {
                    orders: order_indexes,
                },
            );
        }

        // Log summary
        let all_orders = [
            RunSortOrder::Spot,
            RunSortOrder::Psot,
            RunSortOrder::Post,
            RunSortOrder::Opst,
        ];
        let mut order_summary = Vec::new();
        for &order in &all_orders {
            let graph_count = graphs
                .values()
                .filter(|g| g.orders.contains_key(&order))
                .count();
            if graph_count > 0 {
                order_summary.push(format!("{}({}g)", order.dir_name(), graph_count));
            }
        }
        tracing::info!(
            orders = %order_summary.join(", "),
            graphs = graphs.len(),
            max_t = root.index_t,
            base_t = root.base_t,
            "BinaryIndexStore loaded from IRB1 root"
        );

        // Derive counts from root watermarks for DictOverlay watermark tracking.
        let subject_count = subject_reverse_tree
            .as_ref()
            .map(|t| t.total_entries() as u32)
            .unwrap_or(0);
        let string_count = root.string_watermark;

        Ok(Self {
            dicts: DictionarySet {
                predicates,
                predicate_reverse,
                graphs_reverse,
                subject_forward_packs,
                subject_reverse_tree,
                string_forward_packs,
                string_reverse_tree,
                subject_count,
                string_count,
                namespace_codes,
                namespace_reverse,
                prefix_trie,
                language_tags,
                dt_sids,
                numbig_forward,
                vector_forward,
            },
            graph_indexes: GraphIndexes { graphs },
            cas: Some(Arc::clone(&cs)),
            max_t: root.index_t,
            base_t: root.base_t,
            leaflet_cache,
        })
    }

    /// Log dictionary I/O stats (reverse tree disk reads vs cache hits).
    pub fn log_dict_stats(&self) {
        if let Some(tree) = &self.dicts.subject_reverse_tree {
            tracing::info!(
                disk_reads = tree.disk_reads(),
                cache_hits = tree.cache_hits(),
                "dict_stats: subject_reverse"
            );
        }
        if let Some(tree) = &self.dicts.string_reverse_tree {
            tracing::info!(
                disk_reads = tree.disk_reads(),
                cache_hits = tree.cache_hits(),
                "dict_stats: string_reverse"
            );
        }
    }

    /// Replace the leaflet cache on an already-loaded store.
    ///
    /// Use this to attach a shared cache after construction, or to
    /// disable caching by passing `None`. Propagates to dict tree readers
    /// so they share the same global budget.
    pub fn set_leaflet_cache(&mut self, cache: Option<Arc<LeafletCache>>) {
        // Propagate to reverse dict tree readers (forward dicts use packs, no cache).
        if let Some(tree) = &mut self.dicts.subject_reverse_tree {
            tree.set_cache(cache.clone());
        }
        if let Some(tree) = &mut self.dicts.string_reverse_tree {
            tree.set_cache(cache.clone());
        }
        self.leaflet_cache = cache;
    }

    /// Ensure an index leaf file exists on local disk for this store.
    ///
    /// For file-backed content stores, leaf paths resolve directly and no fetch is needed.
    /// For remote stores (e.g., S3), this lazily downloads the leaf by CID the first time
    /// a cursor attempts to open it.
    pub fn ensure_index_leaf_cached(
        &self,
        leaf_cid: &ContentId,
        leaf_path: &Path,
    ) -> io::Result<()> {
        if leaf_path.exists() {
            return Ok(());
        }
        let Some(cs) = &self.cas else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "index leaf missing and no CAS configured: {}",
                    leaf_path.display()
                ),
            ));
        };

        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("index leaf download requires a Tokio runtime"))?;
        let cs = Arc::clone(cs);
        let cid = leaf_cid.clone();
        let leaf_path = leaf_path.to_path_buf();
        let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>, String>>(1);
        std::thread::spawn(move || {
            let res = handle
                .block_on(async { cs.get(&cid).await })
                .map_err(|e| e.to_string());
            let _ = tx.send(res);
        });
        let bytes = rx
            .recv()
            .map_err(|_| io::Error::other("index leaf fetch thread died"))?
            .map_err(io::Error::other)?;

        let cache_dir = leaf_path
            .parent()
            .ok_or_else(|| io::Error::other("leaf path has no parent dir"))?;
        std::fs::create_dir_all(cache_dir)?;
        std::fs::write(&leaf_path, &bytes)?;
        Ok(())
    }

    // ========================================================================
    // IRI encoding / decoding
    // ========================================================================

    /// Encode a full IRI into a `Sid` using namespace codes.
    ///
    /// Uses the PrefixTrie for O(len(iri)) longest-prefix matching.
    /// Returns `Sid::new(0, iri)` if no prefix matches (default namespace).
    pub fn encode_iri(&self, iri: &str) -> Sid {
        match self.dicts.prefix_trie.longest_match(iri) {
            Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
            None => Sid::new(0, iri),
        }
    }

    /// Reconstruct a full IRI from a Sid (namespace_code + name).
    ///
    /// Looks up the namespace prefix by code and concatenates with the name.
    pub fn sid_to_iri(&self, sid: &Sid) -> String {
        let prefix = self
            .dicts
            .namespace_codes
            .get(&sid.namespace_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        format!("{}{}", prefix, sid.name)
    }

    /// Resolve s_id → full IRI string via the forward dictionary pack.
    ///
    /// The forward pack stores only the suffix (namespace prefix stripped),
    /// keyed by local_id within each namespace. Reconstructs the full IRI
    /// by prepending the namespace prefix using the ns_code from the sid64.
    pub fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        let sid = SubjectId::from_u64(s_id);
        let ns_code = sid.ns_code();
        let reader = self
            .dicts
            .subject_forward_packs
            .get(&ns_code)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code {}", ns_code),
                )
            })?;
        let suffix = reader.forward_lookup_str(sid.local_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "s_id {} (ns={}, local={}) not found in subject forward pack",
                    s_id,
                    ns_code,
                    sid.local_id()
                ),
            )
        })?;
        let prefix = self
            .dicts
            .namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        Ok(format!("{}{}", prefix, suffix))
    }

    /// Append the full IRI bytes for s_id to `out` (zero-copy hot path).
    ///
    /// Writes namespace prefix + suffix directly to the buffer without
    /// intermediate String allocation. Returns `Err` if the ID is not found.
    pub fn write_subject_iri_bytes(&self, s_id: u64, out: &mut Vec<u8>) -> io::Result<()> {
        let sid = SubjectId::from_u64(s_id);
        let ns_code = sid.ns_code();
        let prefix = self.namespace_prefix(ns_code)?;
        out.extend_from_slice(prefix.as_bytes());
        let reader = self
            .dicts
            .subject_forward_packs
            .get(&ns_code)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code {}", ns_code),
                )
            })?;
        if reader.forward_lookup_into(sid.local_id(), out)? {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("s_id {} not found in subject forward pack", s_id),
            ))
        }
    }

    /// Resolve p_id → full IRI string.
    pub fn resolve_predicate_iri(&self, p_id: u32) -> Option<&str> {
        self.dicts.predicates.resolve(p_id)
    }

    /// Find p_id from full IRI.
    pub fn find_predicate_id(&self, iri: &str) -> Option<u32> {
        self.dicts.predicate_reverse.get(iri).copied()
    }

    /// Resolve a string dict entry by ID via the forward dictionary pack.
    pub fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        self.dicts
            .string_forward_packs
            .forward_lookup_str(str_id as u64)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("str_id {} not found in string forward pack", str_id),
                )
            })
    }

    /// Append the string value bytes for str_id to `out` (zero-copy hot path).
    ///
    /// Returns `Err` if the ID is not found.
    pub fn write_string_value_bytes(&self, str_id: u32, out: &mut Vec<u8>) -> io::Result<()> {
        if self
            .dicts
            .string_forward_packs
            .forward_lookup_into(str_id as u64, out)?
        {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("str_id {} not found in string forward pack", str_id),
            ))
        }
    }

    // ========================================================================
    // Sid ↔ integer ID translation (Phase 2: query key translation)
    // ========================================================================

    /// Translate a Sid to s_id via the reverse hash index.
    ///
    /// Reconstructs the full IRI from the Sid, then looks it up in subjects.rev.
    pub fn sid_to_s_id(&self, sid: &Sid) -> io::Result<Option<u64>> {
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
            FlakeValue::Ref(sid) => match self.sid_to_s_id(sid)? {
                Some(s_id) => Ok(Some((ObjKind::REF_ID, ObjKey::from_u64(s_id)))),
                None => Ok(None),
            },
            FlakeValue::String(s) => match self.find_string_id(s)? {
                Some(str_id) => Ok(Some((ObjKind::LEX_ID, ObjKey::encode_u32_id(str_id)))),
                None => Ok(None),
            },
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
            // Temporal types — order-preserving encoding enables index scans.
            FlakeValue::DateTime(dt) => Ok(Some((
                ObjKind::DATE_TIME,
                ObjKey::encode_datetime(dt.epoch_micros()),
            ))),
            FlakeValue::Date(d) => Ok(Some((
                ObjKind::DATE,
                ObjKey::encode_date(d.days_since_epoch()),
            ))),
            FlakeValue::Time(t) => Ok(Some((
                ObjKind::TIME,
                ObjKey::encode_time(t.micros_since_midnight()),
            ))),
            FlakeValue::GYear(g) => Ok(Some((ObjKind::G_YEAR, ObjKey::encode_g_year(g.year())))),
            FlakeValue::GYearMonth(g) => Ok(Some((
                ObjKind::G_YEAR_MONTH,
                ObjKey::encode_g_year_month(g.year(), g.month()),
            ))),
            FlakeValue::GMonth(g) => {
                Ok(Some((ObjKind::G_MONTH, ObjKey::encode_g_month(g.month()))))
            }
            FlakeValue::GDay(g) => Ok(Some((ObjKind::G_DAY, ObjKey::encode_g_day(g.day())))),
            FlakeValue::GMonthDay(g) => Ok(Some((
                ObjKind::G_MONTH_DAY,
                ObjKey::encode_g_month_day(g.month(), g.day()),
            ))),
            // BigInt/Decimal need p_id for numbig arena → use value_to_obj_pair_for_predicate
            FlakeValue::BigInt(_) | FlakeValue::Decimal(_) => Ok(None),
            // GeoPoint — packed lat/lng encoding enables latitude-band index scans
            FlakeValue::GeoPoint(bits) => {
                Ok(Some((ObjKind::GEO_POINT, ObjKey::from_u64(bits.as_u64()))))
            }
            // Vector/Json/Duration: not yet supported for key translation
            _ => Ok(None),
        }
    }

    /// Translate a RangeMatch into min/max RunRecord bounds for a given sort order.
    ///
    /// Returns `None` if any required lookup fails (meaning 0 results are possible).
    /// The RunRecord bounds define the key range to search in the branch manifest.
    pub fn translate_range(
        &self,
        s: Option<&Sid>,
        p: Option<&Sid>,
        o: Option<&FlakeValue>,
        _order: RunSortOrder,
        g_id: GraphId,
    ) -> io::Result<Option<(RunRecord, RunRecord)>> {
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
            s_id: SubjectId::from_u64(s_id.unwrap_or(0)),
            p_id: p_id.unwrap_or(0),
            dt: 0,
            o_kind: min_o_kind,
            op: 0,
            o_key: min_o_key,
            t: 0,
            lang_id: 0,
            i: 0,
        };

        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(s_id.unwrap_or(u64::MAX)),
            p_id: p_id.unwrap_or(u32::MAX),
            dt: u16::MAX,
            o_kind: max_o_kind,
            op: 1,
            o_key: max_o_key,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };

        Ok(Some((min_key, max_key)))
    }

    // ========================================================================
    // Subject reverse lookup (IRI → s_id)
    // ========================================================================

    /// Find s_id for an IRI via the reverse dict tree (O(log N) B-tree search).
    ///
    /// Parses the IRI into `(ns_code, suffix)` via the prefix trie, builds a
    /// compressed reverse key `[ns_code BE][suffix]`, and looks it up.
    pub fn find_subject_id(&self, iri: &str) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let (ns_code, prefix_len) =
                    self.dicts.prefix_trie.longest_match(iri).unwrap_or((0, 0));
                let suffix = &iri[prefix_len..];
                let key = subject_reverse_key(ns_code, suffix.as_bytes());
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    /// Find subject ID by namespace code and suffix (avoids IRI construction).
    ///
    /// Same as `find_subject_id(iri)` but skips prefix_trie decomposition
    /// when the caller already has `(ns_code, suffix)` from a `Sid`.
    pub fn find_subject_id_by_parts(&self, ns_code: u16, suffix: &str) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let key = subject_reverse_key(ns_code, suffix.as_bytes());
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    /// Get the namespace prefix for a given namespace code.
    ///
    /// Returns `Err` if the code is not in `namespace_codes` (corrupt root).
    pub fn namespace_prefix(&self, ns_code: u16) -> io::Result<&str> {
        self.dicts
            .namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("namespace code {} not in index root", ns_code),
                )
            })
    }

    /// Augment namespace codes with entries not yet in the index root.
    ///
    /// When novelty introduces new namespace codes (e.g., a transaction uses a
    /// prefix that wasn't present at index time), this method adds them so that
    /// `namespace_prefix()`, `encode_iri()`, and `sid_to_iri()` can resolve
    /// the new codes without requiring a reindex.
    ///
    /// This is safe to call multiple times — existing codes are not overwritten.
    pub fn augment_namespace_codes(&mut self, extra: &HashMap<u16, String>) {
        let mut changed = false;
        for (code, prefix) in extra {
            match self.dicts.namespace_codes.get(code) {
                Some(existing) if existing != prefix => {
                    tracing::warn!(
                        ns_code = code,
                        existing_prefix = %existing,
                        new_prefix = %prefix,
                        "namespace code maps to different prefix in index vs novelty — keeping index mapping"
                    );
                }
                Some(_) => {} // Already present with same prefix
                None => {
                    self.dicts.namespace_codes.insert(*code, prefix.clone());
                    self.dicts.namespace_reverse.insert(prefix.clone(), *code);
                    changed = true;
                }
            }
        }
        if changed {
            self.dicts.prefix_trie = PrefixTrie::from_namespace_codes(&self.dicts.namespace_codes);
        }
    }

    // ========================================================================
    // String reverse lookup (string → str_id)
    // ========================================================================

    /// Find str_id for a string value via the reverse dict tree (O(log N) B-tree search).
    pub fn find_string_id(&self, value: &str) -> io::Result<Option<u32>> {
        match &self.dicts.string_reverse_tree {
            Some(tree) => tree
                .reverse_lookup(value.as_bytes())
                .map(|opt| opt.map(|id| id as u32)),
            None => Ok(None),
        }
    }

    // ========================================================================
    // Graph + branch access (multi-order)
    // ========================================================================

    /// Get the SPOT branch manifest for a graph (backward-compatible shorthand).
    pub fn branch(&self, g_id: GraphId) -> Option<&BranchManifest> {
        self.branch_for_order(g_id, RunSortOrder::Spot)
    }

    /// Get the branch manifest for a graph and sort order.
    pub fn branch_for_order(&self, g_id: GraphId, order: RunSortOrder) -> Option<&BranchManifest> {
        self.graph_indexes
            .graphs
            .get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| &oi.branch)
    }

    /// Get the SPOT leaf directory for a graph (backward-compatible shorthand).
    pub fn leaf_dir(&self, g_id: GraphId) -> Option<&Path> {
        self.leaf_dir_for_order(g_id, RunSortOrder::Spot)
    }

    /// Get the leaf directory for a graph and sort order.
    pub fn leaf_dir_for_order(&self, g_id: GraphId, order: RunSortOrder) -> Option<&Path> {
        self.graph_indexes
            .graphs
            .get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| oi.leaf_dir.as_path())
    }

    /// Check if a given sort order is available for a graph.
    pub fn has_order(&self, g_id: GraphId, order: RunSortOrder) -> bool {
        self.graph_indexes
            .graphs
            .get(&g_id)
            .is_some_and(|g| g.orders.contains_key(&order))
    }

    /// Get the available sort orders for a graph.
    pub fn available_orders(&self, g_id: GraphId) -> Vec<RunSortOrder> {
        self.graph_indexes
            .graphs
            .get(&g_id)
            .map(|g| {
                let mut orders: Vec<_> = g.orders.keys().copied().collect();
                orders.sort_by_key(|o| o.dir_name());
                orders
            })
            .unwrap_or_default()
    }

    /// Get the set of graph IDs available.
    pub fn graph_ids(&self) -> Vec<GraphId> {
        let mut ids: Vec<_> = self.graph_indexes.graphs.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Look up a named graph by IRI, returning its g_id if found.
    ///
    /// Graph IDs: 0 = default graph, 1 = txn-meta, 2+ = user-defined.
    /// Returns `None` if the graph IRI is not in the dictionary.
    pub fn graph_id_for_iri(&self, iri: &str) -> Option<GraphId> {
        // graphs_reverse stores dict_index (0-based), g_id = dict_index + 1
        self.dicts.graphs_reverse.get(iri).map(|&idx| idx + 1)
    }

    // ========================================================================
    // Namespace codes access
    // ========================================================================

    /// Get the namespace code → prefix mapping.
    pub fn namespace_codes(&self) -> &HashMap<u16, String> {
        &self.dicts.namespace_codes
    }

    /// Get the prefix trie for IRI encoding.
    pub fn prefix_trie(&self) -> &PrefixTrie {
        &self.dicts.prefix_trie
    }

    /// Number of subjects in the forward dictionary.
    pub fn subject_count(&self) -> u32 {
        self.dicts.subject_count
    }

    /// Number of predicates in the dictionary.
    pub fn predicate_count(&self) -> u32 {
        self.dicts.predicates.len()
    }

    /// Number of strings in the forward dictionary.
    pub fn string_count(&self) -> u32 {
        self.dicts.string_count
    }

    /// Number of language tags in the dictionary.
    pub fn language_tag_count(&self) -> u16 {
        self.dicts.language_tags.len()
    }

    /// Get pre-computed dt_sids (dt_id → Sid).
    pub fn dt_sids(&self) -> &[Sid] {
        &self.dicts.dt_sids
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
        self.leaflet_cache.as_deref()
    }

    /// Find dt_id for a datatype Sid.
    ///
    /// Iterates the pre-computed `dt_sids` vec (typically <20 entries).
    /// Returns `None` if the datatype is not in the dictionary.
    pub fn find_dt_id(&self, dt_sid: &Sid) -> Option<u16> {
        self.dicts
            .dt_sids
            .iter()
            .position(|s| s == dt_sid)
            .map(|i| i as u16)
    }

    /// Find lang_id for a language tag string.
    ///
    /// Uses the language dict's reverse lookup.
    /// Returns `None` if the tag is not found.
    pub fn find_lang_id(&self, tag: &str) -> Option<u16> {
        self.dicts.language_tags.find_id(tag)
    }

    /// Resolve a lang_id to its language tag string.
    ///
    /// Returns `None` if the ID is 0 (no language) or not found.
    pub fn resolve_lang_id(&self, lang_id: u16) -> Option<&str> {
        if lang_id == 0 {
            None
        } else {
            self.dicts.language_tags.resolve(lang_id)
        }
    }

    // ========================================================================
    // Value → ObjKind/ObjKey translation
    // ========================================================================

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
                if let Some(arena) = self.dicts.numbig_forward.get(&p_id) {
                    if let Some(handle) = arena.find_bigint(bi) {
                        return Ok(Some((ObjKind::NUM_BIG, ObjKey::encode_u32_id(handle))));
                    }
                }
                Ok(None) // BigInt not in arena for this predicate
            }
            FlakeValue::Decimal(bd) => {
                // Decimal → numbig arena lookup (read-only)
                if let Some(arena) = self.dicts.numbig_forward.get(&p_id) {
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
        self.dicts.numbig_forward.get(&p_id)
    }

    /// Get the vector arena for a predicate (for arena-direct f32 scoring).
    pub fn vector_arena(&self, p_id: u32) -> Option<&super::vector_arena::LazyVectorArena> {
        self.dicts.vector_forward.get(&p_id)
    }

    /// Look up a single vector by predicate and handle.
    ///
    /// Returns a `VectorSlice` that borrows the shard through the cache.
    /// The shard is loaded on demand if not already cached.
    pub fn get_vector_f32(
        &self,
        p_id: u32,
        handle: u32,
    ) -> io::Result<Option<super::vector_arena::VectorSlice>> {
        match self.dicts.vector_forward.get(&p_id) {
            Some(arena) => arena.lookup_vector(handle),
            None => Ok(None),
        }
    }

    /// Check whether vectors for a predicate are all unit-normalized.
    pub fn is_vector_normalized(&self, p_id: u32) -> bool {
        self.dicts
            .vector_forward
            .get(&p_id)
            .is_some_and(|arena| arena.is_normalized())
    }

    // ========================================================================
    // Row → Flake conversion
    // ========================================================================

    /// Convert a decoded leaflet row into a Flake.
    ///
    /// Does IRI round-trip: `id → IRI string → encode_iri(iri) → Sid`.
    /// PrefixTrie makes this O(len(iri)) per call.
    pub fn row_to_flake(&self, row: &DecodedRow) -> io::Result<Flake> {
        // Subject: s_id → IRI → Sid
        let s_iri = self.resolve_subject_iri(row.s_id)?;
        let s_sid = self.encode_iri(&s_iri);

        // Predicate: p_id → IRI → Sid
        let p_iri = self.resolve_predicate_iri(row.p_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("p_id {} not in predicate dict", row.p_id),
            )
        })?;
        let p_sid = self.encode_iri(p_iri);

        // Object: (ObjKind, ObjKey) → FlakeValue
        let o_val = self.decode_value(row.o_kind, row.o_key, row.p_id)?;

        // Datatype: dt_id → Sid via pre-computed dt_sids vec
        let dt_sid = self
            .dicts
            .dt_sids
            .get(row.dt as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, ""));

        // Meta: lang + list index
        let meta = self.decode_meta(row.lang_id, row.i);

        Ok(Flake::new(s_sid, p_sid, o_val, dt_sid, row.t, true, meta))
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
            let ref_s_id = o_key;
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
            let time =
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, frac_micros * 1000)
                    .unwrap_or(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
            let iso = time.format("%H:%M:%S%.6f").to_string();
            return match fluree_db_core::temporal::Time::parse(&iso) {
                Ok(t) => Ok(FlakeValue::Time(Box::new(t))),
                Err(_) => Ok(FlakeValue::String(iso)),
            };
        }
        if o_kind == ObjKind::DATE_TIME.as_u8() {
            let epoch_micros = ObjKey::from_u64(o_key).decode_datetime();
            let dt = chrono::DateTime::from_timestamp_micros(epoch_micros).unwrap_or_default();
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
            if let Some(arena) = self.dicts.numbig_forward.get(&p_id) {
                if let Some(stored) = arena.get_by_handle(handle) {
                    return Ok(stored.to_flake_value());
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("NUM_BIG handle {} not found for p_id={}", handle, p_id),
            ));
        }
        if o_kind == ObjKind::VECTOR_ID.as_u8() {
            let handle = o_key as u32;
            let arena = self.dicts.vector_forward.get(&p_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("no vector arena for p_id={}", p_id),
                )
            })?;
            let vs = arena.lookup_vector(handle)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("VECTOR_ID handle {} not found for p_id={}", handle, p_id),
                )
            })?;
            // Upcast f32→f64 for FlakeValue::Vector. Because f:vector values are
            // quantized to f32 at ingest, this upcast is lossless.
            let f64_vec: Vec<f64> = vs.as_f32().iter().map(|&x| x as f64).collect();
            return Ok(FlakeValue::Vector(f64_vec));
        }
        if o_kind == ObjKind::G_YEAR.as_u8() {
            let year = ObjKey::from_u64(o_key).decode_g_year();
            return Ok(FlakeValue::GYear(Box::new(
                fluree_db_core::temporal::GYear::from_year(year),
            )));
        }
        if o_kind == ObjKind::G_YEAR_MONTH.as_u8() {
            let (year, month) = ObjKey::from_u64(o_key).decode_g_year_month();
            return Ok(FlakeValue::GYearMonth(Box::new(
                fluree_db_core::temporal::GYearMonth::from_components(year, month),
            )));
        }
        if o_kind == ObjKind::G_MONTH.as_u8() {
            let month = ObjKey::from_u64(o_key).decode_g_month();
            return Ok(FlakeValue::GMonth(Box::new(
                fluree_db_core::temporal::GMonth::from_month(month),
            )));
        }
        if o_kind == ObjKind::G_DAY.as_u8() {
            let day = ObjKey::from_u64(o_key).decode_g_day();
            return Ok(FlakeValue::GDay(Box::new(
                fluree_db_core::temporal::GDay::from_day(day),
            )));
        }
        if o_kind == ObjKind::G_MONTH_DAY.as_u8() {
            let (month, day) = ObjKey::from_u64(o_key).decode_g_month_day();
            return Ok(FlakeValue::GMonthDay(Box::new(
                fluree_db_core::temporal::GMonthDay::from_components(month, day),
            )));
        }
        if o_kind == ObjKind::YEAR_MONTH_DUR.as_u8() {
            let months = ObjKey::from_u64(o_key).decode_year_month_dur();
            return Ok(FlakeValue::YearMonthDuration(Box::new(
                fluree_db_core::temporal::YearMonthDuration::from_months(months),
            )));
        }
        if o_kind == ObjKind::DAY_TIME_DUR.as_u8() {
            let micros = ObjKey::from_u64(o_key).decode_day_time_dur();
            return Ok(FlakeValue::DayTimeDuration(Box::new(
                fluree_db_core::temporal::DayTimeDuration::from_micros(micros),
            )));
        }
        if o_kind == ObjKind::GEO_POINT.as_u8() {
            return Ok(FlakeValue::GeoPoint(fluree_db_core::GeoPointBits(o_key)));
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
        let has_idx = i_val != ListIndex::none().as_i32();

        if !has_lang && !has_idx {
            return None;
        }

        let mut meta = FlakeMeta::new();
        if has_lang {
            if let Some(tag) = self.dicts.language_tags.resolve(lang_id) {
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
    pub fn query_subject_flakes(&self, g_id: GraphId, s_id: u64) -> io::Result<Vec<Flake>> {
        self.query_subject_predicate_flakes(g_id, s_id, None)
    }

    /// Query flakes for a subject (optionally filtered by predicate) in a graph.
    /// Uses the SPOT index.
    pub fn query_subject_predicate_flakes(
        &self,
        g_id: GraphId,
        s_id: u64,
        p_id: Option<u32>,
    ) -> io::Result<Vec<Flake>> {
        // Return empty if graph has no index (e.g., empty default graph when only named graphs have data)
        let branch = match self.branch_for_order(g_id, RunSortOrder::Spot) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let _leaf_dir = match self.leaf_dir_for_order(g_id, RunSortOrder::Spot) {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        let leaf_range = branch.find_leaves_for_subject(s_id);
        let mut flakes = Vec::new();

        for leaf_idx in leaf_range {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("leaf {} has no resolved path", leaf_entry.leaf_cid),
                )
            })?;
            let leaf_data = std::fs::read(leaf_path)?;
            let header = read_leaf_header(&leaf_data)?;

            for dir_entry in &header.leaflet_dir {
                let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                if end > leaf_data.len() {
                    break;
                }
                let leaflet_bytes = &leaf_data[dir_entry.offset as usize..end];
                let decoded = decode_leaflet(
                    leaflet_bytes,
                    header.p_width,
                    header.dt_width,
                    RunSortOrder::Spot,
                )?;

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

                    let decoded_row = DecodedRow {
                        s_id: row_s_id,
                        p_id: row_p_id,
                        o_kind: decoded.o_kinds[row],
                        o_key: decoded.o_keys[row],
                        dt: decoded.dt_values[row],
                        t: decoded.t_values[row] as i64,
                        lang_id: decoded.lang.as_ref().map_or(0, |c| c.get(row as u16)),
                        i: decoded
                            .i_col
                            .as_ref()
                            .map_or(ListIndex::none().as_i32(), |c| c.get(row as u16)),
                    };
                    let flake = self.row_to_flake(&decoded_row)?;
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

/// Map a `fluree_db_core` storage error to `io::Error`, preserving NotFound.
fn storage_to_io_error(e: fluree_db_core::error::Error) -> io::Error {
    let kind = match &e {
        fluree_db_core::error::Error::NotFound(_) => io::ErrorKind::NotFound,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, e.to_string())
}

/// Write bytes to `cache_dir/{hash}.{ext}` if not already cached.
///
/// Content-addressed files are immutable: if the file already exists at the
/// expected path, same hash = same content, skip the write. Uses atomic
/// write (temp + rename) to prevent partial reads by concurrent accessors.
/// Temp file names include PID + timestamp to avoid clobbering across
/// concurrent writers.
///
/// Returns the path to the (possibly pre-existing) cached file.
fn cache_bytes_to_file(
    cache_dir: &Path,
    hash: &str,
    ext: &str,
    bytes: &[u8],
) -> io::Result<PathBuf> {
    let target = cache_dir.join(format!("{}.{}", hash, ext));
    if target.exists() {
        return Ok(target);
    }
    std::fs::create_dir_all(cache_dir)?;
    // Unique tmp name: PID + monotonic counter to avoid races between
    // concurrent writers (threads / processes / warm-start lambdas).
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = cache_dir.join(format!(".cas_{}_{}.{}.tmp", std::process::id(), seq, ext));
    std::fs::write(&tmp, bytes)?;
    if let Err(_rename_err) = std::fs::rename(&tmp, &target) {
        // Rename failed — concurrent writer may have created target (Windows),
        // or on Unix with different filesystems. Content-addressed: same hash =
        // same content, so if target now exists we can discard our tmp.
        let _ = std::fs::remove_file(&tmp);
        if !target.exists() {
            return Err(io::Error::other(format!(
                "failed to cache {}.{} to {:?}",
                hash, ext, cache_dir
            )));
        }
    }
    Ok(target)
}

/// Fetch artifact bytes by CID: reads from local file if available (file storage),
/// then local cache, otherwise downloads from content store and writes to cache.
async fn fetch_cached_bytes(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
    ext: &str,
) -> io::Result<Vec<u8>> {
    // For file storage: read directly from the CAS file, no cache needed.
    if let Some(local_path) = cs.resolve_local_path(id) {
        return std::fs::read(&local_path);
    }
    let hash = id.digest_hex();
    let cached = cache_dir.join(format!("{}.{}", hash, ext));
    if cached.exists() {
        return std::fs::read(&cached);
    }
    let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
    cache_bytes_to_file(cache_dir, &hash, ext, &bytes)?;
    Ok(bytes)
}

/// Fetch artifact bytes by CID using CID-string filenames (no extension).
///
/// Like `fetch_cached_bytes` but uses `cid.to_string()` as the cache filename
/// instead of `{hash}.{ext}`. Preferred for CID-native artifacts (branches, leaves).
async fn fetch_cached_bytes_cid(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
) -> io::Result<Vec<u8>> {
    if let Some(local_path) = cs.resolve_local_path(id) {
        return std::fs::read(&local_path);
    }
    let cached = cache_dir.join(id.to_string());
    if cached.exists() {
        return std::fs::read(&cached);
    }
    let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
    std::fs::create_dir_all(cache_dir)?;
    std::fs::write(&cached, &bytes)?;
    Ok(bytes)
}

/// Build an in-memory `DictTreeReader` from reverse entries (key → id).
///
/// Entries must be pre-sorted by key.
fn build_dict_reader_reverse(entries: Vec<ReverseEntry>) -> io::Result<DictTreeReader> {
    let result = builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)?;
    let mut leaf_map = HashMap::new();
    for (artifact, bl) in result.leaves.iter().zip(result.branch.leaves.iter()) {
        leaf_map.insert(bl.address.clone(), artifact.bytes.clone());
    }
    Ok(DictTreeReader::from_memory(result.branch, leaf_map))
}

/// Load a `DictTreeReader` from CAS by fetching branch + leaf artifacts via CID.
///
/// Downloads branch manifest, decodes it, validates that the branch's
/// leaf addresses match the root's leaf CID list (GC integrity), then builds
/// a demand-loading reader backed by `LeafSource::LocalFiles`.
///
/// For storage backends with local files (e.g., `FileContentStore`), leaf paths
/// are resolved directly via `resolve_local_path` — no download required.
/// For remote backends, leaves are fetched to `cache_dir` on demand at
/// lookup time.
///
/// If `leaflet_cache` is provided, the reader will use it for in-memory
/// caching of leaf blobs (keyed by `xxh3_128(cas_address)`, immutable).
async fn load_dict_tree_from_cas(
    cs: Arc<dyn ContentStore>,
    refs: &super::index_root::DictTreeRefs,
    cache_dir: &Path,
    _leaf_ext: &str,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<DictTreeReader> {
    fn leaf_digest_hex_from_address(address: &str) -> Option<&str> {
        // Branch leaf addresses are stored as Fluree CAS addresses for file storage, e.g.:
        //   fluree:file://<ledger>/main/index/objects/dicts/<sha256>.dict
        // Root leaf lists store ContentIds; for comparison we need just the digest hex.
        let path = parse_fluree_address(address)
            .map(|p| p.path)
            .unwrap_or(address);
        let file = path.rsplit('/').next().unwrap_or(path);
        let digest = file.split('.').next().unwrap_or(file);
        // Current CAS digests are sha256 hex (64 chars).
        if digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit()) {
            Some(digest)
        } else {
            None
        }
    }

    // Fetch and decode branch
    let branch_bytes = fetch_cached_bytes(cs.as_ref(), &refs.branch, cache_dir, "dtb").await?;
    let branch = DictBranch::decode(&branch_bytes)?;

    // Validate branch/root leaf list consistency.
    if branch.leaves.len() != refs.leaves.len() {
        tracing::warn!(
            branch_leaves = branch.leaves.len(),
            root_leaves = refs.leaves.len(),
            "dict tree: branch leaf count does not match root leaf list"
        );
    }

    // Branch leaf addresses are CAS addresses; check that the digest appears in the root leaf list.
    // This is a GC integrity check: roots are used as retention sets for reachable leaf objects.
    //
    // NOTE: Use a HashSet for O(1) membership. The prior O(n*m) scan could be extremely slow
    // on large dict trees and also spam warnings if address formats didn't match.
    let root_digests: HashSet<String> = refs.leaves.iter().map(|cid| cid.digest_hex()).collect();
    let mut missing_count = 0usize;
    let mut unparsable_count = 0usize;
    let mut sample: Vec<&str> = Vec::new();
    for bl in &branch.leaves {
        match leaf_digest_hex_from_address(&bl.address) {
            Some(digest) => {
                if !root_digests.contains(digest) {
                    missing_count += 1;
                    if sample.len() < 3 {
                        sample.push(&bl.address);
                    }
                }
            }
            None => {
                unparsable_count += 1;
                if sample.len() < 3 {
                    sample.push(&bl.address);
                }
            }
        }
    }
    if missing_count > 0 || unparsable_count > 0 {
        tracing::warn!(
            branch_leaves = branch.leaves.len(),
            root_leaves = refs.leaves.len(),
            missing = missing_count,
            unparsable = unparsable_count,
            sample = ?sample,
            "dict tree: branch leaf references not represented in root leaf list (GC safety warning)"
        );
    }

    // Build leaf sources without pre-downloading.
    // - For file storage: use resolved local CAS paths.
    // - For remote storage: keep CID mapping and fetch leaf bytes on demand at lookup time.
    let mut local_files = HashMap::with_capacity(branch.leaves.len());
    let mut remote_cids = HashMap::new();
    let mut local_resolved = 0usize;
    let mut remote_mapped = 0usize;

    for (cid, bl) in refs.leaves.iter().zip(branch.leaves.iter()) {
        // Try direct local resolution first (zero-copy for file storage)
        if let Some(local_path) = cs.resolve_local_path(cid) {
            local_files.insert(bl.address.clone(), local_path);
            local_resolved += 1;
        } else {
            // Remote storage: map leaf address → CID, fetch on demand.
            remote_cids.insert(bl.address.clone(), cid.clone());
            remote_mapped += 1;
        }
    }

    tracing::info!(
        leaves = branch.leaves.len(),
        local_resolved,
        remote_mapped,
        "dict tree leaf sources resolved"
    );

    let leaf_source = if remote_mapped > 0 {
        LeafSource::CasOnDemand {
            cs: Arc::clone(&cs),
            local_files,
            remote_cids,
        }
    } else {
        LeafSource::LocalFiles(local_files)
    };
    match leaflet_cache {
        Some(cache) => Ok(DictTreeReader::with_cache(
            branch,
            leaf_source,
            Arc::clone(cache),
        )),
        None => Ok(DictTreeReader::new(branch, leaf_source)),
    }
}

/// Load namespace codes from namespaces.json.
fn load_namespace_codes(path: &Path) -> io::Result<HashMap<u16, String>> {
    let json_str = std::fs::read_to_string(path)?;
    let entries: Vec<serde_json::Value> = serde_json::from_str(&json_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut codes = HashMap::with_capacity(entries.len());
    for entry in entries {
        let code = entry["code"]
            .as_u64()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing code"))?
            as u16;
        let prefix = entry["prefix"]
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing prefix"))?
            .to_string();
        codes.insert(code, prefix);
    }
    Ok(codes)
}
