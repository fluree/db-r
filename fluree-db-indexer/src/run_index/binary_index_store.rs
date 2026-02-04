//! BinaryIndexStore: loads Phase C binary columnar indexes and dictionaries from disk.
//!
//! Holds everything needed to read the bulk indexes and convert numeric IDs
//! back to `Flake` values. The store is read-only and snapshot-only (latest state).
//!
//! Supports all four sort orders (SPOT, PSOT, POST, OPST) via per-graph, per-order
//! branch manifests. Dictionaries (subjects, strings, predicates, datatypes) are shared
//! across all orders.

use super::branch::{find_branch_file, read_branch_manifest, read_branch_manifest_from_bytes, BranchManifest};
use super::dict_io::{
    read_forward_index,
    read_language_dict, read_language_dict_from_bytes,
    read_predicate_dict,
    read_predicate_dict_from_bytes,
    read_subject_sid_map,
};
use super::index_root::BinaryIndexRootV2;
use super::global_dict::{LanguageTagDict, PredicateDict};
use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::leaflet_cache::LeafletCache;
use super::prefix_trie::PrefixTrie;
use super::run_record::{RunRecord, RunSortOrder};
use fluree_db_core::ListIndex;
use crate::dict_tree::{DictTreeReader, DictBranch};
use crate::dict_tree::reader::LeafSource;
use crate::dict_tree::forward_leaf::ForwardEntry;
use crate::dict_tree::reverse_leaf::{ReverseEntry, subject_reverse_key};
use crate::dict_tree::builder;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKind, ObjKey};
use fluree_db_core::storage::{extract_hash_from_address, StorageRead};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use std::collections::HashMap;
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

/// All dictionary and encoding state needed to translate between
/// human-readable IRIs/strings and compact integer IDs.
struct DictionarySet {
    predicates: PredicateDict,
    predicate_reverse: HashMap<String, u32>,
    subject_forward_tree: Option<DictTreeReader>,
    subject_reverse_tree: Option<DictTreeReader>,
    string_forward_tree: Option<DictTreeReader>,
    string_reverse_tree: Option<DictTreeReader>,
    namespace_codes: HashMap<u16, String>,
    #[allow(dead_code)]
    namespace_reverse: HashMap<String, u16>,
    prefix_trie: PrefixTrie,
    language_tags: LanguageTagDict,
    dt_sids: Vec<Sid>,
    numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena>,
}

/// Per-graph, per-order branch manifests and leaf directories.
struct GraphIndexes {
    graphs: HashMap<u32, GraphIndex>,
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
            tracing::info!(predicates = dict.len(), "loaded predicate ids (predicates.json)");
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

        // ---- Build subject trees from flat files ----
        // Forward tree stores suffix only (ns_code is in sid64).
        // Reverse tree keys are [ns_code BE][suffix] for namespace compression.
        let subject_fwd_path = run_dir.join("subjects.fwd");
        let subject_idx_path = run_dir.join("subjects.idx");
        let sids_path = run_dir.join("subjects.sids");
        let (subject_forward_tree, subject_reverse_tree) =
            if subject_idx_path.exists() && subject_fwd_path.exists() {
                let (offsets, lens) = read_forward_index(&subject_idx_path)?;
                let fwd_data = std::fs::read(&subject_fwd_path)?;
                let sids: Vec<u64> = if sids_path.exists() {
                    read_subject_sid_map(&sids_path)?
                } else {
                    (0..offsets.len() as u64).collect()
                };
                // Build forward entries with suffix-only values (strip namespace prefix)
                let mut fwd_entries: Vec<ForwardEntry> = Vec::with_capacity(sids.len());
                let mut rev_entries: Vec<ReverseEntry> = Vec::with_capacity(sids.len());
                for (&sid, (&off, &len)) in sids.iter().zip(offsets.iter().zip(lens.iter())) {
                    let iri = &fwd_data[off as usize..(off as usize + len as usize)];
                    let iri_str = std::str::from_utf8(iri).unwrap_or("");
                    let ns_code = SubjectId::from_u64(sid).ns_code();
                    let prefix = namespace_codes.get(&ns_code).map(|s| s.as_str()).unwrap_or("");
                    let suffix = if iri_str.starts_with(prefix) && !prefix.is_empty() {
                        &iri[prefix.len()..]
                    } else {
                        iri
                    };
                    fwd_entries.push(ForwardEntry {
                        id: sid,
                        value: suffix.to_vec(),
                    });
                    rev_entries.push(ReverseEntry {
                        key: subject_reverse_key(ns_code, suffix),
                        id: sid,
                    });
                }
                fwd_entries.sort_by_key(|e| e.id);
                rev_entries.sort_by(|a, b| a.key.cmp(&b.key));
                let fwd_reader = build_dict_reader_forward(fwd_entries)?;
                let rev_reader = build_dict_reader_reverse(rev_entries)?;
                tracing::info!(subjects = sids.len(), "built subject trees from flat files (ns-compressed)");
                (Some(fwd_reader), Some(rev_reader))
            } else {
                (None, None)
            };

        // ---- Build string trees from flat files ----
        // Strings use raw value bytes (no namespace compression).
        let string_fwd_path = run_dir.join("strings.fwd");
        let string_idx_path = run_dir.join("strings.idx");
        let (string_forward_tree, string_reverse_tree) =
            if string_idx_path.exists() && string_fwd_path.exists() {
                let (offsets, lens) = read_forward_index(&string_idx_path)?;
                let fwd_data = std::fs::read(&string_fwd_path)?;
                let fwd_entries: Vec<ForwardEntry> = offsets.iter()
                    .zip(lens.iter())
                    .enumerate()
                    .map(|(i, (&off, &len))| ForwardEntry {
                        id: i as u64,
                        value: fwd_data[off as usize..(off as usize + len as usize)].to_vec(),
                    })
                    .collect();
                let mut rev_entries: Vec<ReverseEntry> = fwd_entries.iter()
                    .map(|e| ReverseEntry { key: e.value.clone(), id: e.id })
                    .collect();
                rev_entries.sort_by(|a, b| a.key.cmp(&b.key));
                let fwd_reader = build_dict_reader_forward(fwd_entries)?;
                let rev_reader = build_dict_reader_reverse(rev_entries)?;
                tracing::info!(strings = offsets.len(), "built string trees from flat files");
                (Some(fwd_reader), Some(rev_reader))
            } else {
                (None, None)
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

        Ok(Self {
            dicts: DictionarySet {
                predicates,
                predicate_reverse,
                subject_forward_tree,
                subject_reverse_tree,
                string_forward_tree,
                string_reverse_tree,
                namespace_codes,
                namespace_reverse,
                prefix_trie,
                language_tags,
                dt_sids,
                numbig_forward,
            },
            graph_indexes: GraphIndexes { graphs },
            max_t,
            base_t: max_t,
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

    /// Load a BinaryIndexStore from CAS (content-addressed storage) using
    /// a v2 index root.
    ///
    /// Fetches dictionary and index artifacts via `storage.read_bytes()`,
    /// materializes mmap-required files to `cache_dir`, and eagerly
    /// downloads all leaf files for sync cursor compatibility.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend for reading CAS artifacts
    /// * `root` - V2 index root with CAS addresses for all artifacts
    /// * `cache_dir` - Local directory for cached files (mmap'd dicts + leaves)
    /// * `leaflet_cache` - Optional shared LRU cache for decoded leaflets
    pub async fn load_from_root<S: StorageRead>(
        storage: &S,
        root: &BinaryIndexRootV2,
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        tracing::info!("BinaryIndexStore::load_from_root starting");

        std::fs::create_dir_all(cache_dir)?;

        // ---- Load predicate ids (inline in root; avoid redundant dict download) ----
        let (predicates, predicate_reverse) = {
            let mut dict = PredicateDict::new();
            let mut rev = HashMap::with_capacity(root.predicate_sids.len());

            // Precompute prefix lookups (ns_code -> prefix)
            for (p_id, (ns_code, suffix)) in root.predicate_sids.iter().enumerate() {
                let prefix = root.namespace_codes.get(ns_code).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("predicate_sids[{}]: unknown namespace code {}", p_id, ns_code),
                    )
                })?;
                let iri = format!("{}{}", prefix, suffix);
                dict.get_or_insert(&iri);
                rev.insert(iri, p_id as u32);
            }

            tracing::info!(predicates = dict.len(), "loaded predicate sids from root");
            (dict, rev)
        };

        // ---- Load subject dict trees from CAS ----
        let subject_forward_tree = Some(
            load_dict_tree_from_cas(storage, &root.dict_addresses.subject_forward, cache_dir, "sdl", leaflet_cache.as_ref()).await?
        );
        let subject_reverse_tree = Some(
            load_dict_tree_from_cas(storage, &root.dict_addresses.subject_reverse, cache_dir, "srl", leaflet_cache.as_ref()).await?
        );
        tracing::info!(
            subj_fwd_entries = subject_forward_tree.as_ref().unwrap().total_entries(),
            subj_rev_entries = subject_reverse_tree.as_ref().unwrap().total_entries(),
            "loaded subject dict trees"
        );

        // ---- Load string dict trees from CAS ----
        let string_forward_tree = Some(
            load_dict_tree_from_cas(storage, &root.dict_addresses.string_forward, cache_dir, "tfl", leaflet_cache.as_ref()).await?
        );
        let string_reverse_tree = Some(
            load_dict_tree_from_cas(storage, &root.dict_addresses.string_reverse, cache_dir, "trl", leaflet_cache.as_ref()).await?
        );
        tracing::info!(
            str_fwd_entries = string_forward_tree.as_ref().unwrap().total_entries(),
            str_rev_entries = string_reverse_tree.as_ref().unwrap().total_entries(),
            "loaded string dict trees"
        );

        // ---- Namespace codes (from root, not from storage) ----
        let namespace_codes: HashMap<u16, String> = root.namespace_codes
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

        // ---- Load language tag dict (cache-aware) ----
        let lang_bytes = fetch_cached_bytes(storage, &root.dict_addresses.languages, cache_dir, "dict").await?;
        let language_tags = if !lang_bytes.is_empty() {
            read_language_dict_from_bytes(&lang_bytes)?
        } else {
            LanguageTagDict::new()
        };
        tracing::info!(tags = language_tags.len(), "loaded language dict");

        // ---- Load datatype dict → pre-compute dt_sids (cache-aware) ----
        let dt_bytes = fetch_cached_bytes(storage, &root.dict_addresses.datatypes, cache_dir, "dict").await?;
        let dt_dict = read_predicate_dict_from_bytes(&dt_bytes)?;
        let dt_sids: Vec<Sid> = (0..dt_dict.len()).map(|id| {
            let iri = dt_dict.resolve(id).unwrap_or("");
            match prefix_trie.longest_match(iri) {
                Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
                None => Sid::new(0, iri),
            }
        }).collect();
        tracing::info!(datatypes = dt_dict.len(), "loaded datatype dict → dt_sids");

        // ---- Load numbig arenas (cache-aware) ----
        let mut numbig_forward: HashMap<u32, super::numbig_dict::NumBigArena> = HashMap::new();
        for (p_id_str, addr) in &root.dict_addresses.numbig {
            if let Ok(p_id) = p_id_str.parse::<u32>() {
                let bytes = fetch_cached_bytes(storage, addr, cache_dir, "nba").await?;
                let arena = super::numbig_dict::read_numbig_arena_from_bytes(&bytes)?;
                numbig_forward.insert(p_id, arena);
            }
        }
        if !numbig_forward.is_empty() {
            tracing::info!(
                predicates = numbig_forward.len(),
                total_entries = numbig_forward.values().map(|a| a.len()).sum::<usize>(),
                "loaded numbig arenas"
            );
        }

        // ---- Load per-graph, per-order branch manifests + leaves ----
        let mut graphs: HashMap<u32, GraphIndex> = HashMap::new();

        for graph_entry in &root.graphs {
            let mut order_indexes = HashMap::new();

            for (order_name, order_addrs) in &graph_entry.orders {
                let order = RunSortOrder::from_dir_name(order_name).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown sort order: {}", order_name),
                    )
                })?;

                // Load branch manifest (cache-aware), resolving leaf paths against cache_dir
                let branch_bytes = fetch_cached_bytes(storage, &order_addrs.branch, cache_dir, "fbr").await?;
                let branch = read_branch_manifest_from_bytes(&branch_bytes, Some(cache_dir))?;
                tracing::info!(
                    g_id = graph_entry.g_id,
                    order = order_name,
                    leaves = branch.leaves.len(),
                    "loaded branch manifest"
                );

                // Eagerly download leaf files to cache_dir (skip on cache hit).
                // Sync cursors open these via File::open or leaf_dir.join(path).
                for leaf_addr in &order_addrs.leaves {
                    ensure_leaf_cached(storage, leaf_addr, cache_dir).await?;
                }

                order_indexes.insert(order, OrderIndex {
                    branch,
                    leaf_dir: cache_dir.to_path_buf(),
                });
            }

            graphs.insert(graph_entry.g_id, GraphIndex { orders: order_indexes });
        }

        // Log summary of loaded orders
        let all_orders = [RunSortOrder::Spot, RunSortOrder::Psot, RunSortOrder::Post, RunSortOrder::Opst];
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
            max_t = root.index_t,
            base_t = root.base_t,
            "BinaryIndexStore loaded from CAS root"
        );

        Ok(Self {
            dicts: DictionarySet {
                predicates,
                predicate_reverse,
                subject_forward_tree,
                subject_reverse_tree,
                string_forward_tree,
                string_reverse_tree,
                namespace_codes,
                namespace_reverse,
                prefix_trie,
                language_tags,
                dt_sids,
                numbig_forward,
            },
            graph_indexes: GraphIndexes { graphs },
            max_t: root.index_t,
            base_t: root.base_t,
            leaflet_cache,
        })
    }

    /// Load from a v2 index root with default leaflet cache.
    ///
    /// Uses 8 GiB cache (or `FLUREE_LEAFLET_CACHE_BYTES` env var).
    pub async fn load_from_root_default<S: StorageRead>(
        storage: &S,
        root: &BinaryIndexRootV2,
        cache_dir: &Path,
    ) -> io::Result<Self> {
        let leaflet_cache_bytes: u64 = std::env::var("FLUREE_LEAFLET_CACHE_BYTES")
            .ok()
            .and_then(|s| u64::from_str(&s).ok())
            .unwrap_or(8 * 1024 * 1024 * 1024);
        let cache = Some(Arc::new(LeafletCache::with_max_bytes(leaflet_cache_bytes)));
        Self::load_from_root(storage, root, cache_dir, cache).await
    }

    /// Replace the leaflet cache on an already-loaded store.
    ///
    /// Use this to attach a shared cache after construction, or to
    /// disable caching by passing `None`. Propagates to dict tree readers
    /// so they share the same global budget.
    pub fn set_leaflet_cache(&mut self, cache: Option<Arc<LeafletCache>>) {
        // Propagate to dict tree readers
        if let Some(tree) = &mut self.dicts.subject_forward_tree {
            tree.set_cache(cache.clone());
        }
        if let Some(tree) = &mut self.dicts.subject_reverse_tree {
            tree.set_cache(cache.clone());
        }
        if let Some(tree) = &mut self.dicts.string_forward_tree {
            tree.set_cache(cache.clone());
        }
        if let Some(tree) = &mut self.dicts.string_reverse_tree {
            tree.set_cache(cache.clone());
        }
        self.leaflet_cache = cache;
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
        let prefix = self.dicts.namespace_codes
            .get(&sid.namespace_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        format!("{}{}", prefix, sid.name)
    }

    /// Resolve s_id → full IRI string via the forward dict tree.
    ///
    /// The forward tree stores only the suffix (namespace prefix stripped).
    /// Reconstructs the full IRI by prepending the namespace prefix looked
    /// up from `namespace_codes` using the ns_code embedded in the sid64.
    pub fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        let tree = self.dicts.subject_forward_tree.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "subject forward tree not loaded")
        })?;
        let suffix_bytes = tree.forward_lookup(s_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("s_id {} not found in subject forward tree", s_id),
            )
        })?;
        let suffix = String::from_utf8(suffix_bytes).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })?;
        let ns_code = SubjectId::from_u64(s_id).ns_code();
        let prefix = self.dicts.namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        Ok(format!("{}{}", prefix, suffix))
    }

    /// Resolve p_id → full IRI string.
    pub fn resolve_predicate_iri(&self, p_id: u32) -> Option<&str> {
        self.dicts.predicates.resolve(p_id)
    }

    /// Find p_id from full IRI.
    pub fn find_predicate_id(&self, iri: &str) -> Option<u32> {
        self.dicts.predicate_reverse.get(iri).copied()
    }

    /// Resolve a string dict entry by ID via the forward dict tree.
    pub fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        let tree = self.dicts.string_forward_tree.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "string forward tree not loaded")
        })?;
        tree.forward_lookup_str(str_id as u64)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("str_id {} not found in string forward tree", str_id),
            )
        })
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
            FlakeValue::Ref(sid) => {
                match self.sid_to_s_id(sid)? {
                    Some(s_id) => Ok(Some((ObjKind::REF_ID, ObjKey::from_u64(s_id)))),
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
        use fluree_db_core::ListIndex;

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
            s_id: SubjectId::from_u64(s_id.unwrap_or(0)),
            p_id: p_id.unwrap_or(0),
            dt: 0,
            o_kind: min_o_kind,
            op: 0,
            o_key: min_o_key,
            t: i64::MIN,
            lang_id: 0,
            i: ListIndex::none().as_i32(),
        };

        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(s_id.unwrap_or(u64::MAX)),
            p_id: p_id.unwrap_or(u32::MAX),
            dt: u16::MAX,
            o_kind: max_o_kind,
            op: 1,
            o_key: max_o_key,
            t: i64::MAX,
            lang_id: u16::MAX,
            i: i32::MAX,
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
                let (ns_code, prefix_len) = self.dicts.prefix_trie
                    .longest_match(iri)
                    .unwrap_or((0, 0));
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
        self.dicts.namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("namespace code {} not in index root", ns_code),
                )
            })
    }

    // ========================================================================
    // String reverse lookup (string → str_id)
    // ========================================================================

    /// Find str_id for a string value via the reverse dict tree (O(log N) B-tree search).
    pub fn find_string_id(&self, value: &str) -> io::Result<Option<u32>> {
        match &self.dicts.string_reverse_tree {
            Some(tree) => tree.reverse_lookup(value.as_bytes()).map(|opt| opt.map(|id| id as u32)),
            None => Ok(None),
        }
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
        self.graph_indexes.graphs.get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| &oi.branch)
    }

    /// Get the SPOT leaf directory for a graph (backward-compatible shorthand).
    pub fn leaf_dir(&self, g_id: u32) -> Option<&Path> {
        self.leaf_dir_for_order(g_id, RunSortOrder::Spot)
    }

    /// Get the leaf directory for a graph and sort order.
    pub fn leaf_dir_for_order(&self, g_id: u32, order: RunSortOrder) -> Option<&Path> {
        self.graph_indexes.graphs.get(&g_id)
            .and_then(|g| g.orders.get(&order))
            .map(|oi| oi.leaf_dir.as_path())
    }

    /// Check if a given sort order is available for a graph.
    pub fn has_order(&self, g_id: u32, order: RunSortOrder) -> bool {
        self.graph_indexes.graphs.get(&g_id)
            .map_or(false, |g| g.orders.contains_key(&order))
    }

    /// Get the available sort orders for a graph.
    pub fn available_orders(&self, g_id: u32) -> Vec<RunSortOrder> {
        self.graph_indexes.graphs.get(&g_id)
            .map(|g| {
                let mut orders: Vec<_> = g.orders.keys().copied().collect();
                orders.sort_by_key(|o| o.dir_name());
                orders
            })
            .unwrap_or_default()
    }

    /// Get the set of graph IDs available.
    pub fn graph_ids(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self.graph_indexes.graphs.keys().copied().collect();
        ids.sort();
        ids
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
        self.dicts.subject_forward_tree.as_ref()
            .map(|t| t.total_entries() as u32)
            .unwrap_or(0)
    }

    /// Number of predicates in the dictionary.
    pub fn predicate_count(&self) -> u32 {
        self.dicts.predicates.len()
    }

    /// Number of strings in the forward dictionary.
    pub fn string_count(&self) -> u32 {
        self.dicts.string_forward_tree.as_ref()
            .map(|t| t.total_entries() as u32)
            .unwrap_or(0)
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
        self.dicts.dt_sids.iter().position(|s| s == dt_sid).map(|i| i as u16)
    }

    /// Find lang_id for a language tag string.
    ///
    /// Uses the language dict's reverse lookup.
    /// Returns `None` if the tag is not found.
    pub fn find_lang_id(&self, tag: &str) -> Option<u16> {
        self.dicts.language_tags.find_id(tag)
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

    // ========================================================================
    // Row → Flake conversion
    // ========================================================================

    /// Convert a decoded leaflet row into a Flake.
    ///
    /// Does IRI round-trip: `id → IRI string → encode_iri(iri) → Sid`.
    /// PrefixTrie makes this O(len(iri)) per call.
    pub fn row_to_flake(
        &self,
        s_id: u64,
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
        let dt_sid = self.dicts.dt_sids.get(dt_raw as usize)
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
    pub fn query_subject_flakes(
        &self,
        g_id: u32,
        s_id: u64,
    ) -> io::Result<Vec<Flake>> {
        self.query_subject_predicate_flakes(g_id, s_id, None)
    }

    /// Query flakes for a subject (optionally filtered by predicate) in a graph.
    /// Uses the SPOT index.
    pub fn query_subject_predicate_flakes(
        &self,
        g_id: u32,
        s_id: u64,
        p_id: Option<u32>,
    ) -> io::Result<Vec<Flake>> {
        let branch = self
            .branch_for_order(g_id, RunSortOrder::Spot)
            .expect("SPOT index must exist for every graph");

        let _leaf_dir = self
            .leaf_dir_for_order(g_id, RunSortOrder::Spot)
            .expect("SPOT leaf dir must exist for every graph");

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
    // Unique tmp name: PID + nanosecond timestamp to avoid races between
    // concurrent writers (threads / processes / warm-start lambdas).
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp = cache_dir.join(format!(".cas_{}_{}.{}.tmp", std::process::id(), nanos, ext));
    std::fs::write(&tmp, bytes)?;
    if let Err(_rename_err) = std::fs::rename(&tmp, &target) {
        // Rename failed — concurrent writer may have created target (Windows),
        // or on Unix with different filesystems. Content-addressed: same hash =
        // same content, so if target now exists we can discard our tmp.
        let _ = std::fs::remove_file(&tmp);
        if !target.exists() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to cache {}.{} to {:?}", hash, ext, cache_dir),
            ));
        }
    }
    Ok(target)
}

/// Fetch artifact bytes: reads from local cache if available, otherwise
/// downloads from storage and writes to cache for next time.
async fn fetch_cached_bytes<S: StorageRead>(
    storage: &S,
    address: &str,
    cache_dir: &Path,
    ext: &str,
) -> io::Result<Vec<u8>> {
    let hash = extract_hash_from_address(address)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData,
            format!("cannot extract hash from address: {}", address)))?;
    let cached = cache_dir.join(format!("{}.{}", hash, ext));
    if cached.exists() {
        return std::fs::read(&cached);
    }
    let bytes = storage.read_bytes(address).await.map_err(storage_to_io_error)?;
    cache_bytes_to_file(cache_dir, &hash, ext, &bytes)?;
    Ok(bytes)
}

/// Build an in-memory `DictTreeReader` from forward entries (id → value).
///
/// Entries are partitioned into leaves, each leaf serialized and kept in memory.
/// Used by `load()` to convert flat dict files into tree readers.
fn build_dict_reader_forward(entries: Vec<ForwardEntry>) -> io::Result<DictTreeReader> {
    let result = builder::build_forward_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)?;
    let mut leaf_map = HashMap::new();
    for (artifact, bl) in result.leaves.iter().zip(result.branch.leaves.iter()) {
        leaf_map.insert(bl.address.clone(), artifact.bytes.clone());
    }
    Ok(DictTreeReader::from_memory(result.branch, leaf_map))
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

/// Load a `DictTreeReader` from CAS by fetching branch + leaf artifacts.
///
/// Downloads branch manifest, decodes it, validates that the branch's
/// leaf addresses match the root's leaf list (GC integrity), then eagerly
/// fetches all leaf artifacts to the local cache directory. Returns a
/// reader backed by `LeafSource::LocalFiles`.
///
/// If `leaflet_cache` is provided, the reader will use it for in-memory
/// caching of leaf blobs (keyed by `xxh3_128(cas_address)`, immutable).
async fn load_dict_tree_from_cas<S: StorageRead>(
    storage: &S,
    addrs: &super::index_root::DictTreeAddresses,
    cache_dir: &Path,
    leaf_ext: &str,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<DictTreeReader> {
    // Fetch and decode branch
    let branch_bytes = fetch_cached_bytes(storage, &addrs.branch, cache_dir, "dtb").await?;
    let branch = DictBranch::decode(&branch_bytes)?;

    // Validate branch/root leaf list consistency.
    // The root's `addrs.leaves` is the authoritative set that GC uses to
    // determine reachable artifacts. If the branch references leaves not
    // in the root's list, GC could delete them. Warn (don't fail) since
    // this is a consistency check, not a hard requirement for loading.
    if branch.leaves.len() != addrs.leaves.len() {
        tracing::warn!(
            branch_leaves = branch.leaves.len(),
            root_leaves = addrs.leaves.len(),
            "dict tree: branch leaf count does not match root leaf list"
        );
    }
    let root_leaf_set: std::collections::HashSet<&str> = addrs.leaves.iter().map(|s| s.as_str()).collect();
    for bl in &branch.leaves {
        if !root_leaf_set.contains(bl.address.as_str()) {
            tracing::warn!(
                address = %bl.address,
                "dict tree: branch references leaf not in root's leaf list (GC may delete it)"
            );
        }
    }

    // Pre-fetch all leaves to local cache, build address → path mapping.
    // Iterates branch leaves (the authoritative set for lookups).
    let mut file_map = HashMap::new();
    for leaf in &branch.leaves {
        let hash = extract_hash_from_address(&leaf.address)
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::InvalidData,
                format!("cannot extract hash from dict leaf address: {}", leaf.address),
            ))?;
        let cached_path = cache_dir.join(format!("{}.{}", hash, leaf_ext));
        if !cached_path.exists() {
            let bytes = storage.read_bytes(&leaf.address).await.map_err(storage_to_io_error)?;
            cache_bytes_to_file(cache_dir, &hash, leaf_ext, &bytes)?;
        }
        file_map.insert(leaf.address.clone(), cached_path);
    }

    let leaf_source = LeafSource::LocalFiles(file_map);
    match leaflet_cache {
        Some(cache) => Ok(DictTreeReader::with_cache(branch, leaf_source, Arc::clone(cache))),
        None => Ok(DictTreeReader::new(branch, leaf_source)),
    }
}

/// Ensure a leaf file exists in the local cache. Skips download on cache hit.
async fn ensure_leaf_cached<S: StorageRead>(
    storage: &S,
    address: &str,
    cache_dir: &Path,
) -> io::Result<()> {
    let hash = extract_hash_from_address(address)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData,
            format!("cannot extract hash from leaf address: {}", address)))?;
    let cached = cache_dir.join(format!("{}.fli", hash));
    if cached.exists() {
        return Ok(());
    }
    let bytes = storage.read_bytes(address).await.map_err(storage_to_io_error)?;
    cache_bytes_to_file(cache_dir, &hash, "fli", &bytes)?;
    Ok(())
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
