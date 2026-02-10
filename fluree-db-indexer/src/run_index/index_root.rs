//! Binary index root descriptor (v3, CID-based).
//!
//! The `BinaryIndexRoot` is the canonical metadata record for a binary
//! columnar index. It is published to the nameservice via `index_head_id`
//! and serves as the entry point for loading a `BinaryIndexStore`.
//!
//! All artifact references use `ContentId` (CIDv1) values.

use fluree_db_core::ContentId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Schema version for `BinaryIndexRoot` with CID references.
pub const BINARY_INDEX_ROOT_VERSION: u32 = 3;

// ============================================================================
// CID reference types
// ============================================================================

/// CID references for a dictionary CoW tree (branch + leaves).
///
/// Mirrors `GraphOrderRefs` — a branch manifest that references
/// a set of leaf blobs. The branch holds the key-range index; leaves
/// hold the actual dictionary entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DictTreeRefs {
    /// CID of the branch manifest (DTB1).
    pub branch: ContentId,
    /// CIDs of leaf blobs, ordered by leaf index.
    pub leaves: Vec<ContentId>,
}

/// CID references for a per-predicate vector arena (manifest + shards).
///
/// Stored explicitly so GC can reach all shard CIDs without
/// parsing manifests during retention walks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorDictRef {
    /// CID of the manifest JSON (VAM1).
    pub manifest: ContentId,
    /// CIDs of all shard blobs (VAS1), ordered by shard index.
    pub shards: Vec<ContentId>,
}

/// CID references for all dictionary artifacts.
///
/// Small-cardinality dictionaries (graphs, datatypes, languages) use a
/// single flat blob. Large dictionaries (subjects, strings) use CoW
/// trees with a branch + leaves structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DictRefs {
    pub graphs: ContentId,
    pub datatypes: ContentId,
    pub languages: ContentId,
    /// Subject forward tree: sid64 → suffix (ns-compressed, prefix stripped).
    pub subject_forward: DictTreeRefs,
    /// Subject reverse tree: [ns_code BE][suffix] → sid64 (ns-compressed).
    pub subject_reverse: DictTreeRefs,
    /// String forward tree: string_id → value.
    pub string_forward: DictTreeRefs,
    /// String reverse tree: value → string_id.
    pub string_reverse: DictTreeRefs,
    /// Per-predicate numbig arenas. Key is `p_id` as string (for JSON
    /// compatibility with integer map keys).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub numbig: BTreeMap<String, ContentId>,
    /// Per-predicate vector arena metadata. Key is `p_id` as string.
    /// Value contains manifest CID + all shard CIDs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vectors: BTreeMap<String, VectorDictRef>,
}

/// CID references for a single graph + sort order (one branch + its leaves).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphOrderRefs {
    /// CID of the branch manifest (FBR1).
    pub branch: ContentId,
    /// CIDs of leaf files (FLI1), ordered by leaf index.
    pub leaves: Vec<ContentId>,
}

/// CID references for all sort orders within a single graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphRefs {
    pub g_id: u32,
    /// Order name (e.g. `"spot"`) → branch + leaves CIDs.
    pub orders: BTreeMap<String, GraphOrderRefs>,
}

// ============================================================================
// GC chain types (prev_index / garbage)
// ============================================================================

/// Reference to the previous index root in the GC chain.
///
/// The garbage collector walks this chain backwards to determine which roots
/// (and their associated CAS artifacts) are eligible for deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryPrevIndexRef {
    /// `index_t` of the previous root.
    pub t: i64,
    /// CID of the previous root JSON blob.
    pub id: ContentId,
}

/// Reference to this root's garbage manifest.
///
/// The garbage manifest lists CIDs that were replaced when building
/// this root from the previous one. The GC collector reads this to know which
/// objects to delete when this root ages out of the retention window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryGarbageRef {
    /// CID of the garbage record JSON blob.
    pub id: ContentId,
}

// ============================================================================
// BinaryIndexRoot (v3, CID-based)
// ============================================================================

/// Graph entry: references CIDs per sort order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: u32,
    /// Per-order CID references.
    pub orders: BTreeMap<String, GraphOrderRefs>,
}

/// Configuration for building a `BinaryIndexRoot` from CAS artifacts.
///
/// Bundles all parameters needed by `BinaryIndexRoot::from_cas_artifacts`.
pub struct CasArtifactsConfig<'a> {
    /// Ledger ID (e.g. `"mydb:main"`).
    pub ledger_id: &'a str,
    /// Maximum transaction time covered by the index.
    pub index_t: i64,
    /// Base (minimum) transaction time of the snapshot.
    pub base_t: i64,
    /// Predicate SID encodings: `(ns_code, local_name)`.
    pub predicate_sids: Vec<(u16, String)>,
    /// Namespace code → full IRI prefix.
    pub namespace_codes: &'a HashMap<u16, String>,
    /// Subject ID encoding mode.
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    /// CID references for all dictionary artifacts.
    pub dict_refs: DictRefs,
    /// CID references for all graph × order combinations.
    pub graph_refs: Vec<GraphRefs>,
    /// Optional stats JSON blob.
    pub stats: Option<serde_json::Value>,
    /// Optional schema JSON blob.
    pub schema: Option<serde_json::Value>,
    /// Previous index reference for GC chain.
    pub prev_index: Option<BinaryPrevIndexRef>,
    /// Garbage manifest reference.
    pub garbage: Option<BinaryGarbageRef>,
    /// Optional CID of the HLL sketch blob.
    pub sketch_ref: Option<ContentId>,
    /// Per-graph subject ID watermarks.
    pub subject_watermarks: Vec<u64>,
    /// String dictionary watermark.
    pub string_watermark: u32,
}

/// Binary index root with CID-based artifact references.
///
/// All artifact references use [`ContentId`] instead of address strings.
/// All map types use [`BTreeMap`] for deterministic JSON serialization,
/// ensuring the canonical form is suitable for content hashing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryIndexRoot {
    /// Schema version (must equal [`BINARY_INDEX_ROOT_VERSION`]).
    pub version: u32,

    /// Ledger ID (e.g. `"mydb:main"`).
    pub ledger_id: String,

    /// Maximum transaction time covered by the index.
    pub index_t: i64,

    /// Earliest transaction time for Region 3 history.
    pub base_t: i64,

    /// Per-graph index entries with CID references.
    pub graphs: Vec<GraphEntry>,

    /// Predicate/property ID → (namespace_code, suffix) mapping.
    ///
    /// This is the minimal inline mapping required at query time to translate
    /// query predicate IRIs into numeric IDs for binary scans, without downloading
    /// a redundant predicate dictionary blob.
    ///
    /// Stored as a vector indexed by `p_id` for compactness and to enforce
    /// contiguous IDs. Each entry is serialized as a JSON array:
    /// `[ns_code, "suffix"]`.
    pub predicate_sids: Vec<(u16, String)>,

    /// Namespace code → IRI prefix mapping.
    pub namespace_codes: BTreeMap<u16, String>,

    /// Physical encoding mode for subject IDs in leaflet columns.
    ///
    /// `Narrow`: `u32 = (ns_code << 16) | local_id` — valid only when all
    /// local IDs fit in `u16`. `Wide`: full `u64`.
    /// Defaults to `Narrow` for new databases.
    #[serde(default)]
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,

    /// CID references of all dictionary artifacts.
    pub dict_refs: DictRefs,

    /// Inline index statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<serde_json::Value>,

    /// Schema hierarchy metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,

    /// Link to the previous index root (for GC chain traversal).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_index: Option<BinaryPrevIndexRef>,

    /// Link to this root's garbage manifest (CIDs replaced by this build).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub garbage: Option<BinaryGarbageRef>,

    /// CID of the HLL stats sketch blob (per-property HyperLogLog registers).
    ///
    /// When present, the referenced blob contains per-(graph, property) HLL
    /// register arrays for incremental stats refresh. Included in
    /// [`all_cas_ids()`] for GC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch_ref: Option<ContentId>,

    /// Per-namespace max assigned local_id at index build time.
    ///
    /// Index `i` = max local_id for namespace code `i`. Empty vec = all zeros
    /// (everything is novel). Used by `DictNovelty` to route forward lookups
    /// between the persisted tree and the novel overlay.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subject_watermarks: Vec<u64>,

    /// Max assigned string_id at index build time. 0 = no strings indexed.
    ///
    /// Used by `DictNovelty` to route string forward lookups.
    #[serde(default)]
    pub string_watermark: u32,

    // =========================================================================
    // Cumulative commit statistics
    // =========================================================================
    /// Total size of all commit blobs in bytes up to `index_t`.
    #[serde(default)]
    pub total_commit_size: u64,

    /// Total number of assertions across all commits up to `index_t`.
    #[serde(default)]
    pub total_asserts: u64,

    /// Total number of retractions across all commits up to `index_t`.
    #[serde(default)]
    pub total_retracts: u64,
}

impl BinaryIndexRoot {
    /// Serialize to canonical compact JSON bytes.
    ///
    /// The combination of `serde_json::to_vec` (compact) and `BTreeMap` keys
    /// produces deterministic output suitable for SHA-256 content hashing.
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes, accepting only v3.
    pub fn from_json_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        let root: Self = serde_json::from_slice(bytes)?;
        if root.version != BINARY_INDEX_ROOT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported binary index root version: {} (expected {})",
                root.version, BINARY_INDEX_ROOT_VERSION,
            )));
        }
        Ok(root)
    }

    /// Build a root from CAS upload results.
    ///
    /// `stats` and `schema` are optional JSON blobs matching the
    /// `RawDbRootStats`/`RawDbRootSchema` format from `fluree-db-core`.
    /// When present, `Db::load()` will parse them into `IndexStats`/`IndexSchema`
    /// for the query planner.
    pub fn from_cas_artifacts(cfg: CasArtifactsConfig<'_>) -> Self {
        let ns_codes: BTreeMap<u16, String> = cfg
            .namespace_codes
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();

        let graphs = cfg
            .graph_refs
            .into_iter()
            .map(|gr| GraphEntry {
                g_id: gr.g_id,
                orders: gr.orders,
            })
            .collect();

        Self {
            version: BINARY_INDEX_ROOT_VERSION,
            ledger_id: cfg.ledger_id.to_string(),
            index_t: cfg.index_t,
            base_t: cfg.base_t,
            graphs,
            predicate_sids: cfg.predicate_sids,
            namespace_codes: ns_codes,
            subject_id_encoding: cfg.subject_id_encoding,
            dict_refs: cfg.dict_refs,
            stats: cfg.stats,
            schema: cfg.schema,
            prev_index: cfg.prev_index,
            garbage: cfg.garbage,
            sketch_ref: cfg.sketch_ref,
            subject_watermarks: cfg.subject_watermarks,
            string_watermark: cfg.string_watermark,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
        }
    }

    /// Collect all CAS content-artifact CIDs referenced by this root.
    ///
    /// Includes: dict artifacts (3 flat + 4 tree branches + tree leaves +
    /// numbig), branch manifests, and leaf files for every graph × order.
    /// Does NOT include the root's own CID or the garbage manifest
    /// CID — those are managed by the GC chain (prev_index / garbage
    /// pointers).
    ///
    /// Returns a sorted, deduplicated `Vec<ContentId>`.
    pub fn all_cas_ids(&self) -> Vec<ContentId> {
        let mut ids = Vec::new();

        // Dict artifacts: 3 flat dicts + 4 trees (branch + leaves each)
        let d = &self.dict_refs;
        ids.push(d.graphs.clone());
        ids.push(d.datatypes.clone());
        ids.push(d.languages.clone());
        // Subject & string dictionary trees
        for tree in [
            &d.subject_forward,
            &d.subject_reverse,
            &d.string_forward,
            &d.string_reverse,
        ] {
            ids.push(tree.branch.clone());
            ids.extend(tree.leaves.iter().cloned());
        }

        // Per-predicate numbig arenas
        for cid in d.numbig.values() {
            ids.push(cid.clone());
        }

        // Per-predicate vector arenas (manifest + shards)
        for entry in d.vectors.values() {
            ids.push(entry.manifest.clone());
            ids.extend(entry.shards.iter().cloned());
        }

        // Per-graph, per-order branches + leaves
        for graph in &self.graphs {
            for order_refs in graph.orders.values() {
                ids.push(order_refs.branch.clone());
                for leaf in &order_refs.leaves {
                    ids.push(leaf.clone());
                }
            }
        }

        // HLL sketch blob
        if let Some(ref sketch) = self.sketch_ref {
            ids.push(sketch.clone());
        }

        ids.sort();
        ids.dedup();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    /// Shorthand for `ContentKind::DictBlob` in tests (DictKind doesn't affect codec).
    const DICT: ContentKind = ContentKind::DictBlob {
        dict: fluree_db_core::DictKind::Graphs,
    };

    /// Helper: create a test ContentId with a deterministic hash from a label.
    fn test_cid(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn sample_dict_refs() -> DictRefs {
        DictRefs {
            graphs: test_cid(DICT, "graphs"),
            datatypes: test_cid(DICT, "datatypes"),
            languages: test_cid(DICT, "languages"),
            subject_forward: DictTreeRefs {
                branch: test_cid(DICT, "sf_branch"),
                leaves: vec![test_cid(DICT, "sf_l0")],
            },
            subject_reverse: DictTreeRefs {
                branch: test_cid(DICT, "sr_branch"),
                leaves: vec![test_cid(DICT, "sr_l0")],
            },
            string_forward: DictTreeRefs {
                branch: test_cid(DICT, "stf_branch"),
                leaves: vec![test_cid(DICT, "stf_l0")],
            },
            string_reverse: DictTreeRefs {
                branch: test_cid(DICT, "str_branch"),
                leaves: vec![test_cid(DICT, "str_l0")],
            },
            numbig: BTreeMap::new(),
            vectors: BTreeMap::new(),
        }
    }

    /// Helper to build a test CasArtifactsConfig with defaults.
    fn test_config<'a>(
        ledger_id: &'a str,
        index_t: i64,
        base_t: i64,
        predicate_sids: Vec<(u16, String)>,
        namespace_codes: &'a HashMap<u16, String>,
        graph_refs: Vec<GraphRefs>,
        stats: Option<serde_json::Value>,
    ) -> CasArtifactsConfig<'a> {
        CasArtifactsConfig {
            ledger_id,
            index_t,
            base_t,
            predicate_sids,
            namespace_codes,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_refs: sample_dict_refs(),
            graph_refs,
            stats,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        }
    }

    #[test]
    fn round_trip_json() {
        let root = BinaryIndexRoot {
            version: BINARY_INDEX_ROOT_VERSION,
            ledger_id: "test:main".to_string(),
            index_t: 42,
            base_t: 1,
            graphs: vec![GraphEntry {
                g_id: 0,
                orders: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "spot".to_string(),
                        GraphOrderRefs {
                            branch: test_cid(ContentKind::IndexBranch, "spot_br"),
                            leaves: vec![test_cid(ContentKind::IndexLeaf, "spot_l0")],
                        },
                    );
                    m
                },
            }],
            predicate_sids: vec![],
            namespace_codes: {
                let mut m = BTreeMap::new();
                m.insert(0, String::new());
                m.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".into());
                m
            },
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_refs: sample_dict_refs(),
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            subject_watermarks: vec![100, 200],
            string_watermark: 50,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
        };

        let bytes = root.to_json_bytes().expect("serialize");
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).expect("deserialize");
        assert_eq!(root, parsed);
    }

    #[test]
    fn canonical_hash_is_deterministic() {
        let ns = {
            let mut m = HashMap::new();
            m.insert(0, String::new());
            m.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
            m
        };
        let graph_refs = vec![GraphRefs {
            g_id: 0,
            orders: {
                let mut m = BTreeMap::new();
                m.insert(
                    "spot".to_string(),
                    GraphOrderRefs {
                        branch: test_cid(ContentKind::IndexBranch, "b1"),
                        leaves: vec![test_cid(ContentKind::IndexLeaf, "l1")],
                    },
                );
                m
            },
        }];

        let predicate_sids: Vec<(u16, String)> = vec![(0, "p0".to_string())];

        let root1 = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            42,
            1,
            predicate_sids.clone(),
            &ns,
            graph_refs.clone(),
            None,
        ));
        let root2 = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            42,
            1,
            predicate_sids,
            &ns,
            graph_refs,
            None,
        ));

        let bytes1 = root1.to_json_bytes().unwrap();
        let bytes2 = root2.to_json_bytes().unwrap();
        assert_eq!(bytes1, bytes2, "canonical JSON must be deterministic");

        let hash1 = fluree_db_core::sha256_hex(&bytes1);
        let hash2 = fluree_db_core::sha256_hex(&bytes2);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn stats_round_trip() {
        let stats = serde_json::json!({
            "flakes": 12345,
            "size": 0,
            "graphs": [{"g_id": 1, "flakes": 10000, "size": 0}]
        });
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            42,
            1,
            vec![],
            &ns,
            vec![],
            Some(stats.clone()),
        ));

        // Round-trip through JSON
        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.stats, Some(stats));
        assert_eq!(parsed.schema, None);
    }

    #[test]
    fn stats_parseable_as_raw_db_root_stats() {
        // Verify the stats JSON we produce is compatible with RawDbRootStats
        let stats = serde_json::json!({
            "flakes": 12345,
            "size": 0,
            "graphs": [{"g_id": 1, "flakes": 10000, "size": 0}]
        });
        let raw: fluree_db_core::serde::json::RawDbRootStats =
            serde_json::from_value(stats).unwrap();
        assert_eq!(raw.flakes, Some(12345));
        assert_eq!(raw.graphs.as_ref().unwrap().len(), 1);
        assert_eq!(raw.graphs.as_ref().unwrap()[0].g_id, 1);
        assert_eq!(raw.graphs.as_ref().unwrap()[0].flakes, 10000);
    }

    #[test]
    fn all_cas_ids_collects_all_artifacts() {
        let mut numbig = BTreeMap::new();
        numbig.insert("5".to_string(), test_cid(DICT, "numbig_5"));
        numbig.insert("12".to_string(), test_cid(DICT, "numbig_12"));

        let dicts = DictRefs {
            graphs: test_cid(DICT, "graphs"),
            datatypes: test_cid(DICT, "dt"),
            languages: test_cid(DICT, "lang"),
            subject_forward: DictTreeRefs {
                branch: test_cid(DICT, "sf_br"),
                leaves: vec![test_cid(DICT, "sf_l0")],
            },
            subject_reverse: DictTreeRefs {
                branch: test_cid(DICT, "sr_br"),
                leaves: vec![test_cid(DICT, "sr_l0")],
            },
            string_forward: DictTreeRefs {
                branch: test_cid(DICT, "stf_br"),
                leaves: vec![test_cid(DICT, "stf_l0")],
            },
            string_reverse: DictTreeRefs {
                branch: test_cid(DICT, "str_br"),
                leaves: vec![test_cid(DICT, "str_l0")],
            },
            numbig,
            vectors: BTreeMap::new(),
        };

        let graph_refs = vec![GraphRefs {
            g_id: 0,
            orders: {
                let mut m = BTreeMap::new();
                m.insert(
                    "spot".into(),
                    GraphOrderRefs {
                        branch: test_cid(ContentKind::IndexBranch, "g0_spot_br"),
                        leaves: vec![
                            test_cid(ContentKind::IndexLeaf, "g0_spot_l0"),
                            test_cid(ContentKind::IndexLeaf, "g0_spot_l1"),
                        ],
                    },
                );
                m.insert(
                    "psot".into(),
                    GraphOrderRefs {
                        branch: test_cid(ContentKind::IndexBranch, "g0_psot_br"),
                        leaves: vec![test_cid(ContentKind::IndexLeaf, "g0_psot_l0")],
                    },
                );
                m
            },
        }];

        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(CasArtifactsConfig {
            ledger_id: "test:main",
            index_t: 10,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_refs: dicts,
            graph_refs,
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        });

        let ids = root.all_cas_ids();

        // 3 flat dicts + 4 tree branches + 4 tree leaves + 2 numbig
        //   + 2 graph branches + 3 graph leaves = 18
        assert_eq!(ids.len(), 18);

        // Verify sorted
        for w in ids.windows(2) {
            assert!(w[0] <= w[1], "not sorted: {} > {}", w[0], w[1]);
        }

        // Spot-check specific CIDs
        assert!(ids.contains(&test_cid(DICT, "numbig_5")));
        assert!(ids.contains(&test_cid(DICT, "sf_br")));
        assert!(ids.contains(&test_cid(DICT, "sf_l0")));
        assert!(ids.contains(&test_cid(ContentKind::IndexBranch, "g0_spot_br")));
        assert!(ids.contains(&test_cid(ContentKind::IndexLeaf, "g0_spot_l1")));
        assert!(ids.contains(&test_cid(ContentKind::IndexLeaf, "g0_psot_l0")));
    }

    #[test]
    fn round_trip_with_gc_fields() {
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(CasArtifactsConfig {
            ledger_id: "test:main",
            index_t: 42,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_refs: sample_dict_refs(),
            graph_refs: vec![],
            stats: None,
            schema: None,
            prev_index: Some(BinaryPrevIndexRef {
                t: 40,
                id: test_cid(ContentKind::IndexRoot, "prev_root"),
            }),
            garbage: Some(BinaryGarbageRef {
                id: test_cid(ContentKind::GarbageRecord, "garbage_record"),
            }),
            sketch_ref: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        });

        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.prev_index.as_ref().unwrap().t, 40);
        assert_eq!(
            parsed.prev_index.as_ref().unwrap().id,
            test_cid(ContentKind::IndexRoot, "prev_root")
        );
        assert_eq!(
            parsed.garbage.as_ref().unwrap().id,
            test_cid(ContentKind::GarbageRecord, "garbage_record")
        );
    }

    #[test]
    fn round_trip_without_gc_fields() {
        // When prev_index and garbage are None, they should not appear in JSON
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            42,
            1,
            vec![],
            &ns,
            vec![],
            None,
        ));

        let bytes = root.to_json_bytes().unwrap();
        let json_str = std::str::from_utf8(&bytes).unwrap();
        assert!(
            !json_str.contains("prev_index"),
            "None prev_index should be skipped"
        );
        assert!(
            !json_str.contains("garbage"),
            "None garbage should be skipped"
        );

        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.prev_index, None);
        assert_eq!(parsed.garbage, None);
    }

    #[test]
    fn watermarks_round_trip() {
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(CasArtifactsConfig {
            ledger_id: "test:main",
            index_t: 42,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_refs: sample_dict_refs(),
            graph_refs: vec![],
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            subject_watermarks: vec![100, 200, 300],
            string_watermark: 500,
        });

        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();

        assert_eq!(parsed.subject_watermarks, vec![100, 200, 300]);
        assert_eq!(parsed.string_watermark, 500);
    }

    #[test]
    fn watermarks_forwards_safe_decoding() {
        // Old roots without watermark fields should deserialize with defaults
        // (empty vec / 0) — equivalent to "everything is novel", safe and conservative.
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            42,
            1,
            vec![],
            &ns,
            vec![],
            None,
        ));

        let bytes = root.to_json_bytes().unwrap();
        let json_str = std::str::from_utf8(&bytes).unwrap();

        // Empty subject_watermarks should be skipped in JSON
        assert!(
            !json_str.contains("subject_watermarks"),
            "empty subject_watermarks should be skipped"
        );
        // string_watermark 0 still appears (no skip_serializing_if for it)

        // Re-parse: defaults kick in for missing fields
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();
        assert!(parsed.subject_watermarks.is_empty());
        assert_eq!(parsed.string_watermark, 0);

        // Simulate an old root that truly lacks watermark fields by
        // stripping them from the JSON manually.
        let mut json_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json_val.as_object_mut().unwrap().remove("string_watermark");
        let stripped_bytes = serde_json::to_vec(&json_val).unwrap();
        let parsed2 = BinaryIndexRoot::from_json_bytes(&stripped_bytes).unwrap();
        assert!(parsed2.subject_watermarks.is_empty());
        assert_eq!(parsed2.string_watermark, 0);
    }

    #[test]
    fn all_cas_ids_includes_sketch_ref() {
        let ns = HashMap::new();
        let sketch_cid = test_cid(ContentKind::StatsSketch, "sketch_blob");
        let mut config = test_config("test:main", 10, 1, vec![], &ns, vec![], None);
        config.sketch_ref = Some(sketch_cid.clone());

        let root = BinaryIndexRoot::from_cas_artifacts(config);
        let ids = root.all_cas_ids();

        assert!(
            ids.contains(&sketch_cid),
            "sketch_ref CID missing from all_cas_ids"
        );
    }

    #[test]
    fn sketch_ref_round_trip() {
        let ns = HashMap::new();
        let sketch_cid = test_cid(ContentKind::StatsSketch, "sketch_blob");
        let mut config = test_config("test:main", 10, 1, vec![], &ns, vec![], None);
        config.sketch_ref = Some(sketch_cid.clone());

        let root = BinaryIndexRoot::from_cas_artifacts(config);
        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).unwrap();

        assert_eq!(parsed.sketch_ref, Some(sketch_cid));
    }

    #[test]
    fn sketch_ref_none_omitted_from_json() {
        let ns = HashMap::new();
        let root = BinaryIndexRoot::from_cas_artifacts(test_config(
            "test:main",
            10,
            1,
            vec![],
            &ns,
            vec![],
            None,
        ));

        let bytes = root.to_json_bytes().unwrap();
        let json_str = std::str::from_utf8(&bytes).unwrap();
        assert!(
            !json_str.contains("sketch_ref"),
            "sketch_ref: None should be omitted from JSON"
        );
    }
}
