//! Binary index root descriptor (v2, CAS-based).
//!
//! The `BinaryIndexRootV2` is the canonical metadata record for a binary
//! columnar index. It is published to the nameservice via `index_address`
//! and serves as the entry point for loading a `BinaryIndexStore`.
//!
//! All artifact references use content-addressed storage (CAS) addresses.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Schema version for `BinaryIndexRootV2` with CAS addresses.
pub const BINARY_INDEX_ROOT_VERSION_V2: u32 = 2;

// ============================================================================
// CAS address types (CAS-2 / CAS-3)
// ============================================================================

/// CAS addresses for a dictionary CoW tree (branch + leaves).
///
/// Mirrors `GraphOrderAddresses` — a branch manifest that references
/// a set of leaf blobs. The branch holds the key-range index; leaves
/// hold the actual dictionary entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DictTreeAddresses {
    /// CAS address of the branch manifest (DTB1).
    pub branch: String,
    /// CAS addresses of leaf blobs, ordered by leaf index.
    pub leaves: Vec<String>,
}

/// CAS addresses for all dictionary artifacts.
///
/// Small-cardinality dictionaries (graphs, datatypes, languages) use a
/// single flat blob. Large dictionaries (subjects, strings) use CoW
/// trees with a branch + leaves structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DictAddresses {
    pub graphs: String,
    pub datatypes: String,
    pub languages: String,
    /// Subject forward tree: sid64 → suffix (ns-compressed, prefix stripped).
    pub subject_forward: DictTreeAddresses,
    /// Subject reverse tree: [ns_code BE][suffix] → sid64 (ns-compressed).
    pub subject_reverse: DictTreeAddresses,
    /// String forward tree: string_id → value.
    pub string_forward: DictTreeAddresses,
    /// String reverse tree: value → string_id.
    pub string_reverse: DictTreeAddresses,
    /// Per-predicate numbig arenas. Key is `p_id` as string (for JSON
    /// compatibility with integer map keys).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub numbig: BTreeMap<String, String>,
}

/// CAS addresses for a single graph + sort order (one branch + its leaves).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphOrderAddresses {
    /// CAS address of the branch manifest (FBR1).
    pub branch: String,
    /// CAS addresses of leaf files (FLI1), ordered by leaf index.
    pub leaves: Vec<String>,
}

/// CAS addresses for all sort orders within a single graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphAddresses {
    pub g_id: u32,
    /// Order name (e.g. `"spot"`) → branch + leaves addresses.
    pub orders: BTreeMap<String, GraphOrderAddresses>,
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
    /// CAS address of the previous root JSON blob.
    pub address: String,
}

/// Reference to this root's garbage manifest.
///
/// The garbage manifest lists CAS addresses that were replaced when building
/// this root from the previous one. The GC collector reads this to know which
/// objects to delete when this root ages out of the retention window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryGarbageRef {
    /// CAS address of the garbage record JSON blob.
    pub address: String,
}

// ============================================================================
// BinaryIndexRootV2 (CAS-4)
// ============================================================================

/// V2 graph entry: references CAS addresses per sort order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEntryV2 {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: u32,
    /// Per-order CAS addresses.
    pub orders: BTreeMap<String, GraphOrderAddresses>,
}

/// Configuration for building a `BinaryIndexRootV2` from CAS artifacts.
///
/// Bundles all parameters needed by `BinaryIndexRootV2::from_cas_artifacts`.
pub struct CasArtifactsConfig<'a> {
    /// Ledger alias (e.g. `"mydb/main"`).
    pub ledger_alias: &'a str,
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
    /// CAS addresses for all dictionary artifacts.
    pub dict_addresses: DictAddresses,
    /// CAS addresses for all graph × order combinations.
    pub graph_addresses: Vec<GraphAddresses>,
    /// Optional stats JSON blob.
    pub stats: Option<serde_json::Value>,
    /// Optional schema JSON blob.
    pub schema: Option<serde_json::Value>,
    /// Previous index reference for GC chain.
    pub prev_index: Option<BinaryPrevIndexRef>,
    /// Garbage manifest reference.
    pub garbage: Option<BinaryGarbageRef>,
    /// Per-graph subject ID watermarks.
    pub subject_watermarks: Vec<u64>,
    /// String dictionary watermark.
    pub string_watermark: u32,
}

/// V2 binary index root with full CAS addresses.
///
/// Replaces v1's `runs_addr_prefix` / `index_addr_prefix` with explicit
/// per-artifact CAS addresses in `dict_addresses` and per-graph `orders`.
/// All map types use [`BTreeMap`] for deterministic JSON serialization,
/// ensuring the canonical form is suitable for content hashing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryIndexRootV2 {
    /// Schema version (must equal [`BINARY_INDEX_ROOT_VERSION_V2`]).
    pub version: u32,

    /// Ledger alias (e.g. `"mydb/main"`).
    pub ledger_alias: String,

    /// Maximum transaction time covered by the index.
    pub index_t: i64,

    /// Earliest transaction time for Region 3 history.
    pub base_t: i64,

    /// Per-graph index entries with CAS addresses.
    pub graphs: Vec<GraphEntryV2>,

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

    /// CAS addresses of all dictionary artifacts.
    pub dict_addresses: DictAddresses,

    /// Inline index statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<serde_json::Value>,

    /// Schema hierarchy metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,

    /// Link to the previous index root (for GC chain traversal).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_index: Option<BinaryPrevIndexRef>,

    /// Link to this root's garbage manifest (CAS addresses replaced by this build).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub garbage: Option<BinaryGarbageRef>,

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

impl BinaryIndexRootV2 {
    /// Serialize to canonical compact JSON bytes.
    ///
    /// The combination of `serde_json::to_vec` (compact) and `BTreeMap` keys
    /// produces deterministic output suitable for SHA-256 content hashing.
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes, accepting only v2.
    pub fn from_json_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        let root: Self = serde_json::from_slice(bytes)?;
        if root.version != BINARY_INDEX_ROOT_VERSION_V2 {
            return Err(serde::de::Error::custom(format!(
                "unsupported binary index root version: {} (expected {})",
                root.version, BINARY_INDEX_ROOT_VERSION_V2,
            )));
        }
        Ok(root)
    }

    /// Build a v2 root from CAS upload results.
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
            .graph_addresses
            .into_iter()
            .map(|ga| GraphEntryV2 {
                g_id: ga.g_id,
                orders: ga.orders,
            })
            .collect();

        Self {
            version: BINARY_INDEX_ROOT_VERSION_V2,
            ledger_alias: cfg.ledger_alias.to_string(),
            index_t: cfg.index_t,
            base_t: cfg.base_t,
            graphs,
            predicate_sids: cfg.predicate_sids,
            namespace_codes: ns_codes,
            subject_id_encoding: cfg.subject_id_encoding,
            dict_addresses: cfg.dict_addresses,
            stats: cfg.stats,
            schema: cfg.schema,
            prev_index: cfg.prev_index,
            garbage: cfg.garbage,
            subject_watermarks: cfg.subject_watermarks,
            string_watermark: cfg.string_watermark,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
        }
    }

    /// Collect all CAS content-artifact addresses referenced by this root.
    ///
    /// Includes: dict artifacts (3 flat + 4 tree branches + tree leaves +
    /// numbig), branch manifests, and leaf files for every graph × order.
    /// Does NOT include the root's own address or the garbage manifest
    /// address — those are managed by the GC chain (prev_index / garbage
    /// pointers).
    ///
    /// Returns a sorted, deduplicated `Vec<String>`.
    pub fn all_cas_addresses(&self) -> Vec<String> {
        let mut addrs = Vec::new();

        // Dict artifacts: 3 flat dicts + 4 trees (branch + leaves each)
        let d = &self.dict_addresses;
        addrs.push(d.graphs.clone());
        addrs.push(d.datatypes.clone());
        addrs.push(d.languages.clone());
        // Subject & string dictionary trees
        for tree in [
            &d.subject_forward,
            &d.subject_reverse,
            &d.string_forward,
            &d.string_reverse,
        ] {
            addrs.push(tree.branch.clone());
            addrs.extend(tree.leaves.iter().cloned());
        }

        // Per-predicate numbig arenas
        for addr in d.numbig.values() {
            addrs.push(addr.clone());
        }

        // Per-graph, per-order branches + leaves
        for graph in &self.graphs {
            for order_addrs in graph.orders.values() {
                addrs.push(order_addrs.branch.clone());
                for leaf in &order_addrs.leaves {
                    addrs.push(leaf.clone());
                }
            }
        }

        addrs.sort();
        addrs.dedup();
        addrs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_dict_addresses() -> DictAddresses {
        DictAddresses {
            graphs: "fluree:file://t/main/objects/dicts/g.dict".into(),
            datatypes: "fluree:file://t/main/objects/dicts/d.dict".into(),
            languages: "fluree:file://t/main/objects/dicts/l.dict".into(),
            subject_forward: DictTreeAddresses {
                branch: "fluree:file://t/main/objects/dicts/sf.br".into(),
                leaves: vec!["fluree:file://t/main/objects/dicts/sf_l0.leaf".into()],
            },
            subject_reverse: DictTreeAddresses {
                branch: "fluree:file://t/main/objects/dicts/sr.br".into(),
                leaves: vec!["fluree:file://t/main/objects/dicts/sr_l0.leaf".into()],
            },
            string_forward: DictTreeAddresses {
                branch: "fluree:file://t/main/objects/dicts/stf.br".into(),
                leaves: vec!["fluree:file://t/main/objects/dicts/stf_l0.leaf".into()],
            },
            string_reverse: DictTreeAddresses {
                branch: "fluree:file://t/main/objects/dicts/str.br".into(),
                leaves: vec!["fluree:file://t/main/objects/dicts/str_l0.leaf".into()],
            },
            numbig: BTreeMap::new(),
        }
    }

    /// Helper to build a test CasArtifactsConfig with defaults.
    fn test_config<'a>(
        ledger_alias: &'a str,
        index_t: i64,
        base_t: i64,
        predicate_sids: Vec<(u16, String)>,
        namespace_codes: &'a HashMap<u16, String>,
        graph_addresses: Vec<GraphAddresses>,
        stats: Option<serde_json::Value>,
    ) -> CasArtifactsConfig<'a> {
        CasArtifactsConfig {
            ledger_alias,
            index_t,
            base_t,
            predicate_sids,
            namespace_codes,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_addresses: sample_dict_addresses(),
            graph_addresses,
            stats,
            schema: None,
            prev_index: None,
            garbage: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        }
    }

    #[test]
    fn round_trip_v2_json() {
        let root = BinaryIndexRootV2 {
            version: BINARY_INDEX_ROOT_VERSION_V2,
            ledger_alias: "test/main".to_string(),
            index_t: 42,
            base_t: 1,
            graphs: vec![GraphEntryV2 {
                g_id: 0,
                orders: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "spot".to_string(),
                        GraphOrderAddresses {
                            branch: "fluree:file://t/main/objects/branches/abc.fbr".into(),
                            leaves: vec!["fluree:file://t/main/objects/leaves/def.fli".into()],
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
            dict_addresses: sample_dict_addresses(),
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            subject_watermarks: vec![100, 200],
            string_watermark: 50,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
        };

        let bytes = root.to_json_bytes().expect("serialize");
        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).expect("deserialize");
        assert_eq!(root, parsed);
    }

    #[test]
    fn v2_canonical_hash_is_deterministic() {
        let ns = {
            let mut m = HashMap::new();
            m.insert(0, String::new());
            m.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
            m
        };
        let graph_addrs = vec![GraphAddresses {
            g_id: 0,
            orders: {
                let mut m = BTreeMap::new();
                m.insert(
                    "spot".to_string(),
                    GraphOrderAddresses {
                        branch: "fluree:file://b1.fbr".into(),
                        leaves: vec!["fluree:file://l1.fli".into()],
                    },
                );
                m
            },
        }];

        let predicate_sids: Vec<(u16, String)> = vec![(0, "p0".to_string())];

        let root1 = BinaryIndexRootV2::from_cas_artifacts(test_config(
            "test/main",
            42,
            1,
            predicate_sids.clone(),
            &ns,
            graph_addrs.clone(),
            None,
        ));
        let root2 = BinaryIndexRootV2::from_cas_artifacts(test_config(
            "test/main",
            42,
            1,
            predicate_sids,
            &ns,
            graph_addrs,
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
    fn v2_stats_round_trip() {
        let stats = serde_json::json!({
            "flakes": 12345,
            "size": 0,
            "graphs": [{"g_id": 1, "flakes": 10000, "size": 0}]
        });
        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(test_config(
            "test/main",
            42,
            1,
            vec![],
            &ns,
            vec![],
            Some(stats.clone()),
        ));

        // Round-trip through JSON
        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.stats, Some(stats));
        assert_eq!(parsed.schema, None);
    }

    #[test]
    fn v2_stats_parseable_as_raw_db_root_stats() {
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
    fn all_cas_addresses_collects_all_artifacts() {
        let mut numbig = BTreeMap::new();
        numbig.insert("5".to_string(), "cas://numbig_5".into());
        numbig.insert("12".to_string(), "cas://numbig_12".into());

        let dicts = DictAddresses {
            graphs: "cas://graphs".into(),
            datatypes: "cas://dt".into(),
            languages: "cas://lang".into(),
            subject_forward: DictTreeAddresses {
                branch: "cas://sf_br".into(),
                leaves: vec!["cas://sf_l0".into()],
            },
            subject_reverse: DictTreeAddresses {
                branch: "cas://sr_br".into(),
                leaves: vec!["cas://sr_l0".into()],
            },
            string_forward: DictTreeAddresses {
                branch: "cas://stf_br".into(),
                leaves: vec!["cas://stf_l0".into()],
            },
            string_reverse: DictTreeAddresses {
                branch: "cas://str_br".into(),
                leaves: vec!["cas://str_l0".into()],
            },
            numbig,
        };

        let graph_addrs = vec![GraphAddresses {
            g_id: 0,
            orders: {
                let mut m = BTreeMap::new();
                m.insert(
                    "spot".into(),
                    GraphOrderAddresses {
                        branch: "cas://g0_spot_br".into(),
                        leaves: vec!["cas://g0_spot_l0".into(), "cas://g0_spot_l1".into()],
                    },
                );
                m.insert(
                    "psot".into(),
                    GraphOrderAddresses {
                        branch: "cas://g0_psot_br".into(),
                        leaves: vec!["cas://g0_psot_l0".into()],
                    },
                );
                m
            },
        }];

        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(CasArtifactsConfig {
            ledger_alias: "test/main",
            index_t: 10,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_addresses: dicts,
            graph_addresses: graph_addrs,
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        });

        let addrs = root.all_cas_addresses();

        // 3 flat dicts + 4 tree branches + 4 tree leaves + 2 numbig
        //   + 2 graph branches + 3 graph leaves = 18
        assert_eq!(addrs.len(), 18);

        // Verify sorted
        for w in addrs.windows(2) {
            assert!(w[0] <= w[1], "not sorted: {} > {}", w[0], w[1]);
        }

        // Spot-check specific addresses
        assert!(addrs.contains(&"cas://numbig_5".to_string()));
        assert!(addrs.contains(&"cas://sf_br".to_string()));
        assert!(addrs.contains(&"cas://sf_l0".to_string()));
        assert!(addrs.contains(&"cas://g0_spot_br".to_string()));
        assert!(addrs.contains(&"cas://g0_spot_l1".to_string()));
        assert!(addrs.contains(&"cas://g0_psot_l0".to_string()));
    }

    #[test]
    fn v2_round_trip_with_gc_fields() {
        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(CasArtifactsConfig {
            ledger_alias: "test/main",
            index_t: 42,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_addresses: sample_dict_addresses(),
            graph_addresses: vec![],
            stats: None,
            schema: None,
            prev_index: Some(BinaryPrevIndexRef {
                t: 40,
                address: "cas://prev_root".into(),
            }),
            garbage: Some(BinaryGarbageRef {
                address: "cas://garbage_record".into(),
            }),
            subject_watermarks: vec![],
            string_watermark: 0,
        });

        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.prev_index.as_ref().unwrap().t, 40);
        assert_eq!(
            parsed.prev_index.as_ref().unwrap().address,
            "cas://prev_root"
        );
        assert_eq!(
            parsed.garbage.as_ref().unwrap().address,
            "cas://garbage_record"
        );
    }

    #[test]
    fn v2_round_trip_without_gc_fields() {
        // When prev_index and garbage are None, they should not appear in JSON
        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(test_config(
            "test/main",
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

        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.prev_index, None);
        assert_eq!(parsed.garbage, None);
    }

    #[test]
    fn watermarks_round_trip() {
        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(CasArtifactsConfig {
            ledger_alias: "test/main",
            index_t: 42,
            base_t: 1,
            predicate_sids: vec![],
            namespace_codes: &ns,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            dict_addresses: sample_dict_addresses(),
            graph_addresses: vec![],
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            subject_watermarks: vec![100, 200, 300],
            string_watermark: 500,
        });

        let bytes = root.to_json_bytes().unwrap();
        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).unwrap();

        assert_eq!(parsed.subject_watermarks, vec![100, 200, 300]);
        assert_eq!(parsed.string_watermark, 500);
    }

    #[test]
    fn watermarks_forwards_safe_decoding() {
        // Old roots without watermark fields should deserialize with defaults
        // (empty vec / 0) — equivalent to "everything is novel", safe and conservative.
        let ns = HashMap::new();
        let root = BinaryIndexRootV2::from_cas_artifacts(test_config(
            "test/main",
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
        let parsed = BinaryIndexRootV2::from_json_bytes(&bytes).unwrap();
        assert!(parsed.subject_watermarks.is_empty());
        assert_eq!(parsed.string_watermark, 0);

        // Simulate an old root that truly lacks watermark fields by
        // stripping them from the JSON manually.
        let mut json_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json_val.as_object_mut().unwrap().remove("string_watermark");
        let stripped_bytes = serde_json::to_vec(&json_val).unwrap();
        let parsed2 = BinaryIndexRootV2::from_json_bytes(&stripped_bytes).unwrap();
        assert!(parsed2.subject_watermarks.is_empty());
        assert_eq!(parsed2.string_watermark, 0);
    }
}
