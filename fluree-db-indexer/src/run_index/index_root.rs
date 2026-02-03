//! Versioned binary index root descriptor.
//!
//! The `BinaryIndexRoot` is the canonical metadata record for a binary columnar
//! index. It is published to the nameservice via `index_address` and serves as
//! the entry point for loading a `BinaryIndexStore`.
//!
//! ## Versioning
//!
//! The JSON representation always includes `"version": <u32>`. Consumers must
//! check the version and reject unknown versions rather than silently ignoring
//! new fields.
//!
//! ## Storage address prefixes
//!
//! `runs_addr_prefix` and `index_addr_prefix` are storage-scheme-qualified
//! prefixes (e.g. `"file:///data/ledger/runs"` or `"s3://bucket/ledger/index"`).
//! Consumers resolve these to concrete paths via their storage backend.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Current schema version for `BinaryIndexRoot` (v1).
pub const BINARY_INDEX_ROOT_VERSION: u32 = 1;

/// Schema version for `BinaryIndexRootV2` with CAS addresses.
pub const BINARY_INDEX_ROOT_VERSION_V2: u32 = 2;

/// Metadata for a single graph within the index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEntry {
    /// Graph dictionary ID (0 = default graph).
    pub g_id: u32,
    /// Relative directory within the index tree (e.g. `"graph_0/spot"`).
    pub directory: String,
}

/// Top-level binary index root descriptor.
///
/// Published to the nameservice as the canonical `index_address`. Contains all
/// metadata needed to locate and load a `BinaryIndexStore`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryIndexRoot {
    /// Schema version (must equal [`BINARY_INDEX_ROOT_VERSION`]).
    pub version: u32,

    /// Ledger alias (e.g. `"mydb/main"`).
    pub ledger_alias: String,

    /// Maximum transaction time covered by the index.
    pub index_t: i64,

    /// Earliest transaction time for which Region 3 history is available.
    /// Time-travel queries are valid for `t_target >= base_t`.
    pub base_t: i64,

    /// Graphs present in the index.
    pub graphs: Vec<GraphEntry>,

    /// Sort orders built (e.g. `["spot", "psot", "post", "opst"]`).
    pub orders: Vec<String>,

    /// Namespace code → IRI prefix mapping.
    pub namespace_codes: HashMap<i32, String>,

    /// Inline index statistics (class/property counts, HLL estimates).
    /// `None` when stats have not been collected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<serde_json::Value>,

    /// Schema hierarchy metadata (class/property relationships).
    /// `None` when schema has not been collected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,

    /// Storage address prefix for run-directory artifacts (dictionaries,
    /// forward/reverse files). Example: `"file:///data/mydb/runs"`.
    pub runs_addr_prefix: String,

    /// Storage address prefix for index-directory artifacts (manifests,
    /// leaf files, branch files). Example: `"file:///data/mydb/index"`.
    pub index_addr_prefix: String,
}

impl BinaryIndexRoot {
    /// Serialize to pretty-printed JSON bytes.
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        let root: Self = serde_json::from_slice(bytes)?;
        // Version check: reject unknown versions early.
        if root.version != BINARY_INDEX_ROOT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported binary index root version: {} (expected {})",
                root.version, BINARY_INDEX_ROOT_VERSION,
            )));
        }
        Ok(root)
    }

    /// Build a `BinaryIndexRoot` from an existing `BinaryIndexStore` and
    /// contextual metadata.
    ///
    /// This is the canonical way to produce a root descriptor after building
    /// or refreshing an index.
    pub fn from_store(
        store: &super::spot_store::BinaryIndexStore,
        ledger_alias: &str,
        runs_addr_prefix: &str,
        index_addr_prefix: &str,
    ) -> Self {
        let graph_ids = store.graph_ids();
        let graphs = graph_ids
            .iter()
            .map(|&g_id| GraphEntry {
                g_id,
                directory: format!("graph_{}", g_id),
            })
            .collect();

        // Derive orders from what's actually loaded in the store
        // (e.g. OPST may be absent if no IRI references exist)
        let mut orders = std::collections::BTreeSet::new();
        for &g_id in &graph_ids {
            for order in store.available_orders(g_id) {
                orders.insert(order.dir_name().to_string());
            }
        }

        Self {
            version: BINARY_INDEX_ROOT_VERSION,
            ledger_alias: ledger_alias.to_string(),
            index_t: store.max_t(),
            base_t: store.base_t(),
            graphs,
            orders: orders.into_iter().collect(),
            namespace_codes: store.namespace_codes().clone(),
            stats: None,
            schema: None,
            runs_addr_prefix: runs_addr_prefix.to_string(),
            index_addr_prefix: index_addr_prefix.to_string(),
        }
    }
}

// ============================================================================
// CAS address types (CAS-2 / CAS-3)
// ============================================================================

/// CAS addresses for all dictionary artifacts.
///
/// Each field holds the full CAS address string returned by
/// `content_write_bytes` (e.g. `"fluree:file://mydb/main/objects/dicts/abc123.dict"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DictAddresses {
    pub predicates: String,
    pub graphs: String,
    pub datatypes: String,
    pub languages: String,
    pub subject_forward: String,
    pub subject_index: String,
    pub subject_reverse: String,
    pub string_forward: String,
    pub string_index: String,
    pub string_reverse: String,
    pub namespaces: String,
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

    /// Namespace code → IRI prefix mapping.
    pub namespace_codes: BTreeMap<i32, String>,

    /// CAS addresses of all dictionary artifacts.
    pub dict_addresses: DictAddresses,

    /// Inline index statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<serde_json::Value>,

    /// Schema hierarchy metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
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
    pub fn from_cas_artifacts(
        ledger_alias: &str,
        index_t: i64,
        base_t: i64,
        namespace_codes: &HashMap<i32, String>,
        dict_addresses: DictAddresses,
        graph_addresses: Vec<GraphAddresses>,
        stats: Option<serde_json::Value>,
        schema: Option<serde_json::Value>,
    ) -> Self {
        let ns_codes: BTreeMap<i32, String> = namespace_codes
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();

        let graphs = graph_addresses
            .into_iter()
            .map(|ga| GraphEntryV2 {
                g_id: ga.g_id,
                orders: ga.orders,
            })
            .collect();

        Self {
            version: BINARY_INDEX_ROOT_VERSION_V2,
            ledger_alias: ledger_alias.to_string(),
            index_t,
            base_t,
            graphs,
            namespace_codes: ns_codes,
            dict_addresses,
            stats,
            schema,
        }
    }
}

// ============================================================================
// Version dispatch
// ============================================================================

/// Either a v1 or v2 binary index root.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryIndexRootAny {
    V1(BinaryIndexRoot),
    V2(BinaryIndexRootV2),
}

/// Parse a binary index root from JSON, dispatching on the `version` field.
pub fn parse_index_root(bytes: &[u8]) -> serde_json::Result<BinaryIndexRootAny> {
    // Peek at the version field without fully parsing.
    let peek: serde_json::Value = serde_json::from_slice(bytes)?;
    let version = peek
        .get("version")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;

    match version {
        1 => Ok(BinaryIndexRootAny::V1(BinaryIndexRoot::from_json_bytes(bytes)?)),
        2 => Ok(BinaryIndexRootAny::V2(BinaryIndexRootV2::from_json_bytes(bytes)?)),
        v => Err(serde::de::Error::custom(format!(
            "unsupported binary index root version: {}",
            v
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_json() {
        let root = BinaryIndexRoot {
            version: BINARY_INDEX_ROOT_VERSION,
            ledger_alias: "test/main".to_string(),
            index_t: 42,
            base_t: 1,
            graphs: vec![
                GraphEntry {
                    g_id: 0,
                    directory: "graph_0".to_string(),
                },
            ],
            orders: vec!["spot".to_string(), "psot".to_string()],
            namespace_codes: {
                let mut m = HashMap::new();
                m.insert(0, String::new());
                m.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
                m
            },
            stats: None,
            schema: None,
            runs_addr_prefix: "file:///tmp/test/runs".to_string(),
            index_addr_prefix: "file:///tmp/test/index".to_string(),
        };

        let bytes = root.to_json_bytes().expect("serialize");
        let parsed = BinaryIndexRoot::from_json_bytes(&bytes).expect("deserialize");
        assert_eq!(root, parsed);
    }

    #[test]
    fn rejects_unknown_version() {
        let json = r#"{"version": 99, "ledger_alias": "x", "index_t": 0, "base_t": 0, "graphs": [], "orders": [], "namespace_codes": {}, "runs_addr_prefix": "", "index_addr_prefix": ""}"#;
        let result = BinaryIndexRoot::from_json_bytes(json.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported binary index root version"), "error: {}", err);
    }

    fn sample_dict_addresses() -> DictAddresses {
        DictAddresses {
            predicates: "fluree:file://t/main/objects/dicts/p.dict".into(),
            graphs: "fluree:file://t/main/objects/dicts/g.dict".into(),
            datatypes: "fluree:file://t/main/objects/dicts/d.dict".into(),
            languages: "fluree:file://t/main/objects/dicts/l.dict".into(),
            subject_forward: "fluree:file://t/main/objects/dicts/sf.fwd".into(),
            subject_index: "fluree:file://t/main/objects/dicts/si.idx".into(),
            subject_reverse: "fluree:file://t/main/objects/dicts/sr.rev".into(),
            string_forward: "fluree:file://t/main/objects/dicts/stf.fwd".into(),
            string_index: "fluree:file://t/main/objects/dicts/sti.idx".into(),
            string_reverse: "fluree:file://t/main/objects/dicts/str.rev".into(),
            namespaces: "fluree:file://t/main/objects/dicts/ns.json".into(),
            numbig: BTreeMap::new(),
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
            namespace_codes: {
                let mut m = BTreeMap::new();
                m.insert(0, String::new());
                m.insert(1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".into());
                m
            },
            dict_addresses: sample_dict_addresses(),
            stats: None,
            schema: None,
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

        let root1 = BinaryIndexRootV2::from_cas_artifacts(
            "test/main", 42, 1, &ns, sample_dict_addresses(), graph_addrs.clone(),
            None, None,
        );
        let root2 = BinaryIndexRootV2::from_cas_artifacts(
            "test/main", 42, 1, &ns, sample_dict_addresses(), graph_addrs,
            None, None,
        );

        let bytes1 = root1.to_json_bytes().unwrap();
        let bytes2 = root2.to_json_bytes().unwrap();
        assert_eq!(bytes1, bytes2, "canonical JSON must be deterministic");

        let hash1 = fluree_db_core::sha256_hex(&bytes1);
        let hash2 = fluree_db_core::sha256_hex(&bytes2);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn parse_index_root_v1() {
        let v1 = BinaryIndexRoot {
            version: 1,
            ledger_alias: "x".into(),
            index_t: 0,
            base_t: 0,
            graphs: vec![],
            orders: vec![],
            namespace_codes: HashMap::new(),
            stats: None,
            schema: None,
            runs_addr_prefix: "".into(),
            index_addr_prefix: "".into(),
        };
        let bytes = v1.to_json_bytes().unwrap();
        let result = parse_index_root(&bytes).unwrap();
        assert!(matches!(result, BinaryIndexRootAny::V1(_)));
    }

    #[test]
    fn parse_index_root_v2() {
        let root = BinaryIndexRootV2::from_cas_artifacts(
            "x", 0, 0, &HashMap::new(), sample_dict_addresses(), vec![],
            None, None,
        );
        let bytes = root.to_json_bytes().unwrap();
        let result = parse_index_root(&bytes).unwrap();
        assert!(matches!(result, BinaryIndexRootAny::V2(_)));
    }

    #[test]
    fn parse_index_root_rejects_unknown() {
        let json = r#"{"version": 99}"#;
        let result = parse_index_root(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn v2_stats_round_trip() {
        let stats = serde_json::json!({
            "flakes": 12345,
            "size": 0,
            "graphs": [{"g_id": 1, "flakes": 10000, "size": 0}]
        });
        let root = BinaryIndexRootV2::from_cas_artifacts(
            "test/main", 42, 1, &HashMap::new(), sample_dict_addresses(), vec![],
            Some(stats.clone()), None,
        );

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
}
