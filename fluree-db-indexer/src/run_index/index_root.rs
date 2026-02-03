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
use std::collections::HashMap;

/// Current schema version for `BinaryIndexRoot`.
pub const BINARY_INDEX_ROOT_VERSION: u32 = 1;

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
}
