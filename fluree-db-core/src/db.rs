//! Database struct
//!
//! The `Db` struct represents a database value at a specific point in time.
//! It is generic over storage and cache implementations.

use crate::error::{Error, Result};
use crate::range_provider::RangeProvider;
use crate::schema_hierarchy::SchemaHierarchy;
use crate::index_stats::IndexStats;
use crate::index_schema::IndexSchema;
use crate::serde::json::{
    raw_schema_to_index_schema, raw_stats_to_index_stats,
    RawDbRootSchema, RawDbRootStats,
};
use crate::sid::Sid;
use crate::storage::Storage;
use crate::namespaces::default_namespace_codes;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Database value at a specific point in time
///
/// Generic over:
/// - `S`: Storage backend
pub struct Db<S> {
    /// Ledger alias (e.g., "mydb/main")
    pub alias: String,
    /// Current transaction time
    pub t: i64,
    /// Index version
    pub version: i32,

    /// Namespace code -> IRI prefix mapping
    pub namespace_codes: HashMap<u16, String>,

    /// Index statistics (flakes count, total size)
    pub stats: Option<IndexStats>,
    /// Schema (class/property hierarchy)
    pub schema: Option<IndexSchema>,

    /// Cached schema hierarchy for reasoning (lazily computed)
    schema_hierarchy_cache: OnceCell<SchemaHierarchy>,

    /// Per-namespace max local_id watermarks from the index root.
    ///
    /// Used by `DictNovelty` to route forward lookups between the
    /// persisted dictionary tree and the novel overlay. Empty vec
    /// means "everything is novel" (genesis or old root without watermarks).
    pub subject_watermarks: Vec<u64>,

    /// Max assigned string_id from the index root. 0 = no strings indexed.
    pub string_watermark: u32,

    /// Binary range provider.
    ///
    /// When set, `range_with_overlay()` delegates to this provider.
    /// All existing range callers (reasoner, API, policy, SHACL) use this
    /// automatically.
    pub range_provider: Option<Arc<dyn RangeProvider>>,

    /// Storage backend
    pub storage: S,
}

impl<S: Clone> Clone for Db<S> {
    fn clone(&self) -> Self {
        Self {
            alias: self.alias.clone(),
            t: self.t,
            version: self.version,
            namespace_codes: self.namespace_codes.clone(),
            stats: self.stats.clone(),
            schema: self.schema.clone(),
            schema_hierarchy_cache: self.schema_hierarchy_cache.clone(),
            subject_watermarks: self.subject_watermarks.clone(),
            string_watermark: self.string_watermark,
            range_provider: self.range_provider.clone(),
            storage: self.storage.clone(),
        }
    }
}

impl<S: std::fmt::Debug> std::fmt::Debug for Db<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("alias", &self.alias)
            .field("t", &self.t)
            .field("version", &self.version)
            .field("range_provider", &self.range_provider.as_ref().map(|_| "..."))
            .finish_non_exhaustive()
    }
}

impl<S: Storage> Db<S> {
    /// Create a genesis (empty) database for a new ledger.
    ///
    /// Used when a nameservice has a commit but no index yet.
    /// The database starts at t=0 with no base data.  Queries against
    /// a genesis Db return overlay (novelty) flakes only.
    pub fn genesis(storage: S, alias: &str) -> Self {
        Self {
            alias: alias.to_string(),
            t: 0,
            version: 2,
            // Seed with baseline Fluree namespace codes (matches Clojure genesis-root-map).
            namespace_codes: default_namespace_codes(),
            stats: None,
            schema: None,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            range_provider: None,
            storage,
        }
    }

    /// Create a Db from metadata only (no index roots).
    ///
    /// Used when the binary columnar index is the source of truth.
    /// The Db carries namespace codes, stats, and schema for callers
    /// that need ledger metadata, while all actual range queries go
    /// through `BinaryIndexStore` / `BinaryScanOperator`.
    pub fn new_meta(
        alias: String,
        t: i64,
        namespace_codes: HashMap<u16, String>,
        stats: Option<IndexStats>,
        schema: Option<IndexSchema>,
        subject_watermarks: Vec<u64>,
        string_watermark: u32,
        storage: S,
    ) -> Self {
        Self {
            alias,
            t,
            version: 2,
            namespace_codes,
            stats,
            schema,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks,
            string_watermark,
            range_provider: None,
            storage,
        }
    }

    /// Load a database from a v2 index root address.
    ///
    /// Only v2 (BinaryIndexRootV2) roots are supported. The `"version"` field
    /// in the JSON root must be `2`; any other value is rejected.
    ///
    /// The returned Db is metadata-only (`range_provider = None`). The caller
    /// (typically the API layer) must load a `BinaryIndexStore` and attach a
    /// `BinaryRangeProvider` before serving range queries.
    pub async fn load(storage: S, root_address: &str) -> Result<Self> {
        let bytes = storage.read_bytes(root_address).await?;
        let root_json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::invalid_index(format!("invalid root JSON: {}", e)))?;

        let version = root_json
            .get("version")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        match version {
            2 => Self::from_v2_json(storage, &root_json),
            v => Err(Error::invalid_index(format!(
                "unsupported index root version: {} (only v2 supported)",
                v
            ))),
        }
    }

    /// Extract metadata from a v2 BinaryIndexRootV2 JSON blob.
    fn from_v2_json(storage: S, root: &serde_json::Value) -> Result<Self> {
        let alias = root["ledger_alias"]
            .as_str()
            .ok_or_else(|| Error::invalid_index("v2 root missing ledger_alias"))?
            .to_string();
        let t = root["index_t"]
            .as_i64()
            .ok_or_else(|| Error::invalid_index("v2 root missing index_t"))?;

        let mut namespace_codes = HashMap::new();
        if let Some(obj) = root["namespace_codes"].as_object() {
            for (k, val) in obj {
                if let (Ok(code), Some(prefix)) = (k.parse::<u16>(), val.as_str()) {
                    namespace_codes.insert(code, prefix.to_string());
                }
            }
        }

        let stats = root
            .get("stats")
            .and_then(|s| serde_json::from_value::<RawDbRootStats>(s.clone()).ok())
            .and_then(|ref raw| raw_stats_to_index_stats(raw));
        let schema = root
            .get("schema")
            .and_then(|s| serde_json::from_value::<RawDbRootSchema>(s.clone()).ok())
            .map(|ref raw| raw_schema_to_index_schema(raw));

        // Dictionary watermarks (forwards-safe: missing fields default to empty/0)
        let subject_watermarks = root
            .get("subject_watermarks")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();
        let string_watermark = root
            .get("string_watermark")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        Ok(Self::new_meta(alias, t, namespace_codes, stats, schema, subject_watermarks, string_watermark, storage))
    }

    /// Attach a range provider for binary index queries.
    pub fn with_range_provider(mut self, provider: Arc<dyn RangeProvider>) -> Self {
        self.range_provider = Some(provider);
        self
    }

    /// Encode an IRI to a SID using this db's namespace codes.
    ///
    /// Returns None if the namespace is not registered.
    pub fn encode_iri(&self, iri: &str) -> Option<Sid> {
        let mut best_match: Option<(u16, usize)> = None;

        for (&code, prefix) in &self.namespace_codes {
            if iri.starts_with(prefix) && prefix.len() > best_match.map(|(_, l)| l).unwrap_or(0) {
                best_match = Some((code, prefix.len()));
            }
        }

        best_match.map(|(code, prefix_len)| {
            let name = &iri[prefix_len..];
            Sid::new(code, name)
        })
    }

    /// Decode a SID to an IRI using this db's namespace codes.
    ///
    /// Returns None if the namespace code is not registered.
    pub fn decode_sid(&self, sid: &Sid) -> Option<String> {
        self.namespace_codes
            .get(&sid.namespace_code)
            .map(|prefix| format!("{}{}", prefix, sid.name))
    }

    /// Get all registered namespace codes
    pub fn namespaces(&self) -> &HashMap<u16, String> {
        &self.namespace_codes
    }

    /// Get the schema hierarchy for RDFS reasoning.
    pub fn schema_hierarchy(&self) -> Option<SchemaHierarchy> {
        self.schema.as_ref().map(|schema| {
            self.schema_hierarchy_cache
                .get_or_init(|| SchemaHierarchy::from_db_root_schema(schema))
                .clone()
        })
    }

    /// Get the schema epoch (transaction ID when schema was last updated).
    pub fn schema_epoch(&self) -> Option<u64> {
        self.schema.as_ref().map(|s| s.t as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;

    #[tokio::test]
    async fn test_db_load_v2() {
        let root_json = serde_json::json!({
            "version": 2,
            "ledger_alias": "test/main",
            "index_t": 42,
            "base_t": 0,
            "namespace_codes": {
                "0": "",
                "1": "@",
                "100": "http://example.org/"
            },
            "dicts": {},
            "graphs": [],
            "stats": {
                "flakes": 1000,
                "size": 0,
                "graphs": [{"g_id": 1, "flakes": 1000, "size": 0}]
            }
        });

        let storage = MemoryStorage::new();
        storage.insert("test-root", serde_json::to_vec(&root_json).unwrap());

        let db = Db::load(storage, "test-root").await.unwrap();
        assert_eq!(db.alias, "test/main");
        assert_eq!(db.t, 42);
        assert_eq!(db.version, 2);
        assert!(db.range_provider.is_none());
        assert!(db.stats.is_some());
    }

    #[tokio::test]
    async fn test_db_load_unsupported_version() {
        let root_json = serde_json::json!({
            "version": 99
        });

        let storage = MemoryStorage::new();
        storage.insert("test-root", serde_json::to_vec(&root_json).unwrap());

        let err = Db::load(storage, "test-root").await.unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_db_encode_decode_sid() {
        let root_json = serde_json::json!({
            "version": 2,
            "ledger_alias": "test/main",
            "index_t": 1,
            "base_t": 0,
            "namespace_codes": {
                "0": "",
                "100": "http://example.org/"
            },
            "dicts": {},
            "graphs": []
        });

        let storage = MemoryStorage::new();
        storage.insert("test-root", serde_json::to_vec(&root_json).unwrap());
        let db = Db::load(storage, "test-root").await.unwrap();

        let sid = db.encode_iri("http://example.org/Alice").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Alice");

        let iri = db.decode_sid(&sid).unwrap();
        assert_eq!(iri, "http://example.org/Alice");
    }

    #[test]
    fn test_genesis_db() {
        let storage = MemoryStorage::new();
        let db = Db::genesis(storage, "test/main");
        assert_eq!(db.t, 0);
        assert_eq!(db.alias, "test/main");
        assert!(db.range_provider.is_none());
    }
}
