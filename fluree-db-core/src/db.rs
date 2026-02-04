//! Database struct
//!
//! The `Db` struct represents a database value at a specific point in time.
//! It is generic over storage and cache implementations.
//!
//! # Reserved Node IDs
//!
//! The node ID `"empty"` is reserved for representing empty index roots in
//! genesis databases. This ID is special-cased in the range query resolver
//! to return an empty leaf without any storage reads. Storage backends must
//! never use `"empty"` as a real node address.

use crate::cache::NodeCache;
use crate::comparator::IndexType;
use crate::error::{Error, Result};
use crate::flake::Flake;
use crate::index::{ChildRef, IndexNode};
use crate::namespaces::default_namespace_codes;
use crate::schema_hierarchy::SchemaHierarchy;
use crate::serde::json::{parse_db_root, DbRoot, DbRootConfig, DbRootSchema, DbRootStats};
use crate::sid::{Sid, SidInterner};
use crate::storage::Storage;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Reserved node ID for empty index roots.
///
/// Used in genesis databases to represent indexes with no data.
/// The range query resolver special-cases this ID to return an empty leaf
/// without storage reads. Storage backends must never use this as a real address.
pub const EMPTY_NODE_ID: &str = "empty";

/// Database value at a specific point in time
///
/// Generic over:
/// - `S`: Storage backend
/// - `C`: Node cache
#[derive(Debug)]
pub struct Db<S, C> {
    /// Ledger alias (e.g., "mydb/main")
    pub alias: String,
    /// Current transaction time
    pub t: i64,
    /// Index version (1 or 2)
    pub version: i32,

    /// SPOT index root
    pub spot: Option<ChildRef>,
    /// PSOT index root
    pub psot: Option<ChildRef>,
    /// POST index root
    pub post: Option<ChildRef>,
    /// OPST index root
    pub opst: Option<ChildRef>,
    /// TSPO index root
    pub tspo: Option<ChildRef>,

    /// Namespace code -> IRI prefix mapping
    pub namespace_codes: HashMap<i32, String>,

    /// Index statistics (flakes count, total size)
    pub stats: Option<DbRootStats>,
    /// Index configuration (reindex thresholds)
    pub config: Option<DbRootConfig>,
    /// Schema (class/property hierarchy)
    pub schema: Option<DbRootSchema>,

    /// Cached schema hierarchy for reasoning (lazily computed)
    schema_hierarchy_cache: OnceCell<SchemaHierarchy>,

    /// SID interner for deduplicating names across decoded index nodes
    ///
    /// This reduces memory usage and makes cloning/compare cheaper when many
    /// flakes share the same SIDs.
    ///
    /// Wrapped in `Arc` to allow cheap cloning for `spawn_blocking` CPU offload
    /// during leaf parsing (native only).
    pub sid_interner: Arc<SidInterner>,

    /// Storage backend
    pub storage: S,
    /// Node cache (Arc-wrapped to enable sharing across cloned Db instances)
    ///
    /// This is critical for prefetch: when Db is cloned for background prefetch tasks,
    /// both the mainline query and prefetch tasks must share the same cache instance
    /// so that prefetch actually warms the cache the query will use.
    pub cache: Arc<C>,
}

impl<S: Clone, C> Clone for Db<S, C> {
    fn clone(&self) -> Self {
        Self {
            alias: self.alias.clone(),
            t: self.t,
            version: self.version,
            spot: self.spot.clone(),
            psot: self.psot.clone(),
            post: self.post.clone(),
            opst: self.opst.clone(),
            tspo: self.tspo.clone(),
            namespace_codes: self.namespace_codes.clone(),
            stats: self.stats.clone(),
            config: self.config.clone(),
            schema: self.schema.clone(),
            schema_hierarchy_cache: self.schema_hierarchy_cache.clone(),
            sid_interner: self.sid_interner.clone(),
            storage: self.storage.clone(),
            cache: Arc::clone(&self.cache), // Share cache across clones for prefetch
        }
    }
}

impl<S: Storage, C: NodeCache> Db<S, C> {
    /// Create a genesis (empty) database for a new ledger
    ///
    /// Used when a nameservice has a commit but no index yet.
    /// The database starts at t=0 with empty index roots using [`EMPTY_NODE_ID`].
    ///
    /// The cache is wrapped in Arc internally to enable sharing across cloned Db instances,
    /// which is critical for prefetch to warm the same cache the query uses.
    pub fn genesis(storage: S, cache: impl Into<Arc<C>>, alias: &str) -> Self {
        let cache = cache.into();
        // Use a consistent empty leaf root for all indexes.
        // The resolver treats EMPTY_NODE_ID as an empty leaf with no storage reads.
        let empty_root = ChildRef {
            id: EMPTY_NODE_ID.to_string(),
            leaf: true,
            first: Some(Flake::max_spot()),
            rhs: None,
            size: 0,
            bytes: Some(0), // Empty node has no serialized content
            leftmost: true,
        };
        Self {
            alias: alias.to_string(),
            t: 0,
            version: 2,
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root),
            // Seed with baseline Fluree namespace codes (matches Clojure genesis-root-map).
            namespace_codes: default_namespace_codes(),
            stats: None,  // Genesis has no stats
            config: None, // Genesis has no config
            schema: None, // Genesis has no schema
            schema_hierarchy_cache: OnceCell::new(),
            sid_interner: Arc::new(SidInterner::with_capacity(4096)),
            storage,
            cache,
        }
    }

    /// Load a database from a root address
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend to read index data from
    /// * `cache` - Cache for resolved nodes (will be Arc-wrapped if not already)
    /// * `root_address` - Address of the DB root (index metadata)
    pub async fn load(storage: S, cache: impl Into<Arc<C>>, root_address: &str) -> Result<Self> {
        let cache = cache.into();
        // Read and parse the DB root
        let bytes = storage.read_bytes(root_address).await?;
        let root = parse_db_root(bytes)?;

        Ok(Self::from_root(storage, cache, root))
    }

    /// Create a Db from a parsed DbRoot
    ///
    /// The cache is wrapped in Arc internally to enable sharing across cloned Db instances,
    /// which is critical for prefetch to warm the same cache the query uses.
    pub fn from_root(storage: S, cache: impl Into<Arc<C>>, root: DbRoot) -> Self {
        Self {
            alias: root.alias,
            t: root.t,
            version: root.version,
            spot: root.spot,
            psot: root.psot,
            post: root.post,
            opst: root.opst,
            tspo: root.tspo,
            namespace_codes: root.namespace_codes,
            stats: root.stats,
            config: root.config,
            schema: root.schema,
            schema_hierarchy_cache: OnceCell::new(),
            sid_interner: Arc::new(SidInterner::with_capacity(4096)),
            storage,
            cache: cache.into(),
        }
    }

    /// Get the index root for a specific index type
    pub fn get_index_root(&self, index: IndexType) -> Result<IndexNode> {
        let child_ref = match index {
            IndexType::Spot => self.spot.as_ref(),
            IndexType::Psot => self.psot.as_ref(),
            IndexType::Post => self.post.as_ref(),
            IndexType::Opst => self.opst.as_ref(),
            IndexType::Tspo => self.tspo.as_ref(),
        };

        child_ref
            .map(|c| IndexNode::from_child_ref(c, index, self.alias.clone()))
            .ok_or_else(|| Error::invalid_index(format!("Index {:?} not available", index)))
    }

    /// Encode an IRI to a SID using this db's namespace codes
    ///
    /// Returns None if the namespace is not registered.
    pub fn encode_iri(&self, iri: &str) -> Option<Sid> {
        // Find the longest matching namespace prefix
        let mut best_match: Option<(i32, usize)> = None;

        for (&code, prefix) in &self.namespace_codes {
            if iri.starts_with(prefix) && prefix.len() > best_match.map(|(_, l)| l).unwrap_or(0) {
                best_match = Some((code, prefix.len()));
            }
        }

        best_match.map(|(code, prefix_len)| {
            let name = &iri[prefix_len..];
            self.sid_interner.intern(code, name)
        })
    }

    /// Decode a SID to an IRI using this db's namespace codes
    ///
    /// Returns None if the namespace code is not registered.
    pub fn decode_sid(&self, sid: &Sid) -> Option<String> {
        self.namespace_codes
            .get(&sid.namespace_code)
            .map(|prefix| format!("{}{}", prefix, sid.name))
    }

    /// Get all registered namespace codes
    pub fn namespaces(&self) -> &HashMap<i32, String> {
        &self.namespace_codes
    }

    /// Get the schema hierarchy for RDFS reasoning.
    ///
    /// Returns `None` if no schema is available.
    /// The hierarchy is lazily computed on first access and cached.
    /// Returns a cheap `Clone` (Arc-backed).
    pub fn schema_hierarchy(&self) -> Option<SchemaHierarchy> {
        self.schema.as_ref().map(|schema| {
            self.schema_hierarchy_cache
                .get_or_init(|| SchemaHierarchy::from_db_root_schema(schema))
                .clone()
        })
    }

    /// Get the schema epoch (transaction ID when schema was last updated).
    ///
    /// Useful for cache invalidation and diagnostics.
    /// Returns `None` if no schema is available.
    pub fn schema_epoch(&self) -> Option<u64> {
        self.schema.as_ref().map(|s| s.t as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SimpleCache;
    use crate::storage::MemoryStorage;

    fn make_db_root_json() -> String {
        r#"{
            "ledger-alias": "test/main",
            "t": 100,
            "v": 2,
            "namespace-codes": {
                "0": "",
                "1": "@",
                "2": "http://www.w3.org/2001/XMLSchema#",
                "100": "http://example.org/"
            },
            "spot": {
                "id": "spot-root",
                "leaf": false,
                "size": 1000
            },
            "psot": {
                "id": "psot-root",
                "leaf": false,
                "size": 1000
            },
            "post": {
                "id": "post-root",
                "leaf": false,
                "size": 1000
            },
            "opst": {
                "id": "opst-root",
                "leaf": false,
                "size": 1000
            },
            "tspo": {
                "id": "tspo-root",
                "leaf": false,
                "size": 1000
            }
        }"#
        .to_string()
    }

    #[tokio::test]
    async fn test_db_load() {
        let storage = MemoryStorage::new();
        storage.insert("test-root", make_db_root_json().into_bytes());

        let cache = SimpleCache::new(100);
        let db = Db::load(storage, cache, "test-root").await.unwrap();

        assert_eq!(db.alias, "test/main");
        assert_eq!(db.t, 100);
        assert_eq!(db.version, 2);
        assert!(db.spot.is_some());
        assert!(db.psot.is_some());
    }

    #[tokio::test]
    async fn test_db_encode_decode_sid() {
        let storage = MemoryStorage::new();
        storage.insert("test-root", make_db_root_json().into_bytes());

        let cache = SimpleCache::new(100);
        let db = Db::load(storage, cache, "test-root").await.unwrap();

        // Encode an IRI
        let sid = db.encode_iri("http://example.org/Alice").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Alice");

        // Decode back
        let iri = db.decode_sid(&sid).unwrap();
        assert_eq!(iri, "http://example.org/Alice");

        // XSD namespace
        let xsd_sid = db
            .encode_iri("http://www.w3.org/2001/XMLSchema#string")
            .unwrap();
        assert_eq!(xsd_sid.namespace_code, 2);
        assert_eq!(xsd_sid.name.as_ref(), "string");
    }

    #[tokio::test]
    async fn test_db_get_index_root() {
        let storage = MemoryStorage::new();
        storage.insert("test-root", make_db_root_json().into_bytes());

        let cache = SimpleCache::new(100);
        let db = Db::load(storage, cache, "test-root").await.unwrap();

        let spot_root = db.get_index_root(IndexType::Spot).unwrap();
        assert_eq!(spot_root.id, "spot-root");
        assert!(!spot_root.leaf);

        let psot_root = db.get_index_root(IndexType::Psot).unwrap();
        assert_eq!(psot_root.id, "psot-root");
    }

    /// Integration test with real test database
    ///
    /// Requires test-database to exist at ./test-database/
    /// Run with: cargo test integration_real_db -- --ignored
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Requires external test-database/ directory"]
    async fn integration_real_db_load() {
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        // Path to test database
        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            eprintln!("Test database not found at {:?}, skipping", test_db_path);
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        // Find the latest root file
        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .expect("Could not read root dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .expect("No root files found");

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        println!("Loading from: {}", root_address);

        let db = Db::load(storage, cache, &root_address).await.unwrap();

        println!("Loaded database:");
        println!("  alias: {}", db.alias);
        println!("  t: {}", db.t);
        println!("  version: {}", db.version);
        println!("  namespaces: {} codes", db.namespace_codes.len());

        // Verify basic properties
        assert!(db.alias.contains("range-scan"));
        assert!(db.t > 0);
        assert_eq!(db.version, 2);
        assert!(db.spot.is_some());
        assert!(db.psot.is_some());
        assert!(db.post.is_some());
        assert!(db.opst.is_some());
        assert!(db.tspo.is_some());

        // Check namespace codes include example.org
        let example_ns = db
            .namespace_codes
            .values()
            .any(|v| v.contains("example.org"));
        assert!(example_ns, "Should have example.org namespace");

        println!("Integration test passed!");
    }
}
