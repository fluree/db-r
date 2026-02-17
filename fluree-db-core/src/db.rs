//! Database struct
//!
//! The `Db` struct represents a database value at a specific point in time.
//! It is a pure value type — no storage backend reference.

use crate::content_id::ContentId;
use crate::content_kind::ContentKind;
use crate::error::{Error, Result};
use crate::index_schema::IndexSchema;
use crate::index_stats::IndexStats;
use crate::namespaces::default_namespace_codes;
use crate::range_provider::RangeProvider;
use crate::schema_hierarchy::SchemaHierarchy;
use crate::sid::Sid;
use crate::storage::StorageRead;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata for creating a database from its index root.
///
/// Bundles all the metadata fields extracted from a v2 BinaryIndexRoot
/// for constructing a metadata-only `Db`.
pub struct DbMetadata {
    /// Ledger ID (e.g., "mydb:main")
    pub ledger_id: String,
    /// Current transaction time
    pub t: i64,
    /// Namespace code -> IRI prefix mapping
    pub namespace_codes: HashMap<u16, String>,
    /// Index statistics (flakes count, total size)
    pub stats: Option<IndexStats>,
    /// Schema (class/property hierarchy)
    pub schema: Option<IndexSchema>,
    /// Per-namespace max local_id watermarks from the index root
    pub subject_watermarks: Vec<u64>,
    /// Max assigned string_id from the index root
    pub string_watermark: u32,
}

/// Database value at a specific point in time.
///
/// A pure value type — no storage backend reference. All I/O (loading,
/// writing) happens at the call site, not inside `Db`.
pub struct Db {
    /// Ledger ID (e.g., "mydb:main")
    pub ledger_id: String,
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
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Self {
            ledger_id: self.ledger_id.clone(),
            t: self.t,
            version: self.version,
            namespace_codes: self.namespace_codes.clone(),
            stats: self.stats.clone(),
            schema: self.schema.clone(),
            schema_hierarchy_cache: self.schema_hierarchy_cache.clone(),
            subject_watermarks: self.subject_watermarks.clone(),
            string_watermark: self.string_watermark,
            range_provider: self.range_provider.clone(),
        }
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("ledger_id", &self.ledger_id)
            .field("t", &self.t)
            .field("version", &self.version)
            .field(
                "range_provider",
                &self.range_provider.as_ref().map(|_| "..."),
            )
            .finish_non_exhaustive()
    }
}

impl Db {
    /// Create a genesis (empty) database for a new ledger.
    ///
    /// Used when a nameservice has a commit but no index yet.
    /// The database starts at t=0 with no base data.  Queries against
    /// a genesis Db return overlay (novelty) flakes only.
    pub fn genesis(ledger_id: &str) -> Self {
        Self {
            ledger_id: ledger_id.to_string(),
            t: 0,
            version: 3,
            // Seed with baseline Fluree namespace codes (matches Clojure genesis-root-map).
            namespace_codes: default_namespace_codes(),
            stats: None,
            schema: None,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            range_provider: None,
        }
    }

    /// Create a Db from metadata only (no index roots).
    ///
    /// Used when the binary columnar index is the source of truth.
    /// The Db carries namespace codes, stats, and schema for callers
    /// that need ledger metadata, while all actual range queries go
    /// through `BinaryIndexStore` / `BinaryScanOperator`.
    pub fn new_meta(meta: DbMetadata) -> Self {
        Self {
            ledger_id: meta.ledger_id,
            t: meta.t,
            version: 3,
            namespace_codes: meta.namespace_codes,
            stats: meta.stats,
            schema: meta.schema,
            schema_hierarchy_cache: OnceCell::new(),
            subject_watermarks: meta.subject_watermarks,
            string_watermark: meta.string_watermark,
            range_provider: None,
        }
    }

    /// Extract metadata from raw index root bytes (IRB1 binary format).
    pub fn from_root_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 || &bytes[0..4] != b"IRB1" {
            return Err(Error::invalid_index(
                "index root: expected IRB1 magic bytes",
            ));
        }
        if bytes.len() < 24 {
            return Err(Error::invalid_index("IRB1: truncated root (< 24 bytes)"));
        }
        Self::from_irb1_header(bytes)
    }

    /// Parse an IRB1 binary root to extract all metadata fields.
    ///
    /// Skips graph routing sections (leaf entries, branch CIDs) and decodes
    /// the optional stats/schema sections using `stats_wire` decoders.
    fn from_irb1_header(data: &[u8]) -> Result<Self> {
        fn read_u8(data: &[u8], pos: &mut usize) -> Result<u8> {
            if *pos >= data.len() {
                return Err(Error::invalid_index("IRB1: unexpected EOF (u8)"));
            }
            let v = data[*pos];
            *pos += 1;
            Ok(v)
        }
        fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
            if *pos + 2 > data.len() {
                return Err(Error::invalid_index("IRB1: unexpected EOF (u16)"));
            }
            let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
            Ok(v)
        }
        fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
            if *pos + 4 > data.len() {
                return Err(Error::invalid_index("IRB1: unexpected EOF (u32)"));
            }
            let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            Ok(v)
        }
        fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
            if *pos + 8 > data.len() {
                return Err(Error::invalid_index("IRB1: unexpected EOF (u64)"));
            }
            let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(v)
        }
        fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64> {
            if *pos + 8 > data.len() {
                return Err(Error::invalid_index("IRB1: unexpected EOF (i64)"));
            }
            let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(v)
        }
        fn read_str(data: &[u8], pos: &mut usize) -> Result<String> {
            let len = read_u16(data, pos)? as usize;
            if *pos + len > data.len() {
                return Err(Error::invalid_index("IRB1: string overflow"));
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .map_err(|e| Error::invalid_index(format!("IRB1: invalid UTF-8: {e}")))?;
            *pos += len;
            Ok(s.to_string())
        }
        fn skip_cid(data: &[u8], pos: &mut usize) -> Result<()> {
            let len = read_u16(data, pos)? as usize;
            if *pos + len > data.len() {
                return Err(Error::invalid_index("IRB1: CID overflow"));
            }
            *pos += len;
            Ok(())
        }
        fn skip_dict_tree_refs(data: &[u8], pos: &mut usize) -> Result<()> {
            // branch CID + leaf_count:u32 + leaf CIDs
            skip_cid(data, pos)?;
            let leaf_count = read_u32(data, pos)? as usize;
            for _ in 0..leaf_count {
                skip_cid(data, pos)?;
            }
            Ok(())
        }
        fn skip_string_array(data: &[u8], pos: &mut usize) -> Result<()> {
            let count = read_u16(data, pos)? as usize;
            for _ in 0..count {
                let len = read_u16(data, pos)? as usize;
                if *pos + len > data.len() {
                    return Err(Error::invalid_index("IRB1: string array overflow"));
                }
                *pos += len;
            }
            Ok(())
        }

        // RunRecord wire size: s_id(8)+o_key(8)+p_id(4)+t(4)+i(4)+dt(2)+lang(2)+o_kind(1)+op(1) = 34
        const RECORD_WIRE_SIZE: usize = 34;
        const FLAG_HAS_STATS: u8 = 1 << 0;
        const FLAG_HAS_SCHEMA: u8 = 1 << 1;

        // Validate version (caller already checked len >= 24 and magic)
        let version = data[4];
        if version != 1 {
            return Err(Error::invalid_index(format!(
                "IRB1: unsupported version {version}"
            )));
        }

        let flags = data[5];
        let mut pos = 8; // skip magic(4) + version(1) + flags(1) + pad(2)
        let index_t = read_i64(data, &mut pos)?;
        let _base_t = read_i64(data, &mut pos)?;

        // Ledger ID
        let ledger_id = read_str(data, &mut pos)?;

        // Subject ID encoding (1 byte, skip)
        pos += 1;

        // Namespace codes
        let ns_count = read_u16(data, &mut pos)? as usize;
        let mut namespace_codes = HashMap::with_capacity(ns_count);
        for _ in 0..ns_count {
            let ns_code = read_u16(data, &mut pos)?;
            let prefix = read_str(data, &mut pos)?;
            namespace_codes.insert(ns_code, prefix);
        }

        // Predicate SIDs (skip)
        let pred_count = read_u32(data, &mut pos)? as usize;
        for _ in 0..pred_count {
            let _ns = read_u16(data, &mut pos)?;
            let _suffix = read_str(data, &mut pos)?;
        }

        // Small dict inlines: graph_iris, datatype_iris, language_tags (skip all)
        skip_string_array(data, &mut pos)?;
        skip_string_array(data, &mut pos)?;
        skip_string_array(data, &mut pos)?;

        // Dict refs: 4 trees + numbig + vectors (skip all)
        skip_dict_tree_refs(data, &mut pos)?; // subject_forward
        skip_dict_tree_refs(data, &mut pos)?; // subject_reverse
        skip_dict_tree_refs(data, &mut pos)?; // string_forward
        skip_dict_tree_refs(data, &mut pos)?; // string_reverse
        let numbig_count = read_u16(data, &mut pos)? as usize;
        for _ in 0..numbig_count {
            let _p_id = read_u32(data, &mut pos)?;
            skip_cid(data, &mut pos)?;
        }
        let vector_count = read_u16(data, &mut pos)? as usize;
        for _ in 0..vector_count {
            let _p_id = read_u32(data, &mut pos)?;
            skip_cid(data, &mut pos)?; // manifest
            let shard_count = read_u16(data, &mut pos)? as usize;
            for _ in 0..shard_count {
                skip_cid(data, &mut pos)?;
            }
        }

        // Watermarks
        let wm_count = read_u16(data, &mut pos)? as usize;
        let mut subject_watermarks = Vec::with_capacity(wm_count);
        for _ in 0..wm_count {
            subject_watermarks.push(read_u64(data, &mut pos)?);
        }
        let string_watermark = read_u32(data, &mut pos)?;

        // Cumulative commit stats (3x u64, skip)
        let _total_commit_size = read_u64(data, &mut pos)?;
        let _total_asserts = read_u64(data, &mut pos)?;
        let _total_retracts = read_u64(data, &mut pos)?;

        // Default graph routing (inline leaf entries, skip)
        let default_order_count = read_u8(data, &mut pos)? as usize;
        for _ in 0..default_order_count {
            let _order_id = read_u8(data, &mut pos)?;
            let leaf_count = read_u32(data, &mut pos)? as usize;
            for _ in 0..leaf_count {
                // first_key + last_key (each RECORD_WIRE_SIZE) + row_count(u64) + leaf_cid
                let skip = RECORD_WIRE_SIZE * 2 + 8;
                if pos + skip > data.len() {
                    return Err(Error::invalid_index("IRB1: leaf entry overflow"));
                }
                pos += skip;
                skip_cid(data, &mut pos)?;
            }
        }

        // Named graph routing (branch CIDs, skip)
        let named_count = read_u16(data, &mut pos)? as usize;
        for _ in 0..named_count {
            let _g_id = read_u16(data, &mut pos)?;
            let order_count = read_u8(data, &mut pos)? as usize;
            for _ in 0..order_count {
                let _order_id = read_u8(data, &mut pos)?;
                skip_cid(data, &mut pos)?;
            }
        }

        // Optional: stats
        let stats = if flags & FLAG_HAS_STATS != 0 {
            let stats_len = read_u32(data, &mut pos)? as usize;
            if pos + stats_len > data.len() {
                return Err(Error::invalid_index("IRB1: stats section overflow"));
            }
            let (stats, _consumed) =
                crate::stats_wire::decode_stats(&data[pos..pos + stats_len])
                    .map_err(|e| Error::invalid_index(format!("IRB1: stats decode: {e}")))?;
            pos += stats_len;
            Some(stats)
        } else {
            None
        };

        // Optional: schema
        let schema = if flags & FLAG_HAS_SCHEMA != 0 {
            let schema_len = read_u32(data, &mut pos)? as usize;
            if pos + schema_len > data.len() {
                return Err(Error::invalid_index("IRB1: schema section overflow"));
            }
            let (schema, _consumed) =
                crate::stats_wire::decode_schema(&data[pos..pos + schema_len])
                    .map_err(|e| Error::invalid_index(format!("IRB1: schema decode: {e}")))?;
            pos += schema_len;
            Some(schema)
        } else {
            None
        };

        let _ = pos; // remaining optional sections (prev_index, garbage, sketch) not needed

        let meta = DbMetadata {
            ledger_id,
            t: index_t,
            namespace_codes,
            stats,
            schema,
            subject_watermarks,
            string_watermark,
        };
        Ok(Self::new_meta(meta))
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

/// Load a database from an index root content ID.
///
/// The returned Db is metadata-only (`range_provider = None`). The caller
/// (typically the API layer) must load a `BinaryIndexStore` and attach a
/// `BinaryRangeProvider` before serving range queries.
///
/// The storage address is derived internally from the `ContentId` and
/// `ledger_id` using the storage backend's method identifier.
pub async fn load_db(
    storage: &(impl StorageRead + crate::storage::StorageMethod),
    root_id: &ContentId,
    ledger_id: &str,
) -> Result<Db> {
    let root_address = crate::content_address(
        storage.storage_method(),
        ContentKind::IndexRoot,
        ledger_id,
        &root_id.digest_hex(),
    );
    let bytes = storage.read_bytes(&root_address).await?;
    Db::from_root_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_root_bytes_rejects_non_irb1() {
        let json = b"{\"ledger_id\": \"test:main\"}";
        let err = Db::from_root_bytes(json).unwrap_err();
        assert!(err.to_string().contains("IRB1"));
    }

    #[test]
    fn test_from_root_bytes_rejects_truncated() {
        let err = Db::from_root_bytes(b"IRB1").unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn test_encode_decode_sid() {
        let mut ns = HashMap::new();
        ns.insert(0u16, String::new());
        ns.insert(100u16, "http://example.org/".to_string());
        let db = Db::new_meta(DbMetadata {
            ledger_id: "test:main".into(),
            t: 1,
            namespace_codes: ns,
            stats: None,
            schema: None,
            subject_watermarks: vec![],
            string_watermark: 0,
        });

        let sid = db.encode_iri("http://example.org/Alice").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Alice");

        let iri = db.decode_sid(&sid).unwrap();
        assert_eq!(iri, "http://example.org/Alice");
    }

    #[test]
    fn test_genesis_db() {
        let db = Db::genesis("test:main");
        assert_eq!(db.t, 0);
        assert_eq!(db.ledger_id, "test:main");
        assert!(db.range_provider.is_none());
    }
}
