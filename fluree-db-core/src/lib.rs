//! # Fluree DB Core
//!
//! Runtime-agnostic core library for Fluree DB queries.
//!
//! This crate provides:
//! - Core types: `Sid`, `FlakeValue`, `Flake`
//! - Index comparators for all 4 orderings (SPOT, PSOT, POST, OPST)
//! - Storage and cache trait interfaces
//! - Range query implementation
//!
//! ## Design Principles
//!
//! 1. **Runtime-agnostic**: No tokio, no forced `Send + Sync`
//! 2. **Async at I/O seam only**: Synchronous traversal once data is in memory
//! 3. **Strict total ordering**: No nil-as-wildcard; use explicit min/max bounds
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_core::{Db, range, IndexType, RangeTest};
//!
//! // Apps provide their own Storage implementation
//! let db = Db::load(storage, address).await?;
//! let flakes = range(&db, IndexType::Spot, RangeTest::Eq, match_val, opts).await?;
//! ```

pub mod address;
pub mod address_path;
pub mod alias;
pub mod coerce;
pub mod comparator;
pub mod db;
pub mod dict_novelty;
pub mod error;
pub mod flake;
pub mod geo;
pub mod ids;
pub mod index_schema;
pub mod index_stats;
pub mod namespaces;
pub mod overlay;
pub mod query_bounds;
pub mod range;
pub mod range_provider;
pub mod schema_hierarchy;
pub mod serde;
pub mod sid;
pub mod stats_view;
pub mod storage;
pub mod subject_id;
pub mod temporal;
pub mod tracking;
pub mod value;
pub mod value_id;

// Re-export main types
pub use address::{extract_identifier, extract_path, parse_fluree_address, ParsedFlureeAddress};
pub use alias::{
    format_alias, normalize_alias, parse_alias_with_time, split_alias, split_time_travel_suffix,
    AliasParseError, AliasTimeSpec, ParsedAlias, DEFAULT_BRANCH,
};
pub use coerce::{coerce_json_value, coerce_value, CoercionError, CoercionResult};
pub use comparator::IndexType;
pub use db::Db;
pub use dict_novelty::DictNovelty;
pub use error::{Error, Result};
pub use flake::{Flake, FlakeMeta};
pub use ids::{DatatypeDictId, GraphId, LangId, ListIndex, PredicateId, StringId, TxnT};
pub use index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
pub use index_stats::{
    ClassPropertyUsage, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry, IndexStats,
    PropertyStatEntry,
};
pub use namespaces::{
    default_namespace_codes, is_owl_equivalent_class, is_owl_equivalent_property,
    is_owl_inverse_of, is_owl_same_as, is_owl_symmetric_property, is_owl_transitive_property,
    is_rdf_first, is_rdf_nil, is_rdf_rest, is_rdf_type, is_rdfs_domain, is_rdfs_range,
    is_rdfs_subclass_of, is_rdfs_subproperty_of,
};
pub use overlay::{NoOverlay, OverlayProvider};
pub use range::{
    range, range_bounded_with_overlay, range_with_overlay, ObjectBounds, RangeMatch, RangeOptions,
    RangeTest, BATCHED_JOIN_SIZE,
};
pub use range_provider::RangeProvider;
pub use schema_hierarchy::SchemaHierarchy;
pub use sid::{Sid, SidInterner};
pub use stats_view::{PropertyStatData, StatsView};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use storage::FileStorage;
pub use storage::{
    // Helper functions for storage implementations
    alias_prefix_for_path,
    content_address,
    content_path,
    sha256_hex,
    ContentAddressedWrite,
    ContentKind,
    ContentWriteResult,
    DictKind,
    MemoryStorage,
    ReadHint,
    Storage,
    StorageRead,
    StorageWrite,
};
pub use subject_id::{SubjectId, SubjectIdColumn, SubjectIdEncoding};
pub use temporal::{Date, DateTime, Time};
pub use tracking::{FuelExceededError, PolicyStats, Tracker, TrackingOptions, TrackingTally};
pub use value::{
    parse_decimal, parse_decimal_string, parse_double, parse_integer, parse_integer_string,
    FlakeValue, GeoPointBits,
};
pub use value_id::{ObjKey, ObjKeyError, ObjKind, ObjPair, ValueTypeTag};

/// Prelude module for convenient imports of storage traits and common types.
///
/// # Example
///
/// ```ignore
/// use fluree_db_core::prelude::*;
///
/// async fn example<S: Storage>(storage: &S) {
///     let bytes = storage.read_bytes("address").await?;
///     storage.write_bytes("address", &bytes).await?;
/// }
/// ```
pub mod prelude {
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    pub use crate::storage::FileStorage;
    pub use crate::storage::{
        ContentAddressedWrite, ContentKind, ContentWriteResult, DictKind, MemoryStorage, ReadHint,
        Storage, StorageRead, StorageWrite,
    };
}
