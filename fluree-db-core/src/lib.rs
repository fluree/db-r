//! # Fluree DB Core
//!
//! Runtime-agnostic core library for Fluree DB queries.
//!
//! This crate provides:
//! - Core types: `Sid`, `FlakeValue`, `Flake`
//! - Index comparators for all 5 orderings (SPOT, PSOT, POST, OPST, TSPO)
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
//! // Apps provide their own Storage and Cache implementations
//! let db = Db::load(&storage, &cache, address).await?;
//! let flakes = range(&db, IndexType::Spot, RangeTest::Eq, match_val, opts).await?;
//! ```

pub mod sid;
pub mod value;
pub mod flake;
pub mod comparator;
pub mod index;
pub mod storage;
pub mod cache;
pub mod error;
pub mod serde;
pub mod overlay;
pub mod query_bounds;
pub mod range_provider;
pub mod range;
pub mod db;
pub mod namespaces;
pub mod schema_hierarchy;
pub mod stats_view;
pub mod tracking;
pub mod temporal;
pub mod alias;
pub mod address;
pub mod address_path;
pub mod prefetch;
pub mod coerce;
pub mod value_id;
pub mod index_stats;
pub mod index_schema;

// Re-export main types
pub use sid::{Sid, SidInterner};
pub use value::{FlakeValue, parse_integer, parse_decimal, parse_double, parse_integer_string, parse_decimal_string};
pub use temporal::{DateTime, Date, Time};
pub use alias::{
    AliasParseError, AliasTimeSpec, ParsedAlias, DEFAULT_BRANCH, format_alias, normalize_alias,
    parse_alias_with_time, split_alias, split_time_travel_suffix,
};
pub use flake::{Flake, FlakeMeta};
pub use comparator::IndexType;
pub use index::{IndexNode, ChildRef, ResolvedNode};
pub use storage::{
    ContentAddressedWrite, ContentKind, ContentWriteResult, MemoryStorage, ReadHint,
    Storage, StorageRead, StorageWrite,
    // Helper functions for storage implementations
    alias_prefix_for_path, content_address, content_path, sha256_hex,
};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use storage::FileStorage;
pub use cache::{NodeCache, SimpleCache, NoCache, CacheKey, CacheStats};
#[cfg(all(feature = "native", feature = "moka", feature = "dashmap", not(target_arch = "wasm32")))]
pub use cache::MokaNodeCache;
pub use overlay::{OverlayProvider, NoOverlay};
pub use error::{Error, Result};
pub use range_provider::RangeProvider;
pub use range::{range, range_with_overlay, range_bounded_with_overlay, RangeTest, RangeMatch, RangeOptions, ObjectBounds, RangeCursor, MultiSeekCursor, BATCHED_JOIN_SIZE, resolve_node_materialized_with_overlay};
#[cfg(feature = "native")]
pub use range::{set_max_concurrent_leaf_parses, DEFAULT_MAX_CONCURRENT_LEAF_PARSES};
#[cfg(feature = "native")]
pub use prefetch::{PrefetchService, PrefetchConfig, PrefetchRequest, DEFAULT_PREFETCH_WORKERS, DEFAULT_PREFETCH_QUEUE_DEPTH, prefetch_stats_reset};
#[cfg(not(feature = "native"))]
pub use prefetch::{PrefetchService, PrefetchConfig};
pub use db::{Db, EMPTY_NODE_ID};
pub use namespaces::{
    default_namespace_codes, is_owl_inverse_of, is_owl_equivalent_class, is_owl_equivalent_property,
    is_owl_same_as, is_owl_symmetric_property, is_owl_transitive_property,
    is_rdf_first, is_rdf_nil, is_rdf_rest, is_rdf_type,
    is_rdfs_domain, is_rdfs_range, is_rdfs_subclass_of, is_rdfs_subproperty_of,
};
pub use schema_hierarchy::SchemaHierarchy;
pub use stats_view::{PropertyStatData, StatsView};
pub use tracking::{FuelExceededError, PolicyStats, Tracker, TrackingOptions, TrackingTally};
pub use coerce::{coerce_value, coerce_json_value, CoercionError, CoercionResult};
pub use address::{ParsedFlureeAddress, parse_fluree_address, extract_identifier, extract_path};
pub use value_id::{ObjKind, ObjKey, ObjKeyError, DatatypeId};
pub use index_stats::{IndexStats, PropertyStatEntry, ClassStatEntry, ClassPropertyUsage, GraphPropertyStatEntry, GraphStatsEntry};
pub use index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};

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
    pub use crate::storage::{
        ContentAddressedWrite, ContentKind, ContentWriteResult, MemoryStorage, ReadHint, Storage,
        StorageRead, StorageWrite,
    };
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    pub use crate::storage::FileStorage;
}
