//! Binary columnar index formats, codecs, and read-side runtime for Fluree DB.
//!
//! This crate owns the on-disk binary index formats (IRB1, FBR2, FLI2, FPK1,
//! DTB1, DLR1, VAS1, NB1) and the read-side runtime for loading and querying
//! binary indexes. It is the dependency for `fluree-db-query` (instead of
//! depending on the full `fluree-db-indexer` build pipeline).

pub mod error;
pub mod types;

pub mod arena;
pub mod dict;
pub mod format;
pub mod read;

// ── Key read-side types ──────────────────────────────────────────────────────
pub use read::batched_lookup::batched_lookup_predicate_refs;
pub use read::binary_cursor::{BinaryCursor, BinaryFilter, DecodedBatch};
pub use read::binary_index_store::{BinaryGraphView, BinaryIndexStore};
pub use read::leaflet_cache::{CachedRegion1, CachedRegion2, LeafletCache, LeafletCacheKey};
pub use read::query::{FactRow, SpotQuery};
pub use read::replay::{replay_leaflet, ReplayedLeaflet};
pub use read::spot_cursor::SpotCursor;

// ── Format types ─────────────────────────────────────────────────────────────
pub use format::branch::{BranchManifest, LeafEntry};
pub use format::index_root::{
    BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefs, DictRefsV5, DictTreeRefs,
    FulltextArenaRefV5, GraphArenaRefsV5, GraphOrderRefs, GraphRefs, IndexRootV5,
    InlineOrderRouting, NamedGraphRouting, PackBranchEntry, SpatialArenaRefV5, VectorDictRef,
    VectorDictRefV5,
};
pub use format::leaf::read_leaf_header;
pub use format::leaflet::{decode_leaflet_region1, decode_leaflet_region2, LeafletHeader};
pub use format::run_record::{cmp_for_order, cmp_psot, cmp_spot, RunRecord, RunSortOrder};

// ── Arena ────────────────────────────────────────────────────────────────────
pub use arena::fulltext::FulltextArena;

// ── Types ────────────────────────────────────────────────────────────────────
pub use types::{sort_overlay_ops, DecodedRow, NumericShape, OverlayOp, RowColumnSlice};

// ── Dict ─────────────────────────────────────────────────────────────────────
pub use dict::{
    DictBranch, DictTreeReader, ForwardPack, ForwardPackReader, LanguageTagDict, PredicateDict,
    TreeBuildResult,
};
