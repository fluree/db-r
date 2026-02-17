//! Run-based index generation pipeline.
//!
//! This module implements Phase B of `INDEX_BUILD_STRATEGY.md`: walking the
//! binary commit chain, resolving commit-local IDs to global numeric IDs, and
//! producing sorted run files for external-merge index building.
//!
//! ## Sub-modules
//!
//! - [`run_record`]: Fixed-width 48-byte (in-memory) / 44-byte (wire) record type + SPOT comparator
//! - [`global_dict`]: Global dictionaries (subject, predicate, string value)
//! - [`run_writer`]: Memory-bounded buffer + flush to sorted run files
//! - [`run_file`]: Run file binary format (header + lang dict + records)
//! - [`resolver`][]: CommitResolver (RawOp → RunRecord)

pub mod global_dict;
pub mod resolver;
pub mod run_file;
pub mod run_record;
pub mod run_writer;
pub mod spool;

pub mod binary_cursor;
pub mod binary_index_store;
pub mod branch;
pub mod chunk_dict;
pub mod dict_io;
pub mod dict_merge;
pub mod index_build;
pub mod index_root;
pub mod lang_remap;
pub mod leaf;
pub mod leaflet;
pub mod leaflet_cache;
pub mod merge;
pub mod novelty_merge;
pub mod numbig_dict;
pub mod numfloat_dict;
pub mod query;
pub mod replay;
pub mod shared_pool;
pub mod sorted_commit_reader;
pub mod spot_cursor;
pub mod streaming_reader;
pub mod types;
pub mod vector_arena;
pub mod vocab_file;
pub mod vocab_merge;

pub use binary_cursor::{BinaryCursor, BinaryFilter, DecodedBatch};
pub use binary_index_store::BinaryIndexStore;
pub use chunk_dict::{hash_subject, ChunkStringDict, ChunkSubjectDict};
// dict_merge is superseded by vocab_merge (external-sort k-way merge)
// but kept for its unit tests as a reference implementation.
pub use fluree_db_core::PrefixTrie;
pub use global_dict::{
    DictAllocator, DictWorkerCache, GlobalDicts, LanguageTagDict, PredicateDict,
    SharedDictAllocator, StringValueDict, SubjectDict,
};
pub use index_build::{
    build_all_indexes, build_index, build_index_from_run_paths, build_spot_from_sorted_commits,
    build_spot_index, precompute_language_dict, ClassBitsetTable, GraphIndexResult,
    IndexBuildConfig, IndexBuildResult, SortedCommitInput, SpotClassStats, SpotFromCommitsConfig,
    DT_REF_ID,
};
pub use index_root::{
    BinaryGarbageRef, BinaryIndexRoot, BinaryPrevIndexRef, CasArtifactsConfig, DictRefs,
    DictTreeRefs, GraphEntry, GraphOrderRefs, GraphRefs, VectorDictRef, BINARY_INDEX_ROOT_VERSION,
};
pub use lang_remap::{build_lang_remap, build_lang_remap_from_vocabs};
pub use leaflet_cache::{CachedRegion1, CachedRegion2, LeafletCache, LeafletCacheKey};
pub use merge::{KWayMerge, MergeSource};
pub use novelty_merge::{merge_novelty, MergeInput, MergeOutput};
pub use query::SpotQuery;
pub use replay::{replay_leaflet, ReplayedLeaflet};
pub use resolver::{CommitResolver, ResolverError};
pub use run_file::{read_run_file, write_run_file, RunFileInfo};
pub use run_record::{cmp_for_order, cmp_spot, RunRecord, RunSortOrder};
pub use run_writer::{
    MultiOrderConfig, MultiOrderRunWriter, RecordSink, RunWriter, RunWriterConfig, RunWriterResult,
};
pub use shared_pool::{SharedNumBigPool, SharedVectorArenaPool};
pub use sorted_commit_reader::StreamingSortedCommitReader;
pub use spool::{
    collect_chunk_run_files, remap_commit_to_runs, remap_spool_to_runs,
    sort_remap_and_write_sorted_commit, spool_to_runs, SortedCommitInfo, SpoolFileInfo,
    SpoolReader, SpoolWriter, TypesMapConfig,
};
pub use spot_cursor::SpotCursor;
pub use streaming_reader::StreamingRunReader;
pub use types::{sort_overlay_ops, OverlayOp};

use std::collections::HashMap;
use std::io;
use std::path::Path;

/// Result of run generation.
#[derive(Debug)]
pub struct RunGenerationResult {
    /// The run files produced (sorted SPOT runs).
    pub run_files: Vec<RunFileInfo>,
    /// Number of distinct subjects in the global dictionary.
    pub subject_count: u64,
    /// Number of distinct predicates in the global dictionary.
    pub predicate_count: u32,
    /// Number of distinct string values in the global dictionary.
    pub string_count: u32,
    /// Whether any namespace's local subject ID exceeded u16::MAX,
    /// requiring wide (u64) subject ID encoding in leaflets.
    pub needs_wide: bool,
    /// Total RunRecords emitted across all run files.
    pub total_records: u64,
    /// Number of commits processed.
    pub commit_count: usize,
    /// ID-based stats hook accumulated across all resolved commits.
    /// Contains per-(graph, property) HLL sketches and datatype usage.
    /// `None` if stats collection was not enabled.
    pub stats_hook: Option<crate::stats::IdStatsHook>,
    /// Total size of all commit blobs in bytes.
    pub total_commit_size: u64,
    /// Total number of assertions across all commits.
    pub total_asserts: u64,
    /// Total number of retractions across all commits.
    pub total_retracts: u64,
}

/// Persist namespace map to `{run_dir}/namespaces.json`.
///
/// Writes a stable JSON array sorted by code for deterministic ordering:
/// ```json
/// [{"code": 0, "prefix": ""}, {"code": 3, "prefix": "http://..."}]
/// ```
pub fn persist_namespaces(ns_prefixes: &HashMap<u16, String>, run_dir: &Path) -> io::Result<()> {
    let mut entries: Vec<_> = ns_prefixes.iter().collect();
    entries.sort_by_key(|(&code, _)| code);

    let json_array: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|(&code, prefix)| serde_json::json!({ "code": code, "prefix": prefix }))
        .collect();

    let json_str = serde_json::to_string_pretty(&json_array).map_err(io::Error::other)?;

    let path = run_dir.join("namespaces.json");
    std::fs::write(&path, json_str)?;
    tracing::info!(
        ?path,
        entries = ns_prefixes.len(),
        "namespace map persisted"
    );
    Ok(())
}
