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
//! - [`resolver`]: CommitResolver (RawOp → RunRecord)

pub mod global_dict;
pub mod resolver;
pub mod run_file;
pub mod run_record;
pub mod run_writer;

pub mod binary_cursor;
pub mod branch;
pub mod dict_io;
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
pub mod prefix_trie;
pub mod query;
pub mod replay;
pub mod spot_cursor;
pub mod spot_store;
pub mod streaming_reader;

pub use binary_cursor::{BinaryCursor, BinaryFilter, DecodedBatch, OverlayOp};
pub use global_dict::{GlobalDicts, LanguageTagDict, PredicateDict, StringValueDict, SubjectDict};
pub use index_build::{
    build_all_indexes, build_index, build_spot_index, IndexBuildConfig, IndexBuildResult,
};
pub use index_root::{
    BinaryGarbageRef, BinaryIndexRootV2, BinaryPrevIndexRef, DictAddresses, DictTreeAddresses,
    GraphAddresses, GraphEntryV2, GraphOrderAddresses, BINARY_INDEX_ROOT_VERSION_V2,
};
pub use lang_remap::build_lang_remap;
pub use leaflet_cache::{CachedRegion1, CachedRegion2, LeafletCache, LeafletCacheKey};
pub use merge::KWayMerge;
pub use novelty_merge::{merge_novelty, MergeInput, MergeOutput};
pub use prefix_trie::PrefixTrie;
pub use query::SpotQuery;
pub use replay::{replay_leaflet, ReplayedLeaflet};
pub use resolver::{CommitResolver, ResolverError};
pub use run_file::{read_run_file, write_run_file, RunFileInfo};
pub use run_record::{cmp_for_order, cmp_spot, RunRecord, RunSortOrder};
pub use run_writer::{
    MultiOrderConfig, MultiOrderRunWriter, RecordSink, RunWriter, RunWriterConfig, RunWriterResult,
};
pub use spot_cursor::SpotCursor;
pub use spot_store::{BinaryIndexStore, SpotIndexStore};
pub use streaming_reader::StreamingRunReader;

use fluree_db_core::StorageRead;
use fluree_db_novelty::commit_v2::{read_commit_envelope, CommitV2Error};
use std::collections::HashMap;
use std::io;
use std::path::Path;

// ============================================================================
// generate_runs: top-level pipeline
// ============================================================================

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
    #[cfg(feature = "hll-stats")]
    pub stats_hook: Option<crate::stats::IdStatsHook>,
}

/// Errors from the run generation pipeline.
#[derive(Debug)]
pub enum RunGenError {
    Storage(String),
    CommitV2(CommitV2Error),
    Resolver(ResolverError),
    Io(io::Error),
}

impl From<CommitV2Error> for RunGenError {
    fn from(e: CommitV2Error) -> Self {
        Self::CommitV2(e)
    }
}

impl From<ResolverError> for RunGenError {
    fn from(e: ResolverError) -> Self {
        Self::Resolver(e)
    }
}

impl From<io::Error> for RunGenError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for RunGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(msg) => write!(f, "storage: {}", msg),
            Self::CommitV2(e) => write!(f, "commit-v2: {}", e),
            Self::Resolver(e) => write!(f, "resolver: {}", e),
            Self::Io(e) => write!(f, "I/O: {}", e),
        }
    }
}

impl std::error::Error for RunGenError {}

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

    let json_str = serde_json::to_string_pretty(&json_array)
        .map_err(io::Error::other)?;

    let path = run_dir.join("namespaces.json");
    std::fs::write(&path, json_str)?;
    tracing::info!(
        ?path,
        entries = ns_prefixes.len(),
        "namespace map persisted"
    );
    Ok(())
}

/// Walk the binary commit chain from `head_commit_address` back to genesis,
/// resolve all ops to global numeric IDs, and produce sorted SPOT run files.
///
/// # Pipeline
///
/// 1. Walk commit chain backwards (envelope-only reads) to collect addresses.
/// 2. Reverse to forward (chronological) order.
/// 3. For each commit: load full blob → `load_commit_ops` → resolve → emit RunRecords.
/// 4. Flush remaining buffer → return run files + stats.
///
/// The commit blobs are read from storage twice: once for the fast backward
/// envelope scan, once for full resolution. OS page cache handles the
/// common case where blobs fit in memory.
pub async fn generate_runs<S: StorageRead>(
    storage: &S,
    head_commit_address: &str,
    ledger_alias: &str,
    config: RunWriterConfig,
) -> Result<RunGenerationResult, RunGenError> {
    let _span = tracing::info_span!("generate_runs", head = %head_commit_address).entered();

    // Ensure run directory exists
    std::fs::create_dir_all(&config.run_dir)?;

    // ---- Phase 1: Walk backward to collect commit addresses ----
    let addresses = {
        let _walk_span = tracing::info_span!("walk_commit_chain").entered();
        let mut addrs = Vec::new();
        let mut current = Some(head_commit_address.to_string());

        while let Some(addr) = current {
            let bytes = storage
                .read_bytes(&addr)
                .await
                .map_err(|e| RunGenError::Storage(format!("read {}: {}", addr, e)))?;
            let envelope = read_commit_envelope(&bytes)?;
            current = envelope.previous_address().map(String::from);
            addrs.push(addr);
        }

        addrs.reverse(); // chronological order (genesis first)
        tracing::info!(commit_count = addrs.len(), "commit chain traversed");
        addrs
    };

    // ---- Phase 2: Initialize resolver + writer + dicts ----
    let run_dir = config.run_dir.clone();
    let subject_forward_path = run_dir.join("subjects.fwd");
    let mut dicts = GlobalDicts::new(&subject_forward_path)?;
    let mut resolver = CommitResolver::new();
    let mut writer = RunWriter::new(config);

    // ---- Phase 3: Resolve commits in forward order ----
    for (i, addr) in addresses.iter().enumerate() {
        let bytes = storage
            .read_bytes(addr)
            .await
            .map_err(|e| RunGenError::Storage(format!("read {}: {}", addr, e)))?;

        let (op_count, t) =
            resolver.resolve_blob(&bytes, addr, ledger_alias, &mut dicts, &mut writer)?;

        tracing::debug!(
            commit = i + 1,
            t = t,
            ops = op_count,
            subjects = dicts.subjects.len(),
            predicates = dicts.predicates.len(),
            "commit resolved"
        );
    }

    // ---- Phase 4: Finish and return results ----
    let writer_result = writer.finish(&mut dicts.languages)?;

    // Persist dictionaries for Phase C (index build)
    dicts.persist(&run_dir)?;

    // Persist namespace map for query-time IRI encoding
    persist_namespaces(resolver.ns_prefixes(), &run_dir)?;

    // Persist subject reverse hash index for O(log N) IRI → s_id lookup
    dicts
        .subjects
        .write_reverse_index(&run_dir.join("subjects.rev"))?;

    // Persist string reverse hash index for O(log N) string → str_id lookup
    dicts
        .strings
        .write_reverse_index(&run_dir.join("strings.rev"))?;

    let result = RunGenerationResult {
        run_files: writer_result.run_files,
        subject_count: dicts.subjects.len(),
        predicate_count: dicts.predicates.len(),
        string_count: dicts.strings.len(),
        needs_wide: dicts.subjects.needs_wide(),
        total_records: writer_result.total_records,
        commit_count: addresses.len(),
        #[cfg(feature = "hll-stats")]
        stats_hook: resolver.take_stats_hook(),
    };

    tracing::info!(
        run_files = result.run_files.len(),
        total_records = result.total_records,
        subjects = result.subject_count,
        predicates = result.predicate_count,
        strings = result.string_count,
        commits = result.commit_count,
        "run generation complete"
    );

    Ok(result)
}
