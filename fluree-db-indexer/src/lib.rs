//! # Fluree DB Indexer
//!
//! Index building for Fluree DB. This crate provides:
//!
//! - Binary columnar index building (`run_index` module)
//! - Background indexing orchestration
//! - Namespace delta replay
//! - Garbage collection support
//!
//! ## Design
//!
//! The indexer supports two deployment modes:
//!
//! 1. **Embedded**: Background indexing within the main process
//! 2. **External**: Standalone Lambda-style indexer
//!
//! The binary index pipeline (`run_index::index_build`) is the sole indexing path.

pub mod config;
pub mod dict_tree;
pub mod error;
pub mod gc;
pub mod hll;
pub mod orchestrator;
pub mod run_index;
pub mod stats;

// Re-export main types
pub use config::IndexerConfig;
pub use error::{IndexerError, Result};
pub use gc::{
    clean_garbage, load_garbage_record, write_garbage_record, CleanGarbageConfig,
    CleanGarbageResult, GarbageRecord, GarbageRef, DEFAULT_MAX_OLD_INDEXES,
    DEFAULT_MIN_TIME_GARBAGE_MINS,
};
#[cfg(feature = "embedded-orchestrator")]
pub use orchestrator::{
    maybe_refresh_after_commit, require_refresh_before_commit, PostCommitIndexResult,
};
pub use orchestrator::{
    BackgroundIndexerWorker, IndexCompletion, IndexOutcome, IndexPhase, IndexStatusSnapshot,
    IndexerHandle, IndexerOrchestrator,
};
pub use stats::{IndexStatsHook, NoOpStatsHook, StatsArtifacts, StatsSummary};

// Note: The following types/functions are defined in this module and are automatically public:
// - build_index_for_ledger (nameservice-aware entry point)
// - build_binary_index (direct entry point given an NsRecord)
// - CURRENT_INDEX_VERSION

use fluree_db_core::{ContentId, ContentKind, ContentStore, ContentWriteResult, Storage};
use fluree_db_nameservice::{NameService, Publisher};

/// Derive a `ContentId` from a `ContentWriteResult`.
///
/// Every `content_write_bytes{,_with_hash}` call returns a SHA-256 hex digest.
/// This helper wraps `ContentId::from_hex_digest` so callers don't repeat
/// the pattern.
fn cid_from_write(kind: ContentKind, result: &ContentWriteResult) -> ContentId {
    ContentId::from_hex_digest(kind.to_codec(), &result.content_hash)
        .expect("storage produced a valid SHA-256 hex digest")
}

/// Normalize a ledger ID for comparison purposes
///
/// Handles both canonical `name:branch` format and storage-path style `name/branch`.
/// This is necessary because the address may be stored in either format depending
/// on the code path that created it.
///
/// # Algorithm
///
/// 1. If the address contains `:`, use canonical parsing via `core_alias::normalize_alias`
///    - If parsing fails, return the original string unchanged (don't manufacture addresses)
/// 2. If the address has exactly one `/` (storage-path style), convert to `name:branch`
/// 3. Falls back to treating the whole string as the name with default branch
///
/// This ONLY treats a single `/` as a branch separator when there's no `:`.
/// For ledger names that legitimately contain `/` (e.g., "org/project:main"),
/// the canonical format with `:` must be used.
#[cfg(test)]
fn normalize_id_for_comparison(ledger_id: &str) -> String {
    use fluree_db_core::alias as core_alias;

    // If it has a colon, use canonical parsing
    if ledger_id.contains(':') {
        // If canonical parse succeeds, use it. If it fails (malformed address),
        // return the original string unchanged rather than manufacturing a new address.
        return core_alias::normalize_alias(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
    }

    // Check for storage-path style "name/branch" (exactly one slash, no colon)
    let slash_count = ledger_id.chars().filter(|c| *c == '/').count();
    if slash_count == 1 {
        if let Some(slash_idx) = ledger_id.rfind('/') {
            let name = &ledger_id[..slash_idx];
            let branch = &ledger_id[slash_idx + 1..];
            if !name.is_empty() && !branch.is_empty() {
                return core_alias::format_alias(name, branch);
            }
        }
    }

    // Last resort: treat entire string as name with default branch
    core_alias::format_alias(ledger_id, core_alias::DEFAULT_BRANCH)
}

/// Result of building an index
#[derive(Debug, Clone)]
pub struct IndexResult {
    /// Storage address of the index root
    pub root_address: String,
    /// Content identifier of the index root (derived from SHA-256 of root bytes).
    ///
    /// Always present — derived from the content hash of the index root during build,
    /// or from the persisted CID / address hash during early-return.
    pub root_id: fluree_db_core::ContentId,
    /// Transaction time the index is current through
    pub index_t: i64,
    /// Ledger ID (name:branch format)
    pub ledger_id: String,
    /// Index build statistics
    pub stats: IndexStats,
}

/// Statistics from index building
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total number of flakes in the index (after dedup)
    pub flake_count: usize,
    /// Number of leaf nodes created
    pub leaf_count: usize,
    /// Number of branch nodes created
    pub branch_count: usize,
    /// Total bytes written
    pub total_bytes: usize,
}

/// Current index version for compatibility checking
pub const CURRENT_INDEX_VERSION: i32 = 2;

/// External indexer entry point
///
/// Builds a binary columnar index from the commit chain. The pipeline:
/// 1. Walks the commit chain and generates sorted run files
/// 2. Builds per-graph leaf/branch indexes for all sort orders
/// 3. Creates a `BinaryIndexRoot` descriptor and writes it to storage
///
/// Returns early if the index is already current (no work needed).
/// Use `build_binary_index` directly to force a rebuild regardless.
pub async fn build_index_for_ledger<S, N>(
    storage: &S,
    nameservice: &N,
    ledger_id: &str,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    let span = tracing::info_span!("index_build", ledger_id = ledger_id);
    let _guard = span.enter();

    // Look up the ledger record
    let record = nameservice
        .lookup(ledger_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))?
        .ok_or_else(|| IndexerError::LedgerNotFound(ledger_id.to_string()))?;

    // If index is already current, return it
    if let Some(ref root_id) = record.index_head_id {
        if record.index_t >= record.commit_t {
            // Derive the storage address for callers that need it
            let root_address = fluree_db_core::storage::content_address(
                storage.storage_method(),
                fluree_db_core::ContentKind::IndexRoot,
                ledger_id,
                &root_id.digest_hex(),
            );
            return Ok(IndexResult {
                root_address,
                root_id: root_id.clone(),
                index_t: record.index_t,
                ledger_id: ledger_id.to_string(),
                stats: IndexStats::default(),
            });
        }
    }

    build_binary_index(storage, ledger_id, &record, config).await
}

/// Build a binary index from an existing nameservice record.
///
/// Unlike `build_index_for_ledger`, this skips the nameservice lookup and
/// the "already current" early-return check. Use this when you already have
/// the `NsRecord` and want to force a rebuild (e.g., `reindex`).
///
/// Runs the entire pipeline on a blocking thread via `spawn_blocking` +
/// `handle.block_on()` because internal dictionaries contain non-Send types
/// held across await points.
///
/// Pipeline:
/// 1. Walk commit chain backward to collect addresses
/// 2. Resolve commits in forward order using `MultiOrderRunWriter` to produce
///    per-order sorted run files (SPOT, PSOT, POST, OPST)
/// 3. Build per-graph leaf/branch indexes from run files
/// 4. Write `BinaryIndexRoot` descriptor to storage
pub async fn build_binary_index<S>(
    storage: &S,
    ledger_id: &str,
    record: &fluree_db_nameservice::NsRecord,
    config: IndexerConfig,
) -> Result<IndexResult>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_novelty::commit_v2::read_commit_envelope;

    let head_commit_id = record
        .commit_head_id
        .clone()
        .ok_or(IndexerError::NoCommits)?;

    // Determine output directory for binary index artifacts
    let data_dir = config
        .data_dir
        .unwrap_or_else(|| std::env::temp_dir().join("fluree-index"));
    let ledger_id_path = fluree_db_core::address_path::alias_to_path_prefix(ledger_id)
        .unwrap_or_else(|_| ledger_id.replace(':', "/"));
    let session_id = uuid::Uuid::new_v4().to_string();
    let run_dir = data_dir
        .join(&ledger_id_path)
        .join("tmp_import")
        .join(&session_id);
    let index_dir = data_dir.join(&ledger_id_path).join("index");

    tracing::info!(
        %head_commit_id,
        ?run_dir,
        ?index_dir,
        "starting binary index build"
    );

    // Capture values for the blocking task
    let storage = storage.clone();
    let ledger_id = ledger_id.to_string();
    let prev_root_id = record.index_head_id.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        handle.block_on(async {
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Build a content store bridge for CID → address resolution
            let content_store = fluree_db_core::storage::content_store_for(storage.clone(), &ledger_id);

            // ---- Phase A: Walk commit chain backward to collect CIDs ----
            let commit_cids = {
                let mut cids = Vec::new();
                let mut current = Some(head_commit_id.clone());

                while let Some(cid) = current {
                    let bytes = content_store
                        .get(&cid)
                        .await
                        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;
                    let envelope = read_commit_envelope(&bytes)
                        .map_err(|e| IndexerError::StorageRead(e.to_string()))?;
                    current = envelope.previous_id().cloned();
                    cids.push(cid);
                }

                cids.reverse(); // chronological order (genesis first)
                cids
            };

            // ---- Phase B: Resolve commits with multi-order run writer ----
            let mut dicts = run_index::GlobalDicts::new(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            let mut resolver = run_index::CommitResolver::new();

            // Pre-insert rdf:type into predicate dictionary so class tracking
            // works from the very first commit.
            let rdf_type_p_id = dicts.predicates.get_or_insert(fluree_vocab::rdf::TYPE);
            let mut stats_hook = crate::stats::IdStatsHook::new();
            stats_hook.set_rdf_type_p_id(rdf_type_p_id);
            resolver.set_stats_hook(stats_hook);

            let multi_config = run_index::MultiOrderConfig {
                total_budget_bytes: config.run_budget_bytes,
                orders: run_index::RunSortOrder::all_build_orders().to_vec(),
                base_run_dir: run_dir.clone(),
            };
            let mut writer = run_index::MultiOrderRunWriter::new(multi_config)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Accumulate commit statistics for index root
            let mut total_commit_size = 0u64;
            let mut total_asserts = 0u64;
            let mut total_retracts = 0u64;

            for (i, cid) in commit_cids.iter().enumerate() {
                let bytes = content_store
                    .get(cid)
                    .await
                    .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", cid, e)))?;

                let resolved = resolver
                    .resolve_blob(&bytes, &cid.digest_hex(), &mut dicts, &mut writer)
                    .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

                // Accumulate totals
                total_commit_size += resolved.size;
                total_asserts += resolved.asserts as u64;
                total_retracts += resolved.retracts as u64;

                tracing::debug!(
                    commit = i + 1,
                    t = resolved.t,
                    ops = resolved.total_records,
                    subjects = dicts.subjects.len(),
                    predicates = dicts.predicates.len(),
                    "commit resolved"
                );
            }

            let id_stats_hook = resolver.take_stats_hook();

            let total_records = writer.total_records();
            let _writer_results = writer
                .finish(&mut dicts.languages)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist dictionaries for index build
            dicts
                .persist(&run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist namespace map for query-time IRI encoding
            run_index::persist_namespaces(resolver.ns_prefixes(), &run_dir)
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Persist reverse hash indexes
            dicts
                .subjects
                .write_reverse_index(&run_dir.join("subjects.rev"))
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            dicts
                .strings
                .write_reverse_index(&run_dir.join("strings.rev"))
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // ---- Phase C: Build per-graph indexes for all sort orders ----
            let build_results = run_index::build_all_indexes(
                &run_dir,
                &index_dir,
                run_index::RunSortOrder::all_build_orders(),
                25_000, // leaflet_rows
                10,     // leaflets_per_leaf
                1,      // zstd_level
            )
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // ---- Phase D: Upload artifacts to CAS and write v2 root ----

            // D.1: Load store for max_t / base_t / namespace_codes
            let store = run_index::BinaryIndexStore::load(&run_dir, &index_dir)
                .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

            // Build predicate p_id -> (ns_code, suffix) mapping for the root (compact).
            let predicate_sids: Vec<(u16, String)> = (0..dicts.predicates.len())
                .map(|p_id| {
                    let iri = dicts.predicates.resolve(p_id).unwrap_or("");
                    let sid = store.encode_iri(iri);
                    (sid.namespace_code, sid.name.as_ref().to_string())
                })
                .collect();

            // D.2: Upload dictionary artifacts to CAS
            let numbig_p_ids: Vec<u32> = dicts.numbigs.keys().copied().collect();
            let dict_addresses = upload_dicts_to_cas(
                &storage,
                &ledger_id,
                &run_dir,
                &dicts,
                &numbig_p_ids,
                store.namespace_codes(),
            )
            .await?;

            // D.3: Upload index artifacts (branches + leaves) to CAS
            let graph_addresses =
                upload_indexes_to_cas(&storage, &ledger_id, &build_results).await?;

            // D.4: Build stats JSON for the planner (RawDbRootStats shape).
            //
            // Preferred: ID-based stats collected during commit resolution (per-graph property
            // stats with datatype counts + HLL NDV). Fallback: SPOT build result for per-graph
            // flake counts only.
            let stats_json = {
                if let Some(hook) = id_stats_hook {
                    let (id_result, agg_props, class_counts, class_properties, class_ref_targets) =
                        hook.finalize_with_aggregate_properties();

                    let graphs_json: Vec<serde_json::Value> = id_result
                        .graphs
                        .iter()
                        .map(|g| {
                            let props_json: Vec<serde_json::Value> = g
                                .properties
                                .iter()
                                .map(|p| {
                                    serde_json::json!({
                                        "p_id": p.p_id,
                                        "count": p.count,
                                        "ndv_values": p.ndv_values,
                                        "ndv_subjects": p.ndv_subjects,
                                        "last_modified_t": p.last_modified_t,
                                        "datatypes": p.datatypes,
                                    })
                                })
                                .collect();

                            serde_json::json!({
                                "g_id": g.g_id,
                                "flakes": g.flakes,
                                "size": g.size,
                                "properties": props_json,
                            })
                        })
                        .collect();

                    // Build top-level properties array with SID keys (for the planner).
                    // Each entry: [[ns_code, "suffix"], count, ndv_values, ndv_subjects, last_modified_t, datatypes]
                    let properties_json: Vec<serde_json::Value> = agg_props
                        .iter()
                        .filter_map(|p| {
                            let sid = predicate_sids.get(p.p_id as usize)?;
                            Some(serde_json::json!({
                                "sid": [sid.0, sid.1],
                                "count": p.count,
                                "ndv_values": p.ndv_values,
                                "ndv_subjects": p.ndv_subjects,
                                "last_modified_t": p.last_modified_t,
                                "datatypes": p.datatypes,
                            }))
                        })
                        .collect();

                    // Build classes array with SID keys.
                    // class sid64 -> (ns_code, suffix) via subject IRI resolution.
                    // Format: [[ns_code, "suffix"], [count, [prop_usages...]]]
                    let classes_json: Vec<serde_json::Value> = class_counts
                        .iter()
                        .filter_map(|&(class_sid64, count)| {
                            let iri = store.resolve_subject_iri(class_sid64).ok()?;
                            let sid = store.encode_iri(&iri);

                            // Build property usages for this class
                            let prop_usages: Vec<serde_json::Value> = class_properties
                                .get(&class_sid64)
                                .map(|props| {
                                    let mut sorted: Vec<u32> = props.iter().copied().collect();
                                    sorted.sort();
                                    sorted
                                        .iter()
                                        .filter_map(|&pid| {
                                            let psid = predicate_sids.get(pid as usize)?;
                                            // Optional ref target class counts for this (class, property) pair.
                                            //
                                            // Raw format: `[[property_sid], {"ref-classes": [[[class_sid], count], ...]}]`
                                            let refs_obj = class_ref_targets
                                                .get(&class_sid64)
                                                .and_then(|m| m.get(&pid))
                                                .and_then(|targets| {
                                                    let mut entries: Vec<(u64, i64)> =
                                                        targets.iter().map(|(&c, &d)| (c, d)).collect();
                                                    entries.sort_by_key(|(c, _)| *c);
                                                    let ref_arr: Vec<serde_json::Value> = entries
                                                        .into_iter()
                                                        .filter_map(|(target_sid64, d)| {
                                                            let c = d.max(0) as u64;
                                                            if c == 0 {
                                                                return None;
                                                            }
                                                            let iri =
                                                                store.resolve_subject_iri(target_sid64).ok()?;
                                                            let sid = store.encode_iri(&iri);
                                                            Some(serde_json::json!([
                                                                [sid.namespace_code, sid.name.as_ref()],
                                                                c
                                                            ]))
                                                        })
                                                        .collect();
                                                    if ref_arr.is_empty() {
                                                        None
                                                    } else {
                                                        Some(serde_json::json!({ "ref-classes": ref_arr }))
                                                    }
                                                });

                                            match refs_obj {
                                                Some(obj) => Some(serde_json::json!([[psid.0, psid.1], obj])),
                                                None => Some(serde_json::json!([[psid.0, psid.1]])),
                                            }
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();

                            Some(serde_json::json!([
                                [sid.namespace_code, sid.name.as_ref()],
                                [count, prop_usages],
                            ]))
                        })
                        .collect();

                    serde_json::json!({
                        "flakes": id_result.total_flakes,
                        "size": 0,
                        "graphs": graphs_json,
                        "properties": properties_json,
                        "classes": classes_json,
                    })
                } else {
                    // Fallback: flake counts only (no per-property / datatype breakdown).
                    let (_, spot_result) = build_results
                        .iter()
                        .find(|(order, _)| *order == run_index::RunSortOrder::Spot)
                        .expect("SPOT index must always be present in build results");

                    let graph_stats: Vec<serde_json::Value> = spot_result
                        .graphs
                        .iter()
                        .map(|g| {
                            serde_json::json!({
                                "g_id": g.g_id,
                                "flakes": g.total_rows,
                                "size": 0
                            })
                        })
                        .collect();

                    let total_flakes: u64 = spot_result.graphs.iter().map(|g| g.total_rows).sum();

                    serde_json::json!({
                        "flakes": total_flakes,
                        "size": 0,
                        "graphs": graph_stats
                    })
                }
            };

            // D.5: Build v2 root with CAS addresses and stats (initially without GC fields)
            let sid_encoding = if dicts.subjects.needs_wide() {
                fluree_db_core::SubjectIdEncoding::Wide
            } else {
                fluree_db_core::SubjectIdEncoding::Narrow
            };
            let subject_watermarks = dicts.subjects.subject_watermarks();
            let string_watermark = dicts.strings.len().saturating_sub(1);

            let mut root =
                run_index::BinaryIndexRoot::from_cas_artifacts(run_index::CasArtifactsConfig {
                    ledger_id: &ledger_id,
                    index_t: store.max_t(),
                    base_t: store.base_t(),
                    predicate_sids,
                    namespace_codes: store.namespace_codes(),
                    subject_id_encoding: sid_encoding,
                    dict_refs: dict_addresses,
                    graph_refs: graph_addresses,
                    stats: Some(stats_json),
                    schema: None,     // schema: requires predicate definitions (future)
                    prev_index: None, // set below after garbage computation
                    garbage: None,    // set below after garbage computation
                    subject_watermarks,
                    string_watermark,
                });

            // Populate cumulative commit stats (kept out of CasArtifactsConfig for now).
            root.total_commit_size = total_commit_size;
            root.total_asserts = total_asserts;
            root.total_retracts = total_retracts;

            // D.5.1: Compute garbage and link prev_index for GC chain.
            //
            // Strategy: use all_cas_ids() on both old and new roots to
            // compute the set difference. This guarantees both sides use the
            // same enumeration method, eliminating divergence risk.
            //
            // Note: The GC chain breaks at the v2→v3 boundary because v2 roots
            // won't parse via `from_json_bytes` (version check). New v3 chains
            // start fresh. The GC collector needs updating for v3 format (future work).
            if let Some(prev_id) = prev_root_id.as_ref() {
                // Try to load the previous root as a v3 binary root.
                // If it's a v2 or legacy format, skip GC linking — mixed-format
                // chains will start a fresh GC chain from this root.
                let prev_bytes = content_store.get(prev_id).await.ok();
                let prev_root = prev_bytes
                    .as_deref()
                    .and_then(|b| run_index::BinaryIndexRoot::from_json_bytes(b).ok());

                if let Some(prev) = prev_root {
                    let old_ids: std::collections::HashSet<ContentId> =
                        prev.all_cas_ids().into_iter().collect();
                    let new_ids: std::collections::HashSet<ContentId> =
                        root.all_cas_ids().into_iter().collect();
                    let garbage_cids: Vec<ContentId> =
                        old_ids.difference(&new_ids).cloned().collect();

                    let garbage_count = garbage_cids.len();

                    // Write garbage record with CID string representations.
                    let garbage_strings: Vec<String> =
                        garbage_cids.iter().map(|c| c.to_string()).collect();
                    root.garbage =
                        gc::write_garbage_record(&storage, &ledger_id, store.max_t(), garbage_strings)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                            .and_then(|r| {
                                ContentId::from_hex_digest(
                                    fluree_db_core::CODEC_FLUREE_GARBAGE,
                                    &r.content_hash,
                                )
                                .map(|id| run_index::BinaryGarbageRef { id })
                            });

                    root.prev_index = Some(run_index::BinaryPrevIndexRef {
                        t: prev.index_t,
                        id: prev_id.clone(),
                    });

                    tracing::info!(
                        prev_t = prev.index_t,
                        garbage_count,
                        "GC chain linked to previous binary index root"
                    );
                }
            }

            tracing::info!(
                index_t = root.index_t,
                base_t = root.base_t,
                graphs = root.graphs.len(),
                "binary index built (v2), writing CAS root"
            );

            // D.6: Write root to CAS (auto-hash of canonical compact JSON)
            let root_bytes = root
                .to_json_bytes()
                .map_err(|e| IndexerError::Serialization(e.to_string()))?;
            let write_result = storage
                .content_write_bytes(fluree_db_core::ContentKind::IndexRoot, &ledger_id, &root_bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Clean up ephemeral tmp_import session directory
            if let Err(e) = std::fs::remove_dir_all(&run_dir) {
                tracing::warn!(?run_dir, %e, "failed to clean up tmp_import session dir");
            }

            // Compute stats from build results
            let total_leaves: usize = build_results
                .iter()
                .flat_map(|(_, r)| r.graphs.iter())
                .map(|g| g.leaf_count as usize)
                .sum();

            // Derive ContentId from the root's content hash
            let root_id = fluree_db_core::ContentId::from_hex_digest(
                fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
                &write_result.content_hash,
            )
            .ok_or_else(|| {
                IndexerError::StorageWrite(format!(
                    "invalid content_hash from write result: {}",
                    write_result.content_hash
                ))
            })?;

            Ok(IndexResult {
                root_address: write_result.address,
                root_id,
                index_t: root.index_t,
                ledger_id: ledger_id.to_string(),
                stats: IndexStats {
                    flake_count: total_records as usize,
                    leaf_count: total_leaves,
                    branch_count: build_results.len(),
                    total_bytes: root_bytes.len(),
                },
            })
        })
    })
    .await
    .map_err(|e| IndexerError::StorageWrite(format!("index build task panicked: {}", e)))?
}

/// Build dictionary CoW trees and upload all dictionary artifacts to CAS.
///
/// Small-cardinality dictionaries (graphs, datatypes, languages) are uploaded
/// as flat blobs. Subject and string dictionaries are built into CoW trees
/// (branch + leaves) for O(log n) lookup at query time.
async fn upload_dicts_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    run_dir: &std::path::Path,
    dicts: &run_index::GlobalDicts,
    numbig_p_ids: &[u32],
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<run_index::DictRefs> {
    use dict_tree::builder::{self, TreeBuildResult};
    use dict_tree::forward_leaf::ForwardEntry;
    use dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind};
    use std::collections::BTreeMap;

    /// Read a file and upload to CAS, returning write result + derived CID.
    async fn upload_flat<S: Storage>(
        storage: &S,
        ledger_id: &str,
        path: &std::path::Path,
        dict: DictKind,
    ) -> Result<(ContentId, ContentWriteResult)> {
        let kind = ContentKind::DictBlob { dict };
        let bytes = std::fs::read(path)
            .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
        let result = storage
            .content_write_bytes(kind, ledger_id, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        tracing::debug!(
            path = %path.display(),
            address = %result.address,
            bytes = result.size_bytes,
            "dict artifact uploaded to CAS"
        );
        let cid = cid_from_write(kind, &result);
        Ok((cid, result))
    }

    /// Upload a tree build result (branch + leaves) to CAS, returning CID refs.
    async fn upload_tree<S: Storage>(
        storage: &S,
        ledger_id: &str,
        result: TreeBuildResult,
        dict: DictKind,
    ) -> Result<run_index::DictTreeRefs> {
        let kind = ContentKind::DictBlob { dict };
        let mut leaf_cids = Vec::with_capacity(result.leaves.len());
        let mut hash_to_address = std::collections::HashMap::new();

        for leaf in &result.leaves {
            let cas_result = storage
                .content_write_bytes(kind, ledger_id, &leaf.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            leaf_cids.push(cid_from_write(kind, &cas_result));
            hash_to_address.insert(leaf.hash.clone(), cas_result.address);
        }

        // Finalize branch with actual CAS addresses, then upload
        let (_, branch_bytes, _) = builder::finalize_branch(result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let branch_result = storage
            .content_write_bytes(kind, ledger_id, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        Ok(run_index::DictTreeRefs {
            branch: cid_from_write(kind, &branch_result),
            leaves: leaf_cids,
        })
    }

    // Small flat dicts
    let (graphs, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("graphs.dict"),
        DictKind::Graphs,
    )
    .await?;
    let (datatypes, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("datatypes.dict"),
        DictKind::Datatypes,
    )
    .await?;
    let (languages, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("languages.dict"),
        DictKind::Languages,
    )
    .await?;

    // Subject trees – strip namespace prefix so trees store suffix-only values,
    // matching the format expected by translate_range / find_subject_id lookups.
    let subject_pairs = dicts
        .subjects
        .read_all_entries()
        .map_err(|e| IndexerError::StorageRead(format!("read subject entries: {}", e)))?;

    let subject_suffix = |sid: u64, iri: &[u8]| -> (u16, Vec<u8>) {
        let iri_str = std::str::from_utf8(iri).unwrap_or("");
        let ns_code = SubjectId::from_u64(sid).ns_code();
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        let suffix = if iri_str.starts_with(prefix) && !prefix.is_empty() {
            &iri[prefix.len()..]
        } else {
            iri
        };
        (ns_code, suffix.to_vec())
    };

    let mut subj_fwd: Vec<ForwardEntry> = subject_pairs
        .iter()
        .map(|(sid, iri)| {
            let (_ns, suffix) = subject_suffix(*sid, iri);
            ForwardEntry {
                id: *sid,
                value: suffix,
            }
        })
        .collect();
    subj_fwd.sort_unstable_by_key(|e| e.id);

    let mut subj_rev: Vec<ReverseEntry> = subject_pairs
        .iter()
        .map(|(sid, iri)| {
            let (ns_code, suffix) = subject_suffix(*sid, iri);
            ReverseEntry {
                key: subject_reverse_key(ns_code, &suffix),
                id: *sid,
            }
        })
        .collect();
    subj_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let sf_tree = builder::build_forward_tree(subj_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject fwd tree: {}", e)))?;
    let sr_tree = builder::build_reverse_tree(subj_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build subject rev tree: {}", e)))?;

    let subject_forward =
        upload_tree(storage, ledger_id, sf_tree, DictKind::SubjectForward).await?;
    let subject_reverse =
        upload_tree(storage, ledger_id, sr_tree, DictKind::SubjectReverse).await?;

    // String trees (read from file-backed forward file)
    let string_pairs = dicts
        .strings
        .all_entries()
        .map_err(|e| IndexerError::StorageWrite(format!("read string entries: {}", e)))?;

    let mut str_fwd: Vec<ForwardEntry> = string_pairs
        .iter()
        .map(|(id, val)| ForwardEntry {
            id: *id,
            value: val.clone(),
        })
        .collect();
    str_fwd.sort_unstable_by_key(|e| e.id);

    let mut str_rev: Vec<ReverseEntry> = string_pairs
        .into_iter()
        .map(|(id, val)| ReverseEntry { key: val, id })
        .collect();
    str_rev.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    let stf_tree = builder::build_forward_tree(str_fwd, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string fwd tree: {}", e)))?;
    let str_tree = builder::build_reverse_tree(str_rev, builder::DEFAULT_TARGET_LEAF_BYTES)
        .map_err(|e| IndexerError::StorageWrite(format!("build string rev tree: {}", e)))?;

    let string_forward = upload_tree(storage, ledger_id, stf_tree, DictKind::StringForward).await?;
    let string_reverse = upload_tree(storage, ledger_id, str_tree, DictKind::StringReverse).await?;

    // Per-predicate numbig arenas
    let mut numbig = BTreeMap::new();
    let nb_dir = run_dir.join("numbig");
    for &p_id in numbig_p_ids {
        let path = nb_dir.join(format!("p_{}.nba", p_id));
        if path.exists() {
            let (cid, _) =
                upload_flat(storage, ledger_id, &path, DictKind::NumBig { p_id }).await?;
            numbig.insert(p_id.to_string(), cid);
        }
    }

    // Per-predicate vector arena shards + manifests
    let mut vectors = BTreeMap::new();
    let vec_dir = run_dir.join("vectors");
    if vec_dir.exists() {
        for (&p_id, arena) in &dicts.vectors {
            if arena.is_empty() {
                continue;
            }
            // Upload each shard file
            let cap = run_index::vector_arena::SHARD_CAPACITY as usize;
            let total = arena.len() as usize;
            let num_shards = total.div_ceil(cap);
            let mut shard_cids = Vec::with_capacity(num_shards);
            let mut shard_infos = Vec::with_capacity(num_shards);

            for shard_idx in 0..num_shards {
                let shard_path = vec_dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
                let (shard_cid, shard_wr) = upload_flat(
                    storage,
                    ledger_id,
                    &shard_path,
                    DictKind::VectorShard { p_id },
                )
                .await?;
                let start_vec = shard_idx * cap;
                let count = (total - start_vec).min(cap) as u32;
                shard_infos.push(run_index::vector_arena::ShardInfo {
                    cas: shard_wr.address,
                    count,
                });
                shard_cids.push(shard_cid);
            }

            // Write final manifest with real CAS addresses, then upload
            let manifest_path = vec_dir.join(format!("p_{}.vam", p_id));
            run_index::vector_arena::write_vector_manifest(&manifest_path, arena, &shard_infos)
                .map_err(|e| IndexerError::StorageWrite(format!("write vector manifest: {}", e)))?;
            let (manifest_cid, _) = upload_flat(
                storage,
                ledger_id,
                &manifest_path,
                DictKind::VectorManifest { p_id },
            )
            .await?;

            vectors.insert(
                p_id.to_string(),
                run_index::VectorDictRef {
                    manifest: manifest_cid,
                    shards: shard_cids,
                },
            );
        }
    }

    tracing::info!(
        subject_count = dicts.subjects.len(),
        string_count = dicts.strings.len(),
        numbig_count = numbig.len(),
        vector_count = vectors.len(),
        "dictionary trees built and uploaded to CAS"
    );

    Ok(run_index::DictRefs {
        graphs,
        datatypes,
        languages,
        subject_forward,
        subject_reverse,
        string_forward,
        string_reverse,
        numbig,
        vectors,
    })
}

/// Upload all index artifacts (branches + leaves) to CAS.
///
/// Reads each `.fbr` and `.fli` file from the index directory, uploads via
/// `content_write_bytes_with_hash` (reusing the existing SHA-256 hashes from
/// the build), and returns per-graph CAS addresses.
pub async fn upload_indexes_to_cas<S: Storage>(
    storage: &S,
    ledger_id: &str,
    build_results: &[(run_index::RunSortOrder, run_index::IndexBuildResult)],
) -> Result<Vec<run_index::GraphRefs>> {
    use fluree_db_core::ContentKind;
    use std::collections::BTreeMap;

    let mut graph_map: BTreeMap<u32, BTreeMap<String, run_index::GraphOrderRefs>> = BTreeMap::new();

    let total_order_count = build_results.len();
    for (order_idx, (order, result)) in build_results.iter().enumerate() {
        let order_name = order.dir_name().to_string();
        let order_graphs = result.graphs.len();
        let order_total_leaves: usize = result.graphs.iter().map(|g| g.leaf_count as usize).sum();
        tracing::info!(
            order = %order_name.to_uppercase(),
            graphs = order_graphs,
            leaves = order_total_leaves,
            progress = format!("{}/{}", order_idx + 1, total_order_count),
            "uploading index order to CAS"
        );

        for graph_result in &result.graphs {
            let g_id = graph_result.g_id;
            let graph_dir = &graph_result.graph_dir;

            // Upload branch manifest
            let branch_path = graph_dir.join(format!("{}.fbr", graph_result.branch_hash));
            let branch_bytes = std::fs::read(&branch_path).map_err(|e| {
                IndexerError::StorageRead(format!("read branch {}: {}", branch_path.display(), e))
            })?;
            let branch_write = storage
                .content_write_bytes_with_hash(
                    ContentKind::IndexBranch,
                    ledger_id,
                    &graph_result.branch_hash,
                    &branch_bytes,
                )
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

            // Parse branch manifest to discover leaf files
            let branch_manifest =
                run_index::branch::read_branch_manifest(&branch_path).map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read branch manifest {}: {}",
                        branch_path.display(),
                        e
                    ))
                })?;

            // Upload each leaf file
            let total_leaves = branch_manifest.leaves.len();
            let mut leaf_cids = Vec::with_capacity(total_leaves);
            for (leaf_idx, leaf_entry) in branch_manifest.leaves.iter().enumerate() {
                let leaf_bytes = std::fs::read(&leaf_entry.path).map_err(|e| {
                    IndexerError::StorageRead(format!(
                        "read leaf {}: {}",
                        leaf_entry.path.display(),
                        e
                    ))
                })?;
                let leaf_write = storage
                    .content_write_bytes_with_hash(
                        ContentKind::IndexLeaf,
                        ledger_id,
                        &leaf_entry.content_hash,
                        &leaf_bytes,
                    )
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                leaf_cids.push(cid_from_write(ContentKind::IndexLeaf, &leaf_write));

                if total_leaves >= 100 && (leaf_idx + 1) % 500 == 0 {
                    tracing::info!(
                        order = %order_name,
                        g_id,
                        leaf = leaf_idx + 1,
                        total_leaves,
                        "leaf upload progress"
                    );
                }
            }

            let branch_cid = cid_from_write(ContentKind::IndexBranch, &branch_write);
            tracing::info!(
                g_id,
                order = %order_name,
                leaves = leaf_cids.len(),
                branch = %branch_cid,
                "graph/order index uploaded to CAS"
            );

            graph_map.entry(g_id).or_default().insert(
                order_name.clone(),
                run_index::GraphOrderRefs {
                    branch: branch_cid,
                    leaves: leaf_cids,
                },
            );
        }
    }

    let graph_addresses: Vec<run_index::GraphRefs> = graph_map
        .into_iter()
        .map(|(g_id, orders)| run_index::GraphRefs { g_id, orders })
        .collect();

    let total_artifacts: usize = graph_addresses
        .iter()
        .flat_map(|ga| ga.orders.values())
        .map(|oa| 1 + oa.leaves.len())
        .sum();
    tracing::info!(
        graphs = graph_addresses.len(),
        total_artifacts,
        "index artifacts uploaded to CAS"
    );

    Ok(graph_addresses)
}

/// Result of uploading persisted dict flat files to CAS.
///
/// Contains the CAS addresses for all dictionary artifacts plus derived metadata
/// needed for `BinaryIndexRoot::from_cas_artifacts`.
#[derive(Debug)]
pub struct UploadedDicts {
    pub dict_refs: run_index::DictRefs,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,
}

/// Upload dictionary artifacts from persisted flat files to CAS.
///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
///
/// Required files in `run_dir`:
///   - `subjects.fwd`, `subjects.idx`, `subjects.sids`
///   - `strings.fwd`, `strings.idx`
///   - `graphs.dict`, `datatypes.dict`, `languages.dict`
///   - `numbig/p_*.nba` (zero or more)
///
/// Watermark derivation from `subjects.sids`:
///   - Decode each sid64 via `SubjectId::from_u64` → `(ns_code, local_id)`
///   - `subject_watermarks[ns_code]` = max local_id for that ns_code
///   - Overflow ns_code (0xFFFF): always wide, watermark = 0
///   - `needs_wide` = any local_id exceeds `u16::MAX`
///   - `string_watermark` = string entry count − 1 (IDs are 0..=N contiguous)
pub async fn upload_dicts_from_disk<S: Storage>(
    storage: &S,
    ledger_id: &str,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> Result<UploadedDicts> {
    use dict_tree::builder::{self, TreeBuildResult};
    use dict_tree::forward_leaf::ForwardEntry;
    use dict_tree::reverse_leaf::{subject_reverse_key, ReverseEntry};
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind, SubjectIdEncoding};
    use std::collections::BTreeMap;

    // ---- Inner helpers (same pattern as upload_dicts_to_cas) ----

    async fn upload_flat<S: Storage>(
        storage: &S,
        ledger_id: &str,
        path: &std::path::Path,
        dict: DictKind,
    ) -> Result<(ContentId, ContentWriteResult)> {
        let kind = ContentKind::DictBlob { dict };
        let bytes = std::fs::read(path)
            .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", path.display(), e)))?;
        let result = storage
            .content_write_bytes(kind, ledger_id, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        tracing::debug!(
            path = %path.display(),
            address = %result.address,
            bytes = result.size_bytes,
            "dict artifact uploaded to CAS (from disk)"
        );
        let cid = cid_from_write(kind, &result);
        Ok((cid, result))
    }

    async fn upload_tree<S: Storage>(
        storage: &S,
        ledger_id: &str,
        result: TreeBuildResult,
        dict: DictKind,
    ) -> Result<run_index::DictTreeRefs> {
        let kind = ContentKind::DictBlob { dict };
        let mut leaf_cids = Vec::with_capacity(result.leaves.len());
        let mut hash_to_address = std::collections::HashMap::new();

        for leaf in &result.leaves {
            let cas_result = storage
                .content_write_bytes(kind, ledger_id, &leaf.bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            leaf_cids.push(cid_from_write(kind, &cas_result));
            hash_to_address.insert(leaf.hash.clone(), cas_result.address);
        }

        let (_, branch_bytes, _) = builder::finalize_branch(result.branch, &hash_to_address)
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let branch_result = storage
            .content_write_bytes(kind, ledger_id, &branch_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

        Ok(run_index::DictTreeRefs {
            branch: cid_from_write(kind, &branch_result),
            leaves: leaf_cids,
        })
    }

    // ---- 1. Upload flat dicts ----
    tracing::info!("uploading flat dictionary artifacts (graphs, datatypes, languages)");
    let (graphs, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("graphs.dict"),
        DictKind::Graphs,
    )
    .await?;
    let (datatypes, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("datatypes.dict"),
        DictKind::Datatypes,
    )
    .await?;
    let (languages, _) = upload_flat(
        storage,
        ledger_id,
        &run_dir.join("languages.dict"),
        DictKind::Languages,
    )
    .await?;
    tracing::info!("flat dicts uploaded");

    // ---- 2. Read subjects.fwd + subjects.idx + subjects.sids → build trees ----
    //
    // Memory strategy: build forward and reverse trees in SEPARATE passes so
    // we never hold both entry sets simultaneously. Each pass iterates through
    // the same fwd_data/offsets/lens but only allocates one entry vec at a time.
    // Subject data (fwd_data, offsets, lens) is dropped before string processing.
    tracing::info!("reading subject dictionary files from disk");
    let sids_path = run_dir.join("subjects.sids");
    let sids: Vec<u64> = run_index::dict_io::read_subject_sid_map(&sids_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", sids_path.display(), e)))?;

    // Subject forward + reverse trees (scoped to drop fwd_data before strings)
    let (subject_forward, subject_reverse) = {
        let subj_idx_path = run_dir.join("subjects.idx");
        let (subj_offsets, subj_lens) = run_index::dict_io::read_forward_index(&subj_idx_path)
            .map_err(|e| {
                IndexerError::StorageRead(format!("read {}: {}", subj_idx_path.display(), e))
            })?;
        let subj_fwd_data = std::fs::read(run_dir.join("subjects.fwd"))
            .map_err(|e| IndexerError::StorageRead(format!("read subjects.fwd: {}", e)))?;
        tracing::info!(
            subjects = sids.len(),
            fwd_bytes = subj_fwd_data.len(),
            "subject dict files loaded"
        );

        // Helper closure: extract ns-compressed suffix from IRI
        let subject_suffix = |sid: u64, off: u64, len: u32| -> (u16, Vec<u8>) {
            let iri = &subj_fwd_data[off as usize..(off as usize + len as usize)];
            let iri_str = std::str::from_utf8(iri).unwrap_or("");
            let ns_code = SubjectId::from_u64(sid).ns_code();
            let prefix = namespace_codes
                .get(&ns_code)
                .map(|s| s.as_str())
                .unwrap_or("");
            let suffix = if iri_str.starts_with(prefix) && !prefix.is_empty() {
                &iri[prefix.len()..]
            } else {
                iri
            };
            (ns_code, suffix.to_vec())
        };

        // Pass 1: forward tree (entries sorted by id)
        tracing::info!("building subject forward tree");
        let sf_tree = {
            let mut entries: Vec<ForwardEntry> = Vec::with_capacity(sids.len());
            for (&sid, (&off, &len)) in sids.iter().zip(subj_offsets.iter().zip(subj_lens.iter())) {
                let (_ns, suffix) = subject_suffix(sid, off, len);
                entries.push(ForwardEntry {
                    id: sid,
                    value: suffix,
                });
            }
            entries.sort_by_key(|e| e.id);
            builder::build_forward_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build subject fwd tree: {}", e)))?
        }; // entries dropped
        tracing::info!(
            leaves = sf_tree.leaves.len(),
            "uploading subject forward tree to CAS"
        );
        let subject_forward =
            upload_tree(storage, ledger_id, sf_tree, DictKind::SubjectForward).await?;

        // Pass 2: reverse tree (entries sorted by key)
        tracing::info!("building subject reverse tree");
        let sr_tree = {
            let mut entries: Vec<ReverseEntry> = Vec::with_capacity(sids.len());
            for (&sid, (&off, &len)) in sids.iter().zip(subj_offsets.iter().zip(subj_lens.iter())) {
                let (ns_code, suffix) = subject_suffix(sid, off, len);
                entries.push(ReverseEntry {
                    key: subject_reverse_key(ns_code, &suffix),
                    id: sid,
                });
            }
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build subject rev tree: {}", e)))?
        }; // entries dropped
        tracing::info!(
            leaves = sr_tree.leaves.len(),
            "uploading subject reverse tree to CAS"
        );
        let subject_reverse =
            upload_tree(storage, ledger_id, sr_tree, DictKind::SubjectReverse).await?;

        tracing::info!("subject CoW trees uploaded");
        (subject_forward, subject_reverse)
    }; // subj_fwd_data, subj_offsets, subj_lens all dropped here

    // ---- 3. Read strings.fwd + strings.idx → build trees ----
    //
    // Same two-pass strategy: forward tree first, drop entries, then reverse tree.
    tracing::info!("reading string dictionary files from disk");
    let str_idx_path = run_dir.join("strings.idx");
    let str_fwd_path = run_dir.join("strings.fwd");
    let (string_count, string_forward, string_reverse) = if str_idx_path.exists()
        && str_fwd_path.exists()
    {
        let (str_offsets, str_lens) = run_index::dict_io::read_forward_index(&str_idx_path)
            .map_err(|e| {
                IndexerError::StorageRead(format!("read {}: {}", str_idx_path.display(), e))
            })?;
        let str_fwd_data = std::fs::read(&str_fwd_path)
            .map_err(|e| IndexerError::StorageRead(format!("read strings.fwd: {}", e)))?;
        let count = str_offsets.len();
        tracing::info!(
            strings = count,
            fwd_bytes = str_fwd_data.len(),
            "string dict files loaded"
        );

        // Pass 1: forward tree
        tracing::info!("building string forward tree");
        let stf_tree = {
            let entries: Vec<ForwardEntry> = str_offsets
                .iter()
                .zip(str_lens.iter())
                .enumerate()
                .map(|(i, (&off, &len))| ForwardEntry {
                    id: i as u64,
                    value: str_fwd_data[off as usize..(off as usize + len as usize)].to_vec(),
                })
                .collect();
            builder::build_forward_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build string fwd tree: {}", e)))?
        }; // entries dropped
        tracing::info!(
            leaves = stf_tree.leaves.len(),
            "uploading string forward tree to CAS"
        );
        let sf = upload_tree(storage, ledger_id, stf_tree, DictKind::StringForward).await?;

        // Pass 2: reverse tree
        tracing::info!("building string reverse tree");
        let str_tree = {
            let mut entries: Vec<ReverseEntry> = str_offsets
                .iter()
                .zip(str_lens.iter())
                .enumerate()
                .map(|(i, (&off, &len))| ReverseEntry {
                    key: str_fwd_data[off as usize..(off as usize + len as usize)].to_vec(),
                    id: i as u64,
                })
                .collect();
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("build string rev tree: {}", e)))?
        }; // entries dropped
        tracing::info!(
            leaves = str_tree.leaves.len(),
            "uploading string reverse tree to CAS"
        );
        let sr = upload_tree(storage, ledger_id, str_tree, DictKind::StringReverse).await?;

        tracing::info!("string CoW trees uploaded");
        (count, sf, sr)
    } else {
        // No strings persisted — empty trees
        let empty_fwd = builder::build_forward_tree(vec![], builder::DEFAULT_TARGET_LEAF_BYTES)
            .map_err(|e| {
                IndexerError::StorageWrite(format!("build empty string fwd tree: {}", e))
            })?;
        let empty_rev = builder::build_reverse_tree(vec![], builder::DEFAULT_TARGET_LEAF_BYTES)
            .map_err(|e| {
                IndexerError::StorageWrite(format!("build empty string rev tree: {}", e))
            })?;
        let sf = upload_tree(storage, ledger_id, empty_fwd, DictKind::StringForward).await?;
        let sr = upload_tree(storage, ledger_id, empty_rev, DictKind::StringReverse).await?;
        (0, sf, sr)
    };

    // ---- 4. Upload per-predicate numbig arenas ----
    let mut numbig = BTreeMap::new();
    let nb_dir = run_dir.join("numbig");
    if nb_dir.exists() {
        for entry in std::fs::read_dir(&nb_dir)
            .map_err(|e| IndexerError::StorageRead(format!("read numbig dir: {}", e)))?
        {
            let entry = entry
                .map_err(|e| IndexerError::StorageRead(format!("read numbig entry: {}", e)))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str.strip_prefix("p_") {
                if let Some(id_str) = rest.strip_suffix(".nba") {
                    if let Ok(p_id) = id_str.parse::<u32>() {
                        let (cid, _) = upload_flat(
                            storage,
                            ledger_id,
                            &entry.path(),
                            DictKind::NumBig { p_id },
                        )
                        .await?;
                        numbig.insert(p_id.to_string(), cid);
                    }
                }
            }
        }
    }

    // ---- 4b. Upload per-predicate vector arena shards + manifests ----
    let mut vectors = BTreeMap::new();
    let vec_dir = run_dir.join("vectors");
    if vec_dir.exists() {
        // Scan for manifest files to discover which predicates have vectors
        for entry in std::fs::read_dir(&vec_dir)
            .map_err(|e| IndexerError::StorageRead(format!("read vectors dir: {}", e)))?
        {
            let entry = entry
                .map_err(|e| IndexerError::StorageRead(format!("read vectors entry: {}", e)))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str.strip_prefix("p_") {
                if let Some(id_str) = rest.strip_suffix(".vam") {
                    if let Ok(p_id) = id_str.parse::<u32>() {
                        // Read manifest to find shard count
                        let manifest_bytes = std::fs::read(entry.path()).map_err(|e| {
                            IndexerError::StorageRead(format!("read vector manifest: {}", e))
                        })?;
                        let manifest =
                            run_index::vector_arena::read_vector_manifest(&manifest_bytes)
                                .map_err(|e| {
                                    IndexerError::StorageRead(format!(
                                        "parse vector manifest: {}",
                                        e
                                    ))
                                })?;

                        // Upload each shard
                        let mut shard_cids = Vec::with_capacity(manifest.shards.len());
                        let mut shard_infos = Vec::with_capacity(manifest.shards.len());
                        for (shard_idx, shard_info) in manifest.shards.iter().enumerate() {
                            let shard_path =
                                vec_dir.join(format!("p_{}_s_{}.vas", p_id, shard_idx));
                            let (shard_cid, shard_wr) = upload_flat(
                                storage,
                                ledger_id,
                                &shard_path,
                                DictKind::VectorShard { p_id },
                            )
                            .await?;
                            shard_infos.push(run_index::vector_arena::ShardInfo {
                                cas: shard_wr.address,
                                count: shard_info.count,
                            });
                            shard_cids.push(shard_cid);
                        }

                        // Re-write manifest with real CAS addresses, then upload
                        let final_manifest = run_index::vector_arena::VectorManifest {
                            shards: shard_infos,
                            ..manifest
                        };
                        let manifest_json =
                            serde_json::to_vec_pretty(&final_manifest).map_err(|e| {
                                IndexerError::StorageWrite(format!(
                                    "serialize vector manifest: {}",
                                    e
                                ))
                            })?;
                        let final_manifest_path = vec_dir.join(format!("p_{}_final.vam", p_id));
                        std::fs::write(&final_manifest_path, &manifest_json).map_err(|e| {
                            IndexerError::StorageWrite(format!(
                                "write final vector manifest: {}",
                                e
                            ))
                        })?;
                        let (manifest_cid, _) = upload_flat(
                            storage,
                            ledger_id,
                            &final_manifest_path,
                            DictKind::VectorManifest { p_id },
                        )
                        .await?;

                        vectors.insert(
                            p_id.to_string(),
                            run_index::VectorDictRef {
                                manifest: manifest_cid,
                                shards: shard_cids,
                            },
                        );
                    }
                }
            }
        }
    }

    // ---- 5. Compute subject_id_encoding + watermarks from sids ----
    let overflow_ns: u16 = 0xFFFF;
    let mut needs_wide = false;
    let mut max_ns_code: u16 = 0;
    let mut watermark_map: BTreeMap<u16, u64> = BTreeMap::new();

    for &sid in &sids {
        let subject_id = SubjectId::from_u64(sid);
        let ns_code = subject_id.ns_code();
        let local_id = subject_id.local_id();

        if ns_code == overflow_ns {
            needs_wide = true;
            // Overflow namespace: watermark stays at 0
            continue;
        }

        if local_id > u16::MAX as u64 {
            needs_wide = true;
        }

        if ns_code > max_ns_code {
            max_ns_code = ns_code;
        }

        let entry = watermark_map.entry(ns_code).or_insert(0);
        if local_id > *entry {
            *entry = local_id;
        }
    }

    let subject_id_encoding = if needs_wide {
        SubjectIdEncoding::Wide
    } else {
        SubjectIdEncoding::Narrow
    };

    // Build watermarks vec: watermarks[i] = max local_id for ns_code i
    let watermark_len = if watermark_map.is_empty() {
        0
    } else {
        max_ns_code as usize + 1
    };
    let mut subject_watermarks: Vec<u64> = vec![0; watermark_len];
    for (&ns_code, &max_local) in &watermark_map {
        subject_watermarks[ns_code as usize] = max_local;
    }

    let string_watermark = if string_count > 0 {
        (string_count - 1) as u32
    } else {
        0
    };

    tracing::info!(
        subjects = sids.len(),
        strings = string_count,
        numbig_count = numbig.len(),
        vector_count = vectors.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        "dictionary trees built and uploaded to CAS (from disk)"
    );

    Ok(UploadedDicts {
        dict_refs: run_index::DictRefs {
            graphs,
            datatypes,
            languages,
            subject_forward,
            subject_reverse,
            string_forward,
            string_reverse,
            numbig,
            vectors,
        },
        subject_id_encoding,
        subject_watermarks,
        string_watermark,
    })
}

/// Publish index result to nameservice
pub async fn publish_index_result<P: Publisher>(publisher: &P, result: &IndexResult) -> Result<()> {
    publisher
        .publish_index(&result.ledger_id, result.index_t, &result.root_id)
        .await
        .map_err(|e| IndexerError::NameService(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stats_default() {
        let stats = IndexStats::default();
        assert_eq!(stats.flake_count, 0);
        assert_eq!(stats.leaf_count, 0);
        assert_eq!(stats.branch_count, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_normalize_id_for_comparison() {
        // Canonical format passes through
        assert_eq!(normalize_id_for_comparison("test:main"), "test:main");
        assert_eq!(
            normalize_id_for_comparison("my-ledger:dev"),
            "my-ledger:dev"
        );

        // Storage-path format with single slash converts to canonical
        assert_eq!(normalize_id_for_comparison("test/main"), "test:main");
        assert_eq!(
            normalize_id_for_comparison("my-ledger/dev"),
            "my-ledger:dev"
        );

        // Both formats normalize to the same canonical form
        assert_eq!(
            normalize_id_for_comparison("test/main"),
            normalize_id_for_comparison("test:main")
        );

        // Name without branch gets default branch
        assert_eq!(normalize_id_for_comparison("test"), "test:main");

        // Canonical format with explicit branch takes precedence over slashes in name
        // "org/project:main" - the colon is the branch separator, not the slash
        assert_eq!(
            normalize_id_for_comparison("org/project:main"),
            "org/project:main"
        );

        // Multiple slashes without colon - treated as name with default branch
        // (we don't know which slash is the "branch separator")
        assert_eq!(normalize_id_for_comparison("a/b/c"), "a/b/c:main");
    }
}
