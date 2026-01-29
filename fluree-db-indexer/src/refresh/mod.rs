//! Incremental index refresh (Phase B)
//!
//! This module implements incremental indexing, rewriting only the tree nodes
//! affected by novelty flakes rather than rebuilding the entire index.
//!
//! # Algorithm
//!
//! 1. For each index type, identify novelty flakes (time-filtered by index_t)
//! 2. Traverse the tree, descending only into nodes whose range intersects novelty
//! 3. At leaves: merge novelty, apply leaf-local dedup, split if overflow
//! 4. At branches: collect updated children, merge with unchanged, split if overflow
//! 5. Propagate changes upward until reaching root
//! 6. Write new nodes bottom-up (content-addressed)
//!
//! # Parallelism
//!
//! Index refresh runs all 5 index types (SPOT, PSOT, POST, OPST, TSPO) in parallel.
//! HLL statistics and schema extraction also run in parallel with index refresh.
//! This mirrors the Clojure implementation's use of `async/merge` + `async/reduce tally`.
//!
//! # Dedup Semantics
//!
//! Incremental refresh uses **leaf-local dedup only**. Redundant same-op flakes
//! can straddle leaf boundaries but query semantics remain correct.
//!
//! # HLL Stats (hll-stats feature)
//!
//! When the `hll-stats` feature is enabled, refresh supports true incremental stats:
//! 1. Load prior HLL sketches from storage
//! 2. Process novelty flakes through HllStatsHook (with prior sketches)
//! 3. Persist updated sketches to storage
//! 4. Include updated property stats in db-root

mod merge;
mod novelty_slice;
mod resolver;
mod seek;
mod split;
mod traversal;

pub use traversal::{integrate_novelty, RefreshInput, RefreshResult, RefreshStats};

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::gc::{collect_garbage_addresses, write_garbage_record};
use crate::writer::IndexWriter;
use fluree_db_core::index::ChildRef;
use fluree_db_core::serde::json::{serialize_db_root, DbRoot, DbRootConfig, PrevIndexRef};
#[cfg(feature = "hll-stats")]
use fluree_db_core::serde::json::PropertyStatEntry;
use fluree_db_core::{ContentAddressedWrite, ContentKind, Db, IndexType, NodeCache, Storage, StorageWrite};
use fluree_db_novelty::Novelty;
use std::time::Instant;

#[cfg(feature = "hll-stats")]
use crate::stats::{load_hll_sketches, persist_hll_sketches_iter, HllStatsHook, IndexStatsHook, PropertyHll};
#[cfg(feature = "hll-stats")]
use fluree_db_core::Sid;
#[cfg(feature = "hll-stats")]
use std::collections::{HashMap, HashSet};

use crate::stats::{compute_class_property_stats_parallel, SchemaExtractor};
use fluree_db_core::serde::json::DbRootSchema;

/// Result of refreshing all index trees
#[derive(Debug, Clone)]
pub struct RefreshIndexResult {
    /// Root address of the new index
    pub root_address: String,
    /// SPOT index root
    pub spot: ChildRef,
    /// PSOT index root
    pub psot: ChildRef,
    /// POST index root
    pub post: ChildRef,
    /// OPST index root
    pub opst: ChildRef,
    /// TSPO index root
    pub tspo: ChildRef,
    /// Transaction time the refreshed index is current through
    pub index_t: i64,
    /// Refresh statistics
    pub stats: RefreshStats,
}

/// Incrementally refresh index with novelty flakes
///
/// Only rewrites nodes affected by novelty. Unchanged nodes are reused.
/// This is a convenience wrapper around [`refresh_index_with_prev`] that
/// doesn't set config or prev_index.
///
/// # Arguments
///
/// * `storage` - Storage backend for reading/writing nodes
/// * `db` - Current database state (contains current index roots)
/// * `novelty` - Novelty overlay with new flakes
/// * `target_t` - Target transaction time for the new index
/// * `config` - Indexer configuration
///
/// # Returns
///
/// The new index result with updated roots and statistics.
pub async fn refresh_index<S, C>(
    storage: &S,
    db: &Db<S, C>,
    novelty: &Novelty,
    target_t: i64,
    config: IndexerConfig,
) -> Result<RefreshIndexResult>
where
    S: Storage + StorageWrite + ContentAddressedWrite + Sync,
    C: NodeCache + Sync,
{
    refresh_index_with_prev(storage, db, novelty, target_t, config, None, None).await
}

/// Incrementally refresh index with novelty flakes, config, and prev-index support
///
/// Only rewrites nodes affected by novelty. Unchanged nodes are reused.
///
/// # Parallelism
///
/// This function runs all operations in parallel where possible:
/// - All 5 index types (SPOT, PSOT, POST, OPST, TSPO) refresh concurrently
/// - HLL stats processing runs in parallel with index refresh
/// - Schema extraction runs in parallel with index refresh
///
/// This mirrors the Clojure implementation's `async/merge` + `async/reduce tally` pattern.
///
/// # Arguments
///
/// * `storage` - Storage backend for reading/writing nodes
/// * `db` - Current database state (contains current index roots)
/// * `novelty` - Novelty overlay with new flakes
/// * `target_t` - Target transaction time for the new index
/// * `indexer_config` - Indexer configuration (leaf/branch sizes)
/// * `db_config` - Optional index configuration to persist in db-root (reindex thresholds)
/// * `prev_index` - Optional reference to previous index for GC chain
///
/// # Returns
///
/// The new index result with updated roots and statistics.
pub async fn refresh_index_with_prev<S, C>(
    storage: &S,
    db: &Db<S, C>,
    novelty: &Novelty,
    target_t: i64,
    indexer_config: IndexerConfig,
    db_config: Option<DbRootConfig>,
    prev_index: Option<PrevIndexRef>,
) -> Result<RefreshIndexResult>
where
    S: Storage + StorageWrite + ContentAddressedWrite + Sync,
    C: NodeCache + Sync,
{
    let start_time = Instant::now();
    let current_index_t = db.t;
    let alias = &db.alias;
    let novelty_size = novelty.len();

    tracing::info!(
        alias = %alias,
        target_t = target_t,
        current_t = current_index_t,
        novelty_size = novelty_size,
        "Index refresh starting"
    );

    // Validate all roots exist before starting parallel refresh
    let spot_root = db.spot.as_ref().ok_or_else(|| {
        crate::error::IndexerError::InvalidConfig("Cannot refresh: no existing SPOT root".to_string())
    })?;
    let psot_root = db.psot.as_ref().ok_or_else(|| {
        crate::error::IndexerError::InvalidConfig("Cannot refresh: no existing PSOT root".to_string())
    })?;
    let post_root = db.post.as_ref().ok_or_else(|| {
        crate::error::IndexerError::InvalidConfig("Cannot refresh: no existing POST root".to_string())
    })?;
    let opst_root = db.opst.as_ref().ok_or_else(|| {
        crate::error::IndexerError::InvalidConfig("Cannot refresh: no existing OPST root".to_string())
    })?;
    let tspo_root = db.tspo.as_ref().ok_or_else(|| {
        crate::error::IndexerError::InvalidConfig("Cannot refresh: no existing TSPO root".to_string())
    })?;

    // ===========================================================================
    // PARALLEL EXECUTION: Run all 5 index types + HLL + schema concurrently
    // ===========================================================================
    // This mirrors Clojure's pattern of using async/merge for parallel index refresh
    // and kicking off stats computation in parallel.

    // Create the parallel refresh tasks for all 5 index types
    let spot_future = refresh_single_index(
        storage, spot_root.clone(), IndexType::Spot, novelty, alias, &indexer_config, current_index_t, target_t
    );
    let psot_future = refresh_single_index(
        storage, psot_root.clone(), IndexType::Psot, novelty, alias, &indexer_config, current_index_t, target_t
    );
    let post_future = refresh_single_index(
        storage, post_root.clone(), IndexType::Post, novelty, alias, &indexer_config, current_index_t, target_t
    );
    let opst_future = refresh_single_index(
        storage, opst_root.clone(), IndexType::Opst, novelty, alias, &indexer_config, current_index_t, target_t
    );
    let tspo_future = refresh_single_index(
        storage, tspo_root.clone(), IndexType::Tspo, novelty, alias, &indexer_config, current_index_t, target_t
    );

    // Create HLL stats future (runs in parallel with index refresh)
    #[cfg(feature = "hll-stats")]
    let hll_future = compute_hll_stats_parallel(
        storage, alias, db.stats.as_ref(), novelty, current_index_t, target_t
    );

    // Create schema extraction future (runs in parallel with index refresh)
    let schema_future = extract_schema_parallel(
        db.schema.as_ref(), novelty, current_index_t, target_t
    );

    // Create class-property stats future (runs in parallel with index refresh)
    // Collects SPOT novelty flakes for processing
    let spot_flakes: Vec<fluree_db_core::Flake> = novelty
        .iter_index(IndexType::Spot)
        .map(|id| novelty.get_flake(id).clone())
        .collect();
    let class_stats_future = compute_class_property_stats_parallel(
        db, db.stats.as_ref(), &spot_flakes
    );

    // ===========================================================================
    // AWAIT ALL: Join all parallel operations
    // ===========================================================================
    // Using tokio::join! to run all operations concurrently

    #[cfg(feature = "hll-stats")]
    let (spot_result, psot_result, post_result, opst_result, tspo_result, hll_result, schema_result, class_stats_result) = tokio::join!(
        spot_future,
        psot_future,
        post_future,
        opst_future,
        tspo_future,
        hll_future,
        schema_future,
        class_stats_future
    );

    #[cfg(not(feature = "hll-stats"))]
    let (spot_result, psot_result, post_result, opst_result, tspo_result, schema_result, class_stats_result) = tokio::join!(
        spot_future,
        psot_future,
        post_future,
        opst_future,
        tspo_future,
        schema_future,
        class_stats_future
    );

    // ===========================================================================
    // TALLY: Assemble results from parallel operations (like Clojure's tally fn)
    // ===========================================================================

    // Unwrap index results (propagate first error if any failed)
    let spot_result = spot_result?;
    let psot_result = psot_result?;
    let post_result = post_result?;
    let opst_result = opst_result?;
    let tspo_result = tspo_result?;

    // Extract roots
    let spot = spot_result.new_root;
    let psot = psot_result.new_root;
    let post = post_result.new_root;
    let opst = opst_result.new_root;
    let tspo = tspo_result.new_root;

    // Merge stats from all index types
    let mut total_stats = RefreshStats::default();
    total_stats.merge(&spot_result.stats);
    total_stats.merge(&psot_result.stats);
    total_stats.merge(&post_result.stats);
    total_stats.merge(&opst_result.stats);
    total_stats.merge(&tspo_result.stats);

    // Compute db-root stats with Clojure semantics (no tree walking):
    // - flakes: subtree flake count from SPOT root
    // - size: estimated flake-bytes updated incrementally via SPOT leaf deltas
    // - properties: updated via HLL sketch merge (when hll-stats feature enabled)
    let prev_size = db.stats.as_ref().map(|s| s.size).unwrap_or(0);
    let size_i128 = prev_size as i128 + (total_stats.flake_bytes_delta as i128);
    let size = if size_i128 < 0 { 0 } else { size_i128 as u64 };

    // Get HLL properties (persist sketches if needed)
    // Also compute obsolete sketch addresses for garbage collection
    #[cfg(feature = "hll-stats")]
    let (updated_properties, obsolete_sketch_addresses) = {
        let hll_result = hll_result?;
        // Only persist sketches for properties that were touched by novelty flakes.
        // This avoids rewriting unchanged sketch files (extra I/O) and matches
        // Clojure's "write only modified this index" behavior.
        // Use iterator-based persist to avoid cloning PropertyHll (which has 512 bytes of HLL data).
        if !hll_result.touched_predicates.is_empty() {
            let touched_iter = hll_result
                .properties
                .iter()
                .filter(|(sid, _)| hll_result.touched_predicates.contains(sid));
            let _ = persist_hll_sketches_iter(storage, alias, touched_iter).await;
        }
        // Compute obsolete sketch addresses before consuming hll_result
        let prior_properties = db.stats.as_ref().and_then(|s| s.properties.as_ref());
        let obsolete = if let Some(prior_props) = prior_properties {
            crate::stats::compute_obsolete_sketch_addresses(alias, prior_props, &hll_result.properties)
        } else {
            Vec::new()
        };
        (hll_result.summary_properties, obsolete)
    };

    // Preserve prior HLL properties if hll-stats disabled (no obsolete sketches)
    #[cfg(not(feature = "hll-stats"))]
    let (updated_properties, obsolete_sketch_addresses): (
        Option<Vec<fluree_db_core::serde::json::PropertyStatEntry>>,
        Vec<String>,
    ) = (db.stats.as_ref().and_then(|s| s.properties.clone()), Vec::new());

    // Get class-property stats from parallel extraction
    let updated_classes = match class_stats_result {
        Ok(result) => result.classes,
        Err(e) => {
            // Log warning but don't fail refresh - class stats are optional
            tracing::warn!("Class-property stats computation failed: {}", e);
            db.stats.as_ref().and_then(|s| s.classes.clone())
        }
    };

    let stats_flakes = spot.size;
    let db_root_stats = fluree_db_core::serde::json::DbRootStats {
        flakes: stats_flakes,
        size,
        properties: updated_properties,
        classes: updated_classes,
    };

    // Determine config: use provided db_config, or fall back to db's existing config
    let final_config = db_config.or_else(|| db.config.clone());

    // Get schema from parallel extraction
    let updated_schema = schema_result;

    // ===========================================================================
    // GARBAGE COLLECTION: Collect and write garbage record
    // ===========================================================================
    // Collect replaced nodes from all 5 index trees + obsolete sketches
    let garbage_addresses = collect_garbage_addresses(
        &[
            &spot_result.stats,
            &psot_result.stats,
            &post_result.stats,
            &opst_result.stats,
            &tspo_result.stats,
        ],
        obsolete_sketch_addresses,
    );

    // Write garbage record (if there are any replaced nodes)
    let garbage_ref = write_garbage_record(
        storage,
        alias,
        target_t,
        garbage_addresses,
    )
    .await?;

    // Build and write the new DbRoot
    let db_root = DbRoot {
        alias: alias.to_string(),
        t: target_t,
        version: 2,
        namespace_codes: db.namespace_codes.clone(),
        spot: Some(spot.clone()),
        psot: Some(psot.clone()),
        post: Some(post.clone()),
        opst: Some(opst.clone()),
        tspo: Some(tspo.clone()),
        timestamp: None,
        stats: Some(db_root_stats),
        config: final_config,
        prev_index,
        schema: updated_schema,
        garbage: garbage_ref,
    };

    let root_bytes = serialize_db_root(&db_root)?;
    let mut root_writer = IndexWriter::new(storage, alias, ContentKind::IndexRoot);
    let root_address = root_writer.write_root(&root_bytes).await?;

    let duration_ms = start_time.elapsed().as_millis() as u64;
    tracing::info!(
        alias = %alias,
        target_t = target_t,
        duration_ms = duration_ms,
        root_address = %root_address,
        flakes = stats_flakes,
        "Index refresh complete"
    );

    Ok(RefreshIndexResult {
        root_address,
        spot,
        psot,
        post,
        opst,
        tspo,
        index_t: target_t,
        stats: total_stats,
    })
}

/// Refresh a single index type
///
/// This is a helper function for parallel refresh. Each index type gets its own
/// IndexWriter to avoid contention.
async fn refresh_single_index<S>(
    storage: &S,
    root: ChildRef,
    index_type: IndexType,
    novelty: &Novelty,
    alias: &str,
    config: &IndexerConfig,
    since_t: i64,
    to_t: i64,
) -> Result<RefreshResult>
where
    S: Storage + StorageWrite + ContentAddressedWrite,
{
    let mut writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode { index_type },
    );
    let input = RefreshInput {
        storage,
        root,
        index_type,
        novelty,
        alias,
        config,
        since_t,
        to_t,
    };
    integrate_novelty(input, &mut writer).await
}

/// Result of parallel HLL computation
#[cfg(feature = "hll-stats")]
struct HllComputeResult {
    /// Property HLLs for persistence (all properties, prior + novelty)
    properties: HashMap<Sid, PropertyHll>,
    /// Summary properties for db-root
    summary_properties: Option<Vec<PropertyStatEntry>>,
    /// Predicates touched by novelty flakes (only these need sketch persistence)
    touched_predicates: HashSet<Sid>,
}

/// Compute HLL stats in parallel with index refresh
///
/// This loads prior sketches, processes novelty, and prepares results for persistence.
/// The actual persistence happens after join to maintain the storage write order.
#[cfg(feature = "hll-stats")]
async fn compute_hll_stats_parallel<S>(
    storage: &S,
    alias: &str,
    prior_stats: Option<&fluree_db_core::serde::json::DbRootStats>,
    novelty: &Novelty,
    current_index_t: i64,
    target_t: i64,
) -> Result<HllComputeResult>
where
    S: Storage,
{
    // Load prior HLL sketches if properties exist
    let prior_hll_properties = if let Some(stats) = prior_stats {
        if let Some(ref props) = stats.properties {
            load_hll_sketches(storage, alias, props).await.ok()
        } else {
            None
        }
    } else {
        None
    };

    // Create stats hook (with or without prior properties)
    let mut stats_hook = if let Some(prior) = prior_hll_properties {
        HllStatsHook::with_prior_properties(prior)
    } else {
        HllStatsHook::new()
    };

    // Process novelty flakes through the hook
    // Use SPOT index to iterate all flakes (OPST only has refs)
    // Apply leaf-local same-op dedup so redundant retracts do not
    // incorrectly decrement property counts multiple times (Clojure parity).
    // Also track which predicates are touched by novelty (for selective persistence).
    let mut novelty_flakes: Vec<fluree_db_core::Flake> = Vec::new();
    let mut touched_predicates: HashSet<Sid> = HashSet::new();
    for id in novelty.iter_index(IndexType::Spot) {
        let flake = novelty.get_flake(id);
        if flake.t > current_index_t && flake.t <= target_t {
            touched_predicates.insert(flake.p.clone());
            novelty_flakes.push(flake.clone());
        }
    }
    let novelty_flakes = merge::dedup_leaf_local(novelty_flakes);
    for flake in &novelty_flakes {
        stats_hook.on_flake(flake);
    }

    // Clone properties for persistence before finalize consumes them
    let properties = stats_hook.into_properties();

    // Create a new hook from the properties to finalize
    let finalize_hook = HllStatsHook::with_prior_properties(properties.clone());
    let artifacts = (Box::new(finalize_hook) as Box<dyn IndexStatsHook>).finalize();

    Ok(HllComputeResult {
        properties,
        summary_properties: artifacts.summary.properties,
        touched_predicates,
    })
}

/// Extract schema in parallel with index refresh
///
/// This processes novelty flakes to extract rdfs:subClassOf and rdfs:subPropertyOf
/// relationships. Runs concurrently with index refresh since it only reads novelty.
async fn extract_schema_parallel(
    prior_schema: Option<&DbRootSchema>,
    novelty: &Novelty,
    current_index_t: i64,
    target_t: i64,
) -> Option<DbRootSchema> {
    let mut extractor = SchemaExtractor::from_prior(prior_schema);

    // Process novelty flakes through the schema extractor
    // Use SPOT index to iterate all flakes
    for id in novelty.iter_index(IndexType::Spot) {
        let flake = novelty.get_flake(id);
        // Only process flakes in the time range
        if flake.t > current_index_t && flake.t <= target_t {
            extractor.on_flake(flake);
        }
    }

    // Finalize and get the updated schema
    // If no schema relationships exist, returns None
    extractor.finalize(target_t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::build_index;
    use crate::config::IndexerConfig;
    use fluree_db_core::cache::NoCache;
    use fluree_db_core::serde::json::parse_branch_node;
    use fluree_db_core::{range, Db, FlakeValue, MemoryStorage, RangeMatch, RangeOptions, RangeTest, Sid, Storage};
    use fluree_db_novelty::Novelty;
    use std::collections::BTreeMap;

    fn make_flake(s_code: i32, s_name: &str, p_code: i32, p_name: &str, val: i64, t: i64) -> fluree_db_core::Flake {
        fluree_db_core::Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "xsd:long"),
            t,
            true,
            None,
        )
    }

    fn make_ref_flake(s_code: i32, s_name: &str, p_code: i32, p_name: &str, o_code: i32, o_name: &str, t: i64) -> fluree_db_core::Flake {
        fluree_db_core::Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Ref(Sid::new(o_code, o_name)),
            Sid::new(0, "$id"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_refresh_index_end_to_end() {
        // Setup: Create storage and initial flakes at t=1
        let storage = MemoryStorage::new();
        let config = IndexerConfig::small();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);

        // Initial flakes at t=1
        let initial_flakes = vec![
            make_flake(1, "ex:alice", 1, "ex:age", 30, 1),
            make_flake(1, "ex:alice", 1, "ex:name", 100, 1), // Using Long for simplicity
            make_flake(1, "ex:bob", 1, "ex:age", 25, 1),
            make_ref_flake(1, "ex:alice", 1, "ex:knows", 1, "ex:bob", 1),
        ];

        // Build initial index at t=1
        let initial_result = build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes.clone(),
            namespace_codes.clone(),
            config.clone(),
        )
        .await
        .expect("Failed to build initial index");

        assert_eq!(initial_result.index_t, 1);

        // Load the Db from the initial index
        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &initial_result.root_address)
            .await
            .expect("Failed to load Db");

        assert_eq!(db.t, 1);
        assert_eq!(db.alias, "test/ledger");

        // Create novelty and add new flakes at t=2 and t=3
        let mut novelty = Novelty::new(1); // Start from t=1

        // t=2: Add new data
        let t2_flakes = vec![
            make_flake(1, "ex:alice", 1, "ex:score", 95, 2),
            make_flake(1, "ex:charlie", 1, "ex:age", 35, 2),
        ];
        novelty.apply_commit(t2_flakes, 2).expect("Failed to apply t=2 commit");

        // t=3: More updates
        let t3_flakes = vec![
            make_flake(1, "ex:bob", 1, "ex:score", 88, 3),
            make_ref_flake(1, "ex:charlie", 1, "ex:knows", 1, "ex:alice", 3),
        ];
        novelty.apply_commit(t3_flakes, 3).expect("Failed to apply t=3 commit");

        // Refresh the index to t=3
        let refresh_result = refresh_index(&storage, &db, &novelty, 3, config.clone())
            .await
            .expect("Failed to refresh index");

        // Verify the refresh result
        assert_eq!(refresh_result.index_t, 3);
        assert!(refresh_result.stats.nodes_visited > 0, "Should have visited some nodes");

        // Load the new Db and verify
        let new_db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &refresh_result.root_address)
            .await
            .expect("Failed to load refreshed Db");

        assert_eq!(new_db.t, 3);
        assert_eq!(new_db.alias, "test/ledger");

        // Verify all index roots are present
        assert!(new_db.spot.is_some());
        assert!(new_db.psot.is_some());
        assert!(new_db.post.is_some());
        assert!(new_db.opst.is_some());
        assert!(new_db.tspo.is_some());
    }

    #[tokio::test]
    async fn test_refresh_with_no_novelty_reuses_root() {
        // Setup: Create storage and initial flakes at t=1
        let storage = MemoryStorage::new();
        let config = IndexerConfig::small();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
        ]);

        let initial_flakes = vec![
            make_flake(1, "ex:alice", 1, "ex:age", 30, 1),
        ];

        // Build initial index
        let initial_result = build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Failed to build initial index");

        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &initial_result.root_address)
            .await
            .expect("Failed to load Db");

        // Create empty novelty (no new commits)
        let novelty = Novelty::new(1);

        // Refresh with target_t=1 (no new data)
        let refresh_result = refresh_index(&storage, &db, &novelty, 1, config.clone())
            .await
            .expect("Failed to refresh index");

        // Should reuse all roots since no novelty
        assert_eq!(refresh_result.stats.nodes_reused, 5, "Should reuse all 5 index roots");
        assert_eq!(refresh_result.stats.nodes_visited, 0, "Should not visit any nodes");
    }

    #[tokio::test]
    async fn test_refresh_respects_to_t_upper_bound() {
        // Setup
        let storage = MemoryStorage::new();
        let config = IndexerConfig::small();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (0, "fluree:".to_string()),
            (1, "ex:".to_string()),
        ]);

        let initial_flakes = vec![
            make_flake(1, "ex:alice", 1, "ex:age", 30, 1),
        ];

        let initial_result = build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Failed to build initial index");

        let db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &initial_result.root_address)
            .await
            .expect("Failed to load Db");

        // Create novelty with flakes at t=2 and t=5
        let mut novelty = Novelty::new(1);
        novelty.apply_commit(vec![make_flake(1, "ex:bob", 1, "ex:age", 25, 2)], 2).unwrap();
        novelty.apply_commit(vec![make_flake(1, "ex:charlie", 1, "ex:age", 35, 5)], 5).unwrap();

        // Refresh only to t=3 (should include t=2 but NOT t=5)
        let refresh_result = refresh_index(&storage, &db, &novelty, 3, config.clone())
            .await
            .expect("Failed to refresh index");

        // The index should claim to be current through t=3
        assert_eq!(refresh_result.index_t, 3);

        // Load and verify
        let new_db: Db<_, NoCache> = Db::load(storage.clone(), NoCache, &refresh_result.root_address)
            .await
            .expect("Failed to load refreshed Db");

        assert_eq!(new_db.t, 3);
    }

    #[tokio::test]
    async fn test_refresh_novelty_between_leaf_boundaries_is_included() {
        let storage = MemoryStorage::new();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);

        fn make_flake(name: &str, t: i64) -> fluree_db_core::Flake {
            fluree_db_core::Flake::new(
                Sid::new(1, name),
                Sid::new(1, "p"),
                FlakeValue::Long(1),
                Sid::new(2, "long"),
                t,
                true,
                None,
            )
        }

        let flake_a = make_flake("a", 1);
        let flake_c = make_flake("c", 1);
        let flake_b = make_flake("b", 2); // between a and c in SPOT order

        // Force leaves to split into single-flake leaves by bytes
        let flake_bytes = flake_a.size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes, flake_bytes, 2, 4);

        let initial_result = build_index(
            &storage,
            "test/ledger",
            1,
            vec![flake_a.clone(), flake_c.clone()],
            namespace_codes.clone(),
            config.clone(),
        )
        .await
        .expect("Failed to build initial index");

        let db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &initial_result.root_address)
                .await
                .expect("Failed to load Db");

        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(vec![flake_b.clone()], 2)
            .expect("Failed to apply novelty");

        let refresh_result = refresh_index(&storage, &db, &novelty, 2, config.clone())
            .await
            .expect("Failed to refresh index");

        let new_db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &refresh_result.root_address)
                .await
                .expect("Failed to load refreshed Db");

        let results = range(
            &new_db,
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject(Sid::new(1, "b")),
            RangeOptions::default(),
        )
        .await
        .expect("Range query failed");

        assert!(
            !results.is_empty(),
            "Expected novelty flake at subject 'b' to be indexed"
        );
    }

    async fn assert_boundary_invariants(storage: &MemoryStorage, root: &fluree_db_core::index::ChildRef) {
        let mut stack = vec![root.clone()];

        while let Some(node) = stack.pop() {
            if node.leaf {
                continue;
            }

            let bytes = storage.read_bytes(&node.id).await.unwrap();
            let children = parse_branch_node(bytes).unwrap();

            for (i, child) in children.iter().enumerate() {
                if child.leftmost {
                    assert!(child.first.is_none(), "leftmost child must have first=None");
                } else {
                    assert!(child.first.is_some(), "non-leftmost child must have first");
                }

                if i + 1 < children.len() {
                    assert_eq!(
                        child.rhs,
                        children[i + 1].first,
                        "child rhs must equal next child first"
                    );
                } else if node.rhs.is_none() {
                    assert!(child.rhs.is_none(), "rightmost child must have rhs=None");
                } else {
                    assert_eq!(child.rhs, node.rhs, "rightmost child must inherit parent rhs");
                }
            }

            stack.extend(children);
        }
    }

    #[tokio::test]
    async fn test_refresh_preserves_boundary_invariants() {
        let storage = MemoryStorage::new();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);

        fn make_flake(name: &str, t: i64) -> fluree_db_core::Flake {
            fluree_db_core::Flake::new(
                Sid::new(1, name),
                Sid::new(1, "p"),
                FlakeValue::Long(1),
                Sid::new(2, "long"),
                t,
                true,
                None,
            )
        }

        let flake_bytes = make_flake("s0", 0).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 2, flake_bytes * 3, 2, 3);

        let initial_flakes: Vec<_> = (0..8)
            .map(|i| make_flake(&format!("s{}", i), 1))
            .collect();

        let initial_result = build_index(
            &storage,
            "test/ledger",
            1,
            initial_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Failed to build initial index");

        let db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &initial_result.root_address)
                .await
                .expect("Failed to load Db");

        let mut novelty = Novelty::new(1);
        let novelty_flakes: Vec<_> = (8..12)
            .map(|i| make_flake(&format!("s{}", i), 2))
            .collect();
        novelty.apply_commit(novelty_flakes, 2).unwrap();

        let refresh_result = refresh_index(&storage, &db, &novelty, 2, config.clone())
            .await
            .expect("Failed to refresh index");

        let new_db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &refresh_result.root_address)
                .await
                .expect("Failed to load refreshed Db");

        let spot_root = new_db.spot.as_ref().expect("missing SPOT root");
        assert_boundary_invariants(&storage, spot_root).await;
    }

    #[tokio::test]
    async fn test_refresh_matches_full_rebuild_for_subject_queries() {
        let storage = MemoryStorage::new();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (1, "ex:".to_string()),
            (2, "xsd:".to_string()),
        ]);

        fn make_flake(s: &str, p: &str, o: i64, t: i64) -> fluree_db_core::Flake {
            fluree_db_core::Flake::new(
                Sid::new(1, s),
                Sid::new(1, p),
                FlakeValue::Long(o),
                Sid::new(2, "long"),
                t,
                true,
                None,
            )
        }

        let flake_bytes = make_flake("s0", "p0", 1, 1).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 2, flake_bytes * 3, 2, 3);

        let base_flakes = vec![
            make_flake("s1", "p1", 10, 1),
            make_flake("s1", "p2", 20, 1),
            make_flake("s2", "p1", 30, 1),
            make_flake("s3", "p3", 40, 1),
        ];

        let novelty_flakes = vec![
            make_flake("s1", "p4", 50, 2),
            make_flake("s2", "p2", 60, 2),
            make_flake("s4", "p1", 70, 2),
        ];

        let all_flakes = base_flakes
            .iter()
            .cloned()
            .chain(novelty_flakes.iter().cloned())
            .collect::<Vec<_>>();

        let base_result = build_index(
            &storage,
            "test/ledger",
            1,
            base_flakes,
            namespace_codes.clone(),
            config.clone(),
        )
        .await
        .expect("Failed to build base index");

        let base_db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &base_result.root_address)
                .await
                .expect("Failed to load base Db");

        let mut novelty = Novelty::new(1);
        novelty.apply_commit(novelty_flakes, 2).unwrap();

        let refreshed = refresh_index(&storage, &base_db, &novelty, 2, config.clone())
            .await
            .expect("Failed to refresh index");

        let refreshed_db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &refreshed.root_address)
                .await
                .expect("Failed to load refreshed Db");

        let rebuilt = build_index(
            &storage,
            "test/ledger",
            2,
            all_flakes,
            namespace_codes,
            config.clone(),
        )
        .await
        .expect("Failed to build full index");

        let rebuilt_db: Db<_, NoCache> =
            Db::load(storage.clone(), NoCache, &rebuilt.root_address)
                .await
                .expect("Failed to load rebuilt Db");

        let subjects = ["s1", "s2", "s3", "s4"];
        for s in subjects {
            let subject = Sid::new(1, s);
            let mut refreshed_flakes = range(
                &refreshed_db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject.clone()),
                RangeOptions::default(),
            )
            .await
            .unwrap();

            let mut rebuilt_flakes = range(
                &rebuilt_db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject),
                RangeOptions::default(),
            )
            .await
            .unwrap();

            refreshed_flakes.sort_by(|a, b| IndexType::Spot.compare(a, b));
            rebuilt_flakes.sort_by(|a, b| IndexType::Spot.compare(a, b));

            assert_eq!(
                refreshed_flakes, rebuilt_flakes,
                "Refresh results differ from rebuild for subject {}",
                s
            );
        }
    }
}
