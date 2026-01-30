//! Index tree builder (TEST-ONLY)
//!
//! **Note**: This module is test-only scaffolding (`#[cfg(test)]`). It provides
//! full index building from flakes for test setup. Production code uses the
//! refresh-based pipeline via `batched_rebuild_from_commits` or `refresh_index`.
//!
//! Builds the 5 index trees (SPOT, PSOT, POST, OPST, TSPO) from flakes.

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::node::build_tree;
use crate::stats::ClassPropertyExtractor;
use crate::stats::IndexStatsHook;
#[cfg(not(feature = "hll-stats"))]
use crate::stats::NoOpStatsHook;
#[cfg(feature = "hll-stats")]
use crate::stats::{persist_hll_sketches, HllStatsHook};
use crate::writer::IndexWriter;
use crate::{IndexResult, IndexStats};
use fluree_db_core::flake::size_flakes_estimate;
use fluree_db_core::serde::json::{serialize_db_root, DbRoot, DbRootStats};
use fluree_db_core::{ContentAddressedWrite, ContentKind, Flake, IndexType, StorageWrite};
use std::collections::BTreeMap;
use std::time::Instant;

/// Build a complete index from flakes
///
/// Creates all 5 index trees and writes them to storage.
pub async fn build_index<S: ContentAddressedWrite + StorageWrite>(
    storage: &S,
    alias: &str,
    index_t: i64,
    flakes: Vec<Flake>,
    namespace_codes: BTreeMap<i32, String>,
    config: IndexerConfig,
) -> Result<IndexResult> {
    let start_time = Instant::now();
    let flake_count = flakes.len();

    tracing::info!(
        alias = %alias,
        target_t = index_t,
        flake_count = flake_count,
        "Index build starting"
    );

    let mut total_stats = IndexStats::default();

    // Create stats hook (HLL when feature enabled, no-op otherwise)
    #[cfg(feature = "hll-stats")]
    let mut stats_hook = HllStatsHook::new();
    #[cfg(not(feature = "hll-stats"))]
    let mut stats_hook: Box<dyn IndexStatsHook> = Box::new(NoOpStatsHook::new());

    // Record flakes in stats
    for flake in &flakes {
        stats_hook.on_flake(flake);
    }

    // Build each index tree (one writer per index type directory for Clojure parity)
    let mut spot_writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode {
            index_type: IndexType::Spot,
        },
    );
    let spot_root = build_tree(&flakes, IndexType::Spot, &config, &mut spot_writer).await?;
    total_stats.leaf_count += spot_writer.leaf_count();
    total_stats.branch_count += spot_writer.branch_count();

    let mut psot_writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode {
            index_type: IndexType::Psot,
        },
    );
    let psot_root = build_tree(&flakes, IndexType::Psot, &config, &mut psot_writer).await?;
    total_stats.leaf_count += psot_writer.leaf_count();
    total_stats.branch_count += psot_writer.branch_count();

    let mut post_writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode {
            index_type: IndexType::Post,
        },
    );
    let post_root = build_tree(&flakes, IndexType::Post, &config, &mut post_writer).await?;
    total_stats.leaf_count += post_writer.leaf_count();
    total_stats.branch_count += post_writer.branch_count();

    // OPST only contains reference flakes
    let ref_flakes: Vec<Flake> = flakes.iter().filter(|f| f.is_ref()).cloned().collect();
    let mut opst_writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode {
            index_type: IndexType::Opst,
        },
    );
    let opst_root = build_tree(&ref_flakes, IndexType::Opst, &config, &mut opst_writer).await?;
    total_stats.leaf_count += opst_writer.leaf_count();
    total_stats.branch_count += opst_writer.branch_count();

    let mut tspo_writer = IndexWriter::new(
        storage,
        alias,
        ContentKind::IndexNode {
            index_type: IndexType::Tspo,
        },
    );
    let tspo_root = build_tree(&flakes, IndexType::Tspo, &config, &mut tspo_writer).await?;
    total_stats.leaf_count += tspo_writer.leaf_count();
    total_stats.branch_count += tspo_writer.branch_count();

    // Finalize stats and persist HLL sketches
    // For HLL: extract properties before finalize, persist to storage, then finalize
    #[cfg(feature = "hll-stats")]
    let stats_artifacts = {
        // Extract properties before finalize consumes the hook
        let properties = stats_hook.into_properties();

        // Persist HLL sketches to storage (for subsequent refreshes to load)
        if !properties.is_empty() {
            let _ = persist_hll_sketches(storage, alias, &properties).await;
        }

        // Create a new hook with the properties to finalize (for the summary)
        let finalize_hook = HllStatsHook::with_prior_properties(properties);
        (Box::new(finalize_hook) as Box<dyn IndexStatsHook>).finalize()
    };

    #[cfg(not(feature = "hll-stats"))]
    let stats_artifacts = stats_hook.finalize();

    // Use the known flake count (stats_artifacts.summary.flake_count may be 0 for HLL
    // due to with_prior_properties not tracking counts from the original hook)
    total_stats.flake_count = flake_count;
    total_stats.total_bytes = spot_writer.total_bytes()
        + psot_writer.total_bytes()
        + post_writer.total_bytes()
        + opst_writer.total_bytes()
        + tspo_writer.total_bytes();

    // Compute class-property statistics from the full flake set.
    //
    // This mirrors the Clojure behavior where class/property stats are present in the db-root
    // even for the first (genesis) index build.
    //
    // We treat all flakes as "novelty" relative to an empty prior state.
    let classes = {
        let mut extractor = ClassPropertyExtractor::new();
        for flake in &flakes {
            extractor.collect_type_flake(flake);
        }
        for flake in &flakes {
            extractor.process_flake(flake);
        }
        extractor.finalize()
    };

    // Convert namespace_codes to HashMap for DbRoot
    let namespace_codes_map = namespace_codes.into_iter().collect();

    // Build DbRootStats (Clojure-style semantics: speed over accuracy)
    //
    // - flakes: total flakes in the index (after dedup)
    // - size: estimated flake bytes (NOT storage bytes of index nodes)
    // - properties: per-property HLL stats (when hll-stats feature enabled)
    let stats_flakes = total_stats.flake_count as u64;
    let db_root_stats = DbRootStats {
        flakes: stats_flakes,
        size: size_flakes_estimate(&flakes),
        #[cfg(feature = "hll-stats")]
        properties: stats_artifacts.summary.properties,
        #[cfg(not(feature = "hll-stats"))]
        properties: None,
        classes,
    };

    // Build the DbRoot with stats (config, prev_index, garbage are None for genesis build)
    let db_root = DbRoot {
        alias: alias.to_string(),
        t: index_t,
        version: 2,
        namespace_codes: namespace_codes_map,
        spot: Some(spot_root),
        psot: Some(psot_root),
        post: Some(post_root),
        opst: Some(opst_root),
        tspo: Some(tspo_root),
        timestamp: None,
        stats: Some(db_root_stats),
        config: None,     // Config can be added later via refresh_index_with_prev
        prev_index: None, // Genesis build has no previous index
        schema: None,     // Schema can be populated during refresh with schema tracking
        garbage: None,    // Genesis build has no garbage
    };

    // Serialize and write the root
    let root_bytes = serialize_db_root(&db_root)?;
    let mut root_writer = IndexWriter::new(storage, alias, ContentKind::IndexRoot);
    let root_address = root_writer.write_root(&root_bytes).await?;

    let duration_ms = start_time.elapsed().as_millis() as u64;
    tracing::info!(
        alias = %alias,
        target_t = index_t,
        duration_ms = duration_ms,
        root_address = %root_address,
        flakes = stats_flakes,
        "Index build complete"
    );

    Ok(IndexResult {
        root_address,
        index_t,
        alias: alias.to_string(),
        stats: total_stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, MemoryStorage, Sid};

    fn make_test_flakes() -> Vec<Flake> {
        vec![
            Flake::new(
                Sid::new(1, "alice"),
                Sid::new(2, "name"),
                FlakeValue::String("Alice".to_string()),
                Sid::new(3, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(1, "bob"),
                Sid::new(2, "name"),
                FlakeValue::String("Bob".to_string()),
                Sid::new(3, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(1, "alice"),
                Sid::new(2, "knows"),
                FlakeValue::Ref(Sid::new(1, "bob")),
                Sid::new(1, "id"),
                2,
                true,
                None,
            ),
        ]
    }

    #[tokio::test]
    async fn test_build_index_basic() {
        let storage = MemoryStorage::new();
        let flakes = make_test_flakes();
        let namespace_codes: BTreeMap<i32, String> = BTreeMap::from([
            (1, "http://example.org/".to_string()),
            (2, "http://example.org/prop/".to_string()),
            (3, "http://www.w3.org/2001/XMLSchema#".to_string()),
        ]);

        let result = build_index(
            &storage,
            "test/main",
            2,
            flakes,
            namespace_codes,
            IndexerConfig::small(),
        )
        .await
        .unwrap();

        assert_eq!(result.alias, "test/main");
        assert_eq!(result.index_t, 2);
        assert_eq!(result.stats.flake_count, 3);
        assert!(result.stats.leaf_count > 0);
        assert!(!result.root_address.is_empty());
    }
}
