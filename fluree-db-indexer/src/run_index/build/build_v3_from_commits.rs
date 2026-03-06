//! V3 index build from sorted commit files.
//!
//! Orchestrates the full V3 pipeline: remap sorted commits → V2 run files →
//! k-way merge → FLI3/FBR3 artifacts. Operates synchronously within a
//! `spawn_blocking` context.
//!
//! This replaces both `build_spot_from_sorted_commits()` (Phase C) and
//! `build_all_indexes()` (Phase E) from the V1 pipeline with a single
//! unified function that builds ALL orders from V2 run files.

use crate::run_index::build::index_build_v2::{
    build_all_indexes_v2, BuildAllV2Config, IndexBuildV2Result,
};
use crate::run_index::runs::run_writer_v2::{
    MultiOrderRunWriterV2, MultiOrderRunWriterV2WithOp, MultiOrderV2Config,
};
use crate::run_index::runs::spool::MmapStringRemap;
use crate::run_index::runs::spool::MmapSubjectRemap;
use crate::run_index::runs::spool_v2::{remap_commit_to_runs_v2, remap_commit_to_runs_v2_with_op};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_core::o_type_registry::OTypeRegistry;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Input for a single sorted commit chunk.
pub struct V3CommitInput {
    /// Path to the sorted commit file (.fsc).
    pub commit_path: PathBuf,
    /// Number of records in the commit file.
    pub record_count: u64,
    /// Path to the mmap'd subject remap file.
    pub subject_remap_path: PathBuf,
    /// Path to the mmap'd string remap file.
    pub string_remap_path: PathBuf,
    /// Chunk-local → global language ID remap.
    pub lang_remap: Vec<u16>,
}

/// Configuration for the V3 build-from-commits pipeline.
pub struct V3BuildConfig {
    /// Base directory for temporary run files.
    pub run_dir: PathBuf,
    /// Output directory for per-graph index artifacts.
    pub index_dir: PathBuf,
    /// Graph ID (builds are graph-scoped).
    pub g_id: u16,
    /// Target rows per leaflet.
    pub leaflet_target_rows: usize,
    /// Target rows per leaf.
    pub leaf_target_rows: usize,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// Memory budget for run writers (bytes).
    pub run_budget_bytes: usize,
    /// Progress counter (optional).
    pub progress: Option<Arc<AtomicU64>>,
}

/// Result of the V3 build pipeline.
pub struct V3BuildResult {
    /// Per-order build results (each contains per-graph artifacts).
    pub order_results: Vec<(RunSortOrder, IndexBuildV2Result)>,
    /// Total rows across all orders (from the POST result for canonical count).
    pub total_rows: u64,
    /// Total records remapped across all chunks.
    pub total_remapped: u64,
    /// Time spent in the remap phase.
    pub remap_elapsed: std::time::Duration,
    /// Time spent in the build phase.
    pub build_elapsed: std::time::Duration,
}

/// Build V3 indexes from sorted V1 commit files.
///
/// This is the V3 equivalent of "Phase C (SPOT) + Phase D (remap) + Phase E
/// (secondary)" from the V1 import pipeline, unified into a single function
/// that handles all four orders.
///
/// Steps:
/// 1. Remap each sorted commit → V2 run files for all 4 orders
/// 2. K-way merge run files → FLI3/FBR3 per graph per order
pub fn build_v3_indexes_from_commits(
    commits: &[V3CommitInput],
    registry: &OTypeRegistry,
    config: &V3BuildConfig,
) -> io::Result<V3BuildResult> {
    // Phase 1: Remap sorted commits → V2 run files for all 4 orders.
    let remap_start = Instant::now();

    let orders = RunSortOrder::all_build_orders().to_vec();
    let mut writer = MultiOrderRunWriterV2::new(MultiOrderV2Config {
        total_budget_bytes: config.run_budget_bytes,
        orders: orders.clone(),
        base_run_dir: config.run_dir.clone(),
    })?;

    let mut total_remapped = 0u64;
    for commit in commits {
        let s_remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
        let str_remap = MmapStringRemap::open(&commit.string_remap_path)?;

        let count = remap_commit_to_runs_v2(
            &commit.commit_path,
            commit.record_count,
            &s_remap,
            &str_remap,
            &commit.lang_remap,
            registry,
            &mut writer,
        )?;
        total_remapped += count;

        // Report progress during remap.
        if let Some(ref ctr) = config.progress {
            ctr.fetch_add(count, Ordering::Relaxed);
        }
    }

    let _run_results = writer.finish()?;
    let remap_elapsed = remap_start.elapsed();

    // Phase 2: Build V3 indexes from run files.
    let build_start = Instant::now();

    let build_config = BuildAllV2Config {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: true,   // Fresh import: unique asserts.
        skip_history: true, // Append-only: no time-travel data.
        g_id: config.g_id,
        progress: None, // Progress already reported during remap.
    };

    let order_results =
        build_all_indexes_v2(&build_config).map_err(|e| io::Error::other(e.to_string()))?;

    let build_elapsed = build_start.elapsed();

    // Total rows from the POST result (canonical count — avoids double-counting).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok(V3BuildResult {
        order_results,
        total_rows,
        total_remapped,
        remap_elapsed,
        build_elapsed,
    })
}

/// Build V3 indexes from globally-remapped sorted commit files (rebuild path).
///
/// Unlike [`build_v3_indexes_from_commits`], which takes `V3CommitInput` with
/// per-chunk remap files, this function takes `SortedCommitInfo` entries whose
/// `.fsc` files already contain globally-remapped IDs (Phase C applied remap
/// in-memory). Uses `IdentitySubjectRemap` / `IdentityStringRemap` since
/// no disk remap files exist.
///
/// Key differences from the import path:
/// - Input: `&[SortedCommitInfo]` (not `&[V3CommitInput]`)
/// - Remap: identity (global IDs already in place)
/// - `skip_dedup: false` (rebuild may have retractions)
/// - `skip_history: false` (produce history sidecars for time-travel)
pub fn build_v3_indexes_from_remapped_commits(
    commit_infos: &[crate::run_index::runs::spool::SortedCommitInfo],
    registry: &OTypeRegistry,
    config: &V3BuildConfig,
) -> io::Result<V3BuildResult> {
    use crate::run_index::runs::spool::{IdentityStringRemap, IdentitySubjectRemap};

    // Phase 1: Remap sorted commits → V2 run files (with op) for all 4 orders.
    // Since records are already globally remapped, use identity remap tables.
    // The op byte (assert=1, retract=0) is preserved from V1 records so that
    // the merge/build phase can filter out retract-winners.
    let remap_start = Instant::now();

    let orders = RunSortOrder::all_build_orders().to_vec();
    let mut writer = MultiOrderRunWriterV2WithOp::new(MultiOrderV2Config {
        total_budget_bytes: config.run_budget_bytes,
        orders: orders.clone(),
        base_run_dir: config.run_dir.clone(),
    })?;

    let s_remap = IdentitySubjectRemap;
    let str_remap = IdentityStringRemap;
    let lang_remap: &[u16] = &[]; // language IDs already global

    let mut total_remapped = 0u64;
    for info in commit_infos {
        let count = remap_commit_to_runs_v2_with_op(
            &info.path,
            info.record_count,
            &s_remap,
            &str_remap,
            lang_remap,
            registry,
            &mut writer,
        )?;
        total_remapped += count;

        if let Some(ref ctr) = config.progress {
            ctr.fetch_add(count, Ordering::Relaxed);
        }
    }

    let _run_results = writer.finish()?;
    let remap_elapsed = remap_start.elapsed();

    // Phase 2: Build V3 indexes from run files.
    let build_start = Instant::now();

    let build_config = BuildAllV2Config {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: false,   // Rebuild: must deduplicate (max-t wins).
        skip_history: false, // Produce history sidecars for time-travel.
        g_id: config.g_id,
        progress: None, // Progress already reported during remap.
    };

    let order_results =
        build_all_indexes_v2(&build_config).map_err(|e| io::Error::other(e.to_string()))?;

    let build_elapsed = build_start.elapsed();

    // Total rows from the POST result (canonical count).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok(V3BuildResult {
        order_results,
        total_rows,
        total_remapped,
        remap_elapsed,
        build_elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::spool::SpoolWriter;
    use fluree_db_binary_index::format::leaf_v3::{decode_leaf_dir_v3, decode_leaf_header_v3};
    use fluree_db_binary_index::format::run_record::RunRecord;

    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    #[test]
    fn end_to_end_v3_build() {
        let dir = std::env::temp_dir().join("fluree_test_v3_e2e");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // Create a minimal sorted commit file with a few records.
        // Using SpoolWriter to create a proper FSP2 spool file.
        let commit_path = dir.join("commit_00000.fsc");

        // For this test, we write records directly in sorted order.
        // We need identity remap files too (just identity: index → same value).
        let records = vec![
            RunRecord::new(
                0,
                SubjectId(1),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(10),
                1,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(2),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(20),
                2,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(3),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(5),
                3,
                true,
                1,
                0,
                None,
            ),
        ];

        // Write spool file.
        let mut spool = SpoolWriter::new(&commit_path, 0).unwrap();
        for rec in &records {
            spool.push(rec).unwrap();
        }
        let spool_info = spool.finish().unwrap();

        // Write identity remap files.
        // Subject remap: 4 entries (0-indexed), identity mapping.
        let subj_remap_path = dir.join("subjects_00000.rmp");
        let subj_data: Vec<u8> = (0u64..4).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&subj_remap_path, &subj_data).unwrap();

        // String remap: 10 entries, identity mapping.
        let str_remap_path = dir.join("strings_00000.rmp");
        let str_data: Vec<u8> = (0u32..10).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&str_remap_path, &str_data).unwrap();

        let commits = vec![V3CommitInput {
            commit_path,
            record_count: spool_info.record_count,
            subject_remap_path: subj_remap_path,
            string_remap_path: str_remap_path,
            lang_remap: vec![],
        }];

        let registry = OTypeRegistry::builtin_only();
        let config = V3BuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            g_id: 0,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            run_budget_bytes: 256 * 1024,
            progress: None,
        };

        let result = build_v3_indexes_from_commits(&commits, &registry, &config).unwrap();

        // Should have results for all 4 orders.
        assert_eq!(result.order_results.len(), 4);
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.total_remapped, 3);

        // Verify POST has predicate-homogeneous leaflets.
        let post_result = result
            .order_results
            .iter()
            .find(|(o, _)| *o == RunSortOrder::Post)
            .unwrap();
        let post_graphs = &post_result.1.graphs;
        assert_eq!(post_graphs.len(), 1);
        assert_eq!(post_graphs[0].g_id, 0);

        // Check the POST leaf has 2 leaflets (p_id=1 and p_id=2).
        let post_leaf = &post_graphs[0].leaf_infos[0];
        let header = decode_leaf_header_v3(&post_leaf.leaf_bytes).unwrap();
        assert_eq!(header.order, RunSortOrder::Post);
        let leaf_dir = decode_leaf_dir_v3(&post_leaf.leaf_bytes, &header).unwrap();
        assert_eq!(leaf_dir.len(), 2); // p_id=1 and p_id=2
        assert_eq!(leaf_dir[0].p_const, Some(1));
        assert_eq!(leaf_dir[1].p_const, Some(2));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
