//! Index build orchestrator: merges run files into per-graph leaf/branch indexes.
//!
//! Top-level entry point for Phase C. Ties together:
//! - Language tag reconciliation
//! - StreamingRunReader streams
//! - K-way merge with dedup
//! - Per-graph LeafWriter + BranchManifest

use super::branch::write_branch_manifest;
use super::dict_io::{read_predicate_dict, write_language_dict};
use super::lang_remap::build_lang_remap;
use super::leaf::{LeafInfo, LeafWriter};
use super::leaflet::p_width_for_max;
use super::merge::KWayMerge;
use super::run_record::{cmp_for_order, RunSortOrder};
use super::streaming_reader::StreamingRunReader;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for building an index from run files.
#[derive(Debug, Clone)]
pub struct IndexBuildConfig {
    /// Directory containing run files for this order (run_*.frn).
    pub run_dir: PathBuf,
    /// Directory containing shared dict files (predicates.dict, datatypes.dict).
    /// For single-order builds, this equals `run_dir`.
    /// For multi-order builds, this is the parent `base_run_dir`.
    pub dicts_dir: PathBuf,
    /// Output directory for per-graph indexes.
    pub index_dir: PathBuf,
    /// Sort order for this index.
    pub sort_order: RunSortOrder,
    /// Target rows per leaflet (default: 5000).
    pub leaflet_rows: usize,
    /// Leaflets per leaf file (default: 10).
    pub leaflets_per_leaf: usize,
    /// zstd compression level (default: 1 for build speed).
    pub zstd_level: i32,
    /// Whether to persist the unified language dict to disk.
    /// Set to false when called from `build_all_indexes` (which pre-computes it).
    pub persist_lang_dict: bool,
}

impl Default for IndexBuildConfig {
    fn default() -> Self {
        Self {
            run_dir: PathBuf::from("."),
            dicts_dir: PathBuf::from("."),
            index_dir: PathBuf::from("index"),
            sort_order: RunSortOrder::Spot,
            leaflet_rows: 25000,
            leaflets_per_leaf: 10,
            zstd_level: 1,
            persist_lang_dict: true,
        }
    }
}

// ============================================================================
// Results
// ============================================================================

/// Result for a single graph's index build.
#[derive(Debug)]
pub struct GraphIndexResult {
    pub g_id: u32,
    pub leaf_count: u32,
    pub total_rows: u64,
    /// Content hash (SHA-256 hex) of the branch manifest file.
    pub branch_hash: String,
    pub graph_dir: PathBuf,
}

/// Result of the full index build.
#[derive(Debug)]
pub struct IndexBuildResult {
    pub graphs: Vec<GraphIndexResult>,
    pub total_rows: u64,
    pub index_dir: PathBuf,
    pub elapsed: Duration,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors from the index build pipeline.
#[derive(Debug)]
pub enum IndexBuildError {
    Io(io::Error),
    NoRunFiles,
}

impl From<io::Error> for IndexBuildError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for IndexBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O: {}", e),
            Self::NoRunFiles => write!(f, "no run files found in run_dir"),
        }
    }
}

impl std::error::Error for IndexBuildError {}

// ============================================================================
// build_index: main entry point
// ============================================================================

/// Build per-graph indexes from sorted run files for a single sort order.
///
/// # Pipeline
///
/// 1. Discover run files in `run_dir` (glob `run_*.frn`).
/// 2. Build unified language tag dictionary + per-run remap tables.
/// 3. Open streaming readers for all run files.
/// 4. Create k-way merge with deduplication (using order-specific comparator).
/// 5. Pull records; detect `g_id` transitions to split per-graph.
/// 6. For each graph: write leaf files + branch manifest.
/// 7. Write per-order index manifest.
pub fn build_index(config: IndexBuildConfig) -> Result<IndexBuildResult, IndexBuildError> {
    let order = config.sort_order;
    let order_name = order.dir_name();
    let start = Instant::now();
    let _span = tracing::info_span!("build_index", order = order_name).entered();

    // ---- Step 1: Discover run files ----
    let mut run_paths = discover_run_files(&config.run_dir)?;
    if run_paths.is_empty() {
        return Err(IndexBuildError::NoRunFiles);
    }
    run_paths.sort();
    tracing::info!(run_files = run_paths.len(), order = order_name, "discovered run files");

    // Ensure output directory exists
    std::fs::create_dir_all(&config.index_dir)?;

    // ---- Step 2: Language tag reconciliation ----
    let (unified_lang_dict, remaps) = build_lang_remap(&run_paths)?;
    tracing::info!(
        unified_lang_tags = unified_lang_dict.len(),
        "language tag reconciliation complete"
    );

    if config.persist_lang_dict {
        let lang_dict_path = config.dicts_dir.join("languages.dict");
        write_language_dict(&lang_dict_path, &unified_lang_dict)?;
        tracing::info!(
            path = %lang_dict_path.display(),
            tags = unified_lang_dict.len(),
            "language dict persisted"
        );
    }

    // ---- Step 2b: Compute per-leaf field widths from dict sizes ----
    let pred_dict_path = config.dicts_dir.join("predicates.dict");
    let dt_dict_path = config.dicts_dir.join("datatypes.dict");
    let p_width = if pred_dict_path.exists() {
        let pred_dict = read_predicate_dict(&pred_dict_path)?;
        let w = p_width_for_max(pred_dict.len().saturating_sub(1));
        tracing::info!(predicates = pred_dict.len(), p_width = w, "predicate width");
        w
    } else {
        2 // default u16
    };
    // Datatypes are u8 by design in the new binary format.
    // Enforce <= 256 entries and always encode dt as a single byte.
    let dt_width = if dt_dict_path.exists() {
        let dt_dict = read_predicate_dict(&dt_dict_path)?;
        if dt_dict.len() > 256 {
            return Err(IndexBuildError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "datatype dict too large for u8 ids: {} (max 256)",
                    dt_dict.len()
                ),
            )));
        }
        tracing::info!(datatypes = dt_dict.len(), dt_width = 1, "datatype width (fixed u8)");
        1
    } else {
        1
    };

    // ---- Step 3: Open streaming readers ----
    let mut streams = Vec::with_capacity(run_paths.len());
    for (i, path) in run_paths.iter().enumerate() {
        let remap = remaps[i].clone();
        let reader = StreamingRunReader::open(path, remap)?;
        tracing::debug!(
            run = i,
            records = reader.record_count(),
            path = %path.display(),
            "opened stream"
        );
        streams.push(reader);
    }

    // ---- Step 4: Create k-way merge ----
    let mut merge = KWayMerge::new(streams, cmp_for_order(order))?;

    // ---- Step 5-6: Pull records, split by g_id, write indexes ----
    let mut graph_results: Vec<GraphIndexResult> = Vec::new();
    let mut total_rows: u64 = 0;
    let mut retract_count: u64 = 0;
    let mut records_since_log: u64 = 0;
    let mut max_t: i64 = 0;

    // Current graph state
    let mut current_g_id: Option<u32> = None;
    let mut current_writer: Option<LeafWriter> = None;

    while let Some(record) = merge.next_deduped()? {
        // Snapshot semantics: if dedup selected a retract (op=0), the fact is
        // no longer asserted and must be excluded from the snapshot index.
        if record.op == 0 {
            retract_count += 1;
            continue;
        }

        // Track max t for manifest metadata.
        if record.t > max_t {
            max_t = record.t;
        }

        let g_id = record.g_id;

        // Detect g_id transition
        if current_g_id != Some(g_id) {
            // Finish previous graph (if any)
            if let Some(writer) = current_writer.take() {
                let leaf_infos = writer.finish()?;
                if let Some(prev_g_id) = current_g_id {
                    let result = finish_graph(
                        prev_g_id,
                        leaf_infos,
                        &config.index_dir,
                        order_name,
                    )?;
                    tracing::info!(
                        g_id = prev_g_id,
                        leaves = result.leaf_count,
                        rows = result.total_rows,
                        order = order_name,
                        "graph index complete"
                    );
                    graph_results.push(result);
                }
            }

            // Start new graph
            let graph_dir = config.index_dir.join(format!("graph_{}/{}", g_id, order_name));
            std::fs::create_dir_all(&graph_dir)?;

            tracing::info!(g_id, path = %graph_dir.display(), "starting graph index");

            current_writer = Some(LeafWriter::with_widths(
                graph_dir,
                config.leaflet_rows,
                config.leaflets_per_leaf,
                config.zstd_level,
                dt_width,
                p_width,
                order,
            ));
            current_g_id = Some(g_id);
        }

        // Push record to current writer
        current_writer.as_mut().unwrap().push_record(record)?;
        total_rows += 1;
        records_since_log += 1;

        if records_since_log >= 10_000_000 {
            tracing::info!(
                total_rows,
                g_id,
                elapsed_s = start.elapsed().as_secs(),
                "merge progress"
            );
            records_since_log = 0;
        }
    }

    // Finish last graph
    if let Some(writer) = current_writer.take() {
        let leaf_infos = writer.finish()?;
        if let Some(g_id) = current_g_id {
            let result = finish_graph(g_id, leaf_infos, &config.index_dir, order_name)?;
            tracing::info!(
                g_id,
                leaves = result.leaf_count,
                rows = result.total_rows,
                order = order_name,
                "graph index complete"
            );
            graph_results.push(result);
        }
    }

    // ---- Step 7: Write per-order manifest ----
    write_index_manifest(&config.index_dir, &graph_results, total_rows, max_t, order_name)?;

    let elapsed = start.elapsed();
    tracing::info!(
        graphs = graph_results.len(),
        total_rows,
        retract_count,
        max_t,
        order = order_name,
        elapsed_s = elapsed.as_secs(),
        "index build complete"
    );

    Ok(IndexBuildResult {
        graphs: graph_results,
        total_rows,
        index_dir: config.index_dir,
        elapsed,
    })
}

/// Build per-graph SPOT indexes from sorted run files.
///
/// Convenience wrapper around `build_index` with `sort_order = Spot`
/// and `dicts_dir = run_dir` (single-directory layout).
pub fn build_spot_index(config: IndexBuildConfig) -> Result<IndexBuildResult, IndexBuildError> {
    let dicts_dir = config.run_dir.clone();
    build_index(IndexBuildConfig {
        dicts_dir,
        sort_order: RunSortOrder::Spot,
        persist_lang_dict: true,
        ..config
    })
}

// ============================================================================
// Helpers
// ============================================================================

/// Discover run files in a directory (sorted by name).
fn discover_run_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "frn")
            && path
                .file_stem()
                .map_or(false, |s| s.to_string_lossy().starts_with("run_"))
        {
            paths.push(path);
        }
    }
    Ok(paths)
}

/// Finish a graph: write content-addressed branch manifest, return result.
fn finish_graph(
    g_id: u32,
    leaf_infos: Vec<LeafInfo>,
    index_dir: &Path,
    order_dir: &str,
) -> io::Result<GraphIndexResult> {
    let graph_dir = index_dir.join(format!("graph_{}/{}", g_id, order_dir));

    let total_rows: u64 = leaf_infos.iter().map(|l| l.total_rows).sum();
    let leaf_count = leaf_infos.len() as u32;

    let branch_hash = write_branch_manifest(&graph_dir, &leaf_infos)?;

    Ok(GraphIndexResult {
        g_id,
        leaf_count,
        total_rows,
        branch_hash,
        graph_dir,
    })
}

/// Write a simple JSON index manifest for a given order.
fn write_index_manifest(
    index_dir: &Path,
    graphs: &[GraphIndexResult],
    total_rows: u64,
    max_t: i64,
    order_name: &str,
) -> io::Result<()> {
    let manifest_path = index_dir.join(format!("index_manifest_{}.json", order_name));
    let mut json = String::from("{\n");
    json.push_str(&format!("  \"order\": \"{}\",\n", order_name));
    json.push_str(&format!("  \"total_rows\": {},\n", total_rows));
    json.push_str(&format!("  \"max_t\": {},\n", max_t));
    json.push_str(&format!("  \"graph_count\": {},\n", graphs.len()));
    json.push_str("  \"graphs\": [\n");
    for (i, g) in graphs.iter().enumerate() {
        json.push_str("    {\n");
        json.push_str(&format!("      \"g_id\": {},\n", g.g_id));
        json.push_str(&format!("      \"leaf_count\": {},\n", g.leaf_count));
        json.push_str(&format!("      \"total_rows\": {},\n", g.total_rows));
        json.push_str(&format!("      \"branch_hash\": \"{}\",\n", g.branch_hash));
        json.push_str(&format!(
            "      \"directory\": \"graph_{}/{}\"\n",
            g.g_id, order_name
        ));
        if i < graphs.len() - 1 {
            json.push_str("    },\n");
        } else {
            json.push_str("    }\n");
        }
    }
    json.push_str("  ]\n");
    json.push_str("}\n");

    std::fs::write(&manifest_path, &json)?;
    Ok(())
}

// ============================================================================
// build_all_indexes: parallel multi-order orchestrator
// ============================================================================

/// Build indexes for multiple sort orders in parallel using `std::thread::scope`.
///
/// Each order is built in its own thread — zero contention because each reads
/// from its own run files and writes to its own output directory.
///
/// The unified language dictionary is pre-computed from SPOT run files and
/// written to `base_run_dir/languages.dict` before spawning threads.
pub fn build_all_indexes(
    base_run_dir: &Path,
    index_dir: &Path,
    orders: &[RunSortOrder],
    leaflet_rows: usize,
    leaflets_per_leaf: usize,
    zstd_level: i32,
) -> Result<Vec<(RunSortOrder, IndexBuildResult)>, IndexBuildError> {
    let _span = tracing::info_span!("build_all_indexes").entered();
    let start = Instant::now();

    // Pre-compute unified language dict from SPOT run files
    let spot_run_dir = base_run_dir.join("spot");
    if spot_run_dir.exists() {
        let mut spot_runs = discover_run_files(&spot_run_dir)?;
        if !spot_runs.is_empty() {
            spot_runs.sort();
            let (unified_lang_dict, _) = build_lang_remap(&spot_runs)?;
            let lang_dict_path = base_run_dir.join("languages.dict");
            write_language_dict(&lang_dict_path, &unified_lang_dict)?;
            tracing::info!(
                tags = unified_lang_dict.len(),
                path = %lang_dict_path.display(),
                "pre-computed unified language dict"
            );
        }
    }

    // Spawn one thread per order
    let results = std::thread::scope(|s| {
        let handles: Vec<_> = orders
            .iter()
            .filter_map(|&order| {
                let run_dir = base_run_dir.join(order.dir_name());
                if !run_dir.exists() {
                    tracing::warn!(order = ?order, dir = %run_dir.display(), "run dir missing, skipping");
                    return None;
                }

                let base = base_run_dir.to_path_buf();
                let idx_dir = index_dir.to_path_buf();

                Some((order, s.spawn(move || {
                    let config = IndexBuildConfig {
                        run_dir,
                        dicts_dir: base,
                        index_dir: idx_dir,
                        sort_order: order,
                        leaflet_rows,
                        leaflets_per_leaf,
                        zstd_level,
                        persist_lang_dict: false, // already pre-computed
                    };
                    build_index(config)
                })))
            })
            .collect();

        let mut results = Vec::new();
        for (order, handle) in handles {
            let result = handle.join().expect("build thread panicked")?;
            results.push((order, result));
        }
        Ok::<_, IndexBuildError>(results)
    })?;

    tracing::info!(
        orders = results.len(),
        elapsed_s = start.elapsed().as_secs(),
        "all indexes built"
    );

    Ok(results)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::global_dict::LanguageTagDict;
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::{cmp_spot, RunRecord};
    use crate::run_index::global_dict::dt_ids;
    use fluree_db_core::value_id::ValueId;

    fn make_record(g_id: u32, s_id: u32, p_id: u32, val: i64, t: i64) -> RunRecord {
        RunRecord::new(
            g_id, s_id, p_id,
            ValueId::num_int(val).unwrap(),
            t, true, dt_ids::INTEGER, 0, None,
        )
    }

    fn write_sorted_run(dir: &Path, name: &str, mut records: Vec<RunRecord>) -> PathBuf {
        records.sort_unstable_by(cmp_spot);
        let path = dir.join(name);
        let lang_dict = LanguageTagDict::new();
        let (min_t, max_t) = if records.is_empty() {
            (0, 0)
        } else {
            records
                .iter()
                .fold((i64::MAX, i64::MIN), |(min, max), r| {
                    (min.min(r.t), max.max(r.t))
                })
        };
        write_run_file(&path, &records, &lang_dict, RunSortOrder::Spot, min_t, max_t)
            .unwrap();
        path
    }

    #[test]
    fn test_end_to_end_small() {
        let dir = std::env::temp_dir().join("fluree_test_index_build_e2e");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Create 2 run files with overlapping data
        write_sorted_run(&run_dir, "run_00000.frn", vec![
            make_record(0, 1, 1, 10, 1),
            make_record(0, 2, 1, 20, 1),
            make_record(0, 3, 1, 30, 1),
        ]);
        write_sorted_run(&run_dir, "run_00001.frn", vec![
            make_record(0, 4, 1, 40, 1),
            make_record(0, 5, 1, 50, 1),
        ]);

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            leaflet_rows: 3,
            leaflets_per_leaf: 2,
            zstd_level: 1,
            ..Default::default()
        };

        let result = build_spot_index(config).unwrap();

        assert_eq!(result.total_rows, 5);
        assert_eq!(result.graphs.len(), 1);
        assert_eq!(result.graphs[0].g_id, 0);
        assert_eq!(result.graphs[0].total_rows, 5);
        assert!(result.graphs[0].leaf_count >= 1);

        // Verify files exist
        let branch_path = result.graphs[0].graph_dir.join(format!("{}.fbr", result.graphs[0].branch_hash));
        assert!(branch_path.exists());
        assert!(index_dir.join("index_manifest_spot.json").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_graph() {
        let dir = std::env::temp_dir().join("fluree_test_index_build_multi_g");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Records in graph 0 and graph 1 (sorted by cmp_spot: g_id first)
        write_sorted_run(&run_dir, "run_00000.frn", vec![
            make_record(0, 1, 1, 10, 1),
            make_record(0, 2, 1, 20, 1),
            make_record(1, 1, 1, 100, 1),
            make_record(1, 2, 1, 200, 1),
        ]);

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            leaflet_rows: 2,
            leaflets_per_leaf: 10,
            zstd_level: 1,
            ..Default::default()
        };

        let result = build_spot_index(config).unwrap();

        assert_eq!(result.total_rows, 4);
        assert_eq!(result.graphs.len(), 2);
        assert_eq!(result.graphs[0].g_id, 0);
        assert_eq!(result.graphs[0].total_rows, 2);
        assert_eq!(result.graphs[1].g_id, 1);
        assert_eq!(result.graphs[1].total_rows, 2);

        // Verify separate graph directories have branch files
        let g0_branch = result.graphs[0].graph_dir.join(format!("{}.fbr", result.graphs[0].branch_hash));
        let g1_branch = result.graphs[1].graph_dir.join(format!("{}.fbr", result.graphs[1].branch_hash));
        assert!(g0_branch.exists());
        assert!(g1_branch.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_in_build() {
        let dir = std::env::temp_dir().join("fluree_test_index_build_dedup");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Same fact at t=1 in run 0, and t=2 in run 1 → dedup keeps t=2
        write_sorted_run(&run_dir, "run_00000.frn", vec![
            make_record(0, 1, 1, 10, 1),
        ]);
        write_sorted_run(&run_dir, "run_00001.frn", vec![
            make_record(0, 1, 1, 10, 2),
        ]);

        let config = IndexBuildConfig {
            run_dir,
            index_dir,
            leaflet_rows: 100,
            leaflets_per_leaf: 10,
            zstd_level: 1,
            ..Default::default()
        };

        let result = build_spot_index(config).unwrap();

        // After dedup: only 1 record
        assert_eq!(result.total_rows, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
