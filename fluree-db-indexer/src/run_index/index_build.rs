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
use super::run_record::{cmp_for_order, RunRecord, RunSortOrder};
use super::streaming_reader::StreamingRunReader;
use fluree_db_core::value_id::ObjKind;
use rustc_hash::FxHashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Batch size for progress counter updates. Instead of a per-row atomic
/// fetch_add, accumulate locally and flush every N rows. Reduces cache-line
/// contention across cores. 4096 chosen as a sweet spot: small enough for
/// responsive progress reporting, large enough to amortize the atomic.
const PROGRESS_BATCH_SIZE: u64 = 4096;

#[inline]
fn create_graph_dir(index_dir: &Path, g_id: u16, order_name: &str) -> io::Result<PathBuf> {
    let graph_dir = index_dir.join(format!("graph_{}/{}", g_id, order_name));
    std::fs::create_dir_all(&graph_dir)?;
    Ok(graph_dir)
}

/// Parameters for constructing a [`LeafWriter`] for a single graph order.
struct LeafWriterParams {
    graph_dir: PathBuf,
    leaflet_rows: usize,
    leaflets_per_leaf: usize,
    zstd_level: i32,
    dt_width: u8,
    p_width: u8,
    order: RunSortOrder,
    skip_region3: bool,
}

#[inline]
fn new_leaf_writer_for_graph(params: LeafWriterParams) -> LeafWriter {
    let mut writer = LeafWriter::with_widths(
        params.graph_dir,
        params.leaflet_rows,
        params.leaflets_per_leaf,
        params.zstd_level,
        params.dt_width,
        params.p_width,
        params.order,
    );
    writer.set_skip_region3(params.skip_region3);
    writer
}

#[inline]
fn finish_open_graph(
    current_g_id: &mut Option<u16>,
    current_writer: &mut Option<LeafWriter>,
    graph_results: &mut Vec<GraphIndexResult>,
    index_dir: &Path,
    order_name: &str,
    mut on_writer_pre_finish: impl FnMut(&LeafWriter),
    mut on_graph_finished: impl FnMut(&GraphIndexResult),
) -> io::Result<()> {
    let Some(writer) = current_writer.take() else {
        return Ok(());
    };

    on_writer_pre_finish(&writer);
    let leaf_infos = writer.finish()?;

    let g_id = current_g_id
        .take()
        .ok_or_else(|| io::Error::other("writer finished but current_g_id was None"))?;

    let result = finish_graph(g_id as u32, leaf_infos, index_dir, order_name)?;
    on_graph_finished(&result);
    graph_results.push(result);
    Ok(())
}

#[inline]
fn start_graph(
    current_g_id: &mut Option<u16>,
    current_writer: &mut Option<LeafWriter>,
    g_id: u16,
    index_dir: &Path,
    order_name: &str,
    make_writer: impl FnOnce(PathBuf) -> LeafWriter,
    mut on_graph_started: impl FnMut(u16, &Path),
) -> io::Result<()> {
    let graph_dir = create_graph_dir(index_dir, g_id, order_name)?;
    on_graph_started(g_id, &graph_dir);
    *current_writer = Some(make_writer(graph_dir));
    *current_g_id = Some(g_id);
    Ok(())
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for building an index from run files.
#[derive(Debug, Clone)]
pub struct IndexBuildConfig {
    /// Directory containing run files for this order (run_*.frn).
    pub run_dir: PathBuf,
    /// Directory containing shared dict files (predicates.json, datatypes.dict).
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
    /// Shared counter incremented for each row merged in this order.
    /// Only attached to a single order (POST) by `build_all_indexes` so that
    /// the reported total matches the actual flake count, not flakes × orders.
    pub progress: Option<Arc<AtomicU64>>,
    /// Skip deduplication during merge. Safe for fresh bulk import where each
    /// chunk produces unique asserts and there are no retractions.
    pub skip_dedup: bool,
    /// Skip Region 3 (history journal) in leaflets. Safe for append-only
    /// bulk import where all ops are asserts with unique `t` values.
    pub skip_region3: bool,
    /// Override g_id for all records read from run files. Run files use a
    /// 34-byte wire format that does not carry g_id (each graph's indexes
    /// are in separate directories). When building per-graph indexes from
    /// pre-partitioned run files, set this to the actual graph ID so that
    /// the output directory is `graph_{g_id}/{order}/`.
    pub g_id_override: Option<u16>,
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
            progress: None,
            skip_dedup: false,
            skip_region3: false,
            g_id_override: None,
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
    tracing::info!(
        run_files = run_paths.len(),
        order = order_name,
        "discovered run files"
    );

    build_index_from_run_paths_inner(config, run_paths, start, order, order_name)
}

/// Build an index for a single order from an explicit list of run files.
///
/// This is used by import pipelines that write runs into
/// `{base_run_dir}/chunk_{idx}/{order}/run_*.frn` (chunk-isolated layout),
/// to avoid flattening run files via symlinks/hardlinks.
pub fn build_index_from_run_paths(
    config: IndexBuildConfig,
    mut run_paths: Vec<PathBuf>,
) -> Result<IndexBuildResult, IndexBuildError> {
    if run_paths.is_empty() {
        return Err(IndexBuildError::NoRunFiles);
    }
    run_paths.sort();

    let order = config.sort_order;
    let order_name = order.dir_name();
    let start = Instant::now();
    let _span = tracing::info_span!("build_index", order = order_name).entered();

    tracing::info!(
        run_files = run_paths.len(),
        order = order_name,
        "discovered run files"
    );

    build_index_from_run_paths_inner(config, run_paths, start, order, order_name)
}

fn build_index_from_run_paths_inner(
    config: IndexBuildConfig,
    run_paths: Vec<PathBuf>,
    start: Instant,
    order: RunSortOrder,
    order_name: &str,
) -> Result<IndexBuildResult, IndexBuildError> {
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
    let pred_ids_path = config.dicts_dir.join("predicates.json");
    let dt_dict_path = config.dicts_dir.join("datatypes.dict");
    let p_width = if pred_ids_path.exists() {
        let bytes = std::fs::read(&pred_ids_path)?;
        let preds: Vec<String> = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let w = p_width_for_max(preds.len().saturating_sub(1) as u32);
        tracing::info!(predicates = preds.len(), p_width = w, "predicate width");
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
        tracing::info!(
            datatypes = dt_dict.len(),
            dt_width = 1,
            "datatype width (fixed u8)"
        );
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
    let mut progress_batch: u64 = 0;
    let mut max_t: u32 = 0;
    let perf_enabled = std::env::var("FLUREE_INDEX_BUILD_PERF")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut perf_leaflets_encoded: u64 = 0;
    let mut perf_leaflet_records: u64 = 0;
    let mut perf_region3_time = Duration::ZERO;
    let mut perf_leaflet_encode_time = Duration::ZERO;
    let mut perf_leaves_flushed: u64 = 0;
    let mut perf_leaf_bytes_written: u64 = 0;
    let mut perf_leaf_flush_time = Duration::ZERO;

    // Current graph state
    let mut current_g_id: Option<u16> = None;
    let mut current_writer: Option<LeafWriter> = None;
    let skip_dedup = config.skip_dedup;
    // Scratch buffer reused across merge iterations to collect non-winning
    // duplicates (history entries for Region 3).
    let mut history_scratch: Vec<super::run_record::RunRecord> = Vec::new();

    loop {
        let record = if skip_dedup {
            merge.next_record()?
        } else {
            history_scratch.clear();
            merge.next_deduped_with_history(&mut history_scratch)?
        };
        let Some(mut record) = record else { break };

        // When building per-graph indexes from pre-partitioned run files,
        // stamp the actual graph ID (run wire format sets g_id=0).
        if let Some(override_id) = config.g_id_override {
            record.g_id = override_id;
        }

        // Track max t for manifest metadata (must happen before retraction
        // skip so that retraction-only commits are still reflected in max_t).
        if record.t > max_t {
            max_t = record.t;
        }

        // Detect g_id transition BEFORE retract handling — ensures the correct
        // graph's writer is active before any R3 history entries are written.
        let g_id = record.g_id;

        if current_g_id != Some(g_id) {
            finish_open_graph(
                &mut current_g_id,
                &mut current_writer,
                &mut graph_results,
                &config.index_dir,
                order_name,
                |writer| {
                    if perf_enabled {
                        if let Some(p) = writer.perf() {
                            perf_leaflets_encoded += p.leaflets_encoded;
                            perf_leaflet_records += p.leaflet_records;
                            perf_region3_time += p.region3_build_time;
                            perf_leaflet_encode_time += p.leaflet_encode_time;
                            perf_leaves_flushed += p.leaves_flushed;
                            perf_leaf_bytes_written += p.leaf_bytes_written;
                            perf_leaf_flush_time += p.leaf_flush_time;
                        }
                    }
                },
                |result| {
                    tracing::info!(
                        g_id = result.g_id,
                        leaves = result.leaf_count,
                        rows = result.total_rows,
                        order = order_name,
                        "graph index complete"
                    );
                },
            )?;

            start_graph(
                &mut current_g_id,
                &mut current_writer,
                g_id,
                &config.index_dir,
                order_name,
                |graph_dir| {
                    new_leaf_writer_for_graph(LeafWriterParams {
                        graph_dir,
                        leaflet_rows: config.leaflet_rows,
                        leaflets_per_leaf: config.leaflets_per_leaf,
                        zstd_level: config.zstd_level,
                        dt_width,
                        p_width,
                        order,
                        skip_region3: config.skip_region3,
                    })
                },
                |g_id, graph_dir| {
                    tracing::info!(g_id, path = %graph_dir.display(), "starting graph index");
                },
            )?;
        }

        // Retract-winner handling: no R1 row, but log the retract event + history to R3.
        if record.op == 0 {
            retract_count += 1;
            if !config.skip_region3 {
                let writer = current_writer
                    .as_mut()
                    .ok_or_else(|| io::Error::other("missing leaf writer for current graph"))?;
                writer.push_history_only(&record, &history_scratch)?;
            }
            continue;
        }

        // Push record (R1) with associated history entries (R3) to current writer.
        let writer = current_writer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing leaf writer for current graph"))?;
        writer.push_record_with_history(record, &history_scratch)?;
        total_rows += 1;
        records_since_log += 1;
        progress_batch += 1;
        if progress_batch >= PROGRESS_BATCH_SIZE {
            if let Some(ref ctr) = config.progress {
                ctr.fetch_add(progress_batch, Ordering::Relaxed);
            }
            progress_batch = 0;
        }

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

    // Flush remaining progress batch
    if progress_batch > 0 {
        if let Some(ref ctr) = config.progress {
            ctr.fetch_add(progress_batch, Ordering::Relaxed);
        }
    }

    // Finish last graph
    finish_open_graph(
        &mut current_g_id,
        &mut current_writer,
        &mut graph_results,
        &config.index_dir,
        order_name,
        |writer| {
            if perf_enabled {
                if let Some(p) = writer.perf() {
                    perf_leaflets_encoded += p.leaflets_encoded;
                    perf_leaflet_records += p.leaflet_records;
                    perf_region3_time += p.region3_build_time;
                    perf_leaflet_encode_time += p.leaflet_encode_time;
                    perf_leaves_flushed += p.leaves_flushed;
                    perf_leaf_bytes_written += p.leaf_bytes_written;
                    perf_leaf_flush_time += p.leaf_flush_time;
                }
            }
        },
        |result| {
            tracing::info!(
                g_id = result.g_id,
                leaves = result.leaf_count,
                rows = result.total_rows,
                order = order_name,
                "graph index complete"
            );
        },
    )?;

    // ---- Step 7: Write per-order manifest ----
    write_index_manifest(
        &config.index_dir,
        &graph_results,
        total_rows,
        max_t,
        order_name,
    )?;

    let elapsed = start.elapsed();
    if perf_enabled {
        let mbytes = perf_leaf_bytes_written as f64 / (1024.0 * 1024.0);
        tracing::info!(
            order = order_name,
            leaflets_encoded = perf_leaflets_encoded,
            leaflet_records = perf_leaflet_records,
            region3_s = perf_region3_time.as_secs_f64(),
            leaflet_encode_s = perf_leaflet_encode_time.as_secs_f64(),
            leaves_flushed = perf_leaves_flushed,
            leaf_flush_s = perf_leaf_flush_time.as_secs_f64(),
            leaf_mb_written = mbytes,
            "index build perf (leaf/leaflet breakdown)"
        );
    }
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
// SPOT merge from sorted commit files (Phase C)
// ============================================================================

// ---- Class statistics collected during SPOT merge ----

/// Sentinel datatype value used in [`SpotClassStats`] for object-reference
/// properties (`ObjKind::REF_ID`). Displayed as `@id` in stats output.
pub const DT_REF_ID: u16 = u16::MAX;

/// Class→property→datatype statistics collected during the SPOT merge.
///
/// Exploits SPOT ordering (subject-grouped) to compute class statistics
/// with O(properties-per-subject) memory per subject group. Only the global
/// accumulators grow with distinct classes/properties.
///
/// # Datatype keys
///
/// The inner `u16` key is a `DatatypeDictId` for literal values, or
/// [`DT_REF_ID`] (`u16::MAX`) for object references (`@id`).
#[derive(Debug, Default)]
pub struct SpotClassStats {
    /// class sid64 → instance count (number of subjects with this rdf:type)
    pub class_counts: FxHashMap<u64, u64>,
    /// class sid64 → p_id → dt → flake count
    pub class_prop_dts: FxHashMap<u64, FxHashMap<u32, FxHashMap<u16, u64>>>,
    /// class sid64 → p_id → target_class sid64 → count
    ///
    /// For each REF_ID property on a typed subject, records what classes the
    /// *target* subject belongs to. Built using the [`ClassBitsetTable`] which
    /// maps every subject to its class membership bitset.
    pub class_prop_refs: FxHashMap<u64, FxHashMap<u32, FxHashMap<u64, u64>>>,
}

/// Internal streaming collector for class stats during SPOT merge.
///
/// Records must arrive in SPOT order (subject-grouped). On subject transition,
/// accumulated per-subject class membership and property usage are flushed
/// into the global `result` accumulators.
struct SpotClassStatsCollector {
    rdf_type_p_id: u32,
    current_s_id: Option<u64>,
    current_g_id: u16,
    /// Classes the current subject belongs to (rdf:type REF_ID targets).
    classes: Vec<u64>,
    /// Per-property datatype counts for current subject: (p_id, dt) → count.
    prop_dts: FxHashMap<(u32, u16), u64>,
    /// REF_ID targets per property for the current subject: (p_id, target_sid).
    /// Collected so we can resolve target classes at flush time via the bitset.
    ref_targets: Vec<(u32, u64)>,
    /// Optional bitset table for resolving target classes.
    class_bitset: Option<ClassBitsetTable>,
    /// Global accumulators.
    result: SpotClassStats,
}

impl SpotClassStatsCollector {
    fn new(rdf_type_p_id: u32, class_bitset: Option<ClassBitsetTable>) -> Self {
        Self {
            rdf_type_p_id,
            current_s_id: None,
            current_g_id: 0,
            classes: Vec::new(),
            prop_dts: FxHashMap::default(),
            ref_targets: Vec::new(),
            class_bitset,
            result: SpotClassStats::default(),
        }
    }

    /// Process a single record from the SPOT merge.
    ///
    /// Must only be called with asserted records (op == 1) — retracted records
    /// are already filtered by the merge loop.
    #[inline]
    fn on_record(&mut self, rec: &RunRecord) {
        let s_id = rec.s_id.as_u64();
        // Flush on subject or graph transition.
        if self.current_s_id != Some(s_id) || self.current_g_id != rec.g_id {
            self.flush_subject();
            self.current_s_id = Some(s_id);
            self.current_g_id = rec.g_id;
        }

        if rec.p_id == self.rdf_type_p_id && rec.o_kind == ObjKind::REF_ID.as_u8() {
            // rdf:type assertion → record class membership.
            self.classes.push(rec.o_key);
        } else {
            // Regular property → track (p_id, dt) pair.
            let is_ref = rec.o_kind == ObjKind::REF_ID.as_u8();
            let dt = if is_ref { DT_REF_ID } else { rec.dt };
            *self.prop_dts.entry((rec.p_id, dt)).or_insert(0) += 1;

            // Collect REF_ID targets for ref-target class resolution at flush time.
            if is_ref && self.class_bitset.is_some() {
                self.ref_targets.push((rec.p_id, rec.o_key));
            }
        }
    }

    /// Flush accumulated per-subject state into global class accumulators.
    fn flush_subject(&mut self) {
        if self.classes.is_empty() {
            self.prop_dts.clear();
            self.ref_targets.clear();
            return;
        }
        for &class_sid in &self.classes {
            *self.result.class_counts.entry(class_sid).or_insert(0) += 1;
            let class_entry = self.result.class_prop_dts.entry(class_sid).or_default();
            for (&(p_id, dt), &count) in &self.prop_dts {
                *class_entry.entry(p_id).or_default().entry(dt).or_insert(0) += count;
            }
        }

        // Resolve ref-target classes: for each REF_ID property, look up the
        // target subject's class bitset (scoped to current graph) and attribute
        // to source classes.
        if let Some(ref bitset) = self.class_bitset {
            for &(p_id, target_sid) in &self.ref_targets {
                let target_bits = bitset.get(self.current_g_id, target_sid);
                if target_bits == 0 {
                    continue; // Target has no known classes.
                }
                // Expand bitset into individual target classes.
                for &src_class in &self.classes {
                    let ref_entry = self
                        .result
                        .class_prop_refs
                        .entry(src_class)
                        .or_default()
                        .entry(p_id)
                        .or_default();
                    let mut bits = target_bits;
                    while bits != 0 {
                        let bit_idx = bits.trailing_zeros() as usize;
                        let target_class = bitset.bit_to_class[bit_idx];
                        *ref_entry.entry(target_class).or_insert(0) += 1;
                        bits &= bits - 1; // Clear lowest set bit.
                    }
                }
            }
        }

        self.classes.clear();
        self.prop_dts.clear();
        self.ref_targets.clear();
    }

    /// Consume the collector, flushing any remaining subject, and return results.
    fn finish(mut self) -> SpotClassStats {
        self.flush_subject();
        self.result
    }
}

// ---- Subject→class bitset table (built from types-map sidecars) ----

/// Dense per-graph, per-namespace subject→class bitset table.
///
/// Each subject can belong to up to 64 classes (one bit per class in a `u64`).
/// Built from Phase A `.types` sidecars after Phase B vocab merge provides
/// the global subject remap tables. Used during the SPOT merge to look up
/// class membership for *both* the source subject and REF_ID targets.
///
/// Graph-scoped: a subject's class membership is specific to the graph where
/// the `rdf:type` assertion appeared. The SPOT merge passes the current
/// `g_id` when looking up class bits.
///
/// Within each graph, the table is indexed by `sid64` decomposition:
/// `ns_code = sid >> 48`, `local_id = sid & 0x0000_FFFF_FFFF_FFFF`.
/// Per-namespace arrays are dense — untyped subjects have bitset value 0.
pub struct ClassBitsetTable {
    /// Class SID → bit index (0..63). Only the first 64 classes get bits.
    /// Used during `build_from_type_maps`; kept for potential future lookups
    /// (e.g., checking whether a specific class is tracked).
    #[expect(dead_code)]
    class_to_bit: FxHashMap<u64, u8>,
    /// Bit index → class SID (for output formatting).
    pub bit_to_class: Vec<u64>,
    /// Per-graph, per-namespace bitset arrays.
    /// `graph_bitsets[g_id][ns_code][local_id]` → class membership bitset.
    graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>>,
}

impl ClassBitsetTable {
    /// Look up the class bitset for a subject in a specific graph.
    /// Returns 0 if untyped or if the graph has no type data.
    #[inline]
    pub fn get(&self, g_id: u16, sid: u64) -> u64 {
        let ns_code = (sid >> 48) as u16;
        let local_id = (sid & 0x0000_FFFF_FFFF_FFFF) as usize;
        self.graph_bitsets
            .get(&g_id)
            .and_then(|ns| ns.get(&ns_code))
            .and_then(|v| v.get(local_id).copied())
            .unwrap_or(0)
    }

    /// Number of classes tracked (≤ 64).
    pub fn class_count(&self) -> usize {
        self.bit_to_class.len()
    }

    /// Build from types-map sidecars + per-chunk subject remap tables.
    ///
    /// Each entry in `inputs` is `(types_map_path, subject_remap)` for one chunk.
    /// The sidecar contains 18-byte entries: `(g_id: u16, s_sorted_local: u64,
    /// class_sorted_local: u64)`. Subject remap converts sorted-local → global sid64.
    ///
    /// Class bits are assigned dynamically (first-come, first-served), capped at 64.
    /// Subjects with classes beyond the 64th are partially tracked (only the first
    /// 64 class assignments produce bits).
    pub fn build_from_type_maps(
        inputs: &[(&Path, &dyn super::spool::SubjectRemap)],
    ) -> io::Result<Self> {
        use std::io::{BufReader, Read};

        let mut class_to_bit: FxHashMap<u64, u8> = FxHashMap::default();
        let mut bit_to_class: Vec<u64> = Vec::new();
        let mut graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>> = FxHashMap::default();

        let mut buf = [0u8; 18];
        for &(path, remap) in inputs {
            let file = std::fs::File::open(path)?;
            let file_len = file.metadata()?.len();
            let entry_count = file_len / 18;
            let mut reader = BufReader::new(file);

            for _ in 0..entry_count {
                reader.read_exact(&mut buf)?;

                // Parse sidecar entry (LE): g_id(2) + s_sorted_local(8) + class_sorted_local(8)
                let g_id = u16::from_le_bytes([buf[0], buf[1]]);
                let s_local = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let c_local = u64::from_le_bytes(buf[10..18].try_into().unwrap());

                let s_global = remap.get(s_local as usize)?;
                let c_global = remap.get(c_local as usize)?;

                // Assign class bit (capped at 64).
                let bit_idx = if let Some(&idx) = class_to_bit.get(&c_global) {
                    idx
                } else if bit_to_class.len() < 64 {
                    let idx = bit_to_class.len() as u8;
                    class_to_bit.insert(c_global, idx);
                    bit_to_class.push(c_global);
                    idx
                } else {
                    continue; // > 64 classes — skip this entry
                };

                // Set bit in subject's per-graph bitset.
                let ns_code = (s_global >> 48) as u16;
                let local_id = (s_global & 0x0000_FFFF_FFFF_FFFF) as usize;
                const MAX_LOCAL_ID: usize = 256 * 1024 * 1024; // 256M subjects per namespace
                if local_id > MAX_LOCAL_ID {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "class bitset: local_id {} exceeds cap ({}) — corrupt remap?",
                            local_id, MAX_LOCAL_ID
                        ),
                    ));
                }
                let ns_map = graph_bitsets.entry(g_id).or_default();
                let vec = ns_map.entry(ns_code).or_default();
                if local_id >= vec.len() {
                    vec.resize(local_id + 1, 0);
                }
                vec[local_id] |= 1u64 << bit_idx;
            }
        }

        tracing::info!(
            classes = bit_to_class.len(),
            graphs = graph_bitsets.len(),
            total_subjects = graph_bitsets
                .values()
                .flat_map(|ns| ns.values())
                .map(|v| v.len())
                .sum::<usize>(),
            "class bitset table built"
        );

        Ok(Self {
            class_to_bit,
            bit_to_class,
            graph_bitsets,
        })
    }
}

/// Input for one sorted commit file in `build_spot_from_sorted_commits`.
pub struct SortedCommitInput {
    /// Path to the sorted commit file (.fsc).
    pub commit_path: PathBuf,
    /// Subject remap: sorted-local → global sid64.
    pub subject_remap: Box<dyn super::spool::SubjectRemap + Send>,
    /// String remap: sorted-local → global string ID.
    pub string_remap: Box<dyn super::spool::StringRemap + Send>,
    /// Language tag remap: chunk-local lang_id → global lang_id.
    /// `remap[0] = 0` (sentinel). Empty means no remap needed.
    pub lang_remap: Vec<u16>,
}

/// Configuration for building SPOT indexes from sorted commit files.
pub struct SpotFromCommitsConfig {
    /// Output directory for per-graph indexes.
    pub index_dir: PathBuf,
    /// Predicate field width in bytes (1, 2, or 4).
    pub p_width: u8,
    /// Datatype field width in bytes (1 or 2, computed from datatype dict size).
    pub dt_width: u8,
    /// Target rows per leaflet.
    pub leaflet_rows: usize,
    /// Leaflets per leaf file.
    pub leaflets_per_leaf: usize,
    /// zstd compression level.
    pub zstd_level: i32,
    /// Progress counter (incremented per row merged).
    pub progress: Option<Arc<AtomicU64>>,
    /// Skip deduplication during merge. Safe for fresh bulk import where each
    /// chunk produces unique asserts and there are no retractions.
    pub skip_dedup: bool,
    /// Skip Region 3 (history journal) in leaflets. Safe for append-only
    /// bulk import where all ops are asserts with unique `t` values.
    pub skip_region3: bool,
    /// Predicate ID for rdf:type. When set, enables inline class→property→datatype
    /// statistics collection during the merge. The returned [`SpotClassStats`]
    /// will contain per-class instance counts and property/datatype breakdowns.
    pub rdf_type_p_id: Option<u32>,
    /// Optional subject→class bitset table. When provided alongside `rdf_type_p_id`,
    /// enables ref-target class attribution: for each REF_ID property, the target
    /// subject's classes are looked up and recorded in [`SpotClassStats::class_prop_refs`].
    pub class_bitset: Option<ClassBitsetTable>,
}

/// Build per-graph SPOT indexes by k-way merging sorted commit files.
///
/// Each commit file contains records sorted by `(g_id, SPOT)` with
/// sorted-position chunk-local IDs. The per-commit remap tables (from the
/// Phase B vocabulary merge) are applied on-the-fly during the merge via
/// [`StreamingSortedCommitReader`].
///
/// Because the chunk-local IDs are assigned in canonical order and the
/// global IDs are assigned in the same order (monotone remap), the
/// per-commit SPOT sort order is preserved after remap. This makes the
/// k-way merge correct — it produces globally-sorted `(g_id, SPOT)` output.
///
/// Graph transitions (`g_id` changes) create per-graph index segments,
/// each with its own leaf files and branch manifest.
pub fn build_spot_from_sorted_commits(
    inputs: Vec<SortedCommitInput>,
    config: SpotFromCommitsConfig,
) -> Result<(IndexBuildResult, Option<SpotClassStats>), IndexBuildError> {
    use super::run_record::cmp_g_spot;
    use super::sorted_commit_reader::StreamingSortedCommitReader;

    let order = RunSortOrder::Spot;
    let order_name = "spot";
    let start = Instant::now();
    let _span = tracing::info_span!("build_spot_from_commits").entered();

    if inputs.is_empty() {
        return Err(IndexBuildError::NoRunFiles);
    }

    std::fs::create_dir_all(&config.index_dir)?;

    // Open streaming readers with per-commit remaps.
    let mut streams = Vec::with_capacity(inputs.len());
    for (i, input) in inputs.into_iter().enumerate() {
        let reader = StreamingSortedCommitReader::open(
            &input.commit_path,
            input.subject_remap,
            input.string_remap,
            input.lang_remap,
        )?;
        tracing::debug!(
            commit = i,
            records = reader.record_count(),
            path = %input.commit_path.display(),
            "opened sorted commit stream"
        );
        streams.push(reader);
    }

    // K-way merge with graph-prefixed SPOT comparator.
    let mut merge = KWayMerge::new(streams, cmp_g_spot)?;

    // Optionally collect class→property→datatype stats during merge.
    let mut class_stats_collector = config
        .rdf_type_p_id
        .map(|p_id| SpotClassStatsCollector::new(p_id, config.class_bitset));

    // Merge loop: pull records, split by g_id, write per-graph indexes.
    let mut graph_results: Vec<GraphIndexResult> = Vec::new();
    let mut total_rows: u64 = 0;
    let mut retract_count: u64 = 0;
    let mut progress_batch: u64 = 0;
    let mut max_t: u32 = 0;
    let skip_dedup = config.skip_dedup;

    let mut current_g_id: Option<u16> = None;
    let mut current_writer: Option<LeafWriter> = None;
    let mut history_scratch: Vec<RunRecord> = Vec::new();

    loop {
        let record = if skip_dedup {
            merge.next_record()?
        } else {
            history_scratch.clear();
            merge.next_deduped_with_history(&mut history_scratch)?
        };
        let Some(record) = record else { break };

        if record.t > max_t {
            max_t = record.t;
        }

        // Detect g_id transition BEFORE retract handling.
        let g_id = record.g_id;

        if current_g_id != Some(g_id) {
            finish_open_graph(
                &mut current_g_id,
                &mut current_writer,
                &mut graph_results,
                &config.index_dir,
                order_name,
                |_writer| {},
                |result| {
                    tracing::info!(
                        g_id = result.g_id,
                        leaves = result.leaf_count,
                        rows = result.total_rows,
                        "graph complete"
                    );
                },
            )?;

            start_graph(
                &mut current_g_id,
                &mut current_writer,
                g_id,
                &config.index_dir,
                order_name,
                |graph_dir| {
                    new_leaf_writer_for_graph(LeafWriterParams {
                        graph_dir,
                        leaflet_rows: config.leaflet_rows,
                        leaflets_per_leaf: config.leaflets_per_leaf,
                        zstd_level: config.zstd_level,
                        dt_width: config.dt_width,
                        p_width: config.p_width,
                        order,
                        skip_region3: config.skip_region3,
                    })
                },
                |_g_id, _graph_dir| {},
            )?;
        }

        // Retract-winner handling: no R1 row, but log the retract event + history to R3.
        if record.op == 0 {
            retract_count += 1;
            if !config.skip_region3 {
                let writer = current_writer
                    .as_mut()
                    .ok_or_else(|| io::Error::other("missing leaf writer for current graph"))?;
                writer.push_history_only(&record, &history_scratch)?;
            }
            continue;
        }

        // Feed asserted record to class stats collector (before writing to leaf).
        if let Some(ref mut collector) = class_stats_collector {
            collector.on_record(&record);
        }

        let writer = current_writer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing leaf writer for current graph"))?;
        writer.push_record_with_history(record, &history_scratch)?;
        total_rows += 1;
        progress_batch += 1;
        if progress_batch >= PROGRESS_BATCH_SIZE {
            if let Some(ref ctr) = config.progress {
                ctr.fetch_add(progress_batch, Ordering::Relaxed);
            }
            progress_batch = 0;
        }
    }

    // Flush remaining progress batch
    if progress_batch > 0 {
        if let Some(ref ctr) = config.progress {
            ctr.fetch_add(progress_batch, Ordering::Relaxed);
        }
    }

    // Finish last graph
    finish_open_graph(
        &mut current_g_id,
        &mut current_writer,
        &mut graph_results,
        &config.index_dir,
        order_name,
        |_writer| {},
        |result| {
            tracing::info!(
                g_id = result.g_id,
                leaves = result.leaf_count,
                rows = result.total_rows,
                "graph complete"
            );
        },
    )?;

    // Finalize class stats collector (flushes last subject).
    let class_stats = class_stats_collector.map(|c| {
        let stats = c.finish();
        let ref_classes = stats
            .class_prop_refs
            .values()
            .flat_map(|props| props.values().flat_map(|targets| targets.keys()))
            .collect::<std::collections::HashSet<_>>()
            .len();
        tracing::info!(
            classes = stats.class_counts.len(),
            ref_target_classes = ref_classes,
            "class stats collected during merge"
        );
        stats
    });

    write_index_manifest(
        &config.index_dir,
        &graph_results,
        total_rows,
        max_t,
        order_name,
    )?;

    let elapsed = start.elapsed();
    tracing::info!(
        graphs = graph_results.len(),
        total_rows,
        retract_count,
        max_t,
        elapsed_s = elapsed.as_secs(),
        "index build from sorted commits complete"
    );

    Ok((
        IndexBuildResult {
            graphs: graph_results,
            total_rows,
            index_dir: config.index_dir,
            elapsed,
        },
        class_stats,
    ))
}

// ============================================================================
// Helpers
// ============================================================================

/// Discover run files in a directory (sorted by name).
pub fn discover_run_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "frn")
            && path
                .file_stem()
                .is_some_and(|s| s.to_string_lossy().starts_with("run_"))
        {
            paths.push(path);
        }
    }
    Ok(paths)
}

/// Discover run files in the "chunk-isolated" layout:
/// `{base_run_dir}/chunk_{idx}/{order}/run_*.frn`.
fn discover_chunked_run_files(
    base_run_dir: &Path,
    order: RunSortOrder,
) -> io::Result<Vec<PathBuf>> {
    let mut chunk_dirs: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(base_run_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with("chunk_") {
            chunk_dirs.push(path);
        }
    }
    chunk_dirs.sort();

    let mut out = Vec::new();
    for chunk_dir in chunk_dirs {
        let order_dir = chunk_dir.join(order.dir_name());
        if !order_dir.exists() {
            continue;
        }
        out.extend(discover_run_files(&order_dir)?);
    }
    Ok(out)
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
    max_t: u32,
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
/// Pre-compute the unified language dictionary from SPOT run files.
///
/// Writes `languages.dict` to `base_run_dir`. Safe to call before
/// `build_all_indexes()` — the index build will skip re-computing
/// if the file already exists.
pub fn precompute_language_dict(base_run_dir: &Path) -> Result<(), IndexBuildError> {
    let lang_dict_path = base_run_dir.join("languages.dict");
    let spot_run_dir = base_run_dir.join("spot");
    let mut spot_runs = if spot_run_dir.exists() {
        discover_run_files(&spot_run_dir)?
    } else {
        discover_chunked_run_files(base_run_dir, RunSortOrder::Spot)?
    };

    if !spot_runs.is_empty() {
        spot_runs.sort();
        let (unified_lang_dict, _) = build_lang_remap(&spot_runs)?;
        write_language_dict(&lang_dict_path, &unified_lang_dict)?;
        tracing::info!(
            tags = unified_lang_dict.len(),
            path = %lang_dict_path.display(),
            "pre-computed unified language dict"
        );
    }
    Ok(())
}

///
/// The unified language dictionary is pre-computed from SPOT run files and
/// written to `base_run_dir/languages.dict` before spawning threads.
#[allow(clippy::too_many_arguments)]
pub fn build_all_indexes(
    base_run_dir: &Path,
    index_dir: &Path,
    orders: &[RunSortOrder],
    leaflet_rows: usize,
    leaflets_per_leaf: usize,
    zstd_level: i32,
    progress: Option<Arc<AtomicU64>>,
    skip_dedup: bool,
    skip_region3: bool,
) -> Result<Vec<(RunSortOrder, IndexBuildResult)>, IndexBuildError> {
    let _span = tracing::info_span!("build_all_indexes").entered();
    let start = Instant::now();

    // Pre-compute unified language dict (skips if already done).
    let lang_dict_path = base_run_dir.join("languages.dict");
    if !lang_dict_path.exists() {
        precompute_language_dict(base_run_dir)?;
    }

    // Spawn one thread per order
    let results = std::thread::scope(|s| {
        let handles: Vec<_> = orders
            .iter()
            .filter_map(|&order| {
                let run_dir = base_run_dir.join(order.dir_name());
                let mut run_paths = match if run_dir.exists() {
                    discover_run_files(&run_dir)
                } else {
                    discover_chunked_run_files(base_run_dir, order)
                } {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(
                            order = ?order,
                            base = %base_run_dir.display(),
                            error = %e,
                            "failed to discover run files for order, skipping"
                        );
                        return None;
                    }
                };

                if run_paths.is_empty() {
                    tracing::warn!(
                        order = ?order,
                        base = %base_run_dir.display(),
                        "no run files discovered for order, skipping"
                    );
                    return None;
                }
                run_paths.sort();

                let base = base_run_dir.to_path_buf();
                let idx_dir = index_dir.to_path_buf();
                // Only attach the progress counter to POST — it's the longest-running
                // order and its row count equals the total flake count, so the
                // caller can use cumulative_flakes (not ×4) as the denominator.
                let order_progress = if order == RunSortOrder::Post {
                    progress.clone()
                } else {
                    None
                };

                Some((
                    order,
                    s.spawn(move || {
                        let config = IndexBuildConfig {
                            run_dir,
                            dicts_dir: base,
                            index_dir: idx_dir,
                            sort_order: order,
                            leaflet_rows,
                            leaflets_per_leaf,
                            zstd_level,
                            persist_lang_dict: false, // already pre-computed
                            progress: order_progress,
                            skip_dedup,
                            skip_region3,
                            g_id_override: None,
                        };
                        build_index_from_run_paths(config, run_paths)
                    }),
                ))
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
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;

    fn make_record(g_id: u16, s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            g_id,
            fluree_db_core::subject_id::SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    fn write_sorted_run(dir: &Path, name: &str, mut records: Vec<RunRecord>) -> PathBuf {
        records.sort_unstable_by(cmp_spot);
        let path = dir.join(name);
        let lang_dict = LanguageTagDict::new();
        let (min_t, max_t) = if records.is_empty() {
            (0u32, 0u32)
        } else {
            records.iter().fold((u32::MAX, 0u32), |(min, max), r| {
                (min.min(r.t), max.max(r.t))
            })
        };
        write_run_file(
            &path,
            &records,
            &lang_dict,
            RunSortOrder::Spot,
            min_t,
            max_t,
        )
        .unwrap();
        path
    }

    #[test]
    fn test_end_to_end_small() {
        let dir = std::env::temp_dir().join("fluree_test_index_build_e2e");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("tmp_import");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Create 2 run files with overlapping data
        write_sorted_run(
            &run_dir,
            "run_00000.frn",
            vec![
                make_record(0, 1, 1, 10, 1),
                make_record(0, 2, 1, 20, 1),
                make_record(0, 3, 1, 30, 1),
            ],
        );
        write_sorted_run(
            &run_dir,
            "run_00001.frn",
            vec![make_record(0, 4, 1, 40, 1), make_record(0, 5, 1, 50, 1)],
        );

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
        let branch_path = result.graphs[0]
            .graph_dir
            .join(format!("{}.fbr", result.graphs[0].branch_hash));
        assert!(branch_path.exists());
        assert!(index_dir.join("index_manifest_spot.json").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_graph() {
        // Run files use the 34-byte wire format which does NOT include g_id.
        // Multi-graph partitioning requires the spool pipeline (36-byte format
        // with g_id). Here we write two separate run files — one per graph —
        // and verify that both are indexed into a single graph (g_id=0) because
        // all records read from run files have g_id=0.
        let dir = std::env::temp_dir().join("fluree_test_index_build_multi_g");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("tmp_import");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Two run files with distinct subjects (g_id param ignored in run wire format)
        write_sorted_run(
            &run_dir,
            "run_00000.frn",
            vec![make_record(0, 1, 1, 10, 1), make_record(0, 2, 1, 20, 1)],
        );
        write_sorted_run(
            &run_dir,
            "run_00001.frn",
            vec![make_record(0, 3, 1, 100, 1), make_record(0, 4, 1, 200, 1)],
        );

        let config = IndexBuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            leaflet_rows: 2,
            leaflets_per_leaf: 10,
            zstd_level: 1,
            ..Default::default()
        };

        let result = build_spot_index(config).unwrap();

        // All records land in graph 0 (run files do not carry g_id)
        assert_eq!(result.total_rows, 4);
        assert_eq!(result.graphs.len(), 1);
        assert_eq!(result.graphs[0].g_id, 0);
        assert_eq!(result.graphs[0].total_rows, 4);

        // Verify branch file exists
        let g0_branch = result.graphs[0]
            .graph_dir
            .join(format!("{}.fbr", result.graphs[0].branch_hash));
        assert!(g0_branch.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_in_build() {
        let dir = std::env::temp_dir().join("fluree_test_index_build_dedup");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("tmp_import");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();

        // Same fact at t=1 in run 0, and t=2 in run 1 → dedup keeps t=2
        write_sorted_run(&run_dir, "run_00000.frn", vec![make_record(0, 1, 1, 10, 1)]);
        write_sorted_run(&run_dir, "run_00001.frn", vec![make_record(0, 1, 1, 10, 2)]);

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

    #[test]
    fn test_spot_from_sorted_commits() {
        use crate::run_index::chunk_dict::{ChunkStringDict, ChunkSubjectDict};
        use crate::run_index::run_record::LIST_INDEX_NONE;
        use crate::run_index::spool::sort_remap_and_write_sorted_commit;
        use fluree_db_core::subject_id::SubjectId;

        let dir = std::env::temp_dir().join("fluree_test_spot_from_commits");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let index_dir = dir.join("index");

        // Chunk 0: subjects A, C (ns=10)
        let mut subj0 = ChunkSubjectDict::new();
        subj0.get_or_insert(10, b"A"); // sorted 0
        subj0.get_or_insert(10, b"C"); // sorted 1
        let str0 = ChunkStringDict::new();

        let recs0 = vec![
            RunRecord {
                g_id: 0,
                s_id: SubjectId::from_u64(0),
                p_id: 10,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(1).as_u64(),
                t: 1,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: LIST_INDEX_NONE,
            },
            RunRecord {
                g_id: 0,
                s_id: SubjectId::from_u64(1),
                p_id: 10,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(3).as_u64(),
                t: 1,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: LIST_INDEX_NONE,
            },
        ];

        sort_remap_and_write_sorted_commit(
            recs0,
            subj0,
            str0,
            &dir.join("subj0.voc"),
            &dir.join("str0.voc"),
            &dir.join("commit_0.fsc"),
            0,
            None,
            None,
        )
        .unwrap();

        // Chunk 1: subjects B, D (ns=10)
        let mut subj1 = ChunkSubjectDict::new();
        subj1.get_or_insert(10, b"B"); // sorted 0
        subj1.get_or_insert(10, b"D"); // sorted 1
        let str1 = ChunkStringDict::new();

        let recs1 = vec![
            RunRecord {
                g_id: 0,
                s_id: SubjectId::from_u64(0),
                p_id: 10,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(2).as_u64(),
                t: 2,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: LIST_INDEX_NONE,
            },
            RunRecord {
                g_id: 0,
                s_id: SubjectId::from_u64(1),
                p_id: 10,
                o_kind: ObjKind::NUM_INT.as_u8(),
                o_key: ObjKey::encode_i64(4).as_u64(),
                t: 2,
                op: 1,
                dt: DatatypeDictId::INTEGER.as_u16(),
                lang_id: 0,
                i: LIST_INDEX_NONE,
            },
        ];

        sort_remap_and_write_sorted_commit(
            recs1,
            subj1,
            str1,
            &dir.join("subj1.voc"),
            &dir.join("str1.voc"),
            &dir.join("commit_1.fsc"),
            1,
            None,
            None,
        )
        .unwrap();

        // Global remaps (monotone):
        // Chunk 0: sorted 0 (A) → 100, sorted 1 (C) → 300
        // Chunk 1: sorted 0 (B) → 200, sorted 1 (D) → 400
        let inputs = vec![
            SortedCommitInput {
                commit_path: dir.join("commit_0.fsc"),
                subject_remap: Box::new(vec![100u64, 300]),
                string_remap: Box::new(Vec::<u32>::new()),
                lang_remap: vec![],
            },
            SortedCommitInput {
                commit_path: dir.join("commit_1.fsc"),
                subject_remap: Box::new(vec![200u64, 400]),
                string_remap: Box::new(Vec::<u32>::new()),
                lang_remap: vec![],
            },
        ];

        let config = SpotFromCommitsConfig {
            index_dir: index_dir.clone(),
            p_width: 2,
            dt_width: 1,
            leaflet_rows: 100,
            leaflets_per_leaf: 10,
            zstd_level: 1,
            progress: None,
            skip_dedup: false,
            skip_region3: false,
            rdf_type_p_id: None,
            class_bitset: None,
        };

        let (result, _class_stats) = build_spot_from_sorted_commits(inputs, config).unwrap();

        assert_eq!(result.total_rows, 4);
        assert_eq!(result.graphs.len(), 1);
        assert_eq!(result.graphs[0].g_id, 0);
        assert_eq!(result.graphs[0].total_rows, 4);

        // Verify branch file exists
        let branch_path = result.graphs[0]
            .graph_dir
            .join(format!("{}.fbr", result.graphs[0].branch_hash));
        assert!(branch_path.exists());
        assert!(index_dir.join("index_manifest_spot.json").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
