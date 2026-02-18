//! Bulk import pipeline: TTL → commits → spool → merge → remap → runs → indexes → CAS → publish.
//!
//! Provides `.create("mydb").import("/path/to/chunks/").execute().await` API
//! on [`Fluree`] for high-throughput bulk import of Turtle data.
//!
//! ## Pipeline overview (Tier 2: parallel local IDs + remap)
//!
//! 1. **Create ledger** — `nameservice.publish_ledger_init(ledger_id)`
//! 2. **Parse + commit** — parallel chunk parsing with chunk-local IDs written
//!    to spool files, serial commit finalization
//! 3. **Dict merge** — merge chunk-local subject/string dicts into global dicts,
//!    produce per-chunk remap tables
//! 4. **Parallel remap** — N threads read spool files, remap IDs to global,
//!    write sorted run files
//! 5. **Build indexes** — `build_all_indexes()` from completed run files
//! 6. **CAS upload** — dicts + indexes uploaded to content-addressed storage
//! 7. **IRB1 root** — `IndexRootV5` encoded and written to CAS
//! 8. **Publish** — `nameservice.publish_index_allow_equal()`
//! 9. **Cleanup** — remove tmp session directory (only on full success)
//!
//! ## Architecture
//!
//! Parse workers resolve subjects/strings to **chunk-local IDs** via per-chunk
//! dictionaries (`ChunkSubjectDict`, `ChunkStringDict`), while predicates,
//! datatypes, and graphs use globally-assigned IDs via `SharedDictAllocator`.
//! After all chunks are parsed, a merge pass deduplicates across chunks and
//! builds remap tables. Parallel remap threads then convert chunk-local IDs to
//! global IDs and produce sorted run files for the index builder.
//!
//! Commits are finalized in strict serial order (`t` increments by 1 per chunk)
//! even though chunk parsing is parallel.

use crate::error::ApiError;
use fluree_db_core::{
    ContentId, ContentKind, ContentStore, Storage, CODEC_FLUREE_INDEX_ROOT,
    CODEC_FLUREE_STATS_SKETCH,
};
use fluree_db_nameservice::{NameService, Publisher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// Configuration
// ============================================================================

/// Progress event emitted at key points during the import pipeline.
#[derive(Debug, Clone)]
pub enum ImportPhase {
    /// Reader thread is scanning through the file (emitted periodically).
    Scanning {
        /// Bytes of data read so far (excludes prefix block header).
        bytes_read: u64,
        /// Total data bytes in the file.
        total_bytes: u64,
    },
    /// Chunk parsing started (emitted before chunk 0 serial parse).
    Parsing {
        chunk: usize,
        total: usize,
        chunk_bytes: u64,
    },
    /// Chunk committed during phase 2.
    Committing {
        chunk: usize,
        total: usize,
        cumulative_flakes: u64,
        elapsed_secs: f64,
    },
    /// Index preparation stage (Tier 2): merge/persist/remap/link runs.
    ///
    /// Emitted before `Indexing` begins so the CLI doesn't appear to "hang" at 0%.
    PreparingIndex {
        /// Human-readable stage label (static string for cheap cloning).
        stage: &'static str,
    },
    /// Index build in progress — reports flakes merged across all sort orders.
    Indexing {
        /// Flakes merged so far (summed across all active sort orders).
        merged_flakes: u64,
        /// Total flakes to merge (flakes * number of sort orders).
        total_flakes: u64,
        /// Seconds elapsed since indexing started.
        elapsed_secs: f64,
    },
    /// Pipeline complete.
    Done,
}

/// Callback type for import progress events.
pub type ProgressFn = Arc<dyn Fn(ImportPhase) + Send + Sync>;

/// Configuration for the bulk import pipeline.
#[derive(Clone)]
pub struct ImportConfig {
    /// Number of parallel TTL parse threads. Default: available parallelism (capped at 6).
    pub parse_threads: usize,
    /// Whether to build multi-order indexes after runs. Default: true.
    pub build_index: bool,
    /// Whether to publish to nameservice after index build. Default: true.
    pub publish: bool,
    /// Whether to delete session tmp dir on success. Default: true.
    pub cleanup_local_files: bool,
    /// Whether to zstd-compress commit blobs. Default: true.
    pub compress_commits: bool,
    /// Whether to collect ID-based stats during commit resolution. Default: true.
    ///
    /// When enabled, the import resolver performs per-op stats collection (HLL NDV,
    /// datatype counts, and optional class/property attribution) while resolving commit
    /// blobs to run records. This can be CPU-intensive and may reduce peak import
    /// throughput, but produces richer `stats.json` for the query planner.
    ///
    /// When disabled, `stats.json` falls back to cheaper summaries derived from the
    /// SPOT index build results (flake counts only).
    pub collect_id_stats: bool,
    /// Publish nameservice head every N chunks during import. Default: 50.
    /// 0 disables periodic checkpoints.
    pub publish_every: usize,
    /// Overall memory budget in MB for the import pipeline. 0 = auto-detect (75% of RAM).
    ///
    /// Used to derive `chunk_size_mb` and `max_inflight_chunks` when those fields
    /// are left at 0.
    pub memory_budget_mb: usize,
    /// Chunk size in MB for splitting a single large Turtle file. 0 = derive from budget.
    pub chunk_size_mb: usize,
    /// Maximum flakes per chunk. When importing a single large file, the chunk is
    /// split at `chunk_size_mb` OR `chunk_max_flakes`, whichever triggers first.
    /// 0 = no flake-count limit (use byte size only). Default: 20_000_000.
    ///
    /// This bounds per-commit buffer memory: 20M flakes × 40 bytes ≈ 800 MB.
    /// When importing from a directory (each file = one commit), this limit is
    /// not applied — files are never split.
    pub chunk_max_flakes: usize,
    /// Maximum number of chunk texts materialized in memory simultaneously.
    /// 0 = derive from budget.
    pub max_inflight_chunks: usize,
    /// Number of records per leaflet in the index. Default: 25_000.
    /// Larger values produce fewer, bigger leaflets (less I/O, more memory per read).
    pub leaflet_rows: usize,
    /// Number of leaflets per leaf file. Default: 10.
    /// Larger values produce fewer, bigger leaf files (less tree depth, bigger reads).
    pub leaflets_per_leaf: usize,
    /// Optional progress callback invoked at key pipeline milestones.
    pub progress: Option<ProgressFn>,
}

impl std::fmt::Debug for ImportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportConfig")
            .field("parse_threads", &self.parse_threads)
            .field("memory_budget_mb", &self.memory_budget_mb)
            .field("chunk_size_mb", &self.chunk_size_mb)
            .field("chunk_max_flakes", &self.chunk_max_flakes)
            .field("progress", &self.progress.as_ref().map(|_| "..."))
            .finish_non_exhaustive()
    }
}

impl Default for ImportConfig {
    fn default() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get().min(6))
            .unwrap_or(4);
        Self {
            parse_threads: threads,
            build_index: true,
            publish: true,
            cleanup_local_files: true,
            compress_commits: true,
            collect_id_stats: true,
            publish_every: 50,
            memory_budget_mb: 0,
            chunk_size_mb: 0,
            chunk_max_flakes: 20_000_000,
            max_inflight_chunks: 0,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            progress: None,
        }
    }
}

// ============================================================================
// Memory budget derivation
// ============================================================================

/// Detect total system memory in MB. Falls back to 16 GB if detection fails.
#[cfg(feature = "native")]
pub fn detect_system_memory_mb() -> usize {
    use sysinfo::{MemoryRefreshKind, System};

    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());
    let total_bytes = sys.total_memory();

    if total_bytes == 0 {
        tracing::warn!("could not detect system memory, falling back to 16 GB");
        16 * 1024
    } else {
        (total_bytes / (1024 * 1024)) as usize
    }
}

/// Fallback: assume 16 GB when native feature is off.
#[cfg(not(feature = "native"))]
pub fn detect_system_memory_mb() -> usize {
    16 * 1024
}

impl ImportConfig {
    /// Effective memory budget in MB (auto-detected if 0).
    pub fn effective_memory_budget_mb(&self) -> usize {
        if self.memory_budget_mb > 0 {
            self.memory_budget_mb
        } else {
            let ram = detect_system_memory_mb();
            // 75% of system RAM
            (ram as f64 * 0.75) as usize
        }
    }

    /// Effective max inflight chunks (derived from budget if 0).
    pub fn effective_max_inflight(&self) -> usize {
        if self.max_inflight_chunks > 0 {
            return self.max_inflight_chunks;
        }
        let budget = self.effective_memory_budget_mb();
        if budget >= 20 * 1024 {
            3
        } else {
            2
        }
    }

    /// Effective chunk size in MB (derived from budget if 0).
    pub fn effective_chunk_size_mb(&self) -> usize {
        if self.chunk_size_mb > 0 {
            return self.chunk_size_mb;
        }
        let budget_mb = self.effective_memory_budget_mb();
        let max_inflight = self.effective_max_inflight();
        // Budget ≈ max_inflight * chunk_size * 2.5 + run_budget + 2GB (fixed overhead)
        // Solve for chunk_size: (budget - 2048) / (max_inflight * 2.5 + 1)
        let numerator = budget_mb.saturating_sub(2048) as f64;
        let denominator = max_inflight as f64 * 2.5 + 1.0;
        let raw = (numerator / denominator).floor() as usize;
        raw.clamp(128, 768)
    }

    /// Effective run budget in MB (always auto-derived from budget and parallelism).
    pub fn effective_run_budget_mb(&self) -> usize {
        let budget_mb = self.effective_memory_budget_mb();
        let chunk_size = self.effective_chunk_size_mb();
        let threads = self.parse_threads.max(1);
        // IMPORTANT: In Tier 2, we have N independent run writers (one per remap worker),
        // so the *total* run budget must scale with parallelism. Otherwise each writer
        // gets a tiny slice and flushes many small run files, exploding disk I/O.
        //
        // Heuristic:
        // - target total run budget ≈ chunk_size × threads (so each worker can buffer ~1 chunk)
        // - cap at ~50% of the overall memory budget (leave room for dicts, parsing, etc.)
        let desired_total = chunk_size.saturating_mul(threads);
        let cap = (budget_mb / 2).max(256);
        desired_total.min(cap).max(256)
    }

    /// Cap the number of concurrent "heavy" workers (remap/run generation).
    ///
    /// Phase D remap workers read pre-written sorted commit files, apply
    /// subject+string remap, sort into 3 secondary orders, and write run files.
    /// Memory per worker is bounded by `per_thread_budget_bytes` (derived from
    /// `effective_run_budget_mb() / worker_count`), so we can safely run as many workers as
    /// we have parse threads (which is already capped at CPU count, max 6).
    ///
    /// Override with `FLUREE_IMPORT_HEAVY_WORKERS=<n>`.
    pub fn effective_heavy_workers(&self) -> usize {
        if let Ok(v) = std::env::var("FLUREE_IMPORT_HEAVY_WORKERS") {
            if let Ok(n) = v.parse::<usize>() {
                return n.max(1);
            }
        }
        self.parse_threads.max(1)
    }

    /// Log all computed import settings.
    pub fn log_effective_settings(&self) {
        let budget = self.effective_memory_budget_mb();
        let chunk_size = self.effective_chunk_size_mb();
        let max_inflight = self.effective_max_inflight();
        let run_budget = self.effective_run_budget_mb();
        let parallelism = self.parse_threads;

        tracing::info!(
            memory_budget_mb = budget,
            chunk_size_mb = chunk_size,
            max_inflight = max_inflight,
            run_budget_mb = run_budget,
            parallelism = parallelism,
            "import pipeline computed settings"
        );
    }

    /// Effective settings that will be used for the import (auto-derived when not set).
    /// Callers can use this to report to the user what resources the import will use.
    pub fn effective_import_settings(&self) -> EffectiveImportSettings {
        EffectiveImportSettings {
            memory_budget_mb: self.effective_memory_budget_mb(),
            parallelism: self.parse_threads,
            chunk_size_mb: self.effective_chunk_size_mb(),
            max_inflight_chunks: self.effective_max_inflight(),
        }
    }

    /// Emit a progress event (no-op when no callback is set).
    fn emit_progress(&self, phase: ImportPhase) {
        if let Some(ref cb) = self.progress {
            cb(phase);
        }
    }
}

/// Effective import resource settings (memory budget, parallelism, chunk size, etc.).
/// Used to report to the user what the import pipeline will use when values are auto-detected.
#[derive(Debug, Clone)]
pub struct EffectiveImportSettings {
    /// Memory budget in MB (75% of system RAM when not set).
    pub memory_budget_mb: usize,
    /// Number of parallel parse threads (system cores capped at 6 when not set).
    pub parallelism: usize,
    /// Chunk size in MB for large-file splitting (derived from budget when not set).
    pub chunk_size_mb: usize,
    /// Max inflight chunks (derived from budget when not set).
    pub max_inflight_chunks: usize,
}

// ============================================================================
// Result
// ============================================================================

/// Result of a successful bulk import.
#[derive(Debug)]
pub struct ImportResult {
    /// Ledger ID.
    pub ledger_id: String,
    /// Final commit t (= number of imported chunks).
    pub t: i64,
    /// Total flake count across all commits.
    pub flake_count: u64,
    /// Content identifier of the head commit.
    pub commit_head_id: fluree_db_core::ContentId,
    /// Content identifier of the index root. `None` if `build_index == false`.
    pub root_id: Option<fluree_db_core::ContentId>,
    /// Index t (same as `t` for fresh import). 0 if `build_index == false`.
    pub index_t: i64,
    /// Optional summary of top classes, properties, and connections.
    pub summary: Option<ImportSummary>,
}

/// Lightweight summary of the imported dataset for CLI display.
#[derive(Debug)]
pub struct ImportSummary {
    /// Top classes by instance count: `(class_iri, count)`.
    pub top_classes: Vec<(String, u64)>,
    /// Top properties by flake count: `(property_iri, count)`.
    pub top_properties: Vec<(String, u64)>,
    /// Top connections by count: `(source_class, property, target_class, count)`.
    pub top_connections: Vec<(String, String, String, u64)>,
}

// ============================================================================
// Error
// ============================================================================

/// Errors from the bulk import pipeline.
#[derive(Debug)]
pub enum ImportError {
    /// Ledger creation / nameservice error.
    Api(ApiError),
    /// Storage I/O error.
    Storage(String),
    /// TTL parse / commit error.
    Transact(String),
    /// Run generation / resolver error.
    RunGeneration(String),
    /// Index build error.
    IndexBuild(String),
    /// CAS upload error.
    Upload(String),
    /// Filesystem I/O error.
    Io(std::io::Error),
    /// Chunk discovery error.
    NoChunks(String),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "api: {}", e),
            Self::Storage(msg) => write!(f, "storage: {}", msg),
            Self::Transact(msg) => write!(f, "transact: {}", msg),
            Self::RunGeneration(msg) => write!(f, "run generation: {}", msg),
            Self::IndexBuild(msg) => write!(f, "index build: {}", msg),
            Self::Upload(msg) => write!(f, "upload: {}", msg),
            Self::Io(e) => write!(f, "I/O: {}", e),
            Self::NoChunks(msg) => write!(f, "no chunks: {}", msg),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<ApiError> for ImportError {
    fn from(e: ApiError) -> Self {
        Self::Api(e)
    }
}

impl From<std::io::Error> for ImportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<fluree_db_core::Error> for ImportError {
    fn from(e: fluree_db_core::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

// ============================================================================
// ChunkSource
// ============================================================================

/// Abstraction over the source of import chunks.
///
/// Either a set of pre-split files (index-based access), or a streaming reader
/// for a single large Turtle file (channel-based, no pre-scan).
pub enum ChunkSource {
    /// Pre-split chunk files (existing behavior: `chunk_*.ttl` / `chunk_*.trig`).
    Files(Vec<PathBuf>),
    /// Streaming reader for a single large Turtle file. Chunks are emitted
    /// through a channel as the file is read — no full pre-scan needed.
    Streaming(fluree_graph_turtle::splitter::StreamingTurtleReader),
}

impl ChunkSource {
    /// Estimated number of chunks.
    ///
    /// Exact for `Files`, estimated for `Streaming` (file_size / chunk_size).
    pub fn estimated_len(&self) -> usize {
        match self {
            Self::Files(files) => files.len(),
            Self::Streaming(reader) => reader.estimated_chunk_count(),
        }
    }

    /// Whether this is a streaming source (no index-based access).
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::Streaming(_))
    }

    /// Read chunk at `index` as a String (only for `Files` variant).
    ///
    /// Panics if called on `Streaming` — use `recv_next` instead.
    pub fn read_chunk(&self, index: usize) -> std::io::Result<String> {
        match self {
            Self::Files(files) => std::fs::read_to_string(&files[index]),
            Self::Streaming(_) => {
                panic!("read_chunk not supported for streaming source; use recv_next")
            }
        }
    }

    /// Receive the next chunk from a streaming source as ready-to-parse TTL text.
    ///
    /// Returns `Ok(Some((index, text)))` for each chunk, `Ok(None)` when done.
    /// The text includes the prefix block prepended to the raw bytes.
    /// Only valid for `Streaming` variant.
    pub fn recv_next(&self) -> std::result::Result<Option<(usize, String)>, ImportError> {
        match self {
            Self::Streaming(reader) => {
                let payload = reader
                    .recv_chunk()
                    .map_err(|e| ImportError::NoChunks(format!("streaming read failed: {}", e)))?;
                match payload {
                    Some((idx, raw)) => {
                        // Note: we return only the raw TTL data for this chunk
                        // (no prefix block prepended). The streaming import
                        // path parses with a pre-extracted header prelude to
                        // avoid an extra full-chunk string copy.
                        let data = String::from_utf8(raw).map_err(|e| {
                            ImportError::Transact(format!("chunk {} invalid UTF-8: {}", idx, e))
                        })?;
                        Ok(Some((idx, data)))
                    }
                    None => Ok(None),
                }
            }
            Self::Files(_) => {
                panic!("recv_next not supported for file-based source; use read_chunk")
            }
        }
    }

    /// Whether chunk at `index` is a TriG file.
    pub fn is_trig(&self, index: usize) -> bool {
        match self {
            Self::Files(files) => files
                .get(index)
                .and_then(|p| p.extension())
                .is_some_and(|ext| ext == "trig"),
            Self::Streaming(_) => false, // Streaming is Turtle only.
        }
    }
}

/// Resolve the import path into a `ChunkSource`.
///
/// - If `path` is a directory: discover `chunk_*.ttl`/`chunk_*.trig` files (existing behavior).
/// - If `path` is a single large `.ttl` file: auto-split using `TurtleChunkReader`.
/// - If `path` is a single small `.ttl` file: treat as a single-element `Files` source.
fn resolve_chunk_source(
    path: &Path,
    config: &ImportConfig,
) -> std::result::Result<ChunkSource, ImportError> {
    if path.is_dir() {
        let files = discover_chunks(path)?;
        return Ok(ChunkSource::Files(files));
    }

    if !path.exists() {
        return Err(ImportError::NoChunks(format!(
            "path does not exist: {}",
            path.display()
        )));
    }

    // Single file — decide whether to auto-split based on size.
    let file_size = std::fs::metadata(path)?.len();
    let chunk_size_bytes = config.effective_chunk_size_mb() as u64 * 1024 * 1024;

    let is_ttl = path.extension().is_some_and(|ext| ext == "ttl");

    if is_ttl && file_size > chunk_size_bytes {
        // Large file: stream chunks via background reader thread.
        let max_inflight = config.effective_max_inflight();

        // Build progress callback that forwards to the import progress handler.
        let scan_progress: Option<fluree_graph_turtle::splitter::ScanProgressFn> =
            config.progress.as_ref().map(|cb| {
                let cb = Arc::clone(cb);
                let f: fluree_graph_turtle::splitter::ScanProgressFn =
                    Arc::new(move |bytes_read, total_bytes| {
                        cb(ImportPhase::Scanning {
                            bytes_read,
                            total_bytes,
                        });
                    });
                f
            });

        let reader = fluree_graph_turtle::splitter::StreamingTurtleReader::new(
            path,
            chunk_size_bytes,
            max_inflight,
            scan_progress,
        )
        .map_err(|e| ImportError::NoChunks(format!("turtle file split failed: {}", e)))?;
        tracing::info!(
            estimated_chunks = reader.estimated_chunk_count(),
            chunk_size_mb = config.effective_chunk_size_mb(),
            file_size_mb = file_size / (1024 * 1024),
            "streaming large Turtle file (no pre-scan)"
        );
        Ok(ChunkSource::Streaming(reader))
    } else {
        // Small file or non-TTL: treat as a single-element source.
        Ok(ChunkSource::Files(vec![path.to_path_buf()]))
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for a bulk import operation.
///
/// Created via `fluree.create("mydb").import("/path/to/chunks")`.
///
/// # Example
///
/// ```ignore
/// let result = fluree.create("mydb")
///     .import("/data/chunks/")
///     .memory_budget_mb(24000)
///     .execute()
///     .await?;
/// ```
pub struct ImportBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a super::Fluree<S, N>,
    ledger_id: String,
    import_path: PathBuf,
    config: ImportConfig,
}

impl<'a, S, N> ImportBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService
        + Publisher
        + fluree_db_nameservice::ConfigPublisher
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub(crate) fn new(
        fluree: &'a super::Fluree<S, N>,
        ledger_id: String,
        import_path: PathBuf,
    ) -> Self {
        Self {
            fluree,
            ledger_id,
            import_path,
            config: ImportConfig::default(),
        }
    }

    /// Set the number of parallel TTL parse threads.
    pub fn threads(mut self, n: usize) -> Self {
        self.config.parse_threads = n;
        self
    }

    /// Set the overall memory budget in MB. 0 = auto-detect (75% of RAM).
    pub fn memory_budget_mb(mut self, mb: usize) -> Self {
        self.config.memory_budget_mb = mb;
        self
    }

    /// Set the chunk size in MB for large-file splitting. 0 = derive from budget.
    pub fn chunk_size_mb(mut self, mb: usize) -> Self {
        self.config.chunk_size_mb = mb;
        self
    }

    /// Set the maximum flakes per chunk. 0 = no limit. Default: 20_000_000.
    pub fn chunk_max_flakes(mut self, n: usize) -> Self {
        self.config.chunk_max_flakes = n;
        self
    }

    /// Set the parallelism (alias for `.threads()`).
    pub fn parallelism(mut self, n: usize) -> Self {
        self.config.parse_threads = n;
        self
    }

    /// Whether to build indexes after import. Default: true.
    pub fn build_index(mut self, v: bool) -> Self {
        self.config.build_index = v;
        self
    }

    /// Whether to publish to nameservice. Default: true.
    pub fn publish(mut self, v: bool) -> Self {
        self.config.publish = v;
        self
    }

    /// Whether to clean up tmp files on success. Default: true.
    pub fn cleanup(mut self, v: bool) -> Self {
        self.config.cleanup_local_files = v;
        self
    }

    /// Whether to zstd-compress commit blobs. Default: true.
    pub fn compress(mut self, v: bool) -> Self {
        self.config.compress_commits = v;
        self
    }

    /// Whether to collect ID-based stats during commit resolution. Default: true.
    pub fn collect_id_stats(mut self, v: bool) -> Self {
        self.config.collect_id_stats = v;
        self
    }

    /// Publish nameservice checkpoint every N chunks. Default: 50. 0 disables.
    pub fn publish_every(mut self, n: usize) -> Self {
        self.config.publish_every = n;
        self
    }

    /// Set the number of records per leaflet. Default: 25_000.
    /// Larger values produce fewer, bigger leaflets (less I/O overhead).
    pub fn leaflet_rows(mut self, n: usize) -> Self {
        self.config.leaflet_rows = n;
        self
    }

    /// Set the number of leaflets per leaf file. Default: 10.
    /// Larger values produce fewer leaf files (shallower tree).
    pub fn leaflets_per_leaf(mut self, n: usize) -> Self {
        self.config.leaflets_per_leaf = n;
        self
    }

    /// Set a progress callback invoked at key pipeline milestones.
    pub fn on_progress(mut self, f: impl Fn(ImportPhase) + Send + Sync + 'static) -> Self {
        self.config.progress = Some(Arc::new(f));
        self
    }

    /// Effective resource settings that will be used for this import (auto-derived when not set).
    /// Use this to report to the user what memory budget and parallelism the import will use.
    pub fn effective_import_settings(&self) -> EffectiveImportSettings {
        self.config.effective_import_settings()
    }

    /// Execute the bulk import pipeline.
    pub async fn execute(self) -> std::result::Result<ImportResult, ImportError> {
        run_import_pipeline(
            self.fluree.storage(),
            self.fluree.nameservice(),
            &self.ledger_id,
            &self.import_path,
            &self.config,
        )
        .await
    }
}

// ============================================================================
// Create builder (intermediate)
// ============================================================================

/// Intermediate builder returned by `fluree.create("mydb")`.
///
/// Supports `.import(path)` for bulk import, or `.execute()` for empty ledger creation.
pub struct CreateBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a super::Fluree<S, N>,
    ledger_id: String,
}

impl<'a, S, N> CreateBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(fluree: &'a super::Fluree<S, N>, ledger_id: String) -> Self {
        Self { fluree, ledger_id }
    }
}

impl<'a, S, N> CreateBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService
        + Publisher
        + fluree_db_nameservice::ConfigPublisher
        + Clone
        + Send
        + Sync
        + 'static,
{
    /// Attach a bulk import to this create operation.
    ///
    /// `path` can be a directory containing `chunk_*.ttl` files, or a single TTL file.
    pub fn import(self, path: impl AsRef<Path>) -> ImportBuilder<'a, S, N> {
        ImportBuilder::new(self.fluree, self.ledger_id, path.as_ref().to_path_buf())
    }
}

// ============================================================================
// Chunk discovery
// ============================================================================

/// Discover and sort `chunk_*.ttl` or `chunk_*.trig` files from a directory.
fn discover_chunks(dir: &Path) -> std::result::Result<Vec<PathBuf>, ImportError> {
    if !dir.is_dir() {
        // Single file import
        if dir.exists() {
            return Ok(vec![dir.to_path_buf()]);
        }
        return Err(ImportError::NoChunks(format!(
            "path does not exist: {}",
            dir.display()
        )));
    }

    let mut chunks: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let is_supported_ext = p
                .extension()
                .is_some_and(|ext| ext == "ttl" || ext == "trig");
            let starts_with_chunk = p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("chunk_"));
            is_supported_ext && starts_with_chunk
        })
        .collect();

    if chunks.is_empty() {
        return Err(ImportError::NoChunks(format!(
            "no chunk_*.ttl or chunk_*.trig files found in {}",
            dir.display()
        )));
    }

    chunks.sort();
    Ok(chunks)
}

// ============================================================================
// Import pipeline
// ============================================================================

/// Core import pipeline. Orchestrates all phases.
async fn run_import_pipeline<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    import_path: &Path,
    config: &ImportConfig,
) -> std::result::Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + fluree_db_nameservice::ConfigPublisher,
{
    let pipeline_start = Instant::now();
    let _span = tracing::info_span!("bulk_import", alias = %alias).entered();

    // ---- Log effective settings and resolve chunk source ----
    config.log_effective_settings();
    let chunk_source = resolve_chunk_source(import_path, config)?;
    let estimated_total = chunk_source.estimated_len();
    tracing::info!(
        estimated_chunks = estimated_total,
        streaming = chunk_source.is_streaming(),
        path = %import_path.display(),
        "resolved import chunks"
    );

    // ---- Phase 1: Create ledger (init nameservice) ----
    let normalized_alias =
        fluree_db_core::ledger_id::normalize_ledger_id(alias).unwrap_or_else(|_| alias.to_string());

    // Check if ledger already exists
    let ns_record = nameservice
        .lookup(&normalized_alias)
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?;

    let needs_init = match &ns_record {
        None => true,
        Some(record) if record.retracted => {
            // Ledger was dropped — safe to re-create.
            tracing::info!(alias = %normalized_alias, "re-initializing retracted ledger");
            true
        }
        Some(record) if record.commit_t > 0 || record.commit_head_id.is_some() => {
            return Err(ImportError::Transact(format!(
                "import requires a fresh ledger, but '{}' already has commits (t={})",
                normalized_alias, record.commit_t
            )));
        }
        Some(_) => false,
    };

    if needs_init {
        nameservice
            .publish_ledger_init(&normalized_alias)
            .await
            .map_err(|e| ImportError::Storage(e.to_string()))?;
        tracing::info!(alias = %normalized_alias, "initialized new ledger in nameservice");
    }

    // ---- Set up session directory for runs/indexes ----
    let alias_prefix = fluree_db_core::address_path::ledger_id_to_path_prefix(&normalized_alias)
        .unwrap_or_else(|_| normalized_alias.replace(':', "/"));

    // Derive session dir from storage's data directory.
    // For file storage: {data_dir}/{alias_path}/tmp_import/{session_id}/
    let sid = session_id();
    let session_dir = derive_session_dir(storage, &alias_prefix, &sid);
    let run_dir = session_dir.join("runs");
    let index_dir = session_dir.join("index");
    std::fs::create_dir_all(&run_dir)?;

    tracing::info!(
        session_dir = %session_dir.display(),
        run_dir = %run_dir.display(),
        "import session directory created"
    );

    // ---- Phases 2-6: Import, build, upload, publish ----
    // Wrapped in a helper to ensure cleanup semantics:
    // - On success or failure + cleanup_local_files=true → delete session dir
    // - If cleanup itself fails → log warning, do not fail import
    let paths = PipelinePaths {
        run_dir: &run_dir,
        index_dir: &index_dir,
    };
    let chunk_source = std::sync::Arc::new(chunk_source);
    let pipeline_result = run_pipeline_phases(
        storage,
        nameservice,
        &normalized_alias,
        &chunk_source,
        paths,
        config,
        pipeline_start,
    )
    .await;

    // Cleanup session dir on both success and failure to avoid accumulating
    // hundreds of GB of orphaned temp files from failed imports.
    if config.cleanup_local_files {
        if let Err(e) = std::fs::remove_dir_all(&session_dir) {
            tracing::warn!(
                session_dir = %session_dir.display(),
                error = %e,
                "failed to clean up import session directory"
            );
        } else {
            tracing::info!(
                session_dir = %session_dir.display(),
                "import session directory cleaned up"
            );
        }
    } else {
        tracing::info!(
            session_dir = %session_dir.display(),
            "cleanup disabled; import artifacts retained"
        );
    }

    match pipeline_result {
        Ok(result) => {
            let total_elapsed = pipeline_start.elapsed();
            tracing::info!(
                alias = %normalized_alias,
                t = result.t,
                flakes = result.flake_count,
                root_id = ?result.root_id,
                elapsed = ?total_elapsed,
                "bulk import pipeline complete"
            );

            Ok(result)
        }
        Err(e) => Err(e),
    }
}

// ============================================================================
// Pipeline phases 2-6
// ============================================================================

/// Paths used by the import pipeline.
struct PipelinePaths<'a> {
    /// Directory for run files.
    run_dir: &'a Path,
    /// Directory for index files.
    index_dir: &'a Path,
}

/// Input parameters for index building and uploading.
struct IndexBuildInput<'a> {
    /// Directory containing run files.
    run_dir: &'a Path,
    /// Directory for index output.
    index_dir: &'a Path,
    /// Final transaction t value.
    final_t: i64,
    /// Namespace code to prefix mappings.
    namespace_codes: &'a HashMap<u16, String>,
    /// Total flakes from commit resolution (used for indexing progress).
    cumulative_flakes: u64,
    /// Sorted commit file infos for Phase C (SPOT build from sorted commits).
    sorted_commit_infos: Vec<fluree_db_indexer::run_index::SortedCommitInfo>,
    /// Unified language dict (global lang_id → tag string), built from per-chunk
    /// lang vocab files. All indexes use this mapping.
    unified_lang_dict: fluree_db_indexer::run_index::LanguageTagDict,
    /// Per-chunk language remap tables (chunk-local lang_id → global lang_id).
    /// Built from per-chunk lang vocab files. Passed to Phase C
    /// to avoid recomputing.
    lang_remaps: Vec<Vec<u16>>,
    /// Predicate field width in bytes (1, 2, or 4). Pre-computed from predicate
    /// dict size to avoid re-reading predicates.json in build_and_upload.
    p_width: u8,
    /// Datatype field width in bytes (1 or 2). Pre-computed from datatype dict
    /// size to avoid re-reading datatypes.dict in build_and_upload.
    dt_width: u8,
    /// Datatype ID → ValueTypeTag mapping (for stats hook in Phase D).
    dt_tags: Vec<fluree_db_core::value_id::ValueTypeTag>,
    /// Predicate ID for rdf:type (for inline class stats during SPOT merge).
    rdf_type_p_id: u32,
    /// Whether to collect ID-based stats during Phase D remap.
    collect_id_stats: bool,
    /// Turtle @prefix IRI → short prefix name, for IRI compaction in display.
    prefix_map: &'a HashMap<String, String>,
}

/// Run phases 2-6: import chunks, build indexes, upload to CAS, write V4 root, publish.
///
/// Separated from `run_import_pipeline` to enable clean error-path handling:
/// on failure, the caller keeps the session dir for debugging.
async fn run_pipeline_phases<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    chunk_source: &std::sync::Arc<ChunkSource>,
    paths: PipelinePaths<'_>,
    config: &ImportConfig,
    pipeline_start: Instant,
) -> std::result::Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + fluree_db_nameservice::ConfigPublisher,
{
    // ---- Phase 2: Import TTL → commits + streaming runs ----
    let import_result = run_import_chunks(
        storage,
        nameservice,
        alias,
        chunk_source,
        paths.run_dir,
        config,
    )
    .await?;

    tracing::info!(
        t = import_result.final_t,
        flakes = import_result.cumulative_flakes,
        commit_head = %import_result.commit_head_id,
        elapsed = ?pipeline_start.elapsed(),
        "import + run generation complete"
    );

    // ---- Phases 3-6: Build index, upload, root, publish ----
    let root_id;
    let index_t;
    let summary;

    if config.build_index {
        let build_input = IndexBuildInput {
            run_dir: paths.run_dir,
            index_dir: paths.index_dir,
            final_t: import_result.final_t,
            namespace_codes: &import_result.namespace_codes,
            cumulative_flakes: import_result.cumulative_flakes,
            sorted_commit_infos: import_result.sorted_commit_infos,
            unified_lang_dict: import_result.unified_lang_dict,
            lang_remaps: import_result.lang_remaps,
            p_width: import_result.p_width,
            dt_width: import_result.dt_width,
            dt_tags: import_result.dt_tags,
            rdf_type_p_id: import_result.rdf_type_p_id,
            collect_id_stats: config.collect_id_stats,
            prefix_map: &import_result.prefix_map,
        };
        let index_result = build_and_upload(
            storage,
            nameservice,
            alias,
            build_input,
            config,
            import_result.total_commit_size,
            import_result.total_asserts,
            import_result.total_retracts,
        )
        .await?;

        root_id = Some(index_result.root_id);
        index_t = index_result.index_t;
        summary = index_result.summary;
    } else {
        root_id = None;
        index_t = 0;
        summary = None;
    }

    // ---- Phase 7: Persist default context from turtle prefixes ----
    if let Err(e) =
        store_default_context(storage, nameservice, alias, &import_result.prefix_map).await
    {
        tracing::warn!(%e, "failed to persist default context (non-fatal)");
    }

    config.emit_progress(ImportPhase::Done);

    Ok(ImportResult {
        ledger_id: alias.to_string(),
        t: import_result.final_t,
        flake_count: import_result.cumulative_flakes,
        commit_head_id: import_result.commit_head_id,
        root_id,
        index_t,
        summary,
    })
}

// ============================================================================
// Phase 2: Import chunks
// ============================================================================

/// Lightweight per-commit metadata collected during the serial commit loop.
/// Used to generate the txn-meta "meta chunk" without re-reading commit blobs.
struct CommitMeta {
    /// ContentId hex digest (64-char SHA-256 hex).
    commit_hash_hex: String,
    /// Transaction number.
    t: i64,
    /// Commit blob size in bytes.
    blob_bytes: usize,
    /// Number of flakes (= asserts for fresh import).
    flake_count: u32,
    /// Epoch milliseconds (parsed once at collection time). `None` if no timestamp.
    time_epoch_ms: Option<i64>,
    /// Previous commit's hex digest (for db:previous), `None` for first commit.
    previous_commit_hex: Option<String>,
}

/// Internal result from the import phase (before index build).
struct ChunkImportResult {
    final_t: i64,
    cumulative_flakes: u64,
    commit_head_id: fluree_db_core::ContentId,
    namespace_codes: HashMap<u16, String>,
    /// Total size of all commit blobs in bytes.
    total_commit_size: u64,
    /// Total number of assertions across all commits.
    total_asserts: u64,
    /// Total number of retractions across all commits.
    total_retracts: u64,
    /// Turtle @prefix short names accumulated across all chunks: IRI → short prefix.
    prefix_map: HashMap<String, String>,
    /// Sorted commit file infos for Phase C (SPOT build from sorted commits).
    sorted_commit_infos: Vec<fluree_db_indexer::run_index::SortedCommitInfo>,
    /// Unified language dict (global lang_id → tag string), built from per-chunk
    /// lang vocab files. All indexes use this mapping.
    unified_lang_dict: fluree_db_indexer::run_index::LanguageTagDict,
    /// Per-chunk language remap tables (chunk-local lang_id → global lang_id).
    lang_remaps: Vec<Vec<u16>>,
    /// Predicate field width in bytes (1, 2, or 4).
    p_width: u8,
    /// Datatype field width in bytes (1 or 2).
    dt_width: u8,
    /// Datatype ID → ValueTypeTag mapping (for stats hook in Phase D).
    dt_tags: Vec<fluree_db_core::value_id::ValueTypeTag>,
    /// Predicate ID for rdf:type (for inline class stats during SPOT merge).
    rdf_type_p_id: u32,
}

/// Import all TTL chunks: parallel parse + serial commit + streaming runs.
async fn run_import_chunks<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    chunk_source: &std::sync::Arc<ChunkSource>,
    run_dir: &Path,
    config: &ImportConfig,
) -> std::result::Result<ChunkImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher,
{
    use fluree_db_indexer::run_index::{persist_namespaces, SortedCommitInfo};
    use fluree_db_transact::import::{
        finalize_parsed_chunk, import_commit, import_trig_commit, parse_chunk,
        parse_chunk_with_prelude, ImportState, ParsedChunk,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn spawn_sorted_commit_write(
        sort_write_handles: &mut Vec<
            tokio::task::JoinHandle<
                std::io::Result<fluree_db_indexer::run_index::SortedCommitInfo>,
            >,
        >,
        sort_write_semaphore: &Arc<tokio::sync::Semaphore>,
        vocab_dir: &Path,
        spool_dir: &Path,
        rdf_type_p_id: u32,
        sr: fluree_db_transact::import_sink::BufferedSpoolResult,
    ) {
        let ci = sr.chunk_idx;
        let vd = vocab_dir.to_path_buf();
        let sd = spool_dir.to_path_buf();
        let permit = sort_write_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        sort_write_handles.push(tokio::task::spawn_blocking(move || {
            let r = fluree_db_indexer::run_index::spool::sort_remap_and_write_sorted_commit(
                sr.records,
                sr.subjects,
                sr.strings,
                &vd.join(format!("chunk_{ci:05}.subjects.voc")),
                &vd.join(format!("chunk_{ci:05}.strings.voc")),
                &sd.join(format!("commit_{ci:05}.fsc")),
                ci,
                Some((
                    &sr.languages,
                    &vd.join(format!("chunk_{ci:05}.languages.voc")),
                )),
                Some(fluree_db_indexer::run_index::TypesMapConfig {
                    rdf_type_p_id,
                    output_dir: &sd,
                }),
            );
            drop(permit);
            r
        }));
    }

    /// Shared immutable environment for the serial commit pipeline.
    struct CommitPipelineEnv<'a, S, N> {
        estimated_total: usize,
        run_start: Instant,
        storage: &'a S,
        nameservice: &'a N,
        alias: &'a str,
        config: &'a ImportConfig,
        sort_write_semaphore: &'a Arc<tokio::sync::Semaphore>,
        vocab_dir: &'a Path,
        spool_dir: &'a Path,
        rdf_type_p_id: u32,
        import_time_epoch_ms: Option<i64>,
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_parsed_chunks_in_order<S, N>(
        result_rx: std::sync::mpsc::Receiver<std::result::Result<(usize, ParsedChunk), String>>,
        env: &CommitPipelineEnv<'_, S, N>,
        state: &mut ImportState,
        published_codes: &mut rustc_hash::FxHashSet<u16>,
        compute_ns_delta: impl Fn(
            &rustc_hash::FxHashSet<u16>,
            &mut rustc_hash::FxHashSet<u16>,
        ) -> std::collections::HashMap<u16, String>,
        sort_write_handles: &mut Vec<
            tokio::task::JoinHandle<
                std::io::Result<fluree_db_indexer::run_index::SortedCommitInfo>,
            >,
        >,
        total_commit_size: &mut u64,
        commit_metas: &mut Vec<CommitMeta>,
    ) -> std::result::Result<usize, ImportError>
    where
        S: Storage + Clone + Send + Sync + 'static,
        N: NameService + Publisher,
    {
        // Serial commit loop: receive parsed chunks, reorder, finalize in order.
        // Parsed chunks arrive out of order from parallel workers.
        let mut next_expected: usize = 0;
        let mut pending: std::collections::BTreeMap<usize, ParsedChunk> =
            std::collections::BTreeMap::new();

        for recv_result in result_rx.iter() {
            let (idx, parsed) = recv_result.map_err(ImportError::Transact)?;
            pending.insert(idx, parsed);

            while let Some(parsed) = pending.remove(&next_expected) {
                let ns_delta = compute_ns_delta(&parsed.new_codes, published_codes);

                // Capture previous commit hex BEFORE finalize advances state.
                let previous_commit_hex = commit_metas.last().map(|m| m.commit_hash_hex.clone());

                let result = finalize_parsed_chunk(state, parsed, ns_delta, env.storage, env.alias)
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?;

                // Collect txn-meta for this commit (no I/O, just captures data already in scope).
                commit_metas.push(CommitMeta {
                    commit_hash_hex: result.commit_id.digest_hex(),
                    t: result.t,
                    blob_bytes: result.blob_bytes,
                    flake_count: result.flake_count,
                    time_epoch_ms: env.import_time_epoch_ms,
                    previous_commit_hex,
                });

                if let Some(sr) = result.spool_result {
                    spawn_sorted_commit_write(
                        sort_write_handles,
                        env.sort_write_semaphore,
                        env.vocab_dir,
                        env.spool_dir,
                        env.rdf_type_p_id,
                        sr,
                    )
                    .await;
                }

                *total_commit_size += result.blob_bytes as u64;

                let total_elapsed = env.run_start.elapsed().as_secs_f64();
                tracing::info!(
                    chunk = next_expected + 1,
                    total = env.estimated_total,
                    t = result.t,
                    flakes = result.flake_count,
                    cumulative_flakes = state.cumulative_flakes,
                    flakes_per_sec = format!(
                        "{:.2}M",
                        state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0
                    ),
                    "chunk committed"
                );
                env.config.emit_progress(ImportPhase::Committing {
                    chunk: next_expected + 1,
                    total: env.estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: total_elapsed,
                });

                // Periodic nameservice checkpoint
                if env.config.publish_every > 0
                    && (next_expected + 1).is_multiple_of(env.config.publish_every)
                {
                    env.nameservice
                        .publish_commit(env.alias, result.t, &result.commit_id)
                        .await
                        .map_err(|e| ImportError::Storage(e.to_string()))?;
                    tracing::info!(
                        t = result.t,
                        chunk = next_expected + 1,
                        "published nameservice checkpoint"
                    );
                }

                next_expected += 1;
            }
        }

        Ok(next_expected)
    }

    let is_streaming = chunk_source.is_streaming();
    let estimated_total = chunk_source.estimated_len();
    let compress = config.compress_commits;
    let num_threads = config.parse_threads;
    let mut state = ImportState::new();
    let run_start = Instant::now();

    // ---- Inflight permit channel (memory budget enforcement) ----
    // For Files mode: limits the number of chunk texts materialized in memory.
    // For Streaming mode: backpressure is handled by the bounded channel in
    // StreamingTurtleReader, so permits are not needed.
    let (permit_tx, permit_rx) = if !is_streaming {
        let max_inflight = config.effective_max_inflight();
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(max_inflight);
        for _ in 0..max_inflight {
            tx.send(()).unwrap();
        }
        (
            Some(tx),
            Some(std::sync::Arc::new(std::sync::Mutex::new(rx))),
        )
    } else {
        (None, None)
    };

    // ---- Pipeline infrastructure ----
    std::fs::create_dir_all(run_dir)?;

    // Spool directory for chunk-local spool files (Tier 2 pipeline).
    let spool_dir = run_dir.join("spool");
    std::fs::create_dir_all(&spool_dir)?;

    // Background sort/write pipeline: sorted commit files (.fsc) + vocab files (.voc)
    // are produced asynchronously after commit-v2 finalization. The serial commit loop
    // hands off records+dicts to spawn_blocking tasks, then continues immediately.
    // Semaphore bounds memory: each inflight job holds a Vec<RunRecord>.
    // Scale permits inversely with chunk size — smaller chunks need less memory per job,
    // so we can safely run more concurrently without RSS explosion.
    // Target: ~3GB total inflight sort/write memory.
    let chunk_mb = config.effective_chunk_size_mb();
    let sort_write_permits = match chunk_mb {
        0..=256 => 8,
        257..=512 => 6,
        513..=768 => 4,
        _ => 3,
    };
    tracing::info!(
        sort_write_permits,
        chunk_mb,
        "background sort/write semaphore"
    );
    let sort_write_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(sort_write_permits));
    let mut sort_write_handles: Vec<tokio::task::JoinHandle<std::io::Result<SortedCommitInfo>>> =
        Vec::new();
    let vocab_dir = run_dir.join("vocab");
    std::fs::create_dir_all(&vocab_dir)?;

    // Track commit metadata across all chunks (previously tracked by resolver).
    let mut total_commit_size: u64 = 0;
    let mut commit_metas: Vec<CommitMeta> = Vec::new();
    // Parse import timestamp once (it's constant for the whole import).
    let import_time_epoch_ms: Option<i64> =
        chrono::DateTime::parse_from_rfc3339(&state.import_time)
            .ok()
            .map(|dt| dt.timestamp_millis());
    // In fresh import, all ops are assertions (no retractions).
    // total_asserts = state.cumulative_flakes at the end.

    // ---- Phase 2a: Create shared allocators, then parse all chunks in parallel ----
    //
    // Create shared allocators before any parsing so all chunks can produce
    // spool output concurrently. For streaming, register prelude prefixes first
    // so the shared allocator knows about the data's namespace IRIs.
    use fluree_db_transact::SharedNamespaceAllocator;
    use fluree_vocab::namespaces::OVERFLOW;
    use rustc_hash::FxHashSet;

    let streaming_prelude = if is_streaming {
        match &**chunk_source {
            ChunkSource::Streaming(reader) => Some(reader.prelude().clone()),
            _ => None,
        }
    } else {
        None
    };

    // For streaming: pre-register prelude prefixes so the shared allocator
    // includes them (they won't appear as @prefix directives in chunk data).
    if let Some(ref prelude) = streaming_prelude {
        for (short, ns_iri) in &prelude.prefixes {
            state.ns_registry.get_or_allocate(ns_iri);
            if !short.is_empty() {
                state.prefix_map.insert(ns_iri.clone(), short.clone());
            }
        }
    }

    let shared_alloc = Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry));
    let mut published_codes: FxHashSet<u16> = state.ns_registry.all_codes();

    // Create shared allocators for the spool pipeline (Tier 2).
    // These are the same seed values as GlobalDicts::new(), but wrapped in
    // thread-safe SharedDictAllocator so parse workers can allocate IDs
    // concurrently without the resolver bottleneck.
    use fluree_db_indexer::run_index::global_dict::SharedDictAllocator;
    use fluree_db_indexer::run_index::shared_pool::{SharedNumBigPool, SharedVectorArenaPool};
    use fluree_db_transact::import_sink::SpoolConfig;

    let spool_config = Arc::new(SpoolConfig {
        predicate_alloc: Arc::new(SharedDictAllocator::new_predicate()),
        datatype_alloc: Arc::new(SharedDictAllocator::new_datatype()),
        graph_alloc: Arc::new(SharedDictAllocator::new_graph()),
        numbig_pool: Arc::new(SharedNumBigPool::new()),
        vector_pool: Arc::new(SharedVectorArenaPool::new()),
        ns_alloc: Arc::clone(&shared_alloc),
    });

    // Pre-insert rdf:type so we know the predicate ID before Phase A begins.
    // This allows sort_remap_and_write_sorted_commit to extract rdf:type edges
    // into a types-map sidecar for building the subject→class bitset table.
    let rdf_type_p_id = spool_config
        .predicate_alloc
        .get_or_insert(fluree_vocab::rdf::TYPE);

    // Pre-insert txn-meta predicates so they get stable IDs in predicates.json
    // and are included in p_width calculation. These match the predicates used
    // by `CommitResolver::emit_txn_meta` in the non-import indexing path.
    {
        use fluree_vocab::{db, fluree};
        for &(prefix, name) in &[
            (fluree::DB, db::ADDRESS),
            (fluree::DB, db::TIME),
            (fluree::DB, db::T),
            (fluree::DB, db::SIZE),
            (fluree::DB, db::ASSERTS),
            (fluree::DB, db::RETRACTS),
            (fluree::DB, db::PREVIOUS),
        ] {
            spool_config
                .predicate_alloc
                .get_or_insert_parts(prefix, name);
        }
    }

    // Helper: compute ns_delta for a parsed chunk and advance published_codes.
    let compute_ns_delta = |new_codes: &FxHashSet<u16>,
                            published: &mut FxHashSet<u16>|
     -> std::collections::HashMap<u16, String> {
        let unpublished: FxHashSet<u16> = new_codes
            .iter()
            .copied()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();
        let delta = if unpublished.is_empty() {
            std::collections::HashMap::new()
        } else {
            shared_alloc.lookup_codes(&unpublished)
        };
        published.extend(&unpublished);
        delta
    };

    /// Shared context for parsing TTL chunks.
    struct ParseChunkContext<'a> {
        shared_alloc: &'a Arc<SharedNamespaceAllocator>,
        prelude: Option<&'a fluree_graph_turtle::splitter::TurtlePrelude>,
        ledger: &'a str,
        compress: bool,
        spool_dir: &'a Path,
        spool_config: &'a Arc<SpoolConfig>,
    }

    fn parse_ttl_chunk(
        ttl: &str,
        ctx: &ParseChunkContext<'_>,
        t: i64,
        idx: usize,
    ) -> std::result::Result<ParsedChunk, String> {
        let parsed = if let Some(prelude) = ctx.prelude {
            parse_chunk_with_prelude(
                ttl,
                ctx.shared_alloc,
                prelude,
                t,
                ctx.ledger,
                ctx.compress,
                Some(ctx.spool_dir),
                Some(ctx.spool_config),
                idx,
            )
        } else {
            parse_chunk(
                ttl,
                ctx.shared_alloc,
                t,
                ctx.ledger,
                ctx.compress,
                Some(ctx.spool_dir),
                Some(ctx.spool_config),
                idx,
            )
        };
        parsed.map_err(|e| e.to_string())
    }

    // ---- Parse all chunks in parallel, commit serially in order ----
    //
    // All chunks (including chunk 0) go through the parallel pipeline.
    // SharedNamespaceAllocator + SharedDictAllocator are created above,
    // so no chunk needs to "establish namespaces" before others can start.

    let commit_env = CommitPipelineEnv {
        estimated_total,
        run_start,
        storage,
        nameservice,
        alias,
        config,
        sort_write_semaphore: &sort_write_semaphore,
        vocab_dir: &vocab_dir,
        spool_dir: &spool_dir,
        rdf_type_p_id,
        import_time_epoch_ms,
    };

    if is_streaming {
        // Streaming path: workers receive chunk data from the reader thread's
        // channel. No worker I/O — the reader is the only entity reading from disk.
        // This avoids double I/O that would kill throughput on external drives.
        let ledger = alias.to_string();

        let (shared_rx, prelude) = match &**chunk_source {
            ChunkSource::Streaming(reader) => (reader.shared_receiver(), reader.prelude().clone()),
            _ => unreachable!(),
        };

        let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
            std::result::Result<(usize, ParsedChunk), String>,
        >(num_threads * 2);

        let mut parse_handles = Vec::with_capacity(num_threads);
        for thread_idx in 0..num_threads {
            let shared_rx = Arc::clone(&shared_rx);
            let result_tx = result_tx.clone();
            let shared_alloc = Arc::clone(&shared_alloc);
            let ledger = ledger.clone();
            let prelude = prelude.clone();
            let spool_dir = spool_dir.clone();
            let spool_config = Arc::clone(&spool_config);

            let handle = std::thread::Builder::new()
                .name(format!("ttl-parser-{}", thread_idx))
                .spawn(move || {
                    let ctx = ParseChunkContext {
                        shared_alloc: &shared_alloc,
                        prelude: Some(&prelude),
                        ledger: &ledger,
                        compress,
                        spool_dir: &spool_dir,
                        spool_config: &spool_config,
                    };
                    loop {
                        // Pull next chunk data from the reader thread (no I/O here).
                        let (idx, raw_bytes) = match shared_rx.lock().unwrap().recv() {
                            Ok(payload) => payload,
                            Err(_) => break, // Reader thread finished.
                        };

                        // Convert raw bytes to String (CPU-only; no copy on success).
                        let ttl = match String::from_utf8(raw_bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = result_tx
                                    .send(Err(format!("chunk {} invalid UTF-8: {}", idx, e)));
                                break;
                            }
                        };

                        let t = (idx + 1) as i64;
                        tracing::debug!(
                            chunk_idx = idx,
                            chunk_text_len = ttl.len(),
                            starts_with = &ttl[..ttl.len().min(200)],
                            "about to parse chunk"
                        );
                        match parse_ttl_chunk(&ttl, &ctx, t, idx) {
                            Ok(parsed) => {
                                if result_tx.send(Ok((idx, parsed))).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = result_tx
                                    .send(Err(format!("parse chunk {} failed: {}", idx, e)));
                                break;
                            }
                        }
                    }
                })
                .map_err(|e| ImportError::Transact(format!("spawn parser: {}", e)))?;

            parse_handles.push(handle);
        }
        drop(result_tx); // main thread's copy

        let next_expected = commit_parsed_chunks_in_order(
            result_rx,
            &commit_env,
            &mut state,
            &mut published_codes,
            compute_ns_delta,
            &mut sort_write_handles,
            &mut total_commit_size,
            &mut commit_metas,
        )
        .await?;

        // Wait for parse threads.
        for handle in parse_handles {
            handle.join().expect("parse thread panicked");
        }

        // Note: The reader thread finishes when all chunks are consumed (channel
        // drained). Any reader errors would have manifested as channel closure,
        // which the parse workers handle by breaking their loop.
        tracing::info!(
            committed_chunks = next_expected,
            "streaming import phase complete"
        );
    } else {
        // File-based path: index-based access to chunk files.
        let has_trig = (0..estimated_total).any(|i| chunk_source.is_trig(i));
        if estimated_total > 0 && num_threads > 0 && !has_trig {
            let ledger = alias.to_string();

            let next_chunk = Arc::new(AtomicUsize::new(0));
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
                std::result::Result<(usize, ParsedChunk), String>,
            >(num_threads * 2);

            let permit_rx = permit_rx.expect("permit_rx must exist for file-based path");
            let permit_tx = permit_tx.expect("permit_tx must exist for file-based path");

            // Spawn parse worker threads
            let mut parse_handles = Vec::with_capacity(num_threads);
            for thread_idx in 0..num_threads {
                let next_chunk = Arc::clone(&next_chunk);
                let result_tx = result_tx.clone();
                let shared_alloc = Arc::clone(&shared_alloc);
                let ledger = ledger.clone();
                let chunk_source = Arc::clone(chunk_source);
                let permit_rx_ref = Arc::clone(&permit_rx);
                let permit_tx_ref = permit_tx.clone();
                let total = estimated_total;
                let spool_dir = spool_dir.clone();
                let spool_config = Arc::clone(&spool_config);

                let handle = std::thread::Builder::new()
                    .name(format!("ttl-parser-{}", thread_idx))
                    .spawn(move || {
                        let ctx = ParseChunkContext {
                            shared_alloc: &shared_alloc,
                            prelude: None,
                            ledger: &ledger,
                            compress,
                            spool_dir: &spool_dir,
                            spool_config: &spool_config,
                        };
                        loop {
                            let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                            if idx >= total {
                                break;
                            }

                            // Acquire inflight permit (blocks if at max_inflight).
                            let permit_result = permit_rx_ref.lock().unwrap().recv();
                            if permit_result.is_err() {
                                break;
                            }

                            let ttl = match chunk_source.read_chunk(idx) {
                                Ok(s) => s,
                                Err(e) => {
                                    let _ = permit_tx_ref.send(()); // release permit
                                    let _ = result_tx
                                        .send(Err(format!("failed to read chunk {}: {}", idx, e)));
                                    break;
                                }
                            };

                            let t = (idx + 1) as i64;
                            match parse_ttl_chunk(&ttl, &ctx, t, idx) {
                                Ok(parsed) => {
                                    let _ = permit_tx_ref.send(());
                                    if result_tx.send(Ok((idx, parsed))).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = permit_tx_ref.send(());
                                    let _ = result_tx
                                        .send(Err(format!("parse chunk {} failed: {}", idx, e)));
                                    break;
                                }
                            }
                        }
                    })
                    .map_err(|e| ImportError::Transact(format!("spawn parser: {}", e)))?;

                parse_handles.push(handle);
            }
            drop(result_tx); // main thread's copy

            let _committed_chunks = commit_parsed_chunks_in_order(
                result_rx,
                &commit_env,
                &mut state,
                &mut published_codes,
                compute_ns_delta,
                &mut sort_write_handles,
                &mut total_commit_size,
                &mut commit_metas,
            )
            .await?;

            // Wait for parse threads
            for handle in parse_handles {
                handle.join().expect("parse thread panicked");
            }
        } else if estimated_total > 0 {
            // Serial fallback (0 threads or TriG files present).
            // Spool is enabled so that the merge/remap pipeline produces run files.
            for i in 0..estimated_total {
                let content = chunk_source.read_chunk(i)?;
                let is_trig = chunk_source.is_trig(i);
                let result = if is_trig {
                    import_trig_commit(
                        &mut state,
                        &content,
                        storage,
                        alias,
                        compress,
                        Some(&spool_dir),
                        Some(&spool_config),
                        i,
                    )
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?
                } else {
                    import_commit(
                        &mut state,
                        &content,
                        storage,
                        alias,
                        compress,
                        Some(&spool_dir),
                        Some(&spool_config),
                        i,
                    )
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?
                };

                // Collect txn-meta for this commit.
                {
                    let previous_commit_hex =
                        commit_metas.last().map(|m| m.commit_hash_hex.clone());
                    commit_metas.push(CommitMeta {
                        commit_hash_hex: result.commit_id.digest_hex(),
                        t: result.t,
                        blob_bytes: result.blob_bytes,
                        flake_count: result.flake_count,
                        time_epoch_ms: import_time_epoch_ms,
                        previous_commit_hex,
                    });
                }

                // Hand off sort + remap + write to a background task.
                if let Some(sr) = result.spool_result {
                    spawn_sorted_commit_write(
                        &mut sort_write_handles,
                        &sort_write_semaphore,
                        &vocab_dir,
                        &spool_dir,
                        rdf_type_p_id,
                        sr,
                    )
                    .await;
                }
                total_commit_size += result.blob_bytes as u64;

                config.emit_progress(ImportPhase::Committing {
                    chunk: i + 1,
                    total: estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: run_start.elapsed().as_secs_f64(),
                });

                if config.publish_every > 0 && (i + 1).is_multiple_of(config.publish_every) {
                    nameservice
                        .publish_commit(alias, result.t, &result.commit_id)
                        .await
                        .map_err(|e| ImportError::Storage(e.to_string()))?;
                }
            }
        }
    }

    // Final commit head publish
    let commit_head_id = state
        .previous_ref
        .as_ref()
        .map(|r| r.id.clone())
        .ok_or_else(|| ImportError::Storage("no commit head after import".to_string()))?;

    nameservice
        .publish_commit(alias, state.t, &commit_head_id)
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?;
    tracing::info!(t = state.t, "published final commit head");

    // ---- Spawn txn-meta "meta chunk" build in background ----
    // Build a tiny extra chunk containing commit metadata records (g_id=1).
    // Runs in spawn_blocking concurrently with the sort_write_handles await below,
    // so it adds zero wall-clock time. The meta chunk participates in Phase B dict
    // merge and Phase C/D/E index builds so `ledger#txn-meta` queries work after import.
    let meta_chunk_handle = if !commit_metas.is_empty() {
        use fluree_vocab::{db, fluree};

        // Resolve predicate/graph IDs while spool_config is still accessible.
        // These were pre-inserted in Phase 2a, so these are pure lookups.
        let p_address = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::ADDRESS);
        let p_time = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::TIME);
        let p_t = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::T);
        let p_size = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::SIZE);
        let p_asserts = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::ASSERTS);
        let p_retracts = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::RETRACTS);
        let p_previous = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::PREVIOUS);

        let g_id = (spool_config
            .graph_alloc
            .get_or_insert_parts(fluree::DB, "txn-meta")
            + 1) as u16;
        debug_assert_eq!(g_id, 1, "txn-meta graph must be g_id=1");

        // meta_chunk_idx = number of data chunks (next sequential index).
        let meta_chunk_idx = sort_write_handles.len();
        let vocab_dir = vocab_dir.clone();
        let spool_dir = spool_dir.clone();

        Some(tokio::task::spawn_blocking(move || {
            use fluree_db_core::value_id::{ObjKey, ObjKind};
            use fluree_db_core::{DatatypeDictId, SubjectId};
            use fluree_db_indexer::run_index::run_record::LIST_INDEX_NONE;
            use fluree_db_indexer::run_index::{
                sort_remap_and_write_sorted_commit, ChunkStringDict, ChunkSubjectDict, RunRecord,
            };
            use fluree_vocab::namespaces;

            let mut meta_subjects = ChunkSubjectDict::new();
            let mut meta_strings = ChunkStringDict::new();
            let mut records: Vec<RunRecord> = Vec::with_capacity(commit_metas.len() * 8);

            for cm in &commit_metas {
                let commit_s = meta_subjects
                    .get_or_insert(namespaces::FLUREE_COMMIT, cm.commit_hash_hex.as_bytes());
                let t = cm.t as u32;

                let mut push =
                    |s_id: u64, p_id: u32, o_kind: ObjKind, o_key: ObjKey, dt: DatatypeDictId| {
                        records.push(RunRecord {
                            g_id,
                            s_id: SubjectId::from_u64(s_id),
                            p_id,
                            dt: dt.as_u16(),
                            o_kind: o_kind.as_u8(),
                            op: 1, // assert
                            o_key: o_key.as_u64(),
                            t,
                            lang_id: 0,
                            i: LIST_INDEX_NONE,
                        });
                    };

                // db:address — commit hash hex as LEX_ID string
                let addr_str_id = meta_strings.get_or_insert(cm.commit_hash_hex.as_bytes());
                push(
                    commit_s,
                    p_address,
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(addr_str_id),
                    DatatypeDictId::STRING,
                );

                // db:time — epoch_ms as LONG (pre-parsed at collection time)
                if let Some(epoch_ms) = cm.time_epoch_ms {
                    push(
                        commit_s,
                        p_time,
                        ObjKind::NUM_INT,
                        ObjKey::encode_i64(epoch_ms),
                        DatatypeDictId::LONG,
                    );
                }

                // db:t — INTEGER
                push(
                    commit_s,
                    p_t,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.t),
                    DatatypeDictId::INTEGER,
                );

                // db:size — LONG (blob bytes)
                push(
                    commit_s,
                    p_size,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.blob_bytes as i64),
                    DatatypeDictId::LONG,
                );

                // db:asserts — INTEGER
                push(
                    commit_s,
                    p_asserts,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.flake_count as i64),
                    DatatypeDictId::INTEGER,
                );

                // db:retracts — INTEGER (always 0 for fresh import)
                push(
                    commit_s,
                    p_retracts,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(0),
                    DatatypeDictId::INTEGER,
                );

                // db:previous — REF_ID (only if this commit has a predecessor)
                if let Some(ref prev_hex) = cm.previous_commit_hex {
                    let prev_s =
                        meta_subjects.get_or_insert(namespaces::FLUREE_COMMIT, prev_hex.as_bytes());
                    push(
                        commit_s,
                        p_previous,
                        ObjKind::REF_ID,
                        ObjKey::encode_sid64(prev_s),
                        DatatypeDictId::ID,
                    );
                }
            }

            let meta_records_count = records.len();
            let commit_count = commit_metas.len();

            let subj_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.subjects.voc"));
            let str_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.strings.voc"));
            let commit_path = spool_dir.join(format!("commit_{meta_chunk_idx:05}.fsc"));

            // Write empty language vocab for uniformity.
            let lang_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.languages.voc"));
            let empty_lang = fluree_db_indexer::run_index::LanguageTagDict::new();
            let lang_bytes =
                fluree_db_indexer::run_index::run_file::serialize_lang_dict(&empty_lang);
            std::fs::write(&lang_voc_path, &lang_bytes)?;

            let meta_sorted_info = sort_remap_and_write_sorted_commit(
                records,
                meta_subjects,
                meta_strings,
                &subj_voc_path,
                &str_voc_path,
                &commit_path,
                meta_chunk_idx,
                None, // no language tags
                None, // no types-map sidecar
            )?;

            tracing::info!(
                meta_chunk_idx,
                commit_count,
                meta_records_count,
                "txn-meta meta chunk built"
            );

            Ok::<_, std::io::Error>(meta_sorted_info)
        }))
    } else {
        None
    };

    // ---- Collect background sort/write results ----
    // Wait for all background sort_remap_and_write_sorted_commit tasks to complete.
    // Their .fsc and .voc files must exist before Phase B can merge dictionaries.
    // The meta chunk build (above) runs concurrently with this await loop.
    let mut sorted_commit_infos: Vec<SortedCommitInfo> =
        Vec::with_capacity(sort_write_handles.len() + 1);
    for handle in sort_write_handles {
        let info = handle
            .await
            .map_err(|e| ImportError::RunGeneration(format!("sort/write task panicked: {}", e)))?
            .map_err(ImportError::Io)?;
        sorted_commit_infos.push(info);
    }

    // Await meta chunk (already running in background, likely finished by now).
    if let Some(handle) = meta_chunk_handle {
        let meta_sorted_info = handle
            .await
            .map_err(|e| ImportError::RunGeneration(format!("meta chunk task panicked: {}", e)))?
            .map_err(ImportError::Io)?;
        sorted_commit_infos.push(meta_sorted_info);
    }

    // ---- Phase 3: Merge chunk dictionaries via k-way sorted merge ----
    //
    // Sorted .voc files were written alongside each commit (one per chunk).
    // Now k-way merge them to produce global forward dicts + mmap'd remap tables.
    // Memory: O(K) where K = number of chunks (no hash maps).
    config.emit_progress(ImportPhase::PreparingIndex {
        stage: "Merging dictionaries",
    });

    // Sort sorted_commit_infos by chunk_idx so downstream remap phase sees them in order.
    sorted_commit_infos.sort_by_key(|si| si.chunk_idx);

    let total_sorted_commit_records: u64 = sorted_commit_infos.iter().map(|s| s.record_count).sum();
    tracing::info!(
        chunks = sorted_commit_infos.len(),
        total_records = total_sorted_commit_records,
        "sorted commit files written"
    );

    tracing::info!(
        chunks = sorted_commit_infos.len(),
        "starting dictionary merge"
    );
    let merge_start = Instant::now();

    // Get namespace codes for IRI reconstruction (subjects.fwd needs full IRIs).
    //
    // Use Arc so Phase B parallel tasks can borrow without cloning the whole map.
    let namespace_codes: Arc<HashMap<u16, String>> =
        Arc::new(shared_alloc.lookup_codes(&published_codes));

    let remap_dir = run_dir.join("remap");
    std::fs::create_dir_all(&remap_dir)?;

    // Build vocab file paths + chunk_ids from sorted_commit_infos (deterministic naming).
    let chunk_ids: Vec<usize> = sorted_commit_infos.iter().map(|si| si.chunk_idx).collect();
    let subject_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.subjects.voc", si.chunk_idx)))
        .collect();
    let string_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.strings.voc", si.chunk_idx)))
        .collect();
    let lang_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.languages.voc", si.chunk_idx)))
        .collect();

    use fluree_db_indexer::run_index::vocab_merge;

    // Phase B can use more CPU: subject, string, and language merges are independent.
    // Run them concurrently to better utilize cores while this phase is otherwise I/O-bound.
    let run_dir_path = run_dir.to_path_buf();
    let remap_dir_path = remap_dir.to_path_buf();

    let subj_vocab_paths_for_task = subject_vocab_paths.clone();
    let chunk_ids_for_subj = chunk_ids.clone();
    let namespace_codes_for_subj = Arc::clone(&namespace_codes);
    let run_dir_for_subj = run_dir_path.clone();
    let remap_dir_for_subj = remap_dir_path.clone();
    let subj_handle = tokio::task::spawn_blocking(move || {
        vocab_merge::merge_subject_vocabs(
            &subj_vocab_paths_for_task,
            &chunk_ids_for_subj,
            &remap_dir_for_subj,
            &run_dir_for_subj,
            namespace_codes_for_subj.as_ref(),
        )
    });

    let str_vocab_paths_for_task = string_vocab_paths.clone();
    let chunk_ids_for_str = chunk_ids.clone();
    let run_dir_for_str = run_dir_path.clone();
    let remap_dir_for_str = remap_dir_path.clone();
    let str_handle = tokio::task::spawn_blocking(move || {
        vocab_merge::merge_string_vocabs(
            &str_vocab_paths_for_task,
            &chunk_ids_for_str,
            &remap_dir_for_str,
            &run_dir_for_str,
        )
    });

    let lang_vocab_paths_for_task = lang_vocab_paths.clone();
    let lang_handle = tokio::task::spawn_blocking(move || {
        fluree_db_indexer::run_index::build_lang_remap_from_vocabs(&lang_vocab_paths_for_task)
    });

    let subj_stats = subj_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("subject vocab merge panicked: {}", e)))?
        .map_err(ImportError::Io)?;
    let str_stats = str_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("string vocab merge panicked: {}", e)))?
        .map_err(ImportError::Io)?;
    let (unified_lang_dict, lang_remaps) = lang_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("language vocab merge panicked: {}", e)))?
        .map_err(|e| ImportError::RunGeneration(format!("lang remap: {}", e)))?;

    let total_unique_subjects = subj_stats.total_unique;
    let needs_wide = subj_stats.needs_wide;
    let next_string_id = str_stats.total_unique;

    tracing::info!(
        tags = unified_lang_dict.len(),
        chunks = lang_remaps.len(),
        "built unified language dict"
    );

    // Delete .voc files now that all merges (subject, string, language) are complete.
    let _ = std::fs::remove_dir_all(&vocab_dir);

    tracing::info!(
        unique_subjects = total_unique_subjects,
        unique_strings = next_string_id,
        needs_wide,
        elapsed_ms = merge_start.elapsed().as_millis(),
        "dictionary merge + persistence complete"
    );

    // Unwrap the spool_config Arc (parse workers are done, only this thread holds it).
    let spool_config = Arc::try_unwrap(spool_config).unwrap_or_else(|_| {
        panic!("spool_config Arc still shared after all parse workers completed")
    });

    // Write predicates.json (JSON array of IRI strings, indexed by predicate ID).
    {
        let pred_alloc = &spool_config.predicate_alloc;
        let pred_count = pred_alloc.len();
        let preds: Vec<String> = (0..pred_count)
            .map(|id| pred_alloc.resolve(id).unwrap_or_default())
            .collect();
        std::fs::write(
            run_dir.join("predicates.json"),
            serde_json::to_vec(&preds).map_err(|e| {
                ImportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?,
        )?;
    }

    // Write graphs.dict and datatypes.dict (FRD1 binary format).
    {
        use fluree_db_indexer::run_index::dict_io::write_predicate_dict;
        let graphs_dict = spool_config.graph_alloc.to_predicate_dict();
        write_predicate_dict(&run_dir.join("graphs.dict"), &graphs_dict)?;
        let datatypes_dict = spool_config.datatype_alloc.to_predicate_dict();
        write_predicate_dict(&run_dir.join("datatypes.dict"), &datatypes_dict)?;
    }

    // Persist namespaces.json from shared allocator.
    persist_namespaces(namespace_codes.as_ref(), run_dir)?;

    // Persist numbig arenas from shared pool.
    {
        let numbig_arenas = Arc::try_unwrap(spool_config.numbig_pool)
            .unwrap_or_else(|_| panic!("numbig_pool still shared after import"))
            .into_arenas();
        if !numbig_arenas.is_empty() {
            let nb_dir = run_dir.join("numbig");
            std::fs::create_dir_all(&nb_dir)?;
            for (&p_id, arena) in &numbig_arenas {
                fluree_db_indexer::run_index::numbig_dict::write_numbig_arena(
                    &nb_dir.join(format!("p_{}.nba", p_id)),
                    arena,
                )?;
            }
            tracing::info!(
                predicates = numbig_arenas.len(),
                total_entries = numbig_arenas.values().map(|a| a.len()).sum::<usize>(),
                "numbig arenas persisted"
            );
        }
    }

    // Persist vector arenas from shared pool.
    {
        let vector_arenas = Arc::try_unwrap(spool_config.vector_pool)
            .unwrap_or_else(|_| panic!("vector_pool still shared after import"))
            .into_arenas();
        if !vector_arenas.is_empty() {
            let vec_dir = run_dir.join("vectors");
            std::fs::create_dir_all(&vec_dir)?;
            for (&p_id, arena) in &vector_arenas {
                if arena.is_empty() {
                    continue;
                }
                let shard_paths = fluree_db_indexer::run_index::vector_arena::write_vector_shards(
                    &vec_dir, p_id, arena,
                )?;
                let shard_infos: Vec<fluree_db_indexer::run_index::vector_arena::ShardInfo> =
                    shard_paths
                        .iter()
                        .enumerate()
                        .map(|(i, path)| {
                            let cap = fluree_db_indexer::run_index::vector_arena::SHARD_CAPACITY;
                            let start = i as u32 * cap;
                            let count = (arena.len() - start).min(cap);
                            fluree_db_indexer::run_index::vector_arena::ShardInfo {
                                cas: path.display().to_string(),
                                count,
                            }
                        })
                        .collect();
                fluree_db_indexer::run_index::vector_arena::write_vector_manifest(
                    &vec_dir.join(format!("p_{}.vam", p_id)),
                    arena,
                    &shard_infos,
                )?;
            }
            tracing::info!(
                predicates = vector_arenas.len(),
                total_vectors = vector_arenas
                    .values()
                    .map(|a| a.len() as usize)
                    .sum::<usize>(),
                "vector arenas persisted"
            );
        }
    }

    tracing::info!(
        subjects = total_unique_subjects,
        predicates = spool_config.predicate_alloc.len(),
        strings = next_string_id,
        "all dictionaries persisted"
    );

    // Build dt_tags table (datatype dict ID → ValueTypeTag) for stats hook in Phase D.
    let dt_tags: Vec<fluree_db_core::value_id::ValueTypeTag> = {
        let dt_count = spool_config.datatype_alloc.len();
        (0..dt_count)
            .map(|id| {
                fluree_db_core::DatatypeDictId(id as u16)
                    .to_value_type_tag()
                    .unwrap_or(fluree_db_core::value_id::ValueTypeTag::UNKNOWN)
            })
            .collect()
    };

    // rdf_type_p_id was pre-inserted before Phase A (used for types-map sidecar +
    // SPOT merge class stats tracking).

    // In a fresh import, all ops are assertions (no retractions).
    let total_asserts = state.cumulative_flakes;
    let total_retracts = 0u64;

    // Pre-compute field widths from dict sizes (avoids re-reading dict files later).
    let p_width = {
        use fluree_db_indexer::run_index::leaflet::p_width_for_max;
        let n = spool_config.predicate_alloc.len();
        p_width_for_max(n.saturating_sub(1))
    };
    let dt_width = {
        use fluree_db_indexer::run_index::leaflet::dt_width_for_max;
        let n = spool_config.datatype_alloc.len();
        dt_width_for_max(n.saturating_sub(1))
    };

    Ok(ChunkImportResult {
        final_t: state.t,
        cumulative_flakes: state.cumulative_flakes,
        commit_head_id,
        // Phase B parallel tasks borrow `namespace_codes` via Arc. By this point
        // they are complete, so we should be able to unwrap without cloning.
        namespace_codes: Arc::try_unwrap(namespace_codes).unwrap_or_else(|arc| (*arc).clone()),
        total_commit_size,
        total_asserts,
        total_retracts,
        prefix_map: state.prefix_map,
        sorted_commit_infos,
        unified_lang_dict,
        lang_remaps,
        p_width,
        dt_width,
        dt_tags,
        rdf_type_p_id,
    })
}

// ============================================================================
// Phase 3-6: Build indexes, upload to CAS, write V4 root, publish
// ============================================================================

struct IndexUploadResult {
    root_id: fluree_db_core::ContentId,
    index_t: i64,
    summary: Option<ImportSummary>,
}

#[allow(clippy::too_many_arguments)]
async fn build_and_upload<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    input: IndexBuildInput<'_>,
    config: &ImportConfig,
    total_commit_size: u64,
    total_asserts: u64,
    total_retracts: u64,
) -> std::result::Result<IndexUploadResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher,
{
    use fluree_db_indexer::run_index::spool::{MmapStringRemap, MmapSubjectRemap};
    use fluree_db_indexer::run_index::{
        build_all_indexes, build_spot_from_sorted_commits, ClassBitsetTable, IndexRootV5,
        PrefixTrie, RunSortOrder, SortedCommitInput, SpotFromCommitsConfig,
    };
    use fluree_db_indexer::{upload_dicts_from_disk, upload_indexes_to_cas};

    // ---- Phase C (SPOT) + Phase D (remap) + Phase E (secondary build) ----
    //
    // Pipeline overlap:
    //   - Start Phase C (SPOT build from sorted commits, k-way merge)
    //   - Start Phase D→E (remap to secondary run files, THEN secondary index build)
    //   - Start dict upload (CoW tree building + CAS writes)
    //   - Phase C and Phase D run concurrently (Phase C doesn't depend on D)
    //   - Phase E chains after Phase D (needs run files from D)
    //   - After all builds complete, upload index segments to CAS
    //   - Wait for dict upload to finish (may already be done)
    let build_start = Instant::now();
    let secondary_orders = RunSortOrder::secondary_orders();

    tracing::info!(
        secondary_orders = ?secondary_orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        sorted_commits = input.sorted_commit_infos.len(),
        run_dir = %input.run_dir.display(),
        index_dir = %input.index_dir.display(),
        "starting Phase C (SPOT) + Phase D→E (remap→secondary) + dict upload (parallel)"
    );
    // Emit initial indexing progress so the bar starts moving immediately
    // after committing finishes (avoids appearance of hanging).
    // Two concurrent pipelines contribute to the shared counter:
    //   - SPOT build (Phase C): ~N flakes
    //   - Secondary build (Phase E, POST order only): ~N flakes
    // Total progress = 2 × actual flake count.
    let total_index_flakes = input.cumulative_flakes * 2;
    config.emit_progress(ImportPhase::Indexing {
        merged_flakes: 0,
        total_flakes: total_index_flakes,
        elapsed_secs: 0.0,
    });

    // Write the authoritative unified language dict to languages.dict so
    // upload_dicts_from_disk can include it. This dict was built from per-chunk
    // lang vocab files — all indexes use these global lang_ids.
    {
        let lang_dict_path = input.run_dir.join("languages.dict");
        fluree_db_indexer::run_index::dict_io::write_language_dict(
            &lang_dict_path,
            &input.unified_lang_dict,
        )?;
        tracing::info!(
            tags = input.unified_lang_dict.len(),
            path = %lang_dict_path.display(),
            "wrote authoritative unified language dict"
        );
    }

    // Start dict upload (reads flat files from run_dir, builds CoW trees, uploads to CAS).
    // This runs concurrently with the index builds below.
    let dict_upload_handle = {
        let storage = storage.clone();
        let alias = alias.to_string();
        let run_dir = input.run_dir.to_path_buf();
        let namespace_codes = input.namespace_codes.clone();
        tokio::spawn(async move {
            upload_dicts_from_disk(&storage, &alias, &run_dir, &namespace_codes).await
        })
    };

    // Shared counter incremented by each merge thread per row processed.
    let merge_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Use pre-computed field widths (avoids re-reading predicates.json / datatypes.dict).
    let p_width = input.p_width;
    let dt_width = input.dt_width;

    // ---- Phase B.5: Load remap tables + build class bitset table ----
    let remap_dir = input.run_dir.join("remap");

    // Use pre-computed lang remaps (avoids re-reading per-chunk lang vocab files).
    let lang_remaps = input.lang_remaps;

    // Load per-chunk remap tables (mmap). We need subject remaps *before* moving
    // them into spot_inputs so we can build the class bitset table.
    let n_chunks = input.sorted_commit_infos.len();
    let mut subject_remaps: Vec<MmapSubjectRemap> = Vec::with_capacity(n_chunks);
    let mut string_remaps: Vec<MmapStringRemap> = Vec::with_capacity(n_chunks);
    let mut chunk_lang_remaps: Vec<Vec<u16>> = Vec::with_capacity(n_chunks);

    for (pos, info) in input.sorted_commit_infos.iter().enumerate() {
        let ci = info.chunk_idx;
        let subj_path = remap_dir.join(format!("subjects_{ci:05}.rmp"));
        let str_path = remap_dir.join(format!("strings_{ci:05}.rmp"));
        let s_remap = MmapSubjectRemap::open(&subj_path).map_err(|e| {
            ImportError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("subject remap {}: {}", subj_path.display(), e),
            ))
        })?;
        let str_remap = MmapStringRemap::open(&str_path).map_err(|e| {
            ImportError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("string remap {}: {}", str_path.display(), e),
            ))
        })?;
        let lang_remap = lang_remaps.get(pos).cloned().unwrap_or_else(|| vec![0]);
        subject_remaps.push(s_remap);
        string_remaps.push(str_remap);
        chunk_lang_remaps.push(lang_remap);
    }

    // Build subject→class bitset table from types-map sidecars (Phase B.5).
    // Each sidecar contains (g_id, s_sorted_local, class_sorted_local) tuples
    // written during Phase A. Subject remap converts sorted-local → global sid64.
    let bitset_inputs: Vec<(&std::path::Path, &MmapSubjectRemap)> = input
        .sorted_commit_infos
        .iter()
        .zip(subject_remaps.iter())
        .filter_map(|(info, remap)| info.types_map_path.as_ref().map(|p| (p.as_path(), remap)))
        .collect();

    let class_bitset = if !bitset_inputs.is_empty() {
        let bitset_start = Instant::now();
        let table =
            ClassBitsetTable::build_from_type_maps(&bitset_inputs).map_err(ImportError::Io)?;
        tracing::info!(
            classes = table.class_count(),
            elapsed_ms = bitset_start.elapsed().as_millis(),
            "class bitset table ready for SPOT merge"
        );
        Some(table)
    } else {
        None
    };

    // Assemble SPOT inputs (move remaps directly, no boxing needed).
    let mut spot_inputs: Vec<SortedCommitInput<MmapSubjectRemap, MmapStringRemap>> =
        Vec::with_capacity(n_chunks);
    for (i, ((s_remap, str_remap), lang_remap)) in subject_remaps
        .into_iter()
        .zip(string_remaps)
        .zip(chunk_lang_remaps)
        .enumerate()
    {
        spot_inputs.push(SortedCommitInput {
            commit_path: input.sorted_commit_infos[i].path.clone(),
            subject_remap: s_remap,
            string_remap: str_remap,
            lang_remap,
        });
    }

    // ---- Phase C: Build SPOT index from sorted commit files (streaming k-way merge) ----
    let spot_leaflet_rows = config.leaflet_rows;
    let spot_leaflets_per_leaf = config.leaflets_per_leaf;
    let sec_leaflet_rows = config.leaflet_rows;
    let sec_leaflets_per_leaf = config.leaflets_per_leaf;

    let spot_index_dir = input.index_dir.to_path_buf();
    let spot_counter = merge_counter.clone();
    let spot_rdf_type_p_id = input.rdf_type_p_id;
    let spot_handle = tokio::task::spawn_blocking(move || {
        build_spot_from_sorted_commits(
            spot_inputs,
            SpotFromCommitsConfig {
                index_dir: spot_index_dir,
                p_width,
                dt_width,
                leaflet_rows: spot_leaflet_rows,
                leaflets_per_leaf: spot_leaflets_per_leaf,
                zstd_level: 1,
                progress: Some(spot_counter),
                skip_dedup: true,   // Fresh import: unique asserts, no retractions.
                skip_region3: true, // Append-only: no history journal needed.
                rdf_type_p_id: Some(spot_rdf_type_p_id),
                class_bitset,
            },
        )
    });

    // ---- Phase D→E: Remap to secondary run files, then build secondary indexes ----
    //
    // Phase D (remap) runs concurrently with Phase C (SPOT build) — no dependency.
    // Phase E (secondary index build) chains after Phase D (needs run files).
    // Both run inside a single spawn_blocking task.
    let remap_run_dir = input.run_dir.to_path_buf();
    let remap_remap_dir = remap_dir.clone();
    let sorted_commit_infos = input.sorted_commit_infos;
    let dt_tags = input.dt_tags;
    let collect_id_stats = input.collect_id_stats;
    let run_budget_mb = config.effective_run_budget_mb();
    let worker_cap = config.effective_heavy_workers();
    let remap_lang_remaps: Vec<Vec<u16>> = lang_remaps.clone();
    let secondary_counter = merge_counter.clone();
    let de_index_dir = input.index_dir.to_path_buf();

    config.emit_progress(ImportPhase::PreparingIndex {
        stage: "Remapping commits → runs",
    });

    // Phase D→E result: (secondary index results, optional stats hook).
    type DeResult = (
        Vec<(RunSortOrder, fluree_db_indexer::run_index::IndexBuildResult)>,
        Option<fluree_db_indexer::stats::IdStatsHook>,
    );

    let de_handle = tokio::task::spawn_blocking(
        move || -> std::result::Result<DeResult, ImportError> {
            use fluree_db_indexer::run_index::spool::{MmapStringRemap, MmapSubjectRemap};
            use fluree_db_indexer::run_index::{
                MultiOrderConfig, MultiOrderRunWriter, RunSortOrder as RSO,
            };

            // ---- Phase D: Parallel remap sorted commits → run files (secondary orders only) ----
            tracing::info!(
                chunks = sorted_commit_infos.len(),
                "starting parallel remap (secondary orders, concurrent with SPOT build)"
            );
            let remap_start = Instant::now();

            let worker_count = std::cmp::min(worker_cap, sorted_commit_infos.len().max(1));
            tracing::info!(
                heavy_worker_cap = worker_cap,
                remap_workers = worker_count,
                "derived remap worker count"
            );
            let per_thread_budget_bytes =
                ((run_budget_mb * 1024 * 1024) / worker_count).max(64 * 1024 * 1024);

            let mut stats_hooks: Vec<fluree_db_indexer::stats::IdStatsHook> = Vec::new();
            let mut total_remap_records: u64 = 0;

            // Bounded remap parallelism: work distribution via atomic counter.
            let next_chunk = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

            std::thread::scope(|scope| -> std::result::Result<(), ImportError> {
                let mut handles = Vec::with_capacity(worker_count);
                for _ in 0..worker_count {
                    let commit_infos_ref = &sorted_commit_infos;
                    let dt_tags_ref = &dt_tags;
                    let lang_remaps_ref = &remap_lang_remaps;
                    let remap_run_dir = remap_run_dir.clone();
                    let remap_dir = remap_remap_dir.clone();
                    let next_chunk = std::sync::Arc::clone(&next_chunk);

                    handles.push(scope.spawn(move || -> std::result::Result<(u64, Vec<fluree_db_indexer::stats::IdStatsHook>), ImportError> {
                    let mut local_total = 0u64;
                    let mut local_hooks = Vec::new();

                    loop {
                        let pos = next_chunk.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if pos >= commit_infos_ref.len() {
                            break;
                        }
                        let commit_info = &commit_infos_ref[pos];
                        let ci = commit_info.chunk_idx;
                        let chunk_run_dir = remap_run_dir.join(format!("chunk_{ci}"));
                        std::fs::create_dir_all(&chunk_run_dir)?;

                        // Load remap tables for this chunk.
                        let subj_path = remap_dir.join(format!("subjects_{ci:05}.rmp"));
                        let str_path = remap_dir.join(format!("strings_{ci:05}.rmp"));
                        let s_remap =
                            MmapSubjectRemap::open(&subj_path).map_err(ImportError::Io)?;
                        let str_remap =
                            MmapStringRemap::open(&str_path).map_err(ImportError::Io)?;

                        // Secondary orders only (PSOT/POST/OPST) — SPOT built from sorted commits in Phase C.
                        let mo_config = MultiOrderConfig {
                            total_budget_bytes: per_thread_budget_bytes,
                            orders: RSO::secondary_orders().to_vec(),
                            base_run_dir: chunk_run_dir,
                        };
                        let mut writer = MultiOrderRunWriter::new(mo_config)
                            .map_err(|e| ImportError::RunGeneration(e.to_string()))?;
                        let mut lang_dict = fluree_db_indexer::run_index::LanguageTagDict::new();

                        let mut hook = if collect_id_stats {
                            Some(fluree_db_indexer::stats::IdStatsHook::new())
                        } else {
                            None
                        };

                        let chunk_lang_remap = lang_remaps_ref
                            .get(pos)
                            .map(|v| v.as_slice())
                            .unwrap_or(&[0]);

                        use fluree_db_indexer::run_index::spool::remap_commit_to_runs;
                        let written = remap_commit_to_runs(
                            &commit_info.path,
                            commit_info.record_count,
                            &s_remap,
                            &str_remap,
                            chunk_lang_remap,
                            &mut writer,
                            &mut lang_dict,
                            hook.as_mut(),
                            Some(dt_tags_ref),
                        )
                        .map_err(|e| ImportError::RunGeneration(e.to_string()))?;

                        writer
                            .finish(&mut lang_dict)
                            .map_err(|e| ImportError::RunGeneration(e.to_string()))?;

                        local_total += written;
                        if let Some(h) = hook {
                            local_hooks.push(h);
                        }
                    }

                    Ok((local_total, local_hooks))
                }));
                }

                for h in handles {
                    let (written, mut hooks) = h.join().map_err(|_| {
                        ImportError::RunGeneration("remap thread panicked".into())
                    })??;
                    total_remap_records += written;
                    stats_hooks.append(&mut hooks);
                }

                Ok(())
            })?;

            tracing::info!(
                total_records = total_remap_records,
                elapsed_ms = remap_start.elapsed().as_millis(),
                "parallel remap complete"
            );

            // Merge stats hooks from all remap threads.
            let stats_hook = if collect_id_stats && !stats_hooks.is_empty() {
                let mut merged = stats_hooks.remove(0);
                for hook in stats_hooks {
                    merged.merge_from(hook);
                }
                Some(merged)
            } else {
                None
            };

            // ---- Phase E: Build secondary indexes from run files (PSOT/POST/OPST) ----
            // Chains after Phase D — needs the run files just written.
            tracing::info!("starting secondary index build (PSOT/POST/OPST)");
            let secondary_results = build_all_indexes(
                &remap_run_dir,
                &de_index_dir,
                secondary_orders,
                sec_leaflet_rows,
                sec_leaflets_per_leaf,
                1, // zstd_level
                Some(secondary_counter),
                true, // skip_dedup: fresh import, unique asserts only
                true, // skip_region3: append-only, no history journal needed
            )
            .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

            Ok((secondary_results, stats_hook))
        },
    );

    // Poll the merge counter every 250ms and emit progress events.
    let poll_progress = config.progress.clone();
    let poll_counter = merge_counter.clone();
    let poll_total = total_index_flakes;
    let poll_start = build_start;
    let poll_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            let merged = poll_counter.load(std::sync::atomic::Ordering::Relaxed);
            if let Some(ref cb) = poll_progress {
                cb(ImportPhase::Indexing {
                    merged_flakes: merged,
                    total_flakes: poll_total,
                    elapsed_secs: poll_start.elapsed().as_secs_f64(),
                });
            }
            // Stop when build is complete (counter won't increase further)
            if merged >= poll_total {
                break;
            }
        }
    });

    // Wait for both concurrent pipelines to complete.
    let (spot_result, spot_class_stats) = spot_handle
        .await
        .map_err(|e| ImportError::IndexBuild(format!("SPOT build task panicked: {}", e)))?
        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

    let (mut build_results, stats_hook) = de_handle
        .await
        .map_err(|e| ImportError::IndexBuild(format!("Phase D→E task panicked: {}", e)))??;

    // Combine SPOT result with secondary results.
    build_results.push((RunSortOrder::Spot, spot_result));

    // Stop the polling task
    poll_handle.abort();

    // Emit final build progress
    let merged = merge_counter.load(std::sync::atomic::Ordering::Relaxed);
    config.emit_progress(ImportPhase::Indexing {
        merged_flakes: merged,
        total_flakes: total_index_flakes,
        elapsed_secs: build_start.elapsed().as_secs_f64(),
    });

    tracing::info!(
        elapsed = ?build_start.elapsed(),
        "index build complete (SPOT from sorted commits + secondary)"
    );

    for (order, result) in &build_results {
        tracing::info!(
            order = order.dir_name().to_uppercase(),
            graphs = result.graphs.len(),
            total_rows = result.total_rows,
            elapsed = ?result.elapsed,
            "index order complete"
        );
    }

    // Upload index segments to CAS (needs build_results).
    // Dict upload may still be running — we overlap with it.
    let uploaded_indexes = upload_indexes_to_cas(storage, alias, &build_results)
        .await
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    // Wait for dict upload to complete.
    let uploaded_dicts = dict_upload_handle
        .await
        .map_err(|e| ImportError::Upload(format!("dict upload task panicked: {}", e)))?
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    tracing::info!(
        elapsed = ?build_start.elapsed(),
        default_orders = uploaded_indexes.default_graph_orders.len(),
        named_graphs = uploaded_indexes.named_graphs.len(),
        "index build + CAS upload complete (overlapped)"
    );

    // ---- Build predicate SIDs via PrefixTrie (no BinaryIndexStore needed) ----
    tracing::info!("post-upload: building predicate SIDs");
    let trie = PrefixTrie::from_namespace_codes(input.namespace_codes);
    let predicates_path = input.run_dir.join("predicates.json");
    let predicate_sids: Vec<(u16, String)> = if predicates_path.exists() {
        let bytes = std::fs::read(&predicates_path)?;
        let by_id: Vec<String> = serde_json::from_slice(&bytes).map_err(|e| {
            ImportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        by_id
            .iter()
            .map(|iri| match trie.longest_match(iri) {
                Some((code, prefix_len)) => (code, iri[prefix_len..].to_string()),
                None => (0, iri.clone()),
            })
            .collect()
    } else {
        Vec::new()
    };

    // ---- Build IndexStats directly ----
    // Preferred: ID-based stats collected during commit resolution (per-graph property
    // stats with datatype counts + HLL NDV). Includes HLL sketch persistence so
    // incremental index refresh can merge registers. Fallback: SPOT build result for
    // per-graph flake counts only.
    tracing::info!("post-upload: predicate SIDs built, starting stats");
    let (index_stats, sketch_ref, summary) = if let Some(hook) = stats_hook {
        // Persist HLL sketches BEFORE finalize consumes the hook.
        tracing::info!("post-upload: serializing HLL sketch blob");
        let sketch_blob = fluree_db_indexer::stats::HllSketchBlob::from_properties(
            input.final_t,
            hook.properties(),
        );
        let sketch_bytes = sketch_blob
            .to_json_bytes()
            .map_err(|e| ImportError::Upload(format!("serialize HLL sketch: {}", e)))?;
        let sketch_write = storage
            .content_write_bytes(ContentKind::StatsSketch, alias, &sketch_bytes)
            .await
            .map_err(|e| ImportError::Upload(format!("write HLL sketch: {}", e)))?;
        let sketch_cid =
            ContentId::from_hex_digest(CODEC_FLUREE_STATS_SKETCH, &sketch_write.content_hash)
                .expect("valid SHA-256 hash from storage write");

        tracing::info!(
            "post-upload: HLL sketch written, calling finalize_with_aggregate_properties"
        );
        let (id_result, agg_props, _class_counts, _class_properties, _class_ref_targets) =
            hook.finalize_with_aggregate_properties();
        tracing::info!("post-upload: finalize complete, building IndexStats");

        // Per-graph stats (already the correct type).
        let graphs = id_result.graphs;

        // Aggregate properties: convert from p_id-keyed to SID-keyed.
        let mut properties: Vec<fluree_db_core::index_stats::PropertyStatEntry> = agg_props
            .iter()
            .filter_map(|p| {
                let sid = predicate_sids.get(p.p_id as usize)?;
                Some(fluree_db_core::index_stats::PropertyStatEntry {
                    sid: (sid.0, sid.1.clone()),
                    count: p.count,
                    ndv_values: p.ndv_values,
                    ndv_subjects: p.ndv_subjects,
                    last_modified_t: p.last_modified_t,
                    datatypes: p.datatypes.clone(),
                })
            })
            .collect();
        properties.sort_by(|a, b| a.sid.0.cmp(&b.sid.0).then_with(|| a.sid.1.cmp(&b.sid.1)));

        // Class stats from SPOT merge.
        let classes = if let Some(ref cs) = spot_class_stats {
            let entries = fluree_db_indexer::stats::build_class_stat_entries(
                cs,
                &predicate_sids,
                input.run_dir,
                input.namespace_codes,
            )
            .map_err(ImportError::Io)?;
            if entries.is_empty() {
                None
            } else {
                Some(entries)
            }
        } else {
            None
        };

        tracing::info!(
            property_stats = properties.len(),
            graph_count = graphs.len(),
            class_count = classes.as_ref().map_or(0, |c| c.len()),
            total_flakes = id_result.total_flakes,
            "stats collected from IdStatsHook"
        );

        // Build CLI display summary from already-computed stats.
        let summary = build_import_summary(
            spot_class_stats.as_ref(),
            &agg_props,
            &predicate_sids,
            input.namespace_codes,
            input.run_dir,
            input.prefix_map,
        );

        let stats = fluree_db_core::index_stats::IndexStats {
            flakes: id_result.total_flakes,
            size: 0,
            properties: if properties.is_empty() {
                None
            } else {
                Some(properties)
            },
            classes,
            graphs: Some(graphs),
        };

        (stats, Some(sketch_cid), Some(summary))
    } else {
        // Fallback: flake counts only (no per-property / datatype breakdown).
        let (_, spot_result) = build_results
            .iter()
            .find(|(order, _)| *order == RunSortOrder::Spot)
            .expect("SPOT index must always be present in build results");

        let graphs: Vec<fluree_db_core::index_stats::GraphStatsEntry> = spot_result
            .graphs
            .iter()
            .map(|g| fluree_db_core::index_stats::GraphStatsEntry {
                g_id: g.g_id,
                flakes: g.total_rows,
                size: 0,
                properties: Vec::new(),
            })
            .collect();

        let total_flakes: u64 = spot_result.graphs.iter().map(|g| g.total_rows).sum();

        let stats = fluree_db_core::index_stats::IndexStats {
            flakes: total_flakes,
            size: 0,
            properties: None,
            classes: None,
            graphs: Some(graphs),
        };

        (stats, None, None)
    };

    // ---- Phase 5: Build IndexRootV5 (binary IRB1) ----
    tracing::info!("post-upload: stats built, constructing IRB1 root");

    // Convert DictRefs (string-keyed maps) → DictRefsV5 (u32 vecs).
    let dict_refs_v5 = {
        let dr = uploaded_dicts.dict_refs;
        use fluree_db_indexer::run_index::{DictRefsV5, VectorDictRefV5};
        let numbig: Vec<(u32, ContentId)> = dr
            .numbig
            .iter()
            .map(|(k, v)| (k.parse::<u32>().unwrap_or(0), v.clone()))
            .collect();
        let vectors: Vec<VectorDictRefV5> = dr
            .vectors
            .iter()
            .map(|(k, v)| VectorDictRefV5 {
                p_id: k.parse::<u32>().unwrap_or(0),
                manifest: v.manifest.clone(),
                shards: v.shards.clone(),
            })
            .collect();
        DictRefsV5 {
            forward_packs: dr.forward_packs,
            subject_reverse: dr.subject_reverse,
            string_reverse: dr.string_reverse,
            numbig,
            vectors,
        }
    };

    let ns_codes: std::collections::BTreeMap<u16, String> = input
        .namespace_codes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();

    let root = IndexRootV5 {
        ledger_id: alias.to_string(),
        index_t: input.final_t,
        base_t: 0, // fresh import
        subject_id_encoding: uploaded_dicts.subject_id_encoding,
        namespace_codes: ns_codes,
        predicate_sids,
        graph_iris: uploaded_dicts.graph_iris,
        datatype_iris: uploaded_dicts.datatype_iris,
        language_tags: uploaded_dicts.language_tags,
        dict_refs: dict_refs_v5,
        subject_watermarks: uploaded_dicts.subject_watermarks,
        string_watermark: uploaded_dicts.string_watermark,
        total_commit_size,
        total_asserts,
        total_retracts,
        default_graph_orders: uploaded_indexes.default_graph_orders,
        named_graphs: uploaded_indexes.named_graphs,
        stats: Some(index_stats),
        schema: None,
        prev_index: None, // fresh import
        garbage: None,    // fresh import
        sketch_ref,
    };

    tracing::info!("post-upload: IRB1 root constructed, encoding binary");
    let root_bytes = root.encode();
    tracing::info!(
        root_bytes = root_bytes.len(),
        "post-upload: root encoded, writing to CAS"
    );

    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, alias, &root_bytes)
        .await
        .map_err(|e| ImportError::Upload(format!("write IRB1 root: {}", e)))?;

    // Derive ContentId from the root's content hash
    let root_id = ContentId::from_hex_digest(CODEC_FLUREE_INDEX_ROOT, &write_result.content_hash)
        .expect("valid SHA-256 hash from storage write");

    tracing::info!(
        root_id = %root_id,
        index_t = input.final_t,
        "IRB1 index root written to CAS"
    );

    // ---- Phase 6: Publish ----
    if config.publish {
        nameservice
            .publish_index(alias, input.final_t, &root_id)
            .await
            .map_err(|e| ImportError::Storage(format!("publish index: {}", e)))?;
        tracing::info!(
            index_t = input.final_t,
            root_id = %root_id,
            "index published to nameservice"
        );
    }

    Ok(IndexUploadResult {
        root_id,
        index_t: input.final_t,
        summary,
    })
}

/// Build a lightweight import summary for CLI display.
///
/// Extracts top-5 classes, properties, and connections from the already-computed
/// stats data. Uses the subject dict files on disk for class IRI resolution.
fn build_import_summary(
    spot_class_stats: Option<&fluree_db_indexer::run_index::SpotClassStats>,
    agg_props: &[fluree_db_core::GraphPropertyStatEntry],
    predicate_sids: &[(u16, String)],
    namespace_codes: &HashMap<u16, String>,
    run_dir: &Path,
    prefix_map: &HashMap<String, String>,
) -> ImportSummary {
    // Build IRI → compact form using turtle @prefix declarations + well-known builtins.
    // prefix_map is IRI → short (e.g., "https://dblp.org/rdf/schema#" → "dblp").
    // Builtins fill in common prefixes that may not appear in @prefix.
    let builtin_prefixes: &[(&str, &str)] = &[
        (fluree_vocab::rdf::NS, "rdf"),
        (fluree_vocab::rdfs::NS, "rdfs"),
        (fluree_vocab::xsd::NS, "xsd"),
        (fluree_vocab::owl::NS, "owl"),
        (fluree_vocab::shacl::NS, "sh"),
    ];
    let mut iri_to_short: Vec<(&str, &str)> = prefix_map
        .iter()
        .map(|(iri, short)| (iri.as_str(), short.as_str()))
        .collect();
    for &(iri, short) in builtin_prefixes {
        if !prefix_map.contains_key(iri) {
            iri_to_short.push((iri, short));
        }
    }
    // Sort longest-first so longest match wins.
    iri_to_short.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    let compact = |full_iri: &str| -> String {
        for &(ns_iri, short) in &iri_to_short {
            if let Some(suffix) = full_iri.strip_prefix(ns_iri) {
                return format!("{}:{}", short, suffix);
            }
        }
        full_iri.to_string()
    };
    // ---- Top 5 properties by count ----
    let mut props_sorted: Vec<_> = agg_props.iter().collect();
    props_sorted.sort_by(|a, b| b.count.cmp(&a.count));
    let top_properties: Vec<(String, u64)> = props_sorted
        .iter()
        .take(5)
        .filter_map(|p| {
            let (ns_code, suffix) = predicate_sids.get(p.p_id as usize)?;
            let ns_iri = namespace_codes
                .get(ns_code)
                .map(|s| s.as_str())
                .unwrap_or("");
            Some((compact(&format!("{}{}", ns_iri, suffix)), p.count))
        })
        .collect();

    let cs = match spot_class_stats {
        Some(cs) if !cs.class_counts.is_empty() => cs,
        _ => {
            return ImportSummary {
                top_classes: Vec::new(),
                top_properties,
                top_connections: Vec::new(),
            };
        }
    };

    // Build SID resolver from dict files on disk.
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_indexer::run_index::dict_io;

    let resolve_sid_to_iri = |sid64: u64| -> Option<String> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let prefix = namespace_codes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        // Try to resolve via dict files; fall back to numeric representation.
        let sids_path = run_dir.join("subjects.sids");
        let idx_path = run_dir.join("subjects.idx");
        let fwd_path = run_dir.join("subjects.fwd");

        let sids_vec = dict_io::read_subject_sid_map(&sids_path).ok()?;
        let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path).ok()?;
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        let mut file = std::fs::File::open(&fwd_path).ok()?;
        use std::io::{Read as _, Seek as _, SeekFrom};
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some(format!("{}{}", prefix, suffix))
    };

    // Cache resolved IRIs to avoid repeated file I/O.
    let mut iri_cache: HashMap<u64, Option<String>> = HashMap::new();
    let mut resolve_cached = |sid64: u64| -> Option<String> {
        iri_cache
            .entry(sid64)
            .or_insert_with(|| resolve_sid_to_iri(sid64))
            .clone()
    };

    // ---- Top 5 classes by count ----
    let mut classes_sorted: Vec<_> = cs.class_counts.iter().collect();
    classes_sorted.sort_by(|a, b| b.1.cmp(a.1));
    let top_classes: Vec<(String, u64)> = classes_sorted
        .iter()
        .take(5)
        .filter_map(|(&sid, &count)| Some((compact(&resolve_cached(sid)?), count)))
        .collect();

    // ---- Top 5 connections: Class → property → Class ----
    let mut connections: Vec<(u64, u32, u64, u64)> = Vec::new();
    for (&src_class, prop_map) in &cs.class_prop_refs {
        for (&p_id, target_map) in prop_map {
            for (&target_class, &count) in target_map {
                connections.push((src_class, p_id, target_class, count));
            }
        }
    }
    connections.sort_by(|a, b| b.3.cmp(&a.3));
    let top_connections: Vec<(String, String, String, u64)> = connections
        .iter()
        .take(5)
        .filter_map(|&(src, p_id, tgt, count)| {
            let src_iri = compact(&resolve_cached(src)?);
            let tgt_iri = compact(&resolve_cached(tgt)?);
            let (ns_code, suffix) = predicate_sids.get(p_id as usize)?;
            let ns_iri = namespace_codes
                .get(ns_code)
                .map(|s| s.as_str())
                .unwrap_or("");
            Some((
                src_iri,
                compact(&format!("{}{}", ns_iri, suffix)),
                tgt_iri,
                count,
            ))
        })
        .collect();

    ImportSummary {
        top_classes,
        top_properties,
        top_connections,
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Generate a unique session identifier for directory naming.
///
/// Uses nanosecond timestamp XOR'd for uniqueness. Not cryptographic,
/// just unique enough for concurrent session directories.
fn session_id() -> String {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", seed ^ (seed >> 64))
}

/// Derive the session directory path.
///
/// Uses `{temp_dir}/fluree-import/{alias_prefix}/tmp_import/{session_id}/`.
/// The cleanup phase removes this directory on success; on failure it is
/// kept for debugging (logged with full path).
fn derive_session_dir<S: Storage>(_storage: &S, alias_prefix: &str, sid: &str) -> PathBuf {
    // Allow overriding import scratch space for large imports.
    //
    // This is critical on macOS where `std::env::temp_dir()` often points to a
    // small system volume. For multi-GB TTL imports, run files + spool files
    // can exceed hundreds of GB temporarily.
    //
    // Set `FLUREE_IMPORT_DIR=/path/with/space` to force the base directory.
    let base = std::env::var_os("FLUREE_IMPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("fluree-import"));
    base.join(alias_prefix).join("tmp_import").join(sid)
}

/// Build a JSON-LD @context from turtle prefix declarations + built-in namespaces,
/// write it to CAS, and push it as the ledger's default context via nameservice config.
async fn store_default_context<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    turtle_prefix_map: &HashMap<String, String>,
) -> std::result::Result<(), ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: fluree_db_nameservice::ConfigPublisher,
{
    use fluree_db_nameservice::{ConfigPayload, ConfigValue};

    // Build IRI → short prefix map, starting with well-known built-in prefixes.
    // Turtle-declared prefixes override built-ins if they map the same IRI.
    let builtin_prefixes: &[(&str, &str)] = &[
        (fluree_vocab::rdf::NS, "rdf"),
        (fluree_vocab::rdfs::NS, "rdfs"),
        (fluree_vocab::xsd::NS, "xsd"),
        (fluree_vocab::owl::NS, "owl"),
        (fluree_vocab::shacl::NS, "sh"),
        (fluree_vocab::geo::NS, "geo"),
    ];

    let mut context_map = serde_json::Map::new();

    // Add built-ins first
    for &(iri, short) in builtin_prefixes {
        context_map.insert(
            short.to_string(),
            serde_json::Value::String(iri.to_string()),
        );
    }

    // Overlay turtle-declared prefixes (IRI → short name)
    for (iri, short) in turtle_prefix_map {
        context_map.insert(short.clone(), serde_json::Value::String(iri.clone()));
    }

    if context_map.is_empty() {
        return Ok(());
    }

    let context_json = serde_json::Value::Object(context_map);
    let context_bytes = serde_json::to_vec(&context_json)
        .map_err(|e| ImportError::Storage(format!("serialize default context: {}", e)))?;

    // Write to CAS via ContentStore (returns CID)
    let cs = fluree_db_core::content_store_for(storage.clone(), alias);
    let cid = cs
        .put(ContentKind::LedgerConfig, &context_bytes)
        .await
        .map_err(|e| ImportError::Storage(format!("write default context to CAS: {}", e)))?;

    tracing::info!(
        cid = %cid,
        prefixes = context_json.as_object().map(|m| m.len()).unwrap_or(0),
        "default context written to CAS"
    );

    // Read current config before push (needed for GC of old blob)
    let current_config = nameservice
        .get_config(alias)
        .await
        .map_err(|e| ImportError::Storage(format!("get config: {}", e)))?;

    let old_default_context = current_config
        .as_ref()
        .and_then(|c| c.payload.as_ref())
        .and_then(|p| p.default_context.clone());

    // Push new CID to nameservice config
    let new_config = ConfigValue::new(
        current_config.as_ref().map_or(1, |c| c.v + 1),
        Some(ConfigPayload::with_default_context(cid.clone())),
    );

    nameservice
        .push_config(alias, current_config.as_ref(), &new_config)
        .await
        .map_err(|e| ImportError::Storage(format!("push default context config: {}", e)))?;

    tracing::info!("default context published to nameservice config");

    // GC: best-effort delete of the old context blob if CID changed
    if let Some(old_cid) = old_default_context {
        if old_cid != cid {
            let kind = old_cid.content_kind().unwrap_or(ContentKind::LedgerConfig);
            let addr = fluree_db_core::content_address(
                storage.storage_method(),
                kind,
                alias,
                &old_cid.digest_hex(),
            );
            if let Err(e) = storage.delete(&addr).await {
                tracing::debug!(%e, old_addr = %addr, "could not GC old default context blob");
            }
        }
    }

    Ok(())
}
