//! Bulk import pipeline: TTL → commits → runs → indexes → CAS → publish.
//!
//! Provides `.create("mydb").import("/path/to/chunks/").execute().await` API
//! on [`Fluree`] for high-throughput bulk import of Turtle data.
//!
//! ## Pipeline overview
//!
//! 1. **Create ledger** — `nameservice.publish_ledger_init(ledger_id)`
//! 2. **Import TTL → commits + runs** — parallel chunk parsing, serial commit
//!    finalization, streaming run generation via background resolver thread
//! 3. **Build indexes** — `build_all_indexes()` from completed run files
//! 4. **CAS upload** — dicts + indexes uploaded to content-addressed storage
//! 5. **V2 root** — `BinaryIndexRoot::from_cas_artifacts()` written to CAS
//! 6. **Publish** — `nameservice.publish_index_allow_equal()`
//! 7. **Cleanup** — remove tmp session directory (only on full success)
//!
//! ## Performance invariants
//!
//! The pipeline maintains three session-scoped singletons for correctness and
//! streaming throughput:
//!
//! - **One `GlobalDicts`** — monotonic global dictionary assignment across all chunks.
//! - **One `CommitResolver`** — resolves triples through the shared GlobalDicts.
//! - **One `MultiOrderRunWriter`** — fed RunRecords during commit ingestion (no second pass).
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
    /// Run writer memory budget in MB. 0 = derive from memory budget. Default: 0.
    pub run_budget_mb: usize,
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
    /// Used to derive `chunk_size_mb`, `max_inflight_chunks`, and `run_budget_mb`
    /// when those fields are left at 0.
    pub memory_budget_mb: usize,
    /// Chunk size in MB for splitting a single large Turtle file. 0 = derive from budget.
    pub chunk_size_mb: usize,
    /// Maximum number of chunk texts materialized in memory simultaneously.
    /// 0 = derive from budget.
    pub max_inflight_chunks: usize,
    /// Whether `run_budget_mb` was explicitly set (vs derived from memory budget).
    run_budget_explicit: bool,
    /// Optional progress callback invoked at key pipeline milestones.
    pub progress: Option<ProgressFn>,
}

impl std::fmt::Debug for ImportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportConfig")
            .field("parse_threads", &self.parse_threads)
            .field("memory_budget_mb", &self.memory_budget_mb)
            .field("chunk_size_mb", &self.chunk_size_mb)
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
            run_budget_mb: 0,
            build_index: true,
            publish: true,
            cleanup_local_files: true,
            compress_commits: true,
            collect_id_stats: true,
            publish_every: 50,
            memory_budget_mb: 0,
            chunk_size_mb: 0,
            max_inflight_chunks: 0,
            run_budget_explicit: false,
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
        raw.clamp(128, 512)
    }

    /// Effective run budget in MB (derived from budget if not explicitly set).
    pub fn effective_run_budget_mb(&self) -> usize {
        if self.run_budget_explicit && self.run_budget_mb > 0 {
            return self.run_budget_mb;
        }
        let budget_mb = self.effective_memory_budget_mb();
        let chunk_size = self.effective_chunk_size_mb();
        // Run budget = min(chunk_size, budget / 3)
        chunk_size.min(budget_mb / 3).max(256)
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
            run_budget_mb: self.effective_run_budget_mb(),
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
    /// Run budget in MB for multi-order indexing (derived when not set).
    pub run_budget_mb: usize,
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
    Streaming(fluree_db_transact::turtle_splitter::StreamingTurtleReader),
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
        let scan_progress: Option<fluree_db_transact::turtle_splitter::ScanProgressFn> =
            config.progress.as_ref().map(|cb| {
                let cb = Arc::clone(cb);
                let f: fluree_db_transact::turtle_splitter::ScanProgressFn =
                    Arc::new(move |bytes_read, total_bytes| {
                        cb(ImportPhase::Scanning {
                            bytes_read,
                            total_bytes,
                        });
                    });
                f
            });

        let reader = fluree_db_transact::turtle_splitter::StreamingTurtleReader::new(
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

    /// Set the run writer memory budget in MB. Overrides budget derivation.
    pub fn run_budget_mb(mut self, mb: usize) -> Self {
        self.config.run_budget_mb = mb;
        self.config.run_budget_explicit = true;
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
    /// Optional stats hook from commit resolution.
    stats_hook: Option<fluree_db_indexer::stats::IdStatsHook>,
    /// Total flakes from commit resolution (used for indexing progress).
    cumulative_flakes: u64,
}

/// Run phases 2-6: import chunks, build indexes, upload to CAS, write V2 root, publish.
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

    if config.build_index {
        let build_input = IndexBuildInput {
            run_dir: paths.run_dir,
            index_dir: paths.index_dir,
            final_t: import_result.final_t,
            namespace_codes: &import_result.namespace_codes,
            stats_hook: import_result.stats_hook,
            cumulative_flakes: import_result.cumulative_flakes,
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
    } else {
        root_id = None;
        index_t = 0;
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
    })
}

// ============================================================================
// Phase 2: Import chunks
// ============================================================================

/// Internal result from the import phase (before index build).
struct ChunkImportResult {
    final_t: i64,
    cumulative_flakes: u64,
    commit_head_id: fluree_db_core::ContentId,
    namespace_codes: HashMap<u16, String>,
    stats_hook: Option<fluree_db_indexer::stats::IdStatsHook>,
    /// Total size of all commit blobs in bytes.
    total_commit_size: u64,
    /// Total number of assertions across all commits.
    total_asserts: u64,
    /// Total number of retractions across all commits.
    total_retracts: u64,
    /// Turtle @prefix short names accumulated across all chunks: IRI → short prefix.
    prefix_map: HashMap<String, String>,
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
    use fluree_db_indexer::run_index::{
        persist_namespaces, CommitResolver, GlobalDicts, MultiOrderConfig, MultiOrderRunWriter,
        RunGenerationResult, RunSortOrder,
    };
    use fluree_db_transact::import::{
        finalize_parsed_chunk, import_commit, import_commit_with_prelude, import_trig_commit,
        parse_chunk, parse_chunk_with_prelude, ImportState, ParsedChunk,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let is_streaming = chunk_source.is_streaming();
    let estimated_total = chunk_source.estimated_len();
    let compress = config.compress_commits;
    let num_threads = config.parse_threads;
    let mut state = ImportState::new();
    let run_start = Instant::now();
    let collect_id_stats = config.collect_id_stats;

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

    // ---- Spawn background run resolver (three session-scoped singletons) ----
    std::fs::create_dir_all(run_dir)?;
    let run_budget = config.effective_run_budget_mb();
    let budget = run_budget * 1024 * 1024;
    let mo_config = MultiOrderConfig {
        total_budget_bytes: budget,
        orders: RunSortOrder::all_build_orders().to_vec(),
        base_run_dir: run_dir.to_path_buf(),
    };

    // Bounded channel: backpressures import if resolver falls behind.
    //
    // Capacity tradeoff:
    // - Larger capacity increases pipeline overlap (parse/commit can run ahead),
    //   keeping CPU utilization higher when the resolver thread is the bottleneck.
    // - Costs memory proportional to buffered commit blob sizes.
    //
    // Use a moderate multiple of parse threads (no new CLI knob).
    //
    // We clamp to avoid unbounded memory growth if commit blobs are large.
    // If the resolver thread is slower than parse/commit finalization, this
    // queue provides overlap without immediately stalling the pipeline.
    let run_queue_cap = (num_threads.saturating_mul(8)).clamp(8, 64);
    let (run_tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, String)>(run_queue_cap);

    let run_dir_clone = run_dir.to_path_buf();
    let resolver_handle: std::thread::JoinHandle<std::result::Result<RunGenerationResult, String>> =
        std::thread::Builder::new()
            .name("run-resolver".into())
            .spawn(move || {
                // Singleton 1: GlobalDicts (file-backed subjects + strings in run_dir)
                let mut dicts =
                    GlobalDicts::new(&run_dir_clone).map_err(|e| format!("init dicts: {}", e))?;
                // Singleton 2: CommitResolver
                let mut resolver = CommitResolver::new();
                if collect_id_stats {
                    // Enable class tracking (class counts + class→property presence).
                    //
                    // NOTE: We intentionally disable ref-target tracking here (the
                    // class→property→ref-class edges) because it can be very memory
                    // intensive. We can compute ref edges later via a background job
                    // or a second pass over subject-grouped records.
                    let rdf_type_p_id = dicts.predicates.get_or_insert(fluree_vocab::rdf::TYPE);
                    let mut stats_hook = fluree_db_indexer::stats::IdStatsHook::new();
                    stats_hook.set_rdf_type_p_id(rdf_type_p_id);
                    stats_hook.set_track_ref_targets(false);
                    resolver.set_stats_hook(stats_hook);
                }

                // Singleton 3: MultiOrderRunWriter
                let mut writer = MultiOrderRunWriter::new(mo_config)
                    .map_err(|e| format!("init multi-order writer: {}", e))?;
                let mut commit_count = 0usize;

                // Accumulate commit statistics
                let mut total_commit_size = 0u64;
                let mut total_asserts = 0u64;
                let mut total_retracts = 0u64;

                while let Ok((bytes, commit_address)) = rx.recv() {
                    let resolved = resolver
                        .resolve_blob(&bytes, &commit_address, &mut dicts, &mut writer)
                        .map_err(|e| format!("{}", e))?;
                    commit_count += 1;

                    // Accumulate totals
                    total_commit_size += resolved.size;
                    total_asserts += resolved.asserts as u64;
                    total_retracts += resolved.retracts as u64;

                    tracing::info!(
                        commit = commit_count,
                        t = resolved.t,
                        ops = resolved.total_records,
                        total_records = writer.total_records(),
                        runs = writer.run_count(),
                        subjects = dicts.subjects.len(),
                        predicates = dicts.predicates.len(),
                        "commit resolved"
                    );
                }

                // Flush remaining run buffers
                let order_results = writer
                    .finish(&mut dicts.languages)
                    .map_err(|e| format!("writer finish: {}", e))?;

                let mut all_run_files = Vec::new();
                let mut total_records = 0u64;
                for (order, result) in &order_results {
                    tracing::info!(
                        order = order.dir_name(),
                        run_files = result.run_files.len(),
                        records = result.total_records,
                        "order run generation complete"
                    );
                    all_run_files.extend(result.run_files.iter().cloned());
                    total_records += result.total_records;
                }

                // Persist dictionaries for index build
                dicts
                    .persist(&run_dir_clone)
                    .map_err(|e| format!("dict persist: {}", e))?;

                // Persist namespace map
                persist_namespaces(resolver.ns_prefixes(), &run_dir_clone)
                    .map_err(|e| format!("namespace persist: {}", e))?;

                // Persist reverse indexes
                dicts
                    .subjects
                    .write_reverse_index(&run_dir_clone.join("subjects.rev"))
                    .map_err(|e| format!("subjects.rev: {}", e))?;
                dicts
                    .strings
                    .write_reverse_index(&run_dir_clone.join("strings.rev"))
                    .map_err(|e| format!("strings.rev: {}", e))?;

                Ok(RunGenerationResult {
                    run_files: all_run_files,
                    subject_count: dicts.subjects.len(),
                    predicate_count: dicts.predicates.len(),
                    string_count: dicts.strings.len(),
                    needs_wide: dicts.subjects.needs_wide(),
                    total_records,
                    commit_count,
                    stats_hook: resolver.take_stats_hook(),
                    total_commit_size,
                    total_asserts,
                    total_retracts,
                })
            })
            .map_err(|e| ImportError::RunGeneration(format!("spawn resolver: {}", e)))?;

    // ---- Phase 2a: Parse + commit chunk 0 serially (establishes namespaces) ----
    // For streaming: pull from channel. For files: read by index.
    let streaming_prelude = if is_streaming {
        match &**chunk_source {
            ChunkSource::Streaming(reader) => Some(reader.prelude().clone()),
            _ => None,
        }
    } else {
        None
    };
    let chunk0_content = if is_streaming {
        // Streaming: receive first chunk (reader thread already read it from disk).
        chunk_source.recv_next()?.map(|(_idx, text)| text)
    } else if estimated_total > 0 {
        Some(chunk_source.read_chunk(0)?)
    } else {
        None
    };

    if let Some(content) = chunk0_content {
        let size_mb = content.len() as f64 / (1024.0 * 1024.0);
        let is_trig = if !is_streaming {
            chunk_source.is_trig(0)
        } else {
            false
        };
        tracing::info!(
            chunk = 1,
            estimated_total,
            size_mb = format!("{:.1}", size_mb),
            is_trig,
            starts_with = &content[..content.len().min(200)],
            "parsing chunk 0 serially (establishes namespaces)"
        );

        config.emit_progress(ImportPhase::Parsing {
            chunk: 1,
            total: estimated_total,
            chunk_bytes: content.len() as u64,
        });

        let result = if is_trig {
            import_trig_commit(&mut state, &content, storage, alias, compress)
                .await
                .map_err(|e| ImportError::Transact(e.to_string()))?
        } else if is_streaming {
            // Streaming path: parse raw chunk data using the pre-extracted prelude.
            let prelude = streaming_prelude
                .as_ref()
                .expect("streaming prelude must exist when streaming");
            import_commit_with_prelude(&mut state, &content, prelude, storage, alias, compress)
                .await
                .map_err(|e| ImportError::Transact(e.to_string()))?
        } else {
            import_commit(&mut state, &content, storage, alias, compress)
                .await
                .map_err(|e| ImportError::Transact(e.to_string()))?
        };

        tracing::info!(
            t = result.t,
            flakes = result.flake_count,
            blob_bytes = result.blob_bytes,
            "chunk 0 committed"
        );

        // Emit progress for chunk 0 (was previously missing).
        config.emit_progress(ImportPhase::Committing {
            chunk: 1,
            total: estimated_total,
            cumulative_flakes: state.cumulative_flakes,
            elapsed_secs: run_start.elapsed().as_secs_f64(),
        });

        // Feed to resolver (pass content hash hex for metadata)
        let hash_hex = result.commit_id.digest_hex();
        tokio::task::block_in_place(|| {
            run_tx
                .send((result.commit_blob, hash_hex))
                .map_err(|_| ImportError::RunGeneration("resolver exited unexpectedly".into()))
        })?;
    }

    // ---- Phase 2b: Parse remaining chunks in parallel, commit serially ----
    //
    // Create the shared namespace allocator from chunk 0's registry.
    // All codes published so far (predefined defaults + chunk 0's delta)
    // are tracked so subsequent commits only publish genuinely new codes.
    use fluree_db_transact::SharedNamespaceAllocator;
    use fluree_vocab::namespaces::OVERFLOW;
    use rustc_hash::FxHashSet;

    let shared_alloc = Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry));
    let mut published_codes: FxHashSet<u16> = state.ns_registry.all_codes();

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

            let handle = std::thread::Builder::new()
                .name(format!("ttl-parser-{}", thread_idx))
                .spawn(move || {
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
                        match parse_chunk_with_prelude(
                            &ttl,
                            &shared_alloc,
                            &prelude,
                            t,
                            &ledger,
                            compress,
                        ) {
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

        // Serial commit loop: receive parsed chunks, reorder, finalize in order.
        // Streaming chunks arrive out of order from parallel workers.
        let mut next_expected: usize = 1;
        let mut pending: BTreeMap<usize, ParsedChunk> = BTreeMap::new();

        for recv_result in result_rx {
            let (idx, parsed) = recv_result.map_err(ImportError::Transact)?;

            pending.insert(idx, parsed);

            while let Some(parsed) = pending.remove(&next_expected) {
                // Commit-order publication: determine which codes from this chunk
                // need to be introduced in this commit's namespace_delta.
                let unpublished: FxHashSet<u16> = parsed
                    .new_codes
                    .iter()
                    .copied()
                    .filter(|c| *c < OVERFLOW && !published_codes.contains(c))
                    .collect();
                let ns_delta = if unpublished.is_empty() {
                    std::collections::HashMap::new()
                } else {
                    shared_alloc.lookup_codes(&unpublished)
                };
                published_codes.extend(&unpublished);

                let result = finalize_parsed_chunk(&mut state, parsed, ns_delta, storage, alias)
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?;

                let total_elapsed = run_start.elapsed().as_secs_f64();
                tracing::info!(
                    chunk = next_expected + 1,
                    estimated_total,
                    t = result.t,
                    flakes = result.flake_count,
                    cumulative_flakes = state.cumulative_flakes,
                    flakes_per_sec = format!(
                        "{:.2}M",
                        state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0
                    ),
                    "chunk committed"
                );
                config.emit_progress(ImportPhase::Committing {
                    chunk: next_expected + 1,
                    total: estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: total_elapsed,
                });

                // Feed to resolver
                let resolver_send_failed = {
                    let hash_hex = result.commit_id.digest_hex();
                    tokio::task::block_in_place(|| {
                        run_tx.send((result.commit_blob, hash_hex)).is_err()
                    })
                };
                if resolver_send_failed {
                    drop(run_tx);
                    let err = match resolver_handle.join() {
                        Ok(Err(e)) => format!("resolver failed: {}", e),
                        Err(p) => format!("resolver panicked: {:?}", p),
                        Ok(Ok(_)) => "resolver exited unexpectedly".to_string(),
                    };
                    return Err(ImportError::RunGeneration(err));
                }

                // Periodic nameservice checkpoint
                if config.publish_every > 0
                    && (next_expected + 1).is_multiple_of(config.publish_every)
                {
                    nameservice
                        .publish_commit(alias, result.t, &result.commit_id)
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
        let has_trig = (1..estimated_total).any(|i| chunk_source.is_trig(i));
        if estimated_total > 1 && num_threads > 0 && !has_trig {
            let ledger = alias.to_string();

            let next_chunk = Arc::new(AtomicUsize::new(1));
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

                let handle = std::thread::Builder::new()
                    .name(format!("ttl-parser-{}", thread_idx))
                    .spawn(move || loop {
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
                        match parse_chunk(&ttl, &shared_alloc, t, &ledger, compress) {
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
                    })
                    .map_err(|e| ImportError::Transact(format!("spawn parser: {}", e)))?;

                parse_handles.push(handle);
            }
            drop(result_tx); // main thread's copy

            // Serial commit loop: receive parsed chunks, reorder, finalize in order
            let mut next_expected: usize = 1;
            let mut pending: BTreeMap<usize, ParsedChunk> = BTreeMap::new();

            for recv_result in result_rx {
                let (idx, parsed) = recv_result.map_err(ImportError::Transact)?;

                pending.insert(idx, parsed);

                while let Some(parsed) = pending.remove(&next_expected) {
                    // Commit-order publication: determine which codes from this chunk
                    // need to be introduced in this commit's namespace_delta.
                    let unpublished: FxHashSet<u16> = parsed
                        .new_codes
                        .iter()
                        .copied()
                        .filter(|c| *c < OVERFLOW && !published_codes.contains(c))
                        .collect();
                    let ns_delta = if unpublished.is_empty() {
                        std::collections::HashMap::new()
                    } else {
                        shared_alloc.lookup_codes(&unpublished)
                    };
                    published_codes.extend(&unpublished);

                    let result =
                        finalize_parsed_chunk(&mut state, parsed, ns_delta, storage, alias)
                            .await
                            .map_err(|e| ImportError::Transact(e.to_string()))?;

                    let total_elapsed = run_start.elapsed().as_secs_f64();
                    tracing::info!(
                        chunk = next_expected + 1,
                        total = estimated_total,
                        t = result.t,
                        flakes = result.flake_count,
                        cumulative_flakes = state.cumulative_flakes,
                        flakes_per_sec = format!(
                            "{:.2}M",
                            state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0
                        ),
                        "chunk committed"
                    );
                    config.emit_progress(ImportPhase::Committing {
                        chunk: next_expected + 1,
                        total: estimated_total,
                        cumulative_flakes: state.cumulative_flakes,
                        elapsed_secs: total_elapsed,
                    });

                    // Feed to resolver
                    let resolver_send_failed = {
                        let hash_hex = result.commit_id.digest_hex();
                        tokio::task::block_in_place(|| {
                            run_tx.send((result.commit_blob, hash_hex)).is_err()
                        })
                    };
                    if resolver_send_failed {
                        drop(run_tx);
                        let err = match resolver_handle.join() {
                            Ok(Err(e)) => format!("resolver failed: {}", e),
                            Err(p) => format!("resolver panicked: {:?}", p),
                            Ok(Ok(_)) => "resolver exited unexpectedly".to_string(),
                        };
                        return Err(ImportError::RunGeneration(err));
                    }

                    // Periodic nameservice checkpoint
                    if config.publish_every > 0
                        && (next_expected + 1).is_multiple_of(config.publish_every)
                    {
                        nameservice
                            .publish_commit(alias, result.t, &result.commit_id)
                            .await
                            .map_err(|e| ImportError::Storage(e.to_string()))?;
                        tracing::info!(
                            t = result.t,
                            chunk = next_expected + 1,
                            total = estimated_total,
                            "published nameservice checkpoint"
                        );
                    }

                    next_expected += 1;
                }
            }

            // Wait for parse threads
            for handle in parse_handles {
                handle.join().expect("parse thread panicked");
            }
        } else if estimated_total > 1 {
            // Serial fallback (0 threads or TriG files present)
            for i in 1..estimated_total {
                let content = chunk_source.read_chunk(i)?;
                let is_trig = chunk_source.is_trig(i);
                let result = if is_trig {
                    import_trig_commit(&mut state, &content, storage, alias, compress)
                        .await
                        .map_err(|e| ImportError::Transact(e.to_string()))?
                } else {
                    import_commit(&mut state, &content, storage, alias, compress)
                        .await
                        .map_err(|e| ImportError::Transact(e.to_string()))?
                };

                config.emit_progress(ImportPhase::Committing {
                    chunk: i + 1,
                    total: estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: run_start.elapsed().as_secs_f64(),
                });

                let hash_hex = result.commit_id.digest_hex();
                let send_failed = tokio::task::block_in_place(|| {
                    run_tx.send((result.commit_blob, hash_hex)).is_err()
                });
                if send_failed {
                    drop(run_tx);
                    let err = match resolver_handle.join() {
                        Ok(Err(e)) => format!("resolver failed: {}", e),
                        Err(p) => format!("resolver panicked: {:?}", p),
                        Ok(Ok(_)) => "resolver exited unexpectedly".to_string(),
                    };
                    return Err(ImportError::RunGeneration(err));
                }

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

    // ---- Finish background resolver ----
    drop(run_tx);
    tracing::info!("waiting for run resolver to finish...");
    let run_result = match resolver_handle.join() {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => {
            return Err(ImportError::RunGeneration(format!(
                "run generation failed: {}",
                e
            )))
        }
        Err(_) => {
            return Err(ImportError::RunGeneration(
                "resolver thread panicked".into(),
            ))
        }
    };

    tracing::info!(
        run_files = run_result.run_files.len(),
        total_records = run_result.total_records,
        subjects = run_result.subject_count,
        predicates = run_result.predicate_count,
        strings = run_result.string_count,
        commits = run_result.commit_count,
        "run generation complete"
    );

    // Load namespace codes from persisted namespaces.json
    let ns_path = run_dir.join("namespaces.json");
    let namespace_codes: HashMap<u16, String> = if ns_path.exists() {
        let bytes = std::fs::read(&ns_path)?;
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&bytes).map_err(|e| {
            ImportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        entries
            .iter()
            .filter_map(|v| {
                let code = v.get("code")?.as_u64()? as u16;
                let prefix = v.get("prefix")?.as_str()?;
                Some((code, prefix.to_string()))
            })
            .collect()
    } else {
        HashMap::new()
    };

    Ok(ChunkImportResult {
        final_t: state.t,
        cumulative_flakes: state.cumulative_flakes,
        commit_head_id,
        namespace_codes,
        stats_hook: run_result.stats_hook,
        total_commit_size: run_result.total_commit_size,
        total_asserts: run_result.total_asserts,
        total_retracts: run_result.total_retracts,
        prefix_map: state.prefix_map,
    })
}

// ============================================================================
// Phase 3-6: Build indexes, upload to CAS, write V2 root, publish
// ============================================================================

struct IndexUploadResult {
    root_id: fluree_db_core::ContentId,
    index_t: i64,
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
    use fluree_db_indexer::run_index::{
        build_all_indexes, precompute_language_dict, BinaryIndexRoot, BinaryIndexStore,
        CasArtifactsConfig, PrefixTrie, RunSortOrder,
    };
    use fluree_db_indexer::{upload_dicts_from_disk, upload_indexes_to_cas};

    // ---- Phase 3+4: Build indexes + upload dicts in parallel ----
    //
    // Pipeline overlap:
    //   - Pre-compute language dict (fast, needed by both paths)
    //   - Start index build (k-way merge, CPU-heavy) AND dict upload
    //     (CoW tree building + CAS writes) concurrently
    //   - After index build completes, upload index segments to CAS
    //   - Wait for dict upload to finish (may already be done)
    let build_start = Instant::now();
    let orders = RunSortOrder::all_build_orders();

    tracing::info!(
        orders = ?orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        run_dir = %input.run_dir.display(),
        index_dir = %input.index_dir.display(),
        "building multi-order indexes + uploading dicts (parallel)"
    );
    // Emit initial indexing progress so the bar starts moving immediately
    // after committing finishes (avoids appearance of hanging).
    // Progress tracks POST merge only (one order), so total = actual flake count.
    let total_index_flakes = input.cumulative_flakes;
    config.emit_progress(ImportPhase::Indexing {
        merged_flakes: 0,
        total_flakes: total_index_flakes,
        elapsed_secs: 0.0,
    });

    // Pre-compute language dict so upload_dicts_from_disk can start immediately.
    let run_dir_for_lang = input.run_dir.to_path_buf();
    tokio::task::spawn_blocking(move || precompute_language_dict(&run_dir_for_lang))
        .await
        .map_err(|e| ImportError::IndexBuild(format!("lang dict task panicked: {}", e)))?
        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

    // Start dict upload (reads flat files from run_dir, builds CoW trees, uploads to CAS).
    // This runs concurrently with the index build below.
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

    // Start index build (k-way merge + leaf/branch file writes).
    let run_dir_owned = input.run_dir.to_path_buf();
    let index_dir_owned = input.index_dir.to_path_buf();
    let build_counter = merge_counter.clone();
    let build_handle = tokio::task::spawn_blocking(move || {
        build_all_indexes(
            &run_dir_owned,
            &index_dir_owned,
            orders,
            25_000, // leaflet_rows
            10,     // leaflets_per_leaf
            1,      // zstd_level
            Some(build_counter),
        )
    });

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

    let build_results = build_handle
        .await
        .map_err(|e| ImportError::IndexBuild(format!("index build task panicked: {}", e)))?
        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

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
        "index build complete"
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
    let graph_refs = upload_indexes_to_cas(storage, alias, &build_results)
        .await
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    // Wait for dict upload to complete.
    let uploaded_dicts = dict_upload_handle
        .await
        .map_err(|e| ImportError::Upload(format!("dict upload task panicked: {}", e)))?
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    tracing::info!(
        elapsed = ?build_start.elapsed(),
        graphs = graph_refs.len(),
        "index build + CAS upload complete (overlapped)"
    );

    // ---- Build predicate SIDs via PrefixTrie (no BinaryIndexStore needed) ----
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

    // ---- Build stats JSON ----
    // Preferred: ID-based stats collected during commit resolution (per-graph property
    // stats with datatype counts + HLL NDV). Includes HLL sketch persistence so
    // incremental index refresh can merge registers. Fallback: SPOT build result for
    // per-graph flake counts only.
    let (stats_json, sketch_ref) = if let Some(hook) = input.stats_hook {
        // Persist HLL sketches BEFORE finalize consumes the hook.
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

        // Finalize with proper HLL merge for aggregate properties.
        // When class tracking is enabled, this also returns class counts + class→property presence.
        let (id_result, agg_props, class_counts, class_properties, _class_ref_targets) =
            hook.finalize_with_aggregate_properties();

        // Per-graph stats (p_id-keyed, for StatsView.graph_properties)
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

        // Top-level aggregate properties: use agg_props directly (proper HLL union).
        // Replaces incorrect max()-based manual aggregation.
        let mut properties_json: Vec<serde_json::Value> = agg_props
            .iter()
            .filter_map(|p| {
                let sid = predicate_sids.get(p.p_id as usize)?;
                Some(serde_json::json!({
                    "sid": [sid.0, &sid.1],
                    "count": p.count,
                    "ndv_values": p.ndv_values,
                    "ndv_subjects": p.ndv_subjects,
                    "last_modified_t": p.last_modified_t,
                    "datatypes": p.datatypes,
                }))
            })
            .collect();
        properties_json.sort_by(|a, b| {
            let a_code = a
                .get("sid")
                .and_then(|v| v.get(0))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let b_code = b
                .get("sid")
                .and_then(|v| v.get(0))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            a_code.cmp(&b_code).then_with(|| {
                let a_name = a
                    .get("sid")
                    .and_then(|v| v.get(1))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let b_name = b
                    .get("sid")
                    .and_then(|v| v.get(1))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                a_name.cmp(b_name)
            })
        });

        tracing::info!(
            property_stats = properties_json.len(),
            graph_count = graphs_json.len(),
            total_flakes = id_result.total_flakes,
            "stats collected from IdStatsHook"
        );

        // Class stats: counts + class→property presence.
        //
        // IMPORTANT: This intentionally does not include per-class per-property counts or
        // per-class datatype breakdowns. Those are available via aggregate property stats
        // (ledger-wide) and per-graph property stats (authoritative), and can be joined
        // by the UI using the property SID.
        let classes_json: Vec<serde_json::Value> = if class_counts.is_empty() {
            Vec::new()
        } else {
            // Use BinaryIndexStore for sid64 -> IRI -> Sid encoding (no giant HashMap reverse map).
            let store =
                BinaryIndexStore::load(input.run_dir, input.index_dir).map_err(ImportError::Io)?;

            class_counts
                .iter()
                .filter_map(|&(class_sid64, count)| {
                    let iri = store.resolve_subject_iri(class_sid64).ok()?;
                    let sid = store.encode_iri(&iri);

                    // Properties used by instances of this class (presence only).
                    let prop_usages: Vec<serde_json::Value> = class_properties
                        .get(&class_sid64)
                        .map(|props| {
                            let mut sorted: Vec<u32> = props.iter().copied().collect();
                            sorted.sort();
                            sorted
                                .iter()
                                .filter_map(|&pid| {
                                    let psid = predicate_sids.get(pid as usize)?;
                                    Some(serde_json::json!([[psid.0, psid.1]]))
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    Some(serde_json::json!([
                        [sid.namespace_code, sid.name.as_ref()],
                        [count, prop_usages],
                    ]))
                })
                .collect()
        };

        (
            serde_json::json!({
                "flakes": id_result.total_flakes,
                "size": 0,
                "properties": properties_json,
                "graphs": graphs_json,
                "classes": classes_json,
            }),
            Some(sketch_cid),
        )
    } else {
        // Fallback: flake counts only (no per-property / datatype breakdown).
        let (_, spot_result) = build_results
            .iter()
            .find(|(order, _)| *order == RunSortOrder::Spot)
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

        (
            serde_json::json!({
                "flakes": total_flakes,
                "size": 0,
                "graphs": graph_stats
            }),
            None,
        )
    };

    // ---- Phase 5: Build V2 root ----
    let mut root = BinaryIndexRoot::from_cas_artifacts(CasArtifactsConfig {
        ledger_id: alias,
        index_t: input.final_t,
        base_t: 0, // fresh import
        predicate_sids,
        namespace_codes: input.namespace_codes,
        subject_id_encoding: uploaded_dicts.subject_id_encoding,
        dict_refs: uploaded_dicts.dict_refs,
        graph_refs,
        stats: Some(stats_json),
        schema: None,
        prev_index: None, // fresh import
        garbage: None,    // fresh import
        sketch_ref,       // persisted HLL sketch (or None if stats disabled)
        subject_watermarks: uploaded_dicts.subject_watermarks,
        string_watermark: uploaded_dicts.string_watermark,
    });

    // Populate cumulative commit statistics (optional planner telemetry).
    root.total_commit_size = total_commit_size;
    root.total_asserts = total_asserts;
    root.total_retracts = total_retracts;

    let root_bytes = root
        .to_json_bytes()
        .map_err(|e| ImportError::Upload(format!("serialize V2 root: {}", e)))?;

    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, alias, &root_bytes)
        .await
        .map_err(|e| ImportError::Upload(format!("write V2 root: {}", e)))?;

    // Derive ContentId from the root's content hash
    let root_id = ContentId::from_hex_digest(CODEC_FLUREE_INDEX_ROOT, &write_result.content_hash)
        .expect("valid SHA-256 hash from storage write");

    tracing::info!(
        root_id = %root_id,
        index_t = input.final_t,
        "V2 index root written to CAS"
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
    })
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
    let base = std::env::temp_dir().join("fluree-import");
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
