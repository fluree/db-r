//! Bulk import pipeline: TTL → commits → runs → indexes → CAS → publish.
//!
//! Provides `.create("mydb").import("/path/to/chunks/").execute().await` API
//! on [`Fluree`] for high-throughput bulk import of Turtle data.
//!
//! ## Pipeline overview
//!
//! 1. **Create ledger** — `nameservice.publish_ledger_init(alias)`
//! 2. **Import TTL → commits + runs** — parallel chunk parsing, serial commit
//!    finalization, streaming run generation via background resolver thread
//! 3. **Build indexes** — `build_all_indexes()` from completed run files
//! 4. **CAS upload** — dicts + indexes uploaded to content-addressed storage
//! 5. **V2 root** — `BinaryIndexRootV2::from_cas_artifacts()` written to CAS
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
use fluree_db_core::{ContentKind, Storage};
use fluree_db_nameservice::{NameService, Publisher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the bulk import pipeline.
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Number of parallel TTL parse threads. Default: available parallelism.
    pub parse_threads: usize,
    /// Run writer memory budget in MB. Default: 256.
    pub run_budget_mb: usize,
    /// Whether to build multi-order indexes after runs. Default: true.
    pub build_index: bool,
    /// Whether to publish to nameservice after index build. Default: true.
    pub publish: bool,
    /// Whether to delete session tmp dir on success. Default: true.
    pub cleanup_local_files: bool,
    /// Whether to zstd-compress commit blobs. Default: true.
    pub compress_commits: bool,
    /// Publish nameservice head every N chunks during import. Default: 50.
    /// 0 disables periodic checkpoints.
    pub publish_every: usize,
}

impl Default for ImportConfig {
    fn default() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            parse_threads: threads,
            run_budget_mb: 256,
            build_index: true,
            publish: true,
            cleanup_local_files: true,
            compress_commits: true,
            publish_every: 50,
        }
    }
}

// ============================================================================
// Result
// ============================================================================

/// Result of a successful bulk import.
#[derive(Debug)]
pub struct ImportResult {
    /// Ledger alias.
    pub alias: String,
    /// Final commit t (= number of imported chunks).
    pub t: i64,
    /// Total flake count across all commits.
    pub flake_count: u64,
    /// CAS address of the head commit.
    pub commit_head_address: String,
    /// CAS address of the V2 index root. `None` if `build_index == false`.
    pub root_address: Option<String>,
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
// Builder
// ============================================================================

/// Builder for a bulk import operation.
///
/// Created via `fluree.create("alias").import("/path/to/chunks")`.
///
/// # Example
///
/// ```ignore
/// let result = fluree.create("mydb")
///     .import("/data/chunks/")
///     .threads(8)
///     .run_budget_mb(4096)
///     .execute()
///     .await?;
/// ```
pub struct ImportBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a super::Fluree<S, N>,
    alias: String,
    import_path: PathBuf,
    config: ImportConfig,
}

impl<'a, S, N> ImportBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        fluree: &'a super::Fluree<S, N>,
        alias: String,
        import_path: PathBuf,
    ) -> Self {
        Self {
            fluree,
            alias,
            import_path,
            config: ImportConfig::default(),
        }
    }

    /// Set the number of parallel TTL parse threads.
    pub fn threads(mut self, n: usize) -> Self {
        self.config.parse_threads = n;
        self
    }

    /// Set the run writer memory budget in MB.
    pub fn run_budget_mb(mut self, mb: usize) -> Self {
        self.config.run_budget_mb = mb;
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

    /// Publish nameservice checkpoint every N chunks. Default: 50. 0 disables.
    pub fn publish_every(mut self, n: usize) -> Self {
        self.config.publish_every = n;
        self
    }

    /// Execute the bulk import pipeline.
    pub async fn execute(self) -> std::result::Result<ImportResult, ImportError> {
        run_import_pipeline(
            self.fluree.storage(),
            self.fluree.nameservice(),
            &self.alias,
            &self.import_path,
            &self.config,
        )
        .await
    }
}

// ============================================================================
// Create builder (intermediate)
// ============================================================================

/// Intermediate builder returned by `fluree.create("alias")`.
///
/// Supports `.import(path)` for bulk import, or `.execute()` for empty ledger creation.
pub struct CreateBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a super::Fluree<S, N>,
    alias: String,
}

impl<'a, S, N> CreateBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(fluree: &'a super::Fluree<S, N>, alias: String) -> Self {
        Self { fluree, alias }
    }
}

impl<'a, S, N> CreateBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher + Clone + Send + Sync + 'static,
{
    /// Attach a bulk import to this create operation.
    ///
    /// `path` can be a directory containing `chunk_*.ttl` files, or a single TTL file.
    pub fn import(self, path: impl AsRef<Path>) -> ImportBuilder<'a, S, N> {
        ImportBuilder::new(self.fluree, self.alias, path.as_ref().to_path_buf())
    }
}

// ============================================================================
// Chunk discovery
// ============================================================================

/// Discover and sort `chunk_*.ttl` files from a directory.
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
            p.extension().is_some_and(|ext| ext == "ttl")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("chunk_"))
        })
        .collect();

    if chunks.is_empty() {
        return Err(ImportError::NoChunks(format!(
            "no chunk_*.ttl files found in {}",
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
    N: NameService + Publisher,
{
    let pipeline_start = Instant::now();
    let _span = tracing::info_span!("bulk_import", alias = %alias).entered();

    // ---- Discover chunks ----
    let chunks = discover_chunks(import_path)?;
    let total = chunks.len();
    tracing::info!(chunks = total, path = %import_path.display(), "discovered import chunks");

    // ---- Phase 1: Create ledger (init nameservice) ----
    let normalized_alias =
        fluree_db_core::alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());

    // Check if ledger already exists
    let ns_record = nameservice
        .lookup(&normalized_alias)
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?;

    if let Some(ref record) = ns_record {
        if record.commit_t > 0 || record.commit_address.is_some() {
            return Err(ImportError::Transact(format!(
                "import requires a fresh ledger, but '{}' already has commits (t={})",
                normalized_alias, record.commit_t
            )));
        }
    }

    if ns_record.is_none() {
        nameservice
            .publish_ledger_init(&normalized_alias)
            .await
            .map_err(|e| ImportError::Storage(e.to_string()))?;
        tracing::info!(alias = %normalized_alias, "initialized new ledger in nameservice");
    }

    // ---- Set up session directory for runs/indexes ----
    let alias_prefix = fluree_db_core::address_path::alias_to_path_prefix(&normalized_alias)
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
    // - On success + cleanup_local_files=true → delete session dir
    // - On any failure → keep session dir for debugging
    // - If cleanup itself fails → log warning, do not fail import
    let pipeline_result = run_pipeline_phases(
        storage,
        nameservice,
        &normalized_alias,
        &chunks,
        &run_dir,
        &index_dir,
        config,
        pipeline_start,
    )
    .await;

    match pipeline_result {
        Ok(result) => {
            // ---- Cleanup: only on full success ----
            if config.cleanup_local_files {
                if let Err(e) = std::fs::remove_dir_all(&session_dir) {
                    tracing::warn!(
                        session_dir = %session_dir.display(),
                        error = %e,
                        "failed to clean up import session directory (import succeeded)"
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

            let total_elapsed = pipeline_start.elapsed();
            tracing::info!(
                alias = %normalized_alias,
                t = result.t,
                flakes = result.flake_count,
                root_address = ?result.root_address,
                elapsed = ?total_elapsed,
                "bulk import pipeline complete"
            );

            Ok(result)
        }
        Err(e) => {
            tracing::warn!(
                session_dir = %session_dir.display(),
                error = %e,
                "import failed; keeping session directory for debugging"
            );
            Err(e)
        }
    }
}

// ============================================================================
// Pipeline phases 2-6
// ============================================================================

/// Run phases 2-6: import chunks, build indexes, upload to CAS, write V2 root, publish.
///
/// Separated from `run_import_pipeline` to enable clean error-path handling:
/// on failure, the caller keeps the session dir for debugging.
#[allow(clippy::too_many_arguments)]
async fn run_pipeline_phases<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    chunks: &[PathBuf],
    run_dir: &Path,
    index_dir: &Path,
    config: &ImportConfig,
    pipeline_start: Instant,
) -> std::result::Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher,
{
    // ---- Phase 2: Import TTL → commits + streaming runs ----
    let import_result =
        run_import_chunks(storage, nameservice, alias, chunks, run_dir, config).await?;

    tracing::info!(
        t = import_result.final_t,
        flakes = import_result.cumulative_flakes,
        commit_head = %import_result.commit_head_address,
        elapsed = ?pipeline_start.elapsed(),
        "import + run generation complete"
    );

    // ---- Phases 3-6: Build index, upload, root, publish ----
    let root_address;
    let index_t;

    if config.build_index {
        let index_result = build_and_upload(
            storage,
            nameservice,
            alias,
            run_dir,
            index_dir,
            import_result.final_t,
            &import_result.namespace_codes,
            import_result.stats_hook,
            config,
        )
        .await?;

        root_address = Some(index_result.root_address);
        index_t = index_result.index_t;
    } else {
        root_address = None;
        index_t = 0;
    }

    Ok(ImportResult {
        alias: alias.to_string(),
        t: import_result.final_t,
        flake_count: import_result.cumulative_flakes,
        commit_head_address: import_result.commit_head_address,
        root_address,
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
    commit_head_address: String,
    namespace_codes: HashMap<u16, String>,
    stats_hook: Option<fluree_db_indexer::stats::IdStatsHook>,
}

/// Import all TTL chunks: parallel parse + serial commit + streaming runs.
async fn run_import_chunks<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    chunks: &[PathBuf],
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
        finalize_parsed_chunk, import_commit, parse_chunk, ImportState, ParsedChunk,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let total = chunks.len();
    let compress = config.compress_commits;
    let num_threads = config.parse_threads;
    let mut state = ImportState::new();
    let run_start = Instant::now();

    // ---- Spawn background run resolver (three session-scoped singletons) ----
    std::fs::create_dir_all(run_dir)?;
    let budget = config.run_budget_mb * 1024 * 1024;
    let mo_config = MultiOrderConfig {
        total_budget_bytes: budget,
        orders: RunSortOrder::all_build_orders().to_vec(),
        base_run_dir: run_dir.to_path_buf(),
    };

    // Bounded channel: backpressures import if resolver falls behind.
    let (run_tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, String)>(2);

    let ledger_alias = alias.to_string();
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
                resolver.set_stats_hook(fluree_db_indexer::stats::IdStatsHook::new());

                // Singleton 3: MultiOrderRunWriter
                let mut writer = MultiOrderRunWriter::new(mo_config)
                    .map_err(|e| format!("init multi-order writer: {}", e))?;
                let mut commit_count = 0usize;

                while let Ok((bytes, commit_address)) = rx.recv() {
                    let (op_count, t) = resolver
                        .resolve_blob(
                            &bytes,
                            &commit_address,
                            &ledger_alias,
                            &mut dicts,
                            &mut writer,
                        )
                        .map_err(|e| format!("{}", e))?;
                    commit_count += 1;
                    tracing::info!(
                        commit = commit_count,
                        t,
                        ops = op_count,
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
                })
            })
            .map_err(|e| ImportError::RunGeneration(format!("spawn resolver: {}", e)))?;

    // ---- Phase 2a: Parse + commit chunk 0 serially (establishes namespaces) ----
    if !chunks.is_empty() {
        let ttl = std::fs::read_to_string(&chunks[0])?;
        let size_mb = ttl.len() as f64 / (1024.0 * 1024.0);
        tracing::info!(
            chunk = 1,
            total,
            size_mb = format!("{:.1}", size_mb),
            "parsing chunk 0 serially (establishes namespaces)"
        );

        let result = import_commit(&mut state, &ttl, storage, alias, compress)
            .await
            .map_err(|e| ImportError::Transact(e.to_string()))?;

        tracing::info!(
            t = result.t,
            flakes = result.flake_count,
            blob_bytes = result.blob_bytes,
            "chunk 0 committed"
        );

        // Feed to resolver
        let addr = result.address.clone();
        tokio::task::block_in_place(|| {
            run_tx
                .send((result.commit_blob, addr))
                .map_err(|_| ImportError::RunGeneration("resolver exited unexpectedly".into()))
        })?;
    }

    // ---- Phase 2b: Parse remaining chunks in parallel, commit serially ----
    if chunks.len() > 1 && num_threads > 0 {
        let base_registry = state.ns_registry.clone();
        let ledger = alias.to_string();

        let next_chunk = Arc::new(AtomicUsize::new(1));
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
            std::result::Result<(usize, ParsedChunk), String>,
        >(num_threads * 2);

        // Spawn parse worker threads
        let mut parse_handles = Vec::with_capacity(num_threads);
        for thread_idx in 0..num_threads {
            let next_chunk = Arc::clone(&next_chunk);
            let result_tx = result_tx.clone();
            let base_registry = base_registry.clone();
            let ledger = ledger.clone();
            let chunks: Vec<PathBuf> = chunks.to_vec();

            let handle = std::thread::Builder::new()
                .name(format!("ttl-parser-{}", thread_idx))
                .spawn(move || loop {
                    let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if idx >= chunks.len() {
                        break;
                    }

                    let ttl = match std::fs::read_to_string(&chunks[idx]) {
                        Ok(s) => s,
                        Err(e) => {
                            let _ =
                                result_tx.send(Err(format!("failed to read chunk {}: {}", idx, e)));
                            break;
                        }
                    };

                    let t = (idx + 1) as i64;
                    match parse_chunk(&ttl, base_registry.clone(), t, &ledger, compress) {
                        Ok(parsed) => {
                            if result_tx.send(Ok((idx, parsed))).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ =
                                result_tx.send(Err(format!("parse chunk {} failed: {}", idx, e)));
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
                let result = finalize_parsed_chunk(&mut state, parsed, storage, alias)
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?;

                let total_elapsed = run_start.elapsed().as_secs_f64();
                tracing::info!(
                    chunk = next_expected + 1,
                    total,
                    t = result.t,
                    flakes = result.flake_count,
                    cumulative_flakes = state.cumulative_flakes,
                    flakes_per_sec = format!(
                        "{:.2}M",
                        state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0
                    ),
                    "chunk committed"
                );

                // Feed to resolver
                let resolver_send_failed = {
                    let addr = result.address.clone();
                    tokio::task::block_in_place(|| run_tx.send((result.commit_blob, addr)).is_err())
                };
                if resolver_send_failed {
                    // Drop sender so resolver thread exits, then join to get error
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
                        .publish_commit(alias, &result.address, result.t)
                        .await
                        .map_err(|e| ImportError::Storage(e.to_string()))?;
                    tracing::info!(
                        t = result.t,
                        chunk = next_expected + 1,
                        total,
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
    } else if chunks.len() > 1 {
        // Serial fallback (0 threads)
        for (i, chunk) in chunks.iter().enumerate().skip(1) {
            let ttl = std::fs::read_to_string(chunk)?;
            let result = import_commit(&mut state, &ttl, storage, alias, compress)
                .await
                .map_err(|e| ImportError::Transact(e.to_string()))?;

            let addr = result.address.clone();
            let send_failed =
                tokio::task::block_in_place(|| run_tx.send((result.commit_blob, addr)).is_err());
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
                    .publish_commit(alias, &result.address, result.t)
                    .await
                    .map_err(|e| ImportError::Storage(e.to_string()))?;
            }
        }
    }

    // Final commit head publish
    let commit_head_address = state
        .previous_ref
        .as_ref()
        .map(|r| r.address.clone())
        .unwrap_or_default();

    nameservice
        .publish_commit(alias, &commit_head_address, state.t)
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
        commit_head_address,
        namespace_codes,
        stats_hook: run_result.stats_hook,
    })
}

// ============================================================================
// Phase 3-6: Build indexes, upload to CAS, write V2 root, publish
// ============================================================================

struct IndexUploadResult {
    root_address: String,
    index_t: i64,
}

async fn build_and_upload<S, N>(
    storage: &S,
    nameservice: &N,
    alias: &str,
    run_dir: &Path,
    index_dir: &Path,
    final_t: i64,
    namespace_codes: &HashMap<u16, String>,
    stats_hook: Option<fluree_db_indexer::stats::IdStatsHook>,
    config: &ImportConfig,
) -> std::result::Result<IndexUploadResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Publisher,
{
    use fluree_db_indexer::run_index::{
        build_all_indexes, precompute_language_dict, BinaryIndexRootV2, PrefixTrie, RunSortOrder,
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
        run_dir = %run_dir.display(),
        index_dir = %index_dir.display(),
        "building multi-order indexes + uploading dicts (parallel)"
    );

    // Pre-compute language dict so upload_dicts_from_disk can start immediately.
    let run_dir_for_lang = run_dir.to_path_buf();
    tokio::task::spawn_blocking(move || precompute_language_dict(&run_dir_for_lang))
        .await
        .map_err(|e| ImportError::IndexBuild(format!("lang dict task panicked: {}", e)))?
        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

    // Start dict upload (reads flat files from run_dir, builds CoW trees, uploads to CAS).
    // This runs concurrently with the index build below.
    let dict_upload_handle = {
        let storage = storage.clone();
        let alias = alias.to_string();
        let run_dir = run_dir.to_path_buf();
        let namespace_codes = namespace_codes.clone();
        tokio::spawn(async move {
            upload_dicts_from_disk(&storage, &alias, &run_dir, &namespace_codes).await
        })
    };

    // Start index build (k-way merge + leaf/branch file writes).
    let run_dir_owned = run_dir.to_path_buf();
    let index_dir_owned = index_dir.to_path_buf();
    let build_results = tokio::task::spawn_blocking(move || {
        build_all_indexes(
            &run_dir_owned,
            &index_dir_owned,
            orders,
            25_000, // leaflet_rows
            10,     // leaflets_per_leaf
            1,      // zstd_level
        )
    })
    .await
    .map_err(|e| ImportError::IndexBuild(format!("index build task panicked: {}", e)))?
    .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

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
    let graph_addresses = upload_indexes_to_cas(storage, alias, &build_results)
        .await
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    // Wait for dict upload to complete.
    let uploaded_dicts = dict_upload_handle
        .await
        .map_err(|e| ImportError::Upload(format!("dict upload task panicked: {}", e)))?
        .map_err(|e| ImportError::Upload(e.to_string()))?;

    tracing::info!(
        elapsed = ?build_start.elapsed(),
        graphs = graph_addresses.len(),
        "index build + CAS upload complete (overlapped)"
    );

    // ---- Build predicate SIDs via PrefixTrie (no BinaryIndexStore needed) ----
    let trie = PrefixTrie::from_namespace_codes(namespace_codes);
    let predicates_path = run_dir.join("predicates.json");
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
    // stats with datatype counts + HLL NDV). Fallback: SPOT build result for per-graph
    // flake counts only.
    let stats_json = if let Some(hook) = stats_hook {
        let id_result = hook.finalize();

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

        // Aggregate per-property stats across graphs, map p_id → SID
        // (for StatsView.properties — the map the planner actually consults)
        struct AggProp {
            count: u64,
            ndv_values: u64,
            ndv_subjects: u64,
            last_modified_t: i64,
        }
        let mut agg: HashMap<u32, AggProp> = HashMap::new();
        for g in &id_result.graphs {
            for p in &g.properties {
                let entry = agg.entry(p.p_id).or_insert(AggProp {
                    count: 0,
                    ndv_values: 0,
                    ndv_subjects: 0,
                    last_modified_t: 0,
                });
                entry.count += p.count;
                entry.ndv_values = entry.ndv_values.max(p.ndv_values);
                entry.ndv_subjects = entry.ndv_subjects.max(p.ndv_subjects);
                entry.last_modified_t = entry.last_modified_t.max(p.last_modified_t);
            }
        }
        let mut properties_json: Vec<serde_json::Value> = agg
            .iter()
            .filter_map(|(&p_id, prop)| {
                let sid = predicate_sids.get(p_id as usize)?;
                Some(serde_json::json!({
                    "sid": [sid.0, &sid.1],
                    "count": prop.count,
                    "ndv_values": prop.ndv_values,
                    "ndv_subjects": prop.ndv_subjects,
                    "last_modified_t": prop.last_modified_t,
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

        serde_json::json!({
            "flakes": id_result.total_flakes,
            "size": 0,
            "properties": properties_json,
            "graphs": graphs_json,
        })
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

        serde_json::json!({
            "flakes": total_flakes,
            "size": 0,
            "graphs": graph_stats
        })
    };

    // ---- Phase 5: Build V2 root ----
    let root = BinaryIndexRootV2::from_cas_artifacts(
        alias,
        final_t, // index_t
        0,       // base_t (fresh import)
        predicate_sids,
        namespace_codes,
        uploaded_dicts.subject_id_encoding,
        uploaded_dicts.dict_addresses,
        graph_addresses,
        Some(stats_json),
        None, // schema
        None, // prev_index (fresh import)
        None, // garbage (fresh import)
        uploaded_dicts.subject_watermarks,
        uploaded_dicts.string_watermark,
    );

    let root_bytes = root
        .to_json_bytes()
        .map_err(|e| ImportError::Upload(format!("serialize V2 root: {}", e)))?;

    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, alias, &root_bytes)
        .await
        .map_err(|e| ImportError::Upload(format!("write V2 root: {}", e)))?;

    tracing::info!(
        root_address = %write_result.address,
        index_t = final_t,
        "V2 index root written to CAS"
    );

    // ---- Phase 6: Publish ----
    if config.publish {
        nameservice
            .publish_index(alias, &write_result.address, final_t)
            .await
            .map_err(|e| ImportError::Storage(format!("publish index: {}", e)))?;
        tracing::info!(
            index_t = final_t,
            root_address = %write_result.address,
            "index published to nameservice"
        );
    }

    Ok(IndexUploadResult {
        root_address: write_result.address,
        index_t: final_t,
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
