//! Bulk Turtle Import
//!
//! Stream-reads a large `.ttl` file and transacts it into Fluree in
//! configurable batches using the `fluree-db-api` library API.
//!
//! Each batch is:
//! 1. **Parsed** from Turtle to JSON-LD (CPU work, outside the commit lock)
//! 2. **Committed** via `.insert(&json).commit()` which queues into Fluree's
//!    per-ledger FIFO write lock
//!
//! Multiple commits can be in-flight simultaneously — while one batch is being
//! staged and persisted under the lock, the next batches are already parsed and
//! waiting in the queue. The `--inflight` flag controls how many commit tasks
//! can be pipelined.
//!
//! # Usage
//!
//! ```bash
//! # In-memory (fastest, data lost on exit):
//! cargo run -p bulk-turtle-import --release -- data.ttl
//!
//! # With persistent file storage:
//! cargo run -p bulk-turtle-import --release -- data.ttl --storage ./fluree-data
//!
//! # Custom batch size, inflight depth, and ledger name:
//! cargo run -p bulk-turtle-import --release -- data.ttl -b 50000 --inflight 8 -l mydb
//!
//! # Tune novelty thresholds (see --help for defaults):
//! cargo run -p bulk-turtle-import --release -- data.ttl \
//!     --reindex-min-bytes 50000000 --reindex-max-bytes 500000000
//!
//! # Disable background indexing (import-then-reindex workflow):
//! cargo run -p bulk-turtle-import --release -- data.ttl --no-indexing \
//!     --reindex-max-bytes 2000000000
//!
//! # Logging — controlled via RUST_LOG env var (tracing-subscriber EnvFilter):
//! #   default:  warn for everything, info for this binary
//! #   see fluree internals at debug level:
//! RUST_LOG=info,fluree_db_api=debug,fluree_db_transact=debug cargo run -p bulk-turtle-import --release -- data.ttl
//! #   everything at debug:
//! RUST_LOG=debug cargo run -p bulk-turtle-import --release -- data.ttl
//! #   only this binary at trace, fluree crates at info:
//! RUST_LOG=info,bulk_turtle_import=trace cargo run -p bulk-turtle-import --release -- data.ttl
//! ```

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser as ClapParser;
use fluree_db_api::IndexConfig;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Stream-import a large Turtle (.ttl) file into Fluree in configurable batches.
#[derive(ClapParser)]
#[command(name = "bulk-turtle-import")]
struct Cli {
    /// Path to the .ttl file to import.
    file: PathBuf,

    /// Ledger alias (created if it doesn't exist). Appends `:main` if no
    /// branch is specified.
    #[arg(short, long, default_value = "import")]
    ledger: String,

    /// Number of RDF statements per batch. Larger batches mean fewer
    /// transactions but more memory per commit.
    #[arg(short, long, default_value_t = 10_000)]
    batch_size: usize,

    /// Persistent storage directory. Omit for in-memory (data lost on exit).
    #[arg(short, long)]
    storage: Option<PathBuf>,

    /// Maximum pipelined in-flight commit tasks. While one commit holds the
    /// ledger write lock, the next N-1 batches are already parsed and queued.
    #[arg(long, default_value_t = 4)]
    inflight: usize,

    /// Novelty size (bytes) that triggers background indexing. Background
    /// indexing drains novelty between commits to keep memory bounded and
    /// queries fast. Default 10 MB.
    #[arg(long, default_value_t = 10_000_000)]
    reindex_min_bytes: u64,

    /// Novelty size (bytes) that **blocks** new commits until indexing
    /// catches up. Must be larger than `--reindex-min-bytes`. Default 200 MB.
    #[arg(long, default_value_t = 200_000_000)]
    reindex_max_bytes: u64,

    /// Disable background indexing entirely. Commits never trigger indexing
    /// and novelty accumulates in RAM. Useful for import-then-reindex
    /// workflows where you index once after all data is loaded. When set,
    /// you typically also want a very large `--reindex-max-bytes` to avoid
    /// hitting the hard novelty limit.
    #[arg(long, default_value_t = false)]
    no_indexing: bool,
}

// ---------------------------------------------------------------------------
// Connection config
// ---------------------------------------------------------------------------

/// Build a JSON-LD connection config with storage and indexing defaults.
///
/// This mirrors what `connect_memory()` / `connect_filesystem()` generate
/// internally, but adds a `defaults.indexing` block so that:
///
/// - `indexingEnabled` controls whether `connect_json_ld` starts a
///   `BackgroundIndexerWorker` (same as `fluree-db-server` does).
/// - `reindexMinBytes` / `reindexMaxBytes` become the default `IndexConfig`
///   for every commit, raising the novelty hard limit above the 1 MB default.
fn build_connection_config(cli: &Cli) -> serde_json::Value {
    let storage_node = if let Some(ref path) = cli.storage {
        let p = path.to_str().expect("storage path must be valid UTF-8");
        json!({"@id": "storage", "@type": "Storage", "filePath": p})
    } else {
        json!({"@id": "storage", "@type": "Storage"})
    };

    json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            storage_node,
            {
                "@id": "connection",
                "@type": "Connection",
                "commitStorage": {"@id": "storage"},
                "indexStorage": {"@id": "storage"},
                "primaryPublisher": {"@type": "Publisher", "storage": {"@id": "storage"}},
                "defaults": {
                    "indexing": {
                        "indexingEnabled": !cli.no_indexing,
                        "reindexMinBytes": cli.reindex_min_bytes,
                        "reindexMaxBytes": cli.reindex_max_bytes
                    }
                }
            }
        ]
    })
}

// ---------------------------------------------------------------------------
// Streaming batch reader
// ---------------------------------------------------------------------------

/// A single batch of Turtle statements ready for parsing.
struct Batch {
    /// Self-contained Turtle text (prefix header + statement body).
    turtle: String,
    /// Number of statements in this batch.
    statements: usize,
}

/// Line-by-line Turtle reader that yields self-contained batches.
///
/// - Collects `@prefix` / `@base` directives into a header that is prepended
///   to every batch, making each batch independently parseable.
/// - Counts statement boundaries (lines ending with ` .`) and yields a new
///   [`Batch`] each time `batch_size` statements have accumulated.
/// - Any remaining statements at EOF are yielded as a final (smaller) batch.
struct BatchReader<R> {
    inner: BufReader<R>,
    prefix_header: String,
    batch_size: usize,
    line_buf: String,
    done: bool,
}

impl<R: io::Read> BatchReader<R> {
    fn new(reader: R, batch_size: usize) -> Self {
        Self {
            inner: BufReader::with_capacity(256 * 1024, reader),
            prefix_header: String::new(),
            batch_size,
            line_buf: String::new(),
            done: false,
        }
    }
}

impl<R: io::Read> Iterator for BatchReader<R> {
    type Item = io::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut body = String::new();
        let mut stmt_count: usize = 0;

        loop {
            self.line_buf.clear();
            match self.inner.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // EOF
                    self.done = true;
                    break;
                }
                Ok(_) => {
                    let trimmed = self.line_buf.trim();

                    // Skip blank lines and comments.
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }

                    // Collect prefix/base directives into the shared header.
                    if is_directive(trimmed) {
                        self.prefix_header.push_str(&self.line_buf);
                        continue;
                    }

                    // Accumulate statement body.
                    body.push_str(&self.line_buf);

                    if is_statement_end(trimmed) {
                        stmt_count += 1;
                        if stmt_count >= self.batch_size {
                            break;
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if body.is_empty() {
            return None;
        }

        let mut turtle = String::with_capacity(self.prefix_header.len() + body.len() + 1);
        turtle.push_str(&self.prefix_header);
        turtle.push('\n');
        turtle.push_str(&body);

        Some(Ok(Batch {
            turtle,
            statements: stmt_count,
        }))
    }
}

/// Returns `true` for Turtle `@prefix`/`@base` and SPARQL-style `PREFIX`/`BASE`.
fn is_directive(trimmed: &str) -> bool {
    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("@prefix ")
        || lower.starts_with("@base ")
        || lower.starts_with("prefix ")
        || lower.starts_with("base ")
}

/// Heuristic: a Turtle statement terminates when the trimmed line ends with
/// ` .`, `\t.`, or is exactly `.`.
///
/// This works reliably for well-formatted Turtle output from standard tools
/// (rapper, riot, rdflib, etc.). It can misidentify boundaries inside
/// multi-line string literals, but those are rare in practice.
fn is_statement_end(trimmed: &str) -> bool {
    trimmed == "." || trimmed.ends_with(" .") || trimmed.ends_with("\t.")
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

/// Output type of each spawned commit task.
type CommitOutput = (usize, usize, std::time::Duration, fluree_db_api::Result<fluree_db_api::TransactResultRef>);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default: warn globally, info for this binary.  Override with RUST_LOG, e.g.:
    //   RUST_LOG=info,fluree_db_api=debug,fluree_db_transact=debug
    // Crate names use underscores (fluree_db_api, fluree_db_transact,
    // fluree_db_ledger, fluree_graph_turtle, etc.).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,bulk_turtle_import=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // --- Connect --------------------------------------------------------
    //
    // Build a JSON-LD connection config that sets up storage and indexing
    // defaults.  `connect_json_ld` automatically starts a
    // `BackgroundIndexerWorker` when `indexingEnabled` is true, mirroring
    // what `fluree-db-server` does at startup.
    //
    // NOTE: The Graph API path (`.graph().transact().commit()`) does not
    // read connection-level `IndexConfig` defaults — it falls back to
    // `IndexConfig::default()` (1 MB hard limit).  We work around this by
    // passing `.index_config(...)` explicitly on each commit below.

    let config = build_connection_config(&cli);
    tracing::info!(
        storage = if cli.storage.is_some() { "file" } else { "memory" },
        indexing = !cli.no_indexing,
        reindex_min_bytes = cli.reindex_min_bytes,
        reindex_max_bytes = cli.reindex_max_bytes,
        "Connecting to Fluree"
    );
    let fluree = fluree_db_api::connect_json_ld(&config).await?;

    // Enable ledger caching so that every `.graph().transact().commit()` call
    // resolves to the *same* LedgerHandle, whose internal Mutex provides FIFO
    // write serialization.
    let fluree = Arc::new(fluree.enable_ledger_caching());

    // Build the IndexConfig we'll pass on every commit (see NOTE above).
    let index_config = IndexConfig {
        reindex_min_bytes: cli.reindex_min_bytes as usize,
        reindex_max_bytes: cli.reindex_max_bytes as usize,
    };

    // --- Create or load ledger ------------------------------------------

    let alias = if cli.ledger.contains(':') {
        cli.ledger.clone()
    } else {
        format!("{}:main", cli.ledger)
    };

    let base_name = alias.split(':').next().unwrap();
    if !fluree.ledger_exists(&alias).await? {
        tracing::info!(ledger = base_name, "Creating new ledger");
        fluree.create_ledger(base_name).await?;
    } else {
        tracing::info!(ledger = %alias, "Ledger already exists");
    }

    // --- Stream, parse, and pipeline commits ----------------------------

    let file = File::open(&cli.file)?;
    let file_size = file.metadata()?.len();
    let size_mb = file_size as f64 / (1024.0 * 1024.0);
    let reader = BatchReader::new(file, cli.batch_size);

    tracing::info!(
        file = %cli.file.display(),
        size_mb = format_args!("{size_mb:.1}"),
        batch_size = cli.batch_size,
        inflight = cli.inflight,
        "Starting import"
    );

    let start = Instant::now();
    let mut total_stmts: usize = 0;
    let mut total_flakes: usize = 0;
    let mut batch_num: usize = 0;

    let inflight = Arc::new(Semaphore::new(cli.inflight));
    let mut tasks: JoinSet<CommitOutput> = JoinSet::new();

    for batch_result in reader {
        let batch = batch_result?;
        batch_num += 1;

        // ---- Parse TTL → JSON-LD (outside the commit lock) -------------
        let json = fluree_graph_turtle::parse_to_json(&batch.turtle)
            .map_err(|e| format!("Parse error in batch {batch_num}: {e}"))?;

        // ---- Acquire a pipeline slot -----------------------------------
        let permit = inflight
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed unexpectedly");

        // ---- Spawn commit task -----------------------------------------
        // The task owns its Arc<Fluree>, alias clone, parsed JSON, and
        // semaphore permit. The commit enters Fluree's per-ledger FIFO
        // write queue; it will block on the Mutex until prior commits
        // finish, but this task is already spawned and the main loop is
        // free to parse the next batch.
        let fluree = Arc::clone(&fluree);
        let alias = alias.clone();
        let num = batch_num;
        let stmts = batch.statements;
        let idx_cfg = index_config.clone();

        tasks.spawn(async move {
            let t0 = Instant::now();
            let result = fluree
                .graph(&alias)
                .transact()
                .insert(&json)
                .index_config(idx_cfg)
                .commit()
                .await;
            drop(permit);
            (num, stmts, t0.elapsed(), result)
        });

        // ---- Eagerly drain any already-finished tasks ------------------
        collect_completed(&mut tasks, &mut total_stmts, &mut total_flakes)?;
    }

    // ---- Drain remaining tasks -----------------------------------------
    while let Some(join_result) = tasks.join_next().await {
        let (num, stmts, elapsed, commit_result) = join_result?;
        let r = commit_result?;
        total_stmts += stmts;
        total_flakes += r.receipt.flake_count;
        tracing::info!(
            batch = num,
            stmts,
            flakes = r.receipt.flake_count,
            total_stmts,
            t = r.receipt.t,
            ms = elapsed.as_millis() as u64,
            "Batch committed"
        );
    }

    // --- Summary --------------------------------------------------------

    let elapsed = start.elapsed();
    eprintln!();
    eprintln!("Import complete");
    eprintln!("  Batches:    {batch_num}");
    eprintln!("  Statements: {total_stmts}");
    eprintln!("  Flakes:     {total_flakes}");
    eprintln!("  Elapsed:    {:.2}s", elapsed.as_secs_f64());
    if elapsed.as_secs_f64() > 0.0 {
        eprintln!(
            "  Throughput: {:.0} stmts/s",
            total_stmts as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}

/// Non-blocking drain of completed tasks from the JoinSet.
fn collect_completed(
    tasks: &mut JoinSet<CommitOutput>,
    total_stmts: &mut usize,
    total_flakes: &mut usize,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(join_result) = tasks.try_join_next() {
        let (num, stmts, elapsed, commit_result) = join_result?;
        let r = commit_result?;
        *total_stmts += stmts;
        *total_flakes += r.receipt.flake_count;
        tracing::info!(
            batch = num,
            stmts,
            flakes = r.receipt.flake_count,
            total_stmts = *total_stmts,
            t = r.receipt.t,
            ms = elapsed.as_millis() as u64,
            "Batch committed"
        );
    }
    Ok(())
}
