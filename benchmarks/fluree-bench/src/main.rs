//! Fluree Benchmark Tool
//!
//! Auto-generates relational supply-chain data and drives configurable
//! transaction and query workloads through the library API.
//!
//! # Usage
//!
//! ```bash
//! # Default ingest (10 MB, sequential, in-memory):
//! cargo run --release -p fluree-bench -- ingest
//!
//! # Large concurrent ingest with file storage:
//! cargo run --release -p fluree-bench -- ingest \
//!     --data-size-mb 100 --concurrency 4 --storage ./bench-data
//!
//! # Full benchmark (ingest + query matrix):
//! cargo run --release -p fluree-bench -- full --data-size-mb 10
//!
//! # With OTEL export (requires --features bench-otel):
//! cargo run --release -p fluree-bench --features bench-otel -- \
//!     ingest --data-size-mb 10 --otel
//! ```

mod datagen;
mod metrics;
mod runner;
mod setup;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Fluree benchmark tool: auto-generates supply-chain data and measures
/// transaction/query performance.
#[derive(Parser)]
#[command(name = "fluree-bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the transaction/ingest benchmark.
    Ingest(IngestArgs),

    /// Run query benchmarks (requires data already ingested with --storage).
    Query(QueryArgs),

    /// Run ingest followed by query benchmarks.
    Full(FullArgs),
}

/// Shared arguments for data generation and Fluree connection.
#[derive(Parser, Clone)]
struct CommonArgs {
    /// Target generated data size in megabytes.
    #[arg(long, default_value_t = 10)]
    data_size_mb: u32,

    /// Novelty size (bytes) that triggers background indexing.
    #[arg(long, default_value_t = 10_000_000)]
    reindex_min_bytes: u64,

    /// Novelty size (bytes) that blocks new commits until indexing catches up.
    #[arg(long, default_value_t = 200_000_000)]
    reindex_max_bytes: u64,

    /// Disable background indexing entirely. Novelty accumulates in RAM.
    /// Pair with a large --reindex-max-bytes to avoid hitting the hard limit.
    #[arg(long, default_value_t = false)]
    no_indexing: bool,

    /// Persistent file storage directory. Omit for in-memory (faster, data
    /// lost on exit).
    #[arg(short, long)]
    storage: Option<PathBuf>,

    /// Ledger alias.
    #[arg(long, default_value = "bench")]
    ledger: String,

    /// Report format.
    #[arg(long, default_value = "text")]
    output: OutputFormat,

    /// Enable OTEL trace export. Reads standard OTEL env vars
    /// (OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT, etc.).
    /// Requires building with --features bench-otel.
    #[arg(long, default_value_t = false)]
    otel: bool,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Parser, Clone)]
struct IngestArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Entities per transaction batch.
    #[arg(long, default_value_t = 500)]
    batch_size: usize,

    /// Maximum in-flight concurrent transactions (1 = sequential).
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
}

#[derive(Parser, Clone)]
struct QueryArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Number of times to repeat each query per cache state.
    #[arg(long, default_value_t = 3)]
    query_iterations: usize,

    /// Comma-separated concurrency levels for the query matrix.
    #[arg(long, default_value = "1,4,8")]
    query_concurrency: String,

    /// Skip result count verification.
    #[arg(long, default_value_t = false)]
    skip_verify: bool,
}

#[derive(Parser, Clone)]
struct FullArgs {
    #[command(flatten)]
    ingest: IngestArgs,

    /// Number of times to repeat each query per cache state.
    #[arg(long, default_value_t = 3)]
    query_iterations: usize,

    /// Comma-separated concurrency levels for the query matrix.
    #[arg(long, default_value = "1,4,8")]
    query_concurrency: String,

    /// Skip result count verification.
    #[arg(long, default_value_t = false)]
    skip_verify: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest(args) => {
            setup::init_tracing(args.common.otel);
            run_ingest(&args).await?;
        }
        Command::Query(args) => {
            setup::init_tracing(args.common.otel);
            run_query(&args).await?;
        }
        Command::Full(args) => {
            setup::init_tracing(args.ingest.common.otel);

            // Connect once and share the client across both phases.
            // With in-memory storage, a second connect() would discard the
            // ingested data (and its registered namespaces).
            let fluree = setup::connect(&args.ingest.common).await?;

            let mut ingest_collector = metrics::MetricsCollector::new();
            runner::transact::run(&fluree, &args.ingest, &mut ingest_collector).await?;
            metrics::report::print_ingest(&args.ingest.common, &args.ingest, &ingest_collector);

            let query_args = QueryArgs {
                common: args.ingest.common.clone(),
                query_iterations: args.query_iterations,
                query_concurrency: args.query_concurrency,
                skip_verify: args.skip_verify,
            };
            let mut query_collector = metrics::MetricsCollector::new();
            runner::query::run(&fluree, &query_args, &mut query_collector).await?;
            metrics::report::print_query(&query_args.common, &query_args, &query_collector);
        }
    }

    setup::shutdown_tracing();
    Ok(())
}

async fn run_ingest(args: &IngestArgs) -> Result<(), Box<dyn std::error::Error>> {
    let fluree = setup::connect(&args.common).await?;

    let mut collector = metrics::MetricsCollector::new();
    runner::transact::run(&fluree, args, &mut collector).await?;
    metrics::report::print_ingest(&args.common, args, &collector);

    Ok(())
}

async fn run_query(args: &QueryArgs) -> Result<(), Box<dyn std::error::Error>> {
    let fluree = setup::connect(&args.common).await?;

    let mut collector = metrics::MetricsCollector::new();
    runner::query::run(&fluree, args, &mut collector).await?;
    metrics::report::print_query(&args.common, args, &collector);

    Ok(())
}
