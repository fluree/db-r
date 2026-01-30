//! OTEL Firehose Demo Client
//!
//! Reads a large `.ttl` file, batches the statements, and POSTs each batch
//! to a running Fluree server via HTTP. Designed to be used alongside
//! `run.sh`, which starts Jaeger and an OTEL-enabled `fluree-db-server` so
//! the full transaction waterfall is visible in the Jaeger UI.
//!
//! The pipeline is the same as `bulk-turtle-import` — read → parse → commit —
//! but commits go over HTTP instead of the library API, exercising the full
//! server request path (axum routing, credential extraction, transaction
//! staging, commit, storage write) and producing OTEL spans for every phase.
//!
//! # Usage
//!
//! ```bash
//! # Typical (run.sh handles this automatically):
//! otel-firehose-client ~/Downloads/open-street-map/alb.osm.ttl \
//!     -b 10000 --inflight 8 -l osm-alb \
//!     --server-url http://localhost:8090
//! ```

mod batch_reader;

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use batch_reader::BatchReader;
use clap::Parser as ClapParser;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Stream a large Turtle (.ttl) file to a Fluree server as batched HTTP
/// transactions for OTEL trace investigation.
#[derive(ClapParser)]
#[command(name = "otel-firehose-client")]
struct Cli {
    /// Path to the .ttl file to import.
    file: PathBuf,

    /// Ledger alias (created if it doesn't exist).
    #[arg(short, long, default_value = "firehose-demo")]
    ledger: String,

    /// Number of RDF statements per batch.
    #[arg(short, long, default_value_t = 10_000)]
    batch_size: usize,

    /// Maximum pipelined in-flight HTTP requests.
    #[arg(long, default_value_t = 8)]
    inflight: usize,

    /// Fluree server base URL.
    #[arg(long, default_value = "http://localhost:8090")]
    server_url: String,
}

// ---------------------------------------------------------------------------
// Ledger creation
// ---------------------------------------------------------------------------

/// Create the ledger via `POST /fluree/create`. Treats 409 (already exists) as
/// success.
async fn create_ledger(
    client: &reqwest::Client,
    cli: &Cli,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/fluree/create", cli.server_url);
    let body = json!({ "ledger": cli.ledger });

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        tracing::info!(ledger = %cli.ledger, "Ledger created");
    } else if status.as_u16() == 409 {
        tracing::info!(ledger = %cli.ledger, "Ledger already exists");
    } else {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to create ledger (HTTP {status}): {text}").into());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Batch commit result
// ---------------------------------------------------------------------------

/// Output from a spawned HTTP commit task.
type BatchOutput = (
    usize,                                                    // batch number
    usize,                                                    // statements in batch
    std::time::Duration,                                      // elapsed
    Result<String, Box<dyn std::error::Error + Send + Sync>>, // response body or error
);

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,otel_firehose_client=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let client = reqwest::Client::new();

    // --- Create ledger -----------------------------------------------------
    create_ledger(&client, &cli).await?;

    // --- Open file and prepare reader --------------------------------------
    let file = File::open(&cli.file)?;
    let file_size = file.metadata()?.len();
    let size_mb = file_size as f64 / (1024.0 * 1024.0);
    let reader = BatchReader::new(file, cli.batch_size);

    tracing::info!(
        file = %cli.file.display(),
        size_mb = format_args!("{size_mb:.1}"),
        batch_size = cli.batch_size,
        inflight = cli.inflight,
        server = %cli.server_url,
        "Starting import"
    );

    // --- Pipeline loop -----------------------------------------------------
    let start = Instant::now();
    let mut total_stmts: usize = 0;
    let mut batch_num: usize = 0;

    let inflight = Arc::new(Semaphore::new(cli.inflight));
    let mut tasks: JoinSet<BatchOutput> = JoinSet::new();

    let insert_url = format!("{}/fluree/insert", cli.server_url);

    for batch_result in reader {
        let batch = batch_result?;
        batch_num += 1;

        // Parse TTL -> JSON-LD (CPU work, main thread, outside HTTP)
        let graph_json = fluree_graph_turtle::parse_to_json(&batch.turtle)
            .map_err(|e| format!("Parse error in batch {batch_num}: {e}"))?;

        // Build insert body
        let body = json!({
            "ledger": &cli.ledger,
            "@graph": graph_json,
        });

        // Acquire pipeline slot
        let permit = inflight
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed unexpectedly");

        // Spawn HTTP commit task
        let client = client.clone();
        let url = insert_url.clone();
        let num = batch_num;
        let stmts = batch.statements;

        tasks.spawn(async move {
            let t0 = Instant::now();
            let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = async {
                let resp = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .json(&body)
                    .send()
                    .await?;

                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    return Err(format!("Batch {num}: HTTP {status}: {text}").into());
                }
                Ok(text)
            }
            .await;

            drop(permit);
            (num, stmts, t0.elapsed(), result)
        });

        // Eagerly drain completed tasks
        collect_completed(&mut tasks, &mut total_stmts)?;
    }

    // Drain remaining tasks
    while let Some(join_result) = tasks.join_next().await {
        let (num, stmts, elapsed, result) = join_result?;
        match result {
            Ok(_) => {
                total_stmts += stmts;
                tracing::info!(
                    batch = num,
                    stmts,
                    total_stmts,
                    ms = elapsed.as_millis() as u64,
                    "Batch committed"
                );
            }
            Err(e) => return Err(format!("Batch {num} failed: {e}").into()),
        }
    }

    // --- Summary -----------------------------------------------------------
    let elapsed = start.elapsed();
    eprintln!();
    eprintln!("Import complete");
    eprintln!("  Batches:    {batch_num}");
    eprintln!("  Statements: {total_stmts}");
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
    tasks: &mut JoinSet<BatchOutput>,
    total_stmts: &mut usize,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(join_result) = tasks.try_join_next() {
        let (num, stmts, elapsed, result) = join_result?;
        match result {
            Ok(_) => {
                *total_stmts += stmts;
                tracing::info!(
                    batch = num,
                    stmts,
                    total_stmts = *total_stmts,
                    ms = elapsed.as_millis() as u64,
                    "Batch committed"
                );
            }
            Err(e) => return Err(format!("Batch {num} failed: {e}").into()),
        }
    }
    Ok(())
}
