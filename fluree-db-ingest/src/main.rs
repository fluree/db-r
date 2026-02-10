use clap::Parser;
use mimalloc::MiMalloc;
use std::path::PathBuf;
use std::time::Instant;
use tracing::Instrument;
use tracing::{error, info};

use fluree_db_api::{FlureeBuilder, IndexConfig};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn alias_prefix(alias: &str) -> String {
    fluree_db_core::address_path::alias_to_path_prefix(alias)
        .unwrap_or_else(|_| alias.replace(':', "/"))
}

fn default_run_dir(args: &Args) -> PathBuf {
    args.run_dir.clone().unwrap_or_else(|| {
        args.db_dir
            .join(alias_prefix(&args.ledger))
            .join("tmp_import")
    })
}

fn default_index_dir(args: &Args) -> PathBuf {
    args.index_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join(alias_prefix(&args.ledger)).join("index"))
}

fn init_logging() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::new(
                // Include query + sparql at info so OTEL spans show up
                // during ingest benchmarks (cold/hot query runs, etc).
                "fluree_ingest=info,fluree_db_api=info,fluree_db_query=info,fluree_db_sparql=info,fluree_db_transact=info,fluree_db_novelty=info,fluree_graph_turtle=info,fluree_db_indexer=info",
            )
        });

    #[cfg(feature = "otel")]
    {
        if std::env::var("OTEL_SERVICE_NAME").is_ok()
            && std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok()
        {
            // IMPORTANT: attach OTEL layer first so its type is `Layer<Registry>`.
            let subscriber = tracing_subscriber::registry()
                .with(init_otel_layer())
                .with(filter)
                .with(tracing_subscriber::fmt::layer().compact());

            let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
            return;
        }
    }

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().compact());

    let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
}

#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "otel")]
fn init_otel_layer() -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync {
    use opentelemetry::{global, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap();
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap();

    // Determine protocol (default: grpc)
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_else(|_| "grpc".to_string())
        .to_lowercase();

    // Configure OTLP span exporter based on protocol
    let exporter = match protocol.as_str() {
        "http/protobuf" | "http" => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .expect("failed to build OTLP HTTP span exporter"),
        _ => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("failed to build OTLP gRPC span exporter"),
    };

    let sampler = match std::env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_else(|_| "always_on".to_string())
        .to_lowercase()
        .as_str()
    {
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => {
            let ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse::<f64>()
                .unwrap_or(1.0);
            Sampler::TraceIdRatioBased(ratio)
        }
        "parentbased_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        _ => Sampler::AlwaysOn,
    };

    let batch = BatchSpanProcessor::builder(exporter, runtime::Tokio).build();

    let resource = Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", service_name),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(batch)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    let _ = OTEL_PROVIDER.set(tracer_provider.clone());
    global::set_tracer_provider(tracer_provider);
    OpenTelemetryLayer::new(global::tracer("fluree-ingest"))
}

#[cfg(feature = "otel")]
fn shutdown_tracer() {
    if let Some(provider) = OTEL_PROVIDER.get() {
        let _ = provider.force_flush();
        let _ = provider.shutdown();
    }
}

#[cfg(not(feature = "otel"))]
fn shutdown_tracer() {}

/// Discover and sort chunk_*.ttl files from a directory.
fn discover_chunks(dir: &std::path::Path) -> Result<Vec<PathBuf>, std::io::Error> {
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
    chunks.sort();
    Ok(chunks)
}

#[derive(Parser)]
#[command(name = "fluree-ingest", about = "Bulk TTL ingestion into Fluree DB")]
struct Args {
    /// Directory containing chunk_NNNN.ttl files.
    /// Required for --import or standard staging pipeline.
    #[arg(long)]
    chunks_dir: Option<PathBuf>,

    /// Directory to store the Fluree database
    #[arg(long)]
    db_dir: PathBuf,

    /// Ledger alias (default: "dblp:main")
    #[arg(long, default_value = "dblp:main")]
    ledger: String,

    /// Reindex max bytes threshold in MB (default: 2048 = 2GB for bulk loading).
    /// The default Fluree threshold is 1MB which will reject large transactions.
    #[arg(long, default_value_t = 2048)]
    reindex_max_mb: usize,

    /// Import mode: bypass staging/novelty, write commit chain directly.
    /// Much faster for bulk import of clean TTL data into a fresh ledger.
    #[arg(long)]
    import: bool,

    /// Publish nameservice head every N chunks (import mode only). Default: 50.
    #[arg(long, default_value_t = 50)]
    publish_every: usize,

    /// Disable zstd compression in import mode (for debugging).
    #[arg(long)]
    no_compress: bool,

    /// Directory for run files (default: {db_dir}/tmp_import).
    #[arg(long)]
    run_dir: Option<PathBuf>,

    /// Run writer memory budget in MB (default: 256).
    #[arg(long, default_value_t = 256)]
    run_budget_mb: usize,

    /// Build multi-order indexes (SPOT, PSOT, POST, OPST).
    /// Included automatically with --import. Can also be run standalone from existing run files.
    #[arg(long)]
    build_index: bool,

    /// Output directory for the indexes (default: {db_dir}/index).
    #[arg(long)]
    index_dir: Option<PathBuf>,

    /// Query a subject by IRI (requires built index). Searches default graph (g_id=0).
    #[arg(long)]
    query_subject: Option<String>,

    /// Query a subject by numeric ID (requires built index). Searches default graph (g_id=0).
    #[arg(long)]
    query_sid: Option<u64>,

    /// List all predicate IRIs stored in the binary index.
    #[arg(long)]
    list_predicates: bool,

    /// Execute a SPARQL query against the binary columnar indexes.
    /// Requires built indexes (--build-index).
    #[arg(long)]
    sparql: Option<String>,

    /// Number of parallel TTL parse threads (import mode only).
    /// 0 = serial (default). Parsing is 90% of import time; parallel parsing
    /// parses multiple chunks simultaneously while commits remain serial.
    #[arg(long, default_value_t = 0)]
    parse_threads: usize,

    /// Enable background indexing in the standard staging pipeline.
    /// Triggers index refresh with stats collection when novelty exceeds thresholds.
    /// Has no effect in --import mode.
    #[arg(long)]
    enable_indexing: bool,

    /// Dump stats diagnostics (nameservice record, pre-index manifest presence, db-root stats).
    #[arg(long)]
    dump_stats: bool,

    /// When used with --dump-stats, also print the full pre-index stats manifest JSON.
    #[arg(long)]
    dump_stats_manifest: bool,

    /// When used with --dump-stats, exit after printing diagnostics (no query/import/build).
    #[arg(long)]
    dump_stats_only: bool,
}

async fn run_build_index(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::{build_all_indexes, RunSortOrder};

    let run_dir = default_run_dir(args);
    let index_dir = default_index_dir(args);

    let orders = RunSortOrder::all_build_orders();

    info!(
        "Building multi-order indexes ({:?}): run_dir={:?}, index_dir={:?}",
        orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        run_dir,
        index_dir,
    );

    let results = build_all_indexes(
        &run_dir, &index_dir, orders, 25000, // leaflet_rows
        10,    // leaflets_per_leaf
        1,     // zstd_level
    )?;

    for (order, result) in &results {
        info!(
            "{} index complete: {} graphs, {:.2}M rows in {:.1}s",
            order.dir_name().to_uppercase(),
            result.graphs.len(),
            result.total_rows as f64 / 1_000_000.0,
            result.elapsed.as_secs_f64(),
        );

        for g in &result.graphs {
            info!(
                "  graph_{}/{}: {} leaves, {:.2}M rows",
                g.g_id,
                order.dir_name(),
                g.leaf_count,
                g.total_rows as f64 / 1_000_000.0,
            );
        }
    }

    Ok(())
}

fn run_query(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::BinaryIndexStore;

    let run_dir = default_run_dir(args);
    let index_dir = default_index_dir(args);

    info!(
        "Loading BinaryIndexStore from {:?} + {:?}",
        run_dir, index_dir
    );
    let store = BinaryIndexStore::load(&run_dir, &index_dir)?;
    info!("Index loaded: graph(s) {:?}", store.graph_ids(),);

    if let Some(ref iri) = args.query_subject {
        info!("Querying subject by IRI: {}", iri);
        let s_id = store.find_subject_id(iri)?;
        match s_id {
            Some(id) => {
                info!("Resolved '{}' → s_id={}", iri, id);
                let flakes = store.query_subject_flakes(0, id)?;
                if flakes.is_empty() {
                    info!("No flakes found in default graph (g_id=0)");
                    // Try txn-meta graph
                    let txn_flakes = store.query_subject_flakes(1, id)?;
                    if !txn_flakes.is_empty() {
                        info!(
                            "{} flakes found in txn-meta graph (g_id=1):",
                            txn_flakes.len()
                        );
                        for f in &txn_flakes {
                            print_flake(f, &store);
                        }
                    }
                } else {
                    info!("{} flakes found:", flakes.len());
                    for f in &flakes {
                        print_flake(f, &store);
                    }
                }
            }
            None => {
                info!("Subject '{}' not found in reverse index", iri);
            }
        }
    }

    if let Some(sid) = args.query_sid {
        info!("Querying subject by s_id: {}", sid);
        match store.resolve_subject_iri(sid) {
            Ok(iri) => info!("s_id={} → '{}'", sid, iri),
            Err(e) => info!("s_id={} not resolvable: {}", sid, e),
        }
        let flakes = store.query_subject_flakes(0, sid)?;
        if flakes.is_empty() {
            info!("No flakes found for s_id {} in default graph", sid);
        } else {
            info!("{} flakes found for s_id {}:", flakes.len(), sid);
            for f in &flakes {
                print_flake(f, &store);
            }
        }
    }

    Ok(())
}

fn run_list_predicates(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::BinaryIndexStore;

    let run_dir = default_run_dir(args);
    let index_dir = default_index_dir(args);

    let store = BinaryIndexStore::load(&run_dir, &index_dir)?;
    let count = store.predicate_count();
    info!("Predicate dictionary: {} entries", count);

    for p_id in 0..count {
        match store.resolve_predicate_iri(p_id) {
            Some(iri) => println!("  p_id={:>3}  {}", p_id, iri),
            None => println!("  p_id={:>3}  (unresolvable)", p_id),
        }
    }

    // Also dump namespace codes for debugging
    let ns = store.namespace_codes();
    info!("Namespace codes: {} entries", ns.len());
    let mut codes: Vec<_> = ns.iter().collect();
    codes.sort_by_key(|(&c, _)| c);
    for (&code, prefix) in &codes {
        println!("  ns_code={:>3}  {}", code, prefix);
    }

    Ok(())
}

fn print_flake(f: &fluree_db_core::Flake, store: &fluree_db_indexer::run_index::BinaryIndexStore) {
    let ns = store.namespace_codes();
    let s = format_sid(&f.s, ns);
    let p = format_sid(&f.p, ns);
    let o = format!("{}", f.o);
    let dt = format_sid(&f.dt, ns);
    let meta = match &f.m {
        Some(m) => {
            let mut parts = Vec::new();
            if let Some(ref lang) = m.lang {
                parts.push(format!("@{}", lang));
            }
            if let Some(i) = m.i {
                parts.push(format!("[{}]", i));
            }
            if parts.is_empty() {
                String::new()
            } else {
                parts.join(" ")
            }
        }
        None => String::new(),
    };
    if meta.is_empty() {
        println!("  <{}> <{}> {} [{}]", s, p, o, dt);
    } else {
        println!("  <{}> <{}> {} [{}] {}", s, p, o, dt, meta);
    }
}

fn format_sid(sid: &fluree_db_core::Sid, ns: &std::collections::HashMap<u16, String>) -> String {
    let prefix = ns
        .get(&sid.namespace_code)
        .map(|s| s.as_str())
        .unwrap_or("");
    format!("{}{}", prefix, sid.name)
}

/// Execute a SPARQL query against binary columnar indexes.
///
/// Pipeline:
/// 1. Load BinaryIndexStore from run_dir + index_dir
/// 2. Parse SPARQL string → SparqlAst
/// 3. Lower to ParsedQuery using namespace-based IRI encoding
/// 4. Build operator tree (ScanOperator auto-selects binary path)
/// 5. Execute with ExecutionContext that has binary_store set
/// 6. Print results
async fn run_sparql(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_api::FlureeBuilder;

    let sparql = args.sparql.as_ref().unwrap();

    let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy().to_string()).build()?;

    let load_start = Instant::now();
    let view = fluree.view(&args.ledger).await?;
    let load_elapsed = load_start.elapsed();
    info!("View loaded in {:.3}s", load_elapsed.as_secs_f64());

    info!("Executing SPARQL: {}", sparql);
    let start = Instant::now();
    let result = fluree.query_view(&view, sparql.as_str()).await?;
    let elapsed = start.elapsed();

    let total_rows: usize = result.batches.iter().map(|b| b.len()).sum();
    info!(
        "Query: {} rows in {:.3}s",
        total_rows,
        elapsed.as_secs_f64()
    );

    // Log dict tree I/O stats
    if let Some(store) = view.binary_store() {
        store.log_dict_stats();
    }

    // Print results as TSV
    let var_names: Vec<String> = result
        .select
        .iter()
        .map(|vid| result.vars.try_name(*vid).unwrap_or("?_").to_string())
        .collect();

    if !var_names.is_empty() {
        println!("{}", var_names.join("\t"));
        println!("{}", "-".repeat(var_names.len() * 20));
    }

    let mut printed = 0;
    for batch in &result.batches {
        for row in 0..batch.len() {
            let cols: Vec<String> = (0..batch.schema().len())
                .map(|col| format!("{:?}", batch.get_by_col(row, col)))
                .collect();
            println!("{}", cols.join("\t"));
            printed += 1;
            if printed >= 100 {
                if total_rows > 100 {
                    println!("... ({} more rows)", total_rows - 100);
                }
                return Ok(());
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let args = Args::parse();
    let overall_start = Instant::now();
    let mut total_flakes: u64 = 0;

    // ---- Stats dump (diagnostics) ----
    if args.dump_stats {
        use fluree_db_api::{FlureeBuilder, NameService};
        use fluree_db_core::StorageRead;

        let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy().to_string()).build()?;
        let storage = fluree.storage().clone();
        let ns = fluree.nameservice();

        let ns_record = ns.lookup(&args.ledger).await?;
        info!(
            ledger = %args.ledger,
            ns_present = ns_record.is_some(),
            commit_t = ns_record.as_ref().map(|r| r.commit_t).unwrap_or(0),
            index_t = ns_record.as_ref().map(|r| r.index_t).unwrap_or(0),
            index_head_id = %ns_record
                .as_ref()
                .and_then(|r| r.index_head_id.as_ref())
                .map(|cid| cid.to_string())
                .unwrap_or_default(),
            "Nameservice ledger record"
        );

        let alias_prefix = fluree_db_core::address_path::alias_to_path_prefix(&args.ledger)
            .unwrap_or_else(|_| args.ledger.replace(':', "/"));
        let manifest_addr = format!("fluree:file://{}/stats/pre-index-stats.json", alias_prefix);
        match storage.read_bytes(&manifest_addr).await {
            Ok(bytes) => {
                info!(
                    manifest_addr = %manifest_addr,
                    manifest_bytes = bytes.len(),
                    "Found pre-index stats manifest in storage"
                );
                if args.dump_stats_manifest {
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        println!("{}", s);
                    } else {
                        println!("<pre-index-stats.json is not valid UTF-8>");
                    }
                } else if let Ok(graphs) =
                    fluree_db_api::ledger_info::parse_pre_index_manifest(&bytes)
                {
                    let prop_count: usize = graphs.iter().map(|g| g.properties.len()).sum();
                    info!(
                        manifest_addr = %manifest_addr,
                        graphs = graphs.len(),
                        properties = prop_count,
                        "Pre-index stats manifest summary"
                    );
                } else {
                    info!(manifest_addr = %manifest_addr, "Pre-index stats manifest is not valid JSON");
                }
            }
            Err(_) => {
                info!(manifest_addr = %manifest_addr, "Pre-index stats manifest NOT found in storage");
            }
        }

        if let Some(index_cid) = ns_record.and_then(|r| r.index_head_id) {
            use fluree_db_core::ContentStore;
            use fluree_db_indexer::run_index::BinaryIndexRoot;

            let cs = fluree_db_core::content_store_for(storage.clone(), &args.ledger);
            match cs.get(&index_cid).await {
                Ok(bytes) => {
                    info!(
                        index_cid = %index_cid,
                        bytes = bytes.len(),
                        "Loaded index root descriptor"
                    );

                    if args.dump_stats_manifest {
                        if let Ok(s) = std::str::from_utf8(&bytes) {
                            println!("{}", s);
                        }
                    } else {
                        match BinaryIndexRoot::from_json_bytes(&bytes) {
                            Ok(root) => {
                                info!(
                                    index_cid = %index_cid,
                                    schema_version = root.version,
                                    index_t = root.index_t,
                                    base_t = root.base_t,
                                    graphs = root.graphs.len(),
                                    predicates = root.predicate_sids.len(),
                                    namespaces = root.namespace_codes.len(),
                                    subject_id_encoding = ?root.subject_id_encoding,
                                    "Binary index root V2 summary"
                                );
                            }
                            Err(e) => {
                                info!(
                                    index_cid = %index_cid,
                                    "Failed to parse index root as V2: {}", e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    info!(index_cid = %index_cid, "Failed to read index root descriptor: {}", e);
                }
            }
        } else {
            info!("No index_head_id in nameservice; cannot load db-root stats");
        }

        if args.dump_stats_only {
            shutdown_tracer();
            return Ok(());
        }
    }

    let has_special_mode = args.import
        || args.build_index
        || args.query_subject.is_some()
        || args.query_sid.is_some()
        || args.list_predicates
        || args.sparql.is_some();

    // ---- Import mode: bulk import via fluree-db-api pipeline ----
    if args.import {
        let chunks_dir = args
            .chunks_dir
            .as_ref()
            .ok_or("--chunks-dir is required for --import")?;

        std::fs::create_dir_all(&args.db_dir)?;
        let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy()).build()?;
        let mut builder = fluree.create(&args.ledger).import(chunks_dir);

        if args.parse_threads > 0 {
            builder = builder.threads(args.parse_threads);
        }
        builder = builder
            .run_budget_mb(args.run_budget_mb)
            .compress(!args.no_compress)
            .publish_every(args.publish_every);

        let result = builder
            .execute()
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

        total_flakes = result.flake_count;
        info!(
            "Import complete: t={}, {:.2}M flakes, root={:?}",
            result.t,
            result.flake_count as f64 / 1_000_000.0,
            result.root_address,
        );
    }

    // ---- Build multi-order indexes (standalone, only when not combined with --import) ----
    if args.build_index && !args.import {
        run_build_index(&args).await?;
    }

    // ---- Query ----
    if args.query_subject.is_some() || args.query_sid.is_some() {
        run_query(&args)?;
    }

    // ---- List predicates ----
    if args.list_predicates {
        run_list_predicates(&args)?;
    }

    // ---- SPARQL query against binary indexes ----
    if args.sparql.is_some() {
        run_sparql(&args).await?;
    }

    if has_special_mode {
        let wall_secs = overall_start.elapsed().as_secs_f64();
        if total_flakes > 0 {
            info!(
                "ALL DONE. {:.2}M flakes, wall time {:.1}s, overall {:.2}M flakes/s",
                total_flakes as f64 / 1_000_000.0,
                wall_secs,
                total_flakes as f64 / wall_secs / 1_000_000.0,
            );
        } else {
            info!("ALL DONE. wall time {:.1}s", wall_secs);
        }
        shutdown_tracer();
        return Ok(());
    }

    // ---- Standard staging pipeline ----
    let chunks_dir = args.chunks_dir.as_ref().ok_or("--chunks-dir is required")?;
    let chunks = discover_chunks(chunks_dir)?;

    if chunks.is_empty() {
        error!("No chunk_*.ttl files found in {:?}", chunks_dir);
        shutdown_tracer();
        return Ok(());
    }

    let total = chunks.len();
    info!("Found {} chunks in {:?}", total, chunks_dir);

    // Build Fluree connection with file-backed storage
    std::fs::create_dir_all(&args.db_dir)?;
    let mut fluree = FlureeBuilder::file(args.db_dir.to_string_lossy()).build()?;

    // Enable background indexing if requested
    if args.enable_indexing {
        use fluree_db_api::{BackgroundIndexerWorker, IndexerConfig, IndexingMode};

        let indexer_config = IndexerConfig::default();
        let (worker, handle) = BackgroundIndexerWorker::new(
            fluree.storage().clone(),
            std::sync::Arc::new(fluree.nameservice().clone()),
            indexer_config,
        );
        tokio::spawn(worker.run());
        fluree.set_indexing_mode(IndexingMode::Background(handle));
        info!("Background indexing enabled");
    }

    // Create or load ledger
    let ledger = match fluree.create_ledger(&args.ledger).await {
        Ok(l) => {
            info!("Created new ledger '{}'", args.ledger);
            l
        }
        Err(e) => {
            info!("Ledger may exist, attempting to load: {e}");
            fluree.ledger(&args.ledger).await?
        }
    };

    let index_config = IndexConfig {
        reindex_min_bytes: if args.enable_indexing {
            50 * 1024 * 1024 // 50 MB soft threshold when indexing enabled
        } else {
            args.reindex_max_mb * 1024 * 1024 / 2
        },
        reindex_max_bytes: if args.enable_indexing {
            200 * 1024 * 1024 // 200 MB hard threshold when indexing enabled
        } else {
            args.reindex_max_mb * 1024 * 1024
        },
    };
    info!(
        "Index thresholds: soft={} MB, hard={} MB{}",
        index_config.reindex_min_bytes / (1024 * 1024),
        index_config.reindex_max_bytes / (1024 * 1024),
        if args.enable_indexing {
            " (indexing enabled)"
        } else {
            ""
        },
    );

    let mut current_ledger = ledger;
    let mut total_mb_ingested = 0.0_f64;
    let mut total_flakes: usize = 0;
    let run_start = Instant::now();

    for (i, chunk_path) in chunks.iter().enumerate() {
        let chunk_span = tracing::info_span!(
            "ingest_chunk",
            chunk_index = i + 1,
            chunk_total = total,
            chunk_file = %chunk_path.display()
        );
        let _chunk_guard = chunk_span.enter();

        let read_span = tracing::info_span!("chunk_read");
        let read_start = Instant::now();
        let ttl = {
            let _g = read_span.enter();
            std::fs::read_to_string(chunk_path)?
        };
        let read_elapsed = read_start.elapsed();
        let size_mb = ttl.len() as f64 / (1024.0 * 1024.0);

        info!(
            "[{}/{}] Ingesting {} ({:.1} MB, read in {:.1}s)",
            i + 1,
            total,
            chunk_path.file_name().unwrap().to_string_lossy(),
            size_mb,
            read_elapsed.as_secs_f64(),
        );

        let tx_span = tracing::info_span!(
            "chunk_transact",
            size_mb = size_mb,
            ledger = %args.ledger
        );
        let tx_start = Instant::now();
        let result = async {
            fluree
                .stage_owned(current_ledger)
                .insert_turtle(&ttl)
                .index_config(index_config.clone())
                .execute()
                .await
        }
        .instrument(tx_span)
        .await?;
        let tx_elapsed = tx_start.elapsed();

        current_ledger = result.ledger;
        let chunk_flakes = result.receipt.flake_count;
        total_mb_ingested += size_mb;
        total_flakes += chunk_flakes;

        let tx_secs = tx_elapsed.as_secs_f64();
        let chunk_mb_s = size_mb / tx_secs;
        let chunk_mf_s = chunk_flakes as f64 / 1_000_000.0 / tx_secs;
        let total_elapsed = run_start.elapsed();
        let total_secs = total_elapsed.as_secs_f64();
        let cumulative_mb_s = total_mb_ingested / total_secs;
        let cumulative_mf_s = total_flakes as f64 / 1_000_000.0 / total_secs;

        info!(
            "  Committed t={}  {:.1}s  {:.2}M flakes ({:.1} MB/s, {:.2}M flakes/s)  cumulative: {:.0} MB, {:.2}M flakes in {:.0}s ({:.1} MB/s, {:.2}M flakes/s)",
            result.receipt.t,
            tx_secs,
            chunk_flakes as f64 / 1_000_000.0,
            chunk_mb_s,
            chunk_mf_s,
            total_mb_ingested,
            total_flakes as f64 / 1_000_000.0,
            total_secs,
            cumulative_mb_s,
            cumulative_mf_s,
        );
    }

    let total_elapsed = run_start.elapsed();
    let total_secs = total_elapsed.as_secs_f64();
    info!(
        "Ingestion complete. {} chunks, {:.0} MB, {:.2}M flakes in {:.1}s ({:.1} MB/s, {:.2}M flakes/s)",
        total,
        total_mb_ingested,
        total_flakes as f64 / 1_000_000.0,
        total_secs,
        total_mb_ingested / total_secs,
        total_flakes as f64 / 1_000_000.0 / total_secs,
    );

    shutdown_tracer();
    Ok(())
}
