use clap::Parser;
use mimalloc::MiMalloc;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info};
use tracing::Instrument;

use fluree_db_api::{FlureeBuilder, IndexConfig};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn init_logging() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::new(
                "fluree_ingest=info,fluree_db_api=info,fluree_db_transact=info,fluree_db_novelty=info,fluree_graph_turtle=info,fluree_db_indexer=info",
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
fn init_otel_layer(
) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync {
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
        "grpc" | _ => opentelemetry_otlp::SpanExporter::builder()
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
            p.extension().map_or(false, |ext| ext == "ttl")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map_or(false, |n| n.starts_with("chunk_"))
        })
        .collect();
    chunks.sort();
    Ok(chunks)
}

#[cfg(feature = "commit-v2")]
async fn run_import(
    args: &Args,
    chunks: &[PathBuf],
) -> Result<u64, Box<dyn std::error::Error>> {
    use fluree_db_api::{FlureeBuilder, NameService, Publisher};
    use fluree_db_transact::import::{import_commit, ImportState};

    let total = chunks.len();

    // Build Fluree connection for storage + nameservice access
    std::fs::create_dir_all(&args.db_dir)?;
    let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy()).build()?;
    let storage = fluree.storage();
    let nameservice = fluree.nameservice();

    // Verify fresh ledger: check nameservice head is empty
    let ns_record = nameservice.lookup(&args.ledger).await?;
    if let Some(ref record) = ns_record {
        if record.commit_t > 0 || record.commit_address.is_some() {
            return Err(format!(
                "Import mode requires a fresh ledger, but '{}' already has commits (t={}). \
                 Use --start to resume from a checkpoint, or delete the ledger first.",
                args.ledger, record.commit_t
            ).into());
        }
    }

    // Initialize the ledger in nameservice if it doesn't exist
    if ns_record.is_none() {
        nameservice.publish_ledger_init(&args.ledger).await?;
        info!("Initialized new ledger '{}' in nameservice", args.ledger);
    }

    let compress = !args.no_compress;
    let mut state = ImportState::new();
    let mut total_mb_ingested = 0.0_f64;
    let run_start = Instant::now();

    info!(
        "Starting import mode: {} chunks, compress={}, publish_every={}",
        total - args.start,
        compress,
        args.publish_every,
    );

    // ---- Spawn background run resolver (optional) ----
    let (run_tx, run_handle) = if args.generate_runs {
        use fluree_db_indexer::run_index::{
            CommitResolver, GlobalDicts, RunGenerationResult, RunSortOrder,
            MultiOrderRunWriter, MultiOrderConfig, persist_namespaces,
        };

        let run_dir = args
            .run_dir
            .clone()
            .unwrap_or_else(|| args.db_dir.join("runs"));
        std::fs::create_dir_all(&run_dir)?;
        let subject_fwd = run_dir.join("subjects.fwd");
        let budget = args.run_budget_mb * 1024 * 1024;
        let config = MultiOrderConfig {
            total_budget_bytes: budget,
            orders: RunSortOrder::all_build_orders().to_vec(),
            base_run_dir: run_dir.clone(),
        };

        // Bounded channel: backpressures import if resolver falls behind.
        // Bound=2 allows one blob in-flight + one queued; keeps memory stable
        // (~120MB max queued vs unbounded risk of multi-GB backlog).
        // Sends (blob_bytes, commit_address) so the resolver can emit txn-meta.
        let (tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, String)>(2);

        info!(
            "Spawning background multi-order run resolver (run_dir={:?}, budget={}MB, orders={:?})",
            run_dir, args.run_budget_mb,
            RunSortOrder::all_build_orders().iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        );

        let ledger_alias = args.ledger.clone();
        let handle: std::thread::JoinHandle<Result<RunGenerationResult, String>> =
            std::thread::Builder::new()
                .name("run-resolver".into())
                .spawn(move || {
                    let mut dicts = GlobalDicts::new(&subject_fwd)
                        .map_err(|e| format!("init dicts: {}", e))?;
                    let mut resolver = CommitResolver::new();
                    let mut writer = MultiOrderRunWriter::new(config)
                        .map_err(|e| format!("init multi-order writer: {}", e))?;
                    let mut commit_count = 0usize;

                    while let Ok((bytes, commit_address)) = rx.recv() {
                        let (op_count, t) = resolver
                            .resolve_blob(&bytes, &commit_address, &ledger_alias, &mut dicts, &mut writer)
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

                    let order_results = writer
                        .finish(&mut dicts.languages)
                        .map_err(|e| format!("writer finish: {}", e))?;

                    // Collect all run files and total records across all orders
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

                    // Persist dictionaries for Phase C (index build)
                    dicts
                        .persist(&run_dir)
                        .map_err(|e| format!("dict persist: {}", e))?;

                    // Persist namespace map for query-time IRI encoding
                    persist_namespaces(resolver.ns_prefixes(), &run_dir)
                        .map_err(|e| format!("namespace persist: {}", e))?;

                    // Persist subject reverse hash index for O(log N) IRI → s_id lookup
                    dicts
                        .subjects
                        .write_reverse_index(&run_dir.join("subjects.rev"))
                        .map_err(|e| format!("subjects.rev: {}", e))?;

                    // Persist string reverse hash index for O(log N) string → str_id lookup
                    dicts
                        .strings
                        .write_reverse_index(&run_dir.join("strings.rev"))
                        .map_err(|e| format!("strings.rev: {}", e))?;

                    Ok(RunGenerationResult {
                        run_files: all_run_files,
                        subject_count: dicts.subjects.len(),
                        predicate_count: dicts.predicates.len(),
                        string_count: dicts.strings.len(),
                        total_records,
                        commit_count,
                    })
                })
                .map_err(|e| format!("spawn resolver: {}", e))?;

        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    for (i, chunk_path) in chunks.iter().enumerate() {
        if i < args.start {
            continue;
        }

        let _chunk_span = tracing::info_span!(
            "import_chunk",
            chunk_index = i + 1,
            chunk_total = total,
        )
        .entered();

        let read_start = Instant::now();
        let ttl = std::fs::read_to_string(chunk_path)?;
        let read_elapsed = read_start.elapsed();
        let size_mb = ttl.len() as f64 / (1024.0 * 1024.0);

        info!(
            "[{}/{}] Importing {} ({:.1} MB, read in {:.1}s)",
            i + 1,
            total,
            chunk_path.file_name().unwrap().to_string_lossy(),
            size_mb,
            read_elapsed.as_secs_f64(),
        );

        let tx_start = Instant::now();
        let result = import_commit(&mut state, &ttl, storage, &args.ledger, compress).await?;
        let tx_elapsed = tx_start.elapsed();

        total_mb_ingested += size_mb;
        let tx_secs = tx_elapsed.as_secs_f64();
        let total_elapsed = run_start.elapsed().as_secs_f64();

        let chunk_flakes_per_sec = result.flake_count as f64 / tx_secs;
        let cumulative_flakes_per_sec = state.cumulative_flakes as f64 / total_elapsed;

        info!(
            "  t={} {:.1}s {:.2}M flakes ({:.1} MB/s, {:.2}M flakes/s) blob={}B  cumulative: {:.0} MB, {:.2}M flakes in {:.0}s ({:.1} MB/s, {:.2}M flakes/s)",
            result.t,
            tx_secs,
            result.flake_count as f64 / 1_000_000.0,
            size_mb / tx_secs,
            chunk_flakes_per_sec / 1_000_000.0,
            result.blob_bytes,
            total_mb_ingested,
            state.cumulative_flakes as f64 / 1_000_000.0,
            total_elapsed,
            total_mb_ingested / total_elapsed,
            cumulative_flakes_per_sec / 1_000_000.0,
        );

        // Feed commit blob + address to background resolver
        if let Some(ref tx) = run_tx {
            // block_in_place: tells tokio we may block (bounded channel backpressure).
            // This is the desired behavior — if the resolver falls behind, import
            // naturally slows down rather than accumulating unbounded memory.
            let addr = result.address.clone();
            tokio::task::block_in_place(|| {
                tx.send((result.commit_blob, addr))
                    .map_err(|_| "resolver thread exited unexpectedly")
            })?;
        }

        // Periodic checkpoint: publish nameservice head
        let chunks_since_start = i - args.start + 1;
        if args.publish_every > 0 && chunks_since_start % args.publish_every == 0 {
            nameservice
                .publish_commit(&args.ledger, &result.address, result.t)
                .await?;
            info!(
                "Published nameservice checkpoint at t={} (chunk {}/{})",
                result.t,
                i + 1,
                total,
            );
        }
    }

    // Final publish
    if let Some(ref addr) = state.previous_address {
        nameservice
            .publish_commit(&args.ledger, addr, state.t)
            .await?;
        info!("Published final nameservice head at t={}", state.t);
    }

    let total_elapsed = run_start.elapsed().as_secs_f64();
    info!(
        "Import complete. {} chunks, {:.0} MB, {:.2}M flakes, {} commits in {:.1}s ({:.1} MB/s)",
        total - args.start,
        total_mb_ingested,
        state.cumulative_flakes as f64 / 1_000_000.0,
        state.t,
        total_elapsed,
        total_mb_ingested / total_elapsed,
    );

    // ---- Finish background run resolver ----
    drop(run_tx); // close channel → resolver thread exits its recv loop
    if let Some(handle) = run_handle {
        info!("Waiting for run resolver to finish...");
        let rr_start = Instant::now();
        let run_result = match handle.join() {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return Err(format!("run generation failed: {}", e).into()),
            Err(_) => return Err("resolver thread panicked".into()),
        };
        let rr_elapsed = rr_start.elapsed().as_secs_f64();

        info!(
            "Run generation complete: {} files, {:.2}M records, {} subjects, {} predicates, {} strings, {} commits (finish wait: {:.1}s)",
            run_result.run_files.len(),
            run_result.total_records as f64 / 1_000_000.0,
            run_result.subject_count,
            run_result.predicate_count,
            run_result.string_count,
            run_result.commit_count,
            rr_elapsed,
        );
    }

    Ok(state.cumulative_flakes)
}

/// Parallel import: parse chunks on N threads, commit serially.
///
/// Chunk 0 is parsed serially to establish all namespace codes, then chunks
/// 1..N are parsed in parallel with cloned NamespaceRegistries. Commits are
/// finalized in chunk order (serial) because of the hash-chain constraint.
#[cfg(feature = "commit-v2")]
async fn run_import_parallel(
    args: &Args,
    chunks: &[PathBuf],
) -> Result<u64, Box<dyn std::error::Error>> {
    use fluree_db_api::{FlureeBuilder, NameService, Publisher};
    use fluree_db_transact::import::{
        finalize_parsed_chunk, import_commit, parse_chunk, ImportState, ParsedChunk,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let num_threads = args.parse_threads;
    let total = chunks.len();

    // ---- Storage + nameservice init (same as serial path) ----
    std::fs::create_dir_all(&args.db_dir)?;
    let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy()).build()?;
    let storage = fluree.storage();
    let nameservice = fluree.nameservice();

    let ns_record = nameservice.lookup(&args.ledger).await?;
    if let Some(ref record) = ns_record {
        if record.commit_t > 0 || record.commit_address.is_some() {
            return Err(format!(
                "Import mode requires a fresh ledger, but '{}' already has commits (t={}). \
                 Use --start to resume from a checkpoint, or delete the ledger first.",
                args.ledger, record.commit_t
            )
            .into());
        }
    }
    if ns_record.is_none() {
        nameservice.publish_ledger_init(&args.ledger).await?;
        info!("Initialized new ledger '{}' in nameservice", args.ledger);
    }

    let compress = !args.no_compress;
    let mut state = ImportState::new();
    let run_start = Instant::now();

    info!(
        "Starting parallel import: {} chunks, {} parse threads, compress={}, publish_every={}",
        total - args.start,
        num_threads,
        compress,
        args.publish_every,
    );

    // ---- Spawn background run resolver (same as serial path) ----
    let (run_tx, run_handle) = if args.generate_runs {
        use fluree_db_indexer::run_index::{
            persist_namespaces, CommitResolver, GlobalDicts, MultiOrderConfig,
            MultiOrderRunWriter, RunGenerationResult, RunSortOrder,
        };

        let run_dir = args
            .run_dir
            .clone()
            .unwrap_or_else(|| args.db_dir.join("runs"));
        std::fs::create_dir_all(&run_dir)?;
        let subject_fwd = run_dir.join("subjects.fwd");
        let budget = args.run_budget_mb * 1024 * 1024;
        let config = MultiOrderConfig {
            total_budget_bytes: budget,
            orders: RunSortOrder::all_build_orders().to_vec(),
            base_run_dir: run_dir.clone(),
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, String)>(2);

        let ledger_alias = args.ledger.clone();
        let handle: std::thread::JoinHandle<Result<RunGenerationResult, String>> =
            std::thread::Builder::new()
                .name("run-resolver".into())
                .spawn(move || {
                    let mut dicts =
                        GlobalDicts::new(&subject_fwd).map_err(|e| format!("init dicts: {}", e))?;
                    let mut resolver = CommitResolver::new();
                    let mut writer = MultiOrderRunWriter::new(config)
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

                    dicts
                        .persist(&run_dir)
                        .map_err(|e| format!("dict persist: {}", e))?;
                    persist_namespaces(resolver.ns_prefixes(), &run_dir)
                        .map_err(|e| format!("namespace persist: {}", e))?;
                    dicts
                        .subjects
                        .write_reverse_index(&run_dir.join("subjects.rev"))
                        .map_err(|e| format!("subjects.rev: {}", e))?;

                    // Persist string reverse hash index for O(log N) string → str_id lookup
                    dicts
                        .strings
                        .write_reverse_index(&run_dir.join("strings.rev"))
                        .map_err(|e| format!("strings.rev: {}", e))?;

                    Ok(RunGenerationResult {
                        run_files: all_run_files,
                        subject_count: dicts.subjects.len(),
                        predicate_count: dicts.predicates.len(),
                        string_count: dicts.strings.len(),
                        total_records,
                        commit_count,
                    })
                })
                .map_err(|e| format!("spawn resolver: {}", e))?;

        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // ---- Phase 1: Parse + commit chunk 0 serially to establish namespaces ----
    let first_chunk = args.start;
    if first_chunk < total {
        let read_start = Instant::now();
        let ttl = std::fs::read_to_string(&chunks[first_chunk])?;
        let size_mb = ttl.len() as f64 / (1024.0 * 1024.0);
        info!(
            "[{}/{}] Parsing chunk 0 serially (establishes namespaces, {:.1} MB)",
            first_chunk + 1,
            total,
            size_mb,
        );

        let result =
            import_commit(&mut state, &ttl, storage, &args.ledger, compress).await?;
        let elapsed = read_start.elapsed().as_secs_f64();

        info!(
            "  t={} {:.1}s {:.2}M flakes, blob={}B",
            result.t,
            elapsed,
            result.flake_count as f64 / 1_000_000.0,
            result.blob_bytes,
        );

        if let Some(ref tx) = run_tx {
            let addr = result.address.clone();
            tokio::task::block_in_place(|| {
                tx.send((result.commit_blob, addr))
                    .map_err(|_| "resolver thread exited unexpectedly")
            })?;
        }
    }

    // ---- Phase 2: Parse remaining chunks in parallel, commit serially ----
    let remaining_start = first_chunk + 1;
    if remaining_start < total {
        let base_registry = state.ns_registry.clone();
        let ledger = args.ledger.clone();

        // Shared atomic counter for work-stealing among parse threads
        let next_chunk = Arc::new(AtomicUsize::new(remaining_start));

        // Result channel: bounded to limit parse-ahead memory
        let (result_tx, result_rx) =
            std::sync::mpsc::sync_channel::<Result<(usize, ParsedChunk), String>>(
                num_threads * 2,
            );

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
                .spawn(move || {
                    loop {
                        let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        if idx >= chunks.len() {
                            break;
                        }

                        let ttl = match std::fs::read_to_string(&chunks[idx]) {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = result_tx.send(Err(format!(
                                    "failed to read chunk {}: {}",
                                    idx, e
                                )));
                                break;
                            }
                        };

                        let t = (idx + 1) as i64;
                        match parse_chunk(&ttl, base_registry.clone(), t, &ledger, compress) {
                            Ok(parsed) => {
                                if result_tx.send(Ok((idx, parsed))).is_err() {
                                    break; // receiver dropped (main thread errored)
                                }
                            }
                            Err(e) => {
                                let _ =
                                    result_tx.send(Err(format!("parse chunk {} failed: {}", idx, e)));
                                break;
                            }
                        }
                    }
                })
                .map_err(|e| format!("spawn parser thread: {}", e))?;

            parse_handles.push(handle);
        }
        drop(result_tx); // main thread's copy — workers have their own clones

        // Serial commit loop: receive parsed chunks, reorder, finalize in order
        let mut next_expected = remaining_start;
        let mut pending: BTreeMap<usize, ParsedChunk> = BTreeMap::new();

        for recv_result in result_rx {
            let (idx, parsed) = match recv_result {
                Ok(v) => v,
                Err(e) => return Err(e.into()),
            };

            pending.insert(idx, parsed);

            // Drain all ready chunks in order
            while let Some(parsed) = pending.remove(&next_expected) {
                let commit_start = Instant::now();
                let result =
                    finalize_parsed_chunk(&mut state, parsed, storage, &args.ledger).await?;
                let commit_elapsed = commit_start.elapsed().as_secs_f64();
                let total_elapsed = run_start.elapsed().as_secs_f64();

                info!(
                    "[{}/{}] t={} committed in {:.2}s ({:.2}M flakes, blob={}B)  cumulative: {:.2}M flakes in {:.0}s ({:.2}M flakes/s)",
                    next_expected + 1,
                    total,
                    result.t,
                    commit_elapsed,
                    result.flake_count as f64 / 1_000_000.0,
                    result.blob_bytes,
                    state.cumulative_flakes as f64 / 1_000_000.0,
                    total_elapsed,
                    state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0,
                );

                // Feed to run resolver
                if let Some(ref tx) = run_tx {
                    let addr = result.address.clone();
                    tokio::task::block_in_place(|| {
                        tx.send((result.commit_blob, addr))
                            .map_err(|_| "resolver thread exited unexpectedly")
                    })?;
                }

                // Periodic nameservice checkpoint
                let chunks_since_start = next_expected - args.start + 1;
                if args.publish_every > 0 && chunks_since_start % args.publish_every == 0 {
                    nameservice
                        .publish_commit(&args.ledger, &result.address, result.t)
                        .await?;
                    info!(
                        "Published nameservice checkpoint at t={} (chunk {}/{})",
                        result.t,
                        next_expected + 1,
                        total,
                    );
                }

                next_expected += 1;
            }
        }

        // Wait for parse threads to finish
        for handle in parse_handles {
            handle.join().expect("parse thread panicked");
        }
    }

    // Final publish
    if let Some(ref addr) = state.previous_address {
        nameservice
            .publish_commit(&args.ledger, addr, state.t)
            .await?;
        info!("Published final nameservice head at t={}", state.t);
    }

    let total_elapsed = run_start.elapsed().as_secs_f64();
    info!(
        "Parallel import complete. {} chunks, {:.2}M flakes, {} commits in {:.1}s ({:.2}M flakes/s)",
        total - args.start,
        state.cumulative_flakes as f64 / 1_000_000.0,
        state.t,
        total_elapsed,
        state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0,
    );

    // ---- Finish background run resolver ----
    drop(run_tx);
    if let Some(handle) = run_handle {
        info!("Waiting for run resolver to finish...");
        let rr_start = Instant::now();
        let run_result = match handle.join() {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return Err(format!("run generation failed: {}", e).into()),
            Err(_) => return Err("resolver thread panicked".into()),
        };
        let rr_elapsed = rr_start.elapsed().as_secs_f64();

        info!(
            "Run generation complete: {} files, {:.2}M records, {} subjects, {} predicates, {} strings, {} commits (finish wait: {:.1}s)",
            run_result.run_files.len(),
            run_result.total_records as f64 / 1_000_000.0,
            run_result.subject_count,
            run_result.predicate_count,
            run_result.string_count,
            run_result.commit_count,
            rr_elapsed,
        );
    }

    Ok(state.cumulative_flakes)
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

    /// Chunk number to start from (0-based, for resuming)
    #[arg(long, default_value_t = 0)]
    start: usize,

    /// Reindex max bytes threshold in MB (default: 2048 = 2GB for bulk loading).
    /// The default Fluree threshold is 1MB which will reject large transactions.
    #[arg(long, default_value_t = 2048)]
    reindex_max_mb: usize,

    /// Import mode: bypass staging/novelty, write commit chain directly.
    /// Much faster for bulk import of clean TTL data into a fresh ledger.
    /// Requires the `commit-v2` feature.
    #[arg(long)]
    import: bool,

    /// Publish nameservice head every N chunks (import mode only). Default: 50.
    #[arg(long, default_value_t = 50)]
    publish_every: usize,

    /// Disable zstd compression in import mode (for debugging).
    #[arg(long)]
    no_compress: bool,

    /// Generate multi-order run files (SPOT, PSOT, POST, OPST) in a background thread during import.
    /// Requires --import and the commit-v2 feature.
    #[arg(long)]
    generate_runs: bool,

    /// Directory for run files (default: {db_dir}/runs).
    #[arg(long)]
    run_dir: Option<PathBuf>,

    /// Run writer memory budget in MB (default: 256).
    #[arg(long, default_value_t = 256)]
    run_budget_mb: usize,

    /// Build multi-order indexes (SPOT, PSOT, POST, OPST) from run files.
    /// Can be combined with --import --generate-runs, or run standalone.
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
    query_sid: Option<u32>,

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
}

#[cfg(feature = "commit-v2")]
fn run_build_index(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::{build_all_indexes, RunSortOrder};

    let run_dir = args
        .run_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("runs"));
    let index_dir = args
        .index_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("index"));

    let orders = RunSortOrder::all_build_orders();

    info!(
        "Building multi-order indexes ({:?}): run_dir={:?}, index_dir={:?}",
        orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        run_dir,
        index_dir,
    );

    let results = build_all_indexes(
        &run_dir,
        &index_dir,
        orders,
        25000, // leaflet_rows
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

#[cfg(feature = "commit-v2")]
fn run_query(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::BinaryIndexStore;

    let run_dir = args
        .run_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("runs"));
    let index_dir = args
        .index_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("index"));

    info!("Loading BinaryIndexStore from {:?} + {:?}", run_dir, index_dir);
    let store = BinaryIndexStore::load(&run_dir, &index_dir)?;
    info!(
        "Index loaded: graph(s) {:?}",
        store.graph_ids(),
    );

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
                        info!("{} flakes found in txn-meta graph (g_id=1):", txn_flakes.len());
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

#[cfg(feature = "commit-v2")]
fn run_list_predicates(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_indexer::run_index::BinaryIndexStore;

    let run_dir = args
        .run_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("runs"));
    let index_dir = args
        .index_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("index"));

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

#[cfg(feature = "commit-v2")]
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
            if parts.is_empty() { String::new() } else { parts.join(" ") }
        }
        None => String::new(),
    };
    if meta.is_empty() {
        println!("  <{}> <{}> {} [{}]", s, p, o, dt);
    } else {
        println!("  <{}> <{}> {} [{}] {}", s, p, o, dt, meta);
    }
}

#[cfg(feature = "commit-v2")]
fn format_sid(sid: &fluree_db_core::Sid, ns: &std::collections::HashMap<i32, String>) -> String {
    let prefix = ns.get(&sid.namespace_code).map(|s| s.as_str()).unwrap_or("");
    format!("{}{}", prefix, sid.name)
}

/// Execute a SPARQL query against binary columnar indexes.
///
/// Pipeline:
/// 1. Load BinaryIndexStore from run_dir + index_dir
/// 2. Parse SPARQL string → SparqlAst
/// 3. Lower to ParsedQuery using namespace-based IRI encoding
/// 4. Build operator tree (DeferredScanOperator auto-selects binary path)
/// 5. Execute with ExecutionContext that has binary_store set
/// 6. Print results
#[cfg(feature = "commit-v2")]
async fn run_sparql(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use fluree_db_core::{Db, MemoryStorage, NoCache};
    use fluree_db_indexer::run_index::BinaryIndexStore;
    use fluree_db_query::context::ExecutionContext;
    use fluree_db_query::parse::encode::MemoryEncoder;
    use fluree_db_query::var_registry::VarRegistry;
    use fluree_db_query::{build_operator_tree, run_operator, ExecutableQuery};
    use fluree_db_sparql::{lower_sparql, parse_sparql};
    use std::sync::Arc;

    let sparql = args.sparql.as_ref().unwrap();
    let run_dir = args
        .run_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("runs"));
    let index_dir = args
        .index_dir
        .clone()
        .unwrap_or_else(|| args.db_dir.join("index"));

    info!("Loading BinaryIndexStore from {:?} + {:?}", run_dir, index_dir);
    let store = Arc::new(BinaryIndexStore::load(&run_dir, &index_dir)?);
    info!(
        "Index loaded: {} predicates, graph(s) {:?}",
        store.predicate_count(),
        store.graph_ids(),
    );

    // Build IRI encoder from namespace codes
    let ns = store.namespace_codes();
    let mut encoder = MemoryEncoder::new();
    for (&code, prefix) in ns {
        encoder.add_namespace(prefix, code);
    }

    // 1. Parse SPARQL
    let query_start = Instant::now();
    info!("Parsing SPARQL: {}", sparql);
    let parse_output = parse_sparql(sparql);
    if let Some(ref diags) = parse_output.diagnostics.first() {
        if diags.severity == fluree_db_sparql::Severity::Error {
            for d in &parse_output.diagnostics {
                error!("  SPARQL parse: {}", d.message);
            }
            return Err("SPARQL parse error".into());
        }
    }
    let ast = parse_output
        .ast
        .ok_or("SPARQL parsing failed (no AST produced)")?;

    // 2. Lower to query IR
    let mut vars = VarRegistry::new();
    let parsed_query = lower_sparql(&ast, &encoder, &mut vars)
        .map_err(|e| format!("SPARQL lowering error: {}", e))?;
    info!(
        "Query lowered: {} patterns, {} variables",
        parsed_query.patterns.len(),
        vars.len(),
    );

    // 3. Build operator tree
    let exec_query = ExecutableQuery::simple(parsed_query.clone());
    let operator = build_operator_tree::<MemoryStorage, NoCache>(
        &parsed_query,
        &exec_query.options,
        None, // no stats for binary index path
    )?;

    // 4. Create execution context with binary_store
    // Use a genesis DB as placeholder — the binary index is the real data source.
    // Set to_t = store.max_t() so the binary path guard recognizes this as a
    // current-time query (genesis DB has t=0, which would fail the guard).
    let db = Db::genesis(MemoryStorage::new(), NoCache, &args.ledger);
    let mut ctx = ExecutionContext::new(&db, &vars)
        .with_binary_store(store.clone(), 0);
    ctx.to_t = store.max_t();

    // 5. Execute
    let start = Instant::now();
    let batches = run_operator(operator, &ctx).await?;
    let elapsed = start.elapsed();

    // 6. Print results
    let query_elapsed = query_start.elapsed();
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();
    info!(
        "Query completed: {} rows in {:.3}s (execute: {:.3}s, parse+plan: {:.3}s)",
        total_rows,
        query_elapsed.as_secs_f64(),
        elapsed.as_secs_f64(),
        (query_elapsed - elapsed).as_secs_f64(),
    );

    // Print variable names header from SELECT clause (or all vars if SELECT *)
    let select_vars = if parsed_query.select.is_empty() {
        // SELECT * — use all variables from the first batch schema
        batches
            .first()
            .map(|b| b.schema().to_vec())
            .unwrap_or_default()
    } else {
        parsed_query.select.clone()
    };

    let var_names: Vec<String> = select_vars
        .iter()
        .map(|vid| vars.try_name(*vid).unwrap_or("?_").to_string())
        .collect();

    if !var_names.is_empty() {
        println!("{}", var_names.join("\t"));
        println!("{}", "-".repeat(var_names.len() * 20));
    }

    // Print rows
    let mut printed = 0;
    for batch in &batches {
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

    let has_special_mode = args.import
        || args.build_index
        || args.query_subject.is_some()
        || args.query_sid.is_some()
        || args.list_predicates
        || args.sparql.is_some();

    // ---- Import mode: bypass staging/novelty pipeline ----
    if args.import {
        let chunks_dir = args
            .chunks_dir
            .as_ref()
            .ok_or("--chunks-dir is required for --import")?;
        let chunks = discover_chunks(chunks_dir)?;

        if chunks.is_empty() {
            error!("No chunk_*.ttl files found in {:?}", chunks_dir);
        } else {
            info!(
                "Found {} chunks in {:?}, starting from chunk {}",
                chunks.len(),
                chunks_dir,
                args.start
            );

            #[cfg(feature = "commit-v2")]
            {
                total_flakes = if args.parse_threads > 0 {
                    run_import_parallel(&args, &chunks).await?
                } else {
                    run_import(&args, &chunks).await?
                };
            }
            #[cfg(not(feature = "commit-v2"))]
            {
                error!("Import mode requires the `commit-v2` feature.");
                shutdown_tracer();
                return Err("commit-v2 feature required for import mode".into());
            }
        }
    }

    // ---- Build multi-order indexes (Phase C) ----
    if args.build_index {
        #[cfg(feature = "commit-v2")]
        {
            run_build_index(&args)?;
        }
        #[cfg(not(feature = "commit-v2"))]
        {
            error!("--build-index requires the `commit-v2` feature.");
            shutdown_tracer();
            return Err("commit-v2 feature required".into());
        }
    }

    // ---- Query ----
    if args.query_subject.is_some() || args.query_sid.is_some() {
        #[cfg(feature = "commit-v2")]
        {
            run_query(&args)?;
        }
        #[cfg(not(feature = "commit-v2"))]
        {
            error!("--query-subject/--query-sid require the `commit-v2` feature.");
            shutdown_tracer();
            return Err("commit-v2 feature required".into());
        }
    }

    // ---- List predicates ----
    if args.list_predicates {
        #[cfg(feature = "commit-v2")]
        {
            run_list_predicates(&args)?;
        }
    }

    // ---- SPARQL query against binary indexes ----
    if args.sparql.is_some() {
        #[cfg(feature = "commit-v2")]
        {
            run_sparql(&args).await?;
        }
        #[cfg(not(feature = "commit-v2"))]
        {
            error!("--sparql requires the `commit-v2` feature.");
            shutdown_tracer();
            return Err("commit-v2 feature required".into());
        }
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
    let chunks_dir = args
        .chunks_dir
        .as_ref()
        .ok_or("--chunks-dir is required")?;
    let chunks = discover_chunks(chunks_dir)?;

    if chunks.is_empty() {
        error!("No chunk_*.ttl files found in {:?}", chunks_dir);
        shutdown_tracer();
        return Ok(());
    }

    let total = chunks.len();
    info!(
        "Found {} chunks in {:?}, starting from chunk {}",
        total, chunks_dir, args.start
    );

    // Build Fluree connection with file-backed storage
    std::fs::create_dir_all(&args.db_dir)?;
    let fluree = FlureeBuilder::file(args.db_dir.to_string_lossy()).build()?;

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
        reindex_min_bytes: args.reindex_max_mb * 1024 * 1024 / 2, // soft = half of hard
        reindex_max_bytes: args.reindex_max_mb * 1024 * 1024,
    };
    info!(
        "Index thresholds: soft={} MB, hard={} MB",
        index_config.reindex_min_bytes / (1024 * 1024),
        index_config.reindex_max_bytes / (1024 * 1024),
    );

    let mut current_ledger = ledger;
    let mut total_mb_ingested = 0.0_f64;
    let mut total_flakes: usize = 0;
    let run_start = Instant::now();

    for (i, chunk_path) in chunks.iter().enumerate() {
        if i < args.start {
            continue;
        }

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
        total - args.start,
        total_mb_ingested,
        total_flakes as f64 / 1_000_000.0,
        total_secs,
        total_mb_ingested / total_secs,
        total_flakes as f64 / 1_000_000.0 / total_secs,
    );

    shutdown_tracer();
    Ok(())
}
