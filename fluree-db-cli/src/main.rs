mod cli;
mod commands;
mod config;
mod context;
mod detect;
mod error;
mod input;
mod output;
mod remote_client;

use clap::Parser;
use cli::{Cli, Commands};
use error::exit_with_error;
#[cfg(feature = "otel")]
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
#[cfg(feature = "otel")]
use tracing_subscriber::Layer as _;

fn init_tracing(cli: &Cli) {
    // CLI tracing policy:
    //   --quiet  → always "off" (no logs, no matter what)
    //   --verbose → "info" level for fluree crates (useful diagnostics)
    //   default  → "off" (clean terminal, progress bars only)
    //   RUST_LOG → honoured only when neither --verbose nor --quiet is set,
    //              so developers can still get fine-grained control.
    let filter = if cli.quiet {
        EnvFilter::new("off")
    } else if cli.verbose {
        // --verbose: honour RUST_LOG if set, otherwise show info for fluree crates.
        EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
    } else {
        // Default: suppress all logs. RUST_LOG is intentionally ignored so that
        // developer env vars don't leak log lines into the user-facing CLI output
        // (which uses progress bars on stderr). Use --verbose to see logs.
        EnvFilter::new("off")
    };

    let ansi = !(cli.no_color || std::env::var_os("NO_COLOR").is_some());

    // When the `otel` feature is enabled and OTEL env vars are set, use a
    // dual-layer subscriber (same architecture as fluree-db-server):
    //   - Console layer: controlled by --quiet/--verbose/RUST_LOG
    //   - OTEL layer: hardcoded Targets filter for fluree_* crates at DEBUG
    #[cfg(feature = "otel")]
    {
        let otel_service = std::env::var("OTEL_SERVICE_NAME").ok();
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

        if otel_service.is_some() && otel_endpoint.is_some() {
            let otel_targets = Targets::new()
                .with_target("fluree_db_cli", tracing::Level::DEBUG)
                .with_target("fluree_db_api", tracing::Level::DEBUG)
                .with_target("fluree_db_query", tracing::Level::DEBUG)
                .with_target("fluree_db_transact", tracing::Level::DEBUG)
                .with_target("fluree_db_indexer", tracing::Level::DEBUG)
                .with_target("fluree_db_ledger", tracing::Level::DEBUG)
                .with_target("fluree_db_connection", tracing::Level::DEBUG)
                .with_target("fluree_db_nameservice", tracing::Level::DEBUG)
                .with_target("fluree_db_core", tracing::Level::DEBUG);

            let otel_layer = init_otel_layer(
                otel_service.as_deref().unwrap(),
                otel_endpoint.as_deref().unwrap(),
            );

            let subscriber = tracing_subscriber::registry()
                .with(otel_layer.with_filter(otel_targets))
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(ansi)
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .compact()
                        .with_filter(filter),
                );

            let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
            return;
        }
    }

    let subscriber = tracing_subscriber::registry().with(filter).with(
        tracing_subscriber::fmt::layer()
            .with_ansi(ansi)
            .with_target(true)
            .with_writer(std::io::stderr)
            .compact(),
    );

    let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
}

/// Initialize the OTEL tracing layer (same configuration as fluree-db-server).
///
/// SYNC: This function mirrors `fluree-db-server/src/telemetry.rs::init_otel_layer`.
/// If you change the exporter, sampler, batch processor, or Targets filter here,
/// apply the same change there. Both must stay in lock-step.
/// See CLAUDE.md § "Tracing & OTEL Spans" for the maintenance protocol.
#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "otel")]
fn init_otel_layer(
    service_name: &str,
    endpoint: &str,
) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync {
    use opentelemetry::{global, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_else(|_| "grpc".to_string())
        .to_lowercase();

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

    // Match server's queue size to handle span volume from large imports.
    let batch = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(1_000_000)
                .build(),
        )
        .build();

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name.to_string()),
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

    OpenTelemetryLayer::new(global::tracer("fluree-cli"))
}

/// Flush and shutdown the OTEL tracer provider.
///
/// No-op when the `otel` feature is disabled or OTEL was not initialized.
async fn shutdown_tracer() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = OTEL_PROVIDER.get() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Disable color when --no-color flag or NO_COLOR env var is set.
    // We intentionally do NOT disable on "stdout is not a TTY" because errors
    // go to stderr — piping stdout (e.g., `fluree query ... | jq`) should not
    // strip color from error messages that appear on the terminal's stderr.
    if cli.no_color || std::env::var_os("NO_COLOR").is_some() {
        colored::control::set_override(false);
    }

    // Skip CLI tracing for:
    // - Server subcommands (Run, Child) that own the global tracing subscriber
    // - MCP serve (stdio transport uses stdout; any tracing to stderr could
    //   interfere with the JSON-RPC protocol)
    let skip_tracing = matches!(cli.command, Commands::Mcp { .. });

    #[cfg(feature = "server")]
    let skip_tracing = skip_tracing
        || matches!(
            cli.command,
            Commands::Server {
                action: cli::ServerAction::Run { .. } | cli::ServerAction::Child { .. }
            }
        );

    if !skip_tracing {
        init_tracing(&cli);
    }

    let result = run(cli).await;
    shutdown_tracer().await;

    if let Err(e) = result {
        exit_with_error(e);
    }
}

async fn run(cli: Cli) -> error::CliResult<()> {
    let config_path = cli.config.as_deref();
    let direct = cli.direct;

    match cli.command {
        Commands::Init { global, format } => {
            let config_format = match format {
                cli::InitFormat::Toml => fluree_db_api::server_defaults::ConfigFormat::Toml,
                cli::InitFormat::Jsonld => fluree_db_api::server_defaults::ConfigFormat::JsonLd,
            };
            commands::init::run(global, config_format)
        }

        Commands::Create {
            ledger,
            from,
            chunk_size_mb,
            memory_budget_mb,
            parallelism,
            leaflet_rows,
            leaflets_per_leaf,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            // Create-specific flags take precedence; fall back to global flags.
            let import_opts = commands::create::ImportOpts {
                memory_budget_mb: if memory_budget_mb > 0 {
                    memory_budget_mb
                } else {
                    cli.memory_budget_mb
                },
                parallelism: if parallelism > 0 {
                    parallelism
                } else {
                    cli.parallelism
                },
                chunk_size_mb,
                leaflet_rows,
                leaflets_per_leaf,
            };
            commands::create::run(
                &ledger,
                from.as_deref(),
                &fluree_dir,
                cli.verbose,
                cli.quiet,
                &import_opts,
            )
            .await
        }

        Commands::Use { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::use_cmd::run(&ledger, &fluree_dir).await
        }

        Commands::List { remote } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::list::run(&fluree_dir, remote.as_deref(), direct).await
        }

        Commands::Info { ledger, remote } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::info::run(ledger.as_deref(), &fluree_dir, remote.as_deref(), direct).await
        }

        Commands::Drop { name, force } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::drop::run(&name, force, &fluree_dir).await
        }

        Commands::Insert {
            args,
            expr,
            file,
            message,
            format,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::insert::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                message.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
            )
            .await
        }

        Commands::Upsert {
            args,
            expr,
            file,
            message,
            format,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upsert::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                message.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
            )
            .await
        }

        Commands::Query {
            args,
            expr,
            file,
            format,
            bench,
            sparql,
            jsonld,
            at,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::query::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                &format,
                bench,
                sparql,
                jsonld,
                at.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
            )
            .await
        }

        Commands::History {
            entity,
            ledger,
            from,
            to,
            predicate,
            format,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::history::run(
                &entity,
                ledger.as_deref(),
                &from,
                &to,
                predicate.as_deref(),
                &format,
                &fluree_dir,
            )
            .await
        }

        Commands::Export { ledger, format, at } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::export::run(ledger.as_deref(), &format, at.as_deref(), &fluree_dir).await
        }

        Commands::Log {
            ledger,
            oneline,
            count,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::log::run(ledger.as_deref(), oneline, count, &fluree_dir).await
        }

        Commands::Config { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            match action {
                cli::ConfigAction::SetOrigins { ledger, file } => {
                    commands::config_cmd::run_set_origins(&ledger, &file, &fluree_dir).await
                }
                other => commands::config_cmd::run(other, &fluree_dir),
            }
        }

        Commands::Prefix { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::prefix::run(action, &fluree_dir)
        }

        Commands::Completions { shell } => {
            commands::completions::run(shell);
            Ok(())
        }

        Commands::Token { action } => commands::token::run(action),

        Commands::Remote { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::remote::run(action, &fluree_dir).await
        }

        Commands::Auth { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::auth::run(action, &fluree_dir).await
        }

        Commands::Upstream { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upstream::run(action, &fluree_dir).await
        }

        Commands::Fetch { remote } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_fetch(&remote, &fluree_dir).await
        }

        Commands::Pull { ledger, no_indexes } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_pull(ledger.as_deref(), no_indexes, &fluree_dir).await
        }

        Commands::Push { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_push(ledger.as_deref(), &fluree_dir).await
        }

        Commands::Clone {
            args,
            origin,
            token,
            alias,
            no_indexes,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            if let Some(origin_uri) = origin {
                // --origin mode: args = [ledger]
                if args.len() != 1 {
                    return Err(error::CliError::Usage(
                        "with --origin, provide exactly one positional arg: <ledger>".into(),
                    ));
                }
                commands::sync::run_clone_origin(
                    &origin_uri,
                    token.as_deref(),
                    &args[0],
                    alias.as_deref(),
                    no_indexes,
                    &fluree_dir,
                )
                .await
            } else {
                // Named-remote mode: args = [remote, ledger]
                if args.len() != 2 {
                    return Err(error::CliError::Usage(
                        "usage: fluree clone <remote> <ledger>  or  fluree clone --origin <uri> <ledger>".into(),
                    ));
                }
                commands::sync::run_clone(
                    &args[0],
                    &args[1],
                    alias.as_deref(),
                    no_indexes,
                    &fluree_dir,
                )
                .await
            }
        }

        Commands::Track { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::track::run(action, &fluree_dir).await
        }

        #[cfg(feature = "server")]
        Commands::Server { action } => commands::server::run(action, config_path).await,

        #[cfg(not(feature = "server"))]
        Commands::Server { .. } => Err(error::CliError::Server(
            "server support not compiled. Rebuild with `--features server`.".into(),
        )),

        Commands::Memory { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::memory::run(action, &fluree_dir).await
        }

        Commands::Mcp { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            match action {
                cli::McpAction::Serve { transport } => {
                    commands::mcp_serve::run(&transport, &fluree_dir).await
                }
            }
        }
    }
}
