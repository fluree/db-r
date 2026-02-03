//! Fluree connection setup and tracing initialization.

use std::sync::Arc;

use fluree_db_api::{connect_json_ld, FlureeClient, IndexConfig};
use serde_json::json;

use crate::CommonArgs;

/// Initialize the tracing subscriber.
///
/// When `otel` is true and the `bench-otel` feature is enabled, sets up an
/// OTEL exporter alongside the fmt layer. Otherwise, uses a plain fmt
/// subscriber.
///
/// # RUST_LOG
///
/// The `RUST_LOG` env var is always respected. If unset, the default is:
/// - Without OTEL: `warn,fluree_bench=info`
/// - With OTEL: `warn,fluree_bench=info,fluree_db_api=info,fluree_db_transact=info,fluree_db_indexer=info`
///
/// Examples:
/// ```text
/// RUST_LOG=debug                               # everything at debug
/// RUST_LOG=info,fluree_db_transact=debug        # transact internals at debug
/// RUST_LOG=info,fluree_db_indexer=debug          # indexer internals at debug
/// ```
pub fn init_tracing(otel: bool) {
    if otel {
        #[cfg(feature = "bench-otel")]
        {
            init_otel_tracing();
            return;
        }
        #[cfg(not(feature = "bench-otel"))]
        {
            eprintln!(
                "Warning: --otel requires building with --features bench-otel. \
                 Falling back to plain logging."
            );
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,fluree_bench=info".into()),
        )
        .init();
}

/// Flush and shut down the OTEL tracer provider (if active).
///
/// Mirrors the `SdkTracerProvider::shutdown()` pattern from
/// `fluree-db-server/src/telemetry.rs` (OTEL 0.31 removed the global
/// `shutdown_tracer_provider()` function).
pub fn shutdown_tracing() {
    #[cfg(feature = "bench-otel")]
    {
        if let Some(provider) = TRACER_PROVIDER.get() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!("OTEL tracer provider shutdown error: {e}");
            }
        }
    }
}

#[cfg(feature = "bench-otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "bench-otel")]
fn init_otel_tracing() {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::SpanExporter;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer as _};

    let exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("failed to build OTLP exporter");

    // Build resource with service name — this is what Jaeger displays.
    // Reads OTEL_SERVICE_NAME env var, defaulting to "fluree-bench".
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "fluree-bench".to_string());

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name)
        .with_attributes([KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    // Store provider for later shutdown.
    let _ = TRACER_PROVIDER.set(provider.clone());

    let tracer = provider.tracer("fluree-bench");

    // OTEL layer gets its OWN per-layer Targets filter so that only fluree_*
    // crate spans are exported.  Without this, third-party crate spans (hyper,
    // h2, tonic, tower-http, tokio) get exported and corrupt parent-child
    // relationships — their spans become false roots in Jaeger, orphaning the
    // actual fluree child spans into separate traces.
    //
    // This mirrors the server's pattern from telemetry.rs:172-181.
    let otel_filter = tracing_subscriber::filter::Targets::new()
        .with_target("fluree_bench", tracing::Level::DEBUG)
        .with_target("fluree_db_api", tracing::Level::DEBUG)
        .with_target("fluree_db_transact", tracing::Level::DEBUG)
        .with_target("fluree_db_query", tracing::Level::DEBUG)
        .with_target("fluree_db_indexer", tracing::Level::DEBUG)
        .with_target("fluree_db_sparql", tracing::Level::DEBUG)
        .with_target("fluree_db_core", tracing::Level::DEBUG)
        .with_target("fluree_db_novelty", tracing::Level::TRACE);

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(otel_filter);

    // The fmt (console) layer uses the global EnvFilter, which respects
    // RUST_LOG.  This is independent of the OTEL Targets filter above.
    let console_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        "warn,fluree_bench=info,fluree_db_api=info,fluree_db_transact=info,fluree_db_indexer=info"
            .into()
    });

    tracing_subscriber::registry()
        .with(console_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();
}

/// Build a JSON-LD connection config and connect to Fluree.
///
/// Mirrors the pattern from `examples/bulk-turtle-import/src/main.rs`.
pub async fn connect(args: &CommonArgs) -> Result<Arc<FlureeClient>, Box<dyn std::error::Error>> {
    let config = build_connection_config(args);

    tracing::info!(
        storage = if args.storage.is_some() {
            "file"
        } else {
            "memory"
        },
        indexing = !args.no_indexing,
        reindex_min_bytes = args.reindex_min_bytes,
        reindex_max_bytes = args.reindex_max_bytes,
        "Connecting to Fluree"
    );

    let fluree = connect_json_ld(&config).await?;
    let fluree = fluree.enable_ledger_caching();

    // Create ledger if it doesn't exist.
    let alias = normalize_alias(&args.ledger);
    let base_name = alias.split(':').next().unwrap();
    if !fluree.ledger_exists(&alias).await? {
        tracing::info!(ledger = base_name, "Creating new ledger");
        fluree.create_ledger(base_name).await?;
    } else {
        tracing::info!(ledger = %alias, "Ledger already exists");
    }

    Ok(Arc::new(fluree))
}

/// Build `IndexConfig` from CLI args.
pub fn index_config(args: &CommonArgs) -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: args.reindex_min_bytes as usize,
        reindex_max_bytes: args.reindex_max_bytes as usize,
    }
}

/// Normalize ledger alias (append `:main` if no branch specified).
pub fn normalize_alias(ledger: &str) -> String {
    if ledger.contains(':') {
        ledger.to_string()
    } else {
        format!("{ledger}:main")
    }
}

fn build_connection_config(args: &CommonArgs) -> serde_json::Value {
    let storage_node = if let Some(ref path) = args.storage {
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
                        "indexingEnabled": !args.no_indexing,
                        "reindexMinBytes": args.reindex_min_bytes,
                        "reindexMaxBytes": args.reindex_max_bytes
                    }
                }
            }
        ]
    })
}
