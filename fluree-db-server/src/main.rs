//! Fluree DB Server CLI
//!
//! Run with: `cargo run -p fluree-db-server -- --help`

use clap::Parser;
use fluree_db_server::{
    telemetry::{init_logging, shutdown_tracer, TelemetryConfig},
    FlureeServer, ServerConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments
    let config = ServerConfig::parse();

    // Initialize telemetry (logging + optional tracing)
    let telemetry_config = TelemetryConfig::with_server_config(&config);
    init_logging(&telemetry_config);

    // Log startup info
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        storage = config.storage_type_str(),
        addr = %config.listen_addr,
        cors = config.cors_enabled,
        indexing = config.indexing_enabled,
        events_auth_mode = ?config.events_auth_mode,
        log_format = ?telemetry_config.log_format,
        sensitive_data = ?telemetry_config.sensitive_data,
        query_text_logging = ?telemetry_config.query_text_logging,
        otel_enabled = telemetry_config.is_otel_enabled(),
        "Starting Fluree server"
    );

    // Create and run server
    let server = FlureeServer::new(config).await?;
    let result = server.run().await;

    // Graceful shutdown of telemetry
    shutdown_tracer().await;

    result.map_err(Into::into)
}
