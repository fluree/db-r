//! Integration tests for telemetry functionality

use fluree_db_server::telemetry::{TelemetryConfig, init_logging, shutdown_tracer};
use std::env;

#[tokio::test]
async fn test_telemetry_initialization() {
    // Set up test environment
    env::set_var("RUST_LOG", "debug");
    env::set_var("LOG_FORMAT", "json");

    // Initialize telemetry
    let config = TelemetryConfig::default();
    init_logging(&config);

    // Basic logging should work
    tracing::info!("telemetry test log");

    // Shutdown
    shutdown_tracer().await;
}

#[tokio::test]
async fn test_request_span_creation() {
    use fluree_db_server::telemetry::{create_request_span, set_span_error_code};

    let config = TelemetryConfig::default();
    init_logging(&config);

    // Create a test span
    let span = create_request_span(
        "test_operation",
        Some("test-request-123"),
        Some("test-trace-456"),
        Some("test-ledger"),
        None,
    );

    // Test setting error code
    {
        let _guard = span.enter();
        set_span_error_code(&span, "error:TestError");
        tracing::error!("test error with code");
    }

    shutdown_tracer().await;
}