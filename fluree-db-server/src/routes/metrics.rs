//! Prometheus metrics endpoint.
//!
//! Exposes a `GET /metrics` endpoint returning Prometheus text format.
//! Gated behind the `metrics` cargo feature.

use axum::http::StatusCode;
use axum::response::IntoResponse;

/// GET /metrics — Prometheus text format
pub async fn metrics_handler() -> impl IntoResponse {
    let handle = match PROM_HANDLE.get() {
        Some(h) => h,
        None => {
            return (StatusCode::SERVICE_UNAVAILABLE, "Metrics not initialized").into_response()
        }
    };
    let output = handle.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
        .into_response()
}

use std::sync::OnceLock;

static PROM_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();

/// Initialize the Prometheus metrics recorder. Must be called once at startup
/// before any metrics are recorded. Returns the handle used by the `/metrics`
/// endpoint to render output.
pub fn init_metrics_recorder() {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let recorder = builder.build_recorder();
    let handle = recorder.handle();

    // Install as the global metrics recorder
    metrics::set_global_recorder(recorder).expect("failed to install metrics recorder");
    PROM_HANDLE
        .set(handle)
        .expect("metrics already initialized");
}

/// Describe all metrics with help text and units (called once at startup).
pub fn describe_metrics() {
    use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

    describe_counter!(
        "fluree_query_requests_total",
        Unit::Count,
        "Total number of query requests handled"
    );
    describe_histogram!(
        "fluree_query_duration_seconds",
        Unit::Seconds,
        "Query execution duration in seconds"
    );
    describe_counter!(
        "fluree_transaction_requests_total",
        Unit::Count,
        "Total number of transaction requests handled"
    );
    describe_histogram!(
        "fluree_transaction_duration_seconds",
        Unit::Seconds,
        "Transaction execution duration in seconds"
    );
    describe_gauge!(
        "fluree_cached_ledgers",
        Unit::Count,
        "Number of currently cached ledgers"
    );
    describe_gauge!(
        "fluree_cache_size_bytes",
        Unit::Bytes,
        "Current cache size in bytes"
    );
    describe_gauge!(
        "fluree_cache_budget_bytes",
        Unit::Bytes,
        "Maximum cache budget in bytes"
    );
}

/// Record a query request metric.
// Kept for: instrumenting query handlers in routes/query.rs.
// Use when: adding per-endpoint metrics collection.
#[expect(dead_code)]
pub fn record_query(kind: &str, duration: std::time::Duration, success: bool) {
    metrics::counter!("fluree_query_requests_total", "kind" => kind.to_string(), "status" => if success { "ok" } else { "error" }.to_string())
        .increment(1);
    metrics::histogram!("fluree_query_duration_seconds", "kind" => kind.to_string())
        .record(duration.as_secs_f64());
}

/// Record a transaction request metric.
// Kept for: instrumenting transaction handlers in routes/transact.rs.
// Use when: adding per-endpoint metrics collection.
#[expect(dead_code)]
pub fn record_transaction(op: &str, duration: std::time::Duration, success: bool) {
    metrics::counter!("fluree_transaction_requests_total", "op" => op.to_string(), "status" => if success { "ok" } else { "error" }.to_string())
        .increment(1);
    metrics::histogram!("fluree_transaction_duration_seconds", "op" => op.to_string())
        .record(duration.as_secs_f64());
}
