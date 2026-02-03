//! Telemetry module for logging and tracing setup
//!
//! Provides unified logging configuration, optional OTEL tracing, and graceful shutdown.
//! Follows the logging plan for performance-conscious observability.

use crate::config::ServerConfig;
use std::env;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
#[cfg(feature = "otel")]
use tracing_subscriber::registry::LookupSpan;

/// Telemetry configuration
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Primary log filter (RUST_LOG env var)
    pub log_filter: String,
    /// Fallback log level if RUST_LOG not set
    pub default_level: String,
    /// Request ID header name (default: "x-request-id")
    pub request_id_header: String,
    /// Log format ("human" or "json")
    pub log_format: LogFormat,
    /// Sensitive data handling
    pub sensitive_data: SensitiveDataHandling,
    /// Query text logging mode
    pub query_text_logging: QueryTextLogging,
    /// OTEL service name (if OTEL enabled)
    pub otel_service_name: Option<String>,
    /// OTEL endpoint (if OTEL enabled)
    pub otel_endpoint: Option<String>,
}

impl TelemetryConfig {
    /// Check if OTEL tracing should be enabled
    ///
    /// Requires both service name and endpoint to be set
    pub fn is_otel_enabled(&self) -> bool {
        self.otel_service_name.is_some() && self.otel_endpoint.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Human,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SensitiveDataHandling {
    /// Show all data
    Off,
    /// Mask sensitive fields (default)
    Mask,
    /// Hash sensitive data
    Hash,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryTextLogging {
    /// Don't log query text (default)
    Off,
    /// Log full query text at debug level
    Full,
    /// Log SHA256 hash of query at info level, full text at trace
    Hash,
}

impl TelemetryConfig {
    /// Create telemetry config with server config for CLI log level support
    pub fn with_server_config(server_config: &ServerConfig) -> Self {
        let rust_log = env::var("RUST_LOG").unwrap_or_default();
        let default_level = if rust_log.is_empty() {
            // Fallback to LOG_LEVEL env var, then server config, then "info"
            env::var("LOG_LEVEL").unwrap_or_else(|_| server_config.log_level.clone())
        } else {
            server_config.log_level.clone() // Not used when RUST_LOG is set, but store for consistency
        };

        Self::from_env_with_defaults(default_level)
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        let rust_log = env::var("RUST_LOG").unwrap_or_default();
        let default_level = if rust_log.is_empty() {
            // Fallback to LOG_LEVEL when RUST_LOG is unset
            env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
        } else {
            "info".to_string() // Not used when RUST_LOG is set
        };

        Self::from_env_with_defaults(default_level)
    }
}

impl TelemetryConfig {
    fn from_env_with_defaults(default_level: String) -> Self {
        Self {
            log_filter: env::var("RUST_LOG").unwrap_or_default(),
            default_level,
            request_id_header: env::var("LOG_REQUEST_ID_HEADER")
                .unwrap_or_else(|_| "x-request-id".to_string()),
            log_format: match env::var("LOG_FORMAT")
                .unwrap_or_default()
                .to_lowercase()
                .as_str()
            {
                "json" => LogFormat::Json,
                _ => LogFormat::Human,
            },
            sensitive_data: match env::var("LOG_SENSITIVE_DATA")
                .unwrap_or_else(|_| "mask".to_string())
                .to_lowercase()
                .as_str()
            {
                "off" => SensitiveDataHandling::Off,
                "hash" => SensitiveDataHandling::Hash,
                _ => SensitiveDataHandling::Mask,
            },
            query_text_logging: match env::var("LOG_QUERY_TEXT")
                .unwrap_or_else(|_| "0".to_string())
                .to_lowercase()
                .as_str()
            {
                "1" | "true" | "full" => QueryTextLogging::Full,
                "hash" => QueryTextLogging::Hash,
                _ => QueryTextLogging::Off,
            },
            otel_service_name: env::var("OTEL_SERVICE_NAME").ok(),
            otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
        }
    }
}

/// Initialize logging and tracing
///
/// Sets up the global tracing subscriber with:
/// - EnvFilter for level filtering
/// - Optional JSON formatting for CloudWatch
/// - Optional OTEL tracing export
///
/// Safe to call multiple times - will only initialize once.
pub fn init_logging(config: &TelemetryConfig) {
    // Check if a global subscriber is already set (e.g., from tests)
    if tracing::dispatcher::has_been_set() {
        tracing::debug!("tracing subscriber already initialized, skipping");
        return;
    }

    let filter = if config.log_filter.is_empty() {
        EnvFilter::new(&config.default_level)
    } else {
        EnvFilter::new(&config.log_filter)
    };

    let fmt_layer = match config.log_format {
        // NOTE: `tracing-subscriber` JSON formatting requires enabling its `json` feature.
        // For now, keep the "json" option as a structured-ish compact format.
        LogFormat::Json => tracing_subscriber::fmt::layer().compact().boxed(),
        LogFormat::Human => tracing_subscriber::fmt::layer().compact().boxed(),
    };

    // Build otel layer as Option (None is a no-op Layer).
    // The OTEL layer gets its own target filter so that only fluree crate spans
    // are exported, regardless of what RUST_LOG enables.  This prevents debug
    // noise from hyper, h2, tokio, tower-http etc. from flooding the batch
    // exporter and causing parent spans to be dropped (which would orphan child
    // spans into separate root traces in Jaeger/Tempo).
    #[cfg(feature = "otel")]
    let otel_layer = if config.is_otel_enabled() {
        let otel_filter = tracing_subscriber::filter::Targets::new()
            .with_target("fluree_db_server", tracing::Level::DEBUG)
            .with_target("fluree_db_api", tracing::Level::DEBUG)
            .with_target("fluree_db_transact", tracing::Level::DEBUG)
            .with_target("fluree_db_query", tracing::Level::DEBUG)
            .with_target("fluree_db_indexer", tracing::Level::DEBUG)
            .with_target("fluree_db_sparql", tracing::Level::DEBUG)
            .with_target("fluree_db_core", tracing::Level::DEBUG)
            .with_target("fluree_db_novelty", tracing::Level::TRACE);
        Some(init_otel_layer(config).with_filter(otel_filter))
    } else {
        None
    };
    #[cfg(not(feature = "otel"))]
    let otel_layer: Option<tracing_subscriber::layer::Identity> = None;

    // Use try_init to avoid panicking if another thread set the subscriber
    // between our has_been_set() check and now (race condition in tests)
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init();
}

/// Global handle to the tracer provider for graceful shutdown.
#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Initialize OTEL tracing layer
///
/// Only call this if OTEL environment variables are set.
/// Returns a tracing layer that exports spans via OTLP.
///
/// The `SdkTracerProvider` is stored in a module-level `OnceLock` so
/// that `shutdown_tracer()` can flush remaining spans on exit.
#[cfg(feature = "otel")]
fn init_otel_layer<S>(config: &TelemetryConfig) -> impl Layer<S> + Send + Sync
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

    // Determine protocol (default: grpc)
    let protocol = env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_else(|_| "grpc".to_string())
        .to_lowercase();

    let endpoint = config.otel_endpoint.as_ref().unwrap().clone();

    // Configure OTLP exporter based on protocol
    let exporter = match protocol.as_str() {
        "http/protobuf" | "http" => SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to create HTTP OTLP span exporter"),
        _ => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to create gRPC OTLP span exporter"),
    };

    // Configure sampler based on environment
    let sampler = match env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_else(|_| "always_on".to_string())
        .to_lowercase()
        .as_str()
    {
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => {
            let ratio = env::var("OTEL_TRACES_SAMPLER_ARG")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse::<f64>()
                .unwrap_or(1.0);
            Sampler::TraceIdRatioBased(ratio)
        }
        "parentbased_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        _ => Sampler::AlwaysOn, // default
    };

    let service_name = config.otel_service_name.as_ref().unwrap().clone();

    // Build resource describing this service
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name)
        .with_attributes([KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))])
        .build();

    // Build tracer provider (batch export uses a background thread, no async runtime needed)
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    // Store provider for later shutdown
    let _ = TRACER_PROVIDER.set(tracer_provider.clone());

    let tracer = tracer_provider.tracer("fluree-db");

    tracing_opentelemetry::layer().with_tracer(tracer)
}

/// Shutdown telemetry gracefully
///
/// Call this before application exit to ensure all spans are exported.
/// This is a no-op if OTEL is not enabled or no provider was initialized.
pub async fn shutdown_tracer() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = TRACER_PROVIDER.get() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!("OTEL tracer provider shutdown error: {e}");
            }
        }
    }
    // No-op when OTEL feature is disabled
}

/// Extract request ID from headers
///
/// Checks multiple common header names in priority order:
/// 1. Configured header name
/// 2. x-amzn-trace-id (AWS Lambda)
/// 3. x-trace-id (generic)
///
/// Returns None if no request ID found.
pub fn extract_request_id(
    headers: &axum::http::HeaderMap,
    config: &TelemetryConfig,
) -> Option<String> {
    // Check configured header first
    if let Some(value) = headers.get(&config.request_id_header) {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    // Fallback to AWS trace ID
    if let Some(value) = headers.get("x-amzn-trace-id") {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    // Generic trace ID fallback
    if let Some(value) = headers.get("x-trace-id") {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    None
}

/// Extract trace ID from headers or generate one
///
/// Looks for OTEL trace ID headers, falls back to request ID.
/// Returns None if no trace context available.
pub fn extract_trace_id(headers: &axum::http::HeaderMap) -> Option<String> {
    // Check for OTEL traceparent header
    if let Some(traceparent) = headers.get("traceparent") {
        if let Ok(tp) = traceparent.to_str() {
            // Parse traceparent format: version-trace_id-span_id-flags
            if let Some(trace_id) = tp.split('-').nth(1) {
                return Some(trace_id.to_string());
            }
        }
    }

    // Check for x-trace-id
    if let Some(trace_id) = headers.get("x-trace-id") {
        if let Ok(id) = trace_id.to_str() {
            return Some(id.to_string());
        }
    }

    None
}

/// Create a request span with correlation context
///
/// This is the main entry point for creating spans at request boundaries.
/// Includes all correlation fields for CloudWatch filtering.
pub fn create_request_span(
    operation: &str,
    request_id: Option<&str>,
    trace_id: Option<&str>,
    ledger_alias: Option<&str>,
    tenant_id: Option<&str>,
) -> tracing::Span {
    tracing::info_span!(
        "request",
        operation = operation,
        request_id = request_id,
        trace_id = trace_id,
        ledger_alias = ledger_alias,
        tenant_id = tenant_id,
        error_code = tracing::field::Empty, // Will be set on error
    )
}

/// Helper to set error code on a span
pub fn set_span_error_code(span: &tracing::Span, error_code: &str) {
    span.record("error_code", error_code);
}

/// Utility to mask sensitive data in logs
///
/// Applies the configured sensitive data handling policy.
pub fn mask_sensitive_data(data: &str, handling: &SensitiveDataHandling) -> String {
    match handling {
        SensitiveDataHandling::Off => data.to_string(),
        SensitiveDataHandling::Mask => {
            // Simple masking: replace with asterisks but keep length
            "*".repeat(data.len().min(20))
        }
        SensitiveDataHandling::Hash => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        }
    }
}

/// Handle query text logging according to configuration
///
/// Returns (info_level_text, debug_level_text, trace_level_text) where:
/// - info_level_text: text to log at info level (or None)
/// - debug_level_text: text to log at debug level (or None)
/// - trace_level_text: text to log at trace level (or None)
pub fn handle_query_text_logging(
    query_text: &str,
    config: &TelemetryConfig,
) -> (Option<String>, Option<String>, Option<String>) {
    match config.query_text_logging {
        QueryTextLogging::Off => (None, None, None),
        QueryTextLogging::Full => (
            None,
            Some(query_text.to_string()),
            Some(query_text.to_string()),
        ),
        QueryTextLogging::Hash => {
            // Log fast non-crypto hash at info level, full text at trace
            use ahash::AHasher;
            use std::hash::Hasher;
            let mut hasher = AHasher::default();
            hasher.write(query_text.as_bytes());
            let hash = format!("ahash64:{:x}", hasher.finish());
            (Some(hash), None, Some(query_text.to_string()))
        }
    }
}

/// Check if query text logging is enabled
pub fn should_log_query_text(config: &TelemetryConfig) -> bool {
    !matches!(config.query_text_logging, QueryTextLogging::Off)
}

/// Log query text at appropriate levels based on configuration
pub fn log_query_text(query_text: &str, config: &TelemetryConfig, span: &tracing::Span) {
    let (info_text, debug_text, trace_text) = handle_query_text_logging(query_text, config);

    if let Some(hash) = info_text {
        span.record("query_hash", hash.as_str());
        tracing::info!(query_hash = %hash, "query logged");
    }

    if let Some(full_text) = debug_text {
        // Apply sensitive data masking if configured
        let logged_text = mask_sensitive_data(&full_text, &config.sensitive_data);
        tracing::debug!(query_text = %logged_text, "full query text");
    }

    // Only log at trace level if query text logging is enabled
    if let Some(raw_text) = trace_text {
        if tracing::enabled!(tracing::Level::TRACE) {
            let logged_text = mask_sensitive_data(&raw_text, &config.sensitive_data);
            tracing::trace!(query_text = %logged_text, "raw query text");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn test_extract_request_id() {
        let config = TelemetryConfig::default();
        let mut headers = HeaderMap::new();

        // Test configured header
        headers.insert("x-request-id", "test-123".parse().unwrap());
        assert_eq!(
            extract_request_id(&headers, &config),
            Some("test-123".to_string())
        );

        // Test AWS header fallback
        let mut headers = HeaderMap::new();
        headers.insert("x-amzn-trace-id", "aws-456".parse().unwrap());
        assert_eq!(
            extract_request_id(&headers, &config),
            Some("aws-456".to_string())
        );
    }

    #[test]
    fn test_extract_trace_id() {
        let mut headers = HeaderMap::new();

        // Test traceparent header
        headers.insert(
            "traceparent",
            "00-12345678901234567890123456789012-1234567890123456-01"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_trace_id(&headers),
            Some("12345678901234567890123456789012".to_string())
        );
    }

    #[test]
    fn test_mask_sensitive_data() {
        let data = "secret-password";

        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Off),
            "secret-password"
        );
        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Mask),
            "***************"
        );
        // Hash should be deterministic and different from input
        let hashed = mask_sensitive_data(data, &SensitiveDataHandling::Hash);
        assert_ne!(hashed, "secret-password");
        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Hash),
            hashed
        ); // deterministic
    }
}
