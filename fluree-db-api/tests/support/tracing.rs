//! Tracing test utilities for span capture and verification.
//!
//! Provides `SpanCaptureLayer`, `SpanStore`, and initialization helpers
//! for verifying that expected spans are emitted during query and transaction
//! execution. Uses `tracing::subscriber::set_default()` for test isolation
//! (each test gets its own subscriber via the returned `DefaultGuard`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

// =============================================================================
// CapturedSpan — one recorded span creation event
// =============================================================================

#[derive(Debug, Clone)]
pub struct CapturedSpan {
    pub name: &'static str,
    pub level: tracing::Level,
    pub fields: HashMap<String, String>,
    pub parent_name: Option<String>,
}

// =============================================================================
// SpanStore — thread-safe accumulator of captured spans
// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct SpanStore(Arc<Mutex<Vec<CapturedSpan>>>);

impl SpanStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if any span with the given name was captured.
    pub fn has_span(&self, name: &str) -> bool {
        self.0.lock().unwrap().iter().any(|s| s.name == name)
    }

    /// Returns the first span with the given name, if any.
    pub fn find_span(&self, name: &str) -> Option<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.name == name)
            .cloned()
    }

    /// Returns all spans with the given name.
    pub fn find_spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.name == name)
            .cloned()
            .collect()
    }

    /// Returns all captured span names in order of creation.
    pub fn span_names(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().iter().map(|s| s.name).collect()
    }

    /// Returns all debug-level spans captured.
    pub fn debug_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.level == tracing::Level::DEBUG)
            .cloned()
            .collect()
    }

    /// Returns all trace-level spans captured.
    pub fn trace_spans(&self) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.level == tracing::Level::TRACE)
            .cloned()
            .collect()
    }
}

// =============================================================================
// SpanCaptureLayer — records span creation events into a SpanStore
// =============================================================================

pub struct SpanCaptureLayer {
    store: SpanStore,
}

impl SpanCaptureLayer {
    pub fn new(store: SpanStore) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for SpanCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut fields = FieldVisitor(HashMap::new());
        attrs.record(&mut fields);

        let parent_name = attrs
            .parent()
            .and_then(|pid| ctx.span(pid))
            .map(|span| span.name().to_string())
            .or_else(|| ctx.lookup_current().map(|span| span.name().to_string()));

        let span_ref = ctx.span(id).expect("span should exist");
        let meta = span_ref.metadata();

        self.store.0.lock().unwrap().push(CapturedSpan {
            name: meta.name(),
            level: *meta.level(),
            fields: fields.0,
            parent_name,
        });
    }
}

// =============================================================================
// FieldVisitor — extracts typed span fields into a HashMap<String, String>
// =============================================================================

struct FieldVisitor(HashMap<String, String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{:?}", value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

// =============================================================================
// Initialization helpers
// =============================================================================

/// Full-detail: captures all spans regardless of level.
///
/// Returns a `SpanStore` for assertions and a `DefaultGuard` that must be
/// held alive for the duration of the test. The subscriber is active only
/// while the guard is alive, providing test isolation.
pub fn init_test_tracing() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::new();
    let layer = SpanCaptureLayer::new(store.clone());
    let subscriber = tracing_subscriber::registry::Registry::default().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (store, guard)
}

/// Info-only: captures only INFO+ spans, for zero-noise verification (AC-5).
///
/// Debug and trace spans should NOT appear in the store when using this.
pub fn init_info_only_tracing() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::new();
    let layer = SpanCaptureLayer::new(store.clone());
    let filter = tracing_subscriber::filter::LevelFilter::INFO;
    let subscriber =
        tracing_subscriber::registry::Registry::default().with(layer.with_filter(filter));
    let guard = tracing::subscriber::set_default(subscriber);
    (store, guard)
}
