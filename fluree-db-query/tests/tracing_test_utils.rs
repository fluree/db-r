//! Test utilities for verifying tracing span emission.
//!
//! Provides a `SpanCapture` layer that records span names and fields during
//! tests, allowing assertions on the span tree produced by instrumented code.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// A recorded span with its name, level, and fields.
#[derive(Debug, Clone)]
pub struct CapturedSpan {
    pub name: &'static str,
    pub level: tracing::Level,
    pub fields: HashMap<String, String>,
    pub parent_name: Option<String>,
}

/// Shared storage for captured spans.
#[derive(Debug, Clone, Default)]
pub struct SpanStore(Arc<Mutex<Vec<CapturedSpan>>>);

impl SpanStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all captured spans.
    pub fn spans(&self) -> Vec<CapturedSpan> {
        self.0.lock().unwrap().clone()
    }

    /// Returns span names in order of creation.
    pub fn span_names(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().iter().map(|s| s.name).collect()
    }

    /// Returns true if a span with the given name was captured.
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
}

/// A tracing `Layer` that captures span creation events into a `SpanStore`.
pub struct SpanCaptureLayer {
    store: SpanStore,
}

impl SpanCaptureLayer {
    pub fn new(store: SpanStore) -> Self {
        Self { store }
    }
}

/// Visitor that records span fields as string key-value pairs.
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

impl<S> Layer<S> for SpanCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = FieldVisitor(HashMap::new());
        attrs.record(&mut fields);

        let parent_name = attrs
            .parent()
            .and_then(|pid| ctx.span(pid))
            .map(|span| span.name().to_string())
            .or_else(|| ctx.lookup_current().map(|span| span.name().to_string()));

        let span_ref = ctx.span(id).expect("span should exist");
        let meta = span_ref.metadata();

        let captured = CapturedSpan {
            name: meta.name(),
            level: *meta.level(),
            fields: fields.0,
            parent_name,
        };

        self.store.0.lock().unwrap().push(captured);
    }
}

/// Initialize a tracing subscriber for tests that captures spans into the
/// returned `SpanStore`. Call this at the beginning of a test function.
///
/// Returns a guard that must be held for the duration of the test (dropping
/// it unsets the subscriber) and the `SpanStore` for assertions.
///
/// # Example
///
/// ```ignore
/// let (store, _guard) = init_test_tracing();
/// // ... run instrumented code ...
/// assert!(store.has_span("parse"));
/// ```
pub fn init_test_tracing() -> (SpanStore, tracing::subscriber::DefaultGuard) {
    let store = SpanStore::new();
    let layer = SpanCaptureLayer::new(store.clone());

    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);

    (store, guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_capture_basic() {
        let (store, _guard) = init_test_tracing();

        let span = tracing::debug_span!("test_span", field1 = 42u64, field2 = "hello");
        let _enter = span.enter();

        let spans = store.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "test_span");
        assert_eq!(spans[0].level, tracing::Level::DEBUG);
        assert_eq!(spans[0].fields.get("field1").unwrap(), "42");
        assert_eq!(spans[0].fields.get("field2").unwrap(), "hello");
    }

    #[test]
    fn test_span_capture_hierarchy() {
        let (store, _guard) = init_test_tracing();

        let parent = tracing::info_span!("parent_span");
        let _parent_guard = parent.enter();

        let child = tracing::debug_span!("child_span", count = 10u64);
        let _child_guard = child.enter();

        assert!(store.has_span("parent_span"));
        assert!(store.has_span("child_span"));

        let child_captured = store.find_span("child_span").unwrap();
        assert_eq!(child_captured.parent_name.as_deref(), Some("parent_span"));
    }

    #[test]
    fn test_span_names() {
        let (store, _guard) = init_test_tracing();

        let _s1 = tracing::info_span!("first").entered();
        let _s2 = tracing::debug_span!("second").entered();

        let names = store.span_names();
        assert_eq!(names, vec!["first", "second"]);
    }

    #[test]
    fn test_find_spans_multiple() {
        let (store, _guard) = init_test_tracing();

        {
            let _s = tracing::trace_span!("scan", index = "spo").entered();
        }
        {
            let _s = tracing::trace_span!("scan", index = "pos").entered();
        }

        let scans = store.find_spans("scan");
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].fields.get("index").unwrap(), "spo");
        assert_eq!(scans[1].fields.get("index").unwrap(), "pos");
    }
}
