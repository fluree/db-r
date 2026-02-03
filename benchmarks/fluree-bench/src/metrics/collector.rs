//! Per-transaction timing and back-pressure event collection.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Records timing and throughput data during a benchmark run.
pub struct MetricsCollector {
    /// Wall-clock start of the benchmark.
    start: Instant,
    /// Per-transaction timings (elapsed duration for each commit).
    txn_timings: Vec<Duration>,
    /// Per-transaction flake counts.
    txn_flake_counts: Vec<usize>,
    /// Total entities processed.
    total_entities: usize,
    /// Back-pressure events observed.
    back_pressure_events: Vec<BackPressureEvent>,
    /// Per-query timings: query_name -> list of (cache_state, concurrency, durations).
    query_results: HashMap<String, Vec<QueryTiming>>,
}

/// A single back-pressure event (commit blocked due to novelty at max).
#[derive(Debug, Clone)]
pub struct BackPressureEvent {
    /// How long the commit was blocked before succeeding.
    pub duration: Duration,
    /// Number of retry attempts before success.
    pub retries: u32,
}

/// Timing result for a single query execution.
#[derive(Debug, Clone)]
#[allow(dead_code)] // row_count used in JSON report (Phase 5)
pub struct QueryTiming {
    pub cache_state: CacheState,
    pub concurrency: usize,
    pub duration: Duration,
    pub row_count: Option<usize>,
    pub verified: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    Cold,
    Warm,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            txn_timings: Vec::new(),
            txn_flake_counts: Vec::new(),
            total_entities: 0,
            back_pressure_events: Vec::new(),
            query_results: HashMap::new(),
        }
    }

    /// Record a completed transaction.
    pub fn record_txn(&mut self, elapsed: Duration, flake_count: usize, entity_count: usize) {
        self.txn_timings.push(elapsed);
        self.txn_flake_counts.push(flake_count);
        self.total_entities += entity_count;
    }

    /// Record a back-pressure event.
    pub fn record_back_pressure(&mut self, event: BackPressureEvent) {
        self.back_pressure_events.push(event);
    }

    /// Record a query timing result.
    pub fn record_query(&mut self, name: &str, timing: QueryTiming) {
        self.query_results
            .entry(name.to_string())
            .or_default()
            .push(timing);
    }

    // --- Accessors for reporting ---

    pub fn total_elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn txn_count(&self) -> usize {
        self.txn_timings.len()
    }

    pub fn total_flakes(&self) -> usize {
        self.txn_flake_counts.iter().sum()
    }

    pub fn total_entities(&self) -> usize {
        self.total_entities
    }

    pub fn back_pressure_events(&self) -> &[BackPressureEvent] {
        &self.back_pressure_events
    }

    pub fn query_results(&self) -> &HashMap<String, Vec<QueryTiming>> {
        &self.query_results
    }

    /// Sorted transaction timings for percentile calculations.
    pub fn sorted_txn_timings(&self) -> Vec<Duration> {
        let mut sorted = self.txn_timings.clone();
        sorted.sort();
        sorted
    }
}

/// Calculate a percentile from a sorted slice.
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p / 100.0).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_basic() {
        let data: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        // p50 of 1..=100 rounds to index 50 -> value 51ms
        let p50 = percentile(&data, 50.0);
        assert!(
            p50 >= Duration::from_millis(49) && p50 <= Duration::from_millis(52),
            "p50 should be near 50ms, got {p50:?}"
        );
        let p95 = percentile(&data, 95.0);
        assert!(
            p95 >= Duration::from_millis(94) && p95 <= Duration::from_millis(96),
            "p95 should be near 95ms, got {p95:?}"
        );
    }

    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile(&[], 50.0), Duration::ZERO);
    }

    #[test]
    fn test_collector_record_txn() {
        let mut c = MetricsCollector::new();
        c.record_txn(Duration::from_millis(100), 500, 50);
        c.record_txn(Duration::from_millis(200), 600, 60);
        assert_eq!(c.txn_count(), 2);
        assert_eq!(c.total_flakes(), 1100);
        assert_eq!(c.total_entities(), 110);
    }
}
