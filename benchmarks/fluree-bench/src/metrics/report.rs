//! Report formatters (text and JSON).

use std::time::Duration;

use crate::metrics::collector::{percentile, CacheState, MetricsCollector};
use crate::{CommonArgs, IngestArgs, QueryArgs};

/// Print the ingest benchmark report.
pub fn print_ingest(common: &CommonArgs, args: &IngestArgs, collector: &MetricsCollector) {
    let elapsed = collector.total_elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let sorted = collector.sorted_txn_timings();

    let units = crate::datagen::compute_units(common.data_size_mb);
    let entities_per_unit = crate::datagen::supply_chain::ENTITIES_PER_UNIT;

    eprintln!();
    eprintln!("=== Fluree Bench: Ingest ===");
    eprintln!();
    eprintln!("Config:");
    eprintln!(
        "  Data size:        {} MB (~{} units, ~{} entities)",
        common.data_size_mb,
        units,
        units * entities_per_unit
    );
    eprintln!("  Txn type:         {}", args.txn_type);
    eprintln!("  Batch size:       {} entities", args.batch_size);
    eprintln!("  Concurrency:      {} in-flight", args.concurrency);
    eprintln!(
        "  Storage:          {}",
        if common.storage.is_some() {
            "file"
        } else {
            "memory"
        }
    );
    eprintln!(
        "  Indexing:         {}",
        if common.no_indexing {
            "disabled".to_string()
        } else {
            format!(
                "enabled (min={}B, max={}B)",
                common.reindex_min_bytes, common.reindex_max_bytes
            )
        }
    );

    eprintln!();
    eprintln!("Transactions:");
    eprintln!("  Total:            {}", collector.txn_count());
    eprintln!("  Total flakes:     {}", collector.total_flakes());
    eprintln!("  Elapsed:          {:.2}s", elapsed_secs);

    if !sorted.is_empty() {
        eprintln!();
        eprintln!("Single-txn latency:");
        eprintln!(
            "  Min:    {:>10}    P95:  {}",
            format_duration(percentile(&sorted, 0.0)),
            format_duration(percentile(&sorted, 95.0))
        );
        eprintln!(
            "  Max:    {:>10}    P99:  {}",
            format_duration(percentile(&sorted, 100.0)),
            format_duration(percentile(&sorted, 99.0))
        );
        eprintln!("  Mean:   {:>10}", format_duration(mean_duration(&sorted)));
        eprintln!(
            "  Median: {:>10}",
            format_duration(percentile(&sorted, 50.0))
        );
    }

    if elapsed_secs > 0.0 {
        eprintln!();
        eprintln!("Throughput:");
        eprintln!(
            "  Flakes/sec:       {:.0}",
            collector.total_flakes() as f64 / elapsed_secs
        );
        eprintln!(
            "  Txns/sec:         {:.2}",
            collector.txn_count() as f64 / elapsed_secs
        );
        eprintln!(
            "  Entities/sec:     {:.0}",
            collector.total_entities() as f64 / elapsed_secs
        );
    }

    let bp = collector.back_pressure_events();
    eprintln!();
    eprintln!("Back-Pressure:");
    if bp.is_empty() {
        eprintln!("  None observed");
    } else {
        let total_blocked: Duration = bp.iter().map(|e| e.duration).sum();
        let max_blocked = bp.iter().map(|e| e.duration).max().unwrap_or_default();
        eprintln!("  Events:           {}", bp.len());
        eprintln!("  Total blocked:    {}", format_duration(total_blocked));
        eprintln!(
            "  Avg duration:     {}",
            format_duration(total_blocked / bp.len() as u32)
        );
        eprintln!("  Max duration:     {}", format_duration(max_blocked));
    }
    eprintln!();
}

/// Print the query benchmark report.
pub fn print_query(_common: &CommonArgs, _args: &QueryArgs, collector: &MetricsCollector) {
    let results = collector.query_results();
    if results.is_empty() {
        eprintln!("No query results.");
        return;
    }

    eprintln!();
    eprintln!("=== Fluree Bench: Query Matrix ===");
    eprintln!();

    // Collect unique concurrency levels and sort them.
    let mut concurrency_levels: Vec<usize> =
        results.values().flatten().map(|t| t.concurrency).collect();
    concurrency_levels.sort();
    concurrency_levels.dedup();

    // Header
    eprint!("{:<24}", "Query");
    for &c in &concurrency_levels {
        eprint!("  Cold/{c}x  Warm/{c}x");
    }
    eprintln!("  Status");

    // Rows
    let mut query_names: Vec<&String> = results.keys().collect();
    query_names.sort();

    for name in query_names {
        let timings = &results[name];
        eprint!("{:<24}", name);

        for &c in &concurrency_levels {
            for &state in &[CacheState::Cold, CacheState::Warm] {
                let matching: Vec<Duration> = timings
                    .iter()
                    .filter(|t| t.cache_state == state && t.concurrency == c)
                    .map(|t| t.duration)
                    .collect();

                if matching.is_empty() {
                    eprint!("  {:>7}", "-");
                } else {
                    let mut sorted = matching;
                    sorted.sort();
                    eprint!("  {:>7}", format_duration(percentile(&sorted, 50.0)));
                }
            }
        }

        // Verification status
        let all_verified = timings.iter().all(|t| t.verified.unwrap_or(true));
        eprintln!("  {}", if all_verified { "[OK]" } else { "[FAIL]" });
    }
    eprintln!();
}

fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.1}us", d.as_secs_f64() * 1_000_000.0)
    } else if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

fn mean_duration(sorted: &[Duration]) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let sum: Duration = sorted.iter().sum();
    sum / sorted.len() as u32
}
