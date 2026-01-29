//! Cache configuration and memory-based sizing
//!
//! This module provides utilities for calculating optimal cache sizes based on
//! available system memory, matching the behavior of the Clojure implementation.

use tracing::info;

/// Default overflow bytes for index segments (375KB)
/// This matches `const/default-overflow-bytes` in Clojure.
pub const DEFAULT_OVERFLOW_BYTES: u64 = 375 * 1024;

/// Minimum cache entries required for operation
pub const MIN_CACHE_ENTRIES: usize = 100;

/// Default cache size in MB when memory detection is unavailable (WASM/JS)
pub const DEFAULT_CACHE_MB_FALLBACK: usize = 1000;

/// Calculate the default cache size in MB based on available system memory.
///
/// This matches the Clojure `default-cache-max-mb` function:
/// - On native: Uses 50% of available system memory
/// - On WASM: Returns a conservative 1000 MB default
///
/// Returns the cache size in megabytes.
#[cfg(feature = "native")]
pub fn default_cache_max_mb() -> usize {
    use sysinfo::{MemoryRefreshKind, System};

    // Create system with memory refresh enabled
    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());

    let total_memory_bytes = sys.total_memory();

    // If sysinfo returns 0 (sandbox/permission issues), fall back to conservative default
    if total_memory_bytes == 0 {
        info!(
            "Could not detect system memory, using fallback cache size of {}MB",
            DEFAULT_CACHE_MB_FALLBACK
        );
        return DEFAULT_CACHE_MB_FALLBACK;
    }

    let total_mb = total_memory_bytes / (1024 * 1024);
    let cache_mb = (total_mb / 2) as usize; // 50% of memory for cache

    info!(
        "Detected {}MB total memory, setting default cache to {}MB",
        total_mb, cache_mb
    );

    cache_mb.max(100) // Minimum 100MB
}

/// Calculate the default cache size in MB (WASM fallback).
#[cfg(not(feature = "native"))]
pub fn default_cache_max_mb() -> usize {
    DEFAULT_CACHE_MB_FALLBACK
}

/// Convert memory allocation (MB) to cache entry count.
///
/// Index segments are rebalanced when they exceed overflow_bytes (default 375KB),
/// so the average size is approximately 50% of overflow_bytes (~183KB in-memory).
///
/// This matches the Clojure `memory->cache-size` function:
/// - 1GB cache holds ~5,592 segments
/// - 10GB cache holds ~55,924 segments
/// - 20GB cache holds ~111,848 segments
///
/// # Arguments
/// * `cache_max_mb` - Maximum memory in MB to use for cache
/// * `overflow_bytes` - Override for index leaf overflow threshold (optional)
///
/// # Returns
/// The number of cache entries that fit in the given memory budget.
///
/// # Panics
/// Panics if the calculated cache size is below MIN_CACHE_ENTRIES (100).
pub fn memory_to_cache_size(cache_max_mb: usize, overflow_bytes: Option<u64>) -> usize {
    let overflow = overflow_bytes.unwrap_or(DEFAULT_OVERFLOW_BYTES);
    let overflow_mb = overflow as f64 / (1024.0 * 1024.0);

    // Average in-memory size per segment is ~50% of overflow
    let avg_segment_mb = overflow_mb * 0.5;

    let cache_size = (cache_max_mb as f64 / avg_segment_mb) as usize;

    if cache_size < MIN_CACHE_ENTRIES {
        panic!(
            "Must allocate at least {}MB of memory for Fluree cache. You've allocated: {}MB",
            (MIN_CACHE_ENTRIES as f64 * avg_segment_mb).ceil() as usize,
            cache_max_mb
        );
    }

    info!(
        "Initialized LRU cache: {}MB capacity, holding up to {} index segments",
        cache_max_mb, cache_size
    );

    cache_size
}

/// Calculate the default cache entry count based on system memory.
///
/// This combines `default_cache_max_mb()` and `memory_to_cache_size()` for
/// a single-call default calculation.
pub fn default_cache_entries() -> usize {
    let mb = default_cache_max_mb();
    memory_to_cache_size(mb, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_to_cache_size() {
        // With default overflow (375KB), avg segment is ~183KB
        // 1GB = 1024MB should give ~5592 entries
        let size_1gb = memory_to_cache_size(1024, None);
        assert!(size_1gb > 5000 && size_1gb < 6000, "1GB should give ~5592 entries, got {}", size_1gb);

        // 10GB should give ~55924 entries
        let size_10gb = memory_to_cache_size(10 * 1024, None);
        assert!(size_10gb > 50000 && size_10gb < 60000, "10GB should give ~55924 entries, got {}", size_10gb);
    }

    #[test]
    #[should_panic(expected = "Must allocate at least")]
    fn test_memory_to_cache_size_too_small() {
        // 1MB is way too small
        memory_to_cache_size(1, None);
    }

    #[test]
    fn test_memory_to_cache_size_custom_overflow() {
        // With smaller overflow bytes, we should get more entries
        let small_overflow = 100 * 1024; // 100KB
        let size = memory_to_cache_size(1024, Some(small_overflow));
        // With 100KB overflow, avg is 50KB, so 1GB = 1024MB / 0.05MB = ~20480 entries
        assert!(size > 15000, "Smaller overflow should give more entries, got {}", size);
    }
}
