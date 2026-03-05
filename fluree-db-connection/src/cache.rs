//! Cache configuration and memory-based sizing.
//!
//! The Fluree API uses a single global cache budget in MB. This module provides
//! the default sizing rule: 50% of system RAM (native) with a conservative
//! fallback on platforms where memory detection is unavailable.

use tracing::info;

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
