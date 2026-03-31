//! Cache configuration and memory-based sizing.
//!
//! The Fluree API uses a single global cache budget in MB. This module provides
//! the default sizing rule: 50% of available memory (native) with a conservative
//! fallback on platforms where memory detection is unavailable.
//!
//! In containerized environments (Docker, Kubernetes, ECS/Fargate), the available
//! memory is read from cgroup limits rather than host RAM, so the cache budget
//! correctly reflects the container's actual memory allocation.

use tracing::info;

/// Default cache size in MB when memory detection is unavailable (WASM/JS)
pub const DEFAULT_CACHE_MB_FALLBACK: usize = 1000;

/// Attempt to read the container's cgroup memory limit.
///
/// Tries cgroup v2 first (`/sys/fs/cgroup/memory.max`), then falls back to
/// cgroup v1 (`/sys/fs/cgroup/memory/memory.limit_in_bytes`). Returns `None`
/// if not running in a cgroup, if the limit is "max" (unlimited), or if the
/// files cannot be read.
#[cfg(target_os = "linux")]
fn detect_cgroup_memory_bytes() -> Option<u64> {
    // cgroup v2: single unified hierarchy
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max" {
            if let Ok(bytes) = trimmed.parse::<u64>() {
                info!(
                    cgroup_version = "v2",
                    memory_limit_bytes = bytes,
                    "Detected cgroup memory limit"
                );
                return Some(bytes);
            }
        }
    }

    // cgroup v1: memory controller
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        let trimmed = content.trim();
        if let Ok(bytes) = trimmed.parse::<u64>() {
            // cgroup v1 reports a very large number (close to i64::MAX) when unlimited
            if bytes < (1u64 << 62) {
                info!(
                    cgroup_version = "v1",
                    memory_limit_bytes = bytes,
                    "Detected cgroup memory limit"
                );
                return Some(bytes);
            }
        }
    }

    None
}

/// Non-Linux platforms never have cgroup limits.
#[cfg(not(target_os = "linux"))]
fn detect_cgroup_memory_bytes() -> Option<u64> {
    None
}

/// Calculate the default cache size in MB based on available memory.
///
/// Detection order:
/// 1. Cgroup limit (container-aware — Docker, K8s, ECS/Fargate)
/// 2. System total memory via `sysinfo` (bare-metal / VM)
/// 3. Conservative fallback (1000 MB)
///
/// Returns 50% of detected memory, with a minimum of 100 MB.
#[cfg(feature = "native")]
pub fn default_cache_max_mb() -> usize {
    // Try cgroup first — this is the correct value in containers
    if let Some(cgroup_bytes) = detect_cgroup_memory_bytes() {
        let total_mb = cgroup_bytes / (1024 * 1024);
        let cache_mb = (total_mb / 2) as usize;
        info!(
            total_mb,
            cache_mb,
            source = "cgroup",
            "Setting default cache from cgroup memory limit"
        );
        return cache_mb.max(100);
    }

    // Fall back to host memory via sysinfo
    use sysinfo::{MemoryRefreshKind, System};

    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());

    let total_memory_bytes = sys.total_memory();

    if total_memory_bytes == 0 {
        info!(
            cache_mb = DEFAULT_CACHE_MB_FALLBACK,
            source = "fallback",
            "Could not detect system memory, using fallback cache size"
        );
        return DEFAULT_CACHE_MB_FALLBACK;
    }

    let total_mb = total_memory_bytes / (1024 * 1024);
    let cache_mb = (total_mb / 2) as usize;

    info!(
        total_mb,
        cache_mb,
        source = "sysinfo",
        "Setting default cache from system memory"
    );

    cache_mb.max(100)
}

/// Calculate the default cache size in MB (WASM fallback).
#[cfg(not(feature = "native"))]
pub fn default_cache_max_mb() -> usize {
    DEFAULT_CACHE_MB_FALLBACK
}
