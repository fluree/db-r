//! Temporary memory profiling utilities for rebuild pipeline investigation.
//!
//! This module is throwaway instrumentation — it will be removed once we've
//! validated our memory hypotheses.

use std::collections::{HashMap, HashSet};

/// Get the current process RSS (Resident Set Size) in bytes.
///
/// Works on both macOS (mach_task_basic_info) and Linux (/proc/self/status).
pub fn current_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        macos_rss()
    }
    #[cfg(target_os = "linux")]
    {
        linux_rss()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

#[cfg(target_os = "macos")]
fn macos_rss() -> u64 {
    use std::mem;
    // mach_task_basic_info
    const MACH_TASK_BASIC_INFO: u32 = 20;
    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [u32; 2],
        system_time: [u32; 2],
        policy: i32,
        suspend_count: i32,
    }
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: u32,
            task_info_out: *mut MachTaskBasicInfo,
            task_info_out_cnt: *mut u32,
        ) -> i32;
    }
    unsafe {
        let mut info: MachTaskBasicInfo = mem::zeroed();
        let mut count = (mem::size_of::<MachTaskBasicInfo>() / mem::size_of::<u32>()) as u32;
        let kr = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info,
            &mut count,
        );
        if kr == 0 {
            info.resident_size
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_rss() -> u64 {
    // Read from /proc/self/status — VmRSS line is in kB
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(kb_str) = parts.get(1) {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        return kb * 1024; // convert kB to bytes
                    }
                }
            }
        }
    }
    0
}

/// Format bytes as human-readable MB string.
pub fn mb(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

/// Format bytes as human-readable (auto-selects MB or GB).
pub fn human(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Estimate the shallow heap size of a Vec<T> (capacity-based).
pub fn vec_heap_bytes<T>(v: &[T]) -> u64 {
    // Vec allocates capacity * size_of::<T>()
    // But we only have a slice reference, so use len
    std::mem::size_of_val(v) as u64
}

/// Estimate heap size of a HashMap<K, V> (shallow — doesn't recurse into values).
pub fn hashmap_shallow_bytes<K, V>(m: &HashMap<K, V>) -> u64 {
    // HashMap bucket array: capacity * (sizeof(K) + sizeof(V) + 8 bytes overhead)
    let entry_size = std::mem::size_of::<K>() + std::mem::size_of::<V>() + 8;
    (m.capacity() * entry_size) as u64
}

/// Estimate heap size of a HashSet<T>.
pub fn hashset_shallow_bytes<T>(s: &HashSet<T>) -> u64 {
    let entry_size = std::mem::size_of::<T>() + 8;
    (s.capacity() * entry_size) as u64
}

/// Deep size estimate for subject_class_deltas: HashMap<(u16, u64), HashMap<u64, i64>>
pub fn deep_size_subject_class_deltas(m: &HashMap<(u16, u64), HashMap<u64, i64>>) -> u64 {
    let mut total = hashmap_shallow_bytes(m);
    for inner in m.values() {
        total += hashmap_shallow_bytes(inner);
    }
    total
}

/// Deep size estimate for subject_props: HashMap<(u16, u64), HashSet<u32>>
pub fn deep_size_subject_props(m: &HashMap<(u16, u64), HashSet<u32>>) -> u64 {
    let mut total = hashmap_shallow_bytes(m);
    for inner in m.values() {
        total += hashset_shallow_bytes(inner);
    }
    total
}

/// Deep size estimate for subject_prop_dts: HashMap<(u16, u64), HashMap<u32, HashMap<u8, i64>>>
pub fn deep_size_3level_u8(m: &HashMap<(u16, u64), HashMap<u32, HashMap<u8, i64>>>) -> u64 {
    let mut total = hashmap_shallow_bytes(m);
    for l1 in m.values() {
        total += hashmap_shallow_bytes(l1);
        for l2 in l1.values() {
            total += hashmap_shallow_bytes(l2);
        }
    }
    total
}

/// Deep size estimate for subject_prop_langs: HashMap<(u16, u64), HashMap<u32, HashMap<u16, i64>>>
pub fn deep_size_3level_u16(m: &HashMap<(u16, u64), HashMap<u32, HashMap<u16, i64>>>) -> u64 {
    let mut total = hashmap_shallow_bytes(m);
    for l1 in m.values() {
        total += hashmap_shallow_bytes(l1);
        for l2 in l1.values() {
            total += hashmap_shallow_bytes(l2);
        }
    }
    total
}

/// Deep size estimate for subject_ref_history: HashMap<(u16, u64), HashMap<u32, HashMap<u64, i64>>>
pub fn deep_size_ref_history(m: &HashMap<(u16, u64), HashMap<u32, HashMap<u64, i64>>>) -> u64 {
    let mut total = hashmap_shallow_bytes(m);
    for l1 in m.values() {
        total += hashmap_shallow_bytes(l1);
        for l2 in l1.values() {
            total += hashmap_shallow_bytes(l2);
        }
    }
    total
}

/// Log current RSS with a label.
pub fn log_rss(label: &str) {
    let rss = current_rss_bytes();
    tracing::info!(
        rss_bytes = rss,
        rss_human = %human(rss),
        label,
        "🔍 MEM_PROFILE: RSS checkpoint"
    );
}
