//! VecBiDict migration benchmark with embedded baselines for comparison.
//!
//! Measures insert, forward lookup, reverse lookup, and memory footprint
//! for each dictionary at realistic low and high scales. Shows side-by-side
//! comparison against static baseline values captured before migration
//! (commit 38cb4af, stored in initial-bench.log).
//!
//! Dictionaries benchmarked (matching new-vec-bidict-plan.md):
//!   1. StringDictNovelty           — fluree-db-core        (Step 3)  [MIGRATED]
//!   2. SubjectDictNovelty          — fluree-db-core        (Step 4)  [MIGRATED]
//!   3. DictOverlay::ext_predicates — ephemeral pattern     (Step 5)  [MIGRATED]
//!   4. DictOverlay::ext_graphs     — ephemeral pattern     (Step 5)  [MIGRATED]
//!   5. DictOverlay::ext_lang_tags  — ephemeral pattern     (Step 5)  [MIGRATED]
//!   6. DictOverlay::ext_subjects   — ephemeral pattern     (Step 5)  [MIGRATED]
//!   7. DictOverlay::ext_strings    — ephemeral pattern     (Step 5)  [MIGRATED]
//!   8. PredicateDict               — fluree-db-indexer     (Step 6)  [MIGRATED]
//!   9. LanguageTagDict             — fluree-db-indexer     (Step 6)  [MIGRATED]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use fluree_db_core::vec_bi_dict::VecBiDict;
use fluree_db_core::DictNovelty;
use fluree_db_indexer::run_index::global_dict::{LanguageTagDict, PredicateDict};

// ============================================================================
// Tracking allocator
// ============================================================================

struct TrackingAllocator;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let prev = ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            let current = prev + layout.size();
            let mut peak = PEAK.load(Ordering::Relaxed);
            while current > peak {
                match PEAK.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}

fn current_allocated() -> usize {
    ALLOCATED.load(Ordering::Relaxed)
}

fn reset_peak() {
    PEAK.store(ALLOCATED.load(Ordering::Relaxed), Ordering::Relaxed);
}

// ============================================================================
// Data generation
// ============================================================================

/// Generate realistic IRI-like strings (mix of predicates and entities).
fn generate_iris(count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let namespaces = [
        "http://www.w3.org/2001/XMLSchema#",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "http://www.w3.org/2000/01/rdf-schema#",
        "http://xmlns.com/foaf/0.1/",
        "http://schema.org/",
        "http://dbpedia.org/resource/",
        "http://example.org/people/",
        "http://example.org/organizations/",
        "http://example.org/products/",
        "http://example.org/events/",
        "https://ns.flur.ee/ledger#",
        "http://purl.org/dc/terms/",
    ];
    let local_parts = [
        "name",
        "label",
        "type",
        "description",
        "value",
        "created",
        "modified",
        "author",
        "title",
        "status",
        "category",
        "identifier",
        "version",
    ];

    (0..count)
        .map(|i| {
            let ns = namespaces[rng.gen_range(0..namespaces.len())];
            if rng.gen_bool(0.3) {
                let local = local_parts[rng.gen_range(0..local_parts.len())];
                format!("{ns}{local}")
            } else {
                format!("{ns}entity_{i}_{}", rng.gen_range(0u32..100_000))
            }
        })
        .collect()
}

/// Generate (ns_code, suffix) pairs with realistic namespace distribution.
/// 80% core namespaces (0-9), 15% medium (10-99), 5% overflow (0xFFFF).
fn generate_subject_data(count: usize, seed: u64) -> Vec<(u16, String)> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            let r: f64 = rng.gen();
            let ns_code: u16 = if r < 0.80 {
                rng.gen_range(0..10)
            } else if r < 0.95 {
                rng.gen_range(10..100)
            } else {
                0xFFFF
            };
            let suffix = format!("entity_{i}_{}", rng.gen_range(0u32..100_000));
            (ns_code, suffix)
        })
        .collect()
}

/// Generate predicate-like IRIs (higher reuse ratio — ~140 unique predicates).
fn generate_predicate_iris(count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let namespaces = [
        "http://www.w3.org/2001/XMLSchema#",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "http://www.w3.org/2000/01/rdf-schema#",
        "http://xmlns.com/foaf/0.1/",
        "http://schema.org/",
        "https://ns.flur.ee/ledger#",
        "http://purl.org/dc/terms/",
    ];
    let local_parts = [
        "name",
        "label",
        "type",
        "description",
        "value",
        "created",
        "modified",
        "author",
        "title",
        "status",
        "category",
        "identifier",
        "version",
        "email",
        "phone",
        "address",
        "age",
        "birthDate",
        "gender",
        "nationality",
    ];

    (0..count)
        .map(|i| {
            let ns = namespaces[rng.gen_range(0..namespaces.len())];
            if rng.gen_bool(0.7) {
                let local = local_parts[rng.gen_range(0..local_parts.len())];
                format!("{ns}{local}")
            } else {
                format!("{ns}prop_{i}_{}", rng.gen_range(0u32..10_000))
            }
        })
        .collect()
}

/// Generate graph IRIs (very few unique — typically < 10 graphs per ledger).
fn generate_graph_iris(count: usize, seed: u64) -> Vec<String> {
    let graphs = [
        "http://example.org/graph/main",
        "http://example.org/graph/audit",
        "http://example.org/graph/policy",
        "http://example.org/graph/system",
        "http://example.org/graph/public",
        "http://example.org/graph/private",
        "https://ns.flur.ee/ledger#default",
        "https://ns.flur.ee/ledger#commit",
    ];
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            if rng.gen_bool(0.9) {
                graphs[rng.gen_range(0..graphs.len())].to_string()
            } else {
                format!("http://example.org/graph/custom_{i}")
            }
        })
        .collect()
}

/// Generate realistic language tags (~30 unique, massive reuse).
fn generate_language_tags(count: usize, seed: u64) -> Vec<String> {
    let tags = [
        "en", "fr", "de", "es", "pt", "zh", "ja", "ko", "ar", "hi", "ru", "it", "nl", "sv", "no",
        "da", "fi", "pl", "cs", "el", "en-US", "en-GB", "pt-BR", "zh-CN", "zh-TW", "es-MX",
        "fr-CA", "de-AT", "de-CH", "nl-BE",
    ];
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| tags[rng.gen_range(0..tags.len())].to_string())
        .collect()
}

/// Generate mixed string values (labels, descriptions, numeric strings).
fn generate_string_values(count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let short_values = [
        "Alice",
        "Bob",
        "Charlie",
        "active",
        "pending",
        "completed",
        "true",
        "false",
        "yes",
        "no",
    ];

    (0..count)
        .map(|i| {
            let r: f64 = rng.gen();
            if r < 0.3 {
                short_values[rng.gen_range(0..short_values.len())].to_string()
            } else if r < 0.6 {
                format!("value_{i}_{}", rng.gen_range(0u32..100_000))
            } else {
                format!(
                    "Description for entity {} with suffix {}",
                    i,
                    rng.gen_range(0u32..100_000)
                )
            }
        })
        .collect()
}

/// Build string lookup data: 50% known values (hits), 50% novel (misses).
fn build_string_lookups(source: &[String], count: usize, seed: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            if rng.gen_bool(0.5) && !source.is_empty() {
                source[rng.gen_range(0..source.len())].clone()
            } else {
                format!(
                    "http://example.org/miss/{}",
                    rng.gen_range(0u64..10_000_000)
                )
            }
        })
        .collect()
}

/// Build u32 lookup IDs: uniformly distributed in [0, max_id).
fn build_u32_lookup_ids(count: usize, max_id: u32, seed: u64) -> Vec<u32> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| rng.gen_range(0..max_id.max(1)))
        .collect()
}

/// Build u16 lookup IDs: uniformly distributed in [0, max_id].
fn build_u16_lookup_ids(count: usize, max_id: u16, seed: u64) -> Vec<u16> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| rng.gen_range(0..=max_id.max(1)))
        .collect()
}

// ============================================================================
// Benchmark result
// ============================================================================

struct BenchResult {
    label: &'static str,
    scale: usize,
    lookup_count: usize,
    insert_ms: f64,
    fwd_lookup_ms: f64,
    rev_lookup_ms: f64,
    mem_bytes: usize,
    unique_count: usize,
}

// ============================================================================
// Static baselines — captured at commit 38cb4af (pre-VecBiDict migration)
//
// Values are from initial-bench.log (detail sections, 2-decimal precision).
// These never change; running the benchmark shows current vs these baselines.
// ============================================================================

struct Baseline {
    label: &'static str,
    scale: usize,
    insert_ms: f64,
    fwd_lookup_ms: f64,
    rev_lookup_ms: f64,
    mem_mb: f64,
    bytes_per_entry: usize,
}

static BASELINES: &[Baseline] = &[
    // Step 3: StringDictNovelty
    Baseline {
        label: "StringDictNovelty",
        scale: 500_000,
        insert_ms: 64.20,
        fwd_lookup_ms: 10.51,
        rev_lookup_ms: 29.41,
        mem_mb: 67.20,
        bytes_per_entry: 201,
    },
    Baseline {
        label: "StringDictNovelty",
        scale: 25_000_000,
        insert_ms: 6582.72,
        fwd_lookup_ms: 23.49,
        rev_lookup_ms: 64.41,
        mem_mb: 3817.21,
        bytes_per_entry: 228,
    },
    // Step 4: SubjectDictNovelty
    Baseline {
        label: "SubjectDictNovelty",
        scale: 500_000,
        insert_ms: 81.59,
        fwd_lookup_ms: 15.80,
        rev_lookup_ms: 39.72,
        mem_mb: 86.91,
        bytes_per_entry: 182,
    },
    Baseline {
        label: "SubjectDictNovelty",
        scale: 25_000_000,
        insert_ms: 7384.31,
        fwd_lookup_ms: 31.19,
        rev_lookup_ms: 81.15,
        mem_mb: 3344.76,
        bytes_per_entry: 140,
    },
    // Step 5: DictOverlay ephemerals
    Baseline {
        label: "DictOverlay::ext_predicates",
        scale: 1_000,
        insert_ms: 0.12,
        fwd_lookup_ms: 0.04,
        rev_lookup_ms: 0.22,
        mem_mb: 0.08,
        bytes_per_entry: 182,
    },
    Baseline {
        label: "DictOverlay::ext_predicates",
        scale: 100_000,
        insert_ms: 5.59,
        fwd_lookup_ms: 0.23,
        rev_lookup_ms: 2.25,
        mem_mb: 5.35,
        bytes_per_entry: 186,
    },
    Baseline {
        label: "DictOverlay::ext_graphs",
        scale: 1_000,
        insert_ms: 0.05,
        fwd_lookup_ms: 0.01,
        rev_lookup_ms: 0.20,
        mem_mb: 0.02,
        bytes_per_entry: 169,
    },
    Baseline {
        label: "DictOverlay::ext_graphs",
        scale: 100_000,
        insert_ms: 2.54,
        fwd_lookup_ms: 0.11,
        rev_lookup_ms: 2.04,
        mem_mb: 1.60,
        bytes_per_entry: 166,
    },
    Baseline {
        label: "DictOverlay::ext_lang_tags",
        scale: 1_000,
        insert_ms: 0.02,
        fwd_lookup_ms: 0.01,
        rev_lookup_ms: 0.13,
        mem_mb: 0.00,
        bytes_per_entry: 102,
    },
    Baseline {
        label: "DictOverlay::ext_lang_tags",
        scale: 100_000,
        insert_ms: 0.97,
        fwd_lookup_ms: 0.07,
        rev_lookup_ms: 1.32,
        mem_mb: 0.00,
        bytes_per_entry: 102,
    },
    Baseline {
        label: "DictOverlay::ext_subjects",
        scale: 1_000,
        insert_ms: 0.12,
        fwd_lookup_ms: 0.00,
        rev_lookup_ms: 0.27,
        mem_mb: 0.12,
        bytes_per_entry: 157,
    },
    Baseline {
        label: "DictOverlay::ext_subjects",
        scale: 100_000,
        insert_ms: 9.35,
        fwd_lookup_ms: 0.22,
        rev_lookup_ms: 2.89,
        mem_mb: 13.38,
        bytes_per_entry: 200,
    },
    Baseline {
        label: "DictOverlay::ext_strings",
        scale: 1_000,
        insert_ms: 0.11,
        fwd_lookup_ms: 0.00,
        rev_lookup_ms: 0.18,
        mem_mb: 0.10,
        bytes_per_entry: 146,
    },
    Baseline {
        label: "DictOverlay::ext_strings",
        scale: 100_000,
        insert_ms: 7.66,
        fwd_lookup_ms: 0.23,
        rev_lookup_ms: 2.22,
        mem_mb: 11.58,
        bytes_per_entry: 173,
    },
    // Step 6: PredicateDict + LanguageTagDict
    Baseline {
        label: "PredicateDict",
        scale: 10_000,
        insert_ms: 0.39,
        fwd_lookup_ms: 0.02,
        rev_lookup_ms: 0.15,
        mem_mb: 0.49,
        bytes_per_entry: 158,
    },
    Baseline {
        label: "PredicateDict",
        scale: 500_000,
        insert_ms: 19.72,
        fwd_lookup_ms: 1.12,
        rev_lookup_ms: 11.39,
        mem_mb: 27.25,
        bytes_per_entry: 190,
    },
    Baseline {
        label: "LanguageTagDict",
        scale: 100,
        insert_ms: 0.02,
        fwd_lookup_ms: 0.01,
        rev_lookup_ms: 0.10,
        mem_mb: 0.00,
        bytes_per_entry: 102,
    },
    Baseline {
        label: "LanguageTagDict",
        scale: 10_000,
        insert_ms: 0.09,
        fwd_lookup_ms: 0.01,
        rev_lookup_ms: 0.09,
        mem_mb: 0.00,
        bytes_per_entry: 102,
    },
];

fn find_baseline(label: &str, scale: usize) -> Option<&'static Baseline> {
    BASELINES
        .iter()
        .find(|b| b.label == label && b.scale == scale)
}

/// Format a percentage delta. Returns "" if the baseline is too small to compare.
fn format_delta(new_val: f64, old_val: f64) -> String {
    if old_val.abs() < 0.005 {
        "   --".to_string()
    } else {
        let pct = (new_val - old_val) / old_val * 100.0;
        format!("{:+.1}%", pct)
    }
}

fn format_delta_int(new_val: usize, old_val: usize) -> String {
    if old_val == 0 {
        "   --".to_string()
    } else {
        let pct = (new_val as f64 - old_val as f64) / old_val as f64 * 100.0;
        format!("{:+.0}%", pct)
    }
}

fn print_detail(r: &BenchResult) {
    let bytes_per_entry = if r.unique_count > 0 {
        r.mem_bytes / r.unique_count
    } else {
        0
    };
    let mem_mb = r.mem_bytes as f64 / 1_048_576.0;
    let bl = find_baseline(r.label, r.scale);

    println!("\n--- {} ({} entries) ---", r.label, format_count(r.scale));
    println!(
        "  Inserts:     {} calls -> {} unique entries",
        format_count(r.scale),
        format_count(r.unique_count)
    );

    if let Some(b) = bl {
        println!(
            "  Insert:      {:>10.2} ms    (was {:>8.2} ms  {})",
            r.insert_ms,
            b.insert_ms,
            format_delta(r.insert_ms, b.insert_ms)
        );
        println!(
            "  Fwd lookup:  {:>10.2} ms    (was {:>8.2} ms  {})  ({} lookups)",
            r.fwd_lookup_ms,
            b.fwd_lookup_ms,
            format_delta(r.fwd_lookup_ms, b.fwd_lookup_ms),
            format_count(r.lookup_count)
        );
        println!(
            "  Rev lookup:  {:>10.2} ms    (was {:>8.2} ms  {})  ({} lookups)",
            r.rev_lookup_ms,
            b.rev_lookup_ms,
            format_delta(r.rev_lookup_ms, b.rev_lookup_ms),
            format_count(r.lookup_count)
        );
        println!(
            "  Memory:      {:>10.2} MB    (was {:>8.2} MB  {})  ({} bytes/entry, was {})",
            mem_mb,
            b.mem_mb,
            format_delta(mem_mb, b.mem_mb),
            bytes_per_entry,
            b.bytes_per_entry
        );
    } else {
        println!("  Insert:      {:>10.2} ms", r.insert_ms);
        println!(
            "  Fwd lookup:  {:>10.2} ms  ({} lookups)",
            r.fwd_lookup_ms,
            format_count(r.lookup_count)
        );
        println!(
            "  Rev lookup:  {:>10.2} ms  ({} lookups)",
            r.rev_lookup_ms,
            format_count(r.lookup_count)
        );
        println!(
            "  Memory:      {:>10.2} MB  ({} bytes/entry)",
            mem_mb, bytes_per_entry
        );
    }
}

fn format_scale(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        format!("{}", n)
    }
}

fn format_count(n: usize) -> String {
    if n >= 1_000_000 {
        let m = n as f64 / 1_000_000.0;
        if m == m.floor() {
            format!("{:.0}M", m)
        } else {
            format!("{:.1}M", m)
        }
    } else if n >= 1_000 {
        let k = n as f64 / 1_000.0;
        if k == k.floor() {
            format!("{:.0}K", k)
        } else {
            format!("{:.1}K", k)
        }
    } else {
        format!("{n}")
    }
}

// ============================================================================
// Benchmark functions — one per target dict
// ============================================================================

// ---------------------------------------------------------------------------
// 1. StringDictNovelty (Step 3)
// ---------------------------------------------------------------------------

fn bench_string_dict_novelty(n: usize) -> BenchResult {
    let iris = generate_iris(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_iris = build_string_lookups(&iris, lookup_count, 99);
    let lookup_ids = build_u32_lookup_ids(lookup_count, n as u32, 100);

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dn = DictNovelty::new_genesis();
    for iri in &iris {
        dn.strings.assign_or_lookup(iri);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dn.strings.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dn.strings.resolve_string(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for iri in &lookup_iris {
        black_box(dn.strings.find_string(iri));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dn);

    BenchResult {
        label: "StringDictNovelty",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 2. SubjectDictNovelty (Step 4)
// ---------------------------------------------------------------------------

fn bench_subject_dict_novelty(n: usize) -> BenchResult {
    let subjects = generate_subject_data(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);

    // Pre-populate to collect valid sid64s for forward lookup generation.
    let mut pre_dn = DictNovelty::new_genesis();
    let assigned_sids: Vec<u64> = subjects
        .iter()
        .map(|(ns, s)| pre_dn.subjects.assign_or_lookup(*ns, s))
        .collect();
    drop(pre_dn);

    let mut rng = SmallRng::seed_from_u64(99);
    let lookup_sid64s: Vec<u64> = (0..lookup_count)
        .map(|_| {
            if rng.gen_bool(0.5) {
                assigned_sids[rng.gen_range(0..assigned_sids.len())]
            } else {
                rng.gen::<u64>()
            }
        })
        .collect();
    drop(assigned_sids);

    let lookup_subjects: Vec<(u16, String)> = (0..lookup_count)
        .map(|_| {
            if rng.gen_bool(0.5) {
                subjects[rng.gen_range(0..subjects.len())].clone()
            } else {
                (
                    rng.gen_range(0..100u16),
                    format!("miss_{}", rng.gen::<u32>()),
                )
            }
        })
        .collect();

    // Actual benchmark
    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dn = DictNovelty::new_genesis();
    for (ns_code, suffix) in &subjects {
        dn.subjects.assign_or_lookup(*ns_code, suffix);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dn.subjects.len();

    let start = Instant::now();
    for &sid64 in &lookup_sid64s {
        black_box(dn.subjects.resolve_subject(sid64));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for (ns_code, suffix) in &lookup_subjects {
        black_box(dn.subjects.find_subject(*ns_code, suffix));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dn);

    BenchResult {
        label: "SubjectDictNovelty",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 3. DictOverlay::ext_predicates (Step 5) [MIGRATED: VecBiDict<u32>]
// ---------------------------------------------------------------------------

fn bench_ext_predicates(n: usize) -> BenchResult {
    let iris = generate_predicate_iris(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_iris = build_string_lookups(&iris, lookup_count, 99);
    let lookup_ids = build_u32_lookup_ids(lookup_count, n as u32, 100);

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = VecBiDict::<u32>::new(100); // base simulates existing persisted predicates
    for iri in &iris {
        dict.assign_or_lookup(iri);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for iri in &lookup_iris {
        black_box(dict.find(iri));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "DictOverlay::ext_predicates",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 4. DictOverlay::ext_graphs (Step 5) [MIGRATED: VecBiDict<u32>]
// ---------------------------------------------------------------------------

fn bench_ext_graphs(n: usize) -> BenchResult {
    let iris = generate_graph_iris(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_iris = build_string_lookups(&iris, lookup_count, 99);
    let lookup_ids = build_u32_lookup_ids(lookup_count, n as u32, 100);

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = VecBiDict::<u32>::new(10); // base simulates existing persisted graphs
    for iri in &iris {
        dict.assign_or_lookup(iri);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for iri in &lookup_iris {
        black_box(dict.find(iri));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "DictOverlay::ext_graphs",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 5. DictOverlay::ext_lang_tags (Step 5) [MIGRATED: VecBiDict<u16>]
// ---------------------------------------------------------------------------

fn bench_ext_lang_tags(n: usize) -> BenchResult {
    let tags = generate_language_tags(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_tags = build_string_lookups(&tags, lookup_count, 99);
    let lookup_ids = build_u16_lookup_ids(lookup_count, 30, 100); // ~30 unique tags

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = VecBiDict::<u16>::new(1); // base_lang_count + 1 (1-based, 0 = no tag)
    for tag in &tags {
        dict.assign_or_lookup(tag);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for tag in &lookup_tags {
        black_box(dict.find(tag));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "DictOverlay::ext_lang_tags",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 6. DictOverlay::ext_subjects (Step 5) [MIGRATED: VecBiDict<u64>]
// ---------------------------------------------------------------------------

fn bench_ext_subjects(n: usize) -> BenchResult {
    let iris = generate_iris(n, 42); // entity-like IRIs
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_iris = build_string_lookups(&iris, lookup_count, 99);
    // u64 IDs — generate as u32 then widen (IDs are sequential from base)
    let lookup_ids: Vec<u64> = build_u32_lookup_ids(lookup_count, n as u32, 100)
        .into_iter()
        .map(u64::from)
        .collect();

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = VecBiDict::<u64>::new(1000); // base simulates existing persisted subjects
    for iri in &iris {
        dict.assign_or_lookup(iri);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for iri in &lookup_iris {
        black_box(dict.find(iri));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "DictOverlay::ext_subjects",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 7. DictOverlay::ext_strings (Step 5) [MIGRATED: VecBiDict<u32>]
// ---------------------------------------------------------------------------

fn bench_ext_strings(n: usize) -> BenchResult {
    let values = generate_string_values(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_values = build_string_lookups(&values, lookup_count, 99);
    let lookup_ids = build_u32_lookup_ids(lookup_count, n as u32, 100);

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = VecBiDict::<u32>::new(5000); // base simulates existing persisted strings
    for val in &values {
        dict.assign_or_lookup(val);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len();

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for val in &lookup_values {
        black_box(dict.find(val));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "DictOverlay::ext_strings",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 8. PredicateDict (Step 6) [MIGRATED: VecBiDict<u32>]
// ---------------------------------------------------------------------------

fn bench_predicate_dict(n: usize) -> BenchResult {
    let iris = generate_predicate_iris(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_iris = build_string_lookups(&iris, lookup_count, 99);
    let lookup_ids = build_u32_lookup_ids(lookup_count, n as u32, 100);

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = PredicateDict::new();
    for iri in &iris {
        dict.get_or_insert(iri);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len() as usize;

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for iri in &lookup_iris {
        black_box(dict.get(iri));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "PredicateDict",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ---------------------------------------------------------------------------
// 9. LanguageTagDict (Step 6) [MIGRATED: VecBiDict<u16>]
// ---------------------------------------------------------------------------

fn bench_language_tag_dict(n: usize) -> BenchResult {
    let tags = generate_language_tags(n, 42);
    let lookup_count = n.clamp(10_000, 500_000);
    let lookup_tags = build_string_lookups(&tags, lookup_count, 99);
    let lookup_ids = build_u16_lookup_ids(lookup_count, 30, 100); // ~30 unique tags

    let baseline = current_allocated();
    reset_peak();

    let start = Instant::now();
    let mut dict = LanguageTagDict::new();
    for tag in &tags {
        dict.get_or_insert(Some(tag.as_str()));
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mem_bytes = current_allocated() - baseline;
    let unique_count = dict.len() as usize;

    let start = Instant::now();
    for &id in &lookup_ids {
        black_box(dict.resolve(id));
    }
    let fwd_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for tag in &lookup_tags {
        black_box(dict.find_id(tag));
    }
    let rev_ms = start.elapsed().as_secs_f64() * 1000.0;

    black_box(&dict);

    BenchResult {
        label: "LanguageTagDict",
        scale: n,
        lookup_count,
        insert_ms,
        fwd_lookup_ms: fwd_ms,
        rev_lookup_ms: rev_ms,
        mem_bytes,
        unique_count,
    }
}

// ============================================================================
// Summary
// ============================================================================

fn print_summary(results: &[BenchResult]) {
    println!("\n{}", "=".repeat(100));
    println!("  VecBiDict COMPARISON -- Current vs Baseline (pre-migration, commit 38cb4af)");
    println!("  Negative Δ = improvement.  '--' = baseline too small to compare.");
    println!("{}", "=".repeat(100));
    println!(
        "  {:<30} {:>6} {:>10} {:>10} {:>10} {:>10} {:>7}",
        "Dict", "Scale", "Insert", "Fwd Lk", "Rev Lk", "Memory", "B/ent"
    );
    println!("  {}", "-".repeat(90));

    for r in results {
        let scale = format_scale(r.scale);
        let mem_mb = r.mem_bytes as f64 / 1_048_576.0;
        let bytes_per_entry = if r.unique_count > 0 {
            r.mem_bytes / r.unique_count
        } else {
            0
        };
        println!(
            "  {:<30} {:>6} {:>8.1}ms {:>8.1}ms {:>8.1}ms {:>8.1}MB {:>5}",
            r.label, scale, r.insert_ms, r.fwd_lookup_ms, r.rev_lookup_ms, mem_mb, bytes_per_entry
        );

        if let Some(bl) = find_baseline(r.label, r.scale) {
            println!(
                "  {:<30} {:>6} {:>10} {:>10} {:>10} {:>10} {:>7}",
                "  Δ vs baseline",
                "",
                format_delta(r.insert_ms, bl.insert_ms),
                format_delta(r.fwd_lookup_ms, bl.fwd_lookup_ms),
                format_delta(r.rev_lookup_ms, bl.rev_lookup_ms),
                format_delta(mem_mb, bl.mem_mb),
                format_delta_int(bytes_per_entry, bl.bytes_per_entry),
            );
        }
    }

    println!("  {}", "-".repeat(90));
    println!();
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    println!("VecBiDict Migration Benchmark");
    println!("==============================");
    println!();
    println!("Benchmarking all 9 dictionary structs. Comparing current vs pre-migration baseline.");
    println!("Baseline from commit 38cb4af (initial-bench.log). Negative Δ = improvement.");
    println!();

    let mut all_results: Vec<BenchResult> = Vec::new();

    // --- Warmup (exercises allocator paths, populates CPU caches) ---
    println!("Warming up...");
    let _ = bench_string_dict_novelty(10_000);
    let _ = bench_ext_predicates(1_000);
    println!("Warmup complete.\n");

    // -----------------------------------------------------------------------
    // Step 3: StringDictNovelty — HashMap<String, u32> + HashMap<u32, Arc<str>>
    // -----------------------------------------------------------------------
    println!("{}", "=".repeat(76));
    println!("  Step 3: StringDictNovelty  [MIGRATED: VecBiDict<u32>]");
    println!("  was: HashMap<String, u32> + HashMap<u32, Arc<str>>");
    println!("{}", "=".repeat(76));

    for &n in &[500_000, 25_000_000] {
        let r = bench_string_dict_novelty(n);
        print_detail(&r);
        all_results.push(r);
    }

    // -----------------------------------------------------------------------
    // Step 4: SubjectDictNovelty — HashMap<Box<[u8]>, u64> + HashMap<u64, (u16, Arc<str>)>
    // -----------------------------------------------------------------------
    println!("\n{}", "=".repeat(76));
    println!("  Step 4: SubjectDictNovelty  [MIGRATED: NsVecBiDict]");
    println!("  was: HashMap<Box<[u8]>, u64> + HashMap<u64, (u16, Arc<str>)>");
    println!("{}", "=".repeat(76));

    for &n in &[500_000, 25_000_000] {
        let r = bench_subject_dict_novelty(n);
        print_detail(&r);
        all_results.push(r);
    }

    // -----------------------------------------------------------------------
    // Step 5: DictOverlay ephemerals [MIGRATED: VecBiDict<Id>]
    // -----------------------------------------------------------------------
    println!("\n{}", "=".repeat(76));
    println!("  Step 5: DictOverlay ephemerals  [MIGRATED: VecBiDict<Id>]");
    println!("  was: HashMap<String, Id> + Vec<String>");
    println!("{}", "=".repeat(76));

    for &n in &[1_000, 100_000] {
        let r = bench_ext_predicates(n);
        print_detail(&r);
        all_results.push(r);
    }

    for &n in &[1_000, 100_000] {
        let r = bench_ext_graphs(n);
        print_detail(&r);
        all_results.push(r);
    }

    for &n in &[1_000, 100_000] {
        let r = bench_ext_lang_tags(n);
        print_detail(&r);
        all_results.push(r);
    }

    for &n in &[1_000, 100_000] {
        let r = bench_ext_subjects(n);
        print_detail(&r);
        all_results.push(r);
    }

    for &n in &[1_000, 100_000] {
        let r = bench_ext_strings(n);
        print_detail(&r);
        all_results.push(r);
    }

    // -----------------------------------------------------------------------
    // Step 6: PredicateDict [MIGRATED: VecBiDict<u32>]
    // -----------------------------------------------------------------------
    println!("\n{}", "=".repeat(76));
    println!("  Step 6: PredicateDict  [MIGRATED: VecBiDict<u32>]");
    println!("  was: FxHashMap<BorrowableString, u32> + Vec<String>");
    println!("{}", "=".repeat(76));

    for &n in &[10_000, 500_000] {
        let r = bench_predicate_dict(n);
        print_detail(&r);
        all_results.push(r);
    }

    // -----------------------------------------------------------------------
    // Step 6: LanguageTagDict [MIGRATED: VecBiDict<u16>]
    // -----------------------------------------------------------------------
    println!("\n{}", "=".repeat(76));
    println!("  Step 6: LanguageTagDict  [MIGRATED: VecBiDict<u16>]");
    println!("  was: FxHashMap<BorrowableString, u16> + Vec<String>");
    println!("{}", "=".repeat(76));

    for &n in &[100, 10_000] {
        let r = bench_language_tag_dict(n);
        print_detail(&r);
        all_results.push(r);
    }

    // -----------------------------------------------------------------------
    // Final summary
    // -----------------------------------------------------------------------
    print_summary(&all_results);

    println!("Done.");
}
