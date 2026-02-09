// JSON-LD vs Turtle insert benchmarks.
//
// Measures throughput of sequential inserts using identical triples in both
// serialization formats, across a matrix of transaction counts and node sizes.
//
// ## Data model
//
// ~10% Company nodes with `ex:employees` and `ex:customers` refs to Person
// nodes within the same transaction.  ~90% Person nodes with scalar properties.
// All data is fully deterministic (no RNG) for reproducibility and future
// data-integrity tests.
//
// ## Matrix
//
//   formats:   jsonld, turtle
//   txn counts: 10, 100, 1000
//   nodes/txn:  10, 100, 1000
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench insert_formats
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --bench insert_formats -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Runtime;

fn init_tracing_for_bench() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        if std::env::var("FLUREE_BENCH_TRACING").ok().as_deref() != Some("1") {
            return;
        }
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info");
        }
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_level(true)
            .try_init()
            .ok();
    });
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Transaction counts to benchmark.
const TXN_COUNTS: &[usize] = &[10, 100, 1_000];

/// Nodes per transaction to benchmark.
const NODES_PER_TXN: &[usize] = &[10, 100, 1_000];

/// Unique ledger alias counter (each Criterion iteration needs a fresh ledger).
static LEDGER_COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Type aliases (matches vector_query.rs pattern)
// ---------------------------------------------------------------------------

type BenchFluree = fluree_db_api::Fluree<
    fluree_db_core::MemoryStorage,
    fluree_db_nameservice::memory::MemoryNameService,
>;
type BenchLedger = fluree_db_api::LedgerState<fluree_db_core::MemoryStorage>;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

struct PersonData {
    id: String,
    name: String,
    email: String,
    age: u32,
}

struct CompanyData {
    id: String,
    name: String,
    founded: String,
    employee_ids: Vec<String>,
    customer_ids: Vec<String>,
}

struct TxnData {
    persons: Vec<PersonData>,
    companies: Vec<CompanyData>,
}

struct PregenData {
    jsonld_txns: Vec<JsonValue>,
    turtle_txns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

fn generate_txn_data(txn_idx: usize, nodes_per_txn: usize) -> TxnData {
    let n_companies = std::cmp::max(1, nodes_per_txn / 10);
    let n_persons = nodes_per_txn - n_companies;

    let global_base = txn_idx * nodes_per_txn;

    let persons: Vec<PersonData> = (0..n_persons)
        .map(|i| {
            let gid = global_base + n_companies + i;
            PersonData {
                id: format!("ex:person-{gid:06}"),
                name: format!("Person {gid:06}"),
                email: format!("person{gid}@example.org"),
                age: 18 + (gid % 48) as u32,
            }
        })
        .collect();

    let companies: Vec<CompanyData> = (0..n_companies)
        .map(|i| {
            let gid = global_base + i;

            // Distribute persons across companies for refs
            let chunk = n_persons / n_companies;
            let start = i * chunk;
            let end = if i == n_companies - 1 {
                n_persons
            } else {
                start + chunk
            };
            let mid = start + (end - start) / 2;

            let employee_ids: Vec<String> = (start..mid).map(|p| persons[p].id.clone()).collect();
            let customer_ids: Vec<String> = (mid..end).map(|p| persons[p].id.clone()).collect();

            // Deterministic date: 2000-01-01 + (gid * 17 % 9000) days
            let base_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let days_offset = (gid * 17 % 9000) as i64;
            let founded = base_date + chrono::Duration::days(days_offset);

            CompanyData {
                id: format!("ex:company-{gid:06}"),
                name: format!("Company {gid:06}"),
                founded: founded.format("%Y-%m-%d").to_string(),
                employee_ids,
                customer_ids,
            }
        })
        .collect();

    TxnData { persons, companies }
}

fn txn_data_to_jsonld(data: &TxnData) -> JsonValue {
    let mut graph = Vec::with_capacity(data.persons.len() + data.companies.len());

    for p in &data.persons {
        graph.push(json!({
            "@id": p.id,
            "@type": "ex:Person",
            "ex:name": p.name,
            "ex:email": p.email,
            "ex:age": {"@value": p.age, "@type": "xsd:integer"}
        }));
    }

    for c in &data.companies {
        let employees: Vec<JsonValue> =
            c.employee_ids.iter().map(|id| json!({"@id": id})).collect();
        let customers: Vec<JsonValue> =
            c.customer_ids.iter().map(|id| json!({"@id": id})).collect();
        graph.push(json!({
            "@id": c.id,
            "@type": "ex:Company",
            "ex:name": c.name,
            "ex:founded": {"@value": c.founded, "@type": "xsd:date"},
            "ex:employees": employees,
            "ex:customers": customers
        }));
    }

    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": graph
    })
}

fn txn_data_to_turtle(data: &TxnData) -> String {
    let mut buf = String::with_capacity(data.persons.len() * 200 + data.companies.len() * 400);
    buf.push_str("@prefix ex: <http://example.org/ns/> .\n");
    buf.push_str("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n");

    for p in &data.persons {
        buf.push_str(&p.id);
        buf.push_str(" a ex:Person ;\n");
        buf.push_str(&format!("    ex:name \"{}\" ;\n", p.name));
        buf.push_str(&format!("    ex:email \"{}\" ;\n", p.email));
        buf.push_str(&format!("    ex:age \"{}\"^^xsd:integer .\n\n", p.age));
    }

    for c in &data.companies {
        buf.push_str(&c.id);
        buf.push_str(" a ex:Company ;\n");
        buf.push_str(&format!("    ex:name \"{}\" ;\n", c.name));
        buf.push_str(&format!("    ex:founded \"{}\"^^xsd:date", c.founded));

        if !c.employee_ids.is_empty() {
            buf.push_str(" ;\n    ex:employees ");
            for (j, eid) in c.employee_ids.iter().enumerate() {
                if j > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(eid);
            }
        }

        if !c.customer_ids.is_empty() {
            buf.push_str(" ;\n    ex:customers ");
            for (j, cid) in c.customer_ids.iter().enumerate() {
                if j > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(cid);
            }
        }

        buf.push_str(" .\n\n");
    }

    buf
}

fn pregen(txn_count: usize, nodes_per_txn: usize) -> PregenData {
    let mut jsonld_txns = Vec::with_capacity(txn_count);
    let mut turtle_txns = Vec::with_capacity(txn_count);

    for txn_idx in 0..txn_count {
        let data = generate_txn_data(txn_idx, nodes_per_txn);
        jsonld_txns.push(txn_data_to_jsonld(&data));
        turtle_txns.push(txn_data_to_turtle(&data));
    }

    PregenData {
        jsonld_txns,
        turtle_txns,
    }
}

// ---------------------------------------------------------------------------
// Insert runners
// ---------------------------------------------------------------------------

async fn run_jsonld_inserts(
    fluree: &BenchFluree,
    ledger: BenchLedger,
    txns: &[JsonValue],
    index_config: &IndexConfig,
) -> BenchLedger {
    let mut ledger = ledger;
    for txn in txns {
        let result = fluree
            .insert_with_opts(
                ledger,
                txn,
                TxnOpts::default(),
                CommitOpts::default(),
                index_config,
            )
            .await
            .unwrap();
        ledger = result.ledger;
    }
    ledger
}

async fn run_turtle_inserts(
    fluree: &BenchFluree,
    ledger: BenchLedger,
    txns: &[String],
    index_config: &IndexConfig,
) -> BenchLedger {
    let mut ledger = ledger;
    for txn in txns {
        let result = fluree
            .insert_turtle_with_opts(
                ledger,
                txn,
                TxnOpts::default(),
                CommitOpts::default(),
                index_config,
            )
            .await
            .unwrap();
        ledger = result.ledger;
    }
    ledger
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_insert_formats(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = Runtime::new().unwrap();
    let fluree = FlureeBuilder::memory().build_memory();

    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let mut group = c.benchmark_group("insert_formats");
    group.sample_size(10);

    for &txn_count in TXN_COUNTS {
        for &nodes_per_txn in NODES_PER_TXN {
            let total_nodes = txn_count * nodes_per_txn;

            eprintln!(
                "  [pregen] {} txns x {} nodes/txn = {} total nodes ...",
                txn_count, nodes_per_txn, total_nodes
            );
            let data = pregen(txn_count, nodes_per_txn);

            group.throughput(Throughput::Elements(total_nodes as u64));

            let param_label = format!("{txn_count}txn_{nodes_per_txn}nodes");

            // --- JSON-LD ---
            group.bench_with_input(
                BenchmarkId::new("jsonld", &param_label),
                &data,
                |b, data| {
                    b.iter(|| {
                        let id = LEDGER_COUNTER.fetch_add(1, Ordering::Relaxed);
                        let alias = format!("bench/jld-{id}:main");
                        rt.block_on(async {
                            let ledger = fluree.create_ledger(&alias).await.unwrap();
                            black_box(
                                run_jsonld_inserts(
                                    &fluree,
                                    ledger,
                                    &data.jsonld_txns,
                                    &index_config,
                                )
                                .await,
                            )
                        })
                    })
                },
            );

            // --- Turtle ---
            group.bench_with_input(
                BenchmarkId::new("turtle", &param_label),
                &data,
                |b, data| {
                    b.iter(|| {
                        let id = LEDGER_COUNTER.fetch_add(1, Ordering::Relaxed);
                        let alias = format!("bench/ttl-{id}:main");
                        rt.block_on(async {
                            let ledger = fluree.create_ledger(&alias).await.unwrap();
                            black_box(
                                run_turtle_inserts(
                                    &fluree,
                                    ledger,
                                    &data.turtle_txns,
                                    &index_config,
                                )
                                .await,
                            )
                        })
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_insert_formats);
criterion_main!(benches);
