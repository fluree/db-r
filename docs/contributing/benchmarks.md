# Benchmarks

The `fluree-bench` tool generates deterministic supply-chain data and drives configurable transaction and query workloads through the library API.

## Quick Start

```bash
# Default: 10 MB, sequential, in-memory
make bench

# Quick smoke test (1 MB)
make bench-quick

# Large with concurrency
make bench-large

# Full: ingest + query matrix
make bench-full
```

Or run directly:

```bash
cargo run --release -p fluree-bench -- ingest --data-size-mb 10
cargo run --release -p fluree-bench -- full --data-size-mb 50 --concurrency 4
```

## Subcommands

| Command | Purpose |
|---------|---------|
| `ingest` | Transaction/ingest benchmark only |
| `query` | Query benchmark matrix only (requires prior ingest with `--storage`) |
| `full` | Ingest followed by query benchmarks |

## Configuration

### Ingest Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-size-mb` | `10` | Target data size in MB |
| `--batch-size` | `500` | Entities per transaction |
| `--concurrency` | `1` | In-flight concurrent transactions (1 = sequential) |
| `--reindex-min-bytes` | `10000000` | Soft indexing trigger (10 MB) |
| `--reindex-max-bytes` | `200000000` | Hard novelty block (200 MB) |
| `--no-indexing` | `false` | Disable background indexing |
| `--storage` | (none) | File storage path; omit for in-memory |
| `--ledger` | `bench` | Ledger alias |
| `--output` | `text` | Report format: `text` or `json` |
| `--otel` | `false` | Enable OTEL export (requires `--features bench-otel`) |

### Query Flags (additional)

| Flag | Default | Description |
|------|---------|-------------|
| `--query-iterations` | `3` | Repeat each query per cache state |
| `--query-concurrency` | `1,4,8` | Comma-separated concurrency levels |
| `--skip-verify` | `false` | Skip result count verification |

## Storage Modes

```bash
# In-memory (fastest, data lost on exit)
cargo run --release -p fluree-bench -- ingest --data-size-mb 10

# File storage (persistent, measures disk I/O)
cargo run --release -p fluree-bench -- ingest --data-size-mb 10 --storage ./bench-data
```

## Data Model

The tool generates a deterministic supply-chain domain:

```
Manufacturer → Warehouse → Distributor → Retailer
                    ↓                        ↓
              Order (+ LineItems)        Shipment
                    ↓
                 Product → Category
```

~4,810 entities per unit, ~25-40K flakes per unit. Entity IDs are deterministic
for any given `--data-size-mb` value (seeded RNG per unit).

## Report Output

The ingest report includes:
- **Config**: data size, batch size, concurrency, storage mode, indexing config
- **Transaction metrics**: total txns/flakes, single-txn latency (min/max/mean/median/p95/p99)
- **Throughput**: flakes/sec, txns/sec, entities/sec
- **Back-pressure**: event count, total/avg/max blocked duration

The query report shows a matrix of median latency across:
- Query complexity (simple count, filtered, multi-hop join, aggregate)
- Concurrency levels
- Cache state (cold vs warm)

## OTEL / Jaeger Integration

For visual trace investigation:

```bash
# Automated: starts Jaeger, runs benchmark with trace export
make bench-otel

# Or manually:
make bench-otel-setup    # start Jaeger container
cargo run --release -p fluree-bench --features bench-otel -- \
    ingest --data-size-mb 10 --concurrency 4 --otel
# Open http://localhost:16686, select "fluree-bench"
make bench-otel-teardown  # stop Jaeger
```

The orchestration script (`benchmarks/fluree-bench/scripts/bench-otel.sh`) handles
Docker lifecycle, build, and OTEL environment variables automatically.

## Testing Back-Pressure

To observe novelty back-pressure (commits blocked while indexing catches up):

```bash
cargo run --release -p fluree-bench -- ingest \
    --data-size-mb 10 \
    --concurrency 4 \
    --reindex-max-bytes 5000000 \
    --reindex-min-bytes 1000000
```

This sets a low novelty limit, forcing the indexer to intervene frequently.
Back-pressure events and durations will appear in the report.

## Makefile Targets

| Target | Description |
|--------|-------------|
| `bench` | 10 MB ingest, sequential, in-memory |
| `bench-quick` | 1 MB smoke test |
| `bench-large` | 100 MB ingest, concurrency=4 |
| `bench-concurrent` | 10 MB ingest, concurrency=8 |
| `bench-full` | Ingest + query matrix |
| `bench-queries` | Query matrix only |
| `bench-otel` | Full OTEL workflow (Jaeger + bench) |
| `bench-otel-setup` | Start Jaeger container |
| `bench-otel-teardown` | Stop Jaeger container |
