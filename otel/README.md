# OTEL Testing & Validation Infrastructure

Validate that OpenTelemetry tracing spans appear correctly in Jaeger. Provides a Makefile-driven workflow to start Jaeger, build with `--features otel`, run the server, and exercise all instrumented code paths.

## Prerequisites

- Docker (for Jaeger)
- Rust toolchain (for `cargo build`)
- curl
- bash

## Quick Start

```bash
cd otel/

# Full setup: start Jaeger, build binaries, start server, run smoke tests
make all

# Open Jaeger UI to inspect traces
make ui
```

## Makefile Targets

### Infrastructure

| Target | Description |
|--------|-------------|
| `make up` | Start Jaeger via docker compose |
| `make down` | Stop Jaeger container |
| `make reset` | Restart Jaeger (clears trace data) |
| `make ui` | Open Jaeger UI in browser |
| `make build` | Build server + ingest with `--features otel` (release) |

### Server

| Target | Description |
|--------|-------------|
| `make server` | Start fluree-server in background with OTEL export |
| `make server-stop` | Stop the background server |
| `make server-logs` | Tail server stdout/stderr |

### Scenarios

| Target | What it exercises | Expected Jaeger spans |
|--------|-------------------|----------------------|
| `make transact` | Insert, upsert, update, Turtle, SPARQL UPDATE | `transact_execute` > `txn_stage` > `txn_commit` |
| `make query` | FQL select/filter/sort, SPARQL basic/OPTIONAL/GROUP BY | `query_execute` > `query_prepare` > `query_run` > operators |
| `make index` | 500-entity burst to trigger background indexing | `index_build` > `build_all_indexes` > `build_index` |
| `make firehose` | Bulk import 100K+ entities via fluree-ingest | Ingest spans, commit spans, indexing spans |
| `make ingest` | Standalone import with OTEL tracing (no server) | `bulk_import` > `import_chunks` > `import_index_build` |
| `make smoke` | Full cycle: seed + transact + query + index | End-to-end span waterfall |
| `make stress` | 50K inserts with backpressure + expensive query battery | Operator bottlenecks, `index_gc` with child spans, backpressure retries |
| `make stress-query` | Re-run only the query battery (no inserts, no server restart) | Quick iteration on query traces when stress data is already loaded |
| `make cycle` | 3x full cycle — triggers multiple index rebuilds | Sustained trace patterns |

### Data & Cleanup

| Target | Description |
|--------|-------------|
| `make generate` | Generate TTL data files for firehose |
| `make clean` | Remove `_data/` (storage, generated TTL, PIDs) |
| `make clean-all` | `clean` + stop Docker |
| `make nuke` | `clean-all` + remove compiled binaries |

### Convenience

| Target | Description |
|--------|-------------|
| `make all` | `up` + `build` + `server` + `smoke` |
| `make fresh` | `reset` + `clean` + `build` + `server` + `smoke` |

## Configuration

All configurable via Make variables:

```bash
make server PORT=9090              # Custom port
make smoke LEDGER=mytest:main      # Custom ledger name
make generate ENTITIES=500000      # More data for firehose
make server INDEXING=false         # Disable background indexing
make firehose PARSE_THREADS=8     # More parallel parse threads
make server RUST_LOG=info,fluree_db_query=trace  # Custom log level
make stress STRESS_PRODUCTS=10000 STRESS_BATCH=200  # Smaller stress test
make ingest INGEST_ENTITIES=50000                  # More data for ingest smoke test
make ingest INGEST_LEDGER=mytest:main              # Custom ledger name
```

## RUST_LOG Patterns

| Level | Pattern | When to use |
|-------|---------|-------------|
| Default | `info` | Production; top-level operation timing |
| Query debug | `info,fluree_db_query=debug` | Investigate slow queries |
| Txn debug | `info,fluree_db_transact=debug` | Investigate slow transactions |
| Full debug | `info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug` | Full phase decomposition (default for `make server`) |
| Operator trace | `info,fluree_db_query=trace` | Per-operator detail: scan, join, filter, sort |
| Everything | `debug` | Console firehose (OTEL layer still filters to `fluree_*`) |

## Directory Layout

```
otel/
├── Makefile                # All targets
├── README.md               # This file
├── docker-compose.yml      # Jaeger all-in-one
├── .gitignore              # Ignores _data/
├── scripts/
│   ├── generate-data.sh    # TTL data generator
│   ├── wait-for-server.sh  # Health check poller
│   ├── seed-ledger.sh      # Create ledger + insert seed data
│   ├── transact-smoke.sh   # Transaction scenario
│   ├── query-smoke.sh      # Query scenario
│   ├── index-smoke.sh      # Indexing scenario
│   ├── firehose.sh         # High-volume ingest
│   ├── ingest-smoke.sh     # Standalone OTEL ingest (no server)
│   ├── stress-test.sh      # 50K inserts + backpressure + query battery
│   └── full-cycle.sh       # Combined scenario
└── _data/                  # gitignored; created at runtime
    ├── storage/            # File-backed Fluree storage
    ├── generated/          # Generated TTL files
    └── server.pid          # Background server PID
```

## What to Look for in Jaeger

After running `make smoke`, open Jaeger at `http://localhost:16686` and search for service `fluree-server`.

### Root span names (otel.name)

Traces are named via `otel.name` for easy identification in Jaeger's trace list:

| Operation | Span name examples |
|-----------|-------------------|
| Query | `query:fql`, `query:sparql`, `query:explain` |
| Transact | `transact:fql`, `transact:sparql-update`, `transact:turtle` |
| Insert | `insert:fql`, `insert:turtle` |
| Upsert | `upsert:fql`, `upsert:turtle` |
| Ledger mgmt | `ledger:create`, `ledger:drop`, `ledger:info`, `ledger:exists` |

The `operation` tag on each span retains the handler-specific name (e.g. `query` vs `query_ledger`) for filtering.

### Transaction traces

```
transact:fql (root, otel.name)
  └─ transact_execute
       └─ txn_stage
            ├─ where_exec          (for updates with WHERE clause)
            ├─ insert_gen          (generate insert flakes)
            ├─ delete_gen          (generate delete flakes)
            └─ cancellation_check  (policy evaluation)
       └─ txn_commit
            ├─ build_novelty
            ├─ resolve_commit
            └─ write_commit
```

### Query traces

```
query:sparql (root, otel.name)
  └─ query_execute / sparql_execute
       └─ query_prepare
            ├─ reasoning_prep
            ├─ pattern_rewrite
            └─ plan
       └─ query_run
            ├─ scan              (per-scan operator)
            ├─ join              (hash/nested-loop)
            ├─ filter
            ├─ sort
            ├─ aggregate
            └─ project
```

### Index traces

```
index_build
  └─ build_all_indexes
       ├─ build_index (SPOT)
       ├─ build_index (PSOT)
       ├─ build_index (POST)
       └─ build_index (OPST)
```

## Stress Test

The `make stress` target exercises high-volume insert throughput and expensive queries. It's designed to trigger multiple index cycles, backpressure retries, and generate traces with meaningful durations.

### What it does

1. **Seeds 20 categories** via a single insert
2. **Inserts 50,000 products** (configurable) in batches of 500, with exponential backoff on novelty-at-max backpressure (HTTP 400 with "Novelty at maximum size")
3. **Waits for indexing to settle**, then fires 2 additional bursts of 5,000 products each
4. **Runs 6 expensive SPARQL queries** (3 iterations each) exercising sort, join, filter, GROUP BY, OPTIONAL, and subqueries

### Backpressure behavior

When the server's novelty buffer fills (default 1MB), transactions are rejected with HTTP 400. The stress script detects this and retries with exponential backoff (2s, 4s, 8s... capped at 30s). This is normal and expected -- it means indexing is working to drain the buffer.

### Configuration

```bash
make stress STRESS_PRODUCTS=10000   # Fewer products (faster)
make stress STRESS_BATCH=200        # Smaller batches
make stress STRESS_PRODUCTS=100000  # More products (triggers more index cycles)
```

### What to look for in Jaeger

- **Backpressure visibility**: Script output shows retry counts and wait times
- **index_build** traces with `gc_walk_chain` + `gc_delete_entries` child spans under `index_gc`
- **query:sparql** traces with `scan`, `join`, `filter`, `project`, `sort` operator spans under `query_run`
- **Query durations >100ms** indicating meaningful operator work on large datasets
- **Multiple index_build traces** showing sustained indexing activity

## Troubleshooting

**Server won't start:**
- Check `make server-logs` for errors
- Ensure port 8090 is free: `lsof -i :8090`
- Ensure binaries are built: `make build`

**No traces in Jaeger:**
- Verify Jaeger is running: `docker compose ps`
- Verify OTEL env vars: server must have `OTEL_SERVICE_NAME` and `OTEL_EXPORTER_OTLP_ENDPOINT` set (handled by Makefile)
- Check that binaries were built with `--features otel`
- Traces batch-export with a slight delay; wait a few seconds after requests

**Stale server PID:**
- `make server-stop` handles stale PIDs gracefully
- Or manually: `rm _data/server.pid`
