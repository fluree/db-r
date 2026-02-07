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
| `make smoke` | Full cycle: seed + transact + query + index | End-to-end span waterfall |
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
│   └── full-cycle.sh       # Combined scenario
└── _data/                  # gitignored; created at runtime
    ├── storage/            # File-backed Fluree storage
    ├── generated/          # Generated TTL files
    └── server.pid          # Background server PID
```

## What to Look for in Jaeger

After running `make smoke`, open Jaeger at `http://localhost:16686` and search for service `fluree-server`.

### Transaction traces

```
transact_execute
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
query_execute
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
