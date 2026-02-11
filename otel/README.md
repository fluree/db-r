# OTEL Deep Tracing — Test Harness

Self-contained environment for validating deep tracing instrumentation via Jaeger.

## Quick Start

```bash
make all      # Start Jaeger, build server, run smoke tests
make ui       # Open Jaeger UI in browser
```

## Prerequisites

- Docker (for Jaeger)
- Rust toolchain (for building the server)
- `curl`, `python3` (for test scripts)

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make all` | Full setup: Jaeger + build + server + smoke tests |
| `make fresh` | Clean slate: stop everything, clean data, then full setup |
| `make up` | Start Jaeger container |
| `make down` | Stop Jaeger container |
| `make build` | Build `fluree-server` with `otel` feature |
| `make server` | Start server in background (stops existing first) |
| `make server-stop` | Stop background server |
| `make smoke` | Seed ledger + run query + transact scenarios |
| `make stress` | Sustained load: batch inserts + expensive queries |
| `make ui` | Open Jaeger UI in browser |
| `make logs` | Tail server log |
| `make clean` | Remove storage directory and log files |

## Configuration

Override via environment or make variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8090` | Server listen port |
| `LEDGER` | `otel-test:main` | Test ledger name |
| `ENTITIES` | `100000` | Entity count for stress test |
| `RUST_LOG` | `info,fluree_db_query=debug,...` | Log filter |

Example:
```bash
RUST_LOG=debug ENTITIES=50000 make fresh
```

## What to Look for in Jaeger

### After `make smoke`

1. **Service:** Select "fluree-server" from the service dropdown
2. **Query traces:** Look for spans named `query:fql` and `query:sparql`
   - Expand to see `query_execute` with `ledger_id` attribute
   - At debug level: `query_prepare` > `reasoning_prep`, `pattern_rewrite`, `plan`
   - At debug level: `query_run` with operator sub-spans
3. **Transaction traces:** Look for `transact:fql`, `insert:fql`, `upsert:fql`, `insert:turtle`
   - Expand to see `txn_stage` > `insert_gen`, `cancellation`, etc.
   - Expand to see `txn_commit` > `commit_nameservice_lookup`, `commit_write_raw_txn`, etc.
4. **Ledger creation:** Look for `ledger:create`

### After `make stress`

1. **Span volume:** Verify Jaeger is receiving all traces (no gaps from batch overflow)
2. **No orphaned spans:** Every `query_execute` / `txn_stage` should be nested under a `request` root
3. **No cross-request contamination:** Independent requests should NOT appear nested under each other
4. **Indexing traces:** Look for `index_build` as separate top-level traces (not nested under HTTP requests)

### Verifying Async Safety

If you see any of these in Jaeger, there is an async tracing bug:
- Sequential independent requests appearing as parent-child
- Child spans that outlive their parent span
- `index_build` spans nested under HTTP request spans
- Duplicate `request` spans (indicates tower-http TraceLayer collision)

### RUST_LOG Patterns

| Goal | RUST_LOG value |
|------|---------------|
| Production default | `info` |
| Debug queries only | `info,fluree_db_query=debug` |
| Debug transactions only | `info,fluree_db_transact=debug` |
| Full phase decomposition | `info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug` |
| Per-operator detail | `info,fluree_db_query=trace` |
| Console firehose | `debug` |

The OTEL layer always filters to `fluree_*` crates at DEBUG max, regardless of `RUST_LOG`.
Setting `RUST_LOG=debug` produces verbose console output but does NOT flood Jaeger with
hyper/tonic/tower noise.
