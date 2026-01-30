# Performance Investigation

This guide walks through diagnosing slow queries and transactions in Fluree
using deep tracing, OpenTelemetry export, and query tracking. All tracing
instrumentation is compiled unconditionally but filtered out at the default
`info` log level, so there is zero overhead until you opt in.

## When to Use This Guide

- Queries that used to be fast are now slow
- Transaction commit times are climbing
- p95 latency spikes under load
- You need to find _where_ time is spent, not just _how long_ things take
- You are running a high-throughput workload (firehose) and need sampling

## Quick Start: Enable Deep Tracing

The fastest way to see where time goes is to raise the log level for query and
transaction crates:

```bash
# Query investigation
RUST_LOG=info,fluree_db_query=debug cargo run -p fluree-db-server

# Transaction investigation
RUST_LOG=info,fluree_db_transact=debug cargo run -p fluree-db-server

# Both at once
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug \
  cargo run -p fluree-db-server
```

At `debug` level you will immediately see phase-level spans in log output
showing durations for parsing, planning, execution, formatting, and commit
phases. At `info` level (the default) none of these appear.

## Setting Up Jaeger for Waterfall Visualization

Deep tracing is most useful as a visual span waterfall. Jaeger or Grafana Tempo
can display the full hierarchy with durations, fields, and parent-child
relationships.

### 1. Start Jaeger

```bash
docker run -d --name jaeger \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/jaeger:latest
```

This exposes the OTLP collector on port 4317 and the Jaeger UI on port 16686.

### 2. Build Fluree with OTEL support

The OTLP exporter is behind the `otel` feature flag to avoid pulling in
dependencies when not needed:

```bash
cargo build --release -p fluree-db-server --features otel
```

### 3. Run with OTEL export enabled

```bash
OTEL_SERVICE_NAME=fluree \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_TRACES_SAMPLER=always_on \
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug \
  ./target/release/fluree-server --storage-path ./dev-data
```

### 4. Open Jaeger UI

Navigate to [http://localhost:16686](http://localhost:16686), select the
`fluree` service, and click **Find Traces**. Each query or transaction appears
as a trace with a full span waterfall.

> **Grafana Tempo** works the same way — point `OTEL_EXPORTER_OTLP_ENDPOINT`
> at your Tempo ingestion endpoint.

## Investigating a Slow Query

### The query waterfall

When running at `RUST_LOG=info,fluree_db_query=debug`, every query produces
this span hierarchy:

```
request
  └─ query_execute (ledger_alias, query_kind, tracker_time, tracker_fuel)
       ├─ parse (input_format, input_bytes)
       ├─ reasoning_prep (rdfs, owl2ql, owl2rl, datalog)
       ├─ pattern_rewrite (patterns_before, patterns_after)
       ├─ plan (pattern_count)
       ├─ execute
       │    ├─ scan* (index, flakes_scanned)          [trace level]
       │    ├─ join* (batched)                         [trace level]
       │    ├─ filter*                                 [trace level]
       │    ├─ sort* (sort_keys)                       [trace level]
       │    ├─ group_by* / aggregate* / distinct*      [trace level]
       │    └─ limit* / offset* / project*             [trace level]
       ├─ policy_eval (flakes_checked, flakes_allowed) [when policy active]
       └─ format (output_format, result_count)
```

### Step-by-step investigation

1. **Find the trace** in Jaeger by searching for the `fluree` service. Filter
   by `operation=query` or sort by duration to find slow ones.

2. **Look at the top-level `query_execute` span.** The `tracker_time` field
   (when tracking is enabled) gives wall-clock time. Compare it with the span
   duration to check for scheduling overhead.

3. **Identify the dominant phase.** Open the span waterfall and find which
   child span has the longest duration:

   | Phase             | What it does                                      | When it's slow                                            |
   | ----------------- | ------------------------------------------------- | --------------------------------------------------------- |
   | `parse`           | Parses FQL JSON-LD or SPARQL text into an AST     | Very large query payloads (rare)                          |
   | `reasoning_prep`  | Materializes RDFS/OWL inferences                  | Large ontologies with many class/property hierarchies     |
   | `pattern_rewrite` | Rewrites WHERE patterns for optimization          | Many patterns with complex rewrites (rare)                |
   | `plan`            | Builds the operator tree (scan/join/filter order) | Many patterns (rare)                                      |
   | `execute`         | Runs the operator tree against the index          | **Most common bottleneck** — large scans, expensive joins |
   | `policy_eval`     | Filters results through policy rules              | Complex policies with many rules or large result sets     |
   | `format`          | Serializes results to JSON-LD, SPARQL JSON, etc.  | Deep graph crawls, large result sets                      |

4. **If `execute` dominates**, drill deeper by raising to trace level:

   ```bash
   RUST_LOG=info,fluree_db_query=trace
   ```

   This reveals per-operator spans (`scan`, `join`, `filter`, `sort`, etc.)
   as children of `execute`. Look for:
   - **`scan` with high `flakes_scanned`** — consider a more selective pattern
     or adding type constraints
   - **`join` taking long** — the join order may be suboptimal; try reordering
     WHERE patterns so the most selective pattern comes first
   - **`sort` on large result sets** — add a LIMIT or use a more targeted query
   - **`filter` discarding most rows** — move the filter condition earlier

5. **If `format` dominates**, the query returns many results or uses deep graph
   crawl. Consider adding LIMIT/OFFSET pagination or reducing crawl depth.

6. **If `reasoning_prep` dominates**, the OWL/RDFS schema is large. Consider
   whether full reasoning is needed for this query, or if a simpler reasoning
   mode can be used.

## Investigating Slow Transactions

### The transaction waterfall

When running at `RUST_LOG=info,fluree_db_transact=debug`, every transaction
produces this span hierarchy:

```
request
  └─ transact_execute (ledger_alias, txn_type, tracker_time, tracker_fuel)
       ├─ parse (input_format, txn_type)
       ├─ txn_stage (txn_type, insert_count)
       │    ├─ where_exec (pattern_count, binding_rows)
       │    ├─ delete_gen (template_count, retraction_count)
       │    ├─ insert_gen (template_count, assertion_count)
       │    ├─ cancellation (flakes_before, cancelled_count)
       │    └─ policy_enforce (flakes_checked)
       └─ commit (flake_count, bytes_written)
            ├─ storage_write (bytes_written)
            └─ ns_publish
```

### Common transaction bottlenecks

| Phase            | What it does                                                            | When it's slow                                             |
| ---------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------- |
| `where_exec`     | Evaluates the WHERE clause to find matching bindings                    | Large WHERE clauses or broad patterns scanning many flakes |
| `delete_gen`     | Generates retraction flakes from DELETE templates                       | Many retractions (large batch deletes)                     |
| `insert_gen`     | Generates assertion flakes from INSERT templates                        | Large batch inserts with many triples                      |
| `cancellation`   | Removes flakes that cancel each other (assert + retract of same triple) | Many overlapping inserts and deletes                       |
| `policy_enforce` | Checks transaction flakes against policy rules                          | Complex policies with many flakes to check                 |
| `storage_write`  | Writes commit data to storage (file, S3, etc.)                          | Large commits, slow storage backend, network I/O to S3     |
| `ns_publish`     | Publishes the new commit to the nameservice                             | Nameservice latency (network, DynamoDB, etc.)              |

### Investigation steps

1. **Find the slow transaction** in Jaeger (filter by `operation=transact`).

2. **Check the `txn_stage` vs. `commit` split.** If staging dominates, the
   query/flake-generation work is the bottleneck. If commit dominates, it's
   storage I/O.

3. **If `where_exec` is slow**, the transaction's WHERE clause is doing
   expensive query work. Apply the same query optimization techniques:
   reorder patterns, add constraints, reduce result set size.

4. **If `storage_write` is slow**, check your storage backend. S3 writes are
   inherently slower than local file I/O. Monitor `bytes_written` to see if
   commit sizes are growing.

5. **If `ns_publish` is slow**, the nameservice (file, DynamoDB, etc.) may be
   under load or have network issues.

## High-Throughput / Firehose Scenarios

When running at high transaction or query rates, tracing every operation is
too expensive. Use sampling to capture a representative subset.

### Enable sampling

```bash
# Sample 1% of traces (good starting point for high-throughput)
OTEL_TRACES_SAMPLER=traceidratio \
OTEL_TRACES_SAMPLER_ARG=0.01 \
OTEL_SERVICE_NAME=fluree \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug \
  ./fluree-server
```

### Sampling strategies

| Sampler                 | `OTEL_TRACES_SAMPLER` value | When to use                                                                                     |
| ----------------------- | --------------------------- | ----------------------------------------------------------------------------------------------- |
| Always on               | `always_on`                 | Development, debugging specific issues                                                          |
| Always off              | `always_off`                | Disable tracing entirely                                                                        |
| Ratio                   | `traceidratio`              | **Production high-throughput** — set `OTEL_TRACES_SAMPLER_ARG` to desired rate (0.01 = 1%)      |
| Parent-based always on  | `parentbased_always_on`     | When upstream services propagate trace context and you want to respect their sampling decisions |
| Parent-based always off | `parentbased_always_off`    | Only trace when upstream explicitly requests it                                                 |

### Correlating metrics with traces

For high-throughput workloads, combine Prometheus metrics with trace
drill-down:

1. **Monitor key metrics** via `curl http://localhost:8090/metrics`:
   - `fluree_query_duration_seconds` histogram — watch p95/p99
   - `fluree_indexing_lag_transactions` gauge — rising lag indicates the
     indexing subsystem is falling behind
   - `fluree_transaction_duration_seconds` histogram — watch for commit
     time increases

2. **When metrics show a problem**, use Jaeger to find sampled traces in
   that time window and drill into the span waterfall.

3. **Monitor indexing lag** as a leading indicator. When novelty grows
   (indexing falls behind), query performance degrades because scans must
   merge indexed data with in-memory novelty.

## Correlating Query Tracking with Tracing

Fluree's [query tracking](../query/tracking-and-fuel.md) system records
execution time and fuel (items processed). When tracking is enabled, these
values are also recorded on the tracing span, making them visible in
Jaeger/Tempo.

### How it works

When you send a query with `"opts": {"meta": true}`:

1. The response includes `time` and `fuel` fields as usual.
2. The `query_execute` tracing span gets `tracker_time` and `tracker_fuel`
   fields populated from the same data.
3. In Jaeger, you can search for traces with high `tracker_fuel` values to
   find expensive queries.

### Using fuel as a proxy for work done

Fuel measures the number of items processed (flakes scanned, graph nodes
crawled). It is a better proxy for "work done" than wall-clock time because
it is not affected by system load, I/O wait, or scheduling.

- **High fuel, low time** — the query is doing a lot of work but the system
  is handling it well. Consider whether all that work is necessary.
- **High fuel, high time** — the query is doing a lot of work and it's slow.
  Focus on reducing the work (more selective patterns, LIMIT, etc.).
- **Low fuel, high time** — time is being spent outside of data processing
  (parsing, formatting, storage I/O, scheduling). Check the span waterfall
  for non-execute phases.

## RUST_LOG Levels Reference

| Level                  | `RUST_LOG` setting                                                         | What you see                                                                  | Overhead                                                        |
| ---------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Info (default)         | `info`                                                                     | Top-level request and operation spans only. No internal phase detail.         | Negligible                                                      |
| Debug (investigation)  | `info,fluree_db_query=debug,fluree_db_transact=debug`                      | Full phase waterfall: parse, reasoning, plan, execute, format, stage, commit. | Low — one span per phase per operation                          |
| Trace (maximum detail) | `info,fluree_db_query=trace,fluree_db_transact=trace,fluree_db_core=trace` | Per-operator spans (scan, join, filter, sort, etc.) with row/flake counts.    | Moderate — many spans per query, proportional to operator count |

> **Production guidance:** `debug` level is safe for production investigation.
> `trace` level generates many spans per query and should only be enabled
> briefly for targeted investigation, ideally with sampling.

### Crate names for RUST_LOG filtering

Use underscore-separated crate names (not hyphens) in `RUST_LOG` filters:

| Crate                | RUST_LOG name        | What it covers                                |
| -------------------- | -------------------- | --------------------------------------------- |
| `fluree-db-query`    | `fluree_db_query`    | Query parsing, planning, execution, operators |
| `fluree-db-transact` | `fluree_db_transact` | Transaction parsing, staging, commit          |
| `fluree-db-core`     | `fluree_db_core`     | Core index operations, range scans, tracking  |
| `fluree-db-server`   | `fluree_db_server`   | HTTP server, routing, request handling        |
| `fluree-db-sparql`   | `fluree_db_sparql`   | SPARQL parsing and lowering                   |
| `fluree-db-api`      | `fluree_db_api`      | API orchestration, result formatting          |

## OTEL Environment Variables Reference

These environment variables configure OpenTelemetry trace export. They
require the server to be built with `--features otel`.

| Variable                      | Description                                                                                                     | Default                  |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `OTEL_SERVICE_NAME`           | Service name for trace exports (e.g., `fluree`, `fluree-prod`)                                                  | None (required for OTEL) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint URL (e.g., `http://localhost:4317`)                                                     | None (required for OTEL) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Transport protocol: `grpc` or `http/protobuf`                                                                   | `grpc`                   |
| `OTEL_TRACES_SAMPLER`         | Sampling strategy: `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off` | `always_on`              |
| `OTEL_TRACES_SAMPLER_ARG`     | Sampler argument (e.g., `0.01` for 1% sampling with `traceidratio`)                                             | `1.0`                    |

These logging variables work alongside OTEL configuration:

| Variable                | Description                                                                                                                                                            | Default                          |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `RUST_LOG`              | Log filter directive (highest priority). Uses `tracing` [EnvFilter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). | None (falls back to `LOG_LEVEL`) |
| `LOG_LEVEL`             | Fallback log level when `RUST_LOG` is not set                                                                                                                          | `info`                           |
| `LOG_FORMAT`            | Output format: `json` or `human`                                                                                                                                       | `human`                          |
| `LOG_SENSITIVE_DATA`    | Sensitive data handling: `off`, `mask`, or `hash`                                                                                                                      | `mask`                           |
| `LOG_QUERY_TEXT`        | Query text logging: `0` (off), `1`/`true`/`full`, or `hash`                                                                                                            | `0`                              |
| `LOG_REQUEST_ID_HEADER` | Custom request ID header name                                                                                                                                          | `x-request-id`                   |

## Related Documentation

- [Telemetry and Logging](../operations/telemetry.md) — full telemetry
  configuration reference
- [Configuration](../operations/configuration.md) — all server configuration
  options
- [Tracking and Fuel Limits](../query/tracking-and-fuel.md) — query tracking
  and fuel system
- [Debugging Queries](debugging-queries.md) — query explain plans and
  optimization
- [Explain Plans](../query/explain.md) — query execution plan analysis
