# Performance Investigation with Distributed Tracing

Fluree includes deep instrumentation that decomposes every query, transaction, and indexing operation into a span waterfall visible in [Jaeger](https://www.jaegertracing.io/), [Grafana Tempo](https://grafana.com/oss/tempo/), AWS X-Ray, or any OpenTelemetry-compatible backend. This guide explains how to use that instrumentation to find and fix performance bottlenecks.

## When to Use Deep Tracing

| Symptom | Start with | Escalate to |
|---------|-----------|-------------|
| Single slow query | `X-Fluree-Explain: true` header | Deep tracing at `debug` level |
| Slow queries in general, unclear which phase | Deep tracing at `debug` level | `trace` level for operator detail |
| Slow transactions / commits | Deep tracing at `debug` level | Check `txn_commit` sub-spans |
| Indexing taking too long | Deep tracing (indexer spans always at `info`) | `debug` for `resolve_commit` detail |
| Intermittent latency spikes | Sustained tracing + Jaeger search by duration | Correlate with indexing traces |
| Production regression | Compare Jaeger traces before/after deploy | Filter by `tracker_time` span attribute |

Deep tracing is complementary to `X-Fluree-Explain`, not a replacement. Explain plans show the *shape* of a query plan; tracing shows *where wall-clock time actually went*.

## Quick Start: Local Investigation

The `otel/` directory at the repository root provides a self-contained Makefile-driven harness for local trace investigation.

### Prerequisites

- Docker (for Jaeger)
- Rust toolchain
- curl, bash

### One-liner setup

```bash
cd otel/
make all    # starts Jaeger, builds with --features otel, starts server, runs smoke tests
make ui     # opens Jaeger UI in browser
```

This gives you a running Fluree server exporting traces to a local Jaeger instance with pre-loaded test data.

### Investigate a specific query

Once the server is running (via `make server` or `make all`):

```bash
# Run your problematic query against the server
curl -s -X POST http://localhost:8090/otel-test:main/query \
  -H 'Content-Type: application/json' \
  -d '{
    "select": ["?name", "?price"],
    "where": [
      {"@id": "?p", "@type": "ex:Product"},
      {"@id": "?p", "ex:name": "?name"},
      {"@id": "?p", "ex:price": "?price"}
    ],
    "orderBy": [{"desc": "?price"}],
    "limit": 100
  }'
```

Then open Jaeger (`make ui` or `http://localhost:16686`), select service `fluree-server`, and find the trace. The waterfall shows exactly where time was spent.

### Teardown

```bash
make clean-all   # stops server, stops Jaeger, removes data
```

## Writing Custom Scenario Scripts

The `otel/scripts/` directory contains scenario scripts you can use as templates. To investigate a specific performance issue:

### 1. Create a scenario script

```bash
#!/usr/bin/env bash
# otel/scripts/my-investigation.sh
set -euo pipefail

PORT="${PORT:-8090}"
LEDGER="${LEDGER:-otel-test:main}"
BASE="http://localhost:${PORT}"

echo "=== My investigation scenario ==="

# Step 1: Insert data that triggers the problem
curl -sf -X POST "${BASE}/${LEDGER}/insert" \
  -H 'Content-Type: application/json' \
  -d '{
    "@context": {"ex": "http://example.org/ns/"},
    "@graph": [
      ... your test data ...
    ]
  }' > /dev/null

sleep 0.5  # let OTEL batch exporter flush

# Step 2: Run the problematic query multiple times
for i in $(seq 1 5); do
  echo "  Query iteration $i..."
  curl -sf -X POST "${BASE}/${LEDGER}/query" \
    -H 'Content-Type: application/json' \
    -d '{ ... your query ... }' > /dev/null
  sleep 0.3
done

echo "=== Done. Check Jaeger for traces. ==="
```

### 2. Run it

```bash
cd otel/
make up build server          # ensure infrastructure is running
bash scripts/my-investigation.sh
make ui                        # inspect traces
```

### 3. Add a Makefile target (optional)

```makefile
# In otel/Makefile
my-investigation: _data/storage
	bash scripts/my-investigation.sh
```

### Tips for effective scenario scripts

- **Pause between requests** (`sleep 0.3-0.5`) to let the OTEL batch exporter flush. Without this, spans from adjacent requests may interleave in Jaeger, making waterfall analysis harder.
- **Run the query multiple times** to see variance. Sort by duration in Jaeger to find the worst case.
- **Use different `RUST_LOG` levels** for different investigations. Override when starting the server: `make server RUST_LOG=info,fluree_db_query=trace`
- **Isolate variables**: test with and without indexing (`INDEXING=false`), with different data volumes, or with different query patterns.

## Reading Jaeger Waterfalls

### Anatomy of a query trace

```
request (info)                              ─────────────────────────── 834ms
  query_execute (info)                      ─────────────────────────── 832ms
    query_prepare (debug)                   ──── 12ms
      reasoning_prep (debug)                ── 3ms
      pattern_rewrite (debug)               ── 2ms
      plan (debug)                          ── 5ms
    query_run (info)                        ──────────────────────── 818ms
      scan (trace)                          ── 4ms
      join (trace)                          ─────────────────── 780ms
        join_flush_scan_spot (info)         ────────────────── 775ms
      filter (trace)                        ── 2ms
      sort (trace)                          ── 15ms
        sort_blocking (info)                ── 14ms
      project (trace)                       ── 1ms
    format (debug)                          ── 2ms
```

In this example, the bottleneck is immediately visible: the `join_flush_scan_spot` span accounts for 775ms of the 834ms total. This tells you the query is doing a large range scan during the join phase.

### Key span attributes to check

| Span | Attribute | What it tells you |
|------|-----------|-------------------|
| `query_execute` | `tracker_time`, `tracker_fuel` | Total tracked time and fuel consumption |
| `pattern_rewrite` | `patterns_before`, `patterns_after` | Whether pattern rewriting is effective |
| `plan` | `pattern_count` | Complexity of the query plan |
| `scan` | (trace level) | How long individual scans take |
| `join_flush_scan_spot` | `unique_subjects`, `total_leaves` | Join scan size — large values indicate broad scans |
| `sort_blocking` | `input_rows`, `sort_ms` | Sort cost — are you sorting a huge result set? |
| `txn_stage` | `insert_count`, `delete_count` | Transaction size |
| `txn_commit` | `flake_count`, `delta_bytes` | Commit I/O volume |

### Anatomy of a transaction trace

```
request (info)                              ─────────────── 245ms
  transact_execute (info)                   ─────────────── 243ms
    txn_stage (info)                        ────── 45ms
      where_exec (debug)                    ── 8ms
      delete_gen (debug)                    ── 3ms
      insert_gen (debug)                    ── 12ms
      cancellation (debug)                  ── 5ms
      policy_enforce (debug)                ── 2ms
    txn_commit (info)                       ──────────── 195ms
      commit_nameservice_lookup (info)      ── 2ms
      commit_verify_sequencing (info)       ── 1ms
      commit_write_raw_txn (info)           ────── 85ms
      commit_build_record (info)            ── 3ms
      commit_write_commit_blob (info)       ────── 65ms
      commit_publish_nameservice (info)     ────── 35ms
```

Here the bottleneck is I/O during commit: `commit_write_raw_txn` (85ms) and `commit_write_commit_blob` (65ms). On AWS S3 storage, this is expected; on local file storage, it may indicate disk contention.

### Anatomy of an indexing trace

Indexing runs as a **separate trace** (not nested under an HTTP request). Search Jaeger for operation name `index_build`:

```
index_build (info)                          ─────────────────── 12.5s
  build_all_indexes (info)                  ─────────────────── 12.4s
    build_index (info, order=SPOT)          ──────── 3.1s
    build_index (info, order=PSOT)          ──────── 3.2s
    build_index (info, order=POST)          ──────── 3.0s
    build_index (info, order=OPST)          ──────── 3.1s
  walk_commit_chain (info)                  ── 50ms
    resolve_commit (debug, t=42)            ── 15ms
    resolve_commit (debug, t=41)            ── 12ms
```

## Common Bottleneck Patterns

### 1. Join scan too broad

**Symptom:** `join_flush_scan_spot` has `unique_subjects` in the thousands and dominates the waterfall.

**Cause:** A join pattern that matches too many subjects, forcing a large range scan.

**Fix:** Add more selective patterns or filters to narrow the join. Check the explain plan for join order.

### 2. Sort on large result set

**Symptom:** `sort_blocking` shows `input_rows` > 10,000 and `sort_ms` dominates.

**Cause:** Sorting happens after all joins/filters, on the full result set.

**Fix:** Add `LIMIT` if possible, or ensure filters run before the sort by placing restrictive patterns first.

### 3. Commit I/O on S3

**Symptom:** `commit_write_raw_txn` and `commit_write_commit_blob` each take 50-200ms.

**Cause:** S3 PutObject latency (~50-100ms per call).

**Fix:** This is inherent to S3. Batch multiple small transactions into fewer larger ones. Consider file storage for latency-sensitive workloads.

### 4. Indexing backlog

**Symptom:** Multiple `index_build` traces in quick succession, each taking 10+ seconds.

**Cause:** Transaction volume exceeds indexing throughput, building up novelty.

**Fix:** Increase the novelty reindex threshold, or reduce transaction frequency. Check `build_index` sub-spans to see which index order is slowest.

### 5. Policy evaluation overhead

**Symptom:** `policy_eval` or `policy_enforce` takes a significant fraction of query/transaction time.

**Cause:** Complex policy rules that require additional queries to evaluate.

**Fix:** Simplify policy rules, or pre-compute policy decisions where possible.

## Controlling Trace Verbosity

### RUST_LOG patterns

| Goal | Pattern | Visible spans |
|------|---------|---------------|
| Production default | `info` | Top-level operations, commit sub-steps, join/sort blocking |
| Query investigation | `info,fluree_db_query=debug` | + parse, plan, rewrite, format, policy_eval |
| Transaction investigation | `info,fluree_db_transact=debug` | + where_exec, delete_gen, insert_gen, cancellation |
| Full debug | `info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug` | All debug spans |
| Operator-level detail | `info,fluree_db_query=trace` | + per-operator: scan, join, filter, sort, project, etc. |
| Everything | `debug` | Console firehose (OTEL layer still filters to `fluree_*` only) |

### With the otel/ harness

```bash
# Override RUST_LOG when starting the server
make server RUST_LOG='info,fluree_db_query=trace'

# Then run your scenario
make query
```

### In production

Set `RUST_LOG` via your container orchestrator's environment variables. Start at `info` and increase selectively:

```bash
# ECS task definition (environment section)
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug
```

## Production Tracing: AWS Deployments

### Architecture: fluree-db-server on ECS/Fargate

```
┌─────────────┐     OTLP/gRPC (4317)     ┌───────────────────┐
│  ECS Task   │ ─────────────────────────▶│  OTEL Collector   │
│  fluree-srv │                           │  (sidecar or      │
│  --features │                           │   Daemon/Service) │
│    otel     │                           └────────┬──────────┘
└─────────────┘                                    │
                                         ┌─────────▼──────────┐
                                         │  Grafana Tempo /    │
                                         │  AWS X-Ray /        │
                                         │  Jaeger             │
                                         └─────────────────────┘
```

**ECS task definition snippet:**

```json
{
  "containerDefinitions": [
    {
      "name": "fluree-server",
      "image": "your-ecr-repo/fluree-db-server:latest",
      "environment": [
        {"name": "RUST_LOG", "value": "info,fluree_db_query=debug,fluree_db_transact=debug"},
        {"name": "OTEL_SERVICE_NAME", "value": "fluree-server"},
        {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://localhost:4317"},
        {"name": "OTEL_EXPORTER_OTLP_PROTOCOL", "value": "grpc"}
      ]
    },
    {
      "name": "otel-collector",
      "image": "amazon/aws-otel-collector:latest",
      "essential": true,
      "command": ["--config=/etc/otel-config.yaml"]
    }
  ]
}
```

**OTEL Collector config (for X-Ray export):**

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  awsxray:
    region: us-east-1
  # Or for Grafana Tempo:
  # otlp:
  #   endpoint: tempo.internal:4317
  #   tls:
  #     insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [awsxray]
```

### Architecture: fluree-db-api as a Rust crate in AWS Lambda

When using `fluree-db-api` directly (not through fluree-db-server), you initialize OTEL yourself. The key pattern is the same dual-layer subscriber with a `Targets` filter on the OTEL layer.

```rust
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, Registry};

fn init_tracing() {
    // OTEL exporter — Lambda uses HTTP/protobuf to the collector sidecar
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint("http://localhost:4318")  // collector sidecar
        .build()
        .expect("Failed to create OTLP exporter");

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("my-lambda-fn")
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("fluree-db");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Critical: filter OTEL layer to fluree_* crates only
    let otel_filter = Targets::new()
        .with_target("fluree_db_api", tracing::Level::DEBUG)
        .with_target("fluree_db_query", tracing::Level::DEBUG)
        .with_target("fluree_db_transact", tracing::Level::DEBUG)
        .with_target("fluree_db_indexer", tracing::Level::DEBUG)
        .with_target("fluree_db_core", tracing::Level::DEBUG);

    let subscriber = Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer.with_filter(otel_filter));

    tracing::subscriber::set_global_default(subscriber).ok();
}
```

**Lambda deployment with ADOT (AWS Distro for OpenTelemetry):**

Add the ADOT Lambda layer and set:

```bash
AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-handler
OTEL_SERVICE_NAME=my-fluree-lambda
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

The ADOT layer runs a collector sidecar that receives OTLP spans and exports them to X-Ray, Tempo, or any configured backend.

### Grafana Tempo + Grafana UI

For production trace exploration, Grafana Tempo with the Grafana UI provides the best experience:

1. **Search by attributes**: Find all queries with `tracker_time > 500ms`
2. **Service graph**: Visualize call patterns between services
3. **Trace-to-logs**: Jump from a slow span to the corresponding log lines
4. **Trace-to-metrics**: Correlate latency spikes with metric dashboards

**Tempo query examples (TraceQL):**

```
# Find slow queries
{ resource.service.name = "fluree-server" && name = "query_execute" && duration > 500ms }

# Find large commits
{ name = "txn_commit" && span.flake_count > 1000 }

# Find indexing operations
{ name = "index_build" }
```

### AWS X-Ray

X-Ray works with OTEL traces exported via the AWS OTEL Collector. Key differences from Jaeger/Tempo:

- X-Ray automatically creates a **service map** showing request flow
- Subsegment annotations map to OTEL span attributes
- X-Ray sampling rules can be configured server-side (no code changes)
- Use **X-Ray Insights** for anomaly detection on latency patterns

## Using the otel/ Harness for Regression Testing

The `otel/` directory is designed for reproducible trace validation. Use it to verify that tracing instrumentation works correctly after code changes:

```bash
cd otel/

# Clean slate
make fresh

# After all scenarios complete, check Jaeger:
# 1. Transaction traces should show txn_stage > txn_commit with sub-spans
# 2. Query traces should show query_prepare > query_run with operator spans
# 3. Index traces should appear as separate traces (not under a request)
```

### Specific test scenarios

| Scenario | Command | What to verify in Jaeger |
|----------|---------|-------------------------|
| All transaction types | `make transact` | 5 traces, each with `txn_stage` + `txn_commit` |
| All query types | `make query` | 7 traces with `query_prepare` + `query_run` |
| Background indexing | `make index` | Separate `index_build` trace (not under a request) |
| Bulk import | `make firehose` | Many commit traces, possibly indexing traces |
| Full end-to-end | `make smoke` | All of the above |
| Multi-cycle stress | `make cycle` | 3 full cycles, multiple `index_build` traces |

## Related Documentation

- [Telemetry and Logging](../operations/telemetry.md) -- OTEL configuration reference
- [Adding Tracing Spans](../contributing/tracing-guide.md) -- How to instrument new code paths
- [Debugging Queries](debugging-queries.md) -- Query-specific debugging (explain plans, etc.)
- [otel/README.md](../../otel/README.md) -- Full otel/ harness reference
