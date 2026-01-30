# OTEL Firehose Demo

Streams a large `.ttl` file as batched HTTP transactions to an OTEL-enabled
Fluree server while Jaeger captures the full request waterfall. One command
starts everything; when it finishes you browse traces in the Jaeger UI.

## Prerequisites

- **Docker** — for the Jaeger container
- **Rust toolchain** — to build the server and client
- **A `.ttl` file** — e.g. the Albania OSM dataset (`alb.osm.ttl`, ~11 GB)

## Quick start

```bash
./examples/otel-firehose-demo/run.sh ~/Downloads/open-street-map/alb.osm.ttl
```

This will:

1. Start a Jaeger container (OTLP on `:4317`, UI on `:16686`)
2. Build `fluree-db-server` with `--features otel` (release)
3. Build `otel-firehose-client` (release)
4. Start the server with OTEL export pointing at Jaeger
5. Stream batched transactions from the TTL file
6. Print the Jaeger URL and wait for you to finish investigating

## Environment overrides

All optional — pass as env vars before the script:

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `10000` | RDF statements per HTTP request |
| `INFLIGHT` | `8` | Max concurrent in-flight HTTP requests |
| `LEDGER` | `firehose-demo` | Ledger alias |
| `SERVER_PORT` | `8090` | Fluree server listen port |
| `STORAGE_DIR` | (tmpdir) | Persistent storage directory (temp by default) |
| `REINDEX_MIN_BYTES` | `10000000` (10 MB) | Novelty size that triggers background indexing |
| `REINDEX_MAX_BYTES` | `200000000` (200 MB) | Novelty size that blocks new commits |
| `OTEL_TRACES_SAMPLER` | `always_on` | Sampling strategy (see below) |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | Sampler argument (e.g. `0.01` for 1%) |
| `RUST_LOG` | `info,fluree_db_query=debug,fluree_db_transact=debug` | Server log filter |

Example with custom settings:

```bash
BATCH_SIZE=50000 INFLIGHT=4 LEDGER=osm-alb \
  ./examples/otel-firehose-demo/run.sh ~/Downloads/open-street-map/alb.osm.ttl
```

## What to look for in Jaeger

After the import runs (even a few batches is enough), open
[http://localhost:16686](http://localhost:16686):

1. **Select service** `fluree` and click **Find Traces**.
2. **Sort by duration** to find the slowest transactions.
3. **Open a trace** to see the transaction waterfall:

```
request
  └─ transact_execute (ledger_alias, txn_type)
       ├─ parse (input_format, txn_type)
       ├─ txn_stage
       │    ├─ insert_gen (template_count, assertion_count)
       │    ├─ cancellation (flakes_before, cancelled_count)
       │    └─ policy_enforce (flakes_checked)
       └─ commit (flake_count, bytes_written)
            ├─ storage_write (bytes_written)
            └─ ns_publish
```

4. **Identify the bottleneck** — is it `insert_gen` (flake generation),
   `storage_write` (disk I/O), or `commit` (overall)?
5. **For per-operator detail**, raise the log level:

```bash
RUST_LOG=info,fluree_db_query=trace,fluree_db_transact=trace \
  ./examples/otel-firehose-demo/run.sh data.ttl
```

See [Performance Investigation](../../docs/troubleshooting/performance-investigation.md)
for the full investigation guide.

## Sampling for large datasets

With `always_on` sampling (the default), every transaction produces a trace.
For large files (millions of statements), this generates significant trace
volume. Use ratio-based sampling to capture a representative subset:

```bash
OTEL_TRACES_SAMPLER=traceidratio OTEL_TRACES_SAMPLER_ARG=0.01 \
  ./examples/otel-firehose-demo/run.sh ~/Downloads/open-street-map/alb.osm.ttl
```

This samples ~1% of traces, which is usually sufficient for performance
investigation.

## Running components manually

If you prefer to control each piece separately:

### 1. Start Jaeger

```bash
docker run -d --name fluree-jaeger-demo \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/jaeger:latest
```

### 2. Build and start the server

```bash
cargo build --release -p fluree-db-server --features otel

OTEL_SERVICE_NAME=fluree \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_TRACES_SAMPLER=always_on \
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug \
  ./target/release/fluree-server --storage-path ./demo-data
```

### 3. Run the client

```bash
cargo build --release -p otel-firehose-client

./target/release/otel-firehose-client ~/Downloads/open-street-map/alb.osm.ttl \
  -b 10000 --inflight 8 -l osm-alb \
  --server-url http://localhost:8090
```

### 4. Cleanup

```bash
docker rm -f fluree-jaeger-demo
```

## How it differs from bulk-turtle-import

| Aspect | `bulk-turtle-import` | This demo |
|--------|----------------------|-----------|
| Fluree interface | Library API (`fluree-db-api`) | HTTP (`POST /fluree/insert`) |
| Tracing | Client-side only | Full server-side OTEL spans |
| Visualization | Log output | Jaeger waterfall UI |
| Server | None (embedded) | `fluree-db-server` (decoupled) |
| Use case | Raw ingest throughput | Performance investigation |
