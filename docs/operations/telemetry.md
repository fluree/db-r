# Telemetry and Logging

Fluree provides comprehensive logging, metrics, and tracing capabilities for monitoring and debugging production deployments.

## Logging

### Log Levels

Configure log verbosity:

```bash
--log-level error|warn|info|debug|trace
```

**error:** Critical errors only
**warn:** Warnings and errors
**info:** Informational messages (default)
**debug:** Detailed debugging information
**trace:** Very detailed tracing

### Log Formats

#### JSON Format (Recommended)

```bash
--log-format json
```

Output:
```json
{
  "timestamp": "2024-01-22T10:30:00.123Z",
  "level": "INFO",
  "target": "fluree_db_server",
  "message": "Transaction committed",
  "fields": {
    "ledger": "mydb:main",
    "t": 42,
    "duration_ms": 45,
    "flakes_added": 3
  }
}
```

Benefits:
- Machine-parseable
- Easy to index (Elasticsearch, etc.)
- Structured fields
- JSON query tools work

#### Text Format

```bash
--log-format text
```

Output:
```text
2024-01-22T10:30:00.123Z INFO  fluree_db_server] Transaction committed ledger=mydb:main t=42 duration_ms=45
```

Benefits:
- Human-readable
- Compact
- Easy to grep

### Log Output

#### Standard Output (Default)

```bash
./fluree-db-server
```

Logs to stdout/stderr.

#### Log File

```bash
--log-file /var/log/fluree/server.log
```

```toml
[logging]
file = "/var/log/fluree/server.log"
```

#### Log Rotation

Use logrotate:

```bash
# /etc/logrotate.d/fluree
/var/log/fluree/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0644 fluree fluree
    sharedscripts
    postrotate
        systemctl reload fluree
    endscript
}
```

### Structured Logging

Add context to logs:

```rust
// Rust code (for reference)
info!(
    ledger = %ledger,
    t = transaction_time,
    duration_ms = duration.as_millis(),
    "Transaction committed"
);
```

Output:
```json
{
  "message": "Transaction committed",
  "ledger": "mydb:main",
  "t": 42,
  "duration_ms": 45
}
```

## Metrics

### Prometheus Metrics

Fluree exposes Prometheus-compatible metrics:

```bash
curl http://localhost:8090/metrics
```

Output:
```text
# HELP fluree_transactions_total Total number of transactions
# TYPE fluree_transactions_total counter
fluree_transactions_total{ledger="mydb:main"} 567

# HELP fluree_queries_total Total number of queries
# TYPE fluree_queries_total counter
fluree_queries_total 12345

# HELP fluree_query_duration_seconds Query execution duration
# TYPE fluree_query_duration_seconds histogram
fluree_query_duration_seconds_bucket{le="0.01"} 8234
fluree_query_duration_seconds_bucket{le="0.05"} 11890
fluree_query_duration_seconds_bucket{le="0.1"} 12100
fluree_query_duration_seconds_sum 556.789
fluree_query_duration_seconds_count 12345

# HELP fluree_indexing_lag_transactions Indexing lag in transactions
# TYPE fluree_indexing_lag_transactions gauge
fluree_indexing_lag_transactions{ledger="mydb:main"} 5
```

### Available Metrics

**Transaction Metrics:**
- `fluree_transactions_total` - Total transactions
- `fluree_transaction_duration_seconds` - Transaction latency
- `fluree_flakes_added_total` - Total flakes added
- `fluree_flakes_retracted_total` - Total flakes retracted

**Query Metrics:**
- `fluree_queries_total` - Total queries
- `fluree_query_duration_seconds` - Query latency
- `fluree_query_errors_total` - Query errors

**Indexing Metrics:**
- `fluree_indexing_lag_transactions` - Novelty count
- `fluree_index_duration_seconds` - Indexing time
- `fluree_index_size_bytes` - Index size

**System Metrics:**
- `fluree_uptime_seconds` - Server uptime
- `fluree_memory_used_bytes` - Memory usage
- `fluree_storage_used_bytes` - Storage usage

### Prometheus Integration

Configure Prometheus to scrape Fluree:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'fluree'
    static_configs:
      - targets: ['localhost:8090']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

## Tracing

### Distributed Tracing

Enable OpenTelemetry tracing:

```bash
./fluree-db-server \
  --tracing-enabled true \
  --tracing-endpoint http://localhost:4317
```

```toml
[tracing]
enabled = true
endpoint = "http://localhost:4317"
service_name = "fluree-db-server"
```

### Trace Context

Fluree propagates trace context:

```bash
curl -X POST http://localhost:8090/query \
  -H "traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
  -d '{...}'
```

### Jaeger Integration

Export traces to Jaeger:

```bash
# Run Jaeger
docker run -d -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one

# Configure Fluree
./fluree-db-server \
  --tracing-enabled true \
  --tracing-endpoint http://localhost:4317
```

View traces: http://localhost:16686

### Span Hierarchy

Fluree instruments queries, transactions, and indexing with structured tracing spans at three levels. All debug/trace spans are opt-in via `RUST_LOG` — at default `info` level, only the top-level operation spans appear.

#### Investigation Tiers

**Tier 1: Default (INFO)** — Always visible. Shows top-level operations and timing.

```bash
RUST_LOG=info  # default
```

Spans: `query_run`, `txn_stage`, `stage_flakes`, `txn_commit`, `index_build`, `sort_blocking`, `groupby_blocking`, `join_flush_*`

**Tier 2: Debug** — Phase-level decomposition of queries and transactions. Use when you need to identify which phase is the bottleneck.

```bash
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug
```

Additional spans:
- **Query path:** `query_prepare` > [`reasoning_prep`, `pattern_rewrite`, `plan`], `parse`, `format`, `policy_eval`
- **Transaction path:** `txn_stage` > [`where_exec`, `delete_gen`, `insert_gen`, `cancellation`, `policy_enforce`]
- **Indexer:** `resolve_commit`, `index_gc`

**Tier 3: Trace** — Per-operator detail. Use for deep performance analysis.

```bash
RUST_LOG=info,fluree_db_query=trace
```

Additional spans: `scan`, `join`, `property_join`, `sort`, `group_by`, `aggregate`, `group_aggregate`, `distinct`, `limit`, `offset`, `project`, `filter`, `union`, `optional`, `subquery`, `having`

#### Span Tree (Query)

```
query_prepare (debug)
├── reasoning_prep (debug)
├── pattern_rewrite (debug)
└── plan (debug)
query_run (info)
├── scan (trace)
├── join (trace)
├── project (trace)
├── sort_blocking (info)
└── ...
```

#### Span Tree (Transaction)

```
txn_stage (info)
├── where_exec (debug)
├── delete_gen (debug)
├── insert_gen (debug)
├── cancellation (debug)
└── policy_enforce (debug)
txn_commit (info)
├── commit_nameservice_lookup (info)
├── commit_verify_sequencing (info)
├── commit_write_raw_txn (info)
├── commit_build_record (info)
├── commit_write_commit_blob (info)
└── commit_publish_nameservice (info)
```

#### Span Tree (Indexing)

```
index_build (info)
├── build_all_indexes (info)
│   └── build_index (info, per order)
├── generate_runs (info)
├── walk_commit_chain (info)
│   └── resolve_commit (debug)
└── index_gc (debug)
```

### OTEL Configuration

The OTEL exporter layer uses a separate `Targets` filter from the console fmt layer. This prevents third-party crate spans (hyper, tonic, h2, tower-http) from flooding the OTEL exporter when `RUST_LOG` is set broadly.

- **OTEL layer:** Exports only `fluree_*` crate targets at DEBUG level
- **Console layer:** Respects `RUST_LOG` environment variable as-is

This means `RUST_LOG=debug` will produce verbose console output from all crates, but the OTEL exporter only receives Fluree spans.

### Tracker-to-Span Bridge

When tracked queries or transactions are executed (via the `/query` or `/transact` HTTP endpoints with tracking enabled), the `tracker_time` and `tracker_fuel` fields are recorded on the `query_execute` and `transact_execute` spans. These values appear as span attributes in OTEL backends (Jaeger, Tempo, etc.), enabling correlation between the tracker's fuel accounting and the span waterfall.

## Monitoring Integration

### Grafana Dashboards

Import Fluree dashboard:

```json
{
  "dashboard": {
    "title": "Fluree Monitoring",
    "panels": [
      {
        "title": "Query Rate",
        "targets": [
          {
            "expr": "rate(fluree_queries_total[5m])"
          }
        ]
      },
      {
        "title": "Query Latency (p95)",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, fluree_query_duration_seconds)"
          }
        ]
      },
      {
        "title": "Indexing Lag",
        "targets": [
          {
            "expr": "fluree_indexing_lag_transactions"
          }
        ]
      }
    ]
  }
}
```

### Datadog Integration

Send logs to Datadog:

```bash
./fluree-db-server \
  --log-format json | \
  datadog-agent stream --service=fluree
```

### New Relic Integration

Use New Relic agent:

```bash
export NEW_RELIC_LICENSE_KEY=your-key
export NEW_RELIC_APP_NAME=fluree-prod

./fluree-db-server
```

### Elasticsearch/Kibana

Ship logs to Elasticsearch:

```bash
./fluree-db-server \
  --log-format json | \
  filebeat -e -c filebeat.yml
```

Filebeat config:
```yaml
filebeat.inputs:
  - type: stdin
    json.keys_under_root: true

output.elasticsearch:
  hosts: ["localhost:9200"]
  index: "fluree-logs-%{+yyyy.MM.dd}"
```

## Health Monitoring

### Health Check Endpoint

```bash
curl http://localhost:8090/health
```

Response (healthy):
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "file",
  "uptime_ms": 3600000,
  "checks": {
    "storage": "healthy",
    "indexing": "healthy",
    "nameservice": "healthy"
  }
}
```

Response (unhealthy):
```json
{
  "status": "unhealthy",
  "checks": {
    "storage": "healthy",
    "indexing": "unhealthy",
    "nameservice": "healthy"
  },
  "errors": [
    {
      "component": "indexing",
      "message": "Indexing lag exceeds threshold"
    }
  ]
}
```

### Liveness Probe

For Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8090
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
```

## Alerting

### Alert Rules

Prometheus alert rules:

```yaml
groups:
  - name: fluree
    rules:
      - alert: HighQueryLatency
        expr: histogram_quantile(0.95, fluree_query_duration_seconds) > 1
        for: 5m
        annotations:
          summary: "High query latency"
          description: "95th percentile query latency is {{ $value }}s"
      
      - alert: HighIndexingLag
        expr: fluree_indexing_lag_transactions > 100
        for: 10m
        annotations:
          summary: "High indexing lag"
          description: "Indexing lag is {{ $value }} transactions"
      
      - alert: HighErrorRate
        expr: rate(fluree_query_errors_total[5m]) > 10
        for: 5m
        annotations:
          summary: "High query error rate"
          description: "Error rate is {{ $value }}/s"
```

### Alert Destinations

Configure alert routing:

```yaml
route:
  receiver: 'team-ops'
  group_by: ['alertname', 'ledger']
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
    - match:
        severity: warning
      receiver: 'slack'

receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'your-key'
  
  - name: 'slack'
    slack_configs:
      - api_url: 'https://hooks.slack.com/...'
        channel: '#alerts'
```

## Performance Monitoring

### Key Metrics to Track

1. **Query Performance:**
   - p50, p95, p99 latency
   - Queries per second
   - Error rate

2. **Transaction Performance:**
   - Commit time
   - Transactions per second
   - Error rate

3. **Indexing:**
   - Novelty count
   - Index time
   - Indexing lag

4. **Resource Usage:**
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network I/O

5. **Storage:**
   - Storage used
   - Storage growth rate
   - S3 request rate (if AWS)

### Dashboards

Create operational dashboards:

**Overview Dashboard:**
- Request rate
- Error rate
- Response times
- Active connections

**Performance Dashboard:**
- Query latency percentiles
- Transaction latency
- Indexing performance
- Resource utilization

**Capacity Dashboard:**
- Storage usage and growth
- Memory usage trends
- Indexing lag trends
- Projection to capacity limits

## Logging Best Practices

### 1. Use Structured Logging

JSON format with consistent fields:

```json
{
  "timestamp": "2024-01-22T10:30:00Z",
  "level": "INFO",
  "ledger": "mydb:main",
  "operation": "query",
  "duration_ms": 45
}
```

### 2. Log Request IDs

Include request IDs for tracing:

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Request-ID: abc-123-def-456" \
  -d '{...}'
```

### 3. Appropriate Log Levels

- Production: `info`
- Debugging: `debug`
- Development: `debug` or `trace`

### 4. Sample High-Volume Logs

For high-traffic deployments, sample logs:

```toml
[logging]
sample_rate = 0.1  # Log 10% of requests
```

### 5. Sensitive Data

Never log sensitive data:
- API keys
- Passwords
- Personal information
- Financial data

## Related Documentation

- [Configuration](configuration.md) - Configuration options
- [Admin and Health](admin-and-health.md) - Health monitoring
- [Troubleshooting](../troubleshooting/README.md) - Debugging guides
