# Admin, Health, and Stats

This document covers administrative operations, health monitoring, and server statistics for Fluree deployments.

## Health Endpoints

### GET /health

Basic health check:

```bash
curl http://localhost:8090/health
```

**Healthy Response (200 OK):**
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "file",
  "uptime_ms": 3600000
}
```

**Unhealthy Response (503 Service Unavailable):**
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
      "message": "Indexing lag exceeds threshold",
      "threshold": 100,
      "actual": 250
    }
  ]
}
```

### GET /ready

Readiness check for load balancers:

```bash
curl http://localhost:8090/ready
```

**Ready Response (200 OK):**
```json
{
  "ready": true
}
```

**Not Ready Response (503):**
```json
{
  "ready": false,
  "reason": "Initialization in progress"
}
```

### GET /live

Liveness check (is process alive?):

```bash
curl http://localhost:8090/live
```

Always returns 200 OK if process is running.

## Statistics Endpoints

### GET /status

Comprehensive server statistics:

```bash
curl http://localhost:8090/status
```

Response:
```json
{
  "version": "0.1.0",
  "uptime_ms": 3600000,
  "started_at": "2024-01-22T10:00:00Z",
  "storage": {
    "mode": "file",
    "total_bytes": 52428800,
    "ledgers": 5
  },
  "queries": {
    "total": 12345,
    "active": 3,
    "avg_duration_ms": 45,
    "errors": 12
  },
  "transactions": {
    "total": 567,
    "active": 1,
    "avg_duration_ms": 89,
    "errors": 3
  },
  "indexing": {
    "active": true,
    "pending_ledgers": 1,
    "avg_lag_transactions": 5,
    "last_index_duration_ms": 234
  },
  "memory": {
    "used_bytes": 536870912,
    "available_bytes": 4294967296
  }
}
```

### GET /metrics

Prometheus metrics:

```bash
curl http://localhost:8090/metrics
```

See [Telemetry](telemetry.md) for metrics details.

### GET /ledgers

List all ledgers:

```bash
curl http://localhost:8090/ledgers
```

Response:
```json
{
  "ledgers": [
    {
      "alias": "mydb:main",
      "commit_t": 150,
      "index_t": 145,
      "novelty_count": 5,
      "created": "2024-01-20T10:00:00Z",
      "last_updated": "2024-01-22T10:30:00Z",
      "storage_bytes": 10485760
    }
  ],
  "total": 1
}
```

### GET /ledgers/:alias

Get specific ledger details:

```bash
curl http://localhost:8090/ledgers/mydb:main
```

Response:
```json
{
  "alias": "mydb:main",
  "branch": "main",
  "commit_t": 150,
  "index_t": 145,
  "novelty_count": 5,
  "commit_address": "fluree:file:commit:abc123...",
  "index_address": "fluree:file:index:def456...",
  "created": "2024-01-20T10:00:00Z",
  "last_updated": "2024-01-22T10:30:00Z",
  "last_query": "2024-01-22T10:29:00Z",
  "last_transaction": "2024-01-22T10:30:00Z",
  "storage_bytes": 10485760,
  "index_bytes": 8388608,
  "transaction_count": 150,
  "query_count": 5432
}
```

## Administrative Operations

### POST /admin/index

Trigger manual indexing:

```bash
curl -X POST http://localhost:8090/admin/index?ledger=mydb:main
```

Response:
```json
{
  "ledger": "mydb:main",
  "status": "indexing",
  "target_t": 150,
  "started": "2024-01-22T10:30:00Z"
}
```

Use when:
- Forcing immediate indexing
- After bulk imports
- Before maintenance

### POST /admin/compact

Compact indexes and storage:

```bash
curl -X POST http://localhost:8090/admin/compact?ledger=mydb:main
```

Response:
```json
{
  "ledger": "mydb:main",
  "status": "compacting",
  "estimated_duration_ms": 30000
}
```

Benefits:
- Reclaims disk space
- Merges small index files
- Improves query performance

### POST /admin/rebuild-index

Rebuild index from scratch:

```bash
curl -X POST http://localhost:8090/admin/rebuild-index?ledger=mydb:main
```

Response:
```json
{
  "ledger": "mydb:main",
  "status": "rebuilding",
  "estimated_duration_ms": 60000
}
```

Use when:
- Index corruption
- Schema changes
- Performance issues

For advanced reindex options including checkpointing, progress monitoring, and resumable operations, see the [Reindex API](../indexing-and-search/reindex.md) documentation.

### POST /admin/maintenance-mode

Enable maintenance mode:

```bash
curl -X POST http://localhost:8090/admin/maintenance-mode
```

Response:
```json
{
  "maintenance": true,
  "message": "Server in maintenance mode - read-only"
}
```

While in maintenance:
- Queries allowed
- Transactions rejected
- Indexing paused

Disable:
```bash
curl -X POST http://localhost:8090/admin/maintenance-mode/off
```

### GET /admin/verify-index

Verify index integrity:

```bash
curl http://localhost:8090/admin/verify-index?ledger=mydb:main
```

Response:
```json
{
  "ledger": "mydb:main",
  "index_t": 145,
  "integrity": "valid",
  "checked_flakes": 1234567,
  "errors": []
}
```

If corruption found:
```json
{
  "integrity": "corrupted",
  "errors": [
    {
      "index": "SPOT",
      "issue": "Missing entry",
      "subject": "ex:alice"
    }
  ]
}
```

## Backup Operations

### POST /admin/backup

Create backup:

```bash
curl -X POST http://localhost:8090/admin/backup \
  -d '{"ledger": "mydb:main", "destination": "/backup/mydb-main.backup"}'
```

### POST /admin/restore

Restore from backup:

```bash
curl -X POST http://localhost:8090/admin/restore \
  -d '{"ledger": "mydb:main", "source": "/backup/mydb-main.backup"}'
```

## Performance Analysis

### GET /admin/query-stats

Query performance statistics:

```bash
curl http://localhost:8090/admin/query-stats
```

Response:
```json
{
  "total_queries": 12345,
  "avg_duration_ms": 45,
  "p50_duration_ms": 23,
  "p95_duration_ms": 120,
  "p99_duration_ms": 450,
  "slowest_queries": [
    {
      "duration_ms": 2345,
      "timestamp": "2024-01-22T10:25:00Z",
      "ledger": "mydb:main",
      "pattern": "Complex join query"
    }
  ]
}
```

### GET /admin/transaction-stats

Transaction statistics:

```bash
curl http://localhost:8090/admin/transaction-stats
```

Response:
```json
{
  "total_transactions": 567,
  "avg_duration_ms": 89,
  "avg_flakes_per_txn": 25,
  "largest_transactions": [
    {
      "t": 123,
      "flakes": 5432,
      "duration_ms": 567,
      "ledger": "mydb:main"
    }
  ]
}
```

### GET /admin/index-stats

Indexing statistics:

```bash
curl http://localhost:8090/admin/index-stats
```

Response:
```json
{
  "active_indexers": 1,
  "ledgers": [
    {
      "alias": "mydb:main",
      "commit_t": 150,
      "index_t": 145,
      "novelty": 5,
      "last_index_duration_ms": 234,
      "avg_index_duration_ms": 189,
      "index_rate_per_second": 2.1
    }
  ]
}
```

## Resource Management

### Memory Usage

Check memory usage:

```bash
curl http://localhost:8090/admin/memory
```

Response:
```json
{
  "total_bytes": 4294967296,
  "used_bytes": 536870912,
  "available_bytes": 3758096384,
  "breakdown": {
    "indexes": 268435456,
    "novelty": 67108864,
    "query_cache": 134217728,
    "other": 67108864
  }
}
```

### Storage Usage

Check storage usage:

```bash
curl http://localhost:8090/admin/storage
```

Response:
```json
{
  "mode": "file",
  "total_bytes": 52428800,
  "breakdown": {
    "commits": 10485760,
    "indexes": 31457280,
    "nameservice": 1048576,
    "virtual_graphs": 9437184
  },
  "ledgers": [
    {
      "alias": "mydb:main",
      "bytes": 20971520
    }
  ]
}
```

## Maintenance Operations

### Cleanup Old Indexes

Remove old index snapshots:

```bash
curl -X POST http://localhost:8090/admin/cleanup-indexes \
  -d '{"retain_count": 10}'
```

Keeps last 10 index snapshots per ledger.

### Vacuum Storage

Reclaim unused storage:

```bash
curl -X POST http://localhost:8090/admin/vacuum
```

### Reset Ledger

Delete all data from ledger:

```bash
curl -X DELETE http://localhost:8090/ledgers/mydb:main
```

Warning: This is destructive and permanent.

## Monitoring Best Practices

### 1. Use Health Checks

Configure load balancers and orchestrators to use health endpoints:

```yaml
# Kubernetes
livenessProbe:
  httpGet:
    path: /health
    port: 8090
readinessProbe:
  httpGet:
    path: /ready
    port: 8090
```

### 2. Track Key Metrics

Monitor critical metrics:
- Query latency (p95, p99)
- Transaction rate
- Indexing lag
- Error rate
- Storage growth

### 3. Set Up Alerts

Alert on anomalies:
- High latency
- High error rate
- High indexing lag
- Low disk space
- High memory usage

### 4. Regular Reports

Generate regular reports:
- Daily: Error summary
- Weekly: Performance trends
- Monthly: Capacity planning

### 5. Incident Response

Define runbooks for common issues:
- High latency → Check query patterns
- High indexing lag → Tune indexing settings
- Out of disk → Cleanup old indexes

## Related Documentation

- [Configuration](configuration.md) - Configuration options
- [Storage](storage.md) - Storage modes
- [Telemetry](telemetry.md) - Logging and metrics
- [Troubleshooting](../troubleshooting/README.md) - Common issues
