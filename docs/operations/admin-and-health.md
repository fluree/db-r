# Admin, Health, and Stats

This document covers administrative operations, health monitoring, and server statistics for Fluree deployments.

## Health Endpoints

### GET /health

Basic health check:

```bash
curl http://localhost:8090/health
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

Use this endpoint for:
- Load balancer health checks
- Container orchestration (Kubernetes liveness/readiness probes)
- Monitoring systems

**Kubernetes Example:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Statistics Endpoints

### GET /fluree/stats

Server statistics:

```bash
curl http://localhost:8090/fluree/stats
```

**Response:**
```json
{
  "uptime_secs": 3600,
  "storage_type": "file",
  "indexing_enabled": true,
  "cached_ledgers": 3,
  "version": "0.1.0"
}
```

| Field | Description |
|-------|-------------|
| `uptime_secs` | Server uptime in seconds |
| `storage_type` | Storage mode (`memory` or `file`) |
| `indexing_enabled` | Whether background indexing is enabled |
| `cached_ledgers` | Number of ledgers currently cached |
| `version` | Server version |

### GET /fluree/ledger-info

Get detailed ledger metadata:

```bash
curl "http://localhost:8090/fluree/ledger-info?ledger=mydb:main"
```

**Optional query params:**

- **`include_property_datatypes=true`**: include `stats.properties[*].datatypes` (datatype → count), **as-of last index**.
- **`realtime_property_details=true`**: merge novelty deltas into property datatype counts (real-time). This also enables `include_property_datatypes`.

Example:

```bash
curl "http://localhost:8090/fluree/ledger-info?ledger=mydb:main&realtime_property_details=true"
```

Or with header:
```bash
curl http://localhost:8090/fluree/ledger-info \
  -H "fluree-ledger: mydb:main"
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "t": 150,
  "commit": {
    "address": "fluree:file://mydb/main/commit/abc123...",
    "t": 150
  },
  "index": {
    "address": "fluree:file://mydb/main/index/def456...",
    "t": 145
  },
  "stats": {
    "flakes": 12345,
    "size": 1048576,
    "indexed": 145,
    "properties": {
      "ex:name": {
        "count": 3,
        "ndv-values": 3,
        "ndv-subjects": 3,
        "last-modified-t": 150,
        "selectivity-value": 1,
        "selectivity-subject": 1
      }
    },
    "classes": {
      "ex:Person": {
        "count": 2,
        "properties": {
          "ex:worksFor": {
            "count": 2,
            "refs": { "ex:Organization": 2 },
            "ref-classes": { "ex:Organization": 2 }
          },
          "ex:name": {}
        },
        "property-list": ["ex:name", "ex:worksFor"]
      }
    }
  }
}
```

#### Stats freshness (real-time vs indexed)

- **Real-time (includes novelty)**:
  - `commit` and top-level `t` reflect the latest committed head.
  - `stats.flakes` and `stats.size` are derived from the current ledger stats view (indexed + novelty deltas).
  - `stats.classes[*].properties` / `property-list` will include properties introduced in novelty, even when the update does not restate `@type`.
  - `stats.properties[*].datatypes` is real-time **only when** `realtime_property_details=true` is used.

- **As-of last index**:
  - `stats.indexed` is the last index \(t\). If `commit.t > indexed`, the index is behind the head.
  - NDV-related fields in `stats.properties[*]` (`ndv-values`, `ndv-subjects`) and selectivity derived from them are only as current as the last index refresh.
  - `stats.properties[*].datatypes` (when present via `include_property_datatypes=true`) is as-of last index unless `realtime_property_details=true` is used.
  - Class property ref-edge counts (`stats.classes[*].properties[*].refs`) are computed during indexing and represent the last indexed view (not novelty-adjusted).

### GET /fluree/exists

Check if a ledger exists:

```bash
curl "http://localhost:8090/fluree/exists?ledger=mydb:main"
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "exists": true
}
```

This is a lightweight check that only queries the nameservice without loading the ledger.

## Administrative Operations

### POST /fluree/create

Create a new ledger:

```bash
curl -X POST http://localhost:8090/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

**Response (201 Created):**
```json
{
  "ledger": "mydb:main",
  "t": 0,
  "tx-id": "fluree:tx:sha256:abc123...",
  "commit": {
    "address": "fluree:file://mydb/main/head",
    "hash": ""
  }
}
```

**Authentication:** When `--admin-auth-mode=required`, requires Bearer token from a trusted issuer.

See [Admin Authentication](../api/endpoints.md#admin-authentication) for details.

### POST /fluree/drop

Drop (delete) a ledger:

```bash
# Soft drop (retract from nameservice, preserve files)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'

# Hard drop (delete all files - IRREVERSIBLE)
curl -X POST http://localhost:8090/fluree/drop \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main", "hard": true}'
```

**Response:**
```json
{
  "ledger": "mydb:main",
  "status": "dropped",
  "files_deleted": 23
}
```

| Status | Description |
|--------|-------------|
| `dropped` | Successfully dropped |
| `already_retracted` | Was previously dropped |
| `not_found` | Ledger doesn't exist |

**Authentication:** When `--admin-auth-mode=required`, requires Bearer token from a trusted issuer.

**Drop Modes:**
- **Soft** (default): Retracts from nameservice, files remain (recoverable)
- **Hard**: Deletes all files (irreversible)

See [Dropping Ledgers](../getting-started/rust-api.md#dropping-ledgers) for more details.

## API Specification

### GET /swagger.json

OpenAPI specification:

```bash
curl http://localhost:8090/swagger.json
```

Returns the OpenAPI 3.0 specification for the server API.

## Monitoring Best Practices

### 1. Use Health Checks

Configure your infrastructure to poll `/health`:

```bash
# Simple monitoring script
while true; do
  curl -sf http://localhost:8090/health > /dev/null || echo "ALERT: Server unhealthy"
  sleep 10
done
```

### 2. Track Server Stats

Periodically collect statistics:

```bash
curl http://localhost:8090/fluree/stats | jq .
```

Key metrics to track:
- `uptime_secs`: Detect restarts
- `cached_ledgers`: Cache efficiency

### 3. Monitor Ledger Health

For each critical ledger:

```bash
curl "http://localhost:8090/fluree/ledger-info?ledger=mydb:main" | jq .
```

Watch for:
- Index lag (`commit.t` vs `index.t`)
- Unexpected state changes

### 4. Set Up Alerts

Alert conditions:
- Health check failures
- Server restarts (low uptime)
- High index lag

### 5. Log Analysis

Enable structured logging:

```bash
fluree-server --log-level info 2>&1 | jq .
```

Search for:
- `level: "error"` - Errors
- `level: "warn"` - Warnings
- Slow query patterns

## Security Considerations

### Protect Admin Endpoints

In production, enable admin authentication:

```bash
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

This protects `/fluree/create` and `/fluree/drop` from unauthorized access.

### Limit Endpoint Exposure

Consider network-level restrictions:
- Health endpoint: Available to load balancers
- Stats endpoint: Internal monitoring only
- Admin endpoints: Restricted access

### Audit Logging

Admin operations are logged. Monitor for:
- Ledger creation
- Ledger drops
- Authentication failures

## Related Documentation

- [Configuration](configuration.md) - Server configuration options
- [Query Peers](query-peers.md) - Distributed deployment
- [Telemetry](telemetry.md) - Logging configuration
- [API Endpoints](../api/endpoints.md) - Full endpoint reference
