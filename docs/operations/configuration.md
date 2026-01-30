# Configuration

Fluree server is configured via command-line flags and environment variables.

## Configuration Methods

### Command-Line Flags

```bash
fluree-server \
  --listen-addr 0.0.0.0:8090 \
  --storage-path /var/lib/fluree \
  --log-level info
```

### Environment Variables

All CLI flags have corresponding environment variables with `FLUREE_` prefix:

```bash
export FLUREE_LISTEN_ADDR=0.0.0.0:8090
export FLUREE_STORAGE_PATH=/var/lib/fluree
export FLUREE_LOG_LEVEL=info

fluree-server
```

### Precedence

Configuration precedence (highest to lowest):
1. Command-line flags
2. Environment variables
3. Default values

## Server Configuration

### Listen Address

Address and port to bind to:

| Flag | Env Var | Default |
|------|---------|---------|
| `--listen-addr` | `FLUREE_LISTEN_ADDR` | `0.0.0.0:8090` |

```bash
fluree-server --listen-addr 0.0.0.0:9090
```

### Storage Path

Path for file-based storage. If not specified, uses in-memory storage:

| Flag | Env Var | Default |
|------|---------|---------|
| `--storage-path` | `FLUREE_STORAGE_PATH` | None (memory) |

```bash
# File storage
fluree-server --storage-path /var/lib/fluree

# Memory storage (default - omit --storage-path)
fluree-server
```

### CORS

Enable Cross-Origin Resource Sharing:

| Flag | Env Var | Default |
|------|---------|---------|
| `--cors-enabled` | `FLUREE_CORS_ENABLED` | `true` |

When enabled, allows requests from any origin.

### Body Limit

Maximum request body size in bytes:

| Flag | Env Var | Default |
|------|---------|---------|
| `--body-limit` | `FLUREE_BODY_LIMIT` | `52428800` (50MB) |

### Log Level

Logging verbosity:

| Flag | Env Var | Default |
|------|---------|---------|
| `--log-level` | `FLUREE_LOG_LEVEL` | `info` |

Options: `trace`, `debug`, `info`, `warn`, `error`

### Cache Size

Maximum cache entries per ledger:

| Flag | Env Var | Default |
|------|---------|---------|
| `--cache-max-entries` | `FLUREE_CACHE_MAX_ENTRIES` | `10000` |

### Background Indexing

Enable background indexing:

| Flag | Env Var | Default |
|------|---------|---------|
| `--indexing-enabled` | `FLUREE_INDEXING_ENABLED` | `false` |

## Server Role Configuration

### Server Role

Operating mode: transaction server or query peer:

| Flag | Env Var | Default |
|------|---------|---------|
| `--server-role` | `FLUREE_SERVER_ROLE` | `transaction` |

Options:
- `transaction`: Write-enabled, produces events stream
- `peer`: Read-only, subscribes to transaction server

### Transaction Server URL (Peer Mode)

Base URL of the transaction server (required in peer mode):

| Flag | Env Var |
|------|---------|
| `--tx-server-url` | `FLUREE_TX_SERVER_URL` |

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090
```

## Authentication Configuration

### Events Endpoint Authentication

Protect the `/fluree/events` SSE endpoint:

| Flag | Env Var | Default |
|------|---------|---------|
| `--events-auth-mode` | `FLUREE_EVENTS_AUTH_MODE` | `none` |
| `--events-auth-audience` | `FLUREE_EVENTS_AUTH_AUDIENCE` | None |
| `--events-auth-trusted-issuer` | `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS` | None |

Modes:
- `none`: No authentication
- `optional`: Accept tokens but don't require them
- `required`: Require valid Bearer token

```bash
fluree-server \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk...
```

### Admin Endpoint Authentication

Protect `/fluree/create` and `/fluree/drop` endpoints:

| Flag | Env Var | Default |
|------|---------|---------|
| `--admin-auth-mode` | `FLUREE_ADMIN_AUTH_MODE` | `none` |
| `--admin-auth-trusted-issuer` | `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | None |

Modes:
- `none`: No authentication (development)
- `required`: Require valid Bearer token (production)

```bash
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

If no admin-specific issuers are configured, falls back to `--events-auth-trusted-issuer`.

### MCP Endpoint Authentication

Protect the `/mcp` Model Context Protocol endpoint:

| Flag | Env Var | Default |
|------|---------|---------|
| `--mcp-enabled` | `FLUREE_MCP_ENABLED` | `false` |
| `--mcp-auth-trusted-issuer` | `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | None |

```bash
fluree-server \
  --mcp-enabled \
  --mcp-auth-trusted-issuer did:key:z6Mk...
```

## Peer Mode Configuration

### Peer Subscription

Configure what the peer subscribes to:

| Flag | Description |
|------|-------------|
| `--peer-subscribe-all` | Subscribe to all ledgers and VGs |
| `--peer-ledger <alias>` | Subscribe to specific ledger (repeatable) |
| `--peer-vg <alias>` | Subscribe to specific VG (repeatable) |

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx:8090 \
  --peer-subscribe-all
```

Or subscribe to specific resources:

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx:8090 \
  --peer-ledger books:main \
  --peer-ledger users:main
```

### Peer Events Configuration

| Flag | Env Var | Description |
|------|---------|-------------|
| `--peer-events-url` | `FLUREE_PEER_EVENTS_URL` | Custom events URL (default: `{tx_server_url}/fluree/events`) |
| `--peer-events-token` | `FLUREE_PEER_EVENTS_TOKEN` | Bearer token for events (supports `@filepath`) |

### Peer Reconnection

| Flag | Default | Description |
|------|---------|-------------|
| `--peer-reconnect-initial-ms` | `1000` | Initial reconnect delay |
| `--peer-reconnect-max-ms` | `30000` | Maximum reconnect delay |
| `--peer-reconnect-multiplier` | `2.0` | Backoff multiplier |

### Peer Storage Access

| Flag | Env Var | Default |
|------|---------|---------|
| `--storage-access-mode` | `FLUREE_STORAGE_ACCESS_MODE` | `shared` |

Options:
- `shared`: Direct storage access (requires `--storage-path`)
- `proxy`: Proxy reads through transaction server

For proxy mode:

| Flag | Env Var |
|------|---------|
| `--storage-proxy-token` | `FLUREE_STORAGE_PROXY_TOKEN` |
| `--storage-proxy-token-file` | `FLUREE_STORAGE_PROXY_TOKEN_FILE` |

## Storage Proxy Configuration (Transaction Server)

Enable storage proxy endpoints for peers without direct storage access:

| Flag | Env Var | Default |
|------|---------|---------|
| `--storage-proxy-enabled` | `FLUREE_STORAGE_PROXY_ENABLED` | `false` |
| `--storage-proxy-trusted-issuer` | `FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS` | None |
| `--storage-proxy-default-identity` | `FLUREE_STORAGE_PROXY_DEFAULT_IDENTITY` | None |
| `--storage-proxy-default-policy-class` | `FLUREE_STORAGE_PROXY_DEFAULT_POLICY_CLASS` | None |
| `--storage-proxy-debug-headers` | `FLUREE_STORAGE_PROXY_DEBUG_HEADERS` | `false` |

```bash
fluree-server \
  --storage-proxy-enabled \
  --storage-proxy-trusted-issuer did:key:z6Mk...
```

## Complete Configuration Examples

### Development (Memory Storage)

```bash
fluree-server \
  --log-level debug
```

### Single Server (File Storage)

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --log-level info
```

### Production with Admin Auth

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk... \
  --log-level info
```

### Transaction Server with Events Auth

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk... \
  --storage-proxy-enabled \
  --admin-auth-mode required
```

### Query Peer (Shared Storage)

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-path /var/lib/fluree \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-token.jwt
```

### Query Peer (Proxy Storage)

```bash
fluree-server \
  --server-role peer \
  --tx-server-url http://tx.internal:8090 \
  --storage-access-mode proxy \
  --storage-proxy-token @/etc/fluree/storage-proxy.jwt \
  --peer-subscribe-all \
  --peer-events-token @/etc/fluree/peer-token.jwt
```

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `FLUREE_LISTEN_ADDR` | Server address:port | `0.0.0.0:8090` |
| `FLUREE_STORAGE_PATH` | File storage path | None (memory) |
| `FLUREE_CORS_ENABLED` | Enable CORS | `true` |
| `FLUREE_INDEXING_ENABLED` | Enable background indexing | `false` |
| `FLUREE_CACHE_MAX_ENTRIES` | Cache size per ledger | `10000` |
| `FLUREE_BODY_LIMIT` | Max request body bytes | `52428800` |
| `FLUREE_LOG_LEVEL` | Log level | `info` |
| `FLUREE_SERVER_ROLE` | Server role | `transaction` |
| `FLUREE_TX_SERVER_URL` | Transaction server URL | None |
| `FLUREE_EVENTS_AUTH_MODE` | Events auth mode | `none` |
| `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS` | Events trusted issuers | None |
| `FLUREE_ADMIN_AUTH_MODE` | Admin auth mode | `none` |
| `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | Admin trusted issuers | None |
| `FLUREE_MCP_ENABLED` | Enable MCP endpoint | `false` |
| `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | MCP trusted issuers | None |
| `FLUREE_STORAGE_ACCESS_MODE` | Peer storage mode | `shared` |
| `FLUREE_STORAGE_PROXY_ENABLED` | Enable storage proxy | `false` |

### Telemetry and OTEL Variables

These variables configure logging and OpenTelemetry trace export. OTEL
variables require the server to be built with `--features otel`.

| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log filter directive ([EnvFilter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)). Highest priority — overrides `LOG_LEVEL`. | None |
| `LOG_LEVEL` | Fallback log level when `RUST_LOG` is not set | `info` |
| `LOG_FORMAT` | Output format: `json` or `human` | `human` |
| `LOG_SENSITIVE_DATA` | Sensitive data handling: `off`, `mask`, or `hash` | `mask` |
| `LOG_QUERY_TEXT` | Query text logging: `0` (off), `1`/`true`/`full`, or `hash` | `0` |
| `LOG_REQUEST_ID_HEADER` | Custom request ID header name | `x-request-id` |
| `OTEL_SERVICE_NAME` | Service name for OTEL trace exports | None (required for OTEL) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint URL | None (required for OTEL) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Transport protocol: `grpc` or `http/protobuf` | `grpc` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy: `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off` | `always_on` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampler argument (e.g., `0.01` for 1% with `traceidratio`) | `1.0` |

See [Telemetry and Logging](telemetry.md) for detailed configuration guidance,
and [Performance Investigation](../troubleshooting/performance-investigation.md)
for using tracing to diagnose slow operations.

## Command-Line Reference

```bash
fluree-server --help
```

## Best Practices

### 1. Use Environment Variables for Secrets

Don't pass tokens on the command line:

```bash
# Good - use @filepath syntax
--peer-events-token @/etc/fluree/token.jwt

# Good - use environment variable
export FLUREE_PEER_EVENTS_TOKEN=$(cat /etc/fluree/token.jwt)
```

### 2. Enable Admin Auth in Production

Always protect admin endpoints in production:

```bash
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

### 3. Use File Storage for Persistence

Memory storage is lost on restart:

```bash
# Development only
fluree-server

# Production
fluree-server --storage-path /var/lib/fluree
```

### 4. Monitor Logs

Use structured logging for production:

```bash
fluree-server --log-level info 2>&1 | jq .
```

## Related Documentation

- [Query Peers](query-peers.md) - Peer mode and replication
- [Storage Modes](storage.md) - Storage backend details
- [Telemetry](telemetry.md) - Monitoring configuration
- [Admin and Health](admin-and-health.md) - Health check endpoints
