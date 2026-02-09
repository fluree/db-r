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

### Replication vs Query Access

Fluree enforces a hard boundary between **replication-scoped** and **query-scoped** access:

- **Replication** (`fluree.storage.*`): Raw commit and index block transfer for peer sync and CLI `fetch`/`pull`/`push`. These operations bypass dataset policy (data must be bit-identical). Replication tokens are operator/service-account credentials — never issue them to end users.
- **Query** (`fluree.ledger.read/write.*`): Application-level data access through the query engine with full dataset policy enforcement. Query tokens are appropriate for end users and application service accounts.

A user holding only query-scoped tokens **cannot** clone or pull a ledger. They can `fluree track` a remote ledger (forwarding queries/transactions to the server) but cannot replicate its storage locally.

### Events Endpoint Authentication

Protect the `/v1/fluree/events` SSE endpoint:

| Flag | Env Var | Default |
|------|---------|---------|
| `--events-auth-mode` | `FLUREE_EVENTS_AUTH_MODE` | `none` |
| `--events-auth-audience` | `FLUREE_EVENTS_AUTH_AUDIENCE` | None |
| `--events-auth-trusted-issuer` | `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS` | None |

Modes:
- `none`: No authentication
- `optional`: Accept tokens but don't require them
- `required`: Require valid Bearer token

Supports both Ed25519 (embedded JWK) and OIDC/JWKS (RS256) tokens when the `oidc` feature is enabled and `--jwks-issuer` is configured. For OIDC tokens, issuer trust is implicit — only tokens signed by keys from configured JWKS endpoints will verify. For Ed25519 tokens, the issuer must appear in `--events-auth-trusted-issuer`.

```bash
# Ed25519 tokens only
fluree-server \
  --events-auth-mode required \
  --events-auth-trusted-issuer did:key:z6Mk...

# OIDC + Ed25519 (both work simultaneously)
fluree-server \
  --events-auth-mode required \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json" \
  --events-auth-trusted-issuer did:key:z6Mk...
```

### Data API Authentication

Protect query/write endpoints (including `/:ledger/query`, `/:ledger/insert`, `/:ledger/upsert`,
`/:ledger/transact`, `/fluree/ledger-info`, and `/fluree/exists`):

| Flag | Env Var | Default |
|------|---------|---------|
| `--data-auth-mode` | `FLUREE_DATA_AUTH_MODE` | `none` |
| `--data-auth-audience` | `FLUREE_DATA_AUTH_AUDIENCE` | None |
| `--data-auth-trusted-issuer` | `FLUREE_DATA_AUTH_TRUSTED_ISSUERS` | None |
| `--data-auth-default-policy-class` | `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | None |

Modes:
- `none`: No authentication (default / backwards compatible)
- `optional`: Accept tokens but don't require them (development only)
- `required`: Require either a valid Bearer token **or** a signed request (JWS/VC)

Bearer token scopes:
- **Read**: `fluree.ledger.read.all=true` or `fluree.ledger.read.ledgers=[...]`
- **Write**: `fluree.ledger.write.all=true` or `fluree.ledger.write.ledgers=[...]`

Back-compat: `fluree.storage.*` claims imply **read** scope for data endpoints.

```bash
fluree-server \
  --data-auth-mode required \
  --data-auth-trusted-issuer did:key:z6Mk...
```

### OIDC / JWKS Token Verification

When the `oidc` feature is enabled, the server can verify JWT tokens signed by external identity
providers (e.g., Fluree Cloud Service) using JWKS (JSON Web Key Set) endpoints. This is in addition to the
existing embedded-JWK (Ed25519 `did:key`) verification path.

**Dual-path dispatch**: The server inspects each Bearer token's header:
- **Embedded JWK** (Ed25519): Uses the existing `verify_jws()` path — no JWKS needed.
- **kid header** (RS256): Uses OIDC/JWKS path — fetches the signing key from the issuer's JWKS endpoint.

Both paths coexist; no configuration change is needed for existing Ed25519 tokens.

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--jwks-issuer` | `FLUREE_JWKS_ISSUERS` | None | OIDC issuer to trust (repeatable) |
| `--jwks-cache-ttl` | `FLUREE_JWKS_CACHE_TTL` | `300` | JWKS cache TTL in seconds |

The `--jwks-issuer` flag takes the format `<issuer_url>=<jwks_url>`:

```bash
fluree-server \
  --data-auth-mode required \
  --jwks-issuer "https://solo.example.com=https://solo.example.com/.well-known/jwks.json"
```

For multiple issuers, repeat the flag or use comma separation in the env var:

```bash
# CLI flags (repeatable)
fluree-server \
  --jwks-issuer "https://issuer1.example.com=https://issuer1.example.com/.well-known/jwks.json" \
  --jwks-issuer "https://issuer2.example.com=https://issuer2.example.com/.well-known/jwks.json"

# Environment variable (comma-separated)
export FLUREE_JWKS_ISSUERS="https://issuer1.example.com=https://issuer1.example.com/.well-known/jwks.json,https://issuer2.example.com=https://issuer2.example.com/.well-known/jwks.json"
```

**Behavior details:**
- JWKS endpoints are fetched at startup (`warm()`) but the server starts even if they're unreachable.
- Keys are cached and refreshed when a `kid` miss occurs (rate-limited to one refresh per issuer every 10 seconds).
- The token's `iss` claim must exactly match a configured issuer URL — unconfigured issuers are rejected immediately with a clear error.
- Data API, events, admin, and storage proxy endpoints all support JWKS verification. A single `--jwks-issuer` flag enables OIDC tokens across all endpoint groups. MCP auth continues to use the existing Ed25519 path only.

#### Connection-Scoped SPARQL Scope Enforcement

When a Bearer token is present for connection-scoped SPARQL queries (`/v1/fluree/query` with
`Content-Type: application/sparql-query`), the server enforces ledger scope:

- FROM / FROM NAMED clauses are parsed to extract ledger addresses (`name:branch`).
- Each ledger address is checked against the token's read scope (`fluree.ledger.read.all` or `fluree.ledger.read.ledgers`).
- Out-of-scope ledgers return 404 (no existence leak).
- If no FROM clause is present, the query proceeds normally (the engine handles missing dataset errors).

### Admin Endpoint Authentication

Protect `/v1/fluree/create` and `/v1/fluree/drop` endpoints:

| Flag | Env Var | Default |
|------|---------|---------|
| `--admin-auth-mode` | `FLUREE_ADMIN_AUTH_MODE` | `none` |
| `--admin-auth-trusted-issuer` | `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | None |

Modes:
- `none`: No authentication (development)
- `required`: Require valid Bearer token (production)

Supports both Ed25519 (embedded JWK) and OIDC/JWKS (RS256) tokens when the `oidc` feature is enabled and `--jwks-issuer` is configured. For OIDC tokens, issuer trust is implicit — only tokens signed by keys from configured JWKS endpoints will verify. For Ed25519 tokens, the issuer must appear in `--admin-auth-trusted-issuer` or the fallback `--events-auth-trusted-issuer`.

```bash
# Ed25519 tokens only
fluree-server \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...

# OIDC (trust comes from --jwks-issuer, no did:key issuers needed)
fluree-server \
  --admin-auth-mode required \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json"
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
| `--peer-subscribe-all` | Subscribe to all ledgers and graph sources |
| `--peer-ledger <alias>` | Subscribe to specific ledger (repeatable) |
| `--peer-graph-source <alias>` | Subscribe to specific graph source (repeatable) |

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
| `--peer-events-url` | `FLUREE_PEER_EVENTS_URL` | Custom events URL (default: `{tx_server_url}/v1/fluree/events`) |
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

Storage proxy provides **replication-scoped** access to raw storage for peer servers and CLI replication commands (`fetch`/`pull`/`push`). Tokens must carry `fluree.storage.*` claims — query-scoped tokens (`fluree.ledger.read/write.*`) are not sufficient. See [Replication vs Query Access](#replication-vs-query-access) above.

Enable storage proxy endpoints for peers without direct storage access:

| Flag | Env Var | Default |
|------|---------|---------|
| `--storage-proxy-enabled` | `FLUREE_STORAGE_PROXY_ENABLED` | `false` |
| `--storage-proxy-trusted-issuer` | `FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS` | None |
| `--storage-proxy-default-identity` | `FLUREE_STORAGE_PROXY_DEFAULT_IDENTITY` | None |
| `--storage-proxy-default-policy-class` | `FLUREE_STORAGE_PROXY_DEFAULT_POLICY_CLASS` | None |
| `--storage-proxy-debug-headers` | `FLUREE_STORAGE_PROXY_DEBUG_HEADERS` | `false` |

```bash
# Ed25519 trust (did:key):
fluree-server \
  --storage-proxy-enabled \
  --storage-proxy-trusted-issuer did:key:z6Mk...

# OIDC/JWKS trust (same --jwks-issuer flag used by other endpoints):
fluree-server \
  --storage-proxy-enabled \
  --jwks-issuer "https://solo.example.com=https://solo.example.com/.well-known/jwks.json"
```

> **JWKS support**: When `--jwks-issuer` is configured, storage proxy endpoints accept RS256 OIDC tokens in addition to Ed25519 JWS tokens. The `--jwks-issuer` flag is shared with data, admin, and events endpoints — a single flag enables OIDC across all endpoint groups.

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

### Production with OIDC (All Endpoints)

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --indexing-enabled \
  --jwks-issuer "https://auth.example.com=https://auth.example.com/.well-known/jwks.json" \
  --data-auth-mode required \
  --events-auth-mode required \
  --admin-auth-mode required \
  --storage-proxy-enabled
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
| `FLUREE_DATA_AUTH_MODE` | Data API auth mode | `none` |
| `FLUREE_DATA_AUTH_AUDIENCE` | Data API expected audience | None |
| `FLUREE_DATA_AUTH_TRUSTED_ISSUERS` | Data API trusted issuers | None |
| `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | Data API default policy class | None |
| `FLUREE_ADMIN_AUTH_MODE` | Admin auth mode | `none` |
| `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | Admin trusted issuers | None |
| `FLUREE_MCP_ENABLED` | Enable MCP endpoint | `false` |
| `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | MCP trusted issuers | None |
| `FLUREE_STORAGE_ACCESS_MODE` | Peer storage mode | `shared` |
| `FLUREE_STORAGE_PROXY_ENABLED` | Enable storage proxy | `false` |

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
