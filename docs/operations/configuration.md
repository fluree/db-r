# Configuration

Fluree server is configured via a configuration file, command-line flags, and environment variables.

## Configuration Methods

### Configuration File (TOML, JSON, or JSON-LD)

The server reads configuration from `.fluree/config.toml` (or `.fluree/config.jsonld`) — the same file used by the Fluree CLI. Server settings live under the `[server]` section (or `"server"` key in JSON/JSON-LD). The server walks up from the current working directory looking for `.fluree/config.toml` or `.fluree/config.jsonld`, falling back to the global Fluree **config** directory (`$FLUREE_HOME`, or the platform config directory — see table below).

#### Global Directory Layout

When `$FLUREE_HOME` is set, both config and data share that single directory. When it is not set, the platform's config and data directories are used:

| Content                     | Linux                   | macOS                                  | Windows                 |
| --------------------------- | ----------------------- | -------------------------------------- | ----------------------- |
| Config (`config.toml`)      | `~/.config/fluree`      | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |
| Data (`storage/`, `active`) | `~/.local/share/fluree` | `~/Library/Application Support/fluree` | `%LOCALAPPDATA%\fluree` |

On Linux, config and data directories are separated per the XDG Base Directory specification. On macOS and Windows both resolve to the same directory. When directories are split, `fluree init --global` writes an absolute `storage_path` into `config.toml` so the server can locate the data directory regardless of working directory.

```bash
# Use default config file discovery
fluree-server

# Override config file path
fluree-server --config /etc/fluree/config.toml

# Activate a profile
fluree-server --profile prod
```

Example `config.toml`:

```toml
[server]
listen_addr = "0.0.0.0:8090"
storage_path = "/var/lib/fluree"
log_level = "info"
# cache_max_mb = 4096  # global cache budget (MB); default: 50% of RAM

[server.indexing]
enabled = true
reindex_min_bytes = 100000
reindex_max_bytes = 1000000

[server.auth.data]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]
```

JSON is also supported (detected by `.json` file extension):

```json
{
  "server": {
    "listen_addr": "0.0.0.0:8090",
    "storage_path": "/var/lib/fluree",
    "indexing": { "enabled": true }
  }
}
```

#### JSON-LD Format

JSON-LD config files (`.jsonld` extension) add a `@context` that maps config keys to the Fluree config vocabulary (`https://ns.flur.ee/config#`), making the file valid JSON-LD. Generate one with:

```bash
fluree init --format jsonld
```

Example `.fluree/config.jsonld`:

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/config#"
  },
  "_comment": "Fluree Configuration — JSON-LD format.",
  "server": {
    "listen_addr": "0.0.0.0:8090",
    "storage_path": ".fluree/storage",
    "log_level": "info",
    "indexing": {
      "enabled": false,
      "reindex_min_bytes": 100000,
      "reindex_max_bytes": 1000000
    }
  },
  "profiles": {
    "prod": {
      "server": {
        "log_level": "warn",
        "indexing": { "enabled": true }
      }
    }
  }
}
```

The `@context` is validated at load time (using the JSON-LD parser) but does not affect config value resolution — `serde` ignores unknown keys like `@context` and `_comment`. If both `config.toml` and `config.jsonld` exist in the same directory, TOML takes precedence and a warning is logged.

### Profiles

Profiles allow environment-specific overrides. Define them in `[profiles.<name>.server]` and activate with `--profile <name>`:

```toml
[server]
log_level = "info"

[profiles.dev.server]
log_level = "debug"

[profiles.prod.server]
log_level = "warn"
[profiles.prod.server.indexing]
enabled = true
[profiles.prod.server.auth.data]
mode = "required"
```

Profile values are deep-merged onto `[server]` — only the fields present in the profile are overridden.

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
3. Profile overrides (`[profiles.<name>.server]`)
4. Config file (`[server]`)
5. Built-in defaults

### Error Handling

If `--config` or `--profile` is specified and the configuration cannot be loaded (file not found, parse error, missing profile), the server **exits with an error**. This prevents silent misconfiguration in production.

If the config file is auto-discovered (no explicit `--config`) and cannot be parsed, the server logs a warning and continues with CLI/env/default values only.

## Server Configuration

### Listen Address

Address and port to bind to:

| Flag            | Env Var              | Default        |
| --------------- | -------------------- | -------------- |
| `--listen-addr` | `FLUREE_LISTEN_ADDR` | `0.0.0.0:8090` |

```bash
fluree-server --listen-addr 0.0.0.0:9090
```

### Storage Path

Path for file-based storage. If not specified, defaults to `.fluree/storage` relative to the working directory (the same location used by `fluree init`):

| Flag             | Env Var               | Default           |
| ---------------- | --------------------- | ----------------- |
| `--storage-path` | `FLUREE_STORAGE_PATH` | `.fluree/storage` |

```bash
# Explicit storage path (e.g. production)
fluree-server --storage-path /var/lib/fluree

# Default: uses .fluree/storage in the working directory
fluree-server
```

### CORS

Enable Cross-Origin Resource Sharing:

| Flag             | Env Var               | Default |
| ---------------- | --------------------- | ------- |
| `--cors-enabled` | `FLUREE_CORS_ENABLED` | `true`  |

When enabled, allows requests from any origin.

### Body Limit

Maximum request body size in bytes:

| Flag           | Env Var             | Default           |
| -------------- | ------------------- | ----------------- |
| `--body-limit` | `FLUREE_BODY_LIMIT` | `52428800` (50MB) |

### Log Level

Logging verbosity:

| Flag          | Env Var            | Default |
| ------------- | ------------------ | ------- |
| `--log-level` | `FLUREE_LOG_LEVEL` | `info`  |

Options: `trace`, `debug`, `info`, `warn`, `error`

### Cache Size

Global cache budget (MB):

| Flag              | Env Var              | Default                |
| ----------------- | -------------------- | ---------------------- |
| `--cache-max-mb`  | `FLUREE_CACHE_MAX_MB`| `50% of system RAM`    |

### Background Indexing

Enable background indexing and configure novelty backpressure thresholds:

| Flag                  | Env Var                    | Default   | Description                                     |
| --------------------- | -------------------------- | --------- | ----------------------------------------------- |
| `--indexing-enabled`  | `FLUREE_INDEXING_ENABLED`  | `false`   | Enable background indexing                      |
| `--reindex-min-bytes` | `FLUREE_REINDEX_MIN_BYTES` | `100000`  | Soft threshold (triggers background indexing)   |
| `--reindex-max-bytes` | `FLUREE_REINDEX_MAX_BYTES` | `1000000` | Hard threshold (blocks commits until reindexed) |

Config file equivalent:

```toml
[server.indexing]
enabled = true
reindex_min_bytes = 100000   # 100 KB
reindex_max_bytes = 1000000  # 1 MB
```

### Parallelism

Thread pool size for query execution and indexing:

| Flag              | Env Var               | Default              |
| ----------------- | --------------------- | -------------------- |
| `--parallelism`   | `FLUREE_PARALLELISM`  | `0` (auto-detect)    |

When set to `0` (the default), the server detects the number of available CPU cores and sizes the thread pool accordingly. Set an explicit value to limit CPU usage in shared-tenancy or container environments.

### Novelty Thresholds

Fine-grained control over novelty backpressure, independent of the `--indexing-enabled` flag above:

| Flag                    | Env Var                      | Default    | Description                                      |
| ----------------------- | ---------------------------- | ---------- | ------------------------------------------------ |
| `--novelty-min-bytes`   | `FLUREE_NOVELTY_MIN_BYTES`   | `100000`   | Soft threshold (triggers background indexing)     |
| `--novelty-max-bytes`   | `FLUREE_NOVELTY_MAX_BYTES`   | `1000000`  | Hard threshold (blocks commits until indexed)     |

These correspond to the `reindex_min_bytes` / `reindex_max_bytes` config file keys. The `--novelty-*` CLI flags are aliases that may be used interchangeably.

### Ledger Preload

| Flag            | Env Var              | Default |
| --------------- | -------------------- | ------- |
| `--no-preload`  | `FLUREE_NO_PRELOAD`  | `false` |

By default, the server walks the storage directory at startup and preloads all discovered ledgers into memory. Pass `--no-preload` (or set `FLUREE_NO_PRELOAD=true`) to skip this step. This is useful for memory-constrained containers with many ledgers — ledgers will instead be loaded on first access.

### Ledger Cache

Control the in-memory ledger cache. When caching is enabled (the default), recently-accessed ledgers are held in memory and evicted after a period of inactivity.

| Flag                            | Env Var                              | Default   | Description                                        |
| ------------------------------- | ------------------------------------ | --------- | -------------------------------------------------- |
| `--no-ledger-cache`             | `FLUREE_NO_LEDGER_CACHE`             | `false`   | Disable in-memory ledger caching entirely           |
| `--ledger-cache-idle-ttl-secs`  | `FLUREE_LEDGER_CACHE_IDLE_TTL`       | `1800`    | Seconds before idle ledgers are evicted from cache  |
| `--ledger-cache-sweep-secs`     | `FLUREE_LEDGER_CACHE_SWEEP_INTERVAL` | `60`      | Background sweep interval for cache cleanup         |

```bash
# Disable caching (every request loads from storage)
fluree-server --no-ledger-cache

# Keep idle ledgers for 1 hour, sweep every 2 minutes
fluree-server --ledger-cache-idle-ttl-secs 3600 --ledger-cache-sweep-secs 120
```

#### Container-Aware Cache Sizing

The `--cache-max-mb` default (50% of system RAM) is container-aware. On startup the server checks for cgroup v2 memory limits (`/sys/fs/cgroup/memory.max`), then falls back to cgroup v1 (`/sys/fs/cgroup/memory/memory.limit_in_bytes`), before using `sysinfo` as a last resort. This means `cache_max_mb` defaults correctly in Docker, Kubernetes, and AWS Fargate environments without any explicit configuration. If the cgroup memory limit is set to "max" (unlimited), the server falls back to `sysinfo` physical memory detection.

To override the automatic detection, set `--cache-max-mb` explicitly:

```bash
# Explicit 2 GB cache in a 4 GB container
fluree-server --cache-max-mb 2048
```

### Query Timeout

Global query timeout:

| Flag                    | Env Var                 | Default          |
| ----------------------- | ----------------------- | ---------------- |
| `--query-timeout-secs`  | `FLUREE_QUERY_TIMEOUT`  | `0` (disabled)   |

When set to a positive value, any query exceeding this duration returns HTTP 504 Gateway Timeout. Per-query `max-fuel` limits still apply independently — whichever limit is reached first terminates the query.

### Shutdown Timeout

Graceful shutdown drain timeout:

| Flag                      | Env Var                    | Default |
| ------------------------- | -------------------------- | ------- |
| `--shutdown-timeout-secs` | `FLUREE_SHUTDOWN_TIMEOUT`  | `30`    |

On receiving SIGTERM (or Ctrl-C), the server stops accepting new connections and waits up to this many seconds for inflight requests to complete before forcing shutdown.

### Maintenance Mode

| Flag                  | Env Var                    | Default |
| --------------------- | -------------------------- | ------- |
| `--maintenance-mode`  | `FLUREE_MAINTENANCE_MODE`  | `false` |

Start the server in read-only mode. All write endpoints (`insert`, `upsert`, `update`, `create`, `drop`) return HTTP 503 Service Unavailable. Read endpoints (`query`, `ledger-info`, `exists`) continue to function normally.

Maintenance mode can be toggled at runtime without restarting the server:

```bash
# Enter maintenance mode
curl -X POST http://localhost:8090/v1/fluree/admin/maintenance

# Exit maintenance mode
curl -X DELETE http://localhost:8090/v1/fluree/admin/maintenance
```

### Encryption

Encrypt data at rest using AES-256-GCM. Provide exactly one of the two options below:

| Flag                    | Env Var                    | Default | Description                                        |
| ----------------------- | -------------------------- | ------- | -------------------------------------------------- |
| `--encryption-key`      | `FLUREE_ENCRYPTION_KEY`    | None    | Base64-encoded 32-byte AES-256-GCM key             |
| `--encryption-key-file` | `FLUREE_ENCRYPTION_KEY_FILE` | None  | Path to file containing the encryption key          |

When an encryption key is configured, all data written to storage is encrypted before writing and decrypted on read. The key must be exactly 32 bytes (256 bits) when decoded from Base64.

> **Warning:** Losing the encryption key means permanent data loss. Back up the key separately from the encrypted data.

```bash
# Inline key (Base64)
fluree-server --encryption-key "dGhpcyBpcyBhIDMyLWJ5dGUga2V5ISEhMTIzNDU2Nzg="

# Key from file (recommended for production)
fluree-server --encryption-key-file /etc/fluree/encryption.key
```

### S3 Storage Backend

Enable S3 as the storage backend (requires the `aws` feature):

| Flag              | Env Var              | Default | Description                       |
| ----------------- | -------------------- | ------- | --------------------------------- |
| `--s3-bucket`     | `FLUREE_S3_BUCKET`   | None    | S3 bucket name                    |
| `--s3-endpoint`   | `FLUREE_S3_ENDPOINT` | None    | S3 endpoint URL                   |
| `--s3-prefix`     | `FLUREE_S3_PREFIX`   | None    | Key prefix within the S3 bucket   |

Setting `--s3-bucket` enables S3 storage. The server uses the default AWS credential chain (environment variables, instance profile, etc.). Use `--s3-endpoint` to point at S3-compatible services (MinIO, LocalStack).

```bash
# AWS S3
fluree-server \
  --s3-bucket my-fluree-data \
  --s3-prefix prod/v1

# LocalStack / MinIO
fluree-server \
  --s3-bucket my-fluree-data \
  --s3-endpoint http://localhost:4566
```

Config file equivalent:

```toml
[server.s3]
bucket = "my-fluree-data"
prefix = "prod/v1"
# endpoint = "http://localhost:4566"  # optional, for S3-compatible services
```

## Server Role Configuration

### Server Role

Operating mode: transaction server or query peer:

| Flag            | Env Var              | Default       |
| --------------- | -------------------- | ------------- |
| `--server-role` | `FLUREE_SERVER_ROLE` | `transaction` |

Options:

- `transaction`: Write-enabled, produces events stream
- `peer`: Read-only, subscribes to transaction server

### Transaction Server URL (Peer Mode)

Base URL of the transaction server (required in peer mode):

| Flag              | Env Var                |
| ----------------- | ---------------------- |
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

| Flag                           | Env Var                              | Default |
| ------------------------------ | ------------------------------------ | ------- |
| `--events-auth-mode`           | `FLUREE_EVENTS_AUTH_MODE`            | `none`  |
| `--events-auth-audience`       | `FLUREE_EVENTS_AUTH_AUDIENCE`        | None    |
| `--events-auth-trusted-issuer` | `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS` | None    |

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

Protect query/transaction endpoints (including `/:ledger/query`, `/:ledger/insert`, `/:ledger/upsert`,
`/:ledger/update`, `/fluree/ledger-info`, and `/fluree/exists`):

| Flag                               | Env Var                                 | Default |
| ---------------------------------- | --------------------------------------- | ------- |
| `--data-auth-mode`                 | `FLUREE_DATA_AUTH_MODE`                 | `none`  |
| `--data-auth-audience`             | `FLUREE_DATA_AUTH_AUDIENCE`             | None    |
| `--data-auth-trusted-issuer`       | `FLUREE_DATA_AUTH_TRUSTED_ISSUERS`      | None    |
| `--data-auth-default-policy-class` | `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | None    |

Modes:

- `none`: No authentication (default)
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

| Flag               | Env Var                 | Default | Description                       |
| ------------------ | ----------------------- | ------- | --------------------------------- |
| `--jwks-issuer`    | `FLUREE_JWKS_ISSUERS`   | None    | OIDC issuer to trust (repeatable) |
| `--jwks-cache-ttl` | `FLUREE_JWKS_CACHE_TTL` | `300`   | JWKS cache TTL in seconds         |

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

- FROM / FROM NAMED clauses are parsed to extract ledger IDs (`name:branch`).
- Each ledger ID is checked against the token's read scope (`fluree.ledger.read.all` or `fluree.ledger.read.ledgers`).
- Out-of-scope ledgers return 404 (no existence leak).
- If no FROM clause is present, the query proceeds normally (the engine handles missing dataset errors).

### Admin Endpoint Authentication

Protect `/v1/fluree/create` and `/v1/fluree/drop` endpoints:

| Flag                          | Env Var                             | Default |
| ----------------------------- | ----------------------------------- | ------- |
| `--admin-auth-mode`           | `FLUREE_ADMIN_AUTH_MODE`            | `none`  |
| `--admin-auth-trusted-issuer` | `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS` | None    |

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

| Flag                        | Env Var                           | Default |
| --------------------------- | --------------------------------- | ------- |
| `--mcp-enabled`             | `FLUREE_MCP_ENABLED`              | `false` |
| `--mcp-auth-trusted-issuer` | `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | None    |

```bash
fluree-server \
  --mcp-enabled \
  --mcp-auth-trusted-issuer did:key:z6Mk...
```

## Peer Mode Configuration

### Peer Subscription

Configure what the peer subscribes to:

| Flag                              | Description                                     |
| --------------------------------- | ----------------------------------------------- |
| `--peer-subscribe-all`            | Subscribe to all ledgers and graph sources      |
| `--peer-ledger <ledger-id>`       | Subscribe to specific ledger (repeatable)       |
| `--peer-graph-source <ledger-id>` | Subscribe to specific graph source (repeatable) |

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

| Flag                  | Env Var                    | Description                                                     |
| --------------------- | -------------------------- | --------------------------------------------------------------- |
| `--peer-events-url`   | `FLUREE_PEER_EVENTS_URL`   | Custom events URL (default: `{tx_server_url}/v1/fluree/events`) |
| `--peer-events-token` | `FLUREE_PEER_EVENTS_TOKEN` | Bearer token for events (supports `@filepath`)                  |

### Peer Reconnection

| Flag                          | Default | Description             |
| ----------------------------- | ------- | ----------------------- |
| `--peer-reconnect-initial-ms` | `1000`  | Initial reconnect delay |
| `--peer-reconnect-max-ms`     | `30000` | Maximum reconnect delay |
| `--peer-reconnect-multiplier` | `2.0`   | Backoff multiplier      |

### Peer Storage Access

| Flag                    | Env Var                      | Default  |
| ----------------------- | ---------------------------- | -------- |
| `--storage-access-mode` | `FLUREE_STORAGE_ACCESS_MODE` | `shared` |

Options:

- `shared`: Direct storage access (requires `--storage-path`)
- `proxy`: Proxy reads through transaction server

For proxy mode:

| Flag                         | Env Var                           |
| ---------------------------- | --------------------------------- |
| `--storage-proxy-token`      | `FLUREE_STORAGE_PROXY_TOKEN`      |
| `--storage-proxy-token-file` | `FLUREE_STORAGE_PROXY_TOKEN_FILE` |

## Storage Proxy Configuration (Transaction Server)

Storage proxy provides **replication-scoped** access to raw storage for peer servers and CLI replication commands (`fetch`/`pull`/`push`). Tokens must carry `fluree.storage.*` claims — query-scoped tokens (`fluree.ledger.read/write.*`) are not sufficient. See [Replication vs Query Access](#replication-vs-query-access) above.

Enable storage proxy endpoints for peers without direct storage access:

| Flag                                   | Env Var                                     | Default |
| -------------------------------------- | ------------------------------------------- | ------- |
| `--storage-proxy-enabled`              | `FLUREE_STORAGE_PROXY_ENABLED`              | `false` |
| `--storage-proxy-trusted-issuer`       | `FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS`      | None    |
| `--storage-proxy-default-identity`     | `FLUREE_STORAGE_PROXY_DEFAULT_IDENTITY`     | None    |
| `--storage-proxy-default-policy-class` | `FLUREE_STORAGE_PROXY_DEFAULT_POLICY_CLASS` | None    |
| `--storage-proxy-debug-headers`        | `FLUREE_STORAGE_PROXY_DEBUG_HEADERS`        | `false` |

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

### Container Deployment (Docker / Kubernetes)

```bash
fluree-server \
  --storage-path /data/fluree \
  --no-preload \
  --cache-max-mb 1024 \
  --parallelism 4 \
  --ledger-cache-idle-ttl-secs 600 \
  --query-timeout-secs 30 \
  --shutdown-timeout-secs 15 \
  --indexing-enabled
```

Cache sizing is container-aware by default (detects cgroup memory limits), so `--cache-max-mb` can often be omitted.

### S3 Storage Backend

```bash
fluree-server \
  --s3-bucket my-fluree-data \
  --s3-prefix prod/v1 \
  --indexing-enabled \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
```

### Production with Encryption

```bash
fluree-server \
  --storage-path /var/lib/fluree \
  --encryption-key-file /etc/fluree/encryption.key \
  --indexing-enabled \
  --admin-auth-mode required \
  --admin-auth-trusted-issuer did:key:z6Mk...
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

| Variable                                | Description                                     | Default                                                                 |
| --------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------- |
| `FLUREE_HOME`                           | Global Fluree directory (unified config + data) | Platform dirs (see [Global Directory Layout](#global-directory-layout)) |
| `FLUREE_CONFIG`                         | Config file path                                | `.fluree/config.{toml,jsonld}` (auto-discovered)                        |
| `FLUREE_PROFILE`                        | Configuration profile name                      | None                                                                    |
| `FLUREE_LISTEN_ADDR`                    | Server address:port                             | `0.0.0.0:8090`                                                          |
| `FLUREE_STORAGE_PATH`                   | File storage path                               | `.fluree/storage`                                                       |
| `FLUREE_CORS_ENABLED`                   | Enable CORS                                     | `true`                                                                  |
| `FLUREE_INDEXING_ENABLED`               | Enable background indexing                      | `false`                                                                 |
| `FLUREE_REINDEX_MIN_BYTES`              | Soft reindex threshold (bytes)                  | `100000`                                                                |
| `FLUREE_REINDEX_MAX_BYTES`              | Hard reindex threshold (bytes)                  | `1000000`                                                               |
| `FLUREE_CACHE_MAX_MB`                   | Global cache budget (MB)                        | `50% of system RAM`                                                     |
| `FLUREE_NO_PRELOAD`                     | Skip ledger preload at startup                  | `false`                                                                 |
| `FLUREE_PARALLELISM`                    | Thread pool size (0 = auto-detect)              | `0`                                                                     |
| `FLUREE_NOVELTY_MIN_BYTES`              | Soft novelty threshold (bytes)                  | `100000`                                                                |
| `FLUREE_NOVELTY_MAX_BYTES`              | Hard novelty threshold (bytes)                  | `1000000`                                                               |
| `FLUREE_NO_LEDGER_CACHE`               | Disable in-memory ledger caching                | `false`                                                                 |
| `FLUREE_LEDGER_CACHE_IDLE_TTL`          | Idle ledger eviction (seconds)                  | `1800`                                                                  |
| `FLUREE_LEDGER_CACHE_SWEEP_INTERVAL`    | Cache sweep interval (seconds)                  | `60`                                                                    |
| `FLUREE_QUERY_TIMEOUT`                  | Global query timeout (seconds, 0 = disabled)    | `0`                                                                     |
| `FLUREE_SHUTDOWN_TIMEOUT`               | Graceful shutdown timeout (seconds)             | `30`                                                                    |
| `FLUREE_MAINTENANCE_MODE`               | Start in read-only mode                         | `false`                                                                 |
| `FLUREE_ENCRYPTION_KEY`                 | Base64-encoded AES-256-GCM encryption key       | None                                                                    |
| `FLUREE_ENCRYPTION_KEY_FILE`            | Path to encryption key file                     | None                                                                    |
| `FLUREE_S3_BUCKET`                      | S3 bucket name (enables S3 storage)             | None                                                                    |
| `FLUREE_S3_ENDPOINT`                    | S3 endpoint URL                                 | None                                                                    |
| `FLUREE_S3_PREFIX`                      | Key prefix within S3 bucket                     | None                                                                    |
| `FLUREE_BODY_LIMIT`                     | Max request body bytes                          | `52428800`                                                              |
| `FLUREE_LOG_LEVEL`                      | Log level                                       | `info`                                                                  |
| `FLUREE_SERVER_ROLE`                    | Server role                                     | `transaction`                                                           |
| `FLUREE_TX_SERVER_URL`                  | Transaction server URL                          | None                                                                    |
| `FLUREE_EVENTS_AUTH_MODE`               | Events auth mode                                | `none`                                                                  |
| `FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS`    | Events trusted issuers                          | None                                                                    |
| `FLUREE_DATA_AUTH_MODE`                 | Data API auth mode                              | `none`                                                                  |
| `FLUREE_DATA_AUTH_AUDIENCE`             | Data API expected audience                      | None                                                                    |
| `FLUREE_DATA_AUTH_TRUSTED_ISSUERS`      | Data API trusted issuers                        | None                                                                    |
| `FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS` | Data API default policy class                   | None                                                                    |
| `FLUREE_ADMIN_AUTH_MODE`                | Admin auth mode                                 | `none`                                                                  |
| `FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS`     | Admin trusted issuers                           | None                                                                    |
| `FLUREE_MCP_ENABLED`                    | Enable MCP endpoint                             | `false`                                                                 |
| `FLUREE_MCP_AUTH_TRUSTED_ISSUERS`       | MCP trusted issuers                             | None                                                                    |
| `FLUREE_STORAGE_ACCESS_MODE`            | Peer storage mode                               | `shared`                                                                |
| `FLUREE_STORAGE_PROXY_ENABLED`          | Enable storage proxy                            | `false`                                                                 |

## Command-Line Reference

```bash
fluree-server --help
```

## Best Practices

### 1. Keep Secrets Out of Config Files

Tokens and credentials should not be stored as plaintext in config files (which may be committed to version control or readable by other processes). Three options, in order of preference:

**Environment variables** (recommended for production):

```bash
export FLUREE_PEER_EVENTS_TOKEN=$(cat /etc/fluree/token.jwt)
export FLUREE_STORAGE_PROXY_TOKEN=$(cat /etc/fluree/proxy-token.jwt)
```

**`@filepath` references** in config files or CLI flags (reads the file at startup):

```toml
[server.peer]
events_token = "@/etc/fluree/peer-token.jwt"
storage_proxy_token = "@/etc/fluree/proxy-token.jwt"
```

```bash
--peer-events-token @/etc/fluree/token.jwt
```

**Direct values** (development only): If a secret-bearing field contains a literal token in the config file, the server logs a warning at startup recommending `@filepath` or env vars.

The following config file fields support `@filepath` resolution:

| Config file key            | Env var alternative          |
| -------------------------- | ---------------------------- |
| `peer.events_token`        | `FLUREE_PEER_EVENTS_TOKEN`   |
| `peer.storage_proxy_token` | `FLUREE_STORAGE_PROXY_TOKEN` |

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
