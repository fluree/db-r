# Configuration

Fluree can be configured via command-line flags, configuration files, or environment variables. This document provides a complete reference for all configuration options.

## Configuration Methods

### Command-Line Flags

```bash
./fluree-db-server \
  --port 8090 \
  --storage file \
  --data-dir /var/lib/fluree \
  --log-level info
```

### Configuration File

Create `fluree-config.toml`:

```toml
[server]
port = 8090
host = "0.0.0.0"

[storage]
mode = "file"
data_dir = "/var/lib/fluree"

[logging]
level = "info"
format = "json"
```

Run with config file:

```bash
./fluree-db-server --config fluree-config.toml
```

### Environment Variables

```bash
export FLUREE_PORT=8090
export FLUREE_STORAGE=file
export FLUREE_DATA_DIR=/var/lib/fluree
export FLUREE_LOG_LEVEL=info

./fluree-db-server
```

### Precedence

Configuration precedence (highest to lowest):
1. Command-line flags
2. Environment variables
3. Configuration file
4. Default values

## Server Configuration

### Port

Port to listen on:

```bash
--port 8090
```

```toml
[server]
port = 8090
```

```bash
FLUREE_PORT=8090
```

Default: 8090

### Host

Host address to bind to:

```bash
--host "0.0.0.0"
```

```toml
[server]
host = "0.0.0.0"
```

Default: "0.0.0.0" (all interfaces)

### TLS/SSL

Enable HTTPS:

```bash
--tls-cert /path/to/cert.pem \
--tls-key /path/to/key.pem \
--tls-ca /path/to/ca.pem
```

```toml
[server.tls]
cert = "/path/to/cert.pem"
key = "/path/to/key.pem"
ca = "/path/to/ca.pem"
```

## Storage Configuration

### Storage Mode

```bash
--storage memory|file|aws
```

```toml
[storage]
mode = "file"
```

```bash
FLUREE_STORAGE=file
```

Options: `memory`, `file`, `aws`

### File Storage

```bash
--storage file \
--data-dir /var/lib/fluree
```

```toml
[storage]
mode = "file"
data_dir = "/var/lib/fluree"
```

### Encrypted Storage

Enable AES-256-GCM encryption for data at rest:

```bash
# Set encryption key via environment variable
export FLUREE_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

JSON-LD configuration:

```json
{
  "@context": {"@vocab": "https://ns.flur.ee/system#"},
  "@graph": [{
    "@type": "Connection",
    "indexStorage": {
      "@type": "Storage",
      "filePath": "/var/lib/fluree",
      "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    }
  }]
}
```

See [Storage Encryption](../security/encryption.md) for full documentation on key management and configuration options.

### AWS Storage

```bash
--storage aws \
--s3-bucket fluree-prod-data \
--s3-region us-east-1 \
--s3-prefix fluree/ \
--dynamodb-table fluree-nameservice \
--dynamodb-region us-east-1
```

```toml
[storage]
mode = "aws"
s3_bucket = "fluree-prod-data"
s3_region = "us-east-1"
s3_prefix = "fluree/"
dynamodb_table = "fluree-nameservice"
dynamodb_region = "us-east-1"
```

### AWS Credentials

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
```

Or use IAM roles (recommended).

## Indexing Configuration

### Index Interval

How often to index:

```bash
--index-interval-ms 5000
```

```toml
[indexing]
interval_ms = 5000
```

Default: 5000 (5 seconds)

### Batch Size

Transactions per indexing batch:

```bash
--index-batch-size 10
```

```toml
[indexing]
batch_size = 10
```

Default: 10

### Index Memory

Memory allocated for indexing:

```bash
--index-memory-mb 2048
```

```toml
[indexing]
memory_mb = 2048
```

Default: 1024 MB

### Index Threads

Parallel indexing threads:

```bash
--index-threads 4
```

```toml
[indexing]
threads = 4
```

Default: 2

### Auto-Index

Enable/disable automatic indexing:

```bash
--auto-index true
```

```toml
[indexing]
auto_index = true
```

Default: true

## Query Configuration

### Query Timeout

Maximum query execution time:

```bash
--query-timeout-ms 30000
```

```toml
[query]
timeout_ms = 30000
```

Default: 30000 (30 seconds)

### Max Query Size

Maximum query request size:

```bash
--max-query-size 1048576
```

```toml
[query]
max_size_bytes = 1048576
```

Default: 1 MB

### Query Threads

Query execution threads:

```bash
--query-threads 8
```

```toml
[query]
threads = 8
```

Default: 4

### Query Memory

Memory per query:

```bash
--query-memory-mb 512
```

```toml
[query]
memory_mb = 512
```

Default: 256 MB

## Transaction Configuration

### Max Transaction Size

Maximum transaction size:

```bash
--max-transaction-size 10485760
```

```toml
[transaction]
max_size_bytes = 10485760
```

Default: 10 MB

### Transaction Timeout

Maximum transaction processing time:

```bash
--transaction-timeout-ms 10000
```

```toml
[transaction]
timeout_ms = 10000
```

Default: 10000 (10 seconds)

## Logging Configuration

### Log Level

```bash
--log-level error|warn|info|debug|trace
```

```toml
[logging]
level = "info"
```

```bash
RUST_LOG=info
```

Default: info

### Log Format

```bash
--log-format json|text
```

```toml
[logging]
format = "json"
```

Options: `json`, `text`

### Log Output

```bash
--log-file /var/log/fluree/server.log
```

```toml
[logging]
file = "/var/log/fluree/server.log"
```

Default: stdout

## CORS Configuration

### Enable CORS

```bash
--cors-enabled true \
--cors-origin "https://example.com" \
--cors-methods "GET,POST,OPTIONS" \
--cors-headers "Content-Type,Authorization"
```

```toml
[server.cors]
enabled = true
origin = "https://example.com"
methods = ["GET", "POST", "OPTIONS"]
headers = ["Content-Type", "Authorization"]
```

### Allow All Origins (Development)

```bash
--cors-origin "*"
```

Never use in production with credentials.

## Authentication Configuration

### Require Authentication

```bash
--require-auth true
```

```toml
[security]
require_auth = true
```

### Require Signed Requests

```bash
--require-signed-requests true
```

```toml
[security]
require_signed_requests = true
```

### API Keys

```bash
--api-keys "key1,key2,key3"
```

```toml
[security]
api_keys = ["key1", "key2", "key3"]
```

## Rate Limiting

### Query Rate Limit

```bash
--rate-limit-queries 100 \
--rate-limit-window 60
```

```toml
[rate_limit]
queries_per_window = 100
window_seconds = 60
```

### Transaction Rate Limit

```bash
--rate-limit-transactions 10
```

```toml
[rate_limit]
transactions_per_window = 10
```

## Complete Configuration Example

```toml
[server]
port = 8090
host = "0.0.0.0"

[server.tls]
cert = "/etc/fluree/certs/server.crt"
key = "/etc/fluree/certs/server.key"

[server.cors]
enabled = true
origin = "https://app.example.com"
methods = ["GET", "POST", "OPTIONS"]
headers = ["Content-Type", "Authorization"]

[storage]
mode = "file"
data_dir = "/var/lib/fluree"

[indexing]
interval_ms = 5000
batch_size = 10
memory_mb = 2048
threads = 4
auto_index = true

[query]
timeout_ms = 30000
max_size_bytes = 1048576
threads = 8
memory_mb = 512

[transaction]
max_size_bytes = 10485760
timeout_ms = 10000

[logging]
level = "info"
format = "json"
file = "/var/log/fluree/server.log"

[security]
require_auth = true
require_signed_requests = false

[rate_limit]
queries_per_window = 100
transactions_per_window = 10
window_seconds = 60
```

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `FLUREE_PORT` | Server port | 8090 |
| `FLUREE_HOST` | Bind address | 0.0.0.0 |
| `FLUREE_STORAGE` | Storage mode | memory |
| `FLUREE_DATA_DIR` | Data directory | ./data |
| `FLUREE_LOG_LEVEL` | Log level | info |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |
| `AWS_REGION` | AWS region | us-east-1 |

## Command-Line Flags Reference

```bash
./fluree-db-server --help
```

Common flags:

```text
--port <PORT>                   Server port (default: 8090)
--host <HOST>                   Bind address (default: 0.0.0.0)
--storage <MODE>                Storage mode: memory|file|aws
--data-dir <DIR>                Data directory for file storage
--log-level <LEVEL>             Log level: error|warn|info|debug|trace
--config <FILE>                 Configuration file path
--index-interval-ms <MS>        Indexing interval in milliseconds
--index-batch-size <N>          Transactions per index batch
--query-timeout-ms <MS>         Query timeout in milliseconds
--require-auth                  Require authentication
--require-signed-requests       Require JWS/VC signed requests
--cors-origin <ORIGIN>          CORS allowed origin
--tls-cert <FILE>              TLS certificate file
--tls-key <FILE>               TLS private key file
```

## Best Practices

### 1. Use Configuration Files

For production, use config files:

```bash
./fluree-db-server --config /etc/fluree/config.toml
```

Easier to manage and version control.

### 2. Secure Sensitive Values

Don't put secrets in config files:

```toml
# Bad
api_keys = ["secret-key-here"]

# Good
api_keys_file = "/etc/fluree/secrets/api-keys.txt"
```

### 3. Document Configuration

Add comments to config files:

```toml
[indexing]
# Increased from default 5000 to reduce novelty lag
interval_ms = 3000

# Increased from default 10 for high transaction volume
batch_size = 20
```

### 4. Test Configuration Changes

Test on dev/staging before production:

```bash
./fluree-db-server --config new-config.toml --dry-run
```

### 5. Monitor After Changes

After configuration changes, monitor:
- Error rates
- Performance metrics
- Resource usage
- Log output

## Related Documentation

- [Storage Modes](storage.md) - Storage backend details
- [Telemetry](telemetry.md) - Monitoring configuration
- [Getting Started: Server](../getting-started/quickstart-server.md) - Initial setup
