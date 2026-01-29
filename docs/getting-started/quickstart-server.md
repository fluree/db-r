# Quickstart: Run the Server

This guide will get the Fluree server running on your machine in minutes.

## Installation

### Option 1: Download Pre-built Binary

Download the latest release for your platform:

```bash
# Linux
curl -L https://github.com/fluree/db-rust/releases/latest/download/fluree-db-server-linux -o fluree-db-server
chmod +x fluree-db-server

# macOS
curl -L https://github.com/fluree/db-rust/releases/latest/download/fluree-db-server-macos -o fluree-db-server
chmod +x fluree-db-server

# Windows
# Download fluree-db-server-windows.exe from GitHub releases
```

### Option 2: Build from Source

If you have Rust installed:

```bash
# Clone the repository
git clone https://github.com/fluree/db-rust.git
cd db-rust

# Build the server
cargo build --release --bin fluree-db-server

# Binary will be at target/release/fluree-db-server
```

### Option 3: Docker

```bash
# Pull the image
docker pull fluree/db-rust:latest

# Run the container
docker run -p 8090:8090 fluree/db-rust:latest
```

## Start the Server

### Default Configuration

Start the server with default settings (memory storage, port 8090):

```bash
./fluree-db-server
```

You should see output like:

```text
2024-01-22T10:00:00.000Z INFO  fluree_db_server] Starting Fluree server
2024-01-22T10:00:00.100Z INFO  fluree_db_server] Storage mode: memory
2024-01-22T10:00:00.200Z INFO  fluree_db_server] Server listening on 0.0.0.0:8090
2024-01-22T10:00:00.250Z INFO  fluree_db_server] Ready to accept requests
```

### Custom Configuration

Specify configuration via command-line flags:

```bash
# Use file storage
./fluree-db-server --storage file --data-dir /path/to/data

# Custom port
./fluree-db-server --port 9090

# AWS S3 storage
./fluree-db-server --storage aws \
  --s3-bucket my-fluree-bucket \
  --s3-region us-east-1

# Enable debug logging
./fluree-db-server --log-level debug
```

### Configuration File

Create a configuration file `fluree-config.toml`:

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

[indexing]
auto_index = true
index_interval_ms = 5000
```

Run with configuration file:

```bash
./fluree-db-server --config fluree-config.toml
```

## Verify Installation

### Check Server Health

```bash
curl http://localhost:8090/health
```

Expected response:

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "memory"
}
```

### Check Server Status

```bash
curl http://localhost:8090/status
```

Expected response:

```json
{
  "uptime_ms": 12345,
  "ledgers": [],
  "storage": {
    "mode": "memory",
    "total_bytes": 0
  }
}
```

## Understanding the Server

### Port and Endpoints

By default, the server listens on port 8090 and provides:

- `POST /transact` - Submit transactions
- `POST /query` - Execute queries
- `GET /health` - Health check endpoint
- `GET /status` - Server status and statistics
- `GET /ledgers` - List all ledgers

See the [HTTP API](../api/README.md) documentation for full endpoint details.

### Storage Modes

Fluree supports three storage modes:

**Memory** (default for development):
- Fast, in-process storage
- Data is lost when server stops
- Best for development and testing

**File** (recommended for single-server production):
- Persistent local file system storage
- Data survives server restarts
- Best for single-server deployments

**AWS** (recommended for distributed production):
- Uses S3 for object storage and DynamoDB for coordination
- Enables multi-server deployments
- Best for scalable, distributed systems

See [Storage Modes](../operations/storage.md) for detailed comparison.

### Logging

The server outputs structured logs with various levels:

- `ERROR` - Critical errors requiring attention
- `WARN` - Warning conditions
- `INFO` - Informational messages (default)
- `DEBUG` - Detailed debugging information
- `TRACE` - Very detailed tracing (rarely needed)

Control logging with the `--log-level` flag or `RUST_LOG` environment variable.

## Common Configuration Options

### Development Setup

For local development with fast startup:

```bash
./fluree-db-server \
  --storage memory \
  --log-level debug
```

### Single-Server Production

For a single production server:

```bash
./fluree-db-server \
  --storage file \
  --data-dir /var/lib/fluree \
  --port 8090 \
  --log-level info
```

### Distributed Production

For distributed deployment with AWS:

```bash
./fluree-db-server \
  --storage aws \
  --s3-bucket fluree-prod-data \
  --s3-region us-east-1 \
  --dynamodb-table fluree-nameservice \
  --log-level info
```

## Docker Deployment

### Basic Docker Run

```bash
docker run -d \
  --name fluree \
  -p 8090:8090 \
  fluree/db-rust:latest
```

### With File Storage

```bash
docker run -d \
  --name fluree \
  -p 8090:8090 \
  -v /path/to/data:/data \
  -e FLUREE_STORAGE=file \
  -e FLUREE_DATA_DIR=/data \
  fluree/db-rust:latest
```

### Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  fluree:
    image: fluree/db-rust:latest
    ports:
      - "8090:8090"
    environment:
      FLUREE_STORAGE: file
      FLUREE_DATA_DIR: /data
      FLUREE_LOG_LEVEL: info
    volumes:
      - fluree-data:/data
    restart: unless-stopped

volumes:
  fluree-data:
```

Start with:

```bash
docker-compose up -d
```

## Troubleshooting

### Port Already in Use

If port 8090 is already in use:

```bash
# Use a different port
./fluree-db-server --port 9090
```

### Permission Denied (File Storage)

Ensure the server has write permissions:

```bash
sudo chown -R $USER:$USER /var/lib/fluree
chmod -R 755 /var/lib/fluree
```

### Cannot Connect to AWS

Verify AWS credentials are configured:

```bash
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1
```

### Server Won't Start

Check the logs for specific error messages:

```bash
./fluree-db-server --log-level debug
```

## Next Steps

Now that your server is running:

1. [Create a Ledger](quickstart-ledger.md) - Set up your first database
2. [Write Data](quickstart-write.md) - Insert your first records
3. [Query Data](quickstart-query.md) - Retrieve and explore your data

For production deployments, review:
- [Configuration](../operations/configuration.md) - Comprehensive configuration guide
- [Storage Modes](../operations/storage.md) - Detailed storage options
- [Operations](../operations/README.md) - Operational best practices
