# Fluree CLI

The `fluree` command-line interface provides a convenient way to manage ledgers, run queries, and perform transactions without running a server.

## Installation

Build from source:

```bash
cargo build --release -p fluree-db-cli
```

The binary will be at `target/release/fluree`.

## Quick Start

```bash
# Initialize a project directory
fluree init

# Create a ledger
fluree create myledger

# Insert data
fluree insert -e '@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .'

# Query
fluree query --sparql -e 'SELECT ?name WHERE { ?s <http://example.org/name> ?name }'
```

## Global Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable verbose output |
| `-q, --quiet` | Suppress non-essential output |
| `--no-color` | Disable colored output (also respects `NO_COLOR` env var) |
| `--config <PATH>` | Path to config file |
| `--memory-budget-mb <MB>` | Memory budget in MB for bulk import (0 = auto: 75% of system RAM). Affects chunk size, concurrency, and run budget when creating a ledger with `--from`. |
| `--parallelism <N>` | Number of parallel parse threads for bulk import (0 = auto: system cores, default cap 6). Used when creating a ledger with `--from`. |
| `-h, --help` | Print help |
| `-V, --version` | Print version |

## Commands

### Core Commands

| Command | Description |
|---------|-------------|
| [`init`](init.md) | Initialize a new Fluree project directory |
| [`create`](create.md) | Create a new ledger |
| [`use`](use.md) | Set the active ledger |
| [`list`](list.md) | List all ledgers |
| [`info`](info.md) | Show detailed information about a ledger |
| [`drop`](drop.md) | Drop (delete) a ledger |
| [`insert`](insert.md) | Insert data into a ledger |
| [`upsert`](upsert.md) | Upsert data (insert or update existing) |
| [`query`](query.md) | Query a ledger |
| [`history`](history.md) | Show change history for an entity |
| [`export`](export.md) | Export ledger data |
| [`log`](log.md) | Show commit log |

### Remote Sync

| Command | Description |
|---------|-------------|
| [`remote`](remote.md) | Manage remote servers |
| [`upstream`](upstream.md) | Manage upstream tracking configuration |
| [`fetch`](fetch.md) | Fetch refs from a remote |
| [`clone`](clone.md) | Clone a ledger from a remote (full commit download) |
| [`pull`](pull.md) | Pull commits from upstream |
| [`push`](push.md) | Push to upstream remote |
| [`track`](track.md) | Track remote-only ledgers (no local data) |

**Clone and pull** transfer commits and, by default, **binary index data** from the remote (pack protocol), so the local ledger is query-ready without a separate reindex. Use `--no-indexes` to skip index transfer and reduce download size; run `fluree reindex` afterward if you need the index. Large transfers may prompt for confirmation before streaming.

### Server Management

| Command | Description |
|---------|-------------|
| [`server`](server.md) | Manage the Fluree HTTP server (run, start, stop, status, restart, logs) |

Start a server directly from a project directory — it inherits the same `.fluree/` context (config, storage) as the CLI. See [`server`](server.md) for details.

### Implementers

If you're building a custom server that must support the CLI end-to-end (for example, integrating into another app), see:

- [`server-integration`](server-integration.md) - endpoints and auth contract required by the CLI

### Authentication

| Command | Description |
|---------|-------------|
| [`token`](token.md) | Create, inspect, and manage JWS tokens |
| [`auth`](auth.md) | Manage bearer tokens stored on remotes (login/logout/status) |

### Configuration

| Command | Description |
|---------|-------------|
| [`config`](config.md) | Manage configuration |
| [`prefix`](prefix.md) | Manage IRI prefix mappings |
| [`completions`](completions.md) | Generate shell completions |

## Project Structure

When you run `fluree init`, a `.fluree/` directory is created with:

```
.fluree/
├── active          # Currently active ledger name
├── config.toml     # Configuration settings
├── prefixes.json   # IRI prefix mappings
└── storage/        # Ledger data storage
```

## Input Resolution

Commands that accept data input (`insert`, `upsert`, `query`) use flexible argument resolution:

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger + stdin or `-e` expression |
| `<arg>` | If file exists: active ledger + file; else: ledger ID + stdin/-e |
| `<ledger> <file>` | Specified ledger + file |

## Data Format Detection

The CLI auto-detects data format based on content:
- Lines starting with `@prefix` or `@base` → Turtle
- Content starting with `{` or `[` → JSON-LD
- Files with `.ttl` extension → Turtle
- Files with `.json` or `.jsonld` extension → JSON-LD

You can override with `--format turtle` or `--format jsonld`.
