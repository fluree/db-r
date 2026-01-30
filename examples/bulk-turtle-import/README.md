# Bulk Turtle Import

Stream-reads a large `.ttl` file and transacts it into Fluree in configurable
batches using the `fluree-db-api` library (no server required).

Each batch is:

1. **Read** from the file line-by-line (only one batch of text in memory at a
   time plus the prefix header).
2. **Parsed** from Turtle to JSON-LD via `fluree_graph_turtle::parse_to_json()`
   — this happens **outside** the commit lock.
3. **Committed** via `.insert(&json).commit()` which enters Fluree's per-ledger
   FIFO write queue (a tokio Mutex inside the cached `LedgerHandle`).

Multiple commits can be in-flight simultaneously: while one batch is being
staged+persisted under the lock, the next batches are already parsed and waiting
in the queue.

## Quick start

```bash
# Minimal — in-memory, default 10k statements/batch:
cargo run -p bulk-turtle-import --release -- path/to/data.ttl

# With persistent file storage:
cargo run -p bulk-turtle-import --release -- path/to/data.ttl \
    --storage ./fluree-data

# See all options:
cargo run -p bulk-turtle-import --release -- --help
```

## CLI flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `<FILE>` | | (required) | Path to the `.ttl` file |
| `--ledger` | `-l` | `import` | Ledger alias (`:main` appended if no branch) |
| `--batch-size` | `-b` | `10000` | RDF statements per batch |
| `--storage` | `-s` | (none = in-memory) | Directory for persistent file storage |
| `--inflight` | | `4` | Max pipelined in-flight commit tasks |
| `--reindex-min-bytes` | | `10000000` (10 MB) | Novelty size that triggers background indexing |
| `--reindex-max-bytes` | | `200000000` (200 MB) | Novelty size that **blocks** new commits |
| `--no-indexing` | | `false` | Disable background indexing entirely |

## Testing with the Albania OSM dataset

The `alb.osm.ttl` file is ~11 GB / ~198 M lines (each line is roughly one
statement in this dataset):

```bash
# Quick sanity check — first 100k statements, in-memory:
cargo run -p bulk-turtle-import --release -- \
    ~/Downloads/open-street-map/alb.osm.ttl \
    -b 50000 -l osm-alb

# Full import to disk with larger batches:
cargo run -p bulk-turtle-import --release -- \
    ~/Downloads/open-street-map/alb.osm.ttl \
    -b 100000 --storage ./osm-data -l osm-alb --inflight 8
```

Because the file is ~198 M statements and the default batch size is 10k, the
default would produce ~19,800 transactions. For a file this large, batch sizes
of 50k–100k are a more practical starting point.

## Logging

The binary uses `tracing-subscriber` with an `EnvFilter`. By default only
warnings and the binary's own `info` logs are shown.

Override with `RUST_LOG` to see Fluree internals:

```bash
# See commit and staging details:
RUST_LOG=info,fluree_db_transact=debug,fluree_db_ledger=debug \
    cargo run -p bulk-turtle-import --release -- data.ttl

# See everything (very verbose):
RUST_LOG=debug cargo run -p bulk-turtle-import --release -- data.ttl

# Only this binary at trace, Fluree crates at info:
RUST_LOG=info,bulk_turtle_import=trace \
    cargo run -p bulk-turtle-import --release -- data.ttl
```

Crate names use underscores in `RUST_LOG` directives. Useful targets:

| Target | What it shows |
|--------|---------------|
| `bulk_turtle_import` | This binary's batch progress |
| `fluree_db_api` | Graph API and ledger manager |
| `fluree_db_transact` | Transaction staging and commit |
| `fluree_db_ledger` | Ledger state and novelty |
| `fluree_db_indexer` | Background indexing |
| `fluree_graph_turtle` | Turtle parser |

## Tuning indexing and novelty

The library API defaults to `IndexingMode::Disabled` with a 1 MB hard novelty
limit. A single 100k-statement batch easily exceeds this, causing a
`NoveltyWouldExceed` error. This example solves it via `connect_json_ld()` with
`defaults.indexing` settings — the same mechanism `fluree-db-server` uses.

### How it works

The `--reindex-min-bytes`, `--reindex-max-bytes`, and `--no-indexing` CLI flags
are passed into a JSON-LD connection config with a `defaults.indexing` block:

```json
"defaults": {
  "indexing": {
    "indexingEnabled": true,
    "reindexMinBytes": 10000000,
    "reindexMaxBytes": 200000000
  }
}
```

When `connect_json_ld()` sees `indexingEnabled: true`, it automatically starts a
`BackgroundIndexerWorker` (identical to what `fluree-db-server` does at startup).
The `reindexMinBytes` / `reindexMaxBytes` values become the default `IndexConfig`
for every commit on this connection.

### Recommended settings by use case

| Scenario | Flags | Notes |
|----------|-------|-------|
| **Default** (balanced) | (none) | 10 MB soft / 200 MB hard, indexing on |
| **Maximum throughput** | `--no-indexing --reindex-max-bytes 2000000000` | No indexing during import; 2 GB hard limit; reindex after |
| **Large batches** | `--reindex-max-bytes 500000000` | 500 MB hard limit gives more headroom |
| **Small machine** | `--reindex-min-bytes 5000000 --reindex-max-bytes 50000000` | Tighter limits, more frequent indexing |

### Advanced: per-commit IndexConfig

The CLI flags set connection-level defaults. For finer control, you can also
pass `IndexConfig` per-commit via `.index_config()` on the transact builder:

```rust
use fluree_db_api::IndexConfig;

fluree.graph(&alias)
    .transact()
    .insert(&json)
    .index_config(IndexConfig {
        reindex_min_bytes: 100_000_000,   // 100 MB
        reindex_max_bytes: 500_000_000,   // 500 MB
    })
    .commit()
    .await?;
```

### Advanced: full JSON-LD connection config

For control beyond what the CLI flags expose (cache size, parallelism, S3
storage, etc.), edit `build_connection_config()` in `main.rs` or use
`connect_json_ld()` directly. See `docs/reference/connection-config-jsonld.md`
for the full schema.

### Memory vs. file storage

| Mode | Pros | Cons |
|------|------|------|
| In-memory (default) | Fastest commits, no disk I/O | Data lost on exit; RAM-bound |
| File (`--storage`) | Persistent, survives restart | Slower commits (fsync), disk space |

For benchmarking raw ingest throughput, start with in-memory. For a realistic
end-to-end test, use file storage.

## How the pipeline works

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │  main loop (single thread)                                          │
 │                                                                     │
 │  for each batch:                                                    │
 │    1. Read lines from .ttl  ──►  BatchReader iterator               │
 │    2. parse_to_json()       ──►  serde_json::Value  (CPU, no lock)  │
 │    3. tokio::spawn(commit)  ──►  enters FIFO write queue            │
 │       ▲                                                             │
 │       └─ bounded by Semaphore(inflight)                             │
 └──────────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────────┐
 │  spawned commit tasks (tokio runtime)                               │
 │                                                                     │
 │  task N:   acquire LedgerHandle Mutex  ──►  stage  ──►  commit      │
 │  task N+1: (waiting on Mutex — already parsed, ready to go)         │
 │  task N+2: (waiting on Mutex)                                       │
 │  ...                                                                │
 └──────────────────────────────────────────────────────────────────────┘
```

The main loop reads + parses ahead while commits serialize. With `--inflight 4`
(default), up to 3 batches can be pre-parsed and queued while the current batch
commits.
