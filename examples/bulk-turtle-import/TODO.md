# TODO — Bulk Turtle Import Example

## Problem

Running against a large TTL file (e.g. `alb.osm.ttl`, 11 GB / ~198M statements)
fails immediately on the first batch:

```
Error: Transact(NoveltyWouldExceed { current_bytes: 0, delta_bytes: 9470251, max_bytes: 1000000 })
```

A single 100k-statement batch produces ~9.5 MB of novelty, but the default
`IndexConfig::reindex_max_bytes` is only 1 MB. The transaction is rejected at
staging time before any commit happens.

## Root cause

The library API (`connect_memory`, `connect_filesystem`, `FlureeBuilder::build`)
creates a `Fluree` instance with **`IndexingMode::Disabled`** by default
(`fluree-db-api/src/lib.rs:2079`). Without background indexing, novelty is never
flushed, so even a single large batch can exceed the hard limit.

The HTTP server works because it wires up a `BackgroundIndexerWorker` at startup,
which drains novelty between commits. The library user must do this manually.

### Key code paths

| What | File | Lines |
|------|------|-------|
| Backpressure rejection | `fluree-db-transact/src/stage.rs` | ~140-146 |
| `at_max_novelty()` check | `fluree-db-ledger/src/lib.rs` | ~189-191 |
| `IndexConfig` defaults (100KB soft / 1MB hard) | `fluree-db-ledger/src/lib.rs` | ~42-60 |
| `IndexingMode::Disabled` default | `fluree-db-api/src/lib.rs` | ~2079-2089 |
| Indexing trigger after commit | `fluree-db-api/src/tx.rs` | ~353-368 |
| `.index_config()` on transact builder | `fluree-db-api/src/graph_transact_builder.rs` | ~116 |

## Fix options

### Option A — Raise limits via `.index_config()` (quick, no indexing)

Pass a custom `IndexConfig` on each commit to raise the hard ceiling:

```rust
use fluree_db_ledger::IndexConfig;

let big_config = IndexConfig {
    reindex_min_bytes: 500_000_000,   // 500 MB soft
    reindex_max_bytes: 2_000_000_000, // 2 GB hard
};

fluree.graph(&alias)
    .transact()
    .insert(&json)
    .index_config(big_config)
    .commit()
    .await?;
```

Pros: one-line fix, no threading.
Cons: novelty stays in RAM forever (no background indexing), queries slow down
as novelty grows.  Suitable for import-then-reindex workflows.

### Option B — Enable background indexing (production-grade)

Start a `BackgroundIndexerWorker` and wire it into the `Fluree` instance so
novelty is drained automatically between commits:

```rust
use fluree_db_api::{BackgroundIndexerWorker, IndexingMode};

let mut fluree = FlureeBuilder::file("./data")
    .with_ledger_caching()
    .build()?;

let worker = BackgroundIndexerWorker::new(
    fluree.clone(),               // needs Arc or Clone
    Duration::from_secs(5),       // poll interval
);
let indexer_handle = worker.start();
fluree.set_indexing_mode(IndexingMode::Background(indexer_handle));
```

This mirrors what `fluree-db-server` does at startup. Commits trigger indexing
when novelty exceeds `reindex_min_bytes`, keeping novelty bounded. The default
1 MB hard limit then works because novelty is flushed between batches.

Pros: fully automatic, mirrors server behavior, queries stay fast.
Cons: more setup, need to manage worker lifecycle (shutdown on exit).

### Option C — Combine both (recommended for this example)

1. Enable background indexing (Option B) so novelty is drained.
2. Also raise `IndexConfig` limits moderately so that large batches aren't
   rejected while the indexer catches up:

```rust
let config = IndexConfig {
    reindex_min_bytes:  10_000_000,  // 10 MB — trigger indexing
    reindex_max_bytes: 200_000_000,  // 200 MB — block only if way behind
};
```

This gives the indexer room to work without blocking the import pipeline.

### Option D — Smaller batches (workaround, no code changes)

Use `--batch-size 1000` or similar so each batch produces < 1 MB of novelty.
Works with defaults but results in many more transactions and slower throughput.

## Implementation plan

1. ~~Add CLI flags for `--reindex-min-bytes` and `--reindex-max-bytes` so the user
   can tune novelty thresholds without editing source.~~ **Done.**
2. ~~Wire up `BackgroundIndexerWorker` in `main()` so indexing runs automatically.
   Add a `--no-indexing` flag to disable it (for import-then-reindex workflows).~~
   **Done** — used `connect_json_ld()` with `defaults.indexing.indexingEnabled`
   instead of manual worker wiring. `connect_json_ld()` starts the worker
   automatically when `indexingEnabled: true`, identical to `fluree-db-server`.
3. ~~Pass the `IndexConfig` into each spawned commit task via `.index_config()`.~~
   **Not needed** — `reindexMinBytes` / `reindexMaxBytes` in the JSON-LD
   connection config become the default `IndexConfig` for all commits.
4. ~~Shut down the indexer worker cleanly after the import loop finishes.~~
   **Handled by tokio runtime shutdown** — the worker is spawned via
   `tokio::spawn` inside `connect_json_ld` and is cancelled when the runtime
   drops at process exit.
5. ~~Update README with guidance on tuning for large files.~~ **Done.**
