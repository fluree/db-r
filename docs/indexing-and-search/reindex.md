# Reindex API

The Reindex API provides memory-bounded, resumable full rebuilds of ledger indexes. Use this when you need to rebuild indexes from scratch, such as after schema changes, suspected corruption, or index configuration changes.

## Overview

Unlike [background indexing](background-indexing.md) which incrementally updates indexes as transactions commit, reindexing rebuilds the entire index from the commit history. The reindex process is:

- **Memory-bounded**: Processes data in configurable batches to avoid OOM
- **Resumable**: Optional checkpointing allows resumption after interruption
- **Observable**: Progress callbacks provide real-time status updates

## When to Reindex

### Common Use Cases

1. **Index corruption** - Query errors or unexpected results suggest corrupted indexes
2. **Configuration changes** - Changing index parameters (leaf size, branch size)
3. **Migration** - Moving between storage backends or if new index strategies / types become available in future versions.

### Before You Reindex

Consider these factors:

- **Duration**: Full reindex scales with ledger size; large ledgers may take hours
- **Resources**: Ensure adequate memory and storage during the operation
- **Availability**: Queries remain available during reindex, but may be slower
- **Backup**: Be sure to back up data before major reindex operations

## Rust API

The reindex API is exposed through the `Fluree` type in `fluree-db-api`. In Rust, `Fluree` serves as the equivalent of Clojure's "connection" object - it owns the storage backend, node cache, nameservice, and provides all ledger operations including queries, transactions, and admin functions like reindex.

### Basic Reindex

```rust
use fluree_db_api::{FlureeBuilder, ReindexOptions, ReindexResult};

// Create Fluree instance
let fluree = FlureeBuilder::file("/path/to/data")
    .build()
    .await?;

// Reindex with default options
let result: ReindexResult = fluree.reindex("mydb:main", ReindexOptions::default()).await?;

println!("Reindexed to t={}", result.index_t);
println!("Root address: {}", result.root_address);
```

### Reindex with Custom Options

```rust
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_indexer::IndexerConfig;

let fluree = FlureeBuilder::file("/path/to/data").build().await?;

let result = fluree.reindex("mydb:main", ReindexOptions::default()
    // Use custom index node sizes
    .with_indexer_config(IndexerConfig::large())
    // Process ~100MB per batch (default)
    .with_batch_bytes(100_000_000)
    // Max 10,000 commits per batch
    .with_max_batch_commits(10_000)
    // Enable checkpointing for resumability
    .with_checkpoint(true)
    // Checkpoint every 5 batches
    .with_checkpoint_interval(5)
).await?;
```

## ReindexOptions Reference

| Option | Default | Description |
|--------|---------|-------------|
| `indexer_config` | `IndexerConfig::default()` | Controls output index structure (leaf/branch sizes) |
| `batch_bytes` | 100,000,000 | Target bytes per batch (~100MB) |
| `max_batch_commits` | 100,000 | Maximum commits per batch |
| `checkpoint` | `false` | Enable checkpointing for resumable operations |
| `checkpoint_interval` | 1 | Flush checkpoint every N batches |
| `progress_callback` | `None` | Callback for progress updates |

### Option Details

#### `batch_bytes` and `max_batch_commits`

These control memory usage during reindex:

- `batch_bytes`: Primary limit on batch size. When accumulated flake data reaches this threshold, the batch is flushed to storage.
- `max_batch_commits`: Secondary limit. Even if batch_bytes isn't reached, flush after this many commits.

For memory-constrained environments:
```rust
ReindexOptions::default()
    .with_batch_bytes(10_000_000)   // 10MB batches
    .with_max_batch_commits(1_000)  // Max 1000 commits
```

For high-memory environments:
```rust
ReindexOptions::default()
    .with_batch_bytes(200_000_000)    // 200MB batches
    .with_max_batch_commits(500_000)  // Up to 500k commits
```

#### `indexer_config`

Controls the output index structure:

```rust
use fluree_db_indexer::IndexerConfig;

// For small datasets (< 100k flakes)
ReindexOptions::default()
    .with_indexer_config(IndexerConfig::small())

// For large datasets (> 10M flakes)
ReindexOptions::default()
    .with_indexer_config(IndexerConfig::large())

// Custom configuration
let config = IndexerConfig::default()
    .with_gc_max_old_indexes(10)  // Keep more old index versions
    .with_gc_min_time_mins(60);   // Retain for at least 60 minutes

ReindexOptions::default()
    .with_indexer_config(config)
```

## Checkpointing and Resumption

For long-running reindex operations, enable checkpointing to allow resumption after interruption (crash, timeout, deployment, etc.).

### Enable Checkpointing

```rust
let result = fluree.reindex("mydb:main", ReindexOptions::default()
    .with_checkpoint(true)
    .with_checkpoint_interval(5)  // Save every 5 batches
).await?;
```

### Resume After Interruption

If a checkpointed reindex is interrupted, resume from where it left off:

```rust
// Check if a checkpoint exists
if let Some(checkpoint) = fluree.reindex_checkpoint("mydb:main").await? {
    println!("Found checkpoint at t={} of {}",
        checkpoint.last_processed_t,
        checkpoint.target_t);

    // Resume the reindex
    let result = fluree.resume_reindex("mydb:main").await?;
    println!("Resumed reindex complete, t={}", result.index_t);
} else {
    println!("No checkpoint found, starting fresh reindex");
    fluree.reindex("mydb:main", ReindexOptions::default()).await?;
}
```

### Checkpoint Behavior

- **Automatic cleanup**: Checkpoints are deleted on successful completion
- **Head validation**: Resume fails if ledger has new commits since checkpoint
- **Config preservation**: IndexerConfig is restored from checkpoint
- **Stale detection**: If head changes, checkpoint is deleted and error returned

### Delete Stale Checkpoint

If you want to start fresh instead of resuming:

```rust
fluree.delete_reindex_checkpoint("mydb:main").await?;
fluree.reindex("mydb:main", ReindexOptions::default()).await?;
```

## Progress Monitoring

Track reindex progress with a callback:

```rust
use fluree_db_indexer::ReindexProgress;
use std::sync::Arc;

let callback: Arc<dyn Fn(&ReindexProgress) + Send + Sync> = Arc::new(|progress| {
    println!(
        "[{}/{}] Batch {} flushed, {:.1} commits/sec, ~{:.0}s remaining",
        progress.commits_processed,
        progress.total_commits,
        progress.batches_flushed,
        progress.commits_per_sec,
        progress.estimated_remaining_secs.unwrap_or(0.0)
    );
});

let result = fluree.reindex("mydb:main", ReindexOptions::default()
    .with_progress_callback(callback)
).await?;
```

### ReindexProgress Fields

| Field | Type | Description |
|-------|------|-------------|
| `alias` | `String` | Ledger alias being reindexed |
| `total_commits` | `usize` | Total commits to process |
| `commits_processed` | `usize` | Commits processed so far |
| `batches_flushed` | `usize` | Number of batches written to storage |
| `bytes_flushed` | `u64` | Total bytes written to storage |
| `current_t` | `i64` | Current transaction time |
| `target_t` | `i64` | Target (head) transaction time |
| `commits_per_sec` | `f64` | Current throughput |
| `estimated_remaining_secs` | `Option<f64>` | Estimated time remaining |

## ReindexResult

The reindex operation returns:

```rust
pub struct ReindexResult {
    /// Ledger alias
    pub alias: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// Storage address of the new index root
    pub root_address: String,
    /// Index build statistics
    pub stats: IndexStats,
}
```

## Error Handling

### Common Errors

```rust
use fluree_db_api::ApiError;

match fluree.reindex("mydb:main", opts).await {
    Ok(result) => println!("Success: t={}", result.index_t),
    Err(ApiError::NotFound(msg)) => {
        // Ledger doesn't exist or has no commits
        println!("Ledger not found: {}", msg);
    }
    Err(ApiError::ReindexConflict { expected, found }) => {
        // Head changed during checkpointed reindex
        println!("Conflict: expected t={}, found t={}", expected, found);
    }
    Err(e) => {
        // Storage, indexing, or other errors
        println!("Reindex failed: {}", e);
    }
}
```

### Resume Errors

```rust
match fluree.resume_reindex("mydb:main").await {
    Ok(result) => println!("Resumed to t={}", result.index_t),
    Err(ApiError::NotFound(_)) => {
        // No checkpoint exists
        println!("No checkpoint found, use reindex() instead");
    }
    Err(ApiError::ReindexConflict { expected, found }) => {
        // Ledger has new commits since checkpoint
        // Checkpoint was automatically deleted
        println!("Stale checkpoint (had t={}, now t={})", expected, found);
        println!("Starting fresh reindex...");
        fluree.reindex("mydb:main", ReindexOptions::default()).await?;
    }
    Err(e) => println!("Resume failed: {}", e),
}
```

## Best Practices

### 1. Use Checkpointing for Large Ledgers

For ledgers with >100,000 transactions, always enable checkpointing:

```rust
ReindexOptions::default()
    .with_checkpoint(true)
    .with_checkpoint_interval(10)  // Every 10 batches
```

### 2. Size Batches Appropriately

Match batch size to available memory:

```rust
// Conservative (low memory, ~256MB heap)
ReindexOptions::default()
    .with_batch_bytes(10_000_000)

// Standard (typical server, ~2GB heap)
ReindexOptions::default()
    .with_batch_bytes(50_000_000)

// Aggressive (high memory, ~8GB+ heap)
ReindexOptions::default()
    .with_batch_bytes(200_000_000)
```

### 3. Monitor Progress

Always attach a progress callback for visibility:

```rust
let callback = Arc::new(|p: &ReindexProgress| {
    tracing::info!(
        alias = %p.alias,
        progress = format!("{}/{}", p.commits_processed, p.total_commits),
        rate = format!("{:.1}/s", p.commits_per_sec),
        "Reindex progress"
    );
});

ReindexOptions::default()
    .with_progress_callback(callback)
```

### 4. Handle Interruptions Gracefully

Design your application to check for and resume incomplete reindexes:

```rust
async fn ensure_reindex_complete<S, N>(fluree: &Fluree<S, SimpleCache, N>, alias: &str) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService,
{
    // Check for interrupted reindex
    if fluree.reindex_checkpoint(alias).await?.is_some() {
        tracing::info!("Resuming interrupted reindex for {}", alias);
        fluree.resume_reindex(alias).await?;
    }
    Ok(())
}
```

### 5. Schedule During Low-Traffic Periods

While queries continue to work during reindex, performance may be impacted. Schedule large reindex operations during maintenance windows when possible.

### 6. Verify After Reindex

After reindex, verify the results:

```rust
// Get ledger info to check state
let info = fluree.ledger_info(alias).execute().await?;
println!("Index rebuilt to t={}", info["index"]["t"]);

// Run a sample query to verify correctness
let query_result = fluree.query(&ledger, &sample_query).await?;
```

## Performance Considerations

### Memory Usage

Memory is bounded by `batch_bytes`:
- Peak memory ≈ 2-3× `batch_bytes` (current batch + write buffer + overhead)
- Larger batches = fewer flushes = faster overall
- Smaller batches = lower peak memory = safer

### Storage I/O

Each batch flush writes to storage:
- More batches = more I/O operations
- Use larger batches on fast storage (SSD, NVMe)
- Use smaller batches on slow storage (network, HDD)

### CPU Usage

Reindex is CPU-intensive:
- Index tree construction
- Flake sorting and deduplication
- Serialization

Consider running during off-peak hours.

### Concurrent Operations

During reindex:
- Queries continue to work (using old index + novelty)
- Transactions continue to work (writes to novelty)
- Background indexing is paused for this ledger

## Related Documentation

- [Background Indexing](background-indexing.md) - Automatic incremental indexing
- [Admin and Health](../operations/admin-and-health.md) - Admin operations
- [Rust API](../getting-started/rust-api.md) - Using Fluree as a library
- [Storage](../operations/storage.md) - Storage configuration
