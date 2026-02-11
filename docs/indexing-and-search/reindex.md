# Reindex API

The Reindex API provides full rebuilds of ledger indexes from the commit chain. Use this when you need to rebuild indexes from scratch, such as after suspected corruption or index configuration changes.

## Overview

Unlike [background indexing](background-indexing.md) which incrementally updates indexes as transactions commit, reindexing rebuilds the entire binary columnar index from the commit history.

## When to Reindex

### Common Use Cases

1. **Index corruption** - Query errors or unexpected results suggest corrupted indexes
2. **Configuration changes** - Changing index parameters (leaf size, branch size)
3. **Storage backend changes** - If you move a deployment between storage backends or adopt a new index strategy/type.

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
println!("Root ID: {}", result.root_id);
```

### Reindex with Custom Options

```rust
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_indexer::IndexerConfig;

let fluree = FlureeBuilder::file("/path/to/data").build().await?;

let result = fluree.reindex("mydb:main", ReindexOptions::default()
    // Use custom index node sizes
    .with_indexer_config(IndexerConfig::large())
).await?;
```

## ReindexOptions Reference

| Option | Default | Description |
|--------|---------|-------------|
| `indexer_config` | `IndexerConfig::default()` | Controls output index structure (leaf/branch sizes, GC settings) |

### `indexer_config`

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

## ReindexResult

The reindex operation returns:

```rust
pub struct ReindexResult {
    /// Ledger ID
    pub ledger_id: String,
    /// Transaction time the index was built to
    pub index_t: i64,
    /// ContentId of the new index root
    pub root_id: ContentId,
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
        // Ledger advanced during reindex (new commits arrived)
        println!("Conflict: expected t={}, found t={}", expected, found);
    }
    Err(e) => {
        // Storage, indexing, or other errors
        println!("Reindex failed: {}", e);
    }
}
```

## How It Works

The reindex operation:

1. **Looks up** the current ledger state and captures `commit_t` for conflict detection
2. **Cancels** any active background indexing for the ledger
3. **Builds** a fresh binary columnar index from the full commit chain using `build_binary_index`
4. **Validates** that no new commits arrived during the build (conflict detection)
5. **Publishes** the new index root via `publish_index_allow_equal`
6. **Spawns** async garbage collection to clean up old index versions

## Best Practices

### 1. Schedule During Low-Traffic Periods

While queries continue to work during reindex, performance may be impacted. Schedule large reindex operations during maintenance windows when possible.

### 2. Verify After Reindex

After reindex, verify the results:

```rust
// Get ledger info to check state
let info = fluree.ledger_info(ledger_id).execute().await?;
println!("Index rebuilt to t={}", info["index"]["t"]);

// Run a sample query to verify correctness
let query_result = fluree.query(&ledger, &sample_query).await?;
```

### 3. Concurrent Operations

During reindex:
- Queries continue to work (using old index + novelty)
- Transactions continue to work (writes to novelty)
- Background indexing is paused for this ledger

## Related Documentation

- [Background Indexing](background-indexing.md) - Automatic incremental indexing
- [Admin and Health](../operations/admin-and-health.md) - Admin operations
- [Rust API](../getting-started/rust-api.md) - Using Fluree as a library
- [Storage](../operations/storage.md) - Storage configuration
