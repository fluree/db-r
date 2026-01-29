# Using Fluree as a Rust Library

This guide shows how to use Fluree programmatically in your Rust applications by depending on the `fluree-db-api` crate.

## Overview

Fluree can be embedded directly in Rust applications, giving you a powerful graph database without requiring a separate server process. This is ideal for:

- Desktop applications
- Edge computing
- Embedded systems
- Library/framework integration
- Testing and development

## Add Dependency

Add Fluree to your `Cargo.toml`:

```toml
[dependencies]
fluree-db-api = { path = "../fluree-db-api" }
tokio = { version = "1", features = ["full"] }
```

Note: Replace `path` with version when published to crates.io:
```toml
[dependencies]
fluree-db-api = "0.1"
```

### Features

Available feature flags:

- `native` (default) - File storage support
- `aws` - AWS-backed storage support (S3, storage-backed nameservice). Enables `connect_s3` and `connect_json_ld` configs that use S3.
- `vector` - Embedded vector similarity search (HNSW indexes via usearch)
- `credential` - DID/JWS/VerifiableCredential support for signed queries and transactions (pulls in crypto dependencies like `ed25519-dalek`, `bs58`). Off by default to reduce compile times.
- `iceberg` - Apache Iceberg/R2RML virtual graph support (pulls in AWS SDK + native deps)
- `shacl` - SHACL validation support (requires fluree-db-transact + fluree-db-shacl)
- `search-remote-client` - Remote search service client (HTTP client for remote BM25 and vector search services)
- `aws-testcontainers` - Opt-in LocalStack-backed S3/DynamoDB tests (auto-start via testcontainers)
- `full` - Enable all features (`native`, `credential`, `iceberg`, `shacl`)

## Quick Start

### Basic Setup

```rust
use fluree_db_api::{connect_memory, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a memory-backed Fluree instance (JSON-LD under the hood)
    let fluree = connect_memory().await?;

    // Create a new ledger
    let ledger = fluree.create_ledger("mydb").await?;

    println!("Ledger created at t={}", ledger.t());

    Ok(())
}
```

### With File Storage

```rust
use fluree_db_api::{connect_filesystem, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Use file-backed storage for persistence
    let fluree = connect_filesystem("./data").await?;

    // Create a new ledger (or load an existing one)
    let ledger = fluree.create_ledger("mydb").await?;

    // Load an existing ledger by alias
    let ledger = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

### With S3 Storage

Requires `fluree-db-api` feature `aws` and standard AWS credential/region configuration.

```rust
use fluree_db_api::{connect_s3, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // LocalStack/MinIO: endpoint is required for parity with Clojure connect-s3
    let fluree = connect_s3("my-bucket", "http://localhost:4566").await?;

    let ledger = fluree.create_ledger("mydb").await?;
    println!("Ledger created at t={}", ledger.t());
    Ok(())
}
```

**S3 Express One Zone note:** for directory buckets (`--x-s3` suffix), prefer using `connect_json_ld`
and omit `s3Endpoint`. (If you call `connect_s3` with an Express bucket, Rust will omit `s3Endpoint`
automatically.)

## Connection Configuration (JSON-LD)

All connection features are available through **JSON-LD configuration**. Convenience helpers
(`connect_memory`, `connect_filesystem`, `connect_s3`) are just thin wrappers that generate JSON-LD
and call `connect_json_ld`.

See also: [JSON-LD connection configuration reference](../reference/connection-config-jsonld.md).

```rust
use fluree_db_api::{connect_json_ld, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = json!({
        "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
        "@graph": [
            {"@id": "s3Index", "@type": "Storage", "s3Bucket": {"envVar": "INDEX_BUCKET"}, "s3Endpoint": {"envVar": "S3_ENDPOINT"}},
            {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "s3Index"}}
        ]
    });
    let fluree = connect_json_ld(&cfg).await?;
    Ok(())
}
```

### Environment variables (`ConfigurationValue`)

Any string/number config value can be specified directly or via a `ConfigurationValue` object:

```json
{
  "s3Bucket": { "envVar": "FLUREE_S3_BUCKET", "defaultVal": "my-bucket" },
  "cacheMaxMb": { "envVar": "FLUREE_CACHE_MAX_MB", "defaultVal": "1024" }
}
```

### Supported JSON-LD fields (Rust)

Connection node:
- `parallelism`
- `cacheMaxMb`
- `indexStorage`, `commitStorage`
- `primaryPublisher` (publisher node)

Storage node:
- File: `filePath`, `AES256Key`
- S3: `s3Bucket`, `s3Prefix`, `s3Endpoint`, `s3ReadTimeoutMs`, `s3WriteTimeoutMs`, `s3ListTimeoutMs`, `s3MaxRetries`, `s3RetryBaseDelayMs`, `s3RetryMaxDelayMs`

Publisher node:
- DynamoDB nameservice: `dynamodbTable`, `dynamodbRegion`, `dynamodbEndpoint`, `dynamodbTimeoutMs`
- Storage-backed nameservice: `storage` (reference to a Storage node)

## Core Patterns

### The Graph API

The primary API revolves around `fluree.graph(alias)`, which returns a lazy `Graph` handle.
No I/O occurs until a terminal method (`.execute()`, `.commit()`, `.load()`) is called.

**When I/O happens:**
- `.execute()` / `.execute_formatted()` / `.execute_tracked()` — loads the graph from storage, then runs the query (each call reloads)
- `.commit()` — loads the cached ledger handle, stages, and commits
- `.stage()` — loads the ledger and stages without committing
- `.load()` — loads the graph once, returning a `GraphSnapshot` for repeated queries without reloading

```rust
// Lazy query — loads graph and executes in one step
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?name WHERE { ?s <http://schema.org/name> ?name }")
    .execute()
    .await?;

// Lazy transact + commit
let out = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit()
    .await?;

// Materialize for reuse (avoids reloading on each query)
let db = fluree.graph("mydb:main").load().await?;
let r1 = db.query().sparql("SELECT ...").execute().await?;
let r2 = db.query().jsonld(&q).execute().await?;

// Time travel
let result = fluree.graph_at("mydb:main", TimeSpec::AtT(42))
    .query()
    .jsonld(&q)
    .execute()
    .await?;
```

### Insert Data

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert JSON-LD data using the Graph API
    let data = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob",
                "schema:email": "bob@example.org",
                "schema:age": 25
            }
        ]
    });

    let result = fluree.graph("mydb:main")
        .transact()
        .insert(&data)
        .commit()
        .await?;

    println!("Transaction committed");

    Ok(())
}
```

### Query Data with JSON-LD Query

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert test data first (see Insert Data above)
    // ...

    // Query with JSON-LD using the lazy Graph API
    let query = json!({
        "select": ["?name", "?email"],
        "where": [
            { "@id": "?person", "@type": "schema:Person" },
            { "@id": "?person", "schema:name": "?name" },
            { "@id": "?person", "schema:email": "?email" },
            { "@id": "?person", "schema:age": "?age" }
        ],
        "filter": "?age > 25"
    });

    let result = fluree.graph("mydb:main")
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    println!("Query results: {}",
        serde_json::to_string_pretty(&result)?);

    Ok(())
}
```

### Query Data with SPARQL

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert test data first (see Insert Data above)
    // ...

    // Query with SPARQL using the lazy Graph API
    let sparql = r#"
        PREFIX schema: <http://schema.org/>

        SELECT ?name ?email
        WHERE {
            ?person a schema:Person .
            ?person schema:name ?name .
            ?person schema:email ?email .
            ?person schema:age ?age .
            FILTER (?age > 25)
        }
        ORDER BY ?name
    "#;

    let result = fluree.graph("mydb:main")
        .query()
        .sparql(sparql)
        .execute_formatted()
        .await?;

    println!("Results: {}",
        serde_json::to_string_pretty(&result)?);

    Ok(())
}
```

### Update Data

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Update using WHERE/DELETE/INSERT pattern
    let update = json!({
        "@context": { "schema": "http://schema.org/" },
        "where": [
            { "@id": "?person", "schema:name": "Alice" },
            { "@id": "?person", "schema:age": "?oldAge" }
        ],
        "delete": [
            { "@id": "?person", "schema:age": "?oldAge" }
        ],
        "insert": [
            { "@id": "?person", "schema:age": 31 }
        ]
    });

    let result = fluree.graph("mydb:main")
        .transact()
        .update(&update)
        .commit()
        .await?;

    println!("Updated successfully");

    Ok(())
}
```

### Stage and Preview Changes

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    let data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });

    // Stage without committing
    let staged = fluree.graph("mydb:main")
        .transact()
        .insert(&data)
        .stage()
        .await?;

    // Query the staged state to preview changes
    let preview_query = json!({
        "select": ["?name"],
        "where": [{"@id": "ex:alice", "ex:name": "?name"}]
    });

    let preview = staged.query()
        .jsonld(&preview_query)
        .execute()
        .await?;

    println!("Preview: {} rows", preview.row_count());

    Ok(())
}
```

**Note:** `StagedGraph` currently supports querying only. Staging on top of a staged transaction and committing from a `StagedGraph` are not yet supported.

### Materialize for Reuse

When you need to run multiple queries against the same snapshot, materialize a `GraphSnapshot` once:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load once, query many times
    let db = fluree.graph("mydb:main").load().await?;

    let r1 = db.query()
        .sparql("SELECT ?name WHERE { ?s <http://schema.org/name> ?name }")
        .execute()
        .await?;

    let q2 = json!({
        "select": ["?email"],
        "where": [{"@id": "?s", "schema:email": "?email"}]
    });
    let r2 = db.query()
        .jsonld(&q2)
        .execute()
        .await?;

    // Access the underlying view if needed
    let view = db.view();

    Ok(())
}
```

## Advanced Usage

### Ledger Caching

When using the Fluree HTTP server, ledger caching is enabled by default to avoid reloading ledger state on every request. You can also enable caching when using Fluree as a library:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Enable ledger caching for connection-level reuse
    let fluree = FlureeBuilder::file("./data")
        .with_ledger_caching()
        .build()?;

    // First call loads from storage
    let ledger = fluree.ledger("mydb:main").await?;

    // Subsequent calls return cached state (fast)
    let ledger2 = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

#### Disconnecting Ledgers

Use `disconnect_ledger` to release a ledger from the connection cache. This forces a fresh load on the next access:

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data")
        .with_ledger_caching()
        .build()?;

    // Load and use ledger
    let ledger = fluree.ledger("mydb:main").await?;
    println!("Ledger at t={}", ledger.t());

    // Release cached state
    fluree.disconnect_ledger("mydb:main").await;

    // Next access will reload from storage
    let ledger = fluree.ledger("mydb:main").await?;

    Ok(())
}
```

**When to use `disconnect_ledger`:**

- **Force fresh load**: After external changes to the ledger (e.g., another process wrote data)
- **Free memory**: Release memory for ledgers you no longer need
- **Clean shutdown**: Release resources before application exit
- **Testing**: Reset state between test cases

**Note:** If caching is disabled (no `with_ledger_caching()` on builder), `disconnect_ledger` is a no-op.

#### Refreshing Cached Ledgers

Use `refresh` to poll-check whether a cached ledger is stale and update it if needed. This is the Rust equivalent of Clojure's `fluree.db.api/refresh`:

```rust
use fluree_db_api::{FlureeBuilder, NotifyResult, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data")
        .with_ledger_caching()
        .build()?;

    // Load ledger into cache
    let _ledger = fluree.ledger_cached("mydb:main").await?;

    // Later, check if the cached state is still fresh
    match fluree.refresh("mydb:main").await? {
        Some(NotifyResult::Current) => {
            println!("Cache is up to date");
        }
        Some(NotifyResult::Reloaded) => {
            println!("Cache was stale - reloaded from storage");
        }
        Some(NotifyResult::IndexUpdated) => {
            // Index advanced (v1: refreshed via full reload)
            println!("Index was updated");
        }
        Some(NotifyResult::CommitApplied) => {
            // Next commit available (v1: refreshed via full reload)
            println!("Commit was applied");
        }
        Some(NotifyResult::NotLoaded) => {
            println!("Ledger not in cache - no action taken");
        }
        None => {
            println!("Ledger not found in nameservice");
        }
    }

    Ok(())
}
```

**Key behaviors:**

- **Does NOT cold-load**: If the ledger isn't already cached, returns `NotLoaded` (no-op)
- **Returns `None`**: If the ledger doesn't exist in the nameservice
- **Alias resolution**: Supports short aliases (`mydb` resolves to `mydb:main`)
- **No-op without caching**: If caching is disabled, returns `NotLoaded`

**When to use `refresh`:**

- **Poll-based freshness**: When you can't use SSE events but need periodic freshness checks
- **Before critical reads**: Ensure you have the latest state before important queries
- **Peer mode**: Check if the local cache is behind the transaction server

**`refresh` vs `disconnect_ledger`:**

| Behavior | `refresh` | `disconnect_ledger` |
|----------|-----------|---------------------|
| Checks freshness | Yes | No |
| Updates in place | Yes | No (forces full reload on next access) |
| Handles not-cached | Returns `NotLoaded` | No-op |
| Use case | Poll-based updates | Force full reload |

### Time Travel Queries

```rust
use fluree_db_api::{FlureeBuilder, TimeSpec, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Query at a specific point in time
    let result = fluree.graph_at("mydb:main", TimeSpec::AtT(100))
        .query()
        .sparql("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        .execute()
        .await?;

    println!("Results at t=100: {:?}", result.row_count());

    Ok(())
}
```

### Multi-Ledger Queries

```rust
use fluree_db_api::{FlureeBuilder, FlureeDataSetView, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load views from multiple ledgers
    let customers = fluree.view("customers:main").await?;
    let orders = fluree.view("orders:main").await?;

    // Compose a dataset from multiple graphs
    let dataset = FlureeDataSetView::new()
        .with_default(customers)
        .with_named("orders:main", orders);

    // Query across ledgers using the dataset builder
    let query = r#"
        SELECT ?customerName ?orderTotal
        WHERE {
            ?customer schema:name ?customerName .
            ?customer ex:customerId ?cid .

            GRAPH <orders:main> {
                ?order ex:customerId ?cid .
                ?order ex:total ?orderTotal .
            }
        }
    "#;

    let result = dataset.query(&fluree)
        .sparql(query)
        .execute()
        .await?;

    Ok(())
}
```

### FROM-Driven Queries (Connection Queries)

When the query body itself specifies which ledgers to target (via `"from"` in JSON-LD or `FROM` in SPARQL), use `query_from()`:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Query where the "from" is embedded in the query body
    let query = json!({
        "from": "mydb:main",
        "select": ["?name"],
        "where": { "@id": "?s", "schema:name": "?name" }
    });

    let result = fluree.query_from()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    // SPARQL with FROM clause
    let result = fluree.query_from()
        .sparql("SELECT ?name FROM <mydb:main> WHERE { ?s <http://schema.org/name> ?name }")
        .execute_formatted()
        .await?;

    Ok(())
}
```

### Background Indexing

```rust
use fluree_db_api::{FlureeBuilder, BackgroundIndexerWorker, Result};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = Arc::new(FlureeBuilder::file("./data").build()?);

    // Start background indexer
    let indexer = BackgroundIndexerWorker::new(
        fluree.clone(),
        Duration::from_secs(5), // Index interval
    );

    let indexer_handle = indexer.start();

    // Application logic
    let ledger = fluree.create_ledger("mydb").await?;

    // Transactions will be indexed automatically in background
    for i in 0..100 {
        let txn = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:item{}", i), "ex:value": i}]
        });

        fluree.graph("mydb:main")
            .transact()
            .insert(&txn)
            .commit()
            .await?;
    }

    // Wait for indexing to complete
    sleep(Duration::from_secs(10)).await;

    // Shutdown indexer
    indexer_handle.shutdown().await?;

    Ok(())
}
```

### BM25 Full-Text Search

```rust
use fluree_db_api::{
    FlureeBuilder, Bm25CreateConfig, Bm25FieldConfig, Result
};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await?;

    // Insert searchable data and create BM25 index
    // ...

    // Query with full-text search
    let search_query = r#"
        PREFIX bm25: <https://ns.flur.ee/bm25#>

        SELECT ?product ?score ?name
        WHERE {
            GRAPH <products-search:main> {
                ?product bm25:matches "laptop" .
                ?product bm25:score ?score .
            }
            ?product schema:name ?name .
        }
        ORDER BY DESC(?score)
        LIMIT 10
    "#;

    let result = fluree.graph("mydb:main")
        .query()
        .sparql(search_query)
        .execute()
        .await?;

    println!("Found {} matching products", result.row_count());

    Ok(())
}
```

## Configuration

### Builder Options

```rust
use fluree_db_api::{FlureeBuilder, ConnectionConfig, IndexConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ConnectionConfig {
        storage_path: "./data".into(),
        index_config: IndexConfig {
            interval_ms: 5000,
            batch_size: 10,
            memory_mb: 2048,
            threads: 4,
        },
        ..Default::default()
    };

    let fluree = FlureeBuilder::with_config(config).build()?;

    Ok(())
}
```

### Custom Storage Backend

```rust
use fluree_db_api::{
    FlureeBuilder, Storage, StorageWrite, Result
};
use async_trait::async_trait;

// Implement custom storage
struct MyStorage;

#[async_trait]
impl Storage for MyStorage {
    async fn read(&self, address: &str) -> Result<Vec<u8>> {
        // Custom implementation
        todo!()
    }
}

#[async_trait]
impl StorageWrite for MyStorage {
    async fn write(&self, address: &str, data: &[u8]) -> Result<()> {
        // Custom implementation
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let storage = MyStorage;
    let fluree = FlureeBuilder::custom(storage).build()?;

    Ok(())
}
```

## Error Handling

```rust
use fluree_db_api::{FlureeBuilder, ApiError, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger — handles duplicates gracefully
    match fluree.create_ledger("mydb").await {
        Ok(ledger) => {
            println!("Ledger created at t={}", ledger.t());
        }
        Err(ApiError::LedgerExists(alias)) => {
            println!("Ledger {} already exists, loading...", alias);
            let ledger = fluree.ledger("mydb:main").await?;
            println!("Loaded at t={}", ledger.t());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use fluree_db_api::{FlureeBuilder, Result};
    use serde_json::json;

    #[tokio::test]
    async fn test_insert_and_query() -> Result<()> {
        // Use memory storage for tests
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("test").await?;

        // Insert data
        let data = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
        });

        fluree.graph("test:main")
            .transact()
            .insert(&data)
            .commit()
            .await?;

        // Query data
        let query = json!({
            "select": ["?name"],
            "where": [{"@id": "ex:alice", "ex:name": "?name"}]
        });

        let result = fluree.graph("test:main")
            .query()
            .jsonld(&query)
            .execute()
            .await?;

        assert_eq!(result.row_count(), 1);

        Ok(())
    }
}
```

### Integration Tests

```rust
// tests/integration_test.rs
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_persistence() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();

    // Create ledger and write data
    {
        let fluree = FlureeBuilder::file(path).build()?;
        let ledger = fluree.create_ledger("test").await?;

        let data = json!({"@context": {}, "@graph": [{"@id": "ex:test"}]});
        fluree.graph("test:main")
            .transact()
            .insert(&data)
            .commit()
            .await?;
    }

    // Verify persistence by reopening
    {
        let fluree = FlureeBuilder::file(path).build()?;
        let ledger = fluree.ledger("test:main").await?;

        assert!(ledger.t() > 0);
    }

    Ok(())
}
```

## Performance Tips

### Batch Transactions

```rust
// Good: Batch related changes
let batch_data = json!({
    "@graph": [
        {"@id": "ex:item1", "ex:value": 1},
        {"@id": "ex:item2", "ex:value": 2},
        {"@id": "ex:item3", "ex:value": 3}
    ]
});
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&batch_data)
    .commit()
    .await?;

// Bad: Individual transactions (more overhead per commit)
for i in 1..=3 {
    let txn = json!({"@graph": [{"@id": format!("ex:item{}", i), "ex:value": i}]});
    fluree.graph("mydb:main")
        .transact()
        .insert(&txn)
        .commit()
        .await?;
}
```

### Use Appropriate Storage

- **Memory**: Fastest, no persistence (tests, temporary data)
- **File**: Good balance (single server, local development)
- **AWS**: Distributed, durable (production, multi-server)

### Query Optimization

```rust
// Good: Specific patterns
let query = json!({
    "select": ["?name"],
    "where": [
        {"@id": "ex:alice", "schema:name": "?name"}
    ]
});

// Bad: Broad patterns
let query = json!({
    "select": ["?s", "?p", "?o"],
    "where": [
        {"@id": "?s", "?p": "?o"}
    ]
});
```

### Enable Query Tracking

```rust
use fluree_db_api::{FlureeBuilder, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // Use execute_tracked() for fuel/time/policy tracking
    let tracked = fluree.graph("mydb:main")
        .query()
        .sparql("SELECT * WHERE { ?s ?p ?o }")
        .execute_tracked()
        .await?;

    println!("Query used {} fuel", tracked.fuel().unwrap_or(0));

    Ok(())
}
```

## Graph API Reference

The Graph API follows a **lazy-handle** pattern: `fluree.graph(alias)` returns a lightweight handle, and all I/O is deferred to terminal methods.

### Getting a Graph Handle

```rust
// Lazy handle to the current (head) state
let graph = fluree.graph("mydb:main");

// Lazy handle at a specific point in time
let graph = fluree.graph_at("mydb:main", TimeSpec::AtT(100));
```

### Querying

```rust
// JSON-LD query (lazy — loads graph at execution time)
let result = fluree.graph("mydb:main")
    .query()
    .jsonld(&query_json)
    .execute().await?;

// SPARQL query
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?s WHERE { ?s a <ex:Person> }")
    .execute().await?;

// Formatted output (JSON-LD or SPARQL JSON based on query type)
let json = fluree.graph("mydb:main")
    .query()
    .jsonld(&query_json)
    .execute_formatted().await?;

// Tracked query (fuel/time/policy metrics)
let tracked = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT * WHERE { ?s ?p ?o }")
    .execute_tracked().await?;
```

### Materializing a GraphSnapshot

```rust
// Load once, query many times (avoids reloading)
let db = fluree.graph("mydb:main").load().await?;

let r1 = db.query().sparql("...").execute().await?;
let r2 = db.query().jsonld(&q).execute().await?;

// Access the underlying FlureeView
let view = db.view();
```

### Transacting

```rust
// Insert and commit
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit().await?;

// Upsert with options
let result = fluree.graph("mydb:main")
    .transact()
    .upsert(&data)
    .txn_opts(TxnOpts { author: Some("did:admin".into()), ..Default::default() })
    .commit_opts(CommitOpts { message: Some("migration".into()), ..Default::default() })
    .commit().await?;

// Stage without committing (preview changes)
let staged = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .stage().await?;

// Query staged state
let preview = staged.query()
    .jsonld(&validation_query)
    .execute().await?;
```

### Terminal Operations

| Method | Returns | Description |
|--------|---------|-------------|
| `.execute()` | `Result<QueryResult>` | Raw query result |
| `.execute_formatted()` | `Result<JsonValue>` | Formatted JSON output (JSON-LD for `.jsonld()`, SPARQL JSON for `.sparql()`) |
| `.execute_tracked()` | `Result<TrackedQueryResponse>` | Result with fuel/time/policy tracking |
| `.commit()` | `Result<TransactResultRef>` | Stage + commit transaction |
| `.stage()` | `Result<StagedGraph>` | Stage without committing |
| `.load()` | `Result<GraphSnapshot>` | Materialize snapshot for reuse |

### Format Override

```rust
use fluree_db_api::FormatterConfig;

// Force JSON-LD format for a SPARQL query
let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?name WHERE { ?s <schema:name> ?name }")
    .format(FormatterConfig::jsonld())
    .execute_formatted()
    .await?;
```

### Multi-Ledger Queries (Dataset)

For multi-ledger queries, use `FlureeView` directly:

```rust
let customers = fluree.view("customers:main").await?;
let orders = fluree.view("orders:main").await?;

let dataset = FlureeDataSetView::new()
    .with_default(customers)
    .with_named("orders:main", orders);

let result = dataset.query(&fluree)
    .sparql(query)
    .execute().await?;
```

### FROM-Driven Queries (Connection Queries)

```rust
let result = fluree.query_from()
    .jsonld(&query_with_from)
    .execute().await?;
```

## Transaction Builder API Reference

There are two transaction builder patterns, each suited for different use cases:

### `stage(&handle)` — Server/Application Pattern (Recommended)

Use `stage(&handle)` when building servers or applications with ledger caching enabled. The handle is borrowed and updated in-place on successful commit, ensuring concurrent readers see the update.

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    // Enable ledger caching (required for stage)
    let fluree = FlureeBuilder::file("./data")
        .with_ledger_caching()
        .build()?;

    // Get a cached handle
    let handle = fluree.ledger_cached("mydb:main").await?;

    // Transaction via builder — handle updated in-place
    let data = json!({"@graph": [{"@id": "ex:test", "ex:name": "Test"}]});
    let result = fluree.stage(&handle)
        .insert(&data)
        .execute()
        .await?;

    println!("Committed at t={}", result.receipt.t);

    // Handle now reflects the new state
    let snapshot = handle.snapshot().await;
    assert_eq!(snapshot.t, result.receipt.t);

    Ok(())
}
```

**Why use `stage(&handle)`:**
- **Concurrent safety**: Multiple requests share the same handle; updates are atomic
- **No ownership dance**: You don't need to track and pass around `LedgerState` values
- **Server-friendly**: Matches how the HTTP server handles transactions internally

### `stage_owned(ledger)` — CLI/Script/Test Pattern

Use `stage_owned(ledger)` when you manage your own `LedgerState` directly. This is typical for CLI tools, scripts, and tests where you don't need ledger caching.

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // You own the ledger state
    let ledger = fluree.create_ledger("mydb").await?;

    // Transaction consumes ledger, returns updated state
    let data = json!({"@graph": [{"@id": "ex:test", "ex:name": "Test"}]});
    let result = fluree.stage_owned(ledger)
        .insert(&data)
        .execute()
        .await?;

    // Get the updated ledger from the result
    let ledger = result.ledger;
    println!("Now at t={}", ledger.t());

    Ok(())
}
```

**Why use `stage_owned(ledger)`:**
- **Simple ownership**: Good for linear workflows (load → transact → done)
- **No caching required**: Works without `with_ledger_caching()`
- **Test-friendly**: Each test manages its own state

### Choosing Between Them

| Use Case | Pattern | Why |
|----------|---------|-----|
| HTTP server | `stage(&handle)` | Shared handles, atomic updates |
| Long-running app | `stage(&handle)` | Concurrent access to same ledger |
| CLI tool | `stage_owned(ledger)` | Simple, no caching needed |
| Integration test | `stage_owned(ledger)` | Isolated state per test |
| Script/batch job | `stage_owned(ledger)` | Linear workflow |

### Builder Methods (Both Patterns)

Both `stage(&handle)` and `stage_owned(ledger)` return a builder with identical methods:

```rust
let result = fluree.stage(&handle)  // or stage_owned(ledger)
    .insert(&data)                   // or .upsert(&data), .update(&data)
    .txn_opts(TxnOpts::default().author("did:admin"))
    .commit_opts(CommitOpts::with_message("migration"))
    .execute()
    .await?;
```

| Method | Description |
|--------|-------------|
| `.insert(&json)` | Insert JSON-LD data |
| `.upsert(&json)` | Upsert JSON-LD data |
| `.update(&json)` | Update with WHERE/DELETE/INSERT |
| `.insert_turtle(&ttl)` | Insert Turtle data |
| `.upsert_turtle(&ttl)` | Upsert Turtle data |
| `.txn_opts(opts)` | Set transaction options (author, context) |
| `.commit_opts(opts)` | Set commit options (message) |
| `.policy(ctx)` | Set policy enforcement |
| `.execute()` | Stage + commit |
| `.stage()` | Stage without committing (returns `Staged`) |
| `.validate()` | Check configuration without executing |

### Graph API Transactions

The Graph API (`fluree.graph(alias).transact()`) is built on top of `stage(&handle)` internally:

```rust
// Graph API (convenient, uses caching internally)
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit()
    .await?;

// Equivalent to:
let handle = fluree.ledger_cached("mydb:main").await?;
let result = fluree.stage(&handle)
    .insert(&data)
    .execute()
    .await?;
```

## Ledger Info API

Get comprehensive metadata about a ledger using the `ledger_info()` builder:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Get ledger info with optional context for IRI compaction
    let context = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    let info = fluree.ledger_info("mydb:main")
        .with_context(&context)
        .execute()
        .await?;

    // Access metadata sections
    println!("Commit: {}", info["commit"]);
    println!("Nameservice: {}", info["nameservice"]);
    println!("Namespace codes: {}", info["namespace-codes"]);
    println!("Stats: {}", info["stats"]);
    println!("Index: {}", info["index"]);

    Ok(())
}
```

### Ledger Info Response

The response includes:

| Section | Description |
|---------|-------------|
| `commit` | Commit info in JSON-LD format (Clojure parity) |
| `nameservice` | NsRecord in JSON-LD format |
| `namespace-codes` | Inverted mapping (prefix → code) for IRI expansion |
| `stats` | Flake counts, size, property/class statistics with selectivity |
| `index` | Index metadata (`t`, address, index ID) |

## Nameservice Query API

Query metadata about all ledgers and virtual graphs using the `nameservice_query()` builder:

```rust
use fluree_db_api::{FlureeBuilder, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let fluree = FlureeBuilder::file("./data").build()?;

    // Find all ledgers on main branch
    let query = json!({
        "@context": {"f": "https://ns.flur.ee/ledger#"},
        "select": ["?ledger", "?t"],
        "where": [{"@id": "?ns", "@type": "f:PhysicalDatabase", "f:ledger": "?ledger", "f:branch": "main", "f:t": "?t"}],
        "orderBy": [{"var": "?t", "desc": true}]
    });

    let results = fluree.nameservice_query()
        .jsonld(&query)
        .execute_formatted()
        .await?;

    println!("Ledgers: {}", serde_json::to_string_pretty(&results)?);

    // SPARQL query
    let results = fluree.nameservice_query()
        .sparql("PREFIX f: <https://ns.flur.ee/ledger#>
                 SELECT ?ledger ?t WHERE { ?ns a f:PhysicalDatabase ; f:ledger ?ledger ; f:t ?t }")
        .execute_formatted()
        .await?;

    println!("SPARQL results: {}", serde_json::to_string_pretty(&results)?);

    // Convenience method (equivalent to builder with defaults)
    let results = fluree.query_nameservice(&query).await?;

    Ok(())
}
```

### Available Properties

**Ledger Records** (`@type: "f:PhysicalDatabase"`):

| Property | Description |
|----------|-------------|
| `f:ledger` | Ledger name (without branch suffix) |
| `f:branch` | Branch name |
| `f:t` | Current transaction number |
| `f:status` | Status: "ready" or "retracted" |
| `f:commit` | Reference to latest commit address |
| `f:index` | Index info with `@id` and `f:t` |

**Virtual Graph Records** (`@type: "f:VirtualGraphDatabase"`):

| Property | Description |
|----------|-------------|
| `f:name` | Virtual graph name |
| `f:branch` | Branch name |
| `fidx:config` | Configuration JSON |
| `fidx:dependencies` | Source ledger dependencies |
| `fidx:indexAddress` | Index storage address |
| `fidx:indexT` | Index transaction number |

### Builder Methods

| Method | Description |
|--------|-------------|
| `.jsonld(&query)` | Set JSON-LD/FQL query input |
| `.sparql(query)` | Set SPARQL query input |
| `.format(config)` | Override output format |
| `.execute_formatted()` | Execute and return formatted JSON |
| `.execute()` | Execute with default formatting |
| `.validate()` | Validate without executing |

### Example Queries

```rust
// Find ledgers with t > 100
let query = json!({
    "@context": {"f": "https://ns.flur.ee/ledger#"},
    "select": ["?ledger", "?t"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}],
    "filter": ["(> ?t 100)"]
});

// Find all BM25 virtual graphs
let query = json!({
    "@context": {"f": "https://ns.flur.ee/ledger#", "fidx": "https://ns.flur.ee/index#"},
    "select": ["?name", "?deps"],
    "where": [{"@id": "?vg", "@type": "fidx:BM25", "f:name": "?name", "fidx:dependencies": "?deps"}]
});
```

## Examples

See complete examples in `fluree-db-api/examples/`:

- `benchmark_aj_query_1.rs` - Basic query patterns
- `benchmark_aj_query_2.rs` - Complex queries
- `benchmark_aj_query_3.rs` - Aggregations
- `benchmark_aj_query_4.rs` - Time travel queries

Run examples:

```bash
cargo run --example benchmark_aj_query_1 --release
```

## API Reference

For detailed API documentation, see:

```bash
cargo doc --open -p fluree-db-api
```

## Related Documentation

- [Getting Started](README.md) - Overview
- [HTTP API](../api/README.md) - Server-based usage
- [Query](../query/README.md) - Query documentation
- [Transactions](../transactions/README.md) - Write operations
- [Crate Map](../reference/crate-map.md) - Architecture overview
- [Dev Setup](../contributing/dev-setup.md) - Development guide
