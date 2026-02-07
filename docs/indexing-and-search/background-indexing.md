# Background Indexing

Fluree maintains query-optimized indexes through a background indexing process. This document covers the indexing architecture, configuration, and monitoring.

## Index Architecture

Fluree maintains four index permutations for efficient query execution:

### SPOT (Subject-Predicate-Object-Time)

Organized by subject first:

```text
ex:alice → schema:name → "Alice" → [t=1, t=5]
ex:alice → schema:age → 30 → [t=1]
ex:alice → schema:age → 31 → [t=10]
```

**Optimized for:** "Give me all properties of this subject"

### POST (Predicate-Object-Subject-Time)

Organized by predicate first:

```text
schema:name → "Alice" → ex:alice → [t=1, t=5]
schema:age → 30 → ex:alice → [t=1]
schema:age → 31 → ex:alice → [t=10]
```

**Optimized for:** "Find all subjects with this property/value"

### OPST (Object-Predicate-Subject-Time)

Organized by object first:

```text
"Alice" → schema:name → ex:alice → [t=1, t=5]
30 → schema:age → ex:alice → [t=1]
31 → schema:age → ex:alice → [t=10]
```

**Optimized for:** "Find subjects that reference this value"

### PSOT (Predicate-Subject-Object-Time)

Organized by predicate, then subject:

```text
schema:name → ex:alice → "Alice" → [t=1, t=5]
schema:age → ex:alice → 30 → [t=1]
schema:age → ex:alice → 31 → [t=10]
```

**Optimized for:** "Get all values for this predicate"

## Indexing Process

### 1. Transaction Commit

```text
t=42: Transaction committed
  - Flakes written to append-only log
  - Commit metadata created
  - Commit published to nameservice (commit_t=42)
```

### 2. Indexer Detection

Background indexer polls for new commits:

```text
Indexer checks: commit_t=42, index_t=40
Indexer: Need to index t=41, t=42
```

### 3. Index Building

For each transaction in gap:

```text
For t=41:
  - Load flakes from transaction log
  - For each flake:
    - Update SPOT index
    - Update POST index
    - Update OPST index
    - Update PSOT index
```

### 4. Index Publishing

When complete:

```text
  - Write index snapshot to storage
  - Publish index_address to nameservice
  - Update index_t to 42
```

## Novelty Layer

The **novelty layer** consists of transactions committed but not yet indexed:

```text
Current State:
  commit_t = 150
  index_t = 145
  novelty = [t=146, t=147, t=148, t=149, t=150]
```

### Query Execution with Novelty

Queries combine indexed data with novelty:

```text
Query for ex:alice's properties:

1. Check SPOT index (up to t=145)
2. Apply novelty layer (t=146 to t=150)
3. Combine results
```

### Impact of Large Novelty

**Small novelty** (< 10 transactions):
- Minimal query overhead
- Fast query execution

**Large novelty** (> 100 transactions):
- Significant query overhead
- Slower query execution
- Higher memory usage

## Configuration

### Index Interval

Control indexing frequency:

```bash
./fluree-db-server --index-interval-ms 5000  # Index every 5 seconds
```

**Trade-offs:**
- Shorter: Less novelty, more overhead
- Longer: More novelty, less overhead

Default: 5000ms (5 seconds)

### Batch Size

Transactions indexed per batch:

```bash
./fluree-db-server --index-batch-size 10
```

**Trade-offs:**
- Smaller: More frequent updates
- Larger: Fewer, longer indexing cycles

Default: 10 transactions

### Memory Allocation

Memory for indexing process:

```bash
./fluree-db-server --index-memory-mb 2048
```

Default: 1024 MB

### Concurrent Indexers

Number of parallel indexing threads:

```bash
./fluree-db-server --index-threads 4
```

Default: 2 threads

## Monitoring

### Check Index Status

```bash
curl http://localhost:8090/ledgers/mydb:main
```

Response:
```json
{
  "ledger_address": "mydb:main",
  "commit_t": 150,
  "index_t": 145,
  "novelty_count": 5,
  "last_index_duration_ms": 234
}
```

**Key Metrics:**
- **novelty_count**: Number of unindexed transactions
- **last_index_duration_ms**: Time to index last batch

### Index Metrics

```bash
curl http://localhost:8090/metrics/indexing
```

Response:
```json
{
  "total_indexed": 145,
  "avg_index_time_ms": 187,
  "current_novelty": 5,
  "index_rate_per_second": 2.3,
  "pending_ledgers": 0
}
```

### Health Indicators

**Healthy:**
```text
novelty_count: 0-10 transactions
index_rate > transaction_rate
avg_index_time_ms: < 500ms
```

**Warning:**
```text
novelty_count: 10-50 transactions
index_rate ≈ transaction_rate
avg_index_time_ms: 500-2000ms
```

**Critical:**
```text
novelty_count: > 50 transactions
index_rate < transaction_rate
avg_index_time_ms: > 2000ms
```

## Performance Tuning

### Optimize for Write-Heavy Loads

```bash
./fluree-db-server \
  --index-interval-ms 10000 \
  --index-batch-size 20 \
  --index-threads 4
```

Longer intervals, larger batches, more threads.

### Optimize for Read-Heavy Loads

```bash
./fluree-db-server \
  --index-interval-ms 1000 \
  --index-batch-size 5
```

Shorter intervals, smaller batches for lower novelty.

### Optimize for Low Latency

```bash
./fluree-db-server \
  --index-interval-ms 500 \
  --index-memory-mb 4096
```

Very short intervals, more memory.

## Index Storage

### Index Snapshots

Indexes are stored as snapshots:

```text
Storage Structure:
  index/
    mydb-main-t145.idx
    mydb-main-t140.idx
    mydb-main-t135.idx
```

Each snapshot contains complete index state at that transaction time.

### Index Retention

Old indexes can be removed:

```bash
./fluree-db-server --index-retention-count 10
```

Keeps last 10 index snapshots per ledger.

### Index Compaction

Compact indexes to reclaim space:

```bash
curl -X POST http://localhost:8090/admin/compact?ledger=mydb:main
```

Merges multiple small snapshots into larger ones.

## Troubleshooting

### High Novelty Count

**Symptom:** Novelty count growing continuously

**Causes:**
- Transaction rate exceeds indexing capacity
- Large transactions
- Insufficient resources

**Solutions:**
1. Increase index interval: `--index-interval-ms 10000`
2. Increase batch size: `--index-batch-size 20`
3. Add more threads: `--index-threads 4`
4. Allocate more memory: `--index-memory-mb 4096`
5. Reduce transaction rate

### Slow Indexing

**Symptom:** `last_index_duration_ms` increasing

**Causes:**
- Disk I/O bottleneck
- CPU bottleneck
- Large index size
- Storage backend latency

**Solutions:**
1. Use faster storage (SSD)
2. Increase CPU allocation
3. Optimize transaction patterns
4. Use local storage vs network storage

### Index Corruption

**Symptom:** Query errors, unexpected results

**Detection:**
```bash
curl http://localhost:8090/admin/verify-index?ledger=mydb:main
```

**Recovery:**
```bash
# Rebuild index from scratch
curl -X POST http://localhost:8090/admin/rebuild-index?ledger=mydb:main
```

## Best Practices

### 1. Monitor Novelty

```javascript
setInterval(async () => {
  const status = await fetch('http://localhost:8090/ledgers/mydb:main')
    .then(r => r.json());
  
  if (status.novelty_count > 50) {
    console.warn(`High novelty: ${status.novelty_count} transactions`);
  }
}, 30000);  // Check every 30 seconds
```

### 2. Tune for Workload

Match configuration to workload pattern:
- Write-heavy: Longer intervals, larger batches
- Read-heavy: Shorter intervals, smaller batches
- Balanced: Default settings

### 3. Capacity Planning

Estimate indexing capacity:

```text
Transaction rate: 10 txn/second
Avg flakes per txn: 100
Total flakes: 1,000 flakes/second

Indexing capacity: 2,000 flakes/second (2× margin)
```

### 4. Alert on Lag

Set up alerting:

```javascript
if (status.novelty_count > 100) {
  alertOps('Critical: Indexing lag > 100 transactions');
}
```

### 5. Scheduled Compaction

Run compaction during off-peak hours:

```bash
# Cron job
0 2 * * * curl -X POST http://localhost:8090/admin/compact
```

## Related Documentation

- [Reindex API](reindex.md) - Manual index rebuilding and recovery
- [Indexing Side-Effects](../transactions/indexing-side-effects.md) - Transaction impact on indexing
- [Query Performance](../query/explain.md) - Query optimization
- [BM25](bm25.md) - Full-text search indexing
- [Vector Search](vector-search.md) - Vector indexing
