# Ledgers and the Nameservice

Ledgers are Fluree's fundamental unit of data organization—similar to databases in traditional RDBMS systems. The nameservice is the metadata registry that enables ledger discovery, coordination, and management across distributed deployments.

## Ledgers

A **ledger** in Fluree is an independent, versioned graph database containing:

- A complete graph of RDF triples
- Complete transaction history with temporal versioning
- Independent indexing and storage
- Configurable permissions and policies
- Support for multiple branches

### Ledger Aliases

Ledgers are identified by **aliases** with the format `ledger-name:branch`. The alias serves as both a human-readable identifier and a query target.

**Examples:**

- `mydb:main` - Primary branch of the "mydb" ledger
- `customers:dev` - Development branch of the "customers" ledger
- `inventory:prod` - Production branch of the "inventory" ledger
- `tenant/app:feature-x` - Feature branch with hierarchical naming

**Branch Semantics:**

- The `:branch` suffix allows multiple isolated versions of the same logical ledger to coexist
- The default branch name is `main` when not specified (e.g., `mydb` is equivalent to `mydb:main`)
- Branches are independent—changes in one branch don't affect others
- Branch names can include slashes for hierarchical organization

### Ledger Lifecycle

Ledgers are created implicitly through the first transaction and persist until explicitly retracted. Each ledger maintains:

- **Transaction History**: Every change is recorded as a transaction with a unique timestamp (`t`)
- **Current State**: The latest indexed state of all data
- **Novelty Layer**: Uncommitted transactions since the last index
- **Metadata**: Creation time, latest commit, indexing status

**Creation Flow:**

1. First transaction to a ledger alias creates the ledger automatically
2. Transaction is committed and assigned a transaction time (`t`)
3. Commit address is published to the nameservice
4. Background indexing process creates queryable indexes
5. Index address is published to the nameservice when complete

**Retraction:**

Ledgers can be marked as retracted (soft delete), which:
- Marks the ledger as inactive in the nameservice
- Preserves all historical data
- Prevents new transactions (but allows historical queries)
- Can be reversed if needed

## The Nameservice

The **nameservice** is Fluree's metadata registry that enables ledger discovery and coordination. It acts as a directory service, tracking where ledger data is stored and what state each ledger is in.

### Purpose and Role

The nameservice provides:

- **Discovery**: Find ledgers by alias across distributed deployments
- **Coordination**: Track commit and index state for consistency
- **Metadata Management**: Store ledger configuration and status
- **Multi-Process Support**: Enable coordination across multiple Fluree instances

### What the Nameservice Stores

For each ledger, the nameservice maintains a **nameservice record** (`NsRecord`) containing:

#### Core Identifiers

- **`alias`**: Canonical ledger identifier (e.g., `"mydb:main"`)
- **`address`**: Lookup address (may be alias or IRI)
- **`branch`**: Branch name (e.g., `"main"`)

#### Commit State

- **`commit_address`**: Storage address of the latest commit
- **`commit_t`**: Transaction time of the latest commit

The commit represents the most recent transaction that has been persisted. Commits are published immediately after each successful transaction.

#### Index State

- **`index_address`**: Storage address of the latest index snapshot
- **`index_t`**: Transaction time of the latest index

The index represents a queryable snapshot of the ledger state. Indexes are created by background processes and may lag behind commits.

#### Additional Metadata

- **`default_context_address`**: Default JSON-LD @context for the ledger
- **`retracted`**: Whether the ledger has been marked as inactive

### Commit vs Index: Understanding the Difference

This distinction is crucial for understanding Fluree's architecture:

**Commits (`commit_t`):**
- Created immediately after each transaction
- Represent the transaction log (what changed)
- Small, append-only files
- Published synchronously
- Always up-to-date with latest transactions

**Indexes (`index_t`):**
- Created by background indexing processes
- Represent queryable database snapshots (complete state)
- Large, optimized data structures
- Published asynchronously
- May lag behind commits (this gap is the "novelty layer")

**Example Timeline:**

```text
t=1:  Transaction committed → commit_t=1, index_t=0
t=2:  Transaction committed → commit_t=2, index_t=0
t=3:  Transaction committed → commit_t=3, index_t=0
       [Background indexing completes] → index_t=3
t=4:  Transaction committed → commit_t=4, index_t=3
t=5:  Transaction committed → commit_t=5, index_t=3
       [Novelty layer: t=4, t=5 not yet indexed]
```

Queries combine the indexed state (up to `index_t`) with the novelty layer (transactions between `index_t` and `commit_t`) to provide real-time results.

### Nameservice Operations

The nameservice supports these key operations:

#### Lookup

Find ledger metadata by alias:

```rust
// Pseudo-code
let record = nameservice.lookup("mydb:main").await?;
// Returns: NsRecord with commit_address, index_address, timestamps, etc.
```

#### Publishing

Record new commits and indexes:

- **`publish_commit(alias, commit_addr, commit_t)`**: Update commit state (monotonic: only if `new_t > existing_t`)
- **`publish_index(alias, index_addr, index_t)`**: Update index state (monotonic: only if `new_t > existing_t`)

Publishing is **monotonic**—the nameservice only accepts updates that advance time forward, ensuring consistency.

#### Discovery

List all available ledgers:

```rust
// Pseudo-code
let all_ledgers = nameservice.all_records().await?;
// Returns: Vec<NsRecord> for all known ledgers
```

### Querying the Nameservice

The nameservice can be queried using standard FQL (JSON-LD Query) or SPARQL syntax. This enables powerful ledger discovery, filtering, and metadata analysis across all managed databases.

#### Rust API (Builder Pattern)

```rust
// Find all ledgers on main branch
let query = json!({
    "@context": {"f": "https://ns.flur.ee/ledger#"},
    "select": ["?ledger"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
});

let results = fluree.nameservice_query()
    .jsonld(&query)
    .execute_formatted()
    .await?;

// Query with SPARQL
let results = fluree.nameservice_query()
    .sparql("PREFIX f: <https://ns.flur.ee/ledger#>
             SELECT ?ledger ?t WHERE {
               ?ns a f:PhysicalDatabase ;
                   f:ledger ?ledger ;
                   f:t ?t
             }")
    .execute_formatted()
    .await?;

// Convenience method (equivalent to builder with defaults)
let results = fluree.query_nameservice(&query).await?;
```

#### HTTP API

```bash
# Query nameservice via POST /nameservice/query
curl -X POST http://localhost:8090/nameservice/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"f": "https://ns.flur.ee/ledger#"},
    "select": ["?ledger", "?branch", "?t"],
    "where": [{"@id": "?ns", "@type": "f:PhysicalDatabase", "f:ledger": "?ledger", "f:branch": "?branch", "f:t": "?t"}],
    "orderBy": [{"var": "?t", "desc": true}]
  }'
```

#### Available Properties

**Ledger Records** (`@type: "f:PhysicalDatabase"`):

| Property | Description |
|----------|-------------|
| `f:ledger` | Ledger name (without branch suffix) |
| `f:branch` | Branch name (e.g., "main", "dev") |
| `f:t` | Current transaction number |
| `f:status` | Status: "ready" or "retracted" |
| `f:commit` | Reference to latest commit address |
| `f:index` | Index info object with `@id` (address) and `f:t` |
| `f:defaultContext` | Default JSON-LD context address (if set) |

**Virtual Graph Records** (`@type: "f:VirtualGraphDatabase"`):

| Property | Description |
|----------|-------------|
| `f:name` | Virtual graph name |
| `f:branch` | Branch name |
| `f:status` | Status: "ready" or "retracted" |
| `fidx:config` | Configuration JSON |
| `fidx:dependencies` | Array of source ledger dependencies |
| `fidx:indexAddress` | Index storage address |
| `fidx:indexT` | Index transaction number |

#### Example Queries

**Find all ledgers with t > 100:**
```json
{
  "@context": {"f": "https://ns.flur.ee/ledger#"},
  "select": ["?ledger", "?t"],
  "where": [
    {"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}
  ],
  "filter": ["(> ?t 100)"]
}
```

**Find ledgers by name pattern (hierarchical):**
```json
{
  "@context": {"f": "https://ns.flur.ee/ledger#"},
  "select": ["?ledger", "?branch"],
  "where": [
    {"@id": "?ns", "f:ledger": "?ledger", "f:branch": "?branch"}
  ],
  "filter": ["(strStarts ?ledger \"tenant1/\")"]
}
```

**Find all BM25 virtual graphs:**
```json
{
  "@context": {
    "f": "https://ns.flur.ee/ledger#",
    "fidx": "https://ns.flur.ee/index#"
  },
  "select": ["?name", "?deps"],
  "where": [
    {"@id": "?vg", "@type": "fidx:BM25", "f:name": "?name", "fidx:dependencies": "?deps"}
  ]
}
```

#### Retraction

Mark ledgers as inactive:

```rust
// Pseudo-code
nameservice.retract("mydb:old-branch").await?;
// Sets retracted=true, prevents new transactions
```

### Storage Backends

The nameservice can be backed by various storage systems, each suited for different deployment scenarios:

#### File System (`FileNameService`)

- **Use Case**: Single-server deployments, development, testing
- **Storage**: Files in `ns@v2/` directory structure
- **Format**: JSON files per ledger (`{ledger}/{branch}.json`)
- **Characteristics**: Simple, local, no external dependencies

#### AWS S3 (`StorageNameService`)

- **Use Case**: Distributed deployments using S3 for both data and metadata
- **Storage**: S3 objects with ETag-based compare-and-swap (CAS)
- **Characteristics**: Scalable, distributed, requires AWS credentials

#### AWS DynamoDB (`DynamoDbNameService`)

- **Use Case**: Distributed deployments needing low-latency metadata coordination
- **Storage**: DynamoDB table with composite-key layout (one item per concern)
- **Format**: Separate items for `meta`, `head`, `index`, `config`, `status` per ledger/graph source
- **Characteristics**: Single-digit millisecond latency, per-concern write independence, conditional expressions for monotonic updates
- See [DynamoDB Nameservice Guide](../operations/dynamodb-guide.md) for setup and schema details

#### Memory (`MemoryNameService`)

- **Use Case**: Testing, in-process applications
- **Storage**: In-memory data structures
- **Format**: No persistence
- **Characteristics**: Fast, ephemeral, process-local

### Virtual Graphs

The nameservice also tracks **virtual graphs**—specialized indexes and integrations:

- **BM25**: Full-text search indexes
- **Vector**: Vector similarity search
- **R2RML**: Relational database mappings
- **Iceberg**: Apache Iceberg table integrations

Virtual graphs have their own nameservice records (`VgNsRecord`) with similar metadata but different semantics. See the [Virtual Graphs](../virtual-graphs/overview.md) documentation for details.

## Example Usage

### Creating a Ledger

Ledgers are created automatically on the first transaction. Specify the ledger alias in your transaction:

```json
POST /transact?ledger=mydb:main
Content-Type: application/json

{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice"
    }
  ]
}
```

**What Happens:**

1. Transaction is processed and committed (assigned `t=1`)
2. Commit is stored and address published to nameservice
3. Nameservice record created/updated with `commit_t=1`
4. Background indexing begins
5. When indexing completes, `index_t=1` is published

### Querying a Ledger

Specify the ledger alias in your query:

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ex:alice foaf:name ?name
}
```

The `FROM <mydb:main>` clause specifies which ledger to query. The query engine:
1. Looks up `mydb:main` in the nameservice
2. Retrieves the index address for efficient querying
3. Combines indexed data with novelty layer for current results

**JSON-LD Query:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "select": ["?name"],
  "from": "mydb:main",
  "where": [
    { "@id": "ex:alice", "foaf:name": "?name" }
  ]
}
```

### Checking Ledger Status

Query the nameservice to check ledger state:

```rust
// Pseudo-code
let record = nameservice.lookup("mydb:main").await?;

if let Some(record) = record {
    println!("Latest commit: t={}", record.commit_t);
    println!("Latest index: t={}", record.index_t);
    
    if record.has_novelty() {
        println!("Novelty layer: {} transactions pending index", 
                 record.commit_t - record.index_t);
    }
    
    if record.retracted {
        println!("Ledger is retracted (inactive)");
    }
}
```

### Branching Workflows

Create feature branches for isolated development:

```text
# Main branch
mydb:main (t=100)

# Create feature branch (copies state from main)
mydb:feature-x (t=100)  # Starts from same state

# Develop independently
mydb:main (t=101, t=102, ...)
mydb:feature-x (t=101, t=102, ...)  # Different changes

# Merge (application-specific logic)
# Compare branches, resolve conflicts, apply to main
```

Branches are independent ledgers—they share no data unless explicitly merged through application logic.

## Architecture Deep Dive

### Ledger State Composition

Each ledger combines two layers for query execution:

#### 1. Indexed Database

- **What**: Persisted, optimized snapshot of ledger state
- **When**: Created by background indexing processes
- **Storage**: Large, read-optimized data structures
- **Query Performance**: Fast, efficient for historical queries
- **Update Frequency**: Asynchronous, may lag behind commits

#### 2. Novelty Overlay

- **What**: In-memory representation of uncommitted transactions
- **When**: Transactions between `index_t` and `commit_t`
- **Storage**: Transaction log entries
- **Query Performance**: Slower, requires transaction replay
- **Update Frequency**: Real-time, always current

**Query Execution Model:**

```text
Query Result = Indexed Database (up to t=index_t) 
             + Novelty Overlay (t=index_t+1 to commit_t)
```

This architecture provides:
- **Fast historical queries**: Use appropriate index snapshot
- **Real-time current queries**: Include latest transactions via novelty
- **Efficient background indexing**: Doesn't block new writes
- **Consistent snapshots**: Each query sees a consistent state

### Concurrency Control

The nameservice ensures consistency through several mechanisms:

#### Monotonic Publishing

- **Commits**: Only accept `publish_commit()` if `new_commit_t > existing_commit_t`
- **Indexes**: Only accept `publish_index()` if `new_index_t > existing_index_t`
- **Guarantee**: Time always moves forward, preventing inconsistencies

#### Optimistic Concurrency

- **CAS Operations**: Storage-backed nameservices use compare-and-swap (ETags)
- **Conflict Handling**: Retry on conflicts (expected under contention)
- **Atomic Updates**: Metadata updates are atomic per ledger

#### Consistency Guarantees

- **Read Consistency**: All readers see the same nameservice state
- **Write Consistency**: Monotonic updates prevent time-travel inconsistencies
- **Eventual Consistency**: In distributed deployments, updates propagate eventually

### Distributed Coordination

The nameservice enables coordination across distributed deployments:

#### Multi-Process Coordination

- **Shared State**: Nameservice provides shared view of ledger state
- **Process Discovery**: Processes can discover ledgers created by other processes
- **State Synchronization**: Commit/index state visible to all processes

#### Geographic Distribution

- **Storage Backends**: S3/DynamoDB enable cross-region coordination
- **Replication**: Storage backends handle replication
- **Consistency**: Eventual consistency with monotonic guarantees

#### Scalability Patterns

- **Horizontal Scaling**: Multiple Fluree instances can share nameservice
- **Load Distribution**: Queries can be distributed across instances
- **Storage Distribution**: Ledger data can be stored across multiple backends

### Nameservice Record Lifecycle

Understanding how records evolve:

```text
1. Initialization
   - publish_ledger_init("mydb:main")
   - Creates record with commit_t=0, index_t=0

2. First Transaction
   - Transaction committed at t=1
   - publish_commit("mydb:main", "addr1", 1)
   - Record: commit_t=1, index_t=0

3. Indexing Completes
   - Index created for t=1
   - publish_index("mydb:main", "idx_addr1", 1)
   - Record: commit_t=1, index_t=1

4. More Transactions
   - Transactions at t=2, t=3, t=4
   - publish_commit() called for each
   - Record: commit_t=4, index_t=1 (novelty: t=2,3,4)

5. Next Index
   - Index created for t=4
   - publish_index("mydb:main", "idx_addr2", 4)
   - Record: commit_t=4, index_t=4 (no novelty)
```

## Best Practices

### Ledger Naming

1. **Use Descriptive Names**: Choose names that clearly indicate purpose
   - Good: `customers:main`, `inventory:prod`, `analytics:warehouse`
   - Bad: `db1:main`, `test:main`, `data:main`

2. **Hierarchical Organization**: Use slashes for logical grouping
   - Good: `tenant/app:main`, `tenant/app:dev`
   - Good: `department/project:branch`

3. **Branch Naming Conventions**: Establish consistent branch naming
   - Good: `feature/authentication`, `bugfix/login-error`
   - Good: `release/v1.2.0`, `hotfix/security-patch`

### Nameservice Configuration

1. **Choose Appropriate Backend**: Match backend to deployment needs
   - Development: File system
   - Single server: File system
   - Distributed/Cloud: S3/DynamoDB

2. **Monitor Novelty Layer**: Track gap between commits and indexes
   - Large gaps indicate indexing lag
   - May need to tune indexing frequency or resources

3. **Handle Retraction Carefully**: Retracted ledgers preserve history
   - Use for soft deletes, not hard deletes
   - Historical queries still work on retracted ledgers

### Performance Considerations

1. **Index Frequency**: Balance indexing frequency with query needs
   - More frequent indexing: Better query performance, more storage
   - Less frequent indexing: Lower overhead, larger novelty layer

2. **Query Patterns**: Understand your query patterns
   - Historical queries: Benefit from frequent indexing
   - Current-only queries: Can tolerate larger novelty layer

3. **Storage Planning**: Plan for index storage growth
   - Each index is a complete snapshot
   - Historical indexes accumulate over time
   - Consider retention policies for old indexes

### Operational Guidelines

1. **Monitor Nameservice Health**: Track nameservice operations
   - Lookup latency
   - Publish success rates
   - Storage backend health

2. **Backup Strategy**: Include nameservice in backup plans
   - File-based: Backup `ns@v2/` directory
   - Storage-based: Use backend backup mechanisms

3. **Error Handling**: Handle nameservice errors gracefully
   - Lookup failures: May indicate ledger doesn't exist
   - Publish failures: May indicate contention (retry)
   - Storage errors: May indicate backend issues

## Troubleshooting

### Ledger Not Found

**Symptom**: Query fails with "ledger not found"

**Possible Causes:**
- Ledger alias misspelled
- Ledger not yet created (no transactions yet)
- Ledger retracted
- Nameservice backend misconfigured

**Solutions:**
- Verify alias spelling and format
- Check if ledger exists: `nameservice.lookup(alias)`
- Verify nameservice backend configuration
- Check ledger status (retracted?)

### Stale Query Results

**Symptom**: Queries don't see latest transactions

**Possible Causes:**
- Novelty layer not being applied
- Index lagging significantly behind commits
- Query caching issues

**Solutions:**
- Check `commit_t` vs `index_t` gap
- Verify indexing process is running
- Check query execution logs
- Consider forcing index update

### Nameservice Contention

**Symptom**: Publish operations failing with conflicts

**Possible Causes:**
- Multiple processes updating same ledger
- High transaction rate
- Storage backend throttling

**Solutions:**
- Implement retry logic with backoff
- Reduce transaction rate if possible
- Scale storage backend (if S3/DynamoDB)
- Check for process coordination issues

This foundation of ledgers and the nameservice enables Fluree's distributed, temporal graph database capabilities, providing the coordination layer needed for scalable, consistent data management.

**Differentiator**: Fluree's nameservice architecture enables true distributed deployments with coordination across multiple processes and machines, unlike single-instance databases. The separation of commits and indexes, combined with the novelty layer, enables real-time queries while maintaining efficient background indexing—a unique architectural advantage.
