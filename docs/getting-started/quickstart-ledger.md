# Quickstart: Create a Ledger

Ledgers are Fluree's fundamental unit of data organization—similar to databases in traditional systems. This guide shows you how to create your first ledger.

## Understanding Ledger Addresses

Ledgers are identified by **ledger addresses** with the format `ledger-name:branch`:

- `mydb:main` - Primary branch of the "mydb" ledger
- `customers:dev` - Development branch of the "customers" ledger
- `inventory:prod` - Production branch

The default branch is `main`, so `mydb` is equivalent to `mydb:main`.

## Creating a Ledger

### Rust API (Library Usage)

When using Fluree as a Rust library, create ledgers explicitly with `create_ledger`:

```rust
let fluree = FlureeBuilder::memory().build_memory();

// Create a new ledger (returns LedgerState at t=0)
let ledger = fluree.create_ledger("mydb").await?;

// Now insert data
let result = fluree.graph("mydb:main")
    .transact()
    .insert(&data)
    .commit()
    .await?;
```

`create_ledger` registers the ledger in the nameservice and returns a genesis `LedgerState` ready for transactions. It returns `ApiError::LedgerExists` (HTTP 409) if the ledger already exists.

To load an existing ledger, use `ledger`:

```rust
let ledger = fluree.ledger("mydb:main").await?;
```

### HTTP API (Server Usage)

Via the HTTP API, ledgers can be created **implicitly** on the first transaction, or **explicitly** via `POST /ledgers`. Simply send a transaction to a ledger address, and the server will create it automatically.

#### Method 1: Via First Transaction

The simplest way to create a ledger is to write data to it:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:alice",
        "@type": "schema:Person",
        "schema:name": "Alice",
        "schema:email": "alice@example.org"
      }
    ]
  }'
```

Response:

```json
{
  "t": 1,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456...",
  "address": "fluree:memory:abc123..."
}
```

The ledger `mydb:main` now exists!

#### Method 2: Empty Initialization

Create an empty ledger without data:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {},
    "@graph": []
  }'
```

This creates the ledger with no initial data (transaction `t=1` will be empty).

## Verifying Ledger Creation

### List All Ledgers

```bash
curl http://localhost:8090/ledgers
```

Response:

```json
{
  "ledgers": [
    {
      "ledger_address": "mydb:main",
      "branch": "main",
      "commit_t": 1,
      "index_t": 0,
      "created": "2024-01-22T10:30:00.000Z"
    }
  ]
}
```

### Query the Ledger

Verify you can query the new ledger:

```bash
curl -X POST http://localhost:8090/query \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "schema": "http://schema.org/"
    },
    "from": "mydb:main",
    "select": ["?name"],
    "where": [
      { "@id": "?person", "schema:name": "?name" }
    ]
  }'
```

Response:

```json
[
  { "name": "Alice" }
]
```

## Ledger Naming Best Practices

### Descriptive Names

Choose names that clearly indicate purpose:

Good examples:
- `customers:main`
- `inventory:prod`
- `analytics:warehouse`

Bad examples:
- `db1:main`
- `test:main`
- `data:main`

### Hierarchical Organization

Use slashes for logical grouping:

```text
tenant/app:main
tenant/app:dev
department/project:feature-x
```

### Branch Naming

Establish consistent branch naming conventions:

```text
mydb:main              - Production branch
mydb:dev               - Development branch
mydb:staging           - Staging branch
mydb:feature-auth      - Feature branch
mydb:bugfix-login      - Bug fix branch
```

## Working with Branches

### Creating a New Branch

Branches are independent ledgers. Create a new branch by transacting to it:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:dev \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/"
    },
    "@graph": [
      {
        "@id": "ex:bob",
        "@type": "schema:Person",
        "schema:name": "Bob"
      }
    ]
  }'
```

Now you have two independent ledgers:
- `mydb:main` (with Alice)
- `mydb:dev` (with Bob)

### Understanding Branch Independence

Branches are completely independent—changes in one don't affect the other:

```bash
# Query main branch
curl -X POST http://localhost:8090/query \
  -d '{"from": "mydb:main", "select": ["?name"], "where": [{"@id": "?person", "schema:name": "?name"}]}'
# Returns: [{"name": "Alice"}]

# Query dev branch
curl -X POST http://localhost:8090/query \
  -d '{"from": "mydb:dev", "select": ["?name"], "where": [{"@id": "?person", "schema:name": "?name"}]}'
# Returns: [{"name": "Bob"}]
```

## Ledger Metadata

Each ledger maintains metadata accessible via the nameservice:

- **commit_t**: Latest transaction time
- **index_t**: Latest indexed transaction time
- **commit_address**: Storage address of latest commit
- **index_address**: Storage address of latest index
- **default_context**: Default JSON-LD @context for the ledger

### Checking Ledger Status

```bash
curl http://localhost:8090/ledgers/mydb:main
```

Response:

```json
{
  "ledger_address": "mydb:main",
  "branch": "main",
  "commit_t": 1,
  "index_t": 1,
  "commit_address": "fluree:memory:commit:abc123...",
  "index_address": "fluree:memory:index:def456...",
  "created": "2024-01-22T10:30:00.000Z",
  "last_updated": "2024-01-22T10:30:05.000Z"
}
```

### Understanding Commit vs Index

- **commit_t**: Most recent transaction (always up-to-date)
- **index_t**: Most recent indexed snapshot (may lag behind commits)
- **Gap**: If `commit_t > index_t`, there's a "novelty layer" being indexed

See [Ledgers and Nameservice](../concepts/ledgers-and-nameservice.md) for details.

## Multi-Tenant Scenarios

For multi-tenant applications, use hierarchical naming:

```text
tenant1/app:main
tenant1/app:dev
tenant2/app:main
tenant2/app:dev
```

Or use separate ledgers per tenant:

```text
tenant1-customers:main
tenant1-orders:main
tenant2-customers:main
tenant2-orders:main
```

## Setting Default Context

Specify a default JSON-LD @context for a ledger to simplify queries and transactions:

```bash
curl -X POST http://localhost:8090/transact?ledger=mydb:main \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {
      "ex": "http://example.org/ns/",
      "schema": "http://schema.org/",
      "xsd": "http://www.w3.org/2001/XMLSchema#"
    },
    "@graph": []
  }'
```

Future transactions and queries can omit the context if desired (though explicit is recommended).

## Common Patterns

### Development Workflow

```text
1. Create main branch: mydb:main
2. Create dev branch: mydb:dev
3. Develop and test in dev
4. Copy desired state to main (application logic)
5. Repeat
```

### Feature Branching

```text
1. Create feature branch: mydb:feature-x
2. Develop feature in isolation
3. Test thoroughly
4. Merge to main (via application logic)
5. Optionally retract feature branch
```

### Environment Separation

```text
mydb:dev      - Development environment
mydb:staging  - Staging environment
mydb:prod     - Production environment
```

## Troubleshooting

### Ledger Already Exists

If you try to query a ledger before it exists:

```text
Error: Ledger not found: mydb:main
```

Solution: Create the ledger with a transaction first.

### Permission Issues (File Storage)

If using file storage, ensure the server has write permissions:

```bash
# Check data directory permissions
ls -la /path/to/data

# Fix permissions if needed
sudo chown -R fluree:fluree /path/to/data
chmod -R 755 /path/to/data
```

### AWS Storage Issues

For AWS storage, verify credentials and bucket access:

```bash
# Test S3 access
aws s3 ls s3://your-fluree-bucket/

# Test DynamoDB access
aws dynamodb describe-table --table-name fluree-nameservice
```

## Next Steps

Now that you have a ledger:

1. [Write Data](quickstart-write.md) - Learn how to insert, upsert, and update data
2. [Query Data](quickstart-query.md) - Explore your data with queries
3. [Concepts: Ledgers](../concepts/ledgers-and-nameservice.md) - Deep dive into ledger architecture

## Related Documentation

- [Ledgers and Nameservice](../concepts/ledgers-and-nameservice.md) - Architectural details
- [Transactions](../transactions/README.md) - Writing data to ledgers
- [Storage Modes](../operations/storage.md) - Storage backend options
