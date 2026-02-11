# Getting Started

Welcome to Fluree! This section will guide you through the essential steps to start using Fluree for your graph database needs.

## Quick Navigation

### [Quickstart: Run the Server](quickstart-server.md)

Get Fluree up and running in minutes. Learn how to:
- Install and run the Fluree server
- Configure basic settings
- Verify the server is running
- Access the HTTP API

### [Quickstart: Create a Ledger](quickstart-ledger.md)

Create your first ledger to store data. Learn how to:
- Create a new ledger using the API
- Understand ledger IDs and branching
- Set up initial configuration
- Verify ledger creation

### [Quickstart: Write Data](quickstart-write.md)

Start writing data to your ledger. Learn how to:
- Insert new entities (basic inserts)
- Upsert data (replace mode for idempotent writes)
- Update existing data (WHERE/DELETE/INSERT pattern)
- Understand JSON-LD transaction format

### [Quickstart: Query Data](quickstart-query.md)

Query your data using Fluree's powerful query languages. Learn how to:
- Write basic JSON-LD queries
- Write basic SPARQL queries
- Filter and select data
- Understand query results

### [Using Fluree as a Rust Library](rust-api.md)

Embed Fluree directly in your Rust applications. Learn how to:
- Add Fluree as a dependency in Cargo.toml
- Use the Rust API programmatically
- Implement common patterns (insert, query, update)
- Integrate BM25 and vector search
- Handle errors and configuration
- Write tests with Fluree

## What is Fluree?

Fluree is a temporal graph database that stores data as RDF triples with built-in support for:

- **Time Travel**: Query data as it existed at any point in time
- **Full-Text Search**: Integrated BM25 indexing for powerful text search
- **Vector Search**: Approximate nearest neighbor (ANN) queries
- **Policy Enforcement**: Fine-grained, data-level access control
- **Verifiable Data**: Cryptographically signed transactions
- **Graph Sources**: Integration with external data sources (Iceberg, R2RML)

## Learning Path

**For HTTP API users (server-based):**

1. **Start with the Server**: [Run the Server](quickstart-server.md) to get Fluree running
2. **Create Your First Ledger**: [Create a Ledger](quickstart-ledger.md) to set up your database
3. **Add Data**: [Write Data](quickstart-write.md) to insert your first entities
4. **Query Your Data**: [Query Data](quickstart-query.md) to retrieve and explore
5. **Core Concepts**: Read [Concepts](../concepts/README.md) to understand Fluree's architecture
6. **Deep Dive**: Explore [Query](../query/README.md), [Transactions](../transactions/README.md), and [Security](../security/README.md)
7. **Production Ready**: Review [Operations](../operations/README.md) for deployment guidance

**For Rust developers (embedded library):**

1. **Rust API Guide**: [Using Fluree as a Rust Library](rust-api.md) for embedding Fluree in your application
2. **Core Concepts**: [Concepts](../concepts/README.md) to understand how Fluree works
3. **Advanced Queries**: [Query](../query/README.md) for complex query patterns
4. **Transactions**: [Transactions](../transactions/README.md) for data modification patterns
5. **Production Ready**: [Operations](../operations/README.md) and [Dev Setup](../contributing/dev-setup.md)

## Prerequisites

- Basic understanding of graph databases or RDF (helpful but not required)
- Familiarity with JSON format
- HTTP client (curl, Postman, or your programming language's HTTP library)

## Support and Resources

- **Documentation**: This documentation provides comprehensive coverage
- **API Reference**: See [HTTP API](../api/README.md) for endpoint details
- **Troubleshooting**: Check [Troubleshooting](../troubleshooting/README.md) for common issues

Let's get started!
