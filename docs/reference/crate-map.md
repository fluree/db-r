# Crate Map

Fluree is organized into multiple Rust crates, each with a specific purpose. This document provides an overview of the crate architecture and dependencies.

## Crate Organization

```text
fluree-db/
├── fluree-db-api/          # Public API and high-level operations
├── fluree-db-query/        # Query engine (JSON-LD Query)
├── fluree-db-sparql/       # SPARQL parser and lowering
├── fluree-db-transact/     # Transaction processing
├── fluree-db-storage/      # Storage backends
├── fluree-db-index/        # Indexing and data structures
├── fluree-db-nameservice/  # Nameservice implementations
├── fluree-db-virtual/      # Virtual graph implementations
├── fluree-db-policy/       # Policy enforcement
├── fluree-db-common/       # Shared types and utilities
├── fluree-db-json-ld/      # JSON-LD processing
└── fluree-db-server/       # HTTP server (binary)
```

## Core Crates

### fluree-db-api

**Purpose:** Public API and orchestration

**Responsibilities:**
- Ledger lifecycle (create, load, drop)
- Query execution coordination
- Transaction execution (insert, upsert, update)
- Time travel resolution
- Policy application
- Dataset and view composition

**Key Types:**
- `Fluree` - Main entry point (connection + nameservice + storage)
- `Graph` - Lazy handle (alias + time) for chaining queries/transactions
- `GraphSnapshot` - Materialized snapshot bound to `Fluree` (query without passing `&fluree`)
- `LedgerState` - Loaded ledger snapshot (indexed DB + novelty overlay)
- `FlureeView` - Composable query/view primitive (policy + reasoning wrappers; underlying type used by `GraphSnapshot`)
- `QueryResult` - Query results with formatting methods
- `TransactResult` - Commit receipt + updated ledger state

**Dependencies:**
- fluree-db-query
- fluree-db-sparql
- fluree-db-transact
- fluree-db-storage
- fluree-db-nameservice

### fluree-db-query

**Purpose:** Query engine for JSON-LD Query

**Responsibilities:**
- Query parsing
- Query planning
- Query execution
- Join optimization
- Filter evaluation

**Key Types:**
- `Query` - Parsed query
- `ExecutionPlan` - Query plan
- `Binding` - Variable binding
- `Solution` - Query solution

**Dependencies:**
- fluree-db-index
- fluree-db-common

### fluree-db-sparql

**Purpose:** SPARQL parsing and execution

**Responsibilities:**
- SPARQL lexing and parsing
- AST construction
- Lowering to internal IR
- SPARQL-specific optimizations

**Key Types:**
- `Query` - SPARQL query AST
- `Pattern` - Graph pattern
- `Expression` - Filter expression

**Dependencies:**
- fluree-db-query
- fluree-db-common

### fluree-db-transact

**Purpose:** Transaction processing

**Responsibilities:**
- JSON-LD parsing
- RDF triple generation
- Validation
- Flake generation
- Commit creation

**Key Types:**
- `Transaction` - Parsed transaction
- `Flake` - Internal triple representation
- `Commit` - Commit metadata

**Dependencies:**
- fluree-db-json-ld
- fluree-db-storage
- fluree-db-common

### fluree-db-storage

**Purpose:** Storage abstraction

**Responsibilities:**
- Storage backend abstraction
- Commit storage
- Index storage
- Address resolution

**Key Types:**
- `Storage` - Storage trait
- `MemoryStorage` - In-memory implementation
- `FileStorage` - File system implementation
- `AwsStorage` - S3/DynamoDB implementation

**Dependencies:**
- fluree-db-common

### fluree-db-index

**Purpose:** Index structures and operations

**Responsibilities:**
- SPOT, POST, OPST, PSOT indexes
- Index building
- Index scanning
- Range queries

**Key Types:**
- `Index` - Index trait
- `Flake` - Indexed triple
- `IndexScan` - Scan iterator

**Dependencies:**
- fluree-db-common

### fluree-db-nameservice

**Purpose:** Nameservice implementations

**Responsibilities:**
- Nameservice abstraction
- Ledger metadata management
- Publish/lookup operations
- Storage backend coordination

**Key Types:**
- `NameService` - Trait
- `NsRecord` - Nameservice record
- `FileNameService` - File-based implementation
- `AwsNameService` - DynamoDB-based implementation

**Dependencies:**
- fluree-db-common
- fluree-db-storage

## Specialized Crates

### fluree-db-virtual

**Purpose:** Virtual graph implementations

**Responsibilities:**
- BM25 indexing
- Vector search integration
- Iceberg integration
- R2RML mapping

**Key Types:**
- `VirtualGraph` - Virtual graph trait
- `Bm25Index` - BM25 implementation
- `VectorIndex` - Vector search wrapper
- `IcebergSource` - Iceberg reader
- `R2rmlMapping` - R2RML mapper

**Dependencies:**
- fluree-db-api
- fluree-db-query
- External: tantivy (BM25), iceberg-rust

### fluree-db-policy

**Purpose:** Policy enforcement

**Responsibilities:**
- Policy parsing
- Policy evaluation
- Query augmentation
- Transaction authorization

**Key Types:**
- `Policy` - Policy definition
- `PolicyEngine` - Evaluation engine
- `PolicyContext` - Evaluation context

**Dependencies:**
- fluree-db-query
- fluree-db-common

### fluree-db-json-ld

**Purpose:** JSON-LD processing

**Responsibilities:**
- JSON-LD expansion
- JSON-LD compaction
- @context handling
- IRI resolution

**Key Types:**
- `JsonLdContext` - Context definition
- `JsonLdExpander` - Expansion logic
- `JsonLdCompactor` - Compaction logic

**Dependencies:**
- json-ld (external)

### fluree-db-common

**Purpose:** Shared types and utilities

**Responsibilities:**
- Common types (IRI, Literal, etc.)
- Error types
- Utility functions
- Constants

**Key Types:**
- `Iri` - IRI representation
- `Literal` - Typed literal
- `Error` - Error types
- `Namespace` - Namespace mappings

**Dependencies:**
- None (foundation crate)

### fluree-search-protocol

**Purpose:** Search service protocol types

**Responsibilities:**
- Request/response structs (serde-serializable)
- Error model and error codes
- Protocol version constants
- Query variant definitions (BM25, Vector)

**Dependencies:**
- serde, serde_json, thiserror

### fluree-search-service

**Purpose:** Search service backend implementations

**Responsibilities:**
- `SearchBackend` trait and dispatch
- BM25 backend (index loading, caching, search)
- Vector backend (index loading, caching, search) — feature-gated on `vector`
- Composite backend for multi-type dispatch
- Index caching with TTL

**Dependencies:**
- fluree-search-protocol
- fluree-db-query
- fluree-db-core

## Binary Crates

### fluree-search-httpd

**Purpose:** Standalone HTTP search server

**Responsibilities:**
- HTTP API for remote search queries (BM25 + vector)
- Index loading from storage/nameservice
- Health and capabilities endpoints
- Configurable via CLI flags

**Dependencies:**
- fluree-search-protocol
- fluree-search-service
- axum, tokio, clap

### fluree-db-server

**Purpose:** HTTP server

**Responsibilities:**
- HTTP API endpoints
- Request routing
- Response formatting
- TLS/SSL
- CORS handling

**Dependencies:**
- fluree-db-api
- fluree-db-transact
- fluree-db-query
- axum (web framework)

## Dependency Graph

```text
fluree-db-server
    └── fluree-db-api
        ├── fluree-db-query
        │   ├── fluree-db-index
        │   │   └── fluree-db-common
        │   └── fluree-db-common
        ├── fluree-db-sparql
        │   ├── fluree-db-query
        │   └── fluree-db-common
        ├── fluree-db-transact
        │   ├── fluree-db-json-ld
        │   ├── fluree-db-storage
        │   └── fluree-db-common
        ├── fluree-db-storage
        │   └── fluree-db-common
        ├── fluree-db-nameservice
        │   ├── fluree-db-storage
        │   └── fluree-db-common
        ├── fluree-db-virtual
        │   ├── fluree-db-query
        │   └── fluree-db-common
        └── fluree-db-policy
            ├── fluree-db-query
            └── fluree-db-common
```

## External Dependencies

### Key External Crates

**Web Framework:**
- `axum` - HTTP server framework
- `tokio` - Async runtime
- `tower` - Service abstractions

**Serialization:**
- `serde` - Serialization framework
- `serde_json` - JSON support

**RDF:**
- `json-ld` - JSON-LD processing
- `oxiri` - IRI parsing and validation

**Storage:**
- `aws-sdk-s3` - AWS S3 client
- `aws-sdk-dynamodb` - AWS DynamoDB client
- `rocksdb` - Embedded database (planned)

**Search:**
- `tantivy` - BM25 full-text search
- `usearch` - Vector similarity search (HNSW indexes)

**Analytics:**
- `iceberg-rust` - Apache Iceberg support
- `parquet` - Parquet file reading

**Cryptography:**
- `ed25519-dalek` - Ed25519 signatures
- `ring` - Cryptographic operations

## Building Specific Crates

### Build Individual Crate

```bash
cd fluree-db-query
cargo build --release
```

### Run Tests

```bash
cd fluree-db-query
cargo test
```

### Build with Features

```bash
cargo build --features experimental
```

### Build Server Only

```bash
cargo build --release --bin fluree-db-server
```

## Crate Versions

All crates use synchronized versioning:
- Current: 0.1.0
- Updated together
- Version parity maintained

Check versions:

```bash
cargo tree | grep fluree-db
```

## Development

### Adding New Features

Typical development flow:

1. **Add to appropriate crate:**
   - New query feature → fluree-db-query
   - New transaction feature → fluree-db-transact
   - New storage backend → fluree-db-storage

2. **Update dependencies:**
   ```toml
   [dependencies]
   fluree-db-common = { path = "../fluree-db-common" }
   ```

3. **Add tests:**
   ```bash
   cargo test
   ```

4. **Update API if needed:**
   Expose in fluree-db-api if public

### Crate Guidelines

- **Single responsibility:** Each crate has clear purpose
- **Minimal dependencies:** Depend only on what's needed
- **Clear interfaces:** Well-defined public APIs
- **Comprehensive tests:** Unit and integration tests
- **Documentation:** Rustdoc for public APIs

## Related Documentation

- [Contributing: Dev Setup](../contributing/dev-setup.md) - Development environment
- [Contributing: Tests](../contributing/tests.md) - Testing guide
- [Glossary](glossary.md) - Term definitions
- [Compatibility](compatibility.md) - Standards compliance
