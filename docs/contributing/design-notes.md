# Design Notes

This document links to detailed design documentation in the repository.

## Architecture Documents

Design documents are located in the `/docs/design/` directory and various planning documents at the repository root.

### Core Architecture

**Storage Traits:**
- Location: `/docs/design/storage-traits.md`
- Covers: Storage trait hierarchy, implementing backends, type erasure

**History Queries (Implemented):**
- History queries are now part of the standard query API using time range specifiers
- JSON-LD: Use `@t` and `@op` annotations with time range in `from` clause
- SPARQL: Use RDF-star syntax (`<< s p o >>`) with `FROM...TO` clauses
- See: [Time Travel](../concepts/time-travel.md) for usage documentation

### Implementation Plans

**BM25 Indexing:**
- Location: `/BM25_INDEXING_IMPLEMENTATION_PLAN.md`
- Covers: Full-text search implementation, BM25 algorithm, index structure

**Graph Source Index Manifests (graph-source-owned history):**
- Location: `/docs/design/graph-source-index-manifests.md`
- Covers: Nameservice pointers only, graph-source-owned manifests, BM25 time travel, vector head-only

**Iceberg/Polaris R2RML:**
- Location: `/ICEBERG_POLARIS_R2RML_IMPLEMENTATION_PLAN.md`
- Covers: Graph source integration, Iceberg support, R2RML mapping

**Reasoning and Rules:**
- Location: `/REASONING_AND_RULES_IMPLEMENTATION_PLAN.md`
- Covers: Inference engine, rule evaluation, reasoning capabilities

**Ledger Update Events:**
- Location: `/LEDGER_UPDATE_EVENTS_AND_MAINTENANCE_PLAN.md`
- Covers: Event system, update notifications, maintenance operations

### Testing and Quality

**W3C SPARQL Compliance Test Suite:**
- Developer guide: `/docs/contributing/sparql-compliance.md`
- Test suite crate: `/testsuite-sparql/`
- Covers: Manifest-driven W3C compliance testing, failure analysis workflow

**Integration Tests Strategy:**
- Location: `/INTEGRATION_TESTS_STRATEGY.md`
- Covers: Test organization, coverage goals, CI/CD integration

**Clojure Test Porting:**
- Location: `/CLOJURE_TEST_PORTING_MAP.md`
- Covers: Mapping Clojure tests to Rust, parity tracking

**Test Suite Mapping:**
- Location: `/parity_clojure_to_rust_suite_map.tsv`
- Covers: Test coverage tracking

### Error Handling

**Error Code Mapping:**
- Location: `/ERROR_CODE_MAPPING.md`
- Covers: Error codes, HTTP status mapping, error taxonomy

**Error Types Implementation:**
- Location: `/ERROR_TYPES_IMPLEMENTATION.md`
- Covers: Error type hierarchy, error construction, cause chains

**Error Cause Chains:**
- Location: `/ERROR_CAUSE_CHAINS_SUMMARY.md`
- Covers: Error propagation, context preservation

### Logging and Observability

**Logging Implementation:**
- Location: `/LOGGING_IMPLEMENTATION_SUMMARY.md`
- Covers: Logging architecture, structured logging, log levels

**Logging and Tracing Plan:**
- Location: `/LOGGING_TRACING_PLAN.md`
- Covers: Tracing integration, distributed tracing, observability

### Feature Parity

**Numeric/Temporal Parity:**
- Location: `/NUMERIC_TEMPORAL_PARITY_PLAN.md`
- Covers: Numeric types, temporal operations, XSD datatypes

**Clojure Test Inventory:**
- Location: `/parity_clojure_test_inventory.tsv`
- Covers: Feature parity tracking

## Reading Design Documents

### Document Structure

Design documents typically include:

1. **Overview:** High-level description
2. **Motivation:** Why this feature/change?
3. **Design:** Detailed design and architecture
4. **Implementation:** Implementation approach
5. **Testing:** How to test
6. **Alternatives:** Considered alternatives
7. **Status:** Implementation status

### Status Indicators

Documents may include status:
- "Implemented" - Complete
- "In Progress" - Being implemented
- "Planned" - Not yet started
- "Deferred" - Postponed

### Implementation Tracking

Many design docs include implementation checklists showing what's complete.

## Contributing Design Documents

### When to Write Design Doc

Write design doc for:
- Major new features
- Significant changes
- Complex implementations
- Breaking changes
- Performance-critical code

### Design Doc Template

```markdown
# Feature Name

## Overview

Brief description of the feature.

## Motivation

Why is this needed? What problem does it solve?

## Design

Detailed design including:
- Architecture
- Data structures
- Algorithms
- Interfaces

## Implementation Plan

1. Phase 1: ...
2. Phase 2: ...
3. Phase 3: ...

## Testing Strategy

How will this be tested?

## Alternatives Considered

What other approaches were considered and why not chosen?

## Open Questions

Unresolved questions or areas needing discussion.

## Status

Current implementation status.
```

### Where to Add

Add design docs to:
- `/docs/design/` - Core architecture and detailed designs
- Repository root - Implementation plans and tracking docs

### Review Process

1. Create draft design doc
2. Open pull request or RFC issue
3. Gather feedback
4. Iterate on design
5. Get approval before implementing
6. Update doc as implementation progresses

## Internal Naming Conventions (Graph Identities)

These conventions govern how we name types and variables in Rust code related to graphs, ledgers, and identifiers. See [Graph Identities and Naming](../reference/graph-identities.md) for the user-facing naming.

### Canonical identities: prefer explicit types, not raw `String`
Internally we should distinguish:
- **`LedgerId`**: the durable ledger identifier (e.g., `acme/people:main`)
- **`GraphIri`**: canonical graph identity used by SPARQL (`Arc<str>` or validated URL/IRI type)
- **`GraphRef`**: user input token, resolved to a `GraphIri` using base rules

Even if these are initially just `struct LedgerId(Arc<str>)` newtypes, they prevent accidental mixing and make APIs self-documenting.

### Naming rules (make each word mean one thing)
This repo reserves `_id` for **content identifiers** (e.g. `commit_id`, `index_id`, `default_context_id`) and **identity/lookup keys** (`name:branch` tokens). The recommended rule set:

- **`id`**: canonical identifier used as a cache key / stable identity.
  - For ledgers this is the full `name:branch` form (e.g., `people:main`).
  - For graph sources this is the full `name:branch` form (e.g., `products-search:main`).
- **`_id` (for content references)**: a content identifier (ContentId) used by the storage layer to fetch immutable artifacts.
  - Examples: `commit_id`, `index_id`, `default_context_id`.
- **`name`**: a base name without branch (e.g., `people`).
- **`branch`**: the branch name (e.g., `main`).
- **`alias`**: a human-friendly label, and only that. For ledger identifiers, prefer `ledger_id`.

Practical guidelines:
- If a string is used to load/cache/lookup a ledger, call it **`ledger_id`** (not `ledger_alias`, not `ledger_address`).
- If a string is used to load/cache/lookup a graph source by `name:branch`, call it **`graph_source_id`** (not `gs_alias`, not `graph_source_alias`).
- If a value identifies a content-addressed artifact (commit, index, context), call it **`*_id`** (e.g., `commit_id`, `index_id`, `default_context_id`).
- If a string is used to identify a graph in SPARQL (`FROM`, `GRAPH`), call it **`graph_iri`** (canonical) or **`graph_ref`** (user input).
- Avoid having two different meanings for the same field name across crates.

### Ledger vs Db vs Graph (internal meaning)
- **Ledger**: the durable identity + commit chain + publication state (nameservice record).
- **LedgerSnapshot**: the indexed snapshot value used for range/scan (hot path).
- **Graph**: query scoping mechanism (active graph, dataset graph selection, `GRAPH` operator).

Avoid using "graph" as a synonym for "db" in code comments. Prefer:
- "ledger graph" (RDF graph inside a ledger)
- "graph IRI" (identifier)
- "graph source" (registry/resolution concept)

### "Dataset" naming internally
SPARQL calls the set-of-graphs a "dataset", and the code already models a `DataSet`.
For product-facing APIs and docs, prefer "federation", but internally keep `DataSet` as the SPARQL-aligned term.

## Architectural Principles

### Separation of Concerns

Each crate has clear responsibility:
- **fluree-db-query:** Query execution only
- **fluree-db-transact:** Transaction processing only
- **fluree-db-core:** Core types, traits (including storage), and runtime-agnostic logic
- **fluree-db-storage-aws:** AWS-specific storage implementations (S3, DynamoDB)

### Interface-Driven Design

Define traits for major abstractions:

```rust
// Layered trait hierarchy for storage
pub trait StorageRead: Debug + Send + Sync {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>>;
    async fn exists(&self, address: &str) -> Result<bool>;
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

pub trait StorageWrite: Debug + Send + Sync {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()>;
    async fn delete(&self, address: &str) -> Result<()>;
}

// Marker trait for full capability (blanket impl)
pub trait Storage: StorageRead + ContentAddressedWrite {}
```

See [Storage Traits Design](../design/storage-traits.md) for the complete architecture.

### Performance by Design

- Use appropriate data structures
- Minimize allocations
- Avoid unnecessary copies
- Profile before optimizing

### Testability

Design for testability:
- Injectable dependencies
- Clear interfaces
- Mockable components

## Code Review Checklist

When reviewing design docs:

- [ ] Problem clearly stated
- [ ] Solution well-explained
- [ ] Alternatives considered
- [ ] Performance implications discussed
- [ ] Testing strategy defined
- [ ] Breaking changes identified
- [ ] Documentation plan included

## Related Documentation

- [Dev Setup](dev-setup.md) - Development environment
- [Tests](tests.md) - Testing guide
- [Crate Map](../reference/crate-map.md) - Code architecture
- [Contributing](README.md) - Contribution guidelines
