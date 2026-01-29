# Design Notes

This document links to detailed design documentation in the repository.

## Architecture Documents

Design documents are located in the `/docs/design/` directory and various planning documents at the repository root.

### Core Architecture

**History Queries (Implemented):**
- History queries are now part of the standard query API using time range specifiers
- JSON-LD: Use `@t` and `@op` annotations with time range in `from` clause
- SPARQL: Use RDF-star syntax (`<< s p o >>`) with `FROM...TO` clauses
- See: [Time Travel](../concepts/time-travel.md) for usage documentation

### Implementation Plans

**BM25 Indexing:**
- Location: `/BM25_INDEXING_IMPLEMENTATION_PLAN.md`
- Covers: Full-text search implementation, BM25 algorithm, index structure

**BM25 Snapshot History and Time Travel:**
- Location: `/BM25_SNAPSHOT_HISTORY_AND_TIME_TRAVEL_DESIGN.md`
- Covers: Time travel for BM25 indexes, snapshot management

**Iceberg/Polaris R2RML:**
- Location: `/ICEBERG_POLARIS_R2RML_IMPLEMENTATION_PLAN.md`
- Covers: Virtual graph integration, Iceberg support, R2RML mapping

**Reasoning and Rules:**
- Location: `/REASONING_AND_RULES_IMPLEMENTATION_PLAN.md`
- Covers: Inference engine, rule evaluation, reasoning capabilities

**Ledger Update Events:**
- Location: `/LEDGER_UPDATE_EVENTS_AND_MAINTENANCE_PLAN.md`
- Covers: Event system, update notifications, maintenance operations

### Testing and Quality

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

## Architectural Principles

### Separation of Concerns

Each crate has clear responsibility:
- **fluree-db-query:** Query execution only
- **fluree-db-transact:** Transaction processing only
- **fluree-db-storage:** Storage abstraction only

### Interface-Driven Design

Define traits for major abstractions:

```rust
pub trait Storage {
    async fn read(&self, address: &str) -> Result<Vec<u8>>;
    async fn write(&self, address: &str, data: &[u8]) -> Result<()>;
}
```

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
- [ ] Migration path provided (if breaking)
- [ ] Documentation plan included

## Related Documentation

- [Dev Setup](dev-setup.md) - Development environment
- [Tests](tests.md) - Testing guide
- [Crate Map](../reference/crate-map.md) - Code architecture
- [Contributing](README.md) - Contribution guidelines
