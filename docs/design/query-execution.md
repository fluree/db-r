---
title: Query execution and overlay merge
---

This document describes the **single query execution pipeline** in DB-R and how it combines:

- **Indexed data** (binary columnar indexes)
- **Overlay data** (novelty + staged flakes)

It also calls out where **graph scoping** (`g_id`) is applied so named graphs remain isolated.

## Pipeline overview

```mermaid
flowchart TD
  LedgerState -->|produces| LedgerSnapshot
  LedgerSnapshot -->|shared substrate| GraphDb
  GraphDb -->|single-ledger| QueryRunner
  GraphDb -->|member_of| DataSetDb
  DataSetDb -->|federated| QueryRunner
  QueryRunner -->|scan index + merge overlay| ScanOperator
  ScanOperator -->|fast path| BinaryScanOperator
  ScanOperator -->|fallback| RangeScanOperator
  BinaryScanOperator --> BinaryCursor
  BinaryCursor -->|graph-scoped decode| BinaryGraphView
  RangeScanOperator -->|delegates| range_with_overlay
  range_with_overlay -->|delegates| RangeProvider
```

## Where this exists in code

- **API entrypoints**
  - `fluree-db-api/src/view/query.rs`: single-ledger `GraphDb` queries (`query_view`)
  - `fluree-db-api/src/view/dataset_query.rs`: dataset queries (`DataSetDb`)

- **Unified query runner**
  - `fluree-db-query/src/execute/runner.rs`
    - `prepare_execution(db: GraphDbRef<'_>, query: &ExecutableQuery)` builds derived facts/ontology (if enabled), rewrites patterns, and builds the operator tree.
    - `execute_prepared(...)` runs the operator tree using an `ExecutionContext`.

- **Scan operators**
  - `fluree-db-query/src/binary_scan.rs`
    - `ScanOperator` selects the execution mode at `open()` time:
      - `BinaryScanOperator` (streaming cursor, integer-ID pipeline) when a `BinaryIndexStore` is available and the query mode is compatible
      - `RangeScanOperator` fallback that calls `fluree-db-core::range_with_overlay(...)`

- **Range fallback**
  - `fluree-db-core/src/range.rs`: `range_with_overlay(snapshot, g_id, overlay, ...)`
  - `fluree-db-core/src/range_provider.rs`: `RangeProvider` trait implemented by the binary range provider

## Graph scoping (`g_id`)

Graph scoping is applied at two key boundaries:

- **Binary streaming path**: `BinaryCursor` operates on a `BinaryGraphView` (graph-scoped decode handle), ensuring leaf/leaflet decoding, predicate dictionaries, and specialty arenas are graph-isolated.
- **Range path**: `range_with_overlay(snapshot, g_id, overlay, ...)` passes `g_id` into the `RangeProvider`, which routes the range query to the correct per-graph index segments.

Overlay providers are **graph-scoped** at the trait boundary: the overlay hook receives `g_id` and must only return flakes for that graph. This keeps multi-tenant named graphs isolated even when overlay data is sourced externally.

## Overlay merge semantics (high level)

Both scan paths implement the same logical behavior:

- Read matching flakes from the **indexed base** (binary files)
- Read matching flakes from the **overlay** (novelty/staged)
- Merge them using `(t, op)` semantics so retractions cancel assertions as-of the query time bound

The details differ:

- `BinaryScanOperator` translates overlay flakes into integer-ID space and merges them into the decoded columnar stream.
- `RangeScanOperator` delegates to `range_with_overlay`, which combines `RangeProvider` output with overlay output.

