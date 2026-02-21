# Expected Span Hierarchies & Tag Catalog

Source of truth: `docs/operations/telemetry.md` (Span Hierarchy section).

All operation spans use `debug_span!`. The only `info_span!` is `request` in `telemetry.rs::create_request_span()`.

## Query Traces

```
request (info, otel.name = "query:fql" or "query:sparql")
└── query_execute / sparql_execute (debug)
    ├── query_prepare (debug)
    │   ├── reasoning_prep (debug)
    │   ├── pattern_rewrite (debug, patterns_before, patterns_after)
    │   └── plan (debug, pattern_count)
    ├── query_run (debug)
    │   ├── scan (debug, per pattern)
    │   ├── join (debug)
    │   │   └── join_next_batch / join_flush_batched_binary (debug)
    │   ├── filter (debug)
    │   ├── project (debug)
    │   ├── sort (debug) → sort_blocking (debug, cross-thread)
    │   │   ├── sort_child_next_batch (cross-thread)
    │   │   └── sort_build_rows_batch (cross-thread)
    │   ├── property_join (trace)
    │   ├── group_by (trace)
    │   ├── distinct (trace)
    │   ├── limit (trace)
    │   ├── offset (trace)
    │   ├── union (trace)
    │   ├── optional (trace)
    │   └── subquery (trace)
    └── format (debug, output_format, result_count)
```

SPARQL queries use `sparql_execute` instead of `query_execute`.

## Transaction Traces

Applies to insert, transact, and upsert operations.

**FQL / SPARQL format:**
```
request (info, otel.name = "transact:fql" / "insert:fql" / "upsert:fql" / etc.)
└── transact_execute (debug)
    ├── txn_stage (debug, insert_count, delete_count)
    │   ├── where_exec (debug, pattern_count, binding_rows)
    │   ├── delete_gen (debug, retraction_count)
    │   ├── insert_gen (debug, assertion_count)
    │   ├── cancellation (debug)
    │   └── policy_enforce (debug)
    └── txn_commit (debug, flake_count, delta_bytes, current_novelty_bytes)
```

**Turtle format** (different staging span):
```
request (info, otel.name = "insert:turtle" / "upsert:turtle")
└── transact_execute (debug)
    ├── stage_turtle_insert (debug)
    │   ├── turtle_parse_to_flakes (debug)
    │   └── stage_flakes (debug, flake_count)
    └── txn_commit (debug, flake_count, delta_bytes, current_novelty_bytes)
        ├── commit_nameservice_lookup (debug)
        ├── commit_verify_sequencing (debug)
        ├── commit_namespace_delta (debug)
        ├── commit_write_raw_txn (debug)
        ├── commit_build_record (debug)
        ├── commit_write_commit_blob (debug)
        ├── commit_publish_nameservice (debug)
        ├── commit_generate_metadata_flakes (debug)
        ├── commit_populate_dict_novelty (debug)
        └── commit_apply_to_novelty (debug)
```

## Index Traces

Separate top-level traces (not nested under HTTP requests).

```
index_build (debug, ledger_id)
├── commit_chain_walk (debug)
├── commit_resolve (debug, per commit)
├── dict_merge_and_remap (debug)
├── build_all_indexes (debug)
│   └── build_index (debug, per sort order: SPOT, PSOT, POST, OPST) [cross-thread]
├── secondary_partition (debug)
├── upload_dicts (debug)
├── upload_indexes (debug)
├── build_index_root (debug)
└── BinaryIndexStore::load (debug) [cross-thread]
```

`index_gc` is a separate top-level trace (intentional, fire-and-forget `tokio::spawn`):
```
index_gc (debug, separate trace)
├── gc_walk_chain (debug)
└── gc_delete_entries (debug)
```

## Import Traces (CLI)

Service: `fluree-cli` (not `fluree-server`).

```
bulk_import (debug, alias)
├── import_chunks (debug, total_chunks, parse_threads)
├── import_index_build (debug)
│   ├── build_all_indexes (debug)
│   │   └── build_index (debug, per order) [cross-thread]
│   ├── import_cas_upload (debug)
│   └── import_publish (debug)
```

## Thread Boundary Rules

| Span | Mechanism | Why cross-thread |
|------|-----------|-----------------|
| `build_index` | `std::thread::scope` | Parallel per-sort-order builds |
| `sort_blocking` | `spawn_blocking` | CPU-intensive sort on blocking pool |
| `sort_child_next_batch` | inherits from sort_blocking | Same blocking thread |
| `sort_build_rows_batch` | inherits from sort_blocking | Same blocking thread |
| `BinaryIndexStore::load` | `spawn_blocking` | I/O on blocking pool |
| `groupby_blocking` | `spawn_blocking` | CPU-intensive grouping |

All use `Span::current()` capture + `.enter()` inside closure (safe: no `.await`).

## Known-Benign Zero-Duration Spans

These sub-microsecond metadata operations appear as zero-duration in Jaeger (duration rounds to 0us):

- `commit_namespace_delta`
- `commit_build_record`
- `commit_verify_sequencing`
- `commit_publish_nameservice`
- `commit_generate_metadata_flakes`
- `commit_populate_dict_novelty`
- `where_exec` (when no WHERE clause)
- `cancellation` (when no cancellation needed)
- `delete_gen` (when no deletes)

## Tag Catalog

| Tag | Type | Found on | Meaning |
|-----|------|----------|---------|
| `ledger_id` | string | root span, query_execute, transact_execute, index_build | Ledger alias (namespace:branch) |
| `operation` | string | root span | Handler name (query, transact, insert, etc.) |
| `error_code` | string | root span | Error identifier (empty if success) |
| `tracker_time` | string | query_execute, transact_execute | Fuel accounting: wall time |
| `tracker_fuel` | string | query_execute, transact_execute | Fuel accounting: fuel consumed |
| `insert_count` | int | txn_stage | Assertions generated |
| `delete_count` | int | txn_stage | Retractions generated |
| `flake_count` | int | txn_commit | Total flakes committed |
| `delta_bytes` | int | txn_commit | Commit size in bytes |
| `patterns_before` | int | pattern_rewrite | Pattern count before optimization |
| `patterns_after` | int | pattern_rewrite | Pattern count after optimization |
| `pattern_count` | int | plan | Patterns in query plan |
| `result_count` | int | format | Rows in query result |
| `order` | string | build_index | Sort order (SPOT/PSOT/POST/OPST) |
| `busy_ns` | int | all spans | CPU busy time (nanoseconds) |
| `idle_ns` | int | all spans | Idle/wait time (nanoseconds) |
| `thread.id` | int | all spans | OS thread ID |
| `thread.name` | string | all spans | Thread name (e.g., tokio-runtime-worker) |
