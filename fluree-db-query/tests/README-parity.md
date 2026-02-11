## Parity test scaffolding (Clojure vs Rust)

Your plan calls for “run identical queries in Clojure and Rust on the same test database and compare result sets”.
This repo already has:

- Rust-side parsing: `fluree_db_query::parse::parse_query`
- Rust-side IR: `fluree_db_query::ir::{Query, Pattern}`
- Rust-side operators: `ScanOperator`, `NestedLoopJoinOperator`, `OptionalOperator`, `FilterOperator`, `PropertyJoinOperator`
- A file-backed fixture DB: `../test-database` (used by ignored integration tests)

### Recommended approach

- **Pick a small, stable subset of the `test-database`** (or create a dedicated tiny fixture database) and define:
  - query JSON inputs
  - expected normalized result sets (sorted rows, stable column order)
- Run those through both engines and compare results.

### Practical implementation sketch

- **Rust**:
  - Load DB with `load_db(&storage, &root_id, "ledger/main").await?` (see `fluree_db_core::load_db`).
  - Parse query JSON via `parse::parse_query(...)` (already supports `FILTER` and `OPTIONAL`).
  - Execute via your planned “query runner” (if you don’t have a single entrypoint yet, it’s worth adding one small `execute_query(db, parsed_query, vars)` wrapper that builds the operator tree and collects rows).
  - Normalize results:
    - project to `select` vars
    - sort rows lexicographically (stringify SIDs/lits deterministically)

- **Clojure**:
  - Run the equivalent query against the same DB fixture.
  - Produce the same normalized shape (ordered columns + sorted rows).

### Suggested parity cases (from plan)

- Multi-pattern join (shared subject, shared object)
- Filter with pushdown (`?age > 18 AND ?age < 65`)
- OPTIONAL with partial matches (missing optional properties)
- Nested node-map traversal (join property / property-join patterns)

### Where to put this

Add a new ignored integration test file (e.g. `fluree-db-query/tests/parity_test.rs`) that:

- iterates over a table of `{name, query_json, expected_rows}`
- executes the Rust query runner and compares to expected

Then you can add a small script in `test-database` / Clojure side to generate the `expected_rows` fixtures from the Clojure engine.
