# CLAUDE.md — Fluree DB (Rust)

Project-level guidance for AI-assisted development on the `db-r` repository.

## What This Project Is

Fluree DB is a semantic graph database implemented in Rust. It stores RDF-style
triples (subject-predicate-object "flakes"), supports JSON-LD Query and SPARQL,
and exposes an HTTP API via `fluree-db-server`. The codebase is a multi-crate
Cargo workspace (~35 crates) organized in a layered architecture.

See `docs/reference/crate-map.md` for the full crate dependency graph.

## Build & Check Commands

```bash
# Full workspace build
cargo build --workspace

# Server binary only (faster iteration)
cargo build -p fluree-db-server

# Release build with OTEL support
cargo build --release -p fluree-db-server --features otel

# --- Run these before every commit ---

# Format (must produce zero diff)
cargo fmt --all -- --check

# Lint (treat warnings as errors)
cargo clippy --workspace --all-targets -- -D warnings

# Tests
cargo test --workspace

# If you touched fluree-db-server with otel feature:
cargo clippy -p fluree-db-server --features otel -- -D warnings
cargo test -p fluree-db-server --features otel
```

## Running the Server

```bash
# Dev mode (in-memory storage, default port 8090)
cargo run -p fluree-db-server

# With file storage and debug logging
cargo run -p fluree-db-server -- --storage-path ./dev-data --log-level debug

# Env-var equivalent
FLUREE_STORAGE_PATH=./dev-data RUST_LOG=debug cargo run -p fluree-db-server
```

## Workspace Architecture

```
Layer 5 (Top)        fluree-db-server          HTTP server binary (axum)
                            |
                     fluree-db-api              Public API, orchestration
                            |
Layer 4 (Features)   policy, shacl, reasoner, credential, crypto
                            |
Layer 3 (Query)      fluree-db-query  <--  fluree-db-sparql
                            |
Layer 2 (Data)       ledger, indexer, novelty, connection
                            |
Layer 1 (Core)       fluree-db-core            Core types, index, range scan
                            |
Layer 0 (Foundation) fluree-vocab, fluree-sse, fluree-db-tabular
```

Dependencies flow **downward only**. Lower layers must never depend on higher
ones. See `docs/reference/crate-map.md` for the full map with responsibilities.

## Core Domain Types

| Type | Crate | Purpose |
|------|-------|---------|
| `Sid` | `fluree-db-core` | Subject ID — compact (namespace_code, name) pair representing an IRI. Uses `Arc<str>` for cheap clones. |
| `Flake` | `fluree-db-core` | The fundamental data unit: `(s, p, o, dt, t, op, meta)`. `op=true` is assert, `op=false` is retract. |
| `FlakeValue` | `fluree-db-core` | Polymorphic object value (Ref, Boolean, Long, Double, String, DateTime, Vector, etc.). Untagged serde. |
| `Binding` | `fluree-db-query` | Query result value. Variants: `Unbound`, `Poisoned` (failed OPTIONAL), `Sid`, `Lit`, `Grouped`. |
| `Batch` | `fluree-db-query` | Columnar result batch — `Vec<Vec<Binding>>` rows with a `VarRegistry` schema. |
| `Db<S, C>` | `fluree-db-core` | Indexed database snapshot, generic over Storage (`S`) and NodeCache (`C`). |
| `LedgerState<S, C>` | `fluree-db-ledger` | Full ledger = indexed `Db` + in-memory `Novelty` overlay. |
| `Tracker` | `fluree-db-core` | Zero-overhead opt-in tracking for time, fuel, and policy stats. |

## The `<S, C>` Generic Pattern

Most query and transaction functions are generic over two type parameters:

- **`S: Storage`** — Where data is read/written (memory, file, S3, proxy).
  Defined in `fluree-db-core/src/storage.rs`.
- **`C: NodeCache`** — How resolved index nodes are cached (Moka, simple LRU,
  none). Defined in `fluree-db-core/src/cache.rs`.

This enables testability (inject `MemoryStorage` + `NoCache` in tests) and
runtime flexibility without dynamic dispatch on the hot path.

## Key Code Patterns

### Error Handling

- All crates use `thiserror::Error` derive for error enums.
- Errors chain via `#[from]` for automatic conversion up the call stack.
- Builders accumulate errors in a `Vec<BuilderError>` and report all at once on
  `.validate()` / `.execute()`, rather than failing on the first problem.
- HTTP errors map through `ServerError -> IntoResponse` with structured JSON
  bodies including `@type` (error IRI), `status`, `error` (message).
- Feature-gated error variants use `#[cfg(feature = "...")]`.

### Async

- Runtime: **tokio** (full features). `#[tokio::test]` for async tests.
- `async_trait` on trait definitions where async methods are needed.
- `S: Storage + 'static, C: NodeCache + 'static` bounds where tasks are spawned.
- `BoxedOperator<S, C> = Box<dyn Operator<S, C> + Send + Sync>` for dynamic
  dispatch in the query operator tree.

### Serialization

- `serde` + `serde_json` for JSON. `simd-json` for high-throughput parsing.
- `postcard` for compact binary encoding in storage/index layers.
- Custom `Serialize`/`Deserialize` impls on `Sid` (tuple format) and
  `FlakeValue` (`#[serde(untagged)]`).
- `#[serde(skip_serializing_if = "Option::is_none")]` on optional metadata.

### Operator Lifecycle

Query operators implement `open() -> next_batch() -> close()` following a state
machine (`Created -> Open -> Exhausted -> Closed`). Always call `close()` even
on error paths.

### Tracing / Observability

- `tracing` is the only logging/tracing framework (no `log` crate).
- Use `tracing::info!`, `debug!`, `warn!`, `error!` for log output.
- Spans: existing code uses manual `let span = info_span!(...); let _guard =
  span.enter();`. New code may use `#[tracing::instrument]` where appropriate.
- OpenTelemetry export is behind the `otel` feature flag in `fluree-db-server`.
- See `fluree-db-server/src/telemetry.rs` for all configuration options.
- See `dev-docs/deep-tracing-design.md` for the deep tracing design effort.

## Testing

### Test Organization

- **Unit tests:** `#[cfg(test)] mod tests { ... }` inside source files.
- **Integration tests:** `crate/tests/*.rs` files with `#[tokio::test]`.
- **Ignored (slow) tests:** `#[ignore]` — run with `cargo test -- --ignored`.
- **AWS tests (opt-in):** Require Docker + LocalStack:
  ```bash
  cargo test -p fluree-db-connection --features aws-testcontainers \
    --test aws_testcontainers_test -- --nocapture
  ```

### Test Conventions

- Names: `test_<what_is_being_tested>` (descriptive, snake_case).
- Pattern: Arrange-Act-Assert.
- Async: always `#[tokio::test]`, never block on futures.
- Server integration tests use `tower::ServiceExt` to test the axum router
  in-process without binding a port.
- Test helpers: `test_state()`, `json_body()`, `json_contains_string()` in
  `fluree-db-server/tests/`.

### Running Tests

```bash
cargo test --workspace                    # All tests
cargo test -p fluree-db-query             # One crate
cargo test -p fluree-db-query test_name   # One test
cargo test --workspace -- --nocapture     # With stdout
cargo nextest run                         # Faster parallel runner
```

## Code Quality Rules

### Before Every Commit

1. `cargo fmt --all -- --check` — zero diff.
2. `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings.
3. `cargo test --workspace` — all pass.

### Unused Code

- **No unused variables.** If intentionally unused for a future phase, prefix
  with `_` and add a comment: `// Phase N: <reason>`.
- **No unused imports.** Remove them. If staged intentionally,
  `#[allow(unused_imports)]` with a comment.
- **Before writing new code, check for existing unused vars/signatures** in the
  files you are about to modify. The codebase has placeholder comments
  (`// TODO`, `// future`, `// Phase`) that may be relevant to your task.
- **No dead code warnings.** Use `#[allow(dead_code)]` only with a comment
  explaining why (e.g., "used by integration tests via `pub(crate)`").

### Style

- Rust 2021 edition. Standard `rustfmt` defaults (no `rustfmt.toml`).
- Standard `clippy` defaults (no `clippy.toml`).
- Types: `PascalCase`. Functions/methods: `snake_case`. Constants:
  `SCREAMING_SNAKE_CASE`. Modules: `snake_case`.
- Prefer `Arc<str>` over `String` for shared immutable strings.
- Box large enum variants to keep enum size small.
- Document public APIs with `///` doc comments. Module-level docs with `//!`.
- New public functions need doc comments. Internal functions need a comment only
  if the logic isn't self-evident.

### Feature Flags

- `native` — native runtime (tokio, FileStorage). On by default.
- `credential` — JWS/VC verification. On by default.
- `otel` — OpenTelemetry OTLP export. Off by default (binary size).
- `shacl` — SHACL validation engine. Feature-gated in transact errors.
- `aws-testcontainers` — Docker-based S3/DynamoDB tests. Test-only.

When adding `#[cfg(feature = "...")]` code, ensure the non-feature path still
compiles: `cargo check --workspace --no-default-features` (where applicable).

## Git & PR Conventions

### Branches

```
feature/<name>     # New feature
fix/<name>         # Bug fix
docs/<name>        # Documentation
refactor/<name>    # Refactoring
test/<name>        # Test additions
```

### Commit Messages

```
Short summary (50 chars or less)

More detail if needed. Wrap at 72 chars.

- Key change 1
- Key change 2

Fixes #123
```

### PR Title Format

`category: short description`

Examples: `feat: Add SPARQL property paths`, `fix: Correct txn time ordering`,
`test: Add time travel integration tests`.

### PR Checklist

- [ ] Code follows style guidelines
- [ ] Tests added/updated for new behavior
- [ ] Documentation updated if public API changed
- [ ] All tests pass (`cargo test --workspace`)
- [ ] No clippy warnings
- [ ] Commit messages are clear
- [ ] PR description includes Summary, Motivation, Changes, Testing

## Documentation

### Where Docs Live

```
docs/
  getting-started/    # Quickstart guides
  concepts/           # Ledgers, IRIs, datatypes, time travel, datasets
  api/                # HTTP endpoint reference
  query/              # JSON-LD Query, SPARQL, CONSTRUCT, explain, tracking
  transactions/       # Insert, upsert, update, retractions, Turtle ingest
  security/           # Policy model, encryption
  indexing-and-search/# Background indexing, BM25, vector search
  virtual-graphs/     # Iceberg, R2RML, BM25 virtual graph
  operations/         # Configuration, storage, telemetry, admin
  troubleshooting/    # Common errors, debugging queries
  reference/          # Glossary, crate map, compatibility
  contributing/       # Dev setup, tests, design notes
  design/             # Architecture design docs (storage traits, etc.)
  SUMMARY.md          # Table of contents (mdbook format)

dev-docs/             # Internal design specs for active work
```

### Design Documents

For major features or significant changes, write a design doc. See
`docs/contributing/design-notes.md` for the template and index of existing
design docs. Place architecture docs in `docs/design/`, implementation plans at
repo root or in `dev-docs/`.

## Common Development Tasks

### Adding a New Query Feature

1. Parse: add to `fluree-db-query/src/parse/` (FQL) or
   `fluree-db-sparql/src/` (SPARQL).
2. IR: extend types in `fluree-db-query/src/ir.rs` if needed.
3. Operator: implement `Operator<S, C>` trait in `fluree-db-query/src/`.
4. Wire up: integrate in `fluree-db-query/src/execute/` pipeline.
5. API surface: expose via `fluree-db-api/src/query/builder.rs`.
6. HTTP: add/modify handler in `fluree-db-server/src/routes/query.rs` if needed.
7. Tests: unit tests in the operator file, integration tests in
   `fluree-db-query/tests/`, HTTP tests in `fluree-db-server/tests/`.

### Adding a New Transaction Feature

1. Parse: extend `fluree-db-transact/src/parse/`.
2. Stage: modify `fluree-db-transact/src/stage.rs`.
3. Commit: modify `fluree-db-transact/src/commit.rs` if needed.
4. API: expose via `fluree-db-api/src/tx_builder.rs`.
5. HTTP: modify `fluree-db-server/src/routes/transact.rs` if needed.
6. Tests: unit + integration in transact crate, HTTP tests in server crate.

### Adding a Storage Backend

1. Implement `StorageRead` + `StorageWrite` traits from
   `fluree-db-core/src/storage.rs`.
2. Optionally implement `ContentAddressedWrite` for the `Storage` marker trait.
3. Wire into `fluree-db-connection` for builder integration.
4. Feature-gate if it pulls in heavy dependencies.

## Useful Reference Files

| What | Path |
|------|------|
| Workspace config | `Cargo.toml` (root) |
| Crate map | `docs/reference/crate-map.md` |
| Telemetry config | `fluree-db-server/src/telemetry.rs` |
| Server config (all CLI args/env vars) | `fluree-db-server/src/config.rs` |
| HTTP routes | `fluree-db-server/src/routes/mod.rs` |
| Error types (API) | `fluree-db-api/src/error.rs` |
| Error types (server) | `fluree-db-server/src/error.rs` |
| Core types (Flake, Sid, etc.) | `fluree-db-core/src/` |
| Query operator trait | `fluree-db-query/src/operator.rs` |
| Query execution pipeline | `fluree-db-query/src/execute/mod.rs` |
| Transaction staging | `fluree-db-transact/src/stage.rs` |
| Transaction commit | `fluree-db-transact/src/commit.rs` |
| Storage traits | `fluree-db-core/src/storage.rs` |
| Tracker (time/fuel/policy) | `fluree-db-core/src/tracking.rs` |
| Contributing guide | `docs/contributing/README.md` |
| Test guide | `docs/contributing/tests.md` |
| Dev setup | `docs/contributing/dev-setup.md` |
| Design notes index | `docs/contributing/design-notes.md` |
| Deep tracing design | `dev-docs/deep-tracing-design.md` |
