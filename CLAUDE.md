# CLAUDE.md — Fluree DB-R

## Project Overview

Fluree DB-R is a 37-crate Rust workspace implementing a semantic, immutable, time-travel-capable graph database with W3C standards compliance (RDF, JSON-LD, SPARQL). The core is runtime-agnostic; storage and networking are feature-gated.

## Documentation — Use It

The `docs/` directory is the canonical source of truth. **Before inventing an approach, check if docs/ already covers it.** Key sections:

| Need to understand...              | Read                                        |
|------------------------------------|---------------------------------------------|
| Crate dependency layers            | `docs/reference/crate-map.md`               |
| Storage/nameservice trait design   | `docs/design/storage-traits.md`             |
| Index format internals             | `docs/design/index-format.md`               |
| Nameservice v2 schema              | `docs/design/nameservice-v2.md`             |
| How to add a query feature         | `docs/contributing/dev-setup.md`            |
| Test organization & patterns       | `docs/contributing/tests.md`                |
| PR workflow & code style           | `docs/contributing/README.md`               |
| HTTP API endpoints                 | `docs/api/endpoints.md`                     |
| Configuration & env vars           | `docs/operations/configuration.md`          |
| Full table of contents             | `docs/SUMMARY.md`                           |

**Docs maintenance mandate:** If your work changes or extends behavior covered by `docs/`, you MUST update the relevant doc files in the same changeset. Conversely, if you produce significant new documentation, update `docs/SUMMARY.md` and add a pointer here if it represents a new top-level concern.

## Workspace Architecture (concise)

```
Layer 5  fluree-db-server (HTTP binary), fluree-db-ingest (bulk import CLI), fluree-db-cli
Layer 4  fluree-db-api (orchestration, builder, import pipeline)
Layer 3  fluree-db-query, fluree-db-sparql, fluree-db-transact, fluree-db-policy
Layer 2  fluree-db-ledger, fluree-db-indexer, fluree-db-novelty, fluree-db-connection
Layer 1  fluree-db-core (runtime-agnostic), fluree-db-nameservice, fluree-db-crypto
Layer 0  fluree-vocab, fluree-graph-ir, fluree-graph-json-ld, fluree-graph-turtle, fluree-sse
```

Generic over `S: Storage + 'static` and `N: NameService`. See `docs/reference/crate-map.md` for full details.

## Feature Flags

| Flag | What it enables | Key crates |
|------|-----------------|------------|
| `native` (default for api/server) | FileStorage, filesystem nameservice, moka cache | core, api, connection, nameservice |
| `aws` | S3 + DynamoDB storage backends | api, connection, storage-aws, iceberg |
| `credential` | JWS / VerifiableCredential verification | api, server, novelty |
| `shacl` | SHACL constraint validation | api, transact |
| `iceberg` | Iceberg REST catalog virtual graphs | api |
| `vector` | Embedded HNSW vector search (usearch) | api, query |
| `import` | Turtle bulk import support | transact |
| `otel` | OpenTelemetry tracing export | server, ingest |
| `aws-testcontainers` | LocalStack Docker integration tests | api, connection |

**CI runs:** `cargo fmt --all -- --check`, `cargo clippy --all --all-features --all-targets`, and `cargo nextest run --workspace --all-features --no-fail-fast`.

## Build & Validation Commands

Use situationally — match scope to the work you actually changed:

```bash
# Check — fast compile verification (no codegen)
cargo check -p <crate>                          # single crate, default features
cargo check -p <crate> --all-features           # single crate, all features
cargo check --workspace --all-features --all-targets  # full workspace (CI parity)

# Format — always all
cargo fmt --all

# Clippy — match scope to changes
cargo clippy -p <crate> -- -D warnings                        # single crate
cargo clippy -p <crate> --all-features -- -D warnings          # single crate, all features
cargo clippy --all --all-features --all-targets -- -D warnings  # full workspace (CI parity)
# Use --fix --allow-dirty to auto-fix when appropriate

# Test — match scope to changes
cargo test -p <crate>                             # single crate
cargo test -p <crate> --all-features              # single crate, all features
cargo nextest run --workspace --all-features --no-fail-fast  # full workspace (CI parity)
```

**When to use which scope:**
- Editing one crate → check/clippy/test that crate (and its `--all-features` if you touched feature-gated code)
- Ready to commit → `cargo fmt --all` + clippy/test on affected crates
- Ready for PR → full workspace CI-parity commands above

## Session Start: Dead Code Audit

**At the start of every session**, scan for `#[allow(dead_code)]` and `#[expect(dead_code)]` annotations:

```bash
# Find all dead_code annotations with surrounding context
grep -rn '#\[allow(dead_code)\]\|#\[expect(dead_code)\]' --include='*.rs' .
```

Read the comments near each annotation. These are signals from past sessions:
- They may contain tools, utilities, or types **you should use** in this session rather than reinventing
- They document *why* the code was kept and *when* it should be activated
- If your current task matches the "when to use" criteria, **use that code now** and remove the annotation

## Dead Code Policy

**NEVER** suppress dead/unused warnings by prefixing with `_` (e.g., `_unused_var`, `_helper_fn`). This hides code from future discovery and leads to permanent rot.

When `cargo check` or `cargo clippy` reports dead/unused code, ask these questions **in order**:

### 1. Should this code be used NOW?
The warning may indicate you forgot to wire something up. This is the most common case during active development. **Complete the work** — do not end the session with genuinely incomplete wiring.

### 2. Should this code be kept for FUTURE use?
If the code is a useful building block that isn't needed yet:
- Annotate with `#[expect(dead_code)]` (NOT `#[allow(dead_code)]`)
- Add a comment block explaining:
  - **WHY** it was kept (what purpose it serves)
  - **WHEN** it should be used (what future task or condition activates it)
  - **HOW** to use it (brief usage hint if non-obvious)

```rust
// Kept for: streaming index merge during compaction (not yet implemented).
// Use when: background compaction is added to fluree-db-indexer.
// See: docs/design/index-format.md for the merge algorithm spec.
#[expect(dead_code)]
fn merge_index_segments(a: &IndexSegment, b: &IndexSegment) -> IndexSegment {
    // ...
}
```

`#[expect(dead_code)]` is preferred over `#[allow(dead_code)]` because it **warns when the annotation becomes unnecessary** — i.e., when someone actually uses the code, the compiler will remind them to clean up the annotation.

### 3. Should this code be REMOVED?
If the code is obsolete, redundant, or from an abandoned approach — delete it. Git history preserves it if we ever need it back.

**Existing `#[allow(dead_code)]` annotations:** When you encounter these during a session, evaluate whether they should be migrated to `#[expect(dead_code)]` with proper comments, or whether the code should be used or removed. Migrate opportunistically when you're already working in that file.

## Key Patterns to Follow

- **Error handling**: `thiserror` (not `anyhow`). Structured error enums per crate. See `fluree-db-core/src/error.rs`.
- **Async**: `#[async_trait]` on trait methods. Tokio runtime. Sync internals once data is loaded.
- **Builders**: `FlureeBuilder` pattern with fluent setters and async terminal `build()` methods.
- **Generics**: Functions generic over `S: Storage + 'static, N: NameService` — avoid `dyn` dispatch in hot paths.
- **Serialization**: `serde` + `serde_json` for structured data; `simd-json` in hot paths; `ciborium` (CBOR) for binary transport; `postcard` for compact index encoding.
- **Concurrency**: `parking_lot::RwLock` (not std). `OnceCell` for lazy-init caches. `Arc` for shared state.
- **Logging**: `tracing` crate. Use structured fields: `tracing::debug!(?value, "processing")`. Respect `LOG_SENSITIVE_DATA` config.
- **Tests**: Integration tests named `it_*.rs`. `#[tokio::test]` for async. Shared helpers in `fluree-db-api/tests/support/`. Clojure parity comments where applicable.
- **Dependencies**: Workspace-level dep declarations in root `Cargo.toml`. Feature-gate optional deps.

## Pre-existing Warnings

These warnings exist in the codebase and are NOT from your current work. Do not "fix" them unless specifically asked or unless you're working in that file:
- `fluree-db-indexer`: unused imports/functions (active development area)
- `fluree-db-transact`: unused import
- `fluree-db-server/src/telemetry.rs`: unused `Layer` import (conditional on `otel` feature)

If you introduce NEW warnings, you must resolve them per the dead code policy above.

## Branch Naming

- `feature/` — New features
- `fix/` — Bug fixes
- `docs/` — Documentation only
- `refactor/` — Code refactoring
- `test/` — Test additions
- `chore/` — Maintenance / tooling

## PR Format

Title: `category: short description` (e.g., `feat: Add SPARQL property paths support`)

CI must pass: clippy (all features, all targets) + nextest (workspace, all features).
