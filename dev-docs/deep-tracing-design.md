# Deep Performance Tracing Design

## 1. Problem Statement & Objectives

### Problem

The Fluree DB server (`fluree-server`) has foundational tracing infrastructure
(`tracing` crate across 16 crates, optional OpenTelemetry export via the `otel`
feature flag) but the instrumentation is **surface-level only**. An operator
running a slow query today sees:

```
INFO request{operation="query" ledger_alias="my/ledger"}: query completed status="success" time="834.21ms"
```

There is no breakdown of *where* time was spent. The ~17 existing spans cover
HTTP entry points and top-level operation names but none of the internal phases
that dominate wall-clock time:

| Phase | Has span today? | Typical cost |
|-------|-----------------|--------------|
| JSON-LD / SPARQL parsing | No | Low (< 5 ms) but spikes on large payloads |
| Reasoning preparation (RDFS/OWL) | No | Moderate (10-100 ms with large schemas) |
| Pattern rewriting | No | Low-moderate |
| Query planning / selectivity estimation | No | Low-moderate |
| Operator execution (scan, join, filter, sort) | No | **Dominant** |
| Range scans against the index | No | **Dominant** |
| Result formatting / graph crawl | No | Moderate-high for deep crawls |
| Transaction staging (WHERE + flake generation) | Partial (one coarse span) | Moderate-high |
| Commit (content-addressing, storage write, nameservice publish) | No | I/O bound |
| Policy evaluation | No | Moderate with complex policies |

The existing `Tracker` system (`fluree-db-core/src/tracking.rs`) collects
wall-clock time and fuel counts but stores them in a separate `Instant::now()`
pipeline that does **not** flow into tracing spans or OTEL exports. This means
Jaeger/Tempo/Grafana users never see those metrics.

### Objectives

1. **On-demand waterfall visibility.** When an operator sets
   `RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug`, every query
   and transaction produces a full span waterfall in Jaeger/Tempo showing
   exactly where time was spent and how much data flowed through each phase.

2. **Zero noise by default.** At `RUST_LOG=info` (the default), no additional
   log lines or spans appear beyond what exists today. New spans use
   `debug_span!` or `trace_span!` and are short-circuited by the `tracing`
   crate's callsite filtering when disabled.

3. **Bridge Tracker into spans.** The existing time/fuel/policy metrics from
   `Tracker` are recorded as span fields on the parent operation span so they
   appear in OTEL exports without duplicating the measurement system.

4. **Structured, queryable fields.** Every span carries typed fields
   (cardinalities, byte counts, flake counts, operator names) so users can
   filter and aggregate in their observability backend.

5. **No runtime recompile for detail.** All internal `debug_span!` /
   `trace_span!` calls are compiled unconditionally (they are zero-cost when
   filtered out). The `otel` feature flag remains only for the OTLP exporter
   dependency. Operators toggle detail via `RUST_LOG` and optionally
   `OTEL_TRACES_SAMPLER`.

6. **Test coverage for every new span.** Each instrumented phase has at least
   one test verifying the span is emitted with expected fields when the
   appropriate tracing level is active.

---

## 2. Design Details

### 2.1 Two-Tier Span Strategy

```
Tier 1 (info_span! - always visible):
  request --> query_execute | transact_execute | sparql_execute
  (these already exist today and remain unchanged)

Tier 2 (debug_span! - opt-in detail):
  query_execute
    |-- parse            (input_format, input_bytes)
    |-- reasoning_prep   (rdfs, owl2ql, owl2rl)
    |-- pattern_rewrite  (patterns_before, patterns_after)
    |-- plan             (pattern_count, estimated_cardinality)
    |-- execute          (operator_count)
    |   |-- scan         [trace_span!] (index, range_desc, flakes_scanned)
    |   |-- join         [trace_span!] (left_rows, right_rows, output_rows)
    |   |-- filter       [trace_span!] (input_rows, output_rows)
    |   |-- sort         [trace_span!] (rows, sort_keys)
    |   |-- group_by     [trace_span!] (groups)
    |   |-- aggregate    [trace_span!] (function)
    |   '-- distinct     [trace_span!] (input_rows, output_rows)
    |-- policy_eval      (rules_checked, rows_filtered)
    '-- format           (output_format, result_count)

  transact_execute
    |-- ledger_load      (ledger_alias)
    |-- parse            (input_format, template_count)
    |-- stage
    |   |-- where_exec   (pattern_count, binding_rows)
    |   |-- delete_gen   (retraction_count)
    |   |-- insert_gen   (assertion_count)
    |   |-- cancellation (cancelled_count)
    |   '-- policy_enforce (flakes_checked, flakes_allowed)
    |-- commit
    |   |-- verify_seq
    |   |-- raw_txn_write
    |   |-- storage_write (bytes_written)
    |   |-- ns_publish
    |   '-- state_build
    |       |-- novelty_clone
    |       '-- novelty_apply  (flake_count)
    |           |-- merge_spot   [trace_span!]
    |           |-- merge_psot   [trace_span!]
    |           |-- merge_post   [trace_span!]
    |           |-- merge_opst   [trace_span!]
    |           '-- merge_tspo   [trace_span!]
    '-- index_status     (novelty_bytes, needs_reindex)

  index_build            (ledger_alias)
    |-- ns_lookup
    |-- [incremental path]
    |   |-- novelty_build  (commit_count, novelty_flakes)
    |   '-- refresh_spot, refresh_psot, refresh_post, refresh_opst, refresh_tspo (concurrent)
    |       |-- tree_walk   (nodes_visited, leaves_modified, branches_modified, nodes_written, nodes_reused)
    |       |   |-- node_load*   [debug_span!] per-node storage I/O
    |       |   '-- leaf_merge*  [debug_span!] merge + dedup + split per leaf
    |       |-- finalize_root
    |       |-- gc_collect
    |       '-- root_write
    '-- [rebuild path]
        '-- commit_scan
            reindex_batch (batch_num, commits_in_batch, novelty_bytes)
```

**Tier 3 (trace_span! - maximum detail):** Individual operator `next_batch()`
calls, per-scan range bounds, individual flake generation. These exist at
`trace` level and are almost never enabled in production.

### 2.2 Span Naming Conventions

- All span names use `snake_case` (matches existing convention).
- Span names are **stable identifiers** (treated as part of the public
  telemetry contract). Renaming a span is a breaking change for dashboards.
- Fields use `snake_case` and are typed: `u64` for counts/sizes, `&str` for
  identifiers, `bool` for flags.

### 2.3 Instrumentation Approach

Use `#[tracing::instrument]` for new spans where feasible. This provides:
- Automatic span entry/exit (no manual `_guard` boilerplate).
- Automatic duration recording (critical for OTEL export).
- Compile-time `level` filtering.

```rust
#[tracing::instrument(
    level = "debug",
    name = "plan",
    skip_all,
    fields(pattern_count, estimated_cardinality)
)]
fn plan_query(stats: &StatsView, patterns: &[Pattern]) -> OperatorTree {
    let pattern_count = patterns.len();
    tracing::Span::current().record("pattern_count", pattern_count);
    // ...
    tracing::Span::current().record("estimated_cardinality", est);
    // ...
}
```

For hot-loop operators (`scan`, `join`, etc.) where the overhead of attribute
macros on every `next_batch()` call could be measurable, use `trace_span!` with
a manual guard inside the loop body only when collecting per-batch granularity.
For the coarser operator-lifetime span, `#[instrument]` on `open()` is
appropriate.

**Existing spans are left as-is.** The manual `let span = info_span!(...); let
_guard = span.enter();` pattern in routes and top-level executors is not
refactored to `#[instrument]` to keep the diff minimal and avoid churn in
reviewed code.

### 2.4 Bridging Tracker into Spans

The `Tracker` (`fluree-db-core/src/tracking.rs`) already collects:
- `time`: `Option<Instant>` -> formatted as `"12.34ms"`
- `fuel`: `AtomicU64` total consumed
- `policy`: `HashMap<String, PolicyStats>`

After execution completes, the caller records these onto the current span:

```rust
if let Some(tally) = tracker.tally() {
    let span = tracing::Span::current();
    if let Some(ref time) = tally.time {
        span.record("tracker_time", time.as_str());
    }
    if let Some(fuel) = tally.fuel {
        span.record("tracker_fuel", fuel);
    }
}
```

This requires the parent span to declare these fields as `Empty` at creation:

```rust
let span = tracing::info_span!(
    "query_execute",
    ledger_alias = alias,
    tracker_time = tracing::field::Empty,
    tracker_fuel = tracing::field::Empty,
);
```

**No changes to `Tracker` internals.** The bridge is purely at the call site in
the route handlers (`routes/query.rs`, `routes/transact.rs`).

### 2.5 Runtime Activation

```bash
# Default: quiet, production-safe
RUST_LOG=info

# Investigation mode: full query/transaction waterfall
RUST_LOG=info,fluree_db_query=debug,fluree_db_transact=debug

# Maximum detail: per-batch operator traces
RUST_LOG=info,fluree_db_query=trace,fluree_db_transact=trace,fluree_db_core=trace

# OTEL export to Jaeger/Tempo (requires --features otel build)
OTEL_SERVICE_NAME=fluree \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_TRACES_SAMPLER=always_on \
RUST_LOG=info,fluree_db_query=debug \
  ./fluree-server
```

### 2.6 Future: Dynamic Level Reload

The current `init_logging` in `telemetry.rs` uses a static `EnvFilter`. A
future enhancement (out of scope for this effort) could wrap it in
`tracing_subscriber::reload::Layer` to allow runtime level changes via an admin
endpoint or signal handler, avoiding restarts entirely.

---

## 3. Implementation Details & Source Reference

### 3.1 Files to Modify (Query Path)

| File | Lines | Change |
|------|-------|--------|
| `fluree-db-server/src/routes/query.rs` | 295-360 | Add `tracker_time`, `tracker_fuel` fields to existing `query_execute` info_span; record Tracker tally after execution. |
| `fluree-db-query/src/execute/runner.rs` | 90-229 | Wrap `prepare_execution` sub-phases in `debug_span!`: `reasoning_prep`, `pattern_rewrite`, `build_operator_tree`. The existing `query_prepare` span (line 96) becomes the parent. |
| `fluree-db-query/src/execute/runner.rs` | 235-262 | Add per-operator `trace_span!` inside the batch drain loop with batch count fields. |
| `fluree-db-query/src/execute/mod.rs` | 93-103 | Add `debug_span!("plan")` around the planning phase. Existing `query_execute` debug_span becomes parent for reasoning/rewrite/plan/execute children. |
| `fluree-db-query/src/parse/mod.rs` | 87-93 | Wrap `parse_query_ast` in `debug_span!("parse", input_format = "fql", input_bytes)`. |
| `fluree-db-sparql/src/lower/mod.rs` | 88 | Existing `sparql_lower` span is adequate; add `input_bytes` field. |
| `fluree-db-query/src/scan.rs` | `open()` impl | Add `trace_span!("scan", index, range_desc)` with `flakes_scanned` recorded on close. |
| `fluree-db-query/src/join.rs` | `open()` impl | Add `trace_span!("join")` with `left_rows`, `right_rows`, `output_rows`. |
| `fluree-db-query/src/filter.rs` | `open()` / `next_batch()` | Add `trace_span!("filter")` with `input_rows`, `output_rows`. |
| `fluree-db-query/src/sort.rs` | `open()` impl | Add `trace_span!("sort", rows, sort_keys)`. |

### 3.2 Files to Modify (Transaction Path)

| File | Lines | Change |
|------|-------|--------|
| `fluree-db-server/src/routes/transact.rs` | 544 | Add `tracker_time`, `tracker_fuel` fields to existing `transact_execute` info_span. |
| `fluree-db-transact/src/stage.rs` | 122-234 | Wrap sub-phases in `debug_span!`: `where_exec` (line 151), `delete_gen` (line 160), `insert_gen` (line 180), `cancellation` (line 201), `policy_enforce` (line 216). The existing `txn_stage` info_span (line 128) becomes parent. |
| `fluree-db-transact/src/commit.rs` | 118-279 | Add `debug_span!("commit")` as parent; children: `content_addr` (trace_span around SHA-256), `storage_write` (debug_span around storage.write_commit), `ns_publish` (debug_span around nameservice publish). |
| `fluree-db-transact/src/parse/mod.rs` | entry fn | Add `debug_span!("parse", input_format, template_count)`. |

### 3.3 Files to Modify (Tracker Bridge)

| File | Lines | Change |
|------|-------|--------|
| `fluree-db-server/src/routes/query.rs` | after execute | Record `tracker.tally()` fields onto current span. |
| `fluree-db-server/src/routes/transact.rs` | after commit | Record `tracker.tally()` fields onto current span. |
| `fluree-db-core/src/tracking.rs` | `TrackingTally` | Add `pub time_ms: Option<f64>` field (numeric, for span recording) alongside existing formatted string. |

### 3.4 Files to Add

| File | Purpose |
|------|---------|
| `fluree-db-query/tests/tracing_tests.rs` | Integration tests verifying query spans are emitted at debug level with correct fields. Uses `tracing-test` or a `tracing_subscriber::fmt::TestWriter` to capture spans. |
| `fluree-db-transact/tests/tracing_tests.rs` | Same for transaction path spans. |
| `fluree-db-server/tests/tracing_integration_test.rs` | End-to-end test: HTTP request with `RUST_LOG=debug` verifying span hierarchy in captured output. |

### 3.5 Dependencies to Add

| Crate | Dependency | Scope | Reason |
|-------|-----------|-------|--------|
| `fluree-db-query` | `tracing` | Already present | No change needed. |
| `fluree-db-transact` | `tracing` | Already present | No change needed. |
| `fluree-db-query` | `tracing-test` or `tracing-subscriber` (test-util) | dev-dependencies | Span capture in tests. |
| `fluree-db-transact` | `tracing-test` or `tracing-subscriber` (test-util) | dev-dependencies | Span capture in tests. |

### 3.6 Existing Artifacts to Preserve

- **All existing spans remain unchanged** in name, level, and fields. They are
  the public telemetry contract.
- **`Tracker` internals remain unchanged.** Only the `TrackingTally` struct gets
  one new field.
- **`telemetry.rs` public API unchanged.** No modifications to `init_logging`,
  `create_request_span`, or OTEL setup.
- **No feature flag changes.** New spans are unconditional; the `otel` flag
  scope is unchanged.

### 3.7 Key Source File Quick Reference

| Artifact | Path | Key Lines |
|----------|------|-----------|
| Telemetry config & init | `fluree-db-server/src/telemetry.rs` | 10-38 (config), 144-183 (init), 326-342 (request span) |
| Query route handler | `fluree-db-server/src/routes/query.rs` | 117 (entry), 295-360 (execute_query) |
| Transaction route handler | `fluree-db-server/src/routes/transact.rs` | 97 (entry), 544 (execute span) |
| Query execution engine | `fluree-db-query/src/execute/mod.rs` | 93-103 (execute fn), phase pipeline |
| Query runner (prepare + run) | `fluree-db-query/src/execute/runner.rs` | 90-229 (prepare), 235-262 (run) |
| Query parser | `fluree-db-query/src/parse/mod.rs` | 87-93 (parse_query_ast) |
| Transaction staging | `fluree-db-transact/src/stage.rs` | 122-234 (stage fn), 128 (txn_stage span) |
| Transaction commit | `fluree-db-transact/src/commit.rs` | 118-279 (commit fn) |
| Tracker | `fluree-db-core/src/tracking.rs` | 114-226 (Tracker), 228-240 (TrackingTally) |
| Scan operator | `fluree-db-query/src/scan.rs` | Operator impl |
| Join operator | `fluree-db-query/src/join.rs` | Operator impl |
| Filter operator | `fluree-db-query/src/filter.rs` | Operator impl |
| Sort operator | `fluree-db-query/src/sort.rs` | Operator impl |

---

## 4. Acceptance Criteria & Guidelines

### 4.1 Acceptance Criteria

**AC-1: Query waterfall.** Running with
`RUST_LOG=info,fluree_db_query=debug` and a SPARQL or FQL query produces a span
tree with at least: `request -> query_execute -> parse, reasoning_prep,
plan, execute, format`. Each child span has a measurable duration and at
least one typed field.

**AC-2: Transaction waterfall.** Running with
`RUST_LOG=info,fluree_db_transact=debug` and a transaction produces:
`request -> transact_execute -> parse, stage -> [where_exec, delete_gen,
insert_gen, cancellation], commit -> [storage_write, ns_publish]`. Each
child span has a duration and at least one typed field.

**AC-3: Operator-level detail.** Running with
`RUST_LOG=info,fluree_db_query=trace` adds per-operator spans (`scan`, `join`,
`filter`, `sort`, `group_by`, `aggregate`, `distinct`) as children of
`execute`, each with row/flake counts.

**AC-4: Tracker bridge.** The `query_execute` and `transact_execute` info-level
spans include `tracker_time` and `tracker_fuel` fields populated from the
`Tracker` tally when tracking is enabled.

**AC-5: Zero default noise.** At `RUST_LOG=info`, the log output and span count
are identical to the baseline (before this work). Verified by diffing log output
of a reference query at info level before and after.

**AC-6: OTEL export.** With `--features otel` and a running Jaeger/OTLP
collector, the full waterfall is visible in the Jaeger UI. Manual verification
(screenshot in PR description).

**AC-7: Tests.** Each crate with new spans has at least one test that:
- Initializes a test tracing subscriber.
- Executes the instrumented code path.
- Asserts the expected span names and key fields are present.

**AC-8: No regressions.** `cargo test --workspace` passes. No new warnings from
`cargo clippy --workspace`. `cargo fmt --all -- --check` passes.

### 4.2 Rust Development Guidelines

These apply to all code written in this effort and should be verified before
every commit.

#### Checks to Run

```bash
# Format check (must pass with zero diff)
cargo fmt --all -- --check

# Clippy (must pass with zero warnings; treat warnings as errors)
cargo clippy --workspace --all-targets -- -D warnings

# Full workspace test suite
cargo test --workspace

# If modifying fluree-db-server with otel feature:
cargo clippy -p fluree-db-server --features otel -- -D warnings
cargo test -p fluree-db-server --features otel
```

#### Code Style

- **No unused variables.** If a variable is intentionally unused for a future
  phase, prefix with `_` and add a `// Phase N: <reason>` comment explaining
  which phase will use it.
- **No unused imports.** Remove them or `#[allow(unused_imports)]` with a
  comment if intentionally staged.
- **Check for existing unused vars/signatures before writing new code.** The
  codebase may contain placeholders intended for this work. Search for
  `// TODO`, `// future`, `// placeholder` in files you are about to modify.
  Notable known items:
  - `fluree-db-query/src/graph.rs` has a comment about tracing::debug being
    removed; this may be a candidate for re-enabling.
  - `fluree-db-query/src/planner.rs` has future-optimization comments around
    stats-based planning that may relate to new span fields.
- **Use `#[tracing::instrument]` for new spans** where the function has a clean
  signature (not too many parameters to skip). Use manual `debug_span!` /
  `trace_span!` for hot paths or where `skip_all` would lose useful context.
- **Prefer `skip_all` + explicit `fields()`** over letting `#[instrument]`
  auto-capture parameters. This avoids accidentally logging large structs and
  keeps span payloads intentional.
- **Field types:** Use `u64` for counts, `&str` for identifiers, `bool` for
  flags. Avoid `Debug` formatting (`?field`) in span fields; prefer explicit
  string conversion.

#### Testing Requirements

- **Unit tests** for any new helper functions (e.g., tally bridge logic).
- **Integration tests** in `tests/` directories for span emission verification.
  Use `tracing_subscriber::fmt::TestWriter` or the `tracing-test` crate to
  capture span output in tests.
- **Existing test suites must pass.** Run `cargo test --workspace` before and
  after changes. If a test fails due to new tracing output appearing in stderr
  captures, adjust the test rather than suppressing the tracing.
- **End-of-session check.** Before marking any task complete, always run:
  ```bash
  cargo fmt --all -- --check && \
  cargo clippy --workspace --all-targets -- -D warnings && \
  cargo test --workspace
  ```

#### Documentation Updates

- Update `dev-docs/deep-tracing-task-breakdown.md` with status and progress
  after completing each task.
- If implementation reveals a flaw in this design (e.g., a span placement
  causes unexpected overhead, a function signature doesn't allow clean
  instrumentation), update this design document with a **Design Revision** note
  at the bottom explaining what changed and why.

---

## Design Revision Log

### Rev 1: OTEL SDK 0.31 API migration

The `telemetry.rs` OTEL setup code used the pre-0.27 OpenTelemetry SDK API
which was incompatible with the pinned 0.31 dependency versions. The code was
updated to use the current API:

- `opentelemetry_otlp::new_exporter()` replaced by
  `SpanExporter::builder().with_tonic()` / `.with_http()`.
- `opentelemetry_sdk::trace::TracerProvider` renamed to `SdkTracerProvider`.
- `Resource::new(vec![...])` replaced by `Resource::builder()` fluent API.
- Batch export no longer requires `runtime::Tokio` (SDK 0.31 uses a background
  thread).
- `opentelemetry::global::shutdown_tracer_provider()` removed; replaced by
  calling `provider.shutdown()` on a `SdkTracerProvider` stored in a module-level
  `OnceLock`.
- `init_otel_layer` made generic over subscriber type `S` to fix layer type
  mismatch when composing with the boxed fmt layer.
- Added `grpc-tonic` feature to `opentelemetry-otlp` dependency for gRPC
  transport support.

This revision affects only `fluree-db-server/src/telemetry.rs` and
`fluree-db-server/Cargo.toml`. No span instrumentation code was changed.

### Rev 2: Async span propagation fix

Replaced `span.enter()` with `.instrument(span)` wherever the guard's scope
contained an `.await` point. The `Entered` guard from `.enter()` is `!Send` and
does not survive `.await` — when the async runtime suspends, the span context is
lost and child spans become orphaned root traces in Jaeger. This affected all
files instrumented in Phases 1-5 (stage.rs, commit.rs, runner.rs,
execute/mod.rs, format/mod.rs, transact.rs, query.rs, ledger.rs, admin.rs).

### Rev 3: Expanded commit, transaction, and indexer child spans

Added child spans to close visibility gaps identified in production traces:

**Transaction path:**
- `ledger_load` — wraps `ledger_cached().await` in `transact.rs` (both FQL and
  SPARQL UPDATE paths). Accounts for the dominant wall-clock gap before `parse`.
- `verify_seq` — wraps nameservice lookup + sequencing check in `commit.rs`.
- `raw_txn_write` — wraps raw transaction JSON storage write in `commit.rs`.
- `state_build` — wraps commit metadata flake generation + novelty merge in
  `commit.rs` (sync block, uses `span.enter()`).

**Indexer path (previously out of scope, now instrumented):**
- `ns_lookup` — wraps nameservice lookup in `build_index_for_ledger`.
- `commit_scan` — wraps Phase 1 backward metadata stream in `batched_rebuild`.
- `refresh_spot`, `refresh_psot`, `refresh_post`, `refresh_opst`, `refresh_tspo`
  — per-index-type spans on the 5 concurrent futures in `refresh_index_with_prev`.
- `gc_collect` — wraps garbage address collection + storage write.
- `root_write` — wraps DbRoot serialization + storage write.

### Rev 4: OTEL export filter and tower-http span level

- Added a `tracing_subscriber::filter::Targets` filter on the OTEL layer that
  only exports spans from `fluree_*` crates. Prevents debug noise from hyper,
  h2, tokio, tower-http from flooding the batch exporter when `RUST_LOG=debug`.
- Raised tower-http's `TraceLayer` per-request span from DEBUG to TRACE level
  via `DefaultMakeSpan::new().level(tracing::Level::TRACE)`. This prevents a
  duplicate `request` span from appearing at `RUST_LOG=debug` that would collide
  with the handler-level `request` info_span.
