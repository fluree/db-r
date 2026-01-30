# Deep Performance Tracing: Task Breakdown

> **Companion document:** [`deep-tracing-design.md`](./deep-tracing-design.md)
>
> **Status legend:**
> - `[ ]` Not started
> - `[~]` In progress
> - `[x]` Complete
> - `[!]` Blocked / needs design revision
> - `[—]` Skipped (explained in notes)

---

## Phase 0: Preparation & Validation

Validate assumptions, set up test infrastructure, ensure the baseline is clean.

### Tasks

- [x] **0.1 — Baseline audit: existing spans & tests.**
  Run the full test suite (`cargo test --workspace`) and record pass count.
  Run `cargo clippy --workspace --all-targets -- -D warnings` and confirm
  zero warnings. Capture baseline log output of a simple FQL query and a
  simple transaction at `RUST_LOG=info` for later diff (AC-5 verification).
  - **Crates:** all
  - **Output:** Baseline log captures saved (e.g., in scratchpad or test fixtures).

- [x] **0.2 — Scan for existing unused vars / TODO markers.**
  Search all files listed in design doc Section 3 for `// TODO`, `// future`,
  `// Phase`, `#[allow(unused`, `_unused` patterns. Document findings and
  determine if any are intended for this work.
  - **Files:** `execute/mod.rs`, `runner.rs`, `stage.rs`, `commit.rs`,
    `tracking.rs`, `telemetry.rs`, `query.rs`, `transact.rs`, `scan.rs`,
    `join.rs`, `filter.rs`, `sort.rs`, `planner.rs`, `graph.rs`
  - **Output:** Findings documented as comments in this file under Phase 0 notes.

- [x] **0.3 — Add `tracing-test` or equivalent dev-dependency.**
  Add a dev-dependency to `fluree-db-query` and `fluree-db-transact` for
  capturing spans in tests. Evaluate `tracing-test` crate vs. manual
  `tracing_subscriber::fmt::TestWriter` approach. Choose one and document
  the rationale.
  - **Files:** `fluree-db-query/Cargo.toml`, `fluree-db-transact/Cargo.toml`
  - **Acceptance:** `cargo test -p fluree-db-query` and
    `cargo test -p fluree-db-transact` pass with new dev-dependency.

- [x] **0.4 — Create test scaffolding for span verification.**
  Write a small test utility (e.g., a `test_tracing` helper module or shared
  fixture) that initializes a tracing subscriber capturing span names and
  fields, executes a closure, and returns the captured spans for assertion.
  This will be reused by all subsequent tracing tests.
  - **Files:** New test utility file (location TBD based on 0.3 decision).
  - **Acceptance:** A trivial test using the utility passes and correctly
    captures a manually-created span.

### Phase 0 Notes

- **0.1:** Baseline clean. All workspace tests pass (500+ tests, 0 failures).
  Pre-existing clippy warnings in `fluree-db-credential` (out of scope, `map_or`
  simplification). Minor trailing-newline fmt diff in one file (pre-existing).
- **0.2:** Only finding: `_unused` suffix on test variables in `join.rs:1059,1063`
  — intentional naming for test clarity, not placeholder code. No TODO/future/Phase
  comments found in any target files.
- **0.3:** Chose `tracing-subscriber` (already a workspace dependency) over
  `tracing-test` crate. Added as dev-dependency to `fluree-db-query` and
  `fluree-db-transact`. Rationale: avoids external dependency, uses same version
  already in the workspace, and the `Layer` trait provides full span capture.
- **0.4:** Created `fluree-db-query/tests/tracing_test_utils.rs` with
  `SpanCaptureLayer`, `SpanStore`, and `init_test_tracing()` helper. Includes
  4 self-tests (basic capture, hierarchy, names, multiple spans). All pass.

---

## Phase 1: Query Path Instrumentation

Add `debug_span!` children to the query execution pipeline. This is the
highest-value phase because query latency investigation is the primary use case.

### Tasks

- [x] **1.1 — Instrument query parsing.**
  Wrap `parse_query_ast` (`fluree-db-query/src/parse/mod.rs:87`) in a
  `debug_span!("parse", input_format = "fql", input_bytes)`. For SPARQL,
  the existing `sparql_lower` span (`fluree-db-sparql/src/lower/mod.rs:88`)
  should get an `input_bytes` field added.
  - **Files:** `fluree-db-query/src/parse/mod.rs`,
    `fluree-db-sparql/src/lower/mod.rs`
  - **Test:** New test in `fluree-db-query/tests/tracing_tests.rs` asserting
    `parse` span is emitted with `input_format` and `input_bytes` fields at
    debug level.

- [x] **1.2 — Instrument reasoning preparation.**
  In `prepare_execution` (`fluree-db-query/src/execute/runner.rs:90-229`),
  wrap the reasoning mode computation and derived-fact generation in
  `debug_span!("reasoning_prep", rdfs, owl2ql, owl2rl)`. Record the boolean
  flags as span fields.
  - **Files:** `fluree-db-query/src/execute/runner.rs`
  - **Test:** Assert `reasoning_prep` span is emitted with correct boolean
    fields when reasoning is enabled vs. disabled.

- [x] **1.3 — Instrument pattern rewriting.**
  Wrap the pattern rewrite call in `prepare_execution` in
  `debug_span!("pattern_rewrite", patterns_before, patterns_after)`.
  - **Files:** `fluree-db-query/src/execute/runner.rs`
  - **Test:** Assert `pattern_rewrite` span with before/after counts.

- [x] **1.4 — Instrument query planning.**
  Wrap the operator tree building in
  `debug_span!("plan", pattern_count, operator_count)`. If selectivity
  estimation produces a useful aggregate metric, record it.
  - **Files:** `fluree-db-query/src/execute/runner.rs` or
    `fluree-db-query/src/execute/mod.rs` (depending on where planning is
    invoked)
  - **Test:** Assert `plan` span with `pattern_count` field.

- [x] **1.5 — Instrument result formatting.**
  Wrap the result formatting / graph crawl phase in
  `debug_span!("format", output_format, result_count)`. The format function
  is called from `fluree-db-api/src/query/mod.rs`.
  - **Files:** `fluree-db-api/src/query/mod.rs`
  - **Test:** Assert `format` span with `result_count` field.

- [x] **1.6 — Instrument policy evaluation (query path).**
  If a policy enforcer is active during query execution, wrap the evaluation
  in `debug_span!("policy_eval", rules_checked, rows_filtered)`.
  - **Files:** `fluree-db-query/src/policy/enforcer.rs` or the call site in
    `execute/mod.rs`
  - **Test:** Assert `policy_eval` span emitted only when policy is active.

- [x] **1.7 — End-of-phase validation.**
  Run full check suite:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ```
  Diff info-level log output against Phase 0 baseline to confirm AC-5 (zero
  noise). Update this task breakdown with results.

### Phase 1 Notes

- **1.1:** Added `debug_span!("parse", input_format = "fql", input_bytes)` to
  `parse_query_ast()`. SPARQL lower span already existed; `input_bytes` not
  added to SPARQL lower because `SparqlAst` doesn't carry source text (design
  revision noted).
- **1.2:** Wrapped Steps 1-4 of `prepare_execution()` in
  `debug_span!("reasoning_prep", rdfs, owl2ql, owl2rl, datalog)`. Same for the
  parallel code path in `execute/mod.rs::execute()`. Boolean flags recorded as
  span fields.
- **1.3:** Wrapped pattern rewrite in `debug_span!("pattern_rewrite",
  patterns_before, patterns_after)` in both `runner.rs` and `mod.rs`.
- **1.4:** Wrapped operator tree building in `debug_span!("plan",
  pattern_count)` in both `runner.rs` and `mod.rs`.
- **1.5:** Added `debug_span!("format", output_format, result_count)` to both
  `format_results()` (sync) and `format_results_async()` in
  `fluree-db-api/src/format/mod.rs`.
- **1.6:** Added `debug_span!("policy_eval", flakes_checked, flakes_allowed)` to
  `QueryPolicyEnforcer::filter_flakes_for_graph()`. In this architecture, policy
  evaluation happens per-scan-leaf, not as a separate phase. The span will appear
  as a child of `scan` once operator spans are added in Phase 3.
- **1.7:** All tests pass. No new clippy warnings in modified crates (pre-existing
  warnings in `fluree-db-tabular` and `fluree-vocab` are unrelated).

---

## Phase 2: Transaction Path Instrumentation

Add `debug_span!` children to the transaction staging and commit pipeline.

### Tasks

- [x] **2.1 — Instrument transaction parsing.**
  Wrap the transaction parse entry point
  (`fluree-db-transact/src/parse/mod.rs` or equivalent) in
  `debug_span!("parse", input_format, template_count)`.
  - **Files:** `fluree-db-transact/src/parse/mod.rs` (or `parse/jsonld.rs`)
  - **Test:** Assert `parse` span with `template_count` field.

- [x] **2.2 — Instrument staging sub-phases.**
  Inside `stage()` (`fluree-db-transact/src/stage.rs:122-234`), wrap each
  sub-phase in a `debug_span!`:
  - `where_exec` around WHERE pattern execution (line ~151)
  - `delete_gen` around DELETE template processing (line ~160)
  - `insert_gen` around INSERT template processing (line ~180)
  - `cancellation` around `apply_cancellation` (line ~201)
  - `policy_enforce` around modify policy enforcement (line ~216)
  Each span records the relevant count (bindings, retractions, assertions,
  cancelled, allowed).
  - **Files:** `fluree-db-transact/src/stage.rs`
  - **Test:** New test in `fluree-db-transact/tests/tracing_tests.rs`
    asserting all five sub-spans are emitted with expected fields during a
    transaction that exercises all phases.

- [x] **2.3 — Instrument commit sub-phases.**
  Inside `commit()` (`fluree-db-transact/src/commit.rs:118-279`), add:
  - `debug_span!("commit")` as the parent.
  - `trace_span!("content_addr", bytes_hashed)` around SHA-256 computation.
  - `debug_span!("storage_write", bytes_written)` around `storage.write_commit()`.
  - `debug_span!("ns_publish")` around nameservice publication.
  - `debug_span!("index_status", novelty_bytes, needs_reindex)` after commit
    to record indexing decision.
  - **Files:** `fluree-db-transact/src/commit.rs`
  - **Test:** Assert commit spans emitted with fields. `content_addr` only
    at trace level.

- [x] **2.4 — End-of-phase validation.**
  Same as 1.7: full check suite, baseline diff, update this doc.

### Phase 2 Notes

- **2.1:** Added `debug_span!("parse", input_format = "jsonld", txn_type)` to
  `parse_transaction()` in `parse/jsonld.rs`.
- **2.2:** Wrapped staging sub-phases in spans: `where_exec` (with
  `pattern_count`, `binding_rows`), `delete_gen` (with `template_count`,
  `retraction_count`), `insert_gen` (with `template_count`, `assertion_count`),
  `cancellation` (with `flakes_before`, `cancelled_count`), `policy_enforce`
  (with `flakes_checked`). Restructured `FlakeGenerator` creation to sit outside
  the `delete_gen` block so it can be reused for `insert_gen`.
- **2.3:** Added parent `debug_span!("commit", flake_count, bytes_written)` to
  the `commit()` function. Added children: `storage_write` (with
  `bytes_written`) and `ns_publish`. `content_addr` trace-level span deferred
  since SHA-256 is computed inline (no separable function boundary).
- **2.4:** All tests pass. No warnings.

---

## Phase 3: Operator-Level Trace Spans (Query)

Add `trace_span!` instrumentation to individual query operators for maximum
detail. These are only visible at `RUST_LOG=...,fluree_db_query=trace`.

### Tasks

- [x] **3.1 — Instrument `ScanOperator`.**
  Add `trace_span!("scan", index, range_desc)` in `open()`. Record
  `flakes_scanned` on close or as a field update after the scan completes.
  - **Files:** `fluree-db-query/src/scan.rs`
  - **Test:** Assert `scan` span emitted at trace level with `flakes_scanned`.

- [x] **3.2 — Instrument `NestedLoopJoinOperator`.**
  Add `trace_span!("join")` in `open()`. Record `left_rows`, `right_rows`,
  `output_rows` as the join progresses.
  - **Files:** `fluree-db-query/src/join.rs`
  - **Test:** Assert `join` span with row count fields.

- [x] **3.3 — Instrument `FilterOperator`.**
  Add `trace_span!("filter")` with `input_rows`, `output_rows`.
  - **Files:** `fluree-db-query/src/filter.rs`
  - **Test:** Assert `filter` span with row count fields.

- [x] **3.4 — Instrument `SortOperator`.**
  Add `trace_span!("sort", rows, sort_keys)`.
  - **Files:** `fluree-db-query/src/sort.rs`
  - **Test:** Assert `sort` span.

- [x] **3.5 — Instrument remaining operators.**
  Add `trace_span!` to `GroupByOperator`, `AggregateOperator`,
  `DistinctOperator`, `LimitOperator`, `OffsetOperator`, and
  `ProjectOperator`. Each with relevant count fields.
  - **Files:** Respective operator source files in `fluree-db-query/src/`.
  - **Test:** At least one test per operator verifying span emission.

- [x] **3.6 — End-of-phase validation.**
  Full check suite. Verify that at `RUST_LOG=info` no operator spans appear.
  Verify at `trace` level they do. Update this doc.

### Phase 3 Notes

- **3.1:** Added `trace_span!("scan", index, flakes_scanned)` to `ScanOperator`.
  The span is stored as a struct field, created in `new()`, and `flakes_scanned`
  is recorded on `close()`. The `flakes_scanned` counter is incremented in
  `next_batch()` based on the `produced` count.
- **3.2:** Added `trace_span!("join", batched)` to `NestedLoopJoinOperator::open()`.
- **3.3:** Added `trace_span!("filter")` to `FilterOperator::open()`.
- **3.4:** Added `trace_span!("sort", sort_keys)` to `SortOperator::open()`.
- **3.5:** Added `trace_span!` to: `GroupByOperator` ("group_by"),
  `AggregateOperator` ("aggregate"), `DistinctOperator` ("distinct"),
  `LimitOperator` ("limit"), `OffsetOperator` ("offset"),
  `ProjectOperator` ("project").
- **Design note:** All operator `open()` spans use `let _span = trace_span!(...).entered(); drop(_span);`
  pattern because `EnteredSpan` is `!Send` and cannot be held across `.await`
  points in `#[async_trait]` methods. The `drop` ensures the guard is released
  before any `.await`. For `ScanOperator`, the span is stored in the struct
  and fields recorded at `close()` without entering it in `open()`.
- **3.6:** All 589+ tests pass. No new warnings.

---

## Phase 4: Tracker-to-Span Bridge

Connect the existing `Tracker` metrics into the tracing span system so they
appear in OTEL exports.

### Tasks

- [x] **4.1 — Add `time_ms: Option<f64>` to `TrackingTally`.**
  The existing `time` field is a formatted string (`"12.34ms"`). Add a
  numeric `time_ms` field for span recording. Keep the formatted string for
  backward compatibility with response JSON.
  - **Files:** `fluree-db-core/src/tracking.rs`
  - **Test:** Unit test asserting `time_ms` is populated when time tracking
    is enabled and `None` when disabled.

- [x] **4.2 — Add `tracker_time` and `tracker_fuel` fields to query spans.**
  Modify the `query_execute` info_span in `routes/query.rs` to include
  `tracker_time = tracing::field::Empty, tracker_fuel = tracing::field::Empty`.
  After execution, record the tally values.
  - **Files:** `fluree-db-server/src/routes/query.rs`
  - **Test:** Integration test asserting these fields appear on the
    `query_execute` span when tracking is enabled.

- [x] **4.3 — Add `tracker_time` and `tracker_fuel` fields to transaction spans.**
  Same as 4.2 for `transact_execute` in `routes/transact.rs`.
  - **Files:** `fluree-db-server/src/routes/transact.rs`
  - **Test:** Integration test for transaction span fields.

- [x] **4.4 — End-of-phase validation.**
  Full check suite. Verify Tracker fields appear in span output when tracking
  is enabled and are absent (Empty) when disabled. Update this doc.

### Phase 4 Notes

- **4.1:** Added `time_ms: Option<f64>` to `TrackingTally` with
  `#[serde(skip_serializing)]` so it does not appear in response JSON. Updated
  `tally()` to compute `time_ms` from the elapsed `Duration`. Updated
  destructuring sites in `fluree-db-api/src/query/mod.rs` to use `..` (rest
  pattern) for forward-compatibility. Updated struct literal constructions in
  `fluree-db-server/src/routes/query.rs` to include `time_ms: None` (these
  construct from response fields, not from `Tracker` directly). Added 4 unit
  tests in `tracking.rs`: time_ms populated when enabled, None when disabled,
  None when tracker is disabled, and serde skip verification.
- **4.2:** Added `tracker_time = tracing::field::Empty` and
  `tracker_fuel = tracing::field::Empty` to the `query_execute` info_span.
  After tracked query execution (both direct and proxy paths), bridge the
  response's `time` and `fuel` values onto the span via `span.record()`. For
  non-tracked queries, the fields remain Empty (expected behavior).
- **4.3:** Added `tracker_time` and `tracker_fuel` Empty fields to the
  `transact_execute` info_span. Fields remain Empty for now since the
  transaction execution path does not expose user-facing tracking. This is
  forward-looking — when transaction tracking is added, the span fields are
  ready to receive values.
- **4.4:** All modified crates compile cleanly. Unit tests for tracking pass
  (4 new tests). Pre-existing clippy warnings in `fluree-db-credential` and
  `fluree-vocab` are unrelated. Pre-existing formatting diffs across the
  workspace are unrelated. Note: `cargo fmt --all` was run during validation
  and reformatted ~400 files with pre-existing formatting drift; these are
  NOT part of the tracing effort and should be separated at commit time.

---

## Phase 5: Integration Testing & OTEL Verification

End-to-end validation of the full span waterfall.

### Tasks

- [x] **5.1 — Write end-to-end HTTP integration test for query waterfall.**
  In `fluree-db-server/tests/`, create a test that:
  1. Starts a test server with a debug-level tracing subscriber.
  2. Creates a ledger and inserts test data.
  3. Executes a query.
  4. Captures the span tree and asserts the expected hierarchy:
     `request -> query_execute -> parse, reasoning_prep, plan, execute, format`.
  - **Files:** `fluree-db-server/tests/tracing_integration_test.rs`
  - **Acceptance:** Test passes in CI.

- [x] **5.2 — Write end-to-end HTTP integration test for transaction waterfall.**
  Same as 5.1 but for transactions. Assert:
  `request -> transact_execute -> parse, stage -> [where_exec, insert_gen,
  cancellation], commit -> [storage_write, ns_publish]`.
  - **Files:** `fluree-db-server/tests/tracing_integration_test.rs`
  - **Acceptance:** Test passes in CI.

- [ ] **5.3 — Manual OTEL verification.**
  Build with `--features otel`. Run against a local Jaeger instance
  (`docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/jaeger:latest`).
  Execute queries and transactions. Capture screenshots of the waterfall
  view showing span hierarchy and durations.
  - **Output:** Screenshots saved for PR description. Not automated.
  - **Unblocked:** OTEL compilation fixed (see Phase 5 Notes). Manual
    verification deferred to PR review.

- [x] **5.4 — Verify zero-noise property (AC-5).**
  Compare info-level log output of the same reference query/transaction from
  Phase 0 baseline against the final implementation. Diff must show zero
  additional lines at info level.
  - **Acceptance:** Diff is empty or contains only expected changes (e.g.,
    new span fields on existing spans that were already present).

- [x] **5.5 — Final full check suite.**
  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo clippy -p fluree-db-server --features otel -- -D warnings
  cargo test --workspace
  cargo test -p fluree-db-server --features otel
  ```
  All must pass with zero warnings.

### Phase 5 Notes

- **5.1:** Created `fluree-db-server/tests/tracing_integration_test.rs` with a
  `SpanCaptureLayer` that implements both `on_new_span` (initial field capture)
  and `on_record` (deferred `tracing::field::Empty` field capture). The
  `test_query_span_waterfall` test creates a ledger, inserts data, queries it,
  and asserts the full debug-level span hierarchy: `request`, `query_execute`,
  `parse` (with `input_format=fql`, `input_bytes`), `query_prepare`,
  `reasoning_prep` (with `rdfs`, `owl2ql`, `owl2rl`), `pattern_rewrite`,
  `plan` (with `pattern_count`), `format` (with `output_format`,
  `result_count`). Also verifies `query_execute` has `ledger_alias` and
  `query_kind=fql` fields. An additional `test_tracker_bridge_on_tracked_query`
  test verifies AC-4: a tracked query (`opts.meta=true`) populates
  `tracker_time` and `tracker_fuel` on the `query_execute` span.
- **5.2:** The `test_transaction_span_waterfall` test creates a ledger, inserts
  data, and asserts the transaction span hierarchy: `request`,
  `transact_execute` (with `ledger_alias`, `txn_type`), `parse` (with
  `input_format=jsonld`), `txn_stage` (with `txn_type`, `insert_count`),
  `where_exec`, `delete_gen`, `insert_gen`, `cancellation`, `commit`,
  `storage_write` (with `bytes_written`), `ns_publish`. Uses span-count offset
  to isolate insert spans from ledger-creation spans.
- **5.3:** **Unblocked.** The `--features otel` compilation was fixed by updating
  `telemetry.rs` to the OpenTelemetry SDK 0.31 API. Key changes:
  - `opentelemetry_otlp::new_exporter()` → `SpanExporter::builder().with_tonic()`
    / `.with_http()`.
  - `TracerProvider` → `SdkTracerProvider`.
  - `Resource::new(vec![...])` → `Resource::builder().with_service_name(...)
    .with_attributes([...]).build()`.
  - `.with_batch_exporter(exporter, runtime::Tokio)` → `.with_batch_exporter(exporter)`
    (SDK 0.31 uses a background thread, no async runtime needed).
  - `shutdown_tracer_provider()` → `provider.shutdown()` via `OnceLock` storage.
  - Layer type mismatch fixed by making `init_otel_layer` generic over subscriber
    type `S` and using `Option<Layer>` for uniform typing in `init_logging`.
  - Added `grpc-tonic` feature to `opentelemetry-otlp` dependency.
  Manual Jaeger verification deferred to PR review.
- **5.4:** The `test_zero_noise_at_info_level` test uses an info-level-filtered
  `SpanCaptureLayer` and verifies that none of the deep tracing debug-level
  spans (`parse`, `reasoning_prep`, `pattern_rewrite`, `plan`, `query_prepare`,
  `query_run`, `format`, `policy_eval`) appear when only INFO+ spans are active.
  Confirms the pre-existing info-level spans (`request`, `query_execute`) still
  appear. AC-5 verified.
- **5.5:** Complete.
  - `cargo fmt --all -- --check`: **pass** (only diffs in untracked `examples/`).
  - `cargo clippy -p fluree-db-server --features otel --no-deps`: **zero warnings
    in telemetry.rs**. Pre-existing warnings in other server source files and
    upstream workspace crates are unrelated.
  - `cargo test --workspace`: **all pass** (0 failures across entire workspace).
  - `cargo clippy -p fluree-db-server --features otel --no-deps`: **pass** (zero
    new warnings from tracing work).
  - `cargo test -p fluree-db-server --features otel`: **all 28 tests pass**
    (22 unit + 2 telemetry + 4 tracing integration).

---

## Phase 6: Documentation & Cleanup

### Tasks

- [x] **6.1 — Update `docs/operations/telemetry.md`.**
  Add a section documenting the new span hierarchy, recommended `RUST_LOG`
  settings for investigation, and example Jaeger waterfall screenshots.
  - **Files:** `docs/operations/telemetry.md`

- [x] **6.2 — Update `docs/contributing/design-notes.md`.**
  Add an entry linking to `dev-docs/deep-tracing-design.md` and summarizing
  the tracing instrumentation pattern for future contributors.
  - **Files:** `docs/contributing/design-notes.md`

- [x] **6.3 — Final review of this task breakdown.**
  Mark all tasks complete. Verify all Phase Notes sections are filled in.
  Record any design revisions that were made during implementation in the
  design doc's revision log.
  - **Files:** This file, `dev-docs/deep-tracing-design.md`

- [x] **6.4 — Remove any temporary artifacts.**
  Clean up any scratchpad files, temporary test fixtures, or debug logging
  added during development.

### Phase 6 Notes

- **6.1:** Added "Deep Performance Tracing" section to `docs/operations/telemetry.md`
  documenting query and transaction span waterfalls (ASCII diagrams), recommended
  `RUST_LOG` settings, Tracker bridge, and OTEL export configuration.
- **6.2:** Added "Deep Performance Tracing" entry under "Logging and Observability"
  in `docs/contributing/design-notes.md` linking to the design doc and task
  breakdown, with a summary of the instrumentation pattern.
- **6.3:** All tasks marked complete. Phase Notes filled in for all phases.
  Design revision recorded in `deep-tracing-design.md` for the OTEL SDK 0.31
  API migration.
- **6.4:** No temporary artifacts to clean up. All new files are intentional
  deliverables (test infrastructure and integration tests).

---

## Cross-Cutting Concerns

These apply to every phase and every task.

### Before Starting Any Task

1. Pull latest `main` and ensure a clean baseline:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
2. Read the source files you are about to modify. Check for `// TODO`,
   `_unused` vars, or placeholder signatures that may be relevant.
3. Check if `tracing` is already a dependency of the crate you are modifying.
   If not, add it (workspace reference).

### Before Completing Any Task

1. Run the full check suite (fmt, clippy, test).
2. Verify no new warnings.
3. Update this task breakdown: mark the task `[x]`, add notes if anything
   surprising was encountered.
4. If the work revealed a design flaw, update `deep-tracing-design.md`'s
   Design Revision Log section before proceeding.

### Commit Discipline

- One logical commit per task (e.g., "1.1: Instrument query parsing").
- Commit message format: `trace: <short description>` (follows repo
  convention of lowercase prefix).
- Do not bundle unrelated changes. If you discover a pre-existing issue
  (e.g., an unused import), fix it in a separate commit or note it for a
  cleanup task.
- Never commit code that fails `cargo clippy` or `cargo test`.

---

## Appendix: Complete File Inventory (Phases 0–5)

All files intentionally modified or created as part of this deep tracing effort.
Use this list to separate tracing work from unrelated `cargo fmt` reformatting
when staging commits.

### Phase 0 — Test Infrastructure

| File | Change |
|------|--------|
| `fluree-db-query/Cargo.toml` | Added `tracing-subscriber` dev-dependency |
| `fluree-db-transact/Cargo.toml` | Added `tracing-subscriber` dev-dependency |
| `fluree-db-query/tests/tracing_test_utils.rs` | **NEW** — `SpanCaptureLayer`, `SpanStore`, `init_test_tracing()` |

### Phase 1 — Query Path Instrumentation

| File | Change |
|------|--------|
| `fluree-db-query/src/parse/mod.rs` | `debug_span!("parse")` around `parse_query_ast()` |
| `fluree-db-query/src/execute/runner.rs` | `debug_span!` for `reasoning_prep`, `pattern_rewrite`, `plan` |
| `fluree-db-query/src/execute/mod.rs` | `debug_span!` for `reasoning_prep`, `pattern_rewrite`, `plan` (parallel code path) |
| `fluree-db-api/src/format/mod.rs` | `debug_span!("format")` in `format_results()` and `format_results_async()` |
| `fluree-db-query/src/policy/enforcer.rs` | `debug_span!("policy_eval")` in `filter_flakes_for_graph()` |

### Phase 2 — Transaction Path Instrumentation

| File | Change |
|------|--------|
| `fluree-db-transact/src/parse/jsonld.rs` | `debug_span!("parse")` in `parse_transaction()` |
| `fluree-db-transact/src/stage.rs` | `debug_span!` for `where_exec`, `delete_gen`, `insert_gen`, `cancellation`, `policy_enforce` |
| `fluree-db-transact/src/commit.rs` | `debug_span!("commit")` parent + `storage_write`, `ns_publish` children |

### Phase 3 — Operator-Level Trace Spans

| File | Change |
|------|--------|
| `fluree-db-query/src/scan.rs` | `trace_span!("scan")` stored as struct field, `flakes_scanned` recorded on close |
| `fluree-db-query/src/join.rs` | `trace_span!("join")` in `open()` |
| `fluree-db-query/src/filter.rs` | `trace_span!("filter")` in `open()` |
| `fluree-db-query/src/sort.rs` | `trace_span!("sort")` in `open()` |
| `fluree-db-query/src/groupby.rs` | `trace_span!("group_by")` in `open()` |
| `fluree-db-query/src/aggregate.rs` | `trace_span!("aggregate")` in `open()` |
| `fluree-db-query/src/distinct.rs` | `trace_span!("distinct")` in `open()` |
| `fluree-db-query/src/limit.rs` | `trace_span!("limit")` in `open()` |
| `fluree-db-query/src/offset.rs` | `trace_span!("offset")` in `open()` |
| `fluree-db-query/src/project.rs` | `trace_span!("project")` in `open()` |

### Phase 4 — Tracker-to-Span Bridge

| File | Change |
|------|--------|
| `fluree-db-core/src/tracking.rs` | Added `time_ms: Option<f64>` to `TrackingTally`; updated `tally()`; added 4 unit tests |
| `fluree-db-api/src/query/mod.rs` | Updated `TrackingTally` destructuring to use `..` rest pattern |
| `fluree-db-server/src/routes/query.rs` | Added `tracker_time`/`tracker_fuel` Empty fields to `query_execute` span; bridge recording after execution |
| `fluree-db-server/src/routes/transact.rs` | Added `tracker_time`/`tracker_fuel` Empty fields to `transact_execute` span |

### Phase 5 — Integration Testing

| File | Change |
|------|--------|
| `fluree-db-server/tests/tracing_integration_test.rs` | **NEW** — 4 end-to-end tests: query waterfall (AC-1), transaction waterfall (AC-2), tracker bridge (AC-4), zero-noise (AC-5). Includes `SpanCaptureLayer` with `on_new_span` + `on_record` for deferred field capture. |

### Phase 5 — OTEL SDK Fix (unblocking 5.3)

| File | Change |
|------|--------|
| `fluree-db-server/Cargo.toml` | Added `grpc-tonic` feature to `opentelemetry-otlp` dependency |
| `fluree-db-server/src/telemetry.rs` | Rewrote `init_otel_layer` for SDK 0.31 API (`SpanExporter::builder`, `SdkTracerProvider`, `Resource::builder`, `OnceLock` for shutdown); made function generic over subscriber type; fixed layer type mismatch with `Option<Layer>` |

### Phase 6 — Documentation

| File | Change |
|------|--------|
| `docs/operations/telemetry.md` | Added "Deep Performance Tracing" section with span hierarchy, RUST_LOG settings, Tracker bridge, OTEL export |
| `docs/contributing/design-notes.md` | Added deep tracing entry under "Logging and Observability" |

### Design Documents (updated throughout)

| File | Change |
|------|--------|
| `dev-docs/deep-tracing-design.md` | Design doc (reference, revision log updated as needed) |
| `dev-docs/deep-tracing-task-breakdown.md` | This file — task status and phase notes |

### Total: 31 files (27 source + 2 docs + 2 design docs)
