# Adding Tracing Spans to New Code

When you add or modify code paths in Fluree, you should instrument them with tracing spans so that performance investigations can decompose wall-clock time into meaningful phases. This guide explains the conventions, patterns, and gotchas.

## The Two-Tier Span Strategy

Fluree uses a tiered approach so that tracing is **zero-noise by default** but **deeply informative on demand**.

### Tier 1: `info_span!` -- always visible

These are the public telemetry contract. They appear at the default `RUST_LOG=info` level and are the spans operators see in production. **Do not add new info-level spans casually** -- they should represent top-level operations or phases that are *always* relevant.

Existing info spans: `request`, `query_execute`, `transact_execute`, `txn_stage`, `txn_commit`, `index_build`, `sort_blocking`, `join_flush_*`, etc.

### Tier 2: `debug_span!` -- opt-in investigation

These decompose an operation into its phases: parse, plan, execute, format, etc. Visible when a developer sets `RUST_LOG=info,fluree_db_query=debug`. Add debug spans when you create a new code path that represents a **distinct, named phase** of an operation.

### Tier 3: `trace_span!` -- maximum detail

Per-operator, per-item, or per-iteration spans. Visible at `RUST_LOG=info,fluree_db_query=trace`. Use for fine-grained instrumentation in hot paths where you only want visibility during deep investigation.

### Decision guide

| You're adding... | Span level | Example |
|-------------------|-----------|---------|
| New top-level operation (HTTP handler, background task) | `info_span!` | `sparql_execute` |
| New phase within an existing operation | `debug_span!` | `reasoning_prep`, `format` |
| Per-item or per-iteration detail in a hot path | `trace_span!` | `scan`, `filter`, `resolve_commit` |

## Code Patterns

### Sync phases (no `.await`)

Use `span.enter()` which creates a guard dropped at end of scope:

```rust
let span = tracing::debug_span!(
    "pattern_rewrite",
    patterns_before = patterns.len() as u64,
    patterns_after = tracing::field::Empty,  // recorded later
);
let _guard = span.enter();

// ... do the rewriting ...

span.record("patterns_after", rewritten.len() as u64);
// _guard dropped here, span ends
```

### Async phases (contains `.await`)

**Never** hold a `span.enter()` guard across an `.await` point. The `Entered` guard is `!Send`, so if the async runtime suspends the task, the span context is lost and child spans become orphaned root traces in Jaeger.

Instead, use `.instrument(span)`:

```rust
let span = tracing::debug_span!(
    "format",
    output_format = %format_name,
    result_count = total_rows as u64,
);
format_results(batch, format).instrument(span).await
```

If you need to record deferred fields on a span that wraps an async block, use `Span::current()` inside the instrumented block:

```rust
let span = tracing::debug_span!(
    "txn_stage",
    insert_count = tracing::field::Empty,
    delete_count = tracing::field::Empty,
);
async {
    // ... do staging work ...
    let current = tracing::Span::current();
    current.record("insert_count", inserts as u64);
    current.record("delete_count", deletes as u64);
    Ok(result)
}.instrument(span).await
```

### spawn_blocking

For `tokio::task::spawn_blocking`, enter the span *inside* the closure:

```rust
let span = tracing::debug_span!("heavy_compute");
tokio::task::spawn_blocking(move || {
    let _guard = span.enter();
    // ... sync work ...
}).await
```

### Lightweight operators (hot path)

For simple operators that just need a span marker, use the terse `.entered()` pattern:

```rust
fn open(&mut self, ctx: &mut Context<S, C>) -> Result<()> {
    let _span = tracing::trace_span!("filter").entered();
    self.child.open(ctx)?;
    // ...
    Ok(())
}
```

## Deferred Fields

Declare fields as `tracing::field::Empty` at span creation, then record values later. This is essential for fields whose values aren't known until the operation completes.

```rust
let span = tracing::debug_span!(
    "plan",
    pattern_count = tracing::field::Empty,
);
let _guard = span.enter();

let plan = build_plan(&patterns)?;
span.record("pattern_count", plan.patterns.len() as u64);
```

**Gotcha:** `tracing::Span::current().record(...)` records on the *current innermost* span. If you've entered a child span, `.record()` targets the child, not the parent. Get a handle to the parent span before entering children:

```rust
let parent_span = tracing::debug_span!("outer", total = tracing::field::Empty);
let _parent_guard = parent_span.enter();

{
    let _child = tracing::trace_span!("inner").entered();
    // Span::current() is now "inner", NOT "outer"
}

// Back to "outer" scope -- safe to record on parent
parent_span.record("total", count as u64);
```

## `#[tracing::instrument]` vs Manual Spans

**Use `#[tracing::instrument]`** for simple functions where:
- You want span entry/exit to match the function boundary
- The function name is a good span name
- You don't need deferred field recording

Always use `skip_all` and explicitly list fields:

```rust
#[tracing::instrument(level = "debug", name = "parse", skip_all, fields(input_format, input_bytes))]
fn parse_query(input: &[u8], format: &str) -> Result<Query> {
    // ...
}
```

**Use manual spans** when:
- The span covers only *part* of a function
- You need a different name than the function
- You need deferred fields
- The function is a hot path (the `#[instrument]` macro captures all arguments by default unless you `skip_all`)

## Where to Add Spans

### New query feature

If you add a new phase to query execution (e.g., a new optimization pass):

1. Add a `debug_span!` in the code path
2. Add the span name to the hierarchy in `docs/operations/telemetry.md`
3. Add a test in `fluree-db-api/tests/it_tracing_spans.rs` verifying the span emits

### New operator

If you add a new query operator:

1. Add a `trace_span!` in the operator's `open()` method
2. If it's a blocking/buffering operator (like sort), add an info-level timing span in `next_batch()`
3. Add a test verifying the span emits at trace level

### New transaction phase

If you add a new phase to transaction processing:

1. Add a `debug_span!` in the phase code
2. Record relevant counts/sizes as deferred fields
3. Add the span to the hierarchy in `docs/operations/telemetry.md`

### New background task

If you add a new background task (like indexing, garbage collection, compaction):

1. Add an `info_span!` as the **trace root** (these are independent traces, not children of HTTP requests)
2. Add debug sub-spans for phases within the task
3. Ensure the crate target is listed in the OTEL `Targets` filter in `telemetry.rs`

## Testing Spans

All new spans should have at least one test verifying they emit with expected fields at the right level.

### Test utilities

The test infrastructure lives in `fluree-db-api/tests/support/tracing.rs`:

```rust
use crate::support::tracing::{init_test_tracing, init_info_only_tracing};

#[tokio::test]
async fn my_new_span_emits_at_debug_level() {
    let (store, _guard) = init_test_tracing(); // captures ALL levels

    // ... run the code that emits the span ...

    assert!(store.has_span("my_new_phase"));
    let span = store.find_span("my_new_phase").unwrap();
    assert_eq!(span.level, tracing::Level::DEBUG);
    assert!(span.fields.contains_key("some_field"));
}

#[tokio::test]
async fn my_new_span_not_visible_at_info() {
    let (store, _guard) = init_info_only_tracing(); // captures only INFO+

    // ... run the code ...

    assert!(!store.has_span("my_new_phase")); // zero noise at info
}
```

### Test helpers available

- `init_test_tracing()` -- captures all spans regardless of level (for verifying span existence)
- `init_info_only_tracing()` -- captures only INFO+ (for verifying zero-noise at default level)
- `SpanStore::has_span(name)` -- check if a span was emitted
- `SpanStore::find_span(name)` -- get span details (level, fields, parent)
- `SpanStore::find_spans(name)` -- find all spans with a given name
- `SpanStore::span_names()` -- list all captured span names

### Where to put tests

- Tracing integration tests go in `fluree-db-api/tests/it_tracing_spans.rs`
- The test utilities are in `fluree-db-api/tests/support/tracing.rs`

## OTEL Layer Configuration

If you add a new crate that emits spans that should be exported via OTEL, add it to the `Targets` filter in `fluree-db-server/src/telemetry.rs`:

```rust
let otel_filter = Targets::new()
    .with_target("fluree_db_server", Level::DEBUG)
    .with_target("fluree_db_api", Level::DEBUG)
    // ... existing targets ...
    .with_target("my_new_crate", Level::DEBUG);  // ADD THIS
```

Without this, spans from the new crate will appear in console logs but not in Jaeger/Tempo.

## Checklist for New Instrumentation

- [ ] Chose the right span level (info / debug / trace)
- [ ] Used `span.enter()` only in sync code, `.instrument(span)` for async
- [ ] Added deferred fields for values computed after span creation
- [ ] Tested span emission with `SpanCaptureLayer`
- [ ] Verified zero-noise at INFO level (debug/trace spans don't appear)
- [ ] Updated span hierarchy in `docs/operations/telemetry.md` if adding debug/info spans
- [ ] Added new crate to OTEL `Targets` filter if applicable

## Common Gotchas

1. **`Entered` is `!Send`** -- cannot cross `.await`. Use `.instrument()` for async.
2. **`Span::current().record()` targets the innermost span** -- not necessarily the one you intend. Hold a reference to the span you want to record on.
3. **OTEL exporter floods** -- if you set `RUST_LOG=debug` globally, third-party crates (hyper, tonic, h2) emit debug spans that overwhelm the OTEL batch processor. The `Targets` filter on the OTEL layer prevents this.
4. **Tower-HTTP duplicate `request` span** -- tower-http's `TraceLayer` creates its own `request` span at DEBUG level. We've configured it to use TRACE level to avoid collision with Fluree's `request` info_span.
5. **`set_global_default` in tests** -- can only be called once per process. Use `set_default()` which returns a guard scoped to the test.

## Related Documentation

- [Performance Investigation](../troubleshooting/performance-tracing.md) -- How operators use deep tracing
- [Telemetry and Logging](../operations/telemetry.md) -- Configuration reference
- [Deep Tracing Playbook](../../dev-docs/deep-tracing-playbook.md) -- Comprehensive implementation reference
