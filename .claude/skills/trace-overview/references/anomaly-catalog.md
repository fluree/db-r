# Anomaly Catalog — Cross-Trace Analysis

Guide for interpreting findings from `trace_corpus_anomalies.py`.

## ERROR: Orphaned Spans

**What**: A span's `references[].spanID` (parent) is not found in the trace's span list.

**Root cause**: Parent span context was lost before the child span was created. Common when `spawn_blocking` or `std::thread::scope` is used without `Span::current()` propagation.

**Fix**: At the call site, capture `let parent = tracing::Span::current();` before the spawn, then `let _guard = parent.enter();` inside the closure.

**Code locations**: Check `fluree-db-indexer/src/lib.rs`, `fluree-db-indexer/src/index_build.rs`, `fluree-db-api/src/import.rs` for spawn points.

## WARNING: Multiple Root Spans

**What**: More than one span in a trace has no parent reference.

**Root cause**: Almost always `.enter()` guard held across `.await` in async code. When the task yields, the span stays "entered" on the thread. Another task picks up the thread and creates spans that erroneously parent under this span — or the original span context is lost entirely, producing a new root.

**Fix**: Replace `let _guard = span.enter();` with `async { ... }.instrument(span).await`.

**Detection**: Check the `code.file.path` tag on the extra root spans — that shows where the span was created. The problematic `.enter()` call is typically in the parent function that *should* have been the parent.

**Exception**: `index_gc` appearing as a separate root is **intentional** (fire-and-forget `tokio::spawn`).

## WARNING: Error Traces

**What**: A trace's root span has a non-empty `error_code` tag.

**Common values**:
- `error:LedgerNotFound` — queried a ledger that doesn't exist
- `error:InvalidQuery` — malformed query
- `error:NoveltyAtMax` — insert rejected due to full novelty buffer (backpressure)
- `error:PolicyViolation` — policy enforcement rejected the operation

**In stress tests**: `error:NoveltyAtMax` is expected and normal. The test scripts retry with backoff.

## WARNING: Missing Expected Children

**What**: A parent span (e.g., `transact_execute`) exists but its expected children (e.g., `txn_stage`, `txn_commit`) are absent.

**Root cause options**:
1. **Error short-circuit**: The operation failed before reaching the child phase. Check if the trace also has an error_code.
2. **Log level filtering**: Child spans at `debug_span!` won't appear if the OTEL Targets filter doesn't include that crate at DEBUG level.
3. **Code path divergence**: A different code path was taken (e.g., cached result, early return).

**What to check**: Look at the parent span's duration. If it's very short (< 1ms for a transaction), the operation likely errored out early. If it's normal duration with no error, the child spans may be filtered by log level.

## WARNING: Unexpected Thread Crossings

**What**: A child span runs on a different `thread.id` than its parent, but the child's operation name is not in the expected cross-thread set.

**Root cause**: Either a new `spawn_blocking`/`thread::scope` was added without being registered in the expected set, or async context propagation is broken.

**Fix**: If the crossing is intentional, add the span name to `EXPECTED_CROSS_THREAD` in both `trace_anomalies.py` and `trace_corpus_anomalies.py`. If unintentional, check for `.enter()` across `.await`.

## INFO: Duration Outliers

**What**: A trace's root span duration is > 2 standard deviations from the mean for its operation type.

**Root cause options**:
1. **I/O contention**: Storage writes taking longer (check `commit_write_commit_blob` duration)
2. **Index lock contention**: Query or transaction waiting for index build to complete
3. **GC pressure**: System GC pause during the operation
4. **Data volume**: More data to process (check `flake_count`, `result_count` tags)
5. **Complex query**: More operators executing (check span count — high span count often correlates with duration)

**Action**: Use `trace-inspect` skill to drill into the outlier trace and identify which child span consumed the extra time.

## INFO: Structural Outliers

**What**: A trace has significantly different span count (> 3x or < 1/3 of average) compared to other traces of the same operation type.

**Root cause**: Usually driven by query/transaction complexity:
- **High span count**: Complex query with many operators (sort, join, filter, etc.)
- **Low span count**: Simple operation or error short-circuit

**When it's normal**: Query traces (`query:fql`, `query:sparql`) naturally vary from 7 spans (simple select) to 100+ (aggregated sorted query with property joins).

**When to investigate**: If `insert:fql` or `transact:fql` traces show high variance, that's unusual — they should have consistent structure.

## INFO: Unexpected Zero-Duration Spans

**What**: Spans with `duration == 0` that are NOT in the known-benign set.

**Root cause**: Either a genuinely sub-microsecond operation or a timing anomaly (Jaeger timestamp sanitization when child spans report negative durations).

**Known-benign set**: `commit_namespace_delta`, `commit_build_record`, `commit_verify_sequencing`, `commit_publish_nameservice`, `commit_generate_metadata_flakes`, `commit_clone_novelty`, `where_exec` (no WHERE clause), `cancellation` (no cancellation), `delete_gen` (no deletes).

**Action**: If a new span appears here consistently, either add it to the known-benign set (if it's genuinely sub-microsecond) or investigate why it has no measurable duration.
