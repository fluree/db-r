# Docs Context

Load project documentation relevant to a topic into the session context. Designed to be context-window-friendly — reads selectively, not exhaustively.

## Arguments

$ARGUMENTS — Optional: topic, area, or keywords to focus on (e.g., "otel and deep tracing", "storage traits", "policy enforcement"). If omitted, read `docs/SUMMARY.md` and provide a high-level map of available documentation.

## Strategy

The goal is to **efficiently load the right docs** without bloating the context window. Follow these principles:

- **Start narrow, widen only if needed.** Don't read every file — identify the 3-8 most relevant docs and read those.
- **Summarize, don't parrot.** After reading, synthesize the key information into a concise briefing. The user needs to understand the concepts and patterns, not see raw markdown repeated back.
- **Preserve actionable detail.** Include specific type names, function signatures, config keys, file paths, and patterns that would be needed to do work in the area. A summary that says "there's a storage trait" is less useful than one that says "`Storage` trait in `fluree-db-core/src/storage.rs` with methods `get`, `put`, `exists`."

## Steps

1. **Read the table of contents** — start with `docs/SUMMARY.md` to understand the full doc structure.

2. **Identify relevant docs** — based on the user's topic (or lack thereof), select the most relevant doc files. Use this priority order:
   - Docs whose title/path directly matches the topic keywords
   - Design docs (`docs/design/`) for architectural understanding
   - Contributing docs (`docs/contributing/`) for development patterns
   - Reference docs (`docs/reference/`) for crate structure and config
   - Operations docs (`docs/operations/`) for runtime behavior
   - The CLAUDE.md table mapping topics to doc files

3. **Read selectively** — use the Explore agent (subagent_type=Explore) to search `docs/` for the topic keywords if the topic doesn't map cleanly to known files. Read the identified files directly. Aim for 3-8 files maximum.

4. **Cross-reference with code** — for key types, traits, or patterns mentioned in the docs, do a quick Grep/Glob to confirm they still exist and note their current locations. Docs can drift from code — the code is the truth.

5. **Synthesize a briefing** — present to the user:
   - **Topic overview**: 2-4 sentences on what this area of the codebase does
   - **Key files & types**: bullet list of the most important source files, traits, structs, and functions with brief descriptions
   - **Architecture notes**: how the pieces fit together, key design decisions, gotchas
   - **Relevant config/env vars**: if applicable
   - **Docs read**: list which doc files you consulted (so the user knows what's loaded)

6. **Offer next steps** — after the briefing, ask the user:
   - What they'd like help with in this area, OR
   - Whether they'd also like to run `/branch-context` to supplement with an understanding of in-progress work on the current branch

## Example Output Shape

```
## Docs Context: OTEL & Deep Tracing

**Overview**: Fluree uses the `tracing` crate with optional OpenTelemetry export
(feature-gated behind `otel`). Spans are structured hierarchically from HTTP
request → query/transact → index operations...

**Key files & types**:
- `fluree-db-server/src/telemetry.rs` — OTEL pipeline setup, `init_telemetry()`
- `fluree-db-core/src/tracker.rs` — `QueryTracker` for fuel/cost tracking
- ...

**Architecture**: ...

**Docs consulted**: telemetry.md, tracing-guide.md, performance-tracing.md, configuration.md

---

What would you like to work on in this area? Or would you also like to run
`/branch-context` to see what's already been done on this branch?
```
