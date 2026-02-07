# Validate Changes

Run cargo fmt, check, clippy, and test appropriate to the scope of current changes. Use this before committing.

## Arguments

$ARGUMENTS — Optional: crate name(s) to scope validation to. If omitted, auto-detect from changed files.

## Steps

1. **Determine scope** — if crate names were provided, use those. Otherwise, check `git diff --name-only` and `git diff --staged --name-only` to identify which crates were modified. If changes span many crates or touch workspace-level config, use workspace-wide commands.

2. **Format** — always format the full workspace:
   ```bash
   cargo fmt --all
   ```

3. **Check + Clippy** — scoped to affected crates. Use `--all-features` if feature-gated code was touched:
   ```bash
   cargo clippy -p <crate> --all-features --all-targets -- -D warnings
   ```
   Use `--fix --allow-dirty` if clippy reports auto-fixable issues, then re-run to confirm clean.

4. **Test** — scoped to affected crates. Prefer nextest when available:
   ```bash
   cargo nextest run -p <crate> --all-features
   ```
   Fall back to `cargo test -p <crate> --all-features` if nextest is not installed.

5. **Dead code review** — if any warnings mention unused/dead code:
   - **Use now?** → Complete the wiring. Do not leave incomplete work.
   - **Keep for later?** → Annotate with `#[expect(dead_code)]` and add WHY/WHEN/HOW comments.
   - **Remove?** → Delete it (only if certain it's obsolete).
   - **NEVER** prefix with `_` to suppress the warning.

6. **Report** — summarize results in a table: what passed, what was fixed, any remaining issues. Then suggest a commit message.

## Commit Message Format

After all checks pass, suggest a commit message following this exact format:

- **Line 1**: `category(scope): short description` (under 72 chars). Categories: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`. Scope is the primary crate or area affected.
- **Lines 3-8**: 4-6 bullet points starting with `-`, each describing a specific change. Focus on *what* and *why*, not *how*.

Example:
```
fix(server): use .instrument() for async span context in route handlers

- Replace span.enter() guards held across .await with .instrument(span)
- Fix cross-request trace contamination in Jaeger caused by corrupted thread-local span stack
- Apply pattern to all 18 async route handlers in query, transact, ledger, and admin modules
- Update tracing-guide.md with strengthened async warning and HTTP handler pattern example

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
```

Present the suggested message in a code block so the user can copy it directly.
