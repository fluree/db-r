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

4. **Test** — scoped to affected crates:
   ```bash
   cargo test -p <crate> --all-features
   ```

5. **Dead code review** — if any warnings mention unused/dead code:
   - **Use now?** → Complete the wiring. Do not leave incomplete work.
   - **Keep for later?** → Annotate with `#[expect(dead_code)]` and add WHY/WHEN/HOW comments.
   - **Remove?** → Delete it (only if certain it's obsolete).
   - **NEVER** prefix with `_` to suppress the warning.

6. **Report** — summarize results: what passed, what was fixed, any remaining issues.
