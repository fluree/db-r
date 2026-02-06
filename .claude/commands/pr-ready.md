# PR Readiness Check

Full CI-parity validation. Run this before creating a pull request to ensure CI will pass.

## Steps

1. **Format check** — verify formatting is clean:
   ```bash
   cargo fmt --all -- --check
   ```
   If this fails, run `cargo fmt --all` and report what changed.

2. **Clippy (CI parity)** — workspace-wide, all features, all targets:
   ```bash
   cargo clippy --all --all-features --all-targets -- -D warnings
   ```
   This matches the CI pipeline exactly. Fix any issues.

3. **Test (CI parity)** — workspace-wide, all features:
   ```bash
   cargo nextest run --workspace --all-features --no-fail-fast
   ```
   If `cargo-nextest` is not installed, fall back to:
   ```bash
   cargo test --workspace --all-features
   ```

4. **Dead code final review** — scan for any `#[allow(dead_code)]` you introduced in this branch:
   ```bash
   git diff main --name-only | xargs grep -n '#\[allow(dead_code)\]\|#\[expect(dead_code)\]' 2>/dev/null
   ```
   Every annotation MUST have an accompanying comment explaining WHY/WHEN/HOW. Flag any that don't.

5. **Docs check** — if your changes affect user-facing behavior, verify that `docs/` has been updated. Check:
   ```bash
   git diff main --stat -- docs/
   ```
   If no docs changes and behavior changed, flag this as a gap.

6. **Report** — provide a go/no-go summary:
   - Format: pass/fail
   - Clippy: pass/fail (with details if fail)
   - Tests: pass/fail (with failure details)
   - Dead code annotations: all properly documented? yes/no
   - Docs: up to date? yes/no/not-applicable
   - Overall: READY / NOT READY (with action items if not ready)
