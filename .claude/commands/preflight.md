# Session Preflight

Run this at the start of a new working session to orient yourself and discover tools left by previous work.

## Steps

1. **Check git status** — understand the current branch, staged/unstaged changes, and recent commits:
   ```bash
   git status
   git log --oneline -10
   ```

2. **Scan for dead_code annotations** — these are signals from past sessions. Search for `#[allow(dead_code)]` and `#[expect(dead_code)]` across the workspace:
   ```bash
   grep -rn '#\[allow(dead_code)\]\|#\[expect(dead_code)\]' --include='*.rs' .
   ```
   For each hit, read the surrounding comments and code. Report a summary:
   - What annotated items exist and where
   - Which ones have explanatory comments (good) vs. which lack context (needs attention)
   - Whether any of them are relevant to the user's stated task for this session

3. **Check for compiler warnings** — run a quick workspace check to see if there are any pre-existing warnings:
   ```bash
   cargo check --workspace --all-features --all-targets 2>&1 | grep -E '^warning' | head -30
   ```
   Distinguish between pre-existing warnings (documented in CLAUDE.md) and new ones.

4. **Report** — provide a concise summary:
   - Current branch and recent work context
   - Dead code annotations found (with relevance assessment)
   - Any warnings that need attention
   - Suggested focus areas based on what you found
