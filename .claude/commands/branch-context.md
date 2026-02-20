# Branch Context

Explore the current branch's changes to understand work done so far and the apparent direction. Context-window-friendly — summarizes rather than dumping raw diffs.

## Arguments

$ARGUMENTS — Optional: topic or area to focus on (e.g., "jaeger integration", "import pipeline"). If omitted, provide a general overview of all branch work.

## Steps

1. **Identify the branch and base** — determine the current branch name and find the merge base with `main`:
   ```bash
   git branch --show-current
   git merge-base main HEAD
   ```

2. **Get the commit narrative** — read the commit log from the merge base to HEAD. This is the primary source of "what was done and why":
   ```bash
   git log --oneline --reverse $(git merge-base main HEAD)..HEAD
   ```
   If there are more than ~30 commits, focus on commit messages rather than reading every diff.

3. **Understand the scope of changes** — get a high-level stat view:
   ```bash
   git diff $(git merge-base main HEAD)..HEAD --stat
   ```
   Identify which crates and areas were touched most heavily.

4. **Topic-focused deep dive** (if topic arguments were provided):
   - Filter the diff to files/hunks relevant to the topic keywords using Grep against the diff output or by reading specific changed files
   - Use the Explore agent if needed to search the branch changes for the topic
   - Focus on understanding the *design decisions* and *current state* rather than every line changed

5. **General deep dive** (if no topic arguments):
   - Group changes by crate/area
   - For each area, read a representative sample of the most significant changes (largest files, new files, files with the most churn)
   - Don't try to read every changed file — focus on new modules, significant refactors, and public API changes

6. **Assess branch trajectory** — based on commit messages, changed files, and any TODO/FIXME comments in the diff, infer:
   - What has been completed
   - What appears to be in progress or partially done
   - What the branch seems to be aiming toward for PR readiness
   - Any loose ends (uncommitted changes, TODO comments, dead code annotations added)

7. **Check current working state** — look at uncommitted work:
   ```bash
   git status
   git diff --stat
   ```
   Note any in-progress work that hasn't been committed yet.

8. **Synthesize a briefing** — present to the user:
   - **Branch**: name and how many commits ahead of main
   - **Summary**: 3-5 sentence overview of what the branch accomplishes
   - **Completed work**: bullet list of finished features/changes, grouped by area
   - **In-progress / partial**: anything that appears incomplete
   - **Apparent goal**: what the branch looks like it's building toward
   - **Loose ends**: uncommitted changes, TODOs, missing tests, doc gaps
   - **Key files**: the most important new or heavily-modified files to be aware of

9. **Offer next steps** — ask the user what they'd like to work on, or whether they'd like to:
   - Run `/docs-context <topic>` to load relevant documentation
   - Run `/validate` to check the current state of the code
   - Continue work on an identified loose end

## Example Output Shape

```
## Branch Context: feat/deep-tracing

**12 commits ahead of main**

**Summary**: This branch adds comprehensive distributed tracing to the Fluree
server using OpenTelemetry. It instruments query, transact, and indexer paths
with hierarchical spans, adds a Jaeger-based validation infrastructure, and
includes documentation for debugging performance issues with traces.

**Completed**:
- OTEL pipeline setup with configurable exporters (Phase 1-2)
- Query path instrumentation with fuel tracking spans (Phase 3)
- Transact path instrumentation (Phase 4-5)
- ...

**In progress**:
- Integration test coverage for span hierarchy validation
- ...

**Loose ends**:
- `dev-docs/` directory is untracked (internal notes?)
- ...

**Key files to know about**:
- `fluree-db-server/src/telemetry.rs` (new OTEL init)
- ...

---

What would you like to work on? I can also run `/docs-context` to load the
tracing documentation, or `/validate` to check the current build state.
```
