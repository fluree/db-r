# W3C SPARQL Compliance Test Suite

The `testsuite-sparql` crate runs **official W3C SPARQL test cases** against Fluree's parser and query engine. Every test is discovered automatically from W3C manifest files — there are zero hand-written test cases.

This guide covers how to run the suite, interpret results, and turn failures into fixes.

## Why This Exists

The W3C publishes its SPARQL test suite as RDF data. Each `manifest.ttl` file declares test entries: a query file, optional input data, and expected results. Every serious SPARQL implementation (Oxigraph, Apache Jena, Eclipse RDF4J) runs these manifests programmatically. We do the same.

The ratio is extraordinary: ~700 lines of Rust infrastructure drive 700+ W3C test cases. Each failure is a spec-backed bug report with built-in test data and expected results.

**Philosophy: failures are features.** When a test fails, the default response is to fix Fluree, not skip the test. Skip entries are reserved for documented, deliberate design divergences reviewed by the team.

## Quick Start

### Run All Tests

```bash
cargo test -p testsuite-sparql
```

This runs all non-ignored W3C test suites. Currently that includes SPARQL 1.0 and 1.1 syntax tests. Query evaluation tests (12 categories, 233 tests) are registered but `#[ignore]`'d — run them with `--include-ignored` or via the Makefile.

### Run a Specific Suite

```bash
# SPARQL 1.1 syntax only
cargo test -p testsuite-sparql sparql11_syntax_query_tests

# SPARQL 1.0 syntax only
cargo test -p testsuite-sparql sparql10_syntax_tests

# Full query evaluation (~5 min, includes all 12 categories)
cargo test -p testsuite-sparql sparql11_query_w3c_testsuite -- --include-ignored

# Single evaluation category
cargo test -p testsuite-sparql sparql11_functions -- --include-ignored
```

### Run With Verbose Output

```bash
cargo test -p testsuite-sparql -- --nocapture 2>&1
```

The suite writes progress to stderr (`Running test N: <test_id> ...`) and a summary at the end.

### Using the Makefile

The `testsuite-sparql/Makefile` provides convenience targets:

```bash
cd testsuite-sparql

make test              # Run syntax tests (live output)
make test-syntax11     # SPARQL 1.1 syntax tests only
make test-syntax10     # SPARQL 1.0 syntax tests only
make test-eval         # Full eval suite (~5 min, all 12 categories)
make test-eval-cat CAT=functions
                       # Run one eval category
make count-eval        # Quick pass/fail counts for eval tests
make report-eval       # Run eval tests, save to report-eval.txt
make report            # Run syntax tests, save to report.txt
make failures          # Show failing syntax tests with details
make count             # Show syntax test pass/fail counts
make show-query TEST=syntax-select-expr-04.rq
                       # Print the .rq file for a test
make investigate TEST=test_34
                       # Search report for a specific test
make clean             # Remove generated report files
```

## Understanding the Output

### Test Summary

After running, the suite prints:

```
=== Test Summary ===
Total:   94
Passed:  79
Ignored: 0
Failed:  15
```

- **Total**: Number of W3C test cases discovered from manifest files
- **Passed**: Tests where Fluree's behavior matched the W3C expectation
- **Ignored**: Tests in the skip list (should be near zero)
- **Failed**: Tests where Fluree diverged from the spec — these are bugs or gaps

### Failure Messages

Each failure includes the test ID, type, and error details:

```
https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34:
  Positive syntax test failed — parser rejected valid query.
  Test: ...#test_34
  File: .../syntax-query/syntax-select-expr-04.rq
```

For syntax tests, failures fall into three categories:

| Failure Type | What It Means | Example |
|-------------|--------------|---------|
| **Positive test fails** | Parser rejects valid SPARQL | Missing feature (subqueries, property path `\|`) |
| **Negative test fails** | Parser accepts invalid SPARQL | Missing validation (BIND scope, GROUP BY scope) |
| **Parser timeout** | Parser enters infinite loop | Bug in grammar handling (collections, BIND expressions) |

### Test IDs

Every test has a unique IRI like:
```
https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34
```

The fragment (`#test_34`) identifies the specific test within that manifest. The path tells you the W3C category (`syntax-query`, `aggregates`, `bind`, etc.).

## From Failure to Fix: The Workflow

### Step 1: Identify the Failure Category

Run the suite and look at the failure message:

```bash
cargo test -p testsuite-sparql sparql11_syntax_query_tests -- --nocapture 2>&1 | tail -40
```

Determine which category:
- **Parser timeout** → Bug in `fluree-db-sparql` grammar rules causing infinite loop
- **Positive syntax rejected** → Missing parser feature or incorrect grammar
- **Negative syntax accepted** → Missing semantic validation pass
- **Query evaluation mismatch** → Bug in query engine, data loading, or result formatting

### Step 2: Find the Test Query

Every W3C test references a `.rq` (query) or `.ru` (update) file. The failure message includes the file URL. Map it to a local path:

```
URL:   https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
Local: testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
```

The pattern: strip `https://w3c.github.io/rdf-tests/` and prepend `testsuite-sparql/rdf-tests/`.

Read the query to understand what SPARQL feature is being tested:

```bash
cat testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
```

### Step 3: Reproduce in Isolation

Try parsing the query directly to see the exact error:

```rust
// Quick test in fluree-db-sparql
let output = fluree_db_sparql::parse_sparql("SELECT (1 + ?x AS ?y) WHERE { ?x ?p ?o }");
println!("has_errors: {}", output.has_errors());
for err in output.errors() {
    println!("  error: {err:?}");
}
```

Or use the `parse_with_timeout` pattern from `sparql_handlers.rs` if you suspect an infinite loop.

### Step 4: Investigate the Root Cause

For **parser issues**, the relevant code is in `fluree-db-sparql/`. Start with:
- `src/parser/` — Grammar rules and parser combinators
- `src/ast/` — AST types the parser emits

For **query evaluation issues**, the chain is:
1. `fluree-db-sparql` → parses to `SparqlAst`
2. `fluree-db-query` → evaluates the AST against a ledger
3. `fluree-db-api` → orchestrates ledger creation and query execution

### Step 5: Create an Issue

Use this template:

```markdown
## W3C SPARQL Compliance: [short description]

**Test ID:** `https://w3c.github.io/rdf-tests/sparql/sparql11/[category]/manifest.ttl#[test_name]`
**Category:** [syntax-query | aggregates | bind | etc.]
**Failure type:** [parser timeout | positive syntax rejected | negative syntax accepted | evaluation mismatch]

### Test Query

\`\`\`sparql
[paste the .rq file contents]
\`\`\`

### Expected Behavior

[For positive syntax: should parse successfully]
[For negative syntax: should be rejected]
[For evaluation: expected results from the .srx/.srj file]

### Actual Behavior

[Error message or incorrect output]

### Root Cause Analysis

[What part of the code needs to change and why]

### W3C Spec Reference

[Link to relevant section of https://www.w3.org/TR/sparql11-query/]
```

### Step 6: Fix and Verify

After making code changes:

```bash
# Verify the specific test passes (run the suite, grep for the test)
cargo test -p testsuite-sparql sparql11_syntax_query_tests -- --nocapture 2>&1 | grep "test_34"

# Verify you haven't regressed other tests
cargo test -p testsuite-sparql

# Run the parser's own tests
cargo test -p fluree-db-sparql

# Full CI parity check
cargo clippy -p fluree-db-sparql --all-features -- -D warnings
```

### Step 7: Update the Compliance Status

After merging a fix, update the pass/fail counts in `dev-docs/sparql/00-sparql-compliance-plan.md`.

## Using Claude Code for Debugging

Claude Code is particularly effective for SPARQL compliance work because each failure is self-contained: a query file, an expected behavior, and a specific error. Here's how to give a session full context.

### Prompt Template for Parser Failures

```
I'm working on W3C SPARQL compliance in Fluree. The following test is failing:

Test ID: https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34
Category: Positive syntax test (parser should accept this query but rejects it)

The query file is at: testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq

The SPARQL parser is in fluree-db-sparql/. The parse entry point is
`parse_sparql()` which returns `ParseOutput<SparqlAst>` — check `has_errors()`.

Please:
1. Read the failing query file
2. Understand what SPARQL feature it tests
3. Find the relevant parser grammar in fluree-db-sparql/src/parser/
4. Identify why the parser rejects this input
5. Propose a fix
```

### Prompt Template for Query Evaluation Failures

```
I'm working on W3C SPARQL compliance. This query evaluation test is failing:

Test ID: https://w3c.github.io/rdf-tests/sparql/sparql11/aggregates/manifest.ttl#agg01
Test data: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.ttl
Query file: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.rq
Expected results: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.srx

The test harness creates an in-memory Fluree ledger, loads the data via
stage_owned().insert_turtle(), executes the query via query_sparql(), and
compares results.

Actual output: [paste actual output]
Expected output: [paste expected from .srx file]

Please investigate why the results differ and propose a fix.
```

### Key Files to Reference

When asking Claude Code for help, these files provide essential context:

| Context Needed | File(s) |
|---------------|---------|
| Test harness architecture | `testsuite-sparql/src/lib.rs`, `src/evaluator.rs` |
| How manifests are parsed | `testsuite-sparql/src/manifest.rs` |
| Syntax test handlers | `testsuite-sparql/src/sparql_handlers.rs` |
| Eval test handler (data load + query + compare) | `testsuite-sparql/src/query_handler.rs` |
| Expected result parsing (.srx/.srj) | `testsuite-sparql/src/result_format.rs` |
| Isomorphic result comparison | `testsuite-sparql/src/result_comparison.rs` |
| SPARQL parser entry point | `fluree-db-sparql/src/lib.rs` (`parse_sparql()`) |
| Parser grammar rules | `fluree-db-sparql/src/parser/` |
| SPARQL AST types | `fluree-db-sparql/src/ast/` |
| Query engine | `fluree-db-query/src/` |
| API orchestration | `fluree-db-api/src/` |
| Design doc with failure analysis | `dev-docs/sparql/00-sparql-compliance-plan.md` |

### Batch Processing Tips

When multiple tests fail for the same root cause (e.g., "all BIND tests timeout"), group them:

```
These 3 tests all timeout in the parser on BIND expressions:
- test_34: SELECT (1 + ?x AS ?y)
- test_40: SELECT (CONCAT(?x, "!") AS ?label)
- test_65: subquery with SELECT expression

All are in testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/.

The parser code for BIND is in fluree-db-sparql/src/parser/. Please find the
common root cause and fix all three.
```

## Architecture Overview

### Crate Structure

```
testsuite-sparql/
├── Cargo.toml                      # Workspace member, publish = false
├── Makefile                        # Developer convenience targets
├── src/
│   ├── lib.rs                      # check_testsuite() entry point
│   ├── vocab.rs                    # W3C namespace constants (mf:, qt:, etc.)
│   ├── files.rs                    # URL → local file path mapping
│   ├── manifest.rs                 # TestManifest: Iterator<Item=Test>
│   ├── evaluator.rs                # TestEvaluator: type → handler dispatch
│   ├── sparql_handlers.rs          # Handler registration (syntax + eval)
│   ├── query_handler.rs            # QueryEvaluationTest: load data, run query, compare
│   ├── result_format.rs            # Parse .srx/.srj expected result files
│   └── result_comparison.rs        # Isomorphic result comparison (blank node mapping)
├── tests/
│   └── w3c_sparql.rs               # Test entry points (syntax + 12 eval categories)
└── rdf-tests/                      # Git submodule → github.com/w3c/rdf-tests
```

### How It Works

**1. Manifest Parsing** (`manifest.rs`): `TestManifest` implements `Iterator<Item = Result<Test>>`. It loads `manifest.ttl` files using Fluree's own Turtle parser, follows `mf:include` links recursively, and extracts per-test metadata: type, query file, data file, expected results.

**2. Handler Dispatch** (`evaluator.rs`): `TestEvaluator` maps test type URIs (e.g., `mf:PositiveSyntaxTest11`) to handler functions. For each test, it finds the matching handler and invokes it.

**3. SPARQL Handlers** (`sparql_handlers.rs` + `query_handler.rs`): The Fluree-specific logic. For syntax tests, calls `fluree_db_sparql::parse_sparql()` with a 5-second timeout (to catch infinite loops) and checks whether parsing succeeded or failed. For evaluation tests, `query_handler.rs` creates an in-memory Fluree ledger, loads Turtle test data, executes the SPARQL query, and compares results against expected `.srx`/`.srj` files using isomorphic matching (handles blank node equivalence, unordered solution sets, and literal datatype normalization).

**4. Test Entry Points** (`tests/w3c_sparql.rs`): Each test function is ~5 lines — just a manifest URL and a skip list. The harness does the rest.

### Key Design Decisions

- **Uses Fluree's own Turtle parser** for manifest files. If our parser can't handle well-formed W3C manifests, that's a bug worth knowing about.
- **5-second timeout** on `parse_sparql()` catches infinite loops without blocking the suite.
- **Fluree's `list_index`** approach (instead of `rdf:first/rdf:rest`) simplifies manifest list handling.
- **`@base` prepended** to manifest files since they use `<>` (empty relative IRI) which requires a base.

## Test Categories

### Syntax Tests (Phase 1)

| Suite | What It Tests | Manifest |
|-------|-------------|----------|
| SPARQL 1.1 syntax | Parser correctness for SPARQL 1.1 grammar | `syntax-query/manifest.ttl` |
| SPARQL 1.0 syntax | Backward compatibility with SPARQL 1.0 | `manifest-syntax.ttl` |

### Query Evaluation Tests (Phase 2)

Each test creates an in-memory Fluree ledger, loads RDF data, executes a SPARQL query, and compares results against W3C expected outputs. Run with `make test-eval-cat CAT=<name>`.

| Suite | What It Tests | Manifest |
|-------|-------------|----------|
| Aggregates | COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE | `aggregates/manifest.ttl` |
| BIND | BIND expressions, variable assignment | `bind/manifest.ttl` |
| Bindings | VALUES inline data | `bindings/manifest.ttl` |
| Cast | xsd:integer(), xsd:double(), xsd:string() | `cast/manifest.ttl` |
| Construct | CONSTRUCT query form | `construct/manifest.ttl` |
| Exists | FILTER EXISTS, FILTER NOT EXISTS | `exists/manifest.ttl` |
| Functions | String, numeric, date/time, hash, IRI functions | `functions/manifest.ttl` |
| Grouping | GROUP BY semantics, error handling | `grouping/manifest.ttl` |
| Negation | MINUS, NOT EXISTS | `negation/manifest.ttl` |
| Project-Expression | SELECT expressions, AS aliases | `project-expression/manifest.ttl` |
| Property-Path | `/`, `\|`, `^`, `+`, `*`, `?` operators | `property-path/manifest.ttl` |
| Subquery | Nested SELECT within WHERE | `subquery/manifest.ttl` |

## Managing the Skip List

Skip entries are the `ignored_tests` parameter in `check_testsuite()` calls:

```rust
check_testsuite(
    "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl",
    &[
        // Deliberately accept bare `1` as integer literal (RDF 1.1 vs 1.0)
        // Spec: https://www.w3.org/TR/sparql11-query/#rNumericLiteral
        // Reviewed: 2025-02-15 by @ajohnson, @bsmith
        "https://...#test_99",
    ],
)
```

**Rules:**
1. Start with an **empty** skip list. Expect full compliance.
2. Only add entries after investigation confirms a *deliberate* design choice, not a bug.
3. Every skip entry must have a comment explaining why, linking to the relevant spec section.
4. Skip entries require review by 2+ team members.
5. The total skip list should be <5% of tests (Oxigraph skips ~25 out of 700+).
6. Review skip entries periodically — remove them as features are added.

## Updating the rdf-tests Submodule

The W3C test data lives in a git submodule at `testsuite-sparql/rdf-tests/`. To update to the latest W3C tests:

```bash
cd testsuite-sparql/rdf-tests
git pull origin main
cd ../..
git add testsuite-sparql/rdf-tests
git commit -m "chore: update W3C rdf-tests submodule"
```

After updating, run the full suite to check for new tests or changed expectations:

```bash
cargo test -p testsuite-sparql
```

## Related Documentation

- [Design doc](../../dev-docs/sparql/00-sparql-compliance-plan.md) — Full architecture, failure analysis, and phased implementation plan
- [Tests guide](tests.md) — General testing practices
- [SPARQL query docs](../query/sparql.md) — User-facing SPARQL feature documentation
- [Compatibility](../reference/compatibility.md) — Standards compliance status
- [Crate map](../reference/crate-map.md) — Workspace architecture
