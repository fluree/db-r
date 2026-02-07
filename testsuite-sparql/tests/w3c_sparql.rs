use anyhow::Result;
use testsuite_sparql::check_testsuite;

// =============================================================================
// SPARQL 1.1 Syntax Tests
// =============================================================================

/// W3C SPARQL 1.1 syntax tests (positive + negative).
///
/// Tests only the parser — no query execution, no data loading.
#[test]
fn sparql11_syntax_query_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Full Query Test Suite (includes all 13 categories)
// =============================================================================

/// W3C SPARQL 1.1 full query test suite.
///
/// This is the top-level manifest that includes all 13 query categories:
/// aggregates, bind, bindings, cast, construct, exists, functions,
/// grouping, negation, project-expression, property-path, subquery,
/// and syntax-query.
///
/// Query evaluation tests run against an in-memory Fluree ledger.
/// Run with `--include-ignored` to execute.
#[test]
#[ignore = "Full suite (~5 min); use per-category tests or --include-ignored"]
fn sparql11_query_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.1 Per-Category Query Evaluation Tests
//
// Each test runs the eval tests for one W3C category. Run individually with:
//   cargo test -p testsuite-sparql sparql11_<category> -- --nocapture --include-ignored
//
// These are #[ignore]'d because they contain failing tests (unsupported features).
// Once ignored_tests lists are populated, they can be un-ignored.
// =============================================================================

/// W3C SPARQL 1.1 aggregates evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 2/46 pass"]
fn sparql11_aggregates() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/aggregates/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 BIND evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 3/11 pass"]
fn sparql11_bind() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bind/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 VALUES (bindings) evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 3/11 pass"]
fn sparql11_bindings() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/bindings/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 CAST evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 0/6 pass"]
fn sparql11_cast() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/cast/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 CONSTRUCT evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 0/7 pass"]
fn sparql11_construct() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/construct/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 EXISTS evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 3/6 pass"]
fn sparql11_exists() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/exists/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 functions evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 24/75 pass"]
fn sparql11_functions() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/functions/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 GROUP BY evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 3/6 pass"]
fn sparql11_grouping() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/grouping/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 MINUS / NOT EXISTS evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 5/12 pass"]
fn sparql11_negation() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/negation/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 SELECT expressions evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 2/7 pass"]
fn sparql11_project_expression() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/project-expression/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 property path evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 12/33 pass"]
fn sparql11_property_path() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/property-path/manifest.ttl",
        &[],
    )
}

/// W3C SPARQL 1.1 subquery evaluation tests.
#[test]
#[ignore = "Phase 2 baseline: 1/14 pass"]
fn sparql11_subquery() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/subquery/manifest.ttl",
        &[],
    )
}

// =============================================================================
// SPARQL 1.0 Syntax Tests
// =============================================================================

/// W3C SPARQL 1.0 syntax tests.
#[test]
fn sparql10_syntax_tests() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
        &[],
    )
}
