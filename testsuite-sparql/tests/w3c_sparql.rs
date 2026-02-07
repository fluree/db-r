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
/// Currently only syntax test handlers are registered, so evaluation
/// tests will fail with "no handler registered". This test is enabled
/// once Phase 2 (query evaluation) is implemented.
#[test]
#[ignore = "Phase 2: requires QueryEvaluationTest handler"]
fn sparql11_query_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
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
