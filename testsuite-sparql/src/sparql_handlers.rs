use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use fluree_db_sparql::{parse_sparql, validate, Capabilities};

use crate::evaluator::TestEvaluator;
use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::query_handler::evaluate_query_evaluation_test;
use crate::vocab::mf;

/// Max time to wait for the SPARQL parser before declaring a timeout.
const PARSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Register all SPARQL test handlers with the evaluator.
pub fn register_sparql_tests(evaluator: &mut TestEvaluator) {
    // Syntax tests (SPARQL 1.0 and 1.1 use the same handlers)
    evaluator.register(mf::POSITIVE_SYNTAX_TEST, evaluate_positive_syntax_test);
    evaluator.register(mf::POSITIVE_SYNTAX_TEST_11, evaluate_positive_syntax_test);
    evaluator.register(mf::NEGATIVE_SYNTAX_TEST, evaluate_negative_syntax_test);
    evaluator.register(mf::NEGATIVE_SYNTAX_TEST_11, evaluate_negative_syntax_test);

    // Update syntax tests — SPARQL UPDATE uses the same parser
    evaluator.register(
        mf::POSITIVE_UPDATE_SYNTAX_TEST_11,
        evaluate_positive_syntax_test,
    );
    evaluator.register(
        mf::NEGATIVE_UPDATE_SYNTAX_TEST_11,
        evaluate_negative_syntax_test,
    );

    // Query evaluation tests
    evaluator.register(mf::QUERY_EVALUATION_TEST, evaluate_query_evaluation_test);

    // Update evaluation tests — not yet implemented
    evaluator.register(mf::UPDATE_EVALUATION_TEST, evaluate_update_evaluation_test);

    // CSV result format tests — not yet implemented
    evaluator.register(mf::CSV_RESULT_FORMAT_TEST, evaluate_csv_result_format_test);

    // Infrastructure tests — not applicable to a database engine
    evaluator.register(mf::PROTOCOL_TEST, evaluate_not_applicable_test);
    evaluator.register(mf::GRAPH_STORE_PROTOCOL_TEST, evaluate_not_applicable_test);
    evaluator.register(mf::SERVICE_DESCRIPTION_TEST, evaluate_not_applicable_test);
}

/// Handler for PositiveSyntaxTest / PositiveSyntaxTest11 / PositiveUpdateSyntaxTest11.
///
/// The query/update file should parse successfully.
fn evaluate_positive_syntax_test(test: &Test) -> Result<()> {
    let query_url = test
        .action
        .as_deref()
        .context("Positive syntax test missing action (query file URL)")?;

    let query_string = read_file_to_string(query_url)
        .with_context(|| format!("Reading query file for test {}", test.id))?;

    let has_errors = parse_with_timeout(&query_string, &test.id)?;

    if has_errors {
        bail!(
            "Positive syntax test failed — parser rejected valid query.\n\
             Test: {}\n\
             File: {query_url}",
            test.id,
        );
    }

    Ok(())
}

/// Handler for NegativeSyntaxTest / NegativeSyntaxTest11 / NegativeUpdateSyntaxTest11.
///
/// The query/update file should fail to parse.
fn evaluate_negative_syntax_test(test: &Test) -> Result<()> {
    let query_url = test
        .action
        .as_deref()
        .context("Negative syntax test missing action (query file URL)")?;

    let query_string = read_file_to_string(query_url)
        .with_context(|| format!("Reading query file for test {}", test.id))?;

    let has_errors = parse_with_timeout(&query_string, &test.id)?;

    ensure!(
        has_errors,
        "Negative syntax test failed — parser accepted invalid query.\n\
         Test: {}\n\
         File: {query_url}",
        test.id,
    );

    Ok(())
}

/// Handler for UpdateEvaluationTest.
///
/// Fluree does not yet support SPARQL UPDATE execution in the test harness.
/// Fails with a descriptive message.
fn evaluate_update_evaluation_test(test: &Test) -> Result<()> {
    bail!(
        "SPARQL UPDATE evaluation not yet implemented.\n\
         Test: {}\n\
         This test type (mf:UpdateEvaluationTest) requires executing SPARQL UPDATE \
         operations and comparing the resulting graph state.",
        test.id,
    )
}

/// Handler for CSVResultFormatTest.
///
/// CSV/TSV result format comparison is not yet implemented.
fn evaluate_csv_result_format_test(test: &Test) -> Result<()> {
    bail!(
        "CSV/TSV result format comparison not yet implemented.\n\
         Test: {}",
        test.id,
    )
}

/// Handler for test types not applicable to a database engine.
///
/// Protocol tests, service description tests, and graph store protocol tests
/// require an HTTP server endpoint and cannot be run as unit tests.
fn evaluate_not_applicable_test(test: &Test) -> Result<()> {
    bail!(
        "Test type not applicable (requires HTTP protocol testing).\n\
         Test: {}\n\
         Types: {:?}",
        test.id,
        test.kinds,
    )
}

/// Run `parse_sparql` + `validate` with a timeout on a dedicated thread.
///
/// Returns `Ok(has_errors)` or `Err` if the parser timed out or panicked.
///
/// Uses `thread::spawn` + `mpsc::recv_timeout` for reliable timeout behavior.
/// This works even when the parser enters an infinite loop because the timeout
/// is checked on the calling thread, independent of the parser thread.
///
/// When a parse times out, the spawned thread continues running until the
/// process exits (Rust cannot kill threads). For a typical test suite run,
/// a handful of timed-out threads is acceptable.
fn parse_with_timeout(query_string: &str, test_id: &str) -> Result<bool> {
    let query = query_string.to_string();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let output = parse_sparql(&query);
        let mut has_errors = output.has_errors();
        // Run validation if parsing produced an AST
        if !has_errors {
            if let Some(ast) = &output.ast {
                let val_diags = validate(ast, &Capabilities::default());
                if val_diags.iter().any(|d| d.is_error()) {
                    has_errors = true;
                }
            }
        }
        let _ = tx.send(has_errors);
    });

    match rx.recv_timeout(PARSE_TIMEOUT) {
        Ok(has_errors) => Ok(has_errors),
        Err(_) => {
            bail!("Parser timeout (>{PARSE_TIMEOUT:?}) — likely infinite loop.\nTest: {test_id}")
        }
    }
}
