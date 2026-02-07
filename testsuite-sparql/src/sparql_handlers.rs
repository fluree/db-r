use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use fluree_db_sparql::parse_sparql;

use crate::evaluator::TestEvaluator;
use crate::files::read_file_to_string;
use crate::manifest::Test;
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
}

/// Handler for PositiveSyntaxTest / PositiveSyntaxTest11.
///
/// The query file should parse successfully.
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

/// Handler for NegativeSyntaxTest / NegativeSyntaxTest11.
///
/// The query file should fail to parse.
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

/// Run parse_sparql with a timeout to catch infinite loops.
/// Returns Ok(has_errors) or Err if the parser timed out.
fn parse_with_timeout(query_string: &str, test_id: &str) -> Result<bool> {
    let query = query_string.to_string();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let output = parse_sparql(&query);
        let _ = tx.send(output.has_errors());
    });

    match rx.recv_timeout(PARSE_TIMEOUT) {
        Ok(has_errors) => Ok(has_errors),
        Err(_) => {
            bail!("Parser timeout (>{PARSE_TIMEOUT:?}) — likely infinite loop.\nTest: {test_id}")
        }
    }
}
