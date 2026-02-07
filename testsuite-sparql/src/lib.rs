pub mod evaluator;
pub mod files;
pub mod manifest;
pub mod sparql_handlers;
pub mod vocab;

use anyhow::Result;

use evaluator::TestEvaluator;
use manifest::TestManifest;
use sparql_handlers::register_sparql_tests;

/// Run all tests from the given manifest URL(s).
///
/// Tests listed in `ignored_tests` are expected to fail and won't cause
/// the overall suite to fail. Every other test must pass.
pub fn check_testsuite(manifest_url: &str, ignored_tests: &[&str]) -> Result<()> {
    let mut evaluator = TestEvaluator::default();
    register_sparql_tests(&mut evaluator);

    let manifest = TestManifest::new([manifest_url]);
    let results = evaluator.evaluate(manifest)?;

    let mut failures = Vec::new();
    let mut pass_count = 0;
    let mut ignore_count = 0;
    let mut total = 0;

    for result in &results {
        total += 1;
        match &result.outcome {
            Ok(()) => {
                pass_count += 1;
            }
            Err(error) => {
                if ignored_tests.contains(&result.test.as_str()) {
                    ignore_count += 1;
                } else {
                    failures.push(format!("{}: {error:#}", result.test));
                }
            }
        }
    }

    eprintln!(
        "\n=== Test Summary ===\n\
         Total:   {total}\n\
         Passed:  {pass_count}\n\
         Ignored: {ignore_count}\n\
         Failed:  {}\n",
        failures.len()
    );

    assert!(
        failures.is_empty(),
        "{} failing test(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );

    Ok(())
}
