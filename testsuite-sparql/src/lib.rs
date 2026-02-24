pub mod evaluator;
pub mod files;
pub mod manifest;
pub mod query_handler;
pub mod report;
pub mod result_comparison;
pub mod result_format;
pub mod sparql_handlers;
pub mod vocab;

use std::sync::OnceLock;

use anyhow::Result;
use tokio::runtime::Runtime;

use evaluator::TestEvaluator;
use manifest::TestManifest;
use report::TestEntry;
use sparql_handlers::register_sparql_tests;

/// Shared Tokio runtime for all test handlers.
///
/// Using a single runtime avoids the overhead of creating 300+ runtimes
/// (one per eval test). The multi-thread scheduler allows `block_on` to
/// be called from multiple test-harness threads concurrently.
pub fn shared_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create shared Tokio runtime")
    })
}

/// Run all tests from the given manifest URL(s).
///
/// Tests listed in `ignored_tests` are expected to fail and won't cause
/// the overall suite to fail. Every other test must pass.
///
/// If the `W3C_REPORT_JSON` environment variable is set, a machine-readable
/// JSON report is written to that path.
pub fn check_testsuite(manifest_url: &str, ignored_tests: &[&str]) -> Result<()> {
    let mut evaluator = TestEvaluator::default();
    register_sparql_tests(&mut evaluator);

    let manifest = TestManifest::new([manifest_url]);
    let results = evaluator.evaluate(manifest)?;

    let mut failures = Vec::new();
    let mut pass_count = 0;
    let mut ignore_count = 0;
    let mut total = 0;
    let mut report_entries = Vec::new();

    for result in &results {
        total += 1;
        let status;
        match &result.outcome {
            Ok(()) => {
                pass_count += 1;
                status = "pass";
            }
            Err(error) => {
                if ignored_tests.contains(&result.test.as_str()) {
                    ignore_count += 1;
                    status = "ignored";
                } else {
                    failures.push(format!("{}: {error:#}", result.test));
                    status = "fail";
                }
            }
        }
        report_entries.push(TestEntry {
            test_id: result.test.clone(),
            status: status.to_string(),
            error: result.outcome.as_ref().err().map(|e| format!("{e:#}")),
        });
    }

    eprintln!(
        "\n=== Test Summary ===\n\
         Total:   {total}\n\
         Passed:  {pass_count}\n\
         Ignored: {ignore_count}\n\
         Failed:  {}\n",
        failures.len()
    );

    // Write JSON report if requested via env var
    if let Ok(report_path) = std::env::var("W3C_REPORT_JSON") {
        report::write_json_report(
            &report_path,
            manifest_url,
            &report_entries,
            total,
            pass_count,
            ignore_count,
            failures.len(),
        )?;
    }

    assert!(
        failures.is_empty(),
        "{} failing test(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );

    Ok(())
}
