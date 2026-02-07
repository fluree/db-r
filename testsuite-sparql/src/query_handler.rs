//! `QueryEvaluationTest` handler: create an in-memory Fluree ledger, load
//! test data, execute a SPARQL query, and compare against expected results.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use fluree_db_api::{format, FlureeBuilder, FormatterConfig, ParsedContext};

use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::result_comparison::{are_results_isomorphic, format_results_diff};
use crate::result_format::{fluree_json_to_sparql_results, parse_expected_results};

/// Max time for a single query evaluation test (data load + query + compare).
const EVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Handler for `mf:QueryEvaluationTest`.
///
/// 1. Creates an in-memory Fluree instance + ledger
/// 2. Loads the test's default graph data (Turtle)
/// 3. Executes the SPARQL query
/// 4. Converts actual results via `to_sparql_json()` → `SparqlResults`
/// 5. Parses expected results from .srx/.srj file
/// 6. Compares isomorphically
pub fn evaluate_query_evaluation_test(test: &Test) -> Result<()> {
    // Run async Fluree operations inside a dedicated Tokio runtime.
    // One runtime per test is fine — runtime creation is cheap.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?;

    let (tx, rx) = std::sync::mpsc::channel();
    let test_id = test.id.clone();
    let query_url = test
        .query
        .clone()
        .context("QueryEvaluationTest missing qt:query (query file URL)")?;
    let data_url = test.data.clone();
    let result_url = test
        .result
        .clone()
        .context("QueryEvaluationTest missing mf:result (expected result file)")?;

    // Spawn the async work on a separate thread with a timeout
    let handle = std::thread::spawn(move || {
        let outcome = rt.block_on(async {
            run_eval_test(&test_id, &query_url, data_url.as_deref(), &result_url).await
        });
        let _ = tx.send(outcome);
    });

    match rx.recv_timeout(EVAL_TIMEOUT) {
        Ok(outcome) => outcome,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            // Don't join the thread — let it die with the runtime
            drop(handle);
            bail!(
                "Query evaluation test timed out (>{EVAL_TIMEOUT:?}).\nTest: {}",
                test.id
            );
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            bail!("Query evaluation test panicked.\nTest: {}", test.id);
        }
    }
}

/// Inner async function that does the actual test work.
async fn run_eval_test(
    test_id: &str,
    query_url: &str,
    data_url: Option<&str>,
    result_url: &str,
) -> Result<()> {
    // 1. Create in-memory Fluree + ledger
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("w3c:test")
        .await
        .context("Failed to create test ledger")?;

    // 2. Load default graph data (.ttl) if provided
    let ledger = if let Some(data_url) = data_url {
        let turtle = read_file_to_string(data_url)
            .with_context(|| format!("Reading test data: {data_url}"))?;
        if turtle.trim().is_empty() {
            ledger
        } else {
            fluree
                .insert_turtle(ledger, &turtle)
                .await
                .with_context(|| format!("Loading test data: {data_url}"))?
                .ledger
        }
    } else {
        ledger
    };

    // 3. Read + execute the SPARQL query
    let sparql = read_file_to_string(query_url)
        .with_context(|| format!("Reading query file: {query_url}"))?;

    let query_result = fluree
        .query_sparql(&ledger, &sparql)
        .await
        .with_context(|| format!("Executing SPARQL query for test {test_id}"))?;

    // 4. Convert actual results via SPARQL JSON format with an empty context.
    //    An empty context prevents IRI compaction — W3C expected results use full IRIs.
    let empty_context = ParsedContext::new();
    let config = FormatterConfig::sparql_json().with_select_mode(query_result.select_mode);
    let actual_json = format::format_results(&query_result, &empty_context, &ledger.db, &config)
        .map_err(|e| anyhow::anyhow!("Formatting SPARQL JSON: {e}"))?;

    let actual = fluree_json_to_sparql_results(&actual_json)
        .context("Converting Fluree results to SparqlResults")?;

    // 5. Parse expected results
    let expected = parse_expected_results(result_url)?;

    // 6. Compare
    if !are_results_isomorphic(&expected, &actual) {
        let diff = format_results_diff(&expected, &actual);
        bail!(
            "Results not isomorphic.\n\
             Test: {test_id}\n\
             Query: {query_url}\n\
             Expected result: {result_url}\n\n\
             {diff}"
        );
    }

    Ok(())
}
