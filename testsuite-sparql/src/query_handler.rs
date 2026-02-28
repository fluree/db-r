//! `QueryEvaluationTest` handler: create an in-memory Fluree ledger, load
//! test data, execute a SPARQL query, and compare against expected results.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use fluree_db_api::{format, FlureeBuilder, FormatterConfig, ParsedContext, QueryOutput};

use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::rdfxml;
use crate::result_comparison::{are_results_isomorphic, format_results_diff};
use crate::result_format::{
    fluree_construct_to_sparql_results, fluree_json_to_sparql_results, parse_expected_results,
};
use crate::subprocess::{run_in_subprocess, TestDescriptor};

/// Max time for a single query evaluation test (data load + query + compare).
const EVAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Handler for `mf:QueryEvaluationTest`.
///
/// Runs the test in an isolated subprocess for reliable timeout enforcement.
/// If the test exceeds `EVAL_TIMEOUT`, the subprocess is killed — no zombie
/// threads, no CPU leak.
pub fn evaluate_query_evaluation_test(test: &Test) -> Result<()> {
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
    let graph_data = test.graph_data.clone();

    let descriptor = TestDescriptor::Eval {
        test_id,
        query_url,
        data_url,
        result_url,
        graph_data,
    };

    let result = run_in_subprocess(&descriptor, EVAL_TIMEOUT)?;

    if !result.passed {
        let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
        bail!("{error_msg}");
    }

    Ok(())
}

/// Inner async function that does the actual test work.
///
/// Public for use by the `run-w3c-test` subprocess binary.
pub async fn run_eval_test(
    test_id: &str,
    query_url: &str,
    data_url: Option<&str>,
    result_url: &str,
    graph_data: &[(String, String)],
) -> Result<()> {
    // 1. Create in-memory Fluree + ledger
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("w3c:test")
        .await
        .context("Failed to create test ledger")?;

    // 2. Load default graph data (.ttl or .rdf) if provided.
    //    For .ttl: prepend @base so relative IRIs resolve correctly.
    //    For .rdf: convert RDF/XML to N-Triples (absolute IRIs) first.
    let ledger = if let Some(data_url) = data_url {
        let raw = read_file_to_string(data_url)
            .with_context(|| format!("Reading test data: {data_url}"))?;
        if raw.trim().is_empty() {
            ledger
        } else {
            let turtle = prepare_for_insert(&raw, data_url)?;
            fluree
                .insert_turtle(ledger, &turtle)
                .await
                .with_context(|| format!("Loading test data: {data_url}"))?
                .ledger
        }
    } else {
        ledger
    };

    // 3. Load named graph data if present.
    //    Fluree's Turtle parser does not support TriG GRAPH blocks, so we load
    //    each named graph's data as a separate insert into the default graph.
    //    This means SPARQL GRAPH queries won't find data in the correct named
    //    graph — tests relying on named graph separation will fail. This is a
    //    known limitation until TriG or per-graph loading is supported.
    let ledger = if graph_data.is_empty() {
        ledger
    } else {
        let mut current_ledger = ledger;
        for (_graph_name, graph_url) in graph_data {
            let raw = read_file_to_string(graph_url)
                .with_context(|| format!("Reading named graph data: {graph_url}"))?;
            if !raw.trim().is_empty() {
                let turtle = prepare_for_insert(&raw, graph_url)?;
                current_ledger = fluree
                    .insert_turtle(current_ledger, &turtle)
                    .await
                    .with_context(|| {
                        format!("Loading named graph data: {graph_url} for test {test_id}")
                    })?
                    .ledger;
            }
        }
        current_ledger
    };

    // 4. Read + execute the SPARQL query
    let sparql = read_file_to_string(query_url)
        .with_context(|| format!("Reading query file: {query_url}"))?;

    let query_result = fluree
        .query_sparql(&ledger, &sparql)
        .await
        .with_context(|| format!("Executing SPARQL query for test {test_id}"))?;

    // 5. Parse expected results
    let expected = parse_expected_results(result_url)?;

    // 6. Detect CONSTRUCT vs SELECT/ASK from the parsed query's select mode.
    //    Previous heuristic checked file extension (.ttl/.rdf), but many SPARQL
    //    1.0 SELECT tests use .ttl result files encoded in the DAWG Result Set
    //    vocabulary — not CONSTRUCT graphs. See issue #44.
    let is_construct = matches!(query_result.output, QueryOutput::Construct(_));

    let actual = if is_construct {
        // CONSTRUCT path: format as JSON-LD graph
        let construct_json = query_result
            .to_construct(&ledger.snapshot)
            .map_err(|e| anyhow::anyhow!("Formatting CONSTRUCT result: {e}"))?;
        fluree_construct_to_sparql_results(&construct_json)
            .context("Converting CONSTRUCT output to graph")?
    } else {
        // SELECT/ASK path: format as SPARQL JSON
        let empty_context = ParsedContext::new();
        let config = FormatterConfig::sparql_json().with_select_mode(query_result.output.select_mode());
        let actual_json =
            format::format_results(&query_result, &empty_context, &ledger.snapshot, &config)
                .map_err(|e| anyhow::anyhow!("Formatting SPARQL JSON: {e}"))?;
        fluree_json_to_sparql_results(&actual_json)
            .context("Converting Fluree results to SparqlResults")?
    };

    // 7. Compare
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

/// Prepare file content for insertion into Fluree.
///
/// - `.rdf` files: convert RDF/XML to N-Triples (absolute IRIs, valid Turtle)
/// - All others: prepend `@base` so relative IRIs resolve correctly
fn prepare_for_insert(content: &str, url: &str) -> Result<String> {
    if url.ends_with(".rdf") {
        rdfxml::rdfxml_to_ntriples(content, url)
            .with_context(|| format!("Converting RDF/XML to N-Triples: {url}"))
    } else {
        Ok(format!("@base <{url}> .\n{content}"))
    }
}
