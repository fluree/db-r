use crate::commands::insert::resolve_positional_args;
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use crate::output::{self, OutputFormatKind};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::QueryResult;
use fluree_db_core::{Db, FlakeValue};
use fluree_db_indexer::run_index::BinaryIndexStore;
use fluree_db_query::binding::Binding;
use std::time::Instant;

/// Parse a `--at` value into a `TimeSpec`.
///
/// Accepts:
/// - Integer → `TimeSpec::AtT(n)`
/// - ISO-8601 datetime string (contains `-` and `:`) → `TimeSpec::AtTime(s)`
/// - Otherwise → `TimeSpec::AtCommit(s)` (commit CID prefix)
pub fn parse_time_spec(at: &str) -> fluree_db_api::TimeSpec {
    if let Ok(t) = at.parse::<i64>() {
        fluree_db_api::TimeSpec::at_t(t)
    } else if at.contains('-') && at.contains(':') {
        // Looks like ISO-8601 timestamp (e.g., "2024-01-15T10:30:00Z")
        fluree_db_api::TimeSpec::at_time(at.to_string())
    } else {
        // Treat as commit CID prefix
        fluree_db_api::TimeSpec::at_commit(at.to_string())
    }
}

/// Format a Duration for human display.
///
/// - >= 1s   → "1.23s"
/// - >= 1ms  → "5.2ms"
/// - < 1ms   → "523μs"
fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{:.2}s", secs)
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1_000.0)
    } else {
        format!("{}μs", d.as_micros())
    }
}

/// Format a usize with comma-separated thousands.
fn format_count(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

fn sanitize_tsv_cell(s: &str) -> String {
    // Keep TSV one-row-per-line even when values contain whitespace.
    s.replace(['\t', '\n', '\r'], " ")
}

fn sid_to_iri(db: &Db, sid: &fluree_db_core::Sid) -> String {
    match db.namespace_codes.get(&sid.namespace_code) {
        Some(prefix) => format!("{}{}", prefix, sid.name),
        None => format!("{}:{}", sid.namespace_code, sid.name),
    }
}

fn flake_value_to_cell(v: &FlakeValue, db: &Db) -> String {
    match v {
        FlakeValue::String(s) => sanitize_tsv_cell(s),
        FlakeValue::Ref(sid) => sanitize_tsv_cell(&sid_to_iri(db, sid)),
        other => sanitize_tsv_cell(&other.to_string()),
    }
}

fn binding_to_tsv_cell(b: &Binding, db: &Db, store: Option<&BinaryIndexStore>) -> String {
    match b {
        Binding::Unbound | Binding::Poisoned => String::new(),
        Binding::Sid(sid) => sanitize_tsv_cell(&sid_to_iri(db, sid)),
        Binding::IriMatch { iri, .. } => sanitize_tsv_cell(iri),
        Binding::Iri(iri) => sanitize_tsv_cell(iri),
        Binding::Lit { val, .. } => flake_value_to_cell(val, db),
        Binding::EncodedSid { s_id } => {
            if let Some(store) = store {
                match store.resolve_subject_iri(*s_id) {
                    Ok(iri) => sanitize_tsv_cell(&iri),
                    Err(_) => sanitize_tsv_cell(&format!("{b:?}")),
                }
            } else {
                sanitize_tsv_cell(&format!("{b:?}"))
            }
        }
        Binding::EncodedPid { p_id } => {
            if let Some(store) = store {
                match store.resolve_predicate_iri(*p_id) {
                    Some(iri) => sanitize_tsv_cell(iri),
                    None => sanitize_tsv_cell(&format!("{b:?}")),
                }
            } else {
                sanitize_tsv_cell(&format!("{b:?}"))
            }
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            ..
        } => {
            if let Some(store) = store {
                match store.decode_value(*o_kind, *o_key, *p_id) {
                    Ok(v) => flake_value_to_cell(&v, db),
                    Err(_) => sanitize_tsv_cell(&format!("{b:?}")),
                }
            } else {
                sanitize_tsv_cell(&format!("{b:?}"))
            }
        }
        // Any remaining exotic variants: show debug without materializing full result JSON.
        other => sanitize_tsv_cell(&format!("{other:?}")),
    }
}

fn format_bench_tsv(result: &QueryResult, db: &Db, limit: usize) -> (String, usize) {
    let total_rows = result.row_count();
    let store = result.binary_store.as_deref();
    let headers: Vec<&str> = result.select.iter().map(|&v| result.vars.name(v)).collect();

    let mut out = String::new();
    out.push_str(&headers.join("\t"));
    out.push('\n');

    let mut printed = 0usize;
    'batches: for batch in &result.batches {
        for row in 0..batch.len() {
            for (i, &var) in result.select.iter().enumerate() {
                if i > 0 {
                    out.push('\t');
                }
                let cell = batch
                    .get(row, var)
                    .map(|b| binding_to_tsv_cell(b, db, store))
                    .unwrap_or_default();
                out.push_str(&cell);
            }
            out.push('\n');
            printed += 1;
            if printed >= limit {
                break 'batches;
            }
        }
    }

    (out, total_rows)
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    format_str: &str,
    bench: bool,
    sparql_flag: bool,
    fql_flag: bool,
    at: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    const BENCH_ROWS: usize = 5;
    let limit = if bench { Some(BENCH_ROWS) } else { None };
    let (explicit_ledger, file_path) = resolve_positional_args(args);

    // Resolve input
    let source = input::resolve_input(file_path.as_deref(), expr)?;
    let content = input::read_input(&source)?;

    // Detect query format
    let query_format =
        detect::detect_query_format(file_path.as_deref(), &content, sparql_flag, fql_flag)?;

    // Parse output format
    let output_format = match format_str.to_lowercase().as_str() {
        "json" => OutputFormatKind::Json,
        "table" => OutputFormatKind::Table,
        "csv" => OutputFormatKind::Csv,
        other => {
            return Err(CliError::Usage(format!(
                "unknown output format '{other}'; valid formats: json, table, csv"
            )));
        }
    };

    // Resolve ledger mode: --remote flag, local, or tracked
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(explicit_ledger, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        context::resolve_ledger_mode(explicit_ledger, dirs).await?
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            if at.is_some() {
                return Err(CliError::Usage(
                    "time-travel (--at) is not supported for tracked ledgers".to_string(),
                ));
            }

            // Execute query via remote HTTP
            let timer = Instant::now();
            let result = match query_format {
                detect::QueryFormat::Sparql => client.query_sparql(&remote_alias, &content).await?,
                detect::QueryFormat::JsonLd => {
                    let json_query: serde_json::Value = serde_json::from_str(&content)?;
                    client.query_jsonld(&remote_alias, &json_query).await?
                }
            };
            let elapsed = timer.elapsed();

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            let output = output::format_result(&result, output_format, query_format, limit)?;
            println!("{}", output.text);
            print_footer(output.total_rows, limit, elapsed);
        }
        LedgerMode::Local { fluree, alias } => {
            // Load a single view (optionally time-traveled) and execute against it.
            // This avoids the redundant `fluree.ledger()` load (and duplicate BinaryIndexStore load)
            // that previously occurred before the lazy graph query loaded its own view.
            let view = match at {
                Some(at_str) => {
                    let spec = parse_time_spec(at_str);
                    fluree.view_at(&alias, spec).await?
                }
                None => fluree.view(&alias).await?,
            };

            // Benchmark mode should measure query execution only (not view loading or result formatting).
            // For FQL, we also exclude CLI-side JSON parsing from the timed region.
            let (result, elapsed) = if bench {
                let timer = Instant::now();
                let result = match query_format {
                    detect::QueryFormat::Sparql => {
                        fluree.query_view(&view, content.as_str()).await?
                    }
                    detect::QueryFormat::Fql => {
                        let json_query: serde_json::Value = serde_json::from_str(&content)?;
                        fluree.query_view(&view, &json_query).await?
                    }
                };
                (result, timer.elapsed())
            } else {
                // Default behavior: include view load + query + formatting in the reported time.
                let timer = Instant::now();
                let result = match query_format {
                    detect::QueryFormat::Sparql => {
                        fluree.query_view(&view, content.as_str()).await?
                    }
                    detect::QueryFormat::Fql => {
                        let json_query: serde_json::Value = serde_json::from_str(&content)?;
                        fluree.query_view(&view, &json_query).await?
                    }
                };
                (result, timer.elapsed())
            };

            if bench {
                // Benchmark output should be representative but cheap: show a table preview
                // without materializing full SPARQL JSON / full-result formatting.
                match query_format {
                    detect::QueryFormat::Sparql => {
                        if let Some(output) = output::format_sparql_table_from_result(
                            &result,
                            &view.db,
                            Some(BENCH_ROWS),
                        )? {
                            println!("{}", output.text);
                            print_footer(output.total_rows, Some(BENCH_ROWS), elapsed);
                        } else {
                            // Rare fallback: GROUP BY produces grouped bindings requiring
                            // disaggregation, so fall back to the existing JSON-based formatter.
                            let formatted_json = result.to_sparql_json(&view.db)?;
                            let output = output::format_result(
                                &formatted_json,
                                OutputFormatKind::Table,
                                query_format,
                                Some(BENCH_ROWS),
                            )?;
                            println!("{}", output.text);
                            print_footer(output.total_rows, Some(BENCH_ROWS), elapsed);
                        }
                    }
                    detect::QueryFormat::Fql => {
                        // JSON-LD can be nested; keep bench output in the lightweight TSV form.
                        let (text, total_rows) = format_bench_tsv(&result, &view.db, BENCH_ROWS);
                        print!("{text}");
                        print_footer(total_rows, Some(BENCH_ROWS), elapsed);
                    }
                }
            } else {
                // JSON-LD queries can produce nested graph crawl results; always render as JSON.
                let output_format = if query_format == detect::QueryFormat::Fql {
                    OutputFormatKind::Json
                } else {
                    output_format
                };

                // Fast path: SPARQL default table output (avoid materializing full SPARQL JSON).
                if query_format == detect::QueryFormat::Sparql
                    && output_format == OutputFormatKind::Table
                    && limit.is_none()
                {
                    if let Some(output) =
                        output::format_sparql_table_from_result(&result, &view.db, None)?
                    {
                        println!("{}", output.text);
                        print_footer(output.total_rows, limit, elapsed);
                        return Ok(());
                    }
                }

                // Full formatting path
                let formatted_json = match query_format {
                    detect::QueryFormat::Sparql => result.to_sparql_json(&view.db)?,
                    detect::QueryFormat::Fql => result.to_jsonld_async(&view.db).await?,
                };
                let output =
                    output::format_result(&formatted_json, output_format, query_format, limit)?;
                println!("{}", output.text);
                print_footer(output.total_rows, limit, elapsed);
            }
        }
    }

    Ok(())
}

/// Print the timing/row-count footer line to stderr.
fn print_footer(total_rows: usize, limit: Option<usize>, elapsed: std::time::Duration) {
    let time_str = format_duration(elapsed);
    match limit {
        Some(n) if n < total_rows => {
            eprintln!(
                "(first {} of {} rows, {})",
                format_count(n),
                format_count(total_rows),
                time_str
            );
        }
        _ => {
            eprintln!("({} rows, {})", format_count(total_rows), time_str);
        }
    }
}
