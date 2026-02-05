//! Explain API: query optimization plan (Clojure parity)
//!
//! This module builds a user-facing explanation of query planning decisions.
//! It is intended for integration testing parity with `db-clojure`'s `explain-test`.

use crate::error::{ApiError, Result};
use crate::format::iri::IriCompactor;
use crate::query::helpers::parse_sparql_to_ir;
use fluree_db_core::{is_rdf_type, StatsView};
use fluree_db_query::{
    parse_query, ExplainPlan, OptimizationStatus, ParsedQuery, Pattern, Term, TriplePattern,
    VarRegistry,
};
use serde_json::{json, Map, Value as JsonValue};

fn status_to_str(s: OptimizationStatus) -> &'static str {
    match s {
        OptimizationStatus::Reordered => "reordered",
        OptimizationStatus::Unchanged => "unchanged",
    }
}

fn term_to_user_string(term: &Term, vars: &VarRegistry, compactor: &IriCompactor) -> String {
    match term {
        Term::Var(v) => vars.name(*v).to_string(),
        Term::Sid(sid) => {
            // Use @type for rdf:type predicate in user output when it appears in property position.
            // For other Sids, compact as vocab IRI.
            compactor.compact_vocab_iri(
                &compactor
                    .decode_sid(sid)
                    .unwrap_or_else(|_| sid.name.to_string()),
            )
        }
        Term::Iri(iri) => {
            // IRI term (from cross-ledger joins) - compact to user-friendly form
            compactor.compact_vocab_iri(iri)
        }
        Term::Value(v) => match v {
            fluree_db_core::FlakeValue::String(s) => s.clone(),
            _ => format!("{:?}", v),
        },
    }
}

fn triple_pattern_to_user_object(
    tp: &TriplePattern,
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> JsonValue {
    let property = if let Term::Sid(pred) = &tp.p {
        if is_rdf_type(pred) {
            "@type".to_string()
        } else {
            term_to_user_string(&tp.p, vars, compactor)
        }
    } else {
        term_to_user_string(&tp.p, vars, compactor)
    };

    json!({
        "subject": term_to_user_string(&tp.s, vars, compactor),
        "property": property,
        "object": term_to_user_string(&tp.o, vars, compactor),
    })
}

fn plan_patterns_to_json(
    explain: &ExplainPlan,
    triples_in_order: &[TriplePattern],
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> (JsonValue, JsonValue) {
    let mut by_pattern_str: std::collections::HashMap<String, &fluree_db_query::PatternDisplay> =
        std::collections::HashMap::new();

    // Use the ExplainPlan's stable formatting of patterns to correlate inputs/scores.
    for pd in &explain.original_patterns {
        by_pattern_str.insert(pd.pattern.clone(), pd);
    }
    for pd in &explain.optimized_patterns {
        by_pattern_str.insert(pd.pattern.clone(), pd);
    }

    let to_entry = |tp: &TriplePattern| -> JsonValue {
        let key = fluree_db_query::explain::format_pattern(tp);
        let pd = by_pattern_str.get(&key);
        let selectivity = pd.map(|p| p.selectivity_score).unwrap_or(0);
        let pattern_type = pd
            .map(|p| p.pattern_type)
            .unwrap_or(fluree_db_query::planner::PatternType::PropertyScan);

        let typ = match pattern_type {
            fluree_db_query::planner::PatternType::ClassPattern => "class",
            _ => "triple",
        };

        // Inputs: keep close to Clojure's fields, but backed by our explain inputs.
        let mut inputs = Map::new();
        inputs.insert("type".to_string(), JsonValue::String(typ.to_string()));
        // Always include these flags for parity/stability; they will be overwritten
        // when NDV inputs are available.
        inputs.insert("used-values-ndv?".to_string(), json!(false));
        inputs.insert("clamped-to-one?".to_string(), json!(false));
        if let Some(inp) = pd.map(|p| &p.inputs) {
            if let Some(sid) = &inp.property_sid {
                inputs.insert("property-sid".to_string(), json!(sid));
            }
            if let Some(c) = inp.count {
                inputs.insert("count".to_string(), json!(c));
            }
            if let Some(n) = inp.ndv_values {
                inputs.insert("ndv-values".to_string(), json!(n));
                // used-values-ndv? is true if we have NDV and the object is bound constant
                let used = tp.o_bound() && n > 0;
                inputs.insert("used-values-ndv?".to_string(), json!(used));
                if let Some(c) = inp.count {
                    let sel = if n == 0 { 1 } else { c.div_ceil(n).max(1) };
                    let clamped = sel == 1 && c > 0 && n > c;
                    inputs.insert("clamped-to-one?".to_string(), json!(clamped));
                } else {
                    inputs.insert("clamped-to-one?".to_string(), json!(false));
                }
            } else if tp.o_bound() {
                // Clojure parity: these flags are present for bound-object patterns even if NDV
                // stats aren't available (they'll just be false).
                inputs.insert("used-values-ndv?".to_string(), json!(false));
                inputs.insert("clamped-to-one?".to_string(), json!(false));
            }
            if let Some(n) = inp.ndv_subjects {
                inputs.insert("ndv-subjects".to_string(), json!(n));
            }
            if let Some(cc) = inp.class_count {
                inputs.insert("class-count".to_string(), json!(cc));
            }
            inputs.insert("fallback".to_string(), json!(inp.fallback));
        }

        json!({
            "type": typ,
            "pattern": triple_pattern_to_user_object(tp, vars, compactor),
            "selectivity": selectivity,
            "inputs": JsonValue::Object(inputs),
        })
    };

    // Original order is the query's triple pattern order.
    let original = JsonValue::Array(triples_in_order.iter().map(to_entry).collect());

    // Optimized order is ExplainPlan's optimized order.
    let optimized = JsonValue::Array(
        explain
            .optimized_patterns
            .iter()
            .filter_map(|pd| {
                // parse the pd.pattern back into a TriplePattern isn't worth it; instead,
                // find the matching TriplePattern by its formatted string.
                triples_in_order
                    .iter()
                    .find(|tp| fluree_db_query::explain::format_pattern(tp) == pd.pattern)
            })
            .map(to_entry)
            .collect(),
    );

    (original, optimized)
}

/// Shared explain logic operating on an already-parsed query.
///
/// Both JSON-LD and SPARQL entry points parse into `(VarRegistry, ParsedQuery)`
/// and then delegate here.  The `query_echo` value is placed in the `"query"`
/// field of the response (JSON-LD echoes the original JSON object; SPARQL echoes
/// the raw SPARQL string).  `where_clause` is optionally included in the
/// no-stats early-return path (only meaningful for JSON-LD).
fn explain_from_parsed<S: fluree_db_core::Storage + 'static>(
    db: &fluree_db_core::Db<S>,
    vars: &VarRegistry,
    parsed: &ParsedQuery,
    query_echo: JsonValue,
    where_clause: Option<JsonValue>,
) -> Result<JsonValue> {
    let compactor = IriCompactor::new(&db.namespace_codes, &parsed.context);

    // Extract triple patterns in query order.
    // Normalize any `Term::Iri` terms into `Term::Sid` when possible so that
    // stats lookups (which are SID-keyed) work for explain/optimization parity.
    let normalize_term = |t: &Term| -> Term {
        match t {
            Term::Iri(iri) => db
                .encode_iri(iri)
                .map(Term::Sid)
                .unwrap_or_else(|| t.clone()),
            _ => t.clone(),
        }
    };

    let triples_in_order: Vec<TriplePattern> = parsed
        .patterns
        .iter()
        .filter_map(|p| match p {
            Pattern::Triple(tp) => Some(TriplePattern {
                s: normalize_term(&tp.s),
                p: normalize_term(&tp.p),
                o: normalize_term(&tp.o),
                dt: tp.dt.clone(),
                lang: tp.lang.clone(),
            }),
            _ => None,
        })
        .collect();

    let stats_view = db
        .stats
        .as_ref()
        .map(|s| StatsView::from_db_stats_with_namespaces(s, &db.namespace_codes));
    let stats_available = stats_view
        .as_ref()
        .map(|s| s.has_property_stats())
        .unwrap_or(false);

    if !stats_available {
        let mut plan = serde_json::Map::new();
        plan.insert("optimization".into(), json!("none"));
        plan.insert("reason".into(), json!("No statistics available"));
        if let Some(wc) = where_clause {
            plan.insert("where-clause".into(), wc);
        }
        return Ok(json!({
            "query": query_echo,
            "plan": JsonValue::Object(plan)
        }));
    }

    let explain = fluree_db_query::explain_patterns(&triples_in_order, stats_view.as_ref());
    let (original, optimized) =
        plan_patterns_to_json(&explain, &triples_in_order, vars, &compactor);

    // Minimal statistics summary (stable + useful).
    let stats = db.stats.as_ref().unwrap();
    let statistics = json!({
        "total-flakes": stats.flakes,
    });

    Ok(json!({
        "query": query_echo,
        "plan": {
            "optimization": status_to_str(explain.optimization),
            "statistics-available": explain.statistics_available,
            "statistics": statistics,
            "original": original,
            "optimized": optimized
        }
    }))
}

/// Explain a JSON-LD query against a Db.
///
/// Returns a JSON object like:
/// `{ "query": <parsed/echo>, "plan": { ... } }`
pub async fn explain_jsonld<S: fluree_db_core::Storage + 'static>(
    db: &fluree_db_core::Db<S>,
    query_json: &JsonValue,
) -> Result<JsonValue> {
    let mut vars = VarRegistry::new();
    let parsed = parse_query(query_json, db, &mut vars)
        .map_err(|e| ApiError::query(format!("Explain parse error: {e}")))?;

    let query_obj = query_json
        .as_object()
        .ok_or_else(|| ApiError::query("Query must be an object"))?;
    let where_clause = query_obj.get("where").cloned();

    explain_from_parsed(db, &vars, &parsed, query_json.clone(), where_clause)
}

/// Explain a SPARQL query against a Db.
///
/// Returns a JSON object like:
/// `{ "query": "<sparql string>", "plan": { ... } }`
pub async fn explain_sparql<S: fluree_db_core::Storage + 'static>(
    db: &fluree_db_core::Db<S>,
    sparql: &str,
) -> Result<JsonValue> {
    let (vars, parsed) = parse_sparql_to_ir(sparql, db)?;

    explain_from_parsed(db, &vars, &parsed, json!(sparql), None)
}
