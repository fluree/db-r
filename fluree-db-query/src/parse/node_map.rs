//! Node-map parsing for WHERE and CONSTRUCT templates
//!
//! JSON-LD node-map format: {"@id": "?s", "ex:name": "?name", ...}
//!
//! This module is shared between WHERE clause parsing and CONSTRUCT template parsing.

use super::ast::{
    UnresolvedIndexSearchPattern, UnresolvedIndexSearchTarget, UnresolvedPathExpr,
    UnresolvedPattern, UnresolvedQuery, UnresolvedTerm, UnresolvedTriplePattern,
    UnresolvedVectorSearchPattern, UnresolvedVectorSearchTarget,
};
use super::error::{ParseError, Result};
use super::PathAliasMap;
use fluree_graph_json_ld::{details, details_with_vocab, ParsedContext, TypeValue};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Result of parsing an index search result specification.
struct IndexSearchResultVars {
    /// The variable for the document/result ID
    id: Arc<str>,
    /// Optional variable for the search score
    score: Option<Arc<str>>,
    /// Optional variable for the ledger alias
    ledger: Option<Arc<str>>,
}

/// Shared context for parsing properties within a WHERE clause.
///
/// Bundles the common parameters needed for property parsing to reduce
/// argument count in recursive calls.
struct PropertyParseContext<'a> {
    /// JSON-LD context for IRI expansion
    context: &'a ParsedContext,
    /// Path alias mappings from @context
    path_aliases: &'a PathAliasMap,
    /// Counter for generating unique nested pattern variables
    nested_counter: &'a mut u32,
    /// Whether to allow variable objects (for history queries)
    object_var_parsing: bool,
}

/// rdf:type IRI constant (re-exported from vocab crate for convenience)
pub(crate) use fluree_vocab::rdf::TYPE as RDF_TYPE;
use fluree_vocab::xsd;

/// Check if a string is a variable (starts with '?')
pub(super) fn is_variable(s: &str) -> bool {
    s.starts_with('?')
}

// ============================================================================
// Helper functions to reduce duplication in parse_property
// ============================================================================

/// Build a triple pattern handling reverse predicates and optional datatype.
///
/// This consolidates the repeated pattern of:
/// - Reversing subject/object for @reverse predicates
/// - Adding datatype constraint when present
#[inline]
fn build_triple_pattern(
    subject: &UnresolvedTerm,
    predicate: UnresolvedTerm,
    object: UnresolvedTerm,
    is_reverse: bool,
    dt_iri: Option<&str>,
) -> UnresolvedTriplePattern {
    if is_reverse {
        UnresolvedTriplePattern::new(object, predicate, subject.clone())
    } else if let Some(dt) = dt_iri {
        UnresolvedTriplePattern::with_dt(subject.clone(), predicate, object, dt)
    } else {
        UnresolvedTriplePattern::new(subject.clone(), predicate, object)
    }
}

/// Add a BIND pattern for a metadata function (datatype, t, op, lang).
///
/// Creates: `BIND(func(?object_var) AS ?bind_var)`
///
/// Returns `Ok(true)` if the pattern was added (caller should mark pattern_added),
/// or an error if the object is not a variable.
fn add_metadata_bind_pattern(
    func_name: &str,
    bind_var: Arc<str>,
    object: &UnresolvedTerm,
    query: &mut UnresolvedQuery,
    pattern: &UnresolvedTriplePattern,
    pattern_added: &mut bool,
    error_context: &str,
) -> Result<()> {
    use crate::parse::ast::UnresolvedFilterExpr;

    if !object.is_var() {
        return Err(ParseError::InvalidWhere(format!(
            "{} requires @value to be a variable",
            error_context
        )));
    }

    let func_expr = UnresolvedFilterExpr::Function {
        name: Arc::from(func_name),
        args: vec![UnresolvedFilterExpr::var(object.as_var().unwrap())],
    };
    let bind_pattern = UnresolvedPattern::Bind {
        var: bind_var,
        expr: func_expr,
    };

    if !*pattern_added {
        query.add_pattern(pattern.clone());
        *pattern_added = true;
    }
    query.patterns.push(bind_pattern);

    Ok(())
}

/// Add a FILTER pattern for a constant comparison (e.g., `op(?val) = "assert"`).
///
/// Creates: `FILTER(func(?object_var) = constant_value)`
fn add_metadata_filter_pattern(
    func_name: &str,
    constant_value: &str,
    object: &UnresolvedTerm,
    query: &mut UnresolvedQuery,
    pattern: &UnresolvedTriplePattern,
    pattern_added: &mut bool,
    error_context: &str,
) -> Result<()> {
    use crate::parse::ast::{UnresolvedCompareOp, UnresolvedFilterExpr};

    if !object.is_var() {
        return Err(ParseError::InvalidWhere(format!(
            "{} requires @value to be a variable",
            error_context
        )));
    }

    let func_expr = UnresolvedFilterExpr::Function {
        name: Arc::from(func_name),
        args: vec![UnresolvedFilterExpr::var(object.as_var().unwrap())],
    };
    let filter_expr = UnresolvedFilterExpr::Compare {
        op: UnresolvedCompareOp::Eq,
        left: Box::new(func_expr),
        right: Box::new(UnresolvedFilterExpr::string(constant_value)),
    };
    let filter_pattern = UnresolvedPattern::Filter(filter_expr);

    if !*pattern_added {
        query.add_pattern(pattern.clone());
        *pattern_added = true;
    }
    query.patterns.push(filter_pattern);

    Ok(())
}

/// Check if a node-map is a BM25 index search pattern.
///
/// Index search patterns have a "graph" key and at least one of:
/// idx:target, idx:limit, idx:result
/// But NOT idx:vector or idx:metric (those are vector search patterns)
fn is_index_search_pattern(map: &serde_json::Map<String, JsonValue>) -> bool {
    map.contains_key("graph")
        && (map.contains_key("idx:target")
            || map.contains_key("idx:limit")
            || map.contains_key("idx:result"))
        && !map.contains_key("idx:vector")
        && !map.contains_key("idx:metric")
}

/// Check if a node-map is a vector search pattern.
///
/// Vector search patterns have "idx:graph" or "graph" and idx:vector
fn is_vector_search_pattern(map: &serde_json::Map<String, JsonValue>) -> bool {
    (map.contains_key("idx:graph") || map.contains_key("graph"))
        && (map.contains_key("idx:vector") || map.contains_key("idx:metric"))
}

/// Parse an index search pattern from a node-map.
///
/// Index search pattern syntax:
/// ```json
/// {
///   "graph": "my-search:main",
///   "idx:target": "software engineer",
///   "idx:limit": 10,
///   "idx:result": "?doc"
/// }
/// ```
///
/// Or with nested result:
/// ```json
/// {
///   "graph": "my-search:main",
///   "idx:target": "software engineer",
///   "idx:result": {
///     "idx:id": "?doc",
///     "idx:score": "?score",
///     "idx:ledger": "?source"
///   }
/// }
/// ```
fn parse_index_search_pattern(
    map: &serde_json::Map<String, JsonValue>,
    query: &mut UnresolvedQuery,
) -> Result<()> {
    // Extract "graph" - the virtual graph alias (required)
    let vg_alias = map.get("graph").and_then(|v| v.as_str()).ok_or_else(|| {
        ParseError::InvalidWhere("index search: 'graph' must be a string".to_string())
    })?;

    // Extract "idx:target" - the search query (required)
    let target_val = map.get("idx:target").ok_or_else(|| {
        ParseError::InvalidWhere("index search: 'idx:target' is required".to_string())
    })?;

    let target = match target_val.as_str() {
        Some(s) if is_variable(s) => UnresolvedIndexSearchTarget::Var(Arc::from(s)),
        Some(s) => UnresolvedIndexSearchTarget::Const(Arc::from(s)),
        None => {
            return Err(ParseError::InvalidWhere(
                "index search: 'idx:target' must be a string or variable".to_string(),
            ));
        }
    };

    // Extract "idx:limit" (optional)
    let limit = map
        .get("idx:limit")
        .and_then(|v| v.as_u64().map(|n| n as usize));

    // Extract "idx:result" (required) - can be a variable or nested object
    let result_val = map.get("idx:result").ok_or_else(|| {
        ParseError::InvalidWhere("index search: 'idx:result' is required".to_string())
    })?;

    let result_vars = parse_index_search_result(result_val)?;

    // Extract "idx:sync" (optional, default false)
    let sync = map
        .get("idx:sync")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract "idx:timeout" (optional)
    let timeout = map.get("idx:timeout").and_then(|v| v.as_u64());

    let mut pattern = UnresolvedIndexSearchPattern::new(vg_alias, target, result_vars.id.as_ref());

    if let Some(limit) = limit {
        pattern = pattern.with_limit(limit);
    }
    if let Some(sv) = result_vars.score {
        pattern = pattern.with_score_var(sv.as_ref());
    }
    if let Some(lv) = result_vars.ledger {
        pattern = pattern.with_ledger_var(lv.as_ref());
    }
    if sync {
        pattern = pattern.with_sync(true);
    }
    if let Some(t) = timeout {
        pattern = pattern.with_timeout(t);
    }

    query.patterns.push(UnresolvedPattern::IndexSearch(pattern));
    Ok(())
}

/// Parse a vector search pattern from a node-map.
///
/// Vector search pattern syntax:
/// ```json
/// {
///   "idx:graph": "embeddings:main",
///   "idx:vector": [0.1, 0.2, 0.3],
///   "idx:metric": "cosine",
///   "idx:limit": 10,
///   "idx:result": "?doc"
/// }
/// ```
///
/// Or with variable vector:
/// ```json
/// {
///   "idx:graph": "embeddings:main",
///   "idx:vector": "?queryVec",
///   "idx:metric": "dot",
///   "idx:result": {"idx:id": "?doc", "idx:score": "?score"}
/// }
/// ```
fn parse_vector_search_pattern(
    map: &serde_json::Map<String, JsonValue>,
    query: &mut UnresolvedQuery,
) -> Result<()> {
    // Extract graph alias (required) - support both "idx:graph" and "graph"
    let vg_alias = map
        .get("idx:graph")
        .or_else(|| map.get("graph"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ParseError::InvalidWhere("vector search: 'idx:graph' must be a string".to_string())
        })?;

    // Extract "idx:vector" - the query vector (required)
    let vector_val = map.get("idx:vector").ok_or_else(|| {
        ParseError::InvalidWhere("vector search: 'idx:vector' is required".to_string())
    })?;

    let target = match vector_val {
        JsonValue::String(s) if is_variable(s) => {
            UnresolvedVectorSearchTarget::Var(Arc::from(s.as_str()))
        }
        JsonValue::Array(arr) => {
            // Parse as constant vector
            let mut vec = Vec::with_capacity(arr.len());
            for v in arr {
                let num = v.as_f64().ok_or_else(|| {
                    ParseError::InvalidWhere(
                        "vector search: idx:vector array must contain numbers".to_string(),
                    )
                })?;
                vec.push(num as f32);
            }
            UnresolvedVectorSearchTarget::Const(vec)
        }
        _ => {
            return Err(ParseError::InvalidWhere(
                "vector search: 'idx:vector' must be a variable or array of numbers".to_string(),
            ));
        }
    };

    // Extract "idx:metric" (optional, defaults to "cosine")
    let metric = map
        .get("idx:metric")
        .and_then(|v| v.as_str())
        .unwrap_or("cosine");

    // Extract "idx:limit" (optional)
    let limit = map
        .get("idx:limit")
        .and_then(|v| v.as_u64().map(|n| n as usize));

    // Extract "idx:result" (required) - can be a variable or nested object
    let result_val = map.get("idx:result").ok_or_else(|| {
        ParseError::InvalidWhere("vector search: 'idx:result' is required".to_string())
    })?;

    let result_vars = parse_index_search_result(result_val)?;

    // Extract "idx:sync" (optional, default false)
    let sync = map
        .get("idx:sync")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract "idx:timeout" (optional)
    let timeout = map.get("idx:timeout").and_then(|v| v.as_u64());

    let mut pattern =
        UnresolvedVectorSearchPattern::new(vg_alias, target, metric, result_vars.id.as_ref());

    if let Some(limit) = limit {
        pattern = pattern.with_limit(limit);
    }
    if let Some(sv) = result_vars.score {
        pattern = pattern.with_score_var(sv.as_ref());
    }
    if let Some(lv) = result_vars.ledger {
        pattern = pattern.with_ledger_var(lv.as_ref());
    }
    if sync {
        pattern = pattern.with_sync(true);
    }
    if let Some(t) = timeout {
        pattern = pattern.with_timeout(t);
    }

    query
        .patterns
        .push(UnresolvedPattern::VectorSearch(pattern));
    Ok(())
}

/// Parse the idx:result value (variable or nested object with id/score/ledger).
fn parse_index_search_result(result_val: &JsonValue) -> Result<IndexSearchResultVars> {
    match result_val {
        // Simple variable: "?doc"
        JsonValue::String(s) => {
            if !is_variable(s) {
                return Err(ParseError::InvalidWhere(
                    "index search: idx:result variable must start with ?".to_string(),
                ));
            }
            Ok(IndexSearchResultVars {
                id: Arc::from(s.as_str()),
                score: None,
                ledger: None,
            })
        }
        // Nested object: {"idx:id": "?doc", "idx:score": "?score", ...}
        JsonValue::Object(obj) => {
            let id_str = obj.get("idx:id").and_then(|v| v.as_str()).ok_or_else(|| {
                ParseError::InvalidWhere(
                    "index search: nested idx:result must have 'idx:id'".to_string(),
                )
            })?;

            if !is_variable(id_str) {
                return Err(ParseError::InvalidWhere(
                    "index search: idx:id must be a variable".to_string(),
                ));
            }

            let score_var = obj.get("idx:score").and_then(|v| v.as_str()).map(|s| {
                if !is_variable(s) {
                    // Return an error if not a variable - but for now, just ignore
                    // In a real impl, we'd want proper error handling here
                }
                Arc::from(s)
            });

            let ledger_var = obj.get("idx:ledger").and_then(|v| v.as_str()).map(|s| {
                if !is_variable(s) {
                    // Same note as above
                }
                Arc::from(s)
            });

            Ok(IndexSearchResultVars {
                id: Arc::from(id_str),
                score: score_var,
                ledger: ledger_var,
            })
        }
        _ => Err(ParseError::InvalidWhere(
            "index search: idx:result must be a variable or object".to_string(),
        )),
    }
}

/// Parse a node-map (single where clause object)
///
/// Extracts triple patterns from JSON-LD node-map format.
/// Used for both WHERE clauses and CONSTRUCT templates.
pub fn parse_node_map(
    map: &serde_json::Map<String, JsonValue>,
    context: &ParsedContext,
    path_aliases: &PathAliasMap,
    query: &mut UnresolvedQuery,
    subject_counter: &mut u32,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<()> {
    // Check for vector search pattern first (has idx:vector)
    if is_vector_search_pattern(map) {
        return parse_vector_search_pattern(map, query);
    }

    // Check for BM25 index search pattern (has "graph" + idx:target)
    if is_index_search_pattern(map) {
        return parse_index_search_pattern(map, query);
    }

    // Determine subject: explicit @id (or aliased @id) or generated unique variable
    let subject = if let Some(id_val) = map.get("@id").or_else(|| map.get(context.id_key.as_str()))
    {
        parse_subject(id_val, context)?
    } else {
        // Generate unique implicit subject variable with reserved prefix to avoid collision
        // with user-provided variables (e.g. ?__s0, ?__s1, etc.)
        let var_name = format!("?__s{}", *subject_counter);
        *subject_counter += 1;
        UnresolvedTerm::var(&var_name)
    };

    // Process each property in the node-map
    for (key, value) in map {
        // Skip @id (already processed; include aliased @id)
        if key == "@id" || key == context.id_key.as_str() {
            continue;
        }

        // Nested @context in node-maps is not supported
        if key == "@context" || key == "context" {
            return Err(ParseError::InvalidWhere(
                "nested @context in where clause is not supported; define context at the query root"
                    .to_string(),
            ));
        }

        // Handle @type specially
        if key == "@type" || key == "type" || Some(key.as_str()) == context.type_key.as_str().into()
        {
            parse_type_property(value, &subject, context, query, object_var_parsing)?;
            continue;
        }

        // Regular property
        let mut ctx = PropertyParseContext {
            context,
            path_aliases,
            nested_counter,
            object_var_parsing,
        };
        parse_property(key, value, &subject, query, &mut ctx)?;
    }

    Ok(())
}

/// Parse the subject (@id value)
fn parse_subject(id_val: &JsonValue, context: &ParsedContext) -> Result<UnresolvedTerm> {
    let id_str = id_val
        .as_str()
        .ok_or_else(|| ParseError::InvalidWhere("@id must be a string".to_string()))?;

    if is_variable(id_str) {
        Ok(UnresolvedTerm::var(id_str))
    } else {
        // Expand IRI using context with vocab=false to use @base for subject IRIs
        let (expanded, _) = details_with_vocab(id_str, context, false);
        Ok(UnresolvedTerm::iri(expanded))
    }
}

/// Parse @type property
fn parse_type_property(
    value: &JsonValue,
    subject: &UnresolvedTerm,
    context: &ParsedContext,
    query: &mut UnresolvedQuery,
    object_var_parsing: bool,
) -> Result<()> {
    let predicate = UnresolvedTerm::iri(RDF_TYPE);

    match value {
        JsonValue::String(s) => {
            let object = parse_object_value(s, context, object_var_parsing)?;
            query.add_pattern(UnresolvedTriplePattern::new(
                subject.clone(),
                predicate,
                object,
            ));
        }
        JsonValue::Array(arr) => {
            // Multiple types - all items must be strings
            for item in arr {
                let s = item.as_str().ok_or_else(|| {
                    ParseError::InvalidWhere("@type array items must be strings".to_string())
                })?;
                let object = parse_object_value(s, context, object_var_parsing)?;
                query.add_pattern(UnresolvedTriplePattern::new(
                    subject.clone(),
                    predicate.clone(),
                    object,
                ));
            }
        }
        _ => {
            return Err(ParseError::InvalidWhere(
                "@type must be a string or array of strings".to_string(),
            ));
        }
    }

    Ok(())
}

/// Parse a regular property
fn parse_property(
    key: &str,
    value: &JsonValue,
    subject: &UnresolvedTerm,
    query: &mut UnresolvedQuery,
    ctx: &mut PropertyParseContext<'_>,
) -> Result<()> {
    // Check if key is a @path alias from @context
    if let Some(path_expr) = ctx.path_aliases.get(key) {
        return parse_path_alias_usage(path_expr, value, subject, ctx.context, query);
    }

    // Check if predicate is a variable (e.g., "?p")
    let (predicate, context_entry, is_reverse) = if is_variable(key) {
        // Clojure parity: variable predicates must bind to references (variables or @id),
        // not literal constants. In history mode, allow value objects with @value (variable)
        // plus metadata bindings like @t/@op.
        let is_valid_object = match value {
            JsonValue::String(s) if is_variable(s) => true,
            JsonValue::Object(map) if map.contains_key("@id") => true,
            JsonValue::Object(map) if map.contains_key("@value") => match map.get("@value") {
                Some(JsonValue::String(s)) if is_variable(s) => true,
                Some(JsonValue::Object(o)) if o.contains_key("@id") => true,
                _ => false,
            },
            _ => false,
        };
        if !is_valid_object {
            return Err(ParseError::InvalidWhere(
                "variable predicate requires object to be a variable or @id reference".to_string(),
            ));
        }
        (UnresolvedTerm::var(key), None, false)
    } else {
        // Expand the property IRI and get context entry
        let (expanded_iri, entry) = details(key, ctx.context);

        // If the term is defined with @reverse in @context, interpret this predicate as reversed:
        // {"@id":"?s","parent":"?x"} where parent is @reverse ex:child
        // becomes (?x ex:child ?s)
        let (pred_iri, is_reverse) = entry
            .as_ref()
            .and_then(|e| e.reverse.as_ref())
            .map(|rev| {
                // JSON-LD allows "@reverse": "@type" as a special keyword mapping.
                // Our engine represents @type as rdf:type.
                if rev == "@type" || rev == "type" || rev.as_str() == ctx.context.type_key {
                    (RDF_TYPE.to_string(), true)
                } else {
                    (rev.clone(), true)
                }
            })
            .unwrap_or((expanded_iri, false));

        (UnresolvedTerm::iri(&pred_iri), entry, is_reverse)
    };

    // Determine if this property is typed as @id (reference)
    let mut is_ref_type = context_entry
        .as_ref()
        .and_then(|e| e.type_.as_ref())
        .is_some_and(|t| matches!(t, TypeValue::Id));

    // rdf:type always expects an IRI object, even if not annotated in @context.
    if matches!(predicate, UnresolvedTerm::Iri(ref iri) if iri.as_ref() == RDF_TYPE) {
        is_ref_type = true;
    }

    // Get datatype IRI from context if present
    let dt_iri: Option<Arc<str>> = context_entry.as_ref().and_then(|e| {
        e.type_.as_ref().and_then(|t| {
            if let TypeValue::Iri(iri) = t {
                Some(Arc::from(iri.as_str()))
            } else {
                None
            }
        })
    });

    // Handle value objects like {"@value": ..., "@type": ..., "@language": ..., "@t": ...} (typed literals in WHERE)
    if let JsonValue::Object(obj) = value {
        if obj.contains_key("@value") || obj.contains_key("@language") {
            let parsed = parse_value_object(obj, ctx.context, ctx.object_var_parsing)?;
            let object = parsed.term;
            let pattern_dt = parsed.dt_iri.or(dt_iri);

            // Build the triple pattern using helper
            let mut pattern = build_triple_pattern(
                subject,
                predicate,
                object.clone(),
                is_reverse,
                pattern_dt.as_deref(),
            );

            // Track whether we've added the pattern (to avoid double-adding)
            let mut pattern_added = false;

            // Handle @type variable: BIND(datatype(?val) AS ?type)
            if let Some(dt_var) = parsed.dt_var {
                add_metadata_bind_pattern(
                    "datatype",
                    dt_var,
                    &object,
                    query,
                    &pattern,
                    &mut pattern_added,
                    "@type variable binding",
                )?;
            }

            // Handle @t variable: BIND(t(?val) AS ?t)
            if let Some(t_var) = parsed.t_var {
                add_metadata_bind_pattern(
                    "t",
                    t_var,
                    &object,
                    query,
                    &pattern,
                    &mut pattern_added,
                    "@t variable binding",
                )?;
            }

            // Handle @op: variable creates BIND, constant creates FILTER
            if let Some(op_var) = parsed.op_var {
                if is_variable(&op_var) {
                    // Variable binding: BIND(op(?val) AS ?op)
                    add_metadata_bind_pattern(
                        "op",
                        op_var,
                        &object,
                        query,
                        &pattern,
                        &mut pattern_added,
                        "@op variable binding",
                    )?;
                } else {
                    // Constant filter: FILTER(op(?val) = "assert")
                    add_metadata_filter_pattern(
                        "op",
                        &op_var,
                        &object,
                        query,
                        &pattern,
                        &mut pattern_added,
                        "@op filter",
                    )?;
                }
            }

            // Handle @language: variable creates BIND, constant adds constraint
            if let Some(lang) = parsed.lang {
                if is_variable(&lang) {
                    // BIND(LANG(?val) AS ?lang)
                    add_metadata_bind_pattern(
                        "lang",
                        lang,
                        &object,
                        query,
                        &pattern,
                        &mut pattern_added,
                        "@language variable binding",
                    )?;
                } else {
                    // Constant language constraint
                    pattern = pattern.with_lang(lang.as_ref());
                }
            }

            // Add pattern if not already added by any of the above
            if !pattern_added {
                query.add_pattern(pattern);
            }

            return Ok(());
        }
    }

    // Handle nested node-maps (object values)
    if let JsonValue::Object(nested_map) = value {
        if nested_map.contains_key("@variable") {
            // Explicit variable wrapper should be treated as a value.
        } else {
            // Determine the nested subject:
            // - If nested object has an explicit @id, use it (var or IRI).
            // - Otherwise generate an implicit variable (?__n0, ?__n1, ...).
            //
            // IMPORTANT: If we used a generated var while the nested object has an explicit @id,
            // we'd break correlation between the connecting triple and the nested properties.
            let nested_subject = if let Some(id_val) = nested_map.get("@id") {
                parse_subject(id_val, ctx.context)?
            } else {
                let nested_subject_name = format!("?__n{}", *ctx.nested_counter);
                *ctx.nested_counter += 1;
                UnresolvedTerm::var(&nested_subject_name)
            };

            // ORDERING: Emit connecting triple FIRST.
            // This ensures deterministic join order for the planner.
            let connecting_pattern = build_triple_pattern(
                subject,
                predicate,
                nested_subject.clone(),
                is_reverse,
                dt_iri.as_deref(),
            );
            query.add_pattern(connecting_pattern);

            // Parse nested object's properties (after connecting triple)
            parse_nested_node_map(
                nested_map,
                &nested_subject,
                ctx.context,
                ctx.path_aliases,
                query,
                ctx.nested_counter,
                ctx.object_var_parsing,
            )?;

            return Ok(());
        }
    }

    // Parse the object value (non-nested case)
    let object = parse_json_value(value, is_ref_type, ctx.context, ctx.object_var_parsing)?;

    // Create and add the pattern
    let pattern = build_triple_pattern(subject, predicate, object, is_reverse, dt_iri.as_deref());
    query.add_pattern(pattern);

    Ok(())
}

/// Normalize numeric datatypes to canonical form for matching
///
/// Delegates to the shared helper in fluree-vocab which normalizes:
/// - xsd:int, xsd:short, xsd:byte, xsd:long → xsd:integer
/// - xsd:float → xsd:double
#[inline]
fn normalize_numeric_datatype(expanded_dt_iri: &str) -> &str {
    xsd::normalize_numeric_datatype(expanded_dt_iri)
}

/// Parsed value object result
struct ParsedValueObject {
    /// The parsed term (value or variable)
    term: UnresolvedTerm,
    /// Explicit datatype IRI (constant)
    dt_iri: Option<Arc<str>>,
    /// Language tag or variable
    lang: Option<Arc<str>>,
    /// Datatype variable (if @type is "?var")
    dt_var: Option<Arc<str>>,
    /// Transaction time variable (if @t is "?var")
    t_var: Option<Arc<str>>,
    /// Operation variable (if @op is "?var") - for history queries
    /// Binds to "assert" or "retract" indicating the flake operation
    op_var: Option<Arc<str>>,
}

fn parse_value_object(
    obj: &serde_json::Map<String, JsonValue>,
    context: &ParsedContext,
    object_var_parsing: bool,
) -> Result<ParsedValueObject> {
    let value_val = obj
        .get("@value")
        .ok_or_else(|| ParseError::InvalidWhere("value object must contain @value".to_string()))?;

    // Optional @type - can be a constant IRI or a variable like "?type"
    let explicit_dt_raw: Option<Arc<str>> =
        obj.get("@type").and_then(|t| t.as_str()).map(Arc::from);

    let (explicit_dt, explicit_dt_var): (Option<Arc<str>>, Option<Arc<str>>) =
        if let Some(ref dt) = explicit_dt_raw {
            if is_variable(dt) {
                // @type is a variable like "?type" - we'll bind it with DATATYPE() function
                (None, Some(dt.clone()))
            } else if dt.as_ref() == "@id" {
                (Some(Arc::from("@id")), None)
            } else {
                // @type is a constant IRI - expand and normalize it
                let (expanded, _) = details(dt, context);
                (
                    Some(Arc::from(normalize_numeric_datatype(expanded.as_str()))),
                    None,
                )
            }
        } else {
            (None, None)
        };

    // Optional @language (can be a constant string or a variable like "?lang")
    let explicit_lang: Option<Arc<str>> =
        obj.get("@language").and_then(|l| l.as_str()).map(Arc::from);

    // Optional @t - Fluree-specific transaction time binding (must be a variable like "?t")
    let explicit_t_var: Option<Arc<str>> = if let Some(t_val) = obj.get("@t") {
        let t_str = t_val.as_str().ok_or_else(|| {
            ParseError::InvalidWhere("@t must be a string variable (e.g., \"?t\")".to_string())
        })?;
        if is_variable(t_str) {
            Some(Arc::from(t_str))
        } else {
            return Err(ParseError::InvalidWhere(
                "@t must be a variable (e.g., \"?t\"), not a constant value".to_string(),
            ));
        }
    } else {
        None
    };

    // Optional @op - Fluree-specific operation binding for history queries (must be a variable like "?op")
    // In history mode, this binds to "assert" or "retract" indicating the flake's operation type.
    // Can also be a constant "assert" or "retract" to filter by operation type.
    let explicit_op_var: Option<Arc<str>> = if let Some(op_val) = obj.get("@op") {
        let op_str = op_val.as_str().ok_or_else(|| {
            ParseError::InvalidWhere(
                "@op must be a string (variable like \"?op\" or constant \"assert\"/\"retract\")"
                    .to_string(),
            )
        })?;
        if is_variable(op_str) {
            Some(Arc::from(op_str))
        } else if op_str == "assert" || op_str == "retract" {
            // Constant filter value - store as-is for filter generation
            Some(Arc::from(op_str))
        } else {
            return Err(ParseError::InvalidWhere(
                "@op must be a variable (e.g., \"?op\") or one of \"assert\", \"retract\""
                    .to_string(),
            ));
        }
    } else {
        None
    };

    // If @type is @id, treat @value as IRI/ref
    if matches!(explicit_dt.as_deref(), Some("@id")) {
        // @t is not supported with @type: "@id" (ref objects don't carry t in bindings)
        if explicit_t_var.is_some() {
            return Err(ParseError::InvalidWhere(
                "@t binding is not supported with @type: \"@id\"; @t only applies to literal values".to_string()
            ));
        }
        // @op is not supported with @type: "@id" (ref objects don't carry op in bindings)
        if explicit_op_var.is_some() {
            return Err(ParseError::InvalidWhere(
                "@op binding is not supported with @type: \"@id\"; @op only applies to literal values".to_string()
            ));
        }
        let s = value_val.as_str().ok_or_else(|| {
            ParseError::InvalidWhere("@value must be a string when @type is @id".to_string())
        })?;
        return Ok(ParsedValueObject {
            term: parse_object_value(s, context, object_var_parsing)?,
            dt_iri: None,
            lang: None,
            dt_var: None,
            t_var: None,
            op_var: None,
        });
    }

    // Otherwise parse @value as a literal (or a variable string like "?x")
    let term = match value_val {
        JsonValue::String(s) => {
            if is_variable(s) {
                UnresolvedTerm::var(s)
            } else {
                UnresolvedTerm::string(s)
            }
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                UnresolvedTerm::long(i)
            } else if let Some(f) = n.as_f64() {
                UnresolvedTerm::double(f)
            } else {
                return Err(ParseError::InvalidWhere(format!(
                    "Unsupported number type in @value: {}",
                    n
                )));
            }
        }
        JsonValue::Bool(b) => UnresolvedTerm::boolean(*b),
        _ => {
            return Err(ParseError::InvalidWhere(
                "@value must be string/number/bool".to_string(),
            ))
        }
    };

    Ok(ParsedValueObject {
        term,
        dt_iri: explicit_dt,
        lang: explicit_lang,
        dt_var: explicit_dt_var,
        t_var: explicit_t_var,
        op_var: explicit_op_var,
    })
}

/// Parse a nested node-map (object value in a property)
///
/// This is similar to `parse_node_map` but uses an already-determined subject
/// (the nested subject variable generated by the parent property).
fn parse_nested_node_map(
    map: &serde_json::Map<String, JsonValue>,
    subject: &UnresolvedTerm,
    context: &ParsedContext,
    path_aliases: &PathAliasMap,
    query: &mut UnresolvedQuery,
    nested_counter: &mut u32,
    object_var_parsing: bool,
) -> Result<()> {
    // Check for explicit @id in nested object - it overrides the generated subject
    let actual_subject = if let Some(id_val) = map.get("@id") {
        parse_subject(id_val, context)?
    } else {
        subject.clone()
    };

    // Process each property in the nested node-map
    for (key, value) in map {
        // Skip @id (already processed)
        if key == "@id" {
            continue;
        }

        // Nested @context is not supported
        if key == "@context" || key == "context" {
            return Err(ParseError::InvalidWhere(
                "nested @context in where clause is not supported; define context at the query root"
                    .to_string(),
            ));
        }

        // Handle @type specially
        if key == "@type" || key == "type" || Some(key.as_str()) == context.type_key.as_str().into()
        {
            parse_type_property(value, &actual_subject, context, query, object_var_parsing)?;
            continue;
        }

        // Regular property (may be recursively nested)
        let mut ctx = PropertyParseContext {
            context,
            path_aliases,
            nested_counter,
            object_var_parsing,
        };
        parse_property(key, value, &actual_subject, query, &mut ctx)?;
    }

    Ok(())
}

/// Parse an object value (string that might be a variable or IRI)
fn parse_object_value(
    s: &str,
    context: &ParsedContext,
    object_var_parsing: bool,
) -> Result<UnresolvedTerm> {
    if is_variable(s) {
        if object_var_parsing {
            Ok(UnresolvedTerm::var(s))
        } else {
            Ok(UnresolvedTerm::string(s))
        }
    } else {
        // Expand as IRI
        let (expanded, _) = details(s, context);
        Ok(UnresolvedTerm::iri(expanded))
    }
}

/// Parse a JSON value to an UnresolvedTerm
fn parse_json_value(
    value: &JsonValue,
    is_ref_type: bool,
    context: &ParsedContext,
    object_var_parsing: bool,
) -> Result<UnresolvedTerm> {
    match value {
        JsonValue::String(s) => {
            if is_variable(s) {
                if object_var_parsing {
                    Ok(UnresolvedTerm::var(s))
                } else {
                    Ok(UnresolvedTerm::string(s))
                }
            } else if is_ref_type {
                // This is a reference - expand as IRI
                let (expanded, _) = details(s, context);
                Ok(UnresolvedTerm::iri(expanded))
            } else {
                // Plain string literal
                Ok(UnresolvedTerm::string(s))
            }
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(UnresolvedTerm::long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(UnresolvedTerm::double(f))
            } else {
                Err(ParseError::InvalidWhere(format!(
                    "Unsupported number type: {}",
                    n
                )))
            }
        }
        JsonValue::Bool(b) => Ok(UnresolvedTerm::boolean(*b)),
        JsonValue::Null => Err(ParseError::InvalidWhere(
            "null values not supported in where clause".to_string(),
        )),
        JsonValue::Object(map) => {
            if let Some(var_val) = map.get("@variable") {
                let var = var_val.as_str().ok_or_else(|| {
                    ParseError::InvalidWhere("@variable must be a string".to_string())
                })?;
                if !is_variable(var) {
                    return Err(ParseError::InvalidWhere(
                        "@variable value must start with '?'".to_string(),
                    ));
                }
                return Ok(UnresolvedTerm::var(var));
            }
            // Phase 2+: nested objects for property paths
            Err(ParseError::InvalidWhere(
                "Nested objects not yet supported (Phase 2)".to_string(),
            ))
        }
        JsonValue::Array(_) => {
            // Could be used for values lists in future
            Err(ParseError::InvalidWhere(
                "Arrays in property values not yet supported".to_string(),
            ))
        }
    }
}

/// Parse a property path pattern from a `@path` alias.
///
/// The path expression was already parsed during `@context` extraction.
/// Here we parse the object value (must be a variable or IRI) and emit
/// an `UnresolvedPattern::Path` pattern.
fn parse_path_alias_usage(
    path_expr: &UnresolvedPathExpr,
    value: &JsonValue,
    subject: &UnresolvedTerm,
    context: &ParsedContext,
    query: &mut UnresolvedQuery,
) -> Result<()> {
    // Parse the object value - must be a variable or IRI, not a literal
    let object = match value {
        JsonValue::String(s) => {
            if is_variable(s) {
                UnresolvedTerm::var(s)
            } else {
                // Treat as IRI reference (property paths traverse refs)
                let (expanded, _) = details(s, context);
                UnresolvedTerm::iri(expanded)
            }
        }
        JsonValue::Object(map) => {
            // Support {"@id":"ex:foo"}
            let id_val = map.get("@id").ok_or_else(|| {
                ParseError::InvalidWhere(
                    "Property path object must be a variable or IRI".to_string(),
                )
            })?;
            let id_str = id_val.as_str().ok_or_else(|| {
                ParseError::InvalidWhere(
                    "Property path object must be a variable or IRI".to_string(),
                )
            })?;
            if is_variable(id_str) {
                UnresolvedTerm::var(id_str)
            } else {
                let (expanded, _) = details(id_str, context);
                UnresolvedTerm::iri(expanded)
            }
        }
        _ => {
            return Err(ParseError::InvalidWhere(
                "Property path object must be a variable or IRI".to_string(),
            ));
        }
    };

    query.patterns.push(UnresolvedPattern::Path {
        subject: subject.clone(),
        path: path_expr.clone(),
        object,
    });

    Ok(())
}
