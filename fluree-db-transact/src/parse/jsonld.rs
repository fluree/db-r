//! JSON-LD transaction parser
//!
//! This module parses JSON-LD transaction documents into the Transaction IR.
//! Supports parsing of insert, upsert, and update transactions with proper
//! JSON-LD context expansion.
//!
//! # Architecture
//!
//! This parser reuses the query parser for WHERE clauses to ensure consistent
//! semantics (OPTIONAL, UNION, FILTER, etc.) between queries and transactions.
//! Only INSERT/DELETE templates are parsed here, which generate flakes rather
//! than match patterns.

use super::txn_meta::extract_txn_meta;
use crate::error::{Result, TransactError};
use crate::ir::{InlineValues, TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::FlakeValue;
use fluree_db_query::parse::{parse_where_with_counters, PathAliasMap, UnresolvedQuery};
use fluree_db_query::VarRegistry;
use fluree_graph_json_ld::{details, expand_with_context, parse_context, ParsedContext};
use fluree_vocab::rdf::{self, TYPE};
use serde_json::Value;
use std::collections::HashMap;

/// Assigns per-transaction graph IDs for JSON-LD `@graph` selectors.
///
/// These IDs are scoped to the transaction envelope via `Txn.graph_delta`.
/// They do not need to be globally stable across commits, as long as the commit
/// carries the mapping used to encode flakes.
struct GraphIdAssigner {
    iri_to_id: HashMap<String, u32>,
    next_id: u32, // 2+ reserved for user graphs
}

impl GraphIdAssigner {
    fn new() -> Self {
        Self {
            iri_to_id: HashMap::new(),
            next_id: 2,
        }
    }

    fn get_or_assign(&mut self, iri: &str) -> u32 {
        if let Some(&id) = self.iri_to_id.get(iri) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.iri_to_id.insert(iri.to_string(), id);
        id
    }

    fn delta(&self) -> rustc_hash::FxHashMap<u32, String> {
        self.iri_to_id
            .iter()
            .map(|(iri, &g_id)| (g_id, iri.clone()))
            .collect()
    }
}

/// Parse a JSON-LD transaction into the Transaction IR
///
/// The transaction format depends on the transaction type:
///
/// ## Insert
/// ```json
/// {
///   "@context": {"ex": "http://example.org/"},
///   "@id": "ex:alice",
///   "ex:name": "Alice",
///   "ex:age": 30
/// }
/// ```
///
/// ## Upsert
/// Same as insert, but existing values for provided predicates are deleted first.
///
/// ## Update (SPARQL-style)
/// ```json
/// {
///   "@context": {"ex": "http://example.org/"},
///   "where": { "@id": "?s", "ex:name": "?name" },
///   "delete": { "@id": "?s", "ex:name": "?name" },
///   "insert": { "@id": "?s", "ex:name": "New Name" }
/// }
/// ```
pub fn parse_transaction(
    json: &Value,
    txn_type: TxnType,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
) -> Result<Txn> {
    match txn_type {
        TxnType::Insert => parse_insert(json, opts, ns_registry),
        TxnType::Upsert => parse_upsert(json, opts, ns_registry),
        TxnType::Update => parse_update(json, opts, ns_registry),
    }
}

/// Parse an insert transaction
fn parse_insert(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    // Parse and merge context
    let context = extract_context(json)?;

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry)?;

    // Expand the document
    let expanded = expand_with_context(json, &context)?;

    let templates = parse_expanded_triples(
        &expanded,
        &context,
        &mut vars,
        ns_registry,
        false,
        &mut graph_ids,
    )?;
    if templates.is_empty() {
        return Err(TransactError::Parse(
            "Insert must contain at least one predicate or @type (an object with only @id is not a valid insert)"
                .to_string(),
        ));
    }

    let mut txn = Txn::insert()
        .with_inserts(templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();
    Ok(txn)
}

/// Parse an upsert transaction
///
/// Upsert is similar to insert, but generates WHERE and DELETE clauses
/// to remove existing values for provided predicates.
fn parse_upsert(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    // For now, upsert is handled the same as insert at parse time
    // The actual upsert logic (query existing, delete old) happens in stage
    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    let context = extract_context(json)?;

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry)?;

    let expanded = expand_with_context(json, &context)?;

    let templates = parse_expanded_triples(
        &expanded,
        &context,
        &mut vars,
        ns_registry,
        false,
        &mut graph_ids,
    )?;
    if templates.is_empty() {
        return Err(TransactError::Parse(
            "Upsert must contain at least one predicate or @type (an object with only @id is not a valid upsert)"
                .to_string(),
        ));
    }

    let mut txn = Txn::upsert()
        .with_inserts(templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();
    Ok(txn)
}

/// Parse an update transaction (SPARQL-style with WHERE/DELETE/INSERT)
///
/// This function reuses the query parser for WHERE clauses, ensuring consistent
/// semantics (OPTIONAL, UNION, FILTER, etc.) between queries and transactions.
///
/// The WHERE clause is parsed to `Vec<UnresolvedPattern>` (keeping IRIs as strings).
/// These patterns are lowered to `Pattern` during staging, when we have access to
/// the ledger's database for IRI encoding.
fn parse_update(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    let obj = json
        .as_object()
        .ok_or_else(|| TransactError::Parse("Update transaction must be an object".to_string()))?;

    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    // Parse context from the outer document
    let context = extract_context(json)?;

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry)?;

    let has_where = obj.get("where").is_some();
    let has_values = obj.get("values").is_some();
    let allow_object_vars = has_where || has_values;
    let object_var_parsing = allow_object_vars && opts.object_var_parsing.unwrap_or(true);

    // Parse WHERE clause using the query parser
    // This reuses full pattern support (OPTIONAL, UNION, FILTER, etc.)
    // Variables remain as strings in UnresolvedPattern; they'll be assigned VarIds
    // during lowering in stage.rs using the same VarRegistry as INSERT/DELETE.
    let where_patterns = if let Some(where_val) = obj.get("where") {
        let mut query = UnresolvedQuery::new(context.clone());
        let mut subject_counter: u32 = 0;
        let mut nested_counter: u32 = 0;
        let no_path_aliases = PathAliasMap::new();
        parse_where_with_counters(
            where_val,
            &context,
            &no_path_aliases,
            &mut query,
            &mut subject_counter,
            &mut nested_counter,
            object_var_parsing,
        )
        .map_err(|e| TransactError::Parse(format!("WHERE clause: {}", e)))?;

        query.patterns
    } else {
        Vec::new()
    };

    // Parse DELETE clause
    let delete_templates = if let Some(delete_val) = obj.get("delete") {
        validate_type_fields(delete_val)?;
        let expanded = expand_with_context(delete_val, &context)?;
        let templates = parse_expanded_triples(
            &expanded,
            &context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            &mut graph_ids,
        )?;
        if templates.is_empty() {
            // Clojure parity: an explicit empty delete (e.g. `"delete": []`) is a no-op.
            // Still reject structurally-empty deletes like `{ "@id": "ex:foo" }`.
            if matches!(delete_val, Value::Array(arr) if arr.is_empty()) {
                Vec::new()
            } else {
                return Err(TransactError::Parse(
                    "delete must contain at least one predicate or @type".to_string(),
                ));
            }
        } else {
            templates
        }
    } else {
        Vec::new()
    };

    // Parse INSERT clause
    let insert_templates = if let Some(insert_val) = obj.get("insert") {
        validate_type_fields(insert_val)?;
        let expanded = expand_with_context(insert_val, &context)?;
        let templates = parse_expanded_triples(
            &expanded,
            &context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            &mut graph_ids,
        )?;
        if templates.is_empty() {
            return Err(TransactError::Parse(
                "insert must contain at least one predicate or @type (an object with only @id is not a valid insert)"
                    .to_string(),
            ));
        }
        templates
    } else {
        Vec::new()
    };

    let mut txn = Txn::update()
        .with_wheres(where_patterns)
        .with_deletes(delete_templates)
        .with_inserts(insert_templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();

    if let Some(values_val) = obj.get("values") {
        let values = parse_inline_values(values_val, &context, &mut txn.vars, ns_registry)?;
        txn = txn.with_values(values);
    }

    Ok(txn)
}

/// Extract and parse the @context from a JSON-LD document
fn extract_context(json: &Value) -> Result<ParsedContext> {
    if let Some(ctx_val) = json.get("@context") {
        Ok(parse_context(&normalize_context_value(ctx_val))?)
    } else {
        Ok(ParsedContext::new())
    }
}

fn normalize_context_value(context_val: &Value) -> Value {
    if let Value::Object(map) = context_val {
        if let Some(base) = map.get("@base") {
            if !map.contains_key("@vocab") {
                let mut out = map.clone();
                out.insert("@vocab".to_string(), base.clone());
                return Value::Object(out);
            }
        }
    }
    context_val.clone()
}

/// Validate that any `@type` fields are strings (IRI) or arrays of strings.
///
/// JSON-LD allows `@type` values only as strings (or arrays). If an object/literal is used
/// (e.g., `{"@value": ...}`), some JSON-LD expansion implementations may silently drop it.
/// We enforce this early for better API errors (Clojure parity).
fn validate_type_fields(v: &Value) -> Result<()> {
    match v {
        Value::Array(arr) => {
            for item in arr {
                validate_type_fields(item)?;
            }
        }
        Value::Object(obj) => {
            if let Some(t) = obj.get("@type") {
                let valid = match t {
                    Value::String(_) => true,
                    Value::Array(a) => a.iter().all(|x| matches!(x, Value::String(_))),
                    _ => false,
                };
                if !valid {
                    return Err(TransactError::Parse(format!(
                        "@type must be a string or array of strings, got: {:?}",
                        t
                    )));
                }
            }
            for (_k, child) in obj {
                validate_type_fields(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// WHERE clause parsing has been removed - we now use the query parser's
// parse_where_with_counters function for full pattern support (OPTIONAL, UNION, etc.)

fn parse_inline_values(
    value: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
) -> Result<InlineValues> {
    let arr = value.as_array().ok_or_else(|| {
        TransactError::Parse("values must be a 2-element array: [vars, rows]".to_string())
    })?;
    if arr.len() != 2 {
        return Err(TransactError::Parse(
            "values must be a 2-element array: [vars, rows]".to_string(),
        ));
    }

    let vars_val = &arr[0];
    let var_names: Vec<&str> = match vars_val {
        Value::String(s) => vec![s.as_str()],
        Value::Array(vs) => vs
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| TransactError::Parse("values vars must be strings".to_string()))
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(TransactError::Parse(
                "values vars must be a string or array of strings".to_string(),
            ))
        }
    };

    let mut var_ids = Vec::with_capacity(var_names.len());
    for name in var_names {
        if !name.starts_with('?') {
            return Err(TransactError::Parse(
                "values vars must start with '?'".to_string(),
            ));
        }
        var_ids.push(vars.get_or_insert(name));
    }

    let rows_val = arr[1]
        .as_array()
        .ok_or_else(|| TransactError::Parse("values rows must be an array".to_string()))?;
    let var_count = var_ids.len();

    let mut rows: Vec<Vec<TemplateTerm>> = Vec::with_capacity(rows_val.len());
    for row_val in rows_val {
        let cells: Vec<&Value> = match row_val {
            Value::Array(cells) => cells.iter().collect(),
            _ if var_count == 1 => vec![row_val],
            _ => {
                return Err(TransactError::Parse(
                    "values row must be an array (or scalar when one var)".to_string(),
                ))
            }
        };

        if cells.len() != var_count {
            return Err(TransactError::Parse(format!(
                "Invalid value binding: number of variables and values don't match (vars={}, row={})",
                var_count,
                cells.len()
            )));
        }

        let mut out_row = Vec::with_capacity(var_count);
        for cell in cells {
            out_row.push(parse_values_cell(cell, context, ns_registry)?);
        }
        rows.push(out_row);
    }

    Ok(InlineValues::new(var_ids, rows))
}

fn parse_values_cell(
    cell: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TemplateTerm> {
    match cell {
        Value::Null => Err(TransactError::Parse(
            "values cell cannot be null".to_string(),
        )),
        Value::Bool(b) => Ok(TemplateTerm::Value(FlakeValue::Boolean(*b))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TemplateTerm::Value(FlakeValue::Long(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(TemplateTerm::Value(FlakeValue::Double(f)))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number type in values: {}",
                    n
                )))
            }
        }
        Value::String(s) => Ok(TemplateTerm::Value(FlakeValue::String(s.clone()))),
        Value::Object(map) => {
            if let Some(id_val) = map.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    TransactError::Parse("@id in values must be a string".to_string())
                })?;
                let (expanded, _) = details(id_str, context);
                if expanded.starts_with("_:") {
                    return Ok(TemplateTerm::BlankNode(expanded.to_string()));
                }
                return Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&expanded)));
            }

            let value_val = map.get("@value").ok_or_else(|| {
                TransactError::Parse("values object must contain @id or @value".to_string())
            })?;

            if let Some(type_val) = map.get("@type").and_then(|v| v.as_str()) {
                if type_val == "@id" {
                    let id_str = value_val.as_str().ok_or_else(|| {
                        TransactError::Parse(
                            "@value must be a string when @type is @id".to_string(),
                        )
                    })?;
                    let (expanded, _) = details(id_str, context);
                    return Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&expanded)));
                }

                let (expanded_type, _) = details(type_val, context);
                let parsed = coerce_value_with_datatype(value_val, &expanded_type, ns_registry)?;
                return Ok(parsed.term);
            }

            match value_val {
                Value::String(s) => Ok(TemplateTerm::Value(FlakeValue::String(s.clone()))),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(TemplateTerm::Value(FlakeValue::Long(i)))
                    } else if let Some(f) = n.as_f64() {
                        Ok(TemplateTerm::Value(FlakeValue::Double(f)))
                    } else {
                        Err(TransactError::Parse(format!(
                            "Unsupported number type in values: {}",
                            n
                        )))
                    }
                }
                Value::Bool(b) => Ok(TemplateTerm::Value(FlakeValue::Boolean(*b))),
                _ => Err(TransactError::Parse(format!(
                    "Unsupported @value type in values: {:?}",
                    value_val
                ))),
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Unsupported values cell: {:?}",
            cell
        ))),
    }
}

/// Parse expanded JSON-LD into triple templates
///
/// Handles both single objects and arrays of objects.
fn parse_expanded_triples(
    expanded: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
    graph_ids: &mut GraphIdAssigner,
) -> Result<Vec<TripleTemplate>> {
    let mut blank_counter: usize = 0;
    match expanded {
        Value::Array(arr) => arr.iter().try_fold(Vec::new(), |mut templates, item| {
            templates.extend(parse_expanded_object(
                item,
                context,
                vars,
                ns_registry,
                object_var_parsing,
                graph_ids,
                &mut blank_counter,
            )?);
            Ok(templates)
        }),
        Value::Object(_) => parse_expanded_object(
            expanded,
            context,
            vars,
            ns_registry,
            object_var_parsing,
            graph_ids,
            &mut blank_counter,
        ),
        _ => Err(TransactError::Parse(
            "Expected expanded object or array of objects".to_string(),
        )),
    }
}

/// Parse a single expanded JSON-LD object into triple templates
fn parse_expanded_object(
    expanded: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
    graph_ids: &mut GraphIdAssigner,
    blank_counter: &mut usize,
) -> Result<Vec<TripleTemplate>> {
    let obj = expanded
        .as_object()
        .ok_or_else(|| TransactError::Parse("Expected expanded object".to_string()))?;

    let mut templates = Vec::new();

    // Optional named-graph selector for this node.
    //
    // Transaction JSON-LD supports a non-standard but convenient form:
    // `{ "@id": "...", "@graph": "<graph iri>", ... }`
    //
    // This is distinct from *envelope form* (top-level `@graph: [...]`) used
    // for txn-meta extraction.
    let graph_id = obj
        .get("@graph")
        .and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            Value::Object(map) => map.get("@id").and_then(|id| id.as_str()),
            Value::Array(arr) => arr.first().and_then(|x| match x {
                Value::String(s) => Some(s.as_str()),
                Value::Object(map) => map.get("@id").and_then(|id| id.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .map(|iri| graph_ids.get_or_assign(iri));

    // Get subject from @id (already expanded IRI or variable)
    let subject = if let Some(id) = obj.get("@id") {
        parse_expanded_id(id, vars, ns_registry)?
    } else {
        // Generate a blank node if no @id
        let n = *blank_counter;
        *blank_counter += 1;
        TemplateTerm::BlankNode(format!("_:b{}", n))
    };

    // Parse each predicate-object pair
    for (key, value) in obj {
        // Skip JSON-LD keywords except @type which becomes rdf:type
        if key == "@id" || key == "@context" || key == "@graph" {
            continue;
        }

        if key == "@type" {
            // @type becomes rdf:type triples
            let rdf_type_iri = TYPE;
            let predicate = TemplateTerm::Sid(ns_registry.sid_for_iri(rdf_type_iri));

            let types = match value {
                Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
                _ => vec![value],
            };

            for type_val in types {
                if let Some(type_iri) = type_val.as_str() {
                    let object = if type_iri.starts_with('?') {
                        let var_id = vars.get_or_insert(type_iri);
                        TemplateTerm::Var(var_id)
                    } else {
                        TemplateTerm::Sid(ns_registry.sid_for_iri(type_iri))
                    };
                    let mut t = TripleTemplate::new(subject.clone(), predicate.clone(), object);
                    if let Some(g_id) = graph_id {
                        t = t.with_graph_id(g_id);
                    }
                    templates.push(t);
                } else {
                    return Err(TransactError::Parse(format!(
                        "Invalid @type value: expected IRI string, got: {:?}",
                        type_val
                    )));
                }
            }
            continue;
        }

        if key == TYPE {
            return Err(TransactError::Parse(format!(
                "\"{}\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead.",
                TYPE
            )));
        }

        // Regular predicate (expanded IRI)
        let predicate = if key.starts_with('?') {
            let var_id = vars.get_or_insert(key);
            TemplateTerm::Var(var_id)
        } else {
            TemplateTerm::Sid(ns_registry.sid_for_iri(key))
        };

        let parsed_values = parse_expanded_objects(
            value,
            context,
            vars,
            ns_registry,
            &mut templates,
            object_var_parsing,
            graph_ids,
            blank_counter,
        )?;

        for parsed_value in parsed_values {
            let mut template =
                TripleTemplate::new(subject.clone(), predicate.clone(), parsed_value.term);
            if let Some(g_id) = graph_id {
                template = template.with_graph_id(g_id);
            }
            if let Some(dt) = parsed_value.datatype {
                template = template.with_datatype(dt);
            }
            if let Some(lang) = parsed_value.language {
                template = template.with_language(lang);
            }
            if let Some(idx) = parsed_value.list_index {
                template = template.with_list_index(idx);
            }
            templates.push(template);
        }
    }

    Ok(templates)
}

/// Parse an expanded @id value
fn parse_expanded_id(
    value: &Value,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TemplateTerm> {
    match value {
        Value::String(s) => {
            if s.starts_with('?') {
                // Variable
                let var_id = vars.get_or_insert(s);
                Ok(TemplateTerm::Var(var_id))
            } else if s.starts_with("_:") {
                // Blank node
                Ok(TemplateTerm::BlankNode(s.clone()))
            } else {
                // Expanded IRI - encode as SID
                Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(s)))
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Expected string for @id, got: {:?}",
            value
        ))),
    }
}

/// Parsed value with optional datatype, language, and list index
struct ParsedValue {
    term: TemplateTerm,
    datatype: Option<fluree_db_core::Sid>,
    language: Option<String>,
    list_index: Option<i32>,
}

impl ParsedValue {
    fn new(term: TemplateTerm) -> Self {
        Self {
            term,
            datatype: None,
            language: None,
            list_index: None,
        }
    }

    fn with_datatype(mut self, dt: fluree_db_core::Sid) -> Self {
        self.datatype = Some(dt);
        self
    }

    fn with_language(mut self, lang: String) -> Self {
        self.language = Some(lang);
        self
    }

    #[allow(dead_code)]
    fn with_list_index(mut self, index: i32) -> Self {
        self.list_index = Some(index);
        self
    }
}

/// Parse expanded object value(s)
///
/// In expanded JSON-LD, values are wrapped in arrays and may have @value/@type/@language.
/// Handles @list specially by expanding list elements into multiple ParsedValues with
/// list_index set.
#[allow(clippy::too_many_arguments)]
fn parse_expanded_objects(
    value: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    templates: &mut Vec<TripleTemplate>,
    object_var_parsing: bool,
    graph_ids: &mut GraphIdAssigner,
    blank_counter: &mut usize,
) -> Result<Vec<ParsedValue>> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for v in arr {
                // Check if this is a @list object
                if let Value::Object(obj) = v {
                    if let Some(list_val) = obj.get("@list") {
                        // Parse list and add all elements with their indices
                        let list_items = parse_list_values(
                            list_val,
                            context,
                            vars,
                            ns_registry,
                            object_var_parsing,
                        )?;
                        results.extend(list_items);
                        continue;
                    }
                }
                // Not a @list, parse normally
                results.push(parse_expanded_value(
                    v,
                    context,
                    vars,
                    ns_registry,
                    templates,
                    object_var_parsing,
                    graph_ids,
                    blank_counter,
                )?);
            }
            Ok(results)
        }
        _ => Ok(vec![parse_expanded_value(
            value,
            context,
            vars,
            ns_registry,
            templates,
            object_var_parsing,
            graph_ids,
            blank_counter,
        )?]),
    }
}

/// Parse a single expanded value
///
/// Handles:
/// - `{"@id": "..."}` - reference (with optional nested property materialization)
/// - `{"@value": "...", "@type": "..."}` - typed literal
/// - `{"@value": "...", "@language": "..."}` - language-tagged string
/// - `{"@value": "..."}` - plain literal
/// - `{"@list": [...]}` - list
/// - `{"@variable": "..."}` - Fluree variable extension
/// - `{...}` - nested blank node (no @id/@value/@list/@variable)
#[allow(clippy::too_many_arguments)]
fn parse_expanded_value(
    value: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    templates: &mut Vec<TripleTemplate>,
    object_var_parsing: bool,
    graph_ids: &mut GraphIdAssigner,
    blank_counter: &mut usize,
) -> Result<ParsedValue> {
    match value {
        Value::Object(obj) => {
            // Check for @id (reference)
            if let Some(id) = obj.get("@id") {
                // If the object has additional keys, materialize it as a nested node.
                let has_nested_props = obj
                    .keys()
                    .any(|k| k.as_str() != "@id" && k.as_str() != "@context");
                if has_nested_props {
                    let nested_templates = parse_expanded_object(
                        value,
                        context,
                        vars,
                        ns_registry,
                        object_var_parsing,
                        graph_ids,
                        blank_counter,
                    )?;
                    templates.extend(nested_templates);
                }
                return Ok(ParsedValue::new(parse_expanded_id(id, vars, ns_registry)?));
            }

            // Check for @value (literal)
            if let Some(val) = obj.get("@value") {
                return parse_literal_value_with_meta(
                    val,
                    obj,
                    context,
                    vars,
                    ns_registry,
                    object_var_parsing,
                );
            }

            // Check for @list (ordered collection)
            if let Some(list_val) = obj.get("@list") {
                return parse_list_value(list_val, context, vars, ns_registry, object_var_parsing);
            }

            if let Some(var_val) = obj.get("@variable") {
                let var = match var_val {
                    Value::String(s) => s.as_str(),
                    Value::Object(map) => {
                        map.get("@value").and_then(|v| v.as_str()).ok_or_else(|| {
                            TransactError::Parse("@variable must be a string".to_string())
                        })?
                    }
                    Value::Array(items) => items
                        .first()
                        .and_then(|item| match item {
                            Value::String(s) => Some(s.as_str()),
                            Value::Object(map) => map.get("@value").and_then(|v| v.as_str()),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            TransactError::Parse("@variable must be a string".to_string())
                        })?,
                    _ => {
                        return Err(TransactError::Parse(
                            "@variable must be a string".to_string(),
                        ))
                    }
                };
                if !var.starts_with('?') {
                    return Err(TransactError::Parse(
                        "@variable value must start with '?'".to_string(),
                    ));
                }
                let var_id = vars.get_or_insert(var);
                return Ok(ParsedValue::new(TemplateTerm::Var(var_id)));
            }

            // Nested node object without @id — treat as a blank node.
            // Any object that reaches this point has properties but none of the
            // JSON-LD value keywords (@id, @value, @list, @variable), so it must
            // be a node object. Per the JSON-LD spec, a node without @id is a
            // blank node — regardless of whether it has @type or not.
            let nested_templates = parse_expanded_object(
                value,
                context,
                vars,
                ns_registry,
                object_var_parsing,
                graph_ids,
                blank_counter,
            )?;
            let subject = nested_templates
                .first()
                .map(|t| t.subject.clone())
                .ok_or_else(|| {
                    TransactError::Parse(
                        "Nested node object must contain at least one property".to_string(),
                    )
                })?;
            templates.extend(nested_templates);
            Ok(ParsedValue::new(subject))
        }
        // Direct values (shouldn't happen in properly expanded JSON-LD, but handle for robustness)
        Value::String(s) => {
            if s.starts_with('?') && object_var_parsing {
                let var_id = vars.get_or_insert(s);
                Ok(ParsedValue::new(TemplateTerm::Var(var_id)))
            } else {
                // Fluree extension / Clojure parity: treat compact IRIs and absolute IRIs
                // as references in templates, even when JSON-LD expansion didn't coerce
                // the value to an `{"@id": ...}` object (i.e., property isn't typed @id).
                let looks_like_iri = s.contains(':') && !s.contains(char::is_whitespace);
                if looks_like_iri {
                    let (expanded, _) = details(s, context);
                    if expanded.starts_with("http://")
                        || expanded.starts_with("https://")
                        || expanded.starts_with("did:")
                        || expanded.starts_with("fluree:")
                        || expanded.starts_with("urn:")
                    {
                        return Ok(ParsedValue::new(TemplateTerm::Sid(
                            ns_registry.sid_for_iri(expanded.as_ref()),
                        )));
                    }
                }
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                    s.clone(),
                ))))
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number format: {}",
                    n
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported value: {:?}",
            value
        ))),
    }
}

/// Parse a literal value with optional @type or @language, returning full metadata
fn parse_literal_value_with_meta(
    val: &Value,
    obj: &serde_json::Map<String, Value>,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
) -> Result<ParsedValue> {
    // Check for @type first - always route through typed coercion when present
    if let Some(type_val) = obj.get("@type") {
        if let Some(type_iri) = type_val.as_str() {
            // Handle @json specially
            if type_iri == "@json" || type_iri == rdf::JSON {
                // If @value is already a string, use it directly (avoid double-serialization)
                // Only serialize if it's an object, array, or other non-string JSON value
                let json_string = match val {
                    Value::String(s) => s.clone(),
                    _ => serde_json::to_string(val).map_err(|e| {
                        TransactError::Parse(format!("Failed to serialize @json value: {}", e))
                    })?,
                };
                let datatype_sid = ns_registry.sid_for_iri(rdf::JSON);
                return Ok(
                    ParsedValue::new(TemplateTerm::Value(FlakeValue::Json(json_string)))
                        .with_datatype(datatype_sid),
                );
            }

            // Handle @vector shorthand: "@vector" or full IRI both route
            // through the standard vector coercion path.
            let resolved_type = if type_iri == "@vector" {
                fluree_vocab::fluree::EMBEDDING_VECTOR
            } else {
                type_iri
            };

            // Route all @value types through typed coercion
            return coerce_value_with_datatype(val, resolved_type, ns_registry);
        }
    }

    // No explicit @type - handle based on JSON value type
    match val {
        Value::String(s) => {
            // Check if it's a variable - allow in @value for transaction WHERE patterns
            if s.starts_with('?') && object_var_parsing {
                let var_id = vars.get_or_insert(s);
                return Ok(ParsedValue::new(TemplateTerm::Var(var_id)));
            }

            // Check for @language
            if let Some(lang_val) = obj.get("@language") {
                if let Some(lang) = lang_val.as_str() {
                    return Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                        s.clone(),
                    )))
                    .with_language(lang.to_string()));
                }
            }

            // Fluree extension / Clojure parity: treat compact IRIs and absolute IRIs
            // as references in templates, even when JSON-LD expansion didn't coerce
            // the value to an `{"@id": ...}` object (i.e., property isn't typed @id).
            let looks_like_iri = s.contains(':') && !s.contains(char::is_whitespace);
            if looks_like_iri {
                let (expanded, _) = details(s, context);
                if expanded.starts_with("http://")
                    || expanded.starts_with("https://")
                    || expanded.starts_with("did:")
                    || expanded.starts_with("fluree:")
                    || expanded.starts_with("urn:")
                {
                    return Ok(ParsedValue::new(TemplateTerm::Sid(
                        ns_registry.sid_for_iri(expanded.as_ref()),
                    )));
                }
            }

            // Plain string literal
            Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                s.clone(),
            ))))
        }
        Value::Number(n) => {
            // No explicit type - infer from JSON number
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in @value: {}",
                    n
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported @value type: {:?}",
            val
        ))),
    }
}

/// Coerce a JSON value to the appropriate FlakeValue based on the explicit datatype IRI.
///
/// This is a thin wrapper around `fluree_db_core::coerce::coerce_json_value` that:
/// 1. Delegates coercion to the core module (which enforces type compatibility and range validation)
/// 2. Wraps the result in `ParsedValue` with the datatype SID
///
/// # Type Compatibility Rules (enforced by core)
/// - String @value can be coerced to any type
/// - Numeric @value + xsd:string → ERROR
/// - Boolean @value + xsd:string → ERROR
/// - Numeric @value + xsd:boolean → ERROR
/// - Integer subtypes enforce range bounds (e.g., xsd:byte must be -128 to 127)
fn coerce_value_with_datatype(
    val: &Value,
    type_iri: &str,
    ns_registry: &mut NamespaceRegistry,
) -> Result<ParsedValue> {
    let datatype_sid = ns_registry.sid_for_iri(type_iri);

    // Delegate to core coercion module
    let flake_value = fluree_db_core::coerce::coerce_json_value(val, type_iri)
        .map_err(|e| TransactError::Parse(e.message))?;

    Ok(ParsedValue::new(TemplateTerm::Value(flake_value)).with_datatype(datatype_sid))
}

/// Convert a string value to the appropriate FlakeValue based on XSD datatype,
/// returning the explicit datatype SID for preservation in the flake.
///
/// This is a thin wrapper around the core coercion module that:
/// 1. Creates a JSON string value for coercion
/// 2. Delegates to `fluree_db_core::coerce::coerce_json_value`
/// 3. Wraps the result in `ParsedValue` with the datatype SID
///
/// # Coercion Policy (enforced by core)
/// - xsd:integer family: Try i64 first, fall back to BigInt; validates range bounds
/// - xsd:decimal: Parse as BigDecimal (preserves precision from string literals)
/// - xsd:double/float: Parse as f64
/// - xsd:dateTime/date/time: Parse into temporal FlakeValue variants
/// - xsd:boolean: Parse "true"/"false"/"1"/"0"
/// - Other types: Store as string with explicit datatype
#[cfg(test)]
fn convert_typed_value_with_meta(
    raw: &str,
    type_iri: &str,
    ns_registry: &mut NamespaceRegistry,
) -> Result<ParsedValue> {
    let datatype_sid = ns_registry.sid_for_iri(type_iri);

    // Create a JSON string value and delegate to core coercion
    let json_value = Value::String(raw.to_string());
    let flake_value = fluree_db_core::coerce::coerce_json_value(&json_value, type_iri)
        .map_err(|e| TransactError::Parse(e.message))?;

    Ok(ParsedValue::new(TemplateTerm::Value(flake_value)).with_datatype(datatype_sid))
}

/// Parse a @list value into a ParsedValue representing the first list element
///
/// Note: This function is called from `parse_expanded_value` which expects a single
/// ParsedValue. For proper @list support, the caller (`parse_expanded_objects`) detects
/// @list objects and uses `parse_list_values` instead to get all elements with indices.
///
/// This function only handles the fallback case and returns the first element.
/// Empty lists produce an error here since we can't return "no value" - the proper
/// empty list handling happens in `parse_expanded_objects` via `parse_list_values`.
fn parse_list_value(
    list_val: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
) -> Result<ParsedValue> {
    // @list should contain an array
    let items = match list_val {
        Value::Array(arr) => arr,
        _ => {
            return Err(TransactError::Parse(
                "@list must contain an array".to_string(),
            ))
        }
    };

    // Empty list: we can't return "no value" from this function.
    // The proper path for empty lists is through parse_expanded_objects which
    // uses parse_list_values and filters empty results. If we get here with an
    // empty list, it's an edge case where @list wasn't detected at the array level.
    if items.is_empty() {
        return Err(TransactError::Parse(
            "Empty @list in unexpected position (should be handled by parse_expanded_objects)"
                .to_string(),
        ));
    }

    // Parse the first element with index 0
    let first = &items[0];
    let mut parsed = parse_single_list_item(first, context, vars, ns_registry, object_var_parsing)?;
    parsed.list_index = Some(0);
    Ok(parsed)
}

/// Parse list items from a @list value, returning all elements with their indices
fn parse_list_values(
    list_val: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
) -> Result<Vec<ParsedValue>> {
    // @list should contain an array
    let items = match list_val {
        Value::Array(arr) => arr,
        _ => {
            return Err(TransactError::Parse(
                "@list must contain an array".to_string(),
            ))
        }
    };

    // Empty list produces zero triples
    if items.is_empty() {
        return Ok(Vec::new());
    }

    // Parse each item with its index
    let mut results = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let mut parsed =
            parse_single_list_item(item, context, vars, ns_registry, object_var_parsing)?;
        parsed.list_index = Some(index as i32);
        results.push(parsed);
    }

    Ok(results)
}

/// Parse a single item from a @list array
fn parse_single_list_item(
    item: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
) -> Result<ParsedValue> {
    match item {
        Value::Object(obj) => {
            // Check for @id (reference)
            if let Some(id) = obj.get("@id") {
                return Ok(ParsedValue::new(parse_expanded_id(id, vars, ns_registry)?));
            }

            // Check for @value (literal)
            if let Some(val) = obj.get("@value") {
                return parse_literal_value_with_meta(
                    val,
                    obj,
                    context,
                    vars,
                    ns_registry,
                    object_var_parsing,
                );
            }

            // Nested @list not supported
            if obj.get("@list").is_some() {
                return Err(TransactError::Parse(
                    "Nested @list not supported".to_string(),
                ));
            }

            Err(TransactError::Parse(format!(
                "Unsupported list item: {:?}",
                item
            )))
        }
        // Direct values
        Value::String(s) => {
            if s.starts_with('?') {
                let var_id = vars.get_or_insert(s);
                Ok(ParsedValue::new(TemplateTerm::Var(var_id)))
            } else {
                // Same IRI heuristic as `parse_expanded_value` for list items.
                let looks_like_iri = s.contains(':') && !s.contains(char::is_whitespace);
                if looks_like_iri {
                    let (expanded, _) = details(s, context);
                    if expanded.starts_with("http://")
                        || expanded.starts_with("https://")
                        || expanded.starts_with("did:")
                        || expanded.starts_with("fluree:")
                        || expanded.starts_with("urn:")
                    {
                        return Ok(ParsedValue::new(TemplateTerm::Sid(
                            ns_registry.sid_for_iri(expanded.as_ref()),
                        )));
                    }
                }
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                    s.clone(),
                ))))
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in list: {}",
                    n
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported list item type: {:?}",
            item
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_registry() -> NamespaceRegistry {
        NamespaceRegistry::new()
    }

    #[test]
    fn test_parse_insert_with_context() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice",
            "ex:name": "Alice",
            "ex:age": 30
        });

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        assert_eq!(txn.txn_type, TxnType::Insert);
        assert_eq!(txn.insert_templates.len(), 2);
        assert!(txn.where_patterns.is_empty());
        assert!(txn.delete_templates.is_empty());

        // Check that http://example.org/ was registered
        assert!(ns_registry.has_prefix("http://example.org/"));
    }

    #[test]
    fn test_parse_update_with_context() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "where": { "@id": "?s", "ex:name": "?name" },
            "delete": { "@id": "?s", "ex:name": "?name" },
            "insert": { "@id": "?s", "ex:name": "New Name" }
        });

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        assert_eq!(txn.txn_type, TxnType::Update);
        assert_eq!(txn.where_patterns.len(), 1);
        assert_eq!(txn.delete_templates.len(), 1);
        assert_eq!(txn.insert_templates.len(), 1);
    }

    #[test]
    fn test_parse_variable() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let term = parse_expanded_id(&json!("?x"), &mut vars, &mut ns_registry).unwrap();

        match term {
            TemplateTerm::Var(id) => assert_eq!(id, vars.get_or_insert("?x")),
            _ => panic!("Expected variable"),
        }
    }

    #[test]
    fn test_parse_blank_node() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let term = parse_expanded_id(&json!("_:b1"), &mut vars, &mut ns_registry).unwrap();

        match term {
            TemplateTerm::BlankNode(label) => assert_eq!(label, "_:b1"),
            _ => panic!("Expected blank node"),
        }
    }

    #[test]
    fn test_parse_typed_literal() {
        let mut ns_registry = test_registry();

        // Integer - should preserve xsd:integer datatype
        let result = convert_typed_value_with_meta(
            "42",
            "http://www.w3.org/2001/XMLSchema#integer",
            &mut ns_registry,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Long(42))
        ));
        // Verify datatype is preserved
        assert!(result.datatype.is_some());
        let dt = result.datatype.unwrap();
        assert!(dt.name.as_ref().contains("integer"));

        // Double - should preserve xsd:double datatype
        let result = convert_typed_value_with_meta(
            "3.13",
            "http://www.w3.org/2001/XMLSchema#double",
            &mut ns_registry,
        )
        .unwrap();
        if let TemplateTerm::Value(FlakeValue::Double(f)) = result.term {
            assert!((f - 3.13).abs() < 0.001);
        } else {
            panic!("Expected double");
        }
        assert!(result.datatype.is_some());

        // Boolean - should preserve xsd:boolean datatype
        let result = convert_typed_value_with_meta(
            "true",
            "http://www.w3.org/2001/XMLSchema#boolean",
            &mut ns_registry,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Boolean(true))
        ));
        assert!(result.datatype.is_some());
    }

    #[test]
    fn test_parse_rdf_type() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/", "Person": "ex:Person"},
            "@id": "ex:alice",
            "@type": "Person"
        });

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        // Should have one triple: ex:alice rdf:type ex:Person
        assert_eq!(txn.insert_templates.len(), 1);

        let template = &txn.insert_templates[0];
        // Predicate should be rdf:type
        if let TemplateTerm::Sid(sid) = &template.predicate {
            assert_eq!(sid.namespace_code, 3); // NS_RDF
            assert_eq!(sid.name.as_ref(), "type");
        } else {
            panic!("Expected Sid for predicate");
        }
    }

    #[test]
    fn test_parse_value_object() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter: usize = 0;

        // @value with @type - should preserve datatype
        let val = json!({"@value": "42", "@type": "http://www.w3.org/2001/XMLSchema#integer"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
            &mut graph_ids,
            &mut blank_counter,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Long(42))
        ));
        assert!(result.datatype.is_some());
        let dt = result.datatype.unwrap();
        assert!(dt.name.as_ref().contains("integer"));
    }

    #[test]
    fn test_parse_language_tagged_string() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter: usize = 0;

        // @value with @language
        let val = json!({"@value": "Hello", "@language": "en"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
            &mut graph_ids,
            &mut blank_counter,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::String(_))
        ));
        assert!(result.language.is_some());
        assert_eq!(result.language.unwrap(), "en");
    }

    #[test]
    fn test_parse_list_values() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // Parse a @list with three string items
        let list_val = json!(["a", "b", "c"]);
        let results =
            parse_list_values(&list_val, &ctx, &mut vars, &mut ns_registry, true).unwrap();

        assert_eq!(results.len(), 3);

        // Check each item has correct list_index
        assert_eq!(results[0].list_index, Some(0));
        assert_eq!(results[1].list_index, Some(1));
        assert_eq!(results[2].list_index, Some(2));

        // Check values
        assert!(matches!(
            &results[0].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "a"
        ));
        assert!(matches!(
            &results[1].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "b"
        ));
        assert!(matches!(
            &results[2].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "c"
        ));
    }

    #[test]
    fn test_parse_empty_list() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // Empty @list produces zero ParsedValues
        let list_val = json!([]);
        let results =
            parse_list_values(&list_val, &ctx, &mut vars, &mut ns_registry, true).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_parse_list_in_insert() {
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // Insert with @list - expanded JSON-LD form
        let json = json!([{
            "@id": "http://example.org/alice",
            "http://example.org/colors": [{"@list": [
                {"@value": "red"},
                {"@value": "green"},
                {"@value": "blue"}
            ]}]
        }]);

        let mut vars = VarRegistry::new();
        let mut graph_ids = GraphIdAssigner::new();
        let templates = parse_expanded_triples(
            &json,
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            &mut graph_ids,
        )
        .unwrap();

        // Should have 3 templates, one for each list item
        assert_eq!(templates.len(), 3);

        // Check list indices
        assert_eq!(templates[0].list_index, Some(0));
        assert_eq!(templates[1].list_index, Some(1));
        assert_eq!(templates[2].list_index, Some(2));

        // Check values
        assert!(matches!(
            &templates[0].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "red"
        ));
        assert!(matches!(
            &templates[1].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "green"
        ));
        assert!(matches!(
            &templates[2].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "blue"
        ));
    }

    #[test]
    fn test_parse_nested_blank_node() {
        // A property value that is a node object without @id should be treated as a blank node.
        // Input is in expanded JSON-LD form (arrays around values, @type is array of strings).
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut graph_ids = GraphIdAssigner::new();

        let expanded = json!([{
            "@id": "http://example.org/thing/1",
            "http://example.org/relatedTo": [{
                "@type": ["http://example.org/Widget"],
                "http://example.org/name": [{"@value": "nested-widget"}]
            }]
        }]);

        let templates = parse_expanded_triples(
            &expanded,
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            &mut graph_ids,
        )
        .unwrap();

        // Should have 3 triples (order: nested triples first, then parent reference):
        //   _:b0    rdf:type  Widget           (nested, materialized first)
        //   _:b0    name      "nested-widget"  (nested, materialized first)
        //   thing/1 relatedTo _:b0             (parent reference, added last)
        assert_eq!(templates.len(), 3);

        // Find the parent→blank node reference triple (parent subject is a Sid)
        let ref_triple = templates
            .iter()
            .find(|t| matches!(&t.subject, TemplateTerm::Sid(_)))
            .expect("Expected a triple with parent Sid subject");
        assert!(matches!(&ref_triple.object, TemplateTerm::BlankNode(_)));

        // Extract the blank node label from the reference
        let bnode_label = match &ref_triple.object {
            TemplateTerm::BlankNode(label) => label.clone(),
            _ => panic!("Expected BlankNode"),
        };

        // The other 2 triples should use the same blank node as subject
        let bnode_triples: Vec<_> = templates
            .iter()
            .filter(|t| matches!(&t.subject, TemplateTerm::BlankNode(_)))
            .collect();
        assert_eq!(bnode_triples.len(), 2);
        for t in &bnode_triples {
            let label = match &t.subject {
                TemplateTerm::BlankNode(l) => l.as_str(),
                _ => unreachable!(),
            };
            assert_eq!(label, bnode_label);
        }
    }

    #[test]
    fn test_parse_doubly_nested_blank_nodes() {
        // Two levels of nesting, both without @id — must get distinct blank node IDs.
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut graph_ids = GraphIdAssigner::new();

        let expanded = json!([{
            "@id": "http://example.org/root",
            "http://example.org/outer": [{
                "@type": ["http://example.org/Outer"],
                "http://example.org/inner": [{
                    "@type": ["http://example.org/Inner"],
                    "http://example.org/value": [{"@value": "deep"}]
                }]
            }]
        }]);

        let templates = parse_expanded_triples(
            &expanded,
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            &mut graph_ids,
        )
        .unwrap();

        // Collect all blank node labels used as subjects
        let bnode_subjects: Vec<&str> = templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();

        // There should be at least 2 distinct blank node labels (outer + inner)
        let mut unique: Vec<&str> = bnode_subjects.clone();
        unique.sort();
        unique.dedup();
        assert!(
            unique.len() >= 2,
            "Expected at least 2 distinct blank node labels, got: {:?}",
            unique
        );
    }

    #[test]
    fn test_parse_sibling_nested_blank_nodes() {
        // Two sibling nested objects without @id under different properties — distinct blank nodes.
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut graph_ids = GraphIdAssigner::new();

        let expanded = json!([{
            "@id": "http://example.org/parent",
            "http://example.org/left": [{
                "@type": ["http://example.org/Left"],
                "http://example.org/label": [{"@value": "L"}]
            }],
            "http://example.org/right": [{
                "@type": ["http://example.org/Right"],
                "http://example.org/label": [{"@value": "R"}]
            }]
        }]);

        let templates = parse_expanded_triples(
            &expanded,
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            &mut graph_ids,
        )
        .unwrap();

        // Collect blank node labels used as objects of the parent (the references)
        let bnode_refs: Vec<&str> = templates
            .iter()
            .filter(|t| matches!(&t.subject, TemplateTerm::Sid(_)))
            .filter_map(|t| match &t.object {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();

        assert_eq!(
            bnode_refs.len(),
            2,
            "Expected 2 blank node references from parent"
        );
        assert_ne!(
            bnode_refs[0], bnode_refs[1],
            "Sibling blank nodes must have distinct labels"
        );
    }

    #[test]
    fn test_parse_nested_blank_node_insert() {
        // End-to-end: parse_insert with compact JSON-LD containing nested blank nodes.
        // This mirrors the real-world scenario from the bug report.
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {
                "ex": "http://example.org/",
                "prov": "http://www.w3.org/ns/prov#"
            },
            "@id": "ex:calendar/1",
            "@type": "ex:Calendar",
            "prov:wasGeneratedBy": {
                "@type": "prov:Generation",
                "prov:atTime": "2026-02-14T18:58:49Z",
                "prov:hadActivity": {
                    "@type": "prov:Activity",
                    "prov:atLocation": "row:1"
                }
            }
        });

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        // Should succeed and produce triples for:
        //   calendar/1  rdf:type       Calendar
        //   calendar/1  wasGeneratedBy _:b0
        //   _:b0        rdf:type       Generation
        //   _:b0        atTime         "2026-02-14T18:58:49Z"
        //   _:b0        hadActivity    _:b1
        //   _:b1        rdf:type       Activity
        //   _:b1        atLocation     "row:1"
        assert!(
            txn.insert_templates.len() >= 7,
            "Expected at least 7 triples, got {}",
            txn.insert_templates.len()
        );

        // Verify at least 2 distinct blank node subjects exist
        let bnode_subjects: std::collections::HashSet<_> = txn
            .insert_templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            bnode_subjects.len() >= 2,
            "Expected at least 2 distinct blank node subjects, got: {:?}",
            bnode_subjects
        );
    }

    #[test]
    fn test_parse_nested_blank_node_without_type() {
        // Nested blank nodes do NOT require @type. Any object with properties
        // but no @id is a blank node per the JSON-LD spec.
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:andrew",
            "ex:name": "andrew",
            "ex:friend": {
                "ex:name": "ben",
                "ex:friend": {
                    "ex:name": "jake"
                }
            }
        });

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        // Should produce:
        //   andrew  name    "andrew"
        //   andrew  friend  _:b0
        //   _:b0    name    "ben"
        //   _:b0    friend  _:b1
        //   _:b1    name    "jake"
        assert_eq!(
            txn.insert_templates.len(),
            5,
            "Expected 5 triples, got {}",
            txn.insert_templates.len()
        );

        // Verify 2 distinct blank node subjects (ben and jake)
        let bnode_subjects: std::collections::HashSet<_> = txn
            .insert_templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            bnode_subjects.len(),
            2,
            "Expected 2 distinct blank node subjects, got: {:?}",
            bnode_subjects
        );
    }
}
