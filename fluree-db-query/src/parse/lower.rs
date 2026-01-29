//! AST to IR lowering
//!
//! Converts unresolved AST types (with string IRIs) to resolved IR types
//! (with Sids and VarIds) using an IriEncoder.

use super::ast::{
    LiteralValue, PathModifier as AstPathModifier, UnresolvedAggregateFn, UnresolvedAggregateSpec,
    UnresolvedConstructTemplate, UnresolvedFilterExpr, UnresolvedGraphSelectSpec,
    UnresolvedNestedSelectSpec, UnresolvedOptions, UnresolvedPattern,
    UnresolvedPropertyPathPattern, UnresolvedQuery, UnresolvedRoot, UnresolvedSelectionSpec,
    UnresolvedSortDirection, UnresolvedSortSpec, UnresolvedTerm, UnresolvedTriplePattern,
    UnresolvedValue,
};
use super::encode::IriEncoder;
use super::error::{ParseError, Result};
use crate::aggregate::{AggregateFn, AggregateSpec};
use crate::binding::Binding;
use crate::context::WellKnownDatatypes;
use crate::ir::{
    FilterExpr, FunctionName, PathModifier, Pattern, IndexSearchPattern, IndexSearchTarget,
    VectorSearchPattern, VectorSearchTarget, PropertyPathPattern, SubqueryPattern,
};
use crate::vector::DistanceMetric;
// Re-export graph select types for external use
pub use crate::ir::{GraphSelectSpec, NestedSelectSpec, Root, SelectionSpec};
use crate::options::QueryOptions;
use crate::pattern::{Term, TriplePattern};
use crate::sort::{SortDirection, SortSpec};
use crate::var_registry::{VarId, VarRegistry};
use fluree_db_core::{FlakeValue, Sid};
use fluree_graph_json_ld::ParsedContext;

/// Select mode determines result shape
///
/// This is derived from the parsed query (select vs selectOne vs construct) and controls
/// whether the formatter returns an array, single value, or JSON-LD graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelectMode {
    /// Normal select: return array of rows
    #[default]
    Many,

    /// selectOne: return first row or null
    One,

    /// Wildcard (*): return all bound variables as object
    ///
    /// Uses `batch.schema()` to get all variables, not just select.
    /// Omits unbound/poisoned variables from output.
    Wildcard,

    /// CONSTRUCT: return JSON-LD graph `{"@context": ..., "@graph": [...]}`
    ///
    /// Template patterns are instantiated with bindings to produce RDF triples.
    /// Projection is skipped (all bindings needed for templating).
    Construct,
}

/// Resolved CONSTRUCT template patterns
///
/// Contains the template patterns that will be instantiated with query bindings
/// to produce output triples. Uses the same TriplePattern type as WHERE clause
/// patterns, but variables are resolved against the query result bindings rather
/// than matched against the database.
#[derive(Debug, Clone)]
pub struct ConstructTemplate {
    /// Template patterns (resolved TriplePatterns with Sids and VarIds)
    pub patterns: Vec<TriplePattern>,
}

impl ConstructTemplate {
    /// Create a new construct template from patterns
    pub fn new(patterns: Vec<TriplePattern>) -> Self {
        Self { patterns }
    }
}

/// Resolved query ready for execution
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Parsed JSON-LD context (for result formatting)
    pub context: ParsedContext,
    /// Original JSON context from the query (for CONSTRUCT output)
    pub orig_context: Option<serde_json::Value>,
    /// Selected variable IDs
    pub select: Vec<VarId>,
    /// Resolved patterns (triples, filters, optionals, etc.)
    pub patterns: Vec<Pattern>,
    /// Query options (limit, offset, order by, group by, etc.)
    pub options: QueryOptions,
    /// Select mode (from parsed query)
    ///
    /// Controls whether result is an array, single value, or JSON-LD graph.
    pub select_mode: SelectMode,
    /// CONSTRUCT template (None for SELECT queries)
    ///
    /// Contains the resolved template patterns that will be instantiated
    /// with query bindings to produce output triples.
    pub construct_template: Option<ConstructTemplate>,
    /// Graph crawl select specification (None for flat SELECT or CONSTRUCT)
    ///
    /// When present, controls nested JSON-LD object expansion during formatting.
    pub graph_select: Option<GraphSelectSpec>,
}

impl ParsedQuery {
    /// Create a new parsed query
    pub fn new(context: ParsedContext) -> Self {
        Self {
            context,
            orig_context: None,
            select: Vec::new(),
            patterns: Vec::new(),
            options: QueryOptions::default(),
            select_mode: SelectMode::default(),
            construct_template: None,
            graph_select: None,
        }
    }

    /// Create a new parsed query with a specific select mode
    pub fn with_select_mode(context: ParsedContext, select_mode: SelectMode) -> Self {
        Self {
            context,
            orig_context: None,
            select: Vec::new(),
            patterns: Vec::new(),
            options: QueryOptions::default(),
            select_mode,
            construct_template: None,
            graph_select: None,
        }
    }

    /// Get all triple patterns (flattening nested structures)
    pub fn triple_patterns(&self) -> Vec<&TriplePattern> {
        fn collect<'a>(patterns: &'a [Pattern], out: &mut Vec<&'a TriplePattern>) {
            for p in patterns {
                match p {
                    Pattern::Triple(tp) => out.push(tp),
                    Pattern::Optional(inner)
                    | Pattern::Minus(inner)
                    | Pattern::Exists(inner)
                    | Pattern::NotExists(inner) => collect(inner, out),
                    Pattern::Union(branches) => {
                        for branch in branches {
                            collect(branch, out);
                        }
                    }
                    Pattern::Graph { patterns: inner, .. } => collect(inner, out),
                    Pattern::Filter(_)
                    | Pattern::Bind { .. }
                    | Pattern::Values { .. }
                    | Pattern::PropertyPath(_)
                    | Pattern::Subquery(_)
                    | Pattern::IndexSearch(_)
                    | Pattern::VectorSearch(_)
                    | Pattern::R2rml(_) => {}
                }
            }
        }

        let mut result = Vec::new();
        collect(&self.patterns, &mut result);
        result
    }

    /// Create a copy of this query with different patterns
    ///
    /// Used by pattern rewriting (RDFS expansion) to create a query with
    /// expanded patterns while preserving all other query properties.
    pub fn with_patterns(&self, patterns: Vec<Pattern>) -> Self {
        Self {
            context: self.context.clone(),
            orig_context: self.orig_context.clone(),
            select: self.select.clone(),
            patterns,
            options: self.options.clone(),
            select_mode: self.select_mode.clone(),
            construct_template: self.construct_template.clone(),
            graph_select: self.graph_select.clone(),
        }
    }
}

/// Lower an unresolved query to a resolved ParsedQuery
///
/// # Arguments
///
/// * `ast` - The unresolved query AST
/// * `encoder` - IRI encoder for converting IRIs to Sids
/// * `vars` - Variable registry (caller provides to enable sharing across subqueries)
/// * `select_mode` - Result shaping mode (Many, One, or Wildcard)
///
/// # Returns
///
/// A resolved `ParsedQuery` with Sids and VarIds
pub fn lower_query<E: IriEncoder>(
    ast: UnresolvedQuery,
    encoder: &E,
    vars: &mut VarRegistry,
    select_mode: SelectMode,
) -> Result<ParsedQuery> {
    let mut query = ParsedQuery::with_select_mode(ast.context, select_mode);

    // Copy original context JSON for CONSTRUCT output
    query.orig_context = ast.orig_context;

    // Lower select variables
    for var_name in &ast.select {
        let var_id = vars.get_or_insert(var_name);
        query.select.push(var_id);
    }

    // Lower patterns
    for unresolved_pattern in ast.patterns {
        let pattern = lower_unresolved_pattern(&unresolved_pattern, encoder, vars)?;
        query.patterns.push(pattern);
    }

    // Lower options
    query.options = lower_options(&ast.options, vars)?;

    // Lower construct template if present
    if let Some(ref template) = ast.construct_template {
        query.construct_template = Some(lower_construct_template(template, encoder, vars)?);
    }

    // Lower graph select if present
    if let Some(ref graph_select) = ast.graph_select {
        query.graph_select = Some(lower_graph_select(graph_select, encoder, vars)?);
    }

    Ok(query)
}

/// Lower an unresolved pattern to a resolved Pattern
/// Lower an unresolved pattern to a resolved Pattern
///
/// This converts string IRIs to encoded Sids using the provided encoder.
/// Also used by fluree-db-transact for lowering WHERE clause patterns.
pub fn lower_unresolved_pattern<E: IriEncoder>(
    pattern: &UnresolvedPattern,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Pattern> {
    match pattern {
        UnresolvedPattern::Triple(tp) => {
            let lowered = lower_triple_pattern(tp, encoder, vars)?;
            Ok(Pattern::Triple(lowered))
        }
        UnresolvedPattern::Filter(expr) => {
            let lowered = lower_filter_expr(expr, vars)?;
            Ok(Pattern::Filter(lowered))
        }
        UnresolvedPattern::Optional(inner) => {
            let lowered: Result<Vec<Pattern>> = inner
                .iter()
                .map(|p| lower_unresolved_pattern(p, encoder, vars))
                .collect();
            Ok(Pattern::Optional(lowered?))
        }
        UnresolvedPattern::Union(branches) => {
            let lowered_branches: Result<Vec<Vec<Pattern>>> = branches
                .iter()
                .map(|branch| {
                    branch
                        .iter()
                        .map(|p| lower_unresolved_pattern(p, encoder, vars))
                        .collect()
                })
                .collect();
            Ok(Pattern::Union(lowered_branches?))
        }
        UnresolvedPattern::Bind { var, expr } => {
            let var_id = vars.get_or_insert(var);
            let lowered_expr = lower_filter_expr(expr, vars)?;
            Ok(Pattern::Bind {
                var: var_id,
                expr: lowered_expr,
            })
        }
        UnresolvedPattern::Values { vars: v, rows } => {
            let var_ids: Vec<VarId> = v.iter().map(|name| vars.get_or_insert(name)).collect();
            let rows: Result<Vec<Vec<Binding>>> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| lower_values_cell(cell, encoder))
                        .collect::<Result<Vec<_>>>()
                })
                .collect();
            Ok(Pattern::Values {
                vars: var_ids,
                rows: rows?,
            })
        }
        UnresolvedPattern::Minus(inner) => {
            let lowered: Result<Vec<Pattern>> = inner
                .iter()
                .map(|p| lower_unresolved_pattern(p, encoder, vars))
                .collect();
            Ok(Pattern::Minus(lowered?))
        }
        UnresolvedPattern::Exists(inner) => {
            let lowered: Result<Vec<Pattern>> = inner
                .iter()
                .map(|p| lower_unresolved_pattern(p, encoder, vars))
                .collect();
            Ok(Pattern::Exists(lowered?))
        }
        UnresolvedPattern::NotExists(inner) => {
            let lowered: Result<Vec<Pattern>> = inner
                .iter()
                .map(|p| lower_unresolved_pattern(p, encoder, vars))
                .collect();
            Ok(Pattern::NotExists(lowered?))
        }
        UnresolvedPattern::PropertyPath(pp) => {
            let lowered = lower_property_path(pp, encoder, vars)?;
            Ok(Pattern::PropertyPath(lowered))
        }
        UnresolvedPattern::Subquery(subquery) => {
            let lowered = lower_subquery(subquery, encoder, vars)?;
            Ok(Pattern::Subquery(lowered))
        }
        UnresolvedPattern::IndexSearch(isp) => {
            // Lower index search (BM25 / virtual graph search) pattern.
            let vg_alias = isp.vg_alias.as_ref().to_string();

            let target = match &isp.target {
                super::ast::UnresolvedIndexSearchTarget::Const(s) => {
                    IndexSearchTarget::Const(s.as_ref().to_string())
                }
                super::ast::UnresolvedIndexSearchTarget::Var(v) => {
                    IndexSearchTarget::Var(vars.get_or_insert(v))
                }
            };

            let id_var = vars.get_or_insert(&isp.id_var);
            let mut pat = IndexSearchPattern::new(vg_alias, target, id_var);

            pat.limit = isp.limit;
            pat.score_var = isp.score_var.as_ref().map(|v| vars.get_or_insert(v));
            pat.ledger_var = isp.ledger_var.as_ref().map(|v| vars.get_or_insert(v));
            pat.sync = isp.sync;
            pat.timeout = isp.timeout;

            Ok(Pattern::IndexSearch(pat))
        }
        UnresolvedPattern::VectorSearch(vsp) => {
            // Lower vector search (similarity search) pattern.
            let vg_alias = vsp.vg_alias.as_ref().to_string();

            let target = match &vsp.target {
                super::ast::UnresolvedVectorSearchTarget::Const(v) => {
                    VectorSearchTarget::Const(v.clone())
                }
                super::ast::UnresolvedVectorSearchTarget::Var(v) => {
                    VectorSearchTarget::Var(vars.get_or_insert(v))
                }
            };

            let metric = DistanceMetric::from_str(&vsp.metric)
                .unwrap_or(DistanceMetric::Cosine);

            let id_var = vars.get_or_insert(&vsp.id_var);
            let mut pat = VectorSearchPattern::new(vg_alias, target, id_var)
                .with_metric(metric);

            if let Some(limit) = vsp.limit {
                pat = pat.with_limit(limit);
            }
            if let Some(sv) = &vsp.score_var {
                pat = pat.with_score_var(vars.get_or_insert(sv));
            }
            if let Some(lv) = &vsp.ledger_var {
                pat = pat.with_ledger_var(vars.get_or_insert(lv));
            }
            if vsp.sync {
                pat = pat.with_sync(true);
            }
            if let Some(t) = vsp.timeout {
                pat = pat.with_timeout(t);
            }

            Ok(Pattern::VectorSearch(pat))
        }
        UnresolvedPattern::Graph { name, patterns } => {
            // Lower GRAPH pattern - scope inner patterns to a named graph
            use crate::ir::GraphName;
            use std::sync::Arc;

            let ir_name = if name.starts_with('?') {
                // Variable graph name
                let var_id = vars.get_or_insert(name);
                GraphName::Var(var_id)
            } else {
                // Concrete graph IRI (kept as string, not encoded)
                GraphName::Iri(Arc::from(name.as_ref()))
            };

            let lowered_patterns: Result<Vec<Pattern>> = patterns
                .iter()
                .map(|p| lower_unresolved_pattern(p, encoder, vars))
                .collect();

            Ok(Pattern::Graph {
                name: ir_name,
                patterns: lowered_patterns?,
            })
        }
    }
}

fn lower_values_cell<E: IriEncoder>(cell: &UnresolvedValue, encoder: &E) -> Result<Binding> {
    let dts = WellKnownDatatypes::new();
    match cell {
        UnresolvedValue::Unbound => Ok(Binding::Unbound),
        UnresolvedValue::Iri(iri) => {
            let sid = encoder
                .encode_iri(iri)
                .ok_or_else(|| ParseError::UnknownNamespace(iri.to_string()))?;
            Ok(Binding::Sid(sid))
        }
        UnresolvedValue::Literal { value, dt_iri, lang } => {
            // Language-tagged literals always use rdf:langString (Clojure/JSON-LD semantics).
            //
            // NOTE: Even if a datatype is provided, rdf:langString is the correct datatype
            // for language-tagged strings. We preserve the lang tag in the Binding.
            let lang_dt = fluree_db_core::Sid::new(3, "langString");

            // Build initial FlakeValue from the literal
            let initial_fv = match value {
                LiteralValue::String(s) => FlakeValue::String(s.as_ref().to_string()),
                LiteralValue::Long(i) => FlakeValue::Long(*i),
                LiteralValue::Double(f) => FlakeValue::Double(*f),
                LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
                LiteralValue::Vector(v) => FlakeValue::Vector(v.clone()),
            };

            let (dt_sid, fv) = if let Some(dt) = dt_iri {
                let sid = encoder
                    .encode_iri(dt)
                    .ok_or_else(|| ParseError::UnknownNamespace(dt.to_string()))?;
                // Apply typed literal coercion for VALUES cells (same as WHERE patterns)
                let coerced = coerce_value_by_datatype(initial_fv, dt)?;
                (sid, coerced)
            } else {
                let sid = match value {
                    LiteralValue::String(_) => dts.xsd_string,
                    LiteralValue::Long(_) => dts.xsd_long,
                    LiteralValue::Double(_) => dts.xsd_double,
                    LiteralValue::Boolean(_) => dts.xsd_boolean,
                    LiteralValue::Vector(_) => dts.fluree_vector,
                };
                (sid, initial_fv)
            };

            Ok(if let Some(lang) = lang {
                // rdf:langString requires a string lexical form
                if !matches!(fv, FlakeValue::String(_)) {
                    return Err(ParseError::InvalidWhere(
                        "Language-tagged VALUES literals must be strings".to_string(),
                    ));
                }
                Binding::lit_lang(fv, lang_dt, lang.as_ref())
            } else {
                Binding::lit(fv, dt_sid)
            })
        }
    }
}

// XSD datatype IRIs for typed literal coercion are imported from fluree_vocab::xsd

/// Coerce a FlakeValue based on its datatype IRI.
///
/// This handles typed literals like `{"@value": "3", "@type": "xsd:integer"}` where
/// the string "3" needs to be coerced to Long(3).
///
/// Policy:
/// - String @value + xsd:integer/long/int/short/byte → Long (or BigInt if > i64)
/// - String @value + xsd:decimal → BigDecimal
/// - String @value + xsd:double/float → Double
/// - String @value + xsd:dateTime/date/time → DateTime/Date/Time
/// - JSON number @value + xsd:string → ERROR (incompatible)
/// - JSON boolean @value + xsd:string → ERROR (incompatible)
/// Coerce a FlakeValue to match the target datatype.
///
/// This is a thin wrapper around `fluree_db_core::coerce::coerce_value` that
/// maps the core `CoercionError` to query `ParseError::TypeCoercion`.
///
/// # Arguments
/// * `value` - The value to coerce
/// * `datatype_iri` - Fully expanded datatype IRI (e.g., `http://www.w3.org/2001/XMLSchema#integer`)
///
/// # Contract
/// The caller is responsible for ensuring the datatype IRI is fully expanded.
/// Prefix expansion should happen at the JSON-LD parsing boundary, not here.
/// Use constants from `fluree_vocab::xsd` for common datatypes.
///
/// # Returns
/// * `Ok(FlakeValue)` - The coerced value
/// * `Err(ParseError::TypeCoercion)` - If coercion fails
pub fn coerce_value_by_datatype(value: FlakeValue, datatype_iri: &str) -> Result<FlakeValue> {
    fluree_db_core::coerce::coerce_value(value, datatype_iri)
        .map_err(|e| ParseError::TypeCoercion(e.message))
}

/// Lower an unresolved triple pattern to a resolved TriplePattern
fn lower_triple_pattern<E: IriEncoder>(
    pattern: &UnresolvedTriplePattern,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<TriplePattern> {
    let s = lower_term(&pattern.s, encoder, vars)?;
    let p = lower_term(&pattern.p, encoder, vars)?;
    let mut o = lower_term(&pattern.o, encoder, vars)?;

    // Handle datatype
    let dt = if let Some(dt_iri) = &pattern.dt_iri {
        let dt_sid = encoder
            .encode_iri(dt_iri)
            .ok_or_else(|| ParseError::UnknownNamespace(dt_iri.to_string()))?;
        
        // Coerce literal values based on datatype
        if let Term::Value(ref value) = o {
            let coerced = coerce_value_by_datatype(value.clone(), dt_iri)?;
            o = Term::Value(coerced);
        }
        
        Some(dt_sid)
    } else {
        None
    };

    // Build pattern with dt and lang
    let mut tp = if let Some(dt) = dt {
        TriplePattern::with_dt(s, p, o, dt)
    } else {
        TriplePattern::new(s, p, o)
    };

    // Add language constraint if present (can be a constant or a variable like "?lang")
    if let Some(lang) = &pattern.lang {
        tp = tp.with_lang(lang.as_ref());
    }

    Ok(tp)
}

/// Lower a path modifier from AST to IR
fn lower_path_modifier(modifier: AstPathModifier) -> PathModifier {
    match modifier {
        AstPathModifier::OneOrMore => PathModifier::OneOrMore,
        AstPathModifier::ZeroOrMore => PathModifier::ZeroOrMore,
    }
}

/// Lower an unresolved property path pattern to a resolved PropertyPathPattern
///
/// Performs validation:
/// - Rejects both-constant patterns (both subject and object are IRIs)
/// - Rejects literal values in subject or object positions
fn lower_property_path<E: IriEncoder>(
    pattern: &UnresolvedPropertyPathPattern,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<PropertyPathPattern> {
    // Lower subject
    let subject = lower_term(&pattern.subject, encoder, vars)?;

    // Validate subject is not a literal
    if matches!(subject, Term::Value(_)) {
        return Err(ParseError::InvalidWhere(
            "Property path subject cannot be a literal value".to_string(),
        ));
    }

    // Lower predicate (always an IRI)
    let predicate = encoder
        .encode_iri(&pattern.predicate_iri)
        .ok_or_else(|| ParseError::UnknownNamespace(pattern.predicate_iri.to_string()))?;

    // Lower object
    let object = lower_term(&pattern.object, encoder, vars)?;

    // Validate object is not a literal
    if matches!(object, Term::Value(_)) {
        return Err(ParseError::InvalidWhere(
            "Property path object cannot be a literal value".to_string(),
        ));
    }

    // Note: we allow both-constant property paths (reachability check).
    // This is used by Clojure integration tests and is safe (returns either 0 or 1 empty row).

    let modifier = lower_path_modifier(pattern.modifier);

    Ok(PropertyPathPattern::new(subject, predicate, modifier, object))
}

/// Lower an unresolved subquery to a resolved SubqueryPattern
///
/// Processes the subquery's select list, patterns, and options.
fn lower_subquery<E: IriEncoder>(
    subquery: &UnresolvedQuery,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<SubqueryPattern> {
    // Lower select list to VarIds
    let select: Vec<VarId> = subquery
        .select
        .iter()
        .map(|var_name| vars.get_or_insert(var_name))
        .collect();

    // Lower WHERE patterns
    let patterns: Result<Vec<Pattern>> = subquery
        .patterns
        .iter()
        .map(|p| lower_unresolved_pattern(p, encoder, vars))
        .collect();
    let patterns = patterns?;

    // Build SubqueryPattern with options
    let mut sq = SubqueryPattern::new(select, patterns);

    if let Some(limit) = subquery.options.limit {
        sq = sq.with_limit(limit);
    }
    if let Some(offset) = subquery.options.offset {
        sq = sq.with_offset(offset);
    }
    if subquery.options.distinct {
        sq = sq.with_distinct();
    }
    if !subquery.options.order_by.is_empty() {
        let sort_specs: Vec<_> = subquery
            .options
            .order_by
            .iter()
            .map(|s| lower_sort_spec(s, vars))
            .collect();
        sq = sq.with_order_by(sort_specs);
    }

    // GROUP BY / aggregates / HAVING (needed for subqueries used in filters/unions)
    if !subquery.options.group_by.is_empty() {
        sq.group_by = subquery
            .options
            .group_by
            .iter()
            .map(|v| vars.get_or_insert(v))
            .collect();
    }
    if !subquery.options.aggregates.is_empty() {
        sq.aggregates = subquery
            .options
            .aggregates
            .iter()
            .map(|a| lower_aggregate_spec(a, vars))
            .collect();
    }
    if let Some(ref having) = subquery.options.having {
        sq.having = Some(lower_filter_expr(having, vars)?);
    }

    Ok(sq)
}

/// Lower an unresolved CONSTRUCT template to a resolved ConstructTemplate
///
/// Only processes triple patterns from the template (filters/optionals are ignored).
fn lower_construct_template<E: IriEncoder>(
    template: &UnresolvedConstructTemplate,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<ConstructTemplate> {
    let mut patterns = Vec::new();

    for unresolved in &template.patterns {
        if let UnresolvedPattern::Triple(tp) = unresolved {
            patterns.push(lower_triple_pattern(tp, encoder, vars)?);
        }
        // Ignore non-triple patterns in templates (filters, optionals, binds)
    }

    Ok(ConstructTemplate::new(patterns))
}

// ============================================================================
// Graph crawl lowering
// ============================================================================

/// Lower an unresolved graph select specification to a resolved GraphSelectSpec
fn lower_graph_select<E: IriEncoder>(
    spec: &UnresolvedGraphSelectSpec,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<GraphSelectSpec> {
    // Handle root - variable or IRI constant
    let root = match &spec.root {
        UnresolvedRoot::Var(name) => {
            // Use get_or_insert: don't require root var to be in WHERE
            // (Clojure allows unbound root vars; formatting skips those rows)
            let var_id = vars.get_or_insert(name);
            Root::Var(var_id)
        }
        UnresolvedRoot::Iri(expanded_iri) => {
            // Clojure parity: allow non-IRI (and “faux compact”) ids to be used as subjects.
            //
            // If the IRI encoder can't encode the value via namespaces, fall back to namespace_code=0
            // (raw id string). This supports ids like "foo" and "foaf:bar" without requiring @base/@vocab.
            let sid = encoder
                .encode_iri(expanded_iri)
                .unwrap_or_else(|| Sid::new(0, expanded_iri));
            Root::Sid(sid)
        }
    };

    // Lower forward selections
    let selections = spec
        .selections
        .iter()
        .map(|s| lower_selection_spec(s, encoder))
        .collect::<Result<Vec<_>>>()?;

    // Lower reverse selections
    let mut reverse = std::collections::HashMap::new();
    for (iri, nested_opt) in &spec.reverse {
        let sid = encoder
            .encode_iri(iri)
            .ok_or_else(|| ParseError::UnknownNamespace(iri.clone()))?;
        let lowered_nested = nested_opt
            .as_ref()
            .map(|nested| lower_nested_select_spec(nested, encoder))
            .transpose()?;
        reverse.insert(sid, lowered_nested);
    }

    Ok(GraphSelectSpec {
        root,
        selections,
        reverse,
        depth: spec.depth,
        has_wildcard: spec.has_wildcard,
    })
}

/// Lower a single selection spec to resolved form
fn lower_selection_spec<E: IriEncoder>(
    spec: &UnresolvedSelectionSpec,
    encoder: &E,
) -> Result<SelectionSpec> {
    match spec {
        UnresolvedSelectionSpec::Id => Ok(SelectionSpec::Id),
        UnresolvedSelectionSpec::Wildcard => Ok(SelectionSpec::Wildcard),
        UnresolvedSelectionSpec::Property { predicate, sub_spec } => {
            let sid = encoder
                .encode_iri(predicate)
                .ok_or_else(|| ParseError::UnknownNamespace(predicate.clone()))?;

            // Lower nested spec (includes both forward and reverse)
            let lowered_sub_spec = sub_spec
                .as_ref()
                .map(|nested| lower_nested_select_spec(nested, encoder))
                .transpose()?;

            Ok(SelectionSpec::Property {
                predicate: sid,
                sub_spec: lowered_sub_spec,
            })
        }
    }
}

/// Lower a nested select spec to resolved form
fn lower_nested_select_spec<E: IriEncoder>(
    spec: &UnresolvedNestedSelectSpec,
    encoder: &E,
) -> Result<Box<NestedSelectSpec>> {
    // Lower forward selections
    let forward = spec
        .forward
        .iter()
        .map(|s| lower_selection_spec(s, encoder))
        .collect::<Result<Vec<_>>>()?;

    // Lower reverse selections (now carries full nested spec, not just forward)
    let mut reverse = std::collections::HashMap::new();
    for (iri, nested_opt) in &spec.reverse {
        let sid = encoder
            .encode_iri(iri)
            .ok_or_else(|| ParseError::UnknownNamespace(iri.clone()))?;
        let lowered_nested = nested_opt
            .as_ref()
            .map(|nested| lower_nested_select_spec(nested, encoder))
            .transpose()?;
        reverse.insert(sid, lowered_nested);
    }

    Ok(Box::new(NestedSelectSpec::new(
        forward,
        reverse,
        spec.has_wildcard,
    )))
}

/// Lower an unresolved filter expression to a resolved FilterExpr
pub(crate) fn lower_filter_expr(
    expr: &UnresolvedFilterExpr,
    vars: &mut VarRegistry,
) -> Result<FilterExpr> {
    match expr {
        UnresolvedFilterExpr::Var(name) => {
            let var_id = vars.get_or_insert(name);
            Ok(FilterExpr::Var(var_id))
        }
        UnresolvedFilterExpr::Const(val) => Ok(FilterExpr::Const(val.into())),
        UnresolvedFilterExpr::Compare { op, left, right } => {
            let lowered_left = lower_filter_expr(left, vars)?;
            let lowered_right = lower_filter_expr(right, vars)?;
            Ok(FilterExpr::Compare {
                op: (*op).into(),
                left: Box::new(lowered_left),
                right: Box::new(lowered_right),
            })
        }
        UnresolvedFilterExpr::Arithmetic { op, left, right } => {
            let lowered_left = lower_filter_expr(left, vars)?;
            let lowered_right = lower_filter_expr(right, vars)?;
            Ok(FilterExpr::Arithmetic {
                op: (*op).into(),
                left: Box::new(lowered_left),
                right: Box::new(lowered_right),
            })
        }
        UnresolvedFilterExpr::Negate(inner) => {
            let lowered = lower_filter_expr(inner, vars)?;
            Ok(FilterExpr::Negate(Box::new(lowered)))
        }
        UnresolvedFilterExpr::And(exprs) => {
            let lowered: Result<Vec<FilterExpr>> =
                exprs.iter().map(|e| lower_filter_expr(e, vars)).collect();
            Ok(FilterExpr::And(lowered?))
        }
        UnresolvedFilterExpr::Or(exprs) => {
            let lowered: Result<Vec<FilterExpr>> =
                exprs.iter().map(|e| lower_filter_expr(e, vars)).collect();
            Ok(FilterExpr::Or(lowered?))
        }
        UnresolvedFilterExpr::Not(inner) => {
            let lowered = lower_filter_expr(inner, vars)?;
            Ok(FilterExpr::Not(Box::new(lowered)))
        }
        UnresolvedFilterExpr::In { expr, values, negated } => {
            let lowered_expr = lower_filter_expr(expr, vars)?;
            let lowered_values: Result<Vec<FilterExpr>> =
                values.iter().map(|v| lower_filter_expr(v, vars)).collect();
            Ok(FilterExpr::In {
                expr: Box::new(lowered_expr),
                values: lowered_values?,
                negated: *negated,
            })
        }
        UnresolvedFilterExpr::Function { name, args } => {
            let lowered_args: Result<Vec<FilterExpr>> =
                args.iter().map(|a| lower_filter_expr(a, vars)).collect();
            let func_name = lower_function_name(name);
            if let FunctionName::Custom(unknown) = &func_name {
                return Err(ParseError::InvalidFilter(format!(
                    "Unknown function: {}",
                    unknown
                )));
            }
            Ok(FilterExpr::Function {
                name: func_name,
                args: lowered_args?,
            })
        }
    }
}

/// Lower a function name string to a FunctionName enum
fn lower_function_name(name: &str) -> FunctionName {
    match name.to_lowercase().as_str() {
        // String functions
        "strlen" => FunctionName::Strlen,
        "substr" | "substring" => FunctionName::Substr,
        "ucase" => FunctionName::Ucase,
        "lcase" => FunctionName::Lcase,
        "contains" => FunctionName::Contains,
        "strstarts" => FunctionName::StrStarts,
        "strends" => FunctionName::StrEnds,
        "regex" => FunctionName::Regex,
        "concat" => FunctionName::Concat,
        "strbefore" => FunctionName::StrBefore,
        "strafter" => FunctionName::StrAfter,
        "replace" => FunctionName::Replace,
        "str" => FunctionName::Str,
        "strdt" | "str-dt" => FunctionName::StrDt,
        "strlang" | "str-lang" => FunctionName::StrLang,
        "encode_for_uri" | "encodeforuri" => FunctionName::EncodeForUri,
        // Numeric functions
        "abs" => FunctionName::Abs,
        "round" => FunctionName::Round,
        "ceil" => FunctionName::Ceil,
        "floor" => FunctionName::Floor,
        "rand" => FunctionName::Rand,
        "iri" => FunctionName::Iri,
        "bnode" => FunctionName::Bnode,
        // DateTime functions
        "now" => FunctionName::Now,
        "year" => FunctionName::Year,
        "month" => FunctionName::Month,
        "day" => FunctionName::Day,
        "hours" => FunctionName::Hours,
        "minutes" => FunctionName::Minutes,
        "seconds" => FunctionName::Seconds,
        "tz" | "timezone" => FunctionName::Tz,
        // Type functions
        "isiri" | "isuri" | "is-iri" | "is-uri" => FunctionName::IsIri,
        "isblank" | "is-blank" => FunctionName::IsBlank,
        "isliteral" | "is-literal" => FunctionName::IsLiteral,
        "isnumeric" | "is-numeric" => FunctionName::IsNumeric,
        // RDF term functions
        "lang" => FunctionName::Lang,
        "datatype" => FunctionName::Datatype,
        "langmatches" => FunctionName::LangMatches,
        "sameterm" => FunctionName::SameTerm,
        // Fluree-specific: transaction time
        "t" => FunctionName::T,
        "op" => FunctionName::Op,
        // Hash functions
        "md5" => FunctionName::Md5,
        "sha1" => FunctionName::Sha1,
        "sha256" => FunctionName::Sha256,
        "sha384" => FunctionName::Sha384,
        "sha512" => FunctionName::Sha512,
        // UUID functions
        "uuid" => FunctionName::Uuid,
        "struuid" => FunctionName::StrUuid,
        // Vector/embedding similarity functions
        "dotproduct" | "dot_product" => FunctionName::DotProduct,
        "cosinesimilarity" | "cosine_similarity" => FunctionName::CosineSimilarity,
        "euclideandistance" | "euclidean_distance" | "euclidiandistance" => {
            FunctionName::EuclideanDistance
        }
        // Other
        "bound" => FunctionName::Bound,
        "if" => FunctionName::If,
        "coalesce" => FunctionName::Coalesce,
        other => FunctionName::Custom(other.to_string()),
    }
}

/// Lower an unresolved term to a resolved Term
fn lower_term<E: IriEncoder>(
    term: &UnresolvedTerm,
    _encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Term> {
    match term {
        UnresolvedTerm::Var(name) => {
            let var_id = vars.get_or_insert(name);
            Ok(Term::Var(var_id))
        }
        UnresolvedTerm::Iri(iri) => {
            // Emit Term::Iri to defer SID encoding to scan time.
            // This enables correct cross-ledger joins where each ledger's namespace
            // table may encode the same IRI differently. The scan operator will
            // encode the IRI per-db using build_range_match_for_db().
            Ok(Term::Iri(iri.clone()))
        }
        UnresolvedTerm::Literal(lit) => {
            let value = lower_literal(lit);
            Ok(Term::Value(value))
        }
    }
}

/// Convert a LiteralValue to a FlakeValue
fn lower_literal(lit: &LiteralValue) -> FlakeValue {
    match lit {
        LiteralValue::String(s) => FlakeValue::String(s.to_string()),
        LiteralValue::Long(l) => FlakeValue::Long(*l),
        LiteralValue::Double(d) => FlakeValue::Double(*d),
        LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        LiteralValue::Vector(v) => FlakeValue::Vector(v.clone()),
    }
}

// ============================================================================
// Query modifier lowering
// ============================================================================

/// Lower an unresolved sort direction to a resolved SortDirection
fn lower_sort_direction(dir: UnresolvedSortDirection) -> SortDirection {
    match dir {
        UnresolvedSortDirection::Asc => SortDirection::Ascending,
        UnresolvedSortDirection::Desc => SortDirection::Descending,
    }
}

/// Lower an unresolved sort spec to a resolved SortSpec
fn lower_sort_spec(spec: &UnresolvedSortSpec, vars: &mut VarRegistry) -> SortSpec {
    let var = vars.get_or_insert(&spec.var);
    let direction = lower_sort_direction(spec.direction);
    SortSpec { var, direction }
}

/// Lower an unresolved aggregate function to a resolved AggregateFn
fn lower_aggregate_fn(f: &UnresolvedAggregateFn) -> AggregateFn {
    match f {
        UnresolvedAggregateFn::Count => AggregateFn::Count,
        UnresolvedAggregateFn::CountDistinct => AggregateFn::CountDistinct,
        UnresolvedAggregateFn::Sum => AggregateFn::Sum,
        UnresolvedAggregateFn::Avg => AggregateFn::Avg,
        UnresolvedAggregateFn::Min => AggregateFn::Min,
        UnresolvedAggregateFn::Max => AggregateFn::Max,
        UnresolvedAggregateFn::Median => AggregateFn::Median,
        UnresolvedAggregateFn::Variance => AggregateFn::Variance,
        UnresolvedAggregateFn::Stddev => AggregateFn::Stddev,
        UnresolvedAggregateFn::GroupConcat { separator } => AggregateFn::GroupConcat {
            separator: separator.clone(),
        },
        UnresolvedAggregateFn::Sample => AggregateFn::Sample,
    }
}

/// Lower an unresolved aggregate spec to a resolved AggregateSpec
fn lower_aggregate_spec(spec: &UnresolvedAggregateSpec, vars: &mut VarRegistry) -> AggregateSpec {
    // Handle COUNT(*) - input="*" means count all rows
    if spec.input_var.as_ref() == "*" {
        // COUNT(*) uses CountAll function and has no input variable
        AggregateSpec {
            function: AggregateFn::CountAll,
            input_var: None,
            output_var: vars.get_or_insert(&spec.output_var),
        }
    } else {
        // Regular aggregate with input variable
        AggregateSpec {
            function: lower_aggregate_fn(&spec.function),
            input_var: Some(vars.get_or_insert(&spec.input_var)),
            output_var: vars.get_or_insert(&spec.output_var),
        }
    }
}

/// Lower unresolved options to resolved QueryOptions
fn lower_options(opts: &UnresolvedOptions, vars: &mut VarRegistry) -> Result<QueryOptions> {
    // Transfer reasoning modes, or use default if not specified
    let reasoning = opts.reasoning.clone().unwrap_or_default();

    Ok(QueryOptions {
        limit: opts.limit,
        offset: opts.offset,
        distinct: opts.distinct,
        order_by: opts
            .order_by
            .iter()
            .map(|s| lower_sort_spec(s, vars))
            .collect(),
        group_by: opts
            .group_by
            .iter()
            .map(|v| vars.get_or_insert(v))
            .collect(),
        aggregates: opts
            .aggregates
            .iter()
            .map(|a| lower_aggregate_spec(a, vars))
            .collect(),
        having: opts
            .having
            .as_ref()
            .map(|e| lower_filter_expr(e, vars))
            .transpose()?,
        post_binds: Vec::new(),
        reasoning,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::encode::MemoryEncoder;
    use fluree_vocab::xsd;

    fn test_encoder() -> MemoryEncoder {
        let mut encoder = MemoryEncoder::with_common_namespaces();
        encoder.add_namespace("http://schema.org/", 100);
        encoder.add_namespace("http://example.org/", 101);
        encoder
    }

    #[test]
    fn test_lower_variable_term() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let term = UnresolvedTerm::var("?name");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();

        assert!(matches!(lowered, Term::Var(VarId(0))));
        assert_eq!(vars.name(VarId(0)), "?name");
    }

    #[test]
    fn test_lower_iri_term() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let term = UnresolvedTerm::iri("http://schema.org/name");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();

        // IRI terms are now lowered to Term::Iri (not Term::Sid) to defer encoding
        // to scan time, enabling correct cross-ledger joins.
        if let Term::Iri(iri) = lowered {
            assert_eq!(iri.as_ref(), "http://schema.org/name");
        } else {
            panic!("Expected Term::Iri, got {:?}", lowered);
        }
    }

    #[test]
    fn test_lower_literal_terms() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // String
        let term = UnresolvedTerm::string("hello");
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::String(s)) if s == "hello"));

        // Long
        let term = UnresolvedTerm::long(42);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::Long(42))));

        // Double
        let term = UnresolvedTerm::double(3.14);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::Double(d)) if (d - 3.14).abs() < f64::EPSILON));

        // Boolean
        let term = UnresolvedTerm::boolean(true);
        let lowered = lower_term(&term, &encoder, &mut vars).unwrap();
        assert!(matches!(lowered, Term::Value(FlakeValue::Boolean(true))));
    }

    #[test]
    fn test_lower_pattern() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let pattern = UnresolvedTriplePattern::new(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/name"),
            UnresolvedTerm::var("?name"),
        );

        let lowered = lower_triple_pattern(&pattern, &encoder, &mut vars).unwrap();

        assert!(matches!(lowered.s, Term::Var(VarId(0))));
        // Predicate IRI is lowered to Term::Iri for deferred encoding
        assert!(matches!(lowered.p, Term::Iri(ref iri) if iri.as_ref() == "http://schema.org/name"));
        assert!(matches!(lowered.o, Term::Var(VarId(1))));
        assert!(lowered.dt.is_none());
    }

    #[test]
    fn test_lower_pattern_with_dt() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let pattern = UnresolvedTriplePattern::with_dt(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/age"),
            UnresolvedTerm::var("?age"),
            xsd::INTEGER,
        );

        let lowered = lower_triple_pattern(&pattern, &encoder, &mut vars).unwrap();

        assert!(lowered.dt.is_some());
        let dt = lowered.dt.unwrap();
        assert_eq!(dt.namespace_code, 2);
        assert_eq!(dt.name.as_ref(), "integer");
    }

    #[test]
    fn test_lower_query() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        let mut ast = UnresolvedQuery::new(ParsedContext::new());
        ast.add_select("?s");
        ast.add_select("?name");
        ast.add_pattern(UnresolvedTriplePattern::new(
            UnresolvedTerm::var("?s"),
            UnresolvedTerm::iri("http://schema.org/name"),
            UnresolvedTerm::var("?name"),
        ));

        let query = lower_query(ast, &encoder, &mut vars, SelectMode::Many).unwrap();

        assert_eq!(query.select.len(), 2);
        assert_eq!(query.patterns.len(), 1);
        assert_eq!(vars.len(), 2);
        assert_eq!(query.select_mode, SelectMode::Many);
    }

    #[test]
    fn test_unknown_namespace_becomes_term_iri() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // Unknown namespace IRIs are now lowered to Term::Iri (not an error).
        // Whether the IRI can be encoded is deferred to scan time, where
        // unknown IRIs simply produce no matches in that ledger.
        let term = UnresolvedTerm::iri("http://unknown.org/thing");
        let result = lower_term(&term, &encoder, &mut vars);

        assert!(result.is_ok());
        let lowered = result.unwrap();
        assert!(matches!(lowered, Term::Iri(ref iri) if iri.as_ref() == "http://unknown.org/thing"));
    }

    #[test]
    fn test_variable_deduplication() {
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();

        // Same variable used twice should get same VarId
        let term1 = UnresolvedTerm::var("?s");
        let term2 = UnresolvedTerm::var("?s");

        let lowered1 = lower_term(&term1, &encoder, &mut vars).unwrap();
        let lowered2 = lower_term(&term2, &encoder, &mut vars).unwrap();

        assert_eq!(lowered1, lowered2);
        assert_eq!(vars.len(), 1);
    }

    // ==========================================================================
    // Typed literal coercion tests
    // ==========================================================================

    #[test]
    fn test_coerce_string_to_integer() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("42".to_string()),
            xsd::INTEGER
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));
    }

    #[test]
    fn test_coerce_string_to_bigint() {
        // String too large for i64
        let big_num = "99999999999999999999999999999";
        let result = coerce_value_by_datatype(
            FlakeValue::String(big_num.to_string()),
            xsd::INTEGER
        );
        assert!(result.is_ok());
        match result.unwrap() {
            FlakeValue::BigInt(bi) => {
                assert_eq!(bi.to_string(), big_num);
            }
            other => panic!("Expected BigInt, got {:?}", other),
        }
    }

    #[test]
    fn test_coerce_string_to_decimal_becomes_bigdecimal() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("3.14159265358979323846".to_string()),
            xsd::DECIMAL
        );
        assert!(result.is_ok());
        match result.unwrap() {
            FlakeValue::Decimal(bd) => {
                // Verify precision is preserved
                assert!(bd.to_string().starts_with("3.14159265358979"));
            }
            other => panic!("Expected Decimal, got {:?}", other),
        }
    }

    #[test]
    fn test_coerce_json_long_to_decimal_becomes_double() {
        // JSON numbers with xsd:decimal → Double (per policy)
        let result = coerce_value_by_datatype(
            FlakeValue::Long(42),
            xsd::DECIMAL
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Double(42.0));
    }

    #[test]
    fn test_coerce_string_to_double() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("3.14".to_string()),
            xsd::DOUBLE
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Double(3.14));
    }

    #[test]
    fn test_coerce_string_to_boolean() {
        let result_true = coerce_value_by_datatype(
            FlakeValue::String("true".to_string()),
            xsd::BOOLEAN
        );
        assert_eq!(result_true.unwrap(), FlakeValue::Boolean(true));

        let result_false = coerce_value_by_datatype(
            FlakeValue::String("false".to_string()),
            xsd::BOOLEAN
        );
        assert_eq!(result_false.unwrap(), FlakeValue::Boolean(false));

        let result_one = coerce_value_by_datatype(
            FlakeValue::String("1".to_string()),
            xsd::BOOLEAN
        );
        assert_eq!(result_one.unwrap(), FlakeValue::Boolean(true));

        let result_zero = coerce_value_by_datatype(
            FlakeValue::String("0".to_string()),
            xsd::BOOLEAN
        );
        assert_eq!(result_zero.unwrap(), FlakeValue::Boolean(false));
    }

    // ==========================================================================
    // Incompatible type coercion errors
    // ==========================================================================

    #[test]
    fn test_coerce_number_to_string_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Long(42),
            xsd::STRING
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_double_to_string_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Double(3.14),
            xsd::STRING
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_boolean_to_string_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Boolean(true),
            xsd::STRING
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_number_to_boolean_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Long(1),
            xsd::BOOLEAN
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_double_to_boolean_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Double(1.0),
            xsd::BOOLEAN
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_non_integral_double_to_integer_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Double(3.14),
            xsd::INTEGER
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::TypeCoercion(_)));
        assert!(err.to_string().contains("non-integer"));
    }

    #[test]
    fn test_coerce_integral_double_to_integer_succeeds() {
        let result = coerce_value_by_datatype(
            FlakeValue::Double(42.0),
            xsd::INTEGER
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));
    }

    #[test]
    fn test_coerce_boolean_to_numeric_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Boolean(true),
            xsd::INTEGER
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_number_to_temporal_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::Long(12345),
            xsd::DATE_TIME
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_integer_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("not-a-number".to_string()),
            xsd::INTEGER
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_decimal_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("not-a-number".to_string()),
            xsd::DECIMAL
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_invalid_string_to_boolean_errors() {
        let result = coerce_value_by_datatype(
            FlakeValue::String("maybe".to_string()),
            xsd::BOOLEAN
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::TypeCoercion(_)));
    }

    #[test]
    fn test_coerce_passthrough_same_type() {
        // Already correct type - should pass through unchanged
        let result = coerce_value_by_datatype(
            FlakeValue::Long(42),
            xsd::INTEGER
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FlakeValue::Long(42));

        let result2 = coerce_value_by_datatype(
            FlakeValue::String("hello".to_string()),
            xsd::STRING
        );
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), FlakeValue::String("hello".to_string()));
    }
}
