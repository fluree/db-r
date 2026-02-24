//! SPARQL UPDATE to Transaction IR lowering.
//!
//! This module converts parsed SPARQL UPDATE AST (`UpdateOperation`) into the
//! Transaction IR (`Txn`) that is shared with JSON-LD transactions.
//!
//! # Architecture
//!
//! ```text
//!        SPARQL UPDATE                    JSON-LD Transaction
//!              │                                  │
//! parse_sparql()                      parse_transaction()
//!              │                                  │
//!              ▼                                  ▼
//!        SPARQL AST                        JSON-LD Value
//!    (UpdateOperation)                           │
//!              │                                  │
//! lower_sparql_update()◄────────────────────────►│
//!              │                                  │
//!              └─────────────► Txn IR ◄──────────┘
//!                       │
//!                       │  (shared from here)
//!                       ▼
//!                   stage()
//!                       │
//!                       ▼
//!                Vec<Flake>
//! ```
//!
//! # MVP Restrictions
//!
//! This implementation supports only triple patterns in UPDATE WHERE clauses.
//! OPTIONAL, FILTER, UNION, VALUES, and other complex pattern types are future work.
//!
//! Additional restrictions:
//! - Blank nodes in WHERE patterns are not supported
//! - WITH/USING clauses are rejected

use std::mem;
use std::sync::Arc;

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::parse::{
    LiteralValue, UnresolvedDatatypeConstraint, UnresolvedPattern, UnresolvedTerm,
    UnresolvedTriplePattern,
};
use fluree_db_query::VarRegistry;
use fluree_db_sparql::ast::{
    BlankNodeValue, Iri, IriValue, Literal, LiteralValue as SparqlLiteralValue, Modify,
    PredicateTerm, Prologue, QuadData, QuadPattern, QueryBody, SparqlAst, SubjectTerm, Term,
    TriplePattern, UpdateOperation, WherePattern,
};
use fluree_db_sparql::SourceSpan;
use thiserror::Error;

use crate::ir::{TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_vocab::xsd;

/// Result of converting a SPARQL term to an unresolved term with metadata.
struct UnresolvedTermWithMeta {
    /// The unresolved term
    term: UnresolvedTerm,
    /// Optional datatype IRI
    datatype: Option<Arc<str>>,
    /// Optional language tag
    lang: Option<Arc<str>>,
}

/// Errors that can occur during SPARQL UPDATE lowering.
#[derive(Debug, Error)]
pub enum LowerError {
    /// Expected SPARQL UPDATE, found a query
    #[error("Expected SPARQL UPDATE, found query")]
    NotAnUpdate { span: SourceSpan },

    /// Blank nodes in WHERE patterns are not supported
    #[error("Blank nodes in WHERE patterns are not supported")]
    BlankNodeInWhere { span: SourceSpan },

    /// Unsupported feature encountered
    #[error("{feature} is not yet supported in SPARQL UPDATE lowering")]
    UnsupportedFeature {
        feature: &'static str,
        span: SourceSpan,
    },

    /// Undefined prefix in IRI
    #[error("Undefined prefix '{prefix}:'")]
    UndefinedPrefix { prefix: String, span: SourceSpan },
}

/// Counter for generating anonymous blank node labels.
struct BlankNodeCounter {
    next: u32,
}

impl BlankNodeCounter {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> String {
        let label = format!("_:b{}", self.next);
        self.next += 1;
        label
    }
}

/// Result of converting a literal to template form.
struct LiteralResult {
    term: TemplateTerm,
    datatype: Option<Sid>,
    language: Option<String>,
}

/// Lower a parsed SPARQL AST to the Transaction IR.
///
/// This is a convenience wrapper that extracts the `UpdateOperation` and `Prologue`
/// from a `SparqlAst` and calls `lower_sparql_update`.
///
/// # Arguments
///
/// * `ast` - The parsed SPARQL AST (must contain an UPDATE operation)
/// * `ns` - The namespace registry for IRI-to-Sid encoding
/// * `opts` - Transaction options (message, author, branch, etc.)
///
/// # Returns
///
/// A `Txn` that can be staged using the shared transaction pipeline.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - The AST body is not an UPDATE operation (is a query)
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update_ast(
    ast: &SparqlAst,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let update_op = match &ast.body {
        QueryBody::Update(op) => op,
        _ => {
            return Err(LowerError::NotAnUpdate { span: ast.span });
        }
    };
    lower_sparql_update(update_op, &ast.prologue, ns, opts)
}

/// Lower a SPARQL UPDATE operation to the Transaction IR.
///
/// # Arguments
///
/// * `op` - The parsed SPARQL UPDATE operation
/// * `prologue` - The prologue containing PREFIX declarations
/// * `ns` - The namespace registry for IRI-to-Sid encoding
/// * `opts` - Transaction options (message, author, branch, etc.)
///
/// # Returns
///
/// A `Txn` that can be staged using the shared transaction pipeline.
///
/// # Errors
///
/// Returns `LowerError` if:
/// - WITH or USING clauses are present
/// - Blank nodes appear in WHERE patterns
/// - RDF-star quoted triples are used
pub fn lower_sparql_update(
    op: &UpdateOperation,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let mut vars = VarRegistry::new();
    let mut bnodes = BlankNodeCounter::new();

    match op {
        UpdateOperation::InsertData(insert) => {
            lower_insert_data(&insert.data, prologue, ns, &mut vars, &mut bnodes, opts)
        }
        UpdateOperation::DeleteData(delete) => {
            lower_delete_data(&delete.data, prologue, ns, &mut vars, &mut bnodes, opts)
        }
        UpdateOperation::DeleteWhere(delete_where) => {
            lower_delete_where(&delete_where.pattern, prologue, ns, &mut vars, opts)
        }
        UpdateOperation::Modify(modify) => {
            lower_modify(modify, prologue, ns, &mut vars, &mut bnodes, opts)
        }
    }
}

/// Lower INSERT DATA operation.
///
/// INSERT DATA contains ground triples (no variables) that are directly inserted.
fn lower_insert_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let insert_templates = lower_triples_to_templates(&data.triples, prologue, ns, vars, bnodes)?;

    Ok(Txn {
        txn_type: TxnType::Insert,
        where_patterns: Vec::new(),
        delete_templates: Vec::new(),
        insert_templates,
        values: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: Default::default(),
    })
}

/// Lower DELETE DATA operation.
///
/// DELETE DATA contains ground triples (no variables) that are retracted.
/// Uses TxnType::Update because it's a retract-only transaction.
fn lower_delete_data(
    data: &QuadData,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    let delete_templates = lower_triples_to_templates(&data.triples, prologue, ns, vars, bnodes)?;

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns: Vec::new(),
        delete_templates,
        insert_templates: Vec::new(),
        values: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: Default::default(),
    })
}

/// Lower DELETE WHERE operation.
///
/// DELETE WHERE uses the same pattern for matching and deletion.
/// The pattern becomes both the WHERE clause and the DELETE template.
fn lower_delete_where(
    pattern: &QuadPattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // Convert triples to WHERE patterns (UnresolvedPattern)
    let where_patterns = lower_triples_to_where(&pattern.triples, prologue)?;

    // For DELETE WHERE, the delete templates mirror the WHERE patterns
    // We need to create templates from the same triples
    let mut bnodes = BlankNodeCounter::new();
    let delete_templates =
        lower_triples_to_templates(&pattern.triples, prologue, ns, vars, &mut bnodes)?;

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns,
        delete_templates,
        insert_templates: Vec::new(),
        values: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: Default::default(),
    })
}

/// Lower Modify operation (DELETE/INSERT with WHERE).
///
/// The most general update form with optional WITH, DELETE, INSERT, and WHERE clauses.
fn lower_modify(
    modify: &Modify,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
    opts: TxnOpts,
) -> Result<Txn, LowerError> {
    // Reject WITH clause
    if let Some(with_iri) = &modify.with_iri {
        return Err(LowerError::UnsupportedFeature {
            feature: "WITH clause",
            span: with_iri.span,
        });
    }

    // Reject USING clause
    if let Some(using) = &modify.using {
        return Err(LowerError::UnsupportedFeature {
            feature: "USING clause",
            span: using.span,
        });
    }

    // Lower WHERE patterns
    let where_patterns = lower_where_pattern(&modify.where_clause, prologue)?;

    // Lower DELETE templates (if present)
    let delete_templates = if let Some(delete_clause) = &modify.delete_clause {
        lower_triples_to_templates(&delete_clause.triples, prologue, ns, vars, bnodes)?
    } else {
        Vec::new()
    };

    // Lower INSERT templates (if present)
    let insert_templates = if let Some(insert_clause) = &modify.insert_clause {
        lower_triples_to_templates(&insert_clause.triples, prologue, ns, vars, bnodes)?
    } else {
        Vec::new()
    };

    Ok(Txn {
        txn_type: TxnType::Update,
        where_patterns,
        delete_templates,
        insert_templates,
        values: None,
        opts,
        vars: mem::take(vars),
        txn_meta: Vec::new(),
        graph_delta: Default::default(),
    })
}

/// Lower a WherePattern to Vec<UnresolvedPattern>.
fn lower_where_pattern(
    where_pattern: &WherePattern,
    prologue: &Prologue,
) -> Result<Vec<UnresolvedPattern>, LowerError> {
    lower_triples_to_where(&where_pattern.triples, prologue)
}

/// Lower triple patterns to UnresolvedPattern for WHERE clauses.
fn lower_triples_to_where(
    triples: &[TriplePattern],
    prologue: &Prologue,
) -> Result<Vec<UnresolvedPattern>, LowerError> {
    triples
        .iter()
        .map(|tp| lower_triple_to_where(tp, prologue))
        .collect()
}

/// Lower a single triple pattern to UnresolvedPattern.
fn lower_triple_to_where(
    triple: &TriplePattern,
    prologue: &Prologue,
) -> Result<UnresolvedPattern, LowerError> {
    let s = subject_to_unresolved(&triple.subject, prologue)?;
    let p = predicate_to_unresolved(&triple.predicate, prologue)?;
    let obj = object_to_unresolved(&triple.object, prologue)?;

    let constraint = if let Some(lang) = obj.lang {
        Some(UnresolvedDatatypeConstraint::LangTag(lang))
    } else {
        obj.datatype.map(UnresolvedDatatypeConstraint::Explicit)
    };

    let pattern = UnresolvedTriplePattern {
        s,
        p,
        o: obj.term,
        dtc: constraint,
    };

    Ok(UnresolvedPattern::Triple(pattern))
}

/// Lower triple patterns to TripleTemplate for DELETE/INSERT templates.
fn lower_triples_to_templates(
    triples: &[TriplePattern],
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<Vec<TripleTemplate>, LowerError> {
    triples
        .iter()
        .map(|tp| lower_triple_to_template(tp, prologue, ns, vars, bnodes))
        .collect()
}

/// Lower a single triple pattern to TripleTemplate.
fn lower_triple_to_template(
    triple: &TriplePattern,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TripleTemplate, LowerError> {
    let subject = subject_to_template(&triple.subject, prologue, ns, vars, bnodes)?;
    let predicate = predicate_to_template(&triple.predicate, prologue, ns, vars)?;

    // Object needs special handling for literal metadata
    let (object, datatype, language) = match &triple.object {
        Term::Literal(lit) => {
            let result = literal_to_template(lit, prologue, ns)?;
            (result.term, result.datatype, result.language)
        }
        other => (
            object_to_template(other, prologue, ns, vars, bnodes)?,
            None,
            None,
        ),
    };

    Ok(TripleTemplate {
        subject,
        predicate,
        object,
        datatype,
        language,
        list_index: None, // Always None for SPARQL UPDATE
        graph_id: None,   // Default graph
    })
}

// =============================================================================
// Term conversion for WHERE patterns (UnresolvedTerm)
// =============================================================================

/// Convert SPARQL SubjectTerm to UnresolvedTerm (for WHERE patterns).
fn subject_to_unresolved(
    term: &SubjectTerm,
    prologue: &Prologue,
) -> Result<UnresolvedTerm, LowerError> {
    match term {
        SubjectTerm::Var(v) => Ok(UnresolvedTerm::Var(Arc::from(format!("?{}", v.name)))),
        SubjectTerm::Iri(iri) => Ok(UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?))),
        SubjectTerm::BlankNode(bn) => Err(LowerError::BlankNodeInWhere { span: bn.span }),
        SubjectTerm::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF-star quoted triple",
            span: qt.span,
        }),
    }
}

/// Convert SPARQL PredicateTerm to UnresolvedTerm.
fn predicate_to_unresolved(
    term: &PredicateTerm,
    prologue: &Prologue,
) -> Result<UnresolvedTerm, LowerError> {
    match term {
        PredicateTerm::Var(v) => Ok(UnresolvedTerm::Var(Arc::from(format!("?{}", v.name)))),
        PredicateTerm::Iri(iri) => Ok(UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?))),
    }
}

/// Convert SPARQL Term (object position) to UnresolvedTerm with metadata.
fn object_to_unresolved(
    term: &Term,
    prologue: &Prologue,
) -> Result<UnresolvedTermWithMeta, LowerError> {
    match term {
        Term::Var(v) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Var(Arc::from(format!("?{}", v.name))),
            datatype: None,
            lang: None,
        }),
        Term::Iri(iri) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Iri(Arc::from(expand_iri(iri, prologue)?)),
            datatype: None,
            lang: None,
        }),
        Term::Literal(lit) => literal_to_unresolved(lit, prologue),
        Term::BlankNode(bn) => Err(LowerError::BlankNodeInWhere { span: bn.span }),
    }
}

/// Convert SPARQL Literal to UnresolvedTerm with metadata.
fn literal_to_unresolved(
    lit: &Literal,
    prologue: &Prologue,
) -> Result<UnresolvedTermWithMeta, LowerError> {
    match &lit.value {
        SparqlLiteralValue::Simple(s) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref()))),
            datatype: None,
            lang: None,
        }),
        SparqlLiteralValue::LangTagged { value, lang } => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::String(Arc::from(value.as_ref()))),
            datatype: None,
            lang: Some(Arc::from(lang.as_ref())),
        }),
        SparqlLiteralValue::Typed { value, datatype } => {
            let dt_iri = expand_iri(datatype, prologue)?;
            let coerced = coerce_typed_value(value, &dt_iri);
            Ok(UnresolvedTermWithMeta {
                term: coerced,
                datatype: Some(Arc::from(dt_iri)),
                lang: None,
            })
        }
        SparqlLiteralValue::Integer(i) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Long(*i)),
            datatype: Some(Arc::from(xsd::INTEGER)),
            lang: None,
        }),
        SparqlLiteralValue::Double(d) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Double(*d)),
            datatype: Some(Arc::from(xsd::DOUBLE)),
            lang: None,
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Try to parse as f64; on failure, keep as string with datatype
            let term = match s.parse::<f64>() {
                Ok(d) => UnresolvedTerm::Literal(LiteralValue::Double(d)),
                Err(_) => UnresolvedTerm::Literal(LiteralValue::String(Arc::from(s.as_ref()))),
            };
            Ok(UnresolvedTermWithMeta {
                term,
                datatype: Some(Arc::from(xsd::DECIMAL)),
                lang: None,
            })
        }
        SparqlLiteralValue::Boolean(b) => Ok(UnresolvedTermWithMeta {
            term: UnresolvedTerm::Literal(LiteralValue::Boolean(*b)),
            datatype: Some(Arc::from(xsd::BOOLEAN)),
            lang: None,
        }),
    }
}

// =============================================================================
// Term conversion for DELETE/INSERT templates (TemplateTerm)
// =============================================================================

/// Convert SPARQL SubjectTerm to TemplateTerm (for DELETE/INSERT templates).
fn subject_to_template(
    term: &SubjectTerm,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TemplateTerm, LowerError> {
    match term {
        SubjectTerm::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        SubjectTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
        SubjectTerm::BlankNode(bn) => {
            let label = match &bn.value {
                BlankNodeValue::Labeled(l) => format!("_:{}", l),
                BlankNodeValue::Anon => bnodes.next(),
            };
            Ok(TemplateTerm::BlankNode(label))
        }
        SubjectTerm::QuotedTriple(qt) => Err(LowerError::UnsupportedFeature {
            feature: "RDF-star quoted triple",
            span: qt.span,
        }),
    }
}

/// Convert SPARQL PredicateTerm to TemplateTerm.
fn predicate_to_template(
    term: &PredicateTerm,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
) -> Result<TemplateTerm, LowerError> {
    match term {
        PredicateTerm::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        PredicateTerm::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
    }
}

/// Convert SPARQL Term (object position) to TemplateTerm.
///
/// Note: For literal terms, use the metadata-aware path in `lower_triple_to_template`
/// which calls `literal_to_template` directly to preserve datatype/language.
fn object_to_template(
    term: &Term,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
    vars: &mut VarRegistry,
    bnodes: &mut BlankNodeCounter,
) -> Result<TemplateTerm, LowerError> {
    match term {
        Term::Var(v) => {
            let var_name = format!("?{}", v.name);
            Ok(TemplateTerm::Var(vars.get_or_insert(&var_name)))
        }
        Term::Iri(iri) => {
            let expanded = expand_iri(iri, prologue)?;
            Ok(TemplateTerm::Sid(ns.sid_for_iri(&expanded)))
        }
        // Literals should go through literal_to_template for metadata; this is a fallback
        Term::Literal(lit) => Ok(literal_to_template(lit, prologue, ns)?.term),
        Term::BlankNode(bn) => {
            let label = match &bn.value {
                BlankNodeValue::Labeled(l) => format!("_:{}", l),
                BlankNodeValue::Anon => bnodes.next(),
            };
            Ok(TemplateTerm::BlankNode(label))
        }
    }
}

/// Convert SPARQL Literal to TemplateTerm with datatype/language metadata.
fn literal_to_template(
    lit: &Literal,
    prologue: &Prologue,
    ns: &mut NamespaceRegistry,
) -> Result<LiteralResult, LowerError> {
    match &lit.value {
        SparqlLiteralValue::Simple(s) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::String(s.to_string())),
            datatype: None,
            language: None,
        }),
        SparqlLiteralValue::LangTagged { value, lang } => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::String(value.to_string())),
            datatype: None,
            language: Some(lang.to_string()),
        }),
        SparqlLiteralValue::Typed { value, datatype } => {
            let dt_iri = expand_iri(datatype, prologue)?;
            let dt_sid = ns.sid_for_iri(&dt_iri);
            let coerced = coerce_typed_flake_value(value, &dt_iri);
            Ok(LiteralResult {
                term: TemplateTerm::Value(coerced),
                datatype: Some(dt_sid),
                language: None,
            })
        }
        SparqlLiteralValue::Integer(i) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Long(*i)),
            datatype: Some(ns.sid_for_iri(xsd::INTEGER)),
            language: None,
        }),
        SparqlLiteralValue::Double(d) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Double(*d)),
            datatype: Some(ns.sid_for_iri(xsd::DOUBLE)),
            language: None,
        }),
        SparqlLiteralValue::Decimal(s) => {
            // Try to parse as f64; on failure, keep as string with datatype
            let term = match s.parse::<f64>() {
                Ok(d) => TemplateTerm::Value(FlakeValue::Double(d)),
                Err(_) => TemplateTerm::Value(FlakeValue::String(s.to_string())),
            };
            Ok(LiteralResult {
                term,
                datatype: Some(ns.sid_for_iri(xsd::DECIMAL)),
                language: None,
            })
        }
        SparqlLiteralValue::Boolean(b) => Ok(LiteralResult {
            term: TemplateTerm::Value(FlakeValue::Boolean(*b)),
            datatype: Some(ns.sid_for_iri(xsd::BOOLEAN)),
            language: None,
        }),
    }
}

// =============================================================================
// IRI expansion
// =============================================================================

/// Expand an IRI using prologue PREFIX declarations.
fn expand_iri(iri: &Iri, prologue: &Prologue) -> Result<String, LowerError> {
    match &iri.value {
        IriValue::Full(full) => Ok(full.to_string()),
        IriValue::Prefixed { prefix, local } => {
            // Look up prefix in prologue
            for decl in &prologue.prefixes {
                if decl.prefix.as_ref() == prefix.as_ref() {
                    return Ok(format!("{}{}", decl.iri, local));
                }
            }
            // Undefined prefix is an error
            Err(LowerError::UndefinedPrefix {
                prefix: prefix.to_string(),
                span: iri.span,
            })
        }
    }
}

// =============================================================================
// Typed value coercion
// =============================================================================

/// Coerce a typed literal lexical value to UnresolvedTerm.
fn coerce_typed_value(lexical: &str, datatype_iri: &str) -> UnresolvedTerm {
    // MVP: basic coercion for common types
    match datatype_iri {
        xsd::INTEGER => {
            if let Ok(i) = lexical.parse::<i64>() {
                return UnresolvedTerm::Literal(LiteralValue::Long(i));
            }
        }
        xsd::DOUBLE | xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<f64>() {
                return UnresolvedTerm::Literal(LiteralValue::Double(d));
            }
        }
        xsd::BOOLEAN => {
            if lexical == "true" || lexical == "1" {
                return UnresolvedTerm::Literal(LiteralValue::Boolean(true));
            } else if lexical == "false" || lexical == "0" {
                return UnresolvedTerm::Literal(LiteralValue::Boolean(false));
            }
        }
        _ => {}
    }
    // Fall back to string
    UnresolvedTerm::Literal(LiteralValue::String(Arc::from(lexical)))
}

/// Coerce a typed literal lexical value to FlakeValue.
fn coerce_typed_flake_value(lexical: &str, datatype_iri: &str) -> FlakeValue {
    // MVP: basic coercion for common types
    match datatype_iri {
        xsd::INTEGER => {
            if let Ok(i) = lexical.parse::<i64>() {
                return FlakeValue::Long(i);
            }
        }
        xsd::DOUBLE | xsd::DECIMAL => {
            if let Ok(d) = lexical.parse::<f64>() {
                return FlakeValue::Double(d);
            }
        }
        xsd::BOOLEAN => {
            if lexical == "true" || lexical == "1" {
                return FlakeValue::Boolean(true);
            } else if lexical == "false" || lexical == "0" {
                return FlakeValue::Boolean(false);
            }
        }
        _ => {}
    }
    // Fall back to string
    FlakeValue::String(lexical.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_sparql::ast::PrefixDecl;
    use fluree_db_sparql::SourceSpan;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    fn test_prologue() -> Prologue {
        Prologue {
            base: None,
            prefixes: vec![PrefixDecl {
                prefix: Arc::from("ex"),
                iri: Arc::from("http://example.org/"),
                span: test_span(),
            }],
        }
    }

    #[test]
    fn test_expand_full_iri() {
        let iri = Iri::full("http://example.org/test", test_span());
        let prologue = test_prologue();
        assert_eq!(
            expand_iri(&iri, &prologue).unwrap(),
            "http://example.org/test"
        );
    }

    #[test]
    fn test_expand_prefixed_iri() {
        let iri = Iri::prefixed("ex", "name", test_span());
        let prologue = test_prologue();
        assert_eq!(
            expand_iri(&iri, &prologue).unwrap(),
            "http://example.org/name"
        );
    }

    #[test]
    fn test_expand_undefined_prefix_error() {
        let iri = Iri::prefixed("unknown", "name", test_span());
        let prologue = test_prologue();
        let result = expand_iri(&iri, &prologue);
        assert!(matches!(
            result,
            Err(LowerError::UndefinedPrefix { prefix, .. }) if prefix == "unknown"
        ));
    }

    #[test]
    fn test_variable_normalization() {
        use fluree_db_sparql::ast::Var;

        let var = Var::new("name", test_span());
        let prologue = test_prologue();
        let subject = SubjectTerm::Var(var);

        let result = subject_to_unresolved(&subject, &prologue).unwrap();
        match result {
            UnresolvedTerm::Var(name) => assert_eq!(name.as_ref(), "?name"),
            _ => panic!("Expected variable term"),
        }
    }

    #[test]
    fn test_blank_node_in_where_error() {
        use fluree_db_sparql::ast::BlankNode;

        let bn = BlankNode::labeled("b1", test_span());
        let prologue = test_prologue();
        let subject = SubjectTerm::BlankNode(bn);

        let result = subject_to_unresolved(&subject, &prologue);
        assert!(matches!(result, Err(LowerError::BlankNodeInWhere { .. })));
    }

    #[test]
    fn test_coerce_typed_integer() {
        let result = coerce_typed_value("42", xsd::INTEGER);
        assert!(matches!(
            result,
            UnresolvedTerm::Literal(LiteralValue::Long(42))
        ));
    }

    #[test]
    fn test_coerce_typed_boolean() {
        let result = coerce_typed_value("true", xsd::BOOLEAN);
        assert!(matches!(
            result,
            UnresolvedTerm::Literal(LiteralValue::Boolean(true))
        ));
    }

    #[test]
    fn test_blank_node_counter() {
        let mut counter = BlankNodeCounter::new();
        assert_eq!(counter.next(), "_:b0");
        assert_eq!(counter.next(), "_:b1");
        assert_eq!(counter.next(), "_:b2");
    }
}
