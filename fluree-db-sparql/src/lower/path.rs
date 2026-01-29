//! Property path lowering.
//!
//! Converts SPARQL property path patterns (e.g., `?s ex:parent+ ?o`) to the
//! query engine's `PropertyPathPattern` representation. Currently supports
//! one-or-more (`+`) and zero-or-more (`*`) modifiers on simple IRI predicates.

use crate::ast::path::PropertyPath as SparqlPropertyPath;
use crate::ast::term::{ObjectTerm, SubjectTerm};
use crate::span::SourceSpan;

use fluree_db_query::ir::{PathModifier, Pattern, PropertyPathPattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::pattern::Term;
use fluree_vocab::rdf::TYPE;

use super::{LowerError, LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub(super) fn lower_property_path(
        &mut self,
        subject: &SubjectTerm,
        path: &SparqlPropertyPath,
        object: &ObjectTerm,
        span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        // Extract the IRI and modifier from the path
        let (predicate_iri, modifier) = self.extract_path_iri_and_modifier(path, span)?;

        // Lower subject term
        let s = self.lower_subject(subject)?;

        // Validate subject is not a literal
        if matches!(s, Term::Value(_)) {
            return Err(LowerError::invalid_property_path(
                "Property path subject cannot be a literal value",
                span,
            ));
        }

        // Lower object term
        let o = self.lower_object(object)?;

        // Validate object is not a literal
        if matches!(o, Term::Value(_)) {
            return Err(LowerError::invalid_property_path(
                "Property path object cannot be a literal value",
                span,
            ));
        }

        // Validate at least one is a variable
        if s.is_bound() && o.is_bound() {
            return Err(LowerError::invalid_property_path(
                "Property path requires at least one variable (cannot have both subject and object as constants)",
                span,
            ));
        }

        // Encode the predicate IRI to a Sid
        let predicate_sid = self
            .encoder
            .encode_iri(&predicate_iri)
            .ok_or_else(|| LowerError::unknown_namespace(&predicate_iri, span))?;

        // Build the PropertyPathPattern
        let pp = PropertyPathPattern::new(s, predicate_sid, modifier, o);

        Ok(vec![Pattern::PropertyPath(pp)])
    }

    /// Extract the IRI and modifier from a property path.
    ///
    /// Only supports simple paths with a single IRI and optional `+` or `*` modifier.
    fn extract_path_iri_and_modifier(
        &mut self,
        path: &SparqlPropertyPath,
        span: SourceSpan,
    ) -> Result<(String, PathModifier)> {
        match path {
            // Simple IRI without modifier shouldn't appear in a Path pattern
            SparqlPropertyPath::Iri(_) => Err(LowerError::invalid_property_path(
                "Simple predicate should be a triple pattern, not a property path. Use + or * modifier for transitive paths.",
                span,
            )),

            // The `a` keyword without modifier
            SparqlPropertyPath::A { span: a_span } => Err(LowerError::invalid_property_path(
                "Simple 'a' predicate should be a triple pattern, not a property path. Use + or * modifier for transitive paths.",
                *a_span,
            )),

            // One or more: path+
            SparqlPropertyPath::OneOrMore { path: inner, .. } => {
                let iri = self.extract_simple_predicate_iri(inner, span)?;
                Ok((iri, PathModifier::OneOrMore))
            }

            // Zero or more: path*
            SparqlPropertyPath::ZeroOrMore { path: inner, .. } => {
                let iri = self.extract_simple_predicate_iri(inner, span)?;
                Ok((iri, PathModifier::ZeroOrMore))
            }

            // Zero or one: path? - not supported for transitive paths
            SparqlPropertyPath::ZeroOrOne { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Optional (?) property paths",
                    *op_span,
                ))
            }

            // Inverse: ^path - not yet supported
            SparqlPropertyPath::Inverse { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Inverse (^) property paths",
                    *op_span,
                ))
            }

            // Sequence: path/path - not yet supported
            SparqlPropertyPath::Sequence { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Sequence (/) property paths",
                    *op_span,
                ))
            }

            // Alternative: path|path - not yet supported
            SparqlPropertyPath::Alternative { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Alternative (|) property paths",
                    *op_span,
                ))
            }

            // Negated property set - not supported by Fluree
            SparqlPropertyPath::NegatedSet { span: op_span, .. } => {
                Err(LowerError::not_implemented(
                    "Negated property sets (!)",
                    *op_span,
                ))
            }

            // Grouped path - unwrap and process
            SparqlPropertyPath::Group { path: inner, .. } => {
                self.extract_path_iri_and_modifier(inner, span)
            }
        }
    }

    /// Extract a simple predicate IRI from a property path.
    ///
    /// The path must be a simple IRI or the `a` keyword.
    fn extract_simple_predicate_iri(
        &mut self,
        path: &SparqlPropertyPath,
        span: SourceSpan,
    ) -> Result<String> {
        match path {
            SparqlPropertyPath::Iri(iri) => self.expand_iri(iri),

            SparqlPropertyPath::A { .. } => {
                // `a` is shorthand for rdf:type
                Ok(TYPE.to_string())
            }

            SparqlPropertyPath::Group { path: inner, .. } => {
                self.extract_simple_predicate_iri(inner, span)
            }

            // Any other path form is not a simple predicate
            _ => Err(LowerError::invalid_property_path(
                "Transitive paths (+, *) require a simple predicate IRI, not a complex path expression",
                span,
            )),
        }
    }
}
