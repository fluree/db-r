//! Term and IRI lowering.
//!
//! Handles lowering of SPARQL terms (variables, IRIs, literals, blank nodes)
//! to the query engine's `Term` type, as well as IRI expansion and variable
//! registration.

use crate::ast::term::{
    BlankNodeValue, Iri, IriValue, Literal, LiteralValue, ObjectTerm, PredicateTerm, SubjectTerm,
    Term as SparqlTerm, Var,
};
use crate::ast::TriplePattern as SparqlTriplePattern;

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::pattern::{Term, TriplePattern};
use fluree_db_query::var_registry::VarId;
use fluree_vocab::namespaces::{RDF, XSD};
use fluree_vocab::{rdf_names, xsd_names};


use super::{LowerError, LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    /// Register a SPARQL variable with the variable registry.
    pub(super) fn register_var(&mut self, v: &Var) -> VarId {
        self.vars.get_or_insert(&format!("?{}", v.name))
    }

    pub(super) fn lower_triple_pattern(
        &mut self,
        tp: &SparqlTriplePattern,
    ) -> Result<TriplePattern> {
        let s = self.lower_subject(&tp.subject)?;
        let p = self.lower_predicate(&tp.predicate)?;
        let o = self.lower_object(&tp.object)?;
        Ok(TriplePattern::new(s, p, o))
    }

    pub(super) fn lower_subject(&mut self, term: &SubjectTerm) -> Result<Term> {
        match term {
            SubjectTerm::Var(v) => Ok(self.lower_var(v)),
            SubjectTerm::Iri(iri) => self.lower_iri(iri),
            SubjectTerm::BlankNode(bn) => match &bn.value {
                BlankNodeValue::Labeled(label) => {
                    let var_id = self.vars.get_or_insert(&format!("_:{}", label));
                    Ok(Term::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:b{}", self.vars.len()));
                    Ok(Term::Var(var_id))
                }
            },
            SubjectTerm::QuotedTriple(_qt) => {
                // This path is reached when a quoted triple appears in a context
                // other than a top-level BGP subject with f:t/f:op predicates.
                //
                // Supported case (handled in lower_bgp_with_rdf_star):
                //   << ex:s ex:p ?o >> f:t ?t ; f:op ?op .
                //
                // Unsupported cases that reach this error:
                //   - Nested quoted triples: << << ex:s ex:p ?o >> ex:annotatedBy ?who >> ...
                //   - Quoted triples in property paths: ?s ex:path+/<< ex:s ex:p ?o >> ...
                //   - Quoted triples converted to generic Term in unsupported contexts
                //
                // Full RDF-star support would require reifying quoted triples.
                Err(LowerError::not_implemented(
                    "RDF-star quoted triples in this context (only top-level BGP with f:t/f:op annotations supported)",
                    term.span(),
                ))
            }
        }
    }

    pub(super) fn lower_predicate(&mut self, term: &PredicateTerm) -> Result<Term> {
        match term {
            PredicateTerm::Var(v) => Ok(self.lower_var(v)),
            PredicateTerm::Iri(iri) => self.lower_iri(iri),
        }
    }

    pub(super) fn lower_object(&mut self, term: &ObjectTerm) -> Result<Term> {
        match term {
            SparqlTerm::Var(v) => Ok(self.lower_var(v)),
            SparqlTerm::Iri(iri) => self.lower_iri(iri),
            SparqlTerm::Literal(lit) => self.lower_literal(lit),
            SparqlTerm::BlankNode(bn) => match &bn.value {
                BlankNodeValue::Labeled(label) => {
                    let var_id = self.vars.get_or_insert(&format!("_:{}", label));
                    Ok(Term::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:b{}", self.vars.len()));
                    Ok(Term::Var(var_id))
                }
            },
        }
    }

    pub(super) fn lower_var(&mut self, var: &Var) -> Term {
        Term::Var(self.register_var(var))
    }

    pub(super) fn lower_iri(&mut self, iri: &Iri) -> Result<Term> {
        let full_iri = self.expand_iri(iri)?;
        let sid = self
            .encoder
            .encode_iri(&full_iri)
            .ok_or_else(|| LowerError::unknown_namespace(&full_iri, iri.span))?;
        Ok(Term::Sid(sid))
    }

    fn lower_literal(&self, lit: &Literal) -> Result<Term> {
        let value = match &lit.value {
            LiteralValue::Simple(s) => FlakeValue::String(s.to_string()),
            LiteralValue::LangTagged { value, .. } => {
                // Language-tagged strings become plain strings for now
                FlakeValue::String(value.to_string())
            }
            LiteralValue::Typed { value, datatype } => {
                self.lower_typed_literal(value, datatype)?
            }
            LiteralValue::Integer(i) => FlakeValue::Long(*i),
            LiteralValue::Decimal(d) => {
                let val: f64 = d
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                FlakeValue::Double(val)
            }
            LiteralValue::Double(d) => FlakeValue::Double(*d),
            LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        };
        Ok(Term::Value(value))
    }

    fn lower_typed_literal(&self, value: &str, datatype: &Iri) -> Result<FlakeValue> {
        let dt_iri = self.expand_iri(datatype)?;

        match dt_iri.as_str() {
            "http://www.w3.org/2001/XMLSchema#string" => Ok(FlakeValue::String(value.to_string())),
            "http://www.w3.org/2001/XMLSchema#integer"
            | "http://www.w3.org/2001/XMLSchema#int"
            | "http://www.w3.org/2001/XMLSchema#long" => {
                let i: i64 = value
                    .parse()
                    .map_err(|_| LowerError::invalid_integer(value, datatype.span))?;
                Ok(FlakeValue::Long(i))
            }
            "http://www.w3.org/2001/XMLSchema#decimal"
            | "http://www.w3.org/2001/XMLSchema#double"
            | "http://www.w3.org/2001/XMLSchema#float" => {
                let d: f64 = value
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(value, datatype.span))?;
                Ok(FlakeValue::Double(d))
            }
            "http://www.w3.org/2001/XMLSchema#boolean" => {
                let b = value == "true" || value == "1";
                Ok(FlakeValue::Boolean(b))
            }
            _ => {
                // Default to string for unknown datatypes
                Ok(FlakeValue::String(value.to_string()))
            }
        }
    }

    pub(super) fn expand_iri(&self, iri: &Iri) -> Result<String> {
        match &iri.value {
            IriValue::Full(s) => {
                // Handle relative IRIs
                if let Some(base) = &self.base {
                    if !s.contains("://") && !s.starts_with('#') {
                        return Ok(format!("{}{}", base, s));
                    }
                }
                Ok(s.to_string())
            }
            IriValue::Prefixed { prefix, local } => {
                let ns = self
                    .prefixes
                    .get(prefix.as_ref())
                    .ok_or_else(|| LowerError::undefined_prefix(prefix.clone(), iri.span))?;
                Ok(format!("{}{}", ns, local))
            }
        }
    }

    /// Convert a SPARQL term to a Binding (for VALUES rows).
    pub(super) fn term_to_binding(&mut self, term: &SparqlTerm) -> Result<Binding> {
        match term {
            SparqlTerm::Iri(iri) => {
                let full_iri = self.expand_iri(iri)?;
                let sid = self
                    .encoder
                    .encode_iri(&full_iri)
                    .ok_or_else(|| LowerError::unknown_namespace(&full_iri, iri.span))?;
                Ok(Binding::Sid(sid))
            }
            SparqlTerm::Literal(lit) => match &lit.value {
                LiteralValue::Simple(s) => Ok(Binding::lit(
                    FlakeValue::String(s.to_string()),
                    Sid::new(XSD, xsd_names::STRING),
                )),
                LiteralValue::LangTagged { value, lang } => Ok(Binding::lit_lang(
                    FlakeValue::String(value.to_string()),
                    Sid::new(RDF, rdf_names::LANG_STRING),
                    lang.clone(),
                )),
                LiteralValue::Integer(i) => Ok(Binding::lit(
                    FlakeValue::Long(*i),
                    Sid::new(XSD, xsd_names::LONG),
                )),
                LiteralValue::Double(d) => Ok(Binding::lit(
                    FlakeValue::Double(*d),
                    Sid::new(XSD, xsd_names::DOUBLE),
                )),
                LiteralValue::Boolean(b) => Ok(Binding::lit(
                    FlakeValue::Boolean(*b),
                    Sid::new(XSD, xsd_names::BOOLEAN),
                )),
                LiteralValue::Decimal(d) => {
                    let val: f64 = d
                        .parse()
                        .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                    Ok(Binding::lit(
                        FlakeValue::Double(val),
                        Sid::new(XSD, xsd_names::DECIMAL),
                    ))
                }
                LiteralValue::Typed { value, .. } => {
                    // For typed literals in VALUES, treat as string
                    Ok(Binding::lit(
                        FlakeValue::String(value.to_string()),
                        Sid::new(XSD, xsd_names::STRING),
                    ))
                }
            },
            SparqlTerm::Var(_) => {
                // Variables shouldn't appear in VALUES data
                Ok(Binding::Unbound)
            }
            SparqlTerm::BlankNode(_) => {
                // Blank nodes in VALUES treated as unbound
                Ok(Binding::Unbound)
            }
        }
    }
}
