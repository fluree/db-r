//! Pattern types for query representation
//!
//! Patterns represent the logical query structure. The planner transforms
//! patterns into physical operators.

use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, Sid};
use fluree_vocab::rdf;
use std::sync::Arc;

/// A term in a triple pattern - variable, constant SID, IRI, or constant value
#[derive(Clone, Debug, PartialEq)]
pub enum Term {
    /// Variable binding
    Var(VarId),
    /// Constant SID (subject, predicate, or ref object)
    Sid(Sid),
    /// Constant IRI (for cross-ledger joins where SID must be encoded per-graph)
    ///
    /// Used when an IriMatch binding is substituted into a pattern during join.
    /// The scan operator will encode this IRI for each target ledger's namespace table.
    Iri(Arc<str>),
    /// Constant value (literal object)
    Value(FlakeValue),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, Term::Var(_))
    }

    /// Check if this term is bound (not a variable)
    pub fn is_bound(&self) -> bool {
        !self.is_var()
    }

    /// Get the variable if this is a Var term
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            Term::Var(v) => Some(*v),
            _ => None,
        }
    }

    /// Get the SID if this is a Sid term
    pub fn as_sid(&self) -> Option<&Sid> {
        match self {
            Term::Sid(s) => Some(s),
            _ => None,
        }
    }

    /// Get the IRI if this is an Iri term
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Get the value if this is a Value term
    pub fn as_value(&self) -> Option<&FlakeValue> {
        match self {
            Term::Value(v) => Some(v),
            _ => None,
        }
    }

    /// Check if this term represents the rdf:type predicate
    ///
    /// Handles both Term::Sid (checks namespace code) and Term::Iri (compares IRI string).
    /// Returns false for variables and values.
    pub fn is_rdf_type(&self) -> bool {
        match self {
            Term::Sid(sid) => fluree_db_core::is_rdf_type(sid),
            Term::Iri(iri) => iri.as_ref() == rdf::TYPE,
            _ => false,
        }
    }
}

/// A triple pattern for matching flakes
///
/// Each position can be a variable or a constant.
#[derive(Clone, Debug, PartialEq)]
pub struct TriplePattern {
    /// Subject term
    pub s: Term,
    /// Predicate term
    pub p: Term,
    /// Object term
    pub o: Term,
    /// Optional datatype constraint for the object
    pub dt: Option<Sid>,
    /// Optional language tag constraint for the object (e.g., "en", "fr")
    pub lang: Option<Arc<str>>,
}

impl TriplePattern {
    /// Create a new triple pattern
    pub fn new(s: Term, p: Term, o: Term) -> Self {
        Self {
            s,
            p,
            o,
            dt: None,
            lang: None,
        }
    }

    /// Create with datatype constraint
    pub fn with_dt(s: Term, p: Term, o: Term, dt: Sid) -> Self {
        Self {
            s,
            p,
            o,
            dt: Some(dt),
            lang: None,
        }
    }

    /// Set language tag constraint
    pub fn with_lang(mut self, lang: impl AsRef<str>) -> Self {
        self.lang = Some(Arc::from(lang.as_ref()));
        self
    }

    /// Get the variables in this pattern (in order: s, p, o)
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(3);
        if let Term::Var(v) = &self.s {
            vars.push(*v);
        }
        if let Term::Var(v) = &self.p {
            vars.push(*v);
        }
        if let Term::Var(v) = &self.o {
            vars.push(*v);
        }
        vars
    }

    /// Check if subject is bound (not a variable)
    pub fn s_bound(&self) -> bool {
        self.s.is_bound()
    }

    /// Check if predicate is bound (not a variable)
    pub fn p_bound(&self) -> bool {
        self.p.is_bound()
    }

    /// Check if object is bound (not a variable)
    pub fn o_bound(&self) -> bool {
        self.o.is_bound()
    }

    /// Check if object is a reference (Sid, Iri, or Ref value type)
    ///
    /// Used for index selection - OPST index is preferred for ref lookups.
    /// Term::Iri is treated as a ref because it represents an IRI that will
    /// be encoded to a Sid for each target ledger.
    pub fn o_is_ref(&self) -> bool {
        match &self.o {
            Term::Sid(_) => true,
            Term::Iri(_) => true, // IRI will be encoded to Sid per-ledger
            Term::Value(FlakeValue::Ref(_)) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_is_var() {
        let var = Term::Var(VarId(0));
        let sid = Term::Sid(Sid::new(1, "test"));
        let val = Term::Value(FlakeValue::Long(42));

        assert!(var.is_var());
        assert!(!sid.is_var());
        assert!(!val.is_var());

        assert!(!var.is_bound());
        assert!(sid.is_bound());
        assert!(val.is_bound());
    }

    #[test]
    fn test_triple_pattern_variables() {
        let pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(1, "name")),
            Term::Var(VarId(1)),
        );

        let vars = pattern.variables();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0], VarId(0));
        assert_eq!(vars[1], VarId(1));
    }

    #[test]
    fn test_triple_pattern_bound_checks() {
        let pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(1, "name")),
            Term::Value(FlakeValue::Long(42)),
        );

        assert!(!pattern.s_bound());
        assert!(pattern.p_bound());
        assert!(pattern.o_bound());
    }
}
