//! Database-specific namespace utilities
//!
//! This module contains utility functions for working with namespaces in the context
//! of Fluree databases. For the actual namespace codes and IRI constants, see the
//! `fluree-vocab` crate.
//!
//! The Clojure implementation seeds the genesis database/root with a baseline
//! namespace table (`default-namespaces` / `default-namespace-codes`) and then
//! allocates new namespace codes lazily at first use during transactions.
//!
//! Rust should mirror that behavior: seed genesis `Db.namespace_codes` with this
//! baseline so query/transaction code can reliably encode standard IRIs even
//! before any index exists.
use std::collections::HashMap;
use fluree_vocab::namespaces::{
    BLANK_NODE, DID_KEY, EMPTY, FLUREE_COMMIT, FLUREE_LEDGER, JSON_LD, OWL, RDF, RDFS, SHACL, XSD,
};
use fluree_vocab::predicates::*;

use crate::sid::Sid;

/// Check if a SID is rdf:type
#[inline]
pub fn is_rdf_type(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_TYPE
}

/// Check if a SID is rdf:first
#[inline]
pub fn is_rdf_first(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_FIRST
}

/// Check if a SID is rdf:rest
#[inline]
pub fn is_rdf_rest(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_REST
}

/// Check if a SID is rdf:nil
#[inline]
pub fn is_rdf_nil(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_NIL
}

/// Check if a SID is rdfs:subClassOf
#[inline]
pub fn is_rdfs_subclass_of(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_SUBCLASSOF
}

/// Check if a SID is rdfs:subPropertyOf
#[inline]
pub fn is_rdfs_subproperty_of(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_SUBPROPERTYOF
}

/// Check if a SID is rdfs:domain
#[inline]
pub fn is_rdfs_domain(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_DOMAIN
}

/// Check if a SID is rdfs:range
#[inline]
pub fn is_rdfs_range(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_RANGE
}

/// Check if a SID is owl:inverseOf
#[inline]
pub fn is_owl_inverse_of(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_INVERSEOF
}

/// Check if a SID is owl:equivalentClass
#[inline]
pub fn is_owl_equivalent_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_EQUIVALENTCLASS
}

/// Check if a SID is owl:equivalentProperty
#[inline]
pub fn is_owl_equivalent_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_EQUIVALENTPROPERTY
}

/// Check if a SID is owl:sameAs
#[inline]
pub fn is_owl_same_as(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_SAMEAS
}

/// Check if a SID is owl:SymmetricProperty
#[inline]
pub fn is_owl_symmetric_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_SYMMETRICPROPERTY
}

/// Check if a SID is owl:TransitiveProperty
#[inline]
pub fn is_owl_transitive_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_TRANSITIVEPROPERTY
}

/// Baseline namespace codes (code -> prefix) matching Fluree's reserved codepoints.
pub fn default_namespace_codes() -> HashMap<i32, String> {
    let mut map = HashMap::new();
    map.insert(EMPTY, "".to_string());
    map.insert(JSON_LD, "@".to_string());
    map.insert(XSD, "http://www.w3.org/2001/XMLSchema#".to_string());
    map.insert(RDF, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
    map.insert(RDFS, "http://www.w3.org/2000/01/rdf-schema#".to_string());
    map.insert(SHACL, "http://www.w3.org/ns/shacl#".to_string());
    map.insert(OWL, "http://www.w3.org/2002/07/owl#".to_string());
    map.insert(FLUREE_LEDGER, "https://ns.flur.ee/ledger#".to_string());
    map.insert(DID_KEY, "did:key:".to_string());
    map.insert(FLUREE_COMMIT, "fluree:commit:sha256:".to_string());
    map.insert(BLANK_NODE, "_:".to_string());
    map
}

