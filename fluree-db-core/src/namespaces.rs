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
use fluree_vocab::index::*;
use fluree_vocab::namespaces::{
    BLANK_NODE, CREDENTIALS, DID_KEY, EMPTY, FLUREE_COMMIT, FLUREE_DB, FLUREE_FILE, FLUREE_INDEX,
    FLUREE_IPFS, FLUREE_LEDGER, FLUREE_MEMORY, FLUREE_S3, FOAF, ISBN, ISSN, JSON_LD, OWL, RDF,
    RDFS, SCHEMA_ORG, SHACL, SKOS, UUID, WIKIDATA, XSD,
};
use fluree_vocab::predicates::*;
use std::collections::HashMap;

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

// ============================================================================
// Fluree Index namespace (idx:) SID checks
// ============================================================================

/// Check if a SID is idx:target
#[inline]
pub fn is_idx_target(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == TARGET
}

/// Check if a SID is idx:limit
#[inline]
pub fn is_idx_limit(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == LIMIT
}

/// Check if a SID is idx:result
#[inline]
pub fn is_idx_result(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == RESULT
}

/// Check if a SID is idx:id
#[inline]
pub fn is_idx_id(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == ID
}

/// Check if a SID is idx:score
#[inline]
pub fn is_idx_score(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == SCORE
}

/// Check if a SID is idx:ledger
#[inline]
pub fn is_idx_ledger(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == LEDGER
}

/// Check if a SID is idx:sync
#[inline]
pub fn is_idx_sync(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == SYNC
}

/// Check if a SID is idx:timeout
#[inline]
pub fn is_idx_timeout(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX && sid.name.as_ref() == TIMEOUT
}

/// Check if a SID is in the Fluree Index namespace
#[inline]
pub fn is_fluree_index_ns(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_INDEX
}

/// Baseline namespace codes (code -> prefix) matching Fluree's reserved codepoints.
pub fn default_namespace_codes() -> HashMap<i32, String> {
    let mut map = HashMap::new();
    map.insert(EMPTY, "".to_string());
    map.insert(JSON_LD, "@".to_string());
    map.insert(XSD, "http://www.w3.org/2001/XMLSchema#".to_string());
    map.insert(
        RDF,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
    );
    map.insert(RDFS, "http://www.w3.org/2000/01/rdf-schema#".to_string());
    map.insert(SHACL, "http://www.w3.org/ns/shacl#".to_string());
    map.insert(OWL, "http://www.w3.org/2002/07/owl#".to_string());
    map.insert(
        CREDENTIALS,
        "https://www.w3.org/2018/credentials#".to_string(),
    );
    map.insert(FLUREE_LEDGER, "https://ns.flur.ee/ledger#".to_string());
    map.insert(FLUREE_DB, "fluree:db:sha256:".to_string());
    map.insert(DID_KEY, "did:key:".to_string());
    map.insert(FLUREE_COMMIT, "fluree:commit:sha256:".to_string());
    map.insert(FLUREE_MEMORY, "fluree:memory://".to_string());
    map.insert(FLUREE_FILE, "fluree:file://".to_string());
    map.insert(FLUREE_IPFS, "fluree:ipfs://".to_string());
    map.insert(FLUREE_S3, "fluree:s3://".to_string());
    map.insert(SCHEMA_ORG, "http://schema.org/".to_string());
    map.insert(WIKIDATA, "https://www.wikidata.org/wiki/".to_string());
    map.insert(FOAF, "http://xmlns.com/foaf/0.1/".to_string());
    map.insert(SKOS, "http://www.w3.org/2008/05/skos#".to_string());
    map.insert(UUID, "urn:uuid".to_string());
    map.insert(ISBN, "urn:isbn:".to_string());
    map.insert(ISSN, "urn:issn:".to_string());
    map.insert(BLANK_NODE, "_:".to_string());
    map.insert(FLUREE_INDEX, "https://ns.flur.ee/index#".to_string());
    map
}
