//! Shared utility functions for rule execution.
//!
//! This module provides helper functions used across multiple rule implementations:
//! - `ref_dt()` - Default datatype SID for Ref values
//! - `rdf_type_sid()` - SID for the rdf:type predicate
//! - `canonicalize_flake()` - Apply sameAs canonicalization to a flake
//! - `try_derive_type()` - Derive a type fact with deduplication

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use fluree_vocab::jsonld_names::ID as JSONLD_ID;
use fluree_vocab::namespaces::{JSON_LD, RDF};
use fluree_vocab::predicates::RDF_TYPE;

use crate::same_as::SameAsTracker;
use crate::ReasoningDiagnostics;
use super::delta::DeltaSet;
use super::derived::DerivedSet;

/// Default datatype SID for derived Ref values.
///
/// When deriving new flakes with Ref objects (e.g., rdf:type assertions),
/// the datatype should be `$id` (JSON_LD namespace, "id" local name).
pub fn ref_dt() -> Sid {
    Sid::new(JSON_LD, JSONLD_ID)
}

/// SID for the rdf:type predicate.
///
/// This is used extensively in reasoning rules to derive type assertions.
pub fn rdf_type_sid() -> Sid {
    Sid::new(RDF, RDF_TYPE)
}

/// Canonicalize a flake's subject and object positions using sameAs equivalence
///
/// This implements eq-rep-s (canonicalize subject) and eq-rep-o (canonicalize object).
/// Returns a new flake with S and O replaced by their canonical representatives.
pub fn canonicalize_flake(flake: &Flake, same_as: &SameAsTracker) -> Flake {
    let canonical_s = same_as.canonical(&flake.s);

    let canonical_o = match &flake.o {
        FlakeValue::Ref(o_sid) => FlakeValue::Ref(same_as.canonical(o_sid)),
        other => other.clone(),
    };

    // Only create a new flake if something changed
    if canonical_s == flake.s && canonical_o == flake.o {
        flake.clone()
    } else {
        Flake::new(
            canonical_s,
            flake.p.clone(),
            canonical_o,
            flake.dt.clone(),
            flake.t,
            flake.op,
            flake.m.clone(),
        )
    }
}

/// Attempt to derive a type fact `rdf:type(subject, type_class)`.
///
/// Creates the flake and adds it to `new_delta` if not already in `derived`.
/// Returns `true` if the fact was new and added, `false` if it already existed.
///
/// This helper reduces boilerplate in rule implementations by encapsulating
/// the common pattern of creating a type flake, checking for duplicates,
/// and recording diagnostics.
pub fn try_derive_type(
    subject: &Sid,
    type_class: &Sid,
    rdf_type_sid: &Sid,
    t: i64,
    derived: &DerivedSet,
    new_delta: &mut DeltaSet,
    diagnostics: &mut ReasoningDiagnostics,
    rule_name: &str,
) -> bool {
    let flake = Flake::new(
        subject.clone(),
        rdf_type_sid.clone(),
        FlakeValue::Ref(type_class.clone()),
        ref_dt(),
        t,
        true,
        None,
    );

    if !derived.contains(&flake.s, &flake.p, &flake.o) {
        new_delta.push(flake);
        diagnostics.record_rule_fired(rule_name);
        true
    } else {
        false
    }
}
