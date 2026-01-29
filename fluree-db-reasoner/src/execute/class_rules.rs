//! Class hierarchy OWL2-RL rules (cax-*).
//!
//! This module implements class hierarchy rules from the OWL2-RL profile:
//! - `cax-sco` - SubClassOf
//! - `cax-eqc` - EquivalentClass

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;

use crate::ontology_rl::OntologyRL;
use crate::same_as::SameAsTracker;
use crate::ReasoningDiagnostics;

use super::delta::DeltaSet;
use super::derived::DerivedSet;
use super::util::ref_dt;

/// Apply cax-sco rule (rdfs:subClassOf class hierarchy)
///
/// Rule: type(x, C1), C1 rdfs:subClassOf* C2 → type(x, C2)
///
/// For each rdf:type assertion in delta where the class has superclasses,
/// derive type facts for all superclasses.
pub fn apply_subclass_rule(
    ontology: &OntologyRL,
    delta: &DeltaSet,
    derived: &DerivedSet,
    new_delta: &mut DeltaSet,
    same_as: &SameAsTracker,
    rdf_type_sid: &Sid,
    t: i64,
    diagnostics: &mut ReasoningDiagnostics,
) {
    // ref datatype for derived flakes
    let ref_dt = ref_dt();

    // Process rdf:type facts in delta
    for flake in delta.get_by_p(rdf_type_sid) {
        // type(x, C1) - object must be a Ref (class)
        if let FlakeValue::Ref(c1) = &flake.o {
            // Get superclasses of C1
            let superclasses = ontology.super_classes_of(c1);
            if superclasses.is_empty() {
                continue;
            }

            // Canonicalize subject for consistent derived facts
            let x_canonical = same_as.canonical(&flake.s);

            // Derive type(x, C2) for each superclass C2
            for c2 in superclasses {
                let derived_flake = Flake::new(
                    x_canonical.clone(),
                    rdf_type_sid.clone(),
                    FlakeValue::Ref(c2.clone()),
                    ref_dt.clone(),
                    t,
                    true,
                    None,
                );

                // Only add if not already derived
                if !derived.contains(&derived_flake.s, &derived_flake.p, &derived_flake.o) {
                    new_delta.push(derived_flake);
                    diagnostics.record_rule_fired("cax-sco");
                }
            }
        }
    }
}

/// Apply cax-eqc1/cax-eqc2 (EquivalentClass) rule
///
/// cax-eqc: type(x, C1), equivalentClass(C1, C2) → type(x, C2)
///
/// Since equivalentClass is bidirectional, we store both directions
/// (C1→C2 and C2→C1), so this implementation works for both directions.
///
/// For each rdf:type assertion in delta where the class has equivalent classes,
/// derive type facts for all equivalent classes.
pub fn apply_equivalent_class_rule(
    ontology: &OntologyRL,
    delta: &DeltaSet,
    derived: &DerivedSet,
    new_delta: &mut DeltaSet,
    same_as: &SameAsTracker,
    rdf_type_sid: &Sid,
    t: i64,
    diagnostics: &mut ReasoningDiagnostics,
) {
    // ref datatype for derived flakes
    let ref_dt = ref_dt();

    // Process rdf:type facts in delta
    for flake in delta.get_by_p(rdf_type_sid) {
        // type(x, C1) - object must be a Ref (class)
        if let FlakeValue::Ref(c1) = &flake.o {
            // Get equivalent classes of C1
            let equiv_classes = ontology.equivalent_classes_of(c1);
            if equiv_classes.is_empty() {
                continue;
            }

            // Canonicalize subject for consistent derived facts
            let x_canonical = same_as.canonical(&flake.s);

            // Derive type(x, C2) for each equivalent class C2
            for c2 in equiv_classes {
                let derived_flake = Flake::new(
                    x_canonical.clone(),
                    rdf_type_sid.clone(),
                    FlakeValue::Ref(c2.clone()),
                    ref_dt.clone(),
                    t,
                    true,
                    None,
                );

                // Only add if not already derived
                if !derived.contains(&derived_flake.s, &derived_flake.p, &derived_flake.o) {
                    new_delta.push(derived_flake);
                    diagnostics.record_rule_fired("cax-eqc");
                }
            }
        }
    }
}
