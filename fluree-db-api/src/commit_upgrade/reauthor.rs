//! Re-author step of the commit-chain upgrade pipeline (fluree/db-r#152).
//!
//! Consumes `DecodedOp`s (produced by `decode::decode_commit`) and builds
//! canonical `Flake`s by re-encoding every IRI through the migration's
//! `NamespaceRegistry::sid_for_iri`. The resulting flakes are identical in
//! shape to what the current post-#148 write path produces — `sid_for_iri`
//! applies `canonical_split` against the registry's prefix table to pick
//! the longest registered prefix, allocating a new code via
//! `get_or_allocate` if none matches.
//!
//! # Scope
//!
//! This module is the **pure per-op flake-construction primitive**. The
//! migration loop that drives decode → re-author → `apply_cancellation` →
//! `stage_flakes` → `commit()` for each source commit lands in a separate
//! commit alongside this one. Keeping the primitive standalone lets us
//! unit-test re-author's Sid re-encoding independently of the commit
//! write path.
//!
//! # Determinism
//!
//! Given an identical input `Vec<DecodedOp>` and an identical initial
//! `NamespaceRegistry` state, `build_canonical_flakes` produces an
//! identical `Vec<Flake>`. `sid_for_iri` is deterministic per `(iri,
//! split_mode, prefix_table)`, and we never mutate the op order.
//!
//! # Idempotence under repeated IRIs
//!
//! Two `DecodedOp`s referencing the same IRI (subject, predicate,
//! datatype, graph, or ref-object) produce the same `Sid` — the
//! registry's `get_or_allocate` returns the cached code after first
//! insertion. So a re-authored chain has consistent Sid encoding across
//! commits for the same logical subject, which is the whole point.

// This module's public surface (`build_canonical_flakes`) is consumed by
// the migration loop in a follow-on commit on the branch. Until that
// commit lands the function is transitively unreachable from non-test lib
// builds; the unit suite below exercises it in isolation. The allow
// comes off when the migration loop ties decode+reauthor together.
#![cfg_attr(not(test), allow(dead_code))]

use fluree_db_core::{Flake, FlakeValue};
use fluree_db_transact::NamespaceRegistry;

use super::decode::{DecodedOp, LogicalObject};

/// Re-encode every `DecodedOp` as a canonical `Flake` using `ns_registry`
/// as the single source of truth for Sid construction.
///
/// Order of the input vector is preserved in the output. The registry is
/// mutated as new prefixes are allocated — callers that care about
/// allocation determinism across commits should seed the registry with
/// the source chain's full namespace table up front (see the migration
/// loop commit).
pub(crate) fn build_canonical_flakes(
    decoded: Vec<DecodedOp>,
    ns_registry: &mut NamespaceRegistry,
) -> Vec<Flake> {
    decoded
        .into_iter()
        .map(|op| build_flake(op, ns_registry))
        .collect()
}

fn build_flake(op: DecodedOp, ns_registry: &mut NamespaceRegistry) -> Flake {
    let s = ns_registry.sid_for_iri(&op.subject_iri);
    let p = ns_registry.sid_for_iri(&op.predicate_iri);
    let dt = ns_registry.sid_for_iri(&op.datatype_iri);
    let g = op
        .graph_iri
        .as_deref()
        .map(|iri| ns_registry.sid_for_iri(iri));
    let o = match op.value {
        LogicalObject::Ref(iri) => FlakeValue::Ref(ns_registry.sid_for_iri(&iri)),
        LogicalObject::Scalar(v) => v,
    };

    let mut flake = Flake::new(s, p, o, dt, op.t, op.op, op.meta);
    flake.g = g;
    flake
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{LedgerSnapshot, Sid};
    use fluree_vocab::namespaces;

    const TEAM_PREFIX: &str = "urn:fsys:team:";
    const TEAM_IRI: &str = "urn:fsys:team:abc";
    const PRED_IRI: &str = "http://example.org/name";
    const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

    /// Build a registry that already knows about `urn:fsys:team:` at
    /// `USER_START` — mirrors the migration loop's up-front prefix
    /// seeding from the source snapshot.
    fn seeded_registry() -> NamespaceRegistry {
        let mut snap = LedgerSnapshot::genesis("test:main");
        snap.insert_namespace_code(namespaces::USER_START, TEAM_PREFIX.to_string())
            .expect("seed team prefix");
        NamespaceRegistry::from_db(&snap)
    }

    fn op(
        subject_iri: &str,
        value: LogicalObject,
        datatype_iri: &str,
        t: i64,
        op_is_assert: bool,
    ) -> DecodedOp {
        DecodedOp {
            subject_iri: subject_iri.to_string(),
            predicate_iri: PRED_IRI.to_string(),
            graph_iri: None,
            datatype_iri: datatype_iri.to_string(),
            value,
            op: op_is_assert,
            t,
            meta: None,
        }
    }

    fn scalar_string(v: &str) -> LogicalObject {
        LogicalObject::Scalar(FlakeValue::String(v.to_string()))
    }

    #[test]
    fn subject_iri_is_split_against_registered_prefix() {
        let mut reg = seeded_registry();
        let ops = vec![op(TEAM_IRI, scalar_string("Pilot"), XSD_STRING, 1, true)];
        let flakes = build_canonical_flakes(ops, &mut reg);

        assert_eq!(flakes.len(), 1);
        assert_eq!(flakes[0].s.namespace_code, namespaces::USER_START);
        assert_eq!(flakes[0].s.name.as_ref(), "abc");
        // Predicate had no matching registered prefix — allocated fresh.
        // (Its ns_code is therefore >= USER_START, and its name is the suffix.)
        assert!(flakes[0].p.namespace_code >= namespaces::USER_START);
    }

    #[test]
    fn repeated_iri_resolves_to_same_sid() {
        let mut reg = seeded_registry();
        let ops = vec![
            op(TEAM_IRI, scalar_string("a"), XSD_STRING, 1, true),
            op(TEAM_IRI, scalar_string("b"), XSD_STRING, 2, true),
        ];
        let flakes = build_canonical_flakes(ops, &mut reg);

        assert_eq!(flakes[0].s, flakes[1].s);
        assert_eq!(flakes[0].p, flakes[1].p);
        assert_eq!(flakes[0].dt, flakes[1].dt);
    }

    #[test]
    fn prefix_not_in_registry_gets_allocated_on_first_use() {
        let mut reg = seeded_registry();
        let ops = vec![op(
            "http://example.org/other/thing",
            scalar_string("x"),
            XSD_STRING,
            1,
            true,
        )];
        let flakes = build_canonical_flakes(ops, &mut reg);

        // `example.org/other/` was not seeded — sid_for_iri allocated a new
        // code for it. Result is a proper split, not the unsplit EMPTY form.
        assert_ne!(flakes[0].s.namespace_code, namespaces::EMPTY);
        assert_eq!(flakes[0].s.name.as_ref(), "thing");
    }

    #[test]
    fn scalar_values_pass_through_unchanged() {
        let mut reg = seeded_registry();
        let ops = vec![
            op(
                TEAM_IRI,
                LogicalObject::Scalar(FlakeValue::Long(42)),
                "http://www.w3.org/2001/XMLSchema#long",
                1,
                true,
            ),
            op(
                TEAM_IRI,
                LogicalObject::Scalar(FlakeValue::Boolean(true)),
                "http://www.w3.org/2001/XMLSchema#boolean",
                2,
                true,
            ),
        ];
        let flakes = build_canonical_flakes(ops, &mut reg);

        assert!(matches!(flakes[0].o, FlakeValue::Long(42)));
        assert!(matches!(flakes[1].o, FlakeValue::Boolean(true)));
    }

    #[test]
    fn ref_object_is_re_encoded_through_registry() {
        let mut reg = seeded_registry();
        let ops = vec![op(
            "urn:fsys:team:alice",
            LogicalObject::Ref("urn:fsys:team:bob".to_string()),
            // anyURI / id-like datatype
            "http://www.w3.org/2001/XMLSchema#anyURI",
            1,
            true,
        )];
        let flakes = build_canonical_flakes(ops, &mut reg);

        match &flakes[0].o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, namespaces::USER_START);
                assert_eq!(sid.name.as_ref(), "bob");
            }
            other => panic!("expected FlakeValue::Ref, got {other:?}"),
        }
    }

    #[test]
    fn named_graph_iri_becomes_flake_g() {
        let mut reg = seeded_registry();
        let mut decoded = op(TEAM_IRI, scalar_string("x"), XSD_STRING, 1, true);
        decoded.graph_iri = Some("urn:fsys:team:g1".to_string());
        let flakes = build_canonical_flakes(vec![decoded], &mut reg);

        let g = flakes[0].g.as_ref().expect("named graph");
        assert_eq!(g.namespace_code, namespaces::USER_START);
        assert_eq!(g.name.as_ref(), "g1");
    }

    #[test]
    fn op_t_and_meta_are_preserved() {
        let mut reg = seeded_registry();
        let ops = vec![
            op(TEAM_IRI, scalar_string("asserted"), XSD_STRING, 5, true),
            op(TEAM_IRI, scalar_string("retracted"), XSD_STRING, 6, false),
        ];
        let flakes = build_canonical_flakes(ops, &mut reg);

        assert_eq!(flakes[0].t, 5);
        assert!(flakes[0].op);
        assert!(flakes[0].m.is_none());

        assert_eq!(flakes[1].t, 6);
        assert!(!flakes[1].op);
    }

    #[test]
    fn input_order_is_preserved() {
        let mut reg = seeded_registry();
        let ops = vec![
            op("urn:fsys:team:a", scalar_string("1"), XSD_STRING, 1, true),
            op("urn:fsys:team:b", scalar_string("2"), XSD_STRING, 2, true),
            op("urn:fsys:team:c", scalar_string("3"), XSD_STRING, 3, true),
        ];
        let flakes = build_canonical_flakes(ops, &mut reg);

        assert_eq!(flakes[0].s.name.as_ref(), "a");
        assert_eq!(flakes[1].s.name.as_ref(), "b");
        assert_eq!(flakes[2].s.name.as_ref(), "c");
    }

    #[test]
    fn determinism_two_identical_inputs_produce_identical_outputs() {
        let build = || {
            let mut reg = seeded_registry();
            let ops = vec![
                op(TEAM_IRI, scalar_string("x"), XSD_STRING, 1, true),
                op(
                    "http://example.org/novel",
                    scalar_string("y"),
                    XSD_STRING,
                    2,
                    true,
                ),
            ];
            build_canonical_flakes(ops, &mut reg)
        };

        let run1: Vec<(Sid, Sid, Sid)> = build().into_iter().map(|f| (f.s, f.p, f.dt)).collect();
        let run2: Vec<(Sid, Sid, Sid)> = build().into_iter().map(|f| (f.s, f.p, f.dt)).collect();
        assert_eq!(run1, run2);
    }
}
