//! Decode step of the commit-chain upgrade pipeline (fluree/db-r#152).
//!
//! Given a loaded legacy `Commit` and a source `LedgerSnapshot`, produces a
//! vector of `DecodedOp`s — the commit's asserts and retracts expressed as
//! logical IRI strings and canonical literal values, independent of how the
//! source commit happened to encode its Sids.
//!
//! The migration's re-author step consumes the decoded ops and constructs
//! fresh flakes through the current canonical write path
//! (`NamespaceRegistry::sid_for_iri` + `stage_flakes`), so the resulting
//! commit chain contains only post-#148 encoding regardless of what the
//! source chain's writer produced.
//!
//! # Why decode against the SOURCE snapshot
//!
//! The source snapshot is built by walking the entire legacy chain to head,
//! so its `namespace_codes` table contains every prefix the source chain
//! ever registered — at any `t`. Decoding against it gives a stable IRI
//! reconstruction regardless of whether a given Sid was written before or
//! after its matching prefix was registered. The migration's own
//! `NamespaceRegistry` grows commit-by-commit and wouldn't know about
//! prefixes the source registered at `t > current-iteration`.
//!
//! # Error semantics
//!
//! If any Sid in the commit references a namespace code not present in the
//! source snapshot, decode returns an error rather than producing a
//! placeholder IRI. That case indicates a corrupted source chain (a commit
//! cites a prefix that was never registered via `namespace_delta`) and the
//! migration must refuse to proceed rather than invent semantics.

use fluree_db_core::{Commit, Flake, FlakeMeta, FlakeValue, LedgerSnapshot, Sid};

use crate::{ApiError, Result};

/// One commit op expressed as logical IRI + value intent, decoupled from
/// any specific Sid encoding.
#[derive(Debug, Clone)]
pub(crate) struct DecodedOp {
    pub subject_iri: String,
    pub predicate_iri: String,
    pub graph_iri: Option<String>,
    pub datatype_iri: String,
    pub value: LogicalObject,
    /// `true` = assert, `false` = retract.
    pub op: bool,
    pub t: i64,
    pub meta: Option<FlakeMeta>,
}

/// Object value in logical form.
///
/// `Ref(iri)` holds the decoded IRI of a subject reference (flakes whose
/// datatype is the built-in `id`). Re-author must re-encode this IRI
/// through the migration's `NamespaceRegistry` because the referenced
/// subject's canonical Sid may differ under the current split rules.
///
/// `Scalar(v)` is a pass-through for every non-`Ref` `FlakeValue` variant.
/// String/number/boolean/dateTime/`@json`/`@vector` values already encode
/// their logical value directly in the `FlakeValue` enum — they are not
/// Sid-encoded and need no decode/re-encode round-trip.
#[derive(Debug, Clone)]
pub(crate) enum LogicalObject {
    Ref(String),
    Scalar(FlakeValue),
}

/// Decode every flake in `commit` to a `DecodedOp` using `source_snapshot`
/// as the authoritative namespace table.
pub(crate) fn decode_commit(
    source_snapshot: &LedgerSnapshot,
    commit: &Commit,
) -> Result<Vec<DecodedOp>> {
    let mut out = Vec::with_capacity(commit.flakes.len());
    for (i, flake) in commit.flakes.iter().enumerate() {
        out.push(decode_flake(source_snapshot, flake, commit.t, i)?);
    }
    Ok(out)
}

fn decode_flake(
    snapshot: &LedgerSnapshot,
    flake: &Flake,
    commit_t: i64,
    flake_index: usize,
) -> Result<DecodedOp> {
    let subject_iri = decode_sid_or_err(snapshot, &flake.s, "subject", commit_t, flake_index)?;
    let predicate_iri = decode_sid_or_err(snapshot, &flake.p, "predicate", commit_t, flake_index)?;
    let datatype_iri = decode_sid_or_err(snapshot, &flake.dt, "datatype", commit_t, flake_index)?;
    let graph_iri = flake
        .g
        .as_ref()
        .map(|sid| decode_sid_or_err(snapshot, sid, "graph", commit_t, flake_index))
        .transpose()?;
    let value = match &flake.o {
        FlakeValue::Ref(sid) => LogicalObject::Ref(decode_sid_or_err(
            snapshot,
            sid,
            "object-ref",
            commit_t,
            flake_index,
        )?),
        other => LogicalObject::Scalar(other.clone()),
    };
    Ok(DecodedOp {
        subject_iri,
        predicate_iri,
        graph_iri,
        datatype_iri,
        value,
        op: flake.op,
        t: flake.t,
        meta: flake.m.clone(),
    })
}

fn decode_sid_or_err(
    snapshot: &LedgerSnapshot,
    sid: &Sid,
    field: &'static str,
    commit_t: i64,
    flake_index: usize,
) -> Result<String> {
    snapshot.decode_sid(sid).ok_or_else(|| {
        ApiError::internal(format!(
            "commit-upgrade decode: flake #{flake_index} at t={commit_t} cites unknown \
             namespace_code {code} on {field} Sid (name={name:?}); source chain is \
             missing a required namespace_delta registration — refusing to migrate",
            code = sid.namespace_code,
            name = sid.name,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind, Sid};
    use fluree_vocab::namespaces;

    const TEAM_PREFIX: &str = "urn:fsys:team:";
    const TEAM_IRI: &str = "urn:fsys:team:abc";
    const USER_CODE: u16 = namespaces::USER_START;

    /// A snapshot seeded with the built-in prefixes plus one user prefix
    /// for the `urn:fsys:team:` namespace. Good enough for decode tests.
    fn snapshot_with_team_prefix() -> LedgerSnapshot {
        let mut s = LedgerSnapshot::genesis("test:main");
        s.insert_namespace_code(USER_CODE, TEAM_PREFIX.to_string())
            .expect("insert user prefix");
        s
    }

    fn flake(s: Sid, p: Sid, o: FlakeValue, dt: Sid, t: i64, op: bool) -> Flake {
        Flake::new(s, p, o, dt, t, op, None)
    }

    /// Build a minimal `Commit` with a placeholder CID (decode never inspects
    /// the id — it only reads flakes and `t`).
    fn commit_at(t: i64, flakes: Vec<Flake>) -> Commit {
        Commit {
            id: Some(ContentId::new(ContentKind::Commit, b"decode-test")),
            t,
            flakes,
            previous_refs: Vec::new(),
            namespace_delta: Default::default(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            commit_signatures: Vec::new(),
            graph_delta: Default::default(),
            ns_split_mode: None,
        }
    }

    #[test]
    fn unsplit_subject_decodes_to_full_iri() {
        let snap = snapshot_with_team_prefix();
        let f = flake(
            Sid::new(namespaces::EMPTY, TEAM_IRI),
            Sid::new(namespaces::EMPTY, "http://example.org/name"),
            FlakeValue::String("Alice".to_string()),
            Sid::new(namespaces::XSD, "string"),
            1,
            true,
        );
        let ops = decode_commit(&snap, &commit_at(1, vec![f])).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].subject_iri, TEAM_IRI);
        assert_eq!(ops[0].predicate_iri, "http://example.org/name");
        assert_eq!(
            ops[0].datatype_iri,
            "http://www.w3.org/2001/XMLSchema#string"
        );
        assert!(
            matches!(&ops[0].value, LogicalObject::Scalar(FlakeValue::String(s)) if s == "Alice")
        );
        assert!(ops[0].op);
        assert!(ops[0].graph_iri.is_none());
        assert_eq!(ops[0].t, 1);
        assert!(ops[0].meta.is_none());
    }

    #[test]
    fn split_subject_decodes_to_full_iri_via_prefix_table() {
        let snap = snapshot_with_team_prefix();
        let f = flake(
            Sid::new(USER_CODE, "abc"),
            Sid::new(namespaces::EMPTY, "http://example.org/label"),
            FlakeValue::String("team-abc".to_string()),
            Sid::new(namespaces::XSD, "string"),
            2,
            true,
        );
        let ops = decode_commit(&snap, &commit_at(2, vec![f])).unwrap();
        assert_eq!(ops[0].subject_iri, TEAM_IRI);
    }

    #[test]
    fn overflow_encoded_sid_decodes_from_name_field() {
        let snap = snapshot_with_team_prefix();
        let f = flake(
            Sid::new(namespaces::OVERFLOW, "http://example.org/overflow-iri"),
            Sid::new(namespaces::EMPTY, "http://example.org/p"),
            FlakeValue::Boolean(true),
            Sid::new(namespaces::XSD, "boolean"),
            3,
            true,
        );
        let ops = decode_commit(&snap, &commit_at(3, vec![f])).unwrap();
        assert_eq!(ops[0].subject_iri, "http://example.org/overflow-iri");
    }

    #[test]
    fn ref_object_decodes_to_subject_iri() {
        let snap = snapshot_with_team_prefix();
        let f = flake(
            Sid::new(namespaces::EMPTY, "urn:alice"),
            Sid::new(namespaces::EMPTY, "urn:knows"),
            FlakeValue::Ref(Sid::new(USER_CODE, "bob")),
            Sid::new(namespaces::FLUREE_DB, "anyURI"),
            4,
            true,
        );
        let ops = decode_commit(&snap, &commit_at(4, vec![f])).unwrap();
        match &ops[0].value {
            LogicalObject::Ref(iri) => assert_eq!(iri, "urn:fsys:team:bob"),
            other => panic!("expected Ref, got {other:?}"),
        }
    }

    #[test]
    fn named_graph_decodes_to_graph_iri() {
        let snap = snapshot_with_team_prefix();
        let mut f = flake(
            Sid::new(namespaces::EMPTY, "urn:s"),
            Sid::new(namespaces::EMPTY, "urn:p"),
            FlakeValue::Long(42),
            Sid::new(namespaces::XSD, "long"),
            5,
            true,
        );
        f.g = Some(Sid::new(USER_CODE, "graph-1"));
        let ops = decode_commit(&snap, &commit_at(5, vec![f])).unwrap();
        assert_eq!(ops[0].graph_iri.as_deref(), Some("urn:fsys:team:graph-1"));
    }

    #[test]
    fn retract_op_preserved() {
        let snap = snapshot_with_team_prefix();
        let f = flake(
            Sid::new(namespaces::EMPTY, "urn:s"),
            Sid::new(namespaces::EMPTY, "urn:p"),
            FlakeValue::String("x".to_string()),
            Sid::new(namespaces::XSD, "string"),
            6,
            false,
        );
        let ops = decode_commit(&snap, &commit_at(6, vec![f])).unwrap();
        assert!(!ops[0].op);
    }

    #[test]
    fn unknown_namespace_code_errors() {
        let snap = LedgerSnapshot::genesis("test:main"); // no user prefixes
        let f = flake(
            Sid::new(USER_CODE, "abc"), // cites an unregistered code
            Sid::new(namespaces::EMPTY, "urn:p"),
            FlakeValue::String("x".to_string()),
            Sid::new(namespaces::XSD, "string"),
            7,
            true,
        );
        let err = decode_commit(&snap, &commit_at(7, vec![f])).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown namespace_code")
                && msg.contains(&USER_CODE.to_string())
                && msg.contains("subject"),
            "error should pinpoint which field and which code; got: {msg}"
        );
    }
}
