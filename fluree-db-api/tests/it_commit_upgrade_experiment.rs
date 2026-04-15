//! Shape A validation experiment for fluree/db-r#152.
//!
//! Before designing the `fluree commit upgrade --force` repair path, we need
//! to know whether current (post-PR-#148) fluree-db-api write and reindex,
//! given the SAME logical intent as a pre-148 commit from `fluree-zip.zip`,
//! produces a consistent SPOT/PSOT representation. If it does, then the
//! repair strategy can be "migrate pre-148 commits into post-148 shape via
//! the current write path" rather than "surgically rewrite the live index."
//!
//! The test reproduces the exact t=13 commit from the production `_system`
//! ledger — a pure-insert creation of `team:4b3d4421-...` with its 9
//! properties plus a TeamMembership record with 6 properties. After the
//! insert, it reindexes the memory ledger, then runs the audit. If the
//! audit reports zero missing flakes, post-148 write + reindex is
//! consistent for this shape — validating the migration strategy for all
//! Shape A (pure-insert) entries in the spot-only bucket.
//!
//! This is an `#[ignore]`-gated experiment. Once the hypothesis is
//! confirmed (or refuted) and the repair design is final, this file can
//! be deleted or converted into a permanent regression test for the
//! migration invariant.

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::json;

#[tokio::test]
#[ignore = "Shape A diagnostic experiment for #152; run explicitly"]
async fn shape_a_pure_insert_post_148_roundtrip_is_clean() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/shape-a-experiment:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Reproduce t=13 from the production _system snapshot: 9 team properties
    // + 6 membership properties, all asserts, no retracts. Exact IRIs, values,
    // and datatypes from `fluree show t:13 --ledger _system` on the fluree-zip
    // snapshot.
    let doc = json!({
        "@graph": [
            {
                "@id": "urn:fsys:team:4b3d4421-0050-4688-a831-64070fce2b2a",
                "@type": "https://ns.flur.ee/system#Team",
                "http://schema.org/dateCreated": "2026-03-25T19:27:09.114790915+00:00",
                "http://schema.org/dateModified": "2026-03-25T19:27:09.114790915+00:00",
                "https://ns.flur.ee/system#createdBy": {
                    "@id": "urn:fsys:user:04b81428-d001-7082-410d-033b4caa9c16"
                },
                "https://ns.flur.ee/system#description": "",
                "https://ns.flur.ee/system#isActive": true,
                "https://ns.flur.ee/system#name": "Pilot Team",
                "https://ns.flur.ee/system#parentTeam": {"@id": "urn:fsys:team:ROOT"},
                "https://ns.flur.ee/system#teamId": "4b3d4421-0050-4688-a831-64070fce2b2a"
            },
            {
                "@id": "urn:fsys:membership:4b3d4421-0050-4688-a831-64070fce2b2a|04b81428-d001-7082-410d-033b4caa9c16",
                "@type": "https://ns.flur.ee/system#TeamMembership",
                "https://ns.flur.ee/system#addedBy": {
                    "@id": "urn:fsys:user:04b81428-d001-7082-410d-033b4caa9c16"
                },
                "https://ns.flur.ee/system#joinedAt": "2026-03-25T19:27:09.114790915+00:00",
                "https://ns.flur.ee/system#role": "owner",
                "https://ns.flur.ee/system#team": {
                    "@id": "urn:fsys:team:4b3d4421-0050-4688-a831-64070fce2b2a"
                },
                "https://ns.flur.ee/system#user": {
                    "@id": "urn:fsys:user:04b81428-d001-7082-410d-033b4caa9c16"
                }
            }
        ]
    });

    let result = fluree.insert(ledger0, &doc).await.unwrap();
    let ledger_after_insert = result.ledger;
    let t_after_insert = ledger_after_insert.t();

    // Reindex so the audit runs against the binary index, matching the
    // scratch-t2-01 state shape.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex of post-148 commit should succeed");

    // Run the same audit the CLI uses.
    let audit = fluree
        .audit_commit_upgrade(ledger_id)
        .await
        .expect("audit should run");

    // If the repair hypothesis is correct, the audit is clean: no missing
    // flakes in either SPOT or PSOT, canonical_count matches what both scans
    // find, t=1 (one commit worth of user data after genesis).
    eprintln!(
        "shape A experiment audit: commits_walked={} canonical={} spot={} psot={} missing={}",
        audit.commits_walked,
        audit.canonical_count,
        audit.spot_scan_count,
        audit.psot_scan_count,
        audit.missing_len()
    );
    for m in &audit.missing {
        eprintln!(
            "  missing [{:?}] {} {} {:?}",
            m.kind, m.subject, m.predicate, m.value
        );
    }

    assert_eq!(t_after_insert, 1, "one user commit after genesis");
    assert!(
        audit.is_clean(),
        "post-148 write + reindex of Shape A shape must produce SPOT/PSOT agreement; \
         found {} missing",
        audit.missing_len()
    );

    // Belt-and-suspenders: confirm the team's properties come back via
    // subject-first query (SPOT path) and via predicate-first query (PSOT
    // path) and that both agree on the name.
    let state = fluree.ledger(ledger_id).await.unwrap();
    let spot = support::query_jsonld(
        &fluree,
        &state,
        &json!({
            "where": {
                "@id": "urn:fsys:team:4b3d4421-0050-4688-a831-64070fce2b2a",
                "?p": "?o"
            },
            "select": ["?p", "?o"]
        }),
    )
    .await
    .expect("SPOT query")
    .to_jsonld_async(fluree_db_api::GraphDb::from_ledger_state(&state).as_graph_db_ref())
    .await
    .expect("SPOT formatter");
    let spot_rows = spot.as_array().expect("array");
    assert!(
        spot_rows.len() >= 9,
        "SPOT subject-first pull on team should return at least 9 rows; got {}",
        spot_rows.len()
    );

    let psot = support::query_jsonld(
        &fluree,
        &state,
        &json!({
            "where": {
                "@id": "?s",
                "https://ns.flur.ee/system#name": "Pilot Team"
            },
            "select": "?s"
        }),
    )
    .await
    .expect("PSOT query")
    .to_jsonld_async(fluree_db_api::GraphDb::from_ledger_state(&state).as_graph_db_ref())
    .await
    .expect("PSOT formatter");
    let psot_rows = psot.as_array().expect("array");
    assert_eq!(
        psot_rows.len(),
        1,
        "PSOT predicate-first should find exactly one team with name 'Pilot Team'"
    );
}
