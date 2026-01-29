//! Commit metadata flakes generation
//!
//! This module generates commit metadata flakes for Clojure parity.
//! These flakes are intended to be indexed alongside transaction flakes,
//! and must be reproducible during ledger load/replay (not only at commit time).
//!
//! # Flake Structure
//!
//! Each commit generates 8-9 flakes:
//!
//! **Commit subject flakes** (subject = commit IRI):
//! - `ledger#address` - commit storage address (xsd:string)
//! - `ledger#alias` - ledger alias (xsd:string)
//! - `ledger#v` - commit version (xsd:int)
//! - `ledger#time` - timestamp in epoch ms (xsd:long)
//! - `ledger#data` - reference to DB subject (@id)
//! - `ledger#previous` - reference to previous commit (@id, optional)
//!
//! **DB data subject flakes** (subject = synthetic DB IRI):
//! - `ledger#t` - transaction number (xsd:int)
//! - `ledger#size` - cumulative size in bytes (xsd:long)
//! - `ledger#flakes` - cumulative flake count (xsd:long)
//!
//! # Note on DB Subject
//!
//! The DB subject is currently synthetic - derived from the commit hash
//! rather than a true content-addressed DB snapshot. This is documented
//! as a known limitation until real DB snapshot addressing is implemented.

use chrono::DateTime;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, FLUREE_LEDGER, JSON_LD, XSD};
use fluree_vocab::{ledger, xsd_names};
use fluree_db_core::{Flake, FlakeValue, Sid};

use crate::Commit;

/// Commit metadata version (matches Clojure's commit_version)
const COMMIT_VERSION: i32 = 2;

/// Parse ISO-8601 timestamp to epoch milliseconds
///
/// Falls back to 0 if parsing fails.
fn iso_to_epoch_ms(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

/// Extract the hex hash local-part from a commit IRI.
///
/// Rust commit IDs are expected to look like: `fluree:commit:sha256:<hex>`.
/// Returns None if it cannot be extracted.
fn commit_iri_hex_local_part(commit_id: &str) -> Option<&str> {
    if let Some(hex) = commit_id.strip_prefix("fluree:commit:sha256:") {
        return Some(hex);
    }
    // Back-compat / defensive: allow "fluree:commit:<sha256:hex>" format
    let tail = commit_id.strip_prefix("fluree:commit:")?;
    tail.strip_prefix("sha256:").or(Some(tail))
}

/// Generate commit metadata flakes for a commit (Clojure parity).
///
/// This function creates flakes that represent commit metadata in the index,
/// enabling efficient queries for commit information and SHA-based time travel.
///
/// ## Important
/// This must be safe to call during ledger load/replay. If a commit is missing
/// an ID (older commit files), this function returns an empty vector rather than panicking.
pub fn generate_commit_flakes(commit: &Commit, ledger_alias: &str, t: i64) -> Vec<Flake> {
    let Some(commit_id) = commit.id.as_deref() else {
        return Vec::new();
    };
    let Some(hex) = commit_iri_hex_local_part(commit_id) else {
        return Vec::new();
    };

    let mut flakes = Vec::with_capacity(9);

    // Build commit subject SID
    // FLUREE_COMMIT prefix is "fluree:commit:sha256:", so name is hex only
    let commit_sid = Sid::new(FLUREE_COMMIT, hex);

    // Build synthetic DB subject SID (Option B: derive from commit hash)
    // FLUREE_DB prefix is "fluree:db:sha256:", so name is hex only
    let db_sid = Sid::new(FLUREE_DB, hex);

    // Datatype SIDs
    let string_dt = Sid::new(XSD, xsd_names::STRING);
    let int_dt = Sid::new(XSD, xsd_names::INT);
    let long_dt = Sid::new(XSD, xsd_names::LONG);
    let ref_dt = Sid::new(JSON_LD, "id"); // Reference datatype

    // === Commit subject flakes ===

    // 1. ledger#address (commit storage address)
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_LEDGER, ledger::ADDRESS),
        FlakeValue::String(commit.address.clone()),
        string_dt.clone(),
        t,
        true,
        None,
    ));

    // 2. ledger#alias
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_LEDGER, ledger::ALIAS),
        FlakeValue::String(ledger_alias.to_string()),
        string_dt.clone(),
        t,
        true,
        None,
    ));

    // 3. ledger#v (commit version)
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_LEDGER, ledger::V),
        FlakeValue::Long(COMMIT_VERSION as i64),
        int_dt.clone(),
        t,
        true,
        None,
    ));

    // 4. ledger#time (timestamp as epoch milliseconds)
    if let Some(time_str) = &commit.time {
        let epoch_ms = iso_to_epoch_ms(time_str);
        flakes.push(Flake::new(
            commit_sid.clone(),
            Sid::new(FLUREE_LEDGER, ledger::TIME),
            FlakeValue::Long(epoch_ms),
            long_dt.clone(),
            t,
            true,
            None,
        ));
    }

    // 5. ledger#data (reference to DB subject)
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_LEDGER, ledger::DATA),
        FlakeValue::Ref(db_sid.clone()),
        ref_dt.clone(),
        t,
        true,
        None,
    ));

    // 6. ledger#previous (optional: reference to previous commit)
    if let Some(prev_iri) = commit.previous_id() {
        if let Some(prev_hex) = commit_iri_hex_local_part(prev_iri) {
            let prev_sid = Sid::new(FLUREE_COMMIT, prev_hex);
            flakes.push(Flake::new(
                commit_sid,
                Sid::new(FLUREE_LEDGER, ledger::PREVIOUS),
                FlakeValue::Ref(prev_sid),
                ref_dt,
                t,
                true,
                None,
            ));
        }
    }

    // === DB data subject flakes ===

    // 7. ledger#t (transaction number)
    // Note: Rust CommitData has no t field; use commit.t
    flakes.push(Flake::new(
        db_sid.clone(),
        Sid::new(FLUREE_LEDGER, ledger::T),
        FlakeValue::Long(commit.t),
        int_dt,
        t,
        true,
        None,
    ));

    // 8-9. ledger#size / ledger#flakes (cumulative stats)
    // Use xsd:long to avoid overflow from u64 stats.
    if let Some(data) = &commit.data {
        // Note: safe cast: clamp to i64::MAX on overflow.
        let size_i64 = i64::try_from(data.size).unwrap_or(i64::MAX);
        let flakes_i64 = i64::try_from(data.flakes).unwrap_or(i64::MAX);

        flakes.push(Flake::new(
            db_sid.clone(),
            Sid::new(FLUREE_LEDGER, ledger::SIZE),
            FlakeValue::Long(size_i64),
            long_dt.clone(),
            t,
            true,
            None,
        ));

        flakes.push(Flake::new(
            db_sid,
            Sid::new(FLUREE_LEDGER, ledger::FLAKES),
            FlakeValue::Long(flakes_i64),
            long_dt,
            t,
            true,
            None,
        ));
    }

    flakes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommitData, CommitRef};

    fn make_test_commit(with_previous: bool) -> Commit {
        let mut commit = Commit::new("fluree:file://test/commit/abc123.json", 5, vec![]);
        commit.id = Some(
            "fluree:commit:sha256:abc123def456789012345678901234567890123456789012345678901234"
                .to_string(),
        );
        commit.time = Some("2025-01-20T12:00:00Z".to_string());
        commit.data = Some(CommitData {
            id: None,
            address: None,
            flakes: 100,
            size: 5000,
            previous: None,
        });

        if with_previous {
            commit.previous_ref = Some(CommitRef {
                id: Some(
                    "fluree:commit:sha256:prev123def456789012345678901234567890123456789012345678901234"
                        .to_string(),
                ),
                address: "fluree:file://test/commit/prev123.json".to_string(),
            });
            commit.previous = Some("fluree:file://test/commit/prev123.json".to_string());
        }

        commit
    }

    #[test]
    fn test_generate_commit_flakes_basic() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Should have 8 flakes (no previous)
        assert_eq!(flakes.len(), 8);

        // Check commit subject uses correct namespace
        let commit_flake = &flakes[0];
        assert_eq!(commit_flake.s.namespace_code, FLUREE_COMMIT);
        // Name should be hex only, not "sha256:<hex>"
        assert!(
            !commit_flake.s.name.starts_with("sha256:"),
            "SID name should be hex only, got: {}",
            commit_flake.s.name
        );
    }

    #[test]
    fn test_generate_commit_flakes_with_previous() {
        let commit = make_test_commit(true);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Should have 9 flakes (includes ledger#previous)
        assert_eq!(flakes.len(), 9);

        // Find the previous flake
        let prev_flake = flakes.iter().find(|f| {
            f.p.namespace_code == FLUREE_LEDGER && f.p.name.as_ref() == ledger::PREVIOUS
        });
        assert!(prev_flake.is_some(), "Should have ledger#previous flake");

        let prev_flake = prev_flake.unwrap();
        // Verify it's a ref with correct datatype
        assert!(
            matches!(&prev_flake.o, FlakeValue::Ref(_)),
            "Previous should be a Ref"
        );
        assert_eq!(prev_flake.dt.namespace_code, JSON_LD);
        assert_eq!(prev_flake.dt.name.as_ref(), "id");
    }

    #[test]
    fn test_ref_flakes_have_correct_datatype() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Find the ledger#data flake
        let data_flake = flakes.iter().find(|f| {
            f.p.namespace_code == FLUREE_LEDGER && f.p.name.as_ref() == ledger::DATA
        });
        assert!(data_flake.is_some(), "Should have ledger#data flake");

        let data_flake = data_flake.unwrap();
        // Verify it's a ref with JSON_LD/"id" datatype
        assert!(
            matches!(&data_flake.o, FlakeValue::Ref(_)),
            "Data should be a Ref"
        );
        assert_eq!(data_flake.dt.namespace_code, JSON_LD);
        assert_eq!(data_flake.dt.name.as_ref(), "id");
    }

    #[test]
    fn test_sid_local_part_is_hex_only() {
        // Regression test: ensure we don't create "sha256:sha256:<hex>"
        let commit_id = "fluree:commit:sha256:abc123def456";
        let hex = commit_id.strip_prefix("fluree:commit:sha256:").unwrap();
        assert_eq!(hex, "abc123def456");

        // Also test via actual flake generation
        let mut commit = Commit::new("addr", 1, vec![]);
        commit.id = Some(commit_id.to_string());
        commit.data = Some(CommitData::default());

        let flakes = generate_commit_flakes(&commit, "test:main", 1);
        let commit_flake = &flakes[0];

        assert_eq!(commit_flake.s.name.as_ref(), "abc123def456");
    }

    #[test]
    fn test_iso_to_epoch_ms() {
        // Test valid ISO timestamp
        let epoch_ms = iso_to_epoch_ms("2025-01-20T12:00:00Z");
        assert!(epoch_ms > 0);
        // 2025-01-20T12:00:00Z should be approximately 1737374400000 ms
        assert!(epoch_ms > 1737000000000);
        assert!(epoch_ms < 1738000000000);

        // Test invalid timestamp returns 0
        let invalid_ms = iso_to_epoch_ms("not-a-date");
        assert_eq!(invalid_ms, 0);
    }

    #[test]
    fn test_db_subject_flakes() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Find ledger#t flake
        let t_flake = flakes.iter().find(|f| {
            f.p.namespace_code == FLUREE_LEDGER && f.p.name.as_ref() == ledger::T
        });
        assert!(t_flake.is_some());
        let t_flake = t_flake.unwrap();
        assert_eq!(t_flake.s.namespace_code, FLUREE_DB);
        assert!(matches!(&t_flake.o, FlakeValue::Long(5)));

        // Find ledger#size flake
        let size_flake = flakes.iter().find(|f| {
            f.p.namespace_code == FLUREE_LEDGER && f.p.name.as_ref() == ledger::SIZE
        });
        assert!(size_flake.is_some());
        let size_flake = size_flake.unwrap();
        assert!(matches!(&size_flake.o, FlakeValue::Long(5000)));

        // Find ledger#flakes flake
        let flakes_flake = flakes.iter().find(|f| {
            f.p.namespace_code == FLUREE_LEDGER && f.p.name.as_ref() == ledger::FLAKES
        });
        assert!(flakes_flake.is_some());
        let flakes_flake = flakes_flake.unwrap();
        assert!(matches!(&flakes_flake.o, FlakeValue::Long(100)));
    }

    #[test]
    fn test_missing_id_is_safe() {
        let commit = Commit::new("addr", 1, vec![]);
        let flakes = generate_commit_flakes(&commit, "test:main", 1);
        assert!(flakes.is_empty(), "missing commit.id should yield no metadata flakes");
    }
}

