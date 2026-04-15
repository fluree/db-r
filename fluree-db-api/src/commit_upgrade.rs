//! Commit Upgrade — legacy-ledger audit and repair for same-canonical
//! retract+assert flakes lost during reindex.
//!
//! Repairs ledgers whose binary index is missing flakes because one or more
//! legacy v3 commits contain a retract and an assert of the same canonical
//! `(s, p, o, dt)` triple within a single commit. Current main's write path
//! cannot produce that shape (stage-time `apply_cancellation` collapses the
//! pair), but pre-fix commits already on disk still exhibit the bug when
//! replayed through `fluree reindex`.
//!
//! Filed as fluree/db-r#152. Background, reproducer, and out-of-scope
//! discussion live in that issue.
//!
//! # Audit algorithm
//!
//! 1. Walk the commit DAG head→genesis, then process commits in ascending
//!    `t` order.
//! 2. For each commit, run `apply_cancellation` (the same primitive used by
//!    the write path) to collapse any same-canonical no-op pairs, then fold
//!    the surviving flakes into a running `HashMap<Flake, bool>` where the
//!    value is the most recent operation (`true` = asserted, `false` =
//!    retracted). `Flake`'s `Eq`/`Hash` ignores `t`/`op`, so the map key is
//!    canonical fact identity.
//! 3. The canonical-should-exist set is every entry whose value is `true`.
//! 4. Scan the live index two ways — per-subject via `IndexType::Spot` and
//!    per-predicate via `IndexType::Psot`. A canonical flake is a repair
//!    target if it is missing from **either** scan (catches total-loss and
//!    SPOT/PSOT-divergence cases with one code path).

use fluree_db_core::{
    collect_dag_cids, load_commit_by_id, range, Flake, FlakeValue, IndexType, LedgerSnapshot,
    RangeMatch, RangeOptions, RangeTest, Sid, DEFAULT_GRAPH_ID,
};
use fluree_db_nameservice::NameService;
use fluree_db_transact::apply_cancellation;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, info};

use crate::{ApiError, Fluree, Result};

/// IRI-level fact identity used for set comparison.
///
/// Raw `Flake` Eq/Hash keys on Sid bytes (ns_code + name), which are not
/// stable across read paths: the legacy v3 commit reader may produce Sids
/// with different ns_code/name packing than what the binary index stores
/// after a rebuild. Decoding every component to its IRI string form and
/// hashing on that gives a stable identity that survives the mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FactKey {
    subject: String,
    predicate: String,
    /// Stable string form of the object — for references, the decoded IRI;
    /// for literals, a type-tagged serialized form.
    object: String,
    datatype: String,
    /// Preserve metadata identity (language tag, list index) so that flakes
    /// differing only in `m` don't collide.
    meta: Option<(Option<String>, Option<i32>)>,
}

fn sid_to_iri(snapshot: &LedgerSnapshot, sid: &Sid) -> String {
    snapshot.decode_sid(sid).unwrap_or_else(|| sid.to_string())
}

fn value_to_key(snapshot: &LedgerSnapshot, v: &FlakeValue) -> String {
    match v {
        FlakeValue::Ref(sid) => format!("iri:{}", sid_to_iri(snapshot, sid)),
        FlakeValue::String(s) => format!("str:{}", s),
        FlakeValue::Json(s) => format!("json:{}", s),
        FlakeValue::Long(n) => format!("long:{}", n),
        FlakeValue::Double(d) => format!("dbl:{}", d),
        FlakeValue::Boolean(b) => format!("bool:{}", b),
        // For the remaining FlakeValue variants, Debug yields a stable form
        // derived from the stored canonical bytes — good enough for audit
        // identity, even if not pretty.
        other => format!("{:?}", other),
    }
}

fn fact_key(snapshot: &LedgerSnapshot, f: &Flake) -> FactKey {
    FactKey {
        subject: sid_to_iri(snapshot, &f.s),
        predicate: sid_to_iri(snapshot, &f.p),
        object: value_to_key(snapshot, &f.o),
        datatype: sid_to_iri(snapshot, &f.dt),
        meta: f.m.as_ref().map(|m| (m.lang.clone(), m.i)),
    }
}

/// Classification of a missing canonical flake discovered by the audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingKind {
    /// Missing from both the SPOT (subject-first) and PSOT (predicate-first) scans.
    /// This is the total-loss shape: the flake is invisible to every query path.
    Both,
    /// Present in the PSOT scan but missing from the SPOT scan.
    /// Subject-first and graph-crawl wildcard pulls don't see it.
    SpotOnly,
    /// Present in the SPOT scan but missing from the PSOT scan.
    /// Predicate-first scans don't see it.
    PsotOnly,
}

/// A canonical flake the audit determined should exist but couldn't find in
/// the live index via one or both scan orders.
#[derive(Debug, Clone)]
pub struct MissingFlake {
    /// Which scans missed this flake.
    pub kind: MissingKind,
    /// Decoded subject IRI (from `snapshot.decode_sid`).
    pub subject: String,
    /// Decoded predicate IRI.
    pub predicate: String,
    /// Decoded datatype IRI.
    pub datatype: String,
    /// Object value as stored in the flake (already canonicalized by the
    /// commit reader if this is legacy v3 data).
    pub value: FlakeValue,
    /// Raw flake in assert form, suitable for direct re-insertion during the
    /// `--force` repair path. The `t` field is an arbitrary survivor from the
    /// replay and should be treated as opaque.
    pub flake: Flake,
}

/// Result of a commit-upgrade audit. Returned by `--dry-run`.
#[derive(Debug, Clone)]
pub struct CommitUpgradeAudit {
    /// Ledger under audit (normalized form, e.g. `_system:main`).
    pub ledger_id: String,
    /// Number of commits walked during chain replay.
    pub commits_walked: usize,
    /// Number of canonical triples the commit chain says should exist at head.
    pub canonical_count: usize,
    /// Number of distinct assert flakes found via per-subject SPOT scans.
    pub spot_scan_count: usize,
    /// Number of distinct assert flakes found via per-predicate PSOT scans.
    pub psot_scan_count: usize,
    /// Canonical flakes missing from one or both scans, ordered for deterministic
    /// output: `Both` first, then `SpotOnly`, then `PsotOnly`.
    pub missing: Vec<MissingFlake>,
}

impl CommitUpgradeAudit {
    /// Total number of flakes needing repair.
    pub fn missing_len(&self) -> usize {
        self.missing.len()
    }

    /// Whether the ledger is clean (no repair needed).
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty()
    }

    /// Count of missing flakes in each classification.
    pub fn counts_by_kind(&self) -> (usize, usize, usize) {
        let mut both = 0;
        let mut spot_only = 0;
        let mut psot_only = 0;
        for m in &self.missing {
            match m.kind {
                MissingKind::Both => both += 1,
                MissingKind::SpotOnly => spot_only += 1,
                MissingKind::PsotOnly => psot_only += 1,
            }
        }
        (both, spot_only, psot_only)
    }
}

impl<N> Fluree<N>
where
    N: NameService + Send + Sync + 'static,
{
    /// Audit a legacy ledger for flakes missing from the live index due to
    /// the fluree/db-r#152 reindex bug. Does not mutate the ledger.
    ///
    /// See module docs for the algorithm. The caller is responsible for
    /// ensuring `ledger_id` is in normalized form (e.g., `_system:main`) or
    /// an alias that will normalize correctly via `fluree.ledger()`.
    pub async fn audit_commit_upgrade(&self, ledger_id: &str) -> Result<CommitUpgradeAudit> {
        info!(ledger_id = %ledger_id, "Starting commit-upgrade audit");

        let state = self.ledger(ledger_id).await?;
        let snapshot = &state.snapshot;
        let normalized_id = snapshot.ledger_id.to_string();

        let head_commit_id = state
            .ns_record
            .as_ref()
            .and_then(|r| r.commit_head_id.clone())
            .or_else(|| state.head_commit_id.clone())
            .ok_or_else(|| {
                ApiError::NotFound(format!("ledger {} has no commits to audit", normalized_id))
            })?;

        let cs = self.content_store(&normalized_id);

        // 1. Walk commit DAG. `collect_dag_cids(..., -1)` returns every commit
        //    with `t > -1`, i.e. the full chain, sorted descending by `t`.
        let mut cids = collect_dag_cids(cs.as_ref(), &head_commit_id, -1).await?;
        cids.reverse(); // ascending order for forward replay
        let commits_walked = cids.len();
        debug!(commits_walked, "Commit DAG walked");

        // 2. Forward replay with per-commit apply_cancellation. Each flake's
        //    canonical identity is keyed via `FactKey` (decoded-IRI form) so
        //    that Sid-encoding differences between the commit reader and the
        //    live index don't create spurious mismatches.
        //
        //    The map value is `(last_op, representative_flake)`: the flake
        //    itself is retained so we can emit it in the repair commit's
        //    assert payload later, and so the missing-flake report can show
        //    the object value without re-plumbing it through FactKey.
        let mut state_map: FxHashMap<FactKey, (bool, Flake)> = FxHashMap::default();
        for (_t, cid) in &cids {
            let commit = load_commit_by_id(cs.as_ref(), cid).await?;
            let canceled = apply_cancellation(commit.flakes);
            for flake in canceled {
                let key = fact_key(snapshot, &flake);
                let op = flake.op;
                let mut flake_assert = flake;
                flake_assert.op = true;
                state_map.insert(key, (op, flake_assert));
            }
        }

        // 3. Canonical-should-exist = entries whose final op was assert.
        let canonical: Vec<(FactKey, Flake)> = state_map
            .into_iter()
            .filter_map(|(key, (op, flake))| op.then_some((key, flake)))
            .collect();
        let canonical_count = canonical.len();
        debug!(canonical_count, "Canonical should-exist set computed");

        // 4. Live-index scans. Per-subject SPOT and per-predicate PSOT, to
        //    avoid the wildcard-?p query-layer incompleteness bug and catch
        //    any SPOT/PSOT divergence at the same time. We derive scan
        //    targets from the canonical flakes' raw Sids (not the decoded
        //    IRIs) because `range()` needs Sid-level matching to seek.
        let subjects: FxHashSet<Sid> = canonical.iter().map(|(_, f)| f.s.clone()).collect();
        let predicates: FxHashSet<Sid> = canonical.iter().map(|(_, f)| f.p.clone()).collect();
        debug!(
            subjects = subjects.len(),
            predicates = predicates.len(),
            "Live-index scan targets derived"
        );

        let mut spot_set: FxHashSet<FactKey> = FxHashSet::default();
        for s in &subjects {
            let flakes = range(
                snapshot,
                DEFAULT_GRAPH_ID,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(s.clone()),
                RangeOptions::default(),
            )
            .await
            .map_err(|e| ApiError::internal(format!("SPOT scan failed for {}: {}", s, e)))?;
            for f in flakes.into_iter().filter(|f| f.op) {
                spot_set.insert(fact_key(snapshot, &f));
            }
        }

        let mut psot_set: FxHashSet<FactKey> = FxHashSet::default();
        for p in &predicates {
            let flakes = range(
                snapshot,
                DEFAULT_GRAPH_ID,
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(p.clone()),
                RangeOptions::default(),
            )
            .await
            .map_err(|e| ApiError::internal(format!("PSOT scan failed for {}: {}", p, e)))?;
            for f in flakes.into_iter().filter(|f| f.op) {
                psot_set.insert(fact_key(snapshot, &f));
            }
        }

        // 5. Diff canonical against both scans.
        let mut missing: Vec<MissingFlake> = Vec::new();
        for (key, f) in canonical {
            let in_spot = spot_set.contains(&key);
            let in_psot = psot_set.contains(&key);
            let kind = match (in_spot, in_psot) {
                (true, true) => continue,
                (false, false) => MissingKind::Both,
                (true, false) => MissingKind::PsotOnly,
                (false, true) => MissingKind::SpotOnly,
            };
            missing.push(MissingFlake {
                kind,
                subject: key.subject,
                predicate: key.predicate,
                datatype: key.datatype,
                value: f.o.clone(),
                flake: f,
            });
        }

        // Stable ordering: Both, SpotOnly, PsotOnly, then by (subject, predicate).
        missing.sort_by(|a, b| {
            let ord_a = match a.kind {
                MissingKind::Both => 0,
                MissingKind::SpotOnly => 1,
                MissingKind::PsotOnly => 2,
            };
            let ord_b = match b.kind {
                MissingKind::Both => 0,
                MissingKind::SpotOnly => 1,
                MissingKind::PsotOnly => 2,
            };
            ord_a
                .cmp(&ord_b)
                .then_with(|| a.subject.cmp(&b.subject))
                .then_with(|| a.predicate.cmp(&b.predicate))
        });

        info!(
            ledger_id = %normalized_id,
            commits_walked,
            canonical_count,
            spot_scan_count = spot_set.len(),
            psot_scan_count = psot_set.len(),
            missing_count = missing.len(),
            "Commit-upgrade audit complete"
        );

        Ok(CommitUpgradeAudit {
            ledger_id: normalized_id,
            commits_walked,
            canonical_count,
            spot_scan_count: spot_set.len(),
            psot_scan_count: psot_set.len(),
            missing,
        })
    }
}
