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

/// Classification of a missing canonical flake.
///
/// Split into two dispositions: `auto_repairable()` flakes are safe to migrate
/// via the `--force` commit-chain upgrade path; everything else is reported
/// for operator review without mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissingKind {
    /// Missing from both SPOT and PSOT, and the audit observed a same-commit
    /// retract+assert pair of the same canonical `(s, p, o, dt)` in the
    /// commit chain. This is the documented fluree/db-r#152 shape — pre-148
    /// write code failed to cancel the pair at stage time, so both sides
    /// persisted in the commit blob, and the post-148 reindex collapsed them
    /// to nothing. **Auto-repairable**: commit migration via the current
    /// write path will produce a canceled (empty) commit at this t, and the
    /// flake survives from whichever earlier commit first asserted it.
    CancellationBug {
        /// Commit t values at which a same-commit retract+assert pair for
        /// this canonical fact was observed.
        same_commit_t: Vec<i64>,
    },
    /// Missing from SPOT, present in PSOT. The canonical set has exactly one
    /// value for `(subject, predicate)` — no multi-cardinality, no intent
    /// ambiguity. Observed on production teams (`team:4b3d4421`,
    /// `team:8d34f35b`, `team:commons`) whose properties were pure inserts
    /// at commit time. **Auto-repairable**.
    SpotDropout,
    /// Missing from PSOT, present in SPOT. Symmetric to `SpotDropout`.
    /// Zero instances observed on the production snapshot today, but
    /// included for completeness and safety. **Auto-repairable**.
    PsotDropout,
    /// Missing from one or both scans, but the same `(subject, predicate)`
    /// has more than one canonical value at head. We can't tell from the
    /// commit chain whether the operator intended multi-cardinality or
    /// whether a pre-148 upsert failed to emit a retract of the earlier
    /// value. **Not auto-repaired**: reported for operator review so they
    /// can decide case-by-case.
    AmbiguousIntent {
        /// How many canonical values exist for `(subject, predicate)` at head.
        canonical_multiplicity: usize,
    },
    /// Missing from both SPOT and PSOT with no observed same-commit
    /// retract+assert pair — a shape we haven't characterized yet. Reported
    /// but **not auto-repaired** until its mechanism is understood.
    UnknownBoth,
}

impl MissingKind {
    /// Whether this class is safe for `--force` commit migration to repair
    /// automatically. Classes that return `false` are reported only.
    pub fn auto_repairable(&self) -> bool {
        matches!(
            self,
            MissingKind::CancellationBug { .. }
                | MissingKind::SpotDropout
                | MissingKind::PsotDropout
        )
    }

    /// Short slug for CLI/logging output.
    pub fn label(&self) -> &'static str {
        match self {
            MissingKind::CancellationBug { .. } => "cancellation-bug",
            MissingKind::SpotDropout => "spot-dropout",
            MissingKind::PsotDropout => "psot-dropout",
            MissingKind::AmbiguousIntent { .. } => "ambiguous-intent",
            MissingKind::UnknownBoth => "unknown-both",
        }
    }
}

/// A canonical flake the audit determined should exist but couldn't find in
/// the live index via one or both scan orders.
#[derive(Debug, Clone)]
pub struct MissingFlake {
    /// Classification + per-kind metadata.
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

/// Histogram of missing-flake classifications.
#[derive(Debug, Clone, Default)]
pub struct MissingKindCounts {
    pub cancellation_bug: usize,
    pub spot_dropout: usize,
    pub psot_dropout: usize,
    pub ambiguous_intent: usize,
    pub unknown_both: usize,
}

impl MissingKindCounts {
    pub fn auto_repairable_total(&self) -> usize {
        self.cancellation_bug + self.spot_dropout + self.psot_dropout
    }

    pub fn review_required_total(&self) -> usize {
        self.ambiguous_intent + self.unknown_both
    }
}

impl CommitUpgradeAudit {
    /// Total number of flakes needing repair or review.
    pub fn missing_len(&self) -> usize {
        self.missing.len()
    }

    /// Whether the ledger is clean (no repair needed).
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty()
    }

    /// Histogram of missing-flake classes.
    pub fn counts_by_kind(&self) -> MissingKindCounts {
        let mut c = MissingKindCounts::default();
        for m in &self.missing {
            match &m.kind {
                MissingKind::CancellationBug { .. } => c.cancellation_bug += 1,
                MissingKind::SpotDropout => c.spot_dropout += 1,
                MissingKind::PsotDropout => c.psot_dropout += 1,
                MissingKind::AmbiguousIntent { .. } => c.ambiguous_intent += 1,
                MissingKind::UnknownBoth => c.unknown_both += 1,
            }
        }
        c
    }

    /// Iterator over the subset of missing flakes that `--force` will repair.
    pub fn auto_repairable(&self) -> impl Iterator<Item = &MissingFlake> {
        self.missing.iter().filter(|m| m.kind.auto_repairable())
    }

    /// Iterator over the subset of missing flakes that require operator review.
    pub fn review_required(&self) -> impl Iterator<Item = &MissingFlake> {
        self.missing.iter().filter(|m| !m.kind.auto_repairable())
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
        //
        // Diagnostic hook: if `FLUREE_COMMIT_UPGRADE_TRACE` is set, we dump
        // per-commit activity for subjects whose decoded IRI or object value
        // contains the provided substring. Pre- and post-cancellation flakes
        // are both printed so we can see whether `apply_cancellation` is
        // dropping a flake on the write-replay side. This is a rare-use
        // developer aid for investigating divergence shapes; it stays off by
        // default and costs nothing when unset.
        let trace_subject = std::env::var("FLUREE_COMMIT_UPGRADE_TRACE").ok();

        // Forward replay. Two side outputs beyond `state_map`:
        //
        // - `same_commit_pairs`: for every FactKey that appeared with both
        //   an assert and a retract in the same commit, the list of commit t
        //   values where that happened. This is the fingerprint of the
        //   fluree/db-r#152 `CancellationBug` class; we use it downstream to
        //   distinguish "missing-both due to known bug" from "missing-both
        //   due to unknown cause."
        // - `state_map`: canonical final state keyed by FactKey (ignoring
        //   t/op), value is `(last_op, representative_assert_flake)`.
        let mut state_map: FxHashMap<FactKey, (bool, Flake)> = FxHashMap::default();
        let mut same_commit_pairs: FxHashMap<FactKey, Vec<i64>> = FxHashMap::default();
        for (t, cid) in &cids {
            let commit = load_commit_by_id(cs.as_ref(), cid).await?;

            if let Some(ref needle) = trace_subject {
                let pre_matches: Vec<_> = commit
                    .flakes
                    .iter()
                    .filter(|f| {
                        let s_match = snapshot
                            .decode_sid(&f.s)
                            .map(|iri| iri.contains(needle.as_str()))
                            .unwrap_or(false);
                        let o_match = value_to_key(snapshot, &f.o).contains(needle.as_str());
                        s_match || o_match
                    })
                    .collect();
                if !pre_matches.is_empty() {
                    eprintln!("  [trace] t={} cid={} pre-cancellation:", t, cid);
                    for f in &pre_matches {
                        eprintln!(
                            "    {} {} {} {} op={}",
                            snapshot.decode_sid(&f.s).unwrap_or_default(),
                            snapshot.decode_sid(&f.p).unwrap_or_default(),
                            value_to_key(snapshot, &f.o),
                            snapshot.decode_sid(&f.dt).unwrap_or_default(),
                            f.op
                        );
                    }
                }
            }

            // Detect same-commit retract+assert pairs BEFORE cancellation
            // strips them. Any FactKey that has both ops in this commit is
            // a cancellation-bug fingerprint.
            let mut per_key_ops: FxHashMap<FactKey, (bool, bool)> = FxHashMap::default();
            for f in &commit.flakes {
                let key = fact_key(snapshot, f);
                let entry = per_key_ops.entry(key).or_insert((false, false));
                if f.op {
                    entry.0 = true;
                } else {
                    entry.1 = true;
                }
            }
            for (key, (has_assert, has_retract)) in per_key_ops {
                if has_assert && has_retract {
                    same_commit_pairs.entry(key).or_default().push(*t);
                }
            }

            let canceled = apply_cancellation(commit.flakes);

            if let Some(ref needle) = trace_subject {
                let post_matches: Vec<_> = canceled
                    .iter()
                    .filter(|f| {
                        let s_match = snapshot
                            .decode_sid(&f.s)
                            .map(|iri| iri.contains(needle.as_str()))
                            .unwrap_or(false);
                        let o_match = value_to_key(snapshot, &f.o).contains(needle.as_str());
                        s_match || o_match
                    })
                    .collect();
                if !post_matches.is_empty() {
                    eprintln!("  [trace] t={} cid={} post-cancellation:", t, cid);
                    for f in &post_matches {
                        eprintln!(
                            "    {} {} {} {} op={}",
                            snapshot.decode_sid(&f.s).unwrap_or_default(),
                            snapshot.decode_sid(&f.p).unwrap_or_default(),
                            value_to_key(snapshot, &f.o),
                            snapshot.decode_sid(&f.dt).unwrap_or_default(),
                            f.op
                        );
                    }
                }
            }

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

        // Per-(subject, predicate) multiplicity at head: distinguishes
        // unambiguous single-value dropouts from multi-value cases where
        // intent can't be recovered from the commit chain alone.
        let mut per_sp_count: FxHashMap<(String, String), usize> = FxHashMap::default();
        for (key, _) in &canonical {
            *per_sp_count
                .entry((key.subject.clone(), key.predicate.clone()))
                .or_insert(0) += 1;
        }

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

        // 5. Diff canonical against both scans and classify by mechanism.
        let mut missing: Vec<MissingFlake> = Vec::new();
        for (key, f) in canonical {
            let in_spot = spot_set.contains(&key);
            let in_psot = psot_set.contains(&key);
            if in_spot && in_psot {
                continue;
            }

            let multiplicity = per_sp_count
                .get(&(key.subject.clone(), key.predicate.clone()))
                .copied()
                .unwrap_or(1);

            let kind = match (in_spot, in_psot) {
                (true, true) => unreachable!(),
                (false, false) => {
                    // Missing-both: check for same-commit retract+assert
                    // fingerprint. If present, it's the documented #152
                    // cancellation bug and is auto-repairable via commit
                    // migration. Otherwise it's an unknown shape we haven't
                    // characterized; report without auto-repair.
                    if let Some(ts) = same_commit_pairs.get(&key) {
                        MissingKind::CancellationBug {
                            same_commit_t: ts.clone(),
                        }
                    } else {
                        MissingKind::UnknownBoth
                    }
                }
                (true, false) | (false, true) => {
                    // Missing from one index only. If the (s, p) has a single
                    // canonical value at head, it's unambiguously a dropout
                    // from that index and is auto-repairable. If > 1, intent
                    // can't be recovered from the commit chain alone — could
                    // be legitimate multi-cardinality or a buggy pre-148
                    // upsert that failed to emit a retract. Flag for review.
                    if multiplicity > 1 {
                        MissingKind::AmbiguousIntent {
                            canonical_multiplicity: multiplicity,
                        }
                    } else if !in_spot {
                        MissingKind::SpotDropout
                    } else {
                        MissingKind::PsotDropout
                    }
                }
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

        // Stable ordering: auto-repairable classes first (for operator focus
        // on the actionable set), then review-required; within each group,
        // by kind label, then by (subject, predicate) for determinism.
        missing.sort_by(|a, b| {
            let repairable_rank = |m: &MissingFlake| if m.kind.auto_repairable() { 0 } else { 1 };
            repairable_rank(a)
                .cmp(&repairable_rank(b))
                .then_with(|| a.kind.label().cmp(b.kind.label()))
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
