//! Assertion/retraction cancellation
//!
//! When the same fact appears both as an assertion and a retraction
//! in a transaction, they cancel each other out. This module implements
//! that cancellation logic.
//!
//! Uses `Flake`'s existing `Eq`/`Hash` implementation which ignores
//! `t` and `op` but includes metadata `m`.

use fluree_db_core::{Flake, IndexType};
use rustc_hash::FxHashMap;

/// Apply cancellation to a set of flakes
///
/// Removes matching assertion/retraction pairs where the flakes are
/// equal (same subject, predicate, object, datatype, metadata) but
/// differ only in operation.
///
/// Uses counter-based tracking so that duplicate flakes (same fact at
/// different `t` values) are handled correctly. For example, if a fact
/// was asserted 4 times (at t=1,2,3,4) and retracted once, 3 assertions
/// survive — not 0 or 1.
///
/// Returns flakes in deterministic sorted order (by SPOT index) for
/// reproducible hashing and tests.
pub fn apply_cancellation(flakes: Vec<Flake>) -> Vec<Flake> {
    let cap = flakes.len();

    // Track counts per fact key. Flake's Eq/Hash ignores `t` and `op`,
    // so all copies of the same fact hash to the same bucket.
    // We store (Vec<assertion_flakes>, Vec<retraction_flakes>) per key.
    let mut buckets: FxHashMap<Flake, (Vec<Flake>, Vec<Flake>)> =
        FxHashMap::with_capacity_and_hasher(cap, Default::default());

    for flake in flakes {
        let entry = buckets.entry(flake.clone()).or_default();
        if flake.op {
            entry.0.push(flake);
        } else {
            entry.1.push(flake);
        }
    }

    // Cancel pairs 1:1 and collect survivors
    let mut result: Vec<Flake> = Vec::with_capacity(cap);
    for (_key, (assertions, retractions)) in buckets {
        let cancel_count = assertions.len().min(retractions.len());
        // Keep un-cancelled assertions
        for flake in assertions.into_iter().skip(cancel_count) {
            result.push(flake);
        }
        // Keep un-cancelled retractions
        for flake in retractions.into_iter().skip(cancel_count) {
            result.push(flake);
        }
    }

    // Sort for deterministic output
    result.sort_by(|a, b| IndexType::Spot.compare(a, b));

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(p, format!("p{}", p)),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    #[test]
    fn test_cancellation_removes_pairs() {
        let assertion = make_flake(1, 2, 100, 1, true);
        let retraction = make_flake(1, 2, 100, 1, false);

        let result = apply_cancellation(vec![assertion, retraction]);

        assert!(result.is_empty());
    }

    #[test]
    fn test_cancellation_keeps_unmatched() {
        let assertion = make_flake(1, 2, 100, 1, true);
        let retraction = make_flake(1, 2, 200, 1, false); // Different object

        let result = apply_cancellation(vec![assertion, retraction]);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cancellation_order_independent() {
        let flakes1 = vec![
            make_flake(1, 2, 100, 1, true),
            make_flake(1, 2, 100, 1, false),
        ];
        let flakes2 = vec![
            make_flake(1, 2, 100, 1, false),
            make_flake(1, 2, 100, 1, true),
        ];

        let result1 = apply_cancellation(flakes1);
        let result2 = apply_cancellation(flakes2);

        assert!(result1.is_empty());
        assert!(result2.is_empty());
    }

    #[test]
    fn test_cancellation_deterministic_output() {
        let flakes = vec![
            make_flake(3, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
        ];

        let result = apply_cancellation(flakes);

        // Should be sorted by SPOT (subject first)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].s.namespace_code, 1);
        assert_eq!(result[1].s.namespace_code, 2);
        assert_eq!(result[2].s.namespace_code, 3);
    }

    #[test]
    fn test_cancellation_multiple_pairs() {
        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, false), // Cancels first
            make_flake(2, 1, 200, 1, true),
            make_flake(2, 1, 200, 1, false), // Cancels third
            make_flake(3, 1, 300, 1, true),  // Remains
        ];

        let result = apply_cancellation(flakes);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].s.namespace_code, 3);
    }

    #[test]
    fn test_cancellation_different_t_still_cancels() {
        // Flake's Eq ignores t, so these should cancel
        let assertion = make_flake(1, 2, 100, 5, true);
        let retraction = make_flake(1, 2, 100, 10, false);

        let result = apply_cancellation(vec![assertion, retraction]);

        assert!(result.is_empty());
    }

    /// Regression test: duplicate retractions (same s,p,o but different t) must
    /// all survive — not be collapsed into one by HashMap dedup.
    ///
    /// Scenario: entity has 4 copies of "open" (asserted at t=1,2,3,4 due to
    /// prior bug). Upsert generates 4 retractions + 1 assertion for "open".
    /// Expected: 1 assertion cancels 1 retraction → 3 retractions remain.
    #[test]
    fn test_cancellation_preserves_duplicate_retractions() {
        let flakes = vec![
            // 4 retractions for same (s,p,o) at different t values
            make_flake(1, 2, 100, 1, false),
            make_flake(1, 2, 100, 2, false),
            make_flake(1, 2, 100, 3, false),
            make_flake(1, 2, 100, 4, false),
            // 1 assertion (upsert re-asserts "open")
            make_flake(1, 2, 100, 5, true),
        ];

        let result = apply_cancellation(flakes);

        // 1 assertion cancels 1 retraction → 3 retractions survive
        assert_eq!(result.len(), 3, "should have 3 surviving retractions");
        assert!(result.iter().all(|f| !f.op), "all survivors should be retractions");
    }

    /// Multiple duplicate assertions with fewer retractions.
    #[test]
    fn test_cancellation_preserves_duplicate_assertions() {
        let flakes = vec![
            // 3 assertions for same (s,p,o) at different t values
            make_flake(1, 2, 100, 1, true),
            make_flake(1, 2, 100, 2, true),
            make_flake(1, 2, 100, 3, true),
            // 1 retraction
            make_flake(1, 2, 100, 5, false),
        ];

        let result = apply_cancellation(flakes);

        // 1 retraction cancels 1 assertion → 2 assertions survive
        assert_eq!(result.len(), 2, "should have 2 surviving assertions");
        assert!(result.iter().all(|f| f.op), "all survivors should be assertions");
    }

    /// Mixed scenario: some facts have duplicates, some don't.
    #[test]
    fn test_cancellation_mixed_duplicates_and_unique() {
        let flakes = vec![
            // Fact A: 3 retractions, 1 assertion → 2 retractions survive
            make_flake(1, 1, 100, 1, false),
            make_flake(1, 1, 100, 2, false),
            make_flake(1, 1, 100, 3, false),
            make_flake(1, 1, 100, 5, true),
            // Fact B: 1 retraction, 1 assertion → cancel out
            make_flake(1, 1, 200, 1, false),
            make_flake(1, 1, 200, 5, true),
            // Fact C: 1 assertion only → survives
            make_flake(1, 1, 300, 5, true),
        ];

        let result = apply_cancellation(flakes);

        // 2 retractions (fact A) + 1 assertion (fact C) = 3
        assert_eq!(result.len(), 3);
        let retraction_count = result.iter().filter(|f| !f.op).count();
        let assertion_count = result.iter().filter(|f| f.op).count();
        assert_eq!(retraction_count, 2, "fact A: 2 retractions survive");
        assert_eq!(assertion_count, 1, "fact C: 1 assertion survives");
    }
}
