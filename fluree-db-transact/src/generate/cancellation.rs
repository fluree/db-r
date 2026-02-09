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
/// Returns flakes in deterministic sorted order (by SPOT index) for
/// reproducible hashing and tests.
pub fn apply_cancellation(flakes: Vec<Flake>) -> Vec<Flake> {
    let cap = flakes.len();

    // Use map keys only (value = ()) to avoid cloning flakes.
    //
    // We rely on `Flake`'s `Eq`/`Hash` implementation, which ignores `t` and `op`
    // but includes metadata `m`, so assertion/retraction pairs can be matched by key.
    let mut assertions: FxHashMap<Flake, ()> = FxHashMap::with_capacity_and_hasher(cap, Default::default());
    let mut retractions: FxHashMap<Flake, ()> = FxHashMap::with_capacity_and_hasher(cap, Default::default());

    for flake in flakes {
        if flake.op {
            // Assertion - try to cancel with a pending retraction
            if retractions.remove(&flake).is_none() {
                assertions.insert(flake, ());
            }
        } else {
            // Retraction - try to cancel with a pending assertion
            if assertions.remove(&flake).is_none() {
                retractions.insert(flake, ());
            }
        }
    }

    // Collect remaining flakes
    let mut result: Vec<Flake> = assertions.into_keys().chain(retractions.into_keys()).collect();

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
}
