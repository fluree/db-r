//! Time-aware novelty filtering for incremental refresh
//!
//! Wraps `Novelty::slice_for_range()` with time filtering to only process
//! novelty flakes within the target time range (since_t, to_t].

use fluree_db_core::{Flake, IndexType};
#[cfg(test)]
use fluree_db_novelty::FlakeId;
use fluree_db_novelty::Novelty;

/// Check if flake is in the time range (since_t, to_t]
///
/// - `flake.t > since_t` (exclusive lower bound - already indexed)
/// - `flake.t <= to_t` (inclusive upper bound - target transaction time)
#[inline]
fn in_time_range(flake: &Flake, since_t: i64, to_t: i64) -> bool {
    flake.t > since_t && flake.t <= to_t
}

/// Check if a node has any novelty affecting it within the time range (since_t, to_t]
///
/// # Arguments
///
/// * `novelty` - The novelty overlay
/// * `index_type` - Which index to check
/// * `first` - Left boundary (exclusive if not leftmost)
/// * `rhs` - Right boundary (inclusive)
/// * `leftmost` - Whether this is the leftmost node
/// * `since_t` - Only include flakes with t > since_t (current index_t)
/// * `to_t` - Only include flakes with t <= to_t (target transaction time)
///
/// # Boundary Semantics
///
/// - `since_t` is the current `index_t`. We use strict greater-than (`flake.t > since_t`)
///   because a flake at exactly `t = index_t` was already indexed in the current index.
/// - `to_t` is the target transaction time. We use inclusive (`flake.t <= to_t`)
///   because the new index should be current through `to_t`.
pub fn has_novelty_since(
    novelty: &Novelty,
    index_type: IndexType,
    first: Option<&Flake>,
    rhs: Option<&Flake>,
    leftmost: bool,
    since_t: i64,
    to_t: i64,
) -> bool {
    let slice = novelty.slice_for_range(index_type, first, rhs, leftmost);
    slice
        .iter()
        .any(|&id| in_time_range(novelty.get_flake(id), since_t, to_t))
}

/// Get novelty flakes for a node's range within time range (since_t, to_t]
///
/// Returns an iterator over flake references that fall within the node's range
/// AND have since_t < t <= to_t.
///
/// # Arguments
///
/// * `novelty` - The novelty overlay
/// * `index_type` - Which index to query
/// * `first` - Left boundary (exclusive if not leftmost)
/// * `rhs` - Right boundary (inclusive)
/// * `leftmost` - Whether this is the leftmost node
/// * `since_t` - Only include flakes with t > since_t
/// * `to_t` - Only include flakes with t <= to_t
pub fn get_novelty_flakes_since<'a>(
    novelty: &'a Novelty,
    index_type: IndexType,
    first: Option<&Flake>,
    rhs: Option<&Flake>,
    leftmost: bool,
    since_t: i64,
    to_t: i64,
) -> impl Iterator<Item = &'a Flake> {
    let slice = novelty.slice_for_range(index_type, first, rhs, leftmost);
    slice
        .iter()
        .map(move |&id| novelty.get_flake(id))
        .filter(move |f| in_time_range(f, since_t, to_t))
}

/// Get novelty flake IDs for a node's range within time range (since_t, to_t]
///
/// Returns the FlakeIds that fall within the node's range AND have since_t < t <= to_t.
#[cfg(test)]
pub fn get_novelty_ids_since(
    novelty: &Novelty,
    index_type: IndexType,
    first: Option<&Flake>,
    rhs: Option<&Flake>,
    leftmost: bool,
    since_t: i64,
    to_t: i64,
) -> Vec<FlakeId> {
    let slice = novelty.slice_for_range(index_type, first, rhs, leftmost);
    slice
        .iter()
        .copied()
        .filter(|&id| in_time_range(novelty.get_flake(id), since_t, to_t))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(s: i32, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(1, "p1"),
            FlakeValue::Long(100),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_in_time_range() {
        let flake = make_flake(1, 5);

        // In range (2, 10]
        assert!(in_time_range(&flake, 2, 10));

        // At upper bound (2, 5] - inclusive
        assert!(in_time_range(&flake, 2, 5));

        // At lower bound (5, 10] - exclusive, so NOT in range
        assert!(!in_time_range(&flake, 5, 10));

        // Below range
        assert!(!in_time_range(&flake, 5, 6));

        // Above range
        assert!(!in_time_range(&flake, 1, 4));
    }

    #[test]
    fn test_has_novelty_since_empty() {
        let novelty = Novelty::new(0);
        // to_t=100 is arbitrary large upper bound
        let result = has_novelty_since(&novelty, IndexType::Spot, None, None, true, 0, 100);
        assert!(!result);
    }

    #[test]
    fn test_has_novelty_since_filters_by_time() {
        let mut novelty = Novelty::new(0);

        // Add flakes at t=1 and t=5
        novelty
            .apply_commit(vec![make_flake(1, 1), make_flake(2, 5)], 5)
            .unwrap();

        // (0, 10]: should have novelty (both flakes in range)
        assert!(has_novelty_since(
            &novelty,
            IndexType::Spot,
            None,
            None,
            true,
            0,
            10
        ));

        // (1, 10]: should have novelty (t=5 in range)
        assert!(has_novelty_since(
            &novelty,
            IndexType::Spot,
            None,
            None,
            true,
            1,
            10
        ));

        // (5, 10]: should NOT have novelty (no flakes with t > 5)
        assert!(!has_novelty_since(
            &novelty,
            IndexType::Spot,
            None,
            None,
            true,
            5,
            10
        ));

        // (0, 3]: should have novelty (t=1 in range, t=5 excluded by upper bound)
        assert!(has_novelty_since(
            &novelty,
            IndexType::Spot,
            None,
            None,
            true,
            0,
            3
        ));

        // (2, 4]: should NOT have novelty (t=1 excluded by lower, t=5 excluded by upper)
        assert!(!has_novelty_since(
            &novelty,
            IndexType::Spot,
            None,
            None,
            true,
            2,
            4
        ));
    }

    #[test]
    fn test_get_novelty_flakes_since_filters_correctly() {
        let mut novelty = Novelty::new(0);

        novelty
            .apply_commit(
                vec![make_flake(1, 1), make_flake(2, 3), make_flake(3, 5)],
                5,
            )
            .unwrap();

        // (2, 10]: should get flakes at t=3 and t=5
        let flakes: Vec<_> =
            get_novelty_flakes_since(&novelty, IndexType::Spot, None, None, true, 2, 10).collect();

        assert_eq!(flakes.len(), 2);
        assert!(flakes.iter().all(|f| f.t > 2 && f.t <= 10));

        // (2, 4]: should get only t=3
        let flakes: Vec<_> =
            get_novelty_flakes_since(&novelty, IndexType::Spot, None, None, true, 2, 4).collect();

        assert_eq!(flakes.len(), 1);
        assert_eq!(flakes[0].t, 3);
    }

    #[test]
    fn test_get_novelty_ids_since() {
        let mut novelty = Novelty::new(0);

        novelty
            .apply_commit(
                vec![make_flake(1, 1), make_flake(2, 3), make_flake(3, 5)],
                5,
            )
            .unwrap();

        // (2, 10]: should get 2 flakes
        let ids = get_novelty_ids_since(&novelty, IndexType::Spot, None, None, true, 2, 10);
        assert_eq!(ids.len(), 2);

        // Verify all returned IDs have 2 < t <= 10
        for id in ids {
            let t = novelty.get_flake(id).t;
            assert!(t > 2 && t <= 10);
        }

        // (0, 3]: should get 2 flakes (t=1 and t=3)
        let ids = get_novelty_ids_since(&novelty, IndexType::Spot, None, None, true, 0, 3);
        assert_eq!(ids.len(), 2);
    }
}
