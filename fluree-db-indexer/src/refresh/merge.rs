//! Sorted merge of flakes for leaf updates
//!
//! Merges novelty flakes into existing leaf flakes using two-pointer merge.

use fluree_db_core::{Flake, IndexType};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Merge novelty flakes into existing leaf flakes
///
/// Both inputs must be sorted by the index comparator.
/// Output is sorted and maintains the index order.
///
/// # Arguments
///
/// * `existing` - Existing flakes from the leaf (already sorted)
/// * `novelty` - New flakes to merge in (will be sorted internally)
/// * `index_type` - Which index comparator to use
///
/// # Returns
///
/// Merged vector of flakes, sorted by index comparator.
pub fn merge_leaf_flakes(
    existing: &[Flake],
    novelty: impl Iterator<Item = Flake>,
    index_type: IndexType,
) -> Vec<Flake> {
    // Collect and sort novelty
    let mut novelty_vec: Vec<Flake> = novelty.collect();
    novelty_vec.sort_by(|a, b| index_type.compare(a, b));

    // Two-pointer merge
    merge_sorted(existing, &novelty_vec, |a, b| index_type.compare(a, b))
}

/// Two-pointer merge of two sorted slices
fn merge_sorted<F>(a: &[Flake], b: &[Flake], cmp: F) -> Vec<Flake>
where
    F: Fn(&Flake, &Flake) -> Ordering,
{
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        if cmp(&a[i], &b[j]) != Ordering::Greater {
            result.push(a[i].clone());
            i += 1;
        } else {
            result.push(b[j].clone());
            j += 1;
        }
    }

    // Append remaining elements
    result.extend(a[i..].iter().cloned());
    result.extend(b[j..].iter().cloned());

    result
}

/// Fact identity key for deduplication (matches Clojure's notion of "same fact ignoring t")
#[derive(Clone, PartialEq, Eq, Hash)]
struct FactKey {
    s: fluree_db_core::Sid,
    p: fluree_db_core::Sid,
    o: fluree_db_core::FlakeValue,
    dt: fluree_db_core::Sid,
    m: Option<fluree_db_core::FlakeMeta>,
}

fn fact_key(flake: &Flake) -> FactKey {
    FactKey {
        s: flake.s.clone(),
        p: flake.p.clone(),
        o: flake.o.clone(),
        dt: flake.dt.clone(),
        m: flake.m.clone(),
    }
}

/// Apply leaf-local deduplication to merged flakes
///
/// Removes redundant consecutive same-op flakes for the same fact.
/// This is the incremental-mode dedup (not global dedup).
///
/// # Arguments
///
/// * `flakes` - Merged flakes, sorted by index comparator
///
/// # Returns
///
/// Deduped flakes with redundant same-op runs collapsed.
pub fn dedup_leaf_local(mut flakes: Vec<Flake>) -> Vec<Flake> {
    if flakes.is_empty() {
        return flakes;
    }

    // Sort by SPOT to ensure t-ordering for dedup
    flakes.sort_by(|a, b| IndexType::Spot.compare(a, b));

    let mut last_op: HashMap<FactKey, bool> = HashMap::new();
    let mut result = Vec::with_capacity(flakes.len());

    for flake in flakes {
        let key = fact_key(&flake);
        if last_op.get(&key) == Some(&flake.op) {
            continue; // Skip redundant same-op
        }
        last_op.insert(key, flake.op);
        result.push(flake);
    }

    result
}

/// Merge and dedup flakes in one operation
///
/// Merges novelty into existing, then applies leaf-local dedup.
pub fn merge_and_dedup(
    existing: &[Flake],
    novelty: impl Iterator<Item = Flake>,
    index_type: IndexType,
) -> Vec<Flake> {
    let merged = merge_leaf_flakes(existing, novelty, index_type);
    let deduped = dedup_leaf_local(merged);

    // Re-sort by target index after dedup (which sorted by SPOT)
    let mut result = deduped;
    result.sort_by(|a, b| index_type.compare(a, b));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(s: i32, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(1, "p1"),
            FlakeValue::Long(100),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    #[test]
    fn test_merge_sorted_empty() {
        let result = merge_sorted(&[], &[], |a: &Flake, b: &Flake| {
            IndexType::Spot.compare(a, b)
        });
        assert!(result.is_empty());
    }

    #[test]
    fn test_merge_sorted_one_empty() {
        let a = vec![make_flake(1, 1, true), make_flake(3, 1, true)];

        let result = merge_sorted(&a, &[], |a, b| IndexType::Spot.compare(a, b));
        assert_eq!(result.len(), 2);

        let result = merge_sorted(&[], &a, |a, b| IndexType::Spot.compare(a, b));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merge_sorted_interleaved() {
        let a = vec![make_flake(1, 1, true), make_flake(3, 1, true)];
        let b = vec![make_flake(2, 1, true), make_flake(4, 1, true)];

        let result = merge_sorted(&a, &b, |a, b| IndexType::Spot.compare(a, b));
        assert_eq!(result.len(), 4);

        // Verify sorted order
        for i in 0..result.len() - 1 {
            assert!(
                IndexType::Spot.compare(&result[i], &result[i + 1]) != Ordering::Greater,
                "Not sorted at position {}",
                i
            );
        }
    }

    #[test]
    fn test_merge_leaf_flakes() {
        let existing = vec![make_flake(1, 1, true), make_flake(3, 1, true)];
        let novelty = vec![make_flake(2, 2, true)];

        let result = merge_leaf_flakes(&existing, novelty.into_iter(), IndexType::Spot);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_dedup_leaf_local_removes_redundant() {
        // Same fact key, same op at different t - should keep only earliest
        let flakes = vec![
            make_flake(1, 1, true),
            make_flake(1, 2, true), // Redundant: same fact, same op
        ];

        let result = dedup_leaf_local(flakes);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].t, 1); // Keeps earliest
    }

    #[test]
    fn test_dedup_leaf_local_keeps_state_transitions() {
        // Same fact key, alternating ops - should keep all
        let flakes = vec![
            make_flake(1, 1, true),  // assert
            make_flake(1, 2, false), // retract
            make_flake(1, 3, true),  // assert again
        ];

        let result = dedup_leaf_local(flakes);
        assert_eq!(result.len(), 3); // All kept - state transitions
    }

    #[test]
    fn test_dedup_leaf_local_different_facts() {
        // Different facts with same op - all kept
        let flakes = vec![
            make_flake(1, 1, true),
            make_flake(2, 1, true),
            make_flake(3, 1, true),
        ];

        let result = dedup_leaf_local(flakes);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_merge_and_dedup() {
        let existing = vec![make_flake(1, 1, true)];
        let novelty = vec![
            make_flake(1, 2, true), // Redundant with existing
            make_flake(2, 2, true), // New fact
        ];

        let result = merge_and_dedup(&existing, novelty.into_iter(), IndexType::Spot);

        // Should have 2 flakes: original at t=1 and new fact at t=2
        assert_eq!(result.len(), 2);
    }
}
