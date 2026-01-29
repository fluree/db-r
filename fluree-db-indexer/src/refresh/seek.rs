//! Binary search for finding children in branch nodes
//!
//! The `floor_child` function finds which child's range contains a target flake.

#[cfg(test)]
use fluree_db_core::index::ChildRef;
#[cfg(test)]
use fluree_db_core::{Flake, IndexType};
#[cfg(test)]
use std::cmp::Ordering;

/// Find child whose range contains target (greatest child.first <= target)
///
/// Since leftmost nodes have `first = None` (treated as -∞), they always qualify.
///
/// # INVARIANT
///
/// Children must be sorted by boundary (first None, then ascending first).
/// This is guaranteed by:
/// 1. How branches are constructed (children added in order)
/// 2. Branch serialization (`serialize_branch_node`) preserves child order
/// 3. Branch parsing (`parse_branch_node`) preserves child order
///
/// A debug assertion verifies this invariant.
///
/// # Arguments
///
/// * `children` - Slice of child references from a branch node
/// * `target` - The flake to locate
/// * `index_type` - Which index comparator to use
///
/// # Returns
///
/// Index of the child whose range contains target.
#[cfg(test)]
pub fn floor_child(children: &[ChildRef], target: &Flake, index_type: IndexType) -> usize {
    debug_assert!(!children.is_empty(), "floor_child called with empty children");

    // Debug-only invariant check: children are sorted by boundary
    #[cfg(debug_assertions)]
    verify_children_sorted(children, index_type);

    // Binary search: find rightmost child where first < target (exclusive left boundary)
    // first = None is treated as -∞, so always qualifies
    // With exclusive left boundary semantics, a child with first=X covers (X, rhs],
    // so target must be strictly greater than first to be in that child's range.
    let idx = children.partition_point(|child| match &child.first {
        None => true, // -∞ < anything (leftmost always qualifies)
        Some(first) => index_type.compare(first, target) == Ordering::Less, // first < target
    });

    // partition_point returns first element where first >= target, we want last where first < target
    idx.saturating_sub(1)
}

/// Find all children whose ranges intersect with the given novelty bounds
///
/// Returns indices of children that might contain flakes in the given range.
///
/// # Arguments
///
/// * `children` - Slice of child references from a branch node
/// * `first` - Left boundary of novelty range (exclusive if not leftmost)
/// * `rhs` - Right boundary of novelty range (inclusive)
/// * `index_type` - Which index comparator to use
///
/// # Returns
///
/// Vector of child indices that intersect with the novelty range.
#[cfg(test)]
pub fn find_affected_children(
    children: &[ChildRef],
    first: Option<&Flake>,
    rhs: Option<&Flake>,
    index_type: IndexType,
) -> Vec<usize> {
    let mut affected = Vec::new();

    for (i, child) in children.iter().enumerate() {
        if child_intersects_range(child, first, rhs, index_type) {
            affected.push(i);
        }
    }

    affected
}

/// Check if a child's range intersects with the given novelty range
#[cfg(test)]
fn child_intersects_range(
    child: &ChildRef,
    range_first: Option<&Flake>,
    range_rhs: Option<&Flake>,
    index_type: IndexType,
) -> bool {
    // Child range: (child.first, child.rhs] (exclusive left, inclusive right)
    // For leftmost: (-∞, child.rhs]
    // For rightmost: (child.first, +∞)

    // Check if child range starts after novelty range ends
    // If child.first > range_rhs, no intersection
    if let (Some(child_first), Some(range_end)) = (&child.first, range_rhs) {
        if !child.leftmost && index_type.compare(child_first, range_end) == Ordering::Greater {
            return false;
        }
    }

    // Check if child range ends before novelty range starts
    // If child.rhs < range_first (exclusive), no intersection
    if let (Some(child_rhs), Some(range_start)) = (&child.rhs, range_first) {
        // range_first is exclusive, so we need child.rhs >= range_first
        if index_type.compare(child_rhs, range_start) != Ordering::Greater {
            return false;
        }
    }

    true
}

/// Verify that children are sorted by boundary (debug only)
#[cfg(all(test, debug_assertions))]
fn verify_children_sorted(children: &[ChildRef], index_type: IndexType) {
    for window in children.windows(2) {
        let a_first = &window[0].first;
        let b_first = &window[1].first;
        match (a_first, b_first) {
            (None, None) => panic!("Multiple leftmost children"),
            (None, Some(_)) => {} // OK: None (leftmost) comes first
            (Some(_), None) => panic!("Children not sorted: non-leftmost before leftmost"),
            (Some(a), Some(b)) => {
                assert!(
                    index_type.compare(a, b) != Ordering::Greater,
                    "Children not sorted by first"
                );
            }
        }
    }
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

    fn make_child_ref(first: Option<Flake>, rhs: Option<Flake>, leftmost: bool) -> ChildRef {
        ChildRef {
            id: "test".to_string(),
            leaf: true,
            first,
            rhs,
            size: 10,
            bytes: None,
            leftmost,
        }
    }

    #[test]
    fn test_floor_child_single() {
        let children = vec![make_child_ref(None, None, true)];
        let target = make_flake(5, 1);

        let idx = floor_child(&children, &target, IndexType::Spot);
        assert_eq!(idx, 0);
    }

    #[test]
    fn test_floor_child_leftmost() {
        // Leftmost child has first=None (-∞)
        let children = vec![
            make_child_ref(None, Some(make_flake(3, 1)), true),
            make_child_ref(Some(make_flake(3, 1)), Some(make_flake(6, 1)), false),
            make_child_ref(Some(make_flake(6, 1)), None, false),
        ];

        // Target in first child range
        let target = make_flake(2, 1);
        assert_eq!(floor_child(&children, &target, IndexType::Spot), 0);

        // Target at boundary (should go to first child since boundary is exclusive)
        let target = make_flake(3, 1);
        assert_eq!(floor_child(&children, &target, IndexType::Spot), 0);
    }

    #[test]
    fn test_floor_child_middle() {
        let children = vec![
            make_child_ref(None, Some(make_flake(3, 1)), true),
            make_child_ref(Some(make_flake(3, 1)), Some(make_flake(6, 1)), false),
            make_child_ref(Some(make_flake(6, 1)), None, false),
        ];

        // Target in second child range (> 3)
        let target = make_flake(5, 1);
        assert_eq!(floor_child(&children, &target, IndexType::Spot), 1);
    }

    #[test]
    fn test_floor_child_rightmost() {
        let children = vec![
            make_child_ref(None, Some(make_flake(3, 1)), true),
            make_child_ref(Some(make_flake(3, 1)), Some(make_flake(6, 1)), false),
            make_child_ref(Some(make_flake(6, 1)), None, false),
        ];

        // Target in last child range
        let target = make_flake(10, 1);
        assert_eq!(floor_child(&children, &target, IndexType::Spot), 2);
    }

    #[test]
    fn test_find_affected_children_full_range() {
        let children = vec![
            make_child_ref(None, Some(make_flake(3, 1)), true),
            make_child_ref(Some(make_flake(3, 1)), Some(make_flake(6, 1)), false),
            make_child_ref(Some(make_flake(6, 1)), None, false),
        ];

        // Full range should affect all children
        let affected = find_affected_children(&children, None, None, IndexType::Spot);
        assert_eq!(affected, vec![0, 1, 2]);
    }

    #[test]
    fn test_find_affected_children_partial() {
        let children = vec![
            make_child_ref(None, Some(make_flake(3, 1)), true),
            make_child_ref(Some(make_flake(3, 1)), Some(make_flake(6, 1)), false),
            make_child_ref(Some(make_flake(6, 1)), None, false),
        ];

        // Range from 4 to 5 should only affect middle child
        let first = make_flake(4, 1);
        let rhs = make_flake(5, 1);
        let affected = find_affected_children(&children, Some(&first), Some(&rhs), IndexType::Spot);
        assert_eq!(affected, vec![1]);
    }
}
