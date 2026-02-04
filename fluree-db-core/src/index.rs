//! Index node types
//!
//! The Fluree index is a tree structure with:
//! - **Branch nodes**: contain references to child nodes
//! - **Leaf nodes**: contain actual flakes
//!
//! Both node types have boundary information (`first` and `rhs`) for
//! efficient range filtering during tree traversal.

use crate::comparator::IndexType;
use crate::flake::Flake;
use std::sync::Arc;

/// Unique identifier for index nodes (content-addressed hash)
pub type NodeId = String;

/// Reference to a child node with boundary information
///
/// This struct is created by custom JSON parsing in `serde/json.rs`,
/// not via serde derives (because Flake uses custom serialization).
#[derive(Clone, Debug)]
pub struct ChildRef {
    /// Storage address/identifier for the child node
    pub id: NodeId,
    /// Whether this is a leaf node
    pub leaf: bool,
    /// First (minimum) flake boundary - None for leftmost
    pub first: Option<Flake>,
    /// Right-hand side (maximum) flake boundary - None for rightmost
    pub rhs: Option<Flake>,
    /// Subtree flake count
    ///
    /// For **leaf nodes**: the number of flakes in this leaf.
    /// For **branch nodes**: the sum of all children's sizes (total flakes in subtree).
    ///
    /// Note: Despite the generic name "size", this is a *flake count*, not bytes.
    /// Byte-size statistics are computed separately using a fast flake-size estimator
    /// (see `Flake::size_estimate_bytes` / `size_flakes_estimate`).
    pub size: u64,
    /// Serialized byte size of this node
    ///
    /// For **leaf nodes**: the exact serialized JSON byte size.
    /// For **branch nodes**: the exact serialized JSON byte size of this branch.
    ///
    /// This is populated at indexing time and persisted for accurate cache eviction.
    /// `None` for indexes created before this field was added (backward compatibility).
    pub bytes: Option<u64>,
    /// Whether this is the leftmost child
    pub leftmost: bool,
}

impl ChildRef {
    /// Create a new child reference
    pub fn new(id: NodeId, leaf: bool) -> Self {
        Self {
            id,
            leaf,
            first: None,
            rhs: None,
            size: 0,
            bytes: None,
            leftmost: false,
        }
    }

    /// Check if this child's range could intersect with the given bounds
    pub fn intersects_range(
        &self,
        start: &Flake,
        end: &Flake,
        cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
    ) -> bool {
        // Check if child is entirely above the range
        if let Some(ref first) = self.first {
            if !self.leftmost && cmp(first, end) == std::cmp::Ordering::Greater {
                return false;
            }
        }

        // Check if child is entirely below the range
        if let Some(ref rhs) = self.rhs {
            if cmp(rhs, start) == std::cmp::Ordering::Less {
                return false;
            }
        }

        true
    }
}

/// An unresolved index node (just metadata, no content)
#[derive(Clone, Debug)]
pub struct IndexNode {
    /// Storage address/identifier
    pub id: NodeId,
    /// Whether this is a leaf node
    pub leaf: bool,
    /// Index type this node belongs to
    pub index_type: IndexType,
    /// Ledger alias
    pub alias: String,
    /// First flake boundary
    pub first: Option<Flake>,
    /// Right-hand side boundary
    pub rhs: Option<Flake>,
    /// Whether this is the leftmost node at its level
    pub leftmost: bool,
    /// Transaction time this node was created
    pub t: i64,
    /// Subtree flake count (see [`ChildRef::size`] for details)
    pub size: u64,
    /// Serialized byte size (see [`ChildRef::bytes`] for details)
    pub bytes: Option<u64>,
}

impl IndexNode {
    /// Create a new branch node reference
    pub fn branch(id: NodeId, index_type: IndexType, alias: String) -> Self {
        Self {
            id,
            leaf: false,
            index_type,
            alias,
            first: None,
            rhs: None,
            leftmost: false,
            t: 0,
            size: 0,
            bytes: None,
        }
    }

    /// Create a new leaf node reference
    pub fn leaf(id: NodeId, index_type: IndexType, alias: String) -> Self {
        Self {
            id,
            leaf: true,
            index_type,
            alias,
            first: None,
            rhs: None,
            leftmost: false,
            t: 0,
            size: 0,
            bytes: None,
        }
    }

    /// Create from a child reference
    pub fn from_child_ref(child: &ChildRef, index_type: IndexType, alias: String) -> Self {
        Self {
            id: child.id.clone(),
            leaf: child.leaf,
            index_type,
            alias,
            first: child.first.clone(),
            rhs: child.rhs.clone(),
            leftmost: child.leftmost,
            t: 0,
            size: child.size,
            bytes: child.bytes,
        }
    }

    /// Check if this node's range could intersect with the given bounds
    pub fn intersects_range(&self, start: &Flake, end: &Flake) -> bool {
        let cmp = self.index_type.comparator();

        // Check if node is entirely above the range
        if let Some(ref first) = self.first {
            if !self.leftmost && cmp(first, end) == std::cmp::Ordering::Greater {
                return false;
            }
        }

        // Check if node is entirely below the range
        if let Some(ref rhs) = self.rhs {
            if cmp(rhs, start) == std::cmp::Ordering::Less {
                return false;
            }
        }

        true
    }
}

/// A resolved index node with content loaded
#[derive(Clone, Debug)]
pub enum ResolvedNode {
    /// Branch node with children
    Branch {
        node: IndexNode,
        children: Arc<[ChildRef]>,
    },
    /// Leaf node with flakes
    Leaf {
        node: IndexNode,
        flakes: Arc<[Flake]>,
    },
}

impl ResolvedNode {
    /// Create a resolved branch node
    pub fn branch(node: IndexNode, children: Arc<[ChildRef]>) -> Self {
        ResolvedNode::Branch { node, children }
    }

    /// Create a resolved leaf node
    pub fn leaf(node: IndexNode, flakes: Arc<[Flake]>) -> Self {
        ResolvedNode::Leaf { node, flakes }
    }

    /// Check if this is a leaf node
    pub fn is_leaf(&self) -> bool {
        matches!(self, ResolvedNode::Leaf { .. })
    }

    /// Check if this is a branch node
    pub fn is_branch(&self) -> bool {
        matches!(self, ResolvedNode::Branch { .. })
    }

    /// Get the underlying index node
    pub fn node(&self) -> &IndexNode {
        match self {
            ResolvedNode::Branch { node, .. } => node,
            ResolvedNode::Leaf { node, .. } => node,
        }
    }

    /// Get children (for branch nodes)
    pub fn children(&self) -> Option<&[ChildRef]> {
        match self {
            ResolvedNode::Branch { children, .. } => Some(children),
            ResolvedNode::Leaf { .. } => None,
        }
    }

    /// Get flakes (for leaf nodes)
    pub fn flakes(&self) -> Option<&[Flake]> {
        match self {
            ResolvedNode::Leaf { flakes, .. } => Some(flakes),
            ResolvedNode::Branch { .. } => None,
        }
    }

    /// Get the byte size of this node for cache eviction
    ///
    /// Uses the persisted `bytes` field when available (O(1)), falling back
    /// to estimation for indexes created before this field was added.
    pub fn size_bytes(&self) -> usize {
        match self {
            ResolvedNode::Branch { node, children } => {
                // Use persisted bytes if available, otherwise estimate
                node.bytes.map(|b| b as usize).unwrap_or_else(|| {
                    node.size as usize + children.len() * 200 // rough estimate per child
                })
            }
            ResolvedNode::Leaf { node, flakes } => {
                // Use persisted bytes if available, otherwise calculate
                node.bytes.map(|b| b as usize).unwrap_or_else(|| {
                    node.size as usize + flakes.iter().map(|f| f.size_bytes()).sum::<usize>()
                })
            }
        }
    }
}

/// Empty root node for a new index
pub fn empty_root(index_type: IndexType, alias: String) -> IndexNode {
    IndexNode {
        id: "empty".to_string(),
        leaf: true,
        index_type,
        alias,
        first: Some(Flake::max_spot()),
        rhs: None,
        leftmost: true,
        t: 0,
        size: 0,
        bytes: Some(0), // Empty node has no serialized content
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sid::Sid;
    use crate::value::FlakeValue;

    fn make_flake(s: i32, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, "test"),
            Sid::new(1, "p"),
            FlakeValue::Long(1),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_child_ref_intersects() {
        let child = ChildRef {
            id: "test".to_string(),
            leaf: true,
            first: Some(make_flake(10, 1)),
            rhs: Some(make_flake(20, 1)),
            size: 100,
            bytes: None,
            leftmost: false,
        };

        let cmp = IndexType::Spot.comparator();

        // Range within child bounds
        assert!(child.intersects_range(&make_flake(12, 1), &make_flake(18, 1), cmp));

        // Range overlapping start
        assert!(child.intersects_range(&make_flake(5, 1), &make_flake(15, 1), cmp));

        // Range overlapping end
        assert!(child.intersects_range(&make_flake(15, 1), &make_flake(25, 1), cmp));

        // Range before child
        assert!(!child.intersects_range(&make_flake(1, 1), &make_flake(5, 1), cmp));

        // Range after child
        assert!(!child.intersects_range(&make_flake(25, 1), &make_flake(30, 1), cmp));
    }

    #[test]
    fn test_index_node_from_child_ref() {
        let child = ChildRef {
            id: "abc123".to_string(),
            leaf: true,
            first: Some(make_flake(1, 1)),
            rhs: Some(make_flake(10, 1)),
            size: 1000,
            bytes: Some(5000),
            leftmost: true,
        };

        let node = IndexNode::from_child_ref(&child, IndexType::Spot, "test".to_string());

        assert_eq!(node.id, "abc123");
        assert!(node.leaf);
        assert!(node.leftmost);
        assert_eq!(node.size, 1000);
    }
}
