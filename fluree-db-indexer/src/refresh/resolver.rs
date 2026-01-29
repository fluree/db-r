//! Node resolution from storage
//!
//! Loads leaf and branch nodes from storage for incremental refresh.

use crate::error::{IndexerError, Result};
use fluree_db_core::db::EMPTY_NODE_ID;
use fluree_db_core::index::ChildRef;
use fluree_db_core::serde::json::{parse_branch_node, parse_leaf_node};
use fluree_db_core::{Flake, Storage};
use std::sync::Arc;

/// Resolved node content
#[derive(Debug, Clone)]
pub enum ResolvedContent {
    /// Leaf node containing flakes
    Leaf(Arc<[Flake]>),
    /// Branch node containing child references
    Branch(Arc<[ChildRef]>),
}

/// Load a node from storage
///
/// # Arguments
///
/// * `storage` - Storage backend to read from
/// * `child` - Child reference containing the node address and type
///
/// # Returns
///
/// The resolved node content (either leaf flakes or branch children).
///
/// # Special Cases
///
/// The EMPTY_NODE_ID ("empty") is a sentinel for empty genesis roots.
/// When encountered, it returns an empty leaf without reading from storage.
/// Note: This always returns a leaf regardless of `child.leaf` flag, because
/// EMPTY_NODE_ID represents an empty collection of flakes, not an empty branch.
/// The `child.leaf` flag is irrelevant for the empty sentinel.
pub async fn resolve_node<S: Storage>(storage: &S, child: &ChildRef) -> Result<ResolvedContent> {
    // Handle the EMPTY_NODE_ID sentinel - genesis roots use "empty" to represent
    // an empty leaf without actually storing anything. We always return an empty
    // leaf here regardless of child.leaf, because the "empty" sentinel represents
    // an empty collection of flakes (the base case for any index tree).
    if child.id == EMPTY_NODE_ID {
        return Ok(ResolvedContent::Leaf(Arc::from(vec![])));
    }

    let bytes = storage
        .read_bytes(&child.id)
        .await
        .map_err(|e| IndexerError::StorageRead(e.to_string()))?;

    if child.leaf {
        let flakes = parse_leaf_node(bytes)?;
        Ok(ResolvedContent::Leaf(Arc::from(flakes)))
    } else {
        let children = parse_branch_node(bytes)?;
        Ok(ResolvedContent::Branch(Arc::from(children)))
    }
}

/// Resolved node with metadata
#[derive(Debug, Clone)]
pub struct ResolvedNode {
    /// The original child reference
    pub child_ref: ChildRef,
    /// The resolved content
    pub content: ResolvedContent,
}

/// Test helper methods for ResolvedNode
#[cfg(test)]
impl ResolvedNode {
    /// Check if this is a leaf node
    pub fn is_leaf(&self) -> bool {
        matches!(self.content, ResolvedContent::Leaf(_))
    }

    /// Get leaf flakes (panics if not a leaf)
    pub fn as_leaf(&self) -> &Arc<[Flake]> {
        match &self.content {
            ResolvedContent::Leaf(flakes) => flakes,
            ResolvedContent::Branch(_) => panic!("Expected leaf, got branch"),
        }
    }

    /// Get branch children (panics if not a branch)
    pub fn as_branch(&self) -> &Arc<[ChildRef]> {
        match &self.content {
            ResolvedContent::Branch(children) => children,
            ResolvedContent::Leaf(_) => panic!("Expected branch, got leaf"),
        }
    }
}

/// Load a node and wrap with metadata
pub async fn load_node<S: Storage>(storage: &S, child: ChildRef) -> Result<ResolvedNode> {
    let content = resolve_node(storage, &child).await?;
    Ok(ResolvedNode { child_ref: child, content })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_child_ref(id: &str, leaf: bool) -> ChildRef {
        ChildRef {
            id: id.to_string(),
            leaf,
            first: None,
            rhs: None,
            size: 10,
            bytes: None,
            leftmost: true,
        }
    }

    #[test]
    fn test_resolved_node_is_leaf() {
        let node = ResolvedNode {
            child_ref: make_child_ref("test", true),
            content: ResolvedContent::Leaf(Arc::from(vec![])),
        };
        assert!(node.is_leaf());

        let node = ResolvedNode {
            child_ref: make_child_ref("test", false),
            content: ResolvedContent::Branch(Arc::from(vec![])),
        };
        assert!(!node.is_leaf());
    }

    #[test]
    fn test_resolved_node_as_leaf() {
        use fluree_db_core::{FlakeValue, Sid};

        let flake = Flake::new(
            Sid::new(1, "s1"),
            Sid::new(1, "p1"),
            FlakeValue::Long(100),
            Sid::new(2, "long"),
            1,
            true,
            None,
        );

        let node = ResolvedNode {
            child_ref: make_child_ref("test", true),
            content: ResolvedContent::Leaf(Arc::from(vec![flake])),
        };

        let flakes = node.as_leaf();
        assert_eq!(flakes.len(), 1);
    }

    #[test]
    fn test_resolved_node_as_branch() {
        let child = make_child_ref("child1", true);

        let node = ResolvedNode {
            child_ref: make_child_ref("test", false),
            content: ResolvedContent::Branch(Arc::from(vec![child])),
        };

        let children = node.as_branch();
        assert_eq!(children.len(), 1);
    }

    #[test]
    #[should_panic(expected = "Expected leaf, got branch")]
    fn test_as_leaf_on_branch_panics() {
        let node = ResolvedNode {
            child_ref: make_child_ref("test", false),
            content: ResolvedContent::Branch(Arc::from(vec![])),
        };
        let _ = node.as_leaf();
    }

    #[test]
    #[should_panic(expected = "Expected branch, got leaf")]
    fn test_as_branch_on_leaf_panics() {
        let node = ResolvedNode {
            child_ref: make_child_ref("test", true),
            content: ResolvedContent::Leaf(Arc::from(vec![])),
        };
        let _ = node.as_branch();
    }
}
