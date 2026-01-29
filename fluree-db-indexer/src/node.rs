//! Index node building (leaves and branches) (TEST-ONLY)
//!
//! **Note**: This module is test-only scaffolding (`#[cfg(test)]`). It provides
//! tree construction from flakes for test setup. Production code uses the
//! refresh-based pipeline which modifies existing trees incrementally.
//!
//! Handles splitting flakes into leaves and building branch nodes.
//!
//! ## Boundary Semantics
//!
//! - **Leftmost nodes**: No left boundary (`first: None` or `first` is min sentinel)
//! - **Non-leftmost**: Left boundary is EXCLUSIVE (`> first`)
//! - **RHS**: INCLUSIVE (`<= rhs`)
//!
//! These must match the semantics used in range queries and novelty overlay.

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::writer::IndexWriter;
use fluree_db_core::index::ChildRef;
use fluree_db_core::serde::json::{serialize_branch_node, serialize_leaf_node};
use fluree_db_core::{ContentAddressedWrite, Flake, IndexType};

/// Build a complete index tree for one index type
///
/// Sorts flakes by the given comparator, splits into leaves, and builds
/// branch nodes recursively until we have a single root.
pub async fn build_tree<S: ContentAddressedWrite>(
    flakes: &[Flake],
    index_type: IndexType,
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<ChildRef> {
    if flakes.is_empty() {
        // Return empty leaf - leftmost with no boundaries
        return Ok(ChildRef {
            id: "empty".to_string(),
            leaf: true,
            first: None, // Leftmost has no left boundary
            rhs: None,   // Empty has no right boundary
            size: 0,
            bytes: Some(0), // Empty node has no serialized content
            leftmost: true,
        });
    }

    // Sort flakes by index comparator
    let mut sorted: Vec<Flake> = flakes.to_vec();
    sorted.sort_by(|a, b| index_type.compare(a, b));

    // Split into leaves
    let leaves = build_leaves(&sorted, config, writer).await?;

    // Build branches recursively until we have a single root
    let mut current_level = leaves;
    while current_level.len() > 1 {
        current_level = build_branch_level(&current_level, config, writer).await?;
    }

    // Return the single root
    Ok(current_level.into_iter().next().unwrap())
}

/// Build leaf nodes from sorted flakes
async fn build_leaves<S: ContentAddressedWrite>(
    sorted_flakes: &[Flake],
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<Vec<ChildRef>> {
    let mut leaves = Vec::new();
    let mut start = 0;
    let total = sorted_flakes.len();
    let target_bytes = config
        .leaf_target_bytes
        .min(config.leaf_max_bytes)
        .max(1);

    while start < total {
        // Determine leaf size by estimated bytes
        let mut end = start;
        let mut bytes: u64 = 0;
        while end < total {
            let flake_bytes = sorted_flakes[end].size_estimate_bytes();
            if end == start {
                // Always include at least one flake per leaf
                bytes = flake_bytes;
                end += 1;
                if bytes >= target_bytes {
                    break;
                }
                continue;
            }

            if bytes + flake_bytes > target_bytes {
                break;
            }

            bytes += flake_bytes;
            end += 1;
            if bytes >= target_bytes {
                break;
            }
        }

        let leaf_flakes = &sorted_flakes[start..end];

        // Create leaf
        let is_leftmost = start == 0;
        let first = if is_leftmost {
            // Leftmost leaf has no left boundary (includes all values from -∞)
            None
        } else {
            // Non-leftmost uses the actual first flake (exclusive boundary)
            Some(leaf_flakes[0].clone())
        };

        let rhs = if end < total {
            // RHS is the first flake of the next leaf (inclusive boundary)
            Some(sorted_flakes[end].clone())
        } else {
            // Rightmost leaf has no RHS
            None
        };

        // Serialize and write
        let serialized = serialize_leaf_node(leaf_flakes)?;
        let serialized_len = serialized.len() as u64;
        let address = writer.write_leaf(&serialized).await?;

        leaves.push(ChildRef {
            id: address,
            leaf: true,
            first,
            rhs,
            size: leaf_flakes.len() as u64,
            bytes: Some(serialized_len),
            leftmost: is_leftmost,
        });

        start = end;
    }

    Ok(leaves)
}

/// Build one level of branch nodes from children
async fn build_branch_level<S: ContentAddressedWrite>(
    children: &[ChildRef],
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<Vec<ChildRef>> {
    let mut branches = Vec::new();
    let mut start = 0;
    let total = children.len();

    while start < total {
        let remaining = total - start;
        let size = std::cmp::min(config.branch_target_children, remaining);

        let end = start + size;
        let branch_children = &children[start..end];

        let is_leftmost = start == 0;

        // First boundary is from first child
        let first = branch_children[0].first.clone();

        // RHS is from last child
        let rhs = branch_children.last().and_then(|c| c.rhs.clone());

        // Total size is sum of children sizes
        let total_size: u64 = branch_children.iter().map(|c| c.size).sum();

        // Serialize and write
        let serialized = serialize_branch_node(branch_children)?;
        let serialized_len = serialized.len() as u64;
        let address = writer.write_branch(&serialized).await?;

        branches.push(ChildRef {
            id: address,
            leaf: false,
            first,
            rhs,
            size: total_size,
            bytes: Some(serialized_len),
            leftmost: is_leftmost,
        });

        start = end;
    }

    Ok(branches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::serde::json::parse_branch_node;
    use fluree_db_core::prelude::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(name: &str, t: i64) -> Flake {
        Flake::new(
            Sid::new(1, name),
            Sid::new(2, "p"),
            FlakeValue::Long(t),
            Sid::new(3, "long"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_build_tree_empty() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:main",
            fluree_db_core::ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );
        let config = IndexerConfig::small();

        let root = build_tree(&[], IndexType::Spot, &config, &mut writer)
            .await
            .unwrap();

        assert!(root.leaf);
        assert_eq!(root.id, "empty");
        assert_eq!(root.size, 0);
    }

    #[tokio::test]
    async fn test_build_tree_single_leaf() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:main",
            fluree_db_core::ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );
        let config = IndexerConfig::small();

        let flakes = vec![
            make_flake("a", 1),
            make_flake("b", 2),
            make_flake("c", 3),
        ];

        let root = build_tree(&flakes, IndexType::Spot, &config, &mut writer)
            .await
            .unwrap();

        assert!(root.leaf);
        assert_eq!(root.size, 3);
        assert!(root.leftmost);
    }

    #[tokio::test]
    async fn test_build_tree_multiple_leaves() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:main",
            fluree_db_core::ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );
        // Very small config to force splitting
        let flake_bytes = make_flake("s0", 0).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 2, flake_bytes * 3, 2, 3);

        // Create enough flakes to require multiple leaves
        let flakes: Vec<Flake> = (0..10).map(|i| make_flake(&format!("s{}", i), i)).collect();

        let root = build_tree(&flakes, IndexType::Spot, &config, &mut writer)
            .await
            .unwrap();

        // Should have created a branch
        assert!(!root.leaf);
        assert_eq!(root.size, 10);
    }

    #[tokio::test]
    async fn test_build_leaves_boundaries() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:main",
            fluree_db_core::ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );
        let flake_bytes = make_flake("s0", 0).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 3, flake_bytes * 5, 10, 20);

        let flakes: Vec<Flake> = (0..6).map(|i| make_flake(&format!("s{}", i), i)).collect();

        let leaves = build_leaves(&flakes, &config, &mut writer).await.unwrap();

        assert_eq!(leaves.len(), 2);

        // First leaf is leftmost - no left boundary
        assert!(leaves[0].leftmost);
        assert!(leaves[0].first.is_none()); // Leftmost has no left boundary
        assert!(leaves[0].rhs.is_some()); // Has RHS because not last

        // Second leaf is not leftmost - has left boundary
        assert!(!leaves[1].leftmost);
        assert!(leaves[1].first.is_some()); // Non-leftmost has exclusive left boundary
        assert!(leaves[1].rhs.is_none()); // No RHS because last
    }

    async fn assert_boundary_invariants(storage: &MemoryStorage, root: &ChildRef) {
        let mut stack = vec![root.clone()];

        while let Some(node) = stack.pop() {
            if node.leaf {
                continue;
            }

            let bytes = storage.read_bytes(&node.id).await.unwrap();
            let children = parse_branch_node(bytes).unwrap();

            for (i, child) in children.iter().enumerate() {
                if child.leftmost {
                    assert!(child.first.is_none(), "leftmost child must have first=None");
                } else {
                    assert!(child.first.is_some(), "non-leftmost child must have first");
                }

                if i + 1 < children.len() {
                    assert_eq!(
                        child.rhs,
                        children[i + 1].first,
                        "child rhs must equal next child first"
                    );
                } else if node.rhs.is_none() {
                    assert!(child.rhs.is_none(), "rightmost child must have rhs=None");
                } else {
                    assert_eq!(child.rhs, node.rhs, "rightmost child must inherit parent rhs");
                }
            }

            stack.extend(children);
        }
    }

    #[tokio::test]
    async fn test_build_tree_boundary_invariants() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:main",
            fluree_db_core::ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );
        let flake_bytes = make_flake("s0", 0).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 2, flake_bytes * 3, 2, 3);

        let flakes: Vec<Flake> = (0..12).map(|i| make_flake(&format!("s{}", i), i)).collect();
        let root = build_tree(&flakes, IndexType::Spot, &config, &mut writer)
            .await
            .unwrap();

        assert_boundary_invariants(&storage, &root).await;
    }
}
