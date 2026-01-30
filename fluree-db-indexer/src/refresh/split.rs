//! Splitting for overflow nodes
//!
//! Handles splitting leaves and branches when they exceed max size.

use crate::config::IndexerConfig;
use crate::error::Result;
use crate::writer::IndexWriter;
use fluree_db_core::flake::size_flakes_estimate;
use fluree_db_core::index::ChildRef;
use fluree_db_core::serde::json::{serialize_branch_node, serialize_leaf_node};
use fluree_db_core::{ContentAddressedWrite, Flake};

/// Write a single leaf and return its ChildRef
async fn write_leaf<S: ContentAddressedWrite>(
    flakes: &[Flake],
    leftmost: bool,
    rhs: Option<Flake>,
    writer: &mut IndexWriter<'_, S>,
) -> Result<ChildRef> {
    let first = if leftmost {
        None // Leftmost has no left boundary
    } else {
        flakes.first().cloned()
    };

    let serialized = serialize_leaf_node(flakes)?;
    let serialized_len = serialized.len() as u64;
    let address = writer.write_leaf(&serialized).await?;

    Ok(ChildRef {
        id: address,
        leaf: true,
        first,
        rhs,
        size: flakes.len() as u64,
        bytes: Some(serialized_len),
        leftmost,
    })
}

/// Split flakes into multiple leaves if count exceeds max
///
/// # Arguments
///
/// * `flakes` - Sorted flakes for the leaf
/// * `original_leftmost` - Whether the original leaf was leftmost
/// * `original_has_rhs` - Whether the original leaf had an rhs (not rightmost)
/// * `config` - Indexer configuration with size thresholds
/// * `writer` - Index writer for persisting nodes
///
/// # Returns
///
/// Vector of ChildRefs, one per resulting leaf (1 if no split needed).
///
/// # Leftmost Flag Correction
///
/// When splitting, only the FIRST output sibling inherits the leftmost status.
/// Subsequent siblings are NOT leftmost.
pub async fn split_leaf_if_needed<S: ContentAddressedWrite>(
    flakes: Vec<Flake>,
    original_leftmost: bool,
    original_rhs: Option<Flake>,
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<Vec<ChildRef>> {
    let total_bytes = size_flakes_estimate(&flakes);
    if total_bytes <= config.leaf_max_bytes {
        // No split needed - single leaf
        return Ok(vec![
            write_leaf(&flakes, original_leftmost, original_rhs, writer).await?,
        ]);
    }

    // Split into chunks by target bytes
    let mut leaves = Vec::new();
    let target_bytes = config.leaf_target_bytes.min(config.leaf_max_bytes).max(1);
    let mut start = 0;
    let total = flakes.len();
    let mut chunks: Vec<&[Flake]> = Vec::new();

    while start < total {
        let mut end = start;
        let mut bytes: u64 = 0;
        while end < total {
            let flake_bytes = flakes[end].size_estimate_bytes();
            if end == start {
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

        chunks.push(&flakes[start..end]);
        start = end;
    }

    let num_chunks = chunks.len();
    for i in 0..num_chunks {
        let chunk = chunks[i];
        let is_first = i == 0;
        let is_last = i == num_chunks - 1;

        // Only first chunk inherits leftmost
        let is_leftmost = is_first && original_leftmost;
        // Consecutive leaf boundaries: rhs of this leaf equals first of next.
        // Preserve original rhs on the final chunk when present.
        let rhs = if is_last {
            original_rhs.clone()
        } else {
            chunks[i + 1].first().cloned()
        };

        leaves.push(write_leaf(chunk, is_leftmost, rhs, writer).await?);
    }

    Ok(leaves)
}

/// Write a single branch and return its ChildRef
async fn write_branch<S: ContentAddressedWrite>(
    children: &[ChildRef],
    leftmost: bool,
    has_rhs: bool,
    writer: &mut IndexWriter<'_, S>,
) -> Result<ChildRef> {
    // First boundary comes from first child
    let first = if leftmost {
        None // Leftmost has no left boundary
    } else {
        children.first().and_then(|c| c.first.clone())
    };

    // RHS comes from last child
    let rhs = if has_rhs {
        children.last().and_then(|c| c.rhs.clone())
    } else {
        None // Rightmost has no right boundary
    };

    // Total size is sum of children sizes
    let total_size: u64 = children.iter().map(|c| c.size).sum();

    let serialized = serialize_branch_node(children)?;
    let serialized_len = serialized.len() as u64;
    let address = writer.write_branch(&serialized).await?;

    Ok(ChildRef {
        id: address,
        leaf: false,
        first,
        rhs,
        size: total_size,
        bytes: Some(serialized_len),
        leftmost,
    })
}

/// Split children into multiple branches if count exceeds max
///
/// # Arguments
///
/// * `children` - Child references for the branch
/// * `original_leftmost` - Whether the original branch was leftmost
/// * `original_has_rhs` - Whether the original branch had an rhs
/// * `config` - Indexer configuration with size thresholds
/// * `writer` - Index writer for persisting nodes
///
/// # Returns
///
/// Vector of ChildRefs, one per resulting branch.
pub async fn split_branch_if_needed<S: ContentAddressedWrite>(
    children: Vec<ChildRef>,
    original_leftmost: bool,
    original_has_rhs: bool,
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<Vec<ChildRef>> {
    if children.len() <= config.branch_max_children {
        // No split needed - single branch
        return Ok(vec![
            write_branch(&children, original_leftmost, original_has_rhs, writer).await?,
        ]);
    }

    // Split into chunks of target_children
    let mut branches = Vec::new();
    let chunks: Vec<_> = children.chunks(config.branch_target_children).collect();
    let num_chunks = chunks.len();

    for (i, chunk) in chunks.into_iter().enumerate() {
        let is_first = i == 0;
        let is_last = i == num_chunks - 1;

        let is_leftmost = is_first && original_leftmost;
        let has_rhs = !is_last || original_has_rhs;

        branches.push(write_branch(chunk, is_leftmost, has_rhs, writer).await?);
    }

    Ok(branches)
}

/// Finalize root with invariant checks
///
/// Root MUST have: leftmost = true, first = None, rhs = None
///
/// # Arguments
///
/// * `children` - Children to potentially wrap in a root
/// * `config` - Indexer configuration
/// * `writer` - Index writer
///
/// # Returns
///
/// The finalized root ChildRef with correct invariants.
pub async fn finalize_root<S: ContentAddressedWrite>(
    children: Vec<ChildRef>,
    config: &IndexerConfig,
    writer: &mut IndexWriter<'_, S>,
) -> Result<ChildRef> {
    // Handle potential root split
    let final_children = split_branch_if_needed(children, true, false, config, writer).await?;

    if final_children.len() == 1 {
        // Single child becomes root - apply root invariants
        let mut root = final_children.into_iter().next().unwrap();
        root.leftmost = true;
        root.first = None;
        root.rhs = None;

        debug_assert!(root.leftmost, "Root must be leftmost");
        debug_assert!(root.first.is_none(), "Root must have first = None");
        debug_assert!(root.rhs.is_none(), "Root must have rhs = None");

        return Ok(root);
    }

    // Multiple children - write new root branch with root invariants
    let total_size: u64 = final_children.iter().map(|c| c.size).sum();
    let serialized = serialize_branch_node(&final_children)?;
    let serialized_len = serialized.len() as u64;
    let address = writer.write_branch(&serialized).await?;

    let root = ChildRef {
        id: address,
        leaf: false,
        first: None, // Root invariant
        rhs: None,   // Root invariant
        size: total_size,
        bytes: Some(serialized_len),
        leftmost: true, // Root invariant
    };

    debug_assert!(root.leftmost, "Root must be leftmost");
    debug_assert!(root.first.is_none(), "Root must have first = None");
    debug_assert!(root.rhs.is_none(), "Root must have rhs = None");

    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, MemoryStorage, Sid};

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

    fn make_child_ref(id: &str, size: u64) -> ChildRef {
        ChildRef {
            id: id.to_string(),
            leaf: true,
            first: None,
            rhs: None,
            size,
            bytes: None,
            leftmost: false,
        }
    }

    #[tokio::test]
    async fn test_split_leaf_no_split() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            fluree_db_core::ContentKind::IndexNode {
                index_type: fluree_db_core::IndexType::Spot,
            },
        );
        let flake_bytes = make_flake(0, 1).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 100, flake_bytes * 200, 50, 100);

        let flakes: Vec<Flake> = (0..50).map(|i| make_flake(i, 1)).collect();

        let result = split_leaf_if_needed(flakes, true, None, &config, &mut writer)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].leftmost);
    }

    #[tokio::test]
    async fn test_split_leaf_splits() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            fluree_db_core::ContentKind::IndexNode {
                index_type: fluree_db_core::IndexType::Spot,
            },
        );
        // Very small config to force splitting
        let flake_bytes = make_flake(0, 1).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 10, flake_bytes * 20, 50, 100);

        let flakes: Vec<Flake> = (0..50).map(|i| make_flake(i, 1)).collect();

        let result = split_leaf_if_needed(flakes, true, None, &config, &mut writer)
            .await
            .unwrap();

        // Should have split into multiple leaves
        assert!(result.len() > 1);

        // First should be leftmost, others not
        assert!(result[0].leftmost);
        for child in &result[1..] {
            assert!(!child.leftmost);
        }
    }

    #[tokio::test]
    async fn test_split_branch_no_split() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            fluree_db_core::ContentKind::IndexNode {
                index_type: fluree_db_core::IndexType::Spot,
            },
        );
        let flake_bytes = make_flake(0, 1).size_estimate_bytes();
        let config = IndexerConfig::new(flake_bytes * 100, flake_bytes * 200, 50, 100);

        let children: Vec<ChildRef> = (0..10)
            .map(|i| make_child_ref(&format!("child{}", i), 10))
            .collect();

        let result = split_branch_if_needed(children, true, true, &config, &mut writer)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_finalize_root_single() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            fluree_db_core::ContentKind::IndexNode {
                index_type: fluree_db_core::IndexType::Spot,
            },
        );
        let config = IndexerConfig::default();

        let children = vec![make_child_ref("child1", 100)];

        let root = finalize_root(children, &config, &mut writer).await.unwrap();

        assert!(root.leftmost);
        assert!(root.first.is_none());
        assert!(root.rhs.is_none());
    }

    #[tokio::test]
    async fn test_finalize_root_multiple() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            fluree_db_core::ContentKind::IndexNode {
                index_type: fluree_db_core::IndexType::Spot,
            },
        );
        let config = IndexerConfig::default();

        let children: Vec<ChildRef> = (0..10)
            .map(|i| make_child_ref(&format!("child{}", i), 10))
            .collect();

        let root = finalize_root(children, &config, &mut writer).await.unwrap();

        assert!(root.leftmost);
        assert!(root.first.is_none());
        assert!(root.rhs.is_none());
        assert!(!root.leaf); // Should be a branch
        assert_eq!(root.size, 100); // Sum of children sizes
    }
}
