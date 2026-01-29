//! Stack-based tree traversal for incremental refresh
//!
//! Implements iterative depth-first traversal with explicit stacks for
//! integrating novelty into the index tree.

use super::merge::merge_and_dedup;
use super::novelty_slice::{get_novelty_flakes_since, has_novelty_since};
use super::resolver::{load_node, ResolvedContent, ResolvedNode};
use super::split::{finalize_root, split_branch_if_needed, split_leaf_if_needed};
use crate::config::IndexerConfig;
use crate::error::Result;
use crate::writer::IndexWriter;
use fluree_db_core::index::ChildRef;
use fluree_db_core::flake::size_flakes_estimate;
use fluree_db_core::{ContentAddressedWrite, IndexType, Storage};
use fluree_db_novelty::Novelty;
use std::collections::HashMap;
use std::sync::Arc;

/// Input for incremental index refresh
pub struct RefreshInput<'a, S> {
    /// Storage backend
    pub storage: &'a S,
    /// Current index root
    pub root: ChildRef,
    /// Which index type to refresh
    pub index_type: IndexType,
    /// Novelty overlay with new flakes
    pub novelty: &'a Novelty,
    /// Ledger alias
    pub alias: &'a str,
    /// Indexer configuration
    pub config: &'a IndexerConfig,
    /// Only include novelty with t > since_t (current index_t)
    pub since_t: i64,
    /// Only include novelty with t <= to_t (target transaction time)
    pub to_t: i64,
}

/// Result of refreshing a single index tree
#[derive(Debug, Clone)]
pub struct RefreshResult {
    /// The new root after refresh
    pub new_root: ChildRef,
    /// Refresh statistics
    pub stats: RefreshStats,
}

/// Statistics from refresh operation
#[derive(Debug, Clone, Default)]
pub struct RefreshStats {
    /// Number of nodes visited during traversal
    pub nodes_visited: usize,
    /// Number of leaves modified
    pub leaves_modified: usize,
    /// Number of branches modified
    pub branches_modified: usize,
    /// Number of new nodes written
    pub nodes_written: usize,
    /// Number of nodes reused (unchanged)
    pub nodes_reused: usize,
    /// Estimated flake-bytes delta for this refresh (Clojure-style stats), SPOT only
    ///
    /// This is computed at modified leaves as:
    /// \(\sum size(new_leaf_flakes) - size(old_leaf_flakes)\).
    ///
    /// For non-SPOT index types this stays 0 so merging across indexes is safe.
    pub flake_bytes_delta: i64,
    /// Addresses of nodes that were replaced (not reused) during this refresh.
    /// These are candidates for garbage collection.
    /// Only tracks nodes that were replaced (content changed), not new splits or additions.
    pub replaced_nodes: Vec<String>,
}

impl RefreshStats {
    /// Merge another stats into this one
    pub fn merge(&mut self, other: &RefreshStats) {
        self.nodes_visited += other.nodes_visited;
        self.leaves_modified += other.leaves_modified;
        self.branches_modified += other.branches_modified;
        self.nodes_written += other.nodes_written;
        self.nodes_reused += other.nodes_reused;
        self.flake_bytes_delta += other.flake_bytes_delta;
        self.replaced_nodes.extend(other.replaced_nodes.iter().cloned());
    }
}

/// Stack entry for iterative depth-first traversal
enum StackEntry {
    /// Process a node (may be leaf or branch)
    ProcessNode { node: ResolvedNode },
    /// Branch being descended (children not yet processed)
    DescendBranch {
        child_ref: ChildRef,
        children: Arc<[ChildRef]>,
        affected_indices: Vec<usize>,
        next_to_process: usize,
    },
    /// Branch ready for reconstruction (children processed)
    ReconstructBranch {
        child_ref: ChildRef,
        original_children: Arc<[ChildRef]>,
        affected_indices: Vec<usize>,
    },
}

/// Integrate novelty into an index tree
///
/// This is the main entry point for incremental refresh of a single index type.
///
/// # Algorithm
///
/// 1. Check if novelty affects root; if not, return unchanged
/// 2. Use stack-based DFS to descend into affected nodes
/// 3. At leaves: merge novelty, apply leaf-local dedup, split if overflow
/// 4. At branches: collect updated children, splice with unchanged, split if overflow
/// 5. Propagate changes upward to root
///
/// # Arguments
///
/// * `input` - Refresh input containing storage, root, novelty, etc.
/// * `writer` - Index writer for persisting new nodes
///
/// # Returns
///
/// The new root and statistics.
pub async fn integrate_novelty<S: Storage + ContentAddressedWrite>(
    input: RefreshInput<'_, S>,
    writer: &mut IndexWriter<'_, S>,
) -> Result<RefreshResult> {
    // Quick check: if no novelty affects root, return unchanged
    if !has_novelty_since(
        input.novelty,
        input.index_type,
        input.root.first.as_ref(),
        input.root.rhs.as_ref(),
        input.root.leftmost,
        input.since_t,
        input.to_t,
    ) {
        return Ok(RefreshResult {
            new_root: input.root.clone(),
            stats: RefreshStats {
                nodes_reused: 1,
                ..Default::default()
            },
        });
    }

    let mut stats = RefreshStats::default();

    // Result stack: stores Vec<ChildRef> produced by processing each node
    let mut result_stack: Vec<Vec<ChildRef>> = Vec::new();

    // Work stack for traversal
    let mut work_stack: Vec<StackEntry> = Vec::new();

    // Initialize with root
    let root_node = load_node(input.storage, input.root.clone()).await?;
    work_stack.push(StackEntry::ProcessNode { node: root_node });

    while let Some(entry) = work_stack.pop() {
        match entry {
            StackEntry::ProcessNode { node } => {
                stats.nodes_visited += 1;

                match &node.content {
                    ResolvedContent::Leaf(flakes) => {
                        // Process leaf: merge novelty, dedup, split if needed
                        let new_children = process_leaf(
                            &node.child_ref,
                            flakes,
                            &input,
                            writer,
                            &mut stats,
                        )
                        .await?;
                        result_stack.push(new_children);
                    }
                    ResolvedContent::Branch(children) => {
                        // Find which children are affected by novelty
                        let affected = find_affected_children_for_novelty(
                            children,
                            &input,
                        );

                        if affected.is_empty() {
                            // No children affected - reuse this branch
                            stats.nodes_reused += 1;
                            result_stack.push(vec![node.child_ref.clone()]);
                        } else {
                            // Push reconstruction entry (will be processed after children)
                            work_stack.push(StackEntry::ReconstructBranch {
                                child_ref: node.child_ref.clone(),
                                original_children: children.clone(),
                                affected_indices: affected.clone(),
                            });

                            // Push descent entry
                            work_stack.push(StackEntry::DescendBranch {
                                child_ref: node.child_ref.clone(),
                                children: children.clone(),
                                affected_indices: affected,
                                next_to_process: 0,
                            });
                        }
                    }
                }
            }

            StackEntry::DescendBranch {
                child_ref,
                children,
                affected_indices,
                next_to_process,
            } => {
                if next_to_process < affected_indices.len() {
                    // More children to process
                    let child_idx = affected_indices[next_to_process];
                    let child = &children[child_idx];

                    // Push self back with incremented index
                    work_stack.push(StackEntry::DescendBranch {
                        child_ref,
                        children: children.clone(),
                        affected_indices: affected_indices.clone(),
                        next_to_process: next_to_process + 1,
                    });

                    // Load and process the child
                    let child_node = load_node(input.storage, child.clone()).await?;
                    work_stack.push(StackEntry::ProcessNode { node: child_node });
                }
                // If all children processed, DescendBranch just falls through
            }

            StackEntry::ReconstructBranch {
                child_ref,
                original_children,
                affected_indices,
            } => {
                // Collect results for affected children (in reverse order since stack)
                let num_affected = affected_indices.len();
                let mut replacements: HashMap<usize, Vec<ChildRef>> = HashMap::new();

                for i in (0..num_affected).rev() {
                    let child_idx = affected_indices[i];
                    if let Some(result) = result_stack.pop() {
                        replacements.insert(child_idx, result);
                    }
                }

                // Reconstruct children
                let new_children = reconstruct_children(&original_children, replacements);

                // Split if needed
                let original_has_rhs = child_ref.rhs.is_some();
                let final_children = split_branch_if_needed(
                    new_children,
                    child_ref.leftmost,
                    original_has_rhs,
                    input.config,
                    writer,
                )
                .await?;

                stats.branches_modified += 1;
                stats.nodes_written += final_children.len();
                // Track the old branch address as garbage (it was replaced, not reused)
                stats.replaced_nodes.push(child_ref.id.clone());

                result_stack.push(final_children);
            }
        }
    }

    // Final result should be on the stack
    let final_children = result_stack
        .pop()
        .expect("Result stack should have one entry");

    // Finalize root with invariant checks
    let new_root = finalize_root(final_children, input.config, writer).await?;

    Ok(RefreshResult {
        new_root,
        stats,
    })
}

/// Process a leaf node: merge novelty, dedup, split if needed
async fn process_leaf<S: ContentAddressedWrite>(
    child_ref: &ChildRef,
    existing_flakes: &Arc<[fluree_db_core::Flake]>,
    input: &RefreshInput<'_, S>,
    writer: &mut IndexWriter<'_, S>,
    stats: &mut RefreshStats,
) -> Result<Vec<ChildRef>> {
    // Size accounting (Clojure-style): only SPOT contributes to flake-byte stats.
    let old_bytes: u64 = if input.index_type == IndexType::Spot {
        size_flakes_estimate(existing_flakes.as_ref())
    } else {
        0
    };

    // Get novelty flakes for this leaf's range
    let novelty_iter = get_novelty_flakes_since(
        input.novelty,
        input.index_type,
        child_ref.first.as_ref(),
        child_ref.rhs.as_ref(),
        child_ref.leftmost,
        input.since_t,
        input.to_t,
    );

    // Merge and dedup
    let merged = merge_and_dedup(
        existing_flakes,
        novelty_iter.cloned(),
        input.index_type,
    );

    let new_bytes: u64 = if input.index_type == IndexType::Spot {
        size_flakes_estimate(&merged)
    } else {
        0
    };
    if input.index_type == IndexType::Spot {
        stats.flake_bytes_delta += new_bytes as i64 - old_bytes as i64;
    }

    // Split if needed
    let new_children = split_leaf_if_needed(
        merged,
        child_ref.leftmost,
        child_ref.rhs.clone(),
        input.config,
        writer,
    )
    .await?;

    stats.leaves_modified += 1;
    stats.nodes_written += new_children.len();
    // Track the old node address as garbage (it was replaced, not reused)
    stats.replaced_nodes.push(child_ref.id.clone());

    Ok(new_children)
}

/// Find children affected by novelty
fn find_affected_children_for_novelty<S>(
    children: &[ChildRef],
    input: &RefreshInput<'_, S>,
) -> Vec<usize> {
    let mut affected = Vec::new();

    for (i, child) in children.iter().enumerate() {
        if has_novelty_since(
            input.novelty,
            input.index_type,
            child.first.as_ref(),
            child.rhs.as_ref(),
            child.leftmost,
            input.since_t,
            input.to_t,
        ) {
            affected.push(i);
        }
    }

    affected
}

/// Reconstruct branch children by splicing updated children with unchanged ones
fn reconstruct_children(
    original: &[ChildRef],
    replacements: HashMap<usize, Vec<ChildRef>>,
) -> Vec<ChildRef> {
    let total_new: usize = replacements.values().map(|v| v.len()).sum();
    let mut result = Vec::with_capacity(original.len() + total_new);

    for (i, child) in original.iter().enumerate() {
        if let Some(new_children) = replacements.get(&i) {
            // Splice in replacement(s) - may be multiple if split occurred
            result.extend(new_children.iter().cloned());
        } else {
            // Unchanged child
            result.push(child.clone());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_child_ref(id: &str, leftmost: bool) -> ChildRef {
        ChildRef {
            id: id.to_string(),
            leaf: true,
            first: if leftmost {
                None
            } else {
                Some(fluree_db_core::Flake::new(
                    fluree_db_core::Sid::new(1, "s1"),
                    fluree_db_core::Sid::new(1, "p1"),
                    fluree_db_core::FlakeValue::Long(1),
                    fluree_db_core::Sid::new(2, "long"),
                    1,
                    true,
                    None,
                ))
            },
            rhs: None,
            size: 10,
            bytes: None,
            leftmost,
        }
    }

    #[test]
    fn test_reconstruct_children_no_changes() {
        let original = vec![
            make_child_ref("c0", true),
            make_child_ref("c1", false),
            make_child_ref("c2", false),
        ];

        let replacements = HashMap::new();
        let result = reconstruct_children(&original, replacements);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, "c0");
        assert_eq!(result[1].id, "c1");
        assert_eq!(result[2].id, "c2");
    }

    #[test]
    fn test_reconstruct_children_single_replacement() {
        let original = vec![
            make_child_ref("c0", true),
            make_child_ref("c1", false),
            make_child_ref("c2", false),
        ];

        let mut replacements = HashMap::new();
        replacements.insert(1, vec![make_child_ref("new1", false)]);

        let result = reconstruct_children(&original, replacements);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, "c0");
        assert_eq!(result[1].id, "new1");
        assert_eq!(result[2].id, "c2");
    }

    #[test]
    fn test_reconstruct_children_split_replacement() {
        let original = vec![
            make_child_ref("c0", true),
            make_child_ref("c1", false),
            make_child_ref("c2", false),
        ];

        // Child 1 split into two
        let mut replacements = HashMap::new();
        replacements.insert(
            1,
            vec![make_child_ref("new1a", false), make_child_ref("new1b", false)],
        );

        let result = reconstruct_children(&original, replacements);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].id, "c0");
        assert_eq!(result[1].id, "new1a");
        assert_eq!(result[2].id, "new1b");
        assert_eq!(result[3].id, "c2");
    }

    #[test]
    fn test_reconstruct_children_multiple_replacements() {
        let original = vec![
            make_child_ref("c0", true),
            make_child_ref("c1", false),
            make_child_ref("c2", false),
        ];

        let mut replacements = HashMap::new();
        replacements.insert(0, vec![make_child_ref("new0", true)]);
        replacements.insert(2, vec![make_child_ref("new2a", false), make_child_ref("new2b", false)]);

        let result = reconstruct_children(&original, replacements);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].id, "new0");
        assert_eq!(result[1].id, "c1");
        assert_eq!(result[2].id, "new2a");
        assert_eq!(result[3].id, "new2b");
    }

    #[test]
    fn test_refresh_stats_merge() {
        let mut stats1 = RefreshStats {
            nodes_visited: 10,
            leaves_modified: 5,
            branches_modified: 2,
            nodes_written: 7,
            nodes_reused: 3,
            flake_bytes_delta: 100,
            replaced_nodes: vec!["addr1".to_string(), "addr2".to_string()],
        };

        let stats2 = RefreshStats {
            nodes_visited: 5,
            leaves_modified: 2,
            branches_modified: 1,
            nodes_written: 3,
            nodes_reused: 2,
            flake_bytes_delta: -40,
            replaced_nodes: vec!["addr3".to_string()],
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.nodes_visited, 15);
        assert_eq!(stats1.leaves_modified, 7);
        assert_eq!(stats1.branches_modified, 3);
        assert_eq!(stats1.nodes_written, 10);
        assert_eq!(stats1.nodes_reused, 5);
        assert_eq!(stats1.flake_bytes_delta, 60);
        assert_eq!(stats1.replaced_nodes.len(), 3);
        assert_eq!(stats1.replaced_nodes[0], "addr1");
        assert_eq!(stats1.replaced_nodes[1], "addr2");
        assert_eq!(stats1.replaced_nodes[2], "addr3");
    }
}
