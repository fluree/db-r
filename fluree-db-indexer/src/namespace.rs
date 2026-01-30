//! Namespace delta replay
//!
//! Commits carry `namespace_delta` (new codes introduced). The indexer must:
//!
//! 1. Start from genesis namespace state
//! 2. Apply each commit's namespace_delta in t-order
//! 3. Derive final namespace_codes deterministically
//!
//! This ensures the indexed database has the correct namespace mappings
//! without requiring the caller to supply them.

use fluree_db_novelty::Commit;
use std::collections::BTreeMap;

/// Load genesis namespace codes from fluree-db-core
///
/// This is the single source of truth for default namespace codes,
/// ensuring consistency with `Db::genesis()`.
pub fn genesis_namespace_codes() -> BTreeMap<i32, String> {
    // Convert HashMap to BTreeMap for deterministic ordering
    fluree_db_core::default_namespace_codes()
        .into_iter()
        .collect()
}

/// Replay namespace deltas from commits to derive final namespace_codes
///
/// Commits are processed in t-order (ascending). Each commit's namespace_delta
/// is applied in order, with later commits overwriting earlier ones if there
/// are conflicts (though this shouldn't happen in practice).
///
/// # Arguments
///
/// * `base` - Initial namespace codes (typically from genesis or last index)
/// * `commits` - Commits in t-ascending order
///
/// # Returns
///
/// Final namespace codes after applying all deltas
pub fn replay_namespace_deltas(
    base: BTreeMap<i32, String>,
    commits: &[Commit],
) -> BTreeMap<i32, String> {
    let mut result = base;

    for commit in commits {
        for (code, prefix) in &commit.namespace_delta {
            result.insert(*code, prefix.clone());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_novelty::Commit;
    use std::collections::HashMap;

    fn make_commit(t: i64, delta: Vec<(i32, &str)>) -> Commit {
        let namespace_delta: HashMap<i32, String> =
            delta.into_iter().map(|(k, v)| (k, v.to_string())).collect();
        Commit::new(format!("commit-{}", t), t, vec![]).with_namespace_delta(namespace_delta)
    }

    #[test]
    fn test_genesis_namespace_codes() {
        let codes = genesis_namespace_codes();

        // Verify some well-known codes
        assert_eq!(codes.get(&0), Some(&"".to_string()));
        assert_eq!(codes.get(&1), Some(&"@".to_string()));
        assert!(codes.get(&2).unwrap().contains("XMLSchema"));
        assert!(codes.get(&3).unwrap().contains("rdf-syntax"));
    }

    #[test]
    fn test_replay_empty_commits() {
        let base: BTreeMap<i32, String> =
            BTreeMap::from([(0, "".to_string()), (1, "@".to_string())]);
        let commits: Vec<Commit> = vec![];

        let result = replay_namespace_deltas(base.clone(), &commits);
        assert_eq!(result, base);
    }

    #[test]
    fn test_replay_adds_new_codes() {
        let base: BTreeMap<i32, String> =
            BTreeMap::from([(0, "".to_string()), (1, "@".to_string())]);
        let commits = vec![
            make_commit(1, vec![(100, "http://example.org/")]),
            make_commit(2, vec![(101, "http://other.org/")]),
        ];

        let result = replay_namespace_deltas(base, &commits);

        assert_eq!(result.get(&100), Some(&"http://example.org/".to_string()));
        assert_eq!(result.get(&101), Some(&"http://other.org/".to_string()));
        // Base codes preserved
        assert_eq!(result.get(&0), Some(&"".to_string()));
        assert_eq!(result.get(&1), Some(&"@".to_string()));
    }

    #[test]
    fn test_replay_overwrites_codes() {
        let base: BTreeMap<i32, String> = BTreeMap::from([(100, "http://old.org/".to_string())]);
        let commits = vec![make_commit(1, vec![(100, "http://new.org/")])];

        let result = replay_namespace_deltas(base, &commits);
        assert_eq!(result.get(&100), Some(&"http://new.org/".to_string()));
    }

    #[test]
    fn test_replay_multiple_in_one_commit() {
        let base: BTreeMap<i32, String> = BTreeMap::new();
        let commits = vec![make_commit(
            1,
            vec![
                (100, "http://a.org/"),
                (101, "http://b.org/"),
                (102, "http://c.org/"),
            ],
        )];

        let result = replay_namespace_deltas(base, &commits);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_replay_preserves_order() {
        // Later commits should override earlier
        let base: BTreeMap<i32, String> = BTreeMap::new();
        let commits = vec![
            make_commit(1, vec![(100, "http://first.org/")]),
            make_commit(2, vec![(100, "http://second.org/")]),
        ];

        let result = replay_namespace_deltas(base, &commits);
        assert_eq!(result.get(&100), Some(&"http://second.org/".to_string()));
    }
}
