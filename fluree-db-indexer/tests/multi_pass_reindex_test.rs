use fluree_db_core::cache::SimpleCache;
use fluree_db_core::range::{range, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::storage::MemoryStorage;
use fluree_db_core::{Db, Flake, FlakeValue, IndexType, Sid};
use fluree_db_indexer::{batched_rebuild_from_commits, refresh_index, BatchedRebuildConfig, IndexerConfig};
use fluree_db_novelty::{Commit, Novelty};
use std::collections::HashMap;

fn s(ns: i32, name: &str) -> Sid {
    Sid::new(ns, name)
}

fn flake(sid_s: Sid, sid_p: Sid, o: FlakeValue, dt: Sid, t: i64, op: bool) -> Flake {
    Flake::new(sid_s, sid_p, o, dt, t, op, None)
}

async fn scan_subject_values(db: &Db<MemoryStorage, SimpleCache>, subject: &Sid) -> Vec<Flake> {
    range(
        db,
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject(subject.clone()),
        RangeOptions::default(),
    )
    .await
    .expect("range should succeed")
    .into_iter()
    .collect()
}

#[tokio::test]
async fn refresh_reuses_unchanged_nodes_with_small_leaf_config() {
    let storage = MemoryStorage::new();
    let alias = "test/main";

    let config = IndexerConfig::new(
        250, // leaf_target_bytes
        500, // leaf_max_bytes (force splits)
        4,   // branch_target_children
        8,   // branch_max_children
    );

    // Introduce namespaces for stable SIDs.
    let mut ns_delta: HashMap<i32, String> = HashMap::new();
    ns_delta.insert(100, "http://example.org/".to_string());
    ns_delta.insert(101, "http://example.org/p/".to_string());

    let dt_string = s(2, "string");
    let name = s(101, "name");

    // Commit 1: create many subjects to force multiple leaves.
    let mut c1_flakes = Vec::new();
    for i in 0..60 {
        let subject_name = format!("s{}", i);
        let subject = Sid::new(100, subject_name.as_str());
        c1_flakes.push(flake(
            subject,
            name.clone(),
            FlakeValue::String(format!("name-{}", i)),
            dt_string.clone(),
            1,
            true,
        ));
    }
    let commit1 = Commit::new("commit-1", 1, c1_flakes).with_namespace_delta(ns_delta);
    storage.insert("commit-1", serde_json::to_vec(&commit1).unwrap());

    let batched_config = BatchedRebuildConfig::default().with_indexer_config(config.clone());
    let base = batched_rebuild_from_commits(&storage, "commit-1", alias, batched_config)
        .await
        .expect("base rebuild should succeed")
        .index_result;

    let base_db = Db::load(storage.clone(), SimpleCache::new(10_000), &base.root_address)
        .await
        .expect("base Db load should succeed");
    assert_eq!(base_db.t, 1);

    // Commit 2: update a single subject (small novelty set).
    let subject_0 = s(100, "s0");
    let c2_flakes = vec![
        flake(
            subject_0.clone(),
            name.clone(),
            FlakeValue::String("name-0".to_string()),
            dt_string.clone(),
            2,
            false,
        ),
        flake(
            subject_0.clone(),
            name.clone(),
            FlakeValue::String("name-0-updated".to_string()),
            dt_string.clone(),
            2,
            true,
        ),
    ];

    let mut novelty = Novelty::new(base_db.t);
    novelty
        .apply_commit(c2_flakes, 2)
        .expect("apply commit2 to novelty");

    let refreshed = refresh_index(&storage, &base_db, &novelty, 2, config.clone())
        .await
        .expect("refresh should succeed");

    // Functional check: updated value is present
    let refreshed_db = Db::load(storage.clone(), SimpleCache::new(10_000), &refreshed.root_address)
        .await
        .expect("refreshed db should load");
    let subject_flakes = scan_subject_values(&refreshed_db, &subject_0).await;
    assert!(
        subject_flakes.iter().any(|f| matches!(f.o, FlakeValue::String(ref s) if s == "name-0-updated")),
        "refreshed index should include updated value"
    );

    // Structural check: refresh reused nodes and wrote some new ones
    assert!(refreshed.stats.nodes_reused > 0, "expected some nodes to be reused");
    assert!(refreshed.stats.nodes_written > 0, "expected some nodes to be written");
    assert!(
        !refreshed.stats.replaced_nodes.is_empty(),
        "expected some nodes to be replaced"
    );
}
