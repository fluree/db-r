use fluree_db_core::cache::SimpleCache;
use fluree_db_core::range::{range, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::storage::MemoryStorage;
use fluree_db_core::{Db, Flake, FlakeValue, IndexType, Sid};
use fluree_db_indexer::{batched_rebuild_from_commits, refresh_index, BatchedRebuildConfig, IndexerConfig};
use fluree_db_novelty::{Commit, Novelty};
use std::collections::HashMap;

#[cfg(feature = "commit-v2")]
fn encode_commit_v2(commit: &Commit) -> Vec<u8> {
    use fluree_db_novelty::commit_v2::envelope::{encode_envelope_fields, CommitV2Envelope};
    use fluree_db_novelty::commit_v2::format::{
        self, CommitV2Footer, CommitV2Header, FOOTER_LEN, HASH_LEN, HEADER_LEN,
    };
    use fluree_db_novelty::commit_v2::op_codec::{encode_op, CommitDicts};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap as StdHashMap;

    let mut dicts = CommitDicts::new();
    let mut ops_buf = Vec::new();
    for f in &commit.flakes {
        encode_op(f, &mut dicts, &mut ops_buf).unwrap();
    }

    let envelope = CommitV2Envelope {
        t: commit.t,
        v: commit.v,
        previous: commit.previous.clone(),
        previous_ref: commit.previous_ref.clone(),
        namespace_delta: commit
            .namespace_delta
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect::<StdHashMap<_, _>>(),
        txn: commit.txn.clone(),
        time: commit.time.clone(),
        data: commit.data.clone(),
        index: commit.index.clone(),
        indexed_at: commit.indexed_at.clone(),
    };
    let mut envelope_bytes = Vec::new();
    encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

    let dict_bytes: Vec<Vec<u8>> = vec![
        dicts.graph.serialize(),
        dicts.subject.serialize(),
        dicts.predicate.serialize(),
        dicts.datatype.serialize(),
        dicts.object_ref.serialize(),
    ];

    let ops_section_len = ops_buf.len() as u32;
    let envelope_len = envelope_bytes.len() as u32;
    let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
    let mut dict_locations = [format::DictLocation::default(); 5];
    let mut offset = dict_start as u64;
    for (i, d) in dict_bytes.iter().enumerate() {
        dict_locations[i] = format::DictLocation {
            offset,
            len: d.len() as u32,
        };
        offset += d.len() as u64;
    }

    let footer = CommitV2Footer {
        dicts: dict_locations,
        ops_section_len,
    };
    let header = CommitV2Header {
        version: format::VERSION,
        flags: 0,
        t: commit.t,
        op_count: commit.flakes.len() as u32,
        envelope_len,
    };

    let total_len = HEADER_LEN
        + envelope_bytes.len()
        + ops_buf.len()
        + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
        + FOOTER_LEN
        + HASH_LEN;
    let mut blob = vec![0u8; total_len];

    let mut pos = 0;
    header.write_to(&mut blob[pos..]);
    pos += HEADER_LEN;
    blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
    pos += envelope_bytes.len();
    blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
    pos += ops_buf.len();
    for d in &dict_bytes {
        blob[pos..pos + d.len()].copy_from_slice(d);
        pos += d.len();
    }
    footer.write_to(&mut blob[pos..]);
    pos += FOOTER_LEN;
    let hash: [u8; 32] = Sha256::digest(&blob[..pos]).into();
    blob[pos..pos + HASH_LEN].copy_from_slice(&hash);
    blob
}

fn s(ns: i32, name: &str) -> Sid {
    Sid::new(ns, name)
}

fn flake(sid_s: Sid, sid_p: Sid, o: FlakeValue, dt: Sid, t: i64, op: bool) -> Flake {
    Flake::new(sid_s, sid_p, o, dt, t, op, None)
}

async fn query_subject(
    db: &Db<MemoryStorage, SimpleCache>,
    subject: &Sid,
) -> Vec<Flake> {
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
async fn refresh_respects_time_range_for_novelty() {
    let storage = MemoryStorage::new();
    let alias = "test/main";
    let config = IndexerConfig::small();

    let mut ns_delta: HashMap<i32, String> = HashMap::new();
    ns_delta.insert(100, "http://example.org/".to_string());
    ns_delta.insert(101, "http://example.org/p/".to_string());

    let alice = s(100, "alice");
    let name = s(101, "name");
    let dt_string = s(2, "string");

    // Commit 1 at t=1
    let commit1 = Commit::new(
        "commit-1",
        1,
        vec![flake(
            alice.clone(),
            name.clone(),
            FlakeValue::String("Alice".to_string()),
            dt_string.clone(),
            1,
            true,
        )],
    )
    .with_namespace_delta(ns_delta);
    #[cfg(feature = "commit-v2")]
    storage.insert("commit-1", encode_commit_v2(&commit1));
    #[cfg(not(feature = "commit-v2"))]
    storage.insert("commit-1", serde_json::to_vec(&commit1).unwrap());

    let batched_config = BatchedRebuildConfig::default().with_indexer_config(config.clone());
    let base = batched_rebuild_from_commits(&storage, "commit-1", alias, batched_config)
        .await
        .expect("base rebuild should succeed")
        .index_result;

    let base_db = Db::load(storage.clone(), SimpleCache::new(10_000), &base.root_address)
        .await
        .expect("base Db load should succeed");

    // Novelty: t=2 and t=3 updates
    let mut novelty = Novelty::new(base_db.t);
    novelty
        .apply_commit(
            vec![flake(
                alice.clone(),
                name.clone(),
                FlakeValue::String("Alice-2".to_string()),
                dt_string.clone(),
                2,
                true,
            )],
            2,
        )
        .expect("apply t=2 novelty");
    novelty
        .apply_commit(
            vec![flake(
                alice.clone(),
                name.clone(),
                FlakeValue::String("Alice-3".to_string()),
                dt_string.clone(),
                3,
                true,
            )],
            3,
        )
        .expect("apply t=3 novelty");

    // Refresh to target_t = 2 (should exclude t=3 change)
    let refreshed = refresh_index(&storage, &base_db, &novelty, 2, config.clone())
        .await
        .expect("refresh should succeed");
    let refreshed_db = Db::load(storage.clone(), SimpleCache::new(10_000), &refreshed.root_address)
        .await
        .expect("refreshed db should load");

    let flakes = query_subject(&refreshed_db, &alice).await;
    assert!(
        flakes.iter().any(|f| matches!(f.o, FlakeValue::String(ref s) if s == "Alice-2")),
        "t=2 value should be visible"
    );
    assert!(
        !flakes.iter().any(|f| matches!(f.o, FlakeValue::String(ref s) if s == "Alice-3")),
        "t=3 value should be excluded when target_t=2"
    );
}
