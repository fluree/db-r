use fluree_db_core::cache::SimpleCache;
use fluree_db_core::range::{range, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::storage::MemoryStorage;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Db, Flake, FlakeMeta, IndexType, Sid};
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct SidKey(i32, String);

#[derive(Clone, Debug, PartialEq, Eq)]
struct MetaKey {
    lang: Option<String>,
    i: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FlakeKey {
    s: SidKey,
    p: SidKey,
    o: FlakeValue,
    dt: SidKey,
    t: i64,
    op: bool,
    m: Option<MetaKey>,
}

fn sid_key(s: &Sid) -> SidKey {
    SidKey(s.namespace_code, s.name.as_ref().to_string())
}

fn meta_key(m: &Option<FlakeMeta>) -> Option<MetaKey> {
    m.as_ref().map(|mm| MetaKey {
        lang: mm.lang.clone(),
        i: mm.i,
    })
}

fn flake_key(f: &Flake) -> FlakeKey {
    FlakeKey {
        s: sid_key(&f.s),
        p: sid_key(&f.p),
        o: f.o.clone(),
        dt: sid_key(&f.dt),
        t: f.t,
        op: f.op,
        m: meta_key(&f.m),
    }
}

fn s(ns: i32, name: &str) -> Sid {
    Sid::new(ns, name)
}

fn flake(sid_s: Sid, sid_p: Sid, o: FlakeValue, dt: Sid, t: i64, op: bool) -> Flake {
    Flake::new(sid_s, sid_p, o, dt, t, op, None)
}

async fn scan_all(db: &Db<MemoryStorage, SimpleCache>, index: IndexType) -> Vec<FlakeKey> {
    let flakes = range(
        db,
        index,
        RangeTest::Eq,
        RangeMatch::new(), // wildcard scan
        RangeOptions::default(),
    )
    .await
    .expect("range should succeed");
    flakes.iter().map(flake_key).collect()
}

#[tokio::test]
async fn test_refresh_equals_rebuild_by_query_with_target_t_bound() {
    let storage = MemoryStorage::new();
    let alias = "test/main";
    let config = IndexerConfig::default();

    // Introduce namespaces so DbRoot has stable mappings for our SIDs.
    let mut ns_delta: HashMap<i32, String> = HashMap::new();
    ns_delta.insert(100, "http://example.org/".to_string());
    ns_delta.insert(101, "http://example.org/p/".to_string());

    // SIDs (use namespace codes above)
    let alice = s(100, "alice");
    let bob = s(100, "bob");
    let name = s(101, "name");
    let age = s(101, "age");
    let friend = s(101, "friend");

    // Datatypes
    let dt_string = s(2, "string");
    let dt_long = s(2, "long");
    let dt_id = s(1, "id");

    // Commit 1 at t=1
    let c1_flakes = vec![
        flake(
            alice.clone(),
            name.clone(),
            FlakeValue::String("Alice".to_string()),
            dt_string.clone(),
            1,
            true,
        ),
        flake(
            alice.clone(),
            age.clone(),
            FlakeValue::Long(30),
            dt_long.clone(),
            1,
            true,
        ),
        flake(
            alice.clone(),
            friend.clone(),
            FlakeValue::Ref(bob.clone()),
            dt_id.clone(),
            1,
            true,
        ),
    ];
    let commit1 = Commit::new("commit-1", 1, c1_flakes).with_namespace_delta(ns_delta);

    // Commit 2 at t=2 (update alice age, and add bob name)
    let c2_flakes = vec![
        // retract age 30
        flake(
            alice.clone(),
            age.clone(),
            FlakeValue::Long(30),
            dt_long.clone(),
            2,
            false,
        ),
        // assert age 31
        flake(
            alice.clone(),
            age.clone(),
            FlakeValue::Long(31),
            dt_long.clone(),
            2,
            true,
        ),
        flake(
            bob.clone(),
            name.clone(),
            FlakeValue::String("Bob".to_string()),
            dt_string.clone(),
            2,
            true,
        ),
    ];
    let commit2 = Commit::new("commit-2", 2, c2_flakes).with_previous("commit-1");

    // Commit 3 at t=3 (add bob age)
    let c3_flakes = vec![flake(
        bob.clone(),
        age.clone(),
        FlakeValue::Long(25),
        dt_long.clone(),
        3,
        true,
    )];
    let commit3 = Commit::new("commit-3", 3, c3_flakes).with_previous("commit-2");

    // Commit 4 at t=4 (update alice age again)
    let c4_flakes = vec![
        flake(
            alice.clone(),
            age.clone(),
            FlakeValue::Long(31),
            dt_long.clone(),
            4,
            false,
        ),
        flake(
            alice.clone(),
            age.clone(),
            FlakeValue::Long(32),
            dt_long.clone(),
            4,
            true,
        ),
    ];
    let commit4 = Commit::new("commit-4", 4, c4_flakes).with_previous("commit-3");

    // Store commits (JSON) in storage.
    #[cfg(feature = "commit-v2")]
    {
        storage.insert("commit-1", encode_commit_v2(&commit1));
        storage.insert("commit-2", encode_commit_v2(&commit2));
        storage.insert("commit-3", encode_commit_v2(&commit3));
        storage.insert("commit-4", encode_commit_v2(&commit4));
    }
    #[cfg(not(feature = "commit-v2"))]
    {
        storage.insert("commit-1", serde_json::to_vec(&commit1).unwrap());
        storage.insert("commit-2", serde_json::to_vec(&commit2).unwrap());
        storage.insert("commit-3", serde_json::to_vec(&commit3).unwrap());
        storage.insert("commit-4", serde_json::to_vec(&commit4).unwrap());
    }

    // Build base index at t=2 (head commit-2)
    let batched_config = BatchedRebuildConfig::default().with_indexer_config(config.clone());
    let base = batched_rebuild_from_commits(&storage, "commit-2", alias, batched_config)
        .await
        .expect("base rebuild should succeed")
        .index_result;
    assert_eq!(base.index_t, 2);

    let base_db = Db::load(storage.clone(), SimpleCache::new(10_000), &base.root_address)
        .await
        .expect("base Db load should succeed");
    assert_eq!(base_db.t, 2);

    // Apply novelty for commits 3 and 4, but refresh only to target_t = 3.
    let mut novelty = Novelty::new(base_db.t);
    novelty
        .apply_commit(commit3.flakes.clone(), commit3.t)
        .expect("apply commit3 to novelty");
    novelty
        .apply_commit(commit4.flakes.clone(), commit4.t)
        .expect("apply commit4 to novelty");

    let refreshed_to_3 = refresh_index(&storage, &base_db, &novelty, 3, config.clone())
        .await
        .expect("refresh to t=3 should succeed");
    assert_eq!(refreshed_to_3.index_t, 3);

    let refresh_db_3 = Db::load(
        storage.clone(),
        SimpleCache::new(10_000),
        &refreshed_to_3.root_address,
    )
    .await
    .expect("refresh db (t=3) load should succeed");
    assert_eq!(refresh_db_3.t, 3);

    // Full rebuild to t=3 should match refresh-to-3 results.
    let batched_config_3 = BatchedRebuildConfig::default().with_indexer_config(config.clone());
    let rebuilt_to_3 = batched_rebuild_from_commits(&storage, "commit-3", alias, batched_config_3)
        .await
        .expect("rebuild to t=3 should succeed")
        .index_result;
    assert_eq!(rebuilt_to_3.index_t, 3);

    let rebuild_db_3 = Db::load(
        storage.clone(),
        SimpleCache::new(10_000),
        &rebuilt_to_3.root_address,
    )
    .await
    .expect("rebuild db (t=3) load should succeed");
    assert_eq!(rebuild_db_3.t, 3);

    // Compare full scans across all 5 index orderings.
    for index in [
        IndexType::Spot,
        IndexType::Psot,
        IndexType::Post,
        IndexType::Opst,
        IndexType::Tspo,
    ] {
        let a = scan_all(&refresh_db_3, index).await;
        let b = scan_all(&rebuild_db_3, index).await;
        assert_eq!(a, b, "index {:?} differs between refresh and rebuild", index);
    }

    // Critical bug regression: ensure the t=4 update was NOT integrated when target_t=3.
    // Query alice age "as-of" t=3 via SPOT subject scan; should still be 31.
    let alice_flakes_t3 = range(
        &refresh_db_3,
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject(alice.clone()),
        RangeOptions::default().with_to_t(3),
    )
    .await
    .unwrap();
    let mut ages: Vec<i64> = alice_flakes_t3
        .iter()
        .filter(|f| f.p == age && matches!(f.o, FlakeValue::Long(_)) && f.op)
        .filter_map(|f| match f.o {
            FlakeValue::Long(v) => Some(v),
            _ => None,
        })
        .collect();
    ages.sort();
    assert_eq!(ages, vec![31], "alice age at t=3 should be 31");

    // Now refresh to t=4 and compare again to rebuild-to-4, proving the upper bound is inclusive.
    let refreshed_to_4 = refresh_index(&storage, &base_db, &novelty, 4, config.clone())
        .await
        .expect("refresh to t=4 should succeed");
    assert_eq!(refreshed_to_4.index_t, 4);
    let refresh_db_4 = Db::load(
        storage.clone(),
        SimpleCache::new(10_000),
        &refreshed_to_4.root_address,
    )
    .await
    .expect("refresh db (t=4) load should succeed");
    assert_eq!(refresh_db_4.t, 4);

    let batched_config_4 = BatchedRebuildConfig::default().with_indexer_config(config);
    let rebuilt_to_4 = batched_rebuild_from_commits(&storage, "commit-4", alias, batched_config_4)
        .await
        .expect("rebuild to t=4 should succeed")
        .index_result;
    assert_eq!(rebuilt_to_4.index_t, 4);
    let rebuild_db_4 = Db::load(
        storage.clone(),
        SimpleCache::new(10_000),
        &rebuilt_to_4.root_address,
    )
    .await
    .expect("rebuild db (t=4) load should succeed");
    assert_eq!(rebuild_db_4.t, 4);

    for index in [
        IndexType::Spot,
        IndexType::Psot,
        IndexType::Post,
        IndexType::Opst,
        IndexType::Tspo,
    ] {
        let a = scan_all(&refresh_db_4, index).await;
        let b = scan_all(&rebuild_db_4, index).await;
        assert_eq!(a, b, "index {:?} differs at t=4 between refresh and rebuild", index);
    }
}

