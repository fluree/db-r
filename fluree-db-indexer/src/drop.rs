//! Ledger drop helpers — collect all CAS CIDs for a ledger.
//!
//! Used by `drop_ledger` to enumerate every content-addressed artifact
//! belonging to a ledger so they can be deleted (or unpinned in IPFS).
//!
//! The collector walks:
//! - Commit chain (commit envelopes + transaction blobs)
//! - Index chain (index roots, all CAS artifacts via `all_cas_ids()`,
//!   garbage records + items, named graph branch → leaf expansion)
//! - Extra NsRecord references (config, default context)

use std::collections::HashSet;

use fluree_db_binary_index::format::branch::read_branch_v2_from_bytes;
use fluree_db_binary_index::IndexRootV5;
use fluree_db_core::content_id::ContentId;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::Storage;
use fluree_db_novelty::commit_v2::read_commit_envelope;

use crate::error::Result;
use crate::gc::collector::{derive_address, walk_prev_index_chain};

/// Collect all CAS content IDs belonging to a ledger.
///
/// Walks the commit chain, index chain (including garbage records and named
/// graph branch → leaf expansion), and extra NsRecord references to build a
/// complete, deduplicated set of CIDs.
///
/// Callers can then derive storage addresses from these CIDs and delete them.
pub async fn collect_ledger_cids<S: Storage>(
    storage: &S,
    ledger_id: &str,
    commit_head_id: Option<&ContentId>,
    index_head_id: Option<&ContentId>,
    config_id: Option<&ContentId>,
    default_context_id: Option<&ContentId>,
) -> Result<HashSet<ContentId>> {
    let mut cids = HashSet::new();

    // 1. Walk commit chain: collect commit CIDs + txn CIDs
    if let Some(head) = commit_head_id {
        collect_commit_chain_cids(storage, head, ledger_id, &mut cids).await?;
    }

    // 2. Walk index chain: collect root CIDs, all CAS artifacts, garbage, branches → leaves
    if let Some(head) = index_head_id {
        collect_index_chain_cids(storage, head, ledger_id, &mut cids).await?;
    }

    // 3. Extra NsRecord references
    if let Some(id) = config_id {
        cids.insert(id.clone());
    }
    if let Some(id) = default_context_id {
        cids.insert(id.clone());
    }

    Ok(cids)
}

/// Walk the commit chain backward from `head`, collecting commit and txn CIDs.
async fn collect_commit_chain_cids<S: Storage>(
    storage: &S,
    head: &ContentId,
    ledger_id: &str,
    cids: &mut HashSet<ContentId>,
) -> Result<()> {
    let storage_method = storage.storage_method();
    let mut current = Some(head.clone());

    while let Some(commit_id) = current {
        cids.insert(commit_id.clone());

        // Read commit envelope to get previous_id and txn CID
        let addr = derive_address(&commit_id, ContentKind::Commit, storage_method, ledger_id);
        let bytes = match storage.read_bytes(&addr).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    commit_id = %commit_id,
                    error = %e,
                    "failed to read commit during drop CID collection, stopping chain walk"
                );
                break;
            }
        };

        let envelope = match read_commit_envelope(&bytes) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    commit_id = %commit_id,
                    error = %e,
                    "failed to parse commit envelope during drop, stopping chain walk"
                );
                break;
            }
        };

        if let Some(txn_id) = &envelope.txn {
            cids.insert(txn_id.clone());
        }

        current = envelope.previous_id().cloned();
    }

    Ok(())
}

/// Walk the index chain, collecting all CAS CIDs from each root.
async fn collect_index_chain_cids<S: Storage>(
    storage: &S,
    head: &ContentId,
    ledger_id: &str,
    cids: &mut HashSet<ContentId>,
) -> Result<()> {
    let storage_method = storage.storage_method();

    // Walk the chain to get all root entries
    let chain = walk_prev_index_chain(storage, head, ledger_id).await?;

    for entry in &chain {
        // Add the root CID itself
        cids.insert(entry.root_id.clone());

        // Load the full IndexRootV5 to get all CAS artifacts
        let root_addr = derive_address(
            &entry.root_id,
            ContentKind::IndexRoot,
            storage_method,
            ledger_id,
        );
        let root_bytes = match storage.read_bytes(&root_addr).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    root_id = %entry.root_id,
                    error = %e,
                    "failed to read index root during drop, skipping"
                );
                continue;
            }
        };

        let root = match IndexRootV5::decode(&root_bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    root_id = %entry.root_id,
                    error = %e,
                    "failed to decode index root during drop, skipping"
                );
                continue;
            }
        };

        // Bulk collect CAS artifact CIDs (dicts, leaves, branches, etc.)
        for id in root.all_cas_ids() {
            cids.insert(id);
        }

        // Expand named graph branches → leaf CIDs
        // (all_cas_ids includes the branch CID but not the leaves within)
        for ng in &root.named_graphs {
            for (_, branch_cid) in &ng.orders {
                let branch_addr = derive_address(
                    branch_cid,
                    ContentKind::IndexBranch,
                    storage_method,
                    ledger_id,
                );
                match storage.read_bytes(&branch_addr).await {
                    Ok(branch_bytes) => match read_branch_v2_from_bytes(&branch_bytes) {
                        Ok(manifest) => {
                            for leaf in &manifest.leaves {
                                cids.insert(leaf.leaf_cid.clone());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                branch_cid = %branch_cid,
                                error = %e,
                                "failed to decode named graph branch during drop, skipping"
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            branch_cid = %branch_cid,
                            error = %e,
                            "failed to read named graph branch during drop, skipping"
                        );
                    }
                }
            }
        }

        // Garbage record + garbage items
        if let Some(ref garbage_id) = entry.garbage_id {
            cids.insert(garbage_id.clone());

            let garbage_addr = derive_address(
                garbage_id,
                ContentKind::GarbageRecord,
                storage_method,
                ledger_id,
            );
            match crate::gc::load_garbage_record(storage, &garbage_addr).await {
                Ok(record) => {
                    for item_str in &record.garbage {
                        if let Ok(item_cid) = item_str.parse::<ContentId>() {
                            cids.insert(item_cid);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        garbage_id = %garbage_id,
                        error = %e,
                        "failed to load garbage record during drop, skipping"
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::{
        BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefsV5, DictTreeRefs, IndexRootV5,
    };
    use fluree_db_core::content_kind::CODEC_FLUREE_COMMIT;
    use fluree_db_core::prelude::*;
    use fluree_db_novelty::commit_v2::write_commit;
    use fluree_db_novelty::{Commit, CommitRef};
    use std::collections::BTreeMap;

    const LEDGER: &str = "test:main";

    /// Helper: create a CID and its derived memory-storage address.
    fn cid_and_addr(kind: ContentKind, data: &[u8]) -> (ContentId, String) {
        let cid = ContentId::new(kind, data);
        let addr = fluree_db_core::content_address("memory", kind, LEDGER, &cid.digest_hex());
        (cid, addr)
    }

    /// Build a minimal IRB1 root.
    fn minimal_irb1(
        t: i64,
        prev_index: Option<BinaryPrevIndexRef>,
        garbage: Option<BinaryGarbageRef>,
    ) -> Vec<u8> {
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let dummy_tree = DictTreeRefs {
            branch: dummy_cid.clone(),
            leaves: Vec::new(),
        };
        let root = IndexRootV5 {
            ledger_id: LEDGER.to_string(),
            index_t: t,
            base_t: 0,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: BTreeMap::new(),
            predicate_sids: Vec::new(),
            graph_iris: Vec::new(),
            datatype_iris: Vec::new(),
            language_tags: Vec::new(),
            dict_refs: DictRefsV5 {
                forward_packs: DictPackRefs {
                    string_fwd_packs: Vec::new(),
                    subject_fwd_ns_packs: Vec::new(),
                },
                subject_reverse: dummy_tree.clone(),
                string_reverse: dummy_tree,
            },
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
            graph_arenas: Vec::new(),
            default_graph_orders: Vec::new(),
            named_graphs: Vec::new(),
            stats: None,
            schema: None,
            prev_index,
            garbage,
            sketch_ref: None,
        };
        root.encode()
    }

    /// Write a minimal commit blob to storage and return its CID.
    ///
    /// Uses `from_hex_digest(CODEC_FLUREE_COMMIT, &content_hash_hex)` to derive
    /// the CID — matching how real NsRecord.commit_head_id is produced. The
    /// content_hash_hex is the SHA-256 of the canonical preimage (excluding the
    /// trailing embedded hash and signature block), NOT SHA-256(full_bytes).
    async fn write_test_commit(
        storage: &MemoryStorage,
        t: i64,
        previous: Option<&ContentId>,
        txn_cid: Option<ContentId>,
    ) -> ContentId {
        let commit = Commit {
            id: None,
            t,
            time: None,
            flakes: Vec::new(),
            previous_ref: previous.map(|id| CommitRef::new(id.clone())),
            txn: txn_cid,
            namespace_delta: std::collections::HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: std::collections::HashMap::new(),
        };

        let result = write_commit(&commit, false, None).expect("write_commit");
        let cid = ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, &result.content_hash_hex)
            .expect("valid hex digest from write_commit");
        let addr = fluree_db_core::content_address(
            "memory",
            ContentKind::Commit,
            LEDGER,
            &result.content_hash_hex,
        );
        storage.write_bytes(&addr, &result.bytes).await.unwrap();
        cid
    }

    #[tokio::test]
    async fn test_collect_empty_ledger() {
        let storage = MemoryStorage::new();
        let cids = collect_ledger_cids(&storage, LEDGER, None, None, None, None)
            .await
            .unwrap();
        assert!(cids.is_empty());
    }

    #[tokio::test]
    async fn test_collect_commit_chain() {
        let storage = MemoryStorage::new();

        // Create a 2-commit chain: c1 (genesis) <- c2 (head)
        let txn1 = ContentId::new(ContentKind::Txn, b"txn1");
        let txn2 = ContentId::new(ContentKind::Txn, b"txn2");
        let c1 = write_test_commit(&storage, 1, None, Some(txn1.clone())).await;
        let c2 = write_test_commit(&storage, 2, Some(&c1), Some(txn2.clone())).await;

        let cids = collect_ledger_cids(&storage, LEDGER, Some(&c2), None, None, None)
            .await
            .unwrap();

        // Should contain: c1, c2, txn1, txn2
        assert!(cids.contains(&c1), "should contain genesis commit");
        assert!(cids.contains(&c2), "should contain head commit");
        assert!(cids.contains(&txn1), "should contain txn1");
        assert!(cids.contains(&txn2), "should contain txn2");
        assert_eq!(cids.len(), 4);
    }

    #[tokio::test]
    async fn test_collect_index_chain() {
        let storage = MemoryStorage::new();

        // Create a 2-root index chain: root1 (genesis) <- root2 (head)
        let (root1_cid, root1_addr) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (root2_cid, root2_addr) = cid_and_addr(ContentKind::IndexRoot, b"root2");

        let root1_bytes = minimal_irb1(1, None, None);
        let root2_bytes = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: root1_cid.clone(),
            }),
            None,
        );

        storage
            .write_bytes(&root1_addr, &root1_bytes)
            .await
            .unwrap();
        storage
            .write_bytes(&root2_addr, &root2_bytes)
            .await
            .unwrap();

        let cids = collect_ledger_cids(&storage, LEDGER, None, Some(&root2_cid), None, None)
            .await
            .unwrap();

        // Should contain both root CIDs + the dummy dict tree CIDs from minimal_irb1
        assert!(cids.contains(&root1_cid), "should contain root1");
        assert!(cids.contains(&root2_cid), "should contain root2");
        // The dummy tree branch CID appears in both roots
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        assert!(cids.contains(&dummy_cid), "should contain dict tree CIDs");
    }

    #[tokio::test]
    async fn test_collect_garbage_items() {
        let storage = MemoryStorage::new();

        // Create root with garbage manifest pointing to old leaf
        let (old_leaf_cid, _) = cid_and_addr(ContentKind::IndexLeaf, b"old_leaf");
        let (garb_cid, garb_addr) = cid_and_addr(ContentKind::GarbageRecord, b"garbage1");
        let (root_cid, root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root_with_gc");

        let garbage_json = format!(
            r#"{{"ledger_id": "{}", "t": 1, "garbage": ["{}"], "created_at_ms": 0}}"#,
            LEDGER, old_leaf_cid
        );
        storage
            .write_bytes(&garb_addr, garbage_json.as_bytes())
            .await
            .unwrap();

        let root_bytes = minimal_irb1(
            1,
            None,
            Some(BinaryGarbageRef {
                id: garb_cid.clone(),
            }),
        );
        storage.write_bytes(&root_addr, &root_bytes).await.unwrap();

        let cids = collect_ledger_cids(&storage, LEDGER, None, Some(&root_cid), None, None)
            .await
            .unwrap();

        assert!(
            cids.contains(&garb_cid),
            "should contain garbage record CID"
        );
        assert!(
            cids.contains(&old_leaf_cid),
            "should contain garbage item CID"
        );
    }

    #[tokio::test]
    async fn test_collect_config_and_context() {
        let storage = MemoryStorage::new();

        let config_cid = ContentId::new(ContentKind::LedgerConfig, b"config");
        let context_cid = ContentId::new(ContentKind::Commit, b"context");

        let cids = collect_ledger_cids(
            &storage,
            LEDGER,
            None,
            None,
            Some(&config_cid),
            Some(&context_cid),
        )
        .await
        .unwrap();

        assert_eq!(cids.len(), 2);
        assert!(cids.contains(&config_cid));
        assert!(cids.contains(&context_cid));
    }

    #[tokio::test]
    async fn test_collect_deduplicates() {
        let storage = MemoryStorage::new();

        // Create two roots that reference the same dict tree CID (dummy)
        let (root1_cid, root1_addr) = cid_and_addr(ContentKind::IndexRoot, b"root_a");
        let (root2_cid, root2_addr) = cid_and_addr(ContentKind::IndexRoot, b"root_b");

        let root1_bytes = minimal_irb1(1, None, None);
        let root2_bytes = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: root1_cid.clone(),
            }),
            None,
        );

        storage
            .write_bytes(&root1_addr, &root1_bytes)
            .await
            .unwrap();
        storage
            .write_bytes(&root2_addr, &root2_bytes)
            .await
            .unwrap();

        let cids = collect_ledger_cids(&storage, LEDGER, None, Some(&root2_cid), None, None)
            .await
            .unwrap();

        // The dummy CID appears in both roots but should only be in the set once
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let dummy_count = cids.iter().filter(|c| **c == dummy_cid).count();
        assert_eq!(dummy_count, 1, "HashSet should deduplicate");
    }
}
