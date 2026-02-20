//! Garbage collection implementation
//!
//! Provides the `clean_garbage` function that walks the prev-index chain,
//! identifies gc-eligible indexes, and deletes obsolete nodes.
//!
//! # Clojure Parity Semantics
//!
//! The garbage record in root N contains addresses of nodes that were replaced
//! when creating root N from root N-1. When we GC:
//! 1. Use root N's garbage manifest to delete nodes from root N-1
//! 2. Delete root N-1's garbage manifest
//! 3. Delete root N-1 itself (truncating the chain)
//!
//! This means GC operates on pairs: (newer root with manifest, older root to delete).

use super::{load_garbage_record, CleanGarbageConfig, CleanGarbageResult};
use super::{DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS};
use crate::error::Result;
use fluree_db_core::{ContentId, ContentKind, Storage};

/// Entry in the prev-index chain.
struct IndexChainEntry {
    /// Transaction time of this index.
    t: i64,
    /// CID of this root blob.
    root_id: ContentId,
    /// CID of this root's garbage manifest (if any).
    garbage_id: Option<ContentId>,
}

/// Extract the GC-relevant fields from an IRB1 index root blob.
///
/// Returns `(index_t, prev_index_id, garbage_id)`.
fn parse_chain_fields(bytes: &[u8]) -> Result<(i64, Option<ContentId>, Option<ContentId>)> {
    let v5 = crate::run_index::IndexRootV5::decode(bytes).map_err(|e| {
        crate::error::IndexerError::Serialization(format!("index root: expected IRB1: {e}"))
    })?;
    let prev_id = v5.prev_index.map(|p| p.id);
    let garbage_id = v5.garbage.map(|g| g.id);
    Ok((v5.index_t, prev_id, garbage_id))
}

/// Derive a storage address from a ContentId.
fn derive_address(
    cid: &ContentId,
    kind: ContentKind,
    storage_method: &str,
    ledger_id: &str,
) -> String {
    fluree_db_core::content_address(storage_method, kind, ledger_id, &cid.digest_hex())
}

/// Walk the prev-index chain starting from the current root CID.
///
/// Returns entries in order from newest to oldest.
///
/// **Tolerant behavior**: If a prev_index link cannot be loaded (e.g., it was
/// deleted by prior GC), the walk stops gracefully at that point rather than
/// returning an error. This ensures GC is idempotent.
async fn walk_prev_index_chain<S: Storage>(
    storage: &S,
    current_root_id: &ContentId,
    ledger_id: &str,
) -> Result<Vec<IndexChainEntry>> {
    let storage_method = storage.storage_method();
    let mut chain = Vec::new();
    let mut current_id = current_root_id.clone();

    loop {
        let address = derive_address(
            &current_id,
            ContentKind::IndexRoot,
            storage_method,
            ledger_id,
        );

        // Load the db-root - if this fails on the first entry, propagate error.
        // For subsequent entries (prev_index links), treat as end of chain.
        let bytes = match storage.read_bytes(&address).await {
            Ok(b) => b,
            Err(e) => {
                if chain.is_empty() {
                    // Can't load the starting root - that's a real error
                    return Err(e.into());
                } else {
                    // Can't load prev_index - chain was truncated by prior GC
                    tracing::debug!(
                        root_id = %current_id,
                        "prev_index not found, chain ends here (prior GC)"
                    );
                    break;
                }
            }
        };

        let (t, prev_index_id, garbage_id) = parse_chain_fields(&bytes)?;

        let next_id = prev_index_id;
        chain.push(IndexChainEntry {
            t,
            root_id: current_id,
            garbage_id,
        });

        match next_id {
            Some(id) => current_id = id,
            None => break,
        }
    }

    Ok(chain)
}

/// Resolve a garbage record item (CID string) to a storage address for deletion.
///
/// Returns `None` if the CID cannot be parsed or has no recognized content kind.
fn resolve_garbage_item(item: &str, storage_method: &str, ledger_id: &str) -> Option<String> {
    let cid = match item.parse::<ContentId>() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(item, error = %e, "Skipping unrecognized garbage item (not a valid CID)");
            return None;
        }
    };
    let kind = match cid.content_kind() {
        Some(k) => k,
        None => {
            tracing::warn!(item, "Skipping garbage item with unknown content kind");
            return None;
        }
    };
    Some(derive_address(&cid, kind, storage_method, ledger_id))
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Clean garbage from old index versions.
///
/// This function implements Clojure-parity GC semantics:
///
/// 1. Walks the prev-index chain to collect all index versions
/// 2. Retains `current + max_old_indexes` versions (e.g., max_old_indexes=5 keeps 6 total)
/// 3. For gc-eligible indexes, uses the newer root's garbage manifest to delete nodes
/// 4. Deletes the older root and its garbage manifest (truncating the chain)
///
/// # Retention Policy
///
/// Both thresholds must be satisfied for GC to occur:
/// - `max_old_indexes`: Maximum old index versions to keep (default: 5)
///   With max_old_indexes=5, we keep current + 5 old = 6 total
/// - `min_time_garbage_mins`: Minimum age before an index can be GC'd (default: 30)
///
/// Age is determined by the garbage record's `created_at_ms` field.
/// If a garbage record is missing or has no timestamp, the index is skipped (conservative).
///
/// # Safety
///
/// This function is idempotent - running it multiple times is safe.
/// Chain walking is tolerant of missing roots (stops gracefully).
/// Already-deleted nodes are skipped without error.
pub async fn clean_garbage<S>(
    storage: &S,
    current_root_id: &ContentId,
    ledger_id: &str,
    config: CleanGarbageConfig,
) -> Result<CleanGarbageResult>
where
    S: Storage,
{
    let max_old_indexes = config.max_old_indexes.unwrap_or(DEFAULT_MAX_OLD_INDEXES) as usize;
    let min_age_mins = config
        .min_time_garbage_mins
        .unwrap_or(DEFAULT_MIN_TIME_GARBAGE_MINS);
    let min_age_ms = min_age_mins as i64 * 60 * 1000;
    let now_ms = current_timestamp_ms();

    // 1. Walk prev_index chain to collect all index versions (tolerant of missing roots)
    let index_chain = walk_prev_index_chain(storage, current_root_id, ledger_id).await?;

    // Retention: keep current + max_old_indexes
    // With max_old_indexes=5, keep_count=6 (indices 0..5)
    let keep_count = 1 + max_old_indexes;

    if index_chain.len() <= keep_count {
        // Not enough indexes to trigger GC
        return Ok(CleanGarbageResult::default());
    }

    // 2. Process ALL gc-eligible entries from oldest to newest.
    //
    // Chain is newest-first. Indices 0..keep_count are retained.
    // Indices keep_count..len are gc-eligible.
    //
    // For each gc-eligible entry at index i, the manifest at index i-1 (the
    // newer entry) lists nodes from entry i that were replaced. We use that
    // manifest to delete those nodes, then delete the entry's own garbage
    // manifest and root.
    //
    // Oldest-first processing (reversed range) is crash-safe: if interrupted,
    // remaining gc-eligible entries are still reachable via prev_index chain
    // from the retained set. Newest-first would truncate the chain at the
    // retention boundary, orphaning everything beyond.
    //
    // We break (not continue) on any failure because skipping an entry and
    // deleting a newer one would orphan the skipped entry and everything
    // older than it.

    let storage_method = storage.storage_method();
    let mut deleted_count = 0;
    let mut indexes_cleaned = 0;

    for i in (keep_count..index_chain.len()).rev() {
        let manifest_entry = &index_chain[i - 1];
        let entry_to_delete = &index_chain[i];

        // Manifest from the newer entry lists nodes from entry_to_delete
        // that were replaced when manifest_entry was built.
        let garbage_id = match &manifest_entry.garbage_id {
            Some(id) => id,
            None => {
                tracing::debug!(
                    t = manifest_entry.t,
                    "No garbage manifest in index, stopping GC"
                );
                break;
            }
        };

        // Derive storage address for the garbage record and load it
        let garbage_addr = derive_address(
            garbage_id,
            ContentKind::GarbageRecord,
            storage_method,
            ledger_id,
        );
        let record = match load_garbage_record(storage, &garbage_addr).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(
                    t = manifest_entry.t,
                    error = %e,
                    "Failed to load garbage record (may already be deleted), stopping GC"
                );
                break;
            }
        };

        // Check age: if created_at_ms is 0 (old format) or too recent, stop.
        // Newer manifests will be even more recent, so break is correct.
        if record.created_at_ms == 0 || now_ms - record.created_at_ms < min_age_ms {
            tracing::debug!(
                t = manifest_entry.t,
                age_mins = (now_ms - record.created_at_ms) / 60000,
                min_age_mins = min_age_mins,
                "Garbage record too recent, stopping GC"
            );
            break;
        }

        // Delete the garbage nodes (CID strings resolved to storage addresses).
        for item in &record.garbage {
            if let Some(addr) = resolve_garbage_item(item, storage_method, ledger_id) {
                if let Err(e) = storage.delete(&addr).await {
                    tracing::debug!(
                        address = %addr,
                        error = %e,
                        "Failed to delete garbage node (may already be deleted)"
                    );
                } else {
                    deleted_count += 1;
                }
            }
        }

        // Delete entry_to_delete's own garbage manifest
        if let Some(ref old_garbage_id) = entry_to_delete.garbage_id {
            let old_garbage_addr = derive_address(
                old_garbage_id,
                ContentKind::GarbageRecord,
                storage_method,
                ledger_id,
            );
            if let Err(e) = storage.delete(&old_garbage_addr).await {
                tracing::debug!(
                    address = %old_garbage_addr,
                    error = %e,
                    "Failed to delete old garbage manifest (may already be deleted)"
                );
            }
        }

        // Delete the old db-root
        let root_addr = derive_address(
            &entry_to_delete.root_id,
            ContentKind::IndexRoot,
            storage_method,
            ledger_id,
        );
        if let Err(e) = storage.delete(&root_addr).await {
            tracing::debug!(
                address = %root_addr,
                error = %e,
                "Failed to delete old db-root (may already be deleted)"
            );
        } else {
            indexes_cleaned += 1;
        }
    }

    if indexes_cleaned > 0 || deleted_count > 0 {
        tracing::info!(
            indexes_cleaned = indexes_cleaned,
            nodes_deleted = deleted_count,
            retained_count = keep_count,
            "Garbage collection complete"
        );
    }

    Ok(CleanGarbageResult {
        indexes_cleaned,
        nodes_deleted: deleted_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::{
        BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefsV5, DictTreeRefs, IndexRootV5,
    };
    use fluree_db_core::prelude::*;
    use std::collections::BTreeMap;

    const LEDGER: &str = "test:main";

    /// Build a minimal IRB1 root with the given t, prev_index, and garbage.
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
                numbig: Vec::new(),
                vectors: Vec::new(),
            },
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
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

    /// Helper: create a CID and its derived memory-storage address.
    fn cid_and_addr(kind: ContentKind, data: &[u8]) -> (ContentId, String) {
        let cid = ContentId::new(kind, data);
        let addr = fluree_db_core::content_address("memory", kind, LEDGER, &cid.digest_hex());
        (cid, addr)
    }

    #[test]
    fn test_current_timestamp_ms() {
        let ts = current_timestamp_ms();
        // Should be a reasonable timestamp (after year 2020)
        assert!(ts > 1577836800000); // Jan 1, 2020 in ms
    }

    #[test]
    fn test_parse_chain_fields_v3_cid() {
        // IRB1 root with prev_index and garbage set.
        let (prev_cid, _) = cid_and_addr(ContentKind::IndexRoot, b"prev");
        let (garb_cid, _) = cid_and_addr(ContentKind::GarbageRecord, b"garb");

        let bytes = minimal_irb1(
            5,
            Some(BinaryPrevIndexRef {
                t: 4,
                id: prev_cid.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid.clone(),
            }),
        );
        let (t, prev, garbage) = parse_chain_fields(&bytes).unwrap();
        assert_eq!(t, 5);
        assert_eq!(prev, Some(prev_cid));
        assert_eq!(garbage, Some(garb_cid));
    }

    #[test]
    fn test_parse_chain_fields_minimal() {
        // IRB1 root without prev_index or garbage.
        let bytes = minimal_irb1(1, None, None);
        let (t, prev, garbage) = parse_chain_fields(&bytes).unwrap();
        assert_eq!(t, 1);
        assert_eq!(prev, None);
        assert_eq!(garbage, None);
    }

    #[test]
    fn test_resolve_garbage_item_cid() {
        let cid = ContentId::new(ContentKind::IndexLeaf, b"leaf-data");
        let cid_str = cid.to_string();
        let resolved = resolve_garbage_item(&cid_str, "memory", LEDGER);
        let expected = fluree_db_core::content_address(
            "memory",
            ContentKind::IndexLeaf,
            LEDGER,
            &cid.digest_hex(),
        );
        assert_eq!(resolved, Some(expected));
    }

    #[test]
    fn test_resolve_garbage_item_invalid() {
        // Non-CID strings are skipped (return None)
        let resolved = resolve_garbage_item("not-a-cid", "memory", LEDGER);
        assert_eq!(resolved, None);
    }

    #[tokio::test]
    async fn test_walk_empty_chain() {
        let storage = MemoryStorage::new();
        let (root_cid, root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root1");

        let root_bytes = minimal_irb1(1, None, None);
        storage.write_bytes(&root_addr, &root_bytes).await.unwrap();

        let chain = walk_prev_index_chain(&storage, &root_cid, LEDGER)
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 1);
        assert_eq!(chain[0].root_id, root_cid);

        // Also verify clean_garbage with this chain (not enough to GC)
        let config = CleanGarbageConfig {
            max_old_indexes: Some(5),
            min_time_garbage_mins: Some(0),
        };
        let result = clean_garbage(&storage, &root_cid, LEDGER, config)
            .await
            .unwrap();
        assert_eq!(result.indexes_cleaned, 0);
        assert_eq!(result.nodes_deleted, 0);
    }

    #[tokio::test]
    async fn test_walk_chain_irb1_format() {
        // Test chain walking with IRB1-encoded roots
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (_, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");

        let root1 = minimal_irb1(1, None, None);
        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            None,
        );
        let root3 = minimal_irb1(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();

        let (cid3, _) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let chain = walk_prev_index_chain(&storage, &cid3, LEDGER)
            .await
            .unwrap();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].t, 3);
        assert_eq!(chain[1].t, 2);
        assert_eq!(chain[2].t, 1);
    }

    #[tokio::test]
    async fn test_walk_chain_tolerant_of_missing_prev() {
        let storage = MemoryStorage::new();

        let (missing_cid, _) = cid_and_addr(ContentKind::IndexRoot, b"missing");
        let (_, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");

        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: missing_cid,
            }),
            None,
        );
        storage.write_bytes(&addr2, &root2).await.unwrap();

        let (cid2, _) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let chain = walk_prev_index_chain(&storage, &cid2, LEDGER)
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 2);
    }

    #[tokio::test]
    async fn test_clean_garbage_clojure_semantics() {
        // Test Clojure parity GC with IRB1-encoded roots and garbage items.
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid1, garb_addr1) = cid_and_addr(ContentKind::GarbageRecord, b"garb1");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (old_leaf_cid, old_leaf_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old_leaf");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        // t=1: oldest, has its own garbage manifest
        let root1 = minimal_irb1(
            1,
            None,
            Some(BinaryGarbageRef {
                id: garb_cid1.clone(),
            }),
        );

        // t=2: points to t=1, has garbage manifest (nodes replaced from t=1->t=2)
        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );

        // t=3: current, points to t=2
        let root3 = minimal_irb1(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        // Garbage record at t=2: CID strings of nodes replaced from t=1
        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, old_leaf_cid, old_ts
        );

        // Garbage record at t=1 (empty, will be deleted with t=1)
        let garbage1 = format!(
            r#"{{"ledger_id": "{}", "t": 1, "garbage": [], "created_at_ms": 0}}"#,
            LEDGER
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr1, garbage1.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&old_leaf_addr, b"old leaf data")
            .await
            .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
        };

        let result = clean_garbage(&storage, &cid3, LEDGER, config)
            .await
            .unwrap();

        // Should clean 1 index (t=1) and delete 1 node (old_leaf)
        assert_eq!(result.indexes_cleaned, 1);
        assert_eq!(result.nodes_deleted, 1);

        // Old leaf deleted via CID->address resolution
        assert!(storage.read_bytes(&old_leaf_addr).await.is_err());
        // t=1 root deleted
        assert!(storage.read_bytes(&addr1).await.is_err());
        // t=1 garbage manifest deleted
        assert!(storage.read_bytes(&garb_addr1).await.is_err());
        // t=2 and t=3 retained
        assert!(storage.read_bytes(&addr2).await.is_ok());
        assert!(storage.read_bytes(&addr3).await.is_ok());
        // t=2 garbage manifest retained
        assert!(storage.read_bytes(&garb_addr2).await.is_ok());
    }

    #[tokio::test]
    async fn test_clean_garbage_respects_time_threshold() {
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");

        // Recent timestamp (5 mins ago) -- NOT old enough
        let recent_ts = current_timestamp_ms() - (5 * 60 * 1000);

        let root1 = minimal_irb1(1, None, None);
        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_irb1(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["old"], "created_at_ms": {}}}"#,
            LEDGER, recent_ts
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
        };

        let result = clean_garbage(&storage, &cid3, LEDGER, config)
            .await
            .unwrap();

        // Nothing cleaned -- garbage too recent
        assert_eq!(result.indexes_cleaned, 0);
        assert_eq!(result.nodes_deleted, 0);

        // All roots still exist
        assert!(storage.read_bytes(&addr1).await.is_ok());
        assert!(storage.read_bytes(&addr2).await.is_ok());
        assert!(storage.read_bytes(&addr3).await.is_ok());
    }

    #[tokio::test]
    async fn test_clean_garbage_idempotent() {
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (old_cid, old_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        let root1 = minimal_irb1(1, None, None);
        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_irb1(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, old_cid, old_ts
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage.write_bytes(&old_addr, b"old data").await.unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
        };

        // First GC run
        let result1 = clean_garbage(&storage, &cid3, LEDGER, config.clone())
            .await
            .unwrap();
        assert_eq!(result1.indexes_cleaned, 1);
        assert!(storage.read_bytes(&addr1).await.is_err());

        // Second GC run -- idempotent (chain is now t=3->t=2, only 2 entries <= keep=2)
        let result2 = clean_garbage(&storage, &cid3, LEDGER, config)
            .await
            .unwrap();
        assert_eq!(result2.indexes_cleaned, 0);
        assert_eq!(result2.nodes_deleted, 0);

        // t=2 and t=3 still exist
        assert!(storage.read_bytes(&addr2).await.is_ok());
        assert!(storage.read_bytes(&addr3).await.is_ok());
    }

    #[tokio::test]
    async fn test_clean_garbage_multi_delete() {
        // Chain: t=5->t=4->t=3->t=2->t=1, max_old_indexes=1, keep=2 (t=5, t=4)
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (cid4, addr4) = cid_and_addr(ContentKind::IndexRoot, b"root4");
        let (cid5, addr5) = cid_and_addr(ContentKind::IndexRoot, b"root5");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (garb_cid3, garb_addr3) = cid_and_addr(ContentKind::GarbageRecord, b"garb3");
        let (garb_cid4, garb_addr4) = cid_and_addr(ContentKind::GarbageRecord, b"garb4");
        let (n1_cid, n1_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node1");
        let (n2_cid, n2_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node2");
        let (n3_cid, n3_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node3");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        let root1 = minimal_irb1(1, None, None);
        let root2 = minimal_irb1(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_irb1(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid3.clone(),
            }),
        );
        let root4 = minimal_irb1(
            4,
            Some(BinaryPrevIndexRef {
                t: 3,
                id: cid3.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid4.clone(),
            }),
        );
        let root5 = minimal_irb1(
            5,
            Some(BinaryPrevIndexRef {
                t: 4,
                id: cid4.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, n1_cid, old_ts
        );
        let garbage3 = format!(
            r#"{{"ledger_id": "{}", "t": 3, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, n2_cid, old_ts
        );
        let garbage4 = format!(
            r#"{{"ledger_id": "{}", "t": 4, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, n3_cid, old_ts
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage.write_bytes(&addr4, &root4).await.unwrap();
        storage.write_bytes(&addr5, &root5).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr3, garbage3.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr4, garbage4.as_bytes())
            .await
            .unwrap();
        storage.write_bytes(&n1_addr, b"n1").await.unwrap();
        storage.write_bytes(&n2_addr, b"n2").await.unwrap();
        storage.write_bytes(&n3_addr, b"n3").await.unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
        };

        let result = clean_garbage(&storage, &cid5, LEDGER, config)
            .await
            .unwrap();

        assert_eq!(result.indexes_cleaned, 3);
        assert_eq!(result.nodes_deleted, 3);

        // GC-eligible roots deleted
        assert!(storage.read_bytes(&addr1).await.is_err());
        assert!(storage.read_bytes(&addr2).await.is_err());
        assert!(storage.read_bytes(&addr3).await.is_err());
        // Nodes deleted
        assert!(storage.read_bytes(&n1_addr).await.is_err());
        assert!(storage.read_bytes(&n2_addr).await.is_err());
        assert!(storage.read_bytes(&n3_addr).await.is_err());
        // Retained
        assert!(storage.read_bytes(&addr4).await.is_ok());
        assert!(storage.read_bytes(&addr5).await.is_ok());
        assert!(storage.read_bytes(&garb_addr4).await.is_ok());
    }

    /// End-to-end: simulates an incremental index update that replaces leaf,
    /// branch, and dict CIDs, publishes a new root with garbage manifest,
    /// then verifies clean_garbage deletes exactly those replaced artifacts
    /// after the retention period.
    #[tokio::test]
    async fn test_incremental_gc_deletes_replaced_artifacts() {
        let storage = MemoryStorage::new();

        // --- Artifacts from the ORIGINAL (base) index at t=5 ---
        // These are the CAS blobs that get replaced during incremental update.
        let (old_leaf_spot_0, old_leaf_spot_0_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"spot-leaf-0-old");
        let (old_leaf_spot_1, old_leaf_spot_1_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"spot-leaf-1-old");
        let (old_branch_g1, old_branch_g1_addr) =
            cid_and_addr(ContentKind::IndexBranch, b"branch-g1-old");
        let (old_rev_branch, old_rev_branch_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"subj-rev-branch-old");
        let (old_rev_leaf, old_rev_leaf_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"subj-rev-leaf-old");

        // Write old artifacts to storage (they exist in CAS)
        storage
            .write_bytes(&old_leaf_spot_0_addr, b"old spot leaf 0")
            .await
            .unwrap();
        storage
            .write_bytes(&old_leaf_spot_1_addr, b"old spot leaf 1")
            .await
            .unwrap();
        storage
            .write_bytes(&old_branch_g1_addr, b"old g1 branch")
            .await
            .unwrap();
        storage
            .write_bytes(&old_rev_branch_addr, b"old rev branch")
            .await
            .unwrap();
        storage
            .write_bytes(&old_rev_leaf_addr, b"old rev leaf")
            .await
            .unwrap();

        // --- Base root at t=5 (the index before incremental update) ---
        let (base_root_cid, base_root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root-t5");
        let base_root_bytes = minimal_irb1(5, None, None);
        storage
            .write_bytes(&base_root_addr, &base_root_bytes)
            .await
            .unwrap();

        // --- Incremental update produces new root at t=10 ---
        // The pipeline accumulated these replaced CIDs:
        let replaced_cids = [
            old_leaf_spot_0.clone(),
            old_leaf_spot_1.clone(),
            old_branch_g1.clone(),
            old_rev_branch.clone(),
            old_rev_leaf.clone(),
        ];

        // Write garbage manifest (as the pipeline does via write_garbage_record)
        let old_ts = current_timestamp_ms() - (60 * 60 * 1000); // 1 hour ago
        let garbage_items: Vec<String> = replaced_cids.iter().map(|c| c.to_string()).collect();
        let garbage_json = format!(
            r#"{{"ledger_id": "{}", "t": 10, "garbage": [{}], "created_at_ms": {}}}"#,
            LEDGER,
            garbage_items
                .iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(","),
            old_ts
        );
        let (garb_cid, garb_addr) = cid_and_addr(ContentKind::GarbageRecord, b"garb-t10");
        storage
            .write_bytes(&garb_addr, garbage_json.as_bytes())
            .await
            .unwrap();

        // New root at t=10: prev_index → base root, garbage → manifest
        let (new_root_cid, new_root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root-t10");
        let new_root_bytes = minimal_irb1(
            10,
            Some(BinaryPrevIndexRef {
                t: 5,
                id: base_root_cid.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid.clone(),
            }),
        );
        storage
            .write_bytes(&new_root_addr, &new_root_bytes)
            .await
            .unwrap();

        // --- Before GC: all artifacts exist ---
        assert!(storage.read_bytes(&old_leaf_spot_0_addr).await.is_ok());
        assert!(storage.read_bytes(&old_leaf_spot_1_addr).await.is_ok());
        assert!(storage.read_bytes(&old_branch_g1_addr).await.is_ok());
        assert!(storage.read_bytes(&old_rev_branch_addr).await.is_ok());
        assert!(storage.read_bytes(&old_rev_leaf_addr).await.is_ok());
        assert!(storage.read_bytes(&base_root_addr).await.is_ok());

        // --- Run GC: max_old_indexes=0 means only keep current ---
        let config = CleanGarbageConfig {
            max_old_indexes: Some(0),
            min_time_garbage_mins: Some(30),
        };
        let result = clean_garbage(&storage, &new_root_cid, LEDGER, config)
            .await
            .unwrap();

        // Should delete 1 old index (t=5) and 5 replaced artifacts
        assert_eq!(result.indexes_cleaned, 1);
        assert_eq!(result.nodes_deleted, 5);

        // --- All replaced artifacts deleted ---
        assert!(storage.read_bytes(&old_leaf_spot_0_addr).await.is_err());
        assert!(storage.read_bytes(&old_leaf_spot_1_addr).await.is_err());
        assert!(storage.read_bytes(&old_branch_g1_addr).await.is_err());
        assert!(storage.read_bytes(&old_rev_branch_addr).await.is_err());
        assert!(storage.read_bytes(&old_rev_leaf_addr).await.is_err());

        // --- Old root deleted ---
        assert!(storage.read_bytes(&base_root_addr).await.is_err());

        // --- Current root + its garbage manifest retained ---
        assert!(storage.read_bytes(&new_root_addr).await.is_ok());
        assert!(storage.read_bytes(&garb_addr).await.is_ok());
    }
}
