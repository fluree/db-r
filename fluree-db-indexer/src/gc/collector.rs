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
use serde::Deserialize;

/// Entry in the prev-index chain.
struct IndexChainEntry {
    /// Transaction time of this index.
    t: i64,
    /// CAS address of this root blob.
    address: String,
    /// CAS address of the previous root in the chain (if any).
    prev_index_address: Option<String>,
    /// CAS address of this root's garbage manifest (if any).
    garbage_address: Option<String>,
}

// Lightweight structs for extracting GC-relevant fields from an index root.
// Handles both v3 (CID-based) and legacy (address-based) formats.

#[derive(Deserialize)]
struct GcRootFields {
    index_t: i64,
    #[serde(default)]
    prev_index: Option<GcPrevRef>,
    #[serde(default)]
    garbage: Option<GcGarbageRef>,
}

/// Prev-index reference. v3 uses `id` (CID string); legacy uses `address`.
#[derive(Deserialize)]
struct GcPrevRef {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    address: Option<String>,
}

/// Garbage manifest reference. v3 uses `id` (CID string); legacy uses `address`.
#[derive(Deserialize)]
struct GcGarbageRef {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    address: Option<String>,
}

/// Resolve a ref's CID or address to a storage address string.
///
/// If the ref has an `id` field (v3 CID string), parse it and derive the storage
/// address using `content_address`. Falls back to the raw `address` field (legacy).
fn resolve_ref_address(
    id: Option<&str>,
    address: Option<&str>,
    storage_method: &str,
    kind: ContentKind,
    ledger_id: &str,
) -> Option<String> {
    if let Some(cid_str) = id {
        if let Ok(cid) = cid_str.parse::<ContentId>() {
            return Some(fluree_db_core::content_address(
                storage_method,
                kind,
                ledger_id,
                &cid.digest_hex(),
            ));
        }
        // If CID parse fails, treat as opaque string (shouldn't happen in practice)
        tracing::warn!(cid_str, "Failed to parse CID in GC ref, skipping");
        return None;
    }
    address.map(|a| a.to_string())
}

/// Extract the GC-relevant fields from an index root blob.
///
/// Returns `(index_t, prev_index_address, garbage_address)` where addresses are
/// derived from CIDs for v3 roots or used directly for legacy roots.
fn parse_chain_fields(
    bytes: &[u8],
    storage_method: &str,
    ledger_id: &str,
) -> Result<(i64, Option<String>, Option<String>)> {
    let fields: GcRootFields = serde_json::from_slice(bytes)?;

    let prev_addr = fields.prev_index.as_ref().and_then(|p| {
        resolve_ref_address(
            p.id.as_deref(),
            p.address.as_deref(),
            storage_method,
            ContentKind::IndexRoot,
            ledger_id,
        )
    });

    let garbage_addr = fields.garbage.as_ref().and_then(|g| {
        resolve_ref_address(
            g.id.as_deref(),
            g.address.as_deref(),
            storage_method,
            ContentKind::GarbageRecord,
            ledger_id,
        )
    });

    Ok((fields.index_t, prev_addr, garbage_addr))
}

/// Walk the prev-index chain starting from the current root.
///
/// Returns entries in order from newest to oldest.
///
/// **Tolerant behavior**: If a prev_index link cannot be loaded (e.g., it was
/// deleted by prior GC), the walk stops gracefully at that point rather than
/// returning an error. This ensures GC is idempotent.
async fn walk_prev_index_chain<S: Storage>(
    storage: &S,
    current_root_address: &str,
    ledger_id: &str,
) -> Result<Vec<IndexChainEntry>> {
    let storage_method = storage.storage_method();
    let mut chain = Vec::new();
    let mut current_address = current_root_address.to_string();

    loop {
        // Load the db-root - if this fails on the first entry, propagate error
        // For subsequent entries (prev_index links), treat as end of chain
        let bytes = match storage.read_bytes(&current_address).await {
            Ok(b) => b,
            Err(e) => {
                if chain.is_empty() {
                    // Can't load the starting root - that's a real error
                    return Err(e.into());
                } else {
                    // Can't load prev_index - chain was truncated by prior GC
                    tracing::debug!(
                        address = %current_address,
                        "prev_index not found, chain ends here (prior GC)"
                    );
                    break;
                }
            }
        };

        let (t, prev_index_address, garbage_address) =
            parse_chain_fields(&bytes, storage_method, ledger_id)?;

        chain.push(IndexChainEntry {
            t,
            address: current_address.clone(),
            prev_index_address,
            garbage_address,
        });

        // Move to previous index if exists
        match chain.last().and_then(|e| e.prev_index_address.as_ref()) {
            Some(prev_addr) => {
                current_address = prev_addr.clone();
            }
            None => break,
        }
    }

    Ok(chain)
}

/// Resolve a garbage record item to a storage address for deletion.
///
/// v3 garbage records store CID strings (base32-lower multibase). Legacy records
/// store raw address strings. This function detects the format and derives the
/// storage address as needed.
fn resolve_garbage_item(item: &str, storage_method: &str, ledger_id: &str) -> String {
    // Try parsing as a CID. If successful, derive the storage address.
    if let Ok(cid) = item.parse::<ContentId>() {
        if let Some(kind) = cid.content_kind() {
            return fluree_db_core::content_address(
                storage_method,
                kind,
                ledger_id,
                &cid.digest_hex(),
            );
        }
    }
    // Not a CID — treat as raw address string (legacy format)
    item.to_string()
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

    // Derive the storage address from the root CID.
    let root_address = fluree_db_core::content_address(
        storage.storage_method(),
        ContentKind::IndexRoot,
        ledger_id,
        &current_root_id.digest_hex(),
    );

    // 1. Walk prev_index chain to collect all index versions (tolerant of missing roots)
    let index_chain = walk_prev_index_chain(storage, &root_address, ledger_id).await?;

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
        let garbage_addr = match &manifest_entry.garbage_address {
            Some(addr) => addr,
            None => {
                tracing::debug!(
                    t = manifest_entry.t,
                    "No garbage manifest in index, stopping GC"
                );
                break;
            }
        };

        // Load the garbage record to check age
        let record = match load_garbage_record(storage, garbage_addr).await {
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

        // Delete the garbage nodes (from entry_to_delete's tree).
        // Items may be CID strings (v3) or address strings (legacy).
        for item in &record.garbage {
            let addr = resolve_garbage_item(item, storage_method, ledger_id);
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

        // Delete entry_to_delete's own garbage manifest
        if let Some(ref old_garbage_addr) = entry_to_delete.garbage_address {
            if let Err(e) = storage.delete(old_garbage_addr).await {
                tracing::debug!(
                    address = %old_garbage_addr,
                    error = %e,
                    "Failed to delete old garbage manifest (may already be deleted)"
                );
            }
        }

        // Delete the old db-root
        if let Err(e) = storage.delete(&entry_to_delete.address).await {
            tracing::debug!(
                address = %entry_to_delete.address,
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
    use fluree_db_core::prelude::*;

    const LEDGER: &str = "test:main";

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
        // v3 format: prev_index/garbage use "id" (CID string)
        let (prev_cid, _) = cid_and_addr(ContentKind::IndexRoot, b"prev");
        let (garb_cid, _) = cid_and_addr(ContentKind::GarbageRecord, b"garb");

        let json = format!(
            r#"{{
                "index_t": 5,
                "prev_index": {{"t": 4, "id": "{}"}},
                "garbage": {{"id": "{}"}}
            }}"#,
            prev_cid, garb_cid,
        );
        let (t, prev, garbage) = parse_chain_fields(json.as_bytes(), "memory", LEDGER).unwrap();
        assert_eq!(t, 5);
        assert!(prev.is_some(), "prev should resolve");
        assert!(garbage.is_some(), "garbage should resolve");
    }

    #[test]
    fn test_parse_chain_fields_legacy_address() {
        // Legacy format: prev_index/garbage use "address"
        let json = r#"{
            "index_t": 5,
            "prev_index": {"address": "cas://prev"},
            "garbage": {"address": "cas://garbage"}
        }"#;
        let (t, prev, garbage) = parse_chain_fields(json.as_bytes(), "memory", LEDGER).unwrap();
        assert_eq!(t, 5);
        assert_eq!(prev.as_deref(), Some("cas://prev"));
        assert_eq!(garbage.as_deref(), Some("cas://garbage"));
    }

    #[test]
    fn test_parse_chain_fields_minimal() {
        let json = r#"{"index_t": 1}"#;
        let (t, prev, garbage) = parse_chain_fields(json.as_bytes(), "memory", LEDGER).unwrap();
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
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_resolve_garbage_item_legacy() {
        let addr = "fluree:file://test/main/index/spot/old.json";
        let resolved = resolve_garbage_item(addr, "memory", LEDGER);
        assert_eq!(resolved, addr);
    }

    #[tokio::test]
    async fn test_walk_empty_chain() {
        let storage = MemoryStorage::new();
        let (root_cid, root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root1");

        let root_json = r#"{"index_t": 1}"#;
        storage
            .write_bytes(&root_addr, root_json.as_bytes())
            .await
            .unwrap();

        let chain = walk_prev_index_chain(&storage, &root_addr, LEDGER)
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 1);
        assert_eq!(chain[0].address, root_addr);

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
    async fn test_walk_chain_v3_format() {
        // Test chain walking with v3 CID-based refs
        let storage = MemoryStorage::new();

        let (_, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid1, _) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (_, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid2, _) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (_, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");

        let root1 = r#"{"index_t": 1}"#;
        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}}}"#,
            cid1
        );
        let root3 = format!(
            r#"{{"index_t": 3, "prev_index": {{"t": 2, "id": "{}"}}}}"#,
            cid2
        );

        storage.write_bytes(&addr1, root1.as_bytes()).await.unwrap();
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();
        storage.write_bytes(&addr3, root3.as_bytes()).await.unwrap();

        let chain = walk_prev_index_chain(&storage, &addr3, LEDGER)
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

        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}}}"#,
            missing_cid
        );
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();

        let chain = walk_prev_index_chain(&storage, &addr2, LEDGER)
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 2);
    }

    #[tokio::test]
    async fn test_clean_garbage_v3_clojure_semantics() {
        // Test Clojure parity GC with v3 CID-based refs and garbage items.
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid1, garb_addr1) = cid_and_addr(ContentKind::GarbageRecord, b"garb1");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (old_leaf_cid, old_leaf_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old_leaf");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        // t=1: oldest, has its own garbage manifest
        let root1 = format!(r#"{{"index_t": 1, "garbage": {{"id": "{}"}}}}"#, garb_cid1);

        // t=2: points to t=1, has garbage manifest (nodes replaced from t=1→t=2)
        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid1, garb_cid2
        );

        // t=3: current, points to t=2
        let root3 = format!(
            r#"{{"index_t": 3, "prev_index": {{"t": 2, "id": "{}"}}}}"#,
            cid2
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

        storage.write_bytes(&addr1, root1.as_bytes()).await.unwrap();
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();
        storage.write_bytes(&addr3, root3.as_bytes()).await.unwrap();
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

        // Old leaf deleted via CID→address resolution
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

        // Recent timestamp (5 mins ago) — NOT old enough
        let recent_ts = current_timestamp_ms() - (5 * 60 * 1000);

        let root1 = r#"{"index_t": 1}"#;
        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid1, garb_cid2
        );
        let root3 = format!(
            r#"{{"index_t": 3, "prev_index": {{"t": 2, "id": "{}"}}}}"#,
            cid2
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["old"], "created_at_ms": {}}}"#,
            LEDGER, recent_ts
        );

        storage.write_bytes(&addr1, root1.as_bytes()).await.unwrap();
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();
        storage.write_bytes(&addr3, root3.as_bytes()).await.unwrap();
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

        // Nothing cleaned — garbage too recent
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

        let root1 = r#"{"index_t": 1}"#;
        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid1, garb_cid2
        );
        let root3 = format!(
            r#"{{"index_t": 3, "prev_index": {{"t": 2, "id": "{}"}}}}"#,
            cid2
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{}", "t": 2, "garbage": ["{}"], "created_at_ms": {}}}"#,
            LEDGER, old_cid, old_ts
        );

        storage.write_bytes(&addr1, root1.as_bytes()).await.unwrap();
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();
        storage.write_bytes(&addr3, root3.as_bytes()).await.unwrap();
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

        // Second GC run — idempotent (chain is now t=3→t=2, only 2 entries ≤ keep=2)
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
        // Chain: t=5→t=4→t=3→t=2→t=1, max_old_indexes=1, keep=2 (t=5, t=4)
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

        let root1 = r#"{"index_t": 1}"#;
        let root2 = format!(
            r#"{{"index_t": 2, "prev_index": {{"t": 1, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid1, garb_cid2
        );
        let root3 = format!(
            r#"{{"index_t": 3, "prev_index": {{"t": 2, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid2, garb_cid3
        );
        let root4 = format!(
            r#"{{"index_t": 4, "prev_index": {{"t": 3, "id": "{}"}}, "garbage": {{"id": "{}"}}}}"#,
            cid3, garb_cid4
        );
        let root5 = format!(
            r#"{{"index_t": 5, "prev_index": {{"t": 4, "id": "{}"}}}}"#,
            cid4
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

        storage.write_bytes(&addr1, root1.as_bytes()).await.unwrap();
        storage.write_bytes(&addr2, root2.as_bytes()).await.unwrap();
        storage.write_bytes(&addr3, root3.as_bytes()).await.unwrap();
        storage.write_bytes(&addr4, root4.as_bytes()).await.unwrap();
        storage.write_bytes(&addr5, root5.as_bytes()).await.unwrap();
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
}
