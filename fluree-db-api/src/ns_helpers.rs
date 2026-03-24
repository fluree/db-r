//! Namespace reconciliation helpers shared across load paths.
//!
//! These helpers deduplicate the pattern of syncing namespace codes between
//! a `BinaryIndexStore` (index root) and a `LedgerSnapshot` (commit chain),
//! enforcing Rule 5 (commit chain is the namespace source of truth) and
//! Rule 3 (bimap uniqueness / immutability).

use crate::error::{ApiError, Result};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::LedgerSnapshot;

/// Sync namespace codes between a `BinaryIndexStore` and a `LedgerSnapshot`.
///
/// Performs three operations in order:
/// 1. **Augments** the store with any namespace codes from the snapshot that the
///    store doesn't have (post-index allocations from novelty commits).
/// 2. **Sets** the store's split mode to match the snapshot.
/// 3. **Reconciles** the store's namespace codes back into the snapshot, validating
///    that no conflicts exist. Any new codes from the index root are inserted into
///    the snapshot (keeps both forward and reverse maps in sync).
///
/// Returns `Err` if a namespace conflict is detected (indicates an indexer/publisher
/// bug or storage corruption).
pub fn sync_store_and_snapshot_ns(
    store: &mut BinaryIndexStore,
    snapshot: &mut LedgerSnapshot,
) -> Result<()> {
    // 1. Augment the store with snapshot namespace codes (post-index allocations).
    store
        .augment_namespace_codes(snapshot.namespaces())
        .map_err(|e| ApiError::internal(format!("augment namespace codes: {}", e)))?;

    // 2. Sync split mode.
    store.set_ns_split_mode(snapshot.ns_split_mode);

    // 3. Reconcile store codes back into snapshot (validation + insert).
    for (code, prefix) in store.namespace_codes() {
        if let Some(existing) = snapshot.namespaces().get(code) {
            if existing != prefix {
                return Err(ApiError::internal(format!(
                    "namespace reconciliation failure: index root ns code {} maps to {:?} \
                     but commit chain has {:?} — possible indexer/publisher bug",
                    code, prefix, existing
                )));
            }
        }
        snapshot.insert_namespace_code(*code, prefix.clone());
    }

    Ok(())
}
