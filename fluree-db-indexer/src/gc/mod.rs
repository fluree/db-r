//! # Garbage Collection
//!
//! Garbage collection for content-addressed storage.
//!
//! During index building, CAS artifacts (dicts, branches, leaves) that are no
//! longer referenced by the new root are recorded in a garbage manifest.
//! The GC collector walks the `prev_index` chain, identifies gc-eligible roots,
//! and deletes their obsolete artifacts.
//!
//! ## Design
//!
//! 1. **During build**: Compute `old_root.all_cas_addresses() \ new_root.all_cas_addresses()`
//! 2. **After build**: Write a garbage record with the replaced addresses
//! 3. **On-demand cleanup**: Walk the prev-index chain, identify eligible garbage,
//!    and delete nodes not reachable from any live index
//!
//! ## Garbage Record Naming
//!
//! Garbage records are written using storage-owned addressing with sorted/deduped
//! record bytes. Each record includes a `created_at_ms` wall-clock timestamp for
//! time-based retention checks. Because of the timestamp, records are
//! indexer-specific (not deterministic across concurrent indexers), but this is
//! harmless since only one indexer wins the publish race.
//!
//! ## Time-Based Retention
//!
//! GC respects two thresholds:
//! - `max_old_indexes`: Maximum number of old index versions to keep (default: 5)
//! - `min_time_garbage_mins`: Minimum age before an index can be GC'd (default: 30)
//!
//! Both thresholds must be satisfied for GC to occur.

mod collector;
mod record;

pub use collector::clean_garbage;
pub use record::{GarbageRecord, GarbageRef};

use crate::error::Result;
use fluree_db_core::{ContentAddressedWrite, ContentKind, Storage};

/// Default maximum number of old indexes to retain
pub const DEFAULT_MAX_OLD_INDEXES: u32 = 5;

/// Default minimum age (in minutes) before an index can be garbage collected
pub const DEFAULT_MIN_TIME_GARBAGE_MINS: u32 = 30;

/// Configuration for garbage collection
#[derive(Debug, Clone, Default)]
pub struct CleanGarbageConfig {
    /// Maximum number of old indexes to keep (None = default 5)
    ///
    /// With max_old_indexes=5, we keep current + 5 old = 6 total index versions.
    pub max_old_indexes: Option<u32>,
    /// Minimum age in minutes before GC (None = default 30)
    ///
    /// Garbage records must be at least this old before their nodes can be deleted.
    pub min_time_garbage_mins: Option<u32>,
}

/// Result of garbage collection
#[derive(Debug, Clone, Default)]
pub struct CleanGarbageResult {
    /// Number of old index versions cleaned up
    pub indexes_cleaned: usize,
    /// Number of nodes deleted
    pub nodes_deleted: usize,
}

/// Write a garbage record to storage.
///
/// Returns `None` if there are no garbage addresses to record.
/// The garbage addresses are sorted and deduplicated before writing.
/// Includes a wall-clock `created_at_ms` timestamp for time-based GC retention.
pub async fn write_garbage_record<S: ContentAddressedWrite>(
    storage: &S,
    ledger_id: &str,
    t: i64,
    garbage_addresses: Vec<String>,
) -> Result<Option<GarbageRef>> {
    let mut garbage_addresses = garbage_addresses;
    if garbage_addresses.is_empty() {
        return Ok(None);
    }

    // Sort and dedupe for determinism
    garbage_addresses.sort();
    garbage_addresses.dedup();

    let record = GarbageRecord {
        ledger_id: ledger_id.to_string(),
        t,
        garbage: garbage_addresses,
        created_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    };

    let bytes = serde_json::to_vec(&record)?;
    let res = storage
        .content_write_bytes(ContentKind::GarbageRecord, ledger_id, &bytes)
        .await?;

    Ok(Some(GarbageRef {
        address: res.address,
        content_hash: res.content_hash,
    }))
}

/// Load a garbage record from storage.
pub async fn load_garbage_record<S: Storage>(storage: &S, address: &str) -> Result<GarbageRecord> {
    let bytes = storage.read_bytes(address).await?;
    let record: GarbageRecord = serde_json::from_slice(&bytes)?;
    Ok(record)
}
