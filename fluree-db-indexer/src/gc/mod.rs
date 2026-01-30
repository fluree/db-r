//! # Garbage Collection
//!
//! Garbage collection for content-addressed storage.
//!
//! During incremental refresh, some index nodes are replaced while others are reused.
//! The GC system tracks replaced nodes and provides a mechanism to clean them up
//! after they're no longer needed.
//!
//! ## Design
//!
//! 1. **During refresh**: Track replaced node addresses in `RefreshStats.replaced_nodes`
//! 2. **After refresh**: Write a garbage record file with all replaced addresses
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
    alias: &str,
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
        alias: alias.to_string(),
        t,
        garbage: garbage_addresses,
        created_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    };

    let bytes = serde_json::to_vec(&record)?;
    let res = storage
        .content_write_bytes(ContentKind::GarbageRecord, alias, &bytes)
        .await?;

    Ok(Some(GarbageRef {
        address: res.address,
    }))
}

/// Load a garbage record from storage.
pub async fn load_garbage_record<S: Storage>(
    storage: &S,
    address: &str,
) -> Result<GarbageRecord> {
    let bytes = storage.read_bytes(address).await?;
    let record: GarbageRecord = serde_json::from_slice(&bytes)?;
    Ok(record)
}

/// Collect garbage addresses from all index refresh stats.
///
/// Merges replaced_nodes from all 5 index trees (spot, psot, post, opst, tspo)
/// and any obsolete sketch files. Returns a sorted, deduplicated list.
pub fn collect_garbage_addresses(
    stats: &[&crate::refresh::RefreshStats],
    obsolete_sketches: Vec<String>,
) -> Vec<String> {
    let mut all_garbage: Vec<String> = Vec::new();

    // Collect replaced nodes from all index stats
    for stat in stats {
        all_garbage.extend(stat.replaced_nodes.iter().cloned());
    }

    // Include obsolete sketch files
    all_garbage.extend(obsolete_sketches);

    // Sort and dedupe for determinism
    all_garbage.sort();
    all_garbage.dedup();

    all_garbage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresh::RefreshStats;

    #[test]
    fn test_collect_garbage_addresses_empty() {
        let stats: Vec<&RefreshStats> = vec![];
        let result = collect_garbage_addresses(&stats, vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_collect_garbage_addresses_merges_and_dedupes() {
        let stat1 = RefreshStats {
            replaced_nodes: vec!["addr1".to_string(), "addr2".to_string()],
            ..Default::default()
        };
        let stat2 = RefreshStats {
            replaced_nodes: vec!["addr2".to_string(), "addr3".to_string()],
            ..Default::default()
        };

        let result = collect_garbage_addresses(
            &[&stat1, &stat2],
            vec!["sketch1".to_string()],
        );

        // Should be sorted and deduped
        assert_eq!(result, vec!["addr1", "addr2", "addr3", "sketch1"]);
    }

    #[test]
    fn test_collect_garbage_addresses_sorts() {
        let stat = RefreshStats {
            replaced_nodes: vec!["z".to_string(), "a".to_string(), "m".to_string()],
            ..Default::default()
        };

        let result = collect_garbage_addresses(&[&stat], vec![]);
        assert_eq!(result, vec!["a", "m", "z"]);
    }
}
