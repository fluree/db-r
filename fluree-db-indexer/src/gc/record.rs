//! Garbage record types
//!
//! Defines the data structures for garbage collection records.

use serde::{Deserialize, Serialize};

// Re-export GarbageRef from core (used in DbRoot)
pub use fluree_db_core::serde::GarbageRef;

/// Garbage record containing obsolete addresses from a refresh operation.
///
/// Extends Clojure format with `created_at_ms` for time-based GC:
/// `{ :ledger_address "...", :t N, :garbage [...], :created_at_ms N }`
///
/// The garbage list is sorted and deduplicated for determinism.
/// Note: `created_at_ms` is safe to include because the garbage record is NOT
/// content-addressed (only its path is deterministic based on t).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GarbageRecord {
    /// Ledger address (e.g., "mydb:main")
    pub ledger_address: String,
    /// Transaction time this record was created for
    pub t: i64,
    /// Sorted, deduped list of obsolete addresses (index nodes + sketch files)
    pub garbage: Vec<String>,
    /// Wall-clock timestamp when this record was created (milliseconds since epoch)
    /// Used for time-based GC retention checks
    #[serde(default)]
    pub created_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_garbage_record_serialization() {
        let record = GarbageRecord {
            ledger_address: "test:ledger".to_string(),
            t: 42,
            garbage: vec![
                "fluree:file://test/ledger/index/spot/abc.json".to_string(),
                "fluree:file://test/ledger/index/spot/def.json".to_string(),
            ],
            created_at_ms: 1700000000000,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: GarbageRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, record);
    }

    #[test]
    fn test_garbage_record_json_format() {
        let record = GarbageRecord {
            ledger_address: "test:main".to_string(),
            t: 100,
            garbage: vec!["addr1".to_string(), "addr2".to_string()],
            created_at_ms: 1700000000000,
        };

        let json = serde_json::to_string(&record).unwrap();

        // Verify expected JSON structure
        assert!(json.contains("\"ledger_address\":\"test:main\""));
        assert!(json.contains("\"t\":100"));
        assert!(json.contains("\"garbage\":["));
        assert!(json.contains("\"created_at_ms\":1700000000000"));
    }

    #[test]
    fn test_garbage_record_empty_garbage() {
        let record = GarbageRecord {
            ledger_address: "test".to_string(),
            t: 1,
            garbage: vec![],
            created_at_ms: 0,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: GarbageRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.garbage.len(), 0);
    }

    #[test]
    fn test_garbage_record_backwards_compatible() {
        // Old format without created_at_ms should deserialize with default 0
        let old_json = r#"{"ledger_address":"test","t":1,"garbage":[]}"#;
        let parsed: GarbageRecord = serde_json::from_str(old_json).unwrap();

        assert_eq!(parsed.ledger_address, "test");
        assert_eq!(parsed.t, 1);
        assert_eq!(parsed.created_at_ms, 0); // Default value
    }
}
