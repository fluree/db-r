//! BM25-owned manifest for snapshot history.
//!
//! The manifest is stored as JSON in content-addressed storage (CAS).
//! Nameservice stores only a head pointer to the latest manifest address.
//! BM25 owns time-travel selection logic via [`Bm25Manifest::select_snapshot`].

use serde::{Deserialize, Serialize};

/// A single snapshot entry in the BM25 manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bm25SnapshotEntry {
    /// Transaction time (watermark) for this snapshot.
    pub index_t: i64,
    /// Storage address of the serialized BM25 index blob.
    pub snapshot_address: String,
}

impl Bm25SnapshotEntry {
    pub fn new(index_t: i64, snapshot_address: impl Into<String>) -> Self {
        Self {
            index_t,
            snapshot_address: snapshot_address.into(),
        }
    }
}

/// BM25-owned manifest for snapshot history and time-travel.
///
/// Each manifest is immutable and content-addressed (keyed by latest `index_t`).
/// The nameservice head pointer stores the CAS address of the latest manifest.
///
/// # Append semantics
///
/// Entries must be strictly monotonically increasing by `index_t`, with one
/// exception: the last entry may be **replaced** when `index_t == last.index_t`
/// (idempotent reindex).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bm25Manifest {
    /// Graph source alias this manifest belongs to (e.g., "my-search:main").
    pub alias: String,
    /// Ordered list of snapshots (ascending by `index_t`).
    pub snapshots: Vec<Bm25SnapshotEntry>,
}

impl Bm25Manifest {
    /// Create a new empty manifest.
    pub fn new(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            snapshots: Vec::new(),
        }
    }

    /// Select the best snapshot for a given `as_of_t`.
    ///
    /// Returns the snapshot with `index_t = max { t | t <= as_of_t }`,
    /// or `None` if no suitable snapshot exists.
    pub fn select_snapshot(&self, as_of_t: i64) -> Option<&Bm25SnapshotEntry> {
        self.snapshots.iter().rev().find(|s| s.index_t <= as_of_t)
    }

    /// Get the most recent snapshot (head).
    pub fn head(&self) -> Option<&Bm25SnapshotEntry> {
        self.snapshots.last()
    }

    /// Check if a snapshot exists at exactly the given `t`.
    pub fn has_snapshot_at(&self, t: i64) -> bool {
        self.snapshots.iter().any(|s| s.index_t == t)
    }

    /// Append a snapshot entry.
    ///
    /// Entries must be strictly monotonically increasing, except that
    /// the last entry may be replaced if `index_t == last.index_t`
    /// (idempotent reindex). Returns `true` if added/replaced, `false`
    /// if rejected (lower `t` than existing).
    pub fn append(&mut self, entry: Bm25SnapshotEntry) -> bool {
        if let Some(last) = self.snapshots.last() {
            if entry.index_t < last.index_t {
                return false; // Reject: going backwards
            }
            if entry.index_t == last.index_t {
                // Idempotent reindex: replace last entry
                *self.snapshots.last_mut().unwrap() = entry;
                return true;
            }
        }
        self.snapshots.push(entry);
        true
    }

    /// Get all snapshot addresses (for cleanup/deletion on drop).
    pub fn all_snapshot_addresses(&self) -> Vec<&str> {
        self.snapshots
            .iter()
            .map(|s| s.snapshot_address.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_manifest() {
        let m = Bm25Manifest::new("test:main");
        assert_eq!(m.alias, "test:main");
        assert!(m.snapshots.is_empty());
        assert!(m.head().is_none());
        assert!(m.select_snapshot(100).is_none());
        assert!(!m.has_snapshot_at(1));
        assert!(m.all_snapshot_addresses().is_empty());
    }

    #[test]
    fn test_append_and_head() {
        let mut m = Bm25Manifest::new("test:main");
        assert!(m.append(Bm25SnapshotEntry::new(5, "addr-5")));
        assert!(m.append(Bm25SnapshotEntry::new(10, "addr-10")));
        assert!(m.append(Bm25SnapshotEntry::new(20, "addr-20")));

        assert_eq!(m.snapshots.len(), 3);
        assert_eq!(m.head().unwrap().index_t, 20);
    }

    #[test]
    fn test_append_monotonic_rejection() {
        let mut m = Bm25Manifest::new("test:main");
        assert!(m.append(Bm25SnapshotEntry::new(10, "addr-10")));
        assert!(!m.append(Bm25SnapshotEntry::new(5, "addr-5"))); // Rejected: going backwards
        assert_eq!(m.snapshots.len(), 1);
    }

    #[test]
    fn test_append_same_t_replaces_last() {
        let mut m = Bm25Manifest::new("test:main");
        assert!(m.append(Bm25SnapshotEntry::new(5, "addr-5")));
        assert!(m.append(Bm25SnapshotEntry::new(10, "addr-10-v1")));
        assert!(m.append(Bm25SnapshotEntry::new(10, "addr-10-v2"))); // Idempotent reindex

        assert_eq!(m.snapshots.len(), 2);
        assert_eq!(m.head().unwrap().snapshot_address, "addr-10-v2");
        // Earlier entry is untouched
        assert_eq!(m.snapshots[0].snapshot_address, "addr-5");
    }

    #[test]
    fn test_select_snapshot() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(5, "addr-5"));
        m.append(Bm25SnapshotEntry::new(10, "addr-10"));
        m.append(Bm25SnapshotEntry::new(20, "addr-20"));

        // Before any snapshot
        assert!(m.select_snapshot(3).is_none());

        // Exact match
        assert_eq!(m.select_snapshot(5).unwrap().index_t, 5);
        assert_eq!(m.select_snapshot(10).unwrap().index_t, 10);
        assert_eq!(m.select_snapshot(20).unwrap().index_t, 20);

        // Between snapshots: returns largest <= as_of_t
        assert_eq!(m.select_snapshot(7).unwrap().index_t, 5);
        assert_eq!(m.select_snapshot(15).unwrap().index_t, 10);

        // After all snapshots
        assert_eq!(m.select_snapshot(100).unwrap().index_t, 20);
    }

    #[test]
    fn test_has_snapshot_at() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(5, "addr-5"));
        m.append(Bm25SnapshotEntry::new(10, "addr-10"));

        assert!(m.has_snapshot_at(5));
        assert!(m.has_snapshot_at(10));
        assert!(!m.has_snapshot_at(7));
        assert!(!m.has_snapshot_at(1));
    }

    #[test]
    fn test_all_snapshot_addresses() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(5, "addr-5"));
        m.append(Bm25SnapshotEntry::new(10, "addr-10"));

        let addrs = m.all_snapshot_addresses();
        assert_eq!(addrs, vec!["addr-5", "addr-10"]);
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut m = Bm25Manifest::new("my-search:main");
        m.append(Bm25SnapshotEntry::new(
            5,
            "fluree:file://graph-sources/search/main/bm25/t5/snapshot.bin",
        ));
        m.append(Bm25SnapshotEntry::new(
            10,
            "fluree:file://graph-sources/search/main/bm25/t10/snapshot.bin",
        ));

        let json = serde_json::to_string(&m).unwrap();
        let deserialized: Bm25Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, deserialized);
    }
}
